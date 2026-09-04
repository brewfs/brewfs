[CmdletBinding()]
param(
    [ValidateSet('all', 'create', 'test', 'destroy', 'status')]
    [string]$Action = 'test',
    [string]$ClusterId,
    [string]$ClusterName,
    [string]$KeyPairName,
    [string]$RegionId = 'ap-northeast-2',
    [string]$ZoneId = 'ap-northeast-2a',
    [string]$Namespace = 'default',
    [string]$KubeconfigPath,
    [string]$OperatorImage = 'ghcr.io/ivanbeethoven/brewfs-operator:latest',
    [string]$BrewfsImage = 'ghcr.io/ivanbeethoven/brewfs:latest',
    [string]$GhcrToken,
    [switch]$NoCleanup,
    [switch]$RunRestartTest
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$ManifestDir = Join-Path $RepoRoot 'operator\brewfs-operator\manifests'

function Resolve-Executable([string]$Name, [string[]]$Candidates = @()) {
    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($command) { return $command.Source }
    foreach ($candidate in $Candidates) {
        if (Test-Path -LiteralPath $candidate) { return $candidate }
    }
    throw "找不到 $Name，请先安装并加入 PATH。"
}

$aliyunCandidates = @()
$kubectlCandidates = @()
if ($env:LOCALAPPDATA) {
    $aliyunCandidates += (Join-Path $env:LOCALAPPDATA 'AliyunCLI\aliyun.exe')
    $kubectlCandidates += (Join-Path $env:LOCALAPPDATA 'Microsoft\WinGet\Packages\Kubernetes.kubectl_Microsoft.Winget.Source_8wekyb3d8bbwe\kubectl.exe')
}
$Aliyun = Resolve-Executable 'aliyun' $aliyunCandidates
$Kubectl = Resolve-Executable 'kubectl' $kubectlCandidates
$Gh = Resolve-Executable 'gh'

function Invoke-Checked([string]$File, [string[]]$Arguments) {
    $output = & $File @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "命令失败: $File $($Arguments -join ' ')`n$($output -join [Environment]::NewLine)"
    }
    return $output
}

function Invoke-Optional([string]$File, [string[]]$Arguments) {
    $output = & $File @Arguments 2>&1
    return [pscustomobject]@{ ExitCode = $LASTEXITCODE; Output = $output }
}

function Invoke-AliyunJson([string[]]$Arguments) {
    $output = Invoke-Checked $Aliyun $Arguments
    return ($output -join [Environment]::NewLine | ConvertFrom-Json)
}

function Invoke-Kubectl([string[]]$Arguments) {
    return Invoke-Checked $Kubectl $Arguments
}

function Invoke-KubectlYaml([string]$Yaml) {
    $output = $Yaml | & $Kubectl @('apply', '-f', '-') 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl apply YAML 失败:`n$($output -join [Environment]::NewLine)"
    }
    return $output
}

function Wait-Until([scriptblock]$Condition, [string]$Description, [int]$TimeoutSeconds = 900) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            if (& $Condition) { return }
        } catch {
            # Resources may not exist during the first reconciliation pass.
        }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)
    throw "等待超时: $Description"
}

function Get-Cluster([string]$Id) {
    return Invoke-AliyunJson @('cs', 'DescribeClusterDetail', '--region', $RegionId, '--ClusterId', $Id)
}

function Wait-AckTask([string]$TaskId, [int]$TimeoutSeconds = 1800) {
    if (-not $TaskId) { return }
    $script:AckTaskFailure = $null
    Wait-Until {
        $task = Invoke-AliyunJson @('cs', 'DescribeTaskInfo', '--region', $RegionId, '--task_id', $TaskId)
        $state = if ($task.state) { $task.state } else { $task.status }
        Write-Host "  ACK task $TaskId state=$state"
        if ($state -in @('success', 'failed', 'error')) {
            if ($state -ne 'success') { $script:AckTaskFailure = $state }
            return $true
        }
        return $false
    } "ACK task $TaskId 完成" $TimeoutSeconds
    if ($script:AckTaskFailure) { throw "ACK task $TaskId failed: $script:AckTaskFailure" }
}

function New-AckCluster {
    if (-not $script:ClusterName) {
        $script:ClusterName = 'brewfs-e2e-{0}' -f (Get-Date -Format 'yyyyMMdd-HHmmss')
    }
    if (-not $script:KeyPairName) {
        $script:KeyPairName = 'brewfs-e2e-{0}' -f (Get-Date -Format 'yyyyMMddHHmmss')
    }
    $body = [ordered]@{
        name = $ClusterName
        cluster_type = 'ManagedKubernetes'
        cluster_spec = 'ack.standard'
        profile = 'Default'
        region_id = $RegionId
        zone_ids = @($ZoneId)
        pod_cidr = '172.20.0.0/16'
        service_cidr = '172.21.0.0/20'
        endpoint_public_access = $true
        snat_entry = $true
        num_of_nodes = 1
        worker_instance_types = @('ecs.e-c1m2.large')
        key_pair = $KeyPairName
        worker_system_disk_category = 'cloud_essd'
        worker_system_disk_size = 40
        node_cidr_mask = '24'
        proxy_mode = 'ipvs'
        runtime = @{ name = 'containerd'; version = '1.6.28' }
        addons = @(
            @{ name = 'flannel'; config = '{}' },
            @{ name = 'csi-plugin'; config = '{}' },
            @{ name = 'csi-provisioner'; config = '{}' }
        )
    }
    $json = $body | ConvertTo-Json -Depth 8 -Compress
    $result = Invoke-AliyunJson @('cs', 'CreateCluster', '--region', $RegionId, '--body', $json)
    $script:ClusterId = $result.cluster_id
    if (-not $ClusterId) { throw 'CreateCluster 未返回 cluster_id。' }
    Write-Host "ACK 集群创建任务已提交: $ClusterId"
    Wait-Until {
        $state = (Get-Cluster $ClusterId).state
        Write-Host "  cluster state=$state"
        $state -eq 'running'
    } 'ACK 集群就绪' 1800
}

function Write-Kubeconfig {
    if (-not $KubeconfigPath) {
        $script:KubeconfigPath = Join-Path $env:TEMP "$ClusterId-ack.yaml"
    }
    $response = Invoke-AliyunJson @(
        'cs', 'DescribeClusterUserKubeconfig', '--region', $RegionId,
        '--ClusterId', $ClusterId, '--TemporaryDurationMinutes', '180'
    )
    $config = $response.config
    if (-not $config) { throw 'DescribeClusterUserKubeconfig 未返回 config。' }
    try {
        $bytes = [Convert]::FromBase64String($config)
        $config = [Text.Encoding]::UTF8.GetString($bytes)
    } catch {
        # 某些 ACK 版本直接返回 YAML。
    }
    # Windows PowerShell 5.1 也支持 UTF-8（带 BOM 的 kubeconfig 对 kubectl 有效）。
    Set-Content -LiteralPath $KubeconfigPath -Value $config -Encoding UTF8
    $env:KUBECONFIG = $KubeconfigPath
    Invoke-Kubectl @('cluster-info') | Out-Null
    Write-Host "kubeconfig: $KubeconfigPath"
}

function Ensure-GhcrPullSecret {
    if (-not $script:GhcrToken) {
        $script:GhcrToken = $env:GHCR_TOKEN
    }
    if (-not $GhcrToken) {
        $script:GhcrToken = ((Invoke-Checked $Gh @('auth', 'token')) -join '').Trim()
    }
    if (-not $GhcrToken) { throw '未取得 GHCR token；请设置 GHCR_TOKEN 或执行 gh auth login。' }
    $ghUser = ((Invoke-Checked $Gh @('api', 'user', '--jq', '.login')) -join '').Trim()
    foreach ($ns in @('brewfs-system', $Namespace)) {
        $yaml = Invoke-Checked $Kubectl @(
            'create', 'secret', 'docker-registry', 'ghcr-pull', '-n', $ns,
            '--docker-server=ghcr.io', "--docker-username=$ghUser",
            "--docker-password=$GhcrToken", '--dry-run=client', '-o', 'yaml'
        )
        Invoke-KubectlYaml ($yaml -join [Environment]::NewLine) | Out-Null
        Invoke-Kubectl @('patch', 'serviceaccount', 'default', '-n', $ns,
            '--type', 'merge', '-p', '{"imagePullSecrets":[{"name":"ghcr-pull"}]}') | Out-Null
    }
    Invoke-Kubectl @('patch', 'serviceaccount', 'brewfs-operator', '-n', 'brewfs-system',
        '--type', 'merge', '-p', '{"imagePullSecrets":[{"name":"ghcr-pull"}]}') | Out-Null
}

function Ensure-StorageClass {
    $result = Invoke-Optional $Kubectl @('get', 'storageclass', 'alicloud-disk-efficiency')
    if ($result.ExitCode -eq 0) {
        Invoke-Kubectl @('annotate', 'storageclass', 'alicloud-disk-efficiency',
            'storageclass.kubernetes.io/is-default-class=false', '--overwrite') | Out-Null
    }
    $result = Invoke-Optional $Kubectl @('get', 'storageclass', 'alicloud-disk-essd')
    if ($result.ExitCode -eq 0) {
        Invoke-Kubectl @('annotate', 'storageclass', 'alicloud-disk-essd',
            'storageclass.kubernetes.io/is-default-class=true', '--overwrite') | Out-Null
    }
}

function Install-Operator {
    Invoke-Kubectl @('apply', '-k', $ManifestDir) | Out-Null
    Invoke-Kubectl @('set', 'image', 'deployment/brewfs-operator',
        "operator=$OperatorImage", '-n', 'brewfs-system') | Out-Null
    Invoke-Kubectl @('rollout', 'status', 'deployment/brewfs-operator', '-n', 'brewfs-system', '--timeout=10m') | Out-Null
}

function Wait-CustomResource([string]$Kind, [string]$Name) {
    Wait-Until {
        $json = (Invoke-Kubectl @('get', $Kind, $Name, '-n', $Namespace, '-o', 'json') -join '') | ConvertFrom-Json
        $ready = @($json.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' })
        if ($ready.Count -gt 0) { return $true }
        Write-Host "  $Kind/$Name 尚未 Ready"
        return $false
    } "$Kind/$Name Ready" 1200
}

function Get-MountPod {
    $json = (Invoke-Kubectl @('get', 'pods', '-n', $Namespace, '-o', 'json') -join '') | ConvertFrom-Json
    $pod = @($json.items | Where-Object { $_.metadata.name -like 'demo-mount-mount-*' -and $_.status.phase -eq 'Running' } | Select-Object -First 1)
    if ($pod.Count -eq 0) { throw '找不到运行中的 BrewFSMount Pod。' }
    return $pod[0].metadata.name
}

function Invoke-SmokeTest([string]$Pod) {
    $script = 'set -eu; root=/mnt/brewfs/e2e; rm -rf "$root"; mkdir -p "$root/sub"; printf alpha > "$root/sub/a"; test "$(cat "$root/sub/a")" = alpha; cp "$root/sub/a" "$root/sub/b"; mv "$root/sub/b" "$root/sub/c"; ln "$root/sub/c" "$root/sub/hard"; ln -s "$root/sub/c" "$root/sub/sym"; chmod 640 "$root/sub/c"; test "$(cat "$root/sub/sym")" = alpha; test "$(stat -c %a "$root/sub/c")" = 640; echo file-ops-ok'
    Invoke-Kubectl @('exec', '-n', $Namespace, $Pod, '--', 'sh', '-c', $script)
}

function Invoke-FuseCleanup {
    $yaml = @"
apiVersion: v1
kind: Pod
metadata:
  name: fuse-cleanup
  namespace: $Namespace
spec:
  hostPID: true
  restartPolicy: Never
  containers:
  - name: cleanup
    image: alpine:3.20
    securityContext:
      privileged: true
    command: ["/bin/sh", "-c"]
    args:
    - apk add --no-cache util-linux >/dev/null && nsenter -t 1 -m -- umount -l /var/lib/brewfs/mounts/demo
"@
    Invoke-KubectlYaml $yaml | Out-Null
    Wait-Until {
        $phase = ((Invoke-Kubectl @('get', 'pod', 'fuse-cleanup', '-n', $Namespace, '-o', 'jsonpath={.status.phase}') -join '')).Trim()
        $phase -in @('Succeeded', 'Failed')
    } '清理旧 FUSE 挂载' 300
    Invoke-Kubectl @('delete', 'pod', 'fuse-cleanup', '-n', $Namespace, '--ignore-not-found') | Out-Null
}

function Run-Test {
    Ensure-GhcrPullSecret
    Ensure-StorageClass
    Install-Operator
    Invoke-Kubectl @('apply', '-f', (Join-Path $ManifestDir 'example-cluster.yaml'), '-n', $Namespace) | Out-Null
    Invoke-Kubectl @('apply', '-f', (Join-Path $ManifestDir 'example-mount.yaml'), '-n', $Namespace) | Out-Null
    Wait-Until {
        $ds = Invoke-Optional $Kubectl @('get', 'daemonset', 'demo-mount-mount', '-n', $Namespace)
        $deploy = Invoke-Optional $Kubectl @('get', 'deployment', 'demo-mount-mount', '-n', $Namespace)
        if ($ds.ExitCode -eq 0) {
            Invoke-Kubectl @('set', 'image', 'daemonset/demo-mount-mount', "brewfs=$BrewfsImage", '-n', $Namespace) | Out-Null
            return $true
        }
        if ($deploy.ExitCode -eq 0) {
            Invoke-Kubectl @('set', 'image', 'deployment/demo-mount-mount', "brewfs=$BrewfsImage", '-n', $Namespace) | Out-Null
            return $true
        }
        return $false
    } 'BrewFSMount 工作负载创建' 600
    Wait-CustomResource 'brewfscluster' 'demo'
    Wait-CustomResource 'brewfsmount' 'demo-mount'
    $pod = Get-MountPod
    Invoke-SmokeTest $pod
    if ($RunRestartTest) {
        Invoke-Kubectl @('delete', 'pod', $pod, '-n', $Namespace, '--wait=false') | Out-Null
        Invoke-FuseCleanup
        Wait-Until { (Get-MountPod) -ne $pod } 'BrewFSMount Pod 重建' 600
        Invoke-SmokeTest (Get-MountPod)
        Write-Host 'persistence-after-restart-ok'
    }
}

function Remove-AckCluster {
    if (-not $ClusterId) { throw 'destroy 需要 -ClusterId。' }
    $nodes = Invoke-AliyunJson @('cs', 'DescribeClusterNodes', '--region', $RegionId, '--ClusterId', $ClusterId)
    $nodeNames = @($nodes.nodes | ForEach-Object {
        $nameProperty = $_.PSObject.Properties['name']
        if ($nameProperty) { $nameProperty.Value } else { $_.node_name }
    })
    if ($nodeNames.Count -gt 0) {
        $body = @{ nodes = $nodeNames; drain_node = $false; release_node = $true } | ConvertTo-Json -Compress
        $nodeTask = Invoke-AliyunJson @('cs', 'DeleteClusterNodes', '--region', $RegionId, '--ClusterId', $ClusterId, '--body', $body)
        Wait-AckTask $nodeTask.task_id
    }
    $deleteTask = Invoke-AliyunJson @('cs', 'DeleteCluster', '--region', $RegionId, '--ClusterId', $ClusterId,
        '--keep_slb', 'false', '--retain_all_resources', 'false', '--retain_resources', '[]')
    Wait-AckTask $deleteTask.task_id
    Write-Host "ACK 删除任务已提交: $ClusterId"
}

if ($Action -in @('create', 'all')) {
    New-AckCluster
    Write-Kubeconfig
}
if ($Action -in @('test', 'all')) {
    if (-not $ClusterId) { throw 'test 需要 -ClusterId，或使用 -Action all 自动创建。' }
    if (-not $env:KUBECONFIG -or -not (Test-Path -LiteralPath $env:KUBECONFIG)) { Write-Kubeconfig }
    Run-Test
}
if ($Action -eq 'status') {
    if (-not $ClusterId) { throw 'status 需要 -ClusterId。' }
    Get-Cluster $ClusterId | ConvertTo-Json -Depth 8
    Invoke-Kubectl @('get', 'brewfscluster,brewfsmount,pods', '-A')
}
if ($Action -eq 'destroy' -or ($Action -eq 'all' -and -not $NoCleanup)) {
    if ($env:KUBECONFIG -and (Test-Path -LiteralPath $env:KUBECONFIG)) {
        Invoke-Optional $Kubectl @('delete', 'brewfsmount', 'demo', '-n', $Namespace, '--ignore-not-found') | Out-Null
        Invoke-Optional $Kubectl @('delete', 'brewfscluster', 'demo', '-n', $Namespace, '--ignore-not-found') | Out-Null
    }
    Remove-AckCluster
}
