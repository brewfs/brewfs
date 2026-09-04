[CmdletBinding()]
param(
    [ValidateSet('test', 'status', 'destroy')]
    [string]$Action = 'test',
    [string]$KubeconfigPath,
    [string]$Namespace = 'brewfs-perf',
    [string]$RegistryImage = 'ghcr.io/ivanbeethoven/brewfs-perf',
    [string]$ImageTag = ('aliyun-{0}' -f (Get-Date -Format 'yyyyMMdd-HHmmss')),
    [string]$GhcrToken,
    [ValidateSet('redis', 'tikv')]
    [string]$Backend = 'redis',
    [ValidateSet('s3', 'local-fs')]
    [string]$DataBackend = 'local-fs',
    [string]$PerfTools = 'dirstress dirperf metaperf looptest',
    [switch]$KeepJob
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$Image = "$RegistryImage`:$ImageTag"
$JobName = "brewfs-perf-$ImageTag".ToLowerInvariant()

function Resolve-Tool([string]$Name, [string[]]$Candidates = @()) {
    $cmd = Get-Command $Name -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    foreach ($candidate in $Candidates) {
        if (Test-Path -LiteralPath $candidate) { return $candidate }
    }
    throw "找不到 $Name，请先安装并加入 PATH。"
}
function Invoke-Checked([string]$File, [string[]]$Args) {
    $out = & $File @Args 2>&1
    if ($LASTEXITCODE -ne 0) { throw "$File 失败:`n$($out -join [Environment]::NewLine)" }
    return $out
}

$Docker = Resolve-Tool 'docker' @('C:\Program Files\Docker\Docker\resources\bin\docker.exe')
$Kubectl = Resolve-Tool 'kubectl'
if ($KubeconfigPath) { $env:KUBECONFIG = (Resolve-Path $KubeconfigPath).Path }

function Ensure-Image {
    Push-Location $RepoRoot
    try {
        Invoke-Checked $Docker @('build', '-f', 'docker/compose-xfstests/aliyun/Dockerfile.perf-local', '-t', $Image, '.') | Out-Host
        if ($GhcrToken) {
            $GhcrToken | & $Docker login ghcr.io --username Ivanbeethoven --password-stdin 2>&1 | Out-Host
            if ($LASTEXITCODE -ne 0) { throw 'docker login ghcr.io 失败。' }
        }
        Invoke-Checked $Docker @('push', $Image) | Out-Host
    } finally { Pop-Location }
}

function Render-Manifests {
    $dataEnv = if ($DataBackend -eq 's3') { 's3' } else { 'local-fs' }
    $initContainers = if ($DataBackend -eq 's3') {
        @"
      initContainers:
      - name: rustfs-init
        image: amazon/aws-cli:2
        command: ["sh", "-ec"]
        args:
        - |
          mkdir -p /root/.aws
          printf '[default]\nregion = us-east-1\ns3 =\n  addressing_style = path\n' > /root/.aws/config
          until aws --endpoint-url http://rustfs:9000 s3api create-bucket --bucket brewfs-data >/dev/null 2>&1 || aws --endpoint-url http://rustfs:9000 s3api head-bucket --bucket brewfs-data >/dev/null 2>&1; do
            sleep 2
          done
        env:
        - { name: AWS_ACCESS_KEY_ID, value: rustfsadmin }
        - { name: AWS_SECRET_ACCESS_KEY, value: rustfsadmin }
        - { name: AWS_DEFAULT_REGION, value: us-east-1 }
        - { name: AWS_EC2_METADATA_DISABLED, value: "true" }
"@
    } else { '' }
    $storage = if ($DataBackend -eq 's3') {
        @"
apiVersion: apps/v1
kind: Deployment
metadata: { name: rustfs, namespace: $Namespace }
spec:
  selector: { matchLabels: { app: rustfs } }
  template:
    metadata: { labels: { app: rustfs } }
    spec:
      containers:
      - name: rustfs
        image: rustfs/rustfs:latest
        args: ["--address", ":9000", "--console-enable", "--server-domains", "rustfs", "--access-key", "rustfsadmin", "--secret-key", "rustfsadmin", "/data"]
        readinessProbe: { tcpSocket: { port: 9000 }, initialDelaySeconds: 5 }
---
apiVersion: v1
kind: Service
metadata: { name: rustfs, namespace: $Namespace }
spec: { selector: { app: rustfs }, ports: [{ port: 9000 }] }
"@
    } else { '' }
    $meta = if ($Backend -eq 'redis') {
        @" 
apiVersion: apps/v1
kind: Deployment
metadata: { name: redis, namespace: $Namespace }
spec:
  selector: { matchLabels: { app: redis } }
  template:
    metadata: { labels: { app: redis } }
    spec:
      containers:
      - name: redis
        image: redis:7.2-alpine
        args: ["redis-server", "--save", "", "--appendonly", "yes", "--appendfsync", "everysec"]
        readinessProbe: { exec: { command: ["redis-cli", "ping"] } }
---
apiVersion: v1
kind: Service
metadata: { name: redis, namespace: $Namespace }
spec: { selector: { app: redis }, ports: [{ port: 6379 }] }
"@ 
    } else {
        @" 
apiVersion: apps/v1
kind: Deployment
metadata: { name: pd, namespace: $Namespace }
spec:
  selector: { matchLabels: { app: pd } }
  template:
    metadata: { labels: { app: pd } }
    spec:
      containers: [{ name: pd, image: pingcap/pd:v8.5.0, args: ["--name=pd", "--data-dir=/data", "--client-urls=http://0.0.0.0:2379", "--advertise-client-urls=http://pd:2379"] }]
---
apiVersion: v1
kind: Service
metadata: { name: pd, namespace: $Namespace }
spec: { selector: { app: pd }, ports: [{ port: 2379 }] }
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: tikv, namespace: $Namespace }
spec:
  selector: { matchLabels: { app: tikv } }
  template:
    metadata: { labels: { app: tikv } }
    spec:
      containers: [{ name: tikv, image: pingcap/tikv:v8.5.0, args: ["--addr=0.0.0.0:20160", "--advertise-addr=tikv:20160", "--pd=pd:2379"] }]
"@
    }
    $job = @"
apiVersion: batch/v1
kind: Job
metadata: { name: $JobName, namespace: $Namespace, labels: { app: brewfs-perf } }
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 86400
  template:
    metadata: { labels: { app: brewfs-perf, job: $JobName } }
    spec:
      restartPolicy: Never
$initContainers      containers:
      - name: perf
        image: $Image
        imagePullPolicy: Always
        env:
        - { name: PERF_TOOLS, value: '$PerfTools' }
        - { name: BREWFS_DATA_BACKEND, value: '$dataEnv' }
        - { name: BREWFS_META_BACKEND, value: '$Backend' }
        - { name: BREWFS_META_URL, value: 'redis://redis:6379/0' }
        - { name: BREWFS_META_TIKV_PD_ENDPOINTS, value: 'pd:2379' }
        - { name: BREWFS_ARTIFACT_ROOT, value: /artifacts }
        - { name: BREWFS_ARTIFACT_DIR, value: /artifacts/$JobName }
        - { name: BREWFS_S3_ENDPOINT, value: http://rustfs:9000 }
        - { name: BREWFS_S3_BUCKET, value: brewfs-data }
        - { name: BREWFS_S3_FORCE_PATH_STYLE, value: "true" }
        - { name: BREWFS_S3_REGION, value: us-east-1 }
        - { name: AWS_ACCESS_KEY_ID, value: rustfsadmin }
        - { name: AWS_SECRET_ACCESS_KEY, value: rustfsadmin }
        securityContext:
          privileged: true
          capabilities: { add: [SYS_ADMIN] }
          appArmorProfile: { type: Unconfined }
        volumeMounts:
        - { name: fuse, mountPath: /dev/fuse }
        - { name: artifacts, mountPath: /artifacts }
        - { name: state, mountPath: /var/lib/brewfs }
      volumes:
      - { name: fuse, hostPath: { path: /dev/fuse, type: CharDevice } }
      - { name: artifacts, emptyDir: {} }
      - { name: state, emptyDir: {} }
"@
    $parts = @($meta.TrimEnd())
    if ($storage) { $parts += $storage.TrimEnd() }
    $parts += $job.TrimStart()
    return ($parts -join "`n---`n")
}

function Apply-Test {
    Invoke-Checked $Kubectl @('create', 'namespace', $Namespace, '--dry-run=client', '-o', 'yaml') | & $Kubectl apply -f - | Out-Host
    $yaml = Render-Manifests
    $yaml | & $Kubectl apply -f - 2>&1 | Out-Host
    if ($LASTEXITCODE -ne 0) { throw 'Kubernetes manifest apply 失败。' }
    Invoke-Checked $Kubectl @('wait', '--for=condition=available', "deployment/$($Backend -eq 'redis' ? 'redis' : 'pd')", '-n', $Namespace, '--timeout=10m') | Out-Host
    Invoke-Checked $Kubectl @('wait', '--for=condition=complete', "job/$JobName", '-n', $Namespace, '--timeout=48h') | Out-Host
    $pod = ((Invoke-Checked $Kubectl @('get', 'pod', '-n', $Namespace, '-l', "job-name=$JobName", '-o', 'jsonpath={.items[0].metadata.name}') -join '')).Trim()
    New-Item -ItemType Directory -Force -Path (Join-Path $RepoRoot 'docker\compose-xfstests\artifacts') | Out-Null
    Invoke-Checked $Kubectl @('cp', "${Namespace}/${pod}:/artifacts/$JobName", (Join-Path $RepoRoot 'docker\compose-xfstests\artifacts\')) | Out-Host
    if (-not $KeepJob) { Invoke-Checked $Kubectl @('delete', 'job', $JobName, '-n', $Namespace, '--ignore-not-found') | Out-Host }
}

switch ($Action) {
    'test' { Ensure-Image; Apply-Test }
    'status' { Invoke-Checked $Kubectl @('get', 'job', $JobName, '-n', $Namespace, '-o', 'wide') }
    'destroy' { Invoke-Checked $Kubectl @('delete', 'job', $JobName, '-n', $Namespace, '--ignore-not-found') }
}
