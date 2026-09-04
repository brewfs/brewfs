[CmdletBinding()]
param(
    [ValidateSet('run', 'create', 'status', 'destroy')]
    [string]$Action = 'run',
    [string]$InstanceId,
    [string]$RegionId = 'ap-northeast-2',
    [string]$ZoneId = 'ap-northeast-2a',
    [string]$VSwitchId,
    [string]$SecurityGroupId,
    [string]$InstanceName,
    [string]$InstanceType = 'ecs.u1-c1m2.2xlarge',
    [string]$ImageId = 'ubuntu_24_04_x64_20G_alibase_20260522.vhd',
    [ValidateSet('redis', 'tikv')]
    [string]$Backend = 'redis',
    [ValidateSet('s3', 'local-fs')]
    [string]$DataBackend = 's3',
    [string]$PerfTools = 'fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest',
    [string]$Repository = 'https://github.com/brewfs/brewfs.git',
    [string]$Ref = 'main',
    [string]$AutoReleaseMinutes = '240',
    [switch]$RunBench,
    [switch]$KeepInstance,
    [switch]$NoCleanup
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$script:CreatedInstance = $false

function Resolve-Executable([string]$Name, [string[]]$Candidates = @()) {
    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($command) { return $command.Source }
    foreach ($candidate in $Candidates) {
        if (Test-Path -LiteralPath $candidate) { return $candidate }
    }
    throw "找不到 $Name，请先安装并加入 PATH。"
}

$aliyunCandidates = @()
if ($env:LOCALAPPDATA) { $aliyunCandidates += (Join-Path $env:LOCALAPPDATA 'AliyunCLI\aliyun.exe') }
$Aliyun = Resolve-Executable 'aliyun' $aliyunCandidates

function Invoke-Checked([string]$File, [string[]]$Arguments) {
    $output = & $File @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "命令失败: $File $($Arguments -join ' ')`n$($output -join [Environment]::NewLine)"
    }
    return $output
}

function Invoke-AliyunJson([string[]]$Arguments) {
    $output = Invoke-Checked $Aliyun $Arguments
    return ($output -join [Environment]::NewLine | ConvertFrom-Json)
}

function Wait-Until([scriptblock]$Condition, [string]$Description, [int]$TimeoutSeconds = 900) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try { if (& $Condition) { return } } catch { }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)
    throw "等待超时: $Description"
}

function Quote-Bash([string]$Value) {
    $replacement = "'" + '"' + "'" + '"' + "'"
    return "'" + $Value.Replace("'", $replacement) + "'"
}

function New-EcsInstance {
    if (-not $VSwitchId -or -not $SecurityGroupId) {
        throw '创建 ECS 需要 -VSwitchId 和 -SecurityGroupId。为避免误改账号网络，脚本不自动创建 VPC。'
    }
    if (-not $script:InstanceName) {
        $script:InstanceName = 'brewfs-perf-{0}' -f (Get-Date -Format 'yyyyMMdd-HHmmss')
    }
    $release = (Get-Date).ToUniversalTime().AddMinutes([int]$AutoReleaseMinutes).ToString('yyyy-MM-ddTHH:mm:ssZ')
    $clientToken = [Guid]::NewGuid().ToString('N')
    $runArgs = @(
        'ecs', 'RunInstances', '--region', $RegionId,
        '--ImageId', $ImageId, '--InstanceType', $InstanceType,
        '--VSwitchId', $VSwitchId, '--SecurityGroupId', $SecurityGroupId,
        '--ZoneId', $ZoneId, '--Amount', '1', '--InstanceName', $InstanceName,
        '--ClientToken', $clientToken,
        '--InstanceChargeType', 'PostPaid', '--InternetChargeType', 'PayByTraffic',
        '--InternetMaxBandwidthOut', '20', '--AutoReleaseTime', $release,
        '--SystemDisk.Category', 'cloud_essd', '--SystemDisk.Size', '80',
        '--SystemDisk.PerformanceLevel', 'PL1',
        '--Tag.1.Key', 'brewfs-test', '--Tag.1.Value', $InstanceName
    )
    try {
        $result = Invoke-AliyunJson $runArgs
    } catch {
        if ($_.Exception.Message -notmatch 'EOF|timeout|timed out') { throw }
        Write-Warning 'RunInstances 返回网络 EOF，使用同一 ClientToken 重试。'
        Start-Sleep -Seconds 5
        $result = Invoke-AliyunJson $runArgs
    }
    $script:InstanceId = @($result.InstanceIdSets.InstanceIdSet)[0]
    if (-not $InstanceId) { throw 'RunInstances 未返回 InstanceId。' }
    $script:CreatedInstance = $true
    Write-Host "ECS 创建成功: $InstanceId"
    Wait-Until {
        $instance = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', "[`"$InstanceId`"]")
        $state = @($instance.Instances.Instance)[0].Status
        Write-Host "  ECS state=$state"
        $state -eq 'Running'
    } 'ECS 启动' 900
}

function Get-RemoteCommand {
    $runner = if ($Backend -eq 'redis') { 'docker/compose-xfstests/run_redis_perf.sh' } else { 'docker/compose-xfstests/run_tikv_perf.sh' }
    $backendArgs = if ($DataBackend -eq 's3') { '--s3' } else { '--local-fs' }
    $benchArg = if ($RunBench) { '--brewfs-bench' } else { '' }
    $remote = @'
#!/usr/bin/env bash
set -Eeuo pipefail
export DEBIAN_FRONTEND=noninteractive
WORK=/opt/brewfs-perf
REPO=__REPO__
REF=__REF__
TOOLS=__TOOLS__
RUNNER=__RUNNER__
DATA_ARGS=__DATA_ARGS__
BENCH_ARGS=__BENCH_ARGS__

apt-get update -qq
apt-get install -y -qq git curl docker.io docker-compose-v2 protobuf-compiler \
  || apt-get install -y -qq git curl docker.io docker-compose-plugin protobuf-compiler
systemctl enable --now docker

# The distro Cargo on the supported Ubuntu image may predate Rust 2024
# edition support.  Install a current stable toolchain with rustup so the
# checked-out BrewFS revision is built with the same language features as CI.
if ! command -v rustup >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
fi
export PATH="/root/.cargo/bin:${PATH}"
rustup toolchain install stable --profile minimal
rustup default stable
mkdir -p "$WORK"
if [[ ! -d "$WORK/.git" ]]; then
  git clone --depth=1 --branch "$REF" "$REPO" "$WORK"
else
  git -C "$WORK" fetch --depth=1 origin "$REF"
  git -C "$WORK" reset --hard FETCH_HEAD
fi
cd "$WORK"
git rev-parse HEAD
export PERF_TOOLS="$TOOLS"
export RUST_LOG="${RUST_LOG:-warn}"
export COMPOSE_PROJECT_NAME="brewfs-$(date +%s)"

args=("$DATA_ARGS")
if [[ -n "$BENCH_ARGS" ]]; then args+=("$BENCH_ARGS"); fi
bash "$RUNNER" --tools "$TOOLS" "${args[@]}"

echo '--- latest perf summary ---'
find docker/compose-xfstests/artifacts -name perf-summary.tsv -type f -printf '%T@ %p\n' 2>/dev/null \
  | sort -nr | awk 'NR == 1 {print $2}' \
  | xargs -r tail -n 80
'@
    $remote = $remote.Replace('__REPO__', (Quote-Bash $Repository))
    $remote = $remote.Replace('__REF__', (Quote-Bash $Ref))
    $remote = $remote.Replace('__TOOLS__', (Quote-Bash $PerfTools))
    $remote = $remote.Replace('__RUNNER__', (Quote-Bash $runner))
    $remote = $remote.Replace('__DATA_ARGS__', (Quote-Bash $backendArgs))
    $remote = $remote.Replace('__BENCH_ARGS__', (Quote-Bash $benchArg))
    return $remote
}

function Invoke-PerfOnEcs {
    $content = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes((Get-RemoteCommand)))
    $run = Invoke-AliyunJson @(
        'ecs', 'RunCommand', '--region', $RegionId, '--Type', 'RunShellScript',
        '--InstanceId.1', $InstanceId, '--CommandContent', $content,
        '--ContentEncoding', 'Base64', '--Timeout', '172800',
        '--KeepCommand', 'false', '--Name', "brewfs-perf-$Backend"
    )
    $invokeId = $run.InvokeId
    if (-not $invokeId) { throw 'RunCommand 未返回 InvokeId。请确认 ECS Cloud Assistant Agent 已在线。' }
    Write-Host "远程性能测试已提交: $invokeId"
    $script:PerfFailure = $null
    Wait-Until {
        $result = Invoke-AliyunJson @('ecs', 'DescribeInvocationResults', '--region', $RegionId, '--InvokeId', $invokeId)
        $item = @($result.Invocation.InvocationResults.InvocationResult)[0]
        if (-not $item) { return $false }
        Write-Host "  invocation status=$($item.InvocationStatus)"
        if ($item.InvocationStatus -in @('Success', 'Failed', 'Stopped', 'Error', 'Terminated')) {
            if ($item.Output) {
                $text = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($item.Output))
                Write-Output $text
            }
            if ($item.InvocationStatus -ne 'Success') { $script:PerfFailure = $item.ErrorInfo }
            return $true
        }
        return $false
    } '远程性能测试完成' 172800
    if ($script:PerfFailure) { throw "远程测试失败: $script:PerfFailure" }
}

function Remove-EcsInstance {
    if (-not $InstanceId) { throw 'destroy 需要 -InstanceId。' }
    try {
        Invoke-AliyunJson @('ecs', 'DeleteInstance', '--region', $RegionId, '--InstanceId', $InstanceId, '--Force', 'true') | Out-Null
    } catch {
        # 新实例可能仍处于初始化锁定状态，先停止后再释放。
        Invoke-AliyunJson @('ecs', 'StopInstance', '--region', $RegionId, '--InstanceId', $InstanceId, '--ForceStop', 'true') | Out-Null
        Wait-Until {
            $instance = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', "[`"$InstanceId`"]")
            @($instance.Instances.Instance)[0].Status -eq 'Stopped'
        } "ECS $InstanceId 停止" 300
        Invoke-AliyunJson @('ecs', 'DeleteInstance', '--region', $RegionId, '--InstanceId', $InstanceId) | Out-Null
    }
    Write-Host "ECS 删除任务已提交: $InstanceId"
}

try {
    if ($Action -in @('run', 'create') -and -not $InstanceId) {
        New-EcsInstance
    }
    if ($Action -eq 'status') {
        if (-not $InstanceId) { throw 'status 需要 -InstanceId。' }
        Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', "[`"$InstanceId`"]") | ConvertTo-Json -Depth 8
    } elseif ($Action -in @('run', 'create')) {
        if ($Action -eq 'run') { Invoke-PerfOnEcs }
    } elseif ($Action -eq 'destroy') {
        Remove-EcsInstance
    }
} finally {
    if ($Action -eq 'run' -and $script:CreatedInstance -and -not $KeepInstance -and -not $NoCleanup) {
        try { Remove-EcsInstance } catch { Write-Warning "ECS 自动清理失败: $($_.Exception.Message)" }
    }
}
