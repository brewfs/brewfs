[CmdletBinding()]
param(
    [ValidateSet('prepare', 'status', 'delete')]
    [string]$Action = 'prepare',
    [string]$ImageId,
    [string]$ImageName = 'brewfs-perf-base',
    [string]$RegionId = 'cn-hangzhou',
    [string]$ZoneId = 'cn-hangzhou-h',
    [string]$VSwitchId = $env:BREWFS_PERF_VSWITCH_ID,
    [string]$SecurityGroupId = $env:BREWFS_PERF_SECURITY_GROUP_ID,
    [string]$BaseImageId = 'ubuntu_24_04_x64_20G_alibase_20260522.vhd',
    [string]$InstanceType = 'ecs.u1-c1m2.2xlarge',
    [string]$Repository = 'https://github.com/brewfs/brewfs.git',
    [string]$Ref = 'main',
    [string]$DockerRegistryMirror = 'https://docker.m.daocloud.io',
    [bool]$InstallIoPagesKernel = $true,
    [string]$FuseKernelSourceUrl = 'https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.8.12.tar.xz',
    [ValidateRange(30, 1440)]
    [int]$AutoReleaseMinutes = 180
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$aliyunCandidates = @()
if ($env:LOCALAPPDATA) { $aliyunCandidates += (Join-Path $env:LOCALAPPDATA 'AliyunCLI\aliyun.exe') }
if ($env:ALIBABA_CLOUD_CLI_PATH) { $aliyunCandidates += $env:ALIBABA_CLOUD_CLI_PATH }
if ($env:LOCALAPPDATA) {
    $wingetPackages = Join-Path $env:LOCALAPPDATA 'Microsoft\WinGet\Packages'
    if (Test-Path -LiteralPath $wingetPackages) {
        $aliyunCandidates += @(Get-ChildItem -LiteralPath $wingetPackages -Filter 'aliyun.exe' -File -Recurse -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty FullName)
    }
}
$script:Aliyun = $null
$script:BuilderInstanceId = $null

function Resolve-Executable([string]$Name, [string[]]$Candidates = @()) {
    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($command) { return $command.Source }
    foreach ($candidate in $Candidates) {
        if (Test-Path -LiteralPath $candidate) { return $candidate }
    }
    throw "找不到 $Name，请先安装并加入 PATH。"
}

function Invoke-Checked([string]$File, [string[]]$Arguments) {
    $startInfo = [Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $File
    $startInfo.UseShellExecute = $false
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    # The aliyun CLI writes UTF-8 JSON. Without an explicit encoding a child
    # process decodes its stdout with the console code page, which corrupts
    # non-ASCII fields (for example ECS "OSName" contains 位) and makes
    # ConvertFrom-Json fail with an unhelpful parse error.
    $startInfo.StandardOutputEncoding = [Text.Encoding]::UTF8
    $startInfo.StandardErrorEncoding = [Text.Encoding]::UTF8
    foreach ($argument in $Arguments) { [void]$startInfo.ArgumentList.Add([string]$argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    if (-not $process.Start()) { throw "无法启动命令: $File" }
    $stdoutTask = $process.StandardOutput.ReadToEndAsync()
    $stderrTask = $process.StandardError.ReadToEndAsync()
    $process.WaitForExit()
    $stdout = $stdoutTask.GetAwaiter().GetResult()
    $stderr = $stderrTask.GetAwaiter().GetResult()
    $output = @($stdout, $stderr) | Where-Object { $_ }
    if ($process.ExitCode -ne 0) {
        throw "命令失败: $File $($Arguments -join ' ')`n$($output -join [Environment]::NewLine)"
    }
    return ($output -join [Environment]::NewLine)
}

function Invoke-AliyunJson([string[]]$Arguments) {
    if (-not $script:Aliyun) { $script:Aliyun = Resolve-Executable 'aliyun' $aliyunCandidates }
    $output = Invoke-Checked $script:Aliyun $Arguments
    return ($output -join [Environment]::NewLine | ConvertFrom-Json)
}

function Invoke-AliyunJsonWithRetry([string[]]$Arguments, [int]$Attempts = 5) {
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            return Invoke-AliyunJson $Arguments
        } catch {
            $message = $_.Exception.Message
            if ($attempt -eq $Attempts -or $message -notmatch 'timeout|timed out|EOF|dial tcp|connection reset|temporarily unavailable') {
                throw
            }
            Write-Warning "Aliyun API transient failure; retrying ($attempt/$Attempts): $message"
            Start-Sleep -Seconds ([Math]::Min(30, $attempt * 5))
        }
    }
    throw 'Aliyun API retry loop exhausted.'
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

function Get-ImageBuildCommand {
    $repo = Quote-Bash $Repository
    $ref = Quote-Bash $Ref
    $mirror = if ($DockerRegistryMirror) { Quote-Bash $DockerRegistryMirror } else { "''" }
    $nativeRunnerPath = Join-Path $PSScriptRoot 'run_native_perf.sh'
    if (-not (Test-Path -LiteralPath $nativeRunnerPath -PathType Leaf)) {
        throw "native runner is missing: $nativeRunnerPath"
    }
    # Ship the runner as LF: this script runs on Windows, where core.autocrlf
    # hands back CRLF text, and bash on the VM rejects the first line otherwise.
    $nativeRunnerText = (Get-Content -LiteralPath $nativeRunnerPath -Raw) -replace "`r`n", "`n"
    $nativeRunnerBytes = [Text.Encoding]::UTF8.GetBytes(($nativeRunnerText -replace "`r", "`n"))
$nativeRunnerStream = [IO.MemoryStream]::new()
$nativeRunnerGzip = [IO.Compression.GZipStream]::new($nativeRunnerStream, [IO.Compression.CompressionMode]::Compress)
$nativeRunnerGzip.Write($nativeRunnerBytes, 0, $nativeRunnerBytes.Length)
$nativeRunnerGzip.Dispose()
$nativeRunnerB64 = [Convert]::ToBase64String($nativeRunnerStream.ToArray())
$nativeRunnerStream.Dispose()

    $command = @"
#!/usr/bin/env bash
set -Eeuo pipefail
export DEBIAN_FRONTEND=noninteractive
REPO=$repo
REF=$ref
REGISTRY_MIRROR=$mirror
ROOT=/opt/brewfs-perf
SOURCE="`$ROOT/source/brewfs"

wait_for_dpkg_lock() {
  exec 9>/var/lib/dpkg/lock-frontend
  for _ in `$(seq 1 180); do
    if flock -n 9; then
      flock -u 9
      exec 9>&-
      return 0
    fi
    echo 'waiting for another dpkg/apt process to release the package lock'
    sleep 5
  done
  exec 9>&-
  echo 'timed out waiting for the dpkg package lock' >&2
  exit 1
}

wait_for_dpkg_lock
dpkg --configure -a || true

apt-get update -qq
# Do not upgrade packages already present in the base image. In particular,
# upgrading the Cloud Assistant/runtime dependency chain can restart the
# agent that is executing this command and leave the image builder stranded.
wait_for_dpkg_lock
apt-get install --no-upgrade -y -qq git curl zip jq ca-certificates unzip tar gzip xz-utils \
  bash build-essential protobuf-compiler util-linux e2fsprogs fuse3 libfuse3-3 \
  xfsprogs fio stress-ng redis-tools \
  acl attr bc dbench dump gawk liburing2 libuuid1 lvm2 make perl psmisc \
  python3 quota sed strace sudo uuid-runtime xfsdump exfatprogs f2fs-tools udftools

mkdir -p /etc/fuse
grep -q '^user_allow_other$' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >> /etc/fuse.conf

curl --fail --location --retry 5 --retry-all-errors \
  --connect-timeout 20 --max-time 1800 \
  https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip --output /tmp/awscliv2.zip
rm -rf /tmp/aws
unzip -q /tmp/awscliv2.zip -d /tmp
/tmp/aws/install --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli --update
command -v aws >/dev/null 2>&1 || { echo 'native AWS CLI installation failed' >&2; exit 1; }

# OSS and Redis/Tair are managed services. The VM image deliberately does not
# install RustFS or a Redis server; the native runner only validates and uses
# the endpoints supplied for each test run.

JUICEFS_VERSION=1.4.1
juicefs_asset="juicefs-`$JUICEFS_VERSION-linux-amd64.tar.gz"
juicefs_urls="https://d.juicefs.com/juicefs/releases/download/v`$JUICEFS_VERSION/`$juicefs_asset"
juicefs_urls+=" https://gh-proxy.com/https://github.com/juicedata/juicefs/releases/download/v`$JUICEFS_VERSION/`$juicefs_asset"
juicefs_urls+=" https://ghfast.top/https://github.com/juicedata/juicefs/releases/download/v`$JUICEFS_VERSION/`$juicefs_asset"
juicefs_downloaded=0
for juicefs_url in `$juicefs_urls; do
  echo "downloading `$juicefs_url"
  rm -f /tmp/juicefs.tar.gz
  if curl --fail --location --retry 2 --retry-all-errors \
      --connect-timeout 20 --max-time 900 "`$juicefs_url" --output /tmp/juicefs.tar.gz && \
      tar -tzf /tmp/juicefs.tar.gz >/dev/null 2>&1; then
    juicefs_downloaded=1
    break
  fi
done
[[ "`$juicefs_downloaded" == 1 ]] || { echo 'unable to download JuiceFS release archive' >&2; exit 1; }
tar -xzf /tmp/juicefs.tar.gz -C /tmp
install -m 0755 /tmp/juicefs /usr/local/bin/juicefs
command -v juicefs >/dev/null 2>&1 || { echo 'native JuiceFS installation failed' >&2; exit 1; }

git config --global http.version HTTP/1.1
git config --global http.lowSpeedLimit 0
git config --global http.lowSpeedTime 300
clone_urls="`$REPO"
if [[ "`$REPO" == https://github.com/* ]]; then
  clone_urls+=" https://gh-proxy.com/`$REPO"
  clone_urls+=" https://ghfast.top/`$REPO"
fi
fetched=0
for clone_url in `$clone_urls; do
  for attempt in `$(seq 1 3); do
    echo "cloning source from `$clone_url (attempt `$attempt/3)"
    rm -rf "`$SOURCE"
    mkdir -p "`$(dirname "`$SOURCE")"
    if git clone --depth=1 --branch "`$REF" "`$clone_url" "`$SOURCE"; then
      fetched=1
      break 2
    fi
    sleep `$((attempt * 10))
  done
done
[[ "`$fetched" == 1 ]] || { echo 'unable to fetch BrewFS source after retries' >&2; exit 1; }
git -C "`$SOURCE" checkout --force --detach HEAD
git config --global --add safe.directory "`$SOURCE"

if [[ '__INSTALL_IO_PAGES_KERNEL__' == 1 ]]; then
  kernel_installer="`$SOURCE/docker/compose-xfstests/aliyun/install_fuse_io_pages_kernel.sh"
  [[ -x "`$kernel_installer" ]] || { echo "FUSE io_pages installer is missing: `$kernel_installer" >&2; exit 1; }
  BREWFS_FUSE_KERNEL_SOURCE_URL='__FUSE_KERNEL_SOURCE_URL__' \
    BREWFS_FUSE_KERNEL_JOBS="`$(nproc)" "`$kernel_installer"
fi

[[ -s "`$SOURCE/docker/compose-xfstests/run_redis_perf.sh" ]] || { echo 'BrewFS source checkout is incomplete: Redis runner missing' >&2; exit 1; }
[[ -s "`$SOURCE/docker/compose-xfstests/run_juicefs_perf.sh" ]] || { echo 'BrewFS source checkout is incomplete: JuiceFS runner missing' >&2; exit 1; }
xfstests_archive="`$SOURCE/tests/scripts/xfstests-prebuilt/xfstests-prebuilt.tar.gz"
if ! gzip -t "`$xfstests_archive" >/dev/null 2>&1; then
  # GitHub clones without Git LFS leave a pointer file. Fetch the public LFS
  # object directly so image preparation does not depend on git-lfs on ECS.
  repo_path="`$(printf '%s' \"`$REPO\" | sed -e 's#^https://github.com/##' -e 's#\\.git##')"
  source_commit="`$(git -C "`$SOURCE" rev-parse HEAD)"
  for archive_url in \
    "https://media.githubusercontent.com/media/`$repo_path/`$source_commit/tests/scripts/xfstests-prebuilt/xfstests-prebuilt.tar.gz" \
    "https://media.githubusercontent.com/media/`$repo_path/`$REF/tests/scripts/xfstests-prebuilt/xfstests-prebuilt.tar.gz" \
    "https://raw.githubusercontent.com/`$repo_path/`$REF/tests/scripts/xfstests-prebuilt/xfstests-prebuilt.tar.gz"; do
    echo "fetching prebuilt xfstests archive from `$archive_url"
    rm -f /tmp/xfstests-prebuilt.tar.gz
    if curl --fail --location --retry 3 --retry-all-errors --connect-timeout 20 \
      --max-time 900 "`$archive_url" --output /tmp/xfstests-prebuilt.tar.gz && \
      gzip -t /tmp/xfstests-prebuilt.tar.gz >/dev/null 2>&1; then
      install -m 0644 /tmp/xfstests-prebuilt.tar.gz "`$xfstests_archive"
      break
    fi
  done
  rm -f /tmp/xfstests-prebuilt.tar.gz
fi
if gzip -t "`$xfstests_archive" >/dev/null 2>&1; then
  rm -rf /opt/xfstests-dev
  tar -xzf "`$xfstests_archive" -C /opt --transform 's|^xfstests|xfstests-dev|'
  chmod +x /opt/xfstests-dev/check /opt/xfstests-dev/src/* 2>/dev/null || true
else
  echo 'xfstests prebuilt archive is missing or invalid' >&2
  exit 1
fi
install -m 0755 "`$SOURCE/docker/compose-xfstests/run_perf_in_container.sh" /usr/local/bin/run_perf_in_container.sh
install -m 0755 "`$SOURCE/docker/compose-xfstests/run_juicefs_perf_in_container.sh" /usr/local/bin/run_juicefs_perf_in_container.sh
install -m 0755 "`$SOURCE/docker/compose-xfstests/perf_metadata_fallback.py" /usr/local/bin/perf_metadata_fallback.py
install -m 0755 "`$SOURCE/tools/perf/perf_manifest.py" /usr/local/bin/perf_manifest.py
mkdir -p "`$ROOT/native"
printf '%s' '__NATIVE_RUNNER_B64__' | base64 -d | gzip -d > "`$ROOT/native/run_native_perf.sh"
chmod 0755 "`$ROOT/native/run_native_perf.sh"
mkdir -p "`$ROOT"/artifacts "`$ROOT"/cache "`$ROOT"/bin
git -C "`$SOURCE" rev-parse HEAD | tee "`$ROOT/image-source-commit"
[[ -s "`$ROOT/image-source-commit" ]] || { echo 'image-source-commit was not written' >&2; exit 1; }
cat > "`$ROOT/image-manifest.json" <<EOF
{
  "repository": "`$REPO",
  "ref": "`$REF",
  "sourceCommit": "`$(git -C "`$SOURCE" rev-parse HEAD)",
  "preparedAt": "`$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "dockerRegistryMirror": "`$REGISTRY_MIRROR",
  "fuseIoPagesPatch": "__FUSE_IO_PAGES_PATCH__",
  "fuseKernelSourceUrl": "__FUSE_KERNEL_SOURCE_URL__"
}
EOF
chmod 0644 "`$ROOT/image-manifest.json" "`$ROOT/image-source-commit"
sync
sleep 10
echo 'BrewFS performance base image preparation complete.'
"@
    $command = $command.Replace('__NATIVE_RUNNER_B64__', $nativeRunnerB64)
    $command = $command.Replace('__INSTALL_IO_PAGES_KERNEL__', $(if ($InstallIoPagesKernel) { '1' } else { '0' }))
    $command = $command.Replace('__FUSE_KERNEL_SOURCE_URL__', (Quote-Bash $FuseKernelSourceUrl).Trim("'"))
    $command = $command.Replace('__FUSE_IO_PAGES_PATCH__', '98b4ca2378e1f6b6c06a74f699623ebecfb3549d')
    return $command
}

function New-BuilderInstance {
    if (-not $VSwitchId -or -not $SecurityGroupId) {
        throw 'prepare 需要 -VSwitchId 和 -SecurityGroupId；脚本不会自动创建 VPC、vSwitch 或安全组。'
    }
    $builderName = "$ImageName-builder-$((Get-Date).ToUniversalTime().ToString('yyyyMMddHHmmss'))"
    $release = (Get-Date).ToUniversalTime().AddMinutes($AutoReleaseMinutes).ToString('yyyy-MM-ddTHH:mm:ssZ')
    $args = @(
        'ecs', 'RunInstances', '--region', $RegionId,
        '--ImageId', $BaseImageId, '--InstanceType', $InstanceType,
        '--VSwitchId', $VSwitchId, '--SecurityGroupId', $SecurityGroupId,
        '--ZoneId', $ZoneId, '--Amount', '1', '--InstanceName', $builderName,
        '--ClientToken', ([Guid]::NewGuid().ToString('N')),
        '--InstanceChargeType', 'PostPaid', '--InternetChargeType', 'PayByTraffic',
        '--InternetMaxBandwidthOut', '20', '--AutoReleaseTime', $release,
        '--SystemDisk.Category', 'cloud_essd', '--SystemDisk.Size', '80',
        '--SystemDisk.PerformanceLevel', 'PL1',
        '--Tag.1.Key', 'brewfs-perf-image-builder', '--Tag.1.Value', $builderName,
        '--Tag.2.Key', 'brewfs-perf-image-name', '--Tag.2.Value', $ImageName
    )
    $result = Invoke-AliyunJson $args
    $script:BuilderInstanceId = @($result.InstanceIdSets.InstanceIdSet)[0]
    if (-not $script:BuilderInstanceId) { throw 'RunInstances 未返回镜像构建实例 ID。' }
    Write-Host "镜像构建 ECS: $($script:BuilderInstanceId)"
    Wait-Until {
        $instance = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', "[`"$($script:BuilderInstanceId)`"]")
        $state = @($instance.Instances.Instance)[0].Status
        Write-Host "  builder state=$state"
        $state -eq 'Running'
    } '镜像构建 ECS 启动' 900
}

function Invoke-BuilderSetup {
    $remoteCommand = (Get-ImageBuildCommand) -replace "`r`n", "`n" -replace "`r", ""
    $content = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($remoteCommand))
    $deadline = (Get-Date).AddSeconds(7200)
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        $run = Invoke-AliyunJson @(
            'ecs', 'RunCommand', '--region', $RegionId, '--Type', 'RunShellScript',
            '--InstanceId.1', $script:BuilderInstanceId, '--CommandContent', $content,
            '--ContentEncoding', 'Base64', '--Timeout', '7200', '--KeepCommand', 'false',
            '--Name', 'brewfs-perf-image-prepare'
        )
        $invokeId = $run.InvokeId
        if (-not $invokeId) { throw 'RunCommand 未返回镜像准备 invocation ID。' }
        Write-Host "镜像准备 invocation: $invokeId (attempt $attempt/3)"
        $terminal = $null
        while ((Get-Date) -lt $deadline) {
            $result = Invoke-AliyunJsonWithRetry @('ecs', 'DescribeInvocationResults', '--region', $RegionId, '--InvokeId', $invokeId)
            $item = @($result.Invocation.InvocationResults.InvocationResult)[0]
            if ($item) {
                Write-Host "  invocation status=$($item.InvocationStatus)"
                if ($item.InvocationStatus -in @('Success', 'Failed', 'Stopped', 'Error', 'Terminated', 'Aborted')) {
                    $terminal = $item
                    if ($item.Output) {
                        [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($item.Output)) | Write-Output
                    }
                    break
                }
            }
            Start-Sleep -Seconds 5
        }
        if (-not $terminal) { throw '等待镜像准备完成超时。' }
        if ($terminal.InvocationStatus -eq 'Success') { return }
        if ($terminal.InvocationStatus -eq 'Terminated' -and
            ($terminal.ErrorCode -eq 'ClientRestarted' -or $terminal.ErrorInfo -match 'service has been restarted')) {
            Write-Warning 'Cloud Assistant 重启，保留镜像构建 ECS 后重试。'
            Start-Sleep -Seconds 15
            continue
        }
        if ($terminal.InvocationStatus -eq 'Aborted') {
            throw "image preparation was aborted: $($terminal.ErrorInfo)"
        }
        throw "镜像准备失败: $($terminal.ErrorInfo)"
    }
    throw '镜像准备重试次数耗尽。'
}

function Remove-BuilderInstance {
    if (-not $script:BuilderInstanceId) { return }
    $id = $script:BuilderInstanceId
    $deadline = (Get-Date).AddMinutes(10)
    try {
        while ((Get-Date) -lt $deadline) {
            $instance = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', "[`"$id`"]")
            $item = @($instance.Instances.Instance)[0]
            if (-not $item) {
                $script:BuilderInstanceId = $null
                Write-Host "镜像构建 ECS 已删除: $id"
                return
            }
            $state = [string]$item.Status
            Write-Host "  builder cleanup state=$state"
            if ($state -in @('Running', 'Starting')) {
                try { Invoke-AliyunJson @('ecs', 'StopInstance', '--region', $RegionId, '--InstanceId', $id, '--ForceStop', 'true') | Out-Null } catch { }
                Start-Sleep -Seconds 10
                continue
            }
            if ($state -eq 'Stopping') {
                Start-Sleep -Seconds 10
                continue
            }
            try {
                Invoke-AliyunJson @('ecs', 'DeleteInstance', '--region', $RegionId, '--InstanceId', $id, '--Force', 'true') | Out-Null
            } catch {
                if ($_.Exception.Message -notmatch 'Initializing|IncorrectInstanceStatus') { throw }
                Start-Sleep -Seconds 15
                continue
            }
            Start-Sleep -Seconds 5
        }
        throw "等待镜像构建 ECS $id 删除超时。"
    } catch {
        Write-Warning "镜像构建 ECS 清理失败，请立即检查 ${id}: $($_.Exception.Message)"
        return
    }
}
function New-PerfImage {
    New-BuilderInstance
    Invoke-BuilderSetup
    Invoke-AliyunJson @('ecs', 'StopInstance', '--region', $RegionId, '--InstanceId', $script:BuilderInstanceId, '--ForceStop', 'true') | Out-Null
    Wait-Until {
        $instance = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', "[`"$($script:BuilderInstanceId)`"]")
        @($instance.Instances.Instance)[0].Status -eq 'Stopped'
    } '镜像构建 ECS 停止' 600
    Start-Sleep -Seconds 10

    $resolvedName = "$ImageName-$((Get-Date).ToUniversalTime().ToString('yyyyMMddHHmmss'))"
    $image = Invoke-AliyunJson @(
        'ecs', 'CreateImage', '--region', $RegionId, '--InstanceId', $script:BuilderInstanceId,
        '--ImageName', $resolvedName,
        '--Description', "BrewFS performance base image; repository=$Repository; ref=$Ref",
        '--ClientToken', ([Guid]::NewGuid().ToString('N')),
        '--Tag.1.Key', 'brewfs-perf-image', '--Tag.1.Value', $ImageName
    )
    $createdImageId = $image.ImageId
    if (-not $createdImageId) { throw 'CreateImage 未返回 ImageId。' }
    Write-Host "自定义镜像创建中: $createdImageId"
    Wait-Until {
        $images = Invoke-AliyunJson @('ecs', 'DescribeImages', '--region', $RegionId, '--ImageId', $createdImageId)
        $status = @($images.Images.Image)[0].Status
        Write-Host "  image status=$status"
        if ($status -eq 'Available') { return $true }
        if ($status -in @('CreateFailed', 'Failed', 'Error')) { throw "自定义镜像创建失败: $status" }
        return $false
    } "自定义镜像 $createdImageId 可用" 3600
    Write-Host "BREWFS_PERF_IMAGE_ID=$createdImageId"
    return $createdImageId
}

try {
    if ($Action -eq 'status') {
        if (-not $ImageId) { throw 'status 需要 -ImageId。' }
        Invoke-AliyunJson @('ecs', 'DescribeImages', '--region', $RegionId, '--ImageId', $ImageId) | ConvertTo-Json -Depth 12
    } elseif ($Action -eq 'delete') {
        if (-not $ImageId) { throw 'delete 需要 -ImageId。' }
        Invoke-AliyunJson @('ecs', 'DeleteImage', '--region', $RegionId, '--ImageId', $ImageId) | ConvertTo-Json -Depth 8
        Write-Host "自定义镜像删除请求已提交: $ImageId"
    } else {
        if ($ImageId) { throw 'prepare 不接受 -ImageId；请使用 -Action status 或 delete。' }
        if ($ImageName -notmatch '^[A-Za-z][A-Za-z0-9_-]{1,100}$') { throw "ImageName 不合法: $ImageName" }
        New-PerfImage | Out-Host
    }
} finally {
    Remove-BuilderInstance
}
