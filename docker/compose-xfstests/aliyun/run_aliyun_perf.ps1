[CmdletBinding()]
param(
    [ValidateSet('run', 'create', 'status', 'destroy')]
    [string]$Action = 'run',
    [string]$InstanceId,
    [string]$RegionId = 'cn-hangzhou',
    [string]$ZoneId = 'cn-hangzhou-h',
    [string]$VSwitchId,
    [string]$SecurityGroupId,
    [string]$InstanceName,
    [string]$InstanceType = 'ecs.u1-c1m4.2xlarge',
    [ValidateRange(40, 1000)]
    [int]$SystemDiskSizeGiB = 100,
    # The native runner always provisions a disposable data disk for the
    # workload. Keep its shape explicit so StrictMode does not leave the
    # RunInstances request dependent on undeclared variables.
    [ValidateSet('cloud_essd', 'cloud_essd_entry', 'cloud_auto')]
    [string]$DataDiskCategory = 'cloud_essd',
    [ValidateRange(100, 32768)]
    [int]$DataDiskSize = 512,
    [ValidateSet('PL0', 'PL1', 'PL2', 'PL3')]
    [string]$DataDiskPerformanceLevel = 'PL2',
    [string]$ImageId = 'ubuntu_24_04_x64_20G_alibase_20260916.vhd',
    [ValidateSet('redis', 'none')]
    [string]$Backend = 'none',
    [ValidateSet('s3')]
    [string]$DataBackend = 's3',
    [ValidateSet('packed-snapshot')]
    [string]$VolumeFormat = 'packed-snapshot',
    [ValidateScript({
        $tools = @($_ -split '\s+' | Where-Object { $_ })
        if ($tools.Count -eq 0 -or @($tools | Where-Object { $_ -notin @('packed-tree', 'packed-stat', 'packed-smallfiles', 'packed-gpu-smallfiles') }).Count -ne 0) {
            throw 'Unsupported packed tool; tree/stat/smallfiles are supported, fio/POSIX fixture layout remains OPEN.'
        }
        $true
    })]
    [string]$PerfTools = 'packed-tree packed-smallfiles',
    [int64]$PackedSmallFileCount = 1000000,
    [int64]$PackedSmallFileSizeBytes = 102400,
    [int64]$PackedSmallFileMinSizeBytes = 0,
    [int64]$PackedSmallFileMaxSizeBytes = 0,
    [ValidateRange(0, 8)]
    [int]$PackedDirLevels = 3,
    [int64]$PackedDirsPerLevel = 10,
    [int64]$PackedFilesPerDir = 1000,
    [ValidateSet('random-small-file', 'sequential-small-file', 'mixed')]
    [string]$PackedAccessProfile = 'random-small-file',
    [int64]$PackedFioFileSizeBytes = 67108864,
    [string]$PackedSmallFileReadBytes = '0',
    [string]$PackedExistingManifestKey,
    [switch]$PackedSkipFixture,
    [int]$FioRuntimeSeconds = 20,
    [UInt64]$ReadMemoryBytes = 0,
    [UInt64]$ReadSsdBytes = 0,
    [UInt64]$PackedFrameWindowCacheBytes = 0,
    [bool]$PackedFrameWindowPrefetch = $false,
    [UInt64]$PackedDecodedFrameCacheBytes = 0,
    [UInt64]$PackedMetadataCacheBytes = 268435456,
    [ValidateSet('off', 'auto', 'eager')]
    [string]$PackedMetadataPrefetch = 'auto',
    [bool]$PrefetchEnabled = $false,
    [UInt64]$PrefetchMaxBytes = 8388608,
    [int]$PrefetchConcurrency = 7,
    [bool]$RangeBackgroundPrefetch = $false,
    [ValidateSet('0', '1')]
    [string]$ReadDirectIo = '1',
    [ValidateSet('0', '1')]
    [string]$FuseKeepCache = '0',
    [ValidateRange(0, 86400000)]
    [int64]$MetadataCacheTtlMs = 0,
    [ValidateRange(30, 14400)]
    [int]$ToolTimeoutSeconds = 900,
    [string]$S3Bucket,
    [string]$S3Endpoint,
    [string]$S3Region = 'cn-hangzhou',
    [string]$S3AccessKey,
    [string]$S3SecretKey,
    [bool]$S3ForcePathStyle = $false,
    [string]$RepoRoot,
    [string]$WslDistribution = 'Ubuntu-24.04',
    [string]$BinaryPath,
    [string]$FixtureBinaryPath,
    [string]$ScannerPath,
    [switch]$SkipBuild,
    [string]$ArtifactDirectory,
    [string]$ObjectPrefix,
    [string]$Repository,
    [string]$Ref,
    [switch]$ColdRead,
    [switch]$RunBench,
    [string]$AutoReleaseMinutes = '480',
    [switch]$KeepInstance,
    [switch]$NoCleanup,
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$script:CreatedInstance = $false
$script:RunArtifactDirectory = $null
$script:CredentialBundleKey = $null
$script:UploadedKeys = @()

function Resolve-Executable([string]$Name, [string[]]$Candidates = @()) {
    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($command) { return $command.Source }
    foreach ($candidate in $Candidates) {
        if (Test-Path -LiteralPath $candidate) { return $candidate }
    }
    throw "找不到 $Name，请先安装并加入 PATH。"
}

$aliyunCandidates = @()
if ($env:LOCALAPPDATA) {
    $aliyunCandidates += (Join-Path $env:LOCALAPPDATA 'AliyunCLI\aliyun.exe')
    $aliyunCandidates += (Join-Path $env:LOCALAPPDATA 'aliyun\aliyun.exe')
    $wingetRoot = Join-Path $env:LOCALAPPDATA 'Microsoft\WinGet\Packages'
    $aliyunCandidates += @(
        Get-ChildItem -Path (Join-Path $wingetRoot 'Alibaba.AlibabaCloudCLI_*\aliyun.exe') -File -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty FullName
    )
}
$script:Aliyun = Resolve-Executable 'aliyun' $aliyunCandidates

function Invoke-Checked([string]$File, [string[]]$Arguments) {
    $output = & $File @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "命令失败: $File $($Arguments -join ' ')$([Environment]::NewLine)$($output -join [Environment]::NewLine)"
    }
    return ($output -join [Environment]::NewLine)
}

function Invoke-AliyunJson([string[]]$Arguments) {
    $output = Invoke-Checked $Aliyun $Arguments
    $text = $output -join [Environment]::NewLine
    $start = $text.IndexOf('{')
    $end = $text.LastIndexOf('}')
    if ($start -lt 0 -or $end -le $start) {
        throw "Aliyun CLI returned no JSON object: $text"
    }
    return ($text.Substring($start, $end - $start + 1) | ConvertFrom-Json)
}

function Format-InstanceIds([string]$Value) {
    return '["' + $Value + '"]'
}

function Get-ResultVaultCurlArguments {
    $arguments = @(
        '--fail-with-body', '--silent', '--show-error', '--location',
        '--retry', '5', '--retry-delay', '2', '--retry-all-errors'
    )
    # Schannel fails closed when the Windows host cannot reach the certificate
    # revocation service (CRYPT_E_REVOCATION_OFFLINE). Keep certificate and
    # hostname verification enabled, but allow Result Vault transfers to run
    # while that separate revocation endpoint is unreachable.
    if ($IsWindows) { $arguments += '--ssl-no-revoke' }
    # TUN-mode VPNs commonly return an RFC 2544 fake IP (198.18.0.0/15). The
    # proxy behind that address can truncate larger multipart uploads. Allow a
    # caller to pin only Result Vault traffic to a real edge address without
    # changing system DNS or disabling the VPN for the rest of the run.
    if ($ResultVaultResolveIp) {
        $parsedIp = $null
        if (-not [Net.IPAddress]::TryParse($ResultVaultResolveIp, [ref]$parsedIp)) {
            throw "ResultVaultResolveIp 不是有效 IP: $ResultVaultResolveIp"
        }
        if (-not $ResultVaultUrl) { throw 'ResultVaultResolveIp 需要 ResultVaultUrl。' }
        $resultVaultUri = [Uri]$ResultVaultUrl
        $resultVaultPort = if ($resultVaultUri.IsDefaultPort) {
            if ($resultVaultUri.Scheme -eq 'https') { 443 } else { 80 }
        } else {
            $resultVaultUri.Port
        }
        $arguments += @('--resolve', "$($resultVaultUri.Host):${resultVaultPort}:$ResultVaultResolveIp")
    }
    return $arguments
}

function Wait-Until([scriptblock]$Condition, [string]$Description, [int]$TimeoutSeconds = 900) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastError = $null
    do {
        try {
            if (& $Condition) { return }
            $lastError = $null
        }
        catch {
            $lastError = $_
            Write-Host "  wait condition error: $($_.Exception.Message)"
        }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)
    if ($lastError) {
        throw "等待超时: $Description; last error: $($lastError.Exception.Message)"
    }
    throw "等待超时: $Description"
}

function Quote-Bash([string]$Value) {
    $replacement = "'" + '"' + "'" + '"' + "'"
    return "'" + $Value.Replace("'", $replacement) + "'"
}

function Get-LocalRepoRoot {
    if ($RepoRoot) { return (Resolve-Path -LiteralPath $RepoRoot).Path }
    return (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..\..')).Path
}

function Get-WslPath([string]$WindowsPath) {
    $full = (Resolve-Path -LiteralPath $WindowsPath).Path
    if ($full -notmatch '^[A-Za-z]:\\') {
        throw "WSL 本地构建要求工作树位于 Windows 本地盘: $full"
    }
    $drive = $full.Substring(0, 1).ToLowerInvariant()
    $rest = $full.Substring(2).Replace('\', '/')
    return "/mnt/$drive$rest"
}

function Get-ConfiguredCredentials {
    $configCandidates = @()
    if ($env:USERPROFILE) { $configCandidates += (Join-Path $env:USERPROFILE '.aliyun\config.json') }
    if ($env:HOME) { $configCandidates += (Join-Path $env:HOME '.aliyun\config.json') }
    foreach ($path in $configCandidates) {
        if (-not (Test-Path -LiteralPath $path)) { continue }
        $config = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
        $profileName = [string]$config.current
        $profile = @($config.profiles | Where-Object { $_.name -eq $profileName })[0]
        if ($profile -and $profile.mode -eq 'AK' -and $profile.access_key_id -and $profile.access_key_secret) {
            return @([string]$profile.access_key_id, [string]$profile.access_key_secret)
        }
    }
    throw '未找到 Aliyun AK/SK。请配置 aliyun CLI，或显式传入 -S3AccessKey/-S3SecretKey。'
}

function Build-LocalBinaries {
    $root = Get-LocalRepoRoot
    if (-not $BinaryPath) { $script:BinaryPath = Join-Path $root 'target\release\brewfs' }
    $fixtureName = 'packed_snapshot_fixture'
    if (-not $FixtureBinaryPath) { $script:FixtureBinaryPath = Join-Path $root "target\release\$fixtureName" }
    if ($SkipBuild) {
        if (-not (Test-Path -LiteralPath $BinaryPath) -or -not (Test-Path -LiteralPath $FixtureBinaryPath)) {
            throw '-SkipBuild 要求 -BinaryPath 和 -FixtureBinaryPath 都存在。'
        }
        return
    }
    $wslRoot = Get-WslPath $root
    $command = @"
set -Eeuo pipefail
cd $(Quote-Bash $wslRoot)
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_RELEASE_DEBUG=0
cargo build --release --features native-packed-base,frozen-base-metadata --bin brewfs --bin $fixtureName
strip target/release/brewfs target/release/$fixtureName
file target/release/brewfs target/release/$fixtureName
"@
    $output = & wsl.exe -d $WslDistribution -- bash -lc $command 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "WSL 本地编译失败:$([Environment]::NewLine)$($output -join [Environment]::NewLine)"
    }
    Write-Host ($output -join [Environment]::NewLine)
    if (-not (Test-Path -LiteralPath $BinaryPath) -or -not (Test-Path -LiteralPath $FixtureBinaryPath)) {
        throw 'WSL 编译完成但没有找到 Linux ELF 二进制。'
    }
}

function Publish-OssObject([string]$Path, [string]$Key) {
    $uri = "oss://$S3Bucket/$Key"
    $uploadPath = $Path
    $temporaryPath = $null
    try {
        # Windows checkouts may contain CRLF. Normalize text payloads before
        # they leave the host so shell/python entrypoints are executable on ECS.
        if ([IO.Path]::GetExtension($Path) -in @('.sh', '.py')) {
            $temporaryPath = Join-Path ([IO.Path]::GetTempPath()) ("brewfs-upload-{0}{1}" -f [Guid]::NewGuid().ToString('N'), [IO.Path]::GetExtension($Path))
            $text = [IO.File]::ReadAllText($Path)
            $text = $text.Replace([string][char]13 + [char]10, [string][char]10)
            [IO.File]::WriteAllText($temporaryPath, $text, [Text.UTF8Encoding]::new($false))
            $uploadPath = $temporaryPath
        }
        Write-Host "上传 OSS 对象: $Key"
        $output = & $Aliyun oss cp $uploadPath $uri --region $S3Region --force 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "OSS 上传失败 ($Key): $($output -join [Environment]::NewLine)"
        }
        $script:UploadedKeys += $Key
    }
    finally {
        if ($temporaryPath) {
            Remove-Item -LiteralPath $temporaryPath -Force -ErrorAction SilentlyContinue
        }
    }
}

function Get-OssSignedUrl([string]$Key, [int]$TimeoutSeconds = 172800) {
    $uri = "oss://$S3Bucket/$Key"
    $output = Invoke-Checked $Aliyun @('oss', 'sign', $uri, '--region', $S3Region, '--timeout', [string]$TimeoutSeconds)
    $joined = $output -join [Environment]::NewLine
    $match = [regex]::Match($joined, 'https?://[^\s]+')
    if (-not $match.Success) { throw "OSS 签名 URL 生成失败 ($Key): $joined" }
    return $match.Value.TrimEnd('.', ',')
}

function Publish-CredentialBundle([string]$Key) {
    $path = Join-Path ([IO.Path]::GetTempPath()) ("brewfs-oss-{0}.env" -f [Guid]::NewGuid().ToString('N'))
    $contents = @(
        "export AWS_ACCESS_KEY_ID=$(Quote-Bash $S3AccessKey)"
        "export AWS_SECRET_ACCESS_KEY=$(Quote-Bash $S3SecretKey)"
        "export AWS_DEFAULT_REGION=$(Quote-Bash $S3Region)"
    ) -join "`n"
    try {
        [IO.File]::WriteAllText($path, $contents, [Text.Encoding]::ASCII)
        Publish-OssObject $path $Key
        $script:CredentialBundleKey = $Key
    }
    finally {
        Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
    }
}

function New-EcsInstance {
    if (-not $VSwitchId -or -not $SecurityGroupId) {
        throw '创建 ECS 需要 -VSwitchId 和 -SecurityGroupId。脚本不自动创建 VPC。'
    }
    if (-not $script:InstanceName) { $script:InstanceName = 'brewfs-packed-{0}' -f (Get-Date -Format 'yyyyMMdd-HHmmss') }
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
        '--SystemDisk.Category', 'cloud_essd', '--SystemDisk.Size', [string]$SystemDiskSizeGiB,
        '--SystemDisk.PerformanceLevel', 'PL1',
        '--DataDisk.1.Category', $DataDiskCategory, '--DataDisk.1.Size', $DataDiskSize.ToString(),
        '--DataDisk.1.PerformanceLevel', $DataDiskPerformanceLevel,
        '--DataDisk.1.DeleteWithInstance', 'true',
        '--Tag.1.Key', 'brewfs-test', '--Tag.1.Value', $InstanceName
    )
    $result = Invoke-AliyunJson $runArgs
    $script:InstanceId = @($result.InstanceIdSets.InstanceIdSet)[0]
    if (-not $script:InstanceId) { throw 'RunInstances 未返回 InstanceId。' }
    $script:CreatedInstance = $true
    Write-Host "ECS 创建成功: $script:InstanceId"
    Wait-Until {
        $instance = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', (Format-InstanceIds $script:InstanceId))
        $state = @($instance.Instances.Instance)[0].Status
        Write-Host "  ECS state=$state"
        $state -eq 'Running'
    } 'ECS 启动' 900
}

function Get-RemoteCommand([string]$BinaryUrl, [string]$FixtureUrl, [string]$RunnerUrl, [string]$ScannerUrl, [string]$CredentialUrl) {
    $fixtureName = 'packed_snapshot_fixture'
    $remote = @'
#!/usr/bin/env bash
set -Eeuo pipefail
export DEBIAN_FRONTEND=noninteractive
DATA_MOUNT=/mnt/brewfs-data
if [[ -b /dev/vdb ]]; then
  mkdir -p "$DATA_MOUNT"
  if ! mountpoint -q "$DATA_MOUNT"; then
    if ! blkid /dev/vdb >/dev/null 2>&1; then
      mkfs.ext4 -F /dev/vdb
    fi
    mount /dev/vdb "$DATA_MOUNT"
  fi
fi
if mountpoint -q "$DATA_MOUNT"; then
  WORK="$DATA_MOUNT/brewfs-packed-native"
else
  WORK=/opt/brewfs-packed-native
fi
ARTIFACT_DIR="$WORK/artifacts"
mkdir -p "$WORK" "$ARTIFACT_DIR"

if ! apt-get update -qq; then
  if [[ -f /etc/apt/sources.list ]]; then
    awk '/^[[:space:]]*(deb|deb-src)[[:space:]]/ || /^[[:space:]]*#/ || /^[[:space:]]*$/ { print; next } { print "# disabled invalid apt source: " $0 }' /etc/apt/sources.list >/etc/apt/sources.list.brewfs-clean
    mv /etc/apt/sources.list.brewfs-clean /etc/apt/sources.list
  fi
  apt-get update -qq
fi
apt-get install -y -qq ca-certificates curl fio fuse3 python3 util-linux procps
modprobe fuse 2>/dev/null || true

curl --fail --location --retry 5 --connect-timeout 20 --output "$WORK/brewfs" __BINARY_URL__
curl --fail --location --retry 5 --connect-timeout 20 --output "$WORK/__FIXTURE_FILE_NAME__" __FIXTURE_URL__
curl --fail --location --retry 5 --connect-timeout 20 --output "$WORK/run_packed_native.sh" __RUNNER_URL__
curl --fail --location --retry 5 --connect-timeout 20 --output "$WORK/smallfiles_scan.py" __SCANNER_URL__
chmod 0755 "$WORK/brewfs" "$WORK/__FIXTURE_FILE_NAME__" "$WORK/run_packed_native.sh" "$WORK/smallfiles_scan.py"

curl --fail --location --retry 5 --connect-timeout 20 --output "$WORK/s3-credentials.env" __CREDENTIAL_URL__
chmod 0600 "$WORK/s3-credentials.env"
source "$WORK/s3-credentials.env"
rm -f "$WORK/s3-credentials.env"
export AWS_EC2_METADATA_DISABLED=true
export BREWFS_S3_BUCKET=__S3_BUCKET__
export BREWFS_S3_ENDPOINT=__S3_ENDPOINT__
export BREWFS_S3_REGION=__S3_REGION__
export BREWFS_S3_FORCE_PATH_STYLE=__S3_FORCE_PATH_STYLE__
export BREWFS_BIN="$WORK/brewfs"
export PACKED_FIXTURE_BIN="$WORK/__FIXTURE_FILE_NAME__"
export PACKED_SMALLFILES_SCANNER="$WORK/smallfiles_scan.py"
export PACKED_VOLUME_FORMAT=__VOLUME_FORMAT__
export BREWFS_NATIVE_WORK="$WORK"
export BREWFS_NATIVE_ARTIFACT_DIR="$ARTIFACT_DIR"
export PACKED_FIXTURE_PREFIX=__FIXTURE_PREFIX__
export PACKED_SKIP_FIXTURE=__PACKED_SKIP_FIXTURE__
export PACKED_EXISTING_MANIFEST_KEY=__PACKED_EXISTING_MANIFEST_KEY__
export PACKED_SMALLFILE_COUNT=__SMALLFILE_COUNT__
export PACKED_SMALLFILE_SIZE=__SMALLFILE_SIZE__
export PACKED_SMALLFILE_MIN_SIZE=__SMALLFILE_MIN_SIZE__
export PACKED_SMALLFILE_MAX_SIZE=__SMALLFILE_MAX_SIZE__
export PACKED_ACCESS_PROFILE=__PACKED_ACCESS_PROFILE__
export PACKED_DIR_LEVELS=__DIR_LEVELS__
export PACKED_DIRS_PER_LEVEL=__DIRS_PER_LEVEL__
export PACKED_FILES_PER_DIR=__FILES_PER_DIR__
export PERF_PACKED_FIO_FILE_SIZE=__FIO_FILE_SIZE__
export PERF_PACKED_SMALLFILE_READ_BYTES=__READ_BYTES__
export PERF_PACKED_SMALLFILE_WORKERS=16
export PERF_FIO_RUNTIME=__FIO_RUNTIME__
export PERF_TOOLS=__TOOLS__
export BREWFS_READ_MEMORY_BYTES=__READ_MEMORY_BYTES__
export BREWFS_READ_SSD_BYTES=__READ_SSD_BYTES__
export BREWFS_PACKED_FRAME_WINDOW_CACHE_BYTES=__PACKED_FRAME_WINDOW_CACHE_BYTES__
export BREWFS_PACKED_FRAME_WINDOW_PREFETCH=__PACKED_FRAME_WINDOW_PREFETCH__
export BREWFS_PACKED_DECODED_FRAME_CACHE_BYTES=__PACKED_DECODED_FRAME_CACHE_BYTES__
export BREWFS_PACKED_METADATA_CACHE_BYTES=__PACKED_METADATA_CACHE_BYTES__
export BREWFS_PACKED_METADATA_PREFETCH=__PACKED_METADATA_PREFETCH__
export BREWFS_PREFETCH_ENABLED=__PREFETCH_ENABLED__
export BREWFS_PREFETCH_MAX_BYTES=__PREFETCH_MAX_BYTES__
export BREWFS_PREFETCH_CONCURRENCY=__PREFETCH_CONCURRENCY__
export BREWFS_RANGE_BACKGROUND_PREFETCH=__RANGE_BACKGROUND_PREFETCH__
export BREWFS_FUSE_READ_DIRECT_IO=__READ_DIRECT_IO__
export PERF_TOOL_TIMEOUT_SECONDS=__TOOL_TIMEOUT_SECONDS__
export BREWFS_FUSE_KEEP_CACHE=__FUSE_KEEP_CACHE__
export BREWFS_METADATA_CACHE_TTL_MS=__METADATA_CACHE_TTL_MS__
export BREWFS_NOFILE_LIMIT=1048576
export RUST_LOG=info

mem_kib="$(awk '/^MemTotal:/ {print $2; exit}' /proc/meminfo)"
disk_bytes="$(df -B1 --output=size "$WORK" | tail -n 1 | tr -d ' ')"
cat >"$WORK/aliyun-resource-proof.env" <<EOF
instance_type=__INSTANCE_TYPE__
requested_memory_gib=32
requested_system_disk_gib=__SYSTEM_DISK_GIB__
mem_total_kib=$mem_kib
work_disk_bytes=$disk_bytes
s3_endpoint=__S3_ENDPOINT__
s3_bucket=__S3_BUCKET__
metadata_backend=none
container_runtime=none
EOF
[[ "$mem_kib" -ge 30000000 ]] || { echo "memory below 32 GiB" >&2; exit 1; }
[[ "$disk_bytes" -ge 90000000000 ]] || { echo "disk below 100 GB" >&2; exit 1; }

if bash "$WORK/run_packed_native.sh"; then
  :
else
  status=$?
  echo "--- packed native runner failed (exit=$status) ---"
  for log in "$ARTIFACT_DIR"/packed-fixture.log "$ARTIFACT_DIR"/brewfs.log "$ARTIFACT_DIR"/tools/*.log; do
    if [[ -f "$log" ]]; then
      echo "### $log"
      tail -n 80 "$log" || true
    fi
  done
  exit "$status"
fi
cat "$WORK/aliyun-resource-proof.env"
echo '--- packed native perf summary ---'
cat "$ARTIFACT_DIR/perf-summary.tsv"
echo '--- packed native concise diagnostics ---'
for log in "$ARTIFACT_DIR"/tools/*.log; do
  [[ -f "$log" ]] || continue
  grep -a 'smallfiles_scan_summary' "$log" || tail -n 4 "$log" || true
done
for mount_log in "$ARTIFACT_DIR"/tools/*-brewfs.log; do
  [[ -f "$mount_log" ]] || continue
  grep -a -E 'packed v3 (metadata prefetch complete|metadata cache stats|runtime read stats)' "$mount_log" || true
done
for stats in "$ARTIFACT_DIR"/tools/*.stats; do
  [[ -f "$stats" ]] || continue
  echo "### $stats"
  grep -a -E '^(brewfs_uptime_seconds|brewfs_fuse_(read|lookup|readdir|getattr)_|brewfs_cache_|brewfs_read_(block_cache|page_cache|range_gets|full_gets))' "$stats" || true
done
'@
    $values = @{
        '__BINARY_URL__' = Quote-Bash $BinaryUrl
        '__FIXTURE_URL__' = Quote-Bash $FixtureUrl
        '__RUNNER_URL__' = Quote-Bash $RunnerUrl
        '__SCANNER_URL__' = Quote-Bash $ScannerUrl
        '__CREDENTIAL_URL__' = Quote-Bash $CredentialUrl
        '__FIXTURE_FILE_NAME__' = $fixtureName
        '__VOLUME_FORMAT__' = Quote-Bash $VolumeFormat
        '__S3_REGION__' = Quote-Bash $S3Region
        '__S3_BUCKET__' = Quote-Bash $S3Bucket
        '__S3_ENDPOINT__' = Quote-Bash $S3Endpoint
        '__S3_FORCE_PATH_STYLE__' = Quote-Bash ($S3ForcePathStyle.ToString().ToLowerInvariant())
        '__FIXTURE_PREFIX__' = Quote-Bash $script:FixturePrefix
        '__PACKED_SKIP_FIXTURE__' = Quote-Bash ($PackedSkipFixture.ToString().ToLowerInvariant())
        '__PACKED_EXISTING_MANIFEST_KEY__' = Quote-Bash ([string]$PackedExistingManifestKey)
        '__SMALLFILE_COUNT__' = Quote-Bash ([string]$PackedSmallFileCount)
        '__SMALLFILE_SIZE__' = Quote-Bash ([string]$PackedSmallFileSizeBytes)
        '__SMALLFILE_MIN_SIZE__' = Quote-Bash ([string]$PackedSmallFileMinSizeBytes)
        '__SMALLFILE_MAX_SIZE__' = Quote-Bash ([string]$PackedSmallFileMaxSizeBytes)
        '__PACKED_ACCESS_PROFILE__' = Quote-Bash $PackedAccessProfile
        '__DIR_LEVELS__' = Quote-Bash ([string]$PackedDirLevels)
        '__DIRS_PER_LEVEL__' = Quote-Bash ([string]$PackedDirsPerLevel)
        '__FILES_PER_DIR__' = Quote-Bash ([string]$PackedFilesPerDir)
        '__FIO_FILE_SIZE__' = Quote-Bash ([string]$PackedFioFileSizeBytes)
        '__READ_BYTES__' = Quote-Bash ([string]$PackedSmallFileReadBytes)
        '__FIO_RUNTIME__' = Quote-Bash ([string]$FioRuntimeSeconds)
        '__READ_MEMORY_BYTES__' = Quote-Bash ([string]$ReadMemoryBytes)
        '__READ_SSD_BYTES__' = Quote-Bash ([string]$ReadSsdBytes)
        '__PACKED_FRAME_WINDOW_CACHE_BYTES__' = Quote-Bash ([string]$PackedFrameWindowCacheBytes)
        '__PACKED_FRAME_WINDOW_PREFETCH__' = Quote-Bash ($PackedFrameWindowPrefetch.ToString().ToLowerInvariant())
        '__PACKED_DECODED_FRAME_CACHE_BYTES__' = Quote-Bash ([string]$PackedDecodedFrameCacheBytes)
        '__PACKED_METADATA_CACHE_BYTES__' = Quote-Bash ([string]$PackedMetadataCacheBytes)
        '__PACKED_METADATA_PREFETCH__' = Quote-Bash $PackedMetadataPrefetch
        '__PREFETCH_ENABLED__' = Quote-Bash ($PrefetchEnabled.ToString().ToLowerInvariant())
        '__PREFETCH_MAX_BYTES__' = Quote-Bash ([string]$PrefetchMaxBytes)
        '__PREFETCH_CONCURRENCY__' = Quote-Bash ([string]$PrefetchConcurrency)
        '__RANGE_BACKGROUND_PREFETCH__' = Quote-Bash ($RangeBackgroundPrefetch.ToString().ToLowerInvariant())
        '__READ_DIRECT_IO__' = Quote-Bash $ReadDirectIo
        '__FUSE_KEEP_CACHE__' = Quote-Bash $FuseKeepCache
        '__METADATA_CACHE_TTL_MS__' = Quote-Bash $MetadataCacheTtlMs
        '__TOOL_TIMEOUT_SECONDS__' = Quote-Bash ([string]$ToolTimeoutSeconds)
        '__TOOLS__' = Quote-Bash $PerfTools
        '__INSTANCE_TYPE__' = Quote-Bash $InstanceType
        '__SYSTEM_DISK_GIB__' = [string]$SystemDiskSizeGiB
    }
    foreach ($key in $values.Keys) { $remote = $remote.Replace($key, [string]$values[$key]) }
    return $remote
}

function Invoke-PerfOnEcs {
    $artifact = if ($ArtifactDirectory) { $ArtifactDirectory } else { Join-Path $PSScriptRoot '..\artifacts\aliyun-native' }
    New-Item -ItemType Directory -Force -Path $artifact | Out-Null
    $script:RunArtifactDirectory = (Resolve-Path -LiteralPath $artifact).Path
    $commandText = Get-RemoteCommand $script:BinaryUrl $script:FixtureUrl $script:RunnerUrl $script:ScannerUrl $script:CredentialUrl
    $content = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($commandText))
    $run = Invoke-AliyunJson @(
        'ecs', 'RunCommand', '--region', $RegionId, '--Type', 'RunShellScript',
        '--InstanceId.1', $script:InstanceId, '--CommandContent', $content,
        '--ContentEncoding', 'Base64', '--Timeout', '172800',
        '--KeepCommand', 'false', '--Name', 'brewfs-packed-native'
    )
    $invokeId = $run.InvokeId
    if (-not $invokeId) { throw 'RunCommand 未返回 InvokeId。请确认 ECS Cloud Assistant Agent 在线。' }
    Write-Host "原生 packed 测试已提交: $invokeId"
    $deadline = (Get-Date).AddHours(48)
    $done = $false
    while (-not $done -and (Get-Date) -lt $deadline) {
        $result = Invoke-AliyunJson @('ecs', 'DescribeInvocationResults', '--region', $RegionId, '--InvokeId', $invokeId)
        $item = @($result.Invocation.InvocationResults.InvocationResult)[0]
        if ($item) {
            Write-Host "  invocation status=$($item.InvocationStatus)"
            if ($item.InvocationStatus -in @('Success', 'Failed', 'Stopped', 'Error', 'Terminated', 'Aborted', 'Timeout')) {
                $decoded = ''
                if ($item.Output) { $decoded = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($item.Output)) }
                Set-Content -LiteralPath (Join-Path $script:RunArtifactDirectory 'remote-output.log') -Value $decoded -Encoding UTF8
                Write-Host $decoded
                $done = $true
                if ($item.InvocationStatus -ne 'Success') { throw "远程原生测试失败: $($item.ErrorInfo)" }
            }
        }
        if (-not $done) { Start-Sleep -Seconds 10 }
    }
    if (-not $done) { throw '等待远程原生测试超时。' }
}

function Remove-EcsInstance {
    if (-not $script:InstanceId) { throw 'destroy 需要 -InstanceId。' }
    $instance = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', (Format-InstanceIds $script:InstanceId))
    $item = @($instance.Instances.Instance)[0]
    if (-not $item) { Write-Host "ECS 已不存在: $script:InstanceId"; return }
    if ($item.Status -ne 'Stopped') {
        if ($item.Status -ne 'Stopping') {
            Invoke-AliyunJson @('ecs', 'StopInstance', '--region', $RegionId, '--InstanceId', $script:InstanceId, '--ForceStop', 'true') | Out-Null
        }
        Wait-Until {
            $current = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', (Format-InstanceIds $script:InstanceId))
            @($current.Instances.Instance)[0].Status -eq 'Stopped'
        } "ECS $script:InstanceId 停止" 300
    }
    Invoke-AliyunJson @('ecs', 'DeleteInstance', '--region', $RegionId, '--InstanceId', $script:InstanceId, '--Force', 'true') | Out-Null
    Write-Host "ECS 删除任务已提交: $script:InstanceId"
}

function Remove-BinaryUpload {
    if (-not $S3Bucket -or @($script:UploadedKeys).Count -eq 0) { return }
    foreach ($key in @($script:UploadedKeys)) {
        try {
            Invoke-Checked $script:Aliyun @('oss', 'rm', "oss://$S3Bucket/$key", '--region', $S3Region, '--force') | Out-Null
        }
        catch {
            Write-Warning "临时 OSS 上传清理失败 ($key): $($_.Exception.Message)"
        }
    }
    $script:UploadedKeys = @()
}

try {
    if ($Action -eq 'status') {
        if (-not $InstanceId) { throw 'status 需要 -InstanceId。' }
        $script:InstanceId = $InstanceId
        Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', (Format-InstanceIds $script:InstanceId)) | ConvertTo-Json -Depth 8
        return
    }
    if ($Action -eq 'destroy') {
        if (-not $InstanceId) { throw 'destroy 需要 -InstanceId。' }
        $script:InstanceId = $InstanceId
        Remove-EcsInstance
        return
    }
    if ($Action -eq 'create') { New-EcsInstance; return }

    if ($VolumeFormat -ne 'packed-snapshot' -or $DataBackend -ne 's3') {
        throw '原生 ECS runner 只接受 packed-snapshot + Aliyun S3/OSS。'
    }
    if (-not $S3Bucket) { throw 'run 必须指定 -S3Bucket（Aliyun OSS bucket）。' }
    $expected = [int64]1
    for ($level = 0; $level -lt $PackedDirLevels; $level++) { $expected *= $PackedDirsPerLevel }
    $expected *= $PackedFilesPerDir
    if ($expected -ne $PackedSmallFileCount) { throw "packed 文件数量不一致: expected=$expected actual=$PackedSmallFileCount" }
    if ($PackedSmallFileSizeBytes -le 0 -or $PackedSmallFileSizeBytes -gt 4MB) { throw 'PackedSmallFileSizeBytes 必须在 1 到 4 MiB 之间。' }
    if ($PackedSmallFileMinSizeBytes -le 0) { $PackedSmallFileMinSizeBytes = $PackedSmallFileSizeBytes }
    if ($PackedSmallFileMaxSizeBytes -le 0) { $PackedSmallFileMaxSizeBytes = $PackedSmallFileSizeBytes }
    if ($PackedSmallFileMinSizeBytes -gt $PackedSmallFileMaxSizeBytes -or $PackedSmallFileMaxSizeBytes -gt 4MB) {
        throw 'PackedSmallFileMinSizeBytes/MaxSizeBytes 必须满足 0 < min <= max <= 4 MiB。'
    }
    if ($PackedSkipFixture -and -not $PackedExistingManifestKey) { throw '-PackedSkipFixture 必须同时指定 -PackedExistingManifestKey。' }
    if (-not $S3Endpoint) { $S3Endpoint = "https://oss-$S3Region.aliyuncs.com" }
    if (-not $S3AccessKey -or -not $S3SecretKey) {
        $credentials = Get-ConfiguredCredentials
        if (-not $S3AccessKey) { $S3AccessKey = $credentials[0] }
        if (-not $S3SecretKey) { $S3SecretKey = $credentials[1] }
    }
    if (-not $ObjectPrefix) { $script:ObjectPrefix = 'brewfs-native-{0}' -f (Get-Date -Format 'yyyyMMdd-HHmmss') }
    $script:FixturePrefix = "$ObjectPrefix/fixture"

    Build-LocalBinaries
    if (-not $ScannerPath) {
        $ScannerPath = Join-Path (Get-LocalRepoRoot) 'tools\perf\smallfiles_scan.py'
    }
    if (-not (Test-Path -LiteralPath $ScannerPath)) {
        throw "找不到共享 small-file scanner: $ScannerPath"
    }
    $ScannerPath = (Resolve-Path -LiteralPath $ScannerPath).ProviderPath
    Publish-OssObject $BinaryPath "$ObjectPrefix/bin/brewfs"
    $fixtureName = 'packed_snapshot_fixture'
    Publish-OssObject $FixtureBinaryPath "$ObjectPrefix/bin/$fixtureName"
    $nativeScriptPath = Join-Path $PSScriptRoot 'run_aliyun_packed_native.sh'
    Publish-OssObject $nativeScriptPath "$ObjectPrefix/bin/run_aliyun_packed_native.sh"
    Publish-OssObject $ScannerPath "$ObjectPrefix/bin/smallfiles_scan.py"
    $credentialKey = "$ObjectPrefix/bootstrap/s3-credentials.env"
    Publish-CredentialBundle $credentialKey
    $script:BinaryUrl = Get-OssSignedUrl "$ObjectPrefix/bin/brewfs"
    $script:FixtureUrl = Get-OssSignedUrl "$ObjectPrefix/bin/$fixtureName"
    $script:RunnerUrl = Get-OssSignedUrl "$ObjectPrefix/bin/run_aliyun_packed_native.sh"
    $script:ScannerUrl = Get-OssSignedUrl "$ObjectPrefix/bin/smallfiles_scan.py"
    $script:CredentialUrl = Get-OssSignedUrl $credentialKey 28800

    if (-not $InstanceId) { New-EcsInstance } else { $script:InstanceId = $InstanceId }
    if ($DryRun) { Write-Host "Dry run: 本地 Linux binary 已上传，未下发远程命令。instance=$script:InstanceId"; return }
    Invoke-PerfOnEcs
}
finally {
    if ($script:CredentialBundleKey) {
        try {
            $credentialUri = "oss://$S3Bucket/$($script:CredentialBundleKey)"
            Invoke-Checked $Aliyun @('oss', 'rm', $credentialUri, '--region', $S3Region, '--force') | Out-Null
            Write-Host '已删除临时 OSS 凭据对象。'
        } catch {
            Write-Warning "临时 OSS 凭据对象清理失败，限时签名 URL 仍会在 8 小时后过期: $($_.Exception.Message)"
        }
        $script:UploadedKeys = @($script:UploadedKeys | Where-Object { $_ -ne $script:CredentialBundleKey })
    }
    if ($Action -eq 'run' -and $script:CreatedInstance -and -not $KeepInstance -and -not $NoCleanup) {
        try { Remove-EcsInstance } catch { Write-Warning "ECS 自动清理失败: $($_.Exception.Message)" }
    }
    Remove-BinaryUpload
}
