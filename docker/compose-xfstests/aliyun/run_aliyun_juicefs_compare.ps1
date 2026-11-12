[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$InstanceId,
    [Parameter(Mandatory = $true)][string]$S3Bucket,
    [string]$S3Region = 'cn-hangzhou',
    [string]$S3Endpoint = 'https://oss-cn-hangzhou-internal.aliyuncs.com',
    [string]$S3AccessKey,
    [string]$S3SecretKey,
    [string]$ObjectPrefix = ('brewfs-jfs-{0}' -f (Get-Date -Format 'yyyyMMdd-HHmmss')),
    [string]$RawObjectPrefix,
    [switch]$SkipRawUpload,
    [switch]$KeepRawObjects,
    [string]$JuiceFsBinaryPath,
    [string]$RawFixtureBinaryPath,
    [string]$RunnerPath,
    [string]$ScannerPath,
    [ValidateSet('redis', 'tikv')]
    [string]$MetadataBackend = 'redis',
    [string]$TikvVersion = 'v6.5.3',
    [int64]$SmallFileCount = 10000,
    [int64]$SmallFileSizeBytes = 102400,
    [int64]$SmallFileMinSizeBytes = 0,
    [int64]$SmallFileMaxSizeBytes = 0,
    [int]$DirLevels = 2,
    [int64]$DirsPerLevel = 10,
    [int64]$FilesPerDir = 100,
    [string]$PerfTools = 'juicefs-tree juicefs-smallfiles',
    [ValidateRange(30, 14400)][int]$ToolTimeoutSeconds = 7200,
    [ValidateRange(0, 86400)]
    [int]$MetadataCacheTtlSec = 0,
    [ValidateRange(0, 200)]
    [int]$MetadataLatencyMs = 0,
    [string]$ArtifactDirectory
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if (-not $RunnerPath) {
    $RunnerPath = Join-Path $PSScriptRoot 'run_aliyun_juicefs_native.sh'
}
if (-not $ScannerPath) {
    $ScannerPath = Join-Path $PSScriptRoot '..\..\..\tools\perf\smallfiles_scan.py'
}
if (-not (Test-Path -LiteralPath $ScannerPath)) {
    throw "找不到共享 smallfiles scanner: $ScannerPath"
}
$ScannerPath = (Resolve-Path -LiteralPath $ScannerPath).ProviderPath

function Resolve-Executable([string]$Name, [string[]]$Candidates = @()) {
    $command = Get-Command $Name -ErrorAction SilentlyContinue
    if ($command) { return $command.Source }
    foreach ($candidate in $Candidates) {
        if (Test-Path -LiteralPath $candidate) { return $candidate }
    }
    throw "找不到 $Name"
}
$aliyunCandidates = @()
if ($env:LOCALAPPDATA) { $aliyunCandidates += (Join-Path $env:LOCALAPPDATA 'AliyunCLI\aliyun.exe') }
$Aliyun = Resolve-Executable 'aliyun' $aliyunCandidates

if ($JuiceFsBinaryPath) {
    if (-not (Test-Path -LiteralPath $JuiceFsBinaryPath)) {
        throw "找不到 JuiceFS 二进制: $JuiceFsBinaryPath"
    }
    $JuiceFsBinaryPath = (Resolve-Path -LiteralPath $JuiceFsBinaryPath).ProviderPath
} else {
    $juiceFsCandidates = @(
        (Join-Path $PSScriptRoot 'juicefs'),
        (Join-Path ([IO.Path]::GetTempPath()) 'juicefs')
    )
    if ($env:LOCALAPPDATA) {
        $juiceFsCandidates += (Join-Path $env:LOCALAPPDATA 'JuiceFS\juicefs.exe')
    }
    $JuiceFsBinaryPath = Resolve-Executable 'juicefs' $juiceFsCandidates
}

if (-not $RawFixtureBinaryPath) {
    $RawFixtureBinaryPath = Join-Path $PSScriptRoot '..\..\..\target\release\packed_snapshot_fixture'
}
if (-not (Test-Path -LiteralPath $RawFixtureBinaryPath)) {
    throw "找不到 raw SDK fixture binary: $RawFixtureBinaryPath"
}
$RawFixtureBinaryPath = (Resolve-Path -LiteralPath $RawFixtureBinaryPath).ProviderPath
if ($SmallFileMinSizeBytes -le 0) { $SmallFileMinSizeBytes = $SmallFileSizeBytes }
if ($SmallFileMaxSizeBytes -le 0) { $SmallFileMaxSizeBytes = $SmallFileSizeBytes }
if ($SmallFileMinSizeBytes -gt $SmallFileMaxSizeBytes -or $SmallFileMaxSizeBytes -gt 4MB) {
    throw 'SmallFileMinSizeBytes/MaxSizeBytes must satisfy 0 < min <= max <= 4 MiB.'
}

function Invoke-Checked([string]$File, [string[]]$Arguments) {
    $output = & $File @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "命令失败: $File $($Arguments -join ' ') $($output -join ' | ')"
    }
    return $output
}
function Invoke-AliyunJson([string[]]$Arguments) {
    return ((Invoke-Checked $Aliyun $Arguments) -join ' ' | ConvertFrom-Json)
}
function Quote-Bash([string]$Value) {
    $replacement = "'" + '"' + "'" + '"' + "'"
    return "'" + $Value.Replace("'", $replacement) + "'"
}
function Get-Credentials {
    if ($S3AccessKey -and $S3SecretKey) { return @($S3AccessKey, $S3SecretKey) }
    $paths = @()
    if ($env:USERPROFILE) { $paths += (Join-Path $env:USERPROFILE '.aliyun\config.json') }
    if ($env:HOME) { $paths += (Join-Path $env:HOME '.aliyun\config.json') }
    foreach ($path in $paths) {
        if (-not (Test-Path -LiteralPath $path)) { continue }
        $config = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
        $profile = @($config.profiles | Where-Object { $_.name -eq [string]$config.current })[0]
        if ($profile -and $profile.mode -eq 'AK') {
            return @([string]$profile.access_key_id, [string]$profile.access_key_secret)
        }
    }
    throw '未找到 Aliyun AK/SK'
}
function Publish([string]$Path, [string]$Key) {
    Invoke-Checked $Aliyun @('oss', 'cp', $Path, "oss://$S3Bucket/$Key", '--region', $S3Region, '--force') | Out-Null
}
function Sign([string]$Key, [int]$Timeout = 28800) {
    $out = Invoke-Checked $Aliyun @('oss', 'sign', "oss://$S3Bucket/$Key", '--region', $S3Region, '--timeout', [string]$Timeout)
    $match = [regex]::Match(($out -join ' '), 'https?://[^\s]+')
    if (-not $match.Success) { throw "签名失败: $Key" }
    return $match.Value.TrimEnd('.', ',')
}

if (-not $ArtifactDirectory) {
    $ArtifactDirectory = Join-Path $PSScriptRoot '..\artifacts\aliyun-jfs-native'
}
New-Item -ItemType Directory -Force -Path $ArtifactDirectory | Out-Null
$script:CredentialKey = $null
$script:RawFixtureKey = $null
$script:JuiceFsObjectPrefix = $null

$credentials = Get-Credentials
$S3AccessKey = $credentials[0]
$S3SecretKey = $credentials[1]
$prefix = $ObjectPrefix.TrimEnd('/')
$script:RawObjectPrefix = if ($RawObjectPrefix) { $RawObjectPrefix.Trim('/') } else { "$prefix/raw" }
$script:JuiceFsObjectPrefix = "$prefix/data"
$script:JuiceFsVolumeName = "jfs-" + $prefix.Replace('/', '-').Replace('_', '-').Replace('.', '-')
$credentialPath = Join-Path ([IO.Path]::GetTempPath()) ("brewfs-jfs-{0}.env" -f [Guid]::NewGuid().ToString('N'))
$credentialText = @(
    "export AWS_ACCESS_KEY_ID=$(Quote-Bash $S3AccessKey)"
    "export AWS_SECRET_ACCESS_KEY=$(Quote-Bash $S3SecretKey)"
    "export AWS_DEFAULT_REGION=$(Quote-Bash $S3Region)"
) -join ([char]10)
[IO.File]::WriteAllText($credentialPath, $credentialText, [Text.Encoding]::ASCII)

try {
    Publish $JuiceFsBinaryPath "$prefix/bin/juicefs"
    $rawFixtureName = 'packed_snapshot_fixture'
    Publish $RawFixtureBinaryPath "$prefix/bin/$rawFixtureName"
    Publish $RunnerPath "$prefix/bin/run_aliyun_juicefs_native.sh"
    Publish $ScannerPath "$prefix/bin/smallfiles_scan.py"
    $script:CredentialKey = "$prefix/bootstrap/s3-credentials.env"
    Publish $credentialPath $script:CredentialKey

    $binaryUrl = Sign "$prefix/bin/juicefs"
    $rawFixtureUrl = Sign "$prefix/bin/$rawFixtureName"
    $runnerUrl = Sign "$prefix/bin/run_aliyun_juicefs_native.sh"
    $scannerUrl = Sign "$prefix/bin/smallfiles_scan.py"
    $credentialUrl = Sign $script:CredentialKey
    $remote = @'
#!/usr/bin/env bash
set -Eeuo pipefail
export DEBIAN_FRONTEND=noninteractive
WORK=/opt/juicefs-native
ARTIFACT_DIR="$WORK/artifacts"
mkdir -p "$WORK" "$ARTIFACT_DIR"
if ! apt-get update -qq; then
  sed -i 's|^deb cdrom:|# deb cdrom:|' /etc/apt/sources.list || true
  apt-get update -qq
fi
apt-get install -y -qq ca-certificates curl fuse3 python3 util-linux procps iproute2
if [[ __META_BACKEND__ == redis ]]; then
  apt-get install -y -qq redis-server redis-tools
fi
modprobe fuse 2>/dev/null || true
curl --fail --location --retry 5 --connect-timeout 20 --output "$WORK/juicefs" __JUICEFS_URL__
curl --fail --location --retry 5 --connect-timeout 20 --output "$WORK/packed_snapshot_fixture" __RAW_FIXTURE_URL__
curl --fail --location --retry 5 --connect-timeout 20 --output "$WORK/run_juicefs.sh" __RUNNER_URL__
curl --fail --location --retry 5 --connect-timeout 20 --output "$WORK/smallfiles_scan.py" __SCANNER_URL__
curl --fail --location --retry 5 --connect-timeout 20 --output "$WORK/s3-credentials.env" __CREDENTIAL_URL__
chmod 0755 "$WORK/juicefs" "$WORK/packed_snapshot_fixture" "$WORK/run_juicefs.sh" "$WORK/smallfiles_scan.py"
chmod 0600 "$WORK/s3-credentials.env"
source "$WORK/s3-credentials.env"
rm -f "$WORK/s3-credentials.env"
export AWS_EC2_METADATA_DISABLED=true
export JUICEFS_BIN="$WORK/juicefs"
export JFS_NATIVE_WORK="$WORK"
export JFS_NATIVE_ARTIFACT_DIR="$ARTIFACT_DIR"
export JFS_S3_BUCKET=__S3_BUCKET__
export JFS_S3_REGION=__S3_REGION__
export JFS_S3_ENDPOINT=__S3_ENDPOINT__
export JFS_RAW_FIXTURE_BIN="$WORK/packed_snapshot_fixture"
export JFS_SMALLFILES_SCANNER="$WORK/smallfiles_scan.py"
export JFS_RAW_OBJECT_PREFIX=__RAW_OBJECT_PREFIX__
export JFS_SKIP_RAW_UPLOAD=__SKIP_RAW_UPLOAD__
export JFS_SMALLFILE_COUNT=__COUNT__
export JFS_SMALLFILE_SIZE=__SIZE__
export JFS_SMALLFILE_MIN_SIZE=__MIN_SIZE__
export JFS_SMALLFILE_MAX_SIZE=__MAX_SIZE__
export JFS_SMALLFILE_WORKERS=16
export JFS_DIR_LEVELS=__LEVELS__
export JFS_DIRS_PER_LEVEL=__FANOUT__
export JFS_FILES_PER_DIR=__FILES_PER_DIR__
export JFS_PERF_TOOLS=__PERF_TOOLS__
export JFS_METADATA_CACHE_TTL_SEC=__METADATA_CACHE_TTL_SEC__
export JFS_TOOL_TIMEOUT_SECONDS=__TOOL_TIMEOUT_SECONDS__
export JFS_VOLUME_NAME=__VOLUME_NAME__
export JFS_DATA_PREFIX=__DATA_PREFIX__
export JFS_META_BACKEND=__META_BACKEND__
export JFS_TIKV_VERSION=__TIKV_VERSION__
export JFS_PREFETCH_CACHE_SIZE_MIB=4096
export JFS_PREFETCH_BLOCKS=16
export JFS_METADATA_LATENCY_MS=__METADATA_LATENCY_MS__
export RUST_LOG=warn
if bash "$WORK/run_juicefs.sh"; then
  :
else
  status=$?
  echo "--- JuiceFS native runner failed (exit=$status) ---"
  for log in "$ARTIFACT_DIR"/raw-upload.log "$ARTIFACT_DIR"/juicefs-sync.log "$ARTIFACT_DIR"/prepare.log "$ARTIFACT_DIR"/scan-*.log "$WORK"/juicefs-*.log; do
    if [[ -f "$log" ]]; then echo "### $log"; tail -n 80 "$log" || true; fi
  done
  exit "$status"
fi
cat "$ARTIFACT_DIR/perf-summary.tsv"
printf '%s\n' '--- JuiceFS scanner summaries ---'
for log in "$ARTIFACT_DIR"/scan-*.log; do [[ -f "$log" ]] && { echo "### $log"; tail -n 4 "$log"; }; done
printf '%s\n' '--- JuiceFS cache proof ---'
cat "$ARTIFACT_DIR/cache-proof.env" 2>/dev/null || true
printf '%s\n' '--- JuiceFS metadata backend snapshots ---'
for proof in "$ARTIFACT_DIR"/metadata-*.env; do [[ -f "$proof" ]] && { echo "### $proof"; cat "$proof"; }; done
'@
    $values = @{
        '__JUICEFS_URL__' = (Quote-Bash $binaryUrl)
        '__RAW_FIXTURE_URL__' = (Quote-Bash $rawFixtureUrl)
        '__RUNNER_URL__' = (Quote-Bash $runnerUrl)
        '__SCANNER_URL__' = (Quote-Bash $scannerUrl)
        '__CREDENTIAL_URL__' = (Quote-Bash $credentialUrl)
        '__S3_BUCKET__' = (Quote-Bash $S3Bucket)
        '__S3_REGION__' = (Quote-Bash $S3Region)
        '__S3_ENDPOINT__' = (Quote-Bash $S3Endpoint)
        '__RAW_OBJECT_PREFIX__' = (Quote-Bash $script:RawObjectPrefix)
        '__SKIP_RAW_UPLOAD__' = (Quote-Bash ($SkipRawUpload.ToString().ToLowerInvariant()))
        '__COUNT__' = (Quote-Bash ([string]$SmallFileCount))
        '__SIZE__' = (Quote-Bash ([string]$SmallFileSizeBytes))
        '__MIN_SIZE__' = (Quote-Bash ([string]$SmallFileMinSizeBytes))
        '__MAX_SIZE__' = (Quote-Bash ([string]$SmallFileMaxSizeBytes))
        '__LEVELS__' = (Quote-Bash ([string]$DirLevels))
        '__FANOUT__' = (Quote-Bash ([string]$DirsPerLevel))
        '__FILES_PER_DIR__' = (Quote-Bash ([string]$FilesPerDir))
        '__PERF_TOOLS__' = (Quote-Bash $PerfTools)
        '__METADATA_CACHE_TTL_SEC__' = (Quote-Bash ([string]$MetadataCacheTtlSec))
        '__TOOL_TIMEOUT_SECONDS__' = (Quote-Bash ([string]$ToolTimeoutSeconds))
        '__META_BACKEND__' = (Quote-Bash $MetadataBackend)
        '__TIKV_VERSION__' = (Quote-Bash $TikvVersion)
        '__METADATA_LATENCY_MS__' = (Quote-Bash ([string]$MetadataLatencyMs))
        '__VOLUME_NAME__' = (Quote-Bash $script:JuiceFsVolumeName)
        '__DATA_PREFIX__' = (Quote-Bash $script:JuiceFsObjectPrefix)
    }
    foreach ($key in $values.Keys) { $remote = $remote.Replace($key, [string]$values[$key]) }
    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($remote))
    $run = Invoke-AliyunJson @('ecs', 'RunCommand', '--region', $S3Region, '--Type', 'RunShellScript', '--InstanceId.1', $InstanceId, '--CommandContent', $encoded, '--ContentEncoding', 'Base64', '--Timeout', '172800', '--KeepCommand', 'false', '--Name', 'brewfs-juicefs-native')
    $invokeId = [string]$run.InvokeId
    if (-not $invokeId) { throw 'RunCommand 未返回 InvokeId' }
    $deadline = (Get-Date).AddHours(48)
    while ((Get-Date) -lt $deadline) {
        $result = Invoke-AliyunJson @('ecs', 'DescribeInvocationResults', '--region', $S3Region, '--InvokeId', $invokeId)
        $item = @($result.Invocation.InvocationResults.InvocationResult)[0]
        if ($item) {
            Write-Host "invocation status=$($item.InvocationStatus)"
            if ($item.InvocationStatus -in @('Success', 'Failed', 'Stopped', 'Error', 'Terminated', 'Timeout')) {
                $decoded = if ($item.Output) { [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($item.Output)) } else { '' }
                Set-Content -LiteralPath (Join-Path $ArtifactDirectory 'remote-output.log') -Value $decoded -Encoding UTF8
                Write-Host $decoded
                if ($item.InvocationStatus -ne 'Success') { throw "远程 JuiceFS 测试失败: $($item.ErrorInfo)" }
                break
            }
        }
        Start-Sleep -Seconds 10
    }
}
finally {
    Remove-Item -LiteralPath $credentialPath -Force -ErrorAction SilentlyContinue
    if ($script:RawObjectPrefix -and -not $KeepRawObjects) {
        try { Invoke-Checked $Aliyun @('oss', 'rm', "oss://$S3Bucket/$($script:RawObjectPrefix)", '--region', $S3Region, '--recursive', '--force') | Out-Null } catch { Write-Warning $_ }
    }
    if ($script:JuiceFsObjectPrefix) {
        try {
            Invoke-Checked $Aliyun @('oss', 'rm', "oss://$S3Bucket/$($script:JuiceFsObjectPrefix)", '--region', $S3Region, '--recursive', '--force') | Out-Null
        } catch { Write-Warning "JuiceFS object prefix cleanup failed: $($_.Exception.Message)" }
    }
    if ($prefix) {
        try {
            Invoke-Checked $Aliyun @('oss', 'rm', "oss://$S3Bucket/$prefix", '--region', $S3Region, '--recursive', '--force') | Out-Null
        } catch { Write-Warning "JuiceFS run prefix cleanup failed: $($_.Exception.Message)" }
    }
    if ($script:CredentialKey) {
        try { Invoke-Checked $Aliyun @('oss', 'rm', "oss://$S3Bucket/$script:CredentialKey", '--region', $S3Region, '--force') | Out-Null } catch { Write-Warning $_ }
    }
}
