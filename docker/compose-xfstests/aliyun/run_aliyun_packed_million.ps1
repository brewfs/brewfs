[CmdletBinding()]
param(
    [ValidateSet('run', 'create', 'status', 'destroy')]
    [string]$Action = 'run',
    [string]$InstanceId,
    [string]$RegionId = 'cn-hangzhou',
    [string]$ZoneId = 'cn-hangzhou-h',
    [string]$ImageId = 'ubuntu_24_04_x64_20G_alibase_20260916.vhd',
    [string]$VSwitchId,
    [string]$SecurityGroupId,
    [string]$InstanceName,
    # ecs.u1-c1m4.2xlarge is the 32 GiB class used by this profile.
    [string]$InstanceType = 'ecs.u1-c1m4.2xlarge',
    [ValidateRange(40, 1000)]
    [int]$SystemDiskSizeGiB = 100,
    [int64]$SmallFileCount = 1000000,
    [int64]$SmallFileSizeBytes = 102400,
    [int64]$SmallFileMinSizeBytes = 0,
    [int64]$SmallFileMaxSizeBytes = 0,
    [ValidateRange(0, 8)]
    [int]$DirLevels = 3,
    [int64]$DirsPerLevel = 10,
    [int64]$FilesPerLeaf = 1000,
    [ValidateSet('random-small-file', 'sequential-small-file', 'mixed')]
    [string]$PackedAccessProfile = 'random-small-file',
    [int64]$FioFileSizeBytes = 67108864,
    [ValidateScript({
        $tools = @($_ -split '\s+' | Where-Object { $_ })
        if ($tools.Count -eq 0 -or @($tools | Where-Object { $_ -notin @('packed-tree', 'packed-stat', 'packed-smallfiles', 'packed-gpu-smallfiles') }).Count -ne 0) {
            throw 'Unsupported packed tool; tree/stat/smallfiles are supported, fio/POSIX fixture layout remains OPEN.'
        }
        $true
    })]
    [string]$PerfTools = 'packed-tree packed-smallfiles',
    [ValidateSet('packed-snapshot')]
    [string]$VolumeFormat = 'packed-snapshot',
    [int]$FioRuntimeSeconds = 20,
    [UInt64]$PackedFrameWindowCacheBytes = 0,
    [bool]$PackedFrameWindowPrefetch = $false,
    [UInt64]$PackedDecodedFrameCacheBytes = 0,
    [ValidateSet('0', '1')][string]$ReadDirectIo = '1',
    [ValidateSet('0', '1')][string]$FuseKeepCache = '0',
    [ValidateRange(0, 86400000)]
    [int64]$MetadataCacheTtlMs = 0,
    [UInt64]$PackedMetadataCacheBytes = 268435456,
    [ValidateSet('off', 'auto', 'eager')]
    [string]$PackedMetadataPrefetch = 'auto',
    [ValidateRange(30, 14400)]
    [int]$ToolTimeoutSeconds = 900,
    [string]$PackedExistingManifestKey,
    [switch]$PackedSkipFixture,
    [ValidateSet('full', 'prefix')]
    [string]$ReadMode = 'full',
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
    [switch]$SkipBuild,
    [string]$ArtifactDirectory,
    [string]$ObjectPrefix,
    [string]$Repository = 'https://github.com/brewfs/brewfs.git',
    [string]$Ref = 'main',
    [string]$AutoReleaseMinutes = '480',
    [switch]$KeepInstance,
    [switch]$NoCleanup,
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$expected = [int64]1
for ($level = 0; $level -lt $DirLevels; $level++) {
    $expected = $expected * $DirsPerLevel
}
$expected = $expected * $FilesPerLeaf
if ($expected -ne $SmallFileCount) {
    throw "SmallFileCount must equal DirsPerLevel^DirLevels*FilesPerLeaf: expected $expected, got $SmallFileCount."
}
if ($SmallFileSizeBytes -le 0 -or $SmallFileSizeBytes -gt 4MB) {
    throw 'SmallFileSizeBytes must be between 1 and 4 MiB.'
}
if ($SmallFileMinSizeBytes -le 0) { $SmallFileMinSizeBytes = $SmallFileSizeBytes }
if ($SmallFileMaxSizeBytes -le 0) { $SmallFileMaxSizeBytes = $SmallFileSizeBytes }
if ($SmallFileMinSizeBytes -gt $SmallFileMaxSizeBytes -or $SmallFileMaxSizeBytes -gt 4MB) {
    throw 'SmallFileMinSizeBytes/MaxSizeBytes must satisfy 0 < min <= max <= 4 MiB.'
}

$scriptPath = Join-Path $PSScriptRoot 'run_aliyun_perf.ps1'
if (-not (Test-Path -LiteralPath $scriptPath)) {
    throw "Missing shared Aliyun runner: $scriptPath"
}

$readBytes = if ($ReadMode -eq 'full') { '0' } else { '1' }
$runnerParams = @{
    Action = $Action
    RegionId = $RegionId
    ZoneId = $ZoneId
    ImageId = $ImageId
    InstanceType = $InstanceType
    SystemDiskSizeGiB = $SystemDiskSizeGiB
    Backend = 'none'
    DataBackend = 's3'
    VolumeFormat = $VolumeFormat
    PerfTools = $PerfTools
    FioRuntimeSeconds = $FioRuntimeSeconds
    PackedFrameWindowCacheBytes = $PackedFrameWindowCacheBytes
    PackedFrameWindowPrefetch = $PackedFrameWindowPrefetch
    PackedDecodedFrameCacheBytes = $PackedDecodedFrameCacheBytes
    ReadDirectIo = $ReadDirectIo
    FuseKeepCache = $FuseKeepCache
    MetadataCacheTtlMs = $MetadataCacheTtlMs
    PackedMetadataCacheBytes = $PackedMetadataCacheBytes
    PackedMetadataPrefetch = $PackedMetadataPrefetch
    ToolTimeoutSeconds = $ToolTimeoutSeconds
    PackedSmallFileCount = $SmallFileCount
    PackedSmallFileSizeBytes = $SmallFileSizeBytes
    PackedSmallFileMinSizeBytes = $SmallFileMinSizeBytes
    PackedSmallFileMaxSizeBytes = $SmallFileMaxSizeBytes
    PackedDirLevels = $DirLevels
    PackedDirsPerLevel = $DirsPerLevel
    PackedFilesPerDir = $FilesPerLeaf
    PackedAccessProfile = $PackedAccessProfile
    PackedFioFileSizeBytes = $FioFileSizeBytes
    PackedSmallFileReadBytes = $readBytes
    PackedExistingManifestKey = $PackedExistingManifestKey
    PackedSkipFixture = $PackedSkipFixture
    S3Bucket = $S3Bucket
    S3Endpoint = $S3Endpoint
    S3Region = $S3Region
    S3AccessKey = $S3AccessKey
    S3SecretKey = $S3SecretKey
    S3ForcePathStyle = $S3ForcePathStyle
    RepoRoot = $RepoRoot
    WslDistribution = $WslDistribution
    BinaryPath = $BinaryPath
    FixtureBinaryPath = $FixtureBinaryPath
    SkipBuild = $SkipBuild
    ArtifactDirectory = $ArtifactDirectory
    ObjectPrefix = $ObjectPrefix
    Repository = $Repository
    Ref = $Ref
    AutoReleaseMinutes = $AutoReleaseMinutes
    ColdRead = $true
}
$runnerArgs = @(
    '-Action', $Action,
    '-RegionId', $RegionId,
    '-ZoneId', $ZoneId,
    '-ImageId', $ImageId,
    '-InstanceType', $InstanceType,
    '-SystemDiskSizeGiB', [string]$SystemDiskSizeGiB,
    '-Backend', 'none',
    '-DataBackend', 's3',
    '-VolumeFormat', $VolumeFormat,
    '-PerfTools', $PerfTools,
    '-FioRuntimeSeconds', [string]$FioRuntimeSeconds,
    '-PackedFrameWindowCacheBytes', [string]$PackedFrameWindowCacheBytes,
    '-PackedDecodedFrameCacheBytes', [string]$PackedDecodedFrameCacheBytes,
    '-ReadDirectIo', $ReadDirectIo,
    '-FuseKeepCache', $FuseKeepCache,
    '-MetadataCacheTtlMs', [string]$MetadataCacheTtlMs,
    '-PackedMetadataCacheBytes', [string]$PackedMetadataCacheBytes,
    '-PackedMetadataPrefetch', $PackedMetadataPrefetch,
    '-ToolTimeoutSeconds', [string]$ToolTimeoutSeconds,
    '-PackedSmallFileCount', [string]$SmallFileCount,
    '-PackedSmallFileSizeBytes', [string]$SmallFileSizeBytes,
    '-PackedSmallFileMinSizeBytes', [string]$SmallFileMinSizeBytes,
    '-PackedSmallFileMaxSizeBytes', [string]$SmallFileMaxSizeBytes,
    '-PackedDirLevels', [string]$DirLevels,
    '-PackedDirsPerLevel', [string]$DirsPerLevel,
    '-PackedFilesPerDir', [string]$FilesPerLeaf,
    '-PackedAccessProfile', $PackedAccessProfile,
    '-PackedSmallFileReadBytes', $readBytes,
    '-PackedExistingManifestKey', $PackedExistingManifestKey,
    '-PackedFioFileSizeBytes', [string]$FioFileSizeBytes,
    '-S3Region', $S3Region,
    '-Repository', $Repository,
    '-Ref', $Ref,
    '-AutoReleaseMinutes', $AutoReleaseMinutes,
    '-ColdRead'
)

foreach ($name in @('InstanceId', 'VSwitchId', 'SecurityGroupId', 'InstanceName')) {
    $value = Get-Variable -Name $name -ValueOnly
    if ($value) {
        $runnerParams[$name] = $value
        $runnerArgs += "-$name"
        $runnerArgs += [string]$value
    }
}
if ($S3Bucket) { $runnerArgs += '-S3Bucket'; $runnerArgs += $S3Bucket }
if ($S3Endpoint) { $runnerArgs += '-S3Endpoint'; $runnerArgs += $S3Endpoint }
if ($S3AccessKey) { $runnerArgs += '-S3AccessKey'; $runnerArgs += $S3AccessKey }
if ($S3SecretKey) { $runnerArgs += '-S3SecretKey'; $runnerArgs += $S3SecretKey }
if ($S3ForcePathStyle) { $runnerArgs += '-S3ForcePathStyle' }
if ($RepoRoot) { $runnerArgs += '-RepoRoot'; $runnerArgs += $RepoRoot }
if ($WslDistribution) { $runnerArgs += '-WslDistribution'; $runnerArgs += $WslDistribution }
if ($BinaryPath) { $runnerArgs += '-BinaryPath'; $runnerArgs += $BinaryPath }
if ($FixtureBinaryPath) { $runnerArgs += '-FixtureBinaryPath'; $runnerArgs += $FixtureBinaryPath }
if ($SkipBuild) { $runnerArgs += '-SkipBuild' }
if ($PackedSkipFixture) { $runnerArgs += '-PackedSkipFixture' }
if ($ArtifactDirectory) { $runnerArgs += '-ArtifactDirectory'; $runnerArgs += $ArtifactDirectory }
if ($ObjectPrefix) { $runnerArgs += '-ObjectPrefix'; $runnerArgs += $ObjectPrefix }
if ($KeepInstance) {
    $runnerParams.KeepInstance = $true
    $runnerArgs += '-KeepInstance'
}
if ($NoCleanup) {
    $runnerParams.NoCleanup = $true
    $runnerArgs += '-NoCleanup'
}

Write-Host 'Aliyun packed million-small-file profile'
Write-Host "  instance_type=$InstanceType"
Write-Host "  system_disk=${SystemDiskSizeGiB}GiB ESSD"
Write-Host "  image=$ImageId"
Write-Host "  files=$SmallFileCount file_size=${SmallFileMinSizeBytes}-${SmallFileMaxSizeBytes} bytes read_mode=$ReadMode"
Write-Host "  hierarchy=${DirLevels} levels x ${DirsPerLevel} dirs/level x ${FilesPerLeaf} files/leaf"
Write-Host '  data cache: disabled; cold-read/drop-caches checks: enabled'

if ($DryRun) {
    Write-Host 'Dry run: no Aliyun API call was made.'
    Write-Host ("powershell -File `"{0}`" {1}" -f $scriptPath, ($runnerArgs -join ' '))
    exit 0
}

& $scriptPath @runnerParams
if ($LASTEXITCODE -ne 0) {
    throw "shared Aliyun runner failed with exit code $LASTEXITCODE"
}
