[CmdletBinding()]
param(
    [ValidateSet('run', 'create', 'status', 'destroy')]
    [string]$Action = 'run',
    [string]$InstanceId,
    [string]$RegionId = 'cn-hangzhou',
    [string]$ZoneId = 'cn-hangzhou-i',
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
    [ValidateRange(1, 32)]
    [int]$DirLevels = 3,
    [int64]$DirsPerLevel = 10,
    [int64]$FilesPerLeaf = 1000,
    [ValidateSet('full', 'prefix')]
    [string]$ReadMode = 'full',
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

$scriptPath = Join-Path $PSScriptRoot 'run_aliyun_perf.ps1'
if (-not (Test-Path -LiteralPath $scriptPath)) {
    throw "Missing shared Aliyun runner: $scriptPath"
}

$readBytes = if ($ReadMode -eq 'full') { '0' } else { '1' }
$runnerArgs = @(
    '-Action', $Action,
    '-RegionId', $RegionId,
    '-ZoneId', $ZoneId,
    '-ImageId', $ImageId,
    '-InstanceType', $InstanceType,
    '-SystemDiskSizeGiB', [string]$SystemDiskSizeGiB,
    '-Backend', 'redis',
    '-DataBackend', 's3',
    '-VolumeFormat', 'packed-metadata-v1',
    '-PerfTools', 'packed-smallfiles packed-posix',
    '-PackedSmallFileCount', [string]$SmallFileCount,
    '-PackedSmallFileSizeBytes', [string]$SmallFileSizeBytes,
    '-PackedDirLevels', [string]$DirLevels,
    '-PackedDirsPerLevel', [string]$DirsPerLevel,
    '-PackedFilesPerDir', [string]$FilesPerLeaf,
    '-PackedSmallFileReadBytes', $readBytes,
    '-Repository', $Repository,
    '-Ref', $Ref,
    '-AutoReleaseMinutes', $AutoReleaseMinutes,
    '-ColdRead'
)

foreach ($name in @('InstanceId', 'VSwitchId', 'SecurityGroupId', 'InstanceName')) {
    $value = Get-Variable -Name $name -ValueOnly
    if ($value) {
        $runnerArgs += "-$name"
        $runnerArgs += [string]$value
    }
}
if ($KeepInstance) { $runnerArgs += '-KeepInstance' }
if ($NoCleanup) { $runnerArgs += '-NoCleanup' }

Write-Host 'Aliyun packed million-small-file profile'
Write-Host "  instance_type=$InstanceType"
Write-Host "  system_disk=${SystemDiskSizeGiB}GiB ESSD"
Write-Host "  image=$ImageId"
Write-Host "  files=$SmallFileCount file_size=$SmallFileSizeBytes bytes read_mode=$ReadMode"
Write-Host "  hierarchy=${DirLevels} levels x ${DirsPerLevel} dirs/level x ${FilesPerLeaf} files/leaf"
Write-Host '  data cache: disabled; cold-read/drop-caches checks: enabled'

if ($DryRun) {
    Write-Host 'Dry run: no Aliyun API call was made.'
    Write-Host ("powershell -File `"{0}`" {1}" -f $scriptPath, ($runnerArgs -join ' '))
    exit 0
}

& $scriptPath @runnerArgs
exit $LASTEXITCODE
