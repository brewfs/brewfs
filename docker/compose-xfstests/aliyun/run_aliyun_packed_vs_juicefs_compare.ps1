[CmdletBinding()]
param(
    [string]$RegionId = 'cn-hangzhou',
    [string]$ZoneId = 'cn-hangzhou-h',
    [string]$VSwitchId,
    [string]$SecurityGroupId,
    [string]$InstanceType = 'ecs.u1-c1m4.2xlarge',
    [ValidateRange(40, 1000)][int]$SystemDiskSizeGiB = 100,
    [string]$ImageId = 'ubuntu_24_04_x64_20G_alibase_20260916.vhd',
    [int64]$SmallFileCount = 100000,
    [int64]$SmallFileSizeBytes = 102400,
    [int64]$SmallFileMinSizeBytes = 102400,
    [int64]$SmallFileMaxSizeBytes = 1048576,
    [ValidateRange(0, 8)]
    [int]$DirLevels = 2,
    [int64]$DirsPerLevel = 10,
    [int64]$FilesPerLeaf = 1000,
    [ValidateSet('random-small-file', 'sequential-small-file', 'mixed')]
    [string]$PackedAccessProfile = 'random-small-file',
    [string]$S3Bucket,
    [string]$S3Endpoint = 'https://oss-cn-hangzhou-internal.aliyuncs.com',
    [string]$S3Region = 'cn-hangzhou',
    [string]$S3AccessKey,
    [string]$S3SecretKey,
    [string]$ObjectPrefix = ('brewfs-v3-jfs-{0}' -f (Get-Date -Format 'yyyyMMdd-HHmmss')),
    [string]$JuiceFsBinaryPath,
    [string]$RawFixtureBinaryPath,
    [string]$BinaryPath,
    [string]$FixtureBinaryPath,
    [string]$RepoRoot,
    [string]$ArtifactDirectory,
    [string]$WslDistribution = 'Ubuntu-24.04',
    [ValidateSet('', 'redis', 'tikv')][string]$MetadataBackend = '',
    [ValidateSet('redis', 'tikv')][string[]]$MetadataBackends = @('redis', 'tikv'),
    [string]$TikvVersion = 'v6.5.3',
    [ValidateScript({
        $tools = @($_ -split '\s+' | Where-Object { $_ })
        if ($tools.Count -eq 0 -or @($tools | Where-Object { $_ -notin @('packed-tree', 'packed-stat', 'packed-smallfiles', 'packed-gpu-smallfiles') }).Count -ne 0) {
            throw 'Unsupported packed tool; tree/stat/smallfiles are supported, fio/POSIX fixture layout remains OPEN.'
        }
        $true
    })]
    [string]$PerfTools = 'packed-tree packed-smallfiles',
    [string]$JuiceFsPerfTools = 'juicefs-tree juicefs-smallfiles',
    [UInt64]$PackedFrameWindowCacheBytes = 0,
    [bool]$PackedFrameWindowPrefetch = $false,
    [UInt64]$PackedDecodedFrameCacheBytes = 0,
    [UInt64]$PackedMetadataCacheBytes = 268435456,
    [ValidateSet('off', 'auto', 'eager')]
    [string]$PackedMetadataPrefetch = 'auto',
    [ValidateRange(30, 7200)][int]$ToolTimeoutSeconds = 7200,
    [string]$AutoReleaseMinutes = '180',
    [switch]$SkipBuild,
    [switch]$KeepInstance,
    [switch]$NoCleanup,
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if (-not $VSwitchId -or -not $SecurityGroupId) {
    throw '必须指定 -VSwitchId 和 -SecurityGroupId。'
}
if (-not $S3Bucket) { throw '必须指定 -S3Bucket。' }
$expected = [int64]1
for ($level = 0; $level -lt $DirLevels; $level++) { $expected *= $DirsPerLevel }
$expected *= $FilesPerLeaf
if ($expected -ne $SmallFileCount) {
    throw "SmallFileCount must equal DirsPerLevel^DirLevels*FilesPerLeaf: expected $expected, got $SmallFileCount."
}
if ($SmallFileMinSizeBytes -le 0 -or $SmallFileMinSizeBytes -gt $SmallFileMaxSizeBytes -or $SmallFileMaxSizeBytes -gt 4MB) {
    throw 'SmallFileMinSizeBytes/MaxSizeBytes must satisfy 0 < min <= max <= 4 MiB.'
}

$scriptDir = $PSScriptRoot
$packedScript = Join-Path $scriptDir 'run_aliyun_packed_million.ps1'
$juiceScript = Join-Path $scriptDir 'run_aliyun_juicefs_compare.ps1'
if (-not (Test-Path -LiteralPath $packedScript)) { throw "Missing packed runner: $packedScript" }
if (-not (Test-Path -LiteralPath $juiceScript)) { throw "Missing JuiceFS runner: $juiceScript" }

function Resolve-Aliyun {
    $command = Get-Command aliyun -ErrorAction SilentlyContinue
    if ($command) { return $command.Source }
    if ($env:LOCALAPPDATA) {
        $candidate = Join-Path $env:LOCALAPPDATA 'AliyunCLI\aliyun.exe'
        if (Test-Path -LiteralPath $candidate) { return $candidate }
    }
    throw '找不到 aliyun CLI。'
}

function Invoke-OssRm([string]$Prefix) {
    if (-not $Prefix) { return }
    try {
        & $script:Aliyun oss rm "oss://$S3Bucket/$Prefix" --region $S3Region --recursive --force 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) { Write-Warning "清理 OSS 前缀失败: $Prefix" }
    } catch { Write-Warning "清理 OSS 前缀失败: $Prefix; $($_.Exception.Message)" }
}

function Remove-TestInstance([string]$InstanceId) {
    if (-not $InstanceId) { return }
    try {
        $raw = & $script:Aliyun ecs DescribeInstances --region $RegionId --InstanceIds (('["{0}"]' -f $InstanceId)) 2>&1
        if ($LASTEXITCODE -ne 0) { throw ($raw -join ' ') }
        $state = (($raw -join [Environment]::NewLine) | ConvertFrom-Json).Instances.Instance
        if (-not $state) { return }
        if ($state.Status -notin @('Stopped', 'Stopping')) {
            & $script:Aliyun ecs StopInstance --region $RegionId --InstanceId $InstanceId --ForceStop true 2>&1 | Out-Null
        }
        for ($i = 0; $i -lt 60; $i++) {
            $raw = & $script:Aliyun ecs DescribeInstances --region $RegionId --InstanceIds (('["{0}"]' -f $InstanceId)) 2>&1
            if ($LASTEXITCODE -eq 0) {
                $state = (($raw -join [Environment]::NewLine) | ConvertFrom-Json).Instances.Instance
                if (-not $state -or $state.Status -eq 'Stopped') { break }
            }
            Start-Sleep -Seconds 5
        }
        for ($i = 0; $i -lt 18; $i++) {
            & $script:Aliyun ecs DeleteInstance --region $RegionId --InstanceId $InstanceId --Force true 2>&1 | Out-Null
            if ($LASTEXITCODE -eq 0) {
                Write-Host "Cleanup requested for ECS $InstanceId"
                return
            }
            Start-Sleep -Seconds 5
        }
        Write-Warning "ECS delete did not succeed after retries: $InstanceId"
    } catch {
        Write-Warning "ECS cleanup failed: $($_.Exception.Message)"
    }
}

function Invoke-Runner([string]$Path, [hashtable]$Parameters, [string]$LogPath) {
    $raw = @(& $Path @Parameters 2>&1 6>&1)
    $status = $LASTEXITCODE
    $text = ($raw | Out-String)
    [IO.File]::WriteAllText((Resolve-Path -LiteralPath (Split-Path -Parent $LogPath) | ForEach-Object { Join-Path $_ (Split-Path -Leaf $LogPath) }), $text, [Text.Encoding]::UTF8)
    if ($text) { Write-Host $text.TrimEnd() }
    if ($status -ne 0) {
        throw "runner failed: $Path (exit=$status)"
    }
    return $text
}

$script:Aliyun = Resolve-Aliyun
$rootPrefix = $ObjectPrefix.TrimEnd('/')
$packedPrefix = "$rootPrefix/packed"
$rawPrefix = "$rootPrefix/juicefs/raw"
$selectedBackends = if ($MetadataBackend) { @($MetadataBackend) } else { @($MetadataBackends) }
if (-not $selectedBackends -or $selectedBackends.Count -eq 0) { throw 'At least one JuiceFS metadata backend is required.' }
$artifactRoot = if ($ArtifactDirectory) { $ArtifactDirectory } else { Join-Path $scriptDir '..\artifacts\aliyun-packed-vs-juicefs' }
New-Item -ItemType Directory -Force -Path $artifactRoot | Out-Null
$createLog = Join-Path $artifactRoot 'create.log'
$packedLog = Join-Path $artifactRoot 'packed-run.log'
$script:InstanceId = $null

if ($DryRun) {
    Write-Host "Dry run: one ECS, packed prefix=$packedPrefix, raw prefix=$rawPrefix, backends=$($selectedBackends -join ',')"
    Write-Host "  files=$SmallFileCount size=${SmallFileMinSizeBytes}-${SmallFileMaxSizeBytes} levels=$DirLevels fanout=$DirsPerLevel files_per_leaf=$FilesPerLeaf"
    exit 0
}

try {
    $createParams = @{
        Action = 'create'
        RegionId = $RegionId
        ZoneId = $ZoneId
        VSwitchId = $VSwitchId
        SecurityGroupId = $SecurityGroupId
        InstanceType = $InstanceType
        SystemDiskSizeGiB = $SystemDiskSizeGiB
        ImageId = $ImageId
        SmallFileCount = $SmallFileCount
        SmallFileSizeBytes = $SmallFileSizeBytes
        SmallFileMinSizeBytes = $SmallFileMinSizeBytes
        SmallFileMaxSizeBytes = $SmallFileMaxSizeBytes
        DirLevels = $DirLevels
        DirsPerLevel = $DirsPerLevel
        FilesPerLeaf = $FilesPerLeaf
        PackedAccessProfile = $PackedAccessProfile
        AutoReleaseMinutes = $AutoReleaseMinutes
    }
    $createdOutput = Invoke-Runner $packedScript $createParams $createLog
    $match = [regex]::Match([string]$createdOutput, '(?m)\b(i-[A-Za-z0-9-]+)\b')
    if (-not $match.Success) { throw "无法从创建输出解析 InstanceId，请检查 $createLog" }
    $script:InstanceId = $match.Groups[1].Value
    Write-Host "Using shared ECS instance: $script:InstanceId"

    $packedParams = @{
        Action = 'run'
        InstanceId = $script:InstanceId
        RegionId = $RegionId
        ZoneId = $ZoneId
        VSwitchId = $VSwitchId
        SecurityGroupId = $SecurityGroupId
        InstanceType = $InstanceType
        SystemDiskSizeGiB = $SystemDiskSizeGiB
        ImageId = $ImageId
        SmallFileCount = $SmallFileCount
        SmallFileSizeBytes = $SmallFileSizeBytes
        SmallFileMinSizeBytes = $SmallFileMinSizeBytes
        SmallFileMaxSizeBytes = $SmallFileMaxSizeBytes
        DirLevels = $DirLevels
        DirsPerLevel = $DirsPerLevel
        FilesPerLeaf = $FilesPerLeaf
        S3Bucket = $S3Bucket
        S3Endpoint = $S3Endpoint
        S3Region = $S3Region
        S3AccessKey = $S3AccessKey
        S3SecretKey = $S3SecretKey
        ObjectPrefix = $packedPrefix
        PerfTools = $PerfTools
        PackedFrameWindowCacheBytes = $PackedFrameWindowCacheBytes
        PackedFrameWindowPrefetch = $PackedFrameWindowPrefetch
        PackedDecodedFrameCacheBytes = $PackedDecodedFrameCacheBytes
        PackedMetadataCacheBytes = $PackedMetadataCacheBytes
        PackedMetadataPrefetch = $PackedMetadataPrefetch
        VolumeFormat = 'packed-snapshot'
        ToolTimeoutSeconds = $ToolTimeoutSeconds
        RepoRoot = $RepoRoot
        WslDistribution = $WslDistribution
        BinaryPath = $BinaryPath
        FixtureBinaryPath = $FixtureBinaryPath
        SkipBuild = $SkipBuild
        ArtifactDirectory = (Join-Path $artifactRoot 'packed')
        AutoReleaseMinutes = $AutoReleaseMinutes
        KeepInstance = $true
    }
    Invoke-Runner $packedScript $packedParams $packedLog | Out-Null

    for ($backendIndex = 0; $backendIndex -lt $selectedBackends.Count; $backendIndex++) {
        $backend = [string]$selectedBackends[$backendIndex]
        $juicePrefix = "$rootPrefix/juicefs/$backend"
        $juiceArtifact = Join-Path $artifactRoot "juicefs-$backend"
        $juiceLog = Join-Path $artifactRoot "juicefs-$backend-run.log"
        $juiceParams = @{
            InstanceId = $script:InstanceId
            S3Bucket = $S3Bucket
            S3Region = $S3Region
            S3Endpoint = $S3Endpoint
            S3AccessKey = $S3AccessKey
            S3SecretKey = $S3SecretKey
            ObjectPrefix = $juicePrefix
            RawObjectPrefix = $rawPrefix
            SkipRawUpload = ($backendIndex -gt 0)
            KeepRawObjects = ($backendIndex -lt $selectedBackends.Count - 1)
            JuiceFsBinaryPath = $JuiceFsBinaryPath
            RawFixtureBinaryPath = $RawFixtureBinaryPath
            RunnerPath = (Join-Path $scriptDir 'run_aliyun_juicefs_native.sh')
            MetadataBackend = $backend
            TikvVersion = $TikvVersion
            SmallFileCount = $SmallFileCount
            SmallFileSizeBytes = $SmallFileSizeBytes
            SmallFileMinSizeBytes = $SmallFileMinSizeBytes
            SmallFileMaxSizeBytes = $SmallFileMaxSizeBytes
            DirLevels = $DirLevels
            DirsPerLevel = $DirsPerLevel
            FilesPerDir = $FilesPerLeaf
            PerfTools = $JuiceFsPerfTools
            ToolTimeoutSeconds = $ToolTimeoutSeconds
            ArtifactDirectory = $juiceArtifact
        }
        Invoke-Runner $juiceScript $juiceParams $juiceLog | Out-Null
    }
    Write-Host "Comparison completed. Artifacts: $artifactRoot"
}
finally {
    if (-not $NoCleanup) {
        Invoke-OssRm $rootPrefix
        if ($script:InstanceId -and -not $KeepInstance) { Remove-TestInstance $script:InstanceId }
    } else {
        Write-Host "NoCleanup requested; ECS/object prefixes retained. instance=$script:InstanceId prefix=$rootPrefix"
    }
}
