[CmdletBinding()]
param(
    [ValidateSet('run', 'create', 'status', 'destroy')]
    [string]$Action = 'run',
    [string]$InstanceId,
    [string]$RegionId = 'cn-hangzhou',
    [string]$ZoneId = 'cn-hangzhou-h',
    [string]$VSwitchId = $env:BREWFS_PERF_VSWITCH_ID,
    [string]$SecurityGroupId = $env:BREWFS_PERF_SECURITY_GROUP_ID,
    [string]$InstanceName,
    [string]$InstanceType = 'ecs.u1-c1m2.2xlarge',
    [string]$ImageId = 'ubuntu_24_04_x64_20G_alibase_20260522.vhd',
    [ValidateSet('cloud_essd', 'cloud_essd_entry', 'cloud_auto')]
    [string]$DataDiskCategory = 'cloud_essd',
    [ValidateSet('PL0', 'PL1', 'PL2', 'PL3')]
    [string]$DataDiskPerformanceLevel = 'PL2',
    [ValidateRange(100, 32768)]
    [int]$DataDiskSize = 512,
    [string]$DataDiskDevice = '/dev/vdb',
    [string]$DataRoot = '/mnt/brewfs-perf-data',
    [string]$DockerRegistryMirror = 'https://docker.m.daocloud.io',
    [ValidateSet('redis', 'tikv')]
    [string]$Backend = 'redis',
    [ValidateSet('brewfs', 'juicefs')]
    [string]$Workload = 'brewfs',
    [ValidateSet('s3', 'local-fs')]
    [string]$DataBackend = 's3',
    [switch]$ManagedBackend,
    [string]$MetaUrl = $env:BREWFS_PERF_META_URL,
    [string]$S3Endpoint = $env:BREWFS_PERF_S3_ENDPOINT,
    [string]$S3Bucket = $env:BREWFS_PERF_S3_BUCKET,
    [string]$S3Region = $env:BREWFS_PERF_S3_REGION,
    [string]$S3AccessKey = $env:BREWFS_PERF_S3_ACCESS_KEY_ID,
    [string]$S3SecretKey = $env:BREWFS_PERF_S3_SECRET_ACCESS_KEY,
    [string]$PerfTools = 'fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest',
    [string]$Repository = 'https://github.com/brewfs/brewfs.git',
    [string]$Ref = 'main',
    [string]$SourceArchiveUrl,
    [string]$SourceArchiveSha256,
    [string]$BinaryPath,
    [switch]$UsePerfImage,
    [string]$ResultVaultUrl = $env:BREWFS_RESULTS_URL,
    [string]$ResultVaultResolveIp = $env:BREWFS_RESULTS_RESOLVE_IP,
    [string]$AutoReleaseMinutes = '240',
    # Optional suffix appended to the Result Vault run name, for example "8g",
    # so an instance-size experiment stays distinguishable in the run list.
    [string]$RunLabel = '',
    # The writeback throughput profile has to reach both filesystems. Only the
    # JuiceFS leg used to get it, which made the previous round's write-side
    # numbers and fio-bigread incomparable. Pass '' to skip it on both legs.
    [string]$WorkloadProfileArgs = '--writeback-throughput-profile',
    [switch]$RunBench,
    [switch]$KeepInstance,
    [switch]$NoCleanup
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$script:CreatedInstance = $false
$script:BinaryUrl = $null
$script:BinarySha256 = $null
$script:BinaryRunId = $null
$script:BinaryZipPath = $null
$script:BinaryIsArchive = $false
if ($env:BREWFS_PERF_IMAGE_ID -and $ImageId -eq 'ubuntu_24_04_x64_20G_alibase_20260522.vhd') {
    $ImageId = $env:BREWFS_PERF_IMAGE_ID
}

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
$script:Aliyun = $null

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
    $attempt = 0
    $consecutiveFailures = 0
    $lastError = $null
    do {
        $attempt++
        try {
            if (& $Condition) { return }
            $consecutiveFailures = 0
        } catch {
            # Surface the first failure of each kind: an API typo or an
            # undecodable response used to hide here and the wait spun silently
            # until the timeout, leaking the instance it was watching.
            $consecutiveFailures++
            if ($_.Exception.Message -ne $lastError) {
                $lastError = $_.Exception.Message
                Write-Warning "waiting for ${Description} (attempt ${attempt}): $lastError"
            }
            if ($consecutiveFailures -ge 12) {
                throw "等待 $Description 连续失败 ${consecutiveFailures} 次: $lastError"
            }
        }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)
    throw "等待超时: $Description"
}

function Quote-Bash([string]$Value) {
    $replacement = "'" + '"' + "'" + '"' + "'"
    return "'" + $Value.Replace("'", $replacement) + "'"
}

function Publish-BinaryArchiveToOss {
    if (-not $S3Bucket -or -not $S3Region -or $S3Endpoint -notmatch 'aliyuncs\.com') {
        throw 'Result Vault 上传失败，且当前配置不能使用 Aliyun OSS 中转二进制。'
    }
    if (-not $script:Aliyun) { $script:Aliyun = Resolve-Executable 'aliyun' $aliyunCandidates }
    $objectKey = '_brewfs-perf/brewfs-upload-{0}.zip' -f ([Guid]::NewGuid().ToString('N'))
    $objectUrl = "oss://$S3Bucket/$objectKey"
    Write-Warning 'Result Vault 二进制上传失败，改用本轮私有 OSS bucket 中转。'
    Invoke-Checked $script:Aliyun @(
        'oss', 'cp', $script:BinaryZipPath, $objectUrl,
        '-f', '--region', $S3Region, '--cli-non-interactive'
    ) | Out-Null
    $signedOutput = Invoke-Checked $script:Aliyun @(
        'oss', 'sign', $objectUrl, '--timeout', '10800', '--region', $S3Region
    )
    $signedUrl = @($signedOutput -split "`r?`n" | Where-Object { $_ -match '^https?://' })[0]
    if (-not $signedUrl) { throw 'Aliyun OSS 未返回二进制归档签名 URL。' }
    $script:BinaryUrl = $signedUrl
    $script:BinaryIsArchive = $true
    Write-Host "BrewFS 临时 OSS 归档已就绪: $objectUrl"
}

function New-BinaryUpload {
    if (-not $BinaryPath) { return }
    if (-not (Test-Path -LiteralPath $BinaryPath -PathType Leaf)) {
        throw "BinaryPath 不存在: $BinaryPath"
    }
    $binary = Get-Item -LiteralPath $BinaryPath
    if ($binary.Length -lt 1024) { throw "BinaryPath 看起来不是有效的 BrewFS 二进制: $BinaryPath" }
    if (-not $ResultVaultUrl) { throw '上传 BrewFS 二进制需要 ResultVaultUrl。' }
    $hash = (Get-FileHash -LiteralPath $binary.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $script:BinaryZipPath = Join-Path ([IO.Path]::GetTempPath()) "brewfs-upload-$([Guid]::NewGuid().ToString('N')).zip"
    $zip = [IO.Compression.ZipFile]::Open($script:BinaryZipPath, [IO.Compression.ZipArchiveMode]::Create)
    try {
        $entry = $zip.CreateEntry('brewfs', [IO.Compression.CompressionLevel]::Optimal)
        $source = [IO.File]::OpenRead($binary.FullName)
        $destination = $entry.Open()
        try { $source.CopyTo($destination) } finally { $destination.Dispose(); $source.Dispose() }
        # The VM image bakes a snapshot of the harness. Shipping the current
        # harness in the same upload keeps runner fixes usable without rebuilding
        # the image; the remote command installs these over the baked copies.
        $parent = Split-Path -Path $PSScriptRoot -Parent
        foreach ($item in @(
            @{ Entry = 'run_native_perf.sh'; Path = (Join-Path $PSScriptRoot 'run_native_perf.sh') },
            @{ Entry = 'run_perf_in_container.sh'; Path = (Join-Path $parent 'run_perf_in_container.sh') },
            @{ Entry = 'run_juicefs_perf_in_container.sh'; Path = (Join-Path $parent 'run_juicefs_perf_in_container.sh') },
            @{ Entry = 'perf_metadata_fallback.py'; Path = (Join-Path $parent 'perf_metadata_fallback.py') },
            @{ Entry = 'perf_manifest.py'; Path = (Join-Path $parent '..\..\tools\perf\perf_manifest.py') }
        )) {
            if (-not (Test-Path -LiteralPath $item.Path -PathType Leaf)) {
                Write-Warning "harness 文件缺失，未随二进制上传: $($item.Path)"
                continue
            }
            $harnessEntry = $zip.CreateEntry($item.Entry, [IO.Compression.CompressionLevel]::Optimal)
            # Windows checkouts with core.autocrlf=true hand us CRLF text. Every
            # harness file is a bash script or a Python helper, so shipping the
            # bytes verbatim makes the VM die on `$'\r': command not found`.
            # Normalize to LF so the payload does not depend on the line endings
            # this checkout happens to have.
            $harnessText = ([IO.File]::ReadAllText($item.Path) -replace "`r`n", "`n") -replace "`r", "`n"
            $harnessBytes = [Text.Encoding]::UTF8.GetBytes($harnessText)
            $harnessDestination = $harnessEntry.Open()
            try { $harnessDestination.Write($harnessBytes, 0, $harnessBytes.Length) } finally { $harnessDestination.Dispose() }
        }
    } finally {
        $zip.Dispose()
    }
    Write-Host "通过 Result Vault 上传 BrewFS 二进制: $($binary.FullName) ($($binary.Length) bytes)"
    $curl = Resolve-Executable 'curl.exe'
    $script:BinarySha256 = $hash
    try {
        $response = Invoke-Checked $curl ((Get-ResultVaultCurlArguments) + @(
            '-F', "archive=@$($script:BinaryZipPath);filename=brewfs-upload.zip",
            "$($ResultVaultUrl.TrimEnd('/'))/api/runs"
        ))
        $run = ($response -join [Environment]::NewLine | ConvertFrom-Json)
        $script:BinaryRunId = $run.id
        if (-not $script:BinaryRunId) { throw 'Result Vault 二进制上传未返回 run id。' }
        $script:BinaryUrl = "$($ResultVaultUrl.TrimEnd('/'))/api/runs/$($script:BinaryRunId)/files/brewfs"
        $script:BinaryIsArchive = $false
    } catch {
        Write-Warning $_.Exception.Message
        Publish-BinaryArchiveToOss
    }
    try {
        $zipSize = (Get-Item -LiteralPath $script:BinaryZipPath).Length
        Write-Host "BrewFS 临时上传已就绪: run=$($script:BinaryRunId), zip=$zipSize bytes"
    } catch {
        Write-Host "BrewFS 临时上传已就绪: run=$($script:BinaryRunId)"
    }
}

function Remove-BinaryUpload {
    if ($script:BinaryRunId) {
        try {
            $curl = Resolve-Executable 'curl.exe'
            Invoke-Checked $curl ((Get-ResultVaultCurlArguments) + @('-X', 'DELETE',
                "$($ResultVaultUrl.TrimEnd('/'))/api/runs/$($script:BinaryRunId)")) | Out-Null
            Write-Host "Result Vault 临时二进制记录已删除: $($script:BinaryRunId)"
        } catch {
            Write-Warning "Result Vault 临时二进制记录清理失败，请手动删除 run $($script:BinaryRunId): $($_.Exception.Message)"
        }
    }
    if ($script:BinaryZipPath -and (Test-Path -LiteralPath $script:BinaryZipPath)) {
        Remove-Item -LiteralPath $script:BinaryZipPath -Force -ErrorAction SilentlyContinue
    }
    $script:BinaryRunId = $null
    $script:BinaryZipPath = $null
    $script:BinaryUrl = $null
    $script:BinarySha256 = $null
    $script:BinaryIsArchive = $false
}

function Save-OssResultArchive([string]$RemoteOutput) {
    $marker = [regex]::Match($RemoteOutput, '(?m)^OSS_RESULT_URI=(oss://[^\r\n]+)$')
    if (-not $marker.Success) { return }
    $objectUrl = $marker.Groups[1].Value.Trim()
    if (-not $script:Aliyun) { $script:Aliyun = Resolve-Executable 'aliyun' $aliyunCandidates }
    $artifactRoot = Join-Path (Split-Path -Path $PSScriptRoot -Parent) 'artifacts\aliyun-results'
    New-Item -ItemType Directory -Path $artifactRoot -Force | Out-Null
    $archiveName = ($objectUrl -split '/')[-1]
    if (-not $archiveName.EndsWith('.zip', [StringComparison]::OrdinalIgnoreCase)) {
        throw "OSS 结果对象不是 zip: $objectUrl"
    }
    $localArchive = Join-Path $artifactRoot $archiveName
    $temporaryArchive = Join-Path ([IO.Path]::GetTempPath()) "brewfs-result-$([Guid]::NewGuid().ToString('N')).zip"
    try {
        Invoke-Checked $script:Aliyun @(
            'oss', 'cp', $objectUrl, $temporaryArchive,
            '-f', '--region', $S3Region, '--cli-non-interactive'
        ) | Out-Null
        Copy-Item -LiteralPath $temporaryArchive -Destination $localArchive -Force
    } finally {
        Remove-Item -LiteralPath $temporaryArchive -Force -ErrorAction SilentlyContinue
    }
    Expand-Archive -LiteralPath $localArchive -DestinationPath $artifactRoot -Force
    Write-Host "OSS 结果已保存到 WSL: $localArchive"
}

function New-EcsInstance {
    if (-not $VSwitchId -or -not $SecurityGroupId) {
        throw '创建 ECS 需要 -VSwitchId 和 -SecurityGroupId。为避免误改账号网络，脚本不自动创建 VPC。'
    }
    # ESSD performance levels have minimum capacities (PL2 needs >=461 GiB, PL3
    # >=1261 GiB). Fall back to PL1 instead of failing the whole run when the
    # caller asks for a smaller data disk.
    $minSizeByLevel = @{ PL0 = 40; PL1 = 20; PL2 = 461; PL3 = 1261 }
    if ($DataDiskCategory -eq 'cloud_essd' -and
        $DataDiskPerformanceLevel -in @('PL2', 'PL3') -and
        $DataDiskSize -lt $minSizeByLevel[$DataDiskPerformanceLevel]) {
        Write-Warning "数据盘 ${DataDiskSize}GiB 不支持 $DataDiskPerformanceLevel（最小 $($minSizeByLevel[$DataDiskPerformanceLevel])GiB），自动改用 PL1。"
        $DataDiskPerformanceLevel = 'PL1'
    }
    if ($DataDiskSize -lt $minSizeByLevel[$DataDiskPerformanceLevel]) {
        throw "数据盘 ${DataDiskSize}GiB 小于 $DataDiskPerformanceLevel 的最小容量 $($minSizeByLevel[$DataDiskPerformanceLevel])GiB。"
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
        '--DataDisk.1.Category', $DataDiskCategory, '--DataDisk.1.Size', $DataDiskSize.ToString(),
        '--DataDisk.1.PerformanceLevel', $DataDiskPerformanceLevel,
        '--DataDisk.1.DeleteWithInstance', 'true',
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
    $runner = if ($UsePerfImage) { '/opt/brewfs-perf/native/run_native_perf.sh' } elseif ($Workload -eq 'juicefs') { 'docker/compose-xfstests/run_juicefs_perf.sh' } elseif ($Backend -eq 'redis') { 'docker/compose-xfstests/run_redis_perf.sh' } else { 'docker/compose-xfstests/run_tikv_perf.sh' }
    $backendArgs = if ($DataBackend -eq 's3') { '--s3' } else { '--local-fs' }
    $benchArg = if ($RunBench) { '--brewfs-bench' } else { '' }
    # $workloadSelect only picks the filesystem. $WorkloadProfileArgs is a
    # separate knob, so the shared writeback profile can no longer reach one
    # leg while the other runs default settings; the old code overloaded a
    # single variable for both jobs and mislabelled BrewFS archives.
    $workloadSelect = if ($Workload -eq 'juicefs') { 'juicefs' } else { 'brewfs' }
    $profileArgs = if ($WorkloadProfileArgs) { (Quote-Bash $WorkloadProfileArgs) } else { "''" }
    $sourceArchive = if ($SourceArchiveUrl) { (Quote-Bash $SourceArchiveUrl) } else { '' }
    $sourceSha = if ($SourceArchiveSha256) { (Quote-Bash $SourceArchiveSha256) } else { '' }
    $binaryUrl = if ($script:BinaryUrl) { (Quote-Bash $script:BinaryUrl) } else { '' }
    $binarySha = if ($script:BinarySha256) { (Quote-Bash $script:BinarySha256) } else { '' }
    $binaryIsArchive = if ($script:BinaryIsArchive) { '1' } else { '' }
    $imageMode = if ($UsePerfImage) { '1' } else { '' }
    $resultVault = if ($ResultVaultUrl) { (Quote-Bash $ResultVaultUrl) } else { '' }
    $registryMirror = if ($DockerRegistryMirror) { (Quote-Bash $DockerRegistryMirror) } else { "''" }
    $managedMode = if ($ManagedBackend) { '1' } else { '' }
    $metaUrl = if ($MetaUrl) { (Quote-Bash $MetaUrl) } else { "''" }
    $s3Endpoint = if ($S3Endpoint) { (Quote-Bash $S3Endpoint) } else { "''" }
    $s3Bucket = if ($S3Bucket) { (Quote-Bash $S3Bucket) } else { "''" }
    $s3Region = if ($S3Region) { (Quote-Bash $S3Region) } else { "''" }
    $s3AccessKey = if ($S3AccessKey) { (Quote-Bash $S3AccessKey) } else { "''" }
    $s3SecretKey = if ($S3SecretKey) { (Quote-Bash $S3SecretKey) } else { "''" }
    $nativeMode = if ($UsePerfImage) { '1' } else { '' }
    $remote = @'
#!/usr/bin/env bash
set -Eeuo pipefail
export DEBIAN_FRONTEND=noninteractive
 WORK=/opt/brewfs-perf
 DATA_ROOT=__DATA_ROOT__
 DATA_DISK=__DATA_DISK__
REPO=__REPO__
REF=__REF__
TOOLS=__TOOLS__
RUNNER=__RUNNER__
DATA_ARGS=__DATA_ARGS__
BENCH_ARGS=__BENCH_ARGS__
  WORKLOAD=__WORKLOAD__
 SOURCE_ARCHIVE_URL=__SOURCE_ARCHIVE_URL__
 SOURCE_ARCHIVE_SHA256=__SOURCE_ARCHIVE_SHA256__
 BINARY_URL=__BINARY_URL__
 BINARY_SHA256=__BINARY_SHA256__
 BINARY_IS_ARCHIVE=__BINARY_IS_ARCHIVE__
 IMAGE_MODE=__IMAGE_MODE__
 RESULT_VAULT_URL=__RESULT_VAULT_URL__
 RUN_LABEL=__RUN_LABEL__
 REGISTRY_MIRROR=__REGISTRY_MIRROR__
 NATIVE_MODE=__NATIVE_MODE__
 PROFILE_ARGS=__PROFILE_ARGS__
 MANAGED_BACKEND=__MANAGED_BACKEND__
META_URL=__META_URL__
S3_ENDPOINT=__S3_ENDPOINT__
S3_BUCKET=__S3_BUCKET__
S3_REGION=__S3_REGION__
S3_ACCESS_KEY=__S3_ACCESS_KEY__
S3_SECRET_KEY=__S3_SECRET_KEY__

 if [[ "$IMAGE_MODE" != "1" ]]; then
   apt-get update -qq
   apt-get install -y -qq git curl zip docker.io docker-compose-v2 protobuf-compiler util-linux e2fsprogs \
     || apt-get install -y -qq git curl zip docker.io docker-compose-plugin protobuf-compiler
   systemctl stop docker >/dev/null 2>&1 || true
 fi
for _ in $(seq 1 60); do [[ -b "$DATA_DISK" ]] && break; sleep 2; done
[[ -b "$DATA_DISK" ]] || { echo "data disk not found: $DATA_DISK" >&2; exit 1; }
mkdir -p "$DATA_ROOT"
if ! mountpoint -q "$DATA_ROOT"; then
  if ! blkid "$DATA_DISK" >/dev/null 2>&1; then
    mkfs.ext4 -F "$DATA_DISK"
  fi
  mount "$DATA_DISK" "$DATA_ROOT"
fi
mkdir -p "$DATA_ROOT"/{source,cargo,target,artifacts,docker,cache,fio,rustup}
 if [[ "$IMAGE_MODE" != "1" ]]; then
   systemctl enable --now docker
   for _ in $(seq 1 60); do docker info >/dev/null 2>&1 && break; sleep 2; done
   docker info >/dev/null 2>&1 || { echo 'docker daemon did not become ready' >&2; exit 1; }
 fi
export DOCKER_CLIENT_TIMEOUT=300
export COMPOSE_HTTP_TIMEOUT=300
pull_docker_image() {
  local image="$1"
  local attempt
  for attempt in $(seq 1 5); do
    echo "pre-pulling Docker image: $image (attempt $attempt/5)"
    if docker pull "$image"; then
      return 0
    fi
    sleep $((attempt * 10))
  done
  return 1
}
 if [[ "$IMAGE_MODE" != "1" ]]; then
   for image in \
     debian:trixie-slim \
     docker.io/library/redis:7.2-alpine \
     rustfs/rustfs:latest \
     amazon/aws-cli:latest; do
     pull_docker_image "$image" || { echo "unable to pre-pull Docker image after retries: $image" >&2; exit 1; }
   done
 fi

 if [[ "$IMAGE_MODE" == "1" ]]; then
   WORK=/opt/brewfs-perf/source/brewfs
 else
   # The distro Cargo on the supported Ubuntu image may predate Rust 2024
   # edition support. Install a current stable toolchain for source mode.
   export CARGO_HOME="$DATA_ROOT/cargo"
   export CARGO_TARGET_DIR="$DATA_ROOT/target"
   export RUSTUP_HOME="$DATA_ROOT/rustup"
   export PATH="$CARGO_HOME/bin:/root/.cargo/bin:${PATH}"
   export RUSTUP_DIST_SERVER="${RUSTUP_DIST_SERVER:-https://rsproxy.cn}"
   export RUSTUP_UPDATE_ROOT="${RUSTUP_UPDATE_ROOT:-https://rsproxy.cn/rustup}"
   export CARGO_REGISTRIES_CRATES_IO_INDEX="${CARGO_REGISTRIES_CRATES_IO_INDEX:-sparse+https://rsproxy.cn/index/}"
   export CARGO_NET_RETRY="${CARGO_NET_RETRY:-5}"
   export CARGO_HTTP_TIMEOUT="${CARGO_HTTP_TIMEOUT:-120}"
   if ! command -v rustup >/dev/null 2>&1; then
     if ! curl --fail --location --retry 5 --retry-all-errors --connect-timeout 10 --max-time 300 \
         https://rsproxy.cn/rustup-init.sh | sh -s -- -y --profile minimal --default-toolchain none; then
       unset RUSTUP_DIST_SERVER
       unset RUSTUP_UPDATE_ROOT
       curl --fail --location --retry 3 --retry-all-errors --connect-timeout 10 --max-time 300 \
         https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none
     fi
   fi
   rustup toolchain install stable --profile minimal --no-self-update
   rustup default stable
   WORK="$DATA_ROOT/source/brewfs"
 fi
 mkdir -p "$WORK"
 if [[ "$IMAGE_MODE" == "1" ]]; then
   [[ -d "$WORK/.git" ]] || { echo "预制镜像缺少 BrewFS 源码: $WORK" >&2; exit 1; }
   mkdir -p "$WORK/target/release"
   BINARY_ARCHIVE_PATH=
   if [[ "$BINARY_IS_ARCHIVE" == "1" ]]; then
     BINARY_ARCHIVE_PATH="$DATA_ROOT/source/brewfs-upload.zip"
     curl --fail --location --retry 5 --retry-all-errors "$BINARY_URL" --output "$BINARY_ARCHIVE_PATH"
     unzip -p "$BINARY_ARCHIVE_PATH" brewfs >"$WORK/target/release/brewfs"
   else
     curl --fail --location --retry 5 --retry-all-errors "$BINARY_URL" --output "$WORK/target/release/brewfs"
   fi
   echo "$BINARY_SHA256  $WORK/target/release/brewfs" | sha256sum -c -
   chmod 755 "$WORK/target/release/brewfs"
   rm -f "$WORK/target/docker/brewfs"
   export BREWFS_REUSE_HOST_BINARY=1
  # Refresh the harness that the image baked in at build time so runner fixes
  # take effect without a new VM image. These files ride along with the same
  # upload as the binary and are served from the Result Vault by entry name.
  if [[ -n "$BINARY_URL" ]]; then
    HARNESS_BASE="${BINARY_URL%/files/*}"
    refresh_harness() {
      local name="$1" dest="$2"
      if { [[ "$BINARY_IS_ARCHIVE" == "1" ]] && unzip -p "$BINARY_ARCHIVE_PATH" "$name" >"$dest"; } || \
         { [[ "$BINARY_IS_ARCHIVE" != "1" ]] && curl --fail --location --retry 5 --retry-all-errors "$HARNESS_BASE/files/$name" --output "$dest"; }; then
        chmod 755 "$dest" 2>/dev/null || true
        echo "harness refreshed: $name"
        return 0
      fi
      echo "harness refresh skipped (upload has no $name)" >&2
      return 1
    }
    refresh_harness run_native_perf.sh /opt/brewfs-perf/native/run_native_perf.sh || true
    refresh_harness run_perf_in_container.sh "$WORK/docker/compose-xfstests/run_perf_in_container.sh" || true
    refresh_harness run_juicefs_perf_in_container.sh "$WORK/docker/compose-xfstests/run_juicefs_perf_in_container.sh" || true
    if refresh_harness perf_metadata_fallback.py "$WORK/docker/compose-xfstests/perf_metadata_fallback.py"; then
      install -m 0755 "$WORK/docker/compose-xfstests/perf_metadata_fallback.py" /usr/local/bin/perf_metadata_fallback.py
    fi
    mkdir -p "$WORK/tools/perf"
    if refresh_harness perf_manifest.py "$WORK/tools/perf/perf_manifest.py"; then
      install -m 0755 "$WORK/tools/perf/perf_manifest.py" /usr/local/bin/perf_manifest.py
    fi
    [[ -z "$BINARY_ARCHIVE_PATH" ]] || rm -f "$BINARY_ARCHIVE_PATH"
  fi
 elif [[ -n "$SOURCE_ARCHIVE_URL" ]]; then
  archive="$DATA_ROOT/source/brewfs-source.tar.gz"
  curl --fail --location --retry 5 --retry-all-errors "$SOURCE_ARCHIVE_URL" --output "$archive"
  if [[ -n "$SOURCE_ARCHIVE_SHA256" ]]; then
    echo "$SOURCE_ARCHIVE_SHA256  $archive" | sha256sum -c -
  fi
  rm -rf "$WORK"
  mkdir -p "$WORK"
  tar -xzf "$archive" -C "$WORK" --strip-components=1
elif [[ ! -d "$WORK/.git" ]]; then
  git clone --depth=1 --branch "$REF" "$REPO" "$WORK"
else
  git -C "$WORK" fetch --depth=1 origin "$REF"
  git -C "$WORK" reset --hard FETCH_HEAD
fi
cd "$WORK"
if [[ "$IMAGE_MODE" == "1" ]]; then
  cat /opt/brewfs-perf/image-source-commit 2>/dev/null || { echo 'prebuilt image source commit unavailable' >&2; exit 1; }
elif [[ -d .git ]]; then
  git rev-parse HEAD
else
  echo "source bundle: $REF"
fi
export PERF_TOOLS="$TOOLS"
export RUST_LOG="${RUST_LOG:-warn}"
export COMPOSE_PROJECT_NAME="brewfs-$(date +%s)"
export JUICEFS_META_BACKEND="__META_BACKEND__"
# Leave BREWFS_ARTIFACT_DIR unset so each runner creates its own
# perf-run-<ts> directory under BREWFS_ARTIFACT_ROOT. Pointing it at the
# shared parent made the run write report files straight into that parent,
# which left the archive step with no *perf-run-* directory to upload.
export JUICEFS_PERF_STATE_MOUNT="$DATA_ROOT/cache/juicefs-state"
export REDIS_PERF_DATA_MOUNT="$DATA_ROOT/cache/redis"
export JUICEFS_PERF_RUSTFS_DATA_MOUNT="$DATA_ROOT/cache/rustfs"
export PERF_FIO_DIR="$DATA_ROOT/fio"

if [[ "$MANAGED_BACKEND" == "1" ]]; then
  export BREWFS_MANAGED_BACKEND=true
  export BREWFS_META_URL="$META_URL"
  export BREWFS_S3_ENDPOINT="$S3_ENDPOINT"
  export BREWFS_S3_BUCKET="$S3_BUCKET"
  export BREWFS_S3_REGION="$S3_REGION"
  export BREWFS_S3_ACCESS_KEY_ID="$S3_ACCESS_KEY"
  export BREWFS_S3_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
  export JFS_META_URL="$META_URL"
  export JFS_S3_ENDPOINT="$S3_ENDPOINT"
  export JFS_S3_BUCKET="$S3_BUCKET"
  export JFS_S3_REGION="$S3_REGION"
  export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
  export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
  export AWS_DEFAULT_REGION="$S3_REGION"
  export AWS_EC2_METADATA_DISABLED=true
fi

# Start empty: expanding an unset-valued element as "${args[@]}" used to hand
# the runner a bare empty argument.
# Host-level CPU/memory sampling. JuiceFS archives carried process-level CPU
# and RSS metrics while BrewFS carried only writeback counters, so the two
# rounds could not be compared on resource use at all. stdbuf keeps vmstat's
# output line-buffered so the file is readable even if the sampler is killed.
host_samples="$DATA_ROOT/host-samples.txt"
host_sampler_pid=''
if command -v vmstat >/dev/null 2>&1 && command -v stdbuf >/dev/null 2>&1; then
  stdbuf -oL vmstat -t -w 5 > "$host_samples" 2>/dev/null &
  host_sampler_pid=$!
  trap '[[ -n "$host_sampler_pid" ]] && kill "$host_sampler_pid" 2>/dev/null || true' EXIT
fi
args=()
if [[ -n "$DATA_ARGS" ]]; then args+=("$DATA_ARGS"); fi
if [[ -n "$BENCH_ARGS" ]]; then args+=("$BENCH_ARGS"); fi
# Keep the profile symmetric. BrewFS used to run default writeback settings
# while JuiceFS ran --writeback-throughput-profile, so the two archives were not
# comparable. $WORKLOAD now only selects the filesystem; the profile travels
# separately in $PROFILE_ARGS and reaches both legs. The value can hold several
# flags, so split it into words instead of passing it as one quoted argument.
profile_args_split=()
if [[ -n "$PROFILE_ARGS" ]]; then read -r -a profile_args_split <<< "$PROFILE_ARGS"; fi
if [[ ${#profile_args_split[@]} -gt 0 ]]; then args+=("${profile_args_split[@]}"); fi
if [[ "$NATIVE_MODE" == "1" ]]; then
  # The native runner consumes --s3/--tools/profile itself and dispatches on the
  # first argument, so both workloads get the same argument shape here.
  bash "$RUNNER" "$WORKLOAD" --tools "$TOOLS" "${args[@]}"
elif [[ "$WORKLOAD" == "juicefs" ]]; then
  # The container JuiceFS runner has its own flag set and rejects --s3, so only
  # the profile flag is forwarded.
  if [[ ${#profile_args_split[@]} -gt 0 ]]; then
    bash "$RUNNER" "${profile_args_split[@]}" --tools "$TOOLS"
  else
    bash "$RUNNER" --tools "$TOOLS"
  fi
else
  bash "$RUNNER" --tools "$TOOLS" "${args[@]}"
fi
if [[ -n "$host_sampler_pid" ]]; then
  kill "$host_sampler_pid" 2>/dev/null || true
  wait "$host_sampler_pid" 2>/dev/null || true
  host_sampler_pid=''
fi

# The native runner writes artefacts either into the source tree or, when
# BREWFS_ARTIFACT_DIR points at the data disk, outside it. Searching only one
# root made an otherwise fully successful run fail at the last step with
# "no performance artifact directory found".
artifact_roots=("$WORK/docker/compose-xfstests/artifacts")
# The script runs under `set -u`, and BREWFS_ARTIFACT_DIR is intentionally left
# unset so the runner names its own run directory; expand it defensively.
if [[ -n "${BREWFS_ARTIFACT_DIR:-}" && "${BREWFS_ARTIFACT_DIR}" != "${artifact_roots[0]}" ]]; then
  artifact_roots+=("${BREWFS_ARTIFACT_DIR}")
fi
latest_dir=$(find "${artifact_roots[@]}" -mindepth 1 -maxdepth 1 -type d -name '*perf-run-*' -printf '%T@ %p\n' 2>/dev/null | sort -nr | awk 'NR == 1 {print $2}')
if [[ -z "$latest_dir" ]]; then
  echo "no performance artifact directory found; searched: ${artifact_roots[*]}" >&2
  for _root in "${artifact_roots[@]}"; do
    if [[ -d "$_root" ]]; then echo "--- $_root" >&2; ls -la "$_root" >&2; fi
  done
  exit 1
fi
# The Result Vault lists runs by their archive root directory name, so the
# workload has to be part of that name: two runs of different filesystems are
# otherwise indistinguishable in the UI. The runners append their own tag now;
# this only fires for a harness copy that predates that change.
artifact_tag="$WORKLOAD"
if [[ -n "$RUN_LABEL" ]]; then artifact_tag="${artifact_tag}-${RUN_LABEL}"; fi
latest_name="$(basename "$latest_dir")"
if [[ "$latest_name" != *"-${artifact_tag}" ]]; then
  tagged_dir="$(dirname "$latest_dir")/${latest_name}-${artifact_tag}"
  mv "$latest_dir" "$tagged_dir"
  latest_dir="$tagged_dir"
  echo "artifact directory tagged for the Result Vault: $(basename "$latest_dir")"
fi
# Record the knobs that decide whether the two filesystem legs are comparable.
# The previous round shipped a BrewFS archive with the profile flag missing and
# nothing inside the zip said so; this file makes that class of mismatch
# visible from the Result Vault alone.
# S3_ENDPOINT here is the driver-supplied argument; run_native_perf.sh rewrites
# it to the in-region internal endpoint, so read back what the filesystem
# actually used rather than recording the argument as if it were the config.
effective_endpoint=''
if [[ -f "$latest_dir/backend.yml" ]]; then
  effective_endpoint=$(awk '/^[[:space:]]+endpoint:[[:space:]]/{print $2; exit}' "$latest_dir/backend.yml")
elif [[ -f "$latest_dir/juicefs-profile.env" ]]; then
  effective_endpoint=$(sed -n 's/^JFS_S3_ENDPOINT=//p' "$latest_dir/juicefs-profile.env" | head -n 1)
fi
{
  echo "workload=$WORKLOAD"
  echo "run_label=$RUN_LABEL"
  echo "native_mode=${NATIVE_MODE:-0}"
  echo "managed_backend=${MANAGED_BACKEND:-0}"
  echo "profile_args=${PROFILE_ARGS:-<none>}"
  echo "data_args=${DATA_ARGS:-<none>}"
  echo "bench_args=${BENCH_ARGS:-<none>}"
  echo "perf_tools=$TOOLS"
  echo "s3_endpoint_arg=${S3_ENDPOINT:-<none>}"
  echo "s3_endpoint_effective=${effective_endpoint:-<unknown>}"
  echo "s3_bucket=${S3_BUCKET:-<none>}"
  echo "s3_region=${S3_REGION:-<none>}"
  echo "meta_url=$(printf '%s' "${META_URL:-}" | sed -E 's#(://)[^@]*@#\1***@#')"
} > "$latest_dir/config-parity.txt"
# Cache budgets as they actually reached the filesystem, so a budget mismatch
# between the two legs shows up in the archive instead of in a re-read of the
# scripts. BrewFS splits memory and disk between read and write caches; JuiceFS
# has one of each.
for _env_file in "$latest_dir/perf-profile.env" "$latest_dir/juicefs-profile.env"; do
  [[ -f "$_env_file" ]] || continue
  grep -E '^(BREWFS_(READ|WRITE)_(MEMORY|SSD)_BYTES|BREWFS_MEMORY_BUDGET_BYTES|JFS_(BUFFER_SIZE_MIB|CACHE_SIZE_MIB))=' "$_env_file" || true
done >> "$latest_dir/config-parity.txt"
if [[ -s "$host_samples" ]]; then
  cp -f "$host_samples" "$latest_dir/host-samples.txt" 2>/dev/null || true
fi
archive="$DATA_ROOT/artifacts/$(basename "$latest_dir").zip"
(cd "$(dirname "$latest_dir")" && zip -qr "$archive" "$(basename "$latest_dir")")
[[ -n "$RESULT_VAULT_URL" ]] || { echo 'RESULT_VAULT_URL is required; refusing to finish without upload target' >&2; exit 1; }
if curl --fail-with-body --silent --show-error --location --retry 5 --retry-all-errors \
    -F "archive=@$archive" "${RESULT_VAULT_URL%/}/api/runs"; then
  echo 'Result Vault upload complete'
elif [[ "$MANAGED_BACKEND" == "1" && -n "$S3_BUCKET" ]]; then
  result_key="_brewfs-perf-results/$(basename "$archive")"
  result_endpoint="${effective_endpoint:-$S3_ENDPOINT}"
  aws configure set default.s3.addressing_style virtual
  AWS_REQUEST_CHECKSUM_CALCULATION=when_required \
  AWS_RESPONSE_CHECKSUM_VALIDATION=when_required \
  aws --endpoint-url "$result_endpoint" --region "$S3_REGION" \
    s3 cp "$archive" "s3://$S3_BUCKET/$result_key" --only-show-errors
  echo "OSS_RESULT_URI=oss://$S3_BUCKET/$result_key"
else
  echo 'Result Vault upload failed and no managed OSS fallback is available' >&2
  exit 1
fi

echo '--- latest perf summary ---'
find "${artifact_roots[@]}" -name perf-summary.tsv -type f -printf '%T@ %p\n' 2>/dev/null \
  | sort -nr | awk 'NR == 1 {print $2}' \
  | xargs -r tail -n 80
'@
    $remote = $remote.Replace('__REPO__', (Quote-Bash $Repository))
    $remote = $remote.Replace('__REF__', (Quote-Bash $Ref))
    $remote = $remote.Replace('__TOOLS__', (Quote-Bash $PerfTools))
    $remote = $remote.Replace('__RUNNER__', (Quote-Bash $runner))
    $remote = $remote.Replace('__DATA_ARGS__', (Quote-Bash $backendArgs))
    $remote = $remote.Replace('__BENCH_ARGS__', (Quote-Bash $benchArg))
    $remote = $remote.Replace('__WORKLOAD__', (Quote-Bash $workloadSelect))
    $remote = $remote.Replace('__DATA_ROOT__', (Quote-Bash $DataRoot))
    $remote = $remote.Replace('__DATA_DISK__', (Quote-Bash $DataDiskDevice))
    $remote = $remote.Replace('__SOURCE_ARCHIVE_URL__', $sourceArchive)
    $remote = $remote.Replace('__SOURCE_ARCHIVE_SHA256__', $sourceSha)
    $remote = $remote.Replace('__BINARY_URL__', $binaryUrl)
    $remote = $remote.Replace('__BINARY_SHA256__', $binarySha)
    $remote = $remote.Replace('__BINARY_IS_ARCHIVE__', $binaryIsArchive)
    $remote = $remote.Replace('__IMAGE_MODE__', (Quote-Bash $imageMode))
    $remote = $remote.Replace('__RESULT_VAULT_URL__', $resultVault)
    $remote = $remote.Replace('__RUN_LABEL__', (Quote-Bash $RunLabel))
    $remote = $remote.Replace('__REGISTRY_MIRROR__', $registryMirror)
    $remote = $remote.Replace('__NATIVE_MODE__', (Quote-Bash $nativeMode))
    $remote = $remote.Replace('__PROFILE_ARGS__', $profileArgs)
    $remote = $remote.Replace('__MANAGED_BACKEND__', (Quote-Bash $managedMode))
    $remote = $remote.Replace('__META_URL__', $metaUrl)
    $remote = $remote.Replace('__S3_ENDPOINT__', $s3Endpoint)
    $remote = $remote.Replace('__S3_BUCKET__', $s3Bucket)
    $remote = $remote.Replace('__S3_REGION__', $s3Region)
    $remote = $remote.Replace('__S3_ACCESS_KEY__', $s3AccessKey)
    $remote = $remote.Replace('__S3_SECRET_KEY__', $s3SecretKey)
    $remote = $remote.Replace('__META_BACKEND__', $Backend)
    return $remote
}

function Invoke-PerfOnEcs {
    $remoteCommand = (Get-RemoteCommand) -replace "`r`n", "`n" -replace "`r", ""
    $content = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($remoteCommand))
    $deadline = (Get-Date).AddSeconds(172800)
    $maxAttempts = 3
    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
        $run = Invoke-AliyunJson @(
            'ecs', 'RunCommand', '--region', $RegionId, '--Type', 'RunShellScript',
            '--InstanceId.1', $InstanceId, '--CommandContent', $content,
            '--ContentEncoding', 'Base64', '--Timeout', '172800',
            '--KeepCommand', 'false', '--Name', "brewfs-perf-$Backend"
        )
        $invokeId = $run.InvokeId
        if (-not $invokeId) { throw 'RunCommand 未返回 InvokeId。请确认 ECS Cloud Assistant Agent 已在线。' }
        Write-Host "远程性能测试已提交: $invokeId (attempt $attempt/$maxAttempts)"

        $terminal = $null
        $text = ''
        while ((Get-Date) -lt $deadline) {
            $result = Invoke-AliyunJson @('ecs', 'DescribeInvocationResults', '--region', $RegionId, '--InvokeId', $invokeId)
            $item = @($result.Invocation.InvocationResults.InvocationResult)[0]
            if ($item) {
                Write-Host "  invocation status=$($item.InvocationStatus)"
                if ($item.InvocationStatus -in @('Success', 'Failed', 'Stopped', 'Error', 'Terminated')) {
                    $terminal = $item
                    if ($item.Output) {
                        $text = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($item.Output))
                        Write-Output $text
                    }
                    break
                }
            }
            Start-Sleep -Seconds 5
        }
        if (-not $terminal) { throw '等待远程性能测试完成超时。' }
        if ($terminal.InvocationStatus -eq 'Success') {
            Save-OssResultArchive $text
            return
        }

        $agentRestarted = $terminal.InvocationStatus -eq 'Terminated' -and
            ($terminal.ErrorCode -eq 'ClientRestarted' -or $terminal.ErrorInfo -match 'service has been restarted')
        if ($agentRestarted -and $attempt -lt $maxAttempts) {
            Write-Warning 'Cloud Assistant 在初始化期间重启；保留当前 ECS/数据盘并重新提交同一脚本，以便从已有下载/构建状态继续。'
            Start-Sleep -Seconds 15
            continue
        }

        $script:PerfFailure = if ($terminal.ErrorInfo) { $terminal.ErrorInfo } else { "InvocationStatus=$($terminal.InvocationStatus), ErrorCode=$($terminal.ErrorCode)" }
        throw "远程测试失败: $script:PerfFailure"
    }
}

function Remove-EcsInstance {
    if (-not $InstanceId) { throw 'destroy 需要 -InstanceId。' }
    Wait-Until {
        $instance = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', "[`"$InstanceId`"]")
        $item = @($instance.Instances.Instance)[0]
        if (-not $item) { return $true }
        Write-Host "  cleanup ECS state=$($item.Status)"
        $item.Status -in @('Running', 'Stopped')
    } "ECS $InstanceId 脱离初始化状态" 900

    $instance = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', "[`"$InstanceId`"]")
    $state = @($instance.Instances.Instance)[0].Status
    if ($state -eq 'Running' -or $state -eq 'Starting') {
        Invoke-AliyunJson @('ecs', 'StopInstance', '--region', $RegionId, '--InstanceId', $InstanceId, '--ForceStop', 'true') | Out-Null
        Wait-Until {
            $current = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', "[`"$InstanceId`"]")
            @($current.Instances.Instance)[0].Status -eq 'Stopped'
        } "ECS $InstanceId 停止" 600
    } elseif ($state -eq 'Stopping') {
        Wait-Until {
            $current = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', "[`"$InstanceId`"]")
            @($current.Instances.Instance)[0].Status -eq 'Stopped'
        } "ECS $InstanceId 停止" 600
    }
    # A freshly used instance can still report IncorrectInstanceStatus
    # (initializing) when the delete is issued, so retry instead of leaking it.
    $deleteDeadline = (Get-Date).AddMinutes(10)
    while ($true) {
        try {
            Invoke-AliyunJson @('ecs', 'DeleteInstance', '--region', $RegionId, '--InstanceId', $InstanceId, '--Force', 'true') | Out-Null
            break
        } catch {
            if ($_.Exception.Message -notmatch 'IncorrectInstanceStatus' -or (Get-Date) -ge $deleteDeadline) { throw }
            Write-Warning "ECS $InstanceId 仍处于初始化尾态，稍后重试删除。"
            Start-Sleep -Seconds 15
        }
    }
    Wait-Until {
        $current = Invoke-AliyunJson @('ecs', 'DescribeInstances', '--region', $RegionId, '--InstanceIds', "[`"$InstanceId`"]")
        @($current.Instances.Instance).Count -eq 0
    } "ECS $InstanceId 删除" 600
    Write-Host "ECS 删除任务已提交: $InstanceId"
    $script:CreatedInstance = $false
}

try {
    if ($Action -eq 'run' -and -not $ResultVaultUrl) {
        throw 'run 需要 -ResultVaultUrl 或 BREWFS_RESULTS_URL；未配置上传目标，不创建或使用 ECS 执行测试。'
    }
    if ($Action -eq 'run') {
        if ($InstanceId) { throw 'run 只允许创建本轮临时 ECS；不要传入已有 -InstanceId。已有实例请使用 status/destroy。' }
        if ($KeepInstance -or $NoCleanup) { throw 'run 始终清理 ECS、数据盘和临时上传资源，不支持 -KeepInstance/-NoCleanup。' }
        if ($UsePerfImage -and -not $BinaryPath) { throw 'UsePerfImage 必须同时提供 -BinaryPath。' }
        if ($BinaryPath -and -not $UsePerfImage) { throw 'BinaryPath 只能配合 -UsePerfImage 使用。' }
        if ($ManagedBackend) {
            if (-not $UsePerfImage) { throw 'ManagedBackend 只允许配合 UsePerfImage 使用。' }
            # Managed Aliyun Redis/Tair is used for metadata in both modes; the
            # OSS object store is only required for the s3 data backend.
            $required = @(@('MetaUrl', $MetaUrl))
            if ($DataBackend -eq 's3') {
                $required += @(@('S3Endpoint', $S3Endpoint), @('S3Bucket', $S3Bucket), @('S3Region', $S3Region), @('S3AccessKey', $S3AccessKey), @('S3SecretKey', $S3SecretKey))
            }
            foreach ($pair in $required) {
                if (-not $pair[1]) { throw "ManagedBackend 缺少 -$($pair[0])。" }
            }
        }
        $parsedResultUrl = $null
        if (-not [Uri]::TryCreate($ResultVaultUrl.TrimEnd('/'), [UriKind]::Absolute, [ref]$parsedResultUrl) -or
            $parsedResultUrl.Scheme -notin @('http', 'https')) {
            throw 'Result Vault URL 必须是 http(s) 绝对地址；未创建或使用 ECS。'
        }
    }
    if ($DataDiskDevice -notmatch '^/dev/[a-zA-Z0-9._-]+$' -or $DataDiskDevice -eq '/dev/vda') {
        throw "DataDiskDevice 必须是明确的非系统盘设备（默认 /dev/vdb），当前值: $DataDiskDevice"
    }
    if ($DataRoot -notmatch '^/[a-zA-Z0-9._/-]+$' -or $DataRoot -eq '/') {
        throw "DataRoot 必须是明确的绝对目录，当前值: $DataRoot"
    }
    if ($Action -in @('run', 'create') -and -not $InstanceId) {
        if ($Action -eq 'run') { New-BinaryUpload }
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
    if ($Action -eq 'run' -and $script:CreatedInstance) {
        try { Remove-EcsInstance } catch { Write-Warning "ECS 自动清理失败: $($_.Exception.Message)" }
    }
    Remove-BinaryUpload
}
