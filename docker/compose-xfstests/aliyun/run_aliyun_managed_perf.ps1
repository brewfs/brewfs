[CmdletBinding()]
param(
    [ValidateSet('run')]
    [string]$Action = 'run',
    # Which filesystem legs to exercise. "both" keeps the original two-leg
    # comparison; a single leg exists so one side can be re-measured under its
    # own profile without paying for the other side's ECS, Redis database and
    # bucket as well.
    [ValidateSet('both', 'brewfs', 'juicefs')]
    [string]$Workloads = 'both',
    [string]$RegionId = 'cn-hangzhou',
    [string]$ZoneId = 'cn-hangzhou-h',
    [string]$VSwitchId = $env:BREWFS_PERF_VSWITCH_ID,
    [string]$VpcId = $env:BREWFS_PERF_VPC_ID,
    [string]$SecurityGroupId = $env:BREWFS_PERF_SECURITY_GROUP_ID,
    [string]$ImageId = $env:BREWFS_PERF_IMAGE_ID,
    [string]$InstanceType = 'ecs.u1-c1m2.2xlarge',
    [ValidateRange(100, 32768)]
    [int]$DataDiskSize = 512,
    [string]$BinaryPath,
    [string]$ResultVaultUrl = $env:BREWFS_RESULTS_URL,
    [string]$ResultVaultResolveIp = $env:BREWFS_RESULTS_RESOLVE_IP,
    [string]$OssEndpoint = 'https://oss-cn-hangzhou.aliyuncs.com',
    [string]$OssRegion = 'cn-hangzhou',
    [string]$OssAccessKey = $env:BREWFS_PERF_OSS_ACCESS_KEY_ID,
    [string]$OssSecretKey = $env:BREWFS_PERF_OSS_SECRET_ACCESS_KEY,
    [ValidateSet('s3', 'local-fs')]
    [string]$DataBackend = 's3',
    [string]$RedisInstanceClass = 'redis.master.small.default',
    [string]$RedisNodeType = 'double',
    [string]$RedisEngineVersion = '5.0',
    [string]$RedisPassword = $env:BREWFS_PERF_REDIS_PASSWORD,
    [string]$PerfTools = 'fio-bigwrite fio-bigread fio-seqread fio-seqwrite fio-randread fio-randwrite fio-randrw dirstress dirperf metaperf looptest',
    [string]$DataRoot = '/mnt/brewfs-perf-data',
    [string]$AutoReleaseMinutes = '360',
    # Optional suffix appended to the Result Vault run name, for example "8g".
    [string]$RunLabel = ''
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$script:Aliyun = $null
$script:RedisInstanceId = $null
$script:RedisUrl = $null
$script:RedisPassword = $null
$script:Buckets = [System.Collections.Generic.List[string]]::new()
$script:BackendsCreated = $false

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

function Invoke-Oss([string[]]$Arguments) {
    if (-not $script:Aliyun) { $script:Aliyun = Resolve-Executable 'aliyun' $aliyunCandidates }
    Invoke-Checked $script:Aliyun (@('oss') + $Arguments) | Out-Host
}

function Wait-Until([scriptblock]$Condition, [string]$Description, [int]$TimeoutSeconds = 900) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastError = $null
    do {
        try {
            if (& $Condition) { return }
        } catch {
            # Surface the first failure of each kind: a typo in an API name used
            # to hide here and the wait spun until the timeout.
            if ($_.Exception.Message -ne $lastError) {
                $lastError = $_.Exception.Message
                Write-Warning "waiting for ${Description}: $lastError"
            }
        }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $deadline)
    throw "等待超时: $Description"
}

function Get-LocalOssCredentials {
    if ($OssAccessKey -and $OssSecretKey) { return }
    $configPath = Join-Path $env:USERPROFILE '.aliyun\config.json'
    if (-not (Test-Path -LiteralPath $configPath)) {
        throw '未提供 -OssAccessKey/-OssSecretKey，且 Aliyun CLI 配置文件不存在。'
    }
    $config = Get-Content -LiteralPath $configPath -Raw | ConvertFrom-Json
    $profileName = [string]$config.current
    $profile = @($config.profiles) | Where-Object { $_.name -eq $profileName } | Select-Object -First 1
    if (-not $profile.access_key_id -or -not $profile.access_key_secret) {
        throw '未能从 Aliyun CLI 当前 profile 读取 OSS 凭证；请显式传入 -OssAccessKey 和 -OssSecretKey。'
    }
    $script:OssAccessKey = [string]$profile.access_key_id
    $script:OssSecretKey = [string]$profile.access_key_secret
}

function Get-VpcIdAndCidr {
    if (-not $VSwitchId) { throw '需要 -VSwitchId 或 BREWFS_PERF_VSWITCH_ID。' }
    $switch = Invoke-AliyunJson @('vpc', 'DescribeVSwitchAttributes', '--region', $RegionId, '--VSwitchId', $VSwitchId)
    if (-not $VpcId) { $script:VpcId = [string]$switch.VpcId }
    if (-not $script:VpcId) { throw '无法从 vSwitch 解析 VPC ID。' }
    return [string]$switch.CidrBlock
}

function New-TemporaryBucket([string]$Workload) {
    $suffix = [Guid]::NewGuid().ToString('N').Substring(0, 8)
    $name = "brewfs-perf-$Workload-$((Get-Date).ToUniversalTime().ToString('yyyyMMddHHmmss'))-$suffix".ToLowerInvariant()
    Invoke-Oss @('mb', "oss://$name", '--region', $OssRegion, '--acl', 'private')
    $script:Buckets.Add($name)
    Write-Host "OSS bucket created: $name"
    return $name
}

function New-TemporaryRedis([string]$VSwitchCidr) {
    if (-not $RedisPassword) {
        $script:RedisPassword = 'BrewfsPerf' + [Guid]::NewGuid().ToString('N').Substring(0, 20)
    } else {
        $script:RedisPassword = $RedisPassword
    }
    $name = 'brewfs-perf-redis-{0}' -f (Get-Date -Format 'yyyyMMdd-HHmmss')
    $args = @(
        'r-kvstore', 'CreateInstance', '--region', $RegionId,
        '--RegionId', $RegionId, '--ZoneId', $ZoneId,
        '--InstanceClass', $RedisInstanceClass, '--InstanceType', 'Redis',
        '--EngineVersion', $RedisEngineVersion, '--ChargeType', 'PostPaid',
        '--NetworkType', 'VPC', '--VpcId', $VpcId, '--VSwitchId', $VSwitchId,
        '--NodeType', $RedisNodeType, '--ShardCount', '1',
        '--Port', '6379', '--Password', $script:RedisPassword,
        '--InstanceName', $name, '--AutoRenew', 'false',
        '--Token', ([Guid]::NewGuid().ToString('N')),
        '--Tag.1.Key', 'brewfs-perf-temporary', '--Tag.1.Value', 'true'
    )
    $result = Invoke-AliyunJson $args
    $script:RedisInstanceId = [string]$result.InstanceId
    if (-not $script:RedisInstanceId) { throw 'CreateInstance 未返回 Redis 实例 ID。' }
    Write-Host "managed Redis/Tair creating: $($script:RedisInstanceId)"

    Wait-Until {
        $state = Invoke-AliyunJson @('r-kvstore', 'DescribeInstances', '--region', $RegionId, '--RegionId', $RegionId, '--InstanceIds', $script:RedisInstanceId)
        $item = @($state.Instances.KVStoreInstance)[0]
        $status = [string]$item.InstanceStatus
        Write-Host "  Redis state=$status"
        if ($status -in @('Normal', 'Running')) { return $true }
        if ($status -in @('Released', 'Inactive')) { throw "Redis 实例进入失败状态: $status" }
        return $false
    } 'managed Redis/Tair ready' 1800

    Invoke-AliyunJson @(
        'r-kvstore', 'ModifySecurityIps', '--region', $RegionId,
        '--InstanceId', $script:RedisInstanceId, '--SecurityIps', $VSwitchCidr,
        '--ModifyMode', 'Cover', '--SecurityIpGroupName', 'default'
    ) | Out-Null

    Wait-Until {
        $net = Invoke-AliyunJson @('r-kvstore', 'DescribeDBInstanceNetInfo', '--region', $RegionId, '--InstanceId', $script:RedisInstanceId, '--NetType', 'Private')
        $item = @($net.NetInfoItems.InstanceNetInfo | Where-Object { $_.ConnectionString })[0]
        if ($item -and $item.ConnectionString -and $item.Port) {
            $script:RedisUrl = "redis://:$($script:RedisPassword)@$($item.ConnectionString):$($item.Port)"
            return $true
        }
        return $false
    } 'managed Redis/Tair private endpoint' 900
    Write-Host "managed Redis/Tair ready: $($script:RedisInstanceId)"
}

function Remove-TemporaryBackends {
    foreach ($bucket in @($script:Buckets)) {
        try {
            Write-Host "deleting OSS bucket and objects: $bucket"
            Invoke-Oss @('rm', "oss://$bucket", '-r', '-f', '--region', $OssRegion)
            Invoke-Oss @('rm', '--bucket', "oss://$bucket", '-f', '--region', $OssRegion)
        } catch {
            Write-Warning "OSS bucket cleanup failed for ${bucket}: $($_.Exception.Message)"
        }
    }

    # `oss rm -r -f` only clears objects; an empty bucket still counts against
    # the account bucket quota, so remove the bucket itself and prove it is gone.
    if (@($script:Buckets).Count -gt 0) {
        $listing = ''
        try {
            $listing = ((& $script:Aliyun @('oss', 'ls')) 2>&1 | Out-String)
        } catch {
            Write-Warning "OSS bucket 清理校验失败: $($_.Exception.Message)"
        }
        $leftover = @($script:Buckets | Where-Object { $listing -match [regex]::Escape("oss://$_") })
        if ($leftover.Count -gt 0) {
            Write-Warning "OSS bucket 残留未删除: $($leftover -join ', ')"
        } else {
            Write-Host 'OSS 临时 bucket 已全部删除'
        }
        $script:Buckets.Clear()
    }

    if ($script:RedisInstanceId) {
        try {
            Invoke-AliyunJson @('r-kvstore', 'DeleteInstance', '--region', $RegionId, '--InstanceId', $script:RedisInstanceId) | Out-Null
            Wait-Until {
                $state = Invoke-AliyunJson @('r-kvstore', 'DescribeInstances', '--region', $RegionId, '--RegionId', $RegionId, '--InstanceIds', $script:RedisInstanceId)
                @($state.Instances.KVStoreInstance).Count -eq 0
            } "Redis 实例 $($script:RedisInstanceId) 删除" 1200
            Write-Host "managed Redis/Tair deleted: $($script:RedisInstanceId)"
        } catch {
            Write-Warning "Redis/Tair cleanup failed for $($script:RedisInstanceId): $($_.Exception.Message)"
        }
    }
    $script:RedisInstanceId = $null
    $script:RedisUrl = $null
}

function Invoke-Workload([string]$Workload, [string]$Bucket) {
    $runner = Join-Path $PSScriptRoot 'run_aliyun_perf.ps1'
    # BrewFS and JuiceFS share one managed Redis/Tair instance and both use the
    # same bare key layout ("i<inode>", "d<inode>", "c<inode>_<chunk>"), so
    # running them against the same logical database made JuiceFS read BrewFS
    # directory hashes and panic in meta.parseEntry ("invalid entry"). Give each
    # workload its own logical database; one instance still covers both.
    $dbIndex = if ($Workload -eq 'juicefs') { 1 } else { 0 }
    $metaUrl = "$($script:RedisUrl)/$dbIndex"
    $args = @(
        '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $runner,
        '-Action', 'run', '-UsePerfImage', '-ManagedBackend',
        '-BinaryPath', $BinaryPath, '-ResultVaultUrl', $ResultVaultUrl,
        '-RegionId', $RegionId, '-ZoneId', $ZoneId,
        '-VSwitchId', $VSwitchId, '-SecurityGroupId', $SecurityGroupId,
        '-ImageId', $ImageId, '-InstanceType', $InstanceType,
        '-DataDiskSize', $DataDiskSize.ToString(), '-DataRoot', $DataRoot,
        '-Workload', $Workload, '-Backend', 'redis', '-DataBackend', $DataBackend,
        '-PerfTools', $PerfTools, '-MetaUrl', $metaUrl,
        '-AutoReleaseMinutes', $AutoReleaseMinutes, '-RunLabel', $RunLabel
    )
    if ($ResultVaultResolveIp) {
        $args += @('-ResultVaultResolveIp', $ResultVaultResolveIp)
    }
    if ($DataBackend -eq 's3') {
        $args += @(
            '-S3Endpoint', $OssEndpoint, '-S3Bucket', $Bucket,
            '-S3Region', $OssRegion, '-S3AccessKey', $OssAccessKey,
            '-S3SecretKey', $OssSecretKey
        )
    }
    Write-Host "starting managed workload: $Workload"
    & (Resolve-Executable 'pwsh') @args
    if ($LASTEXITCODE -ne 0) { throw "$Workload 性能测试失败，exit=$LASTEXITCODE" }
}

try {
    if ($Action -ne 'run') { throw '当前脚本只支持 -Action run；测试资源始终由本轮 finally 自动删除。' }
    if (-not $BinaryPath -or -not (Test-Path -LiteralPath $BinaryPath -PathType Leaf)) { throw '需要有效的 -BinaryPath。' }
    if (-not $ImageId) { throw '需要 -ImageId 或 BREWFS_PERF_IMAGE_ID。' }
    if (-not $VSwitchId -or -not $SecurityGroupId) { throw '需要 vSwitch、安全组和现有 Ubuntu native VM image。' }
    if (-not $ResultVaultUrl) { throw '需要 -ResultVaultUrl 或 BREWFS_RESULTS_URL。' }
    Get-LocalOssCredentials
    $cidr = Get-VpcIdAndCidr
    if ($DataBackend -eq 'local-fs') {
        # Validation mode: managed Redis only, no object store. JuiceFS cannot
        # run without an object backend, so only BrewFS is exercised.
        if ($Workloads -eq 'juicefs') { throw 'JuiceFS 需要对象存储；local-fs 校验模式只能跑 BrewFS。' }
        New-TemporaryRedis $cidr
        $script:BackendsCreated = $true
        Invoke-Workload 'brewfs' $null
        Write-Host 'managed BrewFS local-fs validation complete; the report was uploaded by the ECS run.'
    } else {
        $brewfsBucket = if ($Workloads -eq 'juicefs') { $null } else { New-TemporaryBucket 'brewfs' }
        $juicefsBucket = if ($Workloads -eq 'brewfs') { $null } else { New-TemporaryBucket 'juicefs' }
        New-TemporaryRedis $cidr
        $script:BackendsCreated = $true
        if ($brewfsBucket) { Invoke-Workload 'brewfs' $brewfsBucket }
        if ($juicefsBucket) { Invoke-Workload 'juicefs' $juicefsBucket }
        Write-Host "managed workload(s) $Workloads complete; final reports were uploaded by each ECS run."
    }
} finally {
    if ($script:BackendsCreated -or $script:RedisInstanceId -or $script:Buckets.Count -gt 0) {
        Remove-TemporaryBackends
    }
}
