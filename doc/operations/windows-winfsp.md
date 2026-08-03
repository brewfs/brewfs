# BrewFS 在 Windows 上的安装与挂载（WinFsp）

BrewFS 在 Linux/macOS 上通过 FUSE（asyncfuse）挂载；在 Windows 上通过
[WinFsp](https://winfsp.dev) 提供原生文件系统支持。本文说明如何在 Windows
上安装、编译并把 BrewFS 挂载成盘符或文件夹。

## 原理简述

WinFsp 之于 Windows 相当于 FUSE 之于 Linux：

| | Linux | Windows |
|---|---|---|
| 内核部分 | fuse.ko | WinFsp.sys（驱动） |
| 用户态部分 | libfuse | winfsp-x64.dll |
| BrewFS 代码 | `src/fuse/`（asyncfuse 回调） | `src/winfsp/`（WinFsp 回调） |

Windows 没有“从用户态实现文件系统”的 Win32 API：Win32 文件 API
（`CreateFile`/`ReadFile`/…）是**使用**文件系统的接口，而把 BrewFS 变成
`Z:\` 需要内核里有文件系统驱动应答 IRP。WinFsp 提供了这个驱动，
BrewFS 只实现用户态回调。

## 一、环境要求

- Windows 10/11 x64（WinFsp 支持 x64 / x86 / ARM64）
- Rust 工具链（`x86_64-pc-windows-msvc`，含 MSVC Build Tools）
- **WinFsp 2.x（含驱动 WinFsp.sys）**——只有运行挂载才需要；编译不需要

## 二、安装 WinFsp

推荐用 winget（需要管理员权限的终端）：

```powershell
winget install WinFsp.WinFsp
```

或从 <https://github.com/winfsp/winfsp/releases> 下载安装包手动安装。

验证安装：

```powershell
Get-Service WinFsp*          # 应能看到 WinFsp 相关服务
Get-ChildItem C:\Windows\System32\drivers\WinFsp.sys
```

> BrewFS 二进制在启动时**延迟加载** winfsp-x64.dll：若未安装 WinFsp，
> `brewfs mount` 会直接报“WinFsp is not installed or could not be loaded”，
> 其余命令（`info`/`gc`/`object-put-bench` 等）不受影响。

## 三、编译

```powershell
# 在仓库根目录
cargo build --release --no-default-features --features fuse-winfsp
```

说明：

- 默认 feature 是 `fuse-io-uring-runtime`（io_uring 仅 Linux），Windows 必须用
  `--no-default-features`；
- `fuse-winfsp` 启用 WinFsp 适配（`src/winfsp/`）并把 `brewfs mount` 路由到
  WinFsp；
- **编译不需要安装 WinFsp**（`winfsp-sys` 自带 import library）；
- 完整测试：`cargo test --no-default-features --features fuse-winfsp --lib --bins`。

## 四、挂载

挂载点可以是**盘符**（如 `Z:`）或**空文件夹**（如 `C:\mnt\brewfs`）。

### 4.1 本地文件系统数据后端

挂载点是**位置参数**（不是 `--mount-point`）；`local-fs` 数据后端：

```powershell
.\target\release\brewfs.exe mount `
  --data-backend local-fs `
  --data-dir .\data `
  Z:
```

> - **盘符**（`Z:`）直接挂载即可，任意卷（NTFS/exFAT）都行。
> - **文件夹**（如 `C:\mnt\brewfs`）挂载点**必须位于 NTFS 卷**上（Windows
>   目录挂载点依赖 NTFS junction/reparse point，exFAT/FAT32 会失败），并且
>   **目录不要预先创建**——WinFsp 会自己创建，已存在会报
>   `0xD0000035 (STATUS_OBJECT_NAME_COLLISION)`。
> - 元数据默认使用文件版 SQLite（`./data/brewfs-meta.db`，挂载进程工作目录下
>   的 `data/` 会自动创建）。不要用 `--meta-url sqlite::memory:` 做真实挂载：
>   SQLite 内存库在连接池下每连接互相隔离，会导致“表不存在”/I/O 错误。

### 4.2 阿里云 OSS / S3 兼容对象存储

BrewFS 通过 S3 兼容接口访问 OSS（endpoint 用 S3 形态的
`s3.<region>.aliyuncs.com`，不是 OSS AccessPoint 的
`oss-accesspoint.aliyuncs.com`——后者只支持官方 Java/Python SDK）：

```powershell
$env:AWS_ACCESS_KEY_ID="<你的 AccessKeyId>"
$env:AWS_SECRET_ACCESS_KEY="<你的 AccessKeySecret>"
.\target\release\brewfs.exe mount `
  --data-backend s3 `
  --s3-bucket oss-bucket-name `
  --s3-region cn-shanghai `
  --s3-endpoint https://s3.cn-shanghai.aliyuncs.com `
  Z:
```

> ⚠️ 密钥安全：AccessKey 不要写进命令行/脚本/仓库。优先用环境变量或
> 密钥管理系统，泄露后立即到阿里云控制台轮换。

### 4.3 停止

在挂载进程里按 `Ctrl+C` 会自动卸载（unmount）。挂载信息会打印在控制台。

## 五、控制平面（`brewfs info` / `brewfs gc`）

Windows 上控制平面走 **named pipe**（`\\.\pipe\brewfs-<pid>`），Unix 上仍走
Unix domain socket，协议（JSON）完全一致：

```powershell
# 查看正在运行的挂载实例
.\target\release\brewfs.exe info

# 对指定挂载点发起 GC（dry-run）
.\target\release\brewfs.exe gc --mount-point Z: --dry-run
```

## 六、当前已知限制（WinFsp 适配）

- **符号链接**：BrewFS 中的 symlink 在 Windows 上显示为 reparse point 文件，
  暂不做解析（WinFsp 卷参数 `reparse_points=false`）。
- **ACL**：不持久化 Windows ACL（`persistent_acls=false`），WinFsp 会为文件
  生成默认安全描述符。
- **大小写敏感**：底层 POSIX 命名空间大小写敏感，挂载后 `Foo` 与 `foo` 是
  不同文件（`case_sensitive_search=true`）。
- **Windows 保留名**：`CON`/`NUL`/`COM1` 等 DOS 保留名未做专门处理。
- **字节范围锁、扩展属性、命名流**：暂未映射。
- **PowerShell `Remove-Item` 删除空目录**：偶发报“找不到文件”
  （PowerShell 目录删除走 `FileDispositionInfo` 的特定组合）；可用
  `cmd /c rmdir Z:\dir` 或 `os.rmdir`，文件删除不受影响。
- **cmd `dir /b Z:\`（尾带反斜杠）**：cmd 传参问题会报“文件名、目录名或卷
  标语法不正确”；用 `dir /b Z:\*` 即可正常列出。
- 卷容量：`statfs` 返回 BrewFS 元数据层的配额/容量快照。

## 七、常见问题

| 现象 | 原因 / 处理 |
|---|---|
| `WinFsp is not installed or could not be loaded` | 未安装 WinFsp，或安装后未重启进程。安装后重试。 |
| `failed to mount at X:` | 盘符已被占用，或 WinFsp 驱动未运行（管理员权限安装后重启）。 |
| `failed to create WinFsp filesystem host` | WinFsp 服务未启动，或版本过旧（需 2.x）。 |
| `failed to mount at ...: 0xD0000035` | 挂载点目录已存在（WinFsp 会自己创建，别预建）；或目录所在卷不是 NTFS（exFAT/FAT32 不支持目录挂载点）。 |
| `failed to mount at ...: 0xD000000D` | WinFsp 参数无效：通常发生在给 `VolumeParams` 设置了不合法的 `prefix`（必须形如 `\Server\Share`）。当前实现不用前缀，盘符/文件夹都走 `WinFsp.Disk`。 |
| 目录列出时报“I/O 设备错误” | 元数据连接异常。默认已用文件版 SQLite；若用了 `sqlite::memory:` 会因连接隔离报“表不存在”，换成文件版或 `--meta-url sqlite://...db?mode=rwc`。 |
| `brewfs gc` 报连不上 | 确认挂载进程仍在运行；named pipe 由挂载进程持有。 |

## 八、验证（冒烟测试）

```powershell
# 1. 挂载
.\target\release\brewfs.exe mount --data-backend local-fs --data-dir .\data Z:

# 2. 在另一个终端里读写
New-Item Z:\hello.txt -ItemType File -Value "hello brewfs"
Get-Content Z:\hello.txt
New-Item Z:\docs -ItemType Directory
Move-Item Z:\hello.txt Z:\docs\hello.txt
Remove-Item Z:\docs\hello.txt
Remove-Item Z:\docs

# 3. 控制平面
.\target\release\brewfs.exe info
.\target\release\brewfs.exe gc --mount-point Z: --dry-run

# 4. Ctrl+C 卸载
```
