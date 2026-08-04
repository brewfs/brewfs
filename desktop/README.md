# BrewFS Desktop Tray（Windows）

基于 **Slint 1.17**（用户常称为 Slint 0.17；Slint 1.17 起原生支持
`SystemTrayIcon`，Windows 上走 `Shell_NotifyIcon`）的 BrewFS 桌面托盘应用：

- 编辑/保存挂载配置（数据后端 local-fs / s3、数据目录、S3 Bucket/Endpoint/Region/AK/SK、
  元数据后端与 URL、目标盘符）
- 实时展示「配置参数 ↔ 盘符映射」：读取 brewfs 运行时注册表
  （Windows 上位于 `%TEMP%\brewfs\*.json`）并过滤已退出的陈旧记录
- 一键挂载 / 卸载 / 打开资源管理器
- 系统托盘图标：左键显示窗口，右键菜单列出已挂盘符、卸载全部、退出
- 挂载失败时自动读取 `%LOCALAPPDATA%\brewfs-tray\logs\<配置名>.log` 尾行并显示原因

## 构建

需要 Rust stable（本仓库为 edition 2024）与 brewfs 的 WinFsp 构建：

```powershell
# 1. 构建 brewfs（WinFsp 后端）
cargo build -p brewfs --no-default-features --features fuse-winfsp

# 2. 构建托盘应用（会自动在旁边找到 brewfs.exe）
cargo build -p brewfs-tray
# 产物：target\debug\brewfs-tray.exe（release 用 --release）
```

Windows 上运行托盘应用会直接以无控制台窗口方式启动（`windows_subsystem =
"windows"`）。托盘图标在事件循环运行后出现；关闭主窗口只是隐藏到托盘，点托盘
“退出 BrewFS” 才结束进程。

## OSS 直挂模式（多机网盘，无本地元数据）
配置文件档案的"挂载模式"选 **OSS 直挂（多机）** 后，托盘应用不再走 BrewFS 元数据
（sqlite/redis/...），而是调用 `ossmount`（本仓库自带）把 **S3/OSS bucket 直接挂载成
盘符/挂载目录**：

- 文件路径直接编码为对象 key，bucket 是唯一数据源 → **任意多台机器挂同一
  bucket+prefix 都能看到同一棵树**，不需要共享元数据库
- 表单里填 Bucket / Endpoint / Region / AK / SK / Prefix（可选命名空间，多机要一致）
- 挂载命令：`ossmount --bucket B --endpoint E --region R [--prefix P] <挂载点>`（Windows 盘符 `Z:`，macOS/Linux 目录 `/Volumes/brewfs`）
- 卸载 = 结束进程（数据在关闭/刷盘时已整文件上传；macOS 上进程退出会触发 macFUSE 自动卸载）
- 弱一致（无锁、无原子改名）——适合网盘/上传下载，不适合并发改同一文件

需要先构建 ossmount 二进制：
```powershell
cargo build -p brewfs --bin ossmount --no-default-features --features fuse-winfsp
# 产物 target\debug\ossmount.exe，托盘应用会自动在旁边找到它（或用 OSSMOUNT_EXE 指定）
```

## 使用

- 首次运行在 `%APPDATA%\brewfs-tray\profiles.json` 生成/读取配置档案。
- 托盘应用通过子进程方式执行挂载，生成的 YAML 放在
  `%LOCALAPPDATA%\brewfs-tray\configs\`，日志在
  `%LOCALAPPDATA%\brewfs-tray\logs\`。
- S3 AccessKey/SecretKey 仅保存在本机 `profiles.json`（不入库、不上传），
  挂载时通过环境变量传给 brewfs；请勿把该文件提交到任何仓库。
- 若 brewfs.exe 不在托盘应用同目录，可设置环境变量 `BREWFS_EXE` 指定路径。

## 卸载说明

托盘应用优先走**优雅卸载**：向 brewfs 控制面发送 `Shutdown` 请求（即
`brewfs unmount <盘符>`），让挂载进程像收到 Ctrl+C 一样 flush 并干净拆卷
（`host.stop()` + `host.unmount()`），避免强杀进程导致 Explorer 枚举盘符时
短暂卡死/黑屏。仅当 brewfs 缺失、不认识 `unmount` 子命令或请求失败时才回退到
`taskkill /T /F` 强杀。

注意：`brewfs unmount` 需要 brewfs 二进制包含本仓库的 control-plane Shutdown
支持（重新构建 WinFsp 版即可）。BrewFS 默认写回模式为 `UploadBeforeCommit`
（先上传对象存储再提交元数据），即便强杀，最坏情况也只是最近未提交的文件不进入
目录树，可随后用 `brewfs gc` 清理孤儿对象。

## macOS 支持

- 托盘应用（Slint）跨平台：macOS 上系统托盘走 NSStatusItem，窗口原生渲染。
- 挂载点：macOS/Linux 用**目录路径**（如 `/Volumes/brewfs`），不再是盘符；
  表单字段已改为"挂载点"，校验同时接受 `Z:`（Windows）与 `/Volumes/...`（macOS）。
- **OSS 直挂（多机）模式 macOS/Linux 同样支持**：`ossmount` 在非 Windows 平台走
  FUSE（macOS 用 macFUSE 4.x，Linux 用 libfuse），挂载到目录而不是盘符，
  多机共享语义与 Windows 完全一致（bucket 是唯一数据源）。
- macOS 使用前提：先安装 macFUSE（`brew install --cask macfuse` 或
  https://macfuse.github.io/）；`ossmount` 启动时会检查
  `/Library/Filesystems/macfuse.fs` 并给出友好提示。
- 打开挂载点在 macOS 用 `open <路径>`；OSS 直挂卸载 = 向 `ossmount` 进程发送
  SIGTERM（`kill <pid>`），进程会优雅 umount 并清理运行时记录；BrewFS 元数据模式
  走 `brewfs unmount` 优雅路径，兜底用 `kill`。
- 构建 macOS 版需要在 Mac 上执行 `cargo build --release -p brewfs --bin ossmount`
  与 `cargo build --release -p brewfs-tray`（macFUSE 依赖需在 Mac 上链接）。
  当前仓库在 Windows 上仅能交叉 `cargo check --target x86_64-apple-darwin` 验证
  编译；挂载/读写等运行时行为需在真机 Mac 上验证。

## 开发

```powershell
cargo fmt -p brewfs-tray -- --check
cargo clippy -p brewfs-tray --all-targets
cargo test -p brewfs-tray
```
