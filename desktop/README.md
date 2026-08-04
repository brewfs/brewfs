# BrewFS Desktop Tray（Windows）

基于 **Slint 1.17**（用户常称为 Slint 0.17；Slint 1.17 起原生支持
`SystemTrayIcon`，Windows 上走 `Shell_NotifyIcon`）的 BrewFS 桌面托盘应用，
**只支持 OSS 直挂模式**（无本地元数据，多机共享网盘）：

- 编辑/保存 OSS 挂载配置（Bucket / Endpoint / Region / AK / SK / Prefix / 盘符）
- 实时展示「配置参数 ↔ 盘符映射」：读取 `ossmount` 运行时注册表
  （Windows 上位于 `%TEMP%\brewfs-oss\*.json`）并过滤已退出的陈旧记录
- 一键挂载 / 卸载 / 打开资源管理器
- 系统托盘图标：左键显示窗口，右键菜单列出已挂盘符、卸载全部、退出
- 挂载失败时自动读取 `%LOCALAPPDATA%\brewfs-tray\logs\<配置名>-oss.log` 尾行并显示原因

## OSS 直挂模式（多机网盘，无本地元数据）

托盘应用调用 `ossmount`（本仓库自带）把 **S3/OSS bucket 直接挂载成盘符/挂载目录**：

- 文件路径直接编码为对象 key，bucket 是唯一数据源 → **任意多台机器挂同一
  bucket+prefix 都能看到同一棵树**，不需要共享元数据库
- 表单里填 Bucket / Endpoint / Region / AK / SK / Prefix（可选命名空间，多机要一致）
- 挂载命令：`ossmount --bucket B --endpoint E --region R [--prefix P] <挂载点>`
  （Windows 盘符 `Z:`，macOS/Linux 目录 `/Volumes/brewfs`）
- 卸载 = 结束进程（数据在关闭/刷盘时已整文件上传；WinFsp 在进程退出时自动拆卷，
  macOS/Linux 上进程收到 SIGTERM 后优雅 umount）
- 弱一致（无锁、无原子改名）——适合网盘/上传下载，不适合并发改同一文件

### 定时刷新（多机目录同步）

`ossmount` 内置定时刷新，让**其他机器**写入的文件自动出现在本机目录里：

- **Windows（WinFsp）**：每 10 秒，当有资源管理器窗口正在浏览挂载目录时，
  重新列出 bucket 根目录并对变化的条目发 `FILE_ACTION_ADDED/REMOVED/MODIFIED`
  通知（无窗口浏览时不产生任何 S3 请求）。子目录仍靠 1s 目录缓存超时自动重列。
- **macOS/Linux（FUSE）**：每 10 秒（`--refresh-secs` 可调，0 关闭）对最近浏览过的
  目录做内核缓存失效（`FUSE_NOTIFY_INVAL_INODE`），下次访问时重新列目录。

## 构建

需要 Rust stable（本仓库为 edition 2024）与 brewfs 的 WinFsp 构建：

```powershell
# 1. 构建 ossmount（WinFsp 后端）
cargo build -p brewfs --bin ossmount --no-default-features --features fuse-winfsp

# 2. 构建托盘应用（会自动在旁边找到 ossmount.exe）
cargo build -p brewfs-tray
# 产物：target\debug\brewfs-tray.exe（release 用 --release）
```

Windows 上运行托盘应用会直接以无控制台窗口方式启动（`windows_subsystem =
"windows"`）。托盘图标在事件循环运行后出现；关闭主窗口只是隐藏到托盘，点托盘
“退出 BrewFS” 才结束进程。

## Windows 安装包

`desktop/installer/build-installer.ps1` 用 **WiX Toolset**（`dotnet tool install --global wix`）
构建安装程序，自动完成：

1. 构建 release 版 `brewfs-tray.exe` + `ossmount.exe`
2. 打 **WinFsp 2.1**（`desktop/installer/winfsp-2.1.25156.msi`，2MB）进 Burn bundle：
   - 目标机已装 WinFsp（内核服务 `WinFsp` 存在）→ 跳过
   - 未装 → 静默安装；卸载 BrewFS 时**保留** WinFsp（共享系统组件）
3. 把两个 exe 装到 `%ProgramFiles%\BrewFS`，并创建「BrewFS 托盘」开始菜单快捷方式

```powershell
powershell -ExecutionPolicy Bypass -File desktop\installer\build-installer.ps1 -Version 0.1.0
# 产物：desktop\installer\build\BrewFS-Setup-0.1.0.exe
```

## 应用图标

图标统一以矢量源 `assets/brewfs-icon.svg` 为准（蓝色圆角方块 + 白色云朵内嵌向下箭头，
寓意"云端网盘挂载到本地"；箭头为云朵镂空，16px 托盘与 256px 应用图标渲染一致），构建时按平台生成不同格式：

- `assets/brewfs.png`（256×256）：Slint 窗口图标 + 系统托盘图标（`MainWindow.icon`
  与 `SystemTrayIcon.icon`）
- `assets/brewfs.ico`（16/24/32/48/64 经典 DIB 条目）：Windows 通过 `build.rs` 的
  `embed-resource` + `app.rc` 嵌入 exe，Explorer / 任务栏 / Alt-Tab 都能显示
- `assets/brewfs.icns`：macOS 应用包图标

macOS 打包成 .app 时，把 `brewfs.icns` 放进 `Contents/Resources/`，并在
`Contents/Info.plist` 里加 `CFBundleIconFile`（值为 `brewfs`）即可。

## 使用

- 首次运行在 `%APPDATA%\brewfs-tray\profiles.json` 生成/读取配置档案。
- 托盘应用通过子进程方式执行挂载，日志在 `%LOCALAPPDATA%\brewfs-tray\logs\`。
- S3 AccessKey/SecretKey 仅保存在本机 `profiles.json`（不入库、不上传），
  挂载时通过环境变量传给 ossmount；请勿把该文件提交到任何仓库。
- 若 ossmount.exe 不在托盘应用同目录，可设置环境变量 `OSSMOUNT_EXE` 指定路径。

## 卸载说明

所有挂载都是元数据无关的 `ossmount` 实例：数据在关闭/刷盘时已整文件上传到对象存储，
**直接结束进程即可安全卸载**（WinFsp 在进程退出时自动拆卷，不会出现 Explorer
枚举盘符卡死/黑屏）。托盘卸载时先结束进程再刷新列表，无需二次确认之外的额外操作。

## macOS 支持

- 托盘应用（Slint）跨平台：macOS 上系统托盘走 NSStatusItem，窗口原生渲染。
- 挂载点：macOS/Linux 用**目录路径**（如 `/Volumes/brewfs`），不再是盘符；
  表单字段已改为"挂载点"，校验同时接受 `Z:`（Windows）与 `/Volumes/...`（macOS）。
- **OSS 直挂模式 macOS/Linux 同样支持**：`ossmount` 在非 Windows 平台走
  FUSE（macOS 用 macFUSE 4.x，Linux 用 libfuse），挂载到目录而不是盘符，
  多机共享语义与 Windows 完全一致（bucket 是唯一数据源）。
- macOS 使用前提：先安装 macFUSE（`brew install --cask macfuse` 或
  https://macfuse.github.io/）；`ossmount` 启动时会检查
  `/Library/Filesystems/macfuse.fs` 并给出友好提示。
- 打开挂载点在 macOS 用 `open <路径>`；OSS 直挂卸载 = 向 `ossmount` 进程发送
  SIGTERM（`kill <pid>`），进程会优雅 umount 并清理运行时记录。
- 构建 macOS 版需要在 Mac 上执行 `cargo build --release -p brewfs --bin ossmount`
  与 `cargo build --release -p brewfs-tray`（macFUSE 依赖需在 Mac 上链接）。
- 直接运行裸二进制时，macOS Dock 会显示系统默认的米黄色「Unix 可执行文件」
  图标（看起来偏橙）。要让 Dock/启动台显示 BrewFS 蓝色图标，先构建再打包成 .app：

  ```bash
  cargo build --release -p brewfs-tray
  bash desktop/scripts/make-macos-app.sh
  open target/release/BrewFS.app
  ```
  当前仓库在 Windows 上仅能交叉 `cargo check --target x86_64-apple-darwin` 验证
  编译；挂载/读写等运行时行为需在真机 Mac 上验证。

## 开发

```powershell
cargo fmt -p brewfs-tray -- --check
cargo clippy -p brewfs-tray --all-targets
cargo test -p brewfs-tray
```
