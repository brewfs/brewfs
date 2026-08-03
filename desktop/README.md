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

## 开发

```powershell
cargo fmt -p brewfs-tray -- --check
cargo clippy -p brewfs-tray --all-targets
cargo test -p brewfs-tray
```
