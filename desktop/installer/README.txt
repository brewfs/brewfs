BrewFS 托盘（Windows）
======================

把阿里云 OSS / S3 兼容 bucket 直接挂载成 Windows 盘符（多机共享网盘，
无本地元数据）。

- brewfs-tray.exe   系统托盘管理界面（配置、挂载/卸载）
- ossmount.exe      底层挂载进程（WinFsp 后端）

使用前请先安装 WinFsp（本安装包会自动安装）。macOS/Linux 请直接使用
ossmount（需要 macFUSE / libfuse）。

仓库：https://github.com/brewfs/brewfs