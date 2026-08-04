# Builds the BrewFS Windows installer (WiX Burn bundle).
#
# Prerequisites:
#   - Rust stable (builds brewfs-tray.exe + ossmount.exe)
#   - WiX Toolset as a .NET tool: `dotnet tool install --global wix`
#
# Output:
#   desktop\installer\build\BrewFS-Setup-<version>.exe
#     - installs WinFsp 2.1 silently when not already installed
#     - installs BrewFS tray + ossmount to "%ProgramFiles%\BrewFS"
#     - creates a "BrewFS 托盘" Start Menu shortcut
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File desktop\installer\build-installer.ps1 [-Version 0.1.0]

param(
    [string]$Version = "0.1.0"
)

$ErrorActionPreference = "Stop"

$installerDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$root = Resolve-Path (Join-Path $installerDir "..\..")
$buildDir = Join-Path $installerDir "build"
$targetDir = Join-Path $root "target\release"

New-Item -ItemType Directory -Force -Path $buildDir | Out-Null

Write-Host "==> Building release binaries (this can take a while)..." -ForegroundColor Cyan
Push-Location $root
try {
    cargo build --release -p brewfs --bin ossmount --no-default-features --features fuse-winfsp
    if ($LASTEXITCODE -ne 0) { throw "cargo build ossmount failed" }
    cargo build --release -p brewfs-tray
    if ($LASTEXITCODE -ne 0) { throw "cargo build brewfs-tray failed" }
} finally {
    Pop-Location
}

$trayExe = Join-Path $targetDir "brewfs-tray.exe"
$ossmountExe = Join-Path $targetDir "ossmount.exe"
if (-not (Test-Path $trayExe)) { throw "missing $trayExe" }
if (-not (Test-Path $ossmountExe)) { throw "missing $ossmountExe" }

$wix = Get-Command wix -ErrorAction Stop | Select-Object -First 1
if (-not $wix) { throw "wix tool not found; run: dotnet tool install --global wix" }
wix extension add WixToolset.BootstrapperApplications.wixext -g 2>$null
wix extension add WixToolset.Util.wixext -g 2>$null

$appMsi = Join-Path $buildDir "brewfs-app.msi"
$readme = Join-Path $installerDir "README.txt"
$icon = Join-Path $installerDir "..\assets\brewfs.ico"

Write-Host "==> Building app MSI..." -ForegroundColor Cyan
& $wix.Source build (Join-Path $installerDir "brewfs-app.wxs") -arch x64 `
    -d "Version=$Version" `
    -d "TrayPath=$trayExe" `
    -d "OssmountPath=$ossmountExe" `
    -d "ReadmePath=$readme" `
    -o $appMsi
if ($LASTEXITCODE -ne 0) { throw "wix build app MSI failed" }

$bundleExe = Join-Path $buildDir "BrewFS-Setup-$Version.exe"
$winfspMsi = Join-Path $installerDir "winfsp-2.1.25156.msi"

Write-Host "==> Building installer bundle..." -ForegroundColor Cyan
& $wix.Source build (Join-Path $installerDir "brewfs-bundle.wxs") `
    -ext WixToolset.BootstrapperApplications.wixext `
    -ext WixToolset.Util.wixext `
    -d "Version=$Version" `
    -d "AppMsi=$appMsi" `
    -d "WinFspMsi=$winfspMsi" `
    -d "IconPath=$icon" `
    -o $bundleExe
if ($LASTEXITCODE -ne 0) { throw "wix build bundle failed" }

Write-Host ""
Write-Host "Installer ready: $bundleExe" -ForegroundColor Green