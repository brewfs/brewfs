#!/usr/bin/env bash
# Build a proper macOS .app bundle for BrewFS so the Dock/taskbar shows the
# BrewFS icon (blue cloud-download) instead of the generic tan "Unix
# executable" icon. Requires a successful `cargo build --release -p brewfs-tray`.
#
# Usage:  bash desktop/scripts/make-macos-app.sh
# Output: target/release/BrewFS.app
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
APP="$ROOT/target/release/BrewFS.app"
CONTENTS="$APP/Contents"
MACOS="$CONTENTS/MacOS"
RESOURCES="$CONTENTS/Resources"

BIN="$ROOT/target/release/brewfs-tray"
if [ ! -x "$BIN" ]; then
  echo "brewfs-tray binary not found; run first:"
  echo "  cargo build --release -p brewfs-tray"
  exit 1
fi

rm -rf "$APP"
mkdir -p "$MACOS" "$RESOURCES"

cp "$BIN" "$MACOS/brewfs-tray"
cp "$ROOT/desktop/assets/brewfs.icns" "$RESOURCES/brewfs.icns"

cat > "$CONTENTS/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>BrewFS</string>
  <key>CFBundleDisplayName</key><string>BrewFS</string>
  <key>CFBundleIdentifier</key><string>dev.brewfs.tray</string>
  <key>CFBundleVersion</key><string>0.1.0</string>
  <key>CFBundleShortVersionString</key><string>0.1.0</string>
  <key>CFBundleExecutable</key><string>brewfs-tray</string>
  <key>CFBundleIconFile</key><string>brewfs</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>LSMinimumSystemVersion</key><string>11.0</string>
  <key>NSHighResolutionCapable</key><true/>
</dict>
</plist>
PLIST

chmod +x "$MACOS/brewfs-tray"
echo "Built $APP"
echo "Launch with: open $APP"
