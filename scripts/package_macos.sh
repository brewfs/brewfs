#!/usr/bin/env bash
# Package BrewFS for macOS: build release binaries, assemble a signed .app,
# create a Developer ID-signed DMG, and notarize + staple it.
#
# Requirements:
#   - macOS with Xcode command line tools (codesign, hdiutil, xcrun notarytool)
#   - Developer ID Application identity in the login keychain
#   - macFUSE libs available for linking (pkg-config "fuse"); either installed
#     (brew install --cask macfuse) or a local extracted copy pointed to by
#     MACFUSE_PREFIX (default: ~/brewfs-deps/macfuse-5.3.3)
#   - Notarization credentials: set APPLE_ID, APPLE_TEAM_ID, APPLE_PASSWORD or
#     create ~/Documents/Apple Certificates/{app-specific-passwd.txt,team-id.txt}
#
# Usage:
#   bash scripts/package_macos.sh [--skip-notarize]
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# ---- config ----
IDENTITY="${IDENTITY:-Developer ID Application: qingfeng gao (XFXU84HVK3)}"
BUNDLE_ID="${BUNDLE_ID:-ai.brewfs.tray}"
VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)"
APP_NAME="BrewFS"
MACFUSE_PREFIX="${MACFUSE_PREFIX:-$HOME/brewfs-deps/macfuse-5.3.3}"
PKG_CONFIG_PATH="${MACFUSE_PREFIX}/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
export PKG_CONFIG_PATH

CERT_DIR="${CERT_DIR:-$HOME/Documents/Apple Certificates}"
APPLE_ID="${APPLE_ID:-}"
APPLE_TEAM_ID="${APPLE_TEAM_ID:-$(cat "$CERT_DIR/team-id.txt" 2>/dev/null || true)}"
APPLE_PASSWORD="${APPLE_PASSWORD:-$(cat "$CERT_DIR/app-specific-passwd.txt" 2>/dev/null || true)}"

SKIP_NOTARIZE=0
for arg in "$@"; do
  case "$arg" in
    --skip-notarize) SKIP_NOTARIZE=1 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

ARCH="$(uname -m)"
STAGE="dist/macos/staging"
APP="$STAGE/$APP_NAME.app"
DMG="dist/macos/BrewFS-${VERSION}-macos-${ARCH}.dmg"
ENTITLEMENTS="dist/macos/entitlements.plist"

# ---- 0. ensure plist templates ----
mkdir -p dist/macos
if [[ ! -f dist/macos/Info.plist ]]; then
cat > dist/macos/Info.plist <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>CFBundleDevelopmentRegion</key>
    <string>zh_CN</string>
    <key>CFBundleExecutable</key>
    <string>brewfs-tray</string>
    <key>CFBundleIdentifier</key>
    <string>ai.brewfs.tray</string>
    <key>CFBundleInfoDictionaryVersion</key>
    <string>6.0</string>
    <key>CFBundleName</key>
    <string>BrewFS</string>
    <key>CFBundleDisplayName</key>
    <string>BrewFS</string>
    <key>CFBundlePackageType</key>
    <string>APPL</string>
    <key>CFBundleShortVersionString</key>
    <string>0.1.2</string>
    <key>CFBundleVersion</key>
    <string>0.1.2</string>
    <key>CFBundleIconFile</key>
    <string>brewfs</string>
    <key>LSMinimumSystemVersion</key>
    <string>12.0</string>
    <key>LSApplicationCategoryType</key>
    <string>public.app-category.utilities</string>
    <key>NSHighResolutionCapable</key>
    <true/>
    <key>NSHumanReadableCopyright</key>
    <string>Copyright © 2026 rk8s-dev team. MIT License.</string>
</dict>
</plist>
PLIST
fi
if [[ ! -f dist/macos/entitlements.plist ]]; then
cat > dist/macos/entitlements.plist <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>com.apple.security.cs.disable-library-validation</key>
    <true/>
</dict>
</plist>
PLIST
fi

# ---- 1. build ----
echo "==> Building brewfs / ossmount (fuse-tokio-runtime, release)"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 \
  cargo build --release --no-default-features --features fuse-tokio-runtime \
  --bin brewfs --bin ossmount
echo "==> Building brewfs-tray"
CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 cargo build --release -p brewfs-tray

# ---- 2. assemble .app ----
echo "==> Assembling $APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp dist/macos/Info.plist "$APP/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleIdentifier $BUNDLE_ID" "$APP/Contents/Info.plist" 2>/dev/null || true
/usr/libexec/PlistBuddy -c "Set :CFBundleShortVersionString $VERSION" "$APP/Contents/Info.plist" 2>/dev/null || true
/usr/libexec/PlistBuddy -c "Set :CFBundleVersion $VERSION" "$APP/Contents/Info.plist" 2>/dev/null || true
cp target/release/brewfs-tray "$APP/Contents/MacOS/"
cp target/release/brewfs "$APP/Contents/MacOS/"
cp target/release/ossmount "$APP/Contents/MacOS/"
if [[ ! -f "$APP/Contents/Resources/brewfs.icns" ]]; then
  echo "==> Generating icns"
  ICONSET="dist/macos/iconset.iconset"
  rm -rf "$ICONSET"
  mkdir -p "$ICONSET"
  for s in 16 32 64 128 256 512; do
    sips -z "$s" "$s" desktop/assets/brewfs.png --out "$ICONSET/icon_${s}x${s}.png" >/dev/null
  done
  for s in 32 64 128 256 512 1024; do
    h=$((s / 2))
    sips -z "$s" "$s" desktop/assets/brewfs.png --out "$ICONSET/icon_${h}x${h}@2x.png" >/dev/null
  done
  iconutil -c icns "$ICONSET" -o "$APP/Contents/Resources/brewfs.icns"
fi
chmod +x "$APP/Contents/MacOS/"*

# ---- 3. sign ----
echo "==> Signing with $IDENTITY"
codesign --force --options runtime --timestamp \
  --entitlements "$ENTITLEMENTS" --sign "$IDENTITY" \
  "$APP/Contents/MacOS/brewfs"
codesign --force --options runtime --timestamp \
  --entitlements "$ENTITLEMENTS" --sign "$IDENTITY" \
  "$APP/Contents/MacOS/ossmount"
codesign --force --options runtime --timestamp \
  --entitlements "$ENTITLEMENTS" --sign "$IDENTITY" \
  "$APP/Contents/MacOS/brewfs-tray"
codesign --force --options runtime --timestamp \
  --entitlements "$ENTITLEMENTS" --sign "$IDENTITY" "$APP"
codesign --verify --deep --strict --verbose=2 "$APP"

# ---- 4. notarize the app (zip) and staple ----
if [[ "$SKIP_NOTARIZE" == "0" ]]; then
  if [[ -z "$APPLE_ID" || -z "$APPLE_PASSWORD" || -z "$APPLE_TEAM_ID" ]]; then
    echo "!! Notarization credentials missing (APPLE_ID / APPLE_PASSWORD / APPLE_TEAM_ID)." >&2
    echo "   App is signed but NOT notarized." >&2
    SKIP_NOTARIZE=1
  fi
fi
if [[ "$SKIP_NOTARIZE" == "0" ]]; then
  echo "==> Notarizing app"
  ZIP="dist/macos/${APP_NAME}-app.zip"
  ditto -c -k --keepParent "$APP" "$ZIP"
  xcrun notarytool submit "$ZIP" \
    --apple-id "$APPLE_ID" --password "$APPLE_PASSWORD" --team-id "$APPLE_TEAM_ID" \
    --wait
  xcrun stapler staple "$APP"
  spctl --assess --type execute --verbose=4 "$APP" || true
fi

# ---- 5. create DMG ----
# Bundle macFUSE installer + license alongside BrewFS.app when present
# (non-commercial redistribution is allowed under macFUSE's BSD-style
# license; see dist/macos/macfuse/License.rtf, condition 4).
DMG_ROOT="dist/macos/dmg-root"
rm -rf "$DMG_ROOT"
mkdir -p "$DMG_ROOT"
ditto "$APP" "$DMG_ROOT/$APP_NAME.app"
if [[ -d dist/macos/macfuse ]]; then
  cp dist/macos/macfuse/* "$DMG_ROOT/" 2>/dev/null || true
fi
echo "==> Creating DMG"
hdiutil create -volname "$APP_NAME" -srcfolder "$DMG_ROOT" -ov -format UDZO -fs HFS+ "$DMG"
codesign --force --sign "$IDENTITY" "$DMG"

if [[ "$SKIP_NOTARIZE" == "0" ]]; then
  echo "==> Notarizing DMG"
  xcrun notarytool submit "$DMG" \
    --apple-id "$APPLE_ID" --password "$APPLE_PASSWORD" --team-id "$APPLE_TEAM_ID" \
    --wait
  xcrun stapler staple "$DMG"
  spctl --assess --type open --context context:primary-signature --verbose=4 "$DMG" || true
fi

echo "==> Done: $DMG"
shasum -a 256 "$DMG"
