#!/usr/bin/env bash
# Build, sign, and optionally notarize Brainstack Menu.app.
#
# Lanes:
#   scripts/make-app.sh                      local app bundle + dmg (Developer ID if present, else ad-hoc)
#   scripts/make-app.sh --notarize           distributable: Developer ID + hardened runtime +
#                                            notarytool submit + staple + Gatekeeper verification
#
# Environment:
#   CODESIGN_IDENTITY  override the signing identity (default: first "Developer ID Application"
#                      in the keychain, falling back to ad-hoc "-")
#   NOTARY_PROFILE     notarytool keychain profile name; one supported --notarize auth path
#                      (create one with: xcrun notarytool store-credentials "BrainstackNotary")
#   APP_STORE_CONNECT_API_KEY_PATH
#   APP_STORE_CONNECT_API_KEY_ID
#   APP_STORE_CONNECT_API_ISSUER_ID
#                      alternative --notarize auth path, useful for CI
set -euo pipefail

cd "$(dirname "$0")/.."

APP_NAME="Brainstack Menu"
BUNDLE_ID="com.caimeo.brainstack-menu"
VERSION="${BRAINSTACK_MENU_VERSION:-0.1.0}"
DIST_DIR="dist"
APP_DIR="$DIST_DIR/$APP_NAME.app"
ZIP_PATH="$DIST_DIR/BrainstackMenu-$VERSION.zip"
DMG_ROOT="$DIST_DIR/dmg-root"
DMG_PATH="$DIST_DIR/BrainstackMenu-$VERSION.dmg"

NOTARIZE=0
for arg in "$@"; do
  case "$arg" in
    --notarize) NOTARIZE=1 ;;
    *)
      echo "unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

case "$VERSION" in
  ""|*[!A-Za-z0-9._-]*)
    echo "refused: invalid BRAINSTACK_MENU_VERSION: $VERSION" >&2
    exit 1
    ;;
esac

# --- signing identity selection -------------------------------------------------
IDENTITY="${CODESIGN_IDENTITY:-}"
if [ -z "$IDENTITY" ]; then
  IDENTITY="$(security find-identity -v -p codesigning 2>/dev/null | sed -n 's/.*"\(Developer ID Application: [^"]*\)".*/\1/p' | head -n 1 || true)"
fi
if [ -z "$IDENTITY" ]; then
  IDENTITY="-"
fi

if [ "$NOTARIZE" -eq 1 ]; then
  if [ "$IDENTITY" = "-" ]; then
    echo "refused: --notarize requires a Developer ID Application identity (ad-hoc signatures cannot be notarized)" >&2
    exit 1
  fi
  if [ -n "${NOTARY_PROFILE:-}" ]; then
    NOTARY_ARGS=(--keychain-profile "$NOTARY_PROFILE")
  elif [ -n "${APP_STORE_CONNECT_API_KEY_PATH:-}" ] && [ -n "${APP_STORE_CONNECT_API_KEY_ID:-}" ] && [ -n "${APP_STORE_CONNECT_API_ISSUER_ID:-}" ]; then
    NOTARY_ARGS=(--key "$APP_STORE_CONNECT_API_KEY_PATH" --key-id "$APP_STORE_CONNECT_API_KEY_ID" --issuer "$APP_STORE_CONNECT_API_ISSUER_ID")
  else
    echo "refused: --notarize requires NOTARY_PROFILE or APP_STORE_CONNECT_API_KEY_PATH/APP_STORE_CONNECT_API_KEY_ID/APP_STORE_CONNECT_API_ISSUER_ID" >&2
    exit 1
  fi
fi

# --- build ------------------------------------------------------------------------
swift build -c release

rm -rf "$APP_DIR" "$ZIP_PATH" "$DMG_ROOT" "$DMG_PATH"
mkdir -p "$APP_DIR/Contents/MacOS" "$APP_DIR/Contents/Resources"

cp ".build/release/BrainstackMenu" "$APP_DIR/Contents/MacOS/$APP_NAME"

# --- app icon ----------------------------------------------------------------------
ICON_SOURCE="icon/icon-1024.png"
ICON_KEY=""
if [ -f "$ICON_SOURCE" ]; then
  ICONSET="$(mktemp -d)/AppIcon.iconset"
  mkdir -p "$ICONSET"
  for size in 16 32 128 256 512; do
    sips -z "$size" "$size" "$ICON_SOURCE" --out "$ICONSET/icon_${size}x${size}.png" >/dev/null
    double=$((size * 2))
    sips -z "$double" "$double" "$ICON_SOURCE" --out "$ICONSET/icon_${size}x${size}@2x.png" >/dev/null
  done
  iconutil -c icns "$ICONSET" -o "$APP_DIR/Contents/Resources/AppIcon.icns"
  rm -rf "$(dirname "$ICONSET")"
  ICON_KEY="  <key>CFBundleIconFile</key>
  <string>AppIcon</string>"
else
  echo "note: $ICON_SOURCE missing; building without an app icon" >&2
fi

cat > "$APP_DIR/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleExecutable</key>
  <string>$APP_NAME</string>
  <key>CFBundleIdentifier</key>
  <string>$BUNDLE_ID</string>
  <key>CFBundleName</key>
  <string>$APP_NAME</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>$VERSION</string>
  <key>CFBundleVersion</key>
  <string>$VERSION</string>
$ICON_KEY
  <key>LSMinimumSystemVersion</key>
  <string>13.0</string>
  <key>LSUIElement</key>
  <true/>
  <key>NSHumanReadableCopyright</key>
  <string>Brainstack</string>
</dict>
</plist>
PLIST

# --- sign --------------------------------------------------------------------------
if [ "$IDENTITY" = "-" ]; then
  echo "signing: ad-hoc (local use only; set CODESIGN_IDENTITY or install a Developer ID cert for distribution)"
  codesign --force --sign - "$APP_DIR"
else
  echo "signing: $IDENTITY (hardened runtime)"
  codesign --force --options runtime --timestamp --sign "$IDENTITY" "$APP_DIR"
fi

codesign --verify --deep --strict --verbose=2 "$APP_DIR"

# --- package ------------------------------------------------------------------------
ditto -c -k --keepParent "$APP_DIR" "$ZIP_PATH"
mkdir -p "$DMG_ROOT"
cp -R "$APP_DIR" "$DMG_ROOT/"
ln -s /Applications "$DMG_ROOT/Applications"
hdiutil create -volname "$APP_NAME" -srcfolder "$DMG_ROOT" -ov -format UDZO "$DMG_PATH" >/dev/null
rm -rf "$DMG_ROOT"

if [ "$IDENTITY" != "-" ]; then
  codesign --force --timestamp --sign "$IDENTITY" "$DMG_PATH"
  codesign --verify --verbose=2 "$DMG_PATH"
fi

# --- notarize -------------------------------------------------------------------------
if [ "$NOTARIZE" -eq 1 ]; then
  echo "notarizing app archive …"
  xcrun notarytool submit "$ZIP_PATH" "${NOTARY_ARGS[@]}" --wait
  xcrun stapler staple "$APP_DIR"
  xcrun stapler validate "$APP_DIR"
  # Re-zip so the distributed archive contains the stapled app.
  rm -f "$ZIP_PATH"
  ditto -c -k --keepParent "$APP_DIR" "$ZIP_PATH"
  rm -rf "$DMG_ROOT" "$DMG_PATH"
  mkdir -p "$DMG_ROOT"
  cp -R "$APP_DIR" "$DMG_ROOT/"
  ln -s /Applications "$DMG_ROOT/Applications"
  hdiutil create -volname "$APP_NAME" -srcfolder "$DMG_ROOT" -ov -format UDZO "$DMG_PATH" >/dev/null
  rm -rf "$DMG_ROOT"
  codesign --force --timestamp --sign "$IDENTITY" "$DMG_PATH"
  codesign --verify --verbose=2 "$DMG_PATH"
  echo "notarizing dmg …"
  xcrun notarytool submit "$DMG_PATH" "${NOTARY_ARGS[@]}" --wait
  xcrun stapler staple "$DMG_PATH"
  xcrun stapler validate "$DMG_PATH"
  spctl --assess --type execute --verbose "$APP_DIR"
  spctl --assess --type open --context context:primary-signature --verbose "$DMG_PATH"
fi

echo "built: $APP_DIR"
echo "archive: $ZIP_PATH"
echo "dmg: $DMG_PATH"
echo "install: cp -R \"$APP_DIR\" /Applications/"
