#!/usr/bin/env bash
# Build, sign, and optionally notarize Brainstack Menu.app.
#
# Lanes:
#   scripts/make-app.sh                      local app bundle + dmg (Developer ID if present, else ad-hoc)
#   scripts/make-app.sh --notarize           distributable: Developer ID + hardened runtime +
#                                            notarytool submit + staple + Gatekeeper verification
#   scripts/make-app.sh --skip-build         package existing Swift build outputs
#
# Environment:
#   BRAINSTACK_MENU_ARCHES  space-separated app/helper architectures: arm64, x64,
#                      or both. Default: current host arch. Release script uses both.
#   BRAINSTACK_MENU_BRAINCTL_SOURCE
#                      optional path to a prebuilt brainctl to bundle; otherwise the
#                      script uses/builds dist/brainctl-darwin-<arch>.
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
REPO_ROOT="$(git rev-parse --show-toplevel)"

APP_NAME="Brainstack Menu"
BUNDLE_ID="com.caimeo.brainstack-menu"
VERSION="${BRAINSTACK_MENU_VERSION:-0.1.0}"
DIST_DIR="dist"
APP_DIR="$DIST_DIR/$APP_NAME.app"
ZIP_PATH="$DIST_DIR/BrainstackMenu-$VERSION.zip"
DMG_ROOT="$DIST_DIR/dmg-root"
DMG_PATH="$DIST_DIR/BrainstackMenu-$VERSION.dmg"

NOTARIZE=0
SKIP_BUILD=0
for arg in "$@"; do
  case "$arg" in
    --notarize) NOTARIZE=1 ;;
    --skip-build) SKIP_BUILD=1 ;;
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

host_arch() {
  case "$(uname -m)" in
    arm64|aarch64) printf '%s\n' "arm64" ;;
    x86_64|amd64) printf '%s\n' "x64" ;;
    *)
      echo "refused: unsupported host architecture: $(uname -m)" >&2
      exit 1
      ;;
  esac
}

MENU_ARCHES="${BRAINSTACK_MENU_ARCHES:-$(host_arch)}"
declare -a ARCHES=()
ARCH_COUNT=0
for arch in $MENU_ARCHES; do
  case "$arch" in
    arm64|aarch64) normalized="arm64" ;;
    x64|x86_64|amd64) normalized="x64" ;;
    *)
      echo "refused: unsupported BRAINSTACK_MENU_ARCHES entry: $arch" >&2
      exit 1
      ;;
  esac
  found=0
  if [ "$ARCH_COUNT" -gt 0 ]; then
    for existing in "${ARCHES[@]}"; do
      if [ "$existing" = "$normalized" ]; then
        found=1
        break
      fi
    done
  fi
  if [ "$found" -eq 0 ]; then
    ARCHES+=("$normalized")
    ARCH_COUNT=$((ARCH_COUNT + 1))
  fi
done
if [ "$ARCH_COUNT" -eq 0 ]; then
  echo "refused: BRAINSTACK_MENU_ARCHES selected no architectures" >&2
  exit 1
fi
LIPO_ARCH_ARGS=()
for arch in "${ARCHES[@]}"; do
  case "$arch" in
    arm64) LIPO_ARCH_ARGS+=(arm64) ;;
    x64) LIPO_ARCH_ARGS+=(x86_64) ;;
  esac
done

verify_arches() {
  path="$1"
  label="$2"
  if ! lipo "$path" -verify_arch "${LIPO_ARCH_ARGS[@]}" >/dev/null 2>&1; then
    echo "refused: $label is missing required architecture(s): ${LIPO_ARCH_ARGS[*]}" >&2
    lipo -info "$path" >&2 || true
    exit 1
  fi
}

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
SWIFT_ARCH_ARGS=()
for arch in "${ARCHES[@]}"; do
  case "$arch" in
    arm64) SWIFT_ARCH_ARGS+=(--arch arm64) ;;
    x64) SWIFT_ARCH_ARGS+=(--arch x86_64) ;;
  esac
done
if [ "$SKIP_BUILD" -eq 0 ]; then
  swift build -c release "${SWIFT_ARCH_ARGS[@]}"
fi

rm -rf "$APP_DIR" "$ZIP_PATH" "$DMG_ROOT" "$DMG_PATH"
mkdir -p "$APP_DIR/Contents/MacOS" "$APP_DIR/Contents/Resources"

SWIFT_BINARY=".build/release/BrainstackMenu"
if [ "$ARCH_COUNT" -gt 1 ]; then
  if [ ! -x ".build/apple/Products/Release/BrainstackMenu" ]; then
    echo "refused: universal Swift build output not found" >&2
    exit 1
  fi
  SWIFT_BINARY=".build/apple/Products/Release/BrainstackMenu"
elif [ ! -x "$SWIFT_BINARY" ] && [ -x ".build/apple/Products/Release/BrainstackMenu" ]; then
  SWIFT_BINARY=".build/apple/Products/Release/BrainstackMenu"
fi
if [ ! -x "$SWIFT_BINARY" ]; then
  echo "refused: built BrainstackMenu executable not found" >&2
  exit 1
fi
cp "$SWIFT_BINARY" "$APP_DIR/Contents/MacOS/$APP_NAME"
verify_arches "$APP_DIR/Contents/MacOS/$APP_NAME" "$APP_NAME executable"

ensure_brainctl_asset() {
  arch="$1"
  asset="$REPO_ROOT/dist/brainctl-darwin-$arch"
  if [ -x "$asset" ]; then
    printf '%s\n' "$asset"
    return
  fi
  if ! command -v bun >/dev/null 2>&1; then
    echo "refused: missing $asset and bun is unavailable to build it" >&2
    exit 1
  fi
  (
    cd "$REPO_ROOT"
    mkdir -p dist
    bun build packages/brainctl/src/main.ts \
      --compile \
      --target="bun-darwin-$arch" \
      --no-compile-autoload-dotenv \
      --no-compile-autoload-bunfig \
      --outfile "$asset" >&2
  )
  chmod 0755 "$asset"
  printf '%s\n' "$asset"
}

BUNDLED_BRAINCTL="$APP_DIR/Contents/Resources/brainctl"
if [ -n "${BRAINSTACK_MENU_BRAINCTL_SOURCE:-}" ]; then
  SOURCE_BRAINCTL="${BRAINSTACK_MENU_BRAINCTL_SOURCE/#\~/$HOME}"
  if [ ! -x "$SOURCE_BRAINCTL" ]; then
    echo "refused: BRAINSTACK_MENU_BRAINCTL_SOURCE is not executable: $SOURCE_BRAINCTL" >&2
    exit 1
  fi
  cp "$SOURCE_BRAINCTL" "$BUNDLED_BRAINCTL"
else
  BRAINCTL_INPUTS=()
  for arch in "${ARCHES[@]}"; do
    BRAINCTL_INPUTS+=("$(ensure_brainctl_asset "$arch")")
  done
  if [ "${#BRAINCTL_INPUTS[@]}" -gt 1 ]; then
    lipo -create "${BRAINCTL_INPUTS[@]}" -output "$BUNDLED_BRAINCTL"
  else
    cp "${BRAINCTL_INPUTS[0]}" "$BUNDLED_BRAINCTL"
  fi
fi
chmod 0755 "$BUNDLED_BRAINCTL"
verify_arches "$BUNDLED_BRAINCTL" "bundled brainctl"

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
  codesign --force --sign - "$BUNDLED_BRAINCTL"
  codesign --force --sign - "$APP_DIR"
else
  echo "signing: $IDENTITY (hardened runtime)"
  codesign --force --options runtime --timestamp --sign "$IDENTITY" "$BUNDLED_BRAINCTL"
  codesign --force --options runtime --timestamp --sign "$IDENTITY" "$APP_DIR"
fi

codesign --verify --strict --verbose=2 "$BUNDLED_BRAINCTL"
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
echo "bundled brainctl: $BUNDLED_BRAINCTL"
echo "archive: $ZIP_PATH"
echo "dmg: $DMG_PATH"
echo "install: cp -R \"$APP_DIR\" /Applications/"
