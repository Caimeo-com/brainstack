#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if [ -n "$(git status --porcelain)" ]; then
  echo "release refused: git tree is dirty" >&2
  git status --short >&2
  exit 1
fi

mkdir -p dist

bun install --frozen-lockfile
bun test

checksum() {
  path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
  elif command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "$path" | awk '{print $NF}'
  else
    echo "release refused: need sha256sum, shasum, or openssl for checksums" >&2
    exit 1
  fi
}

commit="$(git rev-parse --short HEAD)"
version="${BRAINSTACK_RELEASE_VERSION:-$commit}"
targets="${BRAINSTACK_RELEASE_TARGETS:-darwin-arm64 darwin-x64 linux-arm64 linux-x64}"

case "$version" in
  ""|*[!A-Za-z0-9._-]*)
    echo "release refused: invalid BRAINSTACK_RELEASE_VERSION: $version" >&2
    exit 1
    ;;
esac

rm -f dist/brainctl dist/brainctl.sha256 dist/brainctl-* dist/manifest.json dist/install.sh dist/brainstack-*.tar.gz dist/brainstack-*.tar.gz.sha256

manifest_tmp="$(mktemp)"
{
  printf '{\n'
  printf '  "schema_version": 1,\n'
  printf '  "version": "%s",\n' "$version"
  printf '  "commit": "%s",\n' "$commit"
  printf '  "assets": [\n'
} > "$manifest_tmp"

first=1
for target in $targets; do
  case "$target" in
    darwin-arm64|darwin-x64|linux-arm64|linux-x64) ;;
    *)
      echo "release refused: unsupported target: $target" >&2
      exit 1
      ;;
  esac
  outfile="dist/brainctl-${target}"
  bun build packages/brainctl/src/main.ts \
    --compile \
    --target="bun-${target}" \
    --no-compile-autoload-dotenv \
    --no-compile-autoload-bunfig \
    --outfile "$outfile"
  if [[ "$target" == darwin-* ]] && command -v codesign >/dev/null 2>&1; then
    codesign --force --sign - "$outfile" >/dev/null
  fi
  digest="$(checksum "$outfile")"
  printf '%s  %s\n' "$digest" "$(basename "$outfile")" > "${outfile}.sha256"
  os="${target%-*}"
  arch="${target##*-}"
  if [ "$first" -eq 0 ]; then
    printf ',\n' >> "$manifest_tmp"
  fi
  first=0
  printf '    {"name":"%s","os":"%s","arch":"%s","sha256":"%s"}' "$(basename "$outfile")" "$os" "$arch" "$digest" >> "$manifest_tmp"
done

{
  printf '\n'
  printf '  ]\n'
  printf '}\n'
} >> "$manifest_tmp"

mv "$manifest_tmp" dist/manifest.json
install -m 0755 scripts/install.sh dist/install.sh

archive="dist/brainstack-${version}.tar.gz"
git archive --format=tar.gz --prefix="brainstack-${version}/" HEAD > "$archive"
printf '%s  %s\n' "$(checksum "$archive")" "$(basename "$archive")" > "${archive}.sha256"

echo "brainctl assets:"
for target in $targets; do
  echo "  dist/brainctl-${target}"
done
echo "installer: dist/install.sh"
echo "manifest: dist/manifest.json"
echo "source archive: $archive"
