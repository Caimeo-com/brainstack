#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if [ -n "$(git status --porcelain)" ]; then
  echo "release refused: git tree is dirty" >&2
  git status --short >&2
  exit 1
fi

mkdir -p dist

bun test
bun build packages/brainctl/src/main.ts \
  --compile \
  --no-compile-autoload-dotenv \
  --no-compile-autoload-bunfig \
  --outfile dist/brainctl

version="$(git rev-parse --short HEAD)"
archive="dist/brainstack-${version}.tar.gz"
git archive --format=tar.gz --prefix="brainstack-${version}/" HEAD > "$archive"

echo "brainctl: dist/brainctl"
echo "source archive: $archive"

