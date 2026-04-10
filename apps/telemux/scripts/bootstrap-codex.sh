#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
FORCE=0

usage() {
  cat <<'EOF'
Usage: ./scripts/bootstrap-codex.sh [--force] [--codex-home PATH]

Installs the tracked Clawdex Codex templates into the target Codex home.
By default it will not overwrite existing files.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      FORCE=1
      shift
      ;;
    --codex-home)
      CODEX_HOME="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$CODEX_HOME" ]]; then
  echo "Missing Codex home path" >&2
  exit 1
fi

install_template() {
  local src="$1"
  local dst="$2"

  if [[ -e "$dst" && "$FORCE" -ne 1 ]]; then
    echo "Skip existing $dst"
    return
  fi

  install -D -m 0644 "$src" "$dst"
  echo "Installed $dst"
}

mkdir -p "$CODEX_HOME"

install_template "$ROOT/templates/codex/config.toml.example" "$CODEX_HOME/config.toml"
install_template "$ROOT/templates/codex/AGENTS.md" "$CODEX_HOME/AGENTS.md"

cat <<EOF
Codex templates are in place under $CODEX_HOME

Review:
- $CODEX_HOME/config.toml
- $CODEX_HOME/AGENTS.md

If your Codex binary is not available as 'codex' in PATH, set FACTORY_CODEX_BIN in the repo's .env file.
EOF
