#!/usr/bin/env bash
set -euo pipefail

REMOTE="${BRAIN_GIT_REMOTE:-swader@valkyrie:/home/swader/shared-brain/bare/shared-brain.git}"
TARGET="${SHARED_BRAIN_LOCAL_PATH:-$HOME/shared-brain}"
CONFIG_DIR="$HOME/.config"
ENV_FILE="$CONFIG_DIR/shared-brain.env"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

backup_file() {
  local path="$1"
  if [ -e "$path" ] || [ -L "$path" ]; then
    cp -a "$path" "$path.brainstack-backup-$STAMP"
  fi
}

mkdir -p "$CONFIG_DIR"
if [ -d "$TARGET/.git" ]; then
  git -C "$TARGET" pull --ff-only
else
  git clone "$REMOTE" "$TARGET"
fi

if [ ! -f "$ENV_FILE" ]; then
  cp "$(dirname "$0")/client.env.example" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
fi

CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
mkdir -p "$CODEX_HOME"
if [ ! -f "$CODEX_HOME/AGENTS.md" ]; then
  cp "$(dirname "$0")/codex-global-AGENTS.md" "$CODEX_HOME/AGENTS.md"
fi

CLAUDE_HOME="$HOME/.claude"
mkdir -p "$CLAUDE_HOME"
if [ ! -f "$CLAUDE_HOME/CLAUDE.md" ]; then
  cp "$(dirname "$0")/claude-user-CLAUDE.md" "$CLAUDE_HOME/CLAUDE.md"
fi

CURSOR_RULE_DIR="$HOME/.cursor/rules"
mkdir -p "$CURSOR_RULE_DIR"
if [ ! -f "$CURSOR_RULE_DIR/shared-brain.md" ]; then
  cp "$(dirname "$0")/cursor-user-rule.md" "$CURSOR_RULE_DIR/shared-brain.md"
fi

echo "shared brain client installed or updated at $TARGET"

