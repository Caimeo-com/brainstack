#!/usr/bin/env bash
set -euo pipefail

REMOTE="${BRAIN_GIT_REMOTE:-operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git}"
TARGET="${SHARED_BRAIN_LOCAL_PATH:-$HOME/shared-brain}"
CONFIG_DIR="$HOME/.config"
ENV_FILE="$CONFIG_DIR/shared-brain.env"
BOOTSTRAP_DIR="$CONFIG_DIR/brainstack/client-bootstrap"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

backup_file() {
  local path="$1"
  if [ -e "$path" ] || [ -L "$path" ]; then
    cp -a "$path" "$path.brainstack-backup-$STAMP"
  fi
}

mkdir -p "$CONFIG_DIR"
mkdir -p "$BOOTSTRAP_DIR"
cp "$(dirname "$0")/codex-global-AGENTS.md" "$BOOTSTRAP_DIR/codex-global-AGENTS.md"
cp "$(dirname "$0")/claude-user-CLAUDE.md" "$BOOTSTRAP_DIR/claude-user-CLAUDE.md"
cp "$(dirname "$0")/cursor-user-rule.md" "$BOOTSTRAP_DIR/cursor-user-rule.md"
cp "$(dirname "$0")/claude-hooks-example.json" "$BOOTSTRAP_DIR/claude-hooks-example.json"
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
  cat > "$CODEX_HOME/AGENTS.md" <<'STUB'
# Codex Local Instructions

Read the product-owned shared-brain snippet at ~/.config/brainstack/client-bootstrap/codex-global-AGENTS.md.
STUB
else
  echo "Codex already has $CODEX_HOME/AGENTS.md; add this line manually if desired:"
  echo "Read ~/.config/brainstack/client-bootstrap/codex-global-AGENTS.md for shared-brain client guidance."
fi

CLAUDE_HOME="$HOME/.claude"
mkdir -p "$CLAUDE_HOME"
if [ ! -f "$CLAUDE_HOME/CLAUDE.md" ]; then
  cat > "$CLAUDE_HOME/CLAUDE.md" <<'STUB'
# Claude Local Instructions

Import ~/.config/brainstack/client-bootstrap/claude-user-CLAUDE.md.
STUB
else
  echo "Claude already has $CLAUDE_HOME/CLAUDE.md; add this line manually if desired:"
  echo "Import ~/.config/brainstack/client-bootstrap/claude-user-CLAUDE.md."
fi

CURSOR_RULE_DIR="$HOME/.cursor/rules"
mkdir -p "$CURSOR_RULE_DIR"
if [ ! -f "$CURSOR_RULE_DIR/shared-brain.md" ]; then
  cat > "$CURSOR_RULE_DIR/shared-brain.md" <<'STUB'
# Shared Brain

Read ~/.config/brainstack/client-bootstrap/cursor-user-rule.md for the shared-brain client workflow.
STUB
else
  echo "Cursor shared-brain rule already exists at $CURSOR_RULE_DIR/shared-brain.md; compare it with $BOOTSTRAP_DIR/cursor-user-rule.md manually."
fi

echo "shared brain client installed or updated at $TARGET"
echo "product-owned bootstrap snippets are in $BOOTSTRAP_DIR"
