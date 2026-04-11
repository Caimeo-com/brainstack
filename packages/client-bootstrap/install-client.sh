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
cp "$(dirname "$0")/codex-shared-brain.include.md" "$BOOTSTRAP_DIR/codex-shared-brain.include.md"
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
  ln -s "$BOOTSTRAP_DIR/codex-shared-brain.include.md" "$CODEX_HOME/AGENTS.md"
else
  echo "Codex already has $CODEX_HOME/AGENTS.md; append the real shared-brain guidance with:"
  echo "cat $BOOTSTRAP_DIR/codex-shared-brain.include.md >> $CODEX_HOME/AGENTS.md"
fi

CLAUDE_HOME="$HOME/.claude"
mkdir -p "$CLAUDE_HOME"
if [ ! -f "$CLAUDE_HOME/CLAUDE.md" ]; then
  cat > "$CLAUDE_HOME/CLAUDE.md" <<'STUB'
@~/.config/brainstack/client-bootstrap/claude-user-CLAUDE.md
STUB
else
  echo "Claude already has $CLAUDE_HOME/CLAUDE.md; append this exact import line manually:"
  echo "@~/.config/brainstack/client-bootstrap/claude-user-CLAUDE.md"
fi

CURSOR_RULE_DIR="$HOME/.cursor/rules"
mkdir -p "$CURSOR_RULE_DIR"
if [ ! -f "$CURSOR_RULE_DIR/shared-brain.md" ]; then
  cp "$BOOTSTRAP_DIR/cursor-user-rule.md" "$CURSOR_RULE_DIR/shared-brain.md"
else
  echo "Cursor shared-brain rule already exists at $CURSOR_RULE_DIR/shared-brain.md; append or merge the actual rule content with:"
  echo "cat $BOOTSTRAP_DIR/cursor-user-rule.md >> $CURSOR_RULE_DIR/shared-brain.md"
fi

echo "shared brain client installed or updated at $TARGET"
echo "product-owned bootstrap snippets are in $BOOTSTRAP_DIR"
