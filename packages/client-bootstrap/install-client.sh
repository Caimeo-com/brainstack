#!/usr/bin/env bash
set -euo pipefail

REMOTE="${BRAIN_GIT_REMOTE:-__BRAIN_GIT_REMOTE__}"
TARGET="${SHARED_BRAIN_LOCAL_PATH:-__SHARED_BRAIN_LOCAL_PATH__}"
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

expand_path() {
  case "$1" in
    "~") printf '%s\n' "$HOME" ;;
    "~/"*) printf '%s/%s\n' "$HOME" "${1#\~/}" ;;
    /*) printf '%s\n' "$1" ;;
    *) printf '%s/%s\n' "$PWD" "$1" ;;
  esac
}

escape_sed_replacement() {
  printf '%s' "$1" | sed 's/[\/&]/\\&/g'
}

escape_sed_pattern() {
  printf '%s' "$1" | sed 's/[#\/&.^$*[]/\\&/g'
}

render_template() {
  local source="$1"
  local target="$2"
  local search="$3"
  local replacement="$4"
  local escaped_search
  local escaped
  escaped_search="$(escape_sed_pattern "$search")"
  escaped="$(escape_sed_replacement "$replacement")"
  sed "s#$escaped_search#$escaped#g" "$source" > "$target"
}

TARGET_ABS="$(expand_path "$TARGET")"
BOOTSTRAP_ABS="$(expand_path "$BOOTSTRAP_DIR")"

mkdir -p "$CONFIG_DIR"
mkdir -p "$BOOTSTRAP_DIR"
render_template "$(dirname "$0")/codex-global-AGENTS.md" "$BOOTSTRAP_DIR/codex-global-AGENTS.md" "__SHARED_BRAIN_LOCAL_PATH__" "$TARGET"
render_template "$(dirname "$0")/codex-shared-brain.include.md" "$BOOTSTRAP_DIR/codex-shared-brain.include.md" "__SHARED_BRAIN_LOCAL_PATH__" "$TARGET"
render_template "$(dirname "$0")/cursor-user-rule.md" "$BOOTSTRAP_DIR/cursor-user-rule.md" "__SHARED_BRAIN_LOCAL_PATH__" "$TARGET"
render_template "$(dirname "$0")/claude-hooks-example.json" "$BOOTSTRAP_DIR/claude-hooks-example.json" "__SHARED_BRAIN_LOCAL_PATH__" "$TARGET"
render_template "$(dirname "$0")/claude-user-CLAUDE.md" "$BOOTSTRAP_DIR/claude-user-CLAUDE.md" "__SHARED_BRAIN_LOCAL_PATH__" "$TARGET_ABS"
if [ -d "$TARGET_ABS/.git" ]; then
  git -C "$TARGET_ABS" pull --ff-only
else
  git clone "$REMOTE" "$TARGET_ABS"
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
  cat > "$CLAUDE_HOME/CLAUDE.md" <<STUB
@$BOOTSTRAP_ABS/claude-user-CLAUDE.md
STUB
else
  echo "Claude already has $CLAUDE_HOME/CLAUDE.md; append this exact import line manually:"
  echo "@$BOOTSTRAP_ABS/claude-user-CLAUDE.md"
fi

CURSOR_RULE_DIR="$HOME/.cursor/rules"
mkdir -p "$CURSOR_RULE_DIR"
if [ ! -f "$CURSOR_RULE_DIR/shared-brain.md" ]; then
  cp "$BOOTSTRAP_DIR/cursor-user-rule.md" "$CURSOR_RULE_DIR/shared-brain.md"
else
  echo "Cursor shared-brain rule already exists at $CURSOR_RULE_DIR/shared-brain.md; append or merge the actual rule content with:"
  echo "cat $BOOTSTRAP_DIR/cursor-user-rule.md >> $CURSOR_RULE_DIR/shared-brain.md"
fi

echo "shared brain client installed or updated at $TARGET_ABS"
echo "product-owned bootstrap snippets are in $BOOTSTRAP_DIR"
