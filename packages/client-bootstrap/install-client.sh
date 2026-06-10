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
  printf '%s' "$1" | sed 's/[#\/&]/\\&/g'
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

read_import_token() {
  if [ -n "${BRAIN_IMPORT_TOKEN_FILE:-}" ]; then
    if [ ! -f "$BRAIN_IMPORT_TOKEN_FILE" ]; then
      echo "BRAIN_IMPORT_TOKEN_FILE does not exist: $BRAIN_IMPORT_TOKEN_FILE" >&2
      exit 2
    fi
    if [ -L "$BRAIN_IMPORT_TOKEN_FILE" ]; then
      echo "BRAIN_IMPORT_TOKEN_FILE must not be a symlink: $BRAIN_IMPORT_TOKEN_FILE" >&2
      exit 2
    fi
    local mode
    mode="$(stat -f '%Lp' "$BRAIN_IMPORT_TOKEN_FILE" 2>/dev/null || stat -c '%a' "$BRAIN_IMPORT_TOKEN_FILE" 2>/dev/null || true)"
    if [ -n "$mode" ] && (( (8#$mode) & 077 )); then
      echo "BRAIN_IMPORT_TOKEN_FILE must not be group/world accessible; run: chmod 600 '$BRAIN_IMPORT_TOKEN_FILE'" >&2
      exit 2
    fi
    sed -n '1p' "$BRAIN_IMPORT_TOKEN_FILE" | tr -d '\r\n'
    return
  fi
  printf '%s' "${BRAIN_IMPORT_TOKEN:-}"
}

set_env_if_blank() {
  local key="$1"
  local value="$2"
  local tmp
  if [ -z "$value" ]; then
    return
  fi
  if grep -Eq "^${key}=.+" "$ENV_FILE" 2>/dev/null; then
    echo "$key already present in $ENV_FILE; leaving existing value in place."
    return
  fi
  tmp="$(mktemp "$ENV_FILE.XXXXXX")"
  if grep -Eq "^${key}=" "$ENV_FILE" 2>/dev/null; then
    awk -v key="$key" -v value="$value" 'BEGIN{prefix=key"="} index($0,prefix)==1{$0=prefix value} {print}' "$ENV_FILE" > "$tmp"
  else
    cat "$ENV_FILE" > "$tmp"
    printf '%s=%s\n' "$key" "$value" >> "$tmp"
  fi
  mv -f "$tmp" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  echo "$key installed in $ENV_FILE."
}

yaml_quote() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/''/g")"
}

write_client_config_if_missing() {
  local config_file="$CONFIG_DIR/brainstack/brainstack.yaml"
  local machine_name
  local host_name
  local user_name
  local product_repo
  local brain_base_url

  if [ -f "$config_file" ]; then
    echo "Brainstack config already exists at $config_file; leaving it in place."
    return
  fi

  mkdir -p "$(dirname "$config_file")"
  machine_name="${BRAINSTACK_MACHINE_NAME:-$(hostname -s 2>/dev/null || hostname 2>/dev/null || printf client)}"
  host_name="${BRAINSTACK_HOSTNAME:-$(hostname 2>/dev/null || printf client)}"
  user_name="${USER:-operator}"
  product_repo="${BRAINSTACK_PRODUCT_REPO:-~/brainstack}"
  brain_base_url="${BRAIN_BASE_URL:-__BRAIN_BASE_URL__}"

  cat > "$config_file" <<YAML
schema_version: 1
profile: client-macos
machine:
  name: $(yaml_quote "$machine_name")
  user: $(yaml_quote "$user_name")
  role: client
  sshUser: $(yaml_quote "$user_name")
  hostname: $(yaml_quote "$host_name")
paths:
  home: $(yaml_quote "$HOME")
  productRepo: $(yaml_quote "$product_repo")
  sharedBrainRoot: $(yaml_quote "$TARGET_ABS")
  stateRoot: ~/.local/state/brainstack
  configRoot: ~/.config/brainstack
brain:
  publicBaseUrl: $(yaml_quote "$brain_base_url")
client:
  localPath: $(yaml_quote "$TARGET_ABS")
  envPath: $(yaml_quote "$ENV_FILE")
  remoteSsh: $(yaml_quote "$REMOTE")
YAML
  chmod 600 "$config_file"
  echo "Brainstack client config installed at $config_file."
}

validate_remote() {
  # A leading hyphen would be parsed as a git option even when quoted, and unexpected
  # protocols can invoke arbitrary transports.
  case "$REMOTE" in
    -*)
      echo "BRAIN_GIT_REMOTE must not start with '-': $REMOTE" >&2
      exit 2
      ;;
    *[$'\n\r\t']*)
      echo "BRAIN_GIT_REMOTE must not contain control characters" >&2
      exit 2
      ;;
    ssh://*|https://*|git@*:*|/*|~/*|file:///*)
      ;;
    *)
      echo "BRAIN_GIT_REMOTE must be an ssh://, https://, git@host:path, or absolute local path remote: $REMOTE" >&2
      exit 2
      ;;
  esac
}

validate_remote

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
  git clone -- "$REMOTE" "$TARGET_ABS"
fi

if [ ! -f "$ENV_FILE" ]; then
  cp "$(dirname "$0")/client.env.example" "$ENV_FILE"
  chmod 600 "$ENV_FILE"
fi
IMPORT_TOKEN_VALUE="$(read_import_token)"
set_env_if_blank "BRAIN_IMPORT_TOKEN" "$IMPORT_TOKEN_VALUE"
write_client_config_if_missing

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
