#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_DST="$HOME/.config/systemd/user/telemux.service"
BUN_BIN="${BUN_BIN:-$HOME/.bun/bin/bun}"
OWNER_USER="${USER}"
OWNER_GROUP="$(id -gn)"

if [[ "${EUID}" -eq 0 ]]; then
  echo "Run ./scripts/install.sh as your normal user, not with sudo."
  echo "The script will call sudo itself for /srv setup and linger."
  exit 1
fi

if [[ ! -x "$BUN_BIN" ]]; then
  echo "Missing Bun runtime at $BUN_BIN"
  exit 1
fi

if [[ ! -f "$ROOT/.env" ]]; then
  cp "$ROOT/.env.example" "$ROOT/.env"
  echo "Created $ROOT/.env from template"
fi

set -a
# shellcheck source=/dev/null
source "$ROOT/.env"
set +a

CONTROL_ROOT="${FACTORY_CONTROL_ROOT:-/srv/telemux}"
FACTORY_ROOT="${FACTORY_FACTORY_ROOT:-/srv/factory}"

sudo -v
sudo install -d -m 0775 -o "$OWNER_USER" -g "$OWNER_GROUP" \
  "$CONTROL_ROOT" \
  "$CONTROL_ROOT/contexts" \
  "$CONTROL_ROOT/logs" \
  "$FACTORY_ROOT" \
  "$FACTORY_ROOT/repos" \
  "$FACTORY_ROOT/hostctx" \
  "$FACTORY_ROOT/scratch"
sudo chown -R "$OWNER_USER:$OWNER_GROUP" "$CONTROL_ROOT" "$FACTORY_ROOT"

mkdir -p \
  "$HOME/.config/systemd/user"

"$BUN_BIN" install

cat > "$SERVICE_DST" <<EOF
[Unit]
Description=Clawdex control plane
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$ROOT
EnvironmentFile=$ROOT/.env
ExecStart=$BUN_BIN run src/main.ts
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
EOF

sudo loginctl enable-linger "$OWNER_USER"
systemctl --user daemon-reload
systemctl --user enable --now telemux.service

echo "Installed and started telemux.service for Clawdex"
