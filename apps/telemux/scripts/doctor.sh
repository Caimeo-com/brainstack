#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUN_BIN="${BUN_BIN:-$HOME/.bun/bin/bun}"

if [[ "${EUID}" -eq 0 ]]; then
  echo "Run ./scripts/doctor.sh as your normal user, not with sudo."
  exit 1
fi

if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "$ROOT/.env"
  set +a
fi

echo "== Bun =="
"$BUN_BIN" --version

echo
echo "== Build =="
(
  cd "$ROOT"
  "$BUN_BIN" build src/main.ts --target bun --outdir dist
)

echo
echo "== Systemd =="
systemctl --user is-enabled telemux.service
systemctl --user is-active telemux.service

echo
echo "== Dashboard =="
curl -fsS "http://${FACTORY_DASHBOARD_HOST:-127.0.0.1}:${FACTORY_DASHBOARD_PORT:-8787}/healthz"

echo
echo "== Tests =="
(
  cd "$ROOT"
  "$BUN_BIN" test
)

echo
echo "== SQLite =="
sqlite3 "${FACTORY_CONTROL_ROOT:-/srv/telemux}/db.sqlite" ".schema contexts"

echo
echo "doctor completed"
