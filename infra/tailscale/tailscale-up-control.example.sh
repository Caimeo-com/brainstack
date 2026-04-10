#!/usr/bin/env bash
set -euo pipefail
: "${TAILSCALE_AUTH_KEY:?set TAILSCALE_AUTH_KEY first}"
sudo tailscale up --auth-key="${TAILSCALE_AUTH_KEY}" --hostname=valkyrie --advertise-tags=tag:brain --operator="${USER}"

