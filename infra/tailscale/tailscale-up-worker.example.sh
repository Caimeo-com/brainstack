#!/usr/bin/env bash
set -euo pipefail
: "${TAILSCALE_AUTH_KEY:?set TAILSCALE_AUTH_KEY first}"
sudo tailscale up --auth-key="${TAILSCALE_AUTH_KEY}" --hostname=brain-worker --advertise-tags=tag:brain-worker --operator="${USER}"
