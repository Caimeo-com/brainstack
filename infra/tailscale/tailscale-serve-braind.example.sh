#!/usr/bin/env bash
set -euo pipefail
CONFIG_FILE="${1:-$(dirname "$0")/serve-config.example.json}"
tailscale serve set-config --all "$CONFIG_FILE"
tailscale serve status
