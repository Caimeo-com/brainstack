#!/usr/bin/env bash
set -euo pipefail
tailscale serve --bg 8080
tailscale serve status

