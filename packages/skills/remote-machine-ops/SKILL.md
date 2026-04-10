---
name: remote-machine-ops
description: Safe operational pattern for managing brainstack control and worker machines over normal OpenSSH through Tailscale.
---

# Remote Machine Ops

Use this skill for control-to-worker setup and diagnostics.

- Default transport is normal OpenSSH over Tailscale, not Tailscale SSH.
- Confirm ACL grants before blaming SSH keys: control tag to worker tag needs `tcp:22` and `icmp`.
- Run `ssh <user>@<host> true` as the first smoke test.
- Keep worker profiles free of Telegram polling and admin ingest tokens.
- Prefer home-directory paths: `~/brainstack`, `~/shared-brain`, `~/.local/state/brainstack`, `~/.config/brainstack`.
- Report exact Tailscale grant fragments instead of widening policy ad hoc.

