---
name: remote-machine-ops
description: Safe operational pattern for managing brainstack control and worker machines over normal OpenSSH through Tailscale.
---

# Remote Machine Ops

Use this skill for control-to-worker setup and diagnostics.

- Default transport is normal OpenSSH over Tailscale, not Tailscale SSH.
- Confirm ACL grants before blaming SSH keys: control tag to worker tag needs `tcp:22` and `icmp`.
- Use the configured control host as the normal command hop when a stack is designed that way. Direct operator-machine access to every worker is not required unless the local topology says it is.
- Run `ssh <user>@<host> true` as the first smoke test for paths that are expected to be direct.
- For worker health, prefer `brainctl doctor --workers --config ~/.config/brainstack/brainstack.yaml` from the control host because it exercises the same SSH, shell, PATH, harness, trust, and runtime assumptions telemux uses.
- Plain non-interactive SSH PATH checks are useful when they are part of the contract, but they do not replace the worker doctor path.
- Keep worker profiles free of Telegram polling and admin ingest tokens.
- Prefer home-directory paths: `~/brainstack`, `~/shared-brain`, `~/.local/state/brainstack`, `~/.config/brainstack`.
- Report exact Tailscale grant fragments instead of widening policy ad hoc.
- Keep `sshTrustMode: pinned` as the steady state. `accept-new` is a bootstrap escape hatch and should be replaced with an explicit known-hosts file before routine operations.
