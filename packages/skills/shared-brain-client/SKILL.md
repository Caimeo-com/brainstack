---
name: shared-brain-client
description: Client-side skill for reading a local shared-brain clone and writing only imports/proposals by default.
---

# Shared Brain Client

Use this skill from client machines and harnesses.

- Default local clone path: `~/shared-brain`.
- Sync with `git pull --ff-only` before relying on local content.
- Prefer reading local markdown over calling remote endpoints.
- Use the HTTP API or client CLI for imports and proposals.
- Verify write readiness with `brainctl doctor --write-smoke` only when an explicit mutating setup proof is wanted.
- Do not directly edit canonical wiki pages unless the operator explicitly asks for trusted power-user mode.
- Never request or store the admin ingest token on client or worker profiles.
- Prefer invite-based enrollment when joining a stack: the operator creates a private invite, the client installer runs `brainctl enroll`, local config lands at `~/.config/brainstack/brainstack.yaml`, and Codex client skills are installed automatically for Codex invites.
- Use `brainctl skills install --target codex --profile client` only to repair or intentionally refresh the public client skill bundle after enrollment.
- Treat token-bearing invites as bearer secrets. Do not paste them into shared logs, shell history, screenshots, or public issues.
- If `brainctl telegram send-file` is configured, use it to send files from the client to the operator through the control host's telemux bot. The client streams over SSH; Telegram bot tokens stay on the control host.
- Do not bypass Brainstack's send-file path with ad hoc Telegram bot calls from the client. That would move provider credentials to the wrong machine and skip Brainstack's size, symlink, SSH trust, and sensitive-filename checks.
- If file relay fails, check `brainctl doctor --config ~/.config/brainstack/brainstack.yaml`, the configured `client.remoteSsh`, and pinned known-hosts state before changing Telegram settings.
