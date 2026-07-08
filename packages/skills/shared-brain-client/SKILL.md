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
- Use `brainctl lifecycle status --config ~/.config/brainstack/brainstack.yaml` for bounded local health and `brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml` for routine post-install repair of runtime files, local harness guidance, daemon service files, hooks, and shared skill packages.
- Use `brainctl skills install --target codex --profile client` only to repair or intentionally refresh the public client skill bundle after enrollment.
- Use `brainctl skills import --config ~/.config/brainstack/brainstack.yaml` to plan local skill sharing across the current directory and default Codex/Claude/Cursor skill roots. Use `brainctl skills import <folder>` to scan one folder, then select numbered entries interactively or pass `--select`/`--apply` after confirming the listed skills should become global shared-brain imports.
- Use `brainctl skills refresh --config ~/.config/brainstack/brainstack.yaml --target codex` to install validated skill packages that were imported into the shared brain. Hook-based refresh is convenience-only and must fail open when the brain or clone is unavailable.
- Use `brainctl daemon install --config ~/.config/brainstack/brainstack.yaml --start` when this machine should keep its shared-brain clone, outbox, and shared skills fresh in the background. `brainstackd` is `brainctl daemon run`, not a second binary, and failures must degrade into local status instead of blocking the harness.
- If daemon status reports a missing, dirty, or non-Git clone, fix the clone state before trusting shared skill refreshes. Brainstack should not install shared skills from an unsafe local source.
- Treat token-bearing invites as bearer secrets. Do not paste them into shared logs, shell history, screenshots, or public issues.
- Use `brainctl uploads put --config ~/.config/brainstack/brainstack.yaml --machine <machine> --file <path>` to stage a local regular file onto a Brainstack machine for later harness use. Uploads are private machine state, not shared-brain content.
- Use `brainctl uploads list --machine <machine> --recent` before telling a remote harness to use "the file I just uploaded"; Telemux bound topics can add recent upload paths to the harness prompt automatically when the message references them.
- If `brainctl telegram send-file` is configured, use it to send files from the client to the operator through the control host's telemux bot. The client streams over SSH; Telegram bot tokens stay on the control host.
- Do not bypass Brainstack's send-file path with ad hoc Telegram bot calls from the client. That would move provider credentials to the wrong machine and skip Brainstack's size, symlink, SSH trust, and sensitive-filename checks.
- If file relay fails, check `brainctl doctor --config ~/.config/brainstack/brainstack.yaml`, the configured `client.remoteSsh`, and pinned known-hosts state before changing Telegram settings.
