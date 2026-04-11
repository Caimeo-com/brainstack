# Brainstack Agent Contract

This repo contains the versioned product code for `brainstack`, not the canonical shared-brain content.

- Product code lives in `apps/`, `packages/`, `infra/`, `examples/`, and `docs/`.
- Shared-brain data remains a separate git repo of markdown, manifests, raw artifacts, and proposals.
- Portable skills live in `packages/skills`, outside any shared-brain content repo.
- Use Bun for runtime, package management, tests, and builds. Do not use node, npm, npx, pnpm, or yarn.
- Default paths must stay home-directory based unless a user explicitly opts into system paths.
- Do not commit secrets, local tokens, Telegram bot tokens, SSH private keys, or real env files.
- New write flows must preserve the default model: clients import/propose, control hosts ingest/lint, direct git push is trusted power-user only.
- Do not make Codex hooks part of correctness. Hooks may sync or summarize as convenience only.
- Keep docs and examples aligned with generated behavior.

## Handoff Bundles

- When the operator asks for a handoff bundle, use `scripts/handoff.sh` unless explicitly told otherwise.
- Default to `scripts/handoff.sh --mode review`; use `--mode forensic` only when a larger audit trail is requested.
- For review handoffs, pass `--base <last-reviewed-commit>` when known and `--notes <file>` when pass-specific context exists.
- Handoffs must include `CHANGES.txt` and `CLAIMS_AND_PROOF.md` so a fresh-context auditor can see the delta and the evidence map without reverse-engineering the tree.
- Handoff bundles are review/audit artifacts, not releases. Do not include compiled binaries, `dist/`, `.git`, dependency trees, env files, private keys, tokens, caches, or Finder/macOS junk.
- Use exactly one source representation in a handoff bundle. The checked-in script uses `source/` from `git archive HEAD`.
- If Telegram delivery is requested on valkyrie, send the resulting zip through the existing telemux/Telegram path without printing secrets.
