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

