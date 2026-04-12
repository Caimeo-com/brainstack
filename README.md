# brainstack

`brainstack` is a Bun-first shared-brain and optional telemux/clawdex control-plane product.

It packages:

- `apps/braind`: shared-brain web/API/search/import service.
- `apps/telemux`: optional Telegram control plane for a selected Codex or Claude harness.
- `packages/brainctl`: installer, renderer, doctor, backup, restore, token, migration, and smoke-test CLI.
- `packages/client-bootstrap`: generated Codex, Claude, Cursor, SSH, and env bootstrap artifacts.
- `packages/skills`: portable skill seeds that are intentionally outside the brain content repo.

The canonical shared brain remains a separate git repo of markdown, manifests, raw artifacts, and proposals. Product code and data repos are deliberately separate.

## Quick Commands

```bash
cd ~/brainstack
bun install --frozen-lockfile
bun test
bun run packages/brainctl/src/main.ts provision --profile single-node --out ~/.config/brainstack/brainstack.yaml --harness codex
bun run packages/brainctl/src/main.ts render --profile single-node --config examples/single-node.yaml --out /tmp/brainstack-render
bun run packages/brainctl/src/main.ts smoke --profile single-node --config examples/single-node.yaml
bun run packages/brainctl/src/main.ts doctor --config examples/control.yaml --workers
bun run packages/brainctl/src/main.ts updates --config examples/control.yaml
bun run packages/brainctl/src/main.ts outbox status --config examples/client-macos.yaml
bun run packages/brainctl/src/main.ts upgrade --profile control --config examples/control.yaml
bun run packages/brainctl/src/main.ts destroy --config ~/.config/brainstack/brainstack.yaml --dry-run
```

`init` is fresh-install only. Use `upgrade` or `apply-runtime` for existing installs; those commands do not silently seed or rewrite canonical shared-brain content.

Build a current-platform standalone CLI:

```bash
cd ~/brainstack
bun build packages/brainctl/src/main.ts --compile --no-compile-autoload-dotenv --no-compile-autoload-bunfig --outfile dist/brainctl
```

The deterministic flags prevent the compiled binary from implicitly loading local `.env` or `bunfig.toml` files from the release machine.

Generated source-run services also invoke Bun with `--no-env-file` and load explicit `*.runtime.env` plus operator-owned `*.secrets.env` files. This keeps service behavior deterministic instead of depending on whichever `.env` happens to exist near the product repo.

## Profiles

- `single-node`: one machine runs `braind`, optional `telemux`, bare/staging/serve clones, and Tailscale Serve.
- `control`: control host with `braind`, optional `telemux`, and worker orchestration.
- `worker`: worker host reachable by normal OpenSSH over Tailscale; no Telegram polling and no admin ingest token.
- `client-macos`: local clone plus Codex/Claude/Cursor bootstrap; no local services.
- `private-journal`: optional separate private brain repo/service/token boundary.

Start with the quickstart docs in `docs/`.
See [`docs/diagrams.md`](./docs/diagrams.md) for the read/write/outbox, Telegram coalescing, and control/client/worker topology diagrams.

`provision` is a first-stage checker/config generator. It does not install Bun, Git, SSH, Tailscale, Codex, or Claude; it fails with install hints when they are missing.

`destroy` is intentionally manifest-driven and destructive only with `--yes`. Use `--scope control|worker|client|all` to limit removal to brainstack-owned artifacts for that role. It never removes package installs, Tailscale enrollment, Codex/Claude auth, or sudo policy.

Client import/propose writes can queue into `~/.local/state/brainstack/outbox/<brain-id>/` when the brain is unreachable. Use `brainctl outbox status|list|flush|purge`; flush replays only import/propose payloads and never mutates canonical wiki pages offline.

Read [`docs/operator-preflight.md`](./docs/operator-preflight.md) before enabling `telemux`. The control-plane profile assumes a trusted private machine; telemux passes authorized Telegram work into the configured harness process and is not a sandbox.
