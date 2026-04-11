# brainstack

`brainstack` is a Bun-first productization of the current valkyrie shared-brain and clawdex/telemux stack.

It packages:

- `apps/braind`: shared-brain web/API/search/import service.
- `apps/telemux`: optional Telegram/Codex control plane vendored from `~/private-dev-factory`.
- `packages/brainctl`: installer, renderer, doctor, backup, restore, token, migration, and smoke-test CLI.
- `packages/client-bootstrap`: generated Codex, Claude, Cursor, SSH, and env bootstrap artifacts.
- `packages/skills`: portable skill seeds that are intentionally outside the brain content repo.

The canonical shared brain remains a separate git repo of markdown, manifests, raw artifacts, and proposals. Product code and data repos are deliberately separate.

## Quick Commands

```bash
cd ~/brainstack
bun test
bun run packages/brainctl/src/main.ts render --profile single-node --config examples/single-node.yaml --out /tmp/brainstack-render
bun run packages/brainctl/src/main.ts smoke --profile single-node --config examples/single-node.yaml
bun run packages/brainctl/src/main.ts doctor --config examples/control.yaml
```

Build a current-platform standalone CLI:

```bash
cd ~/brainstack
bun build packages/brainctl/src/main.ts --compile --outfile dist/brainctl
```

## Profiles

- `single-node`: one machine runs `braind`, optional `telemux`, bare/staging/serve clones, and Tailscale Serve.
- `control`: control host with `braind`, optional `telemux`, and worker orchestration.
- `worker`: worker host reachable by normal OpenSSH over Tailscale; no Telegram polling and no admin ingest token.
- `client-macos`: local clone plus Codex/Claude/Cursor bootstrap; no local services.
- `private-journal`: optional separate private brain repo/service/token boundary.

Start with the quickstart docs in `docs/`.

Read [`docs/operator-preflight.md`](./docs/operator-preflight.md) before enabling `telemux`. The control-plane profile assumes a trusted private machine; telemux passes authorized Telegram work into the configured harness process and is not a sandbox.
