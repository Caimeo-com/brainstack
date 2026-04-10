# Quickstart: Single Node

Single-node is the reference profile for valkyrie-style installs: one machine runs `braind`, optional `telemux`, the shared-brain bare repo, the staging clone, and the serve clone.

## Paths

- Product repo: `~/brainstack`
- Shared brain root: `~/shared-brain`
- Bare repo: `~/shared-brain/bare/shared-brain.git`
- Writable staging clone: `~/shared-brain/staging/shared-brain`
- Serve clone: `~/shared-brain/serve/shared-brain`
- State: `~/.local/state/brainstack`
- Config/env: `~/.config/brainstack`
- User services: `~/.config/systemd/user`

## Dry Run

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts smoke --profile single-node --config examples/single-node.yaml
```

The smoke command creates a disposable install root under `/tmp`, initializes a bare/staging/serve shared-brain repo, renders service files, runs doctor, and rebuilds the local search index.

## Install

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts init --profile single-node --config examples/single-node.yaml
systemctl --user daemon-reload
systemctl --user enable --now braind.service
systemctl --user enable --now telemux.service
```

Do not run this on valkyrie until the migration plan is intentionally applied. The current valkyrie production services still use compatibility paths.

## Tailscale Serve

`braind` should bind only to loopback:

```env
BRAIN_BIND=127.0.0.1
BRAIN_PORT=8080
```

Expose it privately to the tailnet with:

```bash
tailscale serve --bg 8080
tailscale serve status
```

Do not enable Funnel for the shared brain unless you explicitly intend public internet exposure.

## Health

```bash
curl -fsS http://127.0.0.1:8080/health
curl -fsS https://valkyrie.tailb647b6.ts.net/health
systemctl --user status braind.service --no-pager
journalctl --user -u braind.service -n 100 --no-pager
```

