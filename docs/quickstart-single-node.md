# Quickstart: Single Node

Single-node is the reference profile for a one-machine install: one machine runs `braind`, the shared-brain bare repo, the staging clone, and the serve clone. Telemux is optional and disabled in the public example by default.

Read [`operator-preflight.md`](./operator-preflight.md) first. A control/single-node install assumes the chosen harness can run unattended as the current user. If you enable Codex or Claude yolo mode and passwordless sudo, telemux becomes a remote command path into that authority.

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
bun install --frozen-lockfile
bun run packages/brainctl/src/main.ts smoke --profile single-node --config examples/single-node.yaml
```

The smoke command creates a disposable install root under `/tmp`, initializes a bare/staging/serve shared-brain repo, renders service files, runs doctor, and rebuilds the local search index.

## Install

```bash
cd ~/brainstack
bun install --frozen-lockfile
bun run packages/brainctl/src/main.ts init --profile single-node --config examples/single-node.yaml
systemctl --user daemon-reload
systemctl --user enable --now braind.service
loginctl enable-linger "$USER"
```

Do not rerun `init` on an existing install. For product updates or service/hook/env re-rendering, use:

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts upgrade --profile single-node --config examples/single-node.yaml
systemctl --user daemon-reload
systemctl --user restart braind.service
```

To enable Telegram control explicitly, use `examples/control-telegram.yaml` and read `operator-preflight.md` first.

## Runtime And Secrets Env

`brainctl` splits generated runtime config from operator-owned secrets:

- `~/.config/brainstack/braind.runtime.env`: generated and overwritten on upgrade.
- `~/.config/brainstack/braind.secrets.env`: created if missing and never overwritten.

The generated user service loads both files and runs Bun with `--no-env-file` so local repo `.env` files cannot silently change service behavior.

## Tailscale Serve

`braind` should bind only to loopback:

```env
BRAIN_BIND=127.0.0.1
BRAIN_PORT=8080
```

Expose it privately to the tailnet with:

```bash
tailscale serve get-config --all > ~/.config/brainstack/tailscale-serve.before.json
tailscale serve set-config --all ~/.local/state/brainstack/rendered/tailscale/serve-config.json
tailscale serve status
```

Do not enable Funnel for the shared brain unless you explicitly intend public internet exposure.

## Health

```bash
curl -fsS http://127.0.0.1:8080/health
curl -fsS https://brain-control.example.ts.net/health
systemctl --user status braind.service --no-pager
journalctl --user -u braind.service -n 100 --no-pager
```
