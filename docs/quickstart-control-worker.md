# Quickstart: Control + Worker

The control profile runs `braind` and optional `telemux`. The worker profile does not run Telegram polling and does not receive the admin ingest token.

Read [`operator-preflight.md`](./operator-preflight.md) before installing the control profile. Telemux passes Telegram-originated work into the configured harness process; it is not a sandbox.

Read [`tailscale-control-worker.md`](./tailscale-control-worker.md) before pairing a control host with workers. The worker transport is normal OpenSSH over Tailscale, not Tailscale SSH.

## Control Dry Run

```bash
cd ~/brainstack
bun install --frozen-lockfile
bun run packages/brainctl/src/main.ts provision --profile control --out ~/.config/brainstack/brainstack.yaml --harness codex
bun run packages/brainctl/src/main.ts smoke --profile control --config examples/control.yaml
```

For a fresh control host install, run `brainctl init`. For later product updates, use `brainctl upgrade`; it backs up first and applies runtime files without rewriting canonical shared-brain content.

`provision` only checks prerequisites and writes config. It does not install Bun, Git, OpenSSH, Tailscale, Codex, or Claude. If both Codex and Claude are installed in a non-interactive run, pass `--harness codex` or `--harness claude`.

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts init --profile control --config examples/control.yaml
loginctl enable-linger "$USER"
systemctl --user daemon-reload
bun run packages/brainctl/src/main.ts upgrade --profile control --config examples/control.yaml
systemctl --user daemon-reload
systemctl --user restart braind.service
```

## Worker Plan

Generate a worker join plan without touching the worker:

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts join-worker --config examples/control.yaml --worker brain-worker
```

The join plan is a YAML patch for `brainstack.yaml`; `workers.json` remains rendered output. Workers may override the global harness:

```bash
bun run packages/brainctl/src/main.ts join-worker --config examples/control.yaml --worker brain-worker --harness claude
```

Harness precedence is explicit context override, then worker default, then global default. Remote workers resolve `codex` or `claude` via their own `PATH` unless a worker-specific `harnessBin` is configured; the control host's local absolute harness path is not reused on workers.

The worker transport default is normal OpenSSH over Tailscale:

```bash
loginctl enable-linger "$USER"
ssh operator@brain-worker true
```

## Worker Bootstrap

On a worker host, `brainctl init --profile worker` performs a real client bootstrap:

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts init --profile worker --config examples/worker.yaml
```

That clones the shared-brain repo to the configured client path, writes `~/.config/shared-brain.env` if missing, and installs Codex/Claude/Cursor shared-brain guidance. It does not run local `braind`, does not run Telegram polling, and does not write an admin ingest token.

Before a yoda/worker canary, run:

```bash
bun run packages/brainctl/src/main.ts doctor --config ~/.config/brainstack/brainstack.yaml --workers
bun run packages/brainctl/src/main.ts updates --config ~/.config/brainstack/brainstack.yaml
```

Use `--deep` only when you want doctor to invoke the configured harness on the control/worker to prove bypass/yolo sudo behavior. It can consume LLM quota and should not be part of every health check.

If that fails with a timeout while ping works, the likely blocker is Tailscale grants. The required grant shape is:

```json
{
  "src": ["tag:brain"],
  "dst": ["tag:brain-worker"],
  "ip": ["tcp:22", "icmp:*"]
}
```

Workers also need the reverse path back to the control host for shared-brain clone freshness and HTTPS/API access:

```json
{
  "src": ["tag:brain-worker"],
  "dst": ["tag:brain"],
  "ip": ["tcp:22", "tcp:443", "icmp:*"]
}
```

## Tailscale Tags

- Control hosts advertise `tag:brain`.
- Worker hosts advertise `tag:brain-worker`.
- Human laptops should normally stay untagged and access control through user/group grants.
- Tailscale SSH is not the default; leave `"ssh": []` in policy unless intentionally enabling it later.
- Validate server-applied tags with `tailscale status` plus `tailscale whois <tailscale-ip>`, not only `tailscale debug prefs`.
- If SSH says `tailscale: tailnet policy does not permit you to SSH to this node`, Tailscale SSH is still intercepting port 22 on the target. Disable Tailscale SSH on the target before using normal OpenSSH.

## Headless Control Enrollment

```bash
export TAILSCALE_AUTH_KEY=tskey-auth-...
sudo tailscale up --auth-key="${TAILSCALE_AUTH_KEY}" --hostname=brain-control --advertise-tags=tag:brain --operator=operator
```

## Headless Worker Enrollment

```bash
export TAILSCALE_AUTH_KEY=tskey-auth-...
sudo tailscale up --auth-key="${TAILSCALE_AUTH_KEY}" --hostname=brain-worker --advertise-tags=tag:brain-worker --operator=operator
```

Use reusable/preapproved auth keys with restricted tags from the Tailscale dashboard. Do not store auth keys in git.

## Runtime And Secrets Env

Generated runtime env files are overwritten on upgrade:

- `~/.config/brainstack/braind.runtime.env`
- `~/.config/brainstack/telemux.runtime.env` when telemux is explicitly enabled

Operator-managed secrets env files are created only if missing and are never overwritten:

- `~/.config/brainstack/braind.secrets.env`
- `~/.config/brainstack/telemux.secrets.env` when telemux is explicitly enabled

Generated user services load both runtime and secrets env files and invoke Bun with `--no-env-file` so local repo `.env` files cannot silently alter service behavior.

When telemux is enabled, `telemux.runtime.env` includes `BRAIN_BASE_URL` and `telemux.secrets.env` includes a blank `BRAIN_IMPORT_TOKEN`. Filling both opts successful runs into shared-brain raw imports of `SUMMARY.md` and `ARTIFACTS.md`; leaving either blank disables the bridge.

`telemux.runtime.env` also includes `FACTORY_HARNESS`, `FACTORY_HARNESS_BIN`, and `FACTORY_TEXT_COALESCE_MS`. Use `harness.name: claude` and `harness.bin: claude` in `brainstack.yaml` to route jobs through Claude Code instead of Codex by default. Text coalescing merges only short-window plain-text Telegram messages from the same user/chat/topic; commands and attachments flush pending text first.

If the shared brain is unreachable, telemux queues opted-in run-summary imports under the same outbox root used by `brainctl outbox`.
