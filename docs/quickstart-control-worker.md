# Quickstart: Control + Worker

The control profile runs `braind` and optional `telemux`. The worker profile does not run Telegram polling and does not receive the admin ingest token.

Read [`operator-preflight.md`](./operator-preflight.md) before installing the control profile. Telemux passes Telegram-originated work into the configured harness process; it is not a sandbox.

Read [`tailscale-control-worker.md`](./tailscale-control-worker.md) before pairing a control host with workers. The worker transport is normal OpenSSH over Tailscale, not Tailscale SSH.

For first real hardware validation, follow [`runbooks/worker-canary.md`](./runbooks/worker-canary.md) before destructive legacy cleanup or demo work.

## Control Dry Run

```bash
cd ~/brainstack
bun install --frozen-lockfile
bun run packages/brainctl/src/main.ts provision --profile control --out ~/.config/brainstack/brainstack.yaml --harness codex
bun run packages/brainctl/src/main.ts smoke --profile control --config examples/control.yaml
```

For a fresh control host install, run `brainctl init`. For later product updates, use `brainctl upgrade`; it backs up first and applies runtime files without rewriting canonical shared-brain content.

`provision` only checks prerequisites and writes config. It does not install Bun, Git, OpenSSH, Tailscale, Codex, or Claude. If both Codex and Claude are installed in a non-interactive run, pass `--harness codex` or `--harness claude`.

Keep the active control config at `~/.config/brainstack/brainstack.yaml`. Machine-specific compatibility configs such as `brain-control-current.brainstack.yaml` are useful during migration, but the default path should exist before operators copy/paste doctor, upgrade, or worker commands. If the path is missing, rerun `provision --out ~/.config/brainstack/brainstack.yaml` or pass the existing config explicitly.

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts init --profile control --config ~/.config/brainstack/brainstack.yaml
loginctl enable-linger "$USER"
systemctl --user daemon-reload
bun run packages/brainctl/src/main.ts upgrade --profile control --config ~/.config/brainstack/brainstack.yaml
systemctl --user daemon-reload
systemctl --user restart braind.service
```

## Worker Plan

Generate a worker join plan without touching the worker:

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts join-worker --config ~/.config/brainstack/brainstack.yaml --worker brain-worker
```

The join plan is a YAML patch for `brainstack.yaml`; `workers.json` remains rendered output. Workers may override the global harness:

```bash
bun run packages/brainctl/src/main.ts join-worker --config ~/.config/brainstack/brainstack.yaml --worker brain-worker --harness claude
```

Harness precedence is explicit context override, then worker default, then global default. Remote workers resolve `codex` or `claude` via their own `PATH` unless a worker-specific `harnessBin` is configured; the control host's local absolute harness path is not reused on workers.

For remote workers, Brainstack first asks the worker user's own shell for its interactive-login `PATH` and then resolves `codex` or `claude` there. That means user-owned wrappers are acceptable if `codex --version` or `claude --version` works for the SSH user. Brainstack itself still uses Bun; it does not install or invoke Node/npm/npx as part of its own product code. If a worker uses a nonstandard binary location that is not in the user's shell PATH, set that worker's `harnessBin` explicitly in `brainstack.yaml`.

Worker SSH host trust is pinned by default. After merging a worker into `brainstack.yaml` and before restarting telemux, record the host key from the control host:

```bash
bun run packages/brainctl/src/main.ts trust-worker --config ~/.config/brainstack/brainstack.yaml --worker brain-worker
bun run packages/brainctl/src/main.ts doctor --config ~/.config/brainstack/brainstack.yaml --workers
```

The generated worker entry uses `sshTrustMode: pinned`. Temporary `sshTrustMode: accept-new` is allowed only for an explicit bootstrap window. Doctor fails it unless `BRAINSTACK_ALLOW_ACCEPT_NEW_DOCTOR=true`, and telemux dispatch fails it unless `BRAINSTACK_ALLOW_ACCEPT_NEW_DISPATCH=true`; do not leave those overrides set after enrollment.

The worker transport default is normal OpenSSH over Tailscale:

```bash
loginctl enable-linger "$USER"
ssh operator@brain-worker true
```

Also verify the reverse Git freshness path from the worker to the control host before running worker init:

```bash
git ls-remote operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git HEAD
```

If this fails with `Permission denied (publickey)`, the network path is already working and the missing piece is SSH key authorization on the control host. Prefer a restricted worker-to-control Git read key for this path; see [`tailscale-control-worker.md`](./tailscale-control-worker.md#openssh-key-shape). The worker does not need full shell access to the control host just to clone/pull the shared-brain repo.

## Worker Bootstrap

On a worker host, `brainctl init --profile worker` performs a real client bootstrap:

```bash
cd ~/brainstack
chmod 600 ~/brain-import-token.txt
bun run packages/brainctl/src/main.ts init \
  --profile worker \
  --config ~/.config/brainstack/brainstack.yaml \
  --import-token-file ~/brain-import-token.txt
```

That clones the shared-brain repo to the configured client path, writes `~/.config/shared-brain.env` if missing, fills `BRAIN_IMPORT_TOKEN` only when provided and currently blank, and installs Codex/Claude/Cursor shared-brain guidance. It does not run local `braind`, does not run Telegram polling, and does not write an admin ingest token.

Before a first real worker canary, run:

```bash
bun run packages/brainctl/src/main.ts doctor --config ~/.config/brainstack/brainstack.yaml --workers
bun run packages/brainctl/src/main.ts doctor --config ~/.config/brainstack/brainstack.yaml --write-smoke
bun run packages/brainctl/src/main.ts updates --config ~/.config/brainstack/brainstack.yaml
```

Use `--write-smoke` when you intentionally want to post a small import artifact to prove client pushback. Use `--deep` only when you want doctor to invoke the configured harness on the control/worker to prove bypass/yolo sudo behavior. It can consume LLM quota and should not be part of every health check.

`doctor --workers` resolves each worker user's shell PATH before probing, then reports the path and version for required worker tools: Bun, Git, OpenSSH, Tailscale, and the configured Codex/Claude harness. Telemux `/workers` also probes `sudo -n true` and marks the worker degraded when passwordless sudo is missing, because unattended machine-administration prompts will otherwise stall. If an interactive shell can see a tool but doctor cannot, treat that as a Brainstack bug rather than a normal setup warning.

If that fails with a timeout while ping works, the likely blocker is Tailscale grants. The required grant shape is:

```json
{
  "src": ["group:brain-admins", "autogroup:admin"],
  "dst": ["tag:brain-worker"],
  "ip": ["tcp:22", "icmp:*"]
}
```

That lets the operator reach workers directly for setup and debugging. The control host also needs worker SSH:

```json
{
  "src": ["tag:brain"],
  "dst": ["tag:brain-worker"],
  "ip": ["tcp:22", "icmp:*"]
}
```

Workers need the reverse path back to the control host for shared-brain clone freshness and HTTPS/API access:

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
- During a canary, temporary host/IP aliases are acceptable while you verify tags. Remove them after `tailscale whois <tailscale-ip>` shows the expected tag for each host.

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

`telemux.runtime.env` also includes `FACTORY_HARNESS`, `FACTORY_HARNESS_BIN`, `FACTORY_TEXT_COALESCE_MS`, `FACTORY_TEXT_COALESCE_RECOVERY_MAX_AGE_MS`, and the disabled-by-default `FACTORY_PRE_DISPATCH_CLASSIFIER` knobs. Use `harness.name: claude` and `harness.bin: claude` in `brainstack.yaml` to route jobs through Claude Code instead of Codex by default. Text coalescing merges only short-window plain-text Telegram messages from the same user/chat/topic; commands and attachments flush pending text first. After a restart, stale pending coalesced text is dropped instead of being auto-run long after the user sent it.

For plain text in bound Telegram topics, telemux applies deterministic pre-dispatch routing before the harness starts. Bare liveness/status/usage/latency questions are answered locally, short informational questions use a lightweight read-only prompt wrapper, and any mutating, file-like, machine, scheduling, attachment, long, or ambiguous request uses the full durable-work wrapper. Setting `FACTORY_PRE_DISPATCH_CLASSIFIER=1` enables the optional cheap LLM classifier for ambiguous short messages; put its dedicated key in `FACTORY_PRE_DISPATCH_CLASSIFIER_API_KEY`. When enabled, ambiguous message text and minimal context metadata are sent to OpenAI's Responses API. Classifier errors, low confidence, attachments, and risky work indicators fall back to the full work path. Use `/run` or `/resume` to force the full durable-work wrapper.

Run `/explainctx` in an unbound topic to see what binding means before creating a context. Run bare `/newctx` for a guided deterministic binding flow: telemux suggests a slug from the Telegram topic title when Telegram includes it, falls back to `topic-<thread-id>` when it does not, then asks for machine and target choices. The full positional form remains `/newctx <slug> <machine> <target> [base-branch]`; common targets are `scratch` for topic work, `host` for a machine-host workspace, or a git URL/path for repo work.

Compatibility installs may set `telemux.controlRoot` and `telemux.factoryRoot` in `brainstack.yaml` to preserve an existing telemux SQLite DB and factory workspaces such as `/srv/telemux` and `/srv/factory`. Fresh installs should normally use the defaults under `~/.local/state/brainstack`.

If the shared brain is unreachable, telemux queues opted-in run-summary imports under the same outbox root used by `brainctl outbox`.

If `FACTORY_TELEGRAM_CONTROL_CHAT_ID` is set, telemux creates or reuses a `brainstack-routines` scratch context on startup and installs the deterministic `update-check` routine automatically. The routine reports every configured worker known to telemux; `workers.json` is reloaded without a service restart after `brainctl upgrade` rewrites it.

After telemux is healthy, use [`routines.md`](./routines.md) to add optional workflows such as `brain-curator` and `daily-checkin`.
