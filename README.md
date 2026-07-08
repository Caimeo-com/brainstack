# brainstack

`brainstack` is a Bun-first shared-brain and optional telemux control-plane product.

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
bun run packages/brainctl/src/main.ts fleet status --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts fleet update yoda --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts expose tailscale --config examples/control.yaml --dry-run
bun run packages/brainctl/src/main.ts context --repo .
bun run packages/brainctl/src/main.ts search --repo . "runbook"
bun run packages/brainctl/src/main.ts outbox status --config examples/client-macos.yaml
bun run packages/brainctl/src/main.ts outbox retry import-... --config examples/client-macos.yaml
bun run packages/brainctl/src/main.ts skills install --target codex --profile client --dry-run
bun run packages/brainctl/src/main.ts skills doctor --dir ~/.codex/skills
bun run packages/brainctl/src/main.ts skills import --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts skills import ~/.codex/skills/brainstack/SKILL.md --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts status --json --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts capabilities install voice --target erbine --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts capabilities uninstall voice --target erbine --remove-files --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts uploads put --machine erbine --file ./large-runbook.zip --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts uploads list --machine erbine --recent --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts proposals groups --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts proposals merge-group GROUP_KEY --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts proposals auto-merge --config ~/.config/brainstack/brainstack.yaml --json
bun run packages/brainctl/src/main.ts proposals batch-merge --config ~/.config/brainstack/brainstack.yaml --json
bun run packages/brainctl/src/main.ts proposals reprocess --config ~/.config/brainstack/brainstack.yaml --limit 5
bun run packages/brainctl/src/main.ts import codex-session SESSION_ID --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts lifecycle status --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts lifecycle repair --config ~/.config/brainstack/brainstack.yaml --dry-run
bun run packages/brainctl/src/main.ts daemon status --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts hooks status --target all
bun run packages/brainctl/src/main.ts upgrade --profile control --config examples/control.yaml
bun run packages/brainctl/src/main.ts destroy --config ~/.config/brainstack/brainstack.yaml --dry-run
```

`init` is fresh-install only. Use `upgrade` or `apply-runtime` for existing installs; those commands do not silently seed or rewrite canonical shared-brain content.

Proposal merge commands are control-host work. On enrolled client profiles, `proposals merge-group`, `proposals auto-merge`, and `proposals batch-merge` forward over the configured control SSH route by default; use `--local` only for development or single-node testing.

For normal installed-machine maintenance, prefer `brainctl lifecycle status|repair|upgrade|uninstall`. These are safer orchestration wrappers around the lower-level commands: `status` is bounded and read-only, `repair` re-renders runtime and local harness guidance files plus daemon/hooks/skills surfaces, `upgrade` performs the existing backup plus runtime refresh, and `uninstall` delegates to manifest-driven `destroy`. Uninstall defaults to full managed artifact removal for control/single-node installs and client/worker artifact removal for edge installs.

The normal installed config path is `~/.config/brainstack/brainstack.yaml`. If a command points at a missing config, `brainctl` prints the provision command to create it and lists nearby existing `*.brainstack.yaml` candidates instead of surfacing a raw filesystem `ENOENT`.

Use `brainctl status --json` as the stable machine-facing status surface for local UI, menu bar, and automation consumers. It is read-only, uses short bounded checks, reports daemon, shared-brain, outbox, hooks, skills, brain API, curator, proposals, telemux, fleet, and product-git state, and exits successfully with degraded section details when optional services are offline.

Build a current-platform standalone CLI:

```bash
cd ~/brainstack
bun build packages/brainctl/src/main.ts --compile --no-compile-autoload-dotenv --no-compile-autoload-bunfig --outfile dist/brainctl
```

The deterministic flags prevent the compiled binary from implicitly loading local `.env` or `bunfig.toml` files from the release machine. The compiled CLI embeds the `packages/client-bootstrap` assets and `packages/skills`, so `client-macos` provisioning, doctor, init, `bootstrap-client`, and `skills install` can run from the binary without a Brainstack source checkout or Bun installed on that Mac. A binary client still needs Git, SSH, Tailscale for the current tailnet workflow, and the selected harness CLI. Control, worker, and single-node profiles still require Bun because generated services run the product source under Bun.

For a frictionless client install, generate a private invite on the control host:

```bash
brainctl invite create \
  --config ~/.config/brainstack/brainstack.yaml \
  --import-token-file ~/brain-import-token.txt \
  --control-ssh operator@brain-control \
  --ssh-known-hosts-file ~/.config/brainstack/control_ssh_known_hosts
```

Then run the release installer on the client machine after publishing release assets:

```bash
curl -fsSL 'https://github.com/Caimeo-com/brainstack/releases/download/vX.Y.Z/install.sh' | sh
```

Paste the printed invite when prompted. `brainctl invite create` pins the generated installer command to the package release by default, and release-built `install.sh` is stamped so it downloads `brainctl` from the same tag. Pass `--install-version latest` only when you deliberately want a moving release. The installer only downloads and verifies `brainctl`; enrollment, config rendering, token installation, client bootstrap, and post-install doctor checks live in the versioned CLI. See [`docs/install-one-line.md`](./docs/install-one-line.md).

Use `brainctl invite create --skills-profile operator` for an admin or daily-driver machine that should receive every public Brainstack skill during enrollment. The default `client` profile is for ordinary enrolled machines. Installer flags can override the invite: `--skills-profile client|operator|control|worker|none`, `--skip-skills`, `--skip-doctor`, `--skip-init`, and `--skip-enroll` are documented in [`docs/install-one-line.md`](./docs/install-one-line.md#flag-reference).

Generated source-run services also invoke Bun with `--no-env-file` and load explicit `*.runtime.env` plus operator-owned `*.secrets.env` files. This keeps service behavior deterministic instead of depending on whichever `.env` happens to exist near the product repo.

## Profiles

- `single-node`: one machine runs `braind`, optional `telemux`, bare/staging/serve clones, and Tailscale Serve.
- `control`: control host with `braind`, optional `telemux`, and worker orchestration.
- `worker`: worker host reachable by normal OpenSSH over Tailscale; no Telegram polling and no admin ingest token.
- `client-macos`: local clone plus Codex/Claude/Cursor bootstrap; no local services.

Private journaling should use an explicit separate repo/service/token boundary. Local project context can search and write to explicitly configured brains through `.brainstack.yaml` and `~/.config/brainstack/profiles.yaml`, with local allow rules for personal sections and source-labelled retrieval.

Start with the quickstart docs in `docs/`.
See [`docs/fresh-machine-install.md`](./docs/fresh-machine-install.md) for bootstrapping a new control, worker, or client machine from prerequisites through `doctor`.
See [`docs/portable-skills.md`](./docs/portable-skills.md) for installing Brainstack's public Codex skill/runbook bundle, importing local or URL skills into the shared brain, refreshing shared skills, and installing fail-open harness hooks. See [`docs/daemon.md`](./docs/daemon.md) for the local `brainctl daemon` mode that keeps client/worker clones, outbox, and shared skills fresh in the background.
See [`docs/diagrams.md`](./docs/diagrams.md) for the read/write/outbox, Telegram coalescing, and control/client/worker topology diagrams.
See [`docs/routines.md`](./docs/routines.md) for scheduled routines, built-in update checks, brain-curator setup, and daily check-ins.
See [`docs/curation.md`](./docs/curation.md) for the proposal state model, curation policy (`manual`/`approval`/`auto`), curator automation, and the `brainctl proposals`/`curator` and Telegram proposal commands.
See [`apps/brainstack-menu/README.md`](./apps/brainstack-menu/README.md) for the macOS menu bar companion app that surfaces `brainctl status --json` with safe one-click actions and an opt-in Operator Mode.
See [`docs/security-postures.md`](./docs/security-postures.md), [`docs/tailscale-exposure.md`](./docs/tailscale-exposure.md), [`docs/multi-brain.md`](./docs/multi-brain.md), and [`docs/outbox-security.md`](./docs/outbox-security.md) for the current posture, exposure, project-context, and outbox boundaries.

`provision` is a first-stage checker/config generator. It does not install Bun, Git, SSH, Tailscale, Codex, or Claude; it fails with install hints when they are missing.

`destroy` is intentionally manifest-driven and destructive only with `--yes`. Use `--scope control|worker|client|all` to limit removal to brainstack-owned artifacts for that role. It never removes package installs, Tailscale enrollment, Codex/Claude auth, or sudo policy.

Client import/propose writes can queue into `~/.local/state/brainstack/outbox/<brain-id>/` when the brain is unreachable. Use `brainctl outbox status|list|flush|retry|purge|purge-corrupt`; flush replays only import/propose payloads and never mutates canonical wiki pages offline. `retry <id>` clears a terminal item after operator review so a later flush can replay it.

Use `brainctl fleet status --json` on the control host, or from an enrolled Mac client with a configured control SSH route, to see every known machine, reachability, service status, and whether its Brainstack product checkout is behind `origin/main`. Use `brainctl fleet update <machine>` or `brainctl fleet update --all` to pull, rebuild `brainctl`, run `upgrade`, and restart managed Brainstack services on the target machine(s). A Mac client can bootstrap an old control host even when that host's installed `brainctl` predates the `fleet` command.

Mac clients can send local files to mobile through the control host's telemux bot with `brainctl telegram send-file`. The file streams over SSH to the control host, telemux uses its local Telegram env, and the bot token never needs to exist on the client.

Operators can also stage large or sensitive local files directly onto a Brainstack machine with `brainctl uploads put --machine <machine> --file <path>`. Uploads live in that machine's private Brainstack state under date-stamped folders, can be listed with `brainctl uploads list`, and can be removed with `brainctl uploads rm`. Telemux understands `/uploads` and phrases like "the file I just uploaded" in bound topics, so large files can bypass Telegram's Bot API download limit while still being easy for a harness to find.

Security defaults are explicit in config:

```yaml
security:
  posture: trusted-tailnet
  bindHost: 127.0.0.1
  trustedExposure: none
```

Expose through Tailscale Serve with `brainctl expose tailscale`; avoid direct public binds.

Brainstack is designed for trusted private networks. In `trusted-tailnet` mode, anyone who can reach the Brainstack service on your private mesh can read the brain. Tailscale, VPN routing, and grants are the boundary in this mode; do not expose it to the public internet. Future `guarded` mode may add stricter app-layer read auth, but the default path intentionally avoids password and IAM ceremony.

Client/worker bootstrap accepts `BRAIN_IMPORT_TOKEN` or `BRAIN_IMPORT_TOKEN_FILE` and writes it into `~/.config/shared-brain.env` only when the token slot is blank. `brainctl doctor --write-smoke` performs an explicit mutating import smoke test when you want to prove pushback is ready.

Read [`docs/operator-preflight.md`](./docs/operator-preflight.md) before enabling `telemux`. The control-plane profile assumes a trusted private machine; telemux passes authorized Telegram work into the configured harness process and is not a sandbox.

## License

Brainstack is released under the [MIT License](./LICENSE).
