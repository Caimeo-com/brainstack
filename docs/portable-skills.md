# Portable Skills

Brainstack ships public, generic agent skills under `packages/skills`. They are product runbooks for Codex-style skill systems, not shared-brain data.

Install them from a source checkout or standalone `brainctl` binary:

```bash
brainctl skills install --target codex --profile client
```

Use the operator bundle on control hosts or admin machines:

```bash
brainctl skills install --target codex --profile operator
```

Profiles:

- `client`: client read/import/propose discipline plus the generic Brainstack runbook.
- `operator`: all public Brainstack skills.
- `control`: control-host operations, curation, and remote-machine operations.
- `worker`: worker/client discipline and remote-machine operations.

Invite enrollment also accepts `--skills-profile none` and installer `--skip-skills` when no Codex skills should be installed. `none` is an enrollment opt-out, not a `brainctl skills install --profile` bundle.

Options:

- `--skill NAME` installs one or more explicit skills.
- `--all` installs every public Brainstack skill.
- `--dir DIR` writes into a custom Codex skills root instead of `CODEX_HOME/skills` or `~/.codex/skills`.
- `--dry-run` prints the target files without writing.

The public bundle intentionally excludes private topology. Do not add real hostnames, local usernames, Telegram chat ids, tailnet names, env values, or customer-specific service paths to `packages/skills`. Keep those in a private local overlay skill.

## Shared Skill Imports

Use `brainctl import skill` when a skill should become shared-brain material instead of remaining a one-machine local file. Local inputs preserve the local folder as-is; URL inputs fetch from the URL or clone the Git remote when the URL is a repository/tree URL.

```bash
brainctl import skill ~/.codex/skills/brainstack/SKILL.md \
  --config ~/.config/brainstack/brainstack.yaml

brainctl import skill ~/.codex/skills/my-private-overlay \
  --config ~/.config/brainstack/brainstack.yaml

brainctl import skill https://github.com/example/brainstack-skill \
  --config ~/.config/brainstack/brainstack.yaml
```

To discover local skills first, use the deterministic bulk planner:

```bash
brainctl import skills --config ~/.config/brainstack/brainstack.yaml
brainctl import skills --config ~/.config/brainstack/brainstack.yaml --apply
```

`import skills` scans the current directory plus the default skill roots for Codex, Claude, and Cursor. It prints a no-side-effect plan by default, groups duplicate skill names deterministically, prefers the current directory over harness home directories, skips unchanged skills already present in the local shared-brain clone, and only writes global shared-brain imports when `--apply` is passed.

Inputs:

- A `SKILL.md` path packages the entire parent skill directory, not only the one file.
- A directory must contain a regular `SKILL.md`.
- A URL can point at a raw `SKILL.md`, a Git repository URL, or a GitHub repository/tree/blob URL.

Safety flags:

- `--max-bytes N`: total package cap. Default is 2 MiB.
- `--max-files N`: file count cap. Default is 200.
- `--max-file-bytes N`: per-file cap. Default is 512 KiB.
- `--allow-private-url`: allow localhost, RFC1918, Tailscale/CGNAT, link-local, and private IPv6 URL fetches for trusted private skill sources. Omit it for normal public URL imports.

Bulk planner flags:

- `--target codex|claude|cursor|all`: choose which harness home skill roots to scan. Default is `all`.
- `--scan-dir DIR`: add another recursive scan root.
- `--skill NAME`: plan or apply only a named skill.
- `--max-depth N`: cap recursive current-directory scans. Default is 5.
- `--max-scan-dirs N`: cap scanned directories per root. Default is 1500.
- `--json`: emit the deterministic plan as JSON.
- `--force`: include skills whose file content already matches the shared-brain package.
- `--apply`: enqueue the proposed imports. This makes them global shared-brain material that connected harnesses can refresh.

The importer rejects symlinks, hardlinks, non-regular files, traversal paths, oversized packages, and private raw-file URLs unless `--allow-private-url` is explicitly passed. If the shared brain is offline or the import token is missing, the package queues through the normal outbox and can be replayed with `brainctl outbox flush`.

## Refresh And Doctor

Connected machines install shared skill packages from their local shared-brain clone:

```bash
brainctl skills refresh \
  --config ~/.config/brainstack/brainstack.yaml \
  --target codex
```

Useful flags:

- `--repo PATH`: read packages from a specific shared-brain clone instead of the configured client clone.
- `--dir DIR`: install into a custom skill root.
- `--skill NAME`: refresh only one named skill.
- `--no-sync`: skip `git pull --ff-only` before scanning.
- `--force`: replace an existing local skill directory even when Brainstack did not install it.
- `--quiet`: suppress normal output for hook use.

Refresh refuses to clobber an existing unmarked local skill unless `--force` is passed. Brainstack-managed installs include `.brainstack-skill-package.json`.

Run diagnostics before sharing or after a suspicious refresh:

```bash
brainctl skills doctor --dir ~/.codex/skills
brainctl skills doctor --dir ~/.codex/skills --check-remote
```

`--check-remote` asks Git remotes whether the current branch has a different remote head. It can use the network and is intentionally not part of routine hook execution.

## Harness Hooks

Hooks are opt-in background integration. They refresh shared skill packages on session or prompt start and write local checkpoint metadata, but they are fail-open: if Brainstack, Git, or the local clone is unavailable, the harness should keep running.

```bash
brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml --dry-run
brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml
brainctl hooks install --target all \
  --config ~/.config/brainstack/brainstack.yaml

brainctl hooks status --target all
brainctl hooks remove --target all
```

Targets:

- `codex`: writes `~/.codex/hooks.json`.
- `claude`: writes `~/.claude/settings.json`.
- `cursor`: writes `~/.cursor/hooks.json` using Cursor's hooks format.
- `all`: applies all three.

`brainctl hooks install` merges Brainstack-managed entries and preserves unrelated hooks. `remove` deletes only Brainstack-managed entries. Codex and Claude may still require their own hook trust/review flow before non-managed command hooks run.

Prefer `brainctl lifecycle repair` for routine installed-machine repair. It composes runtime refresh, local harness guidance repair, daemon install, hook install, and shared skill refresh. Use `hooks install` directly only when you intentionally want to touch hook config alone.

Hooks do not guarantee full transcript capture. When the harness supplies a regular `transcript_path` on a stop event, Brainstack queues a small `codex-session-checkpoint` import in the local outbox so the daemon can flush it later. The checkpoint records the session id, cwd, and local transcript path, not the prompt/tool transcript body. This avoids per-turn spam and accidental raw prompt leakage, but it also means a missing proposal can still happen when the harness never supplied transcript metadata or the useful lesson was edited directly into a local skill/memory file.

To import a Codex Desktop session explicitly from the machine that owns the log:

```bash
brainctl import codex-session 019ebbfc-3a60-7f61-a4fa-a89282b8d83f \
  --config ~/.config/brainstack/brainstack.yaml

brainctl import codex-session 019ebbfc-3a60-7f61-a4fa-a89282b8d83f \
  --config ~/.config/brainstack/brainstack.yaml \
  --include-transcript
```

The first command imports bounded session metadata plus the last agent message. `--include-transcript` imports the JSONL transcript body as explicit operator action and is capped by `--max-bytes`.

## Local Daemon

On enrolled client and worker machines, use the local daemon when clone freshness, outbox flushes, and shared skill refreshes should happen without every prompt hook doing Git or network work:

```bash
brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml
brainctl daemon install --config ~/.config/brainstack/brainstack.yaml
brainctl daemon install --config ~/.config/brainstack/brainstack.yaml --start
brainctl daemon status --config ~/.config/brainstack/brainstack.yaml
brainctl daemon once --config ~/.config/brainstack/brainstack.yaml
```

`lifecycle repair` is the normal command after enrollment. `brainstackd` is the service name, not a second installed binary. The service runs `brainctl daemon run`, writes bounded local status under Brainstack state, and leaves harness hooks fail-open. If the clone is missing, dirty, or not a real Git checkout, the daemon records degradation and refuses to install shared skills from that unsafe source.

## Client File Relay

The public skills know about `brainctl telegram send-file` because it is product behavior. An enrolled client can stream a file over SSH to the control host, and telemux sends it through the control host's Telegram bot. The client should not store Telegram bot tokens.

Use:

```bash
brainctl telegram send-file \
  --config ~/.config/brainstack/brainstack.yaml \
  --file ~/Downloads/report.pdf \
  --caption "Report from this machine"
```

Invites can carry the control SSH target, remote Brainstack repo path, and pinned host keys so routine file relay does not need ad hoc `scp`, provider credentials, or `ssh-trust accept-new`.
