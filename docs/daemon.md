# Brainstack Daemon

`brainstackd` is not a separate user-facing binary. It is `brainctl` running in daemon mode:

```bash
brainctl daemon run --config ~/.config/brainstack/brainstack.yaml
```

The daemon is for client and worker machines that should keep local Brainstack state fresh in the background. It does not replace `braind`, `telemux`, `brainctl`, or harness hooks.

## Responsibilities

- Pull the local shared-brain clone with `git pull --ff-only`.
- Refuse to pull dirty, missing, non-git, or otherwise unsafe clone paths.
- Flush queued imports/proposals from the local outbox.
- Refresh shared skill packages from the already-local shared-brain clone.
- Write bounded local status under `~/.local/state/brainstack/daemon/status.json`.
- Write local event records under `~/.local/state/brainstack/daemon/events.jsonl`.

The daemon does not make canonical wiki edits, run LLMs, repair dirty clones silently, store admin tokens, or block harness prompts.

## Commands

```bash
brainctl daemon install --config ~/.config/brainstack/brainstack.yaml
brainctl daemon install --config ~/.config/brainstack/brainstack.yaml --start
brainctl daemon status --config ~/.config/brainstack/brainstack.yaml
brainctl daemon status --config ~/.config/brainstack/brainstack.yaml --json
brainctl daemon once --config ~/.config/brainstack/brainstack.yaml
brainctl daemon logs --config ~/.config/brainstack/brainstack.yaml
brainctl daemon uninstall --config ~/.config/brainstack/brainstack.yaml
```

On macOS, `install` writes `~/Library/LaunchAgents/com.brainstack.daemon.plist`.

On Linux, `install` writes `~/.config/systemd/user/brainstackd.service`.

Use `--target codex|claude|cursor|all` to choose which harness skill roots are refreshed. The default is `all`.

Use `--interval-seconds N` for `run`; the default is 60 seconds.

Use `--brainctl PATH_OR_COMMAND` with `install` when the generated service should run a specific standalone `brainctl` binary or source-run command. Without it, `brainctl` records the current executable path.

Use `--platform launchd|systemd` only for testing or unusual cross-platform rendering. The default is `launchd` on macOS and `systemd` elsewhere.

Use `--start` with `install` to ask launchd/systemd to load the service immediately after writing the service file. On systemd this also restarts an already-running `brainstackd.service`, which matters after replacing the `brainctl` binary. Without `--start`, `install` writes the file and prints the activation command.

Use `--lines N` with `logs` to change how many lines are printed from local daemon logs. The default is 80.

Use `--dry-run` with `install` or `uninstall` to print the planned service file or removal without writing.

Use `--no-sync`, `--no-flush`, or `--no-skills` only for debugging a specific daemon job. Routine installs should leave all three enabled.

## Hook Interaction

Harness hooks should stay cheap. They record checkpoint metadata, read daemon freshness, and skip routine Git/network work when the daemon has updated recently. If the daemon is missing or stale, hooks may attempt a local-only shared skill refresh and still fail open. Hooks also refuse to refresh skills from a missing, dirty, symlinked, or non-Git shared-brain clone.

When a harness stop hook supplies a regular `transcript_path`, the hook queues a small `codex-session-checkpoint` import into the local outbox without doing a network write. The daemon later flushes that outbox entry. The checkpoint is intentionally not the full transcript; use `brainctl import codex-session --include-transcript` when raw JSONL evidence should be imported deliberately.

## Doctor

`brainctl doctor` reports:

- daemon service installed or missing
- daemon service running or inactive
- last daemon run time
- local shared-brain freshness
- last outbox and skill-refresh state

`brainctl daemon status --json` reports `ok=false` when the service is missing, inactive, stale, or when the status pid is not alive, even if an old status file still says the last daemon iteration succeeded.

For control hosts, `doctor` also checks whether the configured `braind` port is owned by the managed `braind.service` MainPID. If an orphan process owns the port, doctor fails with the listener pid so the operator can stop it and restart the managed service.

Missing daemon state is a warning, not a correctness failure. Brainstack still works through `brainctl`, hooks, outbox, and the control-host `braind` ingest path.
