# Backup And Restore

## What To Back Up

- Product repo: `~/brainstack`
- Shared brain bare repo: `~/shared-brain/bare/shared-brain.git`
- Shared brain staging clone: `~/shared-brain/staging/shared-brain`
- Shared brain serve clone: `~/shared-brain/serve/shared-brain`
- Blob store: `~/.local/state/brainstack/blobs/shared-brain`
- Config/env: `~/.config/brainstack`
- Telemux state: `~/.local/state/brainstack/telemux`
- Factory workspaces: `~/.local/state/brainstack/factory`
- Optional private brain repo: `~/private-brain`

Legacy compatibility deployments may also need historical service env files, service units, and any pre-brainstack telemux/factory state paths. Keep those details in host-specific migration notes, not public examples.

## Backup Command

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts backup --profile control --config examples/control.yaml
```

For a quieter telemux backup:

```bash
bun run packages/brainctl/src/main.ts backup --profile control --config examples/control.yaml --pause-telemux
```

The backup command copies configured repo/state/config paths into a timestamped directory and writes a manifest. When `sqlite3` is available, telemux `db.sqlite` is copied with SQLite `.backup`; otherwise it falls back to a plain file copy and records that in the manifest. `--pause-telemux` stops the user `telemux.service` before backup and restarts it afterward if it was active.

Keep backup permissions restricted because env files may be included. Backups are not fully crash-consistent for all git working trees or factory workspaces; pausing telemux reduces concurrent writes, but external editors or harnesses can still modify files during backup.

`brainctl destroy` is not a backup substitute. It removes only artifacts recorded in the brainstack ownership manifest or otherwise proven to be product-owned. It intentionally leaves system packages, Tailscale enrollment, Codex/Claude auth, sudo policy, and brain repos unless explicit removal flags are supplied.

## Restore Dry Run

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts restore --backup ~/.local/state/brainstack/backups/brainstack-backup-YYYY... --target /tmp/restore-check
```

## Restore Apply

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts restore --backup ~/.local/state/brainstack/backups/brainstack-backup-YYYY... --target /tmp/restore-check --apply
```

## Restore Smoke Test

After restore, run:

```bash
git --git-dir /tmp/restore-check/shared-brain/bare/shared-brain.git fsck
git -C /tmp/restore-check/shared-brain/staging/shared-brain status --short
```

Then point a temporary config at the restored paths and run `brainctl doctor`.

## Write Lock Recovery

`braind` uses `.shared-brain.lock` in the staging clone to serialize Git writes. It does not auto-break that lock after a timeout because doing so can corrupt a live write. If a host crashes while holding the lock, later writes fail closed until an operator inspects it.

Inspect first:

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts repo-lock status --config ~/.config/brainstack/brainstack.yaml
```

Clear only after confirming no `braind` write is still running and the owner metadata is stale:

```bash
bun run packages/brainctl/src/main.ts repo-lock clear --config ~/.config/brainstack/brainstack.yaml --yes --token <clear_token>
```

Copy `<clear_token>` from the status output after inspection. The clear command only removes the matching Brainstack repo-lock owner/release marker files and refuses unknown entries. It is not a recursive cleanup tool. Use `--force` only after verifying the owner process is not active.

If status reports `clear_token=EMPTY`, the lock directory has no owner metadata or release marker, usually because acquisition or release was interrupted at the filesystem boundary. Clear that only after the same no-active-write check, using `--force --token EMPTY`; normal token-guarded locks should not use this path.

Every clear attempt that reaches deletion is appended to `~/.local/state/brainstack/lock-recovery.jsonl` with the lock path, clear token, force flag, age, safety decision, and owner metadata.

## Idempotency Recovery

Import/propose requests with `Idempotency-Key` are recorded under `derived/idempotency`. If a request crashes after side effects may have started, `braind` moves it to `review_required` instead of replaying it. Client outboxes keep retrying short-lived `425` in-progress responses, but repeated `425` responses are promoted to terminal operator review so they do not retry forever. The default terminal threshold is 12 flush attempts and can be adjusted with `BRAINSTACK_OUTBOX_MAX_425_RETRIES`.

If an idempotency lock directory itself is stuck, inspect it with:

```bash
bun run packages/brainctl/src/main.ts locks status --config ~/.config/brainstack/brainstack.yaml --path /path/to/derived/idempotency/<endpoint>/<key>.json.lock
```

Clear it only after confirming no matching request is active and copying the reported token:

```bash
bun run packages/brainctl/src/main.ts locks clear --config ~/.config/brainstack/brainstack.yaml --path /path/to/derived/idempotency/<endpoint>/<key>.json.lock --yes --token <clear_token>
```

When an outbox item becomes terminal, inspect the related `derived/idempotency/...json` record and the committed artifacts/proposals before retrying with a new key or purging the queued item.
