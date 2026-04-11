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
