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

The backup command copies configured repo/state/config paths into a timestamped directory and writes a manifest. Keep backup permissions restricted because env files may be included.

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
