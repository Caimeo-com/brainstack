# Folder Packs

Folder Packs let an enrolled machine sync a local folder to a Brainstack machine as reusable, on-demand context. Packs are private machine state. They are not shared-brain memory, and prompts receive only paths, metadata, and freshness notes.

Use uploads for one-off files. Use Folder Packs for evolving folders you expect to reference repeatedly, such as local research folders, long-running project notes, or generated runbook collections.

## CLI

Create or refresh a pack from the machine that owns the source folder:

```bash
brainctl context-packs put --machine erbine --name lindy-notes --dir ~/context/lindy-notes --config ~/.config/brainstack/brainstack.yaml
brainctl context-packs sync --machine erbine --name lindy-notes --config ~/.config/brainstack/brainstack.yaml
```

Preview the scan before copying:

```bash
brainctl context-packs put --machine erbine --name lindy-notes --dir ~/context/lindy-notes --dry-run --json
```

List, attach, detach, delete, and clean unused copies:

```bash
brainctl context-packs list --machine erbine
brainctl context-packs attach --context erbine-lindy --machine erbine --name lindy-notes
brainctl context-packs detach --context erbine-lindy --machine erbine --name lindy-notes
brainctl context-packs rm --machine erbine --name lindy-notes
brainctl context-packs gc --machine erbine
brainctl context-packs gc --machine erbine --yes
```

`gc` is a dry run unless `--yes` is passed. Attached packs and source-owned registered packs are protected.

## Telegram

In a bound Telemux topic:

```text
/packs
attach pack lindy-notes
use pack lindy-notes and inspect the README
sync pack lindy-notes
detach pack lindy-notes
```

`use pack ...` does not paste folder contents into the prompt. Telemux attempts a bounded refresh first when the pack is referenced; if the source definition is not available from that surface or the refresh times out, it injects a clear warning and the last synced folder path/manifest metadata so the harness can open only the files it needs. `sync pack ...` is an explicit refresh request and must run from a machine that has the source definition, or it will tell you to sync from the source machine.

## Mac App

The Brainstack menu app includes **Folder Packs…**:

- choose a target machine and folder;
- review the preflight summary before rsync starts;
- sync an existing pack;
- attach or detach a pack from a context slug;
- delete one pack or check/delete unused pack copies.

## Safety

Folder Packs use `rsync --delete` into a stable `current/` directory. Re-syncing updates that directory in place instead of creating duplicate snapshots.

Brainstack rejects symlinks and special files. It skips common cache/build/dependency folders and sensitive-looking files by default, including env files, private keys, cert bundles, token/session/cookie/password names, and shell/config directories. If a folder contains secrets, prefer a one-off upload and delete it after use.

There is no hard size limit. Preflight warns about large folders, high file counts, skipped sensitive files, and destination free space when available.
