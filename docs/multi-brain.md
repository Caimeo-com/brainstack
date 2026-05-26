# Project Context And Multi-Brain Use

Brainstack has a small project-triggered context surface so a harness can discover the right brains from the repository it is working in. Project config can live in a repo-local `.brainstack.yaml` and user-level defaults can live in `profiles.yaml`. Repo-local config declares intent; user-level profiles are what trust concrete URLs, remotes, token environment names, and local clone paths.

Before substantial repo work, run:

```bash
brainctl context --repo .
```

Then use:

```bash
brainctl search --repo . "deployment checklist"
brainctl remember --repo . --summary "Decision summary here"
```

`brainctl context` reads configuration in this order:

1. explicit CLI flags
2. `.brainstack.yaml` in the repo or a parent directory
3. `~/.config/brainstack/profiles.yaml` project match
4. `~/.config/brainstack/profiles.yaml` default profile

## Repo Config

Example `.brainstack.yaml`:

```yaml
project: lindy-api

brains:
  - id: lindy
    url: https://lindy-brain.example.ts.net
    remote: operator@brain-control:/home/operator/shared-brain/lindy.git
    read: true
    write: true

  - id: personal
    url: https://personal-brain.example.ts.net
    remote: operator@brain-control:/home/operator/shared-brain/personal.git
    read:
      sections:
        - shared/work-safe
        - work/devops
      mode: ask-once
    write: propose-only

crossBrainWrites:
  personalToLindy: ask
  lindyToPersonal: never
```

`url` is accepted as the write API base URL. `baseUrl` is also accepted for older configs. `remote` or `gitRemote` lets `brainctl context` clone missing local brains and pull existing clones after those exact fields are trusted in `~/.config/brainstack/profiles.yaml`. Repo-local `.brainstack.yaml` files are not trusted to bind URLs, token environment names, remotes, or explicit `localClone` / `localPath` / `path` values on their own, even when a path points under Brainstack's managed clone root. Until a matching profile entry exists, `brainctl context` marks the brain `[pending-trust]`, `brainctl search` skips it, and `brainctl remember` refuses to write to it.

Repo-local `write: true` also declares intent only. Direct `/api/import` writes are active only when the matching user-level profile entry says `write: true`; otherwise the effective mode is downgraded to `propose-only` and `context` prints the downgrade.

To trust a repo-local brain today, add the concrete fields to `~/.config/brainstack/profiles.yaml`:

```yaml
brains:
  lindy:
    url: https://lindy-brain.example.ts.net
    importTokenEnv: BRAIN_IMPORT_TOKEN
    remote: operator@brain-control:/home/operator/shared-brain/lindy.git
    localClone: ~/shared-brain/lindy
    classification: work
    write: true
```

Then rerun:

```bash
brainctl context --repo .
```

Section names are path prefixes inside the local clone. They are retrieval boundaries, not hard security boundaries. If a harness has shell access to the clone, it can read files directly.

## User Profiles

Example `~/.config/brainstack/profiles.yaml`:

```yaml
default:
  brains:
    - personal
  writeDefault: personal

brains:
  personal:
    url: https://personal-brain.example.ts.net
    localClone: ~/shared-brain/personal
  lindy:
    url: https://lindy-brain.example.ts.net
    localClone: ~/shared-brain/lindy

projects:
  "~/dev/lindy/**":
    brains:
      - lindy
      - personal
    personal:
      sections:
        - shared/work-safe
        - work/devops
      mode: ask-once
    writeDefault: lindy
    crossBrainWrites:
      personalToLindy: ask
      lindyToPersonal: never
```

Personal-looking brains require a local allow rule before non-interactive `context` and `search` include them:

```bash
brainctl allow repo --repo . --brain personal --sections shared/work-safe,work/devops --once
brainctl allow repo --repo . --brain personal --sections shared/work-safe,work/devops --always
```

Allow rules are local files under the Brainstack config root and are written `0600`.

## Source Labels

Multi-brain retrieval must not become one undifferentiated memory pool. `brainctl search` labels every result:

```text
[lindy / wiki/Deploy.md:12] rollback checklist
[personal / shared/work-safe/Debug.md:4] debugging heuristic
```

`brainctl remember` records repo/source metadata and refuses non-interactive personal-to-work writes when recent search sources make the crossing meaningful unless the command includes `--confirm-cross-brain` and the policy permits it. A `never` policy blocks the write.

## Recommended Company Pattern

Good:

```text
user laptop -> company brain + allowed personal sections
```

Bad:

```text
company brain server or company-controlled worker -> user's full private brain
```

For company workers, configure company brain only by default. Let the user's own local Claude/Codex query both personal and company brains when the repo and local allow rules make that explicit.
