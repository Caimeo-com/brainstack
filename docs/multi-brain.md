# Project Context And Multi-Brain Use

Brainstack has a small project-triggered context surface so a harness can discover the right brains from the repository it is working in. Project config can live in a repo-local `.brainstack.yaml` and user-level defaults can live in `profiles.yaml`.

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

`url` is accepted as the write API base URL. `baseUrl` is also accepted for older configs. `remote` or `gitRemote` lets `brainctl context` clone missing local brains and pull existing clones. Repo-local `.brainstack.yaml` files are not trusted to point at arbitrary local filesystem paths; explicit `localClone` / `localPath` values should live in `~/.config/brainstack/profiles.yaml`, or use Brainstack's managed clone root under state.

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
