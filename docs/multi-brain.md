# Project Context And Multi-Brain Use

Brainstack now has a small project-triggered context surface. A repo may contain `.brainstack.yaml` describing which brains are relevant for that project.

Example:

```yaml
defaultBrain: work
writeDefault: work
brains:
  - id: work
    label: Work brain
    localPath: ~/shared-brain-work
    baseUrl: https://work-brain.example.ts.net
    importTokenEnv: BRAIN_WORK_IMPORT_TOKEN
    classification: work
    sections: [wiki, raw, proposals]
    write: propose-only
  - id: personal
    label: Personal brain
    localPath: ~/shared-brain-personal
    classification: personal
    sections: [wiki]
    write: false
```

Commands:

```bash
brainctl context --repo .
brainctl allow repo --repo . --brain personal --always
brainctl search --repo . "deployment checklist"
brainctl remember --repo . --summary "Decision summary here"
```

Personal-looking brains (`personal`, `private`, `journal`, `health`, `family`, `finance`) require an explicit local allow rule before non-interactive context/search can include them. `write: false` is read-only; writable non-default brains must set their own `baseUrl` and `importTokenEnv`, and queued writes retain that credential selector. This is a retrieval guardrail, not a hard security boundary. Do not clone private brains onto company workers unless that is explicitly acceptable.

Local project context can search and write to explicitly configured brains, but automatic private-journal provisioning and hard policy routing are not implemented yet. Until that exists, run private journaling through an explicit separate Brainstack install/config and keep its tokens, repo paths, and Telegram topics separate from the shared dev brain.
