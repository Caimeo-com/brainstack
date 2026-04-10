# Architecture

`brainstack` separates product code from data.

## Product Code

- `apps/braind`: web/API/search/import service.
- `apps/telemux`: optional Telegram/Codex control plane.
- `packages/brainctl`: deterministic installer/operator CLI.
- `packages/client-bootstrap`: harness client bootstrap artifacts.
- `packages/skills`: portable skills, outside brain content.

## Shared Brain Data

The canonical shared brain is a git repo of markdown, manifests, raw artifacts, proposals, and logs.

Fresh installs use three repo views:

- Bare repo: canonical remote.
- Staging clone: writable clone for import, propose, ingest, lint, and admin changes.
- Serve clone: read-serving clone updated by post-receive hook.

Search uses local SQLite under `derived/` in the serve clone and is never shared over a network filesystem.

## Large Files

Text and normalized extracts stay in git. Small binaries may stay in git. Large binary originals above the configured threshold are stored in a content-addressed blob store outside git with pointer manifests and normalized extracts committed to git.

