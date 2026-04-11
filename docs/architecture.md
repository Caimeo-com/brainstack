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

`braind` synchronizes the staging clone under the repo lock before every write. If staging is clean and behind `origin/main`, it fast-forwards. If staging is dirty, ahead, or diverged, the write fails with a precise error instead of risking a stale-clone push or silent overwrite.

Search uses local SQLite under `derived/` in the serve clone and is never shared over a network filesystem.

## Install And Upgrade Boundary

`brainctl init` is a fresh-install command. It seeds canonical shared-brain content only when the canonical repo is empty, unless the operator explicitly passes `--seed-missing` or `--force-seed`.

Use `brainctl upgrade` or `brainctl apply-runtime` for existing installs. Those commands render and apply runtime artifacts such as service files, hooks, env examples, Tailscale Serve config, and bootstrap files, but they do not silently rewrite canonical wiki pages, manifests, raw artifacts, proposals, or logs.

Runtime env and secrets env are split:

- `braind.runtime.env` and `telemux.runtime.env` are generated and overwritten by upgrade.
- `braind.secrets.env` and `telemux.secrets.env` are operator-managed, created only if missing, and never overwritten.

`workers.json` is rendered from `brainstack.yaml`. Treat `brainstack.yaml` as the source of truth for workers; do not edit `workers.json` directly.

When telemux is enabled and both `BRAIN_BASE_URL` and `BRAIN_IMPORT_TOKEN` are present in its control-host env, successful runs import `.factory/SUMMARY.md` and `.factory/ARTIFACTS.md` back into `braind` as raw artifacts. This is import-only; telemux does not directly edit canonical wiki pages.

## Large Files

Text and normalized extracts stay in git. Small binaries may stay in git. Large binary originals above the configured threshold are stored in a content-addressed blob store outside git with pointer manifests and normalized extracts committed to git.
