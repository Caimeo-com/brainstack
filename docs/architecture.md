# Architecture

`brainstack` separates product code from data.

## Product Code

- `apps/braind`: web/API/search/import service.
- `apps/telemux`: optional Telegram control plane for Codex or Claude.
- `packages/brainctl`: deterministic installer/operator CLI.
- `packages/client-bootstrap`: harness client bootstrap artifacts.
- `packages/skills`: portable skills, outside brain content.

`brainstackd` is `brainctl daemon run`, not a separate binary. On client and worker machines it keeps the local shared-brain clone fresh, flushes the outbox, refreshes shared skills from the local clone, and writes local status for doctor/menubar-style consumers. It is convenience automation only: canonical writes still flow through `braind`, and hooks must fail open.

## Shared Brain Data

The canonical shared brain is a git repo of markdown, manifests, raw artifacts, proposals, and logs.

Fresh installs use three repo views:

- Bare repo: canonical remote.
- Staging clone: writable clone for import, propose, ingest, lint, and admin changes.
- Serve clone: read-serving clone updated by post-receive hook.

`braind` synchronizes the staging clone under the repo lock before every write. If staging is clean and behind `origin/main`, it fast-forwards. If staging is dirty, ahead, or diverged, the write fails with a precise error instead of risking a stale-clone push or silent overwrite.

The repo lock fails closed. Brainstack does not auto-break a lock that looks stale because a slow write can look stale from the outside. Use `brainctl repo-lock status --config ~/.config/brainstack/brainstack.yaml` to inspect owner metadata, copy the reported `clear_token`, then run `brainctl repo-lock clear --config ~/.config/brainstack/brainstack.yaml --yes --token <clear_token>` only after confirming no `braind` write is active. The clear command removes only the matching owner/release marker files and then `rmdir`s the lock directory; `--force` is reserved for operator-confirmed recovery, including the special `--force --token EMPTY` path for an interrupted empty lock directory.

Import/propose idempotency records are durable under `derived/idempotency/`. Duplicate requests replay completed responses, conflicting reuse returns a hard conflict, and expired records that reached `running` become `review_required` instead of producing endless retryable responses.

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
