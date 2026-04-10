# Clawdex Publication Readiness

## Current status

This folder is already a git repository and already has a usable ignore policy for public source control.

Intended public home:

- `github.com/swader/clawdex`

Tracked today:

- source under `src/`
- tests under `test/`
- docs under `docs/`
- systemd unit under `systemd/`
- reproducible examples under `.env.example` and `workers.example.json`

Ignored today:

- `.env`
- `workers.json`
- `data/`
- `.factory/`
- `dist/`
- `node_modules/`

That means the repo is close to publishable, but not fully clean for public consumption yet.

## What was not abstract enough

Before this pass, the repo still embedded the current deployment assumptions in a few places:

- machine-specific host names
- a real-looking Telegram user id in `.env.example`
- repo-specific Git remote instructions in `docs/operations.md`
- machine-local Codex and sudo setup existing only outside the repo

Those are survivable privately, but they are poor defaults for a public repo.

## What changed in this pass

- `.env.example` now uses generic placeholder values.
- `workers.example.json` now uses generic worker names.
- `src/config.ts` now defaults to a generic local machine name and a non-usable Telegram user id when no env value is supplied.
- `README.md`, `docs/architecture.md`, `docs/operations.md`, and `NEXT_STEPS.md` were pushed toward generic control-host / remote-worker language.
- `docs/fresh-machine-bootstrap.md` now documents the full machine rebuild path, including passwordless sudo and Codex permanent yolo mode.
- `templates/codex/config.toml.example` and `templates/sudoers/factory-nopasswd` were added so critical non-repo state has tracked templates.
- `templates/codex/AGENTS.md` and `scripts/bootstrap-codex.sh` now make the Codex dotfile bootstrap one command instead of a manual copy exercise.
- An MIT [LICENSE](../LICENSE) file is now present.

## Remaining safe steps before a public GitHub push

These can be done without breaking the current machine because they only affect tracked examples, docs, or release mechanics:

1. Replace any remaining deployment-specific prose in docs with placeholders or explicit “current deployment” callouts.
2. Add a short security note warning that the documented Codex setup is intentionally high-trust.

## What should stay machine-local

These are relevant to the system, but should not be committed with live values:

- `~/clawdex/.env`
- `~/clawdex/workers.json`
- `/srv/telemux/db.sqlite`
- `/srv/telemux/contexts/`
- `/srv/telemux/crons/`
- `/srv/factory/`
- SSH private keys
- Telegram bot token

## Other relevant codebase or config to document

There is not a second large application repo required to understand the system. The main missing pieces are machine-local configuration, not a separate codebase.

The most relevant extra artifacts are:

- `~/.codex/config.toml`
- `~/.codex/AGENTS.md`
- `/etc/sudoers.d/factory-nopasswd`
- optional SSH and Tailscale configuration for remote workers

If you publish this project, document those artifacts and keep redacted templates in-repo, but do not publish live credentials or host-specific secrets.
