# Clawdex

Telegram-driven harness control plane for one control host, with optional remote workers.

Brainstack product note: this app is vendored under `apps/telemux`. Fresh brainstack installs default mutable state to `~/.local/state/brainstack/telemux` and factory workspaces to `~/.local/state/brainstack/factory`. Older `/srv/telemux` and `/srv/factory` paths below describe the original valkyrie deployment and remain relevant only for compatibility migration.

Security note: telemux is not a sandbox. It accepts Telegram messages from the configured allowed user and passes work into the configured harness process, Codex CLI or Claude Code. If that harness is configured for bypass/yolo mode and the Unix user has passwordless sudo, telemux is effectively a remote command path into that authority.

Phase 1 is intentionally usable without a remote worker. `host` and `scratch` topics can run locally on the control machine today, while remote contexts can exist in `pending` state until SSH is ready.

## Quickstart

1. Copy `.env.example` to `.env`.
2. Copy `workers.example.json` to `workers.json`.
3. Set `FACTORY_TELEGRAM_BOT_TOKEN` in `.env`.
4. Run `./scripts/install.sh` as your normal user. It will prompt for `sudo` only for the `/srv` and linger setup.
5. Run `./scripts/doctor.sh` as your normal user.
6. Open `http://127.0.0.1:8787/`.

If you want the worker file somewhere else, set `FACTORY_WORKERS_FILE` in `.env`.

For a fresh-machine bootstrap, including passwordless sudo for the control user, a one-shot Codex dotfile bootstrap, and a permanent Codex `approval_policy = "never"` / `sandbox_mode = "danger-full-access"` setup, start with [docs/fresh-machine-bootstrap.md](./docs/fresh-machine-bootstrap.md).

## Core behaviors

- Telegram long polling only. No webhook dependency.
- One Telegram topic maps to one durable context.
- Context kinds: `repo`, `host`, `scratch`.
- Context states: `active`, `pending`, `archived`, `error`.
- `/newctx` is usually run once per reusable Telegram topic.
- Plain text in a bound topic starts the configured harness if no session exists, otherwise resumes the stored topic session where supported.
- Telegram captions are treated as user text too, so captioned media messages can drive harness runs.
- Inbound Telegram media is staged into `.factory/inbox/telegram/<message_id>/` inside the bound workspace before the run.
- Images are also forwarded to Codex with `--image` for faster visual response; Claude runs currently receive staged file paths as text context.
- Audio/voice-only Telegram messages are intentionally not forwarded to the harness yet; they are reserved for the later transcription phase.
- While a job is active, the bot sends a lightweight `typing` heartbeat into the same Telegram topic so long runs do not look stalled.
- Scheduled jobs live inside `telemux.service`, not OS cron. The scheduler checks the SQLite job registry on an interval and can proactively post into a Telegram topic even when no one is actively chatting there.
- Scheduled jobs can either send a direct reminder into Telegram or run the configured harness against a stored context/session and post the result back into the bound topic.
- Scheduled jobs have their own optional model/effort overrides. Effective runtime is `cron override -> context override -> global default`.
- `host` and `scratch` contexts auto-create lightweight local git repos so the harness always has a safe working directory.
- Managed `host` and `scratch` workspaces get an initial commit so `git status` and future diffs start clean.
- `repo` contexts can bind an existing repo path or clone a git URL into a managed repo root.
- Worker transport is configurable per worker: `local`, `ssh`, or optional `tailscale-ssh`.

## Filesystem layout

Default paths:

- control-plane DB: `/srv/telemux/db.sqlite`
- control-plane context snapshots: `/srv/telemux/contexts/<slug>/context.json`
- control-plane cron snapshots: `/srv/telemux/crons/<job-id>.json`
- control-plane logs: `/srv/telemux/logs/`
- managed repos: `/srv/factory/repos/`
- managed host contexts: `/srv/factory/hostctx/<slug>/`
- managed scratch contexts: `/srv/factory/scratch/<slug>/`

Inside each managed workspace, durable state lives in `.factory/STATE.json`, `.factory/SUMMARY.md`, `.factory/TODO.md`, and `.factory/ARTIFACTS.md`.

When a context has scheduled jobs linked to it or to its bound Telegram topic, the control plane mirrors them into `.factory/CRONS.md`.

When Telegram file delivery is requested, the control plane also checks for the ephemeral file `.factory/TELEGRAM_ATTACHMENTS.json` after a Codex run. That file is not durable context state; it is a one-run upload manifest for Telegram only.

When a user asks in plain language to create, change, move, pause, resume, or delete a scheduled job, Codex can emit the ephemeral control-plane manifest `.factory/CRON_REQUESTS.json` after the turn. The control plane validates and applies those requested cron changes after the normal reply is sent.

If `BRAIN_BASE_URL` and `BRAIN_IMPORT_TOKEN` are set, successful runs import `.factory/SUMMARY.md` and `.factory/ARTIFACTS.md` into the shared brain as raw artifacts. This is opt-in and import-only.

## Commands

- `/help`
- `/whoami`
- `/explainctx`
- `/synccommands`
- `/showcommands`
- `/workers`
- `/crons`
- `/cron <subcommand>`
- `/mode [fast|normal|max|clear]`
- `/model [model-id|clear]`
- `/effort [low|medium|high|xhigh|clear]`
- `/newctx <slug> <machine> <target> [base-branch]`
- `/bind <machine> <target> [base-branch]`
- `/topicinfo`
- `/run <prompt>`
- `/resume [prompt]`
- `/loop <prompt>`
- `/archive`
- `/detach`
- `/tail`
- `/artifacts`
- `/usage`

`/status` is kept as an alias for `/topicinfo`.

`/cron` subcommands currently include:

- `show <id>`
- `pause <id>`
- `resume <id>`
- `delete <id>`
- `move <id> here`
- `context <id> <slug-or-path>`
- `mode <id> [fast|normal|max|clear]`
- `model <id> [model-id|clear]`
- `effort <id> [low|medium|high|xhigh|clear]`

## Topic patterns

- `control-general`
  `/newctx control-general control host`
- `worker-general`
  `/newctx worker-general worker1 host`
- `Project: bitfalls-dashboard`
  `/newctx bitfalls-dashboard worker1 https://github.com/example/project.git master`
  or `/newctx bitfalls-dashboard control /absolute/path/to/repo`
- `Scratchpad`
  `/newctx scratchpad control scratch`

After a topic is bound, plain text behaves like chatting to Codex in that topic.

Use `/mode`, `/model`, and `/effort` to change the Codex runtime for just that topic without rebinding or losing the stored session. Presets are:

- `fast` -> `gpt-5.4-mini` with `low`
- `normal` -> `gpt-5.4` with `medium`
- `max` -> `gpt-5.4` with `xhigh`

Captioned image and file messages work in the same bound-topic flow. The control plane stages the inbound files into the workspace first, then runs Codex against that updated workspace.

Scheduled jobs can be created either by normal conversation in a bound topic, where Codex writes `.factory/CRON_REQUESTS.json`, or by deterministic job-management commands like `/crons` and `/cron ...`.

One-off reminders do not invoke Codex when they fire. Scheduled Codex jobs do, and they reuse the stored context session so proactive thread messages stay in-context.

If Codex records a real file in `.factory/ARTIFACTS.md` and the user explicitly asks for that file to be sent into Telegram, the control plane can upload it into the same topic instead of replying with a path only.

`/artifacts` shows the artifact notes as text.

`/artifacts send [filter]` uploads matching recorded artifact files into the same Telegram topic.

## Topic lifecycle

- A context is the durable workspace and Codex-session binding for one Telegram topic.
- `/newctx` is normally the one-time setup step for a reusable topic.
- `/bind` is for repointing the current topic later.
- `/archive` marks the current context inactive and detaches the topic.
- `/detach` only removes the topic binding; the workspace remains on disk.
- Rebinding changes future routing only.
- Old Telegram messages remain in Telegram and are not automatically imported into a newly bound context.

## Telegram Command Scopes

- Bot commands are registered by Telegram scope, not by topic.
- This control plane registers commands for `default`, `all_private_chats`, and `all_group_chats`.
- It can also register `chat_member` scope for the control supergroup and the allowed user when the control chat id is configured or safely inferred.
- Group topics do not have their own separate command scopes.
- Telegram clients may still fail to visually show slash suggestions in some group or topic contexts even when commands are registered correctly.
- `/synccommands` re-registers the scopes on demand.
- `/showcommands` fetches the currently registered commands by scope from Telegram.

## Transport choice

Recommended phase-1 automation path:

- run `sshd` on the remote worker
- connect from the control host with normal OpenSSH over the Tailscale network
- authenticate with a normal SSH key or other non-interactive SSH auth

`tailscale-ssh` remains available as an optional transport, but it is not the default automation path.

## Docs

- [docs/architecture.md](./docs/architecture.md)
- [docs/operations.md](./docs/operations.md)
- [docs/fresh-machine-bootstrap.md](./docs/fresh-machine-bootstrap.md)
- [docs/publication-readiness.md](./docs/publication-readiness.md)
- [NEXT_STEPS.md](./NEXT_STEPS.md)
