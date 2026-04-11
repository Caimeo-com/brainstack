# Architecture

Brainstack product note: fresh installs use home-directory state roots by default. Original `/srv/telemux` and `/srv/factory` paths in this vendored document describe the legacy valkyrie deployment unless overridden by `FACTORY_CONTROL_ROOT` and `FACTORY_FACTORY_ROOT`.

## Purpose

The control host runs a small Bun service that acts as the control plane for Telegram-driven Codex sessions. It must remain useful even when an optional remote worker is still offline or only partially bootstrapped.

## Components

### Telegram long poller

- Uses `getUpdates` with persisted update offsets in SQLite.
- Accepts commands from the allowed Telegram user id.
- Uses `message_thread_id` to map one Telegram topic to one durable context.
- Sends plain text replies with `sendMessage` and uploads files back into the same topic with `sendDocument`/`sendPhoto` when explicitly requested.
- Treats plain text and captions in a bound topic as conversational Codex input:
  start a new session if none exists, otherwise resume the stored session id.
- Downloads supported inbound Telegram media with `getFile`, stages it into `.factory/inbox/telegram/<message_id>/` inside the bound workspace, and passes staged image files through to Codex with `--image`.
- Rejects audio/voice-only inbound messages before Codex for now; transcription is a later phase.

### SQLite registry

`/srv/telemux/db.sqlite` is the authoritative control-plane registry.

Each context stores at least:

- slug
- Telegram chat id and thread id
- machine
- kind: `repo`, `host`, or `scratch`
- state: `active`, `pending`, `archived`, or `error`
- transport
- target
- root path
- worktree path
- optional branch/base branch
- Codex session id
- optional Codex model override
- optional Codex reasoning-effort override
- last run time
- last summary
- last error
- local log path

Each worker health record stores:

- machine name
- configured transport
- reachability
- local execution availability
- SSH target/user when relevant
- last checked and last seen timestamps
- last error/details

Each cron job stores at least:

- id and human label
- kind: `reminder` or `codex`
- enabled/paused state
- schedule JSON plus computed `next_run_at`
- optional `pending_run_at` when a due Codex run is deferred because the target context is busy
- execution context slug
- Telegram chat id and thread id for proactive delivery
- optional cron-local model override
- optional cron-local reasoning-effort override
- last result/error and timestamps

### Inspectable snapshots

The DB is authoritative, but each context is also mirrored to:

- `/srv/telemux/contexts/<slug>/context.json`

That keeps the live binding state easy to inspect without opening SQLite first.

Cron jobs are also mirrored to:

- `/srv/telemux/crons/<job-id>.json`

### Worker transport

Transport is a narrow abstraction:

- `local`
- `ssh`
- `tailscale-ssh`

Recommended phase-1 remote path is plain OpenSSH over the Tailscale network. `tailscale-ssh` stays optional, not mandatory.

### Context bootstrap

`/newctx` and `/bind` resolve the target into one of three kinds:

- `repo`
  existing repo path or managed clone from a git URL
- `host`
  machine/admin context backed by a managed lightweight git repo
- `scratch`
  throwaway or semi-throwaway context backed by a managed lightweight git repo

Managed paths default to:

- `/srv/factory/repos/<slug>`
- `/srv/factory/hostctx/<slug>`
- `/srv/factory/scratch/<slug>`

For `host` and `scratch`, the bootstrap step creates:

- `.git/`
- `AGENTS.md`
- `.factory/STATE.json`
- `.factory/SUMMARY.md`
- `.factory/TODO.md`
- `.factory/ARTIFACTS.md`

### Pending model

If a remote worker is unavailable, context creation does not hard-fail. The context is stored as `pending` with the planned target paths and the real SSH error. That keeps Telegram topic binding and later reconciliation simple.

### Dispatcher

`/run`, `/resume`, `/loop`, and bound-topic plain text all use the same flow:

1. Ensure the context workspace exists, or keep it `pending` if the worker is unreachable.
2. If the triggering Telegram message included supported media, download it and stage it into `.factory/inbox/telegram/<message_id>/` in the workspace.
3. Run the configured harness in the workspace. Codex uses `codex exec` or `codex exec resume <session_id>` with image/model/effort flags; Claude uses `claude -p` with bypass-permissions flags and text/model/effort overrides.
4. The worker launcher monitors the bound worktree during the run and aborts cleanly if that worktree disappears, so a self-deleting task cannot wedge the topic forever.
5. Capture the current session id.
6. Read `.factory/SUMMARY.md`, `.factory/ARTIFACTS.md`, and `.factory/last-message.txt`.
7. Read the optional ephemeral Telegram upload manifest `.factory/TELEGRAM_ATTACHMENTS.json`.
8. Read the optional ephemeral cron-change manifest `.factory/CRON_REQUESTS.json`.
9. Persist the new session id, summary, error, and timestamps.
10. Apply any validated cron actions requested by the manifest.
11. While the job is active, emit a `sendChatAction(typing)` heartbeat into the same Telegram topic.
12. Post the concise result back into the same Telegram topic.
13. If the manifest requested attachments, fetch the recorded files from the worker workspace and upload them into the same Telegram topic.
14. If shared-brain import env is configured, import `SUMMARY.md` and `ARTIFACTS.md` to `braind` as raw artifacts.

### Internal scheduler

Scheduled jobs are handled by the same Bun daemon, not by OS cron:

1. The scheduler wakes up on a fixed interval.
2. On startup, it fast-forwards missed jobs to their next future occurrence instead of replaying downtime.
3. It checks the SQLite job table deterministically for due work.
4. Reminder jobs send a direct Telegram message.
5. Codex jobs dispatch a normal `resume` run against the stored execution context, optionally with cron-local model/effort overrides.
6. If a Codex job becomes due while the target context already has an active run, the scheduler stores one pending run instead of starting a second concurrent Codex session on the same context.

### Dashboard

The dashboard is a small Bun-served localhost page on `127.0.0.1:<port>` showing:

- worker reachability and transport
- context kind/state/topic binding
- worktree path
- stored session id
- latest summary snippet
- cron jobs with next run, target topic, and execution context

## Failure model

- A remote worker can be fully absent and the control host still works for local `host` and `scratch` topics.
- Worker unreachability becomes `pending`, not a crash.
- Real command errors are sent back to Telegram instead of abstract placeholder failures.
- Attachment upload is best-effort. The text reply still posts even if a requested file upload fails.
- Cron delivery is best-effort. Due reminders or scheduled Codex runs update job state and run history even if Telegram delivery or dispatch fails.
- The transport layer stays small so it can be swapped later without rewriting the bot logic.
