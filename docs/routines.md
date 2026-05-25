# Routines

Brainstack routines are telemux scheduled jobs. They live in the telemux SQLite registry and are mirrored into the bound context workspace as `.factory/CRONS.md`.

Use routines for periodic operator workflows such as update checks, shared-brain curation, and daily check-ins. They are not OS cron jobs.

When telemux starts with `FACTORY_TELEGRAM_CONTROL_CHAT_ID` configured, it idempotently creates a `brainstack-routines` scratch context and installs the deterministic `update-check` routine. If the control chat id is not configured, no automatic routine is created and `/cron install update-check` remains the manual path.

## Commands

List jobs linked to the current topic or context:

```text
/crons
```

The list includes tap-friendly shortcuts:

```text
/cron_show_<token>_1
/cron_run_<token>_1
/cron_pause_<token>_1
/cron_resume_<token>_1
```

The token is a short-lived snapshot of the job list shown by `/crons`. Refresh `/crons` before using an older shortcut so list reordering cannot target the wrong job.

Deletion is intentionally explicit: use `/cron delete <id-or-label>` after checking the job.

Run a job immediately by id or label:

```text
/cron_run update-check
/cron run update-check
```

`/updates` is the same deterministic stack-wide check as the built-in `update-check` routine. It prefers the auto-created active `brainstack-routines` context for report artifacts so update visibility still works from worker-specific topics; if that context is unavailable, it falls back to the current active bound topic.

Create a deterministic reminder:

```text
/cron create reminder standup daily 09:00 Europe/Zagreb Write your standup.
```

Create a deterministic harness job:

```text
/cron create codex repo-sweep weekly monday 08:30 Europe/Zagreb Inspect the repo and summarize risks.
```

Supported schedules:

- `once <ISO timestamp>`
- `daily <HH:MM> <Timezone>`
- `weekly <weekday> <HH:MM> <Timezone>`
- `monthly <day> <HH:MM> <Timezone>`
- `interval <minutes> [anchor ISO timestamp]`

Use an IANA timezone such as `Europe/Zagreb`, `America/New_York`, or `UTC`.

Codex jobs require the Telegram topic to be bound to a Brainstack context, because the run needs a durable workspace and session. Codex interval jobs must be at least 15 minutes apart. Reminder jobs can post to a topic, but the built-in routines require a bound context so replies and artifacts have somewhere to land.

## Built-ins

Install a built-in in the current bound topic:

```text
/cron install update-check
/cron install brain-curator
/cron install daily-checkin
```

Shortcut forms:

```text
/cron_install_update_check
/cron_install_brain_curator
/cron_install_daily_checkin
```

You can override the default schedule:

```text
/cron install update-check weekly monday 09:00 Europe/Zagreb
/cron install brain-curator daily 06:30 Europe/Zagreb
/cron install daily-checkin daily 09:00 Europe/Zagreb
```

### `update-check`

Runs a deterministic read-only update report without invoking an LLM harness. Telemux runs Brainstack's `brainctl updates` path on every configured worker when available, falls back to safe local probes when the product checkout/config is missing on a worker, writes one stack report artifact, records it in `.factory/ARTIFACTS.md`, and posts a concise Telegram summary. A partially unreachable stack is reported as degraded; an all-machine probe failure fails the routine.

The report checks Brainstack git state, Codex/Claude versions and compatibility, and supported OS package managers where available:

- Homebrew: `HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_ENV_HINTS=1 brew outdated --quiet`
- Arch/Omarchy: `pacman -Qu`, plus `checkupdates` when installed
- Debian/Ubuntu: `apt list --upgradable`
- Fedora/RHEL: `dnf --cacheonly check-update --quiet`
- openSUSE: `zypper --no-refresh list-updates`

The routine does not install, upgrade, remove, reboot, restart, or mutate packages/services. It reports manual commands only.

`FACTORY_WORKERS_FILE` is re-read by telemux when worker state is refreshed. After `brainctl join-worker` plus `brainctl upgrade` updates `workers.json`, the next update-check includes the new machine without additional routine setup. If the worker file becomes malformed or disappears after telemux has loaded it, worker dispatch fails closed until the file is fixed.

### `brain-curator`

Runs a shared-brain curator pass. It preserves raw imports and proposals, reviews recent raw/proposal/log material, writes a sourced curation report, and submits proposals when the local `brainctl propose` path is configured.

This routine should not mix private-journal material into the shared dev brain.

### `daily-checkin`

Sends a daily Telegram check-in prompt. When you reply in a bound topic, the normal telemux context flow handles the response. If shared-brain run-summary imports are configured, the reply can flow back into the shared brain through the existing import path.

## Scheduler Health

`/crons` shows job state inside Telegram.

`brainctl doctor --config ~/.config/brainstack/brainstack.yaml` checks telemux health and reports scheduled-job counts from `/healthz`:

- total jobs
- enabled jobs
- pending jobs
- currently due jobs

A nonzero due or pending count is a warning, not always a failure. Inspect `/crons` and telemux logs if it does not clear after the scheduler interval.
