# Routines

Brainstack routines are telemux scheduled jobs. They live in the telemux SQLite registry and are mirrored into the bound context workspace as `.factory/CRONS.md`.

Use routines for periodic operator workflows such as update checks, shared-brain curation, and daily check-ins. They are not OS cron jobs.

## Commands

List jobs linked to the current topic or context:

```text
/crons
```

The list includes tap-friendly shortcuts:

```text
/cron_show_1
/cron_run_1
/cron_pause_1
/cron_resume_1
```

Deletion is intentionally explicit: use `/cron delete <id-or-label>` after checking the job.

Run a job immediately by id or label:

```text
/cron_run update-check
/cron run update-check
```

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

Codex jobs require the Telegram topic to be bound to a Brainstack context, because the run needs a durable workspace and session. Reminder jobs can post to a topic, but the built-in routines require a bound context so replies and artifacts have somewhere to land.

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

Runs a read-only update report. It checks Brainstack git state, Codex/Claude versions and compatibility, and supported OS package managers where available:

- Homebrew: `HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_ENV_HINTS=1 brew outdated --quiet`
- Arch/Omarchy: `pacman -Qu`, plus `checkupdates` when installed
- Debian/Ubuntu: `apt list --upgradable`
- Fedora/RHEL: `dnf --cacheonly check-update --quiet`
- openSUSE: `zypper --no-refresh list-updates`

The routine must not install, upgrade, remove, reboot, restart, or mutate packages/services. It reports manual commands only.

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
