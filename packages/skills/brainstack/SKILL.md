---
name: brainstack
description: Operate, debug, run, improve, and audit Brainstack control hosts, workers, braind, telemux, shared-brain clients, and handoff bundles.
---

# Brainstack

Use this skill when working on the Brainstack product repo, a Brainstack control host, a shared-brain client, or an OpenSSH-over-Tailscale worker.

## First Moves

- Read the repo `AGENTS.md`, then inspect `README.md` and the relevant docs under `docs/`.
- Use Bun for Brainstack product commands: `bun install --frozen-lockfile`, `bun test`, and `bun run packages/brainctl/src/main.ts ...`.
- Keep product code separate from shared-brain content. Product code lives in `apps/`, `packages/`, `infra/`, `examples/`, `docs/`, and `scripts/`.
- Do not install Bun, Git, OpenSSH, Tailscale, Codex, or Claude from Brainstack commands unless the user explicitly asks. Brainstack checks prerequisites and prints install hints.
- Never print tokens, SSH private keys, Telegram bot tokens, or real env file values. Report presence, path, shape, and health only.

## Local Values

Expect machine-specific values to come from local env, shell config, SSH config, or the user's explicit instructions. Useful optional env names:

- `BRAINSTACK_PRODUCT_ROOT`: product repo path, normally `~/brainstack`.
- `BRAINSTACK_CONFIG`: active config path, normally `~/.config/brainstack/brainstack.yaml`.
- `BRAINSTACK_CONTROL_HOST`: control host, for example `brain-control`.
- `BRAINSTACK_CONTROL_USER`: SSH user for the control host.
- `BRAINSTACK_WORKERS`: comma-separated worker hostnames, for example `brain-worker-a,brain-worker-b`.
- `BRAINSTACK_SSH_OPTS`: extra OpenSSH options for diagnostics.
- `BRAINSTACK_BOT_TOKEN_ENV`: name of the env var that contains the Telegram bot token.

If a required value is missing and cannot be discovered from config, ask the user for that value instead of guessing IPs, users, keys, tokens, or chat ids.

## Control Host Health

Prefer this order:

1. Check Tailscale reachability from the current machine: `tailscale status`, `tailscale ping <host>`, and `tailscale whois <tailscale-ip>`.
2. Check normal OpenSSH over Tailscale: `ssh -o BatchMode=yes -o ConnectTimeout=8 <user>@<host> true`.
3. On the control host, run `brainctl doctor --config "$BRAINSTACK_CONFIG" --workers`; use `--deep` only when intentionally proving yolo/bypass sudo through the harness.
4. Check services: `systemctl --user status braind.service telemux.service --no-pager`, `journalctl --user -u telemux.service -n 200 --no-pager`, and `journalctl --user -u braind.service -n 200 --no-pager`.
5. Check local health endpoints on the host: `curl -fsS http://127.0.0.1:8080/health` for braind and `curl -fsS http://127.0.0.1:8787/healthz` for telemux.
6. Check manual update visibility with `brainctl updates --config "$BRAINSTACK_CONFIG"`. Do not auto-apply updates.
7. Use `brainctl doctor --workers --config "$BRAINSTACK_CONFIG"` to verify each worker's shell PATH exposes Bun, Git, SSH, Tailscale, and the configured harness.
8. On installed client/worker profiles, use `brainctl doctor --write-smoke --config "$BRAINSTACK_CONFIG"` only when explicitly proving import/propose pushback; it posts a small import artifact.

Brainstack's intended worker transport is normal OpenSSH over Tailscale, not Tailscale SSH. Use Tailscale SSH only as a temporary recovery/debug path when normal OpenSSH access is blocked.

## Telemux Debugging

When Telegram stops responding:

- Prove whether the bot is configured without printing the token: call `getMe` and report bot id/username only.
- Check `getWebhookInfo`. Brainstack telemux uses polling, so `url` should be empty. A nonzero `pending_update_count` suggests updates are not being consumed.
- Avoid `getUpdates` unless the user asked for live Telegram debugging or service state suggests no poller is active. If used, summarize only metadata: update id, chat id, thread id, sender id, date, text length, and attachment kinds.
- Inspect `telemux.service` and recent logs on the control host. Look for startup crashes, Telegram 409 polling conflicts, missing `FACTORY_TELEGRAM_BOT_TOKEN`, wrong `FACTORY_ALLOWED_TELEGRAM_USER_ID`, bot privacy issues, DB write errors, stuck active context jobs, and worker SSH failures.
- Check `/whoami`, `/workers`, `/updates`, `/topicinfo`, and `/tail` from the bound Telegram topic once the service is consuming updates again.
- Restart `telemux.service` only after recording the current failure evidence, unless the user explicitly requests a blind restart.

## Worker Canary

Use a non-destructive canary before repurposing a reused worker:

1. Prove direct operator access to the worker over normal OpenSSH.
2. Prove worker prerequisites: `bun`, `git`, `ssh`, `tailscale`, chosen `codex` or `claude`, and `sudo -n true` if privileged work is expected.
3. Prove worker-to-control Git freshness with `git ls-remote <control-user>@<control-host>:<shared-brain-bare-repo> HEAD`.
4. Add or verify the worker in control config with a worker-specific harness override when needed.
5. Run `brainctl doctor --workers`; run `--deep` once deliberately after harness auth is known good.
6. Send a boring Telegram worker task that creates a scratch artifact with hostname, user, uname, and current repo commit.
7. Inventory legacy services and dotfiles before deleting anything.

The required grant shape is directional: operator to control/worker, control to worker on `tcp:22`, and worker to control on `tcp:22` plus `tcp:443`.

## Shared-Brain Writes

- Read freshness comes from local clone sync: `git -C ~/shared-brain pull --ff-only`.
- Write continuity comes from `POST /api/import` and `POST /api/propose`, or `brainctl import-text` and `brainctl propose`.
- Client bootstrap can receive `BRAIN_IMPORT_TOKEN` or `BRAIN_IMPORT_TOKEN_FILE`; it fills the local token slot only when blank and never prints the value.
- If the brain is unreachable, use `brainctl outbox status|list|flush|purge`. Outbox files may contain sensitive note text and should stay in private local state.
- Keep `BRAIN_ADMIN_TOKEN` on organizer/control hosts only. Clients and workers should use import/propose scope.

## Handoff Bundles

- Use `scripts/handoff.sh --mode review` for reviewer-minimal bundles and `--mode forensic` only when a larger audit trail is requested.
- Prefer a clean tree. If intentional untracked source files must be reviewed, add explicit notes and verify the archive includes them.
- Include a pass-specific notes file with the purpose, evidence, blocked items, and residual risk.
- Verify archive contents, not just the output path. Check that `.git`, env files, dependency trees, caches, private keys, tokens, and local scratch artifacts are absent.

## Change Discipline

- Keep fixes narrow and boring.
- Update docs/examples when generated behavior changes.
- Add focused tests for CLI, telemux, worker, outbox, or braind behavior that changed.
- Run `bun test` before finalizing unless a real blocker prevents it.
- For control-host changes, distinguish product repo edits from live production service changes in the final report.
