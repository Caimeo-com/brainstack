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
- Treat Brainstack as the shared-memory/control product first. macOS install smoothness, Telegram, and worker delegation are supporting surfaces, not the core value proposition.

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

## Installing This Skill Bundle

Public Brainstack skills are versioned product artifacts under `packages/skills`; they are not shared-brain content. Codex invite enrollment installs the client skill profile automatically. Use these commands to repair, refresh, or deliberately switch bundles:

```bash
brainctl skills install --target codex --profile client
brainctl skills install --target codex --profile operator
```

Use `client` for ordinary enrolled machines and `operator` for admins who operate control hosts, workers, curation, and recovery. `control` is the control-host subset, `worker` is the worker/client subset, and invite enrollment can use `none` or installer `--skip-skills` when no Codex skills should be written. Create operator-machine invites with `brainctl invite create --skills-profile operator` when the installer should lay down the operator bundle during enrollment. Use `--skill NAME` for explicit installs, `--all` for every public skill, `--dir DIR` for a custom Codex skills root, and `--dry-run` before writing.

Keep public skills generic. Private topology, exact hostnames, operator usernames, service paths, Telegram chat ids, and local runbook exceptions belong in a private overlay skill outside `packages/skills`.

## Install And Enrollment Flow

- Control hosts are source-run today: clone Brainstack, install Bun deps, run `brainctl provision`, inspect config, run `brainctl init`, enable user services, and verify with `brainctl doctor`.
- Client machines should use invites when available: the control host runs `brainctl invite create`, the client runs the release installer, pastes the private invite, and `brainctl enroll` writes config, bootstrap guidance, optional tokens, SSH pins, Codex skills for Codex invites, and doctor output.
- Token-bearing invites are bearer secrets. Prefer private files or prompts over putting `bs1_...` values in shell history, chat logs, process argv, or issue trackers.
- The public installer is only a downloader/checksum shim. Setup policy stays in `brainctl enroll` so retries and audits use versioned product code.
- Standalone client binaries must embed client bootstrap templates and portable skills. Do not add source-tree-relative reads for assets required by released binaries.

## Control Host Health

Prefer this order:

1. Check Tailscale reachability from the current machine: `tailscale status`, `tailscale ping <host>`, and `tailscale whois <tailscale-ip>`.
2. Check normal OpenSSH over Tailscale: `ssh -o BatchMode=yes -o ConnectTimeout=8 <user>@<host> true`.
3. On the control host, run `brainctl doctor --config "$BRAINSTACK_CONFIG" --workers`; use `--deep` only when intentionally proving yolo/bypass sudo through the harness.
4. Check services: `systemctl --user status braind.service telemux.service --no-pager`, `journalctl --user -u telemux.service -n 200 --no-pager`, and `journalctl --user -u braind.service -n 200 --no-pager`.
5. Check local health endpoints on the host: `curl -fsS http://127.0.0.1:8080/healthz` for public braind liveness, `curl -H "Authorization: Bearer $BRAIN_ADMIN_TOKEN" http://127.0.0.1:8080/admin/health` for deep braind diagnostics, and `curl -fsS http://127.0.0.1:8787/healthz` for telemux.
6. Check stack update visibility with `/updates` in Telegram or `brainctl updates --config "$BRAINSTACK_CONFIG"` on a host. Do not auto-apply updates.
7. Use `brainctl doctor --workers --config "$BRAINSTACK_CONFIG"` to verify each worker's shell PATH exposes Bun, Git, SSH, Tailscale, and the configured harness.
8. On installed client/worker profiles, use `brainctl doctor --write-smoke --config "$BRAINSTACK_CONFIG"` only when explicitly proving import/propose pushback; it posts a small import artifact.

Brainstack's intended worker transport is normal OpenSSH over Tailscale, not Tailscale SSH. Use Tailscale SSH only as a temporary recovery/debug path when normal OpenSSH access is blocked.

## Telemux Debugging

When Telegram stops responding:

- Prove whether the bot is configured without printing the token: call `getMe` and report bot id/username only.
- Check `getWebhookInfo`. Brainstack telemux uses polling, so `url` should be empty. A nonzero `pending_update_count` suggests updates are not being consumed.
- Avoid `getUpdates` unless the user asked for live Telegram debugging or service state suggests no poller is active. If used, summarize only metadata: update id, chat id, thread id, sender id, date, text length, and attachment kinds.
- Inspect `telemux.service` and recent logs on the control host. Look for startup crashes, Telegram 409 polling conflicts, missing `FACTORY_TELEGRAM_BOT_TOKEN`, wrong `FACTORY_ALLOWED_TELEGRAM_USER_ID`, bot privacy issues, DB write errors, stuck active context jobs, and worker SSH failures.
- Check `/whoami`, `/workers`, `/updates`, `/topicinfo`, `/context`, `/usage`, and `/tail` from the bound Telegram topic once the service is consuming updates again. `/workers` should show each worker's harness, model, thinking effort, and `sudo=ok|fail|missing|n/a`; `/updates` should run the same all-worker deterministic update-check used by the built-in routine.
- Bound-topic plain text is pre-dispatch routed before the harness starts. Bare liveness/status/usage/latency questions should be answered locally by telemux, short informational questions should use the lightweight wrapper, and attachments, file/code/machine work, scheduling, long messages, and ambiguity should use the full durable-work wrapper. Use `/run` or `/resume` to force full work.
- If a simple probe starts an expensive full session, inspect route logs for `pre-dispatch route ...` without raw message text, confirm the deployed code is current, and check only the names/presence of `FACTORY_PRE_DISPATCH_CLASSIFIER*` runtime env values. The optional LLM classifier is disabled by default, must use `FACTORY_PRE_DISPATCH_CLASSIFIER_API_KEY` rather than ambient provider keys, and should fail open to full work on errors, low confidence, attachments, or risky intent.
- Normal Telegram topic resumes should not emit "Dispatched resume" acknowledgements. Expect the typing heartbeat, optional `Compacting thread…`, and then the final result. Use `/compact` only for Codex-backed contexts with an existing session; Claude should report that manual compact is unsupported.
- Restart `telemux.service` only after recording the current failure evidence, unless the user explicitly requests a blind restart.

## Telegram File Relay

Use `brainctl telegram send-file` when an enrolled machine needs to send a local file to the operator through Telegram:

```bash
brainctl telegram send-file --config "$BRAINSTACK_CONFIG" --file ./artifact.zip --caption "artifact from client"
```

Expected shape:

- The client streams bytes over the configured control-host SSH path.
- The control host runs Brainstack's telemux send-file helper from its local product repo.
- Telegram bot tokens and chat defaults stay on the control host in telemux env files; clients should not store bot tokens.
- Invites can embed the control SSH target, remote product repo, and pinned known-hosts data so routine sends do not need ad hoc `scp`, bot tokens, or `accept-new` trust.
- Local and remote guards should reject symlinks, directories, oversize files, and source or display names that look like secrets unless the user explicitly passes `--allow-sensitive`.
- Prefer context slugs for bound telemux topics when available; otherwise use the configured control chat defaults. Do not print chat ids unless the user asks for routing diagnostics.

## Worker Canary

Use a non-destructive canary before repurposing a reused worker:

1. Prove direct operator access to the worker over normal OpenSSH.
2. Prove worker prerequisites: `bun`, `git`, `ssh`, `tailscale`, chosen `codex` or `claude`, and `sudo -n true` if privileged work is expected.
3. Prove worker-to-control Git freshness with `git ls-remote <control-user>@<control-host>:<shared-brain-bare-repo> HEAD`.
4. Add or verify the worker in control config with a worker-specific harness override when needed.
5. Pin OpenSSH host trust from the control host with `brainctl trust-worker --config "$BRAINSTACK_CONFIG" --worker <worker>`; `sshTrustMode: accept-new` is bootstrap-only and should not be the steady state.
6. Run `brainctl doctor --workers`; run `--deep` once deliberately after harness auth is known good.
7. Send a boring Telegram worker task that creates a scratch artifact with hostname, user, uname, and current repo commit.
8. Inventory legacy services and dotfiles before deleting anything.

The required grant shape is directional: operator to control/worker, control to worker on `tcp:22`, and worker to control on `tcp:22` plus `tcp:443`.

## Shared-Brain Writes

- Before substantial work in a repository, run `brainctl context --repo .` and follow the returned Brainstack instructions.
- Read freshness and source labels should come through `brainctl search --repo . "query"` instead of hand-rolled clone/pull logic.
- Write continuity should come through `brainctl remember --repo . --summary "..."`, or the lower-level `brainctl import-text` and `brainctl propose` commands when project context is not available.
- Do not manually POST to Brainstack endpoints unless explicitly instructed.
- If a write reports idempotency `review_required`, inspect the matching record under `derived/idempotency/` and repo state before retrying with a new key; do not force replay an ambiguous side effect.
- Client bootstrap can receive `BRAIN_IMPORT_TOKEN` or `BRAIN_IMPORT_TOKEN_FILE`; it fills the local token slot only when blank and never prints the value.
- If the brain is unreachable, use `brainctl outbox status|list|flush|purge|purge-corrupt`. Outbox files may contain sensitive note text and should stay in private local state.
- Keep `BRAIN_ADMIN_TOKEN` on organizer/control hosts only. Clients and workers should use import/propose scope.

## Repo Lock Recovery

- `braind` does not auto-break `.shared-brain.lock`.
- Use `brainctl repo-lock status --config "$BRAINSTACK_CONFIG"` to inspect owner metadata.
- Clear only after proving no write is active and copying the reported clear token: `brainctl repo-lock clear --config "$BRAINSTACK_CONFIG" --yes --token <clear_token>`.
- For a stuck idempotency lock, use `brainctl locks status --config "$BRAINSTACK_CONFIG" --path <lock-dir>` and clear with the same `--path` plus `--token <clear_token>`.
- If the owner process is still live, unknown, or on another host, `repo-lock clear` should refuse unless `--force` is supplied after manual confirmation.
- If status reports `clear_token=EMPTY`, use `--force --token EMPTY` only after proving no write is active; this is for interrupted empty lock directories, not normal lock cleanup.

## Handoff Bundles

- Use `scripts/handoff.sh --mode review` for reviewer-minimal bundles and `--mode forensic` only when a larger audit trail is requested.
- Review mode records a focused proof set; use `--full-test` or forensic mode when the bundle itself must include the full `bun test` gate.
- Prefer a clean tree. If intentional untracked source files must be reviewed, use `--allow-dirty`, add explicit notes, and verify the archive includes them.
- Include a pass-specific notes file with the purpose, evidence, blocked items, and residual risk.
- Verify archive contents, not just the output path. Check that `.git`, env files, dependency trees, caches, private keys, tokens, and local scratch artifacts are absent.

## Change Discipline

- Keep fixes narrow and boring.
- Update docs/examples when generated behavior changes.
- Add focused tests for CLI, telemux, worker, outbox, or braind behavior that changed.
- Run `bun test` before finalizing unless a real blocker prevents it.
- For control-host changes, distinguish product repo edits from live production service changes in the final report.
