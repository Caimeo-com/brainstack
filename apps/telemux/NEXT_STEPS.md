# Final Manual Steps

## For a fresh control host

1. Copy `.env.example` to `.env` if you have not already.
2. Copy `workers.example.json` to `workers.json`.
3. Set `FACTORY_TELEGRAM_BOT_TOKEN` in a local env file. Do not paste the real value into docs, git commits, or issue text.
4. Add the bot to the target Telegram supergroup and make sure forum topics are enabled.
5. Disable bot privacy in `@BotFather` so plain text inside a bound topic is visible.
6. Run:

```bash
./scripts/install.sh
./scripts/doctor.sh
```

Run those as your control user, not with `sudo`. `install.sh` now prompts for `sudo` internally only for the privileged `/srv` and linger setup, and keeps Bun-related work under that user account.

Only set `FACTORY_WORKERS_FILE` in `.env` if you want the worker config somewhere other than `./workers.json`.

7. In Telegram, send `/whoami` in the exact topic you want to use.
8. Create the first local host context:

```text
/newctx control-general control host
```

9. Optional: create a scratch topic:

```text
/newctx scratchpad control scratch
```

At that point, the control host is usable even if the remote worker is still offline.

Once a topic is active, artifact files can be returned into the same Telegram topic either by asking for them in plain language or by using `/artifacts send [filter]`.

Supported inbound Telegram media now stages into the bound workspace automatically. Voice and audio transcription is still a later phase.

Topic-local Codex runtime switching is also available now through `/mode`, `/model`, and `/effort`.

Scheduled jobs are now available through `/crons`, `/cron ...`, and natural-language cron requests emitted by Codex via `.factory/CRON_REQUESTS.json`.

## Manual remote-worker work for later

These are the remaining manual prerequisites before remote repo execution on the remote worker will work cleanly:

1. Enable `sshd` on the remote worker.
2. Decide on the remote SSH user and update `workers.json`.
3. Trust the control host SSH key on the remote worker.
4. Confirm Tailscale networking between the control host and the remote worker.
5. Confirm `git`, `bash`, and `codex` are installed on the remote worker.
6. Create a pending remote host topic when you are ready:

```text
/newctx worker-general worker1 host
```

7. For a repo topic, either bind an existing path or give a git URL:

```text
/newctx myproj worker1 https://github.com/example/project.git master
```

If the remote worker is still unavailable, those contexts should remain `pending` instead of failing hard.
