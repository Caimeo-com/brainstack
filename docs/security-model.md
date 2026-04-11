# Security Model

## Network

`braind` binds to `127.0.0.1` by default. Tailscale Serve exposes the local port as a tailnet HTTPS endpoint. Funnel is not used.

Normal OpenSSH over Tailscale is the default worker transport. Tailscale SSH is disabled by default.

## Tailscale Policy Shape

Use grants:

```json
{
  "tagOwners": {
    "tag:brain": ["group:brain-admins"],
    "tag:brain-worker": ["group:brain-admins"]
  },
  "grants": [
    {
      "src": ["group:brain-admins", "autogroup:admin"],
      "dst": ["tag:brain"],
      "ip": ["tcp:22", "tcp:443", "icmp:*"]
    },
    {
      "src": ["tag:brain"],
      "dst": ["tag:brain-worker"],
      "ip": ["tcp:22", "icmp:*"]
    },
    {
      "src": ["tag:brain-worker"],
      "dst": ["tag:brain"],
      "ip": ["tcp:443", "icmp:*"]
    }
  ],
  "ssh": [],
  "nodeAttrs": []
}
```

## Tokens

- `BRAIN_IMPORT_TOKEN`: client import/propose scope.
- `BRAIN_ADMIN_TOKEN`: organizer ingest/lint scope; do not copy to clients or workers.
- `FACTORY_TELEGRAM_BOT_TOKEN`: local telemux env only.

Examples must leave secret values blank. `brainctl rotate-token` writes generated shared-brain tokens to secrets env files without printing token values.

Runtime env files are generated and may be overwritten by upgrade. Secrets env files are operator-managed and must not be overwritten by upgrade.

## Import Guardrails

`braind` keeps originals but rejects risky imports before storing them:

- `BRAIN_MAX_IMPORT_BYTES` limits text, uploads, and URL fetch bodies.
- URL response bodies are capped while streaming; the service does not rely on `Content-Length`.
- `BRAIN_URL_FETCH_TIMEOUT_MS` caps URL fetch/header/body read time.
- URL imports only allow `http` and `https`.
- URL imports block loopback, RFC1918, link-local, carrier-grade NAT/Tailscale, and unique-local/private IPv6 addresses by default.
- `BRAIN_ALLOW_PRIVATE_URL_IMPORTS=true` exists only for trusted admin-controlled private fetches and should stay false for normal deployments.

These checks reduce server-side request forgery risk. They do not make arbitrary URL fetching safe enough for public internet exposure.

## Telegram Logging

`apps/telemux/src/telegram.ts` redacts Telegram bot-token-shaped strings from fetch/network error messages before logging. Historical logs may still contain old token material and require manual token rotation.

## Harness Execution Risk

Telemux passes authorized Telegram-topic prompts and staged files to the configured harness process, either Codex CLI or Claude Code. It does not sandbox the harness. If the harness is configured with bypass-all-permissions/yolo settings and the Unix user has passwordless sudo, telemux can indirectly execute privileged commands as that user.

Before enabling telemux on a control host, read [`operator-preflight.md`](./operator-preflight.md).

## Token Compromise Impact

A leaked Telegram bot token should be treated as compromised even after local log cleanup. Anyone with the token can call Telegram Bot API methods for that bot. They cannot forge Telegram's `from.id` for a real user message, so the app-level `FACTORY_ALLOWED_TELEGRAM_USER_ID` still blocks direct command injection through normal updates. The token still allows meaningful abuse: reading or racing future updates, sending messages as the bot, changing bot commands, disrupting polling, fetching bot-accessible files when file ids are known, and social-engineering users inside the bot's chats.

Rotate via BotFather after any log leak because logs, backups, terminal scrollback, journal files, and model transcripts can have copies that local cleanup cannot prove were never read.

## Artifact Delivery Guardrail

Telemux only delivers requested artifacts from the active workspace by default. `.factory/TELEGRAM_ATTACHMENTS.json` and `.factory/ARTIFACTS.md` should record relative paths inside the workspace. Absolute paths such as `/etc/passwd` are rejected unless `FACTORY_ALLOW_ABSOLUTE_ARTIFACT_PATHS=true` is explicitly set, which should be treated as an exfiltration-risk override for trusted local debugging only.

## Private Journal Boundary

Personal/private journaling is a separate profile/repo/service/token boundary:

- Shared dev brain: collaborative development canon.
- Private brain: personal journal/private memory.

Do not co-mingle private journaling secrets or content into the shared dev brain.
