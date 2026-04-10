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

Examples must leave secret values blank. `brainctl rotate-token` writes generated shared-brain tokens to env files without printing token values.

## Telegram Logging

`apps/telemux/src/telegram.ts` redacts Telegram bot-token-shaped strings from fetch/network error messages before logging. Historical logs may still contain old token material and require manual token rotation.

## Private Journal Boundary

Personal/private journaling is a separate profile/repo/service/token boundary:

- Shared dev brain: collaborative development canon.
- Private brain: personal journal/private memory.

Do not co-mingle private journaling secrets or content into the shared dev brain.

