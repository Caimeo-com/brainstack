# Security Model

## Network

`braind` binds to `127.0.0.1` by default. Tailscale Serve exposes the local port as a tailnet HTTPS endpoint. Funnel is not used.

Normal OpenSSH over Tailscale is the default worker transport. Tailscale SSH is disabled by default.

OpenSSH worker trust is pinned by default after bootstrap. `brainctl trust-worker --config ~/.config/brainstack/brainstack.yaml --worker <name>` records the worker host key in Brainstack's product known-hosts file, and telemux dispatch then uses `StrictHostKeyChecking=yes`. `sshTrustMode: accept-new` exists only as an explicit bootstrap/canary escape hatch: `brainctl doctor --workers` refuses remote probes unless `BRAINSTACK_ALLOW_ACCEPT_NEW_DOCTOR=true`, and telemux dispatch refuses runs unless `BRAINSTACK_ALLOW_ACCEPT_NEW_DISPATCH=true`. Switch back to `sshTrustMode: pinned` before treating the worker as enrolled.

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
      "src": ["group:brain-admins", "autogroup:admin"],
      "dst": ["tag:brain-worker"],
      "ip": ["tcp:22", "icmp:*"]
    },
    {
      "src": ["tag:brain"],
      "dst": ["tag:brain-worker"],
      "ip": ["tcp:22", "icmp:*"]
    },
    {
      "src": ["tag:brain-worker"],
      "dst": ["tag:brain"],
      "ip": ["tcp:22", "tcp:443", "icmp:*"]
    }
  ],
  "ssh": [],
  "nodeAttrs": []
}
```

This means operators can reach control and worker hosts directly, control hosts can SSH to workers, and workers can reach the control host for shared-brain Git freshness plus HTTPS/API access. Grants are directional; the reverse path must be listed separately when needed.

## Tokens

- `BRAIN_IMPORT_TOKEN`: client import/propose scope.
- `BRAIN_ADMIN_TOKEN`: organizer ingest/lint scope; do not copy to clients or workers.
- `FACTORY_TELEGRAM_BOT_TOKEN`: local telemux env only.

Examples must leave secret values blank. `brainctl rotate-token` writes generated shared-brain tokens to secrets env files without printing token values.

Runtime env files are generated and may be overwritten by upgrade. Secrets env files are operator-managed and must not be overwritten by upgrade.

Offline outbox files store pending import/propose payloads under the local state root. They are not secrets by design, but they may contain sensitive note text or proposal bodies. Treat the state root as private user data and do not sync it through public storage.

Import/propose writes use idempotency records in `derived/idempotency/`. If a process dies after side effects may have started and the running lease later expires, the record moves to `review_required` and returns a non-retryable client error. Operators must inspect the record and repository state before retrying with a new idempotency key; clients should not replay that write forever.

Outbox flushes also stop retrying persistent `425` in-progress responses after `BRAINSTACK_OUTBOX_MAX_425_RETRIES` attempts, defaulting to 12, and keep the queued item as a terminal operator-review artifact.

## Import Guardrails

`braind` keeps originals but rejects risky imports before storing them:

- `BRAIN_MAX_IMPORT_BYTES` limits text, uploads, and URL fetch bodies.
- `BRAIN_IMPORT_PREPARATION_CONCURRENCY` separately caps concurrent URL/body preparation so slow URL imports cannot consume unbounded sockets while ordinary text writes still reach the mutation queue.
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

Worker harness resolution is intentionally per-worker: context override, worker default, then global default. Remote workers resolve their harness binary through the worker's own `PATH` by default. This avoids accidentally executing a path discovered on the control host.

Before enabling telemux on a control host, read [`operator-preflight.md`](./operator-preflight.md).

## Token Compromise Impact

A leaked Telegram bot token should be treated as compromised even after local log cleanup. Anyone with the token can call Telegram Bot API methods for that bot. They cannot forge Telegram's `from.id` for a real user message, so the app-level `FACTORY_ALLOWED_TELEGRAM_USER_ID` still blocks direct command injection through normal updates. The token still allows meaningful abuse: reading or racing future updates, sending messages as the bot, changing bot commands, disrupting polling, fetching bot-accessible files when file ids are known, and social-engineering users inside the bot's chats.

Rotate via BotFather after any log leak because logs, backups, terminal scrollback, journal files, and model transcripts can have copies that local cleanup cannot prove were never read.

## Artifact Delivery Guardrail

Telemux only delivers requested artifacts from the active workspace by default. `.factory/TELEGRAM_ATTACHMENTS.json` and `.factory/ARTIFACTS.md` should record relative paths inside the workspace. Absolute paths such as `/etc/passwd` are rejected unless `FACTORY_ALLOW_ABSOLUTE_ARTIFACT_PATHS=true` is explicitly set, which should be treated as an exfiltration-risk override for trusted local debugging only.

`.factory/ARTIFACTS.md` is the topic-local send allowlist and user-facing artifact history. New deliverables should be appended so the latest entry is the default target for generic Telegram requests such as "send artifact", "send file", or "send it". Sending a file does not remove the entry. Remove or mark an entry superseded only when the file was deleted, replaced by a newer deliverable, became misleading, or should no longer be offered for Telegram delivery. Internal state files such as `.factory/STATE.json`, `.factory/SUMMARY.md`, and `.factory/TODO.md` should stay out of the artifact list unless the operator explicitly asks for them as deliverables.

`/shred` is the operator cleanup path for artifacts. It deletes only regular files addressed by relative artifact paths inside the active context workspace, then removes matching lines from `.factory/ARTIFACTS.md` and updates the context cache. Absolute paths, home-relative paths, parent traversal, directories, and paths escaping the workspace through symlinks are rejected. Despite the name, this is an unlink/delete operation, not a guaranteed secure overwrite on SSDs or journaled filesystems.

## Private Journal Boundary

Personal/private journaling should use a separate repo/service/token boundary:

- Shared dev brain: collaborative development canon.
- Private brain: personal journal/private memory.

Do not co-mingle private journaling secrets or content into the shared dev brain.

Local project context can include multiple explicitly configured brains for search and `remember`, but it is an operator convenience layer, not a hard private-journal security boundary. Automatic private-journal provisioning and policy routing are not implemented yet. Until that exists, run private journaling through an explicit separate Brainstack install/config and keep its tokens, repo paths, and Telegram topics separate from the shared dev brain.
