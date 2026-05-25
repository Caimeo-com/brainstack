# Outbox Security

The outbox is a durability layer for import/propose writes when `braind` is unreachable. It is not encrypted by Brainstack.

Current guarantees:

- outbox directories are created `0700`
- queued item files are written through same-directory temp files and atomic rename
- queued item files are chmodded `0600`
- duplicate logical payloads use a stable idempotency key
- retryable server failures queue instead of being dropped
- corrupt entries are visible in `brainctl outbox status|list`
- `brainctl outbox purge-corrupt --yes` removes corrupt entries explicitly
- large entries are compressed above the configured threshold and refused above the hard cap
- queued content is never silently truncated

Defaults:

- compress above `BRAINSTACK_OUTBOX_COMPRESS_ABOVE_BYTES` or 1 MiB
- warn/document above `BRAINSTACK_OUTBOX_SOFT_WARN_BYTES` or 10 MiB
- refuse above `BRAINSTACK_OUTBOX_HARD_MAX_BYTES` or 250 MiB

Because payloads can contain sensitive prompt/context material, rely on host disk encryption and normal filesystem privacy. Brainstack intentionally does not add fake local encryption without solving key management.

## Future Sealed Outbox Design

Local encryption with the key beside the ciphertext is mostly fake security. A useful future mode for remote worker disks would be server-sealed:

- `braind` publishes an outbox public key.
- clients encrypt queued payloads to that key before writing disk entries.
- clients cannot decrypt those payloads later.
- flush sends ciphertext to a replay endpoint such as `/api/outbox/replay-sealed`.
- `braind` decrypts and internally routes to import/propose.

That design helps when the outbox lives on a remote worker. It does not help when the server key and outbox live under the same host/user, so it is documented here as future work rather than pretending the current local plaintext outbox is encrypted.
