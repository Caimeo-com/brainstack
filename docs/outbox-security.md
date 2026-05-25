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

Defaults:

- compress above `BRAINSTACK_OUTBOX_COMPRESS_ABOVE_BYTES` or 1 MiB
- warn/document above `BRAINSTACK_OUTBOX_SOFT_WARN_BYTES` or 10 MiB
- refuse above `BRAINSTACK_OUTBOX_HARD_MAX_BYTES` or 250 MiB

Because payloads can contain sensitive prompt/context material, rely on host disk encryption and normal filesystem privacy. Brainstack intentionally does not add fake local encryption without solving key management.

