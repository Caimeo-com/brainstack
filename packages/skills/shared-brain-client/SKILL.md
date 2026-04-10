---
name: shared-brain-client
description: Client-side skill for reading a local shared-brain clone and writing only imports/proposals by default.
---

# Shared Brain Client

Use this skill from client machines and harnesses.

- Default local clone path: `~/shared-brain`.
- Sync with `git pull --ff-only` before relying on local content.
- Prefer reading local markdown over calling remote endpoints.
- Use the HTTP API or client CLI for imports and proposals.
- Do not directly edit canonical wiki pages unless the operator explicitly asks for trusted power-user mode.
- Never request or store the admin ingest token on client or worker profiles.

