# Claude Shared Brain

@__SHARED_BRAIN_LOCAL_PATH__/AGENTS.shared-client.md

Use `brainctl` from PATH when available. If it is not on PATH, try the stable install path shown by the installer, usually `$HOME/.local/bin/brainctl`, before treating Brainstack as unavailable.

Claude-specific notes: use `brainctl context --repo .`, then `brainctl search --repo . "query"` and `brainctl remember --repo . --summary "..."`; preserve source labels and use proposals for synthesized changes unless acting as organizer/admin.
