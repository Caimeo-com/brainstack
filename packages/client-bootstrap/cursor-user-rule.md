# Shared Brain Rule

Use `brainctl` from PATH when available. If it is not on PATH, try the stable install path shown by the installer, usually `$HOME/.local/bin/brainctl`, before treating Brainstack as unavailable.

Before substantial work in a repository, run `brainctl context --repo .`, then use `brainctl search --repo . "query"` and `brainctl remember --repo . --summary "..."`. Preserve source labels across multiple brains; do not directly mutate wiki pages unless explicitly instructed.
