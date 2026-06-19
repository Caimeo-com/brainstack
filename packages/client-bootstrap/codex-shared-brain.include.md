# Shared Brain Client

- Use `brainctl` from PATH when available. If it is not on PATH, try the stable install path shown by the installer, usually `$HOME/.local/bin/brainctl`, before treating Brainstack as unavailable.
- Before substantial work in a repository, run `brainctl context --repo .`.
- Follow the returned Brainstack instructions. Use `brainctl search --repo . "query"` for retrieval and `brainctl remember --repo . --summary "..."` for imports/proposals.
- Preserve source labels when multiple brains are active.
- Do not directly edit canonical wiki pages unless explicitly instructed.
- Keep project-local state in the project, not in global memory.
