# Client Bootstrap

This package contains portable client bootstrap artifacts for Codex, Claude, Cursor, SSH, and shared-brain env setup.

Generated profile-specific copies are produced with:

```bash
bun run packages/brainctl/src/main.ts bootstrap-client --profile client-macos --config examples/client-macos.yaml --out /tmp/brainstack-client
```

The generated installer clones or updates `~/shared-brain`, writes `~/.config/shared-brain.env` if missing, and installs instruction snippets without silently overwriting existing files.

