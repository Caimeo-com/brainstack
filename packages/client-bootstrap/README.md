# Client Bootstrap

This package contains portable client bootstrap artifacts for Codex, Claude, Cursor, SSH, and shared-brain env setup.

Generated profile-specific copies are produced with:

```bash
bun run packages/brainctl/src/main.ts bootstrap-client --profile client-macos --config examples/client-macos.yaml --out /tmp/brainstack-client
```

The generated installer clones or updates `~/shared-brain`, writes `~/.config/shared-brain.env` if missing, and installs instruction snippets without silently overwriting existing files.

- Codex gets the actual shared-brain guidance via `~/.codex/AGENTS.md` symlink when that file is absent. If it already exists, the installer prints an exact command to append the product-owned guidance.
- Claude uses real `@path` import syntax, not prose import instructions.
- Cursor gets the actual rule content when no rule exists. If a rule already exists, the installer prints a manual merge command.
