# Quickstart: macOS Client

The macOS client profile installs no services. It clones the shared-brain repo, writes a local env file if missing, and installs Codex/Claude/Cursor instruction snippets without silently overwriting existing files. The installer normalizes `~/shared-brain` to an absolute path before clone/pull operations, so the default path works on macOS and Linux.

## Render Bootstrap

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts bootstrap-client --profile client-macos --config examples/client-macos.yaml --out /tmp/brainstack-client
```

Copy or fetch that bootstrap directory on the Mac, then run:

```bash
cd /tmp/brainstack-client
BRAIN_GIT_REMOTE=operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git ./install-client.sh
```

## Client Env

`~/.config/shared-brain.env` should contain:

```env
BRAIN_BASE_URL=https://brain-control.example.ts.net
BRAIN_IMPORT_TOKEN=
SHARED_BRAIN_LOCAL_PATH=~/shared-brain
```

Do not put `BRAIN_ADMIN_TOKEN` on ordinary clients. The generated client env example intentionally omits it.

## Harness Instructions

The bootstrap installer installs real guidance, not prose pointers:

- Codex: creates `~/.codex/AGENTS.md` as a symlink to the product-owned shared-brain guidance if no Codex global file exists. If it exists, the installer prints an exact append command.
- Claude: writes real `@~/.config/brainstack/client-bootstrap/claude-user-CLAUDE.md` import syntax.
- Cursor: writes the actual shared-brain rule content if no rule exists. If it exists, the installer prints the exact merge command.
- The generated files come from checked-in templates under `packages/client-bootstrap/`; `brainctl` only renders config placeholders.

## Read/Write Model

- Read from `~/shared-brain` when possible.
- Sync with `git -C ~/shared-brain pull --ff-only`.
- Write imports/proposals with the HTTP API and import token.
- Direct git push is trusted power-user mode only.
