# Quickstart: macOS Client

The macOS client profile installs no services. It clones the shared-brain repo, writes a local env file if missing, and installs Codex/Claude/Cursor instruction snippets without silently overwriting existing files. The installer normalizes `~/shared-brain` to an absolute path before clone/pull operations, so the default path works on macOS and Linux.

The smooth client path is a current-platform compiled `brainctl` binary. It does not need Bun or a Brainstack source checkout on the Mac because the client bootstrap assets are embedded in the binary. It still checks Git, SSH, Tailscale for the current tailnet workflow, and the selected harness, but it does not require passwordless sudo because ordinary clients do not run Brainstack machine-administration services.

## Binary-First Install

```bash
brainctl provision \
  --profile client-macos \
  --out ~/.config/brainstack/brainstack.yaml \
  --harness codex \
  --brain-base-url https://brain-control.example.ts.net \
  --brain-remote operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git

chmod 600 ~/brain-import-token.txt
brainctl init \
  --profile client-macos \
  --config ~/.config/brainstack/brainstack.yaml \
  --import-token-file ~/brain-import-token.txt
```

That is the lowest-friction path when the binary is already on the Mac. To generate a copyable bootstrap directory instead, render it from either the binary or source checkout:

```bash
brainctl bootstrap-client --profile client-macos --config ~/.config/brainstack/brainstack.yaml --out /tmp/brainstack-client
cd /tmp/brainstack-client
chmod 600 ~/brain-import-token.txt
BRAIN_IMPORT_TOKEN_FILE=~/brain-import-token.txt \
  ./install-client.sh
```

## Client Env

`~/.config/shared-brain.env` should contain:

```env
BRAIN_BASE_URL=https://brain-control.example.ts.net
BRAIN_IMPORT_TOKEN=
SHARED_BRAIN_LOCAL_PATH=~/shared-brain
```

Do not put `BRAIN_ADMIN_TOKEN` on ordinary clients. The generated client env example intentionally omits it.

`brainctl init` accepts `--import-token-file FILE`. The shell bootstrap installer accepts `BRAIN_IMPORT_TOKEN_FILE=...` or `BRAIN_IMPORT_TOKEN=...`. Both write the token into `~/.config/shared-brain.env` only when the existing `BRAIN_IMPORT_TOKEN` slot is blank, and neither prints the value. The shell installer also creates `~/.config/brainstack/brainstack.yaml` when that file is missing, so the client has a runnable doctor config immediately after bootstrap.

To prove pushback after install:

```bash
brainctl doctor --config ~/.config/brainstack/brainstack.yaml --write-smoke
```

`--write-smoke` posts a small import artifact, so use it as an explicit setup verification rather than a routine health check.

## Send A Mac File To Telegram

When the control host runs telemux, a Mac client can ask BrainCTL to send a local file through the control host's Telegram bot without storing the bot token on the Mac:

```bash
brainctl telegram send-file \
  --config ~/.config/brainstack/brainstack.yaml \
  --via operator@brain-control \
  --remote-repo ~/brainstack \
  --file ~/Downloads/report.pdf \
  --caption "Report from my Mac"
```

If `client.remoteSsh` is configured, `--via` can be omitted. The command streams the local file over SSH, invokes `apps/telemux/src/send-file.ts` on the control host, and lets telemux read its own `telemux.runtime.env` plus `telemux.secrets.env`. The default target is `FACTORY_TELEGRAM_CONTROL_CHAT_ID`; use `--context SLUG` to send into an existing bound telemux topic. Files are rejected locally and remotely if they are symlinks, directories, over 45 MiB by default, or look like secrets unless `--allow-sensitive` is explicitly supplied.

SSH trust is pinned by default through `~/.config/brainstack/ssh_known_hosts` from the client config root. Run `brainctl trust-worker` or otherwise install the control host key first, or use `--known-hosts FILE` for a custom pin file. `--ssh-trust accept-new` is a bootstrap escape hatch only; it uses OpenSSH TOFU semantics and should be replaced with a pinned host key before routine use. Use `--display-name NAME` to change the Telegram filename and `--max-bytes N` for a smaller explicit cap.

## Harness Instructions

The bootstrap installer installs real guidance, not prose pointers:

- Codex: creates `~/.codex/AGENTS.md` as a symlink to the product-owned shared-brain guidance if no Codex global file exists. If it exists, the installer prints an exact append command.
- Claude: writes real `@~/.config/brainstack/client-bootstrap/claude-user-CLAUDE.md` import syntax.
- Cursor: writes the actual shared-brain rule content if no rule exists. If it exists, the installer prints the exact merge command.
- The generated files come from checked-in templates under `packages/client-bootstrap/`; compiled `brainctl` embeds those templates and only renders config placeholders at install time.

## Read/Write Model

- Read from `~/shared-brain` when possible.
- Sync with `git -C ~/shared-brain pull --ff-only`.
- Write imports/proposals with the HTTP API and import token.
- Direct git push is trusted power-user mode only.
