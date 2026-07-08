# Quickstart: macOS Client

The macOS client profile clones the shared-brain repo, writes a local env file if missing, installs Codex/Claude/Cursor instruction snippets without silently overwriting existing files, and can install an optional LaunchAgent for `brainctl daemon run`. The installer normalizes `~/shared-brain` to an absolute path before clone/pull operations, so the default path works on macOS and Linux.

The smooth client path is a current-platform compiled `brainctl` binary. It does not need Bun or a Brainstack source checkout on the Mac because the client bootstrap assets and public Codex skills are embedded in the binary. It still checks Git, SSH, Tailscale for the current tailnet workflow, and the selected harness, but it does not require passwordless sudo because ordinary clients do not run Brainstack machine-administration services.

Use this path even on a Mac that also has a Brainstack development checkout. The checkout is for product work; the installed client lives under normal user locations such as `~/.local/bin/brainctl`, `~/.config/brainstack/brainstack.yaml`, `~/.config/shared-brain.env`, `~/.codex/skills`, and `~/shared-brain`.

## Menu App Install

For a normal Mac user, the signed `BrainstackMenu-<version>.dmg` is the friendliest path. It includes a standalone `brainctl` in the app bundle. On first run, click **Set Up…**, paste a fresh `bs1_...` invite, and the app will:

1. copy the bundled helper to `~/.local/bin/brainctl`;
2. write the invite to a private temporary file;
3. run `brainctl enroll --invite-file ... --config ~/.config/brainstack/brainstack.yaml`;
4. delete the temporary invite file;
5. run `brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml`.

Copying `brainctl` to `~/.local/bin` first keeps hooks and the LaunchAgent pointed at a stable path instead of the mounted DMG, Downloads, or the app bundle. The pasted invite is never stored in app preferences or diagnostics.

## Binary-First Install

If the control host has a current `brainctl`, prefer an invite:

```bash
brainctl invite create \
  --config ~/.config/brainstack/brainstack.yaml \
  --import-token-file ~/brain-import-token.txt \
  --control-ssh operator@brain-control \
  --ssh-known-hosts-file ~/.config/brainstack/control_ssh_known_hosts
```

Run the printed command on the Mac after the selected release exists, then paste the printed invite at the prompt. The printed command is pinned to the Brainstack package release by default, and release-built `install.sh` is stamped so it downloads `brainctl` from the same tag. Pass `--install-version latest` only when you intentionally want a moving installer. The installer keeps the invite out of `brainctl` argv by writing it to a private temporary file and calling `brainctl enroll --invite-file ...`; enrollment writes `~/.config/brainstack/brainstack.yaml`, installs pinned SSH host keys when embedded, clones the shared brain, installs harness guidance, installs the default Codex client skill bundle when the invite uses Codex, and runs doctor unless `--skip-doctor` is passed to the installer.

If this Mac is the operator's daily-driver machine, create the invite with the operator skill bundle:

```bash
brainctl invite create \
  --config ~/.config/brainstack/brainstack.yaml \
  --import-token-file ~/brain-import-token.txt \
  --control-ssh operator@brain-control \
  --ssh-known-hosts-file ~/.config/brainstack/control_ssh_known_hosts \
  --skills-profile operator
```

`--ssh-known-hosts-file` should contain the control SSH host pin for the `--control-ssh` target. Extra entries are ignored; a file with no matching control-host pin is rejected.

For scripted setup, save the invite to a `chmod 600` file and pass `--invite-file /path/to/invite.txt`. Do not pass `--invite-file -` to the installer when using `curl ... | sh`; stdin is already the installer script. Avoid `--invite bs1_...` on shared machines because token-bearing invites can otherwise land in shell history or process listings.

Skill profile values:

- `client`: ordinary enrolled Mac or Linux client. Installs shared-brain client usage plus the generic Brainstack runbook.
- `operator`: admin or daily-driver machine that should operate control hosts, workers, curation, recovery, and client workflows. Installs every public Brainstack skill.
- `control`: control-host operations, curation, and remote-machine operations.
- `worker`: worker/client discipline plus remote-machine operations.
- `none`: no Codex skills. Use only when the harness is not Codex or skills are managed separately.

Installer flags override the invite profile. Use `--skip-skills` to suppress skill installation without changing the invite, `--skip-doctor` when diagnostics will run separately, `--skip-init` to write config and host pins only, and `--skip-enroll` to install only the `brainctl` binary.

Manual binary-first install remains useful for custom configs:

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

## Install Agent Skills

Brainstack also ships public Codex skills with the CLI. They are generic product runbooks for shared-brain usage, client discipline, file relay, and operator workflows; they do not contain private machine topology.

Invite enrollment installs the Codex client skill profile automatically for Codex clients. On an enrolled client, rerun this only to repair or intentionally switch profiles:

```bash
brainctl skills install --target codex --profile client
```

On an operator or control-host machine:

```bash
brainctl skills install --target codex --profile operator
```

Use `--dry-run` to inspect the files first. Keep exact hostnames, local service paths, and Telegram routing details in a private local overlay skill, not in the public Brainstack package.

Codex App users can satisfy the enrollment harness check with `/Applications/Codex.app/Contents/Resources/codex`; `codex` does not have to be on `PATH`. If the app-bundled CLI is the only Codex binary found, enrollment writes that absolute path into the Brainstack client config.

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

If invite enrollment configured `client.telegramVia` and `client.telegramRemoteRepo`, `--via` and `--remote-repo` can be omitted. The command streams the local file over SSH, invokes `apps/telemux/src/send-file.ts` on the control host, and lets telemux read its own `telemux.runtime.env` plus `telemux.secrets.env`. The default target is `FACTORY_TELEGRAM_CONTROL_CHAT_ID`; use `--context SLUG` to send into an existing bound telemux topic. Files are rejected locally and remotely if they are symlinks, directories, over 45 MiB by default, or look like secrets unless `--allow-sensitive` is explicitly supplied.

SSH trust is pinned by default through `~/.config/brainstack/ssh_known_hosts` from the client config root. Invites can embed the control host pin; otherwise run `brainctl trust-worker` or install the control host key first, or use `--known-hosts FILE` for a custom pin file. `--ssh-trust accept-new` is a bootstrap escape hatch only; it uses OpenSSH TOFU semantics and should be replaced with a pinned host key before routine use. Use `--display-name NAME` to change the Telegram filename and `--max-bytes N` for a smaller explicit cap.

## Upload A Mac File To A Brainstack Machine

Use the uploads registry when the file should land on a Brainstack machine for a harness to read, not be sent back to Telegram:

```bash
brainctl uploads put \
  --config ~/.config/brainstack/brainstack.yaml \
  --machine erbine \
  --file ~/Downloads/retrospective.zip

brainctl uploads list \
  --config ~/.config/brainstack/brainstack.yaml \
  --machine erbine \
  --recent
```

The macOS menu app exposes the same flow under **Uploads…**. After uploading, copy the remote path or tell a bound Telemux topic something like "use the file I just uploaded"; Telemux will add recent upload paths to the harness prompt. Remove short-lived files with `brainctl uploads rm --machine <machine> --id <upload-id>` or the menu app's Delete button.

## Harness Instructions

The bootstrap installer installs real guidance, not prose pointers:

- Codex: creates `~/.codex/AGENTS.md` as a symlink to the product-owned shared-brain guidance if no Codex global file exists. If it exists, the installer prints an exact append command.
- Claude: writes real `@~/.config/brainstack/client-bootstrap/claude-user-CLAUDE.md` import syntax.
- Cursor: writes the actual shared-brain rule content if no rule exists. If it exists, the installer prints the exact merge command.
- The generated files come from checked-in templates under `packages/client-bootstrap/`; compiled `brainctl` embeds those templates and only renders config placeholders at install time.

For background refresh/checkpoint integration, install fail-open hooks after enrollment:

```bash
brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml --dry-run
brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml
brainctl hooks install --target all --config ~/.config/brainstack/brainstack.yaml
brainctl hooks status --target all
```

Prefer `lifecycle repair` for routine client repair: it refreshes generated runtime files, repairs missing local harness guidance stubs, installs/starts the local daemon, installs hooks, and refreshes shared skill packages from the local shared-brain clone. The lower-level `hooks install` command is useful when you only want to touch hook config. `--target codex` writes `~/.codex/hooks.json`, `--target claude` writes `~/.claude/settings.json`, and `--target cursor` writes `~/.cursor/hooks.json`. `--target all` applies all three. The installer preserves unrelated hook entries and `brainctl hooks remove --target all` removes only Brainstack-managed entries. Hooks refresh shared skill packages on session or prompt start and write local checkpoint metadata; if Brainstack is offline, they return success without blocking the harness.

To globalize existing local skills, start with the no-side-effect planner:

```bash
brainctl skills import --config ~/.config/brainstack/brainstack.yaml
brainctl skills import ~/.codex/skills --config ~/.config/brainstack/brainstack.yaml --select 1,3
brainctl skills import ~/.codex/skills --config ~/.config/brainstack/brainstack.yaml --apply
```

The planner scans the current directory plus default Codex, Claude, and Cursor skill roots when no folder is passed. Passing a folder scans just that folder. Selecting numbers interactively, using `--select`, or using `--apply` enqueues skills as global shared-brain imports so connected harnesses can refresh them on the next prompt or manual `brainctl skills refresh`.

## Local Daemon

Install the local daemon when this Mac should keep `~/shared-brain`, outbox, and shared skills fresh without every harness hook doing Git work:

```bash
brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml
brainctl daemon install --config ~/.config/brainstack/brainstack.yaml
brainctl daemon install --config ~/.config/brainstack/brainstack.yaml --start
brainctl daemon status --config ~/.config/brainstack/brainstack.yaml
```

`lifecycle repair` is the normal one-command path. Use the lower-level daemon commands when you only want to inspect or change the daemon service. On macOS daemon install writes `~/Library/LaunchAgents/com.brainstack.daemon.plist`. It runs the same `brainctl` binary in daemon mode; there is no second binary to install. See [`daemon.md`](./daemon.md).

## Read/Write Model

- Read from `~/shared-brain` when possible.
- Sync with `git -C ~/shared-brain pull --ff-only`.
- Write imports/proposals with the HTTP API and import token.
- Direct git push is trusted power-user mode only.
