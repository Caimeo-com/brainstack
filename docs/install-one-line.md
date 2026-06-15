# One-Line Client Install

Brainstack's low-friction client install is a small shell downloader plus the compiled `brainctl` binary. The shell script only detects the platform, downloads the matching release asset, verifies its SHA256 sidecar, installs `brainctl`, and then delegates setup to `brainctl enroll`.

The ordinary user path does not require a Brainstack source checkout. Enrollment writes the client config, installs SSH host pins from the invite, clones or updates the shared-brain checkout, installs harness guidance, installs the default Codex skill bundle for Codex clients, and runs doctor unless explicitly skipped.

On macOS, the signed menu app is an equivalent low-friction installer path: the DMG bundles a standalone `brainctl`, copies it to `~/.local/bin/brainctl`, prompts for the invite, runs `brainctl enroll --invite-file ...`, and finishes with `brainctl lifecycle repair`. Use the terminal installer when you want a scriptable path or when the Mac app is not available for the selected release.

On a control host, create a private invite:

```bash
brainctl invite create \
  --config ~/.config/brainstack/brainstack.yaml \
  --import-token-file ~/brain-import-token.txt \
  --control-ssh operator@brain-control \
  --ssh-known-hosts-file ~/.config/brainstack/control_ssh_known_hosts
```

`--ssh-known-hosts-file` should point at a file containing the selected control SSH host pin. Extra known-host entries are ignored; if the explicit file has no matching control-host pin, invite creation fails instead of embedding unrelated private topology.

That prints a copyable command shaped like this after you publish release artifacts. By default, `brainctl invite create` pins the command to Brainstack's package version so enrollment installs a known release:

```bash
curl -fsSL 'https://github.com/Caimeo-com/brainstack/releases/download/vX.Y.Z/install.sh' | sh
```

Paste the printed invite when the installer prompts for it. The invite is the private part: it contains the client connection config, optional import token, optional pinned SSH host keys, the control-host SSH target, and the control-host Brainstack repo path used by `brainctl telegram send-file`. Treat token-bearing invites as bearer secrets and avoid putting them in shell history, screenshots, shared logs, or issue trackers.

Use `--install-version latest` only when you intentionally want the generated command to track the newest GitHub release. Use `--install-url URL` for staging, forks, or a private release mirror; custom URLs must use HTTPS unless `--allow-insecure-install-url` is passed for local smoke tests.

## Prerequisites

The standalone client install does not require Bun or a Brainstack source checkout. Those are only required for source-run control, worker, and single-node profiles. Client enrollment still needs:

| Platform | Needed before enrollment |
| --- | --- |
| macOS | Xcode Command Line Tools for Git, built-in SSH, Tailscale from `https://tailscale.com/download/mac`, and Codex App/Codex CLI or Claude Code already authenticated. |
| Debian/Ubuntu | `curl` or `wget`, `sha256sum`, Git, OpenSSH client, Tailscale, and the selected harness already authenticated. |
| Arch/Omarchy | `curl` or `wget`, `sha256sum`, Git, OpenSSH, Tailscale, and the selected harness already authenticated. |

Codex App users can satisfy the Codex harness check with the app-bundled CLI at `/Applications/Codex.app/Contents/Resources/codex`; `codex` does not have to be on `PATH` for enrollment. If the one-line path stops on prerequisites, use `docs/fresh-machine-install.md` to prepare the machine and rerun the same invite.

The default invite installs the client Codex skill bundle. For an operator Mac, create the invite with the operator skill profile:

```bash
brainctl invite create \
  --config ~/.config/brainstack/brainstack.yaml \
  --import-token-file ~/brain-import-token.txt \
  --control-ssh operator@brain-control \
  --ssh-known-hosts-file ~/.config/brainstack/control_ssh_known_hosts \
  --skills-profile operator
```

Use `brainctl skills install --target codex --profile client|operator` after enrollment only to repair or deliberately change the installed skill bundle.

## After Install

Use the lifecycle wrapper for day-two maintenance:

```bash
brainctl lifecycle status --config ~/.config/brainstack/brainstack.yaml
brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml --dry-run
brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml
```

`lifecycle status` is a bounded read-only health report. `lifecycle repair` refreshes generated runtime files, repairs missing local Codex/Claude/Cursor guidance stubs, reinstalls the local `brainstackd` service when the profile uses it, reinstalls fail-open hooks, and refreshes shared skill packages from the already-local shared-brain clone. It does not delete data, clone/pull during guidance repair, or reseed canonical shared-brain pages. Pass `--sync-skills` if you explicitly want repair to pull the shared-brain clone before refreshing skill packages.

Use `brainctl lifecycle uninstall --dry-run` to inspect removal. Without `--dry-run`, uninstall requires `--yes` and delegates to the manifest-driven `destroy` command. It defaults to client-owned artifacts on client installs, worker-owned artifacts on worker installs, and full managed artifact removal on control/single-node installs.

## Public Installer

The public installer is safe to host because it contains no stack-specific secrets. After publishing a release, install from that tag:

```bash
curl -fsSL 'https://github.com/Caimeo-com/brainstack/releases/download/vX.Y.Z/install.sh' | sh
```

Common flags:

```bash
curl -fsSL 'https://github.com/Caimeo-com/brainstack/releases/download/vX.Y.Z/install.sh' | sh -s -- \
  --bin-dir "$HOME/.local/bin" \
  --skills-profile operator \
  --skip-doctor
```

For non-interactive setup, write the invite to a private file and pass it without exposing the value in `brainctl` argv:

```bash
chmod 600 ~/brainstack-invite.txt
curl -fsSL 'https://github.com/Caimeo-com/brainstack/releases/download/vX.Y.Z/install.sh' | sh -s -- \
  --invite-file ~/brainstack-invite.txt
```

Do not use `--invite-file -` with `curl ... | sh`; stdin is already occupied by the installer script. The installer rejects that shape. Direct `brainctl enroll --invite-file -` remains available after `brainctl` is installed, but prompt or private-file enrollment is the recommended path.

## Flag Reference

- `--invite-file FILE`: reads the private invite from a local file. Prefer this over raw invite arguments so token-bearing invites do not land in shell history, process listings, or logs.
- `--version vX.Y.Z|latest`: selects the GitHub release to download. Use a tag for reproducible installs and `latest` for the newest published release.
- `--base-url URL`: downloads `brainctl-<os>-<arch>` and `brainctl-<os>-<arch>.sha256` from another HTTPS directory. This is mainly for staging, forks, or local release smoke tests.
- `--bin-dir DIR`: installs `brainctl` into `DIR`. The default is `~/.local/bin`.
- `--config FILE`: writes the enrolled Brainstack config to `FILE` instead of `~/.config/brainstack/brainstack.yaml`.
- `--skills-profile client|operator|control|worker|none`: chooses which Codex skill bundle enrollment installs. `client` installs shared-brain client usage plus the generic Brainstack runbook. `operator` installs all public Brainstack skills. `control` installs control-host operations, curation, and remote-machine operations. `worker` installs client/worker discipline plus remote-machine operations. `none` installs no skills. The installer flag overrides the invite value.
- `--skip-skills`: prevents Codex skill installation even if the invite asks for a profile. Use this for non-Codex harnesses or machines where skills are managed separately.
- `--skip-doctor`: skips the post-enrollment doctor run. Use this only when prerequisites are known missing or diagnostics are being run separately.
- `--skip-init`: writes config and embedded host trust, then stops before cloning the shared brain, writing local env files, installing harness guidance, installing skills, or running doctor.
- `--skip-enroll`: installs the `brainctl` binary only. Finish later with `brainctl enroll --invite-file /path/to/invite.txt`.
- `--force`: allows enrollment to replace an existing Brainstack config. Without it, existing configs are preserved.
- `--allow-unsafe-invite`: permits raw `--invite` or `BRAINSTACK_INVITE` input. Use only for local throwaway smoke tests; real installs should use a prompt or `--invite-file`.

The installer requires HTTPS by default for all download tools. Set `BRAINSTACK_INSTALL_ALLOW_INSECURE=1` only for local release smoke tests against `file://` or temporary HTTP URLs.

## Release Artifacts

`scripts/release.sh` builds:

- `dist/brainctl-darwin-arm64`
- `dist/brainctl-darwin-x64`
- `dist/brainctl-linux-arm64`
- `dist/brainctl-linux-x64`
- `dist/install.sh`
- `dist/manifest.json`
- `dist/brainstack-<version>.tar.gz`
- `dist/BrainstackMenu-<version>.zip`
- `dist/BrainstackMenu-<version>.dmg`

Each binary, source archive, and menu-app artifact gets a `.sha256` sidecar. Set `BRAINSTACK_RELEASE_TARGETS` to a space-separated target subset for local test releases.

The compiled binary embeds both client bootstrap templates and `packages/skills`, so the one-line installer does not need Bun or a source checkout for client enrollment or skill installation.

The release script stamps `dist/install.sh` with the release tag so `curl -fsSL .../releases/download/vX.Y.Z/install.sh | sh` downloads `brainctl` from the same tag by default. The GitHub release workflow builds and can publish CLI assets without Apple signing or notarization secrets. Tag releases also build the signed/notarized universal macOS menu app when signing secrets are configured; manual workflow runs can opt in with `include_menu_app`.
