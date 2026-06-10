# One-Line Client Install

Brainstack's low-friction client install is a small shell downloader plus the compiled `brainctl` binary. The shell script only detects the platform, downloads the matching release asset, verifies its SHA256 sidecar, installs `brainctl`, and then delegates setup to `brainctl enroll`.

The ordinary user path does not require a Brainstack source checkout. Enrollment writes the client config, installs SSH host pins from the invite, clones or updates the shared-brain checkout, installs harness guidance, installs the default Codex skill bundle for Codex clients, and runs doctor unless explicitly skipped.

On a control host, create a private invite:

```bash
brainctl invite create \
  --config ~/.config/brainstack/brainstack.yaml \
  --import-token-file ~/brain-import-token.txt \
  --control-ssh operator@brain-control \
  --ssh-known-hosts-file ~/.config/brainstack/control_ssh_known_hosts
```

`--ssh-known-hosts-file` should point at a file containing the selected control SSH host pin. Extra known-host entries are ignored; if the explicit file has no matching control-host pin, invite creation fails instead of embedding unrelated private topology.

That prints a copyable command shaped like this after you publish release artifacts:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://github.com/Caimeo-com/brainstack/releases/download/$RELEASE_TAG/install.sh" | sh
```

Paste the printed invite when the installer prompts for it. The invite is the private part: it contains the client connection config, optional import token, optional pinned SSH host keys, the control-host SSH target, and the control-host Brainstack repo path used by `brainctl telegram send-file`. Treat token-bearing invites as bearer secrets and avoid putting them in shell history, screenshots, shared logs, or issue trackers.

Fresh macOS clients still need Git, SSH, Tailscale for the tailnet workflow, and the selected harness CLI (`codex` or `claude`). Codex App users can satisfy the Codex harness check with the app-bundled CLI at `/Applications/Codex.app/Contents/Resources/codex`; `codex` does not have to be on `PATH` for enrollment. If the one-line path stops on prerequisites, use `docs/fresh-machine-install.md` to prepare the machine and rerun the same invite.

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

## Public Installer

The public installer is safe to host because it contains no stack-specific secrets. After publishing a release, install from that tag:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://github.com/Caimeo-com/brainstack/releases/download/$RELEASE_TAG/install.sh" | sh
```

Common flags:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://github.com/Caimeo-com/brainstack/releases/download/$RELEASE_TAG/install.sh" | sh -s -- \
  --bin-dir "$HOME/.local/bin" \
  --skills-profile operator \
  --skip-doctor
```

For non-interactive setup, write the invite to a private file and pass it without exposing the value in `brainctl` argv:

```bash
chmod 600 ~/brainstack-invite.txt
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://github.com/Caimeo-com/brainstack/releases/download/$RELEASE_TAG/install.sh" | sh -s -- \
  --invite-file ~/brainstack-invite.txt
```

## Flag Reference

- `--invite-file FILE|-`: reads the private invite from a local file or stdin. Prefer this over raw invite arguments so token-bearing invites do not land in shell history, process listings, or logs.
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

Each binary and source archive gets a `.sha256` sidecar. Set `BRAINSTACK_RELEASE_TARGETS` to a space-separated target subset for local test releases.

The compiled binary embeds both client bootstrap templates and `packages/skills`, so the one-line installer does not need Bun or a source checkout for client enrollment or skill installation.
