# One-Line Client Install

Brainstack's low-friction client install is a small shell downloader plus the compiled `brainctl` binary. The shell script only detects the platform, downloads the matching release asset, verifies its SHA256 sidecar, installs `brainctl`, and then delegates setup to `brainctl enroll`.

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

Fresh macOS clients still need Git, SSH, Tailscale for the tailnet workflow, and the selected harness CLI (`codex` or `claude`). If the one-line path stops on prerequisites, use `docs/fresh-machine-install.md` to prepare the machine and rerun the same invite.

After enrollment, install the public Codex runbook bundle if this machine will use Codex skills:

```bash
brainctl skills install --target codex --profile client
```

## Public Installer

The public installer is safe to host because it contains no stack-specific secrets. After publishing a release, install from that tag:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://github.com/Caimeo-com/brainstack/releases/download/$RELEASE_TAG/install.sh" | sh
```

Useful flags:

```bash
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://github.com/Caimeo-com/brainstack/releases/download/$RELEASE_TAG/install.sh" | sh -s -- \
  --bin-dir "$HOME/.local/bin" \
  --skip-doctor
```

For non-interactive setup, write the invite to a private file and pass it without exposing the value in `brainctl` argv:

```bash
chmod 600 ~/brainstack-invite.txt
RELEASE_TAG=vX.Y.Z
curl -fsSL "https://github.com/Caimeo-com/brainstack/releases/download/$RELEASE_TAG/install.sh" | sh -s -- \
  --invite-file ~/brainstack-invite.txt
```

Use `--version vX.Y.Z` to pin a GitHub release after release assets exist, or `--base-url URL` to point at another directory that serves `brainctl-<os>-<arch>` plus `brainctl-<os>-<arch>.sha256`.

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
