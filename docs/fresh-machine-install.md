# Fresh Machine Install

Use this when bringing a new machine into an existing Brainstack stack. Brainstack does not install OS packages for you; it checks prerequisites, writes config, and then initializes product-owned files.

## Stage 0: Prerequisites

On Arch/Omarchy-style Linux:

```bash
sudo pacman -S --needed git openssh tailscale
sudo systemctl enable --now sshd.service tailscaled.service
curl -fsSL https://bun.sh/install | bash
```

Then open a new shell or export Bun's bin path:

```bash
export PATH="$HOME/.bun/bin:$PATH"
```

Install and authenticate the selected harness, usually Codex or Claude, before running Brainstack provisioning. Brainstack only verifies the harness; it does not create harness accounts or permission-bypass settings.

Do not reboot remote encrypted machines as part of setup. If a host uses disk encryption that needs a local unlock, keep all setup to service/package starts and user-level changes.

## Get Brainstack

```bash
git clone https://github.com/Caimeo-com/brainstack ~/brainstack
cd ~/brainstack
bun install --frozen-lockfile
```

If using a compiled `brainctl` release later, this source checkout is still the clearest path for first installs because services and templates point at the product repo.

## Worker Install

Generate the machine config:

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts provision \
  --profile worker \
  --out ~/.config/brainstack/brainstack.yaml \
  --harness codex \
  --brain-base-url https://brain-control.example.ts.net \
  --brain-remote operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git
```

Worker provisioning does not require passwordless sudo by default. Add `--require-harness-sudo` only for workers expected to perform privileged machine administration.

Initialize the worker as a shared-brain client. If this machine should immediately push imports/proposals back to the brain, pass the import token without printing it:

```bash
BRAIN_IMPORT_TOKEN_FILE=~/brain-import-token.txt \
  bun run packages/brainctl/src/main.ts init --profile worker --config ~/.config/brainstack/brainstack.yaml
```

Verify:

```bash
bun run packages/brainctl/src/main.ts doctor --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts doctor --config ~/.config/brainstack/brainstack.yaml --write-smoke
bun run packages/brainctl/src/main.ts updates --config ~/.config/brainstack/brainstack.yaml
```

`--write-smoke` is intentionally mutating: it posts a small import artifact through `/api/import`. Run it when proving write readiness, not as a routine health check.

## Control Host Install

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts provision \
  --profile control \
  --out ~/.config/brainstack/brainstack.yaml \
  --harness codex \
  --enable-telemux \
  --brain-base-url https://brain-control.example.ts.net

bun run packages/brainctl/src/main.ts init --profile control --config ~/.config/brainstack/brainstack.yaml
loginctl enable-linger "$USER"
systemctl --user daemon-reload
```

Fill `~/.config/brainstack/telemux.secrets.env` before starting telemux:

```env
FACTORY_TELEGRAM_BOT_TOKEN=
FACTORY_TELEGRAM_CONTROL_CHAT_ID=
FACTORY_ALLOWED_TELEGRAM_USER_ID=
BRAIN_IMPORT_TOKEN=
```

When `FACTORY_TELEGRAM_CONTROL_CHAT_ID` is set, telemux bootstraps a `brainstack-routines` scratch context and installs the deterministic `update-check` routine on startup. If a new worker is later added to `brainstack.yaml` and `brainctl upgrade` rewrites `workers.json`, telemux reloads the worker file without a restart and the next update-check report includes that machine. Bad worker config fails closed instead of silently dispatching against stale worker data.

Start or restart services:

```bash
systemctl --user enable --now braind.service
systemctl --user enable --now telemux.service
```

## Add Worker To Control

On the control host:

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts join-worker --config ~/.config/brainstack/brainstack.yaml --worker brain-worker --ssh-user operator
```

Merge the YAML snippet into `~/.config/brainstack/brainstack.yaml`, then apply runtime:

```bash
bun run packages/brainctl/src/main.ts upgrade --profile control --config ~/.config/brainstack/brainstack.yaml
systemctl --user daemon-reload
```

The generated `workers.json` is hot-reloaded by telemux. Restarting telemux is still fine after an upgrade, but worker discovery and update-check inclusion no longer depend on a restart.
