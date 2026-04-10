# Fresh Machine Bootstrap

This document is the reproducible bootstrap path for a clean machine. It is written so a human or another LLM can rebuild the current Clawdex setup without needing hidden local memory.

Use it together with:

- [README.md](../README.md)
- [docs/architecture.md](./architecture.md)
- [docs/operations.md](./operations.md)
- [templates/codex/config.toml.example](../templates/codex/config.toml.example)
- [templates/sudoers/factory-nopasswd](../templates/sudoers/factory-nopasswd)

## Goal

Provision one Linux control host that:

- runs `telemux.service` as a user-level systemd service
- stores control-plane state under `/srv/telemux`
- stores managed workspaces under `/srv/factory`
- runs Codex in permanent no-approval mode for this machine
- can optionally reach one or more remote workers over SSH

The current live deployment uses a user account with passwordless sudo and a Codex config equivalent to:

- `approval_policy = "never"`
- `sandbox_mode = "danger-full-access"`

That is intentionally powerful. Only reproduce it on machines you control.

## Assumptions

- Debian/Ubuntu-like Linux with `systemd`
- one dedicated control user, named `factory` below
- `sudo` available during initial provisioning
- Telegram bot token already created in `@BotFather`
- optional remote worker reachable over SSH or Tailscale

## 1. Create the control user

If the machine does not already have a dedicated control user:

```bash
sudo adduser factory
sudo usermod -aG sudo factory
```

Create a passwordless sudo rule:

```bash
sudo install -d -m 0750 /etc/sudoers.d
sudo cp /path/to/clawdex/templates/sudoers/factory-nopasswd /etc/sudoers.d/factory-nopasswd
sudo chmod 0440 /etc/sudoers.d/factory-nopasswd
sudo visudo -cf /etc/sudoers.d/factory-nopasswd
```

You can also inline the rule:

```bash
echo 'factory ALL=(ALL) NOPASSWD: ALL' | sudo tee /etc/sudoers.d/factory-nopasswd >/dev/null
sudo chmod 0440 /etc/sudoers.d/factory-nopasswd
sudo visudo -cf /etc/sudoers.d/factory-nopasswd
```

After that, validate:

```bash
sudo -u factory -H bash -lc 'sudo -n true && echo ok'
```

## 2. Install base packages

As root or through sudo:

```bash
sudo apt-get update
sudo apt-get install -y git curl unzip sqlite3 openssh-client openssh-server ca-certificates
```

If you plan to use Tailscale for worker reachability, install and join Tailscale too.

## 3. Install Bun

As the control user:

```bash
su - factory
curl -fsSL https://bun.sh/install | bash
source ~/.bashrc
bun --version
```

The current live machine uses Bun at `~/.bun/bin/bun`.

## 4. Install Codex CLI

Install Codex using your preferred distribution method, then make sure the executable is either:

- available as `codex` in `PATH`, or
- referenced explicitly later through `FACTORY_CODEX_BIN`

This repo does not install Codex for you.

## 5. Configure Codex for permanent yolo mode

Create `~/.codex/config.toml` from [templates/codex/config.toml.example](../templates/codex/config.toml.example).

Minimum settings for the current machine model:

```toml
approval_policy = "never"
sandbox_mode = "danger-full-access"
model = "gpt-5.4"
model_reasoning_effort = "xhigh"
```

Run the one-shot bootstrap for the tracked Codex templates:

```bash
cd ~/clawdex
./scripts/bootstrap-codex.sh
```

That installs `~/.codex/config.toml` and `~/.codex/AGENTS.md` if they do not already exist. Review both files afterward.

The tracked AGENTS template currently uses:

```md
# Machine Guidance

- This machine is the Clawdex control plane.
- Prefer Bun and simple shell scripts over heavier runtimes.
- Prefer inspectable files over hidden state.
- Avoid Docker unless absolutely necessary.
- Any repo-bound task should use per-context worktrees.
- Global durable machine guidance belongs in `~/.codex/AGENTS.md`.
- Context and task state belongs in `.factory/` files inside each worktree, not in global memory.
- When working on `~/clawdex`, keep docs current with reality.
```

## 6. Clone and configure the repo

As the control user:

```bash
git clone git@github.com:swader/clawdex.git ~/clawdex
cd ~/clawdex
cp .env.example .env
cp workers.example.json workers.json
```

Edit `.env`:

- set `FACTORY_TELEGRAM_BOT_TOKEN`
- set `FACTORY_ALLOWED_TELEGRAM_USER_ID`
- confirm `FACTORY_LOCAL_MACHINE`
- optionally set `FACTORY_TELEGRAM_CONTROL_CHAT_ID`
- optionally set `FACTORY_CODEX_BIN` if `codex` is not on `PATH`

Edit `workers.json`:

- keep the local `control` worker as `transport: "local"`
- add any remote worker entries with the correct SSH target/user

## 7. Install and verify the service

As the control user:

```bash
cd ~/clawdex
./scripts/install.sh
./scripts/doctor.sh
```

What `install.sh` does:

- creates `/srv/telemux`, `/srv/telemux/contexts`, `/srv/telemux/logs`
- creates `/srv/factory`, `/srv/factory/repos`, `/srv/factory/hostctx`, `/srv/factory/scratch`
- installs the user systemd unit to `~/.config/systemd/user/telemux.service`
- enables linger for the control user
- enables and starts `telemux.service`

Validate:

```bash
systemctl --user status telemux.service
journalctl --user -u telemux.service -n 50 --no-pager
curl -fsS http://127.0.0.1:8787/healthz
```

## 8. Telegram onboarding

1. Create or recover the bot token from `@BotFather`.
2. Add the bot to the target supergroup.
3. Enable forum topics in the supergroup.
4. Disable bot privacy in `@BotFather`.
5. From the allowed Telegram account, send `/whoami` in the target topic.
6. In a topic you want to bind, create the first context:

```text
/newctx scratchpad control scratch
```

At that point, plain text in the bound topic should start or resume a Codex session.

## 9. Optional remote worker bootstrap

On the remote worker machine:

1. Create the same `factory` user or another dedicated worker user.
2. Give that user passwordless sudo if you want Codex to have the same privileges there.
3. Install `git`, `bash`, and the Codex CLI.
4. Enable `sshd`.
5. Trust the control host SSH key.
6. Create the managed roots if you want them to live under `/srv/factory` there too.

Example worker validation from the control host:

```bash
ssh factory@worker1 'hostname && command -v codex && command -v git'
```

Then bind it in Telegram:

```text
/newctx worker-general worker1 host
```

If it is still unreachable, the context should remain `pending` instead of crashing the control plane.

## 10. Reboot test

Because the service is a user unit with linger enabled, test a clean boot:

1. reboot the control host
2. do not log in locally first
3. from Telegram only, try:

```text
/whoami
/workers
/topicinfo
```

If those work, the service survived reboot in the intended way.

## 11. Files that define the system

If another LLM needs to understand or reproduce this deployment, start with these tracked files:

- `README.md`
- `docs/architecture.md`
- `docs/operations.md`
- `docs/fresh-machine-bootstrap.md`
- `workers.example.json`
- `.env.example`
- `systemd/telemux.service`
- `scripts/install.sh`
- `scripts/doctor.sh`
- `templates/codex/config.toml.example`
- `templates/sudoers/factory-nopasswd`

Then add these machine-local files, which are not tracked in git:

- `~/.codex/config.toml`
- `~/.codex/AGENTS.md`
- `~/clawdex/.env`
- `~/clawdex/workers.json`
