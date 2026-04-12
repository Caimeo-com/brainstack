# Operator Preflight

Brainstack is intended for private machines where the operator deliberately gives an AI harness broad local authority. Do not install it on a machine where that authority would be surprising.

## Required Permission Model

For control and single-node profiles, prepare the Unix user before installing services:

1. Install Bun for the target user.
2. Install and authenticate the chosen harness CLI, usually Codex CLI and optionally Claude Code.
3. Configure the harness for unattended execution only if you accept the risk.
4. Configure passwordless sudo for the current user if telemux/Codex/Claude should perform machine administration.
5. Confirm OpenSSH works over Tailscale for worker access.
6. On Linux hosts using user services, enable lingering so services start without an interactive login: `loginctl enable-linger "$USER"`.

For worker access, verify normal OpenSSH explicitly:

```bash
tailscale ping brain-worker
ssh brain-worker true
```

The tailnet policy must allow the operator to reach workers on `tcp:22`, the control host to reach workers on `tcp:22`, and workers to reach the control host on `tcp:22` plus `tcp:443`. See [`tailscale-control-worker.md`](./tailscale-control-worker.md) for the full grant shape and why each direction exists.

If SSH reports `tailscale: tailnet policy does not permit you to SSH to this node`, you are hitting Tailscale SSH, not normal OpenSSH. Disable Tailscale SSH on that worker and ensure `sshd.service` is active.

Example sudoers fragment:

```sudoers
operator ALL=(ALL) NOPASSWD:ALL
```

Install with `visudo`, not by writing directly to `/etc/sudoers`:

```bash
sudo visudo -f /etc/sudoers.d/brainstack-operator
```

## Codex Yolo Mode

For a private control host where you intentionally want Codex to execute setup tasks without prompting:

```toml
approval_policy = "never"
sandbox_mode = "danger-full-access"
```

Put durable Codex configuration under the target user's Codex config, not inside the shared-brain content repo.

## Claude Yolo Mode

If using Claude Code as a harness, configure its equivalent bypass/permission mode only on machines where you accept full command execution as the current user. Keep Claude-specific config outside the brain content repo.

`brainctl provision --harness claude` tests Claude with `--dangerously-skip-permissions --permission-mode bypassPermissions`. `brainctl provision --harness codex` tests Codex with `--dangerously-bypass-approvals-and-sandbox`. Both tests ask the harness to run `sudo -n true`; failures mean the machine is not ready for unattended telemux control.

Provisioning and doctor checks do not install packages or silently change sudo/harness policy. They stop with remediation text if Bun, Git, OpenSSH, Tailscale, Codex, Claude, passwordless sudo, or harness bypass behavior is missing. Use `brainctl doctor --deep` when you want to repeat the expensive harness sudo proof after installation.

## Telemux Trust Boundary

Telemux is not a security sandbox. It receives Telegram messages from an allowed Telegram user, maps the topic to a workspace, and passes prompts/files to the configured local harness process, either Codex CLI or Claude Code.

Consequences:

- If the harness is configured for yolo mode, telemux effectively becomes a remote command path into that Unix user.
- If that Unix user has passwordless sudo, telemux can indirectly perform privileged machine changes.
- Telegram bot-token compromise does not let an attacker forge the allowed Telegram user id, but it can let them read/race bot updates, send messages as the bot, disrupt polling, and exfiltrate bot-visible chat/file metadata.
- Keep `FACTORY_ALLOWED_TELEGRAM_USER_ID` narrow and never expose the Telegram bot token.

## Minimum Preflight Checks

```bash
bun --version
git --version
ssh -V
tailscale status
sudo -n true
codex --version
claude --version
loginctl show-user "$USER" --property=Linger --value
```

If `sudo -n true` fails, either fix passwordless sudo or accept that unattended machine-admin tasks will stall.
