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

## Telemux Trust Boundary

Telemux is not a security sandbox. It receives Telegram messages from an allowed Telegram user, maps the topic to a workspace, and passes prompts/files to the configured local harness process such as Codex CLI, and later optionally Claude Code.

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
loginctl show-user "$USER" --property=Linger --value
```

If `sudo -n true` fails, either fix passwordless sudo or accept that unattended machine-admin tasks will stall.
