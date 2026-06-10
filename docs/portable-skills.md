# Portable Skills

Brainstack ships public, generic agent skills under `packages/skills`. They are product runbooks for Codex-style skill systems, not shared-brain data.

Install them from a source checkout or standalone `brainctl` binary:

```bash
brainctl skills install --target codex --profile client
```

Use the operator bundle on control hosts or admin machines:

```bash
brainctl skills install --target codex --profile operator
```

Profiles:

- `client`: client read/import/propose discipline plus the generic Brainstack runbook.
- `operator`: all public Brainstack skills.
- `control`: control-host operations, curation, and remote-machine operations.
- `worker`: worker/client discipline and remote-machine operations.

Invite enrollment also accepts `--skills-profile none` and installer `--skip-skills` when no Codex skills should be installed. `none` is an enrollment opt-out, not a `brainctl skills install --profile` bundle.

Options:

- `--skill NAME` installs one or more explicit skills.
- `--all` installs every public Brainstack skill.
- `--dir DIR` writes into a custom Codex skills root instead of `CODEX_HOME/skills` or `~/.codex/skills`.
- `--dry-run` prints the target files without writing.

The public bundle intentionally excludes private topology. Do not add real hostnames, local usernames, Telegram chat ids, tailnet names, env values, or customer-specific service paths to `packages/skills`. Keep those in a private local overlay skill.

## Client File Relay

The public skills know about `brainctl telegram send-file` because it is product behavior. An enrolled client can stream a file over SSH to the control host, and telemux sends it through the control host's Telegram bot. The client should not store Telegram bot tokens.

Use:

```bash
brainctl telegram send-file \
  --config ~/.config/brainstack/brainstack.yaml \
  --file ~/Downloads/report.pdf \
  --caption "Report from this machine"
```

Invites can carry the control SSH target, remote Brainstack repo path, and pinned host keys so routine file relay does not need ad hoc `scp`, provider credentials, or `ssh-trust accept-new`.
