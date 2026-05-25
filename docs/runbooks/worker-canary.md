# Worker Canary Runbook

Use this runbook before making a reused machine a real Brainstack worker. The goal is to prove the control-to-worker path with harmless work before deleting legacy services or trusting Telegram as the only control surface.

## 1. Prove Network And Login

From the operator machine:

```bash
tailscale status
tailscale ping <worker-host>
ssh -o BatchMode=yes -o ConnectTimeout=8 <worker-user>@<worker-host> true
```

If `tailscale ping` works but SSH fails with `Permission denied`, install the operator or control-host public key into the worker user's `~/.ssh/authorized_keys`. If SSH reports a Tailscale policy denial, disable Tailscale SSH on the worker or add only a temporary recovery rule; Brainstack uses normal OpenSSH over Tailscale.

## 2. Prove Worker Prerequisites

On the worker, as the same Unix user Brainstack will SSH as:

```bash
command -v bun
command -v git
command -v ssh
command -v tailscale
command -v codex || command -v claude
codex --version || claude --version
sudo -n true
```

`sudo -n true` is required only if worker tasks are expected to administer the machine. Harness yolo/bypass mode is checked later with `doctor --deep`.

## 3. Prove Worker-To-Control Freshness

The worker needs to pull the shared-brain Git remote from the control host:

```bash
git ls-remote <control-user>@<control-host>:/home/<control-user>/shared-brain/bare/shared-brain.git HEAD
```

If this fails with `Permission denied (publickey)`, create a worker-to-control Git read key and restrict it to `git-upload-pack` as documented in `docs/tailscale-control-worker.md`.

## 4. Initialize The Worker Profile

On the worker:

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts provision \
  --profile worker \
  --out ~/.config/brainstack/brainstack.yaml \
  --harness codex \
  --brain-remote <control-user>@<control-host>:/home/<control-user>/shared-brain/bare/shared-brain.git

bun run packages/brainctl/src/main.ts init --profile worker --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts doctor --config ~/.config/brainstack/brainstack.yaml
```

Manually inspect existing harness instruction files if this is a reused machine:

```bash
ls -l ~/.codex/AGENTS.md ~/.claude/CLAUDE.md 2>/dev/null || true
```

If Brainstack printed manual merge commands because files already existed, apply those merges before treating the worker as ready.

## 5. Join From The Control Host

On the control host:

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts join-worker \
  --config ~/.config/brainstack/brainstack.yaml \
  --worker <worker-host> \
  --ssh-user <worker-user> \
  --harness codex
```

Merge the printed YAML into `~/.config/brainstack/brainstack.yaml`, pin OpenSSH host trust, apply runtime, and prove the worker path before restarting telemux:

```bash
bun run packages/brainctl/src/main.ts trust-worker --config ~/.config/brainstack/brainstack.yaml --worker <worker-host>
bun run packages/brainctl/src/main.ts upgrade --profile control --config ~/.config/brainstack/brainstack.yaml
bun run packages/brainctl/src/main.ts doctor --config ~/.config/brainstack/brainstack.yaml --workers
systemctl --user daemon-reload
systemctl --user restart telemux.service
```

## 6. Run Doctors

From the control host after restart, run the normal and deep doctors:

```bash
bun run packages/brainctl/src/main.ts doctor --config ~/.config/brainstack/brainstack.yaml --workers
bun run packages/brainctl/src/main.ts doctor --config ~/.config/brainstack/brainstack.yaml --workers --deep
```

Use `--deep` deliberately. It invokes the selected harness and proves bypass/yolo sudo behavior.
Then smoke `/workers` in Telegram so you see the same configured worker/harness/trust status through the live control plane.

## 7. First Telegram Worker Task

Create or bind a worker topic, then send a harmless task:

```text
/newctx worker-canary <worker-name> scratch
```

Then:

```text
Create a file in this scratch workspace named worker-canary.txt containing hostname, whoami, uname -a, pwd, date -u, and the current brainstack git commit if ~/brainstack exists. Record the file in .factory/ARTIFACTS.md and summarize the result.
```

Fetch it with:

```text
/artifacts send worker-canary.txt
```

## 8. Message Shape Canary

Before real work, verify Telegram behavior:

- Send two quick plain-text messages and confirm they coalesce into one prompt.
- Send a slash command immediately after plain text and confirm pending text flushes first.
- Paste an oversized prompt and inspect whether Telegram sends it as one message or multiple updates.

## 9. Legacy Cleanup Gate

Only after the worker canary is green:

```bash
systemctl --user list-units --all | grep -Ei 'openclaw|factory|telemux|brainstack' || true
systemctl list-units --all | grep -Ei 'openclaw|factory|telemux|brainstack' || true
find ~/.config ~/.local/state ~/.ssh -maxdepth 3 -iname '*openclaw*' -o -iname '*brainstack*'
```

Write a removal plan first. Remove only artifacts you can attribute to the legacy system or to Brainstack-owned manifests.
