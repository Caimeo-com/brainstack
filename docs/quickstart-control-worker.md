# Quickstart: Control + Worker

The control profile runs `braind` and optional `telemux`. The worker profile does not run Telegram polling and does not receive the admin ingest token.

Read [`operator-preflight.md`](./operator-preflight.md) before installing the control profile. Telemux passes Telegram-originated work into the configured harness process; it is not a sandbox.

## Control Dry Run

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts smoke --profile control --config examples/control.yaml
```

## Worker Plan

Generate a worker join plan without touching erbine:

```bash
cd ~/brainstack
bun run packages/brainctl/src/main.ts join-worker --config examples/control.yaml --worker erbine
```

The worker transport default is normal OpenSSH over Tailscale:

```bash
ssh factory@erbine true
```

If that fails with a timeout while ping works, the likely blocker is Tailscale grants. The required grant shape is:

```json
{
  "src": ["tag:brain"],
  "dst": ["tag:brain-worker"],
  "ip": ["tcp:22", "icmp:*"]
}
```

## Tailscale Tags

- Control hosts advertise `tag:brain`.
- Worker hosts advertise `tag:brain-worker`.
- Human laptops should normally stay untagged and access control through user/group grants.
- Tailscale SSH is not the default; leave `"ssh": []` in policy unless intentionally enabling it later.

## Headless Control Enrollment

```bash
export TAILSCALE_AUTH_KEY=tskey-auth-...
sudo tailscale up --auth-key="${TAILSCALE_AUTH_KEY}" --hostname=valkyrie --advertise-tags=tag:brain --operator=swader
```

## Headless Worker Enrollment

```bash
export TAILSCALE_AUTH_KEY=tskey-auth-...
sudo tailscale up --auth-key="${TAILSCALE_AUTH_KEY}" --hostname=erbine --advertise-tags=tag:brain-worker --operator=factory
```

Use reusable/preapproved auth keys with restricted tags from the Tailscale dashboard. Do not store auth keys in git.
