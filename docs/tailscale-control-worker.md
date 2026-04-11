# Tailscale Control And Worker Setup

Brainstack uses normal OpenSSH over Tailscale for control-to-worker transport. Tailscale SSH is intentionally not the default.

## Target Shape

- Human laptops remain user-owned devices.
- Control hosts advertise `tag:brain`.
- Worker hosts advertise `tag:brain-worker`.
- The tailnet policy grants:
  - operator/admin identity to `tag:brain` on `tcp:22`, `tcp:443`, and ICMP
  - operator/admin identity to `tag:brain-worker` on `tcp:22` and ICMP
  - `tag:brain` to `tag:brain-worker` on `tcp:22` and ICMP
  - `tag:brain-worker` to `tag:brain` on `tcp:443` and ICMP
- The `ssh` section stays empty unless you intentionally enable Tailscale SSH.

Use [`../infra/tailscale/policy-fragment.example.json`](../infra/tailscale/policy-fragment.example.json) as the clean tag-based policy shape.

## Control Host Command

Some Tailscale client versions support `--advertise-tags` on `tailscale up` but not on `tailscale set`. Use a full `tailscale up` command so existing flags are not accidentally reset.

```bash
sudo tailscale up \
  --advertise-tags=tag:brain \
  --ssh=false \
  --accept-dns=true \
  --accept-routes=false \
  --shields-up=false \
  --netfilter-mode=on \
  --operator="$USER"
```

## Worker Host Command

```bash
sudo tailscale up \
  --advertise-tags=tag:brain-worker \
  --ssh=false \
  --accept-dns=true \
  --accept-routes=false \
  --shields-up=false \
  --netfilter-mode=on \
  --operator="$USER"
```

Enable normal OpenSSH on Linux workers:

```bash
sudo systemctl enable --now sshd.service
sudo ss -tulpen | grep ':22'
```

## Validation

Validate server-applied tags, not only local prefs:

```bash
tailscale whois brain-control
tailscale whois brain-worker
tailscale ping brain-worker
ssh brain-worker true
```

`tailscale debug prefs` shows local requested tags under `AdvertiseTags`. That is not the same as the control plane applying those tags. Use `tailscale status` to get the device's Tailscale IP, then run `tailscale whois <tailscale-ip>`. The `whois` output should show `Tags: tag:...` when the server-side tag is actually active.

## Caveats Found On Valkyrie/Erbine

- `tailscale set --advertise-tags=...` can fail on some client versions even though `tailscale up --advertise-tags=...` works.
- `tailscale up` with any flags must include the complete intended settings, otherwise Tailscale may reject the command or reset omitted preferences. Keep rendered commands in docs/config.
- If Tailscale SSH is enabled on a worker, port 22 can be intercepted by Tailscale SSH. With `"ssh": []`, the symptom is `tailscale: tailnet policy does not permit you to SSH to this node`, even when normal OpenSSH is installed.
- To use normal OpenSSH, disable Tailscale SSH on the worker with `sudo tailscale up ... --ssh=false` or `sudo tailscale set --ssh=false` when supported.
- If `tailscale ping` works but `ssh` times out, suspect grants/firewall/sshd.
- If SSH reaches OpenSSH but fails with `Permission denied (publickey)`, the network path is fixed and the remaining task is key installation.
- Tags may need dashboard approval or re-enrollment depending on tailnet policy and device state. Do not remove temporary host/IP fallback grants until `tailscale whois` confirms the expected tags.

## Temporary Recovery Rule

If a host is stuck behind Tailscale SSH and you cannot reach its local console, temporarily add a narrow Tailscale SSH rule for the operator, use it to disable Tailscale SSH on the host, then remove the rule.

```json
"ssh": [
  {
    "action": "accept",
    "src": ["group:brain-admins"],
    "dst": ["autogroup:self", "tag:brain-worker"],
    "users": ["operator"]
  }
]
```

This is a recovery path, not the product default.
