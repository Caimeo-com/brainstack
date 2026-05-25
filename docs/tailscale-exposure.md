# Tailscale Exposure

Brainstack's preferred exposure model is loopback `braind` plus Tailscale Serve. Do not bind `braind` directly to a public interface for normal use.

```bash
brainctl expose tailscale --config ~/.config/brainstack/brainstack.yaml --dry-run
brainctl expose tailscale --config ~/.config/brainstack/brainstack.yaml --apply
```

The command renders a Tailscale Serve config that proxies `https://<tailnet-host>/` to `http://127.0.0.1:<brain-port>`.

Keep Tailscale SSH disabled for Brainstack's worker model unless you deliberately change the architecture. Workers use normal OpenSSH over Tailscale so Git freshness and harness dispatch share the same predictable SSH path.

Minimal grants shape for a control plus workers:

```json
{
  "grants": [
    { "src": ["group:brain-admins"], "dst": ["tag:brain"], "ip": ["tcp:22", "tcp:443", "icmp:*"] },
    { "src": ["group:brain-admins"], "dst": ["tag:brain-worker"], "ip": ["tcp:22", "icmp:*"] },
    { "src": ["tag:brain"], "dst": ["tag:brain-worker"], "ip": ["tcp:22", "icmp:*"] },
    { "src": ["tag:brain-worker"], "dst": ["tag:brain"], "ip": ["tcp:22", "tcp:443", "icmp:*"] }
  ]
}
```

