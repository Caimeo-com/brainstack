# Security Postures

Brainstack defaults to a trusted private mesh. The happy path does not require read tokens or IAM in front of local clones: machines read local git clones, and write continuity uses import/propose tokens plus the outbox.

In `trusted-tailnet` mode, anyone who can reach the `braind` service on the private network is trusted to read the brain. Tailscale, VPN routing, firewall policy, and grants are the security boundary in this posture. Do not expose trusted-tailnet mode to the public internet.

## `local`

Use `security.posture: local` when `braind` is only for the current machine. It must bind loopback (`security.bindHost: 127.0.0.1` or `::1`). `brainctl doctor` fails if a local posture binds a non-loopback address.

## `trusted-tailnet`

This is the default. `braind` should still bind loopback, and exposure should happen through Tailscale Serve:

```yaml
security:
  posture: trusted-tailnet
  bindHost: 127.0.0.1
  trustedExposure: tailscale-serve
```

Use `brainctl expose tailscale --config ~/.config/brainstack/brainstack.yaml --dry-run` to inspect the Serve config, then rerun with `--apply` on the control host. `brainctl doctor` reports that read auth is disabled by design, the trust boundary is private network reachability, and Tailscale Serve exposure is either declared or absent.

For non-Tailscale private networks, set `trustedExposure: vpn` or `trustedExposure: manual` deliberately and keep `bindHost` loopback unless you have verified the surrounding network boundary yourself.

## `guarded`

`guarded` is reserved for future broader internal/customer-facing controls. It is not a promise of first-class read-token enforcement today. Doctor reports this as a warning until a real guarded boundary exists.
