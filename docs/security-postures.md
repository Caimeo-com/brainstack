# Security Postures

Brainstack defaults to a trusted private mesh. The happy path does not require read tokens or IAM in front of local clones: machines read local git clones, and write continuity uses import/propose tokens plus the outbox.

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

Use `brainctl expose tailscale --config ~/.config/brainstack/brainstack.yaml --dry-run` to inspect the Serve config, then rerun with `--apply` on the control host.

## `guarded`

`guarded` is reserved for future broader internal/customer-facing controls. It is not a promise of first-class read-token enforcement today. Doctor reports this as a warning until a real guarded boundary exists.

