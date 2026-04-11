# Publication Readiness

Before sharing brainstack with another machine or person:

- `~/brainstack` is a git repo with no real secrets.
- `bun test` passes.
- `brainctl smoke --profile single-node` passes.
- `brainctl smoke --profile control` passes.
- `brainctl bootstrap-client` renders Codex, Claude, Cursor, SSH, env, and install artifacts.
- Product examples do not contain real tokens, chat ids, private keys, or personal repo data.
- Tailscale docs clearly separate Serve from Funnel.
- Worker docs use OpenSSH over Tailscale by default.
- Operator preflight docs explain passwordless sudo, Codex/Claude yolo mode, and the fact that telemux passes work into the configured harness rather than sandboxing it.
- The shared-brain content repo remains markdown/manifests/raw/proposals, not a vector DB or hidden memory service.
- Large binary policy is documented and tested.
- Backup and restore docs are sufficient without tribal knowledge.

## Release Artifacts

Current-platform binary:

```bash
cd ~/brainstack
bun build packages/brainctl/src/main.ts --compile --outfile dist/brainctl
```

Cross-compilation can be added later with Bun compile targets after target support is verified on the release host.
