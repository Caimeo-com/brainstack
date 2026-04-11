# Publication Readiness

Before sharing brainstack with another machine or person:

- `~/brainstack` is a git repo with no real secrets.
- `bun test` passes.
- `brainctl smoke --profile single-node` passes.
- `brainctl smoke --profile control` passes.
- `brainctl init` has been tested only on fresh install roots; use `brainctl upgrade` or `brainctl apply-runtime` for existing installs.
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
scripts/release.sh
```

The release script refuses dirty trees, runs `bun test`, builds `dist/brainctl`, and emits a source archive from `git archive`.

The `brainctl` binary is compiled with `--no-compile-autoload-dotenv` and `--no-compile-autoload-bunfig`. Those flags keep release artifacts from inheriting local release-machine `.env` or `bunfig.toml` behavior. `braind` and `telemux` are intentionally not compiled by default; they run from source under Bun so service behavior stays inspectable.

Cross-compilation can be added later with Bun compile targets after target support is verified on the release host.
