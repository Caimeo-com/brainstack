# Publication Readiness

Before sharing brainstack with another machine or person:

- `~/brainstack` is a git repo with no real secrets.
- `bun install --frozen-lockfile` succeeds from the root lockfile.
- `find . -name bun.lock` returns only `./bun.lock`.
- `bun test` passes.
- `brainctl smoke --profile single-node` passes.
- `brainctl smoke --profile control` passes.
- `brainctl init` has been tested only on fresh install roots; use `brainctl upgrade` or `brainctl apply-runtime` for existing installs.
- `brainctl bootstrap-client` renders Codex, Claude, Cursor, SSH, env, and install artifacts.
- Product examples do not contain real tokens, chat ids, private keys, or personal repo data.
- Tailscale docs clearly separate Serve from Funnel.
- Worker docs use OpenSSH over Tailscale by default.
- Tailscale docs explain the difference between local requested tags and server-applied tags, and document the Tailscale SSH port-22 interception failure mode.
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

The release script refuses dirty trees, runs `bun install --frozen-lockfile`, runs `bun test`, builds `dist/brainctl`, and emits a source archive from `git archive`.

The `brainctl` binary is compiled with `--no-compile-autoload-dotenv` and `--no-compile-autoload-bunfig`. Those flags keep release artifacts from inheriting local release-machine `.env` or `bunfig.toml` behavior. `braind` and `telemux` are intentionally not compiled by default; they run from source under Bun so service behavior stays inspectable.

Generated source-run services use `bun --no-env-file run ...` for the same reason: service env must come from explicit runtime/secrets env files, not ambient repo `.env` loading.

Cross-compilation can be added later with Bun compile targets after target support is verified on the release host.

## Review Handoff Bundles

Review bundles are not release artifacts:

```bash
cd ~/brainstack
scripts/handoff.sh --mode review --base <last-reviewed-commit> --notes /tmp/handoff-notes.md --out /tmp
```

For a larger local-audit bundle:

```bash
cd ~/brainstack
scripts/handoff.sh --mode forensic --out /tmp
```

The handoff script uses exactly one source representation, `source/`, generated from `git archive` at `HEAD`. It excludes compiled binaries, `dist/`, `.git`, dependency trees, Bun cache, env files, private keys, tokens, empty directories, and Finder/macOS junk. It fails if secrets-looking patterns are found before zipping.

Every review handoff should include `CHANGES.txt` for the exact base-to-head delta and `CLAIMS_AND_PROOF.md` for a short claim-to-evidence map. Use `--notes` for pass-specific context so a fresh-context reviewer does not have to infer what the bundle is proving.
