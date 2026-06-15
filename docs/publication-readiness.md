# Publication Readiness

Before sharing brainstack with another machine or person:

- `~/brainstack` is a git repo with no real secrets.
- `bun install --frozen-lockfile` succeeds from the root lockfile.
- `find . -name bun.lock` returns only `./bun.lock`.
- `bun test` passes.
- `brainctl smoke --profile single-node` passes.
- `brainctl smoke --profile control` passes.
- `brainctl init` has been tested only on fresh install roots; use `brainctl lifecycle repair|upgrade` or the lower-level `brainctl upgrade`/`brainctl apply-runtime` for existing installs.
- `brainctl bootstrap-client` renders Codex, Claude, Cursor, SSH, env, and install artifacts from a source run and from the compiled binary.
- Product examples do not contain real tokens, chat ids, private keys, or personal repo data.
- Tailscale docs clearly separate Serve from Funnel.
- Worker docs use OpenSSH over Tailscale by default.
- Tailscale docs explain the difference between local requested tags and server-applied tags, and document the Tailscale SSH port-22 interception failure mode.
- Operator preflight docs explain passwordless sudo, Codex/Claude yolo mode, and the fact that telemux passes work into the configured harness rather than sandboxing it.
- The shared-brain content repo remains markdown/manifests/raw/proposals, not a vector DB or hidden memory service.
- Large binary policy is documented and tested.
- Backup and restore docs are sufficient without tribal knowledge.
- Routine docs explain deterministic `/cron create`, built-in routine installation, read-only update checks, and scheduler health checks.
- Multi-brain docs are honest about the current boundary: brain instances are hard boundaries, sections are retrieval boundaries, and project context uses `.brainstack.yaml`, `profiles.yaml`, local allow rules, source labels, and explicit cross-brain write policy.

## Release Artifacts

CLI release assets:

```bash
cd ~/brainstack
scripts/release.sh
```

The release script refuses dirty trees, runs `bun install --frozen-lockfile`, runs `bun test`, builds `dist/brainctl-darwin-arm64`, `dist/brainctl-darwin-x64`, `dist/brainctl-linux-arm64`, `dist/brainctl-linux-x64`, copies `dist/install.sh`, emits `dist/manifest.json`, and writes a source archive from `git archive`. Every binary and archive gets a `.sha256` sidecar. Release-built `dist/install.sh` is stamped with the release tag so a pinned installer URL downloads the matching `brainctl` binary by default.

The `brainctl` binary is compiled with `--no-compile-autoload-dotenv` and `--no-compile-autoload-bunfig`. Those flags keep release artifacts from inheriting local release-machine `.env` or `bunfig.toml` behavior. The binary embeds the client bootstrap templates so a Mac client can provision, doctor, init, and render `bootstrap-client` without a source checkout or Bun. `braind` and `telemux` are intentionally not compiled by default; they run from source under Bun so service behavior stays inspectable.

Generated source-run services use `bun --no-env-file run ...` for the same reason: service env must come from explicit runtime/secrets env files, not ambient repo `.env` loading.

The GitHub release workflow publishes the CLI installer assets without Apple signing secrets. The signed/notarized macOS menu app is an optional lane so Developer ID or notary failures do not block the one-command install release.

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

Review mode runs the focused proof set and records that the full `bun test` gate was skipped. Add `--full-test` when the handoff itself must include the full suite; forensic mode runs the full suite by default.

By default the script refuses dirty trees. Use `--allow-dirty` only when the handoff must include local, uncommitted work; in that mode `source/` is a working-tree snapshot built from tracked `HEAD` plus tracked local changes and untracked non-ignored files. Otherwise, `source/` is generated from `git archive` at `HEAD`. The exact source identity and dirty-tree status are recorded in `MANIFEST.txt`; the bundled `source/` tree is for portable review context, not byte-for-byte release provenance. It excludes compiled binaries, `dist/`, `.git`, dependency trees, Bun cache, env files, private keys, tokens, empty directories, and Finder/macOS junk. It fails if secrets-looking patterns are found before zipping.

Every review handoff should include `CHANGES.txt` for the exact base-to-head delta and `CLAIMS_AND_PROOF.md` for a short claim-to-evidence map. Use `--notes` for pass-specific context so a fresh-context reviewer does not have to infer what the bundle is proving.
