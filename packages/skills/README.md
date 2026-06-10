# Portable Skills

Portable Brainstack skills live here, outside the shared-brain content repo.

Seed skills:

- `brainstack`: end-to-end Brainstack product and operations workflow.
- `brain-curator`: organizer/admin synthesis and ingest discipline.
- `shared-brain-client`: client-side read/import/propose discipline.
- `remote-machine-ops`: safe OpenSSH-over-Tailscale worker operations.

These are templates for installation into a harness-specific skill directory. They are product artifacts, not canonical brain data.

Install the public Codex bundle from a source checkout or standalone `brainctl` binary:

```bash
brainctl skills install --target codex --profile client
brainctl skills install --target codex --profile operator
```

Profiles are convenience bundles:

- `client`: `shared-brain-client`, `brainstack`.
- `operator`: all public skills.
- `control`: `brainstack`, `brain-curator`, `remote-machine-ops`.
- `worker`: `shared-brain-client`, `remote-machine-ops`.

Invite enrollment also accepts `--skills-profile none` and installer `--skip-skills` when no Codex skills should be written. `none` is an enrollment opt-out, not a `brainctl skills install --profile` bundle.

Use `--skill NAME` for a smaller explicit install, `--all` for every public skill, `--dir DIR` for a non-default Codex skills root, and `--dry-run` to inspect the write plan.

Keep this package generic. Do not add private hostnames, personal paths, live tokens, Telegram chat ids, tailnet names, or customer-specific topology. Operators who need exact machine names or service paths should maintain a private local overlay skill outside this package.
