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

Shared-brain skill imports use a separate path:

```bash
brainctl import skill ~/.codex/skills/brainstack/SKILL.md --config ~/.config/brainstack/brainstack.yaml
brainctl import skill https://github.com/example/skill-repo --config ~/.config/brainstack/brainstack.yaml
brainctl import skills --config ~/.config/brainstack/brainstack.yaml
brainctl import skills --config ~/.config/brainstack/brainstack.yaml --apply
brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml
brainctl skills refresh --config ~/.config/brainstack/brainstack.yaml --target codex
brainctl skills doctor --dir ~/.codex/skills --check-remote
```

Local `SKILL.md` inputs package the whole parent folder. Directory inputs must contain `SKILL.md`. URL inputs fetch a raw skill file or clone a repository/tree URL. Raw-file URL imports block private network targets by default; use `--allow-private-url` only for trusted private sources. `lifecycle repair` is the routine installed-machine repair path; narrow `skills refresh` installs validated shared skill packages from the local shared-brain clone and refuses to overwrite unmarked local skill directories unless `--force` is passed.

`import skills` is the no-side-effect bulk planner. It scans the current directory plus default Codex, Claude, and Cursor skill roots, reports which skills would become global shared-brain imports, notes duplicate and already-current skills, and writes only when `--apply` is passed.

Harness hooks can run refresh in the background:

```bash
brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml --dry-run
brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml
brainctl hooks install --target all --config ~/.config/brainstack/brainstack.yaml
brainctl hooks status --target all
brainctl hooks remove --target all
```

Hooks are fail-open convenience integration; they must not be required for shared-brain correctness. Use `hooks install` directly only when you want to touch hook config without the rest of lifecycle repair.

Client and worker machines can also run the local daemon:

```bash
brainctl lifecycle repair --config ~/.config/brainstack/brainstack.yaml
brainctl daemon install --config ~/.config/brainstack/brainstack.yaml
brainctl daemon install --config ~/.config/brainstack/brainstack.yaml --start
brainctl daemon status --config ~/.config/brainstack/brainstack.yaml
brainctl daemon once --config ~/.config/brainstack/brainstack.yaml
```

`lifecycle repair` is the normal path after enrollment. `brainstackd` is `brainctl daemon run`, not a separate binary. It keeps the local shared-brain clone, outbox, and shared skills fresh in the background; hooks read its status and stay fail-open if it is unavailable.

Keep this package generic. Do not add private hostnames, personal paths, live tokens, Telegram chat ids, tailnet names, or customer-specific topology. Operators who need exact machine names or service paths should maintain a private local overlay skill outside this package.
