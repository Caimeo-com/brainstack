# Machine Guidance

- This machine is the Clawdex control plane.
- Prefer Bun and simple shell scripts over heavier runtimes.
- Prefer inspectable files over hidden state.
- Avoid Docker unless absolutely necessary.
- Any repo-bound task should use per-context worktrees.
- Global durable machine guidance belongs in `~/.codex/AGENTS.md`.
- Context and task state belongs in `.factory/` files inside each worktree, not in global memory.
- When working on `~/clawdex`, keep docs current with reality.
