# Brainstack PMF And Approachability Report

Date: 2026-06-14

## Method

This report synthesizes a 20-persona fresh-eyes panel reviewing the public Brainstack page, with a light comparison against assistants and assistant-control products such as Hermes and OpenClaw. The panel is directional research, not statistical PMF. The useful signal is repeated friction across different buyer/user types.

Average panel score:

- Interest: 3.9 / 5
- Trust: 2.85 / 5

Core read: the product thesis lands. Brainstack is understood as a private, git-backed shared memory and control layer for Codex, Claude, Cursor, scripts, Telegram, and private workers. Conversion is blocked by the first-use story feeling too operator-heavy and not enough like a small, safe, real product someone can try today.

## Findings

### 1. Lead With The Smallest Safe First Use

Problem:
The current page exposes the whole architecture too early: `brainctl`, `braind`, Bun, Tailscale, control hosts, workers, Telegram, invites, systemd, yolo mode, proposals, and security posture. Technical personas understood the idea, but repeatedly asked what the smallest useful install is.

Potential fix:
Create a primary "Try locally in 5 minutes" path that needs no Telegram, no worker, no control host, and no Tailscale. It should install or run `brainctl`, initialize a sample/local brain, run `brainctl context`, run `brainctl search`, run `brainctl remember`, and show the resulting file/proposal artifact.

Estimated ROI:
Very high. This directly attacks the biggest conversion blocker and gives skeptics a low-risk way to touch the product.

Effort:
Medium. Requires a demo/sample brain path, docs, install command polish, and possibly a `brainctl demo init` helper.

Risk:
Low if it is read-only/propose-only by default. The main risk is creating another install path that drifts from real enrollment.

Acceptance criteria:
- A new user can complete the path in under 10 minutes on a clean Mac or Linux machine.
- No daemon, Telegram, Tailscale, or worker setup is required.
- The final output shows a passing health check and a visible memory/proposal artifact.

### 2. Reframe The Hero Around The Category

Problem:
The hero explains Brainstack accurately, but does not yet sharpen the category or contrast. Personas most strongly reacted to "shared memory for existing AI tools" and "git remains the brain."

Potential fix:
Lead with a sharper claim: "Git-backed shared memory for the AI tools you already use." Then immediately state the non-SaaS boundary: "Codex, Claude, Cursor, scripts, and workers share context without sending your brain to a hosted assistant."

Estimated ROI:
Very high. This makes Brainstack legible before architecture details.

Effort:
Low. Primarily copy and information architecture.

Risk:
Low. Avoid overstating automation; keep "memory/control layer" separate from "assistant replacement."

Acceptance criteria:
- A reader can explain the product in one sentence after the first viewport.
- Telegram and workers are not the first mental model.

### 3. Make Codex/Mac Onboarding The Main Wedge

Problem:
The strongest daily-driver persona was a Mac/Codex user who wants Brainstack behind the scenes, not a second harness. The page still makes that user parse control-host operations.

Potential fix:
Create a first-class "New Mac + Codex" onboarding path: install Brainstack as a normal user, paste an invite, install Codex hooks/skills, run doctor, and show the menubar app reporting green/yellow/red status.

Estimated ROI:
Very high. This aligns with the actual daily-driver workflow and turns the companion app into a product surface.

Effort:
Medium. Most primitives exist; this needs packaging, copy, screenshots, and a guided failure story.

Risk:
Medium. The path must fail open if Brainstack is unavailable and must not make hooks part of correctness.

Acceptance criteria:
- A fresh Mac user can ask Codex to install Brainstack from the public page and Codex can determine role/prerequisites.
- Missing Tailscale, invite, or permissions produce deterministic stop conditions.
- The menubar app shows understandable status after enrollment.

### 4. Show The Evidence-To-Proposal Memory Loop

Problem:
Most competitors can claim "memory." Brainstack's stronger claim is curated memory: raw transcripts are evidence, not truth. The page does not yet make this distinction vivid enough.

Potential fix:
Add a concrete visual/demo loop: a session produces raw evidence, Brainstack stores provenance, the curator creates a scoped proposal, the operator approves/rejects/enriches it, and the wiki becomes better future context.

Estimated ROI:
High. This is a defensible product moat and answers prompt-injection/memory-rot concerns.

Effort:
Medium. Requires product screenshots or a short recording plus copy.

Risk:
Low. The main risk is making the flow look like mandatory manual work; show approval modes and automation boundaries clearly.

Acceptance criteria:
- The page distinguishes raw import, proposal, approved/applied wiki change, and rejected/stale item.
- A user understands why Brainstack memory is safer than blindly appending notes to prompt context.

### 5. Replace Placeholders And Simulated Proof

Problem:
Personas repeatedly mentioned `vX.Y.Z`, simulated product views, and dense runbooks as trust reducers. These make the product feel unreleased even when the code is real.

Potential fix:
Use real release tags, real terminal output, real screenshots, and a short recording of the working loop. If a step is private-beta only, label it plainly instead of showing placeholder commands.

Estimated ROI:
High. Trust is currently lower than interest.

Effort:
Low to medium.

Risk:
Low. Be careful not to publish private hostnames, tokens, or topology.

Acceptance criteria:
- No public quickstart command uses a placeholder release tag.
- The page has at least one real install/doctor/search/proposal proof artifact.

### 6. Split Landing Page From Operator Runbook

Problem:
The page currently behaves like a landing page, product manual, install runbook, and security model at the same time. That is useful for operators but costly for conversion.

Potential fix:
Restructure into: first-use story, concrete demo, role paths, trust summary, then links to operator docs. Keep deep control-host setup in docs.

Estimated ROI:
High. Reduces cognitive load without removing rigor.

Effort:
Medium.

Risk:
Low. The operator runbook should remain complete, just not be the main conversion path.

Acceptance criteria:
- First viewport and first two sections are readable by a non-operator.
- Full install docs remain one click away.

### 7. Add A Compact Security And Trust Boundary

Problem:
Security-conscious personas liked the honesty but still wanted concrete answers: read boundary, write boundary, auth, tokens, Telegram risk, prompt injection, auditability, revocation, backup/restore, uninstall, and fail-open hooks.

Potential fix:
Add a public "Trust model" section/page with a table of what can read, what can write, what is proposed-only, what needs admin tokens, what Telegram can do, and what happens when Brainstack is offline.

Estimated ROI:
High for teams, security leads, and skeptical OSS users.

Effort:
Medium.

Risk:
Medium if the docs overpromise future guarded mode. Keep current `trusted-tailnet` constraints explicit.

Acceptance criteria:
- A team lead can identify whether Brainstack is safe for a small pilot without reading source.
- Hooks, daemon, imports, proposals, and Telegram each have explicit failure behavior.

### 8. Productize The Menubar App As A Control Surface

Problem:
The companion app is a major approachability improvement, but it is not yet clearly positioned as the friendly face of Brainstack.

Potential fix:
Show the menubar app prominently. It should explain status, show proposal counts, open the wiki, run doctor, repair hooks, refresh skills, and eventually trigger updates across machines.

Estimated ROI:
High for Mac/Codex adoption.

Effort:
Medium.

Risk:
Medium. Operator actions must stay explicit and token-gated; status checks must not hang or create false degradation.

Acceptance criteria:
- A yellow/red state explains the action needed.
- A non-operator can see "Brainstack is working" without reading logs.

### 9. Add Persona-Specific Paths

Problem:
Different personas wanted different first wins: solo devs want local memory, SREs want incident continuity, team leads want ROI/security, self-hosters want privacy, and non-developers want guided Mac setup.

Potential fix:
Add role cards with one concrete outcome each:
- Solo dev: "Stop re-explaining your project to Codex."
- SRE: "Turn incident traces into reviewed runbook proposals."
- Team lead: "Shared agent context without SaaS memory."
- Self-hoster: "Own the git repo and tailnet boundary."
- Mac/Codex user: "Install once, let hooks and the menubar keep it fresh."

Estimated ROI:
Medium to high. This helps users self-select without broadening the core product.

Effort:
Low to medium.

Risk:
Low.

Acceptance criteria:
- Each role path has one CTA and one success state.
- Advanced surfaces such as Telegram and workers appear only where they are relevant.

### 10. Build Growth Loops Around Import And Shareable Proof

Problem:
The most natural growth motion is importing existing memories/skills and proving immediate continuity. The current page does not yet make that a viral or shareable loop.

Potential fix:
Make imports a major CTA: import Codex memory, Claude skills, Cursor rules, existing markdown docs, and shared skills. Add a redacted `brainctl doctor --share` or "readiness report" that users can post when asking for help or showing setup.

Estimated ROI:
Medium to high. Imports reduce cold-start cost and create "look what it found" moments.

Effort:
Medium.

Risk:
Medium. Import discovery must be plan-only by default and avoid publishing private material without explicit apply.

Acceptance criteria:
- Import commands explain what will become globally available before writing.
- Shareable reports redact tokens, hostnames where appropriate, and local paths if requested.

## Recommended Execution Order

1. Fix immediate UX papercuts in Telegram/context binding and menubar status copy.
2. Add the smallest safe local demo path.
3. Rewrite the Brainstack page around category, demo, and role paths.
4. Add real proof assets and remove placeholders.
5. Publish the trust model and Mac/Codex install path.
6. Make proposal curation visually obvious as the product moat.
7. Add growth/import/report loops after the first-use path is clean.

