# Brainstack ambient harness integration report

Generated: 2026-06-01T21:38:07Z

## Executive verdict

Brainstack has become strong enough as an installable private mesh assistant backbone, but the current handoff still does **not** solve the core product promise: existing harnesses should automatically consult Brainstack and quietly return useful session knowledge without the user having to remember to run `brainctl`.

The current implementation mostly installs **prompt-level guidance**. That is useful, but it is not ambient. It tells Codex, Claude, and Cursor to run `brainctl context`, `brainctl search`, and `brainctl remember`; it does not install a durable session/capture protocol, it does not make a background daemon consume harness events, and it does not reliably capture end-of-session transcripts.

The next product frontier should be named explicitly:

> Brainstack Ambient Layer: one approved install action per harness, then Brainstack quietly runs behind Codex, Claude, Cursor, Codex App, and remote worker-launched harnesses.

The architecture should be:

```text
User uses Codex / Claude / Cursor / Codex App / Telemux normally
        |
Harness-native instructions + skills + hooks where available
        |
brainctl hook/capture commands, called by the harness or watcher
        |
brainstackd local daemon + private local spool
        |
Outbox + idempotent import/propose to braind
        |
Control-host curator routine proposes wiki updates from raw/checkpoint material
```

Telemux should remain transport only. If a task enters through Telegram and runs on yoda, the yoda Codex/Claude instance should capture through the same Brainstack ambient adapter that a local yoda session would use. Telemux may pass neutral metadata, but it should not own memory semantics.

## What I inspected

Fresh bundle: `/mnt/data/handoff-brain.zip`

Fresh extraction path used for this review:

```text
/mnt/data/brainstack_handoff_fresh2/handoff-20260601T211531Z
```

Relevant state from the bundle:

- `packages/client-bootstrap/README.md`
- `packages/client-bootstrap/codex-shared-brain.include.md`
- `packages/client-bootstrap/codex-global-AGENTS.md`
- `packages/client-bootstrap/claude-user-CLAUDE.md`
- `packages/client-bootstrap/claude-hooks-example.json`
- `packages/client-bootstrap/cursor-user-rule.md`
- `packages/brainctl/src/main.ts`
- `apps/telemux/src/dispatcher.ts`
- `docs/quickstart-client-macos.md`
- `docs/routines.md`
- `docs/multi-brain.md`
- `packages/skills/README.md`
- `packages/skills/brainstack/SKILL.md`
- `packages/skills/shared-brain-client/SKILL.md`
- command proof artifacts under `command-outputs/`

I did not run the full Bun test suite locally; Bun is not available in this review environment, and the bundle itself states the full suite was skipped for this handoff while focused proof tests were included. This report is therefore a source/design audit plus targeted bundle inspection, not a fresh execution test.

## Current implementation status

### Strong parts

Brainstack now has real product foundations:

- one-line-ish Mac client polish and compiled-client positioning
- client bootstrap for Codex, Claude, Cursor
- multi-brain context and trusted project profiles
- `brainctl context`, `search`, `remember`, `allow`, `outbox`
- trusted-tailnet posture checks
- source-labelled retrieval and cross-brain write warnings
- Telegram worker routing and file sending
- routines, including `update-check`, `brain-curator`, and `daily-checkin`
- good private-network/operator docs

This is now past the “can it work on yoda?” phase.

### Critical gap

The current bootstrap files are still mostly advisory:

```md
Before substantial work in a repository, run `brainctl context --repo .`.
```

That is not enough. It depends on the model choosing to obey and remember the instruction. It also does not solve session lifecycle, raw transcript capture, dedupe, idle close, app-level sessions, or semantic anti-spam.

Current files confirm this:

- Codex gets an `AGENTS.md` symlink or append instruction.
- Claude gets `CLAUDE.md` with an `@.../AGENTS.shared-client.md` import.
- Cursor gets a rule file.
- Claude hook example is only an example; it pulls the shared brain on `SessionStart`, and on `Stop` merely echoes a reminder.
- There is no `brainctl session` command.
- There is no `brainctl harness install` command.
- There is no installed `brainstackd` daemon.
- There is no automatic hook/spool/upload pipeline.
- There is no `brainctl skills sync` command yet.

### Current command surface observed

`brainctl` usage currently includes:

```text
init
provision
upgrade
apply-runtime
doctor
updates
expose tailscale
backup
restore
render
bootstrap-client
join-worker
trust-worker
worker-cache
repo-lock / locks
rotate-token
telegram send-file
import-text
propose
context
search
remember
allow repo
outbox
destroy
migrate-current-install
smoke
```

It does **not** include:

```text
setup
ui
skills
session
capture
harness install
harness status
harness doctor
daemon
```

Those are the missing surfaces for ambient behavior.

## Product principle to lock in

Brainstack should not become another harness. The user should not have to run `brainctl focus`, `brainctl session start`, or `brainctl remember` manually during normal work.

The user flow should be:

```text
Install Brainstack.
Approve “Install into Codex / Claude / Cursor”.
Keep using existing tools normally.
Brainstack quietly handles context, capture, outbox, and curation.
```

The CLI is still necessary, but as plumbing:

```text
brainctl harness install ...       # called by setup wizard or menubar
brainctl capture event ...         # called by hooks
brainctl daemon run                # launched by LaunchAgent/systemd
brainctl capture flush             # called by daemon, doctor, or UI
brainctl skills sync               # called by setup/context/daemon
```

## Correct architecture

### Three planes

#### 1. Instruction plane

Purpose: teach the model what Brainstack is and how to behave.

Mechanisms:

- Codex: `AGENTS.md`, Codex skills, optional Codex plugin later
- Claude: `CLAUDE.md`, Claude skills, Claude settings/hooks
- Cursor: user rules, project `.cursor/rules`, repo `AGENTS.md`
- Project repos: `.brainstack.yaml`, `.agents/skills`, optional `AGENTS.md`

This layer should say:

```md
You are running on a Brainstack-enabled machine.
At session start, Brainstack may inject context automatically.
Use Brainstack search for durable context when useful.
Mark durable decisions, fixes, blockers, artifacts, and lessons in your final response.
Do not directly mutate canonical wiki pages.
Do not dump every turn into memory.
```

But this layer is **not** reliable enough to own capture.

#### 2. Lifecycle/capture plane

Purpose: reliably observe harness events without trusting the model to remember.

Mechanisms:

- Codex hooks where available
- Claude Code hooks
- Cursor rules plus local watcher/adapter where hooks are weak or absent
- Codex App app-server integration or watcher, if reliable local hooks are not exposed by the desktop app
- Process/session watchers as fallback

This layer writes local events into a private spool. It should be fast, non-blocking, and safe.

#### 3. Publish/curation plane

Purpose: decide when local events become Brainstack imports/proposals.

Mechanisms:

- `brainstackd` daemon drains spool
- outbox queues when offline
- idempotency prevents duplicate imports
- curator routine turns raw/checkpoint artifacts into sourced proposals
- wiki/canon updates remain proposal-first

This layer prevents spam.

## Anti-spam model

Do not ask the LLM to determine whether a conversation is “done.” Treat “done” as a lifecycle heuristic over sessions and generations.

### Core concepts

```text
session_id       harness-native id where available
conversation_id  stable Brainstack id for a project/task/thread
turn_id          harness-native turn id where available
generation       increments after idle close, archive, explicit close, or session end
checkpoint_seq   monotonically increasing sequence per generation
```

### Publish units

Brainstack should publish three kinds of artifacts:

1. **Checkpoint**
   - small summary of a meaningful durable event
   - can happen during active work
   - rate-limited
   - idempotent

2. **Raw transcript artifact**
   - one per generation/session close
   - compressed if large
   - raw import, not auto-wiki
   - source-labelled and sensitivity-tagged

3. **Curation proposal**
   - generated later by control-host curator
   - cites raw/checkpoint IDs
   - human/agent can review before merge

### Gates

Publish a checkpoint when one of these fires:

- explicit user phrase: “remember this”, “save this”, “publish this”
- meaningful durable event: bug fixed, decision made, blocker found, artifact created, test result changed
- stop hook final response contains a durable summary marker
- every N minutes or N turns at most, if work is long-running
- repo diff changed materially since last checkpoint

Publish a raw transcript artifact when:

- Claude `SessionEnd` fires
- Codex CLI session/process exits
- Codex App thread is archived/closed, or app-server says thread closed
- idle timeout expires, e.g. 30–60 minutes with no new user prompt
- explicit close/done/archive signal appears
- telemux-launched harness process exits, but capture still belongs to the harness adapter, not telemux logic

Never publish:

- every user prompt
- every assistant response
- the whole transcript after every Stop hook
- direct canonical wiki edits from a client harness

### Idempotency keys

Use stable keys:

```text
checkpoint: brain_id + harness + session_id + generation + checkpoint_seq + content_sha256
raw:        brain_id + harness + session_id + generation + transcript_sha256
curation:   brain_id + curation_run_id + source_artifact_ids_hash
```

## Harness-specific design

### Codex CLI

Codex now has a strong official hook surface.

Relevant capabilities:

- Codex looks for hooks in `~/.codex/hooks.json`, `~/.codex/config.toml`, `<repo>/.codex/hooks.json`, and `<repo>/.codex/config.toml`.
- Hook input includes `session_id`, `transcript_path`, `cwd`, `hook_event_name`, model, permission mode, and turn ids for turn-scoped hooks.
- `SessionStart` can emit additional developer context.
- `Stop` includes `last_assistant_message` and can return JSON.
- Codex docs warn that `transcript_path` is convenient but not a stable interface.

Recommended Brainstack install:

```text
~/.codex/AGENTS.md                         managed include or marked block
~/.codex/hooks.json                        managed Brainstack lifecycle hooks
~/.agents/skills/brainstack -> symlink     Brainstack skill
~/.agents/skills/shared-brain-client -> symlink
```

Suggested hook behavior:

- `SessionStart startup|resume|compact`
  - call `brainctl capture event --harness codex --event SessionStart --stdin-json`
  - fast path returns dynamic context via stdout JSON/additionalContext
  - never dump whole brain; return concise repo/profile status and command hints

- `UserPromptSubmit`
  - call `brainctl capture event --harness codex --event UserPromptSubmit --stdin-json`
  - spool prompt metadata and maybe prompt hash/length
  - do not upload raw prompt immediately unless explicit memory phrase detected

- `Stop`
  - call `brainctl capture event --harness codex --event Stop --stdin-json`
  - spool `last_assistant_message`, status, transcript path pointer, and changed-files summary
  - do not block continuation; Brainstack should not be a hidden taskmaster by default

- optional `PostToolUse`
  - use only for coarse “files changed” and artifact hints
  - do not record every tool payload by default

Important Codex App note:

Codex skills are available in CLI, IDE extension, and Codex app, which helps with instruction/skill propagation. But reliable Codex App transcript/session capture should not rely only on AGENTS.md. For a deep Codex App integration, use Codex app-server as an optional backend because it provides conversation history, approvals, streamed events, thread listing, thread reading, and archive/closed events. Brainstack should stay above this; app-server is a leaf adapter, not the Brainstack control plane.

### Claude Code

Claude Code has the best fit for ambient capture.

Relevant capabilities:

- User settings live at `~/.claude/settings.json`.
- Claude memory/instructions live at `~/.claude/CLAUDE.md`, project `CLAUDE.md`, or `.claude/CLAUDE.md`.
- Claude hooks receive JSON on stdin.
- `SessionStart` fires on startup/resume/clear/compact and is intended for dynamic context.
- `UserPromptSubmit` receives the prompt text.
- `Stop` receives `last_assistant_message`, background tasks, and session crons, which helps distinguish “done” from “paused.”
- `SessionEnd` receives `session_id`, `transcript_path`, `cwd`, and reason; it cannot block session termination.
- Async hooks exist and allow background work without blocking the user.
- Command hooks run with full user permissions, so Brainstack must install them only after explicit user approval.

Recommended Brainstack install:

```text
~/.claude/CLAUDE.md                         managed import / marked block
~/.claude/settings.json                     managed hooks block, with backup
~/.claude/skills/brainstack                 if supported/detected
~/.claude/skills/shared-brain-client        if supported/detected
```

Suggested hook behavior:

- `SessionStart`
  - synchronous, fast
  - returns a small Brainstack context block
  - should never run expensive search or curation

- `UserPromptSubmit`
  - records prompt metadata and explicit-memory triggers
  - may return brief extra context only when cheap

- `Stop`
  - async preferred
  - captures final assistant message, status, changed-files summary, and background-task state
  - checkpoint only if meaningful

- `SessionEnd`
  - best raw transcript close signal
  - queues/compresses the transcript as raw artifact
  - should not perform network-heavy work directly; hand to daemon/spool

### Cursor

Cursor is mostly a prompt/rules integration, not yet a dependable capture surface.

Relevant capabilities:

- Cursor rules provide persistent prompt context.
- Project rules live in `.cursor/rules`.
- User rules are global.
- `AGENTS.md` is a supported simple alternative.
- Legacy `.cursorrules` is deprecated.

Recommended Brainstack install:

```text
User rule or managed rule entry: “Brainstack enabled…”
Project `.cursor/rules/brainstack.mdc` where repo opts in
Optional root `AGENTS.md` include for simple projects
brainstackd watcher for local session artifacts only if path is known and stable
```

Do not overpromise Cursor capture until there is a stable hook/session API. For now Cursor can be “context-aware” via rules and skills, with capture via explicit final-answer conventions or local watcher if reliable.

### Codex App

Codex App matters because the user specifically wants desktop app integration.

Recommended progression:

1. **Minimum viable**
   - install Brainstack skills into Codex’s supported skill locations
   - install AGENTS.md guidance
   - rely on `brainctl context` instructions for behavior

2. **Better**
   - add Codex hooks if the desktop app uses the same local Codex hook layer in practice
   - verify empirically and have `brainctl harness doctor codex-app` report real status

3. **Best**
   - optional Codex app-server sidecar integration
   - list/read threads, watch status/closed/archive events, and import raw thread artifacts on close/idle/archive

Do not assume Codex App equals Codex CLI for hooks until tested. Treat app-server as the reliable path for deep app integration.

### Telemux

Telemux must stay thin.

Current source still has `importRunNotesToBrain()` inside `apps/telemux/src/dispatcher.ts`, importing `.factory/SUMMARY.md` and `.factory/ARTIFACTS.md` with `source_harness: "telemux"`. That was understandable earlier, but it is now architecturally awkward.

Recommended path:

- keep existing import as a compatibility fallback for now
- stop adding new Brainstack memory semantics to telemux
- pass neutral metadata to the launched harness environment:

```text
BRAINSTACK_INVOCATION_SOURCE=telemux
BRAINSTACK_INVOCATION_ID=...
BRAINSTACK_TELEGRAM_CHAT_ID=...
BRAINSTACK_TELEGRAM_THREAD_ID=...
BRAINSTACK_CONTEXT_SLUG=...
BRAINSTACK_TARGET_MACHINE=...
```

- let the harness adapter/daemon do the same capture it would do for local sessions
- eventually mark telemux run-note import as legacy/simple-run fallback

## `brainstackd`: the missing daemon

Brainstack needs a local daemon/service on every Brainstack-enabled machine.

### Responsibilities

`brainstackd` should:

- run as the current user
- be installed by LaunchAgent on macOS and systemd user service on Linux
- own local private spool processing
- consume hook events written by `brainctl capture event`
- run idle timers
- compress large raw artifacts
- redact obvious secrets before upload
- enforce anti-spam policy
- flush outbox opportunistically
- sync skills periodically or when source changes
- expose a local status socket/API for menubar and `brainctl status`

### Non-responsibilities

`brainstackd` should not:

- be a model runtime
- replace Codex/Claude/Cursor
- make canonical wiki edits
- auto-install harnesses or mutate sudo policy
- become a remote unauthenticated control server

### State layout

Suggested local paths:

```text
~/.local/state/brainstack/spool/events/
~/.local/state/brainstack/spool/sessions/
~/.local/state/brainstack/spool/transcripts/
~/.local/state/brainstack/outbox/
~/.local/state/brainstack/daemon/brainstackd.sock
~/.local/state/brainstack/daemon/brainstackd.log
~/.config/brainstack/harnesses.json
~/.config/brainstack/managed-harness-artifacts.json
~/.config/brainstack/skills-manifest.json
```

Permissions:

```text
directories: 0700
files with payloads: 0600
logs: no raw payloads by default
```

### Event file format

`brainctl capture event` should write one JSON file per event, atomically:

```json
{
  "schema": "brainstack.capture-event.v1",
  "eventId": "sha256:...",
  "createdAt": "2026-06-01T...Z",
  "machineId": "macbook",
  "harness": "codex",
  "harnessSurface": "cli|app|ide|telemux-launched",
  "eventName": "SessionStart|UserPromptSubmit|Stop|SessionEnd|ThreadClosed|ThreadArchived|ProcessExit",
  "sessionId": "...",
  "turnId": "...",
  "cwd": "/path/to/repo",
  "repoRoot": "/path/to/repo",
  "contextProfile": "...",
  "brainIds": ["personal", "work"],
  "sourceLabels": [],
  "payloadPolicy": {
    "containsRawUserText": true,
    "containsRawAssistantText": true,
    "containsTranscriptPath": true
  },
  "payload": {
    "promptText": "optional, depending policy",
    "lastAssistantMessage": "optional",
    "transcriptPath": "optional",
    "summary": "optional"
  }
}
```

## Menubar app

A macOS menubar app is the right UX surface, but it should be a thin controller, not where product logic lives.

### Menubar responsibilities

- show `brainstackd` status
- show current brain health
- show outbox/spool counts
- detect installed harnesses
- show integration state:
  - Codex: instructions / hooks / skills
  - Claude: CLAUDE.md / settings hooks / skills
  - Cursor: rules / AGENTS.md / skills
  - Codex App: app-server/skill status
- offer one-click “Install into Codex” / “Install into Claude” / “Install into Cursor”
- offer pause/resume capture
- offer privacy mode: pause raw transcript upload, checkpoint-only mode
- flush outbox
- sync skills
- open logs/config/UI
- show last captured session and last curator proposal

### Menubar implementation rule

The app should call stable JSON commands:

```bash
brainctl harness status --json
brainctl harness install codex --ambient --json
brainctl harness uninstall codex --json
brainctl daemon status --json
brainctl capture status --json
brainctl outbox status --json
brainctl skills status --json
brainctl skills sync --json
```

Do not duplicate hook detection, config editing, or policy logic in Swift/SwiftUI. The app should be replaceable.

### Suggested user flow

```text
Brainstack detected Codex, Claude, and Cursor.

[Install into Codex]   Not installed
[Install into Claude]  Instructions installed, hooks missing
[Install into Cursor]  Rule installed

Brainstack will add managed instruction/hook files and can remove them later.
It will not change your model provider auth, sudo settings, or shell profile.
```

## Skills

Brainstack skills currently exist as product artifacts, but the bundle still lacks a complete sync/install command.

### End-state skill sources

```text
Product skills:
  ~/.local/share/brainstack/skills/*

Brain skills:
  <shared-brain>/skills/*/SKILL.md

Project skills:
  <repo>/.agents/skills/*/SKILL.md
```

### End-state destinations

Codex:

```text
~/.agents/skills/<skill-name> -> symlink to product/brain/project skill
```

Codex officially supports `$HOME/.agents/skills` and symlinked skill folders.

Claude:

```text
~/.claude/skills/<skill-name> or detected supported location
```

Do not hardcode if the local Claude install disagrees; `brainctl skills doctor` should detect and report.

Cursor:

```text
project .agents/skills for Codex-compatible skill content
Cursor rule that tells the agent how to find/use Brainstack skills
```

Cursor rules are not the same as full Agent Skills. For Cursor, skills are mostly prompt/rule visible unless Cursor’s installed version exposes a compatible skills path.

### Commands to add

```bash
brainctl skills list [--json]
brainctl skills sync [--repo .] [--json]
brainctl skills doctor [--json]
brainctl skills status [--json]
```

### Skill trust rule

Repo-local config can declare intent, but not authority. Do not auto-sync skills from pending-trust personal/company brains just because a repo’s `.brainstack.yaml` asks for them.

## Concrete commands to implement

### Harness integration

```bash
brainctl harness list [--json]
brainctl harness status [codex|claude|cursor|codex-app|all] [--json]
brainctl harness doctor [codex|claude|cursor|codex-app|all] [--json]
brainctl harness install codex|claude|cursor|all --ambient [--dry-run] [--yes] [--json]
brainctl harness uninstall codex|claude|cursor|all [--dry-run] [--yes] [--json]
brainctl harness diff codex|claude|cursor|all [--json]
```

`install` must:

- detect existing user files
- create backups
- use marked blocks or separate include files
- record every owned artifact in managed manifest
- never overwrite unowned content silently
- explain hook security implications
- require `--yes` or interactive approval for hooks

### Capture/session

```bash
brainctl capture event --harness codex|claude|cursor|codex-app --event EVENT --stdin-json [--json]
brainctl capture status [--json]
brainctl capture list [--json]
brainctl capture flush [--json]
brainctl capture purge --older-than 30d [--yes]
brainctl capture pause [--duration 1h|until-restart|forever]
brainctl capture resume
```

Hidden/advanced session commands may also exist:

```bash
brainctl session start --repo . --harness codex --json
brainctl session checkpoint --summary "..." --status active|blocked|complete --json
brainctl session finish --status complete|blocked|abandoned --transcript PATH --json
```

But those should not be normal user UX.

### Daemon

```bash
brainctl daemon install --launch-agent|--systemd-user [--dry-run] [--yes]
brainctl daemon uninstall [--dry-run] [--yes]
brainctl daemon run
brainctl daemon status [--json]
brainctl daemon logs
```

### Curator

Existing `brain-curator` routine should become the consumer of session artifacts.

Add or document:

```bash
brainctl curate status [--json]
brainctl curate run [--since 24h] [--dry-run] [--json]
brainctl routine install brain-curator
```

Curator output should be proposal-first:

```text
raw session artifact -> checkpoint summary -> sourced curation proposal -> wiki update after review/admin ingest
```

## Safety rules

### Hook security

Hooks execute with the user’s permissions. Therefore:

- never install hooks without explicit approval
- show exact files and commands to be installed
- keep hook commands absolute paths to `brainctl`
- do not run shell one-liners with unquoted JSON interpolation
- treat hook stdin as untrusted input
- hooks should write local spool only, not perform expensive network work
- `destroy`/`harness uninstall` must remove only managed blocks/files

### Privacy

Raw transcripts are sensitive.

Default policy:

```text
raw transcript capture: on for trusted-tailnet personal installs, but visible in setup
raw transcript upload: local-spool first, upload at session/generation close
wiki curation: proposal only
secrets: obvious redaction before upload
payload logs: hashes/lengths/status only, not raw text
pause button: always available
```

Menubar should expose:

```text
Capture: On / Paused
Mode: Checkpoints + raw artifacts / Checkpoints only / Off
Outbox: N queued
Last upload: timestamp
```

### No silent truncation

Large payload handling:

- compress large transcripts
- warn on unusually large item
- hard-refuse extreme size with metadata only
- never silently truncate a transcript and pretend memory is complete

## Recommended next Codex loop goal

Paste this into Codex as the next implementation pass.

```md
You are in the Brainstack repo.

This is an ambient harness integration pass. Do not broaden scope into enterprise auth, MCP as core architecture, new model runtimes, or a big frontend framework.

Product rule:
Brainstack must feel like ambient infrastructure behind the harnesses users already use. The normal user should not have to run `brainctl context`, `brainctl remember`, or `brainctl session` during daily work. CLI commands are plumbing for setup, hooks, daemon, diagnostics, and menubar UI.

Architectural rule:
Telemux remains transport/control-plane only. Do not put new Brainstack memory semantics into telemux. If telemux launches Codex/Claude on a worker, that harness instance should be captured by the same ambient harness adapter used for local sessions. Telemux may pass neutral metadata via environment variables.

Primary outcome:
Implement a first working Brainstack Ambient Layer for Codex CLI and Claude Code, with Cursor prompt/rule support and a documented Codex App strategy.

Implement:

1. `brainctl daemon`
- Add `brainctl daemon install|uninstall|run|status|logs`.
- macOS: install LaunchAgent under `~/Library/LaunchAgents/com.caimeo.brainstackd.plist`.
- Linux: install systemd user service.
- Daemon runs as current user.
- Daemon owns local capture spool draining, idle timers, outbox flush, and skill sync checks.
- Daemon must use private dirs/files: 0700 dirs, 0600 payload files.
- Daemon must not require sudo.
- Daemon must not expose a remote HTTP server by default; local socket/status only.

2. Local capture spool
- Add private local spool under `~/.local/state/brainstack/spool`.
- Add atomic JSON event writes.
- Add event schema v1 with machine, harness, surface, eventName, sessionId, turnId, cwd, repoRoot, context profile, brain ids, payload policy, payload hash/size, and optional transcript path.
- Do not log raw payloads by default.
- Compress large raw transcripts before import.
- Never silently truncate.
- Add corruption quarantine.

3. `brainctl capture`
- Add:
  - `brainctl capture event --harness ... --event ... --stdin-json [--json]`
  - `brainctl capture status [--json]`
  - `brainctl capture list [--json]`
  - `brainctl capture flush [--json]`
  - `brainctl capture pause/resume`
  - `brainctl capture purge --older-than ... --yes`
- `capture event` must be fast and safe for hooks.
- `capture event` should never directly perform expensive network upload unless explicitly configured; it should spool.
- Return small extra context only on events that support it.

4. `brainctl harness`
- Add:
  - `brainctl harness list [--json]`
  - `brainctl harness status codex|claude|cursor|codex-app|all [--json]`
  - `brainctl harness doctor ...`
  - `brainctl harness install ... --ambient --dry-run|--yes [--json]`
  - `brainctl harness uninstall ... --dry-run|--yes [--json]`
  - `brainctl harness diff ...`
- Detect installed harnesses and their config locations.
- Install managed files/blocks only after approval.
- Always backup existing files before patching.
- Maintain `~/.config/brainstack/managed-harness-artifacts.json`.
- Destroy/uninstall must remove only managed artifacts.

5. Codex CLI ambient integration
- Install/update:
  - `~/.codex/AGENTS.md` include or managed block
  - `~/.codex/hooks.json` managed Brainstack hooks
  - user skills under `~/.agents/skills` where available
- Use Codex hooks:
  - `SessionStart`: call `brainctl capture event ...`, return concise dynamic context.
  - `UserPromptSubmit`: spool prompt metadata; detect explicit memory phrases.
  - `Stop`: spool final assistant message and transcript pointer; do not upload every turn.
  - optional `PostToolUse`: coarse changed-file/artifact hints only; off by default if noisy.
- Do not rely on Codex transcript file format as stable. Store transcript path/hash, but keep parser tolerant.
- Add tests that hook config is valid JSON and existing hooks are merged without destroying them.

6. Claude Code ambient integration
- Install/update:
  - `~/.claude/CLAUDE.md` import or managed block
  - `~/.claude/settings.json` managed hooks block
  - Claude skill location if detected; otherwise report guidance.
- Use Claude hooks:
  - `SessionStart`: fast dynamic context.
  - `UserPromptSubmit`: spool prompt metadata and explicit-memory triggers.
  - `Stop`: async checkpoint candidate from last assistant message, background_tasks, session_crons.
  - `SessionEnd`: raw transcript artifact close signal.
- Hook commands must be absolute and safe.
- Add tests for settings merge/backup/uninstall.

7. Cursor support
- Install/update:
  - user/project rule where appropriate
  - optional `AGENTS.md` guidance when repo opts in
- Do not claim reliable transcript capture unless a stable hook/session API is detected.
- `brainctl harness status cursor` should clearly say whether Cursor is prompt-guidance-only or capture-enabled.

8. Codex App strategy
- Add `brainctl harness status codex-app` detection.
- Document two tiers:
  - skills/instructions available now
  - app-server watcher for deep thread capture later
- If implementing watcher is small, add experimental disabled-by-default app-server adapter.
- If not implementing, document as deferred and do not fake support.

9. Anti-spam policy
- Implement session/generation logic.
- One raw transcript import per session generation.
- Checkpoint rate limits, e.g. default at most one checkpoint per 10 minutes or meaningful event.
- Idle close default 30-60 minutes, configurable.
- Explicit phrases like “remember this” create immediate high-confidence proposal/import.
- Offline behavior uses existing outbox.
- Stable idempotency keys.

10. Curator integration
- Update `brain-curator` to group raw/checkpoint artifacts by `conversation_id/session_id/generation`.
- It must produce sourced proposals, not silent canon edits.
- It must preserve raw artifacts for reprocessing.
- Add docs showing how daily curation turns session artifacts into wiki proposals.

11. Telemux boundary
- Stop expanding `importRunNotesToBrain` semantics.
- Pass neutral Brainstack invocation metadata to launched harnesses.
- Keep existing telemux import fallback only for backward compatibility if needed.
- Add docs: telemux routes; harness adapters capture.

12. Menubar readiness
- Add stable JSON outputs required by a future macOS menubar app:
  - `brainctl daemon status --json`
  - `brainctl harness status --json`
  - `brainctl capture status --json`
  - `brainctl outbox status --json`
  - `brainctl skills status --json`
- Do not build the menubar app in this pass unless already trivial.
- Document the menubar contract.

13. Docs
- Add:
  - `docs/ambient-harnesses.md`
  - `docs/harness-codex.md`
  - `docs/harness-claude.md`
  - `docs/harness-cursor.md`
  - `docs/macos-menubar-contract.md`
  - `docs/session-capture.md`
- Update quickstart to say:
  - install Brainstack
  - approve harness integration
  - continue using your normal harness
- Explain privacy modes and pause controls.

Validation:
- Fresh temp HOME install simulation.
- `brainctl daemon install --dry-run` creates correct macOS/Linux plans.
- `brainctl harness install codex --ambient --dry-run` shows exact managed artifacts.
- Codex hook JSON validates and merges with existing user hooks.
- Simulated Codex SessionStart/UserPromptSubmit/Stop events create spool entries.
- Codex Stop does not produce one remote POST per turn.
- Claude settings merge preserves existing settings and creates backups.
- Simulated Claude SessionStart/UserPromptSubmit/Stop/SessionEnd events create expected spool entries.
- Claude SessionEnd queues one raw transcript artifact per generation.
- Cursor status honestly reports prompt-guidance-only if no capture API is available.
- Offline braind queues capture uploads through outbox.
- Idle close creates at most one raw transcript import per generation.
- Replaying the same hook event is idempotent.
- Telemux-launched harness receives neutral Brainstack metadata but telemux does not own memory semantics.
- Brain-curator consumes a simulated session artifact and creates a sourced proposal.
- `destroy`/`harness uninstall` removes only managed artifacts.
- Existing tests remain green.

Final response must include:
- exact files changed
- commands added/changed
- whether full tests passed or which focused tests ran
- whether daemon is live
- whether Codex ambient integration is live
- whether Claude ambient integration is live
- whether Cursor support is prompt-only or capture-capable
- whether Codex App support is live or documented/deferred
- whether telemux remained transport-only
- whether curator consumes session artifacts
- remaining blockers before public OSS announcement
- handoff zip path
```

## End-state validation for the product, not just tests

### Mac happy path

1. User installs Brainstack.
2. Menubar app or setup wizard detects Codex, Claude, Cursor.
3. User clicks “Install into Codex.”
4. Brainstack shows exact files to change.
5. User approves once.
6. User opens Codex normally.
7. Codex receives Brainstack context on session start.
8. User completes a task.
9. Brainstack captures a checkpoint and eventually one raw transcript artifact.
10. Curator later creates a proposal from it.
11. User never manually ran `brainctl context` or `brainctl remember`.

### Linux/worker happy path

1. Worker is registered.
2. `brainctl harness install codex --ambient --yes` runs on worker.
3. Telemux routes a Telegram task to worker.
4. Worker Codex runs normally.
5. Brainstack capture occurs on worker through Codex hooks.
6. Outbox queues if control brain is offline.
7. Flush later imports raw/checkpoint artifacts.
8. Telemux only routes and returns output.

### Anti-spam validation

Simulate 20 Stop events for one Codex session:

Expected:

```text
20 local events spooled
<= configured checkpoint max uploaded
1 raw transcript artifact when session/generation closes
0 canonical wiki edits
0 duplicate imports after replay
```

### Privacy validation

- raw transcript stored under 0600 file
- logs contain hash/size/status, not payload
- pause capture stops new uploads
- checkpoint-only mode never uploads raw transcript
- destroy/uninstall removes managed hooks but not user data unless explicitly requested

## Final recommendation

Do not spend the next pass on a fancy UI first. Build the ambient substrate first:

1. `brainstackd`
2. `brainctl capture`
3. `brainctl harness install/status/doctor/uninstall`
4. Codex + Claude hooks
5. skills sync
6. curator session artifact consumption
7. Menubar contract and then the actual menubar app

The menubar app will be excellent UX, but it should be a face over working primitives. If the primitives are weak, the menubar app becomes a pretty button that installs brittle prompt text.

The single biggest risk right now is false confidence: Brainstack “installed into Codex/Claude/Cursor” currently means “the harness has instructions that it may or may not follow.” The next milestone should make it mean: “the harness has lifecycle hooks or a watcher, a local spool, daemon-backed publishing, and a curator path.”
