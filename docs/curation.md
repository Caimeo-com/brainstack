# Curation and Proposals

The shared brain learns continuously without turning every raw import into canon: proposal generation is automatic, wiki mutation is policy-controlled.

## Proposal state model

Every proposal is a markdown file with machine frontmatter, plus an optional `<id>.content.md` sidecar carrying the full proposed page content.

| Status | Directory | Meaning |
| --- | --- | --- |
| `pending` | `proposals/pending/` | Awaiting a decision. |
| `approved` | `proposals/pending/` | Legacy staged state retained for API/CLI compatibility; normal operator UX uses Accept, which applies directly. |
| `needs-human` | `proposals/pending/` | Parked: curator flagged it, or apply was blocked by target drift. |
| `applied` | `proposals/applied/` | Proposed content written to the target wiki page. |
| `rejected` | `proposals/rejected/` | Declined, with an optional reason. |
| `superseded` | `proposals/superseded/` | Absorbed by a consolidated proposal, or made stale by applying another proposal for the same target. |

Machine frontmatter fields: `proposal_id`, `status`, `target_page`, `base_sha256` (drift guard: sha256 of the target content the proposal was computed against, or `absent` for new pages), `risk` (`low|medium|high`), `confidence` (0..1), `curator_run_id`, `reason`, `source_ids`, `decided_at`, `decided_by`.

Memory proposals also carry a bounded context envelope: `source_type`, `related_repo`, `project`, `domain`, `scope`, `memory_kind`, `context`, `applicability`, `non_applicability`, `evidence_refs`, `review_after`, and `expires_at`. `brainctl remember` fills these with conservative repo-scoped defaults unless the caller provides explicit values. The default guardrail is intentionally narrow: do not apply remembered lessons globally without checking the captured project and evidence context.

Every proposal gets a deterministic quality gate in frontmatter and API JSON:

- `quality_score`: 0..1, based on body detail, project/domain context, bounded scope, applicability, evidence, and confidence.
- `quality_decision`: `ready`, `needs-context`, `needs-evidence`, or `too-vague`.
- `quality_reasons`: reviewer-facing reasons for the decision.

Memory-shaped proposals (`source_type: remember`, `memory_kind`, or a `remember` tag) that fail the gate are stored as `needs-human`. This prevents vague project-specific lessons from looking like ordinary canon candidates. Concrete wiki-edit proposals with `target_page` plus `proposed_content` can still be reviewed normally because the sidecar diff is the primary evidence.

Legacy title/body-only `Remember:` proposals are treated as old-format evidence when read: they surface as `needs-human` with `quality_decision: needs-context`, `legacy_format: true`, and deterministic `cluster_key`/`cluster_label` hints. API field names keep `cluster_*` for compatibility, but user-facing tools call these review groups. This is intentionally non-destructive; it does not rewrite proposal files or apply wiki edits. Use review groups to merge related old memories into one scoped card instead of accepting fragments one by one.

Use `brainctl proposals enrich <id>` when a legacy or context-poor memory has enough known context to become a structured replacement proposal. The command reads the original proposal, preserves it as `proposal:<id>` evidence, fills the memory envelope from flags or conservative defaults, and submits a new proposal through the normal propose/outbox path. It does not mutate or apply the original proposal.

Use `brainctl proposals reprocess` for bounded legacy cleanup. It is dry-run by default, selects `needs-human` / `needs-context` proposals, and prints the structured replacement payloads it would create. Add `--apply` to submit the replacement proposals. This is still a proposal-generation step, not wiki mutation.

Use `brainctl proposals merge-group <group-key|label>` when two or more proposals describe the same scoped lesson. It builds one deterministic consolidated wiki proposal from the source proposals, preserves `proposal:<id>` evidence refs, dedupes exact/title-normalized lesson lines, and marks the merged proposal `needs-human` when the source group has conflicts or missing context. It is dry-run by default; add `--submit` to create the consolidated proposal. Review groups default to 20 proposals; use `--limit N` or `--all` deliberately for larger groups, or pass repeated/comma-separated `--id` values to merge only a specific subset. `--close-sources` marks source proposals as `superseded` with an `absorbed into ...` reason after the merge, and remains retry-aware for older sources already closed as `merged into ...`. No embeddings or cosine thresholds are used.

Use `brainctl proposals auto-merge` for unattended consolidation of obvious related batches. It is dry-run by default and inspects deterministic review groups, but it does not turn a whole group into one proposal automatically. Instead it splits each group by relation key: same review group, same created day by default, and a coarse topic bucket such as front-end/UI, docs/content, curation/proposals, install/lifecycle, daemon/fleet, Telegram/telemux, outbox/sync, tests/CI, security/safety, or performance/latency. Each selected batch needs at least 2 proposals, at most 6 proposals by default, no legacy proposals, and no `needs-context` proposals unless the operator explicitly opts in. Add `--submit` for the scheduled curator/control host path; submitted auto-merges create one consolidated proposal and supersede the absorbed source candidates unless `--keep-sources` is set. It still does not apply wiki edits.

Use `brainctl proposals batch-merge` for an operator-triggered harness scan. It injects the top 100 open proposals, with bounded detail snippets, into the configured harness at normal Codex reasoning effort. The harness returns merge candidates in JSON. Candidates at or above `--auto-threshold` (default `0.8`) are submitted as consolidated proposals and have their absorbed source proposals marked `superseded`; lower-confidence candidates are submitted as `needs-human` review proposals. It still does not apply wiki edits. If more than 100 proposals are open, the command warns that the operator should rerun after the first batch is handled.

Proposal consolidation is a control-host operation. On enrolled client profiles, `merge-group`, `auto-merge`, and `batch-merge` forward over the configured control SSH route (`--via`, `BRAINSTACK_TELEGRAM_VIA`, or `client.telegramVia`) and run from the control host's Brainstack repo/config. The client can still list and show proposals for review, but it should not download proposal bodies into a local harness prompt for merge discovery. If a client has no control route, consolidation fails closed instead of running locally; use `--local` only for deliberate development or single-node testing.

Applying a proposal:

1. Validates the target is a `wiki/**.md` path inside the repo.
2. Checks `base_sha256` against the current target content; drift parks the proposal as `needs-human` instead of clobbering the page.
3. Writes the sidecar content to the target page, moves the proposal to `proposals/applied/`, and supersedes other open proposals for the same target.

## Curation policy

```yaml
curation:
  mode: approval # auto | approval | manual
  autoApply:
    allowedPaths:
      - wiki/Status/**
      - wiki/Sources/**
    maxChangedLines: 40
    allowDeletes: false
```

- `manual`: proposals only; nothing applies without an explicit admin action.
- `approval` (default): the curator creates proposals; a human accepts or rejects them. Accept calls the apply path directly.
- `auto`: low-risk proposals that match `allowedPaths`, stay within `maxChangedLines`, respect `allowDeletes`, and pass the drift check apply automatically at propose time. Everything else stays `pending` or `needs-human`.

`brainctl lifecycle upgrade` or the lower-level `brainctl upgrade` renders the policy into `braind.runtime.env` as `BRAIN_CURATION_MODE`, `BRAIN_CURATION_ALLOWED_PATHS`, `BRAIN_CURATION_MAX_CHANGED_LINES`, and `BRAIN_CURATION_ALLOW_DELETES`.

## Curator automation

The `brain-curator` routine installs automatically (daily) into the `brainstack-routines` context when telemux has `FACTORY_TELEGRAM_CONTROL_CHAT_ID` set. Each run:

1. Reads the cursor from `GET /api/curator/status`.
2. Reviews imports/logs/proposals newer than the cursor, grouped by context, repo, source type, and deterministic review group hints.
3. Submits sourced machine proposals via `brainctl propose --target-page ... --content-file ... --base-sha256 ... --risk ... --confidence ... --source-ids ... --curator-run-id ...`.
4. For memory candidates, includes the envelope fields (`--project`, `--domain`, `--scope`, `--memory-kind`, `--applicability`, `--non-applicability`, and `--evidence`) or lets `brainctl remember` provide conservative repo-scoped defaults.
5. Runs `brainctl proposals auto-merge --submit --json` to collapse safe related date/topic batches inside review groups into one consolidated proposal and mark absorbed sources `superseded`. Oversized, legacy, context-poor, or one-off batches stay in the operator queue.
6. telemux reports run outcome, failures, next run, and the new cursor to `POST /api/curator/status` (requires `FACTORY_BRAIN_ADMIN_TOKEN` in the telemux env).

The wiki home page shows the curation panel: mode, curator installed, last/next run, last-run failures, open proposals, recently applied changes, and imports awaiting curation.

## Commands

```bash
brainctl proposals list [--status open|pending|approved|applied|rejected|superseded|needs-human] [--json]
brainctl proposals groups [--status open|pending|approved|applied|rejected|superseded|needs-human] [--min-size N] [--json]
brainctl proposals show <id> [--json]
brainctl proposals enrich <id> [--summary TEXT] [--project NAME] [--domain NAME] [--scope repo|project|global|machine|harness] [--memory-kind KIND] [--applicability TEXT] [--non-applicability TEXT] [--evidence REF] [--dry-run|--json]
brainctl proposals reprocess [--status needs-human|open] [--group KEY] [--id ID] [--limit N] [--apply] [--json] [enrichment flags...]
brainctl proposals merge-group <group-key|group-label> [--id ID] [--status open] [--submit] [--limit N|--all] [--target-page wiki/PATH.md] [--needs-human] [--close-sources] [--json] [--via SSH_TARGET] [--remote-repo PATH] [--local]
brainctl proposals auto-merge [--status open] [--submit] [--min-size N] [--max-group-size N|--allow-large-groups] [--max-source-group-size N|--all-source-groups] [--limit-groups N|--all-groups] [--relation-window day|all] [--include-legacy] [--include-needs-context] [--keep-sources] [--json] [--via SSH_TARGET] [--remote-repo PATH] [--local]
brainctl proposals batch-merge [--status open] [--submit] [--limit 100] [--auto-threshold 0.8] [--harness codex|claude] [--harness-bin PATH] [--keep-sources] [--json] [--via SSH_TARGET] [--remote-repo PATH] [--local]
brainctl proposals approve <id> [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]
brainctl proposals reject <id> [--reason TEXT] [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]
brainctl proposals supersede <id> [--reason TEXT] [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]
brainctl proposals apply <id> [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]
brainctl curator status [--json]
brainctl curator run
brainctl curator install
brainctl import codex-session <SESSION_ID|JSONL_PATH> [--include-transcript] [--max-bytes N] [--dry-run|--json]
brainctl remember --repo PATH --summary TEXT [--project NAME] [--domain NAME] [--scope repo|project|global|machine|harness] [--memory-kind KIND] [--applicability TEXT] [--non-applicability TEXT] [--evidence REF]
brainctl propose --title TITLE --body BODY [--project NAME] [--domain NAME] [--scope repo|project|global|machine|harness] [--memory-kind KIND] [--applicability TEXT] [--non-applicability TEXT] [--evidence REF]
```

Reads are unauthenticated within the tailnet; accept/reject/apply require `BRAIN_ADMIN_TOKEN` on the control host. The CLI `approve` command is retained for approval-mode workflows, but normal review should use `apply`/Accept. On an enrolled client, proposal decision and consolidation commands forward over the explicit control SSH route from `--via`, `BRAINSTACK_TELEGRAM_VIA`, or `client.telegramVia`, using the configured pinned known-hosts file by default. Consolidation commands require that explicit control route and fail closed without it. `curator run`/`curator install` use the local telemux dashboard control endpoints when telemux is enabled; enrolled clients without local telemux forward them over the configured control SSH route, including the `client.remoteSsh` fallback for curator commands.

Telegram mirrors the basics:

```text
/curator_status
/curator_run
/curation
/proposals [open|pending|needs-human|approved|applied|rejected|superseded] [ready|needs-context|needs-evidence|too-vague] [search terms|project:NAME|scope:NAME|group:NAME] [limit=N]
/proposal_groups
/proposal_merges [preview]
/proposal_explain_<token>_<n>
/proposal_accept_<token>_<n>
/proposal_reject_<token>_<n>
```

Run `/curation` in the Telegram topic you want to use for proposal review. It binds that topic to a durable `proposal-curation` scratch context, makes the built-in `brain-curator` routine target that topic, and prints the useful proposal review commands. It accepts an optional machine override: `/curation <machine>`.

Telegram proposal lists include Explain, Accept, and Reject shortcuts. Explain shows the proposal's status, scope, source refs, body preview, and rendered diff preview without making changes. Telegram Accept applies the proposed wiki change when the proposal carries one (drift still parks it as `needs-human`). Context-only candidates without a target page must be enriched or merged before they can be accepted. `/proposal_groups` shows review groups with multiple open proposals. `/proposal_merges` runs the harness batch scan on the control host (`brainctl proposals batch-merge --submit --limit 100 --auto-threshold 0.8`): it reviews the top 100 open proposals, creates consolidated proposals, marks high-confidence absorbed sources superseded, and submits lower-confidence candidates for review, but does not apply wiki edits. Say "look for proposal merges", "show proposal merge candidates", or "merge related proposals" instead of using the commands directly. Accept/reject and merge submission from Telegram require the optional `FACTORY_BRAIN_ADMIN_TOKEN` in the telemux env; without it, Telegram stays read-only for proposals.

Useful review flows:

```text
/proposals pending
/proposals needs-human needs-context
/proposals open project:lindy limit=10
```

## API

- `GET /api/proposals[?status=...]` — list proposals plus deterministic review group hints (public read). The response includes both `review_groups` and legacy `clusters`.
- `GET /api/proposals/groups[?status=open&min_size=2]` — list deterministic memory-review groups (public read). `/api/proposals/clusters` remains as a compatibility alias.
- `GET /api/proposals/<id>` — proposal body plus a rendered diff (public read).
- `POST /api/proposals/<id>/approve|reject|apply` — admin token.
- `GET /api/curator/status` — policy, curator run state, proposal counts (public read).
- `POST /api/curator/status` — admin token; telemux reports run outcomes here.
- `POST /api/propose` — accepts the machine fields (`target_page`, `proposed_content`, `base_sha256`, `risk`, `confidence`, `curator_run_id`, `reason`, `status: pending|needs-human`, `source_ids`) plus the memory envelope fields above. It auto-applies under policy in `auto` mode only after the final proposal status remains applyable.

## Session Evidence

Harness hooks are fail-open and opportunistic. They can queue a small session checkpoint when a hook payload includes a regular `transcript_path`, but they do not import full transcripts by default and some harnesses may send no stdin to hooks. A durable lesson can therefore be absent from proposals even when the local Codex session exists and Brainstack hooks are installed.

When an important session is missing from shared-brain evidence, import it explicitly from the machine where Codex wrote the log:

```bash
brainctl import codex-session 019ebbfc-3a60-7f61-a4fa-a89282b8d83f \
  --config ~/.config/brainstack/brainstack.yaml
```

Use `--include-transcript` only when the full JSONL transcript should become shared-brain raw material. Without it, the import is a bounded checkpoint containing session metadata, last agent message, transcript path, size, and hash.
