---
name: brain-curator
description: Organizer/admin skill for compiling raw shared-brain imports and proposals into concise sourced wiki pages without letting clients directly mutate the canon.
---

# Brain Curator

Use this skill when acting as the control/admin side of a shared brain.

- Read raw artifacts and source manifests first.
- Preserve originals; never rewrite raw imported artifacts.
- Every nontrivial synthesized claim needs a source artifact id, source page, or explicit evidence ref.
- Record contradictions and freshness notes instead of silently overwriting them.
- Use the admin ingest/lint token only on the organizer/control host.
- Append parseable log entries for imports, ingests, lint reports, and manual curation.
- Treat raw `remember` imports as evidence, not canon. A durable memory must stand alone without the original thread context.

## Proposal workflow

Proposal generation is automatic; wiki mutation is policy-controlled (`curation.mode`: `manual`, `approval`, or `auto`). Do not edit canonical wiki pages directly — submit machine proposals and let policy decide:

1. Read the cursor: `brainctl curator status` (or `GET /api/curator/status`). Review imports/logs/proposals newer than it.
2. Group new material by topic/source type. Batch related memory candidates into one scoped card instead of producing many tiny global-looking fragments.
3. Submit each change with:
   `brainctl propose --title T --body WHY --target-page wiki/PATH.md --content-file FILE --base-sha256 $(sha256sum current-page) --risk low|medium|high --confidence 0..1 --source-ids id1,id2 --curator-run-id RUN`
   Use `--base-sha256 absent` for new pages and `--needs-human` for contradictory or ambiguous material.
4. For memory-shaped proposals, include the envelope:
   `--project NAME --domain NAME --scope repo|project|global|machine|harness --memory-kind KIND --applicability TEXT --non-applicability TEXT --evidence REF`
   Prefer `scope=repo` or `scope=project` unless the lesson is truly global. Add `--needs-human` when a future harness would not know where the lesson applies and where it does not.
5. Run the intelligibility test before submitting memory: "Would a future harness, with no original thread context, know where this applies and where it does not?" If no, enrich it or park it as `needs-human`.
6. Risk guidance: `low` = additive, sourced, small (Status/Sources pages); `medium` = restructuring or prose edits; `high` = deletions, decision changes, runbooks.
7. In `auto` mode only low-risk proposals inside `curation.autoApply.allowedPaths` apply automatically; everything else stays `pending` or `needs-human` for explicit Accept/reject review. Accept is the user-facing apply path; the low-level `approve` command exists only for compatibility.

Use `brainctl proposals groups` to find related old or candidate memories that should be reviewed together. Review groups are deterministic hints from project/repo/scope/kind/topic metadata; Brainstack does not use embeddings or cosine thresholds for this path. Legacy title/body-only `Remember:` proposals are expected to show as `needs-human`/`needs-context` until enriched.

Use `brainctl proposals enrich <id>` when a context-poor legacy memory has enough known project/scope/applicability/evidence context to become a structured replacement proposal. Use `brainctl proposals reprocess --status needs-human` for a dry-run batch plan, then add `--apply` only after checking the generated envelope. Enrichment creates replacement proposals; it does not apply wiki edits or mutate the original legacy proposal.

Use `brainctl proposals merge-group <group-key|label>` when a group contains overlapping scoped lessons. It is dry-run by default and processes at most 20 proposals unless `--limit N` or `--all` is passed deliberately. Add `--submit` to create one consolidated proposal, and add `--close-sources` only when running on the control host with admin token available. Source proposals without a target page should be merged or enriched before they are accepted.

If a useful Codex lesson is missing from proposals, first check whether the raw session evidence reached the shared brain. On the machine that owns the Codex log, use `brainctl import codex-session <SESSION_ID|JSONL_PATH>` for a bounded checkpoint or add `--include-transcript` when the full JSONL transcript should become raw evidence.
