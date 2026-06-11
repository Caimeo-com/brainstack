---
name: brain-curator
description: Organizer/admin skill for compiling raw shared-brain imports and proposals into concise sourced wiki pages without letting clients directly mutate the canon.
---

# Brain Curator

Use this skill when acting as the control/admin side of a shared brain.

- Read raw artifacts and source manifests first.
- Preserve originals; never rewrite raw imported artifacts.
- Every nontrivial synthesized claim needs a source artifact id or source page.
- Record contradictions and freshness notes instead of silently overwriting them.
- Use the admin ingest/lint token only on the organizer/control host.
- Append parseable log entries for imports, ingests, lint reports, and manual curation.

## Proposal workflow

Proposal generation is automatic; wiki mutation is policy-controlled (`curation.mode`: `manual`, `approval`, or `auto`). Do not edit canonical wiki pages directly — submit machine proposals and let policy decide:

1. Read the cursor: `brainctl curator status` (or `GET /api/curator/status`). Review imports/logs/proposals newer than it.
2. Group new material by topic/source type and draft a full proposed page content per change.
3. Submit each change with:
   `brainctl propose --title T --body WHY --target-page wiki/PATH.md --content-file FILE --base-sha256 $(sha256sum current-page) --risk low|medium|high --confidence 0..1 --source-ids id1,id2 --curator-run-id RUN`
   Use `--base-sha256 absent` for new pages and `--needs-human` for contradictory or ambiguous material.
4. Risk guidance: `low` = additive, sourced, small (Status/Sources pages); `medium` = restructuring or prose edits; `high` = deletions, decision changes, runbooks.
5. In `auto` mode only low-risk proposals inside `curation.autoApply.allowedPaths` apply automatically; everything else stays `pending` or `needs-human` for `brainctl proposals approve|reject|apply`.

