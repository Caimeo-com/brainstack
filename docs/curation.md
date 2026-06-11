# Curation and Proposals

The shared brain learns continuously without turning every raw import into canon: proposal generation is automatic, wiki mutation is policy-controlled.

## Proposal state model

Every proposal is a markdown file with machine frontmatter, plus an optional `<id>.content.md` sidecar carrying the full proposed page content.

| Status | Directory | Meaning |
| --- | --- | --- |
| `pending` | `proposals/pending/` | Awaiting a decision. |
| `approved` | `proposals/pending/` | Approved by an admin; not yet applied. |
| `needs-human` | `proposals/pending/` | Parked: curator flagged it, or apply was blocked by target drift. |
| `applied` | `proposals/applied/` | Proposed content written to the target wiki page. |
| `rejected` | `proposals/rejected/` | Declined, with an optional reason. |
| `superseded` | `proposals/superseded/` | Another proposal for the same target was applied. |

Machine frontmatter fields: `proposal_id`, `status`, `target_page`, `base_sha256` (drift guard: sha256 of the target content the proposal was computed against, or `absent` for new pages), `risk` (`low|medium|high`), `confidence` (0..1), `curator_run_id`, `reason`, `source_ids`, `decided_at`, `decided_by`.

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
- `approval` (default): the curator creates proposals; a human approves/applies them.
- `auto`: low-risk proposals that match `allowedPaths`, stay within `maxChangedLines`, respect `allowDeletes`, and pass the drift check apply automatically at propose time. Everything else stays `pending` or `needs-human`.

`brainctl upgrade` renders the policy into `braind.runtime.env` as `BRAIN_CURATION_MODE`, `BRAIN_CURATION_ALLOWED_PATHS`, `BRAIN_CURATION_MAX_CHANGED_LINES`, and `BRAIN_CURATION_ALLOW_DELETES`.

## Curator automation

The `brain-curator` routine installs automatically (daily) into the `brainstack-routines` context when telemux has `FACTORY_TELEGRAM_CONTROL_CHAT_ID` set. Each run:

1. Reads the cursor from `GET /api/curator/status`.
2. Reviews imports/logs/proposals newer than the cursor, grouped by topic/source type.
3. Submits sourced machine proposals via `brainctl propose --target-page ... --content-file ... --base-sha256 ... --risk ... --confidence ... --source-ids ... --curator-run-id ...`.
4. telemux reports run outcome, failures, next run, and the new cursor to `POST /api/curator/status` (requires `FACTORY_BRAIN_ADMIN_TOKEN` in the telemux env).

The wiki home page shows the curation panel: mode, curator installed, last/next run, last-run failures, open proposals, recently applied changes, and imports awaiting curation.

## Commands

```bash
brainctl proposals list [--status open|pending|approved|applied|rejected|superseded|needs-human] [--json]
brainctl proposals show <id> [--json]
brainctl proposals approve <id>
brainctl proposals reject <id> [--reason TEXT]
brainctl proposals apply <id>
brainctl curator status [--json]
brainctl curator run
brainctl curator install
```

Reads are unauthenticated within the tailnet; approve/reject/apply require `BRAIN_ADMIN_TOKEN` (control host). `curator run`/`curator install` talk to the local telemux dashboard control endpoints.

Telegram mirrors the basics:

```text
/curator_status
/curator_run
/proposals
/proposal_approve_<token>_<n>
/proposal_reject_<token>_<n>
```

Telegram approval applies the proposed wiki change when the proposal carries one (drift still parks it as `needs-human`). Approve/reject from Telegram require the optional `FACTORY_BRAIN_ADMIN_TOKEN` in the telemux env; without it, Telegram stays read-only for proposals.

## API

- `GET /api/proposals[?status=...]` — list proposals (public read).
- `GET /api/proposals/<id>` — proposal body plus a rendered diff (public read).
- `POST /api/proposals/<id>/approve|reject|apply` — admin token.
- `GET /api/curator/status` — policy, curator run state, proposal counts (public read).
- `POST /api/curator/status` — admin token; telemux reports run outcomes here.
- `POST /api/propose` — accepts the machine fields (`target_page`, `proposed_content`, `base_sha256`, `risk`, `confidence`, `curator_run_id`, `reason`, `status: pending|needs-human`, `source_ids`) and auto-applies under policy in `auto` mode.
