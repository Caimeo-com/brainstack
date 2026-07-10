import { describe, expect, test } from "bun:test";
import { buildCuratorInbox, classifyEvidenceForCuration } from "../../../apps/braind/src/evidence-policy";

describe("curator evidence policy", () => {
  test("classifies only exact built-in routine provenance as audit-only", () => {
    expect(
      classifyEvidenceForCuration({
        source_harness: "telemux",
        source_type: "telemux-run",
        run_origin: "scheduled",
        routine_name: "brain-curator"
      })
    ).toMatchObject({ disposition: "audit-only", inferred: true });

    expect(
      classifyEvidenceForCuration({
        title: "The curator routine revealed a reusable Redis lesson",
        source_harness: "codex",
        source_type: "remember",
        tags: ["remember"]
      })
    ).toEqual({ disposition: "candidate", reason: null, inferred: true });

    expect(
      classifyEvidenceForCuration({
        source_harness: "telemux",
        source_type: "telemux-run",
        run_origin: "scheduled",
        routine_name: "customer-research"
      })
    ).toEqual({ disposition: "candidate", reason: null, inferred: true });
  });

  test("recognizes legacy receipts only when every metadata signal matches", () => {
    const legacy = {
      title: "telemux run notes: brainstack-routines",
      source_harness: "telemux",
      source_type: "telemux-run",
      conversation_id: "brainstack-routines",
      tags: ["telemux", "factory-run"]
    };
    expect(classifyEvidenceForCuration(legacy).disposition).toBe("audit-only");
    expect(classifyEvidenceForCuration({ ...legacy, title: "telemux run notes: proposal-curation", conversation_id: "proposal-curation" }).disposition).toBe("audit-only");
    expect(classifyEvidenceForCuration({ ...legacy, conversation_id: "project-routines" }).disposition).toBe("candidate");
    expect(classifyEvidenceForCuration({ ...legacy, tags: ["telemux"] }).disposition).toBe("candidate");
  });

  test("explicit candidate promotion wins and unknown evidence remains eligible", () => {
    expect(
      classifyEvidenceForCuration({
        source_harness: "telemux",
        source_type: "telemux-run",
        run_origin: "scheduled",
        routine_name: "brain-curator",
        curation_disposition: "candidate"
      }).disposition
    ).toBe("candidate");
    expect(classifyEvidenceForCuration({ curation_disposition: "unexpected" }).disposition).toBe("candidate");
  });

  test("builds a metadata-only inbox with complete counts and bounded lists", () => {
    const inbox = buildCuratorInbox(
      [
        { id: "candidate", title: "Useful", created_at: "2026-07-11T02:00:00Z", source_type: "remember", source_harness: "codex", source_machine: "mac", raw_path: "raw/a.md" },
        { id: "receipt", title: "Receipt", created_at: "2026-07-11T03:00:00Z", source_type: "telemux-run", source_harness: "telemux", source_machine: "control", raw_path: "raw/b.md", run_origin: "scheduled", routine_name: "brain-curator" }
      ],
      "2026-07-11T00:00:00Z",
      1
    );
    expect(inbox).toMatchObject({ total_since_cursor: 2, eligible_count: 1, audit_only_count: 1, overflow: false });
    expect(inbox.candidates.map((item) => item.id)).toEqual(["candidate"]);
    expect(inbox.excluded.map((item) => item.id)).toEqual(["receipt"]);
  });
});
