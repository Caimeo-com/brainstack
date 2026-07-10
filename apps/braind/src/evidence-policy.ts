export type CurationDisposition = "candidate" | "audit-only";

export interface EvidencePolicyInput {
  id?: string;
  title?: string;
  source_type?: string;
  source_harness?: string;
  tags?: string[];
  conversation_id?: string;
  run_origin?: string;
  routine_name?: string;
  curation_disposition?: string;
  curation_reason?: string;
}

export interface EvidencePolicyDecision {
  disposition: CurationDisposition;
  reason: string | null;
  inferred: boolean;
}

const AUDIT_ONLY_BUILTINS = new Set(["brain-curator", "update-check"]);
const LEGACY_BUILTIN_CONTEXTS = new Set(["brainstack-routines", "proposal-curation"]);

function normalized(value: unknown): string {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

function normalizedTags(input: EvidencePolicyInput): Set<string> {
  return new Set((input.tags || []).map(normalized).filter(Boolean));
}

function isBuiltinRoutineReceipt(input: EvidencePolicyInput): boolean {
  const runOrigin = normalized(input.run_origin);
  return (
    normalized(input.source_harness) === "telemux" &&
    normalized(input.source_type) === "telemux-run" &&
    (runOrigin === "scheduled" || runOrigin === "manual") &&
    AUDIT_ONLY_BUILTINS.has(normalized(input.routine_name))
  );
}

function isLegacyBuiltinReceipt(input: EvidencePolicyInput): boolean {
  const tags = normalizedTags(input);
  return (
    normalized(input.source_harness) === "telemux" &&
    normalized(input.source_type) === "telemux-run" &&
    LEGACY_BUILTIN_CONTEXTS.has(normalized(input.conversation_id)) &&
    normalized(input.title) === `telemux run notes: ${normalized(input.conversation_id)}` &&
    tags.has("telemux") &&
    tags.has("factory-run")
  );
}

/**
 * Classify source evidence from bounded manifest metadata only. Unknown and
 * malformed inputs remain candidates so policy cannot silently lose evidence.
 */
export function classifyEvidenceForCuration(input: EvidencePolicyInput): EvidencePolicyDecision {
  const explicit = normalized(input.curation_disposition);
  if (explicit === "audit-only") {
    return {
      disposition: "audit-only",
      reason: input.curation_reason?.trim() || "explicit audit-only evidence",
      inferred: false
    };
  }
  if (explicit === "candidate") {
    return { disposition: "candidate", reason: null, inferred: false };
  }
  if (isBuiltinRoutineReceipt(input)) {
    return {
      disposition: "audit-only",
      reason: `built-in ${normalized(input.routine_name)} routine receipt`,
      inferred: true
    };
  }
  if (isLegacyBuiltinReceipt(input)) {
    return {
      disposition: "audit-only",
      reason: "legacy built-in routine receipt",
      inferred: true
    };
  }
  return { disposition: "candidate", reason: null, inferred: true };
}

export interface CuratorInboxItem {
  id: string;
  title: string;
  created_at: string;
  source_type: string;
  source_harness: string;
  source_machine: string;
  normalized_path: string | null;
  raw_path: string;
  disposition: CurationDisposition;
  reason: string | null;
}

export interface CuratorInbox {
  cursor: string | null;
  total_since_cursor: number;
  eligible_count: number;
  audit_only_count: number;
  overflow: boolean;
  candidates: CuratorInboxItem[];
  excluded: CuratorInboxItem[];
}

function inboxItem(input: EvidencePolicyInput & Record<string, unknown>, decision: EvidencePolicyDecision): CuratorInboxItem {
  return {
    id: String(input.id || ""),
    title: String(input.title || ""),
    created_at: String(input.created_at || ""),
    source_type: String(input.source_type || ""),
    source_harness: String(input.source_harness || ""),
    source_machine: String(input.source_machine || ""),
    normalized_path: typeof input.normalized_path === "string" ? input.normalized_path : null,
    raw_path: String(input.raw_path || ""),
    disposition: decision.disposition,
    reason: decision.reason
  };
}

export function buildCuratorInbox(
  manifests: Array<EvidencePolicyInput & Record<string, unknown>>,
  cursor: string | null,
  limit = 100
): CuratorInbox {
  const boundedLimit = Math.max(0, Math.min(1_000, Math.trunc(limit)));
  const sinceCursor = manifests
    .filter((manifest) => !cursor || String(manifest.created_at || "") > cursor)
    .sort((a, b) => String(b.created_at || "").localeCompare(String(a.created_at || "")));
  const candidates: CuratorInboxItem[] = [];
  const excluded: CuratorInboxItem[] = [];
  for (const manifest of sinceCursor) {
    const decision = classifyEvidenceForCuration(manifest);
    const target = decision.disposition === "audit-only" ? excluded : candidates;
    if (target.length < boundedLimit) {
      target.push(inboxItem(manifest, decision));
    }
  }
  const eligibleCount = sinceCursor.filter((manifest) => classifyEvidenceForCuration(manifest).disposition === "candidate").length;
  const auditOnlyCount = sinceCursor.length - eligibleCount;
  return {
    cursor,
    total_since_cursor: sinceCursor.length,
    eligible_count: eligibleCount,
    audit_only_count: auditOnlyCount,
    overflow: eligibleCount > candidates.length || auditOnlyCount > excluded.length,
    candidates,
    excluded
  };
}
