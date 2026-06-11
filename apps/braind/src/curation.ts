import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, rename, rm, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { basename, dirname, join } from "node:path";
import {
  appendLogEntry,
  getRepoPaths,
  isSafeManifestId,
  isoNow,
  parseFrontmatter,
  safeRepoPath,
  stringifyFrontmatter,
  toRepoRelative,
  type FrontmatterData
} from "./brain-lib";

export type ProposalStatus = "pending" | "approved" | "applied" | "rejected" | "superseded" | "needs-human";
export type ProposalRisk = "low" | "medium" | "high";
export type CurationMode = "manual" | "approval" | "auto";

export const PROPOSAL_STATUSES: ProposalStatus[] = ["pending", "approved", "applied", "rejected", "superseded", "needs-human"];
/** Statuses that live in proposals/pending and still need a decision or application. */
export const OPEN_PROPOSAL_STATUSES: ProposalStatus[] = ["pending", "approved", "needs-human"];

export interface CurationPolicy {
  mode: CurationMode;
  allowedPaths: string[];
  maxChangedLines: number;
  allowDeletes: boolean;
}

export const DEFAULT_CURATION_POLICY: CurationPolicy = {
  mode: "approval",
  allowedPaths: ["wiki/Status/**", "wiki/Sources/**"],
  maxChangedLines: 40,
  allowDeletes: false
};

export function curationPolicyFromEnv(env: Record<string, string | undefined> = process.env): CurationPolicy {
  const rawMode = (env.BRAIN_CURATION_MODE || "").trim().toLowerCase();
  // Unknown modes fail closed to approval: proposals still flow, nothing auto-applies.
  const mode: CurationMode = rawMode === "manual" || rawMode === "auto" || rawMode === "approval" ? rawMode : DEFAULT_CURATION_POLICY.mode;
  const allowedPaths = (env.BRAIN_CURATION_ALLOWED_PATHS || "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
  const maxChangedLinesRaw = Number(env.BRAIN_CURATION_MAX_CHANGED_LINES || "");
  return {
    mode,
    allowedPaths: allowedPaths.length ? allowedPaths : [...DEFAULT_CURATION_POLICY.allowedPaths],
    maxChangedLines:
      Number.isFinite(maxChangedLinesRaw) && maxChangedLinesRaw > 0 ? Math.trunc(maxChangedLinesRaw) : DEFAULT_CURATION_POLICY.maxChangedLines,
    allowDeletes: env.BRAIN_CURATION_ALLOW_DELETES === "1" || env.BRAIN_CURATION_ALLOW_DELETES === "true"
  };
}

export interface ProposalRecord {
  id: string;
  title: string;
  status: ProposalStatus;
  createdAt: string;
  updatedAt: string;
  targetPage: string | null;
  baseSha256: string | null;
  risk: ProposalRisk | null;
  confidence: number | null;
  curatorRunId: string | null;
  reason: string | null;
  sourceIds: string[];
  decidedAt: string | null;
  decidedBy: string | null;
  appliedCommit: string | null;
  /** Repo-relative path of the proposal markdown file. */
  path: string;
  hasProposedContent: boolean;
  body: string;
}

function proposalDirs(repoRoot: string): Record<"pending" | "applied" | "rejected" | "superseded", string> {
  const paths = getRepoPaths(repoRoot);
  return {
    pending: paths.proposalsPendingDir,
    applied: paths.proposalsAppliedDir,
    rejected: join(repoRoot, "proposals", "rejected"),
    superseded: join(repoRoot, "proposals", "superseded")
  };
}

function dirForStatus(repoRoot: string, status: ProposalStatus): string {
  const dirs = proposalDirs(repoRoot);
  if (status === "applied") {
    return dirs.applied;
  }
  if (status === "rejected") {
    return dirs.rejected;
  }
  if (status === "superseded") {
    return dirs.superseded;
  }
  return dirs.pending;
}

export function proposalContentFileName(id: string): string {
  return `${id}.content.md`;
}

export function isSafeProposalId(id: string): boolean {
  return isSafeManifestId(id) && !id.endsWith(".content");
}

function sha256Hex(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

/** Hash used for drift checks; "absent" means the target page does not exist yet. */
export function proposalBaseHash(content: string | null): string {
  return content === null ? "absent" : sha256Hex(content);
}

function normalizeStatus(value: unknown): ProposalStatus {
  const status = String(value || "pending");
  return (PROPOSAL_STATUSES as string[]).includes(status) ? (status as ProposalStatus) : "pending";
}

function stringOrNull(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function riskOrNull(value: unknown): ProposalRisk | null {
  return value === "low" || value === "medium" || value === "high" ? value : null;
}

function recordFromFile(repoRoot: string, absolutePath: string, raw: string): ProposalRecord {
  const { data, body } = parseFrontmatter(raw);
  const id = basename(absolutePath, ".md");
  const confidenceRaw = typeof data.confidence === "number" ? data.confidence : Number(data.confidence ?? Number.NaN);
  return {
    id,
    title: stringOrNull(data.title) || id,
    status: normalizeStatus(data.status),
    createdAt: stringOrNull(data.created_at) || isoNow(),
    updatedAt: stringOrNull(data.updated_at) || isoNow(),
    targetPage: stringOrNull(data.target_page),
    baseSha256: stringOrNull(data.base_sha256),
    risk: riskOrNull(data.risk),
    confidence: Number.isFinite(confidenceRaw) ? Math.max(0, Math.min(1, confidenceRaw)) : null,
    curatorRunId: stringOrNull(data.curator_run_id),
    reason: stringOrNull(data.reason),
    sourceIds: Array.isArray(data.source_ids) ? data.source_ids.filter((value): value is string => typeof value === "string") : [],
    decidedAt: stringOrNull(data.decided_at),
    decidedBy: stringOrNull(data.decided_by),
    appliedCommit: stringOrNull(data.applied_commit),
    path: toRepoRelative(repoRoot, absolutePath),
    hasProposedContent: existsSync(join(dirname(absolutePath), proposalContentFileName(id))),
    body
  };
}

async function listProposalFiles(dir: string): Promise<string[]> {
  if (!existsSync(dir)) {
    return [];
  }
  const names = await readdir(dir);
  return names
    .filter((name) => name.endsWith(".md") && !name.endsWith(".content.md"))
    .map((name) => join(dir, name));
}

export async function listProposals(repoRoot: string, statuses: ProposalStatus[] | null = null): Promise<ProposalRecord[]> {
  const dirs = proposalDirs(repoRoot);
  const records: ProposalRecord[] = [];
  for (const dir of [dirs.pending, dirs.applied, dirs.rejected, dirs.superseded]) {
    for (const file of await listProposalFiles(dir)) {
      try {
        records.push(recordFromFile(repoRoot, file, await readFile(file, "utf8")));
      } catch {
        // Unreadable proposal files are skipped from listings; they remain on disk
        // for manual inspection.
      }
    }
  }
  const filtered = statuses ? records.filter((record) => statuses.includes(record.status)) : records;
  return filtered.sort((a, b) => (a.createdAt < b.createdAt ? 1 : a.createdAt > b.createdAt ? -1 : 0));
}

export async function findProposal(repoRoot: string, id: string): Promise<{ record: ProposalRecord; absolutePath: string } | null> {
  if (!isSafeProposalId(id)) {
    return null;
  }
  const dirs = proposalDirs(repoRoot);
  for (const dir of [dirs.pending, dirs.applied, dirs.rejected, dirs.superseded]) {
    const candidate = join(dir, `${id}.md`);
    if (existsSync(candidate)) {
      return { record: recordFromFile(repoRoot, candidate, await readFile(candidate, "utf8")), absolutePath: candidate };
    }
  }
  return null;
}

export async function readProposedContent(repoRoot: string, record: ProposalRecord): Promise<string | null> {
  const dirs = proposalDirs(repoRoot);
  for (const dir of [dirs.pending, dirs.applied, dirs.rejected, dirs.superseded]) {
    const candidate = join(dir, proposalContentFileName(record.id));
    if (existsSync(candidate)) {
      return await readFile(candidate, "utf8");
    }
  }
  return null;
}

function frontmatterFromRecord(record: ProposalRecord): FrontmatterData {
  const data: FrontmatterData = {
    title: record.title,
    type: "proposal",
    proposal_id: record.id,
    created_at: record.createdAt,
    updated_at: isoNow(),
    status: record.status,
    tags: ["proposal"],
    aliases: [],
    source_ids: record.sourceIds
  };
  if (record.targetPage) data.target_page = record.targetPage;
  if (record.baseSha256) data.base_sha256 = record.baseSha256;
  if (record.risk) data.risk = record.risk;
  if (record.confidence !== null) data.confidence = record.confidence;
  if (record.curatorRunId) data.curator_run_id = record.curatorRunId;
  if (record.reason) data.reason = record.reason;
  if (record.decidedAt) data.decided_at = record.decidedAt;
  if (record.decidedBy) data.decided_by = record.decidedBy;
  if (record.appliedCommit) data.applied_commit = record.appliedCommit;
  return data;
}

async function saveRecord(repoRoot: string, record: ProposalRecord, currentAbsolutePath: string): Promise<string[]> {
  const touched: string[] = [];
  const targetDir = dirForStatus(repoRoot, record.status);
  const targetPath = join(targetDir, `${record.id}.md`);
  const text = stringifyFrontmatter(frontmatterFromRecord(record), record.body);
  await mkdir(targetDir, { recursive: true });
  if (currentAbsolutePath !== targetPath) {
    // Move the proposal file and its content sidecar together.
    const sidecarFrom = join(dirname(currentAbsolutePath), proposalContentFileName(record.id));
    const sidecarTo = join(targetDir, proposalContentFileName(record.id));
    if (existsSync(sidecarFrom)) {
      await rename(sidecarFrom, sidecarTo);
      touched.push(toRepoRelative(repoRoot, sidecarFrom), toRepoRelative(repoRoot, sidecarTo));
    }
    await rm(currentAbsolutePath, { force: true });
    touched.push(toRepoRelative(repoRoot, currentAbsolutePath));
  }
  await writeFile(targetPath, text, "utf8");
  touched.push(toRepoRelative(repoRoot, targetPath));
  record.path = toRepoRelative(repoRoot, targetPath);
  return touched;
}

// --- diff helpers -------------------------------------------------------------

export interface LineDiffStats {
  added: number;
  removed: number;
  changedLines: number;
  hasDeletes: boolean;
}

const DIFF_MAX_LINES = 20_000;

/** LCS-based line diff stats. Throws when inputs exceed the diff budget. */
export function computeLineDiffStats(before: string | null, after: string): LineDiffStats {
  const beforeLines = before === null ? [] : before.split("\n");
  const afterLines = after.split("\n");
  if (beforeLines.length > DIFF_MAX_LINES || afterLines.length > DIFF_MAX_LINES) {
    throw new Error(`diff exceeds ${DIFF_MAX_LINES} line budget`);
  }
  const lcs = lcsLength(beforeLines, afterLines);
  const removed = beforeLines.length - lcs;
  const added = afterLines.length - lcs;
  return { added, removed, changedLines: added + removed, hasDeletes: removed > 0 };
}

function lcsLength(a: string[], b: string[]): number {
  if (!a.length || !b.length) {
    return 0;
  }
  let previous = new Array<number>(b.length + 1).fill(0);
  let current = new Array<number>(b.length + 1).fill(0);
  for (let i = 1; i <= a.length; i += 1) {
    for (let j = 1; j <= b.length; j += 1) {
      current[j] = a[i - 1] === b[j - 1] ? previous[j - 1] + 1 : Math.max(previous[j], current[j - 1]);
    }
    [previous, current] = [current, previous];
  }
  return previous[b.length];
}

/** Compact unified-style diff for display; bounded output. */
export function renderUnifiedDiff(before: string | null, after: string, maxLines = 400): string {
  const beforeLines = before === null ? [] : before.split("\n");
  const afterLines = after.split("\n");
  if (beforeLines.length > DIFF_MAX_LINES || afterLines.length > DIFF_MAX_LINES) {
    return "(diff too large to render)";
  }
  // Build LCS table coordinates via backtracking on a full table when small, else fall
  // back to a whole-file replacement view.
  if (beforeLines.length * afterLines.length > 4_000_000) {
    return "(diff too large to render)";
  }
  const table: number[][] = [];
  for (let i = 0; i <= beforeLines.length; i += 1) {
    table.push(new Array<number>(afterLines.length + 1).fill(0));
  }
  for (let i = 1; i <= beforeLines.length; i += 1) {
    for (let j = 1; j <= afterLines.length; j += 1) {
      table[i][j] = beforeLines[i - 1] === afterLines[j - 1] ? table[i - 1][j - 1] + 1 : Math.max(table[i - 1][j], table[i][j - 1]);
    }
  }
  const lines: string[] = [];
  let i = beforeLines.length;
  let j = afterLines.length;
  const reversed: string[] = [];
  while (i > 0 || j > 0) {
    if (i > 0 && j > 0 && beforeLines[i - 1] === afterLines[j - 1]) {
      reversed.push(`  ${beforeLines[i - 1]}`);
      i -= 1;
      j -= 1;
    } else if (j > 0 && (i === 0 || table[i][j - 1] >= table[i - 1][j])) {
      reversed.push(`+ ${afterLines[j - 1]}`);
      j -= 1;
    } else {
      reversed.push(`- ${beforeLines[i - 1]}`);
      i -= 1;
    }
  }
  reversed.reverse();
  // Keep only changed lines with one line of context on each side.
  const keep = new Set<number>();
  reversed.forEach((line, index) => {
    if (!line.startsWith("  ")) {
      keep.add(index - 1);
      keep.add(index);
      keep.add(index + 1);
    }
  });
  let lastKept = -2;
  for (let index = 0; index < reversed.length; index += 1) {
    if (!keep.has(index)) {
      continue;
    }
    if (index !== lastKept + 1 && lines.length) {
      lines.push("  …");
    }
    lines.push(reversed[index]);
    lastKept = index;
    if (lines.length >= maxLines) {
      lines.push(`  … (diff truncated at ${maxLines} lines)`);
      break;
    }
  }
  return lines.join("\n") || "(no changes)";
}

// --- policy -------------------------------------------------------------------

export function globMatchesPath(pattern: string, path: string): boolean {
  const escaped = pattern
    .split("**")
    .map((part) =>
      part
        .replace(/[.+^${}()|[\]\\]/g, "\\$&")
        .replace(/\*/g, "[^/]*")
        .replace(/\?/g, "[^/]")
    )
    .join("(?:.*)?");
  return new RegExp(`^${escaped}$`).test(path);
}

export interface AutoApplyDecision {
  allowed: boolean;
  reasons: string[];
}

export function evaluateAutoApply(
  policy: CurationPolicy,
  record: Pick<ProposalRecord, "targetPage" | "baseSha256" | "risk">,
  currentContent: string | null,
  proposedContent: string
): AutoApplyDecision {
  const reasons: string[] = [];
  if (policy.mode !== "auto") {
    reasons.push(`curation mode is ${policy.mode}`);
  }
  if (record.risk !== "low") {
    reasons.push(`risk is ${record.risk || "unset"}; only low-risk proposals auto-apply`);
  }
  if (!record.targetPage) {
    reasons.push("no target page");
  } else if (!policy.allowedPaths.some((pattern) => globMatchesPath(pattern, record.targetPage!))) {
    reasons.push(`target ${record.targetPage} is outside autoApply.allowedPaths`);
  }
  // Unattended mutation always requires the drift guard; humans can still apply
  // guard-less proposals explicitly.
  if (!record.baseSha256) {
    reasons.push("proposal has no base_sha256 drift guard; auto-apply requires one");
  } else if (record.baseSha256 !== proposalBaseHash(currentContent)) {
    reasons.push("target page changed since the proposal was generated");
  }
  try {
    const stats = computeLineDiffStats(currentContent, proposedContent);
    if (stats.changedLines > policy.maxChangedLines) {
      reasons.push(`${stats.changedLines} changed line(s) exceed autoApply.maxChangedLines=${policy.maxChangedLines}`);
    }
    if (stats.hasDeletes && !policy.allowDeletes) {
      reasons.push("proposal removes lines and autoApply.allowDeletes is false");
    }
  } catch (error) {
    reasons.push(error instanceof Error ? error.message : String(error));
  }
  return { allowed: reasons.length === 0, reasons };
}

// --- transitions ----------------------------------------------------------------

export function validateProposalTargetPage(repoRoot: string, targetPage: string): string {
  const normalized = targetPage.replace(/^\/+/, "").trim();
  if (!normalized.startsWith("wiki/") || !normalized.endsWith(".md")) {
    throw new Error(`proposal target page must be a wiki markdown path: ${normalized}`);
  }
  if (normalized.split("/").some((part) => !part || part === ".." || part === "." || part.startsWith("."))) {
    throw new Error(`proposal target page contains unsafe segments: ${normalized}`);
  }
  safeRepoPath(repoRoot, normalized);
  return normalized;
}

export interface ProposalDecisionResult {
  record: ProposalRecord;
  touchedFiles: string[];
}

export async function approveProposal(repoRoot: string, id: string, decidedBy: string): Promise<ProposalDecisionResult> {
  const found = await findProposal(repoRoot, id);
  if (!found) {
    throw new Error(`proposal not found: ${id}`);
  }
  if (!OPEN_PROPOSAL_STATUSES.includes(found.record.status)) {
    throw new Error(`proposal ${id} is ${found.record.status}; only open proposals can be approved`);
  }
  found.record.status = "approved";
  found.record.decidedAt = isoNow();
  found.record.decidedBy = decidedBy;
  const touched = await saveRecord(repoRoot, found.record, found.absolutePath);
  touched.push(
    await appendLogEntry(repoRoot, "proposal-approve", found.record.path, found.record.title, [
      `operation: proposal-approve`,
      `inputs: decided_by=${decidedBy}`,
      `files: [[${found.record.path}]]`,
      `commit: pending`,
      `summary: Approved proposal ${id}; it can now be applied.`
    ])
  );
  return { record: found.record, touchedFiles: touched };
}

export async function rejectProposal(repoRoot: string, id: string, decidedBy: string, reason: string | null): Promise<ProposalDecisionResult> {
  const found = await findProposal(repoRoot, id);
  if (!found) {
    throw new Error(`proposal not found: ${id}`);
  }
  if (!OPEN_PROPOSAL_STATUSES.includes(found.record.status)) {
    throw new Error(`proposal ${id} is ${found.record.status}; only open proposals can be rejected`);
  }
  found.record.status = "rejected";
  found.record.decidedAt = isoNow();
  found.record.decidedBy = decidedBy;
  if (reason) {
    found.record.reason = reason;
  }
  const touched = await saveRecord(repoRoot, found.record, found.absolutePath);
  touched.push(
    await appendLogEntry(repoRoot, "proposal-reject", found.record.path, found.record.title, [
      `operation: proposal-reject`,
      `inputs: decided_by=${decidedBy}${reason ? `; reason=${reason}` : ""}`,
      `files: [[${found.record.path}]]`,
      `commit: pending`,
      `summary: Rejected proposal ${id}.`
    ])
  );
  return { record: found.record, touchedFiles: touched };
}

export interface ApplyProposalResult {
  applied: boolean;
  record: ProposalRecord;
  touchedFiles: string[];
  /** Set when the proposal could not be applied and was parked as needs-human. */
  blockedReason: string | null;
  supersededIds: string[];
}

export async function applyProposal(repoRoot: string, id: string, decidedBy: string): Promise<ApplyProposalResult> {
  const found = await findProposal(repoRoot, id);
  if (!found) {
    throw new Error(`proposal not found: ${id}`);
  }
  if (!OPEN_PROPOSAL_STATUSES.includes(found.record.status)) {
    throw new Error(`proposal ${id} is ${found.record.status}; only open proposals can be applied`);
  }
  if (!found.record.targetPage) {
    throw new Error(`proposal ${id} has no target page; nothing to apply`);
  }
  const proposedContent = await readProposedContent(repoRoot, found.record);
  if (proposedContent === null) {
    throw new Error(`proposal ${id} has no proposed content sidecar; nothing to apply`);
  }
  const targetPage = validateProposalTargetPage(repoRoot, found.record.targetPage);
  const targetAbsolute = safeRepoPath(repoRoot, targetPage);
  const currentContent = existsSync(targetAbsolute) ? await readFile(targetAbsolute, "utf8") : null;

  // Drift check: never silently clobber a page that changed since the proposal was
  // generated. Park the proposal for a human instead.
  if (found.record.baseSha256 && found.record.baseSha256 !== proposalBaseHash(currentContent)) {
    found.record.status = "needs-human";
    found.record.reason = `target page changed since this proposal was generated (expected base ${found.record.baseSha256.slice(0, 12)}…); regenerate or resolve manually`;
    const touched = await saveRecord(repoRoot, found.record, found.absolutePath);
    touched.push(
      await appendLogEntry(repoRoot, "proposal-blocked", found.record.path, found.record.title, [
        `operation: proposal-blocked`,
        `inputs: decided_by=${decidedBy}`,
        `files: [[${found.record.path}]]`,
        `commit: pending`,
        `summary: Apply blocked by target drift; proposal moved to needs-human.`
      ])
    );
    return { applied: false, record: found.record, touchedFiles: touched, blockedReason: found.record.reason, supersededIds: [] };
  }

  await mkdir(dirname(targetAbsolute), { recursive: true });
  await writeFile(targetAbsolute, proposedContent, "utf8");

  found.record.status = "applied";
  found.record.decidedAt = isoNow();
  found.record.decidedBy = decidedBy;
  const touched = await saveRecord(repoRoot, found.record, found.absolutePath);
  touched.push(targetPage);

  // Other open proposals for the same target are now stale: supersede them.
  const supersededIds: string[] = [];
  for (const other of await listProposals(repoRoot, OPEN_PROPOSAL_STATUSES)) {
    if (other.id === id || other.targetPage !== targetPage) {
      continue;
    }
    const otherFound = await findProposal(repoRoot, other.id);
    if (!otherFound) {
      continue;
    }
    otherFound.record.status = "superseded";
    otherFound.record.decidedAt = isoNow();
    otherFound.record.decidedBy = "system";
    otherFound.record.reason = `superseded by proposal ${id}`;
    touched.push(...(await saveRecord(repoRoot, otherFound.record, otherFound.absolutePath)));
    supersededIds.push(other.id);
  }

  touched.push(
    await appendLogEntry(repoRoot, "proposal-apply", found.record.path, found.record.title, [
      `operation: proposal-apply`,
      `inputs: decided_by=${decidedBy}${supersededIds.length ? `; superseded=${supersededIds.join(",")}` : ""}`,
      `files: [[${targetPage}]], [[${found.record.path}]]`,
      `commit: pending`,
      `summary: Applied proposal ${id} to ${targetPage}.`
    ])
  );
  return { applied: true, record: found.record, touchedFiles: touched, blockedReason: null, supersededIds };
}

// --- curator status -------------------------------------------------------------

export interface CuratorStatus {
  installed: boolean;
  mode: CurationMode;
  last_run_id: string | null;
  last_run_started_at: string | null;
  last_run_finished_at: string | null;
  last_run_ok: boolean | null;
  last_run_failures: string[];
  last_run_summary: string | null;
  next_run_at: string | null;
  /** ISO timestamp of the newest material the curator has reviewed. */
  cursor: string | null;
  updated_at: string;
}

function curatorStatusPath(repoRoot: string): string {
  return join(getRepoPaths(repoRoot).derivedDir, "curator-status.json");
}

export async function readCuratorStatus(repoRoot: string): Promise<CuratorStatus> {
  const policy = curationPolicyFromEnv();
  const fallback: CuratorStatus = {
    installed: false,
    mode: policy.mode,
    last_run_id: null,
    last_run_started_at: null,
    last_run_finished_at: null,
    last_run_ok: null,
    last_run_failures: [],
    last_run_summary: null,
    next_run_at: null,
    cursor: null,
    updated_at: isoNow()
  };
  const path = curatorStatusPath(repoRoot);
  if (!existsSync(path)) {
    return fallback;
  }
  try {
    const parsed = JSON.parse(await readFile(path, "utf8")) as Partial<CuratorStatus>;
    return {
      ...fallback,
      ...parsed,
      mode: policy.mode,
      last_run_failures: Array.isArray(parsed.last_run_failures)
        ? parsed.last_run_failures.filter((value): value is string => typeof value === "string").slice(0, 20)
        : []
    };
  } catch {
    return fallback;
  }
}

const CURATOR_STATUS_MAX_STRING = 2_000;

export async function writeCuratorStatus(repoRoot: string, update: Partial<CuratorStatus>): Promise<CuratorStatus> {
  const current = await readCuratorStatus(repoRoot);
  const clampString = (value: unknown): string | null =>
    typeof value === "string" && value.trim() ? value.trim().slice(0, CURATOR_STATUS_MAX_STRING) : null;
  const next: CuratorStatus = {
    ...current,
    installed: typeof update.installed === "boolean" ? update.installed : current.installed,
    last_run_id: update.last_run_id !== undefined ? clampString(update.last_run_id) : current.last_run_id,
    last_run_started_at: update.last_run_started_at !== undefined ? clampString(update.last_run_started_at) : current.last_run_started_at,
    last_run_finished_at: update.last_run_finished_at !== undefined ? clampString(update.last_run_finished_at) : current.last_run_finished_at,
    last_run_ok: typeof update.last_run_ok === "boolean" ? update.last_run_ok : update.last_run_ok === null ? null : current.last_run_ok,
    last_run_failures: Array.isArray(update.last_run_failures)
      ? update.last_run_failures
          .filter((value): value is string => typeof value === "string")
          .map((value) => value.slice(0, CURATOR_STATUS_MAX_STRING))
          .slice(0, 20)
      : current.last_run_failures,
    last_run_summary: update.last_run_summary !== undefined ? clampString(update.last_run_summary) : current.last_run_summary,
    next_run_at: update.next_run_at !== undefined ? clampString(update.next_run_at) : current.next_run_at,
    cursor: update.cursor !== undefined ? clampString(update.cursor) : current.cursor,
    updated_at: isoNow()
  };
  const path = curatorStatusPath(repoRoot);
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify(next, null, 2)}\n`, "utf8");
  return next;
}

export interface CurationOverview {
  policy: CurationPolicy;
  status: CuratorStatus;
  counts: Record<ProposalStatus, number>;
  openProposals: ProposalRecord[];
  recentlyApplied: ProposalRecord[];
}

export async function curationOverview(repoRoot: string, statusRoot: string = repoRoot): Promise<CurationOverview> {
  const policy = curationPolicyFromEnv();
  const status = await readCuratorStatus(statusRoot);
  const all = await listProposals(repoRoot);
  const counts: Record<ProposalStatus, number> = {
    pending: 0,
    approved: 0,
    applied: 0,
    rejected: 0,
    superseded: 0,
    "needs-human": 0
  };
  for (const record of all) {
    counts[record.status] += 1;
  }
  return {
    policy,
    status,
    counts,
    openProposals: all.filter((record) => OPEN_PROPOSAL_STATUSES.includes(record.status)).slice(0, 20),
    recentlyApplied: all.filter((record) => record.status === "applied").slice(0, 8)
  };
}
