import { existsSync, readdirSync, readFileSync, realpathSync, statSync } from "node:fs";
import { lstat, mkdir, readFile, readdir, realpath, rm } from "node:fs/promises";
import { isIP } from "node:net";
import { basename, dirname, join, resolve } from "node:path";
import { createInterface } from "node:readline/promises";
import {
  assertOutboxCapacity,
  buildOutboxItem,
  decodeOutboxPayload,
  ensurePrivateOutboxDir,
  outboxDestinationForKey,
  outboxItemFileBytes,
  outboxLimitsFromEnv,
  normalizeOutboxItem as normalizeSharedOutboxItem,
  outboxItemKey as sharedOutboxItemKey,
  purgeCorruptOutboxEntries,
  sanitizedHttpError,
  scanOutbox,
  writeOutboxItem,
  type OutboxEntry,
  type OutboxItem as SharedOutboxItem
} from "../../../outbox/src/outbox";
import { flag, flagValues, hasFlag, requireFlagValue, type ParsedArgs } from "../args";
import { abs, absWithHome, shellSingleQuote } from "../paths";
import { arrayAt, parseSimpleYaml, stringAt, type BrainstackConfig, type DoctorCheck } from "../config";
import { ensureDir, run, writeText } from "../runtime";

type ProjectOutboxDeps = {
  clientEnvPathAbs: (cfg: BrainstackConfig) => string;
  clientLocalPathAbs: (cfg: BrainstackConfig) => string;
  loadConfig: (path?: string | null, profile?: string | null, root?: string | null) => Promise<BrainstackConfig>;
  gitExists: (path: string) => boolean;
  commandPath: (name: string) => string | null;
  outboxParentRoot: (cfg: BrainstackConfig) => string;
  outboxRoot: (cfg: BrainstackConfig) => string;
  outboxRootForDestination: (cfg: BrainstackConfig, destination: { brainId?: string; baseUrl?: string }) => string;
  check: (status: import("../config").CheckStatus, section: string, name: string, detail: string, remediation?: string) => DoctorCheck;
  sha256Hex: (text: string) => string;
  parsePositiveIntegerFlag: (args: ParsedArgs, key: string, fallback: number) => number;
  brainApiRequest: (cfg: BrainstackConfig, method: "GET" | "POST", path: string, options?: { admin?: boolean; body?: Record<string, unknown>; timeoutMs?: number; idempotencyKey?: string }) => Promise<Record<string, unknown>>;
  fetchProposalDetail: (cfg: BrainstackConfig, id: string) => Promise<{ proposal: Record<string, unknown>; body: string }>;
  proposalNeedsContext: (proposal: Record<string, unknown>) => boolean;
};

export function createProjectOutboxCommands(deps: ProjectOutboxDeps) {
  const {
    clientEnvPathAbs,
    clientLocalPathAbs,
    loadConfig,
    gitExists,
    commandPath,
    outboxParentRoot,
    outboxRoot,
    outboxRootForDestination,
    check,
    sha256Hex,
    parsePositiveIntegerFlag,
    brainApiRequest,
    fetchProposalDetail,
    proposalNeedsContext
  } = deps;

function readEnvFile(path: string): Record<string, string> {
    if (!existsSync(path)) {
      return {};
    }
    const env: Record<string, string> = {};
    for (const line of readFileSync(path, "utf8").split(/\r?\n/)) {
      const match = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
      if (match) {
        env[match[1]] = match[2].replace(/^['"]|['"]$/g, "");
      }
    }
    return env;
  }

  function clientEnv(cfg: BrainstackConfig): Record<string, string> {
    return readEnvFile(clientEnvPathAbs(cfg));
  }

  function resolveClientEnvValue(cfg: BrainstackConfig, name: string): string {
    return process.env[name] || clientEnv(cfg)[name] || "";
  }

  function brainWriteConfig(cfg: BrainstackConfig): { baseUrl: string; token: string } {
    const env = clientEnv(cfg);
    const baseUrl = process.env.BRAIN_BASE_URL || env.BRAIN_BASE_URL || cfg.brain.publicBaseUrl;
    const tokenValue = process.env.BRAIN_IMPORT_TOKEN || env.BRAIN_IMPORT_TOKEN || "";
    const legacyToken = process.env.BRAIN_WRITE_TOKEN || env.BRAIN_WRITE_TOKEN || "";
    if (legacyToken && !tokenValue) {
      throw new Error("BRAIN_WRITE_TOKEN is no longer accepted; set BRAIN_IMPORT_TOKEN for client writes and keep BRAIN_ADMIN_TOKEN only on the control host.");
    }
    return { baseUrl, token: tokenValue };
  }

  type ProjectBrainClassification = "work" | "personal" | "neutral";
  type ProjectBrainWriteMode = "true" | "propose-only" | "false";

  interface ProjectBrain {
    id: string;
    label: string;
    classification: ProjectBrainClassification;
    localPath: string;
    gitRemote: string;
    baseUrl: string;
    importTokenEnv: string;
    connectionTrusted: boolean;
    untrustedConnectionFields?: string[];
    sections: string[];
    sectionsRestricted?: boolean;
    readMode?: "allow" | "ask-once" | "ask-always" | "never";
    write: ProjectBrainWriteMode;
    requestedWrite?: ProjectBrainWriteMode;
    writeTrusted?: boolean;
    writeDowngraded?: boolean;
  }

  interface ResolvedProjectContext {
    repo: string;
    configPath: string | null;
    brains: ProjectBrain[];
    allowedBrains: ProjectBrain[];
    deniedBrains: ProjectBrain[];
    writeDefault: string;
    crossBrainWrites: Record<string, string>;
    sessionPath: string;
  }

  interface AllowRule {
    decision: string;
    sections: string[];
    updated_at: string;
  }

  function projectSessionPath(cfg: BrainstackConfig, repo: string): string {
    return join(cfg.paths.stateRoot, "context-sessions", `${sha256Hex(repo).slice(0, 32)}.json`);
  }

  function allowRulesPath(cfg: BrainstackConfig): string {
    return join(cfg.paths.configRoot, "allow-rules.json");
  }

  function profilesPath(cfg: BrainstackConfig): string {
    return join(cfg.paths.configRoot, "profiles.yaml");
  }

  function normalizeProjectBrainWrite(value: unknown, fallback: ProjectBrain["write"]): ProjectBrain["write"] {
    if (value === false) {
      return "false";
    }
    if (value === true) {
      return "true";
    }
    if (typeof value !== "string" || !value.trim()) {
      return fallback;
    }
    const normalized = value.trim();
    if (normalized === "true" || normalized === "propose-only" || normalized === "false") {
      return normalized;
    }
    throw new Error(`Unsupported project brain write mode: ${normalized}`);
  }

  function normalizeProjectReadMode(value: unknown, fallback: ProjectBrain["readMode"] = "allow"): ProjectBrain["readMode"] {
    if (value === false) {
      return "never";
    }
    if (value === true || value === undefined || value === null || value === "") {
      return fallback;
    }
    if (typeof value === "string") {
      const normalized = value.trim().toLowerCase();
      if (normalized === "true" || normalized === "allow") {
        return "allow";
      }
      if (normalized === "false" || normalized === "never" || normalized === "deny") {
        return "never";
      }
      if (normalized === "ask-once" || normalized === "ask-always") {
        return normalized;
      }
    }
    if (typeof value === "object" && value && !Array.isArray(value)) {
      return normalizeProjectReadMode((value as Record<string, unknown>).mode, fallback);
    }
    throw new Error(`Unsupported project brain read mode: ${String(value)}`);
  }

  function applyTrustedClassification(
    trusted: ProjectBrainClassification | "",
    actual: ProjectBrainClassification
  ): ProjectBrainClassification {
    if (trusted === "personal") {
      return "personal";
    }
    if (trusted === "work") {
      return actual === "personal" ? "personal" : "work";
    }
    return actual;
  }

  function normalizeProjectBrainClassification(value: unknown): ProjectBrainClassification | "" {
    if (typeof value !== "string") {
      return "";
    }
    const normalized = value.trim().toLowerCase();
    if (normalized === "work" || normalized === "personal" || normalized === "neutral") {
      return normalized;
    }
    throw new Error(`Unsupported project brain classification: ${value}`);
  }

  function inferredProjectBrainClassification(id: string, label: string): ProjectBrainClassification {
    const text = `${id} ${label}`;
    if (/personal|private|journal|health|family|finance/i.test(text)) {
      return "personal";
    }
    if (/work|company|lindy|corp|corpo|business|team/i.test(text)) {
      return "work";
    }
    return "neutral";
  }

  function classifyProjectBrain(id: string, label: string, explicit?: unknown): ProjectBrainClassification {
    const explicitClassification = normalizeProjectBrainClassification(explicit);
    const inferred = inferredProjectBrainClassification(id, label);
    if (!explicitClassification) {
      return inferred;
    }
    if (inferred === "personal") {
      return "personal";
    }
    if (inferred === "work") {
      return explicitClassification === "personal" ? "personal" : "work";
    }
    return explicitClassification;
  }

  function trustedProjectBrainWrite(trustedSource: Record<string, unknown>): ProjectBrainWriteMode | "" {
    return "write" in trustedSource ? normalizeProjectBrainWrite(trustedSource.write, "propose-only") : "";
  }

  function withTrustedProjectBrainWrite(entry: Record<string, unknown>, trustedSource: Record<string, unknown>): Record<string, unknown> {
    const spec = trustedProjectBrainWrite(trustedSource);
    return spec ? { ...entry, __brainstackTrustedWrite: spec } : entry;
  }

  function projectBrainWriteRank(value: ProjectBrainWriteMode): number {
    switch (value) {
      case "false":
        return 0;
      case "propose-only":
        return 1;
      case "true":
        return 2;
    }
  }

  function effectiveProjectBrainWrite(entry: Record<string, unknown>, requested: ProjectBrainWriteMode): {
    write: ProjectBrainWriteMode;
    writeTrusted: boolean;
    writeDowngraded: boolean;
  } {
    const trustedWrite = stringAt(entry, "__brainstackTrustedWrite", "") as ProjectBrainWriteMode | "";
    const ceiling = trustedWrite || "propose-only";
    const write = projectBrainWriteRank(requested) > projectBrainWriteRank(ceiling) ? ceiling : requested;
    return {
      write,
      writeTrusted: requested !== "true" || trustedWrite === "true",
      writeDowngraded: write !== requested
    };
  }

  function isLoopbackHost(value: string): boolean {
    const normalized = value.trim().toLowerCase().replace(/^\[|\]$/g, "").replace(/\.$/, "");
    if (normalized === "localhost" || normalized === "::1") {
      return true;
    }
    if (normalized.startsWith("::ffff:")) {
      const mapped = normalized.slice("::ffff:".length);
      if (mapped.includes(".")) {
        return isLoopbackHost(mapped);
      }
      const groups = mapped.split(":").map((part) => Number.parseInt(part || "0", 16));
      if (groups.length <= 2 && groups.every((part) => Number.isFinite(part) && part >= 0 && part <= 0xffff)) {
        const value = ((groups[0] || 0) << 16) + (groups[1] || 0);
        return ((value >>> 24) & 0xff) === 127;
      }
      return false;
    }
    if (/^127(?:\.|$)/.test(normalized)) {
      return true;
    }
    const ipVersion = isIP(normalized);
    if (ipVersion === 4) {
      return normalized.split(".")[0] === "127";
    }
    return false;
  }

  function isLocalGitRemote(remote: string): boolean {
    const trimmed = remote.trim();
    if (!trimmed) {
      return false;
    }
    if (trimmed.startsWith("/") || trimmed.startsWith("~/") || trimmed === "~" || trimmed.startsWith("./") || trimmed.startsWith("../")) {
      return true;
    }
    try {
      const parsed = new URL(trimmed);
      if (parsed.protocol === "file:") {
        return true;
      }
      if ((parsed.protocol === "ssh:" || parsed.protocol === "git:" || parsed.protocol === "http:" || parsed.protocol === "https:") && isLoopbackHost(parsed.hostname)) {
        return true;
      }
    } catch {
      // Fall through to scp-like syntax checks.
    }
    const scpLike = trimmed.match(/^([^@/:]+@)?(\[[^\]]+\]|[^:/]+):(.+)$/);
    return Boolean(scpLike && isLoopbackHost(scpLike[2] || ""));
  }

  function stripYamlKeyQuotes(input: string): string {
    const trimmed = input.trim();
    if ((trimmed.startsWith('"') && trimmed.endsWith('"')) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
      return trimmed.slice(1, -1);
    }
    return trimmed;
  }

  function objectAt(input: Record<string, unknown>, key: string): Record<string, unknown> {
    const value = input[key];
    return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
  }

  function entriesFromBrainsValue(value: unknown): Array<Record<string, unknown>> {
    if (Array.isArray(value)) {
      return value.map((entry) => {
        if (typeof entry === "string") {
          return { id: entry };
        }
        return entry && typeof entry === "object" ? (entry as Record<string, unknown>) : {};
      });
    }
    if (value && typeof value === "object") {
      return Object.entries(value as Record<string, unknown>).map(([id, entry]) => ({
        id,
        ...(entry && typeof entry === "object" ? (entry as Record<string, unknown>) : {})
      }));
    }
    return [];
  }

  function safeProjectSection(section: string): boolean {
    return Boolean(section) && !section.startsWith("/") && !section.split("/").includes("..") && /^[A-Za-z0-9._/@+-]+(?:\/[A-Za-z0-9._/@+-]+)*$/.test(section);
  }

  function validateProjectSections(id: string, sections: string[]): void {
    const unsafe = sections.filter((section) => !safeProjectSection(section));
    if (unsafe.length) {
      throw new Error(`Invalid project brain config for ${id}: unsafe section path(s) ${unsafe.join(", ")}`);
    }
  }

  function simpleGlobMatches(patternInput: string, repo: string, home: string): boolean {
    let pattern = absWithHome(stripYamlKeyQuotes(patternInput), home);
    if (pattern.endsWith("/**")) {
      const prefixRaw = pattern.slice(0, -3);
      const prefix = existsSync(prefixRaw) ? realpathSync(prefixRaw) : prefixRaw;
      return repo === prefix || repo.startsWith(`${prefix}/`);
    }
    if (!pattern.includes("*") && existsSync(pattern)) {
      pattern = realpathSync(pattern);
    }
    const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*\*/g, ".*").replace(/\*/g, "[^/]*");
    return new RegExp(`^${escaped}$`).test(repo);
  }

  function bestProfilesProjectMatch(profilesRaw: Record<string, unknown>, repo: string, home: string): Record<string, unknown> {
    const projects = objectAt(profilesRaw, "projects");
    let best: { pattern: string; raw: Record<string, unknown> } | null = null;
    for (const [rawPattern, rawValue] of Object.entries(projects)) {
      if (!rawValue || typeof rawValue !== "object" || Array.isArray(rawValue)) {
        continue;
      }
      const pattern = stripYamlKeyQuotes(rawPattern);
      if (!simpleGlobMatches(pattern, repo, home)) {
        continue;
      }
      if (!best || pattern.length > best.pattern.length) {
        best = { pattern, raw: rawValue as Record<string, unknown> };
      }
    }
    return best?.raw || {};
  }

  function mergeRecord(base: Record<string, unknown>, override: Record<string, unknown>): Record<string, unknown> {
    const merged = { ...base, ...override };
    if (base.read && typeof base.read === "object" && override.read && typeof override.read === "object" && !Array.isArray(base.read) && !Array.isArray(override.read)) {
      merged.read = { ...(base.read as Record<string, unknown>), ...(override.read as Record<string, unknown>) };
    }
    return merged;
  }

  function projectBrainRemoteSpec(entry: Record<string, unknown>): string {
    return stringAt(entry, "gitRemote", stringAt(entry, "remote", stringAt(entry, "remoteSsh", "")));
  }

  function projectBrainBaseUrlSpec(entry: Record<string, unknown>): string {
    return stringAt(entry, "baseUrl", stringAt(entry, "url", ""));
  }

  function projectBrainImportTokenEnvSpec(entry: Record<string, unknown>): string {
    return stringAt(entry, "importTokenEnv", "");
  }

  function projectBrainOverrides(raw: Record<string, unknown>, id: string): Record<string, unknown> {
    const value = raw[id];
    if (value && typeof value === "object" && !Array.isArray(value)) {
      const override = value as Record<string, unknown>;
      if ("mode" in override && !("read" in override)) {
        return { ...override, read: { mode: override.mode, sections: override.sections || [] } };
      }
      return override;
    }
    return {};
  }

  function projectBrainLocalPathSpec(entry: Record<string, unknown>): string {
    return stringAt(entry, "localClone", stringAt(entry, "localPath", stringAt(entry, "path", "")));
  }

  function withTrustedLocalPath(entry: Record<string, unknown>, cfg: BrainstackConfig, trustedSource: Record<string, unknown>): Record<string, unknown> {
    const spec = projectBrainLocalPathSpec(trustedSource);
    return spec ? { ...entry, __brainstackTrustedLocalPath: absWithHome(spec, cfg.paths.home) } : entry;
  }

  function withTrustedGitRemote(entry: Record<string, unknown>, trustedSource: Record<string, unknown>): Record<string, unknown> {
    const spec = projectBrainRemoteSpec(trustedSource);
    return spec ? { ...entry, __brainstackTrustedGitRemote: spec } : entry;
  }

  function withTrustedBrainConnection(entry: Record<string, unknown>, trustedSource: Record<string, unknown>): Record<string, unknown> {
    const baseUrl = projectBrainBaseUrlSpec(trustedSource);
    const importTokenEnv = projectBrainImportTokenEnvSpec(trustedSource);
    return {
      ...entry,
      ...(baseUrl ? { __brainstackTrustedBaseUrl: baseUrl } : {}),
      ...(importTokenEnv ? { __brainstackTrustedImportTokenEnv: importTokenEnv } : {})
    };
  }

  function trustedClassificationFor(id: string, trusted: Record<string, unknown>): string {
    if (!Object.keys(trusted).length) {
      return "";
    }
    return classifyProjectBrain(id, stringAt(trusted, "label", id), trusted.classification);
  }

  function mergeTrustedProjectBrain(id: string, entry: Record<string, unknown>, globalBrains: Record<string, unknown>, matchedProjectRaw: Record<string, unknown>, cfg: BrainstackConfig): Record<string, unknown> {
    const globalRaw = objectAt(globalBrains, id);
    const profileOverride = projectBrainOverrides(matchedProjectRaw, id);
    const trustedBase = mergeRecord({ id, ...globalRaw }, profileOverride);
    const merged = mergeRecord(trustedBase, entry);
    const trustedClassification = trustedClassificationFor(id, trustedBase);
    const withLocalPath = withTrustedLocalPath(withTrustedLocalPath(merged, cfg, globalRaw), cfg, profileOverride);
    const withRemote = withTrustedGitRemote(withTrustedGitRemote(withLocalPath, globalRaw), profileOverride);
    const withConnection = withTrustedBrainConnection(withTrustedBrainConnection(withRemote, globalRaw), profileOverride);
    const withWrite = withTrustedProjectBrainWrite(withTrustedProjectBrainWrite(withConnection, globalRaw), profileOverride);
    return trustedClassification ? { ...withWrite, __brainstackTrustedClassification: trustedClassification } : withWrite;
  }

  function policyRank(value: string | undefined): number {
    switch ((value || "").trim().toLowerCase()) {
      case "never":
        return 0;
      case "ask":
      case "ask-once":
      case "ask-always":
        return 1;
      case "allow":
      case "true":
        return 2;
      default:
        return 1;
    }
  }

  function normalizeCrossBrainPolicy(value: string): string {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") {
      return "allow";
    }
    if (normalized === "ask-once" || normalized === "ask-always") {
      return "ask";
    }
    if (normalized === "allow" || normalized === "ask" || normalized === "never") {
      return normalized;
    }
    throw new Error(`Unsupported crossBrainWrites policy: ${value}`);
  }

  function trustedGenericPolicyKeysFor(repoKey: string): string[] {
    const lower = repoKey.toLowerCase();
    const candidates: string[] = [];
    if (lower.startsWith("personalto")) {
      candidates.push("personalToWork", "personalToCompany");
    }
    if (lower.endsWith("topersonal") || lower.endsWith("toprivate") || lower.startsWith("workto") || lower.startsWith("companyto")) {
      candidates.push("workToPersonal", "companyToPersonal");
    }
    return candidates;
  }

  type CrossBrainPolicyRef = { id: string; classification: ProjectBrain["classification"] };

  function projectBrainEntryPolicyRef(entry: Record<string, unknown>): CrossBrainPolicyRef {
    const id = stringAt(entry, "id", "default");
    const label = stringAt(entry, "label", id);
    const rawClassification = classifyProjectBrain(id, label, entry.classification);
    const trustedClassification = stringAt(entry, "__brainstackTrustedClassification", "") as ProjectBrain["classification"] | "";
    return {
      id,
      classification: applyTrustedClassification(trustedClassification, rawClassification)
    };
  }

  function crossBrainPolicyCandidates(source: CrossBrainPolicyRef, target: CrossBrainPolicyRef): string[] {
    const candidates = [
      `${source.id}To${titleCaseBrainKey(target.id)}`,
      `${source.id}To${titleCaseBrainKey(target.classification)}`,
      `${source.classification}To${titleCaseBrainKey(target.id)}`,
      `${source.classification}To${titleCaseBrainKey(target.classification)}`
    ];
    if (source.classification === "personal" && target.classification === "work") {
      candidates.push("personalToWork", "personalToCompany");
    }
    if (source.classification === "work" && target.classification === "personal") {
      candidates.push("workToPersonal", "companyToPersonal");
    }
    return [...new Set(candidates)];
  }

  function assertRepoLocalCrossBrainRuleMonotonic(
    key: string,
    normalized: string,
    trustedRules: Record<string, string>,
    brainEntries: Array<Record<string, unknown>>
  ): void {
    const refs = brainEntries.map((entry) => projectBrainEntryPolicyRef(entry));
    for (const source of refs) {
      for (const target of refs) {
        if (source.id === target.id) {
          continue;
        }
        if (!crossBrainPolicyCandidates(source, target).includes(key)) {
          continue;
        }
        const trustedPolicy = crossBrainPolicyRule(trustedRules, source, target);
        if (policyRank(normalized) > policyRank(trustedPolicy)) {
          throw new Error(
            `Repo-local crossBrainWrites.${key} cannot weaken trusted/effective profile policy ${trustedPolicy} for ${source.id}->${target.id} to ${normalized}. Change ~/.config/brainstack/profiles.yaml if this is intentional.`
          );
        }
      }
    }
  }

  function mergeCrossBrainWritesMonotonic(
    trusted: Record<string, unknown>,
    repoLocal: Record<string, unknown>,
    brainEntries: Array<Record<string, unknown>> = []
  ): Record<string, string> {
    const merged: Record<string, string> = {};
    for (const [key, value] of Object.entries(trusted)) {
      if (typeof value === "string" && value.trim()) {
        merged[key] = normalizeCrossBrainPolicy(value);
      }
    }
    for (const [key, value] of Object.entries(repoLocal)) {
      if (typeof value !== "string" || !value.trim()) {
        continue;
      }
      const normalized = normalizeCrossBrainPolicy(value);
      const existing = merged[key];
      if (existing && policyRank(normalized) > policyRank(existing)) {
        throw new Error(`Repo-local crossBrainWrites.${key} cannot weaken trusted profile policy ${existing} to ${normalized}. Change ~/.config/brainstack/profiles.yaml if this is intentional.`);
      }
      for (const genericKey of trustedGenericPolicyKeysFor(key)) {
        const generic = merged[genericKey];
        if (generic && policyRank(normalized) > policyRank(generic)) {
          throw new Error(`Repo-local crossBrainWrites.${key} cannot weaken trusted generic profile policy ${genericKey}=${generic} to ${normalized}. Change ~/.config/brainstack/profiles.yaml if this is intentional.`);
        }
      }
      assertRepoLocalCrossBrainRuleMonotonic(key, normalized, merged, brainEntries);
      merged[key] = normalized;
    }
    return merged;
  }

  function projectRawWithProfiles(fileRaw: Record<string, unknown>, profilesRaw: Record<string, unknown>, repo: string, cfg: BrainstackConfig): Record<string, unknown> {
    const defaultRaw = objectAt(profilesRaw, "default");
    const matchedProjectRaw = bestProfilesProjectMatch(profilesRaw, repo, cfg.paths.home);
    const globalBrains = objectAt(profilesRaw, "brains");
    const selectorRaw = "brains" in fileRaw ? fileRaw : Object.keys(matchedProjectRaw).length ? matchedProjectRaw : defaultRaw;
    const selectorEntries = entriesFromBrainsValue(selectorRaw.brains);
    const entries = selectorEntries.length
      ? selectorEntries
      : entriesFromBrainsValue(defaultRaw.brains).length
        ? entriesFromBrainsValue(defaultRaw.brains)
        : [];
    const brains = entries.map((entry) => {
      const id = stringAt(entry, "id", "default");
      return mergeTrustedProjectBrain(id, entry, globalBrains, matchedProjectRaw, cfg);
    });
    const mergedBrains = "brains" in fileRaw ? projectBrainsMergedWithProfiles(fileRaw.brains, globalBrains, matchedProjectRaw, cfg) : brains;
    return {
      ...defaultRaw,
      ...matchedProjectRaw,
      ...fileRaw,
      brains: mergedBrains,
      crossBrainWrites: mergeCrossBrainWritesMonotonic({
        ...objectAt(defaultRaw, "crossBrainWrites"),
        ...objectAt(matchedProjectRaw, "crossBrainWrites")
      }, objectAt(fileRaw, "crossBrainWrites"), mergedBrains),
      writeDefault: stringAt(fileRaw, "writeDefault", stringAt(fileRaw, "defaultBrain", stringAt(matchedProjectRaw, "writeDefault", stringAt(matchedProjectRaw, "defaultBrain", stringAt(defaultRaw, "writeDefault", stringAt(defaultRaw, "defaultBrain", brains[0]?.id ? String(brains[0].id) : "default"))))))
    };
  }

  function projectBrainsMergedWithProfiles(value: unknown, globalBrains: Record<string, unknown>, matchedProjectRaw: Record<string, unknown>, cfg: BrainstackConfig): Array<Record<string, unknown>> {
    return entriesFromBrainsValue(value).map((entry) => {
      const id = stringAt(entry, "id", "default");
      return mergeTrustedProjectBrain(id, entry, globalBrains, matchedProjectRaw, cfg);
    });
  }

  async function resolveRepoPath(input: string | null): Promise<string> {
    const absolute = abs(input || ".");
    return await realpath(absolute).catch(() => absolute);
  }

  async function findProjectBrainstackConfig(repo: string): Promise<string | null> {
    let current = repo;
    while (true) {
      const candidate = join(current, ".brainstack.yaml");
      if (existsSync(candidate)) {
        return candidate;
      }
      const parent = dirname(current);
      if (parent === current) {
        return null;
      }
      current = parent;
    }
  }

  function safeBrainCloneName(id: string): string {
    const safe = id.replace(/[^A-Za-z0-9_-]+/g, "-").replace(/^-+|-+$/g, "");
    return safe && safe !== "." && safe !== ".." && !safe.startsWith(".") ? safe : `brain-${sha256Hex(id).slice(0, 16)}`;
  }

  function computedProjectBrainLocalPath(cfg: BrainstackConfig, id: string): string {
    if (id === "default") {
      return clientLocalPathAbs(cfg);
    }
    return join(cfg.paths.stateRoot, "brain-clones", safeBrainCloneName(id));
  }

  function localCloneForProfileBrain(entry: Record<string, unknown>, cfg: BrainstackConfig, id: string): { localPath: string; untrustedField?: string } {
    const explicit = projectBrainLocalPathSpec(entry);
    if (explicit) {
      const resolvedPath = absWithHome(explicit, cfg.paths.home);
      const trustedPath = stringAt(entry, "__brainstackTrustedLocalPath", "");
      if (trustedPath && resolvedPath === trustedPath) {
        return { localPath: resolvedPath };
      }
      return { localPath: computedProjectBrainLocalPath(cfg, id), untrustedField: "localPath" };
    }
    return { localPath: computedProjectBrainLocalPath(cfg, id) };
  }

  function projectReadSections(entry: Record<string, unknown>): string[] {
    const direct = arrayAt(entry, "sections").map(String).map((item) => item.trim()).filter(Boolean);
    if (direct.length) {
      return direct;
    }
    const read = entry.read;
    if (read && typeof read === "object" && !Array.isArray(read)) {
      return arrayAt(read as Record<string, unknown>, "sections").map(String).map((item) => item.trim()).filter(Boolean);
    }
    return [];
  }

  function normalizeProjectBrainEntry(entry: Record<string, unknown>, cfg: BrainstackConfig): ProjectBrain {
    if ("section" in entry && !("sections" in entry)) {
      throw new Error("Invalid project brain config: use `sections`, not `section`.");
    }
    const id = stringAt(entry, "id", "default");
    if (id.startsWith("cross:")) {
      throw new Error(`Invalid project brain config for ${id}: brain ids cannot use reserved prefix cross:.`);
    }
    const label = stringAt(entry, "label", id);
    const requestedWrite = normalizeProjectBrainWrite(entry.write, "propose-only");
    const writeState = effectiveProjectBrainWrite(entry, requestedWrite);
    const rawBaseUrl = projectBrainBaseUrlSpec(entry);
    const rawImportTokenEnv = projectBrainImportTokenEnvSpec(entry);
    const trustedBaseUrl = stringAt(entry, "__brainstackTrustedBaseUrl", id === "default" ? cfg.brain.publicBaseUrl : "");
    const trustedImportTokenEnv = stringAt(entry, "__brainstackTrustedImportTokenEnv", id === "default" ? "BRAIN_IMPORT_TOKEN" : "");
    const baseUrl = rawBaseUrl || trustedBaseUrl;
    const importTokenEnv = rawImportTokenEnv || trustedImportTokenEnv;
    const gitRemote = projectBrainRemoteSpec(entry);
    const trustedGitRemote = stringAt(entry, "__brainstackTrustedGitRemote", "");
    if (gitRemote && gitRemote !== trustedGitRemote && isLocalGitRemote(gitRemote)) {
      throw new Error(`Repo-local project brain config for ${id} cannot set local git remote ${gitRemote}. Define trusted local remotes in ${profilesPath(cfg)}.`);
    }
    const localPathResolution = localCloneForProfileBrain(entry, cfg, id);
    const untrustedConnectionFields = [
      rawBaseUrl && rawBaseUrl !== trustedBaseUrl ? "baseUrl" : "",
      rawImportTokenEnv && rawImportTokenEnv !== trustedImportTokenEnv ? "importTokenEnv" : "",
      gitRemote && gitRemote !== trustedGitRemote ? "gitRemote" : "",
      localPathResolution.untrustedField || ""
    ].filter(Boolean);
    const rawClassification = classifyProjectBrain(id, label, entry.classification);
    const trustedClassification = stringAt(entry, "__brainstackTrustedClassification", "") as ProjectBrain["classification"] | "";
    if ("sections" in entry && !Array.isArray(entry.sections)) {
      throw new Error(`Invalid project brain config for ${id}: sections must be a YAML list such as [wiki, raw].`);
    }
    const sections = projectReadSections(entry);
    validateProjectSections(id, sections);
    return {
      id,
      label,
      classification: applyTrustedClassification(trustedClassification, rawClassification),
      localPath: localPathResolution.localPath,
      gitRemote,
      baseUrl,
      importTokenEnv,
      connectionTrusted: untrustedConnectionFields.length === 0,
      untrustedConnectionFields,
      sections,
      readMode: normalizeProjectReadMode("read" in entry ? entry.read : "mode" in entry ? entry.mode : undefined, "allow"),
      write: writeState.write,
      requestedWrite,
      writeTrusted: writeState.writeTrusted,
      writeDowngraded: writeState.writeDowngraded
    };
  }

  function projectBrainsFromRaw(raw: Record<string, unknown>, cfg: BrainstackConfig): ProjectBrain[] {
    const brainsValue = raw.brains;
    const entries: Array<Record<string, unknown>> = Array.isArray(brainsValue)
      ? (brainsValue as Array<Record<string, unknown>>)
      : brainsValue && typeof brainsValue === "object"
        ? Object.entries(brainsValue as Record<string, unknown>).map(([id, value]) => ({
            id,
            ...(value && typeof value === "object" ? (value as Record<string, unknown>) : {})
          }))
        : [];
    if (!entries.length) {
      return [
        {
          id: "default",
          label: "default",
          classification: "neutral",
          localPath: clientLocalPathAbs(cfg),
          gitRemote: cfg.client.remoteSsh,
          baseUrl: cfg.brain.publicBaseUrl,
          importTokenEnv: "BRAIN_IMPORT_TOKEN",
          connectionTrusted: true,
          sections: ["wiki", "raw", "proposals"],
          write: "propose-only"
        }
      ];
    }
    return entries.map((entry) => normalizeProjectBrainEntry(entry, cfg));
  }

  function readAllowRules(cfg: BrainstackConfig): Record<string, Record<string, AllowRule>> {
    const path = allowRulesPath(cfg);
    if (!existsSync(path)) {
      return {};
    }
    try {
      return JSON.parse(readFileSync(path, "utf8")) as Record<string, Record<string, AllowRule>>;
    } catch {
      return {};
    }
  }

  async function writeAllowRules(cfg: BrainstackConfig, rules: Record<string, unknown>): Promise<void> {
    await ensureDir(dirname(allowRulesPath(cfg)));
    await writeText(allowRulesPath(cfg), `${JSON.stringify(rules, null, 2)}\n`, 0o600);
  }

  async function saveAllowRule(cfg: BrainstackConfig, repo: string, brain: string, decision: string, sections: string[]): Promise<void> {
    const rules = readAllowRules(cfg);
    rules[repo] = rules[repo] || {};
    rules[repo][brain] = {
      decision,
      sections,
      updated_at: new Date().toISOString()
    };
    await writeAllowRules(cfg, rules);
  }

  async function promptLine(prompt: string): Promise<string> {
    const rl = createInterface({ input: process.stdin, output: process.stdout, terminal: true });
    try {
      return await rl.question(prompt);
    } finally {
      rl.close();
    }
  }

  async function consumeOnceAllowRules(cfg: BrainstackConfig, repo: string, brainIds: string[]): Promise<void> {
    const rules = readAllowRules(cfg);
    const repoRules = rules[repo];
    if (!repoRules) {
      return;
    }
    let changed = false;
    for (const brainId of brainIds) {
      if (repoRules[brainId]?.decision === "once") {
        delete repoRules[brainId];
        changed = true;
      }
    }
    if (changed) {
      if (!Object.keys(repoRules).length) {
        delete rules[repo];
      }
      await writeAllowRules(cfg, rules);
    }
  }

  async function promptForAllowRule(repo: string, brain: ProjectBrain): Promise<"once" | "always" | "deny" | null> {
    const input = (await promptLine(
      [
        `Repo ${repo} requests access to brain ${brain.id}`,
        brain.sections.length ? `sections: ${brain.sections.join(", ")}` : "sections: all configured sections",
        "Allow? [o]nce / [a]lways for this repo / [d]eny: "
      ].join("\n")
    )).trim().toLowerCase();
    if (input === "o" || input === "once") return "once";
    if (input === "a" || input === "always") return "always";
    if (input === "d" || input === "deny" || input === "n" || input === "no") return "deny";
    return null;
  }

  async function maybePromptForProjectAllows(cfg: BrainstackConfig, resolved: ResolvedProjectContext): Promise<boolean> {
    if (!process.stdin.isTTY || process.env.CI) {
      return false;
    }
    let changed = false;
    for (const brain of resolved.deniedBrains) {
      if (brain.readMode === "never" || !brainNeedsExplicitAllow(brain)) {
        continue;
      }
      const decision = await promptForAllowRule(resolved.repo, brain);
      if (!decision) {
        throw new Error("invalid allow choice; rerun non-interactively with brainctl allow repo --once|--always|--deny");
      }
      await saveAllowRule(cfg, resolved.repo, brain.id, decision, brain.sections);
      changed = true;
    }
    return changed;
  }

  function brainNeedsExplicitAllow(brain: ProjectBrain): boolean {
    return brain.readMode === "ask-once" || brain.readMode === "ask-always" || brain.classification === "personal";
  }

  function applyAllowRuleSections(brain: ProjectBrain, rule: AllowRule | undefined): ProjectBrain {
    if (!rule?.sections?.length) {
      return brain;
    }
    const allowed = new Set(rule.sections);
    const sections = (brain.sections.length ? brain.sections : ["wiki", "raw", "proposals"]).filter((section) => allowed.has(section));
    return { ...brain, sections, sectionsRestricted: true };
  }

  async function resolveProjectContext(cfg: BrainstackConfig, repoInput: string | null): Promise<ResolvedProjectContext> {
    const repo = await resolveRepoPath(repoInput);
    const configPath = await findProjectBrainstackConfig(repo);
    const fileRaw = configPath ? parseSimpleYaml(await readFile(configPath, "utf8")) : {};
    const profilesRaw = existsSync(profilesPath(cfg)) ? parseSimpleYaml(await readFile(profilesPath(cfg), "utf8")) : {};
    const raw = projectRawWithProfiles(fileRaw, profilesRaw, repo, cfg);
    const brains = projectBrainsFromRaw(raw, cfg);
    const writeDefault = stringAt(raw, "writeDefault", stringAt(raw, "defaultBrain", brains[0]?.id || "default"));
    const crossBrainWrites = objectAt(raw, "crossBrainWrites") as Record<string, string>;
    const rules = readAllowRules(cfg)[repo] || {};
    const allowedBrains: ProjectBrain[] = [];
    for (const brain of brains) {
      if (brain.readMode === "never") {
        continue;
      }
      const rule = rules[brain.id];
      if (rule?.decision === "deny") {
        continue;
      }
      if (!brainNeedsExplicitAllow(brain) || rule?.decision === "always" || rule?.decision === "once") {
        allowedBrains.push(applyAllowRuleSections(brain, rule));
      }
    }
    const allowedIds = new Set(allowedBrains.map((brain) => brain.id));
    const deniedBrains = brains.filter((brain) => !allowedIds.has(brain.id));
    return { repo, configPath, brains, allowedBrains, deniedBrains, writeDefault, crossBrainWrites, sessionPath: projectSessionPath(cfg, repo) };
  }

  async function maybeSyncBrainClone(brain: ProjectBrain): Promise<string> {
    if (!brain.connectionTrusted) {
      return "pending-trust";
    }
    if (!gitExists(brain.localPath)) {
      if (!brain.gitRemote) {
        return "missing";
      }
      await mkdir(dirname(brain.localPath), { recursive: true });
      const result = run(["git", "clone", brain.gitRemote, brain.localPath], { check: false, timeoutMs: 60_000 });
      return result.code === 0 ? "cloned" : `missing: clone failed: ${(result.stderr || result.stdout).trim().slice(0, 160)}`;
    }
    const result = run(["git", "pull", "--ff-only"], { cwd: brain.localPath, check: false, timeoutMs: 20_000 });
    return result.code === 0 ? "synced" : `stale: ${(result.stderr || result.stdout).trim().slice(0, 160)}`;
  }

  function untrustedBrainConnectionFields(brain: ProjectBrain): string {
    return brain.untrustedConnectionFields?.length ? brain.untrustedConnectionFields.join(",") : "connection";
  }

  function projectBrainTrustInstruction(cfg: BrainstackConfig, brain: ProjectBrain): string {
    return `define matching connection fields for ${brain.id} in ${profilesPath(cfg)} before Brainstack uses repo-local URL/token/remote/path data`;
  }

  async function commandContext(args: ParsedArgs): Promise<void> {
    const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
    let resolved = await resolveProjectContext(cfg, flag(args, "repo"));
    if (!hasFlag(args, "json") && await maybePromptForProjectAllows(cfg, resolved)) {
      resolved = await resolveProjectContext(cfg, resolved.repo);
    }
    const sync = args.flags["no-sync"] === undefined;
    const brainRows: Array<ProjectBrain & { allowed: boolean; allowedByPolicy: boolean; status: string; displayState: "allowed" | "blocked" | "pending-trust" }> = [];
    const allowedIds = new Set(resolved.allowedBrains.map((brain) => brain.id));
    const syncStatus: Record<string, string> = {};
    for (const brain of resolved.brains) {
      const allowedByPolicy = allowedIds.has(brain.id);
      const allowed = allowedByPolicy && brain.connectionTrusted;
      const status = !brain.connectionTrusted ? "pending-trust" : sync && allowed ? await maybeSyncBrainClone(brain) : gitExists(brain.localPath) ? "local" : "missing";
      syncStatus[brain.id] = status;
      const displayState = !brain.connectionTrusted ? "pending-trust" : allowedByPolicy ? "allowed" : "blocked";
      brainRows.push({ ...brain, allowed, allowedByPolicy, status, displayState });
    }
    const contextSources = sessionSourcesFromBrains(resolved.allowedBrains.filter((brain) => brain.connectionTrusted), syncStatus);
    await updateProjectSession(resolved, { recent_context_sources: contextSources });
    if (hasFlag(args, "json")) {
      console.log(JSON.stringify({ repo: resolved.repo, configPath: resolved.configPath, writeDefault: resolved.writeDefault, crossBrainWrites: resolved.crossBrainWrites, brains: brainRows }, null, 2));
      return;
    }
    console.log(`# Brainstack context for ${resolved.repo}`);
    console.log("");
    console.log(`config=${resolved.configPath || profilesPath(cfg)}`);
    console.log(`write_default=${resolved.writeDefault}`);
    if (Object.keys(resolved.crossBrainWrites).length) {
      console.log(`cross_brain_writes=${JSON.stringify(resolved.crossBrainWrites)}`);
    }
    console.log("");
    console.log("Available brains:");
    for (const brain of brainRows) {
      const readScope = brain.sections.length ? brain.sections.join(",") : "all local sections";
      const writeLabel = brain.id === resolved.writeDefault ? `${brain.write} default` : brain.write;
      console.log(`- [${brain.displayState}] ${brain.id} (${brain.classification})`);
      console.log(`  path=${brain.localPath}`);
      console.log(`  freshness=${brain.status}`);
      console.log(`  read=${brain.readMode || "allow"} sections=${readScope}`);
      if (brain.writeDowngraded) {
        const defaultNote = brain.id === resolved.writeDefault ? " (default)" : "";
        if (brain.requestedWrite === "true" && brain.write === "propose-only") {
          console.log(`  write=${brain.write} pending profile trust; repo-local write:true ignored until trusted${defaultNote}`);
        } else {
          console.log(`  write=${brain.write} profile trust restricts repo-local write:${brain.requestedWrite || "propose-only"}${defaultNote}`);
        }
      } else {
        console.log(`  write=${writeLabel}`);
      }
      if (!brain.connectionTrusted) {
        console.log(`  connection=pending-trust fields=${untrustedBrainConnectionFields(brain)}`);
        console.log(`  trust with: ${projectBrainTrustInstruction(cfg, brain)}`);
      }
    }
    for (const brain of resolved.deniedBrains) {
      const sectionFlag = brain.sections.length ? ` --sections ${shellSingleQuote(brain.sections.join(","))}` : "";
      if (brain.readMode === "never") {
        console.log(`blocked: ${brain.id} has read=false/never in this project context`);
      } else {
        console.log(`allow with: brainctl allow repo --repo ${shellSingleQuote(resolved.repo)} --brain ${shellSingleQuote(brain.id)}${sectionFlag} --always`);
      }
    }
    console.log("");
    console.log("Use `brainctl search --repo . \"query\"` for labelled retrieval.");
    console.log("Use `brainctl remember --repo . --summary \"...\"` for import/propose writes.");
    console.log("Preserve source labels when reasoning across multiple brains. Pending-trust brains must not be used or searched until trusted. Do not manually clone, pull, or POST unless explicitly instructed.");
  }

  function searchLocalBrain(brain: ProjectBrain, query: string): Array<{ brain: string; label: string; classification: string; path: string; line: number; text: string }> {
    if (!gitExists(brain.localPath)) {
      return [];
    }
    const configuredScopes = brain.sections.length ? brain.sections : brain.sectionsRestricted ? [] : ["wiki", "raw", "proposals"];
    const scopes = configuredScopes.filter((scope) => existsSync(join(brain.localPath, scope)));
    if (!scopes.length) {
      return [];
    }
    if (!commandPath("rg")) {
      return searchLocalBrainWithoutRg(brain, query, scopes);
    }
    const result = run(["rg", "-n", "-i", "--fixed-strings", "--", query, ...scopes], {
      cwd: brain.localPath,
      check: false,
      timeoutMs: 15_000
    });
    if (result.code !== 0 && result.code !== 1) {
      throw new Error(`search failed for ${brain.id}: ${(result.stderr || result.stdout).trim()}`);
    }
    return result.stdout
      .split(/\r?\n/)
      .filter(Boolean)
      .slice(0, 100)
      .map((line) => {
        const [path = "", lineNo = "0", ...rest] = line.split(":");
        return { brain: brain.id, label: brain.label, classification: brain.classification, path, line: Number(lineNo) || 0, text: rest.join(":").trim() };
      });
  }

  function searchLocalBrainWithoutRg(brain: ProjectBrain, query: string, scopes: string[]): Array<{ brain: string; label: string; classification: string; path: string; line: number; text: string }> {
    const needle = query.toLocaleLowerCase();
    const results: Array<{ brain: string; label: string; classification: string; path: string; line: number; text: string }> = [];
    const maxFileBytes = 2 * 1024 * 1024;
    const visit = (relDir: string): void => {
      if (results.length >= 100) {
        return;
      }
      const absDir = join(brain.localPath, relDir);
      let entries: ReturnType<typeof readdirSync>;
      try {
        entries = readdirSync(absDir, { withFileTypes: true });
      } catch {
        return;
      }
      for (const entry of entries) {
        if (results.length >= 100) {
          return;
        }
        const relPath = join(relDir, entry.name);
        if (entry.isDirectory()) {
          if (entry.name === ".git" || entry.name === "node_modules" || entry.name === "dist") {
            continue;
          }
          visit(relPath);
          continue;
        }
        if (!entry.isFile()) {
          continue;
        }
        const absPath = join(brain.localPath, relPath);
        try {
          const info = statSync(absPath);
          if (info.size > maxFileBytes) {
            continue;
          }
          const text = readFileSync(absPath, "utf8");
          const lines = text.split(/\r?\n/);
          for (let index = 0; index < lines.length; index += 1) {
            if (lines[index]?.toLocaleLowerCase().includes(needle)) {
              results.push({ brain: brain.id, label: brain.label, classification: brain.classification, path: relPath, line: index + 1, text: lines[index]?.trim() || "" });
              if (results.length >= 100) {
                return;
              }
            }
          }
        } catch {
          continue;
        }
      }
    };
    for (const scope of scopes) {
      visit(scope);
    }
    return results;
  }

  function titleCaseBrainKey(value: string): string {
    return value ? `${value[0]?.toUpperCase() || ""}${value.slice(1)}` : "";
  }

  function sourceRecordId(source: unknown): string {
    return source && typeof source === "object" ? String((source as Record<string, unknown>).id || "") : String(source || "");
  }

  function sourceRecordClassification(source: unknown): ProjectBrain["classification"] {
    if (source && typeof source === "object") {
      const record = source as Record<string, unknown>;
      if (record.classification === "work" || record.classification === "personal" || record.classification === "neutral") {
        return record.classification;
      }
      return classifyProjectBrain(String(record.id || ""), String(record.label || ""));
    }
    return classifyProjectBrain(String(source || ""), String(source || ""));
  }

  function sourceRecordEvidenceRef(source: unknown): string {
    const id = sourceRecordId(source);
    if (!source || typeof source !== "object") {
      return id;
    }
    const record = source as Record<string, unknown>;
    const path = typeof record.path === "string" ? record.path : "";
    const line = typeof record.line === "number" || typeof record.line === "string" ? String(record.line) : "";
    if (id && path) {
      return `${id}:${path}${line ? `:${line}` : ""}`;
    }
    if (id) {
      return id;
    }
    return path;
  }

  function boundedCliString(value: string | undefined, maxLength: number): string | undefined {
    const trimmed = value?.trim() || "";
    return trimmed ? trimmed.slice(0, maxLength) : undefined;
  }

  function deriveProjectLabel(repo: string): string {
    const label = basename(repo).replace(/\.git$/i, "").trim();
    return label || "unknown-repo";
  }

  function validateMemoryScope(scope: string): string {
    const normalized = scope.trim().toLowerCase();
    if (!["repo", "project", "global", "machine", "harness"].includes(normalized)) {
      throw new Error("--scope must be one of repo, project, global, machine, or harness");
    }
    return normalized;
  }

  function buildRememberBody(input: {
    summary: string;
    project: string;
    domain?: string;
    scope: string;
    memoryKind: string;
    context: string;
    applicability: string;
    nonApplicability: string;
    evidenceRefs: string[];
  }): string {
    const lines = [
      "## Context",
      "",
      input.context,
      "",
      "## Lesson",
      "",
      input.summary.trim(),
      "",
      "## Applicability",
      "",
      input.applicability,
      "",
      "## Non-applicability",
      "",
      input.nonApplicability,
      "",
      "## Envelope",
      "",
      `- Project: \`${input.project}\``
    ];
    if (input.domain) {
      lines.push(`- Domain: \`${input.domain}\``);
    }
    lines.push(
      `- Scope: \`${input.scope}\``,
      `- Memory kind: \`${input.memoryKind}\``,
      "",
      "## Evidence",
      "",
      ...(input.evidenceRefs.length ? input.evidenceRefs.map((ref) => `- \`${ref}\``) : ["- No explicit evidence reference was available."])
    );
    return lines.join("\n");
  }

  function stringFromRecord(record: Record<string, unknown>, key: string): string | undefined {
    const value = record[key];
    return typeof value === "string" && value.trim() ? value.trim() : undefined;
  }

  function stringArrayFromRecord(record: Record<string, unknown>, key: string): string[] {
    const value = record[key];
    return Array.isArray(value) ? value.map((item) => (typeof item === "string" ? item.trim() : "")).filter(Boolean) : [];
  }

  function uniqueNonEmptyStrings(values: Array<string | undefined | null>): string[] {
    return Array.from(new Set(values.map((value) => value?.trim() || "").filter(Boolean)));
  }

  function uniqueRecordStrings(records: Array<Record<string, unknown>>, key: string): string[] {
    return uniqueNonEmptyStrings(records.map((record) => stringFromRecord(record, key)));
  }

  function collapseWhitespace(value: string): string {
    return value.replace(/\s+/g, " ").trim();
  }

  function stripMarkdownHeading(value: string): string {
    return value.replace(/^#+\s+/, "").trim();
  }

  function firstUsefulProposalLine(body: string): string {
    for (const line of body.split(/\r?\n/)) {
      const trimmed = stripMarkdownHeading(line.trim());
      if (!trimmed || trimmed.startsWith("---") || trimmed.startsWith("- Source ") || trimmed.startsWith("- Target ")) {
        continue;
      }
      if (trimmed.length >= 20) {
        return trimmed;
      }
    }
    return "";
  }

  function proposalSummary(proposal: Record<string, unknown>, body: string, explicit?: string): string {
    if (explicit?.trim()) {
      return collapseWhitespace(explicit).slice(0, 1_000);
    }
    const title = stringFromRecord(proposal, "title") || "";
    const titleSummary = collapseWhitespace(title.replace(/^remember(?:\s*\([^)]+\))?:/i, ""));
    if (titleSummary.length >= 20) {
      return titleSummary.slice(0, 1_000);
    }
    const line = firstUsefulProposalLine(body);
    if (line) {
      return collapseWhitespace(line).slice(0, 1_000);
    }
    return `Enriched memory candidate from proposal ${stringFromRecord(proposal, "id") || "unknown"}`;
  }

  function clusterSubject(proposal: Record<string, unknown>): string | undefined {
    const cluster = stringFromRecord(proposal, "cluster_label");
    if (!cluster) {
      return undefined;
    }
    const subject = cluster.split("/")[0]?.trim();
    return subject || undefined;
  }

  function inferProjectForProposal(proposal: Record<string, unknown>): string {
    return (
      stringFromRecord(proposal, "project") ||
      stringFromRecord(proposal, "domain") ||
      (stringFromRecord(proposal, "related_repo") ? deriveProjectLabel(stringFromRecord(proposal, "related_repo")!) : undefined) ||
      clusterSubject(proposal) ||
      "shared-brain"
    );
  }

  function inferScopeForProposal(proposal: Record<string, unknown>): string {
    const explicit = stringFromRecord(proposal, "scope");
    if (explicit && explicit !== "needs-context") {
      return validateMemoryScope(explicit);
    }
    return stringFromRecord(proposal, "related_repo") ? "repo" : "project";
  }

  function proposalEnrichmentPayload(
    cfg: BrainstackConfig,
    args: ParsedArgs,
    proposal: Record<string, unknown>,
    body: string
  ): Record<string, unknown> {
    const id = stringFromRecord(proposal, "id") || args.positional[1] || "unknown";
    const project = boundedCliString(requireFlagValue(args, "project"), 120) || inferProjectForProposal(proposal);
    const domain = boundedCliString(requireFlagValue(args, "domain"), 120) || stringFromRecord(proposal, "domain") || clusterSubject(proposal) || project;
    const scope = validateMemoryScope(requireFlagValue(args, "scope") || inferScopeForProposal(proposal));
    const memoryKind = boundedCliString(requireFlagValue(args, "memory-kind") || requireFlagValue(args, "kind"), 80) || stringFromRecord(proposal, "memory_kind") || "project_lesson";
    const summary = proposalSummary(proposal, body, requireFlagValue(args, "summary"));
    const relatedRepo = boundedCliString(requireFlagValue(args, "related-repo"), 500) || stringFromRecord(proposal, "related_repo");
    const context =
      boundedCliString(requireFlagValue(args, "context"), 1_000) ||
      stringFromRecord(proposal, "context") ||
      `Enriched from legacy or context-poor Brainstack proposal ${id}. Review the original proposal before applying this memory globally.`;
    const applicability =
      boundedCliString(requireFlagValue(args, "applicability"), 1_000) ||
      stringFromRecord(proposal, "applicability") ||
      (scope === "repo" && relatedRepo
        ? `Use when working in ${relatedRepo} or directly related ${domain} work.`
        : `Use for ${project}/${domain} work after verifying the same project and runtime context.`);
    const nonApplicability =
      boundedCliString(requireFlagValue(args, "non-applicability") || requireFlagValue(args, "non_applicability"), 1_000) ||
      stringFromRecord(proposal, "non_applicability") ||
      `Do not apply outside ${project}/${domain} without checking the original evidence and current system behavior.`;
    const evidenceRefs = uniqueNonEmptyStrings([
      `proposal:${id}`,
      ...stringArrayFromRecord(proposal, "source_ids").map((sourceId) => `source:${sourceId}`),
      ...stringArrayFromRecord(proposal, "evidence_refs"),
      ...(relatedRepo ? [`repo:${relatedRepo}`] : []),
      ...flagValues(args, "evidence")
    ]).slice(0, 20);
    const confidenceRaw = requireFlagValue(args, "confidence");
    const confidence = confidenceRaw === undefined ? 0.55 : Number(confidenceRaw);
    if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) {
      throw new Error("--confidence must be a number between 0 and 1");
    }
    const title = requireFlagValue(args, "title") || `Enriched memory (${project}): ${summary.slice(0, 120)}`;
    const proposalBody = buildRememberBody({ summary, project, domain, scope, memoryKind, context, applicability, nonApplicability, evidenceRefs });
    return {
      title,
      body: proposalBody,
      source_harness: requireFlagValue(args, "source-harness") || cfg.harness.name,
      source_machine: requireFlagValue(args, "source-machine") || cfg.machine.name,
      source_type: "memory",
      related_repo: relatedRepo,
      project,
      domain,
      scope,
      memory_kind: memoryKind,
      context,
      applicability,
      non_applicability: nonApplicability,
      evidence_refs: evidenceRefs,
      source_ids: stringArrayFromRecord(proposal, "source_ids"),
      tags: uniqueNonEmptyStrings(["remember", "enriched-memory", project, domain, memoryKind, scope]),
      confidence,
      review_after: boundedCliString(requireFlagValue(args, "review-after"), 80),
      expires_at: boundedCliString(requireFlagValue(args, "expires-at"), 80)
    };
  }

  function slugPart(value: string): string {
    return value
      .toLowerCase()
      .replace(/'/g, "")
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 80);
  }

  function firstUniqueOrFallback(values: string[], fallback: string): string {
    return values.length === 1 ? values[0]! : fallback;
  }

  function proposalReviewGroupsFromResult(result: Record<string, unknown>): Array<Record<string, unknown>> {
    return Array.isArray(result.review_groups)
      ? (result.review_groups as Array<Record<string, unknown>>)
      : Array.isArray(result.clusters)
        ? (result.clusters as Array<Record<string, unknown>>)
        : [];
  }

  function stripRememberPrefix(value: string): string {
    return collapseWhitespace(value.replace(/^remember(?:\s*\([^)]+\))?:/i, ""));
  }

  function mergedIntoReason(record: Record<string, unknown>): string | null {
    const reason = stringFromRecord(record, "reason") || "";
    const match = reason.match(/^(?:merged|absorbed) into\s+(.+)$/i);
    return match?.[1]?.trim() || null;
  }

  function proposalIsOpen(record: Record<string, unknown>): boolean {
    return ["pending", "approved", "needs-human"].includes(stringFromRecord(record, "status") || "");
  }

  function proposalIdFromRef(ref: string): string | null {
    const trimmed = ref.trim();
    if (!trimmed) return null;
    return trimmed.startsWith("proposal:") ? trimmed.slice("proposal:".length).trim() || null : null;
  }

  function proposalIdsFromRefs(refs: string[]): string[] {
    return uniqueNonEmptyStrings(refs.map((ref) => proposalIdFromRef(ref)).filter(Boolean) as string[]);
  }

  function proposalIsMemoryMerge(record: Record<string, unknown>): boolean {
    return stringFromRecord(record, "source_type") === "memory-merge";
  }

  function proposalLessonLine(detail: { id: string; proposal: Record<string, unknown>; body: string }): string {
    const title = stripRememberPrefix(stringFromRecord(detail.proposal, "title") || detail.id);
    if (title.length >= 12) {
      return title;
    }
    const bodyLine = firstUsefulProposalLine(detail.body);
    return bodyLine || `Review proposal ${detail.id}`;
  }

  function proposalMatchesReviewGroup(proposal: Record<string, unknown>, groupKey: string): boolean {
    return stringFromRecord(proposal, "cluster_key") === groupKey || stringFromRecord(proposal, "cluster_label") === groupKey;
  }

  function selectedProposalIdSet(args: ParsedArgs): Set<string> {
    return new Set(flagValues(args, "id").map((value) => value.trim()).filter(Boolean));
  }

  function firstUsefulProposalText(detail: { id: string; proposal: Record<string, unknown>; body: string }): string {
    return [
      stripRememberPrefix(stringFromRecord(detail.proposal, "title") || ""),
      stringFromRecord(detail.proposal, "context") || "",
      stringFromRecord(detail.proposal, "applicability") || "",
      firstUsefulProposalLine(detail.body)
    ]
      .filter(Boolean)
      .join(" ");
  }

  function proposalCreatedDay(record: Record<string, unknown>): string {
    const created = stringFromRecord(record, "created_at") || stringFromRecord(record, "createdAt") || "";
    const match = created.match(/^(\d{4}-\d{2}-\d{2})/);
    return match?.[1] || "unknown-date";
  }

  const topicBuckets: Array<{ key: string; label: string; patterns: RegExp[] }> = [
    { key: "frontend-ui", label: "front-end/UI", patterns: [/\bfront[- ]?end\b/i, /\bui\b/i, /\bux\b/i, /\bview\b/i, /\bswiftui\b/i, /\bmenu(?:bar)?\b/i, /\boperator\b/i, /\bdashboard\b/i, /\blanding\b/i, /\bcss\b/i, /\blayout\b/i, /\bvisual\b/i] },
    { key: "docs-content", label: "docs/content", patterns: [/\bdocs?\b/i, /\breadme\b/i, /\bcopy\b/i, /\bcontent\b/i, /\bwebsite\b/i, /\bseo\b/i, /\baso\b/i, /\bmarketing\b/i, /\bpage\b/i] },
    { key: "curation-proposals", label: "curation/proposals", patterns: [/\bcurat(?:e|ion|or)\b/i, /\bproposal[- ]review\b/i, /\bproposal[- ]queue\b/i, /\bmerge\b/i, /\bsupersed(?:e|ed)\b/i] },
    { key: "install-lifecycle", label: "install/lifecycle", patterns: [/\binstall(?:er|ation)?\b/i, /\benroll(?:ment)?\b/i, /\binvite\b/i, /\blifecycle\b/i, /\bupgrade\b/i, /\buninstall\b/i, /\brepair\b/i] },
    { key: "daemon-fleet", label: "daemon/fleet", patterns: [/\bdaemon\b/i, /\bbrainstackd\b/i, /\bfleet\b/i, /\bworker\b/i, /\bmachine\b/i, /\btailscale\b/i, /\bssh\b/i] },
    { key: "telemux-telegram", label: "Telegram/telemux", patterns: [/\btelegram\b/i, /\btg\b/i, /\btelemux\b/i, /\bvoice\b/i, /\btranscri(?:be|ption)\b/i, /\bwhisper\b/i] },
    { key: "outbox-sync", label: "outbox/sync", patterns: [/\boutbox\b/i, /\bflush\b/i, /\bretry\b/i, /\bdiscard\b/i, /\bsync\b/i] },
    { key: "tests-ci", label: "tests/CI", patterns: [/\btest(?:s|ing)?\b/i, /\bci\b/i, /\bworkflow\b/i, /\bgithub\b/i, /\brelease\b/i, /\bbuild\b/i] },
    { key: "security-safety", label: "security/safety", patterns: [/\bsecurity\b/i, /\bsafety\b/i, /\btoken\b/i, /\bsecret\b/i, /\bauth\b/i, /\bpermission\b/i] },
    { key: "performance-latency", label: "performance/latency", patterns: [/\bperformance\b/i, /\blatency\b/i, /\btimeout\b/i, /\bhung?\b/i, /\bslow\b/i] }
  ];

  const relationStopwords = new Set([
    "about",
    "added",
    "after",
    "again",
    "against",
    "also",
    "auto",
    "brainstack",
    "candidate",
    "candidates",
    "change",
    "changes",
    "code",
    "consolidate",
    "context",
    "default",
    "during",
    "fix",
    "fixed",
    "from",
    "group",
    "lesson",
    "lessons",
    "local",
    "machine",
    "memory",
    "merge",
    "merged",
    "must",
    "needs",
    "proposal",
    "proposals",
    "repo",
    "review",
    "scope",
    "shared",
    "should",
    "source",
    "status",
    "summary",
    "support",
    "system",
    "that",
    "this",
    "when",
    "with",
    "work",
    "working"
  ]);

  function topicBucketForDetail(detail: { id: string; proposal: Record<string, unknown>; body: string }): { key: string; label: string } {
    const text = firstUsefulProposalText(detail);
    for (const bucket of topicBuckets) {
      if (bucket.patterns.some((pattern) => pattern.test(text))) {
        return { key: bucket.key, label: bucket.label };
      }
    }
    const projectWords = uniqueNonEmptyStrings([
      stringFromRecord(detail.proposal, "project"),
      stringFromRecord(detail.proposal, "domain"),
      stringFromRecord(detail.proposal, "memory_kind"),
      stringFromRecord(detail.proposal, "scope")
    ].filter(Boolean) as string[])
      .flatMap((value) => value.toLowerCase().split(/[^a-z0-9]+/))
      .filter(Boolean);
    const localStopwords = new Set([...relationStopwords, ...projectWords]);
    const tokens = uniqueNonEmptyStrings(
      text
        .toLowerCase()
        .split(/[^a-z0-9]+/)
        .filter((token) => token.length >= 4 && !localStopwords.has(token) && !/^\d+$/.test(token))
    ).slice(0, 3);
    const key = tokens.length ? tokens.join("-") : "misc";
    return { key, label: tokens.length ? tokens.join(" ") : "misc" };
  }

  function targetPageForRelationBatch(groupLabel: string, groupKey: string, topicKey: string, day: string): string {
    const labelSubject = groupLabel.split("/")[0]?.trim() || groupKey;
    const parts = [slugPart(labelSubject || groupKey) || "review-group", slugPart(topicKey) || "topic", day === "unknown-date" ? "" : day.replaceAll("-", "")].filter(Boolean);
    return `wiki/Syntheses/${parts.join("-")}-lessons.md`;
  }

  function titleForRelationBatch(groupLabel: string, topicLabel: string, day: string): string {
    const suffix = [topicLabel, day === "unknown-date" ? null : day].filter(Boolean).join(", ");
    return suffix ? `Consolidate: ${groupLabel} (${suffix})` : `Consolidate: ${groupLabel}`;
  }

  function buildReviewGroupContent(input: {
    title: string;
    groupKey: string;
    groupLabel: string;
    project: string;
    domain: string;
    scope: string;
    memoryKind: string;
    details: Array<{ id: string; proposal: Record<string, unknown>; body: string }>;
    conflicts: string[];
  }): string {
    const seen = new Set<string>();
    const lessonLines = input.details
      .map((detail) => ({ id: detail.id, text: proposalLessonLine(detail) }))
      .filter((item) => {
        const normalized = slugPart(item.text);
        if (seen.has(normalized)) return false;
        seen.add(normalized);
        return true;
      });
    const sourceIds = uniqueNonEmptyStrings(input.details.flatMap((detail) => stringArrayFromRecord(detail.proposal, "source_ids")));
    const evidenceRefs = uniqueNonEmptyStrings([
      ...input.details.map((detail) => `proposal:${detail.id}`),
      ...input.details.flatMap((detail) => stringArrayFromRecord(detail.proposal, "evidence_refs"))
    ]);
    return [
      `# ${input.title}`,
      "",
      `Consolidates ${input.details.length} related Brainstack proposal candidates from review group \`${input.groupKey}\` (${input.groupLabel}).`,
      "",
      "## Lessons",
      "",
      ...lessonLines.map((item) => `- ${item.text} (source: \`proposal:${item.id}\`)`),
      "",
      "## Applicability",
      "",
      `Use for ${input.project}/${input.domain} work when the scope is \`${input.scope}\` and the lesson kind is \`${input.memoryKind}\`.`,
      "",
      "## Non-applicability",
      "",
      `Do not apply outside ${input.project}/${input.domain} without checking the source proposals and current system behavior.`,
      "",
      "## Review Notes",
      "",
      ...(input.conflicts.length ? input.conflicts.map((conflict) => `- Needs human review: ${conflict}`) : ["- Deterministic consolidation only; no embedding or cosine-similarity merge was used."]),
      "",
      "## Evidence",
      "",
      ...evidenceRefs.map((ref) => `- \`${ref}\``),
      ...(sourceIds.length ? ["", "## Source Artifacts", "", ...sourceIds.map((id) => `- \`${id}\``)] : [])
    ].join("\n");
  }

  async function createReviewGroupMergeProposal(
    cfg: BrainstackConfig,
    args: ParsedArgs,
    groupKey: string
  ): Promise<{ payload: Record<string, unknown>; selected: number; conflicts: string[]; dryRun: boolean; write?: BrainWriteOutcome; closed?: string[] }> {
    const closeSources = hasFlag(args, "close-sources");
    // Include previously superseded sources so an interrupted close operation can
    // resume against the same idempotently-created merge proposal.
    const status = requireFlagValue(args, "status") || (closeSources ? "open,superseded" : "open");
    const result = await brainApiRequest(cfg, "GET", `/api/proposals?status=${encodeURIComponent(status)}`);
    const proposals = Array.isArray(result.proposals) ? (result.proposals as Array<Record<string, unknown>>) : [];
    const requestedIds = selectedProposalIdSet(args);
    const matching = proposals
      .filter((proposal) => proposalMatchesReviewGroup(proposal, groupKey))
      .filter((proposal) => !requestedIds.size || requestedIds.has(stringFromRecord(proposal, "id") || ""));
    if (matching.length < 2) {
      throw new Error(`review group ${groupKey} has ${matching.length} matching proposal(s); merge-group requires at least 2`);
    }
    const limit = parsePositiveIntegerFlag(args, "limit", 20);
    if (!hasFlag(args, "all") && matching.length > limit) {
      throw new Error(`review group ${groupKey} has ${matching.length} proposal(s); merge-group defaults to ${limit}. Rerun with --limit N or --all.`);
    }
    const details: Array<{ id: string; proposal: Record<string, unknown>; body: string }> = [];
    for (const item of hasFlag(args, "all") ? matching : matching.slice(0, limit)) {
      const id = stringFromRecord(item, "id");
      if (!id) continue;
      const detail = await fetchProposalDetail(cfg, id);
      details.push({ id, proposal: { ...item, ...detail.proposal }, body: detail.body });
    }
    if (details.length < 2) {
      throw new Error(`review group ${groupKey} has ${details.length} readable proposal(s); merge-group requires at least 2`);
    }
    const records = details.map((detail) => detail.proposal);
    const groupLabel = stringFromRecord(records[0] || {}, "cluster_label") || groupKey;
    const labelSubject = groupLabel.split("/")[0]?.trim() || groupKey;
    const projectValues = uniqueRecordStrings(records, "project");
    const domainValues = uniqueRecordStrings(records, "domain");
    const scopeValues = uniqueRecordStrings(records, "scope").filter((value) => value !== "needs-context");
    const kindValues = uniqueRecordStrings(records, "memory_kind");
    const relatedRepoValues = uniqueRecordStrings(records, "related_repo");
    const conflicts = [
      projectValues.length > 1 ? `multiple projects: ${projectValues.join(", ")}` : null,
      domainValues.length > 1 ? `multiple domains: ${domainValues.join(", ")}` : null,
      scopeValues.length > 1 ? `multiple scopes: ${scopeValues.join(", ")}` : null,
      kindValues.length > 1 ? `multiple memory kinds: ${kindValues.join(", ")}` : null,
      relatedRepoValues.length > 1 ? `multiple related repos: ${relatedRepoValues.join(", ")}` : null,
      records.some(proposalNeedsContext) ? "one or more source proposals need context" : null,
      records.some((proposal) => !stringFromRecord(proposal, "target_page") && !stringFromRecord(proposal, "memory_kind")) ? "one or more source proposals are context-only candidates" : null
    ].filter(Boolean) as string[];
    const project = boundedCliString(requireFlagValue(args, "project"), 120) || firstUniqueOrFallback(projectValues, labelSubject || "shared-brain");
    const domain = boundedCliString(requireFlagValue(args, "domain"), 120) || firstUniqueOrFallback(domainValues, project);
    const scope = validateMemoryScope(requireFlagValue(args, "scope") || firstUniqueOrFallback(scopeValues, "project"));
    const memoryKind = boundedCliString(requireFlagValue(args, "memory-kind") || requireFlagValue(args, "kind"), 80) || firstUniqueOrFallback(kindValues, "project_lesson");
    const title = requireFlagValue(args, "title") || `Consolidate: ${groupLabel}`;
    const targetPage = requireFlagValue(args, "target-page") || `wiki/Syntheses/${slugPart(labelSubject || groupKey) || "review-group"}-lessons.md`;
    const confidenceRaw = requireFlagValue(args, "confidence") || (conflicts.length ? "0.55" : "0.75");
    const confidence = Number(confidenceRaw);
    if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) {
      throw new Error("--confidence must be a number between 0 and 1");
    }
    const content = buildReviewGroupContent({ title, groupKey, groupLabel, project, domain, scope, memoryKind, details, conflicts });
    const sourceIds = uniqueNonEmptyStrings([
      ...details.map((detail) => `proposal:${detail.id}`),
      ...details.flatMap((detail) => stringArrayFromRecord(detail.proposal, "source_ids"))
    ]).slice(0, 100);
    const evidenceRefs = uniqueNonEmptyStrings([
      ...details.map((detail) => `proposal:${detail.id}`),
      ...details.flatMap((detail) => stringArrayFromRecord(detail.proposal, "evidence_refs"))
    ]).slice(0, 100);
    const payload: Record<string, unknown> = {
      title,
      body: `Consolidates ${details.length} related proposal candidates from review group ${groupKey}.`,
      source_harness: requireFlagValue(args, "source-harness") || cfg.harness.name,
      source_machine: requireFlagValue(args, "source-machine") || cfg.machine.name,
      source_type: "memory",
      target_page: targetPage,
      proposed_content: content,
      base_sha256: requireFlagValue(args, "base-sha256") || "absent",
      risk: requireFlagValue(args, "risk") || "low",
      confidence,
      source_ids: sourceIds,
      project,
      domain,
      scope,
      memory_kind: memoryKind,
      context: `Consolidated from Brainstack review group ${groupKey} (${groupLabel}).`,
      applicability: `Use for ${project}/${domain} work when the source proposals and scope match.`,
      non_applicability: `Do not apply outside ${project}/${domain} without checking the source proposals.`,
      evidence_refs: evidenceRefs
    };
    if (conflicts.length || hasFlag(args, "needs-human")) {
      payload.status = "needs-human";
      payload.reason = conflicts.length ? `Review group merge needs human review: ${conflicts.join("; ")}` : "Review group merge was explicitly marked needs-human.";
    }
    const dryRun = !hasFlag(args, "submit") && !hasFlag(args, "apply") && !hasFlag(args, "yes");
    if (dryRun) {
      return { payload, selected: details.length, conflicts, dryRun };
    }
    const existingMergeIds = uniqueNonEmptyStrings(details.map((detail) => mergedIntoReason(detail.proposal)));
    if (closeSources && existingMergeIds.length > 1) {
      throw new Error(`review group ${groupKey} has sources already merged into different proposals: ${existingMergeIds.join(", ")}`);
    }
    if (closeSources) {
      const openProposalStatuses = new Set(["pending", "approved", "needs-human"]);
      const unclosable = details
        .filter((detail) => {
          const proposalStatus = stringFromRecord(detail.proposal, "status") || "";
          return !openProposalStatuses.has(proposalStatus) && !mergedIntoReason(detail.proposal);
        })
        .map((detail) => `${detail.id}:${stringFromRecord(detail.proposal, "status") || "unknown"}`);
      if (unclosable.length) {
        throw new Error(`review group ${groupKey} has source proposals that cannot be closed: ${unclosable.join(", ")}`);
      }
    }
    const write: BrainWriteOutcome =
      closeSources && existingMergeIds[0]
        ? { status: "accepted", response: { proposal_id: existingMergeIds[0], status: "already-created" } }
        : closeSources
          ? { status: "accepted", response: await brainApiRequest(cfg, "POST", "/api/propose", { admin: true, body: payload }) }
          : await postBrainWriteOrQueue(cfg, "propose", payload, {}, { quiet: hasFlag(args, "json") });
    const closed: string[] = [];
    if (closeSources) {
      const mergedId = stringFromRecord(write.response || {}, "proposal_id") || stringFromRecord(write.response || {}, "id") || "merged review-group proposal";
      for (const detail of details) {
        if (mergedIntoReason(detail.proposal)) {
          closed.push(detail.id);
          continue;
        }
        await brainApiRequest(cfg, "POST", `/api/proposals/${encodeURIComponent(detail.id)}/supersede`, {
          admin: true,
          body: {
            decided_by: `${process.env.USER || "operator"}@${cfg.machine.name}`,
            reason: `absorbed into ${mergedId}`
          }
        });
        closed.push(detail.id);
      }
    }
    return { payload, selected: details.length, conflicts, dryRun, write, closed };
  }

  function numericRecordValue(record: Record<string, unknown>, key: string): number {
    const value = record[key];
    return typeof value === "number" && Number.isFinite(value) ? value : 0;
  }

  async function createAutomaticReviewGroupMerges(
    cfg: BrainstackConfig,
    args: ParsedArgs
  ): Promise<{
    dryRun: boolean;
    considered: number;
    selected: number;
    merged: Array<{ groupKey: string; relationKey: string; selected: number; conflicts: string[]; targetPage: string; closed: string[]; writeStatus: string | null }>;
    skipped: Array<{ groupKey: string; reason: string }>;
  }> {
    const status = requireFlagValue(args, "status") || "open";
    const minSize = parsePositiveIntegerFlag(args, "min-size", 2);
    const maxGroupSize = parsePositiveIntegerFlag(args, "max-group-size", 6);
    const limitGroups = hasFlag(args, "all-groups") ? Number.POSITIVE_INFINITY : parsePositiveIntegerFlag(args, "limit-groups", 5);
    const sourceGroupLimit = hasFlag(args, "all-source-groups") ? Number.POSITIVE_INFINITY : parsePositiveIntegerFlag(args, "max-source-group-size", 50);
    const allowLargeGroups = hasFlag(args, "all") || hasFlag(args, "allow-large-groups");
    const relationWindow = (requireFlagValue(args, "relation-window") || "day").toLowerCase();
    if (relationWindow !== "day" && relationWindow !== "all") {
      throw new Error("--relation-window must be day or all");
    }
    const dryRun = !hasFlag(args, "submit") && !hasFlag(args, "apply") && !hasFlag(args, "yes");
    const groupsResult = await brainApiRequest(
      cfg,
      "GET",
      `/api/proposals/groups?status=${encodeURIComponent(status)}&min_size=${encodeURIComponent(String(minSize))}`
    );
    const groups = proposalReviewGroupsFromResult(groupsResult);
    const proposalsResult = await brainApiRequest(cfg, "GET", `/api/proposals?status=${encodeURIComponent(status)}`);
    const proposals = Array.isArray(proposalsResult.proposals) ? (proposalsResult.proposals as Array<Record<string, unknown>>) : [];
    const skipped: Array<{ groupKey: string; reason: string }> = [];
    const selectedBatches: Array<{
      groupKey: string;
      groupLabel: string;
      relationKey: string;
      topicKey: string;
      topicLabel: string;
      day: string;
      ids: string[];
    }> = [];
    for (const group of groups) {
      const groupKey = stringFromRecord(group, "id") || stringFromRecord(group, "label") || "";
      const groupLabel = stringFromRecord(group, "label") || groupKey;
      const count = numericRecordValue(group, "count");
      const legacyCount = numericRecordValue(group, "legacyCount");
      const needsContextCount = numericRecordValue(group, "needsContextCount");
      if (!groupKey) {
        skipped.push({ groupKey: "(unknown)", reason: "missing review group id" });
        continue;
      }
      if (count < minSize) {
        skipped.push({ groupKey, reason: `below min size ${minSize}` });
        continue;
      }
      if (count > sourceGroupLimit && !allowLargeGroups) {
        skipped.push({ groupKey, reason: `review group has ${count} proposals; max automatic source group size is ${sourceGroupLimit}` });
        continue;
      }
      if (legacyCount > 0 && !hasFlag(args, "include-legacy")) {
        skipped.push({ groupKey, reason: "contains legacy proposals" });
        continue;
      }
      if (needsContextCount > 0 && !hasFlag(args, "include-needs-context")) {
        skipped.push({ groupKey, reason: "contains proposals that need context" });
        continue;
      }
      const matching = proposals.filter((proposal) => proposalMatchesReviewGroup(proposal, groupKey));
      if (matching.length < minSize) {
        skipped.push({ groupKey, reason: `only ${matching.length} readable proposals matched this group` });
        continue;
      }
      const details: Array<{ id: string; proposal: Record<string, unknown>; body: string }> = [];
      for (const item of matching.slice(0, Math.max(count, matching.length))) {
        const id = stringFromRecord(item, "id");
        if (!id) continue;
        const detail = await fetchProposalDetail(cfg, id);
        details.push({ id, proposal: { ...item, ...detail.proposal }, body: detail.body });
      }
      const batches = new Map<string, { topicKey: string; topicLabel: string; day: string; ids: string[] }>();
      for (const detail of details) {
        const topic = topicBucketForDetail(detail);
        const day = relationWindow === "day" ? proposalCreatedDay(detail.proposal) : "all-dates";
        const relationKey = `${groupKey}:${day}:${topic.key}`;
        const existing = batches.get(relationKey) || { topicKey: topic.key, topicLabel: topic.label, day, ids: [] };
        existing.ids.push(detail.id);
        batches.set(relationKey, existing);
      }
      let selectedForGroup = 0;
      for (const [relationKey, batch] of [...batches.entries()].sort((a, b) => b[1].ids.length - a[1].ids.length || a[0].localeCompare(b[0]))) {
        if (batch.ids.length < minSize) {
          continue;
        }
        if (batch.ids.length > maxGroupSize && !allowLargeGroups) {
          skipped.push({ groupKey, reason: `related batch ${relationKey} has ${batch.ids.length} proposals; max automatic batch size is ${maxGroupSize}` });
          continue;
        }
        selectedBatches.push({ groupKey, groupLabel, relationKey, topicKey: batch.topicKey, topicLabel: batch.topicLabel, day: batch.day, ids: batch.ids });
        selectedForGroup += 1;
        if (selectedBatches.length >= limitGroups) {
          break;
        }
      }
      if (!selectedForGroup) {
        skipped.push({ groupKey, reason: "no related date/topic batch met the minimum size" });
      }
      if (selectedBatches.length >= limitGroups) {
        break;
      }
    }
    if (Number.isFinite(limitGroups) && selectedBatches.length >= limitGroups) {
      skipped.push({ groupKey: "(remaining)", reason: `limit-groups ${limitGroups} reached` });
    }
    const merged: Array<{ groupKey: string; relationKey: string; selected: number; conflicts: string[]; targetPage: string; closed: string[]; writeStatus: string | null }> = [];
    for (const batch of selectedBatches) {
      const mergeArgs: ParsedArgs = {
        ...args,
        positional: ["merge-group", batch.groupKey],
        flags: {
          ...args.flags,
          status,
          id: batch.ids,
          title: requireFlagValue(args, "title") || titleForRelationBatch(batch.groupLabel, batch.topicLabel, batch.day),
          "target-page": requireFlagValue(args, "target-page") || targetPageForRelationBatch(batch.groupLabel, batch.groupKey, batch.topicKey, batch.day),
          limit: String(Math.max(maxGroupSize, batch.ids.length)),
          ...(allowLargeGroups ? { all: true } : {}),
          ...(dryRun ? {} : { submit: true }),
          ...(dryRun || hasFlag(args, "keep-sources") ? {} : { "close-sources": true })
        }
      };
      const result = await createReviewGroupMergeProposal(cfg, mergeArgs, batch.groupKey);
      merged.push({
        groupKey: batch.groupKey,
        relationKey: batch.relationKey,
        selected: result.selected,
        conflicts: result.conflicts,
        targetPage: String(result.payload.target_page || ""),
        closed: result.closed || [],
        writeStatus: result.write?.status || null
      });
    }
    return { dryRun, considered: groups.length, selected: selectedBatches.length, merged, skipped };
  }

  function parseUnitNumberFlag(args: ParsedArgs, key: string, fallback: number): number {
    const raw = requireFlagValue(args, key);
    if (raw === undefined) return fallback;
    const parsed = Number(raw);
    if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) {
      throw new Error(`--${key} must be a number between 0 and 1`);
    }
    return parsed;
  }

  function truncateForPrompt(value: string, maxChars: number): string {
    const normalized = value.replace(/\r\n/g, "\n").trim();
    if (normalized.length <= maxChars) return normalized;
    return `${normalized.slice(0, maxChars)}\n[truncated ${normalized.length - maxChars} chars]`;
  }

  function proposalPromptRecord(detail: { id: string; proposal: Record<string, unknown>; body: string }): Record<string, unknown> {
    const proposal = detail.proposal;
    return {
      id: detail.id,
      title: stringFromRecord(proposal, "title"),
      status: stringFromRecord(proposal, "status"),
      created_at: stringFromRecord(proposal, "created_at"),
      target_page: stringFromRecord(proposal, "target_page"),
      project: stringFromRecord(proposal, "project"),
      domain: stringFromRecord(proposal, "domain"),
      scope: stringFromRecord(proposal, "scope"),
      memory_kind: stringFromRecord(proposal, "memory_kind"),
      review_group: stringFromRecord(proposal, "cluster_label") || stringFromRecord(proposal, "cluster_key"),
      source_type: stringFromRecord(proposal, "source_type"),
      quality_decision: stringFromRecord(proposal, "quality_decision"),
      confidence: proposal.confidence,
      evidence_refs: stringArrayFromRecord(proposal, "evidence_refs").slice(0, 8),
      source_ids: stringArrayFromRecord(proposal, "source_ids").slice(0, 8),
      body: truncateForPrompt(detail.body, 1_400)
    };
  }

  function extractJsonObject(text: string): Record<string, unknown> {
    const trimmed = text.trim();
    const fenced = trimmed.match(/```(?:json)?\s*([\s\S]*?)\s*```/i)?.[1]?.trim();
    const candidates = [trimmed, fenced].filter(Boolean) as string[];
    for (const candidate of candidates) {
      try {
        const parsed = JSON.parse(candidate);
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          return parsed as Record<string, unknown>;
        }
      } catch {
        // Try the next shape.
      }
    }
    const first = trimmed.indexOf("{");
    const last = trimmed.lastIndexOf("}");
    if (first >= 0 && last > first) {
      try {
        const parsed = JSON.parse(trimmed.slice(first, last + 1));
        if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
          return parsed as Record<string, unknown>;
        }
      } catch {
        // Fall through to the uniform error below.
      }
    }
    throw new Error("proposal merge harness did not return a JSON object");
  }

  function buildHarnessMergePrompt(input: {
    totalOpen: number;
    limit: number;
    overflow: boolean;
    autoThreshold: number;
    proposals: Array<{ id: string; proposal: Record<string, unknown>; body: string }>;
  }): string {
    return [
      "You are Brainstack's proposal merge planner.",
      "",
      "Task: review the supplied open proposal candidates and find related subsets that should be consolidated into fewer durable memory/wiki proposals.",
      "",
      "Rules:",
      "- Return JSON only.",
      "- Do not invent proposal ids; every source_ids entry must come from the supplied proposal ids.",
      "- Each merge candidate must contain 2 to 12 source ids.",
      "- Prefer specific related work, such as the same product area, repo, day, UI surface, incident, or docs pass.",
      "- Do not merge broad unrelated project activity just because it happened in the same repository.",
      "- Do not merge source-page curator proposals with memory lessons unless they clearly describe the same final lesson.",
      "- Confidence >= 0.80 means the candidate is safe for automatic consolidation; below that means human review.",
      "- If there are no good merges, return an empty merge_candidates array.",
      "",
      "Return shape:",
      JSON.stringify({
        merge_candidates: [
          {
            title: "short consolidated proposal title",
            source_ids: ["proposal-id-1", "proposal-id-2"],
            confidence: 0.87,
            topic: "front-end UI polish",
            project: "project or null",
            domain: "domain or null",
            scope: "repo|project|global|machine|harness",
            memory_kind: "project_lesson",
            summary: "the consolidated durable lesson",
            applicability: "where this applies",
            non_applicability: "where this should not be applied",
            reason: "why these proposals belong together"
          }
        ],
        warnings: ["optional warning"]
      }, null, 2),
      "",
      `Open proposals available: ${input.totalOpen}. Supplied in this prompt: ${input.proposals.length}.`,
      input.overflow ? `Only the top ${input.limit} proposals are supplied. The operator must rerun after this batch to cover the rest.` : "All open proposals are supplied.",
      `Automatic consolidation threshold: ${input.autoThreshold}.`,
      "",
      "Proposals:",
      JSON.stringify(input.proposals.map(proposalPromptRecord), null, 2)
    ].join("\n");
  }

  function proposalMergeHarnessEnv(sourceEnv: NodeJS.ProcessEnv = process.env): Record<string, string> {
    const allowedExact = new Set([
      "HOME",
      "PATH",
      "SHELL",
      "TERM",
      "TMPDIR",
      "USER",
      "LOGNAME",
      "LANG",
      "LC_ALL",
      "CODEX_HOME",
      "CODEX_CLI_PATH",
      "CLAUDE_CONFIG_DIR",
      "ANTHROPIC_BASE_URL",
      "OPENAI_BASE_URL",
      "OPENAI_ORG_ID",
      "OPENAI_PROJECT"
    ]);
    const allowedSensitiveExceptions = new Set(["SSH_AUTH_SOCK"]);
    const allowedPrefixes = ["XDG_"];
    const denyPattern = /(?:TOKEN|SECRET|PASSWORD|PASS|KEY|COOKIE|AUTH|CREDENTIAL|WEBHOOK)/i;
    const deniedPrefixes = ["BRAIN_", "BRAINSTACK_", "FACTORY_", "TELEGRAM_", "TG_"];
    const env: Record<string, string> = {};
    for (const [key, value] of Object.entries(sourceEnv)) {
      if (typeof value !== "string") {
        continue;
      }
      if (allowedSensitiveExceptions.has(key) || allowedExact.has(key) || allowedPrefixes.some((prefix) => key.startsWith(prefix))) {
        if (deniedPrefixes.some((prefix) => key.startsWith(prefix))) {
          continue;
        }
        env[key] = value;
        continue;
      }
      if (deniedPrefixes.some((prefix) => key.startsWith(prefix)) || denyPattern.test(key)) {
        continue;
      }
    }
    return env;
  }

  function harnessMergeIdempotencyKey(targetPage: string, sourceIds: string[]): string {
    const normalizedSources = uniqueNonEmptyStrings(sourceIds).sort();
    return `proposal-merge:${sha256Hex(JSON.stringify({ targetPage, sourceIds: normalizedSources })).slice(0, 48)}`;
  }

  async function runProposalMergeHarness(
    cfg: BrainstackConfig,
    args: ParsedArgs,
    prompt: string
  ): Promise<{ output: Record<string, unknown>; harness: string; stderr: string }> {
    const harness = (requireFlagValue(args, "harness") || cfg.harness.name || "codex").toLowerCase();
    const bin = resolveHarnessExecutable(
      requireFlagValue(args, "harness-bin") || process.env.BRAINSTACK_PROPOSAL_MERGE_HARNESS_BIN || cfg.harness.bin || harness
    );
    const timeoutMs = parsePositiveIntegerFlag(args, "harness-timeout-ms", 300_000);
    const tmpRoot = join(cfg.paths.stateRoot, "tmp");
    await ensureDir(tmpRoot, 0o700);
    const nonce = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const outputPath = join(tmpRoot, `proposal-merge-${nonce}.txt`);
    const argv =
      harness === "codex"
        ? [
            bin,
            "exec",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "-c",
            "model_reasoning_effort=\"medium\"",
            "--output-last-message",
            outputPath,
            "-"
          ]
        : harness === "claude"
          ? [bin, "-p", "--output-format", "text"]
          : (() => {
              throw new Error(`proposal batch merge supports codex or claude harnesses, not ${harness}`);
            })();
    const proc = Bun.spawn(argv, {
      cwd: proposalMergeHarnessCwd(cfg),
      env: proposalMergeHarnessEnv(),
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe"
    });
    const stdoutPromise = new Response(proc.stdout).text();
    const stderrPromise = new Response(proc.stderr).text();
    proc.stdin.write(prompt);
    await proc.stdin.flush();
    proc.stdin.end();
    const timeout = setTimeout(() => {
      try {
        proc.kill();
      } catch {
        // Process may already have exited.
      }
    }, timeoutMs);
    const [code, stdout, stderr] = await Promise.all([proc.exited, stdoutPromise, stderrPromise]).finally(() => clearTimeout(timeout));
    const finalMessage = existsSync(outputPath) ? await readFile(outputPath, "utf8").catch(() => "") : "";
    await rm(outputPath, { force: true }).catch(() => undefined);
    if (code !== 0) {
      throw new Error(formatProposalMergeHarnessFailure(code, bin, stderr, stdout));
    }
    const text = finalMessage.trim() || stdout.trim();
    if (!text) {
      throw new Error("proposal merge harness returned no output");
    }
    return { output: extractJsonObject(text), harness, stderr };
  }

  function resolveHarnessExecutable(rawBin: string): string {
    const expanded = rawBin.startsWith("~/") && process.env.HOME ? join(process.env.HOME, rawBin.slice(2)) : rawBin;
    if (expanded.includes("/") || existsSync(expanded)) {
      return expanded;
    }
    for (const candidate of preferredHarnessCandidates(expanded)) {
      if (existsSync(candidate)) {
        return candidate;
      }
    }
    const found = commandPath(expanded);
    if (found) {
      return found;
    }
    const candidates = [
      process.env.HOME ? join(process.env.HOME, ".local", "bin", expanded) : null,
      "/opt/homebrew/bin",
      "/usr/local/bin",
      process.env.HOME ? join(process.env.HOME, ".bun", "bin", expanded) : null,
      expanded === "codex" ? "/Applications/Codex.app/Contents/Resources/codex" : null,
      expanded === "claude" ? "/Applications/Claude.app/Contents/MacOS/Claude" : null
    ]
      .filter((candidate): candidate is string => Boolean(candidate))
      .map((candidate) => (candidate.endsWith(expanded) ? candidate : join(candidate, expanded)));
    for (const candidate of candidates) {
      if (existsSync(candidate)) {
        return candidate;
      }
    }
    return expanded;
  }

  function preferredHarnessCandidates(name: string): string[] {
    if (name === "codex") {
      return [
        process.env.CODEX_CLI_PATH || null,
        "/Applications/Codex.app/Contents/Resources/codex"
      ].filter((candidate): candidate is string => Boolean(candidate));
    }
    return [];
  }

  function proposalMergeHarnessCwd(cfg: BrainstackConfig): string {
    const candidates = [
      cfg.paths.productRepo,
      cfg.paths.sharedBrainRoot,
      cfg.paths.clientLocalPath,
      process.cwd(),
      process.env.HOME || ""
    ];
    for (const candidate of candidates) {
      if (!candidate || candidate === "/") continue;
      try {
        if (existsSync(candidate) && statSync(candidate).isDirectory()) {
          return candidate;
        }
      } catch {
        // Try the next stable directory.
      }
    }
    return process.cwd();
  }

  function formatProposalMergeHarnessFailure(code: number, bin: string, stderr: string, stdout: string): string {
    const combined = [stderr, stdout].filter(Boolean).join("\n").trim();
    const detail = combined.match(/"detail"\s*:\s*"([^"]+)"/)?.[1] || combined.match(/ERROR:\s*(.+)$/m)?.[1] || "";
    const model = combined.match(/^model:\s*(.+)$/m)?.[1]?.trim();
    const version = combined.match(/^(?:OpenAI Codex|codex-cli)\s+(.+)$/m)?.[1]?.trim();
    const actionable = detail.includes("requires a newer version of Codex")
      ? [
          `Codex rejected configured model${model ? ` ${model}` : ""}: ${detail}`,
          `Brainstack used ${bin}${version ? ` (${version})` : ""}. Update that Codex CLI or configure harness.bin/CODEX_CLI_PATH to a newer Codex binary.`
        ].join(" ")
      : detail || combined.split(/\r?\n/).find((line) => /error|failed|unsupported/i.test(line)) || combined.slice(0, 500);
    const diagnostic = combined
      .split(/\r?\n/)
      .filter((line) => /^(OpenAI Codex|codex-cli|workdir:|model:|provider:|ERROR:|stream error:)/.test(line))
      .slice(0, 12)
      .join("\n");
    return [
      `proposal merge harness failed (exit ${code}).`,
      actionable,
      diagnostic ? `Diagnostics:\n${diagnostic}` : ""
    ]
      .filter(Boolean)
      .join("\n");
  }

  type HarnessMergeCandidate = {
    title: string;
    sourceIds: string[];
    confidence: number;
    topic: string;
    project: string;
    domain: string;
    scope: string;
    memoryKind: string;
    summary: string;
    applicability: string;
    nonApplicability: string;
    reason: string;
  };

  function normalizeHarnessMergeCandidate(
    raw: Record<string, unknown>,
    knownIds: Set<string>
  ): { candidate?: HarnessMergeCandidate; error?: string } {
    const sourceIds = uniqueNonEmptyStrings([
      ...stringArrayFromRecord(raw, "source_ids"),
      ...stringArrayFromRecord(raw, "sourceIds"),
      ...stringArrayFromRecord(raw, "ids")
    ]).filter((id) => knownIds.has(id));
    if (sourceIds.length < 2) {
      return { error: "candidate has fewer than two known source ids" };
    }
    if (sourceIds.length > 12) {
      return { error: `candidate has ${sourceIds.length} source ids; max is 12` };
    }
    const confidence = typeof raw.confidence === "number" ? raw.confidence : Number(stringFromRecord(raw, "confidence") || "NaN");
    if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) {
      return { error: "candidate confidence is missing or invalid" };
    }
    const title = boundedCliString(stringFromRecord(raw, "title"), 140) || "Consolidated Brainstack proposal lesson";
    const topic = boundedCliString(stringFromRecord(raw, "topic"), 80) || "related proposals";
    const summary = boundedCliString(stringFromRecord(raw, "summary"), 1_500) || title;
    const project = boundedCliString(stringFromRecord(raw, "project"), 120) || "shared-brain";
    const domain = boundedCliString(stringFromRecord(raw, "domain"), 120) || project;
    const scope = validateMemoryScope(stringFromRecord(raw, "scope") || "project");
    const memoryKind = boundedCliString(stringFromRecord(raw, "memory_kind") || stringFromRecord(raw, "memoryKind"), 80) || "project_lesson";
    return {
      candidate: {
        title,
        sourceIds,
        confidence,
        topic,
        project,
        domain,
        scope,
        memoryKind,
        summary,
        applicability:
          boundedCliString(stringFromRecord(raw, "applicability"), 1_000) ||
          `Use when the supplied source proposals match ${project}/${domain} ${topic} work.`,
        nonApplicability:
          boundedCliString(stringFromRecord(raw, "non_applicability") || stringFromRecord(raw, "nonApplicability"), 1_000) ||
          `Do not apply outside ${project}/${domain} without checking the source proposals.`,
        reason: boundedCliString(stringFromRecord(raw, "reason"), 1_000) || "Harness judged these proposals related enough to consolidate."
      }
    };
  }

  function targetPageForHarnessMerge(candidate: HarnessMergeCandidate, details: Array<{ id: string; proposal: Record<string, unknown>; body: string }>): string {
    const date = proposalCreatedDay(details[0]?.proposal || {});
    const parts = [slugPart(candidate.project) || "shared-brain", slugPart(candidate.topic) || "proposal-merge", date === "unknown-date" ? "" : date.replaceAll("-", "")].filter(Boolean);
    return `wiki/Syntheses/${parts.join("-")}-lessons.md`;
  }

  function harnessMergeSupersedeIdempotencyKey(mergeId: string, sourceId: string): string {
    return `proposal-merge-close:${sha256Hex(JSON.stringify({ mergeId, sourceId })).slice(0, 48)}`;
  }

  async function closeHarnessMergeSources(
    cfg: BrainstackConfig,
    mergeId: string,
    sourceIds: string[]
  ): Promise<string[]> {
    const closed: string[] = [];
    for (const sourceId of sourceIds) {
      await brainApiRequest(cfg, "POST", `/api/proposals/${encodeURIComponent(sourceId)}/supersede`, {
        admin: true,
        idempotencyKey: harnessMergeSupersedeIdempotencyKey(mergeId, sourceId),
        body: {
          decided_by: `${process.env.USER || "operator"}@${cfg.machine.name}`,
          reason: `absorbed into ${mergeId}`
        }
      });
      closed.push(sourceId);
    }
    return closed;
  }

  async function completeOutstandingHarnessMergeClosures(
    cfg: BrainstackConfig,
    proposals: Array<Record<string, unknown>>,
    autoThreshold: number
  ): Promise<Array<{ title: string; confidence: number; sourceIds: string[]; targetPage: string; autoMerged: boolean; writeStatus: string | null; closed: string[] }>> {
    const byId = new Map<string, Record<string, unknown>>();
    for (const proposal of proposals) {
      const id = stringFromRecord(proposal, "id");
      if (id) byId.set(id, proposal);
    }
    const recovered: Array<{ title: string; confidence: number; sourceIds: string[]; targetPage: string; autoMerged: boolean; writeStatus: string | null; closed: string[] }> = [];
    for (const merge of proposals) {
      if (!proposalIsMemoryMerge(merge) || !proposalIsOpen(merge)) {
        continue;
      }
      const mergeId = stringFromRecord(merge, "id");
      if (!mergeId) {
        continue;
      }
      const confidence = numericRecordValue(merge, "confidence");
      if (confidence < autoThreshold) {
        continue;
      }
      const sourceIds = proposalIdsFromRefs(stringArrayFromRecord(merge, "source_ids"));
      if (sourceIds.length < 2) {
        continue;
      }
      const sourceRecords = sourceIds.map((id) => byId.get(id)).filter(Boolean) as Array<Record<string, unknown>>;
      if (sourceRecords.length < 2 || !sourceRecords.some((record) => mergedIntoReason(record) === mergeId)) {
        continue;
      }
      const outstanding = sourceRecords
        .filter((record) => proposalIsOpen(record) && !proposalIsMemoryMerge(record))
        .map((record) => stringFromRecord(record, "id"))
        .filter(Boolean) as string[];
      if (!outstanding.length) {
        continue;
      }
      const closed = await closeHarnessMergeSources(cfg, mergeId, outstanding);
      recovered.push({
        title: stringFromRecord(merge, "title") || `Consolidate: ${mergeId}`,
        confidence,
        sourceIds,
        targetPage: stringFromRecord(merge, "target_page") || "",
        autoMerged: true,
        writeStatus: "already-created",
        closed
      });
    }
    return recovered;
  }

  function buildHarnessMergeContent(
    candidate: HarnessMergeCandidate,
    details: Array<{ id: string; proposal: Record<string, unknown>; body: string }>,
    autoMerged: boolean
  ): string {
    const evidenceRefs = uniqueNonEmptyStrings([
      ...details.map((detail) => `proposal:${detail.id}`),
      ...details.flatMap((detail) => stringArrayFromRecord(detail.proposal, "evidence_refs"))
    ]).slice(0, 100);
    const lessonLines = details.map((detail) => `- ${proposalLessonLine(detail)} (source: \`proposal:${detail.id}\`)`);
    return [
      `# ${candidate.title}`,
      "",
      candidate.summary,
      "",
      "## Lessons",
      "",
      ...lessonLines,
      "",
      "## Applicability",
      "",
      candidate.applicability,
      "",
      "## Non-applicability",
      "",
      candidate.nonApplicability,
      "",
      "## Merge Review",
      "",
      `- Harness confidence: ${Math.round(candidate.confidence * 100)}%`,
      `- Topic: ${candidate.topic}`,
      `- Decision path: ${autoMerged ? "auto-merged source proposals because confidence met threshold" : "needs-human review because confidence was below threshold"}`,
      `- Reason: ${candidate.reason}`,
      "",
      "## Evidence",
      "",
      ...evidenceRefs.map((ref) => `- \`${ref}\``)
    ].join("\n");
  }

  async function createHarnessProposalMerges(
    cfg: BrainstackConfig,
    args: ParsedArgs
  ): Promise<{
    dryRun: boolean;
    harness: string;
    totalOpen: number;
    inspected: number;
    overflow: boolean;
    autoThreshold: number;
    candidates: number;
    merged: Array<{ title: string; confidence: number; sourceIds: string[]; targetPage: string; autoMerged: boolean; writeStatus: string | null; closed: string[] }>;
    skipped: Array<{ ids?: string[]; reason: string }>;
    warnings: string[];
  }> {
    const requestedStatus = requireFlagValue(args, "status");
    const status = requestedStatus || "open";
    const statusQuery = !requestedStatus || requestedStatus === "open" ? "pending,approved,needs-human,superseded" : status;
    const requestedLimit = parsePositiveIntegerFlag(args, "limit", 100);
    const limit = hasFlag(args, "allow-large") ? requestedLimit : Math.min(requestedLimit, 100);
    const autoThreshold = parseUnitNumberFlag(args, "auto-threshold", 0.8);
    const dryRun = !hasFlag(args, "submit") && !hasFlag(args, "apply") && !hasFlag(args, "yes");
    const result = await brainApiRequest(cfg, "GET", `/api/proposals?status=${encodeURIComponent(statusQuery)}`);
    const proposals = Array.isArray(result.proposals) ? (result.proposals as Array<Record<string, unknown>>) : [];
    const recovered =
      dryRun || hasFlag(args, "keep-sources")
        ? []
        : await completeOutstandingHarnessMergeClosures(cfg, proposals, autoThreshold);
    const recoveredIds = new Set(recovered.flatMap((item) => item.closed));
    const sourceProposals = proposals
      .filter((proposal) => (status === "open" ? proposalIsOpen(proposal) : true))
      .filter((proposal) => !proposalIsMemoryMerge(proposal))
      .filter((proposal) => !recoveredIds.has(stringFromRecord(proposal, "id") || ""));
    const selected = sourceProposals.slice(0, limit);
    const details: Array<{ id: string; proposal: Record<string, unknown>; body: string }> = [];
    for (const item of selected) {
      const id = stringFromRecord(item, "id");
      if (!id) continue;
      const detail = await fetchProposalDetail(cfg, id);
      details.push({ id, proposal: { ...item, ...detail.proposal }, body: detail.body });
    }
    const warnings = sourceProposals.length > limit ? [`Only inspected the top ${limit} of ${sourceProposals.length} open source proposals. Rerun after this batch to cover the rest.`] : [];
    if (details.length < 2) {
      return { dryRun, harness: cfg.harness.name, totalOpen: sourceProposals.length, inspected: details.length, overflow: sourceProposals.length > limit, autoThreshold, candidates: recovered.length, merged: recovered, skipped: [{ reason: "fewer than two readable proposals" }], warnings };
    }
    const harnessResult = await runProposalMergeHarness(
      cfg,
      args,
      buildHarnessMergePrompt({ totalOpen: sourceProposals.length, limit, overflow: sourceProposals.length > limit, autoThreshold, proposals: details })
    );
    const rawCandidates = Array.isArray(harnessResult.output.merge_candidates)
      ? (harnessResult.output.merge_candidates as Array<Record<string, unknown>>)
      : Array.isArray(harnessResult.output.candidates)
        ? (harnessResult.output.candidates as Array<Record<string, unknown>>)
        : [];
    const harnessWarnings = stringArrayFromRecord(harnessResult.output, "warnings");
    warnings.push(...harnessWarnings);
    const knownIds = new Set(details.map((detail) => detail.id));
    const detailById = new Map(details.map((detail) => [detail.id, detail]));
    const usedIds = new Set<string>();
    const skipped: Array<{ ids?: string[]; reason: string }> = [];
    const merged: Array<{ title: string; confidence: number; sourceIds: string[]; targetPage: string; autoMerged: boolean; writeStatus: string | null; closed: string[] }> = [...recovered];
    for (const raw of rawCandidates) {
      const normalized = normalizeHarnessMergeCandidate(raw, knownIds);
      if (!normalized.candidate) {
        skipped.push({ reason: normalized.error || "invalid candidate" });
        continue;
      }
      const candidate = normalized.candidate;
      const duplicateIds = candidate.sourceIds.filter((id) => usedIds.has(id));
      if (duplicateIds.length) {
        skipped.push({ ids: candidate.sourceIds, reason: `source ids already used by a higher-ranked candidate: ${duplicateIds.join(", ")}` });
        continue;
      }
      const candidateDetails = candidate.sourceIds.map((id) => detailById.get(id)).filter(Boolean) as Array<{ id: string; proposal: Record<string, unknown>; body: string }>;
      const autoMerged = candidate.confidence >= autoThreshold;
      const targetPage = targetPageForHarnessMerge(candidate, candidateDetails);
      const content = buildHarnessMergeContent(candidate, candidateDetails, autoMerged);
      const sourceRefs = uniqueNonEmptyStrings([
        ...candidate.sourceIds.map((id) => `proposal:${id}`),
        ...candidateDetails.flatMap((detail) => stringArrayFromRecord(detail.proposal, "source_ids"))
      ]).slice(0, 100);
      const payload: Record<string, unknown> = {
        title: `Consolidate: ${candidate.title}`,
        body: `Harness proposed consolidating ${candidate.sourceIds.length} related proposals at ${Math.round(candidate.confidence * 100)}% confidence.`,
        source_harness: requireFlagValue(args, "source-harness") || cfg.harness.name,
        source_machine: requireFlagValue(args, "source-machine") || cfg.machine.name,
        source_type: "memory-merge",
        target_page: targetPage,
        proposed_content: content,
        base_sha256: "absent",
        risk: autoMerged ? "low" : "medium",
        confidence: candidate.confidence,
        source_ids: sourceRefs,
        project: candidate.project,
        domain: candidate.domain,
        scope: candidate.scope,
        memory_kind: candidate.memoryKind,
        context: `Harness-scanned top ${details.length} open Brainstack proposals and proposed this ${candidate.topic} consolidation.`,
        applicability: candidate.applicability,
        non_applicability: candidate.nonApplicability,
        evidence_refs: candidate.sourceIds.map((id) => `proposal:${id}`)
      };
      if (!autoMerged) {
        payload.status = "needs-human";
        payload.reason = `Merge candidate confidence ${Math.round(candidate.confidence * 100)}% is below automatic threshold ${Math.round(autoThreshold * 100)}%. ${candidate.reason}`;
      }
      if (dryRun) {
        merged.push({ title: String(payload.title), confidence: candidate.confidence, sourceIds: candidate.sourceIds, targetPage, autoMerged, writeStatus: null, closed: [] });
        candidate.sourceIds.forEach((id) => usedIds.add(id));
        continue;
      }
      const mergeIdempotencyKey = harnessMergeIdempotencyKey(targetPage, candidate.sourceIds);
      const response = await brainApiRequest(cfg, "POST", "/api/propose", { admin: true, body: payload, idempotencyKey: mergeIdempotencyKey });
      const mergedId = stringFromRecord(response, "proposal_id") || stringFromRecord(response, "id") || String(payload.title);
      const closed: string[] = [];
      if (autoMerged && !hasFlag(args, "keep-sources")) {
        closed.push(...(await closeHarnessMergeSources(cfg, mergedId, candidate.sourceIds)));
      }
      merged.push({ title: String(payload.title), confidence: candidate.confidence, sourceIds: candidate.sourceIds, targetPage, autoMerged, writeStatus: stringFromRecord(response, "status") || "submitted", closed });
      candidate.sourceIds.forEach((id) => usedIds.add(id));
    }
    return { dryRun, harness: harnessResult.harness, totalOpen: sourceProposals.length, inspected: details.length, overflow: sourceProposals.length > limit, autoThreshold, candidates: rawCandidates.length + recovered.length, merged, skipped, warnings };
  }

  function crossBrainAllowKey(sourceId: string, targetId: string): string {
    return `cross:${sourceId}->${targetId}`;
  }

  async function promptForCrossBrainRemember(sourceId: string, targetId: string): Promise<"once" | "always" | "cancel" | null> {
    const input = (await promptLine(
      [
        `Recent Brainstack search/context used ${sourceId}, and remember target is ${targetId}.`,
        "Save across this brain boundary? [y]es once / [a]lways for this repo / [n]o: "
      ].join("\n")
    )).trim().toLowerCase();
    if (input === "y" || input === "yes" || input === "once") return "once";
    if (input === "a" || input === "always") return "always";
    if (input === "n" || input === "no" || input === "cancel") return "cancel";
    return null;
  }

  function crossBrainPolicyRule(
    rules: Record<string, string>,
    source: { id: string; classification: ProjectBrain["classification"] },
    target: ProjectBrain
  ): string {
    const candidates = crossBrainPolicyCandidates(source, target);
    for (const key of candidates) {
      const value = rules[key];
      if (typeof value === "string" && value.trim()) {
        return value.trim().toLowerCase();
      }
    }
    if (
      (source.classification === "personal" && target.classification === "work") ||
      (source.classification === "work" && target.classification === "personal")
    ) {
      return "ask";
    }
    return "allow";
  }

  function sessionSourcesFromBrains(brains: ProjectBrain[], syncStatus: Record<string, string> = {}): Array<Record<string, string>> {
    return brains.map((brain) => ({
      id: brain.id,
      label: brain.label,
      classification: brain.classification,
      sync_status: syncStatus[brain.id] || "not-synced"
    }));
  }

  function sessionSourcesFromSearchResults(
    results: Array<{ brain: string; label: string; classification: string }>,
    syncStatus: Record<string, string>
  ): Array<Record<string, string>> {
    return [
      ...new Map(
        results.map((item) => [
          item.brain,
          {
            id: item.brain,
            label: item.label,
            classification: item.classification,
            sync_status: syncStatus[item.brain] || "not-synced"
          }
        ])
      ).values()
    ];
  }

  function readProjectSession(resolved: ResolvedProjectContext): Record<string, unknown> {
    if (!existsSync(resolved.sessionPath)) {
      return {};
    }
    try {
      return JSON.parse(readFileSync(resolved.sessionPath, "utf8")) as Record<string, unknown>;
    } catch {
      return {};
    }
  }

  async function writeProjectSession(resolved: ResolvedProjectContext, data: Record<string, unknown>): Promise<void> {
    await ensureDir(dirname(resolved.sessionPath));
    await writeText(resolved.sessionPath, `${JSON.stringify({ ...data, repo: resolved.repo, updated_at: new Date().toISOString() }, null, 2)}\n`, 0o600);
  }

  async function updateProjectSession(resolved: ResolvedProjectContext, data: Record<string, unknown>): Promise<void> {
    await writeProjectSession(resolved, { ...readProjectSession(resolved), ...data });
  }

  async function commandSearch(args: ParsedArgs): Promise<void> {
    const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
    const query = (flag(args, "query") || args.positional.join(" ")).trim();
    if (!query) {
      throw new Error("search requires a query");
    }
    const resolved = await resolveProjectContext(cfg, flag(args, "repo"));
    const pendingTrustBrains = resolved.allowedBrains.filter((brain) => !brain.connectionTrusted);
    const searchableBrains = resolved.allowedBrains.filter((brain) => brain.connectionTrusted);
    if (hasFlag(args, "wait-fresh")) {
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
        const pending = searchableBrains.filter((brain) => existsSync(join(brain.localPath, "derived", "search-reindex-needed.json")));
        if (!pending.length) {
          break;
        }
        await Bun.sleep(500);
      }
    }
    const syncStatus: Record<string, string> = {};
    if (args.flags["no-sync"] === undefined) {
      for (const brain of searchableBrains) {
        syncStatus[brain.id] = await maybeSyncBrainClone(brain);
      }
    }
    const results = searchableBrains.flatMap((brain) => searchLocalBrain(brain, query));
    const recentSources = sessionSourcesFromSearchResults(results, syncStatus);
    await updateProjectSession(resolved, { recent_search: query, recent_sources: recentSources });
    for (const brain of pendingTrustBrains) {
      console.warn(`WARN [${brain.id}] skipped pending-trust brain; ${projectBrainTrustInstruction(cfg, brain)}.`);
    }
    for (const brain of searchableBrains) {
      if (existsSync(join(brain.localPath, "derived", "search-reindex-needed.json"))) {
        console.warn(`WARN [${brain.id}] search index refresh pending; local results may be stale.`);
      }
    }
    if (hasFlag(args, "json")) {
      console.log(JSON.stringify({ query, sync_status: syncStatus, skipped_pending_trust: pendingTrustBrains.map((brain) => brain.id), results }, null, 2));
      await consumeOnceAllowRules(cfg, resolved.repo, searchableBrains.map((brain) => brain.id));
      return;
    }
    console.log(results.length ? results.map((item) => `[${item.brain} / ${item.path}:${item.line}] ${item.text}`).join("\n") : "(no local matches)");
    await consumeOnceAllowRules(cfg, resolved.repo, searchableBrains.map((brain) => brain.id));
  }

  async function commandRemember(args: ParsedArgs): Promise<void> {
    const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
    const summary = flag(args, "summary") || flag(args, "text") || args.positional.join(" ");
    if (!summary) {
      throw new Error("remember requires --summary TEXT");
    }
    const resolved = await resolveProjectContext(cfg, flag(args, "repo"));
    const targetId = flag(args, "target") || resolved.writeDefault;
    const target = resolved.allowedBrains.find((brain) => brain.id === targetId);
    if (!target) {
      throw new Error(`target brain ${targetId} is not allowed for ${resolved.repo}. Run brainctl context --repo ${shellSingleQuote(resolved.repo)} for the explicit allow command.`);
    }
    if (target.write === "false") {
      throw new Error(`target brain ${targetId} is read-only in project context`);
    }
    if (!target.connectionTrusted) {
      throw new Error(
        `target brain ${targetId} uses untrusted repo-local connection fields (${untrustedBrainConnectionFields(target)}); ${projectBrainTrustInstruction(cfg, target)}.`
      );
    }
    const session = readProjectSession(resolved);
    const recentSearchSources = Array.isArray(session.recent_sources) ? session.recent_sources : [];
    const recentContextSources = Array.isArray(session.recent_context_sources) ? session.recent_context_sources : [];
    const recentSources = [
      ...new Map(
        [...recentSearchSources, ...recentContextSources].map((source) => [sourceRecordId(source), source])
      ).values()
    ].filter((source) => sourceRecordId(source));
    const crossBrainSources = recentSources
      .map((source) => ({
        id: sourceRecordId(source),
        classification: sourceRecordClassification(source)
      }))
      .filter((source) => source.id && source.id !== target.id);
    const neverSource = crossBrainSources.find((source) => crossBrainPolicyRule(resolved.crossBrainWrites, source, target) === "never");
    if (neverSource) {
      throw new Error(`Refusing cross-brain remember from recent ${neverSource.id} sources into ${target.id}; policy is never.`);
    }
    const askSource = crossBrainSources.find((source) => crossBrainPolicyRule(resolved.crossBrainWrites, source, target) === "ask");
    if (askSource && !hasFlag(args, "confirm-cross-brain")) {
      const repoRules = readAllowRules(cfg)[resolved.repo] || {};
      const storedDecision = repoRules[crossBrainAllowKey(askSource.id, target.id)]?.decision;
      if (storedDecision === "deny") {
        throw new Error(`Refusing cross-brain remember from recent ${askSource.id} sources into ${target.id}; local decision is deny.`);
      }
      if (storedDecision !== "always") {
        if (process.stdin.isTTY && !process.env.CI) {
          const decision = await promptForCrossBrainRemember(askSource.id, target.id);
          if (decision === "always") {
            await saveAllowRule(cfg, resolved.repo, crossBrainAllowKey(askSource.id, target.id), "always", []);
          } else if (decision !== "once") {
            throw new Error(`Refusing cross-brain remember from recent ${askSource.id} sources into ${target.id}; not approved.`);
          }
        } else {
          throw new Error(`Refusing cross-brain remember from recent ${askSource.id} sources into ${target.id} without --confirm-cross-brain; policy=ask. Rerun with --confirm-cross-brain only after verifying the source labels are safe to write into ${target.id}.`);
        }
      }
    }
    if (target.write !== "false" && (!target.baseUrl || !target.importTokenEnv)) {
      throw new Error(`target brain ${targetId} is writable but missing explicit baseUrl/importTokenEnv`);
    }
    const project = boundedCliString(flag(args, "project"), 120) || deriveProjectLabel(resolved.repo);
    const domain = boundedCliString(flag(args, "domain"), 120) || project;
    const scope = validateMemoryScope(flag(args, "scope") || "repo");
    const memoryKind = boundedCliString(flag(args, "memory-kind") || flag(args, "kind"), 80) || "project_lesson";
    const context = boundedCliString(flag(args, "context"), 1_000) || `Captured while working in repo ${resolved.repo}.`;
    const applicability =
      boundedCliString(flag(args, "applicability"), 1_000) ||
      (scope === "global"
        ? "Use only when the lesson is clearly independent of any project, harness, machine, or product domain."
        : `Use when working on ${project} or directly related ${scope}-scoped work.`);
    const nonApplicability =
      boundedCliString(flag(args, "non-applicability") || flag(args, "non_applicability"), 1_000) ||
      "Do not apply globally without checking the captured project and evidence context.";
    const evidenceRefs = Array.from(
      new Set(
        [
          `repo:${resolved.repo}`,
          ...recentSources.map(sourceRecordEvidenceRef),
          ...flagValues(args, "evidence")
        ]
          .map((ref) => ref.trim())
          .filter(Boolean)
      )
    ).slice(0, 20);
    const titlePrefix = project ? `Remember (${project})` : "Remember";
    const proposalBody = buildRememberBody({ summary, project, domain, scope, memoryKind, context, applicability, nonApplicability, evidenceRefs });
    await postBrainWriteOrQueue(cfg, target.write === "true" ? "import" : "propose", {
      title: `${titlePrefix}: ${summary.slice(0, 120)}`,
      text: proposalBody,
      body: proposalBody,
      source_harness: cfg.harness.name,
      source_machine: cfg.machine.name,
      source_type: "remember",
      related_repo: resolved.repo,
      project,
      domain,
      scope,
      memory_kind: memoryKind,
      context,
      applicability,
      non_applicability: nonApplicability,
      evidence_refs: evidenceRefs,
      recent_sources: recentSources,
      recent_context_sources: recentContextSources,
      tags: ["remember", target.id, project, memoryKind, scope],
      review_after: boundedCliString(flag(args, "review-after"), 80),
      expires_at: boundedCliString(flag(args, "expires-at"), 80)
    }, {
      baseUrl: target.baseUrl,
      importTokenEnv: target.importTokenEnv,
      targetBrainId: target.id
    });
    await consumeOnceAllowRules(cfg, resolved.repo, [target.id]);
  }

  async function commandAllow(args: ParsedArgs): Promise<void> {
    const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
    const sub = args.positional[0] || "";
    if (sub !== "repo") {
      throw new Error("allow supports only: repo");
    }
    const repo = await resolveRepoPath(flag(args, "repo"));
    const brain = flag(args, "brain");
    if (!brain) {
      throw new Error("allow repo requires --brain BRAIN_ID");
    }
    const resolved = await resolveProjectContext(cfg, repo);
    const configuredBrain = resolved.brains.find((item) => item.id === brain);
    if (!configuredBrain) {
      throw new Error(`unknown brain ${brain} for repo ${repo}`);
    }
    const requestedSections = flag(args, "sections") ? flag(args, "sections")!.split(",").map((item) => item.trim()).filter(Boolean) : [];
    if (requestedSections.length) {
      const knownSections = new Set(configuredBrain.sections.length ? configuredBrain.sections : ["wiki", "raw", "proposals"]);
      const unknownSections = requestedSections.filter((section) => !knownSections.has(section));
      if (unknownSections.length) {
        throw new Error(`unknown section(s) for brain ${brain}: ${unknownSections.join(", ")}. Known sections: ${[...knownSections].join(", ")}`);
      }
    }
    const decisionFlags = ["always", "once", "deny"].filter((key) => hasFlag(args, key));
    if (decisionFlags.length !== 1) {
      throw new Error("allow repo requires exactly one of --always, --once, or --deny");
    }
    const decision = hasFlag(args, "deny") ? "deny" : hasFlag(args, "once") ? "once" : "always";
    await saveAllowRule(cfg, repo, brain, decision, requestedSections);
    console.log(`allow_rule=${decision} repo=${repo} brain=${brain}`);
  }


  function brainWriteTimeoutMs(): number {
    const value = Number(process.env.BRAINSTACK_BRAIN_WRITE_TIMEOUT_MS || "15000");
    return Number.isFinite(value) && value > 0 ? value : 15000;
  }

  async function brainWriteSmokeCheck(cfg: BrainstackConfig): Promise<DoctorCheck> {
    const { baseUrl, token: writeToken } = brainWriteConfig(cfg);
    if (!baseUrl || !writeToken) {
      return check("FAIL", "client", "brain-write-smoke", "BRAIN_BASE_URL or BRAIN_IMPORT_TOKEN is missing", "Set client write env before running --write-smoke.");
    }

    try {
      const response = await fetch(new URL("/api/import", baseUrl).toString(), {
        method: "POST",
        headers: {
          Authorization: `Bearer ${writeToken}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          title: `Brainstack doctor write smoke ${new Date().toISOString()}`,
          text: "Brainstack doctor write smoke. This is a small import/propose path verification artifact.",
          source_harness: cfg.harness.name,
          source_machine: cfg.machine.name,
          source_type: "doctor-smoke",
          tags: ["brainstack", "doctor-smoke"]
        }),
        signal: AbortSignal.timeout(brainWriteTimeoutMs())
      });
      if (!response.ok) {
        return check("FAIL", "client", "brain-write-smoke", `HTTP ${response.status}`);
      }
      const body = (await response.json()) as Record<string, unknown>;
      return check("PASS", "client", "brain-write-smoke", `import accepted artifact_id=${String(body.artifact_id || "unknown")}`);
    } catch (error) {
      return check("FAIL", "client", "brain-write-smoke", error instanceof Error ? error.message : String(error));
    }
  }

  type OutboxItem = SharedOutboxItem;

  function outboxItemKey(
    endpoint: "import" | "propose",
    payload: Record<string, unknown>,
    destination: { brain_id?: string | null; url?: string | null; import_token_env?: string | null } = {}
  ): string {
    return sharedOutboxItemKey(endpoint, payload, {
      brain_id: destination.brain_id || null,
      url: destination.url || null
    });
  }

  async function findMatchingOutboxItem(root: string, endpoint: "import" | "propose", idempotencyKey: string): Promise<{ path: string; item: OutboxItem } | null> {
    const scan = await scanOutbox(root);
    if (scan.corrupt.length) {
      throw new Error(`outbox namespace contains ${scan.corrupt.length} corrupt/unsafe item(s); inspect with brainctl outbox list and run purge-corrupt before queueing new writes`);
    }
    for (const entry of scan.items) {
      if (entry.item.endpoint !== endpoint) {
        continue;
      }
      if (entry.idempotencyKey === idempotencyKey || entry.item.idempotency_key === idempotencyKey) {
        return { path: entry.path, item: entry.item };
      }
    }
    return null;
  }

  async function queueOutboxItem(cfg: BrainstackConfig, item: Omit<OutboxItem, "id" | "created_at" | "retry_count" | "idempotency_key" | "last_error">, error: string): Promise<string> {
    const payload = item.payload ? item.payload : decodeOutboxPayload(item as OutboxItem);
    // Use the shared destination shape so direct sends, queued entries, and flush
    // replays all present the same idempotency key to the brain.
    const idempotencyKey = outboxItemKey(item.endpoint, payload, outboxDestinationForKey(item));
    const id = `${item.endpoint}-${idempotencyKey.slice(0, 32)}`;
    const root = outboxRootForDestination(cfg, { brainId: item.brain_id, baseUrl: item.url });
    await ensurePrivateOutboxDir(root);
    const stablePath = join(root, `${id}.json`);
    const matched = (await findMatchingOutboxItem(root, item.endpoint, idempotencyKey)) || null;
    const path = matched?.path || stablePath;
    const existing = matched?.item || null;
    const queuedItem = buildOutboxItem(
      {
        ...item,
        payload,
        idempotency_key: idempotencyKey
      },
      error,
      existing
    );
    await assertOutboxCapacity(root, existing ? path : null, outboxItemFileBytes(queuedItem));
    await writeOutboxItem(path, queuedItem);
    const storage = queuedItem.payload_storage;
    const limits = outboxLimitsFromEnv();
    if (storage && storage.uncompressed_bytes >= limits.softWarnBytes) {
      const compressedNote = storage.encoding === "json-gzip-base64" ? `, compressed to ${storage.stored_bytes} bytes` : "";
      console.warn(`WARN queued large outbox item: ${storage.uncompressed_bytes} bytes${compressedNote}. No content was truncated.`);
    }
    return path;
  }

  function brainWriteIdempotencyKey(
    endpoint: "import" | "propose",
    payload: Record<string, unknown>,
    destination: { brain_id?: string | null; url?: string | null } = {}
  ): string {
    // Must match the key shape queued/flushed items use (normalizeOutboxItem), so the
    // brain can recognize an outbox replay of a request that already arrived once.
    return outboxItemKey(
      endpoint,
      payload,
      outboxDestinationForKey({ brain_id: destination.brain_id || undefined, url: destination.url || undefined })
    );
  }

  function normalizeOutboxItem(item: OutboxItem): OutboxItem {
    return normalizeSharedOutboxItem(item);
  }

  function isRetryableBrainWriteStatus(status: number): boolean {
    return status === 408 || status === 425 || status === 429 || status >= 500;
  }

  function maxPendingIdempotencyRetries(): number {
    const raw = Number(process.env.BRAINSTACK_OUTBOX_MAX_425_RETRIES || "");
    return Number.isFinite(raw) && raw > 0 ? Math.trunc(raw) : 12;
  }

  function idempotencyPendingTerminalError(item: OutboxItem, status: number, responseText: string): string | null {
    if (status !== 425) {
      return null;
    }
    const retryLimit = maxPendingIdempotencyRetries();
    if ((item.retry_count || 0) < retryLimit) {
      return null;
    }
    return `HTTP 425 persisted after ${item.retry_count} flush attempt(s); operator review required before replaying this idempotent write. ${sanitizedHttpError(status, responseText)}`;
  }

  type BrainWriteOutcome =
    | { status: "accepted"; response?: Record<string, unknown> }
    | { status: "queued"; path: string; reason: string };

  async function postBrainWriteOrQueue(
    cfg: BrainstackConfig,
    endpoint: "import" | "propose",
    payload: Record<string, unknown>,
    overrides: { baseUrl?: string; importTokenEnv?: string; targetBrainId?: string } = {},
    options: { quiet?: boolean } = {}
  ): Promise<BrainWriteOutcome> {
    const writeConfig = brainWriteConfig(cfg);
    const baseUrl = overrides.baseUrl || writeConfig.baseUrl;
    const writeToken = overrides.importTokenEnv ? resolveClientEnvValue(cfg, overrides.importTokenEnv) : writeConfig.token;
    if (!baseUrl || !writeToken) {
      const reason = "brain base URL or import token is missing";
      const queued = await queueOutboxItem(
        cfg,
        {
          endpoint,
          url: baseUrl || "",
          brain_id: overrides.targetBrainId || undefined,
          import_token_env: overrides.importTokenEnv || undefined,
          payload,
          source_machine: String(payload.source_machine || cfg.machine.name),
          source_harness: String(payload.source_harness || cfg.harness.name)
        },
        reason
      );
      if (!options.quiet) {
        console.warn(`shared-brain write queued: ${queued}`);
      }
      return { status: "queued", path: queued, reason };
    }

    try {
      const response = await fetch(new URL(`/api/${endpoint}`, baseUrl).toString(), {
        method: "POST",
        headers: {
          Authorization: `Bearer ${writeToken}`,
          "Content-Type": "application/json",
          "Idempotency-Key": brainWriteIdempotencyKey(endpoint, payload, {
            brain_id: overrides.targetBrainId || null,
            url: baseUrl || null
          })
        },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(brainWriteTimeoutMs())
      });
      if (!response.ok) {
        const text = await response.text();
        if (isRetryableBrainWriteStatus(response.status)) {
          throw new Error(sanitizedHttpError(response.status, text));
        }
        throw new Error(`brain rejected ${endpoint} with HTTP ${response.status}: ${sanitizedHttpError(response.status, text)}`);
      }
      const text = await response.text();
      let parsed: Record<string, unknown> | undefined;
      if (text.trim()) {
        try {
          parsed = JSON.parse(text) as Record<string, unknown>;
        } catch {
          parsed = undefined;
        }
      }
      if (!options.quiet) {
        console.log(`shared-brain ${endpoint} accepted`);
      }
      return { status: "accepted", response: parsed };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.startsWith("brain rejected ")) {
        throw new Error(message);
      }
      const queued = await queueOutboxItem(
        cfg,
        {
          endpoint,
          url: baseUrl,
          brain_id: overrides.targetBrainId || undefined,
          import_token_env: overrides.importTokenEnv || undefined,
          payload,
          source_machine: String(payload.source_machine || cfg.machine.name),
          source_harness: String(payload.source_harness || cfg.harness.name)
        },
        message
      );
      if (!options.quiet) {
        console.warn(`shared-brain write queued: ${queued}`);
      }
      return { status: "queued", path: queued, reason: message };
    }
  }

  async function queueBrainWriteForBackgroundFlush(
    cfg: BrainstackConfig,
    endpoint: "import" | "propose",
    payload: Record<string, unknown>,
    reason: string
  ): Promise<string> {
    const writeConfig = brainWriteConfig(cfg);
    return await queueOutboxItem(
      cfg,
      {
        endpoint,
        url: writeConfig.baseUrl || "",
        payload,
        source_machine: String(payload.source_machine || cfg.machine.name),
        source_harness: String(payload.source_harness || cfg.harness.name)
      },
      reason
    );
  }

  async function listOutboxEntries(cfg: BrainstackConfig): Promise<OutboxEntry[]> {
    const roots = await outboxRoots(cfg);
    const entries: OutboxEntry[] = [];
    for (const root of roots) {
      entries.push(...(await scanOutbox(root)).items);
    }
    return entries;
  }

  async function outboxRoots(cfg: BrainstackConfig): Promise<string[]> {
    const roots = new Set<string>([outboxRoot(cfg)]);
    const parent = outboxParentRoot(cfg);
    if (existsSync(parent)) {
      const parentInfo = await lstat(parent).catch(() => null);
      if (!parentInfo?.isDirectory() || parentInfo.isSymbolicLink()) {
        return [...roots].sort();
      }
      for (const name of await readdir(parent).catch(() => [])) {
        const path = join(parent, name);
        const info = await lstat(path).catch(() => null);
        if (info?.isDirectory() || info?.isSymbolicLink()) {
          roots.add(path);
        }
      }
    }
    return [...roots].sort();
  }

  async function scanAllOutboxes(cfg: BrainstackConfig): Promise<Array<Awaited<ReturnType<typeof scanOutbox>>>> {
    const scans = [];
    for (const root of await outboxRoots(cfg)) {
      scans.push(await scanOutbox(root));
    }
    return scans;
  }

  async function purgeConfiguredOutboxParentCorruptEntries(
    cfg: BrainstackConfig,
    scans: Array<Awaited<ReturnType<typeof scanOutbox>>>
  ): Promise<number> {
    const parent = resolve(outboxParentRoot(cfg));
    let removed = 0;
    for (const entry of scans.flatMap((scan) => scan.corrupt)) {
      if (resolve(entry.path) !== parent || !/symlink|not a directory/i.test(entry.error)) {
        continue;
      }
      const info = await lstat(entry.path).catch(() => null);
      if (!info || info.isDirectory()) {
        continue;
      }
      await rm(entry.path, { force: true });
      removed += 1;
    }
    return removed;
  }

  async function coalesceOutboxItems(cfg: BrainstackConfig): Promise<void> {
    const seen = new Map<string, OutboxEntry>();
    for (const entry of await listOutboxEntries(cfg)) {
      entry.item = normalizeOutboxItem(entry.item);
      const key = entry.item.idempotency_key;
      const prior = seen.get(key);
      if (!prior) {
        await writeOutboxItem(entry.path, entry.item);
        seen.set(key, entry);
        continue;
      }
      const keep = prior.path.endsWith(`${entry.item.endpoint}-${key.slice(0, 32)}.json`) ? prior : entry;
      const drop = keep === prior ? entry : prior;
      keep.item.retry_count = Math.max(keep.item.retry_count || 0, drop.item.retry_count || 0);
      keep.item.created_at = [keep.item.created_at, drop.item.created_at].filter(Boolean).sort()[0] || keep.item.created_at;
      keep.item.idempotency_key = key;
      keep.item.last_error = keep.item.last_error || drop.item.last_error || null;
      keep.item.terminal_error = keep.item.terminal_error || drop.item.terminal_error || null;
      await writeOutboxItem(keep.path, keep.item);
      await rm(drop.path, { force: true });
      seen.set(key, keep);
    }
  }

  async function queuedProjectDestinationTrustError(
    cfg: BrainstackConfig,
    item: OutboxItem,
    payload: Record<string, unknown>,
    baseUrl: string
  ): Promise<string | null> {
    const repo = typeof payload.related_repo === "string" ? payload.related_repo : "";
    if (!repo || !item.brain_id || !item.import_token_env) {
      return null;
    }
    try {
      const resolved = await resolveProjectContext(cfg, repo);
      const brain = resolved.brains.find((candidate) => candidate.id === item.brain_id);
      if (!brain) {
        return `queued project write target ${item.brain_id} is no longer configured for ${repo}`;
      }
      if (!brain.connectionTrusted) {
        return `queued project write target ${item.brain_id} has untrusted repo-local connection fields (${untrustedBrainConnectionFields(brain)}); ${projectBrainTrustInstruction(cfg, brain)}`;
      }
      if (item.endpoint === "import" && brain.write !== "true") {
        return `queued project write target ${item.brain_id} is no longer trusted for direct import; current write mode is ${brain.write}`;
      }
      if (item.endpoint === "propose" && brain.write === "false") {
        return `queued project write target ${item.brain_id} is now read-only`;
      }
      if (brain.baseUrl !== baseUrl || brain.importTokenEnv !== item.import_token_env) {
        return `queued project write target ${item.brain_id} no longer matches trusted connection data for ${repo}`;
      }
      return null;
    } catch (error) {
      return `queued project write target ${item.brain_id} cannot be trusted: ${error instanceof Error ? error.message : String(error)}`;
    }
  }

  async function flushOutbox(cfg: BrainstackConfig): Promise<{ flushed: number; kept: number; terminalFailures: number; corrupt: number }> {
    let scans = await scanAllOutboxes(cfg);
    const corrupt = scans.reduce((sum, scan) => sum + scan.corrupt.length, 0);
    if (corrupt > 0) {
      const queued = scans.reduce((sum, scan) => sum + scan.items.length, 0);
      const terminalFailures = scans.reduce((sum, scan) => sum + scan.items.filter((entry) => normalizeOutboxItem(entry.item).terminal_error).length, 0);
      return { flushed: 0, kept: queued, terminalFailures, corrupt };
    }
    await coalesceOutboxItems(cfg);
    scans = await scanAllOutboxes(cfg);
    const items = await listOutboxEntries(cfg);
    let flushed = 0;
    let kept = 0;
    let terminalFailures = 0;
    for (const { path, item, payload } of items) {
      const normalizedItem = normalizeOutboxItem(item);
      if (normalizedItem.id !== item.id || normalizedItem.idempotency_key !== item.idempotency_key) {
        await writeOutboxItem(path, normalizedItem);
      }
      if (normalizedItem.terminal_error) {
        terminalFailures += 1;
        continue;
      }
      const baseUrl = normalizedItem.url || brainWriteConfig(cfg).baseUrl;
      const trustError = await queuedProjectDestinationTrustError(cfg, normalizedItem, payload, baseUrl);
      if (trustError) {
        normalizedItem.terminal_error = trustError;
        normalizedItem.last_error = trustError;
        await writeOutboxItem(path, normalizedItem);
        terminalFailures += 1;
        continue;
      }
      const writeToken = normalizedItem.import_token_env ? resolveClientEnvValue(cfg, normalizedItem.import_token_env) : brainWriteConfig(cfg).token;
      if (!baseUrl || !writeToken) {
        kept += 1;
        continue;
      }
      try {
        const response = await fetch(new URL(`/api/${normalizedItem.endpoint}`, baseUrl).toString(), {
          method: "POST",
          headers: {
            Authorization: `Bearer ${writeToken}`,
            "Content-Type": "application/json",
            "Idempotency-Key": normalizedItem.idempotency_key
          },
          body: JSON.stringify(payload),
          signal: AbortSignal.timeout(brainWriteTimeoutMs())
        });
        if (response.ok) {
          await rm(path, { force: true });
          flushed += 1;
        } else {
          const responseText = await response.text();
          normalizedItem.retry_count += 1;
          normalizedItem.last_error = sanitizedHttpError(response.status, responseText);
          const pendingTerminalError = idempotencyPendingTerminalError(normalizedItem, response.status, responseText);
          if (pendingTerminalError) {
            normalizedItem.terminal_error = pendingTerminalError;
            terminalFailures += 1;
          } else if (!isRetryableBrainWriteStatus(response.status)) {
            normalizedItem.terminal_error = normalizedItem.last_error;
            terminalFailures += 1;
          } else {
            kept += 1;
          }
          await writeOutboxItem(path, normalizedItem);
        }
      } catch (error) {
        normalizedItem.retry_count += 1;
        normalizedItem.last_error = error instanceof Error ? error.message : String(error);
        await writeOutboxItem(path, normalizedItem);
        kept += 1;
      }
    }
    return { flushed, kept, terminalFailures, corrupt };
  }

  async function retryOutboxItems(cfg: BrainstackConfig, args: ParsedArgs, scans: Array<Awaited<ReturnType<typeof scanOutbox>>>): Promise<number> {
    const target = args.positional[1];
    const retryAll = hasFlag(args, "all");
    if (!retryAll && !target) {
      throw new Error("outbox retry requires an item id or --all");
    }
    const corrupt = scans.flatMap((scan) => scan.corrupt);
    if (corrupt.length) {
      throw new Error("outbox retry found corrupt/unsafe entries; run `brainctl outbox list` and repair or purge-corrupt first");
    }
    let requeued = 0;
    const now = new Date().toISOString();
    for (const entry of scans.flatMap((scan) => scan.items)) {
      const normalized = normalizeOutboxItem(entry.item);
      const matches = retryAll || normalized.id === target || basename(entry.path) === target || basename(entry.path) === `${target}.json`;
      if (!matches) {
        continue;
      }
      if (retryAll && !normalized.terminal_error) {
        continue;
      }
      const next = {
        ...normalized,
        retry_count: 0,
        last_error: null,
        terminal_error: null,
        requeued_at: now
      } as OutboxItem & { requeued_at: string };
      await writeOutboxItem(entry.path, next);
      requeued += 1;
    }
    if (!requeued && target) {
      throw new Error(`outbox item not found or not retryable: ${target}`);
    }
    return requeued;
  }

  return {
    readEnvFile,
    clientEnv,
    resolveClientEnvValue,
    brainWriteConfig,
    isLoopbackHost,
    objectAt,
    resolveProjectContext,
    commandContext,
    deriveProjectLabel,
    validateMemoryScope,
    stringFromRecord,
    stringArrayFromRecord,
    uniqueNonEmptyStrings,
    boundedCliString,
    proposalEnrichmentPayload,
    proposalReviewGroupsFromResult,
    createReviewGroupMergeProposal,
    createAutomaticReviewGroupMerges,
    createHarnessProposalMerges,
    commandSearch,
    commandRemember,
    commandAllow,
    brainWriteTimeoutMs,
    brainWriteSmokeCheck,
    outboxItemKey,
    normalizeOutboxItem,
    postBrainWriteOrQueue,
    queueBrainWriteForBackgroundFlush,
    listOutboxEntries,
    outboxRoots,
    scanAllOutboxes,
    purgeConfiguredOutboxParentCorruptEntries,
    flushOutbox,
    retryOutboxItems
  };
}
