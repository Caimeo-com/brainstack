import { createReadStream, existsSync } from "node:fs";
import { appendFile, chmod, cp, lstat, mkdtemp, readFile, readdir, realpath, rm, stat, symlink, writeFile } from "node:fs/promises";
import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { lookup } from "node:dns/promises";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { isIP } from "node:net";
import { hostname, tmpdir } from "node:os";
import { basename, dirname, join, relative, resolve, sep } from "node:path";
import { flag, flagValues, hasFlag, requireFlagValue, type ParsedArgs } from "../args";
import { abs, absWithHome, expandHome, shellSingleQuote } from "../paths";
import {
  BRAINSTACK_HOOK_STATUS_MESSAGE,
  BRAINSTACK_SKILL_PACKAGE_KIND,
  brainstackDefaultConfigPath,
  loadConfig,
  type BrainstackConfig,
  type CheckStatus,
  type DoctorCheck,
  type HookTarget
} from "../config";
import { ensureDir, run, safeGitProtocolArgs, safeGitProtocolEnv, writeText } from "../runtime";

type SkillHookDeps = {
  canonicalJson: (value: unknown) => string;
  sha256Hex: (text: string) => string;
  parsePositiveIntegerFlag: (args: ParsedArgs, key: string, fallback: number) => number;
  postBrainWriteOrQueue: (
    cfg: BrainstackConfig,
    endpoint: "import" | "propose",
    payload: Record<string, unknown>,
    overrides?: { baseUrl?: string; importTokenEnv?: string; targetBrainId?: string },
    options?: { quiet?: boolean }
  ) => Promise<unknown>;
  queueBrainWriteForBackgroundFlush: (cfg: BrainstackConfig, endpoint: "import" | "propose", payload: Record<string, unknown>, reason: string) => Promise<string>;
  readDaemonStatus: (cfg: BrainstackConfig) => Promise<{ ok: boolean; updated_at?: string } | null>;
  daemonStatusFresh: (status: { ok: boolean; updated_at?: string } | null, maxAgeMs: number) => boolean;
  localSkillRefreshUnsafeReason: (cfg: BrainstackConfig) => Promise<string | null>;
  deriveProjectLabel: (repo: string) => string;
  uniqueNonEmptyStrings: (values: Array<string | undefined | null>) => string[];
};

export function createSkillHookCommands(deps: SkillHookDeps) {

  function quoteForBash(value: string): string {
    return shellSingleQuote(value);
  }

  const {
    canonicalJson,
    sha256Hex,
    parsePositiveIntegerFlag,
    postBrainWriteOrQueue,
    queueBrainWriteForBackgroundFlush,
    readDaemonStatus,
    daemonStatusFresh,
    localSkillRefreshUnsafeReason,
    deriveProjectLabel,
    uniqueNonEmptyStrings
  } = deps;

interface SkillPackageFile {
    path: string;
    encoding: "utf8" | "base64";
    content: string;
    size_bytes: number;
    sha256: string;
  }

  interface SkillPackage {
    schema_version: 1;
    kind: typeof BRAINSTACK_SKILL_PACKAGE_KIND;
    name: string;
    description?: string;
    imported_at: string;
    source: Record<string, unknown>;
    files: SkillPackageFile[];
    package_sha256: string;
  }

  interface SkillPackageWithManifest {
    manifest_path: string;
    raw_path: string;
    created_at: string;
    package: SkillPackage;
  }

  interface SkillImportScanCandidate {
    name: string;
    description?: string;
    root: string;
    sources: string[];
    priority: number;
    file_count: number;
    total_bytes: number;
    fingerprint: string;
    action: "install" | "update" | "already-current";
  }

  interface SkillImportRejectedCandidate {
    root: string;
    sources: string[];
    error: string;
  }

  interface SkillImportPlan {
    note: string;
    apply: boolean;
    repo: string;
    scan_roots: Array<{ source: string; path: string; exists: boolean }>;
    proposed: SkillImportScanCandidate[];
    skipped: Array<SkillImportScanCandidate & { reason: string }>;
    rejected: SkillImportRejectedCandidate[];
    warnings: string[];
    applied: string[];
  }

  interface RefreshSkillResult {
    repo: string;
    target: HookTarget;
    root: string;
    installed: string[];
    skipped: string[];
    warnings: string[];
  }

  interface SkillDoctorCheck {
    status: CheckStatus;
    skill: string;
    path: string;
    detail: string;
  }

  const SKILL_IMPORT_SKIP_DIRS = new Set([".git", "node_modules", ".venv", "__pycache__", "dist", "build"]);
  const SKILL_IMPORT_DEFAULT_MAX_BYTES = 2 * 1024 * 1024;
  const SKILL_IMPORT_DEFAULT_MAX_FILES = 200;
  const SKILL_IMPORT_DEFAULT_MAX_FILE_BYTES = 512 * 1024;
  const SKILL_IMPORT_SCAN_DEFAULT_MAX_DEPTH = 5;
  const SKILL_IMPORT_SCAN_DEFAULT_MAX_DIRS = 1500;

  function normalizeHookTarget(value: string | undefined, fallback: HookTarget = "codex"): HookTarget {
    const normalized = (value || fallback).trim().toLowerCase();
    if (normalized === "codex" || normalized === "claude" || normalized === "cursor") {
      return normalized;
    }
    throw new Error(`Unsupported hook target: ${value}. Expected codex, claude, cursor, or all.`);
  }

  function hookTargetsFromArgs(args: ParsedArgs): HookTarget[] {
    const target = (requireFlagValue(args, "target") || "codex").trim().toLowerCase();
    if (target === "all") {
      return ["codex", "claude", "cursor"];
    }
    return [normalizeHookTarget(target)];
  }

  function skillInstallRootForTarget(target: HookTarget, args: ParsedArgs): string {
    const explicitDir = requireFlagValue(args, "dir") || requireFlagValue(args, "out");
    if (explicitDir) {
      return abs(explicitDir);
    }
    const home = process.env.HOME ? abs(process.env.HOME) : "";
    if (!home) {
      throw new Error("Cannot locate home directory; set HOME or pass --dir.");
    }
    if (target === "codex") {
      const codexHome = process.env.CODEX_HOME ? abs(process.env.CODEX_HOME) : join(home, ".codex");
      return join(codexHome, "skills");
    }
    if (target === "claude") {
      return join(home, ".claude", "skills");
    }
    return join(home, ".cursor", "skills");
  }

  function safeSkillName(value: string): string {
    const name = value.trim();
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$/.test(name)) {
      throw new Error(`invalid skill name: ${value}`);
    }
    return name;
  }

  function parseSkillMetadata(text: string, fallbackName: string): { name: string; description?: string } {
    const metadata: Record<string, string> = {};
    if (text.startsWith("---\n")) {
      const end = text.indexOf("\n---", 4);
      if (end !== -1) {
        for (const line of text.slice(4, end).split(/\r?\n/)) {
          const match = line.match(/^([A-Za-z_][A-Za-z0-9_-]*)\s*:\s*(.*)$/);
          if (match) {
            metadata[match[1]] = match[2].trim().replace(/^['"]|['"]$/g, "");
          }
        }
      }
    }
    return {
      name: safeSkillName(metadata.name || fallbackName),
      description: metadata.description || undefined
    };
  }

  function isUrlLikeSkillSource(value: string): boolean {
    return /^(https?|ssh|git):\/\//i.test(value) || /^git@[^:]+:[^/].+/.test(value);
  }

  function validateSkillRelativePath(input: string): string {
    const normalized = input.replace(/\\/g, "/");
    if (!normalized || normalized.startsWith("/") || normalized.includes("\0")) {
      throw new Error(`unsafe skill file path: ${input}`);
    }
    const parts = normalized.split("/");
    if (parts.some((part) => !part || part === "." || part === "..")) {
      throw new Error(`unsafe skill file path: ${input}`);
    }
    return normalized;
  }

  function isUtf8Text(bytes: Uint8Array): { ok: true; text: string } | { ok: false } {
    try {
      return { ok: true, text: new TextDecoder("utf-8", { fatal: true }).decode(bytes) };
    } catch {
      return { ok: false };
    }
  }

  async function resolveLocalSkillRoot(input: string): Promise<{ root: string; sourcePath: string }> {
    const sourcePath = abs(input);
    const info = await lstat(sourcePath);
    if (info.isSymbolicLink()) {
      throw new Error(`refusing to import symlinked skill source: ${sourcePath}`);
    }
    if (info.isFile()) {
      if (basename(sourcePath) !== "SKILL.md") {
        throw new Error("local skill file imports must point at SKILL.md or a skill directory");
      }
      return { root: await realpath(dirname(sourcePath)), sourcePath };
    }
    if (!info.isDirectory()) {
      throw new Error(`skill source is not a file or directory: ${sourcePath}`);
    }
    const skillFile = join(sourcePath, "SKILL.md");
    const skillInfo = await lstat(skillFile).catch(() => null);
    if (!skillInfo || skillInfo.isSymbolicLink() || !skillInfo.isFile()) {
      throw new Error(`skill directory must contain a regular SKILL.md: ${sourcePath}`);
    }
    return { root: await realpath(sourcePath), sourcePath };
  }

  async function collectSkillPackageFiles(root: string, options: { maxBytes: number; maxFiles: number; maxFileBytes: number }): Promise<SkillPackageFile[]> {
    const files: SkillPackageFile[] = [];
    let totalBytes = 0;
    async function walk(dir: string): Promise<void> {
      const entries = await readdir(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = join(dir, entry.name);
        const relPath = validateSkillRelativePath(relative(root, fullPath).split(sep).join("/"));
        if (entry.isDirectory()) {
          if (!SKILL_IMPORT_SKIP_DIRS.has(entry.name)) {
            await walk(fullPath);
          }
          continue;
        }
        const info = await lstat(fullPath);
        if (info.isSymbolicLink()) {
          throw new Error(`refusing to import symlink inside skill: ${relPath}`);
        }
        if (!info.isFile()) {
          throw new Error(`refusing to import non-regular file inside skill: ${relPath}`);
        }
        if (info.nlink > 1) {
          throw new Error(`refusing to import hardlinked file inside skill: ${relPath}`);
        }
        if (info.size > options.maxFileBytes) {
          throw new Error(`skill file is too large: ${relPath} ${info.size} bytes > ${options.maxFileBytes}`);
        }
        if (files.length >= options.maxFiles) {
          throw new Error(`skill has too many files: > ${options.maxFiles}`);
        }
        totalBytes += info.size;
        if (totalBytes > options.maxBytes) {
          throw new Error(`skill package is too large: ${totalBytes} bytes > ${options.maxBytes}`);
        }
        const bytes = await readFile(fullPath);
        const text = isUtf8Text(bytes);
        files.push({
          path: relPath,
          encoding: text.ok ? "utf8" : "base64",
          content: text.ok ? text.text : Buffer.from(bytes).toString("base64"),
          size_bytes: bytes.byteLength,
          sha256: createHash("sha256").update(bytes).digest("hex")
        });
      }
    }
    await walk(root);
    if (!files.some((file) => file.path === "SKILL.md")) {
      throw new Error(`skill package missing SKILL.md: ${root}`);
    }
    return files.sort((a, b) => (a.path === "SKILL.md" ? -1 : b.path === "SKILL.md" ? 1 : a.path.localeCompare(b.path)));
  }

  function stripUndefinedForStableHash(value: unknown): unknown {
    if (Array.isArray(value)) {
      return value.map(stripUndefinedForStableHash);
    }
    if (!value || typeof value !== "object") {
      return value;
    }
    const output: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
      if (entry !== undefined) {
        output[key] = stripUndefinedForStableHash(entry);
      }
    }
    return output;
  }

  function skillPackageHash(pkg: Omit<SkillPackage, "package_sha256"> | SkillPackage): string {
    const { package_sha256: _ignored, ...unsigned } = pkg as SkillPackage;
    return sha256Hex(canonicalJson(stripUndefinedForStableHash(unsigned)));
  }

  async function findContainingGitRoot(path: string): Promise<string | null> {
    let current = (await lstat(path)).isDirectory() ? path : dirname(path);
    while (true) {
      if (existsSync(join(current, ".git"))) {
        return current;
      }
      const parent = dirname(current);
      if (parent === current) {
        return null;
      }
      current = parent;
    }
  }

  function gitOutput(args: string[], cwd: string, timeoutMs = 3000): string {
    const result = run(["git", ...args], { cwd, check: false, timeoutMs });
    return result.code === 0 ? result.stdout.trim() : "";
  }

  async function localSkillGitProvenance(skillRoot: string): Promise<Record<string, unknown> | undefined> {
    const gitRoot = await findContainingGitRoot(skillRoot).catch(() => null);
    if (!gitRoot) {
      return undefined;
    }
    const rel = relative(gitRoot, skillRoot) || ".";
    return {
      root: gitRoot,
      relative_path: rel.split(sep).join("/"),
      remote: gitOutput(["remote", "get-url", "origin"], gitRoot) || undefined,
      branch: gitOutput(["rev-parse", "--abbrev-ref", "HEAD"], gitRoot) || undefined,
      commit: gitOutput(["rev-parse", "HEAD"], gitRoot) || undefined,
      dirty: Boolean(gitOutput(["status", "--porcelain", "--", rel], gitRoot))
    };
  }

  async function buildLocalSkillPackage(input: string, options: { maxBytes: number; maxFiles: number; maxFileBytes: number }): Promise<SkillPackage> {
    const { root, sourcePath } = await resolveLocalSkillRoot(input);
    const skillText = await readFile(join(root, "SKILL.md"), "utf8");
    const metadata = parseSkillMetadata(skillText, basename(root));
    const files = await collectSkillPackageFiles(root, options);
    const base: Omit<SkillPackage, "package_sha256"> = {
      schema_version: 1,
      kind: BRAINSTACK_SKILL_PACKAGE_KIND,
      name: metadata.name,
      description: metadata.description,
      imported_at: new Date().toISOString(),
      source: {
        kind: "local",
        input,
        source_path: sourcePath,
        root,
        git: await localSkillGitProvenance(root)
      },
      files
    };
    return { ...base, package_sha256: skillPackageHash(base) };
  }

  function skillFilesFingerprint(files: SkillPackageFile[]): string {
    return sha256Hex(
      canonicalJson(
        files
          .map((file) => ({
            path: file.path,
            encoding: file.encoding,
            size_bytes: file.size_bytes,
            sha256: file.sha256
          }))
          .sort((a, b) => a.path.localeCompare(b.path))
      )
    );
  }

  function parseNonNegativeIntegerFlag(args: ParsedArgs, key: string, fallback: number): number {
    const raw = requireFlagValue(args, key);
    if (raw === undefined) {
      return fallback;
    }
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new Error(`--${key} must be a non-negative integer`);
    }
    return value;
  }

  function skillImportScanTargetsFromArgs(args: ParsedArgs): HookTarget[] {
    const target = (requireFlagValue(args, "target") || "all").trim().toLowerCase();
    if (target === "all") {
      return ["codex", "claude", "cursor"];
    }
    return [normalizeHookTarget(target)];
  }

  function skillScanRoots(args: ParsedArgs, targets: HookTarget[]): Array<{ source: string; path: string; priority: number; recursive: boolean }> {
    const roots: Array<{ source: string; path: string; priority: number; recursive: boolean }> = [];
    if (!hasFlag(args, "no-current")) {
      roots.push({ source: "cwd", path: process.cwd(), priority: 0, recursive: true });
    }
    for (const [index, dir] of flagValues(args, "scan-dir").entries()) {
      roots.push({ source: `scan-dir:${index + 1}`, path: abs(dir), priority: 5 + index, recursive: true });
    }
    for (const target of targets) {
      const targetArgs: ParsedArgs = { ...args, flags: { ...args.flags } };
      delete targetArgs.flags.dir;
      delete targetArgs.flags.out;
      roots.push({ source: target, path: skillInstallRootForTarget(target, targetArgs), priority: target === "codex" ? 20 : target === "claude" ? 30 : 40, recursive: true });
    }
    return roots;
  }

  async function discoverSkillRootsInTree(
    root: string,
    options: { maxDepth: number; maxDirs: number; recursive: boolean }
  ): Promise<{ roots: string[]; warnings: string[] }> {
    const warnings: string[] = [];
    const discovered: string[] = [];
    const absoluteRoot = abs(root);
    if (!existsSync(absoluteRoot)) {
      return { roots: [], warnings };
    }
    let scannedDirs = 0;
    let truncated = false;
    async function walk(dir: string, depth: number): Promise<void> {
      if (truncated) {
        return;
      }
      if (scannedDirs >= options.maxDirs) {
        truncated = true;
        warnings.push(`scan truncated at ${options.maxDirs} directories under ${absoluteRoot}`);
        return;
      }
      scannedDirs += 1;
      let info;
      try {
        info = await lstat(dir);
      } catch (error) {
        warnings.push(`scan skipped unreadable path ${dir}: ${error instanceof Error ? error.message : String(error)}`);
        return;
      }
      if (info.isSymbolicLink() || !info.isDirectory()) {
        return;
      }
      const skillFile = join(dir, "SKILL.md");
      const skillInfo = await lstat(skillFile).catch(() => null);
      if (skillInfo?.isFile() && !skillInfo.isSymbolicLink()) {
        discovered.push(await realpath(dir));
        return;
      }
      if (!options.recursive || depth >= options.maxDepth) {
        return;
      }
      let entries;
      try {
        entries = await readdir(dir, { withFileTypes: true });
      } catch (error) {
        warnings.push(`scan skipped unreadable directory ${dir}: ${error instanceof Error ? error.message : String(error)}`);
        return;
      }
      for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
        if (!entry.isDirectory() || SKILL_IMPORT_SKIP_DIRS.has(entry.name) || entry.name.startsWith(".")) {
          continue;
        }
        const child = join(dir, entry.name);
        const childInfo = await lstat(child).catch(() => null);
        if (!childInfo || childInfo.isSymbolicLink() || !childInfo.isDirectory()) {
          continue;
        }
        await walk(child, depth + 1);
        if (truncated) {
          break;
        }
      }
    }
    await walk(absoluteRoot, 0);
    return { roots: [...new Set(discovered)].sort((a, b) => a.localeCompare(b)), warnings };
  }

  async function existingSharedSkillFingerprints(cfg: BrainstackConfig, args: ParsedArgs): Promise<{ repo: string; fingerprints: Map<string, string>; warnings: string[] }> {
    const repo = absWithHome(requireFlagValue(args, "repo") || cfg.client.localPath || "~/shared-brain", cfg.paths.home);
    const warnings: string[] = [];
    const fingerprints = new Map<string, string>();
    if (!existsSync(repo)) {
      warnings.push(`shared-brain clone not found for existing-skill comparison: ${repo}`);
      return { repo, fingerprints, warnings };
    }
    try {
      for (const entry of chooseLatestSkillPackages(await discoverSkillPackages(repo))) {
        fingerprints.set(entry.package.name, skillFilesFingerprint(entry.package.files));
      }
    } catch (error) {
      warnings.push(`could not inspect existing shared skill packages: ${error instanceof Error ? error.message : String(error)}`);
    }
    return { repo, fingerprints, warnings };
  }

  async function buildSkillImportCandidate(
    root: string,
    sources: string[],
    priority: number,
    options: { maxBytes: number; maxFiles: number; maxFileBytes: number; existingFingerprints: Map<string, string> }
  ): Promise<SkillImportScanCandidate> {
    const skillText = await readFile(join(root, "SKILL.md"), "utf8");
    const metadata = parseSkillMetadata(skillText, basename(root));
    const files = await collectSkillPackageFiles(root, options);
    const fingerprint = skillFilesFingerprint(files);
    const existing = options.existingFingerprints.get(metadata.name);
    const action = existing === undefined ? "install" : existing === fingerprint ? "already-current" : "update";
    return {
      name: metadata.name,
      description: metadata.description,
      root,
      sources,
      priority,
      file_count: files.length,
      total_bytes: files.reduce((sum, file) => sum + file.size_bytes, 0),
      fingerprint,
      action
    };
  }

  async function buildSkillImportPlan(cfg: BrainstackConfig, args: ParsedArgs): Promise<SkillImportPlan> {
    const targets = skillImportScanTargetsFromArgs(args);
    const roots = skillScanRoots(args, targets);
    const maxDepth = parseNonNegativeIntegerFlag(args, "max-depth", SKILL_IMPORT_SCAN_DEFAULT_MAX_DEPTH);
    const maxScanDirs = parsePositiveIntegerFlag(args, "max-scan-dirs", SKILL_IMPORT_SCAN_DEFAULT_MAX_DIRS);
    const maxBytes = parsePositiveIntegerFlag(args, "max-bytes", SKILL_IMPORT_DEFAULT_MAX_BYTES);
    const maxFiles = parsePositiveIntegerFlag(args, "max-files", SKILL_IMPORT_DEFAULT_MAX_FILES);
    const maxFileBytes = parsePositiveIntegerFlag(args, "max-file-bytes", Math.min(SKILL_IMPORT_DEFAULT_MAX_FILE_BYTES, maxBytes));
    const selectedNames = new Set(flagValues(args, "skill").map(safeSkillName));
    const existing = await existingSharedSkillFingerprints(cfg, args);
    const warnings = [...existing.warnings];
    const scanRoots = roots.map((root) => ({ source: root.source, path: root.path, exists: existsSync(root.path) }));
    const rootsByPath = new Map<string, { root: string; sources: Set<string>; priority: number }>();
    const rejected: SkillImportRejectedCandidate[] = [];
    for (const scanRoot of roots) {
      const discovered = await discoverSkillRootsInTree(scanRoot.path, { maxDepth, maxDirs: maxScanDirs, recursive: scanRoot.recursive });
      warnings.push(...discovered.warnings);
      for (const root of discovered.roots) {
        const current = rootsByPath.get(root);
        if (current) {
          current.sources.add(scanRoot.source);
          current.priority = Math.min(current.priority, scanRoot.priority);
        } else {
          rootsByPath.set(root, { root, sources: new Set([scanRoot.source]), priority: scanRoot.priority });
        }
      }
    }
    const candidates: SkillImportScanCandidate[] = [];
    for (const entry of [...rootsByPath.values()].sort((a, b) => a.priority - b.priority || a.root.localeCompare(b.root))) {
      try {
        const candidate = await buildSkillImportCandidate(entry.root, [...entry.sources].sort(), entry.priority, {
          maxBytes,
          maxFiles,
          maxFileBytes,
          existingFingerprints: existing.fingerprints
        });
        if (!selectedNames.size || selectedNames.has(candidate.name)) {
          candidates.push(candidate);
        }
      } catch (error) {
        rejected.push({
          root: entry.root,
          sources: [...entry.sources].sort(),
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }
    const byName = new Map<string, SkillImportScanCandidate[]>();
    for (const candidate of candidates.sort((a, b) => a.name.localeCompare(b.name) || a.priority - b.priority || a.root.localeCompare(b.root))) {
      byName.set(candidate.name, [...(byName.get(candidate.name) || []), candidate]);
    }
    const proposed: SkillImportScanCandidate[] = [];
    const skipped: Array<SkillImportScanCandidate & { reason: string }> = [];
    const force = hasFlag(args, "force");
    for (const [name, group] of [...byName.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
      const [winner, ...duplicates] = group.sort((a, b) => a.priority - b.priority || a.root.localeCompare(b.root));
      if (!winner) {
        continue;
      }
      if (winner.action === "already-current" && !force) {
        skipped.push({ ...winner, reason: "already in shared brain with matching file content" });
      } else {
        proposed.push(winner);
      }
      for (const duplicate of duplicates) {
        skipped.push({ ...duplicate, reason: `duplicate skill name; selected ${name} from ${winner.root}` });
      }
    }
    return {
      note: "Proposed skills are global shared-brain imports; after apply, every connected harness can refresh and install them.",
      apply: hasFlag(args, "apply") || hasFlag(args, "yes"),
      repo: existing.repo,
      scan_roots: scanRoots,
      proposed,
      skipped: skipped.sort((a, b) => a.name.localeCompare(b.name) || a.root.localeCompare(b.root)),
      rejected: rejected.sort((a, b) => a.root.localeCompare(b.root)),
      warnings,
      applied: []
    };
  }

  function formatSkillImportCandidate(candidate: SkillImportScanCandidate): string {
    const description = candidate.description ? ` - ${candidate.description}` : "";
    return `  ${candidate.name} [${candidate.action}] ${candidate.root}${description} files=${candidate.file_count} bytes=${candidate.total_bytes} sources=${candidate.sources.join(",")}`;
  }

  function formatSkillImportPlan(plan: SkillImportPlan): string {
    const lines = [
      "Shared skill import plan",
      plan.note,
      `shared_brain_repo=${plan.repo}`,
      "",
      "Scan roots:",
      ...plan.scan_roots.map((root) => `  ${root.source}: ${root.path}${root.exists ? "" : " (missing)"}`),
      "",
      "Proposed global imports:",
      ...(plan.proposed.length ? plan.proposed.map(formatSkillImportCandidate) : ["  (none)"])
    ];
    if (plan.skipped.length) {
      lines.push("", "Skipped:", ...plan.skipped.map((candidate) => `${formatSkillImportCandidate(candidate)} reason=${candidate.reason}`));
    }
    if (plan.rejected.length) {
      lines.push("", "Rejected:", ...plan.rejected.map((candidate) => `  ${candidate.root} sources=${candidate.sources.join(",")} error=${candidate.error}`));
    }
    if (plan.warnings.length) {
      lines.push("", "Warnings:", ...plan.warnings.map((warning) => `  ${warning}`));
    }
    if (plan.applied.length) {
      lines.push("", `Applied imports: ${plan.applied.join(", ")}`);
    } else if (!plan.apply) {
      lines.push("", "No imports were written. Rerun with --apply to enqueue the proposed global shared-brain skill imports.");
    }
    return lines.join("\n");
  }

  async function commandImportSkills(args: ParsedArgs): Promise<void> {
    const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
    const plan = await buildSkillImportPlan(cfg, args);
    if (plan.apply) {
      const maxBytes = parsePositiveIntegerFlag(args, "max-bytes", SKILL_IMPORT_DEFAULT_MAX_BYTES);
      const maxFiles = parsePositiveIntegerFlag(args, "max-files", SKILL_IMPORT_DEFAULT_MAX_FILES);
      const maxFileBytes = parsePositiveIntegerFlag(args, "max-file-bytes", Math.min(SKILL_IMPORT_DEFAULT_MAX_FILE_BYTES, maxBytes));
      for (const candidate of plan.proposed) {
        const pkg = await buildLocalSkillPackage(candidate.root, { maxBytes, maxFiles, maxFileBytes });
        await postBrainWriteOrQueue(cfg, "import", {
          title: `Skill import: ${pkg.name}`,
          text: `${JSON.stringify(pkg, null, 2)}\n`,
          source_harness: requireFlagValue(args, "source-harness") || cfg.harness.name,
          source_machine: requireFlagValue(args, "source-machine") || cfg.machine.name,
          source_type: "skill",
          tags: ["brainstack", "brainstack-skill", `skill:${pkg.name}`]
        });
        plan.applied.push(pkg.name);
      }
    }
    if (hasFlag(args, "json")) {
      console.log(JSON.stringify(plan, null, 2));
    } else {
      console.log(formatSkillImportPlan(plan));
    }
  }

  function githubSkillUrl(input: string): { kind: "raw"; url: string } | { kind: "git"; remote: string; ref?: string; subdir?: string } | null {
    let parsed: URL;
    try {
      parsed = new URL(input);
    } catch {
      return null;
    }
    if (parsed.hostname !== "github.com") {
      return null;
    }
    const parts = parsed.pathname.split("/").filter(Boolean);
    if (parts.length < 2) {
      return null;
    }
    const [owner, repo, mode, ref, ...rest] = parts;
    const remote = `https://github.com/${owner}/${repo.replace(/\.git$/, "")}.git`;
    if (mode === "blob" && ref && rest.length) {
      return { kind: "raw", url: `https://raw.githubusercontent.com/${owner}/${repo}/${ref}/${rest.join("/")}` };
    }
    if (mode === "tree" && ref) {
      return { kind: "git", remote, ref, subdir: rest.join("/") || undefined };
    }
    if (!mode || mode === undefined) {
      return { kind: "git", remote };
    }
    return null;
  }

  function gitSkillUrl(input: string): { remote: string; ref?: string; subdir?: string } | null {
    const github = githubSkillUrl(input);
    if (github?.kind === "git") {
      return github;
    }
    if (/^git@[^:]+:[^/].+/.test(input) || /^ssh:\/\//i.test(input) || /^git:\/\//i.test(input) || /\.git(?:[#?].*)?$/i.test(input)) {
      return { remote: input };
    }
    return null;
  }

  function skillIpv4ToNumber(address: string): number {
    return address.split(".").reduce((sum, part) => (sum << 8) + Number(part), 0) >>> 0;
  }

  function skillInCidr4(address: string, base: string, bits: number): boolean {
    const mask = bits === 0 ? 0 : (0xffffffff << (32 - bits)) >>> 0;
    return (skillIpv4ToNumber(address) & mask) === (skillIpv4ToNumber(base) & mask);
  }

  function isBlockedSkillUrlIp(address: string): boolean {
    const version = isIP(address);
    if (version === 4) {
      const blockedRanges: Array<[string, number]> = [
        ["0.0.0.0", 8],
        ["10.0.0.0", 8],
        ["100.64.0.0", 10],
        ["127.0.0.0", 8],
        ["169.254.0.0", 16],
        ["172.16.0.0", 12],
        ["192.168.0.0", 16]
      ];
      return blockedRanges.some(([base, bits]) => skillInCidr4(address, base, bits));
    }
    if (version === 6) {
      const normalized = address.toLowerCase();
      const ipv4Mapped = normalized.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/);
      if (ipv4Mapped) {
        return isBlockedSkillUrlIp(ipv4Mapped[1]);
      }
      return normalized === "::1" || normalized.startsWith("fe80:") || normalized.startsWith("fc") || normalized.startsWith("fd");
    }
    return false;
  }

  interface SafeSkillHttpTarget {
    url: URL;
    address: string;
    family: 4 | 6;
  }

  async function safeSkillHttpTarget(input: string, allowPrivateUrl: boolean): Promise<SafeSkillHttpTarget> {
    let url: URL;
    try {
      url = new URL(input);
    } catch {
      throw new Error("skill URL import requires a valid URL");
    }
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      throw new Error("skill URL import only allows http and https for raw file fetches");
    }
    const hostnameValue = url.hostname.replace(/^\[|\]$/g, "").toLowerCase();
    if (!allowPrivateUrl && (hostnameValue === "localhost" || hostnameValue.endsWith(".localhost"))) {
      throw new Error("skill URL import blocked localhost hostname; rerun with --allow-private-url only for trusted private fetches");
    }
    const directIpVersion = isIP(hostnameValue);
    const addresses = directIpVersion
      ? [{ address: hostnameValue, family: directIpVersion }]
      : await lookup(hostnameValue, { all: true, verbatim: true });
    if (!addresses.length) {
      throw new Error("skill URL import DNS lookup returned no addresses");
    }
    if (!allowPrivateUrl) {
      const blocked = addresses.find((entry) => isBlockedSkillUrlIp(entry.address));
      if (blocked) {
        throw new Error(`skill URL import blocked private address ${blocked.address}; rerun with --allow-private-url only for trusted private fetches`);
      }
    }
    const selected = addresses[0];
    return { url, address: selected.address, family: selected.family === 6 ? 6 : 4 };
  }

  async function fetchSkillUrlText(input: string, options: { maxBytes: number; allowPrivateUrl: boolean; redirectsLeft?: number }): Promise<{ url: string; text: string }> {
    const redirectsLeft = options.redirectsLeft ?? 5;
    const target = await safeSkillHttpTarget(input, options.allowPrivateUrl);
    const response = await new Promise<{ statusCode: number; statusMessage: string; headers: Record<string, string | string[] | undefined>; bytes: Buffer }>((resolvePromise, rejectPromise) => {
      const { url, address, family } = target;
      const requestImpl = url.protocol === "https:" ? httpsRequest : httpRequest;
      const req = requestImpl(
        {
          protocol: url.protocol,
          hostname: url.hostname,
          port: url.port || (url.protocol === "https:" ? 443 : 80),
          path: `${url.pathname}${url.search}`,
          method: "GET",
          headers: {
            Host: url.host,
            "User-Agent": "brainstack-skill-url-import"
          },
          servername: url.hostname,
          lookup(_hostname, _options, callback) {
            callback(null, address, family);
          },
          timeout: 20_000
        },
        (res) => {
          const length = Number(res.headers["content-length"] || 0);
          if (length > options.maxBytes) {
            res.destroy();
            rejectPromise(new Error(`skill URL content is too large: ${length} bytes > ${options.maxBytes}`));
            return;
          }
          const chunks: Buffer[] = [];
          let total = 0;
          res.on("data", (chunk: Buffer) => {
            total += chunk.byteLength;
            if (total > options.maxBytes) {
              const error = new Error(`skill URL streamed content is too large: ${total} bytes > ${options.maxBytes}`);
              rejectPromise(error);
              res.destroy(error);
              return;
            }
            chunks.push(chunk);
          });
          res.on("end", () => resolvePromise({ statusCode: res.statusCode || 0, statusMessage: res.statusMessage || "", headers: res.headers, bytes: Buffer.concat(chunks) }));
        }
      );
      req.on("timeout", () => req.destroy(new Error("skill URL fetch timed out")));
      req.on("error", rejectPromise);
      req.end();
    });
    if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
      if (redirectsLeft <= 0) {
        throw new Error("skill URL import exceeded redirect limit");
      }
      const location = Array.isArray(response.headers.location) ? response.headers.location[0] : response.headers.location;
      return await fetchSkillUrlText(new URL(location || "", target.url).toString(), {
        maxBytes: options.maxBytes,
        allowPrivateUrl: options.allowPrivateUrl,
        redirectsLeft: redirectsLeft - 1
      });
    }
    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw new Error(`skill URL fetch failed with HTTP ${response.statusCode}: ${target.url.toString()}`);
    }
    return { url: target.url.toString(), text: new TextDecoder().decode(response.bytes) };
  }

  async function buildSingleFileSkillPackageFromUrl(input: string, url: string, options: { maxBytes: number; allowPrivateUrl: boolean }): Promise<SkillPackage> {
    const fetched = await fetchSkillUrlText(url, options);
    const text = fetched.text;
    const bytes = new TextEncoder().encode(text);
    const metadata = parseSkillMetadata(text, basename(new URL(fetched.url).pathname) === "SKILL.md" ? basename(dirname(new URL(fetched.url).pathname)) || "skill" : "skill");
    const file: SkillPackageFile = {
      path: "SKILL.md",
      encoding: "utf8",
      content: text,
      size_bytes: bytes.byteLength,
      sha256: createHash("sha256").update(bytes).digest("hex")
    };
    const base: Omit<SkillPackage, "package_sha256"> = {
      schema_version: 1,
      kind: BRAINSTACK_SKILL_PACKAGE_KIND,
      name: metadata.name,
      description: metadata.description,
      imported_at: new Date().toISOString(),
      source: {
        kind: "url",
        input,
        url: fetched.url
      },
      files: [file]
    };
    return { ...base, package_sha256: skillPackageHash(base) };
  }

  /**
   * Git skill remotes must pass the same private-network guard as raw HTTP imports, and
   * SSH/git-protocol remotes can invoke local SSH credentials, so they need an explicit
   * opt-in even for public hosts.
   */
  async function assertSafeGitSkillRemote(remote: string, options: { allowPrivateUrl: boolean; allowSshGit: boolean }): Promise<void> {
    const trimmed = remote.trim();
    if (!trimmed || trimmed.startsWith("-")) {
      throw new Error(`skill git import remote is not a valid remote: ${trimmed.slice(0, 120)}`);
    }
    if (/[\u0000-\u001f]/.test(trimmed)) {
      throw new Error("skill git import remote contains control characters");
    }
    let protocol: string;
    let host: string;
    const scpLike = trimmed.match(/^git@([^:/]+):/);
    if (scpLike) {
      protocol = "ssh";
      host = scpLike[1];
    } else {
      let parsed: URL;
      try {
        parsed = new URL(trimmed);
      } catch {
        throw new Error(`skill git import remote is not a valid URL: ${trimmed.slice(0, 120)}`);
      }
      protocol = parsed.protocol.replace(/:$/, "").toLowerCase();
      host = parsed.hostname.replace(/^\[|\]$/g, "").toLowerCase();
    }
    if (protocol === "file") {
      // Local repositories are operator-owned input; the private-network guard does not apply.
      return;
    }
    if (!["https", "http", "ssh", "git"].includes(protocol)) {
      throw new Error(`skill git import does not allow ${protocol}:// remotes`);
    }
    if ((protocol === "ssh" || protocol === "git") && !options.allowSshGit) {
      throw new Error("skill git import over ssh/git protocols can use local SSH credentials; rerun with --allow-ssh-git only for trusted remotes, or use an https remote");
    }
    if (!options.allowPrivateUrl) {
      if (!host || host === "localhost" || host.endsWith(".localhost")) {
        throw new Error("skill git import blocked localhost remote; rerun with --allow-private-url only for trusted private fetches");
      }
      const directIpVersion = isIP(host);
      const addresses = directIpVersion ? [{ address: host, family: directIpVersion }] : await lookup(host, { all: true, verbatim: true });
      if (!addresses.length) {
        throw new Error("skill git import DNS lookup returned no addresses");
      }
      const blocked = addresses.find((entry) => isBlockedSkillUrlIp(entry.address));
      if (blocked) {
        throw new Error(`skill git import blocked private address ${blocked.address}; rerun with --allow-private-url only for trusted private fetches`);
      }
    }
  }

  async function buildUrlSkillPackage(input: string, options: { maxBytes: number; maxFiles: number; maxFileBytes: number; allowPrivateUrl: boolean; allowSshGit: boolean }): Promise<SkillPackage> {
    const github = githubSkillUrl(input);
    if (github?.kind === "raw") {
      return await buildSingleFileSkillPackageFromUrl(input, github.url, { maxBytes: Math.min(options.maxBytes, options.maxFileBytes), allowPrivateUrl: options.allowPrivateUrl });
    }
    const gitSource = gitSkillUrl(input);
    if (!gitSource) {
      return await buildSingleFileSkillPackageFromUrl(input, input, { maxBytes: Math.min(options.maxBytes, options.maxFileBytes), allowPrivateUrl: options.allowPrivateUrl });
    }
    await assertSafeGitSkillRemote(gitSource.remote, { allowPrivateUrl: options.allowPrivateUrl, allowSshGit: options.allowSshGit });
    const tempRoot = await mkdtemp(join(tmpdir(), "brainstack-skill-url-"));
    try {
      const checkout = join(tempRoot, "checkout");
      const cloneArgs = ["git", ...safeGitProtocolArgs(gitSource.remote), "clone", "--depth", "1"];
      if (!options.allowPrivateUrl) {
        // The pre-clone DNS guard checks only the original host; refuse HTTP redirects
        // so the clone cannot be bounced to a private target after the check.
        cloneArgs.splice(1, 0, "-c", "http.followRedirects=false");
      }
      if (gitSource.ref) {
        if (gitSource.ref.startsWith("-")) {
          throw new Error(`skill git import ref is not a valid ref: ${gitSource.ref.slice(0, 120)}`);
        }
        cloneArgs.push("--branch", gitSource.ref);
      }
      cloneArgs.push("--", gitSource.remote, checkout);
      run(cloneArgs, { check: true, timeoutMs: 60_000, env: safeGitProtocolEnv(gitSource.remote) });
      const root = gitSource.subdir ? join(checkout, gitSource.subdir) : checkout;
      const packageRoot = (await resolveLocalSkillRoot(root)).root;
      const skillText = await readFile(join(packageRoot, "SKILL.md"), "utf8");
      const metadata = parseSkillMetadata(skillText, basename(packageRoot));
      const files = await collectSkillPackageFiles(packageRoot, options);
      const base: Omit<SkillPackage, "package_sha256"> = {
        schema_version: 1,
        kind: BRAINSTACK_SKILL_PACKAGE_KIND,
        name: metadata.name,
        description: metadata.description,
        imported_at: new Date().toISOString(),
        source: {
          kind: "url",
          input,
          remote: gitSource.remote,
          ref: gitSource.ref,
          subdir: gitSource.subdir,
          commit: gitOutput(["rev-parse", "HEAD"], checkout) || undefined
        },
        files
      };
      return { ...base, package_sha256: skillPackageHash(base) };
    } finally {
      await rm(tempRoot, { recursive: true, force: true });
    }
  }

  function validateSkillPackage(value: unknown, label: string): SkillPackage {
    if (!value || typeof value !== "object") {
      throw new Error(`invalid skill package ${label}: expected object`);
    }
    const pkg = value as SkillPackage;
    if (pkg.schema_version !== 1 || pkg.kind !== BRAINSTACK_SKILL_PACKAGE_KIND) {
      throw new Error(`invalid skill package ${label}: unsupported schema or kind`);
    }
    pkg.name = safeSkillName(String(pkg.name || ""));
    if (!Array.isArray(pkg.files) || !pkg.files.length) {
      throw new Error(`invalid skill package ${label}: files are required`);
    }
    for (const file of pkg.files) {
      file.path = validateSkillRelativePath(String(file.path || ""));
      if (file.encoding !== "utf8" && file.encoding !== "base64") {
        throw new Error(`invalid skill package ${label}: unsupported file encoding for ${file.path}`);
      }
      if (typeof file.content !== "string" || typeof file.sha256 !== "string") {
        throw new Error(`invalid skill package ${label}: invalid file content metadata for ${file.path}`);
      }
      const bytes = file.encoding === "utf8" ? new TextEncoder().encode(file.content) : Buffer.from(file.content, "base64");
      const actualSha = createHash("sha256").update(bytes).digest("hex");
      if (actualSha !== file.sha256) {
        throw new Error(`invalid skill package ${label}: sha256 mismatch for ${file.path}`);
      }
    }
    if (!pkg.files.some((file) => file.path === "SKILL.md")) {
      throw new Error(`invalid skill package ${label}: missing SKILL.md`);
    }
    if (pkg.package_sha256 && pkg.package_sha256 !== skillPackageHash(pkg)) {
      throw new Error(`invalid skill package ${label}: package sha256 mismatch`);
    }
    return pkg;
  }

  async function commandImport(args: ParsedArgs): Promise<void> {
    const subcommand = args.positional[0] || "help";
    if (subcommand === "skills") {
      return await commandImportSkills(args);
    }
    if (subcommand === "codex-session") {
      return await commandImportCodexSession(args);
    }
    if (subcommand !== "skill") {
      if (subcommand === "help" || subcommand === "--help" || subcommand === "-h") {
        console.log("Usage: brainctl import skill <SKILL.md|DIR|URL> --config brainstack.yaml [--title TITLE] [--source-harness HARNESS] [--source-machine MACHINE] [--max-bytes N] [--max-files N] [--allow-private-url] [--allow-ssh-git]\n       brainctl import skills [--config brainstack.yaml] [--target codex|claude|cursor|all] [--scan-dir DIR] [--skill NAME] [--apply] [--json]\n       brainctl import codex-session <SESSION_ID|JSONL_PATH> [--config brainstack.yaml] [--include-transcript] [--max-bytes N] [--dry-run] [--json]");
        return;
      }
      throw new Error(`Unknown import subcommand: ${subcommand}`);
    }
    const source = args.positional[1] || requireFlagValue(args, "source");
    if (!source) {
      throw new Error("import skill requires a SKILL.md path, skill directory, or URL");
    }
    const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
    const maxBytes = parsePositiveIntegerFlag(args, "max-bytes", SKILL_IMPORT_DEFAULT_MAX_BYTES);
    const maxFiles = parsePositiveIntegerFlag(args, "max-files", SKILL_IMPORT_DEFAULT_MAX_FILES);
    const maxFileBytes = parsePositiveIntegerFlag(args, "max-file-bytes", Math.min(SKILL_IMPORT_DEFAULT_MAX_FILE_BYTES, maxBytes));
    const allowPrivateUrl = hasFlag(args, "allow-private-url");
    const allowSshGit = hasFlag(args, "allow-ssh-git");
    const pkg = isUrlLikeSkillSource(source)
      ? await buildUrlSkillPackage(source, { maxBytes, maxFiles, maxFileBytes, allowPrivateUrl, allowSshGit })
      : await buildLocalSkillPackage(source, { maxBytes, maxFiles, maxFileBytes });
    validateSkillPackage(pkg, source);
    const title = requireFlagValue(args, "title") || `Skill import: ${pkg.name}`;
    await postBrainWriteOrQueue(cfg, "import", {
      title,
      text: `${JSON.stringify(pkg, null, 2)}\n`,
      source_harness: requireFlagValue(args, "source-harness") || cfg.harness.name,
      source_machine: requireFlagValue(args, "source-machine") || cfg.machine.name,
      source_type: "skill",
      tags: ["brainstack", "brainstack-skill", `skill:${pkg.name}`]
    });
    console.log(`skill_package=${pkg.name} files=${pkg.files.length} sha256=${pkg.package_sha256}`);
  }

  function safeRepoRelativeFile(repo: string, repoPath: string): string {
    const normalized = validateSkillRelativePath(repoPath);
    const absolute = resolve(repo, normalized);
    const root = resolve(repo);
    if (absolute !== root && !absolute.startsWith(`${root}${sep}`)) {
      throw new Error(`repo path escapes clone: ${repoPath}`);
    }
    return absolute;
  }

  interface CodexSessionSummary {
    id: string;
    threadName?: string;
    path: string;
    bytes: number;
    sha256: string;
    startedAt?: string;
    updatedAt?: string;
    cwd?: string;
    originator?: string;
    cliVersion?: string;
    source?: string;
    modelProvider?: string;
    lastAgentMessage?: string;
  }

  function codexHomePath(): string {
    return process.env.CODEX_HOME ? abs(process.env.CODEX_HOME) : process.env.HOME ? join(abs(process.env.HOME), ".codex") : ".codex";
  }

  function isCodexSessionId(value: string): boolean {
    return /^[0-9a-f]{8,}-[0-9a-f-]{20,}$/i.test(value.trim());
  }

  async function findCodexSessionById(sessionId: string, codexHome: string): Promise<string | null> {
    const roots = [join(codexHome, "sessions"), join(codexHome, "archived_sessions")];
    const maxFiles = 25_000;
    const maxDirs = 4_000;
    const candidates: Array<{ path: string; mtimeMs: number }> = [];
    let files = 0;
    let dirs = 0;
    for (const root of roots) {
      if (!existsSync(root)) {
        continue;
      }
      const stack = [root];
      while (stack.length) {
        const dir = stack.pop()!;
        dirs += 1;
        if (dirs > maxDirs) {
          throw new Error(`Codex session search exceeded ${maxDirs} directories under ${codexHome}; pass the JSONL path explicitly`);
        }
        const info = await lstat(dir).catch(() => null);
        if (!info?.isDirectory() || info.isSymbolicLink()) {
          continue;
        }
        for (const name of await readdir(dir).catch(() => [])) {
          const path = join(dir, name);
          const entryInfo = await lstat(path).catch(() => null);
          if (!entryInfo || entryInfo.isSymbolicLink()) {
            continue;
          }
          if (entryInfo.isDirectory()) {
            stack.push(path);
            continue;
          }
          if (!entryInfo.isFile()) {
            continue;
          }
          files += 1;
          if (files > maxFiles) {
            throw new Error(`Codex session search exceeded ${maxFiles} files under ${codexHome}; pass the JSONL path explicitly`);
          }
          if (name.endsWith(".jsonl") && name.includes(sessionId)) {
            candidates.push({ path, mtimeMs: entryInfo.mtimeMs });
          }
        }
      }
    }
    candidates.sort((a, b) => b.mtimeMs - a.mtimeMs || b.path.localeCompare(a.path));
    return candidates[0]?.path || null;
  }

  async function codexThreadName(sessionId: string, codexHome: string): Promise<string | undefined> {
    const indexPath = join(codexHome, "session_index.jsonl");
    if (!existsSync(indexPath)) {
      return undefined;
    }
    const indexInfo = await lstat(indexPath).catch(() => null);
    if (!indexInfo?.isFile() || indexInfo.isSymbolicLink() || indexInfo.size > 10 * 1024 * 1024) {
      return undefined;
    }
    let latestName: string | undefined;
    for (const line of (await readFile(indexPath, "utf8")).split(/\r?\n/)) {
      if (!line.includes(sessionId)) {
        continue;
      }
      try {
        const parsed = JSON.parse(line) as Record<string, unknown>;
        const name = typeof parsed.thread_name === "string" ? parsed.thread_name.trim() : "";
        latestName = name || latestName;
      } catch {
        continue;
      }
    }
    return latestName;
  }

  function cappedText(value: unknown, maxChars: number): string | undefined {
    if (typeof value !== "string" || !value.trim()) {
      return undefined;
    }
    const trimmed = value.trim();
    return trimmed.length > maxChars ? `${trimmed.slice(0, maxChars)}\n[truncated]` : trimmed;
  }

  async function resolveCodexSessionPath(reference: string, codexHome: string): Promise<string> {
    const expanded = expandHome(reference);
    if (existsSync(expanded)) {
      return abs(expanded);
    }
    if (!isCodexSessionId(reference)) {
      throw new Error(`Codex session reference is neither an existing path nor a session id: ${reference}`);
    }
    const found = await findCodexSessionById(reference, codexHome);
    if (!found) {
      throw new Error(`Codex session not found: ${reference}. Pass the JSONL path explicitly or verify ${codexHome}/session_index.jsonl.`);
    }
    return found;
  }

  async function readCodexSessionSummary(reference: string, maxBytes: number): Promise<{ summary: CodexSessionSummary; transcript?: string }> {
    const codexHome = codexHomePath();
    const path = await resolveCodexSessionPath(reference, codexHome);
    const info = await lstat(path);
    if (!info.isFile() || info.isSymbolicLink()) {
      throw new Error(`Codex session path must be a regular file: ${path}`);
    }
    if (info.size > maxBytes) {
      throw new Error(`Codex session is too large: ${info.size} bytes > --max-bytes ${maxBytes}`);
    }
    const transcript = await readFile(path, "utf8");
    const summary: CodexSessionSummary = {
      id: reference,
      path,
      bytes: info.size,
      sha256: createHash("sha256").update(transcript).digest("hex"),
      updatedAt: info.mtime.toISOString()
    };
    for (const line of transcript.split(/\r?\n/)) {
      if (!line.trim()) {
        continue;
      }
      let parsed: Record<string, unknown>;
      try {
        parsed = JSON.parse(line) as Record<string, unknown>;
      } catch {
        continue;
      }
      const payload = parsed.payload && typeof parsed.payload === "object" && !Array.isArray(parsed.payload) ? (parsed.payload as Record<string, unknown>) : {};
      if (parsed.type === "session_meta") {
        summary.id = typeof payload.id === "string" ? payload.id : summary.id;
        summary.startedAt = typeof payload.timestamp === "string" ? payload.timestamp : summary.startedAt;
        summary.cwd = typeof payload.cwd === "string" ? payload.cwd : summary.cwd;
        summary.originator = typeof payload.originator === "string" ? payload.originator : summary.originator;
        summary.cliVersion = typeof payload.cli_version === "string" ? payload.cli_version : summary.cliVersion;
        summary.source = typeof payload.source === "string" ? payload.source : summary.source;
        summary.modelProvider = typeof payload.model_provider === "string" ? payload.model_provider : summary.modelProvider;
      }
      if (payload.type === "task_complete") {
        summary.lastAgentMessage = cappedText(payload.last_agent_message, 8_000) || summary.lastAgentMessage;
      }
    }
    summary.threadName = await codexThreadName(summary.id, codexHome);
    return { summary, transcript };
  }

  function codexSessionImportText(summary: CodexSessionSummary, transcript: string | undefined, includeTranscript: boolean): string {
    const lines = [
      `# Codex session ${summary.id}`,
      "",
      `- Session id: \`${summary.id}\``,
      summary.threadName ? `- Thread: ${summary.threadName}` : "",
      summary.cwd ? `- Cwd: \`${summary.cwd}\`` : "",
      summary.startedAt ? `- Started: \`${summary.startedAt}\`` : "",
      summary.updatedAt ? `- Updated: \`${summary.updatedAt}\`` : "",
      summary.originator ? `- Originator: \`${summary.originator}\`` : "",
      summary.cliVersion ? `- CLI version: \`${summary.cliVersion}\`` : "",
      summary.source ? `- Source: \`${summary.source}\`` : "",
      summary.modelProvider ? `- Model provider: \`${summary.modelProvider}\`` : "",
      `- Local transcript path: \`${summary.path}\``,
      `- Transcript bytes: \`${summary.bytes}\``,
      `- Transcript sha256: \`${summary.sha256}\``,
      "",
      "## Last Agent Message",
      "",
      summary.lastAgentMessage || "No task-complete final message was found in the imported JSONL.",
      "",
      "## Capture Note",
      "",
      includeTranscript
        ? "The transcript below was explicitly included by the importing operator."
        : "This import is a bounded session checkpoint. Re-import with `brainctl import codex-session ... --include-transcript` when full raw evidence is needed.",
      ""
    ].filter(Boolean);
    if (includeTranscript && transcript !== undefined) {
      lines.push("## Transcript", "", "```jsonl", transcript.trimEnd(), "```", "");
    }
    return lines.join("\n");
  }

  async function codexSessionImportPayload(
    cfg: BrainstackConfig,
    reference: string,
    args: ParsedArgs,
    options: { includeTranscript: boolean }
  ): Promise<Record<string, unknown>> {
    const maxBytes = parsePositiveIntegerFlag(args, "max-bytes", options.includeTranscript ? 20 * 1024 * 1024 : 5 * 1024 * 1024);
    const { summary, transcript } = await readCodexSessionSummary(reference, maxBytes);
    const title = requireFlagValue(args, "title") || `Codex session: ${summary.threadName || summary.id}`;
    const text = codexSessionImportText(summary, transcript, options.includeTranscript);
    const tags = uniqueNonEmptyStrings([
      "codex-session",
      options.includeTranscript ? "codex-transcript" : "codex-session-checkpoint",
      `session:${summary.id}`,
      summary.cwd ? `repo:${deriveProjectLabel(summary.cwd)}` : undefined,
      ...flagValues(args, "tags")
    ]);
    return {
      title,
      text,
      source_harness: requireFlagValue(args, "source-harness") || cfg.harness.name,
      source_machine: requireFlagValue(args, "source-machine") || cfg.machine.name,
      source_type: options.includeTranscript ? "codex-session-transcript" : "codex-session-checkpoint",
      conversation_id: summary.id,
      related_repo: summary.cwd,
      transcript_path: summary.path,
      transcript_bytes: summary.bytes,
      transcript_sha256: summary.sha256,
      started_at: summary.startedAt,
      updated_at: summary.updatedAt,
      tags
    };
  }

  async function commandImportCodexSession(args: ParsedArgs): Promise<void> {
    const reference = args.positional[1] || requireFlagValue(args, "session") || requireFlagValue(args, "source");
    if (!reference) {
      throw new Error("import codex-session requires a session id or JSONL path");
    }
    const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
    const includeTranscript = hasFlag(args, "include-transcript");
    const payload = await codexSessionImportPayload(cfg, reference, args, { includeTranscript });
    if (hasFlag(args, "json") || hasFlag(args, "dry-run")) {
      console.log(JSON.stringify({ dry_run: hasFlag(args, "dry-run"), payload }, null, 2));
      if (hasFlag(args, "dry-run")) {
        return;
      }
    }
    await postBrainWriteOrQueue(cfg, "import", payload);
  }

  async function discoverSkillPackages(repo: string): Promise<SkillPackageWithManifest[]> {
    const manifestDir = join(repo, "manifests", "sources");
    if (!existsSync(manifestDir)) {
      return [];
    }
    const packages: SkillPackageWithManifest[] = [];
    for (const entry of await readdir(manifestDir)) {
      if (!entry.endsWith(".json")) {
        continue;
      }
      const manifestPath = join(manifestDir, entry);
      let manifest: Record<string, unknown>;
      try {
        manifest = JSON.parse(await readFile(manifestPath, "utf8")) as Record<string, unknown>;
      } catch {
        continue;
      }
      if (manifest.source_type !== "skill") {
        continue;
      }
      const rawPath = typeof manifest.raw_path === "string" ? manifest.raw_path : "";
      if (!rawPath) {
        continue;
      }
      try {
        const rawAbsolute = safeRepoRelativeFile(repo, rawPath);
        const rawInfo = await lstat(rawAbsolute);
        if (rawInfo.isSymbolicLink() || !rawInfo.isFile()) {
          continue;
        }
        const pkg = validateSkillPackage(JSON.parse(await readFile(rawAbsolute, "utf8")), rawPath);
        packages.push({
          manifest_path: relative(repo, manifestPath).split(sep).join("/"),
          raw_path: rawPath,
          created_at: String(manifest.created_at || pkg.imported_at || ""),
          package: pkg
        });
      } catch {
        continue;
      }
    }
    return packages.sort((a, b) => `${a.package.name}\0${a.created_at}`.localeCompare(`${b.package.name}\0${b.created_at}`));
  }

  function chooseLatestSkillPackages(packages: SkillPackageWithManifest[]): SkillPackageWithManifest[] {
    const latest = new Map<string, SkillPackageWithManifest>();
    for (const entry of packages) {
      const current = latest.get(entry.package.name);
      if (!current || entry.created_at.localeCompare(current.created_at) >= 0) {
        latest.set(entry.package.name, entry);
      }
    }
    return [...latest.values()].sort((a, b) => a.package.name.localeCompare(b.package.name));
  }

  async function installSkillPackage(pkg: SkillPackage, root: string, options: { force: boolean }): Promise<"installed" | "skipped"> {
    const skillName = safeSkillName(pkg.name);
    const skillDir = join(root, skillName);
    const marker = join(skillDir, ".brainstack-skill-package.json");
    const existing = await lstat(skillDir).catch(() => null);
    if (existing) {
      if (!existing.isDirectory() || existing.isSymbolicLink()) {
        throw new Error(`refusing to overwrite non-directory skill target: ${skillDir}`);
      }
      if (!options.force && !existsSync(marker)) {
        return "skipped";
      }
      await rm(skillDir, { recursive: true, force: true });
    }
    for (const file of pkg.files) {
      const relPath = validateSkillRelativePath(file.path);
      const target = join(skillDir, relPath);
      const targetRoot = resolve(skillDir);
      const targetAbs = resolve(target);
      if (targetAbs !== targetRoot && !targetAbs.startsWith(`${targetRoot}${sep}`)) {
        throw new Error(`skill file escapes target root: ${file.path}`);
      }
      await ensureDir(dirname(targetAbs));
      if (file.encoding === "utf8") {
        await writeText(targetAbs, file.content, 0o600);
      } else {
        const bytes = Buffer.from(file.content, "base64");
        await writeFile(targetAbs, bytes);
        await chmod(targetAbs, 0o600);
      }
    }
    await writeText(marker, `${JSON.stringify({ name: pkg.name, package_sha256: pkg.package_sha256, installed_at: new Date().toISOString() }, null, 2)}\n`, 0o600);
    return "installed";
  }

  async function refreshBrainstackSkillPackages(
    cfg: BrainstackConfig,
    args: ParsedArgs,
    options: { quiet?: boolean; hookMode?: boolean } = {}
  ): Promise<RefreshSkillResult> {
    const target = normalizeHookTarget(requireFlagValue(args, "target"), normalizeHookTarget(requireFlagValue(args, "harness"), "codex"));
    const root = skillInstallRootForTarget(target, args);
    const repo = absWithHome(requireFlagValue(args, "repo") || cfg.client.localPath || "~/shared-brain", cfg.paths.home);
    const warnings: string[] = [];
    if (!hasFlag(args, "no-sync") && existsSync(join(repo, ".git"))) {
      const pull = run(["git", "pull", "--ff-only"], { cwd: repo, check: false, timeoutMs: options.hookMode ? 1500 : 20_000 });
      if (pull.code !== 0) {
        warnings.push(`git pull skipped/failed: ${(pull.stderr || pull.stdout).replace(/\s+/g, " ").slice(0, 200)}`);
      }
    }
    const selected = new Set(flagValues(args, "skill").map(safeSkillName));
    const packages = chooseLatestSkillPackages(await discoverSkillPackages(repo)).filter((entry) => !selected.size || selected.has(entry.package.name));
    const installed: string[] = [];
    const skipped: string[] = [];
    for (const entry of packages) {
      const result = await installSkillPackage(entry.package, root, { force: hasFlag(args, "force") });
      if (result === "installed") {
        installed.push(entry.package.name);
      } else {
        skipped.push(entry.package.name);
      }
    }
    const output = { repo, target, root, installed, skipped, warnings };
    if (!options.quiet) {
      console.log(`skills_refresh target=${target} root=${root} repo=${repo}`);
      console.log(`installed=${installed.length}${installed.length ? ` ${installed.join(",")}` : ""}`);
      console.log(`skipped=${skipped.length}${skipped.length ? ` ${skipped.join(",")}` : ""}`);
      for (const warning of warnings) {
        console.warn(`WARN ${warning}`);
      }
    }
    return output;
  }

  async function skillDoctorRoots(target: HookTarget, args: ParsedArgs): Promise<string[]> {
    const explicit = requireFlagValue(args, "dir");
    if (explicit) {
      return [abs(explicit)];
    }
    return [skillInstallRootForTarget(target, args)];
  }

  async function skillDirsForDoctor(root: string): Promise<string[]> {
    if (!existsSync(root)) {
      return [];
    }
    if (existsSync(join(root, "SKILL.md"))) {
      return [root];
    }
    const dirs: string[] = [];
    for (const entry of await readdir(root, { withFileTypes: true })) {
      if (entry.isDirectory() && existsSync(join(root, entry.name, "SKILL.md"))) {
        dirs.push(join(root, entry.name));
      }
    }
    return dirs.sort();
  }

  function gitRemoteHeadCheck(skillDir: string, remote: string, branch: string): string {
    const head = gitOutput(["rev-parse", "HEAD"], skillDir);
    if (!head || !remote || !branch || branch === "HEAD") {
      return "remote check unavailable";
    }
    const result = run(["git", ...safeGitProtocolArgs(remote), "ls-remote", remote, `refs/heads/${branch}`], {
      cwd: skillDir,
      check: false,
      timeoutMs: 5000,
      env: safeGitProtocolEnv(remote)
    });
    if (result.code !== 0) {
      return `remote check failed: ${(result.stderr || result.stdout).replace(/\s+/g, " ").slice(0, 160)}`;
    }
    const remoteHead = result.stdout.trim().split(/\s+/)[0] || "";
    if (!remoteHead) {
      return `remote branch not found: ${branch}`;
    }
    return remoteHead === head ? "remote up to date" : `remote differs local=${head.slice(0, 12)} remote=${remoteHead.slice(0, 12)}`;
  }

  async function commandSkillsDoctor(args: ParsedArgs): Promise<void> {
    const target = normalizeHookTarget(requireFlagValue(args, "target"), "codex");
    const checks: SkillDoctorCheck[] = [];
    for (const root of await skillDoctorRoots(target, args)) {
      if (!existsSync(root)) {
        checks.push({ status: "WARN", skill: "(root)", path: root, detail: "skill root missing" });
        continue;
      }
      for (const skillDir of await skillDirsForDoctor(root)) {
        const skillPath = join(skillDir, "SKILL.md");
        const info = await lstat(skillPath).catch(() => null);
        if (!info || info.isSymbolicLink() || !info.isFile()) {
          checks.push({ status: "FAIL", skill: basename(skillDir), path: skillPath, detail: "SKILL.md missing or not a regular file" });
          continue;
        }
        const text = await readFile(skillPath, "utf8");
        let metadata: { name: string; description?: string };
        try {
          metadata = parseSkillMetadata(text, basename(skillDir));
        } catch (error) {
          checks.push({ status: "FAIL", skill: basename(skillDir), path: skillPath, detail: error instanceof Error ? error.message : String(error) });
          continue;
        }
        const git = await localSkillGitProvenance(skillDir);
        const dirty = git?.dirty ? " dirty" : "";
        let remoteDetail = "";
        if (hasFlag(args, "check-remote") && git?.remote && git?.branch) {
          remoteDetail = `; ${gitRemoteHeadCheck(skillDir, String(git.remote), String(git.branch))}`;
        }
        checks.push({
          status: git?.dirty ? "WARN" : "PASS",
          skill: metadata.name,
          path: skillDir,
          detail: `${metadata.description || "no description"}${dirty}${git?.remote ? `; remote=${git.remote}` : ""}${git?.commit ? `; commit=${String(git.commit).slice(0, 12)}` : ""}${remoteDetail}`
        });
      }
    }
    if (hasFlag(args, "json")) {
      console.log(JSON.stringify({ ok: !checks.some((item) => item.status === "FAIL"), checks }, null, 2));
    } else {
      console.log(
        checks.length
          ? checks.map((item) => `${item.status} ${item.skill}: ${item.detail} (${item.path})`).join("\n")
          : "WARN (root): no skills found"
      );
    }
    if (checks.some((item) => item.status === "FAIL")) {
      throw new Error("skills doctor found failing checks");
    }
  }

  function hookConfigPath(target: HookTarget): string {
    const home = process.env.HOME ? abs(process.env.HOME) : ".";
    if (target === "codex") {
      return join(home, ".codex", "hooks.json");
    }
    if (target === "claude") {
      return join(home, ".claude", "settings.json");
    }
    return join(home, ".cursor", "hooks.json");
  }

  function isTransientBrainctlPath(path: string): boolean {
    const normalized = abs(path);
    const tmp = abs(tmpdir());
    const home = process.env.HOME ? abs(process.env.HOME) : "";
    const underHome = Boolean(home && (normalized === home || normalized.startsWith(`${home}/`)));
    return normalized.startsWith("/Volumes/")
      || (normalized.startsWith(`${tmp}/`) && !underHome)
      || normalized.includes("/.build/")
      || normalized.includes(".app/Contents/Resources/");
  }

  function shellCommandForBrainctlPath(pathOrName: string, label: string): string {
    const value = pathOrName.trim();
    if (!value) {
      throw new Error(`${label} must not be empty`);
    }
    const pathLike = value.includes("/") || value.startsWith(".") || value.startsWith("~");
    if (!pathLike) {
      if (/\s/.test(value)) {
        throw new Error(`${label} now accepts an executable path or command name only; use --brainctl-command for raw shell snippets`);
      }
      return quoteForBash(value);
    }
    const resolved = abs(value);
    if (isTransientBrainctlPath(resolved)) {
      throw new Error(`${label} points at a transient path (${resolved}); install brainctl to a stable path such as ~/.local/bin/brainctl and pass that path`);
    }
    return quoteForBash(resolved);
  }

  function currentBrainctlHookCommand(args: ParsedArgs): string {
    const rawCommand = requireFlagValue(args, "brainctl-command");
    const explicit = requireFlagValue(args, "brainctl");
    if (rawCommand && explicit) {
      throw new Error("use either --brainctl or --brainctl-command, not both");
    }
    if (rawCommand) {
      return rawCommand;
    }
    if (explicit) {
      return shellCommandForBrainctlPath(explicit, "--brainctl");
    }
    const executable = process.execPath;
    const script = process.argv[1];
    if (script && script.endsWith(".ts")) {
      return `${quoteForBash(executable)} --no-env-file run ${quoteForBash(abs(script))}`;
    }
    if (isTransientBrainctlPath(executable)) {
      throw new Error(`brainctl is running from a transient path (${executable}); rerun with --brainctl ~/.local/bin/brainctl or reinstall brainctl to a stable path before installing hooks or daemons`);
    }
    return quoteForBash(executable);
  }

  function managedHookCommand(brainctlCommand: string, target: HookTarget, event: string, configPath: string): string {
    return `${brainctlCommand} hook run --harness ${target} --event ${event} --config ${quoteForBash(configPath)}`;
  }

  function isManagedBrainstackHook(hook: unknown, target: HookTarget): boolean {
    if (!hook || typeof hook !== "object") {
      return false;
    }
    const record = hook as Record<string, unknown>;
    const command = typeof record.command === "string" ? record.command : "";
    const statusMessage = typeof record.statusMessage === "string" ? record.statusMessage : "";
    return command.includes(" hook run ") && command.includes(`--harness ${target}`) && (command.includes("brainctl") || statusMessage === BRAINSTACK_HOOK_STATUS_MESSAGE);
  }

  async function readJsonObject(path: string): Promise<Record<string, unknown>> {
    if (!existsSync(path)) {
      return {};
    }
    const text = await readFile(path, "utf8");
    const parsed = JSON.parse(text) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error(`JSON config must be an object: ${path}`);
    }
    return parsed as Record<string, unknown>;
  }

  function mergeCodexStyleHooks(raw: Record<string, unknown>, target: HookTarget, brainctlCommand: string, configPath: string): Record<string, unknown> {
    const hooks = raw.hooks && typeof raw.hooks === "object" && !Array.isArray(raw.hooks) ? (raw.hooks as Record<string, unknown>) : {};
    removeCodexStyleManagedHooks(hooks, target);
    const handler = (event: string) => ({
      type: "command",
      command: managedHookCommand(brainctlCommand, target, event, configPath),
      timeout: 5,
      statusMessage: BRAINSTACK_HOOK_STATUS_MESSAGE
    });
    const additions: Record<string, Array<Record<string, unknown>>> = {
      SessionStart: [{ matcher: "startup|resume|compact|clear", hooks: [handler("SessionStart")] }],
      UserPromptSubmit: [{ hooks: [handler("UserPromptSubmit")] }],
      Stop: [{ hooks: [handler("Stop")] }],
      PostCompact: [{ matcher: "manual|auto", hooks: [handler("PostCompact")] }]
    };
    for (const [event, groups] of Object.entries(additions)) {
      const existing = Array.isArray(hooks[event]) ? (hooks[event] as unknown[]) : [];
      hooks[event] = [...existing, ...groups];
    }
    return { ...raw, hooks };
  }

  function removeCodexStyleManagedHooks(hooks: Record<string, unknown>, target: HookTarget): number {
    let removed = 0;
    for (const [event, groupsRaw] of Object.entries(hooks)) {
      if (!Array.isArray(groupsRaw)) {
        continue;
      }
      const groups: unknown[] = [];
      for (const groupRaw of groupsRaw) {
        if (!groupRaw || typeof groupRaw !== "object") {
          groups.push(groupRaw);
          continue;
        }
        const group = { ...(groupRaw as Record<string, unknown>) };
        const hookList = Array.isArray(group.hooks) ? group.hooks : [];
        const kept = hookList.filter((hook) => {
          const managed = isManagedBrainstackHook(hook, target);
          if (managed) {
            removed += 1;
          }
          return !managed;
        });
        if (kept.length) {
          group.hooks = kept;
          groups.push(group);
        }
      }
      if (groups.length) {
        hooks[event] = groups;
      } else {
        delete hooks[event];
      }
    }
    return removed;
  }

  function countCodexStyleManagedHooks(raw: Record<string, unknown>, target: HookTarget): number {
    const hooks = raw.hooks && typeof raw.hooks === "object" && !Array.isArray(raw.hooks) ? (raw.hooks as Record<string, unknown>) : {};
    let count = 0;
    for (const groupsRaw of Object.values(hooks)) {
      if (!Array.isArray(groupsRaw)) {
        continue;
      }
      for (const groupRaw of groupsRaw) {
        if (!groupRaw || typeof groupRaw !== "object") {
          continue;
        }
        const hookList = Array.isArray((groupRaw as Record<string, unknown>).hooks) ? ((groupRaw as Record<string, unknown>).hooks as unknown[]) : [];
        count += hookList.filter((hook) => isManagedBrainstackHook(hook, target)).length;
      }
    }
    return count;
  }

  function mergeCursorHooks(raw: Record<string, unknown>, target: HookTarget, brainctlCommand: string, configPath: string): Record<string, unknown> {
    const hooks = raw.hooks && typeof raw.hooks === "object" && !Array.isArray(raw.hooks) ? (raw.hooks as Record<string, unknown>) : {};
    removeCursorManagedHooks(hooks, target);
    const handler = (event: string) => ({
      command: managedHookCommand(brainctlCommand, target, event, configPath),
      timeout: 5,
      statusMessage: BRAINSTACK_HOOK_STATUS_MESSAGE
    });
    hooks.beforeSubmitPrompt = [...(Array.isArray(hooks.beforeSubmitPrompt) ? (hooks.beforeSubmitPrompt as unknown[]) : []), handler("beforeSubmitPrompt")];
    hooks.stop = [...(Array.isArray(hooks.stop) ? (hooks.stop as unknown[]) : []), handler("stop")];
    return { version: 1, ...raw, hooks };
  }

  function removeCursorManagedHooks(hooks: Record<string, unknown>, target: HookTarget): number {
    let removed = 0;
    for (const [event, handlersRaw] of Object.entries(hooks)) {
      if (!Array.isArray(handlersRaw)) {
        continue;
      }
      const kept = handlersRaw.filter((hook) => {
        const managed = isManagedBrainstackHook(hook, target);
        if (managed) {
          removed += 1;
        }
        return !managed;
      });
      if (kept.length) {
        hooks[event] = kept;
      } else {
        delete hooks[event];
      }
    }
    return removed;
  }

  function countCursorManagedHooks(raw: Record<string, unknown>, target: HookTarget): number {
    const hooks = raw.hooks && typeof raw.hooks === "object" && !Array.isArray(raw.hooks) ? (raw.hooks as Record<string, unknown>) : {};
    return Object.values(hooks).reduce((sum, handlersRaw) => sum + (Array.isArray(handlersRaw) ? handlersRaw.filter((hook) => isManagedBrainstackHook(hook, target)).length : 0), 0);
  }

  async function commandHooks(args: ParsedArgs): Promise<void> {
    const subcommand = args.positional[0] || "status";
    const targets = hookTargetsFromArgs(args);
    const configPath = abs(requireFlagValue(args, "config") || brainstackDefaultConfigPath());
    const brainctlCommand = currentBrainctlHookCommand(args);
    let statusErrors = 0;
    for (const target of targets) {
      const path = hookConfigPath(target);
      if (subcommand === "status") {
        try {
          const raw = await readJsonObject(path);
          const count = target === "cursor" ? countCursorManagedHooks(raw, target) : countCodexStyleManagedHooks(raw, target);
          console.log(`${target}: ${count ? "installed" : "missing"} hooks=${count} path=${path}`);
        } catch (error) {
          statusErrors += 1;
          const message = error instanceof Error ? error.message : String(error);
          console.log(`${target}: error hooks=0 path=${path} error=${message}`);
        }
        continue;
      }
      if (subcommand === "install") {
        const raw = await readJsonObject(path);
        const next = target === "cursor" ? mergeCursorHooks(raw, target, brainctlCommand, configPath) : mergeCodexStyleHooks(raw, target, brainctlCommand, configPath);
        if (hasFlag(args, "dry-run")) {
          console.log(`${target}: dry-run install path=${path}`);
          console.log(JSON.stringify(next, null, 2));
        } else {
          await writeText(path, `${JSON.stringify(next, null, 2)}\n`, 0o600);
          console.log(`${target}: hooks installed path=${path}`);
        }
        continue;
      }
      if (subcommand === "remove") {
        const raw = await readJsonObject(path);
        const hooks = raw.hooks && typeof raw.hooks === "object" && !Array.isArray(raw.hooks) ? (raw.hooks as Record<string, unknown>) : {};
        const removed = target === "cursor" ? removeCursorManagedHooks(hooks, target) : removeCodexStyleManagedHooks(hooks, target);
        const next = { ...raw, hooks };
        if (hasFlag(args, "dry-run")) {
          console.log(`${target}: dry-run remove removed=${removed} path=${path}`);
        } else {
          await writeText(path, `${JSON.stringify(next, null, 2)}\n`, 0o600);
          console.log(`${target}: hooks removed=${removed} path=${path}`);
        }
        continue;
      }
      throw new Error("hooks subcommand must be install|status|remove");
    }
    if (subcommand === "status" && statusErrors > 0) {
      throw new Error(`hooks status found ${statusErrors} malformed config file(s)`);
    }
  }

  function hookStateRootFallback(): string {
    return process.env.HOME ? join(abs(process.env.HOME), ".local", "state", "brainstack") : join(tmpdir(), "brainstack");
  }

  async function appendHookEventLog(stateRoot: string, event: Record<string, unknown>): Promise<void> {
    const dir = join(stateRoot, "harness-events");
    await ensureDir(dir);
    await chmod(dir, 0o700).catch(() => undefined);
    const path = join(dir, `${new Date().toISOString().slice(0, 10)}.jsonl`);
    await appendFile(path, `${JSON.stringify(event)}\n`, { encoding: "utf8" });
    await chmod(path, 0o600).catch(() => undefined);
  }

  async function readHookStdinCapped(maxBytes = 256 * 1024): Promise<{ text: string; bytes: number; truncated: boolean }> {
    const reader = Bun.stdin.stream().getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    let kept = 0;
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) {
        break;
      }
      total += chunk.value.byteLength;
      if (kept < maxBytes) {
        const remaining = maxBytes - kept;
        const slice = chunk.value.byteLength > remaining ? chunk.value.slice(0, remaining) : chunk.value;
        chunks.push(slice);
        kept += slice.byteLength;
      }
    }
    return {
      text: new TextDecoder().decode(Buffer.concat(chunks)),
      bytes: total,
      truncated: total > maxBytes
    };
  }

  function summarizeHookInput(input: unknown, stdinInfo: { bytes: number; truncated: boolean }): Record<string, unknown> {
    const summary: Record<string, unknown> = {
      input_bytes: stdinInfo.bytes,
      input_truncated: stdinInfo.truncated
    };
    if (!input || typeof input !== "object" || Array.isArray(input)) {
      summary.kind = input === null ? "null" : Array.isArray(input) ? "array" : typeof input;
      return summary;
    }
    const record = input as Record<string, unknown>;
    summary.keys = Object.keys(record).slice(0, 50);
    for (const key of ["hook_event_name", "session_id", "turn_id", "cwd", "trigger", "source", "transcript_path"]) {
      const value = record[key];
      if (typeof value === "string" || typeof value === "number" || typeof value === "boolean" || value === null) {
        summary[key] = value;
      }
    }
    if (typeof record.prompt === "string") {
      summary.prompt_sha256 = sha256Hex(record.prompt);
      summary.prompt_bytes = new TextEncoder().encode(record.prompt).byteLength;
    }
    if (Array.isArray(record.attachments)) {
      summary.attachments_count = record.attachments.length;
    }
    if (typeof record.tool_name === "string") {
      summary.tool_name = record.tool_name;
    }
    return summary;
  }

  function hookInputString(input: unknown, key: string): string | undefined {
    if (!input || typeof input !== "object" || Array.isArray(input)) {
      return undefined;
    }
    const value = (input as Record<string, unknown>)[key];
    return typeof value === "string" && value.trim() ? value.trim() : undefined;
  }

  function sessionIdFromTranscriptPath(path: string): string | undefined {
    const match = basename(path).match(/([0-9a-f]{8,}-[0-9a-f-]{20,})/i);
    return match?.[1];
  }

  async function sha256File(path: string): Promise<string> {
    const hash = createHash("sha256");
    for await (const chunk of createReadStream(path)) {
      hash.update(chunk);
    }
    return hash.digest("hex");
  }

  function isDurableTranscriptPath(path: string, cfg: BrainstackConfig): boolean {
    const normalized = abs(path);
    const tempRoot = abs(tmpdir());
    const home = abs(cfg.paths.home);
    return !normalized.startsWith("/Volumes/")
      && !normalized.includes(".app/Contents/")
      && !(normalized.startsWith(`${tempRoot}/`) && !normalized.startsWith(`${home}/`));
  }

  async function hookSessionCheckpointPayload(
    cfg: BrainstackConfig,
    target: HookTarget,
    event: string,
    input: unknown
  ): Promise<Record<string, unknown> | null> {
    const transcriptPathRaw = hookInputString(input, "transcript_path");
    if (!transcriptPathRaw) {
      return null;
    }
    const transcriptPath = abs(expandHome(transcriptPathRaw));
    const info = await lstat(transcriptPath).catch(() => null);
    if (!info?.isFile() || info.isSymbolicLink()) {
      return null;
    }
    const sessionId = hookInputString(input, "session_id") || sessionIdFromTranscriptPath(transcriptPath) || sha256Hex(transcriptPath).slice(0, 16);
    const cwd = hookInputString(input, "cwd") || process.cwd();
    const transcriptSha256 = await sha256File(transcriptPath);
    const transcriptPathDurable = isDurableTranscriptPath(transcriptPath, cfg);
    const title = `Codex session checkpoint: ${sessionId}`;
    const text = [
      `# ${title}`,
      "",
      `- Session id: \`${sessionId}\``,
      `- Harness: \`${target}\``,
      `- Hook event: \`${event}\``,
      `- Cwd: \`${cwd}\``,
      `- Local transcript path: \`${transcriptPath}\``,
      `- Transcript sha256: \`${transcriptSha256}\``,
      `- Transcript path durable: \`${transcriptPathDurable ? "true" : "false"}\``,
      "",
      "## Capture Note",
      "",
      "This is a hook-created checkpoint, not a transcript import. Use `brainctl import codex-session` from the machine that owns the transcript when full raw evidence is needed.",
      ""
    ].join("\n");
    return {
      title,
      text,
      source_harness: target,
      source_machine: cfg.machine.name,
      source_type: "codex-session-checkpoint",
      conversation_id: sessionId,
      related_repo: cwd,
      transcript_path: transcriptPath,
      transcript_path_durable: transcriptPathDurable,
      transcript_bytes: info.size,
      transcript_sha256: transcriptSha256,
      tags: uniqueNonEmptyStrings(["codex-session", "codex-session-checkpoint", `session:${sessionId}`, `repo:${deriveProjectLabel(cwd)}`])
    };
  }

  async function commandHook(args: ParsedArgs): Promise<void> {
    const subcommand = args.positional[0] || "help";
    if (subcommand !== "run") {
      if (subcommand === "help" || subcommand === "--help" || subcommand === "-h") {
        console.log("Usage: brainctl hook run --harness codex|claude|cursor --event EVENT [--config brainstack.yaml]");
        return;
      }
      throw new Error(`Unknown hook subcommand: ${subcommand}`);
    }
    const target = normalizeHookTarget(requireFlagValue(args, "harness"), "codex");
    const event = requireFlagValue(args, "event") || "unknown";
    const stdinInfo = await readHookStdinCapped().catch(() => ({ text: "", bytes: 0, truncated: false }));
    const stdin = stdinInfo.text;
    let parsedInput: unknown = null;
    try {
      parsedInput = stdin ? JSON.parse(stdin) : null;
    } catch {
      parsedInput = { raw_sha256: sha256Hex(stdin), raw_bytes: stdinInfo.bytes, raw_truncated: stdinInfo.truncated };
    }
    try {
      const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
      await appendHookEventLog(cfg.paths.stateRoot, {
        ts: new Date().toISOString(),
        harness: target,
        event,
        cwd: process.cwd(),
        input: summarizeHookInput(parsedInput, stdinInfo)
      });
      if (event === "SessionStart" || event === "UserPromptSubmit" || event === "beforeSubmitPrompt") {
        const daemonStatus = await readDaemonStatus(cfg);
        const daemonFresh = daemonStatusFresh(daemonStatus, 5 * 60_000);
        await appendHookEventLog(cfg.paths.stateRoot, {
          ts: new Date().toISOString(),
          harness: target,
          event: "daemon-status",
          fresh: daemonFresh,
          updated_at: daemonStatus?.updated_at || null,
          ok: daemonStatus?.ok ?? null
        });
        if (!daemonFresh) {
          const unsafeReason = await localSkillRefreshUnsafeReason(cfg);
          if (unsafeReason) {
            await appendHookEventLog(cfg.paths.stateRoot, {
              ts: new Date().toISOString(),
              harness: target,
              event: "refresh-skipped",
              reason: unsafeReason
            });
          } else {
            await refreshBrainstackSkillPackages(
              cfg,
              { ...args, flags: { ...args.flags, target, "no-sync": true, quiet: true } },
              { quiet: true, hookMode: true }
            ).catch(async (error) => {
              await appendHookEventLog(cfg.paths.stateRoot, {
                ts: new Date().toISOString(),
                harness: target,
                event: "refresh-error",
                error: error instanceof Error ? error.message : String(error)
              });
            });
          }
        }
      }
      if (event === "Stop" || event === "stop") {
        const payload = await hookSessionCheckpointPayload(cfg, target, event, parsedInput);
        if (payload) {
          const queuedPath = await queueBrainWriteForBackgroundFlush(cfg, "import", payload, "queued by harness hook for daemon/outbox flush");
          await appendHookEventLog(cfg.paths.stateRoot, {
            ts: new Date().toISOString(),
            harness: target,
            event: "session-checkpoint-queued",
            outbox: queuedPath,
            session_id: payload.conversation_id || null
          });
        } else {
          await appendHookEventLog(cfg.paths.stateRoot, {
            ts: new Date().toISOString(),
            harness: target,
            event: "session-checkpoint-skipped",
            reason: "no regular transcript_path in hook input"
          });
        }
      }
    } catch (error) {
      await appendHookEventLog(hookStateRootFallback(), {
        ts: new Date().toISOString(),
        harness: target,
        event: "hook-error",
        detail: error instanceof Error ? error.message : String(error)
      }).catch(() => undefined);
    }
    console.log("{}");
  }

  return {
    normalizeHookTarget,
    skillInstallRootForTarget,
    commandImportSkills,
    commandImport,
    refreshBrainstackSkillPackages,
    skillDirsForDoctor,
    commandSkillsDoctor,
    hookConfigPath,
    readJsonObject,
    countCodexStyleManagedHooks,
    countCursorManagedHooks,
    commandHooks,
    currentBrainctlHookCommand,
    commandHook
  };
}
