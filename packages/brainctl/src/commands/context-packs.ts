import { createHash } from "node:crypto";
import { createReadStream, existsSync, mkdirSync } from "node:fs";
import { chmod, lstat, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { basename, dirname, join, relative, sep } from "node:path";
import { tmpdir } from "node:os";
import type { ParsedArgs } from "../args";
import type { BrainstackConfig, BrainstackWorkerConfig } from "../config";
import type { run as runtimeRun } from "../runtime";

type RunResult = ReturnType<typeof runtimeRun>;

export interface BrainstackContextPackManifest {
  schema_version: 1;
  kind: "brainstack.context_pack";
  id: string;
  name: string;
  safe_name: string;
  machine: string;
  source_machine: string;
  source_root: string;
  pack_root: string;
  content_path: string;
  manifest_path: string;
  tree_path: string;
  include: string[];
  exclude: string[];
  file_count: number;
  total_bytes: number;
  largest_files: Array<{ path: string; size_bytes: number }>;
  excluded_count: number;
  sensitive_count: number;
  tree_sha256: string;
  previous_tree_sha256: string | null;
  changed_since_previous: boolean;
  freshness: "fresh" | "unknown";
  free_space_bytes: number | null;
  warnings: string[];
  refreshed_at: string;
}

export interface BrainstackContextPackAttachment {
  name: string;
  safe_name: string;
  machine: string;
  attached_at: string;
}

interface BrainstackContextPackDefinition {
  schema_version: 1;
  kind: "brainstack.context_pack_definition";
  id: string;
  name: string;
  safe_name: string;
  source_machine: string;
  source_root: string;
  target_machine: string;
  include: string[];
  exclude: string[];
  allow_sensitive: boolean;
  created_at: string;
  updated_at: string;
  last_tree_sha256: string | null;
  last_synced_at: string | null;
}

interface PackTreeEntry {
  path: string;
  size_bytes: number;
  mtime_ms: number;
  sha256: string;
}

interface PackScan {
  sourceRoot: string;
  entries: PackTreeEntry[];
  fileCount: number;
  totalBytes: number;
  largestFiles: Array<{ path: string; size_bytes: number }>;
  excludedCount: number;
  sensitiveCount: number;
  warnings: string[];
  treeSha256: string;
  treeJsonl: string;
}

type ContextPacksDeps = {
  abs: (input: string) => string;
  absWithHome: (input: string, home: string) => string;
  brainstackDefaultConfigPath: () => string;
  controlSshTarget: (
    cfg: BrainstackConfig,
    args: ParsedArgs,
    label: string,
    options?: { allowRemoteSshFallback?: boolean }
  ) => string | null;
  defaultWorkers: (cfg: BrainstackConfig) => BrainstackWorkerConfig[];
  flag: (args: ParsedArgs, name: string) => string | undefined;
  flagValues: (args: ParsedArgs, name: string) => string[];
  hasFlag: (args: ParsedArgs, name: string) => boolean;
  loadConfig: (path?: string | null, profile?: string | null, root?: string | null) => Promise<BrainstackConfig>;
  parsePositiveIntegerFlag: (args: ParsedArgs, name: string, fallback: number) => number;
  quoteForBash: (value: string) => string;
  remoteBrainctlScript: (remoteRepo: string, argv: string[], options?: { preferInstalledBinary?: boolean }) => string;
  requireFlagValue: (args: ParsedArgs, name: string) => string | undefined;
  run: typeof runtimeRun;
  runControlRemoteScript: (
    cfg: BrainstackConfig,
    args: ParsedArgs,
    label: string,
    remoteScript: string,
    timeoutMs: number,
    options?: { allowRemoteSshFallback?: boolean }
  ) => RunResult | null;
  runWorkerShell: (cfg: BrainstackConfig, worker: BrainstackWorkerConfig, script: string, timeoutSeconds?: number, usePathCache?: boolean) => RunResult;
  telegramControlWorker: (target: string) => BrainstackWorkerConfig;
  telegramKnownHostsPath: (cfg: BrainstackConfig, args: ParsedArgs) => string;
  telegramSshTrustArgs: (mode: "pinned" | "accept-new" | "default", knownHostsPath: string) => string[];
  telegramSshTrustMode: (args: ParsedArgs) => "pinned" | "accept-new" | "default";
  workerRemoteTarget: (worker: BrainstackWorkerConfig) => string;
  workerSshPortArgs: (worker: BrainstackWorkerConfig) => string[];
  workerSshTrustArgs: (cfg: BrainstackConfig, worker: BrainstackWorkerConfig) => string[];
  whichCommand: (name: string) => string | null;
};

const CONTEXT_PACK_SCHEMA_VERSION = 1;
const DEFAULT_SYNC_TIMEOUT_MS = 60 * 60_000;
const LARGE_PACK_BYTES = 5 * 1024 * 1024 * 1024;
const LARGE_PACK_FILES = 100_000;

const DEFAULT_EXCLUDE_SEGMENTS = new Set([
  ".git",
  ".hg",
  ".svn",
  ".ssh",
  ".config",
  "node_modules",
  ".venv",
  "venv",
  "dist",
  "build",
  ".next",
  ".cache",
  "target"
]);

const SENSITIVE_NAME_RE =
  /(^|[/\\])(?:\.env(?:\..*)?|id_rsa(?:\..*)?|id_dsa(?:\..*)?|id_ed25519(?:\..*)?|.*\.(?:pem|key|p8|p12|mobileprovision)|.*(?:secret|token|passwd|password|credential|cookie|session).*)$/i;

function compactError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function contextPacksRoot(cfg: BrainstackConfig): string {
  return join(cfg.paths.stateRoot, "context-packs");
}

function definitionsRoot(cfg: BrainstackConfig): string {
  return join(contextPacksRoot(cfg), "definitions");
}

function attachmentPath(cfg: BrainstackConfig, contextSlug: string): string {
  return join(cfg.telemux.controlRoot, "contexts", safeContextSlug(contextSlug), "context-packs.json");
}

function remoteContextPacksRoot(): string {
  return "~/.local/state/brainstack/context-packs";
}

function safeContextSlug(input: string): string {
  const value = input.trim();
  if (!/^[A-Za-z0-9._-]{1,120}$/.test(value)) {
    throw new Error(`invalid context slug: ${input}`);
  }
  return value;
}

function safePackName(input: string): string {
  const safe = input
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
  if (!safe || !/^[a-z0-9][a-z0-9._-]{0,79}$/.test(safe)) {
    throw new Error(`invalid context pack name: ${input}`);
  }
  return safe;
}

function packId(sourceMachine: string, name: string, sourceRoot: string): string {
  return `cp_${safePackName(name)}_${createHash("sha256").update(`${sourceMachine}\0${name}\0${sourceRoot}`).digest("hex").slice(0, 10)}`;
}

function localMachineAliases(cfg: BrainstackConfig): Set<string> {
  const aliases = new Set([cfg.machine.name, "local"]);
  if (cfg.profile === "control" || cfg.profile === "single-node") aliases.add("control");
  if (cfg.profile === "worker") aliases.add("worker");
  if (cfg.profile === "client-macos") aliases.add("client");
  return aliases;
}

function isLocalMachine(cfg: BrainstackConfig, machine: string): boolean {
  const normalized = machine.trim().toLowerCase();
  return [...localMachineAliases(cfg)].some((alias) => alias.toLowerCase() === normalized);
}

function findWorker(cfg: BrainstackConfig, machine: string, deps: ContextPacksDeps): BrainstackWorkerConfig | null {
  const normalized = machine.toLowerCase();
  return (
    deps.defaultWorkers(cfg).find((worker) => {
      const names = [worker.name, worker.sshTarget || "", deps.workerRemoteTarget(worker)].filter(Boolean).map((value) => value.toLowerCase());
      return names.includes(normalized);
    }) || null
  );
}

function machineFromArgs(args: ParsedArgs, cfg: BrainstackConfig): string {
  const raw = depsMachineFlag(args);
  return raw || cfg.machine.name;
}

function depsMachineFlag(args: ParsedArgs): string | null {
  return (args.flags.machine === true ? "" : typeof args.flags.machine === "string" ? args.flags.machine : undefined)?.trim() || null;
}

function expandLocalPath(input: string, cfg: BrainstackConfig, deps: ContextPacksDeps): string {
  return deps.absWithHome(input, cfg.paths.home);
}

function definitionPath(cfg: BrainstackConfig, safeName: string): string {
  return join(definitionsRoot(cfg), `${safeName}.json`);
}

function localPackRoot(cfg: BrainstackConfig, safeName: string): string {
  return join(contextPacksRoot(cfg), safeName);
}

function localContentPath(cfg: BrainstackConfig, safeName: string): string {
  return join(localPackRoot(cfg, safeName), "current");
}

function remotePackRoot(safeName: string): string {
  return `${remoteContextPacksRoot()}/${safeName}`;
}

function remoteContentPath(safeName: string): string {
  return `${remotePackRoot(safeName)}/current`;
}

async function writePrivateJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  await chmod(path, 0o600);
}

async function writePrivateText(path: string, text: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  await writeFile(path, text, { mode: 0o600 });
  await chmod(path, 0o600);
}

async function readDefinition(cfg: BrainstackConfig, safeName: string): Promise<BrainstackContextPackDefinition | null> {
  const path = definitionPath(cfg, safeName);
  if (!existsSync(path)) return null;
  return JSON.parse(await readFile(path, "utf8")) as BrainstackContextPackDefinition;
}

async function writeDefinition(cfg: BrainstackConfig, definition: BrainstackContextPackDefinition): Promise<void> {
  await writePrivateJson(definitionPath(cfg, definition.safe_name), definition);
}

function relPath(root: string, child: string): string {
  const rel = relative(root, child).split(sep).join("/");
  if (!rel || rel.startsWith("../") || rel === ".." || rel.startsWith("/") || /[\0\r\n]/.test(rel)) {
    throw new Error(`invalid pack-relative path: ${child}`);
  }
  return rel;
}

function hashFile(path: string): Promise<string> {
  return new Promise((resolveHash, reject) => {
    const hash = createHash("sha256");
    const stream = createReadStream(path);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolveHash(hash.digest("hex")));
  });
}

function globToRegExp(pattern: string): RegExp {
  const normalized = pattern.split(sep).join("/");
  let output = "^";
  for (let index = 0; index < normalized.length; index += 1) {
    const char = normalized[index] || "";
    const next = normalized[index + 1] || "";
    if (char === "*" && next === "*") {
      output += ".*";
      index += 1;
    } else if (char === "*") {
      output += "[^/]*";
    } else if (char === "?") {
      output += "[^/]";
    } else {
      output += char.replace(/[|\\{}()[\]^$+?.]/g, "\\$&");
    }
  }
  output += "$";
  return new RegExp(output);
}

function matchesAny(rel: string, patterns: RegExp[]): boolean {
  return patterns.some((pattern) => pattern.test(rel));
}

function isDefaultExcluded(rel: string): boolean {
  const parts = rel.split("/");
  if (parts.some((part) => DEFAULT_EXCLUDE_SEGMENTS.has(part))) {
    return true;
  }
  return parts[parts.length - 1] === ".DS_Store";
}

async function scanPackSource(
  sourceRoot: string,
  options: { include: string[]; exclude: string[]; allowSensitive: boolean }
): Promise<PackScan> {
  const rootInfo = await lstat(sourceRoot);
  if (rootInfo.isSymbolicLink() || !rootInfo.isDirectory()) {
    throw new Error(`context pack source must be a non-symlink directory: ${sourceRoot}`);
  }

  const includePatterns = options.include.map(globToRegExp);
  const excludePatterns = options.exclude.map(globToRegExp);
  const entries: PackTreeEntry[] = [];
  let excludedCount = 0;
  let sensitiveCount = 0;
  const warnings: string[] = [];

  async function walk(path: string): Promise<void> {
    const children = await readdir(path, { withFileTypes: true });
    for (const child of children) {
      const childPath = join(path, child.name);
      const rel = relPath(sourceRoot, childPath);
      const info = await lstat(childPath);
      if (info.isSymbolicLink()) {
        throw new Error(`context pack source contains a symlink; remove it or exclude it: ${rel}`);
      }
      if (isDefaultExcluded(rel) || matchesAny(rel, excludePatterns)) {
        excludedCount += 1;
        continue;
      }
      const sensitive = SENSITIVE_NAME_RE.test(rel);
      if (sensitive && !options.allowSensitive) {
        sensitiveCount += 1;
        excludedCount += 1;
        continue;
      }
      if (info.isDirectory()) {
        await walk(childPath);
        continue;
      }
      if (!info.isFile()) {
        throw new Error(`context pack source contains a special file; remove it or exclude it: ${rel}`);
      }
      if (includePatterns.length && !matchesAny(rel, includePatterns)) {
        excludedCount += 1;
        continue;
      }
      entries.push({
        path: rel,
        size_bytes: info.size,
        mtime_ms: Math.trunc(info.mtimeMs),
        sha256: await hashFile(childPath)
      });
    }
  }

  await walk(sourceRoot);
  entries.sort((a, b) => a.path.localeCompare(b.path));
  const totalBytes = entries.reduce((sum, entry) => sum + entry.size_bytes, 0);
  const largestFiles = [...entries]
    .sort((a, b) => b.size_bytes - a.size_bytes)
    .slice(0, 5)
    .map((entry) => ({ path: entry.path, size_bytes: entry.size_bytes }));
  if (totalBytes >= LARGE_PACK_BYTES) {
    warnings.push(`large pack: ${formatBytes(totalBytes)}; first sync can take a while`);
  }
  if (entries.length >= LARGE_PACK_FILES) {
    warnings.push(`many files: ${entries.length}; first sync can take a while`);
  }
  if (sensitiveCount > 0) {
    warnings.push(`${sensitiveCount} sensitive-looking file(s) were skipped; use single-file uploads for secrets or rerun with --allow-sensitive`);
  }
  if (!entries.length) {
    warnings.push("pack has no included files after exclusions");
  }
  const treeJsonl = entries.map((entry) => JSON.stringify(entry)).join("\n") + (entries.length ? "\n" : "");
  const treeSha256 = createHash("sha256").update(treeJsonl).digest("hex");
  return {
    sourceRoot,
    entries,
    fileCount: entries.length,
    totalBytes,
    largestFiles,
    excludedCount,
    sensitiveCount,
    warnings,
    treeSha256,
    treeJsonl
  };
}

function rsyncFilterEscape(path: string): string {
  return path.replace(/[\\*?\[\]]/g, (char) => `\\${char}`);
}

function rsyncExactFilter(scan: PackScan): string {
  const dirs = new Set<string>();
  for (const entry of scan.entries) {
    const parts = entry.path.split("/");
    for (let index = 1; index < parts.length; index += 1) {
      dirs.add(parts.slice(0, index).join("/"));
    }
  }
  const lines: string[] = [];
  for (const dir of [...dirs].sort((a, b) => a.localeCompare(b))) {
    lines.push(`+ /${rsyncFilterEscape(dir)}/`);
  }
  for (const entry of scan.entries) {
    lines.push(`+ /${rsyncFilterEscape(entry.path)}`);
  }
  lines.push("- *");
  return `${lines.join("\n")}\n`;
}

async function withRsyncExactFilter<T>(scan: PackScan, fn: (filterFile: string) => T | Promise<T>): Promise<T> {
  const dir = await mkdtemp(join(tmpdir(), "brainstack-context-pack-filter-"));
  const filterFile = join(dir, "filter");
  try {
    await writeFile(filterFile, rsyncExactFilter(scan), { mode: 0o600 });
    await chmod(filterFile, 0o600);
    return await fn(filterFile);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

function rsyncBaseArgs(deps: ContextPacksDeps): string[] {
  const rsync = deps.whichCommand("rsync");
  if (!rsync) {
    throw new Error("context packs require rsync. Install rsync and retry.");
  }
  return [
    rsync,
    "-rt",
    "--delete",
    "--delete-excluded",
    "--prune-empty-dirs",
    "--chmod=Du=rwx,Dgo=,Fu=rw,Fgo=",
    "--checksum"
  ];
}

function runChecked(result: RunResult, label: string): void {
  if (result.code !== 0 || result.timedOut) {
    throw new Error(`${label} failed${result.timedOut ? " (timed out)" : ` (exit ${result.code})`}\n${result.stderr || result.stdout}`);
  }
}

function workerShellArgs(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, script: string, deps: ContextPacksDeps): string[] {
  if (worker.transport === "tailscale-ssh") {
    return [deps.whichCommand("tailscale") || "tailscale", "ssh", deps.workerRemoteTarget(worker), "bash", "-lc", script];
  }
  return [
    deps.whichCommand("ssh") || "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=8",
    ...deps.workerSshTrustArgs(cfg, worker),
    ...deps.workerSshPortArgs(worker),
    deps.workerRemoteTarget(worker),
    `bash -lc ${deps.quoteForBash(script)}`
  ];
}

function workerRsyncShell(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, deps: ContextPacksDeps): string {
  if (worker.transport === "tailscale-ssh") {
    return [deps.whichCommand("tailscale") || "tailscale", "ssh"].map(deps.quoteForBash).join(" ");
  }
  return [
    deps.whichCommand("ssh") || "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=8",
    ...deps.workerSshTrustArgs(cfg, worker),
    ...deps.workerSshPortArgs(worker)
  ]
    .map(deps.quoteForBash)
    .join(" ");
}

function controlRsyncTarget(cfg: BrainstackConfig, args: ParsedArgs, deps: ContextPacksDeps): { worker: BrainstackWorkerConfig; shell: string } {
  const via = deps.controlSshTarget(cfg, args, "context packs control SSH target", { allowRemoteSshFallback: true });
  if (!via) {
    throw new Error("context packs need a control SSH target for client-macos remote sync");
  }
  const worker = deps.telegramControlWorker(via);
  const knownHostsPath = deps.telegramKnownHostsPath(cfg, args);
  const sshTrustMode = deps.telegramSshTrustMode(args);
  if (sshTrustMode === "accept-new") {
    mkdirSync(dirname(knownHostsPath), { recursive: true });
  }
  const shell = [
    deps.whichCommand("ssh") || "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=8",
    ...deps.telegramSshTrustArgs(sshTrustMode, knownHostsPath),
    ...deps.workerSshPortArgs(worker)
  ]
    .map(deps.quoteForBash)
    .join(" ");
  return { worker, shell };
}

function controlShellArgs(cfg: BrainstackConfig, args: ParsedArgs, script: string, deps: ContextPacksDeps): string[] {
  const via = deps.controlSshTarget(cfg, args, "context packs control SSH target", { allowRemoteSshFallback: true });
  if (!via) {
    throw new Error("context packs need a control SSH target for client-macos remote sync");
  }
  const worker = deps.telegramControlWorker(via);
  const knownHostsPath = deps.telegramKnownHostsPath(cfg, args);
  const sshTrustMode = deps.telegramSshTrustMode(args);
  if (sshTrustMode === "accept-new") {
    mkdirSync(dirname(knownHostsPath), { recursive: true });
  }
  return [
    deps.whichCommand("ssh") || "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=8",
    ...deps.telegramSshTrustArgs(sshTrustMode, knownHostsPath),
    ...deps.workerSshPortArgs(worker),
    deps.workerRemoteTarget(worker),
    `bash -lc ${deps.quoteForBash(script)}`
  ];
}

function remoteWriteTextScript(remotePath: string): string {
  return [
    "set -euo pipefail",
    "umask 077",
    "brainstack_expand_home() {",
    '  case "$1" in',
    '    \\~) printf \'%s\\n\' "$HOME" ;;',
    '    \\~/*) printf \'%s/%s\\n\' "$HOME" "${1#\\~/}" ;;',
    '    *) printf \'%s\\n\' "$1" ;;',
    "  esac",
    "}",
    `file="$(brainstack_expand_home ${remotePath})"`,
    'mkdir -p "$(dirname "$file")"',
    'tmp="$file.tmp.$$"',
    'cat > "$tmp"',
    'chmod 600 "$tmp"',
    'mv -f "$tmp" "$file"'
  ].join("\n");
}

async function writeRemoteText(
  cfg: BrainstackConfig,
  worker: BrainstackWorkerConfig,
  remotePath: string,
  text: string,
  deps: ContextPacksDeps,
  timeoutMs: number
): Promise<void> {
  const dir = await mkdtemp(join(tmpdir(), "brainstack-context-pack-"));
  const file = join(dir, "payload");
  try {
    await writeFile(file, text, { mode: 0o600 });
    await chmod(file, 0o600);
    const proc = Bun.spawn(workerShellArgs(cfg, worker, remoteWriteTextScript(deps.quoteForBash(remotePath)), deps), {
      stdin: Bun.file(file),
      stdout: "pipe",
      stderr: "pipe",
      timeout: timeoutMs
    });
    const [code, stdout, stderr] = await Promise.all([proc.exited, new Response(proc.stdout).text(), new Response(proc.stderr).text()]);
    if (code !== 0) {
      throw new Error(`write remote context pack metadata failed (exit ${code})\n${stderr || stdout}`);
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}

async function rsyncLocal(scan: PackScan, dest: string, timeoutMs: number, deps: ContextPacksDeps): Promise<void> {
  await mkdir(dest, { recursive: true, mode: 0o700 });
  await chmod(dest, 0o700);
  await withRsyncExactFilter(scan, async (filterFile) => {
    const args = [
      ...rsyncBaseArgs(deps),
      "--filter",
      `merge ${filterFile}`,
      `${scan.sourceRoot.replace(/\/+$/, "")}/`,
      `${dest.replace(/\/+$/, "")}/`
    ];
    runChecked(deps.run(args, { check: false, timeoutMs }), "context pack local rsync");
  });
}

async function rsyncRemote(
  cfg: BrainstackConfig,
  worker: BrainstackWorkerConfig,
  scan: PackScan,
  remoteDest: string,
  timeoutMs: number,
  deps: ContextPacksDeps
): Promise<void> {
  runChecked(
    deps.run(workerShellArgs(cfg, worker, `mkdir -p ${deps.quoteForBash(remoteDest)} && chmod 700 ${deps.quoteForBash(remoteDest)}`, deps), {
      check: false,
      timeoutMs: 30_000
    }),
    `prepare context pack directory on ${worker.name}`
  );
  await withRsyncExactFilter(scan, async (filterFile) => {
    const args = [
      ...rsyncBaseArgs(deps),
      "-e",
      workerRsyncShell(cfg, worker, deps),
      "--filter",
      `merge ${filterFile}`,
      `${scan.sourceRoot.replace(/\/+$/, "")}/`,
      `${deps.workerRemoteTarget(worker)}:${remoteDest.replace(/\/+$/, "")}/`
    ];
    runChecked(deps.run(args, { check: false, timeoutMs }), `context pack rsync to ${worker.name}`);
  });
}

async function stageSourceOnControl(
  cfg: BrainstackConfig,
  args: ParsedArgs,
  safeName: string,
  scan: PackScan,
  timeoutMs: number,
  deps: ContextPacksDeps
): Promise<{ remoteSource: string; worker: BrainstackWorkerConfig }> {
  const { worker, shell } = controlRsyncTarget(cfg, args, deps);
  const remoteSource = `${remoteContextPacksRoot()}/relay/${safePackName(cfg.machine.name)}/${safeName}/source`;
  runChecked(
    deps.run(controlShellArgs(cfg, args, `mkdir -p ${deps.quoteForBash(remoteSource)} && chmod 700 ${deps.quoteForBash(remoteSource)}`, deps), {
      check: false,
      timeoutMs: 30_000
    }),
    "prepare control context pack relay"
  );
  await withRsyncExactFilter(scan, async (filterFile) => {
    const rsyncArgs = [
      ...rsyncBaseArgs(deps),
      "-e",
      shell,
      "--filter",
      `merge ${filterFile}`,
      `${scan.sourceRoot.replace(/\/+$/, "")}/`,
      `${deps.workerRemoteTarget(worker)}:${remoteSource.replace(/\/+$/, "")}/`
    ];
    runChecked(deps.run(rsyncArgs, { check: false, timeoutMs }), "stage context pack on control host");
  });
  return { remoteSource, worker };
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) return "unknown size";
  if (bytes >= 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GiB`;
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(bytes >= 10 * 1024 * 1024 ? 1 : 2)} MiB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(bytes >= 10 * 1024 ? 1 : 2)} KiB`;
  return `${bytes} bytes`;
}

function parseDfAvailableBytes(stdout: string): number | null {
  const lines = stdout.trim().split(/\r?\n/).filter(Boolean);
  const row = lines[lines.length - 1];
  if (!row) return null;
  const parts = row.trim().split(/\s+/);
  const availableKib = Number(parts[3]);
  if (!Number.isFinite(availableKib) || availableKib < 0) return null;
  return availableKib * 1024;
}

function freeSpaceWarning(totalBytes: number, freeBytes: number | null): string | null {
  if (freeBytes === null || totalBytes <= 0) return null;
  const requiredWithHeadroom = Math.ceil(totalBytes * 1.1);
  if (requiredWithHeadroom <= freeBytes) return null;
  return `destination may be low on space: pack is ${formatBytes(totalBytes)}, destination reports ${formatBytes(freeBytes)} free`;
}

function localFreeSpaceBytes(cfg: BrainstackConfig, deps: ContextPacksDeps): number | null {
  const df = deps.whichCommand("df") || "/bin/df";
  mkdirSync(cfg.paths.stateRoot, { recursive: true });
  const result = deps.run([df, "-Pk", cfg.paths.stateRoot], { check: false, timeoutMs: 10_000 });
  if (result.code !== 0 || result.timedOut) return null;
  return parseDfAvailableBytes(result.stdout);
}

function remoteFreeSpaceBytes(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, deps: ContextPacksDeps): number | null {
  const script = [
    "set -euo pipefail",
    'root="$HOME/.local/state/brainstack"',
    'mkdir -p "$root"',
    'df -Pk "$root"'
  ].join("\n");
  const result = deps.run(workerShellArgs(cfg, worker, script, deps), { check: false, timeoutMs: 15_000 });
  if (result.code !== 0 || result.timedOut) return null;
  return parseDfAvailableBytes(result.stdout);
}

function manifestFromScan(input: {
  cfg: BrainstackConfig;
  scan: PackScan;
  name: string;
  safeName: string;
  machine: string;
  sourceMachine: string;
  sourceRoot: string;
  packRoot: string;
  contentPath: string;
  previousTreeSha256: string | null;
  include: string[];
  exclude: string[];
  freeSpaceBytes: number | null;
  freshness: "fresh" | "unknown";
}): BrainstackContextPackManifest {
  const id = packId(input.sourceMachine, input.name, input.sourceRoot);
  const packRoot = input.packRoot;
  return {
    schema_version: CONTEXT_PACK_SCHEMA_VERSION,
    kind: "brainstack.context_pack",
    id,
    name: input.name,
    safe_name: input.safeName,
    machine: input.machine,
    source_machine: input.sourceMachine,
    source_root: input.sourceRoot,
    pack_root: packRoot,
    content_path: input.contentPath,
    manifest_path: `${packRoot}/manifest.json`,
    tree_path: `${packRoot}/tree.jsonl`,
    include: input.include,
    exclude: input.exclude,
    file_count: input.scan.fileCount,
    total_bytes: input.scan.totalBytes,
    largest_files: input.scan.largestFiles,
    excluded_count: input.scan.excludedCount,
    sensitive_count: input.scan.sensitiveCount,
    tree_sha256: input.scan.treeSha256,
    previous_tree_sha256: input.previousTreeSha256,
    changed_since_previous: input.previousTreeSha256 !== null && input.previousTreeSha256 !== input.scan.treeSha256,
    freshness: input.freshness,
    free_space_bytes: input.freeSpaceBytes,
    warnings: input.scan.warnings,
    refreshed_at: new Date().toISOString()
  };
}

function formatPack(manifest: BrainstackContextPackManifest): string {
  const stale = manifest.changed_since_previous ? "changed" : "synced";
  return [
    `${manifest.name} (${manifest.safe_name})`,
    `machine=${manifest.machine}`,
    `source=${manifest.source_machine}`,
    `files=${manifest.file_count}`,
    `size=${formatBytes(manifest.total_bytes)}`,
    `status=${stale}`,
    `path=${manifest.content_path}`,
    `synced=${manifest.refreshed_at}`
  ].join(" ");
}

function parseJsonLines(text: string): BrainstackContextPackManifest[] {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .flatMap((line) => {
      try {
        return [JSON.parse(line) as BrainstackContextPackManifest];
      } catch {
        return [];
      }
    });
}

function listRemotePacksScript(): string {
  return [
    "set -euo pipefail",
    'root="$HOME/.local/state/brainstack/context-packs"',
    '[ -d "$root" ] || exit 0',
    'find "$root" -mindepth 2 -maxdepth 2 -type f -name manifest.json -print0 | while IFS= read -r -d "" file; do',
    "  tr -d '\\r\\n' < \"$file\"",
    "  printf '\\n'",
    "done"
  ].join("\n");
}

function remoteGcScript(deleteCandidates: boolean, protectedNames: Set<string>): string {
  const protectedCases = [...protectedNames].map((name) => depsSafeCasePattern(name)).join("|");
  return [
    "set -euo pipefail",
    'root="$HOME/.local/state/brainstack/context-packs"',
    '[ -d "$root" ] || exit 0',
    'find "$root" -mindepth 1 -maxdepth 1 -type d ! -name definitions ! -name relay -print | while IFS= read -r dir; do',
    protectedCases ? `  case "$(basename "$dir")" in ${protectedCases}) continue ;; esac` : "  :",
    '  basename "$dir"',
    deleteCandidates ? '  rm -rf -- "$dir"' : "  :",
    "done"
  ].join("\n");
}

function depsSafeCasePattern(value: string): string {
  return value.replace(/([\\*?\[\]|)])/g, "\\$1");
}

async function readLocalPacks(root: string): Promise<BrainstackContextPackManifest[]> {
  if (!existsSync(root)) return [];
  const manifests: BrainstackContextPackManifest[] = [];
  const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    if (entry.name === "definitions" || entry.name === "relay") continue;
    const manifestPath = join(root, entry.name, "manifest.json");
    try {
      manifests.push(JSON.parse(await readFile(manifestPath, "utf8")) as BrainstackContextPackManifest);
    } catch {
      // malformed pack manifests are ignored here; doctor can grow strict checks later
    }
  }
  return manifests;
}

async function readAttachments(cfg: BrainstackConfig, contextSlug: string): Promise<BrainstackContextPackAttachment[]> {
  const path = attachmentPath(cfg, contextSlug);
  if (!existsSync(path)) return [];
  const parsed = JSON.parse(await readFile(path, "utf8")) as { packs?: BrainstackContextPackAttachment[] };
  return Array.isArray(parsed.packs) ? parsed.packs : [];
}

async function writeAttachments(cfg: BrainstackConfig, contextSlug: string, packs: BrainstackContextPackAttachment[]): Promise<void> {
  await writePrivateJson(attachmentPath(cfg, contextSlug), {
    schema_version: 1,
    kind: "brainstack.context_pack_attachments",
    context: contextSlug,
    updated_at: new Date().toISOString(),
    packs
  });
}

async function contextAttachmentFiles(cfg: BrainstackConfig): Promise<Array<{ context: string; path: string }>> {
  const root = join(cfg.telemux.controlRoot, "contexts");
  const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
  return entries
    .filter((entry) => entry.isDirectory())
    .map((entry) => ({ context: entry.name, path: join(root, entry.name, "context-packs.json") }))
    .filter((entry) => existsSync(entry.path));
}

async function attachedPackKeys(cfg: BrainstackConfig): Promise<Set<string>> {
  const keys = new Set<string>();
  for (const file of await contextAttachmentFiles(cfg)) {
    try {
      const parsed = JSON.parse(await readFile(file.path, "utf8")) as { packs?: BrainstackContextPackAttachment[] };
      for (const pack of Array.isArray(parsed.packs) ? parsed.packs : []) {
        keys.add(`${pack.machine}:${pack.safe_name}`);
      }
    } catch {
      // malformed attachment files should not make gc delete aggressively
    }
  }
  return keys;
}

async function pruneAttachments(cfg: BrainstackConfig, machine: string, safeName: string): Promise<string[]> {
  const changedContexts: string[] = [];
  for (const file of await contextAttachmentFiles(cfg)) {
    let parsed: { packs?: BrainstackContextPackAttachment[] };
    try {
      parsed = JSON.parse(await readFile(file.path, "utf8")) as { packs?: BrainstackContextPackAttachment[] };
    } catch {
      continue;
    }
    const current = Array.isArray(parsed.packs) ? parsed.packs : [];
    const next = current.filter((pack) => !(pack.machine === machine && pack.safe_name === safeName));
    if (next.length !== current.length) {
      await writeAttachments(cfg, file.context, next);
      changedContexts.push(file.context);
    }
  }
  return changedContexts;
}

async function protectedLocalPackNames(cfg: BrainstackConfig, machine: string): Promise<Set<string>> {
  const protectedNames = new Set<string>();
  for (const key of await attachedPackKeys(cfg)) {
    const [attachedMachine, safeName] = key.split(":", 2);
    if (attachedMachine === machine && safeName) {
      protectedNames.add(safeName);
    }
  }
  const definitions = await readdir(definitionsRoot(cfg), { withFileTypes: true }).catch(() => []);
  for (const entry of definitions) {
    if (!entry.isFile() || !entry.name.endsWith(".json")) continue;
    try {
      const definition = JSON.parse(await readFile(join(definitionsRoot(cfg), entry.name), "utf8")) as BrainstackContextPackDefinition;
      if (definition.target_machine === machine || (isLocalMachine(cfg, machine) && isLocalMachine(cfg, definition.target_machine))) {
        protectedNames.add(definition.safe_name);
      }
    } catch {
      protectedNames.add(entry.name.replace(/\.json$/, ""));
    }
  }
  return protectedNames;
}

function delegatedControlFailure(label: string, result: RunResult): Error {
  if (result.timedOut) return new Error(`${label} failed on control host (timed out)`);
  const output = [result.stderr.trim(), result.stdout.trim()].filter(Boolean).join("\n");
  if (/Unknown command:\s+(?:context-packs|packs)\b/.test(output)) {
    return new Error(`${label} failed: control host Brainstack is too old for context packs. Update Brainstack on the control host, then retry.`);
  }
  return new Error(`${label} failed on control host (exit ${result.code})${output ? `\n${output}` : ""}`);
}

export function createContextPacksCommands(deps: ContextPacksDeps) {
  async function load(args: ParsedArgs): Promise<{ configPath: string; cfg: BrainstackConfig }> {
    const configPath = deps.abs(deps.requireFlagValue(args, "config") || deps.brainstackDefaultConfigPath());
    return {
      configPath,
      cfg: await deps.loadConfig(configPath, deps.flag(args, "profile"), deps.flag(args, "root"))
    };
  }

  async function delegateClientCommand(args: ParsedArgs, cfg: BrainstackConfig, argv: string[], timeoutMs: number): Promise<RunResult> {
    const remoteRepo =
      deps.requireFlagValue(args, "remote-repo") ||
      process.env.BRAINSTACK_TELEGRAM_REMOTE_REPO?.trim() ||
      cfg.client.telegramRemoteRepo ||
      "~/brainstack";
    const result = deps.runControlRemoteScript(
      cfg,
      args,
      "context packs control SSH target",
      deps.remoteBrainctlScript(remoteRepo, argv, { preferInstalledBinary: true }),
      timeoutMs,
      { allowRemoteSshFallback: true }
    );
    if (!result) throw new Error("context packs need a control SSH target for client-macos delegation");
    return result;
  }

  async function syncFromLocalSource(
    args: ParsedArgs,
    cfg: BrainstackConfig,
    definition: BrainstackContextPackDefinition,
    sourcePath: string,
    options: { dryRun?: boolean } = {}
  ): Promise<BrainstackContextPackManifest> {
    const timeoutMs = deps.parsePositiveIntegerFlag(args, "timeout-ms", DEFAULT_SYNC_TIMEOUT_MS);
    const scan = await scanPackSource(sourcePath, {
      include: definition.include,
      exclude: definition.exclude,
      allowSensitive: definition.allow_sensitive
    });
    const previousTreeSha256 = definition.last_tree_sha256;

    if (cfg.profile === "client-macos" && !isLocalMachine(cfg, definition.target_machine)) {
      if (options.dryRun) {
        const packRoot = remotePackRoot(definition.safe_name);
        const freeSpaceBytes = null;
        const warning = freeSpaceWarning(scan.totalBytes, freeSpaceBytes);
        if (warning) scan.warnings.push(warning);
        scan.warnings.push("dry run only: destination space and reachability are checked during sync on the control host");
        return manifestFromScan({
          cfg,
          scan,
          name: definition.name,
          safeName: definition.safe_name,
          machine: definition.target_machine,
          sourceMachine: definition.source_machine,
          sourceRoot: definition.source_root,
          packRoot,
          contentPath: remoteContentPath(definition.safe_name),
          previousTreeSha256,
          include: definition.include,
          exclude: definition.exclude,
          freeSpaceBytes,
          freshness: "unknown"
        });
      }
      await stageSourceOnControl(
        cfg,
        args,
        definition.safe_name,
        scan,
        timeoutMs,
        deps
      );
      const relaySource = `${remoteContextPacksRoot()}/relay/${safePackName(cfg.machine.name)}/${definition.safe_name}/source`;
      const remoteArgs = [
        "context-packs",
        "put",
        "--machine",
        definition.target_machine,
        "--name",
        definition.name,
        "--dir",
        relaySource,
        "--source-machine",
        definition.source_machine,
        "--source-root",
        definition.source_root,
        "--no-register",
        "--json",
        ...(definition.allow_sensitive ? ["--allow-sensitive"] : []),
        ...definition.include.flatMap((pattern) => ["--include", pattern]),
        ...definition.exclude.flatMap((pattern) => ["--exclude", pattern])
      ];
      const result = await delegateClientCommand(args, cfg, remoteArgs, timeoutMs);
      if (result.code !== 0 || result.timedOut) throw delegatedControlFailure("context pack sync", result);
      const parsed = JSON.parse(result.stdout) as { pack: BrainstackContextPackManifest };
      return parsed.pack;
    }

    const safeName = definition.safe_name;
    const sourceMachine = definition.source_machine;
    const sourceRoot = definition.source_root;
    let manifest: BrainstackContextPackManifest;
    if (isLocalMachine(cfg, definition.target_machine)) {
      const packRoot = localPackRoot(cfg, safeName);
      const freeSpaceBytes = localFreeSpaceBytes(cfg, deps);
      const warning = freeSpaceWarning(scan.totalBytes, freeSpaceBytes);
      if (warning) scan.warnings.push(warning);
      if (options.dryRun) {
        return manifestFromScan({
          cfg,
          scan,
          name: definition.name,
          safeName,
          machine: definition.target_machine,
          sourceMachine,
          sourceRoot,
          packRoot,
          contentPath: localContentPath(cfg, safeName),
          previousTreeSha256,
          include: definition.include,
          exclude: definition.exclude,
          freeSpaceBytes,
          freshness: "unknown"
        });
      }
      await rsyncLocal(scan, localContentPath(cfg, safeName), timeoutMs, deps);
      manifest = manifestFromScan({
        cfg,
        scan,
        name: definition.name,
        safeName,
        machine: definition.target_machine,
        sourceMachine,
        sourceRoot,
        packRoot,
        contentPath: localContentPath(cfg, safeName),
        previousTreeSha256,
        include: definition.include,
        exclude: definition.exclude,
        freeSpaceBytes,
        freshness: "fresh"
      });
      await writePrivateJson(join(packRoot, "manifest.json"), manifest);
      await writePrivateText(join(packRoot, "tree.jsonl"), scan.treeJsonl);
    } else {
      const worker = findWorker(cfg, definition.target_machine, deps);
      if (!worker) throw new Error(`unknown context pack target machine: ${definition.target_machine}`);
      const packRoot = remotePackRoot(safeName);
      const freeSpaceBytes = remoteFreeSpaceBytes(cfg, worker, deps);
      const warning = freeSpaceWarning(scan.totalBytes, freeSpaceBytes);
      if (warning) scan.warnings.push(warning);
      if (options.dryRun) {
        return manifestFromScan({
          cfg,
          scan,
          name: definition.name,
          safeName,
          machine: definition.target_machine,
          sourceMachine,
          sourceRoot,
          packRoot,
          contentPath: remoteContentPath(safeName),
          previousTreeSha256,
          include: definition.include,
          exclude: definition.exclude,
          freeSpaceBytes,
          freshness: "unknown"
        });
      }
      await rsyncRemote(cfg, worker, scan, remoteContentPath(safeName), timeoutMs, deps);
      manifest = manifestFromScan({
        cfg,
        scan,
        name: definition.name,
        safeName,
        machine: definition.target_machine,
        sourceMachine,
        sourceRoot,
        packRoot,
        contentPath: remoteContentPath(safeName),
        previousTreeSha256,
        include: definition.include,
        exclude: definition.exclude,
        freeSpaceBytes,
        freshness: "fresh"
      });
      await writeRemoteText(cfg, worker, `${packRoot}/manifest.json`, `${JSON.stringify(manifest, null, 2)}\n`, deps, timeoutMs);
      await writeRemoteText(cfg, worker, `${packRoot}/tree.jsonl`, scan.treeJsonl, deps, timeoutMs);
    }
    return manifest;
  }

  async function commandPut(args: ParsedArgs): Promise<void> {
    const { cfg } = await load(args);
    const dirInput = deps.requireFlagValue(args, "dir") || deps.requireFlagValue(args, "path") || args.positional[1];
    if (!dirInput) throw new Error("context-packs put requires --dir PATH");
    const sourcePath = expandLocalPath(dirInput, cfg, deps);
    const machine = machineFromArgs(args, cfg);
    const name = deps.requireFlagValue(args, "name") || basename(sourcePath);
    const safeName = safePackName(name);
    const internalRelay = deps.hasFlag(args, "no-register");
    if (!internalRelay && (deps.requireFlagValue(args, "source-machine") || deps.requireFlagValue(args, "source-root"))) {
      throw new Error("--source-machine and --source-root are internal relay options; registered packs must be created on the source machine");
    }
    const sourceMachine = internalRelay ? deps.requireFlagValue(args, "source-machine") || cfg.machine.name : cfg.machine.name;
    const sourceRoot = internalRelay ? deps.requireFlagValue(args, "source-root") || sourcePath : sourcePath;
    const now = new Date().toISOString();
    const existing = await readDefinition(cfg, safeName);
    const definition: BrainstackContextPackDefinition = {
      schema_version: 1,
      kind: "brainstack.context_pack_definition",
      id: packId(sourceMachine, name, sourceRoot),
      name,
      safe_name: safeName,
      source_machine: sourceMachine,
      source_root: sourceRoot,
      target_machine: machine,
      include: deps.flagValues(args, "include"),
      exclude: deps.flagValues(args, "exclude"),
      allow_sensitive: deps.hasFlag(args, "allow-sensitive"),
      created_at: existing?.created_at || now,
      updated_at: now,
      last_tree_sha256: existing?.last_tree_sha256 || null,
      last_synced_at: existing?.last_synced_at || null
    };
    const dryRun = deps.hasFlag(args, "dry-run") || deps.hasFlag(args, "preflight");
    const manifest = await syncFromLocalSource(args, cfg, definition, sourcePath, { dryRun });
    if (!dryRun) {
      definition.last_tree_sha256 = manifest.tree_sha256;
      definition.last_synced_at = manifest.refreshed_at;
      definition.updated_at = manifest.refreshed_at;
    }
    if (!dryRun && !deps.hasFlag(args, "no-register")) {
      await writeDefinition(cfg, definition);
    }
    if (deps.hasFlag(args, "json")) {
      console.log(JSON.stringify({ ok: true, dry_run: dryRun, pack: manifest, warnings: manifest.warnings }, null, 2));
    } else {
      console.log(`${dryRun ? "Preflight for" : "Synced"} context pack ${manifest.name} ${dryRun ? "on" : "to"} ${manifest.machine}`);
      console.log(`path=${manifest.content_path}`);
      console.log(`files=${manifest.file_count}`);
      console.log(`size=${formatBytes(manifest.total_bytes)}`);
      if (manifest.free_space_bytes !== null) console.log(`destination_free=${formatBytes(manifest.free_space_bytes)}`);
      for (const warning of manifest.warnings) console.log(`warning=${warning}`);
      if (dryRun) console.log("Run the same command without --dry-run to sync.");
    }
  }

  async function commandSync(args: ParsedArgs): Promise<void> {
    const { cfg } = await load(args);
    const name = deps.requireFlagValue(args, "name") || args.positional[1];
    if (!name) throw new Error("context-packs sync requires --name NAME");
    const safeName = safePackName(name);
    const existing = await readDefinition(cfg, safeName);
    const dirInput = deps.requireFlagValue(args, "dir") || deps.requireFlagValue(args, "path");
    if (deps.requireFlagValue(args, "source-machine") || deps.requireFlagValue(args, "source-root")) {
      throw new Error("--source-machine and --source-root are only supported by context-packs put --no-register");
    }
    if (!existing && !dirInput) {
      throw new Error(`No local source definition exists for context pack ${safeName}. Run context-packs put on the source machine first.`);
    }
    const definition =
      existing ||
      ({
        schema_version: 1,
        kind: "brainstack.context_pack_definition",
        id: packId(cfg.machine.name, name, expandLocalPath(dirInput!, cfg, deps)),
        name,
        safe_name: safeName,
        source_machine: cfg.machine.name,
        source_root: expandLocalPath(dirInput!, cfg, deps),
        target_machine: machineFromArgs(args, cfg),
        include: deps.flagValues(args, "include"),
        exclude: deps.flagValues(args, "exclude"),
        allow_sensitive: deps.hasFlag(args, "allow-sensitive"),
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        last_tree_sha256: null,
        last_synced_at: null
      } satisfies BrainstackContextPackDefinition);
    const sourcePath = expandLocalPath(dirInput || definition.source_root, cfg, deps);
    const dryRun = deps.hasFlag(args, "dry-run") || deps.hasFlag(args, "preflight");
    const manifest = await syncFromLocalSource(args, cfg, definition, sourcePath, { dryRun });
    if (!dryRun) {
      definition.last_tree_sha256 = manifest.tree_sha256;
      definition.last_synced_at = manifest.refreshed_at;
      definition.updated_at = manifest.refreshed_at;
      await writeDefinition(cfg, definition);
    }
    if (deps.hasFlag(args, "json")) {
      console.log(JSON.stringify({ ok: true, dry_run: dryRun, pack: manifest, warnings: manifest.warnings }, null, 2));
    } else {
      console.log(`${dryRun ? "Preflight for" : "Synced"} context pack ${manifest.name} ${dryRun ? "on" : "to"} ${manifest.machine}`);
      console.log(`path=${manifest.content_path}`);
      console.log(`files=${manifest.file_count}`);
      console.log(`size=${formatBytes(manifest.total_bytes)}`);
      if (manifest.free_space_bytes !== null) console.log(`destination_free=${formatBytes(manifest.free_space_bytes)}`);
      for (const warning of manifest.warnings) console.log(`warning=${warning}`);
      if (dryRun) console.log("Run the same command without --dry-run to sync.");
    }
  }

  async function listForMachine(args: ParsedArgs, cfg: BrainstackConfig, machine: string): Promise<BrainstackContextPackManifest[]> {
    if (cfg.profile === "client-macos" && !isLocalMachine(cfg, machine)) {
      const result = await delegateClientCommand(
        args,
        cfg,
        ["context-packs", "list", "--machine", machine, "--json"],
        deps.parsePositiveIntegerFlag(args, "timeout-ms", 45_000)
      );
      if (result.code !== 0 || result.timedOut) throw delegatedControlFailure("context pack list", result);
      return (JSON.parse(result.stdout) as { packs?: BrainstackContextPackManifest[] }).packs || [];
    }
    if (isLocalMachine(cfg, machine)) {
      return (await readLocalPacks(contextPacksRoot(cfg))).sort((a, b) => Date.parse(b.refreshed_at) - Date.parse(a.refreshed_at));
    }
    const worker = findWorker(cfg, machine, deps);
    if (!worker) throw new Error(`unknown context pack target machine: ${machine}`);
    const result = deps.run(workerShellArgs(cfg, worker, listRemotePacksScript(), deps), {
      check: false,
      timeoutMs: deps.parsePositiveIntegerFlag(args, "timeout-ms", 45_000)
    });
    if (result.code !== 0 || result.timedOut) throw new Error(`context pack list failed on ${machine}\n${result.stderr || result.stdout}`);
    return parseJsonLines(result.stdout).sort((a, b) => Date.parse(b.refreshed_at) - Date.parse(a.refreshed_at));
  }

  async function commandList(args: ParsedArgs): Promise<void> {
    const { cfg } = await load(args);
    const machine = machineFromArgs(args, cfg);
    const packs = await listForMachine(args, cfg, machine);
    if (deps.hasFlag(args, "json")) {
      console.log(JSON.stringify({ ok: true, machine, packs }, null, 2));
      return;
    }
    console.log(packs.length ? packs.map(formatPack).join("\n") : `No context packs found on ${machine}.`);
  }

  async function commandAttach(args: ParsedArgs): Promise<void> {
    const { cfg } = await load(args);
    if (cfg.profile === "client-macos") {
      const passthrough = ["context-packs", "attach", ...args.positional.slice(1), ...Object.entries(args.flags).flatMap(([key, value]) => {
        if (key === "config" || key === "profile" || key === "root") return [];
        if (value === true) return [`--${key}`];
        return Array.isArray(value) ? value.flatMap((entry) => [`--${key}`, entry]) : [`--${key}`, String(value)];
      })];
      const result = await delegateClientCommand(args, cfg, passthrough, deps.parsePositiveIntegerFlag(args, "timeout-ms", 30_000));
      if (result.code !== 0 || result.timedOut) throw delegatedControlFailure("context pack attach", result);
      process.stdout.write(result.stdout);
      if (result.stderr) process.stderr.write(result.stderr);
      return;
    }
    const context = deps.requireFlagValue(args, "context") || args.positional[1];
    const name = deps.requireFlagValue(args, "name") || args.positional[2];
    if (!context || !name) throw new Error("context-packs attach requires --context SLUG --name NAME");
    const machine = machineFromArgs(args, cfg);
    const safeName = safePackName(name);
    const attachments = await readAttachments(cfg, context);
    const withoutExisting = attachments.filter((pack) => !(pack.safe_name === safeName && pack.machine === machine));
    const next = [
      ...withoutExisting,
      {
        name,
        safe_name: safeName,
        machine,
        attached_at: new Date().toISOString()
      }
    ];
    await writeAttachments(cfg, context, next);
    if (deps.hasFlag(args, "json")) {
      console.log(JSON.stringify({ ok: true, context, packs: next }, null, 2));
    } else {
      console.log(`Attached context pack ${safeName} on ${machine} to ${context}.`);
    }
  }

  async function commandDetach(args: ParsedArgs): Promise<void> {
    const { cfg } = await load(args);
    if (cfg.profile === "client-macos") {
      const passthrough = ["context-packs", "detach", ...args.positional.slice(1), ...Object.entries(args.flags).flatMap(([key, value]) => {
        if (key === "config" || key === "profile" || key === "root") return [];
        if (value === true) return [`--${key}`];
        return Array.isArray(value) ? value.flatMap((entry) => [`--${key}`, entry]) : [`--${key}`, String(value)];
      })];
      const result = await delegateClientCommand(args, cfg, passthrough, deps.parsePositiveIntegerFlag(args, "timeout-ms", 30_000));
      if (result.code !== 0 || result.timedOut) throw delegatedControlFailure("context pack detach", result);
      process.stdout.write(result.stdout);
      if (result.stderr) process.stderr.write(result.stderr);
      return;
    }
    const context = deps.requireFlagValue(args, "context") || args.positional[1];
    const name = deps.requireFlagValue(args, "name") || args.positional[2];
    if (!context || !name) throw new Error("context-packs detach requires --context SLUG --name NAME");
    const safeName = safePackName(name);
    const requestedMachine = depsMachineFlag(args);
    const attachments = await readAttachments(cfg, context);
    const matches = attachments.filter((pack) => pack.safe_name === safeName);
    if (!requestedMachine && matches.length > 1) {
      throw new Error(`multiple context packs named ${safeName} are attached; pass --machine MACHINE to detach one`);
    }
    const machine = requestedMachine || matches[0]?.machine || "";
    const next = attachments.filter((pack) => !(pack.safe_name === safeName && (!machine || pack.machine === machine)));
    await writeAttachments(cfg, context, next);
    if (deps.hasFlag(args, "json")) {
      console.log(JSON.stringify({ ok: true, context, machine: machine || null, packs: next }, null, 2));
    } else {
      console.log(`Detached context pack ${safeName}${machine ? ` on ${machine}` : ""} from ${context}.`);
    }
  }

  async function commandRm(args: ParsedArgs): Promise<void> {
    const { cfg } = await load(args);
    const name = deps.requireFlagValue(args, "name") || args.positional[1];
    if (!name) throw new Error("context-packs rm requires --name NAME");
    const safeName = safePackName(name);
    const machine = machineFromArgs(args, cfg);
    if (cfg.profile === "client-macos" && !isLocalMachine(cfg, machine)) {
      const result = await delegateClientCommand(args, cfg, ["context-packs", "rm", "--machine", machine, "--name", safeName, "--json"], deps.parsePositiveIntegerFlag(args, "timeout-ms", 60_000));
      if (result.code !== 0 || result.timedOut) throw delegatedControlFailure("context pack delete", result);
      if (deps.hasFlag(args, "json")) process.stdout.write(result.stdout);
      else console.log(`Deleted context pack ${safeName} from ${machine}.`);
      return;
    }
    if (isLocalMachine(cfg, machine)) {
      await rm(localPackRoot(cfg, safeName), { recursive: true, force: true });
      await rm(definitionPath(cfg, safeName), { force: true });
    } else {
      const worker = findWorker(cfg, machine, deps);
      if (!worker) throw new Error(`unknown context pack target machine: ${machine}`);
      const script = `rm -rf -- "$HOME/.local/state/brainstack/context-packs/${safeName}"`;
      runChecked(deps.run(workerShellArgs(cfg, worker, script, deps), { check: false, timeoutMs: 60_000 }), `delete context pack from ${machine}`);
    }
    const detached_contexts = await pruneAttachments(cfg, machine, safeName);
    if (deps.hasFlag(args, "json")) console.log(JSON.stringify({ ok: true, deleted: true, machine, name: safeName, detached_contexts }, null, 2));
    else console.log(`Deleted context pack ${safeName} from ${machine}.${detached_contexts.length ? ` Detached from ${detached_contexts.length} context(s).` : ""}`);
  }

  async function commandGc(args: ParsedArgs): Promise<void> {
    const { cfg } = await load(args);
    const machine = machineFromArgs(args, cfg);
    const shouldDelete = deps.hasFlag(args, "yes");
    if (cfg.profile === "client-macos" && !isLocalMachine(cfg, machine)) {
      const result = await delegateClientCommand(
        args,
        cfg,
        ["context-packs", "gc", "--machine", machine, ...(shouldDelete ? ["--yes"] : []), "--json"],
        deps.parsePositiveIntegerFlag(args, "timeout-ms", 60_000)
      );
      if (result.code !== 0 || result.timedOut) throw delegatedControlFailure("context pack gc", result);
      if (deps.hasFlag(args, "json")) process.stdout.write(result.stdout);
      else process.stdout.write(result.stdout || (shouldDelete ? `Deleted folder-pack copies on ${machine}.\n` : `GC dry run finished on ${machine}.\n`));
      return;
    }
    if (!isLocalMachine(cfg, machine)) {
      const worker = findWorker(cfg, machine, deps);
      if (!worker) throw new Error(`unknown context pack target machine: ${machine}`);
      const protectedNames = await protectedLocalPackNames(cfg, machine);
      const result = deps.run(workerShellArgs(cfg, worker, remoteGcScript(shouldDelete, protectedNames), deps), { check: false, timeoutMs: 60_000 });
      if (result.code !== 0 || result.timedOut) throw new Error(`context pack gc failed on ${machine}\n${result.stderr || result.stdout}`);
      const candidates = result.stdout.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
      if (deps.hasFlag(args, "json")) {
        console.log(JSON.stringify({ ok: true, machine, dry_run: !shouldDelete, candidates, deleted: shouldDelete ? candidates : [] }, null, 2));
      } else {
        console.log(shouldDelete ? `Deleted ${candidates.length} folder-pack copy/copies on ${machine}.` : `GC dry run: ${candidates.length} folder-pack copy/copies on ${machine} would be deleted. Use --yes to delete.`);
      }
      return;
    }
    const packs = await readLocalPacks(contextPacksRoot(cfg));
    const protectedNames = await protectedLocalPackNames(cfg, machine);
    const candidates = packs.filter((pack) => !protectedNames.has(pack.safe_name));
    const deleted: string[] = [];
    if (shouldDelete) {
      for (const pack of candidates) {
        await rm(localPackRoot(cfg, pack.safe_name), { recursive: true, force: true });
        deleted.push(pack.safe_name);
      }
    }
    if (deps.hasFlag(args, "json")) console.log(JSON.stringify({ ok: true, machine, dry_run: !shouldDelete, candidates: candidates.map((pack) => pack.safe_name), protected: [...protectedNames].sort(), deleted }, null, 2));
    else console.log(shouldDelete ? `Deleted ${deleted.length} local pack(s).` : `GC dry run: ${candidates.length} local pack(s) would be deleted; ${protectedNames.size} protected. Use --yes to delete local pack copies.`);
  }

  async function commandShowAttachments(args: ParsedArgs): Promise<void> {
    const { cfg } = await load(args);
    const context = deps.requireFlagValue(args, "context") || args.positional[1];
    if (!context) throw new Error("context-packs attachments requires --context SLUG");
    const packs = await readAttachments(cfg, context);
    if (deps.hasFlag(args, "json")) console.log(JSON.stringify({ ok: true, context, packs }, null, 2));
    else console.log(packs.length ? packs.map((pack) => `${pack.safe_name} machine=${pack.machine} attached=${pack.attached_at}`).join("\n") : `No context packs attached to ${context}.`);
  }

  async function commandContextPacks(args: ParsedArgs): Promise<void> {
    const subcommand = args.positional[0] || "list";
    switch (subcommand) {
      case "put":
      case "add":
        return await commandPut(args);
      case "sync":
      case "refresh":
        return await commandSync(args);
      case "list":
      case "ls":
        return await commandList(args);
      case "attach":
        return await commandAttach(args);
      case "detach":
        return await commandDetach(args);
      case "rm":
      case "delete":
        return await commandRm(args);
      case "gc":
        return await commandGc(args);
      case "attachments":
        return await commandShowAttachments(args);
      default:
        throw new Error(`Unknown context-packs subcommand: ${subcommand}`);
    }
  }

  return { commandContextPacks };
}
