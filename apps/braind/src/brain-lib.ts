import { Database } from "bun:sqlite";
import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { basename, dirname, extname, join, normalize, relative, resolve, sep } from "node:path";

type FrontmatterValue = string | boolean | number | null | string[];

export interface FrontmatterData {
  [key: string]: FrontmatterValue | undefined;
}

export interface SourceManifest {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
  sha256: string;
  source_machine: string;
  source_harness: string;
  source_type: string;
  mime_type: string;
  raw_path: string;
  normalized_path?: string;
  page_path?: string;
  storage?: "git" | "external";
  size_bytes?: number;
  external_blob_path?: string;
  external_blob_sha256?: string;
  external_blob_size_bytes?: number;
  related_project?: string;
  related_repo?: string;
  tags?: string[];
  conversation_id?: string;
  source_url?: string;
  original_filename?: string;
  deduplicated_from?: string;
  normalization_status?: string;
}

export interface SearchResult {
  path: string;
  title: string;
  scope: "wiki" | "raw";
  type: string;
  tags: string[];
  source_ids: string[];
  snippet: string;
  score: number;
}

export interface ImportMetadata {
  title?: string;
  source_harness: string;
  source_machine: string;
  source_type: string;
  related_project?: string;
  related_repo?: string;
  tags?: string[];
  conversation_id?: string;
  ingest_now?: boolean;
}

export interface ImportArtifactInput extends ImportMetadata {
  text?: string;
  url?: string;
  bytes?: Uint8Array;
  fileName?: string;
  contentType?: string;
}

export interface CreateImportResult {
  artifactId: string;
  manifest: SourceManifest;
  touchedFiles: string[];
  deduplicated: boolean;
  summary: string;
}

export interface IngestResult {
  artifactIds: string[];
  touchedFiles: string[];
  sourcePages: string[];
  relatedPages: string[];
}

export interface LintResult {
  reportPath: string;
  touchedFiles: string[];
}

export interface HealthStatus {
  ok: boolean;
  commit: string | null;
  indexed_docs: number;
  last_reindex_at: string | null;
  repo_root: string;
}

export interface RecentLogEntry {
  timestamp: string;
  operation: string;
  subject: string;
  title: string;
  body: string[];
}

export interface RepoPaths {
  repoRoot: string;
  wikiDir: string;
  rawDir: string;
  importedDir: string;
  normalizedDir: string;
  conversationsDir: string;
  assetsDir: string;
  manifestsDir: string;
  machineManifestDir: string;
  harnessManifestDir: string;
  sourceManifestDir: string;
  proposalsPendingDir: string;
  proposalsAppliedDir: string;
  logsPath: string;
  derivedDir: string;
  searchDbPath: string;
  lockDir: string;
}

const COMMAND_CACHE = new Map<string, boolean>();
const TEXT_EXTENSIONS = new Set([
  ".md",
  ".markdown",
  ".txt",
  ".html",
  ".htm",
  ".json",
  ".yml",
  ".yaml",
  ".csv",
  ".ts",
  ".tsx",
  ".js",
  ".jsx",
  ".sh",
  ".py",
  ".log"
]);
const BINARY_ASSET_EXTENSIONS = new Set([
  ".png",
  ".jpg",
  ".jpeg",
  ".gif",
  ".webp",
  ".svg",
  ".pdf",
  ".docx",
  ".zip",
  ".tar",
  ".gz",
  ".mp3",
  ".mp4",
  ".mov",
  ".wav"
]);

export function getRepoRoot(explicit?: string): string {
  return resolve(
    explicit || process.env.SHARED_BRAIN_REPO_ROOT || resolve(import.meta.dir, "..", "..")
  );
}

export function getRepoPaths(explicit?: string): RepoPaths {
  const repoRoot = getRepoRoot(explicit);
  return {
    repoRoot,
    wikiDir: join(repoRoot, "wiki"),
    rawDir: join(repoRoot, "raw"),
    importedDir: join(repoRoot, "raw", "imported"),
    normalizedDir: join(repoRoot, "raw", "normalized"),
    conversationsDir: join(repoRoot, "raw", "conversations"),
    assetsDir: join(repoRoot, "raw", "assets"),
    manifestsDir: join(repoRoot, "manifests"),
    machineManifestDir: join(repoRoot, "manifests", "machines"),
    harnessManifestDir: join(repoRoot, "manifests", "harnesses"),
    sourceManifestDir: join(repoRoot, "manifests", "sources"),
    proposalsPendingDir: join(repoRoot, "proposals", "pending"),
    proposalsAppliedDir: join(repoRoot, "proposals", "applied"),
    logsPath: join(repoRoot, "logs", "log.md"),
    derivedDir: join(repoRoot, "derived"),
    searchDbPath: join(repoRoot, "derived", "search.sqlite"),
    lockDir: join(repoRoot, ".shared-brain.lock")
  };
}

export function isoNow(): string {
  return new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
}

export function expandHome(input: string): string {
  if (!input.startsWith("~")) {
    return input;
  }
  const home = process.env.HOME || "/home/swader";
  return resolve(home, input.slice(2));
}

export function normalizeRepoPath(filePath: string): string {
  return filePath.split(sep).join("/");
}

export function toRepoRelative(repoRoot: string, absolutePath: string): string {
  return normalizeRepoPath(relative(repoRoot, absolutePath));
}

async function ensureDir(path: string): Promise<void> {
  await mkdir(path, { recursive: true });
}

async function readText(path: string): Promise<string> {
  return await readFile(path, "utf8");
}

async function writeText(path: string, contents: string): Promise<void> {
  await ensureDir(dirname(path));
  await writeFile(path, contents, "utf8");
}

async function writeJson(path: string, value: unknown): Promise<void> {
  await writeText(path, `${JSON.stringify(value, null, 2)}\n`);
}

async function readJson<T>(path: string): Promise<T> {
  return JSON.parse(await readText(path)) as T;
}

async function listFiles(root: string): Promise<string[]> {
  if (!existsSync(root)) {
    return [];
  }
  const entries = await readdir(root, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const fullPath = join(root, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await listFiles(fullPath)));
    } else if (entry.isFile()) {
      files.push(fullPath);
    }
  }
  return files;
}

export function slugify(input: string): string {
  return input
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export function titleToPageStem(title: string): string {
  return title
    .trim()
    .split(/\s+/)
    .map((part) => part.replace(/[^A-Za-z0-9]/g, ""))
    .filter(Boolean)
    .join("-");
}

export function parseFrontmatter(markdown: string): { data: FrontmatterData; body: string } {
  if (!markdown.startsWith("---\n")) {
    return { data: {}, body: markdown };
  }
  const endIndex = markdown.indexOf("\n---\n", 4);
  if (endIndex === -1) {
    return { data: {}, body: markdown };
  }
  const raw = markdown.slice(4, endIndex).split("\n");
  const data: FrontmatterData = {};
  let currentArrayKey: string | null = null;
  for (const line of raw) {
    if (!line.trim()) {
      continue;
    }
    const arrayMatch = line.match(/^\s*-\s+(.*)$/);
    if (arrayMatch && currentArrayKey) {
      const existing = (data[currentArrayKey] as string[] | undefined) || [];
      existing.push(parseScalar(arrayMatch[1]) as string);
      data[currentArrayKey] = existing;
      continue;
    }
    const keyMatch = line.match(/^([A-Za-z0-9_]+):(?:\s+(.*))?$/);
    if (!keyMatch) {
      currentArrayKey = null;
      continue;
    }
    const [, key, rawValue] = keyMatch;
    if (rawValue === undefined || rawValue === "") {
      data[key] = [];
      currentArrayKey = key;
      continue;
    }
    data[key] = parseScalar(rawValue);
    currentArrayKey = null;
  }
  return { data, body: markdown.slice(endIndex + 5) };
}

function parseScalar(rawValue: string): string | boolean | number | null {
  const trimmed = rawValue.trim();
  if (trimmed === "true") {
    return true;
  }
  if (trimmed === "false") {
    return false;
  }
  if (trimmed === "null") {
    return null;
  }
  if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
    return Number(trimmed);
  }
  return trimmed.replace(/^["']|["']$/g, "");
}

export function stringifyFrontmatter(data: FrontmatterData, body: string): string {
  const lines = ["---"];
  const orderedKeys = Object.keys(data);
  for (const key of orderedKeys) {
    const value = data[key];
    if (value === undefined) {
      continue;
    }
    if (Array.isArray(value)) {
      lines.push(`${key}:`);
      for (const item of value) {
        lines.push(`  - ${yamlScalar(item)}`);
      }
      continue;
    }
    lines.push(`${key}: ${yamlScalar(value)}`);
  }
  lines.push("---", "", body.trimEnd(), "");
  return lines.join("\n");
}

function yamlScalar(value: string | number | boolean | null): string {
  if (value === null) {
    return "null";
  }
  if (typeof value === "boolean" || typeof value === "number") {
    return String(value);
  }
  if (/^[A-Za-z0-9_./:@+-]+$/.test(value)) {
    return value;
  }
  return JSON.stringify(value);
}

export function stripFrontmatter(markdown: string): string {
  return parseFrontmatter(markdown).body;
}

export function findFirstHeading(markdown: string): string | null {
  const match = stripFrontmatter(markdown).match(/^#\s+(.+)$/m);
  return match ? match[1].trim() : null;
}

export async function readPage(repoRoot: string, repoRelativePath: string): Promise<{
  path: string;
  absolutePath: string;
  data: FrontmatterData;
  body: string;
  raw: string;
  title: string;
}> {
  const safePath = safeRepoPath(repoRoot, repoRelativePath);
  const raw = await readText(safePath);
  const { data, body } = parseFrontmatter(raw);
  const title =
    typeof data.title === "string"
      ? data.title
      : findFirstHeading(raw) || basename(repoRelativePath, extname(repoRelativePath));
  return {
    path: normalizeRepoPath(repoRelativePath),
    absolutePath: safePath,
    data,
    body,
    raw,
    title
  };
}

export function safeRepoPath(repoRoot: string, repoRelativePath: string): string {
  const normalizedInput = repoRelativePath.replace(/^\/+/, "");
  const candidate = resolve(repoRoot, normalizedInput);
  const normalizedRoot = `${normalize(repoRoot)}${sep}`;
  if (!candidate.startsWith(normalizedRoot) && normalize(candidate) !== normalize(repoRoot)) {
    throw new Error(`Path escapes repo root: ${repoRelativePath}`);
  }
  return candidate;
}

export function extractHeadings(markdown: string): string[] {
  return Array.from(stripFrontmatter(markdown).matchAll(/^#{1,6}\s+(.+)$/gm)).map((match) =>
    match[1].trim()
  );
}

export function extractWikiLinks(markdown: string): Array<{ target: string; label: string | null }> {
  return Array.from(markdown.matchAll(/\[\[([^\]|]+)(?:\|([^\]]+))?\]\]/g)).map((match) => ({
    target: match[1].trim(),
    label: match[2]?.trim() || null
  }));
}

export function extractMarkdownLinks(markdown: string): Array<{ target: string; label: string }> {
  return Array.from(markdown.matchAll(/\[([^\]]+)\]\(([^)]+)\)/g)).map((match) => ({
    label: match[1].trim(),
    target: match[2].trim()
  }));
}

export function resolveInternalLinkPath(currentRepoPath: string, target: string): string | null {
  if (!target || /^https?:\/\//i.test(target) || target.startsWith("mailto:")) {
    return null;
  }
  const cleanTarget = target.replace(/^\/+/, "");
  if (cleanTarget.startsWith("wiki/") || cleanTarget.startsWith("raw/") || cleanTarget.startsWith("docs/")) {
    return cleanTarget.endsWith(".md") || cleanTarget.startsWith("raw/") ? cleanTarget : `${cleanTarget}.md`;
  }
  if (cleanTarget.includes("/") && !cleanTarget.startsWith("./") && !cleanTarget.startsWith("../")) {
    return cleanTarget.endsWith(".md") ? `wiki/${cleanTarget}` : `wiki/${cleanTarget}.md`;
  }
  if (cleanTarget.startsWith("./") || cleanTarget.startsWith("../")) {
    const resolved = normalize(join(dirname(currentRepoPath), cleanTarget));
    return normalizeRepoPath(resolved.endsWith(".md") ? resolved : `${resolved}.md`);
  }
  return normalizeRepoPath(`wiki/${cleanTarget.endsWith(".md") ? cleanTarget : `${cleanTarget}.md`}`);
}

function inferTitleFromPath(repoPath: string): string {
  return basename(repoPath, extname(repoPath)).replace(/[-_]+/g, " ");
}

function normalizeTextSnippet(input: string, limit = 900): string {
  return input.replace(/\s+/g, " ").trim().slice(0, limit);
}

function uniqueStrings(values: string[]): string[] {
  return Array.from(new Set(values.filter(Boolean)));
}

function arrayField(value: FrontmatterValue | undefined): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

async function commandExists(command: string): Promise<boolean> {
  if (COMMAND_CACHE.has(command)) {
    return COMMAND_CACHE.get(command) || false;
  }
  const proc = Bun.spawnSync(["which", command], { stdout: "ignore", stderr: "ignore" });
  const exists = proc.exitCode === 0;
  COMMAND_CACHE.set(command, exists);
  return exists;
}

function runCommand(args: string[], cwd: string, check = true): string {
  const proc = Bun.spawnSync(args, { cwd, stdout: "pipe", stderr: "pipe" });
  const stdout = proc.stdout.toString().trim();
  const stderr = proc.stderr.toString().trim();
  if (check && proc.exitCode !== 0) {
    throw new Error(`${args.join(" ")} failed: ${stderr || stdout}`);
  }
  return stdout;
}

async function hashBytes(bytes: Uint8Array): Promise<string> {
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(bytes);
  return hasher.digest("hex");
}

async function hashText(text: string): Promise<string> {
  return await hashBytes(new TextEncoder().encode(text));
}

function guessMimeFromPath(fileName: string): string {
  const extension = extname(fileName).toLowerCase();
  switch (extension) {
    case ".md":
    case ".markdown":
      return "text/markdown";
    case ".txt":
    case ".log":
      return "text/plain";
    case ".html":
    case ".htm":
      return "text/html";
    case ".json":
      return "application/json";
    case ".pdf":
      return "application/pdf";
    case ".docx":
      return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    case ".png":
      return "image/png";
    case ".jpg":
    case ".jpeg":
      return "image/jpeg";
    case ".gif":
      return "image/gif";
    case ".webp":
      return "image/webp";
    case ".svg":
      return "image/svg+xml";
    default:
      return "application/octet-stream";
  }
}

function largeFileThresholdBytes(): number {
  const raw = process.env.BRAIN_LARGE_FILE_THRESHOLD_BYTES || process.env.BRAINSTACK_LARGE_FILE_THRESHOLD_BYTES || "";
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 10 * 1024 * 1024;
}

function blobStoreRoot(): string {
  const configured = process.env.BRAIN_BLOB_STORE || process.env.BRAINSTACK_BLOB_STORE || "";
  if (configured.trim()) {
    return resolve(expandHome(configured.trim()));
  }
  return resolve(process.env.HOME || ".", ".local", "state", "brainstack", "blobs", "shared-brain");
}

function shouldStoreExternally(fileName: string, mimeType: string, sizeBytes: number): boolean {
  return sizeBytes > largeFileThresholdBytes() && isBinaryAsset(fileName, mimeType) && !isTextLike(fileName, mimeType);
}

function isTextLike(fileName: string, mimeType?: string): boolean {
  const extension = extname(fileName).toLowerCase();
  if (TEXT_EXTENSIONS.has(extension)) {
    return true;
  }
  return Boolean(mimeType && (mimeType.startsWith("text/") || mimeType.includes("json")));
}

function isBinaryAsset(fileName: string, mimeType?: string): boolean {
  const extension = extname(fileName).toLowerCase();
  if (BINARY_ASSET_EXTENSIONS.has(extension)) {
    return true;
  }
  return Boolean(
    mimeType &&
      (mimeType.startsWith("image/") ||
        mimeType === "application/pdf" ||
        mimeType.includes("wordprocessingml"))
  );
}

function stripHtmlToText(html: string): string {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&#39;/gi, "'")
    .replace(/&quot;/gi, '"')
    .replace(/\s+/g, " ")
    .trim();
}

function jsonSummary(title: string, text: string): string {
  try {
    const parsed = JSON.parse(text);
    const keys =
      parsed && typeof parsed === "object" ? Object.keys(parsed as Record<string, unknown>) : [];
    return [
      `# ${title}`,
      "",
      "## JSON summary",
      "",
      `- Top-level keys: ${keys.length ? keys.join(", ") : "none"}`,
      "",
      "```json",
      JSON.stringify(parsed, null, 2),
      "```",
      ""
    ].join("\n");
  } catch {
    return [`# ${title}`, "", "## JSON summary", "", "Unable to parse JSON during normalization.", ""].join(
      "\n"
    );
  }
}

async function normalizeArtifactFile(
  paths: RepoPaths,
  manifest: SourceManifest,
  rawAbsolutePath: string,
  originalText?: string
): Promise<{ relativePath: string; status: string }> {
  const title = manifest.title;
  const rawRepoPath = manifest.raw_path;
  const extension = extname(rawAbsolutePath).toLowerCase();
  const normalizedPath = join(paths.normalizedDir, `${manifest.id}.md`);
  let body = "";
  let status = "complete";

  if (isTextLike(rawAbsolutePath, manifest.mime_type)) {
    const text = originalText ?? (await readText(rawAbsolutePath));
    if (extension === ".html" || extension === ".htm" || manifest.mime_type.includes("html")) {
      body = [`# ${title}`, "", stripHtmlToText(text), ""].join("\n");
    } else if (extension === ".json" || manifest.mime_type.includes("json")) {
      body = jsonSummary(title, text);
    } else {
      body = extension === ".md" ? text : [`# ${title}`, "", text.trim(), ""].join("\n");
    }
  } else if (extension === ".docx") {
    if (await commandExists("pandoc")) {
      body = runCommand(["pandoc", rawAbsolutePath, "-t", "gfm"], paths.repoRoot, false) || "";
      if (!body.trim()) {
        status = "deferred";
        body = [
          `# ${title}`,
          "",
          "Normalization deferred. `pandoc` returned no content.",
          "",
          `- Raw artifact: [[${rawRepoPath}]]`,
          ""
        ].join("\n");
      } else {
        body = `# ${title}\n\n${body.trim()}\n`;
      }
    } else {
      status = "deferred";
      body = [
        `# ${title}`,
        "",
        "Normalization deferred because `pandoc` is not available.",
        "",
        `- Raw artifact: [[${rawRepoPath}]]`,
        ""
      ].join("\n");
    }
  } else if (extension === ".pdf" || manifest.mime_type === "application/pdf") {
    if (await commandExists("pdftotext")) {
      body = runCommand(["pdftotext", rawAbsolutePath, "-"], paths.repoRoot, false);
      if (!body.trim()) {
        status = "deferred";
        body = [
          `# ${title}`,
          "",
          "Normalization deferred. `pdftotext` returned no content.",
          "",
          `- Raw artifact: [[${rawRepoPath}]]`,
          ""
        ].join("\n");
      } else {
        body = `# ${title}\n\n${body.trim()}\n`;
      }
    } else {
      status = "deferred";
      body = [
        `# ${title}`,
        "",
        "Normalization deferred because `pdftotext` is not available.",
        "",
        `- Raw artifact: [[${rawRepoPath}]]`,
        ""
      ].join("\n");
    }
  } else {
    status = "stub";
    body = [
      `# ${title}`,
      "",
      "Binary or asset artifact retained in canonical raw storage.",
      "",
      `- Raw artifact: [[${rawRepoPath}]]`,
      `- MIME type: \`${manifest.mime_type}\``,
      ""
    ].join("\n");
  }

  await writeText(normalizedPath, body);
  return { relativePath: toRepoRelative(paths.repoRoot, normalizedPath), status };
}

async function findSourceManifestFiles(paths: RepoPaths): Promise<string[]> {
  return (await listFiles(paths.sourceManifestDir)).filter((file) => file.endsWith(".json")).sort();
}

export async function findSourceManifestById(repoRoot: string, artifactId: string): Promise<SourceManifest | null> {
  const paths = getRepoPaths(repoRoot);
  const filePath = join(paths.sourceManifestDir, `${artifactId}.json`);
  if (!existsSync(filePath)) {
    return null;
  }
  return await readJson<SourceManifest>(filePath);
}

async function findSourceManifestBySha(paths: RepoPaths, sha256: string): Promise<SourceManifest | null> {
  for (const file of await findSourceManifestFiles(paths)) {
    const manifest = await readJson<SourceManifest>(file);
    if (manifest.sha256 === sha256) {
      return manifest;
    }
  }
  return null;
}

async function findManifestById(directory: string, id: string): Promise<Record<string, unknown> | null> {
  const filePath = join(directory, `${id}.json`);
  if (!existsSync(filePath)) {
    return null;
  }
  return await readJson<Record<string, unknown>>(filePath);
}

async function loadManifestPagePath(
  paths: RepoPaths,
  kind: "machines" | "harnesses",
  id: string
): Promise<string | null> {
  const dir = kind === "machines" ? paths.machineManifestDir : paths.harnessManifestDir;
  const manifest = await findManifestById(dir, id);
  return typeof manifest?.page_path === "string" ? manifest.page_path : null;
}

function chooseRawDirectory(paths: RepoPaths, fileName: string, input: ImportArtifactInput, mimeType: string): string {
  if (input.conversation_id || input.source_type.toLowerCase().includes("conversation")) {
    return paths.conversationsDir;
  }
  if (isBinaryAsset(fileName, mimeType) && !isTextLike(fileName, mimeType)) {
    return paths.assetsDir;
  }
  return paths.importedDir;
}

function deriveArtifactId(sha256: string): string {
  const compactTs = isoNow().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z").toLowerCase();
  return `art-${compactTs}-${sha256.slice(0, 12)}`;
}

function deriveTitle(input: ImportArtifactInput): string {
  if (input.title?.trim()) {
    return input.title.trim();
  }
  if (input.fileName) {
    return basename(input.fileName, extname(input.fileName));
  }
  if (input.url) {
    try {
      const parsed = new URL(input.url);
      return basename(parsed.pathname) || parsed.hostname;
    } catch {
      return "Imported URL";
    }
  }
  if (input.text?.trim()) {
    const firstLine = input.text.trim().split("\n")[0];
    return firstLine.replace(/^#+\s*/, "").slice(0, 80) || "Imported note";
  }
  return "Imported artifact";
}

function collectInputTags(input: ImportArtifactInput): string[] {
  return uniqueStrings([...(input.tags || []), input.source_type, input.source_machine, input.source_harness]);
}

export async function appendLogEntry(
  repoRoot: string,
  operation: string,
  subject: string,
  shortTitle: string,
  detailLines: string[]
): Promise<string> {
  const paths = getRepoPaths(repoRoot);
  const timestamp = isoNow();
  const existing = existsSync(paths.logsPath) ? await readText(paths.logsPath) : "# Shared Brain Log\n";
  const entry = [
    "",
    `## [${timestamp}] ${operation} | ${subject} | ${shortTitle}`,
    "",
    ...detailLines.map((line) => (line.startsWith("- ") ? line : `- ${line}`))
  ].join("\n");
  await writeText(paths.logsPath, `${existing.trimEnd()}\n${entry}\n`);
  return toRepoRelative(repoRoot, paths.logsPath);
}

export async function createImportedArtifact(
  repoRoot: string,
  input: ImportArtifactInput
): Promise<CreateImportResult> {
  if (!input.source_harness || !input.source_machine || !input.source_type) {
    throw new Error("source_harness, source_machine, and source_type are required");
  }
  const paths = getRepoPaths(repoRoot);
  const title = deriveTitle(input);
  const bytes =
    input.bytes ||
    (input.text !== undefined
      ? new TextEncoder().encode(input.text)
      : (() => {
          throw new Error("No import payload provided");
        })());
  const sha256 = await hashBytes(bytes);
  const duplicate = await findSourceManifestBySha(paths, sha256);
  if (duplicate) {
    const logPath = await appendLogEntry(repoRoot, "import", duplicate.id, title, [
      `operation: import`,
      `inputs: duplicate sha256=${sha256}; source_machine=${input.source_machine}; source_harness=${input.source_harness}; source_type=${input.source_type}`,
      `files: [[${duplicate.raw_path}]], [[${duplicate.normalized_path || duplicate.raw_path}]], [[manifests/sources/${duplicate.id}.json]]`,
      `commit: pending`,
      `summary: Duplicate artifact detected. Reused existing canonical artifact ${duplicate.id}.`
    ]);
    return {
      artifactId: duplicate.id,
      manifest: duplicate,
      touchedFiles: [logPath],
      deduplicated: true,
      summary: `Reused existing artifact ${duplicate.id}`
    };
  }

  const fileName = input.fileName || `${slugify(title) || "artifact"}.md`;
  const mimeType = input.contentType || guessMimeFromPath(fileName);
  const artifactId = deriveArtifactId(sha256);
  const rawDir = chooseRawDirectory(paths, fileName, input, mimeType);
  const useExternalBlob = shouldStoreExternally(fileName, mimeType, bytes.byteLength);
  const rawFileName = useExternalBlob
    ? `${artifactId}.external.md`
    : `${artifactId}${extname(fileName) || (isTextLike(fileName, mimeType) ? ".md" : ".bin")}`;
  const rawAbsolutePath = join(rawDir, rawFileName);
  let normalizationInputPath = rawAbsolutePath;
  let externalBlobRelativePath: string | undefined;

  if (useExternalBlob) {
    const blobExtension = extname(fileName) || ".bin";
    externalBlobRelativePath = join(sha256.slice(0, 2), `${sha256}${blobExtension}`).split(sep).join("/");
    normalizationInputPath = join(blobStoreRoot(), externalBlobRelativePath);
    await ensureDir(dirname(normalizationInputPath));
    if (!existsSync(normalizationInputPath)) {
      await Bun.write(normalizationInputPath, bytes);
    }
    await writeText(
      rawAbsolutePath,
      [
        `# External raw artifact: ${title}`,
        "",
        "This raw artifact is larger than the configured git threshold and is stored outside the canonical git repo.",
        "",
        `- Artifact ID: \`${artifactId}\``,
        `- SHA256: \`${sha256}\``,
        `- Size bytes: \`${bytes.byteLength}\``,
        `- MIME type: \`${mimeType}\``,
        `- Blob store env: \`BRAIN_BLOB_STORE\``,
        `- Blob store relative path: \`${externalBlobRelativePath}\``,
        "",
        "The normalized extract and manifest remain in git. Restore the blob store from backups before expecting the original binary to be available.",
        ""
      ].join("\n")
    );
  } else {
    await ensureDir(dirname(rawAbsolutePath));
    await Bun.write(rawAbsolutePath, bytes);
  }

  const manifest: SourceManifest = {
    id: artifactId,
    title,
    created_at: isoNow(),
    updated_at: isoNow(),
    sha256,
    source_machine: input.source_machine,
    source_harness: input.source_harness,
    source_type: input.source_type,
    mime_type: mimeType,
    raw_path: toRepoRelative(repoRoot, rawAbsolutePath),
    storage: useExternalBlob ? "external" : "git",
    size_bytes: bytes.byteLength,
    external_blob_path: externalBlobRelativePath,
    external_blob_sha256: useExternalBlob ? sha256 : undefined,
    external_blob_size_bytes: useExternalBlob ? bytes.byteLength : undefined,
    related_project: input.related_project,
    related_repo: input.related_repo,
    tags: collectInputTags(input),
    conversation_id: input.conversation_id,
    source_url: input.url,
    original_filename: input.fileName
  };

  const originalText = !useExternalBlob && isTextLike(rawFileName, mimeType) ? new TextDecoder().decode(bytes) : undefined;
  const normalized = await normalizeArtifactFile(paths, manifest, normalizationInputPath, originalText);
  manifest.normalized_path = normalized.relativePath;
  manifest.page_path = `wiki/Sources/${artifactId}.md`;
  manifest.normalization_status = normalized.status;

  const manifestAbsolutePath = join(paths.sourceManifestDir, `${artifactId}.json`);
  await writeJson(manifestAbsolutePath, manifest);

  const logPath = await appendLogEntry(repoRoot, "import", artifactId, title, [
    `operation: import`,
    `inputs: source_machine=${input.source_machine}; source_harness=${input.source_harness}; source_type=${input.source_type}`,
    `files: [[${manifest.raw_path}]], [[${manifest.normalized_path}]], [[manifests/sources/${artifactId}.json]]`,
    `commit: pending`,
    `summary: Imported raw artifact, normalized it, and wrote the source manifest.`
  ]);

  return {
    artifactId,
    manifest,
    touchedFiles: [
      manifest.raw_path,
      manifest.normalized_path,
      toRepoRelative(repoRoot, manifestAbsolutePath),
      logPath
    ],
    deduplicated: false,
    summary: `Created artifact ${artifactId}`
  };
}

function sourcePageBody(manifest: SourceManifest, normalizedText: string, relatedLinks: string[]): string {
  const summary = normalizeTextSnippet(stripFrontmatter(normalizedText), 1200) || "No normalized text available.";
  return [
    `# ${manifest.title}`,
    "",
    "## Artifact",
    "",
    `- Raw: [[${manifest.raw_path}]]`,
    manifest.normalized_path ? `- Normalized: [[${manifest.normalized_path}]]` : "- Normalized: unavailable",
    manifest.source_url ? `- Source URL: ${manifest.source_url}` : "",
    manifest.related_repo ? `- Related repo: \`${manifest.related_repo}\`` : "",
    "",
    "## Summary",
    "",
    summary,
    "",
    relatedLinks.length ? "## Related pages" : "",
    ...(relatedLinks.length ? ["", ...relatedLinks, ""] : []),
    "## Metadata",
    "",
    `- Artifact id: \`${manifest.id}\``,
    `- Source machine: \`${manifest.source_machine}\``,
    `- Source harness: \`${manifest.source_harness}\``,
    `- Source type: \`${manifest.source_type}\``,
    `- SHA256: \`${manifest.sha256}\``,
    `- Normalization status: \`${manifest.normalization_status || "unknown"}\``,
    ""
  ]
    .filter(Boolean)
    .join("\n");
}

async function upsertPageSources(
  repoRoot: string,
  repoRelativePath: string,
  sourceId: string,
  bullet: string
): Promise<string> {
  const absolutePath = safeRepoPath(repoRoot, repoRelativePath);
  const existing = existsSync(absolutePath)
    ? await readText(absolutePath)
    : stringifyFrontmatter(
        {
          title: basename(repoRelativePath, extname(repoRelativePath)).replace(/-/g, " "),
          type: repoRelativePath.includes("/Projects/") ? "project" : "synthesis",
          created_at: isoNow(),
          updated_at: isoNow(),
          status: "active",
          tags: [],
          aliases: [],
          source_ids: [sourceId]
        },
        `# ${basename(repoRelativePath, extname(repoRelativePath)).replace(/-/g, " ")}\n`
      );
  const { data, body } = parseFrontmatter(existing);
  const sourceIds = uniqueStrings([...arrayField(data.source_ids), sourceId]);
  data.source_ids = sourceIds;
  data.updated_at = isoNow();
  const lines = body.trimEnd().split("\n");
  const sectionIndex = lines.findIndex((line) => line.trim() === "## Sources");
  let nextBodyLines = [...lines];
  if (sectionIndex === -1) {
    nextBodyLines = [...nextBodyLines, "", "## Sources", "", bullet];
  } else {
    const existingSectionEnd = nextBodyLines.findIndex(
      (line, index) => index > sectionIndex && /^##\s+/.test(line.trim())
    );
    const sectionLines = nextBodyLines.slice(
      sectionIndex + 1,
      existingSectionEnd === -1 ? nextBodyLines.length : existingSectionEnd
    );
    if (!sectionLines.some((line) => line.trim() === bullet.trim())) {
      nextBodyLines.splice(existingSectionEnd === -1 ? nextBodyLines.length : existingSectionEnd, 0, bullet);
    }
  }
  await writeText(absolutePath, stringifyFrontmatter(data, nextBodyLines.join("\n")));
  return repoRelativePath;
}

function pageTypeFromPath(repoRelativePath: string): string {
  const parts = normalizeRepoPath(repoRelativePath).split("/");
  return parts[1]?.replace(/s$/, "").toLowerCase() || "synthesis";
}

async function ensureProjectPage(repoRoot: string, title: string, sourceId: string): Promise<string> {
  const repoRelativePath = `wiki/Projects/${titleToPageStem(title) || "Project"}.md`;
  const absolutePath = safeRepoPath(repoRoot, repoRelativePath);
  if (!existsSync(absolutePath)) {
    const body = [
      `# ${title}`,
      "",
      "Project page created during ingest because an artifact explicitly referenced this project.",
      "",
      "## Status",
      "",
      "- Needs refinement",
      ""
    ].join("\n");
    await writeText(
      absolutePath,
      stringifyFrontmatter(
        {
          title,
          type: "project",
          created_at: isoNow(),
          updated_at: isoNow(),
          status: "active",
          tags: ["project"],
          aliases: [],
          source_ids: [sourceId]
        },
        body
      )
    );
  }
  await upsertPageSources(repoRoot, repoRelativePath, sourceId, `- [[Sources/${sourceId}|${sourceId}]]`);
  return repoRelativePath;
}

export async function rebuildIndex(repoRoot: string): Promise<{ indexedDocs: number; lastReindexAt: string }> {
  const paths = getRepoPaths(repoRoot);
  await ensureDir(paths.derivedDir);
  if (existsSync(paths.searchDbPath)) {
    await rm(paths.searchDbPath, { force: true });
  }
  const db = new Database(paths.searchDbPath);
  try {
    db.exec(`
      PRAGMA journal_mode = WAL;
      CREATE TABLE documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT NOT NULL UNIQUE,
        title TEXT NOT NULL,
        headings TEXT NOT NULL,
        body TEXT NOT NULL,
        scope TEXT NOT NULL,
        type TEXT NOT NULL,
        tags TEXT NOT NULL,
        source_ids TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
      CREATE VIRTUAL TABLE documents_fts USING fts5(
        path,
        title,
        headings,
        body,
        type,
        tags,
        source_ids,
        content='documents',
        content_rowid='id'
      );
      CREATE TABLE links (
        from_path TEXT NOT NULL,
        to_path TEXT NOT NULL
      );
      CREATE TABLE meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
    `);
    const insertDoc = db.prepare(`
      INSERT INTO documents (path, title, headings, body, scope, type, tags, source_ids, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertFts = db.prepare(`
      INSERT INTO documents_fts (rowid, path, title, headings, body, type, tags, source_ids)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insertLink = db.prepare(`INSERT INTO links (from_path, to_path) VALUES (?, ?)`);

    let indexedDocs = 0;
    const wikiFiles = (await listFiles(paths.wikiDir)).filter((file) => file.endsWith(".md")).sort();
    for (const absolutePath of wikiFiles) {
      const repoPath = toRepoRelative(repoRoot, absolutePath);
      const raw = await readText(absolutePath);
      const { data, body } = parseFrontmatter(raw);
      const title =
        typeof data.title === "string" ? data.title : findFirstHeading(raw) || inferTitleFromPath(repoPath);
      const headings = extractHeadings(raw).join("\n");
      const type = typeof data.type === "string" ? data.type : pageTypeFromPath(repoPath);
      const tags = arrayField(data.tags).join(", ");
      const sourceIds = arrayField(data.source_ids).join(", ");
      const updatedAt = typeof data.updated_at === "string" ? data.updated_at : isoNow();
      const result = insertDoc.run(repoPath, title, headings, body, "wiki", type, tags, sourceIds, updatedAt);
      insertFts.run(result.lastInsertRowid, repoPath, title, headings, body, type, tags, sourceIds);
      indexedDocs += 1;
      for (const link of extractWikiLinks(raw)) {
        const resolved = resolveInternalLinkPath(repoPath, link.target);
        if (resolved) {
          insertLink.run(repoPath, resolved);
        }
      }
      for (const link of extractMarkdownLinks(raw)) {
        const resolved = resolveInternalLinkPath(repoPath, link.target);
        if (resolved) {
          insertLink.run(repoPath, resolved);
        }
      }
    }

    const rawFiles = [
      ...(await listFiles(paths.normalizedDir)),
      ...(await listFiles(paths.importedDir)),
      ...(await listFiles(paths.conversationsDir))
    ]
      .filter((file, index, all) => all.indexOf(file) === index)
      .filter((file) => existsSync(file) && isTextLike(file, guessMimeFromPath(file)))
      .sort();

    for (const absolutePath of rawFiles) {
      const repoPath = toRepoRelative(repoRoot, absolutePath);
      const text = await readText(absolutePath);
      const title = findFirstHeading(text) || inferTitleFromPath(repoPath);
      const headings = extractHeadings(text).join("\n");
      const type = repoPath.startsWith("raw/normalized/") ? "normalized-artifact" : "raw-artifact";
      const updatedAt = (await stat(absolutePath)).mtime.toISOString().replace(/\.\d{3}Z$/, "Z");
      const result = insertDoc.run(repoPath, title, headings, text, "raw", type, "", "", updatedAt);
      insertFts.run(result.lastInsertRowid, repoPath, title, headings, text, type, "", "");
      indexedDocs += 1;
    }

    const lastReindexAt = isoNow();
    db.prepare(`INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)`).run("last_reindex_at", lastReindexAt);
    db.prepare(`INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)`).run("indexed_docs", String(indexedDocs));
    return { indexedDocs, lastReindexAt };
  } finally {
    db.close();
  }
}

function openDb(repoRoot: string): Database {
  const paths = getRepoPaths(repoRoot);
  if (!existsSync(paths.searchDbPath)) {
    throw new Error(`Search index missing at ${paths.searchDbPath}`);
  }
  return new Database(paths.searchDbPath, { readonly: true });
}

function ftsQuery(query: string): string {
  return query
    .trim()
    .split(/\s+/)
    .map((part) => `${part.replace(/"/g, '""')}*`)
    .join(" ");
}

export async function searchIndex(
  repoRoot: string,
  query: string,
  scope: "wiki" | "raw" | "all" = "all",
  limit = 10
): Promise<SearchResult[]> {
  const db = openDb(repoRoot);
  try {
    const stmt = db.prepare(`
      SELECT
        d.path AS path,
        d.title AS title,
        d.scope AS scope,
        d.type AS type,
        d.tags AS tags,
        d.source_ids AS source_ids,
        snippet(documents_fts, 3, '<mark>', '</mark>', ' ... ', 20) AS snippet,
        bm25(documents_fts, 10.0, 5.0, 3.0, 1.0, 1.0, 1.0, 1.0) AS score
      FROM documents_fts
      JOIN documents d ON d.id = documents_fts.rowid
      WHERE documents_fts MATCH ? AND (? = 'all' OR d.scope = ?)
      ORDER BY CASE WHEN d.scope = 'wiki' THEN 0 ELSE 1 END, score
      LIMIT ?
    `);
    return stmt
      .all(ftsQuery(query), scope, scope, limit)
      .map((row) => row as Record<string, string | number>)
      .map((row) => ({
        path: String(row.path),
        title: String(row.title),
        scope: String(row.scope) as "wiki" | "raw",
        type: String(row.type),
        tags: String(row.tags || "")
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean),
        source_ids: String(row.source_ids || "")
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean),
        snippet: String(row.snippet || ""),
        score: Number(row.score || 0)
      }));
  } finally {
    db.close();
  }
}

export async function backlinksForPath(repoRoot: string, repoRelativePath: string): Promise<string[]> {
  const db = openDb(repoRoot);
  try {
    const stmt = db.prepare(`SELECT DISTINCT from_path FROM links WHERE to_path = ? ORDER BY from_path ASC`);
    return stmt
      .all(repoRelativePath)
      .map((row) => row as Record<string, string>)
      .map((row) => row.from_path);
  } finally {
    db.close();
  }
}

export async function getHealth(repoRoot: string): Promise<HealthStatus> {
  const paths = getRepoPaths(repoRoot);
  let indexedDocs = 0;
  let lastReindexAt: string | null = null;
  if (existsSync(paths.searchDbPath)) {
    const db = new Database(paths.searchDbPath, { readonly: true });
    try {
      const indexed = db.prepare(`SELECT value FROM meta WHERE key = 'indexed_docs'`).get() as
        | { value: string }
        | undefined;
      const reindex = db.prepare(`SELECT value FROM meta WHERE key = 'last_reindex_at'`).get() as
        | { value: string }
        | undefined;
      indexedDocs = Number(indexed?.value || 0);
      lastReindexAt = reindex?.value || null;
    } finally {
      db.close();
    }
  }
  let commit: string | null = null;
  try {
    commit = runCommand(["git", "rev-parse", "HEAD"], repoRoot, false) || null;
  } catch {
    commit = null;
  }
  return {
    ok: true,
    commit,
    indexed_docs: indexedDocs,
    last_reindex_at: lastReindexAt,
    repo_root: repoRoot
  };
}

export async function getRecentLogEntries(repoRoot: string, limit = 10): Promise<RecentLogEntry[]> {
  const paths = getRepoPaths(repoRoot);
  if (!existsSync(paths.logsPath)) {
    return [];
  }
  const text = await readText(paths.logsPath);
  const lines = text.split("\n");
  const entries: RecentLogEntry[] = [];
  let current: RecentLogEntry | null = null;
  for (const line of lines) {
    const heading = line.match(/^## \[(.+?)\] ([^|]+) \| ([^|]+) \| (.+)$/);
    if (heading) {
      if (current) {
        entries.push(current);
      }
      current = {
        timestamp: heading[1],
        operation: heading[2].trim(),
        subject: heading[3].trim(),
        title: heading[4].trim(),
        body: []
      };
      continue;
    }
    if (current && line.trim()) {
      current.body.push(line.trim());
    }
  }
  if (current) {
    entries.push(current);
  }
  return entries.reverse().slice(0, limit);
}

export async function getRecentImports(repoRoot: string, limit = 10): Promise<SourceManifest[]> {
  const paths = getRepoPaths(repoRoot);
  const manifests = await Promise.all(
    (await findSourceManifestFiles(paths)).map((file) => readJson<SourceManifest>(file))
  );
  return manifests
    .sort((a, b) => b.created_at.localeCompare(a.created_at))
    .slice(0, limit);
}

function indexSection(title: string, pageLinks: string[]): string[] {
  return [title, "", ...pageLinks.map((page) => `- ${page}`), ""];
}

async function pageLinkFor(repoRoot: string, repoRelativePath: string): Promise<string> {
  const page = await readPage(repoRoot, repoRelativePath);
  const target = repoRelativePath.startsWith("wiki/") ? repoRelativePath.slice(5, -3) : repoRelativePath;
  return `[[${target}|${page.title}]]`;
}

export async function rebuildIndexPage(repoRoot: string, recentSourceIds: string[] = []): Promise<string> {
  const paths = getRepoPaths(repoRoot);
  const sections: Array<[string, string]> = [
    ["Decisions", "wiki/Decisions"],
    ["Projects", "wiki/Projects"],
    ["Machines", "wiki/Machines"],
    ["Harnesses", "wiki/Harnesses"],
    ["Skills", "wiki/Skills"],
    ["Runbooks", "wiki/Runbooks"],
    ["Syntheses", "wiki/Syntheses"],
    ["Sources", "wiki/Sources"]
  ];
  const bodyLines = ["# Shared Brain Index", ""];
  for (const [title, repoDir] of sections) {
    const absoluteDir = safeRepoPath(repoRoot, repoDir);
    const files = existsSync(absoluteDir)
      ? (await listFiles(absoluteDir))
          .filter((file) => file.endsWith(".md"))
          .sort((a, b) => basename(a).localeCompare(basename(b)))
      : [];
    const links = await Promise.all(files.map((file) => pageLinkFor(repoRoot, toRepoRelative(repoRoot, file))));
    bodyLines.push(...indexSection(`## ${title}`, links));
  }

  const sourceIds = uniqueStrings([
    "src-20260409-operator-brief",
    ...recentSourceIds.filter((id) => id !== "src-20260409-operator-brief")
  ]);
  const frontmatter: FrontmatterData = {
    title: "Shared Brain Index",
    type: "synthesis",
    created_at: "2026-04-09T08:59:00Z",
    updated_at: isoNow(),
    status: "active",
    tags: ["index", "generated"],
    aliases: ["Index"],
    source_ids: sourceIds
  };
  const indexPath = join(paths.wikiDir, "Index.md");
  await writeText(indexPath, stringifyFrontmatter(frontmatter, bodyLines.join("\n")));
  return toRepoRelative(repoRoot, indexPath);
}

export async function ingestArtifacts(repoRoot: string, artifactIds: string[]): Promise<IngestResult> {
  const paths = getRepoPaths(repoRoot);
  const uniqueIds = uniqueStrings(artifactIds);
  const touched = new Set<string>();
  const sourcePages: string[] = [];
  const relatedPages: string[] = [];

  for (const artifactId of uniqueIds) {
    const manifest = await findSourceManifestById(repoRoot, artifactId);
    if (!manifest) {
      throw new Error(`Unknown artifact id: ${artifactId}`);
    }
    const normalizedPath = manifest.normalized_path
      ? safeRepoPath(repoRoot, manifest.normalized_path)
      : safeRepoPath(repoRoot, manifest.raw_path);
    const normalizedText = existsSync(normalizedPath) ? await readText(normalizedPath) : `# ${manifest.title}\n`;
    const relatedLinks: string[] = [];

    const machinePagePath = await loadManifestPagePath(paths, "machines", manifest.source_machine);
    if (machinePagePath) {
      const page = await upsertPageSources(
        repoRoot,
        machinePagePath,
        artifactId,
        `- [[Sources/${artifactId}|${manifest.title}]] (${manifest.created_at})`
      );
      relatedPages.push(page);
      relatedLinks.push(`- [[${machinePagePath.slice(5, -3)}]]`);
      touched.add(page);
    }

    const harnessPagePath = await loadManifestPagePath(paths, "harnesses", manifest.source_harness);
    if (harnessPagePath) {
      const page = await upsertPageSources(
        repoRoot,
        harnessPagePath,
        artifactId,
        `- [[Sources/${artifactId}|${manifest.title}]] (${manifest.created_at})`
      );
      relatedPages.push(page);
      relatedLinks.push(`- [[${harnessPagePath.slice(5, -3)}]]`);
      touched.add(page);
    }

    if (manifest.related_project) {
      const projectPage = await ensureProjectPage(repoRoot, manifest.related_project, artifactId);
      relatedPages.push(projectPage);
      relatedLinks.push(`- [[${projectPage.slice(5, -3)}]]`);
      touched.add(projectPage);
    }

    const sourcePagePath = safeRepoPath(repoRoot, `wiki/Sources/${artifactId}.md`);
    const sourcePageFrontmatter: FrontmatterData = {
      title: manifest.title,
      type: "source",
      created_at: manifest.created_at,
      updated_at: isoNow(),
      status: "active",
      tags: uniqueStrings(["source", ...(manifest.tags || [])]),
      aliases: [artifactId],
      source_ids: [artifactId]
    };
    await writeText(
      sourcePagePath,
      stringifyFrontmatter(
        sourcePageFrontmatter,
        sourcePageBody(manifest, normalizedText, uniqueStrings(relatedLinks))
      )
    );
    sourcePages.push(toRepoRelative(repoRoot, sourcePagePath));
    touched.add(toRepoRelative(repoRoot, sourcePagePath));
  }

  const indexPath = await rebuildIndexPage(repoRoot, uniqueIds);
  touched.add(indexPath);
  const logPath = await appendLogEntry(repoRoot, "ingest", uniqueIds.join(","), "Compile sources into wiki", [
    `operation: ingest`,
    `inputs: artifact_ids=${uniqueIds.join(",")}`,
    `files: ${uniqueStrings([...sourcePages, ...relatedPages, indexPath]).map((path) => `[[${path}]]`).join(", ")}`,
    `commit: pending`,
    `summary: Updated source pages, related wiki pages, and the shared index.`
  ]);
  touched.add(logPath);

  return {
    artifactIds: uniqueIds,
    touchedFiles: Array.from(touched),
    sourcePages,
    relatedPages: uniqueStrings(relatedPages)
  };
}

export async function createProposal(
  repoRoot: string,
  input: {
    title: string;
    body: string;
    source_harness: string;
    source_machine: string;
    target_page?: string;
    tags?: string[];
  }
): Promise<{ proposalPath: string; touchedFiles: string[] }> {
  if (!input.title.trim() || !input.body.trim()) {
    throw new Error("Proposal title and body are required");
  }
  const paths = getRepoPaths(repoRoot);
  const fileName = `${isoNow().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z").toLowerCase()}-${slugify(input.title)}.md`;
  const absolutePath = join(paths.proposalsPendingDir, fileName);
  const frontmatter: FrontmatterData = {
    title: input.title.trim(),
    type: "proposal",
    created_at: isoNow(),
    updated_at: isoNow(),
    status: "pending",
    tags: uniqueStrings(["proposal", ...(input.tags || [])]),
    aliases: [],
    source_ids: []
  };
  const body = [
    `# ${input.title.trim()}`,
    "",
    `- Source harness: \`${input.source_harness}\``,
    `- Source machine: \`${input.source_machine}\``,
    input.target_page ? `- Target page: \`${input.target_page}\`` : "",
    "",
    "## Request",
    "",
    input.body.trim(),
    ""
  ]
    .filter(Boolean)
    .join("\n");
  await writeText(absolutePath, stringifyFrontmatter(frontmatter, body));
  const proposalPath = toRepoRelative(repoRoot, absolutePath);
  const logPath = await appendLogEntry(repoRoot, "propose", proposalPath, input.title.trim(), [
    `operation: propose`,
    `inputs: source_machine=${input.source_machine}; source_harness=${input.source_harness}`,
    `files: [[${proposalPath}]]`,
    `commit: pending`,
    `summary: Stored proposal in proposals/pending and left it unapplied.`
  ]);
  return { proposalPath, touchedFiles: [proposalPath, logPath] };
}

function findBrokenInternalLinks(pagePath: string, raw: string, knownPaths: Set<string>): string[] {
  const candidates = [
    ...extractWikiLinks(raw).map((link) => link.target),
    ...extractMarkdownLinks(raw).map((link) => link.target)
  ];
  const broken: string[] = [];
  for (const candidate of candidates) {
    const resolved = resolveInternalLinkPath(pagePath, candidate);
    if (resolved && !knownPaths.has(resolved)) {
      broken.push(`${pagePath} -> ${resolved}`);
    }
  }
  return broken;
}

function collectConcepts(raw: string): string[] {
  const words = raw.match(/\b[A-Z][A-Za-z0-9-]{3,}\b/g) || [];
  return words.filter((word) => !["This", "That", "With", "Shared", "Brain"].includes(word));
}

export async function lintWiki(repoRoot: string): Promise<LintResult> {
  const paths = getRepoPaths(repoRoot);
  const wikiFiles = (await listFiles(paths.wikiDir)).filter((file) => file.endsWith(".md")).sort();
  const knownPaths = new Set(wikiFiles.map((file) => toRepoRelative(repoRoot, file)));
  const backlinks = new Map<string, number>();
  const orphanPages: string[] = [];
  const brokenLinks: string[] = [];
  const staleClaims: string[] = [];
  const contradictions: string[] = [];
  const missingSources: string[] = [];
  const missingCrossLinks: string[] = [];
  const conceptCounter = new Map<string, number>();

  for (const file of wikiFiles) {
    const repoPath = toRepoRelative(repoRoot, file);
    const raw = await readText(file);
    const { data, body } = parseFrontmatter(raw);
    for (const target of [
      ...extractWikiLinks(raw).map((link) => resolveInternalLinkPath(repoPath, link.target)),
      ...extractMarkdownLinks(raw).map((link) => resolveInternalLinkPath(repoPath, link.target))
    ]) {
      if (target) {
        backlinks.set(target, (backlinks.get(target) || 0) + 1);
      }
    }
    brokenLinks.push(...findBrokenInternalLinks(repoPath, raw, knownPaths));

    const updatedAt = typeof data.updated_at === "string" ? data.updated_at : null;
    if (!updatedAt || new Date(updatedAt).getTime() < Date.now() - 180 * 24 * 60 * 60 * 1000) {
      staleClaims.push(repoPath);
    }

    const statusWords = ["active", "inactive", "deprecated", "paused"].filter((token) =>
      new RegExp(`\\b${token}\\b`, "i").test(raw)
    );
    if (statusWords.length > 1) {
      contradictions.push(`${repoPath} -> ${statusWords.join(", ")}`);
    }

    const sourceIds = arrayField(data.source_ids);
    if ((data.type as string) !== "source" && sourceIds.length === 0) {
      missingSources.push(repoPath);
    }

    const internalLinkCount = extractWikiLinks(raw).length + extractMarkdownLinks(raw).filter((link) => {
      const target = resolveInternalLinkPath(repoPath, link.target);
      return Boolean(target);
    }).length;
    if (internalLinkCount === 0 && body.trim().length > 240) {
      missingCrossLinks.push(repoPath);
    }

    for (const concept of collectConcepts(stripFrontmatter(raw))) {
      conceptCounter.set(concept, (conceptCounter.get(concept) || 0) + 1);
    }
  }

  for (const repoPath of knownPaths) {
    if (!backlinks.has(repoPath) && !["wiki/Home.md", "wiki/Index.md"].includes(repoPath)) {
      orphanPages.push(repoPath);
    }
  }

  const knownTitles = new Set(
    await Promise.all(
      Array.from(knownPaths).map(async (repoPath) => (await readPage(repoRoot, repoPath)).title)
    )
  );
  const missingConceptPages = Array.from(conceptCounter.entries())
    .filter(([concept, count]) => count >= 3 && !knownTitles.has(concept))
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)
    .map(([concept, count]) => `${concept} (${count})`);

  const reportName = `Lint-Report-${isoNow().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z")}.md`;
  const reportPath = join(paths.wikiDir, "Syntheses", reportName);
  const body = [
    `# Wiki lint report`,
    "",
    `Generated at ${isoNow()}.`,
    "",
    "## Orphan pages",
    "",
    ...(orphanPages.length ? orphanPages.map((item) => `- [[${item.slice(5, -3)}]]`) : ["- none"]),
    "",
    "## Broken links",
    "",
    ...(brokenLinks.length ? brokenLinks.map((item) => `- ${item}`) : ["- none"]),
    "",
    "## Possible stale claims",
    "",
    ...(staleClaims.length ? staleClaims.map((item) => `- [[${item.slice(5, -3)}]]`) : ["- none"]),
    "",
    "## Possible contradictions",
    "",
    ...(contradictions.length ? contradictions.map((item) => `- ${item}`) : ["- none"]),
    "",
    "## Missing cross-links",
    "",
    ...(missingCrossLinks.length ? missingCrossLinks.map((item) => `- [[${item.slice(5, -3)}]]`) : ["- none"]),
    "",
    "## Pages without sources",
    "",
    ...(missingSources.length ? missingSources.map((item) => `- [[${item.slice(5, -3)}]]`) : ["- none"]),
    "",
    "## Repeated concepts without pages",
    "",
    ...(missingConceptPages.length ? missingConceptPages.map((item) => `- ${item}`) : ["- none"]),
    ""
  ].join("\n");

  await writeText(
    reportPath,
    stringifyFrontmatter(
      {
        title: "Wiki lint report",
        type: "synthesis",
        created_at: isoNow(),
        updated_at: isoNow(),
        status: "active",
        tags: ["lint", "report"],
        aliases: [],
        source_ids: []
      },
      body
    )
  );
  const relativeReportPath = toRepoRelative(repoRoot, reportPath);
  const indexPath = await rebuildIndexPage(repoRoot);
  const logPath = await appendLogEntry(repoRoot, "lint", relativeReportPath, "Wiki health pass", [
    `operation: lint`,
    `inputs: repo_root=${repoRoot}`,
    `files: [[${relativeReportPath}]], [[${indexPath}]]`,
    `commit: pending`,
    `summary: Wrote a wiki health report covering orphans, broken links, stale claims, contradictions, missing cross-links, and source gaps.`
  ]);

  return { reportPath: relativeReportPath, touchedFiles: [relativeReportPath, indexPath, logPath] };
}

export async function withRepoLock<T>(repoRoot: string, fn: () => Promise<T>): Promise<T> {
  const { lockDir } = getRepoPaths(repoRoot);
  const startedAt = Date.now();
  while (true) {
    try {
      await mkdir(lockDir);
      break;
    } catch (error) {
      if (existsSync(lockDir)) {
        const info = await stat(lockDir);
        if (Date.now() - info.mtimeMs > 60_000) {
          await rm(lockDir, { recursive: true, force: true });
          continue;
        }
      }
      if (Date.now() - startedAt > 30_000) {
        throw new Error(`Timed out waiting for shared-brain repo lock at ${lockDir}`);
      }
      await Bun.sleep(200);
    }
  }
  try {
    return await fn();
  } finally {
    await rm(lockDir, { recursive: true, force: true });
  }
}

export async function ensureGitIdentity(repoRoot: string): Promise<void> {
  const name = runCommand(["git", "config", "--get", "user.name"], repoRoot, false);
  const email = runCommand(["git", "config", "--get", "user.email"], repoRoot, false);
  if (!name) {
    runCommand(["git", "config", "user.name", "Shared Brain Organizer"], repoRoot);
  }
  if (!email) {
    runCommand(["git", "config", "user.email", "shared-brain@valkyrie.local"], repoRoot);
  }
}

export async function gitCommitAndPush(repoRoot: string, touchedFiles: string[], message: string): Promise<string | null> {
  await ensureGitIdentity(repoRoot);
  const uniqueFiles = uniqueStrings(touchedFiles);
  if (!uniqueFiles.length) {
    return null;
  }
  runCommand(["git", "add", "-A", "--", ...uniqueFiles], repoRoot);
  const changed = runCommand(["git", "status", "--porcelain", "--", ...uniqueFiles], repoRoot, false);
  if (!changed.trim()) {
    return null;
  }
  runCommand(["git", "commit", "-m", message], repoRoot);
  const commit = runCommand(["git", "rev-parse", "HEAD"], repoRoot);
  runCommand(["git", "push", "origin", "HEAD:main"], repoRoot);
  return commit;
}

export async function readEnvFile(path: string): Promise<Record<string, string>> {
  if (!existsSync(path)) {
    return {};
  }
  const content = await readText(path);
  const values: Record<string, string> = {};
  for (const rawLine of content.split("\n")) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }
    const separator = line.indexOf("=");
    if (separator === -1) {
      continue;
    }
    const key = line.slice(0, separator).trim();
    const value = line.slice(separator + 1).trim().replace(/^['"]|['"]$/g, "");
    values[key] = value;
  }
  return values;
}

export async function findClientConfigPath(): Promise<string | null> {
  const candidates = [
    process.env.SHARED_BRAIN_ENV_PATH,
    join(process.env.HOME || "", ".config", "shared-brain.env"),
    "/etc/shared-brain.env"
  ].filter(Boolean) as string[];
  for (const candidate of candidates) {
    if (existsSync(candidate)) {
      return candidate;
    }
  }
  return null;
}
