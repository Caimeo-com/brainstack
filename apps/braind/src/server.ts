import { existsSync } from "node:fs";
import { mkdir, readFile, rename, rm, rmdir, writeFile, readdir } from "node:fs/promises";
import { lookup } from "node:dns/promises";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { isIP } from "node:net";
import { createHash, randomUUID } from "node:crypto";
import { basename, dirname, extname, join, resolve } from "node:path";
import {
  backlinksForPath,
  createImportedArtifact,
  createProposal,
  findSourceManifestById,
  getHealth,
  getRecentImports,
  getRecentLogEntries,
  getRepoRoot,
  getRepoPaths,
  gitCommitAndPush,
  ingestArtifacts,
  isoNow,
  lintWiki,
  parseFrontmatter,
  readPage,
  rebuildIndex,
  resolveInternalLinkPath,
  safeRepoPath,
  searchIndex,
  slugify,
  stripFrontmatter,
  syncWritableRepoAsync,
  withRepoLock
} from "./brain-lib";

class HttpError extends Error {
  constructor(
    public readonly status: number,
    message: string
  ) {
    super(message);
  }
}

function expandHome(input: string): string {
  if (input === "~") {
    return process.env.HOME || input;
  }
  if (input.startsWith("~/")) {
    return resolve(process.env.HOME || ".", input.slice(2));
  }
  return input;
}

function defaultRepoRoot(): string {
  const home = process.env.HOME || "/home/brainstack";
  const serveClone = resolve(home, "shared-brain", "serve", "shared-brain");
  const legacyLiveClone = resolve(home, "shared-brain", "live", "shared-brain");
  return existsSync(serveClone) ? serveClone : legacyLiveClone;
}

const repoRoot = getRepoRoot(
  expandHome(
    process.env.SHARED_BRAIN_REPO_ROOT ||
      process.env.BRAINSTACK_SHARED_BRAIN_SERVE_REPO ||
      defaultRepoRoot()
  )
);
const writeRepoRoot = getRepoRoot(
  expandHome(
    process.env.SHARED_BRAIN_WRITE_REPO_ROOT ||
      process.env.BRAINSTACK_SHARED_BRAIN_STAGING_REPO ||
      repoRoot
  )
);
const rawLegacyWriteToken = process.env.BRAIN_WRITE_TOKEN || "";
const legacyWriteToken = rawLegacyWriteToken.trim();
function readTokenEnv(name: "BRAIN_IMPORT_TOKEN" | "BRAIN_ADMIN_TOKEN"): string {
  const raw = process.env[name] || "";
  const trimmed = raw.trim();
  if (raw && raw !== trimmed) {
    throw new Error(`${name} must not contain leading or trailing whitespace.`);
  }
  return trimmed;
}
const importToken = readTokenEnv("BRAIN_IMPORT_TOKEN");
const adminToken = readTokenEnv("BRAIN_ADMIN_TOKEN");
const host = process.env.BRAIN_BIND || "127.0.0.1";
const port = Number(process.env.BRAIN_PORT || 8080);
const configuredMaxImportBytes = Number(process.env.BRAIN_MAX_IMPORT_BYTES || "");
const maxImportBytes =
  Number.isFinite(configuredMaxImportBytes) && configuredMaxImportBytes > 0
    ? configuredMaxImportBytes
    : 25 * 1024 * 1024;
const configuredUrlFetchTimeoutMs = Number(process.env.BRAIN_URL_FETCH_TIMEOUT_MS || "");
const urlFetchTimeoutMs =
  Number.isFinite(configuredUrlFetchTimeoutMs) && configuredUrlFetchTimeoutMs > 0
    ? configuredUrlFetchTimeoutMs
    : 15_000;
const organizerLabel = process.env.BRAIN_ORGANIZER_LABEL || "organizer";
const allowPrivateUrlImports = ["1", "true", "yes", "on"].includes(
  (process.env.BRAIN_ALLOW_PRIVATE_URL_IMPORTS || "").toLowerCase()
);
const configuredMaxJsonBytes = Number(process.env.BRAIN_MAX_JSON_BYTES || "");
const maxJsonBytes =
  Number.isFinite(configuredMaxJsonBytes) && configuredMaxJsonBytes > 0
    ? configuredMaxJsonBytes
    : 1024 * 1024;
const configuredWriteConcurrency = Number(process.env.BRAIN_WRITE_CONCURRENCY || "");
const writeConcurrencyLimit =
  Number.isFinite(configuredWriteConcurrency) && configuredWriteConcurrency > 0
    ? Math.trunc(configuredWriteConcurrency)
    : 1;
const configuredRateLimit = Number(process.env.BRAIN_WRITE_RATE_LIMIT_PER_MINUTE || "");
const writeRateLimitPerMinute =
  Number.isFinite(configuredRateLimit) && configuredRateLimit > 0
    ? Math.trunc(configuredRateLimit)
    : 60;
const configuredReindexTimeoutMs = Number(process.env.BRAIN_REINDEX_TIMEOUT_MS || "");
const reindexTimeoutMs =
  Number.isFinite(configuredReindexTimeoutMs) && configuredReindexTimeoutMs > 0
    ? Math.trunc(configuredReindexTimeoutMs)
    : 120_000;
const maxSearchLimit = 50;
const reindexWorkerPath = join(import.meta.dir, "reindex-worker.ts");
const pendingReindexPath = join(repoRoot, "derived", "search-reindex-needed.json");
let searchRefreshPromise: Promise<void> | null = null;
let searchRefreshRequestedCommit: string | null = null;
let activeWriteRequests = 0;
const writeRateEvents = new Map<string, number[]>();

if (legacyWriteToken || (rawLegacyWriteToken && rawLegacyWriteToken !== legacyWriteToken)) {
  throw new Error("BRAIN_WRITE_TOKEN is no longer accepted by braind; set distinct BRAIN_IMPORT_TOKEN and BRAIN_ADMIN_TOKEN.");
}
if (!importToken || !adminToken) {
  throw new Error("braind requires explicit BRAIN_IMPORT_TOKEN and BRAIN_ADMIN_TOKEN.");
}
if (importToken === adminToken) {
  throw new Error("BRAIN_IMPORT_TOKEN and BRAIN_ADMIN_TOKEN must be distinct.");
}

async function currentRepoCommit(root: string): Promise<string | null> {
  const proc = Bun.spawn(["git", "rev-parse", "HEAD"], { cwd: root, stdout: "pipe", stderr: "pipe" });
  const [stdout, exitCode] = await Promise.all([new Response(proc.stdout).text(), proc.exited]);
  return exitCode === 0 ? stdout.trim() : null;
}

async function ensureFreshStartupIndex(): Promise<void> {
  const health = existsSync(join(repoRoot, "derived", "search.sqlite")) ? await getHealth(repoRoot) : null;
  const currentCommit = await currentRepoCommit(repoRoot);
  if (!health || (currentCommit && health.indexed_commit !== currentCommit)) {
    await rebuildIndex(repoRoot);
  }
  await rm(pendingReindexPath, { force: true }).catch(() => undefined);
}

await ensureFreshStartupIndex();

function htmlEscape(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function securityHeaders(contentType: string): Headers {
  return new Headers({
    "Content-Type": contentType,
    "X-Content-Type-Options": "nosniff",
    "Content-Security-Policy": [
      "default-src 'none'",
      "script-src 'none'",
      "style-src 'unsafe-inline'",
      "img-src 'self' data:",
      "form-action 'self'",
      "base-uri 'none'",
      "object-src 'none'",
      "frame-ancestors 'none'"
    ].join("; ")
  });
}

function pageHref(repoPath: string): string {
  return `/page/${encodeURIComponent(repoPath).replace(/%2F/g, "/")}`;
}

function rawHref(repoPath: string): string {
  return `/raw/${encodeURIComponent(repoPath).replace(/%2F/g, "/")}`;
}

function isUnsafePublicRepoPath(repoPath: string): boolean {
  return repoPath
    .replace(/^\/+/, "")
    .split("/")
    .some((part) => !part || part === "." || part === ".." || part.startsWith("."));
}

function assertPublicPagePath(repoPath: string): void {
  const normalized = repoPath.replace(/^\/+/, "");
  if (isUnsafePublicRepoPath(normalized) || !normalized.startsWith("wiki/") || !normalized.endsWith(".md")) {
    reject(403, `Page path is not public: ${repoPath}`);
  }
}

function assertPublicRawPath(repoPath: string): void {
  const normalized = repoPath.replace(/^\/+/, "");
  const allowedRoots = [
    "raw/imported/",
    "raw/conversations/",
    "raw/assets/",
    "raw/normalized/",
    "proposals/pending/",
    "proposals/applied/"
  ];
  if (isUnsafePublicRepoPath(normalized) || !allowedRoots.some((root) => normalized.startsWith(root))) {
    reject(403, `Raw path is not public: ${repoPath}`);
  }
}

function attachmentFileName(repoPath: string, suffix = ""): string {
  return `${basename(repoPath).replace(/[^A-Za-z0-9._-]+/g, "_") || "artifact"}${suffix}`;
}

function safeExternalHref(target: string): string | null {
  const trimmed = target.trim();
  try {
    const parsed = new URL(trimmed);
    return parsed.protocol === "http:" || parsed.protocol === "https:" || parsed.protocol === "mailto:" ? trimmed : null;
  } catch {
    return null;
  }
}

function hasUrlScheme(target: string): boolean {
  return /^[a-z][a-z0-9+.-]*:/i.test(target.trim());
}

function isProtocolRelativeUrl(target: string): boolean {
  return target.trim().startsWith("//");
}

function hasControlCharacters(target: string): boolean {
  return /[\u0000-\u001f\u007f]/.test(target);
}

function renderInline(text: string, currentPath: string): string {
  let html = htmlEscape(text);
  html = html.replace(/`([^`]+)`/g, (_match, code) => `<code>${htmlEscape(code)}</code>`);
  html = html.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  html = html.replace(/\[\[([^\]|]+)(?:\|([^\]]+))?\]\]/g, (_match, target, label) => {
    const resolved = resolveInternalLinkPath(currentPath, target);
    if (!resolved) {
      return htmlEscape(label || target);
    }
    return `<a href="${pageHref(resolved)}">${htmlEscape(label || target)}</a>`;
  });
  html = html.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_match, label, target) => {
    if (isProtocolRelativeUrl(target) || hasControlCharacters(target)) {
      return htmlEscape(label);
    }
    if (hasUrlScheme(target)) {
      const safeTarget = safeExternalHref(target);
      return safeTarget
        ? `<a href="${htmlEscape(safeTarget)}" target="_blank" rel="noreferrer noopener">${htmlEscape(label)}</a>`
        : htmlEscape(label);
    }
    const resolved = resolveInternalLinkPath(currentPath, target);
    if (resolved) {
      return `<a href="${pageHref(resolved)}">${htmlEscape(label)}</a>`;
    }
    return htmlEscape(label);
  });
  return html;
}

function renderMarkdown(markdown: string, currentPath: string): string {
  const lines = markdown.split("\n");
  let html = "";
  let inCode = false;
  let listOpen = false;
  for (const line of lines) {
    if (line.startsWith("```")) {
      if (inCode) {
        html += "</code></pre>";
        inCode = false;
      } else {
        if (listOpen) {
          html += "</ul>";
          listOpen = false;
        }
        html += "<pre><code>";
        inCode = true;
      }
      continue;
    }
    if (inCode) {
      html += `${htmlEscape(line)}\n`;
      continue;
    }
    if (!line.trim()) {
      if (listOpen) {
        html += "</ul>";
        listOpen = false;
      }
      continue;
    }
    const heading = line.match(/^(#{1,6})\s+(.+)$/);
    if (heading) {
      if (listOpen) {
        html += "</ul>";
        listOpen = false;
      }
      const level = heading[1].length;
      html += `<h${level}>${renderInline(heading[2], currentPath)}</h${level}>`;
      continue;
    }
    const listItem = line.match(/^- (.+)$/);
    if (listItem) {
      if (!listOpen) {
        html += "<ul>";
        listOpen = true;
      }
      html += `<li>${renderInline(listItem[1], currentPath)}</li>`;
      continue;
    }
    const blockquote = line.match(/^> (.+)$/);
    if (blockquote) {
      if (listOpen) {
        html += "</ul>";
        listOpen = false;
      }
      html += `<blockquote><p>${renderInline(blockquote[1], currentPath)}</p></blockquote>`;
      continue;
    }
    if (listOpen) {
      html += "</ul>";
      listOpen = false;
    }
    html += `<p>${renderInline(line, currentPath)}</p>`;
  }
  if (listOpen) {
    html += "</ul>";
  }
  if (inCode) {
    html += "</code></pre>";
  }
  return html;
}

function layout(title: string, body: string): Response {
  const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${htmlEscape(title)}</title>
  <style>
    :root {
      --bg: #f4efe4;
      --paper: #fffaf1;
      --ink: #1f1c17;
      --muted: #6d6558;
      --line: #d5c8b1;
      --accent: #0f5f54;
      --accent-soft: #dcefe7;
      --shadow: rgba(38, 29, 11, 0.08);
    }
    body {
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", Palatino, serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(15, 95, 84, 0.12), transparent 32%),
        linear-gradient(180deg, #f8f2e8 0%, var(--bg) 100%);
    }
    a { color: var(--accent); text-decoration-thickness: 1px; }
    code, pre {
      font-family: "SFMono-Regular", "JetBrains Mono", "Cascadia Code", Monaco, monospace;
      background: #f1e8d8;
      border-radius: 0.3rem;
    }
    code { padding: 0.1rem 0.3rem; }
    pre { padding: 1rem; overflow-x: auto; }
    .shell {
      max-width: 1100px;
      margin: 0 auto;
      padding: 2.5rem 1.25rem 4rem;
    }
    .masthead {
      display: grid;
      gap: 1rem;
      padding: 1.25rem 1.4rem;
      border: 1px solid var(--line);
      border-radius: 1.1rem;
      background: rgba(255, 250, 241, 0.9);
      box-shadow: 0 12px 32px var(--shadow);
    }
    .masthead h1 { margin: 0; font-size: clamp(2rem, 4vw, 3rem); }
    .masthead p { margin: 0; color: var(--muted); max-width: 70ch; }
    .search {
      display: flex;
      gap: 0.75rem;
      flex-wrap: wrap;
      margin-top: 0.6rem;
    }
    .search input, .search select, .search button {
      padding: 0.8rem 0.95rem;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: var(--paper);
      font: inherit;
    }
    .search input { min-width: min(34rem, 100%); }
    .search button {
      background: var(--accent);
      color: white;
      cursor: pointer;
    }
    .grid {
      display: grid;
      gap: 1rem;
      margin-top: 1.2rem;
    }
    .cols {
      grid-template-columns: 2fr 1fr;
    }
    .card {
      padding: 1rem 1.1rem;
      border: 1px solid var(--line);
      border-radius: 1rem;
      background: rgba(255, 250, 241, 0.92);
      box-shadow: 0 10px 24px var(--shadow);
    }
    .card h2, .card h3 { margin-top: 0; }
    .status {
      display: grid;
      gap: 0.4rem;
      font-size: 0.95rem;
      color: var(--muted);
    }
    .pill {
      display: inline-block;
      padding: 0.15rem 0.45rem;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 0.85rem;
      margin-right: 0.35rem;
      margin-bottom: 0.35rem;
    }
    .crumbs {
      color: var(--muted);
      font-size: 0.95rem;
      margin-bottom: 0.8rem;
    }
    .crumbs span { margin-right: 0.4rem; }
    .content p, .content li { line-height: 1.55; }
    .meta-list { list-style: none; padding: 0; margin: 0; }
    .meta-list li { margin-bottom: 0.4rem; color: var(--muted); }
    @media (max-width: 900px) {
      .cols { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <main class="shell">
    ${body}
  </main>
</body>
</html>`;
  return new Response(html, { headers: securityHeaders("text/html; charset=utf-8") });
}

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: securityHeaders("application/json; charset=utf-8")
  });
}

function textResponse(body: string, status = 200): Response {
  return new Response(body, { status, headers: securityHeaders("text/plain; charset=utf-8") });
}

function errorResponse(status: number, message: string): Response {
  return layout(`Error ${status}`, `<div class="masthead"><h1>Error ${status}</h1><p>${htmlEscape(message)}</p></div>`);
}

function wantsJson(request: Request, url: URL): boolean {
  return url.searchParams.get("format") === "json" || request.headers.get("accept")?.includes("application/json") === true;
}

function reject(status: number, message: string): never {
  throw new HttpError(status, message);
}

function sha256Text(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (value instanceof Uint8Array) {
    return JSON.stringify({ __bytes_sha256: createHash("sha256").update(value).digest("hex"), byteLength: value.byteLength });
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  }
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
    .join(",")}}`;
}

function requestHashFor(value: unknown): string {
  return sha256Text(canonicalJson(value));
}

interface IdempotencyRecord {
  endpoint: "import" | "propose";
  key_hash: string;
  request_hash: string;
  status: "claimed" | "running" | "complete" | "failed";
  created_at: string;
  updated_at: string;
  lease_until?: string;
  side_effect_started_at?: string;
  error?: string;
  response_body?: Record<string, unknown>;
}

async function readJsonFile<T>(path: string): Promise<T | null> {
  try {
    return JSON.parse(await readFile(path, "utf8")) as T;
  } catch {
    return null;
  }
}

async function writeJsonFile(path: string, value: unknown, options?: { createOnly?: boolean }): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const text = `${JSON.stringify(value, null, 2)}\n`;
  if (options?.createOnly) {
    await writeFile(path, text, { encoding: "utf8", flag: "wx" });
    return;
  }
  const tempPath = `${path}.${process.pid}.${randomUUID()}.tmp`;
  await writeFile(tempPath, text, { encoding: "utf8", flag: "wx" });
  await rename(tempPath, path);
}

function errorCode(error: unknown): string | null {
  return error && typeof error === "object" && "code" in error ? String((error as { code?: unknown }).code) : null;
}

async function withIdempotency(
  request: Request,
  endpoint: "import" | "propose",
  requestHash: string,
  run: (markSideEffectStarted: () => Promise<void>) => Promise<Record<string, unknown>>
): Promise<Record<string, unknown>> {
  const rawKey = request.headers.get("idempotency-key")?.trim();
  if (!rawKey) {
    return await run(async () => undefined);
  }
  if (rawKey.length > 200) {
    reject(400, "Idempotency-Key is too long");
  }

  const paths = getRepoPaths(writeRepoRoot);
  const keyHash = sha256Text(rawKey);
  const recordPath = join(paths.idempotencyDir, endpoint, `${keyHash}.json`);
  const lockDir = `${recordPath}.lock`;
  await mkdir(dirname(recordPath), { recursive: true });
  const lockToken = randomUUID();
  const lockOwnerPath = join(lockDir, `owner-${lockToken}.json`);
  const lockTokenPath = join(lockDir, `release-${lockToken}`);
  const lockStartedAt = Date.now();
  while (true) {
    try {
      await mkdir(lockDir);
      try {
        await writeJsonFile(lockOwnerPath, { token: lockToken, pid: process.pid, created_at: isoNow() }, { createOnly: true });
        await writeFile(lockTokenPath, lockToken, { encoding: "utf8", flag: "wx" });
      } catch (error) {
        await rm(lockDir, { recursive: true, force: true }).catch(() => undefined);
        throw error;
      }
      break;
    } catch (error) {
      if (errorCode(error) !== "EEXIST") {
        throw error;
      }
      if (Date.now() - lockStartedAt > 30_000) {
        reject(425, "Idempotent request is already in progress; retry later or inspect the idempotency lock if this persists");
      }
      await Bun.sleep(100);
    }
  }
  const now = isoNow();
  const pending: IdempotencyRecord = {
    endpoint,
    key_hash: keyHash,
    request_hash: requestHash,
    status: "claimed",
    created_at: now,
    updated_at: now,
    lease_until: new Date(Date.now() + 60_000).toISOString()
  };

  try {
    try {
      await writeJsonFile(recordPath, pending, { createOnly: true });
    } catch (error) {
      if (errorCode(error) !== "EEXIST") {
        throw error;
      }
      const existing = await readJsonFile<IdempotencyRecord>(recordPath);
      if (!existing || existing.endpoint !== endpoint) {
        reject(409, "Idempotency-Key record is malformed; retry with a new key");
      }
      if (existing.request_hash !== requestHash) {
        reject(409, "Idempotency-Key was already used for a different request");
      }
      if (existing.status === "complete" && existing.response_body) {
        return { ...existing.response_body, idempotent_replay: true };
      }
      const leaseExpired = existing.lease_until ? Date.parse(existing.lease_until) < Date.now() : false;
      if (existing.status === "claimed" && leaseExpired && !existing.side_effect_started_at) {
        await writeJsonFile(recordPath, pending);
      } else if (existing.status === "failed") {
        reject(409, "Idempotent request failed previously; retry with a new Idempotency-Key after operator review");
      } else {
        reject(425, "Idempotent request is already in progress or requires operator review; retry later");
      }
    }

    let sideEffectStarted = false;
    let activeRecord = pending;
    const markSideEffectStarted = async (): Promise<void> => {
      if (sideEffectStarted) {
        return;
      }
      sideEffectStarted = true;
      activeRecord = {
        ...pending,
        status: "running",
        updated_at: isoNow(),
        lease_until: new Date(Date.now() + 30 * 60_000).toISOString(),
        side_effect_started_at: isoNow()
      };
      await writeJsonFile(recordPath, activeRecord);
    };

    try {
      const response = await run(markSideEffectStarted);
      const complete: IdempotencyRecord = {
        ...activeRecord,
        status: "complete",
        updated_at: isoNow(),
        response_body: response
      };
      await writeJsonFile(recordPath, complete);
      return response;
    } catch (error) {
      if (sideEffectStarted) {
        await writeJsonFile(recordPath, {
          ...activeRecord,
          status: "failed",
          updated_at: isoNow(),
          error: error instanceof Error ? error.message : String(error)
        }).catch(() => undefined);
      } else {
        await rm(recordPath, { force: true }).catch(() => undefined);
      }
      throw error;
    }
  } finally {
    const owner = await readJsonFile<{ token?: string }>(lockOwnerPath);
    if (owner?.token === lockToken && existsSync(lockTokenPath)) {
      await rm(lockTokenPath, { force: true }).catch(() => undefined);
      await rm(lockOwnerPath, { force: true }).catch(() => undefined);
      await rmdir(lockDir).catch(() => undefined);
    }
  }
}

function assertImportSize(size: number, label: string): void {
  if (size > maxImportBytes) {
    reject(413, `${label} is too large: ${size} bytes exceeds BRAIN_MAX_IMPORT_BYTES=${maxImportBytes}`);
  }
}

function assertContentLength(request: Request, maxBytes: number, label: string): void {
  const rawLength = request.headers.get("content-length");
  if (!rawLength) {
    return;
  }
  const length = Number(rawLength);
  if (!Number.isFinite(length) || length < 0) {
    reject(400, `${label} has invalid content-length`);
  }
  if (length > maxBytes) {
    reject(413, `${label} is too large: content-length ${length} exceeds ${maxBytes}`);
  }
}

function requireContentLength(request: Request, maxBytes: number, label: string): void {
  if (!request.headers.get("content-length")) {
    reject(411, `${label} requires content-length`);
  }
  assertContentLength(request, maxBytes, label);
}

async function readRequestBytesCapped(request: Request, label: string, maxBytes: number): Promise<Uint8Array> {
  assertContentLength(request, maxBytes, label);
  if (!request.body) {
    return new Uint8Array();
  }

  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    if (!value) {
      continue;
    }
    total += value.byteLength;
    if (total > maxBytes) {
      await reader.cancel().catch(() => undefined);
      reject(413, `${label} is too large: streamed body exceeds ${maxBytes}`);
    }
    chunks.push(value);
  }

  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return merged;
}

async function readJsonBody(request: Request): Promise<Record<string, unknown>> {
  const bytes = await readRequestBytesCapped(request, "JSON request body", maxJsonBytes);
  if (!bytes.byteLength) {
    return {};
  }
  try {
    const parsed = JSON.parse(new TextDecoder().decode(bytes)) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : {};
  } catch {
    reject(400, "Request body must be valid JSON");
  }
}

function ipv4ToNumber(address: string): number {
  return address.split(".").reduce((sum, part) => (sum << 8) + Number(part), 0) >>> 0;
}

function inCidr4(address: string, base: string, bits: number): boolean {
  const mask = bits === 0 ? 0 : (0xffffffff << (32 - bits)) >>> 0;
  return (ipv4ToNumber(address) & mask) === (ipv4ToNumber(base) & mask);
}

function isBlockedIp(address: string): boolean {
  const version = isIP(address);
  if (version === 4) {
    return [
      ["0.0.0.0", 8],
      ["10.0.0.0", 8],
      ["100.64.0.0", 10],
      ["127.0.0.0", 8],
      ["169.254.0.0", 16],
      ["172.16.0.0", 12],
      ["192.168.0.0", 16]
    ].some(([base, bits]) => inCidr4(address, String(base), Number(bits)));
  }
  if (version === 6) {
    const normalized = address.toLowerCase();
    const ipv4Mapped = normalized.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/);
    if (ipv4Mapped) {
      return isBlockedIp(ipv4Mapped[1]);
    }
    return (
      normalized === "::1" ||
      normalized.startsWith("fe80:") ||
      normalized.startsWith("fc") ||
      normalized.startsWith("fd")
    );
  }
  return false;
}

interface SafeImportTarget {
  url: URL;
  address: string;
  family: 4 | 6;
}

async function assertSafeImportUrl(input: string): Promise<SafeImportTarget> {
  let url: URL;
  try {
    url = new URL(input);
  } catch {
    reject(400, "URL import requires a valid URL");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    reject(400, "URL import only allows http and https URLs");
  }
  const hostname = url.hostname.toLowerCase();
  if (!allowPrivateUrlImports && (hostname === "localhost" || hostname.endsWith(".localhost"))) {
    reject(400, "URL import blocked localhost hostname; set BRAIN_ALLOW_PRIVATE_URL_IMPORTS=true only for trusted private fetches");
  }
  const directIpVersion = isIP(hostname);
  let addresses: Array<{ address: string; family?: number }>;
  try {
    addresses = directIpVersion
      ? [{ address: hostname, family: directIpVersion }]
      : await lookup(hostname, { all: true, verbatim: true });
  } catch (error) {
    reject(400, `URL import DNS lookup failed: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!addresses.length) {
    reject(400, "URL import DNS lookup returned no addresses");
  }
  if (!allowPrivateUrlImports) {
    const blocked = addresses.find((entry) => isBlockedIp(entry.address));
    if (blocked) {
      reject(400, `URL import blocked private address ${blocked.address}; set BRAIN_ALLOW_PRIVATE_URL_IMPORTS=true only for trusted private fetches`);
    }
  }
  const selected = addresses[0];
  const family = selected.family === 6 ? 6 : 4;
  return { url, address: selected.address, family };
}

async function safeFetchImportUrl(input: string, redirectsLeft = 5): Promise<Response> {
  const target = await assertSafeImportUrl(input);
  const { url } = target;
  const response = await fetchPinnedImportTarget(target);
  if (response.status >= 300 && response.status < 400 && response.headers.get("location")) {
    if (redirectsLeft <= 0) {
      reject(400, "URL import exceeded redirect limit");
    }
    const redirected = new URL(response.headers.get("location") || "", url);
    return await safeFetchImportUrl(redirected.toString(), redirectsLeft - 1);
  }
  const length = Number(response.headers.get("content-length") || 0);
  if (length) {
    assertImportSize(length, "URL import response");
  }
  return response;
}

async function fetchPinnedImportTarget(target: SafeImportTarget): Promise<Response> {
  return await new Promise<Response>((resolvePromise, rejectPromise) => {
    const { url, address, family } = target;
    const requestImpl = url.protocol === "https:" ? httpsRequest : httpRequest;
    const headers: Record<string, string> = {
      Host: url.host,
      "User-Agent": "brainstack-braind-url-import"
    };
    const req = requestImpl(
      {
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port || (url.protocol === "https:" ? 443 : 80),
        path: `${url.pathname}${url.search}`,
        method: "GET",
        headers,
        servername: url.hostname,
        lookup(_hostname, _options, callback) {
          callback(null, address, family);
        },
        timeout: urlFetchTimeoutMs
      },
      (res) => {
        const chunks: Buffer[] = [];
        let total = 0;
        const length = Number(res.headers["content-length"] || 0);
        if (length) {
          try {
            assertImportSize(length, "URL import response");
          } catch (error) {
            res.destroy();
            rejectPromise(error);
            return;
          }
        }
        res.on("data", (chunk: Buffer) => {
          total += chunk.byteLength;
          if (total > maxImportBytes) {
            const error = new HttpError(413, `URL import streamed response exceeds BRAIN_MAX_IMPORT_BYTES=${maxImportBytes}`);
            rejectPromise(error);
            res.destroy(error);
            return;
          }
          chunks.push(chunk);
        });
        res.on("end", () => {
          const responseHeaders = new Headers();
          for (const [key, value] of Object.entries(res.headers)) {
            if (Array.isArray(value)) {
              for (const item of value) {
                responseHeaders.append(key, item);
              }
            } else if (value !== undefined) {
              responseHeaders.set(key, String(value));
            }
          }
          resolvePromise(new Response(Buffer.concat(chunks), { status: res.statusCode || 0, headers: responseHeaders }));
        });
        res.on("error", (error) => {
          rejectPromise(error);
        });
      }
    );
    req.on("timeout", () => {
      req.destroy(new Error(`URL import fetch timed out after ${urlFetchTimeoutMs}ms`));
    });
    req.on("error", (error) => {
      rejectPromise(error);
    });
    req.end();
  }).catch((error) => {
    if (error instanceof HttpError) {
      throw error;
    }
    reject(400, `URL import fetch failed: ${error instanceof Error ? error.message : String(error)}`);
  });
}

async function readResponseBytesCapped(response: Response, label: string): Promise<Uint8Array> {
  if (!response.body) {
    const bytes = new Uint8Array(await response.arrayBuffer());
    assertImportSize(bytes.byteLength, label);
    return bytes;
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  let timedOut = false;
  const timeout = setTimeout(() => {
    timedOut = true;
    reader.cancel().catch(() => undefined);
  }, urlFetchTimeoutMs);
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (timedOut) {
        reject(400, `URL import fetch timed out after ${urlFetchTimeoutMs}ms`);
      }
      if (done) {
        break;
      }
      if (!value) {
        continue;
      }
      total += value.byteLength;
      if (total > maxImportBytes) {
        await reader.cancel().catch(() => undefined);
        reject(413, `${label} is too large: streamed response exceeds BRAIN_MAX_IMPORT_BYTES=${maxImportBytes}`);
      }
      chunks.push(value);
    }
  } finally {
    clearTimeout(timeout);
  }

  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return merged;
}

type AuthScope = "import" | "admin";

function requestAuthScope(request: Request): AuthScope | null {
  const token = request.headers.get("authorization")?.replace(/^Bearer\s+/i, "") || "";
  if (adminToken && token === adminToken) {
    return "admin";
  }
  if (importToken && token === importToken) {
    return "import";
  }
  return null;
}

function assertImportAuth(request: Request): AuthScope {
  const scope = requestAuthScope(request);
  if (!scope) {
    throw new Error("Unauthorized");
  }
  return scope;
}

function assertAdminAuth(request: Request): void {
  if (requestAuthScope(request) !== "admin") {
    throw new Error("Forbidden");
  }
}

async function withWriteGate<T>(request: Request, scope: AuthScope, run: () => Promise<T>): Promise<T> {
  const token = request.headers.get("authorization")?.replace(/^Bearer\s+/i, "") || scope;
  const tokenKey = sha256Text(token).slice(0, 16);
  const now = Date.now();
  const windowStart = now - 60_000;
  const recent = (writeRateEvents.get(tokenKey) || []).filter((timestamp) => timestamp >= windowStart);
  if (recent.length >= writeRateLimitPerMinute) {
    reject(429, `write rate limit exceeded for this token; limit=${writeRateLimitPerMinute}/minute`);
  }
  if (activeWriteRequests >= writeConcurrencyLimit) {
    reject(503, `write concurrency limit exceeded; limit=${writeConcurrencyLimit}`);
  }

  recent.push(now);
  writeRateEvents.set(tokenKey, recent);
  activeWriteRequests += 1;
  try {
    return await run();
  } finally {
    activeWriteRequests = Math.max(0, activeWriteRequests - 1);
  }
}

function parseTags(value: FormDataEntryValue | null): string[] {
  if (typeof value !== "string") {
    return [];
  }
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function asBoolean(value: FormDataEntryValue | null): boolean {
  return typeof value === "string" && ["1", "true", "yes", "on"].includes(value.toLowerCase());
}

function fileNameForUrl(url: string, contentType: string | undefined, title: string | undefined): string {
  try {
    const parsed = new URL(url);
    const lastPath = basename(parsed.pathname);
    if (lastPath && lastPath.includes(".")) {
      return lastPath;
    }
  } catch {
    // ignored
  }
  if (contentType?.includes("html")) {
    return `${slugify(title || "imported-url") || "imported-url"}.html`;
  }
  if (contentType?.includes("json")) {
    return `${slugify(title || "imported-url") || "imported-url"}.json`;
  }
  return `${slugify(title || "imported-url") || "imported-url"}.txt`;
}

async function runGit(args: string[], cwd: string): Promise<string> {
  const proc = Bun.spawn(["git", ...args], { cwd, stdout: "pipe", stderr: "pipe" });
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited
  ]);
  if (exitCode !== 0) {
    throw new Error(stderr.trim() || stdout.trim() || `git ${args.join(" ")} failed`);
  }
  return stdout.trim();
}

async function runReindexWorker(root: string): Promise<{ indexedDocs: number; lastReindexAt: string }> {
  const proc = Bun.spawn([process.execPath, "run", reindexWorkerPath, root], {
    stdout: "pipe",
    stderr: "pipe"
  });
  let timedOut = false;
  const timeout = setTimeout(() => {
    timedOut = true;
    proc.kill("SIGTERM");
    setTimeout(() => proc.kill("SIGKILL"), 1_000).unref?.();
  }, reindexTimeoutMs);
  timeout.unref?.();
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited
  ]);
  clearTimeout(timeout);
  if (timedOut) {
    throw new Error(`reindex worker timed out after ${reindexTimeoutMs}ms`);
  }
  if (exitCode !== 0) {
    throw new Error(stderr.trim() || stdout.trim() || `reindex worker failed with exit ${exitCode}`);
  }
  const parsed = JSON.parse(stdout.trim()) as { indexedDocs?: unknown; lastReindexAt?: unknown };
  if (typeof parsed.indexedDocs !== "number" || typeof parsed.lastReindexAt !== "string") {
    throw new Error(`reindex worker returned invalid output: ${stdout.trim().slice(0, 300)}`);
  }
  return { indexedDocs: parsed.indexedDocs, lastReindexAt: parsed.lastReindexAt };
}

async function performSearchRefresh(commit: string | null): Promise<Record<string, unknown>> {
  return await withRepoLock(repoRoot, async () => {
    const targetCommit = searchRefreshRequestedCommit || commit;
    searchRefreshRequestedCommit = null;
    if (targetCommit && repoRoot !== writeRepoRoot && existsSync(join(repoRoot, ".git"))) {
      const branch = await runGit(["rev-parse", "--abbrev-ref", "HEAD"], repoRoot) || "main";
      for (let attempt = 0; attempt < 5; attempt += 1) {
        const current = await runGit(["rev-parse", "HEAD"], repoRoot);
        if (current === targetCommit) {
          break;
        }
        await runGit(["fetch", "origin", branch], repoRoot);
        await runGit(["merge", "--ff-only", "FETCH_HEAD"], repoRoot);
        await Bun.sleep(200);
      }
    }
    const currentCommit = await currentRepoCommit(repoRoot);
    if (targetCommit && currentCommit !== targetCommit) {
      throw new Error(`search repo did not reach committed revision ${targetCommit}; current=${currentCommit || "unknown"}`);
    }
    const result = await runReindexWorker(repoRoot);
    return { ok: true, indexed_docs: result.indexedDocs, last_reindex_at: result.lastReindexAt, indexed_commit: targetCommit || currentCommit };
  });
}

async function recordPendingSearchRefresh(commit: string | null, error: string): Promise<void> {
  await writeJsonFile(pendingReindexPath, {
    commit,
    error,
    updated_at: isoNow()
  });
}

function queueSearchRefresh(commit: string | null): Record<string, unknown> {
  searchRefreshRequestedCommit = commit || searchRefreshRequestedCommit;
  void recordPendingSearchRefresh(searchRefreshRequestedCommit, "queued").catch(() => undefined);
  if (!searchRefreshPromise) {
    searchRefreshPromise = (async () => {
      while (searchRefreshRequestedCommit !== null) {
        const targetCommit = searchRefreshRequestedCommit;
        searchRefreshRequestedCommit = null;
        try {
          const latestCommit = searchRefreshRequestedCommit || targetCommit;
          await performSearchRefresh(latestCommit);
          await rm(pendingReindexPath, { force: true }).catch(() => undefined);
        } catch (error) {
          await recordPendingSearchRefresh(searchRefreshRequestedCommit || targetCommit, error instanceof Error ? error.message : String(error)).catch(() => undefined);
          break;
        }
      }
    })().finally(() => {
      searchRefreshPromise = null;
    });
  }
  return { ok: true, queued: true, target_commit: commit };
}

async function retryPendingSearchRefresh(): Promise<void> {
  const pending = await readJsonFile<{ commit?: string | null }>(pendingReindexPath);
  if (pending && !searchRefreshPromise) {
    queueSearchRefresh(pending.commit || null);
  }
}

async function refreshSearchAfterWrite(commit: string | null): Promise<Record<string, unknown>> {
  return queueSearchRefresh(commit);
}

const searchRefreshRetryTimer = setInterval(() => {
  void retryPendingSearchRefresh().catch((error) => {
    console.error("search reindex retry failed", error);
  });
}, 30_000);
if (typeof searchRefreshRetryTimer === "object" && searchRefreshRetryTimer && "unref" in searchRefreshRetryTimer) {
  (searchRefreshRetryTimer as { unref: () => void }).unref();
}
void retryPendingSearchRefresh().catch(() => undefined);

async function writeImport(input: Parameters<typeof createImportedArtifact>[1]): Promise<Record<string, unknown>> {
  return await withRepoLock(writeRepoRoot, async () => {
    await syncWritableRepoAsync(writeRepoRoot);
    const imported = await createImportedArtifact(writeRepoRoot, input);
    let touchedFiles = [...imported.touchedFiles];
    if (input.ingest_now) {
      const ingest = await ingestArtifacts(writeRepoRoot, [imported.artifactId]);
      touchedFiles = [...touchedFiles, ...ingest.touchedFiles];
    }
    const commit = await gitCommitAndPush(
      writeRepoRoot,
      touchedFiles,
      input.ingest_now ? `brain: import+ingest ${imported.artifactId}` : `brain: import ${imported.artifactId}`
    );
    return {
      ok: true,
      artifact_id: imported.artifactId,
      deduplicated: imported.deduplicated,
      commit,
      touched_files: touchedFiles,
      search_index: await refreshSearchAfterWrite(commit)
    };
  });
}

async function importFromRequest(request: Request, authScope: AuthScope): Promise<Record<string, unknown>> {
  const contentType = request.headers.get("content-type") || "";
  if (contentType.includes("multipart/form-data")) {
    requireContentLength(request, maxImportBytes + 256 * 1024, "Multipart request body");
    const form = await request.formData();
    const file = form.get("file");
    if (!(file instanceof File)) {
      throw new Error("multipart request requires a file field");
    }
    assertImportSize(file.size, "Uploaded file");
    const input = {
      title: String(form.get("title") || file.name),
      source_harness: String(form.get("source_harness") || ""),
      source_machine: String(form.get("source_machine") || ""),
      source_type: String(form.get("source_type") || ""),
      related_project: String(form.get("related_project") || "") || undefined,
      related_repo: String(form.get("related_repo") || "") || undefined,
      conversation_id: String(form.get("conversation_id") || "") || undefined,
      tags: [
        ...parseTags(form.get("tags")),
        ...form.getAll("tags[]").map((entry) => String(entry))
      ],
      ingest_now: asBoolean(form.get("ingest_now")),
      fileName: file.name,
      contentType: file.type,
      bytes: new Uint8Array(await file.arrayBuffer())
    };
    if (input.ingest_now && authScope !== "admin") {
      throw new Error("Forbidden: ingest_now requires the admin token");
    }
    return await withIdempotency(request, "import", requestHashFor({ endpoint: "import", input }), async (markSideEffectStarted) => {
      await markSideEffectStarted();
      return await writeImport(input);
    });
  }

  const body = await readJsonBody(request);
  if (typeof body.text === "string") {
    const text = body.text;
    const textBytes = new TextEncoder().encode(text);
    assertImportSize(textBytes.byteLength, "Text import");
    const title = typeof body.title === "string" ? body.title : "Imported note";
    const input = {
      title,
      text,
      bytes: textBytes,
      fileName: `${slugify(title) || "note"}.md`,
      contentType: "text/markdown",
      source_harness: String(body.source_harness || ""),
      source_machine: String(body.source_machine || ""),
      source_type: String(body.source_type || ""),
      related_project: typeof body.related_project === "string" ? body.related_project : undefined,
      related_repo: typeof body.related_repo === "string" ? body.related_repo : undefined,
      conversation_id: typeof body.conversation_id === "string" ? body.conversation_id : undefined,
      tags: Array.isArray(body.tags) ? body.tags.map(String) : [],
      ingest_now: Boolean(body.ingest_now)
    };
    if (input.ingest_now && authScope !== "admin") {
      throw new Error("Forbidden: ingest_now requires the admin token");
    }
    return await withIdempotency(request, "import", requestHashFor({ endpoint: "import", input }), async (markSideEffectStarted) => {
      await markSideEffectStarted();
      return await writeImport(input);
    });
  }

  if (typeof body.url === "string") {
    const sourceUrl = body.url;
    const title = typeof body.title === "string" ? body.title : sourceUrl;
    const requestShape = {
      title,
      url: sourceUrl,
      source_harness: String(body.source_harness || ""),
      source_machine: String(body.source_machine || ""),
      source_type: String(body.source_type || ""),
      related_project: typeof body.related_project === "string" ? body.related_project : undefined,
      related_repo: typeof body.related_repo === "string" ? body.related_repo : undefined,
      conversation_id: typeof body.conversation_id === "string" ? body.conversation_id : undefined,
      tags: Array.isArray(body.tags) ? body.tags.map(String) : [],
      ingest_now: Boolean(body.ingest_now)
    };
    if (requestShape.ingest_now && authScope !== "admin") {
      throw new Error("Forbidden: ingest_now requires the admin token");
    }
    return await withIdempotency(request, "import", requestHashFor({ endpoint: "import-url", request: requestShape }), async (markSideEffectStarted) => {
      const upstream = await safeFetchImportUrl(sourceUrl);
      if (!upstream.ok) {
        reject(400, `URL fetch failed: ${upstream.status} ${upstream.statusText}`);
      }
      const upstreamType = upstream.headers.get("content-type")?.split(";")[0];
      const bytes = await readResponseBytesCapped(upstream, "URL import response");
      await markSideEffectStarted();
      return await writeImport({
        ...requestShape,
        bytes,
        fileName: fileNameForUrl(sourceUrl, upstreamType, title),
        contentType: upstreamType
      });
    });
  }

  throw new Error("Import request must include text, url, or multipart file");
}

async function proposeFromRequest(request: Request): Promise<Record<string, unknown>> {
  const body = await readJsonBody(request);
  const input = {
    title: String(body.title || ""),
    body: String(body.body || ""),
    source_harness: String(body.source_harness || ""),
    source_machine: String(body.source_machine || ""),
    target_page: typeof body.target_page === "string" ? body.target_page : undefined,
    tags: Array.isArray(body.tags) ? body.tags.map(String) : []
  };
  return await withIdempotency(request, "propose", requestHashFor({ endpoint: "propose", input }), async (markSideEffectStarted) => {
    await markSideEffectStarted();
    return await withRepoLock(writeRepoRoot, async () => {
      await syncWritableRepoAsync(writeRepoRoot);
      const proposal = await createProposal(writeRepoRoot, input);
      const commit = await gitCommitAndPush(
        writeRepoRoot,
        proposal.touchedFiles,
        `brain: propose ${input.title || "proposal"}`
      );
      return {
        ok: true,
        proposal_path: proposal.proposalPath,
        commit,
        touched_files: proposal.touchedFiles,
        search_index: await refreshSearchAfterWrite(commit)
      };
    });
  });
}

async function renderHome(): Promise<Response> {
  const health = await getHealth(repoRoot);
  const logEntries = await getRecentLogEntries(repoRoot, 8);
  const imports = await getRecentImports(repoRoot, 8);
  const sections = await discoverHomeSections();
  return layout(
    "Shared Brain",
    `<section class="masthead">
      <div>
        <span class="pill">canonical git wiki</span>
        <span class="pill">bun service</span>
        <span class="pill">${htmlEscape(organizerLabel)}</span>
      </div>
      <h1>Shared Brain</h1>
      <p>One organizer-hosted canon for machines, harnesses, runbooks, source pages, and append-only raw evidence.</p>
      <form class="search" action="/search" method="get">
        <input type="search" name="q" placeholder="Search wiki pages and raw artifacts" />
        <select name="scope">
          <option value="all">all</option>
          <option value="wiki">wiki</option>
          <option value="raw">raw</option>
        </select>
        <button type="submit">Search</button>
      </form>
    </section>
    <div class="grid cols">
      <section class="card">
        <h2>Recent log entries</h2>
        <ul>
          ${logEntries
            .map(
              (entry) =>
                `<li><strong>${htmlEscape(entry.operation)}</strong> ${htmlEscape(entry.title)} <span class="pill">${htmlEscape(entry.timestamp)}</span></li>`
            )
            .join("")}
        </ul>
      </section>
      <aside class="card">
        <h2>Status</h2>
        <div class="status">
          <div>Commit: <code>${htmlEscape(health.commit || "(none)")}</code></div>
          <div>Indexed docs: <code>${String(health.indexed_docs)}</code></div>
          <div>Last reindex: <code>${htmlEscape(health.last_reindex_at || "(none)")}</code></div>
          <div>Repo root: <code>${htmlEscape(repoRoot)}</code></div>
        </div>
      </aside>
    </div>
    <div class="grid cols">
      <section class="card">
        <h2>Recent imports</h2>
        <ul>
          ${imports
            .map((manifest) => {
              const sourcePage = `wiki/Sources/${manifest.id}.md`;
              const href = existsSync(join(repoRoot, sourcePage))
                ? pageHref(sourcePage)
                : rawHref(manifest.normalized_path || manifest.raw_path);
              return `<li><a href="${href}">${htmlEscape(manifest.title)}</a> <span class="pill">${htmlEscape(manifest.source_type)}</span></li>`;
            })
            .join("")}
        </ul>
      </section>
      <aside class="card">
        <h2>Top-level sections</h2>
        <ul>
          ${sections.map((section) => `<li><a href="${pageHref(section.path)}">${htmlEscape(section.label)}</a></li>`).join("")}
        </ul>
      </aside>
    </div>`
  );
}

async function discoverHomeSections(): Promise<Array<{ label: string; path: string }>> {
  const sections: Array<{ label: string; path: string }> = [];
  const seen = new Set<string>();
  const add = (label: string, path: string) => {
    if (!seen.has(path) && existsSync(join(repoRoot, path))) {
      seen.add(path);
      sections.push({ label, path });
    }
  };

  add("Home", "wiki/Home.md");
  add("Index", "wiki/Index.md");

  const wikiRoot = join(repoRoot, "wiki");
  const entries = await readdir(wikiRoot, { withFileTypes: true }).catch(() => []);
  for (const entry of entries) {
    if (!entry.isDirectory() || entry.name.startsWith(".")) {
      continue;
    }

    const indexPath = `wiki/${entry.name}/Index.md`;
    if (existsSync(join(repoRoot, indexPath))) {
      add(entry.name, indexPath);
      continue;
    }

    const childEntries = await readdir(join(wikiRoot, entry.name), { withFileTypes: true }).catch(() => []);
    const firstPage = childEntries
      .filter((child) => child.isFile() && child.name.endsWith(".md"))
      .map((child) => child.name)
      .sort()[0];
    if (firstPage) {
      add(entry.name, `wiki/${entry.name}/${firstPage}`);
    }
  }

  return sections;
}

async function renderPage(repoPath: string): Promise<Response> {
  assertPublicPagePath(repoPath);
  const page = await readPage(repoRoot, repoPath);
  const backlinks = existsSync(join(repoRoot, "derived", "search.sqlite"))
    ? await backlinksForPath(repoRoot, repoPath)
    : [];
  const sourceIds = Array.isArray(page.data.source_ids) ? page.data.source_ids.map(String) : [];
  const sourceLinks = [];
  for (const sourceId of sourceIds) {
    const manifest = await findSourceManifestById(repoRoot, sourceId);
    if (manifest) {
      sourceLinks.push(
        `<li><a href="${pageHref(`wiki/Sources/${sourceId}.md`)}">${htmlEscape(manifest.title)}</a> · <a href="${rawHref(manifest.raw_path)}">raw</a>${
          manifest.normalized_path ? ` · <a href="${rawHref(manifest.normalized_path)}">normalized</a>` : ""
        }</li>`
      );
    } else {
      sourceLinks.push(`<li><a href="${pageHref(`wiki/Sources/${sourceId}.md`)}">${htmlEscape(sourceId)}</a></li>`);
    }
  }
  const crumbs = repoPath
    .split("/")
    .map((part, index, parts) => {
      const label = index === parts.length - 1 ? part.replace(/\.md$/, "") : part;
      return `<span>${htmlEscape(label)}</span>`;
    })
    .join("&nbsp;/&nbsp;");
  return layout(
    page.title,
    `<section class="masthead">
      <div class="crumbs">${crumbs}</div>
      <h1>${htmlEscape(page.title)}</h1>
      <p>Rendered from <code>${htmlEscape(repoPath)}</code></p>
    </section>
    <div class="grid cols">
      <article class="card content">
        ${renderMarkdown(page.body, repoPath)}
      </article>
      <aside class="grid">
        <section class="card">
          <h3>Backlinks</h3>
          <ul>
            ${backlinks.length ? backlinks.map((item) => `<li><a href="${pageHref(item)}">${htmlEscape(item)}</a></li>`).join("") : "<li>none</li>"}
          </ul>
        </section>
        <section class="card">
          <h3>Sources</h3>
          <ul>
            ${sourceLinks.length ? sourceLinks.join("") : "<li>none</li>"}
          </ul>
        </section>
      </aside>
    </div>`
  );
}

async function renderRaw(repoPath: string): Promise<Response> {
  assertPublicRawPath(repoPath);
  const absolutePath = safeRepoPath(repoRoot, repoPath);
  const file = Bun.file(absolutePath);
  if (!(await file.exists())) {
    return errorResponse(404, `Raw file not found: ${repoPath}`);
  }
  const mime = (file.type || "application/octet-stream").split(";")[0].trim().toLowerCase();
  const extension = extname(repoPath).toLowerCase();
  const activeContent =
    [".html", ".htm", ".xhtml", ".svg"].includes(extension) ||
    mime === "text/html" ||
    mime === "application/xhtml+xml" ||
    mime === "image/svg+xml";
  if (activeContent) {
    const headers = securityHeaders("text/plain; charset=utf-8");
    headers.set("Content-Disposition", `attachment; filename="${attachmentFileName(repoPath, ".txt")}"`);
    const response = new Response(await file.text(), { headers });
    response.headers.set("Content-Type", "text/plain; charset=utf-8");
    return response;
  }
  if (mime.startsWith("text/") || mime.includes("json") || [".md", ".txt", ".json"].includes(extension)) {
    return new Response(await file.text(), {
      headers: securityHeaders(`${mime || "text/plain"}; charset=utf-8`)
    });
  }
  const headers = securityHeaders(mime);
  if (!mime.startsWith("image/") && mime !== "application/pdf") {
    headers.set("Content-Disposition", `attachment; filename="${attachmentFileName(repoPath)}"`);
  }
  return new Response(file, { headers });
}

async function renderSearch(request: Request, url: URL): Promise<Response> {
  const query = (url.searchParams.get("q") || "").trim().slice(0, 500);
  const rawScope = url.searchParams.get("scope") || "all";
  const scope = rawScope === "wiki" || rawScope === "raw" || rawScope === "all" ? rawScope : "all";
  const rawLimit = Number(url.searchParams.get("limit") || 10);
  const limit = Math.min(Math.max(Math.trunc(rawLimit) || 10, 1), maxSearchLimit);
  const results = query ? await searchIndex(repoRoot, query, scope, limit) : [];
  if (wantsJson(request, url)) {
    return json({ query, scope, limit, results });
  }
  return layout(
    `Search: ${query || "shared brain"}`,
    `<section class="masthead">
      <h1>Search</h1>
      <form class="search" action="/search" method="get">
        <input type="search" name="q" value="${htmlEscape(query)}" placeholder="Search wiki pages and raw artifacts" />
        <select name="scope">
          <option value="all"${scope === "all" ? " selected" : ""}>all</option>
          <option value="wiki"${scope === "wiki" ? " selected" : ""}>wiki</option>
          <option value="raw"${scope === "raw" ? " selected" : ""}>raw</option>
        </select>
        <button type="submit">Search</button>
      </form>
    </section>
    <section class="card">
      <h2>${results.length} results</h2>
      <ul>
        ${results
          .map(
            (result) =>
              `<li><a href="${result.path.startsWith("wiki/") ? pageHref(result.path) : rawHref(result.path)}">${htmlEscape(result.title)}</a> <span class="pill">${htmlEscape(result.scope)}</span><div>${result.snippet}</div><code>${htmlEscape(result.path)}</code></li>`
          )
          .join("")}
      </ul>
    </section>`
  );
}

const server = Bun.serve({
  hostname: host,
  port,
  async fetch(request) {
    const url = new URL(request.url);
    try {
      if (request.method === "GET" && url.pathname === "/health") {
        return json(await getHealth(repoRoot));
      }
      if (request.method === "GET" && url.pathname === "/") {
        return await renderHome();
      }
      if (request.method === "GET" && url.pathname.startsWith("/page/")) {
        return await renderPage(decodeURIComponent(url.pathname.slice("/page/".length)));
      }
      if (request.method === "GET" && url.pathname.startsWith("/raw/")) {
        return await renderRaw(decodeURIComponent(url.pathname.slice("/raw/".length)));
      }
      if (request.method === "GET" && url.pathname === "/search") {
        return await renderSearch(request, url);
      }
      if (request.method === "POST" && url.pathname === "/api/import") {
        const authScope = assertImportAuth(request);
        return json(await withWriteGate(request, authScope, () => importFromRequest(request, authScope)));
      }
      if (request.method === "POST" && url.pathname === "/api/ingest") {
        assertAdminAuth(request);
        return json(
          await withWriteGate(request, "admin", async () => {
            const body = await readJsonBody(request);
            const artifactIds = Array.isArray(body.artifact_ids)
              ? body.artifact_ids.map(String)
              : body.artifact_id
                ? [String(body.artifact_id)]
                : [];
            if (!artifactIds.length) {
              reject(400, "artifact_id or artifact_ids is required");
            }
            return await withRepoLock(writeRepoRoot, async () => {
              await syncWritableRepoAsync(writeRepoRoot);
              const ingest = await ingestArtifacts(writeRepoRoot, artifactIds);
              const commit = await gitCommitAndPush(writeRepoRoot, ingest.touchedFiles, `brain: ingest ${artifactIds.join(", ")}`);
              return {
                ok: true,
                artifact_ids: artifactIds,
                commit,
                touched_files: ingest.touchedFiles,
                search_index: await refreshSearchAfterWrite(commit)
              };
            });
          })
        );
      }
      if (request.method === "POST" && url.pathname === "/api/propose") {
        const authScope = assertImportAuth(request);
        return json(await withWriteGate(request, authScope, () => proposeFromRequest(request)));
      }
      if (request.method === "POST" && url.pathname === "/api/lint") {
        assertAdminAuth(request);
        return json(
          await withWriteGate(request, "admin", async () => {
            assertContentLength(request, maxJsonBytes, "Lint request body");
            const result = await withRepoLock(writeRepoRoot, async () => {
              await syncWritableRepoAsync(writeRepoRoot);
              const lint = await lintWiki(writeRepoRoot);
              const commit = await gitCommitAndPush(writeRepoRoot, lint.touchedFiles, `brain: lint ${isoNow()}`);
              return {
                ok: true,
                report_path: lint.reportPath,
                commit,
                touched_files: lint.touchedFiles,
                search_index: await refreshSearchAfterWrite(commit)
              };
            });
            return result;
          })
        );
      }
      return errorResponse(404, `Unknown route: ${url.pathname}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (error instanceof HttpError) {
        return json({ error: message }, error.status);
      }
      if (message === "Unauthorized") {
        return json({ error: message }, 401);
      }
      if (message.startsWith("Forbidden")) {
        return json({ error: message }, 403);
      }
      return wantsJson(request, url) ? json({ error: message }, 500) : errorResponse(500, message);
    }
  }
});

console.log(`shared-brain listening on http://${server.hostname}:${server.port}`);
