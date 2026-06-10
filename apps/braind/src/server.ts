import { existsSync } from "node:fs";
import { mkdir, readFile, rename, rm, rmdir, stat, writeFile, readdir } from "node:fs/promises";
import { lookup } from "node:dns/promises";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { isIP } from "node:net";
import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { hostname } from "node:os";
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
  isSafeManifestId,
  isoNow,
  lintWiki,
  parseFrontmatter,
  readPage,
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
    message: string,
    public readonly code?: string
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
function normalizeSecurityPosture(value: string | undefined): "local" | "trusted-tailnet" | "guarded" {
  const normalized = (value || "trusted-tailnet").trim().toLowerCase();
  if (normalized === "local" || normalized === "trusted-tailnet" || normalized === "guarded") {
    return normalized;
  }
  throw new Error(`Unsupported BRAIN_SECURITY_POSTURE=${value}; expected local, trusted-tailnet, or guarded.`);
}

function normalizeTrustedExposure(value: string | undefined): "none" | "tailscale-serve" | "vpn" | "manual" {
  const normalized = (value || "none").trim().toLowerCase();
  if (normalized === "none" || normalized === "tailscale-serve" || normalized === "vpn" || normalized === "manual") {
    return normalized;
  }
  throw new Error(`Unsupported BRAIN_TRUSTED_EXPOSURE=${value}; expected none, tailscale-serve, vpn, or manual.`);
}

const securityPosture = normalizeSecurityPosture(process.env.BRAIN_SECURITY_POSTURE);
const trustedExposure = normalizeTrustedExposure(process.env.BRAIN_TRUSTED_EXPOSURE);
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
const configuredImportPreparationConcurrency = Number(process.env.BRAIN_IMPORT_PREPARATION_CONCURRENCY || "");
const importPreparationConcurrencyLimit =
  Number.isFinite(configuredImportPreparationConcurrency) && configuredImportPreparationConcurrency > 0
    ? Math.trunc(configuredImportPreparationConcurrency)
    : Math.max(1, writeConcurrencyLimit);
const configuredRateLimit = Number(process.env.BRAIN_WRITE_RATE_LIMIT_PER_MINUTE || "");
const writeRateLimitPerMinute =
  Number.isFinite(configuredRateLimit) && configuredRateLimit > 0
    ? Math.trunc(configuredRateLimit)
    : 60;
const configuredTokenRateLimit = Number(process.env.BRAIN_WRITE_TOKEN_RATE_LIMIT_PER_MINUTE || "");
const writeTokenRateLimitPerMinute =
  Number.isFinite(configuredTokenRateLimit) && configuredTokenRateLimit > 0
    ? Math.trunc(configuredTokenRateLimit)
    : writeRateLimitPerMinute * 5;
const configuredRateLimitMaxKeys = Number(process.env.BRAIN_WRITE_RATE_LIMIT_MAX_KEYS || "");
const writeRateLimitMaxKeys =
  Number.isFinite(configuredRateLimitMaxKeys) && configuredRateLimitMaxKeys > 0
    ? Math.trunc(configuredRateLimitMaxKeys)
    : 10_000;
const trustProxyHeaders = ["1", "true", "yes", "on"].includes(
  (process.env.BRAIN_TRUST_PROXY_HEADERS || "").toLowerCase()
);
const configuredMutationWaitMs = Number(process.env.BRAIN_WRITE_QUEUE_WAIT_MS || process.env.BRAIN_REPO_LOCK_WAIT_MS || "");
const mutationQueueWaitMs =
  Number.isFinite(configuredMutationWaitMs) && configuredMutationWaitMs > 0
    ? Math.trunc(configuredMutationWaitMs)
    : 30_000;
const configuredReindexTimeoutMs = Number(process.env.BRAIN_REINDEX_TIMEOUT_MS || "");
const reindexTimeoutMs =
  Number.isFinite(configuredReindexTimeoutMs) && configuredReindexTimeoutMs > 0
    ? Math.trunc(configuredReindexTimeoutMs)
    : 120_000;
const configuredIdempotencyClaimLeaseMs = Number(process.env.BRAIN_IDEMPOTENCY_CLAIM_LEASE_MS || "");
const idempotencyClaimLeaseMs =
  Number.isFinite(configuredIdempotencyClaimLeaseMs) && configuredIdempotencyClaimLeaseMs > 0
    ? Math.trunc(configuredIdempotencyClaimLeaseMs)
    : 60_000;
const configuredIdempotencyRunningLeaseMs = Number(process.env.BRAIN_IDEMPOTENCY_RUNNING_LEASE_MS || "");
const idempotencyRunningLeaseMs =
  Number.isFinite(configuredIdempotencyRunningLeaseMs) && configuredIdempotencyRunningLeaseMs > 0
    ? Math.trunc(configuredIdempotencyRunningLeaseMs)
    : 30 * 60_000;
const configuredIdempotencyLockWaitMs = Number(process.env.BRAIN_IDEMPOTENCY_LOCK_WAIT_MS || "");
const idempotencyLockWaitMs =
  Number.isFinite(configuredIdempotencyLockWaitMs) && configuredIdempotencyLockWaitMs > 0
    ? Math.trunc(configuredIdempotencyLockWaitMs)
    : 30_000;
const configuredLockStaleMs = Number(process.env.BRAIN_LOCK_STALE_MS || "");
const lockStaleMs =
  Number.isFinite(configuredLockStaleMs) && configuredLockStaleMs > 0
    ? Math.trunc(configuredLockStaleMs)
    : 5 * 60_000;
const maxSearchLimit = 50;
const reindexWorkerPath = join(import.meta.dir, "reindex-worker.ts");
const pendingReindexPath = join(repoRoot, "derived", "search-reindex-needed.json");
let searchRefreshPromise: Promise<void> | null = null;
let searchRefreshRequested = false;
let searchRefreshRequestedCommit: string | null = null;
let searchRefreshGeneration = 0;
let searchRefreshMarkerWritePromise: Promise<void> | null = null;
let activeWriteRequests = 0;
let activeImportPreparations = 0;
const writeRateEvents = new Map<string, number[]>();
const writeTokenRateEvents = new Map<string, number[]>();
const writePreBodyRateEvents = new Map<string, number[]>();

if (legacyWriteToken || (rawLegacyWriteToken && rawLegacyWriteToken !== legacyWriteToken)) {
  throw new Error("BRAIN_WRITE_TOKEN is no longer accepted by braind; set distinct BRAIN_IMPORT_TOKEN and BRAIN_ADMIN_TOKEN.");
}
if (!importToken || !adminToken) {
  throw new Error("braind requires explicit BRAIN_IMPORT_TOKEN and BRAIN_ADMIN_TOKEN.");
}
if (importToken === adminToken) {
  throw new Error("BRAIN_IMPORT_TOKEN and BRAIN_ADMIN_TOKEN must be distinct.");
}
if (securityPosture === "local" && !["127.0.0.1", "::1", "localhost"].includes(host)) {
  throw new Error(`BRAIN_SECURITY_POSTURE=local requires BRAIN_BIND to be loopback; got ${host}`);
}

async function currentRepoCommit(root: string): Promise<string | null> {
  const proc = Bun.spawn(["git", "rev-parse", "HEAD"], { cwd: root, stdout: "pipe", stderr: "pipe" });
  const [stdout, exitCode] = await Promise.all([new Response(proc.stdout).text(), proc.exited]);
  return exitCode === 0 ? stdout.trim() : null;
}

async function ensureFreshStartupIndex(): Promise<void> {
  const pending = await latestPendingReindexMarker();
  if (pending) {
    searchRefreshGeneration = Math.max(searchRefreshGeneration, Number(pending.marker.generation || 0));
    queueSearchRefresh(typeof pending.marker.commit === "string" ? pending.marker.commit : null);
    return;
  }
  const health = existsSync(join(repoRoot, "derived", "search.sqlite")) ? await getHealth(repoRoot) : null;
  const currentCommit = await currentRepoCommit(repoRoot);
  if (!health || (currentCommit && health.indexed_commit !== currentCommit)) {
    queueSearchRefresh(currentCommit);
    return;
  }
  await rm(pendingReindexPath, { force: true }).catch(() => undefined);
}

let startupIndexPromise: Promise<void> | null = null;

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

function layout(title: string, body: string, status = 200): Response {
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
    .notice {
      padding: 0.8rem 1rem;
      border: 1px solid #b98521;
      border-radius: 0.75rem;
      background: #fff2ce;
      color: #5b3a00;
      margin-top: 1rem;
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
  return new Response(html, { status, headers: securityHeaders("text/html; charset=utf-8") });
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
  return layout(`Error ${status}`, `<div class="masthead"><h1>Error ${status}</h1><p>${htmlEscape(message)}</p></div>`, status);
}

function unexpectedErrorResponse(request: Request, url: URL, error: unknown): Response {
  const requestId = randomUUID();
  const detail = error instanceof Error ? { message: error.message, stack: error.stack } : { message: String(error) };
  console.error("braind unexpected request error", {
    request_id: requestId,
    method: request.method,
    path: url.pathname,
    ...detail
  });
  const message = `Internal server error. Reference: ${requestId}`;
  return wantsJson(request, url)
    ? json({ error: "Internal server error", request_id: requestId }, 500)
    : errorResponse(500, message);
}

function wantsJson(request: Request, url: URL): boolean {
  return url.pathname.startsWith("/api/") || url.searchParams.get("format") === "json" || request.headers.get("accept")?.includes("application/json") === true;
}

function reject(status: number, message: string, code?: string): never {
  throw new HttpError(status, message, code);
}

async function lockStatus(lockDir: string): Promise<Record<string, unknown>> {
  if (!existsSync(lockDir)) {
    return { path: lockDir, present: false };
  }
  const info = await stat(lockDir);
  const entries = await readdir(lockDir).catch(() => []);
  const ageMs = Math.max(0, Date.now() - info.mtime.getTime());
  const owners: Array<Record<string, unknown>> = [];
  for (const entry of entries.filter((name) => name.startsWith("owner-") && name.endsWith(".json")).sort()) {
    const owner = await readJsonFile<Record<string, unknown>>(join(lockDir, entry));
    owners.push({ file: entry, ...(owner || { unreadable: true }) });
  }
  return {
    path: lockDir,
    present: true,
    mtime: info.mtime.toISOString(),
    age_ms: ageMs,
    stale: ageMs > lockStaleMs,
    entries,
    owners
  };
}

async function idempotencyLockStatuses(idempotencyDir: string): Promise<Array<Record<string, unknown>>> {
  if (!existsSync(idempotencyDir)) {
    return [];
  }
  const locks: Array<Record<string, unknown>> = [];
  for (const endpoint of await readdir(idempotencyDir).catch(() => [])) {
    const endpointDir = join(idempotencyDir, endpoint);
    for (const name of await readdir(endpointDir).catch(() => [])) {
      if (!name.endsWith(".json.lock")) {
        continue;
      }
      locks.push({ endpoint, ...(await lockStatus(join(endpointDir, name))) });
    }
  }
  return locks;
}

async function latestPendingReindexMarker(): Promise<{ path: string; marker: Record<string, unknown>; mtimeMs: number } | null> {
  const [info, marker] = await Promise.all([
    stat(pendingReindexPath).catch(() => null),
    readJsonFile<Record<string, unknown>>(pendingReindexPath)
  ]);
  if (!info || !marker) {
    return null;
  }
  return { path: pendingReindexPath, marker, mtimeMs: info.mtime.getTime() };
}

async function pendingReindexStatus(): Promise<Record<string, unknown>> {
  const latest = await latestPendingReindexMarker();
  if (!latest) {
    return { present: false, path: pendingReindexPath };
  }
  return {
    present: true,
    path: latest.path,
    age_ms: Math.max(0, Date.now() - latest.mtimeMs),
    marker: latest.marker
  };
}

async function publicPendingReindexStatus(): Promise<Record<string, unknown>> {
  const latest = await latestPendingReindexMarker();
  if (!latest) {
    return { present: false };
  }
  return {
    present: true,
    age_ms: Math.max(0, Date.now() - latest.mtimeMs)
  };
}

async function operationalHealth(): Promise<Record<string, unknown>> {
  const readPaths = getRepoPaths(repoRoot);
  const writePaths = getRepoPaths(writeRepoRoot);
  return {
    ...(await getHealth(repoRoot)),
    security_posture: securityPosture,
    trusted_exposure: trustedExposure,
    bind_host: host,
    write_repo_root: writeRepoRoot,
    repo_lock: await lockStatus(readPaths.lockDir),
    write_repo_lock: await lockStatus(writePaths.lockDir),
    idempotency_locks: await idempotencyLockStatuses(writePaths.idempotencyDir),
    pending_reindex: await pendingReindexStatus()
  };
}

function minimalHealth(): Record<string, unknown> {
  return {
    ok: true,
    service: "braind",
    version: "0.1.0"
  };
}

async function readinessHealth(): Promise<Record<string, unknown>> {
  const pending = await publicPendingReindexStatus();
  const searchIndexPath = join(repoRoot, "derived", "search.sqlite");
  const searchReady = existsSync(searchIndexPath) && !pending.present && !searchRefreshPromise && !startupIndexPromise;
  return {
    ...minimalHealth(),
    ready: searchReady,
    search_ready: searchReady,
    search_refreshing: Boolean(searchRefreshPromise || startupIndexPromise),
    pending_reindex: pending
  };
}

function staleSearchNotice(status: Record<string, unknown>): string {
  if (!status.present) {
    return "";
  }
  const age = typeof status.age_ms === "number" ? ` age_ms=${Math.trunc(status.age_ms)}` : "";
  return `<div class="notice">Search/backlink index refresh is pending; results may be stale.${htmlEscape(age)}</div>`;
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
  status: "claimed" | "running" | "complete" | "failed" | "review_required";
  created_at: string;
  updated_at: string;
  lease_until?: string;
  side_effect_started_at?: string;
  review_required_at?: string;
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

function isLeaseExpired(record: Pick<IdempotencyRecord, "lease_until">): boolean {
  return record.lease_until ? Date.parse(record.lease_until) < Date.now() : false;
}

async function idempotencyLockDetails(lockDir: string): Promise<string> {
  try {
    const entries = await readdir(lockDir);
    const ownerEntry = entries.find((entry) => entry.startsWith("owner-") && entry.endsWith(".json"));
    const lockStat = await stat(lockDir);
    if (ownerEntry) {
      const ownerText = (await readFile(join(lockDir, ownerEntry), "utf8")).trim().slice(0, 500);
      return `lock=${lockDir} owner=${ownerText || ownerEntry} mtime=${lockStat.mtime.toISOString()}`;
    }
    return `lock=${lockDir} owner=missing mtime=${lockStat.mtime.toISOString()}`;
  } catch {
    return `lock=${lockDir} owner=unreadable`;
  }
}

function pidAppearsAlive(pid: unknown): boolean {
  if (typeof pid !== "number" || !Number.isFinite(pid) || pid <= 0) {
    return false;
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function pidIsDefinitelyDead(pid: unknown): boolean {
  if (typeof pid !== "number" || !Number.isInteger(pid) || pid <= 0) {
    return false;
  }
  try {
    process.kill(pid, 0);
    return false;
  } catch (error) {
    const code = error && typeof error === "object" && "code" in error ? String((error as { code?: unknown }).code) : "";
    return code === "ESRCH";
  }
}

function isIdempotencyLockEntry(entry: string): boolean {
  return /^owner-[A-Fa-f0-9-]+\.json$/.test(entry) || /^release-[A-Fa-f0-9-]+$/.test(entry);
}

async function clearPreMutationIdempotencyLock(lockDir: string): Promise<boolean> {
  try {
    const entries = await readdir(lockDir);
    if (!entries.length || entries.some((entry) => !isIdempotencyLockEntry(entry))) {
      return false;
    }
    const ownerEntries = entries.filter((entry) => entry.startsWith("owner-") && entry.endsWith(".json"));
    if (ownerEntries.length !== 1) {
      return false;
    }
    const owner = await readJsonFile<{ pid?: number; hostname?: string }>(join(lockDir, ownerEntries[0]));
    if (!owner || owner.hostname !== hostname() || !pidIsDefinitelyDead(owner.pid)) {
      return false;
    }
    console.warn(`clearing expired pre-mutation idempotency lock for dead pid=${owner.pid} lock=${lockDir}`);
    for (const entry of entries) {
      await rm(join(lockDir, entry), { force: true });
    }
    await rmdir(lockDir);
    return true;
  } catch {
    return false;
  }
}

async function withIdempotency(
  request: Request,
  endpoint: "import" | "propose",
  requestHash: string,
  run: (markSideEffectStarted: () => Promise<void>) => Promise<Record<string, unknown>>
): Promise<Record<string, unknown>> {
  const rawKey = request.headers.get("idempotency-key")?.trim();
  if (rawKey && rawKey.length > 200) {
    reject(400, "Idempotency-Key is too long");
  }
  // Idempotency must not depend on the caller remembering to send a key: without one,
  // derive a semantic key from the canonical request hash so retries of the same
  // logical payload share one record instead of creating duplicate commits.
  const effectiveKey = rawKey || `semantic-${requestHash}`;

  const paths = getRepoPaths(writeRepoRoot);
  const keyHash = sha256Text(effectiveKey);
  const recordPath = join(paths.idempotencyDir, endpoint, `${keyHash}.json`);
  const relativeRecordPath = `derived/idempotency/${endpoint}/${keyHash}.json`;
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
        await writeJsonFile(lockOwnerPath, { token: lockToken, pid: process.pid, hostname: hostname(), created_at: isoNow() }, { createOnly: true });
        await writeFile(lockTokenPath, lockToken, { encoding: "utf8", flag: "wx" });
      } catch (error) {
        await rm(lockTokenPath, { force: true }).catch(() => undefined);
        await rm(lockOwnerPath, { force: true }).catch(() => undefined);
        await rmdir(lockDir).catch(() => undefined);
        throw error;
      }
      break;
    } catch (error) {
      if (errorCode(error) !== "EEXIST") {
        throw error;
      }
      if (Date.now() - lockStartedAt > idempotencyLockWaitMs) {
        const existing = await readJsonFile<IdempotencyRecord>(recordPath);
        if (existing?.endpoint === endpoint && existing.request_hash === requestHash) {
          if (existing.status === "complete" && existing.response_body) {
            return { ...existing.response_body, idempotent_replay: true };
          }
          if (existing.status === "claimed" && isLeaseExpired(existing) && !existing.side_effect_started_at) {
            if (await clearPreMutationIdempotencyLock(lockDir)) {
              continue;
            }
            const reviewRecord: IdempotencyRecord = {
              ...existing,
              status: "review_required",
              updated_at: isoNow(),
              review_required_at: isoNow(),
              error:
                existing.error ||
                `Idempotent request preflight lock could not be proven safe to clear; inspect ${relativeRecordPath} and ${lockDir} before retrying with a new key.`
            };
            await writeJsonFile(recordPath, reviewRecord).catch(() => undefined);
            reject(409, "Idempotent request preflight requires operator review before retry", "IDEMPOTENCY_REVIEW_REQUIRED");
          }
          if ((existing.status === "claimed" || existing.status === "running") && isLeaseExpired(existing)) {
            const reviewRecord: IdempotencyRecord = {
              ...existing,
              status: "review_required",
              updated_at: isoNow(),
              review_required_at: isoNow(),
              error:
                existing.error ||
                `Idempotent request lock persisted past its lease; inspect ${relativeRecordPath} and ${lockDir} before retrying with a new key.`
            };
            await writeJsonFile(recordPath, reviewRecord).catch(() => undefined);
            reject(
              409,
              `Idempotent request requires operator review before retry; inspect ${relativeRecordPath} and clear the persisted idempotency lock only after confirming side effects.`,
              "IDEMPOTENCY_REVIEW_REQUIRED"
            );
          }
          if (existing.status === "failed" || existing.status === "review_required") {
            reject(
              409,
              `Idempotent request requires operator review before retry; inspect ${relativeRecordPath} and retry with a new Idempotency-Key only if the side effect did not land.`,
              "IDEMPOTENCY_REVIEW_REQUIRED"
            );
          }
          reject(425, "Idempotent request is already in progress; retry later", "IDEMPOTENCY_IN_PROGRESS");
        }
        const detail = await idempotencyLockDetails(lockDir);
        reject(409, `Idempotent request is blocked by a persisted request lock and requires operator review. ${detail}`, "IDEMPOTENCY_LOCK_REVIEW_REQUIRED");
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
    lease_until: new Date(Date.now() + idempotencyClaimLeaseMs).toISOString()
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
      const leaseExpired = isLeaseExpired(existing);
      if (existing.status === "claimed" && leaseExpired && !existing.side_effect_started_at) {
        await writeJsonFile(recordPath, pending);
      } else if (existing.status === "claimed" && leaseExpired && existing.side_effect_started_at) {
        const reviewRecord: IdempotencyRecord = {
          ...existing,
          status: "review_required",
          updated_at: isoNow(),
          review_required_at: isoNow(),
          error:
            existing.error ||
            `Idempotent request reached inconsistent claimed state with side effects started; inspect ${relativeRecordPath} before retrying with a new key.`
        };
        await writeJsonFile(recordPath, reviewRecord);
        reject(
          409,
          `Idempotent request requires operator review before retry; inspect ${relativeRecordPath} and retry with a new Idempotency-Key only if the side effect did not land.`
        );
      } else if (existing.status === "running" && leaseExpired) {
        const reviewRecord: IdempotencyRecord = {
          ...existing,
          status: "review_required",
          updated_at: isoNow(),
          review_required_at: isoNow(),
          error:
            existing.error ||
            `Idempotent request exceeded its running lease after side effects may have started; inspect ${relativeRecordPath} before retrying with a new key.`
        };
        await writeJsonFile(recordPath, reviewRecord);
        reject(
          409,
          `Idempotent request requires operator review before retry; inspect ${relativeRecordPath} and retry with a new Idempotency-Key only if the side effect did not land.`,
          "IDEMPOTENCY_REVIEW_REQUIRED"
        );
      } else if (existing.status === "failed") {
        reject(409, "Idempotent request failed previously; retry with a new Idempotency-Key after operator review", "IDEMPOTENCY_REVIEW_REQUIRED");
      } else if (existing.status === "review_required") {
        reject(
          409,
          `Idempotent request requires operator review before retry; inspect ${relativeRecordPath} and retry with a new Idempotency-Key only if the side effect did not land.`,
          "IDEMPOTENCY_REVIEW_REQUIRED"
        );
      } else {
        reject(425, "Idempotent request is already in progress; retry later", "IDEMPOTENCY_IN_PROGRESS");
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
        lease_until: new Date(Date.now() + idempotencyRunningLeaseMs).toISOString(),
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

async function completedIdempotencyReplay(
  request: Request,
  endpoint: "import" | "propose",
  requestHash: string
): Promise<Record<string, unknown> | null> {
  const rawKey = request.headers.get("idempotency-key")?.trim();
  if (rawKey && rawKey.length > 200) {
    reject(400, "Idempotency-Key is too long");
  }
  const effectiveKey = rawKey || `semantic-${requestHash}`;
  const paths = getRepoPaths(writeRepoRoot);
  const recordPath = join(paths.idempotencyDir, endpoint, `${sha256Text(effectiveKey)}.json`);
  const existing = await readJsonFile<IdempotencyRecord>(recordPath);
  if (!existing) {
    return null;
  }
  if (existing.endpoint !== endpoint || existing.request_hash !== requestHash) {
    reject(409, "Idempotency-Key was already used for a different request");
  }
  if (existing.status === "complete" && existing.response_body) {
    return { ...existing.response_body, idempotent_replay: true };
  }
  return null;
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

function assertNoRequestBody(request: Request, label: string): void {
  const rawLength = request.headers.get("content-length");
  if (rawLength) {
    const length = Number(rawLength);
    if (!Number.isFinite(length) || length < 0) {
      reject(400, `${label} has invalid content-length`);
    }
    if (length > 0) {
      reject(413, `${label} does not accept a request body`);
    }
  }
  if (!rawLength && request.body) {
    reject(413, `${label} does not accept a request body`);
  }
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
  let parsed: unknown;
  try {
    parsed = JSON.parse(new TextDecoder().decode(bytes)) as unknown;
  } catch {
    reject(400, "Request body must be valid JSON");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    reject(400, "Request body must be a JSON object");
  }
  return parsed as Record<string, unknown>;
}

function normalizeMultipartContentType(value: string): string {
  const [mediaType, ...parameters] = value.split(";");
  return [mediaType.trim().toLowerCase(), ...parameters].join(";");
}

async function readMultipartFormData(request: Request, contentType: string): Promise<FormData> {
  const normalizedContentType = normalizeMultipartContentType(contentType);
  const formRequest =
    normalizedContentType === contentType
      ? request
      : (() => {
          const headers = new Headers(request.headers);
          headers.set("content-type", normalizedContentType);
          return new Request(request.url, {
            method: request.method,
            headers,
            body: request.body
          });
        })();
  try {
    return await formRequest.formData();
  } catch (error) {
    reject(400, `Multipart request body could not be parsed: ${error instanceof Error ? error.message : String(error)}`);
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

// Compare fixed-length digests so token checks do not leak prefix-match timing.
function tokenEquals(presented: string, expected: string): boolean {
  const presentedHash = createHash("sha256").update(presented).digest();
  const expectedHash = createHash("sha256").update(expected).digest();
  return timingSafeEqual(presentedHash, expectedHash);
}

function requestAuthScope(request: Request): AuthScope | null {
  const token = request.headers.get("authorization")?.replace(/^Bearer\s+/i, "") || "";
  if (adminToken && tokenEquals(token, adminToken)) {
    return "admin";
  }
  if (importToken && tokenEquals(token, importToken)) {
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

function normalizeRateSource(value: string | null | undefined): string {
  const normalized = String(value || "")
    .trim()
    .replace(/[^\x20-\x7e]/g, "?")
    .slice(0, 200);
  return normalized || "unknown-source";
}

function writeRateSource(request: Request, sourceMachine?: string | null): string {
  if (sourceMachine) {
    return normalizeRateSource(sourceMachine);
  }
  const brainstackSource = request.headers.get("x-brainstack-source-machine");
  if (brainstackSource) {
    return normalizeRateSource(brainstackSource);
  }
  if (trustProxyHeaders) {
    return normalizeRateSource(request.headers.get("x-real-ip") || request.headers.get("x-forwarded-for"));
  }
  return "unknown-source";
}

function pruneRateMap(map: Map<string, number[]>, now: number): void {
  const windowStart = now - 60_000;
  const entries: Array<{ key: string; newest: number }> = [];
  for (const [key, timestamps] of map) {
    const recent = timestamps.filter((timestamp) => timestamp >= windowStart);
    if (!recent.length) {
      map.delete(key);
      continue;
    }
    map.set(key, recent);
    entries.push({ key, newest: Math.max(...recent) });
  }
  if (map.size <= writeRateLimitMaxKeys) {
    return;
  }
  for (const entry of entries.sort((a, b) => a.newest - b.newest)) {
    if (map.size <= writeRateLimitMaxKeys) {
      break;
    }
    map.delete(entry.key);
  }
}

function assertWriteRateLimit(request: Request, scope: AuthScope, endpoint: string, sourceMachine?: string | null): void {
  const token = request.headers.get("authorization")?.replace(/^Bearer\s+/i, "") || scope;
  const source = writeRateSource(request, sourceMachine);
  const tokenHash = sha256Text(token).slice(0, 16);
  const globalTokenKey = `${tokenHash}|${endpoint}`;
  const sourceTokenKey = `${tokenHash}|${sha256Text(source).slice(0, 16)}|${endpoint}`;
  const now = Date.now();
  pruneRateMap(writeTokenRateEvents, now);
  pruneRateMap(writeRateEvents, now);
  const windowStart = now - 60_000;
  const globalRecent = (writeTokenRateEvents.get(globalTokenKey) || []).filter((timestamp) => timestamp >= windowStart);
  if (globalRecent.length >= writeTokenRateLimitPerMinute) {
    reject(429, `write rate limit exceeded for this token/endpoint; limit=${writeTokenRateLimitPerMinute}/minute`);
  }
  const recent = (writeRateEvents.get(sourceTokenKey) || []).filter((timestamp) => timestamp >= windowStart);
  if (recent.length >= writeRateLimitPerMinute) {
    reject(429, `write rate limit exceeded for this token/source/endpoint; limit=${writeRateLimitPerMinute}/minute`);
  }
  globalRecent.push(now);
  writeTokenRateEvents.set(globalTokenKey, globalRecent);
  recent.push(now);
  writeRateEvents.set(sourceTokenKey, recent);
}

function assertPreBodyWriteRateLimit(request: Request, scope: AuthScope, endpoint: string): void {
  const token = request.headers.get("authorization")?.replace(/^Bearer\s+/i, "") || scope;
  const tokenHash = sha256Text(token).slice(0, 16);
  const key = `${tokenHash}|pre-body|${endpoint}`;
  const now = Date.now();
  pruneRateMap(writePreBodyRateEvents, now);
  const windowStart = now - 60_000;
  const recent = (writePreBodyRateEvents.get(key) || []).filter((timestamp) => timestamp >= windowStart);
  if (recent.length >= writeTokenRateLimitPerMinute) {
    reject(429, `pre-body write rate limit exceeded for this token/endpoint; limit=${writeTokenRateLimitPerMinute}/minute`);
  }
  recent.push(now);
  writePreBodyRateEvents.set(key, recent);
}

async function withMutationGate<T>(run: () => Promise<T>): Promise<T> {
  const startedAt = Date.now();
  while (activeWriteRequests >= writeConcurrencyLimit) {
    if (Date.now() - startedAt > mutationQueueWaitMs) {
      reject(503, `write queue timed out after ${mutationQueueWaitMs}ms; limit=${writeConcurrencyLimit}`);
    }
    await Bun.sleep(50);
  }
  activeWriteRequests += 1;
  try {
    return await run();
  } finally {
    activeWriteRequests = Math.max(0, activeWriteRequests - 1);
  }
}

async function withImportPreparationGate<T>(run: () => Promise<T>): Promise<T> {
  const startedAt = Date.now();
  while (activeImportPreparations >= importPreparationConcurrencyLimit) {
    if (Date.now() - startedAt > mutationQueueWaitMs) {
      reject(503, `import preparation queue timed out after ${mutationQueueWaitMs}ms; limit=${importPreparationConcurrencyLimit}`);
    }
    await Bun.sleep(50);
  }
  activeImportPreparations += 1;
  try {
    return await run();
  } finally {
    activeImportPreparations = Math.max(0, activeImportPreparations - 1);
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
    const targetCommit = commit;
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

async function recordPendingSearchRefresh(commit: string | null, error: string, generation: number): Promise<void> {
  await writeJsonFile(pendingReindexPath, {
    commit,
    error,
    generation,
    updated_at: isoNow()
  });
}

async function clearPendingSearchRefreshIfCurrent(generation: number): Promise<void> {
  const pending = await latestPendingReindexMarker();
  const pendingGeneration = Number(pending?.marker.generation || 0);
  if (!pending || pendingGeneration <= generation) {
    await rm(pendingReindexPath, { force: true }).catch(() => undefined);
  }
}

function queueSearchRefresh(commit: string | null): Record<string, unknown> {
  searchRefreshGeneration += 1;
  const generation = searchRefreshGeneration;
  searchRefreshRequested = true;
  if (commit) {
    searchRefreshRequestedCommit = commit;
  }
  searchRefreshMarkerWritePromise = recordPendingSearchRefresh(searchRefreshRequestedCommit, "queued", generation);
  void searchRefreshMarkerWritePromise.catch(() => undefined);
  if (!searchRefreshPromise) {
    searchRefreshPromise = (async () => {
      while (searchRefreshRequested) {
        const targetCommit = searchRefreshRequestedCommit;
        const targetGeneration = searchRefreshGeneration;
        const markerWrite = searchRefreshMarkerWritePromise;
        searchRefreshRequested = false;
        searchRefreshRequestedCommit = null;
        try {
          await markerWrite?.catch(() => undefined);
          await performSearchRefresh(targetCommit);
          if (!searchRefreshRequested) {
            await clearPendingSearchRefreshIfCurrent(targetGeneration);
          }
        } catch (error) {
          if (searchRefreshRequested || searchRefreshGeneration > targetGeneration) {
            await searchRefreshMarkerWritePromise?.catch(() => undefined);
            const pending = await latestPendingReindexMarker().catch(() => null);
            const pendingGeneration = Number(pending?.marker.generation || 0);
            if (searchRefreshRequested || pendingGeneration > targetGeneration) {
              continue;
            }
          }
          await recordPendingSearchRefresh(targetCommit, error instanceof Error ? error.message : String(error), targetGeneration).catch(() => undefined);
          break;
        }
      }
    })().finally(() => {
      searchRefreshPromise = null;
    });
  }
  return { ok: true, queued: true, target_commit: commit || searchRefreshRequestedCommit || null };
}

async function retryPendingSearchRefresh(): Promise<void> {
  const pending = await latestPendingReindexMarker();
  if (pending && !searchRefreshPromise) {
    searchRefreshGeneration = Math.max(searchRefreshGeneration, Number(pending.marker.generation || 0));
    queueSearchRefresh(typeof pending.marker.commit === "string" ? pending.marker.commit : null);
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

startupIndexPromise = ensureFreshStartupIndex().catch(async (error) => {
  await recordPendingSearchRefresh(
    await currentRepoCommit(repoRoot).catch(() => null),
    error instanceof Error ? error.message : String(error),
    searchRefreshGeneration
  ).catch(() => undefined);
  console.error("startup search index refresh failed", error);
});
startupIndexPromise.finally(() => {
  startupIndexPromise = null;
}).catch(() => undefined);

async function writeImport(
  input: Parameters<typeof createImportedArtifact>[1],
  beforeMutation?: () => Promise<void>
): Promise<Record<string, unknown>> {
  return await withMutationGate(async () =>
    await withRepoLock(writeRepoRoot, async () => {
      await syncWritableRepoAsync(writeRepoRoot);
      const imported = await createImportedArtifact(writeRepoRoot, input, { beforeMutation });
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
    })
  );
}

async function importFromRequest(request: Request, authScope: AuthScope): Promise<Record<string, unknown>> {
  const contentType = request.headers.get("content-type") || "";
  if (contentType.toLowerCase().includes("multipart/form-data")) {
    requireContentLength(request, maxImportBytes + 256 * 1024, "Multipart request body");
    const form = await readMultipartFormData(request, contentType);
    const file = form.get("file");
    if (!(file instanceof File)) {
      reject(400, "multipart request requires a file field");
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
    const requestHash = requestHashFor({ endpoint: "import", input });
    const replay = await completedIdempotencyReplay(request, "import", requestHash);
    if (replay) {
      return replay;
    }
    assertWriteRateLimit(request, authScope, "import", input.source_machine);
    return await withIdempotency(request, "import", requestHash, async (markSideEffectStarted) => {
      return await writeImport(input, markSideEffectStarted);
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
    const requestHash = requestHashFor({ endpoint: "import", input });
    const replay = await completedIdempotencyReplay(request, "import", requestHash);
    if (replay) {
      return replay;
    }
    assertWriteRateLimit(request, authScope, "import", input.source_machine);
    return await withIdempotency(request, "import", requestHash, async (markSideEffectStarted) => {
      return await writeImport(input, markSideEffectStarted);
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
    const requestHash = requestHashFor({ endpoint: "import-url", request: requestShape });
    const replay = await completedIdempotencyReplay(request, "import", requestHash);
    if (replay) {
      return replay;
    }
    assertWriteRateLimit(request, authScope, "import", requestShape.source_machine);
    return await withIdempotency(request, "import", requestHash, async (markSideEffectStarted) => {
      const upstream = await withImportPreparationGate(async () => safeFetchImportUrl(sourceUrl));
      if (!upstream.ok) {
        reject(400, `URL fetch failed: ${upstream.status} ${upstream.statusText}`);
      }
      const upstreamType = upstream.headers.get("content-type")?.split(";")[0];
      const bytes = await readResponseBytesCapped(upstream, "URL import response");
      return await writeImport({
        ...requestShape,
        bytes,
        fileName: fileNameForUrl(sourceUrl, upstreamType, title),
        contentType: upstreamType
      }, markSideEffectStarted);
    });
  }

  reject(400, "Import request must include text, url, or multipart file");
}

async function proposeFromRequest(request: Request, authScope: AuthScope): Promise<Record<string, unknown>> {
  const body = await readJsonBody(request);
  const input = {
    title: String(body.title || ""),
    body: String(body.body || ""),
    source_harness: String(body.source_harness || ""),
    source_machine: String(body.source_machine || ""),
    target_page: typeof body.target_page === "string" ? body.target_page : undefined,
    tags: Array.isArray(body.tags) ? body.tags.map(String) : []
  };
  const requestHash = requestHashFor({ endpoint: "propose", input });
  const replay = await completedIdempotencyReplay(request, "propose", requestHash);
  if (replay) {
    return replay;
  }
  assertWriteRateLimit(request, authScope, "propose", input.source_machine);
  return await withIdempotency(request, "propose", requestHash, async (markSideEffectStarted) => {
    return await withMutationGate(async () =>
      await withRepoLock(writeRepoRoot, async () => {
        await syncWritableRepoAsync(writeRepoRoot);
        const proposal = await createProposal(writeRepoRoot, input, { beforeMutation: markSideEffectStarted });
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
      })
    );
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
          <div>Storage: <code>local shared-brain repo</code></div>
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
  const freshness = await pendingReindexStatus();
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
    ${staleSearchNotice(freshness)}
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
  const freshness = await pendingReindexStatus();
  if (wantsJson(request, url)) {
    return json({ query, scope, limit, search_freshness: await publicPendingReindexStatus(), results });
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
    ${staleSearchNotice(freshness)}
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
      if (request.method === "GET" && (url.pathname === "/health" || url.pathname === "/healthz")) {
        return json(minimalHealth());
      }
      if (request.method === "GET" && (url.pathname === "/ready" || url.pathname === "/readyz")) {
        const health = await readinessHealth();
        return json(health, health.ready === true ? 200 : 503);
      }
      if (request.method === "GET" && (url.pathname === "/admin/health" || url.pathname === "/health/deep")) {
        assertAdminAuth(request);
        return json(await operationalHealth());
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
        assertPreBodyWriteRateLimit(request, authScope, "import");
        return json(await importFromRequest(request, authScope));
      }
      if (request.method === "POST" && url.pathname === "/api/ingest") {
        assertAdminAuth(request);
        assertWriteRateLimit(request, "admin", "ingest", null);
        const body = await readJsonBody(request);
        const artifactIds = Array.isArray(body.artifact_ids)
          ? body.artifact_ids.map(String)
          : body.artifact_id
            ? [String(body.artifact_id)]
            : [];
        if (!artifactIds.length) {
          reject(400, "artifact_id or artifact_ids is required");
        }
        for (const artifactId of artifactIds) {
          if (!isSafeManifestId(artifactId)) {
            reject(400, `artifact id is not a valid identifier: ${artifactId.slice(0, 128)}`);
          }
        }
        return json(
          await withMutationGate(async () => {
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
        assertPreBodyWriteRateLimit(request, authScope, "propose");
        return json(await proposeFromRequest(request, authScope));
      }
      if (request.method === "POST" && url.pathname === "/api/lint") {
        assertAdminAuth(request);
        assertWriteRateLimit(request, "admin", "lint", null);
        assertNoRequestBody(request, "lint");
        return json(
          await withMutationGate(async () => {
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
      return wantsJson(request, url)
        ? json({ error: `Unknown route: ${url.pathname}` }, 404)
        : errorResponse(404, `Unknown route: ${url.pathname}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (error instanceof HttpError) {
        return json({ error: message, ...(error.code ? { code: error.code } : {}) }, error.status);
      }
      if (message === "Unauthorized") {
        return json({ error: message }, 401);
      }
      if (message.startsWith("Forbidden")) {
        return json({ error: message }, 403);
      }
      return unexpectedErrorResponse(request, url, error);
    }
  }
});

console.log(`shared-brain listening on http://${server.hostname}:${server.port}`);
