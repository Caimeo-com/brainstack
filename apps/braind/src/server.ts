import { existsSync } from "node:fs";
import { readdir } from "node:fs/promises";
import { lookup } from "node:dns/promises";
import { isIP } from "node:net";
import { basename, extname, join, resolve } from "node:path";
import {
  backlinksForPath,
  createImportedArtifact,
  createProposal,
  findSourceManifestById,
  getHealth,
  getRecentImports,
  getRecentLogEntries,
  getRepoRoot,
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
  syncWritableRepo,
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
const legacyWriteToken = process.env.BRAIN_WRITE_TOKEN || "";
const importToken = process.env.BRAIN_IMPORT_TOKEN || legacyWriteToken || "";
const adminToken = process.env.BRAIN_ADMIN_TOKEN || legacyWriteToken || "";
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

if (!existsSync(join(repoRoot, "derived", "search.sqlite"))) {
  await rebuildIndex(repoRoot);
}

function htmlEscape(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function pageHref(repoPath: string): string {
  return `/page/${encodeURIComponent(repoPath).replace(/%2F/g, "/")}`;
}

function rawHref(repoPath: string): string {
  return `/raw/${encodeURIComponent(repoPath).replace(/%2F/g, "/")}`;
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
    const resolved = resolveInternalLinkPath(currentPath, target);
    if (resolved) {
      return `<a href="${pageHref(resolved)}">${htmlEscape(label)}</a>`;
    }
    const safeTarget = htmlEscape(target);
    return `<a href="${safeTarget}" target="_blank" rel="noreferrer">${htmlEscape(label)}</a>`;
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
  return new Response(html, { headers: { "Content-Type": "text/html; charset=utf-8" } });
}

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" }
  });
}

function textResponse(body: string, status = 200): Response {
  return new Response(body, { status, headers: { "Content-Type": "text/plain; charset=utf-8" } });
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

function assertImportSize(size: number, label: string): void {
  if (size > maxImportBytes) {
    reject(413, `${label} is too large: ${size} bytes exceeds BRAIN_MAX_IMPORT_BYTES=${maxImportBytes}`);
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

async function assertSafeImportUrl(input: string): Promise<URL> {
  let url: URL;
  try {
    url = new URL(input);
  } catch {
    reject(400, "URL import requires a valid URL");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    reject(400, "URL import only allows http and https URLs");
  }
  if (!allowPrivateUrlImports) {
    const hostname = url.hostname.toLowerCase();
    if (hostname === "localhost" || hostname.endsWith(".localhost")) {
      reject(400, "URL import blocked private hostname; set BRAIN_ALLOW_PRIVATE_URL_IMPORTS=true only for trusted private fetches");
    }
    const directIpVersion = isIP(hostname);
    let addresses: Array<{ address: string }>;
    try {
      addresses = directIpVersion
        ? [{ address: hostname }]
        : await lookup(hostname, { all: true, verbatim: true });
    } catch (error) {
      reject(400, `URL import DNS lookup failed: ${error instanceof Error ? error.message : String(error)}`);
    }
    const blocked = addresses.find((entry) => isBlockedIp(entry.address));
    if (blocked) {
      reject(400, `URL import blocked private address ${blocked.address}; set BRAIN_ALLOW_PRIVATE_URL_IMPORTS=true only for trusted private fetches`);
    }
  }
  return url;
}

async function safeFetchImportUrl(input: string, redirectsLeft = 5): Promise<Response> {
  const url = await assertSafeImportUrl(input);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), urlFetchTimeoutMs);
  let response: Response;
  try {
    response = await fetch(url, { redirect: "manual", signal: controller.signal });
  } catch (error) {
    reject(400, `URL import fetch failed: ${error instanceof Error ? error.message : String(error)}`);
  } finally {
    clearTimeout(timeout);
  }
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

async function importFromRequest(request: Request, authScope: AuthScope): Promise<Record<string, unknown>> {
  const contentType = request.headers.get("content-type") || "";
  if (contentType.includes("multipart/form-data")) {
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
    return await withRepoLock(writeRepoRoot, async () => {
      syncWritableRepo(writeRepoRoot);
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
      return { ok: true, artifact_id: imported.artifactId, commit, touched_files: touchedFiles };
    });
  }

  const body = (await request.json()) as Record<string, unknown>;
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
    return await withRepoLock(writeRepoRoot, async () => {
      syncWritableRepo(writeRepoRoot);
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
      return { ok: true, artifact_id: imported.artifactId, commit, touched_files: touchedFiles };
    });
  }

  if (typeof body.url === "string") {
    const sourceUrl = body.url;
    const upstream = await safeFetchImportUrl(sourceUrl);
    if (!upstream.ok) {
      reject(400, `URL fetch failed: ${upstream.status} ${upstream.statusText}`);
    }
    const upstreamType = upstream.headers.get("content-type")?.split(";")[0];
    const title = typeof body.title === "string" ? body.title : sourceUrl;
    const bytes = await readResponseBytesCapped(upstream, "URL import response");
    const input = {
      title,
      url: sourceUrl,
      bytes,
      fileName: fileNameForUrl(sourceUrl, upstreamType, title),
      contentType: upstreamType,
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
    return await withRepoLock(writeRepoRoot, async () => {
      syncWritableRepo(writeRepoRoot);
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
      return { ok: true, artifact_id: imported.artifactId, commit, touched_files: touchedFiles };
    });
  }

  throw new Error("Import request must include text, url, or multipart file");
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
  const absolutePath = safeRepoPath(repoRoot, repoPath);
  const file = Bun.file(absolutePath);
  if (!(await file.exists())) {
    return errorResponse(404, `Raw file not found: ${repoPath}`);
  }
  const mime = file.type || "application/octet-stream";
  if (mime.startsWith("text/") || mime.includes("json") || [".md", ".txt", ".html", ".json"].includes(extname(repoPath).toLowerCase())) {
    return new Response(await file.text(), {
      headers: { "Content-Type": `${mime || "text/plain"}; charset=utf-8` }
    });
  }
  const headers = new Headers({ "Content-Type": mime });
  if (!mime.startsWith("image/") && mime !== "application/pdf") {
    headers.set("Content-Disposition", `attachment; filename="${basename(repoPath)}"`);
  }
  return new Response(file, { headers });
}

async function renderSearch(request: Request, url: URL): Promise<Response> {
  const query = (url.searchParams.get("q") || "").trim();
  const scope = (url.searchParams.get("scope") || "all") as "wiki" | "raw" | "all";
  const limit = Number(url.searchParams.get("limit") || 10);
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
        return json(await importFromRequest(request, authScope));
      }
      if (request.method === "POST" && url.pathname === "/api/ingest") {
        assertAdminAuth(request);
        const body = (await request.json()) as Record<string, unknown>;
        const artifactIds = Array.isArray(body.artifact_ids)
          ? body.artifact_ids.map(String)
          : body.artifact_id
            ? [String(body.artifact_id)]
            : [];
        if (!artifactIds.length) {
          return json({ error: "artifact_id or artifact_ids is required" }, 400);
        }
        const result = await withRepoLock(writeRepoRoot, async () => {
          syncWritableRepo(writeRepoRoot);
          const ingest = await ingestArtifacts(writeRepoRoot, artifactIds);
          const commit = await gitCommitAndPush(writeRepoRoot, ingest.touchedFiles, `brain: ingest ${artifactIds.join(", ")}`);
          return { ok: true, artifact_ids: artifactIds, commit, touched_files: ingest.touchedFiles };
        });
        return json(result);
      }
      if (request.method === "POST" && url.pathname === "/api/propose") {
        assertImportAuth(request);
        const body = (await request.json()) as Record<string, unknown>;
        const result = await withRepoLock(writeRepoRoot, async () => {
          syncWritableRepo(writeRepoRoot);
          const proposal = await createProposal(writeRepoRoot, {
            title: String(body.title || ""),
            body: String(body.body || ""),
            source_harness: String(body.source_harness || ""),
            source_machine: String(body.source_machine || ""),
            target_page: typeof body.target_page === "string" ? body.target_page : undefined,
            tags: Array.isArray(body.tags) ? body.tags.map(String) : []
          });
          const commit = await gitCommitAndPush(
            writeRepoRoot,
            proposal.touchedFiles,
            `brain: propose ${String(body.title || "proposal")}`
          );
          return { ok: true, proposal_path: proposal.proposalPath, commit, touched_files: proposal.touchedFiles };
        });
        return json(result);
      }
      if (request.method === "POST" && url.pathname === "/api/lint") {
        assertAdminAuth(request);
        const result = await withRepoLock(writeRepoRoot, async () => {
          syncWritableRepo(writeRepoRoot);
          const lint = await lintWiki(writeRepoRoot);
          const commit = await gitCommitAndPush(writeRepoRoot, lint.touchedFiles, `brain: lint ${isoNow()}`);
          return { ok: true, report_path: lint.reportPath, commit, touched_files: lint.touchedFiles };
        });
        return json(result);
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
