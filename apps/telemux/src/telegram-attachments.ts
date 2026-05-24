import { basename, extname } from "node:path";

export type TelegramAttachmentKind = "document" | "photo";

export interface TelegramAttachmentRequest {
  path: string;
  caption?: string | null;
  type?: TelegramAttachmentKind | null;
}

export interface ArtifactEntry {
  path: string;
  fileName: string;
  line: string;
}

export interface ParsedAttachmentManifest {
  requests: TelegramAttachmentRequest[];
  skipped: string[];
}

export const TELEGRAM_ATTACHMENTS_FILE_NAME = "TELEGRAM_ATTACHMENTS.json";
export const TELEGRAM_ATTACHMENTS_WORKSPACE_PATH = `.factory/${TELEGRAM_ATTACHMENTS_FILE_NAME}`;
export const MAX_TELEGRAM_ATTACHMENT_REQUESTS = 10;

const BACKTICK_ARTIFACT_PATH_PATTERN = /`([^`\n]+)`/g;
const UNQUOTED_ARTIFACT_PATH_PATTERN = /((?:\/|~\/|\.?[\w.-][^\s]*\/)\S+)/g;
const PHOTO_EXTENSIONS = new Set([".jpg", ".jpeg", ".png", ".webp"]);
const PROTECTED_FACTORY_FILES = new Set([
  "ARTIFACTS.md",
  "CRONS.md",
  "CRON_REQUESTS.json",
  "STATE.json",
  "SUMMARY.md",
  "TELEGRAM_ATTACHMENTS.json",
  "TODO.md",
  "control-plane.prompt.md",
  "last-message.txt"
]);
const SENSITIVE_FILE_PATTERN = /(?:^|[./_-])(?:id_rsa|id_ed25519|authorized_keys|known_hosts|token|secret|passwd|shadow|keyring)(?:$|[./_-])/i;

function normalizePathCandidate(value: string): string {
  return value.trim().replace(/[)`,.;:]+$/, "");
}

function isArtifactPathCandidate(value: string, quoted: boolean): boolean {
  if (!value || /\s/.test(value)) {
    return false;
  }

  if (isProtectedArtifactPath(value)) {
    return false;
  }

  if (value.startsWith("/") || value.startsWith("~/") || value.includes("/")) {
    return true;
  }

  return quoted && Boolean(extname(basename(value)));
}

export function isProtectedArtifactPath(value: string): boolean {
  const normalized = value.trim().replaceAll("\\", "/").replace(/^\.\/+/, "");
  if (!normalized || normalized.split("/").includes("..")) {
    return true;
  }

  if (normalized.startsWith("/") || normalized.startsWith("~/")) {
    return false;
  }

  const segments = normalized.split("/").filter(Boolean);
  const fileName = segments.at(-1) || "";
  const lowerFileName = fileName.toLowerCase();
  if (
    lowerFileName === ".env" ||
    lowerFileName.endsWith(".env") ||
    lowerFileName.endsWith(".pem") ||
    lowerFileName.endsWith(".key") ||
    SENSITIVE_FILE_PATTERN.test(normalized)
  ) {
    return true;
  }

  if (segments[0] === ".git") {
    return true;
  }

  if (segments[0] === ".factory") {
    return segments[1] !== "reports" || segments.length < 3 || segments.slice(2).some((segment) => segment.startsWith("."));
  }

  return segments.some((segment) => segment.startsWith("."));
}

function normalizeAttachmentRequest(input: unknown): TelegramAttachmentRequest | null {
  if (!input || typeof input !== "object") {
    return null;
  }

  const raw = input as Record<string, unknown>;
  const path = typeof raw.path === "string" ? raw.path.trim() : "";
  if (!path) {
    return null;
  }

  const caption = typeof raw.caption === "string" && raw.caption.trim() ? raw.caption.trim() : null;
  const type = raw.type === "photo" || raw.type === "document" ? raw.type : null;

  return { path, caption, type };
}

export function parseArtifactEntries(markdown: string | null): ArtifactEntry[] {
  if (!markdown) {
    return [];
  }

  const entries: ArtifactEntry[] = [];
  const seen = new Set<string>();

  for (const line of markdown.split("\n")) {
    for (const path of artifactPathCandidatesForLine(line)) {
      if (seen.has(path)) {
        continue;
      }

      seen.add(path);
      entries.push({
        path,
        fileName: basename(path),
        line: line.trim()
      });
    }
  }

  return entries;
}

function artifactPathCandidatesForLine(line: string): string[] {
  const candidates: string[] = [];
  BACKTICK_ARTIFACT_PATH_PATTERN.lastIndex = 0;
  UNQUOTED_ARTIFACT_PATH_PATTERN.lastIndex = 0;

  for (const match of line.matchAll(BACKTICK_ARTIFACT_PATH_PATTERN)) {
    const path = normalizePathCandidate(match[1] || "");
    if (isArtifactPathCandidate(path, true)) {
      candidates.push(path);
    }
  }

  for (const match of line.matchAll(UNQUOTED_ARTIFACT_PATH_PATTERN)) {
    const path = normalizePathCandidate(match[1] || "");
    if (isArtifactPathCandidate(path, false)) {
      candidates.push(path);
    }
  }

  return candidates;
}

export function removeArtifactEntriesFromMarkdown(markdown: string | null, paths: string[]): string {
  const remove = new Set(paths);
  if (!markdown || !remove.size) {
    return markdown || "# Artifacts\n";
  }

  const keptLines = markdown.split("\n").flatMap((line) => {
    const candidates = artifactPathCandidatesForLine(line);
    if (!candidates.some((path) => remove.has(path))) {
      return [line];
    }

    const remaining = candidates.filter((path) => !remove.has(path));
    return remaining.map((path) => `- \`${path}\``);
  });
  const kept = keptLines.join("\n").trimEnd();

  if (!parseArtifactEntries(kept).length) {
    return "# Artifacts\n";
  }

  return `${kept}\n`;
}

export function selectArtifactEntries(markdown: string | null, filterText: string | null): ArtifactEntry[] {
  const entries = parseArtifactEntries(markdown);
  const filter = filterText?.trim().toLowerCase() || "";

  if (!filter) {
    return entries;
  }

  return entries.filter((entry) => {
    const haystacks = [entry.path, entry.fileName, entry.line].map((value) => value.toLowerCase());
    return haystacks.some((haystack) => haystack.includes(filter));
  });
}

export function parseAttachmentManifest(text: string | null): ParsedAttachmentManifest {
  if (!text?.trim()) {
    return {
      requests: [],
      skipped: []
    };
  }

  try {
    const parsed = JSON.parse(text) as {
      attachments?: unknown;
    };

    const rawAttachments = Array.isArray(parsed.attachments) ? parsed.attachments : [];
    const requests: TelegramAttachmentRequest[] = [];
    const skipped: string[] = [];
    const seen = new Set<string>();

    for (const entry of rawAttachments) {
      if (requests.length >= MAX_TELEGRAM_ATTACHMENT_REQUESTS) {
        skipped.push(`Skipped attachment entries beyond the ${MAX_TELEGRAM_ATTACHMENT_REQUESTS} file limit.`);
        break;
      }
      const normalized = normalizeAttachmentRequest(entry);
      if (!normalized) {
        skipped.push("Ignored malformed attachment entry in TELEGRAM_ATTACHMENTS.json.");
        continue;
      }

      if (isProtectedArtifactPath(normalized.path)) {
        skipped.push(`Skipped protected attachment path: ${normalized.path}`);
        continue;
      }

      if (seen.has(normalized.path)) {
        continue;
      }

      seen.add(normalized.path);
      requests.push(normalized);
    }

    return { requests, skipped };
  } catch (error) {
    return {
      requests: [],
      skipped: [`Could not parse ${TELEGRAM_ATTACHMENTS_FILE_NAME}: ${error instanceof Error ? error.message : String(error)}`]
    };
  }
}

export function resolveManifestRequests(manifestText: string | null, artifactMarkdown: string | null): ParsedAttachmentManifest {
  const parsed = parseAttachmentManifest(manifestText);
  if (!parsed.requests.length) {
    return parsed;
  }

  const allowedPaths = new Set(parseArtifactEntries(artifactMarkdown).map((entry) => entry.path));
  const requests: TelegramAttachmentRequest[] = [];

  for (const request of parsed.requests) {
    if (!allowedPaths.has(request.path)) {
      parsed.skipped.push(`Skipped attachment not recorded in .factory/ARTIFACTS.md: ${request.path}`);
      continue;
    }

    requests.push(request);
  }

  return {
    requests,
    skipped: parsed.skipped
  };
}

export function preferredAttachmentKind(path: string, requestedType: TelegramAttachmentKind | null | undefined): TelegramAttachmentKind {
  if (requestedType === "document" || requestedType === "photo") {
    return requestedType;
  }

  return PHOTO_EXTENSIONS.has(extname(path).toLowerCase()) ? "photo" : "document";
}
