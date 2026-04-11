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

const ARTIFACT_PATH_PATTERN = /`((?:\/|~\/|\.?[\w.-][^`\s]*\/)[^`]+)`|((?:\/|~\/|\.?[\w.-][^\s]*\/)\S+)/g;
const PHOTO_EXTENSIONS = new Set([".jpg", ".jpeg", ".png", ".webp"]);

function normalizePathCandidate(value: string): string {
  return value.trim().replace(/[),.;:]+$/, "");
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
    ARTIFACT_PATH_PATTERN.lastIndex = 0;

    for (const match of line.matchAll(ARTIFACT_PATH_PATTERN)) {
      const raw = match[1] || match[2];
      if (!raw) {
        continue;
      }

      const path = normalizePathCandidate(raw);
      if (!path || seen.has(path)) {
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
      const normalized = normalizeAttachmentRequest(entry);
      if (!normalized) {
        skipped.push("Ignored malformed attachment entry in TELEGRAM_ATTACHMENTS.json.");
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
