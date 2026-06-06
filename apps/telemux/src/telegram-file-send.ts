import { constants, type Stats } from "node:fs";
import { lstat, open, rm } from "node:fs/promises";
import { basename, extname } from "node:path";
import type { FactoryConfig } from "./config";
import type { ContextRecord, FactoryDb } from "./db";
import type { TelegramBot, TelegramTarget } from "./telegram";
import type { TelegramAttachmentKind } from "./telegram-attachments";

export const TELEGRAM_OUTBOUND_FILE_MAX_BYTES = 45 * 1024 * 1024;

export interface TelegramFileSendOptions {
  filePath: string;
  caption: string | null;
  contextSlug: string | null;
  chatId: number | null;
  threadId: number | null;
  kind: TelegramAttachmentKind | null;
  displayName: string | null;
  maxBytes: number;
  allowSensitive: boolean;
  deleteAfterSend: boolean;
}

export interface TelegramFileSendResult {
  fileName: string;
  sizeBytes: number;
  mimeType: string;
  kind: TelegramAttachmentKind;
  target: {
    mode: "chat" | "context" | "control";
    contextSlug: string | null;
    chatId: number;
    threadId: number | null;
  };
  deleted: boolean;
}

export interface TelegramFileSender {
  sendAttachment(target: TelegramTarget, attachment: {
    kind: TelegramAttachmentKind;
    fileName: string;
    bytes: Uint8Array;
    mimeType?: string | null;
    caption?: string | null;
  }): Promise<void>;
}

const PHOTO_EXTENSIONS = new Set([".jpg", ".jpeg", ".png", ".webp"]);
const SENSITIVE_FILE_PATTERN = /(?:^|[./_-])(?:id_rsa|id_ed25519|authorized_keys|known_hosts|token|secret|passwd|shadow|keyring)(?:$|[./_-])/i;

function valueAfter(argv: string[], key: string): string | undefined {
  const index = argv.indexOf(key);
  if (index === -1) {
    return undefined;
  }
  const value = argv[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`${key} requires a value`);
  }
  return value;
}

function hasArg(argv: string[], key: string): boolean {
  return argv.includes(key);
}

function readOptionalNumber(argv: string[], key: string): number | null {
  const raw = valueAfter(argv, key);
  if (raw === undefined) {
    return null;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value)) {
    throw new Error(`${key} must be an integer`);
  }
  return value;
}

function readPositiveNumber(argv: string[], key: string, fallback: number): number {
  const raw = valueAfter(argv, key);
  if (raw === undefined) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${key} must be a positive integer`);
  }
  return value;
}

function normalizeKind(raw: string | undefined): TelegramAttachmentKind | null {
  if (raw === undefined) {
    return null;
  }
  if (raw === "document" || raw === "photo") {
    return raw;
  }
  throw new Error("--kind must be document or photo");
}

export function parseTelegramFileSendArgs(argv: string[]): TelegramFileSendOptions & { json: boolean } {
  const filePath = valueAfter(argv, "--file");
  if (!filePath) {
    throw new Error("send-file requires --file PATH");
  }
  const contextSlug = valueAfter(argv, "--context") || null;
  const chatId = readOptionalNumber(argv, "--chat-id");
  const threadId = readOptionalNumber(argv, "--thread-id");
  if (contextSlug && chatId !== null) {
    throw new Error("send-file accepts either --context or --chat-id, not both");
  }
  if (contextSlug && threadId !== null) {
    throw new Error("send-file uses the bound context thread; omit --thread-id with --context");
  }

  return {
    filePath,
    caption: valueAfter(argv, "--caption") || null,
    contextSlug,
    chatId,
    threadId,
    kind: normalizeKind(valueAfter(argv, "--kind")),
    displayName: valueAfter(argv, "--display-name") || null,
    maxBytes: readPositiveNumber(argv, "--max-bytes", TELEGRAM_OUTBOUND_FILE_MAX_BYTES),
    allowSensitive: hasArg(argv, "--allow-sensitive"),
    deleteAfterSend: hasArg(argv, "--delete-after-send"),
    json: hasArg(argv, "--json")
  };
}

function sanitizeFileName(value: string): string {
  const cleaned = basename(value)
    .replace(/[\u0000-\u001f\u007f]/g, "_")
    .replace(/[/:\\]/g, "_")
    .trim();
  return cleaned || "brainstack-file";
}

function isSensitiveFileName(value: string): boolean {
  const normalized = sanitizeFileName(value).toLowerCase();
  return (
    normalized.startsWith(".") ||
    normalized === ".env" ||
    normalized.endsWith(".env") ||
    normalized.endsWith(".pem") ||
    normalized.endsWith(".key") ||
    SENSITIVE_FILE_PATTERN.test(normalized)
  );
}

function mimeTypeForPath(path: string): string {
  switch (extname(path).toLowerCase()) {
    case ".txt":
    case ".log":
      return "text/plain";
    case ".md":
      return "text/markdown";
    case ".json":
      return "application/json";
    case ".csv":
      return "text/csv";
    case ".pdf":
      return "application/pdf";
    case ".jpg":
    case ".jpeg":
      return "image/jpeg";
    case ".png":
      return "image/png";
    case ".webp":
      return "image/webp";
    case ".gif":
      return "image/gif";
    case ".zip":
      return "application/zip";
    default:
      return "application/octet-stream";
  }
}

function preferredKind(path: string, requested: TelegramAttachmentKind | null): TelegramAttachmentKind {
  if (requested) {
    return requested;
  }
  return PHOTO_EXTENSIONS.has(extname(path).toLowerCase()) ? "photo" : "document";
}

function sameFileIdentity(left: Stats, right: Stats): boolean {
  return left.dev === right.dev && left.ino === right.ino;
}

function noFollowOpenFlag(): number {
  return typeof constants.O_NOFOLLOW === "number" ? constants.O_NOFOLLOW : 0;
}

function contextTarget(db: Pick<FactoryDb, "getContextBySlug">, slug: string): Pick<ContextRecord, "slug" | "telegramChatId" | "telegramThreadId"> {
  const context = db.getContextBySlug(slug);
  if (!context) {
    throw new Error(`unknown telemux context: ${slug}`);
  }
  if (context.telegramChatId === null) {
    throw new Error(`telemux context is not bound to Telegram: ${slug}`);
  }
  return context;
}

function resolveTarget(
  config: Pick<FactoryConfig, "telegramControlChatId">,
  db: Pick<FactoryDb, "getContextBySlug">,
  options: Pick<TelegramFileSendOptions, "chatId" | "threadId" | "contextSlug">
): TelegramFileSendResult["target"] {
  if (options.contextSlug && options.chatId !== null) {
    throw new Error("send-file accepts either --context or --chat-id, not both");
  }
  if (options.contextSlug && options.threadId !== null) {
    throw new Error("send-file uses the bound context thread; omit --thread-id with --context");
  }

  if (options.chatId !== null) {
    return {
      mode: "chat",
      contextSlug: null,
      chatId: options.chatId,
      threadId: options.threadId
    };
  }

  if (options.contextSlug) {
    const context = contextTarget(db, options.contextSlug);
    return {
      mode: "context",
      contextSlug: context.slug,
      chatId: context.telegramChatId as number,
      threadId: context.telegramThreadId
    };
  }

  if (config.telegramControlChatId === null) {
    throw new Error("FACTORY_TELEGRAM_CONTROL_CHAT_ID is required unless --context or --chat-id is supplied");
  }

  return {
    mode: "control",
    contextSlug: null,
    chatId: config.telegramControlChatId,
    threadId: options.threadId
  };
}

export async function sendTelegramLocalFile(
  config: Pick<FactoryConfig, "telegramBotToken" | "telegramControlChatId">,
  db: Pick<FactoryDb, "getContextBySlug">,
  telegram: TelegramFileSender | TelegramBot,
  options: TelegramFileSendOptions
): Promise<TelegramFileSendResult> {
  if (!config.telegramBotToken?.trim()) {
    throw new Error("FACTORY_TELEGRAM_BOT_TOKEN is required to send Telegram files");
  }

  const pathInfo = await lstat(options.filePath);
  if (pathInfo.isSymbolicLink()) {
    throw new Error(`refusing to send symlink: ${options.filePath}`);
  }
  if (!pathInfo.isFile()) {
    throw new Error(`not a regular file: ${options.filePath}`);
  }

  const sourceFileName = sanitizeFileName(basename(options.filePath));
  const fileName = sanitizeFileName(options.displayName || basename(options.filePath));
  if (!options.allowSensitive && (isSensitiveFileName(sourceFileName) || isSensitiveFileName(fileName))) {
    throw new Error(`refusing to send sensitive-looking file name: ${sourceFileName}${fileName !== sourceFileName ? ` as ${fileName}` : ""}; rerun with --allow-sensitive if intentional`);
  }

  const target = resolveTarget(config, db, options);
  const handle = await open(options.filePath, constants.O_RDONLY | noFollowOpenFlag()).catch((error) => {
    if (error && typeof error === "object" && "code" in error && error.code === "ELOOP") {
      throw new Error(`refusing to send symlink: ${options.filePath}`);
    }
    throw error;
  });
  let fileInfo: Stats;
  let bytes: Buffer;
  try {
    fileInfo = await handle.stat();
    if (!sameFileIdentity(pathInfo, fileInfo)) {
      throw new Error(`refusing to send file because it changed during validation: ${options.filePath}`);
    }
    if (!fileInfo.isFile()) {
      throw new Error(`not a regular file: ${options.filePath}`);
    }
    if (fileInfo.size > options.maxBytes) {
      throw new Error(`file too large: ${fileInfo.size} bytes > ${options.maxBytes}`);
    }
    bytes = await handle.readFile();
    if (bytes.byteLength > options.maxBytes) {
      throw new Error(`file too large: ${bytes.byteLength} bytes > ${options.maxBytes}`);
    }
  } finally {
    await handle.close();
  }
  const kind = preferredKind(fileName, options.kind);
  const mimeType = mimeTypeForPath(fileName);

  await telegram.sendAttachment(
    { chatId: target.chatId, threadId: target.threadId },
    {
      kind,
      fileName,
      bytes,
      mimeType,
      caption: options.caption
    }
  );

  let deleted = false;
  if (options.deleteAfterSend) {
    const deleteInfo = await lstat(options.filePath);
    if (deleteInfo.isSymbolicLink() || !sameFileIdentity(fileInfo, deleteInfo)) {
      throw new Error(`refusing to delete file because it changed after send: ${options.filePath}`);
    }
    await rm(options.filePath, { force: true });
    deleted = true;
  }

  return {
    fileName,
    sizeBytes: fileInfo.size,
    mimeType,
    kind,
    target,
    deleted
  };
}
