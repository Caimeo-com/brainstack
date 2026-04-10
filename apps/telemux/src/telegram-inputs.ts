import { basename, extname } from "node:path";
import type {
  TelegramAnimation,
  TelegramAudio,
  TelegramDocument,
  TelegramMessage,
  TelegramPhotoSize,
  TelegramVideo,
  TelegramVoice
} from "./telegram";

export const TELEGRAM_INBOUND_WORKSPACE_ROOT = ".factory/inbox/telegram";
export const TELEGRAM_MAX_INBOUND_FILE_BYTES = 20 * 1024 * 1024;
export const TELEGRAM_MAX_INBOUND_TOTAL_BYTES = 40 * 1024 * 1024;

export type TelegramInboundAttachmentKind = "photo" | "document" | "video" | "audio" | "voice" | "animation";

export interface TelegramInboundAttachment {
  kind: TelegramInboundAttachmentKind;
  fileId: string;
  fileUniqueId: string | null;
  fileName: string | null;
  mimeType: string | null;
  fileSize: number | null;
  width: number | null;
  height: number | null;
  durationSeconds: number | null;
  performer: string | null;
  title: string | null;
}

export interface TelegramInboundMessageInput {
  messageId: number;
  chatId: number;
  threadId: number | null;
  text: string | null;
  attachments: TelegramInboundAttachment[];
}

export interface TelegramPreparedAttachment extends TelegramInboundAttachment {
  telegramFilePath: string;
  workspacePath: string;
  attachedAsImage: boolean;
}

function trimmed(value: string | null | undefined): string | null {
  if (!value) {
    return null;
  }

  const normalized = value.trim();
  return normalized ? normalized : null;
}

function pickLargestPhoto(photos: TelegramPhotoSize[] | undefined): TelegramPhotoSize | null {
  if (!photos?.length) {
    return null;
  }

  return [...photos].sort((left, right) => {
    const leftWeight = (left.file_size || 0) + left.width * left.height;
    const rightWeight = (right.file_size || 0) + right.width * right.height;
    return rightWeight - leftWeight;
  })[0] || null;
}

function fromDocument(kind: "document", document: TelegramDocument): TelegramInboundAttachment {
  return {
    kind,
    fileId: document.file_id,
    fileUniqueId: document.file_unique_id || null,
    fileName: trimmed(document.file_name),
    mimeType: trimmed(document.mime_type),
    fileSize: document.file_size ?? null,
    width: null,
    height: null,
    durationSeconds: null,
    performer: null,
    title: null
  };
}

function fromVideo(kind: "video", video: TelegramVideo): TelegramInboundAttachment {
  return {
    kind,
    fileId: video.file_id,
    fileUniqueId: video.file_unique_id || null,
    fileName: trimmed(video.file_name),
    mimeType: trimmed(video.mime_type),
    fileSize: video.file_size ?? null,
    width: video.width ?? null,
    height: video.height ?? null,
    durationSeconds: video.duration ?? null,
    performer: null,
    title: null
  };
}

function fromAudio(kind: "audio", audio: TelegramAudio): TelegramInboundAttachment {
  return {
    kind,
    fileId: audio.file_id,
    fileUniqueId: audio.file_unique_id || null,
    fileName: trimmed(audio.file_name),
    mimeType: trimmed(audio.mime_type),
    fileSize: audio.file_size ?? null,
    width: null,
    height: null,
    durationSeconds: audio.duration ?? null,
    performer: trimmed(audio.performer),
    title: trimmed(audio.title)
  };
}

function fromVoice(kind: "voice", voice: TelegramVoice): TelegramInboundAttachment {
  return {
    kind,
    fileId: voice.file_id,
    fileUniqueId: voice.file_unique_id || null,
    fileName: null,
    mimeType: trimmed(voice.mime_type),
    fileSize: voice.file_size ?? null,
    width: null,
    height: null,
    durationSeconds: voice.duration ?? null,
    performer: null,
    title: null
  };
}

function fromAnimation(kind: "animation", animation: TelegramAnimation): TelegramInboundAttachment {
  return {
    kind,
    fileId: animation.file_id,
    fileUniqueId: animation.file_unique_id || null,
    fileName: trimmed(animation.file_name),
    mimeType: trimmed(animation.mime_type),
    fileSize: animation.file_size ?? null,
    width: animation.width ?? null,
    height: animation.height ?? null,
    durationSeconds: animation.duration ?? null,
    performer: null,
    title: null
  };
}

function extensionFromMime(mimeType: string | null): string {
  switch (mimeType) {
    case "image/jpeg":
      return ".jpg";
    case "image/png":
      return ".png";
    case "image/webp":
      return ".webp";
    case "image/gif":
      return ".gif";
    case "video/mp4":
      return ".mp4";
    case "audio/mpeg":
      return ".mp3";
    case "audio/ogg":
      return ".ogg";
    case "audio/wav":
    case "audio/x-wav":
      return ".wav";
    case "application/pdf":
      return ".pdf";
    default:
      return "";
  }
}

function sanitizeFileName(fileName: string): string {
  const normalized = fileName.replaceAll("\\", "/");
  const base = basename(normalized);
  const sanitized = base.replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "");
  return sanitized || "file";
}

export function telegramMessageText(message: TelegramMessage): string {
  return trimmed(message.text) || trimmed(message.caption) || "";
}

export function extractTelegramInput(message: TelegramMessage): TelegramInboundMessageInput | null {
  const attachments: TelegramInboundAttachment[] = [];
  const largestPhoto = pickLargestPhoto(message.photo);
  const text = telegramMessageText(message) || null;

  if (largestPhoto) {
    attachments.push({
      kind: "photo",
      fileId: largestPhoto.file_id,
      fileUniqueId: largestPhoto.file_unique_id || null,
      fileName: null,
      mimeType: "image/jpeg",
      fileSize: largestPhoto.file_size ?? null,
      width: largestPhoto.width ?? null,
      height: largestPhoto.height ?? null,
      durationSeconds: null,
      performer: null,
      title: null
    });
  }

  if (message.document) {
    attachments.push(fromDocument("document", message.document));
  }

  if (message.video) {
    attachments.push(fromVideo("video", message.video));
  }

  if (message.audio) {
    attachments.push(fromAudio("audio", message.audio));
  }

  if (message.voice) {
    attachments.push(fromVoice("voice", message.voice));
  }

  if (message.animation) {
    attachments.push(fromAnimation("animation", message.animation));
  }

  if (!text && !attachments.length) {
    return null;
  }

  return {
    messageId: message.message_id,
    chatId: message.chat.id,
    threadId: message.message_thread_id ?? null,
    text,
    attachments
  };
}

export function isAudioOnlyTelegramInput(input: TelegramInboundMessageInput): boolean {
  return input.attachments.length > 0 && input.attachments.every((attachment) => isAudioAttachment(attachment));
}

export function isAudioAttachment(attachment: TelegramInboundAttachment): boolean {
  return attachment.kind === "audio" || attachment.kind === "voice";
}

export function filterPhaseOneTelegramInput(input: TelegramInboundMessageInput): TelegramInboundMessageInput {
  return {
    ...input,
    attachments: input.attachments.filter((attachment) => !isAudioAttachment(attachment))
  };
}

export function isCodexImageAttachment(attachment: TelegramInboundAttachment): boolean {
  return attachment.kind === "photo" || Boolean(attachment.mimeType?.startsWith("image/"));
}

export function inferTelegramWorkspaceFileName(
  attachment: TelegramInboundAttachment,
  index: number,
  telegramFilePath: string
): string {
  const extFromTelegram = extname(basename(telegramFilePath)).toLowerCase();
  const extFromName = extname(attachment.fileName || "").toLowerCase();
  const inferredExt = extFromTelegram || extFromName || extensionFromMime(attachment.mimeType);

  if (attachment.fileName) {
    const base = sanitizeFileName(attachment.fileName);
    if (extname(base)) {
      return base;
    }

    return `${base}${inferredExt}`;
  }

  const stem = `${attachment.kind}-${index + 1}`;
  const ext = inferredExt || (attachment.kind === "photo" ? ".jpg" : "");
  return sanitizeFileName(`${stem}${ext}`);
}

export function telegramWorkspacePath(messageId: number, fileName: string): string {
  return `${TELEGRAM_INBOUND_WORKSPACE_ROOT}/${messageId}/${sanitizeFileName(fileName)}`;
}

export function telegramMetadataPath(messageId: number): string {
  return `${TELEGRAM_INBOUND_WORKSPACE_ROOT}/${messageId}/message.json`;
}

export function formatTelegramPromptSection(
  input: TelegramInboundMessageInput,
  attachments: TelegramPreparedAttachment[],
  metadataPath: string
): string {
  const lines: string[] = [
    "Telegram inbound message:",
    `- message_id: ${input.messageId}`,
    `- chat_id: ${input.chatId}`,
    `- thread_id: ${input.threadId ?? "none"}`
  ];

  if (input.text) {
    lines.push(`- user_text: ${JSON.stringify(input.text)}`);
  } else {
    lines.push("- user_text: none");
  }

  if (!attachments.length) {
    lines.push("- attachments: none");
  } else {
    lines.push("- attachments:");

    for (const attachment of attachments) {
      const details = [
        `kind=${attachment.kind}`,
        `path=${attachment.workspacePath}`,
        attachment.mimeType ? `mime=${attachment.mimeType}` : null,
        attachment.fileName ? `name=${attachment.fileName}` : null,
        attachment.fileSize !== null ? `bytes=${attachment.fileSize}` : null,
        attachment.attachedAsImage ? "attached_via=--image" : "attached_via=workspace"
      ].filter(Boolean);

      lines.push(`  - ${details.join(" ")}`);
    }
  }

  lines.push(`- metadata_path: ${metadataPath}`);
  lines.push("Use local tools to inspect staged non-image files when needed.");
  return lines.join("\n");
}
