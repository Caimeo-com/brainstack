import type { ContextRecord } from "./db";
import type { TelegramBot, TelegramTarget } from "./telegram";
import { preferredAttachmentKind, TelegramAttachmentRequest } from "./telegram-attachments";
import type { WorkerService } from "./workers";

export interface AttachmentDeliveryResult {
  sent: string[];
  skipped: string[];
  failed: string[];
}

export async function deliverAttachmentRequests(
  workers: WorkerService,
  telegram: TelegramBot,
  context: ContextRecord,
  target: TelegramTarget,
  requests: TelegramAttachmentRequest[]
): Promise<AttachmentDeliveryResult> {
  const maxTotalBytes = 90 * 1024 * 1024;
  let totalBytes = 0;
  const result: AttachmentDeliveryResult = {
    sent: [],
    skipped: [],
    failed: []
  };

  for (const request of requests) {
    try {
      const file = await workers.readArtifactFile(context, request.path);
      if (totalBytes + file.sizeBytes > maxTotalBytes) {
        result.skipped.push(`Skipped ${request.path}: attachment batch would exceed ${maxTotalBytes} bytes.`);
        continue;
      }
      totalBytes += file.sizeBytes;
      await telegram.sendAttachment(target, {
        kind: preferredAttachmentKind(file.path, request.type),
        fileName: file.fileName,
        bytes: file.content,
        mimeType: file.mimeType,
        caption: request.caption || null
      });
      result.sent.push(file.fileName);
    } catch (error) {
      result.failed.push(`${request.path}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  return result;
}

export function formatAttachmentDeliveryIssues(result: AttachmentDeliveryResult): string | null {
  const lines: string[] = [];

  if (result.skipped.length) {
    lines.push("Attachment skips:");
    lines.push(...result.skipped.map((entry) => `- ${entry}`));
  }

  if (result.failed.length) {
    lines.push("Attachment failures:");
    lines.push(...result.failed.map((entry) => `- ${entry}`));
  }

  return lines.length ? lines.join("\n") : null;
}
