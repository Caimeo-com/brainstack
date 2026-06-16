import type { FactoryConfig } from "./config";
import type { TelegramBot } from "./telegram";
import {
  inferTelegramWorkspaceFileName,
  isAudioAttachment,
  TELEGRAM_MAX_INBOUND_FILE_BYTES,
  type TelegramInboundMessageInput
} from "./telegram-inputs";
import type { WorkerExecResult, WorkerService } from "./workers";

export interface TelegramTranscriptionSuccess {
  ok: true;
  transcript: string;
}

export interface TelegramTranscriptionFailure {
  ok: false;
  message: string;
}

export type TelegramTranscriptionResult = TelegramTranscriptionSuccess | TelegramTranscriptionFailure;

export function hasAudioTelegramInput(input: TelegramInboundMessageInput): boolean {
  return input.attachments.some((attachment) => isAudioAttachment(attachment));
}

export function withoutAudioTelegramInput(input: TelegramInboundMessageInput, text: string): TelegramInboundMessageInput | null {
  const attachments = input.attachments.filter((attachment) => !isAudioAttachment(attachment));
  const normalizedText = text.trim();
  if (!attachments.length && !normalizedText) {
    return null;
  }

  return {
    ...input,
    text: normalizedText || null,
    attachments
  };
}

export function mergeTelegramTextAndTranscript(text: string, transcript: string): string {
  return [text.trim(), transcript.trim()].filter(Boolean).join("\n\n");
}

export function formatTranscriptEcho(transcript: string, limit = 1800): string {
  const normalized = transcript.replace(/\s+/g, " ").trim();
  const body = normalized.length > limit ? `${normalized.slice(0, limit - 1)}…` : normalized;
  return `Transcribed voice:\n${body}`;
}

function stripAnsi(value: string): string {
  return value.replace(/\x1B\[[0-?]*[ -/]*[@-~]/g, "");
}

function cleanTranscriptOutput(value: string): string {
  return stripAnsi(value)
    .split(/\r?\n/)
    .map((line) => line.trim().replace(/^\[[^\]]+-->\s*[^\]]+\]\s*/, "").trim())
    .filter(Boolean)
    .join("\n")
    .trim();
}

function compact(value: string, limit = 280): string {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (normalized.length <= limit) {
    return normalized;
  }

  return `${normalized.slice(0, limit - 1)}…`;
}

function formatWorkerFailure(workerName: string, result: WorkerExecResult): string {
  const reason = result.stderr.trim() || result.stdout.trim() || `exit ${result.exitCode}`;
  const prefix = result.exitCode === 124 ? "Voice transcription timed out" : "Voice transcription failed";
  return `${prefix} on ${workerName}: ${compact(reason)}`;
}

function formatNoTranscript(workerName: string, result: WorkerExecResult): string {
  const diagnostics = result.stderr.trim() || result.stdout.trim();
  return diagnostics
    ? `Voice transcription on ${workerName} produced no text. Diagnostics: ${compact(diagnostics)}`
    : `Voice transcription on ${workerName} produced no text.`;
}

export async function transcribeTelegramAudioInput(options: {
  input: TelegramInboundMessageInput;
  config: FactoryConfig;
  telegram: TelegramBot;
  workers: WorkerService;
}): Promise<TelegramTranscriptionResult> {
  const { input, config, telegram, workers } = options;
  const audioAttachments = input.attachments.filter((attachment) => isAudioAttachment(attachment));
  if (!audioAttachments.length) {
    return { ok: true, transcript: "" };
  }

  if (!config.transcription.enabled) {
    return { ok: false, message: audioNotConfiguredText() };
  }

  const command = config.transcription.command.trim();
  if (!command) {
    return { ok: false, message: "Voice transcription is enabled, but FACTORY_TRANSCRIPTION_COMMAND is not set." };
  }

  const workerName = config.transcription.target === "worker" ? config.transcription.worker?.trim() : config.localMachine;
  if (!workerName) {
    return { ok: false, message: "Voice transcription target is worker, but FACTORY_TRANSCRIPTION_WORKER is not set." };
  }

  const transcripts: string[] = [];
  for (const [index, attachment] of audioAttachments.entries()) {
    const maxDuration = config.transcription.maxDurationSeconds;
    if (maxDuration !== null && attachment.durationSeconds !== null && attachment.durationSeconds > maxDuration) {
      return {
        ok: false,
        message: `Voice transcription skipped: audio is ${attachment.durationSeconds}s, over FACTORY_TRANSCRIPTION_MAX_DURATION_SECONDS=${maxDuration}.`
      };
    }

    const maxBytes = Math.min(config.transcription.maxBytes, TELEGRAM_MAX_INBOUND_FILE_BYTES);
    if (attachment.fileSize !== null && attachment.fileSize > maxBytes) {
      return {
        ok: false,
        message: `Voice transcription skipped: audio is ${attachment.fileSize} bytes, over FACTORY_TRANSCRIPTION_MAX_BYTES=${maxBytes}.`
      };
    }

    const remoteFile = await telegram.getFile(attachment.fileId);
    const reportedSize = remoteFile.file_size ?? attachment.fileSize ?? null;
    if (reportedSize !== null && reportedSize > maxBytes) {
      return {
        ok: false,
        message: `Voice transcription skipped: Telegram reports ${reportedSize} bytes, over FACTORY_TRANSCRIPTION_MAX_BYTES=${maxBytes}.`
      };
    }

    if (!remoteFile.file_path) {
      return { ok: false, message: `Telegram did not return a downloadable path for ${attachment.kind}.` };
    }

    const bytes = await telegram.downloadFile(remoteFile.file_path, maxBytes);
    if (bytes.byteLength > maxBytes) {
      return {
        ok: false,
        message: `Voice transcription skipped: downloaded audio is ${bytes.byteLength} bytes, over FACTORY_TRANSCRIPTION_MAX_BYTES=${maxBytes}.`
      };
    }

    const fileName = inferTelegramWorkspaceFileName(attachment, index, remoteFile.file_path);
    const result = await workers.runTranscription(workerName, {
      fileName,
      bytes,
      command,
      args: config.transcription.args,
      timeoutMs: config.transcription.timeoutMs
    });
    if (!result.ok) {
      return { ok: false, message: formatWorkerFailure(workerName, result) };
    }

    const transcript = cleanTranscriptOutput(result.stdout);
    if (!transcript) {
      return { ok: false, message: formatNoTranscript(workerName, result) };
    }

    transcripts.push(transcript);
  }

  return { ok: true, transcript: transcripts.join("\n\n") };
}

function audioNotConfiguredText(): string {
  return [
    "Audio and voice Telegram messages are not forwarded to the harness until transcription is installed.",
    "Run `install voice on <machine>` here, or run `brainctl capabilities install voice --target <machine>` on an enrolled Brainstack machine."
  ].join("\n");
}
