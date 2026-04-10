import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { deliverAttachmentRequests, formatAttachmentDeliveryIssues } from "./attachment-delivery";
import { CronManager } from "./cron-manager";
import { CONTEXT_CRONS_FILE_NAME, CONTEXT_CRONS_WORKSPACE_PATH, CRON_REQUESTS_FILE_NAME, CRON_REQUESTS_WORKSPACE_PATH } from "./cron-jobs";
import { ContextRecord, FactoryDb } from "./db";
import { ContextService } from "./contexts";
import { resolveManifestRequests, TELEGRAM_ATTACHMENTS_FILE_NAME, TELEGRAM_ATTACHMENTS_WORKSPACE_PATH } from "./telegram-attachments";
import {
  formatTelegramPromptSection,
  inferTelegramWorkspaceFileName,
  isCodexImageAttachment,
  TELEGRAM_MAX_INBOUND_FILE_BYTES,
  TELEGRAM_MAX_INBOUND_TOTAL_BYTES,
  telegramMetadataPath,
  telegramWorkspacePath,
  type TelegramInboundMessageInput,
  type TelegramPreparedAttachment
} from "./telegram-inputs";
import { WorkerService, type WorkspaceSeedFile } from "./workers";
import { summarizeUsage } from "./usage";
import type { FactoryConfig } from "./config";
import type { TelegramBot, TelegramTarget } from "./telegram";

export type DispatchMode = "run" | "resume" | "loop";

export interface DispatchResponse {
  accepted: boolean;
  message: string;
}

export interface DispatchOptions {
  notifyAccepted?: boolean;
  telegramInput?: TelegramInboundMessageInput | null;
  modelOverride?: string | null;
  reasoningEffortOverride?: ContextRecord["reasoningEffortOverride"];
  sourceLabel?: string | null;
}

function nowStamp(): string {
  return new Date().toISOString().replaceAll(":", "-");
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function snippet(text: string | null, limit = 240): string {
  if (!text) {
    return "n/a";
  }

  const normalized = text.replace(/\s+/g, " ").trim();
  if (normalized.length <= limit) {
    return normalized;
  }

  return `${normalized.slice(0, limit - 1)}…`;
}

function defaultInstruction(context: ContextRecord, mode: DispatchMode): string {
  if (mode === "resume") {
    return context.codexSessionId
      ? "Resume the current session, reread the durable context files, and continue from the active TODO."
      : "Start a new session for this topic, read the durable context files, inspect the workspace, and continue from the active TODO.";
  }

  if (mode === "loop") {
    return "Continue working until you reach a real blocker or a clean reviewable checkpoint, then leave durable notes for the next run.";
  }

  return "";
}

function buildPrompt(context: ContextRecord, mode: DispatchMode, instruction: string, telegramPromptSection: string | null = null): string {
  const autonomousNote =
    mode === "loop"
      ? "Keep working until you hit a genuine blocker or you reach a clean reviewable checkpoint."
      : "Work the instruction to the next useful checkpoint.";

  const scopeNote =
    context.kind === "repo"
      ? `You are working inside the repo context workspace for ${context.slug}.`
      : `You are working inside the managed ${context.kind} workspace for ${context.slug}.`;

  const sections = [
    scopeNote,
    `Machine: ${context.machine}.`,
    `Transport: ${context.transport || "n/a"}.`,
    "Durable context state lives in .factory/STATE.json, .factory/SUMMARY.md, .factory/TODO.md, and .factory/ARTIFACTS.md.",
    `If ${CONTEXT_CRONS_WORKSPACE_PATH} exists, read it too before making scheduling changes.`,
    "Start by reading those files and the current git status.",
    "Before finishing, update all relevant .factory files so the next run can resume cleanly.",
    "Record artifact paths in .factory/ARTIFACTS.md.",
    `These messages are coming through Telegram. If the user explicitly asks you to send or attach a file into the Telegram thread, keep your normal answer and also write ${TELEGRAM_ATTACHMENTS_WORKSPACE_PATH} as JSON like {"attachments":[{"path":"/absolute/path/to/file","caption":"optional short caption","type":"document"}]}.`,
    `Only list regular files that already exist and are already recorded in .factory/ARTIFACTS.md. Use type "photo" for images only when you want Telegram to render them inline. Do not create ${TELEGRAM_ATTACHMENTS_FILE_NAME} unless the user explicitly asked for a Telegram attachment.`,
    `If the user explicitly asks to create, change, move, pause, resume, or delete a scheduled job, keep your normal answer and also write ${CRON_REQUESTS_WORKSPACE_PATH} as JSON like {"actions":[{"type":"create","job":{"label":"example","kind":"reminder","schedule":{"type":"once","at":"2026-04-08T09:00:00+02:00"},"reminderText":"Example reminder"}}]}.`,
    `Use exact cron ids from ${CONTEXT_CRONS_FILE_NAME} when updating existing jobs if they are available. Do not create ${CRON_REQUESTS_FILE_NAME} unless the user explicitly asked about scheduled jobs.`,
    autonomousNote,
    `Control-plane mode: ${mode}.`,
    "Instruction:",
    instruction.trim()
  ];

  if (telegramPromptSection) {
    sections.push(telegramPromptSection);
  }

  return sections.join("\n\n");
}

interface PreparedTelegramInput {
  promptSection: string;
  workspaceFiles: WorkspaceSeedFile[];
  imagePaths: string[];
}

export class Dispatcher {
  private readonly activeJobs = new Map<string, Promise<void>>();

  constructor(
    private readonly config: FactoryConfig,
    private readonly db: FactoryDb,
    private readonly contexts: ContextService,
    private readonly workers: WorkerService,
    private readonly telegram: TelegramBot,
    private readonly cronManager: CronManager
  ) {}

  isActive(slug: string): boolean {
    return this.activeJobs.has(slug);
  }

  async dispatch(
    mode: DispatchMode,
    context: ContextRecord,
    instruction: string,
    replyTarget: TelegramTarget,
    options: DispatchOptions = {}
  ): Promise<DispatchResponse> {
    if (context.state === "archived") {
      return {
        accepted: false,
        message: `${context.slug} is archived. Rebind the topic or create a new context first.`
      };
    }

    const trimmedInstruction = instruction.trim() || defaultInstruction(context, mode);
    if (!trimmedInstruction) {
      return {
        accepted: false,
        message: `Usage: /${mode} <instruction>`
      };
    }

    if (this.activeJobs.has(context.slug)) {
      return {
        accepted: false,
        message: `${context.slug} already has an active job. Use /topicinfo or /tail.`
      };
    }

    await mkdir(this.config.logsDir, { recursive: true });
    const logPath = resolve(this.config.logsDir, `${nowStamp()}-${context.slug}-${mode}.log`);

    const savedContext = this.contexts.saveContext({
      ...context,
      latestRunLogPath: logPath,
      lastRunAt: new Date().toISOString(),
      lastError: null
    });

    const job = this.runJob(
      mode,
      savedContext,
      trimmedInstruction,
      replyTarget,
      logPath,
      options.telegramInput || null,
      options
    );
    this.activeJobs.set(savedContext.slug, job);
    void job.finally(() => this.activeJobs.delete(savedContext.slug));

    return {
      accepted: true,
      message:
        options.notifyAccepted === false
          ? ""
          : [
              `Dispatched ${mode} for ${savedContext.slug}.`,
              `Machine: ${savedContext.machine}`,
              `Log: ${logPath}`
            ].join("\n")
    };
  }

  private async runJob(
    mode: DispatchMode,
    context: ContextRecord,
    instruction: string,
    replyTarget: TelegramTarget,
    logPath: string,
    telegramInput: TelegramInboundMessageInput | null,
    options: DispatchOptions
  ): Promise<void> {
    const stopHeartbeat = this.startTypingHeartbeat(replyTarget);

    try {
      const ensured = await this.workers.ensureContext(context);
      const freshContext = this.db.getContextBySlug(context.slug) || context;

      if (!ensured.ok) {
        const pendingOrError = this.contexts.saveContext({
          ...freshContext,
          kind: ensured.kind,
          state: ensured.state,
          transport: ensured.transport,
          target: ensured.target,
          rootPath: ensured.rootPath,
          worktreePath: ensured.worktreePath,
          branchName: ensured.branchName,
          baseBranch: ensured.baseBranch,
          latestRunLogPath: logPath,
          lastRunAt: new Date().toISOString(),
          lastError: ensured.stderr.trim() || ensured.stdout.trim() || `exit ${ensured.exitCode}`
        });

        await this.telegram.sendText(
          replyTarget,
          [
            `${pendingOrError.slug} on ${pendingOrError.machine} is ${pendingOrError.state}.`,
            `Transport: ${ensured.transport}`,
            `Exit: ${ensured.exitCode}`,
            pendingOrError.lastError || "unknown error"
          ].join("\n")
        );
        return;
      }

      const readyContext = this.contexts.saveContext({
        ...freshContext,
        kind: ensured.kind,
        state: "active",
        transport: ensured.transport,
        target: ensured.target,
        rootPath: ensured.rootPath,
        worktreePath: ensured.worktreePath,
        branchName: ensured.branchName,
        baseBranch: ensured.baseBranch,
        latestRunLogPath: logPath,
        lastRunAt: new Date().toISOString(),
        lastError: null
      });

      let preparedTelegramInput: PreparedTelegramInput | null = null;

      if (telegramInput?.attachments.length) {
        try {
          preparedTelegramInput = await this.prepareTelegramInput(telegramInput);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          this.contexts.saveContext({
            ...readyContext,
            latestRunLogPath: logPath,
            lastRunAt: new Date().toISOString(),
            lastError: message
          });
          await this.telegram.sendText(replyTarget, `Failed to prepare Telegram input: ${message}`);
          return;
        }
      }

      const prompt = buildPrompt(readyContext, mode, instruction, preparedTelegramInput?.promptSection || null);
      const result = await this.workers.runCodex(readyContext, prompt, mode, logPath, {
        workspaceFiles: preparedTelegramInput?.workspaceFiles,
        imagePaths: preparedTelegramInput?.imagePaths,
        modelOverride: options.modelOverride ?? readyContext.modelOverride,
        reasoningEffortOverride: options.reasoningEffortOverride ?? readyContext.reasoningEffortOverride,
        onSessionId: async (sessionId) => {
          const current = this.db.getContextBySlug(context.slug) || readyContext;
          this.contexts.saveContext({
            ...current,
            codexSessionId: sessionId,
            latestRunLogPath: logPath,
            lastRunAt: current.lastRunAt || new Date().toISOString()
          });
        }
      });
      const afterRunContext = this.db.getContextBySlug(context.slug) || readyContext;

      if (result.ok) {
        const summary = await this.workers.readFactoryFile(afterRunContext, "SUMMARY.md");
        const artifacts = await this.workers.readFactoryFile(afterRunContext, "ARTIFACTS.md");
        const lastMessage = await this.workers.readWorkspaceFile(afterRunContext, ".factory/last-message.txt");
        const attachmentManifest = await this.workers.readFactoryFile(afterRunContext, TELEGRAM_ATTACHMENTS_FILE_NAME);
        const cronManifest = await this.workers.readFactoryFile(afterRunContext, CRON_REQUESTS_FILE_NAME);
        const saved = this.contexts.saveContext({
          ...afterRunContext,
          state: "active",
          latestRunLogPath: logPath,
          lastSummary: snippet(summary),
          lastArtifacts: snippet(artifacts),
          codexSessionId: result.sessionId,
          lastRunAt: new Date().toISOString(),
          lastError: null
        });

        const usage = await summarizeUsage(saved);
        const reply = (lastMessage || summary || "").trim() || `${saved.slug} completed.`;
        await this.telegram.sendText(
          replyTarget,
          [
            reply,
            "",
            `session=${saved.codexSessionId || "n/a"} | machine=${saved.machine} | usage=${usage.adapter}${options.sourceLabel ? ` | source=${options.sourceLabel}` : ""}`
          ].join("\n")
        );

        await this.sendTelegramAttachments(saved, replyTarget, artifacts, attachmentManifest);
        await this.applyCronManifest(saved, replyTarget, cronManifest);
        return;
      }

      const saved = this.contexts.saveContext({
        ...afterRunContext,
        state: this.workers.isReachabilityFailure(result) ? "pending" : "error",
        latestRunLogPath: logPath,
        lastRunAt: new Date().toISOString(),
        lastError: result.stderr.trim() || result.stdout.trim() || `exit ${result.exitCode}`
      });

      await this.telegram.sendText(
        replyTarget,
        [
          `${saved.slug} failed on ${saved.machine}.`,
          `state=${saved.state} transport=${result.transport} exit=${result.exitCode}`,
          saved.lastError || "unknown error"
        ].join("\n")
      );
    } finally {
      stopHeartbeat();
    }
  }

  private async applyCronManifest(context: ContextRecord, replyTarget: TelegramTarget, manifestText: string | null): Promise<void> {
    const notes = await this.cronManager.applyManifest(manifestText, {
      context,
      target: replyTarget
    });

    if (notes.length) {
      await this.telegram.sendText(replyTarget, notes.map((note) => `Cron: ${note}`).join("\n"));
    }
  }

  private async sendTelegramAttachments(
    context: ContextRecord,
    replyTarget: TelegramTarget,
    artifactMarkdown: string | null,
    manifestText: string | null
  ): Promise<void> {
    const resolved = resolveManifestRequests(manifestText, artifactMarkdown);
    if (!resolved.requests.length && !resolved.skipped.length) {
      return;
    }

    const delivery = await deliverAttachmentRequests(this.workers, this.telegram, context, replyTarget, resolved.requests);
    delivery.skipped.push(...resolved.skipped);

    const notes = formatAttachmentDeliveryIssues(delivery);
    if (notes) {
      await this.telegram.sendText(replyTarget, notes);
    }
  }

  private startTypingHeartbeat(replyTarget: TelegramTarget): () => void {
    let stopped = false;

    void (async () => {
      while (!stopped) {
        try {
          await this.telegram.sendChatAction(replyTarget, "typing");
        } catch (error) {
          console.error("telegram typing heartbeat failed", error);
        }

        if (stopped) {
          return;
        }

        await delay(4000);
      }
    })();

    return () => {
      stopped = true;
    };
  }

  private async prepareTelegramInput(input: TelegramInboundMessageInput): Promise<PreparedTelegramInput> {
    const workspaceFiles: WorkspaceSeedFile[] = [];
    const preparedAttachments: TelegramPreparedAttachment[] = [];
    let totalBytes = 0;

    for (const [index, attachment] of input.attachments.entries()) {
      const remoteFile = await this.telegram.getFile(attachment.fileId);
      const reportedSize = remoteFile.file_size ?? attachment.fileSize ?? null;

      if (reportedSize !== null && reportedSize > TELEGRAM_MAX_INBOUND_FILE_BYTES) {
        throw new Error(`Telegram file exceeds ${TELEGRAM_MAX_INBOUND_FILE_BYTES} bytes: ${attachment.kind}`);
      }

      if (!remoteFile.file_path) {
        throw new Error(`Telegram did not return a downloadable path for ${attachment.kind}`);
      }

      const bytes = await this.telegram.downloadFile(remoteFile.file_path);
      if (bytes.byteLength > TELEGRAM_MAX_INBOUND_FILE_BYTES) {
        throw new Error(`Downloaded Telegram file exceeds ${TELEGRAM_MAX_INBOUND_FILE_BYTES} bytes`);
      }

      totalBytes += bytes.byteLength;
      if (totalBytes > TELEGRAM_MAX_INBOUND_TOTAL_BYTES) {
        throw new Error(`Telegram input exceeds ${TELEGRAM_MAX_INBOUND_TOTAL_BYTES} total bytes`);
      }

      const fileName = inferTelegramWorkspaceFileName(attachment, index, remoteFile.file_path);
      const workspacePath = telegramWorkspacePath(input.messageId, fileName);
      const attachedAsImage = isCodexImageAttachment(attachment);

      workspaceFiles.push({
        relativePath: workspacePath,
        content: bytes
      });

      preparedAttachments.push({
        ...attachment,
        fileSize: attachment.fileSize ?? remoteFile.file_size ?? bytes.byteLength,
        telegramFilePath: remoteFile.file_path,
        workspacePath,
        attachedAsImage
      });
    }

    const metadataPath = telegramMetadataPath(input.messageId);
    const metadataText = JSON.stringify(
      {
        source: "telegram",
        messageId: input.messageId,
        chatId: input.chatId,
        threadId: input.threadId,
        text: input.text,
        attachments: preparedAttachments
      },
      null,
      2
    );

    workspaceFiles.push({
      relativePath: metadataPath,
      content: Buffer.from(`${metadataText}\n`, "utf8")
    });

    return {
      promptSection: formatTelegramPromptSection(input, preparedAttachments, metadataPath),
      workspaceFiles,
      imagePaths: preparedAttachments.filter((attachment) => attachment.attachedAsImage).map((attachment) => attachment.workspacePath)
    };
  }
}
