import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { deliverAttachmentRequests, formatAttachmentDeliveryIssues } from "./attachment-delivery";
import { CronManager } from "./cron-manager";
import { CONTEXT_CRONS_FILE_NAME, CONTEXT_CRONS_WORKSPACE_PATH, CRON_REQUESTS_FILE_NAME, CRON_REQUESTS_WORKSPACE_PATH } from "./cron-jobs";
import { ContextRecord, FactoryDb, type QueuedTurnStatus } from "./db";
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
import { postBrainImportOrQueue } from "./brain-outbox";
import { normalizeCodexModelOverride } from "./codex-runtime";
import type { FactoryConfig } from "./config";
import type { TelegramBot, TelegramTarget } from "./telegram";

export type DispatchMode = "run" | "resume" | "loop";
export type PromptProfile = "full" | "light";
const MAX_QUEUED_CONTEXTS = 64;

export interface DispatchResponse {
  accepted: boolean;
  message: string;
}

export interface DispatchOptions {
  notifyAccepted?: boolean;
  notifyCompaction?: boolean;
  allowQueue?: boolean;
  rawPrompt?: boolean;
  promptProfile?: PromptProfile;
  telegramInput?: TelegramInboundMessageInput | null;
  modelOverride?: string | null;
  reasoningEffortOverride?: ContextRecord["reasoningEffortOverride"];
  sourceLabel?: string | null;
  queuedTurnId?: string | null;
  userId?: number | null;
  /**
   * Completion hook for callers that need run outcomes (e.g. curator status
   * reporting). Not serializable: queued-turn replays drop it.
   */
  onFinished?: (status: Exclude<QueuedTurnStatus, "queued" | "running" | "abandoned">) => Promise<void> | void;
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

function acceptedMessage(mode: DispatchMode, context: ContextRecord, logPath: string, notifyAccepted: boolean | undefined): string {
  if (notifyAccepted === false || (mode === "resume" && notifyAccepted !== true)) {
    return "";
  }

  return [`Dispatched ${mode} for ${context.slug}.`, `Machine: ${context.machine}`, `Log: ${logPath}`].join("\n");
}

function parseJsonObject(value: string): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(value) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

function collectErrorMessages(value: unknown, messages: string[]): void {
  if (!value || typeof value !== "object") {
    return;
  }

  const record = value as Record<string, unknown>;
  const directMessage = record.message;
  if (typeof directMessage === "string" && directMessage.trim()) {
    const nested = parseJsonObject(directMessage.trim());
    if (nested) {
      collectErrorMessages(nested, messages);
    } else {
      messages.push(directMessage.trim());
    }
  }

  collectErrorMessages(record.error, messages);
}

function codexErrorMessage(errorText: string): string | null {
  const messages: string[] = [];
  for (const line of errorText.split(/\r?\n/)) {
    const parsed = parseJsonObject(line.trim());
    if (parsed) {
      collectErrorMessages(parsed, messages);
    }
  }

  const unique = [...new Set(messages.map((message) => message.trim()).filter(Boolean))];
  return unique[0] || null;
}

export function formatRunFailureForTelegram(errorText: string | null, logPath: string, manualCompaction: boolean): string {
  const message = errorText?.trim() || "unknown error";
  const parsedCodexMessage = codexErrorMessage(message);
  if (manualCompaction || /(?:codex_core::compact_remote|remote compaction failed|compact_error=|compaction failed)/i.test(message)) {
    return ["Codex compaction failed.", `Log: ${logPath}`, `Error: ${snippet(parsedCodexMessage || message)}`].join("\n");
  }

  if (parsedCodexMessage) {
    return ["Codex failed.", `Error: ${snippet(parsedCodexMessage)}`, `Log: ${logPath}`].join("\n");
  }

  return message;
}

function buildLightPrompt(context: ContextRecord, mode: DispatchMode, instruction: string, telegramPromptSection: string | null = null): string {
  const sections = [
    `You are replying inside the Brainstack Telegram control plane for ${context.slug}.`,
    `Machine: ${context.machine}.`,
    `Kind: ${context.kind}.`,
    `Transport: ${context.transport || "n/a"}.`,
    `Target: ${context.target}.`,
    `Root: ${context.rootPath}.`,
    `Worktree: ${context.worktreePath}.`,
    "Treat this as a lightweight informational turn.",
    "You may perform read-only inspection when needed to answer, such as git status, git log, git diff --stat, and focused file reads.",
    "Do not write files, install packages, run tests, make network calls, change system state, perform deployments, or update .factory files unless the user's message explicitly asks for work that requires those actions.",
    "If the message actually needs edits, tests, scheduling, attachment delivery, deployment, or machine-operation work, say that it needs a full work turn instead of attempting a partial edit.",
    `Do not create ${TELEGRAM_ATTACHMENTS_WORKSPACE_PATH} or ${CRON_REQUESTS_WORKSPACE_PATH} unless the user explicitly asked for a Telegram attachment or scheduled-job change.`,
    `Control-plane mode: ${mode}.`,
    "Instruction:",
    instruction.trim()
  ];

  if (telegramPromptSection) {
    sections.push(telegramPromptSection);
  }

  return sections.join("\n\n");
}

function buildPrompt(
  context: ContextRecord,
  mode: DispatchMode,
  instruction: string,
  telegramPromptSection: string | null = null,
  profile: PromptProfile = "full"
): string {
  if (profile === "light") {
    return buildLightPrompt(context, mode, instruction, telegramPromptSection);
  }

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
    "Do not dump large or private state files such as browser profiles, keyrings, histories, token files, or full JSON preference stores into logs or chat. Use narrow structured queries, paths, counts, and redacted summaries instead.",
    `These messages are coming through Telegram. If the user explicitly asks you to send or attach a file into the Telegram thread, keep your normal answer and also write ${TELEGRAM_ATTACHMENTS_WORKSPACE_PATH} as JSON like {"attachments":[{"path":"relative/path/inside/workspace","caption":"optional short caption","type":"document"}]}.`,
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

interface QueuedTurn {
  mode: DispatchMode;
  contextSlug: string;
  instruction: string;
  replyTarget: TelegramTarget;
  options: DispatchOptions;
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

  private queueTurn(turn: QueuedTurn): boolean {
    const existingCount = this.db.countQueuedTurnsForContext(turn.contextSlug);
    if (!existingCount && this.db.countQueuedContexts() >= MAX_QUEUED_CONTEXTS) {
      return false;
    }
    if (existingCount >= 5) {
      return false;
    }
    this.db.enqueueQueuedTurn({
      contextSlug: turn.contextSlug,
      mode: turn.mode,
      instruction: turn.instruction,
      chatId: turn.replyTarget.chatId,
      threadId: turn.replyTarget.threadId,
      userId: turn.options.userId ?? null,
      optionsJson: JSON.stringify(turn.options)
    });
    return true;
  }

  private startNextQueuedTurn(slug: string): void {
    try {
      const next = this.db.claimNextQueuedTurn(slug);
      if (!next) {
        return;
      }

      const context = this.db.getContextBySlug(next.contextSlug);
      const replyTarget: TelegramTarget = { chatId: next.chatId, threadId: next.threadId };
      if (!context || context.state === "archived") {
        this.db.finishQueuedTurn(next.id, "skipped", "context is no longer active");
        void this.telegram.sendText(replyTarget, `${next.contextSlug} queued turn was skipped because the context is no longer active.`);
        this.startNextQueuedTurn(slug);
        return;
      }
      if (context.telegramChatId !== next.chatId || context.telegramThreadId !== next.threadId) {
        this.db.finishQueuedTurn(next.id, "skipped", "context Telegram binding changed before queued turn could run");
        void this.telegram.sendText(replyTarget, `${next.contextSlug} queued turn was skipped because the topic binding changed.`);
        this.startNextQueuedTurn(slug);
        return;
      }

      const options = (parseJsonObject(next.optionsJson) || {}) as DispatchOptions;
      void this.dispatch(next.mode as DispatchMode, context, next.instruction, replyTarget, {
        ...options,
        notifyAccepted: false,
        queuedTurnId: next.id,
        userId: options.userId ?? next.userId ?? null
      })
        .then((response) => {
          if (!response.accepted) {
            this.db.finishQueuedTurn(next.id, "failed", response.message);
          }
          if (!response.accepted && response.message) {
            return this.telegram.sendText(replyTarget, response.message);
          }
        })
        .catch((error) => {
          console.error(`queued turn failed to start for ${next.contextSlug}`, error);
          this.db.finishQueuedTurn(next.id, "failed", error instanceof Error ? error.message : String(error));
          void this.telegram.sendText(
            replyTarget,
            `${next.contextSlug} queued turn failed to start: ${error instanceof Error ? error.message : String(error)}`
          );
        });
    } catch (error) {
      console.error(`queued turn failed to start for ${slug}`, error);
    }
  }

  recoverQueuedTurns(): void {
    // Reconcile durable active-run records before accepting new work: a restart can
    // leave a setsid-detached harness still mutating the workspace, so the operator
    // must hear about it instead of telemux silently accepting another run.
    for (const run of this.db.takeStaleActiveRuns()) {
      console.warn(`active run interrupted by restart: ${run.contextSlug} (${run.mode}, started ${run.startedAt})`);
      const context = this.db.getContextBySlug(run.contextSlug);
      if (context && context.state !== "archived") {
        this.contexts.saveContext({
          ...context,
          lastError: `telemux restarted during an active ${run.mode} run started at ${run.startedAt}; the harness process may still be running. Log: ${run.logPath || "n/a"}`
        });
      }
      if (run.chatId !== null) {
        void this.telegram.sendText(
          { chatId: run.chatId, threadId: run.threadId },
          [
            `${run.contextSlug} had an active ${run.mode} run interrupted by a telemux restart.`,
            `The harness process may still be running detached. Check the log before dispatching new work: ${run.logPath || "n/a"}`
          ].join("\n")
        );
      }
    }

    const abandoned = this.db.markRunningQueuedTurnsAbandoned();
    if (abandoned.length > 0) {
      console.warn(`queued turns abandoned after restart: ${abandoned.length}`);
      for (const turn of abandoned) {
        void this.telegram.sendText(
          { chatId: turn.chatId, threadId: turn.threadId },
          `${turn.contextSlug} had a queued turn interrupted by a telemux restart. It was marked for operator review instead of replayed automatically.`
        );
      }
    }
    for (const slug of this.db.queuedContextSlugs()) {
      if (!this.activeJobs.has(slug)) {
        this.startNextQueuedTurn(slug);
      }
    }
  }

  async withContextLock<T>(
    context: ContextRecord,
    run: () => Promise<T>,
    meta?: { mode: string; logPath: string | null; target?: TelegramTarget | null }
  ): Promise<T> {
    if (this.activeJobs.has(context.slug)) {
      throw new Error(`${context.slug} already has an active job. Use /topicinfo or /tail.`);
    }

    let releaseLock: () => void = () => undefined;
    const lock = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });
    this.activeJobs.set(context.slug, lock);
    if (meta) {
      // Locked runs that mutate workspaces deserve the same durable restart
      // reconciliation as dispatched harness jobs.
      this.db.recordActiveRun({
        contextSlug: context.slug,
        mode: meta.mode,
        logPath: meta.logPath,
        chatId: meta.target?.chatId ?? null,
        threadId: meta.target?.threadId ?? null
      });
    }

    try {
      return await run();
    } finally {
      if (meta) {
        this.db.clearActiveRun(context.slug);
      }
      if (this.activeJobs.get(context.slug) === lock) {
        this.activeJobs.delete(context.slug);
      }
      releaseLock();
      this.startNextQueuedTurn(context.slug);
    }
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
      if (options.allowQueue === false) {
        return {
          accepted: false,
          message: `${context.slug} already has an active job. Use /topicinfo or /tail.`
        };
      }
      const queued = this.queueTurn({
        mode,
        contextSlug: context.slug,
        instruction: trimmedInstruction,
        replyTarget,
        options
      });
      return {
        accepted: queued,
        message: queued
          ? `${context.slug} is busy; queued this turn for after the current run.`
          : `${context.slug} already has an active job and its turn queue is full. Use /topicinfo or /tail.`
      };
    }

    let releaseLock: () => void = () => undefined;
    const lock = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });
    this.activeJobs.set(context.slug, lock);

    let savedContext: ContextRecord;
    let logPath: string;
    try {
      await mkdir(this.config.logsDir, { recursive: true });
      logPath = resolve(this.config.logsDir, `${nowStamp()}-${context.slug}-${mode}.log`);

      const currentBeforeAccept = this.db.getContextBySlug(context.slug) || context;
      if (currentBeforeAccept.state === "archived") {
        if (this.activeJobs.get(context.slug) === lock) {
          this.activeJobs.delete(context.slug);
        }
        releaseLock();
        return {
          accepted: false,
          message: `${context.slug} is archived. Rebind the topic or create a new context first.`
        };
      }

      savedContext = this.contexts.saveContext({
        ...currentBeforeAccept,
        latestRunLogPath: logPath,
        lastRunAt: new Date().toISOString(),
        lastError: null
      });
      this.db.recordActiveRun({
        contextSlug: savedContext.slug,
        mode,
        logPath,
        chatId: replyTarget.chatId,
        threadId: replyTarget.threadId
      });
    } catch (error) {
      this.db.clearActiveRun(context.slug);
      if (this.activeJobs.get(context.slug) === lock) {
        this.activeJobs.delete(context.slug);
      }
      releaseLock();
      throw error;
    }

    const job = this.runJob(mode, savedContext, trimmedInstruction, replyTarget, logPath, options.telegramInput || null, options);
    if (options.onFinished) {
      const hook = options.onFinished;
      void job
        .then(
          (status) => hook(status),
          () => hook("failed")
        )
        .catch((error) => console.error("dispatch onFinished hook failed", error));
    }
    if (options.queuedTurnId) {
      void job
        .then((status) => {
          this.db.finishQueuedTurn(options.queuedTurnId!, status, status === "finished" ? null : "queued worker run did not complete successfully");
        })
        .catch((error) => {
          this.db.finishQueuedTurn(options.queuedTurnId!, "failed", error instanceof Error ? error.message : String(error));
        });
    }
    void job.finally(() => {
      this.db.clearActiveRun(savedContext.slug);
      if (this.activeJobs.get(savedContext.slug) === lock) {
        this.activeJobs.delete(savedContext.slug);
      }
      releaseLock();
      this.startNextQueuedTurn(savedContext.slug);
    });

    return {
      accepted: true,
      message: acceptedMessage(mode, savedContext, logPath, options.notifyAccepted)
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
  ): Promise<Exclude<QueuedTurnStatus, "queued" | "running" | "abandoned">> {
    const stopHeartbeat = this.startTypingHeartbeat(replyTarget);

    try {
      const ensured = await this.workers.ensureContext(context);
      const freshContext = this.db.getContextBySlug(context.slug) || context;
      if (freshContext.state === "archived") {
        await this.telegram.sendText(replyTarget, `${freshContext.slug} was archived before the run started. No worker side effects were applied.`);
        return "skipped";
      }

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
        return "failed";
      }

      const currentBeforeReady = this.db.getContextBySlug(context.slug) || freshContext;
      if (currentBeforeReady.state === "archived") {
        await this.telegram.sendText(replyTarget, `${currentBeforeReady.slug} was archived before the run started. No worker side effects were applied.`);
        return "skipped";
      }

      const readyContext = this.contexts.saveContext({
        ...currentBeforeReady,
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
          const currentAfterPreparationFailure = this.db.getContextBySlug(context.slug) || readyContext;
          if (currentAfterPreparationFailure.state === "archived") {
            await this.telegram.sendText(
              replyTarget,
              `${currentAfterPreparationFailure.slug} was archived while Telegram input was being prepared. No worker side effects were applied.`
            );
            return "skipped";
          }
          this.contexts.saveContext({
            ...currentAfterPreparationFailure,
            latestRunLogPath: logPath,
            lastRunAt: new Date().toISOString(),
            lastError: message
          });
          await this.telegram.sendText(replyTarget, `Failed to prepare Telegram input: ${message}`);
          return "failed";
        }
      }

      const currentBeforeWorker = this.db.getContextBySlug(context.slug) || readyContext;
      if (currentBeforeWorker.state === "archived") {
        await this.telegram.sendText(replyTarget, `${currentBeforeWorker.slug} was archived before the worker started. No worker side effects were applied.`);
        return "skipped";
      }

      const promptProfile: PromptProfile = preparedTelegramInput ? "full" : options.promptProfile || "full";
      const prompt = options.rawPrompt
        ? instruction
        : buildPrompt(currentBeforeWorker, mode, instruction, preparedTelegramInput?.promptSection || null, promptProfile);
      let compactionNotified = false;
      const rawModelOverride = options.modelOverride ?? currentBeforeWorker.modelOverride;
      const result = await this.workers.runCodex(currentBeforeWorker, prompt, mode, logPath, {
        workspaceFiles: preparedTelegramInput?.workspaceFiles,
        imagePaths: preparedTelegramInput?.imagePaths,
        modelOverride: rawModelOverride ? normalizeCodexModelOverride(rawModelOverride) : null,
        reasoningEffortOverride: options.reasoningEffortOverride ?? currentBeforeWorker.reasoningEffortOverride,
        onCompaction: async () => {
          if (options.notifyCompaction === false || compactionNotified) {
            return;
          }
          compactionNotified = true;
          await this.telegram.sendText(replyTarget, "Compacting thread…");
        },
        onSessionId: async (sessionId) => {
          const current = this.db.getContextBySlug(context.slug) || currentBeforeWorker;
          if (current.state === "archived") {
            return;
          }
          this.contexts.saveContext({
            ...current,
            codexSessionId: sessionId,
            latestRunLogPath: logPath,
            lastRunAt: current.lastRunAt || new Date().toISOString()
          });
        }
      });
      const afterRunContext = this.db.getContextBySlug(context.slug) || currentBeforeWorker;

      if (result.ok) {
        if (afterRunContext.state === "archived") {
          await this.telegram.sendText(
            replyTarget,
            `${afterRunContext.slug} completed after it was archived. Completion side effects were skipped.`
          );
          return "skipped";
        }
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
        await this.importRunNotesToBrain(saved, summary, artifacts);
        return "finished";
      }

      if (afterRunContext.state === "archived") {
        await this.telegram.sendText(
          replyTarget,
          `${afterRunContext.slug} failed after it was archived. Context state was left archived.`
        );
        return "skipped";
      }

      const failureOutput = [result.stderr.trim(), result.stdout.trim()].filter(Boolean).join("\n");
      const saved = this.contexts.saveContext({
        ...afterRunContext,
        state: this.workers.isReachabilityFailure(result) ? "pending" : "error",
        latestRunLogPath: logPath,
        lastRunAt: new Date().toISOString(),
        lastError: failureOutput || `exit ${result.exitCode}`
      });

      await this.telegram.sendText(
        replyTarget,
        [
          `${saved.slug} failed on ${saved.machine}.`,
          `state=${saved.state} transport=${result.transport} exit=${result.exitCode}`,
          formatRunFailureForTelegram(saved.lastError, logPath, Boolean(options.rawPrompt && instruction.trim() === "/compact"))
        ].join("\n")
      );
      return "failed";
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

  private async importRunNotesToBrain(context: ContextRecord, summary: string | null, artifacts: string | null): Promise<void> {
    if (!this.config.brainBaseUrl || !this.config.brainImportToken) {
      return;
    }

    const body = [
      `# Telemux run notes: ${context.slug}`,
      "",
      `- context: ${context.slug}`,
      `- machine: ${context.machine}`,
      `- session: ${context.codexSessionId || "n/a"}`,
      `- run_at: ${context.lastRunAt || new Date().toISOString()}`,
      "",
      "## SUMMARY.md",
      "",
      summary?.trim() || "(empty)",
      "",
      "## ARTIFACTS.md",
      "",
      artifacts?.trim() || "(empty)",
      ""
    ].join("\n");

    const status = await postBrainImportOrQueue(this.config, {
      title: `telemux run notes: ${context.slug}`,
      text: body,
      source_harness: "telemux",
      source_machine: context.machine,
      source_type: "telemux-run",
      conversation_id: context.slug,
      tags: ["telemux", "factory-run"]
    });
    if (status === "sent") {
      console.log(`shared brain import succeeded for ${context.slug}`);
    } else if (status === "queued") {
      console.warn(`shared brain import queued for ${context.slug}`);
    }
  }

  private startTypingHeartbeat(replyTarget: TelegramTarget): () => void {
    let stopped = false;
    let lastLoggedAt = 0;
    let suppressed = 0;

    void (async () => {
      while (!stopped) {
        try {
          await this.telegram.sendChatAction(replyTarget, "typing");
        } catch (error) {
          const now = Date.now();
          if (now - lastLoggedAt > 60_000) {
            const suffix = suppressed > 0 ? `; suppressed ${suppressed} repeated heartbeat failure(s)` : "";
            console.error(`telegram typing heartbeat failed${suffix}`, error);
            lastLoggedAt = now;
            suppressed = 0;
          } else {
            suppressed += 1;
          }
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

      const bytes = await this.telegram.downloadFile(remoteFile.file_path, TELEGRAM_MAX_INBOUND_FILE_BYTES);
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
