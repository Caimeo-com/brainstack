import { deliverAttachmentRequests, formatAttachmentDeliveryIssues } from "./attachment-delivery";
import {
  CODEX_MODE_PRESETS,
  formatCodexRuntimeOverrides,
  normalizeCodexModelOverride,
  parseCodexModePreset,
  parseCodexReasoningEffort
} from "./codex-runtime";
import { CronManager } from "./cron-manager";
import { ContextRecord, FactoryDb } from "./db";
import { ContextService, nextRecommendedAction, normalizeSlug } from "./contexts";
import { Dispatcher } from "./dispatcher";
import { CronJobRecord } from "./cron-jobs";
import { selectArtifactEntries, TelegramAttachmentRequest } from "./telegram-attachments";
import { extractTelegramInput, filterPhaseOneTelegramInput, isAudioOnlyTelegramInput, telegramMessageText } from "./telegram-inputs";
import { summarizeUsage } from "./usage";
import { WorkerService } from "./workers";
import type { FactoryConfig } from "./config";
import type { TelegramBot, TelegramBotCommandScope, TelegramCommandSyncResult, TelegramMessage, TelegramTarget } from "./telegram";

function parseCommand(text: string): { command: string; rest: string } | null {
  const trimmed = text.trim();
  if (!trimmed.startsWith("/")) {
    return null;
  }

  const firstSpace = trimmed.indexOf(" ");
  const head = firstSpace === -1 ? trimmed : trimmed.slice(0, firstSpace);
  const rest = firstSpace === -1 ? "" : trimmed.slice(firstSpace + 1).trim();
  return {
    command: head.replace(/@[^@\s]+$/, "").toLowerCase(),
    rest
  };
}

function compact(text: string | null, limit = 280): string {
  if (!text) {
    return "n/a";
  }

  const normalized = text.replace(/\s+/g, " ").trim();
  if (normalized.length <= limit) {
    return normalized;
  }

  return `${normalized.slice(0, limit - 1)}…`;
}

async function readTail(path: string, lines = 40): Promise<string> {
  const file = Bun.file(path);
  if (!(await file.exists())) {
    return `Missing log file: ${path}`;
  }

  const text = await file.text();
  const tail = text.split("\n").slice(-lines).join("\n").trim();
  return tail || "(log is empty)";
}

function messageTarget(message: TelegramMessage): TelegramTarget {
  return {
    chatId: message.chat.id,
    threadId: message.message_thread_id ?? null
  };
}

function commandScopeChatId(message: TelegramMessage): number | null {
  return message.chat.type === "group" || message.chat.type === "supergroup" ? message.chat.id : null;
}

function formatBoundContext(context: ContextRecord | null): string {
  if (!context) {
    return "none";
  }

  return `${context.slug} (${context.machine}/${context.kind}/${context.state})`;
}

function contextRoutingNote(): string[] {
  return [
    "Rebinding changes future routing for this Telegram topic.",
    "The old workspace stays on disk unless you archive it or delete it separately.",
    "Old Telegram messages stay in Telegram and are not automatically imported into a newly bound context."
  ];
}

function formatCommandScopeLabel(scope: TelegramBotCommandScope): string {
  if (scope.type === "chat_member") {
    return `chat_member chat_id=${scope.chat_id} user_id=${scope.user_id}`;
  }

  return scope.type;
}

function formatRegisteredCommands(commands: Array<{ command: string; description: string }>): string {
  if (!commands.length) {
    return "(none)";
  }

  return commands.map((command) => `/${command.command} - ${command.description}`).join("\n");
}

function audioNotSupportedText(): string {
  return "Audio and voice Telegram messages are not forwarded to Codex yet. Phase 2 will transcribe them first.";
}

function codexModeName(context: ContextRecord): string {
  const preset = Object.values(CODEX_MODE_PRESETS).find(
    (candidate) =>
      candidate.modelOverride === context.modelOverride &&
      candidate.reasoningEffortOverride === context.reasoningEffortOverride
  );

  if (preset) {
    return preset.name;
  }

  if (!context.modelOverride && !context.reasoningEffortOverride) {
    return "default";
  }

  return "custom";
}

function codexModeSummary(context: ContextRecord): string {
  return `${codexModeName(context)} (${formatCodexRuntimeOverrides({
    modelOverride: context.modelOverride,
    reasoningEffortOverride: context.reasoningEffortOverride
  })})`;
}

export class CommandHandler {
  constructor(
    private readonly config: FactoryConfig,
    private readonly db: FactoryDb,
    private readonly telegram: TelegramBot,
    private readonly contexts: ContextService,
    private readonly workers: WorkerService,
    private readonly dispatcher: Dispatcher,
    private readonly cronManager: CronManager
  ) {}

  async handleMessage(message: TelegramMessage): Promise<void> {
    const text = telegramMessageText(message);
    const rawTelegramInput = extractTelegramInput(message);
    const telegramInput = rawTelegramInput ? filterPhaseOneTelegramInput(rawTelegramInput) : null;

    if (!text && !rawTelegramInput?.attachments.length) {
      return;
    }

    const target = messageTarget(message);
    const parsed = parseCommand(text);
    const boundContext = this.contexts.getContextByTopic(target.chatId, target.threadId);
    const allowed = message.from?.id === this.config.allowedTelegramUserId;

    if (parsed?.command === "/whoami") {
      await this.telegram.sendText(
        target,
        [
          `Access: ${allowed ? "allowed" : "denied"}`,
          `Allowed user id: ${this.config.allowedTelegramUserId}`,
          `From user id: ${message.from?.id ?? "unknown"}`,
          `Chat id: ${message.chat.id}`,
          `Thread id: ${message.message_thread_id ?? "none"}`,
          `Chat type: ${message.chat.type}`,
          `Bound context: ${formatBoundContext(boundContext)}`
        ].join("\n")
      );
      return;
    }

    if (!allowed) {
      console.warn("ignoring telegram message from unauthorized user", message.from?.id);
      return;
    }

    try {
      if (!parsed) {
        if (!boundContext) {
          await this.telegram.sendText(target, "This topic is not bound. Use /newctx or /bind.");
          return;
        }

        if (boundContext.state === "archived") {
          await this.telegram.sendText(target, `${boundContext.slug} is archived. Use /bind or /newctx first.`);
          return;
        }

        if (rawTelegramInput && isAudioOnlyTelegramInput(rawTelegramInput)) {
          await this.telegram.sendText(target, audioNotSupportedText());
          return;
        }

        const response = await this.dispatcher.dispatch("resume", boundContext, text, target, {
          notifyAccepted: false,
          telegramInput
        });
        if (response.message) {
          await this.telegram.sendText(target, response.message);
        }
        return;
      }

      switch (parsed.command) {
        case "/help":
          await this.telegram.sendText(
            target,
            [
              "A context is the durable Codex workspace and session binding for one Telegram topic.",
              "/newctx creates that binding for a topic and prepares the target workspace.",
              "/newctx is usually run once per reusable Telegram topic, then you keep using plain text, /run, or /resume in that topic.",
              "/bind is for repointing the current topic at a different target or attaching it to an existing stored context.",
              "/archive marks the current context inactive and detaches the topic.",
              "/detach only removes the topic binding; the workspace stays on disk.",
              "Old Telegram messages remain in Telegram and are not automatically imported into a newly bound context.",
              "",
              "Commands:",
              "/help",
              "/explainctx",
              "/synccommands",
              "/showcommands",
              "/whoami",
              "/workers",
              "/crons",
              "/cron <subcommand>",
              "/mode [fast|normal|max|clear]",
              "/model [model-id|clear]",
              "/effort [low|medium|high|xhigh|clear]",
              "/newctx <slug> <machine> <target> [base-branch]",
              "/bind <machine> <target> [base-branch]",
              "/topicinfo",
              "/run <instruction>",
              "/resume [instruction]",
              "/loop <instruction>",
              "/archive",
              "/detach",
              "/tail",
              "/artifacts",
              "/usage",
              "",
              "In a bound topic, plain text starts or resumes the stored Codex session.",
              "Use /mode, /model, and /effort to change Codex runtime behavior for this topic without rebinding it.",
              "Use /crons to inspect scheduled jobs linked to this topic/context. Use /cron with a job id from /crons to pause, resume, move, retarget context, or tune job runtime."
            ].join("\n")
          );
          return;

        case "/explainctx": {
          if (!boundContext) {
            await this.telegram.sendText(
              target,
              [
                "This topic is not bound yet.",
                "Use /newctx <slug> <machine> <target> [base-branch] to create a durable context for it."
              ].join("\n")
            );
            return;
          }

          await this.telegram.sendText(target, this.formatContextExplanation(boundContext));
          return;
        }

        case "/synccommands": {
          const results = await this.telegram.syncCommands({
            currentChatId: commandScopeChatId(message)
          });
          await this.telegram.sendText(target, this.formatCommandSyncResults(results));
          return;
        }

        case "/showcommands": {
          const scopes = this.telegram.listCommandScopes(commandScopeChatId(message));
          const sections: string[] = [];

          for (const scope of scopes) {
            try {
              const commands = await this.telegram.getCommands(scope);
              sections.push([formatCommandScopeLabel(scope), formatRegisteredCommands(commands)].join(":\n"));
            } catch (error) {
              sections.push(
                [formatCommandScopeLabel(scope), `error: ${error instanceof Error ? error.message : String(error)}`].join(
                  ":\n"
                )
              );
            }
          }

          await this.telegram.sendText(target, sections.join("\n\n"));
          return;
        }

        case "/workers": {
          const workers = await this.workers.refreshWorkers();
          await this.telegram.sendText(
            target,
            workers.length
              ? workers
                  .map((worker) =>
                    [
                      worker.host,
                      `status=${worker.status}`,
                      `transport=${worker.transport || "n/a"}`,
                      `local=${worker.localExecution ? "yes" : "no"}`,
                      worker.lastSeenAt ? `last_seen=${worker.lastSeenAt}` : null,
                      worker.lastCheckedAt ? `checked=${worker.lastCheckedAt}` : null,
                      worker.lastError ? `error=${compact(worker.lastError, 140)}` : null
                    ]
                      .filter(Boolean)
                      .join(" | ")
                  )
                  .join("\n")
              : "No workers configured."
          );
          return;
        }

        case "/crons": {
          await this.telegram.sendText(target, this.cronManager.formatJobsOverview(boundContext, target));
          return;
        }

        case "/cron": {
          const parts = parsed.rest.split(/\s+/).filter(Boolean);
          if (parts.length < 2) {
            await this.telegram.sendText(
              target,
              [
                "Usage:",
                "/cron show <id>",
                "/cron pause <id>",
                "/cron resume <id>",
                "/cron delete <id>",
                "/cron move <id> here",
                "/cron context <id> <slug-or-path>",
                "/cron mode <id> [fast|normal|max|clear]",
                "/cron model <id> [model-id|clear]",
                "/cron effort <id> [low|medium|high|xhigh|clear]"
              ].join("\n")
            );
            return;
          }

          const [subcommand, selectorText, ...restParts] = parts;
          const job = this.cronManager.resolveJob({ id: selectorText, label: selectorText }, { context: boundContext, target });
          const restText = restParts.join(" ").trim();

          switch (subcommand.toLowerCase()) {
            case "show": {
              const effectiveContext = job.executionContextSlug ? this.db.getContextBySlug(job.executionContextSlug) : null;
              const runtimeModel = job.modelOverride || effectiveContext?.modelOverride || "default";
              const runtimeEffort = job.reasoningEffortOverride || effectiveContext?.reasoningEffortOverride || "default";
              await this.telegram.sendText(
                target,
                [
                  `Cron: ${job.id}`,
                  `Label: ${job.label}`,
                  `Kind: ${job.kind}`,
                  `Enabled: ${job.enabled ? "yes" : "no"}`,
                  `Schedule: ${this.formatCronSchedule(job)}`,
                  `Next run: ${job.nextRunAt || "none"}`,
                  `Pending run: ${job.pendingRunAt || "none"}`,
                  `Context: ${job.executionContextSlug || "none"}`,
                  `Target: ${job.targetChatId}:${job.targetThreadId ?? "none"}`,
                  `Mode: model=${runtimeModel} effort=${runtimeEffort}`,
                  job.reminderText ? `Reminder: ${job.reminderText}` : null,
                  job.instruction ? `Instruction: ${job.instruction}` : null,
                  job.lastError ? `Last error: ${compact(job.lastError)}` : null
                ]
                  .filter(Boolean)
                  .join("\n")
              );
              return;
            }

            case "pause": {
              const updated = await this.cronManager.pauseJob(job);
              await this.telegram.sendText(target, `Cron paused: ${updated.id} (${updated.label})`);
              return;
            }

            case "resume": {
              const updated = await this.cronManager.resumeJob(job);
              await this.telegram.sendText(target, `Cron resumed: ${updated.id} (${updated.label}) next=${updated.nextRunAt || "none"}`);
              return;
            }

            case "delete": {
              await this.cronManager.deleteJob(job);
              await this.telegram.sendText(target, `Cron deleted: ${job.id} (${job.label})`);
              return;
            }

            case "move": {
              if (restText.toLowerCase() !== "here") {
                await this.telegram.sendText(target, "Usage: /cron move <id> here");
                return;
              }

              const updated = await this.cronManager.updateJob(job, {
                targetChatId: target.chatId,
                targetThreadId: target.threadId
              });
              await this.telegram.sendText(target, `Cron moved: ${updated.id} now targets ${updated.targetChatId}:${updated.targetThreadId ?? "none"}`);
              return;
            }

            case "context": {
              if (!restText) {
                await this.telegram.sendText(target, "Usage: /cron context <id> <slug-or-path>");
                return;
              }

              const updated = await this.cronManager.updateJob(job, {
                executionContextSlug: this.cronManager.requireContextReference(restText).slug
              });
              await this.telegram.sendText(target, `Cron context updated: ${updated.id} -> ${updated.executionContextSlug || "none"}`);
              return;
            }

            case "mode": {
              if (!restText) {
                await this.telegram.sendText(target, "Usage: /cron mode <id> [fast|normal|max|clear]");
                return;
              }

              const normalized = restText.toLowerCase();
              const updated =
                normalized === "clear" || normalized === "default" || normalized === "reset"
                  ? await this.cronManager.updateJob(job, {
                      modelOverride: null,
                      reasoningEffortOverride: null
                    })
                  : await this.cronManager.updateJob(job, (() => {
                      const preset = parseCodexModePreset(restText);
                      if (!preset) {
                        throw new Error("Usage: /cron mode <id> [fast|normal|max|clear]");
                      }

                      return {
                        modelOverride: preset.modelOverride,
                        reasoningEffortOverride: preset.reasoningEffortOverride
                      };
                    })());

              await this.telegram.sendText(target, `Cron mode updated: ${updated.id} (${updated.label})`);
              return;
            }

            case "model": {
              if (!restText) {
                await this.telegram.sendText(target, "Usage: /cron model <id> [model-id|clear]");
                return;
              }

              const normalized = restText.toLowerCase();
              const updated = await this.cronManager.updateJob(job, {
                modelOverride:
                  normalized === "clear" || normalized === "default" || normalized === "reset"
                    ? null
                    : normalizeCodexModelOverride(restText)
              });
              await this.telegram.sendText(target, `Cron model updated: ${updated.id} (${updated.label})`);
              return;
            }

            case "effort": {
              if (!restText) {
                await this.telegram.sendText(target, "Usage: /cron effort <id> [low|medium|high|xhigh|clear]");
                return;
              }

              const normalized = restText.toLowerCase();
              const nextEffort =
                normalized === "clear" || normalized === "default" || normalized === "reset"
                  ? null
                  : parseCodexReasoningEffort(restText);

              if (normalized !== "clear" && normalized !== "default" && normalized !== "reset" && !nextEffort) {
                await this.telegram.sendText(target, "Usage: /cron effort <id> [low|medium|high|xhigh|clear]");
                return;
              }

              const updated = await this.cronManager.updateJob(job, {
                reasoningEffortOverride: nextEffort
              });
              await this.telegram.sendText(target, `Cron effort updated: ${updated.id} (${updated.label})`);
              return;
            }

            default:
              await this.telegram.sendText(target, `Unknown /cron subcommand: ${subcommand}`);
              return;
          }
        }

        case "/mode": {
          if (!boundContext) {
            await this.telegram.sendText(target, "This topic is not bound.");
            return;
          }

          if (!parsed.rest) {
            await this.telegram.sendText(
              target,
              [
                `Codex mode: ${codexModeSummary(boundContext)}`,
                "Presets:",
                ...Object.values(CODEX_MODE_PRESETS).map(
                  (preset) => `${preset.name} -> ${formatCodexRuntimeOverrides(preset)}`
                )
              ].join("\n")
            );
            return;
          }

          const normalized = parsed.rest.trim().toLowerCase();
          if (normalized === "clear" || normalized === "default" || normalized === "reset") {
            const updated = this.contexts.saveContext({
              ...boundContext,
              modelOverride: null,
              reasoningEffortOverride: null
            });
            await this.telegram.sendText(target, `Codex mode reset to ${codexModeSummary(updated)}.`);
            return;
          }

          const preset = parseCodexModePreset(parsed.rest);
          if (!preset) {
            await this.telegram.sendText(target, "Usage: /mode [fast|normal|max|clear]");
            return;
          }

          const updated = this.contexts.saveContext({
            ...boundContext,
            modelOverride: preset.modelOverride,
            reasoningEffortOverride: preset.reasoningEffortOverride
          });
          await this.telegram.sendText(target, `Codex mode set to ${codexModeSummary(updated)}.`);
          return;
        }

        case "/model": {
          if (!boundContext) {
            await this.telegram.sendText(target, "This topic is not bound.");
            return;
          }

          if (!parsed.rest) {
            await this.telegram.sendText(target, `Codex model override: ${boundContext.modelOverride || "default"}`);
            return;
          }

          const normalized = parsed.rest.trim().toLowerCase();
          const updated = this.contexts.saveContext({
            ...boundContext,
            modelOverride:
              normalized === "clear" || normalized === "default" || normalized === "reset"
                ? null
                : normalizeCodexModelOverride(parsed.rest)
          });
          await this.telegram.sendText(target, `Codex mode now ${codexModeSummary(updated)}.`);
          return;
        }

        case "/effort": {
          if (!boundContext) {
            await this.telegram.sendText(target, "This topic is not bound.");
            return;
          }

          if (!parsed.rest) {
            await this.telegram.sendText(
              target,
              `Codex reasoning effort override: ${boundContext.reasoningEffortOverride || "default"}`
            );
            return;
          }

          const normalized = parsed.rest.trim().toLowerCase();
          let nextEffort = null;

          if (!(normalized === "clear" || normalized === "default" || normalized === "reset")) {
            nextEffort = parseCodexReasoningEffort(parsed.rest);
            if (!nextEffort) {
              await this.telegram.sendText(target, "Usage: /effort [low|medium|high|xhigh|clear]");
              return;
            }
          }

          const updated = this.contexts.saveContext({
            ...boundContext,
            reasoningEffortOverride: nextEffort
          });
          await this.telegram.sendText(target, `Codex mode now ${codexModeSummary(updated)}.`);
          return;
        }

        case "/newctx": {
          const parts = parsed.rest.split(/\s+/).filter(Boolean);
          if (parts.length < 3) {
            await this.telegram.sendText(target, "Usage: /newctx <slug> <machine> <target> [base-branch]");
            return;
          }

          const [slugInput, machine, contextTarget, baseBranch] = parts;
          const bound = await this.createOrRebindContext(slugInput, machine, contextTarget, baseBranch || null, target);
          const warning = boundContext ? this.formatRebindWarning(boundContext) : null;
          await this.telegram.sendText(
            target,
            [warning, this.formatContextCreated(bound)]
              .filter(Boolean)
              .join("\n\n")
          );
          return;
        }

        case "/bind": {
          const parts = parsed.rest.split(/\s+/).filter(Boolean);
          if (!parts.length) {
            await this.telegram.sendText(target, "Usage: /bind <machine> <target> [base-branch]");
            return;
          }

          if (parts.length === 1) {
            const slug = normalizeSlug(parts[0]);
            const existing = this.contexts.getContextBySlug(slug);
            if (!existing) {
              await this.telegram.sendText(target, `Unknown context: ${slug}`);
              return;
            }

            const rebound = this.contexts.bindContext(slug, target.chatId, target.threadId);
            await this.telegram.sendText(
              target,
              rebound ? `Bound this topic to ${formatBoundContext(rebound)}.` : `Failed to bind ${slug}.`
            );
            return;
          }

          if (!boundContext) {
            await this.telegram.sendText(
              target,
              "This topic is not bound yet. Use /newctx <slug> <machine> <target> [base-branch]."
            );
            return;
          }

          const [machine, contextTarget, baseBranch] = parts;
          const rebound = await this.createOrRebindContext(
            boundContext.slug,
            machine,
            contextTarget,
            baseBranch || null,
            target
          );
          await this.telegram.sendText(target, this.formatContextCreated(rebound));
          return;
        }

        case "/topicinfo":
        case "/status": {
          if (!boundContext) {
            const contexts = this.db.listContexts();
            await this.telegram.sendText(
              target,
              [
                `Contexts: ${contexts.length}`,
                `Workers known: ${this.workers.knownHosts().join(", ") || "none"}`,
                "Bind this topic with /newctx or /bind."
              ].join("\n")
            );
            return;
          }

          await this.telegram.sendText(target, this.formatContextStatus(boundContext));
          return;
        }

        case "/run":
        case "/resume":
        case "/loop": {
          if (!boundContext) {
            await this.telegram.sendText(target, "This topic is not bound. Use /newctx or /bind.");
            return;
          }

          if (boundContext.state === "archived") {
            await this.telegram.sendText(target, `${boundContext.slug} is archived. Use /bind or /newctx first.`);
            return;
          }

          const mode = parsed.command.slice(1) as "run" | "resume" | "loop";
          if (rawTelegramInput && isAudioOnlyTelegramInput(rawTelegramInput)) {
            await this.telegram.sendText(target, audioNotSupportedText());
            return;
          }

          const response = await this.dispatcher.dispatch(mode, boundContext, parsed.rest, target, {
            telegramInput
          });
          if (response.message) {
            await this.telegram.sendText(target, response.message);
          }
          return;
        }

        case "/archive": {
          if (!boundContext) {
            await this.telegram.sendText(target, "This topic is not bound.");
            return;
          }

          this.contexts.saveContext({
            ...boundContext,
            state: "archived"
          });
          this.contexts.detachTopic(target.chatId, target.threadId);
          await this.telegram.sendText(target, `${boundContext.slug} archived and detached from this topic.`);
          return;
        }

        case "/detach": {
          if (!boundContext) {
            await this.telegram.sendText(target, "This topic is not bound.");
            return;
          }

          this.contexts.detachTopic(target.chatId, target.threadId);
          await this.telegram.sendText(target, `${boundContext.slug} detached from this topic.`);
          return;
        }

        case "/tail": {
          if (!boundContext?.latestRunLogPath) {
            await this.telegram.sendText(target, "No log recorded for this context yet.");
            return;
          }

          const tail = await readTail(boundContext.latestRunLogPath, 40);
          await this.telegram.sendText(target, `Log tail for ${boundContext.slug}:\n\n${tail}`);
          return;
        }

        case "/artifacts": {
          if (!boundContext) {
            await this.telegram.sendText(target, "This topic is not bound.");
            return;
          }

          const artifacts = await this.workers.readFactoryFile(boundContext, "ARTIFACTS.md");
          if (!parsed.rest) {
            await this.telegram.sendText(
              target,
              artifacts ? artifacts : `No artifact file available. Cached snippet: ${boundContext.lastArtifacts || "n/a"}`
            );
            return;
          }

          const sendMatch = parsed.rest.match(/^send(?:\s+(.+))?$/i);
          if (!sendMatch) {
            await this.telegram.sendText(target, "Usage: /artifacts or /artifacts send [filter]");
            return;
          }

          const requests = this.attachmentRequestsForArtifacts(artifacts, sendMatch[1] || null);
          if (!requests.length) {
            await this.telegram.sendText(
              target,
              sendMatch[1]?.trim()
                ? `No artifact file paths matched: ${sendMatch[1].trim()}`
                : "No artifact file paths were found in .factory/ARTIFACTS.md."
            );
            return;
          }

          const delivery = await deliverAttachmentRequests(this.workers, this.telegram, boundContext, target, requests);
          const notes = formatAttachmentDeliveryIssues(delivery);

          if (!delivery.sent.length) {
            await this.telegram.sendText(target, notes || "No recorded artifact files could be uploaded.");
            return;
          }

          if (notes) {
            await this.telegram.sendText(target, notes);
          }

          return;
        }

        case "/usage": {
          if (!boundContext) {
            await this.telegram.sendText(target, "This topic is not bound.");
            return;
          }

          const usage = await summarizeUsage(boundContext);
          await this.telegram.sendText(target, usage.text);
          return;
        }

        default:
          await this.telegram.sendText(target, `Unknown command: ${parsed.command}`);
      }
    } catch (error) {
      const messageText = error instanceof Error ? error.message : String(error);
      await this.telegram.sendText(target, `Error: ${messageText}`);
    }
  }

  private async createOrRebindContext(
    slug: string,
    machine: string,
    contextTarget: string,
    baseBranch: string | null,
    target: TelegramTarget
  ): Promise<ContextRecord> {
    const bootstrap = await this.workers.bootstrapContext({
      slug: normalizeSlug(slug),
      machine,
      target: contextTarget,
      baseBranch
    });

    const context = this.contexts.createOrUpdateContext({
      slug,
      machine,
      kind: bootstrap.kind,
      state: bootstrap.state,
      transport: bootstrap.transport,
      target: contextTarget,
      rootPath: bootstrap.rootPath,
      worktreePath: bootstrap.worktreePath,
      branchName: bootstrap.branchName,
      baseBranch: bootstrap.baseBranch,
      usageAdapter: this.config.usageAdapter,
      chatId: null,
      threadId: null,
      lastError: bootstrap.ok ? null : (bootstrap.stderr.trim() || bootstrap.stdout.trim() || `exit ${bootstrap.exitCode}`)
    });

    return this.contexts.bindContext(context.slug, target.chatId, target.threadId) || context;
  }

  private formatContextCreated(context: ContextRecord): string {
    const lines = [
      `Context ${context.slug} bound to this topic.`,
      `Machine: ${context.machine}`,
      `Kind: ${context.kind}`,
      `State: ${context.state}`,
      `Transport: ${context.transport || "n/a"}`,
      `Target: ${context.target}`,
      `Root: ${context.rootPath}`,
      `Worktree: ${context.worktreePath}`,
      `Codex mode: ${codexModeSummary(context)}`
    ];

    if (context.branchName) {
      lines.push(`Branch: ${context.branchName}`);
    }

    if (context.lastError) {
      lines.push(`Error: ${compact(context.lastError)}`);
    }

    lines.push(`Next: ${nextRecommendedAction(context)}`);
    return lines.join("\n");
  }

  private formatContextStatus(context: ContextRecord): string {
    const cronCount = this.cronManager.listRelevantJobs(
      context,
      context.telegramChatId === null
        ? null
        : {
            chatId: context.telegramChatId,
            threadId: context.telegramThreadId
          }
    ).length;

    return [
      `Context: ${context.slug}`,
      `Machine: ${context.machine}`,
      `Kind: ${context.kind}`,
      `State: ${context.state}`,
      `Busy: ${this.dispatcher.isActive(context.slug) ? "yes" : "no"}`,
      `Crons: ${cronCount}`,
      `Transport: ${context.transport || "n/a"}`,
      `Target: ${context.target}`,
      `Root: ${context.rootPath}`,
      `Worktree: ${context.worktreePath}`,
      context.branchName ? `Branch: ${context.branchName}` : null,
      `Codex mode: ${codexModeSummary(context)}`,
      `Session: ${context.codexSessionId || "none"}`,
      `Last run: ${context.lastRunAt || "never"}`,
      `Updated: ${context.updatedAt}`,
      `Summary: ${context.lastSummary || "n/a"}`,
      `Log: ${context.latestRunLogPath || "n/a"}`,
      context.lastError ? `Last error: ${compact(context.lastError)}` : null,
      `Next: ${nextRecommendedAction(context)}`
    ]
      .filter(Boolean)
      .join("\n");
  }

  private formatContextExplanation(context: ContextRecord): string {
    return [
      `Current context: ${context.slug}`,
      `Machine: ${context.machine}`,
      `Kind: ${context.kind}`,
      `Transport: ${context.transport || "n/a"}`,
      `Root: ${context.rootPath}`,
      `Worktree: ${context.worktreePath}`,
      context.branchName ? `Branch: ${context.branchName}` : null,
      `Codex mode: ${codexModeSummary(context)}`,
      context.codexSessionId ? `Codex session exists: yes (${context.codexSessionId})` : "Codex session exists: no",
      "If this topic is rebound:",
      ...contextRoutingNote()
    ]
      .filter(Boolean)
      .join("\n");
  }

  private formatRebindWarning(currentContext: ContextRecord): string {
    return [
      "Warning: this topic is already bound.",
      `Currently bound: ${formatBoundContext(currentContext)}`,
      ...contextRoutingNote()
    ].join("\n");
  }

  private formatCommandSyncResults(results: TelegramCommandSyncResult[]): string {
    if (!results.length) {
      return "Telegram command sync skipped because the bot token is not configured.";
    }

    return results
      .map((result) =>
        [
          result.label,
          `set=${result.setOk ? "ok" : `failed:${result.setError || "unknown"}`}`,
          `verify=${result.verifyOk ? `ok:${result.commands.length}` : `failed:${result.verifyError || "unknown"}`}`
        ].join(" | ")
      )
      .join("\n");
  }

  private attachmentRequestsForArtifacts(artifacts: string | null, filterText: string | null): TelegramAttachmentRequest[] {
    const maxAttachments = 10;
    const entries = selectArtifactEntries(artifacts, filterText).slice(0, maxAttachments);
    return entries.map((entry) => ({
      path: entry.path,
      type: null
    }));
  }

  private formatCronSchedule(job: CronJobRecord): string {
    switch (job.schedule.type) {
      case "once":
        return `once at ${job.schedule.at}`;
      case "daily":
        return `daily at ${job.schedule.time} ${job.schedule.timezone}`;
      case "weekly":
        return `every ${job.schedule.weekday} at ${job.schedule.time} ${job.schedule.timezone}`;
      case "monthly":
        return `day ${job.schedule.dayOfMonth} monthly at ${job.schedule.time} ${job.schedule.timezone}`;
      case "interval":
        return `every ${job.schedule.everyMinutes} minute(s) from ${job.schedule.anchorAt}`;
    }
  }
}
