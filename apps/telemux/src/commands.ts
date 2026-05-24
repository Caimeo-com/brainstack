import { deliverAttachmentRequests, formatAttachmentDeliveryIssues } from "./attachment-delivery";
import { resolve } from "node:path";
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
import { CronJobDraft, CronJobRecord, CronSchedule, normalizeCronSchedule, scheduleSummary } from "./cron-jobs";
import { CronScheduler } from "./cron-scheduler";
import {
  parseArtifactEntries,
  removeArtifactEntriesFromMarkdown,
  selectArtifactEntries,
  type ArtifactEntry
} from "./telegram-attachments";
import { extractTelegramInput, filterPhaseOneTelegramInput, isAudioOnlyTelegramInput, telegramMessageText } from "./telegram-inputs";
import {
  builtinRoutineCommandToken,
  builtinRoutineDraft,
  defaultBuiltinSchedule,
  getBuiltinRoutine,
  listBuiltinRoutines
} from "./routine-builtins";
import { summarizeUsage } from "./usage";
import { WorkerService } from "./workers";
import type { FactoryConfig } from "./config";
import type { TelegramBot, TelegramBotCommandScope, TelegramCommandSyncResult, TelegramMessage, TelegramTarget } from "./telegram";

function parseCommand(text: string): { command: string; rest: string; mention: string | null } | null {
  const trimmed = text.trim();
  if (!trimmed.startsWith("/")) {
    return null;
  }

  const firstSpace = trimmed.indexOf(" ");
  const head = firstSpace === -1 ? trimmed : trimmed.slice(0, firstSpace);
  const rest = firstSpace === -1 ? "" : trimmed.slice(firstSpace + 1).trim();
  const mention = head.match(/@([^@\s]+)$/)?.[1]?.toLowerCase() || null;
  return {
    command: head.replace(/@[^@\s]+$/, "").toLowerCase(),
    rest,
    mention
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

interface ArtifactSendIntent {
  filterText: string | null;
  latestOnly: boolean;
}

function artifactSendIntentFromText(text: string): ArtifactSendIntent | null {
  const normalized = text.replace(/\s+/g, " ").trim();
  if (!normalized || !/\b(?:send|attach|upload)\b/i.test(normalized)) {
    return null;
  }

  const quotedPath = normalized.match(/`([^`\s]+\.[^`\s]+)`/);
  if (quotedPath?.[1]) {
    return { filterText: quotedPath[1], latestOnly: false };
  }

  const pathLike = normalized.match(/(?:^|\s)((?:\.{0,2}\/|~\/)?[\w.-]+(?:\/[\w.-]+)*\.[A-Za-z0-9]{1,12})(?=$|[\s),.;:!?])/);
  if (pathLike?.[1]) {
    return { filterText: pathLike[1], latestOnly: false };
  }

  const object = "(?:artifact|artifacts|attachment|attachments|file|files|document|documents|report|reports|it|this|that)";
  const qualifier = "(?:(?:the\\s+)?(?:latest|last|current)|the|this|that)";
  const directRequest = new RegExp(
    `^(?:please\\s+)?(?:send|attach|upload)(?:\\s+me)?(?:\\s+${qualifier})?\\s+${object}(?:\\s+please)?[.!?]*$`,
    "i"
  );
  const politeRequest = new RegExp(
    `^(?:can|could|would)\\s+you\\s+(?:please\\s+)?(?:send|attach|upload)(?:\\s+me)?(?:\\s+${qualifier})?\\s+${object}(?:\\s+please)?[.!?]*$`,
    "i"
  );
  if (directRequest.test(normalized) || politeRequest.test(normalized)) {
    return { filterText: null, latestOnly: true };
  }

  return null;
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

interface PendingTextPrompt {
  key: string;
  target: TelegramTarget;
  userId: number | null;
  contextSlug: string;
  parts: string[];
  timer: Timer;
  createdAt: number;
}

interface CronShortcutSnapshot {
  targetKey: string;
  jobIds: string[];
  createdAt: number;
}

interface ArtifactShortcutSnapshot {
  targetKey: string;
  contextSlug: string;
  paths: string[];
  createdAt: number;
}

const CRON_SHORTCUT_TTL_MS = 15 * 60 * 1000;
const CRON_SHORTCUT_MAX_SNAPSHOTS = 50;
const ARTIFACT_SHORTCUT_TTL_MS = 15 * 60 * 1000;
const ARTIFACT_SHORTCUT_MAX_SNAPSHOTS = 50;
const TEXT_COALESCE_MAX_PARTS = 25;
const TEXT_COALESCE_MAX_CHARS = 120_000;
const TEXT_COALESCE_MAX_PENDING = 100;

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

interface ParsedScheduleArgs {
  schedule: CronSchedule;
  consumed: number;
}

function isIsoTimestamp(value: string | undefined): value is string {
  return Boolean(value && !Number.isNaN(Date.parse(value)));
}

function cronCommandUsage(): string {
  return [
    "Usage:",
    "/cron show <id|label>",
    "/cron run <id|label>",
    "/cron pause <id|label>",
    "/cron resume <id|label>",
    "/cron delete <id|label>",
    "/cron create reminder <label> daily <HH:MM> <Timezone> <text>",
    "/cron create codex <label> daily <HH:MM> <Timezone> <instruction>",
    "/cron create reminder <label> weekly <weekday> <HH:MM> <Timezone> <text>",
    "/cron create codex <label> once <ISO timestamp> <instruction>",
    "/cron install <update-check|brain-curator|daily-checkin> [schedule]",
    "/cron builtins",
    "/cron move <id|label> here",
    "/cron context <id|label> <slug-or-path>",
    "/cron mode <id|label> [fast|normal|max|clear]",
    "/cron model <id|label> [model-id|clear]",
    "/cron effort <id|label> [low|medium|high|xhigh|clear]"
  ].join("\n");
}

function parseScheduleArgs(parts: string[], offset = 0): ParsedScheduleArgs {
  const type = parts[offset]?.toLowerCase();
  if (!type) {
    throw new Error("Missing schedule. Use daily, weekly, monthly, once, or interval.");
  }

  if (type === "once") {
    const at = parts[offset + 1];
    if (!at) {
      throw new Error("One-off schedules require an ISO timestamp.");
    }
    return {
      schedule: normalizeCronSchedule({ type: "once", at }),
      consumed: 2
    };
  }

  if (type === "daily") {
    const time = parts[offset + 1];
    const timezone = parts[offset + 2];
    if (!time || !timezone) {
      throw new Error("Daily schedules require <HH:MM> <Timezone>.");
    }
    return {
      schedule: normalizeCronSchedule({ type: "daily", time, timezone }),
      consumed: 3
    };
  }

  if (type === "weekly") {
    const weekday = parts[offset + 1];
    const time = parts[offset + 2];
    const timezone = parts[offset + 3];
    if (!weekday || !time || !timezone) {
      throw new Error("Weekly schedules require <weekday> <HH:MM> <Timezone>.");
    }
    return {
      schedule: normalizeCronSchedule({ type: "weekly", weekday, time, timezone }),
      consumed: 4
    };
  }

  if (type === "monthly") {
    const dayOfMonth = parts[offset + 1];
    const time = parts[offset + 2];
    const timezone = parts[offset + 3];
    if (!dayOfMonth || !time || !timezone) {
      throw new Error("Monthly schedules require <day> <HH:MM> <Timezone>.");
    }
    return {
      schedule: normalizeCronSchedule({ type: "monthly", dayOfMonth, time, timezone }),
      consumed: 4
    };
  }

  if (type === "interval") {
    const everyMinutes = parts[offset + 1];
    if (!everyMinutes) {
      throw new Error("Interval schedules require <minutes>.");
    }
    const anchorAt = isIsoTimestamp(parts[offset + 2]) ? parts[offset + 2] : null;
    return {
      schedule: normalizeCronSchedule({
        type: "interval",
        everyMinutes,
        anchorAt: anchorAt || new Date().toISOString()
      }),
      consumed: anchorAt ? 3 : 2
    };
  }

  throw new Error(`Unknown schedule type: ${type}`);
}

function cronSelectorForShortcut(selector: string, jobs: CronJobRecord[]): CronJobRecord | null {
  if (selector === "latest" || selector === "last") {
    return jobs[0] || null;
  }

  const index = Number(selector) - 1;
  if (!Number.isInteger(index) || index < 0) {
    return null;
  }

  return jobs[index] || null;
}

function telegramTargetKey(target: TelegramTarget): string {
  return `${target.chatId}:${target.threadId ?? "none"}`;
}

function commandTimezone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  } catch {
    return "UTC";
  }
}

async function runBoundedCommand(args: string[], options: { cwd?: string; timeoutMs?: number } = {}): Promise<string> {
  const timeoutMs = options.timeoutMs ?? 5000;
  const proc = Bun.spawn(
    [
      "bash",
      "-lc",
      [
        "if command -v setsid >/dev/null 2>&1; then exec setsid \"$@\"; fi",
        "brainstack_kill_descendants() {",
        "  if ! command -v pgrep >/dev/null 2>&1; then return; fi",
        "  for child in $(pgrep -P \"$1\" 2>/dev/null || true); do",
        "    brainstack_kill_descendants \"$child\" \"$2\"",
        "    kill \"-$2\" \"$child\" 2>/dev/null || true",
        "  done",
        "}",
        "\"$@\" &",
        "child_pid=$!",
        "brainstack_stop_child() {",
        "  brainstack_kill_descendants \"$child_pid\" TERM",
        "  kill -TERM \"$child_pid\" 2>/dev/null || true",
        "  sleep 0.2",
        "  brainstack_kill_descendants \"$child_pid\" KILL",
        "  kill -KILL \"$child_pid\" 2>/dev/null || true",
        "}",
        "trap brainstack_stop_child TERM INT HUP",
        "wait \"$child_pid\""
      ].join("\n"),
      "brainstack-probe",
      ...args
    ],
    {
      cwd: options.cwd,
      stdout: "pipe",
      stderr: "pipe",
      env: process.env
    }
  );
  const killProbe = (signal: "TERM" | "KILL") => {
    proc.kill(`SIG${signal}`);
    if (proc.pid) {
      void Bun.spawn(["bash", "-lc", `kill -${signal} -- -"$1" 2>/dev/null || true`, "brainstack-probe-kill", String(proc.pid)]).exited;
    }
  };
  const collect = (async () => {
    const [stdout, stderr, exitCode] = await Promise.all([
      new Response(proc.stdout).text().catch(() => ""),
      new Response(proc.stderr).text().catch(() => ""),
      proc.exited.catch(() => 124)
    ]);
    const output = (stdout || stderr).trim();
    return exitCode === 0 ? output : output || `exit=${exitCode}`;
  })();

  let timeoutTimer: ReturnType<typeof setTimeout> | null = null;
  let forceKillTimer: ReturnType<typeof setTimeout> | null = null;
  let timedOut = false;
  const timeout = new Promise<string>((resolve) => {
    timeoutTimer = setTimeout(() => {
      timedOut = true;
      killProbe("TERM");
      forceKillTimer = setTimeout(() => killProbe("KILL"), 500);
      resolve(`timeout after ${timeoutMs}ms`);
    }, timeoutMs);
    void collect.finally(() => {
      if (timeoutTimer) {
        clearTimeout(timeoutTimer);
      }
      if (forceKillTimer && !timedOut) {
        clearTimeout(forceKillTimer);
      }
    });
  });

  try {
    return await Promise.race([collect, timeout]);
  } finally {
    if (timeoutTimer) {
      clearTimeout(timeoutTimer);
    }
    if (forceKillTimer && !timedOut) {
      clearTimeout(forceKillTimer);
    }
  }
}

function firstLine(value: string): string {
  return value.split(/\r?\n/)[0]?.trim() || "missing";
}

export class CommandHandler {
  private readonly pendingText = new Map<string, PendingTextPrompt>();
  private readonly cronShortcutSnapshots = new Map<string, CronShortcutSnapshot>();
  private readonly artifactShortcutSnapshots = new Map<string, ArtifactShortcutSnapshot>();

  constructor(
    private readonly config: FactoryConfig,
    private readonly db: FactoryDb,
    private readonly telegram: TelegramBot,
    private readonly contexts: ContextService,
    private readonly workers: WorkerService,
    private readonly dispatcher: Dispatcher,
    private readonly cronManager: CronManager,
    private readonly cronScheduler: CronScheduler
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
    if (parsed?.mention && this.config.telegramBotUsername && parsed.mention !== this.config.telegramBotUsername) {
      return;
    }
    const hasAttachments = Boolean(rawTelegramInput?.attachments.length);
    if (parsed || hasAttachments) {
      await this.flushPendingText(target, message.from?.id ?? null);
    }
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

        if (!hasAttachments && this.config.textCoalesceMs > 0) {
          this.enqueuePendingText(boundContext, text, target, message.from?.id ?? null);
          return;
        }

        if (!hasAttachments && (await this.maybeSendArtifactsFromPlainText(boundContext, text, target))) {
          return;
        }

        const response = await this.dispatcher.dispatch("resume", boundContext, text, target, {
          telegramInput
        });
        if (response.message) {
          await this.telegram.sendText(target, response.message);
        }
        return;
      }

      const artifactSnapshotShortcut = parsed.command.match(/^\/artifact_([a-z0-9]{6,10})_(\d+)(?:_(senddel|del|shred))?$/);
      if (artifactSnapshotShortcut) {
        if (!boundContext) {
          await this.telegram.sendText(target, "This topic is not bound.");
          return;
        }

        const artifacts = await this.workers.readFactoryFile(boundContext, "ARTIFACTS.md");
        const entry = this.resolveArtifactSnapshotShortcut(
          artifactSnapshotShortcut[1] || "",
          artifactSnapshotShortcut[2] || "",
          boundContext,
          target,
          artifacts
        );
        if (!entry) {
          await this.telegram.sendText(target, "That artifact shortcut expired or the artifact changed. Use /artifacts to refresh the list.");
          return;
        }

        const action = artifactSnapshotShortcut[3] || "send";
        if (action === "del" || action === "shred") {
          await this.shredArtifactEntries(boundContext, target, artifacts, [entry], "Deleted");
        } else {
          const delivery = await this.deliverArtifactEntries(boundContext, target, [entry]);
          if (action === "senddel" && delivery.sent.length) {
            await this.shredArtifactEntries(boundContext, target, artifacts, [entry], "Sent and deleted");
          }
        }
        return;
      }

      const artifactLegacyShortcut = parsed.command.match(/^\/artifact_?(\d+)(?:_(senddel|del|shred))?$/);
      if (artifactLegacyShortcut) {
        await this.telegram.sendText(target, "Numeric artifact shortcuts expire when the artifact list changes. Use /artifacts to refresh the list.");
        return;
      }

      const artifactShortcut = parsed.command.match(/^\/artifact_?(latest|last)(?:_(senddel|del|shred))?$/);
      if (artifactShortcut) {
        if (!boundContext) {
          await this.telegram.sendText(target, "This topic is not bound.");
          return;
        }

        const artifacts = await this.workers.readFactoryFile(boundContext, "ARTIFACTS.md");
        const action = artifactShortcut[2] || "send";
        if (action === "del" || action === "shred") {
          await this.shredArtifactShortcut(boundContext, target, artifacts, artifactShortcut[1] || "");
        } else {
          await this.sendArtifactShortcut(boundContext, target, artifacts, artifactShortcut[1] || "", action === "senddel");
        }
        return;
      }

      const shredSnapshotShortcut = parsed.command.match(/^\/shred_([a-z0-9]{6,10})_(\d+)$/);
      if (shredSnapshotShortcut) {
        if (!boundContext) {
          await this.telegram.sendText(target, "This topic is not bound.");
          return;
        }

        const artifacts = await this.workers.readFactoryFile(boundContext, "ARTIFACTS.md");
        const entry = this.resolveArtifactSnapshotShortcut(
          shredSnapshotShortcut[1] || "",
          shredSnapshotShortcut[2] || "",
          boundContext,
          target,
          artifacts
        );
        if (!entry) {
          await this.telegram.sendText(target, "That artifact shortcut expired or the artifact changed. Use /artifacts to refresh the list.");
          return;
        }
        await this.shredArtifactEntries(boundContext, target, artifacts, [entry], "Deleted");
        return;
      }

      const shredLegacyShortcut = parsed.command.match(/^\/shred_?(\d+)$/);
      if (shredLegacyShortcut) {
        await this.telegram.sendText(target, "Numeric artifact shortcuts expire when the artifact list changes. Use /artifacts to refresh the list.");
        return;
      }

      const shredShortcut = parsed.command.match(/^\/shred_?(latest|last)$/);
      if (shredShortcut) {
        if (!boundContext) {
          await this.telegram.sendText(target, "This topic is not bound.");
          return;
        }

        const artifacts = await this.workers.readFactoryFile(boundContext, "ARTIFACTS.md");
        await this.shredArtifactShortcut(boundContext, target, artifacts, shredShortcut[1] || "");
        return;
      }

      const cronSnapshotShortcut = parsed.command.match(/^\/cron_(show|run|pause|resume)_([a-z0-9]{6,10})_(\d+)$/);
      if (cronSnapshotShortcut) {
        const job = this.resolveCronSnapshotShortcut(cronSnapshotShortcut[2] || "", cronSnapshotShortcut[3] || "", target);
        if (!job) {
          await this.telegram.sendText(target, "That scheduled-job shortcut expired or the job was deleted. Use /crons to refresh the list.");
          return;
        }
        await this.handleCronJobAction(cronSnapshotShortcut[1] || "", job, target);
        return;
      }

      const legacyCronShortcut = parsed.command.match(/^\/cron_(show|run|pause|resume)_?(\d+)$/);
      if (legacyCronShortcut) {
        await this.telegram.sendText(target, "Numeric cron shortcuts expire when the job list changes. Use /crons to refresh the list.");
        return;
      }

      const cronLatestShortcut = parsed.command.match(/^\/cron_(show|run|pause|resume)_?(latest|last)$/);
      if (cronLatestShortcut) {
        const jobs = this.cronManager.listRelevantJobs(boundContext, target);
        const job = cronSelectorForShortcut(cronLatestShortcut[2] || "", jobs);
        if (!job) {
          await this.telegram.sendText(target, "No scheduled job matched that shortcut. Use /crons to refresh the list.");
          return;
        }
        await this.handleCronJobAction(cronLatestShortcut[1] || "", job, target);
        return;
      }

      const cronInstallShortcut = parsed.command.match(/^\/cron_install_(update_check|brain_curator|daily_checkin)$/);
      if (cronInstallShortcut) {
        await this.installBuiltinRoutine(cronInstallShortcut[1] || "", [], boundContext, target);
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
              "/updates",
              "/crons",
              "/cron <subcommand>",
              "/cron_run <id|label>",
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
        "/artifact_latest",
        "/shred",
        "/shred_latest",
              "/usage",
              "",
              "In a bound topic, plain text starts or resumes the stored Codex session.",
              "Use /artifacts to list tokenized send/send+del shortcuts. Generic requests such as \"send it\" send the latest artifact.",
              "Use /shred to list tokenized cleanup shortcuts.",
              "Use /mode, /model, and /effort to change Codex runtime behavior for this topic without rebinding it.",
              "Use /crons to inspect scheduled jobs linked to this topic/context. Use /cron install update-check, brain-curator, or daily-checkin to add built-in routines.",
              "Use /cron_run <id|label> or the shortcuts from /crons to test a scheduled job immediately."
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

        case "/updates": {
          const productRoot = resolve(this.config.projectRoot, "..", "..");
          const [head, branch, codex, claude] = await Promise.all([
            runBoundedCommand(["git", "rev-parse", "--short", "HEAD"], { cwd: productRoot }),
            runBoundedCommand(["git", "rev-parse", "--abbrev-ref", "HEAD"], { cwd: productRoot }),
            runBoundedCommand(["bash", "-lc", "command -v codex >/dev/null && codex --version || true"]),
            runBoundedCommand(["bash", "-lc", "command -v claude >/dev/null && claude --version || true"])
          ]);
          await this.telegram.sendText(
            target,
            [
              "Updates are manual/read-only.",
              `brainstack=${firstLine(branch)}@${firstLine(head)}`,
              `selected_harness=${this.config.harness}`,
              `codex=${firstLine(codex)}`,
              `claude=${firstLine(claude)}`,
              "Run on control host: brainctl updates --config ~/.config/brainstack/brainstack.yaml"
            ].join("\n")
          );
          return;
        }

        case "/crons": {
          await this.telegram.sendText(target, this.formatCronOverview(boundContext, target));
          return;
        }

        case "/cron_run": {
          const selectorText = parsed.rest.trim();
          if (!selectorText) {
            await this.telegram.sendText(target, "Usage: /cron_run <id|label>");
            return;
          }

          const job = this.resolveCronJobFromText(selectorText, boundContext, target);
          await this.handleCronJobAction("run", job, target);
          return;
        }

        case "/cron": {
          const parts = parsed.rest.split(/\s+/).filter(Boolean);
          if (!parts.length) {
            await this.telegram.sendText(target, cronCommandUsage());
            return;
          }

          if (parts[0]?.toLowerCase() === "builtins") {
            await this.telegram.sendText(target, this.formatBuiltinRoutineList());
            return;
          }

          if (parts[0]?.toLowerCase() === "install") {
            if (parts.length < 2) {
              await this.telegram.sendText(target, "Usage: /cron install <update-check|brain-curator|daily-checkin> [schedule]");
              return;
            }
            await this.installBuiltinRoutine(parts[1] || "", parts.slice(2), boundContext, target);
            return;
          }

          if (parts[0]?.toLowerCase() === "create") {
            await this.createCronFromCommand(parts.slice(1), boundContext, target);
            return;
          }

          if (parts.length < 2) {
            await this.telegram.sendText(target, cronCommandUsage());
            return;
          }

          const [subcommand, selectorText, ...restParts] = parts;
          const job = this.resolveCronJobFromText(selectorText, boundContext, target);
          const restText = restParts.join(" ").trim();

          switch (subcommand.toLowerCase()) {
            case "show": {
              await this.handleCronJobAction("show", job, target);
              return;
            }

            case "run": {
              await this.handleCronJobAction("run", job, target);
              return;
            }

            case "pause":
            case "resume":
            case "delete": {
              await this.handleCronJobAction(subcommand.toLowerCase(), job, target);
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
              artifacts
                ? this.formatArtifactsList(artifacts, boundContext, target)
                : `No artifact file available. Cached snippet: ${boundContext.lastArtifacts || "n/a"}`
            );
            return;
          }

          const sendMatch = parsed.rest.match(/^send(?:\s+(.+))?$/i);
          if (!sendMatch) {
            await this.telegram.sendText(target, "Usage: /artifacts or /artifacts send [filter]");
            return;
          }

          const filterText = sendMatch[1] || null;
          await this.sendArtifacts(boundContext, target, artifacts, filterText, !filterText);
          return;
        }

        case "/shred": {
          if (!boundContext) {
            await this.telegram.sendText(target, "This topic is not bound.");
            return;
          }

          const artifacts = await this.workers.readFactoryFile(boundContext, "ARTIFACTS.md");
          await this.telegram.sendText(target, this.formatShredList(artifacts, parsed.rest || null, boundContext, target));
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

  private pendingKey(target: TelegramTarget, userId: number | null): string {
    return `${target.chatId}:${target.threadId ?? "none"}:${userId ?? "unknown"}`;
  }

  private enqueuePendingText(context: ContextRecord, text: string, target: TelegramTarget, userId: number | null): void {
    const key = this.pendingKey(target, userId);
    const existing = this.pendingText.get(key);
    if (existing && existing.contextSlug === context.slug) {
      const nextChars = existing.parts.reduce((total, part) => total + part.length, 0) + text.length;
      if (existing.parts.length >= TEXT_COALESCE_MAX_PARTS || nextChars > TEXT_COALESCE_MAX_CHARS) {
        void this.flushPendingText(target, userId);
      } else {
        clearTimeout(existing.timer);
        existing.parts.push(text);
        existing.timer = setTimeout(() => void this.flushPendingText(target, userId), this.config.textCoalesceMs);
        return;
      }
    } else if (existing) {
      void this.flushPendingText(target, userId);
    }
    this.flushOldestPendingTextIfNeeded(key);
    const pending: PendingTextPrompt = {
      key,
      target,
      userId,
      contextSlug: context.slug,
      parts: [text],
      createdAt: Date.now(),
      timer: setTimeout(() => void this.flushPendingText(target, userId), this.config.textCoalesceMs)
    };
    this.pendingText.set(key, pending);
  }

  private flushOldestPendingTextIfNeeded(nextKey: string): void {
    if (this.pendingText.size < TEXT_COALESCE_MAX_PENDING || this.pendingText.has(nextKey)) {
      return;
    }

    const oldest = [...this.pendingText.values()].sort((a, b) => a.createdAt - b.createdAt)[0];
    if (oldest) {
      void this.flushPendingText(oldest.target, oldest.userId);
    }
  }

  private async flushPendingText(target: TelegramTarget, userId: number | null): Promise<void> {
    const key = this.pendingKey(target, userId);
    const pending = this.pendingText.get(key);
    if (!pending) {
      return;
    }
    clearTimeout(pending.timer);
    this.pendingText.delete(key);
    const context = this.contexts.getContextBySlug(pending.contextSlug);
    if (!context || context.state === "archived") {
      return;
    }
    const prompt = pending.parts.join("\n\n");
    if (pending.parts.length > 1) {
      console.log(`coalesced ${pending.parts.length} Telegram text messages for ${context.slug}`);
    }
    if (await this.maybeSendArtifactsFromPlainText(context, prompt, pending.target)) {
      return;
    }
    const response = await this.dispatcher.dispatch("resume", context, prompt, pending.target, {
      telegramInput: null
    });
    if (response.message) {
      await this.telegram.sendText(pending.target, response.message);
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

  private formatCronOverview(context: ContextRecord | null, target: TelegramTarget): string {
    const jobs = this.cronManager.listRelevantJobs(context, target);
    const shortcutToken = jobs.length ? this.createCronShortcutSnapshot(jobs, target) : null;
    const timezone = commandTimezone();
    const lines = [
      "# Scheduled Jobs",
      "",
      "Built-ins:",
      ...listBuiltinRoutines().map(
        (routine) => `- ${routine.label}: /cron_install_${builtinRoutineCommandToken(routine.name)} - ${routine.description}`
      ),
      "",
      "Generic create examples:",
      `- /cron create reminder standup daily 09:00 ${timezone} Write your standup.`,
      `- /cron create codex repo-sweep weekly monday 08:30 ${timezone} Inspect the repo and summarize risks.`,
      ""
    ];

    if (!jobs.length) {
      lines.push("No scheduled jobs are linked to this topic or context.");
      return lines.join("\n");
    }

    lines.push("Tap or send a command under a job:");
    jobs.forEach((job, index) => {
      const number = index + 1;
      lines.push(`- ${number}. ${job.label} (${job.kind}, ${job.enabled ? "enabled" : "paused"})`);
      lines.push(`  next: ${job.nextRunAt || "none"}`);
      lines.push(`  schedule: ${scheduleSummary(job.schedule)}`);
      lines.push(`  show: /cron_show_${shortcutToken}_${number}`);
      lines.push(`  run now: /cron_run_${shortcutToken}_${number}`);
      lines.push(`  pause: /cron_pause_${shortcutToken}_${number}`);
      lines.push(`  resume: /cron_resume_${shortcutToken}_${number}`);
      lines.push(`  id: ${job.id}`);
    });

    return lines.join("\n");
  }

  private createCronShortcutSnapshot(jobs: CronJobRecord[], target: TelegramTarget): string {
    this.pruneCronShortcutSnapshots();
    let token = "";
    do {
      token = Math.random().toString(36).slice(2, 8).padEnd(6, "0");
    } while (this.cronShortcutSnapshots.has(token));

    this.cronShortcutSnapshots.set(token, {
      targetKey: telegramTargetKey(target),
      jobIds: jobs.map((job) => job.id),
      createdAt: Date.now()
    });
    return token;
  }

  private resolveCronSnapshotShortcut(token: string, selector: string, target: TelegramTarget): CronJobRecord | null {
    this.pruneCronShortcutSnapshots();
    const snapshot = this.cronShortcutSnapshots.get(token);
    if (!snapshot || snapshot.targetKey !== telegramTargetKey(target)) {
      return null;
    }

    const index = Number(selector) - 1;
    if (!Number.isInteger(index) || index < 0 || index >= snapshot.jobIds.length) {
      return null;
    }

    const job = this.db.getCronJob(snapshot.jobIds[index] || "");
    if (!job) {
      return null;
    }
    const currentContext = this.contexts.getContextByTopic(target.chatId, target.threadId);
    const stillRelevant = this.cronManager.listRelevantJobs(currentContext, target).some((candidate) => candidate.id === job.id);
    return stillRelevant ? job : null;
  }

  private pruneCronShortcutSnapshots(): void {
    const now = Date.now();
    for (const [token, snapshot] of this.cronShortcutSnapshots) {
      if (now - snapshot.createdAt > CRON_SHORTCUT_TTL_MS) {
        this.cronShortcutSnapshots.delete(token);
      }
    }

    while (this.cronShortcutSnapshots.size > CRON_SHORTCUT_MAX_SNAPSHOTS) {
      const oldest = [...this.cronShortcutSnapshots.entries()].sort((a, b) => a[1].createdAt - b[1].createdAt)[0]?.[0];
      if (!oldest) {
        break;
      }
      this.cronShortcutSnapshots.delete(oldest);
    }
  }

  private formatBuiltinRoutineList(): string {
    return [
      "# Built-in Routines",
      "",
      ...listBuiltinRoutines().flatMap((routine) => {
        const schedule = defaultBuiltinSchedule(routine);
        const custom =
          schedule.type === "weekly"
            ? `/cron install ${routine.name} weekly ${schedule.weekday} ${schedule.time} ${schedule.timezone}`
            : schedule.type === "daily"
              ? `/cron install ${routine.name} daily ${schedule.time} ${schedule.timezone}`
              : `/cron_install_${builtinRoutineCommandToken(routine.name)}`;
        return [
          `- ${routine.label}`,
          `  ${routine.description}`,
          `  install: /cron_install_${builtinRoutineCommandToken(routine.name)}`,
          `  custom: ${custom}`
        ];
      })
    ].join("\n");
  }

  private async createCronFromCommand(
    parts: string[],
    context: ContextRecord | null,
    target: TelegramTarget
  ): Promise<void> {
    const [kind, label] = parts;
    if ((kind !== "reminder" && kind !== "codex") || !label) {
      await this.telegram.sendText(target, cronCommandUsage());
      return;
    }

    if (kind === "codex" && !context) {
      await this.telegram.sendText(target, "Codex scheduled jobs require this topic to be bound to a context first.");
      return;
    }

    const parsedSchedule = parseScheduleArgs(parts, 2);
    const bodyParts = parts.slice(2 + parsedSchedule.consumed);
    const body = bodyParts.join(" ").trim();
    if (!body) {
      await this.telegram.sendText(
        target,
        kind === "reminder" ? "Reminder jobs require reminder text." : "Codex jobs require an instruction."
      );
      return;
    }

    const draft: CronJobDraft = {
      label,
      kind,
      schedule: parsedSchedule.schedule,
      executionContextSlug: kind === "codex" ? context?.slug || null : context?.slug || null,
      targetChatId: target.chatId,
      targetThreadId: target.threadId,
      instruction: kind === "codex" ? body : null,
      reminderText: kind === "reminder" ? body : null
    };
    const job = await this.cronManager.createJob(draft, { context, target });
    await this.telegram.sendText(target, `Cron created: ${job.id} (${job.label}) next=${job.nextRunAt || "none"}`);
  }

  private resolveCronJobFromText(selectorText: string, context: ContextRecord | null, target: TelegramTarget): CronJobRecord {
    const scoped = this.cronManager.listRelevantJobs(context, target);
    const byId = scoped.find((job) => job.id === selectorText);
    if (byId) {
      return byId;
    }

    const byLabel = scoped.filter((job) => job.label.toLowerCase() === selectorText.trim().toLowerCase());
    if (byLabel.length === 1) {
      return byLabel[0];
    }
    if (byLabel.length > 1) {
      throw new Error(`Cron label is ambiguous in this topic/context: ${selectorText}`);
    }

    throw new Error(`No cron job matched in this topic/context: ${selectorText}`);
  }

  private async installBuiltinRoutine(
    routineName: string,
    scheduleParts: string[],
    context: ContextRecord | null,
    target: TelegramTarget
  ): Promise<void> {
    const routine = getBuiltinRoutine(routineName);
    if (!routine) {
      await this.telegram.sendText(target, "Unknown built-in routine. Use /cron builtins.");
      return;
    }

    if (!context) {
      await this.telegram.sendText(
        target,
        `Built-in routine ${routine.label} needs a bound context so replies and artifacts have a durable workspace. Use /newctx first.`
      );
      return;
    }

    let schedule = defaultBuiltinSchedule(routine);
    if (scheduleParts.length) {
      const parsedSchedule = parseScheduleArgs(scheduleParts, 0);
      if (parsedSchedule.consumed !== scheduleParts.length) {
        await this.telegram.sendText(target, "Built-in routine schedule syntax does not accept trailing text. Use /cron install <builtin> daily <HH:MM> <Timezone>.");
        return;
      }
      schedule = parsedSchedule.schedule;
    }
    const draft = {
      ...builtinRoutineDraft(routine, schedule, context.slug),
      targetChatId: target.chatId,
      targetThreadId: target.threadId
    };
    const existing = this.cronManager
      .listRelevantJobs(context, target)
      .find((job) => job.label.toLowerCase() === routine.label.toLowerCase());

    if (existing) {
      const updated = await this.cronManager.updateJob(existing, {
        schedule,
        executionContextSlug: context.slug,
        targetChatId: target.chatId,
        targetThreadId: target.threadId,
        instruction: draft.instruction || null,
        reminderText: draft.reminderText || null,
        runner: draft.runner || null,
        enabled: true
      });
      await this.telegram.sendText(target, `Built-in routine updated: ${updated.id} (${updated.label}) next=${updated.nextRunAt || "none"}`);
      return;
    }

    const created = await this.cronManager.createJob(draft, { context, target });
    await this.telegram.sendText(target, `Built-in routine installed: ${created.id} (${created.label}) next=${created.nextRunAt || "none"}`);
  }

  private async handleCronJobAction(
    action: string,
    job: CronJobRecord,
    target: TelegramTarget
  ): Promise<void> {
    switch (action) {
      case "show":
        await this.telegram.sendText(target, this.formatCronJobDetails(job));
        return;
      case "run": {
        const result = await this.cronScheduler.runJobNow(job);
        await this.telegram.sendText(target, result);
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
      default:
        await this.telegram.sendText(target, `Unknown cron action: ${action}`);
        return;
    }
  }

  private formatCronJobDetails(job: CronJobRecord): string {
    const effectiveContext = job.executionContextSlug ? this.db.getContextBySlug(job.executionContextSlug) : null;
    const runtimeModel = job.modelOverride || effectiveContext?.modelOverride || "default";
    const runtimeEffort = job.reasoningEffortOverride || effectiveContext?.reasoningEffortOverride || "default";
    return [
      `Cron: ${job.id}`,
      `Label: ${job.label}`,
      `Kind: ${job.kind}`,
      `Enabled: ${job.enabled ? "yes" : "no"}`,
      `Schedule: ${scheduleSummary(job.schedule)}`,
      `Next run: ${job.nextRunAt || "none"}`,
      `Pending run: ${job.pendingRunAt || "none"}`,
      `Last run: ${job.lastRunAt || "never"}`,
      `Context: ${job.executionContextSlug || "none"}`,
      `Target: ${job.targetChatId}:${job.targetThreadId ?? "none"}`,
      `Mode: model=${runtimeModel} effort=${runtimeEffort}`,
      job.reminderText ? `Reminder: ${job.reminderText}` : null,
      job.instruction ? `Instruction: ${compact(job.instruction, 900)}` : null,
      job.lastResult ? `Last result: ${compact(job.lastResult)}` : null,
      job.lastError ? `Last error: ${compact(job.lastError)}` : null
    ]
      .filter(Boolean)
      .join("\n");
  }

  private formatArtifactsList(artifacts: string, context: ContextRecord, target: TelegramTarget): string {
    const entries = parseArtifactEntries(artifacts);
    if (!entries.length) {
      return "No sendable artifact paths were found. Record artifacts as file paths, for example: - `report.md`";
    }
    const shortcutToken = this.createArtifactShortcutSnapshot(entries, context, target);

    const lines = [
      "# Artifacts",
      "",
      "Tap or send a command under a file:",
      "- latest send: /artifact_latest",
      "- latest send + del: /artifact_latest_senddel",
      "- latest del: /shred_latest"
    ];

    entries.forEach((entry, index) => {
      const number = index + 1;
      lines.push(`- ${entry.fileName}`);
      lines.push(`  send: /artifact_${shortcutToken}_${number}`);
      lines.push(`  send + del: /artifact_${shortcutToken}_${number}_senddel`);
      lines.push(`  del: /shred_${shortcutToken}_${number}`);
      lines.push(`  path: ${entry.path}`);
    });

    return lines.join("\n");
  }

  private formatShredList(
    artifacts: string | null,
    filterText: string | null,
    context: ContextRecord,
    target: TelegramTarget
  ): string {
    const filter = filterText?.trim().toLowerCase() || "";
    const entries = parseArtifactEntries(artifacts)
      .map((entry, index) => ({ entry, index }))
      .filter(({ entry }) => {
        if (!filter) {
          return true;
        }

        return [entry.path, entry.fileName, entry.line].some((value) => value.toLowerCase().includes(filter));
      });
    if (!entries.length) {
      return filterText?.trim()
        ? `No artifact file paths matched for shredding: ${filterText.trim()}`
        : "No artifact file paths were found in .factory/ARTIFACTS.md.";
    }
    const shortcutToken = this.createArtifactShortcutSnapshot(entries.map(({ entry }) => entry), context, target);

    const lines = [
      "# Shred Artifacts",
      "",
      "Tap or send one command to delete the file from disk and remove it from .factory/ARTIFACTS.md:",
      "- /shred_latest"
    ];

    entries.forEach(({ entry }, index) => {
      lines.push(`- /shred_${shortcutToken}_${index + 1} ${entry.fileName}`);
      lines.push(`  path: ${entry.path}`);
    });

    return lines.join("\n");
  }

  private createArtifactShortcutSnapshot(entries: ArtifactEntry[], context: ContextRecord, target: TelegramTarget): string {
    this.pruneArtifactShortcutSnapshots();
    let token = "";
    do {
      token = Math.random().toString(36).slice(2, 8).padEnd(6, "0");
    } while (this.artifactShortcutSnapshots.has(token));

    this.artifactShortcutSnapshots.set(token, {
      targetKey: telegramTargetKey(target),
      contextSlug: context.slug,
      paths: entries.map((entry) => entry.path),
      createdAt: Date.now()
    });
    return token;
  }

  private resolveArtifactSnapshotShortcut(
    token: string,
    selector: string,
    context: ContextRecord,
    target: TelegramTarget,
    artifacts: string | null
  ): ArtifactEntry | null {
    this.pruneArtifactShortcutSnapshots();
    const snapshot = this.artifactShortcutSnapshots.get(token);
    if (!snapshot || snapshot.targetKey !== telegramTargetKey(target) || snapshot.contextSlug !== context.slug) {
      return null;
    }

    const index = Number(selector) - 1;
    if (!Number.isInteger(index) || index < 0 || index >= snapshot.paths.length) {
      return null;
    }

    const path = snapshot.paths[index];
    return parseArtifactEntries(artifacts).find((entry) => entry.path === path) || null;
  }

  private pruneArtifactShortcutSnapshots(): void {
    const now = Date.now();
    for (const [token, snapshot] of this.artifactShortcutSnapshots) {
      if (now - snapshot.createdAt > ARTIFACT_SHORTCUT_TTL_MS) {
        this.artifactShortcutSnapshots.delete(token);
      }
    }

    while (this.artifactShortcutSnapshots.size > ARTIFACT_SHORTCUT_MAX_SNAPSHOTS) {
      const oldest = [...this.artifactShortcutSnapshots.entries()].sort((a, b) => a[1].createdAt - b[1].createdAt)[0]?.[0];
      if (!oldest) {
        break;
      }
      this.artifactShortcutSnapshots.delete(oldest);
    }
  }

  private artifactEntriesForSend(artifacts: string | null, filterText: string | null, latestOnly: boolean): ArtifactEntry[] {
    const maxAttachments = 10;
    const entries = selectArtifactEntries(artifacts, filterText);
    return latestOnly ? entries.slice(-1) : entries.slice(0, maxAttachments);
  }

  private async maybeSendArtifactsFromPlainText(context: ContextRecord, text: string, target: TelegramTarget): Promise<boolean> {
    const intent = artifactSendIntentFromText(text);
    if (!intent) {
      return false;
    }

    const artifacts = await this.workers.readFactoryFile(context, "ARTIFACTS.md");
    await this.sendArtifacts(context, target, artifacts, intent.filterText, intent.latestOnly);
    return true;
  }

  private async sendArtifactShortcut(
    context: ContextRecord,
    target: TelegramTarget,
    artifacts: string | null,
    selector: string,
    deleteAfterSend = false
  ): Promise<void> {
    const entries = parseArtifactEntries(artifacts);
    if (!entries.length) {
      await this.telegram.sendText(target, "No artifact file paths were found in .factory/ARTIFACTS.md.");
      return;
    }

    const entry =
      selector === "latest" || selector === "last"
        ? entries.at(-1)
        : entries[Number(selector) - 1];

    if (!entry) {
      await this.telegram.sendText(target, `No artifact exists for ${selector}. Use /artifacts to list available artifact commands.`);
      return;
    }

    const delivery = await this.deliverArtifactEntries(context, target, [entry]);
    if (deleteAfterSend && delivery.sent.length) {
      await this.shredArtifactEntries(context, target, artifacts, [entry], "Sent and deleted");
    }
  }

  private async shredArtifactShortcut(
    context: ContextRecord,
    target: TelegramTarget,
    artifacts: string | null,
    selector: string
  ): Promise<void> {
    const entries = parseArtifactEntries(artifacts);
    if (!entries.length) {
      await this.telegram.sendText(target, "No artifact file paths were found in .factory/ARTIFACTS.md.");
      return;
    }

    const entry =
      selector === "latest" || selector === "last"
        ? entries.at(-1)
        : entries[Number(selector) - 1];

    if (!entry) {
      await this.telegram.sendText(target, `No artifact exists for ${selector}. Use /shred to list available cleanup commands.`);
      return;
    }

    await this.shredArtifactEntries(context, target, artifacts, [entry], "Deleted");
  }

  private async sendArtifacts(
    context: ContextRecord,
    target: TelegramTarget,
    artifacts: string | null,
    filterText: string | null,
    latestOnly = false
  ): Promise<void> {
    const entries = this.artifactEntriesForSend(artifacts, filterText, latestOnly);
    if (!entries.length) {
      await this.telegram.sendText(
        target,
        filterText?.trim()
          ? `No artifact file paths matched: ${filterText.trim()}`
          : "No artifact file paths were found in .factory/ARTIFACTS.md."
      );
      return;
    }

    await this.deliverArtifactEntries(context, target, entries);
  }

  private async deliverArtifactEntries(context: ContextRecord, target: TelegramTarget, entries: ArtifactEntry[]) {
    const requests = entries.map((entry) => ({
      path: entry.path,
      type: null
    }));
    if (!requests.length) {
      await this.telegram.sendText(target, "No artifact file paths were found in .factory/ARTIFACTS.md.");
      return {
        sent: [],
        skipped: [],
        failed: []
      };
    }

    const delivery = await deliverAttachmentRequests(this.workers, this.telegram, context, target, requests);
    const notes = formatAttachmentDeliveryIssues(delivery);

    if (!delivery.sent.length) {
      await this.telegram.sendText(target, notes || "No recorded artifact files could be uploaded.");
      return delivery;
    }

    if (notes) {
      await this.telegram.sendText(target, notes);
    }

    return delivery;
  }

  private async shredArtifactEntries(
    context: ContextRecord,
    target: TelegramTarget,
    artifacts: string | null,
    entries: ArtifactEntry[],
    verb: string
  ): Promise<void> {
    if (this.dispatcher.isActive(context.slug)) {
      await this.telegram.sendText(target, `${context.slug} has an active job. Artifact deletion is disabled until the run finishes.`);
      return;
    }

    const removedPaths: string[] = [];
    const deleted: string[] = [];
    const missing: string[] = [];
    const failed: string[] = [];

    for (const entry of entries) {
      try {
        const result = await this.workers.deleteArtifactFile(context, entry.path);
        removedPaths.push(entry.path);
        if (result.status === "deleted") {
          deleted.push(entry.fileName);
        } else {
          missing.push(entry.fileName);
        }
      } catch (error) {
        failed.push(`${entry.path}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }

    if (removedPaths.length) {
      let updatedArtifacts: string | null = null;
      let wroteArtifacts = false;
      for (let attempt = 0; attempt < 3; attempt += 1) {
        const latestArtifacts = await this.workers.readFactoryFile(context, "ARTIFACTS.md");
        updatedArtifacts = removeArtifactEntriesFromMarkdown(latestArtifacts ?? artifacts, removedPaths);
        wroteArtifacts = await this.workers.writeWorkspaceFileIfUnchanged(
          context,
          ".factory/ARTIFACTS.md",
          latestArtifacts,
          updatedArtifacts
        );
        if (wroteArtifacts) {
          break;
        }
      }

      if (wroteArtifacts && updatedArtifacts !== null) {
        this.contexts.saveContext({
          ...context,
          lastArtifacts: snippet(updatedArtifacts)
        });
      } else {
        failed.push(".factory/ARTIFACTS.md changed during cleanup; deleted files were not removed from metadata. Retry /shred.");
      }
    }

    const lines = [`${verb} ${deleted.length + missing.length} artifact(s).`];
    if (deleted.length) {
      lines.push(`Deleted: ${deleted.join(", ")}`);
    }
    if (missing.length) {
      lines.push(`Removed stale entries for missing files: ${missing.join(", ")}`);
    }
    if (failed.length) {
      lines.push("Not deleted:");
      lines.push(...failed.map((entry) => `- ${entry}`));
    }

    await this.telegram.sendText(target, lines.join("\n"));
  }

}
