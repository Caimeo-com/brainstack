import { deliverAttachmentRequests, formatAttachmentDeliveryIssues } from "./attachment-delivery";
import { randomUUID } from "node:crypto";
import { open } from "node:fs/promises";
import {
  CODEX_MODE_PRESETS,
  formatCodexModeSummary,
  formatCodexRuntimeOverrides,
  normalizeCodexModelOverride,
  parseCodexModePreset,
  parseCodexReasoningEffort
} from "./codex-runtime";
import { formatControlMetaResponse, resolveControlMetaKind } from "./control-meta";
import { CronManager } from "./cron-manager";
import { decideProposal, fetchCuratorStatus, fetchProposals, type BrainProposalSummary } from "./curator-report";
import { ContextRecord, FactoryDb, type PendingTextRecord } from "./db";
import { ContextService, nextRecommendedAction, normalizeSlug } from "./contexts";
import { Dispatcher } from "./dispatcher";
import { CronJobDraft, CronJobRecord, CronSchedule, normalizeCronSchedule, scheduleSummary } from "./cron-jobs";
import { CronScheduler } from "./cron-scheduler";
import { classifyPreDispatch } from "./pre-dispatch-router";
import {
  parseArtifactEntries,
  removeArtifactEntriesFromMarkdown,
  selectArtifactEntries,
  type ArtifactEntry
} from "./telegram-attachments";
import {
  extractTelegramInput,
  filterPhaseOneTelegramInput,
  telegramMessageText,
  type TelegramInboundMessageInput
} from "./telegram-inputs";
import {
  formatTranscriptEcho,
  hasAudioTelegramInput,
  mergeTelegramTextAndTranscript,
  transcribeTelegramAudioInput,
  withoutAudioTelegramInput
} from "./transcription";
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

const PROPOSAL_STATUS_FILTERS = new Set(["open", "pending", "approved", "applied", "rejected", "superseded", "needs-human"]);
const PROPOSAL_QUALITY_FILTERS = new Set(["ready", "needs-context", "needs-evidence", "too-vague"]);
const DEFAULT_PROPOSAL_LIST_LIMIT = 15;
const MAX_PROPOSAL_LIST_LIMIT = 30;
const NEW_CONTEXT_WIZARD_MAX_AGE_MS = 10 * 60 * 1000;
const PROPOSAL_CURATION_CONTEXT_SLUG = "proposal-curation";

type NewContextWizardStep = "slug-or-machine" | "machine" | "target";

interface PendingNewContextWizard {
  targetKey: string;
  slug: string;
  machine: string | null;
  step: NewContextWizardStep;
  createdAt: number;
}

interface ProposalListRequest {
  status: string;
  quality: string | null;
  query: string;
  project: string | null;
  scope: string | null;
  group: string | null;
  kind: string | null;
  limit: number;
  help: boolean;
  error: string | null;
}

type VoiceCapabilityAction = "install" | "doctor" | "list";

interface VoiceCapabilityIntent {
  action: VoiceCapabilityAction;
  target: string | null;
  model: string | null;
  help: boolean;
  error: string | null;
}

function voiceCapabilityUsage(): string {
  return [
    "Voice transcription commands:",
    "/voice install <machine> [model tiny.en|small.en|medium.en|large-v2|large-v3]",
    "/voice status",
    "",
    "Plain language also works:",
    "install voice on erbine",
    "install transcription on valkyrie"
  ].join("\n");
}

function parseVoiceModel(parts: string[]): string | null {
  for (let index = 0; index < parts.length; index += 1) {
    const part = parts[index]?.toLowerCase();
    if ((part === "model" || part === "using" || part === "with") && parts[index + 1]) {
      return parts[index + 1] || null;
    }
    const keyed = parts[index]?.match(/^(?:model|using)[:=]([A-Za-z0-9._-]+)$/i)?.[1];
    if (keyed) {
      return keyed;
    }
  }
  return null;
}

function parseVoiceCapabilityCommand(rest: string): VoiceCapabilityIntent {
  const parts = rest.split(/\s+/).map((part) => part.trim()).filter(Boolean);
  if (!parts.length || ["help", "--help", "-h"].includes(parts[0]?.toLowerCase() || "")) {
    return { action: "list", target: null, model: null, help: true, error: null };
  }

  const action = parts[0]?.toLowerCase();
  if (["status", "doctor", "check"].includes(action || "")) {
    return { action: "doctor", target: null, model: null, help: false, error: null };
  }
  if (["list", "show"].includes(action || "")) {
    return { action: "list", target: null, model: null, help: false, error: null };
  }
  if (!["install", "setup", "enable"].includes(action || "")) {
    return { action: "list", target: null, model: null, help: false, error: `Unknown voice action: ${action || "(missing)"}` };
  }

  const model = parseVoiceModel(parts);
  const target = parts.slice(1).find((part) => {
    const normalized = part.toLowerCase();
    return !["on", "to", "for", "machine", "voice", "transcription", "model", "using", "with"].includes(normalized) && !/^model[:=]/i.test(part);
  }) || null;
  return {
    action: "install",
    target,
    model,
    help: false,
    error: target ? null : "Install needs a target machine, for example /voice install erbine."
  };
}

function parseNaturalVoiceCapabilityIntent(text: string): VoiceCapabilityIntent | null {
  const normalized = text.replace(/\s+/g, " ").trim();
  if (!normalized) {
    return null;
  }

  const statusMatch = normalized.match(/^(?:voice|transcription)\s+(?:status|doctor|check)$/i);
  if (statusMatch) {
    return { action: "doctor", target: null, model: null, help: false, error: null };
  }

  const installMatch = normalized.match(
    /^(?:please\s+)?(?:install|setup|set up|enable)\s+(?:voice|voice transcription|transcription)(?:\s+(?:on|to|for)\s+([A-Za-z0-9._-]+))?(?:\s+(.*))?$/i
  );
  if (!installMatch) {
    return null;
  }

  const tail = installMatch[2] || "";
  const model = parseVoiceModel(tail.split(/\s+/).filter(Boolean));
  const target = installMatch[1] || null;
  return {
    action: "install",
    target,
    model,
    help: false,
    error: target ? null : "Install needs a target machine, for example: install voice on erbine."
  };
}

function proposalListUsage(): string {
  return [
    "Usage: /proposals [open|pending|needs-human|approved|applied|rejected|superseded] [ready|needs-context|needs-evidence|too-vague] [search terms|project:NAME|scope:NAME|group:NAME] [limit=N]",
    "Examples:",
    "/proposals pending",
    "/proposals needs-human needs-context",
    "/proposals open project:lindy limit=10"
  ].join("\n");
}

function parseProposalListRequest(text: string): ProposalListRequest {
  const result: ProposalListRequest = {
    status: "open",
    quality: null,
    query: "",
    project: null,
    scope: null,
    group: null,
    kind: null,
    limit: DEFAULT_PROPOSAL_LIST_LIMIT,
    help: false,
    error: null
  };
  const queryParts: string[] = [];
  for (const rawPart of text.split(/\s+/).map((part) => part.trim()).filter(Boolean)) {
    const part = rawPart.toLowerCase();
    if (part === "help" || part === "--help" || part === "-h") {
      result.help = true;
      continue;
    }

    const keyed = part.match(/^(status|quality|q|query|project|scope|group|cluster|kind|limit|n)[:=](.+)$/);
    const key = keyed?.[1] || "";
    const value = keyed?.[2] || "";
    if ((key === "status" || !key) && PROPOSAL_STATUS_FILTERS.has(key ? value : part)) {
      result.status = key ? value : part;
      continue;
    }
    if ((key === "quality" || !key) && PROPOSAL_QUALITY_FILTERS.has(key ? value : part)) {
      result.quality = key ? value : part;
      continue;
    }
    if (key === "limit" || key === "n") {
      const parsed = Number(value);
      if (!Number.isInteger(parsed) || parsed < 1) {
        result.error = `Invalid proposal list limit: ${value}`;
      } else {
        result.limit = Math.min(parsed, MAX_PROPOSAL_LIST_LIMIT);
      }
      continue;
    }
    if (key === "status") {
      result.error = `Unknown proposal status filter: ${value}`;
      continue;
    }
    if (key === "quality") {
      result.error = `Unknown proposal quality filter: ${value}`;
      continue;
    }
    if (key === "q" || key === "query") {
      queryParts.push(value);
      continue;
    }
    if (key === "project") {
      result.project = value;
      continue;
    }
    if (key === "scope") {
      result.scope = value;
      continue;
    }
    if (key === "group" || key === "cluster") {
      result.group = value;
      continue;
    }
    if (key === "kind") {
      result.kind = value;
      continue;
    }
    queryParts.push(rawPart);
  }
  result.query = queryParts.join(" ").trim();
  return result;
}

function proposalSearchText(proposal: BrainProposalSummary): string {
  return [
    proposal.id,
    proposal.title,
    proposal.status,
    proposal.target_page,
    proposal.risk,
    proposal.quality_decision,
    proposal.project,
    proposal.scope,
    proposal.memory_kind,
    proposal.cluster_key,
    proposal.cluster_label,
    proposal.legacy_format ? "legacy" : null
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

function matchesProposalListRequest(proposal: BrainProposalSummary, request: ProposalListRequest): boolean {
  if (request.status !== "open" && proposal.status !== request.status) {
    return false;
  }
  if (request.quality && proposal.quality_decision !== request.quality) {
    return false;
  }
  if (request.project && proposal.project?.toLowerCase() !== request.project) {
    return false;
  }
  if (request.scope && proposal.scope?.toLowerCase() !== request.scope) {
    return false;
  }
  if (request.kind && proposal.memory_kind?.toLowerCase() !== request.kind) {
    return false;
  }
  if (
    request.group &&
    proposal.cluster_key?.toLowerCase() !== request.group &&
    !proposal.cluster_label?.toLowerCase().includes(request.group)
  ) {
    return false;
  }
  const query = request.query.toLowerCase();
  return !query || proposalSearchText(proposal).includes(query);
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

function formatElapsed(ms: number): string {
  const totalSeconds = Math.max(0, Math.floor(ms / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
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

  const object = "(?:artifact|artifacts|attachment|attachments|file|files|document|documents|report|reports|it|this|that)";
  const qualifier = "(?:(?:the\\s+)?(?:latest|last|current)|the|this|that)";
  const pathToken = "`([^`\\s]+\\.[^`\\s]+)`|((?:\\.{0,2}/|~/)?[\\w.-]+(?:/[\\w.-]+)*\\.[A-Za-z0-9]{1,12})";
  const directPathRequest = new RegExp(
    `^(?:please\\s+)?(?:send|attach|upload)(?:\\s+me)?(?:\\s+${qualifier})?(?:\\s+(?:artifact|attachment|file|document|report))?\\s+(?:${pathToken})(?:\\s+please)?[.!?]*$`,
    "i"
  );
  const politePathRequest = new RegExp(
    `^(?:can|could|would)\\s+you\\s+(?:please\\s+)?(?:send|attach|upload)(?:\\s+me)?(?:\\s+${qualifier})?(?:\\s+(?:artifact|attachment|file|document|report))?\\s+(?:${pathToken})(?:\\s+please)?[.!?]*$`,
    "i"
  );
  const pathRequest = normalized.match(directPathRequest) || normalized.match(politePathRequest);
  const requestedPath = pathRequest?.[1] || pathRequest?.[2] || pathRequest?.[3] || pathRequest?.[4];
  if (requestedPath) {
    return { filterText: requestedPath, latestOnly: false };
  }

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

const LOG_TAIL_READ_BYTES = 2 * 1024 * 1024;

// Harness logs can echo env vars, provider keys, and token-shaped strings. Redact
// the common shapes before any log content leaves the trusted host into Telegram.
const LOG_SECRET_PATTERNS: Array<{ pattern: RegExp; replacement: string }> = [
  { pattern: /\b\d{6,12}:[A-Za-z0-9_-]{30,}\b/g, replacement: "[redacted-telegram-token]" },
  { pattern: /\b(sk|rk|pk)-[A-Za-z0-9_-]{16,}\b/g, replacement: "[redacted-api-key]" },
  { pattern: /\b(sk|rk|pk|whsec)_(?:live|test|prod)?_?[A-Za-z0-9]{16,}\b/g, replacement: "[redacted-api-key]" },
  { pattern: /\bgh[pousr]_[A-Za-z0-9]{20,}\b/g, replacement: "[redacted-github-token]" },
  { pattern: /\bxox[baprs]-[A-Za-z0-9-]{10,}\b/g, replacement: "[redacted-slack-token]" },
  { pattern: /\bbs1_[A-Za-z0-9+/=_-]{16,}\b/g, replacement: "[redacted-invite]" },
  { pattern: /\bAKIA[0-9A-Z]{16}\b/g, replacement: "[redacted-aws-key]" },
  { pattern: /\beyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/g, replacement: "[redacted-jwt]" },
  {
    pattern: /\b([A-Z][A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|API_KEY|APIKEY|PRIVATE_KEY|CREDENTIALS?)[A-Z0-9_]*)\s*[=:]\s*\S+/g,
    replacement: "$1=[redacted]"
  },
  { pattern: /(Authorization:\s*Bearer\s+)\S+/gi, replacement: "$1[redacted]" },
  { pattern: /-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g, replacement: "[redacted-private-key]" }
];

export function redactLogSecrets(text: string): string {
  let redacted = text;
  for (const { pattern, replacement } of LOG_SECRET_PATTERNS) {
    redacted = redacted.replace(pattern, replacement);
  }
  return redacted;
}

async function readTail(path: string, lines = 40): Promise<string> {
  const file = Bun.file(path);
  if (!(await file.exists())) {
    return `Missing log file: ${path}`;
  }

  const handle = await open(path, "r");
  let text = "";
  try {
    const info = await handle.stat();
    const start = Math.max(0, info.size - LOG_TAIL_READ_BYTES);
    const length = info.size - start;
    const buffer = Buffer.alloc(length);
    if (length > 0) {
      await handle.read(buffer, 0, length, start);
    }
    text = buffer.toString("utf8");
  } finally {
    await handle.close();
  }
  const tail = text.split("\n").slice(-lines).join("\n").trim();
  return tail ? redactLogSecrets(tail) : "(log is empty)";
}

function messageTarget(message: TelegramMessage): TelegramTarget {
  return {
    chatId: message.chat.id,
    threadId: message.message_thread_id ?? null
  };
}

function slugifyPhrase(value: string, fallback: string): string {
  const slug = value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/-{2,}/g, "-")
    .slice(0, 63)
    .replace(/-+$/g, "");

  const candidate = slug.length >= 2 ? slug : fallback;

  try {
    return normalizeSlug(candidate);
  } catch {
    return normalizeSlug(fallback);
  }
}

function topicSlugCandidate(message: TelegramMessage): { slug: string; source: string } {
  const topicName = message.forum_topic_created?.name?.trim() || message.reply_to_message?.forum_topic_created?.name?.trim() || "";
  if (topicName) {
    return {
      slug: slugifyPhrase(topicName, `topic-${message.message_thread_id ?? Math.abs(message.chat.id)}`),
      source: "Telegram topic title"
    };
  }

  if (!message.is_topic_message && message.chat.title?.trim()) {
    return {
      slug: slugifyPhrase(message.chat.title, `chat-${Math.abs(message.chat.id)}`),
      source: "Telegram chat title"
    };
  }

  if (message.message_thread_id !== undefined) {
    return {
      slug: normalizeSlug(`topic-${message.message_thread_id}`),
      source: "Telegram thread id; topic title was not included in this update"
    };
  }

  return {
    slug: normalizeSlug(`chat-${Math.abs(message.chat.id)}`),
    source: "Telegram chat id; no title was included in this update"
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

function formatMachineChoices(hosts: string[]): string[] {
  if (!hosts.length) {
    return ["No machines are configured yet. Run /workers, fix worker config, or use the full command with a known machine name."];
  }

  return hosts.map((host, index) => `${index + 1}) ${host}`);
}

function parseNewContextTargetReply(input: string): { target: string; baseBranch: string | null } | null {
  const trimmed = input.trim();
  const normalized = trimmed.toLowerCase();
  if (!trimmed) {
    return null;
  }

  if (
    normalized === "1" ||
    normalized === "scratch" ||
    normalized === "topic" ||
    normalized === "topic-workspace" ||
    normalized === "work" ||
    normalized === "workspace" ||
    normalized === "curation" ||
    normalized === "proposal" ||
    normalized === "proposals"
  ) {
    return { target: "scratch", baseBranch: null };
  }
  if (
    normalized === "2" ||
    normalized === "host" ||
    normalized === "machine" ||
    normalized === "admin" ||
    normalized === "ops" ||
    normalized === "machine-admin" ||
    normalized === "machine-administration"
  ) {
    return { target: "host", baseBranch: null };
  }

  const parts = trimmed.split(/\s+/).filter(Boolean);
  if (!parts.length) {
    return null;
  }

  const head = parts[0] || "";
  const second = parts[1];
  const third = parts[2];
  if ((head === "3" || head.toLowerCase() === "repo" || head.toLowerCase() === "path") && second) {
    return { target: second, baseBranch: third || null };
  }

  if (parts.length <= 2) {
    return { target: parts[0] || "", baseBranch: parts[1] || null };
  }

  return null;
}

function looksLikeProposalCurationSlug(slug: string): boolean {
  return /\b(?:proposal|proposals|curation|curator)\b/i.test(slug.replace(/[-_]+/g, " "));
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
  generationId: string;
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

interface ProposalShortcutSnapshot {
  targetKey: string;
  proposals: BrainProposalSummary[];
  createdAt: number;
}

interface PlainTextHandlingResult {
  accepted: boolean;
}

const CRON_SHORTCUT_TTL_MS = 15 * 60 * 1000;
const CRON_SHORTCUT_MAX_SNAPSHOTS = 50;
const ARTIFACT_SHORTCUT_TTL_MS = 15 * 60 * 1000;
const ARTIFACT_SHORTCUT_MAX_SNAPSHOTS = 50;
const PROPOSAL_SHORTCUT_TTL_MS = 15 * 60 * 1000;
const PROPOSAL_SHORTCUT_MAX_SNAPSHOTS = 50;
const TEXT_COALESCE_MAX_PARTS = 25;
const TEXT_COALESCE_MAX_CHARS = 120_000;
const TEXT_COALESCE_MAX_PENDING = 100;

function codexModeSummary(context: ContextRecord): string {
  return formatCodexModeSummary(context);
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

function detailValue(details: string | null, key: string): string | null {
  if (!details) {
    return null;
  }

  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return details.match(new RegExp(`^${escaped}=([^\\n]*)$`, "m"))?.[1]?.trim() || null;
}

function formatUpdateCheckCommandResult(result: {
  ok: boolean;
  stdout: string;
  stderr: string;
  exitCode: number;
  reportPath: string | null;
}): string {
  const body = result.stdout.replace(/^BRAINSTACK_UPDATE_REPORT=.*$/m, "").trim();
  const combined = body || result.stderr.trim() || `update-check exited ${result.exitCode}`;
  const compactBody = combined.length > 3200 ? `${combined.slice(0, 3200)}\n\n... truncated; see artifact.` : combined;
  const heading = !result.ok ? "Update check failed." : body.includes("- status: degraded") ? "Update check degraded." : "Update check complete.";
  return [heading, result.reportPath ? `Artifact: ${result.reportPath}` : null, compactBody].filter(Boolean).join("\n\n");
}

export class CommandHandler {
  private readonly pendingText = new Map<string, PendingTextPrompt>();
  private readonly pendingNewContexts = new Map<string, PendingNewContextWizard>();
  private readonly flushingPendingTextKeys = new Set<string>();
  private readonly cronShortcutSnapshots = new Map<string, CronShortcutSnapshot>();
  private readonly artifactShortcutSnapshots = new Map<string, ArtifactShortcutSnapshot>();
  private readonly proposalShortcutSnapshots = new Map<string, ProposalShortcutSnapshot>();

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
    let telegramInput = rawTelegramInput ? filterPhaseOneTelegramInput(rawTelegramInput) : null;

    if (!text && !rawTelegramInput?.attachments.length) {
      return;
    }

    const target = messageTarget(message);
    const parsed = parseCommand(text);
    if (parsed?.mention && this.config.telegramBotUsername && parsed.mention !== this.config.telegramBotUsername) {
      return;
    }

    // Authorization must gate before any processing (pending-text flush, context
    // lookup, command handling) so unauthorized senders cannot trigger side effects
    // or learn operator/control metadata.
    const allowed = message.from?.id === this.config.allowedTelegramUserId;
    if (!allowed) {
      if (parsed?.command === "/whoami") {
        // Discovery aid: echo only the caller's own identity, never configured ids,
        // chat topology, or bound context state.
        await this.telegram.sendText(target, [`Access: denied`, `From user id: ${message.from?.id ?? "unknown"}`].join("\n"));
        return;
      }
      console.warn("ignoring telegram message from unauthorized user", message.from?.id);
      return;
    }

    const hasAttachments = Boolean(rawTelegramInput?.attachments.length);
    if (parsed || hasAttachments) {
      await this.flushPendingText(target, message.from?.id ?? null);
    }
    const boundContext = this.contexts.getContextByTopic(target.chatId, target.threadId);
    if (!parsed && !hasAttachments && text.trim()) {
      const wizard = this.getPendingNewContextWizard(target, message.from?.id ?? null);
      if (wizard) {
        await this.handleNewContextWizardReply(wizard, text, target, boundContext);
        return;
      }
    }

    const voiceIntent =
      parsed?.command === "/voice" || parsed?.command === "/transcription"
        ? parseVoiceCapabilityCommand(parsed.rest)
        : !parsed && !hasAttachments
          ? parseNaturalVoiceCapabilityIntent(text)
          : null;
    if (voiceIntent) {
      await this.handleVoiceCapabilityIntent(voiceIntent, target);
      return;
    }

    if (parsed?.command === "/whoami") {
      await this.telegram.sendText(
        target,
        [
          `Access: allowed`,
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

        let effectiveText = text;
        if (rawTelegramInput && hasAudioTelegramInput(rawTelegramInput)) {
          const prepared = await this.transcribeTelegramInputForDispatch(rawTelegramInput, text, target);
          if (!prepared) {
            return;
          }
          effectiveText = prepared.text;
          telegramInput = prepared.telegramInput;
        }

        const hasNonAudioAttachments = Boolean(telegramInput?.attachments.length);
        if (!hasNonAudioAttachments && this.config.textCoalesceMs > 0) {
          this.enqueuePendingText(boundContext, effectiveText, target, message.from?.id ?? null);
          return;
        }

        await this.handleBoundPlainText(boundContext, effectiveText, target, telegramInput, message.from?.id ?? null);
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

      const proposalShortcut = parsed.command.match(/^\/proposal_(accept|reject)_([a-z0-9]{6,10})_(\d+)$/);
      if (proposalShortcut) {
        const proposal = this.resolveProposalSnapshotShortcut(proposalShortcut[2] || "", proposalShortcut[3] || "", target);
        if (!proposal) {
          await this.telegram.sendText(target, "That proposal shortcut expired or the list changed. Use /proposals to refresh.");
          return;
        }
        await this.handleProposalDecision(proposalShortcut[1] === "reject" ? "reject" : "accept", proposal, target, message.from?.id ?? null);
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
              "/voice",
              "/context",
              "/compact",
              "/crons",
              "/cron <subcommand>",
              "/cron_run <id|label>",
              "/curator_status",
              "/curator_run",
              "/proposals",
              "/curation",
              "/mode [fast|normal|max|clear]",
              "/model [model-id|clear]",
              "/effort [low|medium|high|xhigh|clear]",
              "/newctx [slug] [machine] [target] [base-branch]",
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
              "Use /context or /usage to inspect the current topic state. Use /compact to compact Codex topics when supported.",
              "Use /voice install <machine> to install local voice transcription for Telegram voice notes.",
              "Use /curation in a proposal-review topic to bind it as Brainstack's proposal curation surface.",
              "Use /crons to inspect scheduled jobs linked to this topic/context. Use /cron install update-check, brain-curator, or daily-checkin to add built-in routines.",
              "Use /cron_run <id|label> or the shortcuts from /crons to test a scheduled job immediately."
            ].join("\n")
          );
          return;

        case "/explainctx": {
          if (!boundContext) {
            const candidate = topicSlugCandidate(message);
            await this.telegram.sendText(
              target,
              [
                "This topic is not bound yet.",
                "A Brainstack context is the durable workspace and session binding for one Telegram topic.",
                "After binding, plain text here starts or resumes work in that workspace; old Telegram messages stay in Telegram and are not automatically imported.",
                "",
                `Suggested slug: ${candidate.slug}`,
                `Slug source: ${candidate.source}`,
                looksLikeProposalCurationSlug(candidate.slug)
                  ? "This looks like a proposal-curation topic. Run /curation to bind it automatically."
                  : "Run /newctx to be guided through the missing values.",
                `Or use the full command: /newctx ${candidate.slug} <machine> scratch`,
                "",
                "Known machines:",
                ...formatMachineChoices(this.workers.knownHosts())
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
                  .map((worker) => {
                    const runtime = this.workers.describeWorkerRuntime(worker.host, boundContext || undefined);
                    const probedModel = detailValue(worker.details, "model") || "default";
                    const probedEffort = detailValue(worker.details, "effort") || "default";
                    const topicApplies = boundContext?.machine === worker.host;
                    return [
                      worker.host,
                      `status=${worker.status}`,
                      `transport=${worker.transport || "n/a"}`,
                      `local=${worker.localExecution ? "yes" : "no"}`,
                      `harness=${runtime?.harness || detailValue(worker.details, "harness") || "n/a"}`,
                      `model=${topicApplies && boundContext?.modelOverride ? boundContext.modelOverride : probedModel}`,
                      `effort=${topicApplies && boundContext?.reasoningEffortOverride ? boundContext.reasoningEffortOverride : probedEffort}`,
                      `sudo=${detailValue(worker.details, "sudo") || "n/a"}`,
                      worker.lastSeenAt ? `last_seen=${worker.lastSeenAt}` : null,
                      worker.lastCheckedAt ? `checked=${worker.lastCheckedAt}` : null,
                      worker.lastError ? `error=${compact(worker.lastError, 140)}` : null
                    ]
                      .filter(Boolean)
                      .join(" | ");
                  })
                  .join("\n")
              : "No workers configured."
          );
          return;
        }

        case "/updates": {
          const routinesContext = this.db.getContextBySlug("brainstack-routines");
          const updateContext =
            routinesContext?.state === "active" ? routinesContext : boundContext?.state === "active" ? boundContext : null;
          if (!updateContext) {
            const knownContexts = [routinesContext, boundContext]
              .filter((context): context is ContextRecord => Boolean(context))
              .map((context) => `${context.slug}:${context.state}`)
              .join(", ");
            await this.telegram.sendText(
              target,
              [
                "No active context is available for update-check artifacts.",
                knownContexts ? `Known update contexts: ${knownContexts}` : null,
                "Use /newctx or install the update-check routine first."
              ]
                .filter(Boolean)
                .join("\n")
            );
            return;
          }

          const logPath = `${this.config.logsDir}/manual-update-check-${Date.now()}.log`;
          let result;
          try {
            result = await this.dispatcher.withContextLock(updateContext, () => this.workers.runUpdateCheck(updateContext, logPath), {
              mode: "update-check",
              logPath,
              target
            });
          } catch (error) {
            await this.telegram.sendText(target, `Update check could not start: ${error instanceof Error ? error.message : String(error)}`);
            return;
          }
          await this.telegram.sendText(target, formatUpdateCheckCommandResult(result));
          return;
        }

        case "/context": {
          if (!boundContext) {
            await this.telegram.sendText(target, "This topic is not bound.");
            return;
          }

          await this.telegram.sendText(target, this.formatContextStatus(boundContext));
          return;
        }

        case "/compact": {
          if (!boundContext) {
            await this.telegram.sendText(target, "This topic is not bound.");
            return;
          }
          if (boundContext.state === "archived") {
            await this.telegram.sendText(target, `${boundContext.slug} is archived. Use /bind or /newctx first.`);
            return;
          }
          if (this.dispatcher.isActive(boundContext.slug)) {
            await this.telegram.sendText(target, `${boundContext.slug} already has an active job. Use /topicinfo or /tail.`);
            return;
          }

          const runtime = this.workers.describeWorkerRuntime(boundContext.machine, boundContext);
          if (runtime?.harness !== "codex") {
            await this.telegram.sendText(target, "Claude has no manual compact support.");
            return;
          }
          if (!boundContext.codexSessionId) {
            await this.telegram.sendText(target, "No Codex session exists for this context yet.");
            return;
          }

          await this.telegram.sendText(target, "Compacting thread…");
          const response = await this.dispatcher.dispatch("resume", boundContext, "/compact", target, {
            notifyAccepted: false,
            notifyCompaction: false,
            rawPrompt: true,
            sourceLabel: "manual compact",
            userId: message.from?.id ?? null
          });
          if (!response.accepted && response.message) {
            await this.telegram.sendText(target, response.message);
          }
          return;
        }

        case "/crons": {
          await this.telegram.sendText(target, this.formatCronOverview(boundContext, target));
          return;
        }

        case "/curator_status": {
          await this.telegram.sendText(target, await this.formatCuratorStatus(target));
          return;
        }

        case "/curator_run": {
          const job = this.findCuratorJob();
          if (!job) {
            await this.telegram.sendText(
              target,
              "No brain-curator job is installed. Use /curation in the proposal-review topic, /cron_install_brain_curator, or set FACTORY_TELEGRAM_CONTROL_CHAT_ID so it installs automatically."
            );
            return;
          }
          await this.handleCronJobAction("run", job, target);
          return;
        }

        case "/curation":
        case "/curation_setup":
        case "/proposal_curation": {
          await this.setupProposalCurationTopic(parsed.rest, target, boundContext);
          return;
        }

        case "/proposals": {
          await this.telegram.sendText(target, await this.formatProposalList(target, parsed.rest));
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
          if (!parts.length) {
            await this.startNewContextWizard(message, target, boundContext);
            return;
          }

          if (parts.length < 3) {
            const key = this.pendingNewContextKey(target, message.from?.id ?? null);
            const maybeMachine = parts.length === 2 ? this.resolveNewContextMachine(parts[1] || "") : null;
            const slug = slugifyPhrase(maybeMachine ? (parts[0] || "") : parsed.rest, topicSlugCandidate(message).slug);
            const wizard: PendingNewContextWizard = {
              targetKey: key,
              slug,
              machine: maybeMachine,
              step: maybeMachine ? "target" : "machine",
              createdAt: Date.now()
            };
            this.pendingNewContexts.set(key, wizard);
            await this.telegram.sendText(target, maybeMachine ? this.promptNewContextTarget(wizard) : this.promptNewContextMachine(wizard));
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
          let instruction = parsed.rest;
          let commandTelegramInput = telegramInput;
          if (rawTelegramInput && hasAudioTelegramInput(rawTelegramInput)) {
            const prepared = await this.transcribeTelegramInputForDispatch(rawTelegramInput, parsed.rest, target);
            if (!prepared) {
              return;
            }
            instruction = prepared.text;
            commandTelegramInput = prepared.telegramInput;
          }

          const response = await this.dispatcher.dispatch(mode, boundContext, instruction, target, {
            telegramInput: commandTelegramInput,
            userId: message.from?.id ?? null
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
          const runtime = this.workers.describeWorkerRuntime(boundContext.machine, boundContext);
          const compactionStatus =
            runtime?.harness === "codex"
              ? boundContext.codexSessionId
                ? "/compact available"
                : "available after the first Codex session"
              : "manual compact unavailable for this harness";
          await this.telegram.sendText(
            target,
            [
              this.formatContextStatus(boundContext),
              "",
              "Usage:",
              usage.text,
              "",
              `Compaction: ${compactionStatus}`
            ].join("\n")
          );
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

  private async handleVoiceCapabilityIntent(intent: VoiceCapabilityIntent, target: TelegramTarget): Promise<void> {
    if (intent.help) {
      await this.telegram.sendText(target, voiceCapabilityUsage());
      return;
    }
    if (intent.error) {
      await this.telegram.sendText(target, `${intent.error}\n\n${voiceCapabilityUsage()}`);
      return;
    }

    const baseArgs = ["capabilities", intent.action, "voice", "--config", this.config.brainstackConfigPath];
    if (intent.action === "install") {
      if (!intent.target) {
        await this.telegram.sendText(target, `Install needs a target machine.\n\n${voiceCapabilityUsage()}`);
        return;
      }
      baseArgs.push("--target", intent.target, "--restart-delay-ms", "1500");
      if (intent.model) {
        baseArgs.push("--model", intent.model);
      }
      await this.telegram.sendText(
        target,
        [
          `Installing voice transcription on ${intent.target}.`,
          "I will check requirements, install the processor, update Brainstack config, and reload Telemux when done."
        ].join("\n")
      );
    }

    if (intent.action === "doctor") {
      await this.telegram.sendText(target, "Checking voice transcription status.");
    }

    let progressTimer: ReturnType<typeof setInterval> | null = null;
    if (intent.action === "install" && intent.target) {
      progressTimer = this.startCapabilityProgressMessages(target, intent.target);
    }

    let result: { ok: boolean; exitCode: number | null; timedOut: boolean; stdout: string; stderr: string };
    try {
      result = await this.runBrainctlCapability(baseArgs, intent.action === "install" ? 40 * 60_000 : 90_000);
    } finally {
      if (progressTimer) {
        clearInterval(progressTimer);
      }
    }
    const output = [result.stdout.trim(), result.stderr.trim()].filter(Boolean).join("\n");
    const trimmed = output.length > 3200 ? `${output.slice(0, 3200)}\n... truncated` : output;
    if (!result.ok) {
      await this.telegram.sendText(
        target,
        [`Voice ${intent.action} failed${result.timedOut ? " (timed out)" : ` (exit ${result.exitCode})`}.`, trimmed || "No output."].join(
          "\n\n"
        )
      );
      return;
    }

    const suffix =
      intent.action === "install"
        ? "Test it by sending a Telegram voice note in any bound Brainstack topic. If Telemux was reloaded, wait a few seconds first."
        : null;
    await this.telegram.sendText(target, [`Voice ${intent.action} complete.`, trimmed || "ok", suffix].filter(Boolean).join("\n\n"));
  }

  private startCapabilityProgressMessages(target: TelegramTarget, machine: string): ReturnType<typeof setInterval> | null {
    const intervalMs = this.config.capabilityProgressIntervalMs;
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      return null;
    }

    const startedAt = Date.now();
    let count = 0;
    const timer = setInterval(() => {
      count += 1;
      const elapsed = formatElapsed(Date.now() - startedAt);
      const extra =
        count === 1
          ? "First install can take several minutes while the model downloads and verifies."
          : "Still waiting on the installer; I will post the final result here.";
      void this.telegram
        .sendText(target, [`Still installing voice transcription on ${machine} (${elapsed}).`, extra].join("\n"))
        .catch((error) => {
          console.warn("failed to send voice capability progress message", error);
        });
    }, intervalMs);
    timer.unref?.();
    return timer;
  }

  private async runBrainctlCapability(
    args: string[],
    timeoutMs: number
  ): Promise<{ ok: boolean; exitCode: number | null; timedOut: boolean; stdout: string; stderr: string }> {
    let proc: ReturnType<typeof Bun.spawn>;
    try {
      proc = Bun.spawn([this.config.brainctlBin, ...args], {
        stdout: "pipe",
        stderr: "pipe",
        stdin: "ignore"
      });
    } catch (error) {
      return {
        ok: false,
        exitCode: 127,
        timedOut: false,
        stdout: "",
        stderr: error instanceof Error ? error.message : String(error)
      };
    }

    const stdoutPromise = new Response(proc.stdout).text();
    const stderrPromise = new Response(proc.stderr).text();
    let timedOut = false;
    let timeout: ReturnType<typeof setTimeout> | null = null;
    const timeoutPromise = new Promise<number | null>((resolve) => {
      timeout = setTimeout(() => {
        timedOut = true;
        proc.kill();
        resolve(124);
      }, timeoutMs);
    });
    const exitCode = await Promise.race([proc.exited, timeoutPromise]);
    if (timeout) {
      clearTimeout(timeout);
    }
    const [stdout, stderr] = await Promise.all([stdoutPromise, stderrPromise]);
    return {
      ok: !timedOut && exitCode === 0,
      exitCode,
      timedOut,
      stdout,
      stderr
    };
  }

  private pendingKey(target: TelegramTarget, userId: number | null): string {
    return `${target.chatId}:${target.threadId ?? "none"}:${userId ?? "unknown"}`;
  }

  private pendingTextTimer(target: TelegramTarget, userId: number | null, delayMs = this.config.textCoalesceMs): ReturnType<typeof setTimeout> {
    return setTimeout(() => void this.flushPendingText(target, userId).catch((error) => this.reportPendingTextFlushError(target, error)), delayMs);
  }

  private pendingTextGenerationFor(stored: PendingTextRecord): string {
    if (stored.generationId) {
      return stored.generationId;
    }
    const generationId = randomUUID();
    return this.db.assignPendingTextGenerationIfBlank(stored.key, generationId) || generationId;
  }

  private restorePendingTextForRetry(pending: PendingTextPrompt, flushedPartsJson: string): "restored" | "preserved-newer" {
    const rawStored = this.db.getPendingText(pending.key);
    const stored = rawStored
      ? {
          ...rawStored,
          generationId: this.pendingTextGenerationFor(rawStored)
        }
      : null;
    if (!stored) {
      pending.timer = this.pendingTextTimer(pending.target, pending.userId);
      this.db.upsertPendingText({
        key: pending.key,
        contextSlug: pending.contextSlug,
        chatId: pending.target.chatId,
        threadId: pending.target.threadId,
        userId: pending.userId,
        partsJson: flushedPartsJson,
        generationId: pending.generationId
      });
      this.pendingText.set(pending.key, pending);
      return "restored";
    }
    if (stored.generationId === pending.generationId && stored.contextSlug === pending.contextSlug) {
      pending.timer = this.pendingTextTimer(pending.target, pending.userId);
      this.pendingText.set(pending.key, pending);
      return "restored";
    }

    let storedParts: string[] = [];
    try {
      const parsed = JSON.parse(stored.partsJson) as unknown;
      storedParts = Array.isArray(parsed) ? parsed.map(String).filter(Boolean) : [];
    } catch {
      storedParts = [];
    }

    const currentPending = this.pendingText.get(pending.key);
    if (stored.contextSlug !== pending.contextSlug) {
      if (!currentPending) {
        this.pendingText.set(pending.key, {
          key: stored.key,
          target: { chatId: stored.chatId, threadId: stored.threadId },
          userId: stored.userId,
          contextSlug: stored.contextSlug,
          parts: storedParts,
          generationId: stored.generationId,
          createdAt: Date.parse(stored.createdAt) || Date.now(),
          timer: this.pendingTextTimer({ chatId: stored.chatId, threadId: stored.threadId }, stored.userId)
        });
      }
      return "preserved-newer";
    }
    if (currentPending) {
      clearTimeout(currentPending.timer);
    }
    const mergedParts = [...pending.parts, ...storedParts];
    const restored: PendingTextPrompt = {
      ...pending,
      parts: mergedParts,
      generationId: randomUUID(),
      createdAt: Math.min(pending.createdAt, Date.parse(stored.createdAt) || pending.createdAt),
      timer: this.pendingTextTimer(pending.target, pending.userId)
    };
    this.db.upsertPendingText({
      key: restored.key,
      contextSlug: restored.contextSlug,
      chatId: restored.target.chatId,
      threadId: restored.target.threadId,
      userId: restored.userId,
      partsJson: JSON.stringify(restored.parts),
      generationId: restored.generationId
    });
    this.pendingText.set(restored.key, restored);
    return "restored";
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
        existing.generationId = randomUUID();
        this.db.upsertPendingText({
          key,
          contextSlug: existing.contextSlug,
          chatId: existing.target.chatId,
          threadId: existing.target.threadId,
          userId: existing.userId,
          partsJson: JSON.stringify(existing.parts),
          generationId: existing.generationId
        });
        existing.timer = setTimeout(() => void this.flushPendingText(target, userId).catch((error) => this.reportPendingTextFlushError(target, error)), this.config.textCoalesceMs);
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
      generationId: randomUUID(),
      createdAt: Date.now(),
      timer: setTimeout(() => void this.flushPendingText(target, userId).catch((error) => this.reportPendingTextFlushError(target, error)), this.config.textCoalesceMs)
    };
    this.pendingText.set(key, pending);
    this.db.upsertPendingText({
      key,
      contextSlug: context.slug,
      chatId: target.chatId,
      threadId: target.threadId,
      userId,
      partsJson: JSON.stringify(pending.parts),
      generationId: pending.generationId
    });
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
    if (this.flushingPendingTextKeys.has(key)) {
      return;
    }
    this.flushingPendingTextKeys.add(key);
    try {
      let pending = this.pendingText.get(key);
      if (!pending) {
        const stored = this.db.getPendingText(key);
        if (stored) {
          const generationId = this.pendingTextGenerationFor(stored);
          let parts: string[] = [];
          try {
            const parsed = JSON.parse(stored.partsJson) as unknown;
            parts = Array.isArray(parsed) ? parsed.map(String).filter(Boolean) : [];
          } catch {
            parts = [];
          }
          pending = {
            key,
            target: { chatId: stored.chatId, threadId: stored.threadId },
            userId: stored.userId,
            contextSlug: stored.contextSlug,
            parts,
            generationId,
            createdAt: Date.parse(stored.createdAt) || Date.now(),
            timer: setTimeout(() => undefined, 0)
          };
        }
      }
      if (!pending) {
        return;
      }
      clearTimeout(pending.timer);
      this.pendingText.delete(key);
      const flushedPartsJson = JSON.stringify(pending.parts);
      const flushedGenerationId = pending.generationId;
      const context = this.contexts.getContextBySlug(pending.contextSlug);
      if (!context || context.state === "archived") {
        this.db.deletePendingTextIfGenerationMatch(key, flushedGenerationId);
        await this.telegram.sendText(
          pending.target,
          `${pending.contextSlug} is archived or unavailable; your pending message was not dispatched. Rebind or create a new context, then resend it.`
        );
        return;
      }
      const prompt = pending.parts.join("\n\n");
      if (pending.parts.length > 1) {
        console.log(`coalesced ${pending.parts.length} Telegram text messages for ${context.slug}`);
      }
      try {
        const result = await this.handleBoundPlainText(context, prompt, pending.target, null, pending.userId);
        if (result.accepted) {
          this.db.deletePendingTextIfGenerationMatch(key, flushedGenerationId);
        } else {
          const restoreStatus = this.restorePendingTextForRetry(pending, flushedPartsJson);
          if (restoreStatus === "preserved-newer") {
            await this.telegram.sendText(
              pending.target,
              "Pending Telegram text was not dispatched; newer text for another context remains queued, so please resend the older text."
            );
          }
        }
      } catch (error) {
        const restoreStatus = this.restorePendingTextForRetry(pending, flushedPartsJson);
        await this.reportPendingTextFlushError(pending.target, error, restoreStatus === "restored");
      }
    } finally {
      this.flushingPendingTextKeys.delete(key);
      const deferred = this.pendingText.get(key);
      if (deferred) {
        clearTimeout(deferred.timer);
        deferred.timer = this.pendingTextTimer(deferred.target, deferred.userId);
      }
    }
  }

  private async handleBoundPlainText(
    context: ContextRecord,
    text: string,
    target: TelegramTarget,
    telegramInput: TelegramInboundMessageInput | null,
    userId: number | null
  ): Promise<PlainTextHandlingResult> {
    const hasAttachments = Boolean(telegramInput?.attachments.length);

    if (!hasAttachments && (await this.maybeSendArtifactsFromPlainText(context, text, target))) {
      return { accepted: true };
    }

    const freshContext = this.db.getContextBySlug(context.slug) || context;
    const preDispatch = await classifyPreDispatch({
      text,
      context: freshContext,
      hasAttachments,
      classifier: this.config.preDispatchClassifier
    });
    this.logPreDispatchRoute(freshContext, preDispatch);

    if (preDispatch.route === "control_meta") {
      const controlKind = resolveControlMetaKind(preDispatch);
      await this.telegram.sendText(
        target,
        await formatControlMetaResponse({
          context: freshContext,
          classification: preDispatch,
          busy: this.dispatcher.isActive(freshContext.slug),
          runtime: controlKind === "liveness" ? this.workers.describeWorkerRuntime(freshContext.machine, freshContext) : null,
          contextStatus: controlKind === "status" ? this.formatContextStatus(freshContext) : ""
        })
      );
      return { accepted: true };
    }

    const promptProfile = preDispatch.route === "light_harness" ? "light" : "full";
    const response = await this.dispatcher.dispatch("resume", freshContext, text, target, {
      telegramInput,
      userId,
      promptProfile,
      sourceLabel: promptProfile === "light" ? "pre-dispatch light" : null
    });
    if (response.message) {
      await this.telegram.sendText(target, response.message);
    }

    return { accepted: response.accepted };
  }

  private async transcribeTelegramInputForDispatch(
    input: TelegramInboundMessageInput,
    text: string,
    target: TelegramTarget
  ): Promise<{ text: string; telegramInput: TelegramInboundMessageInput | null } | null> {
    let result: Awaited<ReturnType<typeof transcribeTelegramAudioInput>>;
    try {
      result = await transcribeTelegramAudioInput({
        input,
        config: this.config,
        telegram: this.telegram,
        workers: this.workers
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await this.telegram.sendText(target, `Voice transcription failed: ${message}`);
      return null;
    }

    if (!result.ok) {
      await this.telegram.sendText(target, result.message);
      return null;
    }

    const mergedText = mergeTelegramTextAndTranscript(text, result.transcript);
    if (!mergedText) {
      await this.telegram.sendText(target, "Voice transcription produced no text.");
      return null;
    }

    if (this.config.transcription.echoTranscript) {
      await this.telegram.sendText(target, formatTranscriptEcho(result.transcript));
    }

    return {
      text: mergedText,
      telegramInput: withoutAudioTelegramInput(input, mergedText)
    };
  }

  private logPreDispatchRoute(context: ContextRecord, classification: Awaited<ReturnType<typeof classifyPreDispatch>>): void {
    const safeReason = classification.source === "llm" ? "llm-classifier" : classification.reason;
    console.log(
      [
        "pre-dispatch route",
        `context=${context.slug}`,
        `route=${classification.route}`,
        `kind=${classification.controlKind || "n/a"}`,
        `source=${classification.source}`,
        `confidence=${classification.confidence.toFixed(2)}`,
        `reason=${safeReason.replace(/\s+/g, "-")}`
      ].join(" ")
    );
  }

  private async reportPendingTextFlushError(target: TelegramTarget, error: unknown, restored = true): Promise<void> {
    const message = error instanceof Error ? error.message : String(error);
    console.error("pending Telegram text flush failed", error);
    const status = restored
      ? "Pending Telegram text was not dispatched and remains queued for retry"
      : "Pending Telegram text was not dispatched; newer text for another context remains queued, so please resend the older text";
    await this.telegram.sendText(target, `${status}: ${message}`);
  }

  recoverPendingText(): void {
    for (const stored of this.db.listPendingText()) {
      if (this.pendingText.has(stored.key)) {
        continue;
      }
      const generationId = this.pendingTextGenerationFor(stored);
      let parts: string[] = [];
      try {
        const parsed = JSON.parse(stored.partsJson) as unknown;
        parts = Array.isArray(parsed) ? parsed.map(String).filter(Boolean) : [];
      } catch {
        this.db.deletePendingText(stored.key);
        continue;
      }
      if (!parts.length) {
        this.db.deletePendingText(stored.key);
        continue;
      }
      const createdAt = Date.parse(stored.createdAt) || Date.now();
      if (Date.now() - createdAt > this.config.pendingTextRecoveryMaxAgeMs) {
        this.db.deletePendingText(stored.key);
        void this.telegram.sendText(
          { chatId: stored.chatId, threadId: stored.threadId },
          "Pending Telegram text from before restart was too old to auto-dispatch. Please resend it."
        );
        continue;
      }
      const target = { chatId: stored.chatId, threadId: stored.threadId };
      this.pendingText.set(stored.key, {
        key: stored.key,
        target,
        userId: stored.userId,
        contextSlug: stored.contextSlug,
        parts,
        generationId,
        createdAt,
        timer: setTimeout(() => void this.flushPendingText(target, stored.userId).catch((error) => this.reportPendingTextFlushError(target, error)), Math.max(1, this.config.textCoalesceMs))
      });
    }
  }

  private pendingNewContextKey(target: TelegramTarget, userId: number | null): string {
    return `${telegramTargetKey(target)}:${userId ?? "unknown"}`;
  }

  private getPendingNewContextWizard(target: TelegramTarget, userId: number | null): PendingNewContextWizard | null {
    const key = this.pendingNewContextKey(target, userId);
    const wizard = this.pendingNewContexts.get(key);
    if (!wizard) {
      return null;
    }

    if (Date.now() - wizard.createdAt > NEW_CONTEXT_WIZARD_MAX_AGE_MS) {
      this.pendingNewContexts.delete(key);
      return null;
    }

    return wizard;
  }

  private async startNewContextWizard(message: TelegramMessage, target: TelegramTarget, boundContext: ContextRecord | null): Promise<void> {
    const candidate = topicSlugCandidate(message);
    const key = this.pendingNewContextKey(target, message.from?.id ?? null);
    this.pendingNewContexts.set(key, {
      targetKey: key,
      slug: candidate.slug,
      machine: null,
      step: "slug-or-machine",
      createdAt: Date.now()
    });
    await this.telegram.sendText(target, this.formatNewContextWizardStart(candidate.slug, candidate.source, boundContext));
  }

  private promptNewContextMachine(wizard: PendingNewContextWizard): string {
    return [
      `Slug: ${wizard.slug}`,
      "Pick a machine to bind this topic to:",
      ...formatMachineChoices(this.workers.knownHosts()),
      "",
      "Reply with the number or machine name.",
      "Reply cancel to stop."
    ].join("\n");
  }

  private promptNewContextTarget(wizard: PendingNewContextWizard): string {
    const curationHint = looksLikeProposalCurationSlug(wizard.slug)
      ? ["", "This looks like proposal curation. Recommended: reply 1, or run /curation for the dedicated setup."]
      : [];
    return [
      `Slug: ${wizard.slug}`,
      `Machine: ${wizard.machine || "unselected"}`,
      "Pick what this topic is for:",
      "1) Topic workspace - ongoing conversation, routines, proposal review; stores a durable scratch workspace and defaults to low thinking",
      "2) Machine administration - inspect or operate the selected machine; stores a durable host workspace",
      "3) Code repository/path - reply with `repo <git-url-or-path> [base-branch]`",
      ...curationHint,
      "",
      `Recommended next command: /newctx ${wizard.slug} ${wizard.machine || "<machine>"} scratch`,
      "Reply cancel to stop."
    ].join("\n");
  }

  private formatNewContextWizardStart(slug: string, source: string, boundContext: ContextRecord | null): string {
    return [
      boundContext ? this.formatRebindWarning(boundContext) : null,
      [
        "Let's bind this Telegram topic to a Brainstack context.",
        "A context is the durable workspace and session binding for one topic. After it is bound, plain text here starts or resumes work in that workspace.",
        "Old Telegram messages stay in Telegram and are not automatically imported into the new context.",
        "",
        `Suggested slug: ${slug}`,
        `Slug source: ${source}`,
        "To change it, reply with a word or phrase now. To keep it, pick a machine:",
        ...formatMachineChoices(this.workers.knownHosts()),
        "",
        `Full command option: /newctx ${slug} <machine> scratch`,
        "Reply cancel to stop."
      ].join("\n")
    ]
      .filter(Boolean)
      .join("\n\n");
  }

  private resolveNewContextMachine(input: string): string | null {
    const hosts = this.workers.knownHosts();
    const trimmed = input.trim();
    const index = Number(trimmed);
    if (Number.isInteger(index) && index >= 1 && index <= hosts.length) {
      return hosts[index - 1] || null;
    }

    const normalized = trimmed.toLowerCase();
    return hosts.find((host) => host.toLowerCase() === normalized) || null;
  }

  private async handleNewContextWizardReply(
    wizard: PendingNewContextWizard,
    text: string,
    target: TelegramTarget,
    boundContext: ContextRecord | null
  ): Promise<void> {
    const trimmed = text.trim();
    if (/^(cancel|stop|never mind|nevermind)$/i.test(trimmed)) {
      this.pendingNewContexts.delete(wizard.targetKey);
      await this.telegram.sendText(target, "Context binding cancelled. Run /newctx to start again.");
      return;
    }

    if (wizard.step === "slug-or-machine") {
      const selectedMachine = this.resolveNewContextMachine(trimmed);
      if (selectedMachine) {
        wizard.machine = selectedMachine;
        wizard.step = "target";
        await this.telegram.sendText(target, this.promptNewContextTarget(wizard));
        return;
      }

      wizard.slug = slugifyPhrase(trimmed, wizard.slug);
      wizard.step = "machine";
      await this.telegram.sendText(target, this.promptNewContextMachine(wizard));
      return;
    }

    if (wizard.step === "machine") {
      const selectedMachine = this.resolveNewContextMachine(trimmed);
      if (!selectedMachine) {
        await this.telegram.sendText(target, [`Unknown machine: ${trimmed}`, "", this.promptNewContextMachine(wizard)].join("\n"));
        return;
      }

      wizard.machine = selectedMachine;
      wizard.step = "target";
      await this.telegram.sendText(target, this.promptNewContextTarget(wizard));
      return;
    }

    const selectedTarget = parseNewContextTargetReply(trimmed);
    if (!selectedTarget?.target || !wizard.machine) {
      await this.telegram.sendText(target, ["I could not parse that target choice.", "", this.promptNewContextTarget(wizard)].join("\n"));
      return;
    }

    this.pendingNewContexts.delete(wizard.targetKey);
    const bound = await this.createOrRebindContext(wizard.slug, wizard.machine, selectedTarget.target, selectedTarget.baseBranch, target);
    const warning = boundContext ? this.formatRebindWarning(boundContext) : null;
    await this.telegram.sendText(target, [warning, this.formatContextCreated(bound)].filter(Boolean).join("\n\n"));
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

  private configuredMachineNames(): string[] {
    return this.workers.knownHosts().filter((host) => Boolean(this.workers.getWorkerConfig(host)));
  }

  private preferredProposalCurationMachine(explicitMachine: string | null): { machine: string | null; error: string | null } {
    const configured = this.configuredMachineNames();
    if (!configured.length) {
      return {
        machine: null,
        error: "No configured machines are available. Run /workers and fix worker config before setting up proposal curation."
      };
    }

    if (explicitMachine) {
      const match = configured.find((host) => host.toLowerCase() === explicitMachine.toLowerCase());
      if (!match) {
        return {
          machine: null,
          error: `Unknown machine: ${explicitMachine}\n\nKnown machines:\n${formatMachineChoices(configured).join("\n")}`
        };
      }
      return { machine: match, error: null };
    }

    const existingCurationContext = this.contexts.getContextBySlug(PROPOSAL_CURATION_CONTEXT_SLUG);
    if (existingCurationContext && configured.includes(existingCurationContext.machine)) {
      return { machine: existingCurationContext.machine, error: null };
    }

    const curatorJob = this.findCuratorJob();
    const curatorContext = curatorJob?.executionContextSlug ? this.contexts.getContextBySlug(curatorJob.executionContextSlug) : null;
    if (curatorContext && configured.includes(curatorContext.machine)) {
      return { machine: curatorContext.machine, error: null };
    }

    const routinesContext = this.contexts.getContextBySlug("brainstack-routines");
    if (routinesContext && configured.includes(routinesContext.machine)) {
      return { machine: routinesContext.machine, error: null };
    }

    if (this.config.localMachine && configured.includes(this.config.localMachine)) {
      return { machine: this.config.localMachine, error: null };
    }

    return { machine: configured[0] || null, error: null };
  }

  private async ensureBrainCuratorRoutineInContext(context: ContextRecord, target: TelegramTarget): Promise<string> {
    const routine = getBuiltinRoutine("brain-curator");
    if (!routine) {
      return "Brain-curator routine is not available in this build.";
    }

    const existing = this.findCuratorJob();
    const draft = builtinRoutineDraft(routine, existing?.schedule || defaultBuiltinSchedule(routine), context.slug);
    if (existing) {
      const updated = await this.cronManager.updateJob(existing, {
        schedule: existing.schedule,
        executionContextSlug: context.slug,
        targetChatId: target.chatId,
        targetThreadId: target.threadId,
        instruction: draft.instruction || null,
        reminderText: draft.reminderText || null,
        runner: draft.runner || null,
        enabled: true
      });
      return `Brain-curator routine ready: ${updated.id} (${updated.enabled ? "enabled" : "paused"}) next=${updated.nextRunAt || "none"}`;
    }

    const created = await this.cronManager.createJob(
      {
        ...draft,
        targetChatId: target.chatId,
        targetThreadId: target.threadId
      },
      { context, target }
    );
    return `Brain-curator routine installed: ${created.id} next=${created.nextRunAt || "none"}`;
  }

  private async setupProposalCurationTopic(rawRest: string, target: TelegramTarget, boundContext: ContextRecord | null): Promise<void> {
    const rest = rawRest.trim();
    if (/^(help|--help|-h)$/i.test(rest)) {
      await this.telegram.sendText(
        target,
        [
          "Usage: /curation [machine]",
          "",
          "Sets this Telegram topic up as Brainstack's proposal curation surface.",
          "It binds the topic to a durable proposal-curation scratch context and points the brain-curator routine at this topic.",
          "",
          "Use /proposals pending, /proposals needs-human needs-context, /curator_status, and /curator_run here."
        ].join("\n")
      );
      return;
    }

    const explicitMachine = rest ? rest.replace(/^machine[:=]/i, "").trim() : null;
    const resolved = this.preferredProposalCurationMachine(explicitMachine || null);
    if (!resolved.machine) {
      await this.telegram.sendText(target, resolved.error || "No machine could be selected for proposal curation.");
      return;
    }

    const bound = await this.createOrRebindContext(PROPOSAL_CURATION_CONTEXT_SLUG, resolved.machine, "scratch", null, target);
    const routineLine = await this.ensureBrainCuratorRoutineInContext(bound, target);
    const warning = boundContext && boundContext.slug !== bound.slug ? this.formatRebindWarning(boundContext) : null;
    await this.telegram.sendText(
      target,
      [
        warning,
        [
          "Proposal curation is ready in this topic.",
          `Context: ${bound.slug}`,
          `Machine: ${bound.machine}`,
          "Workspace: topic scratch",
          routineLine,
          "",
          "Use:",
          "/curator_status",
          "/curator_run",
          "/proposals pending",
          "/proposals needs-human needs-context",
          "/proposals open limit=10",
          "",
          "Accept/reject shortcuts from /proposals are scoped to the list you just loaded."
        ].join("\n")
      ]
        .filter(Boolean)
        .join("\n\n")
    );
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
    const runtime = this.workers.describeWorkerRuntime(context.machine, context);

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
      runtime ? `Harness: ${runtime.harness}` : null,
      runtime ? `Model: ${runtime.model}` : null,
      runtime ? `Thinking effort: ${runtime.effort}` : null,
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

  private findCuratorJob(): CronJobRecord | null {
    const jobs = this.cronManager
      .listJobs()
      .filter((job) => job.kind === "codex" && !job.runner && job.label.toLowerCase() === "brain-curator");
    return jobs.find((job) => job.executionContextSlug === "brainstack-routines") || jobs[0] || null;
  }

  private async formatCuratorStatus(target: TelegramTarget): Promise<string> {
    const job = this.findCuratorJob();
    const lines: string[] = [];
    lines.push(`Curator installed: ${job ? "yes" : "no"}`);
    if (job) {
      lines.push(`Job: ${job.id} (${job.enabled ? "enabled" : "paused"})`);
      lines.push(`Next run: ${job.nextRunAt || job.pendingRunAt || "n/a"}`);
      lines.push(`Last run: ${job.lastRunAt || "never"}`);
      if (job.lastError) {
        lines.push(`Last error: ${snippet(job.lastError)}`);
      }
    }
    const remote = await fetchCuratorStatus(this.config);
    if (remote) {
      const curator = (remote.curator || {}) as Record<string, unknown>;
      const counts = (remote.proposal_counts || {}) as Record<string, number>;
      lines.push(`Mode: ${String(remote.mode || "unknown")}`);
      lines.push(
        `Proposals: ${counts.pending || 0} pending, ${counts.approved || 0} approved, ${counts["needs-human"] || 0} needs-human, ${counts.applied || 0} applied`
      );
      if (curator.cursor) {
        lines.push(`Cursor: ${String(curator.cursor)}`);
      }
      const failures = Array.isArray(curator.last_run_failures) ? (curator.last_run_failures as string[]) : [];
      if (failures.length) {
        lines.push("Last run failures:");
        for (const failure of failures.slice(0, 5)) {
          lines.push(`- ${snippet(failure)}`);
        }
      }
    } else if (this.config.brainBaseUrl) {
      lines.push("Brain curator status is unreachable; is braind running?");
    } else {
      lines.push("BRAIN_BASE_URL is not configured; only local job state is shown.");
    }
    if (!this.config.brainAdminToken) {
      lines.push("Note: FACTORY_BRAIN_ADMIN_TOKEN is not set, so accept/reject from Telegram is disabled.");
    }
    return lines.join("\n");
  }

  private async formatProposalList(target: TelegramTarget, rawFilter: string): Promise<string> {
    const request = parseProposalListRequest(rawFilter);
    if (request.help) {
      return proposalListUsage();
    }
    if (request.error) {
      return `${request.error}\n\n${proposalListUsage()}`;
    }

    const proposals = await fetchProposals(this.config, { status: request.status });
    if (proposals === null) {
      return this.config.brainBaseUrl
        ? "Could not list proposals; is braind running?"
        : "BRAIN_BASE_URL is not configured; proposals live in the shared brain service.";
    }
    const filtered = proposals.filter((proposal) => matchesProposalListRequest(proposal, request));
    if (!filtered.length) {
      const criteria = [
        `status=${request.status}`,
        request.quality ? `quality=${request.quality}` : null,
        request.query ? `query="${request.query}"` : null
      ]
        .filter(Boolean)
        .join(" ");
      return `No proposals matched ${criteria}.\n\n${proposalListUsage()}`;
    }
    const limited = filtered.slice(0, request.limit);
    const token = this.createProposalShortcutSnapshot(limited, target);
    const label = [
      `status=${request.status}`,
      request.quality ? `quality=${request.quality}` : null,
      request.project ? `project=${request.project}` : null,
      request.scope ? `scope=${request.scope}` : null,
      request.group ? `group=${request.group}` : null,
      request.kind ? `kind=${request.kind}` : null,
      request.query ? `query="${request.query}"` : null
    ]
      .filter(Boolean)
      .join(", ");
    const lines = [`Proposals (${filtered.length}/${proposals.length}, ${label}):`];
    limited.forEach((proposal, index) => {
      const tags = [
        proposal.quality_decision ? `quality=${proposal.quality_decision}` : null,
        proposal.project ? `project=${proposal.project}` : null,
        proposal.scope ? `scope=${proposal.scope}` : null,
        proposal.memory_kind ? `kind=${proposal.memory_kind}` : null,
        proposal.legacy_format ? "legacy" : null,
        proposal.risk ? `risk=${proposal.risk}` : null
      ]
        .filter(Boolean)
        .join(", ");
      lines.push(
        `${index + 1}. [${proposal.status}] ${proposal.title}${proposal.target_page ? ` -> ${proposal.target_page}` : ""}${
          tags ? ` (${tags})` : ""
        }`
      );
      const acceptPart = proposal.target_page ? `/proposal_accept_${token}_${index + 1}` : "merge/enrich first";
      lines.push(`   ${acceptPart}  /proposal_reject_${token}_${index + 1}`);
    });
    if (filtered.length > limited.length) {
      lines.push(`Showing ${limited.length}; narrow the search or use limit=${Math.min(filtered.length, MAX_PROPOSAL_LIST_LIMIT)}.`);
    }
    if (!this.config.brainAdminToken) {
      lines.push("");
      lines.push("FACTORY_BRAIN_ADMIN_TOKEN is not set, so the shortcuts above will be refused. Use `brainctl proposals` on the control host.");
    } else {
      lines.push("");
      lines.push("Accept applies the proposed wiki change. Context-only candidates need merge/enrichment first.");
      lines.push("Filter with `/proposals pending`, `/proposals needs-human needs-context`, or `/proposals open project:lindy limit=10`.");
    }
    return lines.join("\n");
  }

  private createProposalShortcutSnapshot(proposals: BrainProposalSummary[], target: TelegramTarget): string {
    this.pruneProposalShortcutSnapshots();
    let token = "";
    do {
      token = Math.random().toString(36).slice(2, 8).padEnd(6, "0");
    } while (this.proposalShortcutSnapshots.has(token));
    this.proposalShortcutSnapshots.set(token, {
      targetKey: telegramTargetKey(target),
      proposals,
      createdAt: Date.now()
    });
    return token;
  }

  private resolveProposalSnapshotShortcut(token: string, selector: string, target: TelegramTarget): BrainProposalSummary | null {
    this.pruneProposalShortcutSnapshots();
    const snapshot = this.proposalShortcutSnapshots.get(token);
    if (!snapshot || snapshot.targetKey !== telegramTargetKey(target)) {
      return null;
    }
    const index = Number(selector) - 1;
    if (!Number.isInteger(index) || index < 0 || index >= snapshot.proposals.length) {
      return null;
    }
    return snapshot.proposals[index];
  }

  private pruneProposalShortcutSnapshots(): void {
    const now = Date.now();
    for (const [token, snapshot] of this.proposalShortcutSnapshots) {
      if (now - snapshot.createdAt > PROPOSAL_SHORTCUT_TTL_MS) {
        this.proposalShortcutSnapshots.delete(token);
      }
    }
    while (this.proposalShortcutSnapshots.size > PROPOSAL_SHORTCUT_MAX_SNAPSHOTS) {
      const oldest = [...this.proposalShortcutSnapshots.entries()].sort((a, b) => a[1].createdAt - b[1].createdAt)[0]?.[0];
      if (!oldest) {
        break;
      }
      this.proposalShortcutSnapshots.delete(oldest);
    }
  }

  private async handleProposalDecision(
    action: "accept" | "reject",
    proposal: BrainProposalSummary,
    target: TelegramTarget,
    userId: number | null
  ): Promise<void> {
    const decidedBy = `telegram:${userId ?? "unknown"}`;
    if (action === "accept" && !proposal.target_page) {
      await this.telegram.sendText(target, "This proposal has no wiki change attached yet. Merge its review group or enrich it before accepting.");
      return;
    }
    const effectiveAction = action === "accept" ? "apply" : action;
    const result = await decideProposal(this.config, proposal.id, effectiveAction, decidedBy);
    await this.telegram.sendText(target, result.message);
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
