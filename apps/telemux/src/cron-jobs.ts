import type { CodexReasoningEffort } from "./codex-runtime";

export type CronJobKind = "reminder" | "codex";
export type CronWeekday = "sunday" | "monday" | "tuesday" | "wednesday" | "thursday" | "friday" | "saturday";

export interface CronScheduleOnce {
  type: "once";
  at: string;
}

export interface CronScheduleDaily {
  type: "daily";
  time: string;
  timezone: string;
}

export interface CronScheduleWeekly {
  type: "weekly";
  weekday: CronWeekday;
  time: string;
  timezone: string;
}

export interface CronScheduleMonthly {
  type: "monthly";
  dayOfMonth: number;
  time: string;
  timezone: string;
}

export interface CronScheduleInterval {
  type: "interval";
  everyMinutes: number;
  anchorAt: string;
}

export type CronSchedule =
  | CronScheduleOnce
  | CronScheduleDaily
  | CronScheduleWeekly
  | CronScheduleMonthly
  | CronScheduleInterval;

export interface CronJobRecord {
  id: string;
  label: string;
  kind: CronJobKind;
  enabled: boolean;
  schedule: CronSchedule;
  nextRunAt: string | null;
  pendingRunAt: string | null;
  lastRunAt: string | null;
  lastScheduledFor: string | null;
  executionContextSlug: string | null;
  targetChatId: number;
  targetThreadId: number | null;
  instruction: string | null;
  reminderText: string | null;
  modelOverride: string | null;
  reasoningEffortOverride: CodexReasoningEffort | null;
  lastResult: string | null;
  lastError: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface CronRunRecord {
  id: string;
  jobId: string;
  scheduledFor: string | null;
  startedAt: string;
  finishedAt: string | null;
  status: "queued" | "sent" | "dispatched" | "skipped" | "failed";
  note: string | null;
}

export interface CronJobDraft {
  label: string;
  kind: CronJobKind;
  schedule: CronSchedule;
  executionContextSlug?: string | null;
  targetChatId?: number | null;
  targetThreadId?: number | null;
  instruction?: string | null;
  reminderText?: string | null;
  modelOverride?: string | null;
  reasoningEffortOverride?: CodexReasoningEffort | null;
  enabled?: boolean;
}

export interface CronJobSelector {
  id?: string | null;
  label?: string | null;
}

export interface CronJobChanges {
  label?: string | null;
  schedule?: CronSchedule | null;
  executionContextSlug?: string | null;
  targetChatId?: number | null;
  targetThreadId?: number | null;
  instruction?: string | null;
  reminderText?: string | null;
  modelOverride?: string | null;
  reasoningEffortOverride?: CodexReasoningEffort | null;
  enabled?: boolean | null;
}

export interface CronCreateAction {
  type: "create";
  job: CronJobDraft;
}

export interface CronUpdateAction {
  type: "update";
  selector: CronJobSelector;
  changes: CronJobChanges;
}

export interface CronDeleteAction {
  type: "delete";
  selector: CronJobSelector;
}

export interface CronPauseAction {
  type: "pause";
  selector: CronJobSelector;
}

export interface CronResumeAction {
  type: "resume";
  selector: CronJobSelector;
}

export type CronManifestAction = CronCreateAction | CronUpdateAction | CronDeleteAction | CronPauseAction | CronResumeAction;

export interface ParsedCronManifest {
  actions: CronManifestAction[];
  skipped: string[];
}

export const CRON_REQUESTS_FILE_NAME = "CRON_REQUESTS.json";
export const CRON_REQUESTS_WORKSPACE_PATH = `.factory/${CRON_REQUESTS_FILE_NAME}`;
export const CONTEXT_CRONS_FILE_NAME = "CRONS.md";
export const CONTEXT_CRONS_WORKSPACE_PATH = `.factory/${CONTEXT_CRONS_FILE_NAME}`;

const WEEKDAYS: CronWeekday[] = [
  "sunday",
  "monday",
  "tuesday",
  "wednesday",
  "thursday",
  "friday",
  "saturday"
];

function utcNowIso(): string {
  return new Date().toISOString();
}

function parseInteger(value: unknown): number | null {
  if (typeof value === "number" && Number.isInteger(value)) {
    return value;
  }

  if (typeof value === "string" && /^-?\d+$/.test(value.trim())) {
    return Number(value.trim());
  }

  return null;
}

function parseTime(value: unknown): { hour: number; minute: number } | null {
  if (typeof value !== "string") {
    return null;
  }

  const match = value.trim().match(/^(\d{1,2}):(\d{2})$/);
  if (!match) {
    return null;
  }

  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (!Number.isInteger(hour) || !Number.isInteger(minute) || hour < 0 || hour > 23 || minute < 0 || minute > 59) {
    return null;
  }

  return { hour, minute };
}

function isValidTimezone(value: unknown): value is string {
  if (typeof value !== "string" || !value.trim()) {
    return false;
  }

  try {
    new Intl.DateTimeFormat("en-US", { timeZone: value.trim() });
    return true;
  } catch {
    return false;
  }
}

function normalizeWeekday(value: unknown): CronWeekday | null {
  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.trim().toLowerCase();
  const aliases = new Map<string, CronWeekday>([
    ["sun", "sunday"],
    ["sunday", "sunday"],
    ["mon", "monday"],
    ["monday", "monday"],
    ["tue", "tuesday"],
    ["tues", "tuesday"],
    ["tuesday", "tuesday"],
    ["wed", "wednesday"],
    ["wednesday", "wednesday"],
    ["thu", "thursday"],
    ["thur", "thursday"],
    ["thurs", "thursday"],
    ["thursday", "thursday"],
    ["fri", "friday"],
    ["friday", "friday"],
    ["sat", "saturday"],
    ["saturday", "saturday"]
  ]);

  return aliases.get(normalized) || null;
}

function zonedParts(date: Date, timezone: string): {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
  weekday: number;
} {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
    weekday: "short"
  });
  const parts = formatter.formatToParts(date);
  const values = new Map<string, string>();

  for (const part of parts) {
    values.set(part.type, part.value);
  }

  const weekdayText = (values.get("weekday") || "").toLowerCase();
  const weekday = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"].indexOf(weekdayText);

  return {
    year: Number(values.get("year") || "0"),
    month: Number(values.get("month") || "0"),
    day: Number(values.get("day") || "0"),
    hour: Number(values.get("hour") || "0"),
    minute: Number(values.get("minute") || "0"),
    second: Number(values.get("second") || "0"),
    weekday: weekday >= 0 ? weekday : 0
  };
}

function timezoneOffsetMs(date: Date, timezone: string): number {
  const parts = zonedParts(date, timezone);
  const asUtc = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second);
  return asUtc - date.getTime();
}

function zonedDateToUtc(timezone: string, year: number, month: number, day: number, hour: number, minute: number): Date {
  const baseUtc = Date.UTC(year, month - 1, day, hour, minute, 0);
  let candidate = new Date(baseUtc - timezoneOffsetMs(new Date(baseUtc), timezone));
  const correctedOffset = timezoneOffsetMs(candidate, timezone);
  candidate = new Date(baseUtc - correctedOffset);
  return candidate;
}

function addUtcDays(year: number, month: number, day: number, days: number): { year: number; month: number; day: number } {
  const candidate = new Date(Date.UTC(year, month - 1, day + days, 12, 0, 0));
  return {
    year: candidate.getUTCFullYear(),
    month: candidate.getUTCMonth() + 1,
    day: candidate.getUTCDate()
  };
}

function daysInMonth(year: number, month: number): number {
  return new Date(Date.UTC(year, month, 0, 12, 0, 0)).getUTCDate();
}

function addUtcMonths(year: number, month: number, months: number): { year: number; month: number } {
  const candidate = new Date(Date.UTC(year, month - 1 + months, 1, 12, 0, 0));
  return {
    year: candidate.getUTCFullYear(),
    month: candidate.getUTCMonth() + 1
  };
}

function laterThan(candidate: Date, reference: Date, inclusive = false): boolean {
  return inclusive ? candidate.getTime() >= reference.getTime() : candidate.getTime() > reference.getTime();
}

function nextDailyRun(schedule: CronScheduleDaily, reference: Date): string {
  const time = parseTime(schedule.time);
  if (!time) {
    throw new Error(`Invalid daily schedule time: ${schedule.time}`);
  }

  const local = zonedParts(reference, schedule.timezone);
  for (let offset = 0; offset < 8; offset += 1) {
    const day = addUtcDays(local.year, local.month, local.day, offset);
    const candidate = zonedDateToUtc(schedule.timezone, day.year, day.month, day.day, time.hour, time.minute);
    if (laterThan(candidate, reference)) {
      return candidate.toISOString();
    }
  }

  throw new Error("Could not compute next daily run");
}

function nextWeeklyRun(schedule: CronScheduleWeekly, reference: Date): string {
  const time = parseTime(schedule.time);
  if (!time) {
    throw new Error(`Invalid weekly schedule time: ${schedule.time}`);
  }

  const targetWeekday = WEEKDAYS.indexOf(schedule.weekday);
  if (targetWeekday === -1) {
    throw new Error(`Invalid weekly schedule weekday: ${schedule.weekday}`);
  }

  const local = zonedParts(reference, schedule.timezone);
  for (let offset = 0; offset < 15; offset += 1) {
    const day = addUtcDays(local.year, local.month, local.day, offset);
    const weekday = zonedParts(zonedDateToUtc(schedule.timezone, day.year, day.month, day.day, 12, 0), schedule.timezone)
      .weekday;
    if (weekday !== targetWeekday) {
      continue;
    }

    const candidate = zonedDateToUtc(schedule.timezone, day.year, day.month, day.day, time.hour, time.minute);
    if (laterThan(candidate, reference)) {
      return candidate.toISOString();
    }
  }

  throw new Error("Could not compute next weekly run");
}

function nextMonthlyRun(schedule: CronScheduleMonthly, reference: Date): string {
  const time = parseTime(schedule.time);
  if (!time) {
    throw new Error(`Invalid monthly schedule time: ${schedule.time}`);
  }

  if (!Number.isInteger(schedule.dayOfMonth) || schedule.dayOfMonth < 1 || schedule.dayOfMonth > 31) {
    throw new Error(`Invalid monthly schedule day: ${schedule.dayOfMonth}`);
  }

  const local = zonedParts(reference, schedule.timezone);
  for (let offset = 0; offset < 24; offset += 1) {
    const month = addUtcMonths(local.year, local.month, offset);
    if (schedule.dayOfMonth > daysInMonth(month.year, month.month)) {
      continue;
    }

    const candidate = zonedDateToUtc(
      schedule.timezone,
      month.year,
      month.month,
      schedule.dayOfMonth,
      time.hour,
      time.minute
    );
    if (laterThan(candidate, reference)) {
      return candidate.toISOString();
    }
  }

  throw new Error("Could not compute next monthly run");
}

function nextIntervalRun(schedule: CronScheduleInterval, reference: Date): string {
  if (!Number.isInteger(schedule.everyMinutes) || schedule.everyMinutes <= 0) {
    throw new Error(`Invalid interval schedule minutes: ${schedule.everyMinutes}`);
  }

  const anchor = new Date(schedule.anchorAt);
  if (Number.isNaN(anchor.getTime())) {
    throw new Error(`Invalid interval anchor: ${schedule.anchorAt}`);
  }

  if (anchor.getTime() > reference.getTime()) {
    return anchor.toISOString();
  }

  const intervalMs = schedule.everyMinutes * 60_000;
  const delta = reference.getTime() - anchor.getTime();
  const steps = Math.floor(delta / intervalMs) + 1;
  return new Date(anchor.getTime() + steps * intervalMs).toISOString();
}

export function normalizeCronSchedule(input: unknown): CronSchedule {
  if (!input || typeof input !== "object") {
    throw new Error("Schedule must be an object");
  }

  const raw = input as Record<string, unknown>;
  const type = typeof raw.type === "string" ? raw.type.trim().toLowerCase() : "";

  if (type === "once") {
    const at = typeof raw.at === "string" ? raw.at.trim() : "";
    const date = new Date(at);
    if (!at || Number.isNaN(date.getTime())) {
      throw new Error("One-off schedules require a valid ISO `at` timestamp");
    }

    return {
      type: "once",
      at: date.toISOString()
    };
  }

  if (type === "daily") {
    if (!isValidTimezone(raw.timezone)) {
      throw new Error("Daily schedules require a valid `timezone`");
    }

    const time = typeof raw.time === "string" ? raw.time.trim() : "";
    if (!parseTime(time)) {
      throw new Error("Daily schedules require `time` in HH:MM format");
    }

    return {
      type: "daily",
      time,
      timezone: raw.timezone.trim()
    };
  }

  if (type === "weekly") {
    if (!isValidTimezone(raw.timezone)) {
      throw new Error("Weekly schedules require a valid `timezone`");
    }

    const weekday = normalizeWeekday(raw.weekday);
    const time = typeof raw.time === "string" ? raw.time.trim() : "";
    if (!weekday) {
      throw new Error("Weekly schedules require `weekday`");
    }

    if (!parseTime(time)) {
      throw new Error("Weekly schedules require `time` in HH:MM format");
    }

    return {
      type: "weekly",
      weekday,
      time,
      timezone: raw.timezone.trim()
    };
  }

  if (type === "monthly") {
    if (!isValidTimezone(raw.timezone)) {
      throw new Error("Monthly schedules require a valid `timezone`");
    }

    const dayOfMonth = parseInteger(raw.dayOfMonth);
    const time = typeof raw.time === "string" ? raw.time.trim() : "";
    if (!dayOfMonth || dayOfMonth < 1 || dayOfMonth > 31) {
      throw new Error("Monthly schedules require `dayOfMonth` between 1 and 31");
    }

    if (!parseTime(time)) {
      throw new Error("Monthly schedules require `time` in HH:MM format");
    }

    return {
      type: "monthly",
      dayOfMonth,
      time,
      timezone: raw.timezone.trim()
    };
  }

  if (type === "interval") {
    const everyMinutes = parseInteger(raw.everyMinutes);
    const anchorAt = typeof raw.anchorAt === "string" ? raw.anchorAt.trim() : "";
    const anchorDate = new Date(anchorAt);
    if (!everyMinutes || everyMinutes <= 0) {
      throw new Error("Interval schedules require a positive integer `everyMinutes`");
    }

    if (!anchorAt || Number.isNaN(anchorDate.getTime())) {
      throw new Error("Interval schedules require a valid ISO `anchorAt` timestamp");
    }

    return {
      type: "interval",
      everyMinutes,
      anchorAt: anchorDate.toISOString()
    };
  }

  throw new Error(`Unsupported schedule type: ${type || "unknown"}`);
}

export function nextCronRunAt(schedule: CronSchedule, fromIso: string | null = null): string | null {
  const reference = fromIso ? new Date(fromIso) : new Date();
  if (Number.isNaN(reference.getTime())) {
    throw new Error(`Invalid schedule reference time: ${fromIso}`);
  }

  switch (schedule.type) {
    case "once": {
      const at = new Date(schedule.at);
      if (Number.isNaN(at.getTime())) {
        throw new Error(`Invalid one-off schedule timestamp: ${schedule.at}`);
      }

      return at.getTime() > reference.getTime() ? at.toISOString() : null;
    }
    case "daily":
      return nextDailyRun(schedule, reference);
    case "weekly":
      return nextWeeklyRun(schedule, reference);
    case "monthly":
      return nextMonthlyRun(schedule, reference);
    case "interval":
      return nextIntervalRun(schedule, reference);
  }
}

export function scheduleSummary(schedule: CronSchedule): string {
  switch (schedule.type) {
    case "once":
      return `once at ${schedule.at}`;
    case "daily":
      return `daily at ${schedule.time} ${schedule.timezone}`;
    case "weekly":
      return `every ${schedule.weekday} at ${schedule.time} ${schedule.timezone}`;
    case "monthly":
      return `day ${schedule.dayOfMonth} monthly at ${schedule.time} ${schedule.timezone}`;
    case "interval":
      return `every ${schedule.everyMinutes} minute(s) from ${schedule.anchorAt}`;
  }
}

function normalizeSelector(input: unknown): CronJobSelector | null {
  if (!input || typeof input !== "object") {
    return null;
  }

  const raw = input as Record<string, unknown>;
  const id = typeof raw.id === "string" && raw.id.trim() ? raw.id.trim() : null;
  const label = typeof raw.label === "string" && raw.label.trim() ? raw.label.trim() : null;
  if (!id && !label) {
    return null;
  }

  return { id, label };
}

function normalizeDraft(input: unknown): CronJobDraft {
  if (!input || typeof input !== "object") {
    throw new Error("Cron job draft must be an object");
  }

  const raw = input as Record<string, unknown>;
  const label = typeof raw.label === "string" ? raw.label.trim() : "";
  const kind = raw.kind === "reminder" || raw.kind === "codex" ? raw.kind : null;
  const schedule = normalizeCronSchedule(raw.schedule);
  const executionContextSlug =
    typeof raw.executionContextSlug === "string" && raw.executionContextSlug.trim()
      ? raw.executionContextSlug.trim()
      : null;
  const targetChatId = parseInteger(raw.targetChatId);
  const targetThreadId = raw.targetThreadId === null ? null : parseInteger(raw.targetThreadId);
  const instruction = typeof raw.instruction === "string" && raw.instruction.trim() ? raw.instruction.trim() : null;
  const reminderText =
    typeof raw.reminderText === "string" && raw.reminderText.trim() ? raw.reminderText.trim() : null;
  const modelOverride =
    typeof raw.modelOverride === "string" && raw.modelOverride.trim() ? raw.modelOverride.trim() : null;
  const reasoningEffortOverride =
    raw.reasoningEffortOverride === "low" ||
    raw.reasoningEffortOverride === "medium" ||
    raw.reasoningEffortOverride === "high" ||
    raw.reasoningEffortOverride === "xhigh"
      ? raw.reasoningEffortOverride
      : null;
  const enabled = typeof raw.enabled === "boolean" ? raw.enabled : undefined;

  if (!label) {
    throw new Error("Cron job drafts require `label`");
  }

  if (!kind) {
    throw new Error("Cron job drafts require `kind`");
  }

  if (kind === "reminder" && !reminderText) {
    throw new Error("Reminder cron jobs require `reminderText`");
  }

  if (kind === "codex" && !instruction) {
    throw new Error("Codex cron jobs require `instruction`");
  }

  return {
    label,
    kind,
    schedule,
    executionContextSlug,
    targetChatId,
    targetThreadId,
    instruction,
    reminderText,
    modelOverride,
    reasoningEffortOverride,
    enabled
  };
}

function normalizeChanges(input: unknown): CronJobChanges {
  if (!input || typeof input !== "object") {
    throw new Error("Cron job changes must be an object");
  }

  const raw = input as Record<string, unknown>;
  const changes: CronJobChanges = {};

  if (raw.label !== undefined) {
    changes.label = typeof raw.label === "string" && raw.label.trim() ? raw.label.trim() : null;
  }

  if (raw.schedule !== undefined) {
    changes.schedule = raw.schedule === null ? null : normalizeCronSchedule(raw.schedule);
  }

  if (raw.executionContextSlug !== undefined) {
    changes.executionContextSlug =
      typeof raw.executionContextSlug === "string" && raw.executionContextSlug.trim()
        ? raw.executionContextSlug.trim()
        : null;
  }

  if (raw.targetChatId !== undefined) {
    changes.targetChatId = raw.targetChatId === null ? null : parseInteger(raw.targetChatId);
  }

  if (raw.targetThreadId !== undefined) {
    changes.targetThreadId = raw.targetThreadId === null ? null : parseInteger(raw.targetThreadId);
  }

  if (raw.instruction !== undefined) {
    changes.instruction = typeof raw.instruction === "string" && raw.instruction.trim() ? raw.instruction.trim() : null;
  }

  if (raw.reminderText !== undefined) {
    changes.reminderText =
      typeof raw.reminderText === "string" && raw.reminderText.trim() ? raw.reminderText.trim() : null;
  }

  if (raw.modelOverride !== undefined) {
    changes.modelOverride =
      typeof raw.modelOverride === "string" && raw.modelOverride.trim() ? raw.modelOverride.trim() : null;
  }

  if (raw.reasoningEffortOverride !== undefined) {
    changes.reasoningEffortOverride =
      raw.reasoningEffortOverride === "low" ||
      raw.reasoningEffortOverride === "medium" ||
      raw.reasoningEffortOverride === "high" ||
      raw.reasoningEffortOverride === "xhigh"
        ? raw.reasoningEffortOverride
        : null;
  }

  if (raw.enabled !== undefined) {
    changes.enabled = typeof raw.enabled === "boolean" ? raw.enabled : null;
  }

  return changes;
}

export function parseCronManifest(text: string | null): ParsedCronManifest {
  if (!text?.trim()) {
    return {
      actions: [],
      skipped: []
    };
  }

  try {
    const parsed = JSON.parse(text) as { actions?: unknown };
    const actions = Array.isArray(parsed.actions) ? parsed.actions : [];
    const normalized: CronManifestAction[] = [];
    const skipped: string[] = [];

    for (const entry of actions) {
      try {
        if (!entry || typeof entry !== "object") {
          throw new Error("Cron action must be an object");
        }

        const raw = entry as Record<string, unknown>;
        const type = typeof raw.type === "string" ? raw.type.trim().toLowerCase() : "";

        if (type === "create") {
          normalized.push({
            type: "create",
            job: normalizeDraft(raw.job)
          });
          continue;
        }

        if (type === "update") {
          const selector = normalizeSelector(raw.selector);
          if (!selector) {
            throw new Error("Update actions require `selector.id` or `selector.label`");
          }

          normalized.push({
            type: "update",
            selector,
            changes: normalizeChanges(raw.changes)
          });
          continue;
        }

        if (type === "delete" || type === "pause" || type === "resume") {
          const selector = normalizeSelector(raw.selector);
          if (!selector) {
            throw new Error(`${type} actions require \`selector.id\` or \`selector.label\``);
          }

          normalized.push({
            type,
            selector
          } as CronDeleteAction | CronPauseAction | CronResumeAction);
          continue;
        }

        throw new Error(`Unsupported cron action type: ${type || "unknown"}`);
      } catch (error) {
        skipped.push(error instanceof Error ? error.message : String(error));
      }
    }

    return {
      actions: normalized,
      skipped
    };
  } catch (error) {
    return {
      actions: [],
      skipped: [`Could not parse ${CRON_REQUESTS_FILE_NAME}: ${error instanceof Error ? error.message : String(error)}`]
    };
  }
}

export function createCronJobId(label: string, nowIso = utcNowIso()): string {
  const stem = label
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 24) || "job";
  const suffix = nowIso.replace(/[-:TZ.]/g, "").slice(0, 14);
  return `${stem}-${suffix}`;
}

export function formatCronJobsMarkdown(contextLabel: string, jobs: CronJobRecord[]): string {
  const lines = ["# Cron Jobs", "", `Context view: ${contextLabel}`, ""];

  if (!jobs.length) {
    lines.push("- No active scheduled jobs are currently linked to this context or its bound Telegram topic.");
    return `${lines.join("\n")}\n`;
  }

  for (const job of jobs) {
    lines.push(`- id: ${job.id}`);
    lines.push(`  label: ${job.label}`);
    lines.push(`  kind: ${job.kind}`);
    lines.push(`  enabled: ${job.enabled ? "yes" : "no"}`);
    lines.push(`  schedule: ${scheduleSummary(job.schedule)}`);
    lines.push(`  next_run_at: ${job.nextRunAt || "none"}`);
    lines.push(`  pending_run_at: ${job.pendingRunAt || "none"}`);
    lines.push(`  execution_context: ${job.executionContextSlug || "none"}`);
    lines.push(`  target: ${job.targetChatId}:${job.targetThreadId ?? "none"}`);
    lines.push(`  model_override: ${job.modelOverride || "default"}`);
    lines.push(`  effort_override: ${job.reasoningEffortOverride || "default"}`);
    if (job.reminderText) {
      lines.push(`  reminder: ${job.reminderText}`);
    }
    if (job.instruction) {
      lines.push(`  instruction: ${job.instruction}`);
    }
    lines.push("");
  }

  return `${lines.join("\n").trimEnd()}\n`;
}
