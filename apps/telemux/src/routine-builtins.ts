import type { CronJobDraft, CronSchedule } from "./cron-jobs";

export type BuiltinRoutineName = "update-check" | "brain-curator" | "daily-checkin";
export const DETERMINISTIC_UPDATE_CHECK_RUNNER = "deterministic-update-check";

export interface BuiltinRoutineDefinition {
  name: BuiltinRoutineName;
  label: string;
  kind: CronJobDraft["kind"];
  description: string;
  defaultTime: string;
  defaultSchedule: "daily" | "weekly";
  defaultWeekday?: string;
  instruction?: string;
  reminderText?: string;
}

function defaultTimezone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC";
  } catch {
    return "UTC";
  }
}

function updateCheckInstruction(): string {
  return [
    "Run Brainstack's deterministic built-in update check.",
    "",
    "This built-in is executed by telemux directly, not by an LLM harness. It runs Brainstack's read-only `brainctl updates` path or a deterministic fallback probe, writes a report artifact, and posts a concise Telegram summary.",
    "",
    "Do not replace this instruction with package install, upgrade, remove, reboot, restart, or service mutation steps."
  ].join("\n");
}

function brainCuratorInstruction(): string {
  return [
    "Run Brainstack's built-in shared-brain curator pass.",
    "",
    "Rules:",
    "- Use the `brain-curator` skill if it is installed or visible in this repo.",
    "- Preserve raw imports and proposals. Do not rewrite raw artifacts.",
    "- Do not mix private-journal material into the shared dev brain.",
    "- Prefer proposals or clearly sourced wiki edits over unsourced summary dumping.",
    "",
    "Checklist:",
    "1. Inspect the shared-brain clone locations under `~/shared-brain`, especially staging/serve clones if present.",
    "2. Sync only with safe fast-forward commands when the tree is clean. If dirty or diverged, report the blocker instead of forcing.",
    "3. Review recent raw imports, proposals, logs, and prior curator state. Keep a small cursor in this workspace if useful.",
    "4. Produce a sourced curation report with: accepted/suggested wiki updates, contradictions, stale facts, skipped private-sensitive items, and exact source paths/ids.",
    "5. If `brainctl propose` is configured, submit concise proposals for worthwhile wiki updates; otherwise record proposal drafts as artifacts.",
    "6. Update `.factory/SUMMARY.md`, `.factory/TODO.md`, and `.factory/ARTIFACTS.md` so the next curator pass can resume."
  ].join("\n");
}

function dailyCheckinText(): string {
  return [
    "Daily check-in.",
    "",
    "Reply in this topic with:",
    "1. What mattered since the last check-in?",
    "2. What matters next?",
    "3. Any blockers or worries?",
    "4. Health, energy, sleep, and focus notes.",
    "",
    "If this topic is bound to a Brainstack context and shared-brain imports are configured, your reply will flow through the normal context/import path."
  ].join("\n");
}

const ROUTINES: BuiltinRoutineDefinition[] = [
  {
    name: "update-check",
    label: "update-check",
    kind: "codex",
    description: "Read-only OS, Brainstack, Codex, and Claude update visibility.",
    defaultSchedule: "weekly",
    defaultWeekday: "monday",
    defaultTime: "09:00",
    instruction: updateCheckInstruction()
  },
  {
    name: "brain-curator",
    label: "brain-curator",
    kind: "codex",
    description: "Periodic shared-brain raw/proposal review and sourced curation report.",
    defaultSchedule: "daily",
    defaultTime: "06:30",
    instruction: brainCuratorInstruction()
  },
  {
    name: "daily-checkin",
    label: "daily-checkin",
    kind: "reminder",
    description: "Daily Telegram check-in prompt that feeds the bound topic workflow when you reply.",
    defaultSchedule: "daily",
    defaultTime: "09:00",
    reminderText: dailyCheckinText()
  }
];

export function listBuiltinRoutines(): BuiltinRoutineDefinition[] {
  return ROUTINES;
}

export function normalizeBuiltinRoutineName(value: string): BuiltinRoutineName | null {
  const normalized = value.trim().toLowerCase().replaceAll("_", "-");
  if (normalized === "update-check" || normalized === "brain-curator" || normalized === "daily-checkin") {
    return normalized;
  }
  return null;
}

export function getBuiltinRoutine(value: string): BuiltinRoutineDefinition | null {
  const name = normalizeBuiltinRoutineName(value);
  return name ? ROUTINES.find((routine) => routine.name === name) || null : null;
}

export function isDeterministicRoutine(runner: string | null | undefined, name: BuiltinRoutineName): boolean {
  return name === "update-check" && runner === DETERMINISTIC_UPDATE_CHECK_RUNNER;
}

export function builtinRoutineCommandToken(name: BuiltinRoutineName): string {
  return name.replaceAll("-", "_");
}

export function defaultBuiltinSchedule(definition: BuiltinRoutineDefinition): CronSchedule {
  const timezone = defaultTimezone();
  if (definition.defaultSchedule === "weekly") {
    return {
      type: "weekly",
      weekday: definition.defaultWeekday || "monday",
      time: definition.defaultTime,
      timezone
    } as CronSchedule;
  }

  return {
    type: "daily",
    time: definition.defaultTime,
    timezone
  };
}

export function builtinRoutineDraft(
  definition: BuiltinRoutineDefinition,
  schedule: CronSchedule,
  contextSlug: string | null
): CronJobDraft {
  return {
    label: definition.label,
    kind: definition.kind,
    runner: definition.name === "update-check" ? DETERMINISTIC_UPDATE_CHECK_RUNNER : null,
    schedule,
    executionContextSlug: contextSlug,
    instruction: definition.instruction || null,
    reminderText: definition.reminderText || null
  };
}
