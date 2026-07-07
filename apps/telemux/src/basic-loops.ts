import type { FactoryConfig } from "./config";
import type { ContextService } from "./contexts";
import type { CronManager } from "./cron-manager";
import type { CronJobRecord } from "./cron-jobs";
import type { ContextRecord } from "./db";
import {
  builtinRoutineDraft,
  defaultBuiltinSchedule,
  DETERMINISTIC_UPDATE_CHECK_RUNNER,
  getBuiltinRoutine
} from "./routine-builtins";
import { reportCuratorStatus } from "./curator-report";
import type { WorkerService } from "./workers";

const ROUTINES_CONTEXT_SLUG = "brainstack-routines";
const PROPOSAL_CURATION_CONTEXT_SLUG = "proposal-curation";

export async function ensureBasicLoops(
  config: FactoryConfig,
  contexts: ContextService,
  workers: WorkerService,
  cronManager: CronManager
): Promise<string> {
  if (!config.telegramControlChatId) {
    return "skipped: FACTORY_TELEGRAM_CONTROL_CHAT_ID is not set";
  }

  const routine = getBuiltinRoutine("update-check");
  if (!routine) {
    throw new Error("built-in update-check routine is missing");
  }

  const slug = ROUTINES_CONTEXT_SLUG;
  const existingContext = contexts.getContextBySlug(slug);
  let context = existingContext;

  if (!context || context.state !== "active") {
    const bootstrap = await workers.bootstrapContext({
      slug,
      machine: config.localMachine,
      target: "scratch",
      baseBranch: null
    });

    context = contexts.createOrUpdateContext({
      slug,
      machine: config.localMachine,
      kind: bootstrap.kind,
      state: bootstrap.state,
      transport: bootstrap.transport,
      target: "scratch",
      rootPath: bootstrap.rootPath,
      worktreePath: bootstrap.worktreePath,
      branchName: bootstrap.branchName,
      baseBranch: bootstrap.baseBranch,
      usageAdapter: config.usageAdapter,
      chatId: null,
      threadId: null,
      lastError: bootstrap.ok ? null : (bootstrap.stderr.trim() || bootstrap.stdout.trim() || `exit ${bootstrap.exitCode}`)
    });
  }

  if (context.state !== "active") {
    return `skipped: ${slug} context is ${context.state}`;
  }

  const notes: string[] = [];

  const existingJob = cronManager.listJobs().find((job) => {
    if (job.runner !== DETERMINISTIC_UPDATE_CHECK_RUNNER || job.label.toLowerCase() !== routine.label.toLowerCase()) {
      return false;
    }

    return job.executionContextSlug === context.slug;
  });

  if (existingJob) {
    await cronManager.updateJob(existingJob, {
      executionContextSlug: context.slug,
      targetChatId: config.telegramControlChatId,
      targetThreadId: null,
      instruction: routine.instruction || null,
      runner: DETERMINISTIC_UPDATE_CHECK_RUNNER
    });
    notes.push(`updated: ${existingJob.id}`);
  } else {
    const schedule = defaultBuiltinSchedule(routine);
    const draft = builtinRoutineDraft(routine, schedule, context.slug);
    const created = await cronManager.createJob(
      {
        ...draft,
        targetChatId: config.telegramControlChatId,
        targetThreadId: null
      },
      {
        context,
        target: { chatId: config.telegramControlChatId, threadId: null }
      }
    );
    notes.push(`created: ${created.id}`);
  }

  notes.push(await ensureCuratorRoutine(config, cronManager, context));

  return notes.join("; ");
}

function isBrainCuratorJob(job: CronJobRecord, label: string): boolean {
  return job.kind === "codex" && !job.runner && job.label.toLowerCase() === label.toLowerCase();
}

function preferCuratorJob(jobs: CronJobRecord[], routinesSlug: string): CronJobRecord | null {
  return (
    jobs.find((job) => job.enabled && job.executionContextSlug === PROPOSAL_CURATION_CONTEXT_SLUG) ||
    jobs.find((job) => job.executionContextSlug === PROPOSAL_CURATION_CONTEXT_SLUG) ||
    jobs.find((job) => job.enabled && job.executionContextSlug === routinesSlug) ||
    jobs.find((job) => job.executionContextSlug === routinesSlug) ||
    jobs.find((job) => job.enabled) ||
    jobs[0] ||
    null
  );
}

async function pauseDuplicateCuratorJobs(
  cronManager: CronManager,
  jobs: CronJobRecord[],
  keeper: CronJobRecord
): Promise<CronJobRecord[]> {
  const paused: CronJobRecord[] = [];
  for (const job of jobs) {
    if (job.id === keeper.id || !job.enabled) {
      continue;
    }
    paused.push(await cronManager.pauseJob(job));
  }
  return paused;
}

/**
 * Keep exactly one enabled built-in brain-curator routine. If the operator has
 * moved curation into the dedicated proposal-curation topic, that topic owns the
 * routine and the General/routines copy is paused.
 */
async function ensureCuratorRoutine(config: FactoryConfig, cronManager: CronManager, context: ContextRecord): Promise<string> {
  const curator = getBuiltinRoutine("brain-curator");
  if (!curator) {
    throw new Error("built-in brain-curator routine is missing");
  }
  const jobs = cronManager.listJobs().filter((job) => isBrainCuratorJob(job, curator.label));
  const existing = preferCuratorJob(jobs, context.slug);
  if (existing) {
    const executionContextSlug = existing.executionContextSlug || context.slug;
    const targetChatId = existing.targetChatId || config.telegramControlChatId!;
    const draft = builtinRoutineDraft(curator, existing.schedule, executionContextSlug);
    const updated = await cronManager.updateJob(existing, {
      executionContextSlug,
      targetChatId,
      targetThreadId: existing.targetThreadId ?? null,
      instruction: draft.instruction || null,
      reminderText: draft.reminderText || null,
      runner: draft.runner || null,
      enabled: true
    });
    const paused = await pauseDuplicateCuratorJobs(cronManager, jobs, updated);
    await reportCuratorStatus(config, { installed: true, next_run_at: updated.nextRunAt ?? null });
    return [`curator updated: ${updated.id}`, paused.length ? `paused duplicate curator job(s): ${paused.map((job) => job.id).join(", ")}` : null]
      .filter(Boolean)
      .join("; ");
  }
  const schedule = defaultBuiltinSchedule(curator);
  const draft = builtinRoutineDraft(curator, schedule, context.slug);
  const created = await cronManager.createJob(
    {
      ...draft,
      targetChatId: config.telegramControlChatId!,
      targetThreadId: null
    },
    {
      context,
      target: { chatId: config.telegramControlChatId!, threadId: null }
    }
  );
  await reportCuratorStatus(config, { installed: true, next_run_at: created.nextRunAt ?? null });
  return `curator created: ${created.id}`;
}
