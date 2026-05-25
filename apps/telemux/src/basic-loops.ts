import type { FactoryConfig } from "./config";
import type { ContextService } from "./contexts";
import type { CronManager } from "./cron-manager";
import {
  builtinRoutineDraft,
  defaultBuiltinSchedule,
  DETERMINISTIC_UPDATE_CHECK_RUNNER,
  getBuiltinRoutine
} from "./routine-builtins";
import type { WorkerService } from "./workers";

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

  const slug = "brainstack-routines";
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
    return `updated: ${existingJob.id}`;
  }

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

  return `created: ${created.id}`;
}
