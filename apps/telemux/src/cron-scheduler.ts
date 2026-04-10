import { CronManager } from "./cron-manager";
import { nextCronRunAt, type CronJobRecord } from "./cron-jobs";
import type { FactoryConfig } from "./config";
import type { FactoryDb } from "./db";
import type { Dispatcher } from "./dispatcher";
import type { TelegramBot } from "./telegram";

function nowIso(): string {
  return new Date().toISOString();
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class CronScheduler {
  private running = false;
  private started = false;
  private readonly serviceStartedAt = nowIso();

  constructor(
    private readonly config: FactoryConfig,
    private readonly db: FactoryDb,
    private readonly manager: CronManager,
    private readonly dispatcher: Dispatcher,
    private readonly telegram: TelegramBot
  ) {}

  start(): void {
    void this.runDueJobs().catch((error) => {
      console.error("initial cron scheduler run failed", error);
    });

    setInterval(() => {
      void this.runDueJobs().catch((error) => {
        console.error("scheduled cron run failed", error);
      });
    }, this.config.cronPollIntervalSeconds * 1000);
  }

  async runDueJobs(referenceIso = nowIso()): Promise<void> {
    if (this.running) {
      return;
    }

    this.running = true;

    try {
      if (!this.started) {
        await this.manager.fastForwardMissedRuns(this.serviceStartedAt);
        this.started = true;
      }

      const dueJobs = this.db.listDueCronJobs(referenceIso);
      for (const due of dueJobs) {
        const current = this.db.getCronJob(due.id) || due;
        await this.advanceWhilePending(current, referenceIso);

        const refreshed = this.db.getCronJob(due.id) || current;
        if (!refreshed.enabled && !refreshed.pendingRunAt) {
          continue;
        }

        if (refreshed.pendingRunAt) {
          await this.runCronJob(refreshed, refreshed.pendingRunAt, true);
          continue;
        }

        if (refreshed.nextRunAt && refreshed.nextRunAt <= referenceIso) {
          await this.runCronJob(refreshed, refreshed.nextRunAt, false);
        }
      }
    } finally {
      this.running = false;
    }
  }

  private async advanceWhilePending(job: CronJobRecord, referenceIso: string): Promise<void> {
    if (!job.pendingRunAt || !job.nextRunAt) {
      return;
    }

    let nextRunAt = job.nextRunAt;
    while (nextRunAt && nextRunAt <= referenceIso) {
      nextRunAt = nextCronRunAt(job.schedule, nextRunAt);
    }

    if (nextRunAt === job.nextRunAt) {
      return;
    }

    await this.manager.saveJob({
      ...job,
      nextRunAt,
      updatedAt: nowIso()
    });
  }

  private async runCronJob(job: CronJobRecord, scheduledFor: string, alreadyAdvanced: boolean): Promise<void> {
    if (job.kind === "reminder") {
      await this.runReminderJob(job, scheduledFor, alreadyAdvanced);
      return;
    }

    await this.runCodexJob(job, scheduledFor, alreadyAdvanced);
  }

  private async runReminderJob(job: CronJobRecord, scheduledFor: string, alreadyAdvanced: boolean): Promise<void> {
    const claimed = await this.claimJobSlot(job, scheduledFor, alreadyAdvanced);

    try {
      await this.telegram.sendText(
        {
          chatId: claimed.targetChatId,
          threadId: claimed.targetThreadId
        },
        claimed.reminderText || `Scheduled reminder: ${claimed.label}`
      );

      await this.manager.saveJob({
        ...claimed,
        lastResult: `Reminder sent for ${scheduledFor}`,
        lastError: null,
        updatedAt: nowIso()
      });
      this.db.saveCronRun(this.manager.createRunRecord(job.id, scheduledFor, "sent", `Reminder sent: ${claimed.label}`));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await this.manager.saveJob({
        ...claimed,
        lastResult: null,
        lastError: message,
        updatedAt: nowIso()
      });
      this.db.saveCronRun(this.manager.createRunRecord(job.id, scheduledFor, "failed", message));
    }
  }

  private async runCodexJob(job: CronJobRecord, scheduledFor: string, alreadyAdvanced: boolean): Promise<void> {
    const context = job.executionContextSlug ? this.db.getContextBySlug(job.executionContextSlug) : null;
    if (!context) {
      const message = `Cron ${job.id} has no valid execution context`;
      await this.manager.saveJob({
        ...job,
        pendingRunAt: null,
        enabled: false,
        lastResult: null,
        lastError: message,
        updatedAt: nowIso()
      });
      await this.safeTelegramError(job, message);
      this.db.saveCronRun(this.manager.createRunRecord(job.id, scheduledFor, "failed", message));
      return;
    }

    if (this.dispatcher.isActive(context.slug)) {
      if (!job.pendingRunAt) {
        const queued = await this.manager.saveJob({
          ...job,
          pendingRunAt: scheduledFor,
          nextRunAt: nextCronRunAt(job.schedule, scheduledFor),
          lastResult: `Queued while ${context.slug} was busy`,
          lastError: null,
          updatedAt: nowIso()
        });
        this.db.saveCronRun(
          this.manager.createRunRecord(job.id, scheduledFor, "queued", `Queued while ${context.slug} was busy`)
        );

        if (job.schedule.type === "once" && !queued.nextRunAt) {
          await delay(0);
        }
      }

      return;
    }

    const claimed = await this.claimJobSlot(job, scheduledFor, alreadyAdvanced);
    const accepted = await this.dispatcher.dispatch(
      "resume",
      context,
      claimed.instruction || `Run scheduled cron job ${claimed.label}.`,
      {
        chatId: claimed.targetChatId,
        threadId: claimed.targetThreadId
      },
      {
        notifyAccepted: false,
        modelOverride: claimed.modelOverride,
        reasoningEffortOverride: claimed.reasoningEffortOverride,
        sourceLabel: `scheduled cron ${claimed.id}`
      }
    );

    if (!accepted.accepted) {
      await this.manager.saveJob({
        ...claimed,
        lastResult: null,
        lastError: accepted.message,
        updatedAt: nowIso()
      });
      this.db.saveCronRun(this.manager.createRunRecord(job.id, scheduledFor, "failed", accepted.message));
      if (accepted.message) {
        await this.safeTelegramError(job, accepted.message);
      }
      return;
    }

    await this.manager.saveJob({
      ...claimed,
      lastResult: `Codex dispatch accepted for ${scheduledFor}`,
      lastError: null,
      updatedAt: nowIso()
    });
    this.db.saveCronRun(
      this.manager.createRunRecord(job.id, scheduledFor, "dispatched", `Dispatched scheduled cron ${claimed.id}`)
    );
  }

  private async claimJobSlot(job: CronJobRecord, scheduledFor: string, alreadyAdvanced: boolean): Promise<CronJobRecord> {
    const nextRunAt = alreadyAdvanced ? job.nextRunAt : nextCronRunAt(job.schedule, scheduledFor);
    const claimed = await this.manager.saveJob({
      ...job,
      enabled: nextRunAt !== null || job.schedule.type !== "once",
      nextRunAt,
      pendingRunAt: null,
      lastRunAt: nowIso(),
      lastScheduledFor: scheduledFor,
      lastError: null,
      updatedAt: nowIso()
    });

    return claimed;
  }

  private async safeTelegramError(job: CronJobRecord, message: string): Promise<void> {
    try {
      await this.telegram.sendText(
        {
          chatId: job.targetChatId,
          threadId: job.targetThreadId
        },
        `Scheduled job ${job.label} failed: ${message}`
      );
    } catch {
      // Best-effort notification only.
    }
  }
}
