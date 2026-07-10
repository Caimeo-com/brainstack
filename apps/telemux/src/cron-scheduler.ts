import { CronManager } from "./cron-manager";
import { nextCronRunAt, type CronJobRecord, type CronRunRecord } from "./cron-jobs";
import type { FactoryConfig } from "./config";
import { reportCuratorStatus } from "./curator-report";
import type { ContextRecord, FactoryDb } from "./db";
import type { DispatchOptions, Dispatcher } from "./dispatcher";
import { isDeterministicRoutine } from "./routine-builtins";
import type { TelegramBot } from "./telegram";
import type { WorkerService } from "./workers";

function isCuratorJob(job: CronJobRecord): boolean {
  return job.kind === "codex" && !job.runner && job.label.toLowerCase() === "brain-curator";
}

function nowIso(): string {
  return new Date().toISOString();
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isContextBusyMessage(message: string): boolean {
  return /already has an active job|is busy/i.test(message);
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
    private readonly workers: WorkerService,
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
        await this.recoverStaleClaims(referenceIso);
        await this.manager.fastForwardMissedRuns(this.serviceStartedAt);
        this.started = true;
      }

      const dueJobs = this.db.listDueCronJobs(referenceIso);
      for (const due of dueJobs) {
        try {
          const current = this.db.getCronJob(due.id);
          if (!current) {
            continue;
          }
          await this.advanceWhilePending(current, referenceIso);

          const refreshed = this.db.getCronJob(due.id);
          if (!refreshed) {
            continue;
          }
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
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          if (message.includes("changed before it could be claimed")) {
            continue;
          }
          console.error(`scheduled cron ${due.id} failed`, error);
          const latest = this.db.getCronJob(due.id);
          if (latest) {
            await this.manager.saveJob({
              ...latest,
              lastResult: null,
              lastError: message,
              updatedAt: nowIso()
            });
            this.db.saveCronRun(this.manager.createRunRecord(due.id, due.pendingRunAt || due.nextRunAt || referenceIso, "failed", message));
            await this.safeTelegramError(latest, message);
          }
        }
      }
    } finally {
      this.running = false;
    }
  }

  private claimRunId(jobId: string, scheduledFor: string): string {
    return `cron-claim-${jobId}-${scheduledFor.replace(/[^0-9]/g, "")}`;
  }

  /**
   * Curator runs report their outcome to braind so the wiki home and
   * `brainctl curator status` reflect installed/last/next run state.
   */
  private curatorFinishHook(job: CronJobRecord, scheduledFor: string): DispatchOptions["onFinished"] | undefined {
    if (!isCuratorJob(job)) {
      return undefined;
    }
    const startedAt = nowIso();
    return async (status) => {
      const refreshed = this.db.getCronJob(job.id);
      await reportCuratorStatus(this.config, {
        installed: true,
        last_run_id: this.claimRunId(job.id, scheduledFor),
        last_run_started_at: startedAt,
        last_run_finished_at: nowIso(),
        last_run_ok: status === "finished",
        last_run_failures: status === "finished" ? [] : [`curator run ${status}; see ${this.db.getContextBySlug(job.executionContextSlug || "")?.latestRunLogPath || "telemux logs"}`],
        next_run_at: refreshed?.nextRunAt ?? null,
        // Material older than this run's start has been reviewed on success.
        ...(status === "finished" ? { cursor: startedAt } : {})
      });
    };
  }

  private buildClaimRun(job: CronJobRecord, scheduledFor: string): CronRunRecord {
    const startedAt = nowIso();
    return {
      id: this.claimRunId(job.id, scheduledFor),
      jobId: job.id,
      scheduledFor,
      startedAt,
      finishedAt: null,
      status: "claimed",
      note: `Claimed scheduled run ${scheduledFor}`
    };
  }

  private finishClaimRun(jobId: string, scheduledFor: string, status: CronRunRecord["status"], note: string | null): void {
    const existing = this.db.listCronRuns(jobId, 100).find((run) => run.id === this.claimRunId(jobId, scheduledFor));
    const finishedAt = nowIso();
    this.db.saveCronRun({
      id: this.claimRunId(jobId, scheduledFor),
      jobId,
      scheduledFor,
      startedAt: existing?.startedAt || finishedAt,
      finishedAt,
      status,
      note
    });
  }

  private async recoverStaleClaims(referenceIso: string): Promise<void> {
    for (const run of this.db.listClaimedCronRunsBefore(referenceIso)) {
      const job = this.db.getCronJob(run.jobId);
      if (!job || !run.scheduledFor) {
        continue;
      }

      // A claimed-but-unfinished run is ambiguous: the side effect (reminder send,
      // dispatch) may or may not have happened before the crash. Say so explicitly
      // so the operator verifies before re-running instead of assuming "not sent".
      const message = `Recovered unfinished claimed run ${run.scheduledFor}; the action may have POSSIBLY COMPLETED before restart (e.g. reminder possibly sent). Job paused for operator review; verify delivery before re-running.`;
      await this.manager.saveJob({
        ...job,
        enabled: false,
        pendingRunAt: run.scheduledFor,
        lastResult: null,
        lastError: message,
        updatedAt: nowIso()
      });
      this.finishClaimRun(job.id, run.scheduledFor, "failed", message);
    }
  }

  async runJobNow(job: CronJobRecord, referenceIso = nowIso()): Promise<string> {
    if (job.kind === "reminder") {
      const manual = this.manualClaimedJob(job, referenceIso);
      const claimed = await this.manager.saveJob({
        ...manual.job,
        lastRunAt: referenceIso,
        lastScheduledFor: manual.scheduledFor,
        lastResult: `Manual reminder started for ${referenceIso}`,
        lastError: null,
        updatedAt: nowIso()
      });
      let sentConfirmed = false;
      try {
        await this.telegram.sendText(
          {
            chatId: claimed.targetChatId,
            threadId: claimed.targetThreadId
          },
          claimed.reminderText || `Scheduled reminder: ${claimed.label}`
        );
        sentConfirmed = true;
        // Durably record the confirmed send before bookkeeping that could throw.
        this.db.saveCronRun(this.manager.createRunRecord(job.id, manual.scheduledFor, "sent", `Manual reminder sent: ${job.label}`));
        await this.saveClaimedOutcome(claimed, {
          lastRunAt: referenceIso,
          lastScheduledFor: manual.scheduledFor,
          lastResult: `Manual reminder sent for ${referenceIso}`,
          lastError: null
        });
        return `Cron run sent: ${job.id} (${job.label})`;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        if (sentConfirmed) {
          // Never downgrade a provider-confirmed send because bookkeeping failed.
          console.error(`manual reminder post-send bookkeeping failed for ${job.id}`, error);
          await this.saveClaimedOutcome(claimed, {
            lastRunAt: referenceIso,
            lastScheduledFor: manual.scheduledFor,
            lastResult: `Manual reminder sent for ${referenceIso}`,
            lastError: `Post-send bookkeeping failed: ${message}`
          }).catch(() => undefined);
          return `Cron run sent: ${job.id} (${job.label})`;
        }
        const saved = await this.saveClaimedOutcome(claimed, {
          lastRunAt: referenceIso,
          lastScheduledFor: manual.scheduledFor,
          lastResult: null,
          lastError: message
        });
        this.db.saveCronRun(this.manager.createRunRecord(job.id, manual.scheduledFor, saved ? "failed" : "skipped", message));
        throw error;
      }
    }

    if (isDeterministicRoutine(job.runner, "update-check")) {
      return this.runDeterministicUpdateCheckNow(job, referenceIso);
    }

    const context = job.executionContextSlug ? this.db.getContextBySlug(job.executionContextSlug) : null;
    if (!context) {
      throw new Error(`Cron ${job.id} has no valid execution context`);
    }
    if (context.state === "archived") {
      throw new Error(`Cron ${job.id} context ${context.slug} is archived; rebind the context before running this job.`);
    }

    if (this.dispatcher.isActive(context.slug)) {
      throw new Error(`Context ${context.slug} is busy; retry after the active run completes.`);
    }

    const manual = this.manualClaimedJob(job, referenceIso);
    const claimed = await this.manager.saveJob({
      ...manual.job,
      lastRunAt: referenceIso,
      lastScheduledFor: manual.scheduledFor,
      lastResult: `Manual Codex dispatch started for ${referenceIso}`,
      lastError: null,
      updatedAt: nowIso()
    });
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
        allowQueue: false,
        modelOverride: claimed.modelOverride,
        reasoningEffortOverride: claimed.reasoningEffortOverride,
        sourceLabel: `manual cron run ${claimed.id}`,
        runOrigin: "manual",
        routineName: claimed.label,
        routineJobId: claimed.id,
        scheduledFor: manual.scheduledFor,
        onFinished: this.curatorFinishHook(claimed, manual.scheduledFor)
      }
    );

    if (!accepted.accepted) {
      if (isContextBusyMessage(accepted.message)) {
        await this.queueJobWhileBusy(claimed, context, manual.scheduledFor);
        return accepted.message || `Cron run failed: ${job.id} (${job.label})`;
      }
      const saved = await this.saveClaimedOutcome(claimed, {
        lastRunAt: referenceIso,
        lastScheduledFor: manual.scheduledFor,
        lastResult: null,
        lastError: accepted.message
      });
      if (saved || this.db.getCronJob(job.id)) {
        this.db.saveCronRun(this.manager.createRunRecord(job.id, manual.scheduledFor, saved ? "failed" : "skipped", accepted.message));
      }
      return accepted.message || `Cron run failed: ${job.id} (${job.label})`;
    }

    const saved = await this.saveClaimedOutcome(claimed, {
      lastRunAt: referenceIso,
      lastScheduledFor: manual.scheduledFor,
      lastResult: `Manual Codex dispatch accepted for ${referenceIso}`,
      lastError: null
    });
    if (!saved) {
      return `Cron run skipped because ${job.id} changed during dispatch.`;
    }
    this.db.saveCronRun(this.manager.createRunRecord(job.id, manual.scheduledFor, "dispatched", `Manual dispatch ${job.id}`));
    return `Cron run dispatched: ${job.id} (${job.label})`;
  }

  private manualClaimedJob(job: CronJobRecord, referenceIso: string): { job: CronJobRecord; scheduledFor: string } {
    const scheduledFor = job.pendingRunAt || (job.nextRunAt && job.nextRunAt <= referenceIso ? job.nextRunAt : referenceIso);
    const shouldConsumeSlot = Boolean(job.pendingRunAt || (job.nextRunAt && job.nextRunAt <= referenceIso));
    if (!shouldConsumeSlot) {
      return { job, scheduledFor };
    }

    let nextRunAt = job.pendingRunAt ? job.nextRunAt : nextCronRunAt(job.schedule, scheduledFor);
    while (nextRunAt && nextRunAt <= referenceIso) {
      nextRunAt = nextCronRunAt(job.schedule, nextRunAt);
    }

    return {
      scheduledFor,
      job: {
        ...job,
        enabled: nextRunAt !== null || job.schedule.type !== "once",
        nextRunAt,
        pendingRunAt: null
      }
    };
  }

  private async runDeterministicUpdateCheckNow(job: CronJobRecord, referenceIso: string): Promise<string> {
    const context = job.executionContextSlug ? this.db.getContextBySlug(job.executionContextSlug) : null;
    if (!context) {
      throw new Error(`Cron ${job.id} has no valid execution context`);
    }
    if (context.state === "archived") {
      throw new Error(`Cron ${job.id} context ${context.slug} is archived; rebind the context before running this job.`);
    }

    if (this.dispatcher.isActive(context.slug)) {
      throw new Error(`Context ${context.slug} is busy; retry after the active run completes.`);
    }

    const manual = this.manualClaimedJob(job, referenceIso);
    const claimed = await this.manager.saveJob({
      ...manual.job,
      lastRunAt: referenceIso,
      lastScheduledFor: manual.scheduledFor,
      lastResult: `Deterministic update check started for ${referenceIso}`,
      lastError: null,
      updatedAt: nowIso()
    });
    const logPath = `${this.config.logsDir}/cron-${job.id}-${Date.now()}.log`;
    const runnableContext = this.revalidateRunnableContext(context, claimed);
    if (!runnableContext) {
      const message = `Cron ${job.id} context ${context.slug} is no longer active`;
      await this.saveClaimedOutcome(claimed, {
        lastRunAt: referenceIso,
        lastScheduledFor: manual.scheduledFor,
        lastResult: null,
        lastError: message
      });
      this.db.saveCronRun(this.manager.createRunRecord(job.id, manual.scheduledFor, "failed", message));
      throw new Error(message);
    }

    let result;
    try {
      result = await this.dispatcher.withContextLock(runnableContext, () => this.workers.runUpdateCheck(runnableContext, logPath), {
        mode: "update-check",
        logPath,
        target: { chatId: job.targetChatId, threadId: job.targetThreadId }
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (isContextBusyMessage(message)) {
        await this.queueJobWhileBusy(claimed, context, manual.scheduledFor);
      }
      throw error;
    }
    if (!result.ok) {
      const message = result.stderr || result.stdout || `update-check exited ${result.exitCode}`;
      await this.saveClaimedOutcome(claimed, {
        lastResult: null,
        lastError: message,
        lastRunAt: referenceIso,
        lastScheduledFor: manual.scheduledFor
      });
      this.db.saveCronRun(this.manager.createRunRecord(job.id, manual.scheduledFor, "failed", message));
      throw new Error(message);
    }

    const saved = await this.saveClaimedOutcome(claimed, {
      lastResult: `Deterministic update check completed${result.reportPath ? `: ${result.reportPath}` : ""}`,
      lastError: null,
      lastRunAt: referenceIso,
      lastScheduledFor: manual.scheduledFor
    });
    if (!saved) {
      const message = `Cron ${job.id} changed during update-check; result was not written`;
      if (this.db.getCronJob(job.id)) {
        this.db.saveCronRun(this.manager.createRunRecord(job.id, manual.scheduledFor, "skipped", message));
      }
      throw new Error(message);
    }
    // Only mark the run "sent" after the Telegram summary is actually delivered.
    try {
      await this.telegram.sendText(
        {
          chatId: job.targetChatId,
          threadId: job.targetThreadId
        },
        this.formatUpdateCheckResult(result)
      );
      this.db.saveCronRun(
        this.manager.createRunRecord(job.id, manual.scheduledFor, "sent", `Deterministic update check ${result.reportPath || ""}`.trim())
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      this.db.saveCronRun(
        this.manager.createRunRecord(
          job.id,
          manual.scheduledFor,
          "completed_notification_failed",
          `Update check completed but Telegram notification failed: ${message}. Artifact: ${result.reportPath || "n/a"}`
        )
      );
      await this.saveClaimedOutcome(saved, {
        lastResult: saved.lastResult,
        lastError: `Telegram notification failed: ${message}`
      }).catch(() => undefined);
    }
    return `Cron run completed: ${job.id} (${job.label})${result.reportPath ? ` artifact=${result.reportPath}` : ""}`;
  }

  private formatUpdateCheckResult(result: { stdout: string; reportPath: string | null }): string {
    const body = result.stdout.replace(/^BRAINSTACK_UPDATE_REPORT=.*$/m, "").trim();
    const compactBody = body.length > 3200 ? `${body.slice(0, 3200)}\n\n... truncated; see artifact.` : body;
    const heading = body.includes("- status: degraded") ? "Update check degraded." : "Update check complete.";
    return [heading, result.reportPath ? `Artifact: ${result.reportPath}` : null, compactBody].filter(Boolean).join("\n\n");
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
    let sentConfirmed = false;

    try {
      await this.telegram.sendText(
        {
          chatId: claimed.targetChatId,
          threadId: claimed.targetThreadId
        },
        claimed.reminderText || `Scheduled reminder: ${claimed.label}`
      );
      sentConfirmed = true;

      // Telegram confirmed the send: durably mark the run "sent" before any further
      // bookkeeping that could fail, so a crash here cannot make recovery treat this
      // run as "not sent" and let the operator replay a duplicate reminder.
      this.finishClaimRun(job.id, scheduledFor, "sent", `Reminder sent: ${claimed.label}`);

      await this.saveClaimedOutcome(claimed, {
        lastResult: `Reminder sent for ${scheduledFor}`,
        lastError: null
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (sentConfirmed) {
        // The provider confirmed delivery; never downgrade the durable "sent" status
        // because post-send bookkeeping failed. Record the bookkeeping failure only.
        console.error(`reminder post-send bookkeeping failed for ${job.id}`, error);
        await this.saveClaimedOutcome(claimed, {
          lastResult: `Reminder sent for ${scheduledFor}`,
          lastError: `Post-send bookkeeping failed: ${message}`
        }).catch(() => undefined);
        return;
      }
      const saved = await this.saveClaimedOutcome(claimed, {
        lastResult: null,
        lastError: message
      });
      if (saved || this.db.getCronJob(job.id)) {
        this.finishClaimRun(job.id, scheduledFor, saved ? "failed" : "skipped", message);
      }
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

    if (isDeterministicRoutine(job.runner, "update-check")) {
      await this.runScheduledDeterministicUpdateCheck(job, context, scheduledFor, alreadyAdvanced);
      return;
    }

    if (context.state === "archived") {
      await this.pauseJobForArchivedContext(job, context, scheduledFor);
      return;
    }

    if (this.dispatcher.isActive(context.slug)) {
      await this.queueJobWhileBusy(job, context, scheduledFor);
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
        allowQueue: false,
        modelOverride: claimed.modelOverride,
        reasoningEffortOverride: claimed.reasoningEffortOverride,
        sourceLabel: `scheduled cron ${claimed.id}`,
        runOrigin: "scheduled",
        routineName: claimed.label,
        routineJobId: claimed.id,
        scheduledFor,
        onFinished: this.curatorFinishHook(claimed, scheduledFor)
      }
    );

    if (!accepted.accepted) {
      if (isContextBusyMessage(accepted.message)) {
        await this.queueJobWhileBusy(claimed, context, scheduledFor);
        this.finishClaimRun(job.id, scheduledFor, "queued", `Queued while ${context.slug} was busy`);
        return;
      }
      const saved = await this.saveClaimedOutcome(claimed, {
        lastResult: null,
        lastError: accepted.message
      });
      if (saved || this.db.getCronJob(job.id)) {
        this.finishClaimRun(job.id, scheduledFor, saved ? "failed" : "skipped", accepted.message);
      }
      if (accepted.message) {
        await this.safeTelegramError(job, accepted.message);
      }
      return;
    }

    const saved = await this.saveClaimedOutcome(claimed, {
      lastResult: `Codex dispatch accepted for ${scheduledFor}`,
      lastError: null
    });
    if (!saved) {
      if (this.db.getCronJob(job.id)) {
        this.finishClaimRun(job.id, scheduledFor, "skipped", `Cron ${job.id} changed during dispatch; result was not written`);
      }
      return;
    }
    this.finishClaimRun(job.id, scheduledFor, "dispatched", `Dispatched scheduled cron ${claimed.id}`);
  }

  private async runScheduledDeterministicUpdateCheck(
    job: CronJobRecord,
    context: ContextRecord,
    scheduledFor: string,
    alreadyAdvanced: boolean
  ): Promise<void> {
    if (context.state === "archived") {
      await this.pauseJobForArchivedContext(job, context, scheduledFor);
      return;
    }

    if (this.dispatcher.isActive(context.slug)) {
      await this.queueJobWhileBusy(job, context, scheduledFor);
      return;
    }

    const claimed = await this.claimJobSlot(job, scheduledFor, alreadyAdvanced);
    try {
      const logPath = `${this.config.logsDir}/cron-${job.id}-${Date.now()}.log`;
      const runnableContext = this.revalidateRunnableContext(context, claimed);
      if (!runnableContext) {
        throw new Error(`Cron ${job.id} context ${context.slug} is no longer active`);
      }

      const result = await this.dispatcher.withContextLock(runnableContext, () => this.workers.runUpdateCheck(runnableContext, logPath), {
        mode: "update-check",
        logPath,
        target: { chatId: job.targetChatId, threadId: job.targetThreadId }
      });
      if (!result.ok) {
        const message = result.stderr || result.stdout || `update-check exited ${result.exitCode}`;
        if (result.exitCode === 89 && message.includes("context archived before update-check launch")) {
          await this.pauseClaimedJob(claimed, scheduledFor, message);
          return;
        }
        throw new Error(message);
      }

      const saved = await this.saveClaimedOutcome(claimed, {
        lastResult: `Deterministic update check completed${result.reportPath ? `: ${result.reportPath}` : ""}`,
        lastError: null
      });
      if (!saved) {
        if (this.db.getCronJob(job.id)) {
          this.finishClaimRun(job.id, scheduledFor, "skipped", `Cron ${job.id} changed during update-check; result was not written`);
        }
        return;
      }

      // Only mark the run "sent" after the Telegram summary is actually delivered;
      // artifact completion and notification delivery are separate outcomes.
      try {
        await this.telegram.sendText(
          {
            chatId: job.targetChatId,
            threadId: job.targetThreadId
          },
          this.formatUpdateCheckResult(result)
        );
        this.finishClaimRun(job.id, scheduledFor, "sent", `Deterministic update check ${result.reportPath || ""}`.trim());
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        this.finishClaimRun(
          job.id,
          scheduledFor,
          "completed_notification_failed",
          `Update check completed but Telegram notification failed: ${message}. Artifact: ${result.reportPath || "n/a"}`
        );
        await this.saveClaimedOutcome(saved, {
          lastResult: saved.lastResult,
          lastError: `Telegram notification failed: ${message}`
        });
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      if (isContextBusyMessage(errorMessage)) {
        await this.queueJobWhileBusy(claimed, context, scheduledFor);
        this.finishClaimRun(job.id, scheduledFor, "queued", `Queued while ${context.slug} was busy`);
        return;
      }
      await this.saveClaimedOutcome(claimed, {
        lastResult: null,
        lastError: errorMessage
      });
      if (this.db.getCronJob(job.id)) {
        this.finishClaimRun(job.id, scheduledFor, "failed", errorMessage);
      }
      await this.safeTelegramError(job, errorMessage);
    }
  }

  private async claimJobSlot(job: CronJobRecord, scheduledFor: string, alreadyAdvanced: boolean): Promise<CronJobRecord> {
    const current = this.db.getCronJob(job.id);
    if (!current) {
      throw new Error(`Cron ${job.id} no longer exists`);
    }
    if (current.updatedAt !== job.updatedAt) {
      throw new Error(`Cron ${job.id} changed before it could be claimed`);
    }
    const nextRunAt = alreadyAdvanced ? job.nextRunAt : nextCronRunAt(job.schedule, scheduledFor);
    const updatedJob = {
      ...job,
      enabled: nextRunAt !== null || job.schedule.type !== "once",
      nextRunAt,
      pendingRunAt: null,
      lastRunAt: nowIso(),
      lastScheduledFor: scheduledFor,
      lastError: null,
      updatedAt: nowIso()
    };
    const claimed = this.db.claimCronJobSlotIfUnchanged(updatedJob, job.updatedAt, this.buildClaimRun(updatedJob, scheduledFor));
    if (!claimed) {
      throw new Error(`Cron ${job.id} changed before it could be claimed`);
    }

    return claimed;
  }

  private async pauseJobForArchivedContext(job: CronJobRecord, context: ContextRecord, scheduledFor: string): Promise<void> {
    const current = this.db.getCronJob(job.id);
    if (!current || current.updatedAt !== job.updatedAt) {
      return;
    }

    const message = `Context ${context.slug} is archived; cron job paused with pending run ${scheduledFor}`;
    const saved = await this.manager.saveJob({
      ...current,
      enabled: false,
      pendingRunAt: scheduledFor,
      lastResult: null,
      lastError: message,
      updatedAt: nowIso()
    });
    this.db.saveCronRun(this.manager.createRunRecord(saved.id, scheduledFor, "skipped", message));
    await this.safeTelegramError(saved, message);
  }

  private async pauseClaimedJob(claimed: CronJobRecord, scheduledFor: string, message: string): Promise<void> {
    const current = this.db.getCronJob(claimed.id);
    if (!current || current.updatedAt !== claimed.updatedAt) {
      this.finishClaimRun(claimed.id, scheduledFor, "skipped", message);
      return;
    }

    const saved = await this.manager.saveJob({
      ...current,
      enabled: false,
      pendingRunAt: scheduledFor,
      lastResult: null,
      lastError: message,
      updatedAt: nowIso()
    });
    this.finishClaimRun(saved.id, scheduledFor, "skipped", message);
    await this.safeTelegramError(saved, message);
  }

  private async queueJobWhileBusy(job: CronJobRecord, context: ContextRecord, scheduledFor: string): Promise<void> {
    const current = this.db.getCronJob(job.id);
    if (!current || current.updatedAt !== job.updatedAt) {
      return;
    }

    if (!current.enabled && !current.pendingRunAt && current.lastScheduledFor !== scheduledFor) {
      return;
    }

    if (current.pendingRunAt === scheduledFor) {
      return;
    }

    const nextRunAt = current.nextRunAt && current.nextRunAt > scheduledFor ? current.nextRunAt : nextCronRunAt(current.schedule, scheduledFor);
    const queued = await this.manager.saveJob({
      ...current,
      enabled: true,
      pendingRunAt: scheduledFor,
      nextRunAt,
      lastResult: `Queued while ${context.slug} was busy`,
      lastError: null,
      updatedAt: nowIso()
    });
    this.db.saveCronRun(this.manager.createRunRecord(job.id, scheduledFor, "queued", `Queued while ${context.slug} was busy`));

    if (current.schedule.type === "once" && !queued.nextRunAt) {
      await delay(0);
    }
  }

  private revalidateRunnableContext(context: ContextRecord, job: CronJobRecord): ContextRecord | null {
    const fresh = this.db.getContextBySlug(context.slug);
    if (!fresh || fresh.state === "archived") {
      return null;
    }

    if (job.executionContextSlug && fresh.slug !== job.executionContextSlug) {
      return null;
    }

    return fresh;
  }

  private async saveClaimedOutcome(
    claimed: CronJobRecord,
    changes: Pick<CronJobRecord, "lastResult" | "lastError"> & Partial<Pick<CronJobRecord, "lastRunAt" | "lastScheduledFor">>
  ): Promise<CronJobRecord | null> {
    const current = this.db.getCronJob(claimed.id);
    if (
      !current ||
      current.updatedAt !== claimed.updatedAt ||
      current.kind !== claimed.kind ||
      JSON.stringify(current.schedule) !== JSON.stringify(claimed.schedule) ||
      current.runner !== claimed.runner ||
      current.executionContextSlug !== claimed.executionContextSlug ||
      current.targetChatId !== claimed.targetChatId ||
      current.targetThreadId !== claimed.targetThreadId
    ) {
      return null;
    }

    return this.manager.saveJob({
      ...current,
      lastRunAt: changes.lastRunAt ?? current.lastRunAt,
      lastScheduledFor: changes.lastScheduledFor ?? current.lastScheduledFor,
      lastResult: changes.lastResult,
      lastError: changes.lastError,
      updatedAt: nowIso()
    });
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
