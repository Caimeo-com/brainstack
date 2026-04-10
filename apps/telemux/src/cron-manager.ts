import { mkdir, rm, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import type { FactoryConfig } from "./config";
import {
  CONTEXT_CRONS_WORKSPACE_PATH,
  createCronJobId,
  CronJobChanges,
  CronJobDraft,
  CronJobRecord,
  CronJobSelector,
  CronManifestAction,
  CronRunRecord,
  formatCronJobsMarkdown,
  nextCronRunAt,
  parseCronManifest,
  scheduleSummary
} from "./cron-jobs";
import type { CodexReasoningEffort } from "./codex-runtime";
import type { ContextRecord, FactoryDb } from "./db";
import type { TelegramTarget } from "./telegram";
import type { WorkerService } from "./workers";

interface ManifestDefaults {
  context: ContextRecord | null;
  target: TelegramTarget;
}

interface JobScope {
  context: ContextRecord | null;
  target: TelegramTarget | null;
}

function nowIso(): string {
  return new Date().toISOString();
}

function uniqueJobs(jobs: CronJobRecord[]): CronJobRecord[] {
  const seen = new Set<string>();
  const ordered: CronJobRecord[] = [];

  for (const job of jobs) {
    if (seen.has(job.id)) {
      continue;
    }

    seen.add(job.id);
    ordered.push(job);
  }

  return ordered;
}

function compact(text: string | null, limit = 120): string {
  if (!text) {
    return "n/a";
  }

  const normalized = text.replace(/\s+/g, " ").trim();
  if (normalized.length <= limit) {
    return normalized;
  }

  return `${normalized.slice(0, limit - 1)}…`;
}

function matchesSelector(job: CronJobRecord, selector: CronJobSelector): boolean {
  if (selector.id && job.id === selector.id) {
    return true;
  }

  return Boolean(selector.label && job.label.toLowerCase() === selector.label.trim().toLowerCase());
}

function effectiveRuntime(
  job: CronJobRecord,
  context: ContextRecord | null
): { modelOverride: string | null; reasoningEffortOverride: CodexReasoningEffort | null } {
  return {
    modelOverride: job.modelOverride || context?.modelOverride || null,
    reasoningEffortOverride: job.reasoningEffortOverride || context?.reasoningEffortOverride || null
  };
}

export class CronManager {
  constructor(
    private readonly config: FactoryConfig,
    private readonly db: FactoryDb,
    private readonly workers: WorkerService
  ) {}

  listJobs(): CronJobRecord[] {
    return this.db.listCronJobs();
  }

  getJob(id: string): CronJobRecord | null {
    return this.db.getCronJob(id);
  }

  listRelevantJobs(context: ContextRecord | null, target: TelegramTarget | null): CronJobRecord[] {
    const jobs: CronJobRecord[] = [];

    if (context) {
      jobs.push(...this.db.listCronJobsForContext(context.slug));
    }

    if (target) {
      jobs.push(...this.db.listCronJobsForTarget(target.chatId, target.threadId));
    }

    return uniqueJobs(jobs);
  }

  formatJobsOverview(context: ContextRecord | null, target: TelegramTarget | null): string {
    const jobs = this.listRelevantJobs(context, target);
    if (!jobs.length) {
      return "No scheduled jobs are linked to this topic or context.";
    }

    return jobs
      .map((job) => {
        const boundContext = job.executionContextSlug ? this.db.getContextBySlug(job.executionContextSlug) : null;
        const runtime = effectiveRuntime(job, boundContext);
        return [
          `${job.id} | ${job.enabled ? "enabled" : "paused"} | ${job.kind} | ${job.label}`,
          `next=${job.nextRunAt || "none"} pending=${job.pendingRunAt || "none"}`,
          `schedule=${scheduleSummary(job.schedule)}`,
          `target=${job.targetChatId}:${job.targetThreadId ?? "none"} context=${job.executionContextSlug || "none"}`,
          `mode=model=${runtime.modelOverride || "default"} effort=${runtime.reasoningEffortOverride || "default"}`,
          job.lastError ? `error=${compact(job.lastError)}` : null
        ]
          .filter(Boolean)
          .join("\n");
      })
      .join("\n\n");
  }

  async createJob(draft: CronJobDraft, defaults: ManifestDefaults): Promise<CronJobRecord> {
    const createdAt = nowIso();
    const executionContextSlug = draft.executionContextSlug ?? defaults.context?.slug ?? null;
    const targetChatId = draft.targetChatId ?? defaults.target.chatId;
    const targetThreadId = draft.targetThreadId ?? defaults.target.threadId;

    if (targetChatId === null || targetChatId === undefined) {
      throw new Error("Cron jobs require a Telegram target chat id");
    }

    if (draft.kind === "codex" && executionContextSlug) {
      this.requireContextReference(executionContextSlug);
    }

    const job: CronJobRecord = {
      id: createCronJobId(draft.label, createdAt),
      label: draft.label,
      kind: draft.kind,
      enabled: draft.enabled ?? true,
      schedule: draft.schedule,
      nextRunAt: draft.enabled === false ? null : nextCronRunAt(draft.schedule, createdAt),
      pendingRunAt: null,
      lastRunAt: null,
      lastScheduledFor: null,
      executionContextSlug,
      targetChatId,
      targetThreadId: targetThreadId ?? null,
      instruction: draft.instruction || null,
      reminderText: draft.reminderText || null,
      modelOverride: draft.modelOverride || null,
      reasoningEffortOverride: draft.reasoningEffortOverride || null,
      lastResult: null,
      lastError: null,
      createdAt,
      updatedAt: createdAt
    };

    return this.saveJob(job);
  }

  async updateJob(job: CronJobRecord, changes: CronJobChanges, defaults?: ManifestDefaults): Promise<CronJobRecord> {
    const next: CronJobRecord = {
      ...job,
      updatedAt: nowIso()
    };

    if (changes.label !== undefined && changes.label) {
      next.label = changes.label;
    }

    if (changes.executionContextSlug !== undefined) {
      next.executionContextSlug =
        changes.executionContextSlug !== null
          ? this.requireContextReference(changes.executionContextSlug).slug
          : null;
    }

    if (changes.targetChatId !== undefined) {
      if (changes.targetChatId === null) {
        throw new Error("Cron jobs require a target chat id");
      }

      next.targetChatId = changes.targetChatId;
    }

    if (changes.targetThreadId !== undefined) {
      next.targetThreadId = changes.targetThreadId;
    }

    if (changes.instruction !== undefined) {
      next.instruction = changes.instruction;
    }

    if (changes.reminderText !== undefined) {
      next.reminderText = changes.reminderText;
    }

    if (changes.modelOverride !== undefined) {
      next.modelOverride = changes.modelOverride;
    }

    if (changes.reasoningEffortOverride !== undefined) {
      next.reasoningEffortOverride = changes.reasoningEffortOverride;
    }

    if (changes.enabled !== undefined && changes.enabled !== null) {
      next.enabled = changes.enabled;
      if (!next.enabled) {
        next.pendingRunAt = null;
      }
    }

    if (changes.schedule) {
      next.schedule = changes.schedule;
      next.pendingRunAt = null;
    }

    if (next.kind === "codex" && next.executionContextSlug) {
      this.requireContextReference(next.executionContextSlug);
    }

    if (next.kind === "reminder" && !next.reminderText) {
      throw new Error(`Reminder cron ${next.id} is missing reminder text`);
    }

    if (next.kind === "codex" && !next.instruction) {
      throw new Error(`Codex cron ${next.id} is missing instruction text`);
    }

    if ((changes.schedule || changes.enabled === true) && next.enabled) {
      next.nextRunAt = nextCronRunAt(next.schedule, nowIso());
    }

    if (defaults && next.targetChatId === null) {
      next.targetChatId = defaults.target.chatId;
    }

    return this.saveJob(next);
  }

  async pauseJob(job: CronJobRecord): Promise<CronJobRecord> {
    return this.saveJob({
      ...job,
      enabled: false,
      pendingRunAt: null,
      updatedAt: nowIso()
    });
  }

  async resumeJob(job: CronJobRecord): Promise<CronJobRecord> {
    return this.saveJob({
      ...job,
      enabled: true,
      nextRunAt: nextCronRunAt(job.schedule, nowIso()),
      pendingRunAt: null,
      updatedAt: nowIso()
    });
  }

  async deleteJob(job: CronJobRecord): Promise<void> {
    const relatedSlugs = this.relatedContextSlugs(job);
    this.db.deleteCronJob(job.id);
    await this.deleteSnapshot(job.id);
    await this.syncContextCronFiles(relatedSlugs);
  }

  resolveJob(selector: CronJobSelector, scope: JobScope): CronJobRecord {
    if (selector.id) {
      const job = this.db.getCronJob(selector.id);
      if (!job) {
        throw new Error(`Unknown cron job: ${selector.id}`);
      }

      return job;
    }

    const scoped = this.listRelevantJobs(scope.context, scope.target);
    const matches = scoped.filter((job) => matchesSelector(job, selector));
    if (!matches.length) {
      throw new Error(`No cron job matched label: ${selector.label}`);
    }

    if (matches.length > 1) {
      throw new Error(`Cron label is ambiguous in this scope: ${selector.label}`);
    }

    return matches[0];
  }

  async applyManifest(manifestText: string | null, defaults: ManifestDefaults): Promise<string[]> {
    const parsed = parseCronManifest(manifestText);
    const notes = [...parsed.skipped];

    for (const action of parsed.actions) {
      try {
        const note = await this.applyManifestAction(action, defaults);
        if (note) {
          notes.push(note);
        }
      } catch (error) {
        notes.push(error instanceof Error ? error.message : String(error));
      }
    }

    return notes;
  }

  async fastForwardMissedRuns(referenceIso: string): Promise<void> {
    for (const job of this.db.listCronJobs()) {
      if (!job.enabled || job.pendingRunAt || !job.nextRunAt || job.nextRunAt >= referenceIso) {
        continue;
      }

      const updated = {
        ...job,
        nextRunAt: nextCronRunAt(job.schedule, referenceIso),
        lastResult: `Skipped missed run before scheduler start at ${referenceIso}`,
        lastError: null,
        updatedAt: nowIso()
      };

      if (job.schedule.type === "once") {
        updated.enabled = false;
      }

      await this.saveJob(updated);
      this.db.saveCronRun({
        id: `cron-run-skip-${job.id}-${referenceIso.replace(/[^0-9]/g, "")}`,
        jobId: job.id,
        scheduledFor: job.nextRunAt,
        startedAt: referenceIso,
        finishedAt: referenceIso,
        status: "skipped",
        note: `Skipped missed run before scheduler start`
      });
    }
  }

  async saveJob(job: CronJobRecord): Promise<CronJobRecord> {
    const previous = this.db.getCronJob(job.id);
    const saved = this.db.saveCronJob({
      ...job,
      updatedAt: nowIso()
    });
    await this.writeSnapshot(saved);
    await this.syncContextCronFiles(uniqueJobs(
      [saved, previous].filter(Boolean) as CronJobRecord[]
    ).flatMap((entry) => this.relatedContextSlugs(entry)));
    return saved;
  }

  async syncContextCronFiles(slugs: string[]): Promise<void> {
    const uniqueSlugs = [...new Set(slugs.filter(Boolean))];
    for (const slug of uniqueSlugs) {
      const context = this.db.getContextBySlug(slug);
      if (!context) {
        continue;
      }

      const jobs = this.listRelevantJobs(context, context.telegramChatId === null ? null : {
        chatId: context.telegramChatId,
        threadId: context.telegramThreadId
      });
      const markdown = formatCronJobsMarkdown(context.slug, jobs);

      try {
        const ensured = await this.workers.ensureContext(context);
        if (!ensured.ok) {
          continue;
        }

        await this.workers.writeWorkspaceFile(context, CONTEXT_CRONS_WORKSPACE_PATH, markdown);
      } catch {
        // Best-effort mirror only.
      }
    }
  }

  createRunRecord(jobId: string, scheduledFor: string | null, status: CronRunRecord["status"], note: string | null): CronRunRecord {
    const startedAt = nowIso();
    return {
      id: `cron-run-${jobId}-${startedAt.replace(/[^0-9]/g, "")}`,
      jobId,
      scheduledFor,
      startedAt,
      finishedAt: startedAt,
      status,
      note
    };
  }

  requireContextReference(reference: string): ContextRecord {
    const direct = this.db.getContextBySlug(reference);
    if (direct) {
      return direct;
    }

    const normalized = reference.trim();
    const match = this.db
      .listContexts()
      .find((context) => context.rootPath === normalized || context.worktreePath === normalized || context.target === normalized);

    if (!match) {
      throw new Error(`Unknown context reference: ${reference}`);
    }

    return match;
  }

  private async applyManifestAction(action: CronManifestAction, defaults: ManifestDefaults): Promise<string | null> {
    switch (action.type) {
      case "create": {
        const job = await this.createJob(action.job, defaults);
        return `Cron created: ${job.id} (${job.label}) next=${job.nextRunAt || "none"}`;
      }
      case "update": {
        const existing = this.resolveJob(action.selector, defaults);
        const updated = await this.updateJob(existing, action.changes, defaults);
        return `Cron updated: ${updated.id} (${updated.label}) next=${updated.nextRunAt || "none"}`;
      }
      case "pause": {
        const existing = this.resolveJob(action.selector, defaults);
        const updated = await this.pauseJob(existing);
        return `Cron paused: ${updated.id} (${updated.label})`;
      }
      case "resume": {
        const existing = this.resolveJob(action.selector, defaults);
        const updated = await this.resumeJob(existing);
        return `Cron resumed: ${updated.id} (${updated.label}) next=${updated.nextRunAt || "none"}`;
      }
      case "delete": {
        const existing = this.resolveJob(action.selector, defaults);
        await this.deleteJob(existing);
        return `Cron deleted: ${existing.id} (${existing.label})`;
      }
    }
  }

  private relatedContextSlugs(job: CronJobRecord): string[] {
    const slugs: string[] = [];
    if (job.executionContextSlug) {
      slugs.push(job.executionContextSlug);
    }

    const boundTopicContext = this.db.getContextByTopic(job.targetChatId, job.targetThreadId);
    if (boundTopicContext) {
      slugs.push(boundTopicContext.slug);
    }

    return [...new Set(slugs)];
  }

  private async writeSnapshot(job: CronJobRecord): Promise<void> {
    await mkdir(this.config.cronSnapshotsDir, { recursive: true });
    await writeFile(resolve(this.config.cronSnapshotsDir, `${job.id}.json`), `${JSON.stringify(job, null, 2)}\n`);
  }

  private async deleteSnapshot(jobId: string): Promise<void> {
    await rm(resolve(this.config.cronSnapshotsDir, `${jobId}.json`), { force: true });
  }
}
