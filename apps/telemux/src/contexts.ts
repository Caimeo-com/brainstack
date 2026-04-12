import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { ContextKind, ContextRecord, ContextState, FactoryDb } from "./db";

export interface NewContextArgs {
  slug: string;
  machine: string;
  kind: ContextKind;
  state: ContextState;
  transport: string;
  target: string;
  rootPath: string;
  worktreePath: string;
  branchName: string | null;
  baseBranch: string | null;
  usageAdapter: string;
  chatId: number | null;
  threadId: number | null;
  lastError?: string | null;
}

function nowIso(): string {
  return new Date().toISOString();
}

function targetChanged(existing: ContextRecord | null, next: NewContextArgs): boolean {
  if (!existing) {
    return false;
  }

  return (
    existing.machine !== next.machine ||
    existing.kind !== next.kind ||
    existing.target !== next.target ||
    existing.rootPath !== next.rootPath ||
    existing.worktreePath !== next.worktreePath
  );
}

export function normalizeSlug(input: string): string {
  const slug = input.trim().toLowerCase();

  if (!/^[a-z0-9][a-z0-9-_]{1,62}$/.test(slug)) {
    throw new Error("Slug must match ^[a-z0-9][a-z0-9-_]{1,62}$");
  }

  return slug;
}

export function nextRecommendedAction(context: ContextRecord): string {
  switch (context.state) {
    case "pending":
      return `Wait for ${context.machine} to come online, then send plain text or rerun /newctx ${context.slug} ${context.machine} ${context.target}.`;
    case "archived":
      return "Use /bind to attach this topic to another context, or create a fresh one with /newctx.";
    case "error":
      return "Inspect /topicinfo or /tail, fix the workspace problem, then retry /run or /resume.";
    case "active":
      return context.codexSessionId
        ? "Send plain text or /resume to continue the current Codex session."
        : "Send plain text or /run to start a new Codex session in this topic.";
  }
}

export class ContextService {
  constructor(
    private readonly db: FactoryDb,
    private readonly defaultUsageAdapter: string,
    private readonly contextsDir: string
  ) {}

  listContexts(): ContextRecord[] {
    return this.db.listContexts();
  }

  getContextBySlug(slug: string): ContextRecord | null {
    return this.db.getContextBySlug(slug);
  }

  getContextByTopic(chatId: number, threadId: number | null): ContextRecord | null {
    return this.db.getContextByTopic(chatId, threadId);
  }

  createOrUpdateContext(args: NewContextArgs): ContextRecord {
    const slug = normalizeSlug(args.slug);
    const existing = this.db.getContextBySlug(slug);
    const timestamp = nowIso();
    const resetSession = targetChanged(existing, args);

    const context: ContextRecord = {
      slug,
      telegramChatId: existing?.telegramChatId ?? null,
      telegramThreadId: existing?.telegramThreadId ?? null,
      machine: args.machine.trim(),
      kind: args.kind,
      state: args.state,
      transport: args.transport,
      target: args.target.trim(),
      rootPath: args.rootPath.trim(),
      worktreePath: args.worktreePath.trim(),
      branchName: args.branchName,
      baseBranch: args.baseBranch,
      latestRunLogPath: resetSession ? null : (existing?.latestRunLogPath || null),
      lastSummary: resetSession ? null : (existing?.lastSummary || null),
      lastArtifacts: resetSession ? null : (existing?.lastArtifacts || null),
      codexSessionId: resetSession ? null : (existing?.codexSessionId || null),
      lastRunAt: resetSession ? null : (existing?.lastRunAt || null),
      usageAdapter: args.usageAdapter || existing?.usageAdapter || this.defaultUsageAdapter,
      harness: existing?.harness || null,
      harnessBin: existing?.harnessBin || null,
      modelOverride: existing?.modelOverride || null,
      reasoningEffortOverride: existing?.reasoningEffortOverride || null,
      lastError: args.lastError ?? (resetSession ? null : (existing?.lastError || null)),
      createdAt: existing?.createdAt || timestamp,
      updatedAt: timestamp
    };

    return this.saveContext(context);
  }

  bindContext(slug: string, chatId: number, threadId: number | null): ContextRecord | null {
    const context = this.db.bindContextToTopic(normalizeSlug(slug), chatId, threadId);
    if (context) {
      this.writeSnapshot(context);
    }
    return context;
  }

  detachTopic(chatId: number, threadId: number | null): void {
    const existing = this.db.getContextByTopic(chatId, threadId);
    this.db.detachTopic(chatId, threadId);
    if (existing) {
      const detached = this.db.getContextBySlug(existing.slug);
      if (detached) {
        this.writeSnapshot(detached);
      }
    }
  }

  updateState(context: ContextRecord, state: ContextState, lastError: string | null = null): ContextRecord {
    return this.saveContext({
      ...context,
      state,
      lastError,
      updatedAt: nowIso()
    });
  }

  saveContext(context: ContextRecord): ContextRecord {
    const saved = this.db.saveContext({
      ...context,
      updatedAt: nowIso()
    });
    this.writeSnapshot(saved);
    return saved;
  }

  private writeSnapshot(context: ContextRecord): void {
    const dir = resolve(this.contextsDir, context.slug);
    mkdirSync(dir, { recursive: true });
    writeFileSync(resolve(dir, "context.json"), `${JSON.stringify(context, null, 2)}\n`);
  }
}
