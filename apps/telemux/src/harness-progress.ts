import type { FactoryConfig } from "./config";
import type { ContextRecord } from "./db";
import type { TelegramBot, TelegramMessage, TelegramTarget } from "./telegram";

export type HarnessProgressEvent =
  | { kind: "session"; sessionId: string }
  | { kind: "turn_started" }
  | { kind: "turn_completed" }
  | { kind: "turn_failed"; message?: string | null }
  | { kind: "thinking" }
  | { kind: "command_started"; command?: string | null }
  | { kind: "command_completed"; command?: string | null; exitCode?: number | null }
  | { kind: "file_changed"; path?: string | null; action?: string | null }
  | { kind: "web_search" }
  | { kind: "tool"; label?: string | null }
  | { kind: "error"; message?: string | null };

const MAX_PROGRESS_LINES = 6;

function compactWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function truncate(value: string, maxChars: number): string {
  if (value.length <= maxChars) {
    return value;
  }
  return `${value.slice(0, Math.max(0, maxChars - 1)).trimEnd()}…`;
}

function stringField(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function numberField(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function recordField(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function nestedString(value: unknown, keys: string[]): string | null {
  let current: unknown = value;
  for (const key of keys) {
    const record = recordField(current);
    if (!record) {
      return null;
    }
    current = record[key];
  }
  return stringField(current);
}

function eventType(event: Record<string, unknown>): string {
  return stringField(event.type) || stringField(event.event) || stringField(event.kind) || "";
}

function itemType(item: Record<string, unknown>): string {
  return stringField(item.type) || stringField(item.kind) || stringField(item.name) || "";
}

function commandFromItem(item: Record<string, unknown>): string | null {
  const direct = stringField(item.command) || stringField(item.cmd);
  if (direct) {
    return direct;
  }

  const command = item.command;
  if (Array.isArray(command) && command.every((part) => typeof part === "string")) {
    return command.join(" ");
  }

  return null;
}

function pathFromItem(item: Record<string, unknown>): string | null {
  return stringField(item.path) || stringField(item.file) || stringField(item.file_path) || stringField(item.name);
}

function errorMessage(value: unknown): string | null {
  if (typeof value === "string") {
    return value.trim() || null;
  }
  const record = recordField(value);
  if (!record) {
    return null;
  }
  return stringField(record.message) || nestedString(record.error, ["message"]) || stringField(record.error);
}

export function safeProgressCommandLabel(command: string | null | undefined): string {
  const normalized = compactWhitespace(command || "");
  if (!normalized) {
    return "shell command";
  }

  if (/(token|secret|password|passwd|authorization|cookie|api[_-]?key|private[_ -]?key)/i.test(normalized)) {
    return "shell command (details hidden)";
  }

  return truncate(normalized, 120);
}

export function safeProgressDetail(value: string | null | undefined, fallback: string, maxChars = 120): string {
  const normalized = compactWhitespace(value || "");
  if (!normalized) {
    return fallback;
  }
  if (/(token|secret|password|passwd|authorization|cookie|api[_-]?key|private[_ -]?key)/i.test(normalized)) {
    return `${fallback} (details hidden)`;
  }
  return truncate(normalized, maxChars);
}

export function parseCodexProgressLine(line: string): HarnessProgressEvent[] {
  const trimmed = line.trim();
  if (!trimmed.startsWith("{")) {
    return [];
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed) as unknown;
  } catch {
    return [];
  }

  const event = recordField(parsed);
  if (!event) {
    return [];
  }

  const type = eventType(event);
  const item = recordField(event.item) || recordField(event.data) || null;
  const itemKind = item ? itemType(item) : "";
  const sessionId = stringField(event.session_id) || stringField(event.thread_id) || nestedString(event.thread, ["id"]);

  if (sessionId && /(?:session|thread)\.(?:started|resumed)/.test(type)) {
    return [{ kind: "session", sessionId }];
  }
  if (type === "turn.started") {
    return [{ kind: "turn_started" }];
  }
  if (type === "turn.completed") {
    return [{ kind: "turn_completed" }];
  }
  if (type === "turn.failed") {
    return [{ kind: "turn_failed", message: errorMessage(event.error) }];
  }
  if (type === "error") {
    return [{ kind: "error", message: errorMessage(event.message) || errorMessage(event.error) }];
  }
  if (/reasoning|thinking/i.test(type) || /reasoning|thinking/i.test(itemKind)) {
    return [{ kind: "thinking" }];
  }
  if (/web_search/i.test(type) || /web_search|web-search|search/i.test(itemKind)) {
    return [{ kind: "web_search" }];
  }

  if (item && /^item\.(?:started|completed)$/.test(type)) {
    if (/command|exec|shell|terminal/i.test(itemKind)) {
      return [
        type === "item.started"
          ? { kind: "command_started", command: commandFromItem(item) }
          : { kind: "command_completed", command: commandFromItem(item), exitCode: numberField(item.exit_code) ?? numberField(item.exitCode) }
      ];
    }

    if (/file|patch|diff/i.test(itemKind)) {
      return [
        {
          kind: "file_changed",
          path: pathFromItem(item),
          action: stringField(item.action) || (type === "item.started" ? "updating" : "updated")
        }
      ];
    }

    if (/tool|mcp/i.test(itemKind)) {
      return [{ kind: "tool", label: stringField(item.name) || itemKind }];
    }
  }

  return [];
}

export class CodexProgressLineParser {
  private buffer = "";

  push(chunk: string): HarnessProgressEvent[] {
    this.buffer += chunk;
    const lines = this.buffer.split(/\r?\n/);
    this.buffer = lines.pop() || "";
    const events: HarnessProgressEvent[] = [];
    for (const line of lines) {
      events.push(...parseCodexProgressLine(line));
    }
    if (this.buffer.length > 1024 * 1024) {
      this.buffer = this.buffer.slice(-1024 * 1024);
    }
    return events;
  }

  flush(): HarnessProgressEvent[] {
    const tail = this.buffer;
    this.buffer = "";
    return tail ? parseCodexProgressLine(tail) : [];
  }
}

interface ProgressTelegram {
  sendText(target: TelegramTarget, text: string): Promise<void>;
  sendTextMessage?(target: TelegramTarget, text: string): Promise<TelegramMessage | null>;
  editText?(target: TelegramTarget, messageId: number, text: string): Promise<void>;
}

export class HarnessProgressReporter {
  private stopped = false;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private messageId: number | null = null;
  private fallbackMessages = 0;
  private readonly startedAt = Date.now();
  private lastSentAt = 0;
  private lastEventAt = this.startedAt;
  private lastRendered = "";
  private currentAction = "Starting harness...";
  private readonly recent: string[] = [];

  constructor(
    private readonly telegram: ProgressTelegram,
    private readonly target: TelegramTarget,
    private readonly context: ContextRecord,
    private readonly config: FactoryConfig,
    private readonly mode: string
  ) {}

  start(): void {
    if (this.config.harnessStreamingMode === "off") {
      return;
    }
    this.schedule();
  }

  record(event: HarnessProgressEvent): void {
    if (this.config.harnessStreamingMode === "off" || this.stopped) {
      return;
    }

    const line = this.describeEvent(event);
    if (line) {
      this.currentAction = line;
      this.recent.push(line);
      while (this.recent.length > MAX_PROGRESS_LINES) {
        this.recent.shift();
      }
    }
    this.lastEventAt = Date.now();
    this.schedule();
  }

  async finish(status: "completed" | "failed" | "skipped" = "completed"): Promise<void> {
    if (this.config.harnessStreamingMode === "off") {
      this.stopTimer();
      this.stopped = true;
      return;
    }

    this.stopped = true;
    this.stopTimer();
    if (this.messageId === null) {
      return;
    }

    const label = status === "completed" ? "Completed. Final response below." : status === "failed" ? "Failed. Details below." : "Skipped. Details below.";
    this.currentAction = label;
    await this.flush(label).catch((error) => console.warn("telegram progress finish failed", error));
  }

  stop(): void {
    this.stopped = true;
    this.stopTimer();
  }

  private describeEvent(event: HarnessProgressEvent): string | null {
    switch (event.kind) {
      case "session":
        return `Session ${safeProgressDetail(event.sessionId, "started", 48)}`;
      case "turn_started":
        return "Harness started the turn.";
      case "turn_completed":
        return "Harness completed the turn.";
      case "turn_failed":
        return `Harness failed: ${safeProgressDetail(event.message, "error")}`;
      case "thinking":
        return "Thinking through the task.";
      case "command_started":
        return `Running: ${safeProgressCommandLabel(event.command)}`;
      case "command_completed":
        return event.exitCode && event.exitCode !== 0
          ? `Command finished with exit ${event.exitCode}: ${safeProgressCommandLabel(event.command)}`
          : `Command finished: ${safeProgressCommandLabel(event.command)}`;
      case "file_changed":
        return `${safeProgressDetail(event.action, "Updated", 32)} ${safeProgressDetail(event.path, "a file", 96)}`;
      case "web_search":
        return "Searching the web.";
      case "tool":
        return `Using ${safeProgressDetail(event.label, "a tool", 80)}.`;
      case "error":
        return `Error: ${safeProgressDetail(event.message, "harness error")}`;
    }
  }

  private schedule(): void {
    if (this.stopped || this.timer || this.config.harnessStreamingMode === "off") {
      return;
    }

    const now = Date.now();
    const delayMs =
      this.lastSentAt === 0
        ? Math.max(0, this.config.harnessStreamingInitialDelayMs - (now - this.startedAt))
        : Math.max(0, this.config.harnessStreamingUpdateIntervalMs - (now - this.lastSentAt));

    this.timer = setTimeout(() => {
      this.timer = null;
      void this.flush().finally(() => {
        if (!this.stopped) {
          this.schedule();
        }
      });
    }, delayMs);
  }

  private stopTimer(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  private render(overrideAction?: string): string {
    const elapsed = Math.max(1, Math.round((Date.now() - this.startedAt) / 1000));
    const lastAge = Math.max(0, Math.round((Date.now() - this.lastEventAt) / 1000));
    const lines = [
      `Working on ${this.context.slug} (${this.mode})`,
      `Machine: ${this.context.machine}`,
      `Status: ${overrideAction || this.currentAction}`,
      `Elapsed: ${elapsed}s${lastAge > 0 ? `; last activity ${lastAge}s ago` : ""}`
    ];

    if (this.recent.length) {
      lines.push("", "Recent:");
      for (const line of this.recent.slice(-4)) {
        lines.push(`- ${line}`);
      }
    }

    return truncate(lines.join("\n"), Math.max(300, this.config.harnessStreamingMaxChars));
  }

  private async flush(overrideAction?: string): Promise<void> {
    const text = this.render(overrideAction);
    if (text === this.lastRendered) {
      return;
    }

    if (this.messageId !== null && this.telegram.editText) {
      try {
        await this.telegram.editText(this.target, this.messageId, text);
        this.lastRendered = text;
        this.lastSentAt = Date.now();
        return;
      } catch (error) {
        console.warn("telegram progress edit failed; falling back to sparse sends", error);
      }
    }

    if (this.messageId === null && this.telegram.sendTextMessage) {
      const message = await this.telegram.sendTextMessage(this.target, text);
      this.messageId = message?.message_id ?? null;
      this.lastRendered = text;
      this.lastSentAt = Date.now();
      return;
    }

    if (this.fallbackMessages >= 3) {
      this.lastRendered = text;
      this.lastSentAt = Date.now();
      return;
    }
    this.fallbackMessages += 1;
    await this.telegram.sendText(this.target, text);
    this.lastRendered = text;
    this.lastSentAt = Date.now();
  }
}
