import { open } from "node:fs/promises";
import type { ContextRecord } from "../db";

const MANUAL_USAGE_LOG_TAIL_BYTES = 2 * 1024 * 1024;

export interface ManualUsageSummary {
  text: string;
  footer: string | null;
}

async function readLogTail(path: string, maxBytes = MANUAL_USAGE_LOG_TAIL_BYTES): Promise<{ text: string; truncated: boolean }> {
  const handle = await open(path, "r");
  try {
    const info = await handle.stat();
    const start = Math.max(0, info.size - maxBytes);
    const length = info.size - start;
    const buffer = Buffer.alloc(length);
    if (length > 0) {
      await handle.read(buffer, 0, length, start);
    }
    return { text: buffer.toString("utf8"), truncated: start > 0 };
  } finally {
    await handle.close();
  }
}

export async function summarizeManualUsage(context: ContextRecord): Promise<ManualUsageSummary> {
  if (!context.latestRunLogPath) {
    return { text: "No local run log recorded yet.", footer: null };
  }

  const file = Bun.file(context.latestRunLogPath);
  if (!(await file.exists())) {
    return { text: `Latest log path is recorded but missing: ${context.latestRunLogPath}`, footer: null };
  }

  const { text, truncated } = await readLogTail(context.latestRunLogPath);
  let inputTokens = 0;
  let cachedInputTokens = 0;
  let outputTokens = 0;
  let turns = 0;

  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed.startsWith("{")) {
      continue;
    }

    try {
      const parsed = JSON.parse(trimmed) as {
        type?: string;
        usage?: {
          input_tokens?: number;
          cached_input_tokens?: number;
          output_tokens?: number;
        };
      };

      if (parsed.type === "turn.completed" && parsed.usage) {
        turns += 1;
        // Log lines are untrusted input: a string or NaN token field would corrupt
        // the running totals through JS coercion.
        const tokenCount = (value: unknown): number => (typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : 0);
        inputTokens += tokenCount(parsed.usage.input_tokens);
        cachedInputTokens += tokenCount(parsed.usage.cached_input_tokens);
        outputTokens += tokenCount(parsed.usage.output_tokens);
      }
    } catch {
      continue;
    }
  }

  if (!turns) {
    return { text: `No structured token usage found in ${context.latestRunLogPath}.`, footer: null };
  }

  const textSummary = [
    `Adapter: manual`,
    truncated ? `Scope: last ${MANUAL_USAGE_LOG_TAIL_BYTES} bytes of log` : null,
    `Turns counted: ${turns}`,
    `Input tokens: ${inputTokens}`,
    `Cached input tokens: ${cachedInputTokens}`,
    `Output tokens: ${outputTokens}`,
    `Log: ${context.latestRunLogPath}`
  ].filter(Boolean).join("\n");
  const totalTokens = inputTokens + outputTokens;
  return {
    text: textSummary,
    footer: `tokens=${totalTokens} (in=${inputTokens} cached=${cachedInputTokens} out=${outputTokens})`
  };
}
