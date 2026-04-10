import type { ContextRecord } from "../db";

export async function summarizeManualUsage(context: ContextRecord): Promise<string> {
  if (!context.latestRunLogPath) {
    return "No local run log recorded yet.";
  }

  const file = Bun.file(context.latestRunLogPath);
  if (!(await file.exists())) {
    return `Latest log path is recorded but missing: ${context.latestRunLogPath}`;
  }

  const text = await file.text();
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
        inputTokens += parsed.usage.input_tokens || 0;
        cachedInputTokens += parsed.usage.cached_input_tokens || 0;
        outputTokens += parsed.usage.output_tokens || 0;
      }
    } catch {
      continue;
    }
  }

  if (!turns) {
    return `No structured token usage found in ${context.latestRunLogPath}.`;
  }

  return [
    `Adapter: manual`,
    `Turns counted: ${turns}`,
    `Input tokens: ${inputTokens}`,
    `Cached input tokens: ${cachedInputTokens}`,
    `Output tokens: ${outputTokens}`,
    `Log: ${context.latestRunLogPath}`
  ].join("\n");
}
