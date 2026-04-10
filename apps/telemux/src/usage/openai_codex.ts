import type { ContextRecord } from "../db";

export async function summarizeOpenAiCodexUsage(context: ContextRecord): Promise<string> {
  return [
    "Adapter: openai_codex",
    "Status: stub",
    "The pluggable adapter hook exists, but no direct OpenAI usage API integration is configured yet.",
    `Context: ${context.slug}`
  ].join("\n");
}
