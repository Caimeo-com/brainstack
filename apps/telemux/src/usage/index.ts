import type { ContextRecord } from "../db";
import { summarizeManualUsage } from "./manual";
import { summarizeOpenAiCodexUsage } from "./openai_codex";

export interface UsageSummary {
  adapter: string;
  text: string;
}

export async function summarizeUsage(context: ContextRecord): Promise<UsageSummary> {
  if (context.usageAdapter === "openai_codex") {
    return {
      adapter: "openai_codex",
      text: await summarizeOpenAiCodexUsage(context)
    };
  }

  return {
    adapter: "manual",
    text: await summarizeManualUsage(context)
  };
}
