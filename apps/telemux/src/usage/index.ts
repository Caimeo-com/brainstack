import type { ContextRecord } from "../db";
import { summarizeManualUsage } from "./manual";

export interface UsageSummary {
  adapter: string;
  text: string;
}

export async function summarizeUsage(context: ContextRecord): Promise<UsageSummary> {
  const adapter = context.usageAdapter.trim() || "manual";
  if (adapter !== "manual") {
    return {
      adapter,
      text: [
        `Unsupported usage adapter: ${adapter}`,
        "Brainstack only supports the manual local run-log parser now.",
        "Set FACTORY_USAGE_ADAPTER=manual and recreate or rebind this context to use current usage reporting."
      ].join("\n")
    };
  }

  return {
    adapter: "manual",
    text: await summarizeManualUsage(context)
  };
}
