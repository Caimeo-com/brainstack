import { formatCodexModeSummary } from "./codex-runtime";
import type { ContextRecord } from "./db";
import type { ControlMetaKind, PreDispatchClassification } from "./pre-dispatch-router";
import { summarizeUsage } from "./usage";

export interface ControlMetaRuntimeSummary {
  harness: string;
  model: string;
  effort: string;
}

export interface ControlMetaResponseInput {
  context: ContextRecord;
  classification: PreDispatchClassification;
  busy: boolean;
  runtime: ControlMetaRuntimeSummary | null;
  contextStatus: string;
}

const LOCAL_HANDLING_NOTE = "Handled locally by telemux; no harness run was started for this message.";

export function resolveControlMetaKind(classification: PreDispatchClassification): ControlMetaKind {
  return classification.controlKind || "status";
}

export async function formatControlMetaResponse(input: ControlMetaResponseInput): Promise<string> {
  const { context, runtime } = input;
  const busy = input.busy ? "yes" : "no";
  const kind = resolveControlMetaKind(input.classification);

  if (kind === "ack") {
    return ["Noted.", LOCAL_HANDLING_NOTE].join("\n");
  }

  if (kind === "usage") {
    const usage = await summarizeUsage(context);
    return ["Usage check.", `Context: ${context.slug}`, LOCAL_HANDLING_NOTE, "", "Latest completed harness run:", usage.text].join(
      "\n"
    );
  }

  if (kind === "latency") {
    const usage = await summarizeUsage(context);
    return [
      "Latest run diagnostics.",
      `Context: ${context.slug}`,
      `Busy: ${busy}`,
      `Last run: ${context.lastRunAt || "never"}`,
      `Log: ${context.latestRunLogPath || "n/a"}`,
      LOCAL_HANDLING_NOTE,
      "",
      "Recorded usage for the latest completed harness run:",
      usage.text
    ].join("\n");
  }

  if (kind === "status") {
    return [input.contextStatus, "", LOCAL_HANDLING_NOTE].join("\n");
  }

  return [
    "Up.",
    `Context: ${context.slug}`,
    `Machine: ${context.machine}`,
    `State: ${context.state}`,
    `Busy: ${busy}`,
    runtime ? `Harness: ${runtime.harness}` : null,
    runtime ? `Model: ${runtime.model}` : null,
    runtime ? `Thinking effort: ${runtime.effort}` : null,
    `Codex mode: ${formatCodexModeSummary(context)}`,
    `Session: ${context.codexSessionId || "none"}`,
    `Last run: ${context.lastRunAt || "never"}`,
    LOCAL_HANDLING_NOTE
  ]
    .filter(Boolean)
    .join("\n");
}
