export type CodexReasoningEffort = "low" | "medium" | "high" | "xhigh";

export interface CodexRuntimeOverrides {
  modelOverride: string | null;
  reasoningEffortOverride: CodexReasoningEffort | null;
}

export interface CodexModePreset extends CodexRuntimeOverrides {
  name: "fast" | "normal" | "max";
}

export const CODEX_MODE_PRESETS: Record<CodexModePreset["name"], CodexModePreset> = {
  fast: {
    name: "fast",
    modelOverride: "gpt-5.4-mini",
    reasoningEffortOverride: "low"
  },
  normal: {
    name: "normal",
    modelOverride: "gpt-5.4",
    reasoningEffortOverride: "medium"
  },
  max: {
    name: "max",
    modelOverride: "gpt-5.4",
    reasoningEffortOverride: "xhigh"
  }
};

export function parseCodexModePreset(value: string): CodexModePreset | null {
  const normalized = value.trim().toLowerCase();
  if (normalized === "fast" || normalized === "normal" || normalized === "max") {
    return CODEX_MODE_PRESETS[normalized];
  }

  return null;
}

export function parseCodexReasoningEffort(value: string): CodexReasoningEffort | null {
  const normalized = value.trim().toLowerCase();
  if (normalized === "low" || normalized === "medium" || normalized === "high" || normalized === "xhigh") {
    return normalized;
  }

  return null;
}

export function normalizeCodexModelOverride(value: string): string | null {
  const normalized = value.trim();
  if (!normalized) {
    return null;
  }
  if (/^\d+(?:\.\d+)+(?:-[a-z0-9][a-z0-9.-]*)?$/i.test(normalized)) {
    return `gpt-${normalized}`;
  }
  return normalized;
}

export function formatCodexRuntimeOverrides(overrides: CodexRuntimeOverrides): string {
  return [
    `model=${overrides.modelOverride || "default"}`,
    `effort=${overrides.reasoningEffortOverride || "default"}`
  ].join(" ");
}
