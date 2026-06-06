import type { PreDispatchClassifierConfig } from "./config";
import type { ContextRecord } from "./db";

export type PreDispatchRoute = "control_meta" | "light_harness" | "full_harness";
export type PreDispatchSource = "deterministic" | "llm" | "fallback";
export type ControlMetaKind = "liveness" | "usage" | "latency" | "status";

export interface PreDispatchClassification {
  route: PreDispatchRoute;
  source: PreDispatchSource;
  reason: string;
  confidence: number;
  controlKind?: ControlMetaKind;
}

export interface PreDispatchInput {
  text: string;
  context: ContextRecord;
  hasAttachments: boolean;
  classifier: PreDispatchClassifierConfig;
  fetcher?: typeof fetch;
}

type DeterministicRoute = PreDispatchClassification | null;

const ROUTES = new Set<PreDispatchRoute>(["control_meta", "light_harness", "full_harness"]);
const CONTROL_META_KINDS = new Set<ControlMetaKind>(["liveness", "usage", "latency", "status"]);
const CLASSIFIER_ENDPOINT = "https://api.openai.com/v1/responses";
const HARD_FULL_MAX_CHARS = 1200;

const CLASSIFIER_PROMPT = [
  "Classify one Telegram message for a developer control plane.",
  "Return only compact JSON: {\"route\":\"control_meta|light_harness|full_harness\",\"controlKind\":\"liveness|usage|latency|status|null\",\"confidence\":0.0,\"reason\":\"short reason\"}.",
  "control_meta is only for liveness, status, latency, token/cost/usage questions about the control plane or latest run.",
  "light_harness is only for short informational, recap, explanation, or planning questions that can use existing conversation context without reading or changing files.",
  "full_harness is for code, filesystem, shell, repo, machine, SSH, scheduling, attachment, deployment, audit, debugging, testing, installation, long, or ambiguous requests.",
  "When uncertain, choose full_harness."
].join("\n");

function normalizeText(text: string): string {
  return text.replace(/\s+/g, " ").trim();
}

function normalizeUtterance(text: string): string {
  return normalizeText(text)
    .toLowerCase()
    .replace(/[.?!]+$/g, "")
    .trim();
}

function classification(
  route: PreDispatchRoute,
  source: PreDispatchSource,
  reason: string,
  confidence: number,
  controlKind?: ControlMetaKind
): PreDispatchClassification {
  return {
    route,
    source,
    reason,
    confidence,
    ...(controlKind ? { controlKind } : {})
  };
}

function hasPathLikeReference(text: string): boolean {
  return /(?:^|\s)(?:\.{0,2}\/|~\/|\/)[^\s]+/.test(text) ||
    /(?:^|\s)[\w.-]+\.(?:ts|tsx|js|jsx|mjs|cjs|json|md|toml|ya?ml|sh|py|go|rs|swift|kt|java|sql|sqlite|db|log|txt)(?=$|[\s:),.;!?])/i.test(
      text
    );
}

function hasSchedulingIntent(text: string): boolean {
  return /\b(?:cron|scheduled job|scheduler|schedule|reschedule|unschedule|pause the job|resume the job|remind me to|reminder every|daily at|weekly at)\b/i.test(
    text
  );
}

function hasAttachmentIntent(text: string): boolean {
  return /\b(?:send|attach|upload|download|shred|delete)\b.{0,40}\b(?:artifact|attachment|file|document|report|image|photo|log)\b/i.test(
    text
  );
}

function hasWorkIntent(text: string): boolean {
  return /\b(?:add|apply|audit|build|change|check|clone|commit|configure|create|debug|delete|deploy|diagnose|edit|execute|export|fix|grep|implement|install|inspect|investigate|modify|move|open|patch|pull|push|read|refactor|remove|repair|restart|review|run|send|set|ship|tail|test|update|write|zip)\b/i.test(
    text
  );
}

function hasMutatingIntent(text: string): boolean {
  return /\b(?:add|apply|build|change|clone|commit|configure|create|delete|deploy|edit|fix|implement|install|make|modify|move|patch|pull|push|refactor|remove|repair|restart|set|ship|update|write|zip)\b/i.test(
    text
  );
}

function hasExecutionIntent(text: string): boolean {
  return /\b(?:run|execute|test|check)\b.{0,50}\b(?:script|command|test|tests|suite|repo|code|file|files|counter|tool)\b/i.test(
    text
  );
}

function hasReadInspectionIntent(text: string): boolean {
  return (
    /\b(?:tail|grep|cat|read|open|inspect)\b.{0,50}\b(?:log|logs|file|files|repo|workspace|diff|status|output|artifact|document)\b/i.test(
      text
    ) ||
    /\b(?:log|logs|file|files|repo|workspace|diff|status|output|artifact|document)\b.{0,50}\b(?:tail|grep|cat|read|open|inspect)\b/i.test(
      text
    )
  );
}

function hasMachineOpsIntent(text: string): boolean {
  return /\b(?:ssh|tailnet|tailscale|systemctl|journalctl|service|daemon|worker|host|machine|port|process|pid|database|sqlite|token|secret)\b/i.test(
    text
  ) && hasWorkIntent(text);
}

function isBareLiveness(text: string): boolean {
  const normalized = normalizeUtterance(text);
  return /^(?:ping|pong|test|testing|you up|are you up|alive|you alive|are you alive|still there|checking in|hello|hi)$/.test(
    normalized
  );
}

function isBareStatus(text: string): boolean {
  const normalized = normalizeUtterance(text);
  return /^(?:status|context|topic status|what context is this|where are we)$/.test(normalized);
}

function isLatencyQuestion(text: string): boolean {
  const normalized = normalizeUtterance(text);
  return (
    /^(?:what|why)\b.{0,60}\b(?:took|taking)\b/.test(normalized) ||
    /\b(?:so|too|really|this|that)\s+(?:slow|long)\b/.test(normalized) ||
    /\bhow long (?:did|does|will|has|is|was|were)\b/.test(normalized) ||
    /^why(?:'s| is| was| are| were)?(?: it| this| that| everything)?\s*so slow$/.test(normalized)
  );
}

function isUsageQuestion(text: string): boolean {
  const endsWithQuestion = /\?\s*$/.test(normalizeText(text));
  const normalized = normalizeUtterance(text);
  if (!/\b(?:token|tokens|cost|spend|spent|usage|expensive)\b/.test(normalized)) {
    return false;
  }

  return (
    /^(?:how|what|why|roughly|about|do|does|did|can|could|tell|show)\b/.test(normalized) ||
    endsWithQuestion
  );
}

function isLightInformationalQuestion(text: string): boolean {
  const normalized = normalizeUtterance(text);
  if (normalized.length > 260) {
    return false;
  }

  if (
    /^(?:what were we doing|where did we leave off|what is next|what's next|next steps|recap|summari[sz]e|explain(?: that| this)?|what happened|why did that happen)$/.test(
      normalized
    )
  ) {
    return true;
  }

  return /^(?:what|why|how|can you explain|could you explain|tell me|remind me)\b/.test(normalized);
}

function deterministicClassification(input: PreDispatchInput): DeterministicRoute {
  const text = normalizeText(input.text);
  if (!text) {
    return classification("full_harness", "deterministic", "empty text falls back to normal dispatch", 1);
  }

  if (input.hasAttachments) {
    return classification("full_harness", "deterministic", "attachments require full workspace staging", 1);
  }

  if (text.length > HARD_FULL_MAX_CHARS) {
    return classification("full_harness", "deterministic", "long message requires full work path", 1);
  }

  if (isBareLiveness(text)) {
    return classification("control_meta", "deterministic", "bare liveness probe", 1, "liveness");
  }

  const pathLike = hasPathLikeReference(text);
  const schedulingIntent = hasSchedulingIntent(text);
  const attachmentIntent = hasAttachmentIntent(text);
  const workIntent = hasMutatingIntent(text) || hasExecutionIntent(text) || hasReadInspectionIntent(text);

  if (isLatencyQuestion(text) && !pathLike && !schedulingIntent && !attachmentIntent && !workIntent) {
    return classification("control_meta", "deterministic", "run latency question", 0.95, "latency");
  }

  if (isUsageQuestion(text) && !pathLike && !schedulingIntent && !attachmentIntent && !workIntent) {
    return classification("control_meta", "deterministic", "token or cost question", 0.95, "usage");
  }

  if (isBareStatus(text)) {
    return classification("control_meta", "deterministic", "bare status question", 1, "status");
  }

  if (
    pathLike ||
    schedulingIntent ||
    attachmentIntent ||
    hasMachineOpsIntent(text) ||
    hasWorkIntent(text)
  ) {
    return classification("full_harness", "deterministic", "message can require workspace, shell, machine, or durable state work", 1);
  }

  if (isLightInformationalQuestion(text)) {
    return classification("light_harness", "deterministic", "short informational question", 0.9);
  }

  return null;
}

function extractResponseText(value: unknown): string | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  const record = value as Record<string, unknown>;
  if (typeof record.output_text === "string") {
    return record.output_text;
  }

  const chunks: string[] = [];
  const output = Array.isArray(record.output) ? record.output : [];
  for (const item of output) {
    if (!item || typeof item !== "object") {
      continue;
    }
    const content = Array.isArray((item as Record<string, unknown>).content) ? ((item as Record<string, unknown>).content as unknown[]) : [];
    for (const contentItem of content) {
      if (!contentItem || typeof contentItem !== "object") {
        continue;
      }
      const contentRecord = contentItem as Record<string, unknown>;
      if (typeof contentRecord.text === "string") {
        chunks.push(contentRecord.text);
      } else if (contentRecord.text && typeof contentRecord.text === "object") {
        const nested = contentRecord.text as Record<string, unknown>;
        if (typeof nested.value === "string") {
          chunks.push(nested.value);
        }
      }
    }
  }

  return chunks.join("").trim() || null;
}

function parseClassifierJson(text: string): Record<string, unknown> | null {
  const trimmed = text.trim();
  const jsonText = trimmed.startsWith("{") ? trimmed : trimmed.match(/\{[\s\S]*\}/)?.[0] || "";
  if (!jsonText) {
    return null;
  }

  try {
    const parsed = JSON.parse(jsonText) as unknown;
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

function parseControlKind(value: unknown): ControlMetaKind | null {
  return typeof value === "string" && CONTROL_META_KINDS.has(value as ControlMetaKind) ? (value as ControlMetaKind) : null;
}

function sanitizeLlmClassification(
  parsed: Record<string, unknown>,
  threshold: number
): PreDispatchClassification | null {
  const route = typeof parsed.route === "string" && ROUTES.has(parsed.route as PreDispatchRoute) ? (parsed.route as PreDispatchRoute) : null;
  if (!route) {
    return null;
  }

  const numericConfidence =
    typeof parsed.confidence === "number" ? parsed.confidence : typeof parsed.confidence === "string" ? Number(parsed.confidence) : 0;
  const rawConfidence = Number.isFinite(numericConfidence) ? numericConfidence : 0;
  const confidence = Math.max(0, Math.min(1, rawConfidence));
  const reason = typeof parsed.reason === "string" && parsed.reason.trim() ? normalizeText(parsed.reason).slice(0, 120) : "classifier route";

  if (route !== "full_harness" && confidence < threshold) {
    return classification("full_harness", "fallback", "classifier confidence below threshold", confidence);
  }

  const controlKind = route === "control_meta" ? parseControlKind(parsed.controlKind) || "status" : undefined;
  return classification(route, "llm", reason, confidence, controlKind);
}

async function classifyWithLlm(input: PreDispatchInput): Promise<PreDispatchClassification> {
  const text = normalizeText(input.text);
  const { classifier } = input;
  if (!classifier.enabled) {
    return classification("full_harness", "fallback", "LLM classifier disabled", 0);
  }
  if (!classifier.apiKey) {
    return classification("full_harness", "fallback", "LLM classifier API key missing", 0);
  }
  if (text.length > classifier.maxChars) {
    return classification("full_harness", "fallback", "message exceeds LLM classifier character cap", 1);
  }

  const fetcher = input.fetcher || fetch;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), classifier.timeoutMs);
  try {
    const response = await fetcher(CLASSIFIER_ENDPOINT, {
      method: "POST",
      signal: controller.signal,
      headers: {
        Authorization: `Bearer ${classifier.apiKey}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: classifier.model,
        max_output_tokens: 120,
        ...(classifier.reasoningEffort ? { reasoning: { effort: classifier.reasoningEffort } } : {}),
        input: [
          {
            role: "system",
            content: CLASSIFIER_PROMPT
          },
          {
            role: "user",
            content: JSON.stringify({
              text,
              context: {
                slug: input.context.slug,
                kind: input.context.kind,
                state: input.context.state,
                machine: input.context.machine
              }
            })
          }
        ]
      })
    });

    if (!response.ok) {
      return classification("full_harness", "fallback", "LLM classifier request failed", 0);
    }

    const body = (await response.json()) as unknown;
    if (body && typeof body === "object" && (body as Record<string, unknown>).status === "incomplete") {
      return classification("full_harness", "fallback", "LLM classifier response incomplete", 0);
    }
    const outputText = extractResponseText(body);
    const parsed = outputText ? parseClassifierJson(outputText) : null;
    const sanitized = parsed ? sanitizeLlmClassification(parsed, classifier.confidenceThreshold) : null;
    return sanitized || classification("full_harness", "fallback", "LLM classifier returned invalid output", 0);
  } catch {
    return classification("full_harness", "fallback", "LLM classifier errored or timed out", 0);
  } finally {
    clearTimeout(timeout);
  }
}

export async function classifyPreDispatch(input: PreDispatchInput): Promise<PreDispatchClassification> {
  const deterministic = deterministicClassification(input);
  if (deterministic) {
    return deterministic;
  }

  return classifyWithLlm(input);
}
