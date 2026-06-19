export type OutboxErrorSummary = {
  message: string;
  count: number;
};

function compactOutboxError(error: unknown): string | null {
  if (typeof error !== "string") {
    return null;
  }
  const compact = error
    .replace(/\s+/g, " ")
    .replace(/\s*response_sha256=[a-f0-9]+\b/gi, "")
    .replace(/\s*response_bytes=\d+\b/gi, "")
    .trim();
  if (!compact) {
    return null;
  }
  if (/\bHTTP 401\b/i.test(compact) || /\bunauthorized\b/i.test(compact)) {
    return "HTTP 401 unauthorized";
  }
  if (/\bHTTP 403\b/i.test(compact) || /\bforbidden\b/i.test(compact)) {
    return "HTTP 403 forbidden";
  }
  if (/\bHTTP 425\b/i.test(compact)) {
    return "HTTP 425 idempotency review";
  }
  return compact.slice(0, 240);
}

export function summarizeOutboxTerminalErrors(errors: unknown[], max = 4): OutboxErrorSummary[] {
  const counts = new Map<string, number>();
  for (const error of errors) {
    const message = compactOutboxError(error);
    if (!message) {
      continue;
    }
    counts.set(message, (counts.get(message) || 0) + 1);
  }
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, max)
    .map(([message, count]) => ({ message, count }));
}

export function formatOutboxErrorSummary(summary: OutboxErrorSummary[]): string {
  return summary.map((entry) => `${entry.message} x${entry.count}`).join("; ");
}

