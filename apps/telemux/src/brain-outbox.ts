import { rm } from "node:fs/promises";
import { resolve } from "node:path";
import type { FactoryConfig } from "./config";
import {
  buildOutboxItem,
  ensurePrivateOutboxDir,
  normalizeOutboxItem,
  outboxLimitsFromEnv,
  outboxItemKey,
  sanitizedHttpError,
  scanOutbox,
  sha256Hex,
  writeOutboxItem,
  type BrainEndpoint,
  type OutboxEntry,
  type OutboxItem as QueuedBrainWrite
} from "../../../packages/outbox/src/outbox";

function idempotencyKeyFor(endpoint: BrainEndpoint, payload: Record<string, unknown>): string {
  return outboxItemKey(endpoint, payload);
}

function isRetryableBrainWriteStatus(status: number): boolean {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

function maxPendingIdempotencyRetries(): number {
  const raw = Number(process.env.BRAINSTACK_OUTBOX_MAX_425_RETRIES || "");
  return Number.isFinite(raw) && raw > 0 ? Math.trunc(raw) : 12;
}

function idempotencyPendingTerminalError(item: QueuedBrainWrite, status: number, responseText: string): string | null {
  if (status !== 425) {
    return null;
  }
  const retryLimit = maxPendingIdempotencyRetries();
  if ((item.retry_count || 0) < retryLimit) {
    return null;
  }
  return `HTTP 425 persisted after ${item.retry_count} flush attempt(s); operator review required before replaying this idempotent write. ${sanitizedHttpError(status, responseText)}`;
}

function outboxRoot(config: FactoryConfig): string {
  const brainId = sha256Hex(config.brainBaseUrl || "unconfigured-brain").slice(0, 16);
  return resolve(config.stateRoot, "outbox", brainId);
}

function queuedDestinationViolation(item: QueuedBrainWrite, config: FactoryConfig): string | null {
  if (!item.url) {
    return null;
  }
  let queuedUrl: URL;
  let configuredUrl: URL;
  try {
    queuedUrl = new URL(item.url);
    configuredUrl = new URL(config.brainBaseUrl);
  } catch (error) {
    return `queued outbox destination is invalid: ${error instanceof Error ? error.message : String(error)}`;
  }
  if (queuedUrl.username || queuedUrl.password) {
    return "queued outbox destination must not contain URL credentials";
  }
  if (queuedUrl.protocol !== configuredUrl.protocol || queuedUrl.host !== configuredUrl.host) {
    return `queued outbox destination origin ${queuedUrl.origin} does not match configured brain origin ${configuredUrl.origin}`;
  }
  return null;
}

async function findMatchingQueuedWrite(root: string, endpoint: BrainEndpoint, idempotencyKey: string): Promise<{ path: string; item: QueuedBrainWrite } | null> {
  const scan = await scanOutbox(root);
  if (scan.corrupt.length) {
    throw new Error(`brain outbox contains ${scan.corrupt.length} corrupt/unsafe item(s); inspect and purge before queueing new writes`);
  }
  for (const entry of scan.items) {
    if (entry.item.endpoint !== endpoint) {
      continue;
    }
    if (entry.idempotencyKey === idempotencyKey || entry.item.idempotency_key === idempotencyKey) {
      return { path: entry.path, item: entry.item };
    }
  }
  return null;
}

async function queueBrainWrite(config: FactoryConfig, endpoint: BrainEndpoint, payload: Record<string, unknown>, error: string): Promise<string> {
  const idempotencyKey = idempotencyKeyFor(endpoint, payload);
  const created = new Date().toISOString();
  const id = `${endpoint}-${idempotencyKey.slice(0, 32)}`;
  const root = outboxRoot(config);
  await ensurePrivateOutboxDir(root);
  const stablePath = resolve(root, `${id}.json`);
  const matched = await findMatchingQueuedWrite(root, endpoint, idempotencyKey);
  const path = matched?.path || stablePath;
  const existing = matched?.item || null;
  const queuedItem = buildOutboxItem(
    {
      endpoint,
      url: config.brainBaseUrl,
      payload,
      created_at: existing?.created_at || created,
      source_machine: String(payload.source_machine || config.localMachine),
      source_harness: String(payload.source_harness || "telemux"),
      retry_count: existing?.retry_count || 0,
      idempotency_key: idempotencyKey,
      last_error: error
    },
    error,
    existing
  );
  await writeOutboxItem(path, queuedItem);
  const storage = queuedItem.payload_storage;
  const limits = outboxLimitsFromEnv();
  if (storage && storage.uncompressed_bytes >= limits.softWarnBytes) {
    const compressedNote = storage.encoding === "json-gzip-base64" ? `, compressed to ${storage.stored_bytes} bytes` : "";
    console.warn(`WARN queued large shared brain import: ${storage.uncompressed_bytes} bytes${compressedNote}. No content was truncated.`);
  }
  return path;
}

export async function postBrainImportOrQueue(config: FactoryConfig, payload: Record<string, unknown>): Promise<"sent" | "queued" | "disabled"> {
  if (!config.brainBaseUrl || !config.brainImportToken) {
    return "disabled";
  }

  const endpoint: BrainEndpoint = "import";
  const idempotencyKey = idempotencyKeyFor(endpoint, payload);
  try {
    const response = await fetch(new URL("/api/import", config.brainBaseUrl).toString(), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${config.brainImportToken}`,
        "Content-Type": "application/json",
        "Idempotency-Key": idempotencyKey
      },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(15_000)
    });

    if (!response.ok) {
      const text = await response.text();
      const sanitized = sanitizedHttpError(response.status, text);
      console.error(`shared brain import failed: ${sanitized}`);
      if (isRetryableBrainWriteStatus(response.status)) {
        const queuedPath = await queueBrainWrite(config, endpoint, payload, sanitized);
        console.warn(`shared brain import queued: ${queuedPath}`);
        return "queued";
      }
      return "disabled";
    }
    return "sent";
  } catch (error) {
    const queuedPath = await queueBrainWrite(config, endpoint, payload, error instanceof Error ? error.message : String(error));
    console.warn(`shared brain import queued: ${queuedPath}`);
    return "queued";
  }
}

export async function flushBrainOutbox(config: FactoryConfig): Promise<{ flushed: number; kept: number }> {
  const root = outboxRoot(config);
  if (!config.brainImportToken) {
    return { flushed: 0, kept: 0 };
  }
  const seen = new Map<string, OutboxEntry>();
  const firstScan = await scanOutbox(root);
  if (firstScan.corrupt.length) {
    throw new Error(`brain outbox contains ${firstScan.corrupt.length} corrupt/unsafe item(s); inspect and purge before flushing`);
  }
  for (const entry of firstScan.items) {
    const path = entry.path;
    const normalized = normalizeOutboxItem(entry.item);
    const key = normalized.idempotency_key;
    const prior = seen.get(key);
    if (!prior) {
      await writeOutboxItem(path, normalized);
      seen.set(key, { ...entry, item: normalized });
      continue;
    }
    const keep = prior.path.endsWith(`${normalized.endpoint}-${key.slice(0, 32)}.json`) ? prior : { ...entry, item: normalized };
    const drop = keep.path === prior.path ? { ...entry, item: normalized } : prior;
    keep.item.retry_count = Math.max(keep.item.retry_count || 0, drop.item.retry_count || 0);
    keep.item.created_at = [keep.item.created_at, drop.item.created_at].filter(Boolean).sort()[0] || keep.item.created_at;
    keep.item.idempotency_key = key;
    keep.item.last_error = keep.item.last_error || drop.item.last_error || null;
    keep.item.terminal_error = keep.item.terminal_error || drop.item.terminal_error || null;
    await writeOutboxItem(keep.path, keep.item);
    await rm(drop.path, { force: true });
    seen.set(key, keep);
  }
  let flushed = 0;
  let kept = 0;
  const secondScan = await scanOutbox(root);
  if (secondScan.corrupt.length) {
    throw new Error(`brain outbox contains ${secondScan.corrupt.length} corrupt/unsafe item(s); inspect and purge before flushing`);
  }
  for (const { path, item, payload } of secondScan.items) {
    const normalized = normalizeOutboxItem(item);
    if (normalized.id !== item.id || normalized.idempotency_key !== item.idempotency_key) {
      await writeOutboxItem(path, normalized);
    }
    if (normalized.terminal_error) {
      kept += 1;
      continue;
    }
    const destinationViolation = queuedDestinationViolation(normalized, config);
    if (destinationViolation) {
      normalized.last_error = destinationViolation;
      normalized.terminal_error = destinationViolation;
      await writeOutboxItem(path, normalized);
      kept += 1;
      continue;
    }
    try {
      const response = await fetch(new URL(`/api/${normalized.endpoint}`, config.brainBaseUrl).toString(), {
        method: "POST",
        headers: {
          Authorization: `Bearer ${config.brainImportToken}`,
          "Content-Type": "application/json",
          "Idempotency-Key": normalized.idempotency_key
        },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(15_000)
      });
      if (response.ok) {
        await rm(path, { force: true });
        flushed += 1;
      } else {
        const responseText = await response.text();
        normalized.retry_count += 1;
        normalized.last_error = sanitizedHttpError(response.status, responseText);
        const pendingTerminalError = idempotencyPendingTerminalError(normalized, response.status, responseText);
        if (pendingTerminalError) {
          normalized.terminal_error = pendingTerminalError;
        } else if (!isRetryableBrainWriteStatus(response.status)) {
          normalized.terminal_error = normalized.last_error;
        }
        await writeOutboxItem(path, normalized);
        kept += 1;
      }
    } catch (error) {
      normalized.retry_count += 1;
      normalized.last_error = error instanceof Error ? error.message : String(error);
      await writeOutboxItem(path, normalized);
      kept += 1;
    }
  }
  return { flushed, kept };
}
