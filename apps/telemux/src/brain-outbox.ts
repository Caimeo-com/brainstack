import { existsSync } from "node:fs";
import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { dirname, resolve } from "node:path";
import type { FactoryConfig } from "./config";

type BrainEndpoint = "import" | "propose";

interface QueuedBrainWrite {
  id: string;
  endpoint: BrainEndpoint;
  url: string;
  payload: Record<string, unknown>;
  created_at: string;
  source_machine: string;
  source_harness: string;
  retry_count: number;
  idempotency_key: string;
  last_error: string | null;
  terminal_error?: string | null;
}

function sha256(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  }
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
    .join(",")}}`;
}

function idempotencyKeyFor(endpoint: BrainEndpoint, payload: Record<string, unknown>): string {
  return sha256(canonicalJson({ endpoint, payload }));
}

function normalizeQueuedWrite(item: QueuedBrainWrite): QueuedBrainWrite {
  const idempotencyKey = idempotencyKeyFor(item.endpoint, item.payload);
  return {
    ...item,
    id: `${item.endpoint}-${idempotencyKey.slice(0, 32)}`,
    idempotency_key: idempotencyKey
  };
}

function isRetryableBrainWriteStatus(status: number): boolean {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

function outboxRoot(config: FactoryConfig): string {
  const brainId = sha256(config.brainBaseUrl || "unconfigured-brain").slice(0, 16);
  return resolve(config.stateRoot, "outbox", brainId);
}

async function writeText(path: string, text: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, text, "utf8");
}

async function readQueuedWrite(path: string): Promise<QueuedBrainWrite | null> {
  try {
    return JSON.parse(await readFile(path, "utf8")) as QueuedBrainWrite;
  } catch {
    return null;
  }
}

async function findMatchingQueuedWrite(root: string, endpoint: BrainEndpoint, idempotencyKey: string): Promise<{ path: string; item: QueuedBrainWrite } | null> {
  if (!existsSync(root)) {
    return null;
  }
  for (const name of (await readdir(root)).filter((entry) => entry.endsWith(".json")).sort()) {
    const path = resolve(root, name);
    const item = await readQueuedWrite(path);
    if (!item || item.endpoint !== endpoint) {
      continue;
    }
    if (idempotencyKeyFor(item.endpoint, item.payload) === idempotencyKey || item.idempotency_key === idempotencyKey) {
      return { path, item };
    }
  }
  return null;
}

async function queueBrainWrite(config: FactoryConfig, endpoint: BrainEndpoint, payload: Record<string, unknown>, error: string): Promise<string> {
  const idempotencyKey = idempotencyKeyFor(endpoint, payload);
  const created = new Date().toISOString();
  const id = `${endpoint}-${idempotencyKey.slice(0, 32)}`;
  const root = outboxRoot(config);
  await mkdir(root, { recursive: true });
  const stablePath = resolve(root, `${id}.json`);
  const matched = await findMatchingQueuedWrite(root, endpoint, idempotencyKey);
  const path = matched?.path || stablePath;
  const existing = matched?.item || (existsSync(stablePath) ? await readQueuedWrite(stablePath) : null);
  await writeText(
    path,
    `${JSON.stringify(
      {
        id,
        endpoint,
        url: config.brainBaseUrl,
        payload,
        created_at: existing?.created_at || created,
        source_machine: String(payload.source_machine || config.localMachine),
        source_harness: String(payload.source_harness || "telemux"),
        retry_count: existing?.retry_count || 0,
        idempotency_key: idempotencyKey,
        last_error: error
      } satisfies QueuedBrainWrite,
      null,
      2
    )}\n`
  );
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
      console.error(`shared brain import failed: ${response.status} ${text.slice(0, 500)}`);
      if (isRetryableBrainWriteStatus(response.status)) {
        const queuedPath = await queueBrainWrite(config, endpoint, payload, `HTTP ${response.status}: ${text.slice(0, 500)}`);
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
  if (!existsSync(root) || !config.brainImportToken) {
    return { flushed: 0, kept: 0 };
  }
  const seen = new Map<string, { path: string; item: QueuedBrainWrite }>();
  for (const name of (await readdir(root)).filter((entry) => entry.endsWith(".json")).sort()) {
    const path = resolve(root, name);
    const item = await readQueuedWrite(path);
    if (!item) {
      continue;
    }
    const normalized = normalizeQueuedWrite(item);
    const key = normalized.idempotency_key;
    const prior = seen.get(key);
    if (!prior) {
      await writeText(path, `${JSON.stringify(normalized, null, 2)}\n`);
      seen.set(key, { path, item: normalized });
      continue;
    }
    const keep = prior.path.endsWith(`${normalized.endpoint}-${key.slice(0, 32)}.json`) ? prior : { path, item: normalized };
    const drop = keep.path === prior.path ? { path, item: normalized } : prior;
    keep.item.retry_count = Math.max(keep.item.retry_count || 0, drop.item.retry_count || 0);
    keep.item.created_at = [keep.item.created_at, drop.item.created_at].filter(Boolean).sort()[0] || keep.item.created_at;
    keep.item.idempotency_key = key;
    keep.item.last_error = keep.item.last_error || drop.item.last_error || null;
    keep.item.terminal_error = keep.item.terminal_error || drop.item.terminal_error || null;
    await writeText(keep.path, `${JSON.stringify(keep.item, null, 2)}\n`);
    await rm(drop.path, { force: true });
    seen.set(key, keep);
  }
  const names = (await readdir(root)).filter((name) => name.endsWith(".json")).sort();
  let flushed = 0;
  let kept = 0;
  for (const name of names) {
    const path = resolve(root, name);
    const item = await readQueuedWrite(path);
    if (!item) {
      kept += 1;
      continue;
    }
    const normalized = normalizeQueuedWrite(item);
    if (normalized.id !== item.id || normalized.idempotency_key !== item.idempotency_key) {
      await writeText(path, `${JSON.stringify(normalized, null, 2)}\n`);
    }
    if (normalized.terminal_error) {
      kept += 1;
      continue;
    }
    try {
      const response = await fetch(new URL(`/api/${normalized.endpoint}`, normalized.url || config.brainBaseUrl).toString(), {
        method: "POST",
        headers: {
          Authorization: `Bearer ${config.brainImportToken}`,
          "Content-Type": "application/json",
          "Idempotency-Key": normalized.idempotency_key
        },
        body: JSON.stringify(normalized.payload),
        signal: AbortSignal.timeout(15_000)
      });
      if (response.ok) {
        await rm(path, { force: true });
        flushed += 1;
      } else {
        const responseText = await response.text();
        normalized.retry_count += 1;
        normalized.last_error = `HTTP ${response.status}: ${responseText.slice(0, 300)}`;
        if (!isRetryableBrainWriteStatus(response.status)) {
          normalized.terminal_error = normalized.last_error;
        }
        await writeText(path, `${JSON.stringify(normalized, null, 2)}\n`);
        kept += 1;
      }
    } catch (error) {
      normalized.retry_count += 1;
      normalized.last_error = error instanceof Error ? error.message : String(error);
      await writeText(path, `${JSON.stringify(normalized, null, 2)}\n`);
      kept += 1;
    }
  }
  return { flushed, kept };
}
