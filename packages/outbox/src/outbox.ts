import { existsSync } from "node:fs";
import { chmod, lstat, mkdir, open, readFile, readdir, readlink, realpath, rename, rm, stat, unlink } from "node:fs/promises";
import { createHash, randomUUID } from "node:crypto";
import { basename, dirname, join, resolve, sep } from "node:path";
import { gzipSync, gunzipSync } from "node:zlib";

export type BrainEndpoint = "import" | "propose";

export interface OutboxPayloadStorage {
  encoding: "json" | "json-gzip-base64";
  data: unknown;
  uncompressed_bytes: number;
  stored_bytes: number;
  sha256: string;
}

export interface OutboxItem {
  id: string;
  endpoint: BrainEndpoint;
  url: string;
  brain_id?: string;
  import_token_env?: string;
  payload?: Record<string, unknown>;
  payload_storage?: OutboxPayloadStorage;
  created_at: string;
  source_machine: string;
  source_harness: string;
  retry_count: number;
  idempotency_key: string;
  last_error: string | null;
  terminal_error?: string | null;
}

export interface OutboxEntry {
  path: string;
  item: OutboxItem;
  payload: Record<string, unknown>;
  idempotencyKey: string;
}

export interface CorruptOutboxEntry {
  path: string;
  name: string;
  error: string;
}

export interface OutboxScan {
  root: string;
  items: OutboxEntry[];
  corrupt: CorruptOutboxEntry[];
}

export interface OutboxLimits {
  compressAboveBytes: number;
  softWarnBytes: number;
  hardMaxBytes: number;
}

export const DEFAULT_OUTBOX_LIMITS: OutboxLimits = {
  compressAboveBytes: 1 * 1024 * 1024,
  softWarnBytes: 10 * 1024 * 1024,
  hardMaxBytes: 250 * 1024 * 1024
};
const OUTBOX_FILE_OVERHEAD_BYTES = 1024 * 1024;

export function outboxLimitsFromEnv(env: Record<string, string | undefined> = process.env): OutboxLimits {
  const numberFromEnv = (key: string, fallback: number): number => {
    const parsed = Number(env[key] || "");
    return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
  };
  return {
    compressAboveBytes: numberFromEnv("BRAINSTACK_OUTBOX_COMPRESS_ABOVE_BYTES", DEFAULT_OUTBOX_LIMITS.compressAboveBytes),
    softWarnBytes: numberFromEnv("BRAINSTACK_OUTBOX_SOFT_WARN_BYTES", DEFAULT_OUTBOX_LIMITS.softWarnBytes),
    hardMaxBytes: numberFromEnv("BRAINSTACK_OUTBOX_HARD_MAX_BYTES", DEFAULT_OUTBOX_LIMITS.hardMaxBytes)
  };
}

export function sha256Hex(text: string | Uint8Array): string {
  return createHash("sha256").update(text).digest("hex");
}

export function canonicalJson(value: unknown): string {
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

export function outboxItemKey(endpoint: BrainEndpoint, payload: Record<string, unknown>, destination?: Record<string, unknown>): string {
  return sha256Hex(canonicalJson({ endpoint, payload, destination: destination || null }));
}

function outboxDestinationForKey(item: Pick<OutboxItem, "brain_id" | "url">): Record<string, unknown> {
  return {
    brain_id: item.brain_id || null,
    url: item.url || null
  };
}

export function outboxItemId(endpoint: BrainEndpoint, idempotencyKey: string): string {
  return `${endpoint}-${idempotencyKey.slice(0, 32)}`;
}

async function unsafeOutboxPath(path: string): Promise<{ path: string; error: string } | null> {
  const absolute = resolve(path);
  let current = absolute;
  const seen = new Set<string>();
  while (true) {
    current = resolve(current);
    if (seen.has(current)) {
      return { path: current, error: "outbox path ancestor symlink loop" };
    }
    seen.add(current);
    const info = await lstat(current).catch(() => null);
    if (!info) {
      const parent = dirname(current);
      if (parent === current) {
        break;
      }
      current = parent;
      continue;
    }
    if (info.isSymbolicLink()) {
      if (info.uid !== 0) {
        return { path: current, error: "outbox path ancestor is a symlink" };
      }
      const linkTarget = await readlink(current).catch(() => "");
      if (!linkTarget) {
        return { path: current, error: "outbox path ancestor symlink target cannot be read" };
      }
      current = resolve(dirname(current), linkTarget);
      continue;
    }
    if (!info.isDirectory()) {
      return { path: current, error: "outbox path ancestor is not a directory" };
    }
    const parent = dirname(current);
    if (parent === current) {
      break;
    }
    current = parent;
  }
  return null;
}

export async function ensurePrivateOutboxDir(root: string): Promise<void> {
  const unsafeBefore = await unsafeOutboxPath(root);
  if (unsafeBefore) {
    throw new Error(`${unsafeBefore.error}: ${unsafeBefore.path}`);
  }
  await mkdir(root, { recursive: true, mode: 0o700 });
  const unsafeAfter = await unsafeOutboxPath(root);
  if (unsafeAfter) {
    throw new Error(`${unsafeAfter.error}: ${unsafeAfter.path}`);
  }
  await chmod(root, 0o700).catch(() => undefined);
}

async function fsyncDirectory(path: string): Promise<void> {
  try {
    const handle = await open(path, "r");
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
  } catch {
    // Directory fsync is best-effort across platforms/filesystems.
  }
}

export async function atomicWritePrivateJson(path: string, value: unknown): Promise<void> {
  const dir = dirname(path);
  await ensurePrivateOutboxDir(dir);
  const tmpPath = join(dir, `.${basename(path)}.${process.pid}.${randomUUID()}.tmp`);
  let handle: Awaited<ReturnType<typeof open>> | null = null;
  try {
    handle = await open(tmpPath, "wx", 0o600);
    await handle.writeFile(`${JSON.stringify(value, null, 2)}\n`, "utf8");
    await handle.sync();
    await handle.close();
    handle = null;
    await chmod(tmpPath, 0o600).catch(() => undefined);
    await rename(tmpPath, path);
    await chmod(path, 0o600).catch(() => undefined);
    await fsyncDirectory(dir);
  } catch (error) {
    if (handle) {
      await handle.close().catch(() => undefined);
    }
    await rm(tmpPath, { force: true }).catch(() => undefined);
    throw error;
  }
}

function validatePayloadSize(bytes: Uint8Array, limits: OutboxLimits, label: string): void {
  if (bytes.byteLength > limits.hardMaxBytes) {
    throw new Error(`${label} is too large: ${bytes.byteLength} bytes exceeds hard cap ${limits.hardMaxBytes}`);
  }
}

function validateStoredNumber(value: unknown, expected: number, label: string): void {
  if (value !== expected) {
    throw new Error(`${label} mismatch: expected ${expected}, recorded ${String(value)}`);
  }
}

function encodePayload(payload: Record<string, unknown>, limits: OutboxLimits): OutboxPayloadStorage {
  const json = JSON.stringify(payload);
  const rawBytes = new TextEncoder().encode(json);
  if (rawBytes.byteLength > limits.hardMaxBytes) {
    throw new Error(`outbox payload is too large: ${rawBytes.byteLength} bytes exceeds hard cap ${limits.hardMaxBytes}`);
  }
  if (rawBytes.byteLength >= limits.compressAboveBytes) {
    const compressed = gzipSync(rawBytes);
    return {
      encoding: "json-gzip-base64",
      data: compressed.toString("base64"),
      uncompressed_bytes: rawBytes.byteLength,
      stored_bytes: compressed.byteLength,
      sha256: sha256Hex(rawBytes)
    };
  }
  return {
    encoding: "json",
    data: payload,
    uncompressed_bytes: rawBytes.byteLength,
    stored_bytes: rawBytes.byteLength,
    sha256: sha256Hex(rawBytes)
  };
}

export function decodeOutboxPayload(
  item: OutboxItem,
  limits: OutboxLimits = outboxLimitsFromEnv()
): Record<string, unknown> {
  if (item.payload && typeof item.payload === "object") {
    const rawBytes = new TextEncoder().encode(JSON.stringify(item.payload));
    validatePayloadSize(rawBytes, limits, "legacy payload");
    return item.payload;
  }
  const storage = item.payload_storage;
  if (!storage || typeof storage !== "object") {
    throw new Error("missing payload storage");
  }
  if (storage.encoding === "json") {
    if (!storage.data || typeof storage.data !== "object" || Array.isArray(storage.data)) {
      throw new Error("invalid json payload storage");
    }
    const rawBytes = new TextEncoder().encode(JSON.stringify(storage.data));
    validatePayloadSize(rawBytes, limits, "json payload");
    validateStoredNumber(storage.uncompressed_bytes, rawBytes.byteLength, "json payload uncompressed_bytes");
    validateStoredNumber(storage.stored_bytes, rawBytes.byteLength, "json payload stored_bytes");
    if (sha256Hex(rawBytes) !== storage.sha256) {
      throw new Error("json payload hash mismatch");
    }
    return storage.data as Record<string, unknown>;
  }
  if (storage.encoding === "json-gzip-base64") {
    if (typeof storage.data !== "string") {
      throw new Error("invalid compressed payload storage");
    }
    if (typeof storage.uncompressed_bytes !== "number" || storage.uncompressed_bytes > limits.hardMaxBytes) {
      throw new Error(`compressed payload declared size is too large: ${String(storage.uncompressed_bytes)} exceeds hard cap ${limits.hardMaxBytes}`);
    }
    const compressed = Buffer.from(storage.data, "base64");
    validateStoredNumber(storage.stored_bytes, compressed.byteLength, "compressed payload stored_bytes");
    const rawBytes = gunzipSync(compressed, { maxOutputLength: limits.hardMaxBytes + 1 });
    validatePayloadSize(rawBytes, limits, "compressed payload");
    validateStoredNumber(storage.uncompressed_bytes, rawBytes.byteLength, "compressed payload uncompressed_bytes");
    if (sha256Hex(rawBytes) !== storage.sha256) {
      throw new Error("compressed payload hash mismatch");
    }
    const json = rawBytes.toString("utf8");
    const parsed = JSON.parse(json);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      throw new Error("decoded payload is not an object");
    }
    return parsed as Record<string, unknown>;
  }
  throw new Error(`unsupported payload encoding: ${String(storage.encoding)}`);
}

export function normalizeOutboxItem(item: OutboxItem, limits: OutboxLimits = outboxLimitsFromEnv()): OutboxItem {
  const payload = decodeOutboxPayload(item, limits);
  const idempotencyKey = outboxItemKey(item.endpoint, payload, outboxDestinationForKey(item));
  return {
    ...item,
    id: outboxItemId(item.endpoint, idempotencyKey),
    payload: undefined,
    payload_storage: encodePayload(payload, limits),
    idempotency_key: idempotencyKey
  };
}

export async function readOutboxEntry(
  path: string,
  limits: OutboxLimits = outboxLimitsFromEnv()
): Promise<OutboxEntry> {
  const info = await stat(path);
  const maxFileBytes = Math.max(limits.hardMaxBytes + OUTBOX_FILE_OVERHEAD_BYTES, limits.hardMaxBytes * 2);
  if (info.size > maxFileBytes) {
    throw new Error(`outbox item file is too large: ${info.size} bytes exceeds scan cap ${maxFileBytes}`);
  }
  const item = JSON.parse(await readFile(path, "utf8")) as OutboxItem;
  const payload = decodeOutboxPayload(item, limits);
  const idempotencyKey = outboxItemKey(item.endpoint, payload, outboxDestinationForKey(item));
  return { path, item, payload, idempotencyKey };
}

export async function scanOutbox(root: string, limits: OutboxLimits = outboxLimitsFromEnv()): Promise<OutboxScan> {
  const unsafe = await unsafeOutboxPath(root);
  if (unsafe) {
    return { root, items: [], corrupt: [{ path: unsafe.path, name: basename(unsafe.path), error: unsafe.error }] };
  }
  if (!existsSync(root)) {
    return { root, items: [], corrupt: [] };
  }
  const rootInfo = await lstat(root).catch(() => null);
  if (!rootInfo) {
    return { root, items: [], corrupt: [] };
  }
  if (rootInfo.isSymbolicLink()) {
    return { root, items: [], corrupt: [{ path: root, name: basename(root), error: "outbox namespace is a symlink" }] };
  }
  if (!rootInfo.isDirectory()) {
    return { root, items: [], corrupt: [{ path: root, name: basename(root), error: "outbox namespace is not a directory" }] };
  }
  await ensurePrivateOutboxDir(root);
  const rootReal = await realpath(root).catch(() => root);
  const names = (await readdir(root)).filter((name) => name.endsWith(".json") || name.endsWith(".tmp")).sort();
  const items: OutboxEntry[] = [];
  const corrupt: CorruptOutboxEntry[] = [];
  for (const name of names) {
    const path = join(root, name);
    const info = await lstat(path).catch(() => null);
    if (!info) {
      corrupt.push({ path, name, error: "outbox item disappeared during scan" });
      continue;
    }
    if (info.isSymbolicLink()) {
      corrupt.push({ path, name, error: "outbox item is a symlink" });
      continue;
    }
    const itemReal = await realpath(path).catch(() => path);
    if (itemReal !== rootReal && !itemReal.startsWith(`${rootReal}${sep}`)) {
      corrupt.push({ path, name, error: "outbox item escapes namespace realpath" });
      continue;
    }
    if (name.endsWith(".tmp")) {
      corrupt.push({ path, name, error: "stale temporary outbox file" });
      continue;
    }
    try {
      items.push(await readOutboxEntry(path, limits));
    } catch (error) {
      corrupt.push({ path, name, error: error instanceof Error ? error.message : String(error) });
    }
  }
  return { root, items, corrupt };
}

export async function writeOutboxItem(path: string, item: OutboxItem, limits: OutboxLimits = outboxLimitsFromEnv()): Promise<void> {
  await atomicWritePrivateJson(path, normalizeOutboxItem(item, limits));
}

export function buildOutboxItem(
  input: Omit<OutboxItem, "id" | "created_at" | "retry_count" | "idempotency_key" | "last_error" | "payload_storage"> & {
    created_at?: string;
    retry_count?: number;
    idempotency_key?: string;
    last_error?: string | null;
    terminal_error?: string | null;
  },
  error: string,
  existing?: OutboxItem | null,
  limits: OutboxLimits = outboxLimitsFromEnv()
): OutboxItem {
  const idempotencyKey = outboxItemKey(input.endpoint, input.payload || {}, outboxDestinationForKey(input));
  return normalizeOutboxItem(
    {
      ...input,
      id: outboxItemId(input.endpoint, idempotencyKey),
      created_at: existing?.created_at || input.created_at || new Date().toISOString(),
      retry_count: existing?.retry_count || input.retry_count || 0,
      idempotency_key: idempotencyKey,
      last_error: sanitizeOutboxError(error),
      terminal_error: existing?.terminal_error || input.terminal_error || null
    },
    limits
  );
}

export function sanitizedHttpError(status: number, responseText: string): string {
  return `HTTP ${status} response_sha256=${sha256Hex(responseText)} response_bytes=${new TextEncoder().encode(responseText).byteLength}`;
}

export function sanitizeOutboxError(error: string): string {
  if (!error) {
    return "unknown error";
  }
  if (error.startsWith("HTTP ") && error.includes("response_sha256=")) {
    return error;
  }
  return error.replace(/\s+/g, " ").slice(0, 500);
}

export async function purgeCorruptOutboxEntries(scan: OutboxScan): Promise<number> {
  let removed = 0;
  const scanRoot = resolve(scan.root);
  for (const entry of scan.corrupt) {
    const entryPath = resolve(entry.path);
    if (entryPath !== scanRoot && !entryPath.startsWith(`${scanRoot}${sep}`)) {
      continue;
    }
    const info = await lstat(entry.path).catch(() => null);
    if (info?.isSymbolicLink()) {
      await unlink(entry.path).catch(() => undefined);
    } else {
      await rm(entry.path, { force: true });
    }
    removed += 1;
  }
  return removed;
}
