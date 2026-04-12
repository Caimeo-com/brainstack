import { existsSync } from "node:fs";
import { mkdir, readdir, rm, writeFile } from "node:fs/promises";
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
}

function sha256(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

function outboxRoot(config: FactoryConfig): string {
  const brainId = sha256(config.brainBaseUrl || "unconfigured-brain").slice(0, 16);
  return resolve(config.stateRoot, "outbox", brainId);
}

async function writeText(path: string, text: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, text, "utf8");
}

async function queueBrainWrite(config: FactoryConfig, endpoint: BrainEndpoint, payload: Record<string, unknown>, error: string): Promise<string> {
  const idempotencyKey = sha256(JSON.stringify({ endpoint, payload }));
  const created = new Date().toISOString();
  const id = `${created.replace(/[:.]/g, "-")}-${idempotencyKey.slice(0, 16)}`;
  const path = resolve(outboxRoot(config), `${id}.json`);
  if (!existsSync(path)) {
    await writeText(
      path,
      `${JSON.stringify(
        {
          id,
          endpoint,
          url: config.brainBaseUrl,
          payload,
          created_at: created,
          source_machine: String(payload.source_machine || config.localMachine),
          source_harness: String(payload.source_harness || "telemux"),
          retry_count: 0,
          idempotency_key: idempotencyKey,
          last_error: error
        } satisfies QueuedBrainWrite,
        null,
        2
      )}\n`
    );
  }
  return path;
}

export async function postBrainImportOrQueue(config: FactoryConfig, payload: Record<string, unknown>): Promise<"sent" | "queued" | "disabled"> {
  if (!config.brainBaseUrl || !config.brainImportToken) {
    return "disabled";
  }

  const endpoint: BrainEndpoint = "import";
  const idempotencyKey = sha256(JSON.stringify({ endpoint, payload }));
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
  const names = (await readdir(root)).filter((name) => name.endsWith(".json")).sort();
  let flushed = 0;
  let kept = 0;
  for (const name of names) {
    const path = resolve(root, name);
    const item = JSON.parse(await Bun.file(path).text()) as QueuedBrainWrite;
    try {
      const response = await fetch(new URL(`/api/${item.endpoint}`, item.url || config.brainBaseUrl).toString(), {
        method: "POST",
        headers: {
          Authorization: `Bearer ${config.brainImportToken}`,
          "Content-Type": "application/json",
          "Idempotency-Key": item.idempotency_key
        },
        body: JSON.stringify(item.payload),
        signal: AbortSignal.timeout(15_000)
      });
      if (response.ok || response.status === 409) {
        await rm(path, { force: true });
        flushed += 1;
      } else {
        kept += 1;
      }
    } catch {
      kept += 1;
    }
  }
  return { flushed, kept };
}
