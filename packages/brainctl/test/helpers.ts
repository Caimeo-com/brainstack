import { describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { existsSync } from "node:fs";
import { chmod, mkdir, mkdtemp, readdir, readFile, realpath, rm, stat, symlink, utimes, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { createServer, type Server } from "node:net";
import { hostname, tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { gzipSync, gunzipSync } from "node:zlib";
import { syncWritableRepo, syncWritableRepoAsync, withRepoLock } from "../../../apps/braind/src/brain-lib";
import { loadConfig, parseSimpleYaml } from "../src/main";

export const PRODUCT_ROOT = resolve(import.meta.dir, "..", "..", "..");
export const BRAINCTL = join(PRODUCT_ROOT, "packages", "brainctl", "src", "main.ts");
export const SERVER = join(PRODUCT_ROOT, "apps", "braind", "src", "server.ts");
export const FORBIDDEN_PUBLIC_PATTERNS: Array<{ label: string; pattern: RegExp }> = [
  { label: "non-placeholder macOS home path", pattern: /\/Users\/(?!operator\b)[A-Za-z0-9._-]+/ },
  { label: "non-placeholder Linux home path", pattern: /\/home\/(?!operator\b|factory\b|brainstack\b)[A-Za-z0-9._-]+/ },
  { label: "non-placeholder Tailscale DNS name", pattern: /(?<!example)\.ts\.net\b/ },
  { label: "old GitHub remote marker", pattern: /github\.com[:/][^"'\s]*(?:proto|legacy)/i },
  { label: "old telemux product name", pattern: new RegExp("claw" + "dex", "i") },
  { label: "old private source name", pattern: new RegExp("private-dev" + "-factory", "i") },
  { label: "old private source env var", pattern: new RegExp("PRIVATE_DEV" + "_FACTORY_ROOT") },
  { label: "local/customer project marker", pattern: new RegExp("bit" + "falls", "i") },
  { label: "old factory phrase spaced", pattern: new RegExp("private\\s+dev\\s+factory", "i") },
  { label: "old factory phrase", pattern: new RegExp("private\\s+factory", "i") },
  { label: "c0 marker", pattern: new RegExp("customer" + "-zero", "i") },
  { label: "generic old-code marker", pattern: new RegExp("\\bproto" + "type\\b", "i") }
];

export function runCommand(args: string[], options: { cwd?: string; env?: Record<string, string> } = {}) {
  const envOverrides = options.env || {};
  const defaultEnv: Record<string, string> = {
    BRAINSTACK_UPDATE_PROBE_TIMEOUT_MS: "1000",
    BRAINSTACK_SHELL_PATH_TIMEOUT_MS: "1000"
  };
  if (!("BRAINSTACK_SKIP_USER_PATH_RESOLVE" in envOverrides || "BRAINSTACK_WORKER_PATH" in envOverrides || "SHELL" in envOverrides)) {
    defaultEnv.BRAINSTACK_SKIP_USER_PATH_RESOLVE = "1";
  }
  const proc = Bun.spawnSync(args, {
    cwd: options.cwd || PRODUCT_ROOT,
    env: { ...process.env, ...defaultEnv, ...envOverrides },
    stdout: "pipe",
    stderr: "pipe"
  });
  return {
    code: proc.exitCode,
    stdout: proc.stdout.toString(),
    stderr: proc.stderr.toString()
  };
}

export function expectSuccess(result: ReturnType<typeof runCommand>): void {
  if (result.code !== 0) {
    throw new Error(`command failed\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`);
  }
  expect(result.code).toBe(0);
}

export function sha256Text(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

export function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value) as string;
  }
  if (value instanceof Uint8Array) {
    return JSON.stringify({ __bytes_sha256: createHash("sha256").update(value).digest("hex"), byteLength: value.byteLength });
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

export function runBrainctl(args: string[], env?: Record<string, string>) {
  return runCommand(["bun", "run", BRAINCTL, ...args], { env });
}

export function shellQuote(value: string): string {
  return `'${value.replace(/'/g, "'\\''")}'`;
}

export async function writeExecutable(path: string, contents: string): Promise<void> {
  await writeFile(path, contents);
  await chmod(path, 0o755);
}

export async function writeFakeTailscale(binDir: string): Promise<void> {
  await writeExecutable(
    join(binDir, "tailscale"),
    [
      "#!/usr/bin/env sh",
      "if [ \"${1:-}\" = \"version\" ] || [ \"${1:-}\" = \"--version\" ]; then echo 'tailscale fake'; exit 0; fi",
      "if [ \"${1:-}\" = \"status\" ]; then echo 'tailscale fake'; exit 0; fi",
      "echo 'tailscale fake'",
      ""
    ].join("\n")
  );
}

export async function writeFakeDoctorCodex(binDir: string): Promise<void> {
  await writeExecutable(
    join(binDir, "codex"),
    [
      "#!/usr/bin/env sh",
      "if [ \"${1:-}\" = \"--version\" ]; then echo 'codex fake'; exit 0; fi",
      "if [ \"${1:-}\" = \"exec\" ] && [ \"${2:-}\" = \"--help\" ]; then",
      "  echo '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --output-last-message'",
      "  exit 0",
      "fi",
      "exit 0",
      ""
    ].join("\n")
  );
}

export async function readQueuedPayloadFromOutput(output: string): Promise<Record<string, unknown>> {
  const queuedPath = output
    .split(/\r?\n/)
    .find((line) => line.includes("shared-brain write queued:"))
    ?.replace(/^.*shared-brain write queued:\s*/, "")
    .trim();
  if (!queuedPath) {
    throw new Error(`queued path not found in output:\n${output}`);
  }
  const item = JSON.parse(await readFile(queuedPath, "utf8")) as Record<string, unknown>;
  if (item.payload && typeof item.payload === "object") {
    return item.payload as Record<string, unknown>;
  }
  const storage = item.payload_storage as Record<string, unknown> | undefined;
  if (storage?.encoding === "json" && storage.data && typeof storage.data === "object" && !Array.isArray(storage.data)) {
    return storage.data as Record<string, unknown>;
  }
  if (storage?.encoding === "json-gzip-base64" && typeof storage.data === "string") {
    return JSON.parse(gunzipSync(Buffer.from(storage.data, "base64")).toString("utf8")) as Record<string, unknown>;
  }
  throw new Error(`queued payload not readable: ${queuedPath}`);
}

export function git(args: string[], cwd: string): string {
  const deterministicArgs = args.includes("commit")
    ? ["-c", "core.hooksPath=/dev/null", "-c", "commit.gpgsign=false", ...args]
    : args;
  const result = runCommand(["git", ...deterministicArgs], { cwd });
  if (result.code !== 0) {
    throw new Error(`git ${deterministicArgs.join(" ")} failed\n${result.stderr || result.stdout}`);
  }
  return result.stdout.trim();
}

export async function writeFixtureConfig(path: string): Promise<void> {
  await writeFile(
    path,
    [
      "schema_version: 1",
      "profile: single-node",
      "machine:",
      "  name: brain-control",
      "  user: operator",
      "paths:",
      "  home: /tmp/brainstack-test-home",
      "brain:",
      "  publicBaseUrl: https://brain-control.example.ts.net",
      "telemux:",
      "  enabled: false",
      ""
    ].join("\n")
  );
}

export async function writeFixtureClientConfig(
  path: string,
  options: { home: string; stateRoot: string; localPath?: string; productRepo?: string; telegramVia?: string; telegramRemoteRepo?: string }
): Promise<void> {
  await writeFile(
    path,
    [
      "schema_version: 1",
      "profile: client-macos",
      "machine:",
      "  name: client",
      "  user: operator",
      "paths:",
      `  home: ${options.home}`,
      `  stateRoot: ${options.stateRoot}`,
      ...(options.productRepo ? [`  productRepo: ${options.productRepo}`] : []),
      "client:",
      `  localPath: ${options.localPath || join(options.home, "shared-brain")}`,
      "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
      ...(options.telegramVia ? [`  telegramVia: ${options.telegramVia}`] : []),
      ...(options.telegramRemoteRepo ? [`  telegramRemoteRepo: ${options.telegramRemoteRepo}`] : []),
      ""
    ].join("\n")
  );
}

export async function waitForBraind(port: number): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      const health = await fetch(`http://127.0.0.1:${port}/health`);
      if (health.ok) {
        return;
      }
    } catch {
      // wait for server startup
    }
    await Bun.sleep(100);
  }
  throw new Error(`braind did not become ready on port ${port}`);
}

export async function waitForCondition(condition: () => Promise<boolean>, label: string, attempts = 50): Promise<void> {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    if (await condition()) {
      return;
    }
    await Bun.sleep(100);
  }
  throw new Error(`timed out waiting for ${label}`);
}

export function braindTestEnv(root: string, port: number, overrides: Record<string, string> = {}): Record<string, string> {
  return {
    ...process.env,
    BRAIN_BIND: "127.0.0.1",
    BRAIN_PORT: String(port),
    BRAIN_IMPORT_TOKEN: "import-test-token",
    BRAIN_ADMIN_TOKEN: "admin-test-token",
    SHARED_BRAIN_REPO_ROOT: join(root, "shared-brain", "serve", "shared-brain"),
    SHARED_BRAIN_WRITE_REPO_ROOT: join(root, "shared-brain", "staging", "shared-brain"),
    BRAIN_BLOB_STORE: join(root, "state", "blobs", "shared-brain"),
    ...overrides
  };
}

export async function createSingleNodeInstall(dir: string): Promise<string> {
  const configPath = join(dir, "config.yaml");
  const root = join(dir, "install");
  await writeFixtureConfig(configPath);
  expectSuccess(runBrainctl(["init", "--profile", "single-node", "--config", configPath, "--root", root]));
  return root;
}

export async function startBraind(root: string, port: number, overrides: Record<string, string> = {}): Promise<ReturnType<typeof Bun.spawn>> {
  const proc = Bun.spawn(["bun", "run", SERVER], {
    cwd: PRODUCT_ROOT,
    env: braindTestEnv(root, port, overrides),
    stdout: "pipe",
    stderr: "pipe"
  }, 15_000);
  await waitForBraind(port);
  return proc;
}

export async function stopBraind(proc: ReturnType<typeof Bun.spawn> | null): Promise<void> {
  if (!proc) {
    return;
  }
  proc.kill();
  await proc.exited;
}

export function repoPathUrl(repoPath: string): string {
  return encodeURIComponent(repoPath).replace(/%2F/g, "/");
}

export async function listFiles(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const fullPath = join(root, entry.name);
    if (entry.isDirectory()) {
      if (["node_modules", "dist", ".git", ".build"].includes(entry.name)) {
        continue;
      }
      files.push(...(await listFiles(fullPath)));
    } else if (entry.isFile()) {
      files.push(fullPath);
    }
  }
  return files;
}


export {
  Buffer,
  basename,
  chmod,
  createHash,
  createServer,
  describe,
  dirname,
  existsSync,
  expect,
  gzipSync,
  gunzipSync,
  hostname,
  join,
  loadConfig,
  mkdir,
  mkdtemp,
  parseSimpleYaml,
  readdir,
  readFile,
  realpath,
  resolve,
  rm,
  stat,
  symlink,
  syncWritableRepo,
  syncWritableRepoAsync,
  test,
  tmpdir,
  utimes,
  withRepoLock,
  writeFile
};
export type { Server };
