import { createHash, randomUUID } from "node:crypto";
import { createReadStream, existsSync, mkdirSync } from "node:fs";
import { chmod, copyFile, lstat, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { basename, dirname, join, sep } from "node:path";
import { tmpdir } from "node:os";
import type { ParsedArgs } from "../args";
import type { BrainstackConfig, BrainstackWorkerConfig } from "../config";
import type { run as runtimeRun, runWithStdinFile as runtimeRunWithStdinFile } from "../runtime";

type RunResult = ReturnType<typeof runtimeRun>;
type StdinRunResult = Awaited<ReturnType<typeof runtimeRunWithStdinFile>>;

export interface BrainstackUploadManifest {
  schema_version: 1;
  id: string;
  machine: string;
  source: string;
  original_name: string;
  file_name: string;
  label: string | null;
  size_bytes: number;
  sha256: string;
  uploaded_at: string;
  remote_path: string;
  manifest_path: string;
}

type UploadsDeps = {
  abs: (input: string) => string;
  absWithHome: (input: string, home: string) => string;
  brainstackDefaultConfigPath: () => string;
  controlSshTarget: (
    cfg: BrainstackConfig,
    args: ParsedArgs,
    label: string,
    options?: { allowRemoteSshFallback?: boolean }
  ) => string | null;
  defaultWorkers: (cfg: BrainstackConfig) => BrainstackWorkerConfig[];
  flag: (args: ParsedArgs, name: string) => string | undefined;
  hasFlag: (args: ParsedArgs, name: string) => boolean;
  loadConfig: (path?: string | null, profile?: string | null, root?: string | null) => Promise<BrainstackConfig>;
  parsePositiveIntegerFlag: (args: ParsedArgs, name: string, fallback: number) => number;
  quoteForBash: (value: string) => string;
  remoteBrainctlScript: (remoteRepo: string, argv: string[], options?: { preferInstalledBinary?: boolean }) => string;
  requireFlagValue: (args: ParsedArgs, name: string) => string | undefined;
  run: typeof runtimeRun;
  runControlRemoteScript: (
    cfg: BrainstackConfig,
    args: ParsedArgs,
    label: string,
    remoteScript: string,
    timeoutMs: number,
    options?: { allowRemoteSshFallback?: boolean }
  ) => RunResult | null;
  runWithStdinFile: typeof runtimeRunWithStdinFile;
  runWorkerShell: (cfg: BrainstackConfig, worker: BrainstackWorkerConfig, script: string, timeoutSeconds?: number, usePathCache?: boolean) => RunResult;
  telegramControlWorker: (target: string) => BrainstackWorkerConfig;
  telegramKnownHostsPath: (cfg: BrainstackConfig, args: ParsedArgs) => string;
  telegramSshTrustArgs: (mode: "pinned" | "accept-new" | "default", knownHostsPath: string) => string[];
  telegramSshTrustMode: (args: ParsedArgs) => "pinned" | "accept-new" | "default";
  workerRemoteTarget: (worker: BrainstackWorkerConfig) => string;
  workerSshPortArgs: (worker: BrainstackWorkerConfig) => string[];
  workerSshTrustArgs: (cfg: BrainstackConfig, worker: BrainstackWorkerConfig) => string[];
  whichCommand: (name: string) => string | null;
};

const UPLOADS_SCHEMA_VERSION = 1;
const DEFAULT_MAX_UPLOAD_BYTES = 512 * 1024 * 1024;
const DEFAULT_UPLOAD_TIMEOUT_MS = 10 * 60_000;

function compactError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function uploadsRoot(cfg: BrainstackConfig): string {
  return join(cfg.paths.stateRoot, "uploads");
}

function remoteUploadsRoot(): string {
  return "~/.local/state/brainstack/uploads";
}

function uploadId(date = new Date()): string {
  return `up_${date.toISOString().replaceAll("-", "").replaceAll(":", "").replace(/\.\d{3}Z$/, "Z")}_${randomUUID().slice(0, 8)}`;
}

function validateUploadId(id: string): string {
  if (!/^up_[A-Za-z0-9TZ_-]{8,80}$/.test(id)) {
    throw new Error(`invalid upload id: ${id}`);
  }
  return id;
}

function safeUploadFileName(input: string): string {
  const raw = basename(input).trim();
  const safe = raw
    .replace(/[\u0000-\u001f\u007f]/g, "")
    .replace(/[/:\\]/g, "_")
    .replace(/^\.+$/, "")
    .slice(0, 160);
  return safe || "upload.bin";
}

async function sha256File(path: string): Promise<string> {
  return await new Promise((resolvePromise, reject) => {
    const hash = createHash("sha256");
    const stream = createReadStream(path);
    stream.on("error", reject);
    stream.on("data", (chunk) => hash.update(chunk));
    stream.on("end", () => resolvePromise(hash.digest("hex")));
  });
}

async function validateSourceFile(path: string, maxBytes: number): Promise<{ size: number; sha256: string }> {
  const info = await lstat(path);
  if (info.isSymbolicLink() || !info.isFile()) {
    throw new Error(`upload source must be a regular non-symlink file: ${path}`);
  }
  if (info.size > maxBytes) {
    throw new Error(`upload source is too large: ${info.size} bytes > ${maxBytes} bytes`);
  }
  return { size: info.size, sha256: await sha256File(path) };
}

async function writePrivateJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  await writeFile(path, `${JSON.stringify(value, null, 2)}\n`, { mode: 0o600 });
  await chmod(path, 0o600);
}

function localMachineAliases(cfg: BrainstackConfig): Set<string> {
  const aliases = new Set([cfg.machine.name, "local"]);
  if (cfg.profile === "control" || cfg.profile === "single-node") {
    aliases.add("control");
  }
  if (cfg.profile === "worker") {
    aliases.add("worker");
  }
  if (cfg.profile === "client-macos") {
    aliases.add("client");
  }
  return aliases;
}

function isLocalMachine(cfg: BrainstackConfig, machine: string): boolean {
  const normalized = machine.trim().toLowerCase();
  return [...localMachineAliases(cfg)].some((alias) => alias.toLowerCase() === normalized);
}

function findWorker(cfg: BrainstackConfig, machine: string, deps: UploadsDeps): BrainstackWorkerConfig | null {
  const normalized = machine.toLowerCase();
  return (
    deps.defaultWorkers(cfg).find((worker) => {
      const names = [worker.name, worker.sshTarget || "", deps.workerRemoteTarget(worker)].filter(Boolean).map((value) => value.toLowerCase());
      return names.includes(normalized);
    }) || null
  );
}

function resolveMachine(args: ParsedArgs, cfg: BrainstackConfig): string {
  return depsMachineFlag(args) || cfg.machine.name;
}

function depsMachineFlag(args: ParsedArgs): string | null {
  return (args.flags.machine === true ? "" : typeof args.flags.machine === "string" ? args.flags.machine : undefined)?.trim() || null;
}

function manifestPathForLocal(cfg: BrainstackConfig, manifest: BrainstackUploadManifest): string {
  return join(uploadsRoot(cfg), manifest.uploaded_at.slice(0, 10), manifest.id, "manifest.json");
}

function expandLocalUploadPath(input: string): string {
  if (input === "~") {
    return process.env.HOME || input;
  }
  if (input.startsWith("~/")) {
    return join(process.env.HOME || "", input.slice(2));
  }
  return input;
}

async function installLocalUpload(cfg: BrainstackConfig, sourcePath: string, manifest: BrainstackUploadManifest): Promise<BrainstackUploadManifest> {
  const finalDir = join(uploadsRoot(cfg), manifest.uploaded_at.slice(0, 10), manifest.id);
  const finalPath = join(finalDir, manifest.file_name);
  const manifestPath = join(finalDir, "manifest.json");
  await mkdir(finalDir, { recursive: true, mode: 0o700 });
  await chmod(finalDir, 0o700);
  await copyFile(sourcePath, finalPath);
  await chmod(finalPath, 0o600);
  const finalManifest = {
    ...manifest,
    remote_path: finalPath,
    manifest_path: manifestPath
  };
  await writePrivateJson(manifestPath, finalManifest);
  return finalManifest;
}

function remoteInstallScript(remoteDir: string, remoteFile: string): string {
  return [
    "set -euo pipefail",
    "umask 077",
    "brainstack_expand_home() {",
    '  case "$1" in',
    '    \\~) printf \'%s\\n\' "$HOME" ;;',
    '    \\~/*) printf \'%s/%s\\n\' "$HOME" "${1#\\~/}" ;;',
    '    *) printf \'%s\\n\' "$1" ;;',
    "  esac",
    "}",
    `dir="$(brainstack_expand_home ${remoteDir})"`,
    `file="$(brainstack_expand_home ${remoteFile})"`,
    "mkdir -p \"$dir\"",
    "chmod 700 \"$dir\"",
    "tmp=\"$file.tmp.$$\"",
    "cat > \"$tmp\"",
    "chmod 600 \"$tmp\"",
    "mv -f \"$tmp\" \"$file\""
  ].join("\n");
}

function workerShellArgs(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, script: string, deps: UploadsDeps): string[] {
  if (worker.transport === "tailscale-ssh") {
    return [deps.whichCommand("tailscale") || "tailscale", "ssh", deps.workerRemoteTarget(worker), "bash", "-lc", script];
  }
  return [
    deps.whichCommand("ssh") || "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=8",
    ...deps.workerSshTrustArgs(cfg, worker),
    ...deps.workerSshPortArgs(worker),
    deps.workerRemoteTarget(worker),
    `bash -lc ${deps.quoteForBash(script)}`
  ];
}

function controlShellArgs(cfg: BrainstackConfig, args: ParsedArgs, script: string, deps: UploadsDeps): string[] {
  const via = deps.controlSshTarget(cfg, args, "uploads control SSH target", { allowRemoteSshFallback: true });
  if (!via) {
    throw new Error("uploads needs a control SSH target for client-macos delegation");
  }
  const worker = deps.telegramControlWorker(via);
  const knownHostsPath = deps.telegramKnownHostsPath(cfg, args);
  const sshTrustMode = deps.telegramSshTrustMode(args);
  if (sshTrustMode === "accept-new") {
    mkdirSync(dirname(knownHostsPath), { recursive: true });
  }
  return [
    deps.whichCommand("ssh") || "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=8",
    ...deps.telegramSshTrustArgs(sshTrustMode, knownHostsPath),
    ...deps.workerSshPortArgs(worker),
    deps.workerRemoteTarget(worker),
    `bash -lc ${deps.quoteForBash(script)}`
  ];
}

async function writeTempManifest(manifest: BrainstackUploadManifest): Promise<{ path: string; cleanup: () => Promise<void> }> {
  const dir = await mkdtemp(join(tmpdir(), "brainstack-upload-"));
  const path = join(dir, "manifest.json");
  await writeFile(path, `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600 });
  await chmod(path, 0o600);
  return {
    path,
    cleanup: () => rm(dir, { recursive: true, force: true })
  };
}

async function runStdinChecked(result: Promise<StdinRunResult>, label: string): Promise<void> {
  const resolved = await result;
  if (resolved.code !== 0 || resolved.timedOut) {
    throw new Error(`${label} failed${resolved.timedOut ? " (timed out)" : ` (exit ${resolved.code})`}\n${resolved.stderr || resolved.stdout}`);
  }
}

async function installRemoteUpload(
  cfg: BrainstackConfig,
  worker: BrainstackWorkerConfig,
  sourcePath: string,
  manifest: BrainstackUploadManifest,
  deps: UploadsDeps,
  timeoutMs: number
): Promise<BrainstackUploadManifest> {
  const date = manifest.uploaded_at.slice(0, 10);
  const remoteDir = `${remoteUploadsRoot()}/${date}/${manifest.id}`;
  const remotePath = `${remoteDir}/${manifest.file_name}`;
  const remoteManifestPath = `${remoteDir}/manifest.json`;
  const finalManifest = {
    ...manifest,
    remote_path: remotePath,
    manifest_path: remoteManifestPath
  };

  await runStdinChecked(
      deps.runWithStdinFile(workerShellArgs(cfg, worker, remoteInstallScript(deps.quoteForBash(remoteDir), deps.quoteForBash(remotePath)), deps), sourcePath, {
      maxBytes: manifest.size_bytes,
      timeoutMs
    }),
    `upload ${manifest.file_name} to ${worker.name}`
  );

  const temp = await writeTempManifest(finalManifest);
  try {
    await runStdinChecked(
      deps.runWithStdinFile(workerShellArgs(cfg, worker, remoteInstallScript(deps.quoteForBash(remoteDir), deps.quoteForBash(remoteManifestPath)), deps), temp.path, {
        maxBytes: 1024 * 1024,
        timeoutMs: Math.min(timeoutMs, 60_000)
      }),
      `upload manifest for ${manifest.id} to ${worker.name}`
    );
  } finally {
    await temp.cleanup();
  }
  return finalManifest;
}

async function stageFileOnControl(
  cfg: BrainstackConfig,
  args: ParsedArgs,
  sourcePath: string,
  id: string,
  fileName: string,
  deps: UploadsDeps,
  timeoutMs: number,
  maxBytes: number
): Promise<string> {
  const remoteDir = `${remoteUploadsRoot()}/incoming/${id}`;
  const remotePath = `${remoteDir}/${fileName}`;
  await runStdinChecked(
    deps.runWithStdinFile(controlShellArgs(cfg, args, remoteInstallScript(deps.quoteForBash(remoteDir), deps.quoteForBash(remotePath)), deps), sourcePath, {
      maxBytes,
      timeoutMs
    }),
    `stage ${fileName} on control host`
  );
  return remotePath;
}

async function readLocalUploads(root: string): Promise<BrainstackUploadManifest[]> {
  if (!existsSync(root)) {
    return [];
  }
  const manifests: BrainstackUploadManifest[] = [];
  async function walk(path: string, depth: number): Promise<void> {
    if (depth > 4) {
      return;
    }
    const entries = await readdir(path, { withFileTypes: true }).catch(() => []);
    for (const entry of entries) {
      const entryPath = join(path, entry.name);
      if (entry.isDirectory()) {
        await walk(entryPath, depth + 1);
      } else if (entry.isFile() && entry.name === "manifest.json") {
        try {
          manifests.push(JSON.parse(await readFile(entryPath, "utf8")) as BrainstackUploadManifest);
        } catch {
          // Ignore malformed manifests; doctor can grow a stricter check later.
        }
      }
    }
  }
  await walk(root, 0);
  return manifests;
}

function listRemoteUploadsScript(): string {
  return [
    "set -euo pipefail",
    'root="$HOME/.local/state/brainstack/uploads"',
    '[ -d "$root" ] || exit 0',
    'find "$root" -mindepth 3 -maxdepth 3 -type f -name manifest.json -print0 | while IFS= read -r -d "" file; do',
    '  cat "$file"',
    "  printf '\\n'",
    "done"
  ].join("\n");
}

function parseJsonLines(text: string): BrainstackUploadManifest[] {
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .flatMap((line) => {
      try {
        return [JSON.parse(line) as BrainstackUploadManifest];
      } catch {
        return [];
      }
    });
}

function filterAndSortUploads(
  uploads: BrainstackUploadManifest[],
  options: { recent: boolean; sinceHours: number | null; limit: number }
): BrainstackUploadManifest[] {
  const sinceMs =
    options.recent || options.sinceHours !== null
      ? Date.now() - (options.sinceHours ?? 24) * 60 * 60 * 1000
      : null;
  return uploads
    .filter((upload) => {
      if (upload.schema_version !== UPLOADS_SCHEMA_VERSION) {
        return false;
      }
      if (sinceMs === null) {
        return true;
      }
      const uploadedAt = Date.parse(upload.uploaded_at);
      return Number.isFinite(uploadedAt) && uploadedAt >= sinceMs;
    })
    .sort((a, b) => Date.parse(b.uploaded_at) - Date.parse(a.uploaded_at))
    .slice(0, options.limit);
}

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) {
    return "unknown size";
  }
  if (bytes >= 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(bytes >= 10 * 1024 * 1024 ? 1 : 2)} MiB`;
  }
  if (bytes >= 1024) {
    return `${(bytes / 1024).toFixed(bytes >= 10 * 1024 ? 1 : 2)} KiB`;
  }
  return `${bytes} bytes`;
}

function formatUploads(uploads: BrainstackUploadManifest[]): string {
  if (!uploads.length) {
    return "No uploads found.";
  }
  return uploads
    .map((upload) =>
      [
        `${upload.id} ${upload.file_name}`,
        `machine=${upload.machine}`,
        `size=${formatBytes(upload.size_bytes)}`,
        `path=${upload.remote_path}`,
        upload.label ? `label=${upload.label}` : null,
        `uploaded=${upload.uploaded_at}`
      ]
        .filter(Boolean)
        .join(" ")
    )
    .join("\n");
}

function delegatedControlFailure(label: string, result: RunResult): Error {
  if (result.timedOut) {
    return new Error(`${label} failed on control host (timed out)`);
  }
  const output = [result.stderr.trim(), result.stdout.trim()].filter(Boolean).join("\n");
  if (/Unknown command:\s+uploads\b/.test(output)) {
    return new Error(`${label} failed: control host Brainstack is too old for uploads. Update Brainstack on the control host, then retry.`);
  }
  return new Error(`${label} failed on control host (exit ${result.code})${output ? `\n${output}` : ""}`);
}

function remoteDeleteScript(id: string): string {
  return [
    "set -euo pipefail",
    `id=${id}`,
    'root="$HOME/.local/state/brainstack/uploads"',
    '[ -d "$root" ] || exit 0',
    'match="$(find "$root" -mindepth 2 -maxdepth 2 -type d -name "$id" -print -quit)"',
    '[ -n "$match" ] || exit 0',
    'case "$match" in "$root"/*) rm -rf -- "$match" ;; *) exit 65 ;; esac',
    "printf 'deleted=%s\\n' \"$id\""
  ].join("\n");
}

export function createUploadsCommands(deps: UploadsDeps) {
  async function load(args: ParsedArgs): Promise<{ configPath: string; cfg: BrainstackConfig }> {
    const configPath = deps.abs(deps.requireFlagValue(args, "config") || deps.brainstackDefaultConfigPath());
    return {
      configPath,
      cfg: await deps.loadConfig(configPath, deps.flag(args, "profile"), deps.flag(args, "root"))
    };
  }

  async function delegateClientCommand(args: ParsedArgs, cfg: BrainstackConfig, argv: string[], timeoutMs: number): Promise<RunResult> {
    const remoteRepo =
      deps.requireFlagValue(args, "remote-repo") ||
      process.env.BRAINSTACK_TELEGRAM_REMOTE_REPO?.trim() ||
      cfg.client.telegramRemoteRepo ||
      "~/brainstack";
    const result = deps.runControlRemoteScript(
      cfg,
      args,
      "uploads control SSH target",
      deps.remoteBrainctlScript(remoteRepo, argv, { preferInstalledBinary: true }),
      timeoutMs,
      { allowRemoteSshFallback: true }
    );
    if (!result) {
      throw new Error("uploads needs a control SSH target for client-macos installs");
    }
    return result;
  }

  async function commandPut(args: ParsedArgs): Promise<void> {
    const { cfg } = await load(args);
    const sourceInput = deps.requireFlagValue(args, "file") || args.positional[1];
    if (!sourceInput) {
      throw new Error("uploads put requires --file PATH");
    }
    const machine = resolveMachine(args, cfg);
    const sourcePath = deps.absWithHome(sourceInput, cfg.paths.home);
    const maxBytes = deps.parsePositiveIntegerFlag(args, "max-bytes", DEFAULT_MAX_UPLOAD_BYTES);
    const timeoutMs = deps.parsePositiveIntegerFlag(args, "timeout-ms", DEFAULT_UPLOAD_TIMEOUT_MS);
    const id = validateUploadId(deps.requireFlagValue(args, "id") || uploadId());
    const originalName = deps.requireFlagValue(args, "name") || basename(sourcePath);
    const fileName = safeUploadFileName(originalName);
    const label = deps.requireFlagValue(args, "label") || null;
    const source = deps.requireFlagValue(args, "source") || (cfg.profile === "client-macos" ? "client-macos" : cfg.machine.name);
    const validated = await validateSourceFile(sourcePath, maxBytes);
    const uploadedAt = new Date().toISOString();

  if (cfg.profile === "client-macos" && !isLocalMachine(cfg, machine)) {
      const stagedPath = await stageFileOnControl(cfg, args, sourcePath, id, fileName, deps, timeoutMs, maxBytes);
      const remoteArgs = [
        "uploads",
        "put",
        "--machine",
        machine,
        "--file",
        stagedPath,
        "--name",
        originalName,
        "--id",
        id,
        "--source",
        source,
        "--max-bytes",
        String(maxBytes),
        "--delete-source",
        ...(label ? ["--label", label] : []),
        ...(deps.hasFlag(args, "json") ? ["--json"] : [])
      ];
      const delegated = await delegateClientCommand(args, cfg, remoteArgs, timeoutMs);
      if (delegated.code !== 0 || delegated.timedOut) {
        throw delegatedControlFailure("upload", delegated);
      }
      if (delegated.stdout) process.stdout.write(delegated.stdout);
      if (delegated.stderr) process.stderr.write(delegated.stderr);
      return;
    }

    const baseManifest: BrainstackUploadManifest = {
      schema_version: UPLOADS_SCHEMA_VERSION,
      id,
      machine,
      source,
      original_name: originalName,
      file_name: fileName,
      label,
      size_bytes: validated.size,
      sha256: validated.sha256,
      uploaded_at: uploadedAt,
      remote_path: "",
      manifest_path: ""
    };

    const manifest = isLocalMachine(cfg, machine)
      ? await installLocalUpload(cfg, sourcePath, baseManifest)
      : await (async () => {
          const worker = findWorker(cfg, machine, deps);
          if (!worker) {
            throw new Error(`unknown upload target machine: ${machine}`);
          }
          return await installRemoteUpload(cfg, worker, sourcePath, baseManifest, deps, timeoutMs);
        })();

    if (deps.hasFlag(args, "delete-source")) {
      const incomingRoot = join(uploadsRoot(cfg), "incoming") + sep;
      if (sourcePath.startsWith(incomingRoot)) {
        await rm(sourcePath, { force: true });
        await rm(dirname(sourcePath), { recursive: true, force: true });
      }
    }

    if (deps.hasFlag(args, "json")) {
      console.log(JSON.stringify({ ok: true, upload: manifest }, null, 2));
    } else {
      console.log(`Uploaded ${manifest.file_name} to ${manifest.machine}`);
      console.log(`id=${manifest.id}`);
      console.log(`path=${manifest.remote_path}`);
      console.log(`size=${formatBytes(manifest.size_bytes)}`);
      console.log(`sha256=${manifest.sha256}`);
    }
  }

  async function listForMachine(args: ParsedArgs, cfg: BrainstackConfig, machine: string): Promise<BrainstackUploadManifest[]> {
    if (cfg.profile === "client-macos" && !isLocalMachine(cfg, machine)) {
      const remoteArgs = [
        "uploads",
        "list",
        "--machine",
        machine,
        "--json",
        ...(deps.hasFlag(args, "recent") ? ["--recent"] : []),
        ...(deps.requireFlagValue(args, "since-hours") ? ["--since-hours", deps.requireFlagValue(args, "since-hours")!] : []),
        "--limit",
        String(deps.parsePositiveIntegerFlag(args, "limit", 25))
      ];
      const result = await delegateClientCommand(args, cfg, remoteArgs, deps.parsePositiveIntegerFlag(args, "timeout-ms", 30_000));
      if (result.code !== 0 || result.timedOut) {
        throw delegatedControlFailure("upload list", result);
      }
      const parsed = JSON.parse(result.stdout) as { uploads?: BrainstackUploadManifest[] };
      return parsed.uploads || [];
    }

    const recent = deps.hasFlag(args, "recent");
    const sinceHoursRaw = deps.requireFlagValue(args, "since-hours");
    const sinceHours = sinceHoursRaw ? deps.parsePositiveIntegerFlag(args, "since-hours", 24) : null;
    const limit = deps.parsePositiveIntegerFlag(args, "limit", 25);
    if (isLocalMachine(cfg, machine)) {
      return filterAndSortUploads(await readLocalUploads(uploadsRoot(cfg)), { recent, sinceHours, limit });
    }

    const worker = findWorker(cfg, machine, deps);
    if (!worker) {
      throw new Error(`unknown upload target machine: ${machine}`);
    }
    const result = deps.runWorkerShell(cfg, worker, listRemoteUploadsScript(), 30, true);
    if (result.code !== 0 || result.timedOut) {
      throw new Error(`upload list failed on ${worker.name}${result.timedOut ? " (timed out)" : ` (exit ${result.code})`}\n${result.stderr || result.stdout}`);
    }
    return filterAndSortUploads(parseJsonLines(result.stdout), { recent, sinceHours, limit });
  }

  async function commandList(args: ParsedArgs): Promise<void> {
    const { cfg } = await load(args);
    const machine = resolveMachine(args, cfg);
    const uploads = await listForMachine(args, cfg, machine);
    if (deps.hasFlag(args, "json")) {
      console.log(JSON.stringify({ ok: true, machine, uploads }, null, 2));
    } else {
      console.log(formatUploads(uploads));
    }
  }

  async function commandDelete(args: ParsedArgs): Promise<void> {
    const { cfg } = await load(args);
    const machine = resolveMachine(args, cfg);
    const id = validateUploadId(deps.requireFlagValue(args, "id") || args.positional[1] || "");
    if (cfg.profile === "client-macos" && !isLocalMachine(cfg, machine)) {
      const result = await delegateClientCommand(
        args,
        cfg,
        ["uploads", "rm", "--machine", machine, "--id", id, ...(deps.hasFlag(args, "json") ? ["--json"] : [])],
        deps.parsePositiveIntegerFlag(args, "timeout-ms", 30_000)
      );
      if (result.code !== 0 || result.timedOut) {
        throw delegatedControlFailure("upload delete", result);
      }
      if (result.stdout) process.stdout.write(result.stdout);
      if (result.stderr) process.stderr.write(result.stderr);
      return;
    }

    let deleted = false;
    if (isLocalMachine(cfg, machine)) {
      const uploads = await readLocalUploads(uploadsRoot(cfg));
      const match = uploads.find((upload) => upload.id === id);
      if (match) {
        await rm(dirname(expandLocalUploadPath(match.manifest_path || manifestPathForLocal(cfg, match))), { recursive: true, force: true });
        deleted = true;
      }
    } else {
      const worker = findWorker(cfg, machine, deps);
      if (!worker) {
        throw new Error(`unknown upload target machine: ${machine}`);
      }
      const result = deps.runWorkerShell(cfg, worker, remoteDeleteScript(deps.quoteForBash(id)), 30, true);
      if (result.code !== 0 || result.timedOut) {
        throw new Error(`upload delete failed on ${worker.name}${result.timedOut ? " (timed out)" : ` (exit ${result.code})`}\n${result.stderr || result.stdout}`);
      }
      deleted = true;
    }

    if (deps.hasFlag(args, "json")) {
      console.log(JSON.stringify({ ok: true, id, machine, deleted }, null, 2));
    } else {
      console.log(deleted ? `Deleted upload ${id} from ${machine}.` : `Upload ${id} was not found on ${machine}.`);
    }
  }

  async function commandUploads(args: ParsedArgs): Promise<void> {
    const sub = args.positional[0] || "list";
    if (sub === "help" || sub === "--help" || sub === "-h") {
      console.log(
        "Usage: brainctl uploads put --machine MACHINE --file PATH [--label TEXT] [--name NAME] [--json]\n" +
          "       brainctl uploads list [--machine MACHINE] [--recent] [--since-hours N] [--limit N] [--json]\n" +
          "       brainctl uploads rm --machine MACHINE --id UPLOAD_ID [--json]"
      );
      return;
    }
    switch (sub) {
      case "put":
      case "upload":
        return await commandPut(args);
      case "list":
      case "ls":
      case "recent":
        if (sub === "recent") {
          args.flags.recent = true;
        }
        return await commandList(args);
      case "rm":
      case "remove":
      case "delete":
      case "discard":
        return await commandDelete(args);
      default:
        throw new Error(`Unknown uploads subcommand: ${sub}`);
    }
  }

  return {
    commandUploads
  };
}
