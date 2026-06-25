import { existsSync } from "node:fs";
import { appendFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { canonicalJson, sha256Hex } from "../../../packages/outbox/src/outbox";
import type { CodexReasoningEffort } from "./codex-runtime";
import { loadWorkerConfigsFromPath, type FactoryConfig, type FactoryWorkerConfig, type HarnessName } from "./config";
import { ContextKind, ContextRecord, ContextState, FactoryDb, WorkerRecord } from "./db";
import { CodexProgressLineParser, type HarnessProgressEvent } from "./harness-progress";
import { isProtectedArtifactPath, TELEGRAM_ATTACHMENTS_WORKSPACE_PATH } from "./telegram-attachments";

export interface WorkerExecResult {
  ok: boolean;
  host: string;
  transport: string;
  exitCode: number;
  stdout: string;
  stderr: string;
  durationMs: number;
  commandLabel: string;
}

const UPDATE_CHECK_CONCURRENCY = 4;
const UPDATE_CHECK_MAX_TARGETS = 32;
const CONTROL_EVENT_BUFFER_MAX_CHARS = 1024 * 1024;

export interface BootstrapResult extends WorkerExecResult {
  kind: ContextKind;
  state: ContextState;
  target: string;
  rootPath: string;
  worktreePath: string;
  branchName: string | null;
  baseBranch: string | null;
}

export interface CodexRunResult extends WorkerExecResult {
  sessionId: string | null;
}

export interface WorkspaceArtifactFile {
  path: string;
  fileName: string;
  sizeBytes: number;
  mimeType: string;
  content: Uint8Array;
}

export interface WorkspaceArtifactDeleteResult {
  requestedPath: string;
  resolvedPath: string | null;
  fileName: string;
  status: "deleted" | "missing";
}

export interface WorkspaceSeedFile {
  relativePath: string;
  content: Uint8Array;
}

export interface CodexRunOptions {
  onSessionId?: (sessionId: string) => Promise<void> | void;
  onCompaction?: () => Promise<void> | void;
  onProgress?: (event: HarnessProgressEvent) => Promise<void> | void;
  workspaceFiles?: WorkspaceSeedFile[];
  imagePaths?: string[];
  modelOverride?: string | null;
  reasoningEffortOverride?: CodexReasoningEffort | null;
}

export interface WorkerTranscriptionRequest {
  fileName: string;
  bytes: Uint8Array;
  command: string;
  args: string[];
  timeoutMs: number;
}

interface ContextBootstrapRequest {
  slug: string;
  machine: string;
  target: string;
  baseBranch: string | null;
}

interface ContextPlan {
  kind: ContextKind;
  target: string;
  rootPath: string;
  worktreePath: string;
  branchName: string | null;
  baseBranch: string | null;
}

function nowIso(): string {
  return new Date().toISOString();
}

function quoteSh(value: string): string {
  return `'${value.replaceAll("'", `'\"'\"'`)}'`;
}

function safeTempFileName(value: string): string {
  const cleaned = value.replaceAll("\\", "/").split("/").pop()?.replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "");
  return cleaned || "audio";
}

function transcriptionCommandLine(command: string, args: string[]): string {
  const normalizedArgs = args.length ? [...args] : ["{input}"];
  if (!normalizedArgs.includes("{input}")) {
    normalizedArgs.push("{input}");
  }

  return [
    quoteSh(command),
    ...normalizedArgs.map((arg) => (arg === "{input}" ? '"$input"' : quoteSh(arg)))
  ].join(" ");
}

function transcriptionPreprocessScript(): string {
  return `
converted_input="$tmp_dir/input.wav"
if command -v ffmpeg >/dev/null 2>&1; then
  if ffmpeg -hide_banner -loglevel error -y -i "$input" -ar 16000 -ac 1 "$converted_input" >/dev/null 2>&1; then
    input="$converted_input"
  fi
fi
`.trim();
}

interface WorkerEnvCacheRecord {
  worker: string;
  fingerprint: string;
  path: string;
  harness: string;
  harnessBin: string;
  harnessVersion: string;
  detectedAt: string;
}

function workerEnvCachePath(config: FactoryConfig): string {
  return join(config.stateRoot, "worker-env-cache.json");
}

async function readWorkerEnvCache(config: FactoryConfig): Promise<Record<string, WorkerEnvCacheRecord>> {
  const path = workerEnvCachePath(config);
  if (!existsSync(path)) {
    return {};
  }
  try {
    return JSON.parse(await readFile(path, "utf8")) as Record<string, WorkerEnvCacheRecord>;
  } catch {
    return {};
  }
}

async function writeWorkerEnvCache(config: FactoryConfig, cache: Record<string, WorkerEnvCacheRecord>): Promise<void> {
  await mkdir(dirname(workerEnvCachePath(config)), { recursive: true });
  await writeFile(workerEnvCachePath(config), `${JSON.stringify(cache, null, 2)}\n`, { mode: 0o600 });
}

function workerEnvFingerprint(worker: FactoryWorkerConfig, harness: { family: HarnessName; bin: string }): string {
  return sha256Hex(canonicalJson({
    worker: worker.name,
    transport: worker.transport,
    sshTarget: worker.sshTarget || null,
    sshUser: worker.sshUser || null,
    harness: harness.family,
    harnessBin: harness.bin
  }));
}

function workerUserPathPrelude(): string {
  return `
# Brainstack itself stays Bun-only, but worker harness commands are resolved
# through the target user's own shell PATH so existing codex/claude installs work.
if [ -z "\${BRAINSTACK_SKIP_USER_PATH_RESOLVE:-}" ]; then
  __brainstack_detected_path=""
  if [ -n "\${BRAINSTACK_WORKER_PATH:-}" ]; then
    __brainstack_detected_path="$BRAINSTACK_WORKER_PATH"
  elif [ -n "\${SHELL:-}" ] && [ -x "$SHELL" ]; then
    __brainstack_detected_path="$(
      if command -v timeout >/dev/null 2>&1; then
        timeout 5s "$SHELL" -lic 'printf "__BRAINSTACK_PATH__%s\\n" "$PATH"' 2>/dev/null
      else
        "$SHELL" -lic 'printf "__BRAINSTACK_PATH__%s\\n" "$PATH"' 2>/dev/null &
        __brainstack_path_pid=$!
        __brainstack_path_elapsed=0
        while kill -0 "$__brainstack_path_pid" 2>/dev/null; do
          if [ "$__brainstack_path_elapsed" -ge 5 ]; then
            kill "$__brainstack_path_pid" 2>/dev/null || true
            sleep 1
            if kill -0 "$__brainstack_path_pid" 2>/dev/null; then
              kill -9 "$__brainstack_path_pid" 2>/dev/null || true
            fi
            break
          fi
          sleep 1
          __brainstack_path_elapsed=$((__brainstack_path_elapsed + 1))
        done
        wait "$__brainstack_path_pid" 2>/dev/null || true
      fi | sed -n 's/.*__BRAINSTACK_PATH__//p' | tail -n 1
    )"
  fi
  if [ -n "$__brainstack_detected_path" ]; then
    PATH="$__brainstack_detected_path:$PATH"
    export PATH
  fi
  unset __brainstack_detected_path
fi
`.trim();
}

function truthyEnv(value: string | undefined): boolean {
  return ["1", "true", "yes", "on"].includes((value || "").trim().toLowerCase());
}

function sshTrustArgs(config: FactoryConfig, worker: FactoryWorkerConfig): string[] {
  const knownHostsPath = worker.sshKnownHostsPath || config.sshKnownHostsPath;
  const trustMode = worker.sshTrustMode || "pinned";
  return [
    "-o",
    trustMode === "accept-new" ? "StrictHostKeyChecking=accept-new" : "StrictHostKeyChecking=yes",
    "-o",
    `UserKnownHostsFile=${knownHostsPath}`
  ];
}

function workerSshHost(worker: FactoryWorkerConfig): string {
  const target = worker.sshTarget || worker.name;
  const withoutUser = target.includes("@") ? target.slice(target.lastIndexOf("@") + 1) : target;
  const bracketMatch = withoutUser.match(/^\[([^\]]+)\](?::\d+)?$/);
  if (bracketMatch) {
    return bracketMatch[1];
  }
  if (/^[^:]+:\d+$/.test(withoutUser)) {
    return withoutUser.replace(/:\d+$/, "");
  }
  return withoutUser;
}

function workerSshEmbeddedUser(worker: FactoryWorkerConfig): string | null {
  const target = worker.sshTarget || worker.name;
  return target.includes("@") ? target.slice(0, target.lastIndexOf("@")) : null;
}

function workerSshPort(worker: FactoryWorkerConfig): string | null {
  const target = worker.sshTarget || worker.name;
  const withoutUser = target.includes("@") ? target.slice(target.lastIndexOf("@") + 1) : target;
  const bracketMatch = withoutUser.match(/^\[[^\]]+\]:(\d+)$/);
  if (bracketMatch) {
    return bracketMatch[1];
  }
  const hostPort = withoutUser.match(/^[^:]+:(\d+)$/);
  return hostPort ? hostPort[1] : null;
}

function workerSshPortArgs(worker: FactoryWorkerConfig): string[] {
  const port = workerSshPort(worker);
  return port ? ["-p", port] : [];
}

function uniqueHosts(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))].sort();
}

function isPathTarget(target: string): boolean {
  return target.startsWith("/") || target.startsWith("~");
}

function isGitUrl(target: string): boolean {
  return /^(?:https?:\/\/|ssh:\/\/|git@|file:\/\/|[^@\s]+@[^:\s]+:).+/.test(target);
}

function cleanRelativePath(relativePath: string): string {
  const normalized = relativePath.replaceAll("\\", "/").replace(/^\/+/, "");
  if (!normalized || normalized.split("/").includes("..")) {
    throw new Error(`Unsafe workspace file path: ${relativePath}`);
  }

  return normalized;
}

function cleanArtifactDeletePath(filePath: string): string {
  const normalized = filePath.replaceAll("\\", "/");
  if (
    !normalized ||
    normalized.startsWith("/") ||
    normalized === "~" ||
    normalized.startsWith("~/") ||
    normalized.split("/").includes("..")
  ) {
    throw new Error(`Unsafe artifact delete path: ${filePath}`);
  }

  return normalized;
}

function probeField(output: string, key: string): string | null {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return output.match(new RegExp(`^${escaped}=([^\\n]*)$`, "m"))?.[1]?.trim() || null;
}

function runtimeFromUpdateOutput(output: string): { harness: HarnessName; harnessBin: string; model: string; effort: string } | null {
  const harness = probeField(output, "runtime_harness");
  if (harness !== "codex" && harness !== "claude") {
    return null;
  }
  return {
    harness,
    harnessBin: probeField(output, "runtime_harness_bin") || harness,
    model: probeField(output, "runtime_model") || "default",
    effort: probeField(output, "runtime_effort") || (harness === "claude" ? "n/a" : "default")
  };
}

function eventLooksLikeCompaction(value: unknown): boolean {
  if (!value || typeof value !== "object") {
    return false;
  }

  if (Array.isArray(value)) {
    return value.some(eventLooksLikeCompaction);
  }

  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    if (typeof child === "string" && /^(type|event|kind|name|status)$/.test(key) && /compact/i.test(child)) {
      return true;
    }
    if (child && typeof child === "object" && eventLooksLikeCompaction(child)) {
      return true;
    }
  }

  return false;
}

function lineLooksLikeCompactionEvent(line: string): boolean {
  const trimmed = line.trim();
  if (!trimmed.startsWith("{")) {
    return false;
  }

  try {
    return eventLooksLikeCompaction(JSON.parse(trimmed));
  } catch {
    return false;
  }
}

function parseSessionId(output: string): string | null {
  const patterns = [
    /"session_id":"([^"]+)"/,
    /"thread_id":"([^"]+)"/
  ];

  for (const pattern of patterns) {
    const match = output.match(pattern);
    if (match?.[1]) {
      return match[1];
    }
  }

  return null;
}

function appendBoundedOutput(current: string, chunk: string, maxBytes: number): string {
  if (maxBytes <= 0) {
    return "";
  }
  const combined = `${current}${chunk}`;
  if (Buffer.byteLength(combined, "utf8") <= maxBytes) {
    return combined;
  }

  const prefix = `[output truncated to last ${maxBytes} bytes]\n`;
  const tailBudget = Math.max(0, maxBytes - Buffer.byteLength(prefix, "utf8"));
  let start = Math.max(0, combined.length - maxBytes);
  let tail = combined.slice(start);
  while (Buffer.byteLength(tail, "utf8") > tailBudget && start < combined.length) {
    start += 1;
    tail = combined.slice(start);
  }
  return `${prefix}${tail}`;
}

function requiredHarnessFlags(harness: HarnessName): string[] {
  return harness === "codex"
    ? ["--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check", "--output-last-message"]
    : ["--dangerously-skip-permissions", "--permission-mode", "--output-format"];
}

function buildWorkspaceSeedScript(files: WorkspaceSeedFile[]): string {
  return files
    .map((file, index) => {
      const safePath = cleanRelativePath(file.relativePath);
      const encoded = Buffer.from(file.content).toString("base64");
      const marker = `__CODEX_WORKSPACE_FILE_${index}__`;

      return [
        `cat <<'${marker}' | brainstack_base64_decode | brainstack_safe_write_file ${quoteSh(safePath)}`,
        encoded,
        marker
      ].join("\n");
    })
    .join("\n");
}

function base64DecodeShellFunction(): string {
  return `
brainstack_base64_decode() {
  if printf '' | base64 --decode >/dev/null 2>&1; then
    base64 --decode
  elif printf '' | base64 -d >/dev/null 2>&1; then
    base64 -d
  else
    base64 -D
  fi
}
`.trim();
}

export class WorkerService {
  private currentWorkers: FactoryWorkerConfig[];
  private workerConfigError: string | null = null;
  private loadedWorkersFromFile: boolean;

  constructor(
    private readonly config: FactoryConfig,
    private readonly db: FactoryDb
  ) {
    this.currentWorkers = config.workers;
    this.loadedWorkersFromFile = Boolean(config.workersFileInitiallyLoaded);
  }

  knownHosts(): string[] {
    const contextHosts = this.db.listContexts().map((context) => context.machine);
    const workerHosts = this.db.listWorkers().map((worker) => worker.host);
    const configured = this.configuredWorkers().map((worker) => worker.name);
    return uniqueHosts([...configured, ...contextHosts, ...workerHosts]);
  }

  getWorkerConfig(machine: string): FactoryWorkerConfig | null {
    return this.configuredWorkers().find((worker) => worker.name === machine) || null;
  }

  private async cachedWorkerPath(worker: FactoryWorkerConfig, harness: { family: HarnessName; bin: string }): Promise<string | null> {
    const cache = await readWorkerEnvCache(this.config);
    const record = cache[worker.name];
    if (!record || record.fingerprint !== workerEnvFingerprint(worker, harness) || !record.path.trim()) {
      return null;
    }
    const detectedAt = Date.parse(record.detectedAt);
    if (!Number.isFinite(detectedAt) || Date.now() - detectedAt > 7 * 24 * 60 * 60 * 1000) {
      return null;
    }
    return record.path;
  }

  private async saveWorkerPathCache(worker: FactoryWorkerConfig, harness: { family: HarnessName; bin: string }, details: string): Promise<void> {
    const pathValue = probeField(details, "path");
    if (!pathValue) {
      return;
    }
    const cache = await readWorkerEnvCache(this.config);
    cache[worker.name] = {
      worker: worker.name,
      fingerprint: workerEnvFingerprint(worker, harness),
      path: pathValue,
      harness: harness.family,
      harnessBin: probeField(details, "harness_bin") || harness.bin,
      harnessVersion: probeField(details, "harness_version") || "",
      detectedAt: new Date().toISOString()
    };
    await writeWorkerEnvCache(this.config, cache);
  }

  async refreshWorkers(): Promise<WorkerRecord[]> {
    const workers = await Promise.all(this.knownHosts().map((host) => this.probeWorker(host)));
    if (this.workerConfigError) {
      workers.push(
        this.db.saveWorker({
          host: "workers-config",
          transport: "file",
          status: "error",
          reachable: false,
          localExecution: false,
          sshTarget: null,
          sshUser: null,
          lastCheckedAt: nowIso(),
          lastSeenAt: null,
          lastError: this.workerConfigError,
          details: this.config.workersFilePath,
          updatedAt: nowIso()
        })
      );
    }
    return workers.sort((left, right) => left.host.localeCompare(right.host));
  }

  private configuredWorkers(): FactoryWorkerConfig[] {
    if (!this.config.workersFilePath) {
      return this.currentWorkers;
    }

    try {
      if (!existsSync(this.config.workersFilePath)) {
        if (this.config.workersFileExplicit || this.loadedWorkersFromFile) {
          throw new Error(`workers file does not exist: ${this.config.workersFilePath}`);
        }

        this.workerConfigError = null;
        return this.currentWorkers;
      }

      this.currentWorkers = loadWorkerConfigsFromPath(this.config.workersFilePath, {
        localMachine: this.config.localMachine,
        managedRepoRoot: this.config.managedRepoRoot,
        managedHostRoot: this.config.managedHostRoot,
        managedScratchRoot: this.config.managedScratchRoot
      });
      this.loadedWorkersFromFile = true;
      this.workerConfigError = null;
    } catch (error) {
      this.workerConfigError = error instanceof Error ? error.message : String(error);
      return [];
    }

    return this.currentWorkers;
  }

  async probeWorker(host: string): Promise<WorkerRecord> {
    const worker = this.getWorkerConfig(host);
    if (!worker) {
      return this.db.saveWorker({
        host,
        transport: "n/a",
        status: "unconfigured",
        reachable: false,
        localExecution: false,
        sshTarget: null,
        sshUser: null,
        lastCheckedAt: nowIso(),
        lastSeenAt: this.db.getWorker(host)?.lastSeenAt || null,
        lastError: `No worker config for ${host}`,
        details: null,
        updatedAt: nowIso()
      });
    }

    const harness = this.resolveHarness(worker);
    const result = await this.runWorkerScript(
      worker,
      [
        "set -euo pipefail",
        "printf 'hostname=%s\\n' \"$(hostname)\"",
        "printf 'cwd=%s\\n' \"$PWD\"",
        "if command -v git >/dev/null 2>&1; then printf 'git=1\\n'; else printf 'git=0\\n'; fi",
        `harness=${quoteSh(harness.family)}`,
        `harness_bin=${quoteSh(harness.bin)}`,
        ...(worker.transport === "local" ? [workerUserPathPrelude()] : []),
        "printf 'path=%s\\n' \"$PATH\"",
        "brainstack_config_value() { key=\"$1\"; file=\"$2\"; [ -f \"$file\" ] || return 0; awk -v key=\"$key\" 'BEGIN { in_section=0 } /^[[:space:]]*\\[/ { in_section=1 } in_section == 0 { pattern=\"^[[:space:]]*\" key \"[[:space:]]*=\"; if ($0 ~ pattern) { sub(/^[^=]*=[[:space:]]*/, \"\"); sub(/[[:space:]]*#.*/, \"\"); gsub(/^[[:space:]\"]+|[[:space:]\"]+$/, \"\"); print; exit } }' \"$file\" || true; }",
        "printf 'harness=%s\\n' \"$harness\"",
        "if command -v \"$harness_bin\" >/dev/null 2>&1; then printf 'harness_bin=1\\n'; else printf 'harness_bin=0\\n'; fi",
        "printf 'harness_version=%s\\n' \"$($harness_bin --version 2>&1 | head -n 1 || true)\"",
        "if [ \"$harness\" = codex ]; then help=\"$($harness_bin exec --help 2>&1 || true)\"; else help=\"$($harness_bin --help 2>&1 || true)\"; fi",
        ...requiredHarnessFlags(harness.family).map((needle) => `case "$help" in *${quoteSh(needle)}*) printf 'flag:${needle}=1\\n' ;; *) printf 'flag:${needle}=0\\n' ;; esac`),
        "if [ \"$harness\" = codex ]; then codex_config=\"${CODEX_HOME:-$HOME/.codex}/config.toml\"; model_config=\"$(brainstack_config_value model \"$codex_config\")\"; effort_config=\"$(brainstack_config_value model_reasoning_effort \"$codex_config\")\"; printf 'model=%s\\n' \"${model_config:-default}\"; printf 'effort=%s\\n' \"${effort_config:-default}\"; else printf 'model=default\\n'; printf 'effort=n/a\\n'; fi",
        "if command -v sudo >/dev/null 2>&1; then if sudo -n true >/dev/null 2>&1; then printf 'sudo=ok\\n'; else printf 'sudo=fail\\n'; fi; else printf 'sudo=missing\\n'; fi",
        "printf 'home=%s\\n' \"$HOME\""
      ].join("\n"),
      undefined,
      8,
      undefined,
      undefined,
      false
    );

    const existing = this.db.getWorker(host);
    const details = result.ok ? result.stdout.trim() : "";
    if (result.ok) {
      await this.saveWorkerPathCache(worker, harness, details).catch((error) => {
        console.warn(`worker path cache update failed for ${worker.name}`, error);
      });
    }
    const probeIssues: string[] = [];
    if (result.ok) {
      if (probeField(details, "harness_bin") !== "1") {
        probeIssues.push(`harness binary not found: ${harness.bin}`);
      } else {
        for (const flag of requiredHarnessFlags(harness.family)) {
          if (probeField(details, `flag:${flag}`) !== "1") {
            probeIssues.push(`missing harness flag: ${flag}`);
          }
        }
      }

      const sudoStatus = probeField(details, "sudo");
      if (sudoStatus !== "ok") {
        probeIssues.push(sudoStatus === "missing" ? "sudo -n true missing" : sudoStatus === "fail" ? "sudo -n true failed" : "sudo probe missing");
      }
    }
    const workerRecord: WorkerRecord = {
      host,
      transport: worker.transport,
      status: result.ok ? (probeIssues.length ? "degraded" : "healthy") : "unreachable",
      reachable: result.ok,
      localExecution: worker.localExecution,
      sshTarget: worker.sshTarget,
      sshUser: worker.sshUser,
      lastCheckedAt: nowIso(),
      lastSeenAt: result.ok ? nowIso() : (existing?.lastSeenAt || null),
      lastError: result.ok ? (probeIssues.length ? probeIssues.join("; ") : null) : (result.stderr.trim() || result.stdout.trim() || `exit ${result.exitCode}`),
      details: result.ok ? details : null,
      updatedAt: nowIso()
    };

    return this.db.saveWorker(workerRecord);
  }

  describeWorkerRuntime(host: string, context?: ContextRecord): { harness: HarnessName; harnessBin: string; model: string; effort: string } | null {
    const worker = this.getWorkerConfig(host);
    if (!worker) {
      return null;
    }

    const contextOverride = context?.machine === host ? context : undefined;
    const harness = this.resolveHarness(worker, contextOverride);
    const latestProbe = this.db.getWorker(host);
    const probedModel = probeField(latestProbe?.details || "", "model");
    const probedEffort = probeField(latestProbe?.details || "", "effort");

    return {
      harness: harness.family,
      harnessBin: harness.bin,
      model: contextOverride?.modelOverride?.trim() || probedModel || "default",
      effort: contextOverride?.reasoningEffortOverride?.trim() || probedEffort || "default"
    };
  }

  async bootstrapContext(request: ContextBootstrapRequest): Promise<BootstrapResult> {
    const worker = this.requireWorker(request.machine);
    const plan = this.planContext(worker, request);
    const result = await this.runWorkerScript(worker, this.buildBootstrapScript(worker, request, plan), undefined, 45);

    if (!result.ok) {
      return {
        ...result,
        kind: plan.kind,
        state: worker.transport === "local" ? "error" : (this.isReachabilityError(result) ? "pending" : "error"),
        target: plan.target,
        rootPath: plan.rootPath,
        worktreePath: plan.worktreePath,
        branchName: plan.branchName,
        baseBranch: plan.baseBranch
      };
    }

    const rootMatch = result.stdout.match(/^ROOT=(.+)$/m);
    const worktreeMatch = result.stdout.match(/^WORKTREE=(.+)$/m);
    const branchMatch = result.stdout.match(/^BRANCH=(.*)$/m);

    return {
      ...result,
      kind: plan.kind,
      state: "active",
      target: plan.target,
      rootPath: rootMatch?.[1]?.trim() || plan.rootPath,
      worktreePath: worktreeMatch?.[1]?.trim() || plan.worktreePath,
      branchName: branchMatch?.[1]?.trim() || plan.branchName,
      baseBranch: plan.baseBranch
    };
  }

  async ensureContext(context: ContextRecord): Promise<BootstrapResult> {
    return this.bootstrapContext({
      slug: context.slug,
      machine: context.machine,
      target: context.target,
      baseBranch: context.baseBranch
    });
  }

  async runCodex(
    context: ContextRecord,
    prompt: string,
    mode: "run" | "resume" | "loop",
    logPath: string,
    options: CodexRunOptions = {}
  ): Promise<CodexRunResult> {
    await mkdir(dirname(logPath), { recursive: true });
    const worker = this.requireWorker(context.machine);
    const harness = this.resolveHarness(worker, context);
    await appendFile(logPath, `== ${new Date().toISOString()} ${mode} ${context.slug} on ${context.machine} via ${harness.family} ==\n`);
    const promptBase64 = Buffer.from(prompt, "utf8").toString("base64");
    const shouldResume = mode !== "run" && Boolean(context.codexSessionId);
    const resumeSessionId = context.codexSessionId || "";
    const workspaceFiles = options.workspaceFiles || [];
    const imagePaths = (options.imagePaths || []).map((imagePath) => cleanRelativePath(imagePath));
    const modelOverride = options.modelOverride?.trim() || "";
    const reasoningEffortOverride = options.reasoningEffortOverride?.trim() || "";
    const workspaceSeedScript = buildWorkspaceSeedScript(workspaceFiles);
    const codexImageArgScript = imagePaths.map((imagePath) => `image_args+=(--image ${quoteSh(imagePath)})`).join("\n");
    const codexModelArgScript = [
      "model_args=()",
      modelOverride ? `model_args+=(-m ${quoteSh(modelOverride)})` : "",
      reasoningEffortOverride
        ? `model_args+=(-c ${quoteSh(`model_reasoning_effort="${reasoningEffortOverride}"`)})`
        : ""
    ]
      .filter(Boolean)
      .join("\n");
    const claudeArgScript = [
      "claude_args=(-p --dangerously-skip-permissions --permission-mode bypassPermissions --output-format text)",
      modelOverride ? `claude_args+=(--model ${quoteSh(modelOverride)})` : "",
      reasoningEffortOverride ? `claude_args+=(--effort ${quoteSh(reasoningEffortOverride)})` : "",
      shouldResume && resumeSessionId ? `claude_args+=(-r ${quoteSh(resumeSessionId)})` : mode !== "run" ? "claude_args+=(-c)" : ""
    ]
      .filter(Boolean)
      .join("\n");

    const script = `
set -euo pipefail
${base64DecodeShellFunction()}
expand_home_path() {
  case "$1" in
    "~") printf '%s\\n' "$HOME" ;;
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

worktree_raw=${quoteSh(context.worktreePath)}
prompt_b64=${quoteSh(promptBase64)}
resume_session=${quoteSh(resumeSessionId)}
harness=${quoteSh(harness.family)}
harness_bin=${quoteSh(harness.bin)}
max_runtime_seconds=${quoteSh(String(this.config.workerRunTimeoutSeconds))}
${worker.transport === "local" ? workerUserPathPrelude() : ""}
worktree="$(expand_home_path "$worktree_raw")"
launcher_dir="$(mktemp -d "\${TMPDIR:-/tmp}/brainstack-run.XXXXXX")"
prompt_file="$launcher_dir/control-plane.prompt.md"
runner_file="$launcher_dir/run-harness.sh"

cleanup() {
  rm -rf "$launcher_dir"
}

kill_process_tree() {
  target_pid="$1"
  target_signal="$2"
  if command -v pgrep >/dev/null 2>&1; then
    for child_pid in $(pgrep -P "$target_pid" 2>/dev/null || true); do
      kill_process_tree "$child_pid" "$target_signal"
    done
  fi
  kill "-$target_signal" "$target_pid" 2>/dev/null || true
}

terminate_harness_run() {
  if [ -z "\${harness_pid:-}" ]; then
    return
  fi
  if [ -n "\${harness_pgid:-}" ]; then
    kill -TERM -- "-$harness_pgid" 2>/dev/null || true
  fi
  kill_process_tree "$harness_pid" TERM
  if command -v pkill >/dev/null 2>&1; then
    pkill -TERM -P "$harness_pid" 2>/dev/null || true
  fi
  kill "$harness_pid" 2>/dev/null || true
  for _brainstack_wait in 1 2; do
    if ! kill -0 "$harness_pid" 2>/dev/null; then
      return
    fi
    sleep 1
  done
  if [ -n "\${harness_pgid:-}" ] && kill -0 -- "-$harness_pgid" 2>/dev/null; then
    kill -KILL -- "-$harness_pgid" 2>/dev/null || true
  fi
  kill_process_tree "$harness_pid" KILL
  if command -v pkill >/dev/null 2>&1; then
    pkill -KILL -P "$harness_pid" 2>/dev/null || true
  fi
  if kill -0 "$harness_pid" 2>/dev/null; then
    kill -9 "$harness_pid" 2>/dev/null || true
  fi
}

handle_wrapper_signal() {
  terminate_harness_run
  cleanup
  exit 124
}

trap cleanup EXIT
trap handle_wrapper_signal TERM INT HUP

if ! command -v "$harness_bin" >/dev/null 2>&1; then
  echo "$harness binary not found on worker: $harness_bin" >&2
  exit 30
fi

printf '%s' "$prompt_b64" | brainstack_base64_decode > "$prompt_file"
cat > "$runner_file" <<'__BRAINSTACK_RUNNER__'
set -euo pipefail
${base64DecodeShellFunction()}
${codexModelArgScript}
${claudeArgScript}
image_args=()
cd "$worktree"
worktree_real="$(pwd -P)"

brainstack_safe_parent_real() {
  safe_path="$1"
  target_path="$worktree_real/$safe_path"
  target_parent="$(dirname "$target_path")"
  mkdir -p "$target_parent"
  parent_real="$(cd "$target_parent" && pwd -P)"
  case "$parent_real/" in
    "$worktree_real"/*) printf '%s\\n' "$parent_real" ;;
    *) echo "workspace path escaped worktree: $safe_path" >&2; return 1 ;;
  esac
}

brainstack_safe_write_file() {
  safe_path="$1"
  parent_real="$(brainstack_safe_parent_real "$safe_path")"
  target_path="$worktree_real/$safe_path"
  if [ -L "$target_path" ]; then
    echo "workspace write refused symlink target: $safe_path" >&2
    return 1
  fi
  tmp_file="$(mktemp "$parent_real/.brainstack-write.XXXXXX")"
  trap 'rm -f "$tmp_file"' RETURN
  cat > "$tmp_file"
  mv -f "$tmp_file" "$target_path"
  trap - RETURN
}

brainstack_safe_remove_file() {
  safe_path="$1"
  parent_real="$(brainstack_safe_parent_real "$safe_path")"
  target_path="$worktree_real/$safe_path"
  case "$target_path" in
    "$parent_real"/*) rm -f -- "$target_path" ;;
    *) echo "workspace remove escaped worktree: $safe_path" >&2; return 1 ;;
  esac
}

mkdir -p .factory
brainstack_safe_remove_file ${quoteSh(TELEGRAM_ATTACHMENTS_WORKSPACE_PATH)}
brainstack_safe_write_file .factory/control-plane.prompt.md < "$prompt_file"
${workspaceSeedScript}
${codexImageArgScript}

last_message_tmp="$(mktemp "\${TMPDIR:-/tmp}/brainstack-last-message.XXXXXX")"
trap 'rm -f "$last_message_tmp"' EXIT
if [ "$harness" = "claude" ]; then
  set +e
  "$harness_bin" "\${claude_args[@]}" < "$prompt_file" | tee "$last_message_tmp"
  status=\${PIPESTATUS[0]}
  set -e
  brainstack_safe_write_file .factory/last-message.txt < "$last_message_tmp"
  exit "$status"
else
  codex_args=("$harness_bin" exec)
  if ${shouldResume ? "true" : "false"}; then
    codex_args+=(resume)
  fi
  codex_args+=(--json --skip-git-repo-check --output-last-message "$last_message_tmp" --dangerously-bypass-approvals-and-sandbox)
  if ((\${#model_args[@]})); then
    codex_args+=("\${model_args[@]}")
  fi
  if ((\${#image_args[@]})); then
    codex_args+=("\${image_args[@]}")
  fi
  if ${shouldResume ? "true" : "false"}; then
    codex_args+=("$resume_session")
  fi
  codex_args+=(-)
  set +e
  "\${codex_args[@]}" < "$prompt_file"
  status=$?
  set -e
  if [ -s "$last_message_tmp" ]; then
    brainstack_safe_write_file .factory/last-message.txt < "$last_message_tmp"
  fi
  exit "$status"
fi
__BRAINSTACK_RUNNER__
chmod +x "$runner_file"
export worktree prompt_file resume_session harness harness_bin

if command -v setsid >/dev/null 2>&1; then
  setsid bash "$runner_file" &
  harness_pid=$!
  harness_pgid=$harness_pid
else
  bash "$runner_file" &
  harness_pid=$!
  harness_pgid=""
fi

run_started_at="$(date +%s)"
while kill -0 "$harness_pid" 2>/dev/null; do
  if [ "$max_runtime_seconds" -gt 0 ]; then
    now_seconds="$(date +%s)"
    if [ $((now_seconds - run_started_at)) -ge "$max_runtime_seconds" ]; then
      echo "harness run timed out after \${max_runtime_seconds}s" >&2
      terminate_harness_run
      exit 124
    fi
  fi

  if [ ! -d "$worktree" ]; then
    echo "worktree disappeared during harness run: $worktree" >&2
    terminate_harness_run
    exit 88
  fi

  sleep 1
done

wait "$harness_pid"
`;

    const currentBeforeLaunch = this.db.getContextBySlug(context.slug);
    if (currentBeforeLaunch?.state === "archived") {
      return {
        ok: false,
        host: worker.name,
        transport: worker.transport,
        exitCode: 89,
        stdout: "",
        stderr: `context archived before harness launch: ${context.slug}`,
        durationMs: 0,
        commandLabel: harness.family
      };
    }

    let stdoutBuffer = "";
    let stdoutControlLineBuffer = "";
    const progressParser = new CodexProgressLineParser();
    let seenSessionId: string | null = null;
    let seenCompaction = false;
    const consumeControlEvents = async (chunk: string) => {
      stdoutControlLineBuffer += chunk;
      const bufferedDetected = parseSessionId(stdoutControlLineBuffer);
      if (bufferedDetected && bufferedDetected !== seenSessionId) {
        seenSessionId = bufferedDetected;
        await options.onSessionId?.(bufferedDetected);
      }
      const lines = stdoutControlLineBuffer.split(/\r?\n/);
      stdoutControlLineBuffer = lines.pop() || "";
      if (stdoutControlLineBuffer.length > CONTROL_EVENT_BUFFER_MAX_CHARS) {
        stdoutControlLineBuffer = stdoutControlLineBuffer.slice(-CONTROL_EVENT_BUFFER_MAX_CHARS);
      }
      for (const line of lines) {
        const detected = parseSessionId(line);
        if (detected && detected !== seenSessionId) {
          seenSessionId = detected;
          await options.onSessionId?.(detected);
        }
        if (!seenCompaction && lineLooksLikeCompactionEvent(line)) {
          seenCompaction = true;
          await options.onCompaction?.();
        }
      }
    };
    const result = await this.runWorkerScript(
      worker,
      script,
      logPath,
      this.config.workerRunTimeoutSeconds,
      async (chunk) => {
        stdoutBuffer = appendBoundedOutput(stdoutBuffer, chunk, this.config.workerCaptureMaxBytes);
        await consumeControlEvents(chunk);
        if (harness.family === "codex") {
          for (const event of progressParser.push(chunk)) {
            await options.onProgress?.(event);
          }
        }
        const detected = parseSessionId(chunk);
        if (detected && detected !== seenSessionId) {
          seenSessionId = detected;
          await options.onSessionId?.(detected);
        }
        if (!seenCompaction && lineLooksLikeCompactionEvent(chunk)) {
          seenCompaction = true;
          await options.onCompaction?.();
        }
      },
      this.config.workerCaptureMaxBytes,
      true,
      harness
    );
    if (stdoutControlLineBuffer) {
      const detected = parseSessionId(stdoutControlLineBuffer);
      if (detected && detected !== seenSessionId) {
        seenSessionId = detected;
        await options.onSessionId?.(detected);
      }
    }
    if (!seenCompaction && lineLooksLikeCompactionEvent(stdoutControlLineBuffer)) {
      seenCompaction = true;
      await options.onCompaction?.();
    }
    if (harness.family === "codex") {
      for (const event of progressParser.flush()) {
        await options.onProgress?.(event);
      }
    }

    return {
      ...result,
      sessionId: seenSessionId || parseSessionId(result.stdout) || context.codexSessionId || null
    };
  }

  async runTranscription(workerName: string, request: WorkerTranscriptionRequest): Promise<WorkerExecResult> {
    const worker = this.requireWorker(workerName);
    const command = request.command.trim();
    if (!command) {
      return {
        ok: false,
        host: worker.name,
        transport: worker.transport,
        exitCode: 64,
        stdout: "",
        stderr: "FACTORY_TRANSCRIPTION_COMMAND is not set",
        durationMs: 0,
        commandLabel: "transcription config"
      };
    }

    const fileName = safeTempFileName(request.fileName);
    const shellCommand = transcriptionCommandLine(command, request.args);
    const script = `
set -euo pipefail
${workerUserPathPrelude()}
tmp_dir="$(mktemp -d "\${TMPDIR:-/tmp}/brainstack-transcribe.XXXXXX")"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT
input="$tmp_dir/${fileName}"
cat > "$input"
${transcriptionPreprocessScript()}
${shellCommand}
`.trim();
    const timeoutSeconds = Math.max(1, Math.ceil(request.timeoutMs / 1000));
    const remoteShellCommand = `bash -lc ${quoteSh(script)}`;

    switch (worker.transport) {
      case "local":
        return this.spawnAndCapture(
          ["bash", "-lc", script],
          request.bytes,
          worker.name,
          "local",
          undefined,
          timeoutSeconds,
          undefined,
          this.config.workerCaptureMaxBytes
        );
      case "ssh":
        if (worker.sshTrustMode === "accept-new" && !truthyEnv(process.env.BRAINSTACK_ALLOW_ACCEPT_NEW_DISPATCH)) {
          return {
            ok: false,
            host: worker.name,
            transport: worker.transport,
            exitCode: 78,
            stdout: "",
            stderr: `worker ${worker.name} uses sshTrustMode=accept-new, which is bootstrap-only. Pin the host key with brainctl trust-worker and set sshTrustMode: pinned before transcription dispatch.`,
            durationMs: 0,
            commandLabel: "ssh trust refused"
          };
        }
        return this.spawnAndCapture(
          [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            ...sshTrustArgs(this.config, worker),
            ...workerSshPortArgs(worker),
            this.remoteTarget(worker),
            remoteShellCommand
          ],
          request.bytes,
          worker.name,
          "ssh",
          undefined,
          timeoutSeconds,
          undefined,
          this.config.workerCaptureMaxBytes
        );
      case "tailscale-ssh":
        return this.spawnAndCapture(
          ["tailscale", "ssh", this.remoteTarget(worker), remoteShellCommand],
          request.bytes,
          worker.name,
          "tailscale-ssh",
          undefined,
          timeoutSeconds,
          undefined,
          this.config.workerCaptureMaxBytes
        );
    }
  }

  async runUpdateCheck(context: ContextRecord, logPath?: string): Promise<WorkerExecResult & { reportPath: string | null }> {
    const contextWorker = this.requireWorker(context.machine);
    const currentBeforeLaunch = this.db.getContextBySlug(context.slug);
    if (currentBeforeLaunch?.state === "archived") {
      return {
        ok: false,
        host: contextWorker.name,
        transport: contextWorker.transport,
        exitCode: 89,
        stdout: "",
        stderr: `context archived before update-check launch: ${context.slug}`,
        durationMs: 0,
        commandLabel: "update-check",
        reportPath: null
      };
    }

    const startedAt = Date.now();
    const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
    const reportPath = `.factory/reports/update-check-${stamp}.md`;
    const targets = this.updateCheckTargets(contextWorker);
    const targetNames = new Set(targets.map((worker) => worker.name));
    const unconfigured = this.knownHosts()
      .filter((host) => !targetNames.has(host))
      .sort()
      .map((host) => ({
        ok: false,
        host,
        transport: "stack" as const,
        exitCode: 77,
        stdout: "",
        stderr: "skipped: known machine has no configured worker entry for update-check",
        durationMs: 0,
        commandLabel: "update-check skipped"
      }));
    const { scheduled, skipped } = this.limitUpdateCheckTargets(targets);
    const probed = await this.runUpdateCheckProbes(scheduled, logPath);
    const results = [...probed, ...skipped, ...unconfigured];
    const report = this.formatStackUpdateReport(context, stamp, results);
    const failed = results.filter((result) => !result.ok);
    const allFailed = results.length > 0 && failed.length === results.length;

    try {
      await this.writeWorkspaceFile(context, reportPath, report);
      await this.recordUpdateCheckArtifact(context, reportPath);
    } catch (error) {
      return {
        ok: false,
        host: contextWorker.name,
        transport: contextWorker.transport,
        exitCode: 46,
        stdout: "",
        stderr: error instanceof Error ? error.message : String(error),
        durationMs: Date.now() - startedAt,
        commandLabel: "update-check",
        reportPath: null
      };
    }

    return {
      ok: !allFailed,
      host: contextWorker.name,
      transport: "stack",
      exitCode: allFailed ? 75 : 0,
      stdout: `BRAINSTACK_UPDATE_REPORT=${reportPath}\n${report}`,
      stderr: allFailed ? `update-check failed on all ${results.length} machine(s)` : "",
      durationMs: Date.now() - startedAt,
      commandLabel: `update-check ${[...targetNames, ...unconfigured.map((result) => result.host)].sort().join(",")}`,
      reportPath
    };
  }

  private limitUpdateCheckTargets(targets: FactoryWorkerConfig[]): { scheduled: FactoryWorkerConfig[]; skipped: WorkerExecResult[] } {
    const scheduled = targets.slice(0, UPDATE_CHECK_MAX_TARGETS);
    const skipped = targets.slice(UPDATE_CHECK_MAX_TARGETS).map((worker) => ({
      ok: false,
      host: worker.name,
      transport: worker.transport,
      exitCode: 76,
      stdout: "",
      stderr: `skipped: update-check target cap ${UPDATE_CHECK_MAX_TARGETS} reached`,
      durationMs: 0,
      commandLabel: "update-check skipped"
    }));

    return { scheduled, skipped };
  }

  private async runUpdateCheckProbes(workers: FactoryWorkerConfig[], logPath?: string): Promise<WorkerExecResult[]> {
    const results: WorkerExecResult[] = [];
    let next = 0;

    const runNext = async (): Promise<void> => {
      while (next < workers.length) {
        const index = next;
        next += 1;
        const worker = workers[index];
        if (!worker) {
          continue;
        }
        results[index] = await this.runUpdateCheckProbe(worker, logPath);
      }
    };

    await Promise.all(Array.from({ length: Math.min(UPDATE_CHECK_CONCURRENCY, workers.length) }, () => runNext()));
    return results;
  }

  private updateCheckTargets(contextWorker: FactoryWorkerConfig): FactoryWorkerConfig[] {
    const ordered = new Map<string, FactoryWorkerConfig>();
    for (const worker of this.configuredWorkers()) {
      ordered.set(worker.name, worker);
    }
    ordered.set(contextWorker.name, contextWorker);
    return [...ordered.values()].sort((left, right) => left.name.localeCompare(right.name));
  }

  private buildUpdateCheckProbeScript(worker: FactoryWorkerConfig): string {
    const productRoot = worker.localExecution ? this.config.projectRoot : "~/brainstack";
    const harness = this.resolveHarness(worker);
    return `
set -uo pipefail
expand_home_path() {
  case "$1" in
    "~") printf '%s\\n' "$HOME" ;;
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

harness=${quoteSh(harness.family)}
harness_bin=${quoteSh(harness.bin)}
${workerUserPathPrelude()}

brainstack_config_value() {
  key="$1"
  file="$2"
  [ -f "$file" ] || return 0
  awk -v key="$key" 'BEGIN { in_section=0 } /^[[:space:]]*\\[/ { in_section=1 } in_section == 0 { pattern="^[[:space:]]*" key "[[:space:]]*="; if ($0 ~ pattern) { sub(/^[^=]*=[[:space:]]*/, ""); sub(/[[:space:]]*#.*/, ""); gsub(/^[[:space:]\"]+|[[:space:]\"]+$/, ""); print; exit } }' "$file" || true
}

if command -v "$harness_bin" >/dev/null 2>&1; then
  resolved_harness_bin="$(command -v "$harness_bin")"
else
  resolved_harness_bin="$harness_bin"
fi
printf 'runtime_harness=%s\\n' "$harness"
printf 'runtime_harness_bin=%s\\n' "$resolved_harness_bin"
if [ "$harness" = codex ]; then
  codex_config="\${CODEX_HOME:-$HOME/.codex}/config.toml"
  model_config="$(brainstack_config_value model "$codex_config")"
  effort_config="$(brainstack_config_value model_reasoning_effort "$codex_config")"
  printf 'runtime_model=%s\\n' "\${model_config:-default}"
  printf 'runtime_effort=%s\\n' "\${effort_config:-default}"
else
  printf 'runtime_model=default\\n'
  printf 'runtime_effort=n/a\\n'
fi

run_readonly() {
  per_command_timeout="\${BRAINSTACK_UPDATE_PROBE_TIMEOUT_SECONDS:-20}"
  printf '\\n$ %s\\n' "$*"
  if command -v timeout >/dev/null 2>&1; then
    timeout "$per_command_timeout" "$@" 2>&1 || printf 'exit=%s\\n' "$?"
    return
  fi
  "$@" > >(cat) 2> >(cat >&2) &
  probe_pid=$!
  elapsed=0
  while kill -0 "$probe_pid" 2>/dev/null; do
    if [ "$elapsed" -ge "$per_command_timeout" ]; then
      kill "$probe_pid" 2>/dev/null || true
      sleep 1
      kill -9 "$probe_pid" 2>/dev/null || true
      wait "$probe_pid" 2>/dev/null || true
      printf 'exit=124\\n'
      return
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  wait "$probe_pid" 2>&1 || printf 'exit=%s\\n' "$?"
}

product_root="$(expand_home_path ${quoteSh(productRoot)})"
config_path="$(expand_home_path ~/.config/brainstack/brainstack.yaml)"
printf -- '- hostname: %s\\n' "$(hostname 2>/dev/null || printf unknown)"
printf -- '- transport: %s\\n' ${quoteSh(worker.transport)}
if [ -f "$product_root/packages/brainctl/src/main.ts" ] && command -v bun >/dev/null 2>&1 && [ -f "$config_path" ]; then
  printf '\\n### brainctl updates\\n'
  (cd "$product_root" && BRAINSTACK_UPDATE_PROBE_TIMEOUT_MS="\${BRAINSTACK_UPDATE_PROBE_TIMEOUT_MS:-20000}" bun run packages/brainctl/src/main.ts updates --config "$config_path") 2>&1 || printf 'brainctl updates failed with exit=%s\\n' "$?"
else
  printf '\\n### fallback checks\\n'
  run_readonly uname -a
  if command -v codex >/dev/null 2>&1; then run_readonly codex --version; else printf '\\ncodex: missing\\n'; fi
  if command -v claude >/dev/null 2>&1; then run_readonly claude --version; else printf '\\nclaude: missing\\n'; fi
  if command -v brew >/dev/null 2>&1; then HOMEBREW_NO_AUTO_UPDATE=1 HOMEBREW_NO_ENV_HINTS=1 HOMEBREW_NO_INSTALL_CLEANUP=1 run_readonly brew outdated --quiet; fi
  if command -v pacman >/dev/null 2>&1; then run_readonly pacman -Qu; fi
  if command -v checkupdates >/dev/null 2>&1; then run_readonly checkupdates; fi
  if command -v apt >/dev/null 2>&1; then run_readonly apt list --upgradable; fi
  if command -v dnf >/dev/null 2>&1; then run_readonly dnf --cacheonly check-update --quiet; fi
  if command -v zypper >/dev/null 2>&1; then run_readonly zypper --no-refresh list-updates; fi
fi
`.trim();
  }

  private async runUpdateCheckProbe(worker: FactoryWorkerConfig, logPath?: string): Promise<WorkerExecResult> {
    return this.runWorkerScript(worker, this.buildUpdateCheckProbeScript(worker), logPath, 120);
  }

  private formatStackUpdateReport(context: ContextRecord, stamp: string, results: WorkerExecResult[]): string {
    const failed = results.filter((result) => !result.ok);
    const status = failed.length === 0 ? "ok" : failed.length === results.length ? "failed" : "degraded";
    const lines = [
      "# Update Check",
      "",
      `- generated_at: ${stamp}`,
      `- context: ${context.slug}`,
      `- status: ${status}`,
      `- machines_checked: ${results.length}`,
      `- failed_machines: ${failed.length}`,
      "",
      "This deterministic update check is read-only. It does not install, upgrade, remove, reboot, restart, or mutate packages/services.",
      ""
    ];

    for (const result of results) {
      const body = `${result.stdout}${result.stderr ? `\n${result.stderr}` : ""}`.trim();
      const runtime = runtimeFromUpdateOutput(body) || this.describeWorkerRuntime(result.host, result.host === context.machine ? context : undefined);
      lines.push(`## ${result.host}`, "");
      lines.push(`- status: ${result.ok ? "ok" : "failed"}`);
      lines.push(`- transport: ${result.transport}`);
      if (runtime) {
        lines.push(`- harness: ${runtime.harness}`);
        lines.push(`- harness_bin: ${runtime.harnessBin}`);
        lines.push(`- model: ${runtime.model}`);
        lines.push(`- effort: ${runtime.effort}`);
      }
      lines.push(`- exit: ${result.exitCode}`);
      lines.push(`- duration_ms: ${result.durationMs}`);
      lines.push("");
      lines.push(body ? body : "(no output)");
      lines.push("");
    }

    lines.push("No packages or services were changed by this deterministic update check.", "");
    return lines.join("\n");
  }

  private async recordUpdateCheckArtifact(context: ContextRecord, reportPath: string): Promise<void> {
    const entry = `- \`${reportPath}\` - read-only stack update report`;
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const current = await this.readFactoryFile(context, "ARTIFACTS.md");
      const next = current?.trim()
        ? current.includes(reportPath)
          ? current
          : `${current.trimEnd()}\n\n${entry}\n`
        : `# Artifacts\n\n${entry}\n`;
      if (current === next || (await this.writeWorkspaceFileIfUnchanged(context, ".factory/ARTIFACTS.md", current, next))) {
        return;
      }
    }
    throw new Error("timed out waiting for artifact metadata lock");
  }

  async readFactoryFile(context: ContextRecord, fileName: string): Promise<string | null> {
    return this.readWorkspaceFile(context, `.factory/${fileName.replaceAll("/", "")}`);
  }

  async readWorkspaceFile(context: ContextRecord, relativePath: string): Promise<string | null> {
    const worker = this.requireWorker(context.machine);
    const safePath = cleanRelativePath(relativePath);
    const script = `
set -euo pipefail
expand_home_path() {
  case "$1" in
    "~") printf '%s\\n' "$HOME" ;;
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

cd "$(expand_home_path ${quoteSh(context.worktreePath)})"
worktree_real="$(pwd -P)"
safe_path=${quoteSh(safePath)}
target_path="$worktree_real/$safe_path"
if [ -L "$target_path" ]; then
  echo "workspace read refused symlink target: $safe_path" >&2
  exit 1
fi
resolved_real="$(realpath "$target_path" 2>/dev/null || true)"
if [ -n "$resolved_real" ]; then
  case "$resolved_real" in
    "$worktree_real"|"$worktree_real"/*) ;;
    *) echo "workspace read escaped worktree: $safe_path" >&2; exit 1 ;;
  esac
fi
if [ -f "$resolved_real" ]; then
  size="$(wc -c < "$resolved_real" | tr -d '[:space:]')"
  if [ "$size" -gt 1048576 ]; then
    echo "workspace read refused oversized file: $safe_path" >&2
    exit 1
  fi
  cat "$resolved_real"
fi
`;

    const result = await this.runWorkerScript(worker, script, undefined, 10);
    if (!result.ok) {
      return null;
    }

    return result.stdout || null;
  }

  async writeWorkspaceFile(context: ContextRecord, relativePath: string, content: string): Promise<void> {
    const worker = this.requireWorker(context.machine);
    const safePath = cleanRelativePath(relativePath);
    const encoded = Buffer.from(content, "utf8").toString("base64");
    const marker = "__WORKSPACE_FILE_CONTENT__";
    const script = `
set -euo pipefail
expand_home_path() {
  case "$1" in
    "~") printf '%s\\n' "$HOME" ;;
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

cd "$(expand_home_path ${quoteSh(context.worktreePath)})"
worktree_real="$(pwd -P)"
safe_path=${quoteSh(safePath)}
target_path="$worktree_real/$safe_path"
target_parent="$(dirname "$target_path")"
mkdir -p "$target_parent"
parent_real="$(cd "$target_parent" && pwd -P)"
case "$parent_real/" in
  "$worktree_real"/*) ;;
  *) echo "workspace write escaped worktree: $safe_path" >&2; exit 1 ;;
esac
if [ -L "$target_path" ]; then
  echo "workspace write refused symlink target: $safe_path" >&2
  exit 1
fi
tmp_file="$(mktemp "$parent_real/.brainstack-write.XXXXXX")"
trap 'rm -f "$tmp_file"' EXIT
${base64DecodeShellFunction()}
cat <<'${marker}' | brainstack_base64_decode > "$tmp_file"
${encoded}
${marker}
mv -f "$tmp_file" "$target_path"
trap - EXIT
`;

    const result = await this.runWorkerScript(worker, script, undefined, 10);
    if (!result.ok) {
      throw new Error(result.stderr.trim() || result.stdout.trim() || `exit ${result.exitCode}`);
    }
  }

  async writeWorkspaceFileIfUnchanged(
    context: ContextRecord,
    relativePath: string,
    expectedContent: string | null,
    content: string
  ): Promise<boolean> {
    const worker = this.requireWorker(context.machine);
    const safePath = cleanRelativePath(relativePath);
    const expectedEncoded = expectedContent === null ? "" : Buffer.from(expectedContent, "utf8").toString("base64");
    const contentEncoded = Buffer.from(content, "utf8").toString("base64");
    const expectedMarker = "__EXPECTED_WORKSPACE_FILE_CONTENT__";
    const contentMarker = "__NEXT_WORKSPACE_FILE_CONTENT__";
    const script = `
set -euo pipefail
expand_home_path() {
  case "$1" in
    "~") printf '%s\\n' "$HOME" ;;
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

cd "$(expand_home_path ${quoteSh(context.worktreePath)})"
worktree_real="$(pwd -P)"
safe_path=${quoteSh(safePath)}
target_path="$worktree_real/$safe_path"
target_parent="$(dirname "$target_path")"
mkdir -p "$target_parent"
parent_real="$(cd "$target_parent" && pwd -P)"
case "$parent_real/" in
  "$worktree_real"/*) ;;
  *) echo "workspace write escaped worktree: $safe_path" >&2; exit 1 ;;
esac
if [ -L "$target_path" ]; then
  echo "workspace write refused symlink target: $safe_path" >&2
  exit 1
fi
${base64DecodeShellFunction()}
expected_mode=${expectedContent === null ? quoteSh("absent") : quoteSh("content")}
expected_file="$(mktemp "$parent_real/.brainstack-expected.XXXXXX")"
tmp_file="$(mktemp "$parent_real/.brainstack-write.XXXXXX")"
lock_dir=""
if [ "$safe_path" = ".factory/ARTIFACTS.md" ]; then
  lock_dir="$parent_real/.artifacts.lock"
  lock_acquired=0
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if mkdir "$lock_dir" 2>/dev/null; then
      lock_acquired=1
      break
    fi
    sleep 0.2
  done
  if [ "$lock_acquired" != "1" ]; then
    echo "timed out waiting for artifact metadata lock" >&2
    exit 75
  fi
fi
cleanup_compare_write() {
  rm -f "$expected_file" "$tmp_file"
  if [ -n "$lock_dir" ] && [ -d "$lock_dir" ]; then
    rmdir "$lock_dir" 2>/dev/null || true
  fi
}
trap cleanup_compare_write EXIT
if [ "$expected_mode" = "content" ]; then
  cat <<'${expectedMarker}' | brainstack_base64_decode > "$expected_file"
${expectedEncoded}
${expectedMarker}
  if [ ! -f "$target_path" ] || ! cmp -s "$target_path" "$expected_file"; then
    echo "workspace file changed: $safe_path" >&2
    exit 75
  fi
elif [ -e "$target_path" ]; then
  echo "workspace file appeared: $safe_path" >&2
  exit 75
fi
cat <<'${contentMarker}' | brainstack_base64_decode > "$tmp_file"
${contentEncoded}
${contentMarker}
mv -f "$tmp_file" "$target_path"
trap - EXIT
cleanup_compare_write
`;

    const result = await this.runWorkerScript(worker, script, undefined, 10);
    if (result.exitCode === 75) {
      return false;
    }
    if (!result.ok) {
      throw new Error(result.stderr.trim() || result.stdout.trim() || `exit ${result.exitCode}`);
    }

    return true;
  }

  async readArtifactFile(context: ContextRecord, filePath: string, maxBytes = 45 * 1024 * 1024): Promise<WorkspaceArtifactFile> {
    if (!this.config.allowAbsoluteArtifactPaths && (filePath.startsWith("/") || filePath === "~" || filePath.startsWith("~/"))) {
      throw new Error("absolute artifact paths are disabled; record a relative path inside the workspace");
    }
    if (!filePath.startsWith("/") && !filePath.startsWith("~/") && isProtectedArtifactPath(filePath)) {
      throw new Error(`protected artifact path cannot be sent: ${filePath}`);
    }

    const worker = this.requireWorker(context.machine);
    const script = `
set -euo pipefail
expand_home_path() {
  case "$1" in
    "~") printf '%s\\n' "$HOME" ;;
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

worktree_raw=${quoteSh(context.worktreePath)}
requested_raw=${quoteSh(filePath)}
max_bytes=${quoteSh(String(maxBytes))}
allow_absolute=${quoteSh(this.config.allowAbsoluteArtifactPaths ? "1" : "0")}
worktree="$(expand_home_path "$worktree_raw")"

brainstack_protected_artifact_path() {
  normalized="$1"
  while [ "\${normalized#./}" != "$normalized" ]; do
    normalized="\${normalized#./}"
  done
  lower="$(printf '%s' "$normalized" | tr '[:upper:]' '[:lower:]')"
  case "$normalized" in
    ""|../*|*/../*|*/..|.git|.git/*)
      return 0
      ;;
    .factory|.factory/*)
      case "$normalized" in
        .factory/reports/*)
          case "$normalized" in
            */.*) return 0 ;;
            *) return 1 ;;
          esac
          ;;
        *) return 0 ;;
      esac
      ;;
    .*|*/.*)
      return 0
      ;;
  esac
  case "$lower" in
    .env|*.env|*.pem|*.key|*id_rsa*|*id_ed25519*|*authorized_keys*|*known_hosts*|*token*|*secret*|*passwd*|*shadow*|*keyring*)
      return 0
      ;;
  esac
  return 1
}

case "$requested_raw" in
  "~"|~/*)
    if [ "$allow_absolute" != "1" ]; then
      echo "absolute artifact paths are disabled" >&2
      exit 44
    fi
    resolved="$(expand_home_path "$requested_raw")"
    ;;
  /*)
    if [ "$allow_absolute" != "1" ]; then
      echo "absolute artifact paths are disabled" >&2
      exit 44
    fi
    resolved="$requested_raw"
    ;;
  *)
    resolved="$worktree/$requested_raw"
    ;;
esac

worktree_real="$(realpath "$worktree")"
if [ -L "$resolved" ]; then
  echo "artifact path is a symlink: $requested_raw" >&2
  exit 46
fi
resolved_real="$(realpath "$resolved" 2>/dev/null || true)"
if [ -z "$resolved_real" ]; then
  echo "missing file: $resolved" >&2
  exit 41
fi

if [ "$allow_absolute" != "1" ]; then
  case "$resolved_real" in
    "$worktree_real"|"$worktree_real"/*) ;;
    *)
      echo "artifact path escapes workspace: $requested_raw" >&2
      exit 45
      ;;
  esac
  resolved_relative="\${resolved_real#$worktree_real/}"
  if brainstack_protected_artifact_path "$resolved_relative"; then
    echo "protected artifact path cannot be sent: $resolved_relative" >&2
    exit 46
  fi
fi

resolved="$resolved_real"

if [ ! -e "$resolved" ]; then
  echo "missing file: $resolved" >&2
  exit 41
fi

if [ ! -f "$resolved" ]; then
  echo "not a regular file: $resolved" >&2
  exit 42
fi

size="$(wc -c < "$resolved" | tr -d '[:space:]')"
if [ "$size" -gt "$max_bytes" ]; then
  echo "file too large: $resolved ($size bytes > $max_bytes)" >&2
  exit 43
fi

name="$(basename "$resolved")"
mime="application/octet-stream"
if command -v file >/dev/null 2>&1; then
  mime="$(file --mime-type -b "$resolved" 2>/dev/null || printf 'application/octet-stream')"
fi

printf 'FILE_PATH=%s\\n' "$resolved"
printf 'FILE_NAME=%s\\n' "$name"
printf 'FILE_SIZE=%s\\n' "$size"
printf 'FILE_MIME=%s\\n' "$mime"
printf '__BASE64__\\n'

if command -v base64 >/dev/null 2>&1; then
  base64 < "$resolved"
else
  python3 - <<'PY' "$resolved"
import base64
import pathlib
import sys
print(base64.b64encode(pathlib.Path(sys.argv[1]).read_bytes()).decode("ascii"))
PY
fi
`;

    const artifactCaptureMaxBytes = Math.ceil(maxBytes * 1.5) + 256 * 1024;
    const result = await this.runWorkerScript(worker, script, undefined, 20, undefined, artifactCaptureMaxBytes);
    if (!result.ok) {
      throw new Error(result.stderr.trim() || result.stdout.trim() || `exit ${result.exitCode}`);
    }

    const marker = "\n__BASE64__\n";
    const markerIndex = result.stdout.indexOf(marker);
    if (markerIndex === -1) {
      throw new Error("artifact fetch response did not contain a base64 payload");
    }

    const metadataText = result.stdout.slice(0, markerIndex);
    const base64Text = result.stdout.slice(markerIndex + marker.length).replace(/\s+/g, "");
    const metadata = new Map<string, string>();

    for (const line of metadataText.split("\n")) {
      const separatorIndex = line.indexOf("=");
      if (separatorIndex === -1) {
        continue;
      }

      metadata.set(line.slice(0, separatorIndex), line.slice(separatorIndex + 1).trim());
    }

    const resolvedPath = metadata.get("FILE_PATH");
    const fileName = metadata.get("FILE_NAME");
    const mimeType = metadata.get("FILE_MIME") || "application/octet-stream";
    const sizeBytes = Number(metadata.get("FILE_SIZE") || "0");

    if (!resolvedPath || !fileName || !base64Text) {
      throw new Error("artifact fetch response was missing required metadata");
    }

    return {
      path: resolvedPath,
      fileName,
      sizeBytes: Number.isFinite(sizeBytes) ? sizeBytes : 0,
      mimeType,
      content: Buffer.from(base64Text, "base64")
    };
  }

  async deleteArtifactFile(context: ContextRecord, filePath: string): Promise<WorkspaceArtifactDeleteResult> {
    const worker = this.requireWorker(context.machine);
    const safePath = cleanArtifactDeletePath(filePath);
    if (isProtectedArtifactPath(safePath)) {
      throw new Error(`protected artifact path cannot be deleted: ${filePath}`);
    }
    const script = `
set -euo pipefail
expand_home_path() {
  case "$1" in
    "~") printf '%s\\n' "$HOME" ;;
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

worktree_raw=${quoteSh(context.worktreePath)}
requested_raw=${quoteSh(safePath)}
worktree="$(expand_home_path "$worktree_raw")"
resolved="$worktree/$requested_raw"

worktree_real="$(realpath "$worktree")"
parent_real="$(realpath "$(dirname "$resolved")" 2>/dev/null || true)"
if [ -z "$parent_real" ]; then
  echo "STATUS=missing"
  echo "REQUESTED_PATH=$requested_raw"
  echo "RESOLVED_PATH="
  echo "FILE_NAME=$(basename "$requested_raw")"
  exit 0
fi

case "$parent_real" in
  "$worktree_real"|"$worktree_real"/*) ;;
  *)
    echo "artifact path escapes workspace: $requested_raw" >&2
    exit 45
    ;;
esac

if [ -L "$resolved" ]; then
  rm -f -- "$resolved"
  echo "STATUS=deleted"
  echo "REQUESTED_PATH=$requested_raw"
  echo "RESOLVED_PATH=$parent_real/$(basename "$resolved")"
  echo "FILE_NAME=$(basename "$requested_raw")"
  exit 0
fi

if [ ! -e "$resolved" ]; then
  echo "STATUS=missing"
  echo "REQUESTED_PATH=$requested_raw"
  echo "RESOLVED_PATH=$parent_real/$(basename "$resolved")"
  echo "FILE_NAME=$(basename "$requested_raw")"
  exit 0
fi

resolved_real="$(realpath "$resolved")"
case "$resolved_real" in
  "$worktree_real"|"$worktree_real"/*) ;;
  *)
    echo "artifact path escapes workspace: $requested_raw" >&2
    exit 45
    ;;
esac

if [ ! -f "$resolved_real" ]; then
  echo "not a regular file: $resolved_real" >&2
  exit 42
fi

rm -f -- "$resolved"
echo "STATUS=deleted"
echo "REQUESTED_PATH=$requested_raw"
echo "RESOLVED_PATH=$resolved_real"
echo "FILE_NAME=$(basename "$resolved_real")"
`;

    const result = await this.runWorkerScript(worker, script, undefined, 10);
    if (!result.ok) {
      throw new Error(result.stderr.trim() || result.stdout.trim() || `exit ${result.exitCode}`);
    }

    const metadata = new Map<string, string>();
    for (const line of result.stdout.split("\n")) {
      const separatorIndex = line.indexOf("=");
      if (separatorIndex === -1) {
        continue;
      }

      metadata.set(line.slice(0, separatorIndex), line.slice(separatorIndex + 1).trim());
    }

    const status = metadata.get("STATUS");
    if (status !== "deleted" && status !== "missing") {
      throw new Error("artifact delete response did not include a valid status");
    }

    return {
      requestedPath: metadata.get("REQUESTED_PATH") || safePath,
      resolvedPath: metadata.get("RESOLVED_PATH") || null,
      fileName: metadata.get("FILE_NAME") || safePath.split("/").at(-1) || safePath,
      status
    };
  }

  isReachabilityFailure(result: WorkerExecResult): boolean {
    return this.isReachabilityError(result);
  }

  private requireWorker(machine: string): FactoryWorkerConfig {
    const worker = this.getWorkerConfig(machine);
    if (!worker) {
      throw new Error(`Unknown machine: ${machine}`);
    }

    return worker;
  }

  private resolveHarness(worker: FactoryWorkerConfig, context?: ContextRecord): { family: HarnessName; bin: string } {
    const contextHarness = context?.harness === "claude" || context?.harness === "codex" ? context.harness : null;
    const family = contextHarness || worker.harness || this.config.harness;
    const contextBin = context?.harnessBin?.trim() || null;
    const bin = contextBin || worker.harnessBin || (worker.transport === "local" && family === this.config.harness ? this.config.harnessBin : family);
    return { family, bin };
  }

  private planContext(worker: FactoryWorkerConfig, request: ContextBootstrapRequest): ContextPlan {
    const target = request.target.trim();
    const baseBranch = request.baseBranch?.trim() || null;

    if (target === "host") {
      return {
        kind: "host",
        target,
        rootPath: `${worker.managedHostRoot}/${request.slug}`,
        worktreePath: `${worker.managedHostRoot}/${request.slug}`,
        branchName: "master",
        baseBranch: null
      };
    }

    if (target === "scratch") {
      return {
        kind: "scratch",
        target,
        rootPath: `${worker.managedScratchRoot}/${request.slug}`,
        worktreePath: `${worker.managedScratchRoot}/${request.slug}`,
        branchName: "master",
        baseBranch: null
      };
    }

    if (isGitUrl(target)) {
      return {
        kind: "repo",
        target,
        rootPath: `${worker.managedRepoRoot}/${request.slug}`,
        worktreePath: `${worker.managedRepoRoot}/${request.slug}`,
        branchName: baseBranch,
        baseBranch
      };
    }

    if (isPathTarget(target)) {
      return {
        kind: "repo",
        target,
        rootPath: target,
        worktreePath: target,
        branchName: baseBranch,
        baseBranch
      };
    }

    throw new Error(`Unsupported target: ${target}. Use an absolute path, git URL, host, or scratch.`);
  }

  private buildBootstrapScript(
    worker: FactoryWorkerConfig,
    request: ContextBootstrapRequest,
    plan: ContextPlan
  ): string {
    const scriptPrelude = `
set -euo pipefail
expand_home_path() {
  case "$1" in
    "~") printf '%s\\n' "$HOME" ;;
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

current_branch() {
  git -C "$1" rev-parse --abbrev-ref HEAD 2>/dev/null || printf '%s\\n' ""
}

ensure_factory_files() {
  workspace="$1"
  workspace_real="$(cd "$workspace" && pwd -P)"
  if [ -L "$workspace/.factory" ]; then
    echo "refusing symlinked .factory directory" >&2
    exit 46
  fi
  mkdir -p "$workspace/.factory"
  factory_real="$(cd "$workspace/.factory" && pwd -P)"
  case "$factory_real" in
    "$workspace_real/.factory") ;;
    *) echo ".factory escapes workspace" >&2; exit 46 ;;
  esac

  if [ ! -f "$workspace/.factory/SUMMARY.md" ]; then
    if [ -L "$workspace/.factory/SUMMARY.md" ]; then
      echo "refusing symlinked .factory/SUMMARY.md" >&2
      exit 46
    fi
    cat > "$workspace/.factory/SUMMARY.md" <<'EOF'
# Summary

- Context initialized.
EOF
  fi

  if [ ! -f "$workspace/.factory/TODO.md" ]; then
    if [ -L "$workspace/.factory/TODO.md" ]; then
      echo "refusing symlinked .factory/TODO.md" >&2
      exit 46
    fi
    cat > "$workspace/.factory/TODO.md" <<'EOF'
# TODO

- Replace this with the active plan for this topic.
EOF
  fi

  if [ ! -f "$workspace/.factory/ARTIFACTS.md" ]; then
    if [ -L "$workspace/.factory/ARTIFACTS.md" ]; then
      echo "refusing symlinked .factory/ARTIFACTS.md" >&2
      exit 46
    fi
    cat > "$workspace/.factory/ARTIFACTS.md" <<'EOF'
# Artifacts

- Record durable outputs and paths here.
EOF
  fi
}

ensure_initial_commit() {
  workspace="$1"
  message="$2"

  if git -C "$workspace" rev-parse --verify --quiet HEAD >/dev/null 2>&1; then
    return
  fi

  if ! git -C "$workspace" config user.name >/dev/null 2>&1; then
    git -C "$workspace" config user.name "Brainstack Factory"
  fi

  if ! git -C "$workspace" config user.email >/dev/null 2>&1; then
    git -C "$workspace" config user.email "factory@localhost"
  fi

  git -C "$workspace" add AGENTS.md .factory >/dev/null 2>&1
  git -C "$workspace" commit -m "$message" >/dev/null 2>&1
}

write_state_file() {
  workspace="$1"
  root_path="$2"
  worktree_path="$3"
  branch_name="$4"
  state_name="$5"
  state_path="$workspace/.factory/STATE.json"
  if [ -L "$state_path" ]; then
    echo "refusing symlinked .factory/STATE.json" >&2
    exit 46
  fi
  tmp_state="$(mktemp "$workspace/.factory/.state.XXXXXX")"
  cat > "$tmp_state" <<EOF
{
  "slug": ${JSON.stringify(request.slug)},
  "machine": ${JSON.stringify(request.machine)},
  "kind": ${JSON.stringify(plan.kind)},
  "state": "$state_name",
  "transport": ${JSON.stringify(worker.transport)},
  "target": ${JSON.stringify(plan.target)},
  "rootPath": "$root_path",
  "worktreePath": "$worktree_path",
  "branchName": "$branch_name",
  "baseBranch": ${JSON.stringify(plan.baseBranch)},
  "updatedAt": ${JSON.stringify(nowIso())}
}
EOF
  mv -f "$tmp_state" "$state_path"
}
`;

    if (plan.kind === "host" || plan.kind === "scratch") {
      return `${scriptPrelude}
workspace_raw=${quoteSh(plan.rootPath)}
workspace="$(expand_home_path "$workspace_raw")"

mkdir -p "$workspace"

if ! git -C "$workspace" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git init -b master "$workspace" >/dev/null 2>&1 || git -C "$workspace" init >/dev/null 2>&1
  git -C "$workspace" symbolic-ref HEAD refs/heads/master >/dev/null 2>&1 || true
fi

ensure_factory_files "$workspace"

if [ ! -f "$workspace/AGENTS.md" ]; then
  cat > "$workspace/AGENTS.md" <<'EOF'
# Managed Context

- This is a control-plane managed ${plan.kind} context.
- Durable state lives in .factory/STATE.json, .factory/SUMMARY.md, .factory/TODO.md, and .factory/ARTIFACTS.md.
- Keep updates concise and durable so the next Telegram topic message can resume cleanly.
EOF
fi

ensure_initial_commit "$workspace" "Initialize managed ${plan.kind} context"

branch_name="$(current_branch "$workspace")"
write_state_file "$workspace" "$workspace" "$workspace" "$branch_name" "active"

printf 'ROOT=%s\\n' "$workspace"
printf 'WORKTREE=%s\\n' "$workspace"
printf 'BRANCH=%s\\n' "$branch_name"
`;
    }

    if (isGitUrl(plan.target)) {
      return `${scriptPrelude}
repo_url=${quoteSh(plan.target)}
repo_root_raw=${quoteSh(plan.rootPath)}
repo_root="$(expand_home_path "$repo_root_raw")"
base_ref=${quoteSh(plan.baseBranch || "")}

mkdir -p "$(dirname "$repo_root")"

if ! git -C "$repo_root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if [ -d "$repo_root" ] && [ -n "$(ls -A "$repo_root" 2>/dev/null)" ]; then
    echo "managed repo path exists but is not a git repo: $repo_root" >&2
    exit 22
  fi

  if [ -n "$base_ref" ]; then
    git clone --branch "$base_ref" --single-branch "$repo_url" "$repo_root"
  else
    git clone "$repo_url" "$repo_root"
  fi
fi

if [ -n "$base_ref" ] && git -C "$repo_root" rev-parse --verify --quiet "$base_ref^{commit}" >/dev/null 2>&1; then
  git -C "$repo_root" checkout "$base_ref" >/dev/null 2>&1 || true
fi

ensure_factory_files "$repo_root"
branch_name="$(current_branch "$repo_root")"
write_state_file "$repo_root" "$repo_root" "$repo_root" "$branch_name" "active"

printf 'ROOT=%s\\n' "$repo_root"
printf 'WORKTREE=%s\\n' "$repo_root"
printf 'BRANCH=%s\\n' "$branch_name"
`;
    }

    return `${scriptPrelude}
repo_root_raw=${quoteSh(plan.rootPath)}
repo_root="$(expand_home_path "$repo_root_raw")"

if ! git -C "$repo_root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "repo missing or not a git work tree: $repo_root" >&2
  exit 20
fi

ensure_factory_files "$repo_root"
branch_name="$(current_branch "$repo_root")"
write_state_file "$repo_root" "$repo_root" "$repo_root" "$branch_name" "active"

printf 'ROOT=%s\\n' "$repo_root"
printf 'WORKTREE=%s\\n' "$repo_root"
printf 'BRANCH=%s\\n' "$branch_name"
`;
  }

  private isReachabilityError(result: WorkerExecResult): boolean {
    const combined = `${result.stderr}\n${result.stdout}`;
    return (
      result.exitCode === 124 ||
      result.exitCode === 255 ||
      /Could not resolve|Name or service not known|Connection refused|No route to host|timed out|Permission denied|Host key verification failed|requires an additional check|Temporary failure/i.test(
        combined
      )
    );
  }

  private remoteTarget(worker: FactoryWorkerConfig): string {
    if (worker.transport === "local") {
      return "";
    }

    const host = workerSshHost(worker);
    const remoteHost = host.includes(":") && !host.startsWith("[") ? `[${host}]` : host;
    const user = worker.sshUser || workerSshEmbeddedUser(worker);
    return user ? `${user}@${remoteHost}` : remoteHost;
  }

  private async runWorkerScript(
    worker: FactoryWorkerConfig,
    script: string,
    logPath?: string,
    timeoutSeconds?: number,
    onStdoutChunk?: (chunk: string) => Promise<void> | void,
    captureMaxBytes = this.config.workerCaptureMaxBytes,
    usePathCache = true,
    harnessOverride?: { family: HarnessName; bin: string }
  ): Promise<WorkerExecResult> {
    const harness = harnessOverride || this.resolveHarness(worker);
    if (worker.transport === "ssh" && worker.sshTrustMode === "accept-new" && !truthyEnv(process.env.BRAINSTACK_ALLOW_ACCEPT_NEW_DISPATCH)) {
      return {
        ok: false,
        host: worker.name,
        transport: worker.transport,
        exitCode: 78,
        stdout: "",
        stderr: `worker ${worker.name} uses sshTrustMode=accept-new, which is bootstrap-only. Pin the host key with brainctl trust-worker and set sshTrustMode: pinned before dispatch.`,
        durationMs: 0,
        commandLabel: "ssh trust refused"
      };
    }
    const cachedPath = usePathCache && worker.transport !== "local" ? await this.cachedWorkerPath(worker, harness) : null;
    const cachePrelude = cachedPath ? `BRAINSTACK_WORKER_PATH=${quoteSh(cachedPath)}\nexport BRAINSTACK_WORKER_PATH\n` : "";
    const uncachedPrelude = usePathCache ? "" : "unset BRAINSTACK_WORKER_PATH\n";
    const harnessPrelude = `harness=${quoteSh(harness.family)}\nharness_bin=${quoteSh(harness.bin)}\n`;
    const scriptToRun = worker.transport === "local" ? `${harnessPrelude}${script}` : `${uncachedPrelude}${cachePrelude}${harnessPrelude}${workerUserPathPrelude()}\n${script}`;
    switch (worker.transport) {
      case "local":
        return this.spawnAndCapture(
          ["bash", "-s", "--"],
          scriptToRun,
          worker.name,
          "local",
          logPath,
          timeoutSeconds,
          onStdoutChunk,
          captureMaxBytes
        );
      case "ssh":
        return this.spawnAndCapture(
          [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            ...sshTrustArgs(this.config, worker),
            ...workerSshPortArgs(worker),
            this.remoteTarget(worker),
            "bash",
            "-s",
            "--"
          ],
          scriptToRun,
          worker.name,
          "ssh",
          logPath,
          timeoutSeconds,
          onStdoutChunk,
          captureMaxBytes
        );
      case "tailscale-ssh":
        return this.spawnAndCapture(
          ["tailscale", "ssh", this.remoteTarget(worker), "bash", "-s", "--"],
          scriptToRun,
          worker.name,
          "tailscale-ssh",
          logPath,
          timeoutSeconds,
          onStdoutChunk,
          captureMaxBytes
        );
    }
  }

  private async spawnAndCapture(
    args: string[],
    script: string | Uint8Array,
    host: string,
    transport: string,
    logPath?: string,
    timeoutSeconds?: number,
    onStdoutChunk?: (chunk: string) => Promise<void> | void,
    captureMaxBytes = this.config.workerCaptureMaxBytes
  ): Promise<WorkerExecResult> {
    const startedAt = Date.now();
    const fullArgs = args;

    const proc = Bun.spawn(fullArgs, {
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
      env: process.env
    });
    let timedOut = false;
    let killTimer: Timer | null = null;
    let forceKillTimer: Timer | null = null;
    if (timeoutSeconds && timeoutSeconds > 0) {
      killTimer = setTimeout(() => {
        timedOut = true;
        proc.kill("SIGTERM");
        forceKillTimer = setTimeout(() => {
          proc.kill("SIGKILL");
        }, 1_000);
      }, timeoutSeconds * 1000);
    }

    proc.stdin.write(script);
    proc.stdin.end();

    const stdoutPromise = this.collectStream(proc.stdout, logPath, "", onStdoutChunk, captureMaxBytes);
    const stderrPromise = this.collectStream(proc.stderr, logPath, "[stderr] ", undefined, captureMaxBytes);

    const rawExitCode = await proc.exited;
    if (killTimer) {
      clearTimeout(killTimer);
    }
    if (forceKillTimer) {
      clearTimeout(forceKillTimer);
    }
    const [stdout, stderr] = await Promise.all([stdoutPromise, stderrPromise]);
    const exitCode = timedOut ? 124 : rawExitCode;
    const finalStderr =
      timedOut && timeoutSeconds
        ? `${stderr}${stderr ? "\n" : ""}worker command timed out after ${timeoutSeconds}s`
        : stderr;

    return {
      ok: exitCode === 0 && !timedOut,
      host,
      transport,
      exitCode,
      stdout,
      stderr: finalStderr,
      durationMs: Date.now() - startedAt,
      commandLabel: fullArgs.join(" ")
    };
  }

  private async collectStream(
    stream: ReadableStream<Uint8Array> | null | undefined,
    logPath: string | undefined,
    prefix: string,
    onChunk?: (chunk: string) => Promise<void> | void,
    captureMaxBytes = this.config.workerCaptureMaxBytes
  ): Promise<string> {
    if (!stream) {
      return "";
    }

    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let output = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }

      const chunk = decoder.decode(value, { stream: true });
      output = appendBoundedOutput(output, chunk, captureMaxBytes);

      if (logPath) {
        await appendFile(logPath, prefix ? `${prefix}${chunk}` : chunk);
      }

      if (!prefix && onChunk) {
        await onChunk(chunk);
      }
    }

    const tail = decoder.decode();
    if (tail) {
      output = appendBoundedOutput(output, tail, captureMaxBytes);
      if (logPath) {
        await appendFile(logPath, prefix ? `${prefix}${tail}` : tail);
      }

      if (!prefix && onChunk) {
        await onChunk(tail);
      }
    }

    return output;
  }
}
