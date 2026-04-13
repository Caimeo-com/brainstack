import { appendFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type { CodexReasoningEffort } from "./codex-runtime";
import { FactoryConfig, FactoryWorkerConfig, HarnessName } from "./config";
import { ContextKind, ContextRecord, ContextState, FactoryDb, WorkerRecord } from "./db";
import { TELEGRAM_ATTACHMENTS_WORKSPACE_PATH } from "./telegram-attachments";

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

export interface WorkspaceSeedFile {
  relativePath: string;
  content: Uint8Array;
}

export interface CodexRunOptions {
  onSessionId?: (sessionId: string) => Promise<void> | void;
  workspaceFiles?: WorkspaceSeedFile[];
  imagePaths?: string[];
  modelOverride?: string | null;
  reasoningEffortOverride?: CodexReasoningEffort | null;
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
        "$SHELL" -lic 'printf "__BRAINSTACK_PATH__%s\\n" "$PATH"' 2>/dev/null
      fi | sed -n 's/.*__BRAINSTACK_PATH__//p' | tail -n 1
    )"
  fi
  if [ -n "$__brainstack_detected_path" ]; then
    PATH="$__brainstack_detected_path"
    export PATH
  fi
  unset __brainstack_detected_path
fi
`.trim();
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

function requiredHarnessFlags(harness: HarnessName): string[] {
  return harness === "codex"
    ? ["--dangerously-bypass-approvals-and-sandbox", "--output-last-message"]
    : ["--dangerously-skip-permissions", "--permission-mode", "--output-format"];
}

function buildWorkspaceSeedScript(files: WorkspaceSeedFile[]): string {
  return files
    .map((file, index) => {
      const safePath = cleanRelativePath(file.relativePath);
      const encoded = Buffer.from(file.content).toString("base64");
      const marker = `__CODEX_WORKSPACE_FILE_${index}__`;

      return [
        `mkdir -p ${quoteSh(dirname(safePath))}`,
        `cat <<'${marker}' | base64 -d > ${quoteSh(safePath)}`,
        encoded,
        marker
      ].join("\n");
    })
    .join("\n");
}

export class WorkerService {
  constructor(
    private readonly config: FactoryConfig,
    private readonly db: FactoryDb
  ) {}

  knownHosts(): string[] {
    const contextHosts = this.db.listContexts().map((context) => context.machine);
    const workerHosts = this.db.listWorkers().map((worker) => worker.host);
    const configured = this.config.workers.map((worker) => worker.name);
    return uniqueHosts([...configured, ...contextHosts, ...workerHosts]);
  }

  getWorkerConfig(machine: string): FactoryWorkerConfig | null {
    return this.config.workers.find((worker) => worker.name === machine) || null;
  }

  async refreshWorkers(): Promise<WorkerRecord[]> {
    const workers = await Promise.all(this.knownHosts().map((host) => this.probeWorker(host)));
    return workers.sort((left, right) => left.host.localeCompare(right.host));
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
        "printf 'harness=%s\\n' \"$harness\"",
        "if command -v \"$harness_bin\" >/dev/null 2>&1; then printf 'harness_bin=1\\n'; else printf 'harness_bin=0\\n'; fi",
        "printf 'harness_version=%s\\n' \"$($harness_bin --version 2>&1 | head -n 1 || true)\"",
        "if [ \"$harness\" = codex ]; then help=\"$($harness_bin exec --help 2>&1 || true)\"; else help=\"$($harness_bin --help 2>&1 || true)\"; fi",
        ...requiredHarnessFlags(harness.family).map((needle) => `case "$help" in *${quoteSh(needle)}*) printf 'flag:${needle}=1\\n' ;; *) printf 'flag:${needle}=0\\n' ;; esac`),
        "printf 'home=%s\\n' \"$HOME\""
      ].join("\n"),
      undefined,
      8
    );

    const existing = this.db.getWorker(host);
    const workerRecord: WorkerRecord = {
      host,
      transport: worker.transport,
      status: result.ok ? "healthy" : "unreachable",
      reachable: result.ok,
      localExecution: worker.localExecution,
      sshTarget: worker.sshTarget,
      sshUser: worker.sshUser,
      lastCheckedAt: nowIso(),
      lastSeenAt: result.ok ? nowIso() : (existing?.lastSeenAt || null),
      lastError: result.ok ? null : (result.stderr.trim() || result.stdout.trim() || `exit ${result.exitCode}`),
      details: result.ok ? result.stdout.trim() : null,
      updatedAt: nowIso()
    };

    return this.db.saveWorker(workerRecord);
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
expand_home_path() {
  case "$1" in
    "~") printf '%s\\n' "$HOME" ;;
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

worktree_raw=${quoteSh(context.worktreePath)}
prompt_b64=${quoteSh(promptBase64)}
resume_session=${quoteSh(resumeSessionId)}
harness=${quoteSh(harness.family)}
harness_bin=${quoteSh(harness.bin)}
worktree="$(expand_home_path "$worktree_raw")"
launcher_dir="$(mktemp -d "\${TMPDIR:-/tmp}/clawdex-run.XXXXXX")"
prompt_file="$launcher_dir/control-plane.prompt.md"
runner_file="$launcher_dir/run-harness.sh"

cleanup() {
  rm -rf "$launcher_dir"
}

terminate_harness_run() {
  if [ -n "\${harness_pgid:-}" ]; then
    kill -TERM -- "-$harness_pgid" 2>/dev/null || true
  fi
  kill "$harness_pid" 2>/dev/null || true
  if command -v pkill >/dev/null 2>&1; then
    pkill -TERM -P "$harness_pid" 2>/dev/null || true
  fi
  sleep 1
  if [ -n "\${harness_pgid:-}" ] && kill -0 -- "-$harness_pgid" 2>/dev/null; then
    kill -KILL -- "-$harness_pgid" 2>/dev/null || true
  fi
  if kill -0 "$harness_pid" 2>/dev/null; then
    kill -9 "$harness_pid" 2>/dev/null || true
  fi
  if command -v pkill >/dev/null 2>&1; then
    pkill -KILL -P "$harness_pid" 2>/dev/null || true
  fi
}

trap cleanup EXIT

if ! command -v "$harness_bin" >/dev/null 2>&1; then
  echo "$harness binary not found on worker: $harness_bin" >&2
  exit 30
fi

printf '%s' "$prompt_b64" | base64 -d > "$prompt_file"
cat > "$runner_file" <<'__CLAWDEX_RUNNER__'
set -euo pipefail
${codexModelArgScript}
${claudeArgScript}
image_args=()
cd "$worktree"
mkdir -p .factory
rm -f ${quoteSh(TELEGRAM_ATTACHMENTS_WORKSPACE_PATH)}
cp "$prompt_file" .factory/control-plane.prompt.md
${workspaceSeedScript}
${codexImageArgScript}

if [ "$harness" = "claude" ]; then
  exec "$harness_bin" "\${claude_args[@]}" < "$prompt_file" | tee .factory/last-message.txt
else
  if ${shouldResume ? "true" : "false"}; then
    exec "$harness_bin" exec resume --json --output-last-message .factory/last-message.txt --dangerously-bypass-approvals-and-sandbox "\${model_args[@]}" "\${image_args[@]}" "$resume_session" - < "$prompt_file"
  else
    exec "$harness_bin" exec --json --output-last-message .factory/last-message.txt --dangerously-bypass-approvals-and-sandbox "\${model_args[@]}" "\${image_args[@]}" - < "$prompt_file"
  fi
fi
__CLAWDEX_RUNNER__
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

while kill -0 "$harness_pid" 2>/dev/null; do
  if [ ! -d "$worktree" ]; then
    echo "worktree disappeared during harness run: $worktree" >&2
    terminate_harness_run
    wait "$harness_pid" || true
    exit 88
  fi

  sleep 1
done

wait "$harness_pid"
`;

    let stdoutBuffer = "";
    let seenSessionId: string | null = null;
    const result = await this.runWorkerScript(worker, script, logPath, undefined, async (chunk) => {
      stdoutBuffer += chunk;
      const detected = parseSessionId(stdoutBuffer);
      if (detected && detected !== seenSessionId) {
        seenSessionId = detected;
        await options.onSessionId?.(detected);
      }
    });

    return {
      ...result,
      sessionId: seenSessionId || parseSessionId(result.stdout) || context.codexSessionId || null
    };
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
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

cd "$(expand_home_path ${quoteSh(context.worktreePath)})"
if [ -f ${quoteSh(safePath)} ]; then
  cat ${quoteSh(safePath)}
fi
`;

    const result = await this.runWorkerScript(worker, script, undefined, 10);
    if (!result.ok) {
      return null;
    }

    return result.stdout.trim() || null;
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
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

cd "$(expand_home_path ${quoteSh(context.worktreePath)})"
mkdir -p ${quoteSh(dirname(safePath))}
cat <<'${marker}' | base64 -d > ${quoteSh(safePath)}
${encoded}
${marker}
`;

    const result = await this.runWorkerScript(worker, script, undefined, 10);
    if (!result.ok) {
      throw new Error(result.stderr.trim() || result.stdout.trim() || `exit ${result.exitCode}`);
    }
  }

  async readArtifactFile(context: ContextRecord, filePath: string, maxBytes = 45 * 1024 * 1024): Promise<WorkspaceArtifactFile> {
    if (!this.config.allowAbsoluteArtifactPaths && (filePath.startsWith("/") || filePath === "~" || filePath.startsWith("~/"))) {
      throw new Error("absolute artifact paths are disabled; record a relative path inside the workspace");
    }

    const worker = this.requireWorker(context.machine);
    const script = `
set -euo pipefail
expand_home_path() {
  case "$1" in
    "~") printf '%s\\n' "$HOME" ;;
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

worktree_raw=${quoteSh(context.worktreePath)}
requested_raw=${quoteSh(filePath)}
max_bytes=${quoteSh(String(maxBytes))}
allow_absolute=${quoteSh(this.config.allowAbsoluteArtifactPaths ? "1" : "0")}
worktree="$(expand_home_path "$worktree_raw")"

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
  base64 "$resolved"
else
  python3 - <<'PY' "$resolved"
import base64
import pathlib
import sys
print(base64.b64encode(pathlib.Path(sys.argv[1]).read_bytes()).decode("ascii"))
PY
fi
`;

    const result = await this.runWorkerScript(worker, script, undefined, 20);
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
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

current_branch() {
  git -C "$1" rev-parse --abbrev-ref HEAD 2>/dev/null || printf '%s\\n' ""
}

ensure_factory_files() {
  workspace="$1"
  mkdir -p "$workspace/.factory"

  if [ ! -f "$workspace/.factory/SUMMARY.md" ]; then
    cat > "$workspace/.factory/SUMMARY.md" <<'EOF'
# Summary

- Context initialized.
EOF
  fi

  if [ ! -f "$workspace/.factory/TODO.md" ]; then
    cat > "$workspace/.factory/TODO.md" <<'EOF'
# TODO

- Replace this with the active plan for this topic.
EOF
  fi

  if [ ! -f "$workspace/.factory/ARTIFACTS.md" ]; then
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
    git -C "$workspace" config user.name "Private Dev Factory"
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
  cat > "$workspace/.factory/STATE.json" <<EOF
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

    const target = worker.sshTarget || worker.name;
    return worker.sshUser ? `${worker.sshUser}@${target}` : target;
  }

  private async runWorkerScript(
    worker: FactoryWorkerConfig,
    script: string,
    logPath?: string,
    timeoutSeconds?: number,
    onStdoutChunk?: (chunk: string) => Promise<void> | void
  ): Promise<WorkerExecResult> {
    const scriptToRun = worker.transport === "local" ? script : `${workerUserPathPrelude()}\n${script}`;
    switch (worker.transport) {
      case "local":
        return this.spawnAndCapture(
          ["bash", "-s", "--"],
          scriptToRun,
          worker.name,
          "local",
          logPath,
          timeoutSeconds,
          onStdoutChunk
        );
      case "ssh":
        return this.spawnAndCapture(
          [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            `UserKnownHostsFile=${this.config.sshKnownHostsPath}`,
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
          onStdoutChunk
        );
      case "tailscale-ssh":
        return this.spawnAndCapture(
          ["tailscale", "ssh", this.remoteTarget(worker), "bash", "-s", "--"],
          scriptToRun,
          worker.name,
          "tailscale-ssh",
          logPath,
          timeoutSeconds,
          onStdoutChunk
        );
    }
  }

  private async spawnAndCapture(
    args: string[],
    script: string,
    host: string,
    transport: string,
    logPath?: string,
    timeoutSeconds?: number,
    onStdoutChunk?: (chunk: string) => Promise<void> | void
  ): Promise<WorkerExecResult> {
    const startedAt = Date.now();
    const fullArgs = timeoutSeconds && timeoutSeconds > 0 ? ["timeout", `${timeoutSeconds}s`, ...args] : args;

    const proc = Bun.spawn(fullArgs, {
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
      env: process.env
    });

    proc.stdin.write(script);
    proc.stdin.end();

    const stdoutPromise = this.collectStream(proc.stdout, logPath, "", onStdoutChunk);
    const stderrPromise = this.collectStream(proc.stderr, logPath, "[stderr] ");

    const exitCode = await proc.exited;
    const [stdout, stderr] = await Promise.all([stdoutPromise, stderrPromise]);

    return {
      ok: exitCode === 0,
      host,
      transport,
      exitCode,
      stdout,
      stderr,
      durationMs: Date.now() - startedAt,
      commandLabel: fullArgs.join(" ")
    };
  }

  private async collectStream(
    stream: ReadableStream<Uint8Array> | null | undefined,
    logPath: string | undefined,
    prefix: string,
    onChunk?: (chunk: string) => Promise<void> | void
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
      output += chunk;

      if (logPath) {
        await appendFile(logPath, prefix ? `${prefix}${chunk}` : chunk);
      }

      if (!prefix && onChunk) {
        await onChunk(chunk);
      }
    }

    const tail = decoder.decode();
    if (tail) {
      output += tail;
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
