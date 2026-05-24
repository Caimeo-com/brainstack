import { appendFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type { CodexReasoningEffort } from "./codex-runtime";
import { FactoryConfig, FactoryWorkerConfig, HarnessName } from "./config";
import { ContextKind, ContextRecord, ContextState, FactoryDb, WorkerRecord } from "./db";
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
  elif [ -n "\${harness_bin:-}" ] && command -v "$harness_bin" >/dev/null 2>&1; then
    __brainstack_detected_path=""
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
    PATH="$__brainstack_detected_path:$PATH"
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
        ...(worker.transport === "local" ? [workerUserPathPrelude()] : []),
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
${base64DecodeShellFunction()}
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
  codex_args+=(--json --output-last-message "$last_message_tmp" --dangerously-bypass-approvals-and-sandbox)
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
    let seenSessionId: string | null = null;
    const result = await this.runWorkerScript(worker, script, logPath, this.config.workerRunTimeoutSeconds, async (chunk) => {
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

  async runUpdateCheck(context: ContextRecord, logPath?: string): Promise<WorkerExecResult & { reportPath: string | null }> {
    const worker = this.requireWorker(context.machine);
    const productRoot = worker.localExecution ? this.config.projectRoot : "~/brainstack";
    const script = `
set -euo pipefail
expand_home_path() {
  case "$1" in
    "~") printf '%s\\n' "$HOME" ;;
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

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

worktree="$(expand_home_path ${quoteSh(context.worktreePath)})"
product_root="$(expand_home_path ${quoteSh(productRoot)})"
config_path="$(expand_home_path ~/.config/brainstack/brainstack.yaml)"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
report=".factory/reports/update-check-$stamp.md"
cd "$worktree"
worktree_real="$(pwd -P)"
if [ -L .factory ]; then
  echo "refusing symlinked .factory directory" >&2
  exit 46
fi
mkdir -p .factory
factory_real="$(cd .factory && pwd -P)"
case "$factory_real" in
  "$worktree_real/.factory") ;;
  *) echo ".factory escapes worktree" >&2; exit 46 ;;
esac
if [ -L .factory/reports ]; then
  echo "refusing symlinked .factory/reports directory" >&2
  exit 46
fi
mkdir -p .factory/reports
reports_real="$(cd .factory/reports && pwd -P)"
case "$reports_real" in
  "$factory_real/reports") ;;
  *) echo ".factory/reports escapes worktree" >&2; exit 46 ;;
esac
report_path="$reports_real/update-check-$stamp.md"
tmp_report="$(mktemp "$reports_real/.update-check.XXXXXX")"
cleanup_update_check() {
  rm -f "$tmp_report"
  if [ -n "\${artifact_lock_dir:-}" ] && [ -d "$artifact_lock_dir" ]; then
    rmdir "$artifact_lock_dir" 2>/dev/null || true
  fi
}
trap cleanup_update_check EXIT

{
  printf '# Update Check\\n\\n'
  printf -- '- machine: %s\\n' "$(hostname 2>/dev/null || printf unknown)"
  printf -- '- generated_at: %s\\n\\n' "$stamp"
  if [ -f "$product_root/packages/brainctl/src/main.ts" ] && command -v bun >/dev/null 2>&1 && [ -f "$config_path" ]; then
    printf '## brainctl updates\\n'
    (cd "$product_root" && BRAINSTACK_UPDATE_PROBE_TIMEOUT_MS="\${BRAINSTACK_UPDATE_PROBE_TIMEOUT_MS:-20000}" bun run packages/brainctl/src/main.ts updates --config "$config_path") 2>&1 || printf 'brainctl updates failed with exit=%s\\n' "$?"
  else
    printf '## fallback checks\\n'
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
  printf '\\nNo packages or services were changed by this deterministic update check.\\n'
} > "$tmp_report"
mv -f "$tmp_report" "$report_path"

artifacts_path="$factory_real/ARTIFACTS.md"
if [ -L "$artifacts_path" ]; then
  echo "refusing symlinked .factory/ARTIFACTS.md" >&2
  exit 46
fi
if [ -e "$artifacts_path" ] && [ ! -f "$artifacts_path" ]; then
  echo "refusing non-file .factory/ARTIFACTS.md" >&2
  exit 46
fi
artifact_lock_dir="$factory_real/.artifacts.lock"
artifact_lock_acquired=0
for _ in 1 2 3 4 5 6 7 8 9 10; do
  if mkdir "$artifact_lock_dir" 2>/dev/null; then
    artifact_lock_acquired=1
    break
  fi
  sleep 0.2
done
if [ "$artifact_lock_acquired" != "1" ]; then
  echo "timed out waiting for artifact metadata lock" >&2
  exit 75
fi
if [ ! -f "$artifacts_path" ]; then
  tmp_artifacts="$(mktemp "$factory_real/.artifacts.XXXXXX")"
  printf '# Artifacts\\n' > "$tmp_artifacts"
  mv -f "$tmp_artifacts" "$artifacts_path"
fi
if ! grep -F "$report" "$artifacts_path" >/dev/null 2>&1; then
  tmp_artifacts="$(mktemp "$factory_real/.artifacts.XXXXXX")"
  cat "$artifacts_path" > "$tmp_artifacts"
  printf '\\n- \`%s\` - read-only OS, Brainstack, Codex, and Claude update report\\n' "$report" >> "$tmp_artifacts"
  mv -f "$tmp_artifacts" "$artifacts_path"
fi

printf 'BRAINSTACK_UPDATE_REPORT=%s\\n' "$report"
cat "$report_path"
	`;

    const currentBeforeLaunch = this.db.getContextBySlug(context.slug);
    if (currentBeforeLaunch?.state === "archived") {
      return {
        ok: false,
        host: worker.name,
        transport: worker.transport,
        exitCode: 89,
        stdout: "",
        stderr: `context archived before update-check launch: ${context.slug}`,
        durationMs: 0,
        commandLabel: "update-check",
        reportPath: null
      };
    }

    const result = await this.runWorkerScript(worker, script, logPath, 120);
    const reportPath = result.stdout.match(/^BRAINSTACK_UPDATE_REPORT=(.+)$/m)?.[1]?.trim() || null;
    return {
      ...result,
      reportPath
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
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#~/}" ;;
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
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#~/}" ;;
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
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#~/}" ;;
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
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#~/}" ;;
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
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#~/}" ;;
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

    const stdoutPromise = this.collectStream(proc.stdout, logPath, "", onStdoutChunk);
    const stderrPromise = this.collectStream(proc.stderr, logPath, "[stderr] ");

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
