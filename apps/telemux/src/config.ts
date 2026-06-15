import { chmodSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

export type WorkerTransport = "local" | "ssh" | "tailscale-ssh";
export type HarnessName = "codex" | "claude";
export type WorkerSshTrustMode = "pinned" | "accept-new";
export type UsageAdapter = "manual";

export interface PreDispatchClassifierConfig {
  enabled: boolean;
  apiKey: string;
  model: string;
  reasoningEffort: string | null;
  timeoutMs: number;
  maxChars: number;
  confidenceThreshold: number;
}

export interface FactoryWorkerConfig {
  name: string;
  transport: WorkerTransport;
  sshTarget: string | null;
  sshUser: string | null;
  managedRepoRoot: string;
  managedHostRoot: string;
  managedScratchRoot: string;
  localExecution: boolean;
  sshTrustMode: WorkerSshTrustMode;
  sshKnownHostsPath: string | null;
  harness: HarnessName | null;
  harnessBin: string | null;
  notes: string | null;
  capabilities: string[];
}

export interface FactoryConfig {
  projectRoot: string;
  stateRoot: string;
  controlRoot: string;
  dbPath: string;
  contextsDir: string;
  cronSnapshotsDir: string;
  logsDir: string;
  sshKnownHostsPath: string;
  factoryRoot: string;
  managedRepoRoot: string;
  managedHostRoot: string;
  managedScratchRoot: string;
  dashboardHost: string;
  dashboardPort: number;
  dashboardToken: string | null;
  telegramBotToken: string;
  telegramBotUsername: string | null;
  telegramControlChatId: number | null;
  allowedTelegramUserId: number;
  telegramPollTimeoutSeconds: number;
  telegramApiTimeoutMs: number;
  telegramFileTransferTimeoutMs: number;
  cronPollIntervalSeconds: number;
  workerRunTimeoutSeconds: number;
  workerCaptureMaxBytes: number;
  localMachine: string;
  workersFilePath?: string | null;
  workersFileExplicit?: boolean;
  workersFileInitiallyLoaded?: boolean;
  workers: FactoryWorkerConfig[];
  usageAdapter: UsageAdapter;
  harness: HarnessName;
  harnessBin: string;
  codexBin: string;
  brainBaseUrl: string;
  brainImportToken: string;
  brainAdminToken: string;
  allowAbsoluteArtifactPaths: boolean;
  textCoalesceMs: number;
  pendingTextRecoveryMaxAgeMs: number;
  preDispatchClassifier: PreDispatchClassifierConfig;
}

export interface WorkerConfigInput {
  name: string;
  transport?: string;
  sshTarget?: string | null;
  sshUser?: string | null;
  managedRepoRoot?: string;
  managedHostRoot?: string;
  managedScratchRoot?: string;
  localExecution?: boolean;
  sshTrustMode?: string | null;
  sshTrust?: string | null;
  sshKnownHostsPath?: string | null;
  harness?: string | null;
  harnessBin?: string | null;
  notes?: string | null;
  capabilities?: string[];
}

function readNumber(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name]?.trim();
  if (!raw) {
    return fallback;
  }

  const value = Number(raw);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function readOptionalNumber(env: NodeJS.ProcessEnv, name: string): number | null {
  const raw = env[name]?.trim();
  if (!raw) {
    return null;
  }

  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

function readBoolean(env: NodeJS.ProcessEnv, name: string, fallback = false): boolean {
  const raw = env[name]?.trim().toLowerCase();
  if (!raw) {
    return fallback;
  }

  if (["1", "true", "yes", "on"].includes(raw)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(raw)) {
    return false;
  }

  return fallback;
}

function readRatio(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name]?.trim();
  if (!raw) {
    return fallback;
  }

  const value = Number(raw);
  return Number.isFinite(value) && value >= 0 && value <= 1 ? value : fallback;
}

function resolveMaybeRelative(projectRoot: string, value: string): string {
  const expanded = expandHome(value);
  if (expanded.startsWith("/")) {
    return expanded;
  }
  return resolve(projectRoot, expanded);
}

function expandHome(value: string): string {
  if (value === "~") {
    return process.env.HOME || value;
  }
  if (value.startsWith("~/")) {
    return resolve(process.env.HOME || ".", value.slice(2));
  }
  return value;
}

function resolvePath(value: string): string {
  const expanded = expandHome(value);
  return expanded.startsWith("/") ? expanded : resolve(expanded);
}

function normalizeTransport(value: string | undefined, fallback: WorkerTransport): WorkerTransport {
  if (value === "local" || value === "ssh" || value === "tailscale-ssh") {
    return value;
  }

  return fallback;
}

function normalizeHarness(value: string | undefined): HarnessName {
  return value === "claude" ? "claude" : "codex";
}

function normalizeUsageAdapter(value: string | undefined): UsageAdapter {
  const adapter = value?.trim() || "manual";
  if (adapter === "manual") {
    return "manual";
  }
  throw new Error(`Unsupported FACTORY_USAGE_ADAPTER=${adapter}; supported value: manual`);
}

function normalizeSshTrustMode(value: string | null | undefined, transport: WorkerTransport): WorkerSshTrustMode {
  if (transport === "local" || transport === "tailscale-ssh") {
    return "pinned";
  }
  if (value === "accept-new") {
    return "accept-new";
  }
  return "pinned";
}

function normalizeWorkerConfig(
  input: WorkerConfigInput,
  defaults: {
    localMachine: string;
    managedRepoRoot: string;
    managedHostRoot: string;
    managedScratchRoot: string;
  }
): FactoryWorkerConfig {
  const name = input.name.trim();
  const fallbackTransport: WorkerTransport = name === defaults.localMachine ? "local" : "ssh";
  const transport = normalizeTransport(input.transport, fallbackTransport);

  return {
    name,
    transport,
    sshTarget: transport === "local" ? null : (input.sshTarget?.trim() || name),
    sshUser: transport === "local" ? null : (input.sshUser?.trim() || null),
    managedRepoRoot: input.managedRepoRoot?.trim() ? resolvePath(input.managedRepoRoot.trim()) : defaults.managedRepoRoot,
    managedHostRoot: input.managedHostRoot?.trim() ? resolvePath(input.managedHostRoot.trim()) : defaults.managedHostRoot,
    managedScratchRoot: input.managedScratchRoot?.trim() ? resolvePath(input.managedScratchRoot.trim()) : defaults.managedScratchRoot,
    localExecution: input.localExecution ?? transport === "local",
    sshTrustMode: normalizeSshTrustMode(input.sshTrustMode?.trim() || input.sshTrust?.trim() || null, transport),
    sshKnownHostsPath: input.sshKnownHostsPath?.trim() ? resolvePath(input.sshKnownHostsPath.trim()) : null,
    harness: input.harness?.trim() ? normalizeHarness(input.harness.trim()) : null,
    harnessBin: input.harnessBin?.trim() || null,
    notes: input.notes?.trim() || null,
    capabilities: Array.isArray(input.capabilities) ? input.capabilities.map(String).filter(Boolean) : []
  };
}

function workersFilePath(projectRoot: string, env: NodeJS.ProcessEnv): { path: string; explicit: boolean } {
  const workersFile = env.FACTORY_WORKERS_FILE?.trim() || "./workers.json";
  return {
    path: resolveMaybeRelative(projectRoot, workersFile),
    explicit: Boolean(env.FACTORY_WORKERS_FILE?.trim())
  };
}

function loadWorkerInputs(projectRoot: string, env: NodeJS.ProcessEnv): {
  path: string;
  explicit: boolean;
  loaded: boolean;
  inputs: WorkerConfigInput[];
} {
  const resolved = workersFilePath(projectRoot, env);
  if (!existsSync(resolved.path)) {
    if (resolved.explicit) {
      throw new Error(`FACTORY_WORKERS_FILE does not exist: ${resolved.path}`);
    }

    return {
      path: resolved.path,
      explicit: resolved.explicit,
      loaded: false,
      inputs: [
        {
          name: env.FACTORY_LOCAL_MACHINE?.trim() || "control",
          transport: "local"
        }
      ]
    };
  }

  return {
    path: resolved.path,
    explicit: resolved.explicit,
    loaded: true,
    inputs: JSON.parse(readFileSync(resolved.path, "utf8")) as WorkerConfigInput[]
  };
}

function uniqueWorkers(workers: FactoryWorkerConfig[]): FactoryWorkerConfig[] {
  const seen = new Set<string>();
  const ordered: FactoryWorkerConfig[] = [];

  for (const worker of workers) {
    if (seen.has(worker.name)) {
      continue;
    }

    seen.add(worker.name);
    ordered.push(worker);
  }

  return ordered;
}

export function normalizeWorkerInputs(
  inputs: WorkerConfigInput[],
  defaults: {
    localMachine: string;
    managedRepoRoot: string;
    managedHostRoot: string;
    managedScratchRoot: string;
  }
): FactoryWorkerConfig[] {
  return uniqueWorkers(inputs.map((input) => normalizeWorkerConfig(input, defaults)));
}

export function loadWorkerConfigsFromPath(
  path: string,
  defaults: {
    localMachine: string;
    managedRepoRoot: string;
    managedHostRoot: string;
    managedScratchRoot: string;
  }
): FactoryWorkerConfig[] {
  return normalizeWorkerInputs(JSON.parse(readFileSync(path, "utf8")) as WorkerConfigInput[], defaults);
}

const projectRoot = resolve(import.meta.dir, "..");

function defaultStateRoot(): string {
  return resolve(process.env.HOME || ".", ".local", "state", "brainstack");
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): FactoryConfig {
  const stateRoot = env.BRAINSTACK_STATE_ROOT?.trim() ? resolvePath(env.BRAINSTACK_STATE_ROOT.trim()) : defaultStateRoot();
  const controlRoot = env.FACTORY_CONTROL_ROOT?.trim() ? resolvePath(env.FACTORY_CONTROL_ROOT.trim()) : resolve(stateRoot, "telemux");
  const factoryRoot = env.FACTORY_FACTORY_ROOT?.trim() ? resolvePath(env.FACTORY_FACTORY_ROOT.trim()) : resolve(stateRoot, "factory");
  const localMachine = env.FACTORY_LOCAL_MACHINE?.trim() || "control";
  const managedRepoRoot = resolve(factoryRoot, "repos");
  const managedHostRoot = resolve(factoryRoot, "hostctx");
  const managedScratchRoot = resolve(factoryRoot, "scratch");
  const workerSource = loadWorkerInputs(projectRoot, env);

  const workerDefaults = {
    localMachine,
    managedRepoRoot,
    managedHostRoot,
    managedScratchRoot
  };
  const workers = normalizeWorkerInputs(workerSource.inputs, workerDefaults);
  const harness = normalizeHarness(env.FACTORY_HARNESS?.trim());
  const harnessBin = env.FACTORY_HARNESS_BIN?.trim() || (harness === "codex" ? env.FACTORY_CODEX_BIN?.trim() || "codex" : "claude");

  return {
    projectRoot,
    stateRoot,
    controlRoot,
    dbPath: resolve(controlRoot, "db.sqlite"),
    contextsDir: resolve(controlRoot, "contexts"),
    cronSnapshotsDir: resolve(controlRoot, "crons"),
    logsDir: resolve(controlRoot, "logs"),
    sshKnownHostsPath: env.FACTORY_SSH_KNOWN_HOSTS?.trim() ? resolvePath(env.FACTORY_SSH_KNOWN_HOSTS.trim()) : resolve(controlRoot, "ssh_known_hosts"),
    factoryRoot,
    managedRepoRoot,
    managedHostRoot,
    managedScratchRoot,
    dashboardHost: env.FACTORY_DASHBOARD_HOST?.trim() || "127.0.0.1",
    dashboardPort: readNumber(env, "FACTORY_DASHBOARD_PORT", 8787),
    dashboardToken: env.FACTORY_DASHBOARD_TOKEN?.trim() || null,
    telegramBotToken: env.FACTORY_TELEGRAM_BOT_TOKEN?.trim() || "",
    telegramBotUsername: env.FACTORY_TELEGRAM_BOT_USERNAME?.trim().replace(/^@/, "").toLowerCase() || null,
    telegramControlChatId: readOptionalNumber(env, "FACTORY_TELEGRAM_CONTROL_CHAT_ID"),
    allowedTelegramUserId: readNumber(env, "FACTORY_ALLOWED_TELEGRAM_USER_ID", 0),
    telegramPollTimeoutSeconds: readNumber(env, "FACTORY_TELEGRAM_POLL_TIMEOUT_SECONDS", 30),
    telegramApiTimeoutMs: readNumber(env, "FACTORY_TELEGRAM_API_TIMEOUT_MS", 15_000),
    telegramFileTransferTimeoutMs: readNumber(env, "FACTORY_TELEGRAM_FILE_TRANSFER_TIMEOUT_MS", 60_000),
    cronPollIntervalSeconds: readNumber(env, "FACTORY_CRON_POLL_INTERVAL_SECONDS", 30),
    workerRunTimeoutSeconds: readNumber(env, "FACTORY_WORKER_RUN_TIMEOUT_SECONDS", 21600),
    workerCaptureMaxBytes: readNumber(env, "FACTORY_WORKER_CAPTURE_MAX_BYTES", 256 * 1024),
    localMachine,
    workersFilePath: workerSource.path,
    workersFileExplicit: workerSource.explicit,
    workersFileInitiallyLoaded: workerSource.loaded,
    workers,
    usageAdapter: normalizeUsageAdapter(env.FACTORY_USAGE_ADAPTER),
    harness,
    harnessBin,
    codexBin: env.FACTORY_CODEX_BIN?.trim() || (harness === "codex" ? harnessBin : "codex"),
    brainBaseUrl: env.BRAIN_BASE_URL?.trim() || "",
    brainImportToken: env.BRAIN_IMPORT_TOKEN?.trim() || "",
    brainAdminToken: env.FACTORY_BRAIN_ADMIN_TOKEN?.trim() || env.BRAIN_ADMIN_TOKEN?.trim() || "",
    allowAbsoluteArtifactPaths: ["1", "true", "yes", "on"].includes((env.FACTORY_ALLOW_ABSOLUTE_ARTIFACT_PATHS || "").toLowerCase()),
    textCoalesceMs: readNumber(env, "FACTORY_TEXT_COALESCE_MS", 1500),
    pendingTextRecoveryMaxAgeMs: readNumber(env, "FACTORY_TEXT_COALESCE_RECOVERY_MAX_AGE_MS", 5 * 60_000),
    preDispatchClassifier: {
      enabled: readBoolean(env, "FACTORY_PRE_DISPATCH_CLASSIFIER", false),
      apiKey: env.FACTORY_PRE_DISPATCH_CLASSIFIER_API_KEY?.trim() || "",
      model: env.FACTORY_PRE_DISPATCH_CLASSIFIER_MODEL?.trim() || "gpt-5.4-mini",
      reasoningEffort: env.FACTORY_PRE_DISPATCH_CLASSIFIER_REASONING_EFFORT?.trim() || "minimal",
      timeoutMs: readNumber(env, "FACTORY_PRE_DISPATCH_CLASSIFIER_TIMEOUT_MS", 800),
      maxChars: readNumber(env, "FACTORY_PRE_DISPATCH_CLASSIFIER_MAX_CHARS", 600),
      confidenceThreshold: readRatio(env, "FACTORY_PRE_DISPATCH_CLASSIFIER_CONFIDENCE", 0.75)
    }
  };
}

export const config = loadConfig();

export function ensureProjectPaths(targetConfig: FactoryConfig = config): void {
  const dirs = [
    targetConfig.stateRoot,
    targetConfig.controlRoot,
    targetConfig.contextsDir,
    targetConfig.cronSnapshotsDir,
    targetConfig.logsDir,
    targetConfig.factoryRoot,
    targetConfig.managedRepoRoot,
    targetConfig.managedHostRoot,
    targetConfig.managedScratchRoot
  ];

  for (const dir of dirs) {
    mkdirSync(dir, { recursive: true, mode: 0o700 });
    try {
      chmodSync(dir, 0o700);
    } catch {
      // Best-effort on filesystems that do not support POSIX modes.
    }
  }
}
