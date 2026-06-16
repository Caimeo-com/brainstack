import { existsSync } from "node:fs";
import type { ParsedArgs } from "../args";
import type {
  BrainctlStatusSection,
  BrainctlStatusState,
  BrainstackConfig,
  BrainstackWorkerConfig,
  DoctorCheck,
  HarnessName,
  Profile
} from "../config";
import type { run as runtimeRun } from "../runtime";

type RunResult = ReturnType<typeof runtimeRun>;

type CommandProbe = {
  ok: boolean;
  output: string;
  code: number;
  timedOut: boolean;
};

type StatusFleetDeps = {
  PRODUCT_ROOT: string;
  abs: (input: string) => string;
  brainstackDefaultConfigPath: () => string;
  commandPath: (name: string) => string | null;
  controlSshTarget: (
    cfg: BrainstackConfig,
    args: ParsedArgs,
    label: string,
    options?: { allowRemoteSshFallback?: boolean }
  ) => string | null;
  defaultWorkers: (cfg: BrainstackConfig) => BrainstackWorkerConfig[];
  flag: (args: ParsedArgs, name: string) => string | null;
  gitExists: (path: string) => boolean;
  gitStatusProbe: (args: string[], cwd: string, timeoutMs: number) => CommandProbe;
  harnessCompatibility: (name: HarnessName, command: string, options?: { required?: boolean }) => DoctorCheck;
  hasFlag: (args: ParsedArgs, name: string) => boolean;
  installHint: (name: string) => string;
  loadConfig: (path?: string | null, profile?: string | null, root?: string | null) => Promise<BrainstackConfig>;
  parsePositiveIntegerFlag: (args: ParsedArgs, name: string, fallback: number) => number;
  quoteForBash: (value: string) => string;
  remoteBrainctlScript: (remoteRepo: string, argv: string[], options?: { preferInstalledBinary?: boolean }) => string;
  requireFlagValue: (args: ParsedArgs, name: string) => string | null;
  run: typeof runtimeRun;
  runControlRemoteScript: (
    cfg: BrainstackConfig,
    args: ParsedArgs,
    label: string,
    remoteScript: string,
    timeoutMs: number,
    options?: { allowRemoteSshFallback?: boolean }
  ) => RunResult | null;
  runWorkerShell: (cfg: BrainstackConfig, worker: BrainstackWorkerConfig, script: string, timeoutSeconds?: number, usePathCache?: boolean) => RunResult;
  sanitizeStatusError: (error: unknown) => string;
  statusSection: <T>(
    state: BrainctlStatusState,
    detail: string,
    data?: T,
    options?: { available?: boolean; error?: string; durationMs?: number }
  ) => BrainctlStatusSection<T>;
  summarizeLines: (text: string, maxLines?: number) => string;
  telegramControlWorker: (target: string) => BrainstackWorkerConfig;
  updateProbeAllowed: (command: string) => boolean;
  updateProbeTimeoutMs: () => number;
  userShellPathEnv: () => Record<string, string> | undefined;
  workerRemoteTarget: (worker: BrainstackWorkerConfig) => string;
  workerSshHost: (worker: BrainstackWorkerConfig) => string;
  workerSshPortArgs: (worker: BrainstackWorkerConfig) => string[];
  workerSshTrustArgs: (cfg: BrainstackConfig, worker: BrainstackWorkerConfig) => string[];
  env: Record<string, string | undefined>;
  nowIso: () => string;
  whichCommand: (name: string) => string | null;
};

export interface FleetMachineStatus {
  name: string;
  role: "client" | "control" | "worker";
  transport: string;
  reachable: boolean;
  status: BrainctlStatusState;
  update_state: "current" | "behind" | "ahead" | "dirty" | "standalone" | "unknown" | "unreachable" | "failed";
  needs_update: boolean;
  detail: string;
  product_repo?: string | null;
  branch?: string | null;
  head?: string | null;
  short?: string | null;
  remote_ref?: string | null;
  origin_head?: string | null;
  ahead?: number | null;
  behind?: number | null;
  dirty_count?: number | null;
  services?: Record<string, string>;
  error?: string;
}

export interface FleetStatusReport {
  schema_version: 1;
  generated_at: string;
  source_machine: string;
  profile: Profile;
  ok: boolean;
  degraded: boolean;
  machines: FleetMachineStatus[];
  summary: {
    total: number;
    reachable: number;
    needs_update: number;
    unhealthy: number;
  };
}

interface FleetUpdateResult {
  machine: string;
  role: FleetMachineStatus["role"];
  ok: boolean;
  dry_run: boolean;
  code: number;
  output: string;
}

export function createStatusFleetCommands(deps: StatusFleetDeps) {
  const {
    PRODUCT_ROOT,
    abs,
    brainstackDefaultConfigPath,
    commandPath,
    controlSshTarget,
    defaultWorkers,
    flag,
    gitExists,
    gitStatusProbe,
    harnessCompatibility,
    hasFlag,
    installHint,
    loadConfig,
    parsePositiveIntegerFlag,
    quoteForBash,
    remoteBrainctlScript,
    requireFlagValue,
    run,
    runControlRemoteScript,
    runWorkerShell,
    sanitizeStatusError,
    statusSection,
    summarizeLines,
    telegramControlWorker,
    updateProbeAllowed,
    updateProbeTimeoutMs,
    userShellPathEnv,
    workerRemoteTarget,
    workerSshHost,
    workerSshPortArgs,
    workerSshTrustArgs,
    env,
    nowIso,
    whichCommand
  } = deps;

  function parseKeyValueLines(text: string): Record<string, string> {
    const values: Record<string, string> = {};
    for (const line of text.split(/\r?\n/)) {
      const index = line.indexOf("=");
      if (index <= 0) {
        continue;
      }
      values[line.slice(0, index)] = line.slice(index + 1).trim();
    }
    return values;
  }

  function parseAheadBehind(value: string | undefined): { ahead: number; behind: number } | null {
    const match = (value || "").trim().match(/^(\d+)\s+(\d+)$/);
    if (!match) {
      return null;
    }
    return { ahead: Number(match[1]), behind: Number(match[2]) };
  }

  function clientControlSshTarget(cfg: BrainstackConfig): string | null {
    return cfg.client.telegramVia || null;
  }

  function remoteControlSourceScript(remoteRepo: string): string {
    return `
  set -u
  repo=${quoteForBash(remoteRepo || "~/brainstack")}
  case "$repo" in
    \\~) repo="$HOME" ;;
    \\~/*) repo="$HOME/\${repo#\\~/}" ;;
  esac
  printf 'repo=%s\\n' "$repo"
  if [ ! -d "$repo/.git" ]; then
    printf 'state=missing\\n'
    exit 20
  fi
  cd "$repo" || exit 21
  branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
  head="$(git rev-parse HEAD 2>/dev/null || true)"
  short="$(git rev-parse --short HEAD 2>/dev/null || true)"
  dirty_count="$(git status --porcelain 2>/dev/null | wc -l | tr -d '[:space:]' || true)"
  remote_ref=""
  for candidate in origin/main refs/remotes/https-main; do
    if git rev-parse --verify "$candidate" >/dev/null 2>&1; then
      remote_ref="$candidate"
      break
    fi
  done
  origin_head=""
  ahead_behind=""
  if [ -n "$remote_ref" ]; then
    origin_head="$(git rev-parse --verify "$remote_ref" 2>/dev/null || true)"
    ahead_behind="$(git rev-list --left-right --count "HEAD...$remote_ref" 2>/dev/null || true)"
  fi
  printf 'state=ok\\n'
  printf 'branch=%s\\n' "$branch"
  printf 'head=%s\\n' "$head"
  printf 'short=%s\\n' "$short"
  printf 'dirty_count=%s\\n' "$dirty_count"
  printf 'remote_ref=%s\\n' "$remote_ref"
  printf 'origin_head=%s\\n' "$origin_head"
  printf 'ahead_behind=%s\\n' "$ahead_behind"
  `.trim();
  }

  async function collectControlSourceStatusSection(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
    if (!cfg.profile.startsWith("client")) {
      return statusSection("disabled", "local profile owns product source status", { profile: cfg.profile });
    }
    const target = clientControlSshTarget(cfg);
    if (!target) {
      return statusSection("disabled", "control SSH target not configured", { remote_repo: cfg.client.telegramRemoteRepo }, { available: false });
    }

    const worker = telegramControlWorker(target);
    const script = remoteControlSourceScript(cfg.client.telegramRemoteRepo || "~/brainstack");
    const sshBin = whichCommand("ssh") || "ssh";
    const result = run(
      [
        sshBin,
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=3",
        ...workerSshTrustArgs(cfg, worker),
        ...workerSshPortArgs(worker),
        workerRemoteTarget(worker),
        "bash",
        "-lc",
        script
      ],
      { check: false, timeoutMs }
    );
    const combined = `${result.stdout}\n${result.stderr}`.trim();
    if (result.timedOut || result.code === 124) {
      return statusSection("warn", "control host source probe timed out", { machine: target, remote_repo: cfg.client.telegramRemoteRepo }, { available: false, error: "timeout" });
    }
    const values = parseKeyValueLines(result.stdout);
    if (result.code !== 0) {
      const missing = values.state === "missing";
      return statusSection(
        "warn",
        missing ? "control host source checkout missing" : "control host source probe failed",
        { machine: target, remote_repo: cfg.client.telegramRemoteRepo, ...values },
        { available: false, error: sanitizeStatusError(combined || `exit ${result.code}`) }
      );
    }
    const dirtyCount = Number(values.dirty_count || "0");
    const aheadBehind = parseAheadBehind(values.ahead_behind);
    const short = values.short || (values.head ? values.head.slice(0, 7) : "unknown");
    let state: BrainctlStatusState = "ok";
    let detail = `control host up to date branch=${values.branch || "unknown"} head=${short}`;
    if (Number.isFinite(dirtyCount) && dirtyCount > 0) {
      state = "warn";
      detail = `control host source dirty files=${dirtyCount} head=${short}`;
    } else if (aheadBehind && aheadBehind.behind > 0) {
      state = "warn";
      detail = `control host behind origin by ${aheadBehind.behind} commit(s) head=${short}`;
    } else if (aheadBehind && aheadBehind.ahead > 0) {
      state = "warn";
      detail = `control host has ${aheadBehind.ahead} unpushed commit(s) head=${short}`;
    } else if (!values.remote_ref) {
      state = "warn";
      detail = `control host remote ref unavailable head=${short}`;
    }
    return statusSection(state, detail, {
      machine: target,
      remote_repo: cfg.client.telegramRemoteRepo,
      repo: values.repo || null,
      branch: values.branch || null,
      head: values.head || null,
      short,
      dirty_count: Number.isFinite(dirtyCount) ? dirtyCount : null,
      remote_ref: values.remote_ref || null,
      origin_head: values.origin_head || null,
      ahead: aheadBehind?.ahead ?? null,
      behind: aheadBehind?.behind ?? null
    });
  }

  function fleetServiceNames(cfg: BrainstackConfig, role: FleetMachineStatus["role"]): string[] {
    if (role === "client") {
      return [];
    }
    if (role === "control") {
      return ["braind.service", ...(cfg.telemux.enabled ? ["telemux.service"] : [])];
    }
    return ["brainstackd.service"];
  }

  function serviceStates(names: string[]): Record<string, string> {
    const states: Record<string, string> = {};
    if (!names.length) {
      return states;
    }
    if (!commandPath("systemctl")) {
      for (const name of names) {
        states[name] = "unknown";
      }
      return states;
    }
    for (const name of names) {
      const result = run(["systemctl", "--user", "is-active", name], { check: false, timeoutMs: 2000 });
      states[name] = result.stdout.trim() || (result.code === 0 ? "active" : "inactive");
    }
    return states;
  }

  function serviceStatesUnhealthy(states: Record<string, string>): boolean {
    return Object.values(states).some((state) => state !== "active" && state !== "unknown");
  }

  function gitSnapshotForFleet(productRoot: string, timeoutMs: number, options: { fetch?: boolean } = {}): Record<string, string> {
    const values: Record<string, string> = { product_repo: productRoot };
    if (!existsSync(productRoot)) {
      values.state = "missing";
      return values;
    }
    if (!gitExists(productRoot)) {
      values.state = "not-git";
      return values;
    }
    if (options.fetch) {
      const fetchResult = gitStatusProbe(["fetch", "--quiet", "origin", "main"], productRoot, Math.min(Math.max(timeoutMs, 500), 20_000));
      if (!fetchResult.ok) {
        values.fetch_error = fetchResult.output || `exit ${fetchResult.code}`;
      }
    }
    values.state = "ok";
    values.branch = gitStatusProbe(["rev-parse", "--abbrev-ref", "HEAD"], productRoot, timeoutMs).output;
    values.head = gitStatusProbe(["rev-parse", "HEAD"], productRoot, timeoutMs).output;
    values.short = gitStatusProbe(["rev-parse", "--short", "HEAD"], productRoot, timeoutMs).output;
    const dirty = gitStatusProbe(["status", "--porcelain"], productRoot, timeoutMs);
    values.dirty_count = dirty.ok && dirty.output ? String(dirty.output.split(/\r?\n/).filter(Boolean).length) : "0";
    values.remote_ref = ["origin/main", "refs/remotes/https-main"].find((candidate) => gitStatusProbe(["rev-parse", "--verify", candidate], productRoot, timeoutMs).ok) || "";
    if (values.remote_ref) {
      values.origin_head = gitStatusProbe(["rev-parse", "--verify", values.remote_ref], productRoot, timeoutMs).output;
      values.ahead_behind = gitStatusProbe(["rev-list", "--left-right", "--count", `HEAD...${values.remote_ref}`], productRoot, timeoutMs).output;
    }
    if (!options.fetch) {
      const liveRemote = gitStatusProbe(["ls-remote", "origin", "refs/heads/main"], productRoot, Math.min(Math.max(timeoutMs, 500), 5000));
      const liveHead = liveRemote.ok ? liveRemote.output.split(/\s+/)[0] || "" : "";
      if (liveHead) {
        const existingAheadBehind = parseAheadBehind(values.ahead_behind);
        values.remote_ref = values.remote_ref || "origin/main";
        values.origin_head = liveHead;
        if (values.head && values.head !== liveHead && !(existingAheadBehind && existingAheadBehind.ahead > 0)) {
          values.ahead_behind = "0 1";
          values.live_remote = "true";
        }
      }
    }
    return values;
  }

  function fleetStatusFromValues(input: {
    name: string;
    role: FleetMachineStatus["role"];
    transport: string;
    values: Record<string, string>;
    services?: Record<string, string>;
    standaloneOk?: boolean;
    error?: string;
  }): FleetMachineStatus {
    const values = input.values;
    const services = input.services || {};
    const reachable = !input.error && values.state !== "unreachable";
    const dirtyCount = Number(values.dirty_count || "0");
    const aheadBehind = parseAheadBehind(values.ahead_behind);
    const short = values.short || (values.head ? values.head.slice(0, 12) : null);
    const serviceUnhealthy = serviceStatesUnhealthy(services);
    if (!reachable) {
      return {
        name: input.name,
        role: input.role,
        transport: input.transport,
        reachable: false,
        status: "warn",
        update_state: "unreachable",
        needs_update: false,
        detail: input.error || "unreachable",
        product_repo: values.product_repo || null,
        services,
        error: input.error
      };
    }
    if (values.state === "missing" || values.state === "not-git") {
      const standalone = input.standaloneOk && input.role === "client";
      return {
        name: input.name,
        role: input.role,
        transport: input.transport,
        reachable: true,
        status: standalone ? "ok" : "warn",
        update_state: standalone ? "standalone" : "unknown",
        needs_update: false,
        detail: standalone ? "client binary install; no product source checkout" : `product repo ${values.state}`,
        product_repo: values.product_repo || null,
        services
      };
    }
    let status: BrainctlStatusState = "ok";
    let updateState: FleetMachineStatus["update_state"] = "current";
    let needsUpdate = false;
    let detail = `current head=${short || "unknown"}`;
    if (Number.isFinite(dirtyCount) && dirtyCount > 0) {
      status = "warn";
      updateState = "dirty";
      detail = `dirty files=${dirtyCount} head=${short || "unknown"}`;
    } else if (aheadBehind && aheadBehind.ahead > 0) {
      status = "warn";
      updateState = "ahead";
      detail =
        aheadBehind.behind > 0
          ? `diverged from origin ahead=${aheadBehind.ahead} behind=${aheadBehind.behind} head=${short || "unknown"}`
          : `ahead of origin by ${aheadBehind.ahead} commit(s) head=${short || "unknown"}`;
    } else if (aheadBehind && aheadBehind.behind > 0) {
      status = "warn";
      updateState = "behind";
      needsUpdate = true;
      detail = `behind origin by ${aheadBehind.behind} commit(s) head=${short || "unknown"}`;
    } else if (!values.remote_ref) {
      status = "warn";
      updateState = "unknown";
      detail = `remote ref unavailable head=${short || "unknown"}`;
    }
    if (serviceUnhealthy) {
      status = "warn";
      detail = `${detail}; service not active`;
    }
    if (values.fetch_error) {
      status = "warn";
      detail = `${detail}; fetch failed`;
    }
    return {
      name: input.name,
      role: input.role,
      transport: input.transport,
      reachable: true,
      status,
      update_state: updateState,
      needs_update: needsUpdate,
      detail,
      product_repo: values.product_repo || null,
      branch: values.branch || null,
      head: values.head || null,
      short,
      remote_ref: values.remote_ref || null,
      origin_head: values.origin_head || null,
      ahead: aheadBehind?.ahead ?? null,
      behind: aheadBehind?.behind ?? null,
      dirty_count: Number.isFinite(dirtyCount) ? dirtyCount : null,
      services,
      ...(values.fetch_error ? { error: values.fetch_error } : {})
    };
  }

  async function localFleetMachineStatus(
    cfg: BrainstackConfig,
    role: FleetMachineStatus["role"],
    timeoutMs: number,
    options: { fetch?: boolean; standaloneOk?: boolean } = {}
  ): Promise<FleetMachineStatus> {
    const values = gitSnapshotForFleet(cfg.paths.productRepo || PRODUCT_ROOT, timeoutMs, { fetch: options.fetch });
    return fleetStatusFromValues({
      name: cfg.machine.name,
      role,
      transport: "local",
      values,
      services: serviceStates(fleetServiceNames(cfg, role)),
      standaloneOk: options.standaloneOk
    });
  }

  function fleetProbeScript(productRepo: string, services: string[], fetch: boolean): string {
    const serviceLines = services.map((service) => `printf 'service:${service}=%s\\n' "$(systemctl --user is-active ${quoteForBash(service)} 2>/dev/null || printf inactive)"`);
    return `
  set -u
  brainstack_expand_home() {
    case "$1" in
      \\~) printf '%s\\n' "$HOME" ;;
      \\~/*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
      *) printf '%s\\n' "$1" ;;
    esac
  }
  repo="$(brainstack_expand_home ${quoteForBash(productRepo)})"
  printf 'state=ok\\n'
  printf 'product_repo=%s\\n' "$repo"
  if [ ! -d "$repo" ]; then
    printf 'state=missing\\n'
    exit 0
  fi
  if [ ! -d "$repo/.git" ]; then
    printf 'state=not-git\\n'
    exit 0
  fi
  if [ ${fetch ? "1" : "0"} -eq 1 ]; then
    fetch_error="$(git -C "$repo" fetch --quiet origin main 2>&1 || true)"
    if [ -n "$fetch_error" ]; then printf 'fetch_error=%s\\n' "$(printf '%s' "$fetch_error" | tr '\\n' ' ' | cut -c1-300)"; fi
  fi
  printf 'branch=%s\\n' "$(git -C "$repo" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
  printf 'head=%s\\n' "$(git -C "$repo" rev-parse HEAD 2>/dev/null || true)"
  printf 'short=%s\\n' "$(git -C "$repo" rev-parse --short HEAD 2>/dev/null || true)"
  printf 'dirty_count=%s\\n' "$(git -C "$repo" status --porcelain 2>/dev/null | wc -l | tr -d '[:space:]' || true)"
  remote_ref=""
  for candidate in origin/main refs/remotes/https-main; do
    if git -C "$repo" rev-parse --verify "$candidate" >/dev/null 2>&1; then
      remote_ref="$candidate"
      break
    fi
  done
  printf 'remote_ref=%s\\n' "$remote_ref"
  if [ -n "$remote_ref" ]; then
    printf 'origin_head=%s\\n' "$(git -C "$repo" rev-parse --verify "$remote_ref" 2>/dev/null || true)"
    printf 'ahead_behind=%s\\n' "$(git -C "$repo" rev-list --left-right --count "HEAD...$remote_ref" 2>/dev/null || true)"
  fi
  if [ ${fetch ? "1" : "0"} -eq 0 ]; then
    live_remote="$(git -C "$repo" ls-remote origin refs/heads/main 2>/dev/null | awk 'NR == 1 { print $1 }' || true)"
    if [ -n "$live_remote" ]; then
      [ -n "$remote_ref" ] || printf 'remote_ref=%s\\n' "origin/main"
      printf 'origin_head=%s\\n' "$live_remote"
      head_now="$(git -C "$repo" rev-parse HEAD 2>/dev/null || true)"
      ahead_now="$(git -C "$repo" rev-list --left-right --count "HEAD...$remote_ref" 2>/dev/null | awk '{ print $1 }' || true)"
      if [ -n "$head_now" ] && [ "$head_now" != "$live_remote" ] && [ "\${ahead_now:-0}" = "0" ]; then
        printf 'ahead_behind=%s\\n' "0 1"
        printf 'live_remote=%s\\n' "true"
      fi
    fi
  fi
  if command -v systemctl >/dev/null 2>&1; then
  ${serviceLines.join("\n")}
  fi
  `.trim();
  }

  async function workerFleetMachineStatus(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, timeoutMs: number, fetch: boolean): Promise<FleetMachineStatus> {
    const script = fleetProbeScript(cfg.paths.productRepo || "~/brainstack", fleetServiceNames(cfg, "worker"), fetch);
    const result = runWorkerShell(cfg, worker, script, Math.max(3, Math.ceil(timeoutMs / 1000)), true);
    const combined = `${result.stdout}\n${result.stderr}`.trim();
    if (result.timedOut || result.code === 124) {
      return fleetStatusFromValues({ name: worker.name, role: "worker", transport: worker.transport, values: { state: "unreachable" }, error: "timed out" });
    }
    if (result.code !== 0) {
      return fleetStatusFromValues({
        name: worker.name,
        role: "worker",
        transport: worker.transport,
        values: { state: "unreachable" },
        error: sanitizeStatusError(combined || `exit ${result.code}`)
      });
    }
    const values = parseKeyValueLines(result.stdout);
    const services: Record<string, string> = {};
    for (const [key, value] of Object.entries(values)) {
      if (key.startsWith("service:")) {
        services[key.slice("service:".length)] = value || "unknown";
      }
    }
    return fleetStatusFromValues({ name: worker.name, role: "worker", transport: worker.transport, values, services });
  }

  function finalizeFleetStatus(report: FleetStatusReport): FleetStatusReport {
    report.summary = {
      total: report.machines.length,
      reachable: report.machines.filter((machine) => machine.reachable).length,
      needs_update: report.machines.filter((machine) => machine.needs_update).length,
      unhealthy: report.machines.filter((machine) => machine.status === "warn" || machine.status === "fail").length
    };
    report.ok = report.summary.unhealthy === 0;
    report.degraded = !report.ok;
    return report;
  }

  async function collectLocalFleetStatus(cfg: BrainstackConfig, timeoutMs: number, options: { fetch?: boolean } = {}): Promise<FleetStatusReport> {
    const role: FleetMachineStatus["role"] = cfg.profile === "client-macos" ? "client" : cfg.profile === "worker" ? "worker" : "control";
    const report: FleetStatusReport = {
      schema_version: 1,
      generated_at: nowIso(),
      source_machine: cfg.machine.name,
      profile: cfg.profile,
      ok: false,
      degraded: true,
      machines: [],
      summary: { total: 0, reachable: 0, needs_update: 0, unhealthy: 0 }
    };
    report.machines.push(await localFleetMachineStatus(cfg, role, timeoutMs, { fetch: options.fetch, standaloneOk: cfg.profile === "client-macos" }));
    if (cfg.profile === "control" || cfg.profile === "single-node") {
      for (const worker of defaultWorkers(cfg)) {
        if (worker.name === cfg.machine.name || worker.transport === "local") {
          continue;
        }
        report.machines.push(await workerFleetMachineStatus(cfg, worker, timeoutMs, Boolean(options.fetch)));
      }
    }
    return finalizeFleetStatus(report);
  }

  function fleetControlSshTarget(cfg: BrainstackConfig, args: ParsedArgs): string | null {
    return controlSshTarget(cfg, args, "fleet control SSH target", { allowRemoteSshFallback: true });
  }

  function fleetControlFallbackName(cfg: BrainstackConfig, args: ParsedArgs): string {
    try {
      const via = fleetControlSshTarget(cfg, args);
      return via ? workerSshHost(telegramControlWorker(via)) : "control";
    } catch {
      return cfg.client.telegramVia || "control";
    }
  }

  function looksLikeUnsupportedFleetCommand(text: string): boolean {
    const normalized = text.toLowerCase();
    return normalized.includes("unknown command: fleet") || (normalized.includes("unknown command") && normalized.includes("brainctl init"));
  }

  function runFleetControlSsh(cfg: BrainstackConfig, args: ParsedArgs, argv: string[], timeoutMs: number): ReturnType<typeof run> | null {
    const remoteRepo = requireFlagValue(args, "remote-repo") || env.BRAINSTACK_TELEGRAM_REMOTE_REPO?.trim() || cfg.client.telegramRemoteRepo || "~/brainstack";
    return runControlRemoteScript(
      cfg,
      args,
      "fleet control SSH target",
      remoteBrainctlScript(remoteRepo, argv, { preferInstalledBinary: true }),
      timeoutMs,
      { allowRemoteSshFallback: true }
    );
  }

  function clientFleetTargetIsControl(cfg: BrainstackConfig, args: ParsedArgs, target: string | undefined): boolean {
    if (!target || target === "control") {
      return true;
    }
    const via = fleetControlSshTarget(cfg, args);
    if (!via) {
      return false;
    }
    const worker = telegramControlWorker(via);
    return target === via || target === workerRemoteTarget(worker) || target === workerSshHost(worker);
  }

  function runFleetControlBootstrapUpdate(cfg: BrainstackConfig, args: ParsedArgs, dryRun: boolean): FleetUpdateResult {
    const remoteRepo = requireFlagValue(args, "remote-repo") || env.BRAINSTACK_TELEGRAM_REMOTE_REPO?.trim() || cfg.client.telegramRemoteRepo || "~/brainstack";
    const script = fleetUpdateScript("~/.config/brainstack/brainstack.yaml", "control", remoteRepo, dryRun);
    if (dryRun) {
      return { machine: fleetControlFallbackName(cfg, args), role: "control", ok: true, dry_run: true, code: 0, output: script };
    }
    const result = runControlRemoteScript(cfg, args, "fleet control SSH target", script, 300_000, { allowRemoteSshFallback: true });
    if (!result) {
      throw new Error("fleet update needs a control SSH target for client-macos installs");
    }
    return {
      machine: fleetControlFallbackName(cfg, args),
      role: "control",
      ok: result.code === 0,
      dry_run: false,
      code: result.code,
      output: summarizeLines(`${result.stdout}\n${result.stderr}`.trim(), 100)
    };
  }

  async function collectFleetStatus(cfg: BrainstackConfig, args: ParsedArgs, timeoutMs: number, options: { fetch?: boolean } = {}): Promise<FleetStatusReport> {
    if (cfg.profile === "client-macos") {
      const local = await collectLocalFleetStatus(cfg, timeoutMs, { fetch: false });
      const remote = runFleetControlSsh(cfg, args, ["fleet", "status", "--json", ...(options.fetch ? ["--fetch"] : [])], timeoutMs);
      if (!remote) {
        return finalizeFleetStatus(local);
      }
      if (remote.code !== 0 || remote.timedOut) {
        const combined = `${remote.stdout}\n${remote.stderr}`.trim();
        if (!remote.timedOut && looksLikeUnsupportedFleetCommand(combined)) {
          local.machines.push({
            name: fleetControlFallbackName(cfg, args),
            role: "control",
            transport: "ssh",
            reachable: true,
            status: "warn",
            update_state: "unknown",
            needs_update: true,
            detail: "control host brainctl is too old for fleet status; update control host",
            services: {},
            error: sanitizeStatusError(combined)
          });
          return finalizeFleetStatus(local);
        }
        local.machines.push(
          fleetStatusFromValues({
            name: fleetControlFallbackName(cfg, args),
            role: "control",
            transport: "ssh",
            values: { state: "unreachable" },
            error: remote.timedOut ? "timed out" : sanitizeStatusError(combined || `exit ${remote.code}`)
          })
        );
        return finalizeFleetStatus(local);
      }
      try {
        const parsed = JSON.parse(remote.stdout) as FleetStatusReport;
        const names = new Set(local.machines.map((machine) => machine.name));
        local.machines.push(...(parsed.machines || []).filter((machine) => !names.has(machine.name)));
        return finalizeFleetStatus(local);
      } catch (error) {
        local.machines.push(
          fleetStatusFromValues({
            name: fleetControlFallbackName(cfg, args),
            role: "control",
            transport: "ssh",
            values: { state: "unreachable" },
            error: `could not parse fleet status: ${sanitizeStatusError(error)}`
          })
        );
        return finalizeFleetStatus(local);
      }
    }
    return await collectLocalFleetStatus(cfg, timeoutMs, options);
  }

  async function collectFleetStatusSection(cfg: BrainstackConfig, args: ParsedArgs, timeoutMs: number): Promise<BrainctlStatusSection> {
    const report = await collectFleetStatus(cfg, args, timeoutMs, { fetch: false });
    const state: BrainctlStatusState = report.summary.unhealthy > 0 ? "warn" : "ok";
    return statusSection(state, `machines=${report.summary.total} reachable=${report.summary.reachable} needs_update=${report.summary.needs_update} unhealthy=${report.summary.unhealthy}`, report);
  }

  function formatFleetStatus(report: FleetStatusReport): string {
    const lines = [
      `fleet: ok=${report.ok} degraded=${report.degraded} machines=${report.summary.total} reachable=${report.summary.reachable} needs_update=${report.summary.needs_update} unhealthy=${report.summary.unhealthy}`
    ];
    for (const machine of report.machines) {
      lines.push(
        [
          `${machine.status.toUpperCase()} ${machine.name}`,
          `role=${machine.role}`,
          `transport=${machine.transport}`,
          `state=${machine.update_state}`,
          machine.short ? `head=${machine.short}` : null,
          machine.behind ? `behind=${machine.behind}` : null,
          machine.ahead ? `ahead=${machine.ahead}` : null,
          machine.dirty_count ? `dirty=${machine.dirty_count}` : null,
          `detail=${machine.detail}`
        ]
          .filter(Boolean)
          .join(" ")
      );
    }
    return lines.join("\n");
  }

  function fleetUpdateScript(configPath: string, role: FleetMachineStatus["role"], productRepo: string, dryRun: boolean): string {
    const services = role === "worker" ? ["brainstackd.service"] : ["braind.service", "telemux.service", "brainstackd.service"];
    const commands = [
      "set -euo pipefail",
      "brainstack_expand_home() {",
      "  case \"$1\" in",
      "    \"~\") printf '%s\\n' \"$HOME\" ;;",
      "    \\~/*) printf '%s/%s\\n' \"$HOME\" \"${1#\\~/}\" ;;",
      "    *) printf '%s\\n' \"$1\" ;;",
      "  esac",
      "}",
      `repo="$(brainstack_expand_home ${quoteForBash(productRepo)})"`,
      `config="$(brainstack_expand_home ${quoteForBash(configPath)})"`,
      "brainctl_bin=\"$HOME/.local/bin/brainctl\"",
      "brainctl_tmp=\"$HOME/.local/bin/.brainctl.$$\"",
      "trap 'rm -f \"$brainctl_tmp\"' EXIT",
      "if [ -x \"$HOME/.bun/bin/bun\" ]; then bun_bin=\"$HOME/.bun/bin/bun\"; else bun_bin=\"$(command -v bun)\"; fi",
      "cd \"$repo\"",
      "git fetch --quiet origin main",
      "git merge --ff-only origin/main",
      "\"$bun_bin\" install --frozen-lockfile",
      "mkdir -p \"$(dirname \"$brainctl_bin\")\"",
      "\"$bun_bin\" build packages/brainctl/src/main.ts --compile --no-compile-autoload-dotenv --no-compile-autoload-bunfig --outfile \"$brainctl_tmp\"",
      "chmod 755 \"$brainctl_tmp\"",
      "mv -f \"$brainctl_tmp\" \"$brainctl_bin\"",
      "\"$brainctl_bin\" upgrade --config \"$config\"",
      "if command -v systemctl >/dev/null 2>&1; then",
      "  systemctl --user daemon-reload || true",
      ...services.map((service) => `  if systemctl --user cat ${quoteForBash(service)} >/dev/null 2>&1; then systemctl --user restart ${quoteForBash(service)}; fi`),
      "fi",
      "printf 'updated=true\\n'",
      "printf 'head=%s\\n' \"$(git rev-parse --short HEAD 2>/dev/null || true)\""
    ];
    if (!dryRun) {
      return commands.join("\n");
    }
    return commands.map((line) => `# ${line}`).join("\n");
  }

  async function runFleetUpdateLocal(
    cfg: BrainstackConfig,
    machine: string,
    role: FleetMachineStatus["role"],
    configPath: string,
    dryRun: boolean
  ): Promise<FleetUpdateResult> {
    const script = fleetUpdateScript(configPath, role, cfg.paths.productRepo || PRODUCT_ROOT, dryRun);
    if (dryRun) {
      return { machine, role, ok: true, dry_run: true, code: 0, output: script };
    }
    const result = run(["bash", "-lc", script], { check: false, timeoutMs: 300_000 });
    return {
      machine,
      role,
      ok: result.code === 0,
      dry_run: false,
      code: result.code,
      output: summarizeLines(`${result.stdout}\n${result.stderr}`.trim(), 80)
    };
  }

  async function runFleetUpdateWorker(
    cfg: BrainstackConfig,
    worker: BrainstackWorkerConfig,
    configPath: string,
    dryRun: boolean
  ): Promise<FleetUpdateResult> {
    const script = fleetUpdateScript(configPath, "worker", cfg.paths.productRepo || "~/brainstack", dryRun);
    if (dryRun) {
      return { machine: worker.name, role: "worker", ok: true, dry_run: true, code: 0, output: script };
    }
    const result = runWorkerShell(cfg, worker, script, 300, true);
    return {
      machine: worker.name,
      role: "worker",
      ok: result.code === 0,
      dry_run: false,
      code: result.code,
      output: summarizeLines(`${result.stdout}\n${result.stderr}`.trim(), 80)
    };
  }

  async function runFleetUpdates(cfg: BrainstackConfig, args: ParsedArgs, configPath: string): Promise<FleetUpdateResult[]> {
    const dryRun = hasFlag(args, "dry-run");
    const target = args.positional[1];
    const updateAll = hasFlag(args, "all") || target === "all";
    if (!updateAll && !target) {
      throw new Error("fleet update requires a machine name or --all");
    }
    const results: FleetUpdateResult[] = [];
    const workers = defaultWorkers(cfg).filter((worker) => worker.name !== cfg.machine.name && worker.transport !== "local");
    if (updateAll) {
      for (const worker of workers) {
        results.push(await runFleetUpdateWorker(cfg, worker, configPath, dryRun));
      }
      if (cfg.profile === "control" || cfg.profile === "single-node") {
        results.push(await runFleetUpdateLocal(cfg, cfg.machine.name, "control", configPath, dryRun));
      } else if (cfg.profile === "worker") {
        results.push(await runFleetUpdateLocal(cfg, cfg.machine.name, "worker", configPath, dryRun));
      }
      return results;
    }
    if (target === cfg.machine.name) {
      const role: FleetMachineStatus["role"] = cfg.profile === "worker" ? "worker" : cfg.profile === "client-macos" ? "client" : "control";
      if (role === "client") {
        throw new Error("fleet update cannot self-update a standalone macOS client install; install a signed release or rebuild the local binary/app bundle");
      }
      results.push(await runFleetUpdateLocal(cfg, cfg.machine.name, role, configPath, dryRun));
      return results;
    }
    const worker = workers.find((candidate) => candidate.name === target);
    if (!worker) {
      throw new Error(`unknown fleet machine: ${target}`);
    }
    results.push(await runFleetUpdateWorker(cfg, worker, configPath, dryRun));
    return results;
  }

  async function commandFleet(args: ParsedArgs): Promise<void> {
    const sub = args.positional[0] || "status";
    if (sub === "help" || sub === "--help" || sub === "-h") {
      console.log("Usage: brainctl fleet status [--json] [--fetch|--no-fetch]\n       brainctl fleet update <machine|all> [--all] [--json] [--dry-run] [--via SSH_TARGET] [--remote-repo PATH]");
      return;
    }
    const configPath = abs(requireFlagValue(args, "config") || brainstackDefaultConfigPath());
    const cfg = await loadConfig(configPath, flag(args, "profile"), flag(args, "root"));
    if (sub === "status") {
      const report = await collectFleetStatus(cfg, args, parsePositiveIntegerFlag(args, "timeout-ms", 20_000), { fetch: !hasFlag(args, "no-fetch") });
      if (hasFlag(args, "json")) {
        console.log(JSON.stringify(report, null, 2));
      } else {
        console.log(formatFleetStatus(report));
      }
      return;
    }
    if (sub === "update") {
      if (cfg.profile === "client-macos") {
        const target = args.positional[1];
        const updateAll = hasFlag(args, "all") || target === "all";
        if (!updateAll && !target) {
          throw new Error("fleet update requires a machine name or --all");
        }
        const results: FleetUpdateResult[] = [];
        if (updateAll || clientFleetTargetIsControl(cfg, args, target)) {
          const bootstrap = runFleetControlBootstrapUpdate(cfg, args, hasFlag(args, "dry-run"));
          results.push(bootstrap);
          if (!bootstrap.ok) {
            if (hasFlag(args, "json")) {
              console.log(JSON.stringify({ ok: false, results }, null, 2));
            } else {
              console.log(`${bootstrap.ok ? "OK" : "FAIL"} ${bootstrap.machine} role=${bootstrap.role} dry_run=${bootstrap.dry_run} exit=${bootstrap.code}`);
              if (bootstrap.output) console.log(bootstrap.output);
            }
            throw new Error("control host bootstrap update failed");
          }
          if (!updateAll || hasFlag(args, "dry-run")) {
            if (hasFlag(args, "json")) {
              console.log(JSON.stringify({ ok: true, results }, null, 2));
            } else {
              console.log(`${bootstrap.ok ? "OK" : "FAIL"} ${bootstrap.machine} role=${bootstrap.role} dry_run=${bootstrap.dry_run} exit=${bootstrap.code}`);
              if (bootstrap.output) console.log(bootstrap.output);
            }
            return;
          }
        }
        const argv = ["fleet", "update", ...(hasFlag(args, "all") ? ["--all"] : args.positional[1] ? [args.positional[1]] : []), ...(hasFlag(args, "json") ? ["--json"] : []), ...(hasFlag(args, "dry-run") ? ["--dry-run"] : [])];
        const result = runFleetControlSsh(cfg, args, argv, 300_000);
        if (!result) {
          throw new Error("fleet update needs a control SSH target for client-macos installs");
        }
        if (result.stdout) process.stdout.write(result.stdout);
        if (result.stderr) process.stderr.write(result.stderr);
        if (result.code !== 0) {
          throw new Error(`fleet update failed over ssh with exit ${result.code}`);
        }
        return;
      }
      const results = await runFleetUpdates(cfg, args, configPath);
      if (hasFlag(args, "json")) {
        console.log(JSON.stringify({ ok: results.every((result) => result.ok), results }, null, 2));
      } else {
        for (const result of results) {
          console.log(`${result.ok ? "OK" : "FAIL"} ${result.machine} role=${result.role} dry_run=${result.dry_run} exit=${result.code}`);
          if (result.output) {
            console.log(result.output);
          }
        }
      }
      if (results.some((result) => !result.ok)) {
        throw new Error("one or more fleet updates failed");
      }
      return;
    }
    throw new Error(`Unknown fleet subcommand: ${sub}`);
  }

  return {
    collectControlSourceStatusSection,
    collectFleetStatusSection,
    commandFleet
  };
}
