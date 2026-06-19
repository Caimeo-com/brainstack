import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { lstat, readlink, rm } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { flag, hasFlag, requireFlagValue, type ParsedArgs } from "../args";
import { abs, absWithHome, shellSingleQuote } from "../paths";
import {
  MIN_BUN_VERSION,
  PRODUCT_ROOT,
  brainstackDefaultConfigPath,
  loadConfig,
  profileRequiresBunRuntime,
  truthyEnv,
  type BrainctlStatusReport,
  type BrainctlStatusSection,
  type BrainctlStatusState,
  type BrainstackConfig,
  type BrainstackWorkerConfig,
  type CheckStatus,
  type ControlSshTrustMode,
  type DoctorCheck,
  type HarnessName,
  type HookTarget,
  type WorkerSshTrustMode
} from "../config";
import { ensureDir, run, writeText } from "../runtime";
import { createStatusFleetCommands } from "./status-fleet";
import { createUpdatesCommand } from "./updates";
import { summarizeOutboxTerminalErrors } from "../outbox-summary";

type CommandProbeResult = { ok: boolean; output: string; code: number; timedOut: boolean };
type RunResult = ReturnType<typeof run>;

type DoctorStatusDeps = {
  compareVersions: (a: string, b: string) => number;
  canonicalJson: (value: unknown) => string;
  sha256Hex: (text: string) => string;
  installedBunVersion: (pathOrName?: string) => string | null;
  runtimeCommandAvailable: (pathOrName: string) => boolean;
  commandPath: (name: string) => string | null;
  userShellPathEnv: () => Record<string, string> | undefined;
  updateProbeTimeoutMs: () => number;
  updateProbeAllowed: (command: string) => boolean;
  installHint: (name: string) => string;
  testHarnessSudo: (harness: { name: HarnessName; bin: string }) => Promise<void>;
  gitExists: (path: string) => boolean;
  managedManifestPath: (cfg: BrainstackConfig) => string;
  runsBraind: (cfg: BrainstackConfig) => boolean;
  usesUserServices: (cfg: BrainstackConfig) => boolean;
  usesBrainstackDaemon: (cfg: BrainstackConfig) => boolean;
  usesLocalHarnessGuidance: (cfg: BrainstackConfig) => boolean;
  telemuxService: (cfg: BrainstackConfig) => string;
  clientLocalPathAbs: (cfg: BrainstackConfig) => string;
  clientEnvPathAbs: (cfg: BrainstackConfig) => string;
  readEnvFile: (path: string) => Record<string, string>;
  resolveClientEnvValue: (cfg: BrainstackConfig, name: string) => string;
  brainWriteConfig: (cfg: BrainstackConfig) => { baseUrl: string; token: string };
  brainApiBaseUrl: (cfg: BrainstackConfig) => string;
  brainWriteSmokeCheck: (cfg: BrainstackConfig) => Promise<DoctorCheck>;
  scanAllOutboxes: (cfg: BrainstackConfig) => Promise<Array<{ root: string; items: Array<{ item: { terminal_error?: unknown } }>; corrupt: unknown[] }>>;
  listOutboxEntries: (cfg: BrainstackConfig) => Promise<Array<{ item: { terminal_error?: unknown } }>>;
  hookConfigPath: (target: HookTarget) => string;
  readJsonObject: (path: string) => Promise<Record<string, unknown>>;
  countCodexStyleManagedHooks: (raw: Record<string, unknown>, target: HookTarget) => number;
  countCursorManagedHooks: (raw: Record<string, unknown>, target: HookTarget) => number;
  skillInstallRootForTarget: (target: HookTarget, args: ParsedArgs) => string;
  skillDirsForDoctor: (root: string) => Promise<string[]>;
  proposalReviewGroupsFromResult: (result: Record<string, unknown>) => Array<Record<string, unknown>>;
  daemonStatusPath: (cfg: BrainstackConfig) => string;
  readDaemonStatus: (cfg: BrainstackConfig) => Promise<{ ok: boolean; updated_at?: string; pid?: number } | null>;
  daemonPlatform: (args: ParsedArgs) => "launchd" | "systemd";
  daemonServicePath: (cfg: BrainstackConfig, platform: "launchd" | "systemd") => string;
  processAlive: (pid: number) => boolean;
  processCommandLine: (pid: number) => string | null;
  daemonStatusFresh: (status: { ok: boolean; updated_at?: string } | null, maxAgeMs: number) => boolean;
  daemonServiceStatus: (cfg: BrainstackConfig, args: ParsedArgs, timeoutMs?: number) => Promise<{ platform: "launchd" | "systemd"; installed: boolean; running: boolean | null; path: string; detail: string }>;
  sshTargetFromRemoteSsh: (remote: string) => string | null;
  validateInviteSshTarget: (input: string, label: string) => string;
  telegramControlWorker: (target: string) => BrainstackWorkerConfig;
  telegramKnownHostsPath: (cfg: BrainstackConfig, args: ParsedArgs) => string;
  telegramSshTrustMode: (args: ParsedArgs) => ControlSshTrustMode;
  telegramSshTrustArgs: (mode: ControlSshTrustMode, knownHostsPath: string) => string[];
  normalizeOutboxItem: (item: { terminal_error?: unknown } & Record<string, unknown>) => { terminal_error?: unknown } & Record<string, unknown>;
  repoLockInfo: (lockPath: string) => Promise<{ exists: boolean; ageMs: number | null; pidAlive: string; safeToClear: boolean; reason: string }>;
  parsePositiveIntegerFlag: (args: ParsedArgs, key: string, fallback: number) => number;
  defaultWorkers: (cfg: BrainstackConfig) => BrainstackWorkerConfig[];
  summarizeLines: (text: string, maxLines?: number) => string;
};

export function createDoctorStatusCommands(deps: DoctorStatusDeps) {
  const {
    compareVersions,
    canonicalJson,
    sha256Hex,
    installedBunVersion,
    runtimeCommandAvailable,
    commandPath,
    userShellPathEnv,
    updateProbeTimeoutMs,
    updateProbeAllowed,
    installHint,
    testHarnessSudo,
    gitExists,
    managedManifestPath,
    runsBraind,
    usesUserServices,
    usesBrainstackDaemon,
    usesLocalHarnessGuidance,
    telemuxService,
    clientLocalPathAbs,
    clientEnvPathAbs,
    readEnvFile,
    resolveClientEnvValue,
    brainWriteConfig,
    brainApiBaseUrl,
    brainWriteSmokeCheck,
    scanAllOutboxes,
    listOutboxEntries,
    hookConfigPath,
    readJsonObject,
    countCodexStyleManagedHooks,
    countCursorManagedHooks,
    skillInstallRootForTarget,
    skillDirsForDoctor,
    proposalReviewGroupsFromResult,
    daemonStatusPath,
    readDaemonStatus,
    daemonPlatform,
    daemonServicePath,
    processAlive,
    processCommandLine,
    daemonStatusFresh,
    daemonServiceStatus,
    sshTargetFromRemoteSsh,
    validateInviteSshTarget,
    telegramControlWorker,
    telegramKnownHostsPath,
    telegramSshTrustMode,
    telegramSshTrustArgs,
    normalizeOutboxItem,
    repoLockInfo,
    parsePositiveIntegerFlag,
    defaultWorkers,
    summarizeLines
  } = deps;

function check(status: CheckStatus, section: string, name: string, detail: string, remediation?: string): DoctorCheck {
    return { status, section, name, detail, remediation };
  }

  function commandOk(name: string): boolean {
    return commandPath(name) !== null;
  }

  interface CommandProbe {
    text: string;
    code: number;
    timedOut: boolean;
  }

  function firstProbeLine(stdout: string, stderr: string): string {
    return (stdout || stderr).trim().split(/\r?\n/)[0] || "unknown";
  }

  function commandVersionProbe(name: string): CommandProbe {
    const executable = commandPath(name) || name;
    const proc =
      name === "ssh"
        ? run([executable, "-V"], { check: false, env: userShellPathEnv(), timeoutMs: updateProbeTimeoutMs() })
        : run([executable, "--version"], { check: false, env: userShellPathEnv(), timeoutMs: updateProbeTimeoutMs() });
    return {
      text: proc.timedOut ? proc.stderr || `timed out after ${updateProbeTimeoutMs()}ms` : firstProbeLine(proc.stdout, proc.stderr),
      code: proc.code,
      timedOut: proc.timedOut
    };
  }

  function commandVersion(name: string): string {
    return commandVersionProbe(name).text;
  }

  function commandHelpProbe(name: string, args: string[] = ["--help"]): CommandProbe {
    const proc = run([commandPath(name) || name, ...args], { check: false, env: userShellPathEnv(), timeoutMs: updateProbeTimeoutMs() });
    return {
      text: `${proc.stdout}\n${proc.stderr}`,
      code: proc.code,
      timedOut: proc.timedOut
    };
  }

  function harnessCompatibility(name: HarnessName, bin: string, options: { required?: boolean } = {}): DoctorCheck {
    const requiredHarness = options.required ?? true;
    const failureStatus: CheckStatus = requiredHarness ? "FAIL" : "WARN";
    if (!commandOk(bin)) {
      return check(failureStatus, "versions", `${name}-harness`, `${bin} not found in PATH`, requiredHarness ? installHint(name) : undefined);
    }
    const versionProbe = commandVersionProbe(bin);
    const helpProbe = name === "codex" ? commandHelpProbe(bin, ["exec", "--help"]) : commandHelpProbe(bin, ["--help"]);
    const version = versionProbe.text;
    if (versionProbe.timedOut || helpProbe.timedOut) {
      return check(failureStatus, "versions", `${name}-harness`, `${version}; CLI compatibility probe timed out`, `Update ${name} manually, then rerun doctor. ${installHint(name)}`);
    }
    if (helpProbe.code !== 0) {
      return check(failureStatus, "versions", `${name}-harness`, `${version}; CLI help probe exited ${helpProbe.code}`, `Update ${name} manually, then rerun doctor. ${installHint(name)}`);
    }
    const help = helpProbe.text;
    const required =
      name === "codex"
        ? ["--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check", "--output-last-message"]
        : ["--dangerously-skip-permissions", "--permission-mode", "--output-format"];
    const missing = required.filter((needle) => !help.includes(needle));
    if (missing.length) {
      return check(
        failureStatus,
        "versions",
        `${name}-harness`,
        `${version}; missing required CLI surface: ${missing.join(", ")}`,
        `Update ${name} manually, then rerun doctor. ${installHint(name)}`
      );
    }
    return check("PASS", "versions", `${name}-harness`, `${version}; required CLI surface present`);
  }

  function envHasKey(path: string, key: string): boolean {
    if (!existsSync(path)) {
      return false;
    }
    const text = readFileSync(path, "utf8");
    return new RegExp(`^${key}=.+`, "m").test(text);
  }

  function gitClean(path: string): DoctorCheck {
    if (!gitExists(path)) {
      return check("FAIL", "git", `git-clean:${path}`, "not a git worktree");
    }
    const dirty = run(["git", "status", "--porcelain"], { cwd: path, check: false }).stdout.trim();
    return dirty
      ? check("WARN", "git", `git-clean:${path}`, "worktree has uncommitted changes", "Inspect before applying runtime changes.")
      : check("PASS", "git", `git-clean:${path}`, "clean");
  }

  async function lockDoctorCheck(label: string, lockPath: string): Promise<DoctorCheck> {
    const info = await repoLockInfo(lockPath);
    if (!info.exists) {
      return check("PASS", "locks", label, `absent: ${lockPath}`);
    }
    const safe = info.safeToClear ? "safe-to-clear=yes" : "safe-to-clear=no";
    return check(
      "WARN",
      "locks",
      label,
      `present age_ms=${info.ageMs ?? "unknown"} pid_alive=${info.pidAlive} ${safe}; ${info.reason}`,
      `Inspect with brainctl repo-lock status --config <config>; clear only after confirming no write is active.`
    );
  }

  function pendingReindexPathFor(cfg: BrainstackConfig): string {
    return join(cfg.repos.serve, "derived", "search-reindex-needed.json");
  }

  function outboxParentRoot(cfg: BrainstackConfig): string {
    return join(cfg.paths.stateRoot, "outbox");
  }

  function outboxRoot(cfg: BrainstackConfig): string {
    return join(outboxParentRoot(cfg), brainInstanceId(cfg));
  }

  function outboxRootForDestination(cfg: BrainstackConfig, destination: { brainId?: string; baseUrl?: string }): string {
    const base = destination.brainId || destination.baseUrl || cfg.brain.publicBaseUrl || cfg.client.remoteSsh || cfg.client.localPath;
    return join(outboxParentRoot(cfg), sha256Hex(base).slice(0, 32));
  }

  function brainInstanceId(cfg: BrainstackConfig): string {
    const base = cfg.brain.publicBaseUrl || cfg.client.remoteSsh || cfg.client.localPath;
    return sha256Hex(base).slice(0, 32);
  }

  async function harnessGuidanceChecks(cfg: BrainstackConfig): Promise<DoctorCheck[]> {
    const checks: DoctorCheck[] = [];
    const bootstrapRoot = join(cfg.paths.configRoot, "client-bootstrap");
    const codexAgents = join(cfg.paths.home, ".codex", "AGENTS.md");
    const codexInclude = join(bootstrapRoot, "codex-shared-brain.include.md");
    const claudeFile = join(cfg.paths.home, ".claude", "CLAUDE.md");
    const claudeInclude = `@${join(bootstrapRoot, "claude-user-CLAUDE.md")}`;
    const cursorRule = join(cfg.paths.home, ".cursor", "rules", "shared-brain.md");
    const guidanceNeedle = "brainctl context --repo .";
    const codexRelevant = cfg.harness.name === "codex" || existsSync(codexAgents) || existsSync(codexInclude);
    const claudeRelevant = cfg.harness.name === "claude" || existsSync(claudeFile);

    if (codexRelevant) {
      if (!existsSync(codexInclude)) {
        checks.push(
          check(
            "WARN",
            "guidance",
            "codex-bootstrap-include",
            `${codexInclude} missing`,
            `Run brainctl init or brainctl bootstrap-client --out ${bootstrapRoot}.`
          )
        );
      }
      if (existsSync(codexAgents)) {
        const info = await lstat(codexAgents).catch(() => null);
        const target = info?.isSymbolicLink() ? await readlink(codexAgents).catch(() => "") : "";
        const text = info?.isSymbolicLink() ? "" : readFileSync(codexAgents, "utf8");
        checks.push(
          target === codexInclude || text.includes(guidanceNeedle)
            ? check("PASS", "guidance", "codex-agents", target === codexInclude ? `symlinked to ${codexInclude}` : "contains Brainstack context guidance")
            : check("WARN", "guidance", "codex-agents", `${codexAgents} exists but Brainstack guidance was not detected`, `Append: cat ${codexInclude} >> ${codexAgents}`)
        );
      } else {
        checks.push(check("WARN", "guidance", "codex-agents", `${codexAgents} missing`, "Run brainctl init or merge the generated client bootstrap guidance."));
      }
    }

    if (claudeRelevant) {
      if (existsSync(claudeFile)) {
        const text = readFileSync(claudeFile, "utf8");
        checks.push(
          text.includes(claudeInclude) || text.includes(guidanceNeedle)
            ? check("PASS", "guidance", "claude", "contains Brainstack context guidance")
            : check("WARN", "guidance", "claude", `${claudeFile} exists but Brainstack guidance was not detected`, `Add this line: ${claudeInclude}`)
        );
      } else {
        checks.push(check("WARN", "guidance", "claude", `${claudeFile} missing`, "Run brainctl init or merge the generated client bootstrap guidance."));
      }
    }

    if (existsSync(cursorRule)) {
      const text = readFileSync(cursorRule, "utf8");
      checks.push(
        text.includes(guidanceNeedle)
          ? check("PASS", "guidance", "cursor", "contains Brainstack context guidance")
          : check("WARN", "guidance", "cursor", `${cursorRule} exists but Brainstack guidance was not detected`, `Append: cat ${join(bootstrapRoot, "cursor-user-rule.md")} >> ${cursorRule}`)
      );
    }
    return checks;
  }

  async function collectDoctorChecks(cfg: BrainstackConfig, args: ParsedArgs): Promise<DoctorCheck[]> {
    const checks: DoctorCheck[] = [];
    const bunRequired = profileRequiresBunRuntime(cfg.profile);
    const bunVersion = installedBunVersion(cfg.runtime.bunBin);
    if (bunRequired) {
      checks.push(
        bunVersion && compareVersions(bunVersion, MIN_BUN_VERSION) >= 0
          ? check("PASS", "versions", "bun", `Bun ${bunVersion}; required >= ${MIN_BUN_VERSION}`)
          : check("FAIL", "versions", "bun", bunVersion ? `Bun ${bunVersion}; required >= ${MIN_BUN_VERSION}` : "Bun runtime missing", installHint("bun"))
      );
    } else {
      checks.push(
        bunVersion && compareVersions(bunVersion, MIN_BUN_VERSION) >= 0
          ? check("PASS", "versions", "bun", `Bun ${bunVersion}; optional for client-macos source workflows`)
          : check("WARN", "versions", "bun", bunVersion ? `Bun ${bunVersion}; optional for client-macos source workflows` : "Bun runtime missing; not required for client-macos standalone binary installs")
      );
    }
    const bunBinAvailable = runtimeCommandAvailable(cfg.runtime.bunBin);
    checks.push(
      bunBinAvailable
        ? check("PASS", "versions", "bun-bin", cfg.runtime.bunBin)
        : bunRequired
          ? check("FAIL", "versions", "bun-bin", `${cfg.runtime.bunBin} does not exist`)
          : check("WARN", "versions", "bun-bin", `${cfg.runtime.bunBin} unavailable; not used by client-macos binary installs`)
    );
    for (const name of ["git", "ssh", "tailscale"]) {
      checks.push(commandOk(name) ? check("PASS", "versions", name, commandVersion(name)) : check("FAIL", "versions", name, "missing", installHint(name)));
    }
    checks.push(harnessCompatibility(cfg.harness.name, cfg.harness.bin));
    if (hasFlag(args, "deep")) {
      const sudo = run(["sudo", "-n", "true"], { check: false });
      checks.push(sudo.code === 0 ? check("PASS", "privileges", "sudo-noninteractive", "sudo -n true works") : check("FAIL", "privileges", "sudo-noninteractive", "sudo -n true failed", "Configure passwordless sudo if this profile requires privileged harness operations."));
      try {
        await testHarnessSudo(cfg.harness);
        checks.push(check("PASS", "privileges", "harness-bypass-sudo", `${cfg.harness.name} proved bypass/yolo sudo execution`));
      } catch (error) {
        checks.push(check("FAIL", "privileges", "harness-bypass-sudo", error instanceof Error ? error.message : String(error), `Verify ${cfg.harness.name} bypass/yolo permissions manually.`));
      }
    }

    checks.push(check("PASS", "config", "schema", `schema_version=${cfg.schemaVersion}`));
    checks.push(existsSync(cfg.paths.configRoot) ? check("PASS", "config", "config-root", cfg.paths.configRoot) : check("WARN", "config", "config-root", `${cfg.paths.configRoot} missing`, "Run brainctl init/apply-runtime after provisioning."));
    checks.push(existsSync(cfg.paths.stateRoot) ? check("PASS", "config", "state-root", cfg.paths.stateRoot) : check("WARN", "config", "state-root", `${cfg.paths.stateRoot} missing`, "Run brainctl init/apply-runtime after provisioning."));
    checks.push(existsSync(managedManifestPath(cfg)) ? check("PASS", "config", "ownership-manifest", managedManifestPath(cfg)) : check("WARN", "config", "ownership-manifest", "missing", "Run brainctl init/apply-runtime so destroy has a deterministic manifest."));
    checks.push(...await harnessGuidanceChecks(cfg));
    const loopbackBind = cfg.security.bindHost === "127.0.0.1" || cfg.security.bindHost === "::1" || cfg.security.bindHost === "localhost";
    const wildcardBind = cfg.security.bindHost === "0.0.0.0" || cfg.security.bindHost === "::";
    if (cfg.security.posture === "local") {
      checks.push(
        loopbackBind
          ? check("PASS", "security", "posture", `local bind=${cfg.security.bindHost}`)
          : check("FAIL", "security", "posture", `local posture must bind loopback, got ${cfg.security.bindHost}`, "Set security.bindHost to 127.0.0.1 or change the posture deliberately.")
      );
    } else if (cfg.security.posture === "trusted-tailnet") {
      checks.push(check("PASS", "security", "read-auth", "disabled by design in trusted-tailnet mode"));
      checks.push(check("PASS", "security", "trust-boundary", "private network reachability"));
      checks.push(check("WARN", "security", "public-exposure", "do not expose trusted-tailnet mode to the public internet", "Bind loopback and expose through Tailscale Serve/VPN, or use guarded/manual controls for broader exposure."));
      checks.push(
        wildcardBind
          ? check("FAIL", "security", "posture", `trusted-tailnet bind=${cfg.security.bindHost}; exposure=${cfg.security.trustedExposure}`, "Wildcard trusted-tailnet binds are too broad for this posture. Set security.bindHost to 127.0.0.1 and expose through Tailscale Serve/VPN, or implement guarded app-layer read controls.")
          : !loopbackBind && (cfg.security.trustedExposure === "manual" || cfg.security.trustedExposure === "vpn")
          ? check("WARN", "security", "posture", `trusted-tailnet bind=${cfg.security.bindHost}; exposure=${cfg.security.trustedExposure}`, "Manual/VPN exposure is explicit; verify the private-network boundary yourself.")
          : !loopbackBind
          ? check("FAIL", "security", "posture", `trusted-tailnet bind=${cfg.security.bindHost}; exposure=${cfg.security.trustedExposure}`, "Set security.bindHost to 127.0.0.1 and expose through Tailscale Serve/VPN, or explicitly choose guarded/manual exposure.")
          : check("PASS", "security", "posture", `trusted-tailnet bind=${cfg.security.bindHost}; exposure=${cfg.security.trustedExposure}`)
      );
      checks.push(
        cfg.security.trustedExposure === "tailscale-serve"
          ? check("PASS", "security", "tailscale-serve-exposure", "configured in brainstack.yaml")
          : check("PASS", "security", "tailscale-serve-exposure", "no Tailscale Serve exposure declared; local-only until exposed")
      );
    } else {
      checks.push(check("WARN", "security", "posture", "guarded posture is reserved; no first-class read-token/IAM enforcement is implemented yet", "Use trusted-tailnet for the current product stance, or add guarded controls before broader exposure."));
    }

    if (runsBraind(cfg)) {
      checks.push(existsSync(join(cfg.paths.configRoot, "braind.runtime.env")) ? check("PASS", "secrets", "braind-runtime-env", "present") : check("WARN", "secrets", "braind-runtime-env", "missing"));
      checks.push(envHasKey(join(cfg.paths.configRoot, "braind.secrets.env"), "BRAIN_IMPORT_TOKEN") ? check("PASS", "secrets", "brain-import-token", "present") : check("WARN", "secrets", "brain-import-token", "missing or empty"));
      checks.push(envHasKey(join(cfg.paths.configRoot, "braind.secrets.env"), "BRAIN_ADMIN_TOKEN") ? check("PASS", "secrets", "brain-admin-token", "present") : check("WARN", "secrets", "brain-admin-token", "missing or empty"));
      checks.push(existsSync(cfg.repos.bare) ? check("PASS", "git", "bare-repo", cfg.repos.bare) : check("WARN", "git", "bare-repo", `${cfg.repos.bare} missing`));
      if (gitExists(cfg.repos.staging)) checks.push(gitClean(cfg.repos.staging)); else checks.push(check("WARN", "git", "staging-clone", `${cfg.repos.staging} missing`));
      if (gitExists(cfg.repos.serve)) checks.push(gitClean(cfg.repos.serve)); else checks.push(check("WARN", "git", "serve-clone", `${cfg.repos.serve} missing`));
      checks.push(await lockDoctorCheck("write-repo-lock", join(cfg.repos.staging, ".shared-brain.lock")));
      checks.push(await lockDoctorCheck("serve-repo-lock", join(cfg.repos.serve, ".shared-brain.lock")));
      checks.push(
        existsSync(pendingReindexPathFor(cfg))
          ? check("WARN", "locks", "pending-reindex", pendingReindexPathFor(cfg), "braind should retry this automatically; inspect braind logs if it persists.")
          : check("PASS", "locks", "pending-reindex", "absent")
      );
      checks.push(braindPortOwnerCheck(cfg));
      try {
        const response = await fetch(`http://${cfg.security.bindHost}:${cfg.brain.port}/healthz`, { signal: AbortSignal.timeout(2000) });
        checks.push(response.ok ? check("PASS", "health", "braind-healthz", `HTTP ${response.status}`) : check("WARN", "health", "braind-healthz", `HTTP ${response.status}`));
      } catch (error) {
        checks.push(check("WARN", "health", "braind-healthz", error instanceof Error ? error.message : String(error), "Start braind or ignore before runtime is installed."));
      }
      try {
        const response = await fetch(`http://${cfg.security.bindHost}:${cfg.brain.port}/readyz`, { signal: AbortSignal.timeout(2000) });
        let detail = `HTTP ${response.status}`;
        try {
          const body = (await response.json()) as Record<string, unknown>;
          if (typeof body.search_ready === "boolean") {
            detail += ` search_ready=${body.search_ready}`;
          }
          const pendingReindex = body.pending_reindex && typeof body.pending_reindex === "object" ? (body.pending_reindex as Record<string, unknown>) : undefined;
          if (pendingReindex && typeof pendingReindex.present === "boolean") {
            detail += ` pending_reindex=${pendingReindex.present}`;
          }
        } catch {
          // Non-JSON readiness responses still carry useful HTTP status.
        }
        checks.push(
          response.ok
            ? check("PASS", "health", "braind-readyz", detail)
            : check("WARN", "health", "braind-readyz", detail, "Search or write-refresh may still be rebuilding; inspect /readyz and braind logs if this persists.")
        );
      } catch (error) {
        checks.push(check("WARN", "health", "braind-readyz", error instanceof Error ? error.message : String(error), "Start braind or ignore before runtime is installed."));
      }
      const adminToken = process.env.BRAIN_ADMIN_TOKEN || readEnvFile(join(cfg.paths.configRoot, "braind.secrets.env")).BRAIN_ADMIN_TOKEN || "";
      if (adminToken) {
        try {
          const response = await fetch(`http://${cfg.security.bindHost}:${cfg.brain.port}/admin/health`, {
            headers: { Authorization: `Bearer ${adminToken}` },
            signal: AbortSignal.timeout(2000)
          });
          checks.push(response.ok ? check("PASS", "health", "braind-admin-health", `HTTP ${response.status}`) : check("WARN", "health", "braind-admin-health", `HTTP ${response.status}`));
        } catch (error) {
          checks.push(check("WARN", "health", "braind-admin-health", error instanceof Error ? error.message : String(error)));
        }
      } else {
        checks.push(check("WARN", "health", "braind-admin-health", "admin token not available locally; skipped deep health"));
      }
    }

    if (cfg.telemux.enabled) {
      checks.push(envHasKey(join(cfg.paths.configRoot, "telemux.secrets.env"), "FACTORY_TELEGRAM_BOT_TOKEN") ? check("PASS", "secrets", "telegram-token", "present") : check("WARN", "secrets", "telegram-token", "missing or empty"));
      const telemuxServicePath = join(cfg.paths.systemdUserRoot, "telemux.service");
      const telemuxServiceText = existsSync(telemuxServicePath) ? readFileSync(telemuxServicePath, "utf8") : "";
      const telemuxHasFactoryAdmin = envHasKey(join(cfg.paths.configRoot, "telemux.secrets.env"), "FACTORY_BRAIN_ADMIN_TOKEN");
      const telemuxHasBrainAdmin = envHasKey(join(cfg.paths.configRoot, "braind.secrets.env"), "BRAIN_ADMIN_TOKEN") && telemuxServiceText.includes("braind.secrets.env");
      checks.push(
        telemuxHasFactoryAdmin || telemuxHasBrainAdmin
          ? check("PASS", "secrets", "telemux-admin-token", telemuxHasFactoryAdmin ? "FACTORY_BRAIN_ADMIN_TOKEN present" : "BRAIN_ADMIN_TOKEN loaded from braind.secrets.env")
          : check("WARN", "secrets", "telemux-admin-token", "missing; curator status reporting and Telegram proposal approval are disabled", `Run brainctl apply-runtime --config ${requireFlagValue(args, "config") || brainstackDefaultConfigPath()} and restart telemux, or set FACTORY_BRAIN_ADMIN_TOKEN in telemux.secrets.env.`)
      );
      try {
        const response = await fetch(`http://${cfg.telemux.dashboardHost}:${cfg.telemux.dashboardPort}/health`, { signal: AbortSignal.timeout(1500) });
        checks.push(response.ok ? check("PASS", "health", "telemux-health", `HTTP ${response.status}`) : check("WARN", "health", "telemux-health", `HTTP ${response.status}`));
      } catch (error) {
        checks.push(check("WARN", "health", "telemux-health", error instanceof Error ? error.message : String(error)));
      }
      try {
        const response = await fetch(`http://${cfg.telemux.dashboardHost}:${cfg.telemux.dashboardPort}/healthz`, { signal: AbortSignal.timeout(1500) });
        if (response.ok) {
          const body = (await response.json()) as Record<string, unknown>;
          const crons = Number(body.crons ?? 0);
          const enabled = Number(body.cronEnabled ?? 0);
          const pending = Number(body.cronPending ?? 0);
          const due = Number(body.cronDue ?? 0);
          const queuedTurns = body.queuedTurns && typeof body.queuedTurns === "object" ? (body.queuedTurns as Record<string, unknown>) : {};
          const runningTurns = Number(queuedTurns.running ?? 0);
          const abandonedTurns = Number(queuedTurns.abandoned ?? 0);
          const pendingText = body.pendingText && typeof body.pendingText === "object" ? (body.pendingText as Record<string, unknown>) : {};
          const pendingTextCount = Number(pendingText.count ?? 0);
          const pendingTextOldestAgeMs = pendingText.oldestAgeMs === null || pendingText.oldestAgeMs === undefined ? 0 : Number(pendingText.oldestAgeMs);
          const workerDegraded = Number(body.workerDegraded ?? 0);
          checks.push(
            due > 0 || pending > 0
              ? check("WARN", "health", "telemux-crons", `total=${crons} enabled=${enabled} due=${due} pending=${pending}`, "Inspect /crons and telemux logs if due or pending jobs do not clear after the scheduler interval.")
              : check("PASS", "health", "telemux-crons", `total=${crons} enabled=${enabled} due=${due} pending=${pending}`)
          );
          checks.push(
            runningTurns > 0 || abandonedTurns > 0 || pendingTextOldestAgeMs > 5 * 60_000
              ? check("WARN", "health", "telemux-durable-work", `queued_running=${runningTurns} abandoned=${abandonedTurns} pending_text=${pendingTextCount} oldest_pending_text_ms=${pendingTextOldestAgeMs}`, "Inspect telemux logs and Telegram topics for interrupted queued turns or stale coalesced input.")
              : check("PASS", "health", "telemux-durable-work", `queued_running=${runningTurns} abandoned=${abandonedTurns} pending_text=${pendingTextCount}`)
          );
          checks.push(
            workerDegraded > 0
              ? check("WARN", "health", "telemux-worker-health", `degraded=${workerDegraded}`, "Run /health or brainctl doctor --workers for worker details.")
              : check("PASS", "health", "telemux-worker-health", "degraded=0")
          );
        } else {
          checks.push(check("WARN", "health", "telemux-crons", `healthz HTTP ${response.status}`));
        }
      } catch (error) {
        checks.push(check("WARN", "health", "telemux-crons", error instanceof Error ? error.message : String(error)));
      }
    }

    if (usesUserServices(cfg) && commandOk("loginctl")) {
      const linger = run(["loginctl", "show-user", cfg.machine.user, "--property=Linger", "--value"], { check: false });
      checks.push(
        linger.stdout.trim() === "yes"
          ? check("PASS", "services", "user-service-linger", `linger enabled for ${cfg.machine.user}`)
          : check("WARN", "services", "user-service-linger", `linger disabled for ${cfg.machine.user}`, `sudo loginctl enable-linger ${cfg.machine.user}`)
      );
    }

    const tailscaleBin = commandPath("tailscale");
    if (tailscaleBin) {
      const status = run([tailscaleBin, "status", "--json"], { check: false, timeoutMs: updateProbeTimeoutMs() });
      checks.push(status.code === 0 ? check("PASS", "tailscale", "status", "tailscale status --json succeeded") : check("WARN", "tailscale", "status", (status.stderr || status.stdout).trim()));
      const serve = run([tailscaleBin, "serve", "status"], { check: false, timeoutMs: updateProbeTimeoutMs() });
      checks.push(serve.code === 0 ? check("PASS", "tailscale", "serve", "serve status available") : check("WARN", "tailscale", "serve", (serve.stderr || serve.stdout).trim()));
    }

    const outboxScans = await scanAllOutboxes(cfg);
    const outboxItems = outboxScans.flatMap((scan) => scan.items);
    const outboxCorrupt = outboxScans.flatMap((scan) => scan.corrupt);
    checks.push(check("PASS", "outbox", "queued-items", `${outboxItems.length} queued item(s)`));
    checks.push(
      outboxCorrupt.length
        ? check("FAIL", "outbox", "corrupt-items", `${outboxCorrupt.length} corrupt/unsafe item(s)`, "Run `brainctl outbox list` and then `brainctl outbox purge-corrupt --yes` only if those drafts are unrecoverable.")
        : check("PASS", "outbox", "corrupt-items", "none")
    );
    const unsafeOutboxDirs: string[] = [];
    for (const dir of [outboxParentRoot(cfg), ...outboxScans.map((scan) => scan.root)]) {
      const info = await lstat(dir).catch(() => null);
      if (!info) {
        continue;
      }
      if (info.isSymbolicLink()) {
        unsafeOutboxDirs.push(`${dir}:symlink`);
        continue;
      }
      const mode = info.mode & 0o777;
      if ((mode & 0o077) !== 0) {
        unsafeOutboxDirs.push(`${dir}:${mode.toString(8)}`);
      }
    }
    checks.push(
      unsafeOutboxDirs.length
        ? check("WARN", "outbox", "permissions", unsafeOutboxDirs.join(", "), `chmod 700 ${outboxParentRoot(cfg)} and each namespace directory after review.`)
        : check("PASS", "outbox", "permissions", "outbox directories are restrictive")
    );
    const unsafeFiles = [];
    for (const entry of [...outboxItems.map((item) => ({ path: item.path, label: item.item.id })), ...outboxCorrupt.map((item) => ({ path: item.path, label: `CORRUPT:${item.name}` }))]) {
      const info = await lstat(entry.path).catch(() => null);
      if (!info) {
        continue;
      }
      if (info.isSymbolicLink()) {
        unsafeFiles.push(`${entry.label}:symlink`);
        continue;
      }
      const fileMode = info.mode & 0o777;
      if ((fileMode & 0o077) !== 0) {
        unsafeFiles.push(`${entry.label}:${fileMode.toString(8)}`);
      }
    }
    checks.push(
      unsafeFiles.length
        ? check("FAIL", "outbox", "file-permissions", unsafeFiles.join(", "), "Run `chmod 600 ~/.local/state/brainstack/outbox/*/*.json` or purge corrupt/temp entries after review.")
        : check("PASS", "outbox", "file-permissions", "queued and corrupt item files are private")
    );
    checks.push(
      outboxItems.length
        ? check("WARN", "outbox", "plaintext-posture", "queued payloads are stored locally and protected by filesystem permissions, not encrypted by brainstack", "Use disk encryption and keep the state root private; see docs/outbox-security.md.")
        : check("PASS", "outbox", "plaintext-posture", "no queued payloads")
    );

    if (cfg.profile === "worker" || cfg.profile === "client-macos") {
      const localClone = clientLocalPathAbs(cfg);
      checks.push(
        gitExists(localClone)
          ? check("PASS", "client", "shared-brain-clone", localClone)
          : check("WARN", "client", "shared-brain-clone", `${localClone} missing or not a git clone`, "Run brainctl init for this profile, or run the rendered client bootstrap installer.")
      );
      const envPath = clientEnvPathAbs(cfg);
      checks.push(
        existsSync(envPath)
          ? check("PASS", "client", "shared-brain-env", envPath)
          : check("WARN", "client", "shared-brain-env", `${envPath} missing`, "Run brainctl init or the rendered client bootstrap installer.")
      );
      const writeConfig = brainWriteConfig(cfg);
      checks.push(
        writeConfig.baseUrl
          ? check("PASS", "client", "brain-base-url", "configured")
          : check("WARN", "client", "brain-base-url", "missing", "Set BRAIN_BASE_URL in env or shared-brain.env.")
      );
      checks.push(
        writeConfig.token
          ? check("PASS", "client", "brain-import-token", "present")
          : check("WARN", "client", "brain-import-token", "missing or empty", "Pass BRAIN_IMPORT_TOKEN or BRAIN_IMPORT_TOKEN_FILE during install/init, or edit shared-brain.env.")
      );
      const daemonService = await daemonServiceStatus(cfg, args);
      const daemonStatus = await readDaemonStatus(cfg);
      checks.push(
        daemonService.installed
          ? check("PASS", "daemon", "service-installed", `${daemonService.platform} ${daemonService.path}`)
          : check("WARN", "daemon", "service-installed", `${daemonService.platform} service missing`, `Run brainctl daemon install --config ${requireFlagValue(args, "config") || brainstackDefaultConfigPath()}`)
      );
      checks.push(
        daemonService.running === true
          ? check("PASS", "daemon", "service-running", daemonService.detail)
          : daemonService.running === false
            ? check("WARN", "daemon", "service-running", daemonService.detail, "Start the daemon service or run brainctl daemon once for a foreground health pass.")
            : check("WARN", "daemon", "service-running", daemonService.detail)
      );
      if (daemonStatus) {
        const ageMs = Date.now() - Date.parse(daemonStatus.updated_at || "");
        checks.push(
          daemonStatus.ok
            ? check("PASS", "daemon", "last-run", `updated=${daemonStatus.updated_at} age_ms=${Number.isFinite(ageMs) ? ageMs : "unknown"} iteration=${daemonStatus.iteration}`)
            : check("WARN", "daemon", "last-run", `degraded: ${daemonStatus.errors.join("; ") || daemonStatus.updated_at}`, "Inspect brainctl daemon status --json and daemon logs.")
        );
        checks.push(
          daemonStatus.repo.exists && daemonStatus.repo.clean !== false
            ? check("PASS", "daemon", "shared-brain-freshness", `head=${daemonStatus.repo.head || "unknown"} last_pull=${daemonStatus.repo.last_pull_at || "never"}`)
            : check("WARN", "daemon", "shared-brain-freshness", `repo=${daemonStatus.repo.path} clean=${daemonStatus.repo.clean ?? "unknown"}`, "Resolve local clone state so the daemon can pull with --ff-only.")
        );
      } else {
        checks.push(check("WARN", "daemon", "last-run", `missing ${daemonStatusPath(cfg)}`, "Run brainctl daemon once --config <config> or start the installed daemon."));
      }
    }

    if (hasFlag(args, "write-smoke")) {
      checks.push(await brainWriteSmokeCheck(cfg));
    }

    if (hasFlag(args, "workers") || hasFlag(args, "deep")) {
      checks.push(...(await workerDoctorChecks(cfg, hasFlag(args, "deep"))));
    }

    return checks;
  }

  function formatDoctorChecks(checks: DoctorCheck[]): string {
    return checks
      .map((item) => `${item.status} [${item.section}] ${item.name}: ${item.detail}${item.remediation ? `\n  remediation: ${item.remediation}` : ""}`)
      .join("\n");
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

  function workerEnvCachePath(cfg: BrainstackConfig): string {
    return join(cfg.paths.stateRoot, "worker-env-cache.json");
  }

  function workerEnvFingerprint(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, family?: HarnessName, bin?: string): string {
    const resolvedFamily = family || workerHarnessFamily(cfg, worker);
    const resolvedBin = bin || workerHarnessBin(cfg, worker, resolvedFamily);
    return sha256Hex(canonicalJson({
      worker: worker.name,
      transport: worker.transport,
      sshTarget: worker.sshTarget || null,
      sshUser: worker.sshUser || null,
      harness: resolvedFamily,
      harnessBin: resolvedBin
    }));
  }

  function readWorkerEnvCache(cfg: BrainstackConfig): Record<string, WorkerEnvCacheRecord> {
    const path = workerEnvCachePath(cfg);
    if (!existsSync(path)) {
      return {};
    }
    try {
      return JSON.parse(readFileSync(path, "utf8")) as Record<string, WorkerEnvCacheRecord>;
    } catch {
      return {};
    }
  }

  async function writeWorkerEnvCache(cfg: BrainstackConfig, cache: Record<string, WorkerEnvCacheRecord>): Promise<void> {
    await ensureDir(dirname(workerEnvCachePath(cfg)));
    await writeText(workerEnvCachePath(cfg), `${JSON.stringify(cache, null, 2)}\n`, 0o600);
  }

  function cachedWorkerPath(cfg: BrainstackConfig, worker: BrainstackWorkerConfig): string | null {
    const record = readWorkerEnvCache(cfg)[worker.name];
    const family = workerHarnessFamily(cfg, worker);
    const bin = workerHarnessBin(cfg, worker, family);
    if (!record || record.fingerprint !== workerEnvFingerprint(cfg, worker, family, bin) || !record.path.trim()) {
      return null;
    }
    const detectedAt = Date.parse(record.detectedAt);
    if (!Number.isFinite(detectedAt) || Date.now() - detectedAt > 7 * 24 * 60 * 60 * 1000) {
      return null;
    }
    return record.path;
  }

  function workerHarnessFamily(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, contextOverride?: HarnessName | null): HarnessName {
    return contextOverride || worker.harness || cfg.harness.name;
  }

  function workerHarnessBin(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, family: HarnessName, contextBin?: string | null): string {
    if (contextBin?.trim()) return contextBin.trim();
    if (worker.harnessBin?.trim()) return worker.harnessBin.trim();
    if (worker.transport === "local" && family === cfg.harness.name) return cfg.harness.bin;
    return family;
  }

  function workerRemoteTarget(worker: BrainstackWorkerConfig): string {
    const host = workerSshHost(worker);
    const remoteHost = host.includes(":") && !host.startsWith("[") ? `[${host}]` : host;
    const embeddedUser = workerSshEmbeddedUser(worker);
    const user = worker.sshUser || embeddedUser;
    return user ? `${user}@${remoteHost}` : remoteHost;
  }

  function workerSshEmbeddedUser(worker: BrainstackWorkerConfig): string | null {
    const target = worker.sshTarget || worker.name;
    return target.includes("@") ? target.slice(0, target.lastIndexOf("@")) : null;
  }

  function workerSshHost(worker: BrainstackWorkerConfig): string {
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

  function workerSshPort(worker: BrainstackWorkerConfig): string | null {
    const target = worker.sshTarget || worker.name;
    const withoutUser = target.includes("@") ? target.slice(target.lastIndexOf("@") + 1) : target;
    const bracketMatch = withoutUser.match(/^\[[^\]]+\]:(\d+)$/);
    if (bracketMatch) {
      return bracketMatch[1];
    }
    const hostPort = withoutUser.match(/^[^:]+:(\d+)$/);
    return hostPort ? hostPort[1] : null;
  }

  function workerSshKnownHostsLookup(worker: BrainstackWorkerConfig): string {
    const host = workerSshHost(worker);
    const port = workerSshPort(worker);
    return port ? `[${host}]:${port}` : host;
  }

  function workerSshKnownHostsPath(cfg: BrainstackConfig, worker: BrainstackWorkerConfig): string {
    return absWithHome(worker.sshKnownHostsPath || join(cfg.paths.configRoot, "ssh_known_hosts"), cfg.paths.home);
  }

  function workerSshTrustMode(worker: BrainstackWorkerConfig): WorkerSshTrustMode {
    return worker.sshTrustMode === "accept-new" ? "accept-new" : "pinned";
  }

  function workerSshTrustArgs(cfg: BrainstackConfig, worker: BrainstackWorkerConfig): string[] {
    const mode = workerSshTrustMode(worker);
    return [
      "-o",
      mode === "accept-new" ? "StrictHostKeyChecking=accept-new" : "StrictHostKeyChecking=yes",
      "-o",
      `UserKnownHostsFile=${workerSshKnownHostsPath(cfg, worker)}`
    ];
  }

  function workerSshPortArgs(worker: BrainstackWorkerConfig): string[] {
    const port = workerSshPort(worker);
    return port ? ["-p", port] : [];
  }

  function workerUserPathPrelude(): string {
    return `
  # Worker tool and harness commands are resolved through the target user's own
  # shell PATH so user-managed Bun/Codex/Claude installs work over non-login SSH.
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

  function runWorkerShell(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, script: string, timeoutSeconds = 10, usePathCache = true) {
    const family = workerHarnessFamily(cfg, worker);
    const bin = workerHarnessBin(cfg, worker, family);
    const cachedPath = usePathCache ? cachedWorkerPath(cfg, worker) : null;
    const cachePrelude = cachedPath ? `BRAINSTACK_WORKER_PATH=${quoteForBash(cachedPath)}\nexport BRAINSTACK_WORKER_PATH\n` : "";
    const uncachedPrelude = usePathCache ? "" : "unset BRAINSTACK_WORKER_PATH\n";
    const harnessPrelude = `harness=${quoteForBash(family)}\nharness_family=${quoteForBash(family)}\nharness_bin=${quoteForBash(bin)}\n`;
    const wrappedScript = `${uncachedPrelude}${cachePrelude}${harnessPrelude}${workerUserPathPrelude()}\n${script}`;
    if (worker.transport === "local") {
      return run(["bash", "-lc", wrappedScript], { check: false, timeoutMs: timeoutSeconds * 1000 });
    }
    const sshArgs =
      worker.transport === "tailscale-ssh"
        ? ["tailscale", "ssh", workerRemoteTarget(worker), "bash", "-lc", wrappedScript]
        : [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            ...workerSshTrustArgs(cfg, worker),
            ...workerSshPortArgs(worker),
            workerRemoteTarget(worker),
            "bash",
            "-lc",
            wrappedScript
          ];
    const timeoutBin = Bun.which("timeout");
    return run(timeoutBin ? [timeoutBin, `${timeoutSeconds}s`, ...sshArgs] : sshArgs, { check: false });
  }

  const WORKER_REQUIRED_COMMANDS = ["bun", "git", "ssh", "tailscale"];

  function regexEscape(value: string): string {
    return value.replace(/[\\^$.*+?()[\]{}|]/g, "\\$&");
  }

  function workerProbeValue(output: string, key: string): string | null {
    const match = output.match(new RegExp(`^${regexEscape(key)}=(.*)$`, "m"));
    return match?.[1]?.trim() || null;
  }

  function workerCommandCheck(worker: BrainstackWorkerConfig, output: string, commandName: string): DoctorCheck {
    const path = workerProbeValue(output, `cmd:${commandName}`);
    if (!path || path === "missing") {
      return check(
        "FAIL",
        "workers",
        `worker:${worker.name}:cmd:${commandName}`,
        "missing from worker shell PATH",
        `Install ${commandName} on ${worker.name}, or add its bin directory to the worker user's login shell PATH.`
      );
    }

    const version = workerProbeValue(output, `cmdver:${commandName}`);
    return check("PASS", "workers", `worker:${worker.name}:cmd:${commandName}`, version ? `${path}; ${version}` : path);
  }

  function workerSshTrustCheck(cfg: BrainstackConfig, worker: BrainstackWorkerConfig): DoctorCheck | null {
    if (worker.transport !== "ssh") {
      return null;
    }
    const mode = workerSshTrustMode(worker);
    if (mode === "accept-new") {
      const bootstrapAllowed = truthyEnv(process.env.BRAINSTACK_ALLOW_ACCEPT_NEW_DOCTOR);
      return check(
        bootstrapAllowed ? "WARN" : "FAIL",
        "workers",
        `worker:${worker.name}:ssh-trust`,
        "bootstrap trust mode accept-new",
        bootstrapAllowed
          ? `Bootstrap probing is explicitly enabled by BRAINSTACK_ALLOW_ACCEPT_NEW_DOCTOR. Run brainctl trust-worker --config <config> --worker ${worker.name} and switch the worker to sshTrustMode: pinned after enrollment.`
          : `Refusing remote doctor probes under TOFU. Run brainctl trust-worker --config <config> --worker ${worker.name}, switch the worker to sshTrustMode: pinned, or set BRAINSTACK_ALLOW_ACCEPT_NEW_DOCTOR=true for one bootstrap probe.`
      );
    }
    const knownHostsPath = workerSshKnownHostsPath(cfg, worker);
    if (!existsSync(knownHostsPath)) {
      return check(
        "FAIL",
        "workers",
        `worker:${worker.name}:ssh-trust`,
        `pinned known_hosts file missing: ${knownHostsPath}`,
        `Run brainctl trust-worker --config <config> --worker ${worker.name} before dispatching to this worker.`
      );
    }
    const sshKeygen = Bun.which("ssh-keygen");
    if (!sshKeygen) {
      return check("WARN", "workers", `worker:${worker.name}:ssh-trust`, "ssh-keygen missing; cannot verify pinned host entry");
    }
    const host = workerSshKnownHostsLookup(worker);
    const found = run([sshKeygen, "-F", host, "-f", knownHostsPath], { check: false });
    return found.code === 0
      ? check("PASS", "workers", `worker:${worker.name}:ssh-trust`, `pinned host key present for ${host}`)
      : check(
          "FAIL",
          "workers",
          `worker:${worker.name}:ssh-trust`,
          `no pinned host key for ${host} in ${knownHostsPath}`,
          `Run brainctl trust-worker --config <config> --worker ${worker.name}, then rerun doctor.`
        );
  }

  async function workerDoctorChecks(cfg: BrainstackConfig, deep: boolean): Promise<DoctorCheck[]> {
    const checks: DoctorCheck[] = [];
    for (const worker of defaultWorkers(cfg)) {
      const trustCheck = workerSshTrustCheck(cfg, worker);
      if (trustCheck) {
        checks.push(trustCheck);
        if (trustCheck.status === "FAIL" && worker.transport === "ssh" && workerSshTrustMode(worker) === "accept-new") {
          continue;
        }
      }
      const family = workerHarnessFamily(cfg, worker);
      const bin = workerHarnessBin(cfg, worker, family);
      const required =
        family === "codex"
          ? ["--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check", "--output-last-message"]
          : ["--dangerously-skip-permissions", "--permission-mode", "--output-format"];
      const script = [
        "set -euo pipefail",
        `harness_bin=${quoteForBash(bin)}`,
        `harness_family=${quoteForBash(family)}`,
        "printf 'worker=%s\\n' \"$(hostname)\"",
        "printf 'path=%s\\n' \"$PATH\"",
        "if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then printf 'sudo=ok\\n'; else printf 'sudo=fail\\n'; fi",
        "probe_command() {",
        "  name=\"$1\"",
        "  if ! command -v \"$name\" >/dev/null 2>&1; then printf 'cmd:%s=missing\\n' \"$name\"; return 0; fi",
        "  path=\"$(command -v \"$name\")\"",
        "  printf 'cmd:%s=%s\\n' \"$name\" \"$path\"",
        "  case \"$name\" in",
        "    ssh) version=\"$($path -V 2>&1 | head -n 1 || true)\" ;;",
        "    *) version=\"$($path --version 2>&1 | head -n 1 || true)\" ;;",
        "  esac",
        "  printf 'cmdver:%s=%s\\n' \"$name\" \"$version\"",
        "}",
        ...WORKER_REQUIRED_COMMANDS.map((name) => `probe_command ${quoteForBash(name)}`),
        "if ! command -v \"$harness_bin\" >/dev/null 2>&1; then printf 'harness_bin=missing\\n'; exit 7; fi",
        "printf 'harness_bin=%s\\n' \"$(command -v \"$harness_bin\")\"",
        "\"$harness_bin\" --version 2>&1 | head -n 1 | sed 's/^/version=/' || true",
        "if [ \"$harness_family\" = codex ]; then help=\"$($harness_bin exec --help 2>&1 || true)\"; else help=\"$($harness_bin --help 2>&1 || true)\"; fi",
        ...required.map((needle) => `case "$help" in *${quoteForBash(needle)}*) printf 'flag:${needle}=ok\\n' ;; *) printf 'flag:${needle}=missing\\n' ;; esac`),
        "brainstack_config_value() { key=\"$1\"; file=\"$2\"; [ -f \"$file\" ] || return 0; awk -v key=\"$key\" 'BEGIN { in_section=0 } /^[[:space:]]*\\[/ { in_section=1 } in_section == 0 { pattern=\"^[[:space:]]*\" key \"[[:space:]]*=\"; if ($0 ~ pattern) { sub(/^[^=]*=[[:space:]]*/, \"\"); sub(/[[:space:]]*#.*/, \"\"); gsub(/^[[:space:]\\\"]+|[[:space:]\\\"]+$/, \"\"); print; exit } }' \"$file\" || true; }",
        "if [ \"$harness_family\" = codex ]; then codex_config=\"${CODEX_HOME:-$HOME/.codex}/config.toml\"; model_config=\"$(brainstack_config_value model \"$codex_config\")\"; effort_config=\"$(brainstack_config_value model_reasoning_effort \"$codex_config\")\"; printf 'model=%s\\n' \"${model_config:-default}\"; printf 'effort=%s\\n' \"${effort_config:-default}\"; else printf 'model=default\\n'; printf 'effort=n/a\\n'; fi",
        deep
          ? [
              "tmpdir=\"$(mktemp -d)\"",
              "trap 'rm -rf \"$tmpdir\"' EXIT",
              "cd \"$tmpdir\"",
              "prompt='Run exactly this shell command and then stop: sudo -n true && printf BRAINSTACK_HARNESS_SUDO_OK'",
              "if [ \"$harness_family\" = codex ]; then",
              "  output=\"$(printf '%s' \"$prompt\" | \"$harness_bin\" exec --skip-git-repo-check --dangerously-bypass-approvals-and-sandbox - 2>&1 || true)\"",
              "else",
              "  output=\"$(printf '%s' \"$prompt\" | \"$harness_bin\" -p --dangerously-skip-permissions --permission-mode bypassPermissions --output-format text 2>&1 || true)\"",
              "fi",
              "case \"$output\" in *BRAINSTACK_HARNESS_SUDO_OK*) printf 'harness_sudo=ok\\n' ;; *) printf 'harness_sudo=fail\\n' ;; esac"
            ].join("\n")
          : "printf 'deep=skipped\\n'"
      ].join("\n");
      const result = runWorkerShell(cfg, worker, script, deep ? 30 : 12, false);
      const combined = `${result.stdout}\n${result.stderr}`.trim();
      if (result.code !== 0) {
        checks.push(check("FAIL", "workers", `worker:${worker.name}`, combined || `exit ${result.code}`, `Verify SSH and ${family} on the worker.`));
        continue;
      }
      const harnessPath = workerProbeValue(combined, "harness_bin") || bin;
      const detectedPath = workerProbeValue(combined, "path");
      if (detectedPath) {
        const cache = readWorkerEnvCache(cfg);
        cache[worker.name] = {
          worker: worker.name,
          fingerprint: workerEnvFingerprint(cfg, worker, family, bin),
          path: detectedPath,
          harness: family,
          harnessBin: harnessPath,
          harnessVersion: workerProbeValue(combined, "version") || "",
          detectedAt: new Date().toISOString()
        };
        await writeWorkerEnvCache(cfg, cache);
      }
      const model = workerProbeValue(combined, "model") || "default";
      const effort = workerProbeValue(combined, "effort") || (family === "claude" ? "n/a" : "default");
      checks.push(check("PASS", "workers", `worker:${worker.name}`, `reachable via ${worker.transport}; harness=${family} bin=${harnessPath} model=${model} effort=${effort}`));
      for (const commandName of WORKER_REQUIRED_COMMANDS) {
        checks.push(workerCommandCheck(worker, combined, commandName));
      }
      checks.push(combined.includes("sudo=ok") ? check("PASS", "workers", `worker:${worker.name}:sudo`, "sudo -n true works") : check("WARN", "workers", `worker:${worker.name}:sudo`, "sudo -n true failed", "Configure passwordless sudo only if this worker profile needs privileged operations."));
      const missingFlags = required.filter((needle) => combined.includes(`flag:${needle}=missing`));
      checks.push(
        missingFlags.length
          ? check("FAIL", "workers", `worker:${worker.name}:harness-compat`, `missing ${missingFlags.join(", ")}`, `Update ${family} on ${worker.name}.`)
          : check("PASS", "workers", `worker:${worker.name}:harness-compat`, `${family} required CLI surface present`)
      );
      if (deep) {
        checks.push(
          combined.includes("harness_sudo=ok")
            ? check("PASS", "workers", `worker:${worker.name}:harness-sudo`, `${family} proved sudo in bypass/yolo mode`)
            : check("FAIL", "workers", `worker:${worker.name}:harness-sudo`, `${family} did not prove sudo in bypass/yolo mode`, `Inspect ${family} auth and permission bypass settings on ${worker.name}.`)
        );
      }
    }
    return checks;
  }

  function quoteForBash(value: string): string {
    return shellSingleQuote(value);
  }

  function listeningTcpPids(port: number): number[] {
    const pids = new Set<number>();
    const lsof = Bun.which("lsof");
    if (lsof) {
      const result = run([lsof, "-nP", `-iTCP:${port}`, "-sTCP:LISTEN", "-t"], { check: false, timeoutMs: 2000 });
      for (const line of result.stdout.split(/\r?\n/)) {
        const pid = Number(line.trim());
        if (Number.isSafeInteger(pid) && pid > 0) {
          pids.add(pid);
        }
      }
    }
    const ss = Bun.which("ss");
    if (!pids.size && ss) {
      const result = run([ss, "-ltnp", `sport = :${port}`], { check: false, timeoutMs: 2000 });
      for (const match of result.stdout.matchAll(/pid=(\d+)/g)) {
        const pid = Number(match[1]);
        if (Number.isSafeInteger(pid) && pid > 0) {
          pids.add(pid);
        }
      }
    }
    return [...pids].sort((left, right) => left - right);
  }

  function systemdUserMainPid(serviceName: string): number | null {
    if (!commandPath("systemctl")) {
      return null;
    }
    const result = run(["systemctl", "--user", "show", serviceName, "--property=MainPID", "--value"], { check: false, timeoutMs: 2000 });
    const pid = Number(result.stdout.trim());
    return result.code === 0 && Number.isSafeInteger(pid) && pid > 0 ? pid : null;
  }

  function systemdUserServiceActive(serviceName: string): boolean | null {
    if (!commandPath("systemctl")) {
      return null;
    }
    const result = run(["systemctl", "--user", "is-active", "--quiet", serviceName], { check: false, timeoutMs: 2000 });
    return result.code === 0;
  }

  function braindPortOwnerCheck(cfg: BrainstackConfig): DoctorCheck {
    if (process.platform !== "linux" || !commandPath("systemctl")) {
      return check("PASS", "health", "braind-port-owner", "systemd unavailable; skipped managed port-owner comparison");
    }
    const pids = listeningTcpPids(cfg.brain.port);
    const active = systemdUserServiceActive("braind.service");
    const mainPid = systemdUserMainPid("braind.service");
    const ownerDetails = pids.map((pid) => `${pid}:${processCommandLine(pid) || "command unavailable"}`).join("; ");
    if (!pids.length) {
      return check("PASS", "health", "braind-port-owner", `no listener on ${cfg.brain.port} outside health checks`);
    }
    if (active === true && mainPid && pids.includes(mainPid)) {
      return check("PASS", "health", "braind-port-owner", `systemd owns port ${cfg.brain.port} pid=${mainPid}`);
    }
    const unmanaged = active === true && mainPid ? `systemd MainPID=${mainPid}, listener(s)=${ownerDetails}` : `listener(s)=${ownerDetails}`;
    if (active === null) {
      return check("WARN", "health", "braind-port-owner", `cannot compare listener to systemd; ${unmanaged}`);
    }
    return check(
      "FAIL",
      "health",
      "braind-port-owner",
      `unmanaged process owns brain port ${cfg.brain.port}: ${unmanaged}`,
      "Stop the orphan listener, then restart the managed service with `systemctl --user restart braind.service`."
    );
  }

  async function commandDoctor(args: ParsedArgs): Promise<void> {
    const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
    const checks = await collectDoctorChecks(cfg, args);
    if (hasFlag(args, "json")) {
      console.log(JSON.stringify({ ok: !checks.some((item) => item.status === "FAIL"), checks }, null, 2));
    } else {
      console.log(formatDoctorChecks(checks));
    }
    const failures = checks.filter((item) => item.status === "FAIL");
    if (failures.length) {
      throw new Error(`doctor failed: ${failures.map((item) => item.name).join(", ")}`);
    }
  }

  function statusTimeoutMs(args: ParsedArgs): number {
    const raw = requireFlagValue(args, "timeout-ms") || process.env.BRAINSTACK_STATUS_TIMEOUT_MS || "";
    if (!raw) {
      return 1500;
    }
    const parsed = Number(raw);
    if (!Number.isSafeInteger(parsed) || parsed <= 0) {
      throw new Error("--timeout-ms must be a positive integer");
    }
    return Math.min(Math.max(parsed, 100), 30_000);
  }

  function sanitizeStatusError(error: unknown): string {
    return (error instanceof Error ? error.message : String(error)).replace(/\s+/g, " ").slice(0, 500);
  }

  function statusSection<T>(
    state: BrainctlStatusState,
    detail: string,
    data?: T,
    options: { available?: boolean; error?: string; durationMs?: number } = {}
  ): BrainctlStatusSection<T> {
    const available = options.available ?? (state === "ok" || state === "warn");
    return {
      state,
      ok: state === "disabled" ? null : state === "ok",
      available,
      detail,
      ...(data === undefined ? {} : { data }),
      ...(options.error ? { error: options.error } : {}),
      ...(options.durationMs === undefined ? {} : { duration_ms: options.durationMs })
    };
  }

  async function collectStatusSection<T>(
    collector: () => Promise<BrainctlStatusSection<T>>,
    timeoutMs: number,
    fallbackState: BrainctlStatusState = "warn"
  ): Promise<BrainctlStatusSection<T>> {
    const started = Date.now();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const timed = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`timed out after ${timeoutMs}ms`)), timeoutMs);
      });
      const section = await Promise.race([collector(), timed]);
      return { ...section, duration_ms: Date.now() - started };
    } catch (error) {
      return statusSection(fallbackState, "unavailable", undefined, {
        available: false,
        error: sanitizeStatusError(error),
        durationMs: Date.now() - started
      });
    } finally {
      if (timer) {
        clearTimeout(timer);
      }
    }
  }

  function finalizeBrainctlStatus(report: BrainctlStatusReport): BrainctlStatusReport {
    const sections = Object.values(report.sections);
    report.ok = !sections.some((section) => section.state === "fail");
    report.degraded = sections.some((section) => section.state === "warn" || section.state === "fail");
    return report;
  }

  function safeBrainApiBaseUrl(cfg: BrainstackConfig): { baseUrl: string; error?: string } {
    try {
      return { baseUrl: brainApiBaseUrl(cfg) };
    } catch (error) {
      return { baseUrl: "", error: sanitizeStatusError(error) };
    }
  }

  function safeBrainWriteStatus(cfg: BrainstackConfig): { baseUrlConfigured: boolean; importTokenPresent: boolean; error?: string } {
    try {
      const write = brainWriteConfig(cfg);
      return { baseUrlConfigured: Boolean(write.baseUrl), importTokenPresent: Boolean(write.token) };
    } catch (error) {
      return { baseUrlConfigured: false, importTokenPresent: false, error: sanitizeStatusError(error) };
    }
  }

  function configUsesTailscale(cfg: BrainstackConfig): boolean {
    const brainUrl = cfg.brain.publicBaseUrl || "";
    return cfg.profile === "client-macos"
      || cfg.profile === "worker"
      || cfg.security.trustedExposure === "tailscale-serve"
      || Boolean(cfg.client.telegramVia || cfg.client.remoteSsh)
      || /\.ts\.net(?::|\/|$)/.test(brainUrl)
      || cfg.tailscale.enableSsh
      || cfg.tailscale.advertiseTags.length > 0;
  }

  function tailscaleCommandPath(): string | null {
    const explicit = process.env.BRAINSTACK_TAILSCALE_BIN?.trim();
    if (explicit && existsSync(explicit)) {
      return explicit;
    }
    const fromPath = commandPath("tailscale");
    if (fromPath) {
      return fromPath;
    }
    if (process.platform !== "darwin") {
      return null;
    }
    const candidates = [
      "/Applications/Tailscale.app/Contents/MacOS/Tailscale",
      process.env.HOME ? join(process.env.HOME, "Applications", "Tailscale.app", "Contents", "MacOS", "Tailscale") : ""
    ].filter(Boolean);
    return candidates.find((candidate) => existsSync(candidate)) || null;
  }

  function tailscaleStoppedText(text: string): boolean {
    const lower = text.toLowerCase();
    return lower.includes("tailscale is stopped")
      || lower.includes("not running")
      || lower.includes("stopped");
  }

  function collectTailscaleStatusSection(cfg: BrainstackConfig, timeoutMs: number): BrainctlStatusSection {
    if (!configUsesTailscale(cfg)) {
      return statusSection("disabled", "Tailscale not required by this profile", {
        required: false,
        installed: null,
        running: null,
        action_hint: "none"
      });
    }
    const command = tailscaleCommandPath();
    if (!command) {
      return statusSection(
        "warn",
        "Tailscale is not installed or the CLI is unavailable",
        {
          required: true,
          installed: false,
          running: false,
          command: null,
          action_hint: "install",
          install_hint: installHint("tailscale")
        },
        { available: false, error: "tailscale command not found" }
      );
    }
    const status = run([command, "status", "--json"], { check: false, env: userShellPathEnv(), timeoutMs });
    const combined = `${status.stdout}\n${status.stderr}`.trim();
    if (status.code !== 0 || status.timedOut) {
      const stopped = tailscaleStoppedText(combined);
      return statusSection(
        "warn",
        stopped ? "Tailscale is stopped" : summarizeLines(combined || `tailscale status failed with exit ${status.code}`, 2),
        {
          required: true,
          installed: true,
          running: false,
          command,
          exit_code: status.code,
          timed_out: status.timedOut,
          action_hint: "start"
        },
        { available: false, error: sanitizeStatusError(combined || `tailscale status failed with exit ${status.code}`) }
      );
    }
    let backendState = "unknown";
    try {
      const parsed = JSON.parse(status.stdout) as Record<string, unknown>;
      backendState = typeof parsed.BackendState === "string" ? parsed.BackendState : backendState;
    } catch {
      backendState = "running";
    }
    const running = backendState.toLowerCase() === "running";
    return statusSection(running ? "ok" : "warn", running ? "Tailscale is running" : `Tailscale state=${backendState}`, {
      required: true,
      installed: true,
      running,
      command,
      backend_state: backendState,
      action_hint: running ? "none" : "start"
    }, running ? {} : { available: false });
  }

  function gitStatusProbe(args: string[], cwd: string, timeoutMs: number): { ok: boolean; output: string; code: number; timedOut: boolean } {
    const result = run(["git", ...args], { cwd, check: false, timeoutMs });
    return {
      ok: result.code === 0,
      output: (result.stdout || result.stderr).trim(),
      code: result.code,
      timedOut: result.timedOut
    };
  }

  async function localGitSnapshot(path: string, timeoutMs: number): Promise<Record<string, unknown>> {
    const info = await lstat(path).catch(() => null);
    if (!info) {
      return { path, exists: false, is_git: false, detail: "missing" };
    }
    if (info.isSymbolicLink()) {
      return { path, exists: true, is_git: false, detail: "refusing symlink" };
    }
    if (!info.isDirectory()) {
      return { path, exists: true, is_git: false, detail: "not a directory" };
    }
    if (!gitExists(path)) {
      return { path, exists: true, is_git: false, detail: "not a git checkout" };
    }
    const branch = gitStatusProbe(["rev-parse", "--abbrev-ref", "HEAD"], path, timeoutMs);
    const head = gitStatusProbe(["rev-parse", "HEAD"], path, timeoutMs);
    const dirty = gitStatusProbe(["status", "--porcelain"], path, timeoutMs);
    return {
      path,
      exists: true,
      is_git: true,
      branch: branch.ok ? branch.output || null : null,
      head: head.ok ? head.output || null : null,
      clean: dirty.ok ? dirty.output.length === 0 : null,
      dirty_count: dirty.ok && dirty.output ? dirty.output.split(/\r?\n/).filter(Boolean).length : 0,
      errors: [branch.ok ? null : `branch: ${branch.output || `exit ${branch.code}`}`, head.ok ? null : `head: ${head.output || `exit ${head.code}`}`, dirty.ok ? null : `status: ${dirty.output || `exit ${dirty.code}`}`].filter(Boolean)
    };
  }

  async function collectSharedBrainStatus(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
    const snapshots: Record<string, unknown>[] = [];
    if (cfg.profile === "client-macos" || cfg.profile === "worker") {
      snapshots.push(await localGitSnapshot(clientLocalPathAbs(cfg), timeoutMs));
    }
    if (runsBraind(cfg)) {
      snapshots.push(await localGitSnapshot(cfg.repos.serve, timeoutMs));
      snapshots.push(await localGitSnapshot(cfg.repos.staging, timeoutMs));
    }
    if (!snapshots.length) {
      return statusSection("disabled", "no shared-brain checkout role for this profile", { snapshots });
    }
    const unhealthy = snapshots.filter((snapshot) => snapshot.exists !== true || snapshot.is_git !== true || snapshot.clean === false || Array.isArray(snapshot.errors) && snapshot.errors.length > 0);
    const state: BrainctlStatusState = unhealthy.length ? "warn" : "ok";
    return statusSection(state, `checkouts=${snapshots.length} unhealthy=${unhealthy.length}`, { snapshots });
  }

  async function collectOutboxStatusSection(cfg: BrainstackConfig): Promise<BrainctlStatusSection> {
    const scans = await scanAllOutboxes(cfg);
    const items = scans.flatMap((scan) => scan.items.map((entry) => ({ ...entry, item: normalizeOutboxItem(entry.item) })));
    const corrupt = scans.flatMap((scan) => scan.corrupt);
    const terminalItems = items.filter((entry) => entry.item.terminal_error);
    const terminal = terminalItems.length;
    const state: BrainctlStatusState = corrupt.length ? "fail" : terminal ? "warn" : "ok";
    return statusSection(state, `queued=${items.length} terminal=${terminal} corrupt=${corrupt.length}`, {
      roots: scans.map((scan) => scan.root),
      queued: items.length,
      terminal,
      corrupt: corrupt.length,
      terminal_errors: summarizeOutboxTerminalErrors(terminalItems.map((entry) => entry.item.terminal_error))
    });
  }

  async function collectHooksStatusSection(cfg: BrainstackConfig): Promise<BrainctlStatusSection> {
    const targets: HookTarget[] = ["codex", "claude", "cursor"];
    const hooks: Array<Record<string, unknown>> = [];
    for (const target of targets) {
      const path = hookConfigPath(target);
      let count = 0;
      let error: string | null = null;
      try {
        const raw = await readJsonObject(path);
        count = target === "cursor" ? countCursorManagedHooks(raw, target) : countCodexStyleManagedHooks(raw, target);
      } catch (hookError) {
        error = sanitizeStatusError(hookError);
      }
      hooks.push({ target, path, hooks: count, installed: count > 0, ...(error ? { error } : {}) });
    }
    const selected = hooks.find((entry) => entry.target === cfg.harness.name);
    const selectedInstalled = Boolean(selected?.installed);
    const errorCount = hooks.filter((entry) => typeof entry.error === "string" && entry.error).length;
    const state: BrainctlStatusState = selectedInstalled && errorCount === 0 ? "ok" : "warn";
    const detail = `${cfg.harness.name} hooks ${selectedInstalled ? "installed" : "missing"}${errorCount ? ` errors=${errorCount}` : ""}`;
    return statusSection(state, detail, {
      selected_target: cfg.harness.name,
      hooks
    });
  }

  async function collectSkillsStatusSection(cfg: BrainstackConfig): Promise<BrainctlStatusSection> {
    const targets: HookTarget[] = ["codex", "claude", "cursor"];
    const roots: Array<Record<string, unknown>> = [];
    for (const target of targets) {
      try {
        const root = skillInstallRootForTarget(target, { command: "status", positional: [], flags: {} });
        const dirs = await skillDirsForDoctor(root);
        roots.push({ target, root, skills: dirs.length, installed: dirs.map((dir) => basename(dir)).slice(0, 100) });
      } catch (error) {
        roots.push({ target, root: null, skills: 0, error: sanitizeStatusError(error) });
      }
    }
    const selected = roots.find((entry) => entry.target === cfg.harness.name);
    const count = Number(selected?.skills || 0);
    return statusSection(count > 0 ? "ok" : "warn", `${cfg.harness.name} skills=${count}`, {
      selected_target: cfg.harness.name,
      roots
    });
  }

  async function statusFetchJson(baseUrl: string, path: string, timeoutMs: number): Promise<Record<string, unknown>> {
    const response = await fetch(new URL(path, baseUrl).toString(), {
      signal: AbortSignal.timeout(timeoutMs)
    });
    const text = await response.text();
    let body: unknown = null;
    try {
      body = text ? JSON.parse(text) : null;
    } catch {
      body = null;
    }
    return {
      ok: response.ok,
      http_status: response.status,
      body,
      content_type: response.headers.get("content-type") || null,
      ...(body === null && text ? { text: text.slice(0, 500) } : {})
    };
  }

  async function collectBrainApiStatusSection(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
    const base = safeBrainApiBaseUrl(cfg);
    if (!base.baseUrl) {
      return statusSection("disabled", "brain API base URL unavailable", { base_url_configured: false }, { available: false, error: base.error });
    }
    const [health, ready] = await Promise.allSettled([
      statusFetchJson(base.baseUrl, "/healthz", timeoutMs),
      statusFetchJson(base.baseUrl, "/readyz", timeoutMs)
    ]);
    const healthData = health.status === "fulfilled" ? health.value : { ok: false, error: sanitizeStatusError(health.reason) };
    const readyData = ready.status === "fulfilled" ? ready.value : { ok: false, error: sanitizeStatusError(ready.reason) };
    const healthOk = Boolean(healthData.ok);
    const readyOk = Boolean(readyData.ok);
    const state: BrainctlStatusState = healthOk && readyOk ? "ok" : "warn";
    return statusSection(state, `base_url=${base.baseUrl} health=${healthOk ? "ok" : "unavailable"} ready=${readyOk ? "ok" : "unavailable"}`, {
      base_url: base.baseUrl,
      health: healthData,
      ready: readyData
    }, { available: healthOk || readyOk });
  }

  async function collectCuratorStatusSection(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
    const base = safeBrainApiBaseUrl(cfg);
    if (!base.baseUrl) {
      return statusSection("disabled", "curator status unavailable without brain API base URL", undefined, { available: false, error: base.error });
    }
    const response = await statusFetchJson(base.baseUrl, "/api/curator/status", timeoutMs);
    if (!response.ok) {
      return statusSection("warn", `curator status unavailable HTTP ${response.http_status}`, response, { available: false });
    }
    const body = response.body && typeof response.body === "object" && !Array.isArray(response.body) ? (response.body as Record<string, unknown>) : {};
    const curator = body.curator && typeof body.curator === "object" && !Array.isArray(body.curator) ? (body.curator as Record<string, unknown>) : {};
    const counts = body.proposal_counts && typeof body.proposal_counts === "object" && !Array.isArray(body.proposal_counts) ? (body.proposal_counts as Record<string, unknown>) : {};
    const open = Number(counts.pending || 0) + Number(counts.approved || 0) + Number(counts["needs-human"] || 0);
    const installed = Boolean(curator.installed);
    return statusSection("ok", `mode=${String(body.mode || cfg.curation.mode)} installed=${installed} open_proposals=${open}`, {
      mode: body.mode || cfg.curation.mode,
      curator,
      proposal_counts: counts,
      open_proposals: open
    });
  }

  function proposalSummaryFallbackFromCurator(curatorSection: BrainctlStatusSection | undefined, error: unknown): BrainctlStatusSection | null {
    if (!curatorSection || curatorSection.state !== "ok" || !curatorSection.data || typeof curatorSection.data !== "object" || Array.isArray(curatorSection.data)) {
      return null;
    }
    const curatorData = curatorSection.data as Record<string, unknown>;
    const open = Number(curatorData.open_proposals);
    if (!Number.isFinite(open)) {
      return null;
    }
    const counts =
      curatorData.proposal_counts && typeof curatorData.proposal_counts === "object" && !Array.isArray(curatorData.proposal_counts)
        ? (curatorData.proposal_counts as Record<string, unknown>)
        : {};
    const byStatus: Record<string, number> = {};
    for (const [key, value] of Object.entries(counts)) {
      const count = Number(value);
      if (Number.isFinite(count)) {
        byStatus[key] = count;
      }
    }
    return statusSection("ok", `open_proposals=${open} (proposal list refresh slow; using curator summary)`, {
      count: open,
      by_status: byStatus,
      review_groups: [],
      proposals: [],
      list_available: false,
      fallback_source: "curator",
      list_error: sanitizeStatusError(error)
    });
  }

  async function collectProposalsStatusSection(cfg: BrainstackConfig, timeoutMs: number, curatorSection?: BrainctlStatusSection): Promise<BrainctlStatusSection> {
    const base = safeBrainApiBaseUrl(cfg);
    if (!base.baseUrl) {
      return statusSection("disabled", "proposal status unavailable without brain API base URL", undefined, { available: false, error: base.error });
    }
    let response: Record<string, unknown>;
    try {
      response = await statusFetchJson(base.baseUrl, "/api/proposals?status=open", timeoutMs);
    } catch (error) {
      const fallback = proposalSummaryFallbackFromCurator(curatorSection, error);
      if (fallback) {
        return fallback;
      }
      throw error;
    }
    if (!response.ok) {
      return statusSection("warn", `open proposal list unavailable HTTP ${response.http_status}`, response, { available: false });
    }
    const body = response.body && typeof response.body === "object" && !Array.isArray(response.body) ? (response.body as Record<string, unknown>) : {};
    const proposals = Array.isArray(body.proposals) ? (body.proposals as Array<Record<string, unknown>>) : [];
    const reviewGroups = proposalReviewGroupsFromResult(body);
    const byStatus: Record<string, number> = {};
    for (const proposal of proposals) {
      const proposalStatus = String(proposal.status || "unknown");
      byStatus[proposalStatus] = (byStatus[proposalStatus] || 0) + 1;
    }
    return statusSection("ok", `open_proposals=${proposals.length}${reviewGroups.length ? ` review_groups=${reviewGroups.length}` : ""}`, {
      count: proposals.length,
      by_status: byStatus,
      review_groups: reviewGroups.slice(0, 10),
      proposals: proposals.slice(0, 25)
    });
  }

  async function collectTelemuxStatusSection(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
    if (!cfg.telemux.enabled) {
      if (cfg.client.telegramVia) {
        return statusSection("ok", `Telegram routed through control host ${cfg.client.telegramVia}; no local telemux service`, {
          enabled: false,
          mode: "remote-control-host",
          via: cfg.client.telegramVia,
          remote_repo: cfg.client.telegramRemoteRepo || "~/brainstack"
        });
      }
      return statusSection("disabled", "local telemux service not enabled for this machine", { enabled: false, mode: "none" });
    }
    const baseUrl = `http://${cfg.telemux.dashboardHost}:${cfg.telemux.dashboardPort}`;
    const response = await statusFetchJson(baseUrl, "/healthz", timeoutMs);
    if (!response.ok) {
      return statusSection("warn", `telemux healthz unavailable HTTP ${response.http_status}`, response, { available: false });
    }
    const body = response.body && typeof response.body === "object" && !Array.isArray(response.body) ? (response.body as Record<string, unknown>) : {};
    const due = Number(body.cronDue || 0);
    const pending = Number(body.cronPending || 0);
    const workerDegraded = Number(body.workerDegraded || 0);
    const state: BrainctlStatusState = due > 0 || pending > 0 || workerDegraded > 0 ? "warn" : "ok";
    return statusSection(state, `crons=${String(body.crons ?? "unknown")} due=${due} pending=${pending} worker_degraded=${workerDegraded}`, {
      base_url: baseUrl,
      healthz: body
    });
  }

  async function collectDaemonStatusSection(cfg: BrainstackConfig, args: ParsedArgs, timeoutMs: number): Promise<BrainctlStatusSection> {
    if (!usesBrainstackDaemon(cfg)) {
      return statusSection("disabled", "brainstackd not used by this profile", { profile: cfg.profile });
    }
    const status = await readDaemonStatus(cfg);
    const platform = daemonPlatform(args);
    const servicePath = daemonServicePath(cfg, platform);
    const pidAlive = typeof status?.pid === "number" ? processAlive(status.pid) : null;
    const fresh = daemonStatusFresh(status, 10 * 60_000);
    const service =
      fresh
        ? {
            platform,
            installed: existsSync(servicePath),
            running: pidAlive === true ? true : null,
            path: servicePath,
            detail: pidAlive === true ? "pid alive from daemon status" : "fresh daemon status; service-manager probe skipped"
          }
        : await daemonServiceStatus(cfg, args, timeoutMs);
    const active = service.running === true || pidAlive === true;
    const state: BrainctlStatusState = status?.ok && fresh && active ? "ok" : "warn";
    return statusSection(state, `service=${service.running === true ? "running" : service.running === false ? "stopped" : "unknown"} fresh=${fresh}`, {
      service,
      status,
      fresh,
      pid_alive: pidAlive
    }, { available: Boolean(status) || service.running === true });
  }

  async function collectProductStatusSection(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
    const productRoot = cfg.paths.productRepo || PRODUCT_ROOT;
    if (!gitExists(productRoot)) {
      const clientProfile = cfg.profile.startsWith("client");
      return statusSection(
        clientProfile ? "disabled" : "warn",
        clientProfile ? "source checkout not installed for this client" : "product repo is not a git checkout",
        { path: productRoot },
        { available: false }
      );
    }
    const branch = gitStatusProbe(["rev-parse", "--abbrev-ref", "HEAD"], productRoot, timeoutMs);
    const head = gitStatusProbe(["rev-parse", "HEAD"], productRoot, timeoutMs);
    const dirty = gitStatusProbe(["status", "--porcelain"], productRoot, timeoutMs);
    const remoteRef = ["origin/main", "refs/remotes/https-main"].find((candidate) => gitStatusProbe(["rev-parse", "--verify", candidate], productRoot, timeoutMs).ok) || null;
    const aheadBehind = remoteRef ? gitStatusProbe(["rev-list", "--left-right", "--count", `HEAD...${remoteRef}`], productRoot, timeoutMs) : null;
    const dirtyCount = dirty.ok && dirty.output ? dirty.output.split(/\r?\n/).filter(Boolean).length : 0;
    const state: BrainctlStatusState = dirtyCount > 0 || branch.ok === false || head.ok === false ? "warn" : "ok";
    return statusSection(state, `branch=${branch.output || "unknown"} dirty=${dirtyCount}`, {
      path: productRoot,
      branch: branch.ok ? branch.output : null,
      head: head.ok ? head.output : null,
      dirty_count: dirtyCount,
      remote_ref: remoteRef,
      ahead_behind: aheadBehind?.ok ? aheadBehind.output : null
    });
  }

  function remoteBrainctlScript(remoteRepo: string, argv: string[], options: { preferInstalledBinary?: boolean } = {}): string {
    const argvText = argv.map(quoteForBash).join(" ");
    return [
      "set -euo pipefail",
      "brainstack_expand_home() {",
      '  case "$1" in',
      '    \\~) printf \'%s\\n\' "$HOME" ;;',
      '    \\~/*) printf \'%s/%s\\n\' "$HOME" "${1#\\~/}" ;;',
      '    *) printf \'%s\\n\' "$1" ;;',
      "  esac",
      "}",
      `repo="$(brainstack_expand_home ${quoteForBash(remoteRepo)})"`,
      'cd "$repo"',
      options.preferInstalledBinary
        ? [
            'if [ -x "$HOME/.local/bin/brainctl" ]; then',
            `  exec "$HOME/.local/bin/brainctl" ${argvText} --config "$HOME/.config/brainstack/brainstack.yaml"`,
            "fi"
          ].join("\n")
        : null,
      'if [ -x "$HOME/.bun/bin/bun" ]; then',
      '  bun_bin="$HOME/.bun/bin/bun"',
      "else",
      '  bun_bin="$(command -v bun)"',
      "fi",
      `exec "$bun_bin" --no-env-file run packages/brainctl/src/main.ts ${argvText} --config "$HOME/.config/brainstack/brainstack.yaml"`
    ]
      .filter((line): line is string => Boolean(line))
      .join("\n");
  }

  function controlSshTarget(
    cfg: BrainstackConfig,
    args: ParsedArgs,
    label: string,
    options: { allowRemoteSshFallback?: boolean } = {}
  ): string | null {
    const via =
      requireFlagValue(args, "via") ||
      process.env.BRAINSTACK_TELEGRAM_VIA?.trim() ||
      cfg.client.telegramVia ||
      (options.allowRemoteSshFallback ? sshTargetFromRemoteSsh(cfg.client.remoteSsh) : null);
    return via ? validateInviteSshTarget(via, label) : null;
  }

  function runControlRemoteScript(
    cfg: BrainstackConfig,
    args: ParsedArgs,
    label: string,
    remoteScript: string,
    timeoutMs: number,
    options: { allowRemoteSshFallback?: boolean } = {}
  ): ReturnType<typeof run> | null {
    const via = controlSshTarget(cfg, args, label, options);
    if (!via) {
      return null;
    }
    const worker = telegramControlWorker(via);
    const knownHostsPath = telegramKnownHostsPath(cfg, args);
    const sshTrustMode = telegramSshTrustMode(args);
    if (sshTrustMode === "accept-new") {
      mkdirSync(dirname(knownHostsPath), { recursive: true });
    }
    const sshBin = Bun.which("ssh") || "ssh";
    return run(
      [
        sshBin,
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=8",
        ...telegramSshTrustArgs(sshTrustMode, knownHostsPath),
        ...workerSshPortArgs(worker),
        workerRemoteTarget(worker),
        `bash -lc ${quoteForBash(remoteScript)}`
      ],
      { check: false, timeoutMs }
    );
  }

  const statusFleetCommands = createStatusFleetCommands({
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
    env: process.env,
    nowIso: () => new Date().toISOString(),
    whichCommand: (name) => Bun.which(name)
  });
  const collectControlSourceStatusSection = statusFleetCommands.collectControlSourceStatusSection;
  const collectFleetStatusSection = statusFleetCommands.collectFleetStatusSection;
  const commandFleet = statusFleetCommands.commandFleet;

  const updatesCommand = createUpdatesCommand({
    PRODUCT_ROOT,
    commandPath,
    flag,
    gitExists,
    harnessCompatibility,
    installHint,
    loadConfig,
    run,
    summarizeLines,
    updateProbeAllowed,
    updateProbeTimeoutMs,
    userShellPathEnv
  });
  const commandUpdates = updatesCommand.commandUpdates;

  function formatBrainctlStatusReport(report: BrainctlStatusReport): string {
    const lines = [
      `brainstack status: ok=${report.ok} degraded=${report.degraded} profile=${report.profile || "unknown"} machine=${report.machine || "unknown"}`,
      `config=${report.config_path}`
    ];
    for (const [name, section] of Object.entries(report.sections)) {
      lines.push(`${section.state.toUpperCase()} ${name}: ${section.detail}${section.error ? ` (${section.error})` : ""}`);
    }
    return lines.join("\n");
  }

  async function commandStatus(args: ParsedArgs): Promise<void> {
    const configPath = abs(requireFlagValue(args, "config") || brainstackDefaultConfigPath());
    const timeoutMs = statusTimeoutMs(args);
    const report: BrainctlStatusReport = {
      schema_version: 1,
      product: "brainstack",
      generated_at: new Date().toISOString(),
      config_path: configPath,
      profile: null,
      machine: null,
      ok: false,
      degraded: true,
      sections: {}
    };

    let cfg: BrainstackConfig | null = null;
    try {
      cfg = await loadConfig(configPath, flag(args, "profile"), flag(args, "root"));
      report.profile = cfg.profile;
      report.machine = cfg.machine.name;
      const write = safeBrainWriteStatus(cfg);
      report.sections.config = statusSection(write.error ? "warn" : "ok", write.error ? "config loaded with write env warning" : "config loaded", {
        profile: cfg.profile,
        machine: cfg.machine,
        harness: cfg.harness,
        paths: {
          product_repo: cfg.paths.productRepo,
          state_root: cfg.paths.stateRoot,
          config_root: cfg.paths.configRoot,
          shared_brain_root: cfg.paths.sharedBrainRoot,
          client_local_path: clientLocalPathAbs(cfg),
          client_env_path: clientEnvPathAbs(cfg)
        },
        brain: {
          public_base_url_configured: Boolean(cfg.brain.publicBaseUrl),
          write_base_url_configured: write.baseUrlConfigured,
          import_token_present: write.importTokenPresent
        },
        curation: cfg.curation
      }, write.error ? { error: write.error } : {});
    } catch (error) {
      report.sections.config = statusSection("fail", "config load failed", undefined, {
        available: false,
        error: sanitizeStatusError(error)
      });
      finalizeBrainctlStatus(report);
      console.log(hasFlag(args, "json") ? JSON.stringify(report, null, 2) : formatBrainctlStatusReport(report));
      if (hasFlag(args, "strict")) {
        throw new Error("brainctl status failed: config");
      }
      return;
    }

    report.sections.daemon = await collectStatusSection(() => collectDaemonStatusSection(cfg!, args, timeoutMs), timeoutMs);
    report.sections.tailscale = await collectStatusSection(() => Promise.resolve(collectTailscaleStatusSection(cfg!, timeoutMs)), timeoutMs);
    report.sections.shared_brain = await collectStatusSection(() => collectSharedBrainStatus(cfg!, Math.min(timeoutMs, 2000)), timeoutMs);
    report.sections.outbox = await collectStatusSection(() => collectOutboxStatusSection(cfg!), timeoutMs, "fail");
    report.sections.hooks = await collectStatusSection(() => collectHooksStatusSection(cfg!), timeoutMs);
    report.sections.skills = await collectStatusSection(() => collectSkillsStatusSection(cfg!), timeoutMs);
    report.sections.brain_api = await collectStatusSection(() => collectBrainApiStatusSection(cfg!, timeoutMs), timeoutMs + 250);
    if (report.sections.brain_api.available === false) {
      report.sections.curator = statusSection("disabled", "blocked by unavailable Brain API", undefined, { available: false });
      report.sections.proposals = statusSection("disabled", "blocked by unavailable Brain API", undefined, { available: false });
    } else {
      report.sections.curator = await collectStatusSection(() => collectCuratorStatusSection(cfg!, timeoutMs), timeoutMs + 250);
      report.sections.proposals = await collectStatusSection(() => collectProposalsStatusSection(cfg!, timeoutMs, report.sections.curator), timeoutMs + 250);
    }
    report.sections.telemux = await collectStatusSection(() => collectTelemuxStatusSection(cfg!, timeoutMs), timeoutMs + 250);
    if (!hasFlag(args, "skip-fleet")) {
      const fleetTimeoutMs = Math.min(Math.max(timeoutMs * 4, 3000), 15_000);
      report.sections.fleet = await collectStatusSection(() => collectFleetStatusSection(cfg!, args, fleetTimeoutMs), fleetTimeoutMs + 500);
    }
    const controlSourceTimeoutMs = cfg.client.telegramVia ? Math.max(Math.min(timeoutMs, 3000), 2500) : timeoutMs;
    report.sections.control_source = await collectStatusSection(() => collectControlSourceStatusSection(cfg!, controlSourceTimeoutMs), controlSourceTimeoutMs + 250);
    report.sections.product = await collectStatusSection(() => collectProductStatusSection(cfg!, Math.min(timeoutMs, 2000)), timeoutMs);
    finalizeBrainctlStatus(report);
    console.log(hasFlag(args, "json") ? JSON.stringify(report, null, 2) : formatBrainctlStatusReport(report));
    if (hasFlag(args, "strict") && !report.ok) {
      const failures = Object.entries(report.sections)
        .filter(([, section]) => section.state === "fail")
        .map(([name]) => name);
      throw new Error(`brainctl status failed: ${failures.join(", ") || "unknown"}`);
    }
  }

  async function commandWorkerCache(args: ParsedArgs): Promise<void> {
    const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
    const sub = args.positional[0] || "status";
    const cache = readWorkerEnvCache(cfg);
    if (sub === "status") {
      const worker = args.positional[1] || flag(args, "worker");
      const records = Object.values(cache).filter((record) => !worker || record.worker === worker);
      console.log(`worker_cache=${workerEnvCachePath(cfg)}`);
      console.log(records.length ? records.map((record) => `${record.worker} harness=${record.harness} bin=${record.harnessBin} detected=${record.detectedAt}`).join("\n") : "(empty)");
      return;
    }
    if (sub === "clear") {
      if (hasFlag(args, "all")) {
        await rm(workerEnvCachePath(cfg), { force: true });
        console.log("worker_cache=cleared");
        return;
      }
      const worker = args.positional[1] || flag(args, "worker");
      if (!worker) {
        throw new Error("worker-cache clear requires a worker name or --all");
      }
      delete cache[worker];
      await writeWorkerEnvCache(cfg, cache);
      console.log(`worker_cache_cleared=${worker}`);
      return;
    }
    throw new Error("worker-cache subcommand must be status|clear");
  }

  return {
    check,
    commandOk,
    harnessCompatibility,
    outboxParentRoot,
    outboxRoot,
    outboxRootForDestination,
    brainInstanceId,
    formatDoctorChecks,
    workerEnvCachePath,
    readWorkerEnvCache,
    writeWorkerEnvCache,
    cachedWorkerPath,
    workerHarnessFamily,
    workerHarnessBin,
    workerRemoteTarget,
    workerSshHost,
    workerSshPort,
    workerSshKnownHostsLookup,
    workerSshKnownHostsPath,
    workerSshTrustMode,
    workerSshTrustArgs,
    workerSshPortArgs,
    runWorkerShell,
    quoteForBash,
    braindPortOwnerCheck,
    commandDoctor,
    sanitizeStatusError,
    statusSection,
    gitStatusProbe,
    remoteBrainctlScript,
    controlSshTarget,
    runControlRemoteScript,
    collectControlSourceStatusSection,
    collectFleetStatusSection,
    commandFleet,
    commandUpdates,
    commandStatus,
    commandWorkerCache
  };
}
