import type { ParsedArgs } from "../args";
import type { BrainstackConfig, DoctorCheck, HarnessName } from "../config";
import type { run as runtimeRun } from "../runtime";

type UpdatesDeps = {
  PRODUCT_ROOT: string;
  commandPath: (name: string) => string | null;
  flag: (args: ParsedArgs, name: string) => string | null;
  gitExists: (path: string) => boolean;
  harnessCompatibility: (name: HarnessName, command: string, options?: { required?: boolean }) => DoctorCheck;
  installHint: (name: string) => string;
  loadConfig: (path?: string | null, profile?: string | null, root?: string | null) => Promise<BrainstackConfig>;
  run: typeof runtimeRun;
  summarizeLines: (text: string, maxLines?: number) => string;
  updateProbeAllowed: (command: string) => boolean;
  updateProbeTimeoutMs: () => number;
  userShellPathEnv: () => Record<string, string> | undefined;
};

export function createUpdatesCommand(deps: UpdatesDeps) {
  const {
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
  } = deps;

  function updateProbe(
    command: string,
    args: string[],
    okCodes: number[] = [0],
    envOverrides: Record<string, string> = {},
    label: string | null = null
  ): { label: string; code: number; output: string } | null {
    if (!updateProbeAllowed(command)) {
      return null;
    }
    const executable = commandPath(command);
    if (!executable) {
      return null;
    }
    const env = { ...userShellPathEnv(), ...envOverrides };
    const proc = run([executable, ...args], { check: false, env, timeoutMs: updateProbeTimeoutMs() });
    const output = `${proc.stdout}${proc.stderr ? `\n${proc.stderr}` : ""}`.trim();
    const status = okCodes.includes(proc.code) ? "ok" : `exit-${proc.code}`;
    return {
      label: label || `${command} ${args.join(" ")}`.trim(),
      code: proc.code,
      output: `${status}\n${summarizeLines(output)}`
    };
  }

  function pacmanUpdateProbe(): { label: string; code: number; output: string } | null {
    const probe = updateProbe("pacman", ["-Qu"], [0]);
    if (!probe) {
      return null;
    }
    if (probe.code === 1 && probe.output === "exit-1\n(none)") {
      return {
        ...probe,
        output: "ok\n(none)"
      };
    }
    return probe;
  }

  function collectOsUpdateProbes(): Array<{ label: string; code: number; output: string }> {
    const probes: Array<{ label: string; code: number; output: string }> = [];
    const add = (probe: { label: string; code: number; output: string } | null) => {
      if (probe) probes.push(probe);
    };

    add(
      updateProbe(
        "brew",
        ["outdated", "--quiet"],
        [0],
        {
          HOMEBREW_NO_AUTO_UPDATE: "1",
          HOMEBREW_NO_ENV_HINTS: "1",
          HOMEBREW_NO_INSTALL_CLEANUP: "1"
        },
        "brew outdated --quiet (HOMEBREW_NO_AUTO_UPDATE=1)"
      )
    );
    add(pacmanUpdateProbe());
    add(updateProbe("checkupdates", []));
    add(updateProbe("apt", ["list", "--upgradable"]));
    // dnf exits 100 when updates are available; that is a successful read-only probe.
    add(updateProbe("dnf", ["--cacheonly", "check-update", "--quiet"], [0, 100]));
    add(updateProbe("zypper", ["--no-refresh", "list-updates"]));
    return probes;
  }

  function gitUpdateProbe(productRoot: string, args: string[]): ReturnType<typeof run> {
    return run(["git", ...args], { cwd: productRoot, check: false, timeoutMs: updateProbeTimeoutMs() });
  }

  function harnessVersionSummary(item: DoctorCheck): string {
    if (item.detail.endsWith(" not found in PATH")) {
      return "missing";
    }
    return item.detail.split(";")[0] || item.detail;
  }

  async function commandUpdates(args: ParsedArgs): Promise<void> {
    const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
    const productRoot = cfg.paths.productRepo || PRODUCT_ROOT;
    const productGitAvailable = gitExists(productRoot);
    const branch = productGitAvailable ? gitUpdateProbe(productRoot, ["rev-parse", "--abbrev-ref", "HEAD"]).stdout.trim() || "unknown" : "unavailable";
    const head = productGitAvailable ? gitUpdateProbe(productRoot, ["rev-parse", "HEAD"]).stdout.trim() || "unavailable" : "unavailable";
    const remoteCandidates = ["origin/main", "refs/remotes/https-main"];
    const remoteRef =
      productGitAvailable ? remoteCandidates.find((candidate) => gitUpdateProbe(productRoot, ["rev-parse", "--verify", candidate]).code === 0) || null : null;
    const origin = remoteRef ? gitUpdateProbe(productRoot, ["rev-parse", "--verify", remoteRef]).stdout.trim() : "";
    const aheadBehind = remoteRef
      ? gitUpdateProbe(productRoot, ["rev-list", "--left-right", "--count", `HEAD...${remoteRef}`]).stdout.trim()
      : "unknown";
    const compat = [
      harnessCompatibility("codex", "codex", { required: cfg.harness.name === "codex" }),
      harnessCompatibility("claude", "claude", { required: cfg.harness.name === "claude" })
    ];
    const codex = harnessVersionSummary(compat[0]);
    const claude = harnessVersionSummary(compat[1]);
    console.log(`brainstack_source=${productGitAvailable ? productRoot : "not-installed"}`);
    console.log(`brainstack_branch=${branch}`);
    console.log(`brainstack_head=${head}`);
    console.log(`origin_main=${origin || "unavailable"}`);
    console.log(`remote_main_ref=${remoteRef || "unavailable"}`);
    console.log(`ahead_behind=${aheadBehind}`);
    console.log(`codex=${codex}`);
    console.log(`claude=${claude}`);
    for (const item of compat) {
      console.log(`${item.status} ${item.name}: ${item.detail}`);
      if (item.remediation) console.log(`  remediation: ${item.remediation}`);
    }
    const osProbes = collectOsUpdateProbes();
    console.log("os_update_checks:");
    if (!osProbes.length) {
      console.log("  no supported package manager found");
    } else {
      for (const probe of osProbes) {
        console.log(`  ${probe.label}:`);
        for (const line of probe.output.split(/\r?\n/)) {
          console.log(`    ${line}`);
        }
      }
    }
    console.log("manual_update_commands:");
    if (productGitAvailable) {
      console.log(`  brainstack: cd ${productRoot} && git pull --ff-only && bun install --frozen-lockfile && bun test`);
    } else if (cfg.profile.startsWith("client")) {
      console.log("  brainstack: rerun the Brainstack installer or replace ~/.local/bin/brainctl from a signed release");
    } else {
      console.log(`  brainstack: install or repair the product checkout at ${productRoot}`);
    }
    console.log("  os packages: use your package manager manually after reviewing the read-only check above");
    console.log(`  selected harness: ${installHint(cfg.harness.name)}`);
  }

  return { commandUpdates };
}
