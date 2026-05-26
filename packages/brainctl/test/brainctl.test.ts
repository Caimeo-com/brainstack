import { describe, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { chmod, mkdir, mkdtemp, readdir, readFile, rm, stat, symlink, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { createServer, type Server } from "node:net";
import { hostname, tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { syncWritableRepo } from "../../../apps/braind/src/brain-lib";
import { loadConfig, parseSimpleYaml } from "../src/main";

const PRODUCT_ROOT = resolve(import.meta.dir, "..", "..", "..");
const BRAINCTL = join(PRODUCT_ROOT, "packages", "brainctl", "src", "main.ts");
const SERVER = join(PRODUCT_ROOT, "apps", "braind", "src", "server.ts");
const FORBIDDEN_PUBLIC_PATTERNS: Array<{ label: string; pattern: RegExp }> = [
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

function runCommand(args: string[], options: { cwd?: string; env?: Record<string, string> } = {}) {
  const proc = Bun.spawnSync(args, {
    cwd: options.cwd || PRODUCT_ROOT,
    env: { ...process.env, ...(options.env || {}) },
    stdout: "pipe",
    stderr: "pipe"
  });
  return {
    code: proc.exitCode,
    stdout: proc.stdout.toString(),
    stderr: proc.stderr.toString()
  };
}

function expectSuccess(result: ReturnType<typeof runCommand>): void {
  if (result.code !== 0) {
    throw new Error(`command failed\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`);
  }
  expect(result.code).toBe(0);
}

function sha256Text(text: string): string {
  return createHash("sha256").update(text).digest("hex");
}

function canonicalJson(value: unknown): string {
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

function runBrainctl(args: string[], env?: Record<string, string>) {
  return runCommand(["bun", "run", BRAINCTL, ...args], { env });
}

function git(args: string[], cwd: string): string {
  const result = runCommand(["git", ...args], { cwd });
  if (result.code !== 0) {
    throw new Error(`git ${args.join(" ")} failed\n${result.stderr || result.stdout}`);
  }
  return result.stdout.trim();
}

async function writeFixtureConfig(path: string): Promise<void> {
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

async function waitForBraind(port: number): Promise<void> {
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

async function waitForCondition(condition: () => Promise<boolean>, label: string, attempts = 50): Promise<void> {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    if (await condition()) {
      return;
    }
    await Bun.sleep(100);
  }
  throw new Error(`timed out waiting for ${label}`);
}

function braindTestEnv(root: string, port: number, overrides: Record<string, string> = {}): Record<string, string> {
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

async function createSingleNodeInstall(dir: string): Promise<string> {
  const configPath = join(dir, "config.yaml");
  const root = join(dir, "install");
  await writeFixtureConfig(configPath);
  expectSuccess(runBrainctl(["init", "--profile", "single-node", "--config", configPath, "--root", root]));
  return root;
}

async function startBraind(root: string, port: number, overrides: Record<string, string> = {}): Promise<ReturnType<typeof Bun.spawn>> {
  const proc = Bun.spawn(["bun", "run", SERVER], {
    cwd: PRODUCT_ROOT,
    env: braindTestEnv(root, port, overrides),
    stdout: "pipe",
    stderr: "pipe"
  });
  await waitForBraind(port);
  return proc;
}

async function stopBraind(proc: ReturnType<typeof Bun.spawn> | null): Promise<void> {
  if (!proc) {
    return;
  }
  proc.kill();
  await proc.exited;
}

function repoPathUrl(repoPath: string): string {
  return encodeURIComponent(repoPath).replace(/%2F/g, "/");
}

async function listFiles(root: string): Promise<string[]> {
  const entries = await readdir(root, { withFileTypes: true });
  const files: string[] = [];
  for (const entry of entries) {
    const fullPath = join(root, entry.name);
    if (entry.isDirectory()) {
      if (["node_modules", "dist", ".git"].includes(entry.name)) {
        continue;
      }
      files.push(...(await listFiles(fullPath)));
    } else if (entry.isFile()) {
      files.push(fullPath);
    }
  }
  return files;
}

describe("brainctl config", () => {
  test("parses nested yaml with list objects", () => {
    const parsed = parseSimpleYaml(`
profile: control
machine:
  name: brain-control
tailscale:
  advertiseTags:
    - tag:brain
telemux:
  workers:
    - name: brain-control
      transport: local
    - name: brain-worker
      transport: ssh
`);
    expect(parsed.profile).toBe("control");
    expect((parsed.machine as Record<string, unknown>).name).toBe("brain-control");
    expect((parsed.tailscale as Record<string, unknown>).advertiseTags).toEqual(["tag:brain"]);
    expect((parsed.telemux as Record<string, unknown>).workers).toHaveLength(2);
  });

  test("applies root override for disposable installs", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-test-"));
    try {
      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "profile: single-node",
          "machine:",
          "  name: brain-control",
          "paths:",
          "  sharedBrainRoot: ~/shared-brain",
          ""
        ].join("\n")
      );
      const cfg = await loadConfig(configPath, "single-node", join(dir, "root"));
      expect(cfg.repos.bare).toBe(join(dir, "root", "shared-brain", "bare", "shared-brain.git"));
      expect(cfg.repos.staging).toBe(join(dir, "root", "shared-brain", "staging", "shared-brain"));
      expect(cfg.repos.serve).toBe(join(dir, "root", "shared-brain", "serve", "shared-brain"));
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("expands configured home paths without using the build machine home", async () => {
    const cfg = await loadConfig(join(PRODUCT_ROOT, "examples", "control.yaml"), "control");
    expect(cfg.paths.productRepo).toBe("/home/operator/brainstack");
    expect(cfg.paths.sharedBrainRoot).toBe("/home/operator/shared-brain");
    expect(cfg.repos.remoteSsh).toBe("operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git");
  });

  test("rejects unsupported schema versions", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-schema-"));
    try {
      const configPath = join(dir, "config.yaml");
      await writeFile(configPath, ["schema_version: 999", "profile: control", ""].join("\n"));
      await expect(loadConfig(configPath, "control")).rejects.toThrow("Unsupported config schema version 999");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("rejects unknown top-level config keys", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-unknown-key-"));
    try {
      const configPath = join(dir, "config.yaml");
      await writeFile(configPath, ["schema_version: 1", "profile: control", "surprise: true", ""].join("\n"));
      await expect(loadConfig(configPath, "control")).rejects.toThrow("Unsupported top-level config keys: surprise");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("rejects private-journal until separate private-brain routing is implemented", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-private-profile-"));
    try {
      const configPath = join(dir, "config.yaml");
      await writeFile(configPath, ["schema_version: 1", "profile: private-journal", ""].join("\n"));
      await expect(loadConfig(configPath)).rejects.toThrow("Unsupported profile private-journal");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("missing config errors are actionable and list nearby candidates", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-missing-config-"));
    try {
      const configDir = join(dir, ".config", "brainstack");
      await mkdir(configDir, { recursive: true });
      const candidate = join(configDir, "current-control.brainstack.yaml");
      await writeFile(candidate, "schema_version: 1\nprofile: control\n");
      const missing = join(configDir, "brainstack.yaml");
      const result = runBrainctl(["doctor", "--config", missing], { HOME: dir });
      expect(result.code).not.toBe(0);
      const output = `${result.stdout}\n${result.stderr}`;
      expect(output).toContain(`Brainstack config not found: ${missing}`);
      expect(output).toContain(`brainctl provision --profile control --out ${missing}`);
      expect(output).toContain(`brainctl doctor --config ${candidate}`);
      expect(output).not.toContain("ENOENT");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("resolves bun path dynamically from PATH", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-bun-path-"));
    const previousPath = process.env.PATH;
    try {
      const fakeBun = join(dir, "bun");
      await writeFile(fakeBun, "#!/usr/bin/env sh\nprintf 'fake bun should not execute\\n'\n");
      await chmod(fakeBun, 0o755);
      process.env.PATH = `${dir}:${previousPath || ""}`;
      const cfg = await loadConfig(join(PRODUCT_ROOT, "examples", "control.yaml"), "control");
      expect(cfg.runtime.bunBin).toBe(fakeBun);
    } finally {
      process.env.PATH = previousPath;
      await rm(dir, { recursive: true, force: true });
    }
  });
});

describe("brainctl install safety", () => {
  test("provision checks prerequisites, tests selected harness, and writes config", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-provision-"));
    try {
      const binDir = join(dir, "bin");
      const configPath = join(dir, "brainstack.yaml");
      await mkdir(binDir, { recursive: true });
      const fakeSudo = join(binDir, "sudo");
      const fakeCodex = join(binDir, "codex");
      await writeFile(fakeSudo, "#!/usr/bin/env sh\nexit 0\n");
      await writeFile(
        fakeCodex,
        "#!/usr/bin/env sh\ncat >/dev/null\nprintf 'BRAINSTACK_HARNESS_SUDO_OK\\n'\n"
      );
      await chmod(fakeSudo, 0o755);
      await chmod(fakeCodex, 0o755);

      const result = runBrainctl(
        [
          "provision",
          "--profile",
          "control",
          "--out",
          configPath,
          "--harness",
          "codex",
          "--enable-telemux",
          "--brain-base-url",
          "https://brain-control.example.ts.net"
        ],
        {
          PATH: `${binDir}:${process.env.PATH || ""}`,
          BRAINSTACK_HARNESS_TEST_TIMEOUT_MS: "5000"
        }
      );
      expectSuccess(result);
      const rendered = await readFile(configPath, "utf8");
      expect(rendered).toContain("harness:");
      expect(rendered).toContain("name: codex");
      expect(rendered).toContain("bin: codex");
      expect(rendered).toContain("enabled: true");
      expect(rendered).toContain('publicBaseUrl: "https://brain-control.example.ts.net"');
      expect(result.stdout).toContain("selected harness: codex (config bin: codex)");
      expect(result.stdout).not.toContain(fakeCodex);
      expect(result.stdout).toContain("brainctl init --profile control");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("provision requires explicit harness when codex and claude are both present non-interactively", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-provision-choice-"));
    try {
      const binDir = join(dir, "bin");
      await mkdir(binDir, { recursive: true });
      for (const name of ["codex", "claude"]) {
        const path = join(binDir, name);
        await writeFile(path, "#!/usr/bin/env sh\nexit 0\n");
        await chmod(path, 0o755);
      }
      const result = runBrainctl(["provision", "--profile", "client-macos", "--out", join(dir, "brainstack.yaml")], {
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expect(result.code).not.toBe(0);
      expect(`${result.stdout}\n${result.stderr}`).toContain("pass --harness codex or --harness claude");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("provision rejects --config so custom output paths are not silently ignored", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-provision-config-"));
    try {
      const result = runBrainctl(["provision", "--profile", "client-macos", "--config", join(dir, "brainstack.yaml")]);
      expect(result.code).not.toBe(0);
      expect(`${result.stdout}\n${result.stderr}`).toContain("provision writes a new config with --out");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("client-macos provision does not require passwordless sudo", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-provision-client-"));
    try {
      const binDir = join(dir, "bin");
      const configPath = join(dir, "brainstack.yaml");
      await mkdir(binDir, { recursive: true });
      await writeFile(join(binDir, "codex"), "#!/usr/bin/env sh\nif [ \"${1:-}\" = \"--version\" ]; then printf 'codex fake\\n'; exit 0; fi\nexit 0\n");
      await writeFile(join(binDir, "sudo"), "#!/usr/bin/env sh\necho 'sudo should not run for client-macos provision' >&2\nexit 42\n");
      await chmod(join(binDir, "codex"), 0o755);
      await chmod(join(binDir, "sudo"), 0o755);

      const result = runBrainctl(
        [
          "provision",
          "--profile",
          "client-macos",
          "--out",
          configPath,
          "--harness",
          "codex",
          "--brain-base-url",
          "https://brain-control.example.ts.net",
          "--brain-remote",
          "operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          "--skip-harness-sudo-test"
        ],
        {
          PATH: `${binDir}:${process.env.PATH || ""}`
        }
      );
      expectSuccess(result);
      expect(await readFile(configPath, "utf8")).toContain("profile: client-macos");
      expect(`${result.stdout}\n${result.stderr}`).not.toContain("sudo should not run");

      const enrollConfigPath = join(dir, "enroll.yaml");
      const enroll = runBrainctl(
        [
          "provision",
          "--profile",
          "client-macos",
          "--out",
          enrollConfigPath,
          "--harness",
          "codex",
          "--brain-base-url",
          "https://brain-control.example.ts.net",
          "--brain-remote",
          "operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          "--skip-harness-sudo-test",
          "--enroll-tailscale"
        ],
        {
          PATH: `${binDir}:${process.env.PATH || ""}`,
          TAILSCALE_AUTH_KEY: "tskey-fake-test-value"
        }
      );
      expect(enroll.code).not.toBe(0);
      expect(`${enroll.stdout}\n${enroll.stderr}`).toContain("sudo should not run for client-macos provision");
      expect(await Bun.file(enrollConfigPath).exists()).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("worker provision makes privileged sudo proof explicit", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-provision-worker-sudo-"));
    try {
      const binDir = join(dir, "bin");
      const configPath = join(dir, "worker.yaml");
      await mkdir(binDir, { recursive: true });
      await writeFile(join(binDir, "codex"), "#!/usr/bin/env sh\nif [ \"${1:-}\" = \"--version\" ]; then printf 'codex fake\\n'; exit 0; fi\nexit 0\n");
      await writeFile(join(binDir, "sudo"), "#!/usr/bin/env sh\necho 'sudo should be explicit for worker provision' >&2\nexit 42\n");
      await writeFile(join(binDir, "tailscale"), "#!/usr/bin/env sh\nexit 0\n");
      await writeFile(join(binDir, "sshd"), "#!/usr/bin/env sh\nexit 0\n");
      await chmod(join(binDir, "codex"), 0o755);
      await chmod(join(binDir, "sudo"), 0o755);
      await chmod(join(binDir, "tailscale"), 0o755);
      await chmod(join(binDir, "sshd"), 0o755);

      const result = runBrainctl(
        [
          "provision",
          "--profile",
          "worker",
          "--out",
          configPath,
          "--harness",
          "codex",
          "--brain-base-url",
          "https://brain-control.example.ts.net",
          "--brain-remote",
          "operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git"
        ],
        {
          PATH: `${binDir}:${process.env.PATH || ""}`
        }
      );
      expectSuccess(result);
      expect(await readFile(configPath, "utf8")).toContain("profile: worker");
      expect(`${result.stdout}\n${result.stderr}`).not.toContain("sudo should be explicit");

      const strict = runBrainctl(
        [
          "provision",
          "--profile",
          "worker",
          "--out",
          join(dir, "strict-worker.yaml"),
          "--harness",
          "codex",
          "--brain-base-url",
          "https://brain-control.example.ts.net",
          "--brain-remote",
          "operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          "--require-harness-sudo"
        ],
        {
          PATH: `${binDir}:${process.env.PATH || ""}`
        }
      );
      expect(strict.code).not.toBe(0);
      expect(`${strict.stdout}\n${strict.stderr}`).toContain("sudo should be explicit for worker provision");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("destroy removes rendered runtime paths but keeps canonical repos unless requested", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-destroy-"));
    try {
      const home = join(dir, "home");
      const configRoot = join(home, ".config", "brainstack");
      const stateRoot = join(home, ".local", "state", "brainstack");
      const systemdRoot = join(home, ".config", "systemd", "user");
      const sharedBrainRoot = join(home, "shared-brain");
      const privateBrainRoot = join(home, "private-brain");
      const configPath = join(dir, "brainstack.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "paths:",
          `  home: ${home}`,
          `  configRoot: ${configRoot}`,
          `  stateRoot: ${stateRoot}`,
          `  systemdUserRoot: ${systemdRoot}`,
          `  sharedBrainRoot: ${sharedBrainRoot}`,
          `  privateBrainRoot: ${privateBrainRoot}`,
          "telemux:",
          "  enabled: false",
          ""
        ].join("\n")
      );
      await mkdir(configRoot, { recursive: true });
      await mkdir(stateRoot, { recursive: true });
      await mkdir(systemdRoot, { recursive: true });
      await mkdir(sharedBrainRoot, { recursive: true });
      await mkdir(privateBrainRoot, { recursive: true });
      await writeFile(join(systemdRoot, "braind.service"), "owned\n");

      const dryRun = runBrainctl(["destroy", "--config", configPath, "--dry-run"]);
      expectSuccess(dryRun);
      expect(existsSync(configRoot)).toBe(true);
      expect(dryRun.stdout).toContain("dry-run destroy plan");

      const unsafe = runBrainctl(["destroy", "--config", configPath]);
      expect(unsafe.code).not.toBe(0);
      expect(`${unsafe.stdout}\n${unsafe.stderr}`).toContain("--yes");

      const destroy = runBrainctl(["destroy", "--config", configPath, "--yes"]);
      expectSuccess(destroy);
      expect(existsSync(configRoot)).toBe(true);
      expect(existsSync(stateRoot)).toBe(true);
      expect(existsSync(join(systemdRoot, "braind.service"))).toBe(true);
      expect(existsSync(sharedBrainRoot)).toBe(true);
      expect(existsSync(privateBrainRoot)).toBe(true);
      expect(destroy.stdout).toContain("ownership manifest missing");
      expect(destroy.stdout).toContain("manual leftovers");
      expect(destroy.stdout).toContain("pass --remove-shared-brain");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("destroy only targets services owned by the selected profile", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-destroy-profile-"));
    try {
      const home = join(dir, "home");
      const systemdRoot = join(home, ".config", "systemd", "user");
      const configPath = join(dir, "client.yaml");
      await mkdir(systemdRoot, { recursive: true });
      await writeFile(join(systemdRoot, "telemux.service"), "unrelated\n");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${home}`,
          `  systemdUserRoot: ${systemdRoot}`,
          ""
        ].join("\n")
      );

      const result = runBrainctl(["destroy", "--config", configPath, "--profile", "client-macos", "--dry-run"]);
      expectSuccess(result);
      expect(result.stdout).not.toContain("telemux.service");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("init is fresh-install only and upgrade does not rewrite canonical content", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-init-safety-"));
    try {
      const configPath = join(dir, "config.yaml");
      const root = join(dir, "install");
      await writeFixtureConfig(configPath);

      expectSuccess(runBrainctl(["init", "--profile", "single-node", "--config", configPath, "--root", root]));
      const staging = join(root, "shared-brain", "staging", "shared-brain");
      const configRoot = join(root, "config");
      const headBefore = git(["rev-parse", "HEAD"], staging);
      const homeBefore = await readFile(join(staging, "wiki", "Home.md"), "utf8");
      const seededClaude = await readFile(join(staging, "CLAUDE.md"), "utf8");
      expect(seededClaude).toContain("@AGENTS.md");
      expect(seededClaude).toContain("Claude-specific delta");
      expect(seededClaude).not.toContain("Import and follow");
      expect(await Bun.file(join(configRoot, "braind.env")).exists()).toBe(false);
      expect(await Bun.file(join(configRoot, "braind.runtime.env")).exists()).toBe(true);
      expect(await Bun.file(join(configRoot, "braind.secrets.env")).exists()).toBe(true);
      expect(await Bun.file(join(configRoot, "managed-artifacts.json")).exists()).toBe(true);
      await writeFile(join(configRoot, "braind.runtime.env"), "BRAIN_BIND=0.0.0.0\n");
      await writeFile(join(configRoot, "braind.secrets.env"), "BRAIN_IMPORT_TOKEN=operator-owned\nBRAIN_ADMIN_TOKEN=operator-owned-admin\n");

      const rerun = runBrainctl(["init", "--profile", "single-node", "--config", configPath, "--root", root]);
      expect(rerun.code).not.toBe(0);
      expect(`${rerun.stdout}\n${rerun.stderr}`).toContain("fresh-install only");
      expect(git(["rev-parse", "HEAD"], staging)).toBe(headBefore);
      expect(await readFile(join(staging, "wiki", "Home.md"), "utf8")).toBe(homeBefore);

      expectSuccess(runBrainctl(["upgrade", "--profile", "single-node", "--config", configPath, "--root", root]));
      expect(git(["rev-parse", "HEAD"], staging)).toBe(headBefore);
      expect(await readFile(join(staging, "wiki", "Home.md"), "utf8")).toBe(homeBefore);
      expect(git(["status", "--porcelain"], staging)).toBe("");
      expect(await readFile(join(configRoot, "braind.runtime.env"), "utf8")).toContain("BRAIN_BIND=127.0.0.1");
      expect(await readFile(join(configRoot, "braind.secrets.env"), "utf8")).toContain("BRAIN_IMPORT_TOKEN=operator-owned");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("worker init bootstraps a shared-brain client without braind or admin token", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-worker-init-"));
    try {
      const home = join(dir, "home");
      const configRoot = join(home, ".config", "brainstack");
      const stateRoot = join(home, ".local", "state", "brainstack");
      const bare = join(dir, "shared-brain.git");
      const seed = join(dir, "seed");
      const configPath = join(dir, "worker.yaml");
      git(["init", "--bare", "--initial-branch=main", bare], dir);
      git(["clone", bare, seed], dir);
      await writeFile(join(seed, "AGENTS.shared-client.md"), "# Shared Client\n");
      git(["add", "AGENTS.shared-client.md"], seed);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "seed"], seed);
      git(["push", "-u", "origin", "main"], seed);

      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: worker",
          "machine:",
          "  name: brain-worker",
          "  user: operator",
          "paths:",
          `  home: ${home}`,
          `  configRoot: ${configRoot}`,
          `  stateRoot: ${stateRoot}`,
          "client:",
          "  localPath: ~/shared-brain",
          `  remoteSsh: ${bare}`,
          "brain:",
          "  publicBaseUrl: https://brain-control.example.ts.net",
          ""
        ].join("\n")
      );

      expectSuccess(runBrainctl(["init", "--profile", "worker", "--config", configPath], { BRAIN_IMPORT_TOKEN: "test-import-token" }));
      expect(await Bun.file(join(home, "shared-brain", "AGENTS.shared-client.md")).exists()).toBe(true);
      expect(await Bun.file(join(configRoot, "client-bootstrap", "codex-shared-brain.include.md")).exists()).toBe(true);
      expect(await Bun.file(join(home, ".codex", "AGENTS.md")).exists()).toBe(true);
      expect(await readFile(join(home, ".config", "shared-brain.env"), "utf8")).toContain("BRAIN_IMPORT_TOKEN=test-import-token");
      expect(await readFile(join(home, ".config", "shared-brain.env"), "utf8")).not.toContain("BRAIN_ADMIN_TOKEN");
      expect(await Bun.file(join(configRoot, "braind.runtime.env")).exists()).toBe(false);
      expect(await Bun.file(join(configRoot, "braind.secrets.env")).exists()).toBe(false);
      expect(await Bun.file(join(configRoot, "telemux.secrets.env")).exists()).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("worker init prints exact manual merge instructions when user agent files already exist", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-worker-existing-agent-files-"));
    try {
      const home = join(dir, "home");
      const configRoot = join(home, ".config", "brainstack");
      const stateRoot = join(home, ".local", "state", "brainstack");
      const bare = join(dir, "shared-brain.git");
      const seed = join(dir, "seed");
      const configPath = join(dir, "worker.yaml");
      git(["init", "--bare", "--initial-branch=main", bare], dir);
      git(["clone", bare, seed], dir);
      await writeFile(join(seed, "AGENTS.shared-client.md"), "# Shared Client\n");
      git(["add", "AGENTS.shared-client.md"], seed);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "seed"], seed);
      git(["push", "-u", "origin", "main"], seed);

      await mkdir(join(home, ".codex"), { recursive: true });
      await mkdir(join(home, ".claude"), { recursive: true });
      await mkdir(join(home, ".cursor", "rules"), { recursive: true });
      await writeFile(join(home, ".codex", "AGENTS.md"), "# Existing Codex\n");
      await writeFile(join(home, ".claude", "CLAUDE.md"), "# Existing Claude\n");
      await writeFile(join(home, ".cursor", "rules", "shared-brain.md"), "# Existing Cursor\n");

      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: worker",
          "machine:",
          "  name: brain-worker",
          "  user: operator",
          "paths:",
          `  home: ${home}`,
          `  configRoot: ${configRoot}`,
          `  stateRoot: ${stateRoot}`,
          "client:",
          "  localPath: ~/shared-brain",
          `  remoteSsh: ${bare}`,
          "brain:",
          "  publicBaseUrl: https://brain-control.example.ts.net",
          ""
        ].join("\n")
      );

      const result = runBrainctl(["init", "--profile", "worker", "--config", configPath]);
      expectSuccess(result);
      expect(result.stdout).toContain(`Codex already has ${join(home, ".codex", "AGENTS.md")}`);
      expect(result.stdout).toContain(`cat ${join(configRoot, "client-bootstrap", "codex-shared-brain.include.md")} >> ${join(home, ".codex", "AGENTS.md")}`);
      expect(result.stdout).toContain(`Claude already has ${join(home, ".claude", "CLAUDE.md")}`);
      expect(result.stdout).toContain(`@${join(configRoot, "client-bootstrap", "claude-user-CLAUDE.md")}`);
      expect(result.stdout).toContain(`Cursor shared-brain rule already exists at ${join(home, ".cursor", "rules", "shared-brain.md")}`);
      expect(result.stdout).toContain(`cat ${join(configRoot, "client-bootstrap", "cursor-user-rule.md")} >> ${join(home, ".cursor", "rules", "shared-brain.md")}`);
      expect(await readFile(join(home, ".codex", "AGENTS.md"), "utf8")).toBe("# Existing Codex\n");
      expect(await readFile(join(home, ".claude", "CLAUDE.md"), "utf8")).toBe("# Existing Claude\n");
      expect(await readFile(join(home, ".cursor", "rules", "shared-brain.md"), "utf8")).toBe("# Existing Cursor\n");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("destroy dry-run and yes clean a disposable install for rerun", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-clean-retry-"));
    try {
      const configPath = join(dir, "config.yaml");
      const root = join(dir, "install");
      await writeFixtureConfig(configPath);
      expectSuccess(runBrainctl(["init", "--profile", "single-node", "--config", configPath, "--root", root]));
      const manifest = join(root, "config", "managed-artifacts.json");
      expect(await Bun.file(manifest).exists()).toBe(true);

      const dryRun = runBrainctl([
        "destroy",
        "--profile",
        "single-node",
        "--config",
        configPath,
        "--root",
        root,
        "--dry-run",
        "--remove-shared-brain"
      ]);
      expectSuccess(dryRun);
      expect(dryRun.stdout).toContain("dry-run destroy plan");
      expect(dryRun.stdout).toContain("managed-artifacts.json");
      expect(await Bun.file(manifest).exists()).toBe(true);

      expectSuccess(
        runBrainctl([
          "destroy",
          "--profile",
          "single-node",
          "--config",
          configPath,
          "--root",
          root,
          "--yes",
          "--remove-shared-brain"
        ])
      );
      expect(await Bun.file(join(root, "config")).exists()).toBe(false);
      expect(await Bun.file(join(root, "state")).exists()).toBe(false);
      expect(await Bun.file(join(root, "shared-brain")).exists()).toBe(false);
      expectSuccess(runBrainctl(["init", "--profile", "single-node", "--config", configPath, "--root", root]));
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 15_000);
});

describe("braind write safety", () => {
  test("staging clone fast-forwards after a direct push to the bare repo", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-sync-"));
    try {
      const bare = join(dir, "shared-brain.git");
      const staging = join(dir, "staging");
      const direct = join(dir, "direct");

      git(["init", "--bare", "--initial-branch=main", bare], dir);
      git(["clone", bare, staging], dir);
      await writeFile(join(staging, "README.md"), "# Shared Brain\n");
      git(["add", "README.md"], staging);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "seed"], staging);
      git(["push", "-u", "origin", "main"], staging);

      git(["clone", bare, direct], dir);
      await mkdir(join(direct, "wiki"), { recursive: true });
      await writeFile(join(direct, "wiki", "Index.md"), "# Index\n\nDirect push content.\n");
      git(["add", "wiki/Index.md"], direct);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "direct push"], direct);
      git(["push", "origin", "HEAD:main"], direct);

      syncWritableRepo(staging);
      expect(await readFile(join(staging, "wiki", "Index.md"), "utf8")).toContain("Direct push content");
      expect(git(["rev-parse", "HEAD"], staging)).toBe(git(["rev-parse", "origin/main"], staging));
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("URL import blocks private network targets with a clear client error", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-private-url-"));
    const port = 19_000 + Math.floor(Math.random() * 10_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const configPath = join(dir, "config.yaml");
      const root = join(dir, "install");
      await writeFixtureConfig(configPath);
      expectSuccess(runBrainctl(["init", "--profile", "single-node", "--config", configPath, "--root", root]));

      proc = Bun.spawn(["bun", "run", SERVER], {
        cwd: PRODUCT_ROOT,
        env: {
          ...process.env,
          BRAIN_BIND: "127.0.0.1",
          BRAIN_PORT: String(port),
          BRAIN_IMPORT_TOKEN: "import-test-token",
          BRAIN_ADMIN_TOKEN: "admin-test-token",
          BRAIN_ALLOW_PRIVATE_URL_IMPORTS: "false",
          SHARED_BRAIN_REPO_ROOT: join(root, "shared-brain", "serve", "shared-brain"),
          SHARED_BRAIN_WRITE_REPO_ROOT: join(root, "shared-brain", "staging", "shared-brain"),
          BRAIN_BLOB_STORE: join(root, "state", "blobs", "shared-brain")
        },
        stdout: "pipe",
        stderr: "pipe"
      });

      let serverReady = false;
      for (let attempt = 0; attempt < 50; attempt += 1) {
        try {
          const health = await fetch(`http://127.0.0.1:${port}/health`);
          if (health.ok) {
            serverReady = true;
            break;
          }
        } catch {
          // wait for server startup
        }
        await Bun.sleep(100);
      }
      expect(serverReady).toBe(true);

      const response = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          title: "Blocked private URL",
          url: "http://127.0.0.1:1/private",
          source_harness: "test-harness",
          source_machine: "test-machine",
          source_type: "url"
        })
      });
      expect(response.status).toBe(400);
      const payload = (await response.json()) as { error?: string };
      expect(payload.error || "").toContain("blocked private address");
    } finally {
      if (proc) {
        proc.kill();
        await proc.exited;
      }
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("URL import rejects oversize streamed bodies without content-length", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-stream-cap-"));
    const braindPort = 29_000 + Math.floor(Math.random() * 5_000);
    const sourcePort = 34_000 + Math.floor(Math.random() * 5_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    let sourceServer: Server | null = null;
    try {
      sourceServer = createServer((socket) => {
        socket.write("HTTP/1.1 200 OK\r\n");
        socket.write("Content-Type: text/plain\r\n");
        socket.write("Transfer-Encoding: chunked\r\n");
        socket.write("\r\n");
        socket.write(`40\r\n${"x".repeat(64)}\r\n`);
        socket.write("0\r\n\r\n");
        socket.end();
      });
      await new Promise<void>((resolveListen) => sourceServer?.listen(sourcePort, "127.0.0.1", resolveListen));

      const configPath = join(dir, "config.yaml");
      const root = join(dir, "install");
      await writeFixtureConfig(configPath);
      expectSuccess(runBrainctl(["init", "--profile", "single-node", "--config", configPath, "--root", root]));

      proc = Bun.spawn(["bun", "run", SERVER], {
        cwd: PRODUCT_ROOT,
        env: {
          ...process.env,
          BRAIN_BIND: "127.0.0.1",
          BRAIN_PORT: String(braindPort),
          BRAIN_IMPORT_TOKEN: "import-test-token",
          BRAIN_ADMIN_TOKEN: "admin-test-token",
          BRAIN_ALLOW_PRIVATE_URL_IMPORTS: "true",
          BRAIN_MAX_IMPORT_BYTES: "16",
          SHARED_BRAIN_REPO_ROOT: join(root, "shared-brain", "serve", "shared-brain"),
          SHARED_BRAIN_WRITE_REPO_ROOT: join(root, "shared-brain", "staging", "shared-brain"),
          BRAIN_BLOB_STORE: join(root, "state", "blobs", "shared-brain")
        },
        stdout: "pipe",
        stderr: "pipe"
      });

      let serverReady = false;
      for (let attempt = 0; attempt < 50; attempt += 1) {
        try {
          const health = await fetch(`http://127.0.0.1:${braindPort}/health`);
          if (health.ok) {
            serverReady = true;
            break;
          }
        } catch {
          // wait for server startup
        }
        await Bun.sleep(100);
      }
      expect(serverReady).toBe(true);

      const response = await fetch(`http://127.0.0.1:${braindPort}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          title: "Oversize stream",
          url: `http://127.0.0.1:${sourcePort}/stream`,
          source_harness: "test-harness",
          source_machine: "test-machine",
          source_type: "url"
        })
      });
      expect(response.status).toBe(413);
      const payload = (await response.json()) as { error?: string };
      expect(payload.error || "").toContain("streamed response exceeds");
    } finally {
      await new Promise<void>((resolveClose) => sourceServer?.close(() => resolveClose()) || resolveClose());
      if (proc) {
        proc.kill();
        await proc.exited;
      }
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("lint rejects request bodies before mutation", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-lint-body-cap-"));
    const port = 44_000 + Math.floor(Math.random() * 1_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port, { BRAIN_MAX_JSON_BYTES: "8" });
      const response = await fetch(`http://127.0.0.1:${port}/api/lint`, {
        method: "POST",
        headers: {
          Authorization: "Bearer admin-test-token",
          "Content-Type": "application/json"
        },
        body: new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode("0123456789abcdef"));
            controller.close();
          }
        })
      });
      expect(response.status).toBe(413);
      expect(await response.text()).toContain("lint does not accept a request body");
    } finally {
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("fresh install homepage does not render dead default page links", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-home-links-"));
    const port = 39_000 + Math.floor(Math.random() * 5_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const configPath = join(dir, "config.yaml");
      const root = join(dir, "install");
      await writeFixtureConfig(configPath);
      expectSuccess(runBrainctl(["init", "--profile", "single-node", "--config", configPath, "--root", root]));

      proc = Bun.spawn(["bun", "run", SERVER], {
        cwd: PRODUCT_ROOT,
        env: {
          ...process.env,
          BRAIN_BIND: "127.0.0.1",
          BRAIN_PORT: String(port),
          BRAIN_IMPORT_TOKEN: "import-test-token",
          BRAIN_ADMIN_TOKEN: "admin-test-token",
          SHARED_BRAIN_REPO_ROOT: join(root, "shared-brain", "serve", "shared-brain"),
          SHARED_BRAIN_WRITE_REPO_ROOT: join(root, "shared-brain", "staging", "shared-brain"),
          BRAIN_BLOB_STORE: join(root, "state", "blobs", "shared-brain")
        },
        stdout: "pipe",
        stderr: "pipe"
      });

      let html = "";
      for (let attempt = 0; attempt < 50; attempt += 1) {
        try {
          const response = await fetch(`http://127.0.0.1:${port}/`);
          if (response.ok) {
            html = await response.text();
            break;
          }
        } catch {
          // wait for server startup
        }
        await Bun.sleep(100);
      }
      expect(html).toContain("Shared Brain");
      expect(html).not.toContain("Legacy-Control.md");
      expect(html).not.toContain("Local-Codex.md");
      expect(html).not.toContain("Shared-Brain-v1.md");

      const serveRoot = join(root, "shared-brain", "serve", "shared-brain");
      expect(html).not.toContain(serveRoot);
      const pageLinks = [...html.matchAll(/href="\/page\/([^"]+)"/g)].map((match) => decodeURIComponent(match[1]));
      expect(pageLinks.length).toBeGreaterThan(0);
      const deadLinks = pageLinks.filter((repoPath) => !existsSync(join(serveRoot, repoPath)));
      expect(deadLinks).toEqual([]);
    } finally {
      if (proc) {
        proc.kill();
        await proc.exited;
      }
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("braind rejects legacy token collapse at startup", () => {
    const legacy = runCommand(["bun", "run", SERVER], {
      env: {
        BRAIN_WRITE_TOKEN: "legacy-write-token",
        BRAIN_IMPORT_TOKEN: "import-test-token",
        BRAIN_ADMIN_TOKEN: "admin-test-token"
      }
    });
    expect(legacy.code).not.toBe(0);
    expect(legacy.stderr).toContain("BRAIN_WRITE_TOKEN is no longer accepted");

    const collapsed = runCommand(["bun", "run", SERVER], {
      env: {
        BRAIN_WRITE_TOKEN: "",
        BRAIN_IMPORT_TOKEN: "same-token",
        BRAIN_ADMIN_TOKEN: "same-token"
      }
    });
    expect(collapsed.code).not.toBe(0);
    expect(collapsed.stderr).toContain("BRAIN_IMPORT_TOKEN and BRAIN_ADMIN_TOKEN must be distinct");

    const padded = runCommand(["bun", "run", SERVER], {
      env: {
        BRAIN_IMPORT_TOKEN: " import-test-token ",
        BRAIN_ADMIN_TOKEN: "admin-test-token"
      }
    });
    expect(padded.code).not.toBe(0);
    expect(padded.stderr).toContain("BRAIN_IMPORT_TOKEN must not contain leading or trailing whitespace");
  });

  test("braind enforces local security posture at startup", async () => {
    const blocked = runCommand(["bun", "run", SERVER], {
      env: {
        BRAIN_IMPORT_TOKEN: "import-test-token",
        BRAIN_ADMIN_TOKEN: "admin-test-token",
        BRAIN_SECURITY_POSTURE: "local",
        BRAIN_BIND: "0.0.0.0"
      }
    });
    expect(blocked.code).not.toBe(0);
    expect(blocked.stderr).toContain("BRAIN_SECURITY_POSTURE=local requires BRAIN_BIND to be loopback");

    const padded = runCommand(["bun", "run", SERVER], {
      env: {
        BRAIN_IMPORT_TOKEN: "import-test-token",
        BRAIN_ADMIN_TOKEN: "admin-test-token",
        BRAIN_SECURITY_POSTURE: " Local ",
        BRAIN_BIND: "0.0.0.0"
      }
    });
    expect(padded.code).not.toBe(0);
    expect(padded.stderr).toContain("BRAIN_SECURITY_POSTURE=local requires BRAIN_BIND to be loopback");

    const unknown = runCommand(["bun", "run", SERVER], {
      env: {
        BRAIN_IMPORT_TOKEN: "import-test-token",
        BRAIN_ADMIN_TOKEN: "admin-test-token",
        BRAIN_SECURITY_POSTURE: "public"
      }
    });
    expect(unknown.code).not.toBe(0);
    expect(unknown.stderr).toContain("Unsupported BRAIN_SECURITY_POSTURE");
  });

  test("pre-mutation idempotent import failures can be retried with the same key", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-idempotency-preflight-"));
    const port = 44_000 + Math.floor(Math.random() * 1_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port);
      const staging = join(root, "shared-brain", "staging", "shared-brain");
      const dirtyPath = join(staging, "wiki", "Home.md");
      const cleanHome = await readFile(dirtyPath, "utf8");
      await writeFile(dirtyPath, `${cleanHome}\nDirty preflight marker.\n`);
      const payload = {
        title: "Preflight retry",
        text: "preflight retry body",
        source_harness: "codex",
        source_machine: "client",
        source_type: "note",
        tags: [] as string[]
      };
      const first = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": "preflight-retry"
        },
        body: JSON.stringify(payload)
      });
      expect(first.status).toBe(500);
      await writeFile(dirtyPath, cleanHome);
      const second = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": "preflight-retry"
        },
        body: JSON.stringify(payload)
      });
      expect(second.status).toBe(200);
      const body = (await second.json()) as Record<string, unknown>;
      expect(typeof body.artifact_id).toBe("string");
      expect(body.idempotent_replay).toBeUndefined();
    } finally {
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("expired pre-mutation idempotency locks with unreadable owners require review", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-idempotency-unreadable-owner-"));
    const port = 44_000 + Math.floor(Math.random() * 1_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port, {
        BRAIN_IDEMPOTENCY_LOCK_WAIT_MS: "100",
        BRAIN_IDEMPOTENCY_CLAIM_LEASE_MS: "100"
      });
      const payload = {
        title: "Unreadable Owner",
        text: "This request has a corrupted pre-mutation lock owner.",
        source_harness: "codex",
        source_machine: "client",
        source_type: "note",
        tags: [] as string[]
      };
      const textBytes = new TextEncoder().encode(payload.text);
      const input = {
        title: payload.title,
        text: payload.text,
        bytes: textBytes,
        fileName: "unreadable-owner.md",
        contentType: "text/markdown",
        source_harness: payload.source_harness,
        source_machine: payload.source_machine,
        source_type: payload.source_type,
        related_project: undefined,
        related_repo: undefined,
        conversation_id: undefined,
        tags: payload.tags,
        ingest_now: false
      };
      const key = "unreadable-owner-key";
      const keyHash = sha256Text(key);
      const requestHash = sha256Text(canonicalJson({ endpoint: "import", input }));
      const writeRoot = join(root, "shared-brain", "staging", "shared-brain");
      const recordPath = join(writeRoot, "derived", "idempotency", "import", `${keyHash}.json`);
      const lockDir = `${recordPath}.lock`;
      const ownerId = "00000000-0000-0000-0000-000000000050";
      const old = new Date(Date.now() - 60_000).toISOString();
      await mkdir(lockDir, { recursive: true });
      await writeFile(join(lockDir, `owner-${ownerId}.json`), "{");
      await writeFile(join(lockDir, `release-${ownerId}`), ownerId);
      await writeFile(
        recordPath,
        `${JSON.stringify(
          {
            endpoint: "import",
            key_hash: keyHash,
            request_hash: requestHash,
            status: "claimed",
            created_at: old,
            updated_at: old,
            lease_until: old
          },
          null,
          2
        )}\n`
      );

      const response = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": key
        },
        body: JSON.stringify(payload)
      });
      expect(response.status).toBe(409);
      const record = JSON.parse(await readFile(recordPath, "utf8")) as { status?: string; error?: string };
      expect(record.status).toBe("review_required");
      expect(record.error || "").toContain("could not be proven safe to clear");
      expect(existsSync(lockDir)).toBe(true);
    } finally {
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("completed idempotent import replays bypass the new-write rate limiter", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-idempotency-rate-"));
    const port = 44_000 + Math.floor(Math.random() * 1_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port, { BRAIN_WRITE_RATE_LIMIT_PER_MINUTE: "1" });
      const payload = {
        title: "Rate replay",
        text: "rate replay body",
        source_harness: "codex",
        source_machine: "client",
        source_type: "note",
        tags: [] as string[]
      };
      const first = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": "rate-replay"
        },
        body: JSON.stringify(payload)
      });
      expect(first.status).toBe(200);
      const replay = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": "rate-replay"
        },
        body: JSON.stringify(payload)
      });
      expect(replay.status).toBe(200);
      const body = (await replay.json()) as Record<string, unknown>;
      expect(body.idempotent_replay).toBe(true);
    } finally {
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("braind queues concurrent mutations instead of rejecting ready writes", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-mutation-queue-"));
    const port = 44_000 + Math.floor(Math.random() * 1_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port, { BRAIN_WRITE_CONCURRENCY: "1", BRAIN_WRITE_QUEUE_WAIT_MS: "5000" });
      const staging = join(root, "shared-brain", "staging", "shared-brain");
      const lockDir = join(staging, ".shared-brain.lock");
      await mkdir(lockDir);
      setTimeout(() => {
        void rm(lockDir, { recursive: true, force: true });
      }, 300);

      const makePayload = (index: number) => ({
        title: `Queued mutation ${index}`,
        text: `queued mutation body ${index}`,
        source_harness: "codex",
        source_machine: `client-${index}`,
        source_type: "note",
        tags: [] as string[]
      });
      const send = (index: number) =>
        fetch(`http://127.0.0.1:${port}/api/import`, {
          method: "POST",
          headers: {
            Authorization: "Bearer import-test-token",
            "Content-Type": "application/json"
          },
          body: JSON.stringify(makePayload(index))
        });

      const [first, second] = await Promise.all([send(1), send(2)]);
      expect(first.status).toBe(200);
      expect(second.status).toBe(200);
      const firstBody = (await first.json()) as Record<string, unknown>;
      const secondBody = (await second.json()) as Record<string, unknown>;
      expect(typeof firstBody.artifact_id).toBe("string");
      expect(typeof secondBody.artifact_id).toBe("string");
      expect(secondBody.artifact_id).not.toBe(firstBody.artifact_id);
    } finally {
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("slow URL import preparation does not hold the mutation gate for ready text imports", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-url-prep-gate-"));
    const port = 44_000 + Math.floor(Math.random() * 1_000);
    const sourcePort = 46_000 + Math.floor(Math.random() * 1_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    let releaseSource: (() => void) | null = null;
    let sourceRequests = 0;
    const sourceServer = Bun.serve({
      hostname: "127.0.0.1",
      port: sourcePort,
      async fetch() {
        sourceRequests += 1;
        await new Promise<void>((resolve) => {
          releaseSource = resolve;
        });
        return new Response("slow url body", { headers: { "content-type": "text/plain" } });
      }
    });
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port, {
        BRAIN_ALLOW_PRIVATE_URL_IMPORTS: "true",
        BRAIN_WRITE_CONCURRENCY: "1",
        BRAIN_IMPORT_PREPARATION_CONCURRENCY: "1",
        BRAIN_WRITE_QUEUE_WAIT_MS: "300"
      });
      const slowUrlImport = fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          title: "Slow URL",
          url: `http://127.0.0.1:${sourcePort}/slow.txt`,
          source_harness: "codex",
          source_machine: "url-client",
          source_type: "url"
        })
      });
      await waitForCondition(async () => sourceRequests === 1, "slow URL source request", 40);
      const secondSlowUrlImport = fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          title: "Second Slow URL",
          url: `http://127.0.0.1:${sourcePort}/second-slow.txt`,
          source_harness: "codex",
          source_machine: "url-client-2",
          source_type: "url"
        })
      });
      const textImport = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          title: "Ready text",
          text: "ready text import should not wait for slow URL fetch",
          source_harness: "codex",
          source_machine: "text-client",
          source_type: "note"
        })
      });
      expect(textImport.status).toBe(200);
      const blockedUrl = await secondSlowUrlImport;
      expect(blockedUrl.status).toBe(503);
      expect(await blockedUrl.text()).toContain("import preparation queue timed out");
      expect(sourceRequests).toBe(1);
      releaseSource?.();
      expect((await slowUrlImport).status).toBe(200);
    } finally {
      releaseSource?.();
      sourceServer.stop(true);
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("write-rate source cache is bounded while preserving per-source quotas", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-rate-cache-"));
    const port = 44_000 + Math.floor(Math.random() * 1_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port, {
        BRAIN_WRITE_RATE_LIMIT_PER_MINUTE: "1",
        BRAIN_WRITE_RATE_LIMIT_MAX_KEYS: "2"
      });
      const send = async (source: string, suffix: string) =>
        await fetch(`http://127.0.0.1:${port}/api/import`, {
          method: "POST",
          headers: {
            Authorization: "Bearer import-test-token",
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            title: `Rate cache ${source} ${suffix}`,
            text: `rate cache body ${source} ${suffix}`,
            source_harness: "codex",
            source_machine: source,
            source_type: "note",
            tags: [] as string[]
          })
        });

      expect((await send("source-a", "1")).status).toBe(200);
      expect((await send("source-b", "1")).status).toBe(200);
      expect((await send("source-c", "1")).status).toBe(200);
      expect((await send("source-a", "2")).status).toBe(200);
      expect((await send("source-c", "2")).status).toBe(429);
    } finally {
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("write-rate limiting has a token-level quota across rotated sources", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-rate-token-"));
    const port = 44_000 + Math.floor(Math.random() * 1_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port, {
        BRAIN_WRITE_RATE_LIMIT_PER_MINUTE: "10",
        BRAIN_WRITE_TOKEN_RATE_LIMIT_PER_MINUTE: "2"
      });
      const send = async (source: string) =>
        await fetch(`http://127.0.0.1:${port}/api/import`, {
          method: "POST",
          headers: {
            Authorization: "Bearer import-test-token",
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            title: `Token quota ${source}`,
            text: `token quota body ${source}`,
            source_harness: "codex",
            source_machine: source,
            source_type: "note",
            tags: [] as string[]
          })
        });

      expect((await send("source-a")).status).toBe(200);
      expect((await send("source-b")).status).toBe(200);
      expect((await send("source-c")).status).toBe(429);
    } finally {
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  }, 12_000);

  test("pre-body token quota rejects import floods before request parsing", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-prebody-rate-"));
    const port = 44_000 + Math.floor(Math.random() * 1_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port, {
        BRAIN_WRITE_TOKEN_RATE_LIMIT_PER_MINUTE: "1",
        BRAIN_WRITE_RATE_LIMIT_PER_MINUTE: "100"
      });
      const send = (title: string) =>
        fetch(`http://127.0.0.1:${port}/api/import`, {
          method: "POST",
          headers: {
            Authorization: "Bearer import-test-token",
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            title,
            text: `body ${title}`,
            source_harness: "codex",
            source_machine: "client",
            source_type: "note",
            tags: [] as string[]
          })
        });

      expect((await send("prebody-one")).status).toBe(200);
      const rejected = await send("prebody-two");
      expect(rejected.status).toBe(429);
      const body = (await rejected.json()) as { error?: string };
      expect(body.error || "").toContain("pre-body write rate limit");
    } finally {
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("braind converts expired running idempotency records with persisted locks into operator review", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-idempotency-stuck-"));
    const port = 45_000 + Math.floor(Math.random() * 3_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port, {
        BRAIN_IDEMPOTENCY_LOCK_WAIT_MS: "100",
        BRAIN_IDEMPOTENCY_RUNNING_LEASE_MS: "100"
      });

      const payload = {
        title: "Stuck Running",
        text: "This request simulates a crash after side effects started.",
        source_harness: "test-harness",
        source_machine: "test-machine",
        source_type: "note",
        tags: [] as string[]
      };
      const textBytes = new TextEncoder().encode(payload.text);
      const input = {
        title: payload.title,
        text: payload.text,
        bytes: textBytes,
        fileName: "stuck-running.md",
        contentType: "text/markdown",
        source_harness: payload.source_harness,
        source_machine: payload.source_machine,
        source_type: payload.source_type,
        related_project: undefined,
        related_repo: undefined,
        conversation_id: undefined,
        tags: payload.tags,
        ingest_now: false
      };
      const key = "stuck-running-key";
      const keyHash = sha256Text(key);
      const requestHash = sha256Text(canonicalJson({ endpoint: "import", input }));
      const writeRoot = join(root, "shared-brain", "staging", "shared-brain");
      const recordPath = join(writeRoot, "derived", "idempotency", "import", `${keyHash}.json`);
      const lockDir = `${recordPath}.lock`;
      const old = new Date(Date.now() - 60_000).toISOString();
      await mkdir(lockDir, { recursive: true });
      await writeFile(join(lockDir, "owner-test.json"), `${JSON.stringify({ token: "test", pid: 999999, created_at: old })}\n`);
      await writeFile(join(lockDir, "release-test"), "test");
      await writeFile(
        recordPath,
        `${JSON.stringify(
          {
            endpoint: "import",
            key_hash: keyHash,
            request_hash: requestHash,
            status: "running",
            created_at: old,
            updated_at: old,
            lease_until: old,
            side_effect_started_at: old
          },
          null,
          2
        )}\n`
      );

      const response = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": key
        },
        body: JSON.stringify(payload)
      });
      expect(response.status).toBe(409);
      const body = (await response.json()) as { error?: string };
      expect(body.error || "").toContain("operator review");
      const record = JSON.parse(await readFile(recordPath, "utf8")) as { status?: string; error?: string };
      expect(record.status).toBe("review_required");
      expect(record.error || "").toContain("persisted past its lease");
    } finally {
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("braind escapes search snippets, allowlists external links, and serves active raw files inertly", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-content-safety-"));
    const port = 44_000 + Math.floor(Math.random() * 4_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    let sourceServer: ReturnType<typeof Bun.serve> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port, { BRAIN_ALLOW_PRIVATE_URL_IMPORTS: "true", BRAIN_WRITE_CONCURRENCY: "2" });

      const payload = {
        title: "Needle Attack",
        text: [
          "# Needle Attack",
          "",
          "Needle <img src=x onerror=alert(1)> should be visible as text only.",
          "[blocked](javascript:alert(1)) and [allowed](https://example.com/ok)."
        ].join("\n"),
        source_harness: "test-harness",
        source_machine: "test-machine",
        source_type: "note"
      };
      const importResponse = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": "content-safety-note"
        },
        body: JSON.stringify(payload)
      });
      expect(importResponse.status).toBe(200);
      const imported = (await importResponse.json()) as {
        artifact_id?: string;
        touched_files?: string[];
      };
      expect(typeof imported.artifact_id).toBe("string");

      const replayResponse = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": "content-safety-note"
        },
        body: JSON.stringify(payload)
      });
      expect(replayResponse.status).toBe(200);
      const replayed = (await replayResponse.json()) as {
        artifact_id?: string;
        idempotent_replay?: boolean;
      };
      expect(replayed.artifact_id).toBe(imported.artifact_id);
      expect(replayed.idempotent_replay).toBe(true);

      const healthResponse = await fetch(`http://127.0.0.1:${port}/healthz`);
      expect(healthResponse.status).toBe(200);
      const health = (await healthResponse.json()) as Record<string, unknown>;
      expect(health).toEqual({ ok: true, service: "braind", version: "0.1.0" });
      const readyResponse = await fetch(`http://127.0.0.1:${port}/readyz`);
      expect([200, 503]).toContain(readyResponse.status);
      const ready = (await readyResponse.json()) as Record<string, unknown>;
      expect(ready).toMatchObject({ ok: true, service: "braind", version: "0.1.0" });
      expect(ready).toHaveProperty("search_ready");
      const adminHealthResponse = await fetch(`http://127.0.0.1:${port}/admin/health`, {
        headers: { Authorization: "Bearer admin-test-token" }
      });
      expect(adminHealthResponse.status).toBe(200);
      const adminHealth = (await adminHealthResponse.json()) as Record<string, unknown>;
      expect(adminHealth.write_repo_root).toBe(join(root, "shared-brain", "staging", "shared-brain"));
      expect((adminHealth.write_repo_lock as { present?: boolean }).present).toBe(false);
      expect(Array.isArray(adminHealth.idempotency_locks)).toBe(true);
      expect(typeof (adminHealth.pending_reindex as { present?: boolean }).present).toBe("boolean");
      const pendingReindexPath = join(root, "shared-brain", "serve", "shared-brain", "derived", "search-reindex-needed.json");
      await mkdir(dirname(pendingReindexPath), { recursive: true });
      await writeFile(
        pendingReindexPath,
        `${JSON.stringify({ commit: "abc123", error: `private path ${join(root, "secret")}`, updated_at: new Date().toISOString() })}\n`
      );
      const publicSearch = await fetch(`http://127.0.0.1:${port}/search?format=json&q=Needle`);
      expect(publicSearch.status).toBe(200);
      const publicSearchText = await publicSearch.text();
      expect(publicSearchText).toContain('"search_freshness"');
      expect(publicSearchText).toContain('"present": true');
      expect(publicSearchText).not.toContain(pendingReindexPath);
      expect(publicSearchText).not.toContain("private path");

      const conflictResponse = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": "content-safety-note"
        },
        body: JSON.stringify({ ...payload, text: `${payload.text}\nchanged` })
      });
      expect(conflictResponse.status).toBe(409);

      const staleRunningKey = "content-safety-stale-running";
      const staleRunningPayload = {
        ...payload,
        title: "Stale running idempotency note",
        text: `${payload.text}\nThis simulates a crash after side effects started.`
      };
      const staleInitial = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": staleRunningKey
        },
        body: JSON.stringify(staleRunningPayload)
      });
      expect(staleInitial.status).toBe(200);
      const staleRecordPath = join(
        root,
        "shared-brain",
        "staging",
        "shared-brain",
        "derived",
        "idempotency",
        "import",
        `${sha256Text(staleRunningKey)}.json`
      );
      const staleRecord = JSON.parse(await readFile(staleRecordPath, "utf8")) as Record<string, unknown>;
      await writeFile(
        staleRecordPath,
        `${JSON.stringify(
          {
            ...staleRecord,
            status: "running",
            updated_at: "2026-01-01T00:00:00Z",
            lease_until: "2026-01-01T00:00:00Z",
            side_effect_started_at: "2026-01-01T00:00:00Z",
            response_body: undefined
          },
          null,
          2
        )}\n`
      );
      const staleReplay = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": staleRunningKey
        },
        body: JSON.stringify(staleRunningPayload)
      });
      expect(staleReplay.status).toBe(409);
      const staleReplayBody = (await staleReplay.json()) as { error?: string };
      expect(staleReplayBody.error || "").toContain("requires operator review");
      const reviewRecord = JSON.parse(await readFile(staleRecordPath, "utf8")) as { status?: string; review_required_at?: string };
      expect(reviewRecord.status).toBe("review_required");
      expect(typeof reviewRecord.review_required_at).toBe("string");

      const concurrentPayload = {
        ...payload,
        title: "Concurrent idempotent note",
        text: `${payload.text}\nConcurrent replay.`
      };
      const [concurrentA, concurrentB] = await Promise.all([
        fetch(`http://127.0.0.1:${port}/api/import`, {
          method: "POST",
          headers: {
            Authorization: "Bearer import-test-token",
            "Content-Type": "application/json",
            "Idempotency-Key": "content-safety-concurrent"
          },
          body: JSON.stringify(concurrentPayload)
        }),
        fetch(`http://127.0.0.1:${port}/api/import`, {
          method: "POST",
          headers: {
            Authorization: "Bearer import-test-token",
            "Content-Type": "application/json",
            "Idempotency-Key": "content-safety-concurrent"
          },
          body: JSON.stringify(concurrentPayload)
        })
      ]);
      expect(concurrentA.status).toBe(200);
      expect(concurrentB.status).toBe(200);
      const concurrentBodyA = (await concurrentA.json()) as { artifact_id?: string; idempotent_replay?: boolean };
      const concurrentBodyB = (await concurrentB.json()) as { artifact_id?: string; idempotent_replay?: boolean };
      expect(concurrentBodyA.artifact_id).toBe(concurrentBodyB.artifact_id);
      expect([concurrentBodyA.idempotent_replay, concurrentBodyB.idempotent_replay]).toContain(true);

      const proposalPayload = {
        title: "Needle Proposal",
        body: "Use this proposal to prove idempotent propose replay.",
        source_harness: "test-harness",
        source_machine: "test-machine"
      };
      const proposalResponse = await fetch(`http://127.0.0.1:${port}/api/propose`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": "content-safety-proposal"
        },
        body: JSON.stringify(proposalPayload)
      });
      expect(proposalResponse.status).toBe(200);
      const proposed = (await proposalResponse.json()) as { proposal_path?: string };
      expect(typeof proposed.proposal_path).toBe("string");
      const proposalReplayResponse = await fetch(`http://127.0.0.1:${port}/api/propose`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": "content-safety-proposal"
        },
        body: JSON.stringify(proposalPayload)
      });
      expect(proposalReplayResponse.status).toBe(200);
      const replayedProposal = (await proposalReplayResponse.json()) as {
        proposal_path?: string;
        idempotent_replay?: boolean;
      };
      expect(replayedProposal.proposal_path).toBe(proposed.proposal_path);
      expect(replayedProposal.idempotent_replay).toBe(true);
      const proposalConflict = await fetch(`http://127.0.0.1:${port}/api/propose`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": "content-safety-proposal"
        },
        body: JSON.stringify({ ...proposalPayload, body: "different proposal body" })
      });
      expect(proposalConflict.status).toBe(409);

      const staleProposalKey = "content-safety-proposal-stale";
      const staleProposalPayload = {
        ...proposalPayload,
        title: "Needle Stale Proposal",
        body: "This proposal simulates a crash after proposal side effects started."
      };
      const staleProposalInitial = await fetch(`http://127.0.0.1:${port}/api/propose`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": staleProposalKey
        },
        body: JSON.stringify(staleProposalPayload)
      });
      expect(staleProposalInitial.status).toBe(200);
      const staleProposalRecordPath = join(
        root,
        "shared-brain",
        "staging",
        "shared-brain",
        "derived",
        "idempotency",
        "propose",
        `${sha256Text(staleProposalKey)}.json`
      );
      const staleProposalRecord = JSON.parse(await readFile(staleProposalRecordPath, "utf8")) as Record<string, unknown>;
      await writeFile(
        staleProposalRecordPath,
        `${JSON.stringify(
          {
            ...staleProposalRecord,
            status: "running",
            updated_at: "2026-01-01T00:00:00Z",
            lease_until: "2026-01-01T00:00:00Z",
            side_effect_started_at: "2026-01-01T00:00:00Z",
            response_body: undefined
          },
          null,
          2
        )}\n`
      );
      const staleProposalReplay = await fetch(`http://127.0.0.1:${port}/api/propose`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": staleProposalKey
        },
        body: JSON.stringify(staleProposalPayload)
      });
      expect(staleProposalReplay.status).toBe(409);
      const staleProposalReplayBody = (await staleProposalReplay.json()) as { error?: string };
      expect(staleProposalReplayBody.error || "").toContain("requires operator review");
      const staleProposalReviewRecord = JSON.parse(await readFile(staleProposalRecordPath, "utf8")) as {
        status?: string;
        review_required_at?: string;
      };
      expect(staleProposalReviewRecord.status).toBe("review_required");
      expect(typeof staleProposalReviewRecord.review_required_at).toBe("string");

      await waitForCondition(async () => {
        const response = await fetch(`http://127.0.0.1:${port}/search?format=json&q=Needle&limit=1000000&scope=bad`);
        if (!response.ok) {
          return false;
        }
        const payload = (await response.json()) as { results?: unknown[] };
        return Array.isArray(payload.results) && payload.results.length > 0;
      }, "search index refresh after import");
      const searchResponse = await fetch(`http://127.0.0.1:${port}/search?format=json&q=Needle&limit=1000000&scope=bad`);
      expect(searchResponse.status).toBe(200);
      const searchPayload = (await searchResponse.json()) as {
        limit: number;
        scope: string;
        results: Array<{ path: string; snippet: string }>;
      };
      expect(searchPayload.limit).toBe(50);
      expect(searchPayload.scope).toBe("all");
      expect(searchPayload.results.length).toBeGreaterThan(0);
      const snippet = searchPayload.results.map((result) => result.snippet).join("\n");
      expect(snippet).toContain("<mark>Needle</mark>");

      const xssSearchResponse = await fetch(`http://127.0.0.1:${port}/search?format=json&q=onerror`);
      expect(xssSearchResponse.status).toBe(200);
      const xssSearchPayload = (await xssSearchResponse.json()) as {
        results: Array<{ snippet: string }>;
      };
      const xssSnippet = xssSearchPayload.results.map((result) => result.snippet).join("\n");
      expect(xssSnippet).not.toContain("<img src=x");
      expect(xssSnippet).toContain("&lt;img");

      const oddQuery = await fetch(`http://127.0.0.1:${port}/search?format=json&q=${encodeURIComponent('" OR *')}`);
      expect(oddQuery.status).toBe(200);

      const serveRoot = join(root, "shared-brain", "serve", "shared-brain");
      await writeFile(join(serveRoot, "wiki", "LinkSafety.md"), "# LinkSafety\n\n[blocked](javascript:alert(1)) and [allowed](https://example.com/ok).\n");
      const pageResponse = await fetch(`http://127.0.0.1:${port}/page/wiki/LinkSafety.md`);
      expect(pageResponse.status).toBe(200);
      expect(pageResponse.headers.get("x-content-type-options")).toBe("nosniff");
      const pageHtml = await pageResponse.text();
      expect(pageHtml).not.toContain('href="javascript:');
      expect(pageHtml).toContain('href="https://example.com/ok"');

      const sourcePort = 49_000 + Math.floor(Math.random() * 2_000);
      sourceServer = Bun.serve({
        hostname: "127.0.0.1",
        port: sourcePort,
        fetch() {
          return new Response("<script>alert(1)</script><h1>Active</h1>", {
            headers: { "content-type": "text/html; charset=utf-8" }
          });
        }
      });
      await fetch(`http://127.0.0.1:${sourcePort}/active.html`);
      const htmlImport = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          title: "Active HTML",
          url: `http://127.0.0.1:${sourcePort}/active.html`,
          source_harness: "test-harness",
          source_machine: "test-machine",
          source_type: "url"
        })
      });
      expect(htmlImport.status).toBe(200);
      const htmlImportPayload = (await htmlImport.json()) as { touched_files?: string[] };
      const activeRawPath = htmlImportPayload.touched_files?.find((path) => path.startsWith("raw/imported/") && path.endsWith(".html"));
      expect(typeof activeRawPath).toBe("string");
      await waitForCondition(async () => {
        const response = await fetch(`http://127.0.0.1:${port}/raw/${repoPathUrl(activeRawPath!)}`);
        return response.status === 200 && (response.headers.get("content-type") || "").includes("text/plain");
      }, "raw active artifact to be available in serve clone");
      const rawResponse = await fetch(`http://127.0.0.1:${port}/raw/${repoPathUrl(activeRawPath!)}`);
      expect(rawResponse.status).toBe(200);
      expect(rawResponse.headers.get("content-type") || "").toContain("text/plain");
      expect(rawResponse.headers.get("x-content-type-options")).toBe("nosniff");
      expect(rawResponse.headers.get("content-disposition") || "").toContain("attachment");
      expect(rawResponse.headers.get("content-disposition") || "").not.toContain('"bad');

      const hiddenRaw = await fetch(`http://127.0.0.1:${port}/raw/.git/config`);
      expect(hiddenRaw.status).toBe(403);
      const derivedRaw = await fetch(`http://127.0.0.1:${port}/raw/derived/idempotency/import/example.json`);
      expect(derivedRaw.status).toBe(403);
      const hiddenPage = await fetch(`http://127.0.0.1:${port}/page/.git/config`);
      expect(hiddenPage.status).toBe(403);

      let fetchCount = 0;
      sourceServer.stop(true);
      sourceServer = Bun.serve({
        hostname: "127.0.0.1",
        port: sourcePort,
        fetch() {
          fetchCount += 1;
          return new Response(`# URL replay ${fetchCount}`, {
            headers: { "content-type": "text/markdown; charset=utf-8" }
          });
        }
      });
      await fetch(`http://127.0.0.1:${sourcePort}/url-replay.md`);
      fetchCount = 0;
      const urlReplayPayload = {
        title: "URL replay",
        url: `http://127.0.0.1:${sourcePort}/url-replay.md`,
        source_harness: "test-harness",
        source_machine: "test-machine",
        source_type: "url"
      };
      const urlImport = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": "url-replay"
        },
        body: JSON.stringify(urlReplayPayload)
      });
      expect(urlImport.status).toBe(200);
      const urlReplay = await fetch(`http://127.0.0.1:${port}/api/import`, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": "application/json",
          "Idempotency-Key": "url-replay"
        },
        body: JSON.stringify(urlReplayPayload)
      });
      expect(urlReplay.status).toBe(200);
      expect(((await urlReplay.json()) as { idempotent_replay?: boolean }).idempotent_replay).toBe(true);
      expect(fetchCount).toBe(1);
    } finally {
      sourceServer?.stop(true);
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  }, 15_000);
});

describe("public release hygiene", () => {
  test("public examples and bootstrap snippets contain no private or local identifiers", async () => {
    const roots = [
      "AGENTS.md",
      "apps",
      "examples",
      "packages",
      "infra/tailscale",
      "docs",
      "README.md",
      "scripts"
    ];
    const files: string[] = [];
    for (const entry of roots) {
      const fullPath = join(PRODUCT_ROOT, entry);
      const info = await stat(fullPath).catch(() => null);
      if (!info) {
        continue;
      }
      if (info.isDirectory()) {
        files.push(...(await listFiles(fullPath)));
      } else {
        files.push(fullPath);
      }
    }
    const offenders: string[] = [];
    for (const file of files) {
      const relativeFile = file.slice(PRODUCT_ROOT.length + 1);
      const text = await readFile(file, "utf8");
      for (const { label, pattern } of FORBIDDEN_PUBLIC_PATTERNS) {
        if (pattern.test(relativeFile)) {
          offenders.push(`${relativeFile}: path contains ${label}`);
        }
        pattern.lastIndex = 0;
        if (pattern.test(text)) {
          offenders.push(`${relativeFile}: ${label}`);
        }
      }
    }
    expect(offenders).toEqual([]);
  });

  test("handoff script rejects unsafe timestamps and keeps safety fallbacks checked in", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainstack-handoff-safety-"));
    let sentinel = "";
    try {
      await mkdir(join(dir, "scripts"), { recursive: true });
      const scriptText = await readFile(join(PRODUCT_ROOT, "scripts", "handoff.sh"), "utf8");
      const copiedScript = join(dir, "scripts", "handoff.sh");
      await writeFile(copiedScript, scriptText);
      await chmod(copiedScript, 0o755);
      expectSuccess(runCommand(["git", "init"], { cwd: dir }));
      expectSuccess(runCommand(["git", "config", "user.email", "test@example.invalid"], { cwd: dir }));
      expectSuccess(runCommand(["git", "config", "user.name", "Test"], { cwd: dir }));
      expectSuccess(runCommand(["git", "add", "scripts/handoff.sh"], { cwd: dir }));
      expectSuccess(runCommand(["git", "commit", "-m", "fixture"], { cwd: dir }));

      sentinel = join(dirname(dir), `${basename(dir)}-sentinel`);
      await writeFile(sentinel, "keep");
      const unsafe = runCommand(["bash", "scripts/handoff.sh", "--out", dir], {
        cwd: dir,
        env: { BRAINSTACK_HANDOFF_UTC: "x/../../sentinel" }
      });
      expect(unsafe.code).not.toBe(0);
      expect(unsafe.stderr).toContain("BRAINSTACK_HANDOFF_UTC must match");
      expect(await readFile(sentinel, "utf8")).toBe("keep");

      expect(scriptText).toContain("[REDACTED ");
      expect(scriptText).toContain("shasum -a 256");
      expect(scriptText).toContain("openssl dgst -sha256");
      expect(scriptText).toContain("BRAINSTACK_HANDOFF_LIVE_HOSTS");
      expect(scriptText).toContain("BRAINSTACK_HANDOFF_INCLUDE_SHARED_BRAIN");
      expect(scriptText).toContain("find \"$bundle_dir\" -type l");
    } finally {
      await rm(dir, { recursive: true, force: true });
      if (sentinel) {
        await rm(sentinel, { force: true });
      }
    }
  });

  test("handoff script scans tracked hidden files for token-shaped secrets before zipping", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainstack-handoff-secret-fixture-"));
    try {
      const binDir = join(dir, "bin");
      await mkdir(join(dir, "scripts"), { recursive: true });
      await mkdir(join(dir, "docs"), { recursive: true });
      await mkdir(join(dir, "packages", "client-bootstrap"), { recursive: true });
      await mkdir(binDir, { recursive: true });
      await writeFile(join(dir, "scripts", "handoff.sh"), await readFile(join(PRODUCT_ROOT, "scripts", "handoff.sh"), "utf8"));
      await chmod(join(dir, "scripts", "handoff.sh"), 0o755);
      await writeFile(
        join(binDir, "bun"),
        [
          "#!/usr/bin/env bash",
          "set -eu",
          "if [ \"${1:-}\" = test ]; then echo '1 pass'; exit 0; fi",
          "if [ \"${1:-}\" = --version ]; then echo 'Bun fake'; exit 0; fi",
          "if [ \"${1:-}\" = run ]; then",
          "  shift",
          "  case \"${1:-}\" in",
          "    build:brainctl) echo 'build ok'; exit 0 ;;",
          "    *.ts) echo 'server fake'; sleep 0.2; exit 0 ;;",
          "    *brainctl*)",
          "      previous=''",
          "      for arg in \"$@\"; do",
          "        if [ \"$previous\" = --out ]; then",
          "          case \"$arg\" in",
          "            *.yaml) mkdir -p \"$(dirname \"$arg\")\"; echo 'schema_version: 1' > \"$arg\" ;;",
          "            *) mkdir -p \"$arg\"; echo generated > \"$arg/README.txt\" ;;",
          "          esac",
          "        fi",
          "        previous=\"$arg\"",
          "      done",
          "      echo \"brainctl fake $*\"",
          "      exit 0",
          "      ;;",
          "  esac",
          "fi",
          "echo \"bun fake $*\"",
          ""
        ].join("\n")
      );
      await chmod(join(binDir, "bun"), 0o755);
      await writeFile(join(dir, "package.json"), "{\"scripts\":{\"build:brainctl\":\"echo build\"}}\n");
      await writeFile(join(dir, "README.md"), "Brainstack runs on trusted private networks.\n");
      await writeFile(join(dir, "docs", "security-postures.md"), "It defaults to a trusted private mesh, does not require read tokens, and says: Do not expose trusted-tailnet mode to the public internet.\n");
      await writeFile(join(dir, "docs", "tailscale-exposure.md"), "Use brainctl expose tailscale with tailscale serve.\n");
      await writeFile(join(dir, "docs", "multi-brain.md"), "project config uses .brainstack.yaml and profiles.yaml. Sections are not hard security boundaries; they are retrieval boundaries.\n");
      await writeFile(join(dir, "docs", "outbox-security.md"), "Outbox payloads are sensitive plaintext by default. No queued payload is silently truncated. Future server-sealed mode is documented.\n");
      await writeFile(join(dir, "docs", "diagrams.md"), "```mermaid\ngraph TD\nA-->B\n```\n");
      await writeFile(join(dir, "packages", "client-bootstrap", "claude-user-CLAUDE.md"), "Use brainctl search --repo . and brainctl remember --repo .\n");
      const fakeToken = "ghp_" + "A".repeat(24);
      await writeFile(join(dir, ".hidden-token"), `${fakeToken}\n`);
      expectSuccess(runCommand(["git", "init"], { cwd: dir }));
      expectSuccess(runCommand(["git", "config", "user.email", "test@example.invalid"], { cwd: dir }));
      expectSuccess(runCommand(["git", "config", "user.name", "Test"], { cwd: dir }));
      expectSuccess(runCommand(["git", "add", "."], { cwd: dir }));
      expectSuccess(runCommand(["git", "commit", "-m", "fixture"], { cwd: dir }));

      const out = join(dir, "out");
      await mkdir(out, { recursive: true });
      const refusal = runCommand(["bash", "scripts/handoff.sh", "--out", out], {
        cwd: dir,
        env: {
          PATH: `${binDir}:${process.env.PATH || ""}`,
          BRAINSTACK_HANDOFF_UTC: "20260525T000000Z"
        }
      });
      expect(refusal.code).not.toBe(0);
      expect(refusal.stderr).toContain("handoff refused: secrets-looking patterns detected");
      expect(refusal.stderr).toContain("[REDACTED github-token]");
      expect(refusal.stderr).not.toContain(fakeToken);
      expect(await Bun.file(join(out, "handoff-20260525T000000Z.zip")).exists()).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 20_000);

  test("generated client bootstrap remains generic", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-client-generic-"));
    try {
      const out = join(dir, "bootstrap");
      expectSuccess(
        runBrainctl([
          "bootstrap-client",
          "--profile",
          "client-macos",
          "--config",
          join(PRODUCT_ROOT, "examples", "client-macos.yaml"),
          "--out",
          out
        ])
      );
      const files = await listFiles(out);
      const offenders: string[] = [];
      for (const file of files) {
        const text = await readFile(file, "utf8");
        for (const { label, pattern } of FORBIDDEN_PUBLIC_PATTERNS) {
          pattern.lastIndex = 0;
          if (pattern.test(text)) {
            offenders.push(`${file}: ${label}`);
          }
        }
      }
      expect(offenders).toEqual([]);
      expect(await readFile(join(out, "client.env.example"), "utf8")).toContain("SHARED_BRAIN_LOCAL_PATH=~/shared-brain");
      const installScript = await readFile(join(out, "install-client.sh"), "utf8");
      expect(installScript).toContain("operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git");
      expect(installScript).toContain("TARGET_ABS=\"$(expand_path \"$TARGET\")\"");
      expect(installScript).toContain("TARGET=\"${SHARED_BRAIN_LOCAL_PATH:-~/shared-brain}\"");
      expect(installScript).toContain("git -C \"$TARGET_ABS\" pull --ff-only");
      expect(installScript).toContain("git clone \"$REMOTE\" \"$TARGET_ABS\"");
      expect(installScript).toContain("ln -s \"$BOOTSTRAP_DIR/codex-shared-brain.include.md\" \"$CODEX_HOME/AGENTS.md\"");
      expect(installScript).not.toContain("Read the product-owned shared-brain snippet");
      expect(installScript).toContain("@$BOOTSTRAP_ABS/claude-user-CLAUDE.md");
      expect(installScript).toContain("cp \"$BOOTSTRAP_DIR/cursor-user-rule.md\" \"$CURSOR_RULE_DIR/shared-brain.md\"");

      const claude = await readFile(join(out, "claude-user-CLAUDE.md"), "utf8");
      expect(claude).toContain("@~/shared-brain/AGENTS.shared-client.md");
      expect(claude).not.toContain("Import `");

      const codex = await readFile(join(out, "codex-shared-brain.include.md"), "utf8");
      expect(codex).toContain("brainctl context --repo .");
      expect(codex).toContain("brainctl search --repo .");
      expect(codex).toContain("Do not directly edit canonical wiki pages");

      const cursor = await readFile(join(out, "cursor-user-rule.md"), "utf8");
      expect(cursor).toContain("Before substantial work in a repository");
      expect(cursor).not.toContain("Read ~/.config");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("install-client default target clones into home shared-brain", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-client-install-default-"));
    try {
      const bare = join(dir, "shared-brain.git");
      const seed = join(dir, "seed");
      const out = join(dir, "bootstrap");
      const home = join(dir, "home");
      git(["init", "--bare", "--initial-branch=main", bare], dir);
      git(["clone", bare, seed], dir);
      await writeFile(join(seed, "AGENTS.shared-client.md"), "# Shared Client\n");
      git(["add", "AGENTS.shared-client.md"], seed);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "seed"], seed);
      git(["push", "-u", "origin", "main"], seed);

      expectSuccess(
        runBrainctl([
          "bootstrap-client",
          "--profile",
          "client-macos",
          "--config",
          join(PRODUCT_ROOT, "examples", "client-macos.yaml"),
          "--out",
          out
        ])
      );

      const install = runCommand(["bash", join(out, "install-client.sh")], {
        cwd: out,
        env: {
          HOME: home,
          BRAIN_GIT_REMOTE: bare,
          PATH: process.env.PATH || ""
        }
      });
      expectSuccess(install);
      expect(await Bun.file(join(home, "shared-brain", "AGENTS.shared-client.md")).exists()).toBe(true);
      expect(await Bun.file(join(home, ".config", "shared-brain.env")).exists()).toBe(true);
      expect(await Bun.file(join(home, ".config", "brainstack", "brainstack.yaml")).exists()).toBe(true);
      expect(await readFile(join(home, ".config", "brainstack", "brainstack.yaml"), "utf8")).toContain("profile: client-macos");
      expect(await Bun.file(join(home, ".codex", "AGENTS.md")).exists()).toBe(true);
      expect(await readFile(join(home, ".claude", "CLAUDE.md"), "utf8")).toContain(
        `@${join(home, ".config", "brainstack", "client-bootstrap", "claude-user-CLAUDE.md")}`
      );
      expect(await readFile(join(home, ".config", "brainstack", "client-bootstrap", "claude-user-CLAUDE.md"), "utf8")).toContain(
        `@${join(home, "shared-brain", "AGENTS.shared-client.md")}`
      );
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("install-client token file succeeds, preserves existing token, and fails on missing token file", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-client-install-token-"));
    try {
      const bare = join(dir, "shared-brain.git");
      const seed = join(dir, "seed");
      const out = join(dir, "bootstrap");
      const home = join(dir, "home");
      const tokenFile = join(dir, "token.txt");
      git(["init", "--bare", "--initial-branch=main", bare], dir);
      git(["clone", bare, seed], dir);
      await writeFile(join(seed, "AGENTS.shared-client.md"), "# Shared Client\n");
      git(["add", "AGENTS.shared-client.md"], seed);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "seed"], seed);
      git(["push", "-u", "origin", "main"], seed);

      expectSuccess(
        runBrainctl([
          "bootstrap-client",
          "--profile",
          "client-macos",
          "--config",
          join(PRODUCT_ROOT, "examples", "client-macos.yaml"),
          "--out",
          out
        ])
      );

      await writeFile(tokenFile, "first-token\n");
      await chmod(tokenFile, 0o600);
      const install = runCommand(["bash", join(out, "install-client.sh")], {
        cwd: out,
        env: {
          HOME: home,
          BRAIN_GIT_REMOTE: bare,
          BRAIN_IMPORT_TOKEN_FILE: tokenFile,
          PATH: process.env.PATH || ""
        }
      });
      expectSuccess(install);
      const envPath = join(home, ".config", "shared-brain.env");
      expect(await readFile(envPath, "utf8")).toContain("BRAIN_IMPORT_TOKEN=first-token");
      expect(`${install.stdout}\n${install.stderr}`).not.toContain("first-token");

      const preserve = runCommand(["bash", join(out, "install-client.sh")], {
        cwd: out,
        env: {
          HOME: home,
          BRAIN_GIT_REMOTE: bare,
          BRAIN_IMPORT_TOKEN: "second-token",
          PATH: process.env.PATH || ""
        }
      });
      expectSuccess(preserve);
      const preservedEnv = await readFile(envPath, "utf8");
      expect(preservedEnv).toContain("BRAIN_IMPORT_TOKEN=first-token");
      expect(preservedEnv).not.toContain("second-token");
      expect(`${preserve.stdout}\n${preserve.stderr}`).not.toContain("second-token");

      const missingHome = join(dir, "missing-home");
      const missing = runCommand(["bash", join(out, "install-client.sh")], {
        cwd: out,
        env: {
          HOME: missingHome,
          BRAIN_GIT_REMOTE: bare,
          BRAIN_IMPORT_TOKEN_FILE: join(dir, "missing-token.txt"),
          PATH: process.env.PATH || ""
        }
      });
      expect(missing.code).toBe(2);
      expect(missing.stderr).toContain("BRAIN_IMPORT_TOKEN_FILE does not exist");

      const looseTokenFile = join(dir, "loose-token.txt");
      await writeFile(looseTokenFile, "loose-token\n");
      await chmod(looseTokenFile, 0o644);
      const loose = runCommand(["bash", join(out, "install-client.sh")], {
        cwd: out,
        env: {
          HOME: join(dir, "loose-home"),
          BRAIN_GIT_REMOTE: bare,
          BRAIN_IMPORT_TOKEN_FILE: looseTokenFile,
          PATH: process.env.PATH || ""
        }
      });
      expect(loose.code).toBe(2);
      expect(loose.stderr).toContain("must not be group/world accessible");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("generated client bootstrap respects custom client.localPath", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-client-path-"));
    try {
      const configPath = join(dir, "config.yaml");
      const out = join(dir, "bootstrap");
      const customLocalPath = join(dir, "custom-client-brain");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client-laptop",
          "  user: operator",
          "paths:",
          "  home: /Users/operator",
          "client:",
          `  localPath: ${customLocalPath}`,
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          "brain:",
          "  publicBaseUrl: https://brain-control.example.ts.net",
          ""
        ].join("\n")
      );

      expectSuccess(runBrainctl(["bootstrap-client", "--profile", "client-macos", "--config", configPath, "--out", out]));
      const files = await listFiles(out);
      const hardcodedDefaults: string[] = [];
      for (const file of files) {
        const text = await readFile(file, "utf8");
        if (text.includes("~/shared-brain")) {
          hardcodedDefaults.push(file);
        }
      }
      expect(hardcodedDefaults).toEqual([]);
      expect(await readFile(join(out, "client.env.example"), "utf8")).toContain(`SHARED_BRAIN_LOCAL_PATH=${customLocalPath}`);
      expect(await readFile(join(out, "codex-shared-brain.include.md"), "utf8")).toContain("brainctl context --repo .");
      expect(await readFile(join(out, "codex-global-AGENTS.md"), "utf8")).toContain("brainctl remember --repo . --summary");
      expect(await readFile(join(out, "claude-user-CLAUDE.md"), "utf8")).toContain(`@${customLocalPath}/AGENTS.shared-client.md`);
      expect(await readFile(join(out, "claude-hooks-example.json"), "utf8")).toContain(`git -C ${customLocalPath} pull --ff-only`);
      expect(await readFile(join(out, "cursor-user-rule.md"), "utf8")).toContain("brainctl context --repo .");
      expect(await readFile(join(out, "install-client.sh"), "utf8")).toContain(`TARGET="\${SHARED_BRAIN_LOCAL_PATH:-${customLocalPath}}"`);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("worker join flow is config-driven", () => {
    const result = runBrainctl(["join-worker", "--config", join(PRODUCT_ROOT, "examples", "control.yaml"), "--worker", "brain-worker"]);
    expectSuccess(result);
    expect(result.stdout).toContain("Merge this YAML into brainstack.yaml");
    expect(result.stdout).toContain("Do not edit `workers.json` directly");
    expect(result.stdout).toContain("sshUser: operator");
    expect(result.stdout).toContain("sshTrustMode: pinned");
    expect(result.stdout).toContain("brainctl trust-worker --config brainstack.yaml --worker brain-worker");
    expect(result.stdout).toContain("brainctl upgrade --config brainstack.yaml --profile control");
    expect(result.stdout).toContain("ssh operator@brain-worker true");
    expect(result.stdout).toContain('"dst": [\n      "tag:brain-worker"');
    expect(result.stdout).toContain('"src": [\n      "tag:brain-worker"');
    expect(result.stdout).toContain('"tcp:443"');
    expect(result.stdout).not.toContain("sshUser: factory");
    expect(result.stdout).not.toContain("Add to workers.json");
  });

  test("worker join paths respect custom stateRoot", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-worker-state-"));
    try {
      const configPath = join(dir, "config.yaml");
      const stateRoot = join(dir, "custom-state");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "paths:",
          "  home: /home/operator",
          `  stateRoot: ${stateRoot}`,
          "telemux:",
          "  enabled: false",
          ""
        ].join("\n")
      );
      const result = runBrainctl(["join-worker", "--config", configPath, "--worker", "brain-worker"]);
      expectSuccess(result);
      expect(result.stdout).toContain(`managedRepoRoot: ${stateRoot}/factory/repos`);
      expect(result.stdout).toContain(`managedHostRoot: ${stateRoot}/factory/hostctx`);
      expect(result.stdout).toContain(`managedScratchRoot: ${stateRoot}/factory/scratch`);
      expect(result.stdout).not.toContain("~/.local/state/brainstack/factory");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("worker SSH trust and repo-lock recovery commands are explicit", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-ops-commands-"));
    try {
      const configPath = join(dir, "config.yaml");
      const stateRoot = join(dir, "state");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${stateRoot}`,
          "telemux:",
          "  enabled: true",
          "  workers:",
          "    - name: worker1",
          "      transport: ssh",
          "      sshTarget: worker1.example",
          "      sshUser: operator",
          "      sshTrustMode: pinned",
          "    - name: worker2",
          "      transport: ssh",
          "      sshTarget: '[worker2.example]:2222'",
          "      sshUser: operator",
          "      sshTrustMode: pinned",
          ""
        ].join("\n")
      );
      const trustDryRun = runBrainctl(["trust-worker", "--config", configPath, "--worker", "worker1", "--dry-run"]);
      expectSuccess(trustDryRun);
      expect(trustDryRun.stdout).toContain("ssh-keyscan -T 8 worker1.example");
      expect(trustDryRun.stdout).toContain(join(dir, ".config", "brainstack", "ssh_known_hosts"));
      const trustPortDryRun = runBrainctl(["trust-worker", "--config", configPath, "--worker", "worker2", "--dry-run"]);
      expectSuccess(trustPortDryRun);
      expect(trustPortDryRun.stdout).toContain("ssh-keyscan -T 8 -p 2222 worker2.example");

      const lockStatusAbsent = runBrainctl(["repo-lock", "status", "--config", configPath]);
      expectSuccess(lockStatusAbsent);
      expect(lockStatusAbsent.stdout).toContain("repo-lock=absent");

      const lockPath = join(dir, "shared-brain", "staging", "shared-brain", ".shared-brain.lock");
      const liveLockId = "00000000-0000-0000-0000-000000000010";
      await mkdir(lockPath, { recursive: true });
      await writeFile(join(lockPath, `owner-${liveLockId}.json`), `${JSON.stringify({ token: liveLockId, pid: process.pid, hostname: hostname() })}\n`);
      await writeFile(join(lockPath, `release-${liveLockId}`), liveLockId);
      const liveRefused = runBrainctl(["repo-lock", "clear", "--config", configPath, "--yes", "--token", liveLockId, "--min-age-ms", "0"]);
      expect(liveRefused.code).not.toBe(0);
      expect(liveRefused.stderr).toContain("owner process is still running");
      const forceWithoutToken = runBrainctl(["repo-lock", "clear", "--config", configPath, "--yes", "--force", "--min-age-ms", "0"]);
      expect(forceWithoutToken.code).not.toBe(0);
      expect(forceWithoutToken.stderr).toContain("without --token");
      const liveForced = runBrainctl(["repo-lock", "clear", "--config", configPath, "--yes", "--force", "--token", liveLockId, "--min-age-ms", "0"]);
      expectSuccess(liveForced);

      const foreignLockId = "00000000-0000-0000-0000-000000000020";
      await mkdir(lockPath, { recursive: true });
      await writeFile(join(lockPath, `owner-${foreignLockId}.json`), `${JSON.stringify({ token: foreignLockId, pid: 999999, hostname: "other-host.example" })}\n`);
      await writeFile(join(lockPath, `release-${foreignLockId}`), foreignLockId);
      const foreignRefused = runBrainctl(["repo-lock", "clear", "--config", configPath, "--yes", "--token", foreignLockId, "--min-age-ms", "0"]);
      expect(foreignRefused.code).not.toBe(0);
      expect(foreignRefused.stderr).toContain("owner host other-host.example is not local host");
      expectSuccess(runBrainctl(["repo-lock", "clear", "--config", configPath, "--yes", "--force", "--token", foreignLockId, "--min-age-ms", "0"]));

      const wrongTokenLockId = "00000000-0000-0000-0000-000000000030";
      await mkdir(lockPath, { recursive: true });
      await writeFile(join(lockPath, `owner-${wrongTokenLockId}.json`), `${JSON.stringify({ token: wrongTokenLockId, pid: 999999, hostname: hostname() })}\n`);
      await writeFile(join(lockPath, `release-${wrongTokenLockId}`), wrongTokenLockId);
      const wrongToken = runBrainctl(["repo-lock", "clear", "--config", configPath, "--yes", "--token", "not-the-token", "--min-age-ms", "0"]);
      expect(wrongToken.code).not.toBe(0);
      expect(wrongToken.stderr).toContain("--token does not match");
      await rm(lockPath, { recursive: true, force: true });

      const symlinkLockId = "00000000-0000-0000-0000-000000000040";
      const symlinkTarget = join(dir, "symlink-lock-target");
      await mkdir(symlinkTarget, { recursive: true });
      await writeFile(join(symlinkTarget, `owner-${symlinkLockId}.json`), `${JSON.stringify({ token: symlinkLockId, pid: 999999, hostname: hostname() })}\n`);
      await writeFile(join(symlinkTarget, `release-${symlinkLockId}`), symlinkLockId);
      await symlink(symlinkTarget, lockPath);
      const symlinkStatus = runBrainctl(["repo-lock", "status", "--config", configPath]);
      expectSuccess(symlinkStatus);
      expect(symlinkStatus.stdout).toContain("lock path is a symlink");
      expect(symlinkStatus.stdout).not.toContain(symlinkLockId);
      const symlinkRefused = runBrainctl(["repo-lock", "clear", "--config", configPath, "--yes", "--force", "--token", symlinkLockId, "--min-age-ms", "0"]);
      expect(symlinkRefused.code).not.toBe(0);
      expect(symlinkRefused.stderr).toContain("non-directory or symlink repo lock");
      await rm(lockPath, { force: true });
      await rm(symlinkTarget, { recursive: true, force: true });

      await mkdir(lockPath, { recursive: true });
      const emptyStatus = runBrainctl(["repo-lock", "status", "--config", configPath]);
      expectSuccess(emptyStatus);
      expect(emptyStatus.stdout).toContain("reason=lock directory is empty");
      expect(emptyStatus.stdout).toContain("clear_token=EMPTY");
      const emptyWithoutForce = runBrainctl(["repo-lock", "clear", "--config", configPath, "--yes", "--token", "EMPTY", "--min-age-ms", "0"]);
      expect(emptyWithoutForce.code).not.toBe(0);
      expect(emptyWithoutForce.stderr).toContain("without --force --token EMPTY");
      const emptyCleared = runBrainctl(["repo-lock", "clear", "--config", configPath, "--yes", "--force", "--token", "EMPTY", "--min-age-ms", "0"]);
      expectSuccess(emptyCleared);
      expect(existsSync(lockPath)).toBe(false);

      const lockId = "00000000-0000-0000-0000-000000000001";
      await mkdir(lockPath, { recursive: true });
      await writeFile(join(lockPath, `owner-${lockId}.json`), `${JSON.stringify({ token: lockId, pid: 999999, hostname: hostname() })}\n`);
      await writeFile(join(lockPath, `release-${lockId}`), lockId);
      const lockStatusPresent = runBrainctl(["repo-lock", "status", "--config", configPath]);
      expectSuccess(lockStatusPresent);
      expect(lockStatusPresent.stdout).toContain("repo-lock=present");
      expect(lockStatusPresent.stdout).toContain(`owner-${lockId}.json`);
      const refusedClear = runBrainctl(["repo-lock", "clear", "--config", configPath]);
      expect(refusedClear.code).not.toBe(0);
      expect(refusedClear.stderr).toContain("Refusing to clear repo lock without --yes");
      await writeFile(join(lockPath, "unexpected.txt"), "do not remove recursively\n");
      const unsafeClear = runBrainctl(["repo-lock", "clear", "--config", configPath, "--yes"]);
      expect(unsafeClear.code).not.toBe(0);
      expect(unsafeClear.stderr).toContain("unknown entries");
      await rm(join(lockPath, "unexpected.txt"), { force: true });
      const clear = runBrainctl(["repo-lock", "clear", "--config", configPath, "--yes", "--token", lockId, "--min-age-ms", "0"]);
      expectSuccess(clear);
      expect(existsSync(lockPath)).toBe(false);

      const idempotencyLock = join(
        dir,
        "shared-brain",
        "staging",
        "shared-brain",
        "derived",
        "idempotency",
        "import",
        "stuck.json.lock"
      );
      const idempotencyToken = "22222222-2222-4222-8222-222222222222";
      await mkdir(idempotencyLock, { recursive: true });
      await writeFile(join(idempotencyLock, `owner-${idempotencyToken}.json`), `${JSON.stringify({ token: idempotencyToken, pid: 999999, hostname: hostname() })}\n`);
      await writeFile(join(idempotencyLock, `release-${idempotencyToken}`), idempotencyToken);
      const idempotencyStatus = runBrainctl(["locks", "status", "--config", configPath, "--path", idempotencyLock]);
      expectSuccess(idempotencyStatus);
      expect(idempotencyStatus.stdout).toContain(`clear_token=${idempotencyToken}`);
      const idempotencyClear = runBrainctl([
        "locks",
        "clear",
        "--config",
        configPath,
        "--path",
        idempotencyLock,
        "--yes",
        "--token",
        idempotencyToken,
        "--min-age-ms",
        "0"
      ]);
      expectSuccess(idempotencyClear);
      expect(existsSync(idempotencyLock)).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("rendered brainstack yaml preserves telemux state and worker entries", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-render-roundtrip-"));
    try {
      const configPath = join(dir, "config.yaml");
      const out = join(dir, "rendered");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "paths:",
          "  home: /home/operator",
          "telemux:",
          "  enabled: true",
          "  controlRoot: /srv/telemux",
          "  factoryRoot: /srv/factory",
          "  workers:",
          "    - name: brain-worker",
          "      transport: ssh",
          "      sshTarget: brain-worker",
          "      sshUser: operator",
          ""
        ].join("\n")
      );
      expectSuccess(runBrainctl(["render", "--profile", "control", "--config", configPath, "--out", out]));
      const rendered = await readFile(join(out, "brainstack.yaml"), "utf8");
      expect(rendered).toContain("telemux:");
      expect(rendered).toContain("enabled: true");
      expect(rendered).toContain("controlRoot: /srv/telemux");
      expect(rendered).toContain("factoryRoot: /srv/factory");
      expect(rendered).toContain("workers:");
      expect(rendered).toContain("name: brain-worker");
      expect(rendered).toContain("sshUser: operator");
      expect(rendered).toContain("repos:");
      const runtimeEnv = await readFile(join(out, "env", "telemux.runtime.env"), "utf8");
      expect(runtimeEnv).toContain("PATH=/home/operator/.local/bin:");
      expect(runtimeEnv).toContain("BRAINSTACK_WORKER_PATH=/home/operator/.local/bin:");
      expect(runtimeEnv).toContain("/home/operator/.bun/bin");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("rendered Tailscale policy grants worker freshness path back to control SSH", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-policy-"));
    try {
      const out = join(dir, "rendered");
      expectSuccess(runBrainctl(["render", "--profile", "control", "--config", join(PRODUCT_ROOT, "examples", "control.yaml"), "--out", out]));
      const rendered = JSON.parse(await readFile(join(out, "tailscale", "policy-fragment.json"), "utf8")) as {
        grants: Array<{ src: string[]; dst: string[]; ip: string[] }>;
      };
      const workerToControl = rendered.grants.find((grant) => grant.src.includes("tag:brain-worker") && grant.dst.includes("tag:brain"));
      expect(workerToControl?.ip).toContain("tcp:22");
      expect(workerToControl?.ip).toContain("tcp:443");
      const adminToWorker = rendered.grants.find(
        (grant) => grant.src.includes("group:brain-admins") && grant.dst.includes("tag:brain-worker")
      );
      expect(adminToWorker?.ip).toContain("tcp:22");

      const staticPolicy = JSON.parse(await readFile(join(PRODUCT_ROOT, "infra", "tailscale", "policy-fragment.example.json"), "utf8")) as {
        grants: Array<{ src: string[]; dst: string[]; ip: string[] }>;
      };
      const staticWorkerToControl = staticPolicy.grants.find((grant) => grant.src.includes("tag:brain-worker") && grant.dst.includes("tag:brain"));
      expect(staticWorkerToControl?.ip).toContain("tcp:22");
      expect(staticWorkerToControl?.ip).toContain("tcp:443");
      const staticAdminToWorker = staticPolicy.grants.find(
        (grant) => grant.src.includes("group:brain-admins") && grant.dst.includes("tag:brain-worker")
      );
      expect(staticAdminToWorker?.ip).toContain("tcp:22");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("expose tailscale refuses placeholder hosts and stale exposure metadata", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-expose-tailscale-"));
    try {
      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          "brain:",
          "  publicBaseUrl: https://brain-control.example.ts.net",
          "security:",
          "  posture: trusted-tailnet",
          "  bindHost: 127.0.0.1",
          "  trustedExposure: none",
          "tailscale:",
          "  tailnetHost: brain-control.example.ts.net",
          ""
        ].join("\n")
      );
      const placeholder = runBrainctl(["expose", "tailscale", "--config", configPath, "--dry-run"]);
      expect(placeholder.code).not.toBe(0);
      expect(placeholder.stderr).toContain("requires a real tailscale.tailnetHost");

      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          "brain:",
          "  publicBaseUrl: https://brain-control.tailnet.invalid",
          "security:",
          "  posture: trusted-tailnet",
          "  bindHost: 127.0.0.1",
          "  trustedExposure: none",
          "tailscale:",
          "  tailnetHost: brain-control.tailnet.invalid",
          ""
        ].join("\n")
      );
      const staleExposure = runBrainctl(["expose", "tailscale", "--config", configPath, "--apply"]);
      expect(staleExposure.code).not.toBe(0);
      expect(staleExposure.stderr).toContain("requires security.trustedExposure: tailscale-serve");

      await writeFile(
        configPath,
        (await readFile(configPath, "utf8")).replace("trustedExposure: none", "trustedExposure: tailscale-serve")
      );
      const dryRun = runBrainctl(["expose", "tailscale", "--config", configPath, "--dry-run"]);
      expectSuccess(dryRun);
      expect(dryRun.stdout).toContain("brain-control.tailnet.invalid:443");

      await writeFile(
        configPath,
        (await readFile(configPath, "utf8")).replace("bindHost: 127.0.0.1", "bindHost: 0.0.0.0")
      );
      const nonLoopbackApply = runBrainctl(["expose", "tailscale", "--config", configPath, "--apply"]);
      expect(nonLoopbackApply.code).not.toBe(0);
      expect(nonLoopbackApply.stderr).toContain("requires trusted-tailnet braind to bind loopback");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("doctor explains trusted-tailnet posture and rejects accidental non-loopback bind", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-posture-doctor-"));
    try {
      const configPath = join(dir, "config.yaml");
      const base = [
        "schema_version: 1",
        "profile: client-macos",
        "machine:",
        "  name: client",
        "  user: operator",
        "paths:",
        `  home: ${dir}`,
        "security:",
        "  posture: trusted-tailnet",
        "  bindHost: 127.0.0.1",
        "  trustedExposure: vpn",
        "client:",
        "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
        ""
      ].join("\n");
      await writeFile(configPath, base);
      const ok = runBrainctl(["doctor", "--config", configPath]);
      expectSuccess(ok);
      expect(ok.stdout).toContain("read-auth: disabled by design in trusted-tailnet mode");
      expect(ok.stdout).toContain("trust-boundary: private network reachability");
      expect(ok.stdout).toContain("do not expose trusted-tailnet mode to the public internet");

      await writeFile(configPath, base.replace("bindHost: 127.0.0.1", "bindHost: 192.168.1.10").replace("trustedExposure: vpn", "trustedExposure: none"));
      const badBind = runBrainctl(["doctor", "--config", configPath]);
      expect(badBind.code).not.toBe(0);
      expect(badBind.stdout).toContain("FAIL [security] posture: trusted-tailnet bind=192.168.1.10");

      await writeFile(configPath, base.replace("bindHost: 127.0.0.1", "bindHost: 0.0.0.0").replace("trustedExposure: vpn", "trustedExposure: manual"));
      const wildcardManual = runBrainctl(["doctor", "--config", configPath]);
      expect(wildcardManual.code).not.toBe(0);
      expect(wildcardManual.stdout).toContain("FAIL [security] posture: trusted-tailnet bind=0.0.0.0; exposure=manual");
      expect(wildcardManual.stdout).toContain("Wildcard trusted-tailnet binds are too broad");

      await writeFile(configPath, base.replace("trustedExposure: vpn", "trustedExposure: public"));
      const badExposure = runBrainctl(["doctor", "--config", configPath]);
      expect(badExposure.code).not.toBe(0);
      expect(badExposure.stderr).toContain("Expected none, tailscale-serve, vpn, or manual");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 12_000);

  test("doctor reports braind readiness separately from liveness", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-readyz-doctor-"));
    const port = 39_000 + Math.floor(Math.random() * 2_000);
    let server: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const binDir = join(dir, "bin");
      const serverScript = join(dir, "readyz-server.ts");
      await mkdir(binDir, { recursive: true });
      await writeFile(
        join(binDir, "codex"),
        "#!/usr/bin/env sh\nif [ \"${1:-}\" = \"--version\" ]; then printf 'codex fake\\n'; exit 0; fi\nprintf '%s\\n' '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --output-last-message'\n"
      );
      await writeFile(join(binDir, "tailscale"), "#!/usr/bin/env sh\nif [ \"${1:-}\" = \"status\" ]; then echo 'offline' >&2; exit 1; fi\nprintf 'tailscale fake\\n'\n");
      await chmod(join(binDir, "codex"), 0o755);
      await chmod(join(binDir, "tailscale"), 0o755);
      await writeFile(
        serverScript,
        [
          `const port = ${port};`,
          "Bun.serve({",
          "  hostname: '127.0.0.1',",
          "  port,",
          "  fetch(req) {",
          "    const path = new URL(req.url).pathname;",
          "    if (path === '/healthz') return Response.json({ ok: true, service: 'braind' });",
          "    if (path === '/readyz') return Response.json({ ok: false, service: 'braind', search_ready: false, pending_reindex: { present: true } }, { status: 503 });",
          "    return Response.json({ error: 'not found' }, { status: 404 });",
          "  }",
          "});",
          "await new Promise(() => {});",
          ""
        ].join("\n")
      );
      server = Bun.spawn(["bun", "run", serverScript], { stdout: "ignore", stderr: "ignore" });
      await waitForCondition(async () => {
        try {
          return (await fetch(`http://127.0.0.1:${port}/healthz`)).ok;
        } catch {
          return false;
        }
      }, "readyz fixture server");
      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          "brain:",
          `  port: ${port}`,
          "  publicBaseUrl: https://brain-control.example.ts.net",
          "security:",
          "  posture: trusted-tailnet",
          "  bindHost: 127.0.0.1",
          "telemux:",
          "  enabled: false",
          ""
        ].join("\n")
      );
      const result = runBrainctl(["doctor", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1"
      });
      expectSuccess(result);
      expect(result.stdout).toContain("PASS [health] braind-healthz: HTTP 200");
      expect(result.stdout).toContain("WARN [health] braind-readyz: HTTP 503 search_ready=false pending_reindex=true");
    } finally {
      if (server) {
        server.kill();
        await server.exited;
      }
      await rm(dir, { recursive: true, force: true });
    }
  }, 12_000);

  test("project context blocks personal brains until allowed and search labels sources", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainstack-project-context-"));
    try {
      const configPath = join(dir, "config.yaml");
      await writeFixtureConfig(configPath);
      const root = join(dir, "install");
      const workBrain = join(dir, "work-brain");
      const personalBrain = join(dir, "personal-brain");
      await mkdir(join(root, "config"), { recursive: true });
      await writeFile(
        join(root, "config", "profiles.yaml"),
        [
          "brains:",
          "  work:",
          `    localPath: ${workBrain}`,
          "  personal:",
          `    localPath: ${personalBrain}`,
          ""
        ].join("\n")
      );
      for (const brain of [workBrain, personalBrain]) {
        await mkdir(join(brain, "wiki"), { recursive: true });
        git(["init"], brain);
      }
      await writeFile(join(workBrain, "wiki", "Runbook.md"), "deploy checklist and owner notes\n");
      await writeFile(join(personalBrain, "wiki", "Journal.md"), "private deploy memory\n");
      const repo = join(dir, "project");
      await mkdir(repo, { recursive: true });
      await writeFile(
        join(repo, ".brainstack.yaml"),
        [
          "defaultBrain: work",
          "writeDefault: work",
          "brains:",
          "  - id: work",
          `    localPath: ${workBrain}`,
          "    sections:",
          "      - wiki",
          "  - id: personal",
          `    localPath: ${personalBrain}`,
          "    sections:",
          "      - wiki",
          ""
        ].join("\n")
      );

      const beforeAllow = runBrainctl(["context", "--repo", repo, "--config", configPath, "--root", root]);
      expectSuccess(beforeAllow);
      expect(beforeAllow.stdout).toContain("[allowed] work");
      expect(beforeAllow.stdout).toContain("[blocked] personal");
      expect(beforeAllow.stdout).toContain("brainctl allow repo");

      const searchBefore = runBrainctl(["search", "--repo", repo, "--config", configPath, "--root", root, "deploy"]);
      expectSuccess(searchBefore);
      expect(searchBefore.stdout).toContain("[work / wiki/Runbook.md:1]");
      expect(searchBefore.stdout).not.toContain("personal");

      const allow = runBrainctl(["allow", "repo", "--repo", repo, "--brain", "personal", "--always", "--config", configPath, "--root", root]);
      expectSuccess(allow);
      const missingDecision = runBrainctl(["allow", "repo", "--repo", repo, "--brain", "personal", "--config", configPath, "--root", root]);
      expect(missingDecision.code).not.toBe(0);
      expect(missingDecision.stderr).toContain("requires exactly one of --always, --once, or --deny");
      const multipleDecision = runBrainctl(["allow", "repo", "--repo", repo, "--brain", "personal", "--once", "--always", "--config", configPath, "--root", root]);
      expect(multipleDecision.code).not.toBe(0);
      expect(multipleDecision.stderr).toContain("requires exactly one of --always, --once, or --deny");
      const rulesPath = join(root, "config", "allow-rules.json");
      expect((await stat(rulesPath)).mode & 0o777).toBe(0o600);

      const searchAfter = runBrainctl(["search", "--repo", repo, "--config", configPath, "--root", root, "deploy"]);
      expectSuccess(searchAfter);
      expect(searchAfter.stdout).toContain("[work / wiki/Runbook.md:1]");
      expect(searchAfter.stdout).toContain("[personal / wiki/Journal.md:1]");

      const personalReadOnlyConfig = await readFile(join(repo, ".brainstack.yaml"), "utf8");
      await writeFile(join(repo, ".brainstack.yaml"), `${personalReadOnlyConfig}    write: false\n`);
      const rememberPersonal = runBrainctl(["remember", "--repo", repo, "--target", "personal", "--summary", "private note", "--config", configPath, "--root", root]);
      expect(rememberPersonal.code).not.toBe(0);
      expect(rememberPersonal.stderr).toContain("target brain personal is read-only");

      const hostileRepo = join(dir, "hostile-project");
      await mkdir(hostileRepo, { recursive: true });
      await writeFile(
        join(hostileRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: steal",
          `    localPath: ${dir}`,
          "    sections: [wiki]",
          ""
        ].join("\n")
      );
      const hostileContext = runBrainctl(["context", "--repo", hostileRepo, "--config", configPath, "--root", root]);
      expect(hostileContext.code).not.toBe(0);
      expect(hostileContext.stderr).toContain("cannot set localClone/localPath/path outside");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("project allow rules scope sections, consume once, and target outbox keeps credential affinity", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainstack-project-target-outbox-"));
    const servers: Array<ReturnType<typeof Bun.spawn>> = [];
    try {
      const configPath = join(dir, "config.yaml");
      const stateRoot = join(dir, "state");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${stateRoot}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const workBrain = join(dir, "work-brain");
      const personalBrain = join(dir, "personal-brain");
      await mkdir(join(dir, ".config", "brainstack"), { recursive: true });
      await writeFile(
        join(dir, ".config", "brainstack", "profiles.yaml"),
        [
          "brains:",
          "  work:",
          `    localPath: ${workBrain}`,
          "  p1:",
          `    localPath: ${personalBrain}`,
          ""
        ].join("\n")
      );
      for (const brain of [workBrain, personalBrain]) {
        await mkdir(join(brain, "wiki"), { recursive: true });
        await mkdir(join(brain, "raw"), { recursive: true });
        git(["init"], brain);
      }
      await writeFile(join(workBrain, "wiki", "Runbook.md"), "alpha work note\n");
      await writeFile(join(personalBrain, "wiki", "Journal.md"), "alpha personal wiki\n");
      await writeFile(join(personalBrain, "raw", "Dump.md"), "alpha personal raw\n");
      const repo = join(dir, "project");
      await mkdir(repo, { recursive: true });
      const portA = 42_000 + Math.floor(Math.random() * 1_000);
      const portB = 43_000 + Math.floor(Math.random() * 1_000);
      await writeFile(
        join(repo, ".brainstack.yaml"),
        [
          "defaultBrain: work",
          "writeDefault: work",
          "brains:",
          "  - id: work",
          "    label: Work brain",
          `    localPath: ${workBrain}`,
          `    baseUrl: http://127.0.0.1:${portA}`,
          "    importTokenEnv: BRAIN_A_TOKEN",
          "    classification: work",
          "    sections: [wiki]",
          "    write: true",
          "  - id: p1",
          "    label: Personal brain",
          `    localPath: ${personalBrain}`,
          `    baseUrl: http://127.0.0.1:${portB}`,
          "    importTokenEnv: BRAIN_B_TOKEN",
          "    classification: personal",
          "    sections: [wiki, raw]",
          "    write: true",
          ""
        ].join("\n")
      );

      expectSuccess(runBrainctl(["allow", "repo", "--repo", repo, "--brain", "p1", "--sections", "wiki", "--once", "--config", configPath]));
      const typoAllow = runBrainctl(["allow", "repo", "--repo", repo, "--brain", "p1", "--sections", "typo", "--once", "--config", configPath]);
      expect(typoAllow.code).not.toBe(0);
      expect(typoAllow.stderr).toContain("unknown section(s) for brain p1: typo");
      const scoped = runBrainctl(["search", "--repo", repo, "--config", configPath, "--no-sync", "--query", "alpha"]);
      expectSuccess(scoped);
      expect(scoped.stdout).toContain("[work / wiki/Runbook.md:1]");
      expect(scoped.stdout).toContain("[p1 / wiki/Journal.md:1]");
      expect(scoped.stdout).not.toContain("Dump.md");
      const afterOnce = runBrainctl(["search", "--repo", repo, "--config", configPath, "--no-sync", "--query", "alpha"]);
      expectSuccess(afterOnce);
      expect(afterOnce.stdout).not.toContain("[p1 /");

      const malformedRepo = join(dir, "malformed-project");
      await mkdir(malformedRepo, { recursive: true });
      await writeFile(
        join(malformedRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: p1",
          "    label: Personal brain",
          `    localPath: ${personalBrain}`,
          "    classification: personal",
          "    section: [wiki]",
          ""
        ].join("\n")
      );
      const singularSection = runBrainctl(["context", "--repo", malformedRepo, "--config", configPath]);
      expect(singularSection.code).not.toBe(0);
      expect(singularSection.stderr).toContain("use `sections`, not `section`");
      await writeFile(
        join(malformedRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: p1",
          "    label: Personal brain",
          `    localPath: ${personalBrain}`,
          "    classification: personal",
          "    sections: wiki",
          ""
        ].join("\n")
      );
      const scalarSections = runBrainctl(["context", "--repo", malformedRepo, "--config", configPath]);
      expect(scalarSections.code).not.toBe(0);
      expect(scalarSections.stderr).toContain("sections must be a YAML list");
      await writeFile(
        join(malformedRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: p1",
          "    label: Personal brain",
          `    localPath: ${personalBrain}`,
          "    classification: personal",
          "    sections: [../typo]",
          ""
        ].join("\n")
      );
      const unsafeSection = runBrainctl(["context", "--repo", malformedRepo, "--config", configPath]);
      expect(unsafeSection.code).not.toBe(0);
      expect(unsafeSection.stderr).toContain("unsafe section path(s) ../typo");

      const clientEnvPath = join(dir, ".config", "shared-brain.env");
      await mkdir(dirname(clientEnvPath), { recursive: true });
      await writeFile(clientEnvPath, "BRAIN_A_TOKEN=token-a\nBRAIN_A_RENAMED_TOKEN=token-a-renamed\nBRAIN_B_TOKEN=token-b\n");

      const queueWork = runBrainctl(["remember", "--repo", repo, "--target", "work", "--summary", "work memory", "--config", configPath]);
      expectSuccess(queueWork);
      await writeFile(join(repo, ".brainstack.yaml"), (await readFile(join(repo, ".brainstack.yaml"), "utf8")).replace("importTokenEnv: BRAIN_A_TOKEN", "importTokenEnv: BRAIN_A_RENAMED_TOKEN"));
      const queueWorkAfterTokenRename = runBrainctl(["remember", "--repo", repo, "--target", "work", "--summary", "work memory", "--config", configPath]);
      expectSuccess(queueWorkAfterTokenRename);
      const allowPersonal = runBrainctl(["allow", "repo", "--repo", repo, "--brain", "p1", "--always", "--config", configPath]);
      expectSuccess(allowPersonal);
      const queuePersonalBlocked = runBrainctl(["remember", "--repo", repo, "--target", "p1", "--summary", "personal memory", "--config", configPath]);
      expect(queuePersonalBlocked.code).not.toBe(0);
      expect(queuePersonalBlocked.stderr).toContain("without --confirm-cross-brain");
      const queuePersonal = runBrainctl(["remember", "--repo", repo, "--target", "p1", "--summary", "personal memory", "--confirm-cross-brain", "--config", configPath]);
      expectSuccess(queuePersonal);
      const list = runBrainctl(["outbox", "list", "--config", configPath]);
      expectSuccess(list);
      expect(list.stdout).toContain("brain=work");
      expect(list.stdout.match(/brain=work/g) || []).toHaveLength(1);
      expect(list.stdout).toContain("token_env=BRAIN_A_RENAMED_TOKEN");
      expect(list.stdout).toContain("brain=p1");
      expect(list.stdout).toContain("token_env=BRAIN_B_TOKEN");

      const receivedPaths: string[] = [];
      for (const port of [portA, portB]) {
        const receivedPath = join(dir, `received-${port}.jsonl`);
        const serverScript = join(dir, `target-${port}.ts`);
        receivedPaths.push(receivedPath);
        await writeFile(
          serverScript,
          [
            `const port = ${port};`,
            `const receivedPath = ${JSON.stringify(receivedPath)};`,
            "Bun.serve({",
            "  hostname: '127.0.0.1',",
            "  port,",
            "  async fetch(req) {",
            "    const existing = await Bun.file(receivedPath).exists() ? await Bun.file(receivedPath).text() : '';",
            "    await Bun.write(receivedPath, `${existing}${JSON.stringify({ auth: req.headers.get('authorization'), path: new URL(req.url).pathname })}\\n`);",
            "    await req.text();",
            "    return Response.json({ ok: true });",
            "  }",
            "});",
            "await new Promise(() => {});",
            ""
          ].join("\n")
        );
        servers.push(Bun.spawn(["bun", "run", serverScript], { stdout: "pipe", stderr: "pipe" }));
      }
      for (const port of [portA, portB]) {
        await waitForCondition(
          async () => {
            try {
              const response = await fetch(`http://127.0.0.1:${port}/health`);
              return response.ok;
            } catch {
              return false;
            }
          },
          `target brain test server ${port}`,
          20
        );
      }
      const flush = runBrainctl(["outbox", "flush", "--config", configPath]);
      expectSuccess(flush);
      expect(flush.stdout).toContain("flushed=2 kept=0 terminal_failures=0 corrupt=0");
      const received = (
        await Promise.all(
          receivedPaths.map(async (path, index) => {
            const text = await readFile(path, "utf8");
            return text
              .trim()
              .split(/\r?\n/)
              .filter(Boolean)
              .map((line) => ({ port: index === 0 ? portA : portB, ...(JSON.parse(line) as { auth: string | null; path: string }) }));
          })
        )
      ).flat();
      expect(received).toContainEqual({ port: portA, auth: "Bearer token-a-renamed", path: "/api/import" });
      expect(received).toContainEqual({ port: portB, auth: "Bearer token-b", path: "/api/import" });
    } finally {
      for (const server of servers) {
        server.kill();
        await server.exited;
      }
      await rm(dir, { recursive: true, force: true });
    }
  }, 20_000);

  test("project context uses profiles.yaml precedence, clones missing brains, and enforces cross-brain rules", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainstack-profiles-context-"));
    try {
      const configPath = join(dir, "config.yaml");
      const root = join(dir, "install");
      await writeFixtureConfig(configPath);
      await mkdir(join(root, "config"), { recursive: true });

      const workRemote = join(dir, "work-remote.git");
      const personalRemote = join(dir, "personal-remote.git");
      git(["init", "--bare", workRemote], dir);
      git(["init", "--bare", personalRemote], dir);

      async function seedBare(remote: string, filePath: string, contents: string) {
        const clone = join(dir, `seed-${basename(remote)}`);
        git(["clone", remote, clone], dir);
        await mkdir(dirname(join(clone, filePath)), { recursive: true });
        await writeFile(join(clone, filePath), contents);
        git(["add", filePath], clone);
        git(["commit", "-m", `seed ${filePath}`], clone);
        git(["push", "origin", "HEAD:master"], clone);
        git(["symbolic-ref", "HEAD", "refs/heads/master"], remote);
      }
      await seedBare(workRemote, "wiki/Runbook.md", "profile deploy work note\n");
      await seedBare(personalRemote, "shared/work-safe/Debug.md", "profile deploy personal note\n");

      const workClone = join(dir, "clones", "lindy");
      const personalClone = join(dir, "clones", "personal");
      const journalClone = join(dir, "clones", "journal");
      const disabledClone = join(dir, "clones", "disabled");
      await writeFile(
        join(root, "config", "profiles.yaml"),
        [
          "default:",
          "  brains:",
          "    - personal",
          "  writeDefault: personal",
          "brains:",
          "  personal:",
          `    remote: ${personalRemote}`,
          `    localClone: ${personalClone}`,
          "    classification: personal",
          "  journal:",
          `    remote: ${personalRemote}`,
          `    localClone: ${journalClone}`,
          "    classification: personal",
          "  lindy:",
          `    remote: ${workRemote}`,
          `    localClone: ${workClone}`,
          "    classification: work",
          "  disabled:",
          `    remote: ${workRemote}`,
          `    localClone: ${disabledClone}`,
          "    classification: work",
          "  urlonly:",
          "    url: http://127.0.0.1:9",
          "    classification: work",
          "projects:",
          `  \"${join(dir, "projects", "lindy")}/**\":`,
          "    brains:",
          "      - lindy",
          "      - personal",
          "      - journal",
          "      - disabled",
          "      - urlonly",
          "    personal:",
          "      sections:",
          "        - shared/work-safe",
          "      mode: ask-once",
          "    disabled:",
          "      read: false",
          "    writeDefault: lindy",
          "    crossBrainWrites:",
          "      personalToWork: ask",
          "      personalToLindy: ask",
          "      lindyToPersonal: never",
          ""
        ].join("\n")
      );

      const repo = join(dir, "projects", "lindy", "api");
      await mkdir(repo, { recursive: true });
      const firstContext = runBrainctl(["context", "--repo", repo, "--config", configPath, "--root", root]);
      expectSuccess(firstContext);
      expect(firstContext.stdout).toContain("[allowed] lindy");
      expect(firstContext.stdout).toContain("[blocked] personal");
      expect(firstContext.stdout).toContain("[blocked] journal");
      expect(firstContext.stdout).toContain("[blocked] disabled");
      expect(firstContext.stdout).toContain("blocked: disabled has read=false/never");
      expect(firstContext.stdout).toContain("[allowed] urlonly");
      expect(firstContext.stdout).toContain(`path=${join(root, "state", "brain-clones", "urlonly")}`);
      expect(firstContext.stdout).toContain("brainctl allow repo");
      expect(await Bun.file(join(workClone, "wiki", "Runbook.md")).exists()).toBe(true);
      expect(await Bun.file(join(personalClone, "shared", "work-safe", "Debug.md")).exists()).toBe(false);
      expect(await Bun.file(join(journalClone, "shared", "work-safe", "Debug.md")).exists()).toBe(false);
      expect(await Bun.file(join(disabledClone, "wiki", "Runbook.md")).exists()).toBe(false);

      const allow = runBrainctl(["allow", "repo", "--repo", repo, "--brain", "personal", "--sections", "shared/work-safe", "--always", "--config", configPath, "--root", root]);
      expectSuccess(allow);
      const context = runBrainctl(["context", "--repo", repo, "--config", configPath, "--root", root]);
      expectSuccess(context);
      expect(context.stdout).toContain("[allowed] personal");
      expect(context.stdout).toContain("cross_brain_writes=");
      expect(await Bun.file(join(personalClone, "shared", "work-safe", "Debug.md")).exists()).toBe(true);

      const search = runBrainctl(["search", "--repo", repo, "--config", configPath, "--root", root, "deploy"]);
      expectSuccess(search);
      expect(search.stdout).toContain("[lindy / wiki/Runbook.md:1]");
      expect(search.stdout).toContain("[personal / shared/work-safe/Debug.md:1]");
      expect(search.stdout).not.toContain("[disabled /");
      expect(search.stdout).not.toContain("[urlonly /");

      await mkdir(join(workClone, "derived"), { recursive: true });
      const reindexMarker = join(workClone, "derived", "search-reindex-needed.json");
      await writeFile(reindexMarker, '{"reason":"test"}\n');
      const staleSearch = runBrainctl(["search", "--repo", repo, "--config", configPath, "--root", root, "--no-sync", "--query", "deploy"]);
      expectSuccess(staleSearch);
      expect(staleSearch.stderr).toContain("WARN [lindy] search index refresh pending");
      await rm(reindexMarker, { force: true });
      const freshSearch = runBrainctl(["search", "--repo", repo, "--config", configPath, "--root", root, "--no-sync", "--query", "deploy"]);
      expectSuccess(freshSearch);
      expect(freshSearch.stderr).not.toContain("search index refresh pending");

      const remember = runBrainctl(["remember", "--repo", repo, "--target", "lindy", "--summary", "profile deploy memory", "--config", configPath, "--root", root]);
      expect(remember.code).not.toBe(0);
      expect(remember.stderr).toContain("without --confirm-cross-brain");
      const rememberPersonalFromWork = runBrainctl(["remember", "--repo", repo, "--target", "personal", "--summary", "profile deploy memory", "--config", configPath, "--root", root]);
      expect(rememberPersonalFromWork.code).not.toBe(0);
      expect(rememberPersonalFromWork.stderr).toContain("recent lindy sources into personal");
      expect(rememberPersonalFromWork.stderr).toContain("policy is never");

      const weakeningRepo = join(dir, "projects", "lindy", "weakening");
      await mkdir(weakeningRepo, { recursive: true });
      await writeFile(
        join(weakeningRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: personal",
          `    remote: ${personalRemote}`,
          `    localClone: ${personalClone}`,
          "    classification: work",
          "    read: true",
          "crossBrainWrites:",
          "  personalToLindy: allow",
          ""
        ].join("\n")
      );
      const weakeningContext = runBrainctl(["context", "--repo", weakeningRepo, "--config", configPath, "--root", root, "--no-sync"]);
      expect(weakeningContext.code).not.toBe(0);
      expect(weakeningContext.stderr).toContain("cannot weaken trusted profile policy ask to allow");

      const genericWeakeningRepo = join(dir, "projects", "lindy", "generic-weakening");
      await mkdir(genericWeakeningRepo, { recursive: true });
      await writeFile(
        join(genericWeakeningRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: urlonly",
          "    url: http://127.0.0.1:9",
          "    classification: work",
          "    read: true",
          "crossBrainWrites:",
          "  personalToUrlonly: allow",
          ""
        ].join("\n")
      );
      const genericWeakeningContext = runBrainctl(["context", "--repo", genericWeakeningRepo, "--config", configPath, "--root", root, "--no-sync"]);
      expect(genericWeakeningContext.code).not.toBe(0);
      expect(genericWeakeningContext.stderr).toContain("cannot weaken trusted generic profile policy personalToWork=ask to allow");

      const classifiedWeakeningRepo = join(dir, "projects", "lindy", "classified-weakening");
      await mkdir(classifiedWeakeningRepo, { recursive: true });
      await writeFile(
        join(classifiedWeakeningRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: journal",
          "    read: true",
          "  - id: lindy",
          "    read: true",
          "crossBrainWrites:",
          "  journalToLindy: allow",
          ""
        ].join("\n")
      );
      const classifiedWeakeningContext = runBrainctl(["context", "--repo", classifiedWeakeningRepo, "--config", configPath, "--root", root, "--no-sync"]);
      expect(classifiedWeakeningContext.code).not.toBe(0);
      expect(classifiedWeakeningContext.stderr).toContain("cannot weaken trusted/effective profile policy ask for journal->lindy to allow");

      const overrideRepo = join(dir, "projects", "lindy", "override");
      await mkdir(overrideRepo, { recursive: true });
      await writeFile(
        join(overrideRepo, ".brainstack.yaml"),
        [
          "defaultBrain: personal",
          "brains:",
          "  - id: personal",
          `    remote: ${personalRemote}`,
          `    localClone: ${personalClone}`,
          "    read: true",
          ""
        ].join("\n")
      );
      const overrideContext = runBrainctl(["context", "--repo", overrideRepo, "--config", configPath, "--root", root, "--no-sync"]);
      expectSuccess(overrideContext);
      expect(overrideContext.stdout).toContain("write_default=personal");
      expect(overrideContext.stdout).toContain("- [blocked] personal (personal)");

      const hostileRemoteRepo = join(dir, "projects", "local-remote");
      await mkdir(hostileRemoteRepo, { recursive: true });
      await writeFile(
        join(hostileRemoteRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: stolen",
          `    remote: ${personalRemote}`,
          "    sections: [wiki]",
          ""
        ].join("\n")
      );
      const hostileRemote = runBrainctl(["context", "--repo", hostileRemoteRepo, "--config", configPath, "--root", root, "--no-sync"]);
      expect(hostileRemote.code).not.toBe(0);
      expect(hostileRemote.stderr).toContain("cannot set local git remote");

      for (const [name, remote] of [
        ["127-short", "ssh://127.1/tmp/shared-brain.git"],
        ["mapped-loopback", "ssh://[::ffff:127.0.0.1]/tmp/shared-brain.git"],
        ["localhost-dot", "ssh://localhost./tmp/shared-brain.git"]
      ] as const) {
        const repoWithLoopbackRemote = join(dir, "projects", name);
        await mkdir(repoWithLoopbackRemote, { recursive: true });
        await writeFile(
          join(repoWithLoopbackRemote, ".brainstack.yaml"),
          [
            "brains:",
            `  - id: ${name}`,
            `    remote: ${remote}`,
            "    sections: [wiki]",
            ""
          ].join("\n")
        );
        const loopbackRemote = runBrainctl(["context", "--repo", repoWithLoopbackRemote, "--config", configPath, "--root", root, "--no-sync"]);
        expect(loopbackRemote.code).not.toBe(0);
        expect(loopbackRemote.stderr).toContain("cannot set local git remote");
      }

      const escapedCloneRepo = join(dir, "projects", "escaped-clone");
      await mkdir(escapedCloneRepo, { recursive: true });
      await writeFile(
        join(escapedCloneRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: escaped",
          `    localClone: ${root}/state/brain-clones/../escaped`,
          "    url: http://127.0.0.1:9",
          "    sections: [wiki]",
          ""
        ].join("\n")
      );
      const escapedClone = runBrainctl(["context", "--repo", escapedCloneRepo, "--config", configPath, "--root", root, "--no-sync"]);
      expect(escapedClone.code).not.toBe(0);
      expect(escapedClone.stderr).toContain("cannot set localClone/localPath/path outside");

      const dotRepo = join(dir, "projects", "dotty");
      await mkdir(dotRepo, { recursive: true });
      await writeFile(
        join(dotRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: ..",
          "    url: http://127.0.0.1:9",
          "    classification: work",
          ""
        ].join("\n")
      );
      const dotContext = runBrainctl(["context", "--repo", dotRepo, "--config", configPath, "--root", root, "--no-sync"]);
      expectSuccess(dotContext);
      expect(dotContext.stdout).toContain(`path=${join(root, "state", "brain-clones", `brain-${sha256Text("..").slice(0, 16)}`)}`);

      const reservedIdRepo = join(dir, "projects", "reserved-id");
      await mkdir(reservedIdRepo, { recursive: true });
      await writeFile(
        join(reservedIdRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: cross:personal->work",
          "    url: http://127.0.0.1:9",
          "    classification: work",
          ""
        ].join("\n")
      );
      const reservedIdContext = runBrainctl(["context", "--repo", reservedIdRepo, "--config", configPath, "--root", root, "--no-sync"]);
      expect(reservedIdContext.code).not.toBe(0);
      expect(reservedIdContext.stderr).toContain("reserved prefix cross:");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("outbox queues unreachable writes and flushes later", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-outbox-"));
    const port = 41_000 + Math.floor(Math.random() * 5_000);
    const received: Array<Record<string, unknown>> = [];
    try {
      const configPath = join(dir, "config.yaml");
      const stateRoot = join(dir, "state");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${stateRoot}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const env = {
        BRAIN_BASE_URL: `http://127.0.0.1:${port}`,
        BRAIN_IMPORT_TOKEN: "outbox-token",
        BRAINSTACK_BRAIN_WRITE_TIMEOUT_MS: "1000"
      };
      const queued = runBrainctl(
        [
          "import-text",
          "--config",
          configPath,
          "--title",
          "Queued note",
          "--text",
          "offline body",
          "--source-harness",
          "codex",
          "--source-machine",
          "client"
        ],
        env
      );
      expectSuccess(queued);
      expect(`${queued.stdout}\n${queued.stderr}`).toContain("shared-brain write queued");
      const queuedAgain = runBrainctl(
        [
          "import-text",
          "--config",
          configPath,
          "--title",
          "Queued note",
          "--text",
          "offline body",
          "--source-harness",
          "codex",
          "--source-machine",
          "client"
        ],
        env
      );
      expectSuccess(queuedAgain);
      const statusBefore = runBrainctl(["outbox", "status", "--config", configPath], env);
      expectSuccess(statusBefore);
      expect(statusBefore.stdout).toContain("queued=1");
      const queuedPathLine = `${queued.stdout}\n${queued.stderr}`
        .split(/\r?\n/)
        .find((line) => line.includes("shared-brain write queued:"));
      const queuedItemPath = queuedPathLine?.replace(/^.*shared-brain write queued:\s*/, "").trim();
      const outboxPath = queuedItemPath ? dirname(queuedItemPath) : undefined;
      expect(typeof outboxPath).toBe("string");
      await writeFile(
        join(outboxPath!, "legacy-timestamp-id.json"),
        `${JSON.stringify(
          {
            id: "import-legacy-timestamp",
            endpoint: "import",
            url: `http://127.0.0.1:${port}`,
            payload: {
              title: "Queued note",
              text: "offline body",
              source_harness: "codex",
              source_machine: "client",
              source_type: "note",
              tags: []
            },
            created_at: "2026-01-01T00:00:00.000Z",
            source_machine: "client",
            source_harness: "codex",
            retry_count: 2,
            idempotency_key: "legacy-json-stringify-key",
            last_error: "legacy item"
          },
          null,
          2
        )}\n`
      );

      const receivedPath = join(dir, "received.jsonl");
      const serverScript = join(dir, "server.ts");
      await writeFile(
        serverScript,
        [
          `const port = ${port};`,
          `const receivedPath = ${JSON.stringify(receivedPath)};`,
          "Bun.serve({",
          "  hostname: '127.0.0.1',",
          "  port,",
          "  async fetch(req) {",
          "    if (new URL(req.url).pathname === '/health') return Response.json({ ok: true });",
          "    const payload = await req.json();",
          "    const key = req.headers.get('idempotency-key');",
          "    const existing = await Bun.file(receivedPath).exists() ? await Bun.file(receivedPath).text() : '';",
          "    await Bun.write(receivedPath, `${existing}${JSON.stringify({ payload, key })}\\n`);",
          "    return Response.json({ ok: true });",
          "  }",
          "});",
          "await new Promise(() => {});",
          ""
        ].join("\n")
      );
      const server = Bun.spawn(["bun", "run", serverScript], {
        stdout: "pipe",
        stderr: "pipe"
      });
      try {
        for (let attempt = 0; attempt < 20; attempt += 1) {
          try {
            await fetch(`http://127.0.0.1:${port}/health`);
            break;
          } catch {
            await Bun.sleep(25);
          }
        }
        const flush = runBrainctl(["outbox", "flush", "--config", configPath], env);
        expectSuccess(flush);
        expect(flush.stdout).toContain("flushed=1 kept=0");
        const receivedText = await readFile(receivedPath, "utf8");
        received.push(...receivedText.trim().split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line) as Record<string, unknown>));
        expect(received).toHaveLength(1);
        expect(received[0].key).toMatch(/^[a-f0-9]{64}$/);
        expect(received[0].key).not.toBe("legacy-json-stringify-key");
        const statusAfter = runBrainctl(["outbox", "status", "--config", configPath], env);
        expectSuccess(statusAfter);
        expect(statusAfter.stdout).toContain("queued=0");
      } finally {
        server.kill();
        await server.exited;
      }
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 15_000);

  test("outbox warns on large queued payloads and refuses above hard cap without truncation", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-outbox-large-"));
    try {
      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${join(dir, "state")}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const largeQueued = runBrainctl(
        ["import-text", "--config", configPath, "--title", "Large queued", "--text", "x".repeat(2048), "--source-harness", "codex", "--source-machine", "client"],
        {
          BRAIN_BASE_URL: "http://127.0.0.1:1",
          BRAIN_IMPORT_TOKEN: "token",
          BRAINSTACK_BRAIN_WRITE_TIMEOUT_MS: "100",
          BRAINSTACK_OUTBOX_SOFT_WARN_BYTES: "100",
          BRAINSTACK_OUTBOX_COMPRESS_ABOVE_BYTES: "100",
          BRAINSTACK_OUTBOX_HARD_MAX_BYTES: "100000"
        }
      );
      expectSuccess(largeQueued);
      expect(largeQueued.stderr).toContain("WARN queued large outbox item");
      expect(largeQueued.stderr).toContain("No content was truncated");

      const refused = runBrainctl(
        ["import-text", "--config", configPath, "--title", "Too large queued", "--text", "x".repeat(2048), "--source-harness", "codex", "--source-machine", "client"],
        {
          BRAIN_BASE_URL: "http://127.0.0.1:2",
          BRAIN_IMPORT_TOKEN: "token",
          BRAINSTACK_BRAIN_WRITE_TIMEOUT_MS: "100",
          BRAINSTACK_OUTBOX_HARD_MAX_BYTES: "100"
        }
      );
      expect(refused.code).not.toBe(0);
      expect(refused.stderr).toContain("exceeds hard cap 100");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("outbox queues retryable brain write failures but rejects auth failures", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-outbox-status-"));
    try {
      const stateRoot = join(dir, "state");
      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${stateRoot}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );

      const retryablePort = 46_000 + Math.floor(Math.random() * 2_000);
      const retryableScript = join(dir, "retryable-server.ts");
      await writeFile(
        retryableScript,
        [
          `Bun.serve({`,
          `  hostname: "127.0.0.1",`,
          `  port: ${retryablePort},`,
          `  fetch() { return Response.json({ error: "try later" }, { status: 503 }); }`,
          `});`,
          `await new Promise(() => {});`,
          ""
        ].join("\n")
      );
      const retryableServer = Bun.spawn(["bun", "run", retryableScript], {
        stdout: "pipe",
        stderr: "pipe"
      });
      try {
        for (let attempt = 0; attempt < 20; attempt += 1) {
          try {
            await fetch(`http://127.0.0.1:${retryablePort}/health`);
            break;
          } catch {
            await Bun.sleep(25);
          }
        }
        const retryable = runBrainctl(
          [
            "import-text",
            "--config",
            configPath,
            "--title",
            "Retryable note",
            "--text",
            "retryable body",
            "--source-harness",
            "codex",
            "--source-machine",
            "client"
          ],
          {
            BRAIN_BASE_URL: `http://127.0.0.1:${retryablePort}`,
            BRAIN_IMPORT_TOKEN: "outbox-token",
            BRAINSTACK_BRAIN_WRITE_TIMEOUT_MS: "1000"
          }
        );
        expectSuccess(retryable);
        expect(`${retryable.stdout}\n${retryable.stderr}`).toContain("shared-brain write queued");
        const status = runBrainctl(["outbox", "status", "--config", configPath]);
        expectSuccess(status);
        expect(status.stdout).toContain("queued=1");
      } finally {
        retryableServer.kill();
        await retryableServer.exited;
      }

      const cleanStateRoot = join(dir, "auth-state");
      const authConfigPath = join(dir, "auth-config.yaml");
      await writeFile(
        authConfigPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${cleanStateRoot}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const authPort = 48_000 + Math.floor(Math.random() * 2_000);
      const authScript = join(dir, "auth-server.ts");
      await writeFile(
        authScript,
        [
          `Bun.serve({`,
          `  hostname: "127.0.0.1",`,
          `  port: ${authPort},`,
          `  fetch() { return Response.json({ error: "unauthorized" }, { status: 401 }); }`,
          `});`,
          `await new Promise(() => {});`,
          ""
        ].join("\n")
      );
      const authServer = Bun.spawn(["bun", "run", authScript], {
        stdout: "pipe",
        stderr: "pipe"
      });
      try {
        for (let attempt = 0; attempt < 20; attempt += 1) {
          try {
            await fetch(`http://127.0.0.1:${authPort}/health`);
            break;
          } catch {
            await Bun.sleep(25);
          }
        }
        const rejected = runBrainctl(
          [
            "import-text",
            "--config",
            authConfigPath,
            "--title",
            "Auth note",
            "--text",
            "auth body",
            "--source-harness",
            "codex",
            "--source-machine",
            "client"
          ],
          {
            BRAIN_BASE_URL: `http://127.0.0.1:${authPort}`,
            BRAIN_IMPORT_TOKEN: "outbox-token",
            BRAINSTACK_BRAIN_WRITE_TIMEOUT_MS: "1000"
          }
        );
        expect(rejected.code).not.toBe(0);
        expect(rejected.stderr).toContain("brain rejected import with HTTP 401");
        const authStatus = runBrainctl(["outbox", "status", "--config", authConfigPath]);
        expectSuccess(authStatus);
        expect(authStatus.stdout).toContain("queued=0");
      } finally {
        authServer.kill();
        await authServer.exited;
      }

      const legacyRejected = runBrainctl(
        [
          "import-text",
          "--config",
          authConfigPath,
          "--title",
          "Legacy token note",
          "--text",
          "legacy token body",
          "--source-harness",
          "codex",
          "--source-machine",
          "client"
        ],
        {
          BRAIN_BASE_URL: `http://127.0.0.1:${authPort}`,
          BRAIN_WRITE_TOKEN: "legacy-token"
        }
      );
      expect(legacyRejected.code).not.toBe(0);
      expect(legacyRejected.stderr).toContain("BRAIN_WRITE_TOKEN is no longer accepted");

      const terminalStateRoot = join(dir, "terminal-state");
      const terminalConfigPath = join(dir, "terminal-config.yaml");
      await writeFile(
        terminalConfigPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${terminalStateRoot}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const terminalStatus = runBrainctl(["outbox", "status", "--config", terminalConfigPath], {
        BRAIN_BASE_URL: `http://127.0.0.1:${authPort}`,
        BRAIN_IMPORT_TOKEN: "outbox-token"
      });
      expectSuccess(terminalStatus);
      const terminalRoot = terminalStatus.stdout.split(/\r?\n/).find((line) => line.startsWith("outbox="))?.slice("outbox=".length);
      expect(typeof terminalRoot).toBe("string");
      await mkdir(terminalRoot!, { recursive: true });
      await writeFile(
        join(terminalRoot!, "legacy-terminal.json"),
        `${JSON.stringify(
          {
            id: "legacy-terminal",
            endpoint: "import",
            url: `http://127.0.0.1:${authPort}`,
            payload: {
              title: "Terminal note",
              text: "terminal body",
              source_harness: "codex",
              source_machine: "client",
              source_type: "note",
              tags: []
            },
            created_at: "2026-01-01T00:00:00.000Z",
            source_machine: "client",
            source_harness: "codex",
            retry_count: 0,
            idempotency_key: "legacy-terminal-key",
            last_error: null
          },
          null,
          2
        )}\n`
      );
      const terminalServer = Bun.spawn(["bun", "run", authScript], {
        stdout: "pipe",
        stderr: "pipe"
      });
      try {
        for (let attempt = 0; attempt < 20; attempt += 1) {
          try {
            await fetch(`http://127.0.0.1:${authPort}/health`);
            break;
          } catch {
            await Bun.sleep(25);
          }
        }
        const terminalFlush = runBrainctl(["outbox", "flush", "--config", terminalConfigPath], {
          BRAIN_BASE_URL: `http://127.0.0.1:${authPort}`,
          BRAIN_IMPORT_TOKEN: "outbox-token",
          BRAINSTACK_BRAIN_WRITE_TIMEOUT_MS: "1000"
        });
        expect(terminalFlush.code).not.toBe(0);
        expect(terminalFlush.stdout).toContain("terminal_failures=1");
        expect(terminalFlush.stderr).toContain("terminal write failures");
        const terminalList = runBrainctl(["outbox", "list", "--config", terminalConfigPath], {
          BRAIN_BASE_URL: `http://127.0.0.1:${authPort}`,
          BRAIN_IMPORT_TOKEN: "outbox-token"
        });
        expectSuccess(terminalList);
        expect(terminalList.stdout).toContain("status=terminal");
        expect(terminalList.stdout).toContain("HTTP 401");
      } finally {
        terminalServer.kill();
        await terminalServer.exited;
      }
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 20_000);

  test("outbox flush refuses corrupt entries before coalescing valid queued files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-outbox-corrupt-before-coalesce-"));
    try {
      const configPath = join(dir, "config.yaml");
      const stateRoot = join(dir, "state");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${stateRoot}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const status = runBrainctl(["outbox", "status", "--config", configPath]);
      expectSuccess(status);
      const outboxPath = status.stdout.split(/\r?\n/).find((line) => line.startsWith("outbox="))?.slice("outbox=".length);
      expect(typeof outboxPath).toBe("string");
      await mkdir(outboxPath!, { recursive: true });
      const payload = {
        title: "Valid queued note",
        text: "valid body",
        source_harness: "codex",
        source_machine: "client",
        source_type: "note",
        tags: []
      };
      const valid = {
        endpoint: "import",
        url: "http://127.0.0.1:1",
        payload,
        created_at: "2026-01-01T00:00:00.000Z",
        source_machine: "client",
        source_harness: "codex",
        retry_count: 0,
        idempotency_key: "same-valid-key",
        last_error: null
      };
      await writeFile(join(outboxPath!, "import-valid-a.json"), `${JSON.stringify({ id: "import-valid-a", ...valid }, null, 2)}\n`);
      await writeFile(join(outboxPath!, "import-valid-b.json"), `${JSON.stringify({ id: "import-valid-b", ...valid }, null, 2)}\n`);
      await writeFile(
        join(outboxPath!, "import-corrupt-json.json"),
        `${JSON.stringify(
          {
            id: "import-corrupt-json",
            endpoint: "import",
            url: "http://127.0.0.1:1",
            payload_storage: {
              encoding: "json",
              data: { ...payload, text: "mutated body" },
              uncompressed_bytes: new TextEncoder().encode(JSON.stringify(payload)).byteLength,
              stored_bytes: new TextEncoder().encode(JSON.stringify(payload)).byteLength,
              sha256: sha256Text(JSON.stringify(payload))
            },
            created_at: "2026-01-01T00:00:00.000Z",
            source_machine: "client",
            source_harness: "codex",
            retry_count: 0,
            idempotency_key: "corrupt-json-key",
            last_error: null
          },
          null,
          2
        )}\n`
      );

      const flush = runBrainctl(["outbox", "flush", "--config", configPath], {
        BRAIN_BASE_URL: "http://127.0.0.1:1",
        BRAIN_IMPORT_TOKEN: "token"
      });
      expect(flush.code).not.toBe(0);
      expect(flush.stdout).toContain("flushed=0 kept=2 terminal_failures=0 corrupt=1");
      expect(await Bun.file(join(outboxPath!, "import-valid-a.json")).exists()).toBe(true);
      expect(await Bun.file(join(outboxPath!, "import-valid-b.json")).exists()).toBe(true);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("outbox surfaces corrupt json payloads, oversize legacy entries, and stale temp files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-outbox-corrupt-"));
    try {
      const configPath = join(dir, "config.yaml");
      const stateRoot = join(dir, "state");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${stateRoot}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const status = runBrainctl(["outbox", "status", "--config", configPath]);
      expectSuccess(status);
      const outboxPath = status.stdout.split(/\r?\n/).find((line) => line.startsWith("outbox="))?.slice("outbox=".length);
      expect(typeof outboxPath).toBe("string");
      await mkdir(outboxPath!, { recursive: true });
      await chmod(dirname(outboxPath!), 0o755);

      const payload = {
        title: "Corrupt note",
        text: "original body",
        source_harness: "codex",
        source_machine: "client",
        source_type: "note",
        tags: []
      };
      await writeFile(
        join(outboxPath!, "import-corrupt-json.json"),
        `${JSON.stringify(
          {
            id: "import-corrupt-json",
            endpoint: "import",
            url: "http://127.0.0.1:1",
            payload_storage: {
              encoding: "json",
              data: { ...payload, text: "mutated body" },
              uncompressed_bytes: new TextEncoder().encode(JSON.stringify(payload)).byteLength,
              stored_bytes: new TextEncoder().encode(JSON.stringify(payload)).byteLength,
              sha256: sha256Text(JSON.stringify(payload))
            },
            created_at: "2026-01-01T00:00:00.000Z",
            source_machine: "client",
            source_harness: "codex",
            retry_count: 0,
            idempotency_key: "corrupt-json-key",
            last_error: null
          },
          null,
          2
        )}\n`
      );
      await writeFile(
        join(outboxPath!, "legacy-oversize.json"),
        `${JSON.stringify({
          id: "legacy-oversize",
          endpoint: "import",
          url: "http://127.0.0.1:1",
          payload: {
            ...payload,
            title: "Oversize note",
            text: "x".repeat(256)
          },
          created_at: "2026-01-01T00:00:00.000Z",
          source_machine: "client",
          source_harness: "codex",
          retry_count: 0,
          idempotency_key: "legacy-oversize-key",
          last_error: null
        })}\n`
      );
      await writeFile(join(outboxPath!, ".import-partial.json.123.tmp"), "partial sensitive payload");
      await chmod(join(outboxPath!, ".import-partial.json.123.tmp"), 0o644);
      const bombRaw = new TextEncoder().encode(JSON.stringify({ ...payload, title: "Compressed bomb", text: "z".repeat(512) }));
      const bombCompressed = gzipSync(bombRaw);
      await writeFile(
        join(outboxPath!, "compressed-bomb.json"),
        `${JSON.stringify({
          id: "compressed-bomb",
          endpoint: "import",
          url: "http://127.0.0.1:1",
          payload_storage: {
            encoding: "json-gzip-base64",
            data: bombCompressed.toString("base64"),
            uncompressed_bytes: 64,
            stored_bytes: bombCompressed.byteLength,
            sha256: sha256Text(Buffer.from(bombRaw).toString("utf8"))
          },
          created_at: "2026-01-01T00:00:00.000Z",
          source_machine: "client",
          source_harness: "codex",
          retry_count: 0,
          idempotency_key: "compressed-bomb-key",
          last_error: null
        })}\n`
      );
      const outsideOutbox = join(dir, "outside-outbox");
      await mkdir(outsideOutbox, { recursive: true });
      await writeFile(join(outsideOutbox, "should-survive.json"), "{}\n");
      await symlink(outsideOutbox, join(dirname(outboxPath!), "symlink-namespace"));

      const env = { BRAINSTACK_OUTBOX_HARD_MAX_BYTES: "128" };
      const corruptStatus = runBrainctl(["outbox", "status", "--config", configPath], env);
      expectSuccess(corruptStatus);
      expect(corruptStatus.stdout).toContain("queued=0");
      expect(corruptStatus.stdout).toContain("corrupt=5");
      const corruptList = runBrainctl(["outbox", "list", "--config", configPath], env);
      expectSuccess(corruptList);
      expect(corruptList.stdout).toContain("CORRUPT import-corrupt-json.json");
      expect(corruptList.stdout).toContain("json payload");
      expect(corruptList.stdout).toContain("CORRUPT legacy-oversize.json");
      expect(corruptList.stdout).toContain("exceeds hard cap 128");
      expect(corruptList.stdout).toContain("CORRUPT .import-partial.json.123.tmp");
      expect(corruptList.stdout).toContain("CORRUPT compressed-bomb.json");
      expect(corruptList.stdout).toContain("CORRUPT symlink-namespace");
      const flush = runBrainctl(["outbox", "flush", "--config", configPath], env);
      expect(flush.code).not.toBe(0);
      expect(flush.stdout).toContain("flushed=0 kept=0 terminal_failures=0 corrupt=5");
      expect(flush.stderr).toContain("corrupt/unsafe entries");
      const doctor = runBrainctl(["doctor", "--config", configPath], env);
      expect(doctor.code).not.toBe(0);
      expect(doctor.stdout).toContain("FAIL [outbox] corrupt-items: 5 corrupt/unsafe item(s)");
      expect(doctor.stdout).toContain("WARN [outbox] permissions:");
      expect(doctor.stdout).toContain("FAIL [outbox] file-permissions:");
      const purge = runBrainctl(["outbox", "purge-corrupt", "--yes", "--config", configPath], env);
      expectSuccess(purge);
      expect(purge.stdout).toContain("purged_corrupt=5");
      expect(await Bun.file(join(outboxPath!, ".import-partial.json.123.tmp")).exists()).toBe(false);
      expect(await Bun.file(join(outsideOutbox, "should-survive.json")).exists()).toBe(true);
      const otherOutboxPath = join(dirname(outboxPath!), "target-specific-queue");
      await mkdir(otherOutboxPath, { recursive: true });
      await writeFile(join(outboxPath!, "default-leftover.json"), "{}\n");
      await writeFile(join(otherOutboxPath, "target-leftover.json"), "{}\n");
      const purgeAll = runBrainctl(["outbox", "purge", "--yes", "--config", configPath], env);
      expectSuccess(purgeAll);
      expect(purgeAll.stdout).toContain("purged=2");
      expect(await Bun.file(outboxPath!).exists()).toBe(false);
      expect(await Bun.file(otherOutboxPath).exists()).toBe(false);

      await mkdir(stateRoot, { recursive: true });
      const outsideParent = join(dir, "outside-parent-outbox");
      await mkdir(outsideParent, { recursive: true });
      await rm(join(stateRoot, "outbox"), { recursive: true, force: true });
      await symlink(outsideParent, join(stateRoot, "outbox"));
      const queueThroughSymlink = runBrainctl(["import-text", "--title", "unsafe", "--text", "payload", "--config", configPath], env);
      expect(queueThroughSymlink.code).not.toBe(0);
      expect(queueThroughSymlink.stderr).toContain("outbox path ancestor is a symlink");
      expect((await readdir(outsideParent)).length).toBe(0);
      const symlinkList = runBrainctl(["outbox", "list", "--config", configPath], env);
      expectSuccess(symlinkList);
      expect(symlinkList.stdout).toContain("CORRUPT outbox");
      const unsafePurge = runBrainctl(["outbox", "purge", "--yes", "--config", configPath], env);
      expect(unsafePurge.code).not.toBe(0);
      expect(unsafePurge.stderr).toContain("purge-corrupt");
      const symlinkPurge = runBrainctl(["outbox", "purge-corrupt", "--yes", "--config", configPath], env);
      expectSuccess(symlinkPurge);
      expect(await Bun.file(join(stateRoot, "outbox")).exists()).toBe(false);
      expect(existsSync(outsideParent)).toBe(true);

      const realStateRoot = join(dir, "real-state-root");
      const stateRootSymlink = join(dir, "state-root-symlink");
      const symlinkStateConfig = join(dir, "symlink-state-config.yaml");
      await mkdir(realStateRoot, { recursive: true });
      await symlink(realStateRoot, stateRootSymlink);
      await writeFile(
        symlinkStateConfig,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${stateRootSymlink}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const ancestorSymlinkStatus = runBrainctl(["outbox", "status", "--config", symlinkStateConfig], env);
      expectSuccess(ancestorSymlinkStatus);
      expect(ancestorSymlinkStatus.stdout).toContain("corrupt=1");
      const ancestorSymlinkList = runBrainctl(["outbox", "list", "--config", symlinkStateConfig], env);
      expectSuccess(ancestorSymlinkList);
      expect(ancestorSymlinkList.stdout).toContain("CORRUPT state-root-symlink");
      expect(ancestorSymlinkList.stdout).toContain("outbox path ancestor is a symlink");

      const fileStateRoot = join(dir, "file-state-root");
      const fileStateConfig = join(dir, "file-state-config.yaml");
      await writeFile(fileStateRoot, "not a directory\n");
      await writeFile(
        fileStateConfig,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${fileStateRoot}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const fileStateList = runBrainctl(["outbox", "list", "--config", fileStateConfig], env);
      expectSuccess(fileStateList);
      expect(fileStateList.stdout).toContain("CORRUPT file-state-root");
      const fileStatePurge = runBrainctl(["outbox", "purge-corrupt", "--yes", "--config", fileStateConfig], env);
      expectSuccess(fileStatePurge);
      expect(fileStatePurge.stdout).toContain("purged_corrupt=0");
      expect(await Bun.file(fileStateRoot).exists()).toBe(true);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("outbox moves repeated HTTP 425 idempotency responses to terminal review", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-outbox-425-"));
    const port = 47_000 + Math.floor(Math.random() * 1_000);
    let server: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const stateRoot = join(dir, "state");
      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${stateRoot}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const serverScript = join(dir, "pending-server.ts");
      await writeFile(
        serverScript,
        [
          `Bun.serve({`,
          `  hostname: "127.0.0.1",`,
          `  port: ${port},`,
          `  fetch() { return Response.json({ error: "idempotent request is already in progress" }, { status: 425 }); }`,
          `});`,
          `await new Promise(() => {});`,
          ""
        ].join("\n")
      );
      server = Bun.spawn(["bun", "run", serverScript], { stdout: "pipe", stderr: "pipe" });
      for (let attempt = 0; attempt < 20; attempt += 1) {
        try {
          await fetch(`http://127.0.0.1:${port}/health`);
          break;
        } catch {
          await Bun.sleep(25);
        }
      }
      const env = {
        BRAIN_BASE_URL: `http://127.0.0.1:${port}`,
        BRAIN_IMPORT_TOKEN: "outbox-token",
        BRAINSTACK_BRAIN_WRITE_TIMEOUT_MS: "1000",
        BRAINSTACK_OUTBOX_MAX_425_RETRIES: "2"
      };
      const queued = runBrainctl(
        [
          "import-text",
          "--config",
          configPath,
          "--title",
          "Pending note",
          "--text",
          "pending body",
          "--source-harness",
          "codex",
          "--source-machine",
          "client"
        ],
        env
      );
      expectSuccess(queued);
      const firstFlush = runBrainctl(["outbox", "flush", "--config", configPath], env);
      expectSuccess(firstFlush);
      expect(firstFlush.stdout).toContain("flushed=0 kept=1 terminal_failures=0");
      const secondFlush = runBrainctl(["outbox", "flush", "--config", configPath], env);
      expect(secondFlush.code).not.toBe(0);
      expect(secondFlush.stdout).toContain("terminal_failures=1");
      const listed = runBrainctl(["outbox", "list", "--config", configPath], env);
      expectSuccess(listed);
      expect(listed.stdout).toContain("status=terminal");
      expect(listed.stdout).toContain("HTTP 425 persisted");

      const proposeConfigPath = join(dir, "config-propose.yaml");
      await writeFile(
        proposeConfigPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${join(dir, "home-propose")}`,
          `  stateRoot: ${join(dir, "state-propose")}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const queuedProposal = runBrainctl(
        [
          "propose",
          "--config",
          proposeConfigPath,
          "--title",
          "Pending proposal",
          "--body",
          "pending proposal body"
        ],
        env
      );
      expectSuccess(queuedProposal);
      const firstProposalFlush = runBrainctl(["outbox", "flush", "--config", proposeConfigPath], env);
      expectSuccess(firstProposalFlush);
      expect(firstProposalFlush.stdout).toContain("flushed=0 kept=1 terminal_failures=0");
      const secondProposalFlush = runBrainctl(["outbox", "flush", "--config", proposeConfigPath], env);
      expect(secondProposalFlush.code).not.toBe(0);
      expect(secondProposalFlush.stdout).toContain("terminal_failures=1");
      const listedProposal = runBrainctl(["outbox", "list", "--config", proposeConfigPath], env);
      expectSuccess(listedProposal);
      expect(listedProposal.stdout).toContain("propose status=terminal");
      expect(listedProposal.stdout).toContain("HTTP 425 persisted");
    } finally {
      if (server) {
        server.kill();
        await server.exited;
      }
      await rm(dir, { recursive: true, force: true });
    }
  }, 20_000);

  test("updates command is read-only and reports versions", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-updates-"));
    try {
      const binDir = join(dir, "bin");
      const configPath = join(dir, "config.yaml");
      await mkdir(binDir, { recursive: true });
      await writeFile(
        join(binDir, "codex"),
        "#!/usr/bin/env sh\nif [ \"${1:-}\" = \"--version\" ]; then printf 'codex-cli 0.133.0\\n'; exit 0; fi\nprintf '%s\\n' '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --output-last-message'\n"
      );
      await writeFile(
        join(binDir, "claude"),
        "#!/usr/bin/env sh\nif [ \"${1:-}\" = \"--version\" ]; then printf '2.1.133 (Claude Code)\\n'; exit 0; fi\nprintf '%s\\n' '--dangerously-skip-permissions --permission-mode --output-format'\n"
      );
      await writeFile(
        join(binDir, "brew"),
        "#!/usr/bin/env sh\nif [ \"${HOMEBREW_NO_AUTO_UPDATE:-}\" != \"1\" ]; then echo 'brew auto-update was not disabled' >&2; exit 42; fi\nprintf 'brew-pkg 1.0 -> 1.1\\n'\n"
      );
      await writeFile(
        join(binDir, "pacman"),
        "#!/usr/bin/env sh\nif [ \"${PACMAN_FAIL:-}\" = \"1\" ]; then echo 'database read failed' >&2; exit 1; fi\nif [ \"${PACMAN_EMPTY:-}\" = \"1\" ]; then exit 1; fi\nprintf 'codex-cli 0.134.0-1 -> 0.135.0-1\\n'\n"
      );
      await writeFile(join(binDir, "sleepy-shell"), "#!/usr/bin/env sh\nsleep 5\n");
      await chmod(join(binDir, "codex"), 0o755);
      await chmod(join(binDir, "claude"), 0o755);
      await chmod(join(binDir, "brew"), 0o755);
      await chmod(join(binDir, "pacman"), 0o755);
      await chmod(join(binDir, "sleepy-shell"), 0o755);
      await writeFixtureConfig(configPath);
      const result = runBrainctl(["updates", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
        BRAINSTACK_UPDATE_PROBE_COMMANDS: "brew,pacman"
      });
      expectSuccess(result);
      expect(result.stdout).toContain("brainstack_head=");
      expect(result.stdout).toContain("remote_main_ref=");
      expect(result.stdout).toContain("os_update_checks:");
      expect(result.stdout).toContain("brew outdated --quiet (HOMEBREW_NO_AUTO_UPDATE=1):");
      expect(result.stdout).toContain("brew-pkg 1.0 -> 1.1");
      expect(result.stdout).toContain("pacman -Qu:");
      expect(result.stdout).toContain("codex-cli 0.134.0-1 -> 0.135.0-1");
      expect(result.stdout).toContain("manual_update_commands:");
      expect(result.stdout).toContain("selected harness:");

      await writeFile(
        join(binDir, "codex"),
        "#!/usr/bin/env sh\nif [ \"${1:-}\" = \"--version\" ]; then printf 'codex-cli missing-output\\n'; exit 0; fi\nprintf '%s\\n' '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check'\n"
      );
      await chmod(join(binDir, "codex"), 0o755);
      const missingOutputFlag = runBrainctl(["updates", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
        BRAINSTACK_UPDATE_PROBE_COMMANDS: "none"
      });
      expectSuccess(missingOutputFlag);
      expect(missingOutputFlag.stdout).toContain("FAIL codex-harness: codex-cli missing-output; missing required CLI surface: --output-last-message");

      await writeFile(
        join(binDir, "codex"),
        "#!/usr/bin/env sh\nif [ \"${1:-}\" = \"--version\" ]; then printf 'codex-cli 0.133.0\\n'; exit 0; fi\nprintf '%s\\n' '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --output-last-message'\n"
      );
      await chmod(join(binDir, "codex"), 0o755);

      const noUpdates = runBrainctl(["updates", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
        BRAINSTACK_UPDATE_PROBE_COMMANDS: "brew,pacman",
        PACMAN_EMPTY: "1"
      });
      expectSuccess(noUpdates);
      expect(noUpdates.stdout).toContain("pacman -Qu:\n    ok\n    (none)");

      const pacmanFailure = runBrainctl(["updates", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
        BRAINSTACK_UPDATE_PROBE_COMMANDS: "brew,pacman",
        PACMAN_FAIL: "1"
      });
      expectSuccess(pacmanFailure);
      expect(pacmanFailure.stdout).toContain("pacman -Qu:\n    exit-1\n    database read failed");

      await writeFile(join(binDir, "brew"), "#!/usr/bin/env sh\nsleep 5\n");
      await chmod(join(binDir, "brew"), 0o755);
      const timeout = runBrainctl(["updates", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
        BRAINSTACK_UPDATE_PROBE_COMMANDS: "brew",
        BRAINSTACK_UPDATE_PROBE_TIMEOUT_MS: "100"
      });
      expectSuccess(timeout);
      expect(timeout.stdout).toContain("brew outdated --quiet (HOMEBREW_NO_AUTO_UPDATE=1):\n    exit-124");
      expect(timeout.stdout).toContain("timed out after 100ms");

      const shellPathTimeout = runBrainctl(["updates", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        SHELL: join(binDir, "sleepy-shell"),
        BRAINSTACK_SHELL_PATH_TIMEOUT_MS: "100",
        BRAINSTACK_UPDATE_PROBE_COMMANDS: "pacman",
        PACMAN_EMPTY: "1"
      });
      expectSuccess(shellPathTimeout);
      expect(shellPathTimeout.stdout).toContain("pacman -Qu:\n    ok\n    (none)");

      await writeFile(join(binDir, "codex"), "#!/usr/bin/env sh\nsleep 5\n");
      await chmod(join(binDir, "codex"), 0o755);
      const harnessTimeout = runBrainctl(["updates", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
        BRAINSTACK_UPDATE_PROBE_COMMANDS: "none",
        BRAINSTACK_UPDATE_PROBE_TIMEOUT_MS: "100"
      });
      expectSuccess(harnessTimeout);
      expect(harnessTimeout.stdout).toContain("codex=timed out after 100ms");
      expect(harnessTimeout.stdout).toContain("FAIL codex-harness: timed out after 100ms; CLI compatibility probe timed out");

      await writeFile(
        join(binDir, "codex"),
        [
          "#!/usr/bin/env sh",
          "if [ \"${1:-}\" = \"--version\" ]; then printf 'codex-cli partial\\n'; exit 0; fi",
          "printf '%s\\n' '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --output-last-message'",
          "sleep 5",
          ""
        ].join("\n")
      );
      await chmod(join(binDir, "codex"), 0o755);
      const partialHelpTimeout = runBrainctl(["updates", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
        BRAINSTACK_UPDATE_PROBE_COMMANDS: "none",
        BRAINSTACK_UPDATE_PROBE_TIMEOUT_MS: "100"
      });
      expectSuccess(partialHelpTimeout);
      expect(partialHelpTimeout.stdout).toContain("codex=codex-cli partial");
      expect(partialHelpTimeout.stdout).toContain("FAIL codex-harness: codex-cli partial; CLI compatibility probe timed out");

      await writeFile(join(binDir, "git"), "#!/usr/bin/env sh\nsleep 5\n");
      await chmod(join(binDir, "git"), 0o755);
      const gitTimeout = runBrainctl(["updates", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
        BRAINSTACK_UPDATE_PROBE_COMMANDS: "none",
        BRAINSTACK_UPDATE_PROBE_TIMEOUT_MS: "100"
      });
      expectSuccess(gitTimeout);
      expect(gitTimeout.stdout).toContain("brainstack_branch=unknown");
      expect(gitTimeout.stdout).toContain("brainstack_head=unknown");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("doctor reports client write readiness and explicit write smoke", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-client-readiness-"));
    const port = 36_000 + Math.floor(Math.random() * 3_000);
    try {
      const binDir = join(dir, "bin");
      const configPath = join(dir, "worker.yaml");
      const home = join(dir, "home");
      const receivedPath = join(dir, "received.jsonl");
      const serverScript = join(dir, "server.ts");
      await mkdir(binDir, { recursive: true });
      await mkdir(join(home, ".config"), { recursive: true });
      await writeFile(
        serverScript,
        [
          `const port = ${port};`,
          `const receivedPath = ${JSON.stringify(receivedPath)};`,
          "Bun.serve({",
          "  hostname: '127.0.0.1',",
          "  port,",
          "  async fetch(req) {",
          "    if (new URL(req.url).pathname === '/health') return Response.json({ ok: true });",
          "    if (new URL(req.url).pathname !== '/api/import') return Response.json({ error: 'not found' }, { status: 404 });",
          "    if (req.headers.get('authorization') !== 'Bearer test-import-token') return Response.json({ error: 'forbidden' }, { status: 403 });",
          "    const payload = await req.json();",
          "    const existing = await Bun.file(receivedPath).exists() ? await Bun.file(receivedPath).text() : '';",
          "    await Bun.write(receivedPath, `${existing}${JSON.stringify(payload)}\\n`);",
          "    return Response.json({ ok: true, artifact_id: 'doctor-smoke-artifact' });",
          "  }",
          "});",
          "await new Promise(() => {});",
          ""
        ].join("\n")
      );
      await writeFile(
        join(binDir, "codex"),
        "#!/usr/bin/env sh\nif [ \"${1:-}\" = \"--version\" ]; then printf 'codex fake\\n'; exit 0; fi\nprintf '%s\\n' '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --output-last-message'\n"
      );
      await writeFile(join(binDir, "tailscale"), "#!/usr/bin/env sh\nexit 0\n");
      await chmod(join(binDir, "codex"), 0o755);
      await chmod(join(binDir, "tailscale"), 0o755);
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: worker",
          "harness:",
          "  name: codex",
          "  bin: codex",
          "machine:",
          "  name: brain-worker",
          "  user: operator",
          "paths:",
          `  home: ${home}`,
          `  configRoot: ${join(home, ".config", "brainstack")}`,
          `  stateRoot: ${join(home, ".local", "state", "brainstack")}`,
          "brain:",
          `  publicBaseUrl: http://127.0.0.1:${port}`,
          "client:",
          "  localPath: ~/shared-brain",
          "  envPath: ~/.config/shared-brain.env",
          ""
        ].join("\n")
      );
      await writeFile(join(home, ".config", "shared-brain.env"), `BRAIN_BASE_URL=http://127.0.0.1:${port}\nBRAIN_IMPORT_TOKEN=\n`);

      const readiness = runBrainctl(["doctor", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1"
      });
      expectSuccess(readiness);
      expect(readiness.stdout).toContain("WARN [client] brain-import-token: missing or empty");

      const server = Bun.spawn(["bun", "run", serverScript], {
        stdout: "pipe",
        stderr: "pipe"
      });
      try {
        for (let attempt = 0; attempt < 20; attempt += 1) {
          try {
            await fetch(`http://127.0.0.1:${port}/health`);
            break;
          } catch {
            await Bun.sleep(25);
          }
        }
        const smoke = runBrainctl(["doctor", "--config", configPath, "--write-smoke"], {
          PATH: `${binDir}:${process.env.PATH || ""}`,
          BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
          BRAIN_IMPORT_TOKEN: "test-import-token"
        });
        expectSuccess(smoke);
        expect(smoke.stdout).toContain("PASS [client] brain-write-smoke: import accepted artifact_id=doctor-smoke-artifact");
        const requests = (await readFile(receivedPath, "utf8")).trim().split(/\r?\n/).map((line) => JSON.parse(line) as Record<string, unknown>);
        expect(requests).toHaveLength(1);
        expect(requests[0]?.source_type).toBe("doctor-smoke");
      } finally {
        server.kill();
        await server.exited;
      }
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 15_000);

  test("doctor resolves remote worker harness through the worker user's shell PATH", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-remote-path-"));
    try {
      const controlBin = join(dir, "control-bin");
      const remoteBin = join(dir, "remote-bin");
      await mkdir(controlBin, { recursive: true });
      await mkdir(remoteBin, { recursive: true });

      const fakeCodex = join(remoteBin, "codex");
      await writeFile(
        fakeCodex,
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          "if [ \"${1:-}\" = \"--version\" ]; then echo 'codex remote fake'; exit 0; fi",
          "if [ \"${1:-}\" = \"exec\" ] && [ \"${2:-}\" = \"--help\" ]; then",
          "  echo '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --output-last-message'",
          "  exit 0",
          "fi",
          "exit 0",
          ""
        ].join("\n")
      );
      await chmod(fakeCodex, 0o755);
      for (const name of ["bun", "git", "ssh", "tailscale"]) {
        const fakeCommand = join(remoteBin, name);
        await writeFile(
          fakeCommand,
          [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "if [ \"${1:-}\" = \"-V\" ]; then echo '" + name + " remote fake'; exit 0; fi",
            "if [ \"${1:-}\" = \"--version\" ]; then echo '" + name + " remote fake'; exit 0; fi",
            "echo '" + name + " remote fake'",
            ""
          ].join("\n")
        );
        await chmod(fakeCommand, 0o755);
      }

      const fakeShell = join(dir, "worker-shell");
      await writeFile(
        fakeShell,
        [
          "#!/usr/bin/env bash",
          "if [[ \"$*\" == *__BRAINSTACK_PATH__* ]]; then",
          `  printf '__BRAINSTACK_PATH__%s\\n' '${remoteBin}:/usr/bin:/bin'`,
          "  exit 0",
          "fi",
          "exec /usr/bin/bash \"$@\"",
          ""
        ].join("\n")
      );
      await chmod(fakeShell, 0o755);

      const fakeSsh = join(controlBin, "ssh");
      await writeFile(
        fakeSsh,
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          "while (($#)); do",
          "  case \"$1\" in",
          "    -o|-p) shift 2 ;;",
          "    -*) shift ;;",
          "    *) break ;;",
          "  esac",
          "done",
          "shift", // remote target
          "export PATH=/usr/bin:/bin",
          `export SHELL='${fakeShell}'`,
          "exec \"$@\"",
          ""
        ].join("\n")
      );
      await chmod(fakeSsh, 0o755);

      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "harness:",
          "  name: codex",
          "  bin: codex",
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "telemux:",
          "  enabled: true",
          "  workers:",
          "    - name: worker1",
          "      transport: ssh",
          "      sshTarget: worker1.example",
          "      sshUser: operator",
          "      sshTrustMode: accept-new",
          "      harness: codex",
          ""
        ].join("\n")
      );

      const result = runBrainctl(["doctor", "--config", configPath, "--workers"], {
        PATH: `${controlBin}:${process.env.PATH || ""}`,
        CODEX_HOME: join(dir, "remote-codex-home"),
        BRAINSTACK_ALLOW_ACCEPT_NEW_DOCTOR: "true"
      });
      expectSuccess(result);
      expect(result.stdout).toContain("WARN [workers] worker:worker1:ssh-trust: bootstrap trust mode accept-new");
      expect(result.stdout).toContain("PASS [workers] worker:worker1");
      expect(result.stdout).toContain("harness=codex");
      expect(result.stdout).toContain("model=default");
      expect(result.stdout).toContain("effort=default");
      expect(result.stdout).toContain(`PASS [workers] worker:worker1:cmd:bun: ${join(remoteBin, "bun")}; bun remote fake`);
      expect(result.stdout).toContain(`PASS [workers] worker:worker1:cmd:tailscale: ${join(remoteBin, "tailscale")}; tailscale remote fake`);
      expect(result.stdout).toContain("PASS [workers] worker:worker1:harness-compat");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("worker doctor refuses accept-new SSH trust without explicit bootstrap override", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-worker-accept-new-refusal-"));
    try {
      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "telemux:",
          "  enabled: true",
          "  workers:",
          "    - name: worker1",
          "      transport: ssh",
          "      sshTarget: worker1.example",
          "      sshUser: operator",
          "      sshTrustMode: accept-new",
          ""
        ].join("\n")
      );

      const result = runBrainctl(["doctor", "--config", configPath, "--workers"], {
        BRAINSTACK_ALLOW_ACCEPT_NEW_DOCTOR: "0"
      });
      expect(result.code).toBe(1);
      expect(result.stdout).toContain("FAIL [workers] worker:worker1:ssh-trust: bootstrap trust mode accept-new");
      expect(result.stdout).toContain("Refusing remote doctor probes under TOFU");
      expect(result.stdout).not.toContain("PASS [workers] worker:worker1");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("doctor enforces pinned SSH trust and uses configured OpenSSH ports", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-pinned-ssh-port-"));
    try {
      const binDir = join(dir, "bin");
      await mkdir(binDir, { recursive: true });
      const argsCapture = join(dir, "ssh-args.txt");
      const fakeSsh = join(binDir, "ssh");
      await writeFile(
        fakeSsh,
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          `printf '%s\\n' "$*" > '${argsCapture}'`,
          "cat <<'EOF'",
          "worker=worker2",
          "sudo=ok",
          "cmd:bun=/usr/bin/bun",
          "cmdver:bun=bun fake",
          "cmd:git=/usr/bin/git",
          "cmdver:git=git fake",
          "cmd:ssh=/usr/bin/ssh",
          "cmdver:ssh=ssh fake",
          "cmd:tailscale=/usr/bin/tailscale",
          "cmdver:tailscale=tailscale fake",
          "harness_bin=/usr/bin/codex",
          "version=codex fake",
          "flag:--dangerously-bypass-approvals-and-sandbox=ok",
          "flag:--skip-git-repo-check=ok",
          "flag:--output-last-message=ok",
          "model=gpt-test",
          "effort=high",
          "deep=skipped",
          "EOF",
          ""
        ].join("\n")
      );
      await chmod(fakeSsh, 0o755);
      const fakeSshKeygen = join(binDir, "ssh-keygen");
      await writeFile(
        fakeSshKeygen,
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          "if [[ \"${1:-}\" == \"-F\" && \"${2:-}\" == \"[worker2.example]:2222\" ]]; then exit 0; fi",
          "exit 1",
          ""
        ].join("\n")
      );
      await chmod(fakeSshKeygen, 0o755);

      const knownHosts = join(dir, "known_hosts");
      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "harness:",
          "  name: codex",
          "  bin: codex",
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "telemux:",
          "  enabled: true",
          "  workers:",
          "    - name: worker2",
          "      transport: ssh",
          "      sshTarget: '[worker2.example]:2222'",
          "      sshUser: operator",
          "      sshTrustMode: pinned",
          `      sshKnownHostsPath: ${knownHosts}`,
          "      harness: codex",
          ""
        ].join("\n")
      );

      const missingTrust = runBrainctl(["doctor", "--config", configPath, "--workers"], {
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expect(missingTrust.code).not.toBe(0);
      expect(missingTrust.stdout).toContain(`FAIL [workers] worker:worker2:ssh-trust: pinned known_hosts file missing: ${knownHosts}`);

      await writeFile(knownHosts, "[worker2.example]:2222 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeBrainstackWorkerKeyForTestsOnly\n");
      const trusted = runBrainctl(["doctor", "--config", configPath, "--workers"], {
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expectSuccess(trusted);
      expect(trusted.stdout).toContain("PASS [workers] worker:worker2:ssh-trust: pinned host key present for [worker2.example]:2222");
      expect(trusted.stdout).toContain("PASS [workers] worker:worker2: reachable via ssh; harness=codex bin=/usr/bin/codex model=gpt-test effort=high");
      const capturedArgs = await readFile(argsCapture, "utf8");
      expect(capturedArgs).toContain("-p 2222");
      expect(capturedArgs).toContain("operator@worker2.example");
      expect(capturedArgs).not.toContain("operator@[worker2.example]:2222");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("doctor runs local harness wrappers with the user's shell PATH", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-local-harness-path-"));
    try {
      const helperBin = join(dir, "helper-bin");
      const wrapperBin = join(dir, "wrapper-bin");
      await mkdir(helperBin, { recursive: true });
      await mkdir(wrapperBin, { recursive: true });

      const fakeNpx = join(helperBin, "npx");
      await writeFile(
        fakeNpx,
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          "if [ \"${1:-}\" = \"--version\" ]; then echo 'codex-cli 9.9.9'; exit 0; fi",
          "if [ \"${1:-}\" = \"exec\" ] && [ \"${2:-}\" = \"--help\" ]; then",
          "  echo '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --output-last-message'",
          "  exit 0",
          "fi",
          "exit 0",
          ""
        ].join("\n")
      );
      await chmod(fakeNpx, 0o755);
      await writeFile(join(helperBin, "tailscale"), "#!/usr/bin/env bash\nif [ \"${1:-}\" = \"version\" ]; then echo 'tailscale fake'; exit 0; fi\nexit 0\n");
      await chmod(join(helperBin, "tailscale"), 0o755);

      const codexWrapper = join(wrapperBin, "codex");
      await writeFile(codexWrapper, "#!/usr/bin/env bash\nexec npx \"$@\"\n");
      await chmod(codexWrapper, 0o755);

      const fakeShell = join(dir, "worker-shell");
      await writeFile(
        fakeShell,
        [
          "#!/usr/bin/env bash",
          "if [[ \"$*\" == *__BRAINSTACK_PATH__* ]]; then",
          `  printf '__BRAINSTACK_PATH__%s\\n' '${helperBin}:/usr/bin:/bin'`,
          "  exit 0",
          "fi",
          "exec /usr/bin/bash \"$@\"",
          ""
        ].join("\n")
      );
      await chmod(fakeShell, 0o755);

      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: worker",
          "harness:",
          "  name: codex",
          `  bin: ${codexWrapper}`,
          "machine:",
          "  name: worker",
          "  user: operator",
          ""
        ].join("\n")
      );

      const result = runBrainctl(["doctor", "--config", configPath], {
        SHELL: fakeShell,
        PATH: `${dirname(process.execPath)}:/usr/bin:/bin`
      });
      expectSuccess(result);
      expect(result.stdout).toContain("PASS [versions] codex-harness");
      expect(result.stdout).toContain("codex-cli 9.9.9");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("doctor fails when worker harness CLI surface is incompatible", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-doctor-worker-"));
    try {
      const fakeCodex = join(dir, "codex");
      await writeFile(
        fakeCodex,
        [
          "#!/usr/bin/env sh",
          "if [ \"$1\" = \"--version\" ]; then echo 'codex fake'; exit 0; fi",
          "if [ \"$1\" = \"exec\" ] && [ \"$2\" = \"--help\" ]; then echo 'Usage: codex exec'; exit 0; fi",
          "exit 0",
          ""
        ].join("\n")
      );
      await chmod(fakeCodex, 0o755);
      for (const name of ["bun", "git", "ssh", "tailscale"]) {
        const fakeCommand = join(dir, name);
        await writeFile(
          fakeCommand,
          [
            "#!/usr/bin/env sh",
            "if [ \"$1\" = \"-V\" ]; then echo '" + name + " fake'; exit 0; fi",
            "if [ \"$1\" = \"--version\" ]; then echo '" + name + " fake'; exit 0; fi",
            "echo '" + name + " fake'",
            ""
          ].join("\n")
        );
        await chmod(fakeCommand, 0o755);
      }
      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "harness:",
          "  name: codex",
          `  bin: ${fakeCodex}`,
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "telemux:",
          "  enabled: true",
          "  workers:",
          "    - name: brain-control",
          "      transport: local",
          `      harnessBin: ${fakeCodex}`,
          ""
        ].join("\n")
      );
      const result = runBrainctl(["doctor", "--config", configPath, "--workers"], {
        BRAINSTACK_WORKER_PATH: dir
      });
      expect(result.code).not.toBe(0);
      expect(`${result.stdout}\n${result.stderr}`).toContain("missing required CLI surface");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("client env examples do not expose admin token slots", async () => {
    const staticEnv = await readFile(join(PRODUCT_ROOT, "packages", "client-bootstrap", "client.env.example"), "utf8");
    expect(staticEnv).not.toContain("BRAIN_ADMIN_TOKEN");

    const dir = await mkdtemp(join(tmpdir(), "brainctl-bootstrap-"));
    try {
      const configPath = join(dir, "config.yaml");
      const out = join(dir, "bootstrap");
      await writeFixtureConfig(configPath);
      expectSuccess(runBrainctl(["bootstrap-client", "--profile", "client-macos", "--config", configPath, "--out", out]));
      const generatedEnv = await readFile(join(out, "client.env.example"), "utf8");
      expect(generatedEnv).not.toContain("BRAIN_ADMIN_TOKEN");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("release build uses deterministic Bun compile flags", async () => {
    const packageJson = await readFile(join(PRODUCT_ROOT, "package.json"), "utf8");
    const releaseScript = await readFile(join(PRODUCT_ROOT, "scripts", "release.sh"), "utf8");
    for (const text of [packageJson, releaseScript]) {
      expect(text).toContain("--no-compile-autoload-dotenv");
      expect(text).toContain("--no-compile-autoload-bunfig");
    }
  });

  test("nested telemux bun lockfile is gone", async () => {
    expect(await Bun.file(join(PRODUCT_ROOT, "apps", "telemux", "bun.lock")).exists()).toBe(false);
    expect(await Bun.file(join(PRODUCT_ROOT, "bun.lock")).exists()).toBe(true);
  });
});
