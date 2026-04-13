import { describe, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { chmod, mkdir, mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { createServer, type Server } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { syncWritableRepo } from "../../../apps/braind/src/brain-lib";
import { loadConfig, parseSimpleYaml } from "../src/main";

const PRODUCT_ROOT = resolve(import.meta.dir, "..", "..", "..");
const BRAINCTL = join(PRODUCT_ROOT, "packages", "brainctl", "src", "main.ts");
const SERVER = join(PRODUCT_ROOT, "apps", "braind", "src", "server.ts");
const FORBIDDEN_PUBLIC_IDENTIFIERS = [
  ["val", "kyrie"].join(""),
  ["er", "bine"].join(""),
  ["swa", "der"].join(""),
  ["tail", "b647b6"].join(""),
  ["/home/", "swa", "der"].join(""),
  ["/Users/", "swa", "der"].join(""),
  ["Bru", "no"].join("")
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
      expect(rendered).toContain(`bin: ${fakeCodex}`);
      expect(rendered).toContain("enabled: true");
      expect(rendered).toContain('publicBaseUrl: "https://brain-control.example.ts.net"');
      expect(result.stdout).toContain("selected harness: codex");
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
      expect(existsSync(configRoot)).toBe(false);
      expect(existsSync(stateRoot)).toBe(false);
      expect(existsSync(join(systemdRoot, "braind.service"))).toBe(false);
      expect(existsSync(sharedBrainRoot)).toBe(true);
      expect(existsSync(privateBrainRoot)).toBe(true);
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

      expectSuccess(runBrainctl(["init", "--profile", "worker", "--config", configPath]));
      expect(await Bun.file(join(home, "shared-brain", "AGENTS.shared-client.md")).exists()).toBe(true);
      expect(await Bun.file(join(configRoot, "client-bootstrap", "codex-shared-brain.include.md")).exists()).toBe(true);
      expect(await Bun.file(join(home, ".codex", "AGENTS.md")).exists()).toBe(true);
      expect(await readFile(join(home, ".config", "shared-brain.env"), "utf8")).toContain("BRAIN_IMPORT_TOKEN=");
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
  });
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
      expect(html).not.toContain("Valkyrie.md");
      expect(html).not.toContain("Local-Codex.md");
      expect(html).not.toContain("Shared-Brain-v1.md");

      const serveRoot = join(root, "shared-brain", "serve", "shared-brain");
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
});

describe("public release hygiene", () => {
  test("public examples and bootstrap snippets contain no customer-zero identifiers", async () => {
    const roots = [
      "examples",
      "packages/client-bootstrap",
      "infra/tailscale",
      "docs/quickstart-single-node.md",
      "docs/quickstart-control-worker.md",
      "docs/quickstart-client-macos.md",
      "README.md"
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
      const text = await readFile(file, "utf8");
      for (const identifier of FORBIDDEN_PUBLIC_IDENTIFIERS) {
        if (text.includes(identifier)) {
          offenders.push(`${file}: ${identifier}`);
        }
      }
    }
    expect(offenders).toEqual([]);
  });

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
        for (const identifier of FORBIDDEN_PUBLIC_IDENTIFIERS) {
          if (text.includes(identifier)) {
            offenders.push(`${file}: ${identifier}`);
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
      expect(codex).toContain("Consult `~/shared-brain`");
      expect(codex).toContain("Do not directly edit canonical wiki pages");

      const cursor = await readFile(join(out, "cursor-user-rule.md"), "utf8");
      expect(cursor).toContain("Before planning unfamiliar work");
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
      expect(await readFile(join(out, "codex-shared-brain.include.md"), "utf8")).toContain(`Consult \`${customLocalPath}\``);
      expect(await readFile(join(out, "codex-global-AGENTS.md"), "utf8")).toContain(`git -C ${customLocalPath} pull --ff-only`);
      expect(await readFile(join(out, "claude-user-CLAUDE.md"), "utf8")).toContain(`@${customLocalPath}/AGENTS.shared-client.md`);
      expect(await readFile(join(out, "claude-hooks-example.json"), "utf8")).toContain(`git -C ${customLocalPath} pull --ff-only`);
      expect(await readFile(join(out, "cursor-user-rule.md"), "utf8")).toContain(`consult \`${customLocalPath}\``);
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
      expect(rendered).toContain("workers:");
      expect(rendered).toContain("name: brain-worker");
      expect(rendered).toContain("sshUser: operator");
      expect(rendered).toContain("repos:");
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
      const statusBefore = runBrainctl(["outbox", "status", "--config", configPath], env);
      expectSuccess(statusBefore);
      expect(statusBefore.stdout).toContain("queued=1");

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
          "    const existing = await Bun.file(receivedPath).exists() ? await Bun.file(receivedPath).text() : '';",
          "    await Bun.write(receivedPath, `${existing}${JSON.stringify(payload)}\\n`);",
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
  });

  test("updates command is read-only and reports versions", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-updates-"));
    try {
      const configPath = join(dir, "config.yaml");
      await writeFixtureConfig(configPath);
      const result = runBrainctl(["updates", "--config", configPath]);
      expectSuccess(result);
      expect(result.stdout).toContain("brainstack_head=");
      expect(result.stdout).toContain("manual_update_commands:");
      expect(result.stdout).toContain("selected harness:");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

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
          "    -o) shift 2 ;;",
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
          "      harness: codex",
          ""
        ].join("\n")
      );

      const result = runBrainctl(["doctor", "--config", configPath, "--workers"], {
        PATH: `${controlBin}:${process.env.PATH || ""}`
      });
      expectSuccess(result);
      expect(result.stdout).toContain("PASS [workers] worker:worker1");
      expect(result.stdout).toContain("PASS [workers] worker:worker1:harness-compat");
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
      const result = runBrainctl(["doctor", "--config", configPath, "--workers"]);
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
