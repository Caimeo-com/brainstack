import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
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
});

describe("brainctl install safety", () => {
  test("init is fresh-install only and upgrade does not rewrite canonical content", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-init-safety-"));
    try {
      const configPath = join(dir, "config.yaml");
      const root = join(dir, "install");
      await writeFixtureConfig(configPath);

      expectSuccess(runBrainctl(["init", "--profile", "single-node", "--config", configPath, "--root", root]));
      const staging = join(root, "shared-brain", "staging", "shared-brain");
      const headBefore = git(["rev-parse", "HEAD"], staging);
      const homeBefore = await readFile(join(staging, "wiki", "Home.md"), "utf8");

      const rerun = runBrainctl(["init", "--profile", "single-node", "--config", configPath, "--root", root]);
      expect(rerun.code).not.toBe(0);
      expect(`${rerun.stdout}\n${rerun.stderr}`).toContain("fresh-install only");
      expect(git(["rev-parse", "HEAD"], staging)).toBe(headBefore);
      expect(await readFile(join(staging, "wiki", "Home.md"), "utf8")).toBe(homeBefore);

      expectSuccess(runBrainctl(["upgrade", "--profile", "single-node", "--config", configPath, "--root", root]));
      expect(git(["rev-parse", "HEAD"], staging)).toBe(headBefore);
      expect(await readFile(join(staging, "wiki", "Home.md"), "utf8")).toBe(homeBefore);
      expect(git(["status", "--porcelain"], staging)).toBe("");
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
      expect(await readFile(join(out, "install-client.sh"), "utf8")).toContain(
        "operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git"
      );
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
});
