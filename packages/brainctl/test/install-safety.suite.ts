import {
  Buffer,
  BRAINCTL,
  FORBIDDEN_PUBLIC_PATTERNS,
  PRODUCT_ROOT,
  SERVER,
  basename,
  braindTestEnv,
  canonicalJson,
  chmod,
  createHash,
  createServer,
  createSingleNodeInstall,
  describe,
  dirname,
  existsSync,
  expect,
  expectSuccess,
  git,
  gzipSync,
  gunzipSync,
  hostname,
  join,
  listFiles,
  loadConfig,
  mkdir,
  mkdtemp,
  parseSimpleYaml,
  readFile,
  readQueuedPayloadFromOutput,
  readdir,
  realpath,
  repoPathUrl,
  resolve,
  rm,
  runBrainctl,
  runCommand,
  sha256Text,
  shellQuote,
  startBraind,
  stat,
  stopBraind,
  symlink,
  syncWritableRepo,
  syncWritableRepoAsync,
  test,
  tmpdir,
  utimes,
  waitForBraind,
  waitForCondition,
  withRepoLock,
  writeExecutable,
  writeFakeDoctorCodex,
  writeFakeTailscale,
  writeFile,
  writeFixtureClientConfig,
  writeFixtureConfig
} from "./helpers";
import type { Server } from "./helpers";

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
      await writeFakeTailscale(binDir);

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
      expect(result.stdout).toContain(`selected harness: codex (config bin: ${fakeCodex})`);
      expect(result.stdout).toContain("brainctl init --profile control");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("telemux runtime pins stable harness binary instead of earlier package-manager wrapper", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-stable-harness-"));
    try {
      const badBin = join(dir, ".local", "bin");
      const stableBin = join(dir, ".bun", "bin");
      const outDir = join(dir, "out");
      await mkdir(badBin, { recursive: true });
      await mkdir(stableBin, { recursive: true });
      await writeExecutable(join(badBin, "codex"), "#!/usr/bin/env bash\nexec npx --yes --package @openai/codex -- codex \"$@\"\n");
      await writeExecutable(
        join(stableBin, "codex"),
        [
          "#!/usr/bin/env sh",
          "if [ \"${1:-}\" = \"--version\" ]; then echo 'codex stable'; exit 0; fi",
          "if [ \"${1:-}\" = \"exec\" ] && [ \"${2:-}\" = \"--help\" ]; then",
          "  echo '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --output-last-message'",
          "  exit 0",
          "fi",
          "exit 0",
          ""
        ].join("\n")
      );
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
          "paths:",
          `  home: ${dir}`,
          "brain:",
          "  publicBaseUrl: https://brain-control.example.ts.net",
          "telemux:",
          "  enabled: true",
          "  localMachine: brain-control",
          ""
        ].join("\n")
      );

      const render = runBrainctl(["render", "--profile", "control", "--config", configPath, "--out", outDir]);
      expectSuccess(render);
      const runtimeEnv = await readFile(join(outDir, "env", "telemux.runtime.env"), "utf8");
      expect(runtimeEnv).toContain(`FACTORY_HARNESS_BIN=${join(stableBin, "codex")}`);
      expect(runtimeEnv).toContain(`FACTORY_CODEX_BIN=${join(stableBin, "codex")}`);
      expect(runtimeEnv).not.toContain(`FACTORY_HARNESS_BIN=${join(badBin, "codex")}`);
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
      await writeFakeTailscale(binDir);
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
      await writeFakeTailscale(binDir);

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

  test("compiled client binary provisions, doctors, and bootstraps without Bun on PATH", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-compiled-client-"));
    try {
      const binary = join(dir, "brainctl");
      expectSuccess(
        runCommand([
          "bun",
          "build",
          BRAINCTL,
          "--compile",
          "--no-compile-autoload-dotenv",
          "--no-compile-autoload-bunfig",
          "--outfile",
          binary
        ])
      );

      const binDir = join(dir, "bin");
      await mkdir(binDir, { recursive: true });
      await writeFile(
        join(binDir, "codex"),
        [
          "#!/usr/bin/env sh",
          "if [ \"${1:-}\" = \"--version\" ]; then echo 'codex fake'; exit 0; fi",
          "if [ \"${1:-}\" = \"exec\" ] && [ \"${2:-}\" = \"--help\" ]; then",
          "  echo '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --output-last-message'",
          "  exit 0",
          "fi",
          "exit 0",
          ""
        ].join("\n")
      );
      await chmod(join(binDir, "codex"), 0o755);
      const realGit = runCommand(["bash", "-lc", "command -v git"]).stdout.trim().split(/\r?\n/)[0] || "/usr/bin/git";
      for (const name of ["git", "ssh", "tailscale"]) {
        const fakeCommand = join(binDir, name);
        if (name === "git") {
          await writeFile(
            fakeCommand,
            [
              "#!/usr/bin/env sh",
              "if [ \"${1:-}\" = \"--version\" ]; then echo 'git fake'; exit 0; fi",
              `exec ${realGit} "$@"`,
              ""
            ].join("\n")
          );
        } else {
          await writeFile(
            fakeCommand,
            [
              "#!/usr/bin/env sh",
              "if [ \"${1:-}\" = \"-V\" ]; then echo '" + name + " fake'; exit 0; fi",
              "if [ \"${1:-}\" = \"--version\" ]; then echo '" + name + " fake'; exit 0; fi",
              "if [ \"${1:-}\" = \"status\" ] && [ \"${2:-}\" = \"--json\" ]; then echo '{}'; exit 0; fi",
              "echo '" + name + " fake'",
              ""
            ].join("\n")
          );
        }
        await chmod(fakeCommand, 0o755);
      }

      const noBunEnv = {
        PATH: `${binDir}:/usr/bin:/bin:/usr/sbin:/sbin`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
        BRAINSTACK_HARNESS_TEST_TIMEOUT_MS: "1000",
        HOME: join(dir, "home"),
        USER: "operator"
      };
      const bare = join(dir, "shared-brain.git");
      const seed = join(dir, "seed");
      git(["init", "--bare", "--initial-branch=main", bare], dir);
      git(["clone", bare, seed], dir);
      await writeFile(join(seed, "AGENTS.shared-client.md"), "# Shared Client\n");
      git(["add", "AGENTS.shared-client.md"], seed);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "seed"], seed);
      git(["push", "-u", "origin", "main"], seed);

      const tokenFile = join(dir, "brain-import-token.txt");
      await writeFile(tokenFile, "compiled-client-token\n");
      await chmod(tokenFile, 0o600);
      const configPath = join(dir, "brainstack.yaml");
      const provision = runCommand(
        [
          binary,
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
          bare
        ],
        { env: noBunEnv }
      );
      expectSuccess(provision);
      expect(provision.stdout).toContain("detected tools: git=present ssh=present tailscale=present");
      expect(provision.stdout).not.toContain("bun=present");

      const doctor = runCommand([binary, "doctor", "--config", configPath], { env: noBunEnv });
      expectSuccess(doctor);
      expect(doctor.stdout).toContain("WARN [versions] bun:");
      expect(doctor.stdout).toContain("not required for client-macos standalone binary installs");
      expect(doctor.stdout).not.toContain("FAIL [versions] bun:");

      const init = runCommand([binary, "init", "--profile", "client-macos", "--config", configPath, "--import-token-file", tokenFile], {
        env: noBunEnv
      });
      expectSuccess(init);
      expect(await Bun.file(join(noBunEnv.HOME, "shared-brain", "AGENTS.shared-client.md")).exists()).toBe(true);
      expect(await readFile(join(noBunEnv.HOME, ".config", "shared-brain.env"), "utf8")).toContain("BRAIN_IMPORT_TOKEN=compiled-client-token");

      const missingTokenFileValue = runCommand([binary, "init", "--profile", "client-macos", "--config", configPath, "--import-token-file"], {
        env: noBunEnv
      });
      expect(missingTokenFileValue.code).not.toBe(0);
      expect(missingTokenFileValue.stderr).toContain("--import-token-file requires a value");

      const out = join(dir, "bootstrap");
      const bootstrap = runCommand([binary, "bootstrap-client", "--profile", "client-macos", "--config", configPath, "--out", out], {
        cwd: dir,
        env: noBunEnv
      });
      expectSuccess(bootstrap);
      expect(await Bun.file(join(out, "install-client.sh")).exists()).toBe(true);
      expect(await readFile(join(out, "client.env.example"), "utf8")).toContain("BRAIN_BASE_URL=https://brain-control.example.ts.net");
      expect(await readFile(join(out, "install-client.sh"), "utf8")).toContain("git clone -- \"$REMOTE\" \"$TARGET_ABS\"");

      const skillRoot = join(dir, "codex-skills");
      const skillInstall = runCommand([binary, "skills", "install", "--profile", "client", "--dir", skillRoot], {
        cwd: dir,
        env: noBunEnv
      });
      expectSuccess(skillInstall);
      expect(skillInstall.stdout).toContain("skills installed: target=codex");
      expect(await readFile(join(skillRoot, "brainstack", "SKILL.md"), "utf8")).toContain("brainctl telegram send-file");
      expect(await readFile(join(skillRoot, "shared-brain-client", "SKILL.md"), "utf8")).toContain("Never request or store the admin ingest token");

      const skillDryRun = runCommand([binary, "skills", "install", "--profile", "operator", "--dir", join(dir, "dry-skills"), "--dry-run"], {
        cwd: dir,
        env: noBunEnv
      });
      expectSuccess(skillDryRun);
      expect(skillDryRun.stdout).toContain("dry-run skills install plan");
      expect(await Bun.file(join(dir, "dry-skills", "brainstack", "SKILL.md")).exists()).toBe(false);

      const missingSkillValue = runCommand([binary, "skills", "install", "--skill"], {
        cwd: dir,
        env: noBunEnv
      });
      expect(missingSkillValue.code).not.toBe(0);
      expect(missingSkillValue.stderr).toContain("--skill requires a value");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 30_000);

  test("doctor accepts configured Bun binary when Bun is absent from PATH", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-doctor-bun-bin-"));
    try {
      const binDir = join(dir, "bin");
      const home = join(dir, "home");
      const configRoot = join(home, ".config", "brainstack");
      const stateRoot = join(home, ".local", "state", "brainstack");
      await mkdir(binDir, { recursive: true });
      await mkdir(configRoot, { recursive: true });
      await mkdir(stateRoot, { recursive: true });

      const fakeBun = join(dir, "configured-bun");
      await writeExecutable(fakeBun, "#!/usr/bin/env sh\nprintf '1.3.14\\n'\n");
      await writeExecutable(join(binDir, "git"), "#!/usr/bin/env sh\nprintf 'git version 2.54.0\\n'\n");
      await writeExecutable(join(binDir, "ssh"), "#!/usr/bin/env sh\nprintf 'OpenSSH fake\\n'\n");
      await writeFakeTailscale(binDir);
      await writeFakeDoctorCodex(binDir);

      const configPath = join(configRoot, "brainstack.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: worker",
          "runtime:",
          `  bunBin: ${fakeBun}`,
          "machine:",
          "  name: worker-a",
          "  user: operator",
          "paths:",
          `  home: ${home}`,
          `  configRoot: ${configRoot}`,
          `  stateRoot: ${stateRoot}`,
          "brain:",
          "  publicBaseUrl: https://brain-control.example.ts.net",
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          "harness:",
          "  name: codex",
          "  bin: codex",
          ""
        ].join("\n")
      );

      const result = runCommand([process.execPath, "run", BRAINCTL, "doctor", "--config", configPath], {
        env: {
          HOME: home,
          USER: "operator",
          PATH: `${binDir}:/usr/bin:/bin`,
          BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1"
        }
      });
      expectSuccess(result);
      expect(result.stdout).toContain("PASS [versions] bun: Bun 1.3.14");
      expect(result.stdout).toContain(`PASS [versions] bun-bin: ${fakeBun}`);
      expect(result.stdout).not.toContain("FAIL [versions] bun:");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("import skill packages local files, folders, and URLs through the outbox", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-skill-import-"));
    const port = 47_000 + Math.floor(Math.random() * 1_000);
    const urlSkill = [
      "---",
      "name: url-skill",
      "description: URL installed skill",
      "---",
      "",
      "Use this URL skill."
    ].join("\n");
    let sourceServer: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const configPath = join(dir, "client.yaml");
      const serverScript = join(dir, "source-server.ts");
      await writeFile(
        serverScript,
        [
          `const port = ${port};`,
          `const body = ${JSON.stringify(urlSkill)};`,
          "Bun.serve({",
          "  hostname: '127.0.0.1',",
          "  port,",
          "  fetch(req) {",
          "    if (new URL(req.url).pathname === '/health') return Response.json({ ok: true });",
          "    return new Response(body, { headers: { 'content-type': 'text/markdown' } });",
          "  }",
          "});",
          "await new Promise(() => {});",
          ""
        ].join("\n")
      );
      sourceServer = Bun.spawn(["bun", "run", serverScript], { stdout: "pipe", stderr: "pipe" });
      for (let attempt = 0; attempt < 30; attempt += 1) {
        try {
          const health = await fetch(`http://127.0.0.1:${port}/health`);
          if (health.ok) {
            break;
          }
        } catch {
          await Bun.sleep(25);
        }
      }
      await writeFixtureClientConfig(configPath, { home, stateRoot });
      const skillDir = join(dir, "brainstack-local");
      await mkdir(join(skillDir, "references"), { recursive: true });
      await writeFile(
        join(skillDir, "SKILL.md"),
        [
          "---",
          "name: local-skill",
          "description: Local folder skill",
          "---",
          "",
          "Use this local skill."
        ].join("\n")
      );
      await writeFile(join(skillDir, "references", "guide.md"), "reference body\n");

      const localImport = runBrainctl(["import", "skill", join(skillDir, "SKILL.md"), "--config", configPath], { HOME: home });
      expectSuccess(localImport);
      const localPayload = await readQueuedPayloadFromOutput(`${localImport.stdout}\n${localImport.stderr}`);
      expect(localPayload.source_type).toBe("skill");
      const localPackage = JSON.parse(String(localPayload.text)) as Record<string, unknown>;
      expect(localPackage.kind).toBe("brainstack.skill_package");
      expect(localPackage.name).toBe("local-skill");
      expect((localPackage.files as Array<Record<string, unknown>>).map((file) => file.path)).toEqual(["SKILL.md", "references/guide.md"]);
      expect((localPackage.source as Record<string, unknown>).kind).toBe("local");

      const privateUrlBlocked = runBrainctl(["import", "skill", `http://127.0.0.1:${port}/SKILL.md`, "--config", configPath], { HOME: home });
      expect(privateUrlBlocked.code).not.toBe(0);
      expect(privateUrlBlocked.stderr).toContain("blocked private address");

      const urlImport = runBrainctl(["import", "skill", `http://127.0.0.1:${port}/SKILL.md`, "--config", configPath, "--allow-private-url"], { HOME: home });
      expectSuccess(urlImport);
      const urlPayload = await readQueuedPayloadFromOutput(`${urlImport.stdout}\n${urlImport.stderr}`);
      const urlPackage = JSON.parse(String(urlPayload.text)) as Record<string, unknown>;
      expect(urlPackage.name).toBe("url-skill");
      expect((urlPackage.files as Array<Record<string, unknown>>).map((file) => file.path)).toEqual(["SKILL.md"]);
      expect((urlPackage.source as Record<string, unknown>).kind).toBe("url");
    } finally {
      sourceServer?.kill();
      await sourceServer?.exited;
      await rm(dir, { recursive: true, force: true });
    }
  }, 10_000);

  test("import codex-session finds local session logs and queues bounded evidence", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-import-codex-session-"));
    try {
      const home = join(dir, "home");
      const codexHome = join(home, ".codex");
      const stateRoot = join(dir, "state");
      const configPath = join(dir, "client.yaml");
      const sessionId = "019ebbfc-3a60-7f61-a4fa-a89282b8d83f";
      const sessionDir = join(codexHome, "sessions", "2026", "06", "12");
      const archivedSessionDir = join(codexHome, "archived_sessions", "2026", "06", "12");
      const sessionPath = join(sessionDir, `rollout-2026-06-12T15-18-49-${sessionId}.jsonl`);
      const archivedSessionPath = join(archivedSessionDir, `rollout-2026-06-11T00-00-00-${sessionId}.jsonl`);
      await writeFixtureClientConfig(configPath, { home, stateRoot });
      await mkdir(sessionDir, { recursive: true });
      await mkdir(archivedSessionDir, { recursive: true });
      await writeFile(
        join(codexHome, "session_index.jsonl"),
        `${JSON.stringify({ id: sessionId, thread_name: "Investigate Slack EA tag miss", updated_at: "2026-06-12T13:19:18.841494Z" })}\n`
      );
      await writeFile(
        archivedSessionPath,
        `${JSON.stringify({
          timestamp: "2026-06-11T00:00:00.000Z",
          type: "event_msg",
          payload: { type: "task_complete", last_agent_message: "stale archived duplicate should not be imported" }
        })}\n`
      );
      await utimes(archivedSessionPath, new Date("2026-06-11T00:00:00.000Z"), new Date("2026-06-11T00:00:00.000Z"));
      const transcript = [
        {
          timestamp: "2026-06-12T13:18:51.563Z",
          type: "session_meta",
          payload: {
            id: sessionId,
            timestamp: "2026-06-12T13:18:49.981Z",
            cwd: "/repo/lindy-debug",
            originator: "Codex Desktop",
            cli_version: "0.140.0-alpha.2",
            source: "vscode",
            model_provider: "openai"
          }
        },
        {
          timestamp: "2026-06-12T13:20:00.000Z",
          type: "event_msg",
          payload: {
            type: "task_complete",
            last_agent_message: "Redis Cluster Lua scripts touching multiple keys need a shared hash tag."
          }
        }
      ]
        .map((entry) => JSON.stringify(entry))
        .join("\n");
      await writeFile(sessionPath, `${transcript}\n`);

      const dryRun = runBrainctl(["import", "codex-session", sessionId, "--config", configPath, "--include-transcript", "--dry-run", "--json"], {
        HOME: home,
        CODEX_HOME: codexHome
      });
      expectSuccess(dryRun);
      const dryRunBody = JSON.parse(dryRun.stdout) as Record<string, any>;
      expect(dryRunBody.payload.title).toBe("Codex session: Investigate Slack EA tag miss");
      expect(dryRunBody.payload.source_type).toBe("codex-session-transcript");
      expect(dryRunBody.payload.transcript_path).toBe(sessionPath);
      expect(dryRunBody.payload.transcript_bytes).toBeGreaterThan(0);
      expect(String(dryRunBody.payload.transcript_sha256)).toHaveLength(64);
      expect(dryRunBody.payload.started_at).toBe("2026-06-12T13:18:49.981Z");
      expect(String(dryRunBody.payload.text)).toContain("Redis Cluster Lua scripts");
      expect(String(dryRunBody.payload.text)).not.toContain("stale archived duplicate");
      expect(String(dryRunBody.payload.text)).toContain("```jsonl");

      const queued = runBrainctl(["import", "codex-session", sessionId, "--config", configPath], {
        HOME: home,
        CODEX_HOME: codexHome
      });
      expectSuccess(queued);
      const payload = await readQueuedPayloadFromOutput(`${queued.stdout}\n${queued.stderr}`);
      expect(payload.source_type).toBe("codex-session-checkpoint");
      expect(payload.conversation_id).toBe(sessionId);
      expect(payload.transcript_path).toBe(sessionPath);
      expect(payload.transcript_bytes).toBeGreaterThan(0);
      expect(String(payload.transcript_sha256)).toHaveLength(64);
      expect(String(payload.text)).toContain("Re-import with `brainctl import codex-session");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("import skills deterministically plans and applies shared skill imports", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-import-skills-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const repo = join(dir, "shared-brain");
      const configPath = join(dir, "client.yaml");
      const cwd = join(dir, "workspace");
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: repo });

      async function writeSkill(root: string, name: string, description: string, body = "Use this skill.\n"): Promise<string> {
        await mkdir(root, { recursive: true });
        const text = ["---", `name: ${name}`, `description: ${description}`, "---", "", body].join("\n");
        await writeFile(join(root, "SKILL.md"), text);
        return text;
      }

      const cwdSkill = await writeSkill(join(cwd, "packages", "skills", "shared-skill"), "shared-skill", "Workspace copy");
      await writeFile(join(cwd, "packages", "skills", "shared-skill", "guide.md"), "workspace reference\n");
      await writeSkill(join(home, ".codex", "skills", "shared-skill"), "shared-skill", "Lower-priority duplicate");
      await writeSkill(join(home, ".codex", "skills", "codex-only"), "codex-only", "Codex skill");
      await writeSkill(join(home, ".claude", "skills", "claude-only"), "claude-only", "Claude skill");
      await writeSkill(join(home, ".cursor", "skills", "cursor-only"), "cursor-only", "Cursor skill");
      const alreadySkill = await writeSkill(join(home, ".codex", "skills", "already-skill"), "already-skill", "Already imported");

      await mkdir(join(repo, "manifests", "sources"), { recursive: true });
      await mkdir(join(repo, "raw", "imported"), { recursive: true });
      const alreadyPackage = {
        schema_version: 1,
        kind: "brainstack.skill_package",
        name: "already-skill",
        description: "Already imported",
        imported_at: "2026-06-10T12:00:00.000Z",
        source: { kind: "test" },
        files: [
          {
            path: "SKILL.md",
            encoding: "utf8",
            content: alreadySkill,
            size_bytes: new TextEncoder().encode(alreadySkill).byteLength,
            sha256: sha256Text(alreadySkill)
          }
        ]
      };
      await writeFile(join(repo, "raw", "imported", "already-skill.json"), `${JSON.stringify(alreadyPackage, null, 2)}\n`);
      await writeFile(
        join(repo, "manifests", "sources", "already-skill.json"),
        `${JSON.stringify(
          {
            id: "already-skill",
            title: "Skill import: already-skill",
            created_at: "2026-06-10T12:00:00.000Z",
            source_type: "skill",
            raw_path: "raw/imported/already-skill.json",
            tags: ["brainstack-skill"]
          },
          null,
          2
        )}\n`
      );

      const env = { HOME: home, CODEX_HOME: join(home, ".codex") };
      const plan = runCommand(["bun", "run", BRAINCTL, "import", "skills", "--config", configPath, "--json"], { cwd, env });
      expectSuccess(plan);
      expect(plan.stderr).not.toContain("shared-brain write queued");
      const planBody = JSON.parse(plan.stdout) as Record<string, unknown>;
      expect(planBody.note).toContain("global shared-brain imports");
      const proposedNames = (planBody.proposed as Array<Record<string, unknown>>).map((item) => item.name);
      expect(proposedNames).toEqual(["claude-only", "codex-only", "cursor-only", "shared-skill"]);
      const sharedProposal = (planBody.proposed as Array<Record<string, unknown>>).find((item) => item.name === "shared-skill") as Record<string, unknown>;
      expect(sharedProposal.root).toBe(await realpath(resolve(cwd, "packages", "skills", "shared-skill")));
      expect(sharedProposal.action).toBe("install");
      const skipped = planBody.skipped as Array<Record<string, unknown>>;
      expect(skipped.some((item) => item.name === "already-skill" && String(item.reason).includes("already in shared brain"))).toBe(true);
      expect(skipped.some((item) => item.name === "shared-skill" && String(item.reason).includes("duplicate skill name"))).toBe(true);

      const apply = runCommand(["bun", "run", BRAINCTL, "import", "skills", "--config", configPath, "--skill", "shared-skill", "--apply", "--json"], { cwd, env });
      expectSuccess(apply);
      const applyBody = JSON.parse(apply.stdout) as Record<string, unknown>;
      expect(applyBody.applied).toEqual(["shared-skill"]);
      const payload = await readQueuedPayloadFromOutput(`${apply.stdout}\n${apply.stderr}`);
      expect(payload.source_type).toBe("skill");
      const importedPackage = JSON.parse(String(payload.text)) as Record<string, unknown>;
      expect(importedPackage.name).toBe("shared-skill");
      expect((importedPackage.files as Array<Record<string, unknown>>).map((file) => file.path)).toEqual(["SKILL.md", "guide.md"]);
      expect(String((importedPackage.files as Array<Record<string, unknown>>)[0].content)).toBe(cwdSkill);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("skills refresh installs verified shared-brain skill packages without clobbering local skills", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-skills-refresh-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const repo = join(dir, "shared-brain");
      const installRoot = join(dir, "codex-skills");
      const configPath = join(dir, "client.yaml");
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: repo });
      await mkdir(join(repo, "manifests", "sources"), { recursive: true });
      await mkdir(join(repo, "raw", "imported"), { recursive: true });
      const skillText = [
        "---",
        "name: imported-skill",
        "description: Imported shared skill",
        "---",
        "",
        "Use this imported skill."
      ].join("\n");
      const referenceText = "shared reference\n";
      const packageBody = {
        schema_version: 1,
        kind: "brainstack.skill_package",
        name: "imported-skill",
        description: "Imported shared skill",
        imported_at: "2026-06-10T12:00:00.000Z",
        source: { kind: "test" },
        files: [
          {
            path: "SKILL.md",
            encoding: "utf8",
            content: skillText,
            size_bytes: new TextEncoder().encode(skillText).byteLength,
            sha256: sha256Text(skillText)
          },
          {
            path: "references/note.md",
            encoding: "utf8",
            content: referenceText,
            size_bytes: new TextEncoder().encode(referenceText).byteLength,
            sha256: sha256Text(referenceText)
          }
        ]
      };
      await writeFile(join(repo, "raw", "imported", "skill.md"), `${JSON.stringify(packageBody, null, 2)}\n`);
      await writeFile(
        join(repo, "manifests", "sources", "skill.json"),
        `${JSON.stringify(
          {
            id: "skill",
            title: "Skill import",
            created_at: "2026-06-10T12:00:00.000Z",
            source_type: "skill",
            raw_path: "raw/imported/skill.md",
            tags: ["brainstack-skill"]
          },
          null,
          2
        )}\n`
      );

      await mkdir(join(installRoot, "imported-skill"), { recursive: true });
      await writeFile(join(installRoot, "imported-skill", "SKILL.md"), "local edits stay put\n");
      const skipped = runBrainctl(["skills", "refresh", "--config", configPath, "--repo", repo, "--dir", installRoot, "--no-sync"], { HOME: home });
      expectSuccess(skipped);
      expect(skipped.stdout).toContain("skipped=1 imported-skill");
      expect(await readFile(join(installRoot, "imported-skill", "SKILL.md"), "utf8")).toBe("local edits stay put\n");

      const forced = runBrainctl(["skills", "refresh", "--config", configPath, "--repo", repo, "--dir", installRoot, "--no-sync", "--force"], { HOME: home });
      expectSuccess(forced);
      expect(forced.stdout).toContain("installed=1 imported-skill");
      expect(await readFile(join(installRoot, "imported-skill", "SKILL.md"), "utf8")).toContain("Imported shared skill");
      expect(await readFile(join(installRoot, "imported-skill", "references", "note.md"), "utf8")).toBe(referenceText);
      expect(await Bun.file(join(installRoot, "imported-skill", ".brainstack-skill-package.json")).exists()).toBe(true);

      const doctor = runBrainctl(["skills", "doctor", "--dir", installRoot, "--json"], { HOME: home });
      expectSuccess(doctor);
      const doctorBody = JSON.parse(doctor.stdout) as Record<string, unknown>;
      expect(doctorBody.ok).toBe(true);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("hooks install status remove preserve unrelated hooks and hook run fails open", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-hooks-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const configPath = join(dir, "client.yaml");
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: join(dir, "shared-brain") });
      await mkdir(join(home, ".codex"), { recursive: true });
      await writeFile(
        join(home, ".codex", "hooks.json"),
        `${JSON.stringify({ hooks: { UserPromptSubmit: [{ hooks: [{ type: "command", command: "echo keep" }] }] } }, null, 2)}\n`
      );

      const install = runBrainctl(["hooks", "install", "--target", "all", "--config", configPath, "--brainctl", "brainctl"], { HOME: home });
      expectSuccess(install);
      const status = runBrainctl(["hooks", "status", "--target", "all"], { HOME: home });
      expectSuccess(status);
      expect(status.stdout).toContain("codex: installed");
      expect(status.stdout).toContain("claude: installed");
      expect(status.stdout).toContain("cursor: installed");

      const codexHooks = JSON.parse(await readFile(join(home, ".codex", "hooks.json"), "utf8")) as Record<string, unknown>;
      expect(JSON.stringify(codexHooks)).toContain("echo keep");
      expect(JSON.stringify(codexHooks)).toContain("hook run --harness codex");
      const claudeHooks = JSON.parse(await readFile(join(home, ".claude", "settings.json"), "utf8")) as Record<string, unknown>;
      expect(JSON.stringify(claudeHooks)).toContain("hook run --harness claude");
      const cursorHooks = JSON.parse(await readFile(join(home, ".cursor", "hooks.json"), "utf8")) as Record<string, unknown>;
      expect(JSON.stringify(cursorHooks)).toContain("beforeSubmitPrompt");
      expect(JSON.stringify(cursorHooks)).toContain("hook run --harness cursor");

      const spacedBrainctl = join(home, "bin with spaces", "brainctl");
      await mkdir(dirname(spacedBrainctl), { recursive: true });
      await writeExecutable(spacedBrainctl, "#!/usr/bin/env sh\nexit 0\n");
      const spacedInstall = runBrainctl(["hooks", "install", "--target", "codex", "--config", configPath, "--brainctl", spacedBrainctl], { HOME: home });
      expectSuccess(spacedInstall);
      const spacedCodexHooks = JSON.parse(await readFile(join(home, ".codex", "hooks.json"), "utf8")) as Record<string, unknown>;
      expect(JSON.stringify(spacedCodexHooks)).toContain(shellQuote(spacedBrainctl));

      const rawInstall = runBrainctl(["hooks", "install", "--target", "codex", "--config", configPath, "--brainctl-command", "env BRAINSTACK_TEST=1 brainctl"], { HOME: home });
      expectSuccess(rawInstall);
      const rawCodexHooks = JSON.parse(await readFile(join(home, ".codex", "hooks.json"), "utf8")) as Record<string, unknown>;
      expect(JSON.stringify(rawCodexHooks)).toContain("env BRAINSTACK_TEST=1 brainctl hook run");

      const transientInstall = runBrainctl(["hooks", "install", "--target", "codex", "--config", configPath, "--brainctl", "/Volumes/Brainstack Menu.app/Contents/Resources/brainctl"], { HOME: home });
      expect(transientInstall.code).not.toBe(0);
      expect(`${transientInstall.stdout}\n${transientInstall.stderr}`).toContain("transient path");

      const hookCommand = [
        "printf",
        "%s",
        shellQuote(JSON.stringify({ prompt: "hello" })),
        "|",
        "bun",
        "--no-env-file",
        "run",
        shellQuote(BRAINCTL),
        "hook",
        "run",
        "--harness",
        "codex",
        "--event",
        "UserPromptSubmit",
        "--config",
        shellQuote(configPath)
      ].join(" ");
      const hookRun = runCommand(["sh", "-c", hookCommand], { env: { HOME: home } });
      expectSuccess(hookRun);
      expect(hookRun.stdout.trim()).toBe("{}");
      const eventFiles = await readdir(join(stateRoot, "harness-events"));
      expect(eventFiles.length).toBeGreaterThan(0);
      const eventLog = await readFile(join(stateRoot, "harness-events", eventFiles[0]), "utf8");
      expect(eventLog).toContain("UserPromptSubmit");
      expect(eventLog).toContain("refresh-skipped");
      expect(eventLog).toContain(sha256Text("hello"));
      expect(eventLog).not.toContain('"prompt":"hello"');

      const transcriptPath = join(dir, "rollout-2026-06-12T15-18-49-019ebbfc-3a60-7f61-a4fa-a89282b8d83f.jsonl");
      const transcriptText = `${JSON.stringify({ type: "session_meta", payload: { id: "019ebbfc-3a60-7f61-a4fa-a89282b8d83f" } })}\n`;
      await writeFile(transcriptPath, transcriptText);
      const stopHookCommand = [
        "printf",
        "%s",
        shellQuote(JSON.stringify({ session_id: "019ebbfc-3a60-7f61-a4fa-a89282b8d83f", cwd: "/repo/lindy-debug", transcript_path: transcriptPath })),
        "|",
        "bun",
        "--no-env-file",
        "run",
        shellQuote(BRAINCTL),
        "hook",
        "run",
        "--harness",
        "codex",
        "--event",
        "Stop",
        "--config",
        shellQuote(configPath)
      ].join(" ");
      const stopHookRun = runCommand(["sh", "-c", stopHookCommand], { env: { HOME: home } });
      expectSuccess(stopHookRun);
      expect(stopHookRun.stdout.trim()).toBe("{}");
      const outboxNamespaces = await readdir(join(stateRoot, "outbox"));
      const queuedFiles = (
        await Promise.all(outboxNamespaces.map(async (namespace) => (await readdir(join(stateRoot, "outbox", namespace))).map((file) => join(stateRoot, "outbox", namespace, file))))
      ).flat();
      expect(queuedFiles.length).toBeGreaterThan(0);
      const queuedItems = await Promise.all(queuedFiles.map(async (file) => JSON.parse(await readFile(file, "utf8")) as Record<string, any>));
      const checkpoint = queuedItems
        .map((item) => item.payload_storage?.data || item.payload)
        .find((payload) => payload?.source_type === "codex-session-checkpoint") as Record<string, unknown> | undefined;
      expect(checkpoint?.conversation_id).toBe("019ebbfc-3a60-7f61-a4fa-a89282b8d83f");
      expect(checkpoint?.transcript_path).toBe(transcriptPath);
      expect(checkpoint?.transcript_path_durable).toBe(false);
      expect(checkpoint?.transcript_bytes).toBeGreaterThan(0);
      expect(checkpoint?.transcript_sha256).toBe(sha256Text(transcriptText));
      expect(String(checkpoint?.text)).toContain("Use `brainctl import codex-session");
      expect(String(checkpoint?.text)).toContain("Transcript sha256");
      expect(String(checkpoint?.text)).not.toContain("session_meta");

      const remove = runBrainctl(["hooks", "remove", "--target", "all"], { HOME: home });
      expectSuccess(remove);
      const removedCodexHooks = JSON.parse(await readFile(join(home, ".codex", "hooks.json"), "utf8")) as Record<string, unknown>;
      expect(JSON.stringify(removedCodexHooks)).toContain("echo keep");
      expect(JSON.stringify(removedCodexHooks)).not.toContain("hook run --harness codex");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("hooks status and remove surface malformed configs instead of clobbering them", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-hooks-malformed-"));
    try {
      const home = join(dir, "home");
      const configPath = join(dir, "client.yaml");
      const hooksPath = join(home, ".codex", "hooks.json");
      await writeFixtureClientConfig(configPath, { home, stateRoot: join(dir, "state"), localPath: join(dir, "shared-brain") });
      await mkdir(dirname(hooksPath), { recursive: true });
      await writeFile(hooksPath, "{not-json");

      const status = runBrainctl(["hooks", "status", "--target", "codex", "--config", configPath], { HOME: home });
      expect(status.code).not.toBe(0);
      expect(`${status.stdout}\n${status.stderr}`).toContain("codex: error");
      expect(`${status.stdout}\n${status.stderr}`).toContain("hooks status found 1 malformed config file");

      const aggregate = runBrainctl(["status", "--json", "--config", configPath, "--timeout-ms", "500"], { HOME: home });
      expectSuccess(aggregate);
      const report = JSON.parse(aggregate.stdout) as Record<string, any>;
      expect(report.sections.hooks.state).toBe("warn");
      const codexHooks = report.sections.hooks.data.hooks.find((entry: Record<string, unknown>) => entry.target === "codex") as Record<string, unknown>;
      expect(String(codexHooks.error)).toContain("JSON Parse error");

      await mkdir(join(home, ".claude"), { recursive: true });
      await writeFile(
        join(home, ".claude", "settings.json"),
        `${JSON.stringify(
          {
            hooks: {
              SessionStart: [
                {
                  hooks: [
                    {
                      type: "command",
                      command: `brainctl hook run --harness claude --event SessionStart --config ${configPath}`
                    }
                  ]
                }
              ]
            }
          },
          null,
          2
        )}\n`
      );
      await writeFile(configPath, `${await readFile(configPath, "utf8")}harness:\n  name: claude\n  bin: claude\n`);
      const selectedHealthyButOtherMalformed = runBrainctl(["status", "--json", "--config", configPath, "--timeout-ms", "500"], { HOME: home });
      expectSuccess(selectedHealthyButOtherMalformed);
      const selectedReport = JSON.parse(selectedHealthyButOtherMalformed.stdout) as Record<string, any>;
      expect(selectedReport.sections.hooks.state).toBe("warn");
      expect(selectedReport.sections.hooks.detail).toContain("claude hooks installed errors=1");
      expect(selectedReport.degraded).toBe(true);
      const selectedClaude = selectedReport.sections.hooks.data.hooks.find((entry: Record<string, unknown>) => entry.target === "claude") as Record<string, unknown>;
      expect(selectedClaude.installed).toBe(true);

      const remove = runBrainctl(["hooks", "remove", "--target", "codex", "--config", configPath], { HOME: home });
      expect(remove.code).not.toBe(0);
      expect(await readFile(hooksPath, "utf8")).toBe("{not-json");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("daemon once keeps the local shared-brain clone fresh and records status", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-daemon-once-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const configPath = join(dir, "client.yaml");
      const bare = join(dir, "shared-brain.git");
      const seed = join(dir, "seed");
      const clone = join(home, "shared-brain");
      git(["init", "--bare", "--initial-branch=main", bare], dir);
      git(["clone", bare, seed], dir);
      await writeFile(join(seed, "README.md"), "v1\n");
      git(["add", "README.md"], seed);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "seed"], seed);
      git(["push", "-u", "origin", "main"], seed);
      git(["clone", bare, clone], dir);
      await writeFile(join(seed, "README.md"), "v2\n");
      git(["add", "README.md"], seed);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "update"], seed);
      git(["push"], seed);
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: clone });

      const result = runBrainctl(["daemon", "once", "--config", configPath, "--target", "codex"], { HOME: home });
      expectSuccess(result);
      expect(await readFile(join(clone, "README.md"), "utf8")).toBe("v2\n");
      const status = JSON.parse(await readFile(join(stateRoot, "daemon", "status.json"), "utf8")) as Record<string, unknown>;
      expect(status.ok).toBe(true);
      expect(status.iteration).toBe(1);
      expect((status.repo as Record<string, unknown>).exists).toBe(true);
      expect((status.repo as Record<string, unknown>).clean).toBe(true);
      expect((status.repo as Record<string, unknown>).head).toBe(git(["rev-parse", "HEAD"], clone));
      expect((status.outbox as Record<string, unknown>).detail).toContain("flushed=");
      expect((status.skills as Record<string, unknown>).detail).toContain("refreshed");
      expect(await Bun.file(join(stateRoot, "daemon", "brainstackd.lock")).exists()).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("daemon once refuses to refresh skills from a dirty shared-brain clone", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-daemon-dirty-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const configPath = join(dir, "client.yaml");
      const bare = join(dir, "shared-brain.git");
      const seed = join(dir, "seed");
      const clone = join(home, "shared-brain");
      git(["init", "--bare", "--initial-branch=main", bare], dir);
      git(["clone", bare, seed], dir);
      await writeFile(join(seed, "README.md"), "v1\n");
      git(["add", "README.md"], seed);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "seed"], seed);
      git(["push", "-u", "origin", "main"], seed);
      git(["clone", bare, clone], dir);
      await writeFile(join(clone, "local-edit.md"), "not canonical yet\n");
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: clone });

      const result = runBrainctl(["daemon", "once", "--config", configPath, "--target", "codex"], { HOME: home });
      expectSuccess(result);
      const status = JSON.parse(await readFile(join(stateRoot, "daemon", "status.json"), "utf8")) as Record<string, unknown>;
      expect(status.ok).toBe(false);
      expect((status.repo as Record<string, unknown>).clean).toBe(false);
      expect((status.skills as Record<string, unknown>).ok).toBe(false);
      expect((status.skills as Record<string, unknown>).detail).toContain("unsafe for skill refresh");
      expect(await Bun.file(join(home, ".codex", "skills")).exists()).toBe(false);

      const daemonStatus = runBrainctl(["daemon", "status", "--config", configPath, "--platform", "systemd", "--json"], { HOME: home });
      expectSuccess(daemonStatus);
      const daemonBody = JSON.parse(daemonStatus.stdout) as Record<string, unknown>;
      expect(daemonBody.fresh).toBe(true);
      expect(daemonBody.last_run_ok).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("status separates daemon heartbeat freshness from last-run degradation", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-daemon-heartbeat-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const configPath = join(dir, "client.yaml");
      const clone = join(home, "shared-brain");
      await mkdir(join(stateRoot, "daemon"), { recursive: true });
      await mkdir(clone, { recursive: true });
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: clone });
      await writeFile(
        join(stateRoot, "daemon", "status.json"),
        `${JSON.stringify(
          {
            schema_version: 1,
            product: "brainstack",
            daemon: "brainctl daemon run",
            ok: false,
            pid: process.pid,
            machine: "client",
            config_path: configPath,
            state_path: join(stateRoot, "daemon", "status.json"),
            started_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
            iteration: 7,
            repo: {
              path: clone,
              exists: true,
              clean: true,
              branch: "main",
              head: "abc123",
              last_pull_at: new Date().toISOString()
            },
            outbox: {
              ok: false,
              detail: "flushed=0 kept=0 terminal_failures=3 corrupt=0",
              flushed: 0,
              kept: 0,
              terminal_failures: 3,
              corrupt: 0
            },
            skills: {
              ok: true,
              detail: "refreshed shared skills from local clone",
              targets: ["codex"],
              installed: [],
              skipped: []
            },
            errors: ["outbox: flushed=0 kept=0 terminal_failures=3 corrupt=0"],
            next_run_after: new Date(Date.now() + 60_000).toISOString()
          },
          null,
          2
        )}\n`
      );

      const reportResult = runBrainctl(["status", "--config", configPath, "--platform", "systemd", "--timeout-ms", "500", "--json"], { HOME: home });
      expectSuccess(reportResult);
      const report = JSON.parse(reportResult.stdout) as Record<string, any>;
      expect(report.sections.daemon.state).toBe("ok");
      expect(report.sections.daemon.detail).toContain("heartbeat=fresh");
      expect(report.sections.daemon.detail).toContain("last_run=degraded");
      expect(report.sections.daemon.data.fresh).toBe(true);
      expect(report.sections.daemon.data.last_run_ok).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("daemon once refuses to start when another daemon lock is live", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-daemon-lock-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const configPath = join(dir, "client.yaml");
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: join(home, "shared-brain") });
      await mkdir(join(stateRoot, "daemon"), { recursive: true });
      await writeFile(join(stateRoot, "daemon", "brainstackd.lock"), `${JSON.stringify({ pid: process.pid, created_at: new Date().toISOString() })}\n`);

      const result = runBrainctl(["daemon", "once", "--config", configPath], { HOME: home });
      expect(result.code).not.toBe(0);
      expect(`${result.stdout}\n${result.stderr}`).toContain("brainstack daemon already running");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("daemon once clears a stale pid-only lock", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-daemon-stale-lock-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const configPath = join(dir, "client.yaml");
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: join(home, "shared-brain") });
      await mkdir(join(stateRoot, "daemon"), { recursive: true });
      await writeFile(join(stateRoot, "daemon", "brainstackd.lock"), `${JSON.stringify({ pid: 999_999_999, created_at: new Date().toISOString() })}\n`);

      const result = runBrainctl(["daemon", "once", "--config", configPath, "--no-sync", "--no-skills", "--no-flush"], { HOME: home });
      expectSuccess(result);
      expect(await Bun.file(join(stateRoot, "daemon", "brainstackd.lock")).exists()).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("daemon lock ignores a live reused pid when identity does not match brainstackd", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-daemon-lock-identity-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const configPath = join(dir, "client.yaml");
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: join(home, "shared-brain") });
      await mkdir(join(stateRoot, "daemon"), { recursive: true });
      await writeFile(
        join(stateRoot, "daemon", "brainstackd.lock"),
        `${JSON.stringify({ pid: process.pid, created_at: new Date().toISOString(), host: hostname(), argv: ["/usr/bin/not-brainstack"] })}\n`
      );

      const result = runBrainctl(["daemon", "once", "--config", configPath, "--no-sync", "--no-skills", "--no-flush"], { HOME: home });
      expectSuccess(result);
      expect(await Bun.file(join(stateRoot, "daemon", "brainstackd.lock")).exists()).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("daemon install status and uninstall manage a user service without a second binary", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-daemon-install-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const systemdRoot = join(dir, "systemd-user");
      const configPath = join(dir, "client.yaml");
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
          `  stateRoot: ${stateRoot}`,
          `  systemdUserRoot: ${systemdRoot}`,
          "client:",
          `  localPath: ${join(home, "shared-brain")}`,
          `  remoteSsh: ${join(dir, "shared-brain.git")}`,
          ""
        ].join("\n")
      );

      const dryRun = runBrainctl(["daemon", "install", "--config", configPath, "--platform", "systemd", "--dry-run", "--brainctl", "brainctl"], { HOME: home });
      expectSuccess(dryRun);
      expect(dryRun.stdout).toContain("daemon run");
      expect(dryRun.stdout).toContain("brainstackd.service");
      const install = runBrainctl(["daemon", "install", "--config", configPath, "--platform", "systemd", "--brainctl", "brainctl"], { HOME: home });
      expectSuccess(install);
      const servicePath = join(systemdRoot, "brainstackd.service");
      const service = await readFile(servicePath, "utf8");
      expect(service).toContain("daemon run");
      expect(service).toContain(configPath);

      const spacedBrainctl = join(home, "stable bin", "brainctl");
      await mkdir(dirname(spacedBrainctl), { recursive: true });
      await writeExecutable(spacedBrainctl, "#!/usr/bin/env sh\nexit 0\n");
      const spacedDryRun = runBrainctl(["daemon", "install", "--config", configPath, "--platform", "systemd", "--dry-run", "--brainctl", spacedBrainctl], { HOME: home });
      expectSuccess(spacedDryRun);
      expect(spacedDryRun.stdout).toContain(shellQuote(spacedBrainctl));

      const transientDryRun = runBrainctl(["daemon", "install", "--config", configPath, "--platform", "systemd", "--dry-run", "--brainctl", "/Volumes/Brainstack Menu.app/Contents/Resources/brainctl"], { HOME: home });
      expect(transientDryRun.code).not.toBe(0);
      expect(`${transientDryRun.stdout}\n${transientDryRun.stderr}`).toContain("transient path");

      const manifest = JSON.parse(await readFile(join(home, ".config", "brainstack", "managed-artifacts.json"), "utf8")) as Record<string, unknown>;
      const artifactPaths = (manifest.artifacts as Array<Record<string, unknown>>).map((artifact) => artifact.path);
      expect(artifactPaths).toContain(servicePath);

      const status = runBrainctl(["daemon", "status", "--config", configPath, "--platform", "systemd", "--json"], { HOME: home });
      expectSuccess(status);
      const body = JSON.parse(status.stdout) as Record<string, unknown>;
      expect((body.service as Record<string, unknown>).installed).toBe(true);
      expect((body.service as Record<string, unknown>).path).toBe(servicePath);

      const uninstall = runBrainctl(["daemon", "uninstall", "--config", configPath, "--platform", "systemd"], { HOME: home });
      expectSuccess(uninstall);
      expect(await Bun.file(servicePath).exists()).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("daemon launchd start does not double-restart after bootstrap", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-daemon-launchd-start-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const configPath = join(dir, "client.yaml");
      const binDir = join(dir, "bin");
      const logPath = join(dir, "launchctl.log");
      await mkdir(binDir, { recursive: true });
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: join(home, "shared-brain") });
      await writeExecutable(
        join(binDir, "launchctl"),
        [
          "#!/usr/bin/env sh",
          `printf '%s\\n' "$*" >> ${shellQuote(logPath)}`,
          "exit 0",
          ""
        ].join("\n")
      );

      const result = runBrainctl(["daemon", "install", "--start", "--platform", "launchd", "--config", configPath, "--brainctl", "brainctl"], {
        HOME: home,
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expectSuccess(result);

      const uid = typeof process.getuid === "function" ? process.getuid() : process.env.UID || "";
      const servicePath = join(home, "Library", "LaunchAgents", "com.brainstack.daemon.plist");
      const calls = (await readFile(logPath, "utf8")).trim().split(/\r?\n/);
      expect(calls).toEqual([
        `bootout gui/${uid} ${servicePath}`,
        `bootstrap gui/${uid} ${servicePath}`
      ]);
      expect(calls.some((call) => call.includes("kickstart"))).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("remember uses the enrolled default config when --config is omitted", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-default-config-"));
    try {
      const home = join(dir, "home");
      const configRoot = join(home, ".config", "brainstack");
      const stateRoot = join(dir, "state");
      const configPath = join(configRoot, "brainstack.yaml");
      await mkdir(configRoot, { recursive: true });
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
          `  stateRoot: ${stateRoot}`,
          `  configRoot: ${configRoot}`,
          "brain:",
          "  publicBaseUrl: http://127.0.0.1:9",
          "client:",
          `  localPath: ${join(home, "shared-brain")}`,
          `  envPath: ${join(home, ".config", "shared-brain.env")}`,
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      await mkdir(join(home, ".config"), { recursive: true });
      await writeFile(join(home, ".config", "shared-brain.env"), "BRAIN_IMPORT_TOKEN=test-token\n");

      const result = runBrainctl(["remember", "--repo", dir, "--summary", "default config smoke"], { HOME: home, BRAINSTACK_CONFIG: "" });
      expectSuccess(result);
      expect(`${result.stdout}\n${result.stderr}`).toContain("shared-brain write queued:");
      expect(`${result.stdout}\n${result.stderr}`).not.toContain("writable but missing explicit baseUrl/importTokenEnv");
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

  test("lifecycle status and help paths are safe without an install", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-lifecycle-help-"));
    try {
      const missingConfig = join(dir, "missing.yaml");
      const status = runBrainctl(["lifecycle", "status", "--json", "--config", missingConfig, "--timeout-ms", "50"], { HOME: dir });
      expectSuccess(status);
      const parsed = JSON.parse(status.stdout) as { ok: boolean; sections: Record<string, { state: string; error?: string }> };
      expect(parsed.ok).toBe(false);
      expect(parsed.sections.config.state).toBe("fail");
      expect(parsed.sections.config.error || "").toContain("Brainstack config not found");

      const lifecycleHelp = runBrainctl(["lifecycle", "help"], { HOME: dir });
      expectSuccess(lifecycleHelp);
      expect(lifecycleHelp.stdout).toContain("brainctl lifecycle status");
      expect(lifecycleHelp.stdout).toContain("brainctl lifecycle repair");

      const uninstallHelp = runBrainctl(["lifecycle", "uninstall", "--help"], { HOME: dir });
      expectSuccess(uninstallHelp);
      expect(uninstallHelp.stdout).toContain("brainctl lifecycle uninstall");
      expect(uninstallHelp.stderr).toBe("");

      const destroyHelp = runBrainctl(["destroy", "--help"], { HOME: dir });
      expectSuccess(destroyHelp);
      expect(destroyHelp.stdout).toContain("Usage: brainctl destroy");
      expect(destroyHelp.stderr).toBe("");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("lifecycle repair dry-run explains the composed client repair without writes", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-lifecycle-repair-plan-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(home, ".local", "state", "brainstack");
      const configPath = join(dir, "client.yaml");
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: join(home, "shared-brain") });

      const result = runBrainctl(["lifecycle", "repair", "--config", configPath, "--dry-run"], { HOME: home });
      expectSuccess(result);
      expect(result.stdout).toContain("lifecycle repair plan");
      expect(result.stdout).toContain("brainctl apply-runtime --config");
      expect(result.stdout).toContain("guidance");
      expect(result.stdout).toContain("brainctl daemon install --config");
      expect(result.stdout).toContain("brainctl hooks install --target all");
      expect(result.stdout).toContain("brainctl skills refresh --target codex");
      expect(result.stdout).toContain("brainctl skills refresh --target claude");
      expect(result.stdout).toContain("brainctl skills refresh --target cursor");
      expect(result.stdout).toContain("--no-sync");
      expect(existsSync(join(home, ".config", "brainstack"))).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("lifecycle repair reinstalls local client runtime, daemon, hooks, and skills surfaces", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-lifecycle-repair-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(home, ".local", "state", "brainstack");
      const configPath = join(dir, "client.yaml");
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: join(home, "shared-brain") });

      const result = runBrainctl(
        ["lifecycle", "repair", "--config", configPath, "--target", "codex", "--platform", "systemd", "--no-start", "--no-status"],
        { HOME: home }
      );
      expectSuccess(result);
      expect(result.stdout).toContain("apply-runtime complete for client-macos");
      expect(result.stdout).toContain("local guidance repaired:");
      expect(result.stdout).toContain("daemon installed: platform=systemd");
      expect(result.stdout).toContain("codex: hooks installed");
      expect(result.stdout).toContain("skills_refresh target=codex");
      expect(existsSync(join(home, ".config", "brainstack", "client-bootstrap", "codex-shared-brain.include.md"))).toBe(true);
      expect(existsSync(join(home, ".config", "brainstack", "managed-artifacts.json"))).toBe(true);
      expect(await readFile(join(home, ".codex", "AGENTS.md"), "utf8")).toContain("Shared Brain");
      expect(await readFile(join(home, ".claude", "CLAUDE.md"), "utf8")).toContain("@");
      expect(existsSync(join(home, ".cursor", "rules", "shared-brain.md"))).toBe(true);
      expect(existsSync(join(home, ".config", "systemd", "user", "brainstackd.service"))).toBe(true);
      const hooks = await readFile(join(home, ".codex", "hooks.json"), "utf8");
      expect(hooks).toContain("hook run --harness codex");
      expect(hooks).toContain(configPath);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 10_000);

  test("lifecycle uninstall defaults to the selected profile scope", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-lifecycle-uninstall-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(home, ".local", "state", "brainstack");
      const configPath = join(dir, "client.yaml");
      await writeFixtureClientConfig(configPath, { home, stateRoot, localPath: join(home, "shared-brain") });

      const result = runBrainctl(["lifecycle", "uninstall", "--config", configPath, "--dry-run"], { HOME: home });
      expectSuccess(result);
      expect(result.stdout).toContain("dry-run destroy plan");
      expect(result.stdout).toContain("scope=client");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("lifecycle uninstall defaults control installs to full managed artifact removal", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-lifecycle-uninstall-control-"));
    try {
      const home = join(dir, "home");
      const configRoot = join(home, ".config", "brainstack");
      const stateRoot = join(home, ".local", "state", "brainstack");
      const sharedBrainRoot = join(home, "shared-brain");
      const configPath = join(dir, "control.yaml");
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
          `  sharedBrainRoot: ${sharedBrainRoot}`,
          "telemux:",
          "  enabled: false",
          ""
        ].join("\n")
      );

      const result = runBrainctl(["lifecycle", "uninstall", "--config", configPath, "--dry-run"], { HOME: home });
      expectSuccess(result);
      expect(result.stdout).toContain("dry-run destroy plan");
      expect(result.stdout).toContain("scope=all");
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
      const runtimeEnv = await readFile(join(configRoot, "braind.runtime.env"), "utf8");
      expect(runtimeEnv).toContain("BRAIN_BIND=127.0.0.1");
      expect(runtimeEnv).toContain("BRAIN_CURATION_MODE=approval");
      expect(runtimeEnv).toContain("BRAIN_CURATION_ALLOWED_PATHS=wiki/Status/**,wiki/Sources/**");
      expect(runtimeEnv).toContain("BRAIN_CURATION_MAX_CHANGED_LINES=40");
      expect(runtimeEnv).toContain("BRAIN_CURATION_ALLOW_DELETES=0");
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

  test("control init renders client bootstrap files for guidance checks", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-control-bootstrap-"));
    try {
      const home = join(dir, "home");
      const configRoot = join(home, ".config", "brainstack");
      const stateRoot = join(home, ".local", "state", "brainstack");
      const configPath = join(dir, "control.yaml");
      await mkdir(home, { recursive: true });
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "harness:",
          "  name: codex",
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "paths:",
          `  home: ${home}`,
          `  configRoot: ${configRoot}`,
          `  stateRoot: ${stateRoot}`,
          "telemux:",
          "  enabled: false",
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );

      expectSuccess(runBrainctl(["init", "--profile", "control", "--config", configPath]));
      expect(await Bun.file(join(configRoot, "client-bootstrap", "codex-shared-brain.include.md")).exists()).toBe(true);
      expect(await Bun.file(join(configRoot, "client-bootstrap", "claude-user-CLAUDE.md")).exists()).toBe(true);
      expect(await Bun.file(join(home, ".claude", "CLAUDE.md")).exists()).toBe(false);
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
