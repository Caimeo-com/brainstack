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

describe("public release hygiene - fleet remote control invites and release", () => {
  test("fleet status detects behind product repos and exposes dry-run updates", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-fleet-status-"));
    try {
      const configPath = join(dir, "config.yaml");
      const bare = join(dir, "product.git");
      const seed = join(dir, "seed");
      const productRepo = join(dir, "product");
      git(["init", "--bare", "--initial-branch=main", bare], dir);
      git(["clone", bare, seed], dir);
      await writeFile(join(seed, "README.md"), "v1\n");
      git(["add", "README.md"], seed);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "seed"], seed);
      git(["push", "-u", "origin", "main"], seed);
      git(["clone", bare, productRepo], dir);
      await writeFile(join(seed, "README.md"), "v2\n");
      git(["add", "README.md"], seed);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "update"], seed);
      git(["push"], seed);
      await writeFixtureConfig(configPath);
      const fixtureConfig = await readFile(configPath, "utf8");
      await writeFile(
        configPath,
        fixtureConfig.replace("paths:\n  home: /tmp/brainstack-test-home\n", `paths:\n  home: ${join(dir, "home")}\n  productRepo: ${productRepo}\n`)
      );

      const status = runBrainctl(["fleet", "status", "--json", "--config", configPath]);
      expectSuccess(status);
      const body = JSON.parse(status.stdout) as Record<string, any>;
      expect(body.summary.needs_update).toBe(1);
      expect(body.machines[0].name).toBe("brain-control");
      expect(body.machines[0].update_state).toBe("behind");
      expect(body.machines[0].needs_update).toBe(true);

      const dryRun = runBrainctl(["fleet", "update", "brain-control", "--dry-run", "--config", configPath]);
      expectSuccess(dryRun);
      expect(dryRun.stdout).toContain("OK brain-control");
      expect(dryRun.stdout).toContain("git fetch --quiet origin main");
      expect(dryRun.stdout).toContain("git merge --ff-only origin/main");
      expect(git(["rev-parse", "HEAD"], productRepo)).not.toBe(git(["rev-parse", "origin/main"], productRepo));

      await writeFile(join(productRepo, "README.md"), "local\n");
      git(["add", "README.md"], productRepo);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "local"], productRepo);
      const diverged = runBrainctl(["fleet", "status", "--json", "--config", configPath]);
      expectSuccess(diverged);
      const divergedBody = JSON.parse(diverged.stdout) as Record<string, any>;
      expect(divergedBody.summary.needs_update).toBe(0);
      expect(divergedBody.machines[0].update_state).toBe("ahead");
      expect(divergedBody.machines[0].needs_update).toBe(false);
      expect(divergedBody.machines[0].detail).toContain("diverged");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

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
      const productRepo = join(dir, "product");
      await mkdir(productRepo, { recursive: true });
      git(["init"], productRepo);
      git(["config", "user.email", "operator@example.test"], productRepo);
      git(["config", "user.name", "Operator"], productRepo);
      await writeFile(join(productRepo, "README.md"), "# product\n");
      git(["add", "README.md"], productRepo);
      git(["commit", "-m", "initial"], productRepo);
      const productHead = git(["rev-parse", "HEAD"], productRepo);
      await writeFixtureConfig(configPath);
      const fixtureConfig = await readFile(configPath, "utf8");
      await writeFile(
        configPath,
        `${fixtureConfig.replace("paths:\n  home: /tmp/brainstack-test-home\n", `paths:\n  home: /tmp/brainstack-test-home\n  productRepo: ${productRepo}\n`)}harness:\n  name: codex\n  bin: ${join(binDir, "codex")}\n`
      );
      const result = runBrainctl(["updates", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
        BRAINSTACK_UPDATE_PROBE_COMMANDS: "brew,pacman"
      });
      expectSuccess(result);
      expect(result.stdout).toContain(`brainstack_source=${productRepo}`);
      expect(result.stdout).toContain(`brainstack_head=${productHead}`);
      expect(result.stdout).toContain("brainstack_head=");
      expect(result.stdout).toContain("remote_main_ref=");
      expect(result.stdout).toContain("os_update_checks:");
      expect(result.stdout).toContain("brew outdated --quiet (HOMEBREW_NO_AUTO_UPDATE=1):");
      expect(result.stdout).toContain("brew-pkg 1.0 -> 1.1");
      expect(result.stdout).toContain("pacman -Qu:");
      expect(result.stdout).toContain("codex-cli 0.134.0-1 -> 0.135.0-1");
      expect(result.stdout).toContain("manual_update_commands:");
      expect(result.stdout).toContain(`brainstack: cd ${productRepo} && git pull --ff-only`);
      expect(result.stdout).toContain("selected harness:");

      await writeFile(
        join(binDir, "codex"),
        "#!/usr/bin/env sh\nif [ \"${1:-}\" = \"--version\" ]; then printf 'codex-cli missing-output\\n'; exit 0; fi\nprintf '%s\\n' '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check'\n"
      );
      await chmod(join(binDir, "codex"), 0o755);
      const missingOutputFlag = runBrainctl(["updates", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
        BRAINSTACK_UPDATE_PROBE_TIMEOUT_MS: "5000",
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

      const partialBinDir = join(dir, "partial-bin");
      await mkdir(partialBinDir, { recursive: true });
      await writeFile(
        join(partialBinDir, "codex"),
        [
          "#!/usr/bin/env sh",
          "if [ \"${1:-}\" = \"--version\" ]; then printf 'codex-cli partial\\n'; exit 0; fi",
          "printf '%s\\n' '--dangerously-bypass-approvals-and-sandbox --skip-git-repo-check --output-last-message'",
          "sleep 5",
          ""
        ].join("\n")
      );
      await chmod(join(partialBinDir, "codex"), 0o755);
      const partialHelpTimeout = runBrainctl(["updates", "--config", configPath], {
        PATH: `${partialBinDir}:${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1",
        BRAINSTACK_UPDATE_PROBE_COMMANDS: "none",
        BRAINSTACK_UPDATE_PROBE_TIMEOUT_MS: "1000"
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
      expect(gitTimeout.stdout).toContain("brainstack_head=unavailable");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 30_000);

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
  }, 10_000);

  test("doctor resolves remote worker harness through the worker user's shell PATH", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-remote-path-"));
    try {
      const controlBin = join(dir, "control-bin");
      const remoteBin = join(dir, "remote-bin");
      await mkdir(controlBin, { recursive: true });
      await mkdir(remoteBin, { recursive: true });
      await writeFakeDoctorCodex(controlBin);
      await writeFakeTailscale(controlBin);

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
      for (const name of ["bun", "git", "ssh", "tailscale", "sudo"]) {
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
          "unset BRAINSTACK_SKIP_USER_PATH_RESOLVE",
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
  }, 30_000);

  test("telegram send-file streams local file over SSH with control-host trust and no Telegram secrets", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-telegram-send-file-"));
    try {
      const binDir = join(dir, "bin");
      const configRoot = join(dir, "config");
      await mkdir(binDir, { recursive: true });
      await mkdir(configRoot, { recursive: true });
      const filePath = join(dir, "mobile-report.txt");
      const uploadedPath = join(dir, "uploaded.txt");
      const argsCapture = join(dir, "ssh-args.txt");
      await writeFile(filePath, "hello telegram mobile\n");
      await writeFile(join(configRoot, "ssh_known_hosts"), "control.example ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeBrainstackControlKeyForTestsOnly\n");

      const fakeSsh = join(binDir, "ssh");
      await writeFile(
        fakeSsh,
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          `printf '<%s>\\n' "$@" > '${argsCapture}'`,
          `cat > '${uploadedPath}'`,
          "cat <<'JSON'",
          '{"ok":true,"fileName":"mobile-report.txt","sizeBytes":22,"mimeType":"text/plain","kind":"document","target":{"mode":"control","contextSlug":null,"chatId":-100111,"threadId":null},"deleted":true}',
          "JSON",
          ""
        ].join("\n")
      );
      await chmod(fakeSsh, 0o755);

      const configPath = join(dir, "client.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: mac-client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  configRoot: ${configRoot}`,
          "client:",
          "  remoteSsh: operator@control.example:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );

      const result = runBrainctl(
        [
          "telegram",
          "send-file",
          "--config",
          configPath,
          "--file",
          filePath,
          "--caption",
          "mobile caption",
          "--remote-repo",
          "~/brainstack",
          "--json"
        ],
        {
          PATH: `${binDir}:${process.env.PATH || ""}`,
          FACTORY_TELEGRAM_BOT_TOKEN: "must-not-leak"
        }
      );

      expectSuccess(result);
      expect(JSON.parse(result.stdout).fileName).toBe("mobile-report.txt");
      expect(await readFile(uploadedPath, "utf8")).toBe("hello telegram mobile\n");
      const sshArgs = await readFile(argsCapture, "utf8");
      expect(sshArgs).toContain("BatchMode=yes");
      expect(sshArgs).toContain("ConnectTimeout=8");
      expect(sshArgs).toContain("StrictHostKeyChecking=yes");
      expect(sshArgs).toContain(`UserKnownHostsFile=${join(configRoot, "ssh_known_hosts")}`);
      expect(sshArgs).toContain("operator@control.example");
      expect(sshArgs).toContain("<bash -lc '");
      expect(sshArgs).not.toContain("\n<bash>\n<-lc>\n");
      expect(sshArgs).toContain("apps/telemux/src/send-file.ts");
      expect(sshArgs).toContain("--display-name");
      expect(sshArgs).toContain("mobile-report.txt");
      expect(sshArgs).not.toContain("must-not-leak");
      expect(result.stdout).not.toContain("must-not-leak");
      expect(result.stderr).not.toContain("must-not-leak");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 10_000);

  test("curator install forwards from client config to the control host over pinned SSH", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-curator-install-client-"));
    try {
      const binDir = join(dir, "bin");
      const configRoot = join(dir, "config");
      await mkdir(binDir, { recursive: true });
      await mkdir(configRoot, { recursive: true });
      const argsCapture = join(dir, "ssh-args.txt");
      await writeFile(join(configRoot, "ssh_known_hosts"), "control.example ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeBrainstackControlKeyForTestsOnly\n");
      const fakeSsh = join(binDir, "ssh");
      await writeFile(
        fakeSsh,
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          `printf '<%s>\\n' "$@" > '${argsCapture}'`,
          "if printf '%s\\n' \"$@\" | grep -q backfill-operational; then",
          "  echo '{\"ok\":true,\"matched\":2,\"superseded\":[\"p1\",\"p2\"],\"overflow\":false}'",
          "else",
          "  echo 'curator install requested remotely'",
          "fi",
          ""
        ].join("\n")
      );
      await chmod(fakeSsh, 0o755);

      const configPath = join(dir, "client.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: mac-client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  configRoot: ${configRoot}`,
          "client:",
          "  remoteSsh: operator@control.example:/home/operator/shared-brain/bare/shared-brain.git",
          "  telegramVia: operator@control.example",
          "  telegramRemoteRepo: /home/operator/brainstack",
          ""
        ].join("\n")
      );

      const result = runBrainctl(["curator", "install", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expectSuccess(result);
      expect(result.stdout).toContain("curator install requested remotely");
      const sshArgs = await readFile(argsCapture, "utf8");
      expect(sshArgs).toContain("BatchMode=yes");
      expect(sshArgs).toContain("ConnectTimeout=8");
      expect(sshArgs).toContain("StrictHostKeyChecking=yes");
      expect(sshArgs).toContain(`UserKnownHostsFile=${join(configRoot, "ssh_known_hosts")}`);
      expect(sshArgs).toContain("operator@control.example");
      expect(sshArgs).toContain("/home/operator/brainstack");
      expect(sshArgs).toContain("'\\''curator'\\'' '\\''install'\\'' --config");

      const backfill = runBrainctl(["curator", "backfill-operational", "--apply", "--limit", "25", "--json", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expectSuccess(backfill);
      expect(JSON.parse(backfill.stdout).superseded).toEqual(["p1", "p2"]);
      const backfillArgs = await readFile(argsCapture, "utf8");
      expect(backfillArgs).toContain("'\\''curator'\\'' '\\''backfill-operational'\\''");
      expect(backfillArgs).toContain("'\\''--apply'\\''");
      expect(backfillArgs).toContain("'\\''--limit'\\'' '\\''25'\\''");
      expect(backfillArgs).toContain("'\\''--json'\\''");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 10_000);

  test("proposal decisions forward from client config to the control host over pinned SSH", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-proposal-decision-client-"));
    try {
      const binDir = join(dir, "bin");
      const configRoot = join(dir, "config");
      await mkdir(binDir, { recursive: true });
      await mkdir(configRoot, { recursive: true });
      const argsCapture = join(dir, "ssh-args.txt");
      await writeFile(join(configRoot, "ssh_known_hosts"), "control.example ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeBrainstackControlKeyForTestsOnly\n");
      const fakeSsh = join(binDir, "ssh");
      await writeFile(
        fakeSsh,
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          `printf '<%s>\\n' "$@" > '${argsCapture}'`,
          "echo '{\"ok\":true,\"proposal_id\":\"proposal-1\",\"action\":\"reject\",\"status\":\"rejected\"}'",
          ""
        ].join("\n")
      );
      await chmod(fakeSsh, 0o755);

      const configPath = join(dir, "client.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: mac-client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  configRoot: ${configRoot}`,
          "client:",
          "  remoteSsh: operator@control.example:/home/operator/shared-brain/bare/shared-brain.git",
          "  telegramVia: operator@control.example",
          "  telegramRemoteRepo: /home/operator/brainstack",
          ""
        ].join("\n")
      );

      const result = runBrainctl(["proposals", "reject", "proposal-1", "--reason", "not useful", "--json", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAIN_ADMIN_TOKEN: ""
      });
      expectSuccess(result);
      expect(JSON.parse(result.stdout).status).toBe("rejected");
      const sshArgs = await readFile(argsCapture, "utf8");
      expect(sshArgs).toContain("BatchMode=yes");
      expect(sshArgs).toContain("ConnectTimeout=8");
      expect(sshArgs).toContain("StrictHostKeyChecking=yes");
      expect(sshArgs).toContain(`UserKnownHostsFile=${join(configRoot, "ssh_known_hosts")}`);
      expect(sshArgs).toContain("operator@control.example");
      expect(sshArgs).toContain("/home/operator/brainstack");
      expect(sshArgs).toContain("'\\''proposals'\\'' '\\''reject'\\'' '\\''proposal-1'\\''");
      expect(sshArgs).toContain("'\\''--reason'\\'' '\\''not useful'\\''");
      expect(sshArgs).toContain("'\\''--json'\\''");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 10_000);

  test("proposal merge discovery forwards from client config to the control host over pinned SSH", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-proposal-merge-client-"));
    try {
      const binDir = join(dir, "bin");
      const configRoot = join(dir, "config");
      await mkdir(binDir, { recursive: true });
      await mkdir(configRoot, { recursive: true });
      const argsCapture = join(dir, "ssh-args.txt");
      await writeFile(join(configRoot, "ssh_known_hosts"), "control.example ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeBrainstackControlKeyForTestsOnly\n");
      const fakeSsh = join(binDir, "ssh");
      await writeFile(
        fakeSsh,
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          `printf '<%s>\\n' "$@" > '${argsCapture}'`,
          "cat <<'JSON'",
          '{"dryRun":false,"harness":"codex","totalOpen":116,"inspected":100,"overflow":true,"autoThreshold":0.8,"candidates":0,"merged":[],"skipped":[],"warnings":["fake remote run"]}',
          "JSON",
          ""
        ].join("\n")
      );
      await chmod(fakeSsh, 0o755);

      const configPath = join(dir, "client.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: mac-client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  configRoot: ${configRoot}`,
          "client:",
          "  remoteSsh: operator@control.example:/home/operator/shared-brain/bare/shared-brain.git",
          "  telegramVia: operator@control.example",
          "  telegramRemoteRepo: /home/operator/brainstack",
          ""
        ].join("\n")
      );

      const result = runBrainctl(["proposals", "batch-merge", "--submit", "--limit", "100", "--auto-threshold", "0.8", "--config", configPath, "--json"], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAIN_BASE_URL: "http://127.0.0.1:1"
      });
      expectSuccess(result);
      const parsed = JSON.parse(result.stdout);
      expect(parsed.warnings).toEqual(["fake remote run"]);
      const sshArgs = await readFile(argsCapture, "utf8");
      expect(sshArgs).toContain("BatchMode=yes");
      expect(sshArgs).toContain("ConnectTimeout=8");
      expect(sshArgs).toContain("StrictHostKeyChecking=yes");
      expect(sshArgs).toContain(`UserKnownHostsFile=${join(configRoot, "ssh_known_hosts")}`);
      expect(sshArgs).toContain("operator@control.example");
      expect(sshArgs).toContain("/home/operator/brainstack");
      expect(sshArgs).toContain("$HOME/.local/bin/brainctl");
      expect(sshArgs).toContain("'\\''proposals'\\'' '\\''batch-merge'\\''");
      expect(sshArgs).toContain("'\\''--local'\\''");
      expect(sshArgs).toContain("'\\''--submit'\\''");
      expect(sshArgs).toContain("'\\''--limit'\\'' '\\''100'\\''");
      expect(sshArgs).toContain("'\\''--auto-threshold'\\'' '\\''0.8'\\''");
      expect(sshArgs).toContain("'\\''--json'\\''");
      expect(sshArgs).not.toContain("127.0.0.1:1");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 10_000);

  test("proposal merge discovery on a client fails closed without an explicit control route", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-proposal-merge-no-control-"));
    try {
      const configRoot = join(dir, "config");
      await mkdir(configRoot, { recursive: true });
      const configPath = join(dir, "client.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: mac-client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  configRoot: ${configRoot}`,
          "client:",
          "  remoteSsh: operator@control.example:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );

      const result = runBrainctl(["proposals", "batch-merge", "--submit", "--config", configPath], {
        BRAIN_BASE_URL: "http://127.0.0.1:1"
      });
      expect(result.code).not.toBe(0);
      expect(result.stderr).toContain("must run on the control host for client profiles");
      expect(result.stderr).toContain("client.telegramVia");
      expect(result.stderr).not.toContain("Could not resolve hostname");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 10_000);

  test("client remote control creates custom known-hosts parent for accept-new trust", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-control-accept-new-known-hosts-"));
    try {
      const binDir = join(dir, "bin");
      const configRoot = join(dir, "config");
      await mkdir(binDir, { recursive: true });
      await mkdir(configRoot, { recursive: true });
      const argsCapture = join(dir, "ssh-args.txt");
      const fakeSsh = join(binDir, "ssh");
      await writeFile(
        fakeSsh,
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          `printf '<%s>\\n' "$@" > '${argsCapture}'`,
          "echo 'proposal=proposal-1 action=reject status=rejected'",
          ""
        ].join("\n")
      );
      await chmod(fakeSsh, 0o755);

      const configPath = join(dir, "client.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "machine:",
          "  name: mac-client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  configRoot: ${configRoot}`,
          "client:",
          "  telegramVia: operator@control.example",
          "  telegramRemoteRepo: /home/operator/brainstack",
          ""
        ].join("\n")
      );

      const knownHostsPath = join(configRoot, "nested", "control_known_hosts");
      const result = runBrainctl(
        [
          "proposals",
          "reject",
          "proposal-1",
          "--config",
          configPath,
          "--known-hosts",
          knownHostsPath,
          "--ssh-trust",
          "accept-new"
        ],
        {
          PATH: `${binDir}:${process.env.PATH || ""}`,
          BRAIN_ADMIN_TOKEN: ""
        }
      );
      expectSuccess(result);
      expect(existsSync(dirname(knownHostsPath))).toBe(true);
      const sshArgs = await readFile(argsCapture, "utf8");
      expect(sshArgs).toContain("StrictHostKeyChecking=accept-new");
      expect(sshArgs).toContain(`UserKnownHostsFile=${knownHostsPath}`);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 10_000);

  test("rotate-token rejects non-owned token kinds", () => {
    const result = runBrainctl(["rotate-token", "--kind", "telegram-placeholder"]);
    expect(result.code).not.toBe(0);
    expect(`${result.stdout}\n${result.stderr}`).toContain("rotate-token requires --kind import|admin");
    expect(`${result.stdout}\n${result.stderr}`).not.toContain("BotFather");
  });

  test("telegram send-file rejects unsafe local files before opening SSH", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-telegram-send-file-unsafe-"));
    try {
      const binDir = join(dir, "bin");
      await mkdir(binDir, { recursive: true });
      const fakeSsh = join(binDir, "ssh");
      await writeFile(
        fakeSsh,
        [
          "#!/usr/bin/env bash",
          "echo should-not-run >&2",
          "exit 99",
          ""
        ].join("\n")
      );
      await chmod(fakeSsh, 0o755);
      const safePath = join(dir, "safe.txt");
      const tokenPath = join(dir, "secret-token.txt");
      const linkPath = join(dir, "safe-link.txt");
      await writeFile(safePath, "safe");
      await writeFile(tokenPath, "token");
      await symlink(safePath, linkPath);

      const env = { PATH: `${binDir}:${process.env.PATH || ""}` };
      const symlinkResult = runBrainctl(["telegram", "send-file", "--file", linkPath, "--via", "operator@control.example"], env);
      expect(symlinkResult.code).toBe(1);
      expect(symlinkResult.stderr).toContain("refusing to send symlink");
      expect(symlinkResult.stderr).not.toContain("should-not-run");

      const sensitiveResult = runBrainctl(["telegram", "send-file", "--file", tokenPath, "--via", "operator@control.example"], env);
      expect(sensitiveResult.code).toBe(1);
      expect(sensitiveResult.stderr).toContain("sensitive-looking file name");
      expect(sensitiveResult.stderr).not.toContain("should-not-run");

      const sensitiveDisplayResult = runBrainctl(
        ["telegram", "send-file", "--file", tokenPath, "--display-name", "notes.txt", "--via", "operator@control.example"],
        env
      );
      expect(sensitiveDisplayResult.code).toBe(1);
      expect(sensitiveDisplayResult.stderr).toContain("sensitive-looking file name");
      expect(sensitiveDisplayResult.stderr).not.toContain("should-not-run");

      const sizeResult = runBrainctl(["telegram", "send-file", "--file", safePath, "--via", "operator@control.example", "--max-bytes", "1"], env);
      expect(sizeResult.code).toBe(1);
      expect(sizeResult.stderr).toContain("file too large");
      expect(sizeResult.stderr).not.toContain("should-not-run");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 10_000);

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
      await writeFakeDoctorCodex(binDir);
      await writeFakeTailscale(binDir);
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
  }, 10_000);

  test("doctor rejects package-manager harness wrappers", async () => {
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
      expect(result.code).not.toBe(0);
      expect(`${result.stdout}\n${result.stderr}`).toContain("unstable package-manager wrapper");
      expect(`${result.stdout}\n${result.stderr}`).toContain("Configure harness.bin to a stable absolute codex binary");
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
  }, 20_000);

  test("invite create and enroll perform one-line client bootstrap without leaking the import token", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-invite-enroll-"));
    try {
      const home = join(dir, "home");
      const binDir = join(dir, "bin");
      const configRoot = join(home, ".config", "brainstack");
      await mkdir(binDir, { recursive: true });
      await mkdir(configRoot, { recursive: true });
      const tokenPath = join(dir, "brain-import-token.txt");
      await writeFile(tokenPath, "import-token-value\n");
      await chmod(tokenPath, 0o600);
      await mkdir(join(home, ".config"), { recursive: true });
      const clientEnvPath = join(home, ".config", "shared-brain.env");
      await writeFile(clientEnvPath, "BRAIN_IMPORT_TOKEN=\n");
      await chmod(clientEnvPath, 0o644);
      const knownHostsPath = join(configRoot, "ssh_known_hosts");
      await writeFile(
        knownHostsPath,
        [
          "control.example ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeBrainstackControlKeyForTestsOnly",
          "other-worker.example ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeOtherWorkerKeyForTestsOnly",
          ""
        ].join("\n")
      );

      await writeFile(
        join(binDir, "git"),
          [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "for arg in \"$@\"; do",
            "  if [ \"$arg\" = clone ]; then",
            "    target=\"${@: -1}\"",
            "    mkdir -p \"$target/.git\"",
            "    exit 0",
            "  fi",
            "done",
            "if [ \"${1:-}\" = --version ]; then echo 'git fake'; exit 0; fi",
            "exit 0",
            ""
        ].join("\n")
      );
      for (const name of ["ssh", "tailscale", "codex"]) {
        await writeFile(join(binDir, name), "#!/usr/bin/env sh\nif [ \"${1:-}\" = --version ]; then echo '" + name + " fake'; fi\nexit 0\n");
        await chmod(join(binDir, name), 0o755);
      }
      await chmod(join(binDir, "git"), 0o755);

      const controlConfig = join(dir, "control.yaml");
      await writeFile(
        controlConfig,
        [
          "schema_version: 1",
          "profile: control",
          "harness:",
          "  name: codex",
          "  bin: codex",
          "machine:",
          "  name: control",
          "  user: operator",
          "  sshUser: operator",
          "  hostname: control.example",
          "paths:",
          `  home: ${home}`,
          `  productRepo: ${join(home, "brainstack")}`,
          `  configRoot: ${configRoot}`,
          "brain:",
          "  publicBaseUrl: https://control.example.ts.net:8443/brain",
          "client:",
          "  remoteSsh: operator@control.example:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );

      const created = runBrainctl(
        [
          "invite",
          "create",
          "--config",
          controlConfig,
          "--import-token-file",
          tokenPath,
          "--ssh-known-hosts-file",
          knownHostsPath,
          "--skills-profile",
          "operator",
          "--install-url",
          "https://example.invalid/brainstack/install.sh",
          "--json"
        ],
        { HOME: home, USER: "operator", PATH: `${binDir}:${process.env.PATH || ""}`, BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1" }
      );
      expectSuccess(created);
      expect(created.stdout).not.toContain("import-token-value");
      const inviteResult = JSON.parse(created.stdout);
      // Token-bearing invites must not be printed to stdout by default; they land in
      // a 0600 file referenced by invitePath.
      expect(inviteResult.invite).toBeUndefined();
      expect(typeof inviteResult.invitePath).toBe("string");
      expect(created.stdout).not.toContain("bs1_");
      const invitePath = String(inviteResult.invitePath);
      expect(((await stat(invitePath)).mode & 0o777)).toBe(0o600);
      const inviteValue = (await readFile(invitePath, "utf8")).trim();
      expect(inviteValue.startsWith("bs1_")).toBe(true);
      expect(inviteResult.installCommand).toContain("https://example.invalid/brainstack/install.sh");
      expect(inviteResult.includesImportToken).toBe(true);
      expect(inviteResult.includesSshKnownHosts).toBe(true);
      expect(inviteResult.skillsProfile).toBe("operator");
      const decoded = JSON.parse(Buffer.from(inviteValue.slice("bs1_".length), "base64url").toString("utf8"));
      expect(decoded.importToken).toBe("import-token-value");
      expect(decoded.control.remoteRepo).toBe(join(home, "brainstack"));
      expect(decoded.control.sshTarget).toBe("operator@control.example");
      expect(decoded.control.sshKnownHosts).toEqual(["control.example ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFakeBrainstackControlKeyForTestsOnly"]);
      expect(decoded.skills.profile).toBe("operator");

      const clientConfig = join(dir, "client.yaml");
      const rawInviteRefused = runBrainctl(["enroll", "--invite", inviteValue, "--config", clientConfig, "--skip-doctor"], {
        HOME: home,
        USER: "operator",
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1"
      });
      expect(rawInviteRefused.code).not.toBe(0);
      expect(`${rawInviteRefused.stdout}\n${rawInviteRefused.stderr}`).toContain("shell history");

      const enrolled = runBrainctl(["enroll", "--invite-file", invitePath, "--config", clientConfig, "--skip-doctor"], {
        HOME: home,
        USER: "operator",
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1"
      });
      expectSuccess(enrolled);
      expect(enrolled.stdout).not.toContain("import-token-value");
      expect(enrolled.stderr).not.toContain("import-token-value");
      const configText = await readFile(clientConfig, "utf8");
      expect(configText).toContain("profile: client-macos");
      expect(configText).toContain("tailnetHost: control.example.ts.net");
      expect(configText).not.toContain("tailnetHost: control.example.ts.net:8443/brain");
      expect(configText).toContain("telegramVia: operator@control.example");
      expect(configText).toContain(`telegramRemoteRepo: ${join(home, "brainstack")}`);
      expect(await readFile(join(configRoot, "ssh_known_hosts"), "utf8")).toContain("control.example ssh-ed25519");
      expect(await readFile(clientEnvPath, "utf8")).toContain("BRAIN_IMPORT_TOKEN=import-token-value");
      expect((await stat(clientEnvPath)).mode & 0o777).toBe(0o600);
      expect((await readdir(configRoot)).filter((name) => name.includes(".invite-import-token")).length).toBe(0);
      expect((await stat(join(home, "shared-brain", ".git"))).isDirectory()).toBe(true);
      expect(await readFile(join(home, ".codex", "skills", "shared-brain-client", "SKILL.md"), "utf8")).toContain("Shared Brain Client");
      expect(await readFile(join(home, ".codex", "skills", "remote-machine-ops", "SKILL.md"), "utf8")).toContain("Remote Machine Ops");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("invite create does not embed import tokens unless explicitly requested", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-invite-no-token-"));
    try {
      const configPath = join(dir, "control.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: control",
          "  user: operator",
          "  sshUser: operator",
          "  hostname: control.example",
          "paths:",
          `  home: ${dir}`,
          "brain:",
          "  publicBaseUrl: https://control.example.ts.net",
          "client:",
          "  remoteSsh: operator@control.example:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const result = runBrainctl(["invite", "create", "--config", configPath, "--json"], {
        HOME: dir,
        USER: "operator",
        BRAIN_IMPORT_TOKEN: "must-not-be-embedded"
      });
      expectSuccess(result);
      const payload = JSON.parse(result.stdout);
      expect(payload.includesImportToken).toBe(false);
      expect(payload.installCommand).toContain("https://github.com/Caimeo-com/brainstack/releases/download/v0.1.0/install.sh");
      expect(payload.installCommand).not.toContain("/releases/latest/");
      const decoded = JSON.parse(Buffer.from(String(payload.invite).slice("bs1_".length), "base64url").toString("utf8"));
      expect(decoded.importToken).toBeUndefined();
      expect(result.stdout).not.toContain("must-not-be-embedded");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("invite create keeps latest explicit and rejects unsafe install URL selectors", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-invite-install-version-"));
    try {
      const configPath = join(dir, "control.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: control",
          "  user: operator",
          "  sshUser: operator",
          "  hostname: control.example",
          "paths:",
          `  home: ${dir}`,
          "brain:",
          "  publicBaseUrl: https://control.example.ts.net",
          "client:",
          "  remoteSsh: operator@control.example:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const latest = runBrainctl(["invite", "create", "--config", configPath, "--install-version", "latest"], {
        HOME: dir,
        USER: "operator"
      });
      expectSuccess(latest);
      expect(latest.stdout).toContain("https://github.com/Caimeo-com/brainstack/releases/latest/download/install.sh");
      expect(latest.stdout).toContain("WARN install URL uses the moving 'latest' release");

      const conflict = runBrainctl(
        [
          "invite",
          "create",
          "--config",
          configPath,
          "--install-version",
          "0.1.0",
          "--install-url",
          "https://example.invalid/install.sh"
        ],
        {
          HOME: dir,
          USER: "operator"
        }
      );
      expect(conflict.code).not.toBe(0);
      expect(`${conflict.stdout}\n${conflict.stderr}`).toContain("either --install-url or --install-version");

      const unsafeUrl = runBrainctl(["invite", "create", "--config", configPath, "--install-url", "http://example.invalid/install.sh"], {
        HOME: dir,
        USER: "operator"
      });
      expect(unsafeUrl.code).not.toBe(0);
      expect(`${unsafeUrl.stdout}\n${unsafeUrl.stderr}`).toContain("--install-url must use https");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("invite create fails loudly for explicit missing SSH host pins", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-invite-missing-known-hosts-"));
    try {
      const configPath = join(dir, "control.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: control",
          "  user: operator",
          "  sshUser: operator",
          "  hostname: control.example",
          "paths:",
          `  home: ${dir}`,
          "brain:",
          "  publicBaseUrl: https://control.example.ts.net",
          "client:",
          "  remoteSsh: operator@control.example:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const result = runBrainctl(["invite", "create", "--config", configPath, "--ssh-known-hosts-file", join(dir, "missing_known_hosts"), "--json"], {
        HOME: dir,
        USER: "operator"
      });
      expect(result.code).not.toBe(0);
      expect(`${result.stdout}\n${result.stderr}`).toContain("ssh known-hosts invite source not found");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("invite create filters known-host pins to the selected control SSH target", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-invite-known-host-filter-"));
    try {
      const configPath = join(dir, "control.yaml");
      const knownHostsPath = join(dir, "known_hosts");
      await writeFile(
        knownHostsPath,
        [
          "unrelated.example ssh-ed25519 AAAAUnrelatedKeyForTestsOnly",
          "[control.example]:2222 ssh-ed25519 AAAAControlKeyForTestsOnly",
          ""
        ].join("\n")
      );
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: control",
          "  user: operator",
          "  sshUser: operator",
          "  hostname: control.example",
          "paths:",
          `  home: ${dir}`,
          "brain:",
          "  publicBaseUrl: https://control.example.ts.net",
          "client:",
          "  remoteSsh: operator@control.example:2222:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );
      const result = runBrainctl(["invite", "create", "--config", configPath, "--ssh-known-hosts-file", knownHostsPath, "--json"], {
        HOME: dir,
        USER: "operator"
      });
      expectSuccess(result);
      const payload = JSON.parse(result.stdout);
      const decoded = JSON.parse(Buffer.from(String(payload.invite).slice("bs1_".length), "base64url").toString("utf8"));
      expect(decoded.control.sshTarget).toBe("operator@control.example:2222");
      expect(decoded.control.sshKnownHosts).toEqual(["[control.example]:2222 ssh-ed25519 AAAAControlKeyForTestsOnly"]);

      const noMatch = runBrainctl(["invite", "create", "--config", configPath, "--control-ssh", "operator@missing.example", "--ssh-known-hosts-file", knownHostsPath, "--json"], {
        HOME: dir,
        USER: "operator"
      });
      expect(noMatch.code).not.toBe(0);
      expect(`${noMatch.stdout}\n${noMatch.stderr}`).toContain("has no entry for missing.example");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("enroll rejects conflicting SSH host pins from invites", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-invite-conflicting-known-hosts-"));
    try {
      const home = join(dir, "home");
      const binDir = join(dir, "bin");
      const configRoot = join(home, ".config", "brainstack");
      await mkdir(binDir, { recursive: true });
      await mkdir(configRoot, { recursive: true });
      for (const name of ["git", "ssh", "tailscale", "codex"]) {
        await writeFile(join(binDir, name), "#!/usr/bin/env sh\nexit 0\n");
        await chmod(join(binDir, name), 0o755);
      }
      await writeFile(join(configRoot, "ssh_known_hosts"), "control.example ssh-ed25519 AAAAExistingDifferentKey\n");
      const invitePayload = {
        schema_version: 1,
        type: "brainstack-client-invite",
        created_at: new Date().toISOString(),
        expires_at: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
        profile: "client-macos",
        brain: {
          publicBaseUrl: "https://control.example.ts.net",
          remoteSsh: "operator@control.example:/home/operator/shared-brain/bare/shared-brain.git"
        },
        control: {
          sshTarget: "operator@control.example",
          remoteRepo: "/home/operator/brainstack",
          sshKnownHosts: ["control.example ssh-ed25519 AAAAInviteDifferentKey"]
        },
        client: {
          localPath: "~/shared-brain",
          envPath: "~/.config/shared-brain.env"
        },
        harness: {
          name: "codex",
          bin: "codex"
        }
      };
      const invite = `bs1_${Buffer.from(JSON.stringify(invitePayload)).toString("base64url")}`;
      const result = runBrainctl(["enroll", "--invite", invite, "--allow-unsafe-invite", "--config", join(dir, "client.yaml"), "--skip-init", "--skip-doctor"], {
        HOME: home,
        USER: "operator",
        PATH: `${binDir}:${process.env.PATH || ""}`,
        BRAINSTACK_SKIP_USER_PATH_RESOLVE: "1"
      });
      expect(result.code).not.toBe(0);
      expect(`${result.stdout}\n${result.stderr}`).toContain("invite SSH host pin conflicts");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("enroll rejects option-shaped control SSH targets from invites", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-invite-unsafe-ssh-target-"));
    try {
      const invitePayload = {
        schema_version: 1,
        type: "brainstack-client-invite",
        created_at: new Date().toISOString(),
        expires_at: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
        profile: "client-macos",
        brain: {
          publicBaseUrl: "https://control.example.ts.net",
          remoteSsh: "operator@control.example:/home/operator/shared-brain/bare/shared-brain.git"
        },
        control: {
          sshTarget: "-oProxyCommand=touch /tmp/nope",
          remoteRepo: "/home/operator/brainstack",
          sshKnownHosts: []
        },
        client: {
          localPath: "~/shared-brain",
          envPath: "~/.config/shared-brain.env"
        },
        harness: {
          name: "codex",
          bin: "codex"
        }
      };
      const invite = `bs1_${Buffer.from(JSON.stringify(invitePayload)).toString("base64url")}`;
      const result = runBrainctl(["enroll", "--invite", invite, "--allow-unsafe-invite", "--config", join(dir, "client.yaml"), "--skip-init", "--skip-doctor"], {
        HOME: dir,
        USER: "operator"
      });
      expect(result.code).not.toBe(0);
      expect(`${result.stdout}\n${result.stderr}`).toContain("safe bare SSH host");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("enroll rejects unsafe invite git remotes and local paths", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-invite-unsafe-remote-"));
    try {
      const baseInvitePayload = {
        schema_version: 1,
        type: "brainstack-client-invite",
        created_at: new Date().toISOString(),
        expires_at: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
        profile: "client-macos",
        brain: {
          publicBaseUrl: "https://control.example.ts.net",
          remoteSsh: "operator@control.example:/home/operator/shared-brain/bare/shared-brain.git"
        },
        control: {
          sshTarget: "operator@control.example",
          remoteRepo: "/home/operator/brainstack",
          sshKnownHosts: []
        },
        client: {
          localPath: "~/shared-brain",
          envPath: "~/.config/shared-brain.env"
        },
        harness: {
          name: "codex",
          bin: "codex"
        }
      };
      const encodeInvite = (payload: unknown) => `bs1_${Buffer.from(JSON.stringify(payload)).toString("base64url")}`;

      const unsafeRemote = runBrainctl(
        [
          "enroll",
          "--invite",
          encodeInvite({
            ...baseInvitePayload,
            brain: {
              ...baseInvitePayload.brain,
              remoteSsh: "ext::sh -c 'touch /tmp/brainstack-poc'"
            }
          }),
          "--allow-unsafe-invite",
          "--config",
          join(dir, "client-unsafe-remote.yaml"),
          "--skip-init",
          "--skip-doctor"
        ],
        { HOME: dir, USER: "operator" }
      );
      expect(unsafeRemote.code).not.toBe(0);
      expect(`${unsafeRemote.stdout}\n${unsafeRemote.stderr}`).toContain("invite brain.remoteSsh");

      const fileRemote = runBrainctl(
        [
          "enroll",
          "--invite",
          encodeInvite({
            ...baseInvitePayload,
            brain: {
              ...baseInvitePayload.brain,
              remoteSsh: `file://${join(dir, "attacker.git")}`
            }
          }),
          "--allow-unsafe-invite",
          "--config",
          join(dir, "client-file-remote.yaml"),
          "--skip-init",
          "--skip-doctor"
        ],
        { HOME: dir, USER: "operator" }
      );
      expect(fileRemote.code).not.toBe(0);
      expect(`${fileRemote.stdout}\n${fileRemote.stderr}`).toContain("invite brain.remoteSsh");

      const optionPathRemote = runBrainctl(
        [
          "enroll",
          "--invite",
          encodeInvite({
            ...baseInvitePayload,
            brain: {
              ...baseInvitePayload.brain,
              remoteSsh: "operator@control.example:-upload-pack=sh"
            }
          }),
          "--allow-unsafe-invite",
          "--config",
          join(dir, "client-option-path-remote.yaml"),
          "--skip-init",
          "--skip-doctor"
        ],
        { HOME: dir, USER: "operator" }
      );
      expect(optionPathRemote.code).not.toBe(0);
      expect(`${optionPathRemote.stdout}\n${optionPathRemote.stderr}`).toContain("invite brain.remoteSsh");

      const traversalRemote = runBrainctl(
        [
          "enroll",
          "--invite",
          encodeInvite({
            ...baseInvitePayload,
            brain: {
              ...baseInvitePayload.brain,
              remoteSsh: "operator@control.example:../shared-brain.git"
            }
          }),
          "--allow-unsafe-invite",
          "--config",
          join(dir, "client-traversal-remote.yaml"),
          "--skip-init",
          "--skip-doctor"
        ],
        { HOME: dir, USER: "operator" }
      );
      expect(traversalRemote.code).not.toBe(0);
      expect(`${traversalRemote.stdout}\n${traversalRemote.stderr}`).toContain("invite brain.remoteSsh");

      const unsafePath = runBrainctl(
        [
          "enroll",
          "--invite",
          encodeInvite({
            ...baseInvitePayload,
            client: {
              ...baseInvitePayload.client,
              localPath: "../shared-brain"
            }
          }),
          "--allow-unsafe-invite",
          "--config",
          join(dir, "client-unsafe-path.yaml"),
          "--skip-init",
          "--skip-doctor"
        ],
        { HOME: dir, USER: "operator" }
      );
      expect(unsafePath.code).not.toBe(0);
      expect(`${unsafePath.stdout}\n${unsafePath.stderr}`).toContain("invite client.localPath");
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
    const releaseWorkflow = await readFile(join(PRODUCT_ROOT, ".github", "workflows", "release.yml"), "utf8");
    const installDoc = await readFile(join(PRODUCT_ROOT, "docs", "install-one-line.md"), "utf8");
    const makeApp = await readFile(join(PRODUCT_ROOT, "apps", "brainstack-menu", "scripts", "make-app.sh"), "utf8");
    const appModel = await readFile(join(PRODUCT_ROOT, "apps", "brainstack-menu", "Sources", "BrainstackMenu", "AppModel.swift"), "utf8");
    const brainctlSrc = join(PRODUCT_ROOT, "packages", "brainctl", "src");
    const commandFiles = (await readdir(join(brainctlSrc, "commands"))).filter((name) => name.endsWith(".ts")).sort();
    const brainctlSource = [
      await readFile(join(brainctlSrc, "main.ts"), "utf8"),
      ...(await Promise.all(commandFiles.map((name) => readFile(join(brainctlSrc, "commands", name), "utf8"))))
    ].join("\n");
    for (const text of [packageJson, releaseScript]) {
      expect(text).toContain("--no-compile-autoload-dotenv");
      expect(text).toContain("--no-compile-autoload-bunfig");
    }
    expect(releaseScript).toContain("darwin-arm64");
    expect(releaseScript).toContain("darwin-x64");
    expect(releaseScript).toContain("dist/install.sh");
    expect(releaseScript).toContain("manifest.json");
    expect(brainctlSource).toContain('brainctl_tmp=\\"$HOME/.local/bin/.brainctl.$$\\"');
    expect(brainctlSource).toContain('--outfile \\"$brainctl_tmp\\"');
    expect(brainctlSource).toContain('mv -f \\"$brainctl_tmp\\" \\"$brainctl_bin\\"');
    expect(brainctlSource).not.toContain('--outfile \\"$brainctl_bin\\"');
    const installScript = await readFile(join(PRODUCT_ROOT, "scripts", "install.sh"), "utf8");
    expect(installScript).toContain("sha256sum");
    expect(installScript).toContain("shasum -a 256");
    expect(installScript).toContain("openssl dgst -sha256");
    expect(installScript).toContain("--invite-file");
    expect(installScript).toContain("--skills-profile");
    expect(installScript).toContain("--skip-skills");
    expect(installScript).toContain("refusing non-HTTPS download URL");
    expect(installScript).toContain("--allow-unsafe-invite");
    expect(installScript).toContain("raw invites in argv/env can leak");
    expect(installScript).toContain("--invite-file - is not supported");
    expect(releaseScript).toContain("BRAINSTACK_RELEASE_INSTALL_VERSION");
    expect(releaseScript).toContain("BRAINSTACK_VERSION:-$default_install_version");
    expect(releaseWorkflow).toContain("cli-release:");
    expect(releaseWorkflow).toContain('BRAINSTACK_RELEASE_MENU_APP: "0"');
    expect(releaseWorkflow).toContain("menu-app-release:");
    expect(releaseWorkflow).toContain("include_menu_app");
    expect(releaseWorkflow).toContain("must match package.json version");
    expect(releaseWorkflow).toContain("Guard privileged release ref");
    expect(releaseWorkflow).toContain("manual release publishing/signing must run from");
    expect(releaseWorkflow).toContain("Verify release provenance");
    expect(releaseWorkflow).toContain("release tag commit must be reachable from origin/$DEFAULT_BRANCH");
    expect(releaseWorkflow).toContain("manual release commit must be reachable from origin/$DEFAULT_BRANCH");
    expect(releaseWorkflow).toMatch(/permissions:\n  contents: read/);
    expect(releaseWorkflow).toMatch(/publish-release:[\s\S]*permissions:\n      contents: write/);
    expect(releaseWorkflow).toContain("persist-credentials: false");
    expect(releaseWorkflow).toContain("actions/checkout@v6");
    expect(releaseWorkflow).toContain("actions/download-artifact@v4");
    expect(releaseWorkflow).not.toContain("actions/checkout@v4");
    expect(releaseWorkflow).not.toContain("node -e");
    expect(releaseWorkflow).toContain("sed -n");
    expect(releaseWorkflow).toContain("github.event_name == 'push'");
    expect(releaseWorkflow).toContain("publish-release:");
    expect(releaseWorkflow).toContain("needs.menu-app-release.outputs.built");
    expect(releaseWorkflow).toContain("menu app release was explicitly requested");
    expect(releaseWorkflow).toContain("swift build -c release --arch arm64 --arch x86_64");
    expect(releaseWorkflow).not.toContain("security import \"$cert_path\" -P \"$MACOS_DEVELOPER_ID_CERT_PASSWORD\" -A");
    expect(releaseScript).toContain('BRAINSTACK_MENU_ARCHES="${BRAINSTACK_MENU_ARCHES:-arm64 x64}"');
    expect(makeApp).toContain("Contents/Resources/brainctl");
    expect(makeApp).toContain("--skip-build");
    expect(makeApp).toContain('lipo "$path" -verify_arch');
    expect(makeApp).toContain("BRAINSTACK_MENU_BRAINCTL_SOURCE");
    expect(makeApp).toContain('codesign --force --options runtime --timestamp --sign "$IDENTITY" "$BUNDLED_BRAINCTL"');
    expect(makeApp).toContain("lipo -create");
    expect(installDoc).not.toContain("--invite-file FILE|-");
    expect(installDoc).toContain("Debian/Ubuntu");
    expect(installDoc).toContain("Arch/Omarchy");
    expect(installDoc).toContain("does not require Bun or a Brainstack source checkout");
    expect(appModel).not.toContain("releases/latest/download/install.sh");
    expect(appModel).toContain("AppVersion.current");
  });

  test("install script verifies local release checksum before installing", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainstack-install-script-smoke-"));
    try {
      const releaseDir = join(dir, "release");
      const binDir = join(dir, "bin");
      await mkdir(releaseDir, { recursive: true });
      const os = process.platform === "darwin" ? "darwin" : process.platform === "linux" ? "linux" : "unsupported";
      const arch = process.arch === "arm64" ? "arm64" : "x64";
      if (os === "unsupported") {
        return;
      }
      const asset = `brainctl-${os}-${arch}`;
      const assetBody = "#!/usr/bin/env sh\necho fake brainctl\n";
      await writeFile(join(releaseDir, asset), assetBody);
      await writeFile(join(releaseDir, `${asset}.sha256`), `${sha256Text(assetBody)}  ${asset}\n`);

      const installed = runCommand(
        ["sh", "scripts/install.sh", "--base-url", `file://${releaseDir}`, "--bin-dir", binDir, "--skip-enroll"],
        {
          cwd: PRODUCT_ROOT,
          env: {
            BRAINSTACK_INSTALL_ALLOW_INSECURE: "1",
            HOME: join(dir, "home")
          }
        }
      );
      expectSuccess(installed);
      expect(await readFile(join(binDir, "brainctl"), "utf8")).toBe(assetBody);

      await writeFile(join(releaseDir, `${asset}.sha256`), `${"0".repeat(64)}  ${asset}\n`);
      const mismatch = runCommand(
        ["sh", "scripts/install.sh", "--base-url", `file://${releaseDir}`, "--bin-dir", join(dir, "bad-bin"), "--skip-enroll"],
        {
          cwd: PRODUCT_ROOT,
          env: {
            BRAINSTACK_INSTALL_ALLOW_INSECURE: "1",
            HOME: join(dir, "home")
          }
        }
      );
      expect(mismatch.code).not.toBe(0);
      expect(`${mismatch.stdout}\n${mismatch.stderr}`).toContain("checksum mismatch");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("install script uses explicit GitHub release version for binary downloads", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainstack-install-script-version-url-"));
    try {
      const binDir = join(dir, "bin");
      const curlLog = join(dir, "curl.log");
      const fakeBin = join(dir, "fake-bin");
      await mkdir(fakeBin, { recursive: true });
      const os = process.platform === "darwin" ? "darwin" : process.platform === "linux" ? "linux" : "unsupported";
      const arch = process.arch === "arm64" ? "arm64" : "x64";
      if (os === "unsupported") {
        return;
      }
      const asset = `brainctl-${os}-${arch}`;
      const assetBody = "#!/usr/bin/env sh\necho fake brainctl\n";
      const assetHash = sha256Text(assetBody);
      await writeFile(
        join(fakeBin, "curl"),
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          "out=''",
          "url=''",
          "while [ $# -gt 0 ]; do",
          "  case \"$1\" in",
          "    -o) out=\"$2\"; shift 2 ;;",
          "    http*) url=\"$1\"; shift ;;",
          "    *) shift ;;",
          "  esac",
          "done",
          `printf '%s\\n' "$url" >> ${shellQuote(curlLog)}`,
          "case \"$url\" in",
          `  *.sha256) printf '%s  %s\\n' ${shellQuote(assetHash)} ${shellQuote(asset)} > "$out" ;;`,
          `  *) printf '%s' ${shellQuote(assetBody)} > "$out" ;;`,
          "esac",
          ""
        ].join("\n")
      );
      await chmod(join(fakeBin, "curl"), 0o755);
      const installed = runCommand(["sh", "scripts/install.sh", "--version", "v1.2.3", "--bin-dir", binDir, "--skip-enroll"], {
        cwd: PRODUCT_ROOT,
        env: {
          HOME: join(dir, "home"),
          PATH: `${fakeBin}:${process.env.PATH || ""}`
        }
      });
      expectSuccess(installed);
      const urls = await readFile(curlLog, "utf8");
      expect(urls).toContain(`https://github.com/Caimeo-com/brainstack/releases/download/v1.2.3/${asset}`);
      expect(urls).toContain(`https://github.com/Caimeo-com/brainstack/releases/download/v1.2.3/${asset}.sha256`);
      expect(urls).not.toContain("/releases/latest/");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("install script rejects stdin invite before download", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainstack-install-script-stdin-invite-"));
    try {
      const refused = runCommand(["sh", "scripts/install.sh", "--invite-file", "-", "--bin-dir", join(dir, "bin")], {
        cwd: PRODUCT_ROOT,
        env: {
          HOME: join(dir, "home")
        }
      });
      expect(refused.code).toBe(2);
      expect(`${refused.stdout}\n${refused.stderr}`).toContain("--invite-file - is not supported");
      expect(`${refused.stdout}\n${refused.stderr}`).toContain("stdin");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("nested telemux bun lockfile is gone", async () => {
    expect(await Bun.file(join(PRODUCT_ROOT, "apps", "telemux", "bun.lock")).exists()).toBe(false);
    expect(await Bun.file(join(PRODUCT_ROOT, "bun.lock")).exists()).toBe(true);
  });
});
