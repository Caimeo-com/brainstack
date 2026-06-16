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

describe("public release hygiene - bootstrap and local install surfaces", () => {
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
      expect(scriptText).toContain("--allow-dirty");
      expect(scriptText).toContain("find \"$bundle_dir\" -type l");
    } finally {
      await rm(dir, { recursive: true, force: true });
      if (sentinel) {
        await rm(sentinel, { force: true });
      }
    }
  });

  test("handoff script times out stuck proof commands with visible progress", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainstack-handoff-timeout-"));
    try {
      const binDir = join(dir, "bin");
      await mkdir(join(dir, "scripts"), { recursive: true });
      await mkdir(binDir, { recursive: true });
      await writeFile(join(dir, "scripts", "handoff.sh"), await readFile(join(PRODUCT_ROOT, "scripts", "handoff.sh"), "utf8"));
      await chmod(join(dir, "scripts", "handoff.sh"), 0o755);
      await writeFile(
        join(binDir, "bun"),
        [
          "#!/usr/bin/env bash",
          "set -eu",
          "if [ \"${1:-}\" = run ]; then exec sleep 5; fi",
          "if [ \"${1:-}\" = test ]; then exec sleep 5; fi",
          "echo 'unexpected fake bun invocation' >&2",
          "exit 1",
          ""
        ].join("\n")
      );
      await chmod(join(binDir, "bun"), 0o755);
      expectSuccess(runCommand(["git", "init"], { cwd: dir }));
      expectSuccess(runCommand(["git", "config", "user.email", "test@example.invalid"], { cwd: dir }));
      expectSuccess(runCommand(["git", "config", "user.name", "Test"], { cwd: dir }));
      expectSuccess(runCommand(["git", "add", "."], { cwd: dir }));
      expectSuccess(runCommand(["git", "commit", "-m", "fixture"], { cwd: dir }));

      const out = join(dir, "out");
      await mkdir(out, { recursive: true });
      const result = runCommand(["bash", "scripts/handoff.sh", "--out", out], {
        cwd: dir,
        env: {
          PATH: `${binDir}:${process.env.PATH || ""}`,
          BRAINSTACK_HANDOFF_UTC: "20260525T000100Z",
          BRAINSTACK_HANDOFF_STEP_TIMEOUT_SECONDS: "1",
          BRAINSTACK_HANDOFF_PROGRESS_INTERVAL_SECONDS: "1"
        }
      });
      expect(result.code).toBe(124);
      expect(result.stderr).toContain("timeout after 1s: brainctl bootstrap-client custom path");
      const proofOutput = await readFile(join(out, "handoff-20260525T000100Z", "command-outputs", "bootstrap-client-custom-path.txt"), "utf8");
      expect(proofOutput).toContain("handoff command timed out after 1s");
      expect(proofOutput).toContain("bootstrap-client");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 20_000);

  test("handoff script allow-dirty snapshots tracked and untracked worktree files", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainstack-handoff-dirty-fixture-"));
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
          "case \" $* \" in *brainctl*'handoff cross-brain blocked example'*) echo 'without --confirm-cross-brain' >&2; exit 1 ;; esac",
          "if [ \"${1:-}\" = run ]; then",
          "  shift",
          "  case \"${1:-}\" in",
          "    build:brainctl) echo 'build ok'; exit 0 ;;",
          "    *.ts) echo 'server fake'; sleep 0.05; exit 0 ;;",
          "    *brainctl*) echo \"brainctl fake $*\"; exit 0 ;;",
          "  esac",
          "fi",
          "echo \"bun fake $*\"",
          ""
        ].join("\n")
      );
      await chmod(join(binDir, "bun"), 0o755);
      await writeFile(join(binDir, "curl"), "#!/usr/bin/env bash\ncase \" $* \" in *127.0.0.1:*'/health'*) exit 0 ;; *) exit 22 ;; esac\n");
      await chmod(join(binDir, "curl"), 0o755);
      await writeFile(join(dir, "package.json"), "{\"scripts\":{\"build:brainctl\":\"echo build\"}}\n");
      await writeFile(join(dir, "README.md"), "Clean README\n");
      await writeFile(join(dir, "docs", "security-postures.md"), "It defaults to a trusted private mesh, does not require read tokens, and says: Do not expose trusted-tailnet mode to the public internet.\n");
      await writeFile(join(dir, "docs", "tailscale-exposure.md"), "Use brainctl expose tailscale with tailscale serve.\n");
      await writeFile(join(dir, "docs", "multi-brain.md"), "project config uses .brainstack.yaml and profiles.yaml. Repo-local URLs, remotes, token environment names, and local clone paths require user trust and may be pending-trust. Sections are not hard security boundaries; they are retrieval boundaries.\n");
      await writeFile(join(dir, "docs", "outbox-security.md"), "Outbox payloads are sensitive plaintext by default. No queued payload is silently truncated. Future server-sealed mode is documented.\n");
      await writeFile(join(dir, "docs", "diagrams.md"), "```mermaid\ngraph TD\nA-->B\n```\n");
      await writeFile(join(dir, "packages", "client-bootstrap", "claude-user-CLAUDE.md"), "Use brainctl search --repo . and brainctl remember --repo .\n");
      expectSuccess(runCommand(["git", "init"], { cwd: dir }));
      expectSuccess(runCommand(["git", "config", "user.email", "test@example.invalid"], { cwd: dir }));
      expectSuccess(runCommand(["git", "config", "user.name", "Test"], { cwd: dir }));
      expectSuccess(runCommand(["git", "add", "."], { cwd: dir }));
      expectSuccess(runCommand(["git", "commit", "-m", "fixture"], { cwd: dir }));

      await writeFile(join(dir, "README.md"), "Dirty README\nBrainstack runs on trusted private networks.\n");
      await writeFile(join(dir, "docs", "extra-proof.md"), "Untracked proof file\n");

      const refused = runCommand(["bash", "scripts/handoff.sh", "--out", join(dir, "refused-out")], {
        cwd: dir,
        env: {
          PATH: `${binDir}:${process.env.PATH || ""}`,
          BRAINSTACK_HANDOFF_UTC: "20260525T000200Z"
        }
      });
      expect(refused.code).not.toBe(0);
      expect(refused.stderr).toContain("git tree is dirty");
      expect(refused.stderr).toContain("--allow-dirty");

      const out = join(dir, "out");
      await mkdir(out, { recursive: true });
      const allowed = runCommand(["bash", "scripts/handoff.sh", "--allow-dirty", "--out", out], {
        cwd: dir,
        env: {
          PATH: `${binDir}:${process.env.PATH || ""}`,
          BRAINSTACK_HANDOFF_UTC: "20260525T000201Z"
        }
      });
      expectSuccess(allowed);
      const zipPath = join(out, "handoff-20260525T000201Z.zip");
      const readme = runCommand(["unzip", "-p", zipPath, "handoff-20260525T000201Z/source/README.md"]);
      expectSuccess(readme);
      expect(readme.stdout).toContain("Dirty README");
      const untracked = runCommand(["unzip", "-p", zipPath, "handoff-20260525T000201Z/source/docs/extra-proof.md"]);
      expectSuccess(untracked);
      expect(untracked.stdout).toBe("Untracked proof file\n");
      const manifest = runCommand(["unzip", "-p", zipPath, "handoff-20260525T000201Z/MANIFEST.txt"]);
      expectSuccess(manifest);
      expect(manifest.stdout).toContain("Allow dirty tree: yes");
      expect(manifest.stdout).toContain("Dirty tree included: yes");
      expect(manifest.stdout).toContain("working-tree snapshot");
      const changes = runCommand(["unzip", "-p", zipPath, "handoff-20260525T000201Z/CHANGES.txt"]);
      expectSuccess(changes);
      expect(changes.stdout).toContain("Dirty Working Tree");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 30_000);

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
          "case \" $* \" in *brainctl*'handoff cross-brain blocked example'*) echo 'without --confirm-cross-brain' >&2; exit 1 ;; esac",
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
          "      case \" $* \" in *'handoff cross-brain blocked example'*) echo 'without --confirm-cross-brain' >&2; exit 1 ;; esac",
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
      await writeFile(
        join(binDir, "curl"),
        [
          "#!/usr/bin/env bash",
          "set -eu",
          "case \" $* \" in",
          "  *127.0.0.1:*'/health'*) exit 0 ;;",
          "  *) exit 22 ;;",
          "esac",
          ""
        ].join("\n")
      );
      await chmod(join(binDir, "curl"), 0o755);
      await writeFile(join(dir, "package.json"), "{\"scripts\":{\"build:brainctl\":\"echo build\"}}\n");
      await writeFile(join(dir, "README.md"), "Brainstack runs on trusted private networks.\n");
      await writeFile(join(dir, "docs", "security-postures.md"), "It defaults to a trusted private mesh, does not require read tokens, and says: Do not expose trusted-tailnet mode to the public internet.\n");
      await writeFile(join(dir, "docs", "tailscale-exposure.md"), "Use brainctl expose tailscale with tailscale serve.\n");
      await writeFile(join(dir, "docs", "multi-brain.md"), "project config uses .brainstack.yaml and profiles.yaml. Repo-local URLs, remotes, token environment names, and local clone paths require user trust and may be pending-trust. Sections are not hard security boundaries; they are retrieval boundaries.\n");
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
      expect(installScript).toContain("git clone -- \"$REMOTE\" \"$TARGET_ABS\"");
      expect(installScript).toContain("validate_remote");
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
      expect(runtimeEnv).toContain("FACTORY_TEXT_COALESCE_RECOVERY_MAX_AGE_MS=300000");
      expect(runtimeEnv).toContain("FACTORY_PRE_DISPATCH_CLASSIFIER=0");
      expect(runtimeEnv).toContain("FACTORY_PRE_DISPATCH_CLASSIFIER_REASONING_EFFORT=minimal");
      expect(runtimeEnv).toContain("FACTORY_PRE_DISPATCH_CLASSIFIER_TIMEOUT_MS=800");
      const secretsEnv = await readFile(join(out, "env", "telemux.secrets.env.example"), "utf8");
      expect(secretsEnv).toContain("FACTORY_PRE_DISPATCH_CLASSIFIER_API_KEY=");
      const telemuxService = await readFile(join(out, "systemd", "user", "telemux.service"), "utf8");
      expect(telemuxService).toContain("EnvironmentFile=/home/operator/.config/brainstack/telemux.secrets.env");
      expect(telemuxService).toContain("EnvironmentFile=/home/operator/.config/brainstack/braind.secrets.env");
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
      const binDir = join(dir, "bin");
      await mkdir(binDir, { recursive: true });
      await writeFakeDoctorCodex(binDir);
      await writeFakeTailscale(binDir);
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
      const ok = runBrainctl(["doctor", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expectSuccess(ok);
      expect(ok.stdout).toContain("read-auth: disabled by design in trusted-tailnet mode");
      expect(ok.stdout).toContain("trust-boundary: private network reachability");
      expect(ok.stdout).toContain("do not expose trusted-tailnet mode to the public internet");

      await writeFile(configPath, base.replace("bindHost: 127.0.0.1", "bindHost: 192.168.1.10").replace("trustedExposure: vpn", "trustedExposure: none"));
      const badBind = runBrainctl(["doctor", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expect(badBind.code).not.toBe(0);
      expect(badBind.stdout).toContain("FAIL [security] posture: trusted-tailnet bind=192.168.1.10");

      await writeFile(configPath, base.replace("bindHost: 127.0.0.1", "bindHost: 0.0.0.0").replace("trustedExposure: vpn", "trustedExposure: manual"));
      const wildcardManual = runBrainctl(["doctor", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expect(wildcardManual.code).not.toBe(0);
      expect(wildcardManual.stdout).toContain("FAIL [security] posture: trusted-tailnet bind=0.0.0.0; exposure=manual");
      expect(wildcardManual.stdout).toContain("Wildcard trusted-tailnet binds are too broad");

      await writeFile(configPath, base.replace("trustedExposure: vpn", "trustedExposure: public"));
      const badExposure = runBrainctl(["doctor", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expect(badExposure.code).not.toBe(0);
      expect(badExposure.stderr).toContain("Expected none, tailscale-serve, vpn, or manual");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  }, 30_000);

  test("doctor skips missing guidance for unused harnesses", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-guidance-doctor-"));
    try {
      const binDir = join(dir, "bin");
      await mkdir(binDir, { recursive: true });
      await writeFakeDoctorCodex(binDir);
      await writeFakeTailscale(binDir);
      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: client-macos",
          "harness:",
          "  name: codex",
          "machine:",
          "  name: client",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          "client:",
          "  remoteSsh: operator@brain-control:/home/operator/shared-brain/bare/shared-brain.git",
          ""
        ].join("\n")
      );

      const missingClaude = runBrainctl(["doctor", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expectSuccess(missingClaude);
      expect(missingClaude.stdout).toContain("WARN [guidance] codex-agents:");
      expect(missingClaude.stdout).not.toContain("[guidance] claude");

      await mkdir(join(dir, ".claude"), { recursive: true });
      await writeFile(join(dir, ".claude", "CLAUDE.md"), "# Existing Claude\n");
      const existingClaude = runBrainctl(["doctor", "--config", configPath], {
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expectSuccess(existingClaude);
      expect(existingClaude.stdout).toContain("WARN [guidance] claude:");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

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
  }, 30_000);

});
