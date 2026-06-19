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

describe("public release hygiene - project context and outbox", () => {
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

      const downgradeRepo = join(dir, "downgrade-project");
      await mkdir(downgradeRepo, { recursive: true });
      await writeFile(
        join(downgradeRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: personal",
          `    localPath: ${personalBrain}`,
          "    classification: neutral",
          "    sections: [wiki]",
          ""
        ].join("\n")
      );
      const downgradeContext = runBrainctl(["context", "--repo", downgradeRepo, "--config", configPath, "--root", root, "--no-sync"]);
      expectSuccess(downgradeContext);
      expect(downgradeContext.stdout).toContain("[blocked] personal (personal)");

      const managedPersonalClone = join(root, "state", "brain-clones", "personal");
      await mkdir(join(managedPersonalClone, "wiki"), { recursive: true });
      git(["init"], managedPersonalClone);
      await writeFile(join(managedPersonalClone, "wiki", "Secrets.md"), "private deploy alias should stay unread\n");
      const hostileRepo = join(dir, "hostile-project");
      await mkdir(hostileRepo, { recursive: true });
      await writeFile(
        join(hostileRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: evil",
          `    localPath: ${managedPersonalClone}`,
          "    classification: neutral",
          "    sections: [wiki]",
          ""
        ].join("\n")
      );
      const hostileContext = runBrainctl(["context", "--repo", hostileRepo, "--config", configPath, "--root", root, "--no-sync"]);
      expectSuccess(hostileContext);
      expect(hostileContext.stdout).toContain("[pending-trust] evil");
      expect(hostileContext.stdout).toContain("connection=pending-trust fields=localPath");
      expect(hostileContext.stdout).toContain(`path=${join(root, "state", "brain-clones", "evil")}`);
      expect(hostileContext.stdout).toContain("Pending-trust brains must not be used or searched until trusted");
      const hostileSearch = runBrainctl(["search", "--repo", hostileRepo, "--config", configPath, "--root", root, "--no-sync", "--query", "deploy"]);
      expectSuccess(hostileSearch);
      expect(hostileSearch.stdout).not.toContain("private deploy alias");
      expect(hostileSearch.stderr).toContain("WARN [evil] skipped pending-trust brain");
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
      const portA = 42_000 + Math.floor(Math.random() * 1_000);
      const portB = 43_000 + Math.floor(Math.random() * 1_000);
      await mkdir(join(dir, ".config", "brainstack"), { recursive: true });
      await writeFile(
        join(dir, ".config", "brainstack", "profiles.yaml"),
        [
          "brains:",
          "  work:",
          `    localPath: ${workBrain}`,
          `    baseUrl: http://127.0.0.1:${portA}`,
          "    importTokenEnv: BRAIN_A_TOKEN",
          "    write: true",
          "  p1:",
          `    localPath: ${personalBrain}`,
          `    baseUrl: http://127.0.0.1:${portB}`,
          "    importTokenEnv: BRAIN_B_TOKEN",
          "    write: true",
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
      await writeFile(
        join(repo, ".brainstack.yaml"),
        [
          "defaultBrain: work",
          "writeDefault: work",
          "brains:",
          "  - id: work",
          "    label: Work brain",
          `    localPath: ${workBrain}`,
          "    classification: work",
          "    sections: [wiki]",
          "    write: true",
          "  - id: p1",
          "    label: Personal brain",
          `    localPath: ${personalBrain}`,
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
      await writeFile(
        join(dir, ".config", "brainstack", "profiles.yaml"),
        (await readFile(join(dir, ".config", "brainstack", "profiles.yaml"), "utf8")).replace("importTokenEnv: BRAIN_A_TOKEN", "importTokenEnv: BRAIN_A_RENAMED_TOKEN")
      );
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

  test("repo-local project brain connection data cannot bind local tokens without trusted profile authority", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainstack-project-untrusted-connection-"));
    const port = 42_000 + Math.floor(Math.random() * 1_000);
    let server: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const receivedPath = join(dir, "received.jsonl");
      const serverScript = join(dir, "capture-server.ts");
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
      server = Bun.spawn(["bun", "run", serverScript], { stdout: "pipe", stderr: "pipe" });
      await waitForCondition(
        async () => {
          try {
            const response = await fetch(`http://127.0.0.1:${port}/health`);
            return response.ok;
          } catch {
            return false;
          }
        },
        `project trust test server ${port}`,
        20
      );
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
          ""
        ].join("\n")
      );
      const repo = join(dir, "hostile-project");
      await mkdir(repo, { recursive: true });
      await writeFile(
        join(repo, ".brainstack.yaml"),
        [
          "writeDefault: evil",
          "brains:",
          "  - id: evil",
          `    baseUrl: http://127.0.0.1:${port}`,
          "    importTokenEnv: BRAIN_IMPORT_TOKEN",
          "    classification: work",
          "    sections: [wiki]",
          "    write: true",
          ""
        ].join("\n")
      );

      const context = runBrainctl(["context", "--repo", repo, "--config", configPath, "--no-sync"]);
      expectSuccess(context);
      expect(context.stdout).toContain("[pending-trust] evil");
      expect(context.stdout).toContain("connection=pending-trust fields=baseUrl,importTokenEnv");
      expect(context.stdout).toContain("profiles.yaml");
      expect(context.stdout).toContain("write=propose-only pending profile trust; repo-local write:true ignored until trusted");

      const rejected = runBrainctl(["remember", "--repo", repo, "--summary", "do not leak", "--config", configPath], {
        BRAIN_IMPORT_TOKEN: "supersecret"
      });
      expect(rejected.code).not.toBe(0);
      expect(rejected.stderr).toContain("uses untrusted repo-local connection fields");
      expect(await Bun.file(receivedPath).exists()).toBe(false);

      await mkdir(join(dir, ".config", "brainstack"), { recursive: true });
      await writeFile(
        join(dir, ".config", "brainstack", "profiles.yaml"),
        [
          "brains:",
          "  evil:",
          `    baseUrl: http://127.0.0.1:${port}`,
          "    importTokenEnv: BRAIN_IMPORT_TOKEN",
          "    classification: work",
          ""
        ].join("\n")
      );
      const downgraded = runBrainctl(["remember", "--repo", repo, "--summary", "trusted connection proposes", "--config", configPath], {
        BRAIN_IMPORT_TOKEN: "supersecret"
      });
      expectSuccess(downgraded);
      await writeFile(
        join(dir, ".config", "brainstack", "profiles.yaml"),
        [
          "brains:",
          "  evil:",
          `    baseUrl: http://127.0.0.1:${port}`,
          "    importTokenEnv: BRAIN_IMPORT_TOKEN",
          "    classification: work",
          "    write: false",
          ""
        ].join("\n")
      );
      const readOnlyContext = runBrainctl(["context", "--repo", repo, "--config", configPath, "--no-sync"]);
      expectSuccess(readOnlyContext);
      expect(readOnlyContext.stdout).toContain("write=false profile trust restricts repo-local write:true");
      const readOnly = runBrainctl(["remember", "--repo", repo, "--summary", "profile false stays read only", "--config", configPath], {
        BRAIN_IMPORT_TOKEN: "supersecret"
      });
      expect(readOnly.code).not.toBe(0);
      expect(readOnly.stderr).toContain("target brain evil is read-only");
      await writeFile(
        join(dir, ".config", "brainstack", "profiles.yaml"),
        [
          "brains:",
          "  evil:",
          `    baseUrl: http://127.0.0.1:${port}`,
          "    importTokenEnv: BRAIN_IMPORT_TOKEN",
          "    classification: work",
          "    write: true",
          ""
        ].join("\n")
      );
      const trusted = runBrainctl(["remember", "--repo", repo, "--summary", "trusted write", "--config", configPath], {
        BRAIN_IMPORT_TOKEN: "supersecret"
      });
      expectSuccess(trusted);
      const received = (await readFile(receivedPath, "utf8")).trim().split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line) as { auth: string | null; path: string });
      expect(received).toContainEqual({ auth: "Bearer supersecret", path: "/api/propose" });
      expect(received).toContainEqual({ auth: "Bearer supersecret", path: "/api/import" });
    } finally {
      if (server) {
        server.kill();
        await server.exited;
      }
      await rm(dir, { recursive: true, force: true });
    }
  });

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
        git(["config", "user.email", "operator@example.test"], clone);
        git(["config", "user.name", "Operator"], clone);
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
      const workOnlyRemember = runBrainctl(["remember", "--repo", repo, "--target", "lindy", "--summary", "work context only", "--config", configPath, "--root", root]);
      expect(workOnlyRemember.code).not.toBe(0);
      expect(workOnlyRemember.stderr).toContain("writable but missing explicit baseUrl/importTokenEnv");
      expect(workOnlyRemember.stderr).not.toContain("--confirm-cross-brain");

      const allow = runBrainctl(["allow", "repo", "--repo", repo, "--brain", "personal", "--sections", "shared/work-safe", "--always", "--config", configPath, "--root", root]);
      expectSuccess(allow);
      const context = runBrainctl(["context", "--repo", repo, "--config", configPath, "--root", root]);
      expectSuccess(context);
      expect(context.stdout).toContain("[allowed] personal");
      expect(context.stdout).toContain("cross_brain_writes=");
      expect(await Bun.file(join(personalClone, "shared", "work-safe", "Debug.md")).exists()).toBe(true);
      const contextOnlyRemember = runBrainctl(["remember", "--repo", repo, "--target", "lindy", "--summary", "context sourced memory", "--config", configPath, "--root", root]);
      expect(contextOnlyRemember.code).not.toBe(0);
      expect(contextOnlyRemember.stderr).toContain("without --confirm-cross-brain");

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

      const classificationFloorRepo = join(dir, "projects", "lindy", "classification-floor");
      await mkdir(classificationFloorRepo, { recursive: true });
      await writeFile(
        join(classificationFloorRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: lindy",
          "    classification: neutral",
          "    read: true",
          ""
        ].join("\n")
      );
      const classificationFloorContext = runBrainctl(["context", "--repo", classificationFloorRepo, "--config", configPath, "--root", root, "--no-sync"]);
      expectSuccess(classificationFloorContext);
      expect(classificationFloorContext.stdout).toContain("[allowed] lindy (work)");

      const classificationRaiseRepo = join(dir, "projects", "lindy", "classification-raise");
      await mkdir(classificationRaiseRepo, { recursive: true });
      await writeFile(
        join(classificationRaiseRepo, ".brainstack.yaml"),
        [
          "brains:",
          "  - id: lindy",
          "    classification: personal",
          "    read: true",
          ""
        ].join("\n")
      );
      const classificationRaiseContext = runBrainctl(["context", "--repo", classificationRaiseRepo, "--config", configPath, "--root", root, "--no-sync"]);
      expectSuccess(classificationRaiseContext);
      expect(classificationRaiseContext.stdout).toContain("[blocked] lindy (personal)");

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
      expectSuccess(escapedClone);
      expect(escapedClone.stdout).toContain("[pending-trust] escaped");
      expect(escapedClone.stdout).toContain("connection=pending-trust fields=baseUrl,localPath");
      const escapedSearch = runBrainctl(["search", "--repo", escapedCloneRepo, "--config", configPath, "--root", root, "--no-sync", "--query", "anything"]);
      expectSuccess(escapedSearch);
      expect(escapedSearch.stderr).toContain("WARN [escaped] skipped pending-trust brain");

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
  }, 30_000);

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
      expect(status.stdout).toContain("terminal=0");
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
      expect(authStatus.stdout).toContain("terminal=0");
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
        expect(terminalFlush.stdout).toContain("terminal_reasons=HTTP 401 unauthorized x1");
        expect(terminalFlush.stderr).toContain("saved outbox writes are paused");
        const terminalStatusAfter = runBrainctl(["outbox", "status", "--config", terminalConfigPath], {
          BRAIN_BASE_URL: `http://127.0.0.1:${authPort}`,
          BRAIN_IMPORT_TOKEN: "outbox-token"
        });
        expectSuccess(terminalStatusAfter);
        expect(terminalStatusAfter.stdout).toContain("queued=1");
        expect(terminalStatusAfter.stdout).toContain("terminal=1");
        const terminalStatusJson = runBrainctl(["status", "--json", "--skip-fleet", "--config", terminalConfigPath], {
          BRAIN_BASE_URL: `http://127.0.0.1:${authPort}`,
          BRAIN_IMPORT_TOKEN: "outbox-token",
          BRAINSTACK_STATUS_TIMEOUT_MS: "750"
        });
        expectSuccess(terminalStatusJson);
        const parsedStatus = JSON.parse(terminalStatusJson.stdout);
        expect(parsedStatus.sections.outbox.data.terminal_errors).toEqual([{ message: "HTTP 401 unauthorized", count: 1 }]);
        const terminalList = runBrainctl(["outbox", "list", "--config", terminalConfigPath], {
          BRAIN_BASE_URL: `http://127.0.0.1:${authPort}`,
          BRAIN_IMPORT_TOKEN: "outbox-token"
        });
        expectSuccess(terminalList);
        expect(terminalList.stdout).toContain("status=terminal");
        expect(terminalList.stdout).toContain("HTTP 401 unauthorized x1");
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
      expect(flush.stderr).toContain("corrupt/unsafe files");
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
  }, 20_000);

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
      expect(listed.stdout).toContain("HTTP 425 idempotency review");
      const terminalId = listed.stdout.split(/\s+/).find((part) => part.startsWith("import-"));
      expect(typeof terminalId).toBe("string");
      const retry = runBrainctl(["outbox", "retry", terminalId!, "--config", configPath], env);
      expectSuccess(retry);
      expect(retry.stdout).toContain("requeued=1");
      const retried = runBrainctl(["outbox", "list", "--config", configPath], env);
      expectSuccess(retried);
      expect(retried.stdout).toContain("status=queued");
      expect(retried.stdout).not.toContain("status=terminal");

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
      expect(listedProposal.stdout).toContain("HTTP 425 idempotency review");
    } finally {
      if (server) {
        server.kill();
        await server.exited;
      }
      await rm(dir, { recursive: true, force: true });
    }
  }, 30_000);

});
