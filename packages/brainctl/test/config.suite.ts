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
  }, 20_000);

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

  test("curation config parses with safe defaults and validates inputs", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-curation-config-"));
    try {
      const defaultsPath = join(dir, "defaults.yaml");
      await writeFile(defaultsPath, ["schema_version: 1", "profile: control", ""].join("\n"));
      const defaults = await loadConfig(defaultsPath, "control");
      expect(defaults.curation.mode).toBe("approval");
      expect(defaults.curation.autoApply.allowedPaths).toEqual(["wiki/Status/**", "wiki/Sources/**"]);
      expect(defaults.curation.autoApply.maxChangedLines).toBe(40);
      expect(defaults.curation.autoApply.allowDeletes).toBe(false);

      const explicitPath = join(dir, "explicit.yaml");
      await writeFile(
        explicitPath,
        [
          "schema_version: 1",
          "profile: control",
          "curation:",
          "  mode: auto",
          "  autoApply:",
          "    allowedPaths:",
          "      - wiki/Status/**",
          "    maxChangedLines: 10",
          "    allowDeletes: true",
          ""
        ].join("\n")
      );
      const explicit = await loadConfig(explicitPath, "control");
      expect(explicit.curation.mode).toBe("auto");
      expect(explicit.curation.autoApply.allowedPaths).toEqual(["wiki/Status/**"]);
      expect(explicit.curation.autoApply.maxChangedLines).toBe(10);
      expect(explicit.curation.autoApply.allowDeletes).toBe(true);

      const badModePath = join(dir, "bad-mode.yaml");
      await writeFile(badModePath, ["schema_version: 1", "profile: control", "curation:", "  mode: yolo", ""].join("\n"));
      await expect(loadConfig(badModePath, "control")).rejects.toThrow("curation.mode must be one of manual|approval|auto");

      const badPathPath = join(dir, "bad-path.yaml");
      await writeFile(
        badPathPath,
        ["schema_version: 1", "profile: control", "curation:", "  autoApply:", "    allowedPaths:", "      - wiki/../escape/**", ""].join("\n")
      );
      await expect(loadConfig(badPathPath, "control")).rejects.toThrow("allowedPaths");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("proposals and curator CLI commands talk to the brain API with correct auth", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-proposals-cli-"));
    const port = 46_000 + Math.floor(Math.random() * 1_000);
    // The fake brain runs as a separate process: runBrainctl uses spawnSync, which
    // would deadlock an in-test Bun.serve.
    const receivedPath = join(dir, "received.jsonl");
    const serverScript = join(dir, "fake-brain.ts");
    await writeFile(
      serverScript,
      [
        `const receivedPath = ${JSON.stringify(receivedPath)};`,
        `const proposals = [`,
        `  { id: "20260611t000000z-cli-proposal", title: "Remember: CLI proposal needs context", status: "needs-human", legacy_format: true, quality_decision: "needs-context", cluster_key: "cli:needs-context:legacy_memory", cluster_label: "CLI / needs-context / legacy_memory", target_page: "wiki/Status/CLI.md", risk: "low", confidence: 0.8, created_at: "2026-06-11T00:00:00Z", source_ids: ["art-1"] },`,
        `  { id: "p1", title: "Remember (cli): CLI proposal output should be bounded", status: "pending", source_type: "remember", project: "cli", domain: "cli", scope: "repo", memory_kind: "project_lesson", quality_decision: "ready", cluster_key: "cli:repo:project_lesson", cluster_label: "CLI / repo / project_lesson", risk: "low", confidence: 0.8, created_at: "2026-06-11T00:01:00Z", source_ids: ["art-1"] },`,
        `  { id: "p2", title: "Remember (cli): CLI proposal merge should preserve evidence", status: "pending", source_type: "remember", project: "cli", domain: "cli", scope: "repo", memory_kind: "project_lesson", quality_decision: "ready", cluster_key: "cli:repo:project_lesson", cluster_label: "CLI / repo / project_lesson", risk: "low", confidence: 0.8, created_at: "2026-06-11T00:02:00Z", source_ids: ["art-2"] }`,
        `];`,
        `const reviewGroups = [{ id: "cli:repo:project_lesson", label: "CLI / repo / project_lesson", count: 2, legacyCount: 0, needsContextCount: 0, proposalIds: ["p1", "p2"] }];`,
        `Bun.serve({`,
        `  hostname: "127.0.0.1",`,
        `  port: ${port},`,
        `  async fetch(req) {`,
        `    const url = new URL(req.url);`,
        `    const body = req.method === "POST" ? await req.json().catch(() => null) : null;`,
        `    const existing = (await Bun.file(receivedPath).exists()) ? await Bun.file(receivedPath).text() : "";`,
        `    await Bun.write(receivedPath, existing + JSON.stringify({ method: req.method, path: url.pathname + url.search, auth: req.headers.get("authorization"), body }) + "\\n");`,
        `    if (req.method === "GET" && url.pathname === "/healthz") {`,
        `      return Response.json({ ok: true, service: "braind" });`,
        `    }`,
        `    if (req.method === "GET" && url.pathname === "/readyz") {`,
        `      return Response.json({ ok: true, service: "braind", search_ready: true, pending_reindex: { present: false } });`,
        `    }`,
        `    if (req.method === "GET" && url.pathname === "/api/proposals") {`,
        `      return Response.json({ ok: true, mode: "approval", proposals, review_groups: reviewGroups });`,
        `    }`,
        `    if (req.method === "GET" && (url.pathname === "/api/proposals/groups" || url.pathname === "/api/proposals/clusters")) {`,
        `      return Response.json({ ok: true, status: url.searchParams.get("status") || "open", min_size: Number(url.searchParams.get("min_size") || "2"), review_groups: reviewGroups });`,
        `    }`,
        `    if (req.method === "GET" && url.pathname.startsWith("/api/proposals/")) {`,
        `      const id = decodeURIComponent(url.pathname.split("/").pop() || "");`,
        `      const proposal = proposals.find((item) => item.id === id) || proposals[0];`,
        `      return Response.json({ ok: true, proposal, body: "## Request\\n\\n" + proposal.title, diff: "+ new line" });`,
        `    }`,
        `    if (req.method === "POST" && url.pathname.endsWith("/approve")) {`,
        `      return Response.json({ ok: true, status: "approved" });`,
        `    }`,
        `    if (req.method === "POST" && url.pathname.endsWith("/reject")) {`,
        `      return Response.json({ ok: true, status: "rejected" });`,
        `    }`,
        `    if (req.method === "POST" && url.pathname.endsWith("/apply")) {`,
        `      return Response.json({ ok: true, status: "applied" });`,
        `    }`,
        `    if (req.method === "GET" && url.pathname === "/api/curator/status") {`,
        `      return Response.json({ ok: true, mode: "approval", curator: { installed: true, last_run_finished_at: "2026-06-11T00:00:00Z", last_run_ok: true, last_run_failures: [], cursor: null, next_run_at: null }, proposal_counts: { pending: 1, approved: 0, applied: 0, rejected: 0, superseded: 0, "needs-human": 0 } });`,
        `    }`,
        `    if (req.method === "POST" && url.pathname === "/api/propose") {`,
        `      return Response.json({ ok: true, proposal_id: "x", status: "pending", auto_applied: false });`,
        `    }`,
        `    return Response.json({ error: "unexpected" }, { status: 500 });`,
        `  }`,
        `});`
      ].join("\n")
    );
    const server = Bun.spawn(["bun", "run", serverScript], { stdout: "ignore", stderr: "ignore" });
    const readRequests = async (): Promise<Array<{ method: string; path: string; auth: string | null; body: Record<string, unknown> | null }>> =>
      (await Bun.file(receivedPath).exists())
        ? (await readFile(receivedPath, "utf8"))
            .trim()
            .split(/\r?\n/)
            .filter(Boolean)
            .map((line) => JSON.parse(line) as { method: string; path: string; auth: string | null; body: Record<string, unknown> | null })
        : [];
    try {
      await waitForCondition(async () => {
        try {
          await fetch(`http://127.0.0.1:${port}/api/curator/status`);
          return true;
        } catch {
          return false;
        }
      }, "fake brain server startup");
      const configPath = join(dir, "config.yaml");
      await writeFixtureConfig(configPath);
      const env = { HOME: dir, BRAIN_BASE_URL: `http://127.0.0.1:${port}`, BRAIN_IMPORT_TOKEN: "cli-import-token", BRAIN_ADMIN_TOKEN: "" };

      const list = runBrainctl(["proposals", "list", "--config", configPath], env);
      expectSuccess(list);
      expect(list.stdout).toContain("20260611t000000z-cli-proposal");
      expect(list.stdout).toContain("status=needs-human");
      expect(list.stdout).toContain("review_groups=1");
      expect((await readRequests()).at(-1)?.auth).toBeNull();

      const clusterList = runBrainctl(["proposals", "groups", "--config", configPath], env);
      expectSuccess(clusterList);
      expect(clusterList.stdout).toContain("proposal review groups=1");
      expect(clusterList.stdout).toContain("cli:repo:project_lesson");

      const mergePlan = runBrainctl(["proposals", "merge-group", "cli:repo:project_lesson", "--config", configPath], env);
      expectSuccess(mergePlan);
      expect(mergePlan.stdout).toContain("dry_run=true");
      expect(mergePlan.stdout).toContain("target=wiki/Syntheses/cli-lessons.md");

      const mergeLimited = runBrainctl(["proposals", "merge-group", "cli:repo:project_lesson", "--limit", "1", "--config", configPath], env);
      expect(mergeLimited.code).not.toBe(0);
      expect(mergeLimited.stderr).toContain("merge-group defaults to 1");

      const mergeSubmit = runBrainctl(["proposals", "merge-group", "cli:repo:project_lesson", "--submit", "--config", configPath], env);
      expectSuccess(mergeSubmit);
      const mergeRequest = (await readRequests()).find((entry) => entry.path === "/api/propose" && String(entry.body?.title || "").startsWith("Consolidate: CLI"));
      expect(mergeRequest?.body?.target_page).toBe("wiki/Syntheses/cli-lessons.md");
      expect(String(mergeRequest?.body?.proposed_content || "")).toContain("proposal:p1");
      expect(String(mergeRequest?.body?.proposed_content || "")).toContain("proposal:p2");

      const mergeClose = runBrainctl(["proposals", "merge-group", "cli:repo:project_lesson", "--submit", "--close-sources", "--config", configPath], {
        ...env,
        BRAIN_ADMIN_TOKEN: "cli-admin-token"
      });
      expectSuccess(mergeClose);
      const closeRequests = await readRequests();
      const closeProposeRequest = closeRequests
        .filter((entry) => entry.path === "/api/propose" && String(entry.body?.title || "").startsWith("Consolidate: CLI"))
        .at(-1);
      expect(closeProposeRequest?.auth).toBe("Bearer cli-admin-token");
      const closeRejects = closeRequests.filter((entry) => entry.path.endsWith("/reject") && String(entry.body?.reason || "").includes("merged into x"));
      expect(closeRejects.map((entry) => entry.path).sort()).toEqual(["/api/proposals/p1/reject", "/api/proposals/p2/reject"]);

      const show = runBrainctl(["proposals", "show", "20260611t000000z-cli-proposal", "--config", configPath], env);
      expectSuccess(show);
      expect(show.stdout).toContain("+ new line");

      const approveDenied = runBrainctl(["proposals", "approve", "20260611t000000z-cli-proposal", "--config", configPath], env);
      expect(approveDenied.code).not.toBe(0);
      expect(approveDenied.stderr).toContain("BRAIN_ADMIN_TOKEN is required");

      const approved = runBrainctl(["proposals", "approve", "20260611t000000z-cli-proposal", "--config", configPath], {
        ...env,
        BRAIN_ADMIN_TOKEN: "cli-admin-token"
      });
      expectSuccess(approved);
      const approveRequest = (await readRequests()).find((entry) => entry.path.endsWith("/approve"));
      expect(approveRequest?.auth).toBe("Bearer cli-admin-token");
      expect(String(approveRequest?.body?.decided_by || "")).toContain("@brain-control");

      const status = runBrainctl(["curator", "status", "--config", configPath], env);
      expectSuccess(status);
      expect(status.stdout).toContain("mode=approval");
      expect(status.stdout).toContain("installed=yes");
      expect(status.stdout).toContain("pending=1");

      const aggregateStatus = runBrainctl(["status", "--json", "--config", configPath, "--timeout-ms", "500"], env);
      expectSuccess(aggregateStatus);
      const aggregate = JSON.parse(aggregateStatus.stdout) as Record<string, any>;
      expect(aggregate.product).toBe("brainstack");
      expect(aggregate.sections.brain_api.state).toBe("ok");
      expect(aggregate.sections.curator.state).toBe("ok");
      expect(aggregate.sections.curator.data.open_proposals).toBe(1);
      expect(aggregate.sections.proposals.state).toBe("ok");
      expect(aggregate.sections.proposals.data.count).toBe(3);

      // propose with machine fields posts them to /api/propose.
      const contentFile = join(dir, "proposed.md");
      await writeFile(contentFile, "# Proposed\n");
      const propose = runBrainctl(
        [
          "propose",
          "--config",
          configPath,
          "--title",
          "Machine proposal",
          "--body",
          "why",
          "--target-page",
          "wiki/Status/CLI.md",
          "--content-file",
          contentFile,
          "--base-sha256",
          "absent",
          "--risk",
          "low",
          "--confidence",
          "0.8",
          "--curator-run-id",
          "run-1",
          "--source-ids",
          "art-1,art-2",
          "--project",
          "brainstack",
          "--domain",
          "curation",
          "--scope",
          "repo",
          "--memory-kind",
          "project_lesson",
          "--applicability",
          "Use for Brainstack curation proposals.",
          "--non-applicability",
          "Do not apply outside Brainstack without checking the target repo.",
          "--evidence",
          "repo:/tmp/brainstack",
          "--evidence",
          "default:wiki/Status/CLI.md:1"
        ],
        env
      );
      expectSuccess(propose);
      const proposeRequest = (await readRequests()).find((entry) => entry.path === "/api/propose" && entry.body?.target_page === "wiki/Status/CLI.md");
      expect(proposeRequest?.body?.target_page).toBe("wiki/Status/CLI.md");
      expect(proposeRequest?.body?.proposed_content).toBe("# Proposed\n");
      expect(proposeRequest?.body?.base_sha256).toBe("absent");
      expect(proposeRequest?.body?.risk).toBe("low");
      expect(proposeRequest?.body?.confidence).toBe(0.8);
      expect(proposeRequest?.body?.curator_run_id).toBe("run-1");
      expect(proposeRequest?.body?.source_ids).toEqual(["art-1", "art-2"]);
      expect(proposeRequest?.body?.project).toBe("brainstack");
      expect(proposeRequest?.body?.domain).toBe("curation");
      expect(proposeRequest?.body?.scope).toBe("repo");
      expect(proposeRequest?.body?.memory_kind).toBe("project_lesson");
      expect(proposeRequest?.body?.applicability).toBe("Use for Brainstack curation proposals.");
      expect(proposeRequest?.body?.non_applicability).toBe("Do not apply outside Brainstack without checking the target repo.");
      expect(proposeRequest?.body?.evidence_refs).toEqual(["repo:/tmp/brainstack", "default:wiki/Status/CLI.md:1"]);

      const enrich = runBrainctl(
        [
          "proposals",
          "enrich",
          "20260611t000000z-cli-proposal",
          "--config",
          configPath,
          "--project",
          "brainstack",
          "--domain",
          "curation",
          "--scope",
          "repo",
          "--related-repo",
          "/tmp/brainstack",
          "--applicability",
          "Use for curation CLI work.",
          "--non-applicability",
          "Do not apply outside Brainstack curation.",
          "--evidence",
          "codex-session:abc"
        ],
        env
      );
      expectSuccess(enrich);
      const enrichRequest = (await readRequests()).filter((entry) => entry.path === "/api/propose").at(-1);
      expect(enrichRequest?.body?.source_type).toBe("memory");
      expect(enrichRequest?.body?.project).toBe("brainstack");
      expect(enrichRequest?.body?.domain).toBe("curation");
      expect(enrichRequest?.body?.scope).toBe("repo");
      expect(enrichRequest?.body?.memory_kind).toBe("project_lesson");
      expect(enrichRequest?.body?.related_repo).toBe("/tmp/brainstack");
      expect(enrichRequest?.body?.applicability).toBe("Use for curation CLI work.");
      expect(enrichRequest?.body?.non_applicability).toBe("Do not apply outside Brainstack curation.");
      expect(enrichRequest?.body?.evidence_refs).toEqual(["proposal:20260611t000000z-cli-proposal", "source:art-1", "repo:/tmp/brainstack", "codex-session:abc"]);

      const enrichJson = runBrainctl(["proposals", "enrich", "20260611t000000z-cli-proposal", "--config", configPath, "--json"], env);
      expectSuccess(enrichJson);
      const enrichJsonBody = JSON.parse(enrichJson.stdout) as Record<string, any>;
      expect(enrichJsonBody.dryRun).toBe(false);
      expect(enrichJsonBody.write.status).toBe("accepted");
      expect(enrichJson.stdout).not.toContain("shared-brain propose accepted");

      const reprocessPlan = runBrainctl(["proposals", "reprocess", "--config", configPath, "--limit", "1", "--json"], env);
      expectSuccess(reprocessPlan);
      const reprocessBody = JSON.parse(reprocessPlan.stdout) as Record<string, any>;
      expect(reprocessBody.apply).toBe(false);
      expect(reprocessBody.results[0].dryRun).toBe(true);
      expect(reprocessBody.results[0].payload.evidence_refs).toContain("proposal:20260611t000000z-cli-proposal");
    } finally {
      server.kill();
      await server.exited;
      await rm(dir, { recursive: true, force: true });
    }
  }, 30_000);

  test("merge-group close-sources resumes after a partially closed source", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-merge-retry-"));
    const receivedPath = join(dir, "requests.jsonl");
    const port = 46_000 + Math.floor(Math.random() * 1_000);
    const configPath = join(dir, "brainstack.yaml");
    await writeFile(
      configPath,
      [
        "schema_version: 1",
        "profile: client-macos",
        "machine:",
        "  name: cli",
        "brain:",
        `  publicBaseUrl: http://127.0.0.1:${port}`,
        "security:",
        "  posture: trusted-tailnet",
        "  bindHost: 127.0.0.1",
        "  trustedExposure: none",
        "paths:",
        `  configRoot: ${dir}`,
        `  stateRoot: ${join(dir, "state")}`,
        `  sharedBrainBareRepo: ${join(dir, "bare.git")}`,
        `  sharedBrainStagingRepo: ${join(dir, "staging")}`,
        `  sharedBrainServeRepo: ${join(dir, "serve")}`,
        "client:",
        `  localPath: ${join(dir, "shared-brain")}`,
        ""
      ].join("\n")
    );
    const serverScript = join(dir, "brain.ts");
    await writeFile(
      serverScript,
      [
        `const receivedPath = ${JSON.stringify(receivedPath)};`,
        `const proposals = [`,
        `  { id: "p1", title: "Remember (cli): old closed", status: "rejected", reason: "merged into old-merge", project: "cli", domain: "cli", scope: "repo", memory_kind: "project_lesson", cluster_key: "cli:repo:project_lesson", cluster_label: "CLI / repo / project_lesson", created_at: "2026-06-11T00:01:00Z" },`,
        `  { id: "p2", title: "Remember (cli): still open", status: "pending", project: "cli", domain: "cli", scope: "repo", memory_kind: "project_lesson", cluster_key: "cli:repo:project_lesson", cluster_label: "CLI / repo / project_lesson", created_at: "2026-06-11T00:02:00Z" }`,
        `];`,
        `Bun.serve({`,
        `  hostname: "127.0.0.1",`,
        `  port: ${port},`,
        `  async fetch(req) {`,
        `    const url = new URL(req.url);`,
        `    const body = req.method === "POST" ? await req.json().catch(() => null) : null;`,
        `    const existing = (await Bun.file(receivedPath).exists()) ? await Bun.file(receivedPath).text() : "";`,
        `    await Bun.write(receivedPath, existing + JSON.stringify({ method: req.method, path: url.pathname + url.search, auth: req.headers.get("authorization"), body }) + "\\n");`,
        `    if (req.method === "GET" && url.pathname === "/api/proposals") return Response.json({ ok: true, proposals });`,
        `    if (req.method === "GET" && url.pathname.startsWith("/api/proposals/")) {`,
        `      const id = decodeURIComponent(url.pathname.split("/").pop() || "");`,
        `      const proposal = proposals.find((item) => item.id === id);`,
        `      return proposal ? Response.json({ ok: true, proposal, body: "## Request\\n\\n" + proposal.title, diff: "" }) : Response.json({ error: "missing" }, { status: 404 });`,
        `    }`,
        `    if (req.method === "POST" && url.pathname === "/api/propose") return Response.json({ error: "must not create duplicate merge" }, { status: 500 });`,
        `    if (req.method === "POST" && url.pathname === "/api/proposals/p1/reject") return Response.json({ error: "already rejected" }, { status: 409 });`,
        `    if (req.method === "POST" && url.pathname === "/api/proposals/p2/reject") return Response.json({ ok: true, status: "rejected" });`,
        `    return Response.json({ error: "unexpected" }, { status: 500 });`,
        `  }`,
        `});`
      ].join("\n")
    );
    const server = Bun.spawn(["bun", "run", serverScript], { stdout: "ignore", stderr: "ignore" });
    const readRequests = async (): Promise<Array<{ method: string; path: string; auth: string | null; body: Record<string, unknown> | null }>> =>
      (await Bun.file(receivedPath).exists())
        ? (await readFile(receivedPath, "utf8"))
            .trim()
            .split(/\r?\n/)
            .filter(Boolean)
            .map((line) => JSON.parse(line) as { method: string; path: string; auth: string | null; body: Record<string, unknown> | null })
        : [];
    try {
      await waitForCondition(async () => {
        const response = await fetch(`http://127.0.0.1:${port}/api/proposals`).catch(() => null);
        return Boolean(response?.ok);
      });
      const env = { ...process.env, BRAIN_BASE_URL: `http://127.0.0.1:${port}`, BRAIN_ADMIN_TOKEN: "cli-admin-token" };
      const result = runBrainctl(["proposals", "merge-group", "cli:repo:project_lesson", "--submit", "--close-sources", "--config", configPath], env);
      expectSuccess(result);
      const requests = await readRequests();
      expect(requests.some((entry) => entry.path === "/api/propose")).toBe(false);
      expect(requests.some((entry) => entry.path === "/api/proposals/p1/reject")).toBe(false);
      const p2Reject = requests.find((entry) => entry.path === "/api/proposals/p2/reject");
      expect(p2Reject?.auth).toBe("Bearer cli-admin-token");
      expect(p2Reject?.body?.reason).toBe("merged into old-merge");
    } finally {
      server.kill();
      await server.exited;
      await rm(dir, { recursive: true, force: true });
    }
  }, 30_000);

  test("status json degrades quickly when brain endpoints do not answer", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-status-timeout-"));
    const port = 47_000 + Math.floor(Math.random() * 1_000);
    const serverScript = join(dir, "slow-brain.ts");
    await writeFile(
      serverScript,
      [
        `Bun.serve({`,
        `  hostname: "127.0.0.1",`,
        `  port: ${port},`,
        `  fetch(req) {`,
        `    const url = new URL(req.url);`,
        `    if (url.pathname === "/ping") return Response.json({ ok: true });`,
        `    return new Promise(() => {});`,
        `  }`,
        `});`
      ].join("\n")
    );
    const server = Bun.spawn(["bun", "run", serverScript], { stdout: "ignore", stderr: "ignore" });
    try {
      await waitForCondition(async () => {
        try {
          return (await fetch(`http://127.0.0.1:${port}/ping`)).ok;
        } catch {
          return false;
        }
      }, "slow fake brain server startup");
      const configPath = join(dir, "config.yaml");
      await writeFixtureConfig(configPath);
      const started = Date.now();
      const result = runBrainctl(["status", "--json", "--config", configPath, "--timeout-ms", "150"], {
        HOME: dir,
        BRAIN_BASE_URL: `http://127.0.0.1:${port}`,
        BRAINSTACK_STATUS_TIMEOUT_MS: "150"
      });
      const elapsedMs = Date.now() - started;
      expectSuccess(result);
      expect(elapsedMs).toBeLessThan(5000);
      const parsed = JSON.parse(result.stdout) as Record<string, any>;
      expect(parsed.sections.config.state).toBe("ok");
      expect(parsed.sections.brain_api.state).toBe("warn");
      expect(parsed.sections.brain_api.available).toBe(false);
      expect(parsed.sections.curator.state).toBe("warn");
      expect(parsed.sections.curator.available).toBe(false);
      expect(parsed.sections.proposals.state).toBe("warn");
      expect(parsed.sections.proposals.available).toBe(false);
    } finally {
      server.kill();
      await server.exited;
      await rm(dir, { recursive: true, force: true });
    }
  }, 10_000);

  test("status json uses curator summary when the full proposal list times out", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-status-proposal-fallback-"));
    const port = 47_000 + Math.floor(Math.random() * 1_000);
    const serverScript = join(dir, "proposal-slow-brain.ts");
    await writeFile(
      serverScript,
      [
        `Bun.serve({`,
        `  hostname: "127.0.0.1",`,
        `  port: ${port},`,
        `  fetch(req) {`,
        `    const url = new URL(req.url);`,
        `    if (url.pathname === "/ping") return Response.json({ ok: true });`,
        `    if (url.pathname === "/healthz") return Response.json({ ok: true, service: "braind" });`,
        `    if (url.pathname === "/readyz") return Response.json({ ok: true, service: "braind", search_ready: true, pending_reindex: { present: false } });`,
        `    if (url.pathname === "/api/curator/status") {`,
        `      return Response.json({ ok: true, mode: "approval", curator: { installed: true, last_run_ok: true, last_run_failures: [] }, proposal_counts: { pending: 2, approved: 0, applied: 1, rejected: 0, superseded: 0, "needs-human": 1 } });`,
        `    }`,
        `    if (url.pathname === "/api/proposals") return new Promise(() => {});`,
        `    return Response.json({ error: "unexpected" }, { status: 500 });`,
        `  }`,
        `});`
      ].join("\n")
    );
    const server = Bun.spawn(["bun", "run", serverScript], { stdout: "ignore", stderr: "ignore" });
    try {
      await waitForCondition(async () => {
        try {
          return (await fetch(`http://127.0.0.1:${port}/ping`)).ok;
        } catch {
          return false;
        }
      }, "proposal fallback fake brain server startup");
      const configPath = join(dir, "config.yaml");
      await writeFixtureConfig(configPath);
      const result = runBrainctl(["status", "--json", "--config", configPath, "--timeout-ms", "150"], {
        HOME: dir,
        BRAIN_BASE_URL: `http://127.0.0.1:${port}`
      });
      expectSuccess(result);
      const parsed = JSON.parse(result.stdout) as Record<string, any>;
      expect(parsed.sections.brain_api.state).toBe("ok");
      expect(parsed.sections.curator.state).toBe("ok");
      expect(parsed.sections.curator.data.open_proposals).toBe(3);
      expect(parsed.sections.proposals.state).toBe("ok");
      expect(parsed.sections.proposals.detail).toContain("using curator summary");
      expect(parsed.sections.proposals.data.count).toBe(3);
      expect(parsed.sections.proposals.data.by_status.pending).toBe(2);
      expect(parsed.sections.proposals.data["list_available"]).toBe(false);
      expect(String(parsed.sections.proposals.data["list_error"]).toLowerCase()).toContain("timed out");
    } finally {
      server.kill();
      await server.exited;
      await rm(dir, { recursive: true, force: true });
    }
  }, 10_000);

  test("status json treats missing product checkout as informational for clients", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-status-client-product-"));
    try {
      const configPath = join(dir, "client.yaml");
      await writeFixtureClientConfig(configPath, {
        home: join(dir, "home"),
        stateRoot: join(dir, "state")
      });
      const status = runBrainctl(["status", "--json", "--config", configPath, "--timeout-ms", "200"], { HOME: join(dir, "home") });
      expectSuccess(status);
      const aggregate = JSON.parse(status.stdout) as Record<string, any>;
      expect(aggregate.sections.product.state).toBe("disabled");
      expect(aggregate.sections.product.detail).toContain("source checkout not installed");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("status json reports client control source state", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-status-control-source-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const binDir = join(dir, "bin");
      const configPath = join(dir, "client.yaml");
      await mkdir(binDir, { recursive: true });
      await writeFile(
        join(binDir, "ssh"),
        [
          "#!/usr/bin/env bash",
          "case \"$*\" in",
          "  *fleet*status*)",
          "    cat <<'JSON'",
          "{\"schema_version\":1,\"generated_at\":\"2026-06-11T00:00:00Z\",\"source_machine\":\"brain-control\",\"profile\":\"control\",\"ok\":true,\"degraded\":false,\"summary\":{\"total\":2,\"reachable\":2,\"needs_update\":0,\"unhealthy\":0},\"machines\":[{\"name\":\"brain-control\",\"role\":\"control\",\"transport\":\"local\",\"reachable\":true,\"status\":\"ok\",\"update_state\":\"current\",\"needs_update\":false,\"detail\":\"current head=abcdef0\",\"short\":\"abcdef0\"},{\"name\":\"worker-a\",\"role\":\"worker\",\"transport\":\"ssh\",\"reachable\":true,\"status\":\"ok\",\"update_state\":\"current\",\"needs_update\":false,\"detail\":\"current head=1234567\",\"short\":\"1234567\"}]}",
          "JSON",
          "    exit 0",
          "    ;;",
          "esac",
          "printf 'repo=/home/operator/brainstack\\n'",
          "printf 'state=ok\\n'",
          "printf 'branch=main\\n'",
          "printf 'head=abcdef0123456789\\n'",
          "printf 'short=abcdef0\\n'",
          "printf 'dirty_count=0\\n'",
          "printf 'remote_ref=origin/main\\n'",
          "printf 'origin_head=abcdef0123456789\\n'",
          "printf 'ahead_behind=0\\t0\\n'"
        ].join("\n")
      );
      await chmod(join(binDir, "ssh"), 0o755);
      await writeFixtureClientConfig(configPath, {
        home,
        stateRoot,
        productRepo: join(dir, "missing-product"),
        telegramVia: "operator@brain-control",
        telegramRemoteRepo: "~/brainstack"
      });
      const status = runBrainctl(["status", "--json", "--config", configPath, "--timeout-ms", "500"], {
        HOME: home,
        PATH: `${binDir}:${process.env.PATH || ""}`
      });
      expectSuccess(status);
      const aggregate = JSON.parse(status.stdout) as Record<string, any>;
      expect(aggregate.sections.control_source.state).toBe("ok");
      expect(aggregate.sections.control_source.detail).toContain("control host up to date");
      expect(aggregate.sections.control_source.data.machine).toBe("operator@brain-control");
      expect(aggregate.sections.control_source.data.short).toBe("abcdef0");
      expect(aggregate.sections.fleet.state).toBe("ok");
      expect(aggregate.sections.fleet.data.summary.total).toBe(3);
      expect(aggregate.sections.fleet.data.machines.some((machine: Record<string, unknown>) => machine.name === "worker-a")).toBe(true);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("client fleet status flags old control host and update dry-run bootstraps it directly", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-status-old-fleet-control-"));
    try {
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const binDir = join(dir, "bin");
      const configPath = join(dir, "client.yaml");
      await mkdir(binDir, { recursive: true });
      await writeFile(
        join(binDir, "ssh"),
        [
          "#!/usr/bin/env bash",
          "case \"$*\" in",
          "  *fleet*status*)",
          "    printf 'Unknown command: fleet\\n' >&2",
          "    exit 1",
          "    ;;",
          "esac",
          "printf 'repo=/home/operator/brainstack\\n'",
          "printf 'state=ok\\n'",
          "printf 'branch=main\\n'",
          "printf 'head=abcdef0123456789\\n'",
          "printf 'short=abcdef0\\n'",
          "printf 'dirty_count=0\\n'",
          "printf 'remote_ref=origin/main\\n'",
          "printf 'origin_head=abcdef0123456789\\n'",
          "printf 'ahead_behind=0\\t0\\n'"
        ].join("\n")
      );
      await chmod(join(binDir, "ssh"), 0o755);
      await writeFixtureClientConfig(configPath, {
        home,
        stateRoot,
        productRepo: join(dir, "missing-product"),
        telegramVia: "operator@brain-control",
        telegramRemoteRepo: "~/brainstack"
      });
      const env = { HOME: home, PATH: `${binDir}:${process.env.PATH || ""}` };
      const status = runBrainctl(["fleet", "status", "--json", "--no-fetch", "--config", configPath], env);
      expectSuccess(status);
      const aggregate = JSON.parse(status.stdout) as Record<string, any>;
      const control = aggregate.machines.find((machine: Record<string, unknown>) => machine.name === "brain-control");
      expect(control).toBeTruthy();
      expect(control.needs_update).toBe(true);
      expect(control.reachable).toBe(true);
      expect(control.detail).toContain("too old");

      const dryRun = runBrainctl(["fleet", "update", "brain-control", "--dry-run", "--config", configPath], env);
      expectSuccess(dryRun);
      expect(dryRun.stdout).toContain("OK brain-control");
      expect(dryRun.stdout).toContain("git fetch --quiet origin main");
      expect(dryRun.stdout).toContain("git merge --ff-only origin/main");
      expect(dryRun.stdout).toContain("~/.config/brainstack/brainstack.yaml");
      expect(dryRun.stdout).toContain("\\~/*)");
      expect(dryRun.stdout).toContain("${1#\\~/}");
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
      const candidate = join(configDir, "current-install.brainstack.yaml");
      await writeFile(candidate, "schema_version: 1\nprofile: control\n");
      const missing = join(configDir, "brainstack.yaml");
      const result = runBrainctl(["doctor", "--config", missing], { HOME: dir });
      expect(result.code).not.toBe(0);
      const output = `${result.stdout}\n${result.stderr}`;
      expect(output).toContain(`Brainstack config not found: ${missing}`);
      expect(output).toContain(`brainctl provision --profile control --out ${missing}`);
      expect(output).toContain(`brainctl doctor --config ${candidate}`);
      expect(output).not.toContain("ENOENT");
      const clientResult = runBrainctl(["doctor", "--profile", "client-macos", "--config", missing], { HOME: dir });
      expect(clientResult.code).not.toBe(0);
      const clientOutput = `${clientResult.stdout}\n${clientResult.stderr}`;
      expect(clientOutput).toContain(`brainctl enroll --invite-file /path/to/invite.txt --config ${missing}`);
      expect(clientOutput).toContain(`brainctl provision --profile client-macos --out ${missing}`);
      expect(clientOutput).not.toContain("brainctl provision --profile control");
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
