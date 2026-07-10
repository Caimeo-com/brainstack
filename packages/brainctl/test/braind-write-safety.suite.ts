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

describe("braind write safety", () => {
  test("operational routine evidence stays auditable without entering proposal generation", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-operational-evidence-"));
    const port = 40_000 + Math.floor(Math.random() * 3_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port);
      const request = async (path: string, method = "GET", body?: Record<string, unknown>, token = "import-test-token", key?: string) =>
        await fetch(`http://127.0.0.1:${port}${path}`, {
          method,
          headers: {
            ...(method === "POST" ? { Authorization: `Bearer ${token}`, "Content-Type": "application/json" } : {}),
            ...(key ? { "Idempotency-Key": key } : {})
          },
          body: body ? JSON.stringify(body) : undefined
        });

      const imported = await request(
        "/api/import",
        "POST",
        {
          title: "brainstack routine receipt: brain-curator",
          text: "The built-in curator completed. This is execution metadata only.",
          source_harness: "telemux",
          source_machine: "control",
          source_type: "telemux-run",
          conversation_id: "brainstack-routines",
          run_origin: "scheduled",
          routine_name: "brain-curator",
          routine_job_id: "cron-curator",
          scheduled_for: "2026-07-11T06:30:00Z",
          tags: ["telemux", "factory-run", "builtin-routine", "operational-receipt"]
        },
        "import-test-token",
        "operational-import"
      );
      expect(imported.status).toBe(200);
      const importedBody = (await imported.json()) as Record<string, unknown>;
      expect(importedBody.curation_disposition).toBe("audit-only");
      const operationalId = String(importedBody.artifact_id);

      const normalImport = await request(
        "/api/import",
        "POST",
        {
          title: "Useful project lesson",
          text: "A useful scoped project lesson that remains eligible for curation.",
          source_harness: "codex",
          source_machine: "worker",
          source_type: "remember",
          tags: ["remember"]
        },
        "import-test-token",
        "candidate-import"
      );
      expect(normalImport.status).toBe(200);
      const candidateId = String(((await normalImport.json()) as Record<string, unknown>).artifact_id);

      const inbox = (await (await request("/api/curator/inbox?limit=10")).json()) as Record<string, unknown>;
      expect(inbox.eligible_count).toBe(1);
      expect(inbox.audit_only_count).toBe(1);
      expect((inbox.excluded as Array<Record<string, unknown>>)[0]).toMatchObject({ id: operationalId, disposition: "audit-only" });
      const cliEnv = { ...braindTestEnv(root, port), BRAIN_BASE_URL: `http://127.0.0.1:${port}` };
      const cliInbox = runBrainctl(["curator", "inbox", "--config", join(dir, "config.yaml"), "--limit", "10", "--json"], cliEnv);
      expectSuccess(cliInbox);
      expect(JSON.parse(cliInbox.stdout).audit_only_count).toBe(1);

      const proposal = (sourceIds: string[], allow = false) => ({
        title: "Operational evidence proposal",
        body: "This proposal exists only to test deterministic evidence admission policy.",
        source_harness: "codex",
        source_machine: "control",
        source_ids: sourceIds,
        ...(allow ? { allow_audit_only_sources: true } : {})
      });
      const blocked = await request("/api/propose", "POST", proposal([operationalId]), "import-test-token", "operational-blocked");
      expect(blocked.status).toBe(422);
      expect(String(((await blocked.json()) as Record<string, unknown>).error)).toContain("operational audit receipts");

      const unauthorizedOverride = await request("/api/propose", "POST", proposal([operationalId], true), "import-test-token", "operational-override-denied");
      expect(unauthorizedOverride.status).toBe(403);

      const mixed = await request("/api/propose", "POST", proposal([operationalId, candidateId]), "import-test-token", "operational-mixed");
      expect(mixed.status).toBe(200);

      const overridden = await request("/api/propose", "POST", proposal([operationalId], true), "admin-test-token", "operational-override");
      expect(overridden.status).toBe(200);
      const overriddenId = String(((await overridden.json()) as Record<string, unknown>).proposal_id);
      const overriddenShown = (await (await request(`/api/proposals/${encodeURIComponent(overriddenId)}`)).json()) as Record<string, unknown>;
      expect((overriddenShown.proposal as Record<string, unknown>).tags).toContain("audit-only-source-override");

      const legacyId = "legacy-operational-proposal";
      const staging = join(root, "shared-brain", "staging", "shared-brain");
      await writeFile(
        join(staging, "proposals", "pending", `${legacyId}.md`),
        [
          "---",
          "title: Legacy operational proposal",
          "type: proposal",
          `proposal_id: ${legacyId}`,
          "created_at: 2026-07-10T00:00:00Z",
          "updated_at: 2026-07-10T00:00:00Z",
          "status: pending",
          "tags:",
          "  - proposal",
          "source_ids:",
          `  - ${operationalId}`,
          "---",
          "",
          "# Legacy operational proposal",
          "",
          "This old proposal records only that a built-in routine ran.",
          ""
        ].join("\n"),
        "utf8"
      );
      git(["add", `proposals/pending/${legacyId}.md`], staging);
      git(["commit", "-m", "test: add legacy operational proposal"], staging);
      git(["push", "origin", "HEAD"], staging);

      const dryRun = (await (await request("/api/curator/operational-backfill?limit=20")).json()) as Record<string, unknown>;
      expect(dryRun.dry_run).toBe(true);
      expect(dryRun.matched).toBe(1);
      expect((dryRun.proposals as Array<Record<string, unknown>>).map((item) => item.id)).toEqual([legacyId]);
      const cliBackfill = runBrainctl(["curator", "backfill-operational", "--config", join(dir, "config.yaml"), "--limit", "20", "--json"], cliEnv);
      expectSuccess(cliBackfill);
      expect(JSON.parse(cliBackfill.stdout).matched).toBe(1);

      const applied = await request(
        "/api/curator/operational-backfill",
        "POST",
        { limit: 20, decided_by: "test-backfill" },
        "admin-test-token"
      );
      expect(applied.status).toBe(200);
      expect(((await applied.json()) as Record<string, unknown>).superseded).toEqual([legacyId]);
      const shown = (await (await request(`/api/proposals/${encodeURIComponent(legacyId)}`)).json()) as Record<string, unknown>;
      expect((shown.proposal as Record<string, unknown>).status).toBe("superseded");
      const overrideStillOpen = (await (await request(`/api/proposals/${encodeURIComponent(overriddenId)}`)).json()) as Record<string, unknown>;
      expect((overrideStillOpen.proposal as Record<string, unknown>).status).toBe("pending");
    } finally {
      if (proc) await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  }, 30_000);

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

  test("staging dirty check ignores Brainstack's own repo lock only", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-lock-sync-"));
    try {
      const bare = join(dir, "shared-brain.git");
      const staging = join(dir, "staging");

      git(["init", "--bare", "--initial-branch=main", bare], dir);
      git(["clone", bare, staging], dir);
      await writeFile(join(staging, "README.md"), "# Shared Brain\n");
      git(["add", "README.md"], staging);
      git(["-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "-m", "seed"], staging);
      git(["push", "-u", "origin", "main"], staging);

      await withRepoLock(staging, async () => {
        await syncWritableRepoAsync(staging);
      });
      expect(git(["status", "--short"], staging)).toBe("");
      expect(existsSync(join(staging, ".shared-brain.lock"))).toBe(false);

      await writeFile(join(staging, "dirty.md"), "real dirt\n");
      await expect(withRepoLock(staging, async () => syncWritableRepoAsync(staging))).rejects.toThrow(
        /Writable repo is dirty/
      );
      expect(existsSync(join(staging, ".shared-brain.lock"))).toBe(false);
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

  test("import returns client errors for malformed payloads and normalizes multipart media type", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-import-client-errors-"));
    const port = 37_000 + Math.floor(Math.random() * 2_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port);
      const endpoint = `http://127.0.0.1:${port}/api/import`;
      const headers = {
        Authorization: "Bearer import-test-token",
        "Content-Type": "application/json"
      };

      const arrayBody = await fetch(endpoint, {
        method: "POST",
        headers,
        body: JSON.stringify(["not", "an", "object"])
      });
      expect(arrayBody.status).toBe(400);
      expect(await arrayBody.text()).toContain("Request body must be a JSON object");

      const missingPayload = await fetch(endpoint, {
        method: "POST",
        headers,
        body: JSON.stringify({
          source_harness: "test-harness",
          source_machine: "test-machine",
          source_type: "note"
        })
      });
      expect(missingPayload.status).toBe(400);
      expect(await missingPayload.text()).toContain("Import request must include text, url, or multipart file");

      const boundary = "----BrainstackCaseInsensitive";
      const multipartBody = [
        `--${boundary}`,
        'Content-Disposition: form-data; name="title"',
        "",
        "Multipart import",
        `--${boundary}`,
        'Content-Disposition: form-data; name="source_harness"',
        "",
        "test-harness",
        `--${boundary}`,
        'Content-Disposition: form-data; name="source_machine"',
        "",
        "test-machine",
        `--${boundary}`,
        'Content-Disposition: form-data; name="source_type"',
        "",
        "multipart",
        `--${boundary}`,
        'Content-Disposition: form-data; name="file"; filename="note.txt"',
        "Content-Type: text/plain",
        "",
        "hello from multipart",
        `--${boundary}--`,
        ""
      ].join("\r\n");
      const multipart = await fetch(endpoint, {
        method: "POST",
        headers: {
          Authorization: "Bearer import-test-token",
          "Content-Type": `Multipart/Form-Data; boundary=${boundary}`
        },
        body: multipartBody
      });
      expect(multipart.status).toBe(200);
      expect(await multipart.text()).toContain("artifact_id");
    } finally {
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  }, 15_000);

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
      const firstBody = (await first.json()) as Record<string, unknown>;
      expect(firstBody.error).toBe("Internal server error");
      expect(typeof firstBody.request_id).toBe("string");
      expect(JSON.stringify(firstBody)).not.toContain(staging);
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
  }, 30_000);

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
  }, 30_000);

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
  }, 30_000);

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
  }, 30_000);

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
  }, 30_000);

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

  test("proposal lifecycle: machine proposals, approval, drift, supersede, reject, and curator status", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-proposal-lifecycle-"));
    const port = 44_000 + Math.floor(Math.random() * 4_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port);
      const staging = join(root, "shared-brain", "staging", "shared-brain");
      const targetPage = "wiki/Status/Curation-Lifecycle.md";
      const proposedContent = "---\ntitle: Curation Lifecycle\n---\n\n# Curation Lifecycle\n\n- first applied fact\n";

      const propose = async (payload: Record<string, unknown>, key: string) =>
        await fetch(`http://127.0.0.1:${port}/api/propose`, {
          method: "POST",
          headers: {
            Authorization: "Bearer import-test-token",
            "Content-Type": "application/json",
            "Idempotency-Key": key
          },
          body: JSON.stringify(payload)
        });
      const decide = async (id: string, action: string, body: Record<string, unknown> = {}, token = "admin-test-token") =>
        await fetch(`http://127.0.0.1:${port}/api/proposals/${encodeURIComponent(id)}/${action}`, {
          method: "POST",
          headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
          body: JSON.stringify(body)
        });

      // Machine proposal in approval mode (default): stored pending, never auto-applied.
      const first = await propose(
        {
          title: "Curation Lifecycle",
          body: "Track the lifecycle test status.",
          source_harness: "test-harness",
          source_machine: "test-machine",
          target_page: targetPage,
          proposed_content: proposedContent,
          base_sha256: "absent",
          risk: "low",
          confidence: 0.9,
          curator_run_id: "curator-run-1",
          source_ids: ["art-test-1"]
        },
        "proposal-lifecycle-1"
      );
      expect(first.status).toBe(200);
      const firstBody = (await first.json()) as Record<string, unknown>;
      expect(firstBody.auto_applied).toBe(false);
      expect(firstBody.status).toBe("pending");
      const firstId = String(firstBody.proposal_id);

      // Listed as open; show returns a diff.
      const openList = (await (await fetch(`http://127.0.0.1:${port}/api/proposals?status=open`)).json()) as {
        proposals: Array<Record<string, unknown>>;
        mode: string;
      };
      expect(openList.mode).toBe("approval");
      expect(openList.proposals.some((proposal) => proposal.id === firstId)).toBe(true);
      const shown = (await (await fetch(`http://127.0.0.1:${port}/api/proposals/${firstId}`)).json()) as Record<string, unknown>;
      expect(String(shown.diff)).toContain("+ - first applied fact");
      const shownProposal = shown.proposal as Record<string, unknown>;
      expect(shownProposal.quality_decision).toBe("ready");
      expect(typeof shownProposal.quality_score).toBe("number");

      // Long proposal titles must not create IDs that the detail/decision APIs
      // reject. New IDs are capped, while already-created long path-safe IDs
      // remain addressable so old queues do not become unrecoverable.
      const longTitle = `Remember: ${"Slack EA Chief of Staff channel routing investigation ".repeat(8)}`;
      const longTitleProposal = await propose(
        {
          title: longTitle,
          body: "Long title proposals should still produce bounded path-safe ids that can be listed and shown.",
          source_harness: "codex",
          source_machine: "test-machine",
          source_type: "remember",
          related_repo: "/work/lindy-debug",
          project: "lindy-debug",
          domain: "slack-ea",
          scope: "repo",
          memory_kind: "project_lesson",
          applicability: "Use when reviewing long remembered investigation titles.",
          evidence_refs: ["repo:/work/lindy-debug"]
        },
        "proposal-lifecycle-long-title"
      );
      expect(longTitleProposal.status).toBe(200);
      const longTitleBody = (await longTitleProposal.json()) as Record<string, unknown>;
      const longTitleId = String(longTitleBody.proposal_id);
      expect(longTitleId.length).toBeLessThanOrEqual(128);
      const longTitleShow = await fetch(`http://127.0.0.1:${port}/api/proposals/${encodeURIComponent(longTitleId)}`);
      expect(longTitleShow.status).toBe(200);

      const existingLongId =
        "20260618t173847z-remember-lindy-debug-slack-ea-chief-of-staff-channel-routing-investigation-live-slack-to-imessage-timer-sends-currently-work-by-storing-the";
      expect(existingLongId.length).toBeGreaterThan(128);
      await writeFile(
        join(staging, "proposals", "pending", `${existingLongId}.md`),
        [
          "---",
          "title: Existing Long Proposal",
          "type: proposal",
          `proposal_id: ${existingLongId}`,
          "created_at: 2026-06-18T17:38:47Z",
          "updated_at: 2026-06-18T17:38:47Z",
          "status: pending",
          "tags:",
          "  - proposal",
          "---",
          "",
          "# Existing Long Proposal",
          "",
          "This fixture simulates a proposal created before proposal IDs were capped.",
          ""
        ].join("\n"),
        "utf8"
      );
      git(["add", `proposals/pending/${existingLongId}.md`], staging);
      git(["commit", "-m", "test: add legacy long proposal"], staging);
      git(["push", "origin", "HEAD"], staging);
      const existingLongShow = await fetch(`http://127.0.0.1:${port}/api/proposals/${encodeURIComponent(existingLongId)}`);
      expect(existingLongShow.status).toBe(200);
      const existingLongReject = await decide(existingLongId, "reject", { decided_by: "tester", reason: "legacy long id remains addressable" });
      expect(existingLongReject.status).toBe(200);

      // Memory-shaped proposals without scope/applicability context are parked for
      // human review instead of entering the normal approval queue as vague canon.
      const vagueMemory = await propose(
        {
          title: "Remember: Slack EA open-loop buttons",
          body: "Slack EA open-loop interactivity should replace stale ephemeral buttons after terminal actions.",
          source_harness: "codex",
          source_machine: "test-machine",
          source_type: "remember",
          tags: ["remember"]
        },
        "proposal-lifecycle-vague-memory"
      );
      expect(vagueMemory.status).toBe(200);
      const vagueMemoryBody = (await vagueMemory.json()) as Record<string, unknown>;
      expect(vagueMemoryBody.status).toBe("needs-human");
      const vagueShown = (await (await fetch(`http://127.0.0.1:${port}/api/proposals/${vagueMemoryBody.proposal_id}`)).json()) as Record<string, unknown>;
      const vagueProposal = vagueShown.proposal as Record<string, unknown>;
      expect(vagueProposal.quality_decision).toBe("needs-context");
      expect(vagueProposal.quality_reasons).toContain("missing project, domain, related repo, or target page context");

      // A structured memory proposal remains pending and carries review metadata.
      const structuredMemory = await propose(
        {
          title: "Remember (lindy): Slack EA open-loop buttons",
          body: "When working on Slack EA terminal action handling, replace the original ephemeral button offer with visible terminal status so the user does not see stale controls after state changes.",
          source_harness: "codex",
          source_machine: "test-machine",
          source_type: "remember",
          tags: ["remember", "lindy", "project_lesson", "repo"],
          related_repo: "/work/lindy",
          project: "lindy",
          domain: "slack-ea",
          scope: "repo",
          memory_kind: "project_lesson",
          context: "Captured during Slack EA hardening.",
          applicability: "Use when working on Slack EA terminal actions or ephemeral button replacement.",
          non_applicability: "Do not apply to unrelated Slack apps without checking their interaction response contract.",
          evidence_refs: ["repo:/work/lindy", "default:raw/slack-ea.md:12"],
          confidence: 0.9
        },
        "proposal-lifecycle-structured-memory"
      );
      expect(structuredMemory.status).toBe(200);
      const structuredMemoryBody = (await structuredMemory.json()) as Record<string, unknown>;
      expect(structuredMemoryBody.status).toBe("pending");
      const structuredShown = (await (await fetch(`http://127.0.0.1:${port}/api/proposals/${structuredMemoryBody.proposal_id}`)).json()) as Record<string, unknown>;
      const structuredProposal = structuredShown.proposal as Record<string, unknown>;
      expect(structuredProposal.project).toBe("lindy");
      expect(structuredProposal.scope).toBe("repo");
      expect(structuredProposal.memory_kind).toBe("project_lesson");
      expect(structuredProposal.quality_decision).toBe("ready");
      expect(structuredProposal.evidence_refs).toEqual(["repo:/work/lindy", "default:raw/slack-ea.md:12"]);
      expect(String(structuredShown.body)).toContain("## Quality Gate");

      // Decisions require admin auth; the import token is insufficient.
      const unauthorized = await decide(firstId, "approve", {}, "import-test-token");
      expect(unauthorized.status).toBe(403);

      const malformedDecisionCandidate = await propose(
        {
          title: "Malformed Decision Candidate",
          body: "This should remain pending after malformed admin JSON.",
          source_harness: "test-harness",
          source_machine: "test-machine"
        },
        "proposal-lifecycle-malformed-decision"
      );
      const malformedDecisionId = String(((await malformedDecisionCandidate.json()) as Record<string, unknown>).proposal_id);
      const malformedDecision = await fetch(`http://127.0.0.1:${port}/api/proposals/${encodeURIComponent(malformedDecisionId)}/reject`, {
        method: "POST",
        headers: { Authorization: "Bearer admin-test-token", "Content-Type": "application/json" },
        body: "{not-json"
      });
      expect(malformedDecision.status).toBe(400);
      expect(existsSync(join(staging, "proposals", "pending", `${malformedDecisionId}.md`))).toBe(true);
      expect(existsSync(join(staging, "proposals", "rejected", `${malformedDecisionId}.md`))).toBe(false);

      const approved = await decide(firstId, "approve", { decided_by: "tester" });
      expect(approved.status).toBe(200);
      expect(((await approved.json()) as Record<string, unknown>).status).toBe("approved");

      const applied = await decide(firstId, "apply", { decided_by: "tester" });
      expect(applied.status).toBe(200);
      const appliedBody = (await applied.json()) as Record<string, unknown>;
      expect(appliedBody.status).toBe("applied");
      expect(await readFile(join(staging, targetPage), "utf8")).toBe(proposedContent);
      expect(existsSync(join(staging, "proposals", "applied", `${firstId}.md`))).toBe(true);
      expect(existsSync(join(staging, "proposals", "pending", `${firstId}.md`))).toBe(false);

      // A stale proposal (base says the page is absent, but it now exists) is parked
      // as needs-human instead of clobbering the page.
      const stale = await propose(
        {
          title: "Stale Curation Update",
          body: "Generated against an outdated base.",
          source_harness: "test-harness",
          source_machine: "test-machine",
          target_page: targetPage,
          proposed_content: "# Stale\n",
          base_sha256: "absent",
          risk: "low"
        },
        "proposal-lifecycle-stale"
      );
      const staleId = String(((await stale.json()) as Record<string, unknown>).proposal_id);
      const staleApply = await decide(staleId, "apply", { decided_by: "tester" });
      expect(staleApply.status).toBe(200);
      const staleApplyBody = (await staleApply.json()) as Record<string, unknown>;
      expect(staleApplyBody.applied).toBe(false);
      expect(staleApplyBody.status).toBe("needs-human");
      expect(await readFile(join(staging, targetPage), "utf8")).toBe(proposedContent);

      // Applying a fresh proposal for the same target supersedes the parked one.
      const currentSha = createHash("sha256").update(proposedContent).digest("hex");
      const fresh = await propose(
        {
          title: "Fresh Curation Update",
          body: "Generated against the current base.",
          source_harness: "test-harness",
          source_machine: "test-machine",
          target_page: targetPage,
          proposed_content: `${proposedContent}- second applied fact\n`,
          base_sha256: currentSha,
          risk: "low"
        },
        "proposal-lifecycle-fresh"
      );
      const freshId = String(((await fresh.json()) as Record<string, unknown>).proposal_id);
      const freshApply = await decide(freshId, "apply", { decided_by: "tester" });
      const freshApplyBody = (await freshApply.json()) as Record<string, unknown>;
      expect(freshApplyBody.applied).toBe(true);
      expect(freshApplyBody.superseded_ids).toContain(staleId);
      expect(existsSync(join(staging, "proposals", "superseded", `${staleId}.md`))).toBe(true);

      // Reject moves proposals to proposals/rejected with a reason.
      const rejectable = await propose(
        {
          title: "Rejectable Proposal",
          body: "Should be rejected.",
          source_harness: "test-harness",
          source_machine: "test-machine"
        },
        "proposal-lifecycle-reject"
      );
      const rejectableId = String(((await rejectable.json()) as Record<string, unknown>).proposal_id);
      const rejected = await decide(rejectableId, "reject", { decided_by: "tester", reason: "not canon-worthy" });
      expect(((await rejected.json()) as Record<string, unknown>).status).toBe("rejected");
      expect(existsSync(join(staging, "proposals", "rejected", `${rejectableId}.md`))).toBe(true);
      const rejectedList = (await (await fetch(`http://127.0.0.1:${port}/api/proposals?status=rejected`)).json()) as {
        proposals: Array<Record<string, unknown>>;
      };
      expect(rejectedList.proposals.some((proposal) => proposal.id === rejectableId)).toBe(true);

      // Needs-work keeps the proposal open while persisting operator feedback for
      // a later enrichment/curator pass.
      const revisable = await propose(
        {
          title: "Proposal That Needs Context",
          body: "The idea is useful but too broad.",
          source_harness: "test-harness",
          source_machine: "test-machine"
        },
        "proposal-lifecycle-needs-work"
      );
      const revisableId = String(((await revisable.json()) as Record<string, unknown>).proposal_id);
      const needsWork = await decide(revisableId, "needs-work", { decided_by: "tester", reason: "narrow this to the repo" });
      const needsWorkBody = (await needsWork.json()) as Record<string, unknown>;
      expect(needsWorkBody.status).toBe("needs-human");
      const needsWorkList = (await (await fetch(`http://127.0.0.1:${port}/api/proposals?status=needs-human`)).json()) as {
        proposals: Array<Record<string, unknown>>;
      };
      const needsWorkProposal = needsWorkList.proposals.find((proposal) => proposal.id === revisableId);
      expect(needsWorkProposal?.reason).toBe("narrow this to the repo");

      // Detail responses enrich proposal references with human-reviewable source
      // titles, provenance, and excerpts.
      const sourced = await propose(
        {
          title: "Sourced Consolidation",
          body: "Review the captured source.",
          source_harness: "test-harness",
          source_machine: "test-machine",
          evidence_refs: [`proposal:${revisableId}`]
        },
        "proposal-lifecycle-source-evidence"
      );
      const sourcedId = String(((await sourced.json()) as Record<string, unknown>).proposal_id);
      const sourcedDetail = (await (await fetch(`http://127.0.0.1:${port}/api/proposals/${sourcedId}`)).json()) as {
        source_proposals: Array<Record<string, unknown>>;
      };
      expect(sourcedDetail.source_proposals).toHaveLength(1);
      expect(sourcedDetail.source_proposals[0]?.id).toBe(revisableId);
      expect(String(sourcedDetail.source_proposals[0]?.excerpt || "")).toContain("useful but too broad");

      // Supersede marks source candidates as absorbed by a better proposal; this is
      // distinct from rejection because the source remains provenance for a merge.
      const absorbable = await propose(
        {
          title: "Absorbable Memory Candidate",
          body: "This candidate should be absorbed by a consolidated memory card.",
          source_harness: "test-harness",
          source_machine: "test-machine",
          source_type: "remember",
          project: "brainstack",
          domain: "operator",
          scope: "repo",
          memory_kind: "project_lesson"
        },
        "proposal-lifecycle-supersede"
      );
      const absorbableId = String(((await absorbable.json()) as Record<string, unknown>).proposal_id);
      const superseded = await decide(absorbableId, "supersede", { decided_by: "tester", reason: "absorbed into consolidated-card" });
      const supersededBody = (await superseded.json()) as Record<string, unknown>;
      expect(supersededBody.status).toBe("superseded");
      expect(existsSync(join(staging, "proposals", "superseded", `${absorbableId}.md`))).toBe(true);
      const supersededList = (await (await fetch(`http://127.0.0.1:${port}/api/proposals?status=superseded`)).json()) as {
        proposals: Array<Record<string, unknown>>;
      };
      expect(supersededList.proposals.some((proposal) => proposal.id === absorbableId && proposal.reason === "absorbed into consolidated-card")).toBe(true);
      const mixedOpenList = await fetch(`http://127.0.0.1:${port}/api/proposals?status=open,superseded`);
      expect(mixedOpenList.status).toBe(200);
      const mixedOpenBody = (await mixedOpenList.json()) as { proposals: Array<Record<string, unknown>> };
      expect(mixedOpenBody.proposals.some((proposal) => proposal.id === absorbableId)).toBe(true);

      // needs-human can be set at proposal time.
      const parked = await propose(
        {
          title: "Contradictory Material",
          body: "Curator could not resolve a contradiction.",
          source_harness: "test-harness",
          source_machine: "test-machine",
          status: "needs-human"
        },
        "proposal-lifecycle-needs-human"
      );
      expect(((await parked.json()) as Record<string, unknown>).status).toBe("needs-human");

      // Curator status: admin-only POST, public GET, persisted fields.
      const statusBefore = (await (await fetch(`http://127.0.0.1:${port}/api/curator/status`)).json()) as Record<string, unknown>;
      expect(((statusBefore.curator || {}) as Record<string, unknown>).installed).toBe(false);
      const statusUnauthorized = await fetch(`http://127.0.0.1:${port}/api/curator/status`, {
        method: "POST",
        headers: { Authorization: "Bearer import-test-token", "Content-Type": "application/json" },
        body: JSON.stringify({ installed: true })
      });
      expect(statusUnauthorized.status).toBe(403);
      const statusUpdate = await fetch(`http://127.0.0.1:${port}/api/curator/status`, {
        method: "POST",
        headers: { Authorization: "Bearer admin-test-token", "Content-Type": "application/json" },
        body: JSON.stringify({
          installed: true,
          last_run_id: "curator-run-1",
          last_run_ok: true,
          last_run_finished_at: "2026-06-11T00:00:00Z",
          next_run_at: "2026-06-12T06:30:00Z",
          cursor: "2026-06-11T00:00:00Z"
        })
      });
      expect(statusUpdate.status).toBe(200);
      const statusAfter = (await (await fetch(`http://127.0.0.1:${port}/api/curator/status`)).json()) as Record<string, unknown>;
      const curator = (statusAfter.curator || {}) as Record<string, unknown>;
      expect(curator.installed).toBe(true);
      expect(curator.last_run_ok).toBe(true);
      expect(curator.cursor).toBe("2026-06-11T00:00:00Z");
      const counts = (statusAfter.proposal_counts || {}) as Record<string, number>;
      expect(counts.applied).toBe(2);
      expect(counts.rejected).toBe(2);
      expect(counts.superseded).toBe(2);

      // Wiki home shows the curation panel.
      const home = await (await fetch(`http://127.0.0.1:${port}/`)).text();
      expect(home).toContain("Curation");
      expect(home).toContain('id="curation"');
      expect(home).toContain("Mode:");

      // Unsafe target pages are rejected before any side effect.
      const badTarget = await propose(
        {
          title: "Bad Target",
          body: "Path escape attempt.",
          source_harness: "test-harness",
          source_machine: "test-machine",
          target_page: "wiki/../secrets.md",
          proposed_content: "# nope\n"
        },
        "proposal-lifecycle-bad-target"
      );
      expect(badTarget.status).toBe(400);

      // Legacy title/body-only remember proposals are not silently treated as
      // ordinary pending canon candidates. They are synthesized as needs-human
      // and grouped into deterministic review-group hints for batch review. This check
      // runs after mutating lifecycle assertions because these direct fixtures are
      // intentionally uncommitted legacy artifacts.
      const legacyDir = join(staging, "proposals", "pending");
      await writeFile(
        join(legacyDir, "20260611t000001z-remember-slack-ea-buttons-a.md"),
        [
          "---",
          "title: 'Remember: Slack EA buttons should not stay stale'",
          "status: pending",
          "legacy_format: true",
          "source_type: remember",
          "memory_kind: legacy_memory",
          "quality_decision: needs-context",
          "created_at: 2026-06-11T00:00:01Z",
          "updated_at: 2026-06-11T00:00:01Z",
          "---",
          "",
          "# Remember: Slack EA buttons should not stay stale",
          "- Source harness: `codex`",
          "- Source machine: `mac`",
          "## Request",
          "",
          "Slack EA buttons should be replaced after terminal action completion.",
          ""
        ].join("\n")
      );
      await writeFile(
        join(legacyDir, "20260611t000002z-remember-slack-ea-buttons-b.md"),
        [
          "---",
          "title: 'Remember: Slack EA buttons need terminal status'",
          "status: pending",
          "source_type: remember",
          "created_at: 2026-06-11T00:00:02Z",
          "updated_at: 2026-06-11T00:00:02Z",
          "---",
          "",
          "# Remember: Slack EA buttons need terminal status",
          "- Source harness: `codex`",
          "- Source machine: `mac`",
          "## Request",
          "",
          "Slack EA buttons should show terminal status after completion.",
          ""
        ].join("\n")
      );
      const needsHumanList = (await (await fetch(`http://127.0.0.1:${port}/api/proposals?status=needs-human`)).json()) as {
        proposals: Array<Record<string, unknown>>;
      };
      const legacyProposal = needsHumanList.proposals.find((proposal) => String(proposal.id).includes("remember-slack-ea-buttons-a"));
      expect(legacyProposal?.status).toBe("needs-human");
      expect(legacyProposal?.legacy_format).toBe(true);
      expect(legacyProposal?.quality_decision).toBe("needs-context");
      expect(legacyProposal?.cluster_label).toBe("Slack EA Buttons / needs-context / legacy_memory");
      const groups = (await (await fetch(`http://127.0.0.1:${port}/api/proposals/groups?status=open&min_size=2`)).json()) as {
        review_groups: Array<Record<string, unknown>>;
      };
      const legacyCluster = groups.review_groups.find((cluster) => String(cluster.id).startsWith("slack-ea-buttons:"));
      expect(legacyCluster?.count).toBe(2);
      expect(legacyCluster?.legacyCount).toBe(2);
      expect(legacyCluster?.needsContextCount).toBe(2);
    } finally {
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  }, 30_000);

  test("auto curation mode applies only low-risk policy-conforming proposals", async () => {
    const dir = await mkdtemp(join(tmpdir(), "braind-auto-curation-"));
    const port = 44_000 + Math.floor(Math.random() * 4_000);
    let proc: ReturnType<typeof Bun.spawn> | null = null;
    try {
      const root = await createSingleNodeInstall(dir);
      proc = await startBraind(root, port, {
        BRAIN_CURATION_MODE: "auto",
        BRAIN_CURATION_ALLOWED_PATHS: "wiki/Status/**,wiki/Sources/**",
        BRAIN_CURATION_MAX_CHANGED_LINES: "40",
        BRAIN_CURATION_ALLOW_DELETES: "0"
      });
      const staging = join(root, "shared-brain", "staging", "shared-brain");
      const propose = async (payload: Record<string, unknown>, key: string) =>
        await fetch(`http://127.0.0.1:${port}/api/propose`, {
          method: "POST",
          headers: {
            Authorization: "Bearer import-test-token",
            "Content-Type": "application/json",
            "Idempotency-Key": key
          },
          body: JSON.stringify(payload)
        });

      // Low-risk additive change inside allowedPaths auto-applies.
      const allowed = await propose(
        {
          title: "Auto Status",
          body: "Low-risk status update.",
          source_harness: "test-harness",
          source_machine: "test-machine",
          target_page: "wiki/Status/Auto-Status.md",
          proposed_content: "# Auto Status\n\n- machine ok\n",
          base_sha256: "absent",
          risk: "low",
          confidence: 0.95
        },
        "auto-curation-allowed"
      );
      const allowedBody = (await allowed.json()) as Record<string, unknown>;
      expect(allowedBody.auto_applied).toBe(true);
      expect(allowedBody.status).toBe("applied");
      expect(await readFile(join(staging, "wiki", "Status", "Auto-Status.md"), "utf8")).toBe("# Auto Status\n\n- machine ok\n");

      // Medium/high risk stays pending with explicit reasons.
      const risky = await propose(
        {
          title: "Risky Change",
          body: "High-risk change must not auto-apply.",
          source_harness: "test-harness",
          source_machine: "test-machine",
          target_page: "wiki/Status/Risky.md",
          proposed_content: "# Risky\n",
          base_sha256: "absent",
          risk: "high"
        },
        "auto-curation-risky"
      );
      const riskyBody = (await risky.json()) as Record<string, unknown>;
      expect(riskyBody.auto_applied).toBe(false);
      expect(riskyBody.status).toBe("pending");
      expect(JSON.stringify(riskyBody.auto_apply_reasons)).toContain("risk");
      expect(existsSync(join(staging, "wiki", "Status", "Risky.md"))).toBe(false);

      // Low risk outside allowedPaths stays pending.
      const outside = await propose(
        {
          title: "Outside Allowed Paths",
          body: "Low risk but not in the allowlist.",
          source_harness: "test-harness",
          source_machine: "test-machine",
          target_page: "wiki/Decisions/Outside.md",
          proposed_content: "# Outside\n",
          base_sha256: "absent",
          risk: "low"
        },
        "auto-curation-outside"
      );
      const outsideBody = (await outside.json()) as Record<string, unknown>;
      expect(outsideBody.auto_applied).toBe(false);
      expect(JSON.stringify(outsideBody.auto_apply_reasons)).toContain("allowedPaths");
      expect(existsSync(join(staging, "wiki", "Decisions", "Outside.md"))).toBe(false);

      // Oversized diffs stay pending even when low risk and inside allowedPaths.
      const bigContent = `# Big\n\n${Array.from({ length: 60 }, (_, index) => `- line ${index}`).join("\n")}\n`;
      const big = await propose(
        {
          title: "Big Change",
          body: "Too many changed lines.",
          source_harness: "test-harness",
          source_machine: "test-machine",
          target_page: "wiki/Status/Big.md",
          proposed_content: bigContent,
          base_sha256: "absent",
          risk: "low"
        },
        "auto-curation-big"
      );
      const bigBody = (await big.json()) as Record<string, unknown>;
      expect(bigBody.auto_applied).toBe(false);
      expect(JSON.stringify(bigBody.auto_apply_reasons)).toContain("maxChangedLines");
    } finally {
      await stopBraind(proc);
      await rm(dir, { recursive: true, force: true });
    }
  }, 30_000);

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
  }, 30_000);
});
