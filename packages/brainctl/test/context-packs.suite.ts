import { existsSync } from "node:fs";
import { describe, expect, join, mkdir, mkdtemp, readFile, rm, runBrainctl, stat, symlink, test, tmpdir, utimes, writeExecutable, writeFile } from "./helpers";

async function writeConfig(path: string, home: string, stateRoot: string, controlRoot?: string): Promise<void> {
  await writeFile(
    path,
    [
      "schema_version: 1",
      "profile: single-node",
      "machine:",
      "  name: brain-control",
      "  user: operator",
      "paths:",
      `  home: ${home}`,
      `  stateRoot: ${stateRoot}`,
      "brain:",
      "  publicBaseUrl: https://brain-control.example.ts.net",
      "telemux:",
      "  enabled: true",
      ...(controlRoot ? [`  controlRoot: ${controlRoot}`] : []),
      ""
    ].join("\n")
  );
}

describe("brainctl context-packs", () => {
  test("stores, lists, refreshes, attaches, detaches, and deletes a local folder pack", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-context-pack-"));
    try {
      const configPath = join(dir, "config.yaml");
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      const controlRoot = join(dir, "telemux");
      const source = join(dir, "source");
      await mkdir(join(source, "docs"), { recursive: true });
      await writeFile(join(source, "docs", "guide.md"), "hello\n");
      await writeConfig(configPath, home, stateRoot, controlRoot);

      const put = runBrainctl(["context-packs", "put", "--config", configPath, "--machine", "brain-control", "--name", "docs", "--dir", source, "--json"]);
      expect(put.code).toBe(0);
      const putJson = JSON.parse(put.stdout) as { pack: { content_path: string; manifest_path: string; tree_sha256: string; file_count: number; freshness: string; free_space_bytes: number | null } };
      expect(putJson.pack.file_count).toBe(1);
      expect(putJson.pack.freshness).toBe("fresh");
      expect(typeof putJson.pack.free_space_bytes === "number" || putJson.pack.free_space_bytes === null).toBe(true);
      expect(await readFile(join(putJson.pack.content_path, "docs", "guide.md"), "utf8")).toBe("hello\n");
      expect((await stat(putJson.pack.manifest_path)).mode & 0o777).toBe(0o600);

      await writeFile(join(source, "docs", "guide.md"), "hello again\n");
      const sync = runBrainctl(["context-packs", "sync", "--config", configPath, "--machine", "brain-control", "--name", "docs", "--json"]);
      expect(sync.code).toBe(0);
      const syncJson = JSON.parse(sync.stdout) as { pack: { content_path: string; previous_tree_sha256: string; tree_sha256: string; changed_since_previous: boolean } };
      expect(syncJson.pack.previous_tree_sha256).toBe(putJson.pack.tree_sha256);
      expect(syncJson.pack.tree_sha256).not.toBe(putJson.pack.tree_sha256);
      expect(syncJson.pack.changed_since_previous).toBe(true);
      expect(await readFile(join(syncJson.pack.content_path, "docs", "guide.md"), "utf8")).toBe("hello again\n");

      const fixedTime = new Date("2026-07-08T00:00:00Z");
      await writeFile(join(source, "docs", "guide.md"), "same-a\n");
      await utimes(join(source, "docs", "guide.md"), fixedTime, fixedTime);
      const sameA = runBrainctl(["context-packs", "sync", "--config", configPath, "--machine", "brain-control", "--name", "docs", "--json"]);
      expect(sameA.code).toBe(0);
      const sameAJson = JSON.parse(sameA.stdout) as { pack: { tree_sha256: string; content_path: string } };
      await writeFile(join(source, "docs", "guide.md"), "same-b\n");
      await utimes(join(source, "docs", "guide.md"), fixedTime, fixedTime);
      const sameB = runBrainctl(["context-packs", "sync", "--config", configPath, "--machine", "brain-control", "--name", "docs", "--json"]);
      expect(sameB.code).toBe(0);
      const sameBJson = JSON.parse(sameB.stdout) as { pack: { tree_sha256: string; content_path: string } };
      expect(sameBJson.pack.tree_sha256).not.toBe(sameAJson.pack.tree_sha256);
      expect(await readFile(join(sameBJson.pack.content_path, "docs", "guide.md"), "utf8")).toBe("same-b\n");

      await rm(join(source, "docs", "guide.md"));
      await writeFile(join(source, "docs", "new.md"), "replacement\n");
      const deleteSync = runBrainctl(["context-packs", "sync", "--config", configPath, "--machine", "brain-control", "--name", "docs", "--json"]);
      expect(deleteSync.code).toBe(0);
      const deleteJson = JSON.parse(deleteSync.stdout) as { pack: { content_path: string } };
      expect(existsSync(join(deleteJson.pack.content_path, "docs", "guide.md"))).toBe(false);
      expect(await readFile(join(deleteJson.pack.content_path, "docs", "new.md"), "utf8")).toBe("replacement\n");

      const list = runBrainctl(["context-packs", "list", "--config", configPath, "--machine", "brain-control", "--json"]);
      expect(list.code).toBe(0);
      const listJson = JSON.parse(list.stdout) as { packs: Array<{ safe_name: string }> };
      expect(listJson.packs.map((pack) => pack.safe_name)).toEqual(["docs"]);

      const attach = runBrainctl(["context-packs", "attach", "--config", configPath, "--context", "topic", "--machine", "brain-control", "--name", "docs", "--json"]);
      expect(attach.code).toBe(0);
      expect(JSON.parse(attach.stdout).packs).toHaveLength(1);
      expect(await readFile(join(controlRoot, "contexts", "topic", "context-packs.json"), "utf8")).toContain("\"safe_name\": \"docs\"");

      const orphanRoot = join(stateRoot, "context-packs", "orphan");
      await mkdir(orphanRoot, { recursive: true });
      await writeFile(join(orphanRoot, "manifest.json"), JSON.stringify({
        schema_version: 1,
        kind: "brainstack.context_pack",
        id: "cp_orphan",
        name: "orphan",
        safe_name: "orphan",
        machine: "brain-control",
        source_machine: "brain-control",
        source_root: source,
        pack_root: orphanRoot,
        content_path: join(orphanRoot, "current"),
        manifest_path: join(orphanRoot, "manifest.json"),
        tree_path: join(orphanRoot, "tree.jsonl"),
        include: [],
        exclude: [],
        file_count: 0,
        total_bytes: 0,
        largest_files: [],
        excluded_count: 0,
        sensitive_count: 0,
        tree_sha256: "orphan",
        previous_tree_sha256: null,
        changed_since_previous: false,
        freshness: "unknown",
        free_space_bytes: null,
        warnings: [],
        refreshed_at: "2026-07-08T00:00:00.000Z"
      }, null, 2));
      const gc = runBrainctl(["context-packs", "gc", "--config", configPath, "--machine", "brain-control", "--yes", "--json"]);
      expect(gc.code).toBe(0);
      const gcJson = JSON.parse(gc.stdout) as { deleted: string[]; protected: string[] };
      expect(gcJson.deleted).toEqual(["orphan"]);
      expect(gcJson.protected).toContain("docs");
      expect(existsSync(join(stateRoot, "context-packs", "docs"))).toBe(true);
      expect(existsSync(orphanRoot)).toBe(false);

      const detach = runBrainctl(["context-packs", "detach", "--config", configPath, "--context", "topic", "--name", "docs", "--json"]);
      expect(detach.code).toBe(0);
      expect(JSON.parse(detach.stdout).packs).toEqual([]);

      const reattach = runBrainctl(["context-packs", "attach", "--config", configPath, "--context", "topic", "--machine", "brain-control", "--name", "docs", "--json"]);
      expect(reattach.code).toBe(0);
      const remove = runBrainctl(["context-packs", "rm", "--config", configPath, "--machine", "brain-control", "--name", "docs", "--json"]);
      expect(remove.code).toBe(0);
      expect(JSON.parse(remove.stdout).detached_contexts).toEqual(["topic"]);
      expect(JSON.parse(await readFile(join(controlRoot, "contexts", "topic", "context-packs.json"), "utf8")).packs).toEqual([]);
      const after = runBrainctl(["context-packs", "list", "--config", configPath, "--machine", "brain-control", "--json"]);
      expect(after.code).toBe(0);
      expect(JSON.parse(after.stdout).packs).toEqual([]);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("skips sensitive files by default and reports the warning", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-context-pack-sensitive-"));
    try {
      const configPath = join(dir, "config.yaml");
      const source = join(dir, "source");
      await mkdir(source, { recursive: true });
      await writeFile(join(source, "README.md"), "safe\n");
      await writeFile(join(source, ".env"), "TOKEN=secret\n");
      await writeFile(join(source, "api-token.txt"), "secret\n");
      await writeFile(join(source, "id_ed25519"), "secret\n");
      await writeFile(join(source, "session.json"), "secret\n");
      await writeFile(join(source, "client.p12"), "secret\n");
      await writeConfig(configPath, join(dir, "home"), join(dir, "state"));

      const put = runBrainctl(["context-packs", "put", "--config", configPath, "--machine", "brain-control", "--name", "safe", "--dir", source, "--json"]);
      expect(put.code).toBe(0);
      const parsed = JSON.parse(put.stdout) as { pack: { content_path: string; sensitive_count: number; warnings: string[]; file_count: number } };
      expect(parsed.pack.file_count).toBe(1);
      expect(parsed.pack.sensitive_count).toBe(5);
      expect(parsed.pack.warnings.join("\n")).toContain("sensitive-looking");
      expect(await readFile(join(parsed.pack.content_path, "README.md"), "utf8")).toBe("safe\n");
      expect(existsSync(join(parsed.pack.content_path, ".env"))).toBe(false);
      expect(existsSync(join(parsed.pack.content_path, "api-token.txt"))).toBe(false);
      expect(existsSync(join(parsed.pack.content_path, "id_ed25519"))).toBe(false);
      expect(existsSync(join(parsed.pack.content_path, "session.json"))).toBe(false);
      expect(existsSync(join(parsed.pack.content_path, "client.p12"))).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("rejects symlinked files before syncing", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-context-pack-symlink-"));
    try {
      const configPath = join(dir, "config.yaml");
      const source = join(dir, "source");
      await mkdir(source, { recursive: true });
      await writeFile(join(dir, "target.txt"), "secret-ish\n");
      await symlink(join(dir, "target.txt"), join(source, "link.txt"));
      await writeConfig(configPath, join(dir, "home"), join(dir, "state"));

      const result = runBrainctl(["context-packs", "put", "--config", configPath, "--machine", "brain-control", "--name", "bad", "--dir", source]);
      expect(result.code).not.toBe(0);
      expect(result.stderr).toContain("contains a symlink");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("dry-run preflight does not write a pack copy", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-context-pack-dry-run-"));
    try {
      const configPath = join(dir, "config.yaml");
      const source = join(dir, "source");
      await mkdir(source, { recursive: true });
      await writeFile(join(source, "README.md"), "safe\n");
      await writeConfig(configPath, join(dir, "home"), join(dir, "state"));

      const result = runBrainctl(["context-packs", "put", "--config", configPath, "--machine", "brain-control", "--name", "dry", "--dir", source, "--dry-run", "--json"]);
      expect(result.code).toBe(0);
      const parsed = JSON.parse(result.stdout) as { dry_run: boolean; pack: { content_path: string; freshness: string } };
      expect(parsed.dry_run).toBe(true);
      expect(parsed.pack.freshness).toBe("unknown");
      expect(existsSync(parsed.pack.content_path)).toBe(false);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("detach is machine-aware when names overlap", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-context-pack-detach-"));
    try {
      const configPath = join(dir, "config.yaml");
      const controlRoot = join(dir, "telemux");
      await writeConfig(configPath, join(dir, "home"), join(dir, "state"), controlRoot);
      expect(runBrainctl(["context-packs", "attach", "--config", configPath, "--context", "topic", "--machine", "brain-control", "--name", "docs", "--json"]).code).toBe(0);
      expect(runBrainctl(["context-packs", "attach", "--config", configPath, "--context", "topic", "--machine", "erbine", "--name", "docs", "--json"]).code).toBe(0);

      const ambiguous = runBrainctl(["context-packs", "detach", "--config", configPath, "--context", "topic", "--name", "docs", "--json"]);
      expect(ambiguous.code).not.toBe(0);
      expect(ambiguous.stderr).toContain("multiple context packs named docs");

      const detach = runBrainctl(["context-packs", "detach", "--config", configPath, "--context", "topic", "--machine", "erbine", "--name", "docs", "--json"]);
      expect(detach.code).toBe(0);
      const packs = JSON.parse(detach.stdout).packs as Array<{ machine: string; safe_name: string }>;
      expect(packs).toHaveLength(1);
      expect(packs[0]).toMatchObject({ safe_name: "docs", machine: "brain-control" });
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("remote sync writes metadata to raw remote paths without double quoting", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-context-pack-remote-"));
    try {
      const binDir = join(dir, "bin");
      const source = join(dir, "source");
      const sshLog = join(dir, "ssh.log");
      const rsyncLog = join(dir, "rsync.log");
      const configPath = join(dir, "config.yaml");
      await mkdir(binDir, { recursive: true });
      await mkdir(source, { recursive: true });
      await writeFile(join(source, "README.md"), "remote pack\n");
      await writeExecutable(
        join(binDir, "ssh"),
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          `log=${JSON.stringify(sshLog)}`,
          "printf '<%s>\\n' \"$@\" >> \"$log\"",
          "printf -- '---\\n' >> \"$log\"",
          'last="${@: -1}"',
          'if [[ "$last" == *"df -Pk"* ]]; then',
          "  printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\\n'",
          "  printf 'fake 1000000 1 999999 1%% /\\n'",
          "fi",
          ""
        ].join("\n")
      );
      await writeExecutable(
        join(binDir, "rsync"),
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          `log=${JSON.stringify(rsyncLog)}`,
          "printf '<%s>\\n' \"$@\" >> \"$log\"",
          ""
        ].join("\n")
      );
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: brain-control",
          "  user: operator",
          "paths:",
          `  home: ${join(dir, "home")}`,
          `  stateRoot: ${join(dir, "state")}`,
          "brain:",
          "  publicBaseUrl: https://brain-control.example.ts.net",
          "telemux:",
          "  enabled: true",
          "  workers:",
          "    - name: erbine",
          "      transport: ssh",
          "      sshTarget: erbine",
          ""
        ].join("\n")
      );

      const result = runBrainctl(["context-packs", "put", "--config", configPath, "--machine", "erbine", "--name", "remote-docs", "--dir", source, "--json"], {
        PATH: `${binDir}:${process.env.PATH || ""}`
      });

      expect(result.code).toBe(0);
      const parsed = JSON.parse(result.stdout) as { pack: { content_path: string; freshness: string } };
      expect(parsed.pack.content_path).toBe("~/.local/state/brainstack/context-packs/remote-docs/current");
      expect(parsed.pack.freshness).toBe("fresh");
      const log = await readFile(sshLog, "utf8");
      expect(log).toContain("brainstack_expand_home");
      expect(log).toContain("~/.local/state/brainstack/context-packs/remote-docs/manifest.json");
      expect(log).toContain("~/.local/state/brainstack/context-packs/remote-docs/tree.jsonl");
      expect(log).not.toContain("brainstack_expand_home ''\\''~/.local");
      expect(await readFile(rsyncLog, "utf8")).toContain("erbine:~/.local/state/brainstack/context-packs/remote-docs/current/");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
