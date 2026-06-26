import { describe, expect, join, mkdir, mkdtemp, readFile, rm, runBrainctl, stat, symlink, test, tmpdir, writeExecutable, writeFile, writeFixtureClientConfig } from "./helpers";

describe("brainctl uploads", () => {
  test("stores, lists, and deletes local uploads with private file modes", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-uploads-"));
    try {
      const configPath = join(dir, "config.yaml");
      const home = join(dir, "home");
      const stateRoot = join(dir, "state");
      await writeFile(
        configPath,
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
          "  enabled: false",
          ""
        ].join("\n")
      );
      const source = join(dir, "runbook.txt");
      await writeFile(source, "runbook contents\n");

      const put = runBrainctl(["uploads", "put", "--config", configPath, "--machine", "brain-control", "--file", source, "--label", "fixture", "--json"]);
      expect(put.code).toBe(0);
      const putJson = JSON.parse(put.stdout) as {
        ok: boolean;
        upload: {
          id: string;
          machine: string;
          file_name: string;
          label: string;
          size_bytes: number;
          remote_path: string;
          manifest_path: string;
          sha256: string;
        };
      };
      expect(putJson.ok).toBe(true);
      expect(putJson.upload.machine).toBe("brain-control");
      expect(putJson.upload.file_name).toBe("runbook.txt");
      expect(putJson.upload.label).toBe("fixture");
      expect(putJson.upload.size_bytes).toBe("runbook contents\n".length);
      expect(await readFile(putJson.upload.remote_path, "utf8")).toBe("runbook contents\n");
      expect((await stat(putJson.upload.remote_path)).mode & 0o777).toBe(0o600);
      expect((await stat(putJson.upload.manifest_path)).mode & 0o777).toBe(0o600);

      const list = runBrainctl(["uploads", "list", "--config", configPath, "--machine", "brain-control", "--recent", "--json"]);
      expect(list.code).toBe(0);
      const listJson = JSON.parse(list.stdout) as { uploads: Array<{ id: string; remote_path: string }> };
      expect(listJson.uploads.map((upload) => upload.id)).toContain(putJson.upload.id);
      expect(listJson.uploads[0].remote_path).toBe(putJson.upload.remote_path);

      const remove = runBrainctl(["uploads", "rm", "--config", configPath, "--machine", "brain-control", "--id", putJson.upload.id, "--json"]);
      expect(remove.code).toBe(0);
      const removeJson = JSON.parse(remove.stdout) as { ok: boolean; deleted: boolean };
      expect(removeJson.ok).toBe(true);
      expect(removeJson.deleted).toBe(true);

      const after = runBrainctl(["uploads", "list", "--config", configPath, "--machine", "brain-control", "--json"]);
      expect(after.code).toBe(0);
      expect((JSON.parse(after.stdout) as { uploads: unknown[] }).uploads).toEqual([]);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("rejects symlink uploads", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-upload-symlink-"));
    try {
      const configPath = join(dir, "config.yaml");
      const target = join(dir, "target.txt");
      const link = join(dir, "link.txt");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: single-node",
          "machine:",
          "  name: brain-control",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${join(dir, "state")}`,
          ""
        ].join("\n")
      );
      await writeFile(target, "secret-ish\n");
      await symlink(target, link);
      const result = runBrainctl(["uploads", "put", "--config", configPath, "--file", link]);
      expect(result.code).not.toBe(0);
      expect(result.stderr).toContain("regular non-symlink file");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("lists pretty remote upload manifests from worker storage", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-upload-remote-list-"));
    try {
      const binDir = join(dir, "bin");
      const remoteHome = join(dir, "remote-home");
      const manifestDir = join(remoteHome, ".local", "state", "brainstack", "uploads", "2026-06-26", "up_20260626T120000Z_fixture");
      await mkdir(binDir, { recursive: true });
      await mkdir(manifestDir, { recursive: true });
      await writeExecutable(
        join(binDir, "ssh"),
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          'last="${@: -1}"',
          'eval "$last"',
          ""
        ].join("\n")
      );
      await writeFile(
        join(manifestDir, "manifest.json"),
        JSON.stringify(
          {
            schema_version: 1,
            id: "up_20260626T120000Z_fixture",
            machine: "erbine",
            source: "client-macos",
            original_name: "runbook.zip",
            file_name: "runbook.zip",
            label: "runbook",
            size_bytes: 1234,
            sha256: "a".repeat(64),
            uploaded_at: "2026-06-26T12:00:00.000Z",
            remote_path: "~/.local/state/brainstack/uploads/2026-06-26/up_20260626T120000Z_fixture/runbook.zip",
            manifest_path: "~/.local/state/brainstack/uploads/2026-06-26/up_20260626T120000Z_fixture/manifest.json"
          },
          null,
          2
        ) + "\n"
      );
      const configPath = join(dir, "config.yaml");
      await writeFile(
        configPath,
        [
          "schema_version: 1",
          "profile: control",
          "machine:",
          "  name: valkyrie",
          "  user: operator",
          "paths:",
          `  home: ${dir}`,
          `  stateRoot: ${join(dir, "state")}`,
          "brain:",
          "  publicBaseUrl: https://valkyrie.example.ts.net",
          "telemux:",
          "  enabled: true",
          "  workers:",
          "    - name: erbine",
          "      transport: ssh",
          "      sshTarget: erbine",
          ""
        ].join("\n")
      );

      const list = runBrainctl(["uploads", "list", "--config", configPath, "--machine", "erbine", "--json"], {
        PATH: `${binDir}:${process.env.PATH || ""}`,
        HOME: remoteHome
      });
      expect(list.code).toBe(0);
      const listJson = JSON.parse(list.stdout) as { uploads: Array<{ id: string; file_name: string; label: string }> };
      expect(listJson.uploads).toHaveLength(1);
      expect(listJson.uploads[0]).toMatchObject({
        id: "up_20260626T120000Z_fixture",
        file_name: "runbook.zip",
        label: "runbook"
      });
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  test("client remote uploads explain when the control host is too old", async () => {
    const dir = await mkdtemp(join(tmpdir(), "brainctl-upload-old-control-"));
    try {
      const binDir = join(dir, "bin");
      await writeFile(join(dir, "source.txt"), "remote upload smoke\n");
      await mkdir(binDir, { recursive: true });
      const callsPath = join(dir, "ssh-calls.txt");
      await writeExecutable(
        join(binDir, "ssh"),
        [
          "#!/usr/bin/env bash",
          "set -euo pipefail",
          `calls=${JSON.stringify(callsPath)}`,
          "count=0",
          "[ -f \"$calls\" ] && count=\"$(cat \"$calls\")\"",
          "count=$((count + 1))",
          "printf '%s' \"$count\" > \"$calls\"",
          "if [[ \"$count\" == \"1\" ]]; then",
          "  cat >/dev/null",
          "  exit 0",
          "fi",
          "printf 'Unknown command: uploads\\nUsage: old brainctl\\n' >&2",
          "exit 1",
          ""
        ].join("\n")
      );
      const configPath = join(dir, "client.yaml");
      await writeFixtureClientConfig(configPath, {
        home: join(dir, "home"),
        stateRoot: join(dir, "state"),
        telegramVia: "operator@control.example",
        telegramRemoteRepo: "/home/operator/brainstack"
      });

      const result = runBrainctl(
        [
          "uploads",
          "put",
          "--config",
          configPath,
          "--machine",
          "erbine",
          "--file",
          join(dir, "source.txt"),
          "--ssh-trust",
          "accept-new"
        ],
        { PATH: `${binDir}:${process.env.PATH || ""}` }
      );

      expect(result.code).not.toBe(0);
      expect(result.stderr).toContain("control host Brainstack is too old for uploads");
      expect(result.stderr).not.toContain("Usage: old brainctl");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
