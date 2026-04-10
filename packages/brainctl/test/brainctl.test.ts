import { describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { loadConfig, parseSimpleYaml } from "../src/main";

describe("brainctl config", () => {
  test("parses nested yaml with list objects", () => {
    const parsed = parseSimpleYaml(`
profile: control
machine:
  name: valkyrie
tailscale:
  advertiseTags:
    - tag:brain
telemux:
  workers:
    - name: valkyrie
      transport: local
    - name: erbine
      transport: ssh
`);
    expect(parsed.profile).toBe("control");
    expect((parsed.machine as Record<string, unknown>).name).toBe("valkyrie");
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
          "  name: valkyrie",
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
});

