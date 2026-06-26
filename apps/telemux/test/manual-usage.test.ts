import { describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import type { ContextRecord } from "../src/db";
import { summarizeManualUsage } from "../src/usage/manual";

describe("manual usage summary", () => {
  test("formats Telegram footer as a compact human usage summary", async () => {
    const dir = await mkdtemp(join(tmpdir(), "telemux-usage-"));
    try {
      const logPath = join(dir, "codex.jsonl");
      await writeFile(
        logPath,
        `${JSON.stringify({
          type: "turn.completed",
          usage: {
            input_tokens: 11_659_667,
            cached_input_tokens: 10_871_296,
            output_tokens: 62_138
          }
        })}\n`
      );

      const summary = await summarizeManualUsage({ latestRunLogPath: logPath } as ContextRecord);

      expect(summary.footer).toBe("11.72M tok (93% cached, 788k fresh in, 62k out)");
      expect(summary.footer).not.toContain("input_tokens");
      expect(summary.footer).not.toContain("cached=");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
