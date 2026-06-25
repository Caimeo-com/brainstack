import { expect, test } from "bun:test";
import { CodexProgressLineParser, parseCodexProgressLine, safeProgressCommandLabel } from "../src/harness-progress";

test("parseCodexProgressLine maps Codex JSONL lifecycle and item events to safe progress events", () => {
  expect(parseCodexProgressLine('{"type":"session.started","session_id":"abc123"}')).toEqual([
    { kind: "session", sessionId: "abc123" }
  ]);
  expect(parseCodexProgressLine('{"type":"turn.started"}')).toEqual([{ kind: "turn_started" }]);
  expect(
    parseCodexProgressLine('{"type":"item.started","item":{"type":"command_execution","command":"bun test --timeout 30000"}}')
  ).toEqual([{ kind: "command_started", command: "bun test --timeout 30000" }]);
  expect(
    parseCodexProgressLine('{"type":"item.completed","item":{"type":"file_change","path":"apps/telemux/src/workers.ts","action":"updated"}}')
  ).toEqual([{ kind: "file_changed", path: "apps/telemux/src/workers.ts", action: "updated" }]);
  expect(parseCodexProgressLine('{"type":"turn.completed","usage":{"input_tokens":1}}')).toEqual([{ kind: "turn_completed" }]);
});

test("CodexProgressLineParser handles JSONL split across stdout chunks", () => {
  const parser = new CodexProgressLineParser();

  expect(parser.push('{"type":"turn.st')).toEqual([]);
  expect(parser.push('arted"}\n{"type":"item.started","item":{"type":"command_execution","command":["git","status"]}}\n')).toEqual([
    { kind: "turn_started" },
    { kind: "command_started", command: "git status" }
  ]);
  expect(parser.flush()).toEqual([]);
});

test("progress command labels hide likely secret-bearing commands", () => {
  expect(safeProgressCommandLabel("curl -H 'Authorization: Bearer secret' https://example.com")).toBe(
    "shell command (details hidden)"
  );
  expect(safeProgressCommandLabel("bun test --timeout 30000")).toBe("bun test --timeout 30000");
});
