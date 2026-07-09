// Keep the integration suites behind one Bun test entrypoint. These tests mutate
// process-wide environment and fake runtime paths, so cross-file parallelism
// makes otherwise valid install/proposal flows flaky.
import { test, expect } from "bun:test";
import { readdirSync } from "node:fs";

const suiteFiles = [
  "config.suite.ts",
  "install-safety.suite.ts",
  "braind-write-safety.suite.ts",
  "uploads.suite.ts",
  "context-packs.suite.ts",
  "public-release-bootstrap.suite.ts",
  "public-release-context-outbox.suite.ts",
  "public-release-fleet-invite.suite.ts"
];

for (const suiteFile of suiteFiles) {
  await import(`./${suiteFile}`);
}

test("brainctl test entrypoint imports every suite file", () => {
  const actual = readdirSync(import.meta.dir).filter((name) => name.endsWith(".suite.ts")).sort();
  expect(suiteFiles.toSorted()).toEqual(actual);
});
