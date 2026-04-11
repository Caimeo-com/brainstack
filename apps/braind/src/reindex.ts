#!/usr/bin/env bun
import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { getRepoRoot, rebuildIndex } from "./brain-lib";

function expandHome(input: string): string {
  if (input === "~") {
    return process.env.HOME || input;
  }
  if (input.startsWith("~/")) {
    return resolve(process.env.HOME || ".", input.slice(2));
  }
  return input;
}

function defaultRepoRoot(): string {
  const home = process.env.HOME || "/home/brainstack";
  const serveClone = resolve(home, "shared-brain", "serve", "shared-brain");
  const legacyLiveClone = resolve(home, "shared-brain", "live", "shared-brain");
  return existsSync(serveClone) ? serveClone : legacyLiveClone;
}

const quiet = process.argv.includes("--quiet");
const repoRoot = getRepoRoot(
  expandHome(
    process.env.SHARED_BRAIN_REPO_ROOT ||
      process.env.BRAINSTACK_SHARED_BRAIN_SERVE_REPO ||
      defaultRepoRoot()
  )
);

const result = await rebuildIndex(repoRoot);
if (!quiet) {
  console.log(JSON.stringify({ ok: true, repo_root: repoRoot, ...result }, null, 2));
}
