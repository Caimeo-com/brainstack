import { rebuildIndex } from "./brain-lib";

const repoRoot = process.argv[2];
if (!repoRoot) {
  console.error("usage: reindex-worker <repo-root>");
  process.exit(2);
}

const result = await rebuildIndex(repoRoot);
console.log(JSON.stringify(result));
