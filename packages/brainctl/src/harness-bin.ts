import { accessSync, closeSync, constants, existsSync, openSync, readSync, realpathSync, statSync } from "node:fs";
import { join, resolve } from "node:path";

export type HarnessBinRisk = "stable" | "unstable-wrapper" | "missing";

export interface HarnessBinCandidate {
  path: string;
  realPath: string;
  risk: HarnessBinRisk;
  reason: string | null;
}

export interface HarnessBinResolution {
  requested: string;
  resolved: string | null;
  risk: HarnessBinRisk;
  reason: string | null;
  candidates: HarnessBinCandidate[];
  skippedUnstable: HarnessBinCandidate[];
}

function expandHomePath(path: string, home = process.env.HOME || ""): string {
  if (path === "~") return home || path;
  if (path.startsWith("~/")) return home ? join(home, path.slice(2)) : path;
  return path;
}

function executable(path: string): boolean {
  try {
    accessSync(path, constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function uniquePush(items: string[], item: string): void {
  if (!items.includes(item)) items.push(item);
}

function executableCandidates(name: string, searchPath: string): string[] {
  const candidates: string[] = [];
  for (const dir of searchPath.split(":")) {
    if (!dir.trim()) continue;
    const candidate = join(dir, name);
    if (executable(candidate)) uniquePush(candidates, candidate);
  }
  return candidates;
}

const UNSTABLE_WRAPPER_PATTERNS: Array<{ pattern: RegExp; reason: string }> = [
  { pattern: /(^|[^A-Za-z0-9_])npx([^A-Za-z0-9_]|$)/, reason: "npx wrapper" },
  { pattern: /(^|[^A-Za-z0-9_])npm\s+(?:exec|x|install|run|--yes)([^A-Za-z0-9_]|$)/, reason: "npm wrapper" },
  { pattern: /(^|[^A-Za-z0-9_])mise\s+/, reason: "mise wrapper" },
  { pattern: /(^|[^A-Za-z0-9_])bunx([^A-Za-z0-9_]|$)/, reason: "bunx wrapper" },
  { pattern: /(^|[^A-Za-z0-9_])pnpm\s+(?:dlx|exec)([^A-Za-z0-9_]|$)/, reason: "pnpm wrapper" },
  { pattern: /(^|[^A-Za-z0-9_])yarn\s+(?:dlx|exec)([^A-Za-z0-9_]|$)/, reason: "yarn wrapper" }
];

function readFileSample(path: string, maxBytes: number): Buffer {
  const fd = openSync(path, "r");
  try {
    const buffer = Buffer.alloc(maxBytes);
    const bytesRead = readSync(fd, buffer, 0, maxBytes, 0);
    return buffer.subarray(0, bytesRead);
  } finally {
    closeSync(fd);
  }
}

export function classifyHarnessBin(path: string): HarnessBinCandidate {
  const realPath = realpathSync(path);
  const stat = statSync(realPath);
  if (!stat.isFile()) {
    return { path, realPath, risk: "missing", reason: "not a regular file" };
  }
  const sample = readFileSample(realPath, 8192);
  if (sample.includes(0)) {
    return { path, realPath, risk: "stable", reason: null };
  }
  const text = sample.toString("utf8");
  for (const { pattern, reason } of UNSTABLE_WRAPPER_PATTERNS) {
    if (pattern.test(text)) {
      return { path, realPath, risk: "unstable-wrapper", reason };
    }
  }
  return { path, realPath, risk: "stable", reason: null };
}

export function resolveHarnessBin(requested: string, options: { searchPath?: string; home?: string } = {}): HarnessBinResolution {
  const trimmed = requested.trim();
  const searchPath = options.searchPath ?? process.env.PATH ?? "";
  if (!trimmed) {
    return { requested, resolved: null, risk: "missing", reason: "empty harness binary", candidates: [], skippedUnstable: [] };
  }

  if (trimmed.includes("/")) {
    const absolute = resolve(expandHomePath(trimmed, options.home));
    if (!existsSync(absolute) || !executable(absolute)) {
      return { requested, resolved: null, risk: "missing", reason: `${absolute} is not executable`, candidates: [], skippedUnstable: [] };
    }
    const candidate = classifyHarnessBin(absolute);
    return { requested, resolved: absolute, risk: candidate.risk, reason: candidate.reason, candidates: [candidate], skippedUnstable: [] };
  }

  const candidates = executableCandidates(trimmed, searchPath).map((candidate) => classifyHarnessBin(candidate));
  if (!candidates.length) {
    return { requested, resolved: null, risk: "missing", reason: `${trimmed} not found in PATH`, candidates: [], skippedUnstable: [] };
  }
  const stable = candidates.find((candidate) => candidate.risk === "stable");
  if (stable) {
    return {
      requested,
      resolved: stable.path,
      risk: "stable",
      reason: null,
      candidates,
      skippedUnstable: candidates.slice(0, candidates.indexOf(stable)).filter((candidate) => candidate.risk === "unstable-wrapper")
    };
  }
  const first = candidates[0];
  return {
    requested,
    resolved: first.path,
    risk: first.risk,
    reason: first.reason,
    candidates,
    skippedUnstable: candidates.filter((candidate) => candidate.risk === "unstable-wrapper")
  };
}
