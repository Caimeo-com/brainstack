#!/usr/bin/env bun
import { existsSync, readFileSync, statSync } from "node:fs";
import { chmod, cp, lstat, mkdir, mkdtemp, readFile, readdir, readlink, rm, symlink, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";

type Profile = "single-node" | "control" | "worker" | "client-macos" | "private-journal";
type SeedMode = "empty-only" | "missing" | "force";
type HarnessName = "codex" | "claude";

const CONFIG_SCHEMA_VERSION = 1;
const MIN_BUN_VERSION = "1.3.11";

interface ParsedArgs {
  command: string;
  positional: string[];
  flags: Record<string, string | boolean | string[]>;
}

interface BrainstackConfig {
  schemaVersion: number;
  profile: Profile;
  runtime: {
    bunBin: string;
  };
  harness: {
    name: HarnessName;
    bin: string;
  };
  machine: {
    name: string;
    user: string;
    role: string;
    sshUser: string;
    hostname: string;
  };
  paths: {
    home: string;
    productRepo: string;
    sharedBrainRoot: string;
    privateBrainRoot: string;
    stateRoot: string;
    configRoot: string;
    systemdUserRoot: string;
  };
  brain: {
    bind: string;
    port: number;
    publicBaseUrl: string;
    largeFileThresholdBytes: number;
    enableTelemux: boolean;
  };
  repos: {
    bare: string;
    staging: string;
    serve: string;
    blobs: string;
    remoteSsh: string;
  };
  telemux: {
    enabled: boolean;
    dashboardHost: string;
    dashboardPort: number;
    controlRoot: string;
    factoryRoot: string;
    localMachine: string;
    workers: Array<Record<string, unknown>>;
  };
  tailscale: {
    tailnetHost: string;
    controlTag: string;
    workerTag: string;
    advertiseTags: string[];
    enableSsh: boolean;
  };
  client: {
    localPath: string;
    envPath: string;
    remoteSsh: string;
  };
}

const PRODUCT_ROOT = resolve(import.meta.dir, "..", "..", "..");
const ALLOWED_TOP_LEVEL_CONFIG_KEYS = new Set([
  "schema_version",
  "config_version",
  "profile",
  "runtime",
  "harness",
  "machine",
  "paths",
  "brain",
  "repos",
  "telemux",
  "tailscale",
  "client"
]);

function usage(): string {
  return `Usage:
  brainctl init --profile single-node|control|worker|client-macos --config brainstack.yaml [--dry-run] [--root /tmp/install-root] [--seed-missing|--force-seed]
  brainctl provision --profile single-node|control|worker|client-macos [--out brainstack.yaml] [--harness codex|claude] [--enable-telemux] [--enroll-tailscale] [--tailscale-tag tag:brain] [--brain-base-url URL] [--brain-remote SSH_OR_PATH] [--test-bot]
  brainctl upgrade --config brainstack.yaml [--profile ...] [--dry-run] [--root /tmp/install-root]
  brainctl apply-runtime --config brainstack.yaml [--profile ...] [--dry-run] [--root /tmp/install-root]
  brainctl doctor --config brainstack.yaml [--profile ...]
  brainctl backup --config brainstack.yaml [--out DIR] [--pause-telemux]
  brainctl restore --backup DIR_OR_TGZ --target DIR [--apply]
  brainctl render --config brainstack.yaml --profile ... --out DIR
  brainctl bootstrap-client --profile client-macos --config brainstack.yaml --out DIR
  brainctl join-worker --config brainstack.yaml --worker WORKER_HOST [--ssh-user USER] [--out DIR]
  brainctl rotate-token --kind import|admin|telegram-placeholder --config brainstack.yaml [--env FILE]
  brainctl destroy --config brainstack.yaml [--profile ...] [--dry-run] [--remove-shared-brain] [--remove-private-brain] [--remove-tailscale-serve]
  brainctl migrate-current-install [--out FILE]
  brainctl smoke --profile single-node|control|worker|client-macos --config brainstack.yaml`;
}

function parseArgs(argv: string[]): ParsedArgs {
  const [command = "help", ...rest] = argv;
  const positional: string[] = [];
  const flags: Record<string, string | boolean | string[]> = {};
  for (let index = 0; index < rest.length; index += 1) {
    const item = rest[index];
    if (!item.startsWith("--")) {
      positional.push(item);
      continue;
    }
    const key = item.slice(2);
    const next = rest[index + 1];
    if (!next || next.startsWith("--")) {
      flags[key] = true;
      continue;
    }
    if (flags[key] === undefined) {
      flags[key] = next;
    } else if (Array.isArray(flags[key])) {
      (flags[key] as string[]).push(next);
    } else {
      flags[key] = [String(flags[key]), next];
    }
    index += 1;
  }
  return { command, positional, flags };
}

function flag(args: ParsedArgs, key: string): string | undefined {
  const value = args.flags[key];
  if (Array.isArray(value)) {
    return value[value.length - 1];
  }
  return typeof value === "string" ? value : undefined;
}

function hasFlag(args: ParsedArgs, key: string): boolean {
  return args.flags[key] === true;
}

function boolFlag(args: ParsedArgs, key: string, fallback = false): boolean {
  if (args.flags[key] === true) {
    return true;
  }
  const value = flag(args, key);
  if (value === undefined) {
    return fallback;
  }
  return ["1", "true", "yes", "on"].includes(value.toLowerCase());
}

function expandHome(input: string): string {
  if (input === "~") {
    return process.env.HOME || input;
  }
  if (input.startsWith("~/")) {
    return resolve(process.env.HOME || ".", input.slice(2));
  }
  return input;
}

function abs(input: string): string {
  const expanded = expandHome(input);
  return expanded.startsWith("/") ? expanded : resolve(expanded);
}

function expandWithHome(input: string, home: string): string {
  if (input === "~") {
    return home;
  }
  if (input.startsWith("~/")) {
    return resolve(home, input.slice(2));
  }
  return input;
}

function absWithHome(input: string, home: string): string {
  const expanded = expandWithHome(input, home);
  return expanded.startsWith("/") ? expanded : resolve(expanded);
}

function shellSingleQuote(input: string): string {
  return `'${input.replace(/'/g, "'\\''")}'`;
}

function renderTemplate(text: string, replacements: Record<string, string>): string {
  let rendered = text;
  for (const [key, value] of Object.entries(replacements)) {
    rendered = rendered.replaceAll(`__${key}__`, value);
  }
  return rendered;
}

function readClientBootstrapTemplate(path: string): string {
  return readFileSync(join(PRODUCT_ROOT, "packages", "client-bootstrap", path), "utf8");
}

function normalizeHarness(value: string | undefined, fallback: HarnessName = "codex"): HarnessName {
  const normalized = (value || fallback).trim().toLowerCase();
  if (normalized === "codex" || normalized === "claude") {
    return normalized;
  }
  throw new Error(`Unsupported harness: ${value}. Expected codex or claude.`);
}

function splitKeyValue(text: string): [string, string] {
  const index = text.indexOf(":");
  if (index === -1) {
    throw new Error(`Invalid YAML line: ${text}`);
  }
  return [text.slice(0, index).trim(), text.slice(index + 1).trim()];
}

function parseScalar(raw: string): unknown {
  const value = raw.trim();
  if (value === "") {
    return "";
  }
  if (value === "true") {
    return true;
  }
  if (value === "false") {
    return false;
  }
  if (value === "null") {
    return null;
  }
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
    return value.slice(1, -1);
  }
  if (/^-?\d+(\.\d+)?$/.test(value)) {
    return Number(value);
  }
  if (value.startsWith("[") && value.endsWith("]")) {
    return value
      .slice(1, -1)
      .split(",")
      .map((entry) => entry.trim())
      .filter(Boolean)
      .map((entry) => String(parseScalar(entry)));
  }
  return value;
}

export function parseSimpleYaml(text: string): Record<string, unknown> {
  const meaningful = text
    .split(/\r?\n/)
    .map((raw, index) => ({
      raw,
      index,
      indent: raw.match(/^ */)?.[0].length || 0,
      text: raw.trim()
    }))
    .filter((line) => line.text && !line.text.startsWith("#"));

  const root: Record<string, unknown> = {};
  const stack: Array<{ indent: number; container: Record<string, unknown> | unknown[] }> = [
    { indent: -1, container: root }
  ];

  function nextLine(index: number) {
    return meaningful.find((line) => line.index > index);
  }

  for (const line of meaningful) {
    while (stack.length > 1 && line.indent <= stack[stack.length - 1].indent) {
      stack.pop();
    }
    const parent = stack[stack.length - 1].container;

    if (line.text.startsWith("- ")) {
      if (!Array.isArray(parent)) {
        throw new Error(`YAML list item has non-list parent on line ${line.index + 1}`);
      }
      const item = line.text.slice(2).trim();
      if (/^[A-Za-z0-9_-]+:\s/.test(item) || /^[A-Za-z0-9_-]+:$/.test(item)) {
        const object: Record<string, unknown> = {};
        const [key, rawValue] = splitKeyValue(item);
        object[key] = rawValue ? parseScalar(rawValue) : {};
        parent.push(object);
        stack.push({ indent: line.indent, container: object });
      } else {
        parent.push(parseScalar(item));
      }
      continue;
    }

    if (Array.isArray(parent)) {
      throw new Error(`YAML key has list parent on line ${line.index + 1}`);
    }

    const [key, rawValue] = splitKeyValue(line.text);
    if (rawValue) {
      parent[key] = parseScalar(rawValue);
      continue;
    }

    const next = nextLine(line.index);
    const child: Record<string, unknown> | unknown[] =
      next && next.indent > line.indent && next.text.startsWith("- ") ? [] : {};
    parent[key] = child;
    stack.push({ indent: line.indent, container: child });
  }

  return root;
}

function yamlEscape(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((item) => yamlEscape(item)).join(", ")}]`;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  const text = String(value ?? "");
  if (!text || /[:#\[\]{},"\n]/.test(text)) {
    return JSON.stringify(text);
  }
  return text;
}

function stringifySimpleYaml(value: Record<string, unknown>, indent = 0): string {
  const pad = " ".repeat(indent);
  const lines: string[] = [];
  for (const [key, entry] of Object.entries(value)) {
    if (Array.isArray(entry)) {
      lines.push(`${pad}${key}:`);
      for (const item of entry) {
        if (item && typeof item === "object" && !Array.isArray(item)) {
          const [first, ...rest] = Object.entries(item as Record<string, unknown>);
          if (first) {
            lines.push(`${pad}  - ${first[0]}: ${yamlEscape(first[1])}`);
          } else {
            lines.push(`${pad}  - {}`);
          }
          for (const [childKey, childValue] of rest) {
            lines.push(`${pad}    ${childKey}: ${yamlEscape(childValue)}`);
          }
        } else {
          lines.push(`${pad}  - ${yamlEscape(item)}`);
        }
      }
    } else if (entry && typeof entry === "object") {
      lines.push(`${pad}${key}:`);
      lines.push(stringifySimpleYaml(entry as Record<string, unknown>, indent + 2));
    } else {
      lines.push(`${pad}${key}: ${yamlEscape(entry)}`);
    }
  }
  return `${lines.join("\n")}\n`;
}

async function loadRawConfig(path?: string): Promise<Record<string, unknown>> {
  if (!path) {
    return {};
  }
  const text = await readFile(abs(path), "utf8");
  if (path.endsWith(".json")) {
    return JSON.parse(text) as Record<string, unknown>;
  }
  return parseSimpleYaml(text);
}

function objectAt(input: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = input[key];
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function stringAt(input: Record<string, unknown>, key: string, fallback: string): string {
  const value = input[key];
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

function numberAt(input: Record<string, unknown>, key: string, fallback: number): number {
  const value = input[key];
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function booleanAt(input: Record<string, unknown>, key: string, fallback: boolean): boolean {
  const value = input[key];
  return typeof value === "boolean" ? value : fallback;
}

function arrayAt(input: Record<string, unknown>, key: string): unknown[] {
  const value = input[key];
  return Array.isArray(value) ? value : [];
}

function validateRawConfig(raw: Record<string, unknown>): void {
  const unknownKeys = Object.keys(raw).filter((key) => !ALLOWED_TOP_LEVEL_CONFIG_KEYS.has(key));
  if (unknownKeys.length) {
    throw new Error(`Unsupported top-level config keys: ${unknownKeys.join(", ")}`);
  }
  const rawSchemaVersion = raw.schema_version ?? raw.config_version ?? CONFIG_SCHEMA_VERSION;
  if (typeof rawSchemaVersion !== "number" || !Number.isFinite(rawSchemaVersion)) {
    throw new Error("Unsupported config schema version: schema_version must be a number");
  }
  if (rawSchemaVersion !== CONFIG_SCHEMA_VERSION) {
    throw new Error(`Unsupported config schema version ${rawSchemaVersion}; supported version is ${CONFIG_SCHEMA_VERSION}`);
  }
}

function resolveBunBin(): string {
  const proc = run(["bash", "-c", "command -v bun"], { check: false });
  const bunBin = proc.stdout.trim();
  if (proc.code !== 0 || !bunBin) {
    throw new Error("Bun binary not found; install Bun and ensure `command -v bun` works before running brainctl.");
  }
  return bunBin;
}

export async function loadConfig(configPath?: string, profileOverride?: string, rootOverride?: string): Promise<BrainstackConfig> {
  const raw = await loadRawConfig(configPath);
  validateRawConfig(raw);
  const schemaVersion = numberAt(raw, "schema_version", numberAt(raw, "config_version", CONFIG_SCHEMA_VERSION));
  const profile = (profileOverride || stringAt(raw, "profile", "single-node")) as Profile;
  const runtime = objectAt(raw, "runtime");
  const harness = objectAt(raw, "harness");
  const machine = objectAt(raw, "machine");
  const paths = objectAt(raw, "paths");
  const brain = objectAt(raw, "brain");
  const telemux = objectAt(raw, "telemux");
  const tailscale = objectAt(raw, "tailscale");
  const client = objectAt(raw, "client");
  const home = abs(stringAt(paths, "home", process.env.HOME || "."));
  const root = rootOverride ? abs(rootOverride) : "";
  const stateRoot = root ? join(root, "state") : absWithHome(stringAt(paths, "stateRoot", "~/.local/state/brainstack"), home);
  const configRoot = root ? join(root, "config") : absWithHome(stringAt(paths, "configRoot", "~/.config/brainstack"), home);
  const sharedBrainRoot = root ? join(root, "shared-brain") : absWithHome(stringAt(paths, "sharedBrainRoot", "~/shared-brain"), home);
  const productRepo = root ? PRODUCT_ROOT : absWithHome(stringAt(paths, "productRepo", "~/brainstack"), home);
  const systemdUserRoot = root ? join(root, "systemd-user") : absWithHome(stringAt(paths, "systemdUserRoot", "~/.config/systemd/user"), home);
  const machineName = stringAt(machine, "name", profile === "worker" ? "worker-host" : "brain-control");
  const machineUser = stringAt(machine, "user", process.env.USER || "operator");
  const enableTelemux =
    profile === "single-node" || profile === "control"
      ? booleanAt(telemux, "enabled", false)
      : booleanAt(telemux, "enabled", false);
  const controlTag = stringAt(tailscale, "controlTag", "tag:brain");
  const workerTag = stringAt(tailscale, "workerTag", "tag:brain-worker");
  const advertiseTags =
    arrayAt(tailscale, "advertiseTags").map(String).filter(Boolean).length > 0
      ? arrayAt(tailscale, "advertiseTags").map(String)
      : profile === "worker"
        ? [workerTag]
        : profile === "client-macos"
          ? []
          : [controlTag];
  const publicBaseUrl = stringAt(brain, "publicBaseUrl", "");
  const workers = arrayAt(telemux, "workers") as Array<Record<string, unknown>>;
  const remoteSsh = `${machineUser}@${machineName}:${join(sharedBrainRoot, "bare", "shared-brain.git")}`;
  const harnessName = normalizeHarness(stringAt(harness, "name", stringAt(telemux, "harness", "codex")));
  const harnessBin = stringAt(harness, "bin", harnessName);

  return {
    schemaVersion,
    runtime: {
      bunBin: stringAt(runtime, "bunBin", "") || resolveBunBin()
    },
    harness: {
      name: harnessName,
      bin: harnessBin
    },
    profile,
    machine: {
      name: machineName,
      user: machineUser,
      role: stringAt(machine, "role", profile),
      sshUser: stringAt(machine, "sshUser", machineUser),
      hostname: stringAt(machine, "hostname", machineName)
    },
    paths: {
      home,
      productRepo,
      sharedBrainRoot,
      privateBrainRoot: root ? join(root, "private-brain") : absWithHome(stringAt(paths, "privateBrainRoot", "~/private-brain"), home),
      stateRoot,
      configRoot,
      systemdUserRoot
    },
    brain: {
      bind: stringAt(brain, "bind", "127.0.0.1"),
      port: numberAt(brain, "port", 8080),
      publicBaseUrl,
      largeFileThresholdBytes: numberAt(brain, "largeFileThresholdBytes", 10 * 1024 * 1024),
      enableTelemux
    },
    repos: {
      bare: join(sharedBrainRoot, "bare", "shared-brain.git"),
      staging: join(sharedBrainRoot, "staging", "shared-brain"),
      serve: join(sharedBrainRoot, "serve", "shared-brain"),
      blobs: join(stateRoot, "blobs", "shared-brain"),
      remoteSsh
    },
    telemux: {
      enabled: enableTelemux,
      dashboardHost: stringAt(telemux, "dashboardHost", "127.0.0.1"),
      dashboardPort: numberAt(telemux, "dashboardPort", 8787),
      controlRoot: join(stateRoot, "telemux"),
      factoryRoot: join(stateRoot, "factory"),
      localMachine: stringAt(telemux, "localMachine", machineName),
      workers
    },
    tailscale: {
      tailnetHost: stringAt(tailscale, "tailnetHost", publicBaseUrl.replace(/^https?:\/\//, "")),
      controlTag,
      workerTag,
      advertiseTags,
      enableSsh: booleanAt(tailscale, "enableSsh", false)
    },
    client: {
      localPath: stringAt(client, "localPath", "~/shared-brain"),
      envPath: stringAt(client, "envPath", "~/.config/shared-brain.env"),
      remoteSsh: stringAt(client, "remoteSsh", remoteSsh)
    }
  };
}

function run(args: string[], options: { cwd?: string; env?: Record<string, string>; check?: boolean } = {}) {
  const proc = Bun.spawnSync(args, {
    cwd: options.cwd || process.cwd(),
    env: { ...process.env, ...(options.env || {}) },
    stdout: "pipe",
    stderr: "pipe"
  });
  const stdout = proc.stdout.toString();
  const stderr = proc.stderr.toString();
  if (options.check !== false && proc.exitCode !== 0) {
    throw new Error(`${args.join(" ")} failed\n${stderr || stdout}`);
  }
  return { code: proc.exitCode, stdout, stderr };
}

async function ensureDir(path: string): Promise<void> {
  await mkdir(path, { recursive: true });
}

async function writeText(path: string, text: string, mode?: number): Promise<void> {
  await ensureDir(dirname(path));
  await writeFile(path, text, "utf8");
  if (mode !== undefined) {
    await chmod(path, mode);
  }
}

async function writeIfMissing(path: string, text: string, mode?: number): Promise<boolean> {
  if (existsSync(path)) {
    return false;
  }
  await writeText(path, text, mode);
  return true;
}

function token(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return `bs_${Buffer.from(bytes).toString("base64url")}`;
}

function compareVersions(a: string, b: string): number {
  const aa = a.split(".").map((part) => Number(part.replace(/[^0-9].*$/, "")) || 0);
  const bb = b.split(".").map((part) => Number(part.replace(/[^0-9].*$/, "")) || 0);
  for (let index = 0; index < Math.max(aa.length, bb.length); index += 1) {
    const delta = (aa[index] || 0) - (bb[index] || 0);
    if (delta !== 0) {
      return delta > 0 ? 1 : -1;
    }
  }
  return 0;
}

function installedBunVersion(): string | null {
  const proc = run(["bun", "--version"], { check: false });
  return proc.code === 0 ? proc.stdout.trim() : null;
}

function refExists(repo: string, ref: string): boolean {
  return run(["git", "--git-dir", repo, "rev-parse", "--verify", ref], { check: false }).code === 0;
}

function isBareRepoInitialized(repo: string): boolean {
  return existsSync(repo) && refExists(repo, "refs/heads/main");
}

function syncCloneToMain(path: string): void {
  const dirty = run(["git", "status", "--porcelain"], { cwd: path }).stdout.trim();
  if (dirty) {
    throw new Error(`Cannot sync dirty clone: ${path}`);
  }
  run(["git", "fetch", "origin", "main"], { cwd: path });
  run(["git", "checkout", "-f", "main"], { cwd: path });
  run(["git", "merge", "--ff-only", "origin/main"], { cwd: path });
}

function sharedBrainSeedFiles(cfg: BrainstackConfig): Record<string, string> {
  const now = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
  return {
    ".gitignore": [
      "derived/",
      ".shared-brain.lock/",
      ".env",
      ".env.*",
      "!.env.example",
      "*.pem",
      "*.key",
      "*.token",
      "node_modules/",
      ".bun/",
      "local-uploads/",
      ""
    ].join("\n"),
    "AGENTS.md": [
      "# Shared Brain Contract",
      "",
      "This repo is the canonical shared-brain content repo. Product code lives in `~/brainstack`.",
      "",
      "- Canonical data is markdown, manifests, raw artifacts, proposals, and logs in git.",
      "- Derived indexes and caches live under `derived/` and are rebuildable.",
      "- Clients write imports and proposals by default.",
      "- Control/admin actors compile raw evidence into wiki pages through ingest/lint flows.",
      "- Direct wiki edits are trusted power-user operations, not the default client write path.",
      "- Do not commit secrets. Use env var names, secret references, or local secret file paths only.",
      ""
    ].join("\n"),
    "AGENTS.shared-client.md": [
      "# Shared Brain Client Contract",
      "",
      "- The default local clone path is `~/shared-brain`.",
      "- Pull with `git pull --ff-only` before assuming the clone is current.",
      "- Read local markdown first when possible.",
      "- Use `BRAIN_BASE_URL` plus an import/propose token for writes.",
      "- Do not mutate canonical wiki pages directly unless explicitly instructed.",
      "- Import raw artifacts and proposals first; the organizer compiles durable pages.",
      ""
    ].join("\n"),
    "CLAUDE.md": [
      "# Claude",
      "",
      "@AGENTS.md",
      "",
      "Claude-specific delta: be conservative with direct wiki edits and prefer proposals unless acting as organizer/admin.",
      ""
    ].join("\n"),
    "README.md": [
      "# Shared Brain",
      "",
      `Created by brainstack at ${now}.`,
      "",
      "- Browse the wiki from the organizer service.",
      "- Canonical content is this git repo.",
      "- Product code and installers live outside this repo.",
      "",
      "## Layout",
      "",
      "- `wiki/`: human-readable pages.",
      "- `raw/`: append-only artifacts and normalized extracts.",
      "- `manifests/`: machines, harnesses, sources, and secret references.",
      "- `proposals/`: client-submitted page/change proposals.",
      "- `logs/log.md`: parseable operations log.",
      "- `derived/`: local rebuildable indexes, ignored by git.",
      ""
    ].join("\n"),
    "docs/ARCHITECTURE.md": [
      "# Architecture",
      "",
      "The shared brain is a git repository of markdown and manifests. `braind` provides browser, search, import, propose, ingest, and lint APIs. Large binary originals may live in an external blob store while manifests and normalized extracts remain in git.",
      ""
    ].join("\n"),
    "docs/OPERATIONS.md": [
      "# Operations",
      "",
      "- Clients use import/propose tokens only.",
      "- Admin/control hosts keep the admin ingest token local.",
      "- Rebuild search with `bun run ~/brainstack/apps/braind/src/reindex.ts`.",
      "- Back up the bare repo, staging clone, serve clone, blob store, and config env files.",
      ""
    ].join("\n"),
    "docs/API.md": [
      "# API",
      "",
      "- `GET /health`",
      "- `GET /`",
      "- `GET /page/*path`",
      "- `GET /raw/*path`",
      "- `GET /search?q=...&scope=wiki|raw|all`",
      "- `POST /api/import` requires import or admin bearer token.",
      "- `POST /api/propose` requires import or admin bearer token.",
      "- `POST /api/ingest` requires admin bearer token.",
      "- `POST /api/lint` requires admin bearer token.",
      ""
    ].join("\n"),
    "wiki/Home.md": [
      "---",
      "title: Home",
      "type: synthesis",
      `created_at: ${now}`,
      `updated_at: ${now}`,
      "status: active",
      "tags: [home]",
      "aliases: []",
      "source_ids: []",
      "---",
      "",
      "# Home",
      "",
      "Start here. Use [[wiki/Index.md|Index]] for the current map.",
      ""
    ].join("\n"),
    "wiki/Index.md": [
      "---",
      "title: Index",
      "type: synthesis",
      `created_at: ${now}`,
      `updated_at: ${now}`,
      "status: active",
      "tags: [index]",
      "aliases: []",
      "source_ids: []",
      "---",
      "",
      "# Index",
      "",
      "- [[wiki/Home.md|Home]]",
      "- Machines",
      "- Harnesses",
      "- Projects",
      "- Decisions",
      "- Runbooks",
      "- Sources",
      ""
    ].join("\n"),
    "logs/log.md": `# Shared Brain Log\n\n## [${now}] init | brainstack | initial seed\n\n- operation: init\n- inputs: profile=${cfg.profile}; machine=${cfg.machine.name}\n- files: seed repo\n- commit: pending\n- summary: Created initial shared-brain content repo.\n`,
    "raw/inbox/.gitkeep": "",
    "raw/imported/.gitkeep": "",
    "raw/normalized/.gitkeep": "",
    "raw/conversations/.gitkeep": "",
    "raw/assets/.gitkeep": "",
    "proposals/pending/.gitkeep": "",
    "proposals/applied/.gitkeep": "",
    "manifests/machines/.gitkeep": "",
    "manifests/harnesses/.gitkeep": "",
    "manifests/sources/.gitkeep": "",
    "manifests/secret-refs/runtime-env.json": `${JSON.stringify(
      {
        id: "runtime-env",
        type: "secret-refs",
        refs: ["BRAIN_IMPORT_TOKEN", "BRAIN_ADMIN_TOKEN", "BRAIN_BLOB_STORE"],
        notes: "Secret values are local env only and must not be committed."
      },
      null,
      2
    )}\n`,
    "wiki/Decisions/.gitkeep": "",
    "wiki/Projects/.gitkeep": "",
    "wiki/Machines/.gitkeep": "",
    "wiki/Harnesses/.gitkeep": "",
    "wiki/Skills/.gitkeep": "",
    "wiki/Runbooks/.gitkeep": "",
    "wiki/Syntheses/.gitkeep": "",
    "wiki/Sources/.gitkeep": ""
  };
}

function braindRuntimeEnv(cfg: BrainstackConfig): string {
  return [
    `BRAIN_BIND=${cfg.brain.bind}`,
    `BRAIN_PORT=${cfg.brain.port}`,
    `SHARED_BRAIN_REPO_ROOT=${cfg.repos.serve}`,
    `SHARED_BRAIN_WRITE_REPO_ROOT=${cfg.repos.staging}`,
    `BRAIN_BLOB_STORE=${cfg.repos.blobs}`,
    `BRAIN_LARGE_FILE_THRESHOLD_BYTES=${cfg.brain.largeFileThresholdBytes}`,
    "BRAIN_MAX_IMPORT_BYTES=26214400",
    "BRAIN_ALLOW_PRIVATE_URL_IMPORTS=false",
    "BRAIN_URL_FETCH_TIMEOUT_MS=15000",
    `BRAIN_ORGANIZER_LABEL=${cfg.machine.name}`,
    ""
  ].join("\n");
}

function braindSecretsEnv(includeSecrets: boolean): string {
  return [
    `BRAIN_IMPORT_TOKEN=${includeSecrets ? token() : ""}`,
    `BRAIN_ADMIN_TOKEN=${includeSecrets ? token() : ""}`,
    ""
  ].join("\n");
}

function telemuxRuntimeEnv(cfg: BrainstackConfig): string {
  return [
    `FACTORY_DASHBOARD_HOST=${cfg.telemux.dashboardHost}`,
    `FACTORY_DASHBOARD_PORT=${cfg.telemux.dashboardPort}`,
    "FACTORY_TELEGRAM_POLL_TIMEOUT_SECONDS=30",
    "FACTORY_CRON_POLL_INTERVAL_SECONDS=30",
    `FACTORY_LOCAL_MACHINE=${cfg.telemux.localMachine}`,
    `BRAINSTACK_STATE_ROOT=${cfg.paths.stateRoot}`,
    `FACTORY_CONTROL_ROOT=${cfg.telemux.controlRoot}`,
    `FACTORY_FACTORY_ROOT=${cfg.telemux.factoryRoot}`,
    `FACTORY_WORKERS_FILE=${join(cfg.paths.configRoot, "workers.json")}`,
    "FACTORY_USAGE_ADAPTER=manual",
    `FACTORY_HARNESS=${cfg.harness.name}`,
    `FACTORY_HARNESS_BIN=${cfg.harness.bin}`,
    `FACTORY_CODEX_BIN=${cfg.harness.name === "codex" ? cfg.harness.bin : "codex"}`,
    `BRAIN_BASE_URL=${cfg.brain.publicBaseUrl}`,
    ""
  ].join("\n");
}

function telemuxSecretsEnv(): string {
  return [
    "FACTORY_TELEGRAM_BOT_TOKEN=",
    "# FACTORY_TELEGRAM_CONTROL_CHAT_ID=-1001234567890",
    "FACTORY_ALLOWED_TELEGRAM_USER_ID=0",
    "BRAIN_IMPORT_TOKEN=",
    ""
  ].join("\n");
}

function defaultWorkers(cfg: BrainstackConfig): Array<Record<string, unknown>> {
  if (cfg.telemux.workers.length) {
    return cfg.telemux.workers;
  }
  if (cfg.profile === "worker" || cfg.profile === "client-macos") {
    return [];
  }
  return [
    {
      name: cfg.machine.name,
      transport: "local",
      managedRepoRoot: join(cfg.telemux.factoryRoot, "repos"),
      managedHostRoot: join(cfg.telemux.factoryRoot, "hostctx"),
      managedScratchRoot: join(cfg.telemux.factoryRoot, "scratch")
    }
  ];
}

function runsBraind(cfg: BrainstackConfig): boolean {
  return cfg.profile === "single-node" || cfg.profile === "control" || cfg.profile === "private-journal";
}

function usesUserServices(cfg: BrainstackConfig): boolean {
  return cfg.profile === "single-node" || cfg.profile === "control" || cfg.profile === "worker";
}

function braindService(cfg: BrainstackConfig): string {
  return [
    "[Unit]",
    "Description=brainstack braind shared-brain server",
    "After=network-online.target",
    "",
    "[Service]",
    "Type=simple",
    `WorkingDirectory=${cfg.paths.productRepo}`,
    `EnvironmentFile=${join(cfg.paths.configRoot, "braind.runtime.env")}`,
    `EnvironmentFile=${join(cfg.paths.configRoot, "braind.secrets.env")}`,
    `ExecStartPre=${cfg.runtime.bunBin} --no-env-file run ${join(cfg.paths.productRepo, "apps", "braind", "src", "reindex.ts")} --quiet`,
    `ExecStart=${cfg.runtime.bunBin} --no-env-file run ${join(cfg.paths.productRepo, "apps", "braind", "src", "server.ts")}`,
    "Restart=on-failure",
    "RestartSec=5",
    "",
    "[Install]",
    "WantedBy=default.target",
    ""
  ].join("\n");
}

function telemuxService(cfg: BrainstackConfig): string {
  return [
    "[Unit]",
    "Description=brainstack telemux Telegram harness control plane",
    "After=network-online.target",
    "",
    "[Service]",
    "Type=simple",
    `WorkingDirectory=${join(cfg.paths.productRepo, "apps", "telemux")}`,
    `EnvironmentFile=${join(cfg.paths.configRoot, "telemux.runtime.env")}`,
    `EnvironmentFile=${join(cfg.paths.configRoot, "telemux.secrets.env")}`,
    `ExecStart=${cfg.runtime.bunBin} --no-env-file run ${join(cfg.paths.productRepo, "apps", "telemux", "src", "main.ts")}`,
    "Restart=on-failure",
    "RestartSec=5",
    "",
    "[Install]",
    "WantedBy=default.target",
    ""
  ].join("\n");
}

function postReceiveHook(cfg: BrainstackConfig): string {
  return `#!/usr/bin/env bash
set -euo pipefail

BARE_REPO=${JSON.stringify(cfg.repos.bare)}
SERVE_REPO=${JSON.stringify(cfg.repos.serve)}
PRODUCT_REPO=${JSON.stringify(cfg.paths.productRepo)}
BUN_BIN=${JSON.stringify(cfg.runtime.bunBin)}

while read -r oldrev newrev refname; do
  if [ "$refname" != "refs/heads/main" ]; then
    continue
  fi
  if [ ! -d "$SERVE_REPO/.git" ]; then
    git clone "$BARE_REPO" "$SERVE_REPO"
  fi
  git --git-dir="$SERVE_REPO/.git" --work-tree="$SERVE_REPO" fetch origin main
  git --git-dir="$SERVE_REPO/.git" --work-tree="$SERVE_REPO" checkout -f main
  git --git-dir="$SERVE_REPO/.git" --work-tree="$SERVE_REPO" reset --hard origin/main
  SHARED_BRAIN_REPO_ROOT="$SERVE_REPO" "$BUN_BIN" --no-env-file run "$PRODUCT_REPO/apps/braind/src/reindex.ts" --quiet || true
done
`;
}

function preReceiveHook(): string {
  return `#!/usr/bin/env bash
set -euo pipefail

while read -r oldrev newrev refname; do
  files=$(git diff-tree --no-commit-id --name-only -r "$newrev")
  if printf '%s\n' "$files" | grep -E '(^|/)(derived/|node_modules/|\\.env$|\\.env\\.|.*\\.pem$|.*\\.key$|.*\\.token$)' >/dev/null; then
    echo "brainstack policy: refusing derived caches, env files, tokens, or private keys" >&2
    exit 1
  fi
done
`;
}

function tailscaleServeScript(cfg: BrainstackConfig): string {
  return `#!/usr/bin/env bash
set -euo pipefail
CONFIG_FILE="\${1:-$(dirname "$0")/serve-config.json}"
tailscale serve set-config --all "$CONFIG_FILE"
tailscale serve status
`;
}

function tailscaleServeConfig(cfg: BrainstackConfig): string {
  const host = cfg.tailscale.tailnetHost || "brain-control.example.ts.net";
  return `${JSON.stringify(
    {
      TCP: {
        "443": {
          HTTPS: true
        }
      },
      Web: {
        [`${host}:443`]: {
          Handlers: {
            "/": {
              Proxy: `http://127.0.0.1:${cfg.brain.port}`
            }
          }
        }
      }
    },
    null,
    2
  )}\n`;
}

function tailscaleUpScript(cfg: BrainstackConfig): string {
  const tags = cfg.tailscale.advertiseTags.join(",");
  const tagFlag = tags ? ` --advertise-tags=${tags}` : "";
  return `#!/usr/bin/env bash
set -euo pipefail
: "\${TAILSCALE_AUTH_KEY:?set TAILSCALE_AUTH_KEY first}"
sudo tailscale up --auth-key="\${TAILSCALE_AUTH_KEY}" --hostname=${cfg.machine.name}${tagFlag} --operator=${cfg.machine.user}
`;
}

function tailscalePolicyFragment(cfg: BrainstackConfig): string {
  const fragment = {
    tagOwners: {
      [cfg.tailscale.controlTag]: ["group:brain-admins"],
      [cfg.tailscale.workerTag]: ["group:brain-admins"]
    },
    grants: [
      {
        src: ["group:brain-admins", "autogroup:admin"],
        dst: [cfg.tailscale.controlTag],
        ip: ["tcp:22", "tcp:443", "icmp:*"]
      },
      {
        src: [cfg.tailscale.controlTag],
        dst: [cfg.tailscale.workerTag],
        ip: ["tcp:22", "icmp:*"]
      },
      {
        src: [cfg.tailscale.workerTag],
        dst: [cfg.tailscale.controlTag],
        ip: ["tcp:443", "icmp:*"]
      }
    ],
    ssh: [],
    nodeAttrs: []
  };
  return `${JSON.stringify(fragment, null, 2)}\n`;
}

function clientBootstrapFiles(cfg: BrainstackConfig): Record<string, string> {
  const clientPath = cfg.client.localPath;
  const replacements = {
    BRAIN_BASE_URL: cfg.brain.publicBaseUrl || "https://brain-control.example.ts.net",
    BRAIN_GIT_REMOTE: cfg.client.remoteSsh,
    MACHINE_USER: cfg.machine.user,
    SHARED_BRAIN_LOCAL_PATH: clientPath
  };
  const templateNames = [
    "client.env.example",
    "codex-shared-brain.include.md",
    "codex-global-AGENTS.md",
    "claude-user-CLAUDE.md",
    "claude-hooks-example.json",
    "cursor-user-rule.md",
    "ssh_config_fragment.example",
    "install-client.sh"
  ];
  return Object.fromEntries(
    templateNames.map((name) => [name, renderTemplate(readClientBootstrapTemplate(name), replacements)])
  );
}

function renderFiles(cfg: BrainstackConfig): Record<string, string> {
  const files: Record<string, string> = {
    "brainstack.yaml": stringifySimpleYaml({
      schema_version: cfg.schemaVersion,
      profile: cfg.profile,
      runtime: cfg.runtime,
      harness: cfg.harness,
      machine: cfg.machine,
      paths: cfg.paths,
      brain: cfg.brain,
      repos: cfg.repos,
      telemux: { ...cfg.telemux, workers: defaultWorkers(cfg) },
      tailscale: cfg.tailscale,
      client: cfg.client
    }),
    "git-hooks/post-receive": postReceiveHook(cfg),
    "git-hooks/pre-receive": preReceiveHook(),
    "tailscale/tailscale-up.sh": tailscaleUpScript(cfg),
    "tailscale/tailscale-serve.sh": tailscaleServeScript(cfg),
    "tailscale/serve-config.json": tailscaleServeConfig(cfg),
    "tailscale/policy-fragment.json": tailscalePolicyFragment(cfg),
    "telemux/workers.json": `${JSON.stringify(defaultWorkers(cfg), null, 2)}\n`,
    "README.generated.md": [
      `# brainstack render: ${cfg.profile}`,
      "",
      `Machine: \`${cfg.machine.name}\``,
      `Shared brain root: \`${cfg.paths.sharedBrainRoot}\``,
      `Bare repo: \`${cfg.repos.bare}\``,
      `Staging clone: \`${cfg.repos.staging}\``,
      `Serve clone: \`${cfg.repos.serve}\``,
      `Public URL: \`${cfg.brain.publicBaseUrl || "(none)"}\``,
      "",
      "Use these rendered files as inspectable install artifacts. Real secret values belong in local env files only.",
      ""
    ].join("\n")
  };
  if (runsBraind(cfg)) {
    files["env/braind.runtime.env"] = braindRuntimeEnv(cfg);
    files["env/braind.secrets.env.example"] = braindSecretsEnv(false);
    files["systemd/user/braind.service"] = braindService(cfg);
  }
  if (cfg.telemux.enabled) {
    files["env/telemux.runtime.env"] = telemuxRuntimeEnv(cfg);
    files["env/telemux.secrets.env.example"] = telemuxSecretsEnv();
    files["systemd/user/telemux.service"] = telemuxService(cfg);
  }
  for (const [path, content] of Object.entries(clientBootstrapFiles(cfg))) {
    files[`client-bootstrap/${path}`] = content;
  }
  return files;
}

async function writeFileMap(root: string, files: Record<string, string>): Promise<void> {
  for (const [path, content] of Object.entries(files)) {
    const fullPath = join(root, path);
    await writeText(fullPath, content, path.endsWith(".sh") || path.includes("git-hooks/") ? 0o755 : undefined);
  }
}

function clientLocalPathAbs(cfg: BrainstackConfig): string {
  return absWithHome(cfg.client.localPath, cfg.paths.home);
}

function clientEnvPathAbs(cfg: BrainstackConfig): string {
  return absWithHome(cfg.client.envPath, cfg.paths.home);
}

async function installLocalClientBootstrap(cfg: BrainstackConfig): Promise<string[]> {
  const touched: string[] = [];
  const bootstrapRoot = join(cfg.paths.configRoot, "client-bootstrap");
  const bootstrapFiles = clientBootstrapFiles(cfg);
  await writeFileMap(bootstrapRoot, bootstrapFiles);
  await writeText(
    join(bootstrapRoot, "claude-user-CLAUDE.md"),
    renderTemplate(readClientBootstrapTemplate("claude-user-CLAUDE.md"), {
      BRAIN_BASE_URL: cfg.brain.publicBaseUrl || "https://brain-control.example.ts.net",
      BRAIN_GIT_REMOTE: cfg.client.remoteSsh,
      MACHINE_USER: cfg.machine.user,
      SHARED_BRAIN_LOCAL_PATH: clientLocalPathAbs(cfg)
    })
  );
  touched.push(bootstrapRoot);

  const target = clientLocalPathAbs(cfg);
  if (gitExists(target)) {
    run(["git", "-C", target, "pull", "--ff-only"]);
  } else {
    await ensureDir(dirname(target));
    run(["git", "clone", cfg.client.remoteSsh, target]);
  }
  touched.push(target);

  const envPath = clientEnvPathAbs(cfg);
  if (await writeIfMissing(envPath, bootstrapFiles["client.env.example"], 0o600)) {
    touched.push(envPath);
  }

  const codexHome = join(cfg.paths.home, ".codex");
  await ensureDir(codexHome);
  const codexAgents = join(codexHome, "AGENTS.md");
  if (!existsSync(codexAgents)) {
    await symlink(join(bootstrapRoot, "codex-shared-brain.include.md"), codexAgents);
    touched.push(codexAgents);
  }

  const claudeHome = join(cfg.paths.home, ".claude");
  await ensureDir(claudeHome);
  const claudeFile = join(claudeHome, "CLAUDE.md");
  if (await writeIfMissing(claudeFile, `@${join(bootstrapRoot, "claude-user-CLAUDE.md")}\n`)) {
    touched.push(claudeFile);
  }

  const cursorRules = join(cfg.paths.home, ".cursor", "rules");
  await ensureDir(cursorRules);
  const cursorRule = join(cursorRules, "shared-brain.md");
  if (await writeIfMissing(cursorRule, bootstrapFiles["cursor-user-rule.md"])) {
    touched.push(cursorRule);
  }

  return touched;
}

function commandPath(name: string): string | null {
  const proc = run(["bash", "-lc", `command -v ${shellSingleQuote(name)}`], { check: false });
  return proc.code === 0 && proc.stdout.trim() ? proc.stdout.trim().split(/\r?\n/)[0] : null;
}

function whereisPath(name: string): string | null {
  const direct = commandPath(name);
  if (direct) {
    return direct;
  }
  const proc = run(["whereis", name], { check: false });
  if (proc.code !== 0) {
    return null;
  }
  const [, rest = ""] = proc.stdout.split(":");
  return rest.trim().split(/\s+/).find((entry) => entry.startsWith("/")) || null;
}

function installHint(name: string): string {
  const osRelease = existsSync("/etc/os-release") ? readFileSync("/etc/os-release", "utf8").toLowerCase() : "";
  if (process.platform === "darwin") {
    if (name === "bun") return "Install Bun: curl -fsSL https://bun.sh/install | bash";
    if (name === "tailscale") return "Install Tailscale: brew install --cask tailscale";
    if (name === "git") return "Install Git: xcode-select --install or brew install git";
    if (name === "ssh") return "OpenSSH client is normally built in; install Xcode Command Line Tools if missing.";
    if (name === "sshd") return "Enable Remote Login in macOS Sharing settings if this machine must accept SSH.";
    if (name === "codex") return "Install and authenticate Codex CLI, then ensure `codex --version` works.";
    if (name === "claude") return "Install and authenticate Claude Code, then ensure `claude --version` works.";
  }
  if (osRelease.includes("arch") || osRelease.includes("omarchy")) {
    if (name === "bun") return "Install Bun: curl -fsSL https://bun.sh/install | bash";
    if (name === "git") return "Install Git: sudo pacman -S git";
    if (name === "ssh") return "Install OpenSSH client/server: sudo pacman -S openssh";
    if (name === "sshd") return "Enable OpenSSH server: sudo pacman -S openssh && sudo systemctl enable --now sshd.service";
    if (name === "tailscale") return "Install Tailscale: sudo pacman -S tailscale && sudo systemctl enable --now tailscaled.service";
  }
  if (osRelease.includes("debian") || osRelease.includes("ubuntu")) {
    if (name === "bun") return "Install Bun: curl -fsSL https://bun.sh/install | bash";
    if (name === "git") return "Install Git: sudo apt-get update && sudo apt-get install -y git";
    if (name === "ssh") return "Install OpenSSH client/server: sudo apt-get update && sudo apt-get install -y openssh-client openssh-server";
    if (name === "sshd") return "Enable OpenSSH server: sudo systemctl enable --now ssh.service";
    if (name === "tailscale") return "Install Tailscale: curl -fsSL https://tailscale.com/install.sh | sh && sudo systemctl enable --now tailscaled.service";
  }
  if (name === "bun") return "Install Bun from https://bun.sh and ensure `command -v bun` works.";
  if (name === "tailscale") return "Install Tailscale and ensure `command -v tailscale` works.";
  if (name === "codex") return "Install and authenticate Codex CLI, then ensure `codex --version` works.";
  if (name === "claude") return "Install and authenticate Claude Code, then ensure `claude --version` works.";
  return `Install ${name} and ensure it is available in PATH.`;
}

function requiredProvisionCommands(profile: Profile): string[] {
  const commands = ["bun", "git", "ssh", "tailscale"];
  if (profile === "single-node" || profile === "control" || profile === "worker") {
    commands.push("sshd");
  }
  return commands;
}

function ensureProvisionPrereqs(profile: Profile): Record<string, string> {
  const found: Record<string, string> = {};
  const missing: string[] = [];
  for (const name of requiredProvisionCommands(profile)) {
    const path = whereisPath(name);
    if (path) {
      found[name] = path;
    } else {
      missing.push(name);
    }
  }
  if (missing.length) {
    throw new Error(
      [
        `provision blocked: missing required tools: ${missing.join(", ")}`,
        "",
        ...missing.map((name) => `- ${name}: ${installHint(name)}`)
      ].join("\n")
    );
  }
  return found;
}

async function promptHarnessChoice(codexPath: string, claudePath: string): Promise<HarnessName> {
  process.stdout.write(`Both Codex and Claude are installed.\n1. codex (${codexPath})\n2. claude (${claudePath})\nSelect default harness [1/2]: `);
  const input = await new Response(Bun.stdin.stream()).text();
  const choice = input.trim().toLowerCase();
  if (choice === "2" || choice === "claude") {
    return "claude";
  }
  if (!choice || choice === "1" || choice === "codex") {
    return "codex";
  }
  throw new Error("provision blocked: invalid harness choice");
}

async function selectProvisionHarness(args: ParsedArgs): Promise<{ name: HarnessName; bin: string; discovered: Record<string, string | null> }> {
  const requested = flag(args, "harness");
  const codexPath = whereisPath("codex");
  const claudePath = whereisPath("claude");
  const discovered = { codex: codexPath, claude: claudePath };
  if (requested) {
    const name = normalizeHarness(requested);
    const bin = name === "codex" ? codexPath : claudePath;
    if (!bin) {
      throw new Error(`provision blocked: requested harness ${name} is missing.\n${installHint(name)}`);
    }
    return { name, bin, discovered };
  }
  if (codexPath && !claudePath) {
    return { name: "codex", bin: codexPath, discovered };
  }
  if (claudePath && !codexPath) {
    return { name: "claude", bin: claudePath, discovered };
  }
  if (codexPath && claudePath) {
    if (process.stdin.isTTY && !hasFlag(args, "yes")) {
      const name = await promptHarnessChoice(codexPath, claudePath);
      return { name, bin: name === "codex" ? codexPath : claudePath, discovered };
    }
    throw new Error("provision blocked: both Codex and Claude were found; pass --harness codex or --harness claude for non-interactive provisioning.");
  }
  throw new Error(`provision blocked: neither Codex nor Claude was found.\n- codex: ${installHint("codex")}\n- claude: ${installHint("claude")}`);
}

async function runWithInputTimeout(
  args: string[],
  input: string,
  options: { cwd?: string; timeoutMs?: number; env?: Record<string, string> } = {}
): Promise<{ code: number; stdout: string; stderr: string; timedOut: boolean }> {
  const proc = Bun.spawn(args, {
    cwd: options.cwd || process.cwd(),
    env: { ...process.env, ...(options.env || {}) },
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe"
  });
  proc.stdin.write(input);
  proc.stdin.end();
  let timedOut = false;
  const timeoutMs = options.timeoutMs || Number(process.env.BRAINSTACK_HARNESS_TEST_TIMEOUT_MS || "120000");
  const timer = setTimeout(() => {
    timedOut = true;
    proc.kill();
  }, timeoutMs);
  try {
    const [stdout, stderr, code] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
      proc.exited
    ]);
    return { code, stdout, stderr, timedOut };
  } finally {
    clearTimeout(timer);
  }
}

function ensurePasswordlessSudo(): void {
  const sudo = run(["sudo", "-n", "true"], { check: false });
  if (sudo.code !== 0) {
    throw new Error(`provision blocked: current user does not have passwordless sudo.\n${sudo.stderr || sudo.stdout}`);
  }
}

async function testHarnessSudo(harness: { name: HarnessName; bin: string }): Promise<void> {
  const marker = "BRAINSTACK_HARNESS_SUDO_OK";
  const prompt = [
    "Run exactly this shell command and then stop:",
    "",
    "sudo -n true && printf BRAINSTACK_HARNESS_SUDO_OK",
    "",
    "Do not summarize. The output must contain BRAINSTACK_HARNESS_SUDO_OK."
  ].join("\n");
  const temp = await mkdtemp(join(tmpdir(), "brainstack-harness-test-"));
  try {
    const args =
      harness.name === "codex"
        ? [harness.bin, "exec", "--skip-git-repo-check", "--dangerously-bypass-approvals-and-sandbox", "-"]
        : [harness.bin, "-p", "--dangerously-skip-permissions", "--permission-mode", "bypassPermissions", "--output-format", "text"];
    const result = await runWithInputTimeout(args, prompt, { cwd: temp });
    if (result.timedOut) {
      throw new Error(`provision blocked: ${harness.name} harness sudo test timed out`);
    }
    const combined = `${result.stdout}\n${result.stderr}`;
    if (result.code !== 0 || !combined.includes(marker)) {
      throw new Error(
        [
          `provision blocked: ${harness.name} did not prove it can run sudo in bypass/yolo mode.`,
          `exit=${result.code}`,
          combined.trim()
        ].join("\n")
      );
    }
  } finally {
    await rm(temp, { recursive: true, force: true });
  }
}

async function testTelegramBotConfig(args: ParsedArgs): Promise<void> {
  const envFile = flag(args, "telegram-env");
  let tokenValue = process.env.FACTORY_TELEGRAM_BOT_TOKEN || "";
  if (envFile && existsSync(abs(envFile))) {
    const text = await readFile(abs(envFile), "utf8");
    const match = text.match(/^FACTORY_TELEGRAM_BOT_TOKEN=(.*)$/m);
    tokenValue = match?.[1]?.replace(/^['"]|['"]$/g, "") || tokenValue;
  }
  if (!tokenValue.trim()) {
    throw new Error("provision blocked: --test-bot requires FACTORY_TELEGRAM_BOT_TOKEN in env or --telegram-env FILE");
  }
  let response: Response;
  try {
    response = await fetch(`https://api.telegram.org/bot${tokenValue.trim()}/getMe`);
  } catch (error) {
    const message = error instanceof Error ? error.message.replaceAll(tokenValue.trim(), "[REDACTED_TELEGRAM_TOKEN]") : String(error);
    throw new Error(`provision blocked: Telegram getMe request failed: ${message}`);
  }
  if (!response.ok) {
    throw new Error(`provision blocked: Telegram getMe failed with HTTP ${response.status}`);
  }
  const payload = (await response.json()) as { ok?: boolean };
  if (!payload.ok) {
    throw new Error("provision blocked: Telegram getMe returned ok=false");
  }
}

function discoveredMachineName(): string {
  return run(["hostname", "-s"], { check: false }).stdout.trim() || run(["hostname"], { check: false }).stdout.trim() || "brain-control";
}

function buildProvisionConfig(profile: Profile, harness: { name: HarnessName; bin: string }, args: ParsedArgs): Record<string, unknown> {
  const user = process.env.USER || "operator";
  const home = process.env.HOME || `/home/${user}`;
  const machineName = flag(args, "machine") || discoveredMachineName();
  const role = flag(args, "role") || profile;
  const publicBaseUrl = flag(args, "brain-base-url") || flag(args, "public-base-url") || "";
  const tailnetHost = flag(args, "tailnet-host") || publicBaseUrl.replace(/^https?:\/\//, "");
  const telemuxEnabled = hasFlag(args, "enable-telemux");
  const sharedBrainRemote =
    flag(args, "brain-remote") || flag(args, "shared-brain-remote") || `${user}@${machineName}:${join(home, "shared-brain", "bare", "shared-brain.git")}`;
  const localPath = flag(args, "client-local-path") || "~/shared-brain";
  const tailscaleTag =
    flag(args, "tailscale-tag") || (profile === "worker" ? "tag:brain-worker" : profile === "client-macos" ? "" : "tag:brain");
  const advertiseTags = tailscaleTag ? [tailscaleTag] : [];
  const controlTag = flag(args, "control-tag") || "tag:brain";
  const workerTag = flag(args, "worker-tag") || "tag:brain-worker";
  return {
    schema_version: CONFIG_SCHEMA_VERSION,
    profile,
    runtime: {
      bunBin: whereisPath("bun") || "bun"
    },
    harness,
    machine: {
      name: machineName,
      user,
      role,
      sshUser: flag(args, "ssh-user") || user,
      hostname: flag(args, "hostname") || machineName
    },
    paths: {
      home,
      productRepo: "~/brainstack",
      sharedBrainRoot: "~/shared-brain",
      privateBrainRoot: "~/private-brain",
      stateRoot: "~/.local/state/brainstack",
      configRoot: "~/.config/brainstack",
      systemdUserRoot: "~/.config/systemd/user"
    },
    brain: {
      bind: "127.0.0.1",
      port: Number(flag(args, "brain-port") || "8080"),
      publicBaseUrl,
      largeFileThresholdBytes: 10 * 1024 * 1024,
      enableTelemux: telemuxEnabled
    },
    telemux: {
      enabled: telemuxEnabled,
      dashboardHost: "127.0.0.1",
      dashboardPort: Number(flag(args, "telemux-port") || "8787"),
      localMachine: machineName,
      workers: []
    },
    tailscale: {
      tailnetHost,
      controlTag,
      workerTag,
      advertiseTags,
      enableSsh: false
    },
    client: {
      localPath,
      envPath: "~/.config/shared-brain.env",
      remoteSsh: sharedBrainRemote
    }
  };
}

function tailscaleUpShellCommand(machineName: string, user: string, tags: string[], authKeyEnv: string): string {
  const tagFlag = tags.length ? ` --advertise-tags=${shellSingleQuote(tags.join(","))}` : "";
  return [
    `: "\${${authKeyEnv}:?set ${authKeyEnv} first}"`,
    `sudo tailscale up --auth-key="\${${authKeyEnv}}" --hostname=${shellSingleQuote(machineName)} --operator=${shellSingleQuote(user)} --ssh=false${tagFlag}`
  ].join("\n");
}

async function commandProvision(args: ParsedArgs): Promise<void> {
  const profile = (flag(args, "profile") || "single-node") as Profile;
  if (!["single-node", "control", "worker", "client-macos"].includes(profile)) {
    throw new Error("provision supports --profile single-node|control|worker|client-macos");
  }
  const found = ensureProvisionPrereqs(profile);
  const selectedHarness = await selectProvisionHarness(args);
  ensurePasswordlessSudo();
  if (!hasFlag(args, "skip-harness-sudo-test")) {
    await testHarnessSudo(selectedHarness);
  }
  if (hasFlag(args, "test-bot")) {
    await testTelegramBotConfig(args);
  }
  const config = buildProvisionConfig(profile, { name: selectedHarness.name, bin: selectedHarness.bin }, args);
  const out = abs(flag(args, "out") || "~/.config/brainstack/brainstack.yaml");
  await writeText(out, stringifySimpleYaml(config));
  if (hasFlag(args, "enroll-tailscale")) {
    const authKeyEnv = flag(args, "tailscale-auth-key-env") || "TAILSCALE_AUTH_KEY";
    if (!process.env[authKeyEnv]) {
      throw new Error(`provision blocked: --enroll-tailscale requires ${authKeyEnv} in env`);
    }
    const tailscale = objectAt(config, "tailscale");
    const machine = objectAt(config, "machine");
    const paths = objectAt(config, "paths");
    run(["bash", "-lc", tailscaleUpShellCommand(String(machine.name), String(machine.user), arrayAt(tailscale, "advertiseTags").map(String), authKeyEnv)]);
    console.log(`tailscale enrolled for ${String(machine.name)}; config still written to ${out}`);
    console.log(`home path: ${String(paths.home)}`);
  }
  console.log(`provision config written: ${out}`);
  console.log(`detected tools: ${Object.entries(found).map(([name, path]) => `${name}=${path}`).join(" ")}`);
  console.log(`selected harness: ${selectedHarness.name} (${selectedHarness.bin})`);
  console.log("next:");
  console.log(`  brainctl init --profile ${profile} --config ${out}`);
  if (profile === "single-node" || profile === "control") {
    console.log("  systemctl --user daemon-reload");
    console.log("  systemctl --user enable --now braind.service");
    if (boolFlag(args, "enable-telemux")) {
      console.log("  edit ~/.config/brainstack/telemux.secrets.env before starting telemux.service");
      console.log("  systemctl --user enable --now telemux.service");
    }
  }
}

async function commandRender(args: ParsedArgs): Promise<void> {
  const out = flag(args, "out");
  if (!out) {
    throw new Error("render requires --out DIR");
  }
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  await rm(abs(out), { recursive: true, force: true });
  await writeFileMap(abs(out), renderFiles(cfg));
  console.log(`rendered ${Object.keys(renderFiles(cfg)).length} files to ${abs(out)}`);
}

async function writeSharedBrainSeed(target: string, cfg: BrainstackConfig, mode: SeedMode): Promise<string[]> {
  const touched: string[] = [];
  const seedFiles = sharedBrainSeedFiles(cfg);
  for (const [path, content] of Object.entries(seedFiles)) {
    const fullPath = join(target, path);
    if (mode !== "force" && existsSync(fullPath)) {
      continue;
    }
    await writeText(fullPath, content, path.endsWith(".sh") || path.includes("git-hooks/") ? 0o755 : undefined);
    touched.push(path);
  }
  return touched;
}

function gitExists(path: string): boolean {
  return existsSync(join(path, ".git"));
}

async function ensureGitRepoLayout(cfg: BrainstackConfig, mode: "fresh" | "runtime", seedMode: SeedMode = "empty-only"): Promise<void> {
  await ensureDir(dirname(cfg.repos.bare));
  await ensureDir(dirname(cfg.repos.staging));
  await ensureDir(dirname(cfg.repos.serve));
  await ensureDir(cfg.repos.blobs);
  const existingCanon = isBareRepoInitialized(cfg.repos.bare);
  if (mode === "runtime" && !existingCanon) {
    throw new Error(`No initialized shared-brain repo at ${cfg.repos.bare}; run brainctl init for a fresh install first.`);
  }
  if (mode === "fresh" && existingCanon && seedMode === "empty-only") {
    throw new Error(
      `Existing canonical shared-brain repo detected at ${cfg.repos.bare}. init is fresh-install only; use brainctl upgrade/apply-runtime for reruns.`
    );
  }
  if (!existsSync(cfg.repos.bare)) {
    run(["git", "init", "--bare", "--initial-branch=main", cfg.repos.bare]);
  }
  if (!gitExists(cfg.repos.staging)) {
    run(["git", "clone", cfg.repos.bare, cfg.repos.staging]);
  } else if (existingCanon) {
    syncCloneToMain(cfg.repos.staging);
  }
  const shouldSeed = mode === "fresh" && (!existingCanon || seedMode === "missing" || seedMode === "force");
  if (shouldSeed) {
    const touched = await writeSharedBrainSeed(cfg.repos.staging, cfg, seedMode === "empty-only" ? "force" : seedMode);
    if (touched.length) {
      run(["git", "add", "--", ...touched], { cwd: cfg.repos.staging });
      const status = run(["git", "status", "--porcelain", "--", ...touched], { cwd: cfg.repos.staging }).stdout.trim();
      if (status) {
        run([
          "git",
          "-c",
          "user.name=brainstack",
          "-c",
          "user.email=brainstack@local",
          "commit",
          "-m",
          `brainstack: initialize ${cfg.profile} shared brain`
        ], { cwd: cfg.repos.staging });
      }
    }
  }
  run(["git", "push", "-u", "origin", "main"], { cwd: cfg.repos.staging, check: false });
  if (!gitExists(cfg.repos.serve)) {
    run(["git", "clone", cfg.repos.bare, cfg.repos.serve]);
  } else {
    syncCloneToMain(cfg.repos.serve);
  }
  await writeText(join(cfg.repos.bare, "hooks", "post-receive"), postReceiveHook(cfg), 0o755);
  await writeText(join(cfg.repos.bare, "hooks", "pre-receive"), preReceiveHook(), 0o755);
}

async function commandInit(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  if (hasFlag(args, "dry-run")) {
    const out = flag(args, "out") || join(tmpdir(), `brainstack-init-render-${cfg.profile}-${Date.now()}`);
    await writeFileMap(out, renderFiles(cfg));
    console.log(`dry-run render complete: ${out}`);
    return;
  }
  await ensureDir(cfg.paths.configRoot);
  await ensureDir(cfg.paths.stateRoot);
  await ensureDir(cfg.paths.systemdUserRoot);
  if (runsBraind(cfg)) {
    const seedMode: SeedMode = hasFlag(args, "force-seed") ? "force" : hasFlag(args, "seed-missing") ? "missing" : "empty-only";
    await ensureGitRepoLayout(cfg, "fresh", seedMode);
    await writeText(join(cfg.paths.configRoot, "braind.runtime.env"), braindRuntimeEnv(cfg), 0o644);
    await writeIfMissing(join(cfg.paths.configRoot, "braind.secrets.env"), braindSecretsEnv(true), 0o600);
    await writeText(join(cfg.paths.systemdUserRoot, "braind.service"), braindService(cfg));
  }
  if (cfg.telemux.enabled) {
    await writeText(join(cfg.paths.configRoot, "telemux.runtime.env"), telemuxRuntimeEnv(cfg), 0o644);
    await writeIfMissing(join(cfg.paths.configRoot, "telemux.secrets.env"), telemuxSecretsEnv(), 0o600);
    await writeText(join(cfg.paths.configRoot, "workers.json"), `${JSON.stringify(defaultWorkers(cfg), null, 2)}\n`);
    await writeText(join(cfg.paths.systemdUserRoot, "telemux.service"), telemuxService(cfg));
  }
  if (cfg.profile === "worker" || cfg.profile === "client-macos") {
    const touched = await installLocalClientBootstrap(cfg);
    console.log(`client bootstrap touched: ${touched.length ? touched.join(", ") : "(none)"}`);
  }
  await writeFileMap(join(cfg.paths.stateRoot, "rendered"), renderFiles(cfg));
  console.log(`initialized ${cfg.profile} at ${cfg.paths.sharedBrainRoot}`);
  console.log(`env: ${cfg.paths.configRoot}`);
  console.log(`user units: ${cfg.paths.systemdUserRoot}`);
}

async function applyRuntime(cfg: BrainstackConfig): Promise<string[]> {
  const touched: string[] = [];
  await ensureDir(cfg.paths.configRoot);
  await ensureDir(cfg.paths.stateRoot);
  await ensureDir(cfg.paths.systemdUserRoot);
  if (runsBraind(cfg)) {
    await ensureGitRepoLayout(cfg, "runtime", "empty-only");
    await writeText(join(cfg.paths.configRoot, "braind.runtime.env"), braindRuntimeEnv(cfg), 0o644);
    await writeIfMissing(join(cfg.paths.configRoot, "braind.secrets.env"), braindSecretsEnv(true), 0o600);
    await writeText(join(cfg.paths.systemdUserRoot, "braind.service"), braindService(cfg));
    touched.push(join(cfg.paths.configRoot, "braind.runtime.env"));
    touched.push(join(cfg.paths.systemdUserRoot, "braind.service"));
    touched.push(join(cfg.repos.bare, "hooks", "post-receive"));
    touched.push(join(cfg.repos.bare, "hooks", "pre-receive"));
  }
  if (cfg.telemux.enabled) {
    await writeText(join(cfg.paths.configRoot, "telemux.runtime.env"), telemuxRuntimeEnv(cfg), 0o644);
    await writeIfMissing(join(cfg.paths.configRoot, "telemux.secrets.env"), telemuxSecretsEnv(), 0o600);
    await writeText(join(cfg.paths.configRoot, "workers.json"), `${JSON.stringify(defaultWorkers(cfg), null, 2)}\n`);
    await writeText(join(cfg.paths.systemdUserRoot, "telemux.service"), telemuxService(cfg));
    touched.push(join(cfg.paths.configRoot, "telemux.runtime.env"));
    touched.push(join(cfg.paths.systemdUserRoot, "telemux.service"));
    touched.push(join(cfg.paths.configRoot, "workers.json"));
  }
  await writeFileMap(join(cfg.paths.stateRoot, "rendered"), renderFiles(cfg));
  touched.push(join(cfg.paths.stateRoot, "rendered"));
  return touched;
}

async function commandApplyRuntime(args: ParsedArgs, withBackup: boolean): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  if (hasFlag(args, "dry-run")) {
    const out = flag(args, "out") || join(tmpdir(), `brainstack-upgrade-render-${cfg.profile}-${Date.now()}`);
    await writeFileMap(out, renderFiles(cfg));
    console.log(`dry-run runtime render complete: ${out}`);
    return;
  }
  if (withBackup) {
    await commandBackup({ ...args, flags: { ...args.flags, profile: cfg.profile } });
  }
  const touched = await applyRuntime(cfg);
  console.log(`${withBackup ? "upgrade" : "apply-runtime"} complete for ${cfg.profile}`);
  console.log(`runtime artifacts touched: ${touched.length ? touched.join(", ") : "(none)"}`);
  console.log("canonical shared-brain content was not seeded or rewritten");
  console.log("activation commands:");
  console.log("  systemctl --user daemon-reload");
  if (runsBraind(cfg)) {
    console.log("  systemctl --user restart braind.service");
  }
  if (cfg.telemux.enabled) {
    console.log("  systemctl --user restart telemux.service");
  }
}

async function commandDoctor(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const checks: Array<{ name: string; ok: boolean; detail: string }> = [];
  const commandOk = (name: string) => run(["bash", "-lc", `command -v ${name}`], { check: false }).code === 0;
  const bunVersion = installedBunVersion();
  checks.push({
    name: "bun",
    ok: Boolean(bunVersion) && compareVersions(bunVersion || "0.0.0", MIN_BUN_VERSION) >= 0,
    detail: bunVersion ? `Bun ${bunVersion}; required >= ${MIN_BUN_VERSION}` : "Bun runtime missing"
  });
  checks.push({
    name: "bun-bin",
    ok: existsSync(cfg.runtime.bunBin),
    detail: cfg.runtime.bunBin
  });
  checks.push({ name: "git", ok: commandOk("git"), detail: "Git CLI" });
  checks.push({ name: "ssh", ok: commandOk("ssh"), detail: "OpenSSH client" });
  checks.push({ name: "tailscale", ok: commandOk("tailscale"), detail: "Tailscale CLI optional for clients" });
  checks.push({ name: "harness", ok: commandOk(cfg.harness.bin), detail: `${cfg.harness.name} via ${cfg.harness.bin}` });
  if (usesUserServices(cfg) && commandOk("loginctl")) {
    const linger = run(["loginctl", "show-user", cfg.machine.user, "--property=Linger", "--value"], { check: false });
    const enabled = linger.stdout.trim() === "yes";
    checks.push({
      name: "user-service-linger",
      ok: enabled,
      detail: enabled ? `linger enabled for ${cfg.machine.user}` : `run: loginctl enable-linger ${cfg.machine.user}`
    });
  }
  checks.push({ name: "config-root", ok: existsSync(cfg.paths.configRoot), detail: cfg.paths.configRoot });
  checks.push({ name: "state-root", ok: existsSync(cfg.paths.stateRoot), detail: cfg.paths.stateRoot });
  if (runsBraind(cfg)) {
    checks.push({ name: "bare-repo", ok: existsSync(cfg.repos.bare), detail: cfg.repos.bare });
    checks.push({ name: "staging-clone", ok: gitExists(cfg.repos.staging), detail: cfg.repos.staging });
    checks.push({ name: "serve-clone", ok: gitExists(cfg.repos.serve), detail: cfg.repos.serve });
  }
  for (const check of checks) {
    console.log(`${check.ok ? "ok" : "warn"} ${check.name}: ${check.detail}`);
  }
  const hardFailures = checks.filter((check) => !check.ok && ["bun", "bun-bin", "git"].includes(check.name));
  if (hardFailures.length) {
    throw new Error(`doctor failed: ${hardFailures.map((item) => item.name).join(", ")}`);
  }
}

async function copyIfExists(source: string, target: string): Promise<void> {
  if (!existsSync(source)) {
    return;
  }
  await cp(source, target, { recursive: true, force: true, errorOnExist: false });
}

function userServiceActive(serviceName: string): boolean {
  return run(["systemctl", "--user", "is-active", "--quiet", serviceName], { check: false }).code === 0;
}

function stopUserService(serviceName: string): boolean {
  if (!userServiceActive(serviceName)) {
    return false;
  }
  run(["systemctl", "--user", "stop", serviceName], { check: false });
  return true;
}

function startUserService(serviceName: string): void {
  run(["systemctl", "--user", "start", serviceName], { check: false });
}

async function backupSqliteIfPossible(sourceDb: string, targetDb: string): Promise<string> {
  if (!existsSync(sourceDb)) {
    return "not-present";
  }
  await ensureDir(dirname(targetDb));
  await rm(targetDb, { force: true });
  const escapedTarget = targetDb.replaceAll("'", "''");
  const sqlite = run(["sqlite3", sourceDb, `.backup '${escapedTarget}'`], { check: false });
  if (sqlite.code === 0) {
    return "sqlite-backup";
  }
  await copyIfExists(sourceDb, targetDb);
  return "plain-copy-fallback";
}

async function commandBackup(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const outRoot = abs(flag(args, "out") || join(cfg.paths.stateRoot, "backups"));
  const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
  const backupDir = join(outRoot, `brainstack-backup-${stamp}`);
  const pauseTelemux = hasFlag(args, "pause-telemux");
  const telemuxWasStopped = pauseTelemux && cfg.telemux.enabled ? stopUserService("telemux.service") : false;
  let sqliteMode = "not-run";
  try {
    await ensureDir(backupDir);
    await copyIfExists(cfg.paths.configRoot, join(backupDir, "config"));
    await copyIfExists(cfg.paths.sharedBrainRoot, join(backupDir, "shared-brain"));
    await copyIfExists(cfg.repos.blobs, join(backupDir, "blobs"));
    await copyIfExists(cfg.telemux.controlRoot, join(backupDir, "telemux"));
    sqliteMode = await backupSqliteIfPossible(join(cfg.telemux.controlRoot, "db.sqlite"), join(backupDir, "telemux", "db.sqlite"));
    await copyIfExists(cfg.telemux.factoryRoot, join(backupDir, "factory"));
    await writeText(
      join(backupDir, "MANIFEST.txt"),
      [
        `created_at=${stamp}`,
        `profile=${cfg.profile}`,
        `machine=${cfg.machine.name}`,
        `shared_brain=${cfg.paths.sharedBrainRoot}`,
        `config=${cfg.paths.configRoot}`,
        `state=${cfg.paths.stateRoot}`,
        `pause_telemux_requested=${pauseTelemux}`,
        `telemux_was_stopped=${telemuxWasStopped}`,
        `telemux_sqlite_backup=${sqliteMode}`,
        "notes=Contains local env/config if present; keep permissions restricted.",
        "crash_consistency=Git repos are copied as filesystem trees; SQLite uses .backup when sqlite3 is available; factory workspaces may still change unless telemux was paused.",
        ""
      ].join("\n")
    );
    await chmod(backupDir, 0o700);
    console.log(`backup created: ${backupDir}`);
    console.log(`telemux sqlite backup: ${sqliteMode}`);
    if (pauseTelemux) {
      console.log(`telemux pause: ${telemuxWasStopped ? "stopped and restarted" : "service was not active or not enabled"}`);
    }
  } finally {
    if (telemuxWasStopped) {
      startUserService("telemux.service");
    }
  }
}

async function commandRestore(args: ParsedArgs): Promise<void> {
  const backup = flag(args, "backup");
  const target = flag(args, "target");
  if (!backup || !target) {
    throw new Error("restore requires --backup and --target");
  }
  const source = abs(backup);
  const dest = abs(target);
  if (!hasFlag(args, "apply")) {
    console.log(`dry-run restore: would copy ${source} to ${dest}`);
    console.log("rerun with --apply to perform the restore");
    return;
  }
  await ensureDir(dest);
  if (statSync(source).isDirectory()) {
    await cp(source, dest, { recursive: true, force: true, errorOnExist: false });
  } else {
    run(["tar", "-xzf", source, "-C", dest]);
  }
  console.log(`restore copied into ${dest}`);
}

async function removePath(path: string, dryRun: boolean, removed: string[]): Promise<void> {
  if (!existsSync(path)) {
    return;
  }
  removed.push(path);
  if (!dryRun) {
    await rm(path, { recursive: true, force: true });
  }
}

async function removeSymlinkIfTarget(path: string, expectedTarget: string, dryRun: boolean, removed: string[], skipped: string[]): Promise<void> {
  if (!existsSync(path)) {
    return;
  }
  const info = await lstat(path);
  if (!info.isSymbolicLink()) {
    skipped.push(`${path} (not a symlink)`);
    return;
  }
  const target = await readlink(path);
  if (target !== expectedTarget) {
    skipped.push(`${path} (symlink target differs)`);
    return;
  }
  removed.push(path);
  if (!dryRun) {
    await rm(path, { force: true });
  }
}

async function removeFileIfExact(path: string, expectedContent: string, dryRun: boolean, removed: string[], skipped: string[]): Promise<void> {
  if (!existsSync(path)) {
    return;
  }
  const actual = await readFile(path, "utf8").catch(() => null);
  if (actual !== expectedContent) {
    skipped.push(`${path} (content differs)`);
    return;
  }
  removed.push(path);
  if (!dryRun) {
    await rm(path, { force: true });
  }
}

async function commandDestroy(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const dryRun = hasFlag(args, "dry-run");
  const removed: string[] = [];
  const skipped: string[] = [];
  const bootstrapRoot = join(cfg.paths.configRoot, "client-bootstrap");
  const bootstrapFiles = clientBootstrapFiles(cfg);

  if (!dryRun && commandPath("systemctl")) {
    if (runsBraind(cfg)) {
      run(["systemctl", "--user", "disable", "--now", "braind.service"], { check: false });
    }
    if (cfg.telemux.enabled) {
      run(["systemctl", "--user", "disable", "--now", "telemux.service"], { check: false });
    }
  }
  if (runsBraind(cfg)) {
    await removePath(join(cfg.paths.systemdUserRoot, "braind.service"), dryRun, removed);
  }
  if (cfg.telemux.enabled) {
    await removePath(join(cfg.paths.systemdUserRoot, "telemux.service"), dryRun, removed);
  }
  await removeSymlinkIfTarget(
    join(cfg.paths.home, ".codex", "AGENTS.md"),
    join(bootstrapRoot, "codex-shared-brain.include.md"),
    dryRun,
    removed,
    skipped
  );
  await removeFileIfExact(
    join(cfg.paths.home, ".claude", "CLAUDE.md"),
    `@${join(bootstrapRoot, "claude-user-CLAUDE.md")}\n`,
    dryRun,
    removed,
    skipped
  );
  await removeFileIfExact(
    join(cfg.paths.home, ".cursor", "rules", "shared-brain.md"),
    bootstrapFiles["cursor-user-rule.md"],
    dryRun,
    removed,
    skipped
  );
  await removeFileIfExact(clientEnvPathAbs(cfg), bootstrapFiles["client.env.example"], dryRun, removed, skipped);
  await removePath(cfg.paths.configRoot, dryRun, removed);
  await removePath(cfg.paths.stateRoot, dryRun, removed);
  if (hasFlag(args, "remove-shared-brain")) {
    await removePath(cfg.paths.sharedBrainRoot, dryRun, removed);
  } else if (existsSync(cfg.paths.sharedBrainRoot)) {
    skipped.push(`${cfg.paths.sharedBrainRoot} (pass --remove-shared-brain to delete)`);
  }
  if (hasFlag(args, "remove-private-brain")) {
    await removePath(cfg.paths.privateBrainRoot, dryRun, removed);
  } else if (existsSync(cfg.paths.privateBrainRoot)) {
    skipped.push(`${cfg.paths.privateBrainRoot} (pass --remove-private-brain to delete)`);
  }
  if (hasFlag(args, "remove-tailscale-serve")) {
    removed.push("tailscale serve config");
    if (!dryRun) {
      run(["tailscale", "serve", "reset", "--yes"], { check: false });
    }
  }
  console.log(`${dryRun ? "dry-run destroy plan" : "destroy complete"} for ${cfg.profile}`);
  console.log("removed:");
  console.log(removed.length ? removed.map((item) => `  ${item}`).join("\n") : "  (none)");
  console.log("skipped:");
  console.log(skipped.length ? skipped.map((item) => `  ${item}`).join("\n") : "  (none)");
  if (!dryRun) {
    console.log("activation commands:");
    console.log("  systemctl --user daemon-reload");
    console.log("  systemctl --user reset-failed braind.service telemux.service || true");
  }
}

async function commandBootstrapClient(args: ParsedArgs): Promise<void> {
  const out = flag(args, "out") || join(tmpdir(), `brainstack-client-bootstrap-${Date.now()}`);
  const cfg = await loadConfig(flag(args, "config"), "client-macos", flag(args, "root"));
  await rm(abs(out), { recursive: true, force: true });
  await writeFileMap(abs(out), clientBootstrapFiles(cfg));
  console.log(`client bootstrap files rendered to ${abs(out)}`);
}

async function commandJoinWorker(args: ParsedArgs): Promise<void> {
  const worker = flag(args, "worker") || args.positional[0];
  if (!worker) {
    throw new Error("join-worker requires --worker NAME");
  }
  const cfg = await loadConfig(flag(args, "config"), "control", flag(args, "root"));
  const sshUser = flag(args, "ssh-user") || cfg.machine.sshUser || cfg.machine.user;
  const snippet = {
    name: worker,
    transport: "ssh",
    sshTarget: worker,
    sshUser,
    managedRepoRoot: join(cfg.telemux.factoryRoot, "repos"),
    managedHostRoot: join(cfg.telemux.factoryRoot, "hostctx"),
    managedScratchRoot: join(cfg.telemux.factoryRoot, "scratch")
  };
  const workers = [...cfg.telemux.workers, snippet];
  const text = [
    `# Worker join plan: ${worker}`,
    "",
    "Do not edit `workers.json` directly. It is rendered from `brainstack.yaml` during `brainctl upgrade`.",
    "",
    "## Merge this YAML into brainstack.yaml",
    "",
    "```yaml",
    stringifySimpleYaml({
      telemux: {
        workers
      }
    }).trimEnd(),
    "```",
    "",
    "## Apply after editing brainstack.yaml",
    "",
    "```bash",
    "brainctl upgrade --config brainstack.yaml --profile control",
    "systemctl --user daemon-reload",
    "# if telemux is enabled: systemctl --user restart telemux.service",
    "```",
    "",
    "## Tailscale grant needed if blocked",
    "",
    "```json",
    JSON.stringify(
      {
        src: [cfg.tailscale.controlTag],
        dst: [cfg.tailscale.workerTag],
        ip: ["tcp:22", "icmp:*"]
      },
      null,
      2
    ),
    "```",
    "",
    "## SSH smoke test",
    "",
    `ssh ${sshUser}@${worker} true`,
    ""
  ].join("\n");
  const out = flag(args, "out");
  if (out) {
    await writeText(abs(out), text);
  }
  console.log(text);
}

async function upsertEnv(path: string, key: string, value: string): Promise<void> {
  const existing = existsSync(path) ? await readFile(path, "utf8") : "";
  const lines = existing.split(/\r?\n/).filter((line) => line.trim() !== "");
  const next: string[] = [];
  let replaced = false;
  for (const line of lines) {
    if (line.startsWith(`${key}=`)) {
      next.push(`${key}=${value}`);
      replaced = true;
    } else {
      next.push(line);
    }
  }
  if (!replaced) {
    next.push(`${key}=${value}`);
  }
  await writeText(path, `${next.join("\n")}\n`, 0o600);
}

async function commandRotateToken(args: ParsedArgs): Promise<void> {
  const kind = flag(args, "kind");
  if (kind === "telegram-placeholder") {
    console.log("Telegram bot tokens must be rotated in BotFather. After rotation, update FACTORY_TELEGRAM_BOT_TOKEN in telemux.secrets.env and restart telemux.");
    return;
  }
  if (kind !== "import" && kind !== "admin") {
    throw new Error("rotate-token requires --kind import|admin|telegram-placeholder");
  }
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const envPath = abs(flag(args, "env") || join(cfg.paths.configRoot, "braind.secrets.env"));
  const key = kind === "import" ? "BRAIN_IMPORT_TOKEN" : "BRAIN_ADMIN_TOKEN";
  await upsertEnv(envPath, key, token());
  console.log(`rotated ${key} in ${envPath}`);
  console.log("token value not printed; restart braind for it to take effect");
}

async function commandMigrateCurrentInstall(args: ParsedArgs): Promise<void> {
  const out = abs(flag(args, "out") || "~/.config/brainstack/current-install.brainstack.yaml");
  const cfg = await loadConfig(undefined, "control");
  const hostname = run(["hostname"], { check: false }).stdout.trim() || "brain-control";
  const user = process.env.USER || "operator";
  const home = process.env.HOME || `/home/${user}`;
  const current = {
    schema_version: CONFIG_SCHEMA_VERSION,
    profile: "control",
    harness: {
      name: "codex",
      bin: "codex"
    },
    machine: {
      name: hostname,
      user,
      role: "control",
      sshUser: user,
      hostname
    },
    paths: {
      home,
      productRepo: "~/brainstack",
      sharedBrainRoot: "~/shared-brain",
      privateBrainRoot: "~/private-brain",
      stateRoot: "~/.local/state/brainstack",
      configRoot: "~/.config/brainstack",
      systemdUserRoot: "~/.config/systemd/user"
    },
    brain: {
      bind: "127.0.0.1",
      port: 8080,
      publicBaseUrl: "",
      largeFileThresholdBytes: 10485760
    },
    telemux: {
      enabled: false,
      dashboardHost: "127.0.0.1",
      dashboardPort: 8787,
      localMachine: hostname,
      workers: []
    },
    tailscale: {
      tailnetHost: "",
      controlTag: "tag:brain",
      workerTag: "tag:brain-worker",
      advertiseTags: ["tag:brain"],
      enableSsh: false
    }
  };
  await writeText(out, stringifySimpleYaml(current));
  console.log(`wrote current-install compatibility config: ${out}`);
  console.log(`existing live clone remains untouched: ${cfg.paths.sharedBrainRoot}/live/shared-brain`);
}

async function commandSmoke(args: ParsedArgs): Promise<void> {
  const profile = (flag(args, "profile") || "single-node") as Profile;
  const root = abs(flag(args, "root") || join(tmpdir(), `brainstack-smoke-${profile}-${Date.now()}`));
  const cfg = await loadConfig(flag(args, "config"), profile, root);
  await commandRender({ ...args, flags: { ...args.flags, out: join(root, "rendered"), root, profile } });
  await commandInit({ ...args, flags: { ...args.flags, root, profile } });
  await commandDoctor({ ...args, flags: { ...args.flags, root, profile } });
  if (runsBraind(cfg)) {
    run([cfg.runtime.bunBin, "--no-env-file", "run", join(PRODUCT_ROOT, "apps", "braind", "src", "reindex.ts"), "--quiet"], {
      env: {
        SHARED_BRAIN_REPO_ROOT: cfg.repos.serve,
        SHARED_BRAIN_WRITE_REPO_ROOT: cfg.repos.staging,
        BRAIN_BLOB_STORE: cfg.repos.blobs
      }
    });
  }
  console.log(`smoke ${profile}: ok`);
  console.log(`smoke root: ${root}`);
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  switch (args.command) {
    case "help":
    case "--help":
    case "-h":
      console.log(usage());
      return;
    case "init":
      return await commandInit(args);
    case "provision":
      return await commandProvision(args);
    case "upgrade":
      return await commandApplyRuntime(args, true);
    case "apply-runtime":
      return await commandApplyRuntime(args, false);
    case "doctor":
      return await commandDoctor(args);
    case "backup":
      return await commandBackup(args);
    case "restore":
      return await commandRestore(args);
    case "destroy":
      return await commandDestroy(args);
    case "render":
      return await commandRender(args);
    case "bootstrap-client":
      return await commandBootstrapClient(args);
    case "join-worker":
      return await commandJoinWorker(args);
    case "rotate-token":
      return await commandRotateToken(args);
    case "migrate-current-install":
      return await commandMigrateCurrentInstall(args);
    case "smoke":
      return await commandSmoke(args);
    default:
      throw new Error(`Unknown command: ${args.command}\n${usage()}`);
  }
}

if (import.meta.main) {
  try {
    await main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}
