#!/usr/bin/env bun
import { existsSync, readFileSync, statSync } from "node:fs";
import { chmod, cp, lstat, mkdir, mkdtemp, readFile, readdir, readlink, rm, symlink, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";

type Profile = "single-node" | "control" | "worker" | "client-macos" | "private-journal";
type SeedMode = "empty-only" | "missing" | "force";
type HarnessName = "codex" | "claude";
type DestroyScope = "control" | "worker" | "client" | "all";
type CheckStatus = "PASS" | "WARN" | "FAIL";

const CONFIG_SCHEMA_VERSION = 1;
const MIN_BUN_VERSION = "1.3.11";

interface ParsedArgs {
  command: string;
  positional: string[];
  flags: Record<string, string | boolean | string[]>;
}

interface BrainstackWorkerConfig {
  name: string;
  transport: string;
  sshTarget?: string | null;
  sshUser?: string | null;
  managedRepoRoot: string;
  managedHostRoot: string;
  managedScratchRoot: string;
  harness?: HarnessName | null;
  harnessBin?: string | null;
  notes?: string;
  capabilities?: string[];
}

interface ManagedArtifact {
  path: string;
  kind: "file" | "dir" | "symlink" | "service" | "repo" | "tailscale-serve";
  scope: DestroyScope;
  reason: string;
  optional?: boolean;
}

interface ManagedArtifactsManifest {
  schema_version: number;
  product: "brainstack";
  created_at: string;
  updated_at: string;
  profile: Profile;
  config_root: string;
  state_root: string;
  artifacts: ManagedArtifact[];
  manual_leftovers: string[];
}

interface DoctorCheck {
  section: string;
  name: string;
  status: CheckStatus;
  detail: string;
  remediation?: string;
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
    workers: BrainstackWorkerConfig[];
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
  brainctl doctor --config brainstack.yaml [--profile ...] [--json] [--workers] [--deep]
  brainctl updates --config brainstack.yaml [--profile ...]
  brainctl backup --config brainstack.yaml [--out DIR] [--pause-telemux]
  brainctl restore --backup DIR_OR_TGZ --target DIR [--apply]
  brainctl render --config brainstack.yaml --profile ... --out DIR
  brainctl bootstrap-client --profile client-macos --config brainstack.yaml --out DIR
  brainctl join-worker --config brainstack.yaml --worker WORKER_HOST [--ssh-user USER] [--out DIR]
  brainctl rotate-token --kind import|admin|telegram-placeholder --config brainstack.yaml [--env FILE]
  brainctl import-text --config brainstack.yaml --title TITLE --text TEXT --source-harness HARNESS --source-machine MACHINE [--source-type note]
  brainctl propose --config brainstack.yaml --title TITLE --body BODY
  brainctl outbox status|list|flush|purge --config brainstack.yaml [--yes]
  brainctl destroy --config brainstack.yaml [--profile ...] --dry-run|--yes [--scope control|worker|client|all] [--remove-shared-brain] [--remove-private-brain] [--remove-tailscale-serve]
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

function optionalStringAt(input: Record<string, unknown>, key: string): string | null {
  const value = input[key];
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function normalizeWorkerConfig(input: Record<string, unknown>, cfg: Pick<BrainstackConfig, "machine" | "telemux" | "harness">): BrainstackWorkerConfig {
  const name = stringAt(input, "name", cfg.machine.name);
  const transport = stringAt(input, "transport", name === cfg.machine.name ? "local" : "ssh");
  const harnessValue = optionalStringAt(input, "harness");
  const capabilities = arrayAt(input, "capabilities").map(String).filter(Boolean);
  return {
    name,
    transport,
    sshTarget: optionalStringAt(input, "sshTarget"),
    sshUser: optionalStringAt(input, "sshUser"),
    managedRepoRoot: stringAt(input, "managedRepoRoot", join(cfg.telemux.factoryRoot, "repos")),
    managedHostRoot: stringAt(input, "managedHostRoot", join(cfg.telemux.factoryRoot, "hostctx")),
    managedScratchRoot: stringAt(input, "managedScratchRoot", join(cfg.telemux.factoryRoot, "scratch")),
    harness: harnessValue ? normalizeHarness(harnessValue, cfg.harness.name) : null,
    harnessBin: optionalStringAt(input, "harnessBin"),
    notes: optionalStringAt(input, "notes") || undefined,
    capabilities: capabilities.length ? capabilities : undefined
  };
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
  const workerInputs = arrayAt(telemux, "workers") as Array<Record<string, unknown>>;
  const remoteSsh = `${machineUser}@${machineName}:${join(sharedBrainRoot, "bare", "shared-brain.git")}`;
  const harnessName = normalizeHarness(stringAt(harness, "name", stringAt(telemux, "harness", "codex")));
  const harnessBin = stringAt(harness, "bin", harnessName);

  const cfg: BrainstackConfig = {
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
      workers: []
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
  cfg.telemux.workers = workerInputs.map((entry) => normalizeWorkerConfig(entry, cfg));
  return cfg;
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
    "FACTORY_TEXT_COALESCE_MS=1500",
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

function defaultWorkers(cfg: BrainstackConfig): BrainstackWorkerConfig[] {
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
      managedScratchRoot: join(cfg.telemux.factoryRoot, "scratch"),
      harness: cfg.harness.name,
      harnessBin: null,
      capabilities: ["control-local"]
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
        ip: ["tcp:22", "tcp:443", "icmp:*"]
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

function managedManifestPath(cfg: BrainstackConfig): string {
  return join(cfg.paths.configRoot, "managed-artifacts.json");
}

function destroyScopeFromProfile(profile: Profile): DestroyScope {
  if (profile === "worker") return "worker";
  if (profile === "client-macos") return "client";
  return "control";
}

function artifactInScope(artifact: ManagedArtifact, scope: DestroyScope): boolean {
  return scope === "all" || artifact.scope === scope;
}

function expectedManagedArtifacts(cfg: BrainstackConfig): ManagedArtifact[] {
  const artifacts: ManagedArtifact[] = [
    { path: cfg.paths.configRoot, kind: "dir", scope: "all", reason: "brainstack config root" },
    { path: cfg.paths.stateRoot, kind: "dir", scope: "all", reason: "brainstack state root" },
    { path: managedManifestPath(cfg), kind: "file", scope: "all", reason: "brainstack ownership manifest", optional: true },
    { path: join(cfg.paths.stateRoot, "rendered"), kind: "dir", scope: "all", reason: "rendered brainstack artifacts", optional: true }
  ];

  if (runsBraind(cfg)) {
    artifacts.push(
      { path: join(cfg.paths.configRoot, "braind.runtime.env"), kind: "file", scope: "control", reason: "generated braind runtime env" },
      { path: join(cfg.paths.systemdUserRoot, "braind.service"), kind: "service", scope: "control", reason: "generated braind user service" },
      { path: join(cfg.repos.bare, "hooks", "post-receive"), kind: "file", scope: "control", reason: "generated shared-brain git hook", optional: true },
      { path: join(cfg.repos.bare, "hooks", "pre-receive"), kind: "file", scope: "control", reason: "generated shared-brain git hook", optional: true }
    );
  }

  if (cfg.telemux.enabled) {
    artifacts.push(
      { path: join(cfg.paths.configRoot, "telemux.runtime.env"), kind: "file", scope: "control", reason: "generated telemux runtime env" },
      { path: join(cfg.paths.configRoot, "workers.json"), kind: "file", scope: "control", reason: "generated worker config render" },
      { path: join(cfg.paths.systemdUserRoot, "telemux.service"), kind: "service", scope: "control", reason: "generated telemux user service" },
      { path: cfg.telemux.controlRoot, kind: "dir", scope: "control", reason: "telemux control state root", optional: true },
      { path: cfg.telemux.factoryRoot, kind: "dir", scope: "control", reason: "telemux factory workspace root", optional: true }
    );
  }

  artifacts.push(
    { path: join(cfg.paths.configRoot, "client-bootstrap"), kind: "dir", scope: cfg.profile === "worker" ? "worker" : "client", reason: "product-owned client bootstrap files", optional: true },
    { path: clientEnvPathAbs(cfg), kind: "file", scope: cfg.profile === "worker" ? "worker" : "client", reason: "shared-brain client env created from example", optional: true },
    { path: join(cfg.paths.home, ".codex", "AGENTS.md"), kind: "symlink", scope: cfg.profile === "worker" ? "worker" : "client", reason: "Codex shared-brain include symlink", optional: true },
    { path: join(cfg.paths.home, ".claude", "CLAUDE.md"), kind: "file", scope: cfg.profile === "worker" ? "worker" : "client", reason: "Claude shared-brain import stub", optional: true },
    { path: join(cfg.paths.home, ".cursor", "rules", "shared-brain.md"), kind: "file", scope: cfg.profile === "worker" ? "worker" : "client", reason: "Cursor shared-brain rule", optional: true }
  );

  return artifacts;
}

function manualLeftovers(cfg: BrainstackConfig): string[] {
  return [
    "Bun/Git/OpenSSH/Tailscale packages are never removed by brainctl.",
    "Tailscale enrollment, auth keys, and device tags are never removed by brainctl.",
    "Codex/Claude binaries, authentication, and permission/yolo settings are never removed by brainctl.",
    "Passwordless sudo policy is never removed by brainctl.",
    `${cfg.paths.sharedBrainRoot} is kept unless --remove-shared-brain is explicitly passed.`,
    `${cfg.paths.privateBrainRoot} is kept unless --remove-private-brain is explicitly passed.`
  ];
}

async function writeManagedManifest(cfg: BrainstackConfig): Promise<void> {
  const manifest: ManagedArtifactsManifest = {
    schema_version: 1,
    product: "brainstack",
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    profile: cfg.profile,
    config_root: cfg.paths.configRoot,
    state_root: cfg.paths.stateRoot,
    artifacts: expectedManagedArtifacts(cfg),
    manual_leftovers: manualLeftovers(cfg)
  };
  await writeText(managedManifestPath(cfg), `${JSON.stringify(manifest, null, 2)}\n`, 0o600);
}

async function loadManagedManifest(cfg: BrainstackConfig): Promise<ManagedArtifactsManifest> {
  const path = managedManifestPath(cfg);
  if (existsSync(path)) {
    return JSON.parse(await readFile(path, "utf8")) as ManagedArtifactsManifest;
  }
  return {
    schema_version: 1,
    product: "brainstack",
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    profile: cfg.profile,
    config_root: cfg.paths.configRoot,
    state_root: cfg.paths.stateRoot,
    artifacts: expectedManagedArtifacts(cfg),
    manual_leftovers: manualLeftovers(cfg)
  };
}

function sha256Hex(text: string): string {
  return createHash("sha256").update(text).digest("hex");
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
  } else {
    console.log(`Codex already has ${codexAgents}; append the real shared-brain guidance with:`);
    console.log(`cat ${join(bootstrapRoot, "codex-shared-brain.include.md")} >> ${codexAgents}`);
  }

  const claudeHome = join(cfg.paths.home, ".claude");
  await ensureDir(claudeHome);
  const claudeFile = join(claudeHome, "CLAUDE.md");
  if (await writeIfMissing(claudeFile, `@${join(bootstrapRoot, "claude-user-CLAUDE.md")}\n`)) {
    touched.push(claudeFile);
  } else {
    console.log(`Claude already has ${claudeFile}; append this exact import line manually:`);
    console.log(`@${join(bootstrapRoot, "claude-user-CLAUDE.md")}`);
  }

  const cursorRules = join(cfg.paths.home, ".cursor", "rules");
  await ensureDir(cursorRules);
  const cursorRule = join(cursorRules, "shared-brain.md");
  if (await writeIfMissing(cursorRule, bootstrapFiles["cursor-user-rule.md"])) {
    touched.push(cursorRule);
  } else {
    console.log(`Cursor shared-brain rule already exists at ${cursorRule}; append or merge the actual rule content with:`);
    console.log(`cat ${join(bootstrapRoot, "cursor-user-rule.md")} >> ${cursorRule}`);
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
  const cfg = await loadConfig(out, profile);
  await ensureDir(cfg.paths.configRoot);
  await writeManagedManifest(cfg);
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
  console.log(`ownership manifest written: ${managedManifestPath(cfg)}`);
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
  await writeManagedManifest(cfg);
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
  await writeManagedManifest(cfg);
  touched.push(managedManifestPath(cfg));
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

function check(status: CheckStatus, section: string, name: string, detail: string, remediation?: string): DoctorCheck {
  return { status, section, name, detail, remediation };
}

function commandOk(name: string): boolean {
  return run(["bash", "-lc", `command -v ${shellSingleQuote(name)}`], { check: false }).code === 0;
}

function commandVersion(name: string): string {
  const proc = name === "ssh" ? run([name, "-V"], { check: false }) : run([name, "--version"], { check: false });
  return (proc.stdout || proc.stderr).trim().split(/\r?\n/)[0] || "unknown";
}

function commandHelp(name: string, args: string[] = ["--help"]): string {
  const proc = run([name, ...args], { check: false });
  return `${proc.stdout}\n${proc.stderr}`;
}

function harnessCompatibility(name: HarnessName, bin: string): DoctorCheck {
  if (!commandOk(bin)) {
    return check("FAIL", "versions", `${name}-harness`, `${bin} not found in PATH`, installHint(name));
  }
  const version = commandVersion(bin);
  const help = name === "codex" ? commandHelp(bin, ["exec", "--help"]) : commandHelp(bin, ["--help"]);
  const required =
    name === "codex"
      ? ["--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check"]
      : ["--dangerously-skip-permissions", "--permission-mode", "--output-format"];
  const missing = required.filter((needle) => !help.includes(needle));
  if (missing.length) {
    return check(
      "FAIL",
      "versions",
      `${name}-harness`,
      `${version}; missing required CLI surface: ${missing.join(", ")}`,
      `Update ${name} manually, then rerun doctor. ${installHint(name)}`
    );
  }
  return check("PASS", "versions", `${name}-harness`, `${version}; required CLI surface present`);
}

function envHasKey(path: string, key: string): boolean {
  if (!existsSync(path)) {
    return false;
  }
  const text = readFileSync(path, "utf8");
  return new RegExp(`^${key}=.+`, "m").test(text);
}

function gitClean(path: string): DoctorCheck {
  if (!gitExists(path)) {
    return check("FAIL", "git", `git-clean:${path}`, "not a git worktree");
  }
  const dirty = run(["git", "status", "--porcelain"], { cwd: path, check: false }).stdout.trim();
  return dirty
    ? check("WARN", "git", `git-clean:${path}`, "worktree has uncommitted changes", "Inspect before applying runtime changes.")
    : check("PASS", "git", `git-clean:${path}`, "clean");
}

function outboxRoot(cfg: BrainstackConfig): string {
  return join(cfg.paths.stateRoot, "outbox", brainInstanceId(cfg));
}

function brainInstanceId(cfg: BrainstackConfig): string {
  const base = cfg.brain.publicBaseUrl || cfg.client.remoteSsh || cfg.client.localPath;
  return sha256Hex(base).slice(0, 16);
}

async function countOutboxItems(cfg: BrainstackConfig): Promise<number> {
  const root = outboxRoot(cfg);
  if (!existsSync(root)) {
    return 0;
  }
  return (await readdir(root)).filter((name) => name.endsWith(".json")).length;
}

async function collectDoctorChecks(cfg: BrainstackConfig, args: ParsedArgs): Promise<DoctorCheck[]> {
  const checks: DoctorCheck[] = [];
  const bunVersion = installedBunVersion();
  checks.push(
    bunVersion && compareVersions(bunVersion, MIN_BUN_VERSION) >= 0
      ? check("PASS", "versions", "bun", `Bun ${bunVersion}; required >= ${MIN_BUN_VERSION}`)
      : check("FAIL", "versions", "bun", bunVersion ? `Bun ${bunVersion}; required >= ${MIN_BUN_VERSION}` : "Bun runtime missing", installHint("bun"))
  );
  checks.push(existsSync(cfg.runtime.bunBin) ? check("PASS", "versions", "bun-bin", cfg.runtime.bunBin) : check("FAIL", "versions", "bun-bin", `${cfg.runtime.bunBin} does not exist`));
  for (const name of ["git", "ssh", "tailscale"]) {
    checks.push(commandOk(name) ? check("PASS", "versions", name, commandVersion(name)) : check("FAIL", "versions", name, "missing", installHint(name)));
  }
  checks.push(harnessCompatibility(cfg.harness.name, cfg.harness.bin));
  if (hasFlag(args, "deep")) {
    const sudo = run(["sudo", "-n", "true"], { check: false });
    checks.push(sudo.code === 0 ? check("PASS", "privileges", "sudo-noninteractive", "sudo -n true works") : check("FAIL", "privileges", "sudo-noninteractive", "sudo -n true failed", "Configure passwordless sudo if this profile requires privileged harness operations."));
    try {
      await testHarnessSudo(cfg.harness);
      checks.push(check("PASS", "privileges", "harness-bypass-sudo", `${cfg.harness.name} proved bypass/yolo sudo execution`));
    } catch (error) {
      checks.push(check("FAIL", "privileges", "harness-bypass-sudo", error instanceof Error ? error.message : String(error), `Verify ${cfg.harness.name} bypass/yolo permissions manually.`));
    }
  }

  checks.push(check("PASS", "config", "schema", `schema_version=${cfg.schemaVersion}`));
  checks.push(existsSync(cfg.paths.configRoot) ? check("PASS", "config", "config-root", cfg.paths.configRoot) : check("WARN", "config", "config-root", `${cfg.paths.configRoot} missing`, "Run brainctl init/apply-runtime after provisioning."));
  checks.push(existsSync(cfg.paths.stateRoot) ? check("PASS", "config", "state-root", cfg.paths.stateRoot) : check("WARN", "config", "state-root", `${cfg.paths.stateRoot} missing`, "Run brainctl init/apply-runtime after provisioning."));
  checks.push(existsSync(managedManifestPath(cfg)) ? check("PASS", "config", "ownership-manifest", managedManifestPath(cfg)) : check("WARN", "config", "ownership-manifest", "missing", "Run brainctl init/apply-runtime so destroy has a deterministic manifest."));

  if (runsBraind(cfg)) {
    checks.push(existsSync(join(cfg.paths.configRoot, "braind.runtime.env")) ? check("PASS", "secrets", "braind-runtime-env", "present") : check("WARN", "secrets", "braind-runtime-env", "missing"));
    checks.push(envHasKey(join(cfg.paths.configRoot, "braind.secrets.env"), "BRAIN_IMPORT_TOKEN") ? check("PASS", "secrets", "brain-import-token", "present") : check("WARN", "secrets", "brain-import-token", "missing or empty"));
    checks.push(envHasKey(join(cfg.paths.configRoot, "braind.secrets.env"), "BRAIN_ADMIN_TOKEN") ? check("PASS", "secrets", "brain-admin-token", "present") : check("WARN", "secrets", "brain-admin-token", "missing or empty"));
    checks.push(existsSync(cfg.repos.bare) ? check("PASS", "git", "bare-repo", cfg.repos.bare) : check("WARN", "git", "bare-repo", `${cfg.repos.bare} missing`));
    if (gitExists(cfg.repos.staging)) checks.push(gitClean(cfg.repos.staging)); else checks.push(check("WARN", "git", "staging-clone", `${cfg.repos.staging} missing`));
    if (gitExists(cfg.repos.serve)) checks.push(gitClean(cfg.repos.serve)); else checks.push(check("WARN", "git", "serve-clone", `${cfg.repos.serve} missing`));
    try {
      const response = await fetch(`http://${cfg.brain.bind}:${cfg.brain.port}/health`, { signal: AbortSignal.timeout(2000) });
      checks.push(response.ok ? check("PASS", "health", "braind-health", `HTTP ${response.status}`) : check("WARN", "health", "braind-health", `HTTP ${response.status}`));
    } catch (error) {
      checks.push(check("WARN", "health", "braind-health", error instanceof Error ? error.message : String(error), "Start braind or ignore before runtime is installed."));
    }
  }

  if (cfg.telemux.enabled) {
    checks.push(envHasKey(join(cfg.paths.configRoot, "telemux.secrets.env"), "FACTORY_TELEGRAM_BOT_TOKEN") ? check("PASS", "secrets", "telegram-token", "present") : check("WARN", "secrets", "telegram-token", "missing or empty"));
    try {
      const response = await fetch(`http://${cfg.telemux.dashboardHost}:${cfg.telemux.dashboardPort}/health`, { signal: AbortSignal.timeout(1500) });
      checks.push(response.ok ? check("PASS", "health", "telemux-health", `HTTP ${response.status}`) : check("WARN", "health", "telemux-health", `HTTP ${response.status}`));
    } catch (error) {
      checks.push(check("WARN", "health", "telemux-health", error instanceof Error ? error.message : String(error)));
    }
  }

  if (usesUserServices(cfg) && commandOk("loginctl")) {
    const linger = run(["loginctl", "show-user", cfg.machine.user, "--property=Linger", "--value"], { check: false });
    checks.push(
      linger.stdout.trim() === "yes"
        ? check("PASS", "services", "user-service-linger", `linger enabled for ${cfg.machine.user}`)
        : check("WARN", "services", "user-service-linger", `linger disabled for ${cfg.machine.user}`, `sudo loginctl enable-linger ${cfg.machine.user}`)
    );
  }

  if (commandOk("tailscale")) {
    const status = run(["tailscale", "status", "--json"], { check: false });
    checks.push(status.code === 0 ? check("PASS", "tailscale", "status", "tailscale status --json succeeded") : check("WARN", "tailscale", "status", (status.stderr || status.stdout).trim()));
    const serve = run(["tailscale", "serve", "status"], { check: false });
    checks.push(serve.code === 0 ? check("PASS", "tailscale", "serve", "serve status available") : check("WARN", "tailscale", "serve", (serve.stderr || serve.stdout).trim()));
  }

  checks.push(check("PASS", "outbox", "queued-items", `${await countOutboxItems(cfg)} queued item(s)`));

  if (hasFlag(args, "workers") || hasFlag(args, "deep")) {
    checks.push(...(await workerDoctorChecks(cfg, hasFlag(args, "deep"))));
  }

  return checks;
}

function formatDoctorChecks(checks: DoctorCheck[]): string {
  return checks
    .map((item) => `${item.status} [${item.section}] ${item.name}: ${item.detail}${item.remediation ? `\n  remediation: ${item.remediation}` : ""}`)
    .join("\n");
}

function workerHarnessFamily(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, contextOverride?: HarnessName | null): HarnessName {
  return contextOverride || worker.harness || cfg.harness.name;
}

function workerHarnessBin(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, family: HarnessName, contextBin?: string | null): string {
  if (contextBin?.trim()) return contextBin.trim();
  if (worker.harnessBin?.trim()) return worker.harnessBin.trim();
  if (worker.transport === "local" && family === cfg.harness.name) return cfg.harness.bin;
  return family;
}

function workerRemoteTarget(worker: BrainstackWorkerConfig): string {
  const target = worker.sshTarget || worker.name;
  return worker.sshUser ? `${worker.sshUser}@${target}` : target;
}

function runWorkerShell(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, script: string, timeoutSeconds = 10) {
  if (worker.transport === "local") {
    return run(["bash", "-lc", script], { check: false });
  }
  const sshArgs =
    worker.transport === "tailscale-ssh"
      ? ["tailscale", "ssh", workerRemoteTarget(worker), "bash", "-lc", script]
      : [
          "ssh",
          "-o",
          "BatchMode=yes",
          "-o",
          "ConnectTimeout=8",
          "-o",
          "StrictHostKeyChecking=accept-new",
          workerRemoteTarget(worker),
          "bash",
          "-lc",
          script
        ];
  return run(["timeout", `${timeoutSeconds}s`, ...sshArgs], { check: false });
}

async function workerDoctorChecks(cfg: BrainstackConfig, deep: boolean): Promise<DoctorCheck[]> {
  const checks: DoctorCheck[] = [];
  for (const worker of defaultWorkers(cfg)) {
    const family = workerHarnessFamily(cfg, worker);
    const bin = workerHarnessBin(cfg, worker, family);
    const required =
      family === "codex"
        ? ["--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check"]
        : ["--dangerously-skip-permissions", "--permission-mode", "--output-format"];
    const script = [
      "set -euo pipefail",
      `harness_bin=${quoteForBash(bin)}`,
      `harness_family=${quoteForBash(family)}`,
      "printf 'worker=%s\\n' \"$(hostname)\"",
      "if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then printf 'sudo=ok\\n'; else printf 'sudo=fail\\n'; fi",
      "if ! command -v \"$harness_bin\" >/dev/null 2>&1; then printf 'harness_bin=missing\\n'; exit 7; fi",
      "printf 'harness_bin=%s\\n' \"$(command -v \"$harness_bin\")\"",
      "\"$harness_bin\" --version 2>&1 | head -n 1 | sed 's/^/version=/' || true",
      "if [ \"$harness_family\" = codex ]; then help=\"$($harness_bin exec --help 2>&1 || true)\"; else help=\"$($harness_bin --help 2>&1 || true)\"; fi",
      ...required.map((needle) => `case "$help" in *${quoteForBash(needle)}*) printf 'flag:${needle}=ok\\n' ;; *) printf 'flag:${needle}=missing\\n' ;; esac`),
      deep
        ? [
            "tmpdir=\"$(mktemp -d)\"",
            "trap 'rm -rf \"$tmpdir\"' EXIT",
            "cd \"$tmpdir\"",
            "prompt='Run exactly this shell command and then stop: sudo -n true && printf BRAINSTACK_HARNESS_SUDO_OK'",
            "if [ \"$harness_family\" = codex ]; then",
            "  output=\"$(printf '%s' \"$prompt\" | \"$harness_bin\" exec --skip-git-repo-check --dangerously-bypass-approvals-and-sandbox - 2>&1 || true)\"",
            "else",
            "  output=\"$(printf '%s' \"$prompt\" | \"$harness_bin\" -p --dangerously-skip-permissions --permission-mode bypassPermissions --output-format text 2>&1 || true)\"",
            "fi",
            "case \"$output\" in *BRAINSTACK_HARNESS_SUDO_OK*) printf 'harness_sudo=ok\\n' ;; *) printf 'harness_sudo=fail\\n' ;; esac"
          ].join("\n")
        : "printf 'deep=skipped\\n'"
    ].join("\n");
    const result = runWorkerShell(cfg, worker, script, deep ? 30 : 12);
    const combined = `${result.stdout}\n${result.stderr}`.trim();
    if (result.code !== 0) {
      checks.push(check("FAIL", "workers", `worker:${worker.name}`, combined || `exit ${result.code}`, `Verify SSH and ${family} on the worker.`));
      continue;
    }
    checks.push(check("PASS", "workers", `worker:${worker.name}`, `reachable via ${worker.transport}; ${family} bin=${bin}`));
    checks.push(combined.includes("sudo=ok") ? check("PASS", "workers", `worker:${worker.name}:sudo`, "sudo -n true works") : check("WARN", "workers", `worker:${worker.name}:sudo`, "sudo -n true failed", "Configure passwordless sudo only if this worker profile needs privileged operations."));
    const missingFlags = required.filter((needle) => combined.includes(`flag:${needle}=missing`));
    checks.push(
      missingFlags.length
        ? check("FAIL", "workers", `worker:${worker.name}:harness-compat`, `missing ${missingFlags.join(", ")}`, `Update ${family} on ${worker.name}.`)
        : check("PASS", "workers", `worker:${worker.name}:harness-compat`, `${family} required CLI surface present`)
    );
    if (deep) {
      checks.push(
        combined.includes("harness_sudo=ok")
          ? check("PASS", "workers", `worker:${worker.name}:harness-sudo`, `${family} proved sudo in bypass/yolo mode`)
          : check("FAIL", "workers", `worker:${worker.name}:harness-sudo`, `${family} did not prove sudo in bypass/yolo mode`, `Inspect ${family} auth and permission bypass settings on ${worker.name}.`)
      );
    }
  }
  return checks;
}

function quoteForBash(value: string): string {
  return shellSingleQuote(value);
}

async function commandDoctor(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const checks = await collectDoctorChecks(cfg, args);
  if (hasFlag(args, "json")) {
    console.log(JSON.stringify({ ok: !checks.some((item) => item.status === "FAIL"), checks }, null, 2));
  } else {
    console.log(formatDoctorChecks(checks));
  }
  const failures = checks.filter((item) => item.status === "FAIL");
  if (failures.length) {
    throw new Error(`doctor failed: ${failures.map((item) => item.name).join(", ")}`);
  }
}

function readEnvFile(path: string): Record<string, string> {
  if (!existsSync(path)) {
    return {};
  }
  const env: Record<string, string> = {};
  for (const line of readFileSync(path, "utf8").split(/\r?\n/)) {
    const match = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (match) {
      env[match[1]] = match[2].replace(/^['"]|['"]$/g, "");
    }
  }
  return env;
}

function brainWriteConfig(cfg: BrainstackConfig): { baseUrl: string; token: string } {
  const env = readEnvFile(clientEnvPathAbs(cfg));
  const baseUrl = process.env.BRAIN_BASE_URL || env.BRAIN_BASE_URL || cfg.brain.publicBaseUrl;
  const tokenValue = process.env.BRAIN_IMPORT_TOKEN || env.BRAIN_IMPORT_TOKEN || process.env.BRAIN_WRITE_TOKEN || "";
  return { baseUrl, token: tokenValue };
}

function brainWriteTimeoutMs(): number {
  const value = Number(process.env.BRAINSTACK_BRAIN_WRITE_TIMEOUT_MS || "15000");
  return Number.isFinite(value) && value > 0 ? value : 15000;
}

interface OutboxItem {
  id: string;
  endpoint: "import" | "propose";
  url: string;
  payload: Record<string, unknown>;
  created_at: string;
  source_machine: string;
  source_harness: string;
  retry_count: number;
  idempotency_key: string;
  last_error: string | null;
}

async function queueOutboxItem(cfg: BrainstackConfig, item: Omit<OutboxItem, "id" | "created_at" | "retry_count" | "idempotency_key" | "last_error">, error: string): Promise<string> {
  const created = new Date().toISOString();
  const idempotencyKey = sha256Hex(JSON.stringify({ endpoint: item.endpoint, payload: item.payload }));
  const id = `${created.replace(/[:.]/g, "-")}-${idempotencyKey.slice(0, 16)}`;
  const root = outboxRoot(cfg);
  await ensureDir(root);
  const path = join(root, `${id}.json`);
  if (!existsSync(path)) {
    await writeText(
      path,
      `${JSON.stringify(
        {
          ...item,
          id,
          created_at: created,
          retry_count: 0,
          idempotency_key: idempotencyKey,
          last_error: error
        } satisfies OutboxItem,
        null,
        2
      )}\n`,
      0o600
    );
  }
  return path;
}

async function postBrainWriteOrQueue(cfg: BrainstackConfig, endpoint: "import" | "propose", payload: Record<string, unknown>): Promise<void> {
  const { baseUrl, token: writeToken } = brainWriteConfig(cfg);
  if (!baseUrl || !writeToken) {
    const queued = await queueOutboxItem(
      cfg,
      {
        endpoint,
        url: baseUrl || "",
        payload,
        source_machine: String(payload.source_machine || cfg.machine.name),
        source_harness: String(payload.source_harness || cfg.harness.name)
      },
      "brain base URL or import token is missing"
    );
    console.warn(`shared-brain write queued: ${queued}`);
    return;
  }

  try {
    const response = await fetch(new URL(`/api/${endpoint}`, baseUrl).toString(), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${writeToken}`,
        "Content-Type": "application/json",
        "Idempotency-Key": sha256Hex(JSON.stringify({ endpoint, payload }))
      },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(brainWriteTimeoutMs())
    });
    if (!response.ok) {
      throw new Error(`brain returned HTTP ${response.status}: ${(await response.text()).slice(0, 500)}`);
    }
    console.log(`shared-brain ${endpoint} accepted`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const queued = await queueOutboxItem(
      cfg,
      {
        endpoint,
        url: baseUrl,
        payload,
        source_machine: String(payload.source_machine || cfg.machine.name),
        source_harness: String(payload.source_harness || cfg.harness.name)
      },
      message
    );
    console.warn(`shared-brain write queued: ${queued}`);
  }
}

async function listOutboxItems(cfg: BrainstackConfig): Promise<Array<{ path: string; item: OutboxItem }>> {
  const root = outboxRoot(cfg);
  if (!existsSync(root)) {
    return [];
  }
  const names = (await readdir(root)).filter((name) => name.endsWith(".json")).sort();
  const items: Array<{ path: string; item: OutboxItem }> = [];
  for (const name of names) {
    const path = join(root, name);
    items.push({ path, item: JSON.parse(await readFile(path, "utf8")) as OutboxItem });
  }
  return items;
}

async function flushOutbox(cfg: BrainstackConfig): Promise<{ flushed: number; kept: number }> {
  const { token: writeToken } = brainWriteConfig(cfg);
  const items = await listOutboxItems(cfg);
  let flushed = 0;
  let kept = 0;
  for (const { path, item } of items) {
    const baseUrl = item.url || brainWriteConfig(cfg).baseUrl;
    if (!baseUrl || !writeToken) {
      kept += 1;
      continue;
    }
    try {
      const response = await fetch(new URL(`/api/${item.endpoint}`, baseUrl).toString(), {
        method: "POST",
        headers: {
          Authorization: `Bearer ${writeToken}`,
          "Content-Type": "application/json",
          "Idempotency-Key": item.idempotency_key
        },
        body: JSON.stringify(item.payload),
        signal: AbortSignal.timeout(brainWriteTimeoutMs())
      });
      if (response.ok || response.status === 409) {
        await rm(path, { force: true });
        flushed += 1;
      } else {
        item.retry_count += 1;
        item.last_error = `HTTP ${response.status}: ${(await response.text()).slice(0, 300)}`;
        await writeText(path, `${JSON.stringify(item, null, 2)}\n`, 0o600);
        kept += 1;
      }
    } catch (error) {
      item.retry_count += 1;
      item.last_error = error instanceof Error ? error.message : String(error);
      await writeText(path, `${JSON.stringify(item, null, 2)}\n`, 0o600);
      kept += 1;
    }
  }
  return { flushed, kept };
}

async function commandImportText(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const title = flag(args, "title");
  const text = flag(args, "text") || args.positional.join(" ");
  if (!title || !text) {
    throw new Error("import-text requires --title and --text");
  }
  await postBrainWriteOrQueue(cfg, "import", {
    title,
    text,
    source_harness: flag(args, "source-harness") || cfg.harness.name,
    source_machine: flag(args, "source-machine") || cfg.machine.name,
    source_type: flag(args, "source-type") || "note",
    tags: flag(args, "tags") ? String(flag(args, "tags")).split(",").map((item) => item.trim()).filter(Boolean) : []
  });
}

async function commandPropose(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const title = flag(args, "title");
  const body = flag(args, "body") || args.positional.join(" ");
  if (!title || !body) {
    throw new Error("propose requires --title and --body");
  }
  await postBrainWriteOrQueue(cfg, "propose", {
    title,
    body,
    source_harness: flag(args, "source-harness") || cfg.harness.name,
    source_machine: flag(args, "source-machine") || cfg.machine.name
  });
}

async function commandOutbox(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const sub = args.positional[0] || "status";
  const items = await listOutboxItems(cfg);
  switch (sub) {
    case "status":
      console.log(`outbox=${outboxRoot(cfg)}`);
      console.log(`queued=${items.length}`);
      return;
    case "list":
      console.log(items.length ? items.map(({ item }) => `${item.id} ${item.endpoint} retries=${item.retry_count} ${item.source_machine}/${item.source_harness}`).join("\n") : "(empty)");
      return;
    case "flush": {
      const result = await flushOutbox(cfg);
      console.log(`flushed=${result.flushed} kept=${result.kept}`);
      return;
    }
    case "purge":
      if (!hasFlag(args, "yes")) {
        throw new Error("outbox purge is destructive; rerun with --yes");
      }
      await rm(outboxRoot(cfg), { recursive: true, force: true });
      console.log(`purged ${outboxRoot(cfg)}`);
      return;
    default:
      throw new Error("outbox subcommand must be status|list|flush|purge");
  }
}

async function commandUpdates(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const branch = run(["git", "rev-parse", "--abbrev-ref", "HEAD"], { cwd: PRODUCT_ROOT, check: false }).stdout.trim() || "unknown";
  const head = run(["git", "rev-parse", "HEAD"], { cwd: PRODUCT_ROOT, check: false }).stdout.trim() || "unknown";
  const origin = run(["git", "rev-parse", "--verify", "origin/main"], { cwd: PRODUCT_ROOT, check: false }).stdout.trim();
  const aheadBehind = origin
    ? run(["git", "rev-list", "--left-right", "--count", `HEAD...origin/main`], { cwd: PRODUCT_ROOT, check: false }).stdout.trim()
    : "unknown";
  const codex = commandOk("codex") ? commandVersion("codex") : "missing";
  const claude = commandOk("claude") ? commandVersion("claude") : "missing";
  const compat = [harnessCompatibility("codex", "codex"), harnessCompatibility("claude", "claude")];
  console.log(`brainstack_branch=${branch}`);
  console.log(`brainstack_head=${head}`);
  console.log(`origin_main=${origin || "unavailable"}`);
  console.log(`ahead_behind=${aheadBehind}`);
  console.log(`codex=${codex}`);
  console.log(`claude=${claude}`);
  for (const item of compat) {
    console.log(`${item.status} ${item.name}: ${item.detail}`);
    if (item.remediation) console.log(`  remediation: ${item.remediation}`);
  }
  console.log("manual_update_commands:");
  console.log("  brainstack: git pull --ff-only && bun install --frozen-lockfile && bun test");
  console.log(`  selected harness: ${installHint(cfg.harness.name)}`);
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
  const scope = (flag(args, "scope") || "all") as DestroyScope;
  if (!["control", "worker", "client", "all"].includes(scope)) {
    throw new Error("destroy --scope must be control|worker|client|all");
  }
  if (!dryRun && !hasFlag(args, "yes")) {
    throw new Error("destroy is destructive; rerun with --dry-run to inspect or --yes to remove brainstack-owned artifacts");
  }
  const removed: string[] = [];
  const skipped: string[] = [];
  const manual: string[] = [];
  const manifest = await loadManagedManifest(cfg);
  const bootstrapRoot = join(cfg.paths.configRoot, "client-bootstrap");
  const bootstrapFiles = clientBootstrapFiles(cfg);

  if ((scope === "all" || scope === "control") && !dryRun && commandPath("systemctl")) {
    if (runsBraind(cfg)) {
      run(["systemctl", "--user", "disable", "--now", "braind.service"], { check: false });
    }
    if (cfg.telemux.enabled) {
      run(["systemctl", "--user", "disable", "--now", "telemux.service"], { check: false });
    }
  }

  if (scope === "all" || scope === "client" || scope === "worker") {
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
  }

  const exactHandled = new Set([
    join(cfg.paths.home, ".codex", "AGENTS.md"),
    join(cfg.paths.home, ".claude", "CLAUDE.md"),
    join(cfg.paths.home, ".cursor", "rules", "shared-brain.md"),
    clientEnvPathAbs(cfg)
  ]);
  const artifacts = manifest.artifacts
    .filter((artifact) => artifactInScope(artifact, scope))
    .filter((artifact) => !exactHandled.has(artifact.path))
    .sort((left, right) => right.path.length - left.path.length);

  for (const artifact of artifacts) {
    if (artifact.kind === "tailscale-serve") {
      continue;
    }
    await removePath(artifact.path, dryRun, removed);
  }

  if (hasFlag(args, "remove-shared-brain")) {
    await removePath(cfg.paths.sharedBrainRoot, dryRun, removed);
  } else if (existsSync(cfg.paths.sharedBrainRoot)) {
    manual.push(`${cfg.paths.sharedBrainRoot} kept; pass --remove-shared-brain if this product-managed clone/repo should be deleted`);
  }
  if (hasFlag(args, "remove-private-brain")) {
    await removePath(cfg.paths.privateBrainRoot, dryRun, removed);
  } else if (existsSync(cfg.paths.privateBrainRoot)) {
    manual.push(`${cfg.paths.privateBrainRoot} kept; pass --remove-private-brain if this product-managed private brain should be deleted`);
  }
  if (hasFlag(args, "remove-tailscale-serve")) {
    removed.push("tailscale serve config");
    if (!dryRun) {
      run(["tailscale", "serve", "reset", "--yes"], { check: false });
    }
  } else {
    manual.push("Tailscale Serve config kept; pass --remove-tailscale-serve only if brainstack owns that Serve config.");
  }
  manual.push(...(manifest.manual_leftovers || manualLeftovers(cfg)));
  console.log(`${dryRun ? "dry-run destroy plan" : "destroy complete"} for ${cfg.profile} scope=${scope}`);
  console.log("removed:");
  console.log(removed.length ? removed.map((item) => `  ${item}`).join("\n") : "  (none)");
  console.log("skipped:");
  console.log(skipped.length ? skipped.map((item) => `  ${item}`).join("\n") : "  (none)");
  console.log("manual leftovers:");
  console.log(manual.length ? [...new Set(manual)].map((item) => `  ${item}`).join("\n") : "  (none)");
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
    managedScratchRoot: join(cfg.telemux.factoryRoot, "scratch"),
    harness: normalizeHarness(flag(args, "harness"), cfg.harness.name),
    harnessBin: flag(args, "harness-bin") || null,
    capabilities: flag(args, "capabilities")
      ? String(flag(args, "capabilities")).split(",").map((item) => item.trim()).filter(Boolean)
      : ["worker"]
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
    case "updates":
      return await commandUpdates(args);
    case "import-text":
      return await commandImportText(args);
    case "propose":
      return await commandPropose(args);
    case "outbox":
      return await commandOutbox(args);
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
