#!/usr/bin/env bun
import { existsSync, readFileSync, realpathSync, statSync } from "node:fs";
import { chmod, cp, lstat, mkdir, mkdtemp, readFile, readdir, readlink, realpath, rm, rmdir, stat, symlink, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { isIP } from "node:net";
import { dirname, join, resolve, sep } from "node:path";
import { hostname, tmpdir } from "node:os";
import { createInterface } from "node:readline/promises";
import {
  buildOutboxItem,
  decodeOutboxPayload,
  ensurePrivateOutboxDir,
  outboxLimitsFromEnv,
  normalizeOutboxItem as normalizeSharedOutboxItem,
  outboxItemKey as sharedOutboxItemKey,
  purgeCorruptOutboxEntries,
  sanitizedHttpError,
  scanOutbox,
  writeOutboxItem,
  type OutboxEntry,
  type OutboxItem as SharedOutboxItem
} from "../../outbox/src/outbox";
import clientEnvExample from "../../client-bootstrap/client.env.example" with { type: "text" };
import codexSharedBrainInclude from "../../client-bootstrap/codex-shared-brain.include.md" with { type: "text" };
import codexGlobalAgents from "../../client-bootstrap/codex-global-AGENTS.md" with { type: "text" };
import claudeUserClaude from "../../client-bootstrap/claude-user-CLAUDE.md" with { type: "text" };
import claudeHooksExample from "../../client-bootstrap/claude-hooks-example.json" with { type: "text" };
import cursorUserRule from "../../client-bootstrap/cursor-user-rule.md" with { type: "text" };
import sshConfigFragmentExample from "../../client-bootstrap/ssh_config_fragment.example" with { type: "text" };
import installClientScript from "../../client-bootstrap/install-client.sh" with { type: "text" };

type Profile = "single-node" | "control" | "worker" | "client-macos" | "private-journal";
const SUPPORTED_PROFILES: Profile[] = ["single-node", "control", "worker", "client-macos"];
type SeedMode = "empty-only" | "missing" | "force";
type HarnessName = "codex" | "claude";
type DestroyScope = "control" | "worker" | "client" | "all";
type CheckStatus = "PASS" | "WARN" | "FAIL";
type WorkerSshTrustMode = "pinned" | "accept-new";
type SecurityPosture = "local" | "trusted-tailnet" | "guarded";
type TrustedExposure = "none" | "tailscale-serve" | "vpn" | "manual";

const CONFIG_SCHEMA_VERSION = 1;
const MIN_BUN_VERSION = "1.3.10";

function truthyEnv(value: string | undefined): boolean {
  return ["1", "true", "yes", "on"].includes((value || "").trim().toLowerCase());
}

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
  sshTrustMode?: WorkerSshTrustMode;
  sshKnownHostsPath?: string | null;
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
  security: {
    posture: SecurityPosture;
    bindHost: string;
    trustedExposure: TrustedExposure;
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
const CLIENT_BOOTSTRAP_TEMPLATE_NAMES = [
  "client.env.example",
  "codex-shared-brain.include.md",
  "codex-global-AGENTS.md",
  "claude-user-CLAUDE.md",
  "claude-hooks-example.json",
  "cursor-user-rule.md",
  "ssh_config_fragment.example",
  "install-client.sh"
] as const;
type ClientBootstrapTemplateName = (typeof CLIENT_BOOTSTRAP_TEMPLATE_NAMES)[number];
const CLIENT_BOOTSTRAP_TEMPLATES: Record<ClientBootstrapTemplateName, string> = {
  "client.env.example": clientEnvExample,
  "codex-shared-brain.include.md": codexSharedBrainInclude,
  "codex-global-AGENTS.md": codexGlobalAgents,
  "claude-user-CLAUDE.md": claudeUserClaude,
  "claude-hooks-example.json": claudeHooksExample,
  "cursor-user-rule.md": cursorUserRule,
  "ssh_config_fragment.example": sshConfigFragmentExample,
  "install-client.sh": installClientScript
};
const ALLOWED_TOP_LEVEL_CONFIG_KEYS = new Set([
  "schema_version",
  "config_version",
  "profile",
  "runtime",
  "harness",
  "machine",
  "paths",
  "security",
  "brain",
  "repos",
  "telemux",
  "tailscale",
  "client"
]);

function usage(): string {
  return `Usage:
  brainctl init --profile single-node|control|worker|client-macos --config brainstack.yaml [--dry-run] [--root /tmp/install-root] [--seed-missing|--force-seed] [--import-token-file FILE]
  brainctl provision --profile single-node|control|worker|client-macos [--out brainstack.yaml] [--harness codex|claude] [--harness-bin PATH_OR_NAME] [--enable-telemux] [--enroll-tailscale] [--tailscale-tag tag:brain] [--brain-base-url URL] [--brain-remote SSH_OR_PATH] [--require-harness-sudo] [--test-bot]
  brainctl upgrade --config brainstack.yaml [--profile ...] [--dry-run] [--root /tmp/install-root]
  brainctl apply-runtime --config brainstack.yaml [--profile ...] [--dry-run] [--root /tmp/install-root]
  brainctl doctor --config brainstack.yaml [--profile ...] [--json] [--workers] [--deep] [--write-smoke]
  brainctl updates --config brainstack.yaml [--profile ...]
  brainctl expose tailscale --config brainstack.yaml --dry-run|--apply
  brainctl backup --config brainstack.yaml [--out DIR] [--pause-telemux]
  brainctl restore --backup DIR_OR_TGZ --target DIR [--apply]
  brainctl render --config brainstack.yaml --profile ... --out DIR
  brainctl bootstrap-client --profile client-macos --config brainstack.yaml --out DIR
  brainctl join-worker --config brainstack.yaml --worker WORKER_HOST [--ssh-user USER] [--out DIR]
  brainctl trust-worker --config brainstack.yaml --worker WORKER_NAME [--host HOST] [--dry-run]
  brainctl worker-cache status|clear --config brainstack.yaml [worker|--all]
  brainctl repo-lock status|clear --config brainstack.yaml [--repo write|serve] [--path LOCK_DIR] [--yes] [--token LOCK_TOKEN] [--force] [--min-age-ms MS]
  brainctl locks status|clear --config brainstack.yaml [--repo write|serve] [--path LOCK_DIR] [--yes] [--token LOCK_TOKEN] [--force] [--min-age-ms MS]
  brainctl rotate-token --kind import|admin|telegram-placeholder --config brainstack.yaml [--env FILE]
  brainctl import-text --config brainstack.yaml --title TITLE --text TEXT --source-harness HARNESS --source-machine MACHINE [--source-type note]
  brainctl propose --config brainstack.yaml --title TITLE --body BODY
  brainctl context --repo PATH [--config brainstack.yaml] [--json] [--sync|--no-sync]
  brainctl search --repo PATH "query" [--config brainstack.yaml] [--json] [--wait-fresh]
  brainctl remember --repo PATH --summary TEXT [--target BRAIN_ID] [--confirm-cross-brain]
  brainctl allow repo --repo PATH --brain BRAIN_ID [--sections a,b] --always|--once|--deny
  brainctl outbox status|list|flush|purge|purge-corrupt --config brainstack.yaml [--yes]
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

function requireFlagValue(args: ParsedArgs, key: string): string | undefined {
  if (args.flags[key] === true) {
    throw new Error(`--${key} requires a value`);
  }
  return flag(args, key);
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
  return resolve(expanded);
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
  const template = CLIENT_BOOTSTRAP_TEMPLATES[path as ClientBootstrapTemplateName];
  if (template === undefined) {
    throw new Error(`Unknown client bootstrap template: ${path}`);
  }
  return template;
}

function profileRequiresBunRuntime(profile: Profile): boolean {
  return profile !== "client-macos";
}

function normalizeHarness(value: string | undefined, fallback: HarnessName = "codex"): HarnessName {
  const normalized = (value || fallback).trim().toLowerCase();
  if (normalized === "codex" || normalized === "claude") {
    return normalized;
  }
  throw new Error(`Unsupported harness: ${value}. Expected codex or claude.`);
}

function normalizeWorkerSshTrustMode(value: string | null | undefined, transport: string): WorkerSshTrustMode {
  if (transport === "local" || transport === "tailscale-ssh") {
    return "pinned";
  }
  if (value === "accept-new") {
    return "accept-new";
  }
  return "pinned";
}

function normalizeSecurityPosture(value: string | null | undefined): SecurityPosture {
  const normalized = (value || "trusted-tailnet").trim().toLowerCase();
  if (normalized === "local" || normalized === "trusted-tailnet" || normalized === "guarded") {
    return normalized;
  }
  throw new Error(`Unsupported security posture: ${value}. Expected local, trusted-tailnet, or guarded.`);
}

function normalizeTrustedExposure(value: string | null | undefined): TrustedExposure {
  const normalized = (value || "none").trim().toLowerCase();
  if (normalized === "none" || normalized === "tailscale-serve" || normalized === "vpn" || normalized === "manual") {
    return normalized;
  }
  throw new Error(`Unsupported trusted exposure: ${value}. Expected none, tailscale-serve, vpn, or manual.`);
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
  const configPath = abs(path);
  if (!existsSync(configPath)) {
    throw new Error(await missingConfigMessage(configPath));
  }
  const text = await readFile(configPath, "utf8");
  if (configPath.endsWith(".json")) {
    return JSON.parse(text) as Record<string, unknown>;
  }
  return parseSimpleYaml(text);
}

async function discoverConfigCandidates(configDir: string): Promise<string[]> {
  if (!existsSync(configDir)) {
    return [];
  }
  const names = await readdir(configDir);
  return names
    .filter((name) => /\.(ya?ml|json)$/.test(name))
    .filter((name) => name === "brainstack.yaml" || name.includes("brainstack"))
    .map((name) => join(configDir, name))
    .sort((a, b) => {
      const score = (path: string): number => {
        if (path.endsWith("/brainstack.yaml")) return 0;
        if (path.endsWith("/current-install.brainstack.yaml")) return 1;
        if (path.endsWith(".brainstack.yaml")) return 2;
        return 3;
      };
      return score(a) - score(b) || a.localeCompare(b);
    });
}

async function missingConfigMessage(configPath: string): Promise<string> {
  const candidates = await discoverConfigCandidates(dirname(configPath));
  return [
    `Brainstack config not found: ${configPath}`,
    "",
    "Create one with:",
    `  brainctl provision --profile control --out ${configPath} --harness codex`,
    "",
    "Or pass an existing config explicitly:",
    ...candidates.map((candidate) => `  brainctl doctor --config ${candidate}`),
    candidates.length ? "" : "  (no existing brainstack config files found beside the requested path)",
    "Common config paths:",
    "  ~/.config/brainstack/brainstack.yaml",
    "  ~/.config/brainstack/current-install.brainstack.yaml",
    "  ~/.config/brainstack/<machine>.brainstack.yaml"
  ].join("\n");
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
  const sshTrustValue = optionalStringAt(input, "sshTrustMode") || optionalStringAt(input, "sshTrust");
  return {
    name,
    transport,
    sshTarget: optionalStringAt(input, "sshTarget"),
    sshUser: optionalStringAt(input, "sshUser"),
    sshTrustMode: normalizeWorkerSshTrustMode(sshTrustValue, transport),
    sshKnownHostsPath: optionalStringAt(input, "sshKnownHostsPath"),
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
  if (profile === "private-journal") {
    throw new Error("Unsupported profile private-journal: first-class private journaling is not implemented yet. Use a separate explicit Brainstack install/config with separate repo paths and tokens.");
  }
  if (!SUPPORTED_PROFILES.includes(profile)) {
    throw new Error(`Unsupported profile ${profile}; supported profiles are ${SUPPORTED_PROFILES.join("|")}`);
  }
  const runtime = objectAt(raw, "runtime");
  const harness = objectAt(raw, "harness");
  const machine = objectAt(raw, "machine");
  const paths = objectAt(raw, "paths");
  const security = objectAt(raw, "security");
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
  const securityPosture = normalizeSecurityPosture(optionalStringAt(security, "posture"));
  const securityBindHost = stringAt(security, "bindHost", stringAt(brain, "bind", "127.0.0.1"));
  const trustedExposure = normalizeTrustedExposure(optionalStringAt(security, "trustedExposure"));
  const workerInputs = arrayAt(telemux, "workers") as Array<Record<string, unknown>>;
  const remoteSsh = `${machineUser}@${machineName}:${join(sharedBrainRoot, "bare", "shared-brain.git")}`;
  const harnessName = normalizeHarness(stringAt(harness, "name", stringAt(telemux, "harness", "codex")));
  const harnessBin = stringAt(harness, "bin", harnessName);
  const configuredBunBin = stringAt(runtime, "bunBin", "");

  const cfg: BrainstackConfig = {
    schemaVersion,
    runtime: {
      bunBin: configuredBunBin || (profileRequiresBunRuntime(profile) ? resolveBunBin() : "bun")
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
    security: {
      posture: securityPosture,
      bindHost: securityBindHost,
      trustedExposure
    },
    brain: {
      bind: securityBindHost,
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
      controlRoot: root ? join(stateRoot, "telemux") : absWithHome(stringAt(telemux, "controlRoot", join(stateRoot, "telemux")), home),
      factoryRoot: root ? join(stateRoot, "factory") : absWithHome(stringAt(telemux, "factoryRoot", join(stateRoot, "factory")), home),
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

function run(args: string[], options: { cwd?: string; env?: Record<string, string>; check?: boolean; timeoutMs?: number } = {}) {
  let proc: ReturnType<typeof Bun.spawnSync>;
  try {
    proc = Bun.spawnSync(args, {
      cwd: options.cwd || process.cwd(),
      env: { ...process.env, ...(options.env || {}) },
      stdout: "pipe",
      stderr: "pipe",
      timeout: options.timeoutMs
    });
  } catch (error) {
    const stderr = error instanceof Error ? error.message : String(error);
    if (options.check !== false) {
      throw new Error(`${args.join(" ")} failed\n${stderr}`);
    }
    return { code: 127, stdout: "", stderr, timedOut: false };
  }
  const stdout = proc.stdout.toString();
  const timedOut = proc.exitCode === null;
  const stderr = proc.stderr.toString() || (timedOut && options.timeoutMs ? `timed out after ${options.timeoutMs}ms` : "");
  const code = timedOut ? 124 : proc.exitCode;
  if (options.check !== false && code !== 0) {
    throw new Error(`${args.join(" ")} failed\n${stderr || stdout}`);
  }
  return { code, stdout, stderr, timedOut };
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

async function readEnvSecretOrFile(envName: string, fileEnvName: string, filePathOverride?: string): Promise<string> {
  const filePath = filePathOverride?.trim() || process.env[fileEnvName]?.trim();
  if (filePath) {
    const absolute = abs(filePath);
    const info = await lstat(absolute);
    if (info.isSymbolicLink() || !info.isFile()) {
      throw new Error(`${fileEnvName} must point to a regular non-symlink file: ${absolute}`);
    }
    if ((info.mode & 0o077) !== 0) {
      throw new Error(`${fileEnvName} must not be group/world accessible; run chmod 600 ${shellSingleQuote(absolute)}`);
    }
    return (await readFile(absolute, "utf8")).split(/\r?\n/)[0]?.trim() || "";
  }
  return process.env[envName]?.trim() || "";
}

async function setEnvIfBlank(path: string, key: string, value: string): Promise<boolean> {
  if (!value.trim()) {
    return false;
  }
  const existing = existsSync(path) ? await readFile(path, "utf8") : "";
  const lines = existing.split(/\r?\n/);
  let changed = false;
  let found = false;
  const next = lines.map((line) => {
    if (!line.startsWith(`${key}=`)) {
      return line;
    }
    found = true;
    if (line.slice(key.length + 1).trim()) {
      return line;
    }
    changed = true;
    return `${key}=${value}`;
  });
  if (!found) {
    if (next.length && next[next.length - 1] !== "") {
      next.push(`${key}=${value}`);
    } else {
      next.splice(Math.max(0, next.length - 1), 0, `${key}=${value}`);
    }
    changed = true;
  }
  if (changed) {
    await writeText(path, `${next.join("\n").replace(/\n+$/, "")}\n`, 0o600);
  }
  return changed;
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

function runtimeCommandAvailable(pathOrName: string): boolean {
  if (!pathOrName.trim()) {
    return false;
  }
  return pathOrName.includes("/") ? existsSync(pathOrName) : commandPath(pathOrName) !== null;
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
      "- `GET /healthz`",
      "- `GET /admin/health` requires admin bearer token.",
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
    `BRAIN_BIND=${cfg.security.bindHost}`,
    `BRAIN_PORT=${cfg.brain.port}`,
    `BRAIN_SECURITY_POSTURE=${cfg.security.posture}`,
    `BRAIN_TRUSTED_EXPOSURE=${cfg.security.trustedExposure}`,
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
  const toolPath = brainstackToolPath(cfg);
  return [
    `PATH=${toolPath}`,
    `BRAINSTACK_WORKER_PATH=${toolPath}`,
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
    `FACTORY_SSH_KNOWN_HOSTS=${join(cfg.paths.configRoot, "ssh_known_hosts")}`,
    "FACTORY_USAGE_ADAPTER=manual",
    `FACTORY_HARNESS=${cfg.harness.name}`,
    `FACTORY_HARNESS_BIN=${cfg.harness.bin}`,
    `FACTORY_CODEX_BIN=${cfg.harness.name === "codex" ? cfg.harness.bin : "codex"}`,
    `BRAIN_BASE_URL=${cfg.brain.publicBaseUrl}`,
    ""
  ].join("\n");
}

function brainstackToolPath(cfg: BrainstackConfig): string {
  return [
    join(cfg.paths.home, ".local", "bin"),
    join(cfg.paths.home, ".local", "share", "mise", "shims"),
    join(cfg.paths.home, ".local", "share", "omarchy", "bin"),
    join(cfg.paths.home, ".bun", "bin"),
    "/usr/local/sbin",
    "/usr/local/bin",
    "/usr/bin",
    "/bin"
  ].join(":");
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

async function preserveSshKnownHosts(cfg: BrainstackConfig): Promise<void> {
  const target = join(cfg.paths.configRoot, "ssh_known_hosts");
  if (existsSync(target)) {
    return;
  }
  const legacy = join(cfg.telemux.controlRoot, "ssh_known_hosts");
  if (!existsSync(legacy)) {
    return;
  }
  const legacyText = await readFile(legacy, "utf8").catch(() => "");
  if (!legacyText.split(/\r?\n/).some(isKnownHostLine)) {
    return;
  }
  await ensureDir(dirname(target));
  await cp(legacy, target, { force: false });
}

function isKnownHostLine(line: string): boolean {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith("#") || trimmed.startsWith("@revoked ")) {
    return false;
  }
  const parts = trimmed.split(/\s+/);
  if (parts[0] === "@cert-authority") {
    return parts.length >= 4 && /^(?:ssh|ecdsa|sk)-/.test(parts[2]);
  }
  return parts.length >= 3 && /^(?:ssh|ecdsa|sk)-/.test(parts[1]);
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
  return cfg.profile === "single-node" || cfg.profile === "control";
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
    "UMask=0077",
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
    "UMask=0077",
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
  const host = cfg.tailscale.tailnetHost;
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
        src: ["group:brain-admins", "autogroup:admin"],
        dst: [cfg.tailscale.workerTag],
        ip: ["tcp:22", "icmp:*"]
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

async function commandExpose(args: ParsedArgs): Promise<void> {
  const sub = args.positional[0] || "";
  if (sub !== "tailscale") {
    throw new Error("expose supports only: tailscale");
  }
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const configPath = join(cfg.paths.configRoot, "tailscale-serve.json");
  const tailnetHost = cfg.tailscale.tailnetHost.trim();
  if (!tailnetHost || tailnetHost === "brain-control.example.ts.net" || tailnetHost.endsWith(".example.ts.net")) {
    throw new Error("expose tailscale requires a real tailscale.tailnetHost; set --tailnet-host during provision or edit brainstack.yaml.");
  }
  if (hasFlag(args, "apply") && cfg.security.trustedExposure !== "tailscale-serve") {
    throw new Error("expose tailscale --apply requires security.trustedExposure: tailscale-serve so runtime health metadata matches exposure.");
  }
  if (hasFlag(args, "apply") && cfg.security.posture === "trusted-tailnet" && !isLoopbackHost(cfg.security.bindHost)) {
    throw new Error(`expose tailscale --apply requires trusted-tailnet braind to bind loopback; got security.bindHost=${cfg.security.bindHost}. Set security.bindHost: 127.0.0.1 before applying Serve.`);
  }
  const rendered = tailscaleServeConfig(cfg);
  const command = `tailscale serve set-config --all ${shellSingleQuote(configPath)}`;
  if (hasFlag(args, "dry-run") || !hasFlag(args, "apply")) {
    console.log("# Tailscale Serve config");
    console.log(rendered.trimEnd());
    console.log("# Apply command");
    console.log(`mkdir -p ${shellSingleQuote(dirname(configPath))}`);
    console.log(`cat > ${shellSingleQuote(configPath)} <<'JSON'`);
    console.log(rendered.trimEnd());
    console.log("JSON");
    console.log(command);
    return;
  }
  const tailscaleBin = commandPath("tailscale");
  if (!tailscaleBin) {
    throw new Error(`tailscale binary missing; install Tailscale and retry, or run manually later:\n${command}`);
  }
  await writeText(configPath, rendered, 0o644);
  run([tailscaleBin, "serve", "set-config", "--all", configPath]);
  console.log(`applied Tailscale Serve config from ${configPath}`);
}

function clientBootstrapFiles(cfg: BrainstackConfig): Record<string, string> {
  const clientPath = cfg.client.localPath;
  const replacements = {
    BRAIN_BASE_URL: cfg.brain.publicBaseUrl || "https://brain-control.example.ts.net",
    BRAIN_GIT_REMOTE: cfg.client.remoteSsh,
    MACHINE_USER: cfg.machine.user,
    SHARED_BRAIN_LOCAL_PATH: clientPath
  };
  return Object.fromEntries(
    CLIENT_BOOTSTRAP_TEMPLATE_NAMES.map((name) => [name, renderTemplate(readClientBootstrapTemplate(name), replacements)])
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
      security: cfg.security,
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
      `Security posture: \`${cfg.security.posture}\` (${cfg.security.trustedExposure}, bind ${cfg.security.bindHost})`,
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
      { path: join(cfg.paths.configRoot, "ssh_known_hosts"), kind: "file", scope: "control", reason: "brainstack worker OpenSSH pinned host keys", optional: true },
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

function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  }
  const record = value as Record<string, unknown>;
  return `{${Object.keys(record)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
    .join(",")}}`;
}

async function installLocalClientBootstrap(cfg: BrainstackConfig, options: { importTokenFile?: string } = {}): Promise<string[]> {
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
  const importToken = await readEnvSecretOrFile("BRAIN_IMPORT_TOKEN", "BRAIN_IMPORT_TOKEN_FILE", options.importTokenFile);
  if (await setEnvIfBlank(envPath, "BRAIN_IMPORT_TOKEN", importToken)) {
    if (!touched.includes(envPath)) {
      touched.push(envPath);
    }
    console.log(`BRAIN_IMPORT_TOKEN installed in ${envPath}; value not printed.`);
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
  const proc = run(["bash", "-c", `command -v ${shellSingleQuote(name)}`], { check: false, env: userShellPathEnv() });
  return proc.code === 0 && proc.stdout.trim() ? proc.stdout.trim().split(/\r?\n/)[0] : null;
}

let cachedUserShellPath: string | null | undefined;

function userShellPathTimeoutMs(): number {
  const raw = Number(process.env.BRAINSTACK_SHELL_PATH_TIMEOUT_MS || "");
  if (Number.isFinite(raw) && raw > 0) {
    return Math.min(raw, 30_000);
  }
  return 5_000;
}

function detectUserShellPath(): string | null {
  if (cachedUserShellPath !== undefined) return cachedUserShellPath;
  if (process.env.BRAINSTACK_SKIP_USER_PATH_RESOLVE) {
    cachedUserShellPath = null;
    return cachedUserShellPath;
  }
  if (process.env.BRAINSTACK_WORKER_PATH?.trim()) {
    cachedUserShellPath = process.env.BRAINSTACK_WORKER_PATH.trim();
    return cachedUserShellPath;
  }
  const shell = process.env.SHELL;
  if (!shell || !existsSync(shell)) {
    cachedUserShellPath = null;
    return cachedUserShellPath;
  }
  const proc = run([shell, "-lic", 'printf "__BRAINSTACK_PATH__%s\\n" "$PATH"'], {
    check: false,
    timeoutMs: userShellPathTimeoutMs()
  });
  const marker = proc.stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.includes("__BRAINSTACK_PATH__"))
    .at(-1);
  cachedUserShellPath = proc.code === 0 && marker ? marker.replace(/^.*__BRAINSTACK_PATH__/, "") || null : null;
  return cachedUserShellPath;
}

function userShellPathEnv(): Record<string, string> | undefined {
  const path = detectUserShellPath();
  return path ? { PATH: `${process.env.PATH ? `${process.env.PATH}:` : ""}${path}` } : undefined;
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
  const commands = profileRequiresBunRuntime(profile) ? ["bun", "git", "ssh", "tailscale"] : ["git", "ssh", "tailscale"];
  if (profile === "single-node" || profile === "control" || profile === "worker") {
    commands.push("sshd");
  }
  return commands;
}

function profileRequiresPasswordlessSudo(profile: Profile): boolean {
  return profile === "single-node" || profile === "control";
}

function provisionRequiresPasswordlessSudo(profile: Profile, args: ParsedArgs): boolean {
  return profileRequiresPasswordlessSudo(profile) || hasFlag(args, "enroll-tailscale") || hasFlag(args, "require-harness-sudo");
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
  const harnessBinOverride = flag(args, "harness-bin");
  const codexPath = whereisPath("codex");
  const claudePath = whereisPath("claude");
  const discovered = { codex: codexPath, claude: claudePath };
  if (requested) {
    const name = normalizeHarness(requested);
    const bin = harnessBinOverride || (name === "codex" ? codexPath : claudePath);
    if (!bin) {
      throw new Error(`provision blocked: requested harness ${name} is missing.\n${installHint(name)}`);
    }
    return { name, bin, discovered };
  }
  if (codexPath && !claudePath) {
    return { name: "codex", bin: harnessBinOverride || codexPath, discovered };
  }
  if (claudePath && !codexPath) {
    return { name: "claude", bin: harnessBinOverride || claudePath, discovered };
  }
  if (codexPath && claudePath) {
    if (process.stdin.isTTY && !hasFlag(args, "yes")) {
      const name = await promptHarnessChoice(codexPath, claudePath);
      return { name, bin: harnessBinOverride || (name === "codex" ? codexPath : claudePath), discovered };
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
    response = await fetch(`https://api.telegram.org/bot${tokenValue.trim()}/getMe`, {
      signal: AbortSignal.timeout(15_000)
    });
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
  const rootOverride = flag(args, "root") ? abs(flag(args, "root")!) : null;
  const user = process.env.USER || "operator";
  const home = rootOverride ? join(rootOverride, "home") : process.env.HOME || `/home/${user}`;
  const machineName = flag(args, "machine") || discoveredMachineName();
  const role = flag(args, "role") || profile;
  const publicBaseUrl = flag(args, "brain-base-url") || flag(args, "public-base-url") || "";
  const tailnetHost = flag(args, "tailnet-host") || publicBaseUrl.replace(/^https?:\/\//, "");
  const telemuxEnabled = hasFlag(args, "enable-telemux");
  const bindHost = flag(args, "brain-bind") || flag(args, "bind-host") || "127.0.0.1";
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
      bunBin: flag(args, "bun-bin") || "bun"
    },
    harness: {
      name: harness.name,
      bin: flag(args, "harness-bin") || harness.name
    },
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
      sharedBrainRoot: rootOverride ? join(rootOverride, "shared-brain") : "~/shared-brain",
      privateBrainRoot: rootOverride ? join(rootOverride, "private-brain") : "~/private-brain",
      stateRoot: rootOverride ? join(rootOverride, "state") : "~/.local/state/brainstack",
      configRoot: rootOverride ? join(rootOverride, "config") : "~/.config/brainstack",
      systemdUserRoot: rootOverride ? join(rootOverride, "systemd-user") : "~/.config/systemd/user"
    },
    security: {
      posture: flag(args, "security-posture") || "trusted-tailnet",
      bindHost,
      trustedExposure: flag(args, "trusted-exposure") || "none"
    },
    brain: {
      bind: bindHost,
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
  if (flag(args, "config") && !flag(args, "out")) {
    throw new Error("provision writes a new config with --out; --config is used by commands that read an existing config");
  }
  const found = ensureProvisionPrereqs(profile);
  const selectedHarness = await selectProvisionHarness(args);
  const enrollTailscale = hasFlag(args, "enroll-tailscale");
  const authKeyEnv = flag(args, "tailscale-auth-key-env") || "TAILSCALE_AUTH_KEY";
  if (enrollTailscale && !process.env[authKeyEnv]) {
    throw new Error(`provision blocked: --enroll-tailscale requires ${authKeyEnv} in env`);
  }
  if (provisionRequiresPasswordlessSudo(profile, args)) {
    ensurePasswordlessSudo();
  }
  if ((profileRequiresPasswordlessSudo(profile) || hasFlag(args, "require-harness-sudo")) && !hasFlag(args, "skip-harness-sudo-test")) {
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
  if (enrollTailscale) {
    const tailscale = objectAt(config, "tailscale");
    const machine = objectAt(config, "machine");
    const paths = objectAt(config, "paths");
    run(["bash", "-lc", tailscaleUpShellCommand(String(machine.name), String(machine.user), arrayAt(tailscale, "advertiseTags").map(String), authKeyEnv)]);
    console.log(`tailscale enrolled for ${String(machine.name)}; config still written to ${out}`);
    console.log(`home path: ${String(paths.home)}`);
  }
  console.log(`provision config written: ${out}`);
  console.log(`ownership manifest written: ${managedManifestPath(cfg)}`);
  console.log(`detected tools: ${Object.entries(found).map(([name]) => `${name}=present`).join(" ")}`);
  console.log(`selected harness: ${selectedHarness.name} (config bin: ${flag(args, "harness-bin") || selectedHarness.name})`);
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
  await writeFileMap(join(cfg.paths.configRoot, "client-bootstrap"), clientBootstrapFiles(cfg));
  if (runsBraind(cfg)) {
    const seedMode: SeedMode = hasFlag(args, "force-seed") ? "force" : hasFlag(args, "seed-missing") ? "missing" : "empty-only";
    await ensureGitRepoLayout(cfg, "fresh", seedMode);
    await writeText(join(cfg.paths.configRoot, "braind.runtime.env"), braindRuntimeEnv(cfg), 0o644);
    await writeIfMissing(join(cfg.paths.configRoot, "braind.secrets.env"), braindSecretsEnv(true), 0o600);
    await writeText(join(cfg.paths.systemdUserRoot, "braind.service"), braindService(cfg));
  }
  if (cfg.telemux.enabled) {
    await preserveSshKnownHosts(cfg);
    await writeText(join(cfg.paths.configRoot, "telemux.runtime.env"), telemuxRuntimeEnv(cfg), 0o644);
    await writeIfMissing(join(cfg.paths.configRoot, "telemux.secrets.env"), telemuxSecretsEnv(), 0o600);
    await writeText(join(cfg.paths.configRoot, "workers.json"), `${JSON.stringify(defaultWorkers(cfg), null, 2)}\n`);
    await writeText(join(cfg.paths.systemdUserRoot, "telemux.service"), telemuxService(cfg));
  }
  if (cfg.profile === "worker" || cfg.profile === "client-macos") {
    const touched = await installLocalClientBootstrap(cfg, { importTokenFile: requireFlagValue(args, "import-token-file") });
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
  await writeFileMap(join(cfg.paths.configRoot, "client-bootstrap"), clientBootstrapFiles(cfg));
  touched.push(join(cfg.paths.configRoot, "client-bootstrap"));
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
    await preserveSshKnownHosts(cfg);
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
  return commandPath(name) !== null;
}

interface CommandProbe {
  text: string;
  code: number;
  timedOut: boolean;
}

function firstProbeLine(stdout: string, stderr: string): string {
  return (stdout || stderr).trim().split(/\r?\n/)[0] || "unknown";
}

function commandVersionProbe(name: string): CommandProbe {
  const executable = commandPath(name) || name;
  const proc =
    name === "ssh"
      ? run([executable, "-V"], { check: false, env: userShellPathEnv(), timeoutMs: updateProbeTimeoutMs() })
      : run([executable, "--version"], { check: false, env: userShellPathEnv(), timeoutMs: updateProbeTimeoutMs() });
  return {
    text: proc.timedOut ? proc.stderr || `timed out after ${updateProbeTimeoutMs()}ms` : firstProbeLine(proc.stdout, proc.stderr),
    code: proc.code,
    timedOut: proc.timedOut
  };
}

function commandVersion(name: string): string {
  return commandVersionProbe(name).text;
}

function commandHelpProbe(name: string, args: string[] = ["--help"]): CommandProbe {
  const proc = run([commandPath(name) || name, ...args], { check: false, env: userShellPathEnv(), timeoutMs: updateProbeTimeoutMs() });
  return {
    text: `${proc.stdout}\n${proc.stderr}`,
    code: proc.code,
    timedOut: proc.timedOut
  };
}

function harnessCompatibility(name: HarnessName, bin: string, options: { required?: boolean } = {}): DoctorCheck {
  const requiredHarness = options.required ?? true;
  const failureStatus: CheckStatus = requiredHarness ? "FAIL" : "WARN";
  if (!commandOk(bin)) {
    return check(failureStatus, "versions", `${name}-harness`, `${bin} not found in PATH`, requiredHarness ? installHint(name) : undefined);
  }
  const versionProbe = commandVersionProbe(bin);
  const helpProbe = name === "codex" ? commandHelpProbe(bin, ["exec", "--help"]) : commandHelpProbe(bin, ["--help"]);
  const version = versionProbe.text;
  if (versionProbe.timedOut || helpProbe.timedOut) {
    return check(failureStatus, "versions", `${name}-harness`, `${version}; CLI compatibility probe timed out`, `Update ${name} manually, then rerun doctor. ${installHint(name)}`);
  }
  if (helpProbe.code !== 0) {
    return check(failureStatus, "versions", `${name}-harness`, `${version}; CLI help probe exited ${helpProbe.code}`, `Update ${name} manually, then rerun doctor. ${installHint(name)}`);
  }
  const help = helpProbe.text;
  const required =
    name === "codex"
      ? ["--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check", "--output-last-message"]
      : ["--dangerously-skip-permissions", "--permission-mode", "--output-format"];
  const missing = required.filter((needle) => !help.includes(needle));
  if (missing.length) {
    return check(
      failureStatus,
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

async function lockDoctorCheck(label: string, lockPath: string): Promise<DoctorCheck> {
  const info = await repoLockInfo(lockPath);
  if (!info.exists) {
    return check("PASS", "locks", label, `absent: ${lockPath}`);
  }
  const safe = info.safeToClear ? "safe-to-clear=yes" : "safe-to-clear=no";
  return check(
    "WARN",
    "locks",
    label,
    `present age_ms=${info.ageMs ?? "unknown"} pid_alive=${info.pidAlive} ${safe}; ${info.reason}`,
    `Inspect with brainctl repo-lock status --config <config>; clear only after confirming no write is active.`
  );
}

function pendingReindexPathFor(cfg: BrainstackConfig): string {
  return join(cfg.repos.serve, "derived", "search-reindex-needed.json");
}

function outboxParentRoot(cfg: BrainstackConfig): string {
  return join(cfg.paths.stateRoot, "outbox");
}

function outboxRoot(cfg: BrainstackConfig): string {
  return join(outboxParentRoot(cfg), brainInstanceId(cfg));
}

function outboxRootForDestination(cfg: BrainstackConfig, destination: { brainId?: string; baseUrl?: string }): string {
  const base = destination.brainId || destination.baseUrl || cfg.brain.publicBaseUrl || cfg.client.remoteSsh || cfg.client.localPath;
  return join(outboxParentRoot(cfg), sha256Hex(base).slice(0, 32));
}

function brainInstanceId(cfg: BrainstackConfig): string {
  const base = cfg.brain.publicBaseUrl || cfg.client.remoteSsh || cfg.client.localPath;
  return sha256Hex(base).slice(0, 32);
}

async function countOutboxItems(cfg: BrainstackConfig): Promise<number> {
  return (await scanOutbox(outboxRoot(cfg))).items.length;
}

async function harnessGuidanceChecks(cfg: BrainstackConfig): Promise<DoctorCheck[]> {
  const checks: DoctorCheck[] = [];
  const bootstrapRoot = join(cfg.paths.configRoot, "client-bootstrap");
  const codexAgents = join(cfg.paths.home, ".codex", "AGENTS.md");
  const codexInclude = join(bootstrapRoot, "codex-shared-brain.include.md");
  const claudeFile = join(cfg.paths.home, ".claude", "CLAUDE.md");
  const claudeInclude = `@${join(bootstrapRoot, "claude-user-CLAUDE.md")}`;
  const cursorRule = join(cfg.paths.home, ".cursor", "rules", "shared-brain.md");
  const guidanceNeedle = "brainctl context --repo .";
  const codexRelevant = cfg.harness.name === "codex" || existsSync(codexAgents) || existsSync(codexInclude);
  const claudeRelevant = cfg.harness.name === "claude" || existsSync(claudeFile);

  if (codexRelevant) {
    if (!existsSync(codexInclude)) {
      checks.push(
        check(
          "WARN",
          "guidance",
          "codex-bootstrap-include",
          `${codexInclude} missing`,
          `Run brainctl init or brainctl bootstrap-client --out ${bootstrapRoot}.`
        )
      );
    }
    if (existsSync(codexAgents)) {
      const info = await lstat(codexAgents).catch(() => null);
      const target = info?.isSymbolicLink() ? await readlink(codexAgents).catch(() => "") : "";
      const text = info?.isSymbolicLink() ? "" : readFileSync(codexAgents, "utf8");
      checks.push(
        target === codexInclude || text.includes(guidanceNeedle)
          ? check("PASS", "guidance", "codex-agents", target === codexInclude ? `symlinked to ${codexInclude}` : "contains Brainstack context guidance")
          : check("WARN", "guidance", "codex-agents", `${codexAgents} exists but Brainstack guidance was not detected`, `Append: cat ${codexInclude} >> ${codexAgents}`)
      );
    } else {
      checks.push(check("WARN", "guidance", "codex-agents", `${codexAgents} missing`, "Run brainctl init or merge the generated client bootstrap guidance."));
    }
  }

  if (claudeRelevant) {
    if (existsSync(claudeFile)) {
      const text = readFileSync(claudeFile, "utf8");
      checks.push(
        text.includes(claudeInclude) || text.includes(guidanceNeedle)
          ? check("PASS", "guidance", "claude", "contains Brainstack context guidance")
          : check("WARN", "guidance", "claude", `${claudeFile} exists but Brainstack guidance was not detected`, `Add this line: ${claudeInclude}`)
      );
    } else {
      checks.push(check("WARN", "guidance", "claude", `${claudeFile} missing`, "Run brainctl init or merge the generated client bootstrap guidance."));
    }
  }

  if (existsSync(cursorRule)) {
    const text = readFileSync(cursorRule, "utf8");
    checks.push(
      text.includes(guidanceNeedle)
        ? check("PASS", "guidance", "cursor", "contains Brainstack context guidance")
        : check("WARN", "guidance", "cursor", `${cursorRule} exists but Brainstack guidance was not detected`, `Append: cat ${join(bootstrapRoot, "cursor-user-rule.md")} >> ${cursorRule}`)
    );
  }
  return checks;
}

async function collectDoctorChecks(cfg: BrainstackConfig, args: ParsedArgs): Promise<DoctorCheck[]> {
  const checks: DoctorCheck[] = [];
  const bunRequired = profileRequiresBunRuntime(cfg.profile);
  const bunVersion = installedBunVersion();
  if (bunRequired) {
    checks.push(
      bunVersion && compareVersions(bunVersion, MIN_BUN_VERSION) >= 0
        ? check("PASS", "versions", "bun", `Bun ${bunVersion}; required >= ${MIN_BUN_VERSION}`)
        : check("FAIL", "versions", "bun", bunVersion ? `Bun ${bunVersion}; required >= ${MIN_BUN_VERSION}` : "Bun runtime missing", installHint("bun"))
    );
  } else {
    checks.push(
      bunVersion && compareVersions(bunVersion, MIN_BUN_VERSION) >= 0
        ? check("PASS", "versions", "bun", `Bun ${bunVersion}; optional for client-macos source workflows`)
        : check("WARN", "versions", "bun", bunVersion ? `Bun ${bunVersion}; optional for client-macos source workflows` : "Bun runtime missing; not required for client-macos standalone binary installs")
    );
  }
  const bunBinAvailable = runtimeCommandAvailable(cfg.runtime.bunBin);
  checks.push(
    bunBinAvailable
      ? check("PASS", "versions", "bun-bin", cfg.runtime.bunBin)
      : bunRequired
        ? check("FAIL", "versions", "bun-bin", `${cfg.runtime.bunBin} does not exist`)
        : check("WARN", "versions", "bun-bin", `${cfg.runtime.bunBin} unavailable; not used by client-macos binary installs`)
  );
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
  checks.push(...await harnessGuidanceChecks(cfg));
  const loopbackBind = cfg.security.bindHost === "127.0.0.1" || cfg.security.bindHost === "::1" || cfg.security.bindHost === "localhost";
  const wildcardBind = cfg.security.bindHost === "0.0.0.0" || cfg.security.bindHost === "::";
  if (cfg.security.posture === "local") {
    checks.push(
      loopbackBind
        ? check("PASS", "security", "posture", `local bind=${cfg.security.bindHost}`)
        : check("FAIL", "security", "posture", `local posture must bind loopback, got ${cfg.security.bindHost}`, "Set security.bindHost to 127.0.0.1 or change the posture deliberately.")
    );
  } else if (cfg.security.posture === "trusted-tailnet") {
    checks.push(check("PASS", "security", "read-auth", "disabled by design in trusted-tailnet mode"));
    checks.push(check("PASS", "security", "trust-boundary", "private network reachability"));
    checks.push(check("WARN", "security", "public-exposure", "do not expose trusted-tailnet mode to the public internet", "Bind loopback and expose through Tailscale Serve/VPN, or use guarded/manual controls for broader exposure."));
    checks.push(
      wildcardBind
        ? check("FAIL", "security", "posture", `trusted-tailnet bind=${cfg.security.bindHost}; exposure=${cfg.security.trustedExposure}`, "Wildcard trusted-tailnet binds are too broad for this posture. Set security.bindHost to 127.0.0.1 and expose through Tailscale Serve/VPN, or implement guarded app-layer read controls.")
        : !loopbackBind && (cfg.security.trustedExposure === "manual" || cfg.security.trustedExposure === "vpn")
        ? check("WARN", "security", "posture", `trusted-tailnet bind=${cfg.security.bindHost}; exposure=${cfg.security.trustedExposure}`, "Manual/VPN exposure is explicit; verify the private-network boundary yourself.")
        : !loopbackBind
        ? check("FAIL", "security", "posture", `trusted-tailnet bind=${cfg.security.bindHost}; exposure=${cfg.security.trustedExposure}`, "Set security.bindHost to 127.0.0.1 and expose through Tailscale Serve/VPN, or explicitly choose guarded/manual exposure.")
        : check("PASS", "security", "posture", `trusted-tailnet bind=${cfg.security.bindHost}; exposure=${cfg.security.trustedExposure}`)
    );
    checks.push(
      cfg.security.trustedExposure === "tailscale-serve"
        ? check("PASS", "security", "tailscale-serve-exposure", "configured in brainstack.yaml")
        : check("PASS", "security", "tailscale-serve-exposure", "no Tailscale Serve exposure declared; local-only until exposed")
    );
  } else {
    checks.push(check("WARN", "security", "posture", "guarded posture is reserved; no first-class read-token/IAM enforcement is implemented yet", "Use trusted-tailnet for the current product stance, or add guarded controls before broader exposure."));
  }

  if (runsBraind(cfg)) {
    checks.push(existsSync(join(cfg.paths.configRoot, "braind.runtime.env")) ? check("PASS", "secrets", "braind-runtime-env", "present") : check("WARN", "secrets", "braind-runtime-env", "missing"));
    checks.push(envHasKey(join(cfg.paths.configRoot, "braind.secrets.env"), "BRAIN_IMPORT_TOKEN") ? check("PASS", "secrets", "brain-import-token", "present") : check("WARN", "secrets", "brain-import-token", "missing or empty"));
    checks.push(envHasKey(join(cfg.paths.configRoot, "braind.secrets.env"), "BRAIN_ADMIN_TOKEN") ? check("PASS", "secrets", "brain-admin-token", "present") : check("WARN", "secrets", "brain-admin-token", "missing or empty"));
    checks.push(existsSync(cfg.repos.bare) ? check("PASS", "git", "bare-repo", cfg.repos.bare) : check("WARN", "git", "bare-repo", `${cfg.repos.bare} missing`));
    if (gitExists(cfg.repos.staging)) checks.push(gitClean(cfg.repos.staging)); else checks.push(check("WARN", "git", "staging-clone", `${cfg.repos.staging} missing`));
    if (gitExists(cfg.repos.serve)) checks.push(gitClean(cfg.repos.serve)); else checks.push(check("WARN", "git", "serve-clone", `${cfg.repos.serve} missing`));
    checks.push(await lockDoctorCheck("write-repo-lock", join(cfg.repos.staging, ".shared-brain.lock")));
    checks.push(await lockDoctorCheck("serve-repo-lock", join(cfg.repos.serve, ".shared-brain.lock")));
    checks.push(
      existsSync(pendingReindexPathFor(cfg))
        ? check("WARN", "locks", "pending-reindex", pendingReindexPathFor(cfg), "braind should retry this automatically; inspect braind logs if it persists.")
        : check("PASS", "locks", "pending-reindex", "absent")
    );
    try {
      const response = await fetch(`http://${cfg.security.bindHost}:${cfg.brain.port}/healthz`, { signal: AbortSignal.timeout(2000) });
      checks.push(response.ok ? check("PASS", "health", "braind-healthz", `HTTP ${response.status}`) : check("WARN", "health", "braind-healthz", `HTTP ${response.status}`));
    } catch (error) {
      checks.push(check("WARN", "health", "braind-healthz", error instanceof Error ? error.message : String(error), "Start braind or ignore before runtime is installed."));
    }
    try {
      const response = await fetch(`http://${cfg.security.bindHost}:${cfg.brain.port}/readyz`, { signal: AbortSignal.timeout(2000) });
      let detail = `HTTP ${response.status}`;
      try {
        const body = (await response.json()) as Record<string, unknown>;
        if (typeof body.search_ready === "boolean") {
          detail += ` search_ready=${body.search_ready}`;
        }
        const pendingReindex = body.pending_reindex && typeof body.pending_reindex === "object" ? (body.pending_reindex as Record<string, unknown>) : undefined;
        if (pendingReindex && typeof pendingReindex.present === "boolean") {
          detail += ` pending_reindex=${pendingReindex.present}`;
        }
      } catch {
        // Non-JSON readiness responses still carry useful HTTP status.
      }
      checks.push(
        response.ok
          ? check("PASS", "health", "braind-readyz", detail)
          : check("WARN", "health", "braind-readyz", detail, "Search or write-refresh may still be rebuilding; inspect /readyz and braind logs if this persists.")
      );
    } catch (error) {
      checks.push(check("WARN", "health", "braind-readyz", error instanceof Error ? error.message : String(error), "Start braind or ignore before runtime is installed."));
    }
    const adminToken = process.env.BRAIN_ADMIN_TOKEN || readEnvFile(join(cfg.paths.configRoot, "braind.secrets.env")).BRAIN_ADMIN_TOKEN || "";
    if (adminToken) {
      try {
        const response = await fetch(`http://${cfg.security.bindHost}:${cfg.brain.port}/admin/health`, {
          headers: { Authorization: `Bearer ${adminToken}` },
          signal: AbortSignal.timeout(2000)
        });
        checks.push(response.ok ? check("PASS", "health", "braind-admin-health", `HTTP ${response.status}`) : check("WARN", "health", "braind-admin-health", `HTTP ${response.status}`));
      } catch (error) {
        checks.push(check("WARN", "health", "braind-admin-health", error instanceof Error ? error.message : String(error)));
      }
    } else {
      checks.push(check("WARN", "health", "braind-admin-health", "admin token not available locally; skipped deep health"));
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
    try {
      const response = await fetch(`http://${cfg.telemux.dashboardHost}:${cfg.telemux.dashboardPort}/healthz`, { signal: AbortSignal.timeout(1500) });
      if (response.ok) {
        const body = (await response.json()) as Record<string, unknown>;
        const crons = Number(body.crons ?? 0);
        const enabled = Number(body.cronEnabled ?? 0);
        const pending = Number(body.cronPending ?? 0);
        const due = Number(body.cronDue ?? 0);
        const queuedTurns = body.queuedTurns && typeof body.queuedTurns === "object" ? (body.queuedTurns as Record<string, unknown>) : {};
        const runningTurns = Number(queuedTurns.running ?? 0);
        const abandonedTurns = Number(queuedTurns.abandoned ?? 0);
        const pendingText = body.pendingText && typeof body.pendingText === "object" ? (body.pendingText as Record<string, unknown>) : {};
        const pendingTextCount = Number(pendingText.count ?? 0);
        const pendingTextOldestAgeMs = pendingText.oldestAgeMs === null || pendingText.oldestAgeMs === undefined ? 0 : Number(pendingText.oldestAgeMs);
        const workerDegraded = Number(body.workerDegraded ?? 0);
        checks.push(
          due > 0 || pending > 0
            ? check("WARN", "health", "telemux-crons", `total=${crons} enabled=${enabled} due=${due} pending=${pending}`, "Inspect /crons and telemux logs if due or pending jobs do not clear after the scheduler interval.")
            : check("PASS", "health", "telemux-crons", `total=${crons} enabled=${enabled} due=${due} pending=${pending}`)
        );
        checks.push(
          runningTurns > 0 || abandonedTurns > 0 || pendingTextOldestAgeMs > 5 * 60_000
            ? check("WARN", "health", "telemux-durable-work", `queued_running=${runningTurns} abandoned=${abandonedTurns} pending_text=${pendingTextCount} oldest_pending_text_ms=${pendingTextOldestAgeMs}`, "Inspect telemux logs and Telegram topics for interrupted queued turns or stale coalesced input.")
            : check("PASS", "health", "telemux-durable-work", `queued_running=${runningTurns} abandoned=${abandonedTurns} pending_text=${pendingTextCount}`)
        );
        checks.push(
          workerDegraded > 0
            ? check("WARN", "health", "telemux-worker-health", `degraded=${workerDegraded}`, "Run /health or brainctl doctor --workers for worker details.")
            : check("PASS", "health", "telemux-worker-health", "degraded=0")
        );
      } else {
        checks.push(check("WARN", "health", "telemux-crons", `healthz HTTP ${response.status}`));
      }
    } catch (error) {
      checks.push(check("WARN", "health", "telemux-crons", error instanceof Error ? error.message : String(error)));
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

  const tailscaleBin = commandPath("tailscale");
  if (tailscaleBin) {
    const status = run([tailscaleBin, "status", "--json"], { check: false });
    checks.push(status.code === 0 ? check("PASS", "tailscale", "status", "tailscale status --json succeeded") : check("WARN", "tailscale", "status", (status.stderr || status.stdout).trim()));
    const serve = run([tailscaleBin, "serve", "status"], { check: false });
    checks.push(serve.code === 0 ? check("PASS", "tailscale", "serve", "serve status available") : check("WARN", "tailscale", "serve", (serve.stderr || serve.stdout).trim()));
  }

  const outboxScans = await scanAllOutboxes(cfg);
  const outboxItems = outboxScans.flatMap((scan) => scan.items);
  const outboxCorrupt = outboxScans.flatMap((scan) => scan.corrupt);
  checks.push(check("PASS", "outbox", "queued-items", `${outboxItems.length} queued item(s)`));
  checks.push(
    outboxCorrupt.length
      ? check("FAIL", "outbox", "corrupt-items", `${outboxCorrupt.length} corrupt/unsafe item(s)`, "Run `brainctl outbox list` and then `brainctl outbox purge-corrupt --yes` only if those drafts are unrecoverable.")
      : check("PASS", "outbox", "corrupt-items", "none")
  );
  const unsafeOutboxDirs: string[] = [];
  for (const dir of [outboxParentRoot(cfg), ...outboxScans.map((scan) => scan.root)]) {
    const info = await lstat(dir).catch(() => null);
    if (!info) {
      continue;
    }
    if (info.isSymbolicLink()) {
      unsafeOutboxDirs.push(`${dir}:symlink`);
      continue;
    }
    const mode = info.mode & 0o777;
    if ((mode & 0o077) !== 0) {
      unsafeOutboxDirs.push(`${dir}:${mode.toString(8)}`);
    }
  }
  checks.push(
    unsafeOutboxDirs.length
      ? check("WARN", "outbox", "permissions", unsafeOutboxDirs.join(", "), `chmod 700 ${outboxParentRoot(cfg)} and each namespace directory after review.`)
      : check("PASS", "outbox", "permissions", "outbox directories are restrictive")
  );
  const unsafeFiles = [];
  for (const entry of [...outboxItems.map((item) => ({ path: item.path, label: item.item.id })), ...outboxCorrupt.map((item) => ({ path: item.path, label: `CORRUPT:${item.name}` }))]) {
    const info = await lstat(entry.path).catch(() => null);
    if (!info) {
      continue;
    }
    if (info.isSymbolicLink()) {
      unsafeFiles.push(`${entry.label}:symlink`);
      continue;
    }
    const fileMode = info.mode & 0o777;
    if ((fileMode & 0o077) !== 0) {
      unsafeFiles.push(`${entry.label}:${fileMode.toString(8)}`);
    }
  }
  checks.push(
    unsafeFiles.length
      ? check("FAIL", "outbox", "file-permissions", unsafeFiles.join(", "), "Run `chmod 600 ~/.local/state/brainstack/outbox/*/*.json` or purge corrupt/temp entries after review.")
      : check("PASS", "outbox", "file-permissions", "queued and corrupt item files are private")
  );
  checks.push(
    outboxItems.length
      ? check("WARN", "outbox", "plaintext-posture", "queued payloads are stored locally and protected by filesystem permissions, not encrypted by brainstack", "Use disk encryption and keep the state root private; see docs/outbox-security.md.")
      : check("PASS", "outbox", "plaintext-posture", "no queued payloads")
  );

  if (cfg.profile === "worker" || cfg.profile === "client-macos") {
    const localClone = clientLocalPathAbs(cfg);
    checks.push(
      gitExists(localClone)
        ? check("PASS", "client", "shared-brain-clone", localClone)
        : check("WARN", "client", "shared-brain-clone", `${localClone} missing or not a git clone`, "Run brainctl init for this profile, or run the rendered client bootstrap installer.")
    );
    const envPath = clientEnvPathAbs(cfg);
    checks.push(
      existsSync(envPath)
        ? check("PASS", "client", "shared-brain-env", envPath)
        : check("WARN", "client", "shared-brain-env", `${envPath} missing`, "Run brainctl init or the rendered client bootstrap installer.")
    );
    const writeConfig = brainWriteConfig(cfg);
    checks.push(
      writeConfig.baseUrl
        ? check("PASS", "client", "brain-base-url", "configured")
        : check("WARN", "client", "brain-base-url", "missing", "Set BRAIN_BASE_URL in env or shared-brain.env.")
    );
    checks.push(
      writeConfig.token
        ? check("PASS", "client", "brain-import-token", "present")
        : check("WARN", "client", "brain-import-token", "missing or empty", "Pass BRAIN_IMPORT_TOKEN or BRAIN_IMPORT_TOKEN_FILE during install/init, or edit shared-brain.env.")
    );
  }

  if (hasFlag(args, "write-smoke")) {
    checks.push(await brainWriteSmokeCheck(cfg));
  }

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

interface WorkerEnvCacheRecord {
  worker: string;
  fingerprint: string;
  path: string;
  harness: string;
  harnessBin: string;
  harnessVersion: string;
  detectedAt: string;
}

function workerEnvCachePath(cfg: BrainstackConfig): string {
  return join(cfg.paths.stateRoot, "worker-env-cache.json");
}

function workerEnvFingerprint(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, family?: HarnessName, bin?: string): string {
  const resolvedFamily = family || workerHarnessFamily(cfg, worker);
  const resolvedBin = bin || workerHarnessBin(cfg, worker, resolvedFamily);
  return sha256Hex(canonicalJson({
    worker: worker.name,
    transport: worker.transport,
    sshTarget: worker.sshTarget || null,
    sshUser: worker.sshUser || null,
    harness: resolvedFamily,
    harnessBin: resolvedBin
  }));
}

function readWorkerEnvCache(cfg: BrainstackConfig): Record<string, WorkerEnvCacheRecord> {
  const path = workerEnvCachePath(cfg);
  if (!existsSync(path)) {
    return {};
  }
  try {
    return JSON.parse(readFileSync(path, "utf8")) as Record<string, WorkerEnvCacheRecord>;
  } catch {
    return {};
  }
}

async function writeWorkerEnvCache(cfg: BrainstackConfig, cache: Record<string, WorkerEnvCacheRecord>): Promise<void> {
  await ensureDir(dirname(workerEnvCachePath(cfg)));
  await writeText(workerEnvCachePath(cfg), `${JSON.stringify(cache, null, 2)}\n`, 0o600);
}

function cachedWorkerPath(cfg: BrainstackConfig, worker: BrainstackWorkerConfig): string | null {
  const record = readWorkerEnvCache(cfg)[worker.name];
  const family = workerHarnessFamily(cfg, worker);
  const bin = workerHarnessBin(cfg, worker, family);
  if (!record || record.fingerprint !== workerEnvFingerprint(cfg, worker, family, bin) || !record.path.trim()) {
    return null;
  }
  const detectedAt = Date.parse(record.detectedAt);
  if (!Number.isFinite(detectedAt) || Date.now() - detectedAt > 7 * 24 * 60 * 60 * 1000) {
    return null;
  }
  return record.path;
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
  const host = workerSshHost(worker);
  const remoteHost = host.includes(":") && !host.startsWith("[") ? `[${host}]` : host;
  const embeddedUser = workerSshEmbeddedUser(worker);
  const user = worker.sshUser || embeddedUser;
  return user ? `${user}@${remoteHost}` : remoteHost;
}

function workerSshEmbeddedUser(worker: BrainstackWorkerConfig): string | null {
  const target = worker.sshTarget || worker.name;
  return target.includes("@") ? target.slice(0, target.lastIndexOf("@")) : null;
}

function workerSshHost(worker: BrainstackWorkerConfig): string {
  const target = worker.sshTarget || worker.name;
  const withoutUser = target.includes("@") ? target.slice(target.lastIndexOf("@") + 1) : target;
  const bracketMatch = withoutUser.match(/^\[([^\]]+)\](?::\d+)?$/);
  if (bracketMatch) {
    return bracketMatch[1];
  }
  if (/^[^:]+:\d+$/.test(withoutUser)) {
    return withoutUser.replace(/:\d+$/, "");
  }
  return withoutUser;
}

function workerSshPort(worker: BrainstackWorkerConfig): string | null {
  const target = worker.sshTarget || worker.name;
  const withoutUser = target.includes("@") ? target.slice(target.lastIndexOf("@") + 1) : target;
  const bracketMatch = withoutUser.match(/^\[[^\]]+\]:(\d+)$/);
  if (bracketMatch) {
    return bracketMatch[1];
  }
  const hostPort = withoutUser.match(/^[^:]+:(\d+)$/);
  return hostPort ? hostPort[1] : null;
}

function workerSshKnownHostsLookup(worker: BrainstackWorkerConfig): string {
  const host = workerSshHost(worker);
  const port = workerSshPort(worker);
  return port ? `[${host}]:${port}` : host;
}

function workerSshKnownHostsPath(cfg: BrainstackConfig, worker: BrainstackWorkerConfig): string {
  return absWithHome(worker.sshKnownHostsPath || join(cfg.paths.configRoot, "ssh_known_hosts"), cfg.paths.home);
}

function workerSshTrustMode(worker: BrainstackWorkerConfig): WorkerSshTrustMode {
  return worker.sshTrustMode === "accept-new" ? "accept-new" : "pinned";
}

function workerSshTrustArgs(cfg: BrainstackConfig, worker: BrainstackWorkerConfig): string[] {
  const mode = workerSshTrustMode(worker);
  return [
    "-o",
    mode === "accept-new" ? "StrictHostKeyChecking=accept-new" : "StrictHostKeyChecking=yes",
    "-o",
    `UserKnownHostsFile=${workerSshKnownHostsPath(cfg, worker)}`
  ];
}

function workerSshPortArgs(worker: BrainstackWorkerConfig): string[] {
  const port = workerSshPort(worker);
  return port ? ["-p", port] : [];
}

function workerUserPathPrelude(): string {
  return `
# Worker tool and harness commands are resolved through the target user's own
# shell PATH so user-managed Bun/Codex/Claude installs work over non-login SSH.
if [ -z "\${BRAINSTACK_SKIP_USER_PATH_RESOLVE:-}" ]; then
  __brainstack_detected_path=""
  if [ -n "\${BRAINSTACK_WORKER_PATH:-}" ]; then
    __brainstack_detected_path="$BRAINSTACK_WORKER_PATH"
  elif [ -n "\${SHELL:-}" ] && [ -x "$SHELL" ]; then
    __brainstack_detected_path="$(
      if command -v timeout >/dev/null 2>&1; then
        timeout 5s "$SHELL" -lic 'printf "__BRAINSTACK_PATH__%s\\n" "$PATH"' 2>/dev/null
      else
        "$SHELL" -lic 'printf "__BRAINSTACK_PATH__%s\\n" "$PATH"' 2>/dev/null &
        __brainstack_path_pid=$!
        __brainstack_path_elapsed=0
        while kill -0 "$__brainstack_path_pid" 2>/dev/null; do
          if [ "$__brainstack_path_elapsed" -ge 5 ]; then
            kill "$__brainstack_path_pid" 2>/dev/null || true
            sleep 1
            if kill -0 "$__brainstack_path_pid" 2>/dev/null; then
              kill -9 "$__brainstack_path_pid" 2>/dev/null || true
            fi
            break
          fi
          sleep 1
          __brainstack_path_elapsed=$((__brainstack_path_elapsed + 1))
        done
        wait "$__brainstack_path_pid" 2>/dev/null || true
      fi | sed -n 's/.*__BRAINSTACK_PATH__//p' | tail -n 1
    )"
  fi
  if [ -n "$__brainstack_detected_path" ]; then
    PATH="$__brainstack_detected_path:$PATH"
    export PATH
  fi
  unset __brainstack_detected_path
fi
`.trim();
}

function runWorkerShell(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, script: string, timeoutSeconds = 10, usePathCache = true) {
  const family = workerHarnessFamily(cfg, worker);
  const bin = workerHarnessBin(cfg, worker, family);
  const cachedPath = usePathCache ? cachedWorkerPath(cfg, worker) : null;
  const cachePrelude = cachedPath ? `BRAINSTACK_WORKER_PATH=${quoteForBash(cachedPath)}\nexport BRAINSTACK_WORKER_PATH\n` : "";
  const uncachedPrelude = usePathCache ? "" : "unset BRAINSTACK_WORKER_PATH\n";
  const harnessPrelude = `harness=${quoteForBash(family)}\nharness_family=${quoteForBash(family)}\nharness_bin=${quoteForBash(bin)}\n`;
  const wrappedScript = `${uncachedPrelude}${cachePrelude}${harnessPrelude}${workerUserPathPrelude()}\n${script}`;
  if (worker.transport === "local") {
    return run(["bash", "-lc", wrappedScript], { check: false, timeoutMs: timeoutSeconds * 1000 });
  }
  const sshArgs =
    worker.transport === "tailscale-ssh"
      ? ["tailscale", "ssh", workerRemoteTarget(worker), "bash", "-lc", wrappedScript]
      : [
          "ssh",
          "-o",
          "BatchMode=yes",
          "-o",
          "ConnectTimeout=8",
          ...workerSshTrustArgs(cfg, worker),
          ...workerSshPortArgs(worker),
          workerRemoteTarget(worker),
          "bash",
          "-lc",
          wrappedScript
        ];
  const timeoutBin = Bun.which("timeout");
  return run(timeoutBin ? [timeoutBin, `${timeoutSeconds}s`, ...sshArgs] : sshArgs, { check: false });
}

const WORKER_REQUIRED_COMMANDS = ["bun", "git", "ssh", "tailscale"];

function regexEscape(value: string): string {
  return value.replace(/[\\^$.*+?()[\]{}|]/g, "\\$&");
}

function workerProbeValue(output: string, key: string): string | null {
  const match = output.match(new RegExp(`^${regexEscape(key)}=(.*)$`, "m"));
  return match?.[1]?.trim() || null;
}

function workerCommandCheck(worker: BrainstackWorkerConfig, output: string, commandName: string): DoctorCheck {
  const path = workerProbeValue(output, `cmd:${commandName}`);
  if (!path || path === "missing") {
    return check(
      "FAIL",
      "workers",
      `worker:${worker.name}:cmd:${commandName}`,
      "missing from worker shell PATH",
      `Install ${commandName} on ${worker.name}, or add its bin directory to the worker user's login shell PATH.`
    );
  }

  const version = workerProbeValue(output, `cmdver:${commandName}`);
  return check("PASS", "workers", `worker:${worker.name}:cmd:${commandName}`, version ? `${path}; ${version}` : path);
}

function workerSshTrustCheck(cfg: BrainstackConfig, worker: BrainstackWorkerConfig): DoctorCheck | null {
  if (worker.transport !== "ssh") {
    return null;
  }
  const mode = workerSshTrustMode(worker);
  if (mode === "accept-new") {
    const bootstrapAllowed = truthyEnv(process.env.BRAINSTACK_ALLOW_ACCEPT_NEW_DOCTOR);
    return check(
      bootstrapAllowed ? "WARN" : "FAIL",
      "workers",
      `worker:${worker.name}:ssh-trust`,
      "bootstrap trust mode accept-new",
      bootstrapAllowed
        ? `Bootstrap probing is explicitly enabled by BRAINSTACK_ALLOW_ACCEPT_NEW_DOCTOR. Run brainctl trust-worker --config <config> --worker ${worker.name} and switch the worker to sshTrustMode: pinned after enrollment.`
        : `Refusing remote doctor probes under TOFU. Run brainctl trust-worker --config <config> --worker ${worker.name}, switch the worker to sshTrustMode: pinned, or set BRAINSTACK_ALLOW_ACCEPT_NEW_DOCTOR=true for one bootstrap probe.`
    );
  }
  const knownHostsPath = workerSshKnownHostsPath(cfg, worker);
  if (!existsSync(knownHostsPath)) {
    return check(
      "FAIL",
      "workers",
      `worker:${worker.name}:ssh-trust`,
      `pinned known_hosts file missing: ${knownHostsPath}`,
      `Run brainctl trust-worker --config <config> --worker ${worker.name} before dispatching to this worker.`
    );
  }
  const sshKeygen = Bun.which("ssh-keygen");
  if (!sshKeygen) {
    return check("WARN", "workers", `worker:${worker.name}:ssh-trust`, "ssh-keygen missing; cannot verify pinned host entry");
  }
  const host = workerSshKnownHostsLookup(worker);
  const found = run([sshKeygen, "-F", host, "-f", knownHostsPath], { check: false });
  return found.code === 0
    ? check("PASS", "workers", `worker:${worker.name}:ssh-trust`, `pinned host key present for ${host}`)
    : check(
        "FAIL",
        "workers",
        `worker:${worker.name}:ssh-trust`,
        `no pinned host key for ${host} in ${knownHostsPath}`,
        `Run brainctl trust-worker --config <config> --worker ${worker.name}, then rerun doctor.`
      );
}

async function workerDoctorChecks(cfg: BrainstackConfig, deep: boolean): Promise<DoctorCheck[]> {
  const checks: DoctorCheck[] = [];
  for (const worker of defaultWorkers(cfg)) {
    const trustCheck = workerSshTrustCheck(cfg, worker);
    if (trustCheck) {
      checks.push(trustCheck);
      if (trustCheck.status === "FAIL" && worker.transport === "ssh" && workerSshTrustMode(worker) === "accept-new") {
        continue;
      }
    }
    const family = workerHarnessFamily(cfg, worker);
    const bin = workerHarnessBin(cfg, worker, family);
    const required =
      family === "codex"
        ? ["--dangerously-bypass-approvals-and-sandbox", "--skip-git-repo-check", "--output-last-message"]
        : ["--dangerously-skip-permissions", "--permission-mode", "--output-format"];
    const script = [
      "set -euo pipefail",
      `harness_bin=${quoteForBash(bin)}`,
      `harness_family=${quoteForBash(family)}`,
      "printf 'worker=%s\\n' \"$(hostname)\"",
      "printf 'path=%s\\n' \"$PATH\"",
      "if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then printf 'sudo=ok\\n'; else printf 'sudo=fail\\n'; fi",
      "probe_command() {",
      "  name=\"$1\"",
      "  if ! command -v \"$name\" >/dev/null 2>&1; then printf 'cmd:%s=missing\\n' \"$name\"; return 0; fi",
      "  path=\"$(command -v \"$name\")\"",
      "  printf 'cmd:%s=%s\\n' \"$name\" \"$path\"",
      "  case \"$name\" in",
      "    ssh) version=\"$($path -V 2>&1 | head -n 1 || true)\" ;;",
      "    *) version=\"$($path --version 2>&1 | head -n 1 || true)\" ;;",
      "  esac",
      "  printf 'cmdver:%s=%s\\n' \"$name\" \"$version\"",
      "}",
      ...WORKER_REQUIRED_COMMANDS.map((name) => `probe_command ${quoteForBash(name)}`),
      "if ! command -v \"$harness_bin\" >/dev/null 2>&1; then printf 'harness_bin=missing\\n'; exit 7; fi",
      "printf 'harness_bin=%s\\n' \"$(command -v \"$harness_bin\")\"",
      "\"$harness_bin\" --version 2>&1 | head -n 1 | sed 's/^/version=/' || true",
      "if [ \"$harness_family\" = codex ]; then help=\"$($harness_bin exec --help 2>&1 || true)\"; else help=\"$($harness_bin --help 2>&1 || true)\"; fi",
      ...required.map((needle) => `case "$help" in *${quoteForBash(needle)}*) printf 'flag:${needle}=ok\\n' ;; *) printf 'flag:${needle}=missing\\n' ;; esac`),
      "brainstack_config_value() { key=\"$1\"; file=\"$2\"; [ -f \"$file\" ] || return 0; awk -v key=\"$key\" 'BEGIN { in_section=0 } /^[[:space:]]*\\[/ { in_section=1 } in_section == 0 { pattern=\"^[[:space:]]*\" key \"[[:space:]]*=\"; if ($0 ~ pattern) { sub(/^[^=]*=[[:space:]]*/, \"\"); sub(/[[:space:]]*#.*/, \"\"); gsub(/^[[:space:]\\\"]+|[[:space:]\\\"]+$/, \"\"); print; exit } }' \"$file\" || true; }",
      "if [ \"$harness_family\" = codex ]; then codex_config=\"${CODEX_HOME:-$HOME/.codex}/config.toml\"; model_config=\"$(brainstack_config_value model \"$codex_config\")\"; effort_config=\"$(brainstack_config_value model_reasoning_effort \"$codex_config\")\"; printf 'model=%s\\n' \"${model_config:-default}\"; printf 'effort=%s\\n' \"${effort_config:-default}\"; else printf 'model=default\\n'; printf 'effort=n/a\\n'; fi",
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
    const result = runWorkerShell(cfg, worker, script, deep ? 30 : 12, false);
    const combined = `${result.stdout}\n${result.stderr}`.trim();
    if (result.code !== 0) {
      checks.push(check("FAIL", "workers", `worker:${worker.name}`, combined || `exit ${result.code}`, `Verify SSH and ${family} on the worker.`));
      continue;
    }
    const harnessPath = workerProbeValue(combined, "harness_bin") || bin;
    const detectedPath = workerProbeValue(combined, "path");
    if (detectedPath) {
      const cache = readWorkerEnvCache(cfg);
      cache[worker.name] = {
        worker: worker.name,
        fingerprint: workerEnvFingerprint(cfg, worker, family, bin),
        path: detectedPath,
        harness: family,
        harnessBin: harnessPath,
        harnessVersion: workerProbeValue(combined, "version") || "",
        detectedAt: new Date().toISOString()
      };
      await writeWorkerEnvCache(cfg, cache);
    }
    const model = workerProbeValue(combined, "model") || "default";
    const effort = workerProbeValue(combined, "effort") || (family === "claude" ? "n/a" : "default");
    checks.push(check("PASS", "workers", `worker:${worker.name}`, `reachable via ${worker.transport}; harness=${family} bin=${harnessPath} model=${model} effort=${effort}`));
    for (const commandName of WORKER_REQUIRED_COMMANDS) {
      checks.push(workerCommandCheck(worker, combined, commandName));
    }
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

function clientEnv(cfg: BrainstackConfig): Record<string, string> {
  return readEnvFile(clientEnvPathAbs(cfg));
}

function resolveClientEnvValue(cfg: BrainstackConfig, name: string): string {
  return process.env[name] || clientEnv(cfg)[name] || "";
}

function brainWriteConfig(cfg: BrainstackConfig): { baseUrl: string; token: string } {
  const env = clientEnv(cfg);
  const baseUrl = process.env.BRAIN_BASE_URL || env.BRAIN_BASE_URL || cfg.brain.publicBaseUrl;
  const tokenValue = process.env.BRAIN_IMPORT_TOKEN || env.BRAIN_IMPORT_TOKEN || "";
  const legacyToken = process.env.BRAIN_WRITE_TOKEN || env.BRAIN_WRITE_TOKEN || "";
  if (legacyToken && !tokenValue) {
    throw new Error("BRAIN_WRITE_TOKEN is no longer accepted; set BRAIN_IMPORT_TOKEN for client writes and keep BRAIN_ADMIN_TOKEN only on the control host.");
  }
  return { baseUrl, token: tokenValue };
}

type ProjectBrainClassification = "work" | "personal" | "neutral";
type ProjectBrainWriteMode = "true" | "propose-only" | "false";

interface ProjectBrain {
  id: string;
  label: string;
  classification: ProjectBrainClassification;
  localPath: string;
  gitRemote: string;
  baseUrl: string;
  importTokenEnv: string;
  connectionTrusted: boolean;
  untrustedConnectionFields?: string[];
  sections: string[];
  sectionsRestricted?: boolean;
  readMode?: "allow" | "ask-once" | "ask-always" | "never";
  write: ProjectBrainWriteMode;
  requestedWrite?: ProjectBrainWriteMode;
  writeTrusted?: boolean;
  writeDowngraded?: boolean;
}

interface ResolvedProjectContext {
  repo: string;
  configPath: string | null;
  brains: ProjectBrain[];
  allowedBrains: ProjectBrain[];
  deniedBrains: ProjectBrain[];
  writeDefault: string;
  crossBrainWrites: Record<string, string>;
  sessionPath: string;
}

interface AllowRule {
  decision: string;
  sections: string[];
  updated_at: string;
}

function projectSessionPath(cfg: BrainstackConfig, repo: string): string {
  return join(cfg.paths.stateRoot, "context-sessions", `${sha256Hex(repo).slice(0, 32)}.json`);
}

function allowRulesPath(cfg: BrainstackConfig): string {
  return join(cfg.paths.configRoot, "allow-rules.json");
}

function profilesPath(cfg: BrainstackConfig): string {
  return join(cfg.paths.configRoot, "profiles.yaml");
}

function normalizeProjectBrainWrite(value: unknown, fallback: ProjectBrain["write"]): ProjectBrain["write"] {
  if (value === false) {
    return "false";
  }
  if (value === true) {
    return "true";
  }
  if (typeof value !== "string" || !value.trim()) {
    return fallback;
  }
  const normalized = value.trim();
  if (normalized === "true" || normalized === "propose-only" || normalized === "false") {
    return normalized;
  }
  throw new Error(`Unsupported project brain write mode: ${normalized}`);
}

function normalizeProjectReadMode(value: unknown, fallback: ProjectBrain["readMode"] = "allow"): ProjectBrain["readMode"] {
  if (value === false) {
    return "never";
  }
  if (value === true || value === undefined || value === null || value === "") {
    return fallback;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true" || normalized === "allow") {
      return "allow";
    }
    if (normalized === "false" || normalized === "never" || normalized === "deny") {
      return "never";
    }
    if (normalized === "ask-once" || normalized === "ask-always") {
      return normalized;
    }
  }
  if (typeof value === "object" && value && !Array.isArray(value)) {
    return normalizeProjectReadMode((value as Record<string, unknown>).mode, fallback);
  }
  throw new Error(`Unsupported project brain read mode: ${String(value)}`);
}

function applyTrustedClassification(
  trusted: ProjectBrainClassification | "",
  actual: ProjectBrainClassification
): ProjectBrainClassification {
  if (trusted === "personal") {
    return "personal";
  }
  if (trusted === "work") {
    return actual === "personal" ? "personal" : "work";
  }
  return actual;
}

function normalizeProjectBrainClassification(value: unknown): ProjectBrainClassification | "" {
  if (typeof value !== "string") {
    return "";
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === "work" || normalized === "personal" || normalized === "neutral") {
    return normalized;
  }
  throw new Error(`Unsupported project brain classification: ${value}`);
}

function inferredProjectBrainClassification(id: string, label: string): ProjectBrainClassification {
  const text = `${id} ${label}`;
  if (/personal|private|journal|health|family|finance/i.test(text)) {
    return "personal";
  }
  if (/work|company|lindy|corp|corpo|business|team/i.test(text)) {
    return "work";
  }
  return "neutral";
}

function classifyProjectBrain(id: string, label: string, explicit?: unknown): ProjectBrainClassification {
  const explicitClassification = normalizeProjectBrainClassification(explicit);
  const inferred = inferredProjectBrainClassification(id, label);
  if (!explicitClassification) {
    return inferred;
  }
  if (inferred === "personal") {
    return "personal";
  }
  if (inferred === "work") {
    return explicitClassification === "personal" ? "personal" : "work";
  }
  return explicitClassification;
}

function trustedProjectBrainWrite(trustedSource: Record<string, unknown>): ProjectBrainWriteMode | "" {
  return "write" in trustedSource ? normalizeProjectBrainWrite(trustedSource.write, "propose-only") : "";
}

function withTrustedProjectBrainWrite(entry: Record<string, unknown>, trustedSource: Record<string, unknown>): Record<string, unknown> {
  const spec = trustedProjectBrainWrite(trustedSource);
  return spec ? { ...entry, __brainstackTrustedWrite: spec } : entry;
}

function projectBrainWriteRank(value: ProjectBrainWriteMode): number {
  switch (value) {
    case "false":
      return 0;
    case "propose-only":
      return 1;
    case "true":
      return 2;
  }
}

function effectiveProjectBrainWrite(entry: Record<string, unknown>, requested: ProjectBrainWriteMode): {
  write: ProjectBrainWriteMode;
  writeTrusted: boolean;
  writeDowngraded: boolean;
} {
  const trustedWrite = stringAt(entry, "__brainstackTrustedWrite", "") as ProjectBrainWriteMode | "";
  const ceiling = trustedWrite || "propose-only";
  const write = projectBrainWriteRank(requested) > projectBrainWriteRank(ceiling) ? ceiling : requested;
  return {
    write,
    writeTrusted: requested !== "true" || trustedWrite === "true",
    writeDowngraded: write !== requested
  };
}

function isLoopbackHost(value: string): boolean {
  const normalized = value.trim().toLowerCase().replace(/^\[|\]$/g, "").replace(/\.$/, "");
  if (normalized === "localhost" || normalized === "::1") {
    return true;
  }
  if (normalized.startsWith("::ffff:")) {
    const mapped = normalized.slice("::ffff:".length);
    if (mapped.includes(".")) {
      return isLoopbackHost(mapped);
    }
    const groups = mapped.split(":").map((part) => Number.parseInt(part || "0", 16));
    if (groups.length <= 2 && groups.every((part) => Number.isFinite(part) && part >= 0 && part <= 0xffff)) {
      const value = ((groups[0] || 0) << 16) + (groups[1] || 0);
      return ((value >>> 24) & 0xff) === 127;
    }
    return false;
  }
  if (/^127(?:\.|$)/.test(normalized)) {
    return true;
  }
  const ipVersion = isIP(normalized);
  if (ipVersion === 4) {
    return normalized.split(".")[0] === "127";
  }
  return false;
}

function isLocalGitRemote(remote: string): boolean {
  const trimmed = remote.trim();
  if (!trimmed) {
    return false;
  }
  if (trimmed.startsWith("/") || trimmed.startsWith("~/") || trimmed === "~" || trimmed.startsWith("./") || trimmed.startsWith("../")) {
    return true;
  }
  try {
    const parsed = new URL(trimmed);
    if (parsed.protocol === "file:") {
      return true;
    }
    if ((parsed.protocol === "ssh:" || parsed.protocol === "git:" || parsed.protocol === "http:" || parsed.protocol === "https:") && isLoopbackHost(parsed.hostname)) {
      return true;
    }
  } catch {
    // Fall through to scp-like syntax checks.
  }
  const scpLike = trimmed.match(/^([^@/:]+@)?(\[[^\]]+\]|[^:/]+):(.+)$/);
  return Boolean(scpLike && isLoopbackHost(scpLike[2] || ""));
}

function stripYamlKeyQuotes(input: string): string {
  const trimmed = input.trim();
  if ((trimmed.startsWith('"') && trimmed.endsWith('"')) || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function objectAt(input: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = input[key];
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function entriesFromBrainsValue(value: unknown): Array<Record<string, unknown>> {
  if (Array.isArray(value)) {
    return value.map((entry) => {
      if (typeof entry === "string") {
        return { id: entry };
      }
      return entry && typeof entry === "object" ? (entry as Record<string, unknown>) : {};
    });
  }
  if (value && typeof value === "object") {
    return Object.entries(value as Record<string, unknown>).map(([id, entry]) => ({
      id,
      ...(entry && typeof entry === "object" ? (entry as Record<string, unknown>) : {})
    }));
  }
  return [];
}

function safeProjectSection(section: string): boolean {
  return Boolean(section) && !section.startsWith("/") && !section.split("/").includes("..") && /^[A-Za-z0-9._/@+-]+(?:\/[A-Za-z0-9._/@+-]+)*$/.test(section);
}

function validateProjectSections(id: string, sections: string[]): void {
  const unsafe = sections.filter((section) => !safeProjectSection(section));
  if (unsafe.length) {
    throw new Error(`Invalid project brain config for ${id}: unsafe section path(s) ${unsafe.join(", ")}`);
  }
}

function simpleGlobMatches(patternInput: string, repo: string, home: string): boolean {
  let pattern = absWithHome(stripYamlKeyQuotes(patternInput), home);
  if (pattern.endsWith("/**")) {
    const prefixRaw = pattern.slice(0, -3);
    const prefix = existsSync(prefixRaw) ? realpathSync(prefixRaw) : prefixRaw;
    return repo === prefix || repo.startsWith(`${prefix}/`);
  }
  if (!pattern.includes("*") && existsSync(pattern)) {
    pattern = realpathSync(pattern);
  }
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*\*/g, ".*").replace(/\*/g, "[^/]*");
  return new RegExp(`^${escaped}$`).test(repo);
}

function bestProfilesProjectMatch(profilesRaw: Record<string, unknown>, repo: string, home: string): Record<string, unknown> {
  const projects = objectAt(profilesRaw, "projects");
  let best: { pattern: string; raw: Record<string, unknown> } | null = null;
  for (const [rawPattern, rawValue] of Object.entries(projects)) {
    if (!rawValue || typeof rawValue !== "object" || Array.isArray(rawValue)) {
      continue;
    }
    const pattern = stripYamlKeyQuotes(rawPattern);
    if (!simpleGlobMatches(pattern, repo, home)) {
      continue;
    }
    if (!best || pattern.length > best.pattern.length) {
      best = { pattern, raw: rawValue as Record<string, unknown> };
    }
  }
  return best?.raw || {};
}

function mergeRecord(base: Record<string, unknown>, override: Record<string, unknown>): Record<string, unknown> {
  const merged = { ...base, ...override };
  if (base.read && typeof base.read === "object" && override.read && typeof override.read === "object" && !Array.isArray(base.read) && !Array.isArray(override.read)) {
    merged.read = { ...(base.read as Record<string, unknown>), ...(override.read as Record<string, unknown>) };
  }
  return merged;
}

function projectBrainRemoteSpec(entry: Record<string, unknown>): string {
  return stringAt(entry, "gitRemote", stringAt(entry, "remote", stringAt(entry, "remoteSsh", "")));
}

function projectBrainBaseUrlSpec(entry: Record<string, unknown>): string {
  return stringAt(entry, "baseUrl", stringAt(entry, "url", ""));
}

function projectBrainImportTokenEnvSpec(entry: Record<string, unknown>): string {
  return stringAt(entry, "importTokenEnv", "");
}

function projectBrainOverrides(raw: Record<string, unknown>, id: string): Record<string, unknown> {
  const value = raw[id];
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const override = value as Record<string, unknown>;
    if ("mode" in override && !("read" in override)) {
      return { ...override, read: { mode: override.mode, sections: override.sections || [] } };
    }
    return override;
  }
  return {};
}

function projectBrainLocalPathSpec(entry: Record<string, unknown>): string {
  return stringAt(entry, "localClone", stringAt(entry, "localPath", stringAt(entry, "path", "")));
}

function withTrustedLocalPath(entry: Record<string, unknown>, cfg: BrainstackConfig, trustedSource: Record<string, unknown>): Record<string, unknown> {
  const spec = projectBrainLocalPathSpec(trustedSource);
  return spec ? { ...entry, __brainstackTrustedLocalPath: absWithHome(spec, cfg.paths.home) } : entry;
}

function withTrustedGitRemote(entry: Record<string, unknown>, trustedSource: Record<string, unknown>): Record<string, unknown> {
  const spec = projectBrainRemoteSpec(trustedSource);
  return spec ? { ...entry, __brainstackTrustedGitRemote: spec } : entry;
}

function withTrustedBrainConnection(entry: Record<string, unknown>, trustedSource: Record<string, unknown>): Record<string, unknown> {
  const baseUrl = projectBrainBaseUrlSpec(trustedSource);
  const importTokenEnv = projectBrainImportTokenEnvSpec(trustedSource);
  return {
    ...entry,
    ...(baseUrl ? { __brainstackTrustedBaseUrl: baseUrl } : {}),
    ...(importTokenEnv ? { __brainstackTrustedImportTokenEnv: importTokenEnv } : {})
  };
}

function trustedClassificationFor(id: string, trusted: Record<string, unknown>): string {
  if (!Object.keys(trusted).length) {
    return "";
  }
  return classifyProjectBrain(id, stringAt(trusted, "label", id), trusted.classification);
}

function mergeTrustedProjectBrain(id: string, entry: Record<string, unknown>, globalBrains: Record<string, unknown>, matchedProjectRaw: Record<string, unknown>, cfg: BrainstackConfig): Record<string, unknown> {
  const globalRaw = objectAt(globalBrains, id);
  const profileOverride = projectBrainOverrides(matchedProjectRaw, id);
  const trustedBase = mergeRecord({ id, ...globalRaw }, profileOverride);
  const merged = mergeRecord(trustedBase, entry);
  const trustedClassification = trustedClassificationFor(id, trustedBase);
  const withLocalPath = withTrustedLocalPath(withTrustedLocalPath(merged, cfg, globalRaw), cfg, profileOverride);
  const withRemote = withTrustedGitRemote(withTrustedGitRemote(withLocalPath, globalRaw), profileOverride);
  const withConnection = withTrustedBrainConnection(withTrustedBrainConnection(withRemote, globalRaw), profileOverride);
  const withWrite = withTrustedProjectBrainWrite(withTrustedProjectBrainWrite(withConnection, globalRaw), profileOverride);
  return trustedClassification ? { ...withWrite, __brainstackTrustedClassification: trustedClassification } : withWrite;
}

function policyRank(value: string | undefined): number {
  switch ((value || "").trim().toLowerCase()) {
    case "never":
      return 0;
    case "ask":
    case "ask-once":
    case "ask-always":
      return 1;
    case "allow":
    case "true":
      return 2;
    default:
      return 1;
  }
}

function normalizeCrossBrainPolicy(value: string): string {
  const normalized = value.trim().toLowerCase();
  if (normalized === "true") {
    return "allow";
  }
  if (normalized === "ask-once" || normalized === "ask-always") {
    return "ask";
  }
  if (normalized === "allow" || normalized === "ask" || normalized === "never") {
    return normalized;
  }
  throw new Error(`Unsupported crossBrainWrites policy: ${value}`);
}

function trustedGenericPolicyKeysFor(repoKey: string): string[] {
  const lower = repoKey.toLowerCase();
  const candidates: string[] = [];
  if (lower.startsWith("personalto")) {
    candidates.push("personalToWork", "personalToCompany");
  }
  if (lower.endsWith("topersonal") || lower.endsWith("toprivate") || lower.startsWith("workto") || lower.startsWith("companyto")) {
    candidates.push("workToPersonal", "companyToPersonal");
  }
  return candidates;
}

type CrossBrainPolicyRef = { id: string; classification: ProjectBrain["classification"] };

function projectBrainEntryPolicyRef(entry: Record<string, unknown>): CrossBrainPolicyRef {
  const id = stringAt(entry, "id", "default");
  const label = stringAt(entry, "label", id);
  const rawClassification = classifyProjectBrain(id, label, entry.classification);
  const trustedClassification = stringAt(entry, "__brainstackTrustedClassification", "") as ProjectBrain["classification"] | "";
  return {
    id,
    classification: applyTrustedClassification(trustedClassification, rawClassification)
  };
}

function crossBrainPolicyCandidates(source: CrossBrainPolicyRef, target: CrossBrainPolicyRef): string[] {
  const candidates = [
    `${source.id}To${titleCaseBrainKey(target.id)}`,
    `${source.id}To${titleCaseBrainKey(target.classification)}`,
    `${source.classification}To${titleCaseBrainKey(target.id)}`,
    `${source.classification}To${titleCaseBrainKey(target.classification)}`
  ];
  if (source.classification === "personal" && target.classification === "work") {
    candidates.push("personalToWork", "personalToCompany");
  }
  if (source.classification === "work" && target.classification === "personal") {
    candidates.push("workToPersonal", "companyToPersonal");
  }
  return [...new Set(candidates)];
}

function assertRepoLocalCrossBrainRuleMonotonic(
  key: string,
  normalized: string,
  trustedRules: Record<string, string>,
  brainEntries: Array<Record<string, unknown>>
): void {
  const refs = brainEntries.map((entry) => projectBrainEntryPolicyRef(entry));
  for (const source of refs) {
    for (const target of refs) {
      if (source.id === target.id) {
        continue;
      }
      if (!crossBrainPolicyCandidates(source, target).includes(key)) {
        continue;
      }
      const trustedPolicy = crossBrainPolicyRule(trustedRules, source, target);
      if (policyRank(normalized) > policyRank(trustedPolicy)) {
        throw new Error(
          `Repo-local crossBrainWrites.${key} cannot weaken trusted/effective profile policy ${trustedPolicy} for ${source.id}->${target.id} to ${normalized}. Change ~/.config/brainstack/profiles.yaml if this is intentional.`
        );
      }
    }
  }
}

function mergeCrossBrainWritesMonotonic(
  trusted: Record<string, unknown>,
  repoLocal: Record<string, unknown>,
  brainEntries: Array<Record<string, unknown>> = []
): Record<string, string> {
  const merged: Record<string, string> = {};
  for (const [key, value] of Object.entries(trusted)) {
    if (typeof value === "string" && value.trim()) {
      merged[key] = normalizeCrossBrainPolicy(value);
    }
  }
  for (const [key, value] of Object.entries(repoLocal)) {
    if (typeof value !== "string" || !value.trim()) {
      continue;
    }
    const normalized = normalizeCrossBrainPolicy(value);
    const existing = merged[key];
    if (existing && policyRank(normalized) > policyRank(existing)) {
      throw new Error(`Repo-local crossBrainWrites.${key} cannot weaken trusted profile policy ${existing} to ${normalized}. Change ~/.config/brainstack/profiles.yaml if this is intentional.`);
    }
    for (const genericKey of trustedGenericPolicyKeysFor(key)) {
      const generic = merged[genericKey];
      if (generic && policyRank(normalized) > policyRank(generic)) {
        throw new Error(`Repo-local crossBrainWrites.${key} cannot weaken trusted generic profile policy ${genericKey}=${generic} to ${normalized}. Change ~/.config/brainstack/profiles.yaml if this is intentional.`);
      }
    }
    assertRepoLocalCrossBrainRuleMonotonic(key, normalized, merged, brainEntries);
    merged[key] = normalized;
  }
  return merged;
}

function projectRawWithProfiles(fileRaw: Record<string, unknown>, profilesRaw: Record<string, unknown>, repo: string, cfg: BrainstackConfig): Record<string, unknown> {
  const defaultRaw = objectAt(profilesRaw, "default");
  const matchedProjectRaw = bestProfilesProjectMatch(profilesRaw, repo, cfg.paths.home);
  const globalBrains = objectAt(profilesRaw, "brains");
  const selectorRaw = "brains" in fileRaw ? fileRaw : Object.keys(matchedProjectRaw).length ? matchedProjectRaw : defaultRaw;
  const selectorEntries = entriesFromBrainsValue(selectorRaw.brains);
  const entries = selectorEntries.length
    ? selectorEntries
    : entriesFromBrainsValue(defaultRaw.brains).length
      ? entriesFromBrainsValue(defaultRaw.brains)
      : [];
  const brains = entries.map((entry) => {
    const id = stringAt(entry, "id", "default");
    return mergeTrustedProjectBrain(id, entry, globalBrains, matchedProjectRaw, cfg);
  });
  const mergedBrains = "brains" in fileRaw ? projectBrainsMergedWithProfiles(fileRaw.brains, globalBrains, matchedProjectRaw, cfg) : brains;
  return {
    ...defaultRaw,
    ...matchedProjectRaw,
    ...fileRaw,
    brains: mergedBrains,
    crossBrainWrites: mergeCrossBrainWritesMonotonic({
      ...objectAt(defaultRaw, "crossBrainWrites"),
      ...objectAt(matchedProjectRaw, "crossBrainWrites")
    }, objectAt(fileRaw, "crossBrainWrites"), mergedBrains),
    writeDefault: stringAt(fileRaw, "writeDefault", stringAt(fileRaw, "defaultBrain", stringAt(matchedProjectRaw, "writeDefault", stringAt(matchedProjectRaw, "defaultBrain", stringAt(defaultRaw, "writeDefault", stringAt(defaultRaw, "defaultBrain", brains[0]?.id ? String(brains[0].id) : "default"))))))
  };
}

function projectBrainsMergedWithProfiles(value: unknown, globalBrains: Record<string, unknown>, matchedProjectRaw: Record<string, unknown>, cfg: BrainstackConfig): Array<Record<string, unknown>> {
  return entriesFromBrainsValue(value).map((entry) => {
    const id = stringAt(entry, "id", "default");
    return mergeTrustedProjectBrain(id, entry, globalBrains, matchedProjectRaw, cfg);
  });
}

async function resolveRepoPath(input: string | null): Promise<string> {
  const absolute = abs(input || ".");
  return await realpath(absolute).catch(() => absolute);
}

async function findProjectBrainstackConfig(repo: string): Promise<string | null> {
  let current = repo;
  while (true) {
    const candidate = join(current, ".brainstack.yaml");
    if (existsSync(candidate)) {
      return candidate;
    }
    const parent = dirname(current);
    if (parent === current) {
      return null;
    }
    current = parent;
  }
}

function safeBrainCloneName(id: string): string {
  const safe = id.replace(/[^A-Za-z0-9_-]+/g, "-").replace(/^-+|-+$/g, "");
  return safe && safe !== "." && safe !== ".." && !safe.startsWith(".") ? safe : `brain-${sha256Hex(id).slice(0, 16)}`;
}

function computedProjectBrainLocalPath(cfg: BrainstackConfig, id: string): string {
  if (id === "default") {
    return clientLocalPathAbs(cfg);
  }
  return join(cfg.paths.stateRoot, "brain-clones", safeBrainCloneName(id));
}

function localCloneForProfileBrain(entry: Record<string, unknown>, cfg: BrainstackConfig, id: string): { localPath: string; untrustedField?: string } {
  const explicit = projectBrainLocalPathSpec(entry);
  if (explicit) {
    const resolvedPath = absWithHome(explicit, cfg.paths.home);
    const trustedPath = stringAt(entry, "__brainstackTrustedLocalPath", "");
    if (trustedPath && resolvedPath === trustedPath) {
      return { localPath: resolvedPath };
    }
    return { localPath: computedProjectBrainLocalPath(cfg, id), untrustedField: "localPath" };
  }
  return { localPath: computedProjectBrainLocalPath(cfg, id) };
}

function projectReadSections(entry: Record<string, unknown>): string[] {
  const direct = arrayAt(entry, "sections").map(String).map((item) => item.trim()).filter(Boolean);
  if (direct.length) {
    return direct;
  }
  const read = entry.read;
  if (read && typeof read === "object" && !Array.isArray(read)) {
    return arrayAt(read as Record<string, unknown>, "sections").map(String).map((item) => item.trim()).filter(Boolean);
  }
  return [];
}

function normalizeProjectBrainEntry(entry: Record<string, unknown>, cfg: BrainstackConfig): ProjectBrain {
  if ("section" in entry && !("sections" in entry)) {
    throw new Error("Invalid project brain config: use `sections`, not `section`.");
  }
  const id = stringAt(entry, "id", "default");
  if (id.startsWith("cross:")) {
    throw new Error(`Invalid project brain config for ${id}: brain ids cannot use reserved prefix cross:.`);
  }
  const label = stringAt(entry, "label", id);
  const requestedWrite = normalizeProjectBrainWrite(entry.write, "propose-only");
  const writeState = effectiveProjectBrainWrite(entry, requestedWrite);
  const rawBaseUrl = projectBrainBaseUrlSpec(entry);
  const rawImportTokenEnv = projectBrainImportTokenEnvSpec(entry);
  const trustedBaseUrl = stringAt(entry, "__brainstackTrustedBaseUrl", id === "default" ? cfg.brain.publicBaseUrl : "");
  const trustedImportTokenEnv = stringAt(entry, "__brainstackTrustedImportTokenEnv", id === "default" ? "BRAIN_IMPORT_TOKEN" : "");
  const baseUrl = rawBaseUrl || trustedBaseUrl;
  const importTokenEnv = rawImportTokenEnv || trustedImportTokenEnv;
  const gitRemote = projectBrainRemoteSpec(entry);
  const trustedGitRemote = stringAt(entry, "__brainstackTrustedGitRemote", "");
  if (gitRemote && gitRemote !== trustedGitRemote && isLocalGitRemote(gitRemote)) {
    throw new Error(`Repo-local project brain config for ${id} cannot set local git remote ${gitRemote}. Define trusted local remotes in ${profilesPath(cfg)}.`);
  }
  const localPathResolution = localCloneForProfileBrain(entry, cfg, id);
  const untrustedConnectionFields = [
    rawBaseUrl && rawBaseUrl !== trustedBaseUrl ? "baseUrl" : "",
    rawImportTokenEnv && rawImportTokenEnv !== trustedImportTokenEnv ? "importTokenEnv" : "",
    gitRemote && gitRemote !== trustedGitRemote ? "gitRemote" : "",
    localPathResolution.untrustedField || ""
  ].filter(Boolean);
  const rawClassification = classifyProjectBrain(id, label, entry.classification);
  const trustedClassification = stringAt(entry, "__brainstackTrustedClassification", "") as ProjectBrain["classification"] | "";
  if ("sections" in entry && !Array.isArray(entry.sections)) {
    throw new Error(`Invalid project brain config for ${id}: sections must be a YAML list such as [wiki, raw].`);
  }
  const sections = projectReadSections(entry);
  validateProjectSections(id, sections);
  return {
    id,
    label,
    classification: applyTrustedClassification(trustedClassification, rawClassification),
    localPath: localPathResolution.localPath,
    gitRemote,
    baseUrl,
    importTokenEnv,
    connectionTrusted: untrustedConnectionFields.length === 0,
    untrustedConnectionFields,
    sections,
    readMode: normalizeProjectReadMode("read" in entry ? entry.read : "mode" in entry ? entry.mode : undefined, "allow"),
    write: writeState.write,
    requestedWrite,
    writeTrusted: writeState.writeTrusted,
    writeDowngraded: writeState.writeDowngraded
  };
}

function projectBrainsFromRaw(raw: Record<string, unknown>, cfg: BrainstackConfig): ProjectBrain[] {
  const brainsValue = raw.brains;
  const entries: Array<Record<string, unknown>> = Array.isArray(brainsValue)
    ? (brainsValue as Array<Record<string, unknown>>)
    : brainsValue && typeof brainsValue === "object"
      ? Object.entries(brainsValue as Record<string, unknown>).map(([id, value]) => ({
          id,
          ...(value && typeof value === "object" ? (value as Record<string, unknown>) : {})
        }))
      : [];
  if (!entries.length) {
    return [
      {
        id: "default",
        label: "default",
        classification: "neutral",
        localPath: clientLocalPathAbs(cfg),
        gitRemote: cfg.client.remoteSsh,
        baseUrl: cfg.brain.publicBaseUrl,
        importTokenEnv: "BRAIN_IMPORT_TOKEN",
        connectionTrusted: true,
        sections: ["wiki", "raw", "proposals"],
        write: "propose-only"
      }
    ];
  }
  return entries.map((entry) => normalizeProjectBrainEntry(entry, cfg));
}

function readAllowRules(cfg: BrainstackConfig): Record<string, Record<string, AllowRule>> {
  const path = allowRulesPath(cfg);
  if (!existsSync(path)) {
    return {};
  }
  try {
    return JSON.parse(readFileSync(path, "utf8")) as Record<string, Record<string, AllowRule>>;
  } catch {
    return {};
  }
}

async function writeAllowRules(cfg: BrainstackConfig, rules: Record<string, unknown>): Promise<void> {
  await ensureDir(dirname(allowRulesPath(cfg)));
  await writeText(allowRulesPath(cfg), `${JSON.stringify(rules, null, 2)}\n`, 0o600);
}

async function saveAllowRule(cfg: BrainstackConfig, repo: string, brain: string, decision: string, sections: string[]): Promise<void> {
  const rules = readAllowRules(cfg);
  rules[repo] = rules[repo] || {};
  rules[repo][brain] = {
    decision,
    sections,
    updated_at: new Date().toISOString()
  };
  await writeAllowRules(cfg, rules);
}

async function promptLine(prompt: string): Promise<string> {
  const rl = createInterface({ input: process.stdin, output: process.stdout, terminal: true });
  try {
    return await rl.question(prompt);
  } finally {
    rl.close();
  }
}

async function consumeOnceAllowRules(cfg: BrainstackConfig, repo: string, brainIds: string[]): Promise<void> {
  const rules = readAllowRules(cfg);
  const repoRules = rules[repo];
  if (!repoRules) {
    return;
  }
  let changed = false;
  for (const brainId of brainIds) {
    if (repoRules[brainId]?.decision === "once") {
      delete repoRules[brainId];
      changed = true;
    }
  }
  if (changed) {
    if (!Object.keys(repoRules).length) {
      delete rules[repo];
    }
    await writeAllowRules(cfg, rules);
  }
}

async function promptForAllowRule(repo: string, brain: ProjectBrain): Promise<"once" | "always" | "deny" | null> {
  const input = (await promptLine(
    [
      `Repo ${repo} requests access to brain ${brain.id}`,
      brain.sections.length ? `sections: ${brain.sections.join(", ")}` : "sections: all configured sections",
      "Allow? [o]nce / [a]lways for this repo / [d]eny: "
    ].join("\n")
  )).trim().toLowerCase();
  if (input === "o" || input === "once") return "once";
  if (input === "a" || input === "always") return "always";
  if (input === "d" || input === "deny" || input === "n" || input === "no") return "deny";
  return null;
}

async function maybePromptForProjectAllows(cfg: BrainstackConfig, resolved: ResolvedProjectContext): Promise<boolean> {
  if (!process.stdin.isTTY || process.env.CI) {
    return false;
  }
  let changed = false;
  for (const brain of resolved.deniedBrains) {
    if (brain.readMode === "never" || !brainNeedsExplicitAllow(brain)) {
      continue;
    }
    const decision = await promptForAllowRule(resolved.repo, brain);
    if (!decision) {
      throw new Error("invalid allow choice; rerun non-interactively with brainctl allow repo --once|--always|--deny");
    }
    await saveAllowRule(cfg, resolved.repo, brain.id, decision, brain.sections);
    changed = true;
  }
  return changed;
}

function brainNeedsExplicitAllow(brain: ProjectBrain): boolean {
  return brain.readMode === "ask-once" || brain.readMode === "ask-always" || brain.classification === "personal";
}

function applyAllowRuleSections(brain: ProjectBrain, rule: AllowRule | undefined): ProjectBrain {
  if (!rule?.sections?.length) {
    return brain;
  }
  const allowed = new Set(rule.sections);
  const sections = (brain.sections.length ? brain.sections : ["wiki", "raw", "proposals"]).filter((section) => allowed.has(section));
  return { ...brain, sections, sectionsRestricted: true };
}

async function resolveProjectContext(cfg: BrainstackConfig, repoInput: string | null): Promise<ResolvedProjectContext> {
  const repo = await resolveRepoPath(repoInput);
  const configPath = await findProjectBrainstackConfig(repo);
  const fileRaw = configPath ? parseSimpleYaml(await readFile(configPath, "utf8")) : {};
  const profilesRaw = existsSync(profilesPath(cfg)) ? parseSimpleYaml(await readFile(profilesPath(cfg), "utf8")) : {};
  const raw = projectRawWithProfiles(fileRaw, profilesRaw, repo, cfg);
  const brains = projectBrainsFromRaw(raw, cfg);
  const writeDefault = stringAt(raw, "writeDefault", stringAt(raw, "defaultBrain", brains[0]?.id || "default"));
  const crossBrainWrites = objectAt(raw, "crossBrainWrites") as Record<string, string>;
  const rules = readAllowRules(cfg)[repo] || {};
  const allowedBrains: ProjectBrain[] = [];
  for (const brain of brains) {
    if (brain.readMode === "never") {
      continue;
    }
    const rule = rules[brain.id];
    if (rule?.decision === "deny") {
      continue;
    }
    if (!brainNeedsExplicitAllow(brain) || rule?.decision === "always" || rule?.decision === "once") {
      allowedBrains.push(applyAllowRuleSections(brain, rule));
    }
  }
  const allowedIds = new Set(allowedBrains.map((brain) => brain.id));
  const deniedBrains = brains.filter((brain) => !allowedIds.has(brain.id));
  return { repo, configPath, brains, allowedBrains, deniedBrains, writeDefault, crossBrainWrites, sessionPath: projectSessionPath(cfg, repo) };
}

async function maybeSyncBrainClone(brain: ProjectBrain): Promise<string> {
  if (!brain.connectionTrusted) {
    return "pending-trust";
  }
  if (!gitExists(brain.localPath)) {
    if (!brain.gitRemote) {
      return "missing";
    }
    await mkdir(dirname(brain.localPath), { recursive: true });
    const result = run(["git", "clone", brain.gitRemote, brain.localPath], { check: false, timeoutMs: 60_000 });
    return result.code === 0 ? "cloned" : `missing: clone failed: ${(result.stderr || result.stdout).trim().slice(0, 160)}`;
  }
  const result = run(["git", "pull", "--ff-only"], { cwd: brain.localPath, check: false, timeoutMs: 20_000 });
  return result.code === 0 ? "synced" : `stale: ${(result.stderr || result.stdout).trim().slice(0, 160)}`;
}

function untrustedBrainConnectionFields(brain: ProjectBrain): string {
  return brain.untrustedConnectionFields?.length ? brain.untrustedConnectionFields.join(",") : "connection";
}

function projectBrainTrustInstruction(cfg: BrainstackConfig, brain: ProjectBrain): string {
  return `define matching connection fields for ${brain.id} in ${profilesPath(cfg)} before Brainstack uses repo-local URL/token/remote/path data`;
}

async function commandContext(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  let resolved = await resolveProjectContext(cfg, flag(args, "repo"));
  if (!hasFlag(args, "json") && await maybePromptForProjectAllows(cfg, resolved)) {
    resolved = await resolveProjectContext(cfg, resolved.repo);
  }
  const sync = args.flags["no-sync"] === undefined;
  const brainRows: Array<ProjectBrain & { allowed: boolean; allowedByPolicy: boolean; status: string; displayState: "allowed" | "blocked" | "pending-trust" }> = [];
  const allowedIds = new Set(resolved.allowedBrains.map((brain) => brain.id));
  const syncStatus: Record<string, string> = {};
  for (const brain of resolved.brains) {
    const allowedByPolicy = allowedIds.has(brain.id);
    const allowed = allowedByPolicy && brain.connectionTrusted;
    const status = !brain.connectionTrusted ? "pending-trust" : sync && allowed ? await maybeSyncBrainClone(brain) : gitExists(brain.localPath) ? "local" : "missing";
    syncStatus[brain.id] = status;
    const displayState = !brain.connectionTrusted ? "pending-trust" : allowedByPolicy ? "allowed" : "blocked";
    brainRows.push({ ...brain, allowed, allowedByPolicy, status, displayState });
  }
  const contextSources = sessionSourcesFromBrains(resolved.allowedBrains.filter((brain) => brain.connectionTrusted), syncStatus);
  await updateProjectSession(resolved, { recent_context_sources: contextSources });
  if (hasFlag(args, "json")) {
    console.log(JSON.stringify({ repo: resolved.repo, configPath: resolved.configPath, writeDefault: resolved.writeDefault, crossBrainWrites: resolved.crossBrainWrites, brains: brainRows }, null, 2));
    return;
  }
  console.log(`# Brainstack context for ${resolved.repo}`);
  console.log("");
  console.log(`config=${resolved.configPath || profilesPath(cfg)}`);
  console.log(`write_default=${resolved.writeDefault}`);
  if (Object.keys(resolved.crossBrainWrites).length) {
    console.log(`cross_brain_writes=${JSON.stringify(resolved.crossBrainWrites)}`);
  }
  console.log("");
  console.log("Available brains:");
  for (const brain of brainRows) {
    const readScope = brain.sections.length ? brain.sections.join(",") : "all local sections";
    const writeLabel = brain.id === resolved.writeDefault ? `${brain.write} default` : brain.write;
    console.log(`- [${brain.displayState}] ${brain.id} (${brain.classification})`);
    console.log(`  path=${brain.localPath}`);
    console.log(`  freshness=${brain.status}`);
    console.log(`  read=${brain.readMode || "allow"} sections=${readScope}`);
    if (brain.writeDowngraded) {
      const defaultNote = brain.id === resolved.writeDefault ? " (default)" : "";
      if (brain.requestedWrite === "true" && brain.write === "propose-only") {
        console.log(`  write=${brain.write} pending profile trust; repo-local write:true ignored until trusted${defaultNote}`);
      } else {
        console.log(`  write=${brain.write} profile trust restricts repo-local write:${brain.requestedWrite || "propose-only"}${defaultNote}`);
      }
    } else {
      console.log(`  write=${writeLabel}`);
    }
    if (!brain.connectionTrusted) {
      console.log(`  connection=pending-trust fields=${untrustedBrainConnectionFields(brain)}`);
      console.log(`  trust with: ${projectBrainTrustInstruction(cfg, brain)}`);
    }
  }
  for (const brain of resolved.deniedBrains) {
    const sectionFlag = brain.sections.length ? ` --sections ${shellSingleQuote(brain.sections.join(","))}` : "";
    if (brain.readMode === "never") {
      console.log(`blocked: ${brain.id} has read=false/never in this project context`);
    } else {
      console.log(`allow with: brainctl allow repo --repo ${shellSingleQuote(resolved.repo)} --brain ${shellSingleQuote(brain.id)}${sectionFlag} --always`);
    }
  }
  console.log("");
  console.log("Use `brainctl search --repo . \"query\"` for labelled retrieval.");
  console.log("Use `brainctl remember --repo . --summary \"...\"` for import/propose writes.");
  console.log("Preserve source labels when reasoning across multiple brains. Pending-trust brains must not be used or searched until trusted. Do not manually clone, pull, or POST unless explicitly instructed.");
}

function searchLocalBrain(brain: ProjectBrain, query: string): Array<{ brain: string; label: string; classification: string; path: string; line: number; text: string }> {
  if (!gitExists(brain.localPath)) {
    return [];
  }
  const configuredScopes = brain.sections.length ? brain.sections : brain.sectionsRestricted ? [] : ["wiki", "raw", "proposals"];
  const scopes = configuredScopes.filter((scope) => existsSync(join(brain.localPath, scope)));
  if (!scopes.length) {
    return [];
  }
  const result = run(["rg", "-n", "-i", "--fixed-strings", "--", query, ...scopes], {
    cwd: brain.localPath,
    check: false,
    timeoutMs: 15_000
  });
  if (result.code !== 0 && result.code !== 1) {
    throw new Error(`search failed for ${brain.id}: ${(result.stderr || result.stdout).trim()}`);
  }
  return result.stdout
    .split(/\r?\n/)
    .filter(Boolean)
    .slice(0, 100)
    .map((line) => {
      const [path = "", lineNo = "0", ...rest] = line.split(":");
      return { brain: brain.id, label: brain.label, classification: brain.classification, path, line: Number(lineNo) || 0, text: rest.join(":").trim() };
    });
}

function titleCaseBrainKey(value: string): string {
  return value ? `${value[0]?.toUpperCase() || ""}${value.slice(1)}` : "";
}

function sourceRecordId(source: unknown): string {
  return source && typeof source === "object" ? String((source as Record<string, unknown>).id || "") : String(source || "");
}

function sourceRecordClassification(source: unknown): ProjectBrain["classification"] {
  if (source && typeof source === "object") {
    const record = source as Record<string, unknown>;
    if (record.classification === "work" || record.classification === "personal" || record.classification === "neutral") {
      return record.classification;
    }
    return classifyProjectBrain(String(record.id || ""), String(record.label || ""));
  }
  return classifyProjectBrain(String(source || ""), String(source || ""));
}

function crossBrainAllowKey(sourceId: string, targetId: string): string {
  return `cross:${sourceId}->${targetId}`;
}

async function promptForCrossBrainRemember(sourceId: string, targetId: string): Promise<"once" | "always" | "cancel" | null> {
  const input = (await promptLine(
    [
      `Recent Brainstack search/context used ${sourceId}, and remember target is ${targetId}.`,
      "Save across this brain boundary? [y]es once / [a]lways for this repo / [n]o: "
    ].join("\n")
  )).trim().toLowerCase();
  if (input === "y" || input === "yes" || input === "once") return "once";
  if (input === "a" || input === "always") return "always";
  if (input === "n" || input === "no" || input === "cancel") return "cancel";
  return null;
}

function crossBrainPolicyRule(
  rules: Record<string, string>,
  source: { id: string; classification: ProjectBrain["classification"] },
  target: ProjectBrain
): string {
  const candidates = crossBrainPolicyCandidates(source, target);
  for (const key of candidates) {
    const value = rules[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim().toLowerCase();
    }
  }
  if (
    (source.classification === "personal" && target.classification === "work") ||
    (source.classification === "work" && target.classification === "personal")
  ) {
    return "ask";
  }
  return "allow";
}

function sessionSourcesFromBrains(brains: ProjectBrain[], syncStatus: Record<string, string> = {}): Array<Record<string, string>> {
  return brains.map((brain) => ({
    id: brain.id,
    label: brain.label,
    classification: brain.classification,
    sync_status: syncStatus[brain.id] || "not-synced"
  }));
}

function sessionSourcesFromSearchResults(
  results: Array<{ brain: string; label: string; classification: string }>,
  syncStatus: Record<string, string>
): Array<Record<string, string>> {
  return [
    ...new Map(
      results.map((item) => [
        item.brain,
        {
          id: item.brain,
          label: item.label,
          classification: item.classification,
          sync_status: syncStatus[item.brain] || "not-synced"
        }
      ])
    ).values()
  ];
}

function readProjectSession(resolved: ResolvedProjectContext): Record<string, unknown> {
  if (!existsSync(resolved.sessionPath)) {
    return {};
  }
  try {
    return JSON.parse(readFileSync(resolved.sessionPath, "utf8")) as Record<string, unknown>;
  } catch {
    return {};
  }
}

async function writeProjectSession(resolved: ResolvedProjectContext, data: Record<string, unknown>): Promise<void> {
  await ensureDir(dirname(resolved.sessionPath));
  await writeText(resolved.sessionPath, `${JSON.stringify({ ...data, repo: resolved.repo, updated_at: new Date().toISOString() }, null, 2)}\n`, 0o600);
}

async function updateProjectSession(resolved: ResolvedProjectContext, data: Record<string, unknown>): Promise<void> {
  await writeProjectSession(resolved, { ...readProjectSession(resolved), ...data });
}

async function commandSearch(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const query = (flag(args, "query") || args.positional.join(" ")).trim();
  if (!query) {
    throw new Error("search requires a query");
  }
  const resolved = await resolveProjectContext(cfg, flag(args, "repo"));
  const pendingTrustBrains = resolved.allowedBrains.filter((brain) => !brain.connectionTrusted);
  const searchableBrains = resolved.allowedBrains.filter((brain) => brain.connectionTrusted);
  if (hasFlag(args, "wait-fresh")) {
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      const pending = searchableBrains.filter((brain) => existsSync(join(brain.localPath, "derived", "search-reindex-needed.json")));
      if (!pending.length) {
        break;
      }
      await Bun.sleep(500);
    }
  }
  const syncStatus: Record<string, string> = {};
  if (args.flags["no-sync"] === undefined) {
    for (const brain of searchableBrains) {
      syncStatus[brain.id] = await maybeSyncBrainClone(brain);
    }
  }
  const results = searchableBrains.flatMap((brain) => searchLocalBrain(brain, query));
  const recentSources = sessionSourcesFromSearchResults(results, syncStatus);
  await updateProjectSession(resolved, { recent_search: query, recent_sources: recentSources });
  for (const brain of pendingTrustBrains) {
    console.warn(`WARN [${brain.id}] skipped pending-trust brain; ${projectBrainTrustInstruction(cfg, brain)}.`);
  }
  for (const brain of searchableBrains) {
    if (existsSync(join(brain.localPath, "derived", "search-reindex-needed.json"))) {
      console.warn(`WARN [${brain.id}] search index refresh pending; local results may be stale.`);
    }
  }
  if (hasFlag(args, "json")) {
    console.log(JSON.stringify({ query, sync_status: syncStatus, skipped_pending_trust: pendingTrustBrains.map((brain) => brain.id), results }, null, 2));
    await consumeOnceAllowRules(cfg, resolved.repo, searchableBrains.map((brain) => brain.id));
    return;
  }
  console.log(results.length ? results.map((item) => `[${item.brain} / ${item.path}:${item.line}] ${item.text}`).join("\n") : "(no local matches)");
  await consumeOnceAllowRules(cfg, resolved.repo, searchableBrains.map((brain) => brain.id));
}

async function commandRemember(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const summary = flag(args, "summary") || flag(args, "text") || args.positional.join(" ");
  if (!summary) {
    throw new Error("remember requires --summary TEXT");
  }
  const resolved = await resolveProjectContext(cfg, flag(args, "repo"));
  const targetId = flag(args, "target") || resolved.writeDefault;
  const target = resolved.allowedBrains.find((brain) => brain.id === targetId);
  if (!target) {
    throw new Error(`target brain ${targetId} is not allowed for ${resolved.repo}. Run brainctl context --repo ${shellSingleQuote(resolved.repo)} for the explicit allow command.`);
  }
  if (target.write === "false") {
    throw new Error(`target brain ${targetId} is read-only in project context`);
  }
  if (!target.connectionTrusted) {
    throw new Error(
      `target brain ${targetId} uses untrusted repo-local connection fields (${untrustedBrainConnectionFields(target)}); ${projectBrainTrustInstruction(cfg, target)}.`
    );
  }
  const session = readProjectSession(resolved);
  const recentSearchSources = Array.isArray(session.recent_sources) ? session.recent_sources : [];
  const recentContextSources = Array.isArray(session.recent_context_sources) ? session.recent_context_sources : [];
  const recentSources = [
    ...new Map(
      [...recentSearchSources, ...recentContextSources].map((source) => [sourceRecordId(source), source])
    ).values()
  ].filter((source) => sourceRecordId(source));
  const crossBrainSources = recentSources
    .map((source) => ({
      id: sourceRecordId(source),
      classification: sourceRecordClassification(source)
    }))
    .filter((source) => source.id && source.id !== target.id);
  const neverSource = crossBrainSources.find((source) => crossBrainPolicyRule(resolved.crossBrainWrites, source, target) === "never");
  if (neverSource) {
    throw new Error(`Refusing cross-brain remember from recent ${neverSource.id} sources into ${target.id}; policy is never.`);
  }
  const askSource = crossBrainSources.find((source) => crossBrainPolicyRule(resolved.crossBrainWrites, source, target) === "ask");
  if (askSource && !hasFlag(args, "confirm-cross-brain")) {
    const repoRules = readAllowRules(cfg)[resolved.repo] || {};
    const storedDecision = repoRules[crossBrainAllowKey(askSource.id, target.id)]?.decision;
    if (storedDecision === "deny") {
      throw new Error(`Refusing cross-brain remember from recent ${askSource.id} sources into ${target.id}; local decision is deny.`);
    }
    if (storedDecision !== "always") {
      if (process.stdin.isTTY && !process.env.CI) {
        const decision = await promptForCrossBrainRemember(askSource.id, target.id);
        if (decision === "always") {
          await saveAllowRule(cfg, resolved.repo, crossBrainAllowKey(askSource.id, target.id), "always", []);
        } else if (decision !== "once") {
          throw new Error(`Refusing cross-brain remember from recent ${askSource.id} sources into ${target.id}; not approved.`);
        }
      } else {
        throw new Error(`Refusing cross-brain remember from recent ${askSource.id} sources into ${target.id} without --confirm-cross-brain; policy=ask. Rerun with --confirm-cross-brain only after verifying the source labels are safe to write into ${target.id}.`);
      }
    }
  }
  if (target.write !== "false" && (!target.baseUrl || !target.importTokenEnv)) {
    throw new Error(`target brain ${targetId} is writable but missing explicit baseUrl/importTokenEnv`);
  }
  await postBrainWriteOrQueue(cfg, target.write === "true" ? "import" : "propose", {
    title: `Remember: ${summary.slice(0, 80)}`,
    text: summary,
    body: summary,
    source_harness: cfg.harness.name,
    source_machine: cfg.machine.name,
    source_type: "remember",
    related_repo: resolved.repo,
    recent_sources: recentSources,
    recent_context_sources: recentContextSources,
    tags: ["remember", target.id]
  }, {
    baseUrl: target.baseUrl,
    importTokenEnv: target.importTokenEnv,
    targetBrainId: target.id
  });
  await consumeOnceAllowRules(cfg, resolved.repo, [target.id]);
}

async function commandAllow(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const sub = args.positional[0] || "";
  if (sub !== "repo") {
    throw new Error("allow supports only: repo");
  }
  const repo = await resolveRepoPath(flag(args, "repo"));
  const brain = flag(args, "brain");
  if (!brain) {
    throw new Error("allow repo requires --brain BRAIN_ID");
  }
  const resolved = await resolveProjectContext(cfg, repo);
  const configuredBrain = resolved.brains.find((item) => item.id === brain);
  if (!configuredBrain) {
    throw new Error(`unknown brain ${brain} for repo ${repo}`);
  }
  const requestedSections = flag(args, "sections") ? flag(args, "sections")!.split(",").map((item) => item.trim()).filter(Boolean) : [];
  if (requestedSections.length) {
    const knownSections = new Set(configuredBrain.sections.length ? configuredBrain.sections : ["wiki", "raw", "proposals"]);
    const unknownSections = requestedSections.filter((section) => !knownSections.has(section));
    if (unknownSections.length) {
      throw new Error(`unknown section(s) for brain ${brain}: ${unknownSections.join(", ")}. Known sections: ${[...knownSections].join(", ")}`);
    }
  }
  const decisionFlags = ["always", "once", "deny"].filter((key) => hasFlag(args, key));
  if (decisionFlags.length !== 1) {
    throw new Error("allow repo requires exactly one of --always, --once, or --deny");
  }
  const decision = hasFlag(args, "deny") ? "deny" : hasFlag(args, "once") ? "once" : "always";
  await saveAllowRule(cfg, repo, brain, decision, requestedSections);
  console.log(`allow_rule=${decision} repo=${repo} brain=${brain}`);
}

async function commandWorkerCache(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const sub = args.positional[0] || "status";
  const cache = readWorkerEnvCache(cfg);
  if (sub === "status") {
    const worker = args.positional[1] || flag(args, "worker");
    const records = Object.values(cache).filter((record) => !worker || record.worker === worker);
    console.log(`worker_cache=${workerEnvCachePath(cfg)}`);
    console.log(records.length ? records.map((record) => `${record.worker} harness=${record.harness} bin=${record.harnessBin} detected=${record.detectedAt}`).join("\n") : "(empty)");
    return;
  }
  if (sub === "clear") {
    if (hasFlag(args, "all")) {
      await rm(workerEnvCachePath(cfg), { force: true });
      console.log("worker_cache=cleared");
      return;
    }
    const worker = args.positional[1] || flag(args, "worker");
    if (!worker) {
      throw new Error("worker-cache clear requires a worker name or --all");
    }
    delete cache[worker];
    await writeWorkerEnvCache(cfg, cache);
    console.log(`worker_cache_cleared=${worker}`);
    return;
  }
  throw new Error("worker-cache subcommand must be status|clear");
}

function brainWriteTimeoutMs(): number {
  const value = Number(process.env.BRAINSTACK_BRAIN_WRITE_TIMEOUT_MS || "15000");
  return Number.isFinite(value) && value > 0 ? value : 15000;
}

async function brainWriteSmokeCheck(cfg: BrainstackConfig): Promise<DoctorCheck> {
  const { baseUrl, token: writeToken } = brainWriteConfig(cfg);
  if (!baseUrl || !writeToken) {
    return check("FAIL", "client", "brain-write-smoke", "BRAIN_BASE_URL or BRAIN_IMPORT_TOKEN is missing", "Set client write env before running --write-smoke.");
  }

  try {
    const response = await fetch(new URL("/api/import", baseUrl).toString(), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${writeToken}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        title: `Brainstack doctor write smoke ${new Date().toISOString()}`,
        text: "Brainstack doctor write smoke. This is a small import/propose path verification artifact.",
        source_harness: cfg.harness.name,
        source_machine: cfg.machine.name,
        source_type: "doctor-smoke",
        tags: ["brainstack", "doctor-smoke"]
      }),
      signal: AbortSignal.timeout(brainWriteTimeoutMs())
    });
    if (!response.ok) {
      return check("FAIL", "client", "brain-write-smoke", `HTTP ${response.status}`);
    }
    const body = (await response.json()) as Record<string, unknown>;
    return check("PASS", "client", "brain-write-smoke", `import accepted artifact_id=${String(body.artifact_id || "unknown")}`);
  } catch (error) {
    return check("FAIL", "client", "brain-write-smoke", error instanceof Error ? error.message : String(error));
  }
}

type OutboxItem = SharedOutboxItem;

function outboxItemKey(
  endpoint: "import" | "propose",
  payload: Record<string, unknown>,
  destination: { brain_id?: string | null; url?: string | null; import_token_env?: string | null } = {}
): string {
  return sharedOutboxItemKey(endpoint, payload, {
    brain_id: destination.brain_id || null,
    url: destination.url || null
  });
}

async function findMatchingOutboxItem(root: string, endpoint: "import" | "propose", idempotencyKey: string): Promise<{ path: string; item: OutboxItem } | null> {
  const scan = await scanOutbox(root);
  if (scan.corrupt.length) {
    throw new Error(`outbox namespace contains ${scan.corrupt.length} corrupt/unsafe item(s); inspect with brainctl outbox list and run purge-corrupt before queueing new writes`);
  }
  for (const entry of scan.items) {
    if (entry.item.endpoint !== endpoint) {
      continue;
    }
    if (entry.idempotencyKey === idempotencyKey || entry.item.idempotency_key === idempotencyKey) {
      return { path: entry.path, item: entry.item };
    }
  }
  return null;
}

async function queueOutboxItem(cfg: BrainstackConfig, item: Omit<OutboxItem, "id" | "created_at" | "retry_count" | "idempotency_key" | "last_error">, error: string): Promise<string> {
  const payload = item.payload ? item.payload : decodeOutboxPayload(item as OutboxItem);
  const destination = {
    brain_id: item.brain_id || null,
    url: item.url || null,
    import_token_env: item.import_token_env || null
  };
  const idempotencyKey = outboxItemKey(item.endpoint, payload, destination);
  const id = `${item.endpoint}-${idempotencyKey.slice(0, 32)}`;
  const root = outboxRootForDestination(cfg, { brainId: item.brain_id, baseUrl: item.url });
  await ensurePrivateOutboxDir(root);
  const stablePath = join(root, `${id}.json`);
  const matched = (await findMatchingOutboxItem(root, item.endpoint, idempotencyKey)) || null;
  const path = matched?.path || stablePath;
  const existing = matched?.item || null;
  const queuedItem = buildOutboxItem(
    {
      ...item,
      payload,
      idempotency_key: idempotencyKey
    },
    error,
    existing
  );
  await writeOutboxItem(path, queuedItem);
  const storage = queuedItem.payload_storage;
  const limits = outboxLimitsFromEnv();
  if (storage && storage.uncompressed_bytes >= limits.softWarnBytes) {
    const compressedNote = storage.encoding === "json-gzip-base64" ? `, compressed to ${storage.stored_bytes} bytes` : "";
    console.warn(`WARN queued large outbox item: ${storage.uncompressed_bytes} bytes${compressedNote}. No content was truncated.`);
  }
  return path;
}

function brainWriteIdempotencyKey(
  endpoint: "import" | "propose",
  payload: Record<string, unknown>,
  destination: { brain_id?: string | null; url?: string | null; import_token_env?: string | null } = {}
): string {
  return outboxItemKey(endpoint, payload, destination);
}

function normalizeOutboxItem(item: OutboxItem): OutboxItem {
  return normalizeSharedOutboxItem(item);
}

function isRetryableBrainWriteStatus(status: number): boolean {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

function maxPendingIdempotencyRetries(): number {
  const raw = Number(process.env.BRAINSTACK_OUTBOX_MAX_425_RETRIES || "");
  return Number.isFinite(raw) && raw > 0 ? Math.trunc(raw) : 12;
}

function idempotencyPendingTerminalError(item: OutboxItem, status: number, responseText: string): string | null {
  if (status !== 425) {
    return null;
  }
  const retryLimit = maxPendingIdempotencyRetries();
  if ((item.retry_count || 0) < retryLimit) {
    return null;
  }
  return `HTTP 425 persisted after ${item.retry_count} flush attempt(s); operator review required before replaying this idempotent write. ${sanitizedHttpError(status, responseText)}`;
}

async function postBrainWriteOrQueue(
  cfg: BrainstackConfig,
  endpoint: "import" | "propose",
  payload: Record<string, unknown>,
  overrides: { baseUrl?: string; importTokenEnv?: string; targetBrainId?: string } = {}
): Promise<void> {
  const writeConfig = brainWriteConfig(cfg);
  const baseUrl = overrides.baseUrl || writeConfig.baseUrl;
  const writeToken = overrides.importTokenEnv ? resolveClientEnvValue(cfg, overrides.importTokenEnv) : writeConfig.token;
  if (!baseUrl || !writeToken) {
    const queued = await queueOutboxItem(
      cfg,
      {
        endpoint,
        url: baseUrl || "",
        brain_id: overrides.targetBrainId || undefined,
        import_token_env: overrides.importTokenEnv || undefined,
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
        "Idempotency-Key": brainWriteIdempotencyKey(endpoint, payload, {
          brain_id: overrides.targetBrainId || null,
          url: baseUrl || null,
          import_token_env: overrides.importTokenEnv || null
        })
      },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(brainWriteTimeoutMs())
    });
    if (!response.ok) {
      const text = await response.text();
      if (isRetryableBrainWriteStatus(response.status)) {
        throw new Error(sanitizedHttpError(response.status, text));
      }
      throw new Error(`brain rejected ${endpoint} with HTTP ${response.status}: ${sanitizedHttpError(response.status, text)}`);
    }
    console.log(`shared-brain ${endpoint} accepted`);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.startsWith("brain rejected ")) {
      throw new Error(message);
    }
    const queued = await queueOutboxItem(
      cfg,
      {
        endpoint,
        url: baseUrl,
        brain_id: overrides.targetBrainId || undefined,
        import_token_env: overrides.importTokenEnv || undefined,
        payload,
        source_machine: String(payload.source_machine || cfg.machine.name),
        source_harness: String(payload.source_harness || cfg.harness.name)
      },
      message
    );
    console.warn(`shared-brain write queued: ${queued}`);
  }
}

async function listOutboxEntries(cfg: BrainstackConfig): Promise<OutboxEntry[]> {
  const roots = await outboxRoots(cfg);
  const entries: OutboxEntry[] = [];
  for (const root of roots) {
    entries.push(...(await scanOutbox(root)).items);
  }
  return entries;
}

async function outboxRoots(cfg: BrainstackConfig): Promise<string[]> {
  const roots = new Set<string>([outboxRoot(cfg)]);
  const parent = outboxParentRoot(cfg);
  if (existsSync(parent)) {
    const parentInfo = await lstat(parent).catch(() => null);
    if (!parentInfo?.isDirectory() || parentInfo.isSymbolicLink()) {
      return [...roots].sort();
    }
    for (const name of await readdir(parent).catch(() => [])) {
      const path = join(parent, name);
      const info = await lstat(path).catch(() => null);
      if (info?.isDirectory() || info?.isSymbolicLink()) {
        roots.add(path);
      }
    }
  }
  return [...roots].sort();
}

async function scanAllOutboxes(cfg: BrainstackConfig): Promise<Array<Awaited<ReturnType<typeof scanOutbox>>>> {
  const scans = [];
  for (const root of await outboxRoots(cfg)) {
    scans.push(await scanOutbox(root));
  }
  return scans;
}

async function purgeConfiguredOutboxParentCorruptEntries(
  cfg: BrainstackConfig,
  scans: Array<Awaited<ReturnType<typeof scanOutbox>>>
): Promise<number> {
  const parent = resolve(outboxParentRoot(cfg));
  let removed = 0;
  for (const entry of scans.flatMap((scan) => scan.corrupt)) {
    if (resolve(entry.path) !== parent || !/symlink|not a directory/i.test(entry.error)) {
      continue;
    }
    const info = await lstat(entry.path).catch(() => null);
    if (!info || info.isDirectory()) {
      continue;
    }
    await rm(entry.path, { force: true });
    removed += 1;
  }
  return removed;
}

async function coalesceOutboxItems(cfg: BrainstackConfig): Promise<void> {
  const seen = new Map<string, OutboxEntry>();
  for (const entry of await listOutboxEntries(cfg)) {
    entry.item = normalizeOutboxItem(entry.item);
    const key = entry.item.idempotency_key;
    const prior = seen.get(key);
    if (!prior) {
      await writeOutboxItem(entry.path, entry.item);
      seen.set(key, entry);
      continue;
    }
    const keep = prior.path.endsWith(`${entry.item.endpoint}-${key.slice(0, 32)}.json`) ? prior : entry;
    const drop = keep === prior ? entry : prior;
    keep.item.retry_count = Math.max(keep.item.retry_count || 0, drop.item.retry_count || 0);
    keep.item.created_at = [keep.item.created_at, drop.item.created_at].filter(Boolean).sort()[0] || keep.item.created_at;
    keep.item.idempotency_key = key;
    keep.item.last_error = keep.item.last_error || drop.item.last_error || null;
    keep.item.terminal_error = keep.item.terminal_error || drop.item.terminal_error || null;
    await writeOutboxItem(keep.path, keep.item);
    await rm(drop.path, { force: true });
    seen.set(key, keep);
  }
}

async function queuedProjectDestinationTrustError(
  cfg: BrainstackConfig,
  item: OutboxItem,
  payload: Record<string, unknown>,
  baseUrl: string
): Promise<string | null> {
  const repo = typeof payload.related_repo === "string" ? payload.related_repo : "";
  if (!repo || !item.brain_id || !item.import_token_env) {
    return null;
  }
  try {
    const resolved = await resolveProjectContext(cfg, repo);
    const brain = resolved.brains.find((candidate) => candidate.id === item.brain_id);
    if (!brain) {
      return `queued project write target ${item.brain_id} is no longer configured for ${repo}`;
    }
    if (!brain.connectionTrusted) {
      return `queued project write target ${item.brain_id} has untrusted repo-local connection fields (${untrustedBrainConnectionFields(brain)}); ${projectBrainTrustInstruction(cfg, brain)}`;
    }
    if (item.endpoint === "import" && brain.write !== "true") {
      return `queued project write target ${item.brain_id} is no longer trusted for direct import; current write mode is ${brain.write}`;
    }
    if (item.endpoint === "propose" && brain.write === "false") {
      return `queued project write target ${item.brain_id} is now read-only`;
    }
    if (brain.baseUrl !== baseUrl || brain.importTokenEnv !== item.import_token_env) {
      return `queued project write target ${item.brain_id} no longer matches trusted connection data for ${repo}`;
    }
    return null;
  } catch (error) {
    return `queued project write target ${item.brain_id} cannot be trusted: ${error instanceof Error ? error.message : String(error)}`;
  }
}

async function flushOutbox(cfg: BrainstackConfig): Promise<{ flushed: number; kept: number; terminalFailures: number; corrupt: number }> {
  let scans = await scanAllOutboxes(cfg);
  const corrupt = scans.reduce((sum, scan) => sum + scan.corrupt.length, 0);
  if (corrupt > 0) {
    const queued = scans.reduce((sum, scan) => sum + scan.items.length, 0);
    const terminalFailures = scans.reduce((sum, scan) => sum + scan.items.filter((entry) => normalizeOutboxItem(entry.item).terminal_error).length, 0);
    return { flushed: 0, kept: queued, terminalFailures, corrupt };
  }
  await coalesceOutboxItems(cfg);
  scans = await scanAllOutboxes(cfg);
  const items = await listOutboxEntries(cfg);
  let flushed = 0;
  let kept = 0;
  let terminalFailures = 0;
  for (const { path, item, payload } of items) {
    const normalizedItem = normalizeOutboxItem(item);
    if (normalizedItem.id !== item.id || normalizedItem.idempotency_key !== item.idempotency_key) {
      await writeOutboxItem(path, normalizedItem);
    }
    if (normalizedItem.terminal_error) {
      terminalFailures += 1;
      continue;
    }
    const baseUrl = normalizedItem.url || brainWriteConfig(cfg).baseUrl;
    const trustError = await queuedProjectDestinationTrustError(cfg, normalizedItem, payload, baseUrl);
    if (trustError) {
      normalizedItem.terminal_error = trustError;
      normalizedItem.last_error = trustError;
      await writeOutboxItem(path, normalizedItem);
      terminalFailures += 1;
      continue;
    }
    const writeToken = normalizedItem.import_token_env ? resolveClientEnvValue(cfg, normalizedItem.import_token_env) : brainWriteConfig(cfg).token;
    if (!baseUrl || !writeToken) {
      kept += 1;
      continue;
    }
    try {
      const response = await fetch(new URL(`/api/${normalizedItem.endpoint}`, baseUrl).toString(), {
        method: "POST",
        headers: {
          Authorization: `Bearer ${writeToken}`,
          "Content-Type": "application/json",
          "Idempotency-Key": normalizedItem.idempotency_key
        },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(brainWriteTimeoutMs())
      });
      if (response.ok) {
        await rm(path, { force: true });
        flushed += 1;
      } else {
        const responseText = await response.text();
        normalizedItem.retry_count += 1;
        normalizedItem.last_error = sanitizedHttpError(response.status, responseText);
        const pendingTerminalError = idempotencyPendingTerminalError(normalizedItem, response.status, responseText);
        if (pendingTerminalError) {
          normalizedItem.terminal_error = pendingTerminalError;
          terminalFailures += 1;
        } else if (!isRetryableBrainWriteStatus(response.status)) {
          normalizedItem.terminal_error = normalizedItem.last_error;
          terminalFailures += 1;
        } else {
          kept += 1;
        }
        await writeOutboxItem(path, normalizedItem);
      }
    } catch (error) {
      normalizedItem.retry_count += 1;
      normalizedItem.last_error = error instanceof Error ? error.message : String(error);
      await writeOutboxItem(path, normalizedItem);
      kept += 1;
    }
  }
  return { flushed, kept, terminalFailures, corrupt };
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
  const scans = await scanAllOutboxes(cfg);
  const items = scans.flatMap((scan) => scan.items);
  const corrupt = scans.flatMap((scan) => scan.corrupt);
  switch (sub) {
    case "status":
      console.log(`outbox=${outboxRoot(cfg)}`);
      console.log(`namespaces=${scans.length}`);
      console.log(`queued=${items.length}`);
      console.log(`corrupt=${corrupt.length}`);
      return;
    case "list":
      console.log(
        items.length || corrupt.length
          ? items
              .map(({ item }) => {
                const normalized = normalizeOutboxItem(item);
                const status = normalized.terminal_error ? "terminal" : normalized.last_error ? "queued-error" : "queued";
                const detail = normalized.terminal_error || normalized.last_error;
                return [
                  normalized.id,
                  normalized.endpoint,
                  `status=${status}`,
                  `retries=${normalized.retry_count}`,
                  normalized.brain_id ? `brain=${normalized.brain_id}` : null,
                  normalized.import_token_env ? `token_env=${normalized.import_token_env}` : null,
                  `source=${normalized.source_machine}/${normalized.source_harness}`,
                  detail ? `error=${detail.replace(/\s+/g, " ").slice(0, 300)}` : null
                ]
                  .filter(Boolean)
                  .join(" ");
              })
              .concat(
                corrupt.map((entry) => `CORRUPT ${entry.name} error=${entry.error.replace(/\s+/g, " ").slice(0, 300)}`)
              )
              .join("\n")
          : "(empty)"
      );
      return;
    case "flush": {
      const result = await flushOutbox(cfg);
      console.log(`flushed=${result.flushed} kept=${result.kept} terminal_failures=${result.terminalFailures} corrupt=${result.corrupt}`);
      if (result.terminalFailures > 0) {
        throw new Error("outbox flush encountered terminal write failures; inspect `brainctl outbox list` and purge or repair affected items");
      }
      if (result.corrupt > 0) {
        throw new Error("outbox flush found corrupt/unsafe entries; inspect `brainctl outbox list` and purge or repair affected items");
      }
      return;
    }
    case "purge":
      if (!hasFlag(args, "yes")) {
        throw new Error("outbox purge is destructive; rerun with --yes");
      }
      {
        const unsafeCorrupt = corrupt.filter((entry) => /symlink|escape|not a directory/i.test(entry.error));
        if (unsafeCorrupt.length) {
          throw new Error("outbox purge found unsafe path entries; run `brainctl outbox purge-corrupt --yes` first so purge does not follow unsafe paths");
        }
      }
      {
        const roots = await outboxRoots(cfg);
        for (const root of roots) {
          await rm(root, { recursive: true, force: true });
        }
        console.log(`purged=${roots.length}`);
      }
      return;
    case "purge-corrupt":
      if (!hasFlag(args, "yes")) {
        throw new Error("outbox purge-corrupt is destructive; rerun with --yes");
      }
      {
        let purged = 0;
        purged += await purgeConfiguredOutboxParentCorruptEntries(cfg, scans);
        for (const scan of scans) {
          purged += await purgeCorruptOutboxEntries(scan);
        }
        console.log(`purged_corrupt=${purged}`);
      }
      return;
    default:
      throw new Error("outbox subcommand must be status|list|flush|purge|purge-corrupt");
  }
}

function summarizeLines(text: string, maxLines = 40): string {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) {
    return "(none)";
  }
  const shown = lines.slice(0, maxLines);
  if (lines.length > maxLines) {
    shown.push(`... ${lines.length - maxLines} more`);
  }
  return shown.join("\n");
}

function updateProbeTimeoutMs(): number {
  const raw = Number(process.env.BRAINSTACK_UPDATE_PROBE_TIMEOUT_MS || "");
  if (Number.isFinite(raw) && raw > 0) {
    return Math.min(raw, 120_000);
  }
  return 20_000;
}

function updateProbeAllowed(command: string): boolean {
  const allowlist = process.env.BRAINSTACK_UPDATE_PROBE_COMMANDS?.trim();
  if (!allowlist) {
    return true;
  }
  return allowlist
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .includes(command);
}

function updateProbe(
  command: string,
  args: string[],
  okCodes: number[] = [0],
  envOverrides: Record<string, string> = {},
  label: string | null = null
): { label: string; code: number; output: string } | null {
  if (!updateProbeAllowed(command)) {
    return null;
  }
  const executable = commandPath(command);
  if (!executable) {
    return null;
  }
  const env = { ...userShellPathEnv(), ...envOverrides };
  const proc = run([executable, ...args], { check: false, env, timeoutMs: updateProbeTimeoutMs() });
  const output = `${proc.stdout}${proc.stderr ? `\n${proc.stderr}` : ""}`.trim();
  const status = okCodes.includes(proc.code) ? "ok" : `exit-${proc.code}`;
  return {
    label: label || `${command} ${args.join(" ")}`.trim(),
    code: proc.code,
    output: `${status}\n${summarizeLines(output)}`
  };
}

function pacmanUpdateProbe(): { label: string; code: number; output: string } | null {
  const probe = updateProbe("pacman", ["-Qu"], [0]);
  if (!probe) {
    return null;
  }
  if (probe.code === 1 && probe.output === "exit-1\n(none)") {
    return {
      ...probe,
      output: "ok\n(none)"
    };
  }
  return probe;
}

function collectOsUpdateProbes(): Array<{ label: string; code: number; output: string }> {
  const probes: Array<{ label: string; code: number; output: string }> = [];
  const add = (probe: { label: string; code: number; output: string } | null) => {
    if (probe) probes.push(probe);
  };

  add(
    updateProbe(
      "brew",
      ["outdated", "--quiet"],
      [0],
      {
        HOMEBREW_NO_AUTO_UPDATE: "1",
        HOMEBREW_NO_ENV_HINTS: "1",
        HOMEBREW_NO_INSTALL_CLEANUP: "1"
      },
      "brew outdated --quiet (HOMEBREW_NO_AUTO_UPDATE=1)"
    )
  );
  add(pacmanUpdateProbe());
  add(updateProbe("checkupdates", []));
  add(updateProbe("apt", ["list", "--upgradable"]));
  // dnf exits 100 when updates are available; that is a successful read-only probe.
  add(updateProbe("dnf", ["--cacheonly", "check-update", "--quiet"], [0, 100]));
  add(updateProbe("zypper", ["--no-refresh", "list-updates"]));
  return probes;
}

function gitUpdateProbe(args: string[]): ReturnType<typeof run> {
  return run(["git", ...args], { cwd: PRODUCT_ROOT, check: false, timeoutMs: updateProbeTimeoutMs() });
}

function harnessVersionSummary(item: DoctorCheck): string {
  if (item.detail.endsWith(" not found in PATH")) {
    return "missing";
  }
  return item.detail.split(";")[0] || item.detail;
}

async function commandUpdates(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const branch = gitUpdateProbe(["rev-parse", "--abbrev-ref", "HEAD"]).stdout.trim() || "unknown";
  const head = gitUpdateProbe(["rev-parse", "HEAD"]).stdout.trim() || "unknown";
  const remoteCandidates = ["origin/main", "refs/remotes/https-main"];
  const remoteRef =
    remoteCandidates.find((candidate) => gitUpdateProbe(["rev-parse", "--verify", candidate]).code === 0) || null;
  const origin = remoteRef ? gitUpdateProbe(["rev-parse", "--verify", remoteRef]).stdout.trim() : "";
  const aheadBehind = remoteRef
    ? gitUpdateProbe(["rev-list", "--left-right", "--count", `HEAD...${remoteRef}`]).stdout.trim()
    : "unknown";
  const compat = [
    harnessCompatibility("codex", "codex", { required: cfg.harness.name === "codex" }),
    harnessCompatibility("claude", "claude", { required: cfg.harness.name === "claude" })
  ];
  const codex = harnessVersionSummary(compat[0]);
  const claude = harnessVersionSummary(compat[1]);
  console.log(`brainstack_branch=${branch}`);
  console.log(`brainstack_head=${head}`);
  console.log(`origin_main=${origin || "unavailable"}`);
  console.log(`remote_main_ref=${remoteRef || "unavailable"}`);
  console.log(`ahead_behind=${aheadBehind}`);
  console.log(`codex=${codex}`);
  console.log(`claude=${claude}`);
  for (const item of compat) {
    console.log(`${item.status} ${item.name}: ${item.detail}`);
    if (item.remediation) console.log(`  remediation: ${item.remediation}`);
  }
  const osProbes = collectOsUpdateProbes();
  console.log("os_update_checks:");
  if (!osProbes.length) {
    console.log("  no supported package manager found");
  } else {
    for (const probe of osProbes) {
      console.log(`  ${probe.label}:`);
      for (const line of probe.output.split(/\r?\n/)) {
        console.log(`    ${line}`);
      }
    }
  }
  console.log("manual_update_commands:");
  console.log("  brainstack: git pull --ff-only && bun install --frozen-lockfile && bun test");
  console.log("  os packages: use your package manager manually after reviewing the read-only check above");
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
  const hasOwnershipManifest = existsSync(managedManifestPath(cfg));
  const manifest = await loadManagedManifest(cfg);
  const bootstrapRoot = join(cfg.paths.configRoot, "client-bootstrap");
  const bootstrapFiles = clientBootstrapFiles(cfg);

  if (hasOwnershipManifest && (scope === "all" || scope === "control") && !dryRun && commandPath("systemctl")) {
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
    if (!hasOwnershipManifest) {
      skipped.push(`${artifact.path} (ownership manifest missing; inferred artifact not removed)`);
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
    sshTrustMode: "pinned",
    sshKnownHostsPath: null,
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
    `brainctl trust-worker --config brainstack.yaml --worker ${worker}`,
    "brainctl upgrade --config brainstack.yaml --profile control",
    "systemctl --user daemon-reload",
    "# if telemux is enabled: systemctl --user restart telemux.service",
    "```",
    "",
    "## Tailscale grants needed if blocked",
    "",
    "```json",
    JSON.stringify(
      [
        {
          src: ["group:brain-admins", "autogroup:admin"],
          dst: [cfg.tailscale.controlTag],
          ip: ["tcp:22", "tcp:443", "icmp:*"]
        },
        {
          src: ["group:brain-admins", "autogroup:admin"],
          dst: [cfg.tailscale.workerTag],
          ip: ["tcp:22", "icmp:*"]
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

async function commandTrustWorker(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const workerName = flag(args, "worker") || args.positional[0];
  if (!workerName) {
    throw new Error("trust-worker requires --worker WORKER_NAME");
  }
  const worker = defaultWorkers(cfg).find((entry) => entry.name === workerName);
  if (!worker) {
    throw new Error(`Unknown worker ${workerName}; run brainctl join-worker first or add it to telemux.workers.`);
  }
  if (worker.transport !== "ssh") {
    throw new Error(`Worker ${worker.name} uses transport=${worker.transport}; trust-worker only manages OpenSSH known_hosts.`);
  }
  const hostOverride = flag(args, "host");
  const hostSource = hostOverride ? { ...worker, sshTarget: hostOverride, sshUser: null } : worker;
  const host = workerSshHost(hostSource);
  const port = workerSshPort(hostSource) || workerSshPort(worker);
  const knownHostsLookup = port ? `[${host}]:${port}` : host;
  const knownHostsPath = workerSshKnownHostsPath(cfg, worker);
  const keyscanArgs = ["-T", "8", ...(port ? ["-p", port] : []), host];
  if (hasFlag(args, "dry-run")) {
    console.log(`would scan host key: ssh-keyscan ${keyscanArgs.join(" ")}`);
    console.log(`would write pinned known_hosts: ${knownHostsPath}`);
    return;
  }
  const sshKeygen = Bun.which("ssh-keygen");
  if (sshKeygen && existsSync(knownHostsPath)) {
    const found = run([sshKeygen, "-F", knownHostsLookup, "-f", knownHostsPath], { check: false });
    if (found.code === 0) {
      console.log(`pinned host key already present for ${knownHostsLookup} in ${knownHostsPath}`);
      return;
    }
  }
  const sshKeyscan = Bun.which("ssh-keyscan");
  if (!sshKeyscan) {
    throw new Error("ssh-keyscan not found; install OpenSSH client tools before pinning worker host keys.");
  }
  const scan = run([sshKeyscan, ...keyscanArgs], { check: false, timeoutMs: 12_000 });
  const scannedLines = scan.stdout
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(isKnownHostLine);
  if (!scannedLines.length) {
    throw new Error(`ssh-keyscan returned no host keys for ${host}; stderr=${scan.stderr.trim() || "(empty)"}`);
  }
  await ensureDir(dirname(knownHostsPath));
  const existing = existsSync(knownHostsPath) ? await readFile(knownHostsPath, "utf8") : "";
  const additions = scannedLines.filter((line) => !existing.split(/\r?\n/).includes(line));
  if (!additions.length) {
    console.log(`pinned host key lines already present for ${host} in ${knownHostsPath}`);
    return;
  }
  const prefix = existing && !existing.endsWith("\n") ? "\n" : "";
  await writeText(knownHostsPath, `${existing}${prefix}${additions.join("\n")}\n`, 0o644);
  console.log(`pinned ${additions.length} host key line(s) for ${host} in ${knownHostsPath}`);
  console.log(`worker ${worker.name} should use sshTrustMode: pinned`);
}

function repoLockPath(cfg: BrainstackConfig, repo: string | null = null): string {
  const selector = repo || "write";
  const root = selector === "serve" || selector === "read" ? cfg.repos.serve : cfg.repos.staging;
  return join(root, ".shared-brain.lock");
}

function operatorLockPath(cfg: BrainstackConfig, args: ParsedArgs): string {
  const explicitPath = flag(args, "path");
  if (!explicitPath) {
    return repoLockPath(cfg, flag(args, "repo"));
  }
  const candidate = resolve(explicitPath);
  const allowedRoots = [resolve(cfg.repos.staging), resolve(cfg.repos.serve)];
  if (!allowedRoots.some((root) => candidate === root || candidate.startsWith(`${root}${sep}`))) {
    throw new Error(`Refusing lock path outside Brainstack repos: ${candidate}`);
  }
  return candidate;
}

function isRepoLockEntry(name: string): boolean {
  return /^owner-[A-Fa-f0-9-]+\.json$/.test(name) || /^release-[A-Fa-f0-9-]+$/.test(name);
}

interface RepoLockInfo {
  path: string;
  exists: boolean;
  ageMs: number | null;
  entries: string[];
  ownerPath: string | null;
  ownerToken: string | null;
  owner: Record<string, unknown> | null;
  pidAlive: "yes" | "no" | "unknown";
  safeToClear: boolean;
  reason: string;
}

function processAlive(pid: unknown): "yes" | "no" | "unknown" {
  if (typeof pid !== "number" || !Number.isInteger(pid) || pid <= 0) {
    return "unknown";
  }
  try {
    process.kill(pid, 0);
    return "yes";
  } catch (error) {
    const code = error && typeof error === "object" && "code" in error ? String((error as { code?: unknown }).code) : "";
    if (code === "ESRCH") return "no";
    if (code === "EPERM") return "yes";
    return "unknown";
  }
}

async function repoLockInfo(lockPath: string): Promise<RepoLockInfo> {
  const linkInfo = await lstat(lockPath).catch(() => null);
  if (!linkInfo) {
    return {
      path: lockPath,
      exists: false,
      ageMs: null,
      entries: [],
      ownerPath: null,
      ownerToken: null,
      owner: null,
      pidAlive: "unknown",
      safeToClear: false,
      reason: "absent"
    };
  }

  const ageMs = Math.max(0, Date.now() - linkInfo.mtime.getTime());
  if (linkInfo.isSymbolicLink() || !linkInfo.isDirectory()) {
    return {
      path: lockPath,
      exists: true,
      ageMs,
      entries: [],
      ownerPath: null,
      ownerToken: null,
      owner: null,
      pidAlive: "unknown",
      safeToClear: false,
      reason: linkInfo.isSymbolicLink() ? "lock path is a symlink; refusing to inspect target" : "lock path is not a directory"
    };
  }

  const lockStat = await stat(lockPath);
  const entries = (await readdir(lockPath).catch(() => [])).sort();
  const ownerEntry = entries.find((entry) => entry.startsWith("owner-") && entry.endsWith(".json")) || null;
  const ownerPath = ownerEntry ? join(lockPath, ownerEntry) : null;
  let owner: Record<string, unknown> | null = null;
  if (ownerPath) {
    try {
      owner = JSON.parse(await readFile(ownerPath, "utf8")) as Record<string, unknown>;
    } catch {
      owner = null;
    }
  }

  const ownerHost = typeof owner?.hostname === "string" ? owner.hostname : null;
  const releaseTokens = entries
    .map((entry) => entry.match(/^release-([A-Fa-f0-9-]+)$/)?.[1] || null)
    .filter((token): token is string => Boolean(token));
  const ownerToken =
    typeof owner?.token === "string" && owner.token.trim()
      ? owner.token.trim()
      : releaseTokens.length === 1
        ? releaseTokens[0]
        : null;
  const localHost = hostname();
  const pidAlive = ownerHost && ownerHost !== localHost ? "unknown" : processAlive(owner?.pid);
  const safeToClear = Boolean(owner && pidAlive === "no");
  const reason = entries.length === 0
    ? "lock directory is empty; likely interrupted acquisition or release"
    : !owner
    ? "owner metadata missing or unreadable"
    : ownerHost && ownerHost !== localHost
      ? `owner host ${ownerHost} is not local host ${localHost}`
      : pidAlive === "no"
        ? "owner process is not running on this host"
        : pidAlive === "yes"
          ? "owner process is still running"
          : "owner process liveness is unknown";

  return {
    path: lockPath,
    exists: true,
    ageMs,
    entries,
    ownerPath,
    ownerToken,
    owner,
    pidAlive,
    safeToClear,
    reason
  };
}

function repoLockMinAgeMs(args: ParsedArgs): number {
  const raw = Number(flag(args, "min-age-ms") || "");
  return Number.isFinite(raw) && raw >= 0 ? Math.trunc(raw) : 60_000;
}

async function appendLockRecoveryLog(cfg: BrainstackConfig, entry: Record<string, unknown>): Promise<void> {
  const logPath = join(cfg.paths.stateRoot, "lock-recovery.jsonl");
  await ensureDir(dirname(logPath));
  await writeFile(
    logPath,
    `${JSON.stringify({ created_at: new Date().toISOString(), ...entry })}\n`,
    { encoding: "utf8", flag: "a", mode: 0o600 }
  );
}

async function commandRepoLock(args: ParsedArgs): Promise<void> {
  const action = args.positional[0] || "status";
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const lockPath = operatorLockPath(cfg, args);
  const lock = await repoLockInfo(lockPath);
  if (action === "status") {
    if (!lock.exists) {
      console.log(`repo-lock=absent path=${lockPath}`);
      return;
    }
    console.log(`repo-lock=present path=${lockPath}`);
    console.log(`age_ms=${lock.ageMs}`);
    console.log(`pid_alive=${lock.pidAlive}`);
    console.log(`safe_to_clear=${lock.safeToClear ? "yes" : "no"}`);
    console.log(`reason=${lock.reason}`);
    console.log(`clear_token=${lock.ownerToken || (lock.entries.length === 0 ? "EMPTY" : "(missing)")}`);
    const entries = lock.entries;
    if (!entries.length) {
      console.log("entries=(empty)");
      return;
    }
    for (const entry of entries) {
      const fullPath = join(lockPath, entry);
      const text = entry.endsWith(".json") ? (await readFile(fullPath, "utf8").catch(() => "")).trim() : "";
      console.log(text ? `${entry}: ${text}` : entry);
    }
    return;
  }
  if (action === "clear") {
    if (!lock.exists) {
      console.log(`repo-lock already absent: ${lockPath}`);
      return;
    }
    if (!hasFlag(args, "yes")) {
      throw new Error(`Refusing to clear repo lock without --yes. First run: brainctl repo-lock status --config <config>`);
    }
    const info = await lstat(lockPath);
    if (!info.isDirectory() || info.isSymbolicLink()) {
      throw new Error(`Refusing to clear non-directory or symlink repo lock: ${lockPath}`);
    }
    const entries = await readdir(lockPath);
    const unsafe = entries.filter((entry) => !isRepoLockEntry(entry));
    if (unsafe.length) {
      throw new Error(`Refusing to clear repo lock with unknown entries: ${unsafe.join(", ")}`);
    }
    const clearToken = flag(args, "token")?.trim() || "";
    if (!clearToken) {
      throw new Error("Refusing to clear repo lock without --token. Copy clear_token from `brainctl repo-lock status` after inspection.");
    }
    const emptyLockRecovery = !lock.ownerToken && entries.length === 0 && clearToken === "EMPTY";
    if (!lock.ownerToken) {
      if (!emptyLockRecovery) {
        throw new Error("Refusing to clear repo lock because owner token is missing; inspect manually instead of using automated cleanup.");
      }
      if (!hasFlag(args, "force")) {
        throw new Error("Refusing to clear empty ownerless repo lock without --force --token EMPTY.");
      }
    }
    if (lock.ownerToken && clearToken !== lock.ownerToken) {
      throw new Error("Refusing to clear repo lock because --token does not match the recorded owner token.");
    }
    const minAgeMs = repoLockMinAgeMs(args);
    const oldEnough = (lock.ageMs ?? 0) >= minAgeMs;
    if ((!lock.safeToClear || !oldEnough) && !hasFlag(args, "force")) {
      throw new Error(
        [
          `Refusing to clear repo lock automatically: ${lock.reason}.`,
          `age_ms=${lock.ageMs ?? "unknown"} min_age_ms=${minAgeMs} safe_to_clear=${lock.safeToClear ? "yes" : "no"}.`,
          "Use --force only after inspecting `brainctl repo-lock status` and confirming no braind write is active."
        ].join(" ")
      );
    }
    await appendLockRecoveryLog(cfg, {
      action: "clear",
      lock_path: lockPath,
      clear_token: clearToken,
      force: hasFlag(args, "force"),
      age_ms: lock.ageMs,
      safe_to_clear: lock.safeToClear,
      reason: lock.reason,
      owner: lock.owner
    });
    if (!emptyLockRecovery) {
      await rm(join(lockPath, `release-${clearToken}`), { force: true });
      await rm(join(lockPath, `owner-${clearToken}.json`), { force: true });
    }
    await rmdir(lockPath);
    console.log(`repo-lock cleared: ${lockPath}${hasFlag(args, "force") ? " force=yes" : ""}`);
    return;
  }
  throw new Error("repo-lock action must be status or clear");
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
    security: {
      posture: "trusted-tailnet",
      bindHost: "127.0.0.1",
      trustedExposure: "none"
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
    case "expose":
      return await commandExpose(args);
    case "import-text":
      return await commandImportText(args);
    case "propose":
      return await commandPropose(args);
    case "context":
      return await commandContext(args);
    case "search":
      return await commandSearch(args);
    case "remember":
      return await commandRemember(args);
    case "allow":
      return await commandAllow(args);
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
    case "trust-worker":
      return await commandTrustWorker(args);
    case "worker-cache":
      return await commandWorkerCache(args);
    case "repo-lock":
    case "locks":
      return await commandRepoLock(args);
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
