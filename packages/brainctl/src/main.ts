#!/usr/bin/env bun
import { accessSync, constants, existsSync, readdirSync, readFileSync, realpathSync, statSync } from "node:fs";
import { appendFile, chmod, cp, lstat, mkdir, mkdtemp, open, readFile, readdir, readlink, realpath, rename, rm, rmdir, stat, symlink, writeFile } from "node:fs/promises";
import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { lookup } from "node:dns/promises";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { isIP } from "node:net";
import { basename, dirname, join, relative, resolve, sep } from "node:path";
import { hostname, tmpdir } from "node:os";
import { createInterface } from "node:readline/promises";
import {
  assertOutboxCapacity,
  buildOutboxItem,
  decodeOutboxPayload,
  ensurePrivateOutboxDir,
  outboxDestinationForKey,
  outboxItemFileBytes,
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
import portableBrainCuratorSkill from "../../skills/brain-curator/SKILL.md" with { type: "text" };
import portableBrainstackSkill from "../../skills/brainstack/SKILL.md" with { type: "text" };
import portableRemoteMachineOpsSkill from "../../skills/remote-machine-ops/SKILL.md" with { type: "text" };
import portableSharedBrainClientSkill from "../../skills/shared-brain-client/SKILL.md" with { type: "text" };
import packageJsonText from "../../../package.json" with { type: "text" };

type Profile = "single-node" | "control" | "worker" | "client-macos" | "private-journal";
const SUPPORTED_PROFILES: Profile[] = ["single-node", "control", "worker", "client-macos"];
type SeedMode = "empty-only" | "missing" | "force";
type HarnessName = "codex" | "claude";
type HookTarget = "codex" | "claude" | "cursor";
type DestroyScope = "control" | "worker" | "client" | "all";
type CheckStatus = "PASS" | "WARN" | "FAIL";
type WorkerSshTrustMode = "pinned" | "accept-new";
type ControlSshTrustMode = WorkerSshTrustMode | "default";
type SecurityPosture = "local" | "trusted-tailnet" | "guarded";
type TrustedExposure = "none" | "tailscale-serve" | "vpn" | "manual";

const CONFIG_SCHEMA_VERSION = 1;
const MIN_BUN_VERSION = "1.3.10";
const BRAINSTACK_SKILL_PACKAGE_KIND = "brainstack.skill_package";
const BRAINSTACK_HOOK_STATUS_MESSAGE = "Brainstack refresh/checkpoint";

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

type BrainctlStatusState = "ok" | "warn" | "fail" | "disabled";

interface BrainctlStatusSection<T = unknown> {
  state: BrainctlStatusState;
  ok: boolean | null;
  available: boolean;
  detail: string;
  data?: T;
  error?: string;
  duration_ms?: number;
}

interface BrainctlStatusReport {
  schema_version: 1;
  product: "brainstack";
  generated_at: string;
  config_path: string;
  profile: Profile | null;
  machine: string | null;
  ok: boolean;
  degraded: boolean;
  sections: Record<string, BrainctlStatusSection>;
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
    telegramRemoteRepo: string;
    telegramVia: string;
  };
  curation: {
    mode: CurationMode;
    autoApply: {
      allowedPaths: string[];
      maxChangedLines: number;
      allowDeletes: boolean;
    };
  };
}

type CurationMode = "manual" | "approval" | "auto";
const CURATION_MODES: CurationMode[] = ["manual", "approval", "auto"];
const DEFAULT_CURATION_ALLOWED_PATHS = ["wiki/Status/**", "wiki/Sources/**"];

interface BrainstackClientInvite {
  schema_version: 1;
  type: typeof CLIENT_INVITE_TYPE;
  created_at: string;
  expires_at: string;
  profile: "client-macos";
  brain: {
    publicBaseUrl: string;
    remoteSsh: string;
  };
  control: {
    sshTarget: string;
    remoteRepo: string;
    sshKnownHosts: string[];
  };
  client: {
    localPath: string;
    envPath: string;
  };
  harness: {
    name: HarnessName;
    bin: string;
  };
  skills?: {
    profile: PortableSkillProfile;
  };
  importToken?: string;
}

const PRODUCT_ROOT = resolve(import.meta.dir, "..", "..", "..");
const TELEGRAM_SEND_DEFAULT_MAX_BYTES = 45 * 1024 * 1024;
const TELEGRAM_SEND_SENSITIVE_FILE_PATTERN =
  /(?:^|[./_-])(?:id_rsa|id_ed25519|id_ecdsa|id_dsa|authorized_keys|known_hosts|token|tokens|secret|secrets|passwd|password|passwords|shadow|keyring|credential|credentials|apikey|api[_-]?keys?|private|cert|certs|certificate|kubeconfig|htpasswd|netrc|npmrc|pgpass|wallet|keystore|otp|totp|2fa)(?:$|[./_-])/i;
const CLIENT_INVITE_PREFIX = "bs1_";
const CLIENT_INVITE_TYPE = "brainstack-client-invite";
const CLIENT_INVITE_MAX_CHARS = 128 * 1024;
const CLIENT_INVITE_KNOWN_HOSTS_MAX_ENTRIES = 128;
const CLIENT_INVITE_KNOWN_HOSTS_MAX_LINE_BYTES = 4096;
const GITHUB_RELEASE_DOWNLOAD_BASE = "https://github.com/Caimeo-com/brainstack/releases/download";
const LATEST_INSTALL_URL = "https://github.com/Caimeo-com/brainstack/releases/latest/download/install.sh";
const BRAINSTACK_PACKAGE_VERSION = (() => {
  try {
    const parsed = JSON.parse(packageJsonText) as { version?: unknown };
    return typeof parsed.version === "string" && parsed.version.trim() ? parsed.version.trim() : "latest";
  } catch {
    return "latest";
  }
})();
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
const PORTABLE_SKILLS = {
  "brain-curator": portableBrainCuratorSkill,
  brainstack: portableBrainstackSkill,
  "remote-machine-ops": portableRemoteMachineOpsSkill,
  "shared-brain-client": portableSharedBrainClientSkill
} as const;
type PortableSkillName = keyof typeof PORTABLE_SKILLS;
type PortableSkillProfile = "client" | "operator" | "control" | "worker";
const PORTABLE_SKILL_PROFILES: Record<PortableSkillProfile, PortableSkillName[]> = {
  client: ["shared-brain-client", "brainstack"],
  operator: ["brainstack", "brain-curator", "remote-machine-ops", "shared-brain-client"],
  control: ["brainstack", "brain-curator", "remote-machine-ops"],
  worker: ["shared-brain-client", "remote-machine-ops"]
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
  "client",
  "curation"
]);

function usage(): string {
  return `Usage:
  brainctl init --profile single-node|control|worker|client-macos --config brainstack.yaml [--dry-run] [--root /tmp/install-root] [--seed-missing|--force-seed] [--import-token-file FILE]
  brainctl provision --profile single-node|control|worker|client-macos [--out brainstack.yaml] [--harness codex|claude] [--harness-bin PATH_OR_NAME] [--enable-telemux] [--enroll-tailscale] [--tailscale-tag tag:brain] [--brain-base-url URL] [--brain-remote SSH_OR_PATH] [--require-harness-sudo] [--test-bot]
  brainctl upgrade --config brainstack.yaml [--profile ...] [--dry-run] [--root /tmp/install-root]
  brainctl apply-runtime --config brainstack.yaml [--profile ...] [--dry-run] [--root /tmp/install-root]
  brainctl doctor --config brainstack.yaml [--profile ...] [--json] [--workers] [--deep] [--write-smoke]
  brainctl status --config brainstack.yaml [--json] [--timeout-ms N]
  brainctl daemon run|once|status|install|uninstall|logs --config brainstack.yaml [--json]
  brainctl updates --config brainstack.yaml [--profile ...]
  brainctl fleet status|update --config brainstack.yaml [machine|--all] [--json] [--dry-run]
  brainctl expose tailscale --config brainstack.yaml --dry-run|--apply
  brainctl backup --config brainstack.yaml [--out DIR] [--pause-telemux]
  brainctl restore --backup DIR_OR_TGZ --target DIR [--apply]
  brainctl render --config brainstack.yaml --profile ... --out DIR
  brainctl bootstrap-client --profile client-macos --config brainstack.yaml --out DIR
  brainctl skills install [--target codex] [--profile client|operator|control|worker] [--skill NAME|--all] [--dir DIR] [--dry-run]
  brainctl skills refresh [--target codex|claude|cursor] [--config brainstack.yaml] [--repo PATH] [--skill NAME] [--dir DIR] [--no-sync] [--force] [--quiet]
  brainctl skills doctor [--target codex|claude|cursor] [--dir DIR] [--check-remote] [--json]
  brainctl skills list
  brainctl import skill <SKILL.md|DIR|URL> --config brainstack.yaml [--title TITLE] [--source-harness HARNESS] [--source-machine MACHINE] [--max-bytes N] [--max-files N] [--allow-private-url] [--allow-ssh-git]
  brainctl import skills [--config brainstack.yaml] [--target codex|claude|cursor|all] [--scan-dir DIR] [--skill NAME] [--apply] [--json]
  brainctl import codex-session <SESSION_ID|JSONL_PATH> [--config brainstack.yaml] [--include-transcript] [--max-bytes N] [--dry-run] [--json]
  brainctl hooks install|status|remove [--target codex|claude|cursor|all] [--config brainstack.yaml] [--brainctl PATH_OR_COMMAND] [--dry-run]
  brainctl hook run --harness codex|claude|cursor --event EVENT [--config brainstack.yaml]
  brainctl invite create --config brainstack.yaml [--import-token-file FILE|--import-token-env ENV] [--ssh-known-hosts-file FILE] [--control-ssh SSH_TARGET] [--control-repo PATH] [--skills-profile client|operator|control|worker|none] [--expires-hours N] [--install-version TAG|latest|--install-url URL] [--allow-insecure-install-url] [--json]
  brainctl enroll --invite bs1_...|--invite-file FILE|- [--config brainstack.yaml] [--skills-profile client|operator|control|worker|none] [--skip-skills] [--skip-init] [--skip-doctor] [--force]
  brainctl join-worker --config brainstack.yaml --worker WORKER_HOST [--ssh-user USER] [--out DIR]
  brainctl trust-worker --config brainstack.yaml --worker WORKER_NAME [--host HOST] [--dry-run]
  brainctl worker-cache status|clear --config brainstack.yaml [worker|--all]
  brainctl repo-lock status|clear --config brainstack.yaml [--repo write|serve] [--path LOCK_DIR] [--yes] [--token LOCK_TOKEN] [--force] [--min-age-ms MS]
  brainctl locks status|clear --config brainstack.yaml [--repo write|serve] [--path LOCK_DIR] [--yes] [--token LOCK_TOKEN] [--force] [--min-age-ms MS]
  brainctl rotate-token --kind import|admin|telegram-placeholder --config brainstack.yaml [--env FILE]
  brainctl telegram send-file --file PATH [--config brainstack.yaml] [--via SSH_TARGET] [--remote-repo PATH] [--caption TEXT] [--context SLUG|--chat-id ID] [--thread-id ID] [--kind document|photo] [--max-bytes N] [--allow-sensitive] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default] [--json]
  brainctl import-text --config brainstack.yaml --title TITLE --text TEXT --source-harness HARNESS --source-machine MACHINE [--source-type note]
  brainctl propose --config brainstack.yaml --title TITLE --body BODY [--target-page wiki/PATH.md] [--content-file FILE] [--base-sha256 HASH|absent] [--risk low|medium|high] [--confidence 0..1] [--curator-run-id ID] [--reason TEXT] [--needs-human] [--source-ids id1,id2] [--project NAME] [--domain NAME] [--scope repo|project|global|machine|harness] [--memory-kind KIND] [--applicability TEXT] [--non-applicability TEXT] [--evidence REF]
  brainctl proposals list|groups|clusters|show|enrich|reprocess|merge-group|approve|reject|apply [...]
  brainctl curator status|run|install [--config brainstack.yaml]
  brainctl context --repo PATH [--config brainstack.yaml] [--json] [--sync|--no-sync]
  brainctl search --repo PATH "query" [--config brainstack.yaml] [--json] [--wait-fresh]
  brainctl remember --repo PATH --summary TEXT [--target BRAIN_ID] [--confirm-cross-brain] [--project NAME] [--domain NAME] [--scope repo|project|global|machine|harness] [--memory-kind KIND] [--applicability TEXT] [--non-applicability TEXT] [--evidence REF]
  brainctl allow repo --repo PATH --brain BRAIN_ID [--sections a,b] --always|--once|--deny
  brainctl outbox status|list|flush|retry|purge|purge-corrupt --config brainstack.yaml [ID|--all] [--yes]
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
    const equalsIndex = item.indexOf("=");
    if (equalsIndex > 2) {
      const key = item.slice(2, equalsIndex);
      const value = item.slice(equalsIndex + 1);
      if (flags[key] === undefined) {
        flags[key] = value;
      } else if (Array.isArray(flags[key])) {
        (flags[key] as string[]).push(value);
      } else {
        flags[key] = [String(flags[key]), value];
      }
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

function flagValues(args: ParsedArgs, key: string): string[] {
  const value = args.flags[key];
  if (value === true) {
    throw new Error(`--${key} requires a value`);
  }
  if (value === undefined) {
    return [];
  }
  return (Array.isArray(value) ? value : [value]).flatMap((entry) => entry.split(",").map((item) => item.trim()).filter(Boolean));
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

async function loadRawConfig(path?: string, profileHint?: string): Promise<Record<string, unknown>> {
  const envPath = process.env.BRAINSTACK_CONFIG?.trim();
  const defaultPath = brainstackDefaultConfigPath();
  const selectedPath = path || envPath || (existsSync(defaultPath) ? defaultPath : "");
  if (!selectedPath) {
    return {};
  }
  const configPath = abs(selectedPath);
  if (!existsSync(configPath)) {
    throw new Error(await missingConfigMessage(configPath, profileHint));
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

async function missingConfigMessage(configPath: string, profileHint?: string): Promise<string> {
  const candidates = await discoverConfigCandidates(dirname(configPath));
  const profile = SUPPORTED_PROFILES.includes(profileHint as Profile) ? (profileHint as Profile) : "control";
  const createLines =
    profile === "client-macos"
      ? [
          "Create one by enrolling with a private invite:",
          `  brainctl enroll --invite-file /path/to/invite.txt --config ${configPath}`,
          "",
          "Or manually provision a client config:",
          `  brainctl provision --profile client-macos --out ${configPath} --harness codex --brain-base-url URL --brain-remote SSH_OR_PATH`
        ]
      : ["Create one with:", `  brainctl provision --profile ${profile} --out ${configPath} --harness codex`];
  return [
    `Brainstack config not found: ${configPath}`,
    "",
    ...createLines,
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
  const proc = run(["bash", "-c", "command -v bun"], { check: false, timeoutMs: 2000 });
  const bunBin = proc.stdout.trim();
  if (proc.code !== 0 || !bunBin) {
    throw new Error("Bun binary not found; install Bun and ensure `command -v bun` works before running brainctl.");
  }
  return bunBin;
}

export async function loadConfig(configPath?: string, profileOverride?: string, rootOverride?: string): Promise<BrainstackConfig> {
  const raw = await loadRawConfig(configPath, profileOverride);
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
  const curation = objectAt(raw, "curation");
  const curationAutoApply = objectAt(curation, "autoApply");
  const curationModeRaw = stringAt(curation, "mode", "approval").trim().toLowerCase();
  if (!CURATION_MODES.includes(curationModeRaw as CurationMode)) {
    throw new Error(`curation.mode must be one of ${CURATION_MODES.join("|")}; found ${curationModeRaw}`);
  }
  const curationAllowedPaths = arrayAt(curationAutoApply, "allowedPaths").map(String).filter(Boolean);
  for (const pattern of curationAllowedPaths) {
    if (!pattern.startsWith("wiki/") || pattern.includes("..") || pattern.includes(",")) {
      throw new Error(`curation.autoApply.allowedPaths entries must be wiki/ glob patterns without '..' or ',': ${pattern}`);
    }
  }
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
      remoteSsh: stringAt(client, "remoteSsh", remoteSsh),
      telegramRemoteRepo: stringAt(client, "telegramRemoteRepo", "~/brainstack"),
      telegramVia: stringAt(client, "telegramVia", "")
    },
    curation: {
      mode: curationModeRaw as CurationMode,
      autoApply: {
        allowedPaths: curationAllowedPaths.length ? curationAllowedPaths : [...DEFAULT_CURATION_ALLOWED_PATHS],
        maxChangedLines: numberAt(curationAutoApply, "maxChangedLines", 40),
        allowDeletes: booleanAt(curationAutoApply, "allowDeletes", false)
      }
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

function isLocalGitRemote(remote: string): boolean {
  const value = remote.trim();
  return value.startsWith("/") || value.startsWith("~/") || value.startsWith("file://");
}

function safeGitProtocolArgs(remote: string): string[] {
  return [
    "-c",
    "protocol.ext.allow=never",
    "-c",
    `protocol.file.allow=${isLocalGitRemote(remote) ? "user" : "never"}`
  ];
}

function safeGitProtocolEnv(remote: string): Record<string, string> {
  return {
    GIT_ALLOW_PROTOCOL: isLocalGitRemote(remote) ? "ssh:https:http:git:file" : "ssh:https:http:git"
  };
}

async function runWithStdinFile(args: string[], filePath: string, options: { cwd?: string; env?: Record<string, string>; maxBytes?: number } = {}) {
  const proc = Bun.spawn(args, {
    cwd: options.cwd || process.cwd(),
    env: { ...process.env, ...(options.env || {}) },
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe"
  });
  const stdoutPromise = new Response(proc.stdout).text();
  const stderrPromise = new Response(proc.stderr).text();
  let inputError: unknown = null;

  try {
    // Open once with O_NOFOLLOW and validate the same descriptor we stream from, so
    // the path cannot be swapped for a symlink or different file between the earlier
    // lstat-based validation and the actual read (TOCTOU).
    const handle = await open(filePath, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const info = await handle.stat();
      if (!info.isFile()) {
        throw new Error(`not a regular file: ${filePath}`);
      }
      if (options.maxBytes !== undefined && info.size > options.maxBytes) {
        throw new Error(`file too large: ${info.size} bytes > ${options.maxBytes}`);
      }
      const buffer = Buffer.alloc(64 * 1024);
      let totalBytes = 0;
      while (true) {
        const { bytesRead } = await handle.read(buffer, 0, buffer.length, -1);
        if (!bytesRead) {
          break;
        }
        totalBytes += bytesRead;
        if (options.maxBytes !== undefined && totalBytes > options.maxBytes) {
          throw new Error(`file grew beyond max bytes while streaming: ${totalBytes} bytes > ${options.maxBytes}`);
        }
        proc.stdin.write(Buffer.from(buffer.subarray(0, bytesRead)));
      }
      await proc.stdin.flush();
    } finally {
      await handle.close().catch(() => undefined);
    }
  } catch (error) {
    inputError = error;
    proc.kill();
  } finally {
    try {
      proc.stdin.end();
    } catch {
      // Process may have exited before stdin was closed.
    }
  }

  const [code, stdout, stderr] = await Promise.all([proc.exited, stdoutPromise, stderrPromise]);
  return {
    code,
    stdout,
    stderr: inputError ? [stderr, inputError instanceof Error ? inputError.message : String(inputError)].filter(Boolean).join("\n") : stderr
  };
}

async function ensureDir(path: string): Promise<void> {
  await mkdir(path, { recursive: true });
}

async function writeText(path: string, text: string, mode?: number): Promise<void> {
  const dir = dirname(path);
  await ensureDir(dir);
  if (mode === undefined) {
    await writeFile(path, text, "utf8");
    return;
  }
  if (existsSync(path)) {
    const existing = await lstat(path);
    if (existing.isSymbolicLink() || !existing.isFile()) {
      throw new Error(`refusing to overwrite non-regular file: ${path}`);
    }
  }
  const tempPath = join(dir, `.${basename(path)}.${process.pid}.${Date.now()}.tmp`);
  const handle = await open(tempPath, "wx", mode);
  try {
    await handle.writeFile(text, "utf8");
    await handle.close();
    await chmod(tempPath, mode);
    await rename(tempPath, path);
  } catch (error) {
    try {
      await handle.close();
    } catch {
      // ignore close failures while preserving the original write error
    }
    await rm(tempPath, { force: true });
    throw error;
  }
}

async function readExistingPrivateText(path: string): Promise<string> {
  if (!existsSync(path)) {
    return "";
  }
  const info = await lstat(path);
  if (info.isSymbolicLink() || !info.isFile()) {
    throw new Error(`refusing to read non-regular private file: ${path}`);
  }
  if ((info.mode & 0o077) !== 0) {
    await chmod(path, 0o600);
  }
  return await readFile(path, "utf8");
}

async function writePrivateText(path: string, text: string): Promise<void> {
  await writeText(path, text, 0o600);
}

async function writePrivateIfMissing(path: string, text: string): Promise<boolean> {
  if (existsSync(path)) {
    const info = await lstat(path);
    if (info.isSymbolicLink() || !info.isFile()) {
      throw new Error(`refusing to use non-regular private file: ${path}`);
    }
    if ((info.mode & 0o077) !== 0) {
      await chmod(path, 0o600);
    }
    return false;
  }
  await writePrivateText(path, text);
  return true;
}

async function writeIfMissing(path: string, text: string, mode?: number): Promise<boolean> {
  if (existsSync(path)) {
    return false;
  }
  if (mode === 0o600) {
    await writePrivateText(path, text);
    return true;
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
  const existing = await readExistingPrivateText(path);
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
    } else if (next.length) {
      next[next.length - 1] = `${key}=${value}`;
    } else {
      next.push(`${key}=${value}`);
    }
    changed = true;
  }
  if (changed) {
    await writePrivateText(path, `${next.join("\n").replace(/\n+$/, "")}\n`);
  }
  return changed;
}

function token(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return `bs_${Buffer.from(bytes).toString("base64url")}`;
}

function encodeClientInvite(invite: BrainstackClientInvite): string {
  return `${CLIENT_INVITE_PREFIX}${Buffer.from(JSON.stringify(invite)).toString("base64url")}`;
}

function httpUrlHostname(input: string, label: string): string {
  let parsed: URL;
  try {
    parsed = new URL(input);
  } catch {
    throw new Error(`${label} must be an http(s) URL with a hostname`);
  }
  if (parsed.protocol !== "https:" && parsed.protocol !== "http:") {
    throw new Error(`${label} must use http or https`);
  }
  if (!parsed.hostname) {
    throw new Error(`${label} must include a hostname`);
  }
  return parsed.hostname;
}

function validateInviteSshTarget(input: string, label: string): string {
  const value = input.trim();
  if (!value) {
    throw new Error(`${label} must not be empty`);
  }
  if (value.startsWith("-") || /[\s/\u0000-\u001f\u007f]/.test(value) || value.endsWith("@")) {
    throw new Error(`${label} must be a safe bare SSH host or user@host target`);
  }
  const match = value.match(/^(?:(?<user>[A-Za-z0-9._~-]+)@)?(?<host>\[[^\]\s/]+\]|[A-Za-z0-9._~-]+)(?::(?<port>[0-9]+))?$/);
  if (!match?.groups?.host) {
    throw new Error(`${label} must be a safe bare SSH host or user@host target`);
  }
  if (match.groups.user?.startsWith("-") || match.groups.host.startsWith("-")) {
    throw new Error(`${label} must not contain option-shaped user or host values`);
  }
  if (match.groups.port !== undefined) {
    const port = Number(match.groups.port);
    if (!Number.isSafeInteger(port) || port < 1 || port > 65535) {
      throw new Error(`${label} port must be between 1 and 65535`);
    }
  }
  return value;
}

function validateInviteGitRemote(input: string, label: string): string {
  const value = input.trim();
  if (!value) {
    throw new Error(`${label} must not be empty`);
  }
  if (value.startsWith("-") || /[\s\u0000-\u001f\u007f]/.test(value) || /^[A-Za-z][A-Za-z0-9+.-]*::/.test(value)) {
    throw new Error(`${label} must be a safe SSH git remote`);
  }
  if (value.startsWith("ssh://")) {
    let parsed: URL;
    try {
      parsed = new URL(value);
    } catch {
      throw new Error(`${label} must be a valid ssh:// git remote`);
    }
    const pathSegments = parsed.pathname.split("/").filter(Boolean);
    if (
      parsed.username.startsWith("-") ||
      parsed.hostname.startsWith("-") ||
      parsed.password ||
      !parsed.hostname ||
      !parsed.pathname ||
      parsed.pathname === "/" ||
      pathSegments.some((segment) => segment === ".." || segment.startsWith("-"))
    ) {
      throw new Error(`${label} must be a safe ssh:// git remote`);
    }
    return value;
  }
  if (/^[A-Za-z][A-Za-z0-9+.-]*:\/\//.test(value) || /^(?:file|ext|fd):/i.test(value)) {
    throw new Error(`${label} must be a safe SSH git remote`);
  }
  const match = value.match(/^(?:(?<user>[A-Za-z0-9._~-]+)@)?(?<host>\[[^\]\s/]+\]|[A-Za-z0-9._~-]+):(?<path>[^\s\u0000-\u001f\u007f]+)$/);
  const pathSegments = match?.groups?.path.split("/").filter(Boolean) || [];
  if (
    !match?.groups?.host ||
    !match.groups.path ||
    match.groups.user?.startsWith("-") ||
    match.groups.host.startsWith("-") ||
    pathSegments.some((segment) => segment === ".." || segment.startsWith("-"))
  ) {
    throw new Error(`${label} must be a safe SSH git remote`);
  }
  return value;
}

function validateInviteRemoteRepoPath(input: string, label: string): string {
  const value = input.trim();
  if (!value) {
    throw new Error(`${label} must not be empty`);
  }
  if (value.startsWith("-") || /[\u0000-\u001f\u007f]/.test(value) || value.split("/").includes("..")) {
    throw new Error(`${label} must be a safe absolute or home-relative remote path`);
  }
  if (!value.startsWith("/") && !value.startsWith("~/")) {
    throw new Error(`${label} must be an absolute or home-relative remote path`);
  }
  return value;
}

function validateInviteClientPath(input: string, label: string): string {
  const value = input.trim();
  if (!value) {
    throw new Error(`${label} must not be empty`);
  }
  if (value.startsWith("-") || /[\u0000-\u001f\u007f]/.test(value) || value.split("/").includes("..")) {
    throw new Error(`${label} must be a safe absolute or home-relative local path`);
  }
  if (!value.startsWith("/") && !value.startsWith("~/")) {
    throw new Error(`${label} must be an absolute or home-relative local path`);
  }
  return value;
}

function inviteBareHost(input: string, label: string): string {
  const value = input.trim();
  if (!value) {
    throw new Error(`${label} must not be empty`);
  }
  if (/^https?:\/\//i.test(value)) {
    return httpUrlHostname(value, label);
  }
  if (value.includes("/")) {
    return httpUrlHostname(`https://${value}`, label);
  }
  if (value.startsWith("[") || /^[^:]+:\d+$/.test(value)) {
    try {
      const parsed = new URL(`ssh://${value}`);
      if (parsed.hostname) {
        return parsed.hostname;
      }
    } catch {
      // Fall through to the raw value and let the SSH-target validator reject if needed.
    }
  }
  return value;
}

interface KnownHostEntry {
  hosts: string[];
  keyType: string;
  key: string;
}

function parseKnownHostEntry(line: string): KnownHostEntry | null {
  const parts = line.trim().split(/\s+/);
  let index = 0;
  if (parts[index]?.startsWith("@")) {
    index += 1;
  }
  if (parts.length - index < 3) {
    return null;
  }
  const hosts = parts[index].split(",").map((host) => host.trim()).filter(Boolean);
  const keyType = parts[index + 1];
  const key = parts[index + 2];
  if (!hosts.length || !keyType || !key) {
    return null;
  }
  return { hosts, keyType, key };
}

function knownHostMatchesLookup(hostPattern: string, lookup: string): boolean {
  if (hostPattern === lookup) {
    return true;
  }
  if (hostPattern.includes("*") || hostPattern.includes("?") || hostPattern.startsWith("|")) {
    return false;
  }
  return false;
}

function knownHostLineMatchesLookup(line: string, lookup: string): boolean {
  const parsed = parseKnownHostEntry(line);
  return Boolean(parsed?.hosts.some((host) => knownHostMatchesLookup(host, lookup)));
}

function controlKnownHostsLookup(sshTarget: string): string {
  return workerSshKnownHostsLookup(telegramControlWorker(sshTarget));
}

function filterKnownHostsForSshTarget(entries: string[], sshTarget: string): string[] {
  const lookup = controlKnownHostsLookup(sshTarget);
  return entries.filter((line) => knownHostLineMatchesLookup(line, lookup));
}

function sanitizeInviteKnownHosts(entries: unknown[], sourceLabel: string): string[] {
  if (entries.length > CLIENT_INVITE_KNOWN_HOSTS_MAX_ENTRIES) {
    throw new Error(`${sourceLabel} contains too many SSH known-host entries`);
  }
  const clean: string[] = [];
  for (const raw of entries) {
    const line = String(raw).trim();
    if (!line) {
      continue;
    }
    if (/[\r\n\u0000]/.test(line)) {
      throw new Error(`${sourceLabel} contains an SSH known-host entry with control characters`);
    }
    if (Buffer.byteLength(line, "utf8") > CLIENT_INVITE_KNOWN_HOSTS_MAX_LINE_BYTES) {
      throw new Error(`${sourceLabel} contains an oversized SSH known-host entry`);
    }
    if (/PRIVATE KEY|BEGIN\s+[A-Z ]*KEY/i.test(line)) {
      throw new Error(`${sourceLabel} contains private-key-looking material`);
    }
    if (!parseKnownHostEntry(line)) {
      throw new Error(`${sourceLabel} contains an invalid SSH known-host entry`);
    }
    clean.push(line);
  }
  return [...new Set(clean)];
}

function decodeClientInvite(input: string): BrainstackClientInvite {
  const trimmed = input.trim();
  if (!trimmed.startsWith(CLIENT_INVITE_PREFIX)) {
    throw new Error(`invite must start with ${CLIENT_INVITE_PREFIX}`);
  }
  if (trimmed.length > CLIENT_INVITE_MAX_CHARS) {
    throw new Error("invite is too large");
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(trimmed.slice(CLIENT_INVITE_PREFIX.length), "base64url").toString("utf8"));
  } catch {
    throw new Error("invite is not valid base64url JSON");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("invite payload must be an object");
  }
  const invite = parsed as Partial<BrainstackClientInvite>;
  if (invite.schema_version !== 1 || invite.type !== CLIENT_INVITE_TYPE || invite.profile !== "client-macos") {
    throw new Error("invite has an unsupported schema, type, or profile");
  }
  if (!invite.brain || !invite.control || !invite.client || !invite.harness) {
    throw new Error("invite is missing required sections");
  }
  if (!invite.brain.publicBaseUrl || !invite.brain.remoteSsh || !invite.control.sshTarget || !invite.control.remoteRepo) {
    throw new Error("invite is missing required connection values");
  }
  const publicBaseUrl = String(invite.brain.publicBaseUrl);
  httpUrlHostname(publicBaseUrl, "invite brain.publicBaseUrl");
  const brainRemoteSsh = validateInviteGitRemote(String(invite.brain.remoteSsh), "invite brain.remoteSsh");
  const controlSshTarget = validateInviteSshTarget(String(invite.control.sshTarget), "invite control.sshTarget");
  const controlRemoteRepo = validateInviteRemoteRepoPath(String(invite.control.remoteRepo), "invite control.remoteRepo");
  const clientLocalPath = validateInviteClientPath(String(invite.client.localPath || "~/shared-brain"), "invite client.localPath");
  const clientEnvPath = validateInviteClientPath(String(invite.client.envPath || "~/.config/shared-brain.env"), "invite client.envPath");
  const expiresAt = Date.parse(String(invite.expires_at || ""));
  if (!Number.isFinite(expiresAt)) {
    throw new Error("invite has an invalid expires_at value");
  }
  if (expiresAt < Date.now()) {
    throw new Error(`invite expired at ${invite.expires_at}`);
  }
  const harnessName = normalizeHarness(invite.harness.name);
  let skillsProfile: PortableSkillProfile | undefined;
  if (invite.skills !== undefined) {
    if (!invite.skills || typeof invite.skills !== "object" || Array.isArray(invite.skills)) {
      throw new Error("invite skills section must be an object");
    }
    const rawSkillsProfile = (invite.skills as { profile?: unknown }).profile;
    if (rawSkillsProfile !== undefined) {
      skillsProfile = normalizePortableSkillProfile(String(rawSkillsProfile));
    }
  }
  return {
    schema_version: 1,
    type: CLIENT_INVITE_TYPE,
    created_at: String(invite.created_at || ""),
    expires_at: String(invite.expires_at),
    profile: "client-macos",
    brain: {
      publicBaseUrl,
      remoteSsh: brainRemoteSsh
    },
    control: {
      sshTarget: controlSshTarget,
      remoteRepo: controlRemoteRepo,
      sshKnownHosts: Array.isArray(invite.control.sshKnownHosts)
        ? sanitizeInviteKnownHosts(invite.control.sshKnownHosts, "invite control.sshKnownHosts")
        : []
    },
    client: {
      localPath: clientLocalPath,
      envPath: clientEnvPath
    },
    harness: {
      name: harnessName,
      bin: String(invite.harness.bin || harnessName)
    },
    skills: skillsProfile ? { profile: skillsProfile } : undefined,
    importToken: typeof invite.importToken === "string" && invite.importToken.trim() ? invite.importToken.trim() : undefined
  };
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

function installedBunVersion(pathOrName = "bun"): string | null {
  const candidate = pathOrName.trim() || "bun";
  const proc = run([candidate, "--version"], { check: false, env: userShellPathEnv(), timeoutMs: updateProbeTimeoutMs() });
  if (proc.code === 0 && proc.stdout.trim()) {
    return proc.stdout.trim();
  }
  if (candidate !== "bun") {
    const fallback = run(["bun", "--version"], { check: false, env: userShellPathEnv(), timeoutMs: updateProbeTimeoutMs() });
    return fallback.code === 0 && fallback.stdout.trim() ? fallback.stdout.trim() : null;
  }
  return null;
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
      "- `POST /api/propose` requires import or admin bearer token; accepts machine proposal fields (`target_page`, `proposed_content`, `base_sha256`, `risk`, `confidence`, `curator_run_id`, `source_ids`) plus memory envelope fields (`project`, `domain`, `scope`, `memory_kind`, `applicability`, `non_applicability`, `evidence_refs`).",
      "- `GET /api/proposals[?status=...]` lists proposals and memory review-group hints.",
      "- `GET /api/proposals/groups[?status=open&min_size=2]` lists deterministic memory review groups.",
      "- `GET /api/proposals/ID` shows one proposal with a diff.",
      "- `POST /api/proposals/ID/approve|reject|apply` requires admin bearer token.",
      "- `GET /api/curator/status` shows curation mode, curator runs, and proposal counts.",
      "- `POST /api/curator/status` requires admin bearer token.",
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
    "proposals/rejected/.gitkeep": "",
    "proposals/superseded/.gitkeep": "",
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
    `BRAIN_CURATION_MODE=${cfg.curation.mode}`,
    `BRAIN_CURATION_ALLOWED_PATHS=${cfg.curation.autoApply.allowedPaths.join(",")}`,
    `BRAIN_CURATION_MAX_CHANGED_LINES=${cfg.curation.autoApply.maxChangedLines}`,
    `BRAIN_CURATION_ALLOW_DELETES=${cfg.curation.autoApply.allowDeletes ? "1" : "0"}`,
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
    "FACTORY_TEXT_COALESCE_RECOVERY_MAX_AGE_MS=300000",
    "FACTORY_PRE_DISPATCH_CLASSIFIER=0",
    "FACTORY_PRE_DISPATCH_CLASSIFIER_MODEL=gpt-5.4-mini",
    "FACTORY_PRE_DISPATCH_CLASSIFIER_REASONING_EFFORT=minimal",
    "FACTORY_PRE_DISPATCH_CLASSIFIER_TIMEOUT_MS=800",
    "FACTORY_PRE_DISPATCH_CLASSIFIER_MAX_CHARS=600",
    "FACTORY_PRE_DISPATCH_CLASSIFIER_CONFIDENCE=0.75",
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
    "FACTORY_PRE_DISPATCH_CLASSIFIER_API_KEY=",
    "BRAIN_IMPORT_TOKEN=",
    "# Optional: enables /proposals Accept/reject and curator status reporting from Telegram.",
    "# FACTORY_BRAIN_ADMIN_TOKEN=",
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

function usesBrainstackDaemon(cfg: BrainstackConfig): boolean {
  return cfg.profile === "client-macos" || cfg.profile === "worker";
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

function brainstackDaemonServiceCommand(cfg: BrainstackConfig, args: ParsedArgs): string {
  const brainctlCommand = currentBrainctlHookCommand(args);
  const configPath = abs(requireFlagValue(args, "config") || brainstackDefaultConfigPath());
  return `${brainctlCommand} daemon run --config ${quoteForBash(configPath)} --target ${quoteForBash(requireFlagValue(args, "target") || "all")}`;
}

function brainstackDaemonSystemdService(cfg: BrainstackConfig, args: ParsedArgs): string {
  return [
    "[Unit]",
    "Description=brainstack local client daemon",
    "After=network-online.target",
    "",
    "[Service]",
    "Type=simple",
    `WorkingDirectory=${cfg.paths.home}`,
    `ExecStart=/bin/sh -lc ${quoteForBash(brainstackDaemonServiceCommand(cfg, args))}`,
    "UMask=0077",
    "Restart=on-failure",
    "RestartSec=10",
    "",
    "[Install]",
    "WantedBy=default.target",
    ""
  ].join("\n");
}

function xmlEscape(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function brainstackDaemonLaunchAgent(cfg: BrainstackConfig, args: ParsedArgs): string {
  const stdout = join(daemonStateDir(cfg), "brainstackd.out.log");
  const stderr = join(daemonStateDir(cfg), "brainstackd.err.log");
  const command = brainstackDaemonServiceCommand(cfg, args);
  return [
    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>",
    "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">",
    "<plist version=\"1.0\">",
    "<dict>",
    "  <key>Label</key>",
    `  <string>${BRAINSTACK_DAEMON_LABEL}</string>`,
    "  <key>ProgramArguments</key>",
    "  <array>",
    "    <string>/bin/sh</string>",
    "    <string>-lc</string>",
    `    <string>${xmlEscape(command)}</string>`,
    "  </array>",
    "  <key>RunAtLoad</key>",
    "  <true/>",
    "  <key>KeepAlive</key>",
    "  <dict>",
    "    <key>SuccessfulExit</key>",
    "    <false/>",
    "  </dict>",
    "  <key>StandardOutPath</key>",
    `  <string>${xmlEscape(stdout)}</string>`,
    "  <key>StandardErrorPath</key>",
    `  <string>${xmlEscape(stderr)}</string>`,
    "  <key>WorkingDirectory</key>",
    `  <string>${xmlEscape(cfg.paths.home)}</string>`,
    "  <key>Umask</key>",
    "  <integer>63</integer>",
    "</dict>",
    "</plist>",
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
    `EnvironmentFile=${join(cfg.paths.configRoot, "braind.secrets.env")}`,
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

function expectedManagedArtifacts(cfg: BrainstackConfig, daemonPlatformOverride?: "launchd" | "systemd"): ManagedArtifact[] {
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

  if (usesBrainstackDaemon(cfg)) {
    const daemonScope = cfg.profile === "worker" ? "worker" : "client";
    const platform = daemonPlatformOverride || (process.platform === "darwin" ? "launchd" : "systemd");
    artifacts.push(
      { path: daemonStateDir(cfg), kind: "dir", scope: daemonScope, reason: "brainstack daemon state root", optional: true },
      { path: daemonServicePath(cfg, platform), kind: "service", scope: daemonScope, reason: "brainstack daemon user service", optional: true }
    );
  }

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

async function writeManagedManifest(cfg: BrainstackConfig, daemonPlatformOverride?: "launchd" | "systemd"): Promise<void> {
  const manifest: ManagedArtifactsManifest = {
    schema_version: 1,
    product: "brainstack",
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
    profile: cfg.profile,
    config_root: cfg.paths.configRoot,
    state_root: cfg.paths.stateRoot,
    artifacts: expectedManagedArtifacts(cfg, daemonPlatformOverride),
    manual_leftovers: manualLeftovers(cfg)
  };
  await writeText(managedManifestPath(cfg), `${JSON.stringify(manifest, null, 2)}\n`, 0o600);
}

async function loadManagedManifest(cfg: BrainstackConfig): Promise<ManagedArtifactsManifest> {
  const path = managedManifestPath(cfg);
  if (existsSync(path)) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(await readFile(path, "utf8"));
    } catch (error) {
      throw new Error(`ownership manifest is corrupt: ${path}; inspect or remove it before destructive operations (${error instanceof Error ? error.message : String(error)})`);
    }
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed) || !Array.isArray((parsed as ManagedArtifactsManifest).artifacts)) {
      throw new Error(`ownership manifest has an unexpected shape: ${path}; inspect or remove it before destructive operations`);
    }
    const manifest = parsed as ManagedArtifactsManifest;
    const validKinds = new Set(["file", "dir", "symlink", "service", "repo", "tailscale-serve"]);
    const validScopes = new Set(["control", "worker", "client", "all"]);
    for (const artifact of manifest.artifacts) {
      if (
        !artifact ||
        typeof artifact !== "object" ||
        Array.isArray(artifact) ||
        typeof artifact.path !== "string" ||
        !artifact.path.trim() ||
        !validKinds.has(String(artifact.kind)) ||
        !validScopes.has(String(artifact.scope))
      ) {
        // Fail before any service-stop or deletion side effect, not midway through.
        throw new Error(`ownership manifest contains a malformed artifact entry: ${path}; inspect or remove it before destructive operations`);
      }
    }
    return manifest;
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

async function installLocalClientBootstrap(cfg: BrainstackConfig, options: { importTokenFile?: string; importToken?: string } = {}): Promise<string[]> {
  if (options.importTokenFile && options.importToken !== undefined) {
    throw new Error("client bootstrap accepts either an import token file or an in-memory import token, not both");
  }
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
    run(["git", "-C", target, ...safeGitProtocolArgs(cfg.client.remoteSsh), "pull", "--ff-only"], {
      env: safeGitProtocolEnv(cfg.client.remoteSsh)
    });
  } else {
    await ensureDir(dirname(target));
    run(["git", ...safeGitProtocolArgs(cfg.client.remoteSsh), "clone", "--", cfg.client.remoteSsh, target], {
      env: safeGitProtocolEnv(cfg.client.remoteSsh)
    });
  }
  touched.push(target);

  const envPath = clientEnvPathAbs(cfg);
  if (await writeIfMissing(envPath, bootstrapFiles["client.env.example"], 0o600)) {
    touched.push(envPath);
  }
  const importToken =
    options.importToken !== undefined
      ? options.importToken.trim()
      : await readEnvSecretOrFile("BRAIN_IMPORT_TOKEN", "BRAIN_IMPORT_TOKEN_FILE", options.importTokenFile);
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

function executableFile(path: string): boolean {
  try {
    accessSync(path, constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

function commonCodexAppCliPath(): string | null {
  const candidates = [
    "/Applications/Codex.app/Contents/Resources/codex",
    process.env.HOME ? join(process.env.HOME, "Applications", "Codex.app", "Contents", "Resources", "codex") : ""
  ].filter(Boolean);
  for (const candidate of candidates) {
    if (executableFile(candidate)) {
      return candidate;
    }
  }
  return null;
}

function resolveEnrollHarnessBin(invite: BrainstackClientInvite): { configBin: string; executable: string } | null {
  const invitedBin = invite.harness.bin || invite.harness.name;
  if (invitedBin.includes("/") && executableFile(absWithHome(invitedBin, process.env.HOME || "."))) {
    const absoluteBin = absWithHome(invitedBin, process.env.HOME || ".");
    return { configBin: absoluteBin, executable: absoluteBin };
  }
  const resolved = commandPath(invitedBin) || commandPath(invite.harness.name);
  if (resolved) {
    return { configBin: invitedBin, executable: resolved };
  }
  if (invite.harness.name === "codex") {
    const codexAppCli = commonCodexAppCliPath();
    if (codexAppCli) {
      return { configBin: codexAppCli, executable: codexAppCli };
    }
  }
  return null;
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
    if (name === "tailscale") return "Install Tailscale from https://tailscale.com/download/mac, or use brew install --cask tailscale if you already use Homebrew.";
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
      remoteSsh: sharedBrainRemote,
      telegramRemoteRepo: flag(args, "telegram-remote-repo") || flag(args, "control-repo") || "~/brainstack",
      telegramVia: flag(args, "telegram-via") || flag(args, "control-ssh") || ""
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
  // Config exposes hostnames, SSH remotes, and topology; never leave it umask-default readable.
  await writeText(out, stringifySimpleYaml(config), 0o600);
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
          "core.hooksPath=/dev/null",
          "-c",
          "commit.gpgsign=false",
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

async function commandInit(args: ParsedArgs, options: { importToken?: string } = {}): Promise<void> {
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
    const touched = await installLocalClientBootstrap(cfg, {
      importTokenFile: requireFlagValue(args, "import-token-file"),
      importToken: options.importToken
    });
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
  const bunVersion = installedBunVersion(cfg.runtime.bunBin);
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
    checks.push(braindPortOwnerCheck(cfg));
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
    const telemuxServicePath = join(cfg.paths.systemdUserRoot, "telemux.service");
    const telemuxServiceText = existsSync(telemuxServicePath) ? readFileSync(telemuxServicePath, "utf8") : "";
    const telemuxHasFactoryAdmin = envHasKey(join(cfg.paths.configRoot, "telemux.secrets.env"), "FACTORY_BRAIN_ADMIN_TOKEN");
    const telemuxHasBrainAdmin = envHasKey(join(cfg.paths.configRoot, "braind.secrets.env"), "BRAIN_ADMIN_TOKEN") && telemuxServiceText.includes("braind.secrets.env");
    checks.push(
      telemuxHasFactoryAdmin || telemuxHasBrainAdmin
        ? check("PASS", "secrets", "telemux-admin-token", telemuxHasFactoryAdmin ? "FACTORY_BRAIN_ADMIN_TOKEN present" : "BRAIN_ADMIN_TOKEN loaded from braind.secrets.env")
        : check("WARN", "secrets", "telemux-admin-token", "missing; curator status reporting and Telegram proposal approval are disabled", `Run brainctl apply-runtime --config ${requireFlagValue(args, "config") || brainstackDefaultConfigPath()} and restart telemux, or set FACTORY_BRAIN_ADMIN_TOKEN in telemux.secrets.env.`)
    );
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
    const status = run([tailscaleBin, "status", "--json"], { check: false, timeoutMs: updateProbeTimeoutMs() });
    checks.push(status.code === 0 ? check("PASS", "tailscale", "status", "tailscale status --json succeeded") : check("WARN", "tailscale", "status", (status.stderr || status.stdout).trim()));
    const serve = run([tailscaleBin, "serve", "status"], { check: false, timeoutMs: updateProbeTimeoutMs() });
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
    const daemonService = await daemonServiceStatus(cfg, args);
    const daemonStatus = await readDaemonStatus(cfg);
    checks.push(
      daemonService.installed
        ? check("PASS", "daemon", "service-installed", `${daemonService.platform} ${daemonService.path}`)
        : check("WARN", "daemon", "service-installed", `${daemonService.platform} service missing`, `Run brainctl daemon install --config ${requireFlagValue(args, "config") || brainstackDefaultConfigPath()}`)
    );
    checks.push(
      daemonService.running === true
        ? check("PASS", "daemon", "service-running", daemonService.detail)
        : daemonService.running === false
          ? check("WARN", "daemon", "service-running", daemonService.detail, "Start the daemon service or run brainctl daemon once for a foreground health pass.")
          : check("WARN", "daemon", "service-running", daemonService.detail)
    );
    if (daemonStatus) {
      const ageMs = Date.now() - Date.parse(daemonStatus.updated_at || "");
      checks.push(
        daemonStatus.ok
          ? check("PASS", "daemon", "last-run", `updated=${daemonStatus.updated_at} age_ms=${Number.isFinite(ageMs) ? ageMs : "unknown"} iteration=${daemonStatus.iteration}`)
          : check("WARN", "daemon", "last-run", `degraded: ${daemonStatus.errors.join("; ") || daemonStatus.updated_at}`, "Inspect brainctl daemon status --json and daemon logs.")
      );
      checks.push(
        daemonStatus.repo.exists && daemonStatus.repo.clean !== false
          ? check("PASS", "daemon", "shared-brain-freshness", `head=${daemonStatus.repo.head || "unknown"} last_pull=${daemonStatus.repo.last_pull_at || "never"}`)
          : check("WARN", "daemon", "shared-brain-freshness", `repo=${daemonStatus.repo.path} clean=${daemonStatus.repo.clean ?? "unknown"}`, "Resolve local clone state so the daemon can pull with --ff-only.")
      );
    } else {
      checks.push(check("WARN", "daemon", "last-run", `missing ${daemonStatusPath(cfg)}`, "Run brainctl daemon once --config <config> or start the installed daemon."));
    }
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

function listeningTcpPids(port: number): number[] {
  const pids = new Set<number>();
  const lsof = Bun.which("lsof");
  if (lsof) {
    const result = run([lsof, "-nP", `-iTCP:${port}`, "-sTCP:LISTEN", "-t"], { check: false, timeoutMs: 2000 });
    for (const line of result.stdout.split(/\r?\n/)) {
      const pid = Number(line.trim());
      if (Number.isSafeInteger(pid) && pid > 0) {
        pids.add(pid);
      }
    }
  }
  const ss = Bun.which("ss");
  if (!pids.size && ss) {
    const result = run([ss, "-ltnp", `sport = :${port}`], { check: false, timeoutMs: 2000 });
    for (const match of result.stdout.matchAll(/pid=(\d+)/g)) {
      const pid = Number(match[1]);
      if (Number.isSafeInteger(pid) && pid > 0) {
        pids.add(pid);
      }
    }
  }
  return [...pids].sort((left, right) => left - right);
}

function systemdUserMainPid(serviceName: string): number | null {
  if (!commandPath("systemctl")) {
    return null;
  }
  const result = run(["systemctl", "--user", "show", serviceName, "--property=MainPID", "--value"], { check: false, timeoutMs: 2000 });
  const pid = Number(result.stdout.trim());
  return result.code === 0 && Number.isSafeInteger(pid) && pid > 0 ? pid : null;
}

function systemdUserServiceActive(serviceName: string): boolean | null {
  if (!commandPath("systemctl")) {
    return null;
  }
  const result = run(["systemctl", "--user", "is-active", "--quiet", serviceName], { check: false, timeoutMs: 2000 });
  return result.code === 0;
}

function braindPortOwnerCheck(cfg: BrainstackConfig): DoctorCheck {
  if (process.platform !== "linux" || !commandPath("systemctl")) {
    return check("PASS", "health", "braind-port-owner", "systemd unavailable; skipped managed port-owner comparison");
  }
  const pids = listeningTcpPids(cfg.brain.port);
  const active = systemdUserServiceActive("braind.service");
  const mainPid = systemdUserMainPid("braind.service");
  const ownerDetails = pids.map((pid) => `${pid}:${processCommandLine(pid) || "command unavailable"}`).join("; ");
  if (!pids.length) {
    return check("PASS", "health", "braind-port-owner", `no listener on ${cfg.brain.port} outside health checks`);
  }
  if (active === true && mainPid && pids.includes(mainPid)) {
    return check("PASS", "health", "braind-port-owner", `systemd owns port ${cfg.brain.port} pid=${mainPid}`);
  }
  const unmanaged = active === true && mainPid ? `systemd MainPID=${mainPid}, listener(s)=${ownerDetails}` : `listener(s)=${ownerDetails}`;
  if (active === null) {
    return check("WARN", "health", "braind-port-owner", `cannot compare listener to systemd; ${unmanaged}`);
  }
  return check(
    "FAIL",
    "health",
    "braind-port-owner",
    `unmanaged process owns brain port ${cfg.brain.port}: ${unmanaged}`,
    "Stop the orphan listener, then restart the managed service with `systemctl --user restart braind.service`."
  );
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

function statusTimeoutMs(args: ParsedArgs): number {
  const raw = requireFlagValue(args, "timeout-ms") || process.env.BRAINSTACK_STATUS_TIMEOUT_MS || "";
  if (!raw) {
    return 1500;
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error("--timeout-ms must be a positive integer");
  }
  return Math.min(Math.max(parsed, 100), 30_000);
}

function sanitizeStatusError(error: unknown): string {
  return (error instanceof Error ? error.message : String(error)).replace(/\s+/g, " ").slice(0, 500);
}

function statusSection<T>(
  state: BrainctlStatusState,
  detail: string,
  data?: T,
  options: { available?: boolean; error?: string; durationMs?: number } = {}
): BrainctlStatusSection<T> {
  const available = options.available ?? (state === "ok" || state === "warn");
  return {
    state,
    ok: state === "disabled" ? null : state === "ok",
    available,
    detail,
    ...(data === undefined ? {} : { data }),
    ...(options.error ? { error: options.error } : {}),
    ...(options.durationMs === undefined ? {} : { duration_ms: options.durationMs })
  };
}

async function collectStatusSection<T>(
  collector: () => Promise<BrainctlStatusSection<T>>,
  timeoutMs: number,
  fallbackState: BrainctlStatusState = "warn"
): Promise<BrainctlStatusSection<T>> {
  const started = Date.now();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const timed = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`timed out after ${timeoutMs}ms`)), timeoutMs);
    });
    const section = await Promise.race([collector(), timed]);
    return { ...section, duration_ms: Date.now() - started };
  } catch (error) {
    return statusSection(fallbackState, "unavailable", undefined, {
      available: false,
      error: sanitizeStatusError(error),
      durationMs: Date.now() - started
    });
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

function finalizeBrainctlStatus(report: BrainctlStatusReport): BrainctlStatusReport {
  const sections = Object.values(report.sections);
  report.ok = !sections.some((section) => section.state === "fail");
  report.degraded = sections.some((section) => section.state === "warn" || section.state === "fail");
  return report;
}

function safeBrainApiBaseUrl(cfg: BrainstackConfig): { baseUrl: string; error?: string } {
  try {
    return { baseUrl: brainApiBaseUrl(cfg) };
  } catch (error) {
    return { baseUrl: "", error: sanitizeStatusError(error) };
  }
}

function safeBrainWriteStatus(cfg: BrainstackConfig): { baseUrlConfigured: boolean; importTokenPresent: boolean; error?: string } {
  try {
    const write = brainWriteConfig(cfg);
    return { baseUrlConfigured: Boolean(write.baseUrl), importTokenPresent: Boolean(write.token) };
  } catch (error) {
    return { baseUrlConfigured: false, importTokenPresent: false, error: sanitizeStatusError(error) };
  }
}

function gitStatusProbe(args: string[], cwd: string, timeoutMs: number): { ok: boolean; output: string; code: number; timedOut: boolean } {
  const result = run(["git", ...args], { cwd, check: false, timeoutMs });
  return {
    ok: result.code === 0,
    output: (result.stdout || result.stderr).trim(),
    code: result.code,
    timedOut: result.timedOut
  };
}

async function localGitSnapshot(path: string, timeoutMs: number): Promise<Record<string, unknown>> {
  const info = await lstat(path).catch(() => null);
  if (!info) {
    return { path, exists: false, is_git: false, detail: "missing" };
  }
  if (info.isSymbolicLink()) {
    return { path, exists: true, is_git: false, detail: "refusing symlink" };
  }
  if (!info.isDirectory()) {
    return { path, exists: true, is_git: false, detail: "not a directory" };
  }
  if (!gitExists(path)) {
    return { path, exists: true, is_git: false, detail: "not a git checkout" };
  }
  const branch = gitStatusProbe(["rev-parse", "--abbrev-ref", "HEAD"], path, timeoutMs);
  const head = gitStatusProbe(["rev-parse", "HEAD"], path, timeoutMs);
  const dirty = gitStatusProbe(["status", "--porcelain"], path, timeoutMs);
  return {
    path,
    exists: true,
    is_git: true,
    branch: branch.ok ? branch.output || null : null,
    head: head.ok ? head.output || null : null,
    clean: dirty.ok ? dirty.output.length === 0 : null,
    dirty_count: dirty.ok && dirty.output ? dirty.output.split(/\r?\n/).filter(Boolean).length : 0,
    errors: [branch.ok ? null : `branch: ${branch.output || `exit ${branch.code}`}`, head.ok ? null : `head: ${head.output || `exit ${head.code}`}`, dirty.ok ? null : `status: ${dirty.output || `exit ${dirty.code}`}`].filter(Boolean)
  };
}

async function collectSharedBrainStatus(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
  const snapshots: Record<string, unknown>[] = [];
  if (cfg.profile === "client-macos" || cfg.profile === "worker") {
    snapshots.push(await localGitSnapshot(clientLocalPathAbs(cfg), timeoutMs));
  }
  if (runsBraind(cfg)) {
    snapshots.push(await localGitSnapshot(cfg.repos.serve, timeoutMs));
    snapshots.push(await localGitSnapshot(cfg.repos.staging, timeoutMs));
  }
  if (!snapshots.length) {
    return statusSection("disabled", "no shared-brain checkout role for this profile", { snapshots });
  }
  const unhealthy = snapshots.filter((snapshot) => snapshot.exists !== true || snapshot.is_git !== true || snapshot.clean === false || Array.isArray(snapshot.errors) && snapshot.errors.length > 0);
  const state: BrainctlStatusState = unhealthy.length ? "warn" : "ok";
  return statusSection(state, `checkouts=${snapshots.length} unhealthy=${unhealthy.length}`, { snapshots });
}

async function collectOutboxStatusSection(cfg: BrainstackConfig): Promise<BrainctlStatusSection> {
  const scans = await scanAllOutboxes(cfg);
  const items = scans.flatMap((scan) => scan.items.map((entry) => ({ ...entry, item: normalizeOutboxItem(entry.item) })));
  const corrupt = scans.flatMap((scan) => scan.corrupt);
  const terminal = items.filter((entry) => entry.item.terminal_error).length;
  const state: BrainctlStatusState = corrupt.length ? "fail" : terminal ? "warn" : "ok";
  return statusSection(state, `queued=${items.length} terminal=${terminal} corrupt=${corrupt.length}`, {
    roots: scans.map((scan) => scan.root),
    queued: items.length,
    terminal,
    corrupt: corrupt.length
  });
}

async function collectHooksStatusSection(cfg: BrainstackConfig): Promise<BrainctlStatusSection> {
  const targets: HookTarget[] = ["codex", "claude", "cursor"];
  const hooks: Array<Record<string, unknown>> = [];
  for (const target of targets) {
    const path = hookConfigPath(target);
    let count = 0;
    let error: string | null = null;
    try {
      const raw = await readJsonObject(path).catch(() => ({}));
      count = target === "cursor" ? countCursorManagedHooks(raw, target) : countCodexStyleManagedHooks(raw, target);
    } catch (hookError) {
      error = sanitizeStatusError(hookError);
    }
    hooks.push({ target, path, hooks: count, installed: count > 0, ...(error ? { error } : {}) });
  }
  const selected = hooks.find((entry) => entry.target === cfg.harness.name);
  const selectedInstalled = Boolean(selected?.installed);
  return statusSection(selectedInstalled ? "ok" : "warn", `${cfg.harness.name} hooks ${selectedInstalled ? "installed" : "missing"}`, {
    selected_target: cfg.harness.name,
    hooks
  });
}

async function collectSkillsStatusSection(cfg: BrainstackConfig): Promise<BrainctlStatusSection> {
  const targets: HookTarget[] = ["codex", "claude", "cursor"];
  const roots: Array<Record<string, unknown>> = [];
  for (const target of targets) {
    try {
      const root = skillInstallRootForTarget(target, { command: "status", positional: [], flags: {} });
      const dirs = await skillDirsForDoctor(root);
      roots.push({ target, root, skills: dirs.length, installed: dirs.map((dir) => basename(dir)).slice(0, 100) });
    } catch (error) {
      roots.push({ target, root: null, skills: 0, error: sanitizeStatusError(error) });
    }
  }
  const selected = roots.find((entry) => entry.target === cfg.harness.name);
  const count = Number(selected?.skills || 0);
  return statusSection(count > 0 ? "ok" : "warn", `${cfg.harness.name} skills=${count}`, {
    selected_target: cfg.harness.name,
    roots
  });
}

async function statusFetchJson(baseUrl: string, path: string, timeoutMs: number): Promise<Record<string, unknown>> {
  const response = await fetch(new URL(path, baseUrl).toString(), {
    signal: AbortSignal.timeout(timeoutMs)
  });
  const text = await response.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = null;
  }
  return {
    ok: response.ok,
    http_status: response.status,
    body,
    content_type: response.headers.get("content-type") || null,
    ...(body === null && text ? { text: text.slice(0, 500) } : {})
  };
}

async function collectBrainApiStatusSection(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
  const base = safeBrainApiBaseUrl(cfg);
  if (!base.baseUrl) {
    return statusSection("disabled", "brain API base URL unavailable", { base_url_configured: false }, { available: false, error: base.error });
  }
  const [health, ready] = await Promise.allSettled([
    statusFetchJson(base.baseUrl, "/healthz", timeoutMs),
    statusFetchJson(base.baseUrl, "/readyz", timeoutMs)
  ]);
  const healthData = health.status === "fulfilled" ? health.value : { ok: false, error: sanitizeStatusError(health.reason) };
  const readyData = ready.status === "fulfilled" ? ready.value : { ok: false, error: sanitizeStatusError(ready.reason) };
  const healthOk = Boolean(healthData.ok);
  const readyOk = Boolean(readyData.ok);
  const state: BrainctlStatusState = healthOk && readyOk ? "ok" : "warn";
  return statusSection(state, `base_url=${base.baseUrl} health=${healthOk ? "ok" : "unavailable"} ready=${readyOk ? "ok" : "unavailable"}`, {
    base_url: base.baseUrl,
    health: healthData,
    ready: readyData
  }, { available: healthOk || readyOk });
}

async function collectCuratorStatusSection(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
  const base = safeBrainApiBaseUrl(cfg);
  if (!base.baseUrl) {
    return statusSection("disabled", "curator status unavailable without brain API base URL", undefined, { available: false, error: base.error });
  }
  const response = await statusFetchJson(base.baseUrl, "/api/curator/status", timeoutMs);
  if (!response.ok) {
    return statusSection("warn", `curator status unavailable HTTP ${response.http_status}`, response, { available: false });
  }
  const body = response.body && typeof response.body === "object" && !Array.isArray(response.body) ? (response.body as Record<string, unknown>) : {};
  const curator = body.curator && typeof body.curator === "object" && !Array.isArray(body.curator) ? (body.curator as Record<string, unknown>) : {};
  const counts = body.proposal_counts && typeof body.proposal_counts === "object" && !Array.isArray(body.proposal_counts) ? (body.proposal_counts as Record<string, unknown>) : {};
  const open = Number(counts.pending || 0) + Number(counts.approved || 0) + Number(counts["needs-human"] || 0);
  const installed = Boolean(curator.installed);
  return statusSection("ok", `mode=${String(body.mode || cfg.curation.mode)} installed=${installed} open_proposals=${open}`, {
    mode: body.mode || cfg.curation.mode,
    curator,
    proposal_counts: counts,
    open_proposals: open
  });
}

async function collectProposalsStatusSection(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
  const base = safeBrainApiBaseUrl(cfg);
  if (!base.baseUrl) {
    return statusSection("disabled", "proposal status unavailable without brain API base URL", undefined, { available: false, error: base.error });
  }
  const response = await statusFetchJson(base.baseUrl, "/api/proposals?status=open", timeoutMs);
  if (!response.ok) {
    return statusSection("warn", `open proposal list unavailable HTTP ${response.http_status}`, response, { available: false });
  }
  const body = response.body && typeof response.body === "object" && !Array.isArray(response.body) ? (response.body as Record<string, unknown>) : {};
  const proposals = Array.isArray(body.proposals) ? (body.proposals as Array<Record<string, unknown>>) : [];
  const reviewGroups = proposalReviewGroupsFromResult(body);
  const byStatus: Record<string, number> = {};
  for (const proposal of proposals) {
    const proposalStatus = String(proposal.status || "unknown");
    byStatus[proposalStatus] = (byStatus[proposalStatus] || 0) + 1;
  }
  return statusSection("ok", `open_proposals=${proposals.length}${reviewGroups.length ? ` review_groups=${reviewGroups.length}` : ""}`, {
    count: proposals.length,
    by_status: byStatus,
    review_groups: reviewGroups.slice(0, 10),
    proposals: proposals.slice(0, 25)
  });
}

async function collectTelemuxStatusSection(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
  if (!cfg.telemux.enabled) {
    return statusSection("disabled", "telemux disabled in config", { enabled: false });
  }
  const baseUrl = `http://${cfg.telemux.dashboardHost}:${cfg.telemux.dashboardPort}`;
  const response = await statusFetchJson(baseUrl, "/healthz", timeoutMs);
  if (!response.ok) {
    return statusSection("warn", `telemux healthz unavailable HTTP ${response.http_status}`, response, { available: false });
  }
  const body = response.body && typeof response.body === "object" && !Array.isArray(response.body) ? (response.body as Record<string, unknown>) : {};
  const due = Number(body.cronDue || 0);
  const pending = Number(body.cronPending || 0);
  const workerDegraded = Number(body.workerDegraded || 0);
  const state: BrainctlStatusState = due > 0 || pending > 0 || workerDegraded > 0 ? "warn" : "ok";
  return statusSection(state, `crons=${String(body.crons ?? "unknown")} due=${due} pending=${pending} worker_degraded=${workerDegraded}`, {
    base_url: baseUrl,
    healthz: body
  });
}

async function collectDaemonStatusSection(cfg: BrainstackConfig, args: ParsedArgs, timeoutMs: number): Promise<BrainctlStatusSection> {
  if (!usesBrainstackDaemon(cfg)) {
    return statusSection("disabled", "brainstackd not used by this profile", { profile: cfg.profile });
  }
  const status = await readDaemonStatus(cfg);
  const platform = daemonPlatform(args);
  const servicePath = daemonServicePath(cfg, platform);
  const pidAlive = typeof status?.pid === "number" ? processAlive(status.pid) : null;
  const fresh = daemonStatusFresh(status, 10 * 60_000);
  const service =
    fresh
      ? {
          platform,
          installed: existsSync(servicePath),
          running: pidAlive === true ? true : null,
          path: servicePath,
          detail: pidAlive === true ? "pid alive from daemon status" : "fresh daemon status; service-manager probe skipped"
        }
      : await daemonServiceStatus(cfg, args, timeoutMs);
  const active = service.running === true || pidAlive === true;
  const state: BrainctlStatusState = status?.ok && fresh && active ? "ok" : "warn";
  return statusSection(state, `service=${service.running === true ? "running" : service.running === false ? "stopped" : "unknown"} fresh=${fresh}`, {
    service,
    status,
    fresh,
    pid_alive: pidAlive
  }, { available: Boolean(status) || service.running === true });
}

async function collectProductStatusSection(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
  const productRoot = cfg.paths.productRepo || PRODUCT_ROOT;
  if (!gitExists(productRoot)) {
    const clientProfile = cfg.profile.startsWith("client");
    return statusSection(
      clientProfile ? "disabled" : "warn",
      clientProfile ? "source checkout not installed for this client" : "product repo is not a git checkout",
      { path: productRoot },
      { available: false }
    );
  }
  const branch = gitStatusProbe(["rev-parse", "--abbrev-ref", "HEAD"], productRoot, timeoutMs);
  const head = gitStatusProbe(["rev-parse", "HEAD"], productRoot, timeoutMs);
  const dirty = gitStatusProbe(["status", "--porcelain"], productRoot, timeoutMs);
  const remoteRef = ["origin/main", "refs/remotes/https-main"].find((candidate) => gitStatusProbe(["rev-parse", "--verify", candidate], productRoot, timeoutMs).ok) || null;
  const aheadBehind = remoteRef ? gitStatusProbe(["rev-list", "--left-right", "--count", `HEAD...${remoteRef}`], productRoot, timeoutMs) : null;
  const dirtyCount = dirty.ok && dirty.output ? dirty.output.split(/\r?\n/).filter(Boolean).length : 0;
  const state: BrainctlStatusState = dirtyCount > 0 || branch.ok === false || head.ok === false ? "warn" : "ok";
  return statusSection(state, `branch=${branch.output || "unknown"} dirty=${dirtyCount}`, {
    path: productRoot,
    branch: branch.ok ? branch.output : null,
    head: head.ok ? head.output : null,
    dirty_count: dirtyCount,
    remote_ref: remoteRef,
    ahead_behind: aheadBehind?.ok ? aheadBehind.output : null
  });
}

function parseKeyValueLines(text: string): Record<string, string> {
  const values: Record<string, string> = {};
  for (const line of text.split(/\r?\n/)) {
    const index = line.indexOf("=");
    if (index <= 0) {
      continue;
    }
    values[line.slice(0, index)] = line.slice(index + 1).trim();
  }
  return values;
}

function parseAheadBehind(value: string | undefined): { ahead: number; behind: number } | null {
  const match = (value || "").trim().match(/^(\d+)\s+(\d+)$/);
  if (!match) {
    return null;
  }
  return { ahead: Number(match[1]), behind: Number(match[2]) };
}

function clientControlSshTarget(cfg: BrainstackConfig): string | null {
  return cfg.client.telegramVia || null;
}

function remoteControlSourceScript(remoteRepo: string): string {
  return `
set -u
repo=${quoteForBash(remoteRepo || "~/brainstack")}
case "$repo" in
  \\~) repo="$HOME" ;;
  \\~/*) repo="$HOME/\${repo#\\~/}" ;;
esac
printf 'repo=%s\\n' "$repo"
if [ ! -d "$repo/.git" ]; then
  printf 'state=missing\\n'
  exit 20
fi
cd "$repo" || exit 21
branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
head="$(git rev-parse HEAD 2>/dev/null || true)"
short="$(git rev-parse --short HEAD 2>/dev/null || true)"
dirty_count="$(git status --porcelain 2>/dev/null | wc -l | tr -d '[:space:]' || true)"
remote_ref=""
for candidate in origin/main refs/remotes/https-main; do
  if git rev-parse --verify "$candidate" >/dev/null 2>&1; then
    remote_ref="$candidate"
    break
  fi
done
origin_head=""
ahead_behind=""
if [ -n "$remote_ref" ]; then
  origin_head="$(git rev-parse --verify "$remote_ref" 2>/dev/null || true)"
  ahead_behind="$(git rev-list --left-right --count "HEAD...$remote_ref" 2>/dev/null || true)"
fi
printf 'state=ok\\n'
printf 'branch=%s\\n' "$branch"
printf 'head=%s\\n' "$head"
printf 'short=%s\\n' "$short"
printf 'dirty_count=%s\\n' "$dirty_count"
printf 'remote_ref=%s\\n' "$remote_ref"
printf 'origin_head=%s\\n' "$origin_head"
printf 'ahead_behind=%s\\n' "$ahead_behind"
`.trim();
}

async function collectControlSourceStatusSection(cfg: BrainstackConfig, timeoutMs: number): Promise<BrainctlStatusSection> {
  if (!cfg.profile.startsWith("client")) {
    return statusSection("disabled", "local profile owns product source status", { profile: cfg.profile });
  }
  const target = clientControlSshTarget(cfg);
  if (!target) {
    return statusSection("disabled", "control SSH target not configured", { remote_repo: cfg.client.telegramRemoteRepo }, { available: false });
  }

  const worker = telegramControlWorker(target);
  const script = remoteControlSourceScript(cfg.client.telegramRemoteRepo || "~/brainstack");
  const sshBin = Bun.which("ssh") || "ssh";
  const result = run(
    [
      sshBin,
      "-o",
      "BatchMode=yes",
      "-o",
      "ConnectTimeout=3",
      ...workerSshTrustArgs(cfg, worker),
      ...workerSshPortArgs(worker),
      workerRemoteTarget(worker),
      "bash",
      "-lc",
      script
    ],
    { check: false, timeoutMs }
  );
  const combined = `${result.stdout}\n${result.stderr}`.trim();
  if (result.timedOut || result.code === 124) {
    return statusSection("warn", "control host source probe timed out", { machine: target, remote_repo: cfg.client.telegramRemoteRepo }, { available: false, error: "timeout" });
  }
  const values = parseKeyValueLines(result.stdout);
  if (result.code !== 0) {
    const missing = values.state === "missing";
    return statusSection(
      "warn",
      missing ? "control host source checkout missing" : "control host source probe failed",
      { machine: target, remote_repo: cfg.client.telegramRemoteRepo, ...values },
      { available: false, error: sanitizeStatusError(combined || `exit ${result.code}`) }
    );
  }
  const dirtyCount = Number(values.dirty_count || "0");
  const aheadBehind = parseAheadBehind(values.ahead_behind);
  const short = values.short || (values.head ? values.head.slice(0, 7) : "unknown");
  let state: BrainctlStatusState = "ok";
  let detail = `control host up to date branch=${values.branch || "unknown"} head=${short}`;
  if (Number.isFinite(dirtyCount) && dirtyCount > 0) {
    state = "warn";
    detail = `control host source dirty files=${dirtyCount} head=${short}`;
  } else if (aheadBehind && aheadBehind.behind > 0) {
    state = "warn";
    detail = `control host behind origin by ${aheadBehind.behind} commit(s) head=${short}`;
  } else if (aheadBehind && aheadBehind.ahead > 0) {
    state = "warn";
    detail = `control host has ${aheadBehind.ahead} unpushed commit(s) head=${short}`;
  } else if (!values.remote_ref) {
    state = "warn";
    detail = `control host remote ref unavailable head=${short}`;
  }
  return statusSection(state, detail, {
    machine: target,
    remote_repo: cfg.client.telegramRemoteRepo,
    repo: values.repo || null,
    branch: values.branch || null,
    head: values.head || null,
    short,
    dirty_count: Number.isFinite(dirtyCount) ? dirtyCount : null,
    remote_ref: values.remote_ref || null,
    origin_head: values.origin_head || null,
    ahead: aheadBehind?.ahead ?? null,
    behind: aheadBehind?.behind ?? null
  });
}

interface FleetMachineStatus {
  name: string;
  role: "client" | "control" | "worker";
  transport: string;
  reachable: boolean;
  status: BrainctlStatusState;
  update_state: "current" | "behind" | "ahead" | "dirty" | "standalone" | "unknown" | "unreachable" | "failed";
  needs_update: boolean;
  detail: string;
  product_repo?: string | null;
  branch?: string | null;
  head?: string | null;
  short?: string | null;
  remote_ref?: string | null;
  origin_head?: string | null;
  ahead?: number | null;
  behind?: number | null;
  dirty_count?: number | null;
  services?: Record<string, string>;
  error?: string;
}

interface FleetStatusReport {
  schema_version: 1;
  generated_at: string;
  source_machine: string;
  profile: Profile;
  ok: boolean;
  degraded: boolean;
  machines: FleetMachineStatus[];
  summary: {
    total: number;
    reachable: number;
    needs_update: number;
    unhealthy: number;
  };
}

function fleetServiceNames(cfg: BrainstackConfig, role: FleetMachineStatus["role"]): string[] {
  if (role === "client") {
    return [];
  }
  if (role === "control") {
    return ["braind.service", ...(cfg.telemux.enabled ? ["telemux.service"] : [])];
  }
  return ["brainstackd.service"];
}

function serviceStates(names: string[]): Record<string, string> {
  const states: Record<string, string> = {};
  if (!names.length) {
    return states;
  }
  if (!commandPath("systemctl")) {
    for (const name of names) {
      states[name] = "unknown";
    }
    return states;
  }
  for (const name of names) {
    const result = run(["systemctl", "--user", "is-active", name], { check: false, timeoutMs: 2000 });
    states[name] = result.stdout.trim() || (result.code === 0 ? "active" : "inactive");
  }
  return states;
}

function serviceStatesUnhealthy(states: Record<string, string>): boolean {
  return Object.values(states).some((state) => state !== "active" && state !== "unknown");
}

function gitSnapshotForFleet(productRoot: string, timeoutMs: number, options: { fetch?: boolean } = {}): Record<string, string> {
  const values: Record<string, string> = { product_repo: productRoot };
  if (!existsSync(productRoot)) {
    values.state = "missing";
    return values;
  }
  if (!gitExists(productRoot)) {
    values.state = "not-git";
    return values;
  }
  if (options.fetch) {
    const fetchResult = gitStatusProbe(["fetch", "--quiet", "origin", "main"], productRoot, Math.min(Math.max(timeoutMs, 500), 20_000));
    if (!fetchResult.ok) {
      values.fetch_error = fetchResult.output || `exit ${fetchResult.code}`;
    }
  }
  values.state = "ok";
  values.branch = gitStatusProbe(["rev-parse", "--abbrev-ref", "HEAD"], productRoot, timeoutMs).output;
  values.head = gitStatusProbe(["rev-parse", "HEAD"], productRoot, timeoutMs).output;
  values.short = gitStatusProbe(["rev-parse", "--short", "HEAD"], productRoot, timeoutMs).output;
  const dirty = gitStatusProbe(["status", "--porcelain"], productRoot, timeoutMs);
  values.dirty_count = dirty.ok && dirty.output ? String(dirty.output.split(/\r?\n/).filter(Boolean).length) : "0";
  values.remote_ref = ["origin/main", "refs/remotes/https-main"].find((candidate) => gitStatusProbe(["rev-parse", "--verify", candidate], productRoot, timeoutMs).ok) || "";
  if (values.remote_ref) {
    values.origin_head = gitStatusProbe(["rev-parse", "--verify", values.remote_ref], productRoot, timeoutMs).output;
    values.ahead_behind = gitStatusProbe(["rev-list", "--left-right", "--count", `HEAD...${values.remote_ref}`], productRoot, timeoutMs).output;
  }
  if (!options.fetch) {
    const liveRemote = gitStatusProbe(["ls-remote", "origin", "refs/heads/main"], productRoot, Math.min(Math.max(timeoutMs, 500), 5000));
    const liveHead = liveRemote.ok ? liveRemote.output.split(/\s+/)[0] || "" : "";
    if (liveHead) {
      const existingAheadBehind = parseAheadBehind(values.ahead_behind);
      values.remote_ref = values.remote_ref || "origin/main";
      values.origin_head = liveHead;
      if (values.head && values.head !== liveHead && !(existingAheadBehind && existingAheadBehind.ahead > 0)) {
        values.ahead_behind = "0 1";
        values.live_remote = "true";
      }
    }
  }
  return values;
}

function fleetStatusFromValues(input: {
  name: string;
  role: FleetMachineStatus["role"];
  transport: string;
  values: Record<string, string>;
  services?: Record<string, string>;
  standaloneOk?: boolean;
  error?: string;
}): FleetMachineStatus {
  const values = input.values;
  const services = input.services || {};
  const reachable = !input.error && values.state !== "unreachable";
  const dirtyCount = Number(values.dirty_count || "0");
  const aheadBehind = parseAheadBehind(values.ahead_behind);
  const short = values.short || (values.head ? values.head.slice(0, 12) : null);
  const serviceUnhealthy = serviceStatesUnhealthy(services);
  if (!reachable) {
    return {
      name: input.name,
      role: input.role,
      transport: input.transport,
      reachable: false,
      status: "warn",
      update_state: "unreachable",
      needs_update: false,
      detail: input.error || "unreachable",
      product_repo: values.product_repo || null,
      services,
      error: input.error
    };
  }
  if (values.state === "missing" || values.state === "not-git") {
    const standalone = input.standaloneOk && input.role === "client";
    return {
      name: input.name,
      role: input.role,
      transport: input.transport,
      reachable: true,
      status: standalone ? "ok" : "warn",
      update_state: standalone ? "standalone" : "unknown",
      needs_update: false,
      detail: standalone ? "client binary install; no product source checkout" : `product repo ${values.state}`,
      product_repo: values.product_repo || null,
      services
    };
  }
  let status: BrainctlStatusState = "ok";
  let updateState: FleetMachineStatus["update_state"] = "current";
  let needsUpdate = false;
  let detail = `current head=${short || "unknown"}`;
  if (Number.isFinite(dirtyCount) && dirtyCount > 0) {
    status = "warn";
    updateState = "dirty";
    detail = `dirty files=${dirtyCount} head=${short || "unknown"}`;
  } else if (aheadBehind && aheadBehind.ahead > 0) {
    status = "warn";
    updateState = "ahead";
    detail =
      aheadBehind.behind > 0
        ? `diverged from origin ahead=${aheadBehind.ahead} behind=${aheadBehind.behind} head=${short || "unknown"}`
        : `ahead of origin by ${aheadBehind.ahead} commit(s) head=${short || "unknown"}`;
  } else if (aheadBehind && aheadBehind.behind > 0) {
    status = "warn";
    updateState = "behind";
    needsUpdate = true;
    detail = `behind origin by ${aheadBehind.behind} commit(s) head=${short || "unknown"}`;
  } else if (!values.remote_ref) {
    status = "warn";
    updateState = "unknown";
    detail = `remote ref unavailable head=${short || "unknown"}`;
  }
  if (serviceUnhealthy) {
    status = "warn";
    detail = `${detail}; service not active`;
  }
  if (values.fetch_error) {
    status = "warn";
    detail = `${detail}; fetch failed`;
  }
  return {
    name: input.name,
    role: input.role,
    transport: input.transport,
    reachable: true,
    status,
    update_state: updateState,
    needs_update: needsUpdate,
    detail,
    product_repo: values.product_repo || null,
    branch: values.branch || null,
    head: values.head || null,
    short,
    remote_ref: values.remote_ref || null,
    origin_head: values.origin_head || null,
    ahead: aheadBehind?.ahead ?? null,
    behind: aheadBehind?.behind ?? null,
    dirty_count: Number.isFinite(dirtyCount) ? dirtyCount : null,
    services,
    ...(values.fetch_error ? { error: values.fetch_error } : {})
  };
}

async function localFleetMachineStatus(
  cfg: BrainstackConfig,
  role: FleetMachineStatus["role"],
  timeoutMs: number,
  options: { fetch?: boolean; standaloneOk?: boolean } = {}
): Promise<FleetMachineStatus> {
  const values = gitSnapshotForFleet(cfg.paths.productRepo || PRODUCT_ROOT, timeoutMs, { fetch: options.fetch });
  return fleetStatusFromValues({
    name: cfg.machine.name,
    role,
    transport: "local",
    values,
    services: serviceStates(fleetServiceNames(cfg, role)),
    standaloneOk: options.standaloneOk
  });
}

function fleetProbeScript(productRepo: string, services: string[], fetch: boolean): string {
  const serviceLines = services.map((service) => `printf 'service:${service}=%s\\n' "$(systemctl --user is-active ${quoteForBash(service)} 2>/dev/null || printf inactive)"`);
  return `
set -u
brainstack_expand_home() {
  case "$1" in
    \\~) printf '%s\\n' "$HOME" ;;
    \\~/*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}
repo="$(brainstack_expand_home ${quoteForBash(productRepo)})"
printf 'state=ok\\n'
printf 'product_repo=%s\\n' "$repo"
if [ ! -d "$repo" ]; then
  printf 'state=missing\\n'
  exit 0
fi
if [ ! -d "$repo/.git" ]; then
  printf 'state=not-git\\n'
  exit 0
fi
if [ ${fetch ? "1" : "0"} -eq 1 ]; then
  fetch_error="$(git -C "$repo" fetch --quiet origin main 2>&1 || true)"
  if [ -n "$fetch_error" ]; then printf 'fetch_error=%s\\n' "$(printf '%s' "$fetch_error" | tr '\\n' ' ' | cut -c1-300)"; fi
fi
printf 'branch=%s\\n' "$(git -C "$repo" rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
printf 'head=%s\\n' "$(git -C "$repo" rev-parse HEAD 2>/dev/null || true)"
printf 'short=%s\\n' "$(git -C "$repo" rev-parse --short HEAD 2>/dev/null || true)"
printf 'dirty_count=%s\\n' "$(git -C "$repo" status --porcelain 2>/dev/null | wc -l | tr -d '[:space:]' || true)"
remote_ref=""
for candidate in origin/main refs/remotes/https-main; do
  if git -C "$repo" rev-parse --verify "$candidate" >/dev/null 2>&1; then
    remote_ref="$candidate"
    break
  fi
done
printf 'remote_ref=%s\\n' "$remote_ref"
if [ -n "$remote_ref" ]; then
  printf 'origin_head=%s\\n' "$(git -C "$repo" rev-parse --verify "$remote_ref" 2>/dev/null || true)"
  printf 'ahead_behind=%s\\n' "$(git -C "$repo" rev-list --left-right --count "HEAD...$remote_ref" 2>/dev/null || true)"
fi
if [ ${fetch ? "1" : "0"} -eq 0 ]; then
  live_remote="$(git -C "$repo" ls-remote origin refs/heads/main 2>/dev/null | awk 'NR == 1 { print $1 }' || true)"
  if [ -n "$live_remote" ]; then
    [ -n "$remote_ref" ] || printf 'remote_ref=%s\\n' "origin/main"
    printf 'origin_head=%s\\n' "$live_remote"
    head_now="$(git -C "$repo" rev-parse HEAD 2>/dev/null || true)"
    ahead_now="$(git -C "$repo" rev-list --left-right --count "HEAD...$remote_ref" 2>/dev/null | awk '{ print $1 }' || true)"
    if [ -n "$head_now" ] && [ "$head_now" != "$live_remote" ] && [ "\${ahead_now:-0}" = "0" ]; then
      printf 'ahead_behind=%s\\n' "0 1"
      printf 'live_remote=%s\\n' "true"
    fi
  fi
fi
if command -v systemctl >/dev/null 2>&1; then
${serviceLines.join("\n")}
fi
`.trim();
}

async function workerFleetMachineStatus(cfg: BrainstackConfig, worker: BrainstackWorkerConfig, timeoutMs: number, fetch: boolean): Promise<FleetMachineStatus> {
  const script = fleetProbeScript(cfg.paths.productRepo || "~/brainstack", fleetServiceNames(cfg, "worker"), fetch);
  const result = runWorkerShell(cfg, worker, script, Math.max(3, Math.ceil(timeoutMs / 1000)), true);
  const combined = `${result.stdout}\n${result.stderr}`.trim();
  if (result.timedOut || result.code === 124) {
    return fleetStatusFromValues({ name: worker.name, role: "worker", transport: worker.transport, values: { state: "unreachable" }, error: "timed out" });
  }
  if (result.code !== 0) {
    return fleetStatusFromValues({
      name: worker.name,
      role: "worker",
      transport: worker.transport,
      values: { state: "unreachable" },
      error: sanitizeStatusError(combined || `exit ${result.code}`)
    });
  }
  const values = parseKeyValueLines(result.stdout);
  const services: Record<string, string> = {};
  for (const [key, value] of Object.entries(values)) {
    if (key.startsWith("service:")) {
      services[key.slice("service:".length)] = value || "unknown";
    }
  }
  return fleetStatusFromValues({ name: worker.name, role: "worker", transport: worker.transport, values, services });
}

function finalizeFleetStatus(report: FleetStatusReport): FleetStatusReport {
  report.summary = {
    total: report.machines.length,
    reachable: report.machines.filter((machine) => machine.reachable).length,
    needs_update: report.machines.filter((machine) => machine.needs_update).length,
    unhealthy: report.machines.filter((machine) => machine.status === "warn" || machine.status === "fail").length
  };
  report.ok = report.summary.unhealthy === 0;
  report.degraded = !report.ok;
  return report;
}

async function collectLocalFleetStatus(cfg: BrainstackConfig, timeoutMs: number, options: { fetch?: boolean } = {}): Promise<FleetStatusReport> {
  const role: FleetMachineStatus["role"] = cfg.profile === "client-macos" ? "client" : cfg.profile === "worker" ? "worker" : "control";
  const report: FleetStatusReport = {
    schema_version: 1,
    generated_at: new Date().toISOString(),
    source_machine: cfg.machine.name,
    profile: cfg.profile,
    ok: false,
    degraded: true,
    machines: [],
    summary: { total: 0, reachable: 0, needs_update: 0, unhealthy: 0 }
  };
  report.machines.push(await localFleetMachineStatus(cfg, role, timeoutMs, { fetch: options.fetch, standaloneOk: cfg.profile === "client-macos" }));
  if (cfg.profile === "control" || cfg.profile === "single-node") {
    for (const worker of defaultWorkers(cfg)) {
      if (worker.name === cfg.machine.name || worker.transport === "local") {
        continue;
      }
      report.machines.push(await workerFleetMachineStatus(cfg, worker, timeoutMs, Boolean(options.fetch)));
    }
  }
  return finalizeFleetStatus(report);
}

function remoteFleetControlScript(remoteRepo: string, argv: string[]): string {
  return `
set -euo pipefail
brainstack_expand_home() {
  case "$1" in
    \\~) printf '%s\\n' "$HOME" ;;
    \\~/*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}
repo="$(brainstack_expand_home ${quoteForBash(remoteRepo)})"
cd "$repo"
if [ -x "$HOME/.local/bin/brainctl" ]; then
  exec "$HOME/.local/bin/brainctl" ${argv.map(quoteForBash).join(" ")} --config "$HOME/.config/brainstack/brainstack.yaml"
fi
if [ -x "$HOME/.bun/bin/bun" ]; then
  bun_bin="$HOME/.bun/bin/bun"
else
  bun_bin="$(command -v bun)"
fi
exec "$bun_bin" --no-env-file run packages/brainctl/src/main.ts ${argv.map(quoteForBash).join(" ")} --config "$HOME/.config/brainstack/brainstack.yaml"
`.trim();
}

function fleetControlSshTarget(cfg: BrainstackConfig, args: ParsedArgs): string | null {
  const via = requireFlagValue(args, "via") || process.env.BRAINSTACK_TELEGRAM_VIA?.trim() || cfg.client.telegramVia || sshTargetFromRemoteSsh(cfg.client.remoteSsh);
  return via ? validateInviteSshTarget(via, "fleet control SSH target") : null;
}

function fleetControlFallbackName(cfg: BrainstackConfig, args: ParsedArgs): string {
  try {
    const via = fleetControlSshTarget(cfg, args);
    return via ? workerSshHost(telegramControlWorker(via)) : "control";
  } catch {
    return cfg.client.telegramVia || "control";
  }
}

function looksLikeUnsupportedFleetCommand(text: string): boolean {
  const normalized = text.toLowerCase();
  return normalized.includes("unknown command: fleet") || (normalized.includes("unknown command") && normalized.includes("brainctl init"));
}

function runFleetControlRemoteScript(cfg: BrainstackConfig, args: ParsedArgs, remoteScript: string, timeoutMs: number): ReturnType<typeof run> | null {
  const via = fleetControlSshTarget(cfg, args);
  if (!via) {
    return null;
  }
  const worker = telegramControlWorker(via);
  const knownHostsPath = telegramKnownHostsPath(cfg, args);
  const sshTrustMode = telegramSshTrustMode(args);
  const sshBin = Bun.which("ssh") || "ssh";
  return run(
    [
      sshBin,
      "-o",
      "BatchMode=yes",
      "-o",
      "ConnectTimeout=8",
      ...telegramSshTrustArgs(sshTrustMode, knownHostsPath),
      ...workerSshPortArgs(worker),
      workerRemoteTarget(worker),
      `bash -lc ${quoteForBash(remoteScript)}`
    ],
    { check: false, timeoutMs }
  );
}

function runFleetControlSsh(cfg: BrainstackConfig, args: ParsedArgs, argv: string[], timeoutMs: number): ReturnType<typeof run> | null {
  const remoteRepo = requireFlagValue(args, "remote-repo") || process.env.BRAINSTACK_TELEGRAM_REMOTE_REPO?.trim() || cfg.client.telegramRemoteRepo || "~/brainstack";
  return runFleetControlRemoteScript(cfg, args, remoteFleetControlScript(remoteRepo, argv), timeoutMs);
}

function clientFleetTargetIsControl(cfg: BrainstackConfig, args: ParsedArgs, target: string | undefined): boolean {
  if (!target || target === "control") {
    return true;
  }
  const via = fleetControlSshTarget(cfg, args);
  if (!via) {
    return false;
  }
  const worker = telegramControlWorker(via);
  return target === via || target === workerRemoteTarget(worker) || target === workerSshHost(worker);
}

function runFleetControlBootstrapUpdate(cfg: BrainstackConfig, args: ParsedArgs, dryRun: boolean): FleetUpdateResult {
  const remoteRepo = requireFlagValue(args, "remote-repo") || process.env.BRAINSTACK_TELEGRAM_REMOTE_REPO?.trim() || cfg.client.telegramRemoteRepo || "~/brainstack";
  const script = fleetUpdateScript("~/.config/brainstack/brainstack.yaml", "control", remoteRepo, dryRun);
  if (dryRun) {
    return { machine: fleetControlFallbackName(cfg, args), role: "control", ok: true, dry_run: true, code: 0, output: script };
  }
  const result = runFleetControlRemoteScript(cfg, args, script, 300_000);
  if (!result) {
    throw new Error("fleet update needs a control SSH target for client-macos installs");
  }
  return {
    machine: fleetControlFallbackName(cfg, args),
    role: "control",
    ok: result.code === 0,
    dry_run: false,
    code: result.code,
    output: summarizeLines(`${result.stdout}\n${result.stderr}`.trim(), 100)
  };
}

async function collectFleetStatus(cfg: BrainstackConfig, args: ParsedArgs, timeoutMs: number, options: { fetch?: boolean } = {}): Promise<FleetStatusReport> {
  if (cfg.profile === "client-macos") {
    const local = await collectLocalFleetStatus(cfg, timeoutMs, { fetch: false });
    const remote = runFleetControlSsh(cfg, args, ["fleet", "status", "--json", ...(options.fetch ? ["--fetch"] : [])], timeoutMs);
    if (!remote) {
      return finalizeFleetStatus(local);
    }
    if (remote.code !== 0 || remote.timedOut) {
      const combined = `${remote.stdout}\n${remote.stderr}`.trim();
      if (!remote.timedOut && looksLikeUnsupportedFleetCommand(combined)) {
        local.machines.push({
          name: fleetControlFallbackName(cfg, args),
          role: "control",
          transport: "ssh",
          reachable: true,
          status: "warn",
          update_state: "unknown",
          needs_update: true,
          detail: "control host brainctl is too old for fleet status; update control host",
          services: {},
          error: sanitizeStatusError(combined)
        });
        return finalizeFleetStatus(local);
      }
      local.machines.push(
        fleetStatusFromValues({
          name: fleetControlFallbackName(cfg, args),
          role: "control",
          transport: "ssh",
          values: { state: "unreachable" },
          error: remote.timedOut ? "timed out" : sanitizeStatusError(combined || `exit ${remote.code}`)
        })
      );
      return finalizeFleetStatus(local);
    }
    try {
      const parsed = JSON.parse(remote.stdout) as FleetStatusReport;
      const names = new Set(local.machines.map((machine) => machine.name));
      local.machines.push(...(parsed.machines || []).filter((machine) => !names.has(machine.name)));
      return finalizeFleetStatus(local);
    } catch (error) {
      local.machines.push(
        fleetStatusFromValues({
          name: fleetControlFallbackName(cfg, args),
          role: "control",
          transport: "ssh",
          values: { state: "unreachable" },
          error: `could not parse fleet status: ${sanitizeStatusError(error)}`
        })
      );
      return finalizeFleetStatus(local);
    }
  }
  return await collectLocalFleetStatus(cfg, timeoutMs, options);
}

async function collectFleetStatusSection(cfg: BrainstackConfig, args: ParsedArgs, timeoutMs: number): Promise<BrainctlStatusSection> {
  const report = await collectFleetStatus(cfg, args, timeoutMs, { fetch: false });
  const state: BrainctlStatusState = report.summary.unhealthy > 0 ? "warn" : "ok";
  return statusSection(state, `machines=${report.summary.total} reachable=${report.summary.reachable} needs_update=${report.summary.needs_update} unhealthy=${report.summary.unhealthy}`, report);
}

function formatBrainctlStatusReport(report: BrainctlStatusReport): string {
  const lines = [
    `brainstack status: ok=${report.ok} degraded=${report.degraded} profile=${report.profile || "unknown"} machine=${report.machine || "unknown"}`,
    `config=${report.config_path}`
  ];
  for (const [name, section] of Object.entries(report.sections)) {
    lines.push(`${section.state.toUpperCase()} ${name}: ${section.detail}${section.error ? ` (${section.error})` : ""}`);
  }
  return lines.join("\n");
}

async function commandStatus(args: ParsedArgs): Promise<void> {
  const configPath = abs(requireFlagValue(args, "config") || brainstackDefaultConfigPath());
  const timeoutMs = statusTimeoutMs(args);
  const report: BrainctlStatusReport = {
    schema_version: 1,
    product: "brainstack",
    generated_at: new Date().toISOString(),
    config_path: configPath,
    profile: null,
    machine: null,
    ok: false,
    degraded: true,
    sections: {}
  };

  let cfg: BrainstackConfig | null = null;
  try {
    cfg = await loadConfig(configPath, flag(args, "profile"), flag(args, "root"));
    report.profile = cfg.profile;
    report.machine = cfg.machine.name;
    const write = safeBrainWriteStatus(cfg);
    report.sections.config = statusSection(write.error ? "warn" : "ok", write.error ? "config loaded with write env warning" : "config loaded", {
      profile: cfg.profile,
      machine: cfg.machine,
      harness: cfg.harness,
      paths: {
        product_repo: cfg.paths.productRepo,
        state_root: cfg.paths.stateRoot,
        config_root: cfg.paths.configRoot,
        shared_brain_root: cfg.paths.sharedBrainRoot,
        client_local_path: clientLocalPathAbs(cfg),
        client_env_path: clientEnvPathAbs(cfg)
      },
      brain: {
        public_base_url_configured: Boolean(cfg.brain.publicBaseUrl),
        write_base_url_configured: write.baseUrlConfigured,
        import_token_present: write.importTokenPresent
      },
      curation: cfg.curation
    }, write.error ? { error: write.error } : {});
  } catch (error) {
    report.sections.config = statusSection("fail", "config load failed", undefined, {
      available: false,
      error: sanitizeStatusError(error)
    });
    finalizeBrainctlStatus(report);
    console.log(hasFlag(args, "json") ? JSON.stringify(report, null, 2) : formatBrainctlStatusReport(report));
    if (hasFlag(args, "strict")) {
      throw new Error("brainctl status failed: config");
    }
    return;
  }

  report.sections.daemon = await collectStatusSection(() => collectDaemonStatusSection(cfg!, args, timeoutMs), timeoutMs);
  report.sections.shared_brain = await collectStatusSection(() => collectSharedBrainStatus(cfg!, Math.min(timeoutMs, 2000)), timeoutMs);
  report.sections.outbox = await collectStatusSection(() => collectOutboxStatusSection(cfg!), timeoutMs, "fail");
  report.sections.hooks = await collectStatusSection(() => collectHooksStatusSection(cfg!), timeoutMs);
  report.sections.skills = await collectStatusSection(() => collectSkillsStatusSection(cfg!), timeoutMs);
  report.sections.brain_api = await collectStatusSection(() => collectBrainApiStatusSection(cfg!, timeoutMs), timeoutMs + 250);
  report.sections.curator = await collectStatusSection(() => collectCuratorStatusSection(cfg!, timeoutMs), timeoutMs + 250);
  report.sections.proposals = await collectStatusSection(() => collectProposalsStatusSection(cfg!, timeoutMs), timeoutMs + 250);
  report.sections.telemux = await collectStatusSection(() => collectTelemuxStatusSection(cfg!, timeoutMs), timeoutMs + 250);
  const fleetTimeoutMs = Math.min(Math.max(timeoutMs * 4, 3000), 15_000);
  report.sections.fleet = await collectStatusSection(() => collectFleetStatusSection(cfg!, args, fleetTimeoutMs), fleetTimeoutMs + 500);
  const controlSourceTimeoutMs = cfg.client.telegramVia ? Math.max(Math.min(timeoutMs, 3000), 2500) : timeoutMs;
  report.sections.control_source = await collectStatusSection(() => collectControlSourceStatusSection(cfg!, controlSourceTimeoutMs), controlSourceTimeoutMs + 250);
  report.sections.product = await collectStatusSection(() => collectProductStatusSection(cfg!, Math.min(timeoutMs, 2000)), timeoutMs);
  finalizeBrainctlStatus(report);
  console.log(hasFlag(args, "json") ? JSON.stringify(report, null, 2) : formatBrainctlStatusReport(report));
  if (hasFlag(args, "strict") && !report.ok) {
    const failures = Object.entries(report.sections)
      .filter(([, section]) => section.state === "fail")
      .map(([name]) => name);
    throw new Error(`brainctl status failed: ${failures.join(", ") || "unknown"}`);
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
  if (!commandPath("rg")) {
    return searchLocalBrainWithoutRg(brain, query, scopes);
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

function searchLocalBrainWithoutRg(brain: ProjectBrain, query: string, scopes: string[]): Array<{ brain: string; label: string; classification: string; path: string; line: number; text: string }> {
  const needle = query.toLocaleLowerCase();
  const results: Array<{ brain: string; label: string; classification: string; path: string; line: number; text: string }> = [];
  const maxFileBytes = 2 * 1024 * 1024;
  const visit = (relDir: string): void => {
    if (results.length >= 100) {
      return;
    }
    const absDir = join(brain.localPath, relDir);
    let entries: ReturnType<typeof readdirSync>;
    try {
      entries = readdirSync(absDir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (results.length >= 100) {
        return;
      }
      const relPath = join(relDir, entry.name);
      if (entry.isDirectory()) {
        if (entry.name === ".git" || entry.name === "node_modules" || entry.name === "dist") {
          continue;
        }
        visit(relPath);
        continue;
      }
      if (!entry.isFile()) {
        continue;
      }
      const absPath = join(brain.localPath, relPath);
      try {
        const info = statSync(absPath);
        if (info.size > maxFileBytes) {
          continue;
        }
        const text = readFileSync(absPath, "utf8");
        const lines = text.split(/\r?\n/);
        for (let index = 0; index < lines.length; index += 1) {
          if (lines[index]?.toLocaleLowerCase().includes(needle)) {
            results.push({ brain: brain.id, label: brain.label, classification: brain.classification, path: relPath, line: index + 1, text: lines[index]?.trim() || "" });
            if (results.length >= 100) {
              return;
            }
          }
        }
      } catch {
        continue;
      }
    }
  };
  for (const scope of scopes) {
    visit(scope);
  }
  return results;
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

function sourceRecordEvidenceRef(source: unknown): string {
  const id = sourceRecordId(source);
  if (!source || typeof source !== "object") {
    return id;
  }
  const record = source as Record<string, unknown>;
  const path = typeof record.path === "string" ? record.path : "";
  const line = typeof record.line === "number" || typeof record.line === "string" ? String(record.line) : "";
  if (id && path) {
    return `${id}:${path}${line ? `:${line}` : ""}`;
  }
  if (id) {
    return id;
  }
  return path;
}

function boundedCliString(value: string | undefined, maxLength: number): string | undefined {
  const trimmed = value?.trim() || "";
  return trimmed ? trimmed.slice(0, maxLength) : undefined;
}

function deriveProjectLabel(repo: string): string {
  const label = basename(repo).replace(/\.git$/i, "").trim();
  return label || "unknown-repo";
}

function validateMemoryScope(scope: string): string {
  const normalized = scope.trim().toLowerCase();
  if (!["repo", "project", "global", "machine", "harness"].includes(normalized)) {
    throw new Error("--scope must be one of repo, project, global, machine, or harness");
  }
  return normalized;
}

function buildRememberBody(input: {
  summary: string;
  project: string;
  domain?: string;
  scope: string;
  memoryKind: string;
  context: string;
  applicability: string;
  nonApplicability: string;
  evidenceRefs: string[];
}): string {
  const lines = [
    "## Context",
    "",
    input.context,
    "",
    "## Lesson",
    "",
    input.summary.trim(),
    "",
    "## Applicability",
    "",
    input.applicability,
    "",
    "## Non-applicability",
    "",
    input.nonApplicability,
    "",
    "## Envelope",
    "",
    `- Project: \`${input.project}\``
  ];
  if (input.domain) {
    lines.push(`- Domain: \`${input.domain}\``);
  }
  lines.push(
    `- Scope: \`${input.scope}\``,
    `- Memory kind: \`${input.memoryKind}\``,
    "",
    "## Evidence",
    "",
    ...(input.evidenceRefs.length ? input.evidenceRefs.map((ref) => `- \`${ref}\``) : ["- No explicit evidence reference was available."])
  );
  return lines.join("\n");
}

function stringFromRecord(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function stringArrayFromRecord(record: Record<string, unknown>, key: string): string[] {
  const value = record[key];
  return Array.isArray(value) ? value.map((item) => (typeof item === "string" ? item.trim() : "")).filter(Boolean) : [];
}

function uniqueNonEmptyStrings(values: Array<string | undefined | null>): string[] {
  return Array.from(new Set(values.map((value) => value?.trim() || "").filter(Boolean)));
}

function uniqueRecordStrings(records: Array<Record<string, unknown>>, key: string): string[] {
  return uniqueNonEmptyStrings(records.map((record) => stringFromRecord(record, key)));
}

function collapseWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function stripMarkdownHeading(value: string): string {
  return value.replace(/^#+\s+/, "").trim();
}

function firstUsefulProposalLine(body: string): string {
  for (const line of body.split(/\r?\n/)) {
    const trimmed = stripMarkdownHeading(line.trim());
    if (!trimmed || trimmed.startsWith("---") || trimmed.startsWith("- Source ") || trimmed.startsWith("- Target ")) {
      continue;
    }
    if (trimmed.length >= 20) {
      return trimmed;
    }
  }
  return "";
}

function proposalSummary(proposal: Record<string, unknown>, body: string, explicit?: string): string {
  if (explicit?.trim()) {
    return collapseWhitespace(explicit).slice(0, 1_000);
  }
  const title = stringFromRecord(proposal, "title") || "";
  const titleSummary = collapseWhitespace(title.replace(/^remember(?:\s*\([^)]+\))?:/i, ""));
  if (titleSummary.length >= 20) {
    return titleSummary.slice(0, 1_000);
  }
  const line = firstUsefulProposalLine(body);
  if (line) {
    return collapseWhitespace(line).slice(0, 1_000);
  }
  return `Enriched memory candidate from proposal ${stringFromRecord(proposal, "id") || "unknown"}`;
}

function clusterSubject(proposal: Record<string, unknown>): string | undefined {
  const cluster = stringFromRecord(proposal, "cluster_label");
  if (!cluster) {
    return undefined;
  }
  const subject = cluster.split("/")[0]?.trim();
  return subject || undefined;
}

function inferProjectForProposal(proposal: Record<string, unknown>): string {
  return (
    stringFromRecord(proposal, "project") ||
    stringFromRecord(proposal, "domain") ||
    (stringFromRecord(proposal, "related_repo") ? deriveProjectLabel(stringFromRecord(proposal, "related_repo")!) : undefined) ||
    clusterSubject(proposal) ||
    "shared-brain"
  );
}

function inferScopeForProposal(proposal: Record<string, unknown>): string {
  const explicit = stringFromRecord(proposal, "scope");
  if (explicit && explicit !== "needs-context") {
    return validateMemoryScope(explicit);
  }
  return stringFromRecord(proposal, "related_repo") ? "repo" : "project";
}

function proposalEnrichmentPayload(
  cfg: BrainstackConfig,
  args: ParsedArgs,
  proposal: Record<string, unknown>,
  body: string
): Record<string, unknown> {
  const id = stringFromRecord(proposal, "id") || args.positional[1] || "unknown";
  const project = boundedCliString(requireFlagValue(args, "project"), 120) || inferProjectForProposal(proposal);
  const domain = boundedCliString(requireFlagValue(args, "domain"), 120) || stringFromRecord(proposal, "domain") || clusterSubject(proposal) || project;
  const scope = validateMemoryScope(requireFlagValue(args, "scope") || inferScopeForProposal(proposal));
  const memoryKind = boundedCliString(requireFlagValue(args, "memory-kind") || requireFlagValue(args, "kind"), 80) || stringFromRecord(proposal, "memory_kind") || "project_lesson";
  const summary = proposalSummary(proposal, body, requireFlagValue(args, "summary"));
  const relatedRepo = boundedCliString(requireFlagValue(args, "related-repo"), 500) || stringFromRecord(proposal, "related_repo");
  const context =
    boundedCliString(requireFlagValue(args, "context"), 1_000) ||
    stringFromRecord(proposal, "context") ||
    `Enriched from legacy or context-poor Brainstack proposal ${id}. Review the original proposal before applying this memory globally.`;
  const applicability =
    boundedCliString(requireFlagValue(args, "applicability"), 1_000) ||
    stringFromRecord(proposal, "applicability") ||
    (scope === "repo" && relatedRepo
      ? `Use when working in ${relatedRepo} or directly related ${domain} work.`
      : `Use for ${project}/${domain} work after verifying the same project and runtime context.`);
  const nonApplicability =
    boundedCliString(requireFlagValue(args, "non-applicability") || requireFlagValue(args, "non_applicability"), 1_000) ||
    stringFromRecord(proposal, "non_applicability") ||
    `Do not apply outside ${project}/${domain} without checking the original evidence and current system behavior.`;
  const evidenceRefs = uniqueNonEmptyStrings([
    `proposal:${id}`,
    ...stringArrayFromRecord(proposal, "source_ids").map((sourceId) => `source:${sourceId}`),
    ...stringArrayFromRecord(proposal, "evidence_refs"),
    ...(relatedRepo ? [`repo:${relatedRepo}`] : []),
    ...flagValues(args, "evidence")
  ]).slice(0, 20);
  const confidenceRaw = requireFlagValue(args, "confidence");
  const confidence = confidenceRaw === undefined ? 0.55 : Number(confidenceRaw);
  if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) {
    throw new Error("--confidence must be a number between 0 and 1");
  }
  const title = requireFlagValue(args, "title") || `Enriched memory (${project}): ${summary.slice(0, 120)}`;
  const proposalBody = buildRememberBody({ summary, project, domain, scope, memoryKind, context, applicability, nonApplicability, evidenceRefs });
  return {
    title,
    body: proposalBody,
    source_harness: requireFlagValue(args, "source-harness") || cfg.harness.name,
    source_machine: requireFlagValue(args, "source-machine") || cfg.machine.name,
    source_type: "memory",
    related_repo: relatedRepo,
    project,
    domain,
    scope,
    memory_kind: memoryKind,
    context,
    applicability,
    non_applicability: nonApplicability,
    evidence_refs: evidenceRefs,
    source_ids: stringArrayFromRecord(proposal, "source_ids"),
    tags: uniqueNonEmptyStrings(["remember", "enriched-memory", project, domain, memoryKind, scope]),
    confidence,
    review_after: boundedCliString(requireFlagValue(args, "review-after"), 80),
    expires_at: boundedCliString(requireFlagValue(args, "expires-at"), 80)
  };
}

function slugPart(value: string): string {
  return value
    .toLowerCase()
    .replace(/'/g, "")
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
}

function firstUniqueOrFallback(values: string[], fallback: string): string {
  return values.length === 1 ? values[0]! : fallback;
}

function proposalReviewGroupsFromResult(result: Record<string, unknown>): Array<Record<string, unknown>> {
  return Array.isArray(result.review_groups)
    ? (result.review_groups as Array<Record<string, unknown>>)
    : Array.isArray(result.clusters)
      ? (result.clusters as Array<Record<string, unknown>>)
      : [];
}

function stripRememberPrefix(value: string): string {
  return collapseWhitespace(value.replace(/^remember(?:\s*\([^)]+\))?:/i, ""));
}

function mergedIntoReason(record: Record<string, unknown>): string | null {
  const reason = stringFromRecord(record, "reason") || "";
  const match = reason.match(/^merged into\s+(.+)$/i);
  return match?.[1]?.trim() || null;
}

function proposalLessonLine(detail: { id: string; proposal: Record<string, unknown>; body: string }): string {
  const title = stripRememberPrefix(stringFromRecord(detail.proposal, "title") || detail.id);
  if (title.length >= 12) {
    return title;
  }
  const bodyLine = firstUsefulProposalLine(detail.body);
  return bodyLine || `Review proposal ${detail.id}`;
}

function buildReviewGroupContent(input: {
  title: string;
  groupKey: string;
  groupLabel: string;
  project: string;
  domain: string;
  scope: string;
  memoryKind: string;
  details: Array<{ id: string; proposal: Record<string, unknown>; body: string }>;
  conflicts: string[];
}): string {
  const seen = new Set<string>();
  const lessonLines = input.details
    .map((detail) => ({ id: detail.id, text: proposalLessonLine(detail) }))
    .filter((item) => {
      const normalized = slugPart(item.text);
      if (seen.has(normalized)) return false;
      seen.add(normalized);
      return true;
    });
  const sourceIds = uniqueNonEmptyStrings(input.details.flatMap((detail) => stringArrayFromRecord(detail.proposal, "source_ids")));
  const evidenceRefs = uniqueNonEmptyStrings([
    ...input.details.map((detail) => `proposal:${detail.id}`),
    ...input.details.flatMap((detail) => stringArrayFromRecord(detail.proposal, "evidence_refs"))
  ]);
  return [
    `# ${input.title}`,
    "",
    `Consolidates ${input.details.length} related Brainstack proposal candidates from review group \`${input.groupKey}\` (${input.groupLabel}).`,
    "",
    "## Lessons",
    "",
    ...lessonLines.map((item) => `- ${item.text} (source: \`proposal:${item.id}\`)`),
    "",
    "## Applicability",
    "",
    `Use for ${input.project}/${input.domain} work when the scope is \`${input.scope}\` and the lesson kind is \`${input.memoryKind}\`.`,
    "",
    "## Non-applicability",
    "",
    `Do not apply outside ${input.project}/${input.domain} without checking the source proposals and current system behavior.`,
    "",
    "## Review Notes",
    "",
    ...(input.conflicts.length ? input.conflicts.map((conflict) => `- Needs human review: ${conflict}`) : ["- Deterministic consolidation only; no embedding or cosine-similarity merge was used."]),
    "",
    "## Evidence",
    "",
    ...evidenceRefs.map((ref) => `- \`${ref}\``),
    ...(sourceIds.length ? ["", "## Source Artifacts", "", ...sourceIds.map((id) => `- \`${id}\``)] : [])
  ].join("\n");
}

async function createReviewGroupMergeProposal(
  cfg: BrainstackConfig,
  args: ParsedArgs,
  groupKey: string
): Promise<{ payload: Record<string, unknown>; selected: number; conflicts: string[]; dryRun: boolean; write?: BrainWriteOutcome; closed?: string[] }> {
  const closeSources = hasFlag(args, "close-sources");
  const status = requireFlagValue(args, "status") || (closeSources ? "open,rejected" : "open");
  const result = await brainApiRequest(cfg, "GET", `/api/proposals?status=${encodeURIComponent(status)}`);
  const proposals = Array.isArray(result.proposals) ? (result.proposals as Array<Record<string, unknown>>) : [];
  const matching = proposals.filter((proposal) => stringFromRecord(proposal, "cluster_key") === groupKey || stringFromRecord(proposal, "cluster_label") === groupKey);
  if (matching.length < 2) {
    throw new Error(`review group ${groupKey} has ${matching.length} matching proposal(s); merge-group requires at least 2`);
  }
  const limit = parsePositiveIntegerFlag(args, "limit", 20);
  if (!hasFlag(args, "all") && matching.length > limit) {
    throw new Error(`review group ${groupKey} has ${matching.length} proposal(s); merge-group defaults to ${limit}. Rerun with --limit N or --all.`);
  }
  const details: Array<{ id: string; proposal: Record<string, unknown>; body: string }> = [];
  for (const item of hasFlag(args, "all") ? matching : matching.slice(0, limit)) {
    const id = stringFromRecord(item, "id");
    if (!id) continue;
    const detail = await fetchProposalDetail(cfg, id);
    details.push({ id, proposal: { ...item, ...detail.proposal }, body: detail.body });
  }
  if (details.length < 2) {
    throw new Error(`review group ${groupKey} has ${details.length} readable proposal(s); merge-group requires at least 2`);
  }
  const records = details.map((detail) => detail.proposal);
  const groupLabel = stringFromRecord(records[0] || {}, "cluster_label") || groupKey;
  const labelSubject = groupLabel.split("/")[0]?.trim() || groupKey;
  const projectValues = uniqueRecordStrings(records, "project");
  const domainValues = uniqueRecordStrings(records, "domain");
  const scopeValues = uniqueRecordStrings(records, "scope").filter((value) => value !== "needs-context");
  const kindValues = uniqueRecordStrings(records, "memory_kind");
  const relatedRepoValues = uniqueRecordStrings(records, "related_repo");
  const conflicts = [
    projectValues.length > 1 ? `multiple projects: ${projectValues.join(", ")}` : null,
    domainValues.length > 1 ? `multiple domains: ${domainValues.join(", ")}` : null,
    scopeValues.length > 1 ? `multiple scopes: ${scopeValues.join(", ")}` : null,
    kindValues.length > 1 ? `multiple memory kinds: ${kindValues.join(", ")}` : null,
    relatedRepoValues.length > 1 ? `multiple related repos: ${relatedRepoValues.join(", ")}` : null,
    records.some(proposalNeedsContext) ? "one or more source proposals need context" : null,
    records.some((proposal) => !stringFromRecord(proposal, "target_page") && !stringFromRecord(proposal, "memory_kind")) ? "one or more source proposals are context-only candidates" : null
  ].filter(Boolean) as string[];
  const project = boundedCliString(requireFlagValue(args, "project"), 120) || firstUniqueOrFallback(projectValues, labelSubject || "shared-brain");
  const domain = boundedCliString(requireFlagValue(args, "domain"), 120) || firstUniqueOrFallback(domainValues, project);
  const scope = validateMemoryScope(requireFlagValue(args, "scope") || firstUniqueOrFallback(scopeValues, "project"));
  const memoryKind = boundedCliString(requireFlagValue(args, "memory-kind") || requireFlagValue(args, "kind"), 80) || firstUniqueOrFallback(kindValues, "project_lesson");
  const title = requireFlagValue(args, "title") || `Consolidate: ${groupLabel}`;
  const targetPage = requireFlagValue(args, "target-page") || `wiki/Syntheses/${slugPart(labelSubject || groupKey) || "review-group"}-lessons.md`;
  const confidenceRaw = requireFlagValue(args, "confidence") || (conflicts.length ? "0.55" : "0.75");
  const confidence = Number(confidenceRaw);
  if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) {
    throw new Error("--confidence must be a number between 0 and 1");
  }
  const content = buildReviewGroupContent({ title, groupKey, groupLabel, project, domain, scope, memoryKind, details, conflicts });
  const sourceIds = uniqueNonEmptyStrings([
    ...details.map((detail) => `proposal:${detail.id}`),
    ...details.flatMap((detail) => stringArrayFromRecord(detail.proposal, "source_ids"))
  ]).slice(0, 100);
  const evidenceRefs = uniqueNonEmptyStrings([
    ...details.map((detail) => `proposal:${detail.id}`),
    ...details.flatMap((detail) => stringArrayFromRecord(detail.proposal, "evidence_refs"))
  ]).slice(0, 100);
  const payload: Record<string, unknown> = {
    title,
    body: `Consolidates ${details.length} related proposal candidates from review group ${groupKey}.`,
    source_harness: requireFlagValue(args, "source-harness") || cfg.harness.name,
    source_machine: requireFlagValue(args, "source-machine") || cfg.machine.name,
    source_type: "memory",
    target_page: targetPage,
    proposed_content: content,
    base_sha256: requireFlagValue(args, "base-sha256") || "absent",
    risk: requireFlagValue(args, "risk") || "low",
    confidence,
    source_ids: sourceIds,
    project,
    domain,
    scope,
    memory_kind: memoryKind,
    context: `Consolidated from Brainstack review group ${groupKey} (${groupLabel}).`,
    applicability: `Use for ${project}/${domain} work when the source proposals and scope match.`,
    non_applicability: `Do not apply outside ${project}/${domain} without checking the source proposals.`,
    evidence_refs: evidenceRefs
  };
  if (conflicts.length || hasFlag(args, "needs-human")) {
    payload.status = "needs-human";
    payload.reason = conflicts.length ? `Review group merge needs human review: ${conflicts.join("; ")}` : "Review group merge was explicitly marked needs-human.";
  }
  const dryRun = !hasFlag(args, "submit") && !hasFlag(args, "apply") && !hasFlag(args, "yes");
  if (dryRun) {
    return { payload, selected: details.length, conflicts, dryRun };
  }
  const existingMergeIds = uniqueNonEmptyStrings(details.map((detail) => mergedIntoReason(detail.proposal)));
  if (closeSources && existingMergeIds.length > 1) {
    throw new Error(`review group ${groupKey} has sources already merged into different proposals: ${existingMergeIds.join(", ")}`);
  }
  if (closeSources) {
    const openProposalStatuses = new Set(["pending", "approved", "needs-human"]);
    const unclosable = details
      .filter((detail) => {
        const proposalStatus = stringFromRecord(detail.proposal, "status") || "";
        return !openProposalStatuses.has(proposalStatus) && !mergedIntoReason(detail.proposal);
      })
      .map((detail) => `${detail.id}:${stringFromRecord(detail.proposal, "status") || "unknown"}`);
    if (unclosable.length) {
      throw new Error(`review group ${groupKey} has source proposals that cannot be closed: ${unclosable.join(", ")}`);
    }
  }
  const write: BrainWriteOutcome =
    closeSources && existingMergeIds[0]
      ? { status: "accepted", response: { proposal_id: existingMergeIds[0], status: "already-created" } }
      : closeSources
        ? { status: "accepted", response: await brainApiRequest(cfg, "POST", "/api/propose", { admin: true, body: payload }) }
        : await postBrainWriteOrQueue(cfg, "propose", payload, {}, { quiet: hasFlag(args, "json") });
  const closed: string[] = [];
  if (closeSources) {
    const mergedId = stringFromRecord(write.response || {}, "proposal_id") || stringFromRecord(write.response || {}, "id") || "merged review-group proposal";
    for (const detail of details) {
      if (mergedIntoReason(detail.proposal)) {
        closed.push(detail.id);
        continue;
      }
      await brainApiRequest(cfg, "POST", `/api/proposals/${encodeURIComponent(detail.id)}/reject`, {
        admin: true,
        body: {
          decided_by: `${process.env.USER || "operator"}@${cfg.machine.name}`,
          reason: `merged into ${mergedId}`
        }
      });
      closed.push(detail.id);
    }
  }
  return { payload, selected: details.length, conflicts, dryRun, write, closed };
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
  const project = boundedCliString(flag(args, "project"), 120) || deriveProjectLabel(resolved.repo);
  const domain = boundedCliString(flag(args, "domain"), 120) || project;
  const scope = validateMemoryScope(flag(args, "scope") || "repo");
  const memoryKind = boundedCliString(flag(args, "memory-kind") || flag(args, "kind"), 80) || "project_lesson";
  const context = boundedCliString(flag(args, "context"), 1_000) || `Captured while working in repo ${resolved.repo}.`;
  const applicability =
    boundedCliString(flag(args, "applicability"), 1_000) ||
    (scope === "global"
      ? "Use only when the lesson is clearly independent of any project, harness, machine, or product domain."
      : `Use when working on ${project} or directly related ${scope}-scoped work.`);
  const nonApplicability =
    boundedCliString(flag(args, "non-applicability") || flag(args, "non_applicability"), 1_000) ||
    "Do not apply globally without checking the captured project and evidence context.";
  const evidenceRefs = Array.from(
    new Set(
      [
        `repo:${resolved.repo}`,
        ...recentSources.map(sourceRecordEvidenceRef),
        ...flagValues(args, "evidence")
      ]
        .map((ref) => ref.trim())
        .filter(Boolean)
    )
  ).slice(0, 20);
  const titlePrefix = project ? `Remember (${project})` : "Remember";
  const proposalBody = buildRememberBody({ summary, project, domain, scope, memoryKind, context, applicability, nonApplicability, evidenceRefs });
  await postBrainWriteOrQueue(cfg, target.write === "true" ? "import" : "propose", {
    title: `${titlePrefix}: ${summary.slice(0, 120)}`,
    text: proposalBody,
    body: proposalBody,
    source_harness: cfg.harness.name,
    source_machine: cfg.machine.name,
    source_type: "remember",
    related_repo: resolved.repo,
    project,
    domain,
    scope,
    memory_kind: memoryKind,
    context,
    applicability,
    non_applicability: nonApplicability,
    evidence_refs: evidenceRefs,
    recent_sources: recentSources,
    recent_context_sources: recentContextSources,
    tags: ["remember", target.id, project, memoryKind, scope],
    review_after: boundedCliString(flag(args, "review-after"), 80),
    expires_at: boundedCliString(flag(args, "expires-at"), 80)
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
  // Use the shared destination shape so direct sends, queued entries, and flush
  // replays all present the same idempotency key to the brain.
  const idempotencyKey = outboxItemKey(item.endpoint, payload, outboxDestinationForKey(item));
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
  await assertOutboxCapacity(root, existing ? path : null, outboxItemFileBytes(queuedItem));
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
  destination: { brain_id?: string | null; url?: string | null } = {}
): string {
  // Must match the key shape queued/flushed items use (normalizeOutboxItem), so the
  // brain can recognize an outbox replay of a request that already arrived once.
  return outboxItemKey(
    endpoint,
    payload,
    outboxDestinationForKey({ brain_id: destination.brain_id || undefined, url: destination.url || undefined })
  );
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

type BrainWriteOutcome =
  | { status: "accepted"; response?: Record<string, unknown> }
  | { status: "queued"; path: string; reason: string };

async function postBrainWriteOrQueue(
  cfg: BrainstackConfig,
  endpoint: "import" | "propose",
  payload: Record<string, unknown>,
  overrides: { baseUrl?: string; importTokenEnv?: string; targetBrainId?: string } = {},
  options: { quiet?: boolean } = {}
): Promise<BrainWriteOutcome> {
  const writeConfig = brainWriteConfig(cfg);
  const baseUrl = overrides.baseUrl || writeConfig.baseUrl;
  const writeToken = overrides.importTokenEnv ? resolveClientEnvValue(cfg, overrides.importTokenEnv) : writeConfig.token;
  if (!baseUrl || !writeToken) {
    const reason = "brain base URL or import token is missing";
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
      reason
    );
    if (!options.quiet) {
      console.warn(`shared-brain write queued: ${queued}`);
    }
    return { status: "queued", path: queued, reason };
  }

  try {
    const response = await fetch(new URL(`/api/${endpoint}`, baseUrl).toString(), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${writeToken}`,
        "Content-Type": "application/json",
        "Idempotency-Key": brainWriteIdempotencyKey(endpoint, payload, {
          brain_id: overrides.targetBrainId || null,
          url: baseUrl || null
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
    const text = await response.text();
    let parsed: Record<string, unknown> | undefined;
    if (text.trim()) {
      try {
        parsed = JSON.parse(text) as Record<string, unknown>;
      } catch {
        parsed = undefined;
      }
    }
    if (!options.quiet) {
      console.log(`shared-brain ${endpoint} accepted`);
    }
    return { status: "accepted", response: parsed };
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
    if (!options.quiet) {
      console.warn(`shared-brain write queued: ${queued}`);
    }
    return { status: "queued", path: queued, reason: message };
  }
}

async function queueBrainWriteForBackgroundFlush(
  cfg: BrainstackConfig,
  endpoint: "import" | "propose",
  payload: Record<string, unknown>,
  reason: string
): Promise<string> {
  const writeConfig = brainWriteConfig(cfg);
  return await queueOutboxItem(
    cfg,
    {
      endpoint,
      url: writeConfig.baseUrl || "",
      payload,
      source_machine: String(payload.source_machine || cfg.machine.name),
      source_harness: String(payload.source_harness || cfg.harness.name)
    },
    reason
  );
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

async function retryOutboxItems(cfg: BrainstackConfig, args: ParsedArgs, scans: Array<Awaited<ReturnType<typeof scanOutbox>>>): Promise<number> {
  const target = args.positional[1];
  const retryAll = hasFlag(args, "all");
  if (!retryAll && !target) {
    throw new Error("outbox retry requires an item id or --all");
  }
  const corrupt = scans.flatMap((scan) => scan.corrupt);
  if (corrupt.length) {
    throw new Error("outbox retry found corrupt/unsafe entries; run `brainctl outbox list` and repair or purge-corrupt first");
  }
  let requeued = 0;
  const now = new Date().toISOString();
  for (const entry of scans.flatMap((scan) => scan.items)) {
    const normalized = normalizeOutboxItem(entry.item);
    const matches = retryAll || normalized.id === target || basename(entry.path) === target || basename(entry.path) === `${target}.json`;
    if (!matches) {
      continue;
    }
    if (retryAll && !normalized.terminal_error) {
      continue;
    }
    const next = {
      ...normalized,
      retry_count: 0,
      last_error: null,
      terminal_error: null,
      requeued_at: now
    } as OutboxItem & { requeued_at: string };
    await writeOutboxItem(entry.path, next);
    requeued += 1;
  }
  if (!requeued && target) {
    throw new Error(`outbox item not found or not retryable: ${target}`);
  }
  return requeued;
}

function parseIntegerFlag(args: ParsedArgs, key: string): number | null {
  const raw = requireFlagValue(args, key);
  if (raw === undefined) {
    return null;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value)) {
    throw new Error(`--${key} must be an integer`);
  }
  return value;
}

function parsePositiveIntegerFlag(args: ParsedArgs, key: string, fallback: number): number {
  const raw = requireFlagValue(args, key);
  if (raw === undefined) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`--${key} must be a positive integer`);
  }
  return value;
}

function inviteControlSshTarget(cfg: BrainstackConfig, args: ParsedArgs): string {
  const explicit = requireFlagValue(args, "control-ssh") || requireFlagValue(args, "via");
  if (explicit) {
    return validateInviteSshTarget(explicit, "invite control SSH target");
  }
  const remoteTarget = sshTargetFromRemoteSsh(cfg.client.remoteSsh || cfg.repos.remoteSsh);
  if (remoteTarget) {
    return validateInviteSshTarget(remoteTarget, "invite control SSH target from client remote");
  }
  const host = inviteBareHost(cfg.tailscale.tailnetHost || cfg.machine.hostname || cfg.machine.name, "invite control host");
  if (!host.trim()) {
    throw new Error("invite create requires a control host: pass --control-ssh, configure client.remoteSsh, or fill tailscale.tailnetHost, machine.hostname, or machine.name");
  }
  const user = cfg.machine.sshUser || cfg.machine.user;
  return validateInviteSshTarget(user ? `${user}@${host}` : host, "invite control SSH target");
}

async function readInviteKnownHosts(args: ParsedArgs, cfg: BrainstackConfig, controlSshTarget: string): Promise<string[]> {
  const explicit = requireFlagValue(args, "ssh-known-hosts-file") || requireFlagValue(args, "known-hosts");
  const input = explicit || join(cfg.paths.configRoot, "ssh_known_hosts");
  const path = absWithHome(input, cfg.paths.home);
  if (!existsSync(path)) {
    if (explicit) {
      throw new Error(`ssh known-hosts invite source not found: ${path}`);
    }
    return [];
  }
  const info = await lstat(path);
  if (info.isSymbolicLink() || !info.isFile()) {
    throw new Error(`ssh known-hosts invite source must be a regular non-symlink file: ${path}`);
  }
  if (info.size > 64 * 1024) {
    throw new Error(`ssh known-hosts invite source is too large: ${path}`);
  }
  const lines = (await readFile(path, "utf8"))
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"));
  const clean = sanitizeInviteKnownHosts(lines, `ssh known-hosts invite source ${path}`);
  const matching = filterKnownHostsForSshTarget(clean, controlSshTarget);
  if (explicit && !matching.length) {
    throw new Error(`ssh known-hosts invite source ${path} has no entry for ${controlKnownHostsLookup(controlSshTarget)}`);
  }
  return matching;
}

async function inviteImportToken(args: ParsedArgs): Promise<string | undefined> {
  const filePath = requireFlagValue(args, "import-token-file");
  const envName = requireFlagValue(args, "import-token-env");
  if (filePath && envName) {
    throw new Error("invite create accepts either --import-token-file or --import-token-env, not both");
  }
  if (filePath) {
    const value = await readEnvSecretOrFile("BRAIN_IMPORT_TOKEN", "BRAIN_IMPORT_TOKEN_FILE", filePath);
    if (!value) {
      throw new Error("--import-token-file did not contain a token");
    }
    return value;
  }
  if (envName) {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(envName)) {
      throw new Error("--import-token-env must be an environment variable name");
    }
    const value = process.env[envName]?.trim();
    if (!value) {
      throw new Error(`--import-token-env ${envName} is empty or unset`);
    }
    return value;
  }
  return undefined;
}

function normalizeInstallReleaseTag(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error("--install-version must not be empty");
  }
  if (trimmed === "latest") {
    return "latest";
  }
  const tag = trimmed.startsWith("v") ? trimmed : `v${trimmed}`;
  if (!/^v[A-Za-z0-9._-]+$/.test(tag)) {
    throw new Error(`invalid Brainstack release tag: ${trimmed}`);
  }
  return tag;
}

function releaseInstallUrlForTag(tag: string): string {
  const normalized = normalizeInstallReleaseTag(tag);
  return normalized === "latest" ? LATEST_INSTALL_URL : `${GITHUB_RELEASE_DOWNLOAD_BASE}/${normalized}/install.sh`;
}

function defaultInviteInstallUrl(): string {
  return releaseInstallUrlForTag(BRAINSTACK_PACKAGE_VERSION);
}

function validateInviteInstallUrl(url: string, args: ParsedArgs): string {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    throw new Error(`invalid --install-url: ${url}`);
  }
  if (parsed.protocol !== "https:" && !hasFlag(args, "allow-insecure-install-url")) {
    throw new Error("--install-url must use https; pass --allow-insecure-install-url only for local release smoke tests");
  }
  return url;
}

function inviteInstallUrl(args: ParsedArgs): string {
  const explicitUrl = requireFlagValue(args, "install-url");
  const explicitVersion = requireFlagValue(args, "install-version");
  if (explicitUrl && explicitVersion) {
    throw new Error("invite create accepts either --install-url or --install-version, not both");
  }
  if (explicitUrl) {
    return validateInviteInstallUrl(explicitUrl, args);
  }
  if (explicitVersion) {
    return releaseInstallUrlForTag(explicitVersion);
  }
  return defaultInviteInstallUrl();
}

function inviteInstallCommand(installUrl: string): string {
  return `curl -fsSL ${shellSingleQuote(installUrl)} | sh`;
}

type EnrollSkillsProfile = PortableSkillProfile | "none";

function normalizeEnrollSkillsProfile(value: string | undefined, fallback: EnrollSkillsProfile): EnrollSkillsProfile {
  const normalized = (value || fallback).trim().toLowerCase();
  if (normalized === "none" || normalized === "off" || normalized === "skip") {
    return "none";
  }
  return normalizePortableSkillProfile(normalized);
}

function defaultInviteSkillsProfile(harnessName: HarnessName): EnrollSkillsProfile {
  return harnessName === "codex" ? "client" : "none";
}

async function commandInviteCreate(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "control", flag(args, "root"));
  const expiresHours = parsePositiveIntegerFlag(args, "expires-hours", 24);
  const createdAt = new Date();
  const expiresAt = new Date(createdAt.getTime() + expiresHours * 60 * 60 * 1000);
  const harnessName = normalizeHarness(requireFlagValue(args, "harness") || cfg.harness.name);
  const skillsProfile = normalizeEnrollSkillsProfile(requireFlagValue(args, "skills-profile"), defaultInviteSkillsProfile(harnessName));
  const controlSshTarget = inviteControlSshTarget(cfg, args);
  const invitePayload: BrainstackClientInvite = {
    schema_version: 1,
    type: CLIENT_INVITE_TYPE,
    created_at: createdAt.toISOString(),
    expires_at: expiresAt.toISOString(),
    profile: "client-macos",
    brain: {
      publicBaseUrl: requireFlagValue(args, "brain-base-url") || cfg.brain.publicBaseUrl,
      remoteSsh: requireFlagValue(args, "brain-remote") || cfg.client.remoteSsh || cfg.repos.remoteSsh
    },
    control: {
      sshTarget: controlSshTarget,
      remoteRepo: requireFlagValue(args, "control-repo") || requireFlagValue(args, "remote-repo") || cfg.paths.productRepo,
      sshKnownHosts: await readInviteKnownHosts(args, cfg, controlSshTarget)
    },
    client: {
      localPath: requireFlagValue(args, "client-local-path") || cfg.client.localPath || "~/shared-brain",
      envPath: requireFlagValue(args, "client-env-path") || cfg.client.envPath || "~/.config/shared-brain.env"
    },
    harness: {
      name: harnessName,
      bin: requireFlagValue(args, "harness-bin") || harnessName
    },
    skills: skillsProfile === "none" ? undefined : { profile: skillsProfile }
  };
  const importToken = await inviteImportToken(args);
  if (importToken) {
    invitePayload.importToken = importToken;
  }
  if (!invitePayload.brain.publicBaseUrl) {
    throw new Error("invite create requires brain.publicBaseUrl in config or --brain-base-url");
  }
  if (!invitePayload.brain.remoteSsh) {
    throw new Error("invite create requires client.remoteSsh in config or --brain-remote");
  }
  const invite = encodeClientInvite(invitePayload);
  const installUrl = inviteInstallUrl(args);
  const installCommand = inviteInstallCommand(installUrl);

  // Token-bearing invites are bearer secrets: never print them to stdout by default,
  // where they land in scrollback, CI logs, and shell transcripts. Write them to a
  // 0600 file instead, unless the operator explicitly opts into printing.
  const outFlag = requireFlagValue(args, "out");
  const printSecret = hasFlag(args, "print-secret");
  let invitePath: string | null = null;
  if (outFlag || (importToken && !printSecret)) {
    const defaultOut = join(cfg.paths.configRoot, "invites", `client-invite-${createdAt.toISOString().replace(/[:.]/g, "-")}.txt`);
    invitePath = absWithHome(outFlag || defaultOut, cfg.paths.home);
    await writeText(invitePath, `${invite}\n`, 0o600);
  }
  const printInvite = invitePath === null;

  if (hasFlag(args, "json")) {
    console.log(
      JSON.stringify(
        {
          ...(printInvite ? { invite } : { invitePath }),
          installCommand,
          expiresAt: invitePayload.expires_at,
          includesImportToken: Boolean(importToken),
          includesSshKnownHosts: invitePayload.control.sshKnownHosts.length > 0,
          skillsProfile
        },
        null,
        2
      )
    );
    return;
  }
  console.log(installCommand);
  console.log("");
  if (printInvite) {
    console.log("Paste this invite when the installer prompts for it:");
    console.log(invite);
  } else {
    console.log(`Invite written to: ${invitePath}`);
    console.log("Transfer it over a private channel and pass it to the installer with --invite-file.");
    console.log("Do not use installer --invite-file - with `curl | sh`; stdin is already the installer script.");
    console.log("Use --print-secret to print the invite to stdout instead (it will land in terminal scrollback).");
  }
  console.log("");
  console.log(`expires: ${invitePayload.expires_at}`);
  console.log(`import token embedded: ${importToken ? "yes" : "no"}`);
  console.log(`ssh host pin embedded: ${invitePayload.control.sshKnownHosts.length ? "yes" : "no"}`);
  console.log(`codex skills profile: ${skillsProfile}`);
  if (importToken) {
    console.log("treat the invite as a bearer secret; avoid putting it in shell history or shared logs.");
  }
  if (/\/releases\/latest\//.test(installUrl)) {
    console.log("WARN install URL uses the moving 'latest' release; pass --install-version vX.Y.Z or --install-url with a pinned release tag so enrollment installs a known binary.");
  }
}

async function commandInvite(args: ParsedArgs): Promise<void> {
  const subcommand = args.positional[0] || "help";
  switch (subcommand) {
    case "create":
      return await commandInviteCreate(args);
    case "help":
    case "--help":
    case "-h":
      console.log("Usage: brainctl invite create --config brainstack.yaml [--import-token-file FILE|--import-token-env ENV] [--ssh-known-hosts-file FILE] [--control-ssh SSH_TARGET] [--control-repo PATH] [--skills-profile client|operator|control|worker|none] [--expires-hours N] [--install-version TAG|latest|--install-url URL] [--allow-insecure-install-url] [--out FILE] [--print-secret] [--json]");
      return;
    default:
      throw new Error(`Unknown invite command: ${subcommand}`);
  }
}

function buildEnrollConfig(invite: BrainstackClientInvite, args: ParsedArgs, harnessBin: string): Record<string, unknown> {
  const user = process.env.USER || "operator";
  const home = abs(requireFlagValue(args, "home") || process.env.HOME || ".");
  const machineName = requireFlagValue(args, "machine") || discoveredMachineName();
  return {
    schema_version: CONFIG_SCHEMA_VERSION,
    profile: "client-macos",
    runtime: {
      bunBin: "bun"
    },
    harness: {
      name: invite.harness.name,
      bin: harnessBin
    },
    machine: {
      name: machineName,
      user,
      role: "client-macos",
      sshUser: user,
      hostname: machineName
    },
    paths: {
      home,
      productRepo: "~/brainstack",
      sharedBrainRoot: invite.client.localPath,
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
      publicBaseUrl: invite.brain.publicBaseUrl,
      largeFileThresholdBytes: 10 * 1024 * 1024,
      enableTelemux: false
    },
    telemux: {
      enabled: false,
      dashboardHost: "127.0.0.1",
      dashboardPort: 8787,
      localMachine: machineName,
      workers: []
    },
    tailscale: {
      tailnetHost: httpUrlHostname(invite.brain.publicBaseUrl, "invite brain.publicBaseUrl"),
      controlTag: "tag:brain",
      workerTag: "tag:brain-worker",
      advertiseTags: [],
      enableSsh: false
    },
    client: {
      localPath: invite.client.localPath,
      envPath: invite.client.envPath,
      remoteSsh: invite.brain.remoteSsh,
      telegramRemoteRepo: invite.control.remoteRepo,
      telegramVia: invite.control.sshTarget
    }
  };
}

async function writeEnrollConfig(configPath: string, rendered: string, force: boolean): Promise<void> {
  if (existsSync(configPath)) {
    const existing = await readFile(configPath, "utf8");
    if (existing === rendered) {
      return;
    }
    if (!force) {
      throw new Error(`refusing to overwrite existing config ${configPath}; rerun enroll with --force if this invite should replace it`);
    }
  }
  await writeText(configPath, rendered, 0o600);
}

async function installInviteKnownHosts(cfg: BrainstackConfig, entries: string[]): Promise<boolean> {
  const clean = sanitizeInviteKnownHosts(entries, "invite control.sshKnownHosts");
  if (!clean.length) {
    return false;
  }
  const path = join(cfg.paths.configRoot, "ssh_known_hosts");
  const existingRaw = (await readExistingPrivateText(path))
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"));
  const existing = sanitizeInviteKnownHosts(existingRaw, `${path}`);
  const seenKeys = new Map<string, { key: string; line: string }>();
  for (const line of existing) {
    const parsed = parseKnownHostEntry(line);
    if (!parsed) {
      continue;
    }
    for (const host of parsed.hosts) {
      seenKeys.set(`${host}\u0000${parsed.keyType}`, { key: parsed.key, line });
    }
  }
  for (const line of clean) {
    const parsed = parseKnownHostEntry(line);
    if (!parsed) {
      continue;
    }
    for (const host of parsed.hosts) {
      const key = `${host}\u0000${parsed.keyType}`;
      const previous = seenKeys.get(key);
      if (previous && previous.key !== parsed.key) {
        throw new Error(`invite SSH host pin conflicts with existing known_hosts entry for ${host} ${parsed.keyType}; resolve ${path} manually before enrolling`);
      }
      seenKeys.set(key, { key: parsed.key, line });
    }
  }
  const merged = [...new Set([...existing, ...clean])];
  await writeText(path, `${merged.join("\n")}\n`, 0o600);
  return merged.length !== existing.length;
}

async function readStdinText(): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function readInviteFile(path: string): Promise<string> {
  if (path === "-") {
    return await readStdinText();
  }
  const absolute = abs(path);
  const info = await lstat(absolute);
  if (info.isSymbolicLink() || !info.isFile()) {
    throw new Error(`--invite-file must point to a regular non-symlink file: ${absolute}`);
  }
  if (info.size > CLIENT_INVITE_MAX_CHARS) {
    throw new Error(`--invite-file is too large: ${absolute}`);
  }
  if ((info.mode & 0o077) !== 0) {
    throw new Error(`--invite-file must not be group/world accessible; run chmod 600 ${shellSingleQuote(absolute)}`);
  }
  return await readFile(absolute, "utf8");
}

async function commandEnroll(args: ParsedArgs): Promise<void> {
  if (hasFlag(args, "help") || args.positional[0] === "help" || args.positional[0] === "-h" || args.positional[0] === "--help") {
    console.log("Usage: brainctl enroll --invite bs1_...|--invite-file FILE|- [--config brainstack.yaml] [--skills-profile client|operator|control|worker|none] [--skip-skills] [--skip-init] [--skip-doctor] [--force]");
    return;
  }
  const inviteInput = requireFlagValue(args, "invite");
  const inviteFile = requireFlagValue(args, "invite-file");
  const inviteEnv = process.env.BRAINSTACK_INVITE?.trim();
  const inviteSources = [inviteInput, inviteFile, inviteEnv].filter((value) => value !== undefined && value !== "");
  if (inviteSources.length > 1) {
    throw new Error("enroll accepts only one invite source: --invite, --invite-file, or BRAINSTACK_INVITE");
  }
  // Raw invites in argv or env leak through shell history, process listings, and
  // environment snapshots. Match install.sh: file/stdin is the default-safe path.
  if ((inviteInput || inviteEnv) && !hasFlag(args, "allow-unsafe-invite")) {
    throw new Error(
      "raw invites in --invite/BRAINSTACK_INVITE can leak through shell history or process listings; use --invite-file FILE (or --invite-file - for stdin). Pass --allow-unsafe-invite only for local throwaway smoke tests."
    );
  }
  const rawInvite =
    inviteInput ||
    (inviteFile ? await readInviteFile(inviteFile) : "") ||
    inviteEnv ||
    "";
  const invite = decodeClientInvite(rawInvite);
  ensureProvisionPrereqs("client-macos");
  const harness = resolveEnrollHarnessBin(invite);
  if (!harness) {
    throw new Error(`enroll blocked: invited harness ${invite.harness.bin} is missing.\n${installHint(invite.harness.name)}`);
  }

  const configPath = abs(requireFlagValue(args, "config") || "~/.config/brainstack/brainstack.yaml");
  const rendered = stringifySimpleYaml(buildEnrollConfig(invite, args, harness.configBin));
  await writeEnrollConfig(configPath, rendered, hasFlag(args, "force"));
  const cfg = await loadConfig(configPath, "client-macos", flag(args, "root"));
  const installedKnownHosts = await installInviteKnownHosts(cfg, invite.control.sshKnownHosts);
  if (!installedKnownHosts && invite.control.sshKnownHosts.length === 0) {
    console.log("ssh host pin not embedded; Telegram file send will need a pinned known-hosts file before routine use.");
  }

  const overrideTokenFile = requireFlagValue(args, "import-token-file");

  if (!hasFlag(args, "skip-init")) {
    const initFlags: Record<string, string | boolean | string[]> = {
      config: configPath,
      profile: "client-macos"
    };
    if (overrideTokenFile) {
      initFlags["import-token-file"] = overrideTokenFile;
    }
    await commandInit({ command: "init", positional: [], flags: initFlags }, { importToken: overrideTokenFile ? undefined : invite.importToken });
  }

  const defaultSkillsProfile = invite.skills?.profile || defaultInviteSkillsProfile(invite.harness.name);
  const skillsProfile = normalizeEnrollSkillsProfile(requireFlagValue(args, "skills-profile"), defaultSkillsProfile);
  if (!hasFlag(args, "skip-skills") && skillsProfile !== "none") {
    await commandSkills({ command: "skills", positional: ["install"], flags: { target: "codex", profile: skillsProfile } });
  }

  if (!hasFlag(args, "skip-doctor")) {
    await commandDoctor({ command: "doctor", positional: [], flags: { config: configPath, profile: "client-macos" } });
  }
  console.log(`enrolled client with config: ${configPath}`);
  console.log(`control ssh: ${invite.control.sshTarget}`);
  console.log(`control repo: ${invite.control.remoteRepo}`);
}

function sanitizeTelegramSendFileName(value: string): string {
  const cleaned = basename(value)
    .replace(/[\u0000-\u001f\u007f]/g, "_")
    .replace(/[/:\\]/g, "_")
    .trim();
  return cleaned || "brainstack-file";
}

function hasMixedScriptConfusables(value: string): boolean {
  // Names mixing Latin with Cyrillic/Greek letters are a classic trick to dodge
  // keyword-based sensitive-name checks (e.g. "secrеt.txt" with a Cyrillic е).
  const hasLatin = /[a-z]/i.test(value);
  const hasConfusableScript = /[\u0370-\u03ff\u0400-\u04ff]/.test(value);
  return hasLatin && hasConfusableScript;
}

function isSensitiveTelegramFileName(value: string): boolean {
  // NFKC-normalize before matching so unicode presentation tricks cannot dodge the
  // keyword checks.
  const normalized = sanitizeTelegramSendFileName(value).normalize("NFKC").toLowerCase();
  return (
    normalized.startsWith(".") ||
    normalized === ".env" ||
    normalized.endsWith(".env") ||
    normalized.endsWith(".pem") ||
    normalized.endsWith(".key") ||
    normalized.endsWith(".p12") ||
    normalized.endsWith(".pfx") ||
    normalized.endsWith(".jks") ||
    TELEGRAM_SEND_SENSITIVE_FILE_PATTERN.test(normalized) ||
    hasMixedScriptConfusables(normalized)
  );
}

async function validateTelegramSendLocalFile(filePath: string, displayName: string | undefined, maxBytes: number, allowSensitive: boolean): Promise<{ absolutePath: string; fileName: string; sizeBytes: number }> {
  const absolutePath = abs(filePath);
  const info = await lstat(absolutePath);
  if (info.isSymbolicLink()) {
    throw new Error(`refusing to send symlink: ${absolutePath}`);
  }
  if (!info.isFile()) {
    throw new Error(`not a regular file: ${absolutePath}`);
  }
  if (info.size > maxBytes) {
    throw new Error(`file too large: ${info.size} bytes > ${maxBytes}`);
  }

  const sourceFileName = sanitizeTelegramSendFileName(basename(absolutePath));
  const fileName = sanitizeTelegramSendFileName(displayName || basename(absolutePath));
  if (!allowSensitive && (isSensitiveTelegramFileName(sourceFileName) || isSensitiveTelegramFileName(fileName))) {
    throw new Error(`refusing to send sensitive-looking file name: ${sourceFileName}${fileName !== sourceFileName ? ` as ${fileName}` : ""}; rerun with --allow-sensitive if intentional`);
  }

  return { absolutePath, fileName, sizeBytes: info.size };
}

function sshTargetFromRemoteSsh(remote: string): string | null {
  const trimmed = remote.trim();
  if (!trimmed) {
    return null;
  }
  if (trimmed.startsWith("ssh://")) {
    try {
      const parsed = new URL(trimmed);
      const user = decodeURIComponent(parsed.username || "");
      const host = parsed.hostname.includes(":") ? `[${parsed.hostname}]` : parsed.hostname;
      return `${user ? `${user}@` : ""}${host}${parsed.port ? `:${parsed.port}` : ""}`;
    } catch {
      return null;
    }
  }

  const match = trimmed.match(/^((?:[^@/\s]+@)?(?:\[[^\]]+\]|[^:/\s]+)(?::\d+)?):.+$/);
  return match?.[1] || null;
}

function telegramControlSshTarget(cfg: BrainstackConfig, args: ParsedArgs): string {
  const via = requireFlagValue(args, "via") || process.env.BRAINSTACK_TELEGRAM_VIA?.trim() || cfg.client.telegramVia || sshTargetFromRemoteSsh(cfg.client.remoteSsh);
  if (!via) {
    throw new Error("telegram send-file requires --via SSH_TARGET or client.remoteSsh in config");
  }
  return validateInviteSshTarget(via, "telegram control SSH target");
}

function normalizeControlSshTrustMode(value: string | undefined): ControlSshTrustMode | null {
  if (value === undefined) {
    return null;
  }
  if (value === "pinned" || value === "accept-new" || value === "default") {
    return value;
  }
  throw new Error("--ssh-trust must be pinned, accept-new, or default");
}

function telegramControlWorker(via: string): BrainstackWorkerConfig {
  return {
    name: "control",
    transport: "ssh",
    sshTarget: via,
    sshUser: null,
    managedRepoRoot: "",
    managedHostRoot: "",
    managedScratchRoot: ""
  };
}

function telegramKnownHostsPath(cfg: BrainstackConfig, args: ParsedArgs): string {
  const knownHostsInput = requireFlagValue(args, "known-hosts") || process.env.BRAINSTACK_TELEGRAM_KNOWN_HOSTS?.trim();
  return knownHostsInput ? absWithHome(knownHostsInput, cfg.paths.home) : join(cfg.paths.configRoot, "ssh_known_hosts");
}

function telegramSshTrustMode(args: ParsedArgs): ControlSshTrustMode {
  const rawMode = normalizeControlSshTrustMode(requireFlagValue(args, "ssh-trust") || process.env.BRAINSTACK_TELEGRAM_SSH_TRUST?.trim());
  return rawMode || "pinned";
}

function telegramSshTrustArgs(mode: ControlSshTrustMode, knownHostsPath: string): string[] {
  if (mode === "default") {
    return [];
  }
  if (mode === "pinned") {
    return ["-o", "StrictHostKeyChecking=yes", "-o", `UserKnownHostsFile=${knownHostsPath}`];
  }

  return ["-o", "StrictHostKeyChecking=accept-new", "-o", `UserKnownHostsFile=${knownHostsPath}`];
}

function normalizeTelegramSendKind(args: ParsedArgs): string | null {
  const kind = requireFlagValue(args, "kind");
  if (kind === undefined) {
    return null;
  }
  if (kind === "document" || kind === "photo") {
    return kind;
  }
  throw new Error("--kind must be document or photo");
}

function telegramRemoteSendScript(options: {
  remoteRepo: string;
  displayName: string;
  caption?: string;
  context?: string;
  chatId: number | null;
  threadId: number | null;
  kind: string | null;
  maxBytes: number;
  allowSensitive: boolean;
}): string {
  const sendArgs = [
    "apps/telemux/src/send-file.ts",
    "--file",
    '"$tmp_file"',
    "--display-name",
    quoteForBash(options.displayName),
    "--max-bytes",
    quoteForBash(String(options.maxBytes)),
    "--delete-after-send",
    "--json"
  ];
  if (options.caption) sendArgs.push("--caption", quoteForBash(options.caption));
  if (options.context) sendArgs.push("--context", quoteForBash(options.context));
  if (options.chatId !== null) sendArgs.push("--chat-id", quoteForBash(String(options.chatId)));
  if (options.threadId !== null) sendArgs.push("--thread-id", quoteForBash(String(options.threadId)));
  if (options.kind) sendArgs.push("--kind", quoteForBash(options.kind));
  if (options.allowSensitive) sendArgs.push("--allow-sensitive");

  return `
set -euo pipefail
brainstack_expand_home() {
  case "$1" in
    \\~) printf '%s\\n' "$HOME" ;;
    \\~/*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}

runtime_env="$HOME/.config/brainstack/telemux.runtime.env"
secrets_env="$HOME/.config/brainstack/telemux.secrets.env"
set -a
if [ -r "$runtime_env" ]; then . "$runtime_env"; fi
if [ -r "$secrets_env" ]; then . "$secrets_env"; fi
set +a

state_root="\${BRAINSTACK_STATE_ROOT:-$HOME/.local/state/brainstack}"
tmp_root="$state_root/telemux/incoming"
mkdir -p "$tmp_root"
chmod 700 "$tmp_root" 2>/dev/null || true
tmp_file="$(mktemp "$tmp_root/brainctl-send.XXXXXX")"
cleanup() { rm -f "$tmp_file"; }
trap cleanup EXIT
max_bytes=${quoteForBash(String(options.maxBytes))}
head -c "$((max_bytes + 1))" > "$tmp_file"
received_bytes="$(wc -c < "$tmp_file" | tr -d '[:space:]')"
if [ "$received_bytes" -gt "$max_bytes" ]; then
  echo "file too large while receiving: $received_bytes bytes > $max_bytes" >&2
  exit 43
fi

repo="$(brainstack_expand_home ${quoteForBash(options.remoteRepo)})"
cd "$repo"
if [ -x "$HOME/.bun/bin/bun" ]; then
  bun_bin="$HOME/.bun/bin/bun"
else
  bun_bin="$(command -v bun)"
fi
"$bun_bin" --no-env-file run ${sendArgs.join(" ")}
`.trim();
}

function formatTelegramSendSummary(result: Record<string, unknown>): string {
  const target = result.target && typeof result.target === "object" ? (result.target as Record<string, unknown>) : {};
  const mode = typeof target.mode === "string" ? target.mode : "unknown";
  const context = typeof target.contextSlug === "string" ? `:${target.contextSlug}` : "";
  const thread = target.threadId === null || target.threadId === undefined ? "none" : String(target.threadId);
  return `sent ${String(result.fileName || "file")} (${String(result.sizeBytes || "unknown")} bytes) to Telegram target=${mode}${context} thread=${thread}`;
}

async function commandTelegramSendFile(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
  const filePath = requireFlagValue(args, "file");
  if (!filePath) {
    throw new Error("telegram send-file requires --file PATH");
  }
  const chatId = parseIntegerFlag(args, "chat-id");
  const threadId = parseIntegerFlag(args, "thread-id");
  const context = requireFlagValue(args, "context");
  if (context && chatId !== null) {
    throw new Error("telegram send-file accepts either --context or --chat-id, not both");
  }
  if (context && threadId !== null) {
    throw new Error("telegram send-file uses the bound context thread; omit --thread-id with --context");
  }
  const maxBytes = parsePositiveIntegerFlag(args, "max-bytes", TELEGRAM_SEND_DEFAULT_MAX_BYTES);
  const kind = normalizeTelegramSendKind(args);
  const allowSensitive = hasFlag(args, "allow-sensitive");
  const displayName = requireFlagValue(args, "display-name");
  const localFile = await validateTelegramSendLocalFile(filePath, displayName, maxBytes, allowSensitive);
  const via = telegramControlSshTarget(cfg, args);
  const worker = telegramControlWorker(via);
  const remoteRepo = requireFlagValue(args, "remote-repo") || process.env.BRAINSTACK_TELEGRAM_REMOTE_REPO?.trim() || cfg.client.telegramRemoteRepo || "~/brainstack";
  const remoteScript = telegramRemoteSendScript({
    remoteRepo,
    displayName: localFile.fileName,
    caption: requireFlagValue(args, "caption"),
    context,
    chatId,
    threadId,
    kind,
    maxBytes,
    allowSensitive
  });
  const knownHostsPath = telegramKnownHostsPath(cfg, args);
  const sshTrustMode = telegramSshTrustMode(args);
  if (sshTrustMode === "accept-new") {
    await ensureDir(dirname(knownHostsPath));
  }
  const sshArgs = [
    "ssh",
    "-o",
    "BatchMode=yes",
    "-o",
    "ConnectTimeout=8",
    ...telegramSshTrustArgs(sshTrustMode, knownHostsPath),
    ...workerSshPortArgs(worker),
    workerRemoteTarget(worker),
    `bash -lc ${quoteForBash(remoteScript)}`
  ];
  const result = await runWithStdinFile(sshArgs, localFile.absolutePath, { maxBytes });
  if (result.code !== 0) {
    throw new Error(`telegram send-file failed over ssh with exit ${result.code}\n${result.stderr || result.stdout}`);
  }
  const output = result.stdout.trim();
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(output) as Record<string, unknown>;
  } catch {
    throw new Error(`telegram send-file returned non-JSON output\n${output}`);
  }
  if (hasFlag(args, "json")) {
    console.log(JSON.stringify(parsed, null, 2));
    return;
  }
  console.log(formatTelegramSendSummary(parsed));
}

async function commandTelegram(args: ParsedArgs): Promise<void> {
  const subcommand = args.positional[0] || "help";
  switch (subcommand) {
    case "send-file":
      return await commandTelegramSendFile(args);
    case "help":
    case "--help":
    case "-h":
      console.log("Usage: brainctl telegram send-file --file PATH [--config brainstack.yaml] [--via SSH_TARGET] [--remote-repo PATH] [--caption TEXT] [--context SLUG|--chat-id ID] [--thread-id ID] [--kind document|photo] [--max-bytes N] [--allow-sensitive] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default] [--json]");
      return;
    default:
      throw new Error(`Unknown telegram command: ${subcommand}`);
  }
}

interface SkillPackageFile {
  path: string;
  encoding: "utf8" | "base64";
  content: string;
  size_bytes: number;
  sha256: string;
}

interface SkillPackage {
  schema_version: 1;
  kind: typeof BRAINSTACK_SKILL_PACKAGE_KIND;
  name: string;
  description?: string;
  imported_at: string;
  source: Record<string, unknown>;
  files: SkillPackageFile[];
  package_sha256: string;
}

interface SkillPackageWithManifest {
  manifest_path: string;
  raw_path: string;
  created_at: string;
  package: SkillPackage;
}

interface SkillImportScanCandidate {
  name: string;
  description?: string;
  root: string;
  sources: string[];
  priority: number;
  file_count: number;
  total_bytes: number;
  fingerprint: string;
  action: "install" | "update" | "already-current";
}

interface SkillImportRejectedCandidate {
  root: string;
  sources: string[];
  error: string;
}

interface SkillImportPlan {
  note: string;
  apply: boolean;
  repo: string;
  scan_roots: Array<{ source: string; path: string; exists: boolean }>;
  proposed: SkillImportScanCandidate[];
  skipped: Array<SkillImportScanCandidate & { reason: string }>;
  rejected: SkillImportRejectedCandidate[];
  warnings: string[];
  applied: string[];
}

interface RefreshSkillResult {
  repo: string;
  target: HookTarget;
  root: string;
  installed: string[];
  skipped: string[];
  warnings: string[];
}

interface SkillDoctorCheck {
  status: CheckStatus;
  skill: string;
  path: string;
  detail: string;
}

const SKILL_IMPORT_SKIP_DIRS = new Set([".git", "node_modules", ".venv", "__pycache__", "dist", "build"]);
const SKILL_IMPORT_DEFAULT_MAX_BYTES = 2 * 1024 * 1024;
const SKILL_IMPORT_DEFAULT_MAX_FILES = 200;
const SKILL_IMPORT_DEFAULT_MAX_FILE_BYTES = 512 * 1024;
const SKILL_IMPORT_SCAN_DEFAULT_MAX_DEPTH = 5;
const SKILL_IMPORT_SCAN_DEFAULT_MAX_DIRS = 1500;
const BRAINSTACK_DAEMON_LABEL = "com.brainstack.daemon";
const BRAINSTACK_DAEMON_SERVICE = "brainstackd.service";
const BRAINSTACK_DAEMON_STATUS_SCHEMA = 1;

interface BrainstackDaemonJobStatus {
  ok: boolean;
  detail: string;
  started_at?: string;
  finished_at?: string;
  duration_ms?: number;
}

interface BrainstackDaemonStatus {
  schema_version: 1;
  product: "brainstack";
  daemon: "brainctl daemon run";
  ok: boolean;
  pid: number;
  machine: string;
  config_path: string;
  state_path: string;
  started_at: string;
  updated_at: string;
  iteration: number;
  next_run_after?: string;
  repo: {
    path: string;
    exists: boolean;
    clean: boolean | null;
    branch: string | null;
    head: string | null;
    last_pull_at?: string;
  };
  outbox: BrainstackDaemonJobStatus & {
    flushed?: number;
    kept?: number;
    terminal_failures?: number;
    corrupt?: number;
  };
  skills: BrainstackDaemonJobStatus & {
    targets?: HookTarget[];
    installed?: string[];
    skipped?: string[];
  };
  errors: string[];
}

interface DaemonServiceStatus {
  platform: "launchd" | "systemd";
  installed: boolean;
  running: boolean | null;
  path: string;
  detail: string;
}

function normalizeHookTarget(value: string | undefined, fallback: HookTarget = "codex"): HookTarget {
  const normalized = (value || fallback).trim().toLowerCase();
  if (normalized === "codex" || normalized === "claude" || normalized === "cursor") {
    return normalized;
  }
  throw new Error(`Unsupported hook target: ${value}. Expected codex, claude, cursor, or all.`);
}

function hookTargetsFromArgs(args: ParsedArgs): HookTarget[] {
  const target = (requireFlagValue(args, "target") || "codex").trim().toLowerCase();
  if (target === "all") {
    return ["codex", "claude", "cursor"];
  }
  return [normalizeHookTarget(target)];
}

function skillInstallRootForTarget(target: HookTarget, args: ParsedArgs): string {
  const explicitDir = requireFlagValue(args, "dir") || requireFlagValue(args, "out");
  if (explicitDir) {
    return abs(explicitDir);
  }
  const home = process.env.HOME ? abs(process.env.HOME) : "";
  if (!home) {
    throw new Error("Cannot locate home directory; set HOME or pass --dir.");
  }
  if (target === "codex") {
    const codexHome = process.env.CODEX_HOME ? abs(process.env.CODEX_HOME) : join(home, ".codex");
    return join(codexHome, "skills");
  }
  if (target === "claude") {
    return join(home, ".claude", "skills");
  }
  return join(home, ".cursor", "skills");
}

function safeSkillName(value: string): string {
  const name = value.trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$/.test(name)) {
    throw new Error(`invalid skill name: ${value}`);
  }
  return name;
}

function parseSkillMetadata(text: string, fallbackName: string): { name: string; description?: string } {
  const metadata: Record<string, string> = {};
  if (text.startsWith("---\n")) {
    const end = text.indexOf("\n---", 4);
    if (end !== -1) {
      for (const line of text.slice(4, end).split(/\r?\n/)) {
        const match = line.match(/^([A-Za-z_][A-Za-z0-9_-]*)\s*:\s*(.*)$/);
        if (match) {
          metadata[match[1]] = match[2].trim().replace(/^['"]|['"]$/g, "");
        }
      }
    }
  }
  return {
    name: safeSkillName(metadata.name || fallbackName),
    description: metadata.description || undefined
  };
}

function isUrlLikeSkillSource(value: string): boolean {
  return /^(https?|ssh|git):\/\//i.test(value) || /^git@[^:]+:[^/].+/.test(value);
}

function validateSkillRelativePath(input: string): string {
  const normalized = input.replace(/\\/g, "/");
  if (!normalized || normalized.startsWith("/") || normalized.includes("\0")) {
    throw new Error(`unsafe skill file path: ${input}`);
  }
  const parts = normalized.split("/");
  if (parts.some((part) => !part || part === "." || part === "..")) {
    throw new Error(`unsafe skill file path: ${input}`);
  }
  return normalized;
}

function isUtf8Text(bytes: Uint8Array): { ok: true; text: string } | { ok: false } {
  try {
    return { ok: true, text: new TextDecoder("utf-8", { fatal: true }).decode(bytes) };
  } catch {
    return { ok: false };
  }
}

async function resolveLocalSkillRoot(input: string): Promise<{ root: string; sourcePath: string }> {
  const sourcePath = abs(input);
  const info = await lstat(sourcePath);
  if (info.isSymbolicLink()) {
    throw new Error(`refusing to import symlinked skill source: ${sourcePath}`);
  }
  if (info.isFile()) {
    if (basename(sourcePath) !== "SKILL.md") {
      throw new Error("local skill file imports must point at SKILL.md or a skill directory");
    }
    return { root: await realpath(dirname(sourcePath)), sourcePath };
  }
  if (!info.isDirectory()) {
    throw new Error(`skill source is not a file or directory: ${sourcePath}`);
  }
  const skillFile = join(sourcePath, "SKILL.md");
  const skillInfo = await lstat(skillFile).catch(() => null);
  if (!skillInfo || skillInfo.isSymbolicLink() || !skillInfo.isFile()) {
    throw new Error(`skill directory must contain a regular SKILL.md: ${sourcePath}`);
  }
  return { root: await realpath(sourcePath), sourcePath };
}

async function collectSkillPackageFiles(root: string, options: { maxBytes: number; maxFiles: number; maxFileBytes: number }): Promise<SkillPackageFile[]> {
  const files: SkillPackageFile[] = [];
  let totalBytes = 0;
  async function walk(dir: string): Promise<void> {
    const entries = await readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = join(dir, entry.name);
      const relPath = validateSkillRelativePath(relative(root, fullPath).split(sep).join("/"));
      if (entry.isDirectory()) {
        if (!SKILL_IMPORT_SKIP_DIRS.has(entry.name)) {
          await walk(fullPath);
        }
        continue;
      }
      const info = await lstat(fullPath);
      if (info.isSymbolicLink()) {
        throw new Error(`refusing to import symlink inside skill: ${relPath}`);
      }
      if (!info.isFile()) {
        throw new Error(`refusing to import non-regular file inside skill: ${relPath}`);
      }
      if (info.nlink > 1) {
        throw new Error(`refusing to import hardlinked file inside skill: ${relPath}`);
      }
      if (info.size > options.maxFileBytes) {
        throw new Error(`skill file is too large: ${relPath} ${info.size} bytes > ${options.maxFileBytes}`);
      }
      if (files.length >= options.maxFiles) {
        throw new Error(`skill has too many files: > ${options.maxFiles}`);
      }
      totalBytes += info.size;
      if (totalBytes > options.maxBytes) {
        throw new Error(`skill package is too large: ${totalBytes} bytes > ${options.maxBytes}`);
      }
      const bytes = await readFile(fullPath);
      const text = isUtf8Text(bytes);
      files.push({
        path: relPath,
        encoding: text.ok ? "utf8" : "base64",
        content: text.ok ? text.text : Buffer.from(bytes).toString("base64"),
        size_bytes: bytes.byteLength,
        sha256: createHash("sha256").update(bytes).digest("hex")
      });
    }
  }
  await walk(root);
  if (!files.some((file) => file.path === "SKILL.md")) {
    throw new Error(`skill package missing SKILL.md: ${root}`);
  }
  return files.sort((a, b) => (a.path === "SKILL.md" ? -1 : b.path === "SKILL.md" ? 1 : a.path.localeCompare(b.path)));
}

function stripUndefinedForStableHash(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(stripUndefinedForStableHash);
  }
  if (!value || typeof value !== "object") {
    return value;
  }
  const output: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
    if (entry !== undefined) {
      output[key] = stripUndefinedForStableHash(entry);
    }
  }
  return output;
}

function skillPackageHash(pkg: Omit<SkillPackage, "package_sha256"> | SkillPackage): string {
  const { package_sha256: _ignored, ...unsigned } = pkg as SkillPackage;
  return sha256Hex(canonicalJson(stripUndefinedForStableHash(unsigned)));
}

async function findContainingGitRoot(path: string): Promise<string | null> {
  let current = (await lstat(path)).isDirectory() ? path : dirname(path);
  while (true) {
    if (existsSync(join(current, ".git"))) {
      return current;
    }
    const parent = dirname(current);
    if (parent === current) {
      return null;
    }
    current = parent;
  }
}

function gitOutput(args: string[], cwd: string, timeoutMs = 3000): string {
  const result = run(["git", ...args], { cwd, check: false, timeoutMs });
  return result.code === 0 ? result.stdout.trim() : "";
}

async function localSkillGitProvenance(skillRoot: string): Promise<Record<string, unknown> | undefined> {
  const gitRoot = await findContainingGitRoot(skillRoot).catch(() => null);
  if (!gitRoot) {
    return undefined;
  }
  const rel = relative(gitRoot, skillRoot) || ".";
  return {
    root: gitRoot,
    relative_path: rel.split(sep).join("/"),
    remote: gitOutput(["remote", "get-url", "origin"], gitRoot) || undefined,
    branch: gitOutput(["rev-parse", "--abbrev-ref", "HEAD"], gitRoot) || undefined,
    commit: gitOutput(["rev-parse", "HEAD"], gitRoot) || undefined,
    dirty: Boolean(gitOutput(["status", "--porcelain", "--", rel], gitRoot))
  };
}

async function buildLocalSkillPackage(input: string, options: { maxBytes: number; maxFiles: number; maxFileBytes: number }): Promise<SkillPackage> {
  const { root, sourcePath } = await resolveLocalSkillRoot(input);
  const skillText = await readFile(join(root, "SKILL.md"), "utf8");
  const metadata = parseSkillMetadata(skillText, basename(root));
  const files = await collectSkillPackageFiles(root, options);
  const base: Omit<SkillPackage, "package_sha256"> = {
    schema_version: 1,
    kind: BRAINSTACK_SKILL_PACKAGE_KIND,
    name: metadata.name,
    description: metadata.description,
    imported_at: new Date().toISOString(),
    source: {
      kind: "local",
      input,
      source_path: sourcePath,
      root,
      git: await localSkillGitProvenance(root)
    },
    files
  };
  return { ...base, package_sha256: skillPackageHash(base) };
}

function skillFilesFingerprint(files: SkillPackageFile[]): string {
  return sha256Hex(
    canonicalJson(
      files
        .map((file) => ({
          path: file.path,
          encoding: file.encoding,
          size_bytes: file.size_bytes,
          sha256: file.sha256
        }))
        .sort((a, b) => a.path.localeCompare(b.path))
    )
  );
}

function parseNonNegativeIntegerFlag(args: ParsedArgs, key: string, fallback: number): number {
  const raw = requireFlagValue(args, key);
  if (raw === undefined) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`--${key} must be a non-negative integer`);
  }
  return value;
}

function skillImportScanTargetsFromArgs(args: ParsedArgs): HookTarget[] {
  const target = (requireFlagValue(args, "target") || "all").trim().toLowerCase();
  if (target === "all") {
    return ["codex", "claude", "cursor"];
  }
  return [normalizeHookTarget(target)];
}

function skillScanRoots(args: ParsedArgs, targets: HookTarget[]): Array<{ source: string; path: string; priority: number; recursive: boolean }> {
  const roots: Array<{ source: string; path: string; priority: number; recursive: boolean }> = [];
  if (!hasFlag(args, "no-current")) {
    roots.push({ source: "cwd", path: process.cwd(), priority: 0, recursive: true });
  }
  for (const [index, dir] of flagValues(args, "scan-dir").entries()) {
    roots.push({ source: `scan-dir:${index + 1}`, path: abs(dir), priority: 5 + index, recursive: true });
  }
  for (const target of targets) {
    const targetArgs: ParsedArgs = { ...args, flags: { ...args.flags } };
    delete targetArgs.flags.dir;
    delete targetArgs.flags.out;
    roots.push({ source: target, path: skillInstallRootForTarget(target, targetArgs), priority: target === "codex" ? 20 : target === "claude" ? 30 : 40, recursive: true });
  }
  return roots;
}

async function discoverSkillRootsInTree(
  root: string,
  options: { maxDepth: number; maxDirs: number; recursive: boolean }
): Promise<{ roots: string[]; warnings: string[] }> {
  const warnings: string[] = [];
  const discovered: string[] = [];
  const absoluteRoot = abs(root);
  if (!existsSync(absoluteRoot)) {
    return { roots: [], warnings };
  }
  let scannedDirs = 0;
  let truncated = false;
  async function walk(dir: string, depth: number): Promise<void> {
    if (truncated) {
      return;
    }
    if (scannedDirs >= options.maxDirs) {
      truncated = true;
      warnings.push(`scan truncated at ${options.maxDirs} directories under ${absoluteRoot}`);
      return;
    }
    scannedDirs += 1;
    let info;
    try {
      info = await lstat(dir);
    } catch (error) {
      warnings.push(`scan skipped unreadable path ${dir}: ${error instanceof Error ? error.message : String(error)}`);
      return;
    }
    if (info.isSymbolicLink() || !info.isDirectory()) {
      return;
    }
    const skillFile = join(dir, "SKILL.md");
    const skillInfo = await lstat(skillFile).catch(() => null);
    if (skillInfo?.isFile() && !skillInfo.isSymbolicLink()) {
      discovered.push(await realpath(dir));
      return;
    }
    if (!options.recursive || depth >= options.maxDepth) {
      return;
    }
    let entries;
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (error) {
      warnings.push(`scan skipped unreadable directory ${dir}: ${error instanceof Error ? error.message : String(error)}`);
      return;
    }
    for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      if (!entry.isDirectory() || SKILL_IMPORT_SKIP_DIRS.has(entry.name) || entry.name.startsWith(".")) {
        continue;
      }
      const child = join(dir, entry.name);
      const childInfo = await lstat(child).catch(() => null);
      if (!childInfo || childInfo.isSymbolicLink() || !childInfo.isDirectory()) {
        continue;
      }
      await walk(child, depth + 1);
      if (truncated) {
        break;
      }
    }
  }
  await walk(absoluteRoot, 0);
  return { roots: [...new Set(discovered)].sort((a, b) => a.localeCompare(b)), warnings };
}

async function existingSharedSkillFingerprints(cfg: BrainstackConfig, args: ParsedArgs): Promise<{ repo: string; fingerprints: Map<string, string>; warnings: string[] }> {
  const repo = absWithHome(requireFlagValue(args, "repo") || cfg.client.localPath || "~/shared-brain", cfg.paths.home);
  const warnings: string[] = [];
  const fingerprints = new Map<string, string>();
  if (!existsSync(repo)) {
    warnings.push(`shared-brain clone not found for existing-skill comparison: ${repo}`);
    return { repo, fingerprints, warnings };
  }
  try {
    for (const entry of chooseLatestSkillPackages(await discoverSkillPackages(repo))) {
      fingerprints.set(entry.package.name, skillFilesFingerprint(entry.package.files));
    }
  } catch (error) {
    warnings.push(`could not inspect existing shared skill packages: ${error instanceof Error ? error.message : String(error)}`);
  }
  return { repo, fingerprints, warnings };
}

async function buildSkillImportCandidate(
  root: string,
  sources: string[],
  priority: number,
  options: { maxBytes: number; maxFiles: number; maxFileBytes: number; existingFingerprints: Map<string, string> }
): Promise<SkillImportScanCandidate> {
  const skillText = await readFile(join(root, "SKILL.md"), "utf8");
  const metadata = parseSkillMetadata(skillText, basename(root));
  const files = await collectSkillPackageFiles(root, options);
  const fingerprint = skillFilesFingerprint(files);
  const existing = options.existingFingerprints.get(metadata.name);
  const action = existing === undefined ? "install" : existing === fingerprint ? "already-current" : "update";
  return {
    name: metadata.name,
    description: metadata.description,
    root,
    sources,
    priority,
    file_count: files.length,
    total_bytes: files.reduce((sum, file) => sum + file.size_bytes, 0),
    fingerprint,
    action
  };
}

async function buildSkillImportPlan(cfg: BrainstackConfig, args: ParsedArgs): Promise<SkillImportPlan> {
  const targets = skillImportScanTargetsFromArgs(args);
  const roots = skillScanRoots(args, targets);
  const maxDepth = parseNonNegativeIntegerFlag(args, "max-depth", SKILL_IMPORT_SCAN_DEFAULT_MAX_DEPTH);
  const maxScanDirs = parsePositiveIntegerFlag(args, "max-scan-dirs", SKILL_IMPORT_SCAN_DEFAULT_MAX_DIRS);
  const maxBytes = parsePositiveIntegerFlag(args, "max-bytes", SKILL_IMPORT_DEFAULT_MAX_BYTES);
  const maxFiles = parsePositiveIntegerFlag(args, "max-files", SKILL_IMPORT_DEFAULT_MAX_FILES);
  const maxFileBytes = parsePositiveIntegerFlag(args, "max-file-bytes", Math.min(SKILL_IMPORT_DEFAULT_MAX_FILE_BYTES, maxBytes));
  const selectedNames = new Set(flagValues(args, "skill").map(safeSkillName));
  const existing = await existingSharedSkillFingerprints(cfg, args);
  const warnings = [...existing.warnings];
  const scanRoots = roots.map((root) => ({ source: root.source, path: root.path, exists: existsSync(root.path) }));
  const rootsByPath = new Map<string, { root: string; sources: Set<string>; priority: number }>();
  const rejected: SkillImportRejectedCandidate[] = [];
  for (const scanRoot of roots) {
    const discovered = await discoverSkillRootsInTree(scanRoot.path, { maxDepth, maxDirs: maxScanDirs, recursive: scanRoot.recursive });
    warnings.push(...discovered.warnings);
    for (const root of discovered.roots) {
      const current = rootsByPath.get(root);
      if (current) {
        current.sources.add(scanRoot.source);
        current.priority = Math.min(current.priority, scanRoot.priority);
      } else {
        rootsByPath.set(root, { root, sources: new Set([scanRoot.source]), priority: scanRoot.priority });
      }
    }
  }
  const candidates: SkillImportScanCandidate[] = [];
  for (const entry of [...rootsByPath.values()].sort((a, b) => a.priority - b.priority || a.root.localeCompare(b.root))) {
    try {
      const candidate = await buildSkillImportCandidate(entry.root, [...entry.sources].sort(), entry.priority, {
        maxBytes,
        maxFiles,
        maxFileBytes,
        existingFingerprints: existing.fingerprints
      });
      if (!selectedNames.size || selectedNames.has(candidate.name)) {
        candidates.push(candidate);
      }
    } catch (error) {
      rejected.push({
        root: entry.root,
        sources: [...entry.sources].sort(),
        error: error instanceof Error ? error.message : String(error)
      });
    }
  }
  const byName = new Map<string, SkillImportScanCandidate[]>();
  for (const candidate of candidates.sort((a, b) => a.name.localeCompare(b.name) || a.priority - b.priority || a.root.localeCompare(b.root))) {
    byName.set(candidate.name, [...(byName.get(candidate.name) || []), candidate]);
  }
  const proposed: SkillImportScanCandidate[] = [];
  const skipped: Array<SkillImportScanCandidate & { reason: string }> = [];
  const force = hasFlag(args, "force");
  for (const [name, group] of [...byName.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
    const [winner, ...duplicates] = group.sort((a, b) => a.priority - b.priority || a.root.localeCompare(b.root));
    if (!winner) {
      continue;
    }
    if (winner.action === "already-current" && !force) {
      skipped.push({ ...winner, reason: "already in shared brain with matching file content" });
    } else {
      proposed.push(winner);
    }
    for (const duplicate of duplicates) {
      skipped.push({ ...duplicate, reason: `duplicate skill name; selected ${name} from ${winner.root}` });
    }
  }
  return {
    note: "Proposed skills are global shared-brain imports; after apply, every connected harness can refresh and install them.",
    apply: hasFlag(args, "apply") || hasFlag(args, "yes"),
    repo: existing.repo,
    scan_roots: scanRoots,
    proposed,
    skipped: skipped.sort((a, b) => a.name.localeCompare(b.name) || a.root.localeCompare(b.root)),
    rejected: rejected.sort((a, b) => a.root.localeCompare(b.root)),
    warnings,
    applied: []
  };
}

function formatSkillImportCandidate(candidate: SkillImportScanCandidate): string {
  const description = candidate.description ? ` - ${candidate.description}` : "";
  return `  ${candidate.name} [${candidate.action}] ${candidate.root}${description} files=${candidate.file_count} bytes=${candidate.total_bytes} sources=${candidate.sources.join(",")}`;
}

function formatSkillImportPlan(plan: SkillImportPlan): string {
  const lines = [
    "Shared skill import plan",
    plan.note,
    `shared_brain_repo=${plan.repo}`,
    "",
    "Scan roots:",
    ...plan.scan_roots.map((root) => `  ${root.source}: ${root.path}${root.exists ? "" : " (missing)"}`),
    "",
    "Proposed global imports:",
    ...(plan.proposed.length ? plan.proposed.map(formatSkillImportCandidate) : ["  (none)"])
  ];
  if (plan.skipped.length) {
    lines.push("", "Skipped:", ...plan.skipped.map((candidate) => `${formatSkillImportCandidate(candidate)} reason=${candidate.reason}`));
  }
  if (plan.rejected.length) {
    lines.push("", "Rejected:", ...plan.rejected.map((candidate) => `  ${candidate.root} sources=${candidate.sources.join(",")} error=${candidate.error}`));
  }
  if (plan.warnings.length) {
    lines.push("", "Warnings:", ...plan.warnings.map((warning) => `  ${warning}`));
  }
  if (plan.applied.length) {
    lines.push("", `Applied imports: ${plan.applied.join(", ")}`);
  } else if (!plan.apply) {
    lines.push("", "No imports were written. Rerun with --apply to enqueue the proposed global shared-brain skill imports.");
  }
  return lines.join("\n");
}

async function commandImportSkills(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
  const plan = await buildSkillImportPlan(cfg, args);
  if (plan.apply) {
    const maxBytes = parsePositiveIntegerFlag(args, "max-bytes", SKILL_IMPORT_DEFAULT_MAX_BYTES);
    const maxFiles = parsePositiveIntegerFlag(args, "max-files", SKILL_IMPORT_DEFAULT_MAX_FILES);
    const maxFileBytes = parsePositiveIntegerFlag(args, "max-file-bytes", Math.min(SKILL_IMPORT_DEFAULT_MAX_FILE_BYTES, maxBytes));
    for (const candidate of plan.proposed) {
      const pkg = await buildLocalSkillPackage(candidate.root, { maxBytes, maxFiles, maxFileBytes });
      await postBrainWriteOrQueue(cfg, "import", {
        title: `Skill import: ${pkg.name}`,
        text: `${JSON.stringify(pkg, null, 2)}\n`,
        source_harness: requireFlagValue(args, "source-harness") || cfg.harness.name,
        source_machine: requireFlagValue(args, "source-machine") || cfg.machine.name,
        source_type: "skill",
        tags: ["brainstack", "brainstack-skill", `skill:${pkg.name}`]
      });
      plan.applied.push(pkg.name);
    }
  }
  if (hasFlag(args, "json")) {
    console.log(JSON.stringify(plan, null, 2));
  } else {
    console.log(formatSkillImportPlan(plan));
  }
}

function githubSkillUrl(input: string): { kind: "raw"; url: string } | { kind: "git"; remote: string; ref?: string; subdir?: string } | null {
  let parsed: URL;
  try {
    parsed = new URL(input);
  } catch {
    return null;
  }
  if (parsed.hostname !== "github.com") {
    return null;
  }
  const parts = parsed.pathname.split("/").filter(Boolean);
  if (parts.length < 2) {
    return null;
  }
  const [owner, repo, mode, ref, ...rest] = parts;
  const remote = `https://github.com/${owner}/${repo.replace(/\.git$/, "")}.git`;
  if (mode === "blob" && ref && rest.length) {
    return { kind: "raw", url: `https://raw.githubusercontent.com/${owner}/${repo}/${ref}/${rest.join("/")}` };
  }
  if (mode === "tree" && ref) {
    return { kind: "git", remote, ref, subdir: rest.join("/") || undefined };
  }
  if (!mode || mode === undefined) {
    return { kind: "git", remote };
  }
  return null;
}

function gitSkillUrl(input: string): { remote: string; ref?: string; subdir?: string } | null {
  const github = githubSkillUrl(input);
  if (github?.kind === "git") {
    return github;
  }
  if (/^git@[^:]+:[^/].+/.test(input) || /^ssh:\/\//i.test(input) || /^git:\/\//i.test(input) || /\.git(?:[#?].*)?$/i.test(input)) {
    return { remote: input };
  }
  return null;
}

function skillIpv4ToNumber(address: string): number {
  return address.split(".").reduce((sum, part) => (sum << 8) + Number(part), 0) >>> 0;
}

function skillInCidr4(address: string, base: string, bits: number): boolean {
  const mask = bits === 0 ? 0 : (0xffffffff << (32 - bits)) >>> 0;
  return (skillIpv4ToNumber(address) & mask) === (skillIpv4ToNumber(base) & mask);
}

function isBlockedSkillUrlIp(address: string): boolean {
  const version = isIP(address);
  if (version === 4) {
    const blockedRanges: Array<[string, number]> = [
      ["0.0.0.0", 8],
      ["10.0.0.0", 8],
      ["100.64.0.0", 10],
      ["127.0.0.0", 8],
      ["169.254.0.0", 16],
      ["172.16.0.0", 12],
      ["192.168.0.0", 16]
    ];
    return blockedRanges.some(([base, bits]) => skillInCidr4(address, base, bits));
  }
  if (version === 6) {
    const normalized = address.toLowerCase();
    const ipv4Mapped = normalized.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/);
    if (ipv4Mapped) {
      return isBlockedSkillUrlIp(ipv4Mapped[1]);
    }
    return normalized === "::1" || normalized.startsWith("fe80:") || normalized.startsWith("fc") || normalized.startsWith("fd");
  }
  return false;
}

interface SafeSkillHttpTarget {
  url: URL;
  address: string;
  family: 4 | 6;
}

async function safeSkillHttpTarget(input: string, allowPrivateUrl: boolean): Promise<SafeSkillHttpTarget> {
  let url: URL;
  try {
    url = new URL(input);
  } catch {
    throw new Error("skill URL import requires a valid URL");
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("skill URL import only allows http and https for raw file fetches");
  }
  const hostnameValue = url.hostname.replace(/^\[|\]$/g, "").toLowerCase();
  if (!allowPrivateUrl && (hostnameValue === "localhost" || hostnameValue.endsWith(".localhost"))) {
    throw new Error("skill URL import blocked localhost hostname; rerun with --allow-private-url only for trusted private fetches");
  }
  const directIpVersion = isIP(hostnameValue);
  const addresses = directIpVersion
    ? [{ address: hostnameValue, family: directIpVersion }]
    : await lookup(hostnameValue, { all: true, verbatim: true });
  if (!addresses.length) {
    throw new Error("skill URL import DNS lookup returned no addresses");
  }
  if (!allowPrivateUrl) {
    const blocked = addresses.find((entry) => isBlockedSkillUrlIp(entry.address));
    if (blocked) {
      throw new Error(`skill URL import blocked private address ${blocked.address}; rerun with --allow-private-url only for trusted private fetches`);
    }
  }
  const selected = addresses[0];
  return { url, address: selected.address, family: selected.family === 6 ? 6 : 4 };
}

async function fetchSkillUrlText(input: string, options: { maxBytes: number; allowPrivateUrl: boolean; redirectsLeft?: number }): Promise<{ url: string; text: string }> {
  const redirectsLeft = options.redirectsLeft ?? 5;
  const target = await safeSkillHttpTarget(input, options.allowPrivateUrl);
  const response = await new Promise<{ statusCode: number; statusMessage: string; headers: Record<string, string | string[] | undefined>; bytes: Buffer }>((resolvePromise, rejectPromise) => {
    const { url, address, family } = target;
    const requestImpl = url.protocol === "https:" ? httpsRequest : httpRequest;
    const req = requestImpl(
      {
        protocol: url.protocol,
        hostname: url.hostname,
        port: url.port || (url.protocol === "https:" ? 443 : 80),
        path: `${url.pathname}${url.search}`,
        method: "GET",
        headers: {
          Host: url.host,
          "User-Agent": "brainstack-skill-url-import"
        },
        servername: url.hostname,
        lookup(_hostname, _options, callback) {
          callback(null, address, family);
        },
        timeout: 20_000
      },
      (res) => {
        const length = Number(res.headers["content-length"] || 0);
        if (length > options.maxBytes) {
          res.destroy();
          rejectPromise(new Error(`skill URL content is too large: ${length} bytes > ${options.maxBytes}`));
          return;
        }
        const chunks: Buffer[] = [];
        let total = 0;
        res.on("data", (chunk: Buffer) => {
          total += chunk.byteLength;
          if (total > options.maxBytes) {
            const error = new Error(`skill URL streamed content is too large: ${total} bytes > ${options.maxBytes}`);
            rejectPromise(error);
            res.destroy(error);
            return;
          }
          chunks.push(chunk);
        });
        res.on("end", () => resolvePromise({ statusCode: res.statusCode || 0, statusMessage: res.statusMessage || "", headers: res.headers, bytes: Buffer.concat(chunks) }));
      }
    );
    req.on("timeout", () => req.destroy(new Error("skill URL fetch timed out")));
    req.on("error", rejectPromise);
    req.end();
  });
  if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
    if (redirectsLeft <= 0) {
      throw new Error("skill URL import exceeded redirect limit");
    }
    const location = Array.isArray(response.headers.location) ? response.headers.location[0] : response.headers.location;
    return await fetchSkillUrlText(new URL(location || "", target.url).toString(), {
      maxBytes: options.maxBytes,
      allowPrivateUrl: options.allowPrivateUrl,
      redirectsLeft: redirectsLeft - 1
    });
  }
  if (response.statusCode < 200 || response.statusCode >= 300) {
    throw new Error(`skill URL fetch failed with HTTP ${response.statusCode}: ${target.url.toString()}`);
  }
  return { url: target.url.toString(), text: new TextDecoder().decode(response.bytes) };
}

async function buildSingleFileSkillPackageFromUrl(input: string, url: string, options: { maxBytes: number; allowPrivateUrl: boolean }): Promise<SkillPackage> {
  const fetched = await fetchSkillUrlText(url, options);
  const text = fetched.text;
  const bytes = new TextEncoder().encode(text);
  const metadata = parseSkillMetadata(text, basename(new URL(fetched.url).pathname) === "SKILL.md" ? basename(dirname(new URL(fetched.url).pathname)) || "skill" : "skill");
  const file: SkillPackageFile = {
    path: "SKILL.md",
    encoding: "utf8",
    content: text,
    size_bytes: bytes.byteLength,
    sha256: createHash("sha256").update(bytes).digest("hex")
  };
  const base: Omit<SkillPackage, "package_sha256"> = {
    schema_version: 1,
    kind: BRAINSTACK_SKILL_PACKAGE_KIND,
    name: metadata.name,
    description: metadata.description,
    imported_at: new Date().toISOString(),
    source: {
      kind: "url",
      input,
      url: fetched.url
    },
    files: [file]
  };
  return { ...base, package_sha256: skillPackageHash(base) };
}

/**
 * Git skill remotes must pass the same private-network guard as raw HTTP imports, and
 * SSH/git-protocol remotes can invoke local SSH credentials, so they need an explicit
 * opt-in even for public hosts.
 */
async function assertSafeGitSkillRemote(remote: string, options: { allowPrivateUrl: boolean; allowSshGit: boolean }): Promise<void> {
  const trimmed = remote.trim();
  if (!trimmed || trimmed.startsWith("-")) {
    throw new Error(`skill git import remote is not a valid remote: ${trimmed.slice(0, 120)}`);
  }
  if (/[\u0000-\u001f]/.test(trimmed)) {
    throw new Error("skill git import remote contains control characters");
  }
  let protocol: string;
  let host: string;
  const scpLike = trimmed.match(/^git@([^:/]+):/);
  if (scpLike) {
    protocol = "ssh";
    host = scpLike[1];
  } else {
    let parsed: URL;
    try {
      parsed = new URL(trimmed);
    } catch {
      throw new Error(`skill git import remote is not a valid URL: ${trimmed.slice(0, 120)}`);
    }
    protocol = parsed.protocol.replace(/:$/, "").toLowerCase();
    host = parsed.hostname.replace(/^\[|\]$/g, "").toLowerCase();
  }
  if (protocol === "file") {
    // Local repositories are operator-owned input; the private-network guard does not apply.
    return;
  }
  if (!["https", "http", "ssh", "git"].includes(protocol)) {
    throw new Error(`skill git import does not allow ${protocol}:// remotes`);
  }
  if ((protocol === "ssh" || protocol === "git") && !options.allowSshGit) {
    throw new Error("skill git import over ssh/git protocols can use local SSH credentials; rerun with --allow-ssh-git only for trusted remotes, or use an https remote");
  }
  if (!options.allowPrivateUrl) {
    if (!host || host === "localhost" || host.endsWith(".localhost")) {
      throw new Error("skill git import blocked localhost remote; rerun with --allow-private-url only for trusted private fetches");
    }
    const directIpVersion = isIP(host);
    const addresses = directIpVersion ? [{ address: host, family: directIpVersion }] : await lookup(host, { all: true, verbatim: true });
    if (!addresses.length) {
      throw new Error("skill git import DNS lookup returned no addresses");
    }
    const blocked = addresses.find((entry) => isBlockedSkillUrlIp(entry.address));
    if (blocked) {
      throw new Error(`skill git import blocked private address ${blocked.address}; rerun with --allow-private-url only for trusted private fetches`);
    }
  }
}

async function buildUrlSkillPackage(input: string, options: { maxBytes: number; maxFiles: number; maxFileBytes: number; allowPrivateUrl: boolean; allowSshGit: boolean }): Promise<SkillPackage> {
  const github = githubSkillUrl(input);
  if (github?.kind === "raw") {
    return await buildSingleFileSkillPackageFromUrl(input, github.url, { maxBytes: Math.min(options.maxBytes, options.maxFileBytes), allowPrivateUrl: options.allowPrivateUrl });
  }
  const gitSource = gitSkillUrl(input);
  if (!gitSource) {
    return await buildSingleFileSkillPackageFromUrl(input, input, { maxBytes: Math.min(options.maxBytes, options.maxFileBytes), allowPrivateUrl: options.allowPrivateUrl });
  }
  await assertSafeGitSkillRemote(gitSource.remote, { allowPrivateUrl: options.allowPrivateUrl, allowSshGit: options.allowSshGit });
  const tempRoot = await mkdtemp(join(tmpdir(), "brainstack-skill-url-"));
  try {
    const checkout = join(tempRoot, "checkout");
    const cloneArgs = ["git", ...safeGitProtocolArgs(gitSource.remote), "clone", "--depth", "1"];
    if (!options.allowPrivateUrl) {
      // The pre-clone DNS guard checks only the original host; refuse HTTP redirects
      // so the clone cannot be bounced to a private target after the check.
      cloneArgs.splice(1, 0, "-c", "http.followRedirects=false");
    }
    if (gitSource.ref) {
      if (gitSource.ref.startsWith("-")) {
        throw new Error(`skill git import ref is not a valid ref: ${gitSource.ref.slice(0, 120)}`);
      }
      cloneArgs.push("--branch", gitSource.ref);
    }
    cloneArgs.push("--", gitSource.remote, checkout);
    run(cloneArgs, { check: true, timeoutMs: 60_000, env: safeGitProtocolEnv(gitSource.remote) });
    const root = gitSource.subdir ? join(checkout, gitSource.subdir) : checkout;
    const packageRoot = (await resolveLocalSkillRoot(root)).root;
    const skillText = await readFile(join(packageRoot, "SKILL.md"), "utf8");
    const metadata = parseSkillMetadata(skillText, basename(packageRoot));
    const files = await collectSkillPackageFiles(packageRoot, options);
    const base: Omit<SkillPackage, "package_sha256"> = {
      schema_version: 1,
      kind: BRAINSTACK_SKILL_PACKAGE_KIND,
      name: metadata.name,
      description: metadata.description,
      imported_at: new Date().toISOString(),
      source: {
        kind: "url",
        input,
        remote: gitSource.remote,
        ref: gitSource.ref,
        subdir: gitSource.subdir,
        commit: gitOutput(["rev-parse", "HEAD"], checkout) || undefined
      },
      files
    };
    return { ...base, package_sha256: skillPackageHash(base) };
  } finally {
    await rm(tempRoot, { recursive: true, force: true });
  }
}

function validateSkillPackage(value: unknown, label: string): SkillPackage {
  if (!value || typeof value !== "object") {
    throw new Error(`invalid skill package ${label}: expected object`);
  }
  const pkg = value as SkillPackage;
  if (pkg.schema_version !== 1 || pkg.kind !== BRAINSTACK_SKILL_PACKAGE_KIND) {
    throw new Error(`invalid skill package ${label}: unsupported schema or kind`);
  }
  pkg.name = safeSkillName(String(pkg.name || ""));
  if (!Array.isArray(pkg.files) || !pkg.files.length) {
    throw new Error(`invalid skill package ${label}: files are required`);
  }
  for (const file of pkg.files) {
    file.path = validateSkillRelativePath(String(file.path || ""));
    if (file.encoding !== "utf8" && file.encoding !== "base64") {
      throw new Error(`invalid skill package ${label}: unsupported file encoding for ${file.path}`);
    }
    if (typeof file.content !== "string" || typeof file.sha256 !== "string") {
      throw new Error(`invalid skill package ${label}: invalid file content metadata for ${file.path}`);
    }
    const bytes = file.encoding === "utf8" ? new TextEncoder().encode(file.content) : Buffer.from(file.content, "base64");
    const actualSha = createHash("sha256").update(bytes).digest("hex");
    if (actualSha !== file.sha256) {
      throw new Error(`invalid skill package ${label}: sha256 mismatch for ${file.path}`);
    }
  }
  if (!pkg.files.some((file) => file.path === "SKILL.md")) {
    throw new Error(`invalid skill package ${label}: missing SKILL.md`);
  }
  if (pkg.package_sha256 && pkg.package_sha256 !== skillPackageHash(pkg)) {
    throw new Error(`invalid skill package ${label}: package sha256 mismatch`);
  }
  return pkg;
}

async function commandImport(args: ParsedArgs): Promise<void> {
  const subcommand = args.positional[0] || "help";
  if (subcommand === "skills") {
    return await commandImportSkills(args);
  }
  if (subcommand === "codex-session") {
    return await commandImportCodexSession(args);
  }
  if (subcommand !== "skill") {
    if (subcommand === "help" || subcommand === "--help" || subcommand === "-h") {
      console.log("Usage: brainctl import skill <SKILL.md|DIR|URL> --config brainstack.yaml [--title TITLE] [--source-harness HARNESS] [--source-machine MACHINE] [--max-bytes N] [--max-files N] [--allow-private-url] [--allow-ssh-git]\n       brainctl import skills [--config brainstack.yaml] [--target codex|claude|cursor|all] [--scan-dir DIR] [--skill NAME] [--apply] [--json]\n       brainctl import codex-session <SESSION_ID|JSONL_PATH> [--config brainstack.yaml] [--include-transcript] [--max-bytes N] [--dry-run] [--json]");
      return;
    }
    throw new Error(`Unknown import subcommand: ${subcommand}`);
  }
  const source = args.positional[1] || requireFlagValue(args, "source");
  if (!source) {
    throw new Error("import skill requires a SKILL.md path, skill directory, or URL");
  }
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
  const maxBytes = parsePositiveIntegerFlag(args, "max-bytes", SKILL_IMPORT_DEFAULT_MAX_BYTES);
  const maxFiles = parsePositiveIntegerFlag(args, "max-files", SKILL_IMPORT_DEFAULT_MAX_FILES);
  const maxFileBytes = parsePositiveIntegerFlag(args, "max-file-bytes", Math.min(SKILL_IMPORT_DEFAULT_MAX_FILE_BYTES, maxBytes));
  const allowPrivateUrl = hasFlag(args, "allow-private-url");
  const allowSshGit = hasFlag(args, "allow-ssh-git");
  const pkg = isUrlLikeSkillSource(source)
    ? await buildUrlSkillPackage(source, { maxBytes, maxFiles, maxFileBytes, allowPrivateUrl, allowSshGit })
    : await buildLocalSkillPackage(source, { maxBytes, maxFiles, maxFileBytes });
  validateSkillPackage(pkg, source);
  const title = requireFlagValue(args, "title") || `Skill import: ${pkg.name}`;
  await postBrainWriteOrQueue(cfg, "import", {
    title,
    text: `${JSON.stringify(pkg, null, 2)}\n`,
    source_harness: requireFlagValue(args, "source-harness") || cfg.harness.name,
    source_machine: requireFlagValue(args, "source-machine") || cfg.machine.name,
    source_type: "skill",
    tags: ["brainstack", "brainstack-skill", `skill:${pkg.name}`]
  });
  console.log(`skill_package=${pkg.name} files=${pkg.files.length} sha256=${pkg.package_sha256}`);
}

function safeRepoRelativeFile(repo: string, repoPath: string): string {
  const normalized = validateSkillRelativePath(repoPath);
  const absolute = resolve(repo, normalized);
  const root = resolve(repo);
  if (absolute !== root && !absolute.startsWith(`${root}${sep}`)) {
    throw new Error(`repo path escapes clone: ${repoPath}`);
  }
  return absolute;
}

interface CodexSessionSummary {
  id: string;
  threadName?: string;
  path: string;
  bytes: number;
  sha256: string;
  startedAt?: string;
  updatedAt?: string;
  cwd?: string;
  originator?: string;
  cliVersion?: string;
  source?: string;
  modelProvider?: string;
  lastAgentMessage?: string;
}

function codexHomePath(): string {
  return process.env.CODEX_HOME ? abs(process.env.CODEX_HOME) : process.env.HOME ? join(abs(process.env.HOME), ".codex") : ".codex";
}

function isCodexSessionId(value: string): boolean {
  return /^[0-9a-f]{8,}-[0-9a-f-]{20,}$/i.test(value.trim());
}

async function findCodexSessionById(sessionId: string, codexHome: string): Promise<string | null> {
  const roots = [join(codexHome, "sessions"), join(codexHome, "archived_sessions")];
  const maxFiles = 25_000;
  const maxDirs = 4_000;
  const candidates: Array<{ path: string; mtimeMs: number }> = [];
  let files = 0;
  let dirs = 0;
  for (const root of roots) {
    if (!existsSync(root)) {
      continue;
    }
    const stack = [root];
    while (stack.length) {
      const dir = stack.pop()!;
      dirs += 1;
      if (dirs > maxDirs) {
        throw new Error(`Codex session search exceeded ${maxDirs} directories under ${codexHome}; pass the JSONL path explicitly`);
      }
      const info = await lstat(dir).catch(() => null);
      if (!info?.isDirectory() || info.isSymbolicLink()) {
        continue;
      }
      for (const name of await readdir(dir).catch(() => [])) {
        const path = join(dir, name);
        const entryInfo = await lstat(path).catch(() => null);
        if (!entryInfo || entryInfo.isSymbolicLink()) {
          continue;
        }
        if (entryInfo.isDirectory()) {
          stack.push(path);
          continue;
        }
        if (!entryInfo.isFile()) {
          continue;
        }
        files += 1;
        if (files > maxFiles) {
          throw new Error(`Codex session search exceeded ${maxFiles} files under ${codexHome}; pass the JSONL path explicitly`);
        }
        if (name.endsWith(".jsonl") && name.includes(sessionId)) {
          candidates.push({ path, mtimeMs: entryInfo.mtimeMs });
        }
      }
    }
  }
  candidates.sort((a, b) => b.mtimeMs - a.mtimeMs || b.path.localeCompare(a.path));
  return candidates[0]?.path || null;
}

async function codexThreadName(sessionId: string, codexHome: string): Promise<string | undefined> {
  const indexPath = join(codexHome, "session_index.jsonl");
  if (!existsSync(indexPath)) {
    return undefined;
  }
  const indexInfo = await lstat(indexPath).catch(() => null);
  if (!indexInfo?.isFile() || indexInfo.isSymbolicLink() || indexInfo.size > 10 * 1024 * 1024) {
    return undefined;
  }
  let latestName: string | undefined;
  for (const line of (await readFile(indexPath, "utf8")).split(/\r?\n/)) {
    if (!line.includes(sessionId)) {
      continue;
    }
    try {
      const parsed = JSON.parse(line) as Record<string, unknown>;
      const name = typeof parsed.thread_name === "string" ? parsed.thread_name.trim() : "";
      latestName = name || latestName;
    } catch {
      continue;
    }
  }
  return latestName;
}

function cappedText(value: unknown, maxChars: number): string | undefined {
  if (typeof value !== "string" || !value.trim()) {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed.length > maxChars ? `${trimmed.slice(0, maxChars)}\n[truncated]` : trimmed;
}

async function resolveCodexSessionPath(reference: string, codexHome: string): Promise<string> {
  const expanded = expandHome(reference);
  if (existsSync(expanded)) {
    return abs(expanded);
  }
  if (!isCodexSessionId(reference)) {
    throw new Error(`Codex session reference is neither an existing path nor a session id: ${reference}`);
  }
  const found = await findCodexSessionById(reference, codexHome);
  if (!found) {
    throw new Error(`Codex session not found: ${reference}. Pass the JSONL path explicitly or verify ${codexHome}/session_index.jsonl.`);
  }
  return found;
}

async function readCodexSessionSummary(reference: string, maxBytes: number): Promise<{ summary: CodexSessionSummary; transcript?: string }> {
  const codexHome = codexHomePath();
  const path = await resolveCodexSessionPath(reference, codexHome);
  const info = await lstat(path);
  if (!info.isFile() || info.isSymbolicLink()) {
    throw new Error(`Codex session path must be a regular file: ${path}`);
  }
  if (info.size > maxBytes) {
    throw new Error(`Codex session is too large: ${info.size} bytes > --max-bytes ${maxBytes}`);
  }
  const transcript = await readFile(path, "utf8");
  const summary: CodexSessionSummary = {
    id: reference,
    path,
    bytes: info.size,
    sha256: createHash("sha256").update(transcript).digest("hex"),
    updatedAt: info.mtime.toISOString()
  };
  for (const line of transcript.split(/\r?\n/)) {
    if (!line.trim()) {
      continue;
    }
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(line) as Record<string, unknown>;
    } catch {
      continue;
    }
    const payload = parsed.payload && typeof parsed.payload === "object" && !Array.isArray(parsed.payload) ? (parsed.payload as Record<string, unknown>) : {};
    if (parsed.type === "session_meta") {
      summary.id = typeof payload.id === "string" ? payload.id : summary.id;
      summary.startedAt = typeof payload.timestamp === "string" ? payload.timestamp : summary.startedAt;
      summary.cwd = typeof payload.cwd === "string" ? payload.cwd : summary.cwd;
      summary.originator = typeof payload.originator === "string" ? payload.originator : summary.originator;
      summary.cliVersion = typeof payload.cli_version === "string" ? payload.cli_version : summary.cliVersion;
      summary.source = typeof payload.source === "string" ? payload.source : summary.source;
      summary.modelProvider = typeof payload.model_provider === "string" ? payload.model_provider : summary.modelProvider;
    }
    if (payload.type === "task_complete") {
      summary.lastAgentMessage = cappedText(payload.last_agent_message, 8_000) || summary.lastAgentMessage;
    }
  }
  summary.threadName = await codexThreadName(summary.id, codexHome);
  return { summary, transcript };
}

function codexSessionImportText(summary: CodexSessionSummary, transcript: string | undefined, includeTranscript: boolean): string {
  const lines = [
    `# Codex session ${summary.id}`,
    "",
    `- Session id: \`${summary.id}\``,
    summary.threadName ? `- Thread: ${summary.threadName}` : "",
    summary.cwd ? `- Cwd: \`${summary.cwd}\`` : "",
    summary.startedAt ? `- Started: \`${summary.startedAt}\`` : "",
    summary.updatedAt ? `- Updated: \`${summary.updatedAt}\`` : "",
    summary.originator ? `- Originator: \`${summary.originator}\`` : "",
    summary.cliVersion ? `- CLI version: \`${summary.cliVersion}\`` : "",
    summary.source ? `- Source: \`${summary.source}\`` : "",
    summary.modelProvider ? `- Model provider: \`${summary.modelProvider}\`` : "",
    `- Local transcript path: \`${summary.path}\``,
    `- Transcript bytes: \`${summary.bytes}\``,
    `- Transcript sha256: \`${summary.sha256}\``,
    "",
    "## Last Agent Message",
    "",
    summary.lastAgentMessage || "No task-complete final message was found in the imported JSONL.",
    "",
    "## Capture Note",
    "",
    includeTranscript
      ? "The transcript below was explicitly included by the importing operator."
      : "This import is a bounded session checkpoint. Re-import with `brainctl import codex-session ... --include-transcript` when full raw evidence is needed.",
    ""
  ].filter(Boolean);
  if (includeTranscript && transcript !== undefined) {
    lines.push("## Transcript", "", "```jsonl", transcript.trimEnd(), "```", "");
  }
  return lines.join("\n");
}

async function codexSessionImportPayload(
  cfg: BrainstackConfig,
  reference: string,
  args: ParsedArgs,
  options: { includeTranscript: boolean }
): Promise<Record<string, unknown>> {
  const maxBytes = parsePositiveIntegerFlag(args, "max-bytes", options.includeTranscript ? 20 * 1024 * 1024 : 5 * 1024 * 1024);
  const { summary, transcript } = await readCodexSessionSummary(reference, maxBytes);
  const title = requireFlagValue(args, "title") || `Codex session: ${summary.threadName || summary.id}`;
  const text = codexSessionImportText(summary, transcript, options.includeTranscript);
  const tags = uniqueNonEmptyStrings([
    "codex-session",
    options.includeTranscript ? "codex-transcript" : "codex-session-checkpoint",
    `session:${summary.id}`,
    summary.cwd ? `repo:${deriveProjectLabel(summary.cwd)}` : undefined,
    ...flagValues(args, "tags")
  ]);
  return {
    title,
    text,
    source_harness: requireFlagValue(args, "source-harness") || cfg.harness.name,
    source_machine: requireFlagValue(args, "source-machine") || cfg.machine.name,
    source_type: options.includeTranscript ? "codex-session-transcript" : "codex-session-checkpoint",
    conversation_id: summary.id,
    related_repo: summary.cwd,
    transcript_path: summary.path,
    transcript_bytes: summary.bytes,
    transcript_sha256: summary.sha256,
    started_at: summary.startedAt,
    updated_at: summary.updatedAt,
    tags
  };
}

async function commandImportCodexSession(args: ParsedArgs): Promise<void> {
  const reference = args.positional[1] || requireFlagValue(args, "session") || requireFlagValue(args, "source");
  if (!reference) {
    throw new Error("import codex-session requires a session id or JSONL path");
  }
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
  const includeTranscript = hasFlag(args, "include-transcript");
  const payload = await codexSessionImportPayload(cfg, reference, args, { includeTranscript });
  if (hasFlag(args, "json") || hasFlag(args, "dry-run")) {
    console.log(JSON.stringify({ dry_run: hasFlag(args, "dry-run"), payload }, null, 2));
    if (hasFlag(args, "dry-run")) {
      return;
    }
  }
  await postBrainWriteOrQueue(cfg, "import", payload);
}

async function discoverSkillPackages(repo: string): Promise<SkillPackageWithManifest[]> {
  const manifestDir = join(repo, "manifests", "sources");
  if (!existsSync(manifestDir)) {
    return [];
  }
  const packages: SkillPackageWithManifest[] = [];
  for (const entry of await readdir(manifestDir)) {
    if (!entry.endsWith(".json")) {
      continue;
    }
    const manifestPath = join(manifestDir, entry);
    let manifest: Record<string, unknown>;
    try {
      manifest = JSON.parse(await readFile(manifestPath, "utf8")) as Record<string, unknown>;
    } catch {
      continue;
    }
    if (manifest.source_type !== "skill") {
      continue;
    }
    const rawPath = typeof manifest.raw_path === "string" ? manifest.raw_path : "";
    if (!rawPath) {
      continue;
    }
    try {
      const rawAbsolute = safeRepoRelativeFile(repo, rawPath);
      const rawInfo = await lstat(rawAbsolute);
      if (rawInfo.isSymbolicLink() || !rawInfo.isFile()) {
        continue;
      }
      const pkg = validateSkillPackage(JSON.parse(await readFile(rawAbsolute, "utf8")), rawPath);
      packages.push({
        manifest_path: relative(repo, manifestPath).split(sep).join("/"),
        raw_path: rawPath,
        created_at: String(manifest.created_at || pkg.imported_at || ""),
        package: pkg
      });
    } catch {
      continue;
    }
  }
  return packages.sort((a, b) => `${a.package.name}\0${a.created_at}`.localeCompare(`${b.package.name}\0${b.created_at}`));
}

function chooseLatestSkillPackages(packages: SkillPackageWithManifest[]): SkillPackageWithManifest[] {
  const latest = new Map<string, SkillPackageWithManifest>();
  for (const entry of packages) {
    const current = latest.get(entry.package.name);
    if (!current || entry.created_at.localeCompare(current.created_at) >= 0) {
      latest.set(entry.package.name, entry);
    }
  }
  return [...latest.values()].sort((a, b) => a.package.name.localeCompare(b.package.name));
}

async function installSkillPackage(pkg: SkillPackage, root: string, options: { force: boolean }): Promise<"installed" | "skipped"> {
  const skillName = safeSkillName(pkg.name);
  const skillDir = join(root, skillName);
  const marker = join(skillDir, ".brainstack-skill-package.json");
  const existing = await lstat(skillDir).catch(() => null);
  if (existing) {
    if (!existing.isDirectory() || existing.isSymbolicLink()) {
      throw new Error(`refusing to overwrite non-directory skill target: ${skillDir}`);
    }
    if (!options.force && !existsSync(marker)) {
      return "skipped";
    }
    await rm(skillDir, { recursive: true, force: true });
  }
  for (const file of pkg.files) {
    const relPath = validateSkillRelativePath(file.path);
    const target = join(skillDir, relPath);
    const targetRoot = resolve(skillDir);
    const targetAbs = resolve(target);
    if (targetAbs !== targetRoot && !targetAbs.startsWith(`${targetRoot}${sep}`)) {
      throw new Error(`skill file escapes target root: ${file.path}`);
    }
    await ensureDir(dirname(targetAbs));
    if (file.encoding === "utf8") {
      await writeText(targetAbs, file.content, 0o600);
    } else {
      const bytes = Buffer.from(file.content, "base64");
      await writeFile(targetAbs, bytes);
      await chmod(targetAbs, 0o600);
    }
  }
  await writeText(marker, `${JSON.stringify({ name: pkg.name, package_sha256: pkg.package_sha256, installed_at: new Date().toISOString() }, null, 2)}\n`, 0o600);
  return "installed";
}

async function refreshBrainstackSkillPackages(
  cfg: BrainstackConfig,
  args: ParsedArgs,
  options: { quiet?: boolean; hookMode?: boolean } = {}
): Promise<RefreshSkillResult> {
  const target = normalizeHookTarget(requireFlagValue(args, "target"), normalizeHookTarget(requireFlagValue(args, "harness"), "codex"));
  const root = skillInstallRootForTarget(target, args);
  const repo = absWithHome(requireFlagValue(args, "repo") || cfg.client.localPath || "~/shared-brain", cfg.paths.home);
  const warnings: string[] = [];
  if (!hasFlag(args, "no-sync") && existsSync(join(repo, ".git"))) {
    const pull = run(["git", "pull", "--ff-only"], { cwd: repo, check: false, timeoutMs: options.hookMode ? 1500 : 20_000 });
    if (pull.code !== 0) {
      warnings.push(`git pull skipped/failed: ${(pull.stderr || pull.stdout).replace(/\s+/g, " ").slice(0, 200)}`);
    }
  }
  const selected = new Set(flagValues(args, "skill").map(safeSkillName));
  const packages = chooseLatestSkillPackages(await discoverSkillPackages(repo)).filter((entry) => !selected.size || selected.has(entry.package.name));
  const installed: string[] = [];
  const skipped: string[] = [];
  for (const entry of packages) {
    const result = await installSkillPackage(entry.package, root, { force: hasFlag(args, "force") });
    if (result === "installed") {
      installed.push(entry.package.name);
    } else {
      skipped.push(entry.package.name);
    }
  }
  const output = { repo, target, root, installed, skipped, warnings };
  if (!options.quiet) {
    console.log(`skills_refresh target=${target} root=${root} repo=${repo}`);
    console.log(`installed=${installed.length}${installed.length ? ` ${installed.join(",")}` : ""}`);
    console.log(`skipped=${skipped.length}${skipped.length ? ` ${skipped.join(",")}` : ""}`);
    for (const warning of warnings) {
      console.warn(`WARN ${warning}`);
    }
  }
  return output;
}

async function skillDoctorRoots(target: HookTarget, args: ParsedArgs): Promise<string[]> {
  const explicit = requireFlagValue(args, "dir");
  if (explicit) {
    return [abs(explicit)];
  }
  return [skillInstallRootForTarget(target, args)];
}

async function skillDirsForDoctor(root: string): Promise<string[]> {
  if (!existsSync(root)) {
    return [];
  }
  if (existsSync(join(root, "SKILL.md"))) {
    return [root];
  }
  const dirs: string[] = [];
  for (const entry of await readdir(root, { withFileTypes: true })) {
    if (entry.isDirectory() && existsSync(join(root, entry.name, "SKILL.md"))) {
      dirs.push(join(root, entry.name));
    }
  }
  return dirs.sort();
}

function gitRemoteHeadCheck(skillDir: string, remote: string, branch: string): string {
  const head = gitOutput(["rev-parse", "HEAD"], skillDir);
  if (!head || !remote || !branch || branch === "HEAD") {
    return "remote check unavailable";
  }
  const result = run(["git", ...safeGitProtocolArgs(remote), "ls-remote", remote, `refs/heads/${branch}`], {
    cwd: skillDir,
    check: false,
    timeoutMs: 5000,
    env: safeGitProtocolEnv(remote)
  });
  if (result.code !== 0) {
    return `remote check failed: ${(result.stderr || result.stdout).replace(/\s+/g, " ").slice(0, 160)}`;
  }
  const remoteHead = result.stdout.trim().split(/\s+/)[0] || "";
  if (!remoteHead) {
    return `remote branch not found: ${branch}`;
  }
  return remoteHead === head ? "remote up to date" : `remote differs local=${head.slice(0, 12)} remote=${remoteHead.slice(0, 12)}`;
}

async function commandSkillsDoctor(args: ParsedArgs): Promise<void> {
  const target = normalizeHookTarget(requireFlagValue(args, "target"), "codex");
  const checks: SkillDoctorCheck[] = [];
  for (const root of await skillDoctorRoots(target, args)) {
    if (!existsSync(root)) {
      checks.push({ status: "WARN", skill: "(root)", path: root, detail: "skill root missing" });
      continue;
    }
    for (const skillDir of await skillDirsForDoctor(root)) {
      const skillPath = join(skillDir, "SKILL.md");
      const info = await lstat(skillPath).catch(() => null);
      if (!info || info.isSymbolicLink() || !info.isFile()) {
        checks.push({ status: "FAIL", skill: basename(skillDir), path: skillPath, detail: "SKILL.md missing or not a regular file" });
        continue;
      }
      const text = await readFile(skillPath, "utf8");
      let metadata: { name: string; description?: string };
      try {
        metadata = parseSkillMetadata(text, basename(skillDir));
      } catch (error) {
        checks.push({ status: "FAIL", skill: basename(skillDir), path: skillPath, detail: error instanceof Error ? error.message : String(error) });
        continue;
      }
      const git = await localSkillGitProvenance(skillDir);
      const dirty = git?.dirty ? " dirty" : "";
      let remoteDetail = "";
      if (hasFlag(args, "check-remote") && git?.remote && git?.branch) {
        remoteDetail = `; ${gitRemoteHeadCheck(skillDir, String(git.remote), String(git.branch))}`;
      }
      checks.push({
        status: git?.dirty ? "WARN" : "PASS",
        skill: metadata.name,
        path: skillDir,
        detail: `${metadata.description || "no description"}${dirty}${git?.remote ? `; remote=${git.remote}` : ""}${git?.commit ? `; commit=${String(git.commit).slice(0, 12)}` : ""}${remoteDetail}`
      });
    }
  }
  if (hasFlag(args, "json")) {
    console.log(JSON.stringify({ ok: !checks.some((item) => item.status === "FAIL"), checks }, null, 2));
  } else {
    console.log(
      checks.length
        ? checks.map((item) => `${item.status} ${item.skill}: ${item.detail} (${item.path})`).join("\n")
        : "WARN (root): no skills found"
    );
  }
  if (checks.some((item) => item.status === "FAIL")) {
    throw new Error("skills doctor found failing checks");
  }
}

function brainstackDefaultConfigPath(): string {
  return abs("~/.config/brainstack/brainstack.yaml");
}

function hookConfigPath(target: HookTarget): string {
  const home = process.env.HOME ? abs(process.env.HOME) : ".";
  if (target === "codex") {
    return join(home, ".codex", "hooks.json");
  }
  if (target === "claude") {
    return join(home, ".claude", "settings.json");
  }
  return join(home, ".cursor", "hooks.json");
}

function currentBrainctlHookCommand(args: ParsedArgs): string {
  const explicit = requireFlagValue(args, "brainctl");
  if (explicit) {
    if (explicit.includes(" ")) {
      return explicit;
    }
    return explicit.includes("/") || explicit.startsWith(".") || explicit.startsWith("~") ? quoteForBash(abs(explicit)) : quoteForBash(explicit);
  }
  const executable = process.execPath;
  const script = process.argv[1];
  if (script && script.endsWith(".ts")) {
    return `${quoteForBash(executable)} --no-env-file run ${quoteForBash(abs(script))}`;
  }
  return quoteForBash(executable);
}

function managedHookCommand(brainctlCommand: string, target: HookTarget, event: string, configPath: string): string {
  return `${brainctlCommand} hook run --harness ${target} --event ${event} --config ${quoteForBash(configPath)}`;
}

function isManagedBrainstackHook(hook: unknown, target: HookTarget): boolean {
  if (!hook || typeof hook !== "object") {
    return false;
  }
  const record = hook as Record<string, unknown>;
  const command = typeof record.command === "string" ? record.command : "";
  const statusMessage = typeof record.statusMessage === "string" ? record.statusMessage : "";
  return command.includes(" hook run ") && command.includes(`--harness ${target}`) && (command.includes("brainctl") || statusMessage === BRAINSTACK_HOOK_STATUS_MESSAGE);
}

async function readJsonObject(path: string): Promise<Record<string, unknown>> {
  if (!existsSync(path)) {
    return {};
  }
  const text = await readFile(path, "utf8");
  const parsed = JSON.parse(text) as unknown;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`JSON config must be an object: ${path}`);
  }
  return parsed as Record<string, unknown>;
}

function mergeCodexStyleHooks(raw: Record<string, unknown>, target: HookTarget, brainctlCommand: string, configPath: string): Record<string, unknown> {
  const hooks = raw.hooks && typeof raw.hooks === "object" && !Array.isArray(raw.hooks) ? (raw.hooks as Record<string, unknown>) : {};
  removeCodexStyleManagedHooks(hooks, target);
  const handler = (event: string) => ({
    type: "command",
    command: managedHookCommand(brainctlCommand, target, event, configPath),
    timeout: 5,
    statusMessage: BRAINSTACK_HOOK_STATUS_MESSAGE
  });
  const additions: Record<string, Array<Record<string, unknown>>> = {
    SessionStart: [{ matcher: "startup|resume|compact|clear", hooks: [handler("SessionStart")] }],
    UserPromptSubmit: [{ hooks: [handler("UserPromptSubmit")] }],
    Stop: [{ hooks: [handler("Stop")] }],
    PostCompact: [{ matcher: "manual|auto", hooks: [handler("PostCompact")] }]
  };
  for (const [event, groups] of Object.entries(additions)) {
    const existing = Array.isArray(hooks[event]) ? (hooks[event] as unknown[]) : [];
    hooks[event] = [...existing, ...groups];
  }
  return { ...raw, hooks };
}

function removeCodexStyleManagedHooks(hooks: Record<string, unknown>, target: HookTarget): number {
  let removed = 0;
  for (const [event, groupsRaw] of Object.entries(hooks)) {
    if (!Array.isArray(groupsRaw)) {
      continue;
    }
    const groups: unknown[] = [];
    for (const groupRaw of groupsRaw) {
      if (!groupRaw || typeof groupRaw !== "object") {
        groups.push(groupRaw);
        continue;
      }
      const group = { ...(groupRaw as Record<string, unknown>) };
      const hookList = Array.isArray(group.hooks) ? group.hooks : [];
      const kept = hookList.filter((hook) => {
        const managed = isManagedBrainstackHook(hook, target);
        if (managed) {
          removed += 1;
        }
        return !managed;
      });
      if (kept.length) {
        group.hooks = kept;
        groups.push(group);
      }
    }
    if (groups.length) {
      hooks[event] = groups;
    } else {
      delete hooks[event];
    }
  }
  return removed;
}

function countCodexStyleManagedHooks(raw: Record<string, unknown>, target: HookTarget): number {
  const hooks = raw.hooks && typeof raw.hooks === "object" && !Array.isArray(raw.hooks) ? (raw.hooks as Record<string, unknown>) : {};
  let count = 0;
  for (const groupsRaw of Object.values(hooks)) {
    if (!Array.isArray(groupsRaw)) {
      continue;
    }
    for (const groupRaw of groupsRaw) {
      if (!groupRaw || typeof groupRaw !== "object") {
        continue;
      }
      const hookList = Array.isArray((groupRaw as Record<string, unknown>).hooks) ? ((groupRaw as Record<string, unknown>).hooks as unknown[]) : [];
      count += hookList.filter((hook) => isManagedBrainstackHook(hook, target)).length;
    }
  }
  return count;
}

function mergeCursorHooks(raw: Record<string, unknown>, target: HookTarget, brainctlCommand: string, configPath: string): Record<string, unknown> {
  const hooks = raw.hooks && typeof raw.hooks === "object" && !Array.isArray(raw.hooks) ? (raw.hooks as Record<string, unknown>) : {};
  removeCursorManagedHooks(hooks, target);
  const handler = (event: string) => ({
    command: managedHookCommand(brainctlCommand, target, event, configPath),
    timeout: 5,
    statusMessage: BRAINSTACK_HOOK_STATUS_MESSAGE
  });
  hooks.beforeSubmitPrompt = [...(Array.isArray(hooks.beforeSubmitPrompt) ? (hooks.beforeSubmitPrompt as unknown[]) : []), handler("beforeSubmitPrompt")];
  hooks.stop = [...(Array.isArray(hooks.stop) ? (hooks.stop as unknown[]) : []), handler("stop")];
  return { version: 1, ...raw, hooks };
}

function removeCursorManagedHooks(hooks: Record<string, unknown>, target: HookTarget): number {
  let removed = 0;
  for (const [event, handlersRaw] of Object.entries(hooks)) {
    if (!Array.isArray(handlersRaw)) {
      continue;
    }
    const kept = handlersRaw.filter((hook) => {
      const managed = isManagedBrainstackHook(hook, target);
      if (managed) {
        removed += 1;
      }
      return !managed;
    });
    if (kept.length) {
      hooks[event] = kept;
    } else {
      delete hooks[event];
    }
  }
  return removed;
}

function countCursorManagedHooks(raw: Record<string, unknown>, target: HookTarget): number {
  const hooks = raw.hooks && typeof raw.hooks === "object" && !Array.isArray(raw.hooks) ? (raw.hooks as Record<string, unknown>) : {};
  return Object.values(hooks).reduce((sum, handlersRaw) => sum + (Array.isArray(handlersRaw) ? handlersRaw.filter((hook) => isManagedBrainstackHook(hook, target)).length : 0), 0);
}

async function commandHooks(args: ParsedArgs): Promise<void> {
  const subcommand = args.positional[0] || "status";
  const targets = hookTargetsFromArgs(args);
  const configPath = abs(requireFlagValue(args, "config") || brainstackDefaultConfigPath());
  const brainctlCommand = currentBrainctlHookCommand(args);
  for (const target of targets) {
    const path = hookConfigPath(target);
    if (subcommand === "status") {
      const raw = await readJsonObject(path).catch(() => ({}));
      const count = target === "cursor" ? countCursorManagedHooks(raw, target) : countCodexStyleManagedHooks(raw, target);
      console.log(`${target}: ${count ? "installed" : "missing"} hooks=${count} path=${path}`);
      continue;
    }
    if (subcommand === "install") {
      const raw = await readJsonObject(path);
      const next = target === "cursor" ? mergeCursorHooks(raw, target, brainctlCommand, configPath) : mergeCodexStyleHooks(raw, target, brainctlCommand, configPath);
      if (hasFlag(args, "dry-run")) {
        console.log(`${target}: dry-run install path=${path}`);
        console.log(JSON.stringify(next, null, 2));
      } else {
        await writeText(path, `${JSON.stringify(next, null, 2)}\n`, 0o600);
        console.log(`${target}: hooks installed path=${path}`);
      }
      continue;
    }
    if (subcommand === "remove") {
      const raw = await readJsonObject(path).catch(() => ({}));
      const hooks = raw.hooks && typeof raw.hooks === "object" && !Array.isArray(raw.hooks) ? (raw.hooks as Record<string, unknown>) : {};
      const removed = target === "cursor" ? removeCursorManagedHooks(hooks, target) : removeCodexStyleManagedHooks(hooks, target);
      const next = { ...raw, hooks };
      if (hasFlag(args, "dry-run")) {
        console.log(`${target}: dry-run remove removed=${removed} path=${path}`);
      } else {
        await writeText(path, `${JSON.stringify(next, null, 2)}\n`, 0o600);
        console.log(`${target}: hooks removed=${removed} path=${path}`);
      }
      continue;
    }
    throw new Error("hooks subcommand must be install|status|remove");
  }
}

function hookStateRootFallback(): string {
  return process.env.HOME ? join(abs(process.env.HOME), ".local", "state", "brainstack") : join(tmpdir(), "brainstack");
}

async function appendHookEventLog(stateRoot: string, event: Record<string, unknown>): Promise<void> {
  const dir = join(stateRoot, "harness-events");
  await ensureDir(dir);
  await chmod(dir, 0o700).catch(() => undefined);
  const path = join(dir, `${new Date().toISOString().slice(0, 10)}.jsonl`);
  await appendFile(path, `${JSON.stringify(event)}\n`, { encoding: "utf8" });
  await chmod(path, 0o600).catch(() => undefined);
}

async function readHookStdinCapped(maxBytes = 256 * 1024): Promise<{ text: string; bytes: number; truncated: boolean }> {
  const reader = Bun.stdin.stream().getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  let kept = 0;
  while (true) {
    const chunk = await reader.read();
    if (chunk.done) {
      break;
    }
    total += chunk.value.byteLength;
    if (kept < maxBytes) {
      const remaining = maxBytes - kept;
      const slice = chunk.value.byteLength > remaining ? chunk.value.slice(0, remaining) : chunk.value;
      chunks.push(slice);
      kept += slice.byteLength;
    }
  }
  return {
    text: new TextDecoder().decode(Buffer.concat(chunks)),
    bytes: total,
    truncated: total > maxBytes
  };
}

function summarizeHookInput(input: unknown, stdinInfo: { bytes: number; truncated: boolean }): Record<string, unknown> {
  const summary: Record<string, unknown> = {
    input_bytes: stdinInfo.bytes,
    input_truncated: stdinInfo.truncated
  };
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    summary.kind = input === null ? "null" : Array.isArray(input) ? "array" : typeof input;
    return summary;
  }
  const record = input as Record<string, unknown>;
  summary.keys = Object.keys(record).slice(0, 50);
  for (const key of ["hook_event_name", "session_id", "turn_id", "cwd", "trigger", "source", "transcript_path"]) {
    const value = record[key];
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean" || value === null) {
      summary[key] = value;
    }
  }
  if (typeof record.prompt === "string") {
    summary.prompt_sha256 = sha256Hex(record.prompt);
    summary.prompt_bytes = new TextEncoder().encode(record.prompt).byteLength;
  }
  if (Array.isArray(record.attachments)) {
    summary.attachments_count = record.attachments.length;
  }
  if (typeof record.tool_name === "string") {
    summary.tool_name = record.tool_name;
  }
  return summary;
}

function hookInputString(input: unknown, key: string): string | undefined {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return undefined;
  }
  const value = (input as Record<string, unknown>)[key];
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function sessionIdFromTranscriptPath(path: string): string | undefined {
  const match = basename(path).match(/([0-9a-f]{8,}-[0-9a-f-]{20,})/i);
  return match?.[1];
}

async function hookSessionCheckpointPayload(
  cfg: BrainstackConfig,
  target: HookTarget,
  event: string,
  input: unknown
): Promise<Record<string, unknown> | null> {
  const transcriptPathRaw = hookInputString(input, "transcript_path");
  if (!transcriptPathRaw) {
    return null;
  }
  const transcriptPath = abs(expandHome(transcriptPathRaw));
  const info = await lstat(transcriptPath).catch(() => null);
  if (!info?.isFile() || info.isSymbolicLink()) {
    return null;
  }
  const sessionId = hookInputString(input, "session_id") || sessionIdFromTranscriptPath(transcriptPath) || sha256Hex(transcriptPath).slice(0, 16);
  const cwd = hookInputString(input, "cwd") || process.cwd();
  const title = `Codex session checkpoint: ${sessionId}`;
  const text = [
    `# ${title}`,
    "",
    `- Session id: \`${sessionId}\``,
    `- Harness: \`${target}\``,
    `- Hook event: \`${event}\``,
    `- Cwd: \`${cwd}\``,
    `- Local transcript path: \`${transcriptPath}\``,
    "",
    "## Capture Note",
    "",
    "This is a hook-created checkpoint, not a transcript import. Use `brainctl import codex-session` from the machine that owns the transcript when full raw evidence is needed.",
    ""
  ].join("\n");
  return {
    title,
    text,
    source_harness: target,
    source_machine: cfg.machine.name,
    source_type: "codex-session-checkpoint",
    conversation_id: sessionId,
    related_repo: cwd,
    transcript_path: transcriptPath,
    transcript_bytes: info.size,
    tags: uniqueNonEmptyStrings(["codex-session", "codex-session-checkpoint", `session:${sessionId}`, `repo:${deriveProjectLabel(cwd)}`])
  };
}

async function commandHook(args: ParsedArgs): Promise<void> {
  const subcommand = args.positional[0] || "help";
  if (subcommand !== "run") {
    if (subcommand === "help" || subcommand === "--help" || subcommand === "-h") {
      console.log("Usage: brainctl hook run --harness codex|claude|cursor --event EVENT [--config brainstack.yaml]");
      return;
    }
    throw new Error(`Unknown hook subcommand: ${subcommand}`);
  }
  const target = normalizeHookTarget(requireFlagValue(args, "harness"), "codex");
  const event = requireFlagValue(args, "event") || "unknown";
  const stdinInfo = await readHookStdinCapped().catch(() => ({ text: "", bytes: 0, truncated: false }));
  const stdin = stdinInfo.text;
  let parsedInput: unknown = null;
  try {
    parsedInput = stdin ? JSON.parse(stdin) : null;
  } catch {
    parsedInput = { raw_sha256: sha256Hex(stdin), raw_bytes: stdinInfo.bytes, raw_truncated: stdinInfo.truncated };
  }
  try {
    const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
    await appendHookEventLog(cfg.paths.stateRoot, {
      ts: new Date().toISOString(),
      harness: target,
      event,
      cwd: process.cwd(),
      input: summarizeHookInput(parsedInput, stdinInfo)
    });
    if (event === "SessionStart" || event === "UserPromptSubmit" || event === "beforeSubmitPrompt") {
      const daemonStatus = await readDaemonStatus(cfg);
      const daemonFresh = daemonStatusFresh(daemonStatus, 5 * 60_000);
      await appendHookEventLog(cfg.paths.stateRoot, {
        ts: new Date().toISOString(),
        harness: target,
        event: "daemon-status",
        fresh: daemonFresh,
        updated_at: daemonStatus?.updated_at || null,
        ok: daemonStatus?.ok ?? null
      });
      if (!daemonFresh) {
        const unsafeReason = await localSkillRefreshUnsafeReason(cfg);
        if (unsafeReason) {
          await appendHookEventLog(cfg.paths.stateRoot, {
            ts: new Date().toISOString(),
            harness: target,
            event: "refresh-skipped",
            reason: unsafeReason
          });
        } else {
          await refreshBrainstackSkillPackages(
            cfg,
            { ...args, flags: { ...args.flags, target, "no-sync": true, quiet: true } },
            { quiet: true, hookMode: true }
          ).catch(async (error) => {
            await appendHookEventLog(cfg.paths.stateRoot, {
              ts: new Date().toISOString(),
              harness: target,
              event: "refresh-error",
              error: error instanceof Error ? error.message : String(error)
            });
          });
        }
      }
    }
    if (event === "Stop" || event === "stop") {
      const payload = await hookSessionCheckpointPayload(cfg, target, event, parsedInput);
      if (payload) {
        const queuedPath = await queueBrainWriteForBackgroundFlush(cfg, "import", payload, "queued by harness hook for daemon/outbox flush");
        await appendHookEventLog(cfg.paths.stateRoot, {
          ts: new Date().toISOString(),
          harness: target,
          event: "session-checkpoint-queued",
          outbox: queuedPath,
          session_id: payload.conversation_id || null
        });
      } else {
        await appendHookEventLog(cfg.paths.stateRoot, {
          ts: new Date().toISOString(),
          harness: target,
          event: "session-checkpoint-skipped",
          reason: "no regular transcript_path in hook input"
        });
      }
    }
  } catch (error) {
    await appendHookEventLog(hookStateRootFallback(), {
      ts: new Date().toISOString(),
      harness: target,
      event: "hook-error",
      detail: error instanceof Error ? error.message : String(error)
    }).catch(() => undefined);
  }
  console.log("{}");
}

function daemonStateDir(cfg: BrainstackConfig): string {
  return join(cfg.paths.stateRoot, "daemon");
}

function daemonStatusPath(cfg: BrainstackConfig): string {
  return join(daemonStateDir(cfg), "status.json");
}

function daemonEventsPath(cfg: BrainstackConfig): string {
  return join(daemonStateDir(cfg), "events.jsonl");
}

function daemonLockPath(cfg: BrainstackConfig): string {
  return join(daemonStateDir(cfg), "brainstackd.lock");
}

function daemonPlatform(args: ParsedArgs): "launchd" | "systemd" {
  const explicit = requireFlagValue(args, "platform");
  if (explicit) {
    if (explicit === "launchd" || explicit === "systemd") {
      return explicit;
    }
    throw new Error("--platform must be launchd or systemd");
  }
  return process.platform === "darwin" ? "launchd" : "systemd";
}

function daemonServicePath(cfg: BrainstackConfig, platform: "launchd" | "systemd"): string {
  if (platform === "launchd") {
    return join(cfg.paths.home, "Library", "LaunchAgents", `${BRAINSTACK_DAEMON_LABEL}.plist`);
  }
  return join(cfg.paths.systemdUserRoot, BRAINSTACK_DAEMON_SERVICE);
}

function daemonLogPaths(cfg: BrainstackConfig): { stdout: string; stderr: string; events: string; status: string } {
  return {
    stdout: join(daemonStateDir(cfg), "brainstackd.out.log"),
    stderr: join(daemonStateDir(cfg), "brainstackd.err.log"),
    events: daemonEventsPath(cfg),
    status: daemonStatusPath(cfg)
  };
}

function daemonIntervalMs(args: ParsedArgs): number {
  return parsePositiveIntegerFlag(args, "interval-seconds", 60) * 1000;
}

function daemonJitterMs(intervalMs: number): number {
  if (intervalMs < 5000) {
    return 0;
  }
  const spread = Math.max(1000, Math.floor(intervalMs * 0.1));
  return Math.floor(Math.random() * spread);
}

async function appendDaemonEvent(cfg: BrainstackConfig, event: Record<string, unknown>): Promise<void> {
  await ensureDir(daemonStateDir(cfg));
  await chmod(daemonStateDir(cfg), 0o700).catch(() => undefined);
  await appendFile(daemonEventsPath(cfg), `${JSON.stringify({ ts: new Date().toISOString(), ...event })}\n`, { encoding: "utf8" });
  await chmod(daemonEventsPath(cfg), 0o600).catch(() => undefined);
}

function processAlive(pid: number): boolean {
  if (!Number.isSafeInteger(pid) || pid <= 0) {
    return false;
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

interface DaemonLockRecord {
  pid?: number;
  created_at?: string;
  host?: string;
  exec_path?: string;
  argv?: string[];
  cwd?: string;
}

function currentDaemonLockRecord(): DaemonLockRecord {
  return {
    pid: process.pid,
    created_at: new Date().toISOString(),
    host: hostname(),
    exec_path: process.execPath,
    argv: process.argv,
    cwd: process.cwd()
  };
}

function processCommandLine(pid: number): string | null {
  if (process.platform !== "linux") {
    return null;
  }
  try {
    return readFileSync(`/proc/${pid}/cmdline`, "utf8")
      .replace(/\0/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  } catch {
    return null;
  }
}

function looksLikeBrainstackDaemonCommand(value: string): boolean {
  const text = value.toLowerCase();
  return (text.includes("brainctl") || text.includes("packages/brainctl/src/main.ts") || text.includes("main.ts")) && text.includes("daemon");
}

function daemonLockHasIdentity(record: DaemonLockRecord): boolean {
  return Boolean(record.host || record.exec_path || Array.isArray(record.argv));
}

function daemonLockOwnerState(record: DaemonLockRecord): { live: boolean; detail: string } {
  if (typeof record.pid !== "number" || !Number.isSafeInteger(record.pid) || record.pid <= 0) {
    return { live: false, detail: "lock has no valid pid" };
  }
  if (!processAlive(record.pid)) {
    return { live: false, detail: `pid ${record.pid} is not alive` };
  }
  if (record.host && record.host !== hostname()) {
    return { live: false, detail: `pid ${record.pid} belongs to stale lock for host ${record.host}` };
  }
  if (!daemonLockHasIdentity(record)) {
    // Legacy pid-only locks cannot prove identity. Keep the old conservative
    // behavior for already-live PIDs so an upgrade cannot steal a real old daemon.
    return { live: true, detail: `pid-only lock for live pid ${record.pid}` };
  }
  const lockCommand = Array.isArray(record.argv) ? record.argv.join(" ") : "";
  const lockLooksLikeDaemon = looksLikeBrainstackDaemonCommand(lockCommand);
  const currentCommand = processCommandLine(record.pid);
  const currentLooksLikeDaemon = currentCommand === null ? null : looksLikeBrainstackDaemonCommand(currentCommand);
  if (lockLooksLikeDaemon && currentLooksLikeDaemon !== false) {
    return { live: true, detail: currentCommand ? `pid ${record.pid} matches daemon command` : `pid ${record.pid} alive; command identity unavailable` };
  }
  if (currentLooksLikeDaemon === true) {
    return { live: true, detail: `pid ${record.pid} command is a daemon` };
  }
  return {
    live: false,
    detail: currentCommand
      ? `pid ${record.pid} command does not match daemon identity: ${currentCommand.slice(0, 200)}`
      : `pid ${record.pid} alive but lock identity does not describe a daemon`
  };
}

async function acquireDaemonLock(cfg: BrainstackConfig): Promise<() => Promise<void>> {
  await ensureDir(daemonStateDir(cfg));
  await chmod(daemonStateDir(cfg), 0o700).catch(() => undefined);
  const path = daemonLockPath(cfg);
  let acquired = false;
  for (let attempt = 0; attempt < 5 && !acquired; attempt += 1) {
    // Atomic create first: O_EXCL means exactly one contender can win a fresh lock.
    try {
      const handle = await open(path, "wx", 0o600);
      try {
        await handle.writeFile(`${JSON.stringify(currentDaemonLockRecord(), null, 2)}\n`, "utf8");
      } finally {
        await handle.close();
      }
      acquired = true;
      break;
    } catch (error) {
      if ((error as { code?: string }).code !== "EEXIST") {
        throw error;
      }
    }

    const existing = await readFile(path, "utf8").catch(() => null);
    if (existing === null) {
      // Lock disappeared between create and read; retry the atomic create.
      continue;
    }
    let prior: DaemonLockRecord;
    try {
      prior = JSON.parse(existing) as DaemonLockRecord;
    } catch {
      // A malformed lock is treated as held: removing it blindly could break a live
      // daemon whose lock was corrupted, so require explicit operator recovery.
      throw new Error(`brainstack daemon lock is unreadable: ${path}; inspect and remove it manually if no daemon is running`);
    }
    const owner = daemonLockOwnerState(prior);
    if (owner.live) {
      throw new Error(`brainstack daemon already running with pid ${prior.pid}: ${owner.detail}`);
    }
    // Stale lock: removal happens under a short-lived takeover mutex (atomic mkdir)
    // with a content re-check, so two contenders can never remove each other's
    // freshly created locks.
    const takeoverDir = `${path}.takeover`;
    try {
      await mkdir(takeoverDir);
    } catch (error) {
      if ((error as { code?: string }).code !== "EEXIST") {
        throw error;
      }
      // Another process is recovering; clear an abandoned mutex, then retry.
      const mutexInfo = await lstat(takeoverDir).catch(() => null);
      if (mutexInfo && Date.now() - mutexInfo.mtimeMs > 60_000) {
        await rmdir(takeoverDir).catch(() => undefined);
      }
      await Bun.sleep(50);
      continue;
    }
    try {
      const recheck = await readFile(path, "utf8").catch(() => null);
      if (recheck === existing) {
        await rm(path, { force: true });
      }
    } finally {
      await rmdir(takeoverDir).catch(() => undefined);
    }
  }
  if (!acquired) {
    throw new Error(`brainstack daemon lock changed while acquiring: ${path}`);
  }
  return async () => {
    const current = await readFile(path, "utf8").catch(() => "");
    if (!current) {
      return;
    }
    try {
      const parsed = JSON.parse(current) as { pid?: number };
      if (parsed.pid === process.pid) {
        await rm(path, { force: true });
      }
    } catch {
      // Preserve an unparseable lock for operator review.
    }
  };
}

function emptyDaemonJobStatus(detail = "not run"): BrainstackDaemonJobStatus {
  return { ok: true, detail };
}

async function readDaemonStatus(cfg: BrainstackConfig): Promise<BrainstackDaemonStatus | null> {
  const path = daemonStatusPath(cfg);
  if (!existsSync(path)) {
    return null;
  }
  const info = await lstat(path).catch(() => null);
  if (!info || info.isSymbolicLink() || !info.isFile() || info.size > 256 * 1024) {
    return null;
  }
  try {
    const parsed = JSON.parse(await readFile(path, "utf8")) as BrainstackDaemonStatus;
    return parsed.schema_version === BRAINSTACK_DAEMON_STATUS_SCHEMA ? parsed : null;
  } catch {
    return null;
  }
}

async function writeDaemonStatus(status: BrainstackDaemonStatus): Promise<void> {
  await writeText(status.state_path, `${JSON.stringify(status, null, 2)}\n`, 0o600);
}

function initialDaemonStatus(cfg: BrainstackConfig, configPath: string, startedAt: string): BrainstackDaemonStatus {
  const repo = clientLocalPathAbs(cfg);
  return {
    schema_version: BRAINSTACK_DAEMON_STATUS_SCHEMA,
    product: "brainstack",
    daemon: "brainctl daemon run",
    ok: true,
    pid: process.pid,
    machine: cfg.machine.name,
    config_path: configPath,
    state_path: daemonStatusPath(cfg),
    started_at: startedAt,
    updated_at: startedAt,
    iteration: 0,
    repo: {
      path: repo,
      exists: existsSync(join(repo, ".git")),
      clean: null,
      branch: null,
      head: null
    },
    outbox: emptyDaemonJobStatus(),
    skills: emptyDaemonJobStatus(),
    errors: []
  };
}

function gitShortOutput(args: string[], cwd: string, timeoutMs = 10_000): string {
  const result = run(["git", ...args], { cwd, check: false, timeoutMs });
  return result.code === 0 ? result.stdout.trim() : "";
}

async function localSkillRefreshUnsafeReason(cfg: BrainstackConfig): Promise<string | null> {
  const repo = clientLocalPathAbs(cfg);
  const info = await lstat(repo).catch(() => null);
  if (!info || info.isSymbolicLink() || !info.isDirectory() || !gitExists(repo)) {
    return "shared-brain clone missing or not a non-symlink git checkout";
  }
  const status = run(["git", "status", "--porcelain"], { cwd: repo, check: false, timeoutMs: 10_000 });
  if (status.code !== 0) {
    return `git status failed: ${(status.stderr || status.stdout).replace(/\s+/g, " ").slice(0, 240)}`;
  }
  if (status.stdout.trim()) {
    return "shared-brain clone dirty";
  }
  return null;
}

async function daemonSyncSharedBrain(cfg: BrainstackConfig): Promise<BrainstackDaemonStatus["repo"] & { ok: boolean; detail: string; pulled: boolean }> {
  const repo = clientLocalPathAbs(cfg);
  if (!existsSync(repo)) {
    return { path: repo, exists: false, clean: null, branch: null, head: null, ok: false, detail: "clone missing", pulled: false };
  }
  const info = await lstat(repo).catch(() => null);
  if (!info || info.isSymbolicLink() || !info.isDirectory() || !gitExists(repo)) {
    return { path: repo, exists: false, clean: null, branch: null, head: null, ok: false, detail: "path is not a non-symlink git clone", pulled: false };
  }
  const dirty = gitShortOutput(["status", "--porcelain"], repo);
  const branch = gitShortOutput(["rev-parse", "--abbrev-ref", "HEAD"], repo) || null;
  const headBefore = gitShortOutput(["rev-parse", "HEAD"], repo) || null;
  if (dirty) {
    return { path: repo, exists: true, clean: false, branch, head: headBefore, ok: false, detail: "clone dirty; sync skipped", pulled: false };
  }
  const pull = run(["git", "pull", "--ff-only"], { cwd: repo, check: false, timeoutMs: 20_000 });
  const headAfter = gitShortOutput(["rev-parse", "HEAD"], repo) || headBefore;
  if (pull.code !== 0) {
    return {
      path: repo,
      exists: true,
      clean: true,
      branch,
      head: headAfter,
      ok: false,
      detail: `git pull failed: ${(pull.stderr || pull.stdout).replace(/\s+/g, " ").slice(0, 240)}`,
      pulled: false
    };
  }
  return {
    path: repo,
    exists: true,
    clean: true,
    branch,
    head: headAfter,
    ok: true,
    detail: headBefore && headAfter && headBefore !== headAfter ? `updated ${headBefore.slice(0, 12)}..${headAfter.slice(0, 12)}` : "already current",
    pulled: true,
    last_pull_at: new Date().toISOString()
  };
}

async function runDaemonJob<T extends BrainstackDaemonJobStatus>(job: () => Promise<T>): Promise<T> {
  const started = Date.now();
  const startedAt = new Date(started).toISOString();
  try {
    const result = await job();
    const finished = Date.now();
    return { ...result, started_at: startedAt, finished_at: new Date(finished).toISOString(), duration_ms: finished - started };
  } catch (error) {
    const finished = Date.now();
    return {
      ok: false,
      detail: error instanceof Error ? error.message : String(error),
      started_at: startedAt,
      finished_at: new Date(finished).toISOString(),
      duration_ms: finished - started
    } as T;
  }
}

function daemonSkillTargets(args: ParsedArgs, cfg: BrainstackConfig): HookTarget[] {
  const target = requireFlagValue(args, "target") || "all";
  if (target === "all") {
    return ["codex", "claude", "cursor"];
  }
  return [normalizeHookTarget(target, normalizeHookTarget(cfg.harness.name, "codex"))];
}

async function daemonRefreshSkills(cfg: BrainstackConfig, args: ParsedArgs): Promise<BrainstackDaemonStatus["skills"]> {
  const targets = daemonSkillTargets(args, cfg);
  const installed: string[] = [];
  const skipped: string[] = [];
  const warnings: string[] = [];
  for (const target of targets) {
    const result = await refreshBrainstackSkillPackages(
      cfg,
      { ...args, flags: { ...args.flags, target, "no-sync": true, quiet: true } },
      { quiet: true, hookMode: true }
    );
    installed.push(...result.installed.map((name) => `${target}:${name}`));
    skipped.push(...result.skipped.map((name) => `${target}:${name}`));
    warnings.push(...result.warnings.map((warning) => `${target}:${warning}`));
  }
  return {
    ok: true,
    detail: warnings.length ? `refreshed with warnings: ${warnings.join("; ").slice(0, 500)}` : "refreshed shared skills from local clone",
    targets,
    installed,
    skipped
  };
}

async function daemonFlushOutbox(cfg: BrainstackConfig): Promise<BrainstackDaemonStatus["outbox"]> {
  const result = await flushOutbox(cfg);
  return {
    ok: result.corrupt === 0 && result.terminalFailures === 0,
    detail: `flushed=${result.flushed} kept=${result.kept} terminal_failures=${result.terminalFailures} corrupt=${result.corrupt}`,
    flushed: result.flushed,
    kept: result.kept,
    terminal_failures: result.terminalFailures,
    corrupt: result.corrupt
  };
}

async function daemonIteration(cfg: BrainstackConfig, args: ParsedArgs, status: BrainstackDaemonStatus): Promise<BrainstackDaemonStatus> {
  const errors: string[] = [];
  status.iteration += 1;
  const sync = hasFlag(args, "no-sync")
    ? { path: clientLocalPathAbs(cfg), exists: gitExists(clientLocalPathAbs(cfg)), clean: null, branch: null, head: null, ok: true, detail: "sync disabled", pulled: false }
    : await daemonSyncSharedBrain(cfg);
  status.repo = {
    path: sync.path,
    exists: sync.exists,
    clean: sync.clean,
    branch: sync.branch,
    head: sync.head,
    last_pull_at: sync.last_pull_at || status.repo.last_pull_at
  };
  if (!sync.ok) {
    errors.push(sync.detail);
  }
  status.outbox = hasFlag(args, "no-flush") ? emptyDaemonJobStatus("outbox flush disabled") : await runDaemonJob(() => daemonFlushOutbox(cfg));
  if (!status.outbox.ok) {
    errors.push(`outbox: ${status.outbox.detail}`);
  }
  const unsafeSkillSource = !hasFlag(args, "no-sync") && (!sync.exists || sync.clean === false);
  status.skills = hasFlag(args, "no-skills")
    ? emptyDaemonJobStatus("skills refresh disabled")
    : unsafeSkillSource
      ? { ok: false, detail: `shared-brain clone unsafe for skill refresh: ${sync.detail}` }
      : await runDaemonJob(() => daemonRefreshSkills(cfg, args));
  if (!status.skills.ok) {
    errors.push(`skills: ${status.skills.detail}`);
  }
  status.ok = errors.length === 0;
  status.errors = errors;
  status.updated_at = new Date().toISOString();
  status.next_run_after = new Date(Date.now() + daemonIntervalMs(args)).toISOString();
  await writeDaemonStatus(status);
  await appendDaemonEvent(cfg, { event: "iteration", iteration: status.iteration, ok: status.ok, errors });
  return status;
}

function daemonStatusFresh(status: BrainstackDaemonStatus | null, maxAgeMs: number): boolean {
  if (!status?.ok) {
    return false;
  }
  const updated = Date.parse(status.updated_at || "");
  const ageMs = Date.now() - updated;
  return Number.isFinite(updated) && ageMs >= -60_000 && ageMs <= maxAgeMs;
}

async function commandDaemonRun(args: ParsedArgs): Promise<void> {
  const configPath = abs(requireFlagValue(args, "config") || brainstackDefaultConfigPath());
  const cfg = await loadConfig(configPath, flag(args, "profile") || "client-macos", flag(args, "root"));
  const releaseLock = await acquireDaemonLock(cfg);
  const startedAt = new Date().toISOString();
  let stopping = false;
  let wakeStop: (() => void) | null = null;
  const stop = () => {
    stopping = true;
    wakeStop?.();
  };
  process.once("SIGINT", stop);
  process.once("SIGTERM", stop);
  try {
    let status = initialDaemonStatus(cfg, configPath, startedAt);
    await writeDaemonStatus(status);
    await appendDaemonEvent(cfg, { event: "start", pid: process.pid, config_path: configPath });
    while (!stopping) {
      status = await daemonIteration(cfg, args, status);
      if (hasFlag(args, "once") || args.positional[0] === "once") {
        break;
      }
      const intervalMs = daemonIntervalMs(args);
      await Promise.race([
        Bun.sleep(intervalMs + daemonJitterMs(intervalMs)),
        new Promise<void>((resolveStop) => {
          wakeStop = resolveStop;
        })
      ]).finally(() => {
        wakeStop = null;
      });
    }
    await appendDaemonEvent(cfg, { event: "stop", pid: process.pid, iteration: status.iteration });
  } finally {
    await releaseLock();
  }
}

async function daemonServiceStatus(cfg: BrainstackConfig, args: ParsedArgs, timeoutMs = 2000): Promise<DaemonServiceStatus> {
  const platform = daemonPlatform(args);
  const path = daemonServicePath(cfg, platform);
  const installed = existsSync(path);
  const probeTimeoutMs = Math.min(Math.max(timeoutMs, 100), 10_000);
  if (platform === "systemd") {
    if (!commandPath("systemctl")) {
      return { platform, installed, running: null, path, detail: "systemctl unavailable" };
    }
    const active = run(["systemctl", "--user", "is-active", "--quiet", BRAINSTACK_DAEMON_SERVICE], { check: false, timeoutMs: probeTimeoutMs });
    return { platform, installed, running: active.code === 0, path, detail: active.code === 0 ? "active" : "not active" };
  }
  if (!commandPath("launchctl")) {
    return { platform, installed, running: null, path, detail: "launchctl unavailable" };
  }
  const uid = typeof process.getuid === "function" ? process.getuid() : null;
  const domain = uid === null ? `gui/${process.env.UID || ""}` : `gui/${uid}`;
  const result = run(["launchctl", "print", `${domain}/${BRAINSTACK_DAEMON_LABEL}`], { check: false, timeoutMs: probeTimeoutMs });
  return { platform, installed, running: result.code === 0, path, detail: result.code === 0 ? "loaded" : "not loaded" };
}

async function commandDaemonStatus(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
  const service = await daemonServiceStatus(cfg, args);
  const status = await readDaemonStatus(cfg);
  const fresh = daemonStatusFresh(status, 10 * 60_000);
  const pidAlive = typeof status?.pid === "number" ? processAlive(status.pid) : null;
  const active = service.running === true || pidAlive === true;
  const ok = Boolean(status?.ok && fresh && active && service.installed && service.running !== false);
  const body = { ok, fresh, pid_alive: pidAlive, service, status };
  if (hasFlag(args, "json")) {
    console.log(JSON.stringify(body, null, 2));
    return;
  }
  console.log(`daemon service: platform=${service.platform} installed=${service.installed} running=${service.running ?? "unknown"} path=${service.path}`);
  if (!status) {
    console.log(`daemon status: missing (${daemonStatusPath(cfg)})`);
    return;
  }
  console.log(`daemon status: ok=${status.ok} fresh=${fresh} pid=${status.pid} pid_alive=${pidAlive ?? "unknown"} updated=${status.updated_at} iteration=${status.iteration}`);
  console.log(`shared-brain: path=${status.repo.path} head=${status.repo.head || "unknown"} clean=${status.repo.clean ?? "unknown"} last_pull=${status.repo.last_pull_at || "never"}`);
  console.log(`outbox: ${status.outbox.detail}`);
  console.log(`skills: ${status.skills.detail}`);
  if (status.errors.length) {
    console.log(`errors: ${status.errors.join("; ")}`);
  }
}

async function commandDaemonInstall(args: ParsedArgs): Promise<void> {
  const configPath = abs(requireFlagValue(args, "config") || brainstackDefaultConfigPath());
  const cfg = await loadConfig(configPath, flag(args, "profile") || "client-macos", flag(args, "root"));
  const platform = daemonPlatform(args);
  const path = daemonServicePath(cfg, platform);
  const rendered = platform === "launchd" ? brainstackDaemonLaunchAgent(cfg, { ...args, flags: { ...args.flags, config: configPath } }) : brainstackDaemonSystemdService(cfg, { ...args, flags: { ...args.flags, config: configPath } });
  if (hasFlag(args, "dry-run")) {
    console.log(`# ${platform} service: ${path}`);
    console.log(rendered.trimEnd());
    return;
  }
  await ensureDir(daemonStateDir(cfg));
  await chmod(daemonStateDir(cfg), 0o700).catch(() => undefined);
  await writeText(path, rendered, 0o600);
  await writeManagedManifest(cfg, platform);
  if (hasFlag(args, "start")) {
    if (platform === "systemd") {
      run(["systemctl", "--user", "daemon-reload"], { check: false });
      run(["systemctl", "--user", "enable", "--now", BRAINSTACK_DAEMON_SERVICE], { check: false });
      run(["systemctl", "--user", "restart", BRAINSTACK_DAEMON_SERVICE], { check: false });
    } else {
      const uid = typeof process.getuid === "function" ? process.getuid() : null;
      const domain = uid === null ? `gui/${process.env.UID || ""}` : `gui/${uid}`;
      run(["launchctl", "bootout", domain, path], { check: false });
      run(["launchctl", "bootstrap", domain, path], { check: false });
      run(["launchctl", "kickstart", "-k", `${domain}/${BRAINSTACK_DAEMON_LABEL}`], { check: false });
    }
  }
  console.log(`daemon installed: platform=${platform} path=${path}`);
  console.log(
    platform === "systemd"
      ? `activation: systemctl --user daemon-reload && systemctl --user enable --now ${BRAINSTACK_DAEMON_SERVICE} && systemctl --user restart ${BRAINSTACK_DAEMON_SERVICE}`
      : `activation: launchctl bootstrap gui/$(id -u) ${shellSingleQuote(path)}`
  );
}

async function commandDaemonUninstall(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
  const platform = daemonPlatform(args);
  const path = daemonServicePath(cfg, platform);
  if (hasFlag(args, "dry-run")) {
    console.log(`would remove daemon service: platform=${platform} path=${path}`);
    return;
  }
  if (platform === "systemd") {
    run(["systemctl", "--user", "disable", "--now", BRAINSTACK_DAEMON_SERVICE], { check: false });
    await rm(path, { force: true });
    run(["systemctl", "--user", "daemon-reload"], { check: false });
    run(["systemctl", "--user", "reset-failed", BRAINSTACK_DAEMON_SERVICE], { check: false });
  } else {
    const uid = typeof process.getuid === "function" ? process.getuid() : null;
    const domain = uid === null ? `gui/${process.env.UID || ""}` : `gui/${uid}`;
    run(["launchctl", "bootout", domain, path], { check: false });
    await rm(path, { force: true });
  }
  console.log(`daemon uninstalled: platform=${platform} path=${path}`);
}

function tailLines(text: string, count: number): string {
  const lines = text.split(/\r?\n/);
  return lines.slice(Math.max(0, lines.length - count)).join("\n");
}

async function commandDaemonLogs(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
  const lines = parsePositiveIntegerFlag(args, "lines", 80);
  const logs = daemonLogPaths(cfg);
  for (const [label, path] of Object.entries(logs)) {
    console.log(`== ${label}: ${path} ==`);
    if (!existsSync(path)) {
      console.log("(missing)");
      continue;
    }
    const info = await lstat(path).catch(() => null);
    if (!info || info.isSymbolicLink() || !info.isFile() || info.size > 1024 * 1024) {
      console.log("(not a regular log file or too large for inline display)");
      continue;
    }
    console.log(tailLines(await readFile(path, "utf8"), lines));
  }
}

async function commandDaemon(args: ParsedArgs): Promise<void> {
  const subcommand = args.positional[0] || "status";
  switch (subcommand) {
    case "run":
      return await commandDaemonRun(args);
    case "once":
      return await commandDaemonRun({ ...args, positional: ["once"], flags: { ...args.flags, once: true } });
    case "status":
      return await commandDaemonStatus(args);
    case "install":
      return await commandDaemonInstall(args);
    case "uninstall":
    case "remove":
      return await commandDaemonUninstall(args);
    case "logs":
      return await commandDaemonLogs(args);
    case "help":
    case "--help":
    case "-h":
      console.log("Usage: brainctl daemon run|once|status|install|uninstall|logs --config brainstack.yaml [--target codex|claude|cursor|all] [--interval-seconds N] [--platform launchd|systemd] [--json]");
      return;
    default:
      throw new Error(`Unknown daemon subcommand: ${subcommand}`);
  }
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
  const payload: Record<string, unknown> = {
    title,
    body,
    source_harness: flag(args, "source-harness") || cfg.harness.name,
    source_machine: flag(args, "source-machine") || cfg.machine.name
  };
  // Machine-proposal fields for curator-generated wiki changes.
  const targetPage = requireFlagValue(args, "target-page");
  if (targetPage) {
    payload.target_page = targetPage;
  }
  const contentFile = requireFlagValue(args, "content-file");
  if (contentFile) {
    if (!targetPage) {
      throw new Error("--content-file requires --target-page");
    }
    payload.proposed_content = await readFile(abs(contentFile), "utf8");
  }
  const baseSha = requireFlagValue(args, "base-sha256");
  if (baseSha) {
    payload.base_sha256 = baseSha;
  }
  const risk = requireFlagValue(args, "risk");
  if (risk) {
    if (!["low", "medium", "high"].includes(risk)) {
      throw new Error("--risk must be low, medium, or high");
    }
    payload.risk = risk;
  }
  const confidence = requireFlagValue(args, "confidence");
  if (confidence) {
    const parsed = Number(confidence);
    if (!Number.isFinite(parsed) || parsed < 0 || parsed > 1) {
      throw new Error("--confidence must be a number between 0 and 1");
    }
    payload.confidence = parsed;
  }
  const curatorRunId = requireFlagValue(args, "curator-run-id");
  if (curatorRunId) {
    payload.curator_run_id = curatorRunId;
  }
  const reason = requireFlagValue(args, "reason");
  if (reason) {
    payload.reason = reason;
  }
  if (hasFlag(args, "needs-human")) {
    payload.status = "needs-human";
  }
  const sourceIds = requireFlagValue(args, "source-ids");
  if (sourceIds) {
    payload.source_ids = sourceIds.split(",").map((value) => value.trim()).filter(Boolean);
  }
  const sourceType = requireFlagValue(args, "source-type");
  if (sourceType) {
    payload.source_type = sourceType;
  }
  const relatedRepo = requireFlagValue(args, "related-repo");
  if (relatedRepo) {
    payload.related_repo = relatedRepo;
  }
  const project = requireFlagValue(args, "project");
  if (project) {
    payload.project = project;
  }
  const domain = requireFlagValue(args, "domain");
  if (domain) {
    payload.domain = domain;
  }
  const scope = requireFlagValue(args, "scope");
  if (scope) {
    payload.scope = validateMemoryScope(scope);
  }
  const memoryKind = requireFlagValue(args, "memory-kind") || requireFlagValue(args, "kind");
  if (memoryKind) {
    payload.memory_kind = memoryKind;
  }
  const context = requireFlagValue(args, "context");
  if (context) {
    payload.context = context;
  }
  const applicability = requireFlagValue(args, "applicability");
  if (applicability) {
    payload.applicability = applicability;
  }
  const nonApplicability = requireFlagValue(args, "non-applicability") || requireFlagValue(args, "non_applicability");
  if (nonApplicability) {
    payload.non_applicability = nonApplicability;
  }
  const evidenceRefs = flagValues(args, "evidence");
  if (evidenceRefs.length) {
    payload.evidence_refs = evidenceRefs;
  }
  const reviewAfter = requireFlagValue(args, "review-after");
  if (reviewAfter) {
    payload.review_after = reviewAfter;
  }
  const expiresAt = requireFlagValue(args, "expires-at");
  if (expiresAt) {
    payload.expires_at = expiresAt;
  }
  await postBrainWriteOrQueue(cfg, "propose", payload);
}

function brainApiBaseUrl(cfg: BrainstackConfig): string {
  const env = clientEnv(cfg);
  const fromEnv = process.env.BRAIN_BASE_URL || env.BRAIN_BASE_URL || cfg.brain.publicBaseUrl;
  if (fromEnv) {
    return fromEnv;
  }
  if (runsBraind(cfg)) {
    return `http://${cfg.security.bindHost}:${cfg.brain.port}`;
  }
  throw new Error("no brain base URL configured; set brain.publicBaseUrl or BRAIN_BASE_URL");
}

function brainAdminToken(cfg: BrainstackConfig): string {
  const token =
    process.env.BRAIN_ADMIN_TOKEN || readEnvFile(join(cfg.paths.configRoot, "braind.secrets.env")).BRAIN_ADMIN_TOKEN || "";
  if (!token) {
    throw new Error("BRAIN_ADMIN_TOKEN is required for this action; run it on the control host or export BRAIN_ADMIN_TOKEN");
  }
  return token;
}

async function brainApiRequest(
  cfg: BrainstackConfig,
  method: "GET" | "POST",
  path: string,
  options: { admin?: boolean; body?: Record<string, unknown> } = {}
): Promise<Record<string, unknown>> {
  const baseUrl = brainApiBaseUrl(cfg);
  const headers: Record<string, string> = {};
  if (options.admin) {
    headers.Authorization = `Bearer ${brainAdminToken(cfg)}`;
  }
  if (options.body) {
    headers["Content-Type"] = "application/json";
  }
  const response = await fetch(new URL(path, baseUrl).toString(), {
    method,
    headers,
    body: options.body ? JSON.stringify(options.body) : undefined,
    signal: AbortSignal.timeout(brainWriteTimeoutMs())
  });
  const text = await response.text();
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(text) as Record<string, unknown>;
  } catch {
    throw new Error(`brain ${method} ${path} returned non-JSON HTTP ${response.status}`);
  }
  if (!response.ok) {
    throw new Error(`brain ${method} ${path} failed with HTTP ${response.status}: ${String(parsed.error || "").slice(0, 500)}`);
  }
  return parsed;
}

async function fetchProposalDetail(cfg: BrainstackConfig, id: string): Promise<{ proposal: Record<string, unknown>; body: string }> {
  const result = await brainApiRequest(cfg, "GET", `/api/proposals/${encodeURIComponent(id)}`);
  const proposal = result.proposal && typeof result.proposal === "object" && !Array.isArray(result.proposal) ? (result.proposal as Record<string, unknown>) : {};
  return { proposal, body: typeof result.body === "string" ? result.body : "" };
}

function proposalNeedsContext(proposal: Record<string, unknown>): boolean {
  const quality = stringFromRecord(proposal, "quality_decision") || "";
  const status = stringFromRecord(proposal, "status") || "";
  return Boolean(proposal.legacy_format) || quality === "needs-context" || status === "needs-human";
}

async function createEnrichedProposal(
  cfg: BrainstackConfig,
  args: ParsedArgs,
  id: string
): Promise<{ id: string; payload: Record<string, unknown>; dryRun: boolean; write?: BrainWriteOutcome }> {
  const { proposal, body } = await fetchProposalDetail(cfg, id);
  const payload = proposalEnrichmentPayload(cfg, args, { ...proposal, id: stringFromRecord(proposal, "id") || id }, body);
  if (hasFlag(args, "dry-run")) {
    return { id, payload, dryRun: true };
  }
  const write = await postBrainWriteOrQueue(cfg, "propose", payload, {}, { quiet: hasFlag(args, "json") });
  return { id, payload, dryRun: false, write };
}

function formatProposalLine(proposal: Record<string, unknown>): string {
  return [
    String(proposal.id),
    `status=${String(proposal.status)}`,
    proposal.legacy_format ? "legacy=yes" : null,
    proposal.cluster_label ? `group=${String(proposal.cluster_label)}` : null,
    proposal.target_page ? `target=${String(proposal.target_page)}` : null,
    proposal.risk ? `risk=${String(proposal.risk)}` : null,
    proposal.quality_decision ? `quality=${String(proposal.quality_decision)}` : null,
    proposal.confidence !== null && proposal.confidence !== undefined ? `confidence=${String(proposal.confidence)}` : null,
    `created=${String(proposal.created_at)}`,
    `title=${String(proposal.title)}`
  ]
    .filter(Boolean)
    .join(" ");
}

function formatProposalClusterLine(cluster: Record<string, unknown>): string {
  return [
    String(cluster.id),
    `count=${String(cluster.count)}`,
    cluster.legacyCount !== undefined ? `legacy=${String(cluster.legacyCount)}` : null,
    cluster.needsContextCount !== undefined ? `needs_context=${String(cluster.needsContextCount)}` : null,
    cluster.label ? `label=${String(cluster.label)}` : null,
    Array.isArray(cluster.proposalIds) ? `proposals=${(cluster.proposalIds as string[]).join(",")}` : null
  ]
    .filter(Boolean)
    .join(" ");
}

type ProposalDecisionAction = "approve" | "reject" | "apply";

function proposalDecisionControlSshTarget(cfg: BrainstackConfig, args: ParsedArgs): string | null {
  const via = requireFlagValue(args, "via") || process.env.BRAINSTACK_TELEGRAM_VIA?.trim() || cfg.client.telegramVia;
  return via ? validateInviteSshTarget(via, "proposal decision SSH target") : null;
}

function proposalDecisionRemoteScript(remoteRepo: string, action: ProposalDecisionAction, id: string, reason?: string): string {
  const reasonArgs = reason ? ` --reason ${quoteForBash(reason)}` : "";
  return `
set -euo pipefail
brainstack_expand_home() {
  case "$1" in
    \\~) printf '%s\\n' "$HOME" ;;
    \\~/*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}
repo="$(brainstack_expand_home ${quoteForBash(remoteRepo)})"
cd "$repo"
if [ -x "$HOME/.bun/bin/bun" ]; then
  bun_bin="$HOME/.bun/bin/bun"
else
  bun_bin="$(command -v bun)"
fi
"$bun_bin" --no-env-file run packages/brainctl/src/main.ts proposals ${action} ${quoteForBash(id)} --config "$HOME/.config/brainstack/brainstack.yaml"${reasonArgs}
`.trim();
}

async function maybeRunRemoteProposalDecision(cfg: BrainstackConfig, args: ParsedArgs, action: ProposalDecisionAction, id: string, reason?: string): Promise<boolean> {
  if (cfg.telemux.enabled || process.env.BRAIN_ADMIN_TOKEN || readEnvFile(join(cfg.paths.configRoot, "braind.secrets.env")).BRAIN_ADMIN_TOKEN) {
    return false;
  }
  const via = proposalDecisionControlSshTarget(cfg, args);
  if (!via) {
    return false;
  }
  const worker = telegramControlWorker(via);
  const remoteRepo = requireFlagValue(args, "remote-repo") || process.env.BRAINSTACK_TELEGRAM_REMOTE_REPO?.trim() || cfg.client.telegramRemoteRepo || "~/brainstack";
  const knownHostsPath = telegramKnownHostsPath(cfg, args);
  const sshTrustMode = telegramSshTrustMode(args);
  if (sshTrustMode === "accept-new") {
    await ensureDir(dirname(knownHostsPath));
  }
  const remoteScript = proposalDecisionRemoteScript(remoteRepo, action, id, reason);
  const result = run(
    [
      "ssh",
      "-o",
      "BatchMode=yes",
      "-o",
      "ConnectTimeout=8",
      ...telegramSshTrustArgs(sshTrustMode, knownHostsPath),
      ...workerSshPortArgs(worker),
      workerRemoteTarget(worker),
      `bash -lc ${quoteForBash(remoteScript)}`
    ],
    { check: false, timeoutMs: 120_000 }
  );
  if (result.code !== 0) {
    throw new Error(`proposal ${action} failed over ssh with exit ${result.code}\n${result.stderr || result.stdout}`);
  }
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);
  return true;
}

async function commandProposals(args: ParsedArgs): Promise<void> {
  const sub = args.positional[0] || "list";
  if (sub === "help" || sub === "--help" || sub === "-h") {
    console.log(
      "Usage: brainctl proposals list [--status open|pending|approved|applied|rejected|superseded|needs-human] [--json]\n       brainctl proposals groups [--status open|pending|approved|applied|rejected|superseded|needs-human] [--min-size N] [--json]\n       brainctl proposals show <id> [--json]\n       brainctl proposals merge-group <group-key|group-label> [--submit] [--limit N|--all] [--target-page wiki/PATH.md] [--needs-human] [--close-sources] [--json]\n       brainctl proposals approve <id> [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]\n       brainctl proposals reject <id> [--reason TEXT] [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]\n       brainctl proposals apply <id> [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]"
      + "\n       brainctl proposals enrich <id> [--summary TEXT] [--project NAME] [--domain NAME] [--scope repo|project|global|machine|harness] [--memory-kind KIND] [--applicability TEXT] [--non-applicability TEXT] [--evidence REF] [--dry-run|--json]"
        + "\n       brainctl proposals reprocess [--status needs-human|open] [--group KEY] [--cluster KEY] [--id ID] [--limit N] [--apply] [--json] [enrichment flags...]"
    );
    return;
  }
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const decidedBy = `${process.env.USER || "operator"}@${cfg.machine.name}`;
  switch (sub) {
    case "list": {
      const status = requireFlagValue(args, "status") || "open";
      const result = await brainApiRequest(cfg, "GET", `/api/proposals?status=${encodeURIComponent(status)}`);
      if (hasFlag(args, "json")) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }
      const proposals = Array.isArray(result.proposals) ? (result.proposals as Array<Record<string, unknown>>) : [];
      console.log(`mode=${String(result.mode)} proposals=${proposals.length} (status filter: ${status})`);
      for (const proposal of proposals) {
        console.log(formatProposalLine(proposal));
      }
      const clusters = proposalReviewGroupsFromResult(result);
      if (clusters.length) {
        console.log(`review_groups=${clusters.length}`);
        for (const cluster of clusters) {
          console.log(formatProposalClusterLine(cluster));
        }
      }
      return;
    }
    case "groups":
    case "clusters": {
      const status = requireFlagValue(args, "status") || "open";
      const minSize = requireFlagValue(args, "min-size") || requireFlagValue(args, "min") || "2";
      const result = await brainApiRequest(
        cfg,
        "GET",
        `/api/proposals/groups?status=${encodeURIComponent(status)}&min_size=${encodeURIComponent(minSize)}`
      );
      if (hasFlag(args, "json")) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }
      const clusters = proposalReviewGroupsFromResult(result);
      console.log(`proposal review groups=${clusters.length} (status filter: ${status}, min size: ${String(result.min_size || minSize)})`);
      for (const cluster of clusters) {
        console.log(formatProposalClusterLine(cluster));
      }
      return;
    }
    case "show": {
      const id = args.positional[1];
      if (!id) {
        throw new Error("proposals show requires a proposal id");
      }
      const result = await brainApiRequest(cfg, "GET", `/api/proposals/${encodeURIComponent(id)}`);
      if (hasFlag(args, "json")) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }
      const proposal = (result.proposal || {}) as Record<string, unknown>;
      console.log(formatProposalLine(proposal));
      if (proposal.reason) {
        console.log(`reason: ${String(proposal.reason)}`);
      }
      if (Array.isArray(proposal.source_ids) && proposal.source_ids.length) {
        console.log(`sources: ${(proposal.source_ids as string[]).join(", ")}`);
      }
      console.log("");
      console.log(String(result.body || ""));
      if (result.diff) {
        console.log("");
        console.log("--- proposed change ---");
        console.log(String(result.diff));
      }
      return;
    }
    case "enrich": {
      const id = args.positional[1];
      if (!id) {
        throw new Error("proposals enrich requires a proposal id");
      }
      const result = await createEnrichedProposal(cfg, args, id);
      if (hasFlag(args, "json")) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }
      console.log(
        [
          `proposal=${id}`,
          result.dryRun ? "dry_run=true" : `enriched=${result.write?.status || "submitted"}`,
          `title=${String(result.payload.title)}`,
          `project=${String(result.payload.project)}`,
          `scope=${String(result.payload.scope)}`,
          `quality_input=evidence_refs:${Array.isArray(result.payload.evidence_refs) ? result.payload.evidence_refs.length : 0}`
        ].join(" ")
      );
      return;
    }
    case "reprocess": {
      const status = requireFlagValue(args, "status") || "needs-human";
      const limit = parsePositiveIntegerFlag(args, "limit", 10);
      const apply = hasFlag(args, "apply");
      const result = await brainApiRequest(cfg, "GET", `/api/proposals?status=${encodeURIComponent(status)}`);
      const proposals = Array.isArray(result.proposals) ? (result.proposals as Array<Record<string, unknown>>) : [];
      const ids = new Set(flagValues(args, "id"));
      const cluster = requireFlagValue(args, "group") || requireFlagValue(args, "cluster");
      const selected = proposals
        .filter((proposal) => !ids.size || ids.has(stringFromRecord(proposal, "id") || ""))
        .filter((proposal) => !cluster || stringFromRecord(proposal, "cluster_key") === cluster || stringFromRecord(proposal, "cluster_label") === cluster)
        .filter((proposal) => hasFlag(args, "all") || proposalNeedsContext(proposal))
        .slice(0, limit);
      const outputs: Array<{ id: string; payload: Record<string, unknown>; dryRun: boolean; write?: BrainWriteOutcome }> = [];
      for (const proposal of selected) {
        const id = stringFromRecord(proposal, "id");
        if (!id) {
          continue;
        }
        outputs.push(await createEnrichedProposal(cfg, { ...args, flags: apply ? args.flags : { ...args.flags, "dry-run": true } }, id));
      }
      if (hasFlag(args, "json")) {
        console.log(JSON.stringify({ apply, status, selected: selected.length, results: outputs }, null, 2));
        return;
      }
      console.log(`${apply ? "reprocessed" : "dry-run reprocess plan"} proposals=${outputs.length} status=${status}`);
      for (const item of outputs) {
        console.log(`${item.id} -> ${String(item.payload.title)} project=${String(item.payload.project)} scope=${String(item.payload.scope)}`);
      }
      if (!apply) {
        console.log("No writes performed. Rerun with --apply to create enriched replacement proposals.");
      }
      return;
    }
    case "merge-group":
    case "merge-cluster": {
      const groupKey = args.positional[1] || requireFlagValue(args, "group") || requireFlagValue(args, "cluster");
      if (!groupKey) {
        throw new Error("proposals merge-group requires a review group key or label");
      }
      const result = await createReviewGroupMergeProposal(cfg, args, groupKey);
      if (hasFlag(args, "json")) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }
      console.log(
        [
          `review_group=${groupKey}`,
          `selected=${result.selected}`,
          result.dryRun ? "dry_run=true" : `merged=${result.write?.status || "submitted"}`,
          `target=${String(result.payload.target_page)}`,
          result.conflicts.length ? `needs_human=${result.conflicts.join("; ")}` : "needs_human=false",
          result.closed?.length ? `closed_sources=${result.closed.join(",")}` : null
        ]
          .filter(Boolean)
          .join(" ")
      );
      if (result.dryRun) {
        console.log("No writes performed. Rerun with --submit to create the consolidated proposal.");
      }
      return;
    }
    case "approve":
    case "reject":
    case "apply": {
      const id = args.positional[1];
      if (!id) {
        throw new Error(`proposals ${sub} requires a proposal id`);
      }
      const body: Record<string, unknown> = { decided_by: decidedBy };
      const reason = requireFlagValue(args, "reason");
      if (reason) {
        body.reason = reason;
      }
      if (await maybeRunRemoteProposalDecision(cfg, args, sub, id, reason)) {
        return;
      }
      const result = await brainApiRequest(cfg, "POST", `/api/proposals/${encodeURIComponent(id)}/${sub}`, { admin: true, body });
      console.log(
        [
          `proposal=${id}`,
          `action=${sub}`,
          `status=${String(result.status)}`,
          result.blocked_reason ? `blocked=${String(result.blocked_reason)}` : null,
          Array.isArray(result.superseded_ids) && result.superseded_ids.length ? `superseded=${(result.superseded_ids as string[]).join(",")}` : null,
          result.commit ? `commit=${String(result.commit)}` : null
        ]
          .filter(Boolean)
          .join(" ")
      );
      return;
    }
    default:
      throw new Error(`Unknown proposals subcommand: ${sub}`);
  }
}

async function telemuxControlRequest(cfg: BrainstackConfig, path: string): Promise<Record<string, unknown>> {
  if (!cfg.telemux.enabled) {
    throw new Error("telemux is not enabled in this config; the curator routine runs through telemux. Enable telemux or run the brain-curator skill manually.");
  }
  const headers: Record<string, string> = {};
  const dashboardToken = process.env.FACTORY_DASHBOARD_TOKEN?.trim();
  if (dashboardToken) {
    headers.Authorization = `Bearer ${dashboardToken}`;
  }
  const response = await fetch(`http://${cfg.telemux.dashboardHost}:${cfg.telemux.dashboardPort}${path}`, {
    method: "POST",
    headers,
    signal: AbortSignal.timeout(120_000)
  });
  const text = await response.text();
  let parsed: Record<string, unknown>;
  try {
    parsed = JSON.parse(text) as Record<string, unknown>;
  } catch {
    throw new Error(`telemux control ${path} returned non-JSON HTTP ${response.status}; is telemux running?`);
  }
  if (!response.ok) {
    throw new Error(`telemux control ${path} failed with HTTP ${response.status}: ${String(parsed.error || "").slice(0, 500)}`);
  }
  return parsed;
}

function curatorControlSshTarget(cfg: BrainstackConfig, args: ParsedArgs): string | null {
  const via = requireFlagValue(args, "via") || process.env.BRAINSTACK_TELEGRAM_VIA?.trim() || cfg.client.telegramVia || sshTargetFromRemoteSsh(cfg.client.remoteSsh);
  return via ? validateInviteSshTarget(via, "curator control SSH target") : null;
}

function curatorRemoteControlScript(remoteRepo: string, subcommand: "run" | "install"): string {
  return `
set -euo pipefail
brainstack_expand_home() {
  case "$1" in
    \\~) printf '%s\\n' "$HOME" ;;
    \\~/*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}
repo="$(brainstack_expand_home ${quoteForBash(remoteRepo)})"
cd "$repo"
if [ -x "$HOME/.bun/bin/bun" ]; then
  bun_bin="$HOME/.bun/bin/bun"
else
  bun_bin="$(command -v bun)"
fi
"$bun_bin" --no-env-file run packages/brainctl/src/main.ts curator ${subcommand} --config "$HOME/.config/brainstack/brainstack.yaml"
`.trim();
}

async function maybeRunRemoteCuratorControl(cfg: BrainstackConfig, args: ParsedArgs, subcommand: "run" | "install"): Promise<boolean> {
  if (cfg.telemux.enabled) {
    return false;
  }
  const via = curatorControlSshTarget(cfg, args);
  if (!via) {
    return false;
  }
  const worker = telegramControlWorker(via);
  const remoteRepo = requireFlagValue(args, "remote-repo") || process.env.BRAINSTACK_TELEGRAM_REMOTE_REPO?.trim() || cfg.client.telegramRemoteRepo || "~/brainstack";
  const knownHostsPath = telegramKnownHostsPath(cfg, args);
  const sshTrustMode = telegramSshTrustMode(args);
  if (sshTrustMode === "accept-new") {
    await ensureDir(dirname(knownHostsPath));
  }
  const remoteScript = curatorRemoteControlScript(remoteRepo, subcommand);
  const result = run(
    [
      "ssh",
      "-o",
      "BatchMode=yes",
      "-o",
      "ConnectTimeout=8",
      ...telegramSshTrustArgs(sshTrustMode, knownHostsPath),
      ...workerSshPortArgs(worker),
      workerRemoteTarget(worker),
      `bash -lc ${quoteForBash(remoteScript)}`
    ],
    { check: false, timeoutMs: 120_000 }
  );
  if (result.code !== 0) {
    throw new Error(`curator ${subcommand} failed over ssh with exit ${result.code}\n${result.stderr || result.stdout}`);
  }
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);
  return true;
}

async function commandCurator(args: ParsedArgs): Promise<void> {
  const sub = args.positional[0] || "status";
  if (sub === "help" || sub === "--help" || sub === "-h") {
    console.log("Usage: brainctl curator status [--json]\n       brainctl curator run [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]\n       brainctl curator install [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]");
    return;
  }
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  switch (sub) {
    case "status": {
      const result = await brainApiRequest(cfg, "GET", "/api/curator/status");
      if (hasFlag(args, "json")) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }
      const curator = (result.curator || {}) as Record<string, unknown>;
      const counts = (result.proposal_counts || {}) as Record<string, number>;
      console.log(`mode=${String(result.mode)}`);
      console.log(`installed=${curator.installed ? "yes" : "no"}`);
      console.log(`last_run=${String(curator.last_run_finished_at || "(never)")} ok=${curator.last_run_ok === null || curator.last_run_ok === undefined ? "n/a" : String(curator.last_run_ok)}`);
      console.log(`next_run=${String(curator.next_run_at || "(not scheduled)")}`);
      console.log(`cursor=${String(curator.cursor || "(none)")}`);
      console.log(
        `proposals: pending=${counts.pending || 0} approved=${counts.approved || 0} needs-human=${counts["needs-human"] || 0} applied=${counts.applied || 0} rejected=${counts.rejected || 0} superseded=${counts.superseded || 0}`
      );
      const failures = Array.isArray(curator.last_run_failures) ? (curator.last_run_failures as string[]) : [];
      if (failures.length) {
        console.log("last run failures:");
        for (const failure of failures) {
          console.log(`  - ${failure}`);
        }
      }
      if (curator.last_run_summary) {
        console.log(`last run summary: ${String(curator.last_run_summary)}`);
      }
      return;
    }
    case "run": {
      if (await maybeRunRemoteCuratorControl(cfg, args, "run")) {
        return;
      }
      const result = await telemuxControlRequest(cfg, "/control/curator/run");
      console.log(String(result.message || "curator run requested"));
      return;
    }
    case "install": {
      if (await maybeRunRemoteCuratorControl(cfg, args, "install")) {
        return;
      }
      const result = await telemuxControlRequest(cfg, "/control/curator/install");
      console.log(String(result.message || "curator install requested"));
      return;
    }
    default:
      throw new Error(`Unknown curator subcommand: ${sub}`);
  }
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
    case "retry": {
      const requeued = await retryOutboxItems(cfg, args, scans);
      console.log(`requeued=${requeued}`);
      console.log("Run `brainctl outbox flush` to replay the requeued item(s).");
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
      throw new Error("outbox subcommand must be status|list|flush|retry|purge|purge-corrupt");
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

function gitUpdateProbe(productRoot: string, args: string[]): ReturnType<typeof run> {
  return run(["git", ...args], { cwd: productRoot, check: false, timeoutMs: updateProbeTimeoutMs() });
}

function harnessVersionSummary(item: DoctorCheck): string {
  if (item.detail.endsWith(" not found in PATH")) {
    return "missing";
  }
  return item.detail.split(";")[0] || item.detail;
}

async function commandUpdates(args: ParsedArgs): Promise<void> {
  const cfg = await loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
  const productRoot = cfg.paths.productRepo || PRODUCT_ROOT;
  const productGitAvailable = gitExists(productRoot);
  const branch = productGitAvailable ? gitUpdateProbe(productRoot, ["rev-parse", "--abbrev-ref", "HEAD"]).stdout.trim() || "unknown" : "unavailable";
  const head = productGitAvailable ? gitUpdateProbe(productRoot, ["rev-parse", "HEAD"]).stdout.trim() || "unavailable" : "unavailable";
  const remoteCandidates = ["origin/main", "refs/remotes/https-main"];
  const remoteRef =
    productGitAvailable ? remoteCandidates.find((candidate) => gitUpdateProbe(productRoot, ["rev-parse", "--verify", candidate]).code === 0) || null : null;
  const origin = remoteRef ? gitUpdateProbe(productRoot, ["rev-parse", "--verify", remoteRef]).stdout.trim() : "";
  const aheadBehind = remoteRef
    ? gitUpdateProbe(productRoot, ["rev-list", "--left-right", "--count", `HEAD...${remoteRef}`]).stdout.trim()
    : "unknown";
  const compat = [
    harnessCompatibility("codex", "codex", { required: cfg.harness.name === "codex" }),
    harnessCompatibility("claude", "claude", { required: cfg.harness.name === "claude" })
  ];
  const codex = harnessVersionSummary(compat[0]);
  const claude = harnessVersionSummary(compat[1]);
  console.log(`brainstack_source=${productGitAvailable ? productRoot : "not-installed"}`);
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
  if (productGitAvailable) {
    console.log(`  brainstack: cd ${productRoot} && git pull --ff-only && bun install --frozen-lockfile && bun test`);
  } else if (cfg.profile.startsWith("client")) {
    console.log("  brainstack: rerun the Brainstack installer or replace ~/.local/bin/brainctl from a signed release");
  } else {
    console.log(`  brainstack: install or repair the product checkout at ${productRoot}`);
  }
  console.log("  os packages: use your package manager manually after reviewing the read-only check above");
  console.log(`  selected harness: ${installHint(cfg.harness.name)}`);
}

function formatFleetStatus(report: FleetStatusReport): string {
  const lines = [
    `fleet: ok=${report.ok} degraded=${report.degraded} machines=${report.summary.total} reachable=${report.summary.reachable} needs_update=${report.summary.needs_update} unhealthy=${report.summary.unhealthy}`
  ];
  for (const machine of report.machines) {
    lines.push(
      [
        `${machine.status.toUpperCase()} ${machine.name}`,
        `role=${machine.role}`,
        `transport=${machine.transport}`,
        `state=${machine.update_state}`,
        machine.short ? `head=${machine.short}` : null,
        machine.behind ? `behind=${machine.behind}` : null,
        machine.ahead ? `ahead=${machine.ahead}` : null,
        machine.dirty_count ? `dirty=${machine.dirty_count}` : null,
        `detail=${machine.detail}`
      ]
        .filter(Boolean)
        .join(" ")
    );
  }
  return lines.join("\n");
}

function fleetUpdateScript(configPath: string, role: FleetMachineStatus["role"], productRepo: string, dryRun: boolean): string {
  const services = role === "worker" ? ["brainstackd.service"] : ["braind.service", "telemux.service", "brainstackd.service"];
  const commands = [
    "set -euo pipefail",
    "brainstack_expand_home() {",
    "  case \"$1\" in",
    "    \"~\") printf '%s\\n' \"$HOME\" ;;",
    "    \\~/*) printf '%s/%s\\n' \"$HOME\" \"${1#\\~/}\" ;;",
    "    *) printf '%s\\n' \"$1\" ;;",
    "  esac",
    "}",
    `repo="$(brainstack_expand_home ${quoteForBash(productRepo)})"`,
    `config="$(brainstack_expand_home ${quoteForBash(configPath)})"`,
    "brainctl_bin=\"$HOME/.local/bin/brainctl\"",
    "if [ -x \"$HOME/.bun/bin/bun\" ]; then bun_bin=\"$HOME/.bun/bin/bun\"; else bun_bin=\"$(command -v bun)\"; fi",
    "cd \"$repo\"",
    "git fetch --quiet origin main",
    "git merge --ff-only origin/main",
    "\"$bun_bin\" install --frozen-lockfile",
    "\"$bun_bin\" build packages/brainctl/src/main.ts --compile --no-compile-autoload-dotenv --no-compile-autoload-bunfig --outfile \"$brainctl_bin\"",
    "chmod 755 \"$brainctl_bin\"",
    "\"$brainctl_bin\" upgrade --config \"$config\"",
    "if command -v systemctl >/dev/null 2>&1; then",
    "  systemctl --user daemon-reload || true",
    ...services.map((service) => `  if systemctl --user cat ${quoteForBash(service)} >/dev/null 2>&1; then systemctl --user restart ${quoteForBash(service)}; fi`),
    "fi",
    "printf 'updated=true\\n'",
    "printf 'head=%s\\n' \"$(git rev-parse --short HEAD 2>/dev/null || true)\""
  ];
  if (!dryRun) {
    return commands.join("\n");
  }
  return commands.map((line) => `# ${line}`).join("\n");
}

interface FleetUpdateResult {
  machine: string;
  role: FleetMachineStatus["role"];
  ok: boolean;
  dry_run: boolean;
  code: number;
  output: string;
}

async function runFleetUpdateLocal(
  cfg: BrainstackConfig,
  machine: string,
  role: FleetMachineStatus["role"],
  configPath: string,
  dryRun: boolean
): Promise<FleetUpdateResult> {
  const script = fleetUpdateScript(configPath, role, cfg.paths.productRepo || PRODUCT_ROOT, dryRun);
  if (dryRun) {
    return { machine, role, ok: true, dry_run: true, code: 0, output: script };
  }
  const result = run(["bash", "-lc", script], { check: false, timeoutMs: 300_000 });
  return {
    machine,
    role,
    ok: result.code === 0,
    dry_run: false,
    code: result.code,
    output: summarizeLines(`${result.stdout}\n${result.stderr}`.trim(), 80)
  };
}

async function runFleetUpdateWorker(
  cfg: BrainstackConfig,
  worker: BrainstackWorkerConfig,
  configPath: string,
  dryRun: boolean
): Promise<FleetUpdateResult> {
  const script = fleetUpdateScript(configPath, "worker", cfg.paths.productRepo || "~/brainstack", dryRun);
  if (dryRun) {
    return { machine: worker.name, role: "worker", ok: true, dry_run: true, code: 0, output: script };
  }
  const result = runWorkerShell(cfg, worker, script, 300, true);
  return {
    machine: worker.name,
    role: "worker",
    ok: result.code === 0,
    dry_run: false,
    code: result.code,
    output: summarizeLines(`${result.stdout}\n${result.stderr}`.trim(), 80)
  };
}

async function runFleetUpdates(cfg: BrainstackConfig, args: ParsedArgs, configPath: string): Promise<FleetUpdateResult[]> {
  const dryRun = hasFlag(args, "dry-run");
  const target = args.positional[1];
  const updateAll = hasFlag(args, "all") || target === "all";
  if (!updateAll && !target) {
    throw new Error("fleet update requires a machine name or --all");
  }
  const results: FleetUpdateResult[] = [];
  const workers = defaultWorkers(cfg).filter((worker) => worker.name !== cfg.machine.name && worker.transport !== "local");
  if (updateAll) {
    for (const worker of workers) {
      results.push(await runFleetUpdateWorker(cfg, worker, configPath, dryRun));
    }
    if (cfg.profile === "control" || cfg.profile === "single-node") {
      results.push(await runFleetUpdateLocal(cfg, cfg.machine.name, "control", configPath, dryRun));
    } else if (cfg.profile === "worker") {
      results.push(await runFleetUpdateLocal(cfg, cfg.machine.name, "worker", configPath, dryRun));
    }
    return results;
  }
  if (target === cfg.machine.name) {
    const role: FleetMachineStatus["role"] = cfg.profile === "worker" ? "worker" : cfg.profile === "client-macos" ? "client" : "control";
    if (role === "client") {
      throw new Error("fleet update cannot self-update a standalone macOS client install; install a signed release or rebuild the local binary/app bundle");
    }
    results.push(await runFleetUpdateLocal(cfg, cfg.machine.name, role, configPath, dryRun));
    return results;
  }
  const worker = workers.find((candidate) => candidate.name === target);
  if (!worker) {
    throw new Error(`unknown fleet machine: ${target}`);
  }
  results.push(await runFleetUpdateWorker(cfg, worker, configPath, dryRun));
  return results;
}

async function commandFleet(args: ParsedArgs): Promise<void> {
  const sub = args.positional[0] || "status";
  if (sub === "help" || sub === "--help" || sub === "-h") {
    console.log("Usage: brainctl fleet status [--json] [--fetch|--no-fetch]\n       brainctl fleet update <machine|all> [--all] [--json] [--dry-run] [--via SSH_TARGET] [--remote-repo PATH]");
    return;
  }
  const configPath = abs(requireFlagValue(args, "config") || brainstackDefaultConfigPath());
  const cfg = await loadConfig(configPath, flag(args, "profile"), flag(args, "root"));
  if (sub === "status") {
    const report = await collectFleetStatus(cfg, args, parsePositiveIntegerFlag(args, "timeout-ms", 20_000), { fetch: !hasFlag(args, "no-fetch") });
    if (hasFlag(args, "json")) {
      console.log(JSON.stringify(report, null, 2));
    } else {
      console.log(formatFleetStatus(report));
    }
    return;
  }
  if (sub === "update") {
    if (cfg.profile === "client-macos") {
      const target = args.positional[1];
      const updateAll = hasFlag(args, "all") || target === "all";
      if (!updateAll && !target) {
        throw new Error("fleet update requires a machine name or --all");
      }
      const results: FleetUpdateResult[] = [];
      if (updateAll || clientFleetTargetIsControl(cfg, args, target)) {
        const bootstrap = runFleetControlBootstrapUpdate(cfg, args, hasFlag(args, "dry-run"));
        results.push(bootstrap);
        if (!bootstrap.ok) {
          if (hasFlag(args, "json")) {
            console.log(JSON.stringify({ ok: false, results }, null, 2));
          } else {
            console.log(`${bootstrap.ok ? "OK" : "FAIL"} ${bootstrap.machine} role=${bootstrap.role} dry_run=${bootstrap.dry_run} exit=${bootstrap.code}`);
            if (bootstrap.output) console.log(bootstrap.output);
          }
          throw new Error("control host bootstrap update failed");
        }
        if (!updateAll || hasFlag(args, "dry-run")) {
          if (hasFlag(args, "json")) {
            console.log(JSON.stringify({ ok: true, results }, null, 2));
          } else {
            console.log(`${bootstrap.ok ? "OK" : "FAIL"} ${bootstrap.machine} role=${bootstrap.role} dry_run=${bootstrap.dry_run} exit=${bootstrap.code}`);
            if (bootstrap.output) console.log(bootstrap.output);
          }
          return;
        }
      }
      const argv = ["fleet", "update", ...(hasFlag(args, "all") ? ["--all"] : args.positional[1] ? [args.positional[1]] : []), ...(hasFlag(args, "json") ? ["--json"] : []), ...(hasFlag(args, "dry-run") ? ["--dry-run"] : [])];
      const result = runFleetControlSsh(cfg, args, argv, 300_000);
      if (!result) {
        throw new Error("fleet update needs a control SSH target for client-macos installs");
      }
      if (result.stdout) process.stdout.write(result.stdout);
      if (result.stderr) process.stderr.write(result.stderr);
      if (result.code !== 0) {
        throw new Error(`fleet update failed over ssh with exit ${result.code}`);
      }
      return;
    }
    const results = await runFleetUpdates(cfg, args, configPath);
    if (hasFlag(args, "json")) {
      console.log(JSON.stringify({ ok: results.every((result) => result.ok), results }, null, 2));
    } else {
      for (const result of results) {
        console.log(`${result.ok ? "OK" : "FAIL"} ${result.machine} role=${result.role} dry_run=${result.dry_run} exit=${result.code}`);
        if (result.output) {
          console.log(result.output);
        }
      }
    }
    if (results.some((result) => !result.ok)) {
      throw new Error("one or more fleet updates failed");
    }
    return;
  }
  throw new Error(`Unknown fleet subcommand: ${sub}`);
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

function normalizedTarEntry(entry: string): string {
  return entry.replace(/^\.\//, "");
}

async function assertRestoreSourceTreeSafe(root: string): Promise<void> {
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    const info = await lstat(path);
    if (info.isSymbolicLink()) {
      throw new Error(`backup contains a symlink; refusing to restore: ${path}`);
    }
    if (info.isDirectory()) {
      await assertRestoreSourceTreeSafe(path);
      continue;
    }
    if (!info.isFile()) {
      throw new Error(`backup contains a non-regular file; refusing to restore: ${path}`);
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
  const force = hasFlag(args, "force");
  const isDirectorySource = statSync(source).isDirectory();

  // Validate the backup before touching the target: require the Brainstack backup
  // manifest, and for archives reject absolute or traversal member paths.
  if (isDirectorySource) {
    if (!existsSync(join(source, "MANIFEST.txt"))) {
      throw new Error(`backup directory is missing MANIFEST.txt; refusing to restore unverified content: ${source}`);
    }
    // Symlinks or special files in a backup tree could be followed by later
    // Brainstack reads/writes inside the restored target.
    await assertRestoreSourceTreeSafe(source);
  } else {
    const listing = run(["tar", "-tzf", source], { check: true, timeoutMs: 300_000 })
      .stdout.split("\n")
      .map((entry) => entry.trim())
      .filter(Boolean);
    if (!listing.length) {
      throw new Error(`backup archive is empty: ${source}`);
    }
    for (const entry of listing) {
      const normalizedEntry = normalizedTarEntry(entry);
      if (normalizedEntry.startsWith("/") || normalizedEntry.split("/").includes("..")) {
        throw new Error(`backup archive contains unsafe member path: ${entry}`);
      }
    }
    if (!listing.some((entry) => normalizedTarEntry(entry) === "MANIFEST.txt" || normalizedTarEntry(entry).endsWith("/MANIFEST.txt"))) {
      throw new Error(`backup archive is missing MANIFEST.txt; refusing to restore unverified content: ${source}`);
    }
    // Reject symlink, hardlink, and device/FIFO members before extraction; only
    // regular files and directories are restorable.
    const verbose = run(["tar", "-tvzf", source], { check: true, timeoutMs: 300_000 })
      .stdout.split("\n")
      .map((entry) => entry.trimEnd())
      .filter(Boolean);
    for (const line of verbose) {
      const typeChar = line[0];
      if (typeChar !== "-" && typeChar !== "d") {
        throw new Error(`backup archive contains a non-regular member (type '${typeChar}'); refusing to restore: ${line.slice(0, 200)}`);
      }
    }
  }

  const destEntries = existsSync(dest) ? await readdir(dest).catch(() => null) : [];
  if (destEntries === null) {
    throw new Error(`restore target exists and is not a directory: ${dest}`);
  }
  if (destEntries.length && !force) {
    throw new Error(`restore target is not empty: ${dest}; rerun with --force to swap it aside and restore`);
  }

  if (!hasFlag(args, "apply")) {
    console.log(`dry-run restore: would restore ${source} into ${dest}${destEntries.length ? " (existing content moved aside)" : ""}`);
    console.log("rerun with --apply to perform the restore");
    return;
  }

  // Restore into a fresh temp directory first, then swap into place, so a partial
  // extraction failure never leaves the target with mixed old/new state.
  const stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
  const tempDest = `${dest}.restore-tmp-${process.pid}-${stamp}`;
  await ensureDir(tempDest);
  await chmod(tempDest, 0o700).catch(() => undefined);
  try {
    if (isDirectorySource) {
      await cp(source, tempDest, { recursive: true, force: true, errorOnExist: false });
    } else {
      run(["tar", "-xzf", source, "-C", tempDest]);
    }
  } catch (error) {
    await rm(tempDest, { recursive: true, force: true }).catch(() => undefined);
    throw error;
  }

  if (existsSync(dest)) {
    if (destEntries.length) {
      const preservePath = `${dest}.pre-restore-${stamp}`;
      await rename(dest, preservePath);
      console.log(`existing target moved aside: ${preservePath}`);
    } else {
      await rmdir(dest);
    }
  } else {
    await ensureDir(dirname(dest));
  }
  await rename(tempDest, dest);
  console.log(`restore completed into ${dest}`);
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

  if (hasOwnershipManifest && usesBrainstackDaemon(cfg) && (scope === "all" || scope === "client" || scope === "worker") && !dryRun) {
    const platform = process.platform === "darwin" ? "launchd" : "systemd";
    const daemonPath = daemonServicePath(cfg, platform);
    if (platform === "systemd" && commandPath("systemctl")) {
      run(["systemctl", "--user", "disable", "--now", BRAINSTACK_DAEMON_SERVICE], { check: false });
    } else if (platform === "launchd" && commandPath("launchctl")) {
      const uid = typeof process.getuid === "function" ? process.getuid() : null;
      const domain = uid === null ? `gui/${process.env.UID || ""}` : `gui/${uid}`;
      run(["launchctl", "bootout", domain, daemonPath], { check: false });
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

  // The on-disk manifest is mutable state. Never trust its paths directly: validate
  // every entry against a freshly computed allowlist from the live config so a
  // corrupt or tampered manifest cannot direct destroy at arbitrary user paths.
  const allowedArtifactPaths = new Set<string>();
  for (const platform of ["launchd", "systemd"] as const) {
    for (const expected of expectedManagedArtifacts(cfg, platform)) {
      allowedArtifactPaths.add(absWithHome(expected.path, cfg.paths.home));
    }
  }

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
    if (typeof artifact.path !== "string" || !artifact.path.trim()) {
      skipped.push(`(manifest entry with empty path ignored)`);
      continue;
    }
    const resolvedArtifactPath = absWithHome(artifact.path, cfg.paths.home);
    if (!allowedArtifactPaths.has(resolvedArtifactPath)) {
      skipped.push(`${artifact.path} (not a brainstack-owned artifact for this config; manifest entry ignored)`);
      continue;
    }
    await removePath(resolvedArtifactPath, dryRun, removed);
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
    console.log("  systemctl --user reset-failed braind.service telemux.service brainstackd.service || true");
  }
}

async function commandBootstrapClient(args: ParsedArgs): Promise<void> {
  const out = flag(args, "out") || join(tmpdir(), `brainstack-client-bootstrap-${Date.now()}`);
  const cfg = await loadConfig(flag(args, "config"), "client-macos", flag(args, "root"));
  await rm(abs(out), { recursive: true, force: true });
  await writeFileMap(abs(out), clientBootstrapFiles(cfg));
  console.log(`client bootstrap files rendered to ${abs(out)}`);
}

function portableSkillNames(): PortableSkillName[] {
  return Object.keys(PORTABLE_SKILLS) as PortableSkillName[];
}

function normalizePortableSkillProfile(value: string | undefined): PortableSkillProfile {
  const normalized = (value || "client").trim().toLowerCase();
  if (normalized === "client" || normalized === "operator" || normalized === "control" || normalized === "worker") {
    return normalized;
  }
  throw new Error(`Unsupported skills profile: ${value}. Expected client, operator, control, or worker.`);
}

function normalizePortableSkillName(value: string): PortableSkillName {
  if (portableSkillNames().includes(value as PortableSkillName)) {
    return value as PortableSkillName;
  }
  throw new Error(`Unknown portable skill: ${value}. Available: ${portableSkillNames().join(", ")}`);
}

function selectedPortableSkills(args: ParsedArgs): PortableSkillName[] {
  if (hasFlag(args, "all")) {
    return portableSkillNames();
  }
  const explicit = flagValues(args, "skill").map(normalizePortableSkillName);
  if (explicit.length) {
    return [...new Set(explicit)];
  }
  return PORTABLE_SKILL_PROFILES[normalizePortableSkillProfile(requireFlagValue(args, "profile"))];
}

function portableSkillInstallRoot(args: ParsedArgs): string {
  const target = (requireFlagValue(args, "target") || "codex").trim().toLowerCase();
  if (target !== "codex") {
    throw new Error(`Unsupported skills target: ${target}. Only codex is supported today.`);
  }
  const explicitDir = requireFlagValue(args, "dir") || requireFlagValue(args, "out");
  if (explicitDir) {
    return abs(explicitDir);
  }
  const codexHome = process.env.CODEX_HOME ? abs(process.env.CODEX_HOME) : process.env.HOME ? join(process.env.HOME, ".codex") : "";
  if (!codexHome) {
    throw new Error("Cannot locate Codex home; set CODEX_HOME or pass --dir.");
  }
  return join(codexHome, "skills");
}

async function commandSkills(args: ParsedArgs): Promise<void> {
  const subcommand = args.positional[0] || "help";
  switch (subcommand) {
    case "list":
      console.log("portable skills:");
      for (const name of portableSkillNames()) {
        console.log(`  ${name}`);
      }
      console.log("profiles:");
      for (const [profile, skills] of Object.entries(PORTABLE_SKILL_PROFILES)) {
        console.log(`  ${profile}: ${skills.join(", ")}`);
      }
      return;
    case "install": {
      const root = portableSkillInstallRoot(args);
      const skills = selectedPortableSkills(args);
      const dryRun = hasFlag(args, "dry-run");
      const written: string[] = [];
      for (const name of skills) {
        const skillPath = join(root, name, "SKILL.md");
        written.push(skillPath);
        if (!dryRun) {
          await writeText(skillPath, `${PORTABLE_SKILLS[name].trimEnd()}\n`);
        }
      }
      console.log(`${dryRun ? "dry-run skills install plan" : "skills installed"}: target=codex root=${root}`);
      console.log(written.map((path) => `  ${path}`).join("\n"));
      return;
    }
    case "refresh": {
      const cfg = await loadConfig(flag(args, "config"), flag(args, "profile") || "client-macos", flag(args, "root"));
      await refreshBrainstackSkillPackages(cfg, args, { quiet: hasFlag(args, "quiet") });
      return;
    }
    case "doctor":
      return await commandSkillsDoctor(args);
    case "help":
    case "--help":
    case "-h":
      console.log("Usage: brainctl skills install [--target codex] [--profile client|operator|control|worker] [--skill NAME|--all] [--dir DIR] [--dry-run]\n       brainctl skills refresh [--target codex|claude|cursor] [--config brainstack.yaml] [--repo PATH] [--skill NAME] [--dir DIR] [--no-sync] [--force] [--quiet]\n       brainctl skills doctor [--target codex|claude|cursor] [--dir DIR] [--check-remote] [--json]\n       brainctl skills list");
      return;
    default:
      throw new Error(`Unknown skills subcommand: ${subcommand}`);
  }
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
  if (existsSync(knownHostsPath)) {
    const info = await lstat(knownHostsPath);
    if (info.isSymbolicLink() || !info.isFile()) {
      throw new Error(`refusing to update non-regular known-hosts file: ${knownHostsPath}`);
    }
  }
  const existing = existsSync(knownHostsPath) ? await readFile(knownHostsPath, "utf8") : "";
  const additions = scannedLines.filter((line) => !existing.split(/\r?\n/).includes(line));
  if (!additions.length) {
    // Pinned host topology should not be group/world readable even when no new
    // entries are added; tighten legacy 0644 files in passing.
    await chmod(knownHostsPath, 0o600).catch(() => undefined);
    console.log(`pinned host key lines already present for ${host} in ${knownHostsPath}`);
    return;
  }
  const prefix = existing && !existing.endsWith("\n") ? "\n" : "";
  await writeText(knownHostsPath, `${existing}${prefix}${additions.join("\n")}\n`, 0o600);
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

function processAliveLabel(pid: unknown): "yes" | "no" | "unknown" {
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
  const pidAlive = ownerHost && ownerHost !== localHost ? "unknown" : processAliveLabel(owner?.pid);
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
  await writeText(out, stringifySimpleYaml(current), 0o600);
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
    case "status":
      return await commandStatus(args);
    case "daemon":
      return await commandDaemon(args);
    case "updates":
      return await commandUpdates(args);
    case "fleet":
      return await commandFleet(args);
    case "expose":
      return await commandExpose(args);
    case "import":
      return await commandImport(args);
    case "import-text":
      return await commandImportText(args);
    case "propose":
      return await commandPropose(args);
    case "proposals":
      return await commandProposals(args);
    case "curator":
      return await commandCurator(args);
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
    case "skills":
      return await commandSkills(args);
    case "hooks":
      return await commandHooks(args);
    case "hook":
      return await commandHook(args);
    case "invite":
      return await commandInvite(args);
    case "enroll":
      return await commandEnroll(args);
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
    case "telegram":
      return await commandTelegram(args);
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
