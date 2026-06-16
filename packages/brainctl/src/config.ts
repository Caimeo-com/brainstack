import { existsSync } from "node:fs";
import { readFile, readdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
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
import { abs, absWithHome } from "./paths";

export const SUPPORTED_PROFILES = ["single-node", "control", "worker", "client-macos"] as const;
export type Profile = (typeof SUPPORTED_PROFILES)[number];
const RESERVED_PROFILE_ERRORS: Record<string, string> = {
  "private-journal":
    "Unsupported profile private-journal: first-class private journaling is not implemented yet. Use a separate explicit Brainstack install/config with separate repo paths and tokens."
};
export type SeedMode = "empty-only" | "missing" | "force";
export type HarnessName = "codex" | "claude";
export type HookTarget = "codex" | "claude" | "cursor";
export type DestroyScope = "control" | "worker" | "client" | "all";
export type CheckStatus = "PASS" | "WARN" | "FAIL";
export type WorkerSshTrustMode = "pinned" | "accept-new";
export type ControlSshTrustMode = WorkerSshTrustMode | "default";
export type SecurityPosture = "local" | "trusted-tailnet" | "guarded";
export type TrustedExposure = "none" | "tailscale-serve" | "vpn" | "manual";
export type VoiceCapabilityTarget = "local" | "worker";
export type VoiceCapabilityEngine = "whisperfile";

export const CONFIG_SCHEMA_VERSION = 1;
export const MIN_BUN_VERSION = "1.3.10";
export const BRAINSTACK_SKILL_PACKAGE_KIND = "brainstack.skill_package";
export const BRAINSTACK_HOOK_STATUS_MESSAGE = "Brainstack refresh/checkpoint";

export function truthyEnv(value: string | undefined): boolean {
  return ["1", "true", "yes", "on"].includes((value || "").trim().toLowerCase());
}

export function isSupportedProfile(value: string): value is Profile {
  return (SUPPORTED_PROFILES as readonly string[]).includes(value);
}

export interface BrainstackWorkerConfig {
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

export interface ManagedArtifact {
  path: string;
  kind: "file" | "dir" | "symlink" | "service" | "repo" | "tailscale-serve";
  scope: DestroyScope;
  reason: string;
  optional?: boolean;
}

export interface ManagedArtifactsManifest {
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

export interface DoctorCheck {
  section: string;
  name: string;
  status: CheckStatus;
  detail: string;
  remediation?: string;
}

export type BrainctlStatusState = "ok" | "warn" | "fail" | "disabled";

export interface BrainctlStatusSection<T = unknown> {
  state: BrainctlStatusState;
  ok: boolean | null;
  available: boolean;
  detail: string;
  data?: T;
  error?: string;
  duration_ms?: number;
}

export interface BrainctlStatusReport {
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

export interface BrainstackConfig {
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
  capabilities: {
    voice: {
      enabled: boolean;
      target: VoiceCapabilityTarget;
      worker: string | null;
      engine: VoiceCapabilityEngine;
      model: string;
      installRoot: string;
      command: string;
      args: string[];
      timeoutMs: number;
      echoTranscript: boolean;
      maxBytes: number;
      maxDurationSeconds: number | null;
      modelUrl: string;
      sha256: string | null;
      installedAt: string | null;
    };
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

export type CurationMode = "manual" | "approval" | "auto";
export const CURATION_MODES: CurationMode[] = ["manual", "approval", "auto"];
export const DEFAULT_CURATION_ALLOWED_PATHS = ["wiki/Status/**", "wiki/Sources/**"];

export interface BrainstackClientInvite {
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

export const PRODUCT_ROOT = resolve(import.meta.dir, "..", "..", "..");
export const TELEGRAM_SEND_DEFAULT_MAX_BYTES = 45 * 1024 * 1024;
export const TELEGRAM_SEND_SENSITIVE_FILE_PATTERN =
  /(?:^|[./_-])(?:id_rsa|id_ed25519|id_ecdsa|id_dsa|authorized_keys|known_hosts|token|tokens|secret|secrets|passwd|password|passwords|shadow|keyring|credential|credentials|apikey|api[_-]?keys?|private|cert|certs|certificate|kubeconfig|htpasswd|netrc|npmrc|pgpass|wallet|keystore|otp|totp|2fa)(?:$|[./_-])/i;
export const CLIENT_INVITE_PREFIX = "bs1_";
export const CLIENT_INVITE_TYPE = "brainstack-client-invite";
export const CLIENT_INVITE_MAX_CHARS = 128 * 1024;
export const CLIENT_INVITE_KNOWN_HOSTS_MAX_ENTRIES = 128;
export const CLIENT_INVITE_KNOWN_HOSTS_MAX_LINE_BYTES = 4096;
export const GITHUB_RELEASE_DOWNLOAD_BASE = "https://github.com/Caimeo-com/brainstack/releases/download";
export const LATEST_INSTALL_URL = "https://github.com/Caimeo-com/brainstack/releases/latest/download/install.sh";
export const BRAINSTACK_PACKAGE_VERSION = (() => {
  try {
    const parsed = JSON.parse(packageJsonText) as { version?: unknown };
    return typeof parsed.version === "string" && parsed.version.trim() ? parsed.version.trim() : "latest";
  } catch {
    return "latest";
  }
})();
export const CLIENT_BOOTSTRAP_TEMPLATE_NAMES = [
  "client.env.example",
  "codex-shared-brain.include.md",
  "codex-global-AGENTS.md",
  "claude-user-CLAUDE.md",
  "claude-hooks-example.json",
  "cursor-user-rule.md",
  "ssh_config_fragment.example",
  "install-client.sh"
] as const;
export type ClientBootstrapTemplateName = (typeof CLIENT_BOOTSTRAP_TEMPLATE_NAMES)[number];
export const CLIENT_BOOTSTRAP_TEMPLATES: Record<ClientBootstrapTemplateName, string> = {
  "client.env.example": clientEnvExample,
  "codex-shared-brain.include.md": codexSharedBrainInclude,
  "codex-global-AGENTS.md": codexGlobalAgents,
  "claude-user-CLAUDE.md": claudeUserClaude,
  "claude-hooks-example.json": claudeHooksExample,
  "cursor-user-rule.md": cursorUserRule,
  "ssh_config_fragment.example": sshConfigFragmentExample,
  "install-client.sh": installClientScript
};

export function readClientBootstrapTemplate(path: string): string {
  const template = CLIENT_BOOTSTRAP_TEMPLATES[path as ClientBootstrapTemplateName];
  if (template === undefined) {
    throw new Error(`Unknown client bootstrap template: ${path}`);
  }
  return template;
}
export const PORTABLE_SKILLS = {
  "brain-curator": portableBrainCuratorSkill,
  brainstack: portableBrainstackSkill,
  "remote-machine-ops": portableRemoteMachineOpsSkill,
  "shared-brain-client": portableSharedBrainClientSkill
} as const;
export type PortableSkillName = keyof typeof PORTABLE_SKILLS;
export type PortableSkillProfile = "client" | "operator" | "control" | "worker";
export const PORTABLE_SKILL_PROFILES: Record<PortableSkillProfile, PortableSkillName[]> = {
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
  "capabilities",
  "tailscale",
  "client",
  "curation"
]);

export function profileRequiresBunRuntime(profile: Profile): boolean {
  return profile !== "client-macos";
}

export function normalizeHarness(value: string | undefined, fallback: HarnessName = "codex"): HarnessName {
  const normalized = (value || fallback).trim().toLowerCase();
  if (normalized === "codex" || normalized === "claude") {
    return normalized;
  }
  throw new Error(`Unsupported harness: ${value}. Expected codex or claude.`);
}

export function normalizeWorkerSshTrustMode(value: string | null | undefined, transport: string): WorkerSshTrustMode {
  if (transport === "local" || transport === "tailscale-ssh") {
    return "pinned";
  }
  if (value === "accept-new") {
    return "accept-new";
  }
  return "pinned";
}

export function normalizeSecurityPosture(value: string | null | undefined): SecurityPosture {
  const normalized = (value || "trusted-tailnet").trim().toLowerCase();
  if (normalized === "local" || normalized === "trusted-tailnet" || normalized === "guarded") {
    return normalized;
  }
  throw new Error(`Unsupported security posture: ${value}. Expected local, trusted-tailnet, or guarded.`);
}

export function normalizeTrustedExposure(value: string | null | undefined): TrustedExposure {
  const normalized = (value || "none").trim().toLowerCase();
  if (normalized === "none" || normalized === "tailscale-serve" || normalized === "vpn" || normalized === "manual") {
    return normalized;
  }
  throw new Error(`Unsupported trusted exposure: ${value}. Expected none, tailscale-serve, vpn, or manual.`);
}

export function normalizeVoiceCapabilityTarget(value: string | null | undefined): VoiceCapabilityTarget {
  const normalized = (value || "local").trim().toLowerCase();
  if (normalized === "local" || normalized === "worker") {
    return normalized;
  }
  throw new Error(`Unsupported capabilities.voice.target: ${value}. Expected local or worker.`);
}

export function normalizeVoiceCapabilityEngine(value: string | null | undefined): VoiceCapabilityEngine {
  const normalized = (value || "whisperfile").trim().toLowerCase();
  if (normalized === "whisperfile") {
    return normalized;
  }
  throw new Error(`Unsupported capabilities.voice.engine: ${value}. Expected whisperfile.`);
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

export function stringifySimpleYaml(value: Record<string, unknown>, indent = 0): string {
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

export function brainstackDefaultConfigPath(): string {
  return abs("~/.config/brainstack/brainstack.yaml");
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
  const profile = profileHint && isSupportedProfile(profileHint) ? profileHint : "control";
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

export function objectAt(input: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = input[key];
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

export function stringAt(input: Record<string, unknown>, key: string, fallback: string): string {
  const value = input[key];
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

export function numberAt(input: Record<string, unknown>, key: string, fallback: number): number {
  const value = input[key];
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

export function booleanAt(input: Record<string, unknown>, key: string, fallback: boolean): boolean {
  const value = input[key];
  return typeof value === "boolean" ? value : fallback;
}

export function arrayAt(input: Record<string, unknown>, key: string): unknown[] {
  const value = input[key];
  return Array.isArray(value) ? value : [];
}

export function optionalStringAt(input: Record<string, unknown>, key: string): string | null {
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
  const proc = Bun.spawnSync(["bash", "-c", "command -v bun"], { env: process.env, stdout: "pipe", stderr: "pipe", timeout: 2000 });
  const bunBin = proc.stdout.toString().trim();
  if (proc.exitCode === null) {
    throw new Error("Timed out while locating Bun binary; install Bun and ensure `command -v bun` returns quickly before running brainctl.");
  }
  if (proc.exitCode !== 0 || !bunBin) {
    throw new Error("Bun binary not found; install Bun and ensure `command -v bun` works before running brainctl.");
  }
  return bunBin;
}

export async function loadConfig(configPath?: string, profileOverride?: string, rootOverride?: string): Promise<BrainstackConfig> {
  const raw = await loadRawConfig(configPath, profileOverride);
  validateRawConfig(raw);
  const schemaVersion = numberAt(raw, "schema_version", numberAt(raw, "config_version", CONFIG_SCHEMA_VERSION));
  const profileName = profileOverride || stringAt(raw, "profile", "single-node");
  if (RESERVED_PROFILE_ERRORS[profileName]) {
    throw new Error(RESERVED_PROFILE_ERRORS[profileName]);
  }
  if (!isSupportedProfile(profileName)) {
    throw new Error(`Unsupported profile ${profileName}; supported profiles are ${SUPPORTED_PROFILES.join("|")}`);
  }
  const profile = profileName;
  const runtime = objectAt(raw, "runtime");
  const harness = objectAt(raw, "harness");
  const machine = objectAt(raw, "machine");
  const paths = objectAt(raw, "paths");
  const security = objectAt(raw, "security");
  const brain = objectAt(raw, "brain");
  const telemux = objectAt(raw, "telemux");
  const capabilities = objectAt(raw, "capabilities");
  const voiceCapability = objectAt(capabilities, "voice");
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
    capabilities: {
      voice: {
        enabled: booleanAt(voiceCapability, "enabled", false),
        target: normalizeVoiceCapabilityTarget(optionalStringAt(voiceCapability, "target")),
        worker: optionalStringAt(voiceCapability, "worker"),
        engine: normalizeVoiceCapabilityEngine(optionalStringAt(voiceCapability, "engine")),
        model: stringAt(voiceCapability, "model", "tiny.en"),
        installRoot: stringAt(voiceCapability, "installRoot", "~/.local/share/brainstack/capabilities/voice"),
        command: stringAt(voiceCapability, "command", ""),
        args: arrayAt(voiceCapability, "args").map(String).filter(Boolean),
        timeoutMs: numberAt(voiceCapability, "timeoutMs", 120_000),
        echoTranscript: booleanAt(voiceCapability, "echoTranscript", true),
        maxBytes: numberAt(voiceCapability, "maxBytes", 20 * 1024 * 1024),
        maxDurationSeconds:
          voiceCapability.maxDurationSeconds === null ? null : numberAt(voiceCapability, "maxDurationSeconds", 300),
        modelUrl: stringAt(voiceCapability, "modelUrl", ""),
        sha256: optionalStringAt(voiceCapability, "sha256"),
        installedAt: optionalStringAt(voiceCapability, "installedAt")
      }
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
