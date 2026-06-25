import { accessSync, constants, existsSync, readFileSync, realpathSync, statSync } from "node:fs";
import { chmod, cp, lstat, mkdir, mkdtemp, readFile, readlink, realpath, rm, stat, symlink, writeFile } from "node:fs/promises";
import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { lookup } from "node:dns/promises";
import { basename, dirname, join, resolve, sep } from "node:path";
import { hostname, tmpdir } from "node:os";
import { createInterface } from "node:readline/promises";
import { boolFlag, flag, hasFlag, requireFlagValue, type ParsedArgs } from "../args";
import { abs, absWithHome, expandHome, renderTemplate, shellSingleQuote } from "../paths";
import {
  BRAINSTACK_PACKAGE_VERSION,
  CLIENT_BOOTSTRAP_TEMPLATE_NAMES,
  CLIENT_INVITE_KNOWN_HOSTS_MAX_ENTRIES,
  CLIENT_INVITE_KNOWN_HOSTS_MAX_LINE_BYTES,
  CLIENT_INVITE_MAX_CHARS,
  CLIENT_INVITE_PREFIX,
  CLIENT_INVITE_TYPE,
  CONFIG_SCHEMA_VERSION,
  DEFAULT_CURATION_ALLOWED_PATHS,
  GITHUB_RELEASE_DOWNLOAD_BASE,
  LATEST_INSTALL_URL,
  MIN_BUN_VERSION,
  SUPPORTED_PROFILES,
  TELEGRAM_SEND_DEFAULT_MAX_BYTES,
  TELEGRAM_SEND_SENSITIVE_FILE_PATTERN,
  arrayAt,
  brainstackDefaultConfigPath,
  isSupportedProfile,
  normalizeHarness,
  normalizeSecurityPosture,
  normalizeWorkerSshTrustMode,
  optionalStringAt,
  profileRequiresBunRuntime,
  readClientBootstrapTemplate,
  stringAt,
  stringifySimpleYaml,
  type BrainstackClientInvite,
  type BrainstackConfig,
  type BrainstackWorkerConfig,
  type ControlSshTrustMode,
  type DestroyScope,
  type HarnessName,
  type ManagedArtifact,
  type ManagedArtifactsManifest,
  type PortableSkillProfile,
  type Profile,
  type SeedMode
} from "../config";
import {
  ensureDir,
  readEnvSecretOrFile,
  readExistingPrivateText,
  run,
  runWithStdinFile,
  safeGitProtocolArgs,
  safeGitProtocolEnv,
  setEnvIfBlank,
  writeIfMissing,
  writePrivateText,
  writeText
} from "../runtime";

type InstallEnrollDeps = {
  loadConfig: (path?: string | null, profile?: string | null, root?: string | null) => Promise<BrainstackConfig>;
  objectAt: (input: Record<string, unknown>, key: string) => Record<string, unknown>;
  isLoopbackHost: (host: string) => boolean;
  currentBrainctlHookCommand: (args: ParsedArgs) => string;
  commandBackup: (args: ParsedArgs) => Promise<void>;
  commandDoctor: (args: ParsedArgs) => Promise<void>;
  commandSkills: (args: ParsedArgs) => Promise<void>;
  normalizePortableSkillProfile: (value: string | undefined) => PortableSkillProfile;
  parseIntegerFlag: (args: ParsedArgs, key: string) => number | null;
  parsePositiveIntegerFlag: (args: ParsedArgs, key: string, fallback: number) => number;
  updateProbeTimeoutMs: () => number;
  workerSshKnownHostsLookup: (worker: BrainstackWorkerConfig) => string;
  workerSshPortArgs: (worker: BrainstackWorkerConfig) => string[];
  workerRemoteTarget: (worker: BrainstackWorkerConfig) => string;
};

const BRAINSTACK_DAEMON_LABEL = "com.brainstack.daemon";
const BRAINSTACK_DAEMON_SERVICE = "brainstackd.service";

function daemonStateDir(cfg: BrainstackConfig): string {
  return join(cfg.paths.stateRoot, "daemon");
}

function daemonServicePath(cfg: BrainstackConfig, platform: "launchd" | "systemd"): string {
  if (platform === "launchd") {
    return join(cfg.paths.home, "Library", "LaunchAgents", `${BRAINSTACK_DAEMON_LABEL}.plist`);
  }
  return join(cfg.paths.systemdUserRoot, BRAINSTACK_DAEMON_SERVICE);
}

export function createInstallEnrollCommands(deps: InstallEnrollDeps) {
  const {
    loadConfig,
    objectAt,
    isLoopbackHost,
    currentBrainctlHookCommand,
    commandBackup,
    commandDoctor,
    commandSkills,
    normalizePortableSkillProfile,
    parseIntegerFlag,
    parsePositiveIntegerFlag,
    updateProbeTimeoutMs,
    workerSshKnownHostsLookup,
    workerSshPortArgs,
    workerRemoteTarget
  } = deps;

  function quoteForBash(value: string): string {
    return shellSingleQuote(value);
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
    const voice = cfg.capabilities.voice;
    const voiceArgs = voice.args.length ? voice.args : ["-f", "{input}", "-pc"];
    return [
      `PATH=${toolPath}`,
      `BRAINSTACK_WORKER_PATH=${toolPath}`,
      `BRAINSTACK_CONFIG=${join(cfg.paths.configRoot, "brainstack.yaml")}`,
      "FACTORY_BRAINCTL_BIN=brainctl",
      `FACTORY_DASHBOARD_HOST=${cfg.telemux.dashboardHost}`,
      `FACTORY_DASHBOARD_PORT=${cfg.telemux.dashboardPort}`,
      "FACTORY_TELEGRAM_POLL_TIMEOUT_SECONDS=30",
      "FACTORY_TEXT_COALESCE_MS=1500",
      "FACTORY_TEXT_COALESCE_RECOVERY_MAX_AGE_MS=300000",
      "FACTORY_CAPABILITY_PROGRESS_INTERVAL_MS=45000",
      "FACTORY_HARNESS_STREAMING=status",
      "FACTORY_HARNESS_STREAMING_INITIAL_DELAY_MS=8000",
      "FACTORY_HARNESS_STREAMING_UPDATE_INTERVAL_MS=12000",
      "FACTORY_HARNESS_STREAMING_MAX_CHARS=1800",
      "FACTORY_PRE_DISPATCH_CLASSIFIER=0",
      "FACTORY_PRE_DISPATCH_CLASSIFIER_MODEL=gpt-5.4-mini",
      "FACTORY_PRE_DISPATCH_CLASSIFIER_REASONING_EFFORT=minimal",
      "FACTORY_PRE_DISPATCH_CLASSIFIER_TIMEOUT_MS=800",
      "FACTORY_PRE_DISPATCH_CLASSIFIER_MAX_CHARS=600",
      "FACTORY_PRE_DISPATCH_CLASSIFIER_CONFIDENCE=0.75",
      `FACTORY_TRANSCRIPTION_ENABLED=${voice.enabled ? "1" : "0"}`,
      `FACTORY_TRANSCRIPTION_TARGET=${voice.target}`,
      `FACTORY_TRANSCRIPTION_WORKER=${voice.worker || ""}`,
      `FACTORY_TRANSCRIPTION_COMMAND=${voice.command}`,
      `FACTORY_TRANSCRIPTION_ARGS_JSON=${JSON.stringify(voiceArgs)}`,
      `FACTORY_TRANSCRIPTION_TIMEOUT_MS=${voice.timeoutMs}`,
      `FACTORY_TRANSCRIPTION_ECHO=${voice.echoTranscript ? "1" : "0"}`,
      `FACTORY_TRANSCRIPTION_MAX_BYTES=${voice.maxBytes}`,
      `FACTORY_TRANSCRIPTION_MAX_DURATION_SECONDS=${voice.maxDurationSeconds ?? ""}`,
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

  function usesLocalHarnessGuidance(cfg: BrainstackConfig): boolean {
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

  async function renderLocalClientBootstrapTemplates(cfg: BrainstackConfig): Promise<{ bootstrapRoot: string; bootstrapFiles: Record<string, string> }> {
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
    return { bootstrapRoot, bootstrapFiles };
  }

  async function installLocalHarnessGuidance(cfg: BrainstackConfig, bootstrapRoot: string, bootstrapFiles: Record<string, string>): Promise<string[]> {
    const touched: string[] = [];
    const codexHome = join(cfg.paths.home, ".codex");
    await ensureDir(codexHome);
    const codexAgents = join(codexHome, "AGENTS.md");
    const codexTarget = join(bootstrapRoot, "codex-shared-brain.include.md");
    const codexInfo = await lstat(codexAgents).catch(() => null);
    if (!codexInfo) {
      await symlink(codexTarget, codexAgents);
      touched.push(codexAgents);
    } else {
      console.log(`Codex already has ${codexAgents}; append the real shared-brain guidance with:`);
      console.log(`cat ${codexTarget} >> ${codexAgents}`);
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

  async function repairLocalClientGuidance(cfg: BrainstackConfig): Promise<string[]> {
    const touched: string[] = [];
    const { bootstrapRoot, bootstrapFiles } = await renderLocalClientBootstrapTemplates(cfg);
    touched.push(bootstrapRoot);
    const envPath = clientEnvPathAbs(cfg);
    if (await writeIfMissing(envPath, bootstrapFiles["client.env.example"], 0o600)) {
      touched.push(envPath);
    }
    touched.push(...(await installLocalHarnessGuidance(cfg, bootstrapRoot, bootstrapFiles)));
    return touched;
  }

  async function installLocalClientBootstrap(cfg: BrainstackConfig, options: { importTokenFile?: string; importToken?: string } = {}): Promise<string[]> {
    if (options.importTokenFile && options.importToken !== undefined) {
      throw new Error("client bootstrap accepts either an import token file or an in-memory import token, not both");
    }
    const touched: string[] = [];
    const { bootstrapRoot, bootstrapFiles } = await renderLocalClientBootstrapTemplates(cfg);
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

    touched.push(...(await installLocalHarnessGuidance(cfg, bootstrapRoot, bootstrapFiles)));
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

  return {
    token,
    encodeClientInvite,
    httpUrlHostname,
    validateInviteSshTarget,
    validateInviteGitRemote,
    validateInviteRemoteRepoPath,
    validateInviteClientPath,
    inviteBareHost,
    parseKnownHostEntry,
    knownHostMatchesLookup,
    knownHostLineMatchesLookup,
    controlKnownHostsLookup,
    filterKnownHostsForSshTarget,
    sanitizeInviteKnownHosts,
    decodeClientInvite,
    compareVersions,
    installedBunVersion,
    runtimeCommandAvailable,
    refExists,
    isBareRepoInitialized,
    syncCloneToMain,
    sharedBrainSeedFiles,
    braindRuntimeEnv,
    braindSecretsEnv,
    telemuxRuntimeEnv,
    brainstackToolPath,
    telemuxSecretsEnv,
    preserveSshKnownHosts,
    isKnownHostLine,
    defaultWorkers,
    runsBraind,
    usesUserServices,
    usesBrainstackDaemon,
    usesLocalHarnessGuidance,
    braindService,
    brainstackDaemonServiceCommand,
    brainstackDaemonSystemdService,
    xmlEscape,
    brainstackDaemonLaunchAgent,
    telemuxService,
    postReceiveHook,
    preReceiveHook,
    tailscaleServeScript,
    tailscaleServeConfig,
    tailscaleUpScript,
    tailscalePolicyFragment,
    commandExpose,
    clientBootstrapFiles,
    renderFiles,
    writeFileMap,
    clientLocalPathAbs,
    clientEnvPathAbs,
    managedManifestPath,
    destroyScopeFromProfile,
    artifactInScope,
    expectedManagedArtifacts,
    manualLeftovers,
    writeManagedManifest,
    loadManagedManifest,
    sha256Hex,
    canonicalJson,
    renderLocalClientBootstrapTemplates,
    installLocalHarnessGuidance,
    repairLocalClientGuidance,
    installLocalClientBootstrap,
    commandPath,
    executableFile,
    commonCodexAppCliPath,
    resolveEnrollHarnessBin,
    userShellPathTimeoutMs,
    detectUserShellPath,
    userShellPathEnv,
    whereisPath,
    installHint,
    requiredProvisionCommands,
    profileRequiresPasswordlessSudo,
    provisionRequiresPasswordlessSudo,
    ensureProvisionPrereqs,
    promptHarnessChoice,
    selectProvisionHarness,
    runWithInputTimeout,
    ensurePasswordlessSudo,
    testHarnessSudo,
    testTelegramBotConfig,
    discoveredMachineName,
    buildProvisionConfig,
    tailscaleUpShellCommand,
    commandProvision,
    commandRender,
    writeSharedBrainSeed,
    gitExists,
    ensureGitRepoLayout,
    commandInit,
    applyRuntime,
    commandApplyRuntime,
    inviteControlSshTarget,
    readInviteKnownHosts,
    inviteImportToken,
    normalizeInstallReleaseTag,
    releaseInstallUrlForTag,
    defaultInviteInstallUrl,
    validateInviteInstallUrl,
    inviteInstallUrl,
    inviteInstallCommand,
    normalizeEnrollSkillsProfile,
    defaultInviteSkillsProfile,
    commandInviteCreate,
    commandInvite,
    buildEnrollConfig,
    writeEnrollConfig,
    installInviteKnownHosts,
    readStdinText,
    readInviteFile,
    commandEnroll,
    sanitizeTelegramSendFileName,
    hasMixedScriptConfusables,
    isSensitiveTelegramFileName,
    validateTelegramSendLocalFile,
    sshTargetFromRemoteSsh,
    telegramControlSshTarget,
    normalizeControlSshTrustMode,
    telegramControlWorker,
    telegramKnownHostsPath,
    telegramSshTrustMode,
    telegramSshTrustArgs,
    normalizeTelegramSendKind,
    telegramRemoteSendScript,
    formatTelegramSendSummary,
    commandTelegramSendFile,
    commandTelegram
  };
}
