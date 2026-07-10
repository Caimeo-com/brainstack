#!/usr/bin/env bun
import { existsSync, readFileSync, statSync } from "node:fs";
import { appendFile, chmod, cp, lstat, mkdir, open, readFile, readdir, readlink, rename, rm, rmdir, stat, writeFile } from "node:fs/promises";
import { dirname, join, resolve, sep } from "node:path";
import { hostname, tmpdir } from "node:os";
import { purgeCorruptOutboxEntries } from "../../outbox/src/outbox";
import { commandHasHelp, flag, flagValues, hasFlag, parseArgs, requireFlagValue, usage, type ParsedArgs } from "./args";
import { abs, absWithHome, shellSingleQuote } from "./paths";
import {
  CONFIG_SCHEMA_VERSION,
  PORTABLE_SKILLS,
  PORTABLE_SKILL_PROFILES,
  PRODUCT_ROOT,
  brainstackDefaultConfigPath,
  loadConfig,
  normalizeHarness,
  parseSimpleYaml,
  stringifySimpleYaml,
  type BrainstackConfig,
  type DestroyScope,
  type HookTarget,
  type PortableSkillName,
  type PortableSkillProfile,
  type Profile
} from "./config";
export { loadConfig, parseSimpleYaml } from "./config";
import { createDoctorStatusCommands } from "./commands/doctor-status";
import { createInstallEnrollCommands } from "./commands/install-enroll";
import { createCapabilitiesCommands } from "./commands/capabilities";
import { createContextPacksCommands } from "./commands/context-packs";
import { createProjectOutboxCommands } from "./commands/project-outbox";
import { createSkillHookCommands } from "./commands/skills-hooks";
import { createUploadsCommands } from "./commands/uploads";
import { formatOutboxErrorSummary, summarizeOutboxTerminalErrors } from "./outbox-summary";
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
} from "./runtime";

const installEnrollCommands = createInstallEnrollCommands({
  loadConfig,
  objectAt: (input, key) => objectAt(input, key),
  isLoopbackHost: (host) => isLoopbackHost(host),
  currentBrainctlHookCommand: (args) => currentBrainctlHookCommand(args),
  commandBackup: (args) => commandBackup(args),
  commandDoctor: (args) => commandDoctor(args),
  commandSkills: (args) => commandSkills(args),
  normalizePortableSkillProfile: (value) => normalizePortableSkillProfile(value),
  parseIntegerFlag,
  parsePositiveIntegerFlag,
  updateProbeTimeoutMs,
  workerSshKnownHostsLookup: (worker) => workerSshKnownHostsLookup(worker),
  workerSshPortArgs: (worker) => workerSshPortArgs(worker),
  workerRemoteTarget: (worker) => workerRemoteTarget(worker)
});
const token = installEnrollCommands.token;
const encodeClientInvite = installEnrollCommands.encodeClientInvite;
const httpUrlHostname = installEnrollCommands.httpUrlHostname;
const validateInviteSshTarget = installEnrollCommands.validateInviteSshTarget;
const validateInviteGitRemote = installEnrollCommands.validateInviteGitRemote;
const validateInviteRemoteRepoPath = installEnrollCommands.validateInviteRemoteRepoPath;
const validateInviteClientPath = installEnrollCommands.validateInviteClientPath;
const inviteBareHost = installEnrollCommands.inviteBareHost;
const parseKnownHostEntry = installEnrollCommands.parseKnownHostEntry;
const knownHostMatchesLookup = installEnrollCommands.knownHostMatchesLookup;
const knownHostLineMatchesLookup = installEnrollCommands.knownHostLineMatchesLookup;
const controlKnownHostsLookup = installEnrollCommands.controlKnownHostsLookup;
const filterKnownHostsForSshTarget = installEnrollCommands.filterKnownHostsForSshTarget;
const sanitizeInviteKnownHosts = installEnrollCommands.sanitizeInviteKnownHosts;
const decodeClientInvite = installEnrollCommands.decodeClientInvite;
const compareVersions = installEnrollCommands.compareVersions;
const installedBunVersion = installEnrollCommands.installedBunVersion;
const runtimeCommandAvailable = installEnrollCommands.runtimeCommandAvailable;
const refExists = installEnrollCommands.refExists;
const isBareRepoInitialized = installEnrollCommands.isBareRepoInitialized;
const syncCloneToMain = installEnrollCommands.syncCloneToMain;
const sharedBrainSeedFiles = installEnrollCommands.sharedBrainSeedFiles;
const braindRuntimeEnv = installEnrollCommands.braindRuntimeEnv;
const braindSecretsEnv = installEnrollCommands.braindSecretsEnv;
const telemuxRuntimeEnv = installEnrollCommands.telemuxRuntimeEnv;
const brainstackToolPath = installEnrollCommands.brainstackToolPath;
const telemuxSecretsEnv = installEnrollCommands.telemuxSecretsEnv;
const preserveSshKnownHosts = installEnrollCommands.preserveSshKnownHosts;
const isKnownHostLine = installEnrollCommands.isKnownHostLine;
const defaultWorkers = installEnrollCommands.defaultWorkers;
const runsBraind = installEnrollCommands.runsBraind;
const usesUserServices = installEnrollCommands.usesUserServices;
const usesBrainstackDaemon = installEnrollCommands.usesBrainstackDaemon;
const usesLocalHarnessGuidance = installEnrollCommands.usesLocalHarnessGuidance;
const braindService = installEnrollCommands.braindService;
const brainstackDaemonServiceCommand = installEnrollCommands.brainstackDaemonServiceCommand;
const brainstackDaemonSystemdService = installEnrollCommands.brainstackDaemonSystemdService;
const xmlEscape = installEnrollCommands.xmlEscape;
const brainstackDaemonLaunchAgent = installEnrollCommands.brainstackDaemonLaunchAgent;
const telemuxService = installEnrollCommands.telemuxService;
const postReceiveHook = installEnrollCommands.postReceiveHook;
const preReceiveHook = installEnrollCommands.preReceiveHook;
const tailscaleServeScript = installEnrollCommands.tailscaleServeScript;
const tailscaleServeConfig = installEnrollCommands.tailscaleServeConfig;
const tailscaleUpScript = installEnrollCommands.tailscaleUpScript;
const tailscalePolicyFragment = installEnrollCommands.tailscalePolicyFragment;
const commandExpose = installEnrollCommands.commandExpose;
const clientBootstrapFiles = installEnrollCommands.clientBootstrapFiles;
const renderFiles = installEnrollCommands.renderFiles;
const writeFileMap = installEnrollCommands.writeFileMap;
const clientLocalPathAbs = installEnrollCommands.clientLocalPathAbs;
const clientEnvPathAbs = installEnrollCommands.clientEnvPathAbs;
const managedManifestPath = installEnrollCommands.managedManifestPath;
const destroyScopeFromProfile = installEnrollCommands.destroyScopeFromProfile;
const artifactInScope = installEnrollCommands.artifactInScope;
const expectedManagedArtifacts = installEnrollCommands.expectedManagedArtifacts;
const manualLeftovers = installEnrollCommands.manualLeftovers;
const writeManagedManifest = installEnrollCommands.writeManagedManifest;
const loadManagedManifest = installEnrollCommands.loadManagedManifest;
const sha256Hex = installEnrollCommands.sha256Hex;
const canonicalJson = installEnrollCommands.canonicalJson;
const renderLocalClientBootstrapTemplates = installEnrollCommands.renderLocalClientBootstrapTemplates;
const installLocalHarnessGuidance = installEnrollCommands.installLocalHarnessGuidance;
const repairLocalClientGuidance = installEnrollCommands.repairLocalClientGuidance;
const installLocalClientBootstrap = installEnrollCommands.installLocalClientBootstrap;
const commandPath = installEnrollCommands.commandPath;
const executableFile = installEnrollCommands.executableFile;
const commonCodexAppCliPath = installEnrollCommands.commonCodexAppCliPath;
const resolveEnrollHarnessBin = installEnrollCommands.resolveEnrollHarnessBin;
const userShellPathTimeoutMs = installEnrollCommands.userShellPathTimeoutMs;
const detectUserShellPath = installEnrollCommands.detectUserShellPath;
const userShellPathEnv = installEnrollCommands.userShellPathEnv;
const whereisPath = installEnrollCommands.whereisPath;
const installHint = installEnrollCommands.installHint;
const requiredProvisionCommands = installEnrollCommands.requiredProvisionCommands;
const profileRequiresPasswordlessSudo = installEnrollCommands.profileRequiresPasswordlessSudo;
const provisionRequiresPasswordlessSudo = installEnrollCommands.provisionRequiresPasswordlessSudo;
const ensureProvisionPrereqs = installEnrollCommands.ensureProvisionPrereqs;
const promptHarnessChoice = installEnrollCommands.promptHarnessChoice;
const selectProvisionHarness = installEnrollCommands.selectProvisionHarness;
const runWithInputTimeout = installEnrollCommands.runWithInputTimeout;
const ensurePasswordlessSudo = installEnrollCommands.ensurePasswordlessSudo;
const testHarnessSudo = installEnrollCommands.testHarnessSudo;
const testTelegramBotConfig = installEnrollCommands.testTelegramBotConfig;
const discoveredMachineName = installEnrollCommands.discoveredMachineName;
const buildProvisionConfig = installEnrollCommands.buildProvisionConfig;
const tailscaleUpShellCommand = installEnrollCommands.tailscaleUpShellCommand;
const commandProvision = installEnrollCommands.commandProvision;
const commandRender = installEnrollCommands.commandRender;
const writeSharedBrainSeed = installEnrollCommands.writeSharedBrainSeed;
const gitExists = installEnrollCommands.gitExists;
const ensureGitRepoLayout = installEnrollCommands.ensureGitRepoLayout;
const commandInit = installEnrollCommands.commandInit;
const applyRuntime = installEnrollCommands.applyRuntime;
const commandApplyRuntime = installEnrollCommands.commandApplyRuntime;
const inviteControlSshTarget = installEnrollCommands.inviteControlSshTarget;
const readInviteKnownHosts = installEnrollCommands.readInviteKnownHosts;
const inviteImportToken = installEnrollCommands.inviteImportToken;
const normalizeInstallReleaseTag = installEnrollCommands.normalizeInstallReleaseTag;
const releaseInstallUrlForTag = installEnrollCommands.releaseInstallUrlForTag;
const defaultInviteInstallUrl = installEnrollCommands.defaultInviteInstallUrl;
const validateInviteInstallUrl = installEnrollCommands.validateInviteInstallUrl;
const inviteInstallUrl = installEnrollCommands.inviteInstallUrl;
const inviteInstallCommand = installEnrollCommands.inviteInstallCommand;
const normalizeEnrollSkillsProfile = installEnrollCommands.normalizeEnrollSkillsProfile;
const defaultInviteSkillsProfile = installEnrollCommands.defaultInviteSkillsProfile;
const commandInviteCreate = installEnrollCommands.commandInviteCreate;
const commandInvite = installEnrollCommands.commandInvite;
const buildEnrollConfig = installEnrollCommands.buildEnrollConfig;
const writeEnrollConfig = installEnrollCommands.writeEnrollConfig;
const installInviteKnownHosts = installEnrollCommands.installInviteKnownHosts;
const readStdinText = installEnrollCommands.readStdinText;
const readInviteFile = installEnrollCommands.readInviteFile;
const commandEnroll = installEnrollCommands.commandEnroll;
const sanitizeTelegramSendFileName = installEnrollCommands.sanitizeTelegramSendFileName;
const hasMixedScriptConfusables = installEnrollCommands.hasMixedScriptConfusables;
const isSensitiveTelegramFileName = installEnrollCommands.isSensitiveTelegramFileName;
const validateTelegramSendLocalFile = installEnrollCommands.validateTelegramSendLocalFile;
const sshTargetFromRemoteSsh = installEnrollCommands.sshTargetFromRemoteSsh;
const telegramControlSshTarget = installEnrollCommands.telegramControlSshTarget;
const normalizeControlSshTrustMode = installEnrollCommands.normalizeControlSshTrustMode;
const telegramControlWorker = installEnrollCommands.telegramControlWorker;
const telegramKnownHostsPath = installEnrollCommands.telegramKnownHostsPath;
const telegramSshTrustMode = installEnrollCommands.telegramSshTrustMode;
const telegramSshTrustArgs = installEnrollCommands.telegramSshTrustArgs;
const normalizeTelegramSendKind = installEnrollCommands.normalizeTelegramSendKind;
const telegramRemoteSendScript = installEnrollCommands.telegramRemoteSendScript;
const formatTelegramSendSummary = installEnrollCommands.formatTelegramSendSummary;
const commandTelegramSendFile = installEnrollCommands.commandTelegramSendFile;
const commandTelegram = installEnrollCommands.commandTelegram;

const doctorStatusCommands = createDoctorStatusCommands({
  compareVersions,
  canonicalJson,
  sha256Hex,
  installedBunVersion,
  runtimeCommandAvailable,
  commandPath,
  userShellPathEnv,
  updateProbeTimeoutMs,
  updateProbeAllowed,
  installHint,
  testHarnessSudo,
  gitExists,
  managedManifestPath,
  runsBraind,
  usesUserServices,
  usesBrainstackDaemon,
  usesLocalHarnessGuidance,
  telemuxService,
  clientLocalPathAbs,
  clientEnvPathAbs,
  readEnvFile: (path) => readEnvFile(path),
  resolveClientEnvValue: (cfg, name) => resolveClientEnvValue(cfg, name),
  brainWriteConfig: (cfg) => brainWriteConfig(cfg),
  brainApiBaseUrl,
  brainWriteSmokeCheck: (cfg) => brainWriteSmokeCheck(cfg),
  scanAllOutboxes: (cfg) => scanAllOutboxes(cfg),
  listOutboxEntries: (cfg) => listOutboxEntries(cfg),
  hookConfigPath: (target) => hookConfigPath(target),
  readJsonObject: (path) => readJsonObject(path),
  countCodexStyleManagedHooks: (raw, target) => countCodexStyleManagedHooks(raw, target),
  countCursorManagedHooks: (raw, target) => countCursorManagedHooks(raw, target),
  skillInstallRootForTarget: (target, args) => skillInstallRootForTarget(target, args),
  skillDirsForDoctor: (root) => skillDirsForDoctor(root),
  proposalReviewGroupsFromResult: (result) => proposalReviewGroupsFromResult(result),
  daemonStatusPath,
  readDaemonStatus: (cfg) => readDaemonStatus(cfg),
  daemonPlatform,
  daemonServicePath,
  processAlive,
  processCommandLine,
  daemonStatusFresh: (status, maxAgeMs) => daemonStatusFresh(status, maxAgeMs),
  daemonServiceStatus: (cfg, args, timeoutMs) => daemonServiceStatus(cfg, args, timeoutMs),
  sshTargetFromRemoteSsh,
  validateInviteSshTarget,
  telegramControlWorker,
  telegramKnownHostsPath,
  telegramSshTrustMode,
  telegramSshTrustArgs,
  normalizeOutboxItem: (item) => normalizeOutboxItem(item),
  repoLockInfo,
  parsePositiveIntegerFlag,
  defaultWorkers,
  summarizeLines
});
const check = doctorStatusCommands.check;
const commandOk = doctorStatusCommands.commandOk;
const harnessCompatibility = doctorStatusCommands.harnessCompatibility;
const outboxParentRoot = doctorStatusCommands.outboxParentRoot;
const outboxRoot = doctorStatusCommands.outboxRoot;
const outboxRootForDestination = doctorStatusCommands.outboxRootForDestination;
const brainInstanceId = doctorStatusCommands.brainInstanceId;
const formatDoctorChecks = doctorStatusCommands.formatDoctorChecks;
const workerEnvCachePath = doctorStatusCommands.workerEnvCachePath;
const readWorkerEnvCache = doctorStatusCommands.readWorkerEnvCache;
const writeWorkerEnvCache = doctorStatusCommands.writeWorkerEnvCache;
const cachedWorkerPath = doctorStatusCommands.cachedWorkerPath;
const workerHarnessFamily = doctorStatusCommands.workerHarnessFamily;
const workerHarnessBin = doctorStatusCommands.workerHarnessBin;
const workerRemoteTarget = doctorStatusCommands.workerRemoteTarget;
const workerSshHost = doctorStatusCommands.workerSshHost;
const workerSshPort = doctorStatusCommands.workerSshPort;
const workerSshKnownHostsLookup = doctorStatusCommands.workerSshKnownHostsLookup;
const workerSshKnownHostsPath = doctorStatusCommands.workerSshKnownHostsPath;
const workerSshTrustMode = doctorStatusCommands.workerSshTrustMode;
const workerSshTrustArgs = doctorStatusCommands.workerSshTrustArgs;
const workerSshPortArgs = doctorStatusCommands.workerSshPortArgs;
const runWorkerShell = doctorStatusCommands.runWorkerShell;
const quoteForBash = doctorStatusCommands.quoteForBash;
const braindPortOwnerCheck = doctorStatusCommands.braindPortOwnerCheck;
const commandDoctor = doctorStatusCommands.commandDoctor;
const sanitizeStatusError = doctorStatusCommands.sanitizeStatusError;
const statusSection = doctorStatusCommands.statusSection;
const gitStatusProbe = doctorStatusCommands.gitStatusProbe;
const remoteBrainctlScript = doctorStatusCommands.remoteBrainctlScript;
const controlSshTarget = doctorStatusCommands.controlSshTarget;
const runControlRemoteScript = doctorStatusCommands.runControlRemoteScript;
const collectControlSourceStatusSection = doctorStatusCommands.collectControlSourceStatusSection;
const collectFleetStatusSection = doctorStatusCommands.collectFleetStatusSection;
const commandFleet = doctorStatusCommands.commandFleet;
const commandUpdates = doctorStatusCommands.commandUpdates;
const commandStatus = doctorStatusCommands.commandStatus;
const commandWorkerCache = doctorStatusCommands.commandWorkerCache;

const capabilitiesCommands = createCapabilitiesCommands({
  loadConfig,
  defaultWorkers,
  runWorkerShell,
  workerRemoteTarget,
  workerSshPortArgs,
  workerSshTrustArgs,
  runRemoteBrainctl: async (cfg, args, argv, timeoutMs) => {
    const via = controlSshTarget(cfg, args, "capabilities control SSH target", { allowRemoteSshFallback: true });
    if (!via) {
      return false;
    }
    const remoteRepo =
      requireFlagValue(args, "remote-repo") ||
      process.env.BRAINSTACK_TELEGRAM_REMOTE_REPO?.trim() ||
      cfg.client.telegramRemoteRepo ||
      "~/brainstack";
    const result = runControlRemoteScript(
      cfg,
      args,
      "capabilities control SSH target",
      remoteBrainctlScript(remoteRepo, argv, { preferInstalledBinary: true }),
      timeoutMs,
      { allowRemoteSshFallback: true }
    );
    if (!result) {
      return false;
    }
    if (result.code !== 0) {
      throw new Error(`capabilities command failed over ssh with exit ${result.code}\n${result.stderr || result.stdout}`);
    }
    if (result.stdout) process.stdout.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
    return true;
  }
});
const commandCapabilities = capabilitiesCommands.commandCapabilities;

const uploadsCommands = createUploadsCommands({
  abs,
  absWithHome,
  brainstackDefaultConfigPath,
  controlSshTarget,
  defaultWorkers,
  flag,
  hasFlag,
  loadConfig,
  parsePositiveIntegerFlag,
  quoteForBash,
  remoteBrainctlScript,
  requireFlagValue,
  run,
  runControlRemoteScript,
  runWithStdinFile,
  runWorkerShell,
  telegramControlWorker,
  telegramKnownHostsPath,
  telegramSshTrustArgs,
  telegramSshTrustMode,
  workerRemoteTarget,
  workerSshPortArgs,
  workerSshTrustArgs,
  whichCommand: commandPath
});
const commandUploads = uploadsCommands.commandUploads;

const contextPacksCommands = createContextPacksCommands({
  abs,
  absWithHome,
  brainstackDefaultConfigPath,
  controlSshTarget,
  defaultWorkers,
  flag,
  flagValues,
  hasFlag,
  loadConfig,
  parsePositiveIntegerFlag,
  quoteForBash: shellSingleQuote,
  remoteBrainctlScript,
  requireFlagValue,
  run,
  runControlRemoteScript,
  runWorkerShell,
  telegramControlWorker,
  telegramKnownHostsPath,
  telegramSshTrustArgs,
  telegramSshTrustMode,
  workerRemoteTarget,
  workerSshPortArgs,
  workerSshTrustArgs,
  whichCommand: commandPath
});
const commandContextPacks = contextPacksCommands.commandContextPacks;

const projectOutboxCommands = createProjectOutboxCommands({
  clientEnvPathAbs,
  clientLocalPathAbs,
  loadConfig,
  gitExists,
  commandPath,
  outboxParentRoot,
  outboxRoot,
  outboxRootForDestination,
  check,
  sha256Hex,
  parsePositiveIntegerFlag,
  brainApiRequest,
  fetchProposalDetail,
  proposalNeedsContext
});
const readEnvFile = projectOutboxCommands.readEnvFile;
const clientEnv = projectOutboxCommands.clientEnv;
const resolveClientEnvValue = projectOutboxCommands.resolveClientEnvValue;
const brainWriteConfig = projectOutboxCommands.brainWriteConfig;
const isLoopbackHost = projectOutboxCommands.isLoopbackHost;
const objectAt = projectOutboxCommands.objectAt;
const resolveProjectContext = projectOutboxCommands.resolveProjectContext;
const commandContext = projectOutboxCommands.commandContext;
const deriveProjectLabel = projectOutboxCommands.deriveProjectLabel;
const validateMemoryScope = projectOutboxCommands.validateMemoryScope;
const stringFromRecord = projectOutboxCommands.stringFromRecord;
const stringArrayFromRecord = projectOutboxCommands.stringArrayFromRecord;
const uniqueNonEmptyStrings = projectOutboxCommands.uniqueNonEmptyStrings;
const boundedCliString = projectOutboxCommands.boundedCliString;
const proposalEnrichmentPayload = projectOutboxCommands.proposalEnrichmentPayload;
const proposalReviewGroupsFromResult = projectOutboxCommands.proposalReviewGroupsFromResult;
const createReviewGroupMergeProposal = projectOutboxCommands.createReviewGroupMergeProposal;
const createAutomaticReviewGroupMerges = projectOutboxCommands.createAutomaticReviewGroupMerges;
const createHarnessProposalMerges = projectOutboxCommands.createHarnessProposalMerges;
const commandSearch = projectOutboxCommands.commandSearch;
const commandRemember = projectOutboxCommands.commandRemember;
const commandAllow = projectOutboxCommands.commandAllow;
const brainWriteTimeoutMs = projectOutboxCommands.brainWriteTimeoutMs;
const brainWriteSmokeCheck = projectOutboxCommands.brainWriteSmokeCheck;
const outboxItemKey = projectOutboxCommands.outboxItemKey;
const normalizeOutboxItem = projectOutboxCommands.normalizeOutboxItem;
const postBrainWriteOrQueue = projectOutboxCommands.postBrainWriteOrQueue;
const queueBrainWriteForBackgroundFlush = projectOutboxCommands.queueBrainWriteForBackgroundFlush;
const listOutboxEntries = projectOutboxCommands.listOutboxEntries;
const outboxRoots = projectOutboxCommands.outboxRoots;
const scanAllOutboxes = projectOutboxCommands.scanAllOutboxes;
const purgeConfiguredOutboxParentCorruptEntries = projectOutboxCommands.purgeConfiguredOutboxParentCorruptEntries;
const flushOutbox = projectOutboxCommands.flushOutbox;
const retryOutboxItems = projectOutboxCommands.retryOutboxItems;

type BrainWriteOutcome = Awaited<ReturnType<typeof postBrainWriteOrQueue>>;

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

const skillHookCommands = createSkillHookCommands({
  canonicalJson,
  sha256Hex,
  parsePositiveIntegerFlag,
  postBrainWriteOrQueue,
  queueBrainWriteForBackgroundFlush,
  readDaemonStatus,
  daemonStatusFresh,
  localSkillRefreshUnsafeReason,
  deriveProjectLabel,
  uniqueNonEmptyStrings
});
const normalizeHookTarget = skillHookCommands.normalizeHookTarget;
const skillInstallRootForTarget = skillHookCommands.skillInstallRootForTarget;
const commandImportSkills = skillHookCommands.commandImportSkills;
const commandImport = skillHookCommands.commandImport;
const commandSkillsImport = skillHookCommands.commandSkillsImport;
const refreshBrainstackSkillPackages = skillHookCommands.refreshBrainstackSkillPackages;
const skillDirsForDoctor = skillHookCommands.skillDirsForDoctor;
const commandSkillsDoctor = skillHookCommands.commandSkillsDoctor;
const hookConfigPath = skillHookCommands.hookConfigPath;
const readJsonObject = skillHookCommands.readJsonObject;
const countCodexStyleManagedHooks = skillHookCommands.countCodexStyleManagedHooks;
const countCursorManagedHooks = skillHookCommands.countCursorManagedHooks;
const commandHooks = skillHookCommands.commandHooks;
const currentBrainctlHookCommand = skillHookCommands.currentBrainctlHookCommand;
const commandHook = skillHookCommands.commandHook;

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
  const lastPullAt = "last_pull_at" in sync && typeof sync.last_pull_at === "string"
    ? sync.last_pull_at
    : status.repo.last_pull_at;
  status.repo = {
    path: sync.path,
    exists: sync.exists,
    clean: sync.clean,
    branch: sync.branch,
    head: sync.head,
    last_pull_at: lastPullAt
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
  if (!status) {
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
  const ok = Boolean(status && fresh && active && service.installed && service.running !== false);
  const body = { ok, fresh, last_run_ok: status?.ok ?? null, pid_alive: pidAlive, service, status };
  if (hasFlag(args, "json")) {
    console.log(JSON.stringify(body, null, 2));
    return;
  }
  console.log(`daemon service: platform=${service.platform} installed=${service.installed} running=${service.running ?? "unknown"} path=${service.path}`);
  if (!status) {
    console.log(`daemon status: missing (${daemonStatusPath(cfg)})`);
    return;
  }
  console.log(`daemon status: heartbeat_fresh=${fresh} last_run_ok=${status.ok} pid=${status.pid} pid_alive=${pidAlive ?? "unknown"} updated=${status.updated_at} iteration=${status.iteration}`);
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
      run(["systemctl", "--user", "enable", BRAINSTACK_DAEMON_SERVICE], { check: false });
      run(["systemctl", "--user", "restart", BRAINSTACK_DAEMON_SERVICE], { check: false });
    } else {
      const uid = typeof process.getuid === "function" ? process.getuid() : null;
      const domain = uid === null ? `gui/${process.env.UID || ""}` : `gui/${uid}`;
      run(["launchctl", "bootout", domain, path], { check: false });
      run(["launchctl", "bootstrap", domain, path], { check: false });
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
  options: { admin?: boolean; body?: Record<string, unknown>; idempotencyKey?: string; timeoutMs?: number } = {}
): Promise<Record<string, unknown>> {
  const baseUrl = brainApiBaseUrl(cfg);
  const headers: Record<string, string> = {};
  if (options.admin) {
    headers.Authorization = `Bearer ${brainAdminToken(cfg)}`;
  }
  if (options.idempotencyKey) {
    headers["Idempotency-Key"] = options.idempotencyKey;
  }
  if (options.body) {
    headers["Content-Type"] = "application/json";
  }
  const response = await fetch(new URL(path, baseUrl).toString(), {
    method,
    headers,
    body: options.body ? JSON.stringify(options.body) : undefined,
    signal: AbortSignal.timeout(options.timeoutMs ?? brainWriteTimeoutMs())
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

type ProposalDecisionAction = "approve" | "reject" | "apply" | "supersede" | "needs-work";

function proposalRemoteRepo(cfg: BrainstackConfig, args: ParsedArgs): string {
  return requireFlagValue(args, "remote-repo") || process.env.BRAINSTACK_TELEGRAM_REMOTE_REPO?.trim() || cfg.client.telegramRemoteRepo || "~/brainstack";
}

function shouldRunProposalCurationOnControl(cfg: BrainstackConfig, args: ParsedArgs): boolean {
  return cfg.profile.startsWith("client") && !hasFlag(args, "local");
}

function proposalCurationRemoteArgv(args: ParsedArgs, subcommand: string): string[] {
  const forwarded = ["proposals", subcommand, ...args.positional.slice(1), "--local"];
  const localOnlyFlags = new Set(["config", "profile", "root", "via", "remote-repo", "known-hosts", "ssh-trust", "local"]);
  for (const [key, value] of Object.entries(args.flags)) {
    if (localOnlyFlags.has(key)) {
      continue;
    }
    if (value === true) {
      forwarded.push(`--${key}`);
    } else if (Array.isArray(value)) {
      for (const item of value) {
        forwarded.push(`--${key}`, item);
      }
    } else {
      forwarded.push(`--${key}`, value);
    }
  }
  return forwarded;
}

function proposalCurationTimeoutMs(subcommand: string): number {
  if (["batch-merge", "harness-merge", "scan-merges"].includes(subcommand)) {
    return 660_000;
  }
  if (["auto-merge", "auto-consolidate"].includes(subcommand)) {
    return 180_000;
  }
  return 120_000;
}

async function maybeRunRemoteProposalCuration(cfg: BrainstackConfig, args: ParsedArgs, subcommand: string): Promise<boolean> {
  if (!shouldRunProposalCurationOnControl(cfg, args)) {
    return false;
  }
  const via = controlSshTarget(cfg, args, "proposal curation SSH target");
  if (!via) {
    throw new Error(
      `proposal ${subcommand} must run on the control host for client profiles; configure client.telegramVia, set BRAINSTACK_TELEGRAM_VIA, pass --via, or use --local for deliberate development/testing`
    );
  }
  const result = runControlRemoteScript(
    cfg,
    args,
    "proposal curation SSH target",
    remoteBrainctlScript(proposalRemoteRepo(cfg, args), proposalCurationRemoteArgv(args, subcommand), { preferInstalledBinary: true }),
    proposalCurationTimeoutMs(subcommand)
  );
  if (!result) {
    throw new Error(
      `proposal ${subcommand} must run on the control host for client profiles; configure client.telegramVia, set BRAINSTACK_TELEGRAM_VIA, pass --via, or use --local for deliberate development/testing`
    );
  }
  if (result.code !== 0) {
    throw new Error(`proposal ${subcommand} failed over ssh with exit ${result.code}\n${result.stderr || result.stdout}`);
  }
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);
  return true;
}

async function maybeRunRemoteProposalDecision(cfg: BrainstackConfig, args: ParsedArgs, action: ProposalDecisionAction, id: string, reason?: string): Promise<boolean> {
  if (cfg.telemux.enabled || process.env.BRAIN_ADMIN_TOKEN || readEnvFile(join(cfg.paths.configRoot, "braind.secrets.env")).BRAIN_ADMIN_TOKEN) {
    return false;
  }
  const via = controlSshTarget(cfg, args, "proposal decision SSH target");
  if (!via) {
    return false;
  }
  const argv = ["proposals", action, id, ...(reason ? ["--reason", reason] : []), ...(hasFlag(args, "json") ? ["--json"] : [])];
  const result = runControlRemoteScript(cfg, args, "proposal decision SSH target", remoteBrainctlScript(proposalRemoteRepo(cfg, args), argv), 120_000);
  if (!result) {
    return false;
  }
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
      "Usage: brainctl proposals list [--status open|pending|approved|applied|rejected|superseded|needs-human] [--json]\n       brainctl proposals groups [--status open|pending|approved|applied|rejected|superseded|needs-human] [--min-size N] [--json]\n       brainctl proposals show <id> [--json]\n       brainctl proposals merge-group <group-key|group-label> [--id ID] [--submit] [--limit N|--all] [--target-page wiki/PATH.md] [--needs-human] [--close-sources] [--json] [--via SSH_TARGET] [--remote-repo PATH] [--local]\n       brainctl proposals auto-merge [--submit] [--min-size N] [--max-group-size N|--allow-large-groups] [--max-source-group-size N|--all-source-groups] [--limit-groups N|--all-groups] [--relation-window day|all] [--keep-sources] [--json] [--via SSH_TARGET] [--remote-repo PATH] [--local]\n       brainctl proposals batch-merge [--submit] [--limit 100] [--auto-threshold 0.8] [--harness codex|claude] [--harness-bin PATH] [--keep-sources] [--json] [--via SSH_TARGET] [--remote-repo PATH] [--local]\n       brainctl proposals approve <id> [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]\n       brainctl proposals reject <id> [--reason TEXT] [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]\n       brainctl proposals supersede <id> [--reason TEXT] [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]\n       brainctl proposals needs-work <id> [--reason TEXT] [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]\n       brainctl proposals apply <id> [--via SSH_TARGET] [--remote-repo PATH] [--known-hosts FILE] [--ssh-trust pinned|accept-new|default]"
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
      if (await maybeRunRemoteProposalCuration(cfg, args, sub)) {
        return;
      }
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
    case "auto-merge":
    case "auto-consolidate": {
      if (await maybeRunRemoteProposalCuration(cfg, args, sub)) {
        return;
      }
      const result = await createAutomaticReviewGroupMerges(cfg, args);
      if (hasFlag(args, "json")) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }
      console.log(
        [
          `auto_merge=${result.dryRun ? "dry-run" : "submitted"}`,
          `considered=${result.considered}`,
          `selected=${result.selected}`,
          `merged=${result.merged.length}`,
          `skipped=${result.skipped.length}`
        ].join(" ")
      );
      for (const item of result.merged) {
        console.log(
          [
            item.groupKey,
            `relation=${item.relationKey}`,
            `selected=${item.selected}`,
            `target=${item.targetPage}`,
            item.conflicts.length ? `needs_human=${item.conflicts.join("; ")}` : "needs_human=false",
            item.closed.length ? `closed_sources=${item.closed.join(",")}` : null
          ]
            .filter(Boolean)
            .join(" ")
        );
      }
      for (const item of result.skipped.slice(0, 10)) {
        console.log(`skipped ${item.groupKey}: ${item.reason}`);
      }
      if (result.dryRun) {
        console.log("No writes performed. Rerun with --submit to create consolidated proposals and supersede source candidates.");
      }
      return;
    }
    case "batch-merge":
    case "harness-merge":
    case "scan-merges": {
      if (await maybeRunRemoteProposalCuration(cfg, args, sub)) {
        return;
      }
      const result = await createHarnessProposalMerges(cfg, args);
      if (hasFlag(args, "json")) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }
      console.log(
        [
          `batch_merge=${result.dryRun ? "dry-run" : "submitted"}`,
          `harness=${result.harness}`,
          `inspected=${result.inspected}/${result.totalOpen}`,
          `candidates=${result.candidates}`,
          `created=${result.merged.length}`,
          `auto_threshold=${result.autoThreshold}`
        ].join(" ")
      );
      for (const warning of result.warnings) {
        console.log(`warning: ${warning}`);
      }
      for (const item of result.merged) {
        console.log(
          [
            item.autoMerged ? "auto-merged" : "needs-review",
            `confidence=${Math.round(item.confidence * 100)}%`,
            `sources=${item.sourceIds.join(",")}`,
            `target=${item.targetPage}`,
            item.closed.length ? `closed_sources=${item.closed.join(",")}` : null,
            `title=${item.title}`
          ]
            .filter(Boolean)
            .join(" ")
        );
      }
      for (const item of result.skipped.slice(0, 10)) {
        console.log(`skipped${item.ids?.length ? ` ${item.ids.join(",")}` : ""}: ${item.reason}`);
      }
      if (result.dryRun) {
        console.log("No writes performed. Rerun with --submit to create consolidated proposals.");
      }
      return;
    }
    case "approve":
    case "reject":
    case "supersede":
    case "needs-work":
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
      if (hasFlag(args, "json")) {
        console.log(JSON.stringify(result, null, 2));
        return;
      }
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

async function maybeRunRemoteCuratorControl(cfg: BrainstackConfig, args: ParsedArgs, subcommand: "run" | "install"): Promise<boolean> {
  if (cfg.telemux.enabled) {
    return false;
  }
  const via = controlSshTarget(cfg, args, "curator control SSH target", { allowRemoteSshFallback: true });
  if (!via) {
    return false;
  }
  const remoteRepo = requireFlagValue(args, "remote-repo") || process.env.BRAINSTACK_TELEGRAM_REMOTE_REPO?.trim() || cfg.client.telegramRemoteRepo || "~/brainstack";
  const result = runControlRemoteScript(
    cfg,
    args,
    "curator control SSH target",
    remoteBrainctlScript(remoteRepo, ["curator", subcommand]),
    120_000,
    { allowRemoteSshFallback: true }
  );
  if (!result) {
    return false;
  }
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
  const terminalItems = () => items.map((entry) => normalizeOutboxItem(entry.item)).filter((item) => item.terminal_error);
  switch (sub) {
    case "status":
      console.log(`outbox=${outboxRoot(cfg)}`);
      console.log(`namespaces=${scans.length}`);
      console.log(`queued=${items.length}`);
      console.log(`terminal=${terminalItems().length}`);
      console.log(`corrupt=${corrupt.length}`);
      return;
    case "list":
      console.log(
        items.length || corrupt.length
          ? items
              .map(({ item }) => {
                const normalized = normalizeOutboxItem(item);
                const status = normalized.terminal_error ? "terminal" : normalized.last_error ? "queued-error" : "queued";
                const detail = formatOutboxErrorSummary(summarizeOutboxTerminalErrors([normalized.terminal_error || normalized.last_error]));
                return [
                  normalized.id,
                  normalized.endpoint,
                  `status=${status}`,
                  `retries=${normalized.retry_count}`,
                  normalized.brain_id ? `brain=${normalized.brain_id}` : null,
                  normalized.import_token_env ? `token_env=${normalized.import_token_env}` : null,
                  `source=${normalized.source_machine}/${normalized.source_harness}`,
                  detail ? `error=${detail}` : null
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
        const after = await scanAllOutboxes(cfg);
        const terminalErrors = summarizeOutboxTerminalErrors(
          after
            .flatMap((scan) => scan.items)
            .map((entry) => normalizeOutboxItem(entry.item).terminal_error)
        );
        if (terminalErrors.length) {
          console.log(`terminal_reasons=${formatOutboxErrorSummary(terminalErrors)}`);
        }
        throw new Error("saved outbox writes are paused after non-retryable failures; fix the reported cause, then run `brainctl outbox retry --all` and `brainctl outbox flush`, or purge only if the saved writes are obsolete");
      }
      if (result.corrupt > 0) {
        throw new Error("saved outbox writes include corrupt/unsafe files; inspect `brainctl outbox list` and run `brainctl outbox purge-corrupt --yes` only if those saved writes are unrecoverable");
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

function lifecycleUsage(): string {
  return [
    "Usage: brainctl lifecycle status --config brainstack.yaml [--json] [--timeout-ms N]",
    "       brainctl lifecycle repair --config brainstack.yaml [--target codex|claude|cursor|all] [--dry-run] [--no-runtime] [--no-guidance] [--no-daemon] [--no-hooks] [--no-skills] [--no-status] [--no-start] [--sync-skills]",
    "       brainctl lifecycle upgrade --config brainstack.yaml [--dry-run] [--no-daemon] [--no-status] [--no-start]",
    "       brainctl lifecycle uninstall --config brainstack.yaml --dry-run|--yes [--scope control|worker|client|all]",
    "",
    "Lifecycle commands are safe wrappers around existing primitives:",
    "  status    bounded read-only aggregate status",
    "  repair    re-render runtime/guidance files, reinstall daemon/hooks, refresh shared skills from the local clone",
    "  upgrade   backup plus runtime refresh for the currently installed version",
    "  uninstall guarded manifest-driven destroy; defaults to full removal for control installs and client/worker removal for edge installs"
  ].join("\n");
}

function lifecycleConfigPath(args: ParsedArgs): string {
  return abs(requireFlagValue(args, "config") || brainstackDefaultConfigPath());
}

function lifecycleTargetsFromArgs(args: ParsedArgs): HookTarget[] {
  const target = (requireFlagValue(args, "target") || "all").trim().toLowerCase();
  if (target === "all") {
    return ["codex", "claude", "cursor"];
  }
  return [normalizeHookTarget(target)];
}

function lifecycleDisplayCommand(command: string, parts: string[]): string {
  return ["brainctl", command, ...parts].join(" ");
}

function lifecycleRepairPlan(cfg: BrainstackConfig, args: ParsedArgs, configPath: string): Array<{ name: string; command: string; detail: string }> {
  const target = requireFlagValue(args, "target") || "all";
  const selectedTargets = target === "all" ? ["codex", "claude", "cursor"] as HookTarget[] : [normalizeHookTarget(target)];
  const noStart = hasFlag(args, "no-start");
  const steps: Array<{ name: string; command: string; detail: string }> = [];
  if (!hasFlag(args, "no-runtime")) {
    steps.push({
      name: "runtime",
      command: lifecycleDisplayCommand("apply-runtime", ["--config", configPath, "--profile", cfg.profile]),
      detail: "refresh generated runtime, bootstrap templates, service files, and ownership manifest; canonical brain content is not seeded"
    });
  }
  if (!hasFlag(args, "no-guidance") && usesLocalHarnessGuidance(cfg)) {
    steps.push({
      name: "guidance",
      command: "managed by lifecycle repair",
      detail: "repair missing local Codex/Claude/Cursor shared-brain guidance stubs without cloning or pulling the shared-brain repo"
    });
  }
  if (!hasFlag(args, "no-daemon") && usesBrainstackDaemon(cfg)) {
    steps.push({
      name: "daemon",
      command: lifecycleDisplayCommand("daemon install", ["--config", configPath, ...(!noStart ? ["--start"] : [])]),
      detail: "install or replace the local background daemon service"
    });
  }
  if (!hasFlag(args, "no-hooks")) {
    steps.push({
      name: "hooks",
      command: lifecycleDisplayCommand("hooks install", ["--target", target, "--config", configPath]),
      detail: "install fail-open Brainstack hooks for selected harness targets"
    });
  }
  if (!hasFlag(args, "no-skills")) {
    for (const skillTarget of selectedTargets) {
      steps.push({
        name: `skills:${skillTarget}`,
        command: lifecycleDisplayCommand("skills refresh", ["--target", skillTarget, "--config", configPath, ...(!hasFlag(args, "sync-skills") ? ["--no-sync"] : [])]),
        detail: "refresh shared skill packages from the local shared-brain clone; pass --sync-skills to pull first"
      });
    }
  }
  if (!hasFlag(args, "no-status")) {
    steps.push({
      name: "status",
      command: lifecycleDisplayCommand("status", ["--config", configPath, "--timeout-ms", requireFlagValue(args, "timeout-ms") || "3000"]),
      detail: "finish with bounded aggregate diagnostics"
    });
  }
  return steps;
}

function printLifecycleRepairPlan(cfg: BrainstackConfig, args: ParsedArgs, configPath: string): void {
  const steps = lifecycleRepairPlan(cfg, args, configPath);
  if (hasFlag(args, "json")) {
    console.log(JSON.stringify({ ok: true, dry_run: true, profile: cfg.profile, machine: cfg.machine.name, config_path: configPath, steps }, null, 2));
    return;
  }
  console.log(`lifecycle repair plan profile=${cfg.profile} machine=${cfg.machine.name} config=${configPath}`);
  for (const step of steps) {
    console.log(`- ${step.name}: ${step.detail}`);
    console.log(`  ${step.command}`);
  }
  if (!steps.length) {
    console.log("(no steps selected)");
  }
}

async function commandLifecycleRepair(args: ParsedArgs): Promise<void> {
  const configPath = lifecycleConfigPath(args);
  const cfg = await loadConfig(configPath, flag(args, "profile"), flag(args, "root"));
  if (hasFlag(args, "dry-run")) {
    printLifecycleRepairPlan(cfg, args, configPath);
    return;
  }
  if (hasFlag(args, "json")) {
    throw new Error("lifecycle repair --json is only supported with --dry-run; use lifecycle status --json after repair for machine-readable diagnostics");
  }
  if (!hasFlag(args, "no-runtime")) {
    await commandApplyRuntime({ ...args, flags: { ...args.flags, config: configPath, profile: cfg.profile } }, false);
  }
  if (!hasFlag(args, "no-guidance") && usesLocalHarnessGuidance(cfg)) {
    const touched = await repairLocalClientGuidance(cfg);
    console.log(`local guidance repaired: ${touched.length ? touched.join(", ") : "(none)"}`);
  }
  if (!hasFlag(args, "no-daemon") && usesBrainstackDaemon(cfg)) {
    const daemonFlags: ParsedArgs["flags"] = { ...args.flags, config: configPath, profile: cfg.profile };
    delete daemonFlags["dry-run"];
    if (!hasFlag(args, "no-start")) {
      daemonFlags.start = true;
    }
    await commandDaemonInstall({ command: "daemon", positional: ["install"], flags: daemonFlags });
  }
  if (!hasFlag(args, "no-hooks")) {
    const hooksFlags: ParsedArgs["flags"] = { ...args.flags, config: configPath, target: requireFlagValue(args, "target") || "all" };
    delete hooksFlags["dry-run"];
    await commandHooks({ command: "hooks", positional: ["install"], flags: hooksFlags });
  }
  if (!hasFlag(args, "no-skills")) {
    for (const target of lifecycleTargetsFromArgs(args)) {
      const skillFlags: ParsedArgs["flags"] = { ...args.flags, config: configPath, target };
      delete skillFlags["dry-run"];
      if (!hasFlag(args, "sync-skills")) {
        skillFlags["no-sync"] = true;
      }
      await commandSkills({ command: "skills", positional: ["refresh"], flags: skillFlags });
    }
  }
  if (!hasFlag(args, "no-status")) {
    await commandStatus({
      command: "status",
      positional: [],
      flags: { config: configPath, "timeout-ms": requireFlagValue(args, "timeout-ms") || "3000" }
    });
  }
}

async function commandLifecycleUpgrade(args: ParsedArgs): Promise<void> {
  const configPath = lifecycleConfigPath(args);
  const cfg = await loadConfig(configPath, flag(args, "profile"), flag(args, "root"));
  if (hasFlag(args, "dry-run")) {
    console.log(`lifecycle upgrade dry-run profile=${cfg.profile} machine=${cfg.machine.name} config=${configPath}`);
    console.log("- backup current runtime state");
    console.log("- refresh generated runtime artifacts without seeding canonical shared-brain content");
    if (!hasFlag(args, "no-daemon") && usesBrainstackDaemon(cfg)) {
      console.log(`- reinstall local daemon service${hasFlag(args, "no-start") ? "" : " and start it"}`);
    }
    if (!hasFlag(args, "no-status")) {
      console.log("- run bounded aggregate status");
    }
    return;
  }
  await commandApplyRuntime({ ...args, flags: { ...args.flags, config: configPath, profile: cfg.profile } }, true);
  if (!hasFlag(args, "no-daemon") && usesBrainstackDaemon(cfg)) {
    const daemonFlags: ParsedArgs["flags"] = { ...args.flags, config: configPath, profile: cfg.profile };
    if (!hasFlag(args, "no-start")) {
      daemonFlags.start = true;
    }
    await commandDaemonInstall({ command: "daemon", positional: ["install"], flags: daemonFlags });
  }
  if (!hasFlag(args, "no-status")) {
    await commandStatus({
      command: "status",
      positional: [],
      flags: { config: configPath, "timeout-ms": requireFlagValue(args, "timeout-ms") || "3000" }
    });
  }
}

async function commandLifecycleUninstall(args: ParsedArgs): Promise<void> {
  if (commandHasHelp(args)) {
    console.log(lifecycleUsage());
    return;
  }
  const configPath = lifecycleConfigPath(args);
  const cfg = await loadConfig(configPath, flag(args, "profile"), flag(args, "root"));
  const scope = requireFlagValue(args, "scope") || (runsBraind(cfg) ? "all" : destroyScopeFromProfile(cfg.profile));
  await commandDestroy({ ...args, command: "destroy", positional: [], flags: { ...args.flags, config: configPath, profile: cfg.profile, scope } });
}

async function commandLifecycle(args: ParsedArgs): Promise<void> {
  const sub = args.positional[0] || "status";
  if (sub === "help" || sub === "--help" || sub === "-h") {
    console.log(lifecycleUsage());
    return;
  }
  switch (sub) {
    case "status":
      return await commandStatus({ ...args, positional: args.positional.slice(1), flags: { ...args.flags, config: lifecycleConfigPath(args) } });
    case "repair":
      return await commandLifecycleRepair({ ...args, positional: args.positional.slice(1) });
    case "upgrade":
      return await commandLifecycleUpgrade({ ...args, positional: args.positional.slice(1) });
    case "uninstall":
    case "remove":
    case "destroy":
      return await commandLifecycleUninstall({ ...args, positional: args.positional.slice(1) });
    default:
      throw new Error(`Unknown lifecycle subcommand: ${sub}\n${lifecycleUsage()}`);
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
  if (commandHasHelp(args)) {
    console.log("Usage: brainctl destroy --config brainstack.yaml [--profile ...] --dry-run|--yes [--scope control|worker|client|all] [--remove-shared-brain] [--remove-private-brain] [--remove-tailscale-serve]");
    return;
  }
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
      for (const [profile, skills] of Object.entries(PORTABLE_SKILL_PROFILES) as Array<[string, PortableSkillName[]]>) {
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
    case "import":
      return await commandSkillsImport(args);
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
      console.log("Usage: brainctl skills install [--target codex] [--profile client|operator|control|worker] [--skill NAME|--all] [--dir DIR] [--dry-run]\n       brainctl skills import [PATH_OR_URL] [--config brainstack.yaml] [--select 1,3|all] [--apply] [--json]\n       brainctl skills refresh [--target codex|claude|cursor] [--config brainstack.yaml] [--repo PATH] [--skill NAME] [--dir DIR] [--no-sync] [--force] [--quiet]\n       brainctl skills doctor [--target codex|claude|cursor] [--dir DIR] [--check-remote] [--json]\n       brainctl skills list");
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
  if (kind !== "import" && kind !== "admin") {
    throw new Error("rotate-token requires --kind import|admin");
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
    case "lifecycle":
      return await commandLifecycle(args);
    case "daemon":
      return await commandDaemon(args);
    case "updates":
      return await commandUpdates(args);
    case "fleet":
      return await commandFleet(args);
    case "capabilities":
    case "capability":
      return await commandCapabilities(args);
    case "uploads":
    case "upload":
      return await commandUploads(args);
    case "context-packs":
    case "packs":
      return await commandContextPacks(args);
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
