import { existsSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { basename, join } from "node:path";
import { flag, hasFlag, requireFlagValue, type ParsedArgs } from "../args";
import {
  brainstackDefaultConfigPath,
  objectAt,
  parseSimpleYaml,
  stringifySimpleYaml,
  type BrainstackConfig,
  type BrainstackWorkerConfig
} from "../config";
import { abs, shellSingleQuote } from "../paths";
import { run, runWithStdinFile, writeText } from "../runtime";

interface ShellResult {
  code: number | null;
  stdout: string;
  stderr: string;
  timedOut?: boolean;
}

interface CapabilityDeps {
  loadConfig: (path?: string | null, profile?: string | null, root?: string | null) => Promise<BrainstackConfig>;
  defaultWorkers: (cfg: BrainstackConfig) => BrainstackWorkerConfig[];
  runWorkerShell: (
    cfg: BrainstackConfig,
    worker: BrainstackWorkerConfig,
    script: string,
    timeoutSeconds?: number,
    usePathCache?: boolean
  ) => ShellResult;
  workerRemoteTarget: (worker: BrainstackWorkerConfig) => string;
  workerSshPortArgs: (worker: BrainstackWorkerConfig) => string[];
  workerSshTrustArgs: (cfg: BrainstackConfig, worker: BrainstackWorkerConfig) => string[];
  runRemoteBrainctl: (cfg: BrainstackConfig, args: ParsedArgs, argv: string[], timeoutMs: number) => Promise<boolean>;
}

interface VoiceModelSpec {
  id: string;
  fileName: string;
  url: string;
  sha256: string | null;
  size: string;
}

const WHISPERFILE_MODELS: Record<string, VoiceModelSpec> = {
  "tiny.en": {
    id: "tiny.en",
    fileName: "whisper-tiny.en.llamafile",
    url: "https://huggingface.co/Mozilla/whisperfile/resolve/main/whisper-tiny.en.llamafile",
    sha256: "0e8d17c72d3fd259d4ac761dd9f8f3a30ad21affb818c1aaf17f63945254f25a",
    size: "87 MB"
  },
  "small.en": {
    id: "small.en",
    fileName: "whisper-small.en.llamafile",
    url: "https://huggingface.co/Mozilla/whisperfile/resolve/main/whisper-small.en.llamafile",
    sha256: null,
    size: "497 MB"
  },
  "medium.en": {
    id: "medium.en",
    fileName: "whisper-medium.en.llamafile",
    url: "https://huggingface.co/Mozilla/whisperfile/resolve/main/whisper-medium.en.llamafile",
    sha256: null,
    size: "1.83 GB"
  },
  "large-v2": {
    id: "large-v2",
    fileName: "whisper-large-v2.llamafile",
    url: "https://huggingface.co/Mozilla/whisperfile/resolve/main/whisper-large-v2.llamafile",
    sha256: null,
    size: "3.39 GB"
  },
  "large-v3": {
    id: "large-v3",
    fileName: "whisper-large-v3.llamafile",
    url: "https://huggingface.co/Mozilla/whisperfile/resolve/main/whisper-large-v3.llamafile",
    sha256: null,
    size: "3.39 GB"
  }
};
const TELEMUX_SERVICE_NAME = "telemux.service";

function capabilitiesUsage(): string {
  return [
    "Usage:",
    "  brainctl capabilities list --config brainstack.yaml",
    "  brainctl capabilities install voice --target MACHINE [--model tiny.en|small.en|medium.en|large-v2|large-v3] [--install-root DIR] [--echo|--no-echo] [--dry-run]",
    "  brainctl capabilities doctor voice --config brainstack.yaml [--json]",
    "  brainctl capabilities test voice --file AUDIO_FILE --config brainstack.yaml [--json]",
    "",
    "Natural-language surfaces should delegate to the same install command, for example: install voice on erbine."
  ].join("\n");
}

function quote(value: string): string {
  return shellSingleQuote(value);
}

function configPathForArgs(args: ParsedArgs): string {
  return abs(flag(args, "config") || process.env.BRAINSTACK_CONFIG?.trim() || brainstackDefaultConfigPath());
}

function parsedArgsToCanonicalArgv(args: ParsedArgs): string[] {
  const argv = ["capabilities", ...args.positional];
  for (const [key, value] of Object.entries(args.flags)) {
    if (value === true) {
      argv.push(`--${key}`);
    } else if (Array.isArray(value)) {
      for (const item of value) {
        argv.push(`--${key}`, item);
      }
    } else {
      argv.push(`--${key}`, value);
    }
  }
  return argv;
}

function normalizeVoiceModel(input: string): string {
  const normalized = input.trim().toLowerCase().replace(/_/g, "-");
  const aliases: Record<string, string> = {
    tiny: "tiny.en",
    "tiny-en": "tiny.en",
    small: "small.en",
    "small-en": "small.en",
    medium: "medium.en",
    "medium-en": "medium.en"
  };
  return aliases[normalized] || normalized;
}

function selectedVoiceModel(args: ParsedArgs): VoiceModelSpec {
  const modelId = normalizeVoiceModel(flag(args, "model") || "tiny.en");
  const modelUrl = requireFlagValue(args, "model-url");
  if (modelUrl) {
    validateModelUrl(modelUrl, args);
    const fileName = requireFlagValue(args, "file-name") || basename(new URL(modelUrl).pathname) || "whisper.llamafile";
    return {
      id: modelId === "tiny.en" && !flag(args, "model") ? "custom" : modelId,
      fileName,
      url: modelUrl,
      sha256: requireFlagValue(args, "sha256") || null,
      size: "custom"
    };
  }
  const spec = WHISPERFILE_MODELS[modelId];
  if (!spec) {
    throw new Error(`Unknown voice model: ${modelId}. Expected one of ${Object.keys(WHISPERFILE_MODELS).join(", ")}, or pass --model-url.`);
  }
  return spec;
}

function validateModelUrl(input: string, args: ParsedArgs): void {
  let parsed: URL;
  try {
    parsed = new URL(input);
  } catch {
    throw new Error("--model-url must be a URL");
  }
  if (parsed.protocol === "https:") {
    return;
  }
  const host = parsed.hostname.toLowerCase();
  const localHttp = parsed.protocol === "http:" && (host === "127.0.0.1" || host === "localhost" || host === "::1");
  if ((localHttp || parsed.protocol === "file:") && hasFlag(args, "allow-insecure-model-url")) {
    return;
  }
  throw new Error("--model-url must use HTTPS. For trusted local testing only, pass --allow-insecure-model-url.");
}

function findLocalWorker(cfg: BrainstackConfig, workers: BrainstackWorkerConfig[]): BrainstackWorkerConfig | null {
  return (
    workers.find((worker) => worker.transport === "local") ||
    workers.find((worker) => worker.name === cfg.telemux.localMachine) ||
    workers.find((worker) => worker.name === cfg.machine.name) ||
    null
  );
}

function resolveTargetWorker(deps: Pick<CapabilityDeps, "defaultWorkers">, cfg: BrainstackConfig, args: ParsedArgs): {
  requested: string;
  worker: BrainstackWorkerConfig;
  targetMode: "local" | "worker";
} {
  const workers = deps.defaultWorkers(cfg);
  const localWorker = findLocalWorker(cfg, workers);
  const requested =
    requireFlagValue(args, "target") ||
    requireFlagValue(args, "machine") ||
    args.positional[2] ||
    cfg.capabilities.voice.worker ||
    cfg.telemux.localMachine ||
    cfg.machine.name;
  const requestedLower = requested.toLowerCase();
  const exact = workers.find((worker) => worker.name === requested);
  const worker =
    exact ||
    (["local", "here", "control", cfg.machine.name.toLowerCase(), cfg.telemux.localMachine.toLowerCase()].includes(requestedLower)
      ? localWorker
      : null);
  if (!worker) {
    const names = workers.map((entry) => entry.name).join(", ") || "(none)";
    throw new Error(`Unknown target machine ${requested}. Known machines: ${names}`);
  }
  return {
    requested,
    worker,
    targetMode: worker.transport === "local" ? "local" : "worker"
  };
}

function safeInstallRoot(args: ParsedArgs, cfg: BrainstackConfig): string {
  const value = requireFlagValue(args, "install-root") || cfg.capabilities.voice.installRoot || "~/.local/share/brainstack/capabilities/voice";
  if (!value.trim() || value.startsWith("-") || value.includes("\0") || value.split("/").includes("..")) {
    throw new Error("--install-root must be a safe absolute or home-relative path");
  }
  if (!value.startsWith("/") && !value.startsWith("~/") && value !== "~") {
    throw new Error("--install-root must be absolute or home-relative");
  }
  return value;
}

function maxBytes(args: ParsedArgs, fallback: number): number {
  const raw = requireFlagValue(args, "max-bytes");
  if (raw === undefined) {
    return fallback;
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed < 1024 * 1024 || parsed > 512 * 1024 * 1024) {
    throw new Error("--max-bytes must be an integer from 1048576 to 536870912");
  }
  return parsed;
}

function maxDurationSeconds(args: ParsedArgs, fallback: number | null): number | null {
  if (hasFlag(args, "no-max-duration")) {
    return null;
  }
  const raw = requireFlagValue(args, "max-duration-seconds");
  if (raw === undefined) {
    return fallback;
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error("--max-duration-seconds must be a positive integer");
  }
  return parsed;
}

function commandLine(command: string, args: string[]): string {
  const normalizedArgs = args.length ? [...args] : ["-f", "{input}", "-pc"];
  if (!normalizedArgs.includes("{input}")) {
    normalizedArgs.push("{input}");
  }
  return [quote(command), ...normalizedArgs.map((arg) => (arg === "{input}" ? '"$input"' : quote(arg)))].join(" ");
}

function installScript(spec: VoiceModelSpec, installRoot: string): string {
  const expectedSha = spec.sha256 || "";
  return `
set -euo pipefail
brainstack_expand_home() {
  case "$1" in
    "~") printf '%s\\n' "$HOME" ;;
    "~/"*) printf '%s/%s\\n' "$HOME" "\${1#\\~/}" ;;
    *) printf '%s\\n' "$1" ;;
  esac
}
brainstack_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "missing sha256sum or shasum" >&2
    return 127
  fi
}
install_root="$(brainstack_expand_home ${quote(installRoot)})"
url=${quote(spec.url)}
file_name=${quote(spec.fileName)}
expected_sha=${quote(expectedSha)}
if ! command -v ffmpeg >/dev/null 2>&1; then
  echo "missing ffmpeg on target machine; install ffmpeg before voice transcription" >&2
  exit 127
fi
mkdir -p "$install_root"
command_path="$install_root/$file_name"
if [ -x "$command_path" ]; then
  if [ -z "$expected_sha" ] || [ "$(brainstack_sha256 "$command_path")" = "$expected_sha" ]; then
    printf 'installed=already\\n'
    printf 'command=%s\\n' "$command_path"
    exit 0
  fi
fi
tmp="$install_root/.$file_name.$$"
rm -f "$tmp"
cleanup() {
  rm -f "$tmp"
}
trap cleanup EXIT
if command -v curl >/dev/null 2>&1; then
  curl -fL --retry 3 --connect-timeout 20 --max-time 1800 "$url" -o "$tmp"
elif command -v wget >/dev/null 2>&1; then
  wget -O "$tmp" "$url"
else
  echo "missing curl or wget on target machine" >&2
  exit 127
fi
if [ -n "$expected_sha" ]; then
  actual_sha="$(brainstack_sha256 "$tmp")"
  if [ "$actual_sha" != "$expected_sha" ]; then
    echo "checksum mismatch for $file_name: expected $expected_sha got $actual_sha" >&2
    exit 65
  fi
fi
chmod 0755 "$tmp"
mv -f "$tmp" "$command_path"
trap - EXIT
"$command_path" --help >/dev/null 2>&1 || true
printf 'installed=downloaded\\n'
printf 'command=%s\\n' "$command_path"
`.trim();
}

function stdoutValue(output: string, key: string): string | null {
  const match = output.match(new RegExp(`^${key}=([^\\n]+)$`, "m"));
  return match?.[1]?.trim() || null;
}

async function loadRawConfigObject(configPath: string): Promise<Record<string, unknown>> {
  if (!existsSync(configPath)) {
    throw new Error(`Brainstack config not found: ${configPath}`);
  }
  const text = await readFile(configPath, "utf8");
  return configPath.endsWith(".json") ? (JSON.parse(text) as Record<string, unknown>) : parseSimpleYaml(text);
}

async function writeRawConfigObject(configPath: string, raw: Record<string, unknown>): Promise<void> {
  const text = configPath.endsWith(".json") ? `${JSON.stringify(raw, null, 2)}\n` : stringifySimpleYaml(raw);
  await writeText(configPath, text, 0o600);
}

function voiceRuntimeUpdates(cfg: BrainstackConfig, configPath: string, voice: Record<string, unknown>): Record<string, string> {
  const args = Array.isArray(voice.args) ? voice.args.map(String) : ["-f", "{input}", "-pc"];
  return {
    BRAINSTACK_CONFIG: configPath,
    FACTORY_BRAINCTL_BIN: "brainctl",
    FACTORY_TRANSCRIPTION_ENABLED: voice.enabled ? "1" : "0",
    FACTORY_TRANSCRIPTION_TARGET: String(voice.target || "local"),
    FACTORY_TRANSCRIPTION_WORKER: String(voice.worker || ""),
    FACTORY_TRANSCRIPTION_COMMAND: String(voice.command || ""),
    FACTORY_TRANSCRIPTION_ARGS_JSON: JSON.stringify(args),
    FACTORY_TRANSCRIPTION_TIMEOUT_MS: String(voice.timeoutMs || 120_000),
    FACTORY_TRANSCRIPTION_ECHO: voice.echoTranscript ? "1" : "0",
    FACTORY_TRANSCRIPTION_MAX_BYTES: String(voice.maxBytes || 20 * 1024 * 1024),
    FACTORY_TRANSCRIPTION_MAX_DURATION_SECONDS: voice.maxDurationSeconds === null ? "" : String(voice.maxDurationSeconds || 300),
    FACTORY_LOCAL_MACHINE: cfg.telemux.localMachine
  };
}

async function upsertEnvMany(path: string, updates: Record<string, string>, mode = 0o644): Promise<void> {
  const existing = existsSync(path) ? await readFile(path, "utf8") : "";
  const seen = new Set<string>();
  const next: string[] = [];
  for (const rawLine of existing.split(/\r?\n/)) {
    const line = rawLine.trimEnd();
    if (!line) {
      continue;
    }
    const key = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=/)?.[1];
    if (key && Object.hasOwn(updates, key)) {
      next.push(`${key}=${updates[key]}`);
      seen.add(key);
      continue;
    }
    next.push(line);
  }
  for (const [key, value] of Object.entries(updates)) {
    if (!seen.has(key)) {
      next.push(`${key}=${value}`);
    }
  }
  await writeText(path, `${next.join("\n")}\n`, mode);
}

async function persistVoiceCapability(options: {
  cfg: BrainstackConfig;
  args: ParsedArgs;
  configPath: string;
  targetMode: "local" | "worker";
  worker: BrainstackWorkerConfig;
  spec: VoiceModelSpec;
  installRoot: string;
  commandPath: string;
}): Promise<Record<string, unknown>> {
  const { cfg, args, configPath, targetMode, worker, spec, installRoot, commandPath } = options;
  const raw = await loadRawConfigObject(configPath);
  const capabilities = objectAt(raw, "capabilities");
  const maxDuration = maxDurationSeconds(args, cfg.capabilities.voice.maxDurationSeconds);
  const voice = {
    enabled: true,
    target: targetMode,
    worker: targetMode === "worker" ? worker.name : null,
    engine: "whisperfile",
    model: spec.id,
    installRoot,
    command: commandPath,
    args: ["-f", "{input}", "-pc"],
    timeoutMs: depsParseTimeout(args),
    echoTranscript: !hasFlag(args, "no-echo"),
    maxBytes: maxBytes(args, cfg.capabilities.voice.maxBytes),
    maxDurationSeconds: maxDuration,
    modelUrl: spec.url,
    sha256: spec.sha256,
    installedAt: new Date().toISOString()
  };
  capabilities.voice = voice;
  raw.capabilities = capabilities;
  await writeRawConfigObject(configPath, raw);
  await upsertEnvMany(join(cfg.paths.configRoot, "telemux.runtime.env"), voiceRuntimeUpdates(cfg, configPath, voice));
  return voice;
}

function depsParseTimeout(args: ParsedArgs): number {
  const raw = requireFlagValue(args, "timeout-ms");
  if (raw === undefined) {
    return 120_000;
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed < 10_000 || parsed > 30 * 60_000) {
    throw new Error("--timeout-ms must be an integer from 10000 to 1800000");
  }
  return parsed;
}

async function restartTelemux(cfg: BrainstackConfig, args: ParsedArgs): Promise<string> {
  if (hasFlag(args, "no-restart")) {
    return "restart=skipped (--no-restart)";
  }
  if (!cfg.telemux.enabled) {
    return "restart=skipped (telemux disabled)";
  }
  const service = TELEMUX_SERVICE_NAME;
  const systemctl = Bun.which("systemctl");
  if (!systemctl) {
    return `restart=manual (systemctl not found; restart ${service})`;
  }
  const delayMs = Number(requireFlagValue(args, "restart-delay-ms") || "0");
  if (Number.isFinite(delayMs) && delayMs > 0) {
    const seconds = Math.max(1, Math.ceil(delayMs / 1000));
    Bun.spawn(["bash", "-lc", `(sleep ${seconds}; ${quote(systemctl)} --user restart ${quote(service)} >/dev/null 2>&1) >/dev/null 2>&1 &`], {
      stdout: "ignore",
      stderr: "ignore"
    });
    return `restart=scheduled service=${service} delay_ms=${delayMs}`;
  }
  const result = run([systemctl, "--user", "restart", service], { check: false, timeoutMs: 30_000 });
  if (result.code !== 0) {
    return `restart=failed service=${service} ${String(result.stderr || result.stdout).replace(/\s+/g, " ").trim()}`;
  }
  return `restart=ok service=${service}`;
}

function voiceCommandConfigured(cfg: BrainstackConfig): string {
  const command = cfg.capabilities.voice.command.trim();
  if (!cfg.capabilities.voice.enabled) {
    throw new Error("Voice transcription is not installed. Run: brainctl capabilities install voice --target MACHINE");
  }
  if (!command) {
    throw new Error("Voice transcription is enabled, but capabilities.voice.command is empty. Re-run install.");
  }
  return command;
}

function doctorScript(command: string): string {
  return `
set -euo pipefail
command_path=${quote(command)}
if [ -x "$command_path" ]; then
  printf 'executable=1\\n'
else
  printf 'executable=0\\n'
  exit 66
fi
if command -v ffmpeg >/dev/null 2>&1; then
  printf 'ffmpeg=ok\\n'
else
  printf 'ffmpeg=missing\\n'
  exit 66
fi
"$command_path" --help >/dev/null 2>&1 || true
printf 'help_probe=ok\\n'
printf 'command=%s\\n' "$command_path"
`.trim();
}

function safeFileName(input: string): string {
  return basename(input).replace(/[^A-Za-z0-9._-]+/g, "-") || "audio";
}

function testScript(command: string, args: string[], fileName: string): string {
  return `
set -euo pipefail
tmp_dir="$(mktemp -d "\${TMPDIR:-/tmp}/brainstack-voice-test.XXXXXX")"
cleanup() { rm -rf "$tmp_dir"; }
trap cleanup EXIT
input="$tmp_dir/${safeFileName(fileName)}"
cat > "$input"
converted_input="$tmp_dir/input.wav"
if command -v ffmpeg >/dev/null 2>&1; then
  if ffmpeg -hide_banner -loglevel error -y -i "$input" -ar 16000 -ac 1 "$converted_input" >/dev/null 2>&1; then
    input="$converted_input"
  fi
fi
${commandLine(command, args)}
`.trim();
}

async function runVoiceTest(deps: CapabilityDeps, cfg: BrainstackConfig, worker: BrainstackWorkerConfig, filePath: string): Promise<ShellResult> {
  const command = voiceCommandConfigured(cfg);
  const script = testScript(command, cfg.capabilities.voice.args, filePath);
  const timeoutSeconds = Math.max(1, Math.ceil(cfg.capabilities.voice.timeoutMs / 1000));
  if (worker.transport === "local") {
    return await runWithStdinFile(["bash", "-lc", script], abs(filePath), {
      maxBytes: cfg.capabilities.voice.maxBytes
    });
  }
  const remoteShellCommand = `bash -lc ${quote(script)}`;
  const sshArgs =
    worker.transport === "tailscale-ssh"
      ? ["tailscale", "ssh", deps.workerRemoteTarget(worker), remoteShellCommand]
      : [
          "ssh",
          "-o",
          "BatchMode=yes",
          "-o",
          "ConnectTimeout=8",
          ...deps.workerSshTrustArgs(cfg, worker),
          ...deps.workerSshPortArgs(worker),
          deps.workerRemoteTarget(worker),
          remoteShellCommand
        ];
  const timeoutBin = Bun.which("timeout");
  return await runWithStdinFile(timeoutBin ? [timeoutBin, `${timeoutSeconds}s`, ...sshArgs] : sshArgs, abs(filePath), {
    maxBytes: cfg.capabilities.voice.maxBytes
  });
}

function configuredVoiceWorker(cfg: BrainstackConfig, deps: CapabilityDeps): BrainstackWorkerConfig {
  const workers = deps.defaultWorkers(cfg);
  if (cfg.capabilities.voice.target === "worker") {
    const workerName = cfg.capabilities.voice.worker;
    const worker = workers.find((entry) => entry.name === workerName);
    if (!worker) {
      throw new Error(`Voice transcription worker is not configured: ${workerName || "(missing)"}`);
    }
    return worker;
  }
  const local = findLocalWorker(cfg, workers);
  if (!local) {
    throw new Error("Voice transcription is configured for local execution, but no local worker exists.");
  }
  return local;
}

function printVoiceStatus(cfg: BrainstackConfig, deps: CapabilityDeps): void {
  const voice = cfg.capabilities.voice;
  const workers = deps.defaultWorkers(cfg);
  console.log(`voice=${voice.enabled ? "enabled" : "disabled"}`);
  console.log(`target=${voice.target}${voice.worker ? ` worker=${voice.worker}` : ""}`);
  console.log(`model=${voice.model || "n/a"} engine=${voice.engine}`);
  console.log(`command=${voice.command || "(not installed)"}`);
  console.log(`echo=${voice.echoTranscript ? "yes" : "no"} timeout_ms=${voice.timeoutMs} max_bytes=${voice.maxBytes}`);
  console.log("machines:");
  for (const worker of workers) {
    console.log(`  - ${worker.name} transport=${worker.transport} capabilities=${(worker.capabilities || []).join(",") || "n/a"}`);
  }
}

export function createCapabilitiesCommands(deps: CapabilityDeps) {
  async function commandCapabilities(args: ParsedArgs): Promise<void> {
    const sub = args.positional[0] || "list";
    if (sub === "help" || sub === "--help" || sub === "-h") {
      console.log(capabilitiesUsage());
      return;
    }

    const cfg = await deps.loadConfig(flag(args, "config"), flag(args, "profile"), flag(args, "root"));
    const shouldForward =
      !cfg.telemux.enabled &&
      !hasFlag(args, "no-remote") &&
      sub !== "list" &&
      !(sub === "test" && (requireFlagValue(args, "file") || args.positional[2]));
    if (shouldForward && (await deps.runRemoteBrainctl(cfg, args, parsedArgsToCanonicalArgv(args), sub === "install" ? 35 * 60_000 : 120_000))) {
      return;
    }

    switch (sub) {
      case "list": {
        printVoiceStatus(cfg, deps);
        return;
      }
      case "install": {
        const capability = args.positional[1] || "voice";
        if (capability !== "voice" && capability !== "transcription") {
          throw new Error(`Unknown capability: ${capability}`);
        }
        const spec = selectedVoiceModel(args);
        const installRoot = safeInstallRoot(args, cfg);
        const target = resolveTargetWorker(deps, cfg, args);
        if (hasFlag(args, "dry-run")) {
          console.log(`would_install=voice target=${target.worker.name} transport=${target.worker.transport}`);
          console.log(`model=${spec.id} file=${spec.fileName} size=${spec.size}`);
          console.log(`download=${spec.url}`);
          console.log(`install_root=${installRoot}`);
          console.log(`would_update=${configPathForArgs(args)}`);
          console.log(`would_restart=${cfg.telemux.enabled ? TELEMUX_SERVICE_NAME : "no (telemux disabled)"}`);
          return;
        }
        const result = deps.runWorkerShell(cfg, target.worker, installScript(spec, installRoot), 35 * 60, false);
        if (result.code !== 0) {
          throw new Error(`voice install failed on ${target.worker.name} with exit ${result.code}\n${result.stderr || result.stdout}`);
        }
        const commandPath = stdoutValue(result.stdout, "command");
        if (!commandPath) {
          throw new Error(`voice install on ${target.worker.name} did not report command path\n${result.stdout}`);
        }
        const configPath = configPathForArgs(args);
        const voice = await persistVoiceCapability({
          cfg,
          args,
          configPath,
          targetMode: target.targetMode,
          worker: target.worker,
          spec,
          installRoot,
          commandPath
        });
        const restart = await restartTelemux(cfg, args);
        console.log(`installed=voice target=${target.worker.name} mode=${target.targetMode}`);
        console.log(`model=${spec.id} command=${commandPath}`);
        console.log(`config=${configPath}`);
        console.log(restart);
        console.log(`echo=${voice.echoTranscript ? "yes" : "no"}`);
        console.log("test=send a Telegram voice note in any bound topic, or run: brainctl capabilities test voice --file /path/to/audio.ogg");
        return;
      }
      case "doctor":
      case "status": {
        const capability = args.positional[1] || "voice";
        if (capability !== "voice" && capability !== "transcription") {
          throw new Error(`Unknown capability: ${capability}`);
        }
        let payload: Record<string, unknown>;
        try {
          const worker = configuredVoiceWorker(cfg, deps);
          const command = voiceCommandConfigured(cfg);
          const result = deps.runWorkerShell(cfg, worker, doctorScript(command), 15, false);
          payload = {
            ok: result.code === 0,
            enabled: cfg.capabilities.voice.enabled,
            target: cfg.capabilities.voice.target,
            worker: worker.name,
            command,
            model: cfg.capabilities.voice.model,
            stdout: result.stdout.trim(),
            stderr: result.stderr.trim()
          };
          if (hasFlag(args, "json")) {
            console.log(JSON.stringify(payload, null, 2));
            return;
          }
          console.log(`voice=${payload.ok ? "ok" : "fail"} enabled=yes target=${worker.name} command=${command}`);
          if (result.stdout.trim()) console.log(result.stdout.trim());
          if (result.stderr.trim()) console.error(result.stderr.trim());
          if (result.code !== 0) {
            throw new Error(`voice doctor failed on ${worker.name}`);
          }
        } catch (error) {
          payload = { ok: false, error: error instanceof Error ? error.message : String(error) };
          if (hasFlag(args, "json")) {
            console.log(JSON.stringify(payload, null, 2));
            return;
          }
          throw error;
        }
        return;
      }
      case "test": {
        const capability = args.positional[1] || "voice";
        if (capability !== "voice" && capability !== "transcription") {
          throw new Error(`Unknown capability: ${capability}`);
        }
        const filePath = requireFlagValue(args, "file") || args.positional[2];
        if (!filePath) {
          throw new Error("Usage: brainctl capabilities test voice --file AUDIO_FILE");
        }
        if (!cfg.telemux.enabled) {
          throw new Error("Voice file tests must run on the control host. From a client, test with a Telegram voice message after install.");
        }
        const worker = configuredVoiceWorker(cfg, deps);
        const result = await runVoiceTest(deps, cfg, worker, filePath);
        if (hasFlag(args, "json")) {
          console.log(
            JSON.stringify(
              {
                ok: result.code === 0,
                worker: worker.name,
                exitCode: result.code,
                stdout: result.stdout.trim(),
                stderr: result.stderr.trim()
              },
              null,
              2
            )
          );
          return;
        }
        if (result.stdout.trim()) console.log(result.stdout.trim());
        if (result.stderr.trim()) console.error(result.stderr.trim());
        if (result.code !== 0) {
          throw new Error(`voice test failed on ${worker.name} with exit ${result.code}`);
        }
        return;
      }
      default:
        throw new Error(`Unknown capabilities subcommand: ${sub}\n${capabilitiesUsage()}`);
    }
  }

  return { commandCapabilities };
}
