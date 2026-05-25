import { expect, test } from "bun:test";
import { chmod, mkdtemp, mkdir, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { CommandHandler } from "../src/commands";
import { loadConfig, ensureProjectPaths } from "../src/config";
import { CronManager } from "../src/cron-manager";
import { CronScheduler } from "../src/cron-scheduler";
import { ContextService } from "../src/contexts";
import { FactoryDb } from "../src/db";
import { Dispatcher } from "../src/dispatcher";
import { ensureBasicLoops } from "../src/basic-loops";
import { flushBrainOutbox, postBrainImportOrQueue } from "../src/brain-outbox";
import { canonicalJson, sha256Hex } from "../../../packages/outbox/src/outbox";
import {
  parseArtifactEntries,
  removeArtifactEntriesFromMarkdown,
  resolveManifestRequests,
  type TelegramAttachmentKind
} from "../src/telegram-attachments";
import type { TelegramMessage, TelegramTarget } from "../src/telegram";
import { WorkerService } from "../src/workers";

class FakeTelegram {
  readonly sent: Array<{ target: TelegramTarget; text: string }> = [];
  readonly attachments: Array<{
    target: TelegramTarget;
    kind: TelegramAttachmentKind;
    fileName: string;
    caption: string | null;
    text: string;
  }> = [];
  readonly actions: Array<{ target: TelegramTarget; action: string }> = [];
  readonly remoteFiles = new Map<string, { filePath: string; bytes: Uint8Array }>();

  async sendText(target: TelegramTarget, text: string): Promise<void> {
    this.sent.push({ target, text });
  }

  async sendAttachment(
    target: TelegramTarget,
    attachment: {
      kind: TelegramAttachmentKind;
      fileName: string;
      bytes: Uint8Array;
      caption?: string | null;
    }
  ): Promise<void> {
    this.attachments.push({
      target,
      kind: attachment.kind,
      fileName: attachment.fileName,
      caption: attachment.caption || null,
      text: Buffer.from(attachment.bytes).toString("utf8")
    });
  }

  async sendChatAction(target: TelegramTarget, action: string): Promise<void> {
    this.actions.push({ target, action });
  }

  registerRemoteFile(fileId: string, filePath: string, contents: string | Uint8Array): void {
    this.remoteFiles.set(fileId, {
      filePath,
      bytes: typeof contents === "string" ? new TextEncoder().encode(contents) : contents
    });
  }

  async getFile(fileId: string): Promise<{
    file_id: string;
    file_size: number;
    file_path: string;
  }> {
    const file = this.remoteFiles.get(fileId);
    if (!file) {
      throw new Error(`Missing fake Telegram file: ${fileId}`);
    }

    return {
      file_id: fileId,
      file_size: file.bytes.byteLength,
      file_path: file.filePath
    };
  }

  async downloadFile(filePath: string, maxBytes?: number): Promise<Uint8Array> {
    const file = [...this.remoteFiles.values()].find((entry) => entry.filePath === filePath);
    if (!file) {
      throw new Error(`Missing fake Telegram file path: ${filePath}`);
    }
    if (maxBytes !== undefined && file.bytes.byteLength > maxBytes) {
      throw new Error(`Fake Telegram file exceeds ${maxBytes} bytes`);
    }

    return file.bytes;
  }

  isConfigured(): boolean {
    return true;
  }
}

let nextMessageId = 1;
const TEST_ALLOWED_TELEGRAM_USER_ID = 123456789;

function telegramMessage(text: string, threadId: number, userId = TEST_ALLOWED_TELEGRAM_USER_ID): TelegramMessage {
  return {
    message_id: nextMessageId++,
    date: Math.floor(Date.now() / 1000),
    text,
    is_topic_message: true,
    message_thread_id: threadId,
    from: { id: userId, username: "tester" },
    chat: {
      id: 4242,
      type: "supergroup",
      title: "Factory"
    }
  };
}

function telegramPhotoMessage(
  caption: string,
  threadId: number,
  fileId: string,
  userId = TEST_ALLOWED_TELEGRAM_USER_ID
): TelegramMessage {
  return {
    message_id: nextMessageId++,
    date: Math.floor(Date.now() / 1000),
    caption,
    is_topic_message: true,
    message_thread_id: threadId,
    photo: [
      {
        file_id: `${fileId}-small`,
        file_unique_id: `${fileId}-small-uniq`,
        width: 320,
        height: 240,
        file_size: 10
      },
      {
        file_id: fileId,
        file_unique_id: `${fileId}-uniq`,
        width: 1440,
        height: 900,
        file_size: 18
      }
    ],
    from: { id: userId, username: "tester" },
    chat: {
      id: 4242,
      type: "supergroup",
      title: "Factory"
    }
  };
}

function telegramDocumentMessage(
  caption: string,
  threadId: number,
  fileId: string,
  fileName: string,
  mimeType = "text/plain",
  userId = TEST_ALLOWED_TELEGRAM_USER_ID
): TelegramMessage {
  return {
    message_id: nextMessageId++,
    date: Math.floor(Date.now() / 1000),
    caption,
    is_topic_message: true,
    message_thread_id: threadId,
    document: {
      file_id: fileId,
      file_unique_id: `${fileId}-uniq`,
      file_name: fileName,
      mime_type: mimeType,
      file_size: 64
    },
    from: { id: userId, username: "tester" },
    chat: {
      id: 4242,
      type: "supergroup",
      title: "Factory"
    }
  };
}

function telegramVoiceMessage(threadId: number, fileId: string, userId = TEST_ALLOWED_TELEGRAM_USER_ID): TelegramMessage {
  return {
    message_id: nextMessageId++,
    date: Math.floor(Date.now() / 1000),
    is_topic_message: true,
    message_thread_id: threadId,
    voice: {
      file_id: fileId,
      file_unique_id: `${fileId}-uniq`,
      duration: 4,
      mime_type: "audio/ogg",
      file_size: 32
    },
    from: { id: userId, username: "tester" },
    chat: {
      id: 4242,
      type: "supergroup",
      title: "Factory"
    }
  };
}

async function makeExecutable(path: string, contents: string): Promise<void> {
  await writeFile(path, contents);
  await chmod(path, 0o755);
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 5_000): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (await predicate()) {
      return;
    }

    await Bun.sleep(25);
  }

  throw new Error("Timed out waiting for condition");
}

function gitHasCommit(repoPath: string): boolean {
  const result = Bun.spawnSync(["git", "-C", repoPath, "rev-parse", "--verify", "HEAD"], {
    stdout: "ignore",
    stderr: "ignore"
  });

  return result.exitCode === 0;
}

async function createFixture(envOverrides: Record<string, string> = {}) {
  const root = await mkdtemp(join(tmpdir(), "factory-phase1-"));
  const binDir = join(root, "bin");
  const controlRoot = join(root, "telemux");
  const factoryRoot = join(root, "factory");
  const workersFile = join(root, "workers.json");
  const fakeCodex = join(binDir, "codex");
  const fakeClaude = join(binDir, "claude");
  const fakeSsh = join(binDir, "ssh");
  const fakeSudo = join(binDir, "sudo");

  await mkdir(binDir, { recursive: true });

  await makeExecutable(
    fakeCodex,
    `#!/usr/bin/env bash
set -euo pipefail
if (($# >= 2)) && [[ "$1" == "exec" && "$2" == "--help" ]]; then
  echo '--dangerously-bypass-approvals-and-sandbox --output-last-message --skip-git-repo-check'
  exit 0
fi
if (($# >= 1)) && [[ "$1" == "--version" ]]; then
  echo 'codex fake'
  exit 0
fi
mode="new"
session_id=""
output_file=""
images=()
model=""
reasoning=""
while (($#)); do
  case "$1" in
    exec)
      shift
      ;;
    resume)
      mode="resume"
      shift
      if (($#)) && [[ "$1" != -* ]]; then
        session_id="$1"
        shift
      fi
      ;;
    --output-last-message|-o)
      output_file="$2"
      shift 2
      ;;
    -m|--model)
      model="$2"
      shift 2
      ;;
    -c|--config)
      if [[ "$2" == model_reasoning_effort=* ]]; then
        reasoning="$2"
      fi
      shift 2
      ;;
    --image|-i)
      images+=("$2")
      shift 2
      ;;
    --json|--dangerously-bypass-approvals-and-sandbox|-)
      shift
      ;;
    *)
      shift
      ;;
  esac
done

prompt="$(cat)"
mkdir -p .factory
session_file=".factory/fake-session-id"
turn_file=".factory/fake-turn-count"
turns=0
if [[ -f "$turn_file" ]]; then
  turns="$(cat "$turn_file")"
fi
turns=$((turns + 1))
printf '%s' "$turns" > "$turn_file"

if [[ "$mode" == "resume" && -z "$session_id" && -f "$session_file" ]]; then
  session_id="$(cat "$session_file")"
fi

if [[ -z "$session_id" ]]; then
  session_id="session-$$-$turns-$(basename "$PWD")"
fi

printf '%s' "$session_id" > "$session_file"

if [[ "$prompt" == "/compact" ]]; then
  printf '# Summary\\n\\nCompacted thread.\\n' > .factory/SUMMARY.md
  printf '# TODO\\n\\n- Continue after compaction.\\n' > .factory/TODO.md
  printf '# Artifacts\\n' > .factory/ARTIFACTS.md
  cat > .factory/STATE.json <<EOF
{
  "sessionId": "$session_id",
  "turns": $turns
}
EOF
  if [[ -n "$output_file" ]]; then
    printf 'Compacted thread.' > "$output_file"
  fi
  printf '{"type":"thread.compaction.started"}\\n'
  printf '{"type":"turn.completed","usage":{"input_tokens":2,"cached_input_tokens":0,"output_tokens":1}}\\n'
  exit 0
fi

printf '# Summary\\n\\nTurn %s for %s.\\n\\nPrompt: %s\\n' "$turns" "$(basename "$PWD")" "$prompt" > .factory/SUMMARY.md
printf '%s' "$model" > .factory/fake-model.txt
printf '%s' "$reasoning" > .factory/fake-reasoning.txt
: > .factory/fake-images.txt
if ((\${#images[@]})); then
  printf 'Images:\\n' >> .factory/SUMMARY.md
  for image in "\${images[@]}"; do
    printf -- '- %s\\n' "$image" >> .factory/SUMMARY.md
    printf '%s\\n' "$image" >> .factory/fake-images.txt
  done
fi
printf '# TODO\\n\\n- Keep working from turn %s.\\n' "$turns" > .factory/TODO.md
cat > .factory/STATE.json <<EOF
{
  "sessionId": "$session_id",
  "turns": $turns
}
EOF

if printf '%s' "$prompt" | grep -q 'send-file'; then
  mkdir -p output
  artifact_file="output/attachment-turn-$turns.txt"
  printf 'attachment turn %s for %s' "$turns" "$(basename "$PWD")" > "$artifact_file"
  printf '# Artifacts\\n\\n- %s - generated attachment for turn %s\\n' "$artifact_file" "$turns" > .factory/ARTIFACTS.md
  cat > .factory/TELEGRAM_ATTACHMENTS.json <<EOF
{"attachments":[{"path":"$artifact_file","caption":"attachment turn $turns","type":"document"}]}
EOF
else
  printf '# Artifacts\\n\\n- artifact-turn-%s\\n' "$turns" > .factory/ARTIFACTS.md
fi

rm -f .factory/CRON_REQUESTS.json
if printf '%s' "$prompt" | grep -qi 'remind me to implement stripe every monday at 09:00'; then
  cat > .factory/CRON_REQUESTS.json <<'EOF'
{"actions":[{"type":"create","job":{"label":"stripe-reminder","kind":"reminder","schedule":{"type":"weekly","weekday":"monday","time":"09:00","timezone":"Europe/Zagreb"},"reminderText":"Reminder: implement Stripe."}}]}
EOF
fi
if printf '%s' "$prompt" | grep -qi 'change mode to fast for stripe cron'; then
  cat > .factory/CRON_REQUESTS.json <<'EOF'
{"actions":[{"type":"update","selector":{"label":"stripe-reminder"},"changes":{"modelOverride":"gpt-5.4-mini","reasoningEffortOverride":"low"}}]}
EOF
fi

if [[ -n "$output_file" ]]; then
  printf 'Reply turn %s for %s.' "$turns" "$(basename "$PWD")" > "$output_file"
fi

if [[ "$mode" == "resume" ]]; then
  printf '{"type":"session.resumed","session_id":"%s"}\\n' "$session_id"
else
  printf '{"type":"session.started","session_id":"%s"}\\n' "$session_id"
fi
if printf '%s' "$prompt" | grep -q 'slow live session'; then
  sleep 1
fi
if printf '%s' "$prompt" | grep -q 'emit codex json error'; then
  if printf '%s' "$prompt" | grep -q 'with stderr'; then
    printf 'non-json stderr diagnostic\\n' >&2
  fi
  cat <<EOF
{"type":"thread.started","thread_id":"$session_id"}
{"type":"turn.started"}
{"type":"error","message":"{\\"type\\":\\"error\\",\\"status\\":400,\\"error\\":{\\"type\\":\\"invalid_request_error\\",\\"message\\":\\"The '5.5' model is not supported when using Codex with a ChatGPT account.\\"}}"}
{"type":"turn.failed","error":{"message":"{\\"type\\":\\"error\\",\\"status\\":400,\\"error\\":{\\"type\\":\\"invalid_request_error\\",\\"message\\":\\"The '5.5' model is not supported when using Codex with a ChatGPT account.\\"}}"}}
EOF
  exit 1
fi
if printf '%s' "$prompt" | grep -q 'emit compact event'; then
  printf '{"type":"thread.compaction.started"}\\n'
fi
if printf '%s' "$prompt" | grep -q 'emit final compact event without newline'; then
  printf '{"type":"thread.compaction.started"}'
  exit 0
fi
printf '{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":0,"output_tokens":5}}\\n'
`
  );

  await makeExecutable(
    fakeSsh,
    `#!/usr/bin/env bash
if [[ -n "\${FACTORY_TEST_CAPTURE_SSH_ARGS:-}" ]]; then
  printf '%s\\n' "$*" > "$FACTORY_TEST_CAPTURE_SSH_ARGS"
fi
if [[ -n "\${FACTORY_TEST_CAPTURE_SSH_SCRIPT:-}" ]]; then
  cat > "$FACTORY_TEST_CAPTURE_SSH_SCRIPT"
  echo "ssh capture: No route to host" >&2
  exit 255
fi
if [[ "\${FACTORY_TEST_FAKE_SSH_EXEC:-}" == "1" ]]; then
  exec bash -s --
fi
echo "ssh: connect to host unreachable: No route to host" >&2
exit 255
`
  );

  await makeExecutable(
    fakeSudo,
    `#!/usr/bin/env bash
if [[ "\${FACTORY_TEST_FAKE_SUDO_FAIL:-}" == "1" ]]; then
  exit 1
fi
if [[ "\${1:-}" == "-n" && "\${2:-}" == "true" ]]; then
  exit 0
fi
exit 0
`
  );

  await makeExecutable(
    fakeClaude,
    `#!/usr/bin/env bash
set -euo pipefail
if (($# >= 1)) && [[ "$1" == "--help" ]]; then
  echo '--dangerously-skip-permissions --permission-mode --output-format'
  exit 0
fi
if (($# >= 1)) && [[ "$1" == "--version" ]]; then
  echo 'claude fake'
  exit 0
fi
while (($#)); do
  shift
done
prompt="$(cat)"
mkdir -p .factory
turn_file=".factory/fake-claude-turn-count"
turns=0
if [[ -f "$turn_file" ]]; then
  turns="$(cat "$turn_file")"
fi
turns=$((turns + 1))
printf '%s' "$turns" > "$turn_file"
printf '# Summary\\n\\nClaude turn %s for %s.\\n\\nPrompt: %s\\n' "$turns" "$(basename "$PWD")" "$prompt" > .factory/SUMMARY.md
printf '# Artifacts\\n\\n- claude-artifact-turn-%s\\n' "$turns" > .factory/ARTIFACTS.md
printf '# TODO\\n\\n- Continue with Claude from turn %s.\\n' "$turns" > .factory/TODO.md
cat > .factory/STATE.json <<EOF
{
  "sessionId": "claude-session-$turns",
  "turns": $turns
}
EOF
rm -f .factory/CRON_REQUESTS.json
printf 'Claude reply turn %s for %s.' "$turns" "$(basename "$PWD")"
`
  );

  await writeFile(
    workersFile,
    `${JSON.stringify(
      [
        {
          name: "control",
          transport: "local",
          managedRepoRoot: resolve(factoryRoot, "repos"),
          managedHostRoot: resolve(factoryRoot, "hostctx"),
          managedScratchRoot: resolve(factoryRoot, "scratch"),
          harness: envOverrides.TEST_CONTROL_WORKER_HARNESS || null,
          harnessBin: envOverrides.TEST_CONTROL_WORKER_HARNESS_BIN || null
        },
        {
          name: "worker1",
          transport: "ssh",
          sshTarget: envOverrides.TEST_WORKER1_SSH_TARGET || "worker1.tailnet",
          sshUser: "factory",
          managedRepoRoot: "/srv/factory/repos",
          managedHostRoot: "/srv/factory/hostctx",
          managedScratchRoot: "/srv/factory/scratch",
          harness: envOverrides.TEST_WORKER1_HARNESS || null,
          harnessBin: envOverrides.TEST_WORKER1_HARNESS_BIN || null
        }
      ],
      null,
      2
    )}\n`
  );

  const previousPath = process.env.PATH || "";
  process.env.PATH = `${binDir}:${previousPath}`;

  const selectedHarness = envOverrides.FACTORY_HARNESS || "codex";
  const config = loadConfig({
    ...process.env,
    FACTORY_CONTROL_ROOT: controlRoot,
    FACTORY_FACTORY_ROOT: factoryRoot,
    FACTORY_LOCAL_MACHINE: "control",
    FACTORY_WORKERS_FILE: workersFile,
    FACTORY_CODEX_BIN: fakeCodex,
    FACTORY_HARNESS: selectedHarness,
    FACTORY_HARNESS_BIN: envOverrides.FACTORY_HARNESS_BIN || (selectedHarness === "claude" ? fakeClaude : fakeCodex),
    FACTORY_TEXT_COALESCE_MS: envOverrides.FACTORY_TEXT_COALESCE_MS || "20",
    FACTORY_TELEGRAM_BOT_TOKEN: "test-token",
    FACTORY_ALLOWED_TELEGRAM_USER_ID: String(TEST_ALLOWED_TELEGRAM_USER_ID),
    ...envOverrides
  });

  ensureProjectPaths(config);

  const db = new FactoryDb(config.dbPath);
  const contexts = new ContextService(db, config.usageAdapter, config.contextsDir);
  const workers = new WorkerService(config, db);
  const telegram = new FakeTelegram();
  const cronManager = new CronManager(config, db, workers);
  const dispatcher = new Dispatcher(config, db, contexts, workers, telegram as never, cronManager);
  const cronScheduler = new CronScheduler(config, db, cronManager, dispatcher, workers, telegram as never);
  const commands = new CommandHandler(config, db, telegram as never, contexts, workers, dispatcher, cronManager, cronScheduler);

  return {
    root,
    controlRoot,
    factoryRoot,
    workersFile,
    fakeCodex,
    fakeClaude,
    fakeSsh,
    fakeSudo,
    previousPath,
    config,
    db,
    contexts,
    cronManager,
    cronScheduler,
    dispatcher,
    workers,
    telegram,
    commands
  };
}

test("telemux state database and runtime roots are private under permissive umask", async () => {
  const previousUmask = process.umask(0o000);
  let fixture: Awaited<ReturnType<typeof createFixture>> | null = null;
  try {
    fixture = await createFixture();
  } finally {
    process.umask(previousUmask);
  }

  try {
    expect(fixture).toBeTruthy();
    if (!fixture) {
      throw new Error("fixture setup failed");
    }
    const dbMode = (await stat(fixture.config.dbPath)).mode & 0o777;
    const stateMode = (await stat(fixture.config.stateRoot)).mode & 0o777;
    const controlMode = (await stat(fixture.config.controlRoot)).mode & 0o777;
    expect(dbMode & 0o077).toBe(0);
    expect(stateMode & 0o077).toBe(0);
    expect(controlMode & 0o077).toBe(0);
  } finally {
    process.env.PATH = fixture?.previousPath || process.env.PATH;
    process.umask(previousUmask);
    if (fixture) {
      await rm(fixture.root, { recursive: true, force: true });
    }
  }
});

test("phase 1 workflow covers local host/scratch and pending remote behavior", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/help", 10));
    const helpText = fixture.telegram.sent.at(-1)?.text || "";
    expect(helpText).toContain("A context is the durable Codex workspace and session binding for one Telegram topic.");
    expect(helpText).toContain("/newctx is usually run once per reusable Telegram topic");
    expect(helpText).toContain("/bind is for repointing the current topic");
    expect(helpText).not.toContain("/artifact_N");
    expect(helpText).not.toContain("/shred_N");
    expect(helpText).toContain("Old Telegram messages remain in Telegram");
    expect(helpText).toContain("/mode [fast|normal|max|clear]");
    expect(helpText).toContain("/model [model-id|clear]");
    expect(helpText).toContain("/effort [low|medium|high|xhigh|clear]");
    expect(helpText).toContain("/context");
    expect(helpText).toContain("/compact");
    expect(helpText).toContain("/crons");
    expect(helpText).toContain("/cron <subcommand>");

    await fixture.commands.handleMessage(telegramMessage("/whoami", 10));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Access: allowed");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Chat id: 4242");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Thread id: 10");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Bound context: none");

    await fixture.commands.handleMessage(telegramMessage("/newctx control-general control host", 10));
    const controlGeneral = fixture.db.getContextBySlug("control-general");
    expect(controlGeneral?.kind).toBe("host");
    expect(controlGeneral?.state).toBe("active");
    expect(controlGeneral?.transport).toBe("local");
    expect(await Bun.file(join(fixture.factoryRoot, "hostctx", "control-general", ".git", "HEAD")).exists()).toBe(true);
    expect(gitHasCommit(join(fixture.factoryRoot, "hostctx", "control-general"))).toBe(true);

    await fixture.commands.handleMessage(telegramMessage("/explainctx", 10));
    const explainText = fixture.telegram.sent.at(-1)?.text || "";
    expect(explainText).toContain("Machine: control");
    expect(explainText).toContain("Kind: host");
    expect(explainText).toContain("Transport: local");
    expect(explainText).toContain("Codex session exists: no");
    expect(explainText).toContain("If this topic is rebound:");
    expect(explainText).toContain("Old Telegram messages stay in Telegram");

    await fixture.commands.handleMessage(telegramMessage("Check free disk space and leave a note.", 10));
    await waitFor(() => Boolean(fixture.db.getContextBySlug("control-general")?.codexSessionId));
    const firstSession = fixture.db.getContextBySlug("control-general")?.codexSessionId || "";
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for control-general.")));
    expect(firstSession).not.toBe("");
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for control-general."))).toBe(true);
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("Dispatched resume for control-general."))).toBe(false);

    await fixture.commands.handleMessage(telegramMessage("Continue the same topic.", 10));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 2 for control-general.")));
    expect(fixture.db.getContextBySlug("control-general")?.codexSessionId).toBe(firstSession);

    await fixture.commands.handleMessage(telegramMessage("/newctx scratchpad control scratch", 20));
    const scratchpad = fixture.db.getContextBySlug("scratchpad");
    expect(scratchpad?.kind).toBe("scratch");
    expect(scratchpad?.state).toBe("active");
    expect(await Bun.file(join(fixture.factoryRoot, "scratch", "scratchpad", ".git", "HEAD")).exists()).toBe(true);
    expect(gitHasCommit(join(fixture.factoryRoot, "scratch", "scratchpad"))).toBe(true);

    await fixture.commands.handleMessage(telegramMessage("Do some scratch work.", 20));
    await waitFor(() => Boolean(fixture.db.getContextBySlug("scratchpad")?.codexSessionId));

    await fixture.commands.handleMessage(telegramMessage("/newctx rebound control scratch", 10));
    const reboundText = fixture.telegram.sent.at(-1)?.text || "";
    expect(reboundText).toContain("Warning: this topic is already bound.");
    expect(reboundText).toContain("Currently bound: control-general");
    expect(reboundText).toContain("Rebinding changes future routing for this Telegram topic.");
    expect(reboundText).toContain("The old workspace stays on disk");
    expect(reboundText).toContain("Old Telegram messages stay in Telegram");
    expect(fixture.db.getContextByTopic(4242, 10)?.slug).toBe("rebound");
    expect(await Bun.file(join(fixture.factoryRoot, "hostctx", "control-general", ".git", "HEAD")).exists()).toBe(true);

    await fixture.commands.handleMessage(telegramMessage("/newctx live-session control scratch", 50));
    await fixture.commands.handleMessage(telegramMessage("/run slow live session test", 50));
    await waitFor(() => Boolean(fixture.db.getContextBySlug("live-session")?.codexSessionId) && fixture.dispatcher.isActive("live-session"));
    const liveSessionId = fixture.db.getContextBySlug("live-session")?.codexSessionId || "";
    await fixture.commands.handleMessage(telegramMessage("/topicinfo", 50));
    const liveTopicInfo = fixture.telegram.sent.at(-1)?.text || "";
    expect(liveTopicInfo).toContain("Busy: yes");
    expect(liveTopicInfo).toContain(`Session: ${liveSessionId}`);
    await waitFor(() =>
      fixture.telegram.actions.some((entry) => entry.target.threadId === 50 && entry.action === "typing")
    );
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for live-session.")));

    await fixture.commands.handleMessage(telegramMessage("/newctx worker-general worker1 host", 30));
    const workerGeneral = fixture.db.getContextBySlug("worker-general");
    expect(workerGeneral?.kind).toBe("host");
    expect(workerGeneral?.state).toBe("pending");
    expect(workerGeneral?.lastError).toContain("No route to host");

    await fixture.commands.handleMessage(telegramMessage("/workers", 30));
    const workersText = fixture.telegram.sent.at(-1)?.text || "";
    expect(workersText).toContain("worker1");
    expect(workersText).toContain("status=unreachable");
    expect(workersText).toContain("transport=ssh");
    expect(workersText).toContain("local=no");
    expect(workersText).toContain("harness=codex");
    expect(workersText).toContain("model=default");
    expect(workersText).toContain("effort=default");
    expect(workersText).toContain("sudo=ok");

    await fixture.commands.handleMessage(
      telegramMessage("/newctx myproj worker1 https://example.com/acme/project.git", 40)
    );
    const workerRepo = fixture.db.getContextBySlug("myproj");
    expect(workerRepo?.kind).toBe("repo");
    expect(workerRepo?.state).toBe("pending");
    expect(workerRepo?.transport).toBe("ssh");
    expect(workerRepo?.rootPath).toBe("/srv/factory/repos/myproj");

    expect(await Bun.file(join(fixture.factoryRoot, "hostctx", "control-general", ".factory", "SUMMARY.md")).exists()).toBe(
      true
    );
    expect(await Bun.file(join(fixture.factoryRoot, "hostctx", "control-general", ".factory", "STATE.json")).exists()).toBe(
      true
    );
    expect(await Bun.file(join(fixture.factoryRoot, "scratch", "scratchpad", ".factory", "SUMMARY.md")).exists()).toBe(true);
    expect(await readFile(join(fixture.factoryRoot, "hostctx", "control-general", ".factory", "SUMMARY.md"), "utf8")).toContain(
      "Turn 2 for control-general."
    );

    await fixture.commands.handleMessage(telegramMessage("/run send-file", 10));
    await waitFor(() => fixture.telegram.attachments.some((entry) => entry.fileName === "attachment-turn-1.txt"));
    const sentAttachment = fixture.telegram.attachments.find((entry) => entry.fileName === "attachment-turn-1.txt");
    expect(sentAttachment?.target.threadId).toBe(10);
    expect(sentAttachment?.kind).toBe("document");
    expect(sentAttachment?.caption).toBe("attachment turn 1");
    expect(sentAttachment?.text).toContain("attachment turn 1 for rebound");

    await fixture.commands.handleMessage(telegramMessage("/artifacts send attachment-turn-1", 10));
    await waitFor(() => fixture.telegram.attachments.filter((entry) => entry.fileName === "attachment-turn-1.txt").length >= 2);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("addressed commands for other bots are ignored and updates probes are bounded", async () => {
  const fixture = await createFixture({ FACTORY_TELEGRAM_BOT_USERNAME: "brainstackbot" });
  const previousProbeTimeout = process.env.BRAINSTACK_UPDATE_PROBE_TIMEOUT_SECONDS;

  try {
    await fixture.commands.handleMessage(telegramMessage("/help@otherbot", 10));
    expect(fixture.telegram.sent).toHaveLength(0);

    await fixture.commands.handleMessage(telegramMessage("/newctx update-command control scratch", 10));
    await makeExecutable(
      fixture.fakeCodex,
      `#!/usr/bin/env bash
exec sleep 30
`
    );
    process.env.BRAINSTACK_UPDATE_PROBE_TIMEOUT_SECONDS = "1";
    const startedAt = Date.now();
    await fixture.commands.handleMessage(telegramMessage("/updates@brainstackbot", 10));
    expect(Date.now() - startedAt).toBeLessThan(7000);
    const updatesText = fixture.telegram.sent.at(-1)?.text || "";
    expect(updatesText).toContain("Update check degraded.");
    expect(updatesText).toContain("Artifact: .factory/reports/update-check-");
    expect(updatesText).toContain("## control");
    expect(updatesText).toContain("exit=124");
    const reportPath = updatesText.match(/Artifact: (.+)/)?.[1]?.trim() || "";
    const reportText = await readFile(join(fixture.factoryRoot, "scratch", "update-command", reportPath), "utf8");
    expect(reportText).toContain("## worker1");
  } finally {
    if (previousProbeTimeout === undefined) {
      delete process.env.BRAINSTACK_UPDATE_PROBE_TIMEOUT_SECONDS;
    } else {
      process.env.BRAINSTACK_UPDATE_PROBE_TIMEOUT_SECONDS = previousProbeTimeout;
    }
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("/updates prefers the routines context and rejects non-active report contexts", async () => {
  const fixture = await createFixture({
    FACTORY_TELEGRAM_CONTROL_CHAT_ID: "4242"
  });

  try {
    await ensureBasicLoops(fixture.config, fixture.contexts, fixture.workers, fixture.cronManager);
    await fixture.commands.handleMessage(telegramMessage("/newctx topic-updates control scratch", 89));

    const originalRunUpdateCheck = fixture.workers.runUpdateCheck.bind(fixture.workers);
    let usedContextSlug: string | null = null;
    fixture.workers.runUpdateCheck = async (context, _logPath) => {
      usedContextSlug = context.slug;
      return {
        ok: true,
        host: context.machine,
        transport: "stack",
        exitCode: 0,
        stdout: "BRAINSTACK_UPDATE_REPORT=.factory/reports/mock-update.md\n# Update Check\n\n- status: ok\n\n## control\n\nok",
        stderr: "",
        durationMs: 1,
        commandLabel: "update-check control",
        reportPath: ".factory/reports/mock-update.md"
      };
    };

    await fixture.commands.handleMessage(telegramMessage("/updates", 89));
    expect(usedContextSlug).toBe("brainstack-routines");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Update check complete.");
    fixture.workers.runUpdateCheck = originalRunUpdateCheck;

    const routines = fixture.db.getContextBySlug("brainstack-routines")!;
    fixture.db.saveContext({ ...routines, state: "archived", updatedAt: new Date().toISOString() });
    const bound = fixture.db.getContextBySlug("topic-updates")!;
    fixture.db.saveContext({ ...bound, state: "pending", updatedAt: new Date().toISOString() });

    await fixture.commands.handleMessage(telegramMessage("/updates", 89));
    const rejectedText = fixture.telegram.sent.at(-1)?.text || "";
    expect(rejectedText).toContain("No active context is available");
    expect(rejectedText).toContain("brainstack-routines:archived");
    expect(rejectedText).toContain("topic-updates:pending");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("cron jobs can be created from a normal Codex turn, tuned later, mirrored into the workspace, and dispatched by the scheduler", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx cronlab control scratch", 80));
    await fixture.commands.handleMessage(telegramMessage("Remind me to implement Stripe every Monday at 09:00 Europe/Zagreb.", 80));
    await waitFor(() => fixture.db.listCronJobs().length === 1);

    const created = fixture.db.listCronJobs()[0];
    expect(created?.label).toBe("stripe-reminder");
    expect(created?.kind).toBe("reminder");
    expect(created?.executionContextSlug).toBe("cronlab");
    expect(created?.targetThreadId).toBe(80);
    expect(created?.nextRunAt).not.toBeNull();

    const cronRoot = join(fixture.factoryRoot, "scratch", "cronlab");
    await waitFor(async () => {
      const cronsPath = join(cronRoot, ".factory", "CRONS.md");
      return (await Bun.file(cronsPath).exists()) && (await readFile(cronsPath, "utf8")).includes("stripe-reminder");
    });
    expect(await readFile(join(cronRoot, ".factory", "CRONS.md"), "utf8")).toContain("stripe-reminder");

    await fixture.commands.handleMessage(telegramMessage("/crons", 80));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("stripe-reminder");
    expect(fixture.telegram.sent.at(-1)?.text || "").toMatch(/\/cron_run_[a-z0-9]{6,10}_1/);
    expect(fixture.telegram.sent.at(-1)?.text).toContain("/cron_install_update_check");

    await fixture.commands.handleMessage(telegramMessage("Change mode to fast for stripe cron.", 80));
    await waitFor(() => fixture.db.listCronJobs()[0]?.modelOverride === "gpt-5.4-mini");
    expect(fixture.db.listCronJobs()[0]?.reasoningEffortOverride).toBe("low");
    await fixture.cronScheduler.runDueJobs("2026-04-06T00:00:00.000Z");

    const dueReminder = fixture.db.listCronJobs()[0];
    await fixture.cronManager.saveJob({
      ...dueReminder,
      nextRunAt: "2026-04-07T07:00:00.000Z",
      updatedAt: "2026-04-07T07:00:00.000Z"
    });

    await fixture.cronScheduler.runDueJobs("2026-04-08T07:00:00.000Z");
    await waitFor(() =>
      fixture.telegram.sent.some((entry) => entry.target.threadId === 80 && entry.text.includes("Reminder: implement Stripe."))
    );

    const refreshedReminder = fixture.db.getCronJob(dueReminder.id);
    expect(refreshedReminder?.lastRunAt).not.toBeNull();
    expect(refreshedReminder?.nextRunAt).not.toBeNull();

    const codexJob = await fixture.cronManager.createJob(
      {
        label: "email-cron",
        kind: "codex",
        schedule: {
          type: "interval",
          everyMinutes: 60,
          anchorAt: "2026-04-08T06:00:00.000Z"
        },
        executionContextSlug: "cronlab",
        targetChatId: 4242,
        targetThreadId: 80,
        instruction: "Check the inbox and summarize anything urgent.",
        modelOverride: "gpt-5.4-mini",
        reasoningEffortOverride: "low"
      },
      {
        context: fixture.db.getContextBySlug("cronlab"),
        target: { chatId: 4242, threadId: 80 }
      }
    );

    await fixture.cronManager.saveJob({
      ...codexJob,
      nextRunAt: "2026-04-08T07:00:00.000Z",
      updatedAt: "2026-04-08T07:00:00.000Z"
    });

    await fixture.cronScheduler.runDueJobs("2026-04-08T08:00:00.000Z");
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 3 for cronlab.")));

    expect(await readFile(join(cronRoot, ".factory", "fake-model.txt"), "utf8")).toBe("gpt-5.4-mini");
    expect(await readFile(join(cronRoot, ".factory", "fake-reasoning.txt"), "utf8")).toBe('model_reasoning_effort="low"');

    await fixture.commands.handleMessage(telegramMessage("/crons", 80));
    const cronsText = fixture.telegram.sent.at(-1)?.text || "";
    expect(cronsText).toContain("stripe-reminder");
    expect(cronsText).toContain("email-cron");
    expect(cronsText).toContain("/cron_show_");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 20_000);

test("dispatcher reserves context locks before asynchronous setup can race", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx lockrace control scratch", 82));
    const context = fixture.db.getContextBySlug("lockrace");
    expect(context).toBeTruthy();

    const first = await fixture.dispatcher.dispatch(
      "resume",
      context!,
      "slow live session from first dispatch",
      { chatId: 4242, threadId: 82 },
      { notifyAccepted: false }
    );
    const second = await fixture.dispatcher.dispatch(
      "resume",
      context!,
      "second dispatch should not start",
      { chatId: 4242, threadId: 82 },
      { notifyAccepted: false }
    );

    expect(first.accepted).toBe(true);
    expect(second.accepted).toBe(true);
    expect(second.message).toContain("queued this turn");
    await waitFor(() => !fixture.dispatcher.isActive("lockrace"));
    await waitFor(async () => (await readFile(join(context!.worktreePath, ".factory", "fake-turn-count"), "utf8")) === "2");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("dispatcher can reject busy turns instead of accepting volatile queue entries", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx lockreject control scratch", 87));
    const context = fixture.db.getContextBySlug("lockreject");
    expect(context).toBeTruthy();

    const first = await fixture.dispatcher.dispatch(
      "resume",
      context!,
      "slow live session from first dispatch",
      { chatId: 4242, threadId: 87 },
      { notifyAccepted: false }
    );
    const second = await fixture.dispatcher.dispatch(
      "resume",
      context!,
      "cron-style dispatch must not enter the in-memory queue",
      { chatId: 4242, threadId: 87 },
      { notifyAccepted: false, allowQueue: false }
    );

    expect(first.accepted).toBe(true);
    expect(second.accepted).toBe(false);
    expect(second.message).toContain("already has an active job");
    await waitFor(() => !fixture.dispatcher.isActive("lockreject"));
    await waitFor(async () => (await readFile(join(context!.worktreePath, ".factory", "fake-turn-count"), "utf8")) === "1");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("dispatcher drains queued turns after lock-only jobs finish", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx lockqueue control scratch", 86));
    const context = fixture.db.getContextBySlug("lockqueue");
    expect(context).toBeTruthy();

    const lockDone = fixture.dispatcher.withContextLock(context!, async () => {
      const queued = await fixture.dispatcher.dispatch(
        "resume",
        context!,
        "queued behind lock-only job",
        { chatId: 4242, threadId: 86 },
        { notifyAccepted: false }
      );
      expect(queued.accepted).toBe(true);
      expect(queued.message).toContain("queued this turn for after the current run");
      await Bun.sleep(100);
    });

    await lockDone;
    await waitFor(() => !fixture.dispatcher.isActive("lockqueue"));
    await waitFor(async () => (await readFile(join(context!.worktreePath, ".factory", "fake-turn-count"), "utf8")) === "1");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("dispatcher preserves Telegram user id on durable queued turns", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx queueuser control scratch", 83));
    const context = fixture.db.getContextBySlug("queueuser");
    expect(context).toBeTruthy();

    const lockDone = fixture.dispatcher.withContextLock(context!, async () => {
      const queued = await fixture.dispatcher.dispatch(
        "resume",
        context!,
        "queued with user metadata",
        { chatId: 4242, threadId: 83 },
        { notifyAccepted: false, userId: 987654 }
      );
      expect(queued.accepted).toBe(true);
      const claimed = fixture.db.claimNextQueuedTurn("queueuser");
      expect(claimed?.userId).toBe(987654);
      fixture.db.finishQueuedTurn(claimed!.id, "skipped", "metadata assertion complete");
    });

    await lockDone;
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("dispatcher rehydrates queued turn user id from the durable column when options are sparse", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx replayuser control scratch", 86));
    const context = fixture.db.getContextBySlug("replayuser");
    expect(context).toBeTruthy();
    fixture.db.enqueueQueuedTurn({
      contextSlug: "replayuser",
      mode: "resume",
      instruction: "queued sparse replay",
      chatId: 4242,
      threadId: 86,
      userId: 24680,
      optionsJson: "{}"
    });

    let seenUserId: number | null | undefined;
    const dispatcherInternals = fixture.dispatcher as unknown as {
      dispatch: typeof fixture.dispatcher.dispatch;
      startNextQueuedTurn: (slug: string) => void;
    };
    const originalDispatch = dispatcherInternals.dispatch.bind(fixture.dispatcher);
    dispatcherInternals.dispatch = async (_mode, _context, _instruction, _target, options) => {
      seenUserId = options?.userId;
      return { accepted: false, message: null };
    };

    dispatcherInternals.startNextQueuedTurn("replayuser");
    await waitFor(() => seenUserId === 24680);
    dispatcherInternals.dispatch = originalDispatch;
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("dispatcher skips queued turns when Telegram topic binding changes before drain", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx bindingqueue control scratch", 84));
    const context = fixture.db.getContextBySlug("bindingqueue");
    expect(context).toBeTruthy();

    const first = await fixture.dispatcher.dispatch(
      "resume",
      context!,
      "slow live session from first dispatch",
      { chatId: 4242, threadId: 84 },
      { notifyAccepted: false }
    );
    expect(first.accepted).toBe(true);
    const queued = await fixture.dispatcher.dispatch(
      "resume",
      context!,
      "queued turn should be skipped after detach",
      { chatId: 4242, threadId: 84 },
      { notifyAccepted: false }
    );
    expect(queued.accepted).toBe(true);
    await fixture.commands.handleMessage(telegramMessage("/detach", 84));
    await waitFor(() => !fixture.dispatcher.isActive("bindingqueue"));
    await waitFor(async () => (await readFile(join(context!.worktreePath, ".factory", "fake-turn-count"), "utf8")) === "1");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("dispatcher reports running queued turns abandoned after restart", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx recoverqueue control scratch", 85));
    const context = fixture.db.getContextBySlug("recoverqueue");
    expect(context).toBeTruthy();
    const queued = fixture.db.enqueueQueuedTurn({
      contextSlug: "recoverqueue",
      mode: "resume",
      instruction: "queued before restart",
      chatId: 4242,
      threadId: 85,
      userId: TEST_ALLOWED_TELEGRAM_USER_ID,
      optionsJson: "{}"
    });
    const claimed = fixture.db.claimNextQueuedTurn("recoverqueue");
    expect(claimed?.id).toBe(queued.id);

    fixture.dispatcher.recoverQueuedTurns();

    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("interrupted by a telemux restart")));
    expect(fixture.db.getQueuedTurn(queued.id)?.status).toBe("abandoned");
    const counts = fixture.db.queuedTurnStatusCounts();
    expect(counts.abandoned).toBeGreaterThanOrEqual(1);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("archiving an active context prevents completion side effects from reactivating it", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx archive-race control scratch", 83));
    await fixture.commands.handleMessage(telegramMessage("slow live session", 83));
    await fixture.commands.handleMessage(telegramMessage("/archive", 83));
    await waitFor(() => !fixture.dispatcher.isActive("archive-race"));
    expect(fixture.db.getContextBySlug("archive-race")?.state).toBe("archived");
    expect(fixture.db.getContextBySlug("archive-race")?.telegramThreadId).toBeNull();
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("archived contexts stop pre-worker dispatch and pause linked cron slots", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx archive-preworker control scratch", 85));
    const context = fixture.db.getContextBySlug("archive-preworker");
    expect(context?.worktreePath).toBeTruthy();

    fixture.telegram.registerRemoteFile("archive-photo", "photos/archive.jpg", "archive image");
    const originalDownloadFile = fixture.telegram.downloadFile.bind(fixture.telegram);
    fixture.telegram.downloadFile = async (...args) => {
      const current = fixture.db.getContextBySlug("archive-preworker");
      fixture.contexts.saveContext({
        ...current!,
        state: "archived",
        telegramChatId: null,
        telegramThreadId: null
      });
      return originalDownloadFile(...args);
    };
    await fixture.commands.handleMessage(telegramPhotoMessage("Archive during prep.", 85, "archive-photo"));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("was archived before the worker started")));
    fixture.telegram.downloadFile = originalDownloadFile;
    expect(await Bun.file(join(context!.worktreePath, ".factory", "fake-turn-count")).exists()).toBe(false);
    expect(fixture.db.getContextBySlug("archive-preworker")?.state).toBe("archived");
    const guardedRun = await fixture.workers.runCodex(
      context!,
      "This should not reach the harness.",
      "run",
      join(fixture.controlRoot, "logs", "archive-preworker.log")
    );
    expect(guardedRun.ok).toBe(false);
    expect(guardedRun.exitCode).toBe(89);
    expect(guardedRun.stderr).toContain("context archived before harness launch");
    expect(await Bun.file(join(context!.worktreePath, ".factory", "fake-turn-count")).exists()).toBe(false);
    const guardedUpdateCheck = await fixture.workers.runUpdateCheck(context!);
    expect(guardedUpdateCheck.ok).toBe(false);
    expect(guardedUpdateCheck.exitCode).toBe(89);
    expect(guardedUpdateCheck.stderr).toContain("context archived before update-check launch");

    const codex = await fixture.cronManager.createJob(
      {
        label: "paused-archived-codex",
        kind: "codex",
        schedule: { type: "once", at: "2999-04-01T07:00:00.000Z" },
        executionContextSlug: "archive-preworker",
        targetChatId: 4242,
        targetThreadId: 85,
        instruction: "Should not run."
      },
      { context: fixture.db.getContextBySlug("archive-preworker"), target: { chatId: 4242, threadId: 85 } }
    );
    await fixture.cronScheduler.runDueJobs("2999-04-01T08:00:00.000Z");
    const pausedCodex = fixture.db.getCronJob(codex.id);
    expect(pausedCodex?.enabled).toBe(false);
    expect(pausedCodex?.pendingRunAt).toBe("2999-04-01T07:00:00.000Z");
    expect(pausedCodex?.lastError).toContain("is archived");

    const updateCheck = await fixture.cronManager.createJob(
      {
        label: "paused-archived-update-check",
        kind: "codex",
        runner: "deterministic-update-check",
        schedule: { type: "once", at: "2999-04-02T07:00:00.000Z" },
        executionContextSlug: "archive-preworker",
        targetChatId: 4242,
        targetThreadId: 85,
        instruction: "Run update check."
      },
      { context: fixture.db.getContextBySlug("archive-preworker"), target: { chatId: 4242, threadId: 85 } }
    );
    await fixture.cronScheduler.runDueJobs("2999-04-02T08:00:00.000Z");
    const pausedUpdateCheck = fixture.db.getCronJob(updateCheck.id);
    expect(pausedUpdateCheck?.enabled).toBe(false);
    expect(pausedUpdateCheck?.pendingRunAt).toBe("2999-04-02T07:00:00.000Z");
    expect(pausedUpdateCheck?.lastError).toContain("is archived");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("worker execution keeps full logs but bounds in-memory stdout and stderr", async () => {
  const fixture = await createFixture({ FACTORY_WORKER_CAPTURE_MAX_BYTES: "256" });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx noisy control scratch", 91));
    const context = fixture.db.getContextBySlug("noisy");
    expect(context).toBeTruthy();

    await makeExecutable(
      fixture.fakeCodex,
      `#!/usr/bin/env bash
	set -euo pipefail
	cat >/dev/null
	printf '{"type":"session.started","session_'
	sleep 0.1
	printf 'id":"split-session-noisy"}'
	printf 'stdout-start\\n'
	for _ in $(seq 1 400); do printf '0123456789'; done
	printf '\\nstdout-end\\n'
printf 'stderr-start\\n' >&2
for _ in $(seq 1 400); do printf 'abcdefghij'; done >&2
printf '\\nstderr-end\\n' >&2
`
    );

    const logPath = join(fixture.controlRoot, "logs", "noisy.log");
    const result = await fixture.workers.runCodex(context!, "Produce noisy output.", "run", logPath);

    expect(result.ok).toBe(true);
    expect(result.sessionId).toBe("split-session-noisy");
    expect(Buffer.byteLength(result.stdout, "utf8")).toBeLessThanOrEqual(256);
    expect(Buffer.byteLength(result.stderr, "utf8")).toBeLessThanOrEqual(256);
    expect(result.stdout).toContain("output truncated");
    expect(result.stdout).toContain("stdout-end");
    expect(result.stderr).toContain("output truncated");
    expect(result.stderr).toContain("stderr-end");

    const fullLog = await readFile(logPath, "utf8");
    expect(fullLog).toContain("stdout-start");
    expect(fullLog).toContain("stderr-start");
    expect(fullLog.length).toBeGreaterThan(4000);

    await mkdir(join(context!.worktreePath, "output"), { recursive: true });
    await writeFile(join(context!.worktreePath, "output", "large-artifact.txt"), "artifact-body-".repeat(100));
    const artifact = await fixture.workers.readArtifactFile(context!, "output/large-artifact.txt", 4096);
    expect(artifact.fileName).toBe("large-artifact.txt");
    expect(Buffer.from(artifact.content).toString("utf8")).toContain("artifact-body-artifact-body");

    const biggerContent = "artifact-large-body\n".repeat(18_000);
    await writeFile(join(context!.worktreePath, "output", "capture-cap-bypass.txt"), biggerContent);
    const biggerArtifact = await fixture.workers.readArtifactFile(context!, "output/capture-cap-bypass.txt", 512 * 1024);
    expect(biggerArtifact.fileName).toBe("capture-cap-bypass.txt");
    expect(Buffer.from(biggerArtifact.content).toString("utf8")).toBe(biggerContent);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("cron outcome saves do not resurrect jobs deleted after claim", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx claim-races control scratch", 84));
    const reminder = await fixture.cronManager.createJob(
      {
        label: "delete-reminder-after-claim",
        kind: "reminder",
        schedule: { type: "once", at: "2999-03-01T07:00:00.000Z" },
        executionContextSlug: "claim-races",
        targetChatId: 4242,
        targetThreadId: 84,
        reminderText: "Delete me during send."
      },
      { context: fixture.db.getContextBySlug("claim-races"), target: { chatId: 4242, threadId: 84 } }
    );
    const originalSendText = fixture.telegram.sendText.bind(fixture.telegram);
    fixture.telegram.sendText = async (target, text) => {
      if (text === "Delete me during send.") {
        fixture.db.deleteCronJob(reminder.id);
      }
      await originalSendText(target, text);
    };
    await fixture.cronScheduler.runDueJobs("2999-03-01T08:00:00.000Z");
    fixture.telegram.sendText = originalSendText;
    expect(fixture.db.getCronJob(reminder.id)).toBeNull();

    const codex = await fixture.cronManager.createJob(
      {
        label: "delete-codex-after-claim",
        kind: "codex",
        schedule: { type: "once", at: "2999-03-02T07:00:00.000Z" },
        executionContextSlug: "claim-races",
        targetChatId: 4242,
        targetThreadId: 84,
        instruction: "Delete me during dispatch."
      },
      { context: fixture.db.getContextBySlug("claim-races"), target: { chatId: 4242, threadId: 84 } }
    );
    const originalDispatch = fixture.dispatcher.dispatch.bind(fixture.dispatcher);
    fixture.dispatcher.dispatch = async () => {
      fixture.db.deleteCronJob(codex.id);
      return { accepted: true, message: "" };
    };
    await fixture.cronScheduler.runDueJobs("2999-03-02T08:00:00.000Z");
    fixture.dispatcher.dispatch = originalDispatch;
    expect(fixture.db.getCronJob(codex.id)).toBeNull();

  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("scheduler startup recovers unfinished claimed cron slots for operator review", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx recover-claims control scratch", 86));
    const recover = await fixture.cronManager.createJob(
      {
        label: "recover-claimed-slot",
        kind: "reminder",
        schedule: { type: "once", at: "2999-03-03T07:00:00.000Z" },
        executionContextSlug: "recover-claims",
        targetChatId: 4242,
        targetThreadId: 86,
        reminderText: "Recover me."
      },
      { context: fixture.db.getContextBySlug("recover-claims"), target: { chatId: 4242, threadId: 86 } }
    );
    const scheduledFor = "2999-03-03T07:00:00.000Z";
    await fixture.cronManager.saveJob({
      ...recover,
      enabled: false,
      nextRunAt: null,
      pendingRunAt: null,
      lastRunAt: scheduledFor,
      lastScheduledFor: scheduledFor,
      updatedAt: "2999-03-03T07:01:00.000Z"
    });
    fixture.db.saveCronRun({
      id: `cron-claim-${recover.id}-${scheduledFor.replace(/[^0-9]/g, "")}`,
      jobId: recover.id,
      scheduledFor,
      startedAt: "2999-03-03T07:01:00.000Z",
      finishedAt: null,
      status: "claimed",
      note: "Claimed before simulated crash"
    });

    await fixture.cronScheduler.runDueJobs("2999-03-03T08:00:00.000Z");
    const recovered = fixture.db.getCronJob(recover.id);
    expect(recovered?.enabled).toBe(false);
    expect(recovered?.pendingRunAt).toBe(scheduledFor);
    expect(recovered?.lastError).toContain("Recovered unfinished claimed run");
    expect(fixture.db.listCronRuns(recover.id).find((run) => run.id.startsWith("cron-claim-"))?.status).toBe("failed");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("deterministic cron commands install built-ins and run jobs immediately", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx routines control scratch", 81));

    await fixture.commands.handleMessage(
      telegramMessage("/cron create codex too-fast interval 1 Sweep the repo too often.", 81)
    );
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Codex interval jobs must run at least every 15 minutes");

    await fixture.commands.handleMessage(
      telegramMessage("/cron create reminder water daily 09:00 Europe/Zagreb Drink water.", 81)
    );
    let water = fixture.db.listCronJobs().find((job) => job.label === "water");
    expect(water?.kind).toBe("reminder");
    expect(water?.reminderText).toBe("Drink water.");

    await fixture.commands.handleMessage(telegramMessage("/cron_run water", 81));
    expect(fixture.telegram.sent.at(-2)?.text).toBe("Drink water.");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Cron run sent:");

    await fixture.cronManager.saveJob({
      ...water!,
      nextRunAt: "2999-01-01T09:00:00.000Z",
      pendingRunAt: null,
      updatedAt: "2999-01-01T09:00:00.000Z"
    });
    const dueWater = fixture.db.getCronJob(water!.id)!;
    const drinkCountBefore = fixture.telegram.sent.filter((entry) => entry.text === "Drink water.").length;
    await fixture.cronScheduler.runJobNow(dueWater, "2999-01-02T10:00:00.000Z");
    const manuallyClaimedWater = fixture.db.getCronJob(water!.id)!;
    expect(manuallyClaimedWater.pendingRunAt).toBeNull();
    expect(manuallyClaimedWater.nextRunAt && manuallyClaimedWater.nextRunAt > "2999-01-02T10:00:00.000Z").toBe(true);
    await fixture.cronScheduler.runDueJobs("2999-01-02T10:00:00.000Z");
    expect(fixture.telegram.sent.filter((entry) => entry.text === "Drink water.")).toHaveLength(drinkCountBefore + 1);

    await fixture.commands.handleMessage(
      telegramMessage("/cron create reminder race once 2999-01-03T09:00:00.000Z Race reminder.", 81)
    );
    const race = fixture.db.listCronJobs().find((job) => job.label === "race")!;
    const originalSendText = fixture.telegram.sendText.bind(fixture.telegram);
    let racedScheduler = false;
    fixture.telegram.sendText = async (target, text) => {
      if (text === "Race reminder." && !racedScheduler) {
        racedScheduler = true;
        const schedulerRun = fixture.cronScheduler.runDueJobs("2999-01-03T10:00:00.000Z");
        await Bun.sleep(10);
        await originalSendText(target, text);
        await schedulerRun;
        return;
      }
      await originalSendText(target, text);
    };
    await fixture.cronScheduler.runJobNow(race, "2999-01-03T10:00:00.000Z");
    fixture.telegram.sendText = originalSendText;
    expect(fixture.telegram.sent.filter((entry) => entry.text === "Race reminder.")).toHaveLength(1);

    const legacyUpdateCheck = await fixture.cronManager.createJob(
      {
        label: "update-check",
        kind: "codex",
        schedule: {
          type: "interval",
          everyMinutes: 60,
          anchorAt: "2026-04-08T06:00:00.000Z"
        },
        executionContextSlug: "routines",
        targetChatId: 4242,
        targetThreadId: 81,
        instruction: "Legacy yolo update check.",
        enabled: false
      },
      {
        context: fixture.db.getContextBySlug("routines"),
        target: { chatId: 4242, threadId: 81 }
      }
    );
    expect(legacyUpdateCheck.runner).toBeNull();
    await fixture.commands.handleMessage(telegramMessage("/cron install update-check daily 08:00 Europe/Zagreb", 81));
    const updateCheck = fixture.db.getCronJob(legacyUpdateCheck.id);
    expect(updateCheck?.kind).toBe("codex");
    expect(updateCheck?.runner).toBe("deterministic-update-check");
    expect(updateCheck?.executionContextSlug).toBe("routines");
    expect(updateCheck?.instruction).toContain("deterministic built-in update check");
    expect(updateCheck?.instruction).toContain("not by an LLM harness");

    await fixture.cronManager.updateJob(updateCheck!, { label: "weekly-updates" });
    await fixture.commands.handleMessage(telegramMessage("/cron_run weekly-updates", 81));
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("Update check degraded."))).toBe(true);
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("## worker1"))).toBe(true);
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("- status: degraded"))).toBe(true);
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Cron run completed:");
    const routinesRoot = join(fixture.factoryRoot, "scratch", "routines");
    expect(await Bun.file(join(routinesRoot, ".factory", "fake-codex-turn-count")).exists()).toBe(false);
    expect(await readFile(join(routinesRoot, ".factory", "ARTIFACTS.md"), "utf8")).toContain(
      ".factory/reports/update-check-"
    );

    const customUpdateCheck = await fixture.cronManager.createJob(
      {
        label: "update-check",
        kind: "codex",
        schedule: {
          type: "interval",
          everyMinutes: 60,
          anchorAt: "2026-04-08T06:00:00.000Z"
        },
        executionContextSlug: "routines",
        targetChatId: 4242,
        targetThreadId: 81,
        instruction: "Run a custom update-check-labeled Codex job."
      },
      {
        context: fixture.db.getContextBySlug("routines"),
        target: { chatId: 4242, threadId: 81 }
      }
    );
    expect(customUpdateCheck.runner).toBeNull();
    let customUsedDeterministicRunner = false;
    const originalRunUpdateCheck = fixture.workers.runUpdateCheck.bind(fixture.workers);
    fixture.workers.runUpdateCheck = async (...args) => {
      customUsedDeterministicRunner = true;
      return originalRunUpdateCheck(...args);
    };
    const customRunMessageStart = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage(`/cron_run ${customUpdateCheck.id}`, 81));
    expect(fixture.telegram.sent.slice(customRunMessageStart).some((entry) => entry.text.includes("Cron run dispatched:"))).toBe(true);
    expect(customUsedDeterministicRunner).toBe(false);
    await waitFor(() => !fixture.dispatcher.isActive("routines"));
    fixture.workers.runUpdateCheck = originalRunUpdateCheck;

    let archivedDuringClaim = false;
    let staleContextRan = false;
    const originalSaveJob = fixture.cronManager.saveJob.bind(fixture.cronManager);
    fixture.cronManager.saveJob = async (job) => {
      const saved = await originalSaveJob(job);
      if (
        saved.id === updateCheck!.id &&
        saved.lastResult?.startsWith("Deterministic update check started") &&
        !archivedDuringClaim
      ) {
        archivedDuringClaim = true;
        fixture.contexts.updateState(fixture.db.getContextBySlug("routines")!, "archived");
      }
      return saved;
    };
    fixture.workers.runUpdateCheck = async (...args) => {
      staleContextRan = true;
      return originalRunUpdateCheck(...args);
    };
    await fixture.commands.handleMessage(telegramMessage("/cron_run weekly-updates", 81));
    expect(staleContextRan).toBe(false);
    expect(fixture.telegram.sent.at(-1)?.text).toContain("context routines is no longer active");
    fixture.cronManager.saveJob = originalSaveJob;
    fixture.workers.runUpdateCheck = originalRunUpdateCheck;
    fixture.contexts.updateState(fixture.db.getContextBySlug("routines")!, "active");
    await fixture.cronManager.pauseJob(fixture.db.getCronJob(updateCheck!.id)!);
    await fixture.cronManager.pauseJob(fixture.db.getCronJob(customUpdateCheck.id)!);

    const staleFastJob = fixture.db.saveCronJob({
      id: "legacy-fast-codex",
      label: "legacy-fast",
      kind: "codex",
      runner: null,
      enabled: false,
      schedule: {
        type: "interval",
        everyMinutes: 1,
        anchorAt: "2026-04-08T06:00:00.000Z"
      },
      nextRunAt: null,
      pendingRunAt: null,
      lastRunAt: null,
      lastScheduledFor: null,
      executionContextSlug: "routines",
      targetChatId: 4242,
      targetThreadId: 81,
      instruction: "Legacy high-frequency job.",
      reminderText: null,
      modelOverride: null,
      reasoningEffortOverride: null,
      lastResult: null,
      lastError: null,
      createdAt: "2026-04-08T06:00:00.000Z",
      updatedAt: "2026-04-08T06:00:00.000Z"
    });
    await expect(fixture.cronManager.resumeJob(staleFastJob)).rejects.toThrow("Codex interval jobs must run at least every 15 minutes");

    const postClaimBusyCodex = await fixture.cronManager.createJob(
      {
        label: "post-claim-busy-codex",
        kind: "codex",
        schedule: {
          type: "once",
          at: "2999-01-31T07:00:00.000Z"
        },
        executionContextSlug: "routines",
        targetChatId: 4242,
        targetThreadId: 81,
        instruction: "Race normal dispatch."
      },
      {
        context: fixture.db.getContextBySlug("routines"),
        target: { chatId: 4242, threadId: 81 }
      }
    );
    const originalDispatch = fixture.dispatcher.dispatch.bind(fixture.dispatcher);
    fixture.dispatcher.dispatch = async () => ({
      accepted: false,
      message: "routines already has an active job. Use /topicinfo or /tail."
    });
    await fixture.cronScheduler.runDueJobs("2999-01-31T08:00:00.000Z");
    fixture.dispatcher.dispatch = originalDispatch;
    const queuedCodex = fixture.db.getCronJob(postClaimBusyCodex.id);
    expect(queuedCodex?.enabled).toBe(true);
    expect(queuedCodex?.pendingRunAt).toBe("2999-01-31T07:00:00.000Z");
    expect(fixture.db.listCronRuns(postClaimBusyCodex.id)[0]?.status).toBe("queued");
    await fixture.cronManager.pauseJob(queuedCodex!);

    const raceUpdateCheck = await fixture.cronManager.createJob(
      {
        label: "update-check-race",
        kind: "codex",
        runner: "deterministic-update-check",
        schedule: {
          type: "once",
          at: "2999-02-01T07:00:00.000Z"
        },
        executionContextSlug: "routines",
        targetChatId: 4242,
        targetThreadId: 81,
        instruction: "Race update check."
      },
      {
        context: fixture.db.getContextBySlug("routines"),
        target: { chatId: 4242, threadId: 81 }
      }
    );
    fixture.workers.runUpdateCheck = async (...args) => {
      fixture.db.deleteCronJob(raceUpdateCheck.id);
      return originalRunUpdateCheck(...args);
    };
    await fixture.cronScheduler.runDueJobs("2999-02-01T08:00:00.000Z");
    expect(fixture.db.getCronJob(raceUpdateCheck.id)).toBeNull();
    expect(fixture.db.listCronRuns(raceUpdateCheck.id)).toEqual([]);
    fixture.workers.runUpdateCheck = originalRunUpdateCheck;

    const busyUpdateCheck = await fixture.cronManager.createJob(
      {
        label: "update-check-busy-after-claim",
        kind: "codex",
        runner: "deterministic-update-check",
        schedule: {
          type: "once",
          at: "2999-02-01T09:00:00.000Z"
        },
        executionContextSlug: "routines",
        targetChatId: 4242,
        targetThreadId: 81,
        instruction: "Busy update check."
      },
      {
        context: fixture.db.getContextBySlug("routines"),
        target: { chatId: 4242, threadId: 81 }
      }
    );
    const originalWithContextLock = fixture.dispatcher.withContextLock.bind(fixture.dispatcher);
    fixture.dispatcher.withContextLock = async () => {
      throw new Error("routines already has an active job. Use /topicinfo or /tail.");
    };
    await fixture.cronScheduler.runDueJobs("2999-02-01T10:00:00.000Z");
    fixture.dispatcher.withContextLock = originalWithContextLock;
    const queuedUpdateCheck = fixture.db.getCronJob(busyUpdateCheck.id);
    expect(queuedUpdateCheck?.enabled).toBe(true);
    expect(queuedUpdateCheck?.pendingRunAt).toBe("2999-02-01T09:00:00.000Z");
    expect(fixture.db.listCronRuns(busyUpdateCheck.id)[0]?.status).toBe("queued");
    await fixture.cronManager.pauseJob(queuedUpdateCheck!);

    const failingUpdateCheck = await fixture.cronManager.createJob(
      {
        label: "update-check-fails",
        kind: "codex",
        runner: "deterministic-update-check",
        schedule: {
          type: "once",
          at: "2999-02-02T07:00:00.000Z"
        },
        executionContextSlug: "routines",
        targetChatId: 4242,
        targetThreadId: 81,
        instruction: "Failing update check."
      },
      {
        context: fixture.db.getContextBySlug("routines"),
        target: { chatId: 4242, threadId: 81 }
      }
    );
    await fixture.commands.handleMessage(
      telegramMessage("/cron create reminder after-failure once 2999-02-02T07:00:00.000Z After failure.", 81)
    );
    fixture.workers.runUpdateCheck = async () => {
      throw new Error("synthetic update-check failure");
    };
    await fixture.cronScheduler.runDueJobs("2999-02-02T08:00:00.000Z");
    expect(fixture.db.getCronJob(failingUpdateCheck.id)?.lastError).toContain("synthetic update-check failure");
    expect(fixture.telegram.sent.some((entry) => entry.text === "After failure.")).toBe(true);
    fixture.workers.runUpdateCheck = originalRunUpdateCheck;

    await fixture.commands.handleMessage(telegramMessage("/cron_install_daily_checkin", 81));
    const checkin = fixture.db.listCronJobs().find((job) => job.label === "daily-checkin");
    expect(checkin?.kind).toBe("reminder");
    expect(checkin?.reminderText).toContain("Daily check-in.");

    await fixture.commands.handleMessage(telegramMessage("/cron install brain-curator daily 06:30 Europe/Zagreb", 81));
    const curator = fixture.db.listCronJobs().find((job) => job.label === "brain-curator");
    expect(curator?.kind).toBe("codex");
    expect(curator?.instruction).toContain("shared-brain curator pass");
    expect(curator?.instruction).toContain("Preserve raw imports and proposals");

    await fixture.commands.handleMessage(telegramMessage("/crons", 81));
    const overview = fixture.telegram.sent.at(-1)?.text || "";
    expect(overview).toContain("/cron_install_brain_curator");
    expect(overview).toMatch(/\/cron_run_[a-z0-9]{6,10}_\d+/);
    expect(overview).toContain("daily-checkin");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("basic loops bootstrap installs update-check and workers reload without restart", async () => {
  const fixture = await createFixture({
    FACTORY_TELEGRAM_CONTROL_CHAT_ID: "4242"
  });

  try {
    const result = await ensureBasicLoops(fixture.config, fixture.contexts, fixture.workers, fixture.cronManager);
    expect(result).toContain("created:");
    const updateCheck = fixture.db.listCronJobs().find((job) => job.label === "update-check");
    expect(updateCheck?.runner).toBe("deterministic-update-check");
    expect(updateCheck?.targetChatId).toBe(4242);
    expect(updateCheck?.executionContextSlug).toBe("brainstack-routines");

    await fixture.cronScheduler.runJobNow(updateCheck!);
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("Update check degraded."))).toBe(true);
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("- status: degraded"))).toBe(true);

    const workers = JSON.parse(await readFile(fixture.workersFile, "utf8")) as Array<Record<string, unknown>>;
    workers.push({
      name: "worker2",
      transport: "ssh",
      sshTarget: "worker2.tailnet",
      sshUser: "factory",
      managedRepoRoot: "/srv/factory/repos",
      managedHostRoot: "/srv/factory/hostctx",
      managedScratchRoot: "/srv/factory/scratch"
    });
    await writeFile(fixture.workersFile, `${JSON.stringify(workers, null, 2)}\n`);

    await fixture.commands.handleMessage(telegramMessage("/workers", 81));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("worker2");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("sudo=ok");

    await fixture.cronScheduler.runJobNow(fixture.db.getCronJob(updateCheck!.id)!);
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("## worker2"))).toBe(true);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("basic loops do not retarget user-owned update-check jobs", async () => {
  const fixture = await createFixture({
    FACTORY_TELEGRAM_CONTROL_CHAT_ID: "4242"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx user-updates control scratch", 86));
    const userContext = fixture.db.getContextBySlug("user-updates");
    const userJob = await fixture.cronManager.createJob(
      {
        label: "update-check",
        kind: "codex",
        runner: "deterministic-update-check",
        schedule: {
          type: "daily",
          time: "09:30",
          timezone: "Europe/Zagreb"
        },
        executionContextSlug: "user-updates",
        targetChatId: 4242,
        targetThreadId: 86,
        instruction: "User-scoped update check."
      },
      { context: userContext, target: { chatId: 4242, threadId: 86 } }
    );

    const result = await ensureBasicLoops(fixture.config, fixture.contexts, fixture.workers, fixture.cronManager);
    expect(result).toContain("created:");
    const preserved = fixture.db.getCronJob(userJob.id);
    expect(preserved?.executionContextSlug).toBe("user-updates");
    expect(preserved?.targetThreadId).toBe(86);
    expect(preserved?.instruction).toBe("User-scoped update check.");
    expect(fixture.db.listCronJobs().some((job) => job.label === "update-check" && job.executionContextSlug === "brainstack-routines")).toBe(true);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("worker config reload fails closed after malformed or deleted workers file", async () => {
  const fixture = await createFixture();

  try {
    expect(fixture.workers.getWorkerConfig("worker1")?.name).toBe("worker1");
    await writeFile(fixture.workersFile, "{ not-json");
    expect(fixture.workers.getWorkerConfig("worker1")).toBeNull();
    const malformed = await fixture.workers.refreshWorkers();
    expect(malformed.some((worker) => worker.host === "workers-config" && worker.status === "error")).toBe(true);

    await rm(fixture.workersFile, { force: true });
    expect(fixture.workers.getWorkerConfig("control")).toBeNull();
    const deleted = await fixture.workers.refreshWorkers();
    expect(deleted.some((worker) => worker.host === "workers-config" && worker.lastError?.includes("does not exist"))).toBe(true);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("update-check marks an all-machine probe failure as failed", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx update-failures control scratch", 87));
    const context = fixture.db.getContextBySlug("update-failures")!;
    const workerInternals = fixture.workers as unknown as {
      runUpdateCheckProbe: (worker: { name: string; transport: string }) => Promise<{
        ok: boolean;
        host: string;
        transport: string;
        exitCode: number;
        stdout: string;
        stderr: string;
        durationMs: number;
        commandLabel: string;
      }>;
    };
    const originalProbe = workerInternals.runUpdateCheckProbe.bind(fixture.workers);
    workerInternals.runUpdateCheckProbe = async (worker) => ({
      ok: false,
      host: worker.name,
      transport: worker.transport,
      exitCode: 70,
      stdout: "",
      stderr: "synthetic update probe failure",
      durationMs: 1,
      commandLabel: "update-check"
    });

    const result = await fixture.workers.runUpdateCheck(context);
    workerInternals.runUpdateCheckProbe = originalProbe;
    expect(result.ok).toBe(false);
    expect(result.exitCode).toBe(75);
    expect(result.stderr).toContain("failed on all");
    expect(result.stdout).toContain("- status: failed");
    expect(result.stdout).toContain("- failed_machines: 2");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("update-check reports known but unconfigured machines instead of silently omitting them", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx update-known control scratch", 88));
    const context = fixture.db.getContextBySlug("update-known")!;
    fixture.db.saveWorker({
      host: "seen-only",
      transport: "ssh",
      status: "healthy",
      reachable: true,
      localExecution: false,
      sshTarget: "seen-only.tailnet",
      sshUser: "factory",
      lastCheckedAt: new Date().toISOString(),
      lastSeenAt: new Date().toISOString(),
      lastError: null,
      details: null,
      updatedAt: new Date().toISOString()
    });
    const workerInternals = fixture.workers as unknown as {
      runUpdateCheckProbe: (worker: { name: string; transport: string }) => Promise<{
        ok: boolean;
        host: string;
        transport: string;
        exitCode: number;
        stdout: string;
        stderr: string;
        durationMs: number;
        commandLabel: string;
      }>;
    };
    const originalProbe = workerInternals.runUpdateCheckProbe.bind(fixture.workers);
    workerInternals.runUpdateCheckProbe = async (worker) => ({
      ok: true,
      host: worker.name,
      transport: worker.transport,
      exitCode: 0,
      stdout: "ok",
      stderr: "",
      durationMs: 1,
      commandLabel: "update-check"
    });

    const result = await fixture.workers.runUpdateCheck(context);
    workerInternals.runUpdateCheckProbe = originalProbe;
    expect(result.ok).toBe(true);
    expect(result.stdout).toContain("## seen-only");
    expect(result.stdout).toContain("skipped: known machine has no configured worker entry for update-check");
    expect(result.commandLabel).toContain("seen-only");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("worker health degrades when sudo or harness probe checks fail", async () => {
  const fixture = await createFixture();
  const previousFakeSudoFail = process.env.FACTORY_TEST_FAKE_SUDO_FAIL;
  const previousFakeSshExec = process.env.FACTORY_TEST_FAKE_SSH_EXEC;

  try {
    process.env.FACTORY_TEST_FAKE_SUDO_FAIL = "1";
    await fixture.commands.handleMessage(telegramMessage("/workers", 88));
    const sudoText = fixture.telegram.sent.at(-1)?.text || "";
    expect(sudoText).toContain("control | status=degraded");
    expect(sudoText).toContain("sudo=fail");
    expect(sudoText).toContain("error=sudo -n true failed");

    if (previousFakeSudoFail === undefined) {
      delete process.env.FACTORY_TEST_FAKE_SUDO_FAIL;
    } else {
      process.env.FACTORY_TEST_FAKE_SUDO_FAIL = previousFakeSudoFail;
    }

    process.env.FACTORY_TEST_FAKE_SSH_EXEC = "1";
    process.env.FACTORY_TEST_FAKE_SUDO_FAIL = "1";
    await fixture.commands.handleMessage(telegramMessage("/workers", 88));
    const remoteSudoText = fixture.telegram.sent.at(-1)?.text || "";
    expect(remoteSudoText).toContain("worker1 | status=degraded");
    expect(remoteSudoText).toContain("sudo=fail");

    if (previousFakeSudoFail === undefined) {
      delete process.env.FACTORY_TEST_FAKE_SUDO_FAIL;
    } else {
      process.env.FACTORY_TEST_FAKE_SUDO_FAIL = previousFakeSudoFail;
    }
    process.env.FACTORY_TEST_FAKE_SUDO_FAIL = "0";

    const badCodex = join(dirname(fixture.fakeCodex), "bad-codex");
    await makeExecutable(
      badCodex,
      `#!/usr/bin/env bash
if [[ "\${1:-}" == "--version" ]]; then
  echo 'bad codex'
  exit 0
fi
if [[ "\${1:-}" == "exec" && "\${2:-}" == "--help" ]]; then
  echo 'exec help without required flags'
  exit 0
fi
exit 0
`
    );

    const workers = JSON.parse(await readFile(fixture.workersFile, "utf8")) as Array<Record<string, unknown>>;
    workers[0] = {
      ...workers[0],
      harnessBin: badCodex
    };
    await writeFile(fixture.workersFile, `${JSON.stringify(workers, null, 2)}\n`);

    await fixture.commands.handleMessage(telegramMessage("/workers", 88));
    const harnessText = fixture.telegram.sent.at(-1)?.text || "";
    expect(harnessText).toContain("control | status=degraded");
    expect(harnessText).toContain("sudo=ok");
    expect(harnessText).toContain("error=missing harness flag: --dangerously-bypass-approvals-and-sandbox");
  } finally {
    if (previousFakeSudoFail === undefined) {
      delete process.env.FACTORY_TEST_FAKE_SUDO_FAIL;
    } else {
      process.env.FACTORY_TEST_FAKE_SUDO_FAIL = previousFakeSudoFail;
    }
    if (previousFakeSshExec === undefined) {
      delete process.env.FACTORY_TEST_FAKE_SSH_EXEC;
    } else {
      process.env.FACTORY_TEST_FAKE_SSH_EXEC = previousFakeSshExec;
    }
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("worker health reads Codex runtime defaults without failing on sparse config", async () => {
  const fixture = await createFixture();
  const previousCodexHome = process.env.CODEX_HOME;
  const codexHome = join(fixture.root, "codex-home");
  const codexConfig = join(codexHome, "config.toml");

  try {
    await mkdir(codexHome, { recursive: true });
    process.env.CODEX_HOME = codexHome;

    await writeFile(codexConfig, "[profiles.default]\nmodel = \"nested-ignored\"\nmodel_reasoning_effort = \"xhigh\"\n");
    await fixture.commands.handleMessage(telegramMessage("/workers", 89));
    const sparseText = fixture.telegram.sent.at(-1)?.text || "";
    expect(sparseText).toContain("control | status=healthy");
    expect(sparseText).toContain("model=default");
    expect(sparseText).toContain("effort=default");

    await writeFile(codexConfig, "model = \"gpt-configured\" # inline comment\n[profiles.default]\nmodel = \"nested-ignored\"\n");
    await fixture.commands.handleMessage(telegramMessage("/workers", 89));
    const modelOnlyText = fixture.telegram.sent.at(-1)?.text || "";
    expect(modelOnlyText).toContain("control | status=healthy");
    expect(modelOnlyText).toContain("model=gpt-configured");
    expect(modelOnlyText).toContain("effort=default");

    await writeFile(codexConfig, "model_reasoning_effort = \"high\" # inline comment\n[profiles.default]\nmodel_reasoning_effort = \"xhigh\"\n");
    await fixture.commands.handleMessage(telegramMessage("/workers", 89));
    const effortOnlyText = fixture.telegram.sent.at(-1)?.text || "";
    expect(effortOnlyText).toContain("control | status=healthy");
    expect(effortOnlyText).toContain("model=default");
    expect(effortOnlyText).toContain("effort=high");
  } finally {
    if (previousCodexHome === undefined) {
      delete process.env.CODEX_HOME;
    } else {
      process.env.CODEX_HOME = previousCodexHome;
    }
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("context usage and manual compaction stay concise and harness-aware", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx compact-empty control scratch", 92));
    await fixture.commands.handleMessage(telegramMessage("/usage", 92));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Compaction: available after the first Codex session");

    await fixture.commands.handleMessage(telegramMessage("/newctx compact-lab control scratch", 90));
    await fixture.commands.handleMessage(telegramMessage("Start the compact lab.", 90));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for compact-lab.")));
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("Dispatched resume for compact-lab."))).toBe(false);

    await fixture.commands.handleMessage(telegramMessage("/context", 90));
    const contextText = fixture.telegram.sent.at(-1)?.text || "";
    expect(contextText).toContain("Context: compact-lab");
    expect(contextText).toContain("Harness: codex");
    expect(contextText).toContain("Model: default");
    expect(contextText).toContain("Thinking effort: default");

    await fixture.commands.handleMessage(telegramMessage("/usage", 90));
    const usageText = fixture.telegram.sent.at(-1)?.text || "";
    expect(usageText).toContain("Context: compact-lab");
    expect(usageText).toContain("Usage:");
    expect(usageText).toContain("Turns counted:");
    expect(usageText).toContain("Compaction: /compact available");

    const beforeCompactCount = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("/compact", 90));
    await waitFor(() => fixture.telegram.sent.some((entry, index) => index >= beforeCompactCount && entry.text.includes("Compacted thread.")));
    const compactMessages = fixture.telegram.sent.slice(beforeCompactCount).map((entry) => entry.text);
    expect(compactMessages[0]).toBe("Compacting thread…");
    expect(compactMessages.some((text) => text.includes("Dispatched resume"))).toBe(false);
    expect(await readFile(join(fixture.factoryRoot, "scratch", "compact-lab", ".factory", "control-plane.prompt.md"), "utf8")).toBe("/compact");

    const beforeAutoCompactCount = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("emit compact event", 90));
    await waitFor(() => fixture.telegram.sent.slice(beforeAutoCompactCount).some((entry) => entry.text === "Compacting thread…"));
    await waitFor(() => fixture.telegram.sent.slice(beforeAutoCompactCount).some((entry) => entry.text.includes("Reply turn 3 for compact-lab.")));

    const beforeFinalCompactCount = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("emit final compact event without newline", 90));
    await waitFor(() => fixture.telegram.sent.slice(beforeFinalCompactCount).some((entry) => entry.text === "Compacting thread…"));
    await waitFor(() => fixture.telegram.sent.slice(beforeFinalCompactCount).some((entry) => entry.text.includes("Reply turn 4 for compact-lab.")));
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("manual compaction failures are classified without false positives from normal compact text", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx compact-failure control scratch", 93));
    await fixture.commands.handleMessage(telegramMessage("Start the compact failure lab.", 93));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for compact-failure.")));

    await makeExecutable(
      fixture.fakeCodex,
      `#!/usr/bin/env bash
cat >/dev/null
echo 'ordinary run mentioned compact and failed' >&2
exit 31
`
    );

    const beforeNormalFailureCount = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("ordinary compact word failure", 93));
    await waitFor(() =>
      fixture.telegram.sent.slice(beforeNormalFailureCount).some((entry) => entry.text.includes("ordinary run mentioned compact and failed"))
    );
    expect(fixture.telegram.sent.slice(beforeNormalFailureCount).some((entry) => entry.text.includes("Codex compaction failed."))).toBe(false);

    const beforeManualFailureCount = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("/compact", 93));
    await waitFor(() =>
      fixture.telegram.sent.slice(beforeManualFailureCount).some((entry) => entry.text.includes("Codex compaction failed."))
    );
    const manualFailureText = fixture.telegram.sent.slice(beforeManualFailureCount).map((entry) => entry.text).join("\n\n");
    expect(manualFailureText).toContain("Compacting thread…");
    expect(manualFailureText).toContain("Log:");
    expect(manualFailureText).toContain("ordinary run mentioned compact and failed");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("manual compaction reports unsupported harnesses", async () => {
  const fixture = await createFixture({
    FACTORY_HARNESS: "claude"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx compact-claude control scratch", 91));
    await fixture.commands.handleMessage(telegramMessage("/compact", 91));
    expect(fixture.telegram.sent.at(-1)?.text).toBe("Claude has no manual compact support.");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("cron shortcuts use stable scoped snapshots and cron ids cannot cross topics", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx topic-a control scratch", 82));
    await fixture.commands.handleMessage(
      telegramMessage("/cron create reminder alpha daily 09:00 Europe/Zagreb Alpha reminder.", 82)
    );
    const alpha = fixture.db.listCronJobs().find((job) => job.label === "alpha")!;

    await fixture.commands.handleMessage(telegramMessage("/newctx topic-b control scratch", 83));
    await fixture.commands.handleMessage(
      telegramMessage("/cron create reminder beta daily 09:00 Europe/Zagreb Beta reminder.", 83)
    );
    const beta = fixture.db.listCronJobs().find((job) => job.label === "beta")!;

    await fixture.commands.handleMessage(telegramMessage("/crons", 82));
    const overview = fixture.telegram.sent.at(-1)?.text || "";
    const runShortcut = overview.match(/\/cron_run_[a-z0-9]{6,10}_1/)?.[0];
    expect(runShortcut).toBeTruthy();

    await fixture.commands.handleMessage(
      telegramMessage("/cron create reminder newest daily 09:05 Europe/Zagreb Newest reminder.", 82)
    );
    await fixture.commands.handleMessage(telegramMessage(runShortcut!, 82));
    expect(fixture.telegram.sent.at(-2)?.text).toBe("Alpha reminder.");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Cron run sent:");

    const topicBContext = fixture.db.getContextBySlug("topic-b")!;
    const crossDeleteNotes = await fixture.cronManager.applyManifest(
      JSON.stringify({ actions: [{ type: "delete", selector: { id: alpha.id } }] }),
      { context: topicBContext, target: { chatId: 4242, threadId: 83 } }
    );
    expect(crossDeleteNotes.join("\n")).toContain("Unknown cron job in this topic/context");
    expect(fixture.db.getCronJob(alpha.id)).not.toBeNull();

    const scopedCreateNotes = await fixture.cronManager.applyManifest(
      JSON.stringify({
        actions: [
          {
            type: "create",
            job: {
              label: "cross-create",
              kind: "reminder",
              schedule: { type: "daily", time: "09:00", timezone: "Europe/Zagreb" },
              targetThreadId: 82,
              reminderText: "should not cross"
            }
          }
        ]
      }),
      { context: topicBContext, target: { chatId: 4242, threadId: 83 } }
    );
    expect(scopedCreateNotes.join("\n")).toContain("Cron manifest create actions cannot set scoped fields");
    expect(fixture.db.listCronJobs().some((job) => job.label === "cross-create")).toBe(false);

    await fixture.cronManager.updateJob(alpha, { executionContextSlug: "topic-b", targetThreadId: 83 });
    await fixture.commands.handleMessage(telegramMessage(runShortcut!, 82));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("shortcut expired or the job was deleted");

    await fixture.commands.handleMessage(telegramMessage("/cron_run_1", 82));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Numeric cron shortcuts expire");

    await fixture.commands.handleMessage(telegramMessage(`/cron show ${beta.id}`, 82));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Error: No cron job matched in this topic/context");
    await fixture.commands.handleMessage(telegramMessage(`/cron_run ${beta.id}`, 82));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Error: No cron job matched in this topic/context");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("per-topic Codex mode, model, and effort overrides persist across resume without losing the session", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx tuning control scratch", 70));

    await fixture.commands.handleMessage(telegramMessage("/mode fast", 70));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Codex mode set to fast");

    await fixture.commands.handleMessage(telegramMessage("/topicinfo", 70));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Codex mode: fast (model=gpt-5.4-mini effort=low)");

    await fixture.commands.handleMessage(telegramMessage("Handle this quickly.", 70));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for tuning.")));

    const tuningRoot = join(fixture.factoryRoot, "scratch", "tuning");
    expect(await readFile(join(tuningRoot, ".factory", "fake-model.txt"), "utf8")).toBe("gpt-5.4-mini");
    expect(await readFile(join(tuningRoot, ".factory", "fake-reasoning.txt"), "utf8")).toBe('model_reasoning_effort="low"');

    const firstSession = fixture.db.getContextBySlug("tuning")?.codexSessionId || "";
    expect(firstSession).not.toBe("");

    await fixture.commands.handleMessage(telegramMessage("/model 5.5", 70));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Codex mode now custom");

    await fixture.commands.handleMessage(telegramMessage("/effort high", 70));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Codex mode now custom");

    await fixture.commands.handleMessage(telegramMessage("Continue with the same session.", 70));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 2 for tuning.")));

    expect(fixture.db.getContextBySlug("tuning")?.codexSessionId).toBe(firstSession);
    expect(await readFile(join(tuningRoot, ".factory", "fake-model.txt"), "utf8")).toBe("gpt-5.5");
    expect(await readFile(join(tuningRoot, ".factory", "fake-reasoning.txt"), "utf8")).toBe('model_reasoning_effort="high"');

    await fixture.commands.handleMessage(telegramMessage("/mode clear", 70));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Codex mode reset to default");

    await fixture.commands.handleMessage(telegramMessage("Back to defaults.", 70));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 3 for tuning.")));

    expect(await readFile(join(tuningRoot, ".factory", "fake-model.txt"), "utf8")).toBe("");
    expect(await readFile(join(tuningRoot, ".factory", "fake-reasoning.txt"), "utf8")).toBe("");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("legacy persisted bare numeric Codex model overrides are normalized at dispatch", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx legacy-model control scratch", 72));
    const context = fixture.db.getContextBySlug("legacy-model");
    fixture.contexts.saveContext({ ...context!, modelOverride: "5.5" });

    await fixture.commands.handleMessage(telegramMessage("use persisted model override", 72));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for legacy-model.")));

    const modelFile = join(context!.worktreePath, ".factory", "fake-model.txt");
    expect(await readFile(modelFile, "utf8")).toBe("gpt-5.5");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("Codex JSON errors are summarized instead of dumped into Telegram", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx jsonerror control scratch", 71));
    await fixture.commands.handleMessage(telegramMessage("emit codex json error", 71));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Codex failed.")));
    const failureText = fixture.telegram.sent.at(-1)?.text || "";
    expect(failureText).toContain("jsonerror failed on control.");
    expect(failureText).toContain("Codex failed.");
    expect(failureText).toContain("The '5.5' model is not supported when using Codex with a ChatGPT account.");
    expect(failureText).not.toContain('{"type":"thread.started"');
    expect(failureText).not.toContain('\\"type\\":\\"error\\"');
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("Codex JSON errors are summarized even when stderr has diagnostics", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx jsonstderr control scratch", 73));
    await fixture.commands.handleMessage(telegramMessage("emit codex json error with stderr", 73));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("jsonstderr failed on control.")));
    const failureText = fixture.telegram.sent.at(-1)?.text || "";
    expect(failureText).toContain("Codex failed.");
    expect(failureText).toContain("The '5.5' model is not supported when using Codex with a ChatGPT account.");
    expect(failureText).not.toContain("non-json stderr diagnostic");
    expect(failureText).not.toContain('{"type":"thread.started"');
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("artifact delivery rejects absolute paths recorded in ARTIFACTS by default", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx exfil control host", 66));
    const context = fixture.db.getContextBySlug("exfil");
    expect(context?.worktreePath).toBeTruthy();
    await writeFile(join(context!.worktreePath, ".factory", "ARTIFACTS.md"), "# Artifacts\n\n- /etc/passwd - should not leave host\n");

    await fixture.commands.handleMessage(telegramMessage("/artifacts send passwd", 66));

    expect(fixture.telegram.attachments).toHaveLength(0);
    expect(fixture.telegram.sent.at(-1)?.text).toContain("absolute artifact paths are disabled");

    await fixture.commands.handleMessage(telegramMessage("/shred", 66));
    const shredList = fixture.telegram.sent.at(-1)?.text || "";
    const shredShortcut = shredList.match(/\/shred_[a-z0-9]{6,10}_1/)?.[0] || "";
    expect(shredShortcut).not.toBe("");
    await fixture.commands.handleMessage(telegramMessage(shredShortcut, 66));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Not deleted:");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Unsafe artifact delete path");
    expect(parseArtifactEntries(await readFile(join(context!.worktreePath, ".factory", "ARTIFACTS.md"), "utf8"))).toHaveLength(1);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("artifact delivery supports backticked bare relative filenames", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx bareartifact control scratch", 67));
    const context = fixture.db.getContextBySlug("bareartifact");
    expect(context?.worktreePath).toBeTruthy();

    const artifactMarkdown = [
      "# Artifacts",
      "",
      "- `worker-openclaw-audit.md` - read-only audit report on `worker-host`",
      "- `openclaw-retirement-phase1.md` - phase 1 cleanup report"
    ].join("\n");
    await writeFile(join(context!.worktreePath, "worker-openclaw-audit.md"), "audit contents");
    await writeFile(join(context!.worktreePath, "openclaw-retirement-phase1.md"), "phase 1 contents");
    await writeFile(join(context!.worktreePath, ".factory", "ARTIFACTS.md"), artifactMarkdown);

    expect(parseArtifactEntries(artifactMarkdown).map((entry) => entry.path)).toEqual([
      "worker-openclaw-audit.md",
      "openclaw-retirement-phase1.md"
    ]);
    expect(
      parseArtifactEntries("# Artifacts\n\n- `.factory/STATE.json`\n- `.git/config`\n- `.factory/reports/update-check.md`\n").map(
        (entry) => entry.path
      )
    ).toEqual([".factory/reports/update-check.md"]);
    const splitLineArtifacts = removeArtifactEntriesFromMarkdown(
      "# Artifacts\n\n- `one.md` and `two.md`\n",
      ["one.md"]
    );
    expect(parseArtifactEntries(splitLineArtifacts).map((entry) => entry.path)).toEqual(["two.md"]);
    expect(
      resolveManifestRequests(
        '{"attachments":[{"path":"worker-openclaw-audit.md","type":"document"}]}',
        artifactMarkdown
      ).requests.map((request) => request.path)
    ).toEqual(["worker-openclaw-audit.md"]);
    const manyAttachments = resolveManifestRequests(
      JSON.stringify({
        attachments: Array.from({ length: 12 }, (_, index) => ({ path: `artifact-${index}.md`, type: "document" }))
      }),
      Array.from({ length: 12 }, (_, index) => `- \`artifact-${index}.md\``).join("\n")
    );
    expect(manyAttachments.requests).toHaveLength(10);
    expect(manyAttachments.skipped.join("\n")).toContain("file limit");

    await fixture.commands.handleMessage(telegramMessage("/artifacts send worker-openclaw-audit.md", 67));
    await waitFor(() => fixture.telegram.attachments.some((entry) => entry.fileName === "worker-openclaw-audit.md"));

    const sent = fixture.telegram.attachments.find((entry) => entry.fileName === "worker-openclaw-audit.md");
    expect(sent?.text).toBe("audit contents");
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("No artifact file paths matched"))).toBe(false);

    await symlink(join(context!.worktreePath, ".factory", "STATE.json"), join(context!.worktreePath, "safe-report.md"));
    await symlink(join(context!.worktreePath, ".factory"), join(context!.worktreePath, "reports-link"));
    await writeFile(join(context!.worktreePath, ".factory", "ARTIFACTS.md"), "# Artifacts\n\n- `safe-report.md`\n- `reports-link/STATE.json`\n");
    const symlinkSendCount = fixture.telegram.attachments.length;
    await fixture.commands.handleMessage(telegramMessage("/artifacts send safe-report.md", 67));
    expect(fixture.telegram.attachments).toHaveLength(symlinkSendCount);
    expect(fixture.telegram.sent.at(-1)?.text).toContain("artifact path is a symlink");
    await fixture.commands.handleMessage(telegramMessage("/artifacts send reports-link/STATE.json", 67));
    expect(fixture.telegram.attachments).toHaveLength(symlinkSendCount);
    expect(fixture.telegram.sent.at(-1)?.text).toContain("protected artifact path cannot be sent");
    await writeFile(join(context!.worktreePath, ".factory", "ARTIFACTS.md"), artifactMarkdown);

    await fixture.commands.handleMessage(telegramMessage("/artifacts", 67));
    const artifactListText = fixture.telegram.sent.at(-1)?.text || "";
    const firstSendShortcut = artifactListText.match(/\/artifact_[a-z0-9]{6,10}_1/)?.[0];
    const firstShredShortcut = artifactListText.match(/\/shred_[a-z0-9]{6,10}_1/)?.[0];
    expect(firstSendShortcut).toBeTruthy();
    expect(artifactListText).toMatch(/send \+ del: \/artifact_[a-z0-9]{6,10}_2_senddel/);
    expect(firstShredShortcut).toBeTruthy();
    expect(fixture.telegram.sent.at(-1)?.text).toContain("/artifact_latest");

    await fixture.commands.handleMessage(telegramMessage(firstSendShortcut!, 67));
    await waitFor(() => fixture.telegram.attachments.filter((entry) => entry.fileName === "worker-openclaw-audit.md").length >= 2);

    await fixture.commands.handleMessage(telegramMessage("/artifact_1", 67));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Numeric artifact shortcuts expire");

    const splitSendCount = fixture.telegram.attachments.length;
    await fixture.commands.handleMessage(telegramMessage("Send me this", 67));
    await fixture.commands.handleMessage(telegramMessage("worker-openclaw-audit.md", 67));
    await waitFor(() => fixture.telegram.attachments.length > splitSendCount);
    expect(fixture.telegram.attachments.at(-1)?.fileName).toBe("worker-openclaw-audit.md");

    await fixture.commands.handleMessage(telegramMessage("Send me this artifact", 67));
    await waitFor(() => fixture.telegram.attachments.some((entry) => entry.fileName === "openclaw-retirement-phase1.md"));
    expect(fixture.telegram.attachments.at(-1)?.fileName).toBe("openclaw-retirement-phase1.md");
    expect(fixture.telegram.attachments.at(-1)?.text).toBe("phase 1 contents");
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn"))).toBe(false);

    const broadTaskAttachmentCount = fixture.telegram.attachments.length;
    await fixture.commands.handleMessage(telegramMessage("Generate a report about the workspace and send it to me.", 67));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for bareartifact.")));
    await waitFor(() => !fixture.dispatcher.isActive("bareartifact"));
    expect(fixture.telegram.attachments).toHaveLength(broadTaskAttachmentCount);
    await writeFile(join(context!.worktreePath, ".factory", "ARTIFACTS.md"), artifactMarkdown);

    const attachmentCount = fixture.telegram.attachments.length;
    await fixture.commands.handleMessage(telegramMessage("Send file", 67));
    await waitFor(() => fixture.telegram.attachments.length > attachmentCount);
    expect(fixture.telegram.attachments.at(-1)?.fileName).toBe("openclaw-retirement-phase1.md");

    const noFilterSendCount = fixture.telegram.attachments.length;
    await fixture.commands.handleMessage(telegramMessage("/artifacts send", 67));
    await waitFor(() => fixture.telegram.attachments.length > noFilterSendCount);
    expect(fixture.telegram.attachments.at(-1)?.fileName).toBe("openclaw-retirement-phase1.md");

    await fixture.commands.handleMessage(telegramMessage("/shred", 67));
    const shredListText = fixture.telegram.sent.at(-1)?.text || "";
    const currentFirstShredShortcut = shredListText.match(/\/shred_[a-z0-9]{6,10}_1/)?.[0];
    expect(currentFirstShredShortcut).toBeTruthy();
    expect(shredListText).toMatch(/\/shred_[a-z0-9]{6,10}_2 openclaw-retirement-phase1\.md/);
    expect(fixture.telegram.sent.at(-1)?.text).toContain("/shred_latest");

    await writeFile(join(context!.worktreePath, ".factory", "ARTIFACTS.md"), "# Artifacts\n\n- `openclaw-retirement-phase1.md`\n");
    await fixture.commands.handleMessage(telegramMessage(currentFirstShredShortcut!, 67));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("artifact shortcut expired or the artifact changed");
    await writeFile(join(context!.worktreePath, ".factory", "ARTIFACTS.md"), artifactMarkdown);

    await fixture.commands.handleMessage(telegramMessage("/shred", 67));
    const refreshedShredShortcut = (fixture.telegram.sent.at(-1)?.text || "").match(/\/shred_[a-z0-9]{6,10}_1/)?.[0];
    await fixture.commands.handleMessage(telegramMessage(refreshedShredShortcut!, 67));
    expect(await Bun.file(join(context!.worktreePath, "worker-openclaw-audit.md")).exists()).toBe(false);
    const afterFirstShred = await readFile(join(context!.worktreePath, ".factory", "ARTIFACTS.md"), "utf8");
    expect(parseArtifactEntries(afterFirstShred).map((entry) => entry.path)).toEqual(["openclaw-retirement-phase1.md"]);
    expect(fixture.db.getContextBySlug("bareartifact")?.lastArtifacts).toContain("openclaw-retirement-phase1.md");
    expect(fixture.db.getContextBySlug("bareartifact")?.lastArtifacts).not.toContain("worker-openclaw-audit.md");

    const sendDeleteCount = fixture.telegram.attachments.length;
    await fixture.commands.handleMessage(telegramMessage("/artifact_latest_senddel", 67));
    await waitFor(() => fixture.telegram.attachments.length > sendDeleteCount);
    expect(fixture.telegram.attachments.at(-1)?.fileName).toBe("openclaw-retirement-phase1.md");
    expect(await Bun.file(join(context!.worktreePath, "openclaw-retirement-phase1.md")).exists()).toBe(false);
    const afterSendDel = await readFile(join(context!.worktreePath, ".factory", "ARTIFACTS.md"), "utf8");
    expect(parseArtifactEntries(afterSendDel)).toHaveLength(0);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("artifact cleanup preserves concurrent manifest additions and rejects symlinked metadata writes", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx artifact-races control scratch", 68));
    const context = fixture.db.getContextBySlug("artifact-races");
    expect(context?.worktreePath).toBeTruthy();

    const artifactPath = join(context!.worktreePath, ".factory", "ARTIFACTS.md");
    await writeFile(join(context!.worktreePath, "old.md"), "old");
    await writeFile(join(context!.worktreePath, "survivor.md"), "survivor");
    await writeFile(artifactPath, "# Artifacts\n\n- `old.md`\n- `survivor.md`\n");

    await fixture.commands.handleMessage(telegramMessage("/shred", 68));
    const shredShortcut = (fixture.telegram.sent.at(-1)?.text || "").match(/\/shred_[a-z0-9]{6,10}_1/)?.[0];
    expect(shredShortcut).toBeTruthy();

    const originalDeleteArtifactFile = fixture.workers.deleteArtifactFile.bind(fixture.workers);
    let appended = false;
    fixture.workers.deleteArtifactFile = async (...args) => {
      const result = await originalDeleteArtifactFile(...args);
      if (!appended) {
        appended = true;
        await writeFile(join(context!.worktreePath, "late.md"), "late");
        await writeFile(artifactPath, "# Artifacts\n\n- `old.md`\n- `survivor.md`\n- `late.md`\n");
      }
      return result;
    };
    await fixture.commands.handleMessage(telegramMessage(shredShortcut!, 68));
    fixture.workers.deleteArtifactFile = originalDeleteArtifactFile;
    expect(parseArtifactEntries(await readFile(artifactPath, "utf8")).map((entry) => entry.path)).toEqual([
      "survivor.md",
      "late.md"
    ]);

    await writeFile(join(context!.worktreePath, "old-after-read.md"), "old");
    await writeFile(join(context!.worktreePath, "survivor-after-read.md"), "survivor");
    await writeFile(artifactPath, "# Artifacts\n\n- `old-after-read.md`\n- `survivor-after-read.md`\n");
    await fixture.commands.handleMessage(telegramMessage("/shred", 68));
    const retryShredShortcut = (fixture.telegram.sent.at(-1)?.text || "").match(/\/shred_[a-z0-9]{6,10}_1/)?.[0];
    const originalWriteIfUnchanged = fixture.workers.writeWorkspaceFileIfUnchanged.bind(fixture.workers);
    let forcedManifestDrift = false;
    fixture.workers.writeWorkspaceFileIfUnchanged = async (...args) => {
      if (!forcedManifestDrift) {
        forcedManifestDrift = true;
        await writeFile(join(context!.worktreePath, "late-after-read.md"), "late");
        await writeFile(
          artifactPath,
          "# Artifacts\n\n- `old-after-read.md`\n- `survivor-after-read.md`\n- `late-after-read.md`\n"
        );
        return false;
      }
      return originalWriteIfUnchanged(...args);
    };
    await fixture.commands.handleMessage(telegramMessage(retryShredShortcut!, 68));
    fixture.workers.writeWorkspaceFileIfUnchanged = originalWriteIfUnchanged;
    expect(parseArtifactEntries(await readFile(artifactPath, "utf8")).map((entry) => entry.path)).toEqual([
      "survivor-after-read.md",
      "late-after-read.md"
    ]);

    await rm(artifactPath, { force: true });
    const outside = join(fixture.root, "outside-artifacts.md");
    await writeFile(outside, "outside");
    await symlink(outside, artifactPath);
    expect(await fixture.workers.readFactoryFile(context!, "ARTIFACTS.md")).toBeNull();
    await expect(fixture.workers.writeWorkspaceFile(context!, ".factory/ARTIFACTS.md", "# Artifacts\n")).rejects.toThrow(
      "workspace write refused symlink target"
    );
    expect(await readFile(outside, "utf8")).toBe("outside");

    const updateCheck = await fixture.workers.runUpdateCheck(context!);
    expect(updateCheck.ok).toBe(false);
    expect(updateCheck.stderr).toContain("workspace write refused symlink target: .factory/ARTIFACTS.md");
    expect(await readFile(outside, "utf8")).toBe("outside");

    await rm(artifactPath, { force: true });
    await writeFile(artifactPath, "# Artifacts\n");
    await writeFile(join(context!.worktreePath, "important.md"), "important");
    await symlink(join(context!.worktreePath, "important.md"), join(context!.worktreePath, "artifact-link.md"));
    const deleteLink = await fixture.workers.deleteArtifactFile(context!, "artifact-link.md");
    expect(deleteLink.status).toBe("deleted");
    expect(await Bun.file(join(context!.worktreePath, "artifact-link.md")).exists()).toBe(false);
    expect(await readFile(join(context!.worktreePath, "important.md"), "utf8")).toBe("important");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("successful runs can opt into shared brain raw imports", async () => {
  const received: Array<Record<string, unknown>> = [];
  const port = 35_000 + Math.floor(Math.random() * 5_000);
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port,
    async fetch(req) {
      expect(new URL(req.url).pathname).toBe("/api/import");
      expect(req.headers.get("authorization")).toBe("Bearer brain-import-token");
      received.push((await req.json()) as Record<string, unknown>);
      return Response.json({ ok: true, artifact_id: "test-artifact" });
    }
  });
  const fixture = await createFixture({
    BRAIN_BASE_URL: `http://127.0.0.1:${port}`,
    BRAIN_IMPORT_TOKEN: "brain-import-token"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx brainbridge control scratch", 67));
    await fixture.commands.handleMessage(telegramMessage("Summarize bridge state.", 67));
    await waitFor(() => received.length === 1);

    expect(received[0].source_harness).toBe("telemux");
    expect(received[0].source_type).toBe("telemux-run");
    expect(String(received[0].text)).toContain("## SUMMARY.md");
    expect(String(received[0].text)).toContain("## ARTIFACTS.md");
  } finally {
    server.stop(true);
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("telemux can run Claude as the selected harness", async () => {
  const fixture = await createFixture({
    FACTORY_HARNESS: "claude"
  });

  try {
    expect(fixture.workers["config"].harness).toBe("claude");
    await fixture.commands.handleMessage(telegramMessage("/newctx claudeharness control scratch", 68));
    await fixture.commands.handleMessage(telegramMessage("Use the selected harness.", 68));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Claude reply turn 1 for claudeharness.")));

    const context = fixture.db.getContextBySlug("claudeharness");
    expect(context?.state).toBe("active");
    expect(await readFile(join(fixture.factoryRoot, "scratch", "claudeharness", ".factory", "SUMMARY.md"), "utf8")).toContain(
      "Claude turn 1"
    );
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("worker harness selection supports worker and context overrides without control-host binary leakage", async () => {
      const capture = join(tmpdir(), `telemux-ssh-capture-${Date.now()}.sh`);
      const cachedCapture = join(tmpdir(), `telemux-ssh-cached-capture-${Date.now()}.sh`);
      const argsCapture = join(tmpdir(), `telemux-ssh-args-${Date.now()}.txt`);
  const fixture = await createFixture({
    FACTORY_HARNESS: "codex",
    TEST_CONTROL_WORKER_HARNESS: "claude"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx workerharness control scratch", 69));
    await fixture.commands.handleMessage(telegramMessage("Worker default should choose Claude.", 69));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Claude reply turn 1 for workerharness.")));

    const context = fixture.db.getContextBySlug("workerharness");
    fixture.contexts.saveContext({
      ...context!,
      harness: "codex",
      harnessBin: null
    });
    await fixture.commands.handleMessage(telegramMessage("Context override should choose Codex.", 69));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for workerharness.")));

      process.env.FACTORY_TEST_CAPTURE_SSH_SCRIPT = capture;
      process.env.FACTORY_TEST_CAPTURE_SSH_ARGS = argsCapture;
      const doctorCompatibleFingerprint = sha256Hex(
        canonicalJson({
          worker: "worker1",
          transport: "ssh",
          sshTarget: "worker1.tailnet",
          sshUser: "factory",
          harness: "codex",
          harnessBin: "codex"
        })
      );
      await writeFile(
        join(fixture.config.stateRoot, "worker-env-cache.json"),
        `${JSON.stringify(
          {
            worker1: {
              worker: "worker1",
              fingerprint: doctorCompatibleFingerprint,
              path: "/doctor/cached/bin:/usr/bin",
              harness: "codex",
              harnessBin: "codex",
              harnessVersion: "codex fake",
              detectedAt: new Date().toISOString()
            }
          },
          null,
          2
        )}\n`
      );
      await fixture.workers.probeWorker("worker1");
      const capturedArgs = await readFile(argsCapture, "utf8");
      expect(capturedArgs).toContain("StrictHostKeyChecking=yes");
      expect(capturedArgs).toContain(`UserKnownHostsFile=${fixture.config.sshKnownHostsPath}`);
      const capturedScript = await readFile(capture, "utf8");
      expect(capturedScript).toContain("harness_bin='codex'");
      expect(capturedScript).toContain("__BRAINSTACK_PATH__");
      expect(capturedScript).toContain("BRAINSTACK_WORKER_PATH");
      expect(capturedScript).not.toContain(fixture.fakeCodex);
      process.env.FACTORY_TEST_CAPTURE_SSH_SCRIPT = cachedCapture;
      await fixture.workers.readFactoryFile({ ...context!, machine: "worker1", worktreePath: "/srv/factory/scratch/cache-test" }, "SUMMARY.md");
      const cachedScript = await readFile(cachedCapture, "utf8");
      expect(cachedScript).toContain("BRAINSTACK_WORKER_PATH='/doctor/cached/bin:/usr/bin'");
    } finally {
      delete process.env.FACTORY_TEST_CAPTURE_SSH_SCRIPT;
      delete process.env.FACTORY_TEST_CAPTURE_SSH_ARGS;
    process.env.PATH = fixture.previousPath;
      await rm(capture, { force: true });
      await rm(cachedCapture, { force: true });
      await rm(argsCapture, { force: true });
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("SSH workers with explicit ports use the port for dispatch without leaking it into the remote target", async () => {
  const argsCapture = join(tmpdir(), `telemux-ssh-port-args-${Date.now()}.txt`);
  const fixture = await createFixture({
    TEST_WORKER1_SSH_TARGET: "[worker1.tailnet]:2222"
  });

  try {
    process.env.FACTORY_TEST_CAPTURE_SSH_ARGS = argsCapture;
    await fixture.workers.probeWorker("worker1");
    const capturedArgs = await readFile(argsCapture, "utf8");
    expect(capturedArgs).toContain("-p 2222");
    expect(capturedArgs).toContain("factory@worker1.tailnet");
    expect(capturedArgs).not.toContain("factory@[worker1.tailnet]:2222");
  } finally {
    delete process.env.FACTORY_TEST_CAPTURE_SSH_ARGS;
    process.env.PATH = fixture.previousPath;
    await rm(argsCapture, { force: true });
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("local worker resolves harness through BRAINSTACK_WORKER_PATH when service PATH is minimal", async () => {
  const fixture = await createFixture({
    FACTORY_CODEX_BIN: "codex",
    FACTORY_HARNESS_BIN: "codex"
  });
  const previousPath = process.env.PATH;
  const previousWorkerPath = process.env.BRAINSTACK_WORKER_PATH;

  try {
    process.env.PATH = "/usr/bin:/bin";
    process.env.BRAINSTACK_WORKER_PATH = dirname(fixture.fakeCodex);

    await fixture.commands.handleMessage(telegramMessage("/newctx localpath control scratch", 70));
    await fixture.commands.handleMessage(telegramMessage("Use codex from worker path.", 70));

    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for localpath.")));
  } finally {
    process.env.PATH = previousPath;
    if (previousWorkerPath === undefined) {
      delete process.env.BRAINSTACK_WORKER_PATH;
    } else {
      process.env.BRAINSTACK_WORKER_PATH = previousWorkerPath;
    }
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("telemux brain outbox moves repeated HTTP 425 responses to terminal review", async () => {
  const port = 45_000 + Math.floor(Math.random() * 2_000);
  const stateRoot = await mkdtemp(join(tmpdir(), "telemux-outbox-state-"));
  const previousMax425 = process.env.BRAINSTACK_OUTBOX_MAX_425_RETRIES;
  let server: ReturnType<typeof Bun.serve> | null = null;
  const fixture = await createFixture({
    BRAINSTACK_STATE_ROOT: stateRoot,
    BRAIN_BASE_URL: `http://127.0.0.1:${port}`,
    BRAIN_IMPORT_TOKEN: "outbox-token"
  });

  async function jsonFiles(root: string): Promise<string[]> {
    const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
    const files: string[] = [];
    for (const entry of entries) {
      const fullPath = join(root, entry.name);
      if (entry.isDirectory()) {
        files.push(...(await jsonFiles(fullPath)));
      } else if (entry.isFile() && entry.name.endsWith(".json")) {
        files.push(fullPath);
      }
    }
    return files;
  }

  try {
    process.env.BRAINSTACK_OUTBOX_MAX_425_RETRIES = "2";
    server = Bun.serve({
      hostname: "127.0.0.1",
      port,
      fetch() {
        return Response.json({ error: "idempotent request is already in progress" }, { status: 425 });
      }
    });

    const status = await postBrainImportOrQueue(fixture.config, {
      title: "Pending import",
      text: "pending body",
      source_machine: "control",
      source_harness: "telemux",
      source_type: "test"
    });
    expect(status).toBe("queued");

    expect(await flushBrainOutbox(fixture.config)).toEqual({ flushed: 0, kept: 1 });
    expect(await flushBrainOutbox(fixture.config)).toEqual({ flushed: 0, kept: 1 });

    const files = await jsonFiles(join(fixture.config.stateRoot, "outbox"));
    expect(files.length).toBe(1);
    const item = JSON.parse(await readFile(files[0], "utf8")) as { terminal_error?: string };
    expect(item.terminal_error || "").toContain("HTTP 425 persisted");
  } finally {
    if (server) {
      server.stop(true);
    }
    if (previousMax425 === undefined) {
      delete process.env.BRAINSTACK_OUTBOX_MAX_425_RETRIES;
    } else {
      process.env.BRAINSTACK_OUTBOX_MAX_425_RETRIES = previousMax425;
    }
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
    await rm(stateRoot, { recursive: true, force: true });
  }
}, 10_000);

test("Telegram text coalescing merges quick text and commands flush pending text", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "80"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx coalesce control scratch", 71));
    await fixture.commands.handleMessage(telegramMessage("First part", 71));
    expect(fixture.db.listPendingText()).toHaveLength(1);
    await fixture.commands.handleMessage(telegramMessage("Second part", 71));
    expect(fixture.db.listPendingText()[0]?.partsJson).toContain("Second part");
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for coalesce.")));
    expect(fixture.db.listPendingText()).toHaveLength(0);
    const prompt = await readFile(join(fixture.factoryRoot, "scratch", "coalesce", ".factory", "control-plane.prompt.md"), "utf8");
    expect(prompt).toContain("First part\n\nSecond part");

    await fixture.commands.handleMessage(telegramMessage("Flush before command", 71));
    await fixture.commands.handleMessage(telegramMessage("/topicinfo", 71));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 2 for coalesce.")));
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("Context: coalesce"))).toBe(true);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("Telegram text coalescing keeps pending text durable when dispatch setup fails", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "30"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx coalescefail control scratch", 70));
    const originalDispatcher = (fixture.commands as unknown as { dispatcher: unknown }).dispatcher;
    (fixture.commands as unknown as { dispatcher: { dispatch: () => Promise<never> } }).dispatcher = {
      dispatch: async () => {
        throw new Error("setup exploded before dispatch acceptance");
      }
    };

    await fixture.commands.handleMessage(telegramMessage("This should remain durable", 70));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("remains queued")));
    expect(fixture.db.listPendingText()).toHaveLength(1);
    expect(fixture.db.pendingTextStats().count).toBe(1);
    (fixture.commands as unknown as { dispatcher: unknown }).dispatcher = originalDispatcher;
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("Telegram text coalescing keeps pending text durable when dispatch refuses the turn", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "30"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx coalescerefuse control scratch", 69));
    const context = fixture.db.getContextBySlug("coalescerefuse");
    expect(context).toBeTruthy();
    let unlock: (() => void) | null = null;
    const releaseLock = fixture.dispatcher.withContextLock(
      context!,
      async () =>
        await new Promise<void>((resolve) => {
          unlock = resolve;
        })
    );
    const seededIds: string[] = [];
    for (let index = 0; index < 5; index += 1) {
      const row = fixture.db.enqueueQueuedTurn({
        contextSlug: "coalescerefuse",
        mode: "resume",
        instruction: `already queued ${index}`,
        chatId: 4242,
        threadId: 69,
        userId: TEST_ALLOWED_TELEGRAM_USER_ID,
        optionsJson: "{}"
      });
      seededIds.push(row.id);
    }

    await fixture.commands.handleMessage(telegramMessage("This should survive refusal", 69));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("turn queue is full")));
    expect(fixture.db.listPendingText()).toHaveLength(1);
    expect(fixture.db.listPendingText()[0]?.partsJson).toContain("This should survive refusal");
    for (const id of seededIds) {
      fixture.db.finishQueuedTurn(id, "skipped", "test cleanup");
    }
    unlock?.();
    await releaseLock;
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("Telegram text recovery drops stale pending text instead of auto-running it", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_RECOVERY_MAX_AGE_MS: "1"
  });

  try {
    fixture.db.upsertPendingText({
      key: "4242:99:123456789",
      contextSlug: "stale",
      chatId: 4242,
      threadId: 99,
      userId: TEST_ALLOWED_TELEGRAM_USER_ID,
      partsJson: JSON.stringify(["old text"])
    });
    await Bun.sleep(5);
    fixture.commands.recoverPendingText();
    expect(fixture.db.pendingTextStats().count).toBe(0);
    expect(fixture.telegram.sent.at(-1)?.text).toContain("too old to auto-dispatch");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("Telegram text coalescing flushes before buffers grow without bound", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "5000"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx coalesce-cap control scratch", 73));
    for (let index = 0; index < 26; index += 1) {
      await fixture.commands.handleMessage(telegramMessage(`part ${index}`, 73));
    }

    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for coalesce-cap.")));
    const prompt = await readFile(
      join(fixture.factoryRoot, "scratch", "coalesce-cap", ".factory", "control-plane.prompt.md"),
      "utf8"
    );
    expect(prompt).toContain("part 0");
    expect(prompt).toContain("part 24");
    expect(prompt).not.toContain("part 25");

    const pendingText = (fixture.commands as unknown as { pendingText: Map<string, { parts: string[] }> }).pendingText;
    expect([...pendingText.values()][0]?.parts).toEqual(["part 25"]);

    await fixture.commands.handleMessage(telegramMessage("/topicinfo", 73));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 2 for coalesce-cap.")));
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("Telegram text coalescing caps the number of pending buffers", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "5000"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx coalesce-map-cap control scratch", 74));
    const context = fixture.db.getContextBySlug("coalesce-map-cap")!;
    const enqueue = (fixture.commands as unknown as {
      enqueuePendingText: (context: typeof context, text: string, target: TelegramTarget, userId: number | null) => void;
    }).enqueuePendingText.bind(fixture.commands);
    for (let index = 0; index < 101; index += 1) {
      enqueue(context, `pending ${index}`, { chatId: 4242, threadId: 10_000 + index }, TEST_ALLOWED_TELEGRAM_USER_ID);
    }

    const pendingText = (fixture.commands as unknown as { pendingText: Map<string, { timer: Timer }> }).pendingText;
    expect(pendingText.size).toBeLessThanOrEqual(100);
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for coalesce-map-cap.")));
    for (const pending of pendingText.values()) {
      clearTimeout(pending.timer);
    }
    pendingText.clear();
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("Telegram text outside coalesce window stays separate", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "20"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx coalesce-gap control scratch", 72));
    await fixture.commands.handleMessage(telegramMessage("One message", 72));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for coalesce-gap.")));
    await Bun.sleep(60);
    await fixture.commands.handleMessage(telegramMessage("Second message", 72));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 2 for coalesce-gap.")));
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("phase 1 inbound Telegram media stages files and only forwards images to Codex", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx media control scratch", 60));
    const mediaContext = fixture.db.getContextBySlug("media");
    expect(mediaContext?.kind).toBe("scratch");

    fixture.telegram.registerRemoteFile("photo-main", "photos/example.jpg", "fake image bytes");
    const photoMessage = telegramPhotoMessage("Inspect this image.", 60, "photo-main");
    await fixture.commands.handleMessage(photoMessage);
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for media.")));

    const mediaRoot = join(fixture.factoryRoot, "scratch", "media");
    const stagedPhotoPath = join(mediaRoot, ".factory", "inbox", "telegram", String(photoMessage.message_id), "photo-1.jpg");
    const photoMetadataPath = join(mediaRoot, ".factory", "inbox", "telegram", String(photoMessage.message_id), "message.json");
    expect(await readFile(stagedPhotoPath, "utf8")).toBe("fake image bytes");
    expect(await readFile(photoMetadataPath, "utf8")).toContain("\"attachedAsImage\": true");
    expect(await readFile(join(mediaRoot, ".factory", "control-plane.prompt.md"), "utf8")).toContain("Telegram inbound message:");
    expect(await readFile(join(mediaRoot, ".factory", "fake-images.txt"), "utf8")).toContain(
      `.factory/inbox/telegram/${photoMessage.message_id}/photo-1.jpg`
    );

    fixture.telegram.registerRemoteFile("doc-main", "docs/notes.txt", "document body");
    const documentMessage = telegramDocumentMessage("Check this document.", 60, "doc-main", "notes.txt");
    await fixture.commands.handleMessage(documentMessage);
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 2 for media.")));

    const stagedDocumentPath = join(mediaRoot, ".factory", "inbox", "telegram", String(documentMessage.message_id), "notes.txt");
    const documentMetadataPath = join(mediaRoot, ".factory", "inbox", "telegram", String(documentMessage.message_id), "message.json");
    expect(await readFile(stagedDocumentPath, "utf8")).toBe("document body");
    expect(await readFile(documentMetadataPath, "utf8")).toContain("\"attachedAsImage\": false");
    expect(await readFile(join(mediaRoot, ".factory", "fake-images.txt"), "utf8")).toBe("");

    const turnCountBeforeVoice = await readFile(join(mediaRoot, ".factory", "fake-turn-count"), "utf8");
    const voiceMessage = telegramVoiceMessage(60, "voice-main");
    await fixture.commands.handleMessage(voiceMessage);
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Audio and voice Telegram messages are not forwarded to Codex yet.");
    expect(await readFile(join(mediaRoot, ".factory", "fake-turn-count"), "utf8")).toBe(turnCountBeforeVoice);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("runCodex aborts quickly if the worktree deletes itself during execution", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx selfdestruct control scratch", 90));
    const context = fixture.db.getContextBySlug("selfdestruct");
    const worktreePath = context?.worktreePath;
    expect(worktreePath).toBeTruthy();

    await makeExecutable(
      fixture.fakeCodex,
      `#!/usr/bin/env bash
set -euo pipefail
cat >/dev/null
sleep 30
`
    );

    const startedAt = Date.now();
    const resultPromise = fixture.workers.runCodex(
      context!,
      "Remove everything.",
      "run",
      join(fixture.controlRoot, "logs", "selfdestruct.log")
    );
    await Bun.sleep(250);
    await rm(worktreePath!, { recursive: true, force: true });
    const result = await resultPromise;

    expect(result.ok).toBe(false);
    expect(result.exitCode).toBe(88);
    expect(result.stderr).toContain("worktree disappeared during harness run");
    expect(Date.now() - startedAt).toBeLessThan(10_000);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);
