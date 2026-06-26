import { afterEach, expect, test } from "bun:test";
import { chmod, mkdtemp, mkdir, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { CommandHandler } from "../src/commands";
import { loadConfig, ensureProjectPaths } from "../src/config";
import { CronManager } from "../src/cron-manager";
import { CronScheduler } from "../src/cron-scheduler";
import { ContextService } from "../src/contexts";
import { FactoryDb } from "../src/db";
import { Dispatcher, formatRunFailureForTelegram } from "../src/dispatcher";
import { classifyPreDispatch } from "../src/pre-dispatch-router";
import { ensureBasicLoops } from "../src/basic-loops";
import { flushBrainOutbox, postBrainImportOrQueue } from "../src/brain-outbox";
import { buildOutboxItem, canonicalJson, sha256Hex, writeOutboxItem } from "../../../packages/outbox/src/outbox";
import {
  parseArtifactEntries,
  removeArtifactEntriesFromMarkdown,
  resolveManifestRequests,
  type TelegramAttachmentKind
} from "../src/telegram-attachments";
import { TELEGRAM_MAX_INBOUND_FILE_BYTES } from "../src/telegram-inputs";
import type { TelegramMessage, TelegramTarget } from "../src/telegram";
import { WorkerService } from "../src/workers";

const originalSkipUserPathResolve = process.env.BRAINSTACK_SKIP_USER_PATH_RESOLVE;
const originalPath = process.env.PATH;

afterEach(() => {
  process.env.PATH = originalPath;
  if (originalSkipUserPathResolve === undefined) {
    delete process.env.BRAINSTACK_SKIP_USER_PATH_RESOLVE;
  } else {
    process.env.BRAINSTACK_SKIP_USER_PATH_RESOLVE = originalSkipUserPathResolve;
  }
});

class FakeTelegram {
  readonly sent: Array<{ target: TelegramTarget; text: string }> = [];
  readonly edited: Array<{ target: TelegramTarget; messageId: number; text: string }> = [];
  readonly attachments: Array<{
    target: TelegramTarget;
    kind: TelegramAttachmentKind;
    fileName: string;
    caption: string | null;
    text: string;
  }> = [];
  readonly actions: Array<{ target: TelegramTarget; action: string }> = [];
  readonly remoteFiles = new Map<string, { filePath: string; bytes: Uint8Array }>();
  readonly getFileFailures = new Map<string, string>();

  async sendText(target: TelegramTarget, text: string): Promise<void> {
    this.sent.push({ target, text });
  }

  async sendTextMessage(target: TelegramTarget, text: string): Promise<TelegramMessage> {
    this.sent.push({ target, text });
    return {
      message_id: nextMessageId++,
      date: Math.floor(Date.now() / 1000),
      text,
      is_topic_message: target.threadId !== null,
      message_thread_id: target.threadId ?? undefined,
      chat: {
        id: target.chatId,
        type: "supergroup"
      }
    };
  }

  async editText(target: TelegramTarget, messageId: number, text: string): Promise<void> {
    this.edited.push({ target, messageId, text });
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

  registerGetFileFailure(fileId: string, message: string): void {
    this.getFileFailures.set(fileId, message);
  }

  async getFile(fileId: string): Promise<{
    file_id: string;
    file_size: number;
    file_path: string;
  }> {
    const failure = this.getFileFailures.get(fileId);
    if (failure) {
      throw new Error(failure);
    }

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

function telegramTopicMessage(
  text: string,
  threadId: number,
  topicName: string,
  userId = TEST_ALLOWED_TELEGRAM_USER_ID
): TelegramMessage {
  return {
    ...telegramMessage(text, threadId, userId),
    reply_to_message: {
      message_thread_id: threadId,
      chat: {
        id: 4242,
        type: "supergroup",
        title: "Factory"
      },
      forum_topic_created: {
        name: topicName
      }
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

function telegramVoiceCaptionMessage(
  caption: string,
  threadId: number,
  fileId: string,
  userId = TEST_ALLOWED_TELEGRAM_USER_ID
): TelegramMessage {
  return {
    ...telegramVoiceMessage(threadId, fileId, userId),
    caption
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
  const fakeTranscriber = join(binDir, "fake-transcribe");
  const fakeBrainctl = join(binDir, "brainctl");
  const fakeBrainctlCalls = join(root, "brainctl-calls.txt");

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
if [[ "\${FACTORY_TEST_FAKE_SSH_REMOTE_COMMAND:-}" == "1" ]]; then
  remote_command="\${@: -1}"
  exec bash -lc "$remote_command"
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
    fakeTranscriber,
    `#!/usr/bin/env bash
set -euo pipefail
input=""
while (($#)); do
  case "$1" in
    -f|--file)
      input="$2"
      shift 2
      ;;
    *)
      input="$1"
      shift
      ;;
  esac
done
if [[ -z "$input" ]]; then
  echo "missing input" >&2
  exit 64
fi
printf 'transcribed: '
cat "$input"
`
  );

  await makeExecutable(
    fakeBrainctl,
    `#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> ${JSON.stringify(fakeBrainctlCalls)}
if [[ "$*" == *" fail-voice "* ]]; then
  echo "fake brainctl failure" >&2
  exit 2
fi
case "$*" in
  *"capabilities install voice"*)
    if [[ -n "\${FACTORY_TEST_BRAINCTL_INSTALL_SLEEP_SECONDS:-}" ]]; then
      sleep "\${FACTORY_TEST_BRAINCTL_INSTALL_SLEEP_SECONDS}"
    fi
    echo "installed=voice"
    echo "model=tiny.en command=/tmp/whisper-tiny.en.llamafile"
    echo "restart=scheduled service=telemux.service delay_ms=1500"
    echo "test=send a Telegram voice note"
    ;;
	  *"capabilities doctor voice"*)
	    echo "voice=ok enabled=yes target=erbine command=/tmp/whisper-tiny.en.llamafile"
	    ;;
	  *"capabilities uninstall voice"*)
	    echo "uninstalled=voice"
	    echo "files=removed target=erbine"
	    echo "restart=scheduled service=telemux.service delay_ms=1500"
	    ;;
  *"proposals groups"*)
    cat <<'JSON'
{"ok":true,"review_groups":[{"id":"brainstack:repo:project_lesson","label":"brainstack / repo / project_lesson","count":3,"needsContextCount":0,"legacyCount":0},{"id":"lindy:repo:project_lesson","label":"lindy / repo / project_lesson","count":2,"needsContextCount":1,"legacyCount":0}]}
JSON
    ;;
  *"proposals batch-merge"*)
    if [[ "$*" == *"--submit"* ]]; then
      cat <<'JSON'
{"dryRun":false,"harness":"codex","totalOpen":116,"inspected":100,"overflow":true,"autoThreshold":0.8,"candidates":2,"merged":[{"title":"Consolidate: brainstack proposal UI","confidence":0.86,"sourceIds":["p1","p2","p3"],"targetPage":"wiki/Syntheses/brainstack-curation-2026-06-20.md","autoMerged":true,"closed":["p1","p2","p3"],"writeStatus":"pending"},{"title":"Consolidate: lindy proposal context","confidence":0.72,"sourceIds":["p4","p5"],"targetPage":"wiki/Syntheses/lindy-context-20260620-lessons.md","autoMerged":false,"closed":[],"writeStatus":"needs-human"}],"skipped":[{"reason":"candidate has fewer than two known source ids"}],"warnings":["Only inspected the top 100 of 116 open proposals. Rerun after this batch to cover the rest."]}
JSON
    else
      cat <<'JSON'
{"dryRun":true,"harness":"codex","totalOpen":116,"inspected":100,"overflow":true,"autoThreshold":0.8,"candidates":1,"merged":[{"title":"Consolidate: brainstack proposal UI","confidence":0.86,"sourceIds":["p1","p2","p3"],"targetPage":"wiki/Syntheses/brainstack-curation-2026-06-20.md","autoMerged":true,"closed":[],"writeStatus":null}],"skipped":[],"warnings":["Only inspected the top 100 of 116 open proposals. Rerun after this batch to cover the rest."]}
JSON
    fi
    ;;
	  *)
	    echo "brainctl fake: $*"
	    ;;
esac
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
  if ("BRAINSTACK_SKIP_USER_PATH_RESOLVE" in envOverrides) {
    if (envOverrides.BRAINSTACK_SKIP_USER_PATH_RESOLVE) {
      process.env.BRAINSTACK_SKIP_USER_PATH_RESOLVE = envOverrides.BRAINSTACK_SKIP_USER_PATH_RESOLVE;
    } else {
      delete process.env.BRAINSTACK_SKIP_USER_PATH_RESOLVE;
    }
  } else {
    process.env.BRAINSTACK_SKIP_USER_PATH_RESOLVE = "1";
  }

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
    FACTORY_BRAINCTL_BIN: fakeBrainctl,
    BRAINSTACK_CONFIG: join(root, "brainstack.yaml"),
    FACTORY_TRANSCRIPTION_COMMAND: fakeTranscriber,
    FACTORY_TEXT_COALESCE_MS: envOverrides.FACTORY_TEXT_COALESCE_MS || "20",
    FACTORY_TELEGRAM_BOT_TOKEN: "test-token",
    FACTORY_ALLOWED_TELEGRAM_USER_ID: String(TEST_ALLOWED_TELEGRAM_USER_ID),
    BRAIN_BASE_URL: "",
    BRAIN_IMPORT_TOKEN: "",
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
    fakeTranscriber,
    fakeBrainctl,
    fakeBrainctlCalls,
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

function clearPendingTextTimers(fixture: Awaited<ReturnType<typeof createFixture>>): void {
  const pendingText = (fixture.commands as unknown as { pendingText: Map<string, { timer: ReturnType<typeof setTimeout> }> }).pendingText;
  for (const pending of pendingText.values()) {
    clearTimeout(pending.timer);
  }
  pendingText.clear();
}

function clearPendingMediaTimers(fixture: Awaited<ReturnType<typeof createFixture>>): void {
  const pendingMedia = (fixture.commands as unknown as { pendingMedia: Map<string, { timer: ReturnType<typeof setTimeout> }> }).pendingMedia;
  for (const pending of pendingMedia.values()) {
    clearTimeout(pending.timer);
  }
  pendingMedia.clear();
}

function setPendingTextGenerationForTest(fixture: Awaited<ReturnType<typeof createFixture>>, key: string, generationId: string): void {
  const rawDb = (fixture.db as unknown as { db: { query: (sql: string) => { run: (...args: unknown[]) => unknown } } }).db;
  rawDb.query("UPDATE pending_text_prompts SET generation_id = ? WHERE key = ?").run(generationId, key);
}

test("config accepts BRAIN_ADMIN_TOKEN as Telemux curator admin fallback", async () => {
  const fixture = await createFixture({
    BRAIN_ADMIN_TOKEN: "brain-admin-token",
    FACTORY_BRAIN_ADMIN_TOKEN: ""
  });
  try {
    expect(fixture.config.brainAdminToken).toBe("brain-admin-token");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
});

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

test("unbound context commands explain and guide deterministic newctx binding", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramTopicMessage("/explainctx", 43, "Proposal curation"));
    const explainText = fixture.telegram.sent.at(-1)?.text || "";
    expect(explainText).toContain("This topic is not bound yet.");
    expect(explainText).toContain("A Brainstack context is the durable workspace");
    expect(explainText).toContain("Suggested slug: proposal-curation");
    expect(explainText).toContain("Slug source: Telegram topic title");
    expect(explainText).toContain("Run /curation to bind it automatically.");
    expect(explainText).toContain("/newctx proposal-curation <machine> scratch");
    expect(explainText).toContain("1) control");
    expect(explainText).toContain("2) worker1");

    await fixture.commands.handleMessage(telegramTopicMessage("/newctx", 43, "Proposal curation"));
    const startText = fixture.telegram.sent.at(-1)?.text || "";
    expect(startText).toContain("Let's bind this Telegram topic to a Brainstack context.");
    expect(startText).toContain("Suggested slug: proposal-curation");
    expect(startText).toContain("To change it, reply with a word or phrase now. To keep it, pick a machine:");

    await fixture.commands.handleMessage(telegramMessage("Proposal curation v2", 43));
    const slugChangedText = fixture.telegram.sent.at(-1)?.text || "";
    expect(slugChangedText).toContain("Slug: proposal-curation-v2");
    expect(slugChangedText).toContain("Pick a machine");

    await fixture.commands.handleMessage(telegramMessage("1", 43));
    const targetPrompt = fixture.telegram.sent.at(-1)?.text || "";
    expect(targetPrompt).toContain("Machine: control");
    expect(targetPrompt).toContain("1) Topic workspace");
    expect(targetPrompt).toContain("ongoing conversation, routines, proposal review");
    expect(targetPrompt).toContain("2) Machine administration");
    expect(targetPrompt).toContain("/newctx proposal-curation-v2 control scratch");

    await fixture.commands.handleMessage(telegramMessage("1", 43));
    const created = fixture.db.getContextBySlug("proposal-curation-v2");
    expect(created?.kind).toBe("scratch");
    expect(created?.machine).toBe("control");
    expect(created?.state).toBe("active");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Context proposal-curation-v2 bound to this topic.");

    await fixture.commands.handleMessage(telegramTopicMessage("/newctx", 44, "Fast path"));
    await fixture.commands.handleMessage(telegramMessage("1", 44));
    await fixture.commands.handleMessage(telegramMessage("topic", 44));
    const fastPath = fixture.db.getContextBySlug("fast-path");
    expect(fastPath?.kind).toBe("scratch");
    expect(fastPath?.machine).toBe("control");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("newctx accepts interactive repo targets and GitHub org/repo shorthand", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramTopicMessage("/newctx", 45, "Erbine Lindy"));
    await fixture.commands.handleMessage(telegramMessage("2", 45));
    const targetPrompt = fixture.telegram.sent.at(-1)?.text || "";
    expect(targetPrompt).toContain("3) Code repository/path");
    expect(targetPrompt).toContain("org/repo");

    await fixture.commands.handleMessage(telegramMessage("3", 45));
    const repoPrompt = fixture.telegram.sent.at(-1)?.text || "";
    expect(repoPrompt).toContain("Send the repository or path for this topic.");
    expect(repoPrompt).toContain("lindy-ai/lindy main");

    await fixture.commands.handleMessage(telegramMessage("Lindy-ai/lindy main", 45));
    const wizardRepo = fixture.db.getContextBySlug("erbine-lindy");
    expect(wizardRepo?.kind).toBe("repo");
    expect(wizardRepo?.machine).toBe("worker1");
    expect(wizardRepo?.target).toBe("https://github.com/Lindy-ai/lindy.git");
    expect(wizardRepo?.baseBranch).toBe("main");
    expect(wizardRepo?.state).toBe("pending");
    expect(fixture.telegram.sent.at(-2)?.text).toContain("preparing the repo workspace");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Context erbine-lindy bound to this topic.");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Workspace: bound, but setup is not ready yet");

    await fixture.commands.handleMessage(telegramMessage("/newctx erbine-lindy-retry worker1 lindy-ai/lindy main", 45));
    const retryText = fixture.telegram.sent.at(-1)?.text || "";
    expect(retryText).toContain("Replaced previous unfinished setup erbine-lindy with erbine-lindy-retry.");
    expect(retryText).toContain("Future messages in this topic will use the new context.");
    expect(retryText).not.toContain("Warning: this topic is already bound.");
    expect(fixture.db.getContextByTopic(4242, 45)?.slug).toBe("erbine-lindy-retry");

    await fixture.commands.handleMessage(telegramTopicMessage("/newctx", 46, "Repo Direct"));
    await fixture.commands.handleMessage(telegramMessage("2", 46));
    await fixture.commands.handleMessage(telegramMessage("repo lindy-ai/lindy main", 46));
    const directWizardRepo = fixture.db.getContextBySlug("repo-direct");
    expect(directWizardRepo?.target).toBe("https://github.com/lindy-ai/lindy.git");
    expect(directWizardRepo?.baseBranch).toBe("main");

    await fixture.commands.handleMessage(telegramMessage("/newctx one-line worker1 lindy-ai/lindy main", 47));
    const oneLineRepo = fixture.db.getContextBySlug("one-line");
    expect(oneLineRepo?.target).toBe("https://github.com/lindy-ai/lindy.git");
    expect(oneLineRepo?.baseBranch).toBe("main");

    await fixture.commands.handleMessage(telegramMessage("/newctx bind-shorthand control scratch", 48));
    await fixture.commands.handleMessage(telegramMessage("/bind worker1 lindy-ai/lindy main", 48));
    const reboundRepo = fixture.db.getContextBySlug("bind-shorthand");
    expect(reboundRepo?.machine).toBe("worker1");
    expect(reboundRepo?.target).toBe("https://github.com/lindy-ai/lindy.git");
    expect(reboundRepo?.baseBranch).toBe("main");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("curation command binds the current topic and owns exactly one curator routine", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramTopicMessage("/curation", 45, "Proposal curation"));
    const context = fixture.db.getContextBySlug("proposal-curation");
    expect(context?.kind).toBe("scratch");
    expect(context?.machine).toBe("control");
    expect(context?.telegramThreadId).toBe(45);

    const jobs = fixture.db.listCronJobs().filter((job) => job.label === "brain-curator");
    expect(jobs.length).toBe(1);
    expect(jobs[0]?.executionContextSlug).toBe("proposal-curation");
    expect(jobs[0]?.targetThreadId).toBe(45);
    expect(jobs[0]?.instruction).toContain("shared-brain curator pass");
    const setupText = fixture.telegram.sent.at(-1)?.text || "";
    expect(setupText).toContain("Proposal curation is ready in this topic.");
    expect(setupText).toContain("Brain-curator routine installed:");
    expect(setupText).toContain("/proposals pending");
    expect(setupText).toContain("/proposals needs-human needs-context");

    await fixture.commands.handleMessage(telegramTopicMessage("/curation", 46, "Proposal curation v2"));
    const movedContext = fixture.db.getContextBySlug("proposal-curation");
    expect(movedContext?.telegramThreadId).toBe(46);
    const movedJobs = fixture.db.listCronJobs().filter((job) => job.label === "brain-curator");
    expect(movedJobs.length).toBe(1);
    expect(movedJobs[0]?.executionContextSlug).toBe("proposal-curation");
    expect(movedJobs[0]?.targetThreadId).toBe(46);
    expect(fixture.telegram.sent.at(-1)?.text || "").toContain("Brain-curator routine ready:");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
});

test("dispatcher emits a throttled editable progress card for slow Codex runs", async () => {
  const fixture = await createFixture({
    FACTORY_HARNESS_STREAMING: "status",
    FACTORY_HARNESS_STREAMING_INITIAL_DELAY_MS: "1",
    FACTORY_HARNESS_STREAMING_UPDATE_INTERVAL_MS: "10"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx live-progress control scratch", 49));
    await fixture.commands.handleMessage(telegramMessage("/run slow live session progress test", 49));

    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Working on live-progress (run)")));
    await waitFor(() => fixture.telegram.edited.some((entry) => entry.text.includes("Completed. Final response below.")));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for live-progress.")));

    const progressText = fixture.telegram.sent.find((entry) => entry.text.includes("Working on live-progress (run)"))?.text || "";
    expect(progressText).toContain("Machine: control");
    expect(progressText).not.toContain("Authorization");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

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
    expect(scratchpad?.reasoningEffortOverride).toBe("low");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Codex mode: custom (model=default effort=low)");
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
}, 15_000);

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
    clearPendingTextTimers(fixture);
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
    clearPendingTextTimers(fixture);
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
    clearPendingTextTimers(fixture);
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
    clearPendingTextTimers(fixture);
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
    clearPendingTextTimers(fixture);
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
    expect(curator?.instruction).toContain("Do not call `brainctl curator run`");
    expect(curator?.instruction).toContain("Preserve raw imports and proposals");
    expect(curator?.instruction).toContain("proposals auto-merge");

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

test("basic loops install the brain-curator routine and curator commands work end to end", async () => {
  const statusPosts: Array<{ auth: string | null; body: Record<string, unknown> }> = [];
  const decisions: Array<{ path: string; auth: string | null; body: Record<string, unknown> }> = [];
  const proposalStatusQueries: string[] = [];
  const port = 35_000 + Math.floor(Math.random() * 5_000);
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port,
    async fetch(req) {
      const url = new URL(req.url);
      if (req.method === "GET" && url.pathname === "/api/curator/status") {
        return Response.json({
          ok: true,
          mode: "approval",
          curator: {
            installed: true,
            last_run_finished_at: "2026-06-10T06:30:00Z",
            last_run_ok: true,
            last_run_failures: [],
            cursor: "2026-06-10T06:30:00Z"
          },
          proposal_counts: { pending: 2, approved: 0, applied: 1, rejected: 0, superseded: 0, "needs-human": 1 }
        });
      }
      if (req.method === "POST" && url.pathname === "/api/curator/status") {
        statusPosts.push({ auth: req.headers.get("authorization"), body: (await req.json()) as Record<string, unknown> });
        return Response.json({ ok: true });
      }
      if (req.method === "GET" && url.pathname === "/api/proposals") {
        proposalStatusQueries.push(url.searchParams.get("status") || "");
        return Response.json({
          ok: true,
          mode: "approval",
          proposals: [
            {
              id: "20260610t060000z-status-update",
              title: "Status update",
              status: "pending",
              target_page: "wiki/Status/Machines.md",
              risk: "low",
              created_at: "2026-06-10T06:00:00Z"
            },
            {
              id: "20260610t061000z-note-only",
              title: "Note only",
              status: "pending",
              target_page: null,
              risk: null,
              created_at: "2026-06-10T06:10:00Z"
            },
            {
              id: "20260610t062000z-needs-context-redis",
              title: "Needs context Redis cluster note",
              status: "needs-human",
              target_page: null,
              risk: null,
              created_at: "2026-06-10T06:20:00Z",
              quality_decision: "needs-context",
              project: "Lindy",
              scope: "repo",
              memory_kind: "project_lesson",
              cluster_key: "lindy:repo:project_lesson",
              cluster_label: "Lindy / repo / project_lesson",
              legacy_format: true
            }
          ]
        });
      }
      if (req.method === "POST" && url.pathname === "/api/import") {
        return Response.json({ ok: true, artifact_id: "curator-run-notes" });
      }
      const decision = url.pathname.match(/^\/api\/proposals\/([^/]+)\/(approve|reject|apply)$/);
      if (req.method === "POST" && decision) {
        decisions.push({ path: url.pathname, auth: req.headers.get("authorization"), body: (await req.json()) as Record<string, unknown> });
        return Response.json({ ok: true, proposal_id: decision[1], action: decision[2], status: decision[2] === "reject" ? "rejected" : "applied", commit: "abc1234" });
      }
      return Response.json({ error: `unexpected ${req.method} ${url.pathname}` }, { status: 500 });
    }
  });
  const curatorStateRoot = await mkdtemp(join(tmpdir(), "telemux-curator-state-"));
  const fixture = await createFixture({
    FACTORY_TELEGRAM_CONTROL_CHAT_ID: "4242",
    BRAINSTACK_STATE_ROOT: curatorStateRoot,
    BRAIN_BASE_URL: `http://127.0.0.1:${port}`,
    BRAIN_IMPORT_TOKEN: "brain-import-token",
    FACTORY_BRAIN_ADMIN_TOKEN: "brain-admin-token"
  });

  try {
    const result = await ensureBasicLoops(fixture.config, fixture.contexts, fixture.workers, fixture.cronManager);
    expect(result).toContain("curator created:");
    expect(statusPosts.length).toBeGreaterThanOrEqual(1);
    expect(statusPosts.at(-1)?.auth).toBe("Bearer brain-admin-token");
    expect(statusPosts.at(-1)?.body.installed).toBe(true);
    expect(typeof statusPosts.at(-1)?.body.next_run_at).toBe("string");
    const curatorJob = fixture.db.listCronJobs().find((job) => job.label === "brain-curator");
    expect(curatorJob?.kind).toBe("codex");
    expect(curatorJob?.runner).toBeNull();
    expect(curatorJob?.executionContextSlug).toBe("brainstack-routines");
    expect(curatorJob?.schedule.type).toBe("daily");

    // Re-running basic loops updates the existing curator job instead of duplicating it.
    const second = await ensureBasicLoops(fixture.config, fixture.contexts, fixture.workers, fixture.cronManager);
    expect(second).toContain("curator updated:");
    expect(statusPosts.length).toBeGreaterThanOrEqual(2);
    expect(statusPosts.at(-1)?.body.installed).toBe(true);
    expect(fixture.db.listCronJobs().filter((job) => job.label === "brain-curator").length).toBe(1);

    await fixture.commands.handleMessage(telegramMessage("/curator_status", 90));
    const statusText = fixture.telegram.sent.at(-1)?.text || "";
    expect(statusText).toContain("Curator installed: yes");
    expect(statusText).toContain("Mode: approval");
    expect(statusText).toContain("needs-human");

    await fixture.commands.handleMessage(telegramMessage("/proposals", 90));
    const listText = fixture.telegram.sent.at(-1)?.text || "";
    expect(proposalStatusQueries.at(-1)).toBe("open");
    expect(listText).toContain("Status update");
    const shortcut = listText.match(/\/proposal_accept_([a-z0-9]{6,10})_1/);
    expect(shortcut).not.toBeNull();
    const token = shortcut![1];
    expect(listText).not.toContain(`/proposal_accept_${token}_2`);
    expect(listText).toContain("merge/enrich first");

    await fixture.commands.handleMessage(telegramMessage("/proposals needs-human needs-context project:lindy scope:repo group:lindy kind:project_lesson limit=5", 90));
    const filteredText = fixture.telegram.sent.at(-1)?.text || "";
    expect(proposalStatusQueries.at(-1)).toBe("needs-human");
    expect(filteredText).toContain("Needs context Redis cluster note");
    expect(filteredText).toContain("quality=needs-context");
    expect(filteredText).toContain("project=Lindy");
    expect(filteredText).toContain("kind=project_lesson");
    expect(filteredText).not.toContain("Status update");

    await fixture.commands.handleMessage(telegramMessage("/proposal_groups", 90));
    const groupText = fixture.telegram.sent.at(-1)?.text || "";
    expect(groupText).toContain("Proposal merge candidates");
    expect(groupText).toContain("brainstack / repo / project_lesson");
    expect(groupText).toContain("Say \"look for proposal merges\"");

    await fixture.commands.handleMessage(telegramMessage("/proposal_merges preview", 90));
    const previewText = fixture.telegram.sent.at(-1)?.text || "";
    expect(previewText).toContain("Proposal merge preview complete");
    expect(previewText).toContain("would create 1 consolidated proposal");
    expect(previewText).toContain("Harness inspected 100/116");
    expect(previewText).toContain("No proposals were changed");

    await fixture.commands.handleMessage(telegramMessage("look for proposal merges", 90));
    const mergeText = fixture.telegram.sent.at(-1)?.text || "";
    expect(mergeText).toContain("Proposal merge scan complete");
    expect(mergeText).toContain("created 2 consolidated proposal");
    expect(mergeText).toContain("1 auto-merge, 1 needs review");
    expect(mergeText).toContain("Rerun after this batch");
    expect(mergeText).toContain("No wiki edits were applied");
    const brainctlCalls = await readFile(fixture.fakeBrainctlCalls, "utf8");
    expect(brainctlCalls).toContain("proposals groups --status open --min-size 2 --json --config");
    expect(brainctlCalls).toContain("proposals batch-merge --limit 100 --auto-threshold 0.8 --json --config");
    expect(brainctlCalls).toContain("proposals batch-merge --submit --limit 100 --auto-threshold 0.8 --json --config");

    // Accepting a proposal that carries a wiki change applies it.
    await fixture.commands.handleMessage(telegramMessage(`/proposal_accept_${token}_1`, 90));
    await waitFor(() => decisions.length === 1);
    expect(decisions[0].path).toBe("/api/proposals/20260610t060000z-status-update/apply");
    expect(decisions[0].auth).toBe("Bearer brain-admin-token");
    expect(String(decisions[0].body.decided_by)).toContain("telegram:");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("status=applied");

    // Context-only proposals need enrichment or group merge before Accept can apply them.
    await fixture.commands.handleMessage(telegramMessage(`/proposal_accept_${token}_2`, 90));
    expect(decisions.length).toBe(1);
    expect(fixture.telegram.sent.at(-1)?.text).toContain("no wiki change attached");

    await fixture.commands.handleMessage(telegramMessage(`/proposal_reject_${token}_1`, 90));
    await waitFor(() => decisions.length === 2);
    expect(decisions[1].path).toBe("/api/proposals/20260610t060000z-status-update/reject");

    // Manual curator run dispatches the codex routine and reports status to braind.
    await fixture.commands.handleMessage(telegramMessage("/curator_run", 90));
    await waitFor(() => statusPosts.length >= 3, 20_000);
    const reported = statusPosts.at(-1)!;
    expect(reported.auth).toBe("Bearer brain-admin-token");
    expect(reported.body.installed).toBe(true);
    expect(reported.body.last_run_ok).toBe(true);
    expect(typeof reported.body.cursor).toBe("string");
  } finally {
    server.stop(true);
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
    await rm(curatorStateRoot, { recursive: true, force: true });
  }
}, 30_000);

test("telegram proposal decisions are refused without the brain admin token", async () => {
  const port = 35_000 + Math.floor(Math.random() * 5_000);
  let decisionAttempts = 0;
  const server = Bun.serve({
    hostname: "127.0.0.1",
    port,
    fetch(req) {
      const url = new URL(req.url);
      if (req.method === "GET" && url.pathname === "/api/proposals") {
        return Response.json({
          ok: true,
          mode: "approval",
          proposals: [
            {
              id: "20260610t060000z-status-update",
              title: "Status update",
              status: "pending",
              target_page: "wiki/Status/Machines.md",
              risk: "low",
              created_at: "2026-06-10T06:00:00Z"
            }
          ]
        });
      }
      if (req.method === "POST") {
        decisionAttempts += 1;
      }
      return Response.json({ error: "unexpected" }, { status: 500 });
    }
  });
  const fixture = await createFixture({
    FACTORY_TELEGRAM_CONTROL_CHAT_ID: "4242",
    BRAIN_BASE_URL: `http://127.0.0.1:${port}`,
    BRAIN_IMPORT_TOKEN: "brain-import-token"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/proposals", 91));
    const listText = fixture.telegram.sent.at(-1)?.text || "";
    expect(listText).toContain("FACTORY_BRAIN_ADMIN_TOKEN is not set");
    const token = listText.match(/\/proposal_accept_([a-z0-9]{6,10})_1/)![1];

    await fixture.commands.handleMessage(telegramMessage(`/proposal_accept_${token}_1`, 91));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("accept/reject from Telegram is disabled");
    expect(decisionAttempts).toBe(0);
  } finally {
    server.stop(true);
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

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
      stdout: worker.name === "worker1"
        ? "runtime_harness=claude\nruntime_harness_bin=/remote/bin/claude\nruntime_model=opus\nruntime_effort=n/a\nok"
        : "ok",
      stderr: "",
      durationMs: 1,
      commandLabel: "update-check"
    });

    const result = await fixture.workers.runUpdateCheck(context);
    workerInternals.runUpdateCheckProbe = originalProbe;
    expect(result.ok).toBe(true);
    expect(result.stdout).toContain("## seen-only");
    expect(result.stdout).toContain("## worker1");
    expect(result.stdout).toContain("- harness: claude");
    expect(result.stdout).toContain("- harness_bin: /remote/bin/claude");
    expect(result.stdout).toContain("- model: opus");
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
    expect(fixture.telegram.sent.find((entry) => entry.text.includes("Reply turn 1 for compact-lab."))?.text).toContain(
      "tokens=15 (in=10 cached=0 out=5)"
    );
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("Dispatched resume for compact-lab."))).toBe(false);

    await fixture.commands.handleMessage(telegramMessage("/context", 90));
    const contextText = fixture.telegram.sent.at(-1)?.text || "";
    expect(contextText).toContain("Context: compact-lab");
    expect(contextText).toContain("Harness: codex");
    expect(contextText).toContain("Model: default");
    expect(contextText).toContain("Thinking effort: low");

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

test("usage adapter config is manual-only and legacy context adapters are explicit", async () => {
  expect(() => loadConfig({ ...process.env, FACTORY_USAGE_ADAPTER: "openai_codex" })).toThrow(
    "Unsupported FACTORY_USAGE_ADAPTER=openai_codex"
  );
  expect(() => loadConfig({ ...process.env, FACTORY_USAGE_ADAPTER: "typo" })).toThrow(
    "Unsupported FACTORY_USAGE_ADAPTER=typo"
  );

  const fixture = await createFixture();
  try {
    const context = fixture.contexts.createOrUpdateContext({
      slug: "legacy-usage",
      machine: "control",
      kind: "scratch",
      state: "active",
      transport: "local",
      target: "scratch",
      rootPath: join(fixture.factoryRoot, "scratch", "legacy-usage"),
      worktreePath: join(fixture.factoryRoot, "scratch", "legacy-usage"),
      branchName: null,
      baseBranch: null,
      usageAdapter: "openai_codex",
      chatId: null,
      threadId: null
    });
    fixture.contexts.bindContext(context.slug, 4242, 93);

    await fixture.commands.handleMessage(telegramMessage("/usage", 93));
    const usageText = fixture.telegram.sent.at(-1)?.text || "";
    expect(usageText).toContain("Context: legacy-usage");
    expect(usageText).toContain("Unsupported usage adapter: openai_codex");
    expect(usageText).toContain("Brainstack only supports the manual local run-log parser now.");
    expect(usageText).not.toContain("Turns counted:");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 30_000);

test("plain-text pre-dispatch answers liveness and usage locally without starting the harness", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "20"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx meta-local control scratch", 94));
    const worktree = join(fixture.factoryRoot, "scratch", "meta-local");
    const turnFile = join(worktree, ".factory", "fake-turn-count");
    const promptFile = join(worktree, ".factory", "control-plane.prompt.md");

    const beforeLiveness = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("You up?", 94));
    await waitFor(() =>
      fixture.telegram.sent.slice(beforeLiveness).some((entry) => entry.text.includes("Handled locally by telemux"))
    );
    const livenessText = fixture.telegram.sent.at(-1)?.text || "";
    expect(livenessText).toContain("Up.");
    expect(livenessText).toContain("Context: meta-local");
    expect(livenessText).toContain("no harness run was started");
    expect(await Bun.file(turnFile).exists()).toBe(false);
    expect(await Bun.file(promptFile).exists()).toBe(false);

    const beforeAck = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("thanks", 94));
    await waitFor(() => fixture.telegram.sent.slice(beforeAck).some((entry) => entry.text.includes("Noted.")));
    const ackText = fixture.telegram.sent.at(-1)?.text || "";
    expect(ackText).toContain("Handled locally by telemux");
    expect(await Bun.file(turnFile).exists()).toBe(false);
    expect(await Bun.file(promptFile).exists()).toBe(false);

    const beforeUsage = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("How many tokens does this query spend?", 94));
    await waitFor(() => fixture.telegram.sent.slice(beforeUsage).some((entry) => entry.text.includes("Usage check.")));
    const usageText = fixture.telegram.sent.at(-1)?.text || "";
    expect(usageText).toContain("Handled locally by telemux");
    expect(usageText).toContain("Latest completed harness run");
    expect(usageText).toContain("No local run log recorded yet.");
    expect(await Bun.file(turnFile).exists()).toBe(false);
    expect(await Bun.file(promptFile).exists()).toBe(false);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("plain-text pre-dispatch uses a light prompt for informational questions and full prompts for work", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "20"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx light-route control scratch", 95));
    await fixture.commands.handleMessage(telegramMessage("What were we doing?", 95));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for light-route.")));
    const lightPrompt = await readFile(join(fixture.factoryRoot, "scratch", "light-route", ".factory", "control-plane.prompt.md"), "utf8");
    expect(lightPrompt).toContain("Treat this as a lightweight informational turn.");
    expect(lightPrompt).toContain("You may perform read-only inspection when needed to answer");
    expect(lightPrompt).toContain("What were we doing?");
    expect(lightPrompt).not.toContain("Start by reading those files and the current git status.");
    expect(lightPrompt).not.toContain("Before finishing, update all relevant .factory files");

    await fixture.commands.handleMessage(telegramMessage("/newctx full-route control scratch", 96));
    await fixture.commands.handleMessage(telegramMessage("Fix tests.", 96));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for full-route.")));
    const fullPrompt = await readFile(join(fixture.factoryRoot, "scratch", "full-route", ".factory", "control-plane.prompt.md"), "utf8");
    expect(fullPrompt).toContain("Start by reading those files and the current git status.");
    expect(fullPrompt).toContain("Before finishing, update all relevant .factory files");
    expect(fullPrompt).toContain("Fix tests.");

    await fixture.commands.handleMessage(telegramMessage("/newctx token-work-route control scratch", 99));
    const beforeTokenWork = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("Can you update the token budget?", 99));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for token-work-route.")));
    expect(fixture.telegram.sent.slice(beforeTokenWork).some((entry) => entry.text.includes("Usage check."))).toBe(false);
    const tokenWorkPrompt = await readFile(
      join(fixture.factoryRoot, "scratch", "token-work-route", ".factory", "control-plane.prompt.md"),
      "utf8"
    );
    expect(tokenWorkPrompt).toContain("Start by reading those files and the current git status.");

    await fixture.commands.handleMessage(telegramMessage("/newctx latency-work-route control scratch", 100));
    const beforeLatencyWork = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("What would it take to add dark mode?", 100));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for latency-work-route.")));
    expect(fixture.telegram.sent.slice(beforeLatencyWork).some((entry) => entry.text.includes("Latest run diagnostics."))).toBe(false);
    const latencyWorkPrompt = await readFile(
      join(fixture.factoryRoot, "scratch", "latency-work-route", ".factory", "control-plane.prompt.md"),
      "utf8"
    );
    expect(latencyWorkPrompt).toContain("Start by reading those files and the current git status.");

    await fixture.commands.handleMessage(telegramMessage("/newctx read-usage-route control scratch", 101));
    const beforeReadUsage = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("Tail the log, how many tokens did that take?", 101));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for read-usage-route.")));
    expect(fixture.telegram.sent.slice(beforeReadUsage).some((entry) => entry.text.includes("Usage check."))).toBe(false);
    const readUsagePrompt = await readFile(
      join(fixture.factoryRoot, "scratch", "read-usage-route", ".factory", "control-plane.prompt.md"),
      "utf8"
    );
    expect(readUsagePrompt).toContain("Start by reading those files and the current git status.");

    await fixture.commands.handleMessage(telegramMessage("/newctx investigate-slow-route control scratch", 102));
    const beforeInvestigateSlow = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("Can you investigate why this is so slow?", 102));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for investigate-slow-route.")));
    expect(fixture.telegram.sent.slice(beforeInvestigateSlow).some((entry) => entry.text.includes("Latest run diagnostics."))).toBe(false);
    const investigateSlowPrompt = await readFile(
      join(fixture.factoryRoot, "scratch", "investigate-slow-route", ".factory", "control-plane.prompt.md"),
      "utf8"
    );
    expect(investigateSlowPrompt).toContain("Can you investigate why this is so slow?");

    await fixture.commands.handleMessage(telegramMessage("/newctx review-usage-route control scratch", 103));
    const beforeReviewUsage = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("Can you review the token usage?", 103));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for review-usage-route.")));
    expect(fixture.telegram.sent.slice(beforeReviewUsage).some((entry) => entry.text.includes("Usage check."))).toBe(false);
    const reviewUsagePrompt = await readFile(
      join(fixture.factoryRoot, "scratch", "review-usage-route", ".factory", "control-plane.prompt.md"),
      "utf8"
    );
    expect(reviewUsagePrompt).toContain("Can you review the token usage?");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("coalesced mixed meta and work text fails open to the full work prompt", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "80"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx mixed-route control scratch", 97));
    await fixture.commands.handleMessage(telegramMessage("You up?", 97));
    await fixture.commands.handleMessage(telegramMessage("Fix tests.", 97));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for mixed-route.")));
    const prompt = await readFile(join(fixture.factoryRoot, "scratch", "mixed-route", ".factory", "control-plane.prompt.md"), "utf8");
    expect(prompt).toContain("You up?\n\nFix tests.");
    expect(prompt).toContain("Start by reading those files and the current git status.");
    expect(prompt).not.toContain("Treat this as a lightweight informational turn.");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("optional LLM pre-dispatch classifier is advisory and fails open", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx classifier-policy control scratch", 98));
    const context = fixture.db.getContextBySlug("classifier-policy");
    expect(context).toBeTruthy();

    const classifier = {
      ...fixture.config.preDispatchClassifier,
      enabled: true,
      apiKey: "test-key",
      model: "cheap-test-model",
      timeoutMs: 50,
      maxChars: 600,
      confidenceThreshold: 0.75
    };
    const bodies = [
      { output_text: '{"route":"light_harness","confidence":0.91,"reason":"short informational question"}' },
      { output_text: '{"route":"control_meta","controlKind":"usage","confidence":"0.93","reason":"usage question"}' },
      {
        output: [
          {
            content: [
              {
                text: {
                  value: '{"route":"light_harness","confidence":0.88,"reason":"nested response text"}'
                }
              }
            ]
          }
        ]
      },
      { output_text: '{"route":"light_harness","confidence":0.2,"reason":"weak guess"}' },
      { output_text: "not json" },
      { status: "incomplete", output: [] }
    ];
    let calls = 0;
    const fetcher: typeof fetch = async () => {
      const body = bodies[calls] || bodies.at(-1)!;
      calls += 1;
      return new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      });
    };

    const llmLight = await classifyPreDispatch({
      text: "Any concerns?",
      context: context!,
      hasAttachments: false,
      classifier,
      fetcher
    });
    expect(llmLight.route).toBe("light_harness");
    expect(llmLight.source).toBe("llm");
    expect(calls).toBe(1);

    const llmUsage = await classifyPreDispatch({
      text: "Costs ran hot earlier",
      context: context!,
      hasAttachments: false,
      classifier,
      fetcher
    });
    expect(llmUsage.route).toBe("control_meta");
    expect(llmUsage.source).toBe("llm");
    expect(llmUsage.controlKind).toBe("usage");
    expect(calls).toBe(2);

    const nestedText = await classifyPreDispatch({
      text: "Talk through alternatives",
      context: context!,
      hasAttachments: false,
      classifier,
      fetcher
    });
    expect(nestedText.route).toBe("light_harness");
    expect(nestedText.source).toBe("llm");
    expect(calls).toBe(3);

    const deterministicFull = await classifyPreDispatch({
      text: "Fix the bug in src/main.ts",
      context: context!,
      hasAttachments: false,
      classifier,
      fetcher
    });
    expect(deterministicFull.route).toBe("full_harness");
    expect(deterministicFull.source).toBe("deterministic");
    expect(calls).toBe(3);

    const lowConfidence = await classifyPreDispatch({
      text: "Maybe talk through options",
      context: context!,
      hasAttachments: false,
      classifier,
      fetcher
    });
    expect(lowConfidence.route).toBe("full_harness");
    expect(lowConfidence.source).toBe("fallback");
    expect(calls).toBe(4);

    const invalidOutput = await classifyPreDispatch({
      text: "A vague note",
      context: context!,
      hasAttachments: false,
      classifier,
      fetcher
    });
    expect(invalidOutput.route).toBe("full_harness");
    expect(invalidOutput.source).toBe("fallback");
    expect(calls).toBe(5);

    const incompleteOutput = await classifyPreDispatch({
      text: "A second vague note",
      context: context!,
      hasAttachments: false,
      classifier,
      fetcher
    });
    expect(incompleteOutput.route).toBe("full_harness");
    expect(incompleteOutput.source).toBe("fallback");
    expect(calls).toBe(6);

    const attachmentFull = await classifyPreDispatch({
      text: "Any concerns?",
      context: context!,
      hasAttachments: true,
      classifier,
      fetcher
    });
    expect(attachmentFull.route).toBe("full_harness");
    expect(attachmentFull.source).toBe("deterministic");
    expect(calls).toBe(6);

    const configWithoutDedicatedKey = loadConfig({
      ...process.env,
      FACTORY_CONTROL_ROOT: fixture.controlRoot,
      FACTORY_FACTORY_ROOT: fixture.factoryRoot,
      FACTORY_WORKERS_FILE: fixture.workersFile,
      FACTORY_PRE_DISPATCH_CLASSIFIER: "1",
      FACTORY_PRE_DISPATCH_CLASSIFIER_API_KEY: "",
      OPENAI_API_KEY: "ambient-key-must-not-be-used"
    });
    expect(configWithoutDedicatedKey.preDispatchClassifier.apiKey).toBe("");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("manual compaction failures are classified without false positives from normal compact text", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx compact-failure control scratch", 93));
    const context = fixture.db.getContextBySlug("compact-failure");
    expect(context).toBeTruthy();
    fixture.contexts.saveContext({
      ...context!,
      codexSessionId: "session-compact-failure"
    });

    await makeExecutable(
      fixture.fakeCodex,
      `#!/usr/bin/env bash
if (($# >= 2)) && [[ "$1" == "exec" && "$2" == "--help" ]]; then
  echo '--dangerously-bypass-approvals-and-sandbox --output-last-message --skip-git-repo-check'
  exit 0
fi
if (($# >= 1)) && [[ "$1" == "--version" ]]; then
  echo 'codex fake'
  exit 0
fi
echo 'ordinary run mentioned compact and failed' >&2
exit 31
`
    );

    const normalFailureText = formatRunFailureForTelegram("ordinary run mentioned compact and failed", "/tmp/normal.log", false);
    expect(normalFailureText).toBe("ordinary run mentioned compact and failed");
    expect(normalFailureText).not.toContain("Codex compaction failed.");

    const beforeManualFailureCount = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("/compact", 93));
    await waitFor(
      () => fixture.telegram.sent.slice(beforeManualFailureCount).some((entry) => entry.text.includes("Codex compaction failed.")),
      45_000
    );
    const manualFailureText = fixture.telegram.sent.slice(beforeManualFailureCount).map((entry) => entry.text).join("\n\n");
    expect(manualFailureText).toContain("Compacting thread…");
    expect(manualFailureText).toContain("Log:");
    expect(manualFailureText).toContain("ordinary run mentioned compact and failed");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 70_000);

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

    const broadFileTaskAttachmentCount = fixture.telegram.attachments.length;
    const beforeBroadFileTask = fixture.telegram.sent.length;
    await fixture.commands.handleMessage(telegramMessage("Upload the new config.json to the worker", 67));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 2 for bareartifact.")));
    await waitFor(() => !fixture.dispatcher.isActive("bareartifact"));
    expect(fixture.telegram.attachments).toHaveLength(broadFileTaskAttachmentCount);
    expect(fixture.telegram.sent.slice(beforeBroadFileTask).some((entry) => entry.text.includes("No artifact file paths matched"))).toBe(false);
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
  const previousSkipPathResolve = process.env.BRAINSTACK_SKIP_USER_PATH_RESOLVE;
  const fixture = await createFixture({
    FACTORY_HARNESS: "codex",
    TEST_CONTROL_WORKER_HARNESS: "claude"
  });

  try {
    process.env.BRAINSTACK_SKIP_USER_PATH_RESOLVE = "1";
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
      if (previousSkipPathResolve === undefined) {
        delete process.env.BRAINSTACK_SKIP_USER_PATH_RESOLVE;
      } else {
        process.env.BRAINSTACK_SKIP_USER_PATH_RESOLVE = previousSkipPathResolve;
      }
    process.env.PATH = fixture.previousPath;
      await rm(capture, { force: true });
      await rm(cachedCapture, { force: true });
      await rm(argsCapture, { force: true });
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("SSH accept-new trust mode is bootstrap-only and refused for worker dispatch", async () => {
  const fixture = await createFixture();
  const previousAllowAcceptNew = process.env.BRAINSTACK_ALLOW_ACCEPT_NEW_DISPATCH;

  try {
    process.env.BRAINSTACK_ALLOW_ACCEPT_NEW_DISPATCH = "0";
    const workers = JSON.parse(await readFile(fixture.workersFile, "utf8")) as Array<Record<string, unknown>>;
    workers[1] = {
      ...workers[1],
      sshTrustMode: "accept-new"
    };
    await writeFile(fixture.workersFile, `${JSON.stringify(workers, null, 2)}\n`);

    await fixture.commands.handleMessage(telegramMessage("/newctx bootstrap-trust worker1 host", 69));
    const context = fixture.db.getContextBySlug("bootstrap-trust");
    expect(context?.state).toBe("error");
    expect(context?.lastError).toContain("sshTrustMode=accept-new");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Pin the host key with brainctl trust-worker");
  } finally {
    if (previousAllowAcceptNew === undefined) {
      delete process.env.BRAINSTACK_ALLOW_ACCEPT_NEW_DISPATCH;
    } else {
      process.env.BRAINSTACK_ALLOW_ACCEPT_NEW_DISPATCH = previousAllowAcceptNew;
    }
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("worker transcription streams audio bytes through the configured SSH worker", async () => {
  const previousFakeRemoteCommand = process.env.FACTORY_TEST_FAKE_SSH_REMOTE_COMMAND;
  process.env.FACTORY_TEST_FAKE_SSH_REMOTE_COMMAND = "1";
  const fixture = await createFixture();

  try {
    const result = await fixture.workers.runTranscription("worker1", {
      fileName: "voice.ogg",
      bytes: new TextEncoder().encode("remote voice payload"),
      command: "cat",
      args: ["{input}"],
      timeoutMs: 5_000
    });

    expect(result.ok).toBe(true);
    expect(result.transport).toBe("ssh");
    expect(result.stdout).toBe("remote voice payload");
  } finally {
    if (previousFakeRemoteCommand === undefined) {
      delete process.env.FACTORY_TEST_FAKE_SSH_REMOTE_COMMAND;
    } else {
      process.env.FACTORY_TEST_FAKE_SSH_REMOTE_COMMAND = previousFakeRemoteCommand;
    }
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("worker transcription preconverts Telegram audio with ffmpeg when available", async () => {
  const fixture = await createFixture();

  try {
    await makeExecutable(
      join(fixture.root, "bin", "ffmpeg"),
      `#!/usr/bin/env bash
set -euo pipefail
output="\${@: -1}"
printf 'converted voice payload' > "$output"
`
    );

    const result = await fixture.workers.runTranscription("control", {
      fileName: "telegram-voice.ogg",
      bytes: new TextEncoder().encode("raw opus bytes"),
      command: fixture.fakeTranscriber,
      args: ["-f", "{input}"],
      timeoutMs: 5_000
    });

    expect(result.ok).toBe(true);
    expect(result.stdout).toBe("transcribed: converted voice payload");
  } finally {
    process.env.PATH = fixture.previousPath;
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

test("worker shell PATH fallback escalates when timeout is unavailable and the login shell ignores TERM", async () => {
  const capture = join(tmpdir(), `telemux-ssh-path-fallback-${Date.now()}.sh`);
  const fixture = await createFixture();

  try {
    process.env.FACTORY_TEST_CAPTURE_SSH_SCRIPT = capture;
    await fixture.workers.probeWorker("worker1");
    const capturedScript = await readFile(capture, "utf8");
    const startMarker = 'if [ -z "${BRAINSTACK_SKIP_USER_PATH_RESOLVE:-}" ]; then';
    const endMarker = "  unset __brainstack_detected_path\nfi";
    const start = capturedScript.indexOf(startMarker);
    const end = capturedScript.indexOf(endMarker, start);
    expect(start).toBeGreaterThanOrEqual(0);
    expect(end).toBeGreaterThan(start);
    const pathPrelude = capturedScript.slice(start, end + endMarker.length);

    const binDir = join(fixture.root, "path-fallback-bin");
    const logPath = join(fixture.root, "path-fallback-shell.log");
    const fakeShell = join(binDir, "fake-login-shell");
    await mkdir(binDir, { recursive: true });
    await makeExecutable(
      join(binDir, "sed"),
      "#!/bin/sh\nwhile IFS= read -r _line; do :; done\n"
    );
    await makeExecutable(
      join(binDir, "tail"),
      "#!/bin/sh\nlast=''\nwhile IFS= read -r line; do last=\"$line\"; done\n[ -n \"$last\" ] && printf '%s\\n' \"$last\"\n"
    );
    await makeExecutable(join(binDir, "sleep"), "#!/bin/sh\nexec /bin/sleep 0.2\n");
    await makeExecutable(
      fakeShell,
      `#!/bin/sh
printf 'started=%s\\n' "$$" >> ${JSON.stringify(logPath)}
trap 'printf "term=%s\\n" "$$" >> ${JSON.stringify(logPath)}' TERM
while :; do :; done
`
    );

    const result = Bun.spawnSync(["/bin/bash", "-c", `${pathPrelude}\nprintf 'PATH_AFTER=%s\\n' "$PATH"\n`], {
      env: {
        PATH: binDir,
        SHELL: fakeShell
      },
      stdout: "pipe",
      stderr: "pipe",
      timeout: 8_000
    });
    expect(result.exitCode).toBe(0);
    expect(result.stdout.toString()).toContain(`PATH_AFTER=${binDir}`);
    await waitFor(async () => {
      try {
        return (await readFile(logPath, "utf8")).includes("term=");
      } catch {
        return false;
      }
    });
    expect(await readFile(logPath, "utf8")).toContain("term=");
  } finally {
    delete process.env.FACTORY_TEST_CAPTURE_SSH_SCRIPT;
    process.env.PATH = fixture.previousPath;
    await rm(capture, { force: true });
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 12_000);

test("local worker resolves harness through BRAINSTACK_WORKER_PATH when service PATH is minimal", async () => {
  const fixture = await createFixture({
    FACTORY_CODEX_BIN: "codex",
    FACTORY_HARNESS_BIN: "codex",
    BRAINSTACK_SKIP_USER_PATH_RESOLVE: ""
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

test("telemux brain outbox refuses queued writes that retarget the live import token", async () => {
  const configuredPort = 45_000 + Math.floor(Math.random() * 2_000);
  const forgedPort = configuredPort + 2_500;
  const stateRoot = await mkdtemp(join(tmpdir(), "telemux-outbox-destination-"));
  let forgedHits = 0;
  let forgedAuth = "";
  let forgedServer: ReturnType<typeof Bun.serve> | null = null;
  const fixture = await createFixture({
    BRAINSTACK_STATE_ROOT: stateRoot,
    BRAIN_BASE_URL: `http://127.0.0.1:${configuredPort}`,
    BRAIN_IMPORT_TOKEN: "outbox-token"
  });

  try {
    forgedServer = Bun.serve({
      hostname: "127.0.0.1",
      port: forgedPort,
      fetch(request) {
        forgedHits += 1;
        forgedAuth = request.headers.get("authorization") || "";
        return Response.json({ ok: true });
      }
    });
    const payload = {
      title: "Forged import",
      text: "should not be replayed to the queued URL",
      source_machine: "control",
      source_harness: "telemux",
      source_type: "test"
    };
    const queued = buildOutboxItem(
      {
        endpoint: "import",
        url: `http://127.0.0.1:${forgedPort}`,
        payload,
        source_machine: "control",
        source_harness: "telemux"
      },
      "forged queued destination"
    );
    const outboxRoot = join(fixture.config.stateRoot, "outbox", sha256Hex(fixture.config.brainBaseUrl).slice(0, 16));
    await mkdir(outboxRoot, { recursive: true });
    const queuedPath = join(outboxRoot, `${queued.id}.json`);
    await writeOutboxItem(queuedPath, queued);

    expect(await flushBrainOutbox(fixture.config)).toEqual({ flushed: 0, kept: 1 });
    expect(forgedHits).toBe(0);
    expect(forgedAuth).toBe("");
    const item = JSON.parse(await readFile(queuedPath, "utf8")) as { terminal_error?: string; last_error?: string };
    expect(item.terminal_error || "").toContain("does not match configured brain origin");
    expect(item.last_error || "").toContain("does not match configured brain origin");
  } finally {
    if (forgedServer) {
      forgedServer.stop(true);
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
    clearPendingTextTimers(fixture);
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("Telegram text recovery persists legacy blank generations before accepted dispatch", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "30"
  });
  const key = `4242:70:${TEST_ALLOWED_TELEGRAM_USER_ID}`;

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx legacygen control scratch", 70));
    fixture.db.upsertPendingText({
      key,
      contextSlug: "legacygen",
      chatId: 4242,
      threadId: 70,
      userId: TEST_ALLOWED_TELEGRAM_USER_ID,
      partsJson: JSON.stringify(["legacy pending text"]),
      generationId: "legacy-seed"
    });
    setPendingTextGenerationForTest(fixture, key, "");

    fixture.commands.recoverPendingText();
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for legacygen.")));

    expect(fixture.db.listPendingText()).toHaveLength(0);
    const prompt = await readFile(join(fixture.factoryRoot, "scratch", "legacygen", ".factory", "control-plane.prompt.md"), "utf8");
    expect(prompt.match(/legacy pending text/g)).toHaveLength(1);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("Telegram text recovery does not duplicate legacy blank generations after dispatch refusal", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "30"
  });
  const key = `4242:70:${TEST_ALLOWED_TELEGRAM_USER_ID}`;

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx legacyrefuse control scratch", 70));
    const originalDispatcher = (fixture.commands as unknown as { dispatcher: unknown }).dispatcher;
    (fixture.commands as unknown as { dispatcher: { dispatch: () => Promise<{ accepted: boolean; message: string }> } }).dispatcher = {
      dispatch: async () => ({ accepted: false, message: "synthetic refusal" })
    };
    fixture.db.upsertPendingText({
      key,
      contextSlug: "legacyrefuse",
      chatId: 4242,
      threadId: 70,
      userId: TEST_ALLOWED_TELEGRAM_USER_ID,
      partsJson: JSON.stringify(["legacy retry text"]),
      generationId: "legacy-seed"
    });
    setPendingTextGenerationForTest(fixture, key, "");

    fixture.commands.recoverPendingText();
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("synthetic refusal")));

    const pending = fixture.db.listPendingText();
    expect(pending).toHaveLength(1);
    expect(pending[0]!.generationId).not.toBe("");
    expect(JSON.parse(pending[0]!.partsJson)).toEqual(["legacy retry text"]);
    (fixture.commands as unknown as { dispatcher: unknown }).dispatcher = originalDispatcher;
  } finally {
    process.env.PATH = fixture.previousPath;
    clearPendingTextTimers(fixture);
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("Telegram text coalescing preserves older text when newer text arrives before refusal completes", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "30"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx coalescerace control scratch", 70));
    const originalDispatcher = (fixture.commands as unknown as { dispatcher: unknown }).dispatcher;
    let releaseDispatch: (() => void) | null = null;
    let dispatchStarted = false;
    (fixture.commands as unknown as { dispatcher: { dispatch: () => Promise<{ accepted: boolean; message: string }> } }).dispatcher = {
      dispatch: async () => {
        dispatchStarted = true;
        await new Promise<void>((resolve) => {
          releaseDispatch = resolve;
        });
        return { accepted: false, message: "synthetic refusal" };
      }
    };

    await fixture.commands.handleMessage(telegramMessage("older text", 70));
    await waitFor(() => dispatchStarted);
    await fixture.commands.handleMessage(telegramMessage("newer text", 70));
    releaseDispatch?.();
    (fixture.commands as unknown as { dispatcher: unknown }).dispatcher = originalDispatcher;
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("synthetic refusal")));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for coalescerace.")));

    const pending = fixture.db.listPendingText();
    expect(pending).toHaveLength(0);
    const prompt = await readFile(join(fixture.factoryRoot, "scratch", "coalescerace", ".factory", "control-plane.prompt.md"), "utf8");
    expect(prompt).toContain("older text\n\nnewer text");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("Telegram text coalescing reschedules newer text when its timer fires during an accepted flush", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "30"
  });
  let originalDispatcher: unknown = null;
  let releaseDispatch: (() => void) | null = null;

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx coalescedefer control scratch", 70));
    originalDispatcher = (fixture.commands as unknown as { dispatcher: unknown }).dispatcher;
    let dispatchStarted = false;
    (fixture.commands as unknown as { dispatcher: { dispatch: () => Promise<{ accepted: boolean; message: string }> } }).dispatcher = {
      dispatch: async () => {
        dispatchStarted = true;
        await new Promise<void>((resolve) => {
          releaseDispatch = resolve;
        });
        return { accepted: true, message: "synthetic acceptance" };
      }
    };

    await fixture.commands.handleMessage(telegramMessage("older accepted text", 70));
    await waitFor(() => dispatchStarted);
    await fixture.commands.handleMessage(telegramMessage("newer text whose timer fires during old flush", 70));
    await Bun.sleep(80);

    (fixture.commands as unknown as { dispatcher: unknown }).dispatcher = originalDispatcher;
    releaseDispatch?.();
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for coalescedefer.")));

    expect(fixture.db.listPendingText()).toHaveLength(0);
    const prompt = await readFile(join(fixture.factoryRoot, "scratch", "coalescedefer", ".factory", "control-plane.prompt.md"), "utf8");
    expect(prompt).toContain("newer text whose timer fires during old flush");
    expect(prompt).not.toContain("older accepted text");
  } finally {
    process.env.PATH = fixture.previousPath;
    if (originalDispatcher) {
      (fixture.commands as unknown as { dispatcher: unknown }).dispatcher = originalDispatcher;
    }
    releaseDispatch?.();
    clearPendingTextTimers(fixture);
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("Telegram text coalescing does not merge a failed older flush into a newer context", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "30"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx coalesceold control scratch", 70));
    const originalDispatcher = (fixture.commands as unknown as { dispatcher: unknown }).dispatcher;
    let releaseDispatch: (() => void) | null = null;
    let dispatchStarted = false;
    (fixture.commands as unknown as { dispatcher: { dispatch: () => Promise<{ accepted: boolean; message: string }> } }).dispatcher = {
      dispatch: async () => {
        dispatchStarted = true;
        await new Promise<void>((resolve) => {
          releaseDispatch = resolve;
        });
        return { accepted: false, message: "synthetic refusal" };
      }
    };

    await fixture.commands.handleMessage(telegramMessage("older text", 70));
    await waitFor(() => dispatchStarted);
    const key = "4242:70:123456789";
    fixture.db.upsertPendingText({
      key,
      contextSlug: "coalescenew",
      chatId: 4242,
      threadId: 70,
      userId: TEST_ALLOWED_TELEGRAM_USER_ID,
      partsJson: JSON.stringify(["older text"])
    });
    releaseDispatch?.();
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("synthetic refusal")));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("newer text for another context remains queued")));

    const pending = fixture.db.listPendingText();
    expect(pending).toHaveLength(1);
    expect(pending[0]!.contextSlug).toBe("coalescenew");
    expect(JSON.parse(pending[0]!.partsJson)).toEqual(["older text"]);
    (fixture.commands as unknown as { dispatcher: unknown }).dispatcher = originalDispatcher;
  } finally {
    process.env.PATH = fixture.previousPath;
    clearPendingTextTimers(fixture);
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 10_000);

test("Telegram text coalescing does not delete a newer identical generation after old dispatch accepts", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "30"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx coalesceacceptold control scratch", 70));
    const originalDispatcher = (fixture.commands as unknown as { dispatcher: unknown }).dispatcher;
    let releaseDispatch: (() => void) | null = null;
    let dispatchStarted = false;
    let dispatchDone = false;
    (fixture.commands as unknown as { dispatcher: { dispatch: () => Promise<{ accepted: boolean; message: string }> } }).dispatcher = {
      dispatch: async () => {
        dispatchStarted = true;
        await new Promise<void>((resolve) => {
          releaseDispatch = resolve;
        });
        dispatchDone = true;
        return { accepted: true, message: "" };
      }
    };

    await fixture.commands.handleMessage(telegramMessage("same text", 70));
    await waitFor(() => dispatchStarted);
    const key = "4242:70:123456789";
    fixture.db.upsertPendingText({
      key,
      contextSlug: "coalesceacceptnew",
      chatId: 4242,
      threadId: 70,
      userId: TEST_ALLOWED_TELEGRAM_USER_ID,
      partsJson: JSON.stringify(["same text"])
    });
    releaseDispatch?.();
    await waitFor(() => dispatchDone);

    const pending = fixture.db.listPendingText();
    expect(pending).toHaveLength(1);
    expect(pending[0]!.contextSlug).toBe("coalesceacceptnew");
    expect(JSON.parse(pending[0]!.partsJson)).toEqual(["same text"]);
    (fixture.commands as unknown as { dispatcher: unknown }).dispatcher = originalDispatcher;
  } finally {
    process.env.PATH = fixture.previousPath;
    clearPendingTextTimers(fixture);
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
    clearPendingTextTimers(fixture);
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
    expect(fixture.db.getPendingText(`4242:73:${TEST_ALLOWED_TELEGRAM_USER_ID}`)?.partsJson).toBe(JSON.stringify(["part 25"]));

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

    const pendingText = (fixture.commands as unknown as { pendingText: Map<string, { timer: ReturnType<typeof setTimeout> }> }).pendingText;
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
    const mediaPrompt = await readFile(join(mediaRoot, ".factory", "control-plane.prompt.md"), "utf8");
    expect(mediaPrompt).toContain("Telegram inbound message:");
    expect(mediaPrompt).toContain("Start by reading those files and the current git status.");
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
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Audio and voice Telegram messages are not forwarded to the harness until transcription is installed.");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("install voice on <machine>");
    expect(await readFile(join(mediaRoot, ".factory", "fake-turn-count"), "utf8")).toBe(turnCountBeforeVoice);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("Telegram media coalescing batches quick multi-file uploads into one turn", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "80"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx media-burst control scratch", 65));
    fixture.telegram.registerRemoteFile("doc-a", "docs/first.txt", "first body");
    fixture.telegram.registerRemoteFile("doc-b", "docs/second.txt", "second body");

    const firstMessage = telegramDocumentMessage("Here are the env files.", 65, "doc-a", "notes.txt");
    const secondMessage = telegramDocumentMessage("", 65, "doc-b", "notes.txt");
    await fixture.commands.handleMessage(firstMessage);
    await fixture.commands.handleMessage(secondMessage);

    expect(fixture.telegram.sent.some((entry) => entry.text.includes("busy; queued this turn"))).toBe(false);
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for media-burst.")));
    await Bun.sleep(140);
    expect(fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 2 for media-burst."))).toBe(false);

    const mediaRoot = join(fixture.factoryRoot, "scratch", "media-burst");
    expect(await readFile(join(mediaRoot, ".factory", "fake-turn-count"), "utf8")).toBe("1");
    expect(await readFile(join(mediaRoot, ".factory", "inbox", "telegram", String(firstMessage.message_id), "notes.txt"), "utf8")).toBe(
      "first body"
    );
    expect(await readFile(join(mediaRoot, ".factory", "inbox", "telegram", String(firstMessage.message_id), "notes-2.txt"), "utf8")).toBe(
      "second body"
    );
    const prompt = await readFile(join(mediaRoot, ".factory", "control-plane.prompt.md"), "utf8");
    expect(prompt).toContain("Here are the env files.");
    expect(prompt).toContain("path=.factory/inbox/telegram");
    expect(prompt).toContain("notes.txt");
    expect(prompt).toContain("notes-2.txt");
  } finally {
    process.env.PATH = fixture.previousPath;
    clearPendingMediaTimers(fixture);
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("oversized Telegram documents fail with clear remediation before getFile", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "80"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx oversized control scratch", 66));
    const firstDocument = telegramDocumentMessage("Unzip these runbook archives.", 66, "large-doc-a", "runbook.zip", "application/zip");
    firstDocument.document!.file_size = TELEGRAM_MAX_INBOUND_FILE_BYTES + 8 * 1024 * 1024;
    const secondDocument = telegramDocumentMessage("", 66, "large-doc-b", "retrospective.zip", "application/zip");
    secondDocument.document!.file_size = TELEGRAM_MAX_INBOUND_FILE_BYTES + 5 * 1024 * 1024;

    await fixture.commands.handleMessage(firstDocument);
    await fixture.commands.handleMessage(secondDocument);
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Telegram cannot download these attachment")));

    const reply = fixture.telegram.sent.find((entry) => entry.text.includes("Telegram cannot download these attachment"));
    expect(reply?.text).toContain("standard 20.0 MiB Bot API download limit");
    expect(reply?.text).toContain("runbook.zip is 28.0 MiB");
    expect(reply?.text).toContain("retrospective.zip is 25.0 MiB");
    expect(reply?.text).toContain("put the files on the target machine and send their paths");
    expect(reply?.text).toContain(".factory/inbox/telegram/<message_id>/");
    const root = join(fixture.factoryRoot, "scratch", "oversized");
    expect(await Bun.file(join(root, ".factory", "fake-turn-count")).exists()).toBe(false);
  } finally {
    process.env.PATH = fixture.previousPath;
    clearPendingMediaTimers(fixture);
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("Telegram getFile 400 for attachments is translated into large-file guidance", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "0"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx getfile-limit control scratch", 67));
    const largeDocument = telegramDocumentMessage("Inspect this archive.", 67, "large-unknown", "retrospective.zip", "application/zip");
    delete largeDocument.document!.file_size;
    fixture.telegram.registerGetFileFailure("large-unknown", "telegram api getFile failed with 400: Bad Request: file is too big");

    await fixture.commands.handleMessage(largeDocument);
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Telegram did not provide a downloadable file path")));

    const reply = fixture.telegram.sent.find((entry) => entry.text.includes("Telegram did not provide a downloadable file path"));
    expect(reply?.text).toContain("retrospective.zip");
    expect(reply?.text).toContain("usually means the file is over the standard 20.0 MiB Bot API download limit");
    expect(reply?.text).toContain("send a normal download URL");
    const root = join(fixture.factoryRoot, "scratch", "getfile-limit");
    expect(await Bun.file(join(root, ".factory", "fake-turn-count")).exists()).toBe(false);
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("voice transcription routes through configured worker and dispatches transcript as plain text", async () => {
  const fixture = await createFixture({
    FACTORY_TRANSCRIPTION_ENABLED: "1",
    FACTORY_TRANSCRIPTION_TARGET: "worker",
    FACTORY_TRANSCRIPTION_WORKER: "control",
    FACTORY_TRANSCRIPTION_ARGS_JSON: '["-f","{input}"]',
    FACTORY_TRANSCRIPTION_ECHO: "1",
    FACTORY_TEXT_COALESCE_MS: "1"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx voice control scratch", 61));
    fixture.telegram.registerRemoteFile("voice-main", "voice/message.ogg", "please inspect redis slots");

    await fixture.commands.handleMessage(telegramVoiceMessage(61, "voice-main"));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for voice.")));

    expect(fixture.telegram.sent.some((entry) => entry.text.includes("Transcribed voice:\ntranscribed: please inspect redis slots"))).toBe(
      true
    );
    const voiceRoot = join(fixture.factoryRoot, "scratch", "voice");
    const prompt = await readFile(join(voiceRoot, ".factory", "control-plane.prompt.md"), "utf8");
    expect(prompt).toContain("Instruction:\n\ntranscribed: please inspect redis slots");
    expect(prompt).not.toContain("Voice transcript");
    expect(prompt).not.toContain("Telegram inbound message:");

    fixture.telegram.registerRemoteFile("voice-run", "voice/run.ogg", "run the explicit redis check");
    await fixture.commands.handleMessage(telegramVoiceCaptionMessage("/run", 61, "voice-run"));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 2 for voice.")));

    const runPrompt = await readFile(join(voiceRoot, ".factory", "control-plane.prompt.md"), "utf8");
    expect(runPrompt).toContain("Instruction:\n\ntranscribed: run the explicit redis check");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("voice transcription reports diagnostics when a command exits successfully without text", async () => {
  const fixture = await createFixture({
    FACTORY_TRANSCRIPTION_ENABLED: "1",
    FACTORY_TRANSCRIPTION_TARGET: "worker",
    FACTORY_TRANSCRIPTION_WORKER: "control",
    FACTORY_TRANSCRIPTION_ARGS_JSON: '["{input}"]',
    FACTORY_TEXT_COALESCE_MS: "1"
  });

  try {
    const silentTranscriber = join(fixture.root, "bin", "silent-transcribe");
    await makeExecutable(
      silentTranscriber,
      `#!/usr/bin/env bash
set -euo pipefail
echo "failed to read audio file" >&2
exit 0
`
    );
    fixture.config.transcription.command = silentTranscriber;

    await fixture.commands.handleMessage(telegramMessage("/newctx silent-voice control scratch", 64));
    fixture.telegram.registerRemoteFile("voice-empty", "voice/empty.ogg", "not really audio");
    await fixture.commands.handleMessage(telegramVoiceMessage(64, "voice-empty"));

    expect(fixture.telegram.sent.at(-1)?.text).toContain("Voice transcription on control produced no text.");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Diagnostics: failed to read audio file");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("voice install requests delegate to canonical brainctl capability command without a bound context", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("install voice on erbine", 62));
    expect(fixture.telegram.sent[0]?.text).toContain("Installing voice transcription on erbine.");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Voice install complete.");
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Test it by sending a Telegram voice note");

    const calls = await readFile(fixture.fakeBrainctlCalls, "utf8");
    expect(calls).toContain(`capabilities install voice --config ${join(fixture.root, "brainstack.yaml")} --target erbine --restart-delay-ms 1500`);

    await fixture.commands.handleMessage(telegramMessage("/voice status", 62));
    const updatedCalls = await readFile(fixture.fakeBrainctlCalls, "utf8");
    expect(updatedCalls).toContain(`capabilities doctor voice --config ${join(fixture.root, "brainstack.yaml")}`);
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Voice doctor complete.");
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
	  }
	}, 15_000);

  test("voice uninstall requests delegate to canonical brainctl capability command", async () => {
    const fixture = await createFixture();

    try {
      await fixture.commands.handleMessage(telegramMessage("uninstall voice on erbine", 62));
      expect(fixture.telegram.sent[0]?.text).toContain("Uninstalling voice transcription on erbine.");
      expect(fixture.telegram.sent.at(-1)?.text).toContain("Voice uninstall complete.");
      expect(fixture.telegram.sent.at(-1)?.text).toContain("install voice on <machine>");

      const calls = await readFile(fixture.fakeBrainctlCalls, "utf8");
      expect(calls).toContain(`capabilities uninstall voice --config ${join(fixture.root, "brainstack.yaml")} --remove-files --restart-delay-ms 1500 --target erbine`);
    } finally {
      process.env.PATH = fixture.previousPath;
      await rm(fixture.root, { recursive: true, force: true });
    }
  }, 15_000);

	test("voice install sends progress messages during slow capability setup", async () => {
  const fixture = await createFixture({ FACTORY_CAPABILITY_PROGRESS_INTERVAL_MS: "25" });
  const previousSleep = process.env.FACTORY_TEST_BRAINCTL_INSTALL_SLEEP_SECONDS;
  process.env.FACTORY_TEST_BRAINCTL_INSTALL_SLEEP_SECONDS = "1";

  try {
    await fixture.commands.handleMessage(telegramMessage("install voice on erbine", 63));

    const messages = fixture.telegram.sent.map((entry) => entry.text);
    expect(messages[0]).toContain("Installing voice transcription on erbine.");
    expect(messages.some((text) => text.includes("Still installing voice transcription on erbine"))).toBe(true);
    expect(messages.at(-1)).toContain("Voice install complete.");
  } finally {
    if (previousSleep === undefined) {
      delete process.env.FACTORY_TEST_BRAINCTL_INSTALL_SLEEP_SECONDS;
    } else {
      process.env.FACTORY_TEST_BRAINCTL_INSTALL_SLEEP_SECONDS = previousSleep;
    }
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
