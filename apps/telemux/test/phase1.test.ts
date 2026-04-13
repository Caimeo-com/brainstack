import { expect, test } from "bun:test";
import { chmod, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { CommandHandler } from "../src/commands";
import { loadConfig, ensureProjectPaths } from "../src/config";
import { CronManager } from "../src/cron-manager";
import { CronScheduler } from "../src/cron-scheduler";
import { ContextService } from "../src/contexts";
import { FactoryDb } from "../src/db";
import { Dispatcher } from "../src/dispatcher";
import type { TelegramAttachmentKind } from "../src/telegram-attachments";
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

  async downloadFile(filePath: string): Promise<Uint8Array> {
    const file = [...this.remoteFiles.values()].find((entry) => entry.filePath === filePath);
    if (!file) {
      throw new Error(`Missing fake Telegram file path: ${filePath}`);
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
printf '{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":0,"output_tokens":5}}\\n'
`
  );

  await makeExecutable(
    fakeSsh,
    `#!/usr/bin/env bash
if [[ -n "\${FACTORY_TEST_CAPTURE_SSH_SCRIPT:-}" ]]; then
  cat > "$FACTORY_TEST_CAPTURE_SSH_SCRIPT"
  echo "ssh capture: No route to host" >&2
  exit 255
fi
echo "ssh: connect to host unreachable: No route to host" >&2
exit 255
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
          sshTarget: "worker1.tailnet",
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
  const commands = new CommandHandler(config, db, telegram as never, contexts, workers, dispatcher, cronManager);
  const cronScheduler = new CronScheduler(config, db, cronManager, dispatcher, telegram as never);

  return {
    root,
    controlRoot,
    factoryRoot,
    fakeCodex,
    fakeClaude,
    fakeSsh,
    previousPath,
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

test("phase 1 workflow covers local host/scratch and pending remote behavior", async () => {
  const fixture = await createFixture();

  try {
    await fixture.commands.handleMessage(telegramMessage("/help", 10));
    const helpText = fixture.telegram.sent.at(-1)?.text || "";
    expect(helpText).toContain("A context is the durable Codex workspace and session binding for one Telegram topic.");
    expect(helpText).toContain("/newctx is usually run once per reusable Telegram topic");
    expect(helpText).toContain("/bind is for repointing the current topic");
    expect(helpText).toContain("Old Telegram messages remain in Telegram");
    expect(helpText).toContain("/mode [fast|normal|max|clear]");
    expect(helpText).toContain("/model [model-id|clear]");
    expect(helpText).toContain("/effort [low|medium|high|xhigh|clear]");
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
}, 15_000);

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
    await waitFor(() => Bun.file(join(cronRoot, ".factory", "CRONS.md")).exists());
    expect(await readFile(join(cronRoot, ".factory", "CRONS.md"), "utf8")).toContain("stripe-reminder");

    await fixture.commands.handleMessage(telegramMessage("/crons", 80));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("stripe-reminder");

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
  } finally {
    process.env.PATH = fixture.previousPath;
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 20_000);

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

    await fixture.commands.handleMessage(telegramMessage("/model gpt-5-codex", 70));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Codex mode now custom");

    await fixture.commands.handleMessage(telegramMessage("/effort high", 70));
    expect(fixture.telegram.sent.at(-1)?.text).toContain("Codex mode now custom");

    await fixture.commands.handleMessage(telegramMessage("Continue with the same session.", 70));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 2 for tuning.")));

    expect(fixture.db.getContextBySlug("tuning")?.codexSessionId).toBe(firstSession);
    expect(await readFile(join(tuningRoot, ".factory", "fake-model.txt"), "utf8")).toBe("gpt-5-codex");
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
    await fixture.workers.probeWorker("worker1");
    const capturedScript = await readFile(capture, "utf8");
    expect(capturedScript).toContain("harness_bin='codex'");
    expect(capturedScript).toContain("__BRAINSTACK_PATH__");
    expect(capturedScript).toContain("BRAINSTACK_WORKER_PATH");
    expect(capturedScript).not.toContain(fixture.fakeCodex);
  } finally {
    delete process.env.FACTORY_TEST_CAPTURE_SSH_SCRIPT;
    process.env.PATH = fixture.previousPath;
    await rm(capture, { force: true });
    await rm(fixture.root, { recursive: true, force: true });
  }
}, 15_000);

test("Telegram text coalescing merges quick text and commands flush pending text", async () => {
  const fixture = await createFixture({
    FACTORY_TEXT_COALESCE_MS: "80"
  });

  try {
    await fixture.commands.handleMessage(telegramMessage("/newctx coalesce control scratch", 71));
    await fixture.commands.handleMessage(telegramMessage("First part", 71));
    await fixture.commands.handleMessage(telegramMessage("Second part", 71));
    await waitFor(() => fixture.telegram.sent.some((entry) => entry.text.includes("Reply turn 1 for coalesce.")));
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
