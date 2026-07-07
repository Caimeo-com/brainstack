import { expect, test } from "bun:test";
import { lstat, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { FactoryConfig } from "../src/config";
import type { ContextRecord } from "../src/db";
import { TELEGRAM_BOT_COMMANDS, TelegramBot } from "../src/telegram";
import { sendTelegramLocalFile } from "../src/telegram-file-send";

const TEST_ALLOWED_TELEGRAM_USER_ID = 123456789;

function testContext(slug: string, chatId: number | null, threadId: number | null): ContextRecord {
  const now = new Date().toISOString();
  return {
    slug,
    telegramChatId: chatId,
    telegramThreadId: threadId,
    machine: "control",
    kind: "scratch",
    state: "active",
    transport: "local",
    target: "scratch",
    rootPath: "/tmp/scratch",
    worktreePath: "/tmp/scratch",
    branchName: null,
    baseBranch: null,
    latestRunLogPath: null,
    lastSummary: null,
    lastArtifacts: null,
    codexSessionId: null,
    lastRunAt: null,
    usageAdapter: "manual",
    harness: null,
    harnessBin: null,
    modelOverride: null,
    reasoningEffortOverride: null,
    lastError: null,
    createdAt: now,
    updatedAt: now
  };
}

test("registered Telegram commands include implemented operator commands", () => {
  const commands = new Set(TELEGRAM_BOT_COMMANDS.map((command) => command.command));

  for (const command of ["whoami", "ip", "workers", "updates", "voice", "uploads", "context", "compact", "crons", "cron_run", "run", "resume", "loop", "artifacts", "shred", "usage"]) {
    expect(commands.has(command)).toBe(true);
  }
});

test("syncCommands registers and verifies commands for all configured scopes", async () => {
  const calls: Array<{ method: string; body: Record<string, unknown> }> = [];
  const originalFetch = globalThis.fetch;

  globalThis.fetch = (async (_input: RequestInfo | URL, init?: RequestInit) => {
    const method = String(_input).split("/").at(-1) || "";
    const body = JSON.parse(String(init?.body || "{}")) as Record<string, unknown>;
    calls.push({ method, body });

    if (method === "getMyCommands") {
      return new Response(
        JSON.stringify({
          ok: true,
          result: TELEGRAM_BOT_COMMANDS.map((command) => ({
            command: command.command,
            description: command.description
          }))
        }),
        {
          status: 200,
          headers: { "content-type": "application/json" }
        }
      );
    }

    return new Response(
      JSON.stringify({
        ok: true,
        result: true
      }),
      {
        status: 200,
        headers: { "content-type": "application/json" }
      }
    );
  }) as typeof fetch;

  try {
    const config: FactoryConfig = {
      projectRoot: "/tmp/project",
      controlRoot: "/tmp/telemux",
      dbPath: "/tmp/telemux/db.sqlite",
      contextsDir: "/tmp/telemux/contexts",
      cronSnapshotsDir: "/tmp/telemux/crons",
      logsDir: "/tmp/telemux/logs",
      sshKnownHostsPath: "/tmp/telemux/ssh_known_hosts",
      factoryRoot: "/tmp/factory",
      managedRepoRoot: "/tmp/factory/repos",
      managedHostRoot: "/tmp/factory/hostctx",
      managedScratchRoot: "/tmp/factory/scratch",
      dashboardHost: "127.0.0.1",
      dashboardPort: 8787,
      telegramBotToken: "test-token",
      telegramControlChatId: -1001234567890,
      allowedTelegramUserId: TEST_ALLOWED_TELEGRAM_USER_ID,
      telegramPollTimeoutSeconds: 30,
      cronPollIntervalSeconds: 30,
      localMachine: "control",
      workers: [],
      usageAdapter: "manual",
      codexBin: "codex"
    };

    const telegram = new TelegramBot(config, { listContexts: () => [] } as never);
    const results = await telegram.syncCommands();

    expect(results).toHaveLength(4);
    expect(results.map((result) => result.label)).toEqual([
      "default",
      "all_private_chats",
      "all_group_chats",
      `chat_member(-1001234567890,${TEST_ALLOWED_TELEGRAM_USER_ID})`
    ]);
    expect(results.every((result) => result.setOk)).toBe(true);
    expect(results.every((result) => result.verifyOk)).toBe(true);
    expect(results.every((result) => result.commands.length === TELEGRAM_BOT_COMMANDS.length)).toBe(true);

    const setCalls = calls.filter((call) => call.method === "setMyCommands");
    const getCalls = calls.filter((call) => call.method === "getMyCommands");

    expect(setCalls).toHaveLength(4);
    expect(getCalls).toHaveLength(4);
    expect(setCalls.map((call) => call.body.scope)).toEqual([
      { type: "default" },
      { type: "all_private_chats" },
      { type: "all_group_chats" },
      { type: "chat_member", chat_id: -1001234567890, user_id: TEST_ALLOWED_TELEGRAM_USER_ID }
    ]);
    expect(setCalls[0]?.body.commands).toEqual(
      TELEGRAM_BOT_COMMANDS.map((command) => ({
        command: command.command,
        description: command.description
      }))
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("sendAttachment uploads multipart documents into the correct Telegram thread", async () => {
  let capturedMethod = "";
  let capturedBody: FormData | null = null;
  const originalFetch = globalThis.fetch;

  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    capturedMethod = String(input).split("/").at(-1) || "";
    capturedBody = (init?.body as FormData) || null;

    return new Response(
      JSON.stringify({
        ok: true,
        result: { message_id: 123 }
      }),
      {
        status: 200,
        headers: { "content-type": "application/json" }
      }
    );
  }) as typeof fetch;

  try {
    const config: FactoryConfig = {
      projectRoot: "/tmp/project",
      controlRoot: "/tmp/telemux",
      dbPath: "/tmp/telemux/db.sqlite",
      contextsDir: "/tmp/telemux/contexts",
      cronSnapshotsDir: "/tmp/telemux/crons",
      logsDir: "/tmp/telemux/logs",
      sshKnownHostsPath: "/tmp/telemux/ssh_known_hosts",
      factoryRoot: "/tmp/factory",
      managedRepoRoot: "/tmp/factory/repos",
      managedHostRoot: "/tmp/factory/hostctx",
      managedScratchRoot: "/tmp/factory/scratch",
      dashboardHost: "127.0.0.1",
      dashboardPort: 8787,
      telegramBotToken: "test-token",
      telegramControlChatId: -1001234567890,
      allowedTelegramUserId: TEST_ALLOWED_TELEGRAM_USER_ID,
      telegramPollTimeoutSeconds: 30,
      cronPollIntervalSeconds: 30,
      localMachine: "control",
      workers: [],
      usageAdapter: "manual",
      codexBin: "codex"
    };

    const telegram = new TelegramBot(config, { listContexts: () => [] } as never);
    await telegram.sendAttachment(
      { chatId: 4242, threadId: 77 },
      {
        kind: "document",
        fileName: "sample.txt",
        bytes: new TextEncoder().encode("hello world"),
        mimeType: "text/plain",
        caption: "sample caption"
      }
    );

    expect(capturedMethod).toBe("sendDocument");
    expect(capturedBody).toBeInstanceOf(FormData);
    expect(capturedBody?.get("chat_id")).toBe("4242");
    expect(capturedBody?.get("message_thread_id")).toBe("77");
    expect(capturedBody?.get("caption")).toBe("sample caption");

    const document = capturedBody?.get("document");
    expect(document).toBeInstanceOf(File);
    expect((document as File).name).toBe("sample.txt");
    expect(await (document as File).text()).toBe("hello world");
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("sendTelegramLocalFile sends a spooled file to control chat or bound context", async () => {
  const dir = await mkdtemp(join(tmpdir(), "telemux-send-file-"));
  const filePath = join(dir, "report.md");
  await writeFile(filePath, "# Report\n\nhello mobile\n");
  const sent: Array<{
    target: { chatId: number; threadId: number | null };
    attachment: { kind: string; fileName: string; bytes: Uint8Array; mimeType?: string | null; caption?: string | null };
  }> = [];
  const telegram = {
    async sendAttachment(
      target: { chatId: number; threadId: number | null },
      attachment: { kind: string; fileName: string; bytes: Uint8Array; mimeType?: string | null; caption?: string | null }
    ) {
      sent.push({ target, attachment });
    }
  };
  const context = testContext("scratchpad", -100222, 77);
  const db = {
    getContextBySlug(slug: string) {
      return slug === context.slug ? context : null;
    }
  };

  try {
    const controlResult = await sendTelegramLocalFile(
      { telegramBotToken: "test-token", telegramControlChatId: -100111 },
      db,
      telegram,
      {
        filePath,
        caption: "control report",
        contextSlug: null,
        chatId: null,
        threadId: null,
        kind: null,
        displayName: null,
        maxBytes: 1024 * 1024,
        allowSensitive: false,
        deleteAfterSend: false
      }
    );

    expect(controlResult.target.mode).toBe("control");
    expect(sent[0]?.target).toEqual({ chatId: -100111, threadId: null });
    expect(sent[0]?.attachment.kind).toBe("document");
    expect(sent[0]?.attachment.fileName).toBe("report.md");
    expect(sent[0]?.attachment.mimeType).toBe("text/markdown");
    expect(sent[0]?.attachment.caption).toBe("control report");
    expect(new TextDecoder().decode(sent[0]?.attachment.bytes)).toBe("# Report\n\nhello mobile\n");

    const contextResult = await sendTelegramLocalFile(
      { telegramBotToken: "test-token", telegramControlChatId: -100111 },
      db,
      telegram,
      {
        filePath,
        caption: null,
        contextSlug: "scratchpad",
        chatId: null,
        threadId: null,
        kind: "document",
        displayName: "mobile-report.md",
        maxBytes: 1024 * 1024,
        allowSensitive: false,
        deleteAfterSend: false
      }
    );

    expect(contextResult.target).toEqual({
      mode: "context",
      contextSlug: "scratchpad",
      chatId: -100222,
      threadId: 77
    });
    expect(sent[1]?.target).toEqual({ chatId: -100222, threadId: 77 });
    expect(sent[1]?.attachment.fileName).toBe("mobile-report.md");

    const deletePath = join(dir, "delete-after-send.txt");
    await writeFile(deletePath, "delete me after send");
    const deleteResult = await sendTelegramLocalFile(
      { telegramBotToken: "test-token", telegramControlChatId: -100111 },
      db,
      telegram,
      {
        filePath: deletePath,
        caption: null,
        contextSlug: null,
        chatId: -100333,
        threadId: 12,
        kind: null,
        displayName: null,
        maxBytes: 1024 * 1024,
        allowSensitive: false,
        deleteAfterSend: true
      }
    );
    expect(deleteResult.deleted).toBe(true);
    expect(await Bun.file(deletePath).exists()).toBe(false);
    expect(sent[2]?.target).toEqual({ chatId: -100333, threadId: 12 });
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("sendTelegramLocalFile fails closed for missing token, unsafe files, and unresolved targets", async () => {
  const dir = await mkdtemp(join(tmpdir(), "telemux-send-file-security-"));
  const safePath = join(dir, "safe.txt");
  const tokenPath = join(dir, "secret-token.txt");
  const symlinkPath = join(dir, "safe-link.txt");
  await writeFile(safePath, "safe");
  await writeFile(tokenPath, "token");
  await symlink(safePath, symlinkPath);
  const telegram = {
    async sendAttachment() {
      throw new Error("should not send");
    }
  };
  const db = {
    getContextBySlug() {
      return null;
    }
  };
  const baseOptions = {
    filePath: safePath,
    caption: null,
    contextSlug: null,
    chatId: null,
    threadId: null,
    kind: null,
    displayName: null,
    maxBytes: 1024,
    allowSensitive: false,
    deleteAfterSend: false
  };

  try {
    await expect(sendTelegramLocalFile({ telegramBotToken: "", telegramControlChatId: -100111 }, db, telegram, baseOptions)).rejects.toThrow(
      "FACTORY_TELEGRAM_BOT_TOKEN"
    );
    await expect(
      sendTelegramLocalFile({ telegramBotToken: "test-token", telegramControlChatId: null }, db, telegram, baseOptions)
    ).rejects.toThrow("FACTORY_TELEGRAM_CONTROL_CHAT_ID");
    await expect(
      sendTelegramLocalFile({ telegramBotToken: "test-token", telegramControlChatId: -100111 }, db, telegram, {
        ...baseOptions,
        filePath: symlinkPath
      })
    ).rejects.toThrow("symlink");
    await expect(
      sendTelegramLocalFile({ telegramBotToken: "test-token", telegramControlChatId: -100111 }, db, telegram, {
        ...baseOptions,
        filePath: tokenPath
      })
    ).rejects.toThrow("sensitive-looking");
    await expect(
      sendTelegramLocalFile({ telegramBotToken: "test-token", telegramControlChatId: -100111 }, db, telegram, {
        ...baseOptions,
        filePath: tokenPath,
        displayName: "notes.txt"
      })
    ).rejects.toThrow("sensitive-looking");
    await expect(
      sendTelegramLocalFile({ telegramBotToken: "test-token", telegramControlChatId: -100111 }, db, telegram, {
        ...baseOptions,
        maxBytes: 1
      })
    ).rejects.toThrow("file too large");
    await expect(
      sendTelegramLocalFile({ telegramBotToken: "test-token", telegramControlChatId: -100111 }, db, telegram, {
        ...baseOptions,
        contextSlug: "scratchpad",
        chatId: -100222
      })
    ).rejects.toThrow("either --context or --chat-id");
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("sendTelegramLocalFile refuses delete-after-send when the path changes before cleanup", async () => {
  const dir = await mkdtemp(join(tmpdir(), "telemux-send-file-delete-race-"));
  const sendPath = join(dir, "report.txt");
  const replacementTarget = join(dir, "replacement.txt");
  await writeFile(sendPath, "report");
  await writeFile(replacementTarget, "replacement");
  const telegram = {
    async sendAttachment() {
      await rm(sendPath, { force: true });
      await symlink(replacementTarget, sendPath);
    }
  };
  const db = {
    getContextBySlug() {
      return null;
    }
  };

  try {
    await expect(
      sendTelegramLocalFile({ telegramBotToken: "test-token", telegramControlChatId: -100111 }, db, telegram, {
        filePath: sendPath,
        caption: null,
        contextSlug: null,
        chatId: null,
        threadId: null,
        kind: null,
        displayName: null,
        maxBytes: 1024,
        allowSensitive: false,
        deleteAfterSend: true
      })
    ).rejects.toThrow("changed after send");
    expect((await lstat(sendPath)).isSymbolicLink()).toBe(true);
    expect(await Bun.file(replacementTarget).exists()).toBe(true);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("sendChatAction targets the correct Telegram thread", async () => {
  let capturedMethod = "";
  let capturedBody: Record<string, unknown> | null = null;
  const originalFetch = globalThis.fetch;

  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    capturedMethod = String(input).split("/").at(-1) || "";
    capturedBody = JSON.parse(String(init?.body || "{}")) as Record<string, unknown>;

    return new Response(
      JSON.stringify({
        ok: true,
        result: true
      }),
      {
        status: 200,
        headers: { "content-type": "application/json" }
      }
    );
  }) as typeof fetch;

  try {
    const config: FactoryConfig = {
      projectRoot: "/tmp/project",
      controlRoot: "/tmp/telemux",
      dbPath: "/tmp/telemux/db.sqlite",
      contextsDir: "/tmp/telemux/contexts",
      cronSnapshotsDir: "/tmp/telemux/crons",
      logsDir: "/tmp/telemux/logs",
      sshKnownHostsPath: "/tmp/telemux/ssh_known_hosts",
      factoryRoot: "/tmp/factory",
      managedRepoRoot: "/tmp/factory/repos",
      managedHostRoot: "/tmp/factory/hostctx",
      managedScratchRoot: "/tmp/factory/scratch",
      dashboardHost: "127.0.0.1",
      dashboardPort: 8787,
      telegramBotToken: "test-token",
      telegramControlChatId: -1001234567890,
      allowedTelegramUserId: TEST_ALLOWED_TELEGRAM_USER_ID,
      telegramPollTimeoutSeconds: 30,
      cronPollIntervalSeconds: 30,
      localMachine: "control",
      workers: [],
      usageAdapter: "manual",
      codexBin: "codex"
    };

    const telegram = new TelegramBot(config, { listContexts: () => [] } as never);
    await telegram.sendChatAction({ chatId: 4242, threadId: 77 }, "typing");

    expect(capturedMethod).toBe("sendChatAction");
    expect(capturedBody).toEqual({
      chat_id: 4242,
      message_thread_id: 77,
      action: "typing"
    });
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("getFile requests Telegram file metadata and downloadFile fetches the file bytes", async () => {
  const calls: Array<{ url: string; method: string; body?: string; hasSignal: boolean }> = [];
  const originalFetch = globalThis.fetch;

  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    calls.push({
      url,
      method: init?.method || "GET",
      body: typeof init?.body === "string" ? init.body : undefined,
      hasSignal: Boolean(init?.signal)
    });

    if (url.endsWith("/getFile")) {
      return new Response(
        JSON.stringify({
          ok: true,
          result: {
            file_id: "abc123",
            file_size: 11,
            file_path: "photos/test.jpg"
          }
        }),
        {
          status: 200,
          headers: { "content-type": "application/json" }
        }
      );
    }

    return new Response(new TextEncoder().encode("hello file"), {
      status: 200,
      headers: { "content-type": "application/octet-stream" }
    });
  }) as typeof fetch;

  try {
    const config: FactoryConfig = {
      projectRoot: "/tmp/project",
      controlRoot: "/tmp/telemux",
      dbPath: "/tmp/telemux/db.sqlite",
      contextsDir: "/tmp/telemux/contexts",
      cronSnapshotsDir: "/tmp/telemux/crons",
      logsDir: "/tmp/telemux/logs",
      sshKnownHostsPath: "/tmp/telemux/ssh_known_hosts",
      factoryRoot: "/tmp/factory",
      managedRepoRoot: "/tmp/factory/repos",
      managedHostRoot: "/tmp/factory/hostctx",
      managedScratchRoot: "/tmp/factory/scratch",
      dashboardHost: "127.0.0.1",
      dashboardPort: 8787,
      telegramBotToken: "test-token",
      telegramControlChatId: -1001234567890,
      allowedTelegramUserId: TEST_ALLOWED_TELEGRAM_USER_ID,
      telegramPollTimeoutSeconds: 30,
      cronPollIntervalSeconds: 30,
      localMachine: "control",
      workers: [],
      usageAdapter: "manual",
      codexBin: "codex"
    };

    const telegram = new TelegramBot(config, { listContexts: () => [] } as never);
    const remoteFile = await telegram.getFile("abc123");
    const bytes = await telegram.downloadFile("photos/test.jpg");

    expect(remoteFile).toEqual({
      file_id: "abc123",
      file_size: 11,
      file_path: "photos/test.jpg"
    });
    expect(new TextDecoder().decode(bytes)).toBe("hello file");
    expect(calls[0]?.url).toBe("https://api.telegram.org/bottest-token/getFile");
    expect(calls[0]?.method).toBe("POST");
    expect(calls[0]?.body).toContain("\"file_id\":\"abc123\"");
    expect(calls[0]?.hasSignal).toBe(true);
    expect(calls[1]?.url).toBe("https://api.telegram.org/file/bottest-token/photos/test.jpg");
    expect(calls[1]?.method).toBe("GET");
    expect(calls[1]?.hasSignal).toBe(true);

    globalThis.fetch = (async () =>
      new Response(
        JSON.stringify({
          ok: false,
          description: "Bad Request: file is too big"
        }),
        {
          status: 400,
          headers: { "content-type": "application/json" }
        }
      )) as typeof fetch;
    await expect(telegram.getFile("too-large")).rejects.toThrow("telegram api getFile failed with 400: Bad Request: file is too big");

    globalThis.fetch = (async () =>
      new Response(new TextEncoder().encode("large body"), {
        status: 200,
        headers: {
          "content-type": "application/octet-stream",
          "content-length": "10"
        }
      })) as typeof fetch;
    await expect(telegram.downloadFile("photos/large.jpg", 5)).rejects.toThrow("content-length");

    globalThis.fetch = (async () =>
      new Response(
        new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new TextEncoder().encode("1234"));
            controller.enqueue(new TextEncoder().encode("5678"));
            controller.close();
          }
        }),
        {
          status: 200,
          headers: { "content-type": "application/octet-stream" }
        }
      )) as typeof fetch;
    await expect(telegram.downloadFile("photos/stream.jpg", 5)).rejects.toThrow("exceeds 5 bytes");
  } finally {
    globalThis.fetch = originalFetch;
  }
});
