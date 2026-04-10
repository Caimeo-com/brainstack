import { expect, test } from "bun:test";
import type { FactoryConfig } from "../src/config";
import { TELEGRAM_BOT_COMMANDS, TelegramBot } from "../src/telegram";

const TEST_ALLOWED_TELEGRAM_USER_ID = 123456789;

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
  const calls: Array<{ url: string; method: string; body?: string }> = [];
  const originalFetch = globalThis.fetch;

  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input);
    calls.push({
      url,
      method: init?.method || "GET",
      body: typeof init?.body === "string" ? init.body : undefined
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
    expect(calls[1]?.url).toBe("https://api.telegram.org/file/bottest-token/photos/test.jpg");
    expect(calls[1]?.method).toBe("GET");
  } finally {
    globalThis.fetch = originalFetch;
  }
});
