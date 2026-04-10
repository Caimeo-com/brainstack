import { FactoryDb } from "./db";
import type { FactoryConfig } from "./config";
import type { TelegramAttachmentKind } from "./telegram-attachments";

export interface TelegramBotCommand {
  command: string;
  description: string;
}

export interface TelegramBotCommandScope {
  type: "default" | "all_private_chats" | "all_group_chats" | "chat_member";
  chat_id?: number;
  user_id?: number;
}

export interface TelegramCommandSyncResult {
  label: string;
  scope: TelegramBotCommandScope;
  setOk: boolean;
  setError: string | null;
  verifyOk: boolean;
  verifyError: string | null;
  commands: TelegramBotCommand[];
}

export const TELEGRAM_BOT_COMMANDS: TelegramBotCommand[] = [
  { command: "help", description: "Explain contexts and the command flow" },
  { command: "explainctx", description: "Explain the current topic binding" },
  { command: "synccommands", description: "Re-register Telegram command scopes" },
  { command: "showcommands", description: "Show registered Telegram commands" },
  { command: "whoami", description: "Show Telegram ids and current binding" },
  { command: "workers", description: "List worker status and transport" },
  { command: "crons", description: "List scheduled jobs for this topic/context" },
  { command: "cron", description: "Inspect or manage one scheduled job" },
  { command: "mode", description: "Set or show the Codex mode for this topic" },
  { command: "model", description: "Set or show the Codex model override" },
  { command: "effort", description: "Set or show the Codex reasoning effort" },
  { command: "newctx", description: "Create and bind a context to this topic" },
  { command: "bind", description: "Rebind this topic to another context" },
  { command: "topicinfo", description: "Show context state, paths, and session" },
  { command: "run", description: "Run one explicit task in this topic" },
  { command: "resume", description: "Resume the current Codex session" },
  { command: "loop", description: "Resume and keep pushing to a checkpoint" },
  { command: "archive", description: "Archive this context and detach topic" },
  { command: "detach", description: "Detach this topic without deleting files" },
  { command: "tail", description: "Show the latest local run log tail" },
  { command: "artifacts", description: "Show cached or live artifacts notes" },
  { command: "usage", description: "Show token usage from the latest log" }
];

export interface TelegramUser {
  id: number;
  username?: string;
  first_name?: string;
  last_name?: string;
}

export interface TelegramChat {
  id: number;
  type: string;
  title?: string;
  username?: string;
}

export interface TelegramPhotoSize {
  file_id: string;
  file_unique_id: string;
  width: number;
  height: number;
  file_size?: number;
}

export interface TelegramDocument {
  file_id: string;
  file_unique_id: string;
  file_name?: string;
  mime_type?: string;
  file_size?: number;
}

export interface TelegramVideo {
  file_id: string;
  file_unique_id: string;
  width: number;
  height: number;
  duration: number;
  file_name?: string;
  mime_type?: string;
  file_size?: number;
}

export interface TelegramAudio {
  file_id: string;
  file_unique_id: string;
  duration: number;
  performer?: string;
  title?: string;
  file_name?: string;
  mime_type?: string;
  file_size?: number;
}

export interface TelegramVoice {
  file_id: string;
  file_unique_id: string;
  duration: number;
  mime_type?: string;
  file_size?: number;
}

export interface TelegramAnimation {
  file_id: string;
  file_unique_id: string;
  width: number;
  height: number;
  duration: number;
  file_name?: string;
  mime_type?: string;
  file_size?: number;
}

export interface TelegramMessage {
  message_id: number;
  date: number;
  text?: string;
  caption?: string;
  is_topic_message?: boolean;
  message_thread_id?: number;
  photo?: TelegramPhotoSize[];
  document?: TelegramDocument;
  video?: TelegramVideo;
  audio?: TelegramAudio;
  voice?: TelegramVoice;
  animation?: TelegramAnimation;
  from?: TelegramUser;
  chat: TelegramChat;
}

interface TelegramUpdate {
  update_id: number;
  message?: TelegramMessage;
}

export interface TelegramTarget {
  chatId: number;
  threadId: number | null;
}

export type TelegramChatAction = "typing" | "upload_photo" | "upload_document";

export interface TelegramOutgoingAttachment {
  kind: TelegramAttachmentKind;
  fileName: string;
  bytes: Uint8Array;
  mimeType?: string | null;
  caption?: string | null;
}

export interface TelegramRemoteFile {
  file_id: string;
  file_unique_id?: string;
  file_size?: number;
  file_path?: string;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function compactError(error: unknown): string {
  return redactTelegramSecrets(error instanceof Error ? error.message : String(error));
}

function redactTelegramSecrets(value: string): string {
  return value
    .replace(/bot\d+:[A-Za-z0-9_-]+/g, "bot<redacted>")
    .replace(/(api\.telegram\.org\/(?:file\/)?bot)[^/\s)]+/g, "$1<redacted>");
}

function scopeLabel(scope: TelegramBotCommandScope): string {
  switch (scope.type) {
    case "default":
      return "default";
    case "all_private_chats":
      return "all_private_chats";
    case "all_group_chats":
      return "all_group_chats";
    case "chat_member":
      return `chat_member(${scope.chat_id ?? "unknown"},${scope.user_id ?? "unknown"})`;
  }
}

function scopePayload(scope: TelegramBotCommandScope): Record<string, unknown> {
  if (scope.type === "chat_member") {
    return {
      type: scope.type,
      chat_id: scope.chat_id,
      user_id: scope.user_id
    };
  }

  return {
    type: scope.type
  };
}

function chunkText(text: string, limit = 3500): string[] {
  if (text.length <= limit) {
    return [text];
  }

  const chunks: string[] = [];
  let remaining = text;

  while (remaining.length > limit) {
    let splitAt = remaining.lastIndexOf("\n", limit);
    if (splitAt < limit * 0.5) {
      splitAt = limit;
    }

    chunks.push(remaining.slice(0, splitAt).trimEnd());
    remaining = remaining.slice(splitAt).trimStart();
  }

  if (remaining) {
    chunks.push(remaining);
  }

  return chunks;
}

export class TelegramBot {
  private running = false;

  constructor(
    private readonly config: FactoryConfig,
    private readonly db: FactoryDb
  ) {}

  isConfigured(): boolean {
    return Boolean(this.config.telegramBotToken);
  }

  async sendText(target: TelegramTarget, text: string): Promise<void> {
    if (!this.isConfigured()) {
      return;
    }

    for (const chunk of chunkText(text)) {
      await this.api("sendMessage", {
        chat_id: target.chatId,
        message_thread_id: target.threadId ?? undefined,
        text: chunk,
        disable_web_page_preview: true
      });
    }
  }

  async sendAttachment(target: TelegramTarget, attachment: TelegramOutgoingAttachment): Promise<void> {
    if (!this.isConfigured()) {
      return;
    }

    const form = new FormData();
    form.set("chat_id", String(target.chatId));

    if (target.threadId !== null) {
      form.set("message_thread_id", String(target.threadId));
    }

    if (attachment.caption?.trim()) {
      form.set("caption", attachment.caption.trim());
    }

    const fieldName = attachment.kind === "photo" ? "photo" : "document";
    const blob = new Blob([attachment.bytes], {
      type: attachment.mimeType || "application/octet-stream"
    });
    form.set(fieldName, blob, attachment.fileName);

    await this.apiMultipart(attachment.kind === "photo" ? "sendPhoto" : "sendDocument", form);
  }

  async sendChatAction(target: TelegramTarget, action: TelegramChatAction): Promise<void> {
    if (!this.isConfigured()) {
      return;
    }

    await this.api("sendChatAction", {
      chat_id: target.chatId,
      message_thread_id: target.threadId ?? undefined,
      action
    });
  }

  async getFile(fileId: string): Promise<TelegramRemoteFile> {
    if (!this.isConfigured()) {
      throw new Error("telegram bot token is not configured");
    }

    return this.api<TelegramRemoteFile>("getFile", {
      file_id: fileId
    });
  }

  async downloadFile(filePath: string): Promise<Uint8Array> {
    if (!this.isConfigured()) {
      throw new Error("telegram bot token is not configured");
    }

    let response: Response;
    try {
      response = await fetch(`https://api.telegram.org/file/bot${this.config.telegramBotToken}/${filePath}`);
    } catch (error) {
      throw new Error(`telegram file download request failed: ${compactError(error)}`);
    }
    if (!response.ok) {
      throw new Error(`telegram file download failed with ${response.status}`);
    }

    return new Uint8Array(await response.arrayBuffer());
  }

  async getCommands(scope: TelegramBotCommandScope): Promise<TelegramBotCommand[]> {
    if (!this.isConfigured()) {
      return [];
    }

    const commands = await this.api<Array<{ command: string; description: string }>>("getMyCommands", {
      scope: scopePayload(scope)
    });

    return commands.map((command) => ({
      command: command.command,
      description: command.description
    }));
  }

  async syncCommands(
    options: {
      currentChatId?: number | null;
      commands?: TelegramBotCommand[];
    } = {}
  ): Promise<TelegramCommandSyncResult[]> {
    if (!this.isConfigured()) {
      return [];
    }

    const commands = options.commands || TELEGRAM_BOT_COMMANDS;
    const scopes = this.commandScopes(options.currentChatId ?? null);
    const payloadCommands = commands.map((command) => ({
      command: command.command,
      description: command.description
    }));
    const results: TelegramCommandSyncResult[] = [];

    for (const scope of scopes) {
      let setOk = false;
      let setError: string | null = null;
      let verifyOk = false;
      let verifyError: string | null = null;
      let verifiedCommands: TelegramBotCommand[] = [];

      try {
        await this.api("setMyCommands", {
          scope: scopePayload(scope),
          commands: payloadCommands
        });
        setOk = true;
      } catch (error) {
        setError = compactError(error);
      }

      try {
        verifiedCommands = await this.getCommands(scope);
        verifyOk = true;
      } catch (error) {
        verifyError = compactError(error);
      }

      results.push({
        label: scopeLabel(scope),
        scope,
        setOk,
        setError,
        verifyOk,
        verifyError,
        commands: verifiedCommands
      });
    }

    return results;
  }

  listCommandScopes(currentChatId: number | null = null): TelegramBotCommandScope[] {
    return this.commandScopes(currentChatId);
  }

  start(onMessage: (message: TelegramMessage) => Promise<void>): void {
    if (this.running || !this.isConfigured()) {
      return;
    }

    this.running = true;
    void this.pollLoop(onMessage);
  }

  private async pollLoop(onMessage: (message: TelegramMessage) => Promise<void>): Promise<void> {
    let nextOffset = Number(this.db.getSetting("telegram.last_update_id") || "0");

    while (this.running) {
      try {
        const updates = await this.api<TelegramUpdate[]>("getUpdates", {
          offset: nextOffset > 0 ? nextOffset + 1 : undefined,
          timeout: this.config.telegramPollTimeoutSeconds,
          allowed_updates: ["message"]
        });

        for (const update of updates) {
          nextOffset = update.update_id;
          this.db.setSetting("telegram.last_update_id", String(update.update_id));

          if (update.message) {
            await onMessage(update.message);
          }
        }
      } catch (error) {
        console.error("telegram poll failed", compactError(error));
        await sleep(3000);
      }
    }
  }

  private commandScopes(currentChatId: number | null): TelegramBotCommandScope[] {
    const scopes: TelegramBotCommandScope[] = [
      { type: "default" },
      { type: "all_private_chats" },
      { type: "all_group_chats" }
    ];
    const chatId = this.resolveScopedChatId(currentChatId);

    if (chatId !== null) {
      scopes.push({
        type: "chat_member",
        chat_id: chatId,
        user_id: this.config.allowedTelegramUserId
      });
    }

    return scopes;
  }

  private resolveScopedChatId(currentChatId: number | null): number | null {
    if (this.config.telegramControlChatId !== null) {
      return this.config.telegramControlChatId;
    }

    if (currentChatId !== null) {
      return currentChatId;
    }

    const knownChatIds = [...new Set(this.db.listContexts().map((context) => context.telegramChatId).filter((chatId) => chatId !== null))];
    return knownChatIds.length === 1 ? knownChatIds[0] : null;
  }

  private async api<T>(method: string, payload: Record<string, unknown>): Promise<T> {
    let response: Response;
    try {
      response = await fetch(`https://api.telegram.org/bot${this.config.telegramBotToken}/${method}`, {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify(payload)
      });
    } catch (error) {
      throw new Error(`telegram api ${method} request failed: ${compactError(error)}`);
    }

    return this.parseApiResponse<T>(method, response);
  }

  private async apiMultipart<T>(method: string, payload: FormData): Promise<T> {
    let response: Response;
    try {
      response = await fetch(`https://api.telegram.org/bot${this.config.telegramBotToken}/${method}`, {
        method: "POST",
        body: payload
      });
    } catch (error) {
      throw new Error(`telegram api ${method} multipart request failed: ${compactError(error)}`);
    }

    return this.parseApiResponse<T>(method, response);
  }

  private async parseApiResponse<T>(method: string, response: Response): Promise<T> {
    if (!response.ok) {
      throw new Error(`telegram api ${method} failed with ${response.status}`);
    }

    const body = (await response.json()) as {
      ok: boolean;
      description?: string;
      result: T;
    };

    if (!body.ok) {
      throw new Error(body.description || `telegram api ${method} failed`);
    }

    return body.result;
  }
}
