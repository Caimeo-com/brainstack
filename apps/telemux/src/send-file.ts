import { config, ensureProjectPaths } from "./config";
import { FactoryDb } from "./db";
import { TelegramBot } from "./telegram";
import { parseTelegramFileSendArgs, sendTelegramLocalFile } from "./telegram-file-send";

function usage(): string {
  return [
    "Usage:",
    "  bun run apps/telemux/src/send-file.ts --file PATH [--caption TEXT] [--context SLUG|--chat-id ID] [--thread-id ID] [--kind document|photo] [--display-name NAME] [--max-bytes N] [--allow-sensitive] [--delete-after-send] [--json]",
    "",
    "Defaults to FACTORY_TELEGRAM_CONTROL_CHAT_ID when no context or chat id is supplied."
  ].join("\n");
}

async function main(argv: string[]): Promise<void> {
  if (argv.includes("--help") || argv.includes("-h")) {
    console.log(usage());
    return;
  }

  const options = parseTelegramFileSendArgs(argv);
  ensureProjectPaths();
  const db = new FactoryDb(config.dbPath);
  const telegram = new TelegramBot(config, db);
  const result = await sendTelegramLocalFile(config, db, telegram, options);

  if (options.json) {
    console.log(JSON.stringify({ ok: true, ...result }, null, 2));
    return;
  }

  console.log(
    [
      `sent ${result.fileName}`,
      `bytes=${result.sizeBytes}`,
      `kind=${result.kind}`,
      `target=${result.target.mode}${result.target.contextSlug ? `:${result.target.contextSlug}` : ""}`,
      `thread=${result.target.threadId ?? "none"}`
    ].join(" ")
  );
}

main(Bun.argv.slice(2)).catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
