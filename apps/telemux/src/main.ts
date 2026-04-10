import { config, ensureProjectPaths } from "./config";
import { CronManager } from "./cron-manager";
import { CronScheduler } from "./cron-scheduler";
import { ContextService } from "./contexts";
import { FactoryDb } from "./db";
import { Dispatcher } from "./dispatcher";
import { CommandHandler } from "./commands";
import { startDashboard } from "./dashboard";
import { TelegramBot } from "./telegram";
import { WorkerService } from "./workers";

ensureProjectPaths();

const db = new FactoryDb(config.dbPath);
const contexts = new ContextService(db, config.usageAdapter, config.contextsDir);
const workers = new WorkerService(config, db);
const telegram = new TelegramBot(config, db);
const cronManager = new CronManager(config, db, workers);
const dispatcher = new Dispatcher(config, db, contexts, workers, telegram, cronManager);
const commands = new CommandHandler(config, db, telegram, contexts, workers, dispatcher, cronManager);
const cronScheduler = new CronScheduler(config, db, cronManager, dispatcher, telegram);

startDashboard(config, db, workers);

void workers.refreshWorkers().catch((error) => {
  console.error("initial worker refresh failed", error);
});

setInterval(() => {
  void workers.refreshWorkers().catch((error) => {
    console.error("scheduled worker refresh failed", error);
  });
}, 60_000);

if (telegram.isConfigured()) {
  void telegram
    .syncCommands()
    .then((results) => {
      for (const result of results) {
        console.log(
          [
            `telegram commands scope=${result.label}`,
            `set=${result.setOk ? "ok" : `failed:${result.setError || "unknown"}`}`,
            `verify=${result.verifyOk ? `ok:${result.commands.length}` : `failed:${result.verifyError || "unknown"}`}`
          ].join(" ")
        );
      }
    })
    .catch((error) => {
      console.error("telegram command registration failed", error);
    });
  telegram.start((message) => commands.handleMessage(message));
  cronScheduler.start();
  console.log("telegram polling enabled");
} else {
  console.log("telegram polling disabled: FACTORY_TELEGRAM_BOT_TOKEN is empty");
}

console.log(`dashboard listening on http://${config.dashboardHost}:${config.dashboardPort}/`);
