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
import { ensureBasicLoops } from "./basic-loops";
import { reportCuratorStatus } from "./curator-report";

ensureProjectPaths();

const db = new FactoryDb(config.dbPath);
const contexts = new ContextService(db, config.usageAdapter, config.contextsDir);
const workers = new WorkerService(config, db);
const telegram = new TelegramBot(config, db);
const cronManager = new CronManager(config, db, workers);
const dispatcher = new Dispatcher(config, db, contexts, workers, telegram, cronManager);
const cronScheduler = new CronScheduler(config, db, cronManager, dispatcher, workers, telegram);
const commands = new CommandHandler(config, db, telegram, contexts, workers, dispatcher, cronManager, cronScheduler);

const curatorControls = telegram.isConfigured()
  ? {
      runCurator: async (): Promise<string> => {
        const job = db
          .listCronJobs()
          .filter((entry) => entry.kind === "codex" && !entry.runner && entry.label.toLowerCase() === "brain-curator")
          .sort((a, b) => (a.executionContextSlug === "brainstack-routines" ? -1 : b.executionContextSlug === "brainstack-routines" ? 1 : 0))[0];
        if (!job) {
          throw new Error("no brain-curator job is installed; run `brainctl curator install` or /cron_install_brain_curator first");
        }
        return await cronScheduler.runJobNow(job);
      },
      installCurator: async (): Promise<string> => {
        return await ensureBasicLoops(config, contexts, workers, cronManager);
      }
    }
  : undefined;

startDashboard(config, db, workers, telegram, curatorControls);

try {
  const pruned = db.pruneHistory();
  if (pruned.queuedTurns || pruned.cronRuns) {
    console.log(`pruned job history: ${pruned.queuedTurns} queued turn(s), ${pruned.cronRuns} cron run(s)`);
  }
} catch (error) {
  console.error("job history pruning failed", error);
}

setInterval(() => {
  try {
    db.pruneHistory();
  } catch (error) {
    console.error("scheduled job history pruning failed", error);
  }
}, 24 * 60 * 60 * 1000);

void workers.refreshWorkers().catch((error) => {
  console.error("initial worker refresh failed", error);
});

setInterval(() => {
  void workers.refreshWorkers().catch((error) => {
    console.error("scheduled worker refresh failed", error);
  });
}, 60_000);

if (telegram.isConfigured()) {
  dispatcher.recoverQueuedTurns();
  commands.recoverPendingText();
  void ensureBasicLoops(config, contexts, workers, cronManager)
    .then(async (result) => {
      console.log(`basic loops: ${result}`);
      const curatorJob = db
        .listCronJobs()
        .find((entry) => entry.kind === "codex" && !entry.runner && entry.label.toLowerCase() === "brain-curator" && entry.executionContextSlug === "brainstack-routines");
      if (curatorJob) {
        await reportCuratorStatus(config, { installed: true, next_run_at: curatorJob.nextRunAt ?? null });
      }
    })
    .catch((error) => {
      console.error("basic loops bootstrap failed", error);
    });
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
