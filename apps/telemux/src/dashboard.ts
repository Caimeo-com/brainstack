import { FactoryDb } from "./db";
import { WorkerService } from "./workers";
import type { FactoryConfig } from "./config";

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function badge(status: string): string {
  const color =
    status === "healthy" || status === "active"
      ? "#165534"
      : status === "pending"
        ? "#92400e"
        : status === "archived"
          ? "#374151"
          : "#991b1b";
  const bg =
    status === "healthy" || status === "active"
      ? "#dcfce7"
      : status === "pending"
        ? "#fef3c7"
        : status === "archived"
          ? "#e5e7eb"
          : "#fee2e2";

  return `<span style="display:inline-block;padding:0.15rem 0.5rem;border-radius:999px;background:${bg};color:${color};font-size:0.85rem;">${escapeHtml(
    status
  )}</span>`;
}

export function startDashboard(config: FactoryConfig, db: FactoryDb, workers: WorkerService): void {
  Bun.serve({
    hostname: config.dashboardHost,
    port: config.dashboardPort,
    fetch(request) {
      const url = new URL(request.url);

      if (url.pathname === "/healthz") {
        return Response.json({
          ok: true,
          dashboard: `${config.dashboardHost}:${config.dashboardPort}`,
          telegramConfigured: Boolean(config.telegramBotToken),
          workers: workers.knownHosts().length,
          contexts: db.listContexts().length,
          crons: db.listCronJobs().length
        });
      }

      const workerRows = db.listWorkers();
      const contextRows = db.listContexts();
      const cronRows = db.listCronJobs();

      const workerHtml = workerRows.length
        ? workerRows
            .map(
              (worker) => `
                <tr>
                  <td>${escapeHtml(worker.host)}</td>
                  <td>${badge(worker.status)}</td>
                  <td>${escapeHtml(worker.transport || "n/a")}</td>
                  <td>${escapeHtml(worker.localExecution ? "yes" : "no")}</td>
                  <td>${escapeHtml(worker.lastSeenAt || "n/a")}</td>
                  <td><pre>${escapeHtml(worker.lastError || worker.details || "")}</pre></td>
                </tr>
              `
            )
            .join("")
        : `<tr><td colspan="6">No worker health data yet.</td></tr>`;

      const contextHtml = contextRows.length
        ? contextRows
            .map(
              (context) => `
                <tr>
                  <td>${escapeHtml(context.slug)}</td>
                  <td>${escapeHtml(context.machine)}</td>
                  <td>${escapeHtml(context.kind)}</td>
                  <td>${badge(context.state)}</td>
                  <td>${escapeHtml(
                    context.telegramThreadId === null
                      ? `${context.telegramChatId ?? "unbound"}`
                      : `${context.telegramChatId}:${context.telegramThreadId}`
                  )}</td>
                  <td><code>${escapeHtml(context.worktreePath)}</code></td>
                  <td>${escapeHtml(context.codexSessionId || "none")}</td>
                  <td><pre>${escapeHtml(context.lastSummary || "")}</pre></td>
                </tr>
              `
            )
            .join("")
        : `<tr><td colspan="8">No contexts yet.</td></tr>`;

      const cronHtml = cronRows.length
        ? cronRows
            .map(
              (job) => `
                <tr>
                  <td>${escapeHtml(job.id)}</td>
                  <td>${escapeHtml(job.label)}</td>
                  <td>${escapeHtml(job.kind)}</td>
                  <td>${badge(job.enabled ? "active" : "archived")}</td>
                  <td>${escapeHtml(job.nextRunAt || "none")}</td>
                  <td>${escapeHtml(job.executionContextSlug || "none")}</td>
                  <td>${escapeHtml(`${job.targetChatId}:${job.targetThreadId ?? "none"}`)}</td>
                </tr>
              `
            )
            .join("")
        : `<tr><td colspan="7">No cron jobs yet.</td></tr>`;

      const html = `
        <!doctype html>
        <html lang="en">
          <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <meta http-equiv="refresh" content="15" />
            <title>Private Dev Factory</title>
            <style>
              :root {
                color-scheme: light;
                --bg: #f5efe6;
                --panel: #fffdf8;
                --ink: #1f2937;
                --muted: #6b7280;
                --line: #d6d3d1;
                --accent: #1d4ed8;
              }
              * { box-sizing: border-box; }
              body {
                margin: 0;
                font-family: "Iosevka Etoile", "IBM Plex Sans", sans-serif;
                background:
                  radial-gradient(circle at top right, rgba(29, 78, 216, 0.12), transparent 28rem),
                  linear-gradient(180deg, #f8f5ef 0%, var(--bg) 100%);
                color: var(--ink);
              }
              main { max-width: 1280px; margin: 0 auto; padding: 2rem; }
              h1, h2 { margin: 0 0 0.8rem; }
              p { color: var(--muted); }
              .grid { display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); }
              .panel {
                background: rgba(255, 253, 248, 0.9);
                border: 1px solid var(--line);
                border-radius: 18px;
                padding: 1rem 1.2rem;
                box-shadow: 0 16px 40px rgba(15, 23, 42, 0.08);
              }
              table { width: 100%; border-collapse: collapse; font-size: 0.95rem; }
              th, td { border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; padding: 0.65rem 0.5rem; }
              th { font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }
              pre, code {
                margin: 0;
                font-family: "Iosevka Term", "IBM Plex Mono", monospace;
                white-space: pre-wrap;
                word-break: break-word;
              }
              .meta { display: flex; gap: 1rem; flex-wrap: wrap; color: var(--muted); font-size: 0.95rem; }
              @media (max-width: 760px) {
                main { padding: 1rem; }
                table, thead, tbody, tr, th, td { display: block; }
                th { padding-bottom: 0.2rem; }
                td { padding-top: 0; }
              }
            </style>
          </head>
          <body>
            <main>
              <div class="panel">
                <h1>Private Dev Factory</h1>
                <div class="meta">
                  <span>dashboard: ${escapeHtml(`${config.dashboardHost}:${config.dashboardPort}`)}</span>
                  <span>telegram: ${config.telegramBotToken ? "configured" : "missing token"}</span>
                  <span>workers configured: ${escapeHtml(String(workers.knownHosts().length))}</span>
                  <span>contexts: ${escapeHtml(String(contextRows.length))}</span>
                  <span>crons: ${escapeHtml(String(cronRows.length))}</span>
                </div>
              </div>

              <div class="grid" style="margin-top: 1rem;">
                <section class="panel">
                  <h2>Workers</h2>
                  <table>
                    <thead>
                      <tr>
                        <th>Host</th>
                        <th>Status</th>
                        <th>Transport</th>
                        <th>Local</th>
                        <th>Last Seen</th>
                        <th>Details</th>
                      </tr>
                    </thead>
                    <tbody>${workerHtml}</tbody>
                  </table>
                </section>

                <section class="panel">
                  <h2>Contexts</h2>
                  <table>
                    <thead>
                      <tr>
                        <th>Slug</th>
                        <th>Machine</th>
                        <th>Kind</th>
                        <th>State</th>
                        <th>Topic</th>
                        <th>Worktree</th>
                        <th>Session</th>
                        <th>Summary</th>
                      </tr>
                    </thead>
                    <tbody>${contextHtml}</tbody>
                  </table>
                </section>

                <section class="panel">
                  <h2>Crons</h2>
                  <table>
                    <thead>
                      <tr>
                        <th>Id</th>
                        <th>Label</th>
                        <th>Kind</th>
                        <th>State</th>
                        <th>Next Run</th>
                        <th>Context</th>
                        <th>Target</th>
                      </tr>
                    </thead>
                    <tbody>${cronHtml}</tbody>
                  </table>
                </section>
              </div>
            </main>
          </body>
        </html>
      `;

      return new Response(html, {
        headers: {
          "content-type": "text/html; charset=utf-8"
        }
      });
    }
  });
}
