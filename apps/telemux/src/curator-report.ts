import type { FactoryConfig } from "./config";

export interface CuratorStatusUpdate {
  installed?: boolean;
  last_run_id?: string | null;
  last_run_started_at?: string | null;
  last_run_finished_at?: string | null;
  last_run_ok?: boolean | null;
  last_run_failures?: string[];
  last_run_summary?: string | null;
  next_run_at?: string | null;
  cursor?: string | null;
}

/**
 * Best-effort curator status reporting into braind. Requires the optional brain
 * admin token; silently a no-op without it so Telegram-only installs keep working.
 */
export async function reportCuratorStatus(config: FactoryConfig, update: CuratorStatusUpdate): Promise<boolean> {
  if (!config.brainBaseUrl || !config.brainAdminToken) {
    return false;
  }
  try {
    const response = await fetch(new URL("/api/curator/status", config.brainBaseUrl).toString(), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${config.brainAdminToken}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(update),
      signal: AbortSignal.timeout(10_000)
    });
    return response.ok;
  } catch {
    return false;
  }
}

export async function fetchCuratorStatus(config: FactoryConfig): Promise<Record<string, unknown> | null> {
  if (!config.brainBaseUrl) {
    return null;
  }
  try {
    const response = await fetch(new URL("/api/curator/status", config.brainBaseUrl).toString(), {
      signal: AbortSignal.timeout(10_000)
    });
    if (!response.ok) {
      return null;
    }
    return (await response.json()) as Record<string, unknown>;
  } catch {
    return null;
  }
}

export interface BrainProposalSummary {
  id: string;
  title: string;
  status: string;
  target_page: string | null;
  risk: string | null;
  created_at: string;
  quality_decision: string | null;
  project: string | null;
  scope: string | null;
  cluster_label: string | null;
  legacy_format: boolean;
}

export interface ProposalListOptions {
  status?: string;
}

export async function fetchProposals(config: FactoryConfig, options: ProposalListOptions = {}): Promise<BrainProposalSummary[] | null> {
  if (!config.brainBaseUrl) {
    return null;
  }
  try {
    const url = new URL("/api/proposals", config.brainBaseUrl);
    url.searchParams.set("status", options.status || "open");
    const response = await fetch(url.toString(), {
      signal: AbortSignal.timeout(10_000)
    });
    if (!response.ok) {
      return null;
    }
    const parsed = (await response.json()) as { proposals?: Array<Record<string, unknown>> };
    return (parsed.proposals || []).map((proposal) => ({
      id: String(proposal.id || ""),
      title: String(proposal.title || ""),
      status: String(proposal.status || ""),
      target_page: typeof proposal.target_page === "string" ? proposal.target_page : null,
      risk: typeof proposal.risk === "string" ? proposal.risk : null,
      created_at: String(proposal.created_at || ""),
      quality_decision: typeof proposal.quality_decision === "string" ? proposal.quality_decision : null,
      project: typeof proposal.project === "string" ? proposal.project : null,
      scope: typeof proposal.scope === "string" ? proposal.scope : null,
      cluster_label: typeof proposal.cluster_label === "string" ? proposal.cluster_label : null,
      legacy_format: proposal.legacy_format === true
    }));
  } catch {
    return null;
  }
}

export async function decideProposal(
  config: FactoryConfig,
  id: string,
  action: "approve" | "reject" | "apply",
  decidedBy: string,
  reason?: string
): Promise<{ ok: boolean; message: string }> {
  if (!config.brainBaseUrl) {
    return { ok: false, message: "BRAIN_BASE_URL is not configured." };
  }
  if (!config.brainAdminToken) {
    return { ok: false, message: "FACTORY_BRAIN_ADMIN_TOKEN is not configured; approve/reject from Telegram is disabled. Use `brainctl proposals` on the control host." };
  }
  try {
    const response = await fetch(new URL(`/api/proposals/${encodeURIComponent(id)}/${action}`, config.brainBaseUrl).toString(), {
      method: "POST",
      headers: {
        Authorization: `Bearer ${config.brainAdminToken}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ decided_by: decidedBy, ...(reason ? { reason } : {}) }),
      signal: AbortSignal.timeout(30_000)
    });
    const text = await response.text();
    let parsed: Record<string, unknown> = {};
    try {
      parsed = JSON.parse(text) as Record<string, unknown>;
    } catch {
      // Non-JSON error bodies fall through to the generic message below.
    }
    if (!response.ok) {
      return { ok: false, message: `Proposal ${action} failed (HTTP ${response.status}): ${String(parsed.error || "unknown error").slice(0, 300)}` };
    }
    const blocked = typeof parsed.blocked_reason === "string" ? parsed.blocked_reason : null;
    return {
      ok: true,
      message: blocked
        ? `Proposal ${id} was not applied: ${blocked}`
        : `Proposal ${id} ${action} → status=${String(parsed.status)}${parsed.commit ? ` commit=${String(parsed.commit).slice(0, 10)}` : ""}`
    };
  } catch (error) {
    return { ok: false, message: `Proposal ${action} failed: ${error instanceof Error ? error.message : String(error)}` };
  }
}
