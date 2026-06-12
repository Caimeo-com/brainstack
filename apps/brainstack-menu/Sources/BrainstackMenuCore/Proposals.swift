import Foundation

/// Lightweight proposal summary parsed from `brainctl proposals list --json`.
public struct ProposalSummary: Identifiable, Sendable, Equatable {
  public let id: String
  public let title: String
  public let status: String
  public let targetPage: String?
  public let risk: String?
  public let createdAt: String
  public let project: String?
  public let scope: String?
  public let memoryKind: String?
  public let qualityDecision: String?
  public let qualityScore: Double?
  public let legacyFormat: Bool
  public let clusterKey: String?
  public let clusterLabel: String?

  public init(json: JSONValue) {
    self.id = json["id"]?.stringValue ?? ""
    self.title = json["title"]?.stringValue ?? "(untitled)"
    self.status = json["status"]?.stringValue ?? "unknown"
    self.targetPage = json["target_page"]?.stringValue
    self.risk = json["risk"]?.stringValue
    self.createdAt = json["created_at"]?.stringValue ?? ""
    self.project = json["project"]?.stringValue
    self.scope = json["scope"]?.stringValue
    self.memoryKind = json["memory_kind"]?.stringValue
    self.qualityDecision = json["quality_decision"]?.stringValue
    self.qualityScore = json["quality_score"]?.numberValue
    self.legacyFormat = json["legacy_format"]?.boolValue ?? false
    self.clusterKey = json["cluster_key"]?.stringValue
    self.clusterLabel = json["cluster_label"]?.stringValue
  }

  public static func parseList(_ text: String) -> [ProposalSummary]? {
    guard let data = text.data(using: .utf8),
          let decoded = try? JSONDecoder().decode(JSONValue.self, from: data),
          let proposals = decoded["proposals"]?.arrayValue else {
      return nil
    }
    return proposals.map(ProposalSummary.init(json:)).filter { !$0.id.isEmpty }
  }
}

/// Full proposal detail parsed from `brainctl proposals show <id> --json`.
public struct ProposalDetail: Sendable, Equatable {
  public let summary: ProposalSummary
  public let body: String
  public let diff: String?
  public let reason: String?
  public let sourceIds: [String]
  public let confidence: Double?
  public let sourceHarness: String?
  public let sourceMachine: String?
  public let sourceType: String?
  public let relatedRepo: String?
  public let project: String?
  public let domain: String?
  public let scope: String?
  public let memoryKind: String?
  public let context: String?
  public let applicability: String?
  public let nonApplicability: String?
  public let evidenceRefs: [String]
  public let reviewAfter: String?
  public let expiresAt: String?
  public let qualityDecision: String?
  public let qualityScore: Double?
  public let qualityReasons: [String]
  public let legacyFormat: Bool
  public let clusterKey: String?
  public let clusterLabel: String?

  public static func parse(_ text: String) -> ProposalDetail? {
    guard let data = text.data(using: .utf8),
          let decoded = try? JSONDecoder().decode(JSONValue.self, from: data),
          let proposal = decoded["proposal"] else {
      return nil
    }
    let summary = ProposalSummary(json: proposal)
    guard !summary.id.isEmpty else {
      return nil
    }
    return ProposalDetail(
      summary: summary,
      body: decoded["body"]?.stringValue ?? "",
      diff: decoded["diff"]?.stringValue,
      reason: proposal["reason"]?.stringValue,
      sourceIds: proposal["source_ids"]?.arrayValue?.compactMap(\.stringValue) ?? [],
      confidence: proposal["confidence"]?.numberValue,
      sourceHarness: proposal["source_harness"]?.stringValue,
      sourceMachine: proposal["source_machine"]?.stringValue,
      sourceType: proposal["source_type"]?.stringValue,
      relatedRepo: proposal["related_repo"]?.stringValue,
      project: proposal["project"]?.stringValue,
      domain: proposal["domain"]?.stringValue,
      scope: proposal["scope"]?.stringValue,
      memoryKind: proposal["memory_kind"]?.stringValue,
      context: proposal["context"]?.stringValue,
      applicability: proposal["applicability"]?.stringValue,
      nonApplicability: proposal["non_applicability"]?.stringValue,
      evidenceRefs: proposal["evidence_refs"]?.arrayValue?.compactMap(\.stringValue) ?? [],
      reviewAfter: proposal["review_after"]?.stringValue,
      expiresAt: proposal["expires_at"]?.stringValue,
      qualityDecision: proposal["quality_decision"]?.stringValue,
      qualityScore: proposal["quality_score"]?.numberValue,
      qualityReasons: proposal["quality_reasons"]?.arrayValue?.compactMap(\.stringValue) ?? [],
      legacyFormat: proposal["legacy_format"]?.boolValue ?? false,
      clusterKey: proposal["cluster_key"]?.stringValue,
      clusterLabel: proposal["cluster_label"]?.stringValue
    )
  }
}

/// Notification policy: notify only on meaningful state transitions, never repeatedly
/// for an unchanged degraded state.
public struct TransitionDetector: Sendable {
  public struct Snapshot: Equatable, Sendable {
    public let overall: OverallState
    public let outboxTerminalOrCorrupt: Bool
    public let curatorFailing: Bool
    public let openProposals: Int

    public init(overall: OverallState, outboxTerminalOrCorrupt: Bool, curatorFailing: Bool, openProposals: Int) {
      self.overall = overall
      self.outboxTerminalOrCorrupt = outboxTerminalOrCorrupt
      self.curatorFailing = curatorFailing
      self.openProposals = openProposals
    }

    public static func from(report: StatusReport?, overall: OverallState) -> Snapshot {
      let outbox = report?.sections["outbox"]
      let outboxStuck = outbox?.state == .fail || (outbox?.detail.contains("terminal=0") == false && outbox?.detail.contains("terminal=") == true)
      let curator = report?.sections["curator"]
      let curatorFailing = curator?.data?["curator"]?["last_run_ok"]?.boolValue == false
      let open = Int(report?.sections["curator"]?.data?["open_proposals"]?.numberValue ?? 0)
      return Snapshot(overall: overall, outboxTerminalOrCorrupt: outboxStuck, curatorFailing: curatorFailing, openProposals: open)
    }
  }

  public init() {}

  /// Returns user-facing notification messages for the transition between snapshots.
  public func messages(from previous: Snapshot?, to current: Snapshot, operatorMode: Bool) -> [String] {
    guard let previous else {
      return []
    }
    var output: [String] = []
    if previous.overall != .red && current.overall == .red {
      output.append("Brainstack is broken on this machine.")
    }
    if previous.overall == .red && (current.overall == .green || current.overall == .yellow) {
      output.append("Brainstack recovered.")
    }
    if !previous.outboxTerminalOrCorrupt && current.outboxTerminalOrCorrupt {
      output.append("Outbox has stuck or corrupt items.")
    }
    if !previous.curatorFailing && current.curatorFailing {
      output.append("Brain curator run failed.")
    }
    if operatorMode && current.openProposals > previous.openProposals {
      output.append("\(current.openProposals) proposal(s) awaiting action.")
    }
    return output
  }
}
