import Foundation

/// Lightweight proposal summary parsed from `brainctl proposals list --json`.
public struct ProposalSummary: Identifiable, Sendable, Equatable {
  public let id: String
  public let title: String
  public let status: String
  public let targetPage: String?
  public let baseSHA256: String?
  public let risk: String?
  public let createdAt: String
  public let updatedAt: String?
  public let project: String?
  public let domain: String?
  public let scope: String?
  public let memoryKind: String?
  public let sourceIds: [String]
  public let sourceHarness: String?
  public let sourceMachine: String?
  public let sourceType: String?
  public let relatedRepo: String?
  public let context: String?
  public let applicability: String?
  public let nonApplicability: String?
  public let evidenceRefs: [String]
  public let reviewAfter: String?
  public let expiresAt: String?
  public let confidence: Double?
  public let reason: String?
  public let searchText: String?
  public let qualityDecision: String?
  public let qualityScore: Double?
  public let qualityReasons: [String]
  public let legacyFormat: Bool
  public let clusterKey: String?
  public let clusterLabel: String?
  public let hasProposedContent: Bool

  public init(json: JSONValue) {
    self.id = json["id"]?.stringValue ?? ""
    self.title = json["title"]?.stringValue ?? "(untitled)"
    self.status = json["status"]?.stringValue ?? "unknown"
    self.targetPage = json["target_page"]?.stringValue
    self.baseSHA256 = json["base_sha256"]?.stringValue
    self.risk = json["risk"]?.stringValue
    self.createdAt = json["created_at"]?.stringValue ?? ""
    self.updatedAt = json["updated_at"]?.stringValue
    self.project = json["project"]?.stringValue
    self.domain = json["domain"]?.stringValue
    self.scope = json["scope"]?.stringValue
    self.memoryKind = json["memory_kind"]?.stringValue
    self.sourceIds = json["source_ids"]?.arrayValue?.compactMap(\.stringValue) ?? []
    self.sourceHarness = json["source_harness"]?.stringValue
    self.sourceMachine = json["source_machine"]?.stringValue
    self.sourceType = json["source_type"]?.stringValue
    self.relatedRepo = json["related_repo"]?.stringValue
    self.context = json["context"]?.stringValue
    self.applicability = json["applicability"]?.stringValue
    self.nonApplicability = json["non_applicability"]?.stringValue
    self.evidenceRefs = json["evidence_refs"]?.arrayValue?.compactMap(\.stringValue) ?? []
    self.reviewAfter = json["review_after"]?.stringValue
    self.expiresAt = json["expires_at"]?.stringValue
    self.confidence = json["confidence"]?.numberValue
    self.reason = json["reason"]?.stringValue
    self.searchText = json["search_text"]?.stringValue
    self.qualityDecision = json["quality_decision"]?.stringValue
    self.qualityScore = json["quality_score"]?.numberValue
    self.qualityReasons = json["quality_reasons"]?.arrayValue?.compactMap(\.stringValue) ?? []
    self.legacyFormat = json["legacy_format"]?.boolValue ?? false
    self.clusterKey = json["cluster_key"]?.stringValue
    self.clusterLabel = json["cluster_label"]?.stringValue
    self.hasProposedContent = json["has_proposed_content"]?.boolValue ?? false
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

public enum ProposalReviewLane: String, CaseIterable, Identifiable, Sendable {
  case reviewNow
  case mergeSuggestions
  case needsContext
  case conflicts
  case reviewed
  case all

  public var id: String { rawValue }

  public var title: String {
    switch self {
    case .reviewNow: return "Review now"
    case .mergeSuggestions: return "Merge suggestions"
    case .needsContext: return "Needs context"
    case .conflicts: return "Conflicts / stale"
    case .reviewed: return "Reviewed"
    case .all: return "All"
    }
  }
}

public enum ProposalReviewSort: String, CaseIterable, Identifiable, Sendable {
  case mostLeverage
  case needsAttention
  case oldest
  case newest

  public var id: String { rawValue }

  public var title: String {
    switch self {
    case .mostLeverage: return "Most leverage"
    case .needsAttention: return "Needs attention"
    case .oldest: return "Oldest"
    case .newest: return "Newest"
    }
  }
}

public extension ProposalSummary {
  var isOpen: Bool {
    ["pending", "approved", "needs-human"].contains(status)
  }

  var isReviewed: Bool {
    ["applied", "rejected", "superseded"].contains(status)
  }

  var includedSourceCount: Int {
    let refs = evidenceRefs.isEmpty ? sourceIds : evidenceRefs
    return max(refs.isEmpty ? 0 : 1, Set(refs).count)
  }

  var hasConflictSignal: Bool {
    let text = ([title, reason ?? ""] + qualityReasons).joined(separator: " ").lowercased()
    return ["conflict", "contradict", "target drift", "target page changed", "stale target", "base changed"].contains { text.contains($0) }
  }

  var isMemoryProposal: Bool {
    let type = sourceType?.lowercased()
    return memoryKind != nil || ["remember", "memory", "memory-merge"].contains(type)
  }

  var isMergeProposal: Bool {
    sourceType?.lowercased() == "memory-merge" || title.lowercased().hasPrefix("consolidate:")
  }

  var reviewLane: ProposalReviewLane {
    if isReviewed {
      return .reviewed
    }
    if hasConflictSignal || risk?.lowercased() == "high" {
      return .conflicts
    }
    if isMergeProposal {
      return .mergeSuggestions
    }
    let quality = qualityDecision?.lowercased()
    let incompleteMemoryEnvelope = isMemoryProposal && (
      scope?.isEmpty != false || applicability?.isEmpty != false
        || nonApplicability?.isEmpty != false || (sourceIds.isEmpty && evidenceRefs.isEmpty)
    )
    if status == "needs-human" || legacyFormat || targetPage == nil || incompleteMemoryEnvelope
      || (quality != nil && quality != "ready") {
      return .needsContext
    }
    return .reviewNow
  }

  var reviewLeverage: Int {
    max(1, includedSourceCount)
  }

  func matchesReviewSearch(_ query: String) -> Bool {
    let needle = query.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    guard !needle.isEmpty else {
      return true
    }
    let haystack = [
      title, project, domain, scope, memoryKind, sourceHarness, sourceMachine,
      sourceType, relatedRepo, context, applicability, nonApplicability,
      clusterLabel, targetPage, reason, searchText
    ].compactMap { $0 }.joined(separator: "\n").lowercased()
    return haystack.contains(needle)
      || sourceIds.contains { $0.lowercased().contains(needle) }
      || evidenceRefs.contains { $0.lowercased().contains(needle) }
  }
}

public struct ProposalDiffStats: Equatable, Sendable {
  public let additions: Int
  public let removals: Int

  public init(diff: String?) {
    guard let diff else {
      additions = 0
      removals = 0
      return
    }
    var additions = 0
    var removals = 0
    for line in diff.split(separator: "\n", omittingEmptySubsequences: false) {
      if line.hasPrefix("+") && !line.hasPrefix("+++") {
        additions += 1
      } else if line.hasPrefix("-") && !line.hasPrefix("---") {
        removals += 1
      }
    }
    self.additions = additions
    self.removals = removals
  }
}

/// Pure state machine for optimistic proposal decisions. IDs disappear as soon as
/// a decision starts. Failed IDs remain hidden until the next explicit reload;
/// successful IDs stay hidden for the rest of the app session to prevent an older
/// in-flight list response from briefly resurrecting them.
public struct OptimisticProposalDecisionState: Equatable, Sendable {
  public private(set) var pendingIds = Set<String>()
  public private(set) var hiddenIds = Set<String>()
  public private(set) var failedIds = Set<String>()

  public init() {}

  public mutating func start(ids: Set<String>) {
    pendingIds.formUnion(ids)
    hiddenIds.formUnion(ids)
    failedIds.subtract(ids)
  }

  public mutating func complete(succeededIds: Set<String>, failedIds: Set<String>) {
    pendingIds.subtract(succeededIds)
    pendingIds.subtract(failedIds)
    self.failedIds.formUnion(failedIds)
  }

  public mutating func beginReload() {
    hiddenIds.subtract(failedIds)
    failedIds.removeAll()
  }

  public func filterVisible(_ proposals: [ProposalSummary]) -> [ProposalSummary] {
    proposals.filter { !hiddenIds.contains($0.id) }
  }
}

public enum ProposalBatchValidation {
  public static let maximumBatchSize = 20

  public static func duplicateTargets(in proposals: [ProposalSummary]) -> Set<String> {
    let counts = Dictionary(grouping: proposals.compactMap(\.targetPage), by: { $0 }).mapValues(\.count)
    return Set(counts.compactMap { target, count in count > 1 ? target : nil })
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
  public let sourceProposals: [ProposalSourceEvidence]
  public let targetUnchanged: Bool?

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
      clusterLabel: proposal["cluster_label"]?.stringValue,
      sourceProposals: decoded["source_proposals"]?.arrayValue?.map(ProposalSourceEvidence.init(json:)) ?? [],
      targetUnchanged: decoded["target_unchanged"]?.boolValue
    )
  }
}

public struct ProposalSourceEvidence: Identifiable, Sendable, Equatable {
  public let id: String
  public let title: String
  public let status: String?
  public let createdAt: String?
  public let sourceHarness: String?
  public let sourceMachine: String?
  public let excerpt: String?

  public init(json: JSONValue) {
    id = json["id"]?.stringValue ?? ""
    title = json["title"]?.stringValue ?? "Captured proposal"
    status = json["status"]?.stringValue
    createdAt = json["created_at"]?.stringValue
    sourceHarness = json["source_harness"]?.stringValue
    sourceMachine = json["source_machine"]?.stringValue
    excerpt = json["excerpt"]?.stringValue
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
