import AppKit
import SwiftUI
import BrainstackMenuCore

enum ProposalDetailTab: String, CaseIterable, Identifiable {
  case summary = "Summary"
  case sources = "Sources"
  case diff = "Diff"
  case metadata = "Metadata"

  var id: String { rawValue }
}

struct ProposalDecisionPane: View {
  @ObservedObject var model: AppModel
  let proposal: ProposalSummary
  let selectedProposals: [ProposalSummary]
  let relatedCount: Int
  let reviewLater: () -> Void
  let needsWork: () -> Void
  let mergeSelected: () -> Void
  let decide: (String) -> Void

  @State private var tab: ProposalDetailTab = .summary
  @State private var showRawProposal = false

  private var detail: ProposalDetail? { model.proposalDetails[proposal.id] }
  private var diffStats: ProposalDiffStats { ProposalDiffStats(diff: detail?.diff) }
  private var missingSelectedDetails: [ProposalSummary] {
    selectedProposals.filter { model.proposalDetails[$0.id] == nil }
  }
  private var hasDuplicateTargets: Bool {
    !ProposalBatchValidation.duplicateTargets(in: selectedProposals).isEmpty
  }
  private var hasPendingTargetConflict: Bool {
    !model.pendingProposalApplyTargets.isDisjoint(with: Set(selectedProposals.compactMap(\.targetPage)))
  }
  private var applyBlockReason: String? {
    if model.busyAction != nil { return "Another proposal operation is running." }
    if hasPendingTargetConflict {
      return "A decision for this destination is already being saved."
    }
    if !missingSelectedDetails.isEmpty { return "Load every selected decision packet before applying." }
    if hasDuplicateTargets { return "Choose one proposal per destination, or merge the related proposals first." }
    if selectedProposals.contains(where: { $0.targetPage == nil }) { return "Every selected proposal needs a destination." }
    if selectedProposals.contains(where: { $0.isMemoryProposal && $0.includedSourceCount == 0 }) {
      return "Memory proposals need at least one source before they can be applied."
    }
    if !changedTargetProposals.isEmpty {
      return "Changed destination: \(changedTargetSummary). Send the affected proposal back for regeneration."
    }
    if selectedProposals.contains(where: { ![.reviewNow, .mergeSuggestions].contains($0.reviewLane) }) {
      return "Needs-context and conflict proposals must be resolved before applying."
    }
    return nil
  }
  private var visibleApplyWarning: String? {
    guard missingSelectedDetails.isEmpty else { return nil }
    if hasDuplicateTargets { return "These proposals write to the same destination. Merge them or keep only one selected." }
    if selectedProposals.contains(where: { $0.targetPage == nil }) { return "At least one selected proposal has no destination." }
    if selectedProposals.contains(where: { $0.isMemoryProposal && $0.includedSourceCount == 0 }) {
      return "At least one selected memory proposal has no evidence source."
    }
    if !changedTargetProposals.isEmpty {
      return "Changed destination: \(changedTargetSummary)."
    }
    if selectedProposals.contains(where: { ![.reviewNow, .mergeSuggestions].contains($0.reviewLane) }) {
      return "Resolve needs-context or conflict proposals before applying them."
    }
    return nil
  }
  private var canMergeSelection: Bool {
    selectedProposals.count > 1 && Set(selectedProposals.compactMap(\.clusterKey)).count == 1
      && selectedProposals.allSatisfy { $0.clusterKey?.isEmpty == false }
      && selectedProposals.allSatisfy { !$0.isMergeProposal }
  }
  private var changedTargetProposals: [ProposalSummary] {
    selectedProposals.filter { model.proposalDetails[$0.id]?.targetUnchanged == false }
  }
  private var changedTargetSummary: String {
    let names = changedTargetProposals.prefix(3).map {
      "\(displayProposalTitle($0.title)) (\($0.targetPage ?? "unknown target"))"
    }
    return names.joined(separator: ", ")
      + (changedTargetProposals.count > 3 ? ", and \(changedTargetProposals.count - 3) more" : "")
  }

  var body: some View {
    VStack(alignment: .leading, spacing: 0) {
      detailHeader
      Divider()
      if let detail {
        ScrollView {
          VStack(alignment: .leading, spacing: 14) {
            impactStrip
            detailTabs
            tabContent(detail)
          }
          .padding(16)
          .frame(maxWidth: .infinity, alignment: .leading)
        }
      } else {
        detailLoadingState
      }
      Divider()
      decisionBar
    }
    .background(Color(nsColor: .textBackgroundColor))
  }

  private var detailHeader: some View {
    VStack(alignment: .leading, spacing: 7) {
      Text(displayProposalTitle(proposal.title))
        .font(.title2.weight(.semibold))
        .fixedSize(horizontal: false, vertical: true)
        .textSelection(.enabled)
      Text(consequenceSummary)
        .font(.callout)
        .foregroundColor(.secondary)
        .fixedSize(horizontal: false, vertical: true)
        .textSelection(.enabled)
      HStack(spacing: 7) {
        ProposalBadge(label: readinessLabel, color: readinessColor, icon: readinessIcon)
        if let risk = proposal.risk, risk.lowercased() != "low" {
          ProposalBadge(label: "\(risk.capitalized) impact", color: risk.lowercased() == "high" ? .red : .orange)
        } else if proposal.risk?.lowercased() == "low" {
          ProposalBadge(label: "Low impact", color: .green)
        } else {
          ProposalBadge(label: "Impact not assessed", color: .secondary)
        }
        if selectedProposals.count > 1 {
          ProposalBadge(label: "\(selectedProposals.count) selected", color: .accentColor)
        }
        Spacer()
      }
    }
    .padding(.horizontal, 18)
    .padding(.vertical, 14)
  }

  private var consequenceSummary: String {
    if !isMemoryProposal {
      return "Add a sourced entry to Brainstack from captured evidence."
    }
    let lessonCount = max(1, lessonLines.count)
    let kind = humanizeIdentifier(detail?.memoryKind ?? proposal.memoryKind ?? "memory")
    let project = detail?.project ?? proposal.project
    if let project, !project.isEmpty {
      return "Add \(lessonCount == 1 ? "a" : String(lessonCount)) \(project)-scoped \(kind)\(lessonCount == 1 ? "" : "s") to Brainstack from captured evidence."
    }
    return "Add \(lessonCount == 1 ? "a scoped lesson" : "\(lessonCount) scoped lessons") to Brainstack from captured evidence."
  }

  private var readinessLabel: String {
    switch proposal.reviewLane {
    case .conflicts: return "Needs a decision"
    case .needsContext: return "Needs context"
    case .mergeSuggestions: return "Merge suggestion"
    case .reviewed: return proposal.status.capitalized
    default: return "Ready to review"
    }
  }

  private var readinessColor: Color {
    switch proposal.reviewLane {
    case .conflicts: return .orange
    case .needsContext: return .yellow
    case .reviewed: return .secondary
    default: return .green
    }
  }

  private var readinessIcon: String? {
    switch proposal.reviewLane {
    case .conflicts: return "exclamationmark.triangle.fill"
    case .needsContext: return "questionmark.circle.fill"
    case .reviewed: return "checkmark.circle.fill"
    default: return "checkmark.circle.fill"
    }
  }

  private var impactStrip: some View {
    HStack(spacing: 0) {
      ImpactMetric(
        icon: isMemoryProposal ? "book" : "doc.text",
        value: "\(max(1, lessonLines.count))",
        label: isMemoryProposal ? (max(1, lessonLines.count) == 1 ? "lesson" : "lessons") : "entry"
      )
      ImpactDivider()
      let sourceCount = max(proposal.includedSourceCount, detail?.sourceIds.count ?? 0)
      ImpactMetric(icon: "doc.on.doc", value: "\(sourceCount)", label: sourceCount == 1 ? "source" : "sources")
      if relatedCount > 1 {
        ImpactDivider()
        ImpactMetric(icon: "person.2", value: "\(relatedCount - 1)", label: "related remain")
      }
      ImpactDivider()
      ImpactMetric(icon: "chevron.left.forwardslash.chevron.right", value: "+\(diffStats.additions) / -\(diffStats.removals)", label: "lines")
      ImpactDivider()
      ImpactMetric(
        icon: detail?.targetUnchanged == false ? "exclamationmark.triangle" : "scope",
        value: targetStateLabel,
        label: "target"
      )
    }
    .frame(maxWidth: .infinity)
    .padding(.vertical, 10)
    .background(Color(nsColor: .underPageBackgroundColor))
    .overlay(RoundedRectangle(cornerRadius: 7).stroke(Color(nsColor: .separatorColor), lineWidth: 0.5))
    .clipShape(RoundedRectangle(cornerRadius: 7))
  }

  private var targetStateLabel: String {
    switch detail?.targetUnchanged {
    case true: return "Unchanged"
    case false: return "Changed"
    case nil: return proposal.baseSHA256 == nil ? "Checked on apply" : "Guarded"
    }
  }

  private var detailTabs: some View {
    Picker("Proposal detail", selection: $tab) {
      ForEach(ProposalDetailTab.allCases) { option in
        if option == .sources {
          Text("Sources \(max(proposal.includedSourceCount, detail?.sourceIds.count ?? 0))").tag(option)
        } else {
          Text(option.rawValue).tag(option)
        }
      }
    }
    .pickerStyle(.segmented)
    .frame(maxWidth: 520)
  }

  @ViewBuilder
  private func tabContent(_ detail: ProposalDetail) -> some View {
    switch tab {
    case .summary:
      summaryTab(detail)
    case .sources:
      sourcesTab(detail)
    case .diff:
      diffTab(detail)
    case .metadata:
      metadataTab(detail)
    }
  }

  private func summaryTab(_ detail: ProposalDetail) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      ProposalSection(title: isMemoryProposal ? "Proposed memory" : "Proposed entry") {
        VStack(alignment: .leading, spacing: 12) {
          VStack(alignment: .leading, spacing: 5) {
            Text(isMemoryProposal ? (lessonLines.count == 1 ? "Lesson" : "Lessons") : "Change")
              .font(.callout.weight(.semibold))
            if lessonLines.isEmpty {
              Text(firstUsefulBodyText(detail.body) ?? "No concise lesson was extracted. Review the raw proposal in Metadata.")
                .font(.callout)
                .foregroundColor(lessonLines.isEmpty ? .secondary : .primary)
            } else {
              ForEach(Array(lessonLines.enumerated()), id: \.offset) { _, lesson in
                Label {
                  Text(lesson).textSelection(.enabled)
                } icon: {
                  Image(systemName: "circle.fill").font(.system(size: 4))
                }
                .font(.callout)
              }
            }
          }
          if let applicability = nonEmpty(detail.applicability) {
            Divider()
            BoundaryBlock(title: "Use when", text: applicability)
          }
          if let nonApplicability = nonEmpty(detail.nonApplicability) {
            Divider()
            BoundaryBlock(title: "Do not use when", text: nonApplicability)
          }
        }
      }

      HStack(alignment: .top, spacing: 14) {
        ProposalSection(title: "Review checks") {
          VStack(alignment: .leading, spacing: 7) {
            if isMemoryProposal {
              ReviewCheck(label: "Scope identified", passed: nonEmpty(detail.scope) != nil)
              ReviewCheck(label: "Applicability stated", passed: nonEmpty(detail.applicability) != nil)
              ReviewCheck(label: "Boundaries stated", passed: nonEmpty(detail.nonApplicability) != nil)
            }
            ReviewCheck(label: "Evidence attached", passed: !detail.evidenceRefs.isEmpty || !detail.sourceIds.isEmpty)
            ReviewCheck(label: "Destination identified", passed: proposal.targetPage != nil)
            if detail.body.count < 180 {
              Divider()
              Label("Detail is brief; verify sufficiency", systemImage: "exclamationmark.triangle")
                .font(.caption)
                .foregroundColor(.orange)
            }
          }
        }
        .frame(maxWidth: .infinity, alignment: .top)

        if isMergeProposal, let confidence = detail.confidence {
          ProposalSection(title: "Merge match") {
            VStack(alignment: .leading, spacing: 7) {
              Text(String(format: "%.0f%%", confidence * 100))
                .font(.title2.weight(.semibold))
                .foregroundColor(confidence >= 0.8 ? .green : .orange)
              Text("Harness confidence that the included captures describe the same topic.")
                .font(.caption)
                .foregroundColor(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            }
          }
          .frame(maxWidth: .infinity, alignment: .top)
        }
      }

      if relatedCount > 1 {
        ProposalSection(title: "Group coverage") {
          Text("This proposal includes \(max(proposal.includedSourceCount, detail.sourceIds.count)) captured source\(max(proposal.includedSourceCount, detail.sourceIds.count) == 1 ? "" : "s"). \(relatedCount - 1) other open proposal\(relatedCount - 1 == 1 ? " remains" : "s remain") in the same review group.")
            .font(.callout)
            .textSelection(.enabled)
        }
      }

      if let context = nonEmpty(detail.context) {
        ProposalSection(title: "Why this exists") {
          Text(context)
            .font(.callout)
            .textSelection(.enabled)
        }
      } else if let reason = nonEmpty(detail.reason) {
        ProposalSection(title: "Why this needs review") {
          Text(reason)
            .font(.callout)
            .textSelection(.enabled)
        }
      }
    }
  }

  private func sourcesTab(_ detail: ProposalDetail) -> some View {
    let enrichedIds = Set(detail.sourceProposals.map(\.id))
    let refs = uniqueRefs(detail).filter { ref in
      guard ref.hasPrefix("proposal:") else { return true }
      return !enrichedIds.contains(String(ref.dropFirst("proposal:".count)))
    }
    return VStack(alignment: .leading, spacing: 12) {
      Text("Source evidence")
        .font(.headline)
      if let origin = sourceOrigin(detail) {
        ProposalSection(title: "Captured by") {
          Text(origin)
            .font(.callout)
            .textSelection(.enabled)
        }
      }
      ForEach(detail.sourceProposals) { source in
        ProposalSection(title: displayProposalTitle(source.title)) {
          VStack(alignment: .leading, spacing: 7) {
            let provenance = [source.sourceHarness, source.sourceMachine, source.createdAt.map(proposalRelativeDate)]
              .compactMap { nonEmpty($0) }
            if !provenance.isEmpty {
              Text(provenance.joined(separator: " · "))
                .font(.caption)
                .foregroundColor(.secondary)
            }
            if let excerpt = nonEmpty(source.excerpt) {
              Text(sourceExcerptText(excerpt))
                .font(.callout)
                .textSelection(.enabled)
                .fixedSize(horizontal: false, vertical: true)
            }
            Text(source.id)
              .font(.system(size: 10, design: .monospaced))
              .foregroundColor(.secondary)
              .textSelection(.enabled)
          }
        }
      }
      if refs.isEmpty && detail.sourceProposals.isEmpty {
        ProposalSection(title: "No evidence references") {
          Text("This proposal does not identify a source artifact. Send it back for more context before applying it.")
            .font(.callout)
            .foregroundColor(.orange)
        }
      } else if !refs.isEmpty {
        ForEach(Array(refs.enumerated()), id: \.offset) { index, ref in
          ProposalSection(title: "Source \(index + 1)") {
            VStack(alignment: .leading, spacing: 5) {
              Text(humanizeSourceRef(ref))
                .font(.callout.weight(.semibold))
              if let context = nonEmpty(detail.context) {
                Text(context)
                  .font(.caption)
                  .foregroundColor(.secondary)
                  .lineLimit(3)
              }
              Text(ref)
                .font(.system(size: 10, design: .monospaced))
                .foregroundColor(.secondary)
                .textSelection(.enabled)
            }
          }
        }
      }
      if relatedCount > 1 {
        Text("\(relatedCount - 1) related open proposal\(relatedCount - 1 == 1 ? " is" : "s are") not included in this decision packet.")
          .font(.caption)
          .foregroundColor(.secondary)
      }
    }
  }

  private func diffTab(_ detail: ProposalDetail) -> some View {
    VStack(alignment: .leading, spacing: 10) {
      HStack {
        Text("Proposed change")
          .font(.headline)
        Spacer()
        Text("+\(diffStats.additions) / -\(diffStats.removals) lines")
          .font(.caption)
          .foregroundColor(.secondary)
      }
      if let diff = nonEmpty(detail.diff) {
        WrappedDiffView(diff: diff)
      } else {
        ProposalSection(title: "No textual diff") {
          Text("This proposal has no rendered file diff. Review the proposed memory and destination before deciding.")
            .font(.callout)
            .foregroundColor(.secondary)
        }
      }
    }
  }

  private func metadataTab(_ detail: ProposalDetail) -> some View {
    VStack(alignment: .leading, spacing: 14) {
      ProposalSection(title: "Proposal metadata") {
        VStack(alignment: .leading, spacing: 10) {
          HStack(spacing: 8) {
            Text("Proposal ID").font(.caption).foregroundColor(.secondary)
            Text(proposal.id).font(.system(size: 10, design: .monospaced)).textSelection(.enabled)
            Spacer()
            Button {
              NSPasteboard.general.clearContents()
              NSPasteboard.general.setString(proposal.id, forType: .string)
            } label: {
              Image(systemName: "doc.on.doc")
            }
            .buttonStyle(.borderless)
            .help("Copy proposal ID")
            .accessibilityLabel("Copy proposal ID")
          }
          Grid(alignment: .leading, horizontalSpacing: 14, verticalSpacing: 7) {
          MetadataRow(label: "Status", value: proposal.status)
          MetadataRow(label: "Created", value: proposal.createdAt)
          MetadataRow(label: "Target", value: proposal.targetPage)
          MetadataRow(label: "Project", value: detail.project ?? proposal.project)
          MetadataRow(label: "Domain", value: detail.domain ?? proposal.domain)
          MetadataRow(label: "Scope", value: detail.scope ?? proposal.scope)
          MetadataRow(label: "Kind", value: detail.memoryKind ?? proposal.memoryKind)
          MetadataRow(label: "Review group", value: detail.clusterLabel ?? proposal.clusterLabel)
          MetadataRow(label: "Related repo", value: detail.relatedRepo ?? proposal.relatedRepo)
          MetadataRow(label: "Review after", value: detail.reviewAfter ?? proposal.reviewAfter)
          MetadataRow(label: "Expires", value: detail.expiresAt ?? proposal.expiresAt)
          if let quality = detail.qualityDecision ?? proposal.qualityDecision {
            MetadataRow(label: "Structural check", value: quality)
          }
          if let score = detail.qualityScore ?? proposal.qualityScore {
            MetadataRow(label: "Structural completeness", value: String(format: "%.0f%%", score * 100))
          }
          }
        }
      }
      if !detail.qualityReasons.isEmpty {
        ProposalSection(title: "Structural check notes") {
          VStack(alignment: .leading, spacing: 5) {
            ForEach(detail.qualityReasons, id: \.self) { reason in
              Text("• \(reason)")
                .font(.callout)
                .textSelection(.enabled)
            }
          }
        }
      }
      DisclosureGroup("Raw proposal", isExpanded: $showRawProposal) {
        Text(detail.body)
          .font(.system(size: 11, design: .monospaced))
          .textSelection(.enabled)
          .frame(maxWidth: .infinity, alignment: .leading)
          .padding(.top, 7)
      }
      .font(.caption.weight(.semibold))
      .padding(12)
      .background(Color(nsColor: .underPageBackgroundColor))
      .clipShape(RoundedRectangle(cornerRadius: 7))
    }
  }

  @ViewBuilder
  private var detailLoadingState: some View {
    if let error = model.detailErrors[proposal.id] {
      VStack(spacing: 10) {
        Image(systemName: "exclamationmark.triangle")
          .font(.title2)
          .foregroundColor(.orange)
        Text("Proposal detail could not be loaded")
          .font(.headline)
        Text(error)
          .font(.caption)
          .foregroundColor(.secondary)
          .multilineTextAlignment(.center)
        Button("Try again") { model.loadProposalDetail(proposal.id, force: true) }
      }
      .frame(maxWidth: .infinity, maxHeight: .infinity)
      .padding(30)
    } else if model.loadingDetailIds.contains(proposal.id) {
      VStack(spacing: 10) {
        ProgressView()
        Text("Loading decision packet…")
          .font(.callout)
          .foregroundColor(.secondary)
      }
      .frame(maxWidth: .infinity, maxHeight: .infinity)
    } else {
      VStack(spacing: 10) {
        Text("Proposal detail is not loaded.")
          .font(.callout)
          .foregroundColor(.secondary)
        Button("Load detail") { model.loadProposalDetail(proposal.id, force: true) }
      }
      .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
  }

  private var decisionBar: some View {
    VStack(alignment: .leading, spacing: 7) {
      HStack(spacing: 10) {
        if proposal.isReviewed {
          Label("This proposal was \(proposal.status).", systemImage: "checkmark.circle")
            .font(.caption)
            .foregroundColor(.secondary)
        } else {
          Text(selectedProposals.count > 1 ? "\(selectedProposals.count) proposals selected" : "Decisions are saved on the control host.")
            .font(.caption)
            .foregroundColor(.secondary)
          Spacer()
          Button(selectedProposals.count > 1 ? "Move selected to bottom" : "Move to bottom") { reviewLater() }
            .help("Move this selection behind the active queue for this review session")
          if selectedProposals.count == 1 {
            Button("Needs work…") { needsWork() }
              .disabled(model.busyAction != nil || !model.pendingProposalDecisionIds.isEmpty)
          } else if canMergeSelection {
            Button("Merge selected…") { mergeSelected() }
              .disabled(model.busyAction != nil || !model.pendingProposalDecisionIds.isEmpty)
          }
          Button(selectedProposals.count > 1 ? "Reject selected…" : "Reject…") { decide("reject") }
            .disabled(model.busyAction != nil || hasPendingTargetConflict)
            .help(hasPendingTargetConflict ? "A successful apply is already closing this destination's older proposals." : "Reject the selected proposal without changing the shared brain")
          if !missingSelectedDetails.isEmpty {
            HStack(spacing: 6) {
              if selectedLoadingCount > 0 {
                ProgressView().controlSize(.small)
                Text("\(selectedLoadingCount) loading")
                  .font(.caption2)
                  .foregroundColor(.secondary)
              }
              if !selectedDetailFailures.isEmpty {
                Text("\(selectedDetailFailures.count) failed")
                  .font(.caption2)
                  .foregroundColor(.orange)
                  .help(selectedDetailFailureSummary)
              }
              Button(!selectedDetailFailures.isEmpty ? "Retry failed" : (selectedProposals.count > 1 ? "Load selected" : "Load decision packet")) {
                model.loadProposalDetails(selectedProposals)
              }
              .buttonStyle(.borderedProminent)
              .disabled(model.busyAction != nil || selectedLoadingCount > 0)
              .help("Load the content and impact for every selected proposal before applying")
            }
          } else {
            Button(selectedProposals.count > 1 ? "Apply selected" : "Apply to Brain") { decide("apply") }
              .buttonStyle(.borderedProminent)
              .disabled(applyBlockReason != nil)
              .help(applyBlockReason ?? "Apply the selected proposal content to the shared brain")
          }
        }
      }
      if let visibleApplyWarning, !proposal.isReviewed {
        Label(visibleApplyWarning, systemImage: "exclamationmark.triangle")
          .font(.caption)
          .foregroundColor(.orange)
          .fixedSize(horizontal: false, vertical: true)
      }
    }
    .controlSize(.regular)
    .padding(.horizontal, 16)
    .padding(.vertical, 11)
  }

  private var selectedLoadingCount: Int {
    model.loadingDetailIds.intersection(Set(selectedProposals.map(\.id))).count
  }
  private var selectedDetailFailures: [ProposalSummary] {
    selectedProposals.filter { model.proposalDetails[$0.id] == nil && model.detailErrors[$0.id] != nil }
  }
  private var selectedDetailFailureSummary: String {
    selectedDetailFailures.map { displayProposalTitle($0.title) }.joined(separator: ", ")
  }

  private var lessonLines: [String] {
    guard let body = detail?.body else { return [] }
    let section = markdownSection(named: "Lessons", in: body)
      ?? markdownSection(named: "Lesson", in: body)
      ?? markdownSection(named: "Request", in: body)
    guard let section else { return [] }
    let bullets = section.components(separatedBy: .newlines)
      .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
      .filter { !$0.isEmpty }
      .map { line -> String in
        if line.hasPrefix("- ") || line.hasPrefix("* ") { return String(line.dropFirst(2)) }
        return line
      }
    return bullets.count > 1 ? bullets : bullets.filter { !$0.hasPrefix("#") }
  }

  private var isMemoryProposal: Bool {
    proposal.isMemoryProposal || detail?.memoryKind != nil
  }

  private var isMergeProposal: Bool {
    proposal.isMergeProposal || detail?.sourceType?.lowercased() == "memory-merge"
  }

  private func markdownSection(named name: String, in body: String) -> String? {
    let lines = body.components(separatedBy: .newlines)
    let marker = "## \(name)"
    guard let start = lines.firstIndex(where: { $0.trimmingCharacters(in: .whitespacesAndNewlines).caseInsensitiveCompare(marker) == .orderedSame }) else {
      return nil
    }
    let content = lines[(start + 1)...]
      .prefix { !$0.trimmingCharacters(in: .whitespacesAndNewlines).hasPrefix("## ") }
      .joined(separator: "\n")
      .trimmingCharacters(in: .whitespacesAndNewlines)
    return content.isEmpty ? nil : content
  }

  private func uniqueRefs(_ detail: ProposalDetail) -> [String] {
    var seen = Set<String>()
    return (detail.evidenceRefs + detail.sourceIds).filter { seen.insert($0).inserted }
  }

  private func sourceOrigin(_ detail: ProposalDetail) -> String? {
    let pieces = [detail.sourceHarness, detail.sourceMachine]
      .compactMap { nonEmpty($0) }
    return pieces.isEmpty ? nil : pieces.joined(separator: " on ")
  }
}

struct ProposalQueueRow: View {
  let proposal: ProposalSummary
  let isPrimary: Bool
  let isSelected: Bool
  let relatedCount: Int
  let isDeferred: Bool
  let select: () -> Void
  let toggle: () -> Void

  var body: some View {
    HStack(alignment: .top, spacing: 10) {
      Button(action: toggle) {
        Image(systemName: isSelected ? "checkmark.square.fill" : "square")
          .foregroundColor(isSelected ? .accentColor : .secondary)
          .font(.system(size: 16))
          .frame(width: 28, height: 28)
          .contentShape(Rectangle())
      }
      .buttonStyle(.plain)
      .help(isSelected ? "Remove from selection" : "Add to selection")
      .accessibilityLabel("\(isSelected ? "Deselect" : "Select") \(displayProposalTitle(proposal.title))")
      .accessibilityValue(isSelected ? "Selected" : "Not selected")

      Button(action: select) {
        VStack(alignment: .leading, spacing: 5) {
          HStack(alignment: .firstTextBaseline, spacing: 7) {
            Text(displayProposalTitle(proposal.title))
              .font(.system(size: 13, weight: .medium))
              .foregroundColor(.primary)
              .multilineTextAlignment(.leading)
              .lineLimit(3)
              .help(displayProposalTitle(proposal.title))
            Spacer(minLength: 8)
            Text(proposalRelativeDate(proposal.createdAt))
              .font(.caption2)
              .foregroundColor(.secondary)
          }
          HStack(spacing: 5) {
            Text(proposal.project ?? "Brainstack")
            Text("·")
            Text(humanizeIdentifier(proposal.memoryKind ?? proposal.sourceType ?? proposal.scope ?? "proposal"))
            if proposal.includedSourceCount > 0 {
              Text("·")
              Text("\(proposal.includedSourceCount) source\(proposal.includedSourceCount == 1 ? "" : "s")")
            }
            Spacer()
            if let warning = proposalException(proposal, relatedCount: relatedCount) {
              Text(warning.label)
                .foregroundColor(warning.color)
            }
            if isDeferred {
              Text("Later")
                .foregroundColor(.secondary)
            }
          }
          .font(.caption2)
          .foregroundColor(.secondary)
          .lineLimit(1)
        }
      }
      .buttonStyle(.plain)
      .accessibilityLabel(displayProposalTitle(proposal.title))
      .accessibilityValue("\(isSelected ? "Selected for batch. " : "")\(isPrimary ? "Current proposal. " : "")\(isDeferred ? "Deferred until later." : "")")
      .accessibilityAddTraits(isPrimary ? .isSelected : [])
    }
    .padding(.horizontal, 12)
    .padding(.vertical, 10)
    .background(isPrimary ? Color.accentColor.opacity(0.10) : Color.clear)
    .overlay(alignment: .leading) {
      Rectangle()
        .fill(isPrimary ? Color.accentColor : Color.clear)
        .frame(width: 3)
    }
    .overlay(alignment: .bottom) {
      Divider().padding(.leading, 38)
    }
  }
}

struct ProposalBadge: View {
  let label: String
  let color: Color
  var icon: String? = nil

  var body: some View {
    HStack(spacing: 5) {
      if let icon {
        Image(systemName: icon).foregroundColor(color)
      }
      Text(label).foregroundColor(.primary)
    }
    .font(.caption)
    .padding(.horizontal, 8)
    .padding(.vertical, 4)
    .background(color.opacity(0.12))
    .overlay(RoundedRectangle(cornerRadius: 5).stroke(color.opacity(0.35), lineWidth: 0.5))
    .clipShape(RoundedRectangle(cornerRadius: 5))
  }
}

private struct ImpactMetric: View {
  let icon: String
  let value: String
  let label: String

  var body: some View {
    HStack(spacing: 7) {
      Image(systemName: icon)
        .foregroundColor(.secondary)
      VStack(alignment: .leading, spacing: 1) {
        Text(value).font(.callout.weight(.medium))
        Text(label).font(.caption2).foregroundColor(.secondary)
      }
    }
    .frame(maxWidth: .infinity)
    .padding(.horizontal, 8)
  }
}

private struct ImpactDivider: View {
  var body: some View {
    Divider().frame(height: 34)
  }
}

struct ProposalSection<Content: View>: View {
  let title: String
  @ViewBuilder let content: Content

  init(title: String, @ViewBuilder content: () -> Content) {
    self.title = title
    self.content = content()
  }

  var body: some View {
    VStack(alignment: .leading, spacing: 8) {
      Text(title)
        .font(.headline)
      content
    }
    .padding(13)
    .frame(maxWidth: .infinity, alignment: .leading)
    .background(Color(nsColor: .underPageBackgroundColor))
    .overlay(RoundedRectangle(cornerRadius: 7).stroke(Color(nsColor: .separatorColor), lineWidth: 0.5))
    .clipShape(RoundedRectangle(cornerRadius: 7))
  }
}

private struct BoundaryBlock: View {
  let title: String
  let text: String

  var body: some View {
    VStack(alignment: .leading, spacing: 3) {
      Text(title).font(.callout.weight(.semibold))
      Text(text).font(.callout).foregroundColor(.secondary).textSelection(.enabled)
    }
  }
}

private struct ReviewCheck: View {
  let label: String
  let passed: Bool

  var body: some View {
    Label(label, systemImage: passed ? "checkmark.circle.fill" : "exclamationmark.circle.fill")
      .font(.callout)
      .foregroundColor(passed ? .primary : .orange)
      .symbolRenderingMode(.palette)
      .foregroundStyle(passed ? Color.green : Color.orange, Color.primary)
  }
}

private struct MetadataRow: View {
  let label: String
  let value: String?

  var body: some View {
    if let value, !value.isEmpty {
      GridRow {
        Text(label).font(.caption).foregroundColor(.secondary)
        Text(value)
          .font(.system(size: 11, design: .monospaced))
          .textSelection(.enabled)
      }
    }
  }
}

private struct WrappedDiffView: View {
  let diff: String

  var body: some View {
    VStack(alignment: .leading, spacing: 0) {
      ForEach(Array(diff.split(separator: "\n", omittingEmptySubsequences: false).enumerated()), id: \.offset) { _, rawLine in
        let line = String(rawLine)
        Text(line.isEmpty ? " " : line)
          .font(.system(size: 11, design: .monospaced))
          .foregroundColor(diffLineColor(line))
          .textSelection(.enabled)
          .fixedSize(horizontal: false, vertical: true)
          .frame(maxWidth: .infinity, alignment: .leading)
          .padding(.horizontal, 8)
          .padding(.vertical, 1)
          .background(diffLineBackground(line))
      }
    }
    .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color(nsColor: .separatorColor), lineWidth: 0.5))
    .clipShape(RoundedRectangle(cornerRadius: 6))
  }

  private func diffLineColor(_ line: String) -> Color {
    if line.hasPrefix("+") { return .green }
    if line.hasPrefix("-") { return .red }
    return .secondary
  }

  private func diffLineBackground(_ line: String) -> Color {
    if line.hasPrefix("+") { return .green.opacity(0.07) }
    if line.hasPrefix("-") { return .red.opacity(0.07) }
    return .clear
  }
}

func displayProposalTitle(_ title: String) -> String {
  if title.hasPrefix("Remember: ") {
    return String(title.dropFirst("Remember: ".count))
  }
  if title.hasPrefix("Remember ("), let range = title.range(of: "): ") {
    return String(title[range.upperBound...])
  }
  return title
}

func humanizeIdentifier(_ value: String) -> String {
  value.replacingOccurrences(of: "_", with: " ").replacingOccurrences(of: "-", with: " ")
}

private func proposalRelativeDate(_ iso: String) -> String {
  guard let date = ISO8601DateFormatter().date(from: iso) else { return String(iso.prefix(10)) }
  return relativeTimeText(since: date)
}

private func proposalException(_ proposal: ProposalSummary, relatedCount: Int) -> (label: String, color: Color)? {
  switch proposal.reviewLane {
  case .conflicts: return ("Conflict", .orange)
  case .needsContext: return (proposal.legacyFormat ? "Legacy" : "Needs context", .orange)
  default:
    if relatedCount > 1 { return ("\(relatedCount - 1) related remain", .secondary) }
    return nil
  }
}

private func humanizeSourceRef(_ ref: String) -> String {
  if ref.hasPrefix("proposal:") { return "Captured proposal" }
  if ref.hasPrefix("repo:") { return "Repository evidence" }
  if ref.hasPrefix("raw/") { return "Raw conversation evidence" }
  if ref.hasPrefix("art-") { return "Captured artifact" }
  return "Evidence reference"
}

private func firstUsefulBodyText(_ body: String) -> String? {
  body.components(separatedBy: .newlines)
    .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
    .first { !$0.isEmpty && !$0.hasPrefix("#") && !$0.hasPrefix("-") }
}

private func sourceExcerptText(_ body: String) -> String {
  let useful = body.components(separatedBy: .newlines)
    .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
    .filter { !$0.isEmpty && !$0.hasPrefix("#") }
    .prefix(4)
    .joined(separator: "\n")
  return useful.isEmpty ? body : useful
}

private func nonEmpty(_ value: String?) -> String? {
  guard let value else { return nil }
  let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
  return trimmed.isEmpty ? nil : trimmed
}
