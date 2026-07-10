import AppKit
import SwiftUI
import BrainstackMenuCore

/// A decision-oriented proposal queue. The sidebar answers what deserves attention;
/// the detail pane answers what Brainstack will learn and what the decision changes.
struct OperatorConsoleView: View {
  @ObservedObject var model: AppModel
  @State private var primarySelectionId: String?
  @State private var selectedIds = Set<String>()
  @State private var lane: ProposalReviewLane = .reviewNow
  @State private var sort: ProposalReviewSort = .mostLeverage
  @State private var search = ""
  @State private var reviewLaterIds = Set<String>()
  @State private var showMergeActionDetails = false
  @State private var sessionDecisionCount = 0
  @State private var selectionAnchorId: String?

  var body: some View {
    HSplitView {
      sidebar
        .frame(minWidth: 330, idealWidth: 390, maxWidth: 470)
      detailPane
        .frame(minWidth: 620, maxWidth: .infinity, maxHeight: .infinity)
    }
    .frame(minWidth: 1_020, minHeight: 660)
    .onAppear {
      model.loadProposals()
      ensureSelection()
    }
    .onChange(of: lane) { selectedLane in
      if selectedLane == .reviewed {
        model.loadReviewedProposals()
      }
      resetSelection()
    }
    .onChange(of: search) { _ in ensureSelection() }
    .onChange(of: sort) { _ in ensureSelection() }
    .onChange(of: model.proposals) { _ in ensureSelection() }
    .onChange(of: model.reviewedProposals) { _ in ensureSelection() }
    .onChange(of: primarySelectionId) { id in
      if let id {
        model.loadProposalDetail(id)
      }
    }
    .onMoveCommand(perform: moveSelection)
  }

  private var sourceProposals: [ProposalSummary] {
    lane == .reviewed ? model.reviewedProposals : model.proposals
  }

  private var visibleProposals: [ProposalSummary] {
    sourceProposals
      .filter { proposal in
        (lane == .all || lane == .reviewed || proposal.reviewLane == lane)
          && proposal.matchesReviewSearch(search)
      }
      .sorted(by: proposalComesFirst)
  }

  private var selectedProposals: [ProposalSummary] {
    let byId = Dictionary(sourceProposals.map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
    return selectedIds.compactMap { byId[$0] }.sorted(by: proposalComesFirst)
  }

  private var primaryProposal: ProposalSummary? {
    guard let primarySelectionId else { return nil }
    return sourceProposals.first { $0.id == primarySelectionId }
  }

  private var queueError: String? {
    lane == .reviewed ? model.reviewedProposalsError : model.proposalsError
  }

  // MARK: - Sidebar

  private var sidebar: some View {
    VStack(alignment: .leading, spacing: 0) {
      queueHeader
      primaryLanes
      searchAndSort
      activityStrip
      Divider()
      proposalList
      Divider()
      queueFooter
    }
    .background(Color(nsColor: .windowBackgroundColor))
  }

  private var queueHeader: some View {
    HStack(alignment: .firstTextBaseline, spacing: 8) {
      Text("Review queue")
        .font(.headline)
      Text("\(model.proposals.count) open")
        .font(.caption)
        .foregroundColor(.secondary)
      Spacer()
      if model.isLoadingProposals || model.isLoadingReviewedProposals {
        ProgressView().controlSize(.small)
      } else {
        Button {
          if lane == .reviewed {
            model.loadReviewedProposals()
          } else {
            model.loadProposals()
          }
          if let primarySelectionId {
            model.loadProposalDetail(primarySelectionId, force: true)
          }
        } label: {
          Image(systemName: "arrow.clockwise")
        }
        .buttonStyle(.borderless)
        .help("Reload this review queue")
      }
      curatorMenu
    }
    .padding(.horizontal, 14)
    .padding(.top, 12)
    .padding(.bottom, 8)
  }

  private var primaryLanes: some View {
    HStack(spacing: 4) {
      laneButton(.reviewNow)
      laneButton(.mergeSuggestions)
      laneButton(.needsContext)
      Menu {
        laneMenuButton(.conflicts)
        laneMenuButton(.reviewed)
        laneMenuButton(.all)
        Divider()
        Button("Select first \(ProposalBatchValidation.maximumBatchSize) visible") {
          let selection = Array(visibleProposals.prefix(ProposalBatchValidation.maximumBatchSize))
          selectedIds = Set(selection.map(\.id))
          primarySelectionId = visibleProposals.first?.id
          selectionAnchorId = primarySelectionId
        }
        .disabled(visibleProposals.isEmpty)
        Button("Clear selection") {
          selectedIds.removeAll()
          primarySelectionId = nil
          ensureSelection()
        }
        .disabled(selectedIds.count < 2)
        Divider()
        Button("Look for merges…") { lookForMerges() }
          .disabled(model.busyAction != nil || !model.pendingProposalDecisionIds.isEmpty || model.proposals.count < 2)
      } label: {
        Image(systemName: "ellipsis")
          .frame(width: 24, height: 22)
      }
      .menuStyle(.borderlessButton)
      .help("More queues and curator tools")
    }
    .padding(.horizontal, 12)
    .padding(.bottom, 8)
  }

  private func laneButton(_ target: ProposalReviewLane) -> some View {
    Button {
      lane = target
    } label: {
      HStack(spacing: 5) {
        Text(target.title)
          .lineLimit(1)
        Text("\(laneCount(target))")
          .foregroundColor(lane == target ? .primary : .secondary)
      }
      .font(.caption)
      .padding(.horizontal, 8)
      .frame(height: 26)
      .background(lane == target ? Color.accentColor.opacity(0.16) : Color.clear)
      .clipShape(RoundedRectangle(cornerRadius: 5))
    }
    .buttonStyle(.plain)
    .accessibilityLabel("\(target.title), \(laneCount(target)) proposals")
    .accessibilityAddTraits(lane == target ? .isSelected : [])
  }

  private func laneMenuButton(_ target: ProposalReviewLane) -> some View {
    Button("\(target.title) (\(laneCount(target)))") { lane = target }
  }

  private var searchAndSort: some View {
    HStack(spacing: 8) {
      Image(systemName: "magnifyingglass")
        .foregroundColor(.secondary)
      TextField("Search proposals", text: $search)
        .textFieldStyle(.plain)
      Menu {
        ForEach(ProposalReviewSort.allCases) { option in
          Button {
            sort = option
          } label: {
            if sort == option {
              Label(option.title, systemImage: "checkmark")
            } else {
              Text(option.title)
            }
          }
        }
      } label: {
        Image(systemName: "line.3.horizontal.decrease")
      }
      .menuStyle(.borderlessButton)
      .frame(width: 24)
      .help("Sort: \(sort.title)")
    }
    .padding(.horizontal, 10)
    .frame(height: 34)
    .background(Color(nsColor: .controlBackgroundColor))
    .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color(nsColor: .separatorColor), lineWidth: 0.5))
    .padding(.horizontal, 12)
    .padding(.bottom, 9)
  }

  @ViewBuilder
  private var activityStrip: some View {
    if let notice = model.proposalDecisionNotice {
      HStack(spacing: 7) {
        Image(systemName: notice.failed == 0 ? "checkmark.circle.fill" : "exclamationmark.triangle.fill")
          .foregroundColor(notice.failed == 0 ? .green : .orange)
        Text(notice.message)
          .font(.caption)
          .lineLimit(2)
        Spacer()
        if notice.failed > 0 {
          Button("Reload") { model.loadProposals() }
            .controlSize(.small)
        }
        Button {
          model.clearProposalDecisionNotice()
        } label: {
          Image(systemName: "xmark")
        }
        .buttonStyle(.borderless)
        .help("Dismiss")
      }
      .padding(.horizontal, 12)
      .padding(.vertical, 7)
      .background((notice.failed == 0 ? Color.green : Color.orange).opacity(0.10))
    } else if let busyAction = model.busyAction,
              ["Looking for merges", "Merging selected proposals", "Sending proposal back"].contains(busyAction) {
      HStack(spacing: 7) {
        ProgressView().controlSize(.small)
        Text(busyAction == "Looking for merges" ? "Looking for related proposals…" : "\(busyAction)…")
          .font(.caption)
        Spacer()
      }
      .padding(.horizontal, 12)
      .padding(.vertical, 7)
      .background(Color.accentColor.opacity(0.08))
      .accessibilityLabel(busyAction)
    } else if let action = model.lastAction,
              ["Look for Merges", "Merge Selected Proposals", "Merge Proposal Group", "Send Proposal Back"].contains(action.title) {
      if action.succeeded {
        HStack(spacing: 7) {
          Image(systemName: "checkmark.circle.fill").foregroundColor(.green)
          Text(action.summary).font(.caption).lineLimit(2)
          Spacer()
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 7)
        .background(Color.green.opacity(0.10))
        .accessibilityElement(children: .combine)
      } else {
        compactMergeFailure(action)
      }
    }
  }

  private var proposalList: some View {
    Group {
      if let error = queueError {
        VStack(spacing: 9) {
          Image(systemName: "exclamationmark.triangle")
            .font(.title2)
            .foregroundColor(.orange)
          Text("Could not load proposals")
            .font(.callout.weight(.semibold))
          Text(error)
            .font(.caption)
            .foregroundColor(.secondary)
            .multilineTextAlignment(.center)
          Button("Retry") {
            if lane == .reviewed {
              model.loadReviewedProposals()
            } else {
              model.loadProposals()
            }
          }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .padding(24)
      } else if visibleProposals.isEmpty && !model.isLoadingProposals && !model.isLoadingReviewedProposals {
        VStack(spacing: 8) {
          Image(systemName: search.isEmpty ? "tray" : "magnifyingglass")
            .font(.title2)
            .foregroundColor(.secondary)
          Text(search.isEmpty ? "Nothing in \(lane.title.lowercased())." : "No matching proposals.")
            .font(.callout)
            .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .padding(20)
      } else {
        ScrollViewReader { proxy in
          ScrollView {
            LazyVStack(spacing: 0) {
              ForEach(visibleProposals) { proposal in
                ProposalQueueRow(
                  proposal: proposal,
                  isPrimary: primarySelectionId == proposal.id,
                  isSelected: selectedIds.contains(proposal.id),
                  relatedCount: relatedCount(for: proposal),
                  isDeferred: reviewLaterIds.contains(proposal.id),
                  select: { selectRow(proposal.id) },
                  toggle: { toggleSelection(proposal.id) }
                )
                .id(proposal.id)
              }
            }
          }
          .onChange(of: primarySelectionId) { id in
            if let id {
              withAnimation(.easeOut(duration: 0.12)) {
                proxy.scrollTo(id, anchor: .center)
              }
            }
          }
        }
      }
    }
  }

  private var queueFooter: some View {
    HStack(spacing: 8) {
      if selectedIds.count > 1 {
        Text("\(selectedIds.count) selected\(selectedIds.count == ProposalBatchValidation.maximumBatchSize ? " · batch limit" : "")")
          .font(.caption.weight(.semibold))
        Button("Clear") {
          selectedIds.removeAll()
          primarySelectionId = nil
          ensureSelection()
        }
        .controlSize(.small)
      } else {
        adminBadge
      }
      Spacer()
      if !model.pendingProposalDecisionIds.isEmpty {
        ProgressView().controlSize(.mini)
        Text("Saving \(model.pendingProposalDecisionIds.count) decision\(model.pendingProposalDecisionIds.count == 1 ? "" : "s")")
          .font(.caption2)
          .foregroundColor(.secondary)
      } else if !reviewLaterIds.isEmpty {
        Text("\(reviewLaterIds.count) later")
          .font(.caption2)
          .foregroundColor(.secondary)
      }
      if sessionDecisionCount > 0 {
        Text("\(sessionDecisionCount) sent · \(model.proposals.count) remaining")
          .font(.caption2)
          .foregroundColor(.secondary)
      }
    }
    .padding(.horizontal, 12)
    .padding(.vertical, 8)
  }

  // MARK: - Detail and decisions

  @ViewBuilder
  private var detailPane: some View {
    if let proposal = primaryProposal {
      ProposalDecisionPane(
        model: model,
        proposal: proposal,
        selectedProposals: selectedProposals,
        relatedCount: relatedCount(for: proposal),
        reviewLater: reviewLater,
        needsWork: requestNeedsWork,
        mergeSelected: mergeSelected,
        decide: confirmDecision
      )
      .id(proposal.id)
    } else {
      VStack(spacing: 10) {
        Image(systemName: "doc.text.magnifyingglass")
          .font(.largeTitle)
          .foregroundColor(.secondary)
        Text("Select a proposal to review what Brainstack will learn.")
          .font(.callout)
          .foregroundColor(.secondary)
      }
      .frame(maxWidth: .infinity, maxHeight: .infinity)
      .background(Color(nsColor: .textBackgroundColor))
    }
  }

  private func confirmDecision(_ action: String) {
    let candidates = selectedProposals.filter(\.isOpen)
    guard !candidates.isEmpty else { return }
    let applying = action == "apply"
    guard !applying || candidates.allSatisfy({ $0.targetPage != nil }) else { return }
    if applying {
      guard ProposalBatchValidation.duplicateTargets(in: candidates).isEmpty else { return }
      guard candidates.allSatisfy({ model.proposalDetails[$0.id] != nil }) else { return }
      guard candidates.allSatisfy({ [.reviewNow, .mergeSuggestions].contains($0.reviewLane) }) else { return }
    }
    let targets = Set(candidates.compactMap(\.targetPage))
    let sameTargetRemaining = sourceProposals.filter { proposal in
      proposal.isOpen && !candidates.contains(where: { $0.id == proposal.id })
        && proposal.targetPage.map(targets.contains) == true
    }
    let totalSources = candidates.reduce(0) { $0 + $1.includedSourceCount }
    let totalDiff = candidates.compactMap { model.proposalDetails[$0.id]?.diff }.reduce(into: (additions: 0, removals: 0)) { result, diff in
      let stats = ProposalDiffStats(diff: diff)
      result.additions += stats.additions
      result.removals += stats.removals
    }
    let manifest = candidates.map { proposal -> String in
      let sources = proposal.includedSourceCount
      return "• \(displayProposalTitle(proposal.title)) — \(proposal.targetPage ?? "no destination"), \(sources) source\(sources == 1 ? "" : "s")"
    }
    let message: String
    if candidates.count == 1, let proposal = candidates.first {
      message = [
        applying ? "Apply this proposal to the shared brain?" : "Reject this proposal?",
        "",
        displayProposalTitle(proposal.title),
        applying ? "Target: \(proposal.targetPage ?? "configured wiki page")" : nil,
        applying ? "Change: +\(totalDiff.additions) / -\(totalDiff.removals) lines from \(totalSources) source\(totalSources == 1 ? "" : "s")" : nil,
        relatedCount(for: proposal) > 1 ? "Related proposals remaining: \(relatedCount(for: proposal) - 1)" : nil,
        applying && !sameTargetRemaining.isEmpty
          ? "Also closes \(sameTargetRemaining.count) older open proposal\(sameTargetRemaining.count == 1 ? "" : "s") for the same destination."
          : nil,
        applying ? "The queue will advance immediately while Brainstack saves the decision." : "The queue will advance immediately while Brainstack saves the rejection."
      ].compactMap { $0 }.joined(separator: "\n")
    } else {
      message = [
        applying ? "Apply \(candidates.count) selected proposals to the shared brain?" : "Reject \(candidates.count) selected proposals?",
        "",
        applying ? "Targets: \(targets.count) wiki page\(targets.count == 1 ? "" : "s")" : nil,
        applying ? "Change: +\(totalDiff.additions) / -\(totalDiff.removals) lines from \(totalSources) sources" : nil,
        applying && !sameTargetRemaining.isEmpty
          ? "Also closes \(sameTargetRemaining.count) older open proposal\(sameTargetRemaining.count == 1 ? "" : "s") for those destinations."
          : nil,
        "",
        manifest.joined(separator: "\n"),
        "The queue will update immediately. Any decision the destination rejects will return when you reload."
      ].compactMap { $0 }.joined(separator: "\n")
    }
    let title = applying ? (candidates.count == 1 ? "Apply to Brain" : "Apply Selected to Brain") : (candidates.count == 1 ? "Reject Proposal" : "Reject Selected Proposals")
    if Confirm.ask(title: title, message: message) {
      sessionDecisionCount += candidates.count
      model.decideProposals(candidates, action: action)
    }
  }

  private func reviewLater() {
    let ids = Set(selectedProposals.map(\.id))
    reviewLaterIds.formUnion(ids)
    selectedIds.subtract(ids)
    primarySelectionId = nil
    ensureSelection(preferNonDeferred: true)
  }

  private func requestNeedsWork() {
    guard selectedProposals.count == 1, let proposal = selectedProposals.first else { return }
    guard let feedback = TextPrompt.ask(
      title: "What needs work?",
      message: "Tell the curator what context, evidence, scope, or correction this proposal needs before it can enter the brain.",
      placeholder: "For example: narrow this to the Brainstack repo and add a source excerpt"
    ) else { return }
    model.requestProposalWork(proposal, reason: feedback)
  }

  private func mergeSelected() {
    let selected = selectedProposals.filter(\.isOpen)
    guard selected.count > 1 else { return }
    let groups = Set(selected.compactMap(\.clusterKey))
    guard groups.count == 1 else { return }
    let names = selected.map { "• \(displayProposalTitle($0.title))" }.joined(separator: "\n")
    let message = [
      "Create one consolidated proposal from these \(selected.count) related items?",
      "",
      names,
      "",
      "The selected source proposals will be marked as absorbed. The resulting proposal will still require review before it enters the brain."
    ].joined(separator: "\n")
    if Confirm.ask(title: "Merge Selected Proposals", message: message) {
      model.mergeSelectedProposals(selected)
    }
  }

  // MARK: - Queue behavior

  private func laneCount(_ target: ProposalReviewLane) -> Int {
    if target == .reviewed { return model.reviewedProposals.count }
    if target == .all { return model.proposals.count }
    return model.proposals.filter { $0.reviewLane == target }.count
  }

  private func relatedCount(for proposal: ProposalSummary) -> Int {
    guard let key = proposal.clusterKey, !key.isEmpty else { return 0 }
    return model.proposals.filter { $0.clusterKey == key }.count
  }

  private func proposalComesFirst(_ left: ProposalSummary, _ right: ProposalSummary) -> Bool {
    let leftDeferred = reviewLaterIds.contains(left.id)
    let rightDeferred = reviewLaterIds.contains(right.id)
    if leftDeferred != rightDeferred { return !leftDeferred }
    switch sort {
    case .mostLeverage:
      if left.reviewLeverage != right.reviewLeverage { return left.reviewLeverage > right.reviewLeverage }
    case .needsAttention:
      let leftPriority = attentionPriority(left)
      let rightPriority = attentionPriority(right)
      if leftPriority != rightPriority { return leftPriority > rightPriority }
    case .oldest:
      return proposalDate(left) < proposalDate(right)
    case .newest:
      return proposalDate(left) > proposalDate(right)
    }
    return proposalDate(left) > proposalDate(right)
  }

  private func attentionPriority(_ proposal: ProposalSummary) -> Int {
    switch proposal.reviewLane {
    case .conflicts: return 4
    case .needsContext: return 3
    case .mergeSuggestions: return 2
    case .reviewNow: return 1
    default: return 0
    }
  }

  private func proposalDate(_ proposal: ProposalSummary) -> Date {
    ISO8601DateFormatter().date(from: proposal.createdAt) ?? .distantPast
  }

  private func selectOnly(_ id: String) {
    primarySelectionId = id
    selectedIds = [id]
    selectionAnchorId = id
  }

  private func selectRow(_ id: String) {
    let modifiers = NSApp.currentEvent?.modifierFlags ?? []
    if modifiers.contains(.command) {
      toggleSelection(id)
      return
    }
    if modifiers.contains(.shift),
       let anchor = selectionAnchorId ?? primarySelectionId,
       let start = visibleProposals.firstIndex(where: { $0.id == anchor }),
       let end = visibleProposals.firstIndex(where: { $0.id == id }) {
      let step = start <= end ? 1 : -1
      let indices = Array(stride(from: start, through: end, by: step).prefix(ProposalBatchValidation.maximumBatchSize))
      selectedIds = Set(indices.map { visibleProposals[$0].id })
      self.primarySelectionId = indices.last.map { visibleProposals[$0].id }
      return
    }
    selectOnly(id)
  }

  private func toggleSelection(_ id: String) {
    if selectedIds.contains(id) {
      selectedIds.remove(id)
      if primarySelectionId == id {
        primarySelectionId = visibleProposals.first { selectedIds.contains($0.id) }?.id
      }
    } else {
      guard selectedIds.count < ProposalBatchValidation.maximumBatchSize else { return }
      selectedIds.insert(id)
      primarySelectionId = id
      selectionAnchorId = id
    }
    ensureSelection()
  }

  private func resetSelection() {
    selectedIds.removeAll()
    primarySelectionId = nil
    selectionAnchorId = nil
    ensureSelection()
  }

  private func ensureSelection(preferNonDeferred: Bool = false) {
    let visibleIds = Set(visibleProposals.map(\.id))
    selectedIds.formIntersection(visibleIds)
    if selectionAnchorId.map(visibleIds.contains) != true {
      selectionAnchorId = primarySelectionId.flatMap { visibleIds.contains($0) ? $0 : nil }
    }
    if let primarySelectionId, visibleIds.contains(primarySelectionId) {
      if selectedIds.isEmpty { selectedIds.insert(primarySelectionId) }
      if selectionAnchorId == nil { selectionAnchorId = primarySelectionId }
      return
    }
    let candidate = preferNonDeferred
      ? visibleProposals.first { !reviewLaterIds.contains($0.id) } ?? visibleProposals.first
      : visibleProposals.first
    primarySelectionId = candidate?.id
    selectionAnchorId = candidate?.id
    if let id = candidate?.id, selectedIds.isEmpty { selectedIds.insert(id) }
  }

  private func moveSelection(_ direction: MoveCommandDirection) {
    guard direction == .up || direction == .down, !visibleProposals.isEmpty else { return }
    let currentIndex = primarySelectionId.flatMap { id in visibleProposals.firstIndex { $0.id == id } } ?? 0
    let delta = direction == .up ? -1 : 1
    let nextIndex = min(max(currentIndex + delta, 0), visibleProposals.count - 1)
    selectOnly(visibleProposals[nextIndex].id)
  }

  // MARK: - Curator and status

  private var curatorMenu: some View {
    Menu {
      Button("Look for merges…") { lookForMerges() }
        .disabled(!model.pendingProposalDecisionIds.isEmpty || model.proposals.count < 2)
      Divider()
      Button("Curator status") {
        model.runAction("Curator Status", refreshAfter: false) { await $0.curatorStatus() }
      }
      Button("Run curator now…") {
        if Confirm.ask(title: "Run Curator", message: "Review new imports and submit proposals now? This does not edit the wiki.") {
          model.runAction("Curator Run") { await $0.curatorRun() }
        }
      }
      Button("Install curator routine…") {
        if Confirm.ask(title: "Install Curator", message: "Install scheduled proposal generation on the control host? Wiki changes still require a review decision.") {
          model.runAction("Install Curator") { await $0.curatorInstall() }
        }
      }
      Divider()
      Button("Open curation page") { model.openCurationPage() }
    } label: {
      Image(systemName: "ellipsis.circle")
    }
    .menuStyle(.borderlessButton)
    .frame(width: 28)
    .help("Queue and curator tools")
    .disabled(model.busyAction != nil || !model.pendingProposalDecisionIds.isEmpty)
  }

  private func lookForMerges() {
    let message = [
      "Scan the top 100 open proposals for related lessons?",
      "",
      "High-confidence matches create consolidated proposals. Lower-confidence matches remain review suggestions. This does not apply wiki edits."
    ].joined(separator: "\n")
    if Confirm.ask(title: "Look for Proposal Merges", message: message) {
      showMergeActionDetails = false
      model.lookForProposalMerges()
    }
  }

  private func compactMergeFailure(_ action: ActionOutcome) -> some View {
    HStack(alignment: .top, spacing: 7) {
      Image(systemName: "exclamationmark.triangle.fill")
        .foregroundColor(.orange)
      VStack(alignment: .leading, spacing: 2) {
        Text(action.title == "Look for Merges" ? "Merge scan needs attention" : "\(action.title) needs attention")
          .font(.caption.weight(.semibold))
        Text(action.summary)
          .font(.caption2)
          .foregroundColor(.secondary)
          .lineLimit(showMergeActionDetails ? nil : 2)
        if !action.output.isEmpty {
          Button(showMergeActionDetails ? "Hide details" : "Show details") {
            showMergeActionDetails.toggle()
          }
          .buttonStyle(.link)
          .font(.caption2)
        }
        if showMergeActionDetails && !action.output.isEmpty {
          Text(action.output)
            .font(.system(size: 10, design: .monospaced))
            .textSelection(.enabled)
            .lineLimit(8)
        }
      }
      Spacer()
    }
    .padding(.horizontal, 12)
    .padding(.vertical, 7)
    .background(Color.orange.opacity(0.10))
  }

  private var adminBadge: some View {
    let presentation: (String, String, Color) = {
      switch model.adminAvailability {
      case .available: return ("System connected", "The last decision reached the control host.", .green)
      case .unavailable: return ("Decision path blocked", "Reloading proposals still works, but decisions need attention.", .orange)
      case .unknown: return ("Ready to review", "The decision path will be verified when you apply or reject.", .secondary)
      }
    }()
    return Label(presentation.0, systemImage: model.adminAvailability == .available ? "checkmark.circle" : "circle")
      .font(.caption2)
      .foregroundColor(presentation.2)
      .help(presentation.1)
  }
}
