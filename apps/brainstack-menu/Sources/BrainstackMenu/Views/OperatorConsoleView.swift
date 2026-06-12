import AppKit
import SwiftUI
import BrainstackMenuCore

/// Dedicated operator window: master-detail proposal review with curator controls.
/// This is the content-management surface; the popover stays a status glance.
struct OperatorConsoleView: View {
  @ObservedObject var model: AppModel
  @State private var selectedId: String?
  @State private var statusFilter: String = "all"

  var body: some View {
    HSplitView {
      sidebar
        .frame(minWidth: 280, idealWidth: 320, maxWidth: 420)
      detailPane
        .frame(minWidth: 420, maxWidth: .infinity, maxHeight: .infinity)
    }
    .frame(minWidth: 760, minHeight: 480)
    .onAppear {
      model.loadProposals()
      if selectedId == nil {
        selectedId = filteredProposals.first?.id
      }
    }
    .onChange(of: selectedId) { id in
      if let id {
        model.loadProposalDetail(id)
      }
    }
    .onChange(of: model.proposals) { proposals in
      // Keep a valid selection: pick the first proposal on load and move on when
      // the selected one leaves the open list after a decision.
      if selectedId == nil || !proposals.contains(where: { $0.id == selectedId }) {
        selectedId = filteredProposals.first?.id ?? proposals.first?.id
      }
    }
  }

  // MARK: - Sidebar

  private var filteredProposals: [ProposalSummary] {
    statusFilter == "all" ? model.proposals : model.proposals.filter { $0.status == statusFilter }
  }

  private var sidebar: some View {
    VStack(alignment: .leading, spacing: 0) {
      HStack(spacing: 8) {
        Text("Proposals").font(.headline)
        Spacer()
        if model.isLoadingProposals {
          ProgressView().controlSize(.small)
        } else {
          Button {
            model.loadProposals()
            if let id = selectedId {
              model.loadProposalDetail(id, force: true)
            }
          } label: {
            Image(systemName: "arrow.clockwise")
          }
          .buttonStyle(.borderless)
          .help("Refresh proposal list")
        }
        curatorMenu
      }
      .padding(.horizontal, 12)
      .padding(.vertical, 10)

      Picker("", selection: $statusFilter) {
        Text("All (\(model.proposals.count))").tag("all")
        ForEach(statusCounts, id: \.status) { entry in
          Text("\(entry.status) (\(entry.count))").tag(entry.status)
        }
      }
      .pickerStyle(.menu)
      .controlSize(.small)
      .labelsHidden()
      .padding(.horizontal, 12)
      .padding(.bottom, 6)

      Divider()

      if let error = model.proposalsError {
        VStack(alignment: .leading, spacing: 6) {
          Text(error).font(.caption).foregroundColor(.orange)
          Button("Retry") { model.loadProposals() }.controlSize(.small)
        }
        .padding(12)
        Spacer()
      } else if filteredProposals.isEmpty && !model.isLoadingProposals {
        VStack(spacing: 8) {
          Image(systemName: "tray").font(.title2).foregroundColor(.secondary)
          Text("No open proposals.").font(.callout).foregroundColor(.secondary)
          Text("The curator submits proposals as it reviews new imports.")
            .font(.caption)
            .foregroundColor(.secondary)
            .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .padding(20)
      } else {
        List(selection: $selectedId) {
          ForEach(filteredProposals) { proposal in
            ProposalListRow(proposal: proposal)
              .tag(proposal.id)
          }
        }
        .listStyle(.inset)
      }

      Divider()
      sidebarFooter
    }
    .background(Color(nsColor: .windowBackgroundColor))
  }

  private var statusCounts: [(status: String, count: Int)] {
    Dictionary(grouping: model.proposals, by: \.status)
      .map { (status: $0.key, count: $0.value.count) }
      .sorted { $0.status < $1.status }
  }

  private var curatorMenu: some View {
    Menu {
      Button("Curator Status") {
        model.runAction("Curator Status", refreshAfter: false) { await $0.curatorStatus() }
      }
      Button("Run Curator Now…") {
        if Confirm.ask(title: "Run Curator", message: "Dispatch a brain-curator run now? It reviews new imports and submits proposals; it does not edit the wiki directly.") {
          model.runAction("Curator Run") { await $0.curatorRun() }
        }
      }
      Button("Install Curator Routine…") {
        if Confirm.ask(title: "Install Curator", message: "Install the brain-curator routine on the control host? It schedules proposal generation; it does not approve or apply wiki edits.") {
          model.runAction("Install Curator") { await $0.curatorInstall() }
        }
      }
      Divider()
      Button("Open Curation Page") { model.openCurationPage() }
    } label: {
      Image(systemName: "ellipsis.circle")
    }
    .menuStyle(.borderlessButton)
    .frame(width: 28)
    .help("Curator tools")
    .disabled(model.busyAction != nil)
  }

  private var sidebarFooter: some View {
    HStack(spacing: 6) {
      adminBadge
      Spacer()
      if let busy = model.busyAction {
        ProgressView().controlSize(.mini)
        Text(busy).font(.caption2).foregroundColor(.secondary).lineLimit(1)
      }
    }
    .padding(.horizontal, 12)
    .padding(.vertical, 8)
  }

  private var adminBadge: some View {
    let (label, tint, help): (String, Color, String) = {
      switch model.adminAvailability {
      case .available:
        return ("decision path: ready", .green, "The last approve/apply/reject command reached admin auth successfully.")
      case .unavailable:
        return ("decision path: blocked", .orange, "The last approve/apply/reject command could not reach admin auth.")
      case .unknown:
        return ("decision path: not tested", .secondary, "Listing and showing proposals are read-only. Approve, apply, or reject will test the admin decision path.")
      }
    }()
    return Label(label, systemImage: "key.fill")
      .font(.caption2)
      .foregroundColor(tint)
      .help(help)
  }

  // MARK: - Detail

  @ViewBuilder
  private var detailPane: some View {
    if let id = selectedId, let proposal = model.proposals.first(where: { $0.id == id }) {
      ProposalDetailPane(model: model, proposal: proposal)
    } else {
      VStack(spacing: 10) {
        Image(systemName: "doc.text.magnifyingglass").font(.largeTitle).foregroundColor(.secondary)
        Text("Select a proposal to review it.").font(.callout).foregroundColor(.secondary)
        Text("Approve/apply changes the shared brain wiki. Nothing is ever auto-approved by this app.")
          .font(.caption)
          .foregroundColor(.secondary)
      }
      .frame(maxWidth: .infinity, maxHeight: .infinity)
      .background(Color(nsColor: .textBackgroundColor))
    }
  }
}

private struct ProposalListRow: View {
  let proposal: ProposalSummary

  var body: some View {
    VStack(alignment: .leading, spacing: 3) {
      HStack(spacing: 6) {
        statusBadge(proposal.status)
        if proposal.legacyFormat {
          qualityBadge("legacy")
        }
        if let risk = proposal.risk {
          riskBadge(risk)
        }
        if let quality = proposal.qualityDecision, quality != "ready" {
          qualityBadge(quality)
        }
        Spacer()
        Text(shortDate(proposal.createdAt))
          .font(.caption2)
          .foregroundColor(.secondary)
      }
      Text(displayTitle(proposal.title))
        .font(.system(size: 12, weight: .medium))
        .lineLimit(2)
      if let target = proposal.targetPage {
        Text(target)
          .font(.system(size: 10, design: .monospaced))
          .foregroundColor(.secondary)
          .lineLimit(1)
          .truncationMode(.middle)
      } else if proposal.project != nil || proposal.scope != nil || proposal.clusterLabel != nil {
        Text([proposal.project, proposal.scope, proposal.clusterLabel].compactMap { $0 }.joined(separator: " / "))
          .font(.system(size: 10, design: .monospaced))
          .foregroundColor(.secondary)
          .lineLimit(1)
          .truncationMode(.middle)
      }
    }
    .padding(.vertical, 4)
  }
}

private struct ProposalDetailPane: View {
  @ObservedObject var model: AppModel
  let proposal: ProposalSummary

  private var detail: ProposalDetail? {
    model.proposalDetails[proposal.id]
  }

  var body: some View {
    VStack(alignment: .leading, spacing: 0) {
      detailHeader
      Divider()
      ScrollView {
        VStack(alignment: .leading, spacing: 14) {
          metadataGrid
          if let reason = detail?.reason, !reason.isEmpty {
            labeledBlock("Reason") {
              Text(reason).font(.callout).textSelection(.enabled)
            }
          }
          if detail?.qualityDecision != nil || detail?.qualityScore != nil || !(detail?.qualityReasons ?? []).isEmpty {
            labeledBlock("Quality Gate") {
              VStack(alignment: .leading, spacing: 5) {
                if let decision = detail?.qualityDecision {
                  Text("Decision: \(decision)").font(.callout.weight(.medium)).textSelection(.enabled)
                }
                if let score = detail?.qualityScore {
                  Text("Score: \(String(format: "%.0f%%", score * 100))").font(.caption).foregroundColor(.secondary)
                }
                ForEach(detail?.qualityReasons ?? [], id: \.self) { reason in
                  Text("- \(reason)").font(.caption).foregroundColor(.secondary).textSelection(.enabled)
                }
              }
            }
          }
          if let context = detail?.context, !context.isEmpty {
            labeledBlock("Context") {
              Text(context).font(.callout).textSelection(.enabled)
            }
          }
          if detail?.applicability != nil || detail?.nonApplicability != nil {
            labeledBlock("Applicability") {
              VStack(alignment: .leading, spacing: 8) {
                if let applicability = detail?.applicability {
                  Text(applicability).font(.callout).textSelection(.enabled)
                }
                if let nonApplicability = detail?.nonApplicability {
                  Text("Do not apply: \(nonApplicability)").font(.caption).foregroundColor(.secondary).textSelection(.enabled)
                }
              }
            }
          }
          if let evidence = detail?.evidenceRefs, !evidence.isEmpty {
            labeledBlock("Evidence") {
              Text(evidence.joined(separator: "\n"))
                .font(.system(size: 11, design: .monospaced))
                .textSelection(.enabled)
            }
          }
          if let body = detail?.body, !body.isEmpty {
            labeledBlock("Request") {
              Text(body)
                .font(.system(size: 12))
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
            }
          }
          if let diff = detail?.diff, !diff.isEmpty {
            labeledBlock("Proposed Change") {
              DiffView(diff: diff)
            }
          } else if proposal.targetPage != nil && detail != nil {
            Text("No machine-applicable content attached; this proposal documents a requested change.")
              .font(.caption)
              .foregroundColor(.secondary)
          }
          if let error = model.detailErrors[proposal.id] {
            Text(error).font(.caption).foregroundColor(.orange)
          }
          if detail == nil && model.detailErrors[proposal.id] == nil {
            HStack(spacing: 6) {
              ProgressView().controlSize(.small)
              Text("Loading proposal…").font(.caption).foregroundColor(.secondary)
            }
          }
        }
        .padding(16)
        .frame(maxWidth: .infinity, alignment: .leading)
      }
      Divider()
      decisionBar
    }
    .background(Color(nsColor: .textBackgroundColor))
  }

  private var detailHeader: some View {
    VStack(alignment: .leading, spacing: 4) {
      Text(displayTitle(proposal.title))
        .font(.title3.weight(.semibold))
        .lineLimit(2)
        .textSelection(.enabled)
      HStack(spacing: 8) {
        statusBadge(proposal.status)
        if detail?.legacyFormat == true || proposal.legacyFormat {
          qualityBadge("legacy")
        }
        if let risk = proposal.risk {
          riskBadge(risk)
        }
        if let quality = detail?.qualityDecision ?? proposal.qualityDecision {
          qualityBadge(quality)
        }
        if let confidence = detail?.confidence {
          Text("confidence \(String(format: "%.0f%%", confidence * 100))")
            .font(.caption2)
            .foregroundColor(.secondary)
        }
        Spacer()
        Text(proposal.id)
          .font(.system(size: 10, design: .monospaced))
          .foregroundColor(.secondary)
          .textSelection(.enabled)
      }
    }
    .padding(.horizontal, 16)
    .padding(.vertical, 12)
  }

  private var metadataGrid: some View {
    Grid(alignment: .leading, horizontalSpacing: 12, verticalSpacing: 4) {
      if let target = proposal.targetPage {
        GridRow {
          Text("Target").font(.caption).foregroundColor(.secondary)
          Text(target).font(.system(size: 11, design: .monospaced)).textSelection(.enabled)
        }
      }
      GridRow {
        Text("Created").font(.caption).foregroundColor(.secondary)
        Text(proposal.createdAt).font(.system(size: 11)).foregroundColor(.secondary)
      }
      if let sources = detail?.sourceIds, !sources.isEmpty {
        GridRow {
          Text("Sources").font(.caption).foregroundColor(.secondary)
          Text(sources.joined(separator: ", "))
            .font(.system(size: 11, design: .monospaced))
            .foregroundColor(.secondary)
            .textSelection(.enabled)
        }
      }
      if let source = sourceLabel {
        GridRow {
          Text("Source").font(.caption).foregroundColor(.secondary)
          Text(source).font(.system(size: 11, design: .monospaced)).foregroundColor(.secondary).textSelection(.enabled)
        }
      }
      metadataRow("Type", detail?.sourceType)
      metadataRow("Project", detail?.project ?? proposal.project)
      metadataRow("Domain", detail?.domain)
      metadataRow("Scope", detail?.scope ?? proposal.scope)
      metadataRow("Kind", detail?.memoryKind ?? proposal.memoryKind)
      metadataRow("Cluster", detail?.clusterLabel ?? proposal.clusterLabel)
      metadataRow("Related repo", detail?.relatedRepo)
      metadataRow("Review after", detail?.reviewAfter)
      metadataRow("Expires", detail?.expiresAt)
    }
  }

  private var sourceLabel: String? {
    let parts = [detail?.sourceHarness, detail?.sourceMachine]
      .compactMap { $0?.isEmpty == false ? $0 : nil }
    return parts.isEmpty ? nil : parts.joined(separator: " @ ")
  }

  @ViewBuilder
  private func metadataRow(_ label: String, _ value: String?) -> some View {
    if let value, !value.isEmpty {
      GridRow {
        Text(label).font(.caption).foregroundColor(.secondary)
        Text(value)
          .font(.system(size: 11, design: .monospaced))
          .foregroundColor(.secondary)
          .textSelection(.enabled)
      }
    }
  }

  @ViewBuilder
  private func labeledBlock(_ title: String, @ViewBuilder content: () -> some View) -> some View {
    VStack(alignment: .leading, spacing: 6) {
      Text(title.uppercased())
        .font(.caption2.weight(.semibold))
        .foregroundColor(.secondary)
      content()
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color(nsColor: .underPageBackgroundColor))
        .clipShape(RoundedRectangle(cornerRadius: 6))
    }
  }

  private var decisionBar: some View {
    HStack(spacing: 10) {
      Text("Decisions change the shared brain wiki and require confirmation.")
        .font(.caption2)
        .foregroundColor(.secondary)
      Spacer()
      Button("Reject…") { decide("reject") }
      Button("Approve…") { decide("approve") }
      if proposal.targetPage != nil {
        Button("Apply…") { decide("apply") }
          .keyboardShortcut(.defaultAction)
          .buttonStyle(.borderedProminent)
      }
    }
    .controlSize(.regular)
    .disabled(model.busyAction != nil)
    .padding(.horizontal, 16)
    .padding(.vertical, 10)
  }

  private func decide(_ action: String) {
    let consequence: String
    switch action {
    case "apply":
      consequence = "This APPLIES the proposed content to \(proposal.targetPage ?? "its target page") in the shared brain wiki."
    case "approve":
      consequence = proposal.targetPage != nil
        ? "This marks the proposal approved. Applying it to \(proposal.targetPage!) is a separate action."
        : "This marks the proposal approved."
    default:
      consequence = "This rejects the proposal."
    }
    let message = [
      "Proposal: \(displayTitle(proposal.title))",
      "Id: \(proposal.id)",
      proposal.targetPage.map { "Target: \($0)" },
      "Brain: \(model.lastReport?.sections["brain_api"]?.data?["base_url"]?.stringValue ?? "configured brain")",
      "",
      consequence
    ].compactMap { $0 }.joined(separator: "\n")
    if Confirm.ask(title: "\(action.capitalized) Proposal", message: message) {
      model.decideProposal(proposal, action: action)
    }
  }
}

/// Monospaced diff rendering with +/- line tinting.
private struct DiffView: View {
  let diff: String

  var body: some View {
    ScrollView(.horizontal) {
      VStack(alignment: .leading, spacing: 0) {
        ForEach(Array(diff.split(separator: "\n", omittingEmptySubsequences: false).enumerated()), id: \.offset) { _, line in
          Text(String(line.isEmpty ? " " : line))
            .font(.system(size: 11, design: .monospaced))
            .foregroundColor(lineColor(String(line)))
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(lineBackground(String(line)))
        }
      }
      .textSelection(.enabled)
    }
    .frame(maxHeight: 360)
  }

  private func lineColor(_ line: String) -> Color {
    if line.hasPrefix("+") { return .green }
    if line.hasPrefix("-") { return .red }
    return .secondary
  }

  private func lineBackground(_ line: String) -> Color {
    if line.hasPrefix("+") { return .green.opacity(0.08) }
    if line.hasPrefix("-") { return .red.opacity(0.08) }
    return .clear
  }
}

// MARK: - Shared bits

/// Curator proposals are commonly titled "Remember: …"; strip the boilerplate prefix
/// for display so the list reads as content, not ceremony.
private func displayTitle(_ title: String) -> String {
  if title.hasPrefix("Remember: ") {
    return String(title.dropFirst("Remember: ".count))
  }
  if title.hasPrefix("Remember ("), let range = title.range(of: "): ") {
    return String(title[range.upperBound...])
  }
  return title
}

private func shortDate(_ iso: String) -> String {
  guard let date = ISO8601DateFormatter().date(from: iso) else {
    return String(iso.prefix(10))
  }
  return relativeTimeText(since: date)
}

private func statusBadge(_ status: String) -> some View {
  let tint: Color = {
    switch status {
    case "pending": return .blue
    case "approved": return .green
    case "needs-human": return .orange
    default: return .secondary
    }
  }()
  return Text(status)
    .font(.caption2.weight(.medium))
    .padding(.horizontal, 6)
    .padding(.vertical, 1)
    .background(tint.opacity(0.18))
    .foregroundColor(tint)
    .clipShape(Capsule())
}

private func riskBadge(_ risk: String) -> some View {
  let tint: Color = {
    switch risk {
    case "low": return .green
    case "medium": return .yellow
    default: return .red
    }
  }()
  return Text("risk: \(risk)")
    .font(.caption2)
    .padding(.horizontal, 6)
    .padding(.vertical, 1)
    .background(tint.opacity(0.15))
    .foregroundColor(tint)
    .clipShape(Capsule())
}

private func qualityBadge(_ decision: String) -> some View {
  let tint: Color = {
    switch decision {
    case "ready": return .green
    case "legacy": return .secondary
    case "needs-evidence": return .yellow
    case "needs-context": return .orange
    case "too-vague": return .red
    default: return .secondary
    }
  }()
  return Text(decision)
    .font(.caption2.weight(.medium))
    .padding(.horizontal, 6)
    .padding(.vertical, 1)
    .background(tint.opacity(0.16))
    .foregroundColor(tint)
    .clipShape(Capsule())
}
