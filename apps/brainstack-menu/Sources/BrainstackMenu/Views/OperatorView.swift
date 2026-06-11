import AppKit
import SwiftUI
import BrainstackMenuCore

/// Operator Mode panel: curator controls plus proposal review. Every wiki-mutating
/// action requires an explicit confirmation showing the proposal id/title and target.
struct OperatorView: View {
  @ObservedObject var model: AppModel
  @State private var showTools = false

  var body: some View {
    VStack(alignment: .leading, spacing: 6) {
      HStack {
        Text("Operator").font(.caption.weight(.semibold)).foregroundColor(.orange)
        Spacer()
        adminBadge
      }
      HStack(spacing: 8) {
        Button("Refresh Proposals") { model.loadProposals() }
          .disabled(model.isLoadingProposals)
        Button(showTools ? "Hide Tools" : "Tools...") {
          showTools.toggle()
        }
      }
      .controlSize(.small)
      .disabled(model.busyAction != nil)

      if showTools {
        toolActions
      }

      if let error = model.proposalsError {
        Text(error).font(.caption).foregroundColor(.orange)
      }
      if model.isLoadingProposals {
        HStack(spacing: 6) {
          ProgressView().controlSize(.small)
          Text("Loading proposals…")
            .font(.caption)
            .foregroundColor(.secondary)
        }
      } else if !model.proposals.isEmpty {
        proposalCounts
        ForEach(model.proposals.prefix(8)) { proposal in
          proposalRow(proposal)
        }
      } else if model.proposalsError == nil {
        Text("No proposals loaded yet.")
          .font(.caption)
          .foregroundColor(.secondary)
      }
      Text("Approve/apply changes the shared brain wiki. Nothing is ever auto-approved by this app.")
        .font(.caption2)
        .foregroundColor(.secondary)
    }
  }

  private var adminBadge: some View {
    let (label, tint, help): (String, Color, String) = {
      switch model.adminAvailability {
      case .available:
        return ("decision path: ready", .green, "The last approve/apply/reject command reached admin auth successfully.")
      case .unavailable:
        return ("decision path: blocked", .orange, "The last approve/apply/reject command could not reach admin auth. Check the action output below.")
      case .unknown:
        return ("decision path: not tested", .secondary, "Listing and showing proposals are read-only. Approve, apply, or reject will test the admin decision path.")
      }
    }()
    return Text(label)
      .font(.caption2)
      .foregroundColor(tint)
      .help(help)
  }

  private var proposalCounts: some View {
    let counts = Dictionary(grouping: model.proposals, by: { $0.status }).mapValues(\.count)
    let summary = ["pending", "approved", "needs-human"]
      .compactMap { key in counts[key].map { "\($0) \(key)" } }
      .joined(separator: ", ")
    return Text("Open proposals: \(summary.isEmpty ? String(model.proposals.count) : summary)")
      .font(.caption)
      .foregroundColor(.secondary)
  }

  private func proposalRow(_ proposal: ProposalSummary) -> some View {
    VStack(alignment: .leading, spacing: 2) {
      HStack(spacing: 6) {
        Text("[\(proposal.status)]").font(.system(size: 10, design: .monospaced)).foregroundColor(.secondary)
        Text(proposal.title).font(.system(size: 11, weight: .medium)).lineLimit(1).help(proposal.title)
        if let risk = proposal.risk {
          Text(risk)
            .font(.caption2)
            .padding(.horizontal, 4)
            .background(riskColor(risk).opacity(0.2))
            .clipShape(Capsule())
        }
        Spacer()
      }
      if let target = proposal.targetPage {
        Text(target).font(.system(size: 10, design: .monospaced)).foregroundColor(.secondary).lineLimit(1).truncationMode(.middle).help(target)
      }
      HStack(spacing: 8) {
        Button("Show") { showProposal(proposal) }
        Button("Decide...") { chooseDecision(proposal) }
      }
      .controlSize(.mini)
      .disabled(model.busyAction != nil)
    }
    .padding(.vertical, 2)
    .help([
      proposal.title,
      "id: \(proposal.id)",
      "status: \(proposal.status)",
      proposal.targetPage.map { "target: \($0)" },
      proposal.risk.map { "risk: \($0)" }
    ].compactMap { $0 }.joined(separator: "\n"))
  }

  private func riskColor(_ risk: String) -> Color {
    switch risk {
    case "low": return .green
    case "medium": return .yellow
    default: return .red
    }
  }

  private var toolActions: some View {
    let columns = [GridItem(.adaptive(minimum: 118), spacing: 6)]
    return LazyVGrid(columns: columns, alignment: .leading, spacing: 6) {
      Button("Curation Page") { model.openCurationPage() }
      Button("Curator Status") {
        model.runAction("Curator Status", refreshAfter: false) { await $0.curatorStatus() }
      }
      Button("Run Curator") {
        if Confirm.ask(title: "Run Curator", message: "Dispatch a brain-curator run now? It reviews new imports and submits proposals; it does not edit the wiki directly.") {
          model.runAction("Curator Run") { await $0.curatorRun() }
        }
      }
      Button("Install Curator") {
        if Confirm.ask(title: "Install Curator", message: "Install the brain-curator routine on the control host? It schedules proposal generation; it does not approve or apply wiki edits.") {
          model.runAction("Install Curator") { await $0.curatorInstall() }
        }
      }
      Button("Copy Command") { model.copyCuratorInstallCommand() }
    }
    .controlSize(.small)
    .disabled(model.busyAction != nil)
  }

  private func showProposal(_ proposal: ProposalSummary) {
    model.runAction("Show \(proposal.id)", refreshAfter: false) { client in
      await client.proposalShow(id: proposal.id)
    }
  }

  private func chooseDecision(_ proposal: ProposalSummary) {
    var actions: [(title: String, action: String)] = []
    if proposal.targetPage != nil {
      actions.append(("Apply", "apply"))
    }
    actions.append(("Approve", "approve"))
    actions.append(("Reject", "reject"))

    let alert = NSAlert()
    alert.messageText = "Choose Proposal Decision"
    alert.informativeText = [
      proposal.title,
      "Id: \(proposal.id)",
      proposal.targetPage.map { "Target: \($0)" },
      "",
      "Apply writes the proposed content to the wiki. Approve marks it ready but does not write it. Reject declines it."
    ].compactMap { $0 }.joined(separator: "\n")
    for action in actions {
      alert.addButton(withTitle: action.title)
    }
    alert.addButton(withTitle: "Cancel")

    let response = alert.runModal()
    let index = response.rawValue - NSApplication.ModalResponse.alertFirstButtonReturn.rawValue
    guard actions.indices.contains(index) else {
      return
    }
    decide(proposal, action: actions[index].action)
  }

  private func decide(_ proposal: ProposalSummary, action: String) {
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
      "Proposal: \(proposal.title)",
      "Id: \(proposal.id)",
      proposal.targetPage.map { "Target: \($0)" },
      "Brain: \(model.lastReport?.sections["brain_api"]?.data?["base_url"]?.stringValue ?? "configured brain")",
      "",
      consequence
    ].compactMap { $0 }.joined(separator: "\n")
    // Every decision is explicit and confirmable; nothing is ever auto-approved.
    if Confirm.ask(title: "\(action.capitalized) Proposal", message: message) {
      model.decideProposal(proposal, action: action)
    }
  }
}
