import AppKit
import SwiftUI
import BrainstackMenuCore

/// Operator Mode panel: curator controls plus proposal review. Every wiki-mutating
/// action requires an explicit confirmation showing the proposal id/title and target.
struct OperatorView: View {
  @ObservedObject var model: AppModel

  var body: some View {
    VStack(alignment: .leading, spacing: 6) {
      HStack {
        Text("Operator").font(.caption.weight(.semibold)).foregroundColor(.orange)
        Spacer()
        adminBadge
      }
      HStack(spacing: 8) {
        Button("Refresh Proposals") { model.loadProposals() }
        Menu("More") {
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
          Button("Copy Install Command") { model.copyCuratorInstallCommand() }
        }
      }
      .controlSize(.small)
      .disabled(model.busyAction != nil)

      if let error = model.proposalsError {
        Text(error).font(.caption).foregroundColor(.orange)
      }
      if !model.proposals.isEmpty {
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
    let (label, tint): (String, Color) = {
      switch model.adminAvailability {
      case .available: return ("admin: available", .green)
      case .unavailable: return ("admin: unavailable", .orange)
      case .unknown: return ("admin: not checked", .secondary)
      }
    }()
    return Text(label).font(.caption2).foregroundColor(tint)
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
        Text(proposal.title).font(.system(size: 11, weight: .medium)).lineLimit(1)
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
        Text(target).font(.system(size: 10, design: .monospaced)).foregroundColor(.secondary).lineLimit(1)
      }
      HStack(spacing: 8) {
        Button("Show") { showProposal(proposal) }
        Menu("Decision") {
          Button("Approve") { decide(proposal, action: "approve") }
          Button("Apply") { decide(proposal, action: "apply") }
            .disabled(proposal.targetPage == nil)
          Button("Reject") { decide(proposal, action: "reject") }
        }
      }
      .controlSize(.mini)
      .disabled(model.busyAction != nil)
    }
    .padding(.vertical, 2)
  }

  private func riskColor(_ risk: String) -> Color {
    switch risk {
    case "low": return .green
    case "medium": return .yellow
    default: return .red
    }
  }

  private func showProposal(_ proposal: ProposalSummary) {
    model.runAction("Show \(proposal.id)", refreshAfter: false) { client in
      await client.proposalShow(id: proposal.id)
    }
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
