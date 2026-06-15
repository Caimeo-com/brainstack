import AppKit
import SwiftUI
import BrainstackMenuCore

private struct ScrollContentHeightKey: PreferenceKey {
  static var defaultValue: CGFloat = 0
  static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) {
    value = max(value, nextValue())
  }
}

struct DashboardView: View {
  @ObservedObject var model: AppModel
  var openPreferences: () -> Void
  var openOperatorConsole: () -> Void
  // Start with a plausible height so the popover doesn't open tiny and balloon a
  // moment later (which can make AppKit re-place it).
  @State private var scrollContentHeight: CGFloat = 360
  @State private var showDetails = false

  var body: some View {
    VStack(alignment: .leading, spacing: 10) {
      header
      Divider()
      // The middle scrolls; header and footer stay pinned so the popover can never
      // grow past the screen edge regardless of section/proposal count. The scroll
      // region is exactly content height until it would exceed the screen cap.
      ScrollView(.vertical) {
        VStack(alignment: .leading, spacing: 10) {
          if model.lastReport == nil {
            setupGuidance
          } else {
            attentionSummary
            actions
            DisclosureGroup("Details", isExpanded: $showDetails) {
              sectionList
            }
            .font(.caption)
          }
          if let action = model.lastAction {
            Divider()
            lastActionView(action)
          }
        }
        .padding(.bottom, 2)
        .background(
          GeometryReader { proxy in
            Color.clear.preference(key: ScrollContentHeightKey.self, value: proxy.size.height)
          }
        )
      }
      .onPreferenceChange(ScrollContentHeightKey.self) { scrollContentHeight = $0 }
      .frame(height: min(max(scrollContentHeight, 80), Self.maxScrollHeight))
      Divider()
      footer
    }
    .padding(12)
    .frame(width: 380)
  }

  /// Cap the scrollable region so header + content + footer always fit inside the
  /// screen's visible frame (menu bar excluded). The margin covers the pinned
  /// header/footer/action rows, popover chrome, and anchor offset under the menu bar.
  static var maxScrollHeight: CGFloat {
    let visible = NSScreen.main?.visibleFrame.height ?? 800
    return max(240, visible - 320)
  }

  private var header: some View {
    HStack(spacing: 8) {
      Circle()
        .fill(color(for: model.overallState))
        .frame(width: 10, height: 10)
      VStack(alignment: .leading, spacing: 1) {
        HStack(spacing: 6) {
          Text("Brainstack").font(.headline)
          if let machine = model.lastReport?.machine {
            Text(machine).font(.subheadline).foregroundColor(.secondary)
          }
          if let profile = model.lastReport?.profile {
            Text(profile)
              .font(.caption)
              .foregroundColor(.black)
              .padding(.horizontal, 6)
              .padding(.vertical, 1)
              .background(Color.white.opacity(0.88))
              .clipShape(Capsule())
          }
        }
        Text(headerSubtitle).font(.caption).foregroundColor(.secondary)
      }
      Spacer()
      if model.isRefreshing {
        ProgressView().controlSize(.small)
      } else {
        Button {
          model.refresh()
        } label: {
          Image(systemName: "arrow.clockwise")
        }
        .buttonStyle(.borderless)
        .help("Refresh Status")
      }
      overflowMenu
    }
  }

  /// Everything that is not primary or contextually actionable lives here, grouped
  /// by intent, instead of as button rows in the popover body.
  private var overflowMenu: some View {
    Menu {
      Section("Open") {
        Button("Wiki") { model.openWiki() }
        Button("Shared Brain Folder") { model.openSharedBrainFolder() }
        Button("Config Folder") { model.openConfigFolder() }
        if model.operatorModeEnabled {
          Button("Curation Page") { model.openCurationPage() }
        }
      }
      Section("Maintain") {
        Button("Run Doctor") { performRepair(.doctor) }
        Button("Check Stack Updates") { performRepair(.checkUpdates) }
        Button("Flush Outbox…") { performRepair(.flushOutbox) }
        Button("Refresh Skills…") { performRepair(.refreshSkills) }
        Button("Install/Restart Daemon…") { performRepair(.restartDaemon) }
        Button("Install/Repair Hooks…") { performRepair(.repairHooks) }
      }
      Section {
        Button("Copy Diagnostics") { model.copyDiagnostics() }
      }
    } label: {
      Image(systemName: "ellipsis.circle")
    }
    .menuStyle(.borderlessButton)
    .menuIndicator(.hidden)
    .frame(width: 24)
    .help("More actions")
    .disabled(model.busyAction != nil)
  }

  /// Run a repair action, with confirmation when it mutates state.
  private func performRepair(_ kind: RepairKind) {
    if let confirmation = kind.confirmation, !Confirm.ask(title: confirmation.title, message: confirmation.message) {
      return
    }
    switch kind {
    case .doctor:
      model.runAction("Doctor") { await $0.doctor() }
    case .checkUpdates:
      model.runAction("Check Stack Updates", refreshAfter: false) { await $0.updates() }
    case .flushOutbox:
      model.runAction("Flush Outbox") { await $0.outboxFlush() }
    case .refreshSkills:
      model.runAction("Refresh Skills") { await $0.skillsRefresh() }
    case .restartDaemon:
      model.runAction("Install/Restart Daemon") { await $0.daemonInstall() }
    case .repairHooks:
      model.runAction("Install/Repair Hooks") { await $0.hooksInstall() }
    case .installCurator:
      model.runAction("Install Curator") { await $0.curatorInstall() }
    }
  }

  private var headerSubtitle: String {
    var parts: [String] = []
    if let checked = model.lastChecked {
      parts.append("checked \(relativeTimeText(since: checked))")
    }
    if model.stale {
      parts.append("STALE")
    }
    parts.append(model.summaryLine)
    return parts.joined(separator: " · ")
  }

  private var setupGuidance: some View {
    VStack(alignment: .leading, spacing: 8) {
      if case .binaryMissing(let path) = model.lastFailure {
        Text("brainctl was not found.").font(.callout).bold()
        Text("Looked at \(path) and standard locations. Install Brainstack or choose the binary in Preferences.")
          .font(.caption)
          .foregroundColor(.secondary)
        HStack {
          Button("Copy Install Command") { model.copySetupCommand() }
          Button("Choose Binary…") { openPreferences() }
        }
      } else if case .unsupportedBinary(let path) = model.lastFailure {
        Text("Installed brainctl is too old.").font(.callout).bold()
        Text("\(path) has no `status` command. Update Brainstack on this machine — the update command reinstalls the latest brainctl in place.")
          .font(.caption)
          .foregroundColor(.secondary)
        HStack {
          Button("Copy Update Command") { model.copySetupCommand() }
          Button("Choose Binary…") { openPreferences() }
        }
      } else if let failure = model.lastFailure {
        Text("Status unavailable").font(.callout).bold()
        Text(Diagnostics.describe(failure)).font(.caption).foregroundColor(.secondary)
        Button("Copy Install Command") { model.copySetupCommand() }
      } else {
        Text("No status yet.").font(.callout)
      }
    }
  }

  private var sectionList: some View {
    VStack(alignment: .leading, spacing: 4) {
      let localSections = ["daemon", "shared_brain", "outbox", "hooks", "skills"]
      let controlSections = ["brain_api", "control_source", "curator", "proposals", "product"]
      sectionGroup(title: "Local", names: localSections)
      sectionGroup(title: "Control", names: controlSections)
      let known = Set(localSections + controlSections + ["config"])
      let extra = (model.lastReport?.sectionNames ?? []).filter { !known.contains($0) }
      if !extra.isEmpty {
        sectionGroup(title: "Other", names: extra)
      }
      if let config = model.lastReport?.sections["config"], config.state != .ok {
        SectionRowView(name: "config", section: config)
      }
    }
  }

  private var attentionSummary: some View {
    VStack(alignment: .leading, spacing: 6) {
      let items = attentionItems
      if items.isEmpty {
        AttentionRowView(
          item: AttentionItem(
            title: "Everything usable",
            detail: "Daemon, shared brain, outbox, hooks, skills, and brain API are healthy.",
            severity: .ok
          )
        )
      } else {
        ForEach(items.prefix(4)) { item in
          if item.detail.contains("Operator Console") && model.operatorModeEnabled {
            Button {
              openOperatorConsole()
            } label: {
              AttentionRowView(item: item)
            }
            .buttonStyle(.plain)
            .help("Open the Operator Console")
          } else {
            AttentionRowView(item: item, repairTitle: item.repair?.buttonTitle, isBusy: model.busyAction != nil) {
              if let repair = item.repair {
                performRepair(repair)
              }
            }
          }
        }
        if items.count > 4 {
          Text("\(items.count - 4) more item(s) in Details")
            .font(.caption2)
            .foregroundColor(.secondary)
        }
      }
    }
  }

  private var attentionItems: [AttentionItem] {
    guard let report = model.lastReport else {
      return []
    }
    var items: [AttentionItem] = []
    if controlHostLooksOld(report) {
      items.append(AttentionItem(
        title: "Control host needs update",
        detail: "The control host is missing curator/proposal API endpoints. Update it, then refresh.",
        severity: .warn,
        repair: .checkUpdates
      ))
    }
    if let curator = report.sections["curator"], curator.state == .ok {
      let installed = curator.data?["curator"]?["installed"]?.boolValue
      if installed == false {
        items.append(AttentionItem(
          title: "Curator routine is not installed",
          detail: "Use Install to schedule proposal generation on the control host; wiki edits still require Accept.",
          severity: .warn,
          repair: .installCurator
        ))
      }
      let open = Int(curator.data?["open_proposals"]?.numberValue ?? 0)
      if open > 0 {
        items.append(AttentionItem(
          title: "\(open) proposal\(open == 1 ? "" : "s") awaiting review",
          detail: model.operatorModeEnabled ? "Review them in the Operator Console." : "Enable Operator Mode in Preferences to review them.",
          severity: .info
        ))
      }
    }
    for name in report.sectionNames {
      guard let section = report.sections[name] else {
        continue
      }
      if section.state == .fail {
        items.append(AttentionItem(title: "\(sectionLabel(name)) failed", detail: sectionMessage(name: name, section: section), severity: .fail, repair: repairKind(forSection: name)))
      } else if section.state == .warn && !isBenignProductWarning(name: name, section: section) && !isOldControlEndpointWarning(section) {
        items.append(AttentionItem(title: "\(sectionLabel(name)) needs attention", detail: sectionMessage(name: name, section: section), severity: .warn, repair: repairKind(forSection: name)))
      }
    }
    return uniqueAttentionItems(items)
  }

  private func uniqueAttentionItems(_ items: [AttentionItem]) -> [AttentionItem] {
    var seen = Set<String>()
    return items.filter { item in
      let key = "\(item.title)\n\(item.detail)"
      if seen.contains(key) {
        return false
      }
      seen.insert(key)
      return true
    }
  }

  @ViewBuilder
  private func sectionGroup(title: String, names: [String]) -> some View {
    let present = names.filter { model.lastReport?.sections[$0] != nil }
    if !present.isEmpty {
      Text(title.uppercased())
        .font(.caption2)
        .foregroundColor(.secondary)
        .padding(.top, 4)
      ForEach(present, id: \.self) { name in
        if let section = model.lastReport?.sections[name] {
          SectionRowView(name: name, section: section)
        }
      }
    }
  }

  /// Exactly two visible buttons: the everyday destination and the operator surface.
  /// Repairs appear inline on attention rows; the rest is in the overflow menu.
  private var actions: some View {
    VStack(alignment: .leading, spacing: 6) {
      HStack(spacing: 8) {
        Button {
          model.openWiki()
        } label: {
          Label("Open Wiki", systemImage: "globe")
        }
        if model.operatorModeEnabled {
          Button {
            openOperatorConsole()
          } label: {
            Label("Operator Console", systemImage: "checklist")
          }
        }
        Spacer()
      }
      .controlSize(.small)
      if let busy = model.busyAction {
        HStack(spacing: 6) {
          ProgressView().controlSize(.small)
          Text("Running: \(busy)…").font(.caption).foregroundColor(.secondary)
        }
      } else if model.isLoadingProposals {
        HStack(spacing: 6) {
          ProgressView().controlSize(.small)
          Text("Loading proposals…").font(.caption).foregroundColor(.secondary)
        }
      }
    }
  }

  private func lastActionView(_ action: ActionOutcome) -> some View {
    VStack(alignment: .leading, spacing: 2) {
      Text("\(action.title): \(action.summary)")
        .font(.caption)
        .foregroundColor(action.succeeded ? .secondary : .red)
      if !action.output.isEmpty {
        ScrollView {
          Text(action.output)
            .font(.system(size: 10, design: .monospaced))
            .frame(maxWidth: .infinity, alignment: .leading)
            .textSelection(.enabled)
        }
        .frame(maxHeight: 80)
      }
      if action.adminUnavailable {
        Text("Make BRAIN_ADMIN_TOKEN available to brainctl on this machine (control host secrets env or shell env), then retry.")
          .font(.caption2)
          .foregroundColor(.orange)
      }
      if action.unsupported {
        Text("Update Brainstack: git -C ~/brainstack pull && brainctl upgrade")
          .font(.caption2)
          .foregroundColor(.orange)
      }
    }
  }

  private var footer: some View {
    HStack {
      Button("Preferences…") { openPreferences() }
        .controlSize(.small)
      Spacer()
      Text("v\(AppVersion.current)").font(.caption2).foregroundColor(.secondary)
      Button("Quit") { NSApp.terminate(nil) }
        .controlSize(.small)
    }
  }
}

/// "just now" for fresh checks; RelativeDateTimeFormatter otherwise (it phrases
/// sub-second intervals as the future-tense "in 0 sec").
func relativeTimeText(since date: Date) -> String {
  let elapsed = Date().timeIntervalSince(date)
  if elapsed < 5 {
    return "just now"
  }
  let formatter = RelativeDateTimeFormatter()
  formatter.unitsStyle = .short
  return formatter.localizedString(for: date, relativeTo: Date())
}

func color(for state: OverallState) -> Color {
  switch state {
  case .green: return .green
  case .yellow: return .yellow
  case .red: return .red
  case .gray: return .gray
  }
}

struct SectionRowView: View {
  let name: String
  let section: StatusSection

  var body: some View {
    HStack(alignment: .firstTextBaseline, spacing: 6) {
      Circle()
        .fill(stateColor)
        .frame(width: 7, height: 7)
      Text(label)
        .font(.system(size: 12, weight: .medium))
        .frame(width: 92, alignment: .leading)
      Text(sectionMessage(name: name, section: section))
        .font(.system(size: 11))
        .foregroundColor(.secondary)
        .lineLimit(2)
        .truncationMode(.middle)
        .help(helpText)
      Spacer(minLength: 0)
    }
    .help(helpText)
  }

  private var label: String {
    sectionLabel(name)
  }

  private var helpText: String {
    var parts = [sectionMessage(name: name, section: section)]
    if let error = section.error {
      parts.append("error: \(error)")
    }
    if let duration = section.durationMs {
      parts.append(String(format: "%.0fms", duration))
    }
    return parts.filter { !$0.isEmpty }.joined(separator: "\n")
  }

  private var stateColor: Color {
    switch section.state {
    case .ok: return .green
    case .warn, .unknown: return .yellow
    case .fail: return .red
    case .disabled: return .gray
    }
  }
}

struct AttentionRowView: View {
  let item: AttentionItem
  var repairTitle: String?
  var isBusy = false
  var onRepair: () -> Void = {}

  var body: some View {
    HStack(alignment: .top, spacing: 8) {
      Circle()
        .fill(color)
        .frame(width: 8, height: 8)
        .padding(.top, 5)
      VStack(alignment: .leading, spacing: 2) {
        Text(item.title)
          .font(.system(size: 12, weight: .semibold))
        Text(item.detail)
          .font(.system(size: 11))
          .foregroundColor(.secondary)
          .fixedSize(horizontal: false, vertical: true)
      }
      Spacer(minLength: 0)
      if let repairTitle {
        // The fix lives on the problem, not in a button farm below it.
        Button(repairTitle) { onRepair() }
          .controlSize(.small)
          .disabled(isBusy)
          .padding(.top, 1)
      }
    }
    .padding(8)
    .background(color.opacity(0.12))
    .clipShape(RoundedRectangle(cornerRadius: 8))
  }

  private var color: Color {
    switch item.severity {
    case .ok: return .green
    case .info: return .blue
    case .warn: return .yellow
    case .fail: return .red
    }
  }
}

enum Confirm {
  /// Modal confirmation; safe to call from menu/popover button handlers.
  static func ask(title: String, message: String) -> Bool {
    let alert = NSAlert()
    alert.messageText = title
    alert.informativeText = message
    alert.alertStyle = .warning
    alert.addButton(withTitle: title)
    alert.addButton(withTitle: "Cancel")
    NSApp.activate(ignoringOtherApps: true)
    return alert.runModal() == .alertFirstButtonReturn
  }
}
