import AppKit
import Combine
import Foundation
import BrainstackMenuCore

/// Central observable state for the menu bar app. All brainctl work happens off the
/// main thread; this model only publishes results.
@MainActor
final class AppModel: ObservableObject {
  @Published private(set) var lastReport: StatusReport?
  @Published private(set) var lastFailure: StatusFailure?
  @Published private(set) var stale = false
  @Published private(set) var lastChecked: Date?
  @Published private(set) var isRefreshing = false
  @Published private(set) var busyAction: String?
  @Published private(set) var actionLog: [ActionOutcome] = []
  @Published private(set) var proposals: [ProposalSummary] = []
  @Published private(set) var proposalsError: String?
  @Published private(set) var isLoadingProposals = false
  @Published private(set) var uploads: [UploadSummary] = []
  @Published private(set) var uploadsError: String?
  @Published private(set) var isLoadingUploads = false
  @Published private(set) var isUploading = false
  @Published private(set) var adminAvailability: AdminAvailability = .unknown
  @Published private(set) var proposalDetails: [String: ProposalDetail] = [:]
  @Published private(set) var loadingDetailId: String?
  @Published private(set) var detailErrors: [String: String] = [:]
  @Published private(set) var installerRunning = false
  @Published private(set) var pendingVerificationSections = Set<String>()
  @Published private(set) var isRefreshingFleet = false
  @Published private(set) var lastFleetChecked: Date?

  @Published var binaryPathPreference: String? {
    didSet { preferences.binaryPathPreference = binaryPathPreference }
  }
  @Published var configPath: String {
    didSet { preferences.configPath = configPath }
  }
  @Published var pollInterval: TimeInterval {
    didSet {
      preferences.pollInterval = pollInterval
      restartPolling()
    }
  }
  @Published var operatorModeEnabled: Bool {
    didSet { preferences.operatorModeEnabled = operatorModeEnabled }
  }
  @Published var notificationsEnabled: Bool {
    didSet { preferences.notificationsEnabled = notificationsEnabled }
  }
  @Published var startTailscaleOnLaunch: Bool {
    didSet { preferences.startTailscaleOnLaunch = startTailscaleOnLaunch }
  }

  enum AdminAvailability: String {
    case unknown
    case available
    case unavailable
  }

  let preferences: Preferences
  private let notifier = Notifier()
  private let transitionDetector = TransitionDetector()
  private var lastSnapshot: TransitionDetector.Snapshot?
  private var pollTimer: Timer?
  private var lastDurations: [String: TimeInterval] = [:]
  private var triedTailscaleAutoStart = false
  private var refreshQueued = false
  private var fleetRefreshQueued = false
  private var cachedFleetSection: StatusSection?
  private var cachedFleetRawSection: JSONValue?

  var onStateChange: (() -> Void)?

  init(preferences: Preferences = Preferences()) {
    self.preferences = preferences
    self.binaryPathPreference = preferences.binaryPathPreference
    self.configPath = preferences.configPath
    self.pollInterval = preferences.pollInterval
    self.operatorModeEnabled = preferences.operatorModeEnabled
    self.notificationsEnabled = preferences.notificationsEnabled
    self.startTailscaleOnLaunch = preferences.startTailscaleOnLaunch
  }

  var resolvedBinaryPath: String? {
    BrainctlClient.resolveBinary(preferred: binaryPathPreference)
  }

  var overallState: OverallState {
    guard let report = lastReport else {
      return .gray
    }
    if report.sectionNames.contains(where: { report.sections[$0]?.state == .fail }) {
      return .red
    }
    let actionableWarning = report.sectionNames.contains { name in
      guard let section = report.sections[name], section.state == .warn else {
        return false
      }
      return !isBenignWarning(name: name, section: section, report: report)
    }
    if actionableWarning || controlHostLooksOld(report) || curatorRoutineMissing(report) {
      return .yellow
    }
    return .green
  }

  var tooltip: String {
    var parts: [String] = ["Brainstack"]
    if let report = lastReport {
      if let machine = report.machine { parts.append(machine) }
      if let profile = report.profile { parts.append(profile) }
    }
    if let lastChecked {
      let formatter = DateFormatter()
      formatter.dateStyle = .none
      formatter.timeStyle = .medium
      parts.append("checked \(formatter.string(from: lastChecked))\(stale ? " (stale)" : "")")
    }
    parts.append(summaryLine)
    return parts.joined(separator: " · ")
  }

  var summaryLine: String {
    if let lastFailure, lastReport == nil {
      return Diagnostics.describe(lastFailure)
    }
    if let pending = pendingVerificationSummary {
      return pending
    }
    guard let report = lastReport else {
      return "no status yet"
    }
    return statusSummaryLine(for: report)
  }

  var pendingVerificationSummary: String? {
    guard !pendingVerificationSections.isEmpty else {
      return nil
    }
    let labels = pendingVerificationSections.sorted().map { sectionLabel($0).lowercased() }
    if labels.count == 1, let label = labels.first {
      return "checking updated \(label) status"
    }
    return "checking updated status: \(labels.joined(separator: ", "))"
  }

  func isSectionVerificationPending(_ name: String) -> Bool {
    pendingVerificationSections.contains(name)
  }

  private func client() -> BrainctlClient? {
    guard let binary = resolvedBinaryPath else {
      return nil
    }
    return BrainctlClient(binaryPath: binary, configPath: (configPath as NSString).expandingTildeInPath)
  }

  var uploadTargetMachines: [String] {
    var names: [String] = []
    if let machine = lastReport?.machine, !machine.isEmpty {
      names.append(machine)
    }
    names.append(contentsOf: lastReport?.fleetMachines.map(\.name) ?? [])
    var seen = Set<String>()
    let unique = names.filter { name in
      let key = name.lowercased()
      guard !seen.contains(key) else {
        return false
      }
      seen.insert(key)
      return true
    }
    return unique.isEmpty ? ["local"] : unique
  }

  // MARK: - Refresh / polling

  func refresh() {
    guard !isRefreshing else {
      refreshQueued = true
      return
    }
    guard !installerRunning else {
      return
    }
    isRefreshing = true
    Task {
      await self.performRefresh()
    }
  }

  private func performRefresh() async {
    defer {
      let shouldRefreshAgain = refreshQueued
      refreshQueued = false
      if !shouldRefreshAgain {
        pendingVerificationSections = Set(pendingVerificationSections.filter { name in
          name == "fleet" && (isRefreshingFleet || fleetRefreshQueued)
        })
      }
      isRefreshing = false
      onStateChange?()
      if shouldRefreshAgain {
        refresh()
      }
    }
    guard let client = client() else {
      lastFailure = .binaryMissing(binaryPathPreference ?? "~/.local/bin/brainctl")
      stale = lastReport != nil
      lastChecked = Date()
      notifyTransitions()
      return
    }
    let outcome = await client.fetchStatus()
    lastChecked = Date()
    if let result = outcome.result {
      lastDurations["status"] = result.duration
    }
    if let report = outcome.report {
      lastReport = reportWithCachedFleet(report)
      lastFailure = nil
      stale = false
      maybeAutoOpenTailscale(for: report)
    } else {
      // Keep the last good report and mark it stale instead of blanking the UI.
      lastFailure = outcome.failure
      stale = lastReport != nil
    }
    refreshFleet()
    notifyTransitions()
  }

  private func reportWithCachedFleet(_ report: StatusReport) -> StatusReport {
    guard let cachedFleetSection else {
      return report
    }
    return report.replacingSection("fleet", with: cachedFleetSection, rawSection: cachedFleetRawSection)
  }

  func refreshFleet() {
    guard !isRefreshingFleet else {
      fleetRefreshQueued = true
      return
    }
    guard !installerRunning else {
      return
    }
    guard client() != nil else {
      return
    }
    isRefreshingFleet = true
    Task {
      await self.performFleetRefresh()
    }
  }

  private func performFleetRefresh() async {
    defer {
      let shouldRefreshAgain = fleetRefreshQueued
      fleetRefreshQueued = false
      if !shouldRefreshAgain {
        pendingVerificationSections.remove("fleet")
      }
      isRefreshingFleet = false
      onStateChange?()
      if shouldRefreshAgain {
        refreshFleet()
      }
    }
    guard let client = client() else {
      return
    }
    let outcome = await client.fetchFleetStatus()
    lastFleetChecked = Date()
    if let result = outcome.result {
      lastDurations["fleet status"] = result.duration
    }
    guard let section = outcome.section else {
      return
    }
    guard shouldAcceptFleetSection(section) else {
      return
    }
    cachedFleetSection = section
    cachedFleetRawSection = outcome.rawSection
    if let report = lastReport {
      lastReport = report.replacingSection("fleet", with: section, rawSection: outcome.rawSection)
    }
    notifyTransitions()
  }

  private func shouldAcceptFleetSection(_ section: StatusSection) -> Bool {
    guard let cachedFleetSection else {
      return true
    }
    let newMachines = fleetMachineNames(in: section)
    let cachedMachines = fleetMachineNames(in: cachedFleetSection)
    guard !cachedMachines.isEmpty else {
      return true
    }
    guard !newMachines.isEmpty else {
      return false
    }
    return newMachines.count >= cachedMachines.count
  }

  private func fleetMachineNames(in section: StatusSection) -> Set<String> {
    let machines = section.data?["machines"]?.arrayValue?.compactMap { $0["name"]?.stringValue } ?? []
    return Set(machines)
  }

  private func notifyTransitions() {
    let snapshot = TransitionDetector.Snapshot.from(report: lastReport, overall: overallState)
    if notificationsEnabled {
      for message in transitionDetector.messages(from: lastSnapshot, to: snapshot, operatorMode: operatorModeEnabled) {
        notifier.notify(title: "Brainstack", body: message)
      }
    }
    lastSnapshot = snapshot
  }

  private func maybeAutoOpenTailscale(for report: StatusReport) {
    guard startTailscaleOnLaunch, !triedTailscaleAutoStart, tailscaleNeedsStart(report) else {
      return
    }
    triedTailscaleAutoStart = true
    openTailscale(title: "Open Tailscale", summaryPrefix: "Auto-start is enabled.")
  }

  func startPolling() {
    restartPolling()
    refresh()
  }

  private func restartPolling() {
    pollTimer?.invalidate()
    pollTimer = Timer.scheduledTimer(withTimeInterval: pollInterval, repeats: true) { [weak self] _ in
      Task { @MainActor [weak self] in
        self?.refresh()
      }
    }
    pollTimer?.tolerance = pollInterval * 0.1
  }

  // MARK: - Actions

  func runAction(
    _ title: String,
    refreshAfter: Bool = true,
    verifying sections: Set<String> = [],
    _ operation: @escaping (BrainctlClient) async -> ActionOutcome
  ) {
    guard busyAction == nil else {
      return
    }
    guard let client = client() else {
      record(ActionOutcome(title: title, succeeded: false, unsupported: false, adminUnavailable: false, summary: "brainctl is missing; choose the binary in Preferences.", output: "", duration: 0))
      return
    }
    busyAction = title
    Task {
      let outcome = await operation(client)
      await MainActor.run {
        self.record(outcome)
        self.busyAction = nil
        if outcome.succeeded {
          self.pendingVerificationSections.formUnion(sections)
        }
        if outcome.adminUnavailable {
          self.adminAvailability = .unavailable
        }
        if refreshAfter {
          self.refresh()
        }
      }
    }
  }

  private func record(_ outcome: ActionOutcome) {
    lastDurations[outcome.title] = outcome.duration
    actionLog.append(outcome)
    if actionLog.count > 20 {
      actionLog.removeFirst(actionLog.count - 20)
    }
  }

  var lastAction: ActionOutcome? {
    actionLog.last
  }

  var lastProposalMergeAction: ActionOutcome? {
    actionLog.reversed().first { action in
      action.title == "Look for Merges"
        || action.title == "Update Control Host"
        || action.recovery?.kind == .updateControlHost
    }
  }

  var controlHostMachineName: String? {
    lastReport?.fleetMachines.first { $0.role.lowercased() == "control" }?.name
  }

  var controlHostDisplayName: String {
    controlHostMachineName ?? "control host"
  }

  func updateFleetMachine(_ machine: String) {
    runAction("Update \(machine)", verifying: ["fleet"]) { client in
      await client.fleetUpdate(machine: machine)
    }
  }

  func updateControlHost() {
    let target = controlHostMachineName ?? "control"
    runAction("Update Control Host", verifying: ["fleet", "control_source", "curator", "proposals"]) { client in
      await client.fleetUpdate(machine: target, title: "Update Control Host")
    }
  }

  // MARK: - Uploads

  func loadUploads(machine: String) {
    guard let client = client() else {
      uploadsError = "brainctl is missing; choose the binary in Preferences."
      uploads = []
      return
    }
    guard !isLoadingUploads else {
      return
    }
    isLoadingUploads = true
    Task {
      let (parsed, outcome) = await client.fetchUploads(machine: machine)
      await MainActor.run {
        self.isLoadingUploads = false
        self.lastDurations["uploads list"] = outcome.duration
        if let parsed {
          self.uploads = parsed
          self.uploadsError = nil
        } else {
          self.uploads = []
          self.uploadsError = outcome.succeeded ? "Could not parse uploads." : outcome.summary
          self.record(outcome)
        }
      }
    }
  }

  func uploadFiles(_ urls: [URL], machine: String) {
    guard !urls.isEmpty else {
      return
    }
    guard let client = client() else {
      uploadsError = "brainctl is missing; choose the binary in Preferences."
      return
    }
    guard !isUploading else {
      uploadsError = "An upload is already running."
      return
    }
    isUploading = true
    uploadsError = nil
    Task {
      var failed: ActionOutcome?
      var uploaded = 0
      var totalDuration: TimeInterval = 0
      for url in urls {
        let outcome = await client.uploadFile(machine: machine, path: url.path)
        totalDuration += outcome.duration
        if outcome.succeeded {
          uploaded += 1
        } else {
          failed = outcome
          break
        }
      }
      await MainActor.run {
        self.isUploading = false
        if let failed {
          self.record(failed)
          self.uploadsError = failed.summary
        } else {
          self.record(ActionOutcome(
            title: "Upload Files",
            succeeded: true,
            unsupported: false,
            adminUnavailable: false,
            summary: "Uploaded \(uploaded) file\(uploaded == 1 ? "" : "s") to \(machine).",
            output: "",
            duration: totalDuration
          ))
          self.uploadsError = nil
        }
        self.loadUploads(machine: machine)
      }
    }
  }

  func deleteUpload(_ upload: UploadSummary) {
    guard let client = client() else {
      uploadsError = "brainctl is missing; choose the binary in Preferences."
      return
    }
    guard !isUploading else {
      uploadsError = "An upload operation is already running."
      return
    }
    isUploading = true
    Task {
      let outcome = await client.deleteUpload(machine: upload.machine, id: upload.id)
      await MainActor.run {
        self.isUploading = false
        self.record(outcome)
        self.uploadsError = outcome.succeeded ? nil : outcome.summary
        self.loadUploads(machine: upload.machine)
      }
    }
  }

  func installOrRepair(invite: String?) {
    guard busyAction == nil, !installerRunning else {
      return
    }
    let trimmed = invite?.trimmingCharacters(in: .whitespacesAndNewlines)
    let title = trimmed?.isEmpty == false ? "Set Up Brainstack" : "Repair Brainstack"
    busyAction = title
    installerRunning = true
    Task {
      let outcome = await BrainstackMenuInstaller.installAndRepair(
        configPath: self.configPath,
        invite: trimmed
      )
      await MainActor.run {
        self.record(outcome)
        self.installerRunning = false
        self.busyAction = nil
        if outcome.succeeded {
          self.binaryPathPreference = BrainstackMenuInstaller.defaultInstallPath()
          self.pendingVerificationSections.formUnion(["daemon", "hooks", "skills", "shared_brain", "outbox"])
        }
        self.refresh()
      }
    }
  }

  var bundledBrainctlPath: String? {
    BrainstackMenuInstaller.bundledBrainctlPath()
  }

  var defaultInstallPath: String {
    BrainstackMenuInstaller.defaultInstallPath()
  }

  // MARK: - Operator mode

  func loadProposals() {
    guard operatorModeEnabled, let client = client() else {
      return
    }
    guard !isLoadingProposals else {
      return
    }
    isLoadingProposals = true
    Task {
      let (parsed, outcome) = await client.fetchOpenProposals()
      await MainActor.run {
        self.isLoadingProposals = false
        self.lastDurations["proposals list"] = outcome.duration
        if outcome.unsupported {
          self.proposalsError = "Proposals are unsupported by the installed brainctl/control host."
          self.proposals = []
          return
        }
        if let parsed {
          self.proposals = parsed
          self.proposalsError = nil
        } else {
          self.proposals = []
          self.proposalsError = outcome.succeeded ? "Could not parse proposal list." : outcome.summary
        }
      }
    }
  }

  func decideProposal(_ proposal: ProposalSummary, action: String) {
    guard busyAction == nil, let client = client() else {
      return
    }
    let actionLabel = action == "apply" ? "Accept" : action.capitalized
    busyAction = "\(actionLabel) \(proposal.id)"
    Task {
      let outcome = await client.proposalDecision(id: proposal.id, action: action)
      await MainActor.run {
        self.record(outcome)
        self.busyAction = nil
        if outcome.adminUnavailable {
          self.adminAvailability = .unavailable
        } else if outcome.succeeded {
          self.adminAvailability = .available
        } else {
          self.adminAvailability = .unavailable
        }
        // The decision changed proposal state; the cached detail is stale.
        self.proposalDetails[proposal.id] = nil
        // Applying or rejecting changes proposal/wiki state; refresh both surfaces.
        self.refresh()
        self.loadProposals()
      }
    }
  }

  func mergeProposalGroup(_ proposal: ProposalSummary) {
    guard busyAction == nil, let client = client(), let groupKey = proposal.clusterKey, !groupKey.isEmpty else {
      return
    }
    busyAction = "Merge \(groupKey)"
    Task {
      let outcome = await client.proposalMergeGroup(groupKey: groupKey)
      await MainActor.run {
        self.record(outcome)
        self.busyAction = nil
        if outcome.adminUnavailable {
          self.adminAvailability = .unavailable
        } else if outcome.succeeded {
          self.adminAvailability = .available
        }
        self.proposalDetails.removeAll()
        self.refresh()
        self.loadProposals()
      }
    }
  }

  func lookForProposalMerges() {
    guard busyAction == nil, let client = client() else {
      return
    }
    busyAction = "Looking for merges"
    Task {
      let outcome = await client.proposalAutoMerge()
      await MainActor.run {
        self.record(outcome)
        self.busyAction = nil
        if outcome.adminUnavailable {
          self.adminAvailability = .unavailable
        } else if outcome.succeeded {
          self.adminAvailability = .available
        }
        self.proposalDetails.removeAll()
        self.refresh()
        self.loadProposals()
      }
    }
  }

  /// Fetch the full proposal body/diff for the console detail pane (cached per id).
  func loadProposalDetail(_ id: String, force: Bool = false) {
    guard operatorModeEnabled, let client = client() else {
      return
    }
    if !force, proposalDetails[id] != nil {
      return
    }
    guard loadingDetailId != id else {
      return
    }
    loadingDetailId = id
    Task {
      let (detail, outcome) = await client.fetchProposalDetail(id: id)
      await MainActor.run {
        if self.loadingDetailId == id {
          self.loadingDetailId = nil
        }
        self.lastDurations["proposal show"] = outcome.duration
        if let detail {
          self.proposalDetails[id] = detail
          self.detailErrors[id] = nil
        } else {
          self.detailErrors[id] = outcome.succeeded ? "Could not parse proposal detail." : outcome.summary
        }
      }
    }
  }

  // MARK: - Open / copy helpers

  func openWiki() {
    guard let url = brainURL() else {
      record(ActionOutcome(title: "Open Wiki", succeeded: false, unsupported: false, adminUnavailable: false, summary: "Brain API base URL is unavailable in the last status.", output: "", duration: 0))
      return
    }
    NSWorkspace.shared.open(url)
  }

  func openCurationPage() {
    guard let url = brainURL(fragment: "curation") else {
      record(ActionOutcome(title: "Open Curation Page", succeeded: false, unsupported: false, adminUnavailable: false, summary: "Brain API base URL is unavailable in the last status.", output: "", duration: 0))
      return
    }
    NSWorkspace.shared.open(url)
  }

  private func brainURL(fragment: String? = nil) -> URL? {
    guard let base = lastReport?.sections["brain_api"]?.data?["base_url"]?.stringValue,
          var components = URLComponents(string: base) else {
      return nil
    }
    components.fragment = fragment
    return components.url
  }

  func openSharedBrainFolder() {
    openPath(lastReport?.sections["config"]?.data?["paths"]?["client_local_path"]?.stringValue
      ?? lastReport?.sections["config"]?.data?["paths"]?["shared_brain_root"]?.stringValue, title: "Open Shared Brain Folder")
  }

  func openConfigFolder() {
    let fallback = (configPath as NSString).expandingTildeInPath
    openPath(lastReport?.sections["config"]?.data?["paths"]?["config_root"]?.stringValue ?? (fallback as NSString).deletingLastPathComponent, title: "Open Config Folder")
  }

  private func openPath(_ path: String?, title: String) {
    guard let path, FileManager.default.fileExists(atPath: (path as NSString).expandingTildeInPath) else {
      record(ActionOutcome(title: title, succeeded: false, unsupported: false, adminUnavailable: false, summary: "Path is unavailable or does not exist.", output: path ?? "", duration: 0))
      return
    }
    NSWorkspace.shared.open(URL(fileURLWithPath: (path as NSString).expandingTildeInPath))
  }

  func openTailscale(title: String = "Open Tailscale", summaryPrefix: String? = nil) {
    let appPath = "/Applications/Tailscale.app"
    let workspace = NSWorkspace.shared
    let appURL = workspace.urlForApplication(withBundleIdentifier: "io.tailscale.ipn.macos")
      ?? (FileManager.default.fileExists(atPath: appPath) ? URL(fileURLWithPath: appPath) : nil)
    guard let appURL else {
      let downloadURL = URL(string: "https://tailscale.com/download/mac")!
      let opened = workspace.open(downloadURL)
      record(ActionOutcome(
        title: title,
        succeeded: opened,
        unsupported: false,
        adminUnavailable: false,
        summary: opened ? "Tailscale.app was not found; opened the download page." : "Tailscale.app was not found and the download page could not be opened.",
        output: downloadURL.absoluteString,
        duration: 0
      ))
      return
    }
    let opened = workspace.open(appURL)
    let prefix = summaryPrefix.map { "\($0) " } ?? ""
    record(ActionOutcome(
      title: title,
      succeeded: opened,
      unsupported: false,
      adminUnavailable: false,
      summary: opened ? "\(prefix)Tailscale opened." : "Could not open Tailscale.",
      output: appURL.path,
      duration: 0
    ))
    if opened {
      pendingVerificationSections.insert("tailscale")
      DispatchQueue.main.asyncAfter(deadline: .now() + 2.0) { [weak self] in
        self?.refresh()
      }
    }
  }

  func copyDiagnostics() {
    let text = Diagnostics.bundle(
      appVersion: AppVersion.current,
      binaryPath: resolvedBinaryPath ?? "(unresolved)",
      configPath: configPath,
      lastReport: lastReport,
      stale: stale,
      lastFailure: lastFailure,
      lastDurations: lastDurations,
      lastActionOutputs: actionLog.suffix(5).map { "\($0.title): \($0.summary)\n\($0.output)" }
    )
    let pasteboard = NSPasteboard.general
    pasteboard.clearContents()
    pasteboard.setString(text, forType: .string)
    record(ActionOutcome(title: "Copy Diagnostics", succeeded: true, unsupported: false, adminUnavailable: false, summary: "Redacted diagnostics copied to clipboard.", output: "", duration: 0))
  }

  func copySetupCommand() {
    let pasteboard = NSPasteboard.general
    pasteboard.clearContents()
    pasteboard.setString("curl -fsSL https://github.com/Caimeo-com/brainstack/releases/download/v\(AppVersion.current)/install.sh | sh", forType: .string)
  }

}

enum AppVersion {
  /// Bundled runs report the Info.plist version; bare `swift run` falls back.
  static let current = Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "0.1.0"
}
