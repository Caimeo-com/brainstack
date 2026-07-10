import AppKit
import Combine
import Foundation
import BrainstackMenuCore

struct ProposalDecisionNotice: Equatable {
  let action: String
  let attempted: Int
  let succeeded: Int
  let failed: Int

  var message: String {
    let verb = action == "apply" ? "Applied" : "Rejected"
    if failed == 0 {
      return "\(verb) \(succeeded) proposal\(succeeded == 1 ? "" : "s")."
    }
    return "\(verb) \(succeeded) of \(attempted). \(failed) unresolved proposal\(failed == 1 ? " will" : "s will") return when you refresh."
  }
}

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
  @Published private(set) var reviewedProposals: [ProposalSummary] = []
  @Published private(set) var proposalsError: String?
  @Published private(set) var reviewedProposalsError: String?
  @Published private(set) var isLoadingProposals = false
  @Published private(set) var isLoadingReviewedProposals = false
  @Published private(set) var pendingProposalDecisionIds = Set<String>()
  @Published private(set) var pendingProposalApplyTargets = Set<String>()
  @Published private(set) var proposalDecisionNotice: ProposalDecisionNotice?
  @Published private(set) var uploads: [UploadSummary] = []
  @Published private(set) var uploadsError: String?
  @Published private(set) var isLoadingUploads = false
  @Published private(set) var isUploading = false
  @Published private(set) var contextPacks: [ContextPackSummary] = []
  @Published private(set) var contextPacksError: String?
  @Published private(set) var isLoadingContextPacks = false
  @Published private(set) var isSyncingContextPack = false
  @Published private(set) var skillImportPlan: SkillImportPlan?
  @Published private(set) var skillImportSourcePath = ""
  @Published private(set) var skillImportError: String?
  @Published private(set) var isLoadingSkillImportPlan = false
  @Published private(set) var isImportingSkills = false
  @Published private(set) var adminAvailability: AdminAvailability = .unknown
  @Published private(set) var proposalDetails: [String: ProposalDetail] = [:]
  @Published private(set) var loadingDetailIds = Set<String>()
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
  private var proposalDecisionState = OptimisticProposalDecisionState()
  private var proposalDecisionTask: Task<Void, Never>?
  private var cancelledProposalDecisionIds = Set<String>()
  private var proposalDetailGenerations: [String: Int] = [:]

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
    guard busyAction == nil, pendingProposalDecisionIds.isEmpty else {
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

  // MARK: - Folder packs

  func loadContextPacks(machine: String) {
    guard let client = client() else {
      contextPacksError = "brainctl is missing; choose the binary in Preferences."
      contextPacks = []
      return
    }
    guard !isLoadingContextPacks else {
      return
    }
    isLoadingContextPacks = true
    Task {
      let (parsed, outcome) = await client.fetchContextPacks(machine: machine)
      await MainActor.run {
        self.isLoadingContextPacks = false
        self.lastDurations["context-packs list"] = outcome.duration
        if let parsed {
          self.contextPacks = parsed
          self.contextPacksError = nil
        } else {
          self.contextPacks = []
          self.contextPacksError = outcome.succeeded ? "Could not parse folder packs." : outcome.summary
          self.record(outcome)
        }
      }
    }
  }

  func addContextPack(folder: URL, machine: String, name: String) {
    guard let client = client() else {
      contextPacksError = "brainctl is missing; choose the binary in Preferences."
      return
    }
    let packName = name.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !packName.isEmpty else {
      contextPacksError = "Folder pack needs a name."
      return
    }
    guard !isSyncingContextPack else {
      contextPacksError = "A folder pack operation is already running."
      return
    }
    isSyncingContextPack = true
    contextPacksError = nil
    Task {
      let (preflight, preflightOutcome) = await client.preflightContextPack(machine: machine, name: packName, path: folder.path)
      let shouldContinue = await MainActor.run {
        self.record(preflightOutcome)
        if !preflightOutcome.succeeded {
          self.isSyncingContextPack = false
          self.contextPacksError = preflightOutcome.summary
          return false
        }
        guard let preflight else {
          self.isSyncingContextPack = false
          self.contextPacksError = "Could not parse folder pack preflight."
          return false
        }
        return Confirm.ask(
          title: "Sync Folder Pack",
          message: self.contextPackPreflightMessage(preflight)
        )
      }
      guard shouldContinue else {
        await MainActor.run {
          self.isSyncingContextPack = false
        }
        return
      }
      let outcome = await client.putContextPack(machine: machine, name: packName, path: folder.path)
      await MainActor.run {
        self.isSyncingContextPack = false
        self.record(outcome)
        self.contextPacksError = outcome.succeeded ? nil : outcome.summary
        self.loadContextPacks(machine: machine)
      }
    }
  }

  func syncContextPack(_ pack: ContextPackSummary) {
    guard let client = client() else {
      contextPacksError = "brainctl is missing; choose the binary in Preferences."
      return
    }
    guard !isSyncingContextPack else {
      contextPacksError = "A folder pack operation is already running."
      return
    }
    isSyncingContextPack = true
    contextPacksError = nil
    Task {
      let outcome = await client.syncContextPack(machine: pack.machine, name: pack.safeName)
      await MainActor.run {
        self.isSyncingContextPack = false
        self.record(outcome)
        self.contextPacksError = outcome.succeeded ? nil : self.contextPackFriendlyError(outcome, pack: pack)
        self.loadContextPacks(machine: pack.machine)
      }
    }
  }

  func attachContextPack(_ pack: ContextPackSummary, context: String) {
    let slug = context.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !slug.isEmpty else {
      contextPacksError = "Enter a context slug to attach this pack."
      return
    }
    guard let client = client() else {
      contextPacksError = "brainctl is missing; choose the binary in Preferences."
      return
    }
    guard !isSyncingContextPack else {
      contextPacksError = "A folder pack operation is already running."
      return
    }
    isSyncingContextPack = true
    contextPacksError = nil
    Task {
      let outcome = await client.attachContextPack(context: slug, machine: pack.machine, name: pack.safeName)
      await MainActor.run {
        self.isSyncingContextPack = false
        self.record(outcome)
        self.contextPacksError = outcome.succeeded ? nil : outcome.summary
      }
    }
  }

  func detachContextPack(_ pack: ContextPackSummary, context: String) {
    let slug = context.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !slug.isEmpty else {
      contextPacksError = "Enter a context slug to detach this pack."
      return
    }
    guard let client = client() else {
      contextPacksError = "brainctl is missing; choose the binary in Preferences."
      return
    }
    guard !isSyncingContextPack else {
      contextPacksError = "A folder pack operation is already running."
      return
    }
    isSyncingContextPack = true
    contextPacksError = nil
    Task {
      let outcome = await client.detachContextPack(context: slug, machine: pack.machine, name: pack.safeName)
      await MainActor.run {
        self.isSyncingContextPack = false
        self.record(outcome)
        self.contextPacksError = outcome.succeeded ? nil : outcome.summary
      }
    }
  }

  func deleteContextPack(_ pack: ContextPackSummary) {
    guard let client = client() else {
      contextPacksError = "brainctl is missing; choose the binary in Preferences."
      return
    }
    guard !isSyncingContextPack else {
      contextPacksError = "A folder pack operation is already running."
      return
    }
    isSyncingContextPack = true
    Task {
      let outcome = await client.deleteContextPack(machine: pack.machine, name: pack.safeName)
      await MainActor.run {
        self.isSyncingContextPack = false
        self.record(outcome)
        self.contextPacksError = outcome.succeeded ? nil : outcome.summary
        self.loadContextPacks(machine: pack.machine)
      }
    }
  }

  func gcContextPacks(machine: String, delete: Bool) {
    guard let client = client() else {
      contextPacksError = "brainctl is missing; choose the binary in Preferences."
      return
    }
    guard !isSyncingContextPack else {
      contextPacksError = "A folder pack operation is already running."
      return
    }
    isSyncingContextPack = true
    contextPacksError = nil
    Task {
      let outcome = await client.gcContextPacks(machine: machine, delete: delete)
      await MainActor.run {
        self.isSyncingContextPack = false
        self.record(outcome)
        self.contextPacksError = outcome.succeeded ? nil : outcome.summary
        self.loadContextPacks(machine: machine)
      }
    }
  }

  private func contextPackPreflightMessage(_ pack: ContextPackSummary) -> String {
    var lines = [
      "Sync \(pack.displayName) to \(pack.machine)?",
      "",
      "Files: \(pack.fileCount)",
      "Size: \(pack.formattedSize)",
      "Destination free: \(pack.formattedFreeSpace)",
      "Source: \(pack.sourceMachine.isEmpty ? "this Mac" : pack.sourceMachine)",
      "",
      "Brainstack will copy the included files with rsync into a stable current/ folder. Prompts receive only the folder path and metadata."
    ]
    if !pack.warnings.isEmpty {
      lines.append("")
      lines.append("Warnings:")
      lines.append(contentsOf: pack.warnings.map { "- \($0)" })
    }
    return lines.joined(separator: "\n")
  }

  private func contextPackFriendlyError(_ outcome: ActionOutcome, pack: ContextPackSummary? = nil) -> String {
    let combined = "\(outcome.summary)\n\(outcome.output)"
    if combined.contains("No local source definition exists") {
      let packName = pack?.displayName ?? "this pack"
      let source = pack?.sourceMachine.isEmpty == false ? pack!.sourceMachine : "the source machine"
      let synced = pack?.refreshedAt.isEmpty == false ? " Last synced: \(pack!.refreshedAt)." : ""
      return "\(packName) is owned by \(source), so this Mac cannot refresh it directly.\(synced) Use the last synced copy, or run Sync Now on the source machine."
    }
    return outcome.summary
  }

  // MARK: - Skills import

  func loadSkillImportPlan(sourcePath: String?) {
    guard let client = client() else {
      skillImportError = "brainctl is missing; choose the binary in Preferences."
      skillImportPlan = nil
      return
    }
    guard !isLoadingSkillImportPlan else {
      return
    }
    let trimmed = sourcePath?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
    skillImportSourcePath = trimmed
    isLoadingSkillImportPlan = true
    skillImportError = nil
    Task {
      let (plan, outcome) = await client.fetchSkillImportPlan(sourcePath: trimmed.isEmpty ? nil : trimmed)
      await MainActor.run {
        self.isLoadingSkillImportPlan = false
        self.lastDurations["skills import plan"] = outcome.duration
        if let plan {
          self.skillImportPlan = plan
          self.skillImportError = nil
        } else {
          self.skillImportPlan = nil
          self.skillImportError = outcome.succeeded ? "Could not parse the skill import plan." : outcome.summary
          self.record(outcome)
        }
      }
    }
  }

  func importSkills(indexes: [Int]) {
    guard !indexes.isEmpty else {
      skillImportError = "Select at least one skill to import."
      return
    }
    guard let client = client() else {
      skillImportError = "brainctl is missing; choose the binary in Preferences."
      return
    }
    guard !isImportingSkills else {
      skillImportError = "A skill import is already running."
      return
    }
    let source = skillImportSourcePath.trimmingCharacters(in: .whitespacesAndNewlines)
    isImportingSkills = true
    skillImportError = nil
    Task {
      let outcome = await client.importSelectedSkills(sourcePath: source.isEmpty ? nil : source, indexes: indexes)
      await MainActor.run {
        self.isImportingSkills = false
        self.record(outcome)
        if outcome.succeeded {
          self.pendingVerificationSections.insert("skills")
          self.skillImportError = nil
          self.loadSkillImportPlan(sourcePath: source.isEmpty ? nil : source)
          self.refresh()
        } else {
          self.skillImportError = outcome.summary
        }
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
    // A reload is the explicit reconciliation point for failed optimistic
    // decisions. Successful decisions remain absent at the source of truth.
    proposalDecisionState.beginReload()
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
          self.proposals = self.proposalDecisionState.filterVisible(parsed)
          self.proposalsError = nil
        } else {
          self.proposals = []
          self.proposalsError = outcome.succeeded ? "Could not parse proposal list." : outcome.summary
        }
      }
    }
  }

  func loadReviewedProposals() {
    guard operatorModeEnabled, let client = client(), !isLoadingReviewedProposals else {
      return
    }
    isLoadingReviewedProposals = true
    Task {
      let (parsed, outcome) = await client.fetchProposals(status: "applied,rejected,superseded")
      await MainActor.run {
        self.isLoadingReviewedProposals = false
        self.lastDurations["reviewed proposals list"] = outcome.duration
        if let parsed {
          self.reviewedProposals = parsed
          self.reviewedProposalsError = nil
        } else {
          self.reviewedProposalsError = outcome.succeeded ? "Could not parse reviewed proposal list." : outcome.summary
        }
      }
    }
  }

  func decideProposal(_ proposal: ProposalSummary, action: String) {
    decideProposals([proposal], action: action)
  }

  func decideProposals(_ selected: [ProposalSummary], action: String) {
    guard ["apply", "reject"].contains(action), busyAction == nil, let client = client() else {
      return
    }
    let unique = Dictionary(selected.map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
      .values
      .filter { $0.isOpen && !pendingProposalDecisionIds.contains($0.id) }
      .sorted { $0.id < $1.id }
    guard !unique.isEmpty else {
      return
    }
    if action == "apply" {
      guard ProposalBatchValidation.duplicateTargets(in: Array(unique)).isEmpty else {
        return
      }
    }
    let selectedTargets = Set(unique.compactMap(\.targetPage))
    guard pendingProposalApplyTargets.isDisjoint(with: selectedTargets) else {
      return
    }
    let applyTargets = action == "apply" ? selectedTargets : []
    let ids = Set(unique.map(\.id))
    proposalDecisionNotice = nil
    proposalDecisionState.start(ids: ids)
    pendingProposalDecisionIds = proposalDecisionState.pendingIds
    pendingProposalApplyTargets.formUnion(applyTargets)
    proposals.removeAll { ids.contains($0.id) }
    invalidateProposalDetails(ids)
    let previousDecisionTask = proposalDecisionTask
    proposalDecisionTask = Task {
      await previousDecisionTask?.value
      var executions: [ProposalDecisionExecution] = []
      for proposal in unique {
        if self.cancelledProposalDecisionIds.contains(proposal.id) {
          executions.append(ProposalDecisionExecution(outcome: ActionOutcome(
            title: action == "apply" ? "Apply Proposal" : "Reject Proposal",
            succeeded: true,
            unsupported: false,
            adminUnavailable: false,
            summary: "Proposal was already superseded by an earlier decision.",
            output: "",
            duration: 0
          )))
        } else {
          executions.append(await client.proposalDecision(id: proposal.id, action: action))
        }
      }
      await MainActor.run {
        let outcomes = executions.map(\.outcome)
        let succeededIds = Set(zip(unique, outcomes).compactMap { pair in pair.1.succeeded ? pair.0.id : nil })
        let failedIds = ids.subtracting(succeededIds)
        self.proposalDecisionState.complete(succeededIds: succeededIds, failedIds: failedIds)
        self.pendingProposalDecisionIds = self.proposalDecisionState.pendingIds
        self.pendingProposalApplyTargets.subtract(applyTargets)
        let authoritativeSupersededIds = Set(executions.flatMap(\.supersededIds))
        if !authoritativeSupersededIds.isEmpty {
          self.cancelledProposalDecisionIds.formUnion(authoritativeSupersededIds)
          self.proposalDecisionState.start(ids: authoritativeSupersededIds)
          self.proposalDecisionState.complete(succeededIds: authoritativeSupersededIds, failedIds: [])
          self.proposals.removeAll { authoritativeSupersededIds.contains($0.id) }
          self.invalidateProposalDetails(authoritativeSupersededIds)
        }
        let succeeded = outcomes.filter(\.succeeded).count
        let failed = outcomes.count - succeeded
        let duration = outcomes.reduce(0) { $0 + $1.duration }
        let output = outcomes.filter { !$0.succeeded }.map(\.output).filter { !$0.isEmpty }.joined(separator: "\n\n")
        let aggregate = ActionOutcome(
          title: action == "apply" ? "Apply Proposals" : "Reject Proposals",
          succeeded: failed == 0,
          unsupported: outcomes.contains(where: \.unsupported),
          adminUnavailable: outcomes.contains(where: \.adminUnavailable),
          summary: failed == 0
            ? "\(action == "apply" ? "Applied" : "Rejected") \(succeeded) proposal\(succeeded == 1 ? "" : "s")."
            : "\(failed) of \(outcomes.count) proposal decisions failed. Refresh to restore unresolved proposals.",
          output: output,
          duration: duration
        )
        self.record(aggregate)
        self.proposalDecisionNotice = ProposalDecisionNotice(
          action: action,
          attempted: outcomes.count,
          succeeded: succeeded,
          failed: failed
        )
        if aggregate.adminUnavailable {
          self.adminAvailability = .unavailable
        } else {
          self.adminAvailability = .available
        }
        // The queue was updated optimistically. Do not reload it here: if a
        // destination rejects a decision, the proposal returns on explicit reload.
        self.refresh()
      }
    }
  }

  func clearProposalDecisionNotice() {
    proposalDecisionNotice = nil
  }

  func requestProposalWork(_ proposal: ProposalSummary, reason: String) {
    guard busyAction == nil, pendingProposalDecisionIds.isEmpty, let client = client() else { return }
    let feedback = reason.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !feedback.isEmpty else { return }
    proposalDecisionNotice = nil
    busyAction = "Sending proposal back"
    Task {
      let outcome = await client.proposalNeedsWork(id: proposal.id, reason: feedback)
      await MainActor.run {
        self.record(outcome)
        self.busyAction = nil
        if outcome.adminUnavailable {
          self.adminAvailability = .unavailable
        } else if outcome.succeeded {
          self.adminAvailability = .available
          self.invalidateProposalDetails([proposal.id])
          self.loadProposals()
        }
        self.refresh()
      }
    }
  }

  func mergeProposalGroup(_ proposal: ProposalSummary) {
    guard busyAction == nil, pendingProposalDecisionIds.isEmpty, let client = client(), let groupKey = proposal.clusterKey, !groupKey.isEmpty else {
      return
    }
    proposalDecisionNotice = nil
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
        self.invalidateAllProposalDetails()
        self.refresh()
        self.loadProposals()
      }
    }
  }

  func mergeSelectedProposals(_ proposals: [ProposalSummary]) {
    let selected = proposals.filter(\.isOpen)
    let groups = Set(selected.compactMap(\.clusterKey))
    guard selected.count > 1, groups.count == 1, let groupKey = groups.first, !groupKey.isEmpty,
          busyAction == nil, pendingProposalDecisionIds.isEmpty, let client = client() else { return }
    proposalDecisionNotice = nil
    busyAction = "Merging selected proposals"
    Task {
      let outcome = await client.proposalMergeSelection(groupKey: groupKey, ids: selected.map(\.id))
      await MainActor.run {
        self.record(outcome)
        self.busyAction = nil
        if outcome.adminUnavailable {
          self.adminAvailability = .unavailable
        } else if outcome.succeeded {
          self.adminAvailability = .available
        }
        self.invalidateAllProposalDetails()
        self.refresh()
        self.loadProposals()
      }
    }
  }

  func lookForProposalMerges() {
    guard busyAction == nil, pendingProposalDecisionIds.isEmpty, let client = client() else {
      return
    }
    proposalDecisionNotice = nil
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
        self.invalidateAllProposalDetails()
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
    guard !loadingDetailIds.contains(id) else {
      return
    }
    let generation = proposalDetailGenerations[id, default: 0] + 1
    proposalDetailGenerations[id] = generation
    loadingDetailIds.insert(id)
    detailErrors[id] = nil
    Task {
      let (detail, outcome) = await client.fetchProposalDetail(id: id)
      await MainActor.run {
        guard self.proposalDetailGenerations[id] == generation else { return }
        self.loadingDetailIds.remove(id)
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

  func loadProposalDetails(_ proposals: [ProposalSummary]) {
    guard operatorModeEnabled, let client = client() else { return }
    let ids = Array(Dictionary(
      proposals.prefix(ProposalBatchValidation.maximumBatchSize).map { ($0.id, $0.id) },
      uniquingKeysWith: { first, _ in first }
    ).keys).sorted()
    var requests: [(id: String, generation: Int)] = []
    for id in ids where proposalDetails[id] == nil && !loadingDetailIds.contains(id) {
      let generation = proposalDetailGenerations[id, default: 0] + 1
      proposalDetailGenerations[id] = generation
      loadingDetailIds.insert(id)
      detailErrors[id] = nil
      requests.append((id, generation))
    }
    guard !requests.isEmpty else { return }
    Task {
      // Keep remote admin reads bounded. The queue remains responsive and shows
      // aggregate loading progress while each decision packet arrives.
      for request in requests {
        let (detail, outcome) = await client.fetchProposalDetail(id: request.id)
        await MainActor.run {
          guard self.proposalDetailGenerations[request.id] == request.generation else { return }
          self.loadingDetailIds.remove(request.id)
          self.lastDurations["proposal show"] = outcome.duration
          if let detail {
            self.proposalDetails[request.id] = detail
            self.detailErrors[request.id] = nil
          } else {
            self.detailErrors[request.id] = outcome.succeeded ? "Could not parse proposal detail." : outcome.summary
          }
        }
      }
    }
  }

  private func invalidateProposalDetails(_ ids: Set<String>) {
    for id in ids {
      proposalDetailGenerations[id, default: 0] += 1
      loadingDetailIds.remove(id)
      proposalDetails[id] = nil
      detailErrors[id] = nil
    }
  }

  private func invalidateAllProposalDetails() {
    let ids = Set(proposalDetailGenerations.keys)
      .union(loadingDetailIds)
      .union(proposalDetails.keys)
      .union(detailErrors.keys)
    invalidateProposalDetails(ids)
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
