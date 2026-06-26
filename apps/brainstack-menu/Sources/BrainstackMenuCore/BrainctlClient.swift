import Foundation

/// Why a refresh produced no usable report. Distinguishes setup problems (gray) from
/// transient command problems (stale-but-keep-last-good).
public enum StatusFailure: Equatable, Sendable {
  case binaryMissing(String)
  /// The binary exists but predates the `status` command: update guidance, not a generic failure.
  case unsupportedBinary(String)
  case timedOut
  case commandFailed(exitCode: Int32?, stderr: String)
  case parseFailed(detail: String, stderr: String)
}

public struct StatusFetchOutcome: Sendable {
  public let report: StatusReport?
  public let failure: StatusFailure?
  public let result: CommandResult?
}

public struct FleetStatusFetchOutcome: Sendable {
  public let section: StatusSection?
  public let rawSection: JSONValue?
  public let failure: StatusFailure?
  public let result: CommandResult?
}

public struct ActionOutcome: Sendable {
  public let title: String
  public let succeeded: Bool
  public let unsupported: Bool
  public let adminUnavailable: Bool
  public let summary: String
  public let output: String
  public let duration: TimeInterval
  public let recovery: ActionRecovery?

  public init(
    title: String,
    succeeded: Bool,
    unsupported: Bool,
    adminUnavailable: Bool,
    summary: String,
    output: String,
    duration: TimeInterval,
    recovery: ActionRecovery? = nil
  ) {
    self.title = title
    self.succeeded = succeeded
    self.unsupported = unsupported
    self.adminUnavailable = adminUnavailable
    self.summary = summary
    self.output = output
    self.duration = duration
    self.recovery = recovery
  }
}

public enum ActionRecoveryKind: String, Equatable, Sendable {
  case updateControlHost
}

public struct ActionRecovery: Equatable, Sendable {
  public let kind: ActionRecoveryKind
  public let title: String
  public let message: String
  public let primaryButtonTitle: String
  public let technicalHint: String?

  public init(kind: ActionRecoveryKind, title: String, message: String, primaryButtonTitle: String, technicalHint: String? = nil) {
    self.kind = kind
    self.title = title
    self.message = message
    self.primaryButtonTitle = primaryButtonTitle
    self.technicalHint = technicalHint
  }
}

/// All Brainstack access goes through named `brainctl` commands; the app never edits
/// files or calls HTTP endpoints directly.
public struct BrainctlClient: Sendable {
  public let binaryPath: String
  public let configPath: String
  public let statusTimeout: TimeInterval
  public let actionTimeout: TimeInterval

  /// The status command probes sections sequentially with a per-section budget, so
  /// the hard process timeout must comfortably exceed sections x budget.
  public init(binaryPath: String, configPath: String, statusTimeout: TimeInterval = 15.0, actionTimeout: TimeInterval = 120.0) {
    self.binaryPath = binaryPath
    self.configPath = configPath
    self.statusTimeout = statusTimeout
    self.actionTimeout = actionTimeout
  }

  /// Resolve the brainctl binary: explicit preference, then the default install
  /// location, then a controlled PATH (never the GUI inherited PATH).
  public static func resolveBinary(preferred: String?, fileManager: FileManager = .default) -> String? {
    let home = fileManager.homeDirectoryForCurrentUser.path
    var candidates: [String] = []
    if let preferred, !preferred.isEmpty {
      candidates.append((preferred as NSString).expandingTildeInPath)
    }
    candidates.append("\(home)/.local/bin/brainctl")
    for dir in ["/opt/homebrew/bin", "/usr/local/bin", "\(home)/.bun/bin"] {
      candidates.append("\(dir)/brainctl")
    }
    return candidates.first { fileManager.isExecutableFile(atPath: $0) }
  }

  public static func defaultConfigPath(fileManager: FileManager = .default) -> String {
    "\(fileManager.homeDirectoryForCurrentUser.path)/.config/brainstack/brainstack.yaml"
  }

  // MARK: - Status

  public func fetchStatus() async -> StatusFetchOutcome {
    let perSectionBudgetMs = 1500
    let result = await CommandRunner.run(
      executable: binaryPath,
      arguments: ["status", "--json", "--skip-fleet", "--config", configPath, "--timeout-ms", String(perSectionBudgetMs)],
      timeout: statusTimeout
    )
    if result.launchFailure != nil {
      return StatusFetchOutcome(report: nil, failure: .binaryMissing(binaryPath), result: result)
    }
    if result.timedOut {
      return StatusFetchOutcome(report: nil, failure: .timedOut, result: result)
    }
    if Self.looksUnsupported(result) {
      return StatusFetchOutcome(report: nil, failure: .unsupportedBinary(binaryPath), result: result)
    }
    // `status` prints the report on stdout even for degraded installs; only treat the
    // run as failed when there is no parseable JSON at all.
    if let data = result.stdout.data(using: .utf8), !result.stdout.isEmpty {
      do {
        let report = try StatusReport.parse(data: data)
        return StatusFetchOutcome(report: report, failure: nil, result: result)
      } catch {
        if result.exitCode != 0 {
          return StatusFetchOutcome(report: nil, failure: .commandFailed(exitCode: result.exitCode, stderr: result.stderr), result: result)
        }
        return StatusFetchOutcome(report: nil, failure: .parseFailed(detail: String(describing: error), stderr: result.stderr), result: result)
      }
    }
    return StatusFetchOutcome(report: nil, failure: .commandFailed(exitCode: result.exitCode, stderr: result.stderr), result: result)
  }

  public func fetchFleetStatus() async -> FleetStatusFetchOutcome {
    let fleetBudgetMs = 6000
    let result = await CommandRunner.run(
      executable: binaryPath,
      arguments: ["fleet", "status", "--json", "--config", configPath, "--timeout-ms", String(fleetBudgetMs), "--no-fetch"],
      timeout: 10
    )
    if result.launchFailure != nil {
      return FleetStatusFetchOutcome(section: nil, rawSection: nil, failure: .binaryMissing(binaryPath), result: result)
    }
    if result.timedOut {
      return FleetStatusFetchOutcome(section: nil, rawSection: nil, failure: .timedOut, result: result)
    }
    if Self.looksUnsupported(result) {
      return FleetStatusFetchOutcome(section: nil, rawSection: nil, failure: .unsupportedBinary(binaryPath), result: result)
    }
    guard let data = result.stdout.data(using: .utf8), !result.stdout.isEmpty else {
      return FleetStatusFetchOutcome(section: nil, rawSection: nil, failure: .commandFailed(exitCode: result.exitCode, stderr: result.stderr), result: result)
    }
    do {
      let report = try JSONDecoder().decode(JSONValue.self, from: data)
      let section = Self.fleetSection(from: report, duration: result.duration)
      return FleetStatusFetchOutcome(section: section, rawSection: section.jsonValue, failure: nil, result: result)
    } catch {
      if result.exitCode != 0 {
        return FleetStatusFetchOutcome(section: nil, rawSection: nil, failure: .commandFailed(exitCode: result.exitCode, stderr: result.stderr), result: result)
      }
      return FleetStatusFetchOutcome(section: nil, rawSection: nil, failure: .parseFailed(detail: String(describing: error), stderr: result.stderr), result: result)
    }
  }

  private static func fleetSection(from report: JSONValue, duration: TimeInterval) -> StatusSection {
    let summary = report["summary"]
    let total = Int(summary?["total"]?.numberValue ?? report["machines"]?.arrayValue.map { Double($0.count) } ?? 0)
    let reachable = Int(summary?["reachable"]?.numberValue ?? 0)
    let needsUpdate = Int(summary?["needs_update"]?.numberValue ?? 0)
    let unhealthy = Int(summary?["unhealthy"]?.numberValue ?? 0)
    let state: SectionState = unhealthy > 0 ? .warn : .ok
    return StatusSection(
      state: state,
      ok: state == .ok,
      available: true,
      detail: "machines=\(total) reachable=\(reachable) needs_update=\(needsUpdate) unhealthy=\(unhealthy)",
      data: report,
      error: nil,
      durationMs: duration * 1000
    )
  }

  // MARK: - Actions

  static func looksUnsupported(_ result: CommandResult) -> Bool {
    guard result.exitCode != 0 else {
      return false
    }
    let combined = "\(result.stdout)\n\(result.stderr)".lowercased()
    return combined.contains("unknown command")
      || combined.contains("unknown subcommand")
      || combined.contains("unknown ") && combined.contains(" subcommand")
      || combined.contains("unknown route")
      || combined.contains("http 404")
      || combined.contains("unknown ") && combined.contains(" command:")
  }

  static func looksAdminUnavailable(_ result: CommandResult) -> Bool {
    let combined = "\(result.stdout)\n\(result.stderr)"
    return combined.contains("BRAIN_ADMIN_TOKEN is required") || combined.contains("FACTORY_BRAIN_ADMIN_TOKEN")
  }

  public func runAction(title: String, arguments: [String], timeout: TimeInterval? = nil) async -> ActionOutcome {
    let result = await CommandRunner.run(
      executable: binaryPath,
      arguments: arguments,
      timeout: timeout ?? actionTimeout
    )
    return Self.outcome(title: title, result: result, timeout: timeout ?? actionTimeout)
  }

  static func outcome(title: String, result: CommandResult, timeout: TimeInterval) -> ActionOutcome {
    let remoteMergeUnsupported = title == "Look for Merges" && Self.looksRemoteProposalMergeUnsupported(result)
    let unsupported = Self.looksUnsupported(result) && !remoteMergeUnsupported
    let adminUnavailable = Self.looksAdminUnavailable(result)
    let baseSummary: String
    let recovery: ActionRecovery?
    if result.launchFailure != nil {
      baseSummary = "brainctl is missing; choose the binary in Preferences."
      recovery = nil
    } else if result.timedOut {
      baseSummary = "\(title) timed out after \(Int(timeout))s."
      recovery = nil
    } else if remoteMergeUnsupported {
      baseSummary = "Merge scan needs a control host update. Nothing was changed."
      recovery = ActionRecovery(
        kind: .updateControlHost,
        title: "Update the control host",
        message: "The control host does not support proposal merge discovery yet. Proposal review still works; update the control host, then retry the merge scan.",
        primaryButtonTitle: "Update Control Host",
        technicalHint: "This usually means the Mac app/brainctl is newer than the Brainstack checkout running on the control host."
      )
    } else if unsupported {
      baseSummary = "Unsupported by installed brainctl; update Brainstack on this machine."
      recovery = nil
    } else if adminUnavailable {
      baseSummary = "Admin auth is unavailable to brainctl on this machine."
      recovery = nil
    } else if result.exitCode == 0 {
      baseSummary = "\(title) succeeded."
      recovery = nil
    } else if title == "Look for Merges", Self.looksCodexModelMismatch(result) {
      baseSummary = "Codex cannot run the configured merge model; update Codex or choose a compatible harness binary."
      recovery = nil
    } else {
      baseSummary = "\(title) failed (exit \(result.exitCode.map(String.init) ?? "n/a"))."
      recovery = nil
    }
    let rawOutput = [result.stdout, result.stderr]
      .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
      .filter { !$0.isEmpty }
      .joined(separator: "\n")
    let redactedOutput = Redaction.redact(String(rawOutput.prefix(8_000)))
    let outboxGuide = OutboxActionPresentation.guide(title: title, succeeded: result.succeeded, rawOutput: redactedOutput)
    return ActionOutcome(
      title: title,
      succeeded: result.succeeded,
      unsupported: unsupported,
      adminUnavailable: adminUnavailable,
      summary: outboxGuide?.summary ?? baseSummary,
      output: outboxGuide?.output ?? redactedOutput,
      duration: result.duration,
      recovery: recovery
    )
  }

  static func looksCodexModelMismatch(_ result: CommandResult) -> Bool {
    let combined = "\(result.stdout)\n\(result.stderr)".lowercased()
    return combined.contains("proposal merge harness failed")
      && combined.contains("codex")
      && (combined.contains("requires a newer version of codex") || combined.contains("model is not supported"))
  }

  static func looksRemoteProposalMergeUnsupported(_ result: CommandResult) -> Bool {
    guard result.exitCode != 0 else {
      return false
    }
    let combined = "\(result.stdout)\n\(result.stderr)".lowercased()
    return combined.contains("proposal batch-merge failed over ssh")
      && (combined.contains("unknown proposals subcommand: batch-merge")
        || combined.contains("unknown subcommand")
        || combined.contains("unknown command"))
  }

  // Named action builders so views never assemble argv strings.

  public func doctor() async -> ActionOutcome {
    await runAction(title: "Doctor", arguments: ["doctor", "--config", configPath])
  }

  public func outboxFlush() async -> ActionOutcome {
    await runAction(title: "Flush Outbox", arguments: ["outbox", "flush", "--config", configPath])
  }

  public func outboxRetryAllAndFlush() async -> ActionOutcome {
    let retry = await runAction(title: "Retry Saved Writes", arguments: ["outbox", "retry", "--all", "--config", configPath])
    guard retry.succeeded else {
      return retry
    }
    let flush = await outboxFlush()
    let output = [
      "Retry step: requeued paused saved writes.",
      flush.output
    ]
      .filter { !$0.isEmpty }
      .joined(separator: "\n\n")
    return ActionOutcome(
      title: "Retry Saved Writes",
      succeeded: flush.succeeded,
      unsupported: retry.unsupported || flush.unsupported,
      adminUnavailable: retry.adminUnavailable || flush.adminUnavailable,
      summary: flush.summary,
      output: output,
      duration: retry.duration + flush.duration
    )
  }

  public func outboxDiscardAll() async -> ActionOutcome {
    await runAction(title: "Discard Saved Writes", arguments: ["outbox", "purge", "--yes", "--config", configPath])
  }

  public func outboxDiscardCorrupt() async -> ActionOutcome {
    await runAction(title: "Discard Damaged Saved Writes", arguments: ["outbox", "purge-corrupt", "--yes", "--config", configPath])
  }

  public func skillsRefresh() async -> ActionOutcome {
    await runAction(title: "Refresh Skills", arguments: ["skills", "refresh", "--config", configPath])
  }

  public func daemonInstall() async -> ActionOutcome {
    await runAction(title: "Install/Restart Daemon", arguments: ["daemon", "install", "--start", "--config", configPath])
  }

  public func daemonStatus() async -> ActionOutcome {
    await runAction(title: "Daemon Status", arguments: ["daemon", "status", "--config", configPath, "--json"], timeout: 15)
  }

  public func hooksStatus() async -> ActionOutcome {
    await runAction(title: "Hooks Status", arguments: ["hooks", "status", "--target", "all", "--config", configPath], timeout: 15)
  }

  public func hooksInstall() async -> ActionOutcome {
    await runAction(title: "Install/Repair Hooks", arguments: ["hooks", "install", "--target", "all", "--config", configPath])
  }

  public func updates() async -> ActionOutcome {
    await runAction(title: "Check Stack Updates", arguments: ["updates", "--config", configPath], timeout: 60)
  }

  public func fleetUpdate(machine: String, title: String? = nil) async -> ActionOutcome {
    await runAction(title: title ?? "Update \(machine)", arguments: ["fleet", "update", machine, "--config", configPath], timeout: 300)
  }

  public func enroll(inviteFile: String, timeout: TimeInterval? = nil) async -> ActionOutcome {
    await runAction(
      title: "Enroll",
      arguments: ["enroll", "--invite-file", inviteFile, "--config", configPath],
      timeout: timeout ?? actionTimeout
    )
  }

  public func lifecycleRepair(timeout: TimeInterval? = nil) async -> ActionOutcome {
    await runAction(
      title: "Lifecycle Repair",
      arguments: ["lifecycle", "repair", "--config", configPath],
      timeout: timeout ?? actionTimeout
    )
  }

  public func curatorStatus() async -> ActionOutcome {
    await runAction(title: "Curator Status", arguments: ["curator", "status", "--config", configPath], timeout: 20)
  }

  public func curatorRun() async -> ActionOutcome {
    await runAction(title: "Curator Run", arguments: ["curator", "run", "--config", configPath])
  }

  public func curatorInstall() async -> ActionOutcome {
    await runAction(title: "Install Curator", arguments: ["curator", "install", "--config", configPath])
  }

  public func proposalsList() async -> ActionOutcome {
    await runAction(title: "List Proposals", arguments: ["proposals", "list", "--status", "open", "--json", "--config", configPath], timeout: 20)
  }

  /// List + parse in one step. Parsing happens on the raw stdout *before* redaction,
  /// since redaction is for human-facing output and could mangle JSON fields.
  public func fetchOpenProposals() async -> (proposals: [ProposalSummary]?, outcome: ActionOutcome) {
    let result = await CommandRunner.run(
      executable: binaryPath,
      arguments: ["proposals", "list", "--status", "open", "--json", "--config", configPath],
      timeout: 20
    )
    let outcome = Self.outcome(title: "List Proposals", result: result, timeout: 20)
    guard result.succeeded else {
      return (nil, outcome)
    }
    return (ProposalSummary.parseList(result.stdout), outcome)
  }

  public func proposalShow(id: String) async -> ActionOutcome {
    await runAction(title: "Show Proposal", arguments: ["proposals", "show", id, "--json", "--config", configPath], timeout: 20)
  }

  /// Show + parse in one step; parses raw stdout before redaction (see fetchOpenProposals).
  public func fetchProposalDetail(id: String) async -> (detail: ProposalDetail?, outcome: ActionOutcome) {
    let result = await CommandRunner.run(
      executable: binaryPath,
      arguments: ["proposals", "show", id, "--json", "--config", configPath],
      timeout: 20
    )
    let outcome = Self.outcome(title: "Show Proposal", result: result, timeout: 20)
    guard result.succeeded else {
      return (nil, outcome)
    }
    return (ProposalDetail.parse(result.stdout), outcome)
  }

  public func proposalDecision(id: String, action: String) async -> ActionOutcome {
    let title = action == "apply" ? "Accept Proposal" : "\(action.capitalized) Proposal"
    return await runAction(title: title, arguments: ["proposals", action, id, "--config", configPath], timeout: 60)
  }

  public func proposalMergeGroup(groupKey: String) async -> ActionOutcome {
    await runAction(
      title: "Merge Proposal Group",
      arguments: ["proposals", "merge-group", groupKey, "--submit", "--close-sources", "--config", configPath],
      timeout: 120
    )
  }

  public func proposalAutoMerge() async -> ActionOutcome {
    await runAction(
      title: "Look for Merges",
      arguments: ["proposals", "batch-merge", "--submit", "--limit", "100", "--auto-threshold", "0.8", "--config", configPath],
      timeout: 600
    )
  }

  public func fetchUploads(machine: String) async -> (uploads: [UploadSummary]?, outcome: ActionOutcome) {
    let result = await CommandRunner.run(
      executable: binaryPath,
      arguments: ["uploads", "list", "--machine", machine, "--recent", "--limit", "50", "--json", "--config", configPath],
      timeout: 45
    )
    let outcome = Self.outcome(title: "List Uploads", result: result, timeout: 45)
    guard result.succeeded else {
      return (nil, outcome)
    }
    return (UploadSummary.parseList(result.stdout), outcome)
  }

  public func uploadFile(machine: String, path: String) async -> ActionOutcome {
    await runAction(
      title: "Upload File",
      arguments: ["uploads", "put", "--machine", machine, "--file", path, "--json", "--config", configPath],
      timeout: 900
    )
  }

  public func deleteUpload(machine: String, id: String) async -> ActionOutcome {
    await runAction(
      title: "Delete Upload",
      arguments: ["uploads", "rm", "--machine", machine, "--id", id, "--json", "--config", configPath],
      timeout: 60
    )
  }
}
