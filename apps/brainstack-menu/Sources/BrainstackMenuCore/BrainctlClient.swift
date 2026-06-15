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

public struct ActionOutcome: Sendable {
  public let title: String
  public let succeeded: Bool
  public let unsupported: Bool
  public let adminUnavailable: Bool
  public let summary: String
  public let output: String
  public let duration: TimeInterval

  public init(title: String, succeeded: Bool, unsupported: Bool, adminUnavailable: Bool, summary: String, output: String, duration: TimeInterval) {
    self.title = title
    self.succeeded = succeeded
    self.unsupported = unsupported
    self.adminUnavailable = adminUnavailable
    self.summary = summary
    self.output = output
    self.duration = duration
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
    let perSectionBudgetMs = 750
    let result = await CommandRunner.run(
      executable: binaryPath,
      arguments: ["status", "--json", "--config", configPath, "--timeout-ms", String(perSectionBudgetMs)],
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

  // MARK: - Actions

  static func looksUnsupported(_ result: CommandResult) -> Bool {
    guard result.exitCode != 0 else {
      return false
    }
    let combined = "\(result.stdout)\n\(result.stderr)".lowercased()
    return combined.contains("unknown command")
      || combined.contains("unknown subcommand")
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
    let unsupported = Self.looksUnsupported(result)
    let adminUnavailable = Self.looksAdminUnavailable(result)
    let summary: String
    if result.launchFailure != nil {
      summary = "brainctl is missing; choose the binary in Preferences."
    } else if result.timedOut {
      summary = "\(title) timed out after \(Int(timeout))s."
    } else if unsupported {
      summary = "Unsupported by installed brainctl; update Brainstack on this machine."
    } else if adminUnavailable {
      summary = "Admin auth is unavailable to brainctl on this machine."
    } else if result.exitCode == 0 {
      summary = "\(title) succeeded."
    } else {
      summary = "\(title) failed (exit \(result.exitCode.map(String.init) ?? "n/a"))."
    }
    let output = [result.stdout, result.stderr]
      .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
      .filter { !$0.isEmpty }
      .joined(separator: "\n")
    return ActionOutcome(
      title: title,
      succeeded: result.succeeded,
      unsupported: unsupported,
      adminUnavailable: adminUnavailable,
      summary: summary,
      output: Redaction.redact(String(output.prefix(8_000))),
      duration: result.duration
    )
  }

  // Named action builders so views never assemble argv strings.

  public func doctor() async -> ActionOutcome {
    await runAction(title: "Doctor", arguments: ["doctor", "--config", configPath])
  }

  public func outboxFlush() async -> ActionOutcome {
    await runAction(title: "Flush Outbox", arguments: ["outbox", "flush", "--config", configPath])
  }

  public func skillsRefresh() async -> ActionOutcome {
    await runAction(title: "Refresh Skills", arguments: ["skills", "refresh", "--config", configPath])
  }

  public func daemonInstall() async -> ActionOutcome {
    await runAction(title: "Install/Restart Daemon", arguments: ["daemon", "install", "--config", configPath])
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
}
