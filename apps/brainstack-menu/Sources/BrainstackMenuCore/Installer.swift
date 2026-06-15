import Darwin
import Foundation

public enum BrainstackMenuInstaller {
  public static let bundledBinaryName = "brainctl"
  private static let maxInviteBytes = 128 * 1024

  public static func defaultInstallPath(fileManager: FileManager = .default) -> String {
    "\(fileManager.homeDirectoryForCurrentUser.path)/.local/bin/brainctl"
  }

  public static func bundledBrainctlPath(
    bundle: Bundle = .main,
    environment: [String: String] = ProcessInfo.processInfo.environment,
    fileManager: FileManager = .default
  ) -> String? {
    if let override = environment["BRAINSTACK_MENU_BUNDLED_BRAINCTL"], !override.isEmpty {
      let expanded = (override as NSString).expandingTildeInPath
      return fileManager.isExecutableFile(atPath: expanded) ? expanded : nil
    }
    guard let resourceURL = bundle.resourceURL else {
      return nil
    }
    let path = resourceURL.appendingPathComponent(bundledBinaryName).path
    return fileManager.isExecutableFile(atPath: path) ? path : nil
  }

  public static func installAndRepair(
    sourcePath: String? = nil,
    targetPath: String? = nil,
    configPath: String,
    invite: String?,
    timeout: TimeInterval = 300.0,
    fileManager: FileManager = .default
  ) async -> ActionOutcome {
    let title = invite?.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false ? "Set Up Brainstack" : "Repair Brainstack"
    let started = Date()
    guard let source = sourcePath ?? bundledBrainctlPath(fileManager: fileManager) else {
      return ActionOutcome(
        title: title,
        succeeded: false,
        unsupported: false,
        adminUnavailable: false,
        summary: "This app bundle does not include brainctl. Install from a signed release DMG or use the terminal installer.",
        output: "",
        duration: Date().timeIntervalSince(started)
      )
    }

    let expandedConfigPath = (configPath as NSString).expandingTildeInPath
    let destination = targetPath ?? defaultInstallPath(fileManager: fileManager)
    let installOutcome = await installBundledBrainctl(sourcePath: source, targetPath: destination, fileManager: fileManager)
    guard installOutcome.succeeded else {
      return installOutcome
    }

    let client = BrainctlClient(binaryPath: destination, configPath: expandedConfigPath, actionTimeout: timeout)
    var outcomes = [installOutcome]
    let trimmedInvite = invite?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""

    if !trimmedInvite.isEmpty {
      do {
        let inviteFile = try writePrivateInviteFile(trimmedInvite, fileManager: fileManager)
        defer { cleanupPrivateInviteFile(inviteFile, fileManager: fileManager) }
        let enroll = await client.enroll(inviteFile: inviteFile.path, timeout: timeout)
        outcomes.append(enroll)
        guard enroll.succeeded else {
          return combinedOutcome(title: title, outcomes: outcomes, started: started)
        }
      } catch {
        outcomes.append(ActionOutcome(
          title: "Write Invite",
          succeeded: false,
          unsupported: false,
          adminUnavailable: false,
          summary: "Could not write a private temporary invite file.",
          output: String(describing: error),
          duration: 0
        ))
        return combinedOutcome(title: title, outcomes: outcomes, started: started)
      }
    } else if !fileManager.fileExists(atPath: expandedConfigPath) {
      outcomes.append(ActionOutcome(
        title: "Enroll",
        succeeded: false,
        unsupported: false,
        adminUnavailable: false,
        summary: "Paste a Brainstack invite to enroll this Mac.",
        output: "",
        duration: 0
      ))
      return combinedOutcome(title: title, outcomes: outcomes, started: started)
    }

    let repair = await client.lifecycleRepair(timeout: timeout)
    outcomes.append(repair)
    return combinedOutcome(title: title, outcomes: outcomes, started: started)
  }

  public static func installBundledBrainctl(
    sourcePath: String,
    targetPath: String? = nil,
    fileManager: FileManager = .default
  ) async -> ActionOutcome {
    let started = Date()
    let title = "Install brainctl"
    let source = (sourcePath as NSString).expandingTildeInPath
    let destination = ((targetPath ?? defaultInstallPath(fileManager: fileManager)) as NSString).expandingTildeInPath
    guard fileManager.isExecutableFile(atPath: source) else {
      return ActionOutcome(
        title: title,
        succeeded: false,
        unsupported: false,
        adminUnavailable: false,
        summary: "Bundled brainctl is missing or not executable.",
        output: source,
        duration: Date().timeIntervalSince(started)
      )
    }

    do {
      let destinationURL = URL(fileURLWithPath: destination)
      let parent = destinationURL.deletingLastPathComponent()
      try fileManager.createDirectory(at: parent, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o755])
      let temporaryURL = parent.appendingPathComponent(".brainctl.\(UUID().uuidString)")
      try? fileManager.removeItem(at: temporaryURL)
      try fileManager.copyItem(at: URL(fileURLWithPath: source), to: temporaryURL)
      try fileManager.setAttributes([.posixPermissions: 0o755], ofItemAtPath: temporaryURL.path)
      if rename(temporaryURL.path, destinationURL.path) != 0 {
        let error = errno
        try? fileManager.removeItem(at: temporaryURL)
        throw POSIXError(POSIXErrorCode(rawValue: error) ?? .EIO)
      }
      await clearQuarantineIfPresent(destination)
      return ActionOutcome(
        title: title,
        succeeded: true,
        unsupported: false,
        adminUnavailable: false,
        summary: "Installed bundled brainctl.",
        output: destination,
        duration: Date().timeIntervalSince(started)
      )
    } catch {
      return ActionOutcome(
        title: title,
        succeeded: false,
        unsupported: false,
        adminUnavailable: false,
        summary: "Could not install bundled brainctl.",
        output: String(describing: error),
        duration: Date().timeIntervalSince(started)
      )
    }
  }

  static func writePrivateInviteFile(_ invite: String, fileManager: FileManager = .default) throws -> URL {
    let data = Data("\(invite.trimmingCharacters(in: .whitespacesAndNewlines))\n".utf8)
    if data.isEmpty || data.count > maxInviteBytes {
      throw InstallerError.invalidInviteSize
    }
    let folder = URL(fileURLWithPath: NSTemporaryDirectory())
      .appendingPathComponent("brainstack-menu-\(UUID().uuidString)", isDirectory: true)
    try fileManager.createDirectory(at: folder, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
    let file = folder.appendingPathComponent("invite.txt")
    guard fileManager.createFile(atPath: file.path, contents: data, attributes: [.posixPermissions: 0o600]) else {
      throw InstallerError.couldNotCreateInviteFile
    }
    try fileManager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
    return file
  }

  static func cleanupPrivateInviteFile(_ file: URL, fileManager: FileManager = .default) {
    try? fileManager.removeItem(at: file.deletingLastPathComponent())
  }

  private static func clearQuarantineIfPresent(_ path: String) async {
    guard FileManager.default.isExecutableFile(atPath: "/usr/bin/xattr") else {
      return
    }
    _ = await CommandRunner.run(
      executable: "/usr/bin/xattr",
      arguments: ["-d", "com.apple.quarantine", path],
      timeout: 3
    )
  }

  private static func combinedOutcome(title: String, outcomes: [ActionOutcome], started: Date) -> ActionOutcome {
    let succeeded = outcomes.allSatisfy(\.succeeded)
    let last = outcomes.last
    let output = outcomes
      .map { outcome in
        var parts = ["\(outcome.title): \(outcome.summary)"]
        if !outcome.output.isEmpty {
          parts.append(outcome.output)
        }
        return parts.joined(separator: "\n")
      }
      .joined(separator: "\n\n")
    return ActionOutcome(
      title: title,
      succeeded: succeeded,
      unsupported: outcomes.contains(where: \.unsupported),
      adminUnavailable: outcomes.contains(where: \.adminUnavailable),
      summary: succeeded ? "\(title) succeeded." : (last?.summary ?? "\(title) failed."),
      output: Redaction.redact(String(output.prefix(8_000))),
      duration: Date().timeIntervalSince(started)
    )
  }
}

enum InstallerError: Error {
  case invalidInviteSize
  case couldNotCreateInviteFile
}
