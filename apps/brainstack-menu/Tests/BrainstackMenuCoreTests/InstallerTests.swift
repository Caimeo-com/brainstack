import XCTest
@testable import BrainstackMenuCore

final class InstallerTests: XCTestCase {
  private var scratch: URL!

  override func setUpWithError() throws {
    scratch = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent("brainstack-menu-installer-tests-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: scratch, withIntermediateDirectories: true)
  }

  override func tearDownWithError() throws {
    try? FileManager.default.removeItem(at: scratch)
  }

  private func fakeBrainctl(_ body: String) throws -> String {
    let path = scratch.appendingPathComponent("brainctl")
    try "#!/bin/sh\n\(body)\n".write(to: path, atomically: true, encoding: .utf8)
    try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: path.path)
    return path.path
  }

  func testBundledBrainctlCanResolveFromEnvironmentOverride() throws {
    let binary = try fakeBrainctl("exit 0")
    XCTAssertEqual(
      BrainstackMenuInstaller.bundledBrainctlPath(environment: ["BRAINSTACK_MENU_BUNDLED_BRAINCTL": binary]),
      binary
    )
    XCTAssertNil(
      BrainstackMenuInstaller.bundledBrainctlPath(environment: ["BRAINSTACK_MENU_BUNDLED_BRAINCTL": scratch.appendingPathComponent("missing").path])
    )
  }

  func testInstallerCopiesStableBrainctlAndUsesPrivateInviteFile() async throws {
    let log = scratch.appendingPathComponent("args.log")
    let source = try fakeBrainctl(
      """
      printf '%s\\n' "$@" >> '\(log.path)'
      if [ "${1:-}" = "enroll" ]; then
        perm="$(stat -f '%Lp' "$3" 2>/dev/null || stat -c '%a' "$3" 2>/dev/null || echo unknown)"
        printf 'invite_perm=%s\\n' "$perm" >> '\(log.path)'
        grep -q '^bs1_secret_value$' "$3" || exit 7
      fi
      exit 0
      """
    )
    let target = scratch.appendingPathComponent("bin/brainctl").path
    let config = scratch.appendingPathComponent("brainstack.yaml").path

    let outcome = await BrainstackMenuInstaller.installAndRepair(
      sourcePath: source,
      targetPath: target,
      configPath: config,
      invite: "bs1_secret_value"
    )

    XCTAssertTrue(outcome.succeeded, outcome.output)
    XCTAssertTrue(FileManager.default.isExecutableFile(atPath: target))
    XCTAssertFalse(outcome.output.contains("bs1_secret_value"))
    let args = try String(contentsOf: log)
    XCTAssertFalse(args.contains("bs1_secret_value"))
    XCTAssertTrue(args.contains("enroll\n--invite-file\n"))
    XCTAssertTrue(args.contains("invite_perm=600"))
    XCTAssertTrue(args.contains("lifecycle\nrepair\n--config\n\(config)\n"))
  }

  func testInstallerWithoutInviteRequiresExistingConfigBeforeRepair() async throws {
    let log = scratch.appendingPathComponent("args.log")
    let source = try fakeBrainctl("printf '%s\\n' \"$@\" >> '\(log.path)'")
    let target = scratch.appendingPathComponent("bin/brainctl").path
    let config = scratch.appendingPathComponent("missing.yaml").path

    let outcome = await BrainstackMenuInstaller.installAndRepair(
      sourcePath: source,
      targetPath: target,
      configPath: config,
      invite: nil
    )

    XCTAssertFalse(outcome.succeeded)
    XCTAssertTrue(outcome.summary.contains("Paste a Brainstack invite"))
    XCTAssertFalse(FileManager.default.fileExists(atPath: log.path))
  }

  func testInstallerWithoutInviteRepairsExistingConfig() async throws {
    let log = scratch.appendingPathComponent("args.log")
    let source = try fakeBrainctl("printf '%s\\n' \"$@\" >> '\(log.path)'")
    let target = scratch.appendingPathComponent("bin/brainctl").path
    let config = scratch.appendingPathComponent("brainstack.yaml").path
    try "schema_version: 1\nprofile: client-macos\n".write(toFile: config, atomically: true, encoding: .utf8)

    let outcome = await BrainstackMenuInstaller.installAndRepair(
      sourcePath: source,
      targetPath: target,
      configPath: config,
      invite: "   "
    )

    XCTAssertTrue(outcome.succeeded, outcome.output)
    let args = try String(contentsOf: log)
    XCTAssertFalse(args.contains("enroll"))
    XCTAssertTrue(args.contains("lifecycle\nrepair\n--config\n\(config)\n"))
  }
}
