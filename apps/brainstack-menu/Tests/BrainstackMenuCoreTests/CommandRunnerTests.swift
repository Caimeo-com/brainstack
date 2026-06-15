import XCTest
@testable import BrainstackMenuCore

final class CommandRunnerTests: XCTestCase {
  private var scratch: URL!

  override func setUpWithError() throws {
    scratch = URL(fileURLWithPath: NSTemporaryDirectory()).appendingPathComponent("brainstack-menu-tests-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: scratch, withIntermediateDirectories: true)
  }

  override func tearDownWithError() throws {
    try? FileManager.default.removeItem(at: scratch)
  }

  /// Writes an executable fake `brainctl` shell script and returns its path.
  private func fakeBrainctl(_ body: String) throws -> String {
    let path = scratch.appendingPathComponent("brainctl")
    try "#!/bin/sh\n\(body)\n".write(to: path, atomically: true, encoding: .utf8)
    try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: path.path)
    return path.path
  }

  private func client(_ binary: String) -> BrainctlClient {
    BrainctlClient(binaryPath: binary, configPath: "/tmp/test.yaml", statusTimeout: 2.0, actionTimeout: 2.0)
  }

  func testValidJsonStatus() async throws {
    let binary = try fakeBrainctl(#"echo '{"ok": true, "degraded": false, "profile": "client-macos", "machine": "m", "sections": {"config": {"state": "ok", "detail": "config loaded"}}}'"#)
    let outcome = await client(binary).fetchStatus()
    XCTAssertNil(outcome.failure)
    XCTAssertEqual(outcome.report?.machine, "m")
    XCTAssertEqual(OverallStateMapper.map(report: outcome.report), .green)
  }

  func testStatusUsesDefaultBrainctlBudget() async throws {
    let argsPath = scratch.appendingPathComponent("status-args.txt")
    let binary = try fakeBrainctl("printf '%s\\n' \"$@\" > '\(argsPath.path)'; echo '{\"ok\": true, \"degraded\": false, \"sections\": {}}'")
    let outcome = await client(binary).fetchStatus()
    XCTAssertNil(outcome.failure)
    XCTAssertEqual(try String(contentsOf: argsPath), "status\n--json\n--config\n/tmp/test.yaml\n--timeout-ms\n1500\n")
  }

  func testInvalidJsonProducesParseFailure() async throws {
    let binary = try fakeBrainctl("echo 'this is not json'; echo 'warning noise' >&2")
    let outcome = await client(binary).fetchStatus()
    XCTAssertNil(outcome.report)
    guard case .parseFailed(_, let stderr) = outcome.failure else {
      return XCTFail("expected parseFailed, got \(String(describing: outcome.failure))")
    }
    XCTAssertTrue(stderr.contains("warning noise"))
  }

  func testNonzeroExitWithoutJsonProducesCommandFailure() async throws {
    let binary = try fakeBrainctl("echo 'fatal: config exploded' >&2; exit 3")
    let outcome = await client(binary).fetchStatus()
    XCTAssertNil(outcome.report)
    guard case .commandFailed(let exitCode, let stderr) = outcome.failure else {
      return XCTFail("expected commandFailed, got \(String(describing: outcome.failure))")
    }
    XCTAssertEqual(exitCode, 3)
    XCTAssertTrue(stderr.contains("config exploded"))
  }

  func testHangingProcessTimesOut() async throws {
    let binary = try fakeBrainctl("sleep 30")
    let started = Date()
    let outcome = await client(binary).fetchStatus()
    let elapsed = Date().timeIntervalSince(started)
    XCTAssertLessThan(elapsed, 10, "timeout must be enforced")
    guard case .timedOut = outcome.failure else {
      return XCTFail("expected timedOut, got \(String(describing: outcome.failure))")
    }
  }

  func testOldBinaryWithoutStatusCommandIsUnsupported() async throws {
    // An installed brainctl that predates `status` prints "Unknown command: status"
    // plus usage and exits 1; the app must show update guidance, not a generic failure.
    let binary = try fakeBrainctl("echo 'Unknown command: status' >&2; echo 'Usage: brainctl init ...' >&2; exit 1")
    let outcome = await client(binary).fetchStatus()
    XCTAssertNil(outcome.report)
    guard case .unsupportedBinary(let path) = outcome.failure else {
      return XCTFail("expected unsupportedBinary, got \(String(describing: outcome.failure))")
    }
    XCTAssertEqual(path, binary)
  }

  func testMissingBinaryIsStructuredFailure() async {
    let outcome = await client("/nonexistent/brainctl").fetchStatus()
    guard case .binaryMissing = outcome.failure else {
      return XCTFail("expected binaryMissing, got \(String(describing: outcome.failure))")
    }
  }

  func testActionTimeoutIsStructured() async throws {
    let binary = try fakeBrainctl("sleep 30")
    let outcome = await client(binary).runAction(title: "Doctor", arguments: ["doctor"], timeout: 1.0)
    XCTAssertFalse(outcome.succeeded)
    XCTAssertTrue(outcome.summary.contains("timed out"))
  }

  func testUnsupportedCommandDetection() async throws {
    let binary = try fakeBrainctl("echo 'Unknown command: curator' >&2; exit 1")
    let outcome = await client(binary).runAction(title: "Curator Run", arguments: ["curator", "run"])
    XCTAssertTrue(outcome.unsupported)
    XCTAssertTrue(outcome.summary.contains("Unsupported by installed brainctl"))
  }

  func testAdminUnavailableDetection() async throws {
    let binary = try fakeBrainctl("echo 'BRAIN_ADMIN_TOKEN is required for this action; run it on the control host or export BRAIN_ADMIN_TOKEN' >&2; exit 1")
    let outcome = await client(binary).runAction(title: "Accept Proposal", arguments: ["proposals", "apply", "x"])
    XCTAssertTrue(outcome.adminUnavailable)
    XCTAssertFalse(outcome.succeeded)
  }

  func testProposalDecisionApplyUsesBrainctlApply() async throws {
    let argsPath = scratch.appendingPathComponent("proposal-args.txt")
    let binary = try fakeBrainctl("printf '%s\\n' \"$@\" > '\(argsPath.path)'; echo 'accepted'")
    let outcome = await client(binary).proposalDecision(id: "p1", action: "apply")
    XCTAssertTrue(outcome.succeeded)
    XCTAssertEqual(try String(contentsOf: argsPath), "proposals\napply\np1\n--config\n/tmp/test.yaml\n")
    XCTAssertEqual(outcome.title, "Accept Proposal")
  }

  func testCuratorInstallUsesNamedArguments() async throws {
    let argsPath = scratch.appendingPathComponent("args.txt")
    let binary = try fakeBrainctl("printf '%s\\n' \"$@\" > '\(argsPath.path)'; echo 'installed'")
    let outcome = await client(binary).curatorInstall()
    XCTAssertTrue(outcome.succeeded)
    XCTAssertEqual(try String(contentsOf: argsPath), "curator\ninstall\n--config\n/tmp/test.yaml\n")
  }

  func testActionOutputIsRedacted() async throws {
    let binary = try fakeBrainctl("echo 'BRAIN_ADMIN_TOKEN=8f14e45fceea167a5a36dedd4bea2543aa758ff6cc9929d8efaf3279a3e9b414'")
    let outcome = await client(binary).runAction(title: "Doctor", arguments: ["doctor"])
    XCTAssertFalse(outcome.output.contains("8f14e45fceea167a"))
    XCTAssertTrue(outcome.output.contains("[redacted"))
  }

  func testFetchOpenProposalsParsesRawStdout() async throws {
    let binary = try fakeBrainctl(#"echo '{"ok": true, "mode": "approval", "proposals": [{"id": "p1", "title": "T", "status": "pending", "target_page": "wiki/Status/X.md", "risk": "low", "created_at": "2026-06-11T00:00:00Z"}]}'"#)
    let (proposals, outcome) = await client(binary).fetchOpenProposals()
    XCTAssertTrue(outcome.succeeded)
    XCTAssertEqual(proposals?.count, 1)
    XCTAssertEqual(proposals?.first?.id, "p1")
  }

  func testFetchOpenProposalsFailureIsStructured() async throws {
    let binary = try fakeBrainctl("echo 'brain GET /api/proposals failed' >&2; exit 1")
    let (proposals, outcome) = await client(binary).fetchOpenProposals()
    XCTAssertNil(proposals)
    XCTAssertFalse(outcome.succeeded)
  }

  func testBinaryResolutionPrefersExplicitPath() throws {
    let binary = try fakeBrainctl("exit 0")
    XCTAssertEqual(BrainctlClient.resolveBinary(preferred: binary), binary)
    XCTAssertNotEqual(BrainctlClient.resolveBinary(preferred: "/nonexistent/brainctl"), "/nonexistent/brainctl")
  }
}
