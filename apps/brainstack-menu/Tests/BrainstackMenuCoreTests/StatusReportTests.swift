import XCTest
@testable import BrainstackMenuCore

final class StatusReportTests: XCTestCase {
  private func fixture(_ name: String) throws -> Data {
    guard let url = Bundle.module.url(forResource: "Fixtures/\(name)", withExtension: "json") ??
      Bundle.module.url(forResource: name, withExtension: "json", subdirectory: "Fixtures") else {
      throw XCTSkip("fixture \(name) missing")
    }
    return try Data(contentsOf: url)
  }

  func testHealthyFixtureMapsGreen() throws {
    let report = try StatusReport.parse(data: fixture("status-healthy"))
    XCTAssertEqual(report.profile, "client-macos")
    XCTAssertEqual(report.machine, "mac-client")
    XCTAssertTrue(report.ok)
    XCTAssertFalse(report.degraded)
    XCTAssertEqual(report.sections["daemon"]?.state, .ok)
    XCTAssertEqual(OverallStateMapper.map(report: report), .green)
    // Contract-first ordering: config first, then daemon.
    XCTAssertEqual(report.sectionNames.prefix(2), ["config", "daemon"])
  }

  func testDegradedFixtureMapsYellow() throws {
    let report = try StatusReport.parse(data: fixture("status-degraded"))
    XCTAssertEqual(OverallStateMapper.map(report: report), .yellow)
    XCTAssertEqual(report.sections["daemon"]?.state, .warn)
    XCTAssertEqual(report.sections["brain_api"]?.available, false)
  }

  func testFailedFixtureMapsRed() throws {
    let report = try StatusReport.parse(data: fixture("status-failed"))
    XCTAssertEqual(OverallStateMapper.map(report: report), .red)
    XCTAssertEqual(report.sections["outbox"]?.state, .fail)
  }

  func testMissingConfigFixtureMapsRed() throws {
    let report = try StatusReport.parse(data: fixture("status-missing-config"))
    XCTAssertNil(report.profile)
    XCTAssertEqual(OverallStateMapper.map(report: report), .red)
    XCTAssertTrue(report.sections["config"]?.error?.contains("config not found") ?? false)
  }

  func testOldControlHostIsDegradedNotBroken() throws {
    let report = try StatusReport.parse(data: fixture("status-old-control-host"))
    XCTAssertEqual(OverallStateMapper.map(report: report), .yellow)
    XCTAssertEqual(report.sections["curator"]?.available, false)
    XCTAssertEqual(report.sections["proposals"]?.state, .warn)
  }

  func testUnknownSectionsAndStatesDoNotCrash() throws {
    let report = try StatusReport.parse(data: fixture("status-unknown-sections"))
    XCTAssertTrue(report.sectionNames.contains("quantum_sync"))
    XCTAssertTrue(report.sectionNames.contains("future_thing"))
    XCTAssertEqual(report.sections["future_thing"]?.state, .unknown)
    // Unknown states render as degraded, never green or crash.
    XCTAssertEqual(OverallStateMapper.map(report: report), .yellow)
  }

  func testNilAndEmptyReportsMapGray() throws {
    XCTAssertEqual(OverallStateMapper.map(report: nil), .gray)
    let empty = try StatusReport.parse(data: Data(#"{"ok": false, "degraded": true, "sections": {}}"#.utf8))
    XCTAssertEqual(OverallStateMapper.map(report: empty), .gray)
  }

  func testInvalidJsonThrows() {
    XCTAssertThrowsError(try StatusReport.parse(data: Data("not json".utf8)))
    XCTAssertThrowsError(try StatusReport.parse(data: Data("[1,2,3]".utf8)))
  }
}

final class ProposalTests: XCTestCase {
  func testParseProposalList() {
    let json = """
    {"ok": true, "mode": "approval", "proposals": [
      {"id": "p1", "title": "First", "status": "pending", "target_page": "wiki/Status/A.md", "risk": "low", "created_at": "2026-06-11T00:00:00Z"},
      {"id": "p2", "title": "Second", "status": "needs-human", "target_page": null, "risk": null, "created_at": "2026-06-11T01:00:00Z"}
    ]}
    """
    let proposals = ProposalSummary.parseList(json)
    XCTAssertEqual(proposals?.count, 2)
    XCTAssertEqual(proposals?[0].targetPage, "wiki/Status/A.md")
    XCTAssertNil(proposals?[1].targetPage)
    XCTAssertNil(ProposalSummary.parseList("not json"))
  }
}

final class ProposalDetailTests: XCTestCase {
  func testParseDetail() {
    let json = """
    {"ok": true,
     "proposal": {"id": "p1", "title": "T", "status": "pending", "target_page": "wiki/Status/A.md", "risk": "low", "confidence": 0.85, "created_at": "2026-06-11T00:00:00Z", "source_ids": ["a", "b"], "reason": "why"},
     "body": "## Request\\n\\ncontent",
     "diff": "+ added line"}
    """
    let detail = ProposalDetail.parse(json)
    XCTAssertEqual(detail?.summary.id, "p1")
    XCTAssertEqual(detail?.diff, "+ added line")
    XCTAssertEqual(detail?.reason, "why")
    XCTAssertEqual(detail?.sourceIds, ["a", "b"])
    XCTAssertEqual(detail?.confidence, 0.85)
    XCTAssertNil(ProposalDetail.parse("not json"))
    XCTAssertNil(ProposalDetail.parse(#"{"ok": true}"#))
  }
}

final class TransitionDetectorTests: XCTestCase {
  private func snapshot(_ overall: OverallState, outbox: Bool = false, curator: Bool = false, open: Int = 0) -> TransitionDetector.Snapshot {
    TransitionDetector.Snapshot(overall: overall, outboxTerminalOrCorrupt: outbox, curatorFailing: curator, openProposals: open)
  }

  func testNotifiesOnlyOnTransitions() {
    let detector = TransitionDetector()
    XCTAssertTrue(detector.messages(from: nil, to: snapshot(.red), operatorMode: false).isEmpty)
    XCTAssertEqual(detector.messages(from: snapshot(.green), to: snapshot(.red), operatorMode: false).count, 1)
    // Unchanged degraded state: no spam.
    XCTAssertTrue(detector.messages(from: snapshot(.red), to: snapshot(.red), operatorMode: false).isEmpty)
    XCTAssertEqual(detector.messages(from: snapshot(.red), to: snapshot(.green), operatorMode: false).count, 1)
    XCTAssertEqual(detector.messages(from: snapshot(.green), to: snapshot(.green, outbox: true), operatorMode: false).count, 1)
    XCTAssertTrue(detector.messages(from: snapshot(.green, outbox: true), to: snapshot(.green, outbox: true), operatorMode: false).isEmpty)
  }

  func testProposalNotificationsRequireOperatorMode() {
    let detector = TransitionDetector()
    XCTAssertTrue(detector.messages(from: snapshot(.green), to: snapshot(.green, open: 3), operatorMode: false).isEmpty)
    XCTAssertEqual(detector.messages(from: snapshot(.green), to: snapshot(.green, open: 3), operatorMode: true).count, 1)
  }
}
