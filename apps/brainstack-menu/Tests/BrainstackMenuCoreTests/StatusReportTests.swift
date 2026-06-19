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

  func testProposalSummaryFallbackMapsGreen() throws {
    let json = """
    {"ok": true, "degraded": false, "sections": {
      "config": {"state": "ok", "ok": true, "available": true, "detail": "config loaded"},
      "curator": {"state": "ok", "ok": true, "available": true, "detail": "mode=approval installed=true open_proposals=3", "data": {"open_proposals": 3, "curator": {"installed": true}}},
      "proposals": {"state": "ok", "ok": true, "available": true, "detail": "open_proposals=3 (proposal list refresh slow; using curator summary)", "data": {"count": 3, "list_available": false, "fallback_source": "curator", "list_error": "The operation timed out."}}
    }}
    """
    let report = try StatusReport.parse(data: Data(json.utf8))
    XCTAssertEqual(report.sections["proposals"]?.state, .ok)
    XCTAssertEqual(report.sections["proposals"]?.data?["list_available"]?.boolValue, false)
    XCTAssertEqual(OverallStateMapper.map(report: report), .green)
  }

  func testUnknownSectionsAndStatesDoNotCrash() throws {
    let report = try StatusReport.parse(data: fixture("status-unknown-sections"))
    XCTAssertTrue(report.sectionNames.contains("quantum_sync"))
    XCTAssertTrue(report.sectionNames.contains("future_thing"))
    XCTAssertEqual(report.sections["future_thing"]?.state, .unknown)
    // Unknown states render as degraded, never green or crash.
    XCTAssertEqual(OverallStateMapper.map(report: report), .yellow)
  }

  func testFleetMachinesParseFromStatusSection() throws {
    let json = """
    {"ok": false, "degraded": true, "sections": {
      "config": {"state": "ok", "ok": true, "available": true, "detail": "config loaded"},
      "fleet": {"state": "warn", "ok": false, "available": true, "detail": "machines=2 reachable=2 needs_update=1 unhealthy=1", "data": {
        "machines": [
          {"name": "valkyrie", "role": "control", "transport": "local", "reachable": true, "status": "ok", "update_state": "current", "needs_update": false, "detail": "current head=abc", "short": "abc"},
          {"name": "yoda", "role": "worker", "transport": "ssh", "reachable": true, "status": "warn", "update_state": "behind", "needs_update": true, "detail": "behind origin by 1 commit", "short": "def", "behind": 1, "dirty_count": 0}
        ]
      }}
    }}
    """
    let report = try StatusReport.parse(data: Data(json.utf8))
    XCTAssertEqual(report.sectionNames, ["config", "fleet"])
    XCTAssertEqual(report.fleetMachines.count, 2)
    XCTAssertEqual(report.fleetMachines[1].name, "yoda")
    XCTAssertEqual(report.fleetMachines[1].needsUpdate, true)
    XCTAssertEqual(report.fleetMachines[1].behind, 1)
    XCTAssertEqual(OverallStateMapper.map(report: report), .yellow)
  }

  func testReplacingFleetSectionPreservesOrderAndUpdatesRawJson() throws {
    let base = try StatusReport.parse(data: Data(#"{"ok": true, "degraded": false, "sections": {"config": {"state": "ok", "ok": true, "available": true, "detail": "config loaded"}, "daemon": {"state": "ok", "ok": true, "available": true, "detail": "running"}}}"#.utf8))
    let fleetData: JSONValue = .object([
      "machines": .array([
        .object([
          "name": .string("erbine"),
          "role": .string("worker"),
          "transport": .string("ssh"),
          "reachable": .bool(true),
          "status": .string("warn"),
          "update_state": .string("behind"),
          "needs_update": .bool(true),
          "detail": .string("behind origin by 2 commits"),
          "behind": .number(2)
        ])
      ]),
      "summary": .object([
        "total": .number(1),
        "reachable": .number(1),
        "needs_update": .number(1),
        "unhealthy": .number(1)
      ])
    ])
    let fleet = StatusSection(state: .warn, ok: false, available: true, detail: "machines=1 reachable=1 needs_update=1 unhealthy=1", data: fleetData, error: nil, durationMs: 1200)

    let merged = base.replacingSection("fleet", with: fleet)

    XCTAssertEqual(merged.sectionNames, ["config", "daemon", "fleet"])
    XCTAssertEqual(merged.sections["fleet"]?.state, .warn)
    XCTAssertEqual(merged.fleetMachines.first?.name, "erbine")
    XCTAssertFalse(merged.ok)
    XCTAssertTrue(merged.degraded)
    XCTAssertEqual(merged.raw["sections"]?["fleet"]?["data"]?["summary"]?["needs_update"]?.numberValue, 1)
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
      {"id": "p2", "title": "Second", "status": "needs-human", "target_page": null, "risk": null, "created_at": "2026-06-11T01:00:00Z", "legacy_format": true, "cluster_key": "slack-ea:needs-context:legacy-memory", "cluster_label": "Slack EA / needs-context / legacy-memory"}
    ]}
    """
    let proposals = ProposalSummary.parseList(json)
    XCTAssertEqual(proposals?.count, 2)
    XCTAssertEqual(proposals?[0].targetPage, "wiki/Status/A.md")
    XCTAssertNil(proposals?[1].targetPage)
    XCTAssertEqual(proposals?[1].legacyFormat, true)
    XCTAssertEqual(proposals?[1].clusterLabel, "Slack EA / needs-context / legacy-memory")
    XCTAssertNil(proposals?[0].qualityDecision)
    XCTAssertNil(ProposalSummary.parseList("not json"))
  }
}

final class ProposalDetailTests: XCTestCase {
  func testParseDetail() {
    let json = """
    {"ok": true,
     "proposal": {"id": "p1", "title": "T", "status": "pending", "target_page": "wiki/Status/A.md", "risk": "low", "confidence": 0.85, "created_at": "2026-06-11T00:00:00Z", "source_ids": ["a", "b"], "reason": "why", "source_harness": "codex", "source_machine": "mac", "source_type": "remember", "related_repo": "/repo/app", "project": "app", "domain": "product", "scope": "repo", "memory_kind": "project_lesson", "context": "during test", "applicability": "use for this app", "non_applicability": "do not use globally", "evidence_refs": ["repo:/repo/app"], "review_after": "2026-07-01", "expires_at": "2027-01-01", "quality_decision": "ready", "quality_score": 0.92, "quality_reasons": ["has context"], "legacy_format": true, "cluster_key": "app:repo:project_lesson", "cluster_label": "app / repo / project_lesson"},
     "body": "## Request\\n\\ncontent",
     "diff": "+ added line"}
    """
    let detail = ProposalDetail.parse(json)
    XCTAssertEqual(detail?.summary.id, "p1")
    XCTAssertEqual(detail?.diff, "+ added line")
    XCTAssertEqual(detail?.reason, "why")
    XCTAssertEqual(detail?.sourceIds, ["a", "b"])
    XCTAssertEqual(detail?.confidence, 0.85)
    XCTAssertEqual(detail?.sourceHarness, "codex")
    XCTAssertEqual(detail?.sourceMachine, "mac")
    XCTAssertEqual(detail?.sourceType, "remember")
    XCTAssertEqual(detail?.relatedRepo, "/repo/app")
    XCTAssertEqual(detail?.project, "app")
    XCTAssertEqual(detail?.domain, "product")
    XCTAssertEqual(detail?.scope, "repo")
    XCTAssertEqual(detail?.memoryKind, "project_lesson")
    XCTAssertEqual(detail?.context, "during test")
    XCTAssertEqual(detail?.applicability, "use for this app")
    XCTAssertEqual(detail?.nonApplicability, "do not use globally")
    XCTAssertEqual(detail?.evidenceRefs, ["repo:/repo/app"])
    XCTAssertEqual(detail?.reviewAfter, "2026-07-01")
    XCTAssertEqual(detail?.expiresAt, "2027-01-01")
    XCTAssertEqual(detail?.qualityDecision, "ready")
    XCTAssertEqual(detail?.qualityScore, 0.92)
    XCTAssertEqual(detail?.qualityReasons, ["has context"])
    XCTAssertEqual(detail?.legacyFormat, true)
    XCTAssertEqual(detail?.clusterKey, "app:repo:project_lesson")
    XCTAssertEqual(detail?.clusterLabel, "app / repo / project_lesson")
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
