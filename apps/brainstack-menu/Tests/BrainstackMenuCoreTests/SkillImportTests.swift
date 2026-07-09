import XCTest
@testable import BrainstackMenuCore

final class SkillImportTests: XCTestCase {
  func testParseImportPlanIndexesCandidatesForCliSelection() {
    let json = """
    {
      "note": "Review candidates and rerun with --select.",
      "repo": "/Users/operator/shared-brain",
      "proposed": [
        {
          "name": "be-thorough",
          "description": "High-rigor completion workflow.",
          "root": "/Users/operator/.codex/skills/be-thorough",
          "sources": ["arg"],
          "action": "install",
          "file_count": 2,
          "total_bytes": 4096
        },
        {
          "name": "brainstack",
          "description": "",
          "root": "/Users/operator/.codex/skills/brainstack",
          "sources": ["codex"],
          "action": "update",
          "file_count": 1,
          "total_bytes": 1024
        }
      ],
      "skipped": [
        {"name": "old-skill", "root": "/tmp/old-skill", "reason": "already current"}
      ],
      "rejected": [
        {"root": "/tmp/bad", "error": "missing SKILL.md"}
      ],
      "warnings": ["one warning"],
      "applied": []
    }
    """

    let plan = SkillImportPlan.parse(json)

    XCTAssertEqual(plan?.repo, "/Users/operator/shared-brain")
    XCTAssertEqual(plan?.proposed.map(\.id), [1, 2])
    XCTAssertEqual(plan?.proposed[0].name, "be-thorough")
    XCTAssertEqual(plan?.proposed[0].fileCount, 2)
    XCTAssertEqual(plan?.proposed[1].displayDescription, "No description provided.")
    XCTAssertEqual(plan?.skipped.first?.reason, "already current")
    XCTAssertEqual(plan?.rejected.first?.error, "missing SKILL.md")
    XCTAssertEqual(plan?.warnings, ["one warning"])
  }

  func testInvalidImportPlanReturnsNil() {
    XCTAssertNil(SkillImportPlan.parse("not json"))
  }
}
