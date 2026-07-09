import XCTest
@testable import BrainstackMenuCore

final class ContextPacksTests: XCTestCase {
  func testParseContextPackList() {
    let json = """
    {
      "ok": true,
      "machine": "erbine",
      "packs": [
        {
          "schema_version": 1,
          "kind": "brainstack.context_pack",
          "id": "cp_docs_fixture",
          "name": "docs",
          "safe_name": "docs",
          "machine": "erbine",
          "source_machine": "Brunos-MAKINA",
          "source_root": "/Users/operator/docs",
          "content_path": "/home/operator/.local/state/brainstack/context-packs/docs/current",
          "manifest_path": "/home/operator/.local/state/brainstack/context-packs/docs/manifest.json",
          "tree_path": "/home/operator/.local/state/brainstack/context-packs/docs/tree.jsonl",
          "file_count": 42,
          "total_bytes": 1048576,
          "free_space_bytes": 2097152,
          "freshness": "fresh",
          "refreshed_at": "2026-07-08T00:00:00Z",
          "warnings": ["large pack"]
        }
      ]
    }
    """

    let packs = ContextPackSummary.parseList(json)

    XCTAssertEqual(packs?.count, 1)
    XCTAssertEqual(packs?.first?.displayName, "docs")
    XCTAssertEqual(packs?.first?.machine, "erbine")
    XCTAssertEqual(packs?.first?.fileCount, 42)
    XCTAssertEqual(packs?.first?.formattedSize, "1.00 MiB")
    XCTAssertEqual(packs?.first?.formattedFreeSpace, "2.00 MiB")
    XCTAssertEqual(packs?.first?.freshness, "fresh")
    XCTAssertEqual(packs?.first?.warnings, ["large pack"])
  }

  func testParseSingleContextPackPreflight() {
    let json = """
    {
      "ok": true,
      "dry_run": true,
      "pack": {
        "id": "cp_docs",
        "name": "docs",
        "safe_name": "docs",
        "machine": "erbine",
        "source_machine": "mac",
        "source_root": "/Users/operator/docs",
        "content_path": "/home/operator/.local/state/brainstack/context-packs/docs/current",
        "manifest_path": "/home/operator/.local/state/brainstack/context-packs/docs/manifest.json",
        "tree_path": "/home/operator/.local/state/brainstack/context-packs/docs/tree.jsonl",
        "file_count": 1,
        "total_bytes": 5,
        "free_space_bytes": null,
        "freshness": "unknown",
        "warnings": []
      }
    }
    """
    let pack = ContextPackSummary.parseSingle(json)
    XCTAssertEqual(pack?.safeName, "docs")
    XCTAssertEqual(pack?.freshness, "unknown")
    XCTAssertEqual(pack?.formattedFreeSpace, "unknown")
  }

  func testInvalidContextPackListReturnsNil() {
    XCTAssertNil(ContextPackSummary.parseList("not json"))
  }
}
