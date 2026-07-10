import XCTest
@testable import BrainstackMenuCore

final class RedactionTests: XCTestCase {
  func testRedactsTokenLikeValues() {
    // Assemble realistic fixtures at runtime so source-distribution scanners do
    // not mistake the test data for credentials that were committed by accident.
    let githubToken = "ghp_" + "abcdefghijklmnopqrstuvwxyz123456"
    let telegramToken = "123456789" + ":" + "AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsaw1"
    let samples: [String: String] = [
      "Authorization: Bearer abcDEF123456secretvalue": "Bearer",
      "BRAIN_ADMIN_TOKEN=8f14e45fceea167a5a36dedd4bea2543aa758ff6cc9929d8efaf3279a3e9b414": "BRAIN_ADMIN_TOKEN",
      "\"import_token\": \"super-secret-value\"": "import_token",
      "sk-abc123def456ghi789jkl012": "sk-",
      "sk_" + "live_4242424242424242abcdef": "sk_live",
      githubToken: "ghp_",
      "xox" + "b-12345-abcdefghij-klmno": "xoxb",
      "bs1_eyJzY2hlbWFfdmVyc2lvbiI6MX0abcdefgh": "bs1_",
      telegramToken: "telegram",
      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U": "jwt"
    ]
    for (input, label) in samples {
      let output = Redaction.redact(input)
      XCTAssertTrue(output.contains("[redacted"), "expected \(label) sample to be redacted: \(output)")
    }
  }

  func testSecretValuesDisappear() {
    let secret = "8f14e45fceea167a5a36dedd4bea2543aa758ff6cc9929d8efaf3279a3e9b414"
    let output = Redaction.redact("BRAIN_IMPORT_TOKEN=\(secret)\nplain text stays")
    XCTAssertFalse(output.contains(secret))
    XCTAssertTrue(output.contains("plain text stays"))
  }

  func testKeepsPathsAndMachineNames() {
    let input = "config=/Users/operator/.config/brainstack/brainstack.yaml machine=mac-client profile=client-macos detail=queued=3"
    XCTAssertEqual(Redaction.redact(input), input)
  }
}
