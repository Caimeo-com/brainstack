import Foundation

/// Removes token-like values from text destined for diagnostics or pasteboard.
/// Mirrors the redaction families used by telemux log tails: provider keys, bearer
/// headers, env-style secret assignments, invites, JWTs, and long opaque blobs.
public enum Redaction {
  private static let secretAssignmentPattern =
    "(?i)([\"']?[A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|PASSWD|API_KEY|APIKEY|PRIVATE" +
    "_KEY|CREDENTIALS?)[A-Z0-9_]*[\"']?\\s*[=:]\\s*)(\"[^\"]*\"|'[^']*'|\\S+)"
  private static let pemPrivateKeyPattern =
    "-----BEGIN [A-Z ]*PRIVATE " +
    "KEY-----[\\s\\S]*?-----END [A-Z ]*PRIVATE " +
    "KEY-----"

  private static let patterns: [(pattern: String, replacement: String)] = [
    // Telegram bot tokens.
    ("\\b\\d{6,12}:[A-Za-z0-9_-]{30,}\\b", "[redacted-telegram-token]"),
    // Dashed and underscored provider keys.
    ("\\b(sk|rk|pk)-[A-Za-z0-9_-]{16,}\\b", "[redacted-api-key]"),
    ("\\b(sk|rk|pk|whsec)_(?:live|test|prod)?_?[A-Za-z0-9]{16,}\\b", "[redacted-api-key]"),
    ("\\bgh[pousr]_[A-Za-z0-9]{20,}\\b", "[redacted-github-token]"),
    ("\\bxox" + "[baprs]-[A-Za-z0-9-]{10,}\\b", "[redacted-slack-token]"),
    // Brainstack invites embed bearer import tokens.
    ("\\bbs1_[A-Za-z0-9+/=_-]{16,}\\b", "[redacted-invite]"),
    ("\\bAKIA[0-9A-Z]{16}\\b", "[redacted-aws-key]"),
    // JWTs.
    ("\\beyJ[A-Za-z0-9_-]{20,}\\.[A-Za-z0-9_-]{10,}\\.[A-Za-z0-9_-]{10,}\\b", "[redacted-jwt]"),
    // KEY=value / "key": "value" assignments whose key looks secret-bearing.
    (secretAssignmentPattern, "$1[redacted]"),
    ("(?i)(authorization:\\s*bearer\\s+)\\S+", "$1[redacted]"),
    (pemPrivateKeyPattern, "[redacted-private-key]"),
    // Long opaque hex/base64 blobs (40+ chars) that look like raw secrets.
    ("\\b[a-f0-9]{40,}\\b", "[redacted-hex]"),
    ("\\b[A-Za-z0-9+/]{48,}={0,2}\\b", "[redacted-blob]")
  ]

  public static func redact(_ text: String) -> String {
    var output = text
    for entry in patterns {
      guard let regex = try? NSRegularExpression(pattern: entry.pattern, options: []) else {
        continue
      }
      let range = NSRange(output.startIndex..<output.endIndex, in: output)
      output = regex.stringByReplacingMatches(in: output, options: [], range: range, withTemplate: entry.replacement)
    }
    return output
  }
}
