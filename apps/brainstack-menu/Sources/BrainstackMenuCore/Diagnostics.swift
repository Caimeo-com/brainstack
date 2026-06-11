import Foundation

/// Assembles the redacted "Copy Diagnostics" payload. May include local paths and
/// machine names; must never include token-like values.
public enum Diagnostics {
  public static func bundle(
    appVersion: String,
    binaryPath: String,
    configPath: String,
    lastReport: StatusReport?,
    stale: Bool,
    lastFailure: StatusFailure?,
    lastDurations: [String: TimeInterval],
    lastActionOutputs: [String]
  ) -> String {
    var lines: [String] = []
    lines.append("Brainstack Menu diagnostics")
    lines.append("app_version=\(appVersion)")
    lines.append("brainctl=\(binaryPath)")
    lines.append("config=\(configPath)")
    lines.append("generated_at=\(ISO8601DateFormatter().string(from: Date()))")
    lines.append("stale=\(stale)")
    if let lastFailure {
      lines.append("last_failure=\(describe(lastFailure))")
    }
    if !lastDurations.isEmpty {
      let durations = lastDurations
        .sorted { $0.key < $1.key }
        .map { "\($0.key)=\(String(format: "%.0fms", $0.value * 1000))" }
        .joined(separator: " ")
      lines.append("command_durations: \(durations)")
    }
    if !lastActionOutputs.isEmpty {
      lines.append("")
      lines.append("recent actions:")
      lines.append(contentsOf: lastActionOutputs.suffix(5))
    }
    if let lastReport {
      lines.append("")
      lines.append("status json:")
      if let data = try? JSONEncoder().encode(lastReport.raw),
         let object = try? JSONSerialization.jsonObject(with: data),
         let pretty = try? JSONSerialization.data(withJSONObject: object, options: [.prettyPrinted, .sortedKeys]),
         let text = String(data: pretty, encoding: .utf8) {
        lines.append(text)
      }
    }
    return Redaction.redact(lines.joined(separator: "\n"))
  }

  public static func describe(_ failure: StatusFailure) -> String {
    switch failure {
    case .binaryMissing(let path):
      return "brainctl missing at \(path)"
    case .unsupportedBinary(let path):
      return "installed brainctl at \(path) is too old (no status command); update Brainstack"
    case .timedOut:
      return "status command timed out"
    case .commandFailed(let exitCode, let stderr):
      return "status command failed (exit \(exitCode.map(String.init) ?? "n/a")): \(stderr.prefix(400))"
    case .parseFailed(let detail, let stderr):
      return "status JSON parse failed: \(detail.prefix(200)); stderr: \(stderr.prefix(400))"
    }
  }
}
