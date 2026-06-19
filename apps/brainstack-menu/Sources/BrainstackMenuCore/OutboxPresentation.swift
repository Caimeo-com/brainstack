import Foundation

public struct OutboxTerminalReason: Equatable, Sendable {
  public let message: String
  public let count: Int

  public init(message: String, count: Int) {
    self.message = message
    self.count = count
  }
}

public struct OutboxStatusSummary: Equatable, Sendable {
  public let queued: Int
  public let terminal: Int
  public let corrupt: Int
  public let terminalReasons: [OutboxTerminalReason]

  public init(section: StatusSection) {
    self.queued = Self.integer(from: section.data?["queued"]) ?? Self.integer(named: "queued", in: section.detail) ?? 0
    self.terminal = Self.integer(from: section.data?["terminal"]) ?? Self.integer(named: "terminal", in: section.detail) ?? 0
    self.corrupt = Self.integer(from: section.data?["corrupt"]) ?? Self.integer(named: "corrupt", in: section.detail) ?? 0
    self.terminalReasons = section.data?["terminal_errors"]?.arrayValue?.compactMap(Self.reason(from:)) ?? []
  }

  public var hasSavedWrites: Bool {
    queued > 0 || terminal > 0 || corrupt > 0
  }

  public var needsRetry: Bool {
    terminal > 0 && corrupt == 0
  }

  public var attentionTitle: String {
    if corrupt > 0 {
      return "Saved writes need cleanup"
    }
    if terminal > 0 {
      return "Saved writes need review"
    }
    if queued > 0 {
      return "Saved writes are waiting"
    }
    return "Outbox is clear"
  }

  public var userMessage: String {
    if corrupt > 0 {
      return plural(corrupt, "saved write file is damaged", "saved write files are damaged") + ". Review or discard the damaged files before Brainstack can safely send the queue."
    }
    if terminal > 0 {
      let prefix = plural(terminal, "saved write could not be sent", "saved writes could not be sent")
      if let reason = terminalReasons.first {
        return "\(prefix). \(remediation(for: reason.message))"
      }
      return "\(prefix). Brainstack paused automatic retries to avoid repeating a bad write. Repair the cause, then retry; discard only if the saved writes are obsolete."
    }
    if queued > 0 {
      return plural(queued, "saved write is waiting", "saved writes are waiting") + " to be sent to the shared brain."
    }
    return "No saved writes are waiting."
  }

  public var compactMessage: String {
    if corrupt > 0 {
      return plural(corrupt, "damaged saved write", "damaged saved writes")
    }
    if terminal > 0 {
      if let reason = terminalReasons.first {
        return "\(terminal) paused: \(friendlyReason(reason.message))"
      }
      return plural(terminal, "paused saved write", "paused saved writes")
    }
    if queued > 0 {
      return plural(queued, "saved write waiting", "saved writes waiting")
    }
    return "no saved writes waiting"
  }

  private static func reason(from value: JSONValue) -> OutboxTerminalReason? {
    guard let message = value["message"]?.stringValue, !message.isEmpty else {
      return nil
    }
    let count = value["count"]?.numberValue.map(Int.init) ?? 1
    return OutboxTerminalReason(message: message, count: count)
  }

  private static func integer(from value: JSONValue?) -> Int? {
    value?.numberValue.map(Int.init)
  }

  private static func integer(named name: String, in text: String) -> Int? {
    let pattern = "\(NSRegularExpression.escapedPattern(for: name))=(\\d+)"
    guard let regex = try? NSRegularExpression(pattern: pattern) else {
      return nil
    }
    let range = NSRange(text.startIndex..<text.endIndex, in: text)
    guard let match = regex.firstMatch(in: text, range: range), match.numberOfRanges > 1,
          let valueRange = Range(match.range(at: 1), in: text) else {
      return nil
    }
    return Int(text[valueRange])
  }
}

public struct OutboxActionGuide: Equatable, Sendable {
  public let summary: String
  public let output: String
}

public enum OutboxActionPresentation {
  public static func guide(title: String, succeeded: Bool, rawOutput: String) -> OutboxActionGuide? {
    guard title.lowercased().contains("outbox") || title.lowercased().contains("saved writes") else {
      return nil
    }
    let flushed = integer(named: "flushed", in: rawOutput) ?? 0
    let kept = integer(named: "kept", in: rawOutput) ?? 0
    let terminal = integer(named: "terminal_failures", in: rawOutput) ?? integer(named: "terminal", in: rawOutput) ?? 0
    let corrupt = integer(named: "corrupt", in: rawOutput) ?? 0
    let purged = integer(named: "purged", in: rawOutput) ?? 0
    let purgedCorrupt = integer(named: "purged_corrupt", in: rawOutput) ?? 0
    let reason = terminalReason(in: rawOutput)

    if title.lowercased().contains("discard") && succeeded {
      if purgedCorrupt > 0 {
        return OutboxActionGuide(
          summary: "Damaged saved writes discarded.",
          output: "Brainstack deleted \(plural(purgedCorrupt, "damaged saved write file", "damaged saved write files"))."
        )
      }
      if purged > 0 {
        return OutboxActionGuide(
          summary: "Saved writes discarded.",
          output: "Brainstack deleted \(plural(purged, "local saved-write queue", "local saved-write queues"))."
        )
      }
      return OutboxActionGuide(summary: "No saved writes were waiting.", output: "The outbox is already clear.")
    }

    if corrupt > 0 {
      return OutboxActionGuide(
        summary: "Saved writes need cleanup.",
        output: "Brainstack found \(plural(corrupt, "damaged saved write file", "damaged saved write files")). It did not send the queue.\n\nNext: review the saved writes, then discard damaged files only if they are unrecoverable."
      )
    }
    if terminal > 0 {
      let reasonLine = reason.map { " \(remediation(for: $0))" } ?? " Repair the cause, then retry saved writes."
      return OutboxActionGuide(
        summary: "Saved writes still need attention.",
        output: "Brainstack did not send \(plural(terminal, "saved write", "saved writes")).\(reasonLine)\n\nDiscard them only if those imports or memories are obsolete."
      )
    }
    if succeeded {
      if flushed > 0 {
        return OutboxActionGuide(
          summary: "Saved writes sent.",
          output: "Brainstack sent \(plural(flushed, "saved write", "saved writes")) to the shared brain."
        )
      }
      if kept > 0 {
        return OutboxActionGuide(
          summary: "Saved writes are still waiting.",
          output: "\(plural(kept, "saved write is", "saved writes are")) still waiting. Brainstack will try again later, or you can retry once connectivity and credentials look correct."
        )
      }
      return OutboxActionGuide(summary: "No saved writes were waiting.", output: "The outbox is already clear.")
    }
    return nil
  }

  private static func terminalReason(in text: String) -> String? {
    if let line = text.split(whereSeparator: \.isNewline).first(where: { $0.hasPrefix("terminal_reasons=") }) {
      return String(line.dropFirst("terminal_reasons=".count))
    }
    if text.range(of: "HTTP 401", options: .caseInsensitive) != nil {
      return "HTTP 401 unauthorized"
    }
    if text.range(of: "HTTP 403", options: .caseInsensitive) != nil {
      return "HTTP 403 forbidden"
    }
    if text.range(of: "HTTP 425", options: .caseInsensitive) != nil {
      return "HTTP 425 idempotency review"
    }
    return nil
  }

  private static func integer(named name: String, in text: String) -> Int? {
    let pattern = "\(NSRegularExpression.escapedPattern(for: name))=(\\d+)"
    guard let regex = try? NSRegularExpression(pattern: pattern) else {
      return nil
    }
    let range = NSRange(text.startIndex..<text.endIndex, in: text)
    guard let match = regex.firstMatch(in: text, range: range), match.numberOfRanges > 1,
          let valueRange = Range(match.range(at: 1), in: text) else {
      return nil
    }
    return Int(text[valueRange])
  }
}

private func plural(_ count: Int, _ singular: String, _ plural: String) -> String {
  "\(count) \(count == 1 ? singular : plural)"
}

private func friendlyReason(_ reason: String) -> String {
  let lower = reason.lowercased()
  if lower.contains("http 401") || lower.contains("unauthorized") {
    return "authorization failed"
  }
  if lower.contains("http 403") || lower.contains("forbidden") {
    return "the shared brain rejected this client"
  }
  if lower.contains("http 425") || lower.contains("idempotency") {
    return "Brainstack needs an operator review before replaying this write"
  }
  return reason
}

private func remediation(for reason: String) -> String {
  let lower = reason.lowercased()
  if lower.contains("http 401") || lower.contains("unauthorized") {
    return "Brainstack rejected this Mac's import credential. Re-enroll this Mac or refresh its import token, then retry; discard only if the saved writes are obsolete."
  }
  if lower.contains("http 403") || lower.contains("forbidden") {
    return "The shared brain rejected this client. Check whether this Mac is allowed to write, then retry; discard only if the saved writes are obsolete."
  }
  if lower.contains("http 425") || lower.contains("idempotency") {
    return "Brainstack needs operator review before replaying this write. Review the matching proposal/idempotency record, then retry or discard it."
  }
  return "Cause: \(friendlyReason(reason)). Repair that cause, then retry; discard only if the saved writes are obsolete."
}
