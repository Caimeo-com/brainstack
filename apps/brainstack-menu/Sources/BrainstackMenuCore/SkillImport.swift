import Foundation

public struct SkillImportCandidate: Identifiable, Equatable, Sendable {
  public let id: Int
  public let name: String
  public let description: String
  public let root: String
  public let action: String
  public let fileCount: Int
  public let totalBytes: Int
  public let sources: [String]

  public init(index: Int, json: JSONValue) {
    self.id = index
    self.name = json["name"]?.stringValue ?? ""
    self.description = json["description"]?.stringValue ?? ""
    self.root = json["root"]?.stringValue ?? ""
    self.action = json["action"]?.stringValue ?? "install"
    self.fileCount = Int(json["file_count"]?.numberValue ?? 0)
    self.totalBytes = Int(json["total_bytes"]?.numberValue ?? 0)
    self.sources = json["sources"]?.arrayValue?.compactMap(\.stringValue) ?? []
  }

  public var formattedSize: String {
    ByteCountFormatter.string(fromByteCount: Int64(totalBytes), countStyle: .file)
  }

  public var displayDescription: String {
    description.isEmpty ? "No description provided." : description
  }
}

public struct SkillImportSkipped: Equatable, Sendable {
  public let name: String
  public let root: String
  public let reason: String

  public init(json: JSONValue) {
    self.name = json["name"]?.stringValue ?? ""
    self.root = json["root"]?.stringValue ?? ""
    self.reason = json["reason"]?.stringValue ?? "Already current or not importable."
  }
}

public struct SkillImportRejected: Equatable, Sendable {
  public let root: String
  public let error: String

  public init(json: JSONValue) {
    self.root = json["root"]?.stringValue ?? ""
    self.error = json["error"]?.stringValue ?? "Could not inspect this skill."
  }
}

public struct SkillImportPlan: Equatable, Sendable {
  public let note: String
  public let repo: String?
  public let proposed: [SkillImportCandidate]
  public let skipped: [SkillImportSkipped]
  public let rejected: [SkillImportRejected]
  public let warnings: [String]
  public let appliedCount: Int

  public init(json: JSONValue) {
    self.note = json["note"]?.stringValue ?? ""
    self.repo = json["repo"]?.stringValue
    self.proposed = (json["proposed"]?.arrayValue ?? [])
      .enumerated()
      .map { SkillImportCandidate(index: $0.offset + 1, json: $0.element) }
      .filter { !$0.name.isEmpty || !$0.root.isEmpty }
    self.skipped = (json["skipped"]?.arrayValue ?? []).map(SkillImportSkipped.init(json:))
    self.rejected = (json["rejected"]?.arrayValue ?? []).map(SkillImportRejected.init(json:))
    self.warnings = json["warnings"]?.arrayValue?.compactMap(\.stringValue) ?? []
    self.appliedCount = json["applied"]?.arrayValue?.count ?? 0
  }

  public static func parse(_ text: String) -> SkillImportPlan? {
    guard let data = text.data(using: .utf8),
          let decoded = try? JSONDecoder().decode(JSONValue.self, from: data) else {
      return nil
    }
    return SkillImportPlan(json: decoded)
  }
}
