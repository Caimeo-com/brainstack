import Foundation

/// Folder-pack manifest summary parsed from `brainctl context-packs list --json`.
public struct ContextPackSummary: Identifiable, Sendable, Equatable {
  public let id: String
  public let name: String
  public let safeName: String
  public let machine: String
  public let sourceMachine: String
  public let sourceRoot: String
  public let contentPath: String
  public let manifestPath: String
  public let treePath: String
  public let fileCount: Int
  public let totalBytes: Int
  public let freeSpaceBytes: Int?
  public let freshness: String
  public let refreshedAt: String
  public let warnings: [String]

  public init(json: JSONValue) {
    self.id = json["id"]?.stringValue ?? ""
    self.name = json["name"]?.stringValue ?? ""
    self.safeName = json["safe_name"]?.stringValue ?? name
    self.machine = json["machine"]?.stringValue ?? ""
    self.sourceMachine = json["source_machine"]?.stringValue ?? ""
    self.sourceRoot = json["source_root"]?.stringValue ?? ""
    self.contentPath = json["content_path"]?.stringValue ?? ""
    self.manifestPath = json["manifest_path"]?.stringValue ?? ""
    self.treePath = json["tree_path"]?.stringValue ?? ""
    self.fileCount = Int(json["file_count"]?.numberValue ?? 0)
    self.totalBytes = Int(json["total_bytes"]?.numberValue ?? 0)
    if let value = json["free_space_bytes"]?.numberValue {
      self.freeSpaceBytes = Int(value)
    } else {
      self.freeSpaceBytes = nil
    }
    self.freshness = json["freshness"]?.stringValue ?? ""
    self.refreshedAt = json["refreshed_at"]?.stringValue ?? ""
    self.warnings = json["warnings"]?.arrayValue?.compactMap(\.stringValue) ?? []
  }

  public var displayName: String {
    name.isEmpty ? safeName : name
  }

  public var formattedSize: String {
    Self.formatBytes(totalBytes)
  }

  public var formattedFreeSpace: String {
    guard let freeSpaceBytes else {
      return "unknown"
    }
    return Self.formatBytes(freeSpaceBytes)
  }

  public static func formatBytes(_ value: Int) -> String {
    let bytes = Double(value)
    if bytes >= 1024 * 1024 * 1024 {
      return String(format: "%.2f GiB", bytes / (1024 * 1024 * 1024))
    }
    if bytes >= 1024 * 1024 {
      return String(format: bytes >= 10 * 1024 * 1024 ? "%.1f MiB" : "%.2f MiB", bytes / (1024 * 1024))
    }
    if bytes >= 1024 {
      return String(format: bytes >= 10 * 1024 ? "%.1f KiB" : "%.2f KiB", bytes / 1024)
    }
    return "\(value) bytes"
  }

  public static func parseList(_ text: String) -> [ContextPackSummary]? {
    guard let data = text.data(using: .utf8),
          let decoded = try? JSONDecoder().decode(JSONValue.self, from: data),
          let packs = decoded["packs"]?.arrayValue else {
      return nil
    }
    return packs.map(ContextPackSummary.init(json:)).filter { !$0.safeName.isEmpty && !$0.contentPath.isEmpty }
  }

  public static func parseSingle(_ text: String) -> ContextPackSummary? {
    guard let data = text.data(using: .utf8),
          let decoded = try? JSONDecoder().decode(JSONValue.self, from: data),
          let pack = decoded["pack"] else {
      return nil
    }
    let summary = ContextPackSummary(json: pack)
    return summary.safeName.isEmpty || summary.contentPath.isEmpty ? nil : summary
  }
}
