import Foundation

/// Upload manifest summary parsed from `brainctl uploads list --json`.
public struct UploadSummary: Identifiable, Sendable, Equatable {
  public let id: String
  public let machine: String
  public let source: String
  public let originalName: String
  public let fileName: String
  public let label: String?
  public let sizeBytes: Int
  public let sha256: String
  public let uploadedAt: String
  public let remotePath: String
  public let manifestPath: String

  public init(json: JSONValue) {
    self.id = json["id"]?.stringValue ?? ""
    self.machine = json["machine"]?.stringValue ?? ""
    self.source = json["source"]?.stringValue ?? ""
    self.originalName = json["original_name"]?.stringValue ?? ""
    self.fileName = json["file_name"]?.stringValue ?? originalName
    self.label = json["label"]?.stringValue
    self.sizeBytes = Int(json["size_bytes"]?.numberValue ?? 0)
    self.sha256 = json["sha256"]?.stringValue ?? ""
    self.uploadedAt = json["uploaded_at"]?.stringValue ?? ""
    self.remotePath = json["remote_path"]?.stringValue ?? ""
    self.manifestPath = json["manifest_path"]?.stringValue ?? ""
  }

  public var displayName: String {
    fileName.isEmpty ? originalName : fileName
  }

  public var formattedSize: String {
    let bytes = Double(sizeBytes)
    if bytes >= 1024 * 1024 {
      return String(format: bytes >= 10 * 1024 * 1024 ? "%.1f MiB" : "%.2f MiB", bytes / (1024 * 1024))
    }
    if bytes >= 1024 {
      return String(format: bytes >= 10 * 1024 ? "%.1f KiB" : "%.2f KiB", bytes / 1024)
    }
    return "\(sizeBytes) bytes"
  }

  public static func parseList(_ text: String) -> [UploadSummary]? {
    guard let data = text.data(using: .utf8),
          let decoded = try? JSONDecoder().decode(JSONValue.self, from: data),
          let uploads = decoded["uploads"]?.arrayValue else {
      return nil
    }
    return uploads.map(UploadSummary.init(json:)).filter { !$0.id.isEmpty && !$0.remotePath.isEmpty }
  }
}
