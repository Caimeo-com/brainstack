import Foundation

/// Mirror of the `brainctl status --json` section state values. Unknown future
/// states map to `.unknown` and render as degraded rather than crashing.
public enum SectionState: String, Sendable {
  case ok
  case warn
  case fail
  case disabled
  case unknown

  public init(raw: String?) {
    self = SectionState(rawValue: (raw ?? "").lowercased()) ?? .unknown
  }
}

/// One status section, decoded generically per the CLI contract:
/// `state`, `ok`, `available`, `detail`, `data`, `error`, `duration_ms`.
public struct StatusSection: Sendable {
  public let state: SectionState
  public let ok: Bool?
  public let available: Bool
  public let detail: String
  public let data: JSONValue?
  public let error: String?
  public let durationMs: Double?

  public init(json: JSONValue) {
    self.state = SectionState(raw: json["state"]?.stringValue)
    self.ok = json["ok"]?.boolValue
    self.available = json["available"]?.boolValue ?? (self.state == .ok || self.state == .warn)
    self.detail = json["detail"]?.stringValue ?? ""
    self.data = json["data"]
    self.error = json["error"]?.stringValue
    self.durationMs = json["duration_ms"]?.numberValue
  }
}

public struct FleetMachineSummary: Identifiable, Equatable, Sendable {
  public let name: String
  public let role: String
  public let transport: String
  public let reachable: Bool
  public let status: String
  public let updateState: String
  public let needsUpdate: Bool
  public let detail: String
  public let short: String?
  public let branch: String?
  public let behind: Int?
  public let ahead: Int?
  public let dirtyCount: Int?

  public var id: String { name }

  public init?(json: JSONValue) {
    guard let name = json["name"]?.stringValue, !name.isEmpty else {
      return nil
    }
    self.name = name
    self.role = json["role"]?.stringValue ?? "machine"
    self.transport = json["transport"]?.stringValue ?? "unknown"
    self.reachable = json["reachable"]?.boolValue ?? false
    self.status = json["status"]?.stringValue ?? "unknown"
    self.updateState = json["update_state"]?.stringValue ?? "unknown"
    self.needsUpdate = json["needs_update"]?.boolValue ?? false
    self.detail = json["detail"]?.stringValue ?? ""
    self.short = json["short"]?.stringValue
    self.branch = json["branch"]?.stringValue
    self.behind = json["behind"]?.numberValue.map(Int.init)
    self.ahead = json["ahead"]?.numberValue.map(Int.init)
    self.dirtyCount = json["dirty_count"]?.numberValue.map(Int.init)
  }
}

/// Parsed aggregate status report. Section order is preserved for rendering, and
/// sections the app does not know about are kept and rendered generically.
public struct StatusReport: Sendable {
  public let generatedAt: String?
  public let configPath: String?
  public let profile: String?
  public let machine: String?
  public let ok: Bool
  public let degraded: Bool
  public let sectionNames: [String]
  public let sections: [String: StatusSection]
  public let raw: JSONValue

  public init(json: JSONValue) throws {
    guard let object = json.objectValue else {
      throw StatusParseError.notAnObject
    }
    self.generatedAt = object["generated_at"]?.stringValue
    self.configPath = object["config_path"]?.stringValue
    self.profile = object["profile"]?.stringValue
    self.machine = object["machine"]?.stringValue
    self.ok = object["ok"]?.boolValue ?? false
    self.degraded = object["degraded"]?.boolValue ?? true
    var names: [String] = []
    var parsed: [String: StatusSection] = [:]
    if let sectionsObject = object["sections"]?.objectValue {
      // Render in a stable, contract-first order; unknown sections follow alphabetically.
      let preferredOrder = ["config", "daemon", "tailscale", "shared_brain", "outbox", "hooks", "skills", "brain_api", "curator", "proposals", "telemux", "fleet", "control_source", "product"]
      let known = preferredOrder.filter { sectionsObject.keys.contains($0) }
      let unknown = sectionsObject.keys.filter { !preferredOrder.contains($0) }.sorted()
      names = known + unknown
      for name in names {
        if let sectionJson = sectionsObject[name] {
          parsed[name] = StatusSection(json: sectionJson)
        }
      }
    }
    self.sectionNames = names
    self.sections = parsed
    self.raw = json
  }

  public static func parse(data: Data) throws -> StatusReport {
    let decoded: JSONValue
    do {
      decoded = try JSONDecoder().decode(JSONValue.self, from: data)
    } catch {
      throw StatusParseError.invalidJson(String(describing: error))
    }
    return try StatusReport(json: decoded)
  }
}

public extension StatusReport {
  var fleetMachines: [FleetMachineSummary] {
    sections["fleet"]?.data?["machines"]?.arrayValue?.compactMap(FleetMachineSummary.init(json:)) ?? []
  }
}

public enum StatusParseError: Error, Equatable {
  case notAnObject
  case invalidJson(String)
}

/// Menu bar traffic-light state.
public enum OverallState: String, Sendable {
  /// Healthy, no degraded sections.
  case green
  /// Degraded but usable.
  case yellow
  /// Broken local setup.
  case red
  /// brainctl missing/unusable, timed out, or status unparseable.
  case gray
}

public enum OverallStateMapper {
  /// Derive the icon state from sections, not just the aggregate booleans, so a
  /// single failing section is visibly red even when most sections are fine.
  public static func map(report: StatusReport?) -> OverallState {
    guard let report else {
      return .gray
    }
    let states = report.sectionNames.compactMap { report.sections[$0]?.state }
    if states.isEmpty {
      return .gray
    }
    if states.contains(.fail) {
      return .red
    }
    if states.contains(.warn) || states.contains(.unknown) || report.degraded {
      return .yellow
    }
    return .green
  }
}
