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

  public init(
    state: SectionState,
    ok: Bool?,
    available: Bool,
    detail: String,
    data: JSONValue?,
    error: String?,
    durationMs: Double?
  ) {
    self.state = state
    self.ok = ok
    self.available = available
    self.detail = detail
    self.data = data
    self.error = error
    self.durationMs = durationMs
  }

  public init(json: JSONValue) {
    self.state = SectionState(raw: json["state"]?.stringValue)
    self.ok = json["ok"]?.boolValue
    self.available = json["available"]?.boolValue ?? (self.state == .ok || self.state == .warn)
    self.detail = json["detail"]?.stringValue ?? ""
    self.data = json["data"]
    self.error = json["error"]?.stringValue
    self.durationMs = json["duration_ms"]?.numberValue
  }

  public var jsonValue: JSONValue {
    var object: [String: JSONValue] = [
      "state": .string(state.rawValue),
      "available": .bool(available),
      "detail": .string(detail)
    ]
    if let ok {
      object["ok"] = .bool(ok)
    }
    if let data {
      object["data"] = data
    }
    if let error {
      object["error"] = .string(error)
    }
    if let durationMs {
      object["duration_ms"] = .number(durationMs)
    }
    return .object(object)
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

  public var isReachableCurrentCleanControlHost: Bool {
    let normalizedRole = role.lowercased()
    let normalizedStatus = status.lowercased()
    let normalizedUpdateState = updateState.lowercased()
    let clean = (dirtyCount ?? 0) == 0
    return normalizedRole == "control"
      && reachable
      && normalizedStatus == "ok"
      && !needsUpdate
      && clean
      && ["current", "up-to-date", "up_to_date"].contains(normalizedUpdateState)
  }

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

  private static let preferredSectionOrder = ["config", "daemon", "tailscale", "shared_brain", "outbox", "hooks", "skills", "brain_api", "curator", "proposals", "telemux", "fleet", "control_source", "product"]

  public init(
    generatedAt: String?,
    configPath: String?,
    profile: String?,
    machine: String?,
    ok: Bool,
    degraded: Bool,
    sectionNames: [String],
    sections: [String: StatusSection],
    raw: JSONValue
  ) {
    self.generatedAt = generatedAt
    self.configPath = configPath
    self.profile = profile
    self.machine = machine
    self.ok = ok
    self.degraded = degraded
    self.sectionNames = sectionNames
    self.sections = sections
    self.raw = raw
  }

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
      names = Self.orderedSectionNames(Array(sectionsObject.keys))
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

  public func replacingSection(_ name: String, with section: StatusSection, rawSection: JSONValue? = nil) -> StatusReport {
    var nextSections = sections
    nextSections[name] = section
    let nextNames = Self.orderedSectionNames(Array(nextSections.keys))
    let states = nextNames.compactMap { nextSections[$0]?.state }
    let nextOk = !states.isEmpty && !states.contains(.warn) && !states.contains(.fail) && !states.contains(.unknown)
    var nextRaw = raw
    if case .object(var root) = nextRaw {
      var rawSections = root["sections"]?.objectValue ?? [:]
      rawSections[name] = rawSection ?? section.jsonValue
      root["sections"] = .object(rawSections)
      root["ok"] = .bool(nextOk)
      root["degraded"] = .bool(!nextOk)
      nextRaw = .object(root)
    }
    return StatusReport(
      generatedAt: generatedAt,
      configPath: configPath,
      profile: profile,
      machine: machine,
      ok: nextOk,
      degraded: !nextOk,
      sectionNames: nextNames,
      sections: nextSections,
      raw: nextRaw
    )
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

  private static func orderedSectionNames(_ keys: [String]) -> [String] {
    let keySet = Set(keys)
    let known = preferredSectionOrder.filter { keySet.contains($0) }
    let unknown = keys.filter { !preferredSectionOrder.contains($0) }.sorted()
    return known + unknown
  }
}

public extension StatusReport {
  var fleetMachines: [FleetMachineSummary] {
    sections["fleet"]?.data?["machines"]?.arrayValue?.compactMap(FleetMachineSummary.init(json:)) ?? []
  }

  var reachableCurrentControlHostFromFleet: FleetMachineSummary? {
    fleetMachines.first(where: \.isReachableCurrentCleanControlHost)
  }

  func isBenignControlSourceProbeWarning(name: String) -> Bool {
    guard name == "control_source",
          let section = sections[name],
          section.state == .warn,
          reachableCurrentControlHostFromFleet != nil else {
      return false
    }
    let combined = "\(section.detail) \(section.error ?? "")".lowercased()
    return combined.contains("source probe failed") || combined.contains("probe failed")
  }

  var hasOnlyBenignControlSourceProbeWarning: Bool {
    guard isBenignControlSourceProbeWarning(name: "control_source") else {
      return false
    }
    return sectionNames.allSatisfy { name in
      guard let section = sections[name] else {
        return true
      }
      if name == "control_source" {
        return true
      }
      return section.state == .ok || section.state == .disabled
    }
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
    let states = report.sectionNames.compactMap { name -> SectionState? in
      guard let state = report.sections[name]?.state else {
        return nil
      }
      return report.isBenignControlSourceProbeWarning(name: name) ? nil : state
    }
    if states.isEmpty {
      return .gray
    }
    if states.contains(.fail) {
      return .red
    }
    if states.contains(.warn) || states.contains(.unknown) || (report.degraded && !report.hasOnlyBenignControlSourceProbeWarning) {
      return .yellow
    }
    return .green
  }
}
