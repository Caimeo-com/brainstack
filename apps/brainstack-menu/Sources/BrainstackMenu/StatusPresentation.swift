import Foundation
import BrainstackMenuCore

enum AttentionSeverity: Sendable {
  case ok
  case info
  case warn
  case fail
}

/// A one-click fix attached to an attention row. Buttons only appear when the
/// matching problem is present, which keeps the popover calm when healthy.
enum RepairKind: Equatable, Sendable {
  case doctor
  case flushOutbox
  case retryOutbox
  case discardOutbox
  case discardCorruptOutbox
  case refreshSkills
  case restartDaemon
  case repairHooks
  case checkUpdates
  case updateControlHost
  case installCurator
  case openTailscale

  var buttonTitle: String {
    switch self {
    case .doctor: return "Run Doctor"
    case .flushOutbox: return "Send"
    case .retryOutbox: return "Retry"
    case .discardOutbox: return "Discard"
    case .discardCorruptOutbox: return "Discard Damaged"
    case .refreshSkills: return "Refresh"
    case .restartDaemon: return "Restart"
    case .repairHooks: return "Repair"
    case .checkUpdates: return "Check Updates"
    case .updateControlHost: return "Update"
    case .installCurator: return "Install"
    case .openTailscale: return "Open"
    }
  }

  /// Confirmation copy for mutating repairs; nil means run immediately.
  var confirmation: (title: String, message: String)? {
    switch self {
    case .doctor, .checkUpdates, .openTailscale:
      return nil
    case .flushOutbox:
      return ("Send Saved Writes", "Send saved outbox writes to the shared brain now?")
    case .retryOutbox:
      return ("Retry Saved Writes", "Retry paused saved writes? If the original cause is still present, Brainstack will pause them again instead of losing data.")
    case .discardOutbox:
      return ("Discard Saved Writes", "Permanently delete the local saved writes from this Mac? Use this only when those imports, memories, or proposals are obsolete.")
    case .discardCorruptOutbox:
      return ("Discard Damaged Saved Writes", "Permanently delete damaged saved write files from this Mac? Use this only when those local files are unrecoverable.")
    case .refreshSkills:
      return ("Refresh Skills", "Refresh shared skills from the shared brain?")
    case .restartDaemon:
      return ("Install/Restart Daemon", "Install (or reinstall and restart) the brainstackd user service?")
    case .repairHooks:
      return ("Install/Repair Hooks", "Install or repair Brainstack hooks for Codex, Claude, and Cursor?")
    case .updateControlHost:
      return ("Update Control Host", "Pull, rebuild, upgrade, and restart Brainstack on the control host?")
    case .installCurator:
      return ("Install Curator", "Install the brain-curator routine on the control host? It schedules proposal generation; it does not accept or apply wiki edits.")
    }
  }
}

/// Map a degraded section to its local one-click repair, if one exists.
func repairKind(forSection name: String) -> RepairKind? {
  switch name {
  case "daemon": return .restartDaemon
  case "outbox": return .flushOutbox
  case "hooks": return .repairHooks
  case "skills": return .refreshSkills
  case "tailscale": return .openTailscale
  case "shared_brain", "config": return .doctor
  default: return nil
  }
}

func repairKind(forSection name: String, section: StatusSection) -> RepairKind? {
  if name == "outbox" {
    let outbox = OutboxStatusSummary(section: section)
    if outbox.corrupt > 0 {
      return nil
    }
    return outbox.needsRetry ? .retryOutbox : .flushOutbox
  }
  return repairKind(forSection: name)
}

func discardKind(forOutbox summary: OutboxStatusSummary) -> RepairKind? {
  if summary.corrupt > 0 {
    return .discardCorruptOutbox
  }
  if summary.hasSavedWrites {
    return .discardOutbox
  }
  return nil
}

struct AttentionItem: Identifiable, Equatable, Sendable {
  let title: String
  let detail: String
  let severity: AttentionSeverity
  var repair: RepairKind? = nil
  var secondaryRepair: RepairKind? = nil

  var id: String {
    "\(title)\n\(detail)\n\(severity)\n\(String(describing: repair))\n\(String(describing: secondaryRepair))"
  }
}

func statusSummaryLine(for report: StatusReport) -> String {
  if tailscaleNeedsStart(report) {
    return tailscaleSummary(report)
  }
  if controlHostLooksOld(report) {
    return "control host needs update"
  }
  let staleFleet = report.fleetMachines.filter(\.needsUpdate)
  if !staleFleet.isEmpty {
    return "\(staleFleet.count) machine\(staleFleet.count == 1 ? "" : "s") need update"
  }
  if curatorRoutineMissing(report) {
    return "curator routine not installed"
  }
  let failures = report.sectionNames.filter { name in
    report.sections[name]?.state == .fail
  }
  if !failures.isEmpty {
    return "needs repair: \(failures.map(sectionLabel).joined(separator: ", "))"
  }
  let warnings = report.sectionNames.filter { name in
    guard let section = report.sections[name], section.state == .warn else {
      return false
    }
    return !isBenignWarning(name: name, section: section, report: report)
      && !isBlockedByTailscaleRootCause(name: name, section: section, report: report)
  }
  if !warnings.isEmpty {
    return "needs attention: \(warnings.map(sectionLabel).joined(separator: ", "))"
  }
  if let open = openProposalCount(report), open > 0 {
    return "\(open) proposal\(open == 1 ? "" : "s") awaiting review"
  }
  return "healthy"
}

func tailscaleNeedsStart(_ report: StatusReport) -> Bool {
  guard let section = report.sections["tailscale"], section.state == .warn else {
    return false
  }
  if section.data?["running"]?.boolValue == false {
    return true
  }
  let detail = section.detail.lowercased()
  return detail.contains("stopped") || detail.contains("not installed") || detail.contains("unavailable")
}

func tailscaleSummary(_ report: StatusReport) -> String {
  guard let section = report.sections["tailscale"] else {
    return "Tailscale needs attention"
  }
  if section.data?["installed"]?.boolValue == false {
    return "Tailscale is not installed"
  }
  return section.detail.isEmpty ? "Tailscale needs attention" : section.detail
}

func isRemoteDependencySection(_ name: String) -> Bool {
  ["brain_api", "curator", "proposals", "fleet", "control_source"].contains(name)
}

func daemonWarningLooksRemoteOnly(_ section: StatusSection) -> Bool {
  guard section.state == .warn else {
    return false
  }
  let errorText = section.data?["status"]?["errors"]?.arrayValue?
    .compactMap(\.stringValue)
    .joined(separator: " ")
    .lowercased() ?? ""
  let combined = "\(section.detail) \(section.error ?? "") \(errorText)".lowercased()
  return combined.contains("git pull failed")
    || combined.contains("could not resolve hostname")
    || combined.contains("tailscale")
}

func isBlockedByTailscaleRootCause(name: String, section: StatusSection, report: StatusReport) -> Bool {
  guard tailscaleNeedsStart(report) else {
    return false
  }
  return isRemoteDependencySection(name) || (name == "daemon" && daemonWarningLooksRemoteOnly(section))
}

func openProposalCount(_ report: StatusReport) -> Int? {
  if let open = report.sections["curator"]?.data?["open_proposals"]?.numberValue {
    return Int(open)
  }
  if let count = report.sections["proposals"]?.data?["count"]?.numberValue {
    return Int(count)
  }
  return nil
}

func controlHostLooksOld(_ report: StatusReport) -> Bool {
  ["curator", "proposals"].contains { name in
    guard let section = report.sections[name] else {
      return false
    }
    return isOldControlEndpointWarning(section)
  }
}

func curatorRoutineMissing(_ report: StatusReport) -> Bool {
  guard let curator = report.sections["curator"], curator.state == .ok else {
    return false
  }
  return curator.data?["curator"]?["installed"]?.boolValue == false
}

func isOldControlEndpointWarning(_ section: StatusSection) -> Bool {
  if section.detail.contains("HTTP 404") {
    return true
  }
  let error = section.data?["body"]?["error"]?.stringValue ?? ""
  return error.contains("Unknown route")
}

func isBenignProductWarning(name: String, section: StatusSection) -> Bool {
  guard name == "product" else {
    return false
  }
  return section.state == .disabled
    || section.detail.contains("not a git checkout")
    || section.detail.contains("source checkout not installed")
}

func isBenignProposalLatencyWarning(name: String, section: StatusSection, report: StatusReport) -> Bool {
  guard name == "proposals", section.state == .warn else {
    return false
  }
  guard let curator = report.sections["curator"], curator.state == .ok else {
    return false
  }
  let error = (section.error ?? "").lowercased()
  return error.contains("timed out") || error.contains("operation timed out")
}

func isBenignWarning(name: String, section: StatusSection, report: StatusReport) -> Bool {
  isBenignProductWarning(name: name, section: section)
    || isBenignProposalLatencyWarning(name: name, section: section, report: report)
}

func sectionLabel(_ name: String) -> String {
  switch name {
  case "tailscale": return "Tailscale"
  case "brain_api": return "Brain API"
  case "shared_brain": return "Shared Brain"
  case "outbox": return "Outbox"
  case "hooks": return "Hooks"
  case "skills": return "Skills"
  case "curator": return "Curator"
  case "proposals": return "Proposals"
  case "fleet": return "Fleet"
  case "control_source": return "Control Host"
  case "product": return "Local Source"
  case "telemux": return "Telegram"
  case "daemon": return "Daemon"
  case "config": return "Config"
  default:
    return name.replacingOccurrences(of: "_", with: " ").capitalized
  }
}

func sectionMessage(name: String, section: StatusSection) -> String {
  if name == "outbox" {
    return OutboxStatusSummary(section: section).compactMessage
  }
  if name == "tailscale" {
    if section.data?["installed"]?.boolValue == false {
      return "Install Tailscale, then refresh Brainstack."
    }
    if section.data?["running"]?.boolValue == false {
      return "Start Tailscale to reach the Brainstack control host."
    }
  }
  if isOldControlEndpointWarning(section) {
    return "The control host is running an older Brainstack. Update it, then refresh."
  }
  if isBenignProductWarning(name: name, section: section) {
    return "This Mac is using the installed client binary without a local product source checkout."
  }
  if name == "fleet" {
    let machines = section.data?["machines"]?.arrayValue?.compactMap(FleetMachineSummary.init(json:)) ?? []
    let stale = machines.filter(\.needsUpdate).map(\.name)
    if !stale.isEmpty {
      return "Update needed: \(stale.joined(separator: ", "))"
    }
  }
  if !section.detail.isEmpty {
    return section.detail
  }
  return section.error ?? "No detail available."
}
