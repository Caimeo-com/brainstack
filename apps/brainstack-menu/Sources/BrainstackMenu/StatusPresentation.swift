import Foundation
import BrainstackMenuCore

enum AttentionSeverity: Sendable {
  case ok
  case info
  case warn
  case fail
}

struct AttentionItem: Identifiable, Equatable, Sendable {
  let title: String
  let detail: String
  let severity: AttentionSeverity

  var id: String {
    "\(title)\n\(detail)\n\(severity)"
  }
}

func statusSummaryLine(for report: StatusReport) -> String {
  if controlHostLooksOld(report) {
    return "control host needs update"
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
    return !isBenignProductWarning(name: name, section: section)
  }
  if !warnings.isEmpty {
    return "needs attention: \(warnings.map(sectionLabel).joined(separator: ", "))"
  }
  if let open = openProposalCount(report), open > 0 {
    return "\(open) proposal\(open == 1 ? "" : "s") awaiting review"
  }
  return "healthy"
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

func sectionLabel(_ name: String) -> String {
  switch name {
  case "brain_api": return "Brain API"
  case "shared_brain": return "Shared Brain"
  case "outbox": return "Outbox"
  case "hooks": return "Hooks"
  case "skills": return "Skills"
  case "curator": return "Curator"
  case "proposals": return "Proposals"
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
  if isOldControlEndpointWarning(section) {
    return "The control host is running an older Brainstack. Update it, then refresh."
  }
  if isBenignProductWarning(name: name, section: section) {
    return "This Mac is using the installed client binary without a local product source checkout."
  }
  if !section.detail.isEmpty {
    return section.detail
  }
  return section.error ?? "No detail available."
}
