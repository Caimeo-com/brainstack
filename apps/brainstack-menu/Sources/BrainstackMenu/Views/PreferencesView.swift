import AppKit
import ServiceManagement
import SwiftUI
import BrainstackMenuCore

struct PreferencesView: View {
  @ObservedObject var model: AppModel
  @State private var launchAtLogin = false
  @State private var launchAtLoginError: String?

  var body: some View {
    Form {
      Section {
        HStack {
          TextField("brainctl path", text: Binding(
            get: { model.binaryPathPreference ?? "" },
            set: { model.binaryPathPreference = $0.isEmpty ? nil : $0 }
          ))
          .font(.system(.body, design: .monospaced))
          Button("Choose…") { chooseBinary() }
        }
        Text("Resolved: \(model.resolvedBinaryPath ?? "not found — install Brainstack or pick the binary")")
          .font(.caption)
          .foregroundColor(model.resolvedBinaryPath == nil ? .red : .secondary)

        TextField("Config path", text: $model.configPath)
          .font(.system(.body, design: .monospaced))
      } header: {
        Text("Paths")
      }

      Section {
        Picker("Poll interval", selection: $model.pollInterval) {
          Text("15 seconds").tag(TimeInterval(15))
          Text("30 seconds").tag(TimeInterval(30))
          Text("60 seconds").tag(TimeInterval(60))
          Text("5 minutes").tag(TimeInterval(300))
        }
        Toggle("Launch at login", isOn: Binding(
          get: { launchAtLogin },
          set: { updateLaunchAtLogin($0) }
        ))
        if let launchAtLoginError {
          Text(launchAtLoginError)
            .font(.caption)
            .foregroundColor(.orange)
            .fixedSize(horizontal: false, vertical: true)
            .textSelection(.enabled)
        }
        Toggle("Enable notifications", isOn: $model.notificationsEnabled)
        Text("Notifications fire only on state transitions (broken, recovered, outbox stuck, curator failing).")
          .font(.caption)
          .foregroundColor(.secondary)
      } header: {
        Text("Behavior")
      }

      Section {
        Toggle("Enable Operator Mode", isOn: $model.operatorModeEnabled)
        Text("Operator Mode exposes curator runs and proposal approve/reject/apply. Actions work when brainctl can reach admin auth locally or through the enrolled control host; the app never stores tokens.")
          .font(.caption)
          .foregroundColor(.secondary)
      } header: {
        Text("Operator Mode")
      }
    }
    .formStyle(.grouped)
    .frame(width: 460, height: 420)
    .onAppear {
      syncLaunchAtLoginStatus()
    }
  }

  private func chooseBinary() {
    let panel = NSOpenPanel()
    panel.canChooseFiles = true
    panel.canChooseDirectories = false
    panel.allowsMultipleSelection = false
    panel.showsHiddenFiles = true
    panel.directoryURL = URL(fileURLWithPath: FileManager.default.homeDirectoryForCurrentUser.path + "/.local/bin")
    if panel.runModal() == .OK, let url = panel.url {
      model.binaryPathPreference = url.path
    }
  }

  private func updateLaunchAtLogin(_ enabled: Bool) {
    guard isBundledApp else {
      launchAtLogin = false
      model.preferences.launchAtLoginEnabled = false
      launchAtLoginError = "Launch at login requires running the bundled app (scripts/make-app.sh), not swift run."
      return
    }
    do {
      if enabled {
        try SMAppService.mainApp.register()
      } else {
        try SMAppService.mainApp.unregister()
      }
      syncLaunchAtLoginStatus()
    } catch {
      syncLaunchAtLoginStatus()
      launchAtLoginError = "Could not update login item: \(error.localizedDescription)"
    }
  }

  private func syncLaunchAtLoginStatus() {
    guard isBundledApp else {
      launchAtLogin = model.preferences.launchAtLoginEnabled
      return
    }
    let status = SMAppService.mainApp.status
    launchAtLogin = status == .enabled || status == .requiresApproval
    model.preferences.launchAtLoginEnabled = launchAtLogin
    switch status {
    case .enabled, .notRegistered:
      launchAtLoginError = nil
    case .requiresApproval:
      launchAtLoginError = "macOS requires approval in System Settings > General > Login Items."
    case .notFound:
      launchAtLoginError = "macOS cannot register this app copy as a login item. Use a notarized release in /Applications; local signed-only builds can run manually but may still be rejected by ServiceManagement."
    @unknown default:
      launchAtLoginError = "Unknown login item status: \(status.rawValue)"
    }
  }

  private var isBundledApp: Bool {
    Bundle.main.bundleIdentifier != nil && Bundle.main.bundleURL.pathExtension == "app"
  }
}
