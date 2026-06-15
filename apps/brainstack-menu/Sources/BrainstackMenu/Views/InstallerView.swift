import SwiftUI
import BrainstackMenuCore

struct InstallerView: View {
  @ObservedObject var model: AppModel
  @State private var invite = ""
  @State private var showInvite = false

  var body: some View {
    VStack(alignment: .leading, spacing: 14) {
      VStack(alignment: .leading, spacing: 4) {
        Text("Set Up Brainstack")
          .font(.title2)
          .bold()
        Text("Install the bundled brainctl, enroll this Mac with an invite, then repair daemon, hooks, skills, and local guidance.")
          .font(.callout)
          .foregroundColor(.secondary)
          .fixedSize(horizontal: false, vertical: true)
      }

      VStack(alignment: .leading, spacing: 8) {
        InstallerFactRow(label: "Bundled brainctl", value: model.bundledBrainctlPath ?? "missing from this app bundle", ok: model.bundledBrainctlPath != nil)
        InstallerFactRow(label: "Install target", value: model.defaultInstallPath, ok: true)
        HStack {
          Text("Config")
            .frame(width: 110, alignment: .leading)
            .foregroundColor(.secondary)
          TextField("Config path", text: $model.configPath)
            .font(.system(.body, design: .monospaced))
        }
      }

      VStack(alignment: .leading, spacing: 6) {
        Text("Invite")
          .font(.headline)
        Group {
          if showInvite {
            TextField("Paste bs1_… invite", text: $invite)
          } else {
            SecureField("Paste bs1_… invite", text: $invite)
          }
        }
        .font(.system(.body, design: .monospaced))
        Toggle("Show invite while editing", isOn: $showInvite)
          .font(.caption)
        Text(inviteHelp)
          .font(.caption)
          .foregroundColor(.secondary)
          .fixedSize(horizontal: false, vertical: true)
      }

      HStack {
        Button {
          model.installOrRepair(invite: invite)
          invite = ""
        } label: {
          Label(configExists ? "Re-enroll / Repair" : "Set Up This Mac", systemImage: "wand.and.stars")
        }
        .keyboardShortcut(.defaultAction)
        .disabled(model.installerRunning || model.bundledBrainctlPath == nil || invite.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)

        Button {
          model.installOrRepair(invite: nil)
        } label: {
          Label("Repair Existing Install", systemImage: "wrench.and.screwdriver")
        }
        .disabled(model.installerRunning || model.bundledBrainctlPath == nil)

        Spacer()
        if model.installerRunning {
          ProgressView()
            .controlSize(.small)
        }
      }

      if !configExists && invite.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
        Text("This Mac is not enrolled yet. Paste a fresh invite from your control host to continue.")
          .font(.caption)
          .foregroundColor(.orange)
      }

      if let action = installerAction {
        Divider()
        InstallerActionView(action: action)
      }
    }
    .padding(18)
    .frame(width: 560)
  }

  private var configExists: Bool {
    FileManager.default.fileExists(atPath: (model.configPath as NSString).expandingTildeInPath)
  }

  private var inviteHelp: String {
    "Invites are bearer secrets. The app writes the pasted value to a private temporary file for `brainctl enroll --invite-file`, then deletes it. It is not saved to Preferences or diagnostics."
  }

  private var installerAction: ActionOutcome? {
    guard let action = model.lastAction else {
      return nil
    }
    return ["Set Up Brainstack", "Repair Brainstack"].contains(action.title) ? action : nil
  }
}

private struct InstallerFactRow: View {
  let label: String
  let value: String
  let ok: Bool

  var body: some View {
    HStack(alignment: .firstTextBaseline, spacing: 8) {
      Circle()
        .fill(ok ? Color.green : Color.red)
        .frame(width: 7, height: 7)
      Text(label)
        .frame(width: 110, alignment: .leading)
        .foregroundColor(.secondary)
      Text(value)
        .font(.system(.caption, design: .monospaced))
        .lineLimit(2)
        .truncationMode(.middle)
        .textSelection(.enabled)
      Spacer(minLength: 0)
    }
  }
}

private struct InstallerActionView: View {
  let action: ActionOutcome

  var body: some View {
    VStack(alignment: .leading, spacing: 4) {
      Text(action.summary)
        .font(.callout)
        .foregroundColor(action.succeeded ? .secondary : .red)
      if !action.output.isEmpty {
        ScrollView {
          Text(action.output)
            .font(.system(size: 10, design: .monospaced))
            .frame(maxWidth: .infinity, alignment: .leading)
            .textSelection(.enabled)
        }
        .frame(maxHeight: 140)
      }
    }
  }
}
