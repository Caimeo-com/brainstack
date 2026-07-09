import AppKit
import SwiftUI
import BrainstackMenuCore

struct FolderPacksView: View {
  @ObservedObject var model: AppModel
  @State private var selectedMachine = ""
  @State private var packName = ""
  @State private var contextSlug = ""

  var body: some View {
    VStack(alignment: .leading, spacing: 14) {
      header
      controls
      if let error = model.contextPacksError {
        Text(error)
          .font(.caption)
          .foregroundColor(.red)
          .fixedSize(horizontal: false, vertical: true)
      }
      packList
    }
    .padding(18)
    .frame(minWidth: 720, minHeight: 500)
    .onAppear {
      normalizeSelectedMachine()
      model.loadContextPacks(machine: currentMachine)
    }
    .onChange(of: model.uploadTargetMachines) { _ in
      normalizeSelectedMachine()
    }
    .onChange(of: selectedMachine) { machine in
      guard !machine.isEmpty else { return }
      model.loadContextPacks(machine: machine)
    }
  }

  private var currentMachine: String {
    if !selectedMachine.isEmpty {
      return selectedMachine
    }
    return model.uploadTargetMachines.first ?? "local"
  }

  private var header: some View {
    VStack(alignment: .leading, spacing: 4) {
      Text("Folder Packs")
        .font(.title2.weight(.semibold))
      Text("Sync a local folder to a Brainstack machine as reusable context. Harnesses see paths and metadata, not file contents.")
        .font(.callout)
        .foregroundColor(.secondary)
        .fixedSize(horizontal: false, vertical: true)
    }
  }

  private var controls: some View {
    VStack(alignment: .leading, spacing: 10) {
      HStack(spacing: 10) {
        Picker("Machine", selection: $selectedMachine) {
          ForEach(model.uploadTargetMachines, id: \.self) { machine in
            Text(machine).tag(machine)
          }
        }
        .frame(width: 230)
        TextField("Pack name", text: $packName)
          .textFieldStyle(.roundedBorder)
          .frame(width: 180)
        Button {
          chooseFolder()
        } label: {
          Label("Add Folder…", systemImage: "folder.badge.plus")
        }
        .disabled(model.isSyncingContextPack)
        Button {
          model.loadContextPacks(machine: currentMachine)
        } label: {
          Label("Refresh", systemImage: "arrow.clockwise")
        }
        .disabled(model.isLoadingContextPacks)
        Button {
          model.gcContextPacks(machine: currentMachine, delete: false)
        } label: {
          Label("Check Unused", systemImage: "magnifyingglass")
        }
        .disabled(model.isSyncingContextPack)
        Button(role: .destructive) {
          guard Confirm.ask(title: "Delete Unused Folder Packs", message: "Delete unused folder-pack copies on \(currentMachine)? Attached packs and source-owned packs are protected.") else {
            return
          }
          model.gcContextPacks(machine: currentMachine, delete: true)
        } label: {
          Label("Delete Unused…", systemImage: "trash")
        }
        .disabled(model.isSyncingContextPack)
        Spacer()
        if model.isSyncingContextPack || model.isLoadingContextPacks {
          ProgressView()
            .controlSize(.small)
        }
      }
      HStack(spacing: 10) {
        TextField("Context slug for attach/detach", text: $contextSlug)
          .textFieldStyle(.roundedBorder)
          .frame(width: 320)
        Text("Inside Telegram you can also say `attach pack <name>` from a bound topic.")
          .font(.caption)
          .foregroundColor(.secondary)
      }
    }
  }

  @ViewBuilder
  private var packList: some View {
    VStack(alignment: .leading, spacing: 8) {
      Text("Available")
        .font(.headline)
      if model.contextPacks.isEmpty && !model.isLoadingContextPacks {
        Text("No folder packs on \(currentMachine).")
          .font(.callout)
          .foregroundColor(.secondary)
          .frame(maxWidth: .infinity, minHeight: 180, alignment: .center)
      } else {
        ScrollView {
          LazyVStack(alignment: .leading, spacing: 8) {
            ForEach(model.contextPacks) { pack in
              FolderPackRowView(
                pack: pack,
                isBusy: model.isSyncingContextPack,
                contextSlug: contextSlug,
                onCopy: { copyPath(pack.contentPath) },
                onSync: { model.syncContextPack(pack) },
                onAttach: { model.attachContextPack(pack, context: contextSlug) },
                onDetach: { model.detachContextPack(pack, context: contextSlug) },
                onDelete: {
                  guard Confirm.ask(title: "Delete Folder Pack", message: "Delete \(pack.displayName) from \(pack.machine)? The remote folder-pack copy will be removed.") else {
                    return
                  }
                  model.deleteContextPack(pack)
                }
              )
            }
          }
          .padding(.vertical, 2)
        }
      }
    }
  }

  private func normalizeSelectedMachine() {
    let machines = model.uploadTargetMachines
    guard !machines.isEmpty else {
      selectedMachine = "local"
      return
    }
    if selectedMachine.isEmpty || !machines.contains(selectedMachine) {
      selectedMachine = machines[0]
    }
  }

  private func chooseFolder() {
    let panel = NSOpenPanel()
    panel.canChooseFiles = false
    panel.canChooseDirectories = true
    panel.allowsMultipleSelection = false
    panel.prompt = "Add Folder"
    if panel.runModal() == .OK, let url = panel.urls.first {
      let name = packName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? url.lastPathComponent : packName
      model.addContextPack(folder: url, machine: currentMachine, name: name)
    }
  }

  private func copyPath(_ path: String) {
    let pasteboard = NSPasteboard.general
    pasteboard.clearContents()
    pasteboard.setString(path, forType: .string)
  }
}

private struct FolderPackRowView: View {
  let pack: ContextPackSummary
  let isBusy: Bool
  let contextSlug: String
  let onCopy: () -> Void
  let onSync: () -> Void
  let onAttach: () -> Void
  let onDetach: () -> Void
  let onDelete: () -> Void

  var body: some View {
    HStack(alignment: .top, spacing: 10) {
      Image(systemName: "folder")
        .foregroundColor(.secondary)
        .frame(width: 20)
      VStack(alignment: .leading, spacing: 4) {
        HStack(spacing: 6) {
          Text(pack.displayName)
            .font(.callout.weight(.semibold))
            .lineLimit(1)
            .truncationMode(.middle)
          Text("\(pack.fileCount) files")
            .font(.caption)
            .foregroundColor(.secondary)
          Text(pack.formattedSize)
            .font(.caption)
            .foregroundColor(.secondary)
        }
        Text(pack.contentPath)
          .font(.system(size: 11, design: .monospaced))
          .foregroundColor(.secondary)
          .lineLimit(2)
          .truncationMode(.middle)
          .textSelection(.enabled)
        HStack(spacing: 8) {
          Text(pack.machine)
          if !pack.sourceMachine.isEmpty {
            Text("source: \(pack.sourceMachine)")
          }
          if !pack.refreshedAt.isEmpty {
            Text(pack.refreshedAt)
          }
          if !pack.freshness.isEmpty {
            Text(pack.freshness)
          }
        }
        .font(.caption2)
        .foregroundColor(.secondary)
        if let warning = pack.warnings.first {
          Text(warning)
            .font(.caption2)
            .foregroundColor(.orange)
            .lineLimit(2)
        }
      }
      Spacer()
      VStack(alignment: .trailing, spacing: 6) {
        Button("Copy Path") { onCopy() }
          .controlSize(.small)
        Button("Sync") { onSync() }
          .controlSize(.small)
          .disabled(isBusy)
        HStack(spacing: 6) {
          Button("Attach") { onAttach() }
            .controlSize(.small)
            .disabled(isBusy || contextSlug.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
          Button("Detach") { onDetach() }
            .controlSize(.small)
            .disabled(isBusy || contextSlug.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
        }
        Button("Delete", role: .destructive) { onDelete() }
          .controlSize(.small)
          .disabled(isBusy)
      }
    }
    .padding(10)
    .background(Color.secondary.opacity(0.08))
    .clipShape(RoundedRectangle(cornerRadius: 8))
  }
}
