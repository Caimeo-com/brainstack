import AppKit
import SwiftUI
import UniformTypeIdentifiers
import BrainstackMenuCore

struct UploadsView: View {
  @ObservedObject var model: AppModel
  @State private var selectedMachine = ""
  @State private var dropIsTargeted = false

  var body: some View {
    VStack(alignment: .leading, spacing: 14) {
      header
      controls
      dropZone
      if let error = model.uploadsError {
        Text(error)
          .font(.caption)
          .foregroundColor(.red)
          .fixedSize(horizontal: false, vertical: true)
      }
      uploadList
    }
    .padding(18)
    .frame(minWidth: 620, minHeight: 420)
    .onAppear {
      normalizeSelectedMachine()
      model.loadUploads(machine: currentMachine)
    }
    .onChange(of: model.uploadTargetMachines) { _ in
      normalizeSelectedMachine()
    }
    .onChange(of: selectedMachine) { machine in
      guard !machine.isEmpty else {
        return
      }
      model.loadUploads(machine: machine)
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
      Text("Uploads")
        .font(.title2.weight(.semibold))
      Text("Move large or sensitive files to a Brainstack machine, then reference the saved path from Telegram or a harness.")
        .font(.callout)
        .foregroundColor(.secondary)
        .fixedSize(horizontal: false, vertical: true)
    }
  }

  private var controls: some View {
    HStack(spacing: 10) {
      Picker("Machine", selection: $selectedMachine) {
        ForEach(model.uploadTargetMachines, id: \.self) { machine in
          Text(machine).tag(machine)
        }
      }
      .frame(width: 240)
      Button {
        chooseFiles()
      } label: {
        Label("Choose Files…", systemImage: "plus")
      }
      .disabled(model.isUploading)
      Button {
        model.loadUploads(machine: currentMachine)
      } label: {
        Label("Refresh", systemImage: "arrow.clockwise")
      }
      .disabled(model.isLoadingUploads)
      Spacer()
      if model.isUploading {
        ProgressView()
          .controlSize(.small)
        Text("Uploading…")
          .font(.caption)
          .foregroundColor(.secondary)
      } else if model.isLoadingUploads {
        ProgressView()
          .controlSize(.small)
        Text("Loading…")
          .font(.caption)
          .foregroundColor(.secondary)
      }
    }
  }

  private var dropZone: some View {
    VStack(spacing: 8) {
      Image(systemName: "tray.and.arrow.down")
        .font(.system(size: 28, weight: .semibold))
        .foregroundColor(dropIsTargeted ? .accentColor : .secondary)
      Text("Drop files here")
        .font(.headline)
      Text("Files are copied to \(currentMachine)'s Brainstack uploads folder with private file permissions.")
        .font(.caption)
        .foregroundColor(.secondary)
        .multilineTextAlignment(.center)
    }
    .frame(maxWidth: .infinity, minHeight: 112)
    .background((dropIsTargeted ? Color.accentColor : Color.secondary).opacity(dropIsTargeted ? 0.16 : 0.08))
    .clipShape(RoundedRectangle(cornerRadius: 8))
    .overlay(
      RoundedRectangle(cornerRadius: 8)
        .stroke(dropIsTargeted ? Color.accentColor : Color.secondary.opacity(0.24), style: StrokeStyle(lineWidth: 1, dash: [6, 5]))
    )
    .onDrop(of: [UTType.fileURL.identifier], isTargeted: $dropIsTargeted) { providers in
      handleDrop(providers)
    }
  }

  @ViewBuilder
  private var uploadList: some View {
    VStack(alignment: .leading, spacing: 8) {
      Text("Recent")
        .font(.headline)
      if model.uploads.isEmpty && !model.isLoadingUploads {
        Text("No recent uploads on \(currentMachine).")
          .font(.callout)
          .foregroundColor(.secondary)
          .frame(maxWidth: .infinity, minHeight: 120, alignment: .center)
      } else {
        ScrollView {
          LazyVStack(alignment: .leading, spacing: 8) {
            ForEach(model.uploads) { upload in
              UploadRowView(upload: upload, isBusy: model.isUploading) {
                copyPath(upload.remotePath)
              } onDelete: {
                guard Confirm.ask(title: "Delete Upload", message: "Delete \(upload.displayName) from \(upload.machine)? The file will be removed from the remote uploads folder.") else {
                  return
                }
                model.deleteUpload(upload)
              }
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

  private func chooseFiles() {
    let panel = NSOpenPanel()
    panel.canChooseFiles = true
    panel.canChooseDirectories = false
    panel.allowsMultipleSelection = true
    panel.prompt = "Upload"
    if panel.runModal() == .OK {
      model.uploadFiles(panel.urls, machine: currentMachine)
    }
  }

  private func handleDrop(_ providers: [NSItemProvider]) -> Bool {
    let machine = currentMachine
    let group = DispatchGroup()
    let lock = NSLock()
    var urls: [URL] = []
    for provider in providers {
      group.enter()
      provider.loadItem(forTypeIdentifier: UTType.fileURL.identifier, options: nil) { item, _ in
        defer { group.leave() }
        guard let url = Self.fileURL(from: item) else {
          return
        }
        lock.lock()
        urls.append(url)
        lock.unlock()
      }
    }
    group.notify(queue: .main) {
      if !urls.isEmpty {
        model.uploadFiles(urls, machine: machine)
      }
    }
    return true
  }

  private static func fileURL(from item: NSSecureCoding?) -> URL? {
    if let url = item as? URL {
      return url
    }
    if let data = item as? Data {
      return URL(dataRepresentation: data, relativeTo: nil)
    }
    if let data = item as? NSData {
      return URL(dataRepresentation: data as Data, relativeTo: nil)
    }
    if let string = item as? String {
      return URL(string: string)
    }
    return nil
  }

  private func copyPath(_ path: String) {
    let pasteboard = NSPasteboard.general
    pasteboard.clearContents()
    pasteboard.setString(path, forType: .string)
  }
}

private struct UploadRowView: View {
  let upload: UploadSummary
  let isBusy: Bool
  let onCopy: () -> Void
  let onDelete: () -> Void

  var body: some View {
    HStack(alignment: .top, spacing: 10) {
      Image(systemName: "doc")
        .foregroundColor(.secondary)
        .frame(width: 20)
      VStack(alignment: .leading, spacing: 4) {
        HStack(spacing: 6) {
          Text(upload.displayName)
            .font(.callout.weight(.semibold))
            .lineLimit(1)
            .truncationMode(.middle)
          Text(upload.formattedSize)
            .font(.caption)
            .foregroundColor(.secondary)
        }
        Text(upload.remotePath)
          .font(.system(size: 11, design: .monospaced))
          .foregroundColor(.secondary)
          .lineLimit(2)
          .truncationMode(.middle)
          .textSelection(.enabled)
        HStack(spacing: 8) {
          Text(upload.machine)
          if !upload.uploadedAt.isEmpty {
            Text(upload.uploadedAt)
          }
          if let label = upload.label, !label.isEmpty {
            Text(label)
          }
        }
        .font(.caption2)
        .foregroundColor(.secondary)
      }
      Spacer()
      VStack(alignment: .trailing, spacing: 6) {
        Button("Copy Path") { onCopy() }
          .controlSize(.small)
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
