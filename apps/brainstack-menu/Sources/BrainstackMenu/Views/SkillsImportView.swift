import AppKit
import SwiftUI
import BrainstackMenuCore

struct SkillsImportView: View {
  @ObservedObject var model: AppModel
  @State private var selectedIndexes = Set<Int>()
  @State private var loadedOnce = false

  var body: some View {
    VStack(alignment: .leading, spacing: 14) {
      header
      controls
      statusLine
      candidates
      footer
    }
    .padding(18)
    .frame(minWidth: 720, minHeight: 500)
    .onAppear {
      guard !loadedOnce else {
        return
      }
      loadedOnce = true
      model.loadSkillImportPlan(sourcePath: model.skillImportSourcePath.isEmpty ? nil : model.skillImportSourcePath)
    }
    .onChange(of: model.skillImportPlan) { plan in
      selectedIndexes = Set(plan?.proposed.map(\.id) ?? [])
    }
  }

  private var header: some View {
    VStack(alignment: .leading, spacing: 4) {
      Text("Import Skills")
        .font(.title2.weight(.semibold))
      Text("Scan local skills, choose what to publish, and import them into the shared brain for connected harnesses.")
        .font(.callout)
        .foregroundColor(.secondary)
        .fixedSize(horizontal: false, vertical: true)
    }
  }

  private var controls: some View {
    HStack(spacing: 10) {
      Button {
        chooseSource()
      } label: {
        Label("Choose Folder or Skill…", systemImage: "folder")
      }
      .disabled(model.isLoadingSkillImportPlan || model.isImportingSkills)

      Button {
        model.loadSkillImportPlan(sourcePath: nil)
      } label: {
        Label("Default Scan", systemImage: "magnifyingglass")
      }
      .disabled(model.isLoadingSkillImportPlan || model.isImportingSkills)

      Button {
        model.loadSkillImportPlan(sourcePath: model.skillImportSourcePath.isEmpty ? nil : model.skillImportSourcePath)
      } label: {
        Label("Refresh", systemImage: "arrow.clockwise")
      }
      .disabled(model.isLoadingSkillImportPlan || model.isImportingSkills)

      Spacer()

      if model.isLoadingSkillImportPlan {
        ProgressView().controlSize(.small)
        Text("Scanning…")
          .font(.caption)
          .foregroundColor(.secondary)
      } else if model.isImportingSkills {
        ProgressView().controlSize(.small)
        Text("Importing…")
          .font(.caption)
          .foregroundColor(.secondary)
      }
    }
  }

  @ViewBuilder
  private var statusLine: some View {
    VStack(alignment: .leading, spacing: 6) {
      HStack(spacing: 6) {
        Text("Source")
          .font(.caption.weight(.semibold))
          .foregroundColor(.secondary)
        Text(model.skillImportSourcePath.isEmpty ? "Default skill locations" : model.skillImportSourcePath)
          .font(.system(size: 11, design: .monospaced))
          .lineLimit(1)
          .truncationMode(.middle)
          .textSelection(.enabled)
      }
      if let plan = model.skillImportPlan {
        let proposed = plan.proposed.count
        let skipped = plan.skipped.count
        let rejected = plan.rejected.count
        Text("\(proposed) proposed, \(skipped) already current or skipped, \(rejected) rejected.")
          .font(.caption)
          .foregroundColor(.secondary)
      }
      if let error = model.skillImportError {
        Text(error)
          .font(.caption)
          .foregroundColor(.red)
          .fixedSize(horizontal: false, vertical: true)
      }
    }
  }

  @ViewBuilder
  private var candidates: some View {
    VStack(alignment: .leading, spacing: 8) {
      HStack {
        Text("Candidates")
          .font(.headline)
        Spacer()
        Button("Select All") {
          selectedIndexes = Set(model.skillImportPlan?.proposed.map(\.id) ?? [])
        }
        .disabled(model.skillImportPlan?.proposed.isEmpty ?? true)
        Button("Clear") {
          selectedIndexes.removeAll()
        }
        .disabled(selectedIndexes.isEmpty)
      }

      if model.isLoadingSkillImportPlan && model.skillImportPlan == nil {
        VStack(spacing: 8) {
          ProgressView()
          Text("Scanning skills…")
            .font(.callout)
            .foregroundColor(.secondary)
        }
        .frame(maxWidth: .infinity, minHeight: 220)
      } else if let plan = model.skillImportPlan, !plan.proposed.isEmpty {
        ScrollView {
          LazyVStack(alignment: .leading, spacing: 8) {
            ForEach(plan.proposed) { candidate in
              SkillImportRowView(
                candidate: candidate,
                isSelected: selectedIndexes.contains(candidate.id),
                isBusy: model.isImportingSkills
              ) {
                toggle(candidate.id)
              }
            }
            if !plan.skipped.isEmpty || !plan.rejected.isEmpty || !plan.warnings.isEmpty {
              SkillImportNotesView(plan: plan)
            }
          }
          .padding(.vertical, 2)
        }
      } else if let plan = model.skillImportPlan {
        VStack(spacing: 8) {
          Image(systemName: "checkmark.circle")
            .font(.system(size: 26, weight: .semibold))
            .foregroundColor(.secondary)
          Text(plan.note.isEmpty ? "No skills need importing." : plan.note)
            .font(.callout)
            .foregroundColor(.secondary)
            .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity, minHeight: 220)
        if !plan.skipped.isEmpty || !plan.rejected.isEmpty || !plan.warnings.isEmpty {
          SkillImportNotesView(plan: plan)
        }
      } else {
        Text("Choose a folder, SKILL.md, or run the default scan.")
          .font(.callout)
          .foregroundColor(.secondary)
          .frame(maxWidth: .infinity, minHeight: 220, alignment: .center)
      }
    }
  }

  private var footer: some View {
    HStack {
      Text("\(selectedIndexes.count) selected")
        .font(.caption)
        .foregroundColor(.secondary)
      Spacer()
      Button {
        importSelected()
      } label: {
        Label("Import Selected", systemImage: "square.and.arrow.down")
      }
      .keyboardShortcut(.defaultAction)
      .disabled(selectedIndexes.isEmpty || model.isImportingSkills || model.isLoadingSkillImportPlan)
    }
  }

  private func chooseSource() {
    let panel = NSOpenPanel()
    panel.canChooseFiles = true
    panel.canChooseDirectories = true
    panel.allowsMultipleSelection = false
    panel.prompt = "Scan"
    if panel.runModal() == .OK, let url = panel.urls.first {
      model.loadSkillImportPlan(sourcePath: url.path)
    }
  }

  private func toggle(_ id: Int) {
    if selectedIndexes.contains(id) {
      selectedIndexes.remove(id)
    } else {
      selectedIndexes.insert(id)
    }
  }

  private func importSelected() {
    let count = selectedIndexes.count
    guard Confirm.ask(
      title: "Import Skills",
      message: "Import \(count) skill\(count == 1 ? "" : "s") into the shared brain? Connected harnesses can install them on their next skills refresh."
    ) else {
      return
    }
    model.importSkills(indexes: Array(selectedIndexes))
  }
}

private struct SkillImportRowView: View {
  let candidate: SkillImportCandidate
  let isSelected: Bool
  let isBusy: Bool
  let toggle: () -> Void

  var body: some View {
    Button(action: toggle) {
      HStack(alignment: .top, spacing: 10) {
        Image(systemName: isSelected ? "checkmark.square.fill" : "square")
          .foregroundColor(isSelected ? .accentColor : .secondary)
          .frame(width: 18)
        VStack(alignment: .leading, spacing: 5) {
          HStack(alignment: .firstTextBaseline, spacing: 8) {
            Text("\(candidate.id). \(candidate.name.isEmpty ? "Unnamed skill" : candidate.name)")
              .font(.callout.weight(.semibold))
              .lineLimit(1)
            Text(candidate.action)
              .font(.caption2.weight(.semibold))
              .foregroundColor(.green)
              .padding(.horizontal, 6)
              .padding(.vertical, 2)
              .background(Color.green.opacity(0.18))
              .clipShape(Capsule())
            Spacer()
            Text("\(candidate.fileCount) files · \(candidate.formattedSize)")
              .font(.caption)
              .foregroundColor(.secondary)
          }
          Text(candidate.displayDescription)
            .font(.caption)
            .foregroundColor(.secondary)
            .lineLimit(2)
          Text(candidate.root)
            .font(.system(size: 11, design: .monospaced))
            .foregroundColor(.secondary)
            .lineLimit(1)
            .truncationMode(.middle)
        }
      }
      .padding(10)
      .frame(maxWidth: .infinity, alignment: .leading)
      .background(Color.secondary.opacity(isSelected ? 0.14 : 0.08))
      .clipShape(RoundedRectangle(cornerRadius: 8))
    }
    .buttonStyle(.plain)
    .disabled(isBusy)
  }
}

private struct SkillImportNotesView: View {
  let plan: SkillImportPlan
  @State private var expanded = false

  var body: some View {
    DisclosureGroup("Skipped and warnings", isExpanded: $expanded) {
      VStack(alignment: .leading, spacing: 6) {
        ForEach(plan.warnings, id: \.self) { warning in
          note("Warning", warning)
        }
        ForEach(Array(plan.skipped.enumerated()), id: \.offset) { _, skipped in
          note(skipped.name.isEmpty ? "Skipped" : skipped.name, skipped.reason)
        }
        ForEach(Array(plan.rejected.enumerated()), id: \.offset) { _, rejected in
          note(rejected.root.isEmpty ? "Rejected" : rejected.root, rejected.error)
        }
      }
      .padding(.top, 4)
    }
    .font(.caption)
    .foregroundColor(.secondary)
  }

  private func note(_ title: String, _ detail: String) -> some View {
    VStack(alignment: .leading, spacing: 2) {
      Text(title)
        .font(.caption.weight(.semibold))
      Text(detail)
        .fixedSize(horizontal: false, vertical: true)
    }
  }
}
