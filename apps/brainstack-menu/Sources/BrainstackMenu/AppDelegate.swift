import AppKit
import SwiftUI
import UniformTypeIdentifiers
import BrainstackMenuCore

@MainActor
private extension CGRect {
  var area: CGFloat { isNull ? 0 : width * height }
}

@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate, NSPopoverDelegate {
  private var statusItem: NSStatusItem!
  private var popover: NSPopover!
  private var preferencesWindow: NSWindow?
  private let model = AppModel()

  func applicationDidFinishLaunching(_ notification: Notification) {
    // Menu bar accessory app: no Dock icon, no main window.
    NSApp.setActivationPolicy(.accessory)

    statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
    if let button = statusItem.button {
      button.action = #selector(togglePopover(_:))
      button.target = self
    }

    popover = NSPopover()
    popover.behavior = .transient
    popover.delegate = self
    popover.contentViewController = NSHostingController(
      rootView: DashboardView(model: model, openPreferences: { [weak self] in
        self?.openPreferences()
      })
    )

    model.onStateChange = { [weak self] in
      self?.renderStatusItem()
    }
    renderStatusItem()
    model.startPolling()

    // Test/debug affordance: open the popover right after launch so UI smoke checks
    // can run unattended. No effect unless the env var is set.
    if ProcessInfo.processInfo.environment["BRAINSTACK_MENU_AUTO_OPEN"] == "1" {
      DispatchQueue.main.asyncAfter(deadline: .now() + 1.5) { [weak self] in
        // Drive the real button action path (same dispatch as a user click), not a
        // direct method call, so positioning bugs in the click path reproduce here.
        self?.statusItem.button?.performClick(nil)
        DispatchQueue.main.asyncAfter(deadline: .now() + 3.0) {
          self?.reportDebugGeometryAndSnapshot()
        }
      }
    }
  }

  /// Debug-only: print the live popover frame vs the screen's visible frame and
  /// optionally render a pixel snapshot of the dashboard, then exit if requested.
  /// Lets UI layout be verified headlessly without screen-recording permissions.
  private func reportDebugGeometryAndSnapshot() {
    let env = ProcessInfo.processInfo.environment
    if let window = popover.contentViewController?.view.window {
      let frame = window.frame
      // Multi-display: judge containment against the screen the popover actually
      // occupies (largest intersection), not NSScreen.main.
      let screen = window.screen
        ?? NSScreen.screens.max(by: { $0.frame.intersection(frame).area < $1.frame.intersection(frame).area })
        ?? NSScreen.main
      let visible = screen?.visibleFrame ?? .zero
      let physical = screen?.frame ?? .zero
      // Anchored menu bar popovers legitimately poke their arrow a few points into
      // the menu bar band, so judge against the physical screen with the bottom and
      // sides held to the visible frame.
      let acceptable = physical.contains(frame) && frame.minY >= visible.minY - 1 && frame.minX >= visible.minX - 1 && frame.maxX <= visible.maxX + 1
      let payload: [String: Any] = [
        "popover_frame": [frame.origin.x, frame.origin.y, frame.size.width, frame.size.height],
        "screen_visible_frame": [visible.origin.x, visible.origin.y, visible.size.width, visible.size.height],
        "screen_frame": [physical.origin.x, physical.origin.y, physical.size.width, physical.size.height],
        "window_screen_known": window.screen != nil,
        "screen_count": NSScreen.screens.count,
        "fully_on_screen": acceptable
      ]
      if let data = try? JSONSerialization.data(withJSONObject: payload),
         let text = String(data: data, encoding: .utf8) {
        print("BRAINSTACK_MENU_GEOMETRY \(text)")
      }
    } else {
      print("BRAINSTACK_MENU_GEOMETRY {\"error\": \"popover window unavailable\"}")
    }
    if let snapshotPath = env["BRAINSTACK_MENU_SNAPSHOT_PATH"], !snapshotPath.isEmpty {
      // Snapshot the real rendered popover view (no screen-recording permission needed).
      if let view = popover.contentViewController?.view,
         let bitmap = view.bitmapImageRepForCachingDisplay(in: view.bounds) {
        view.cacheDisplay(in: view.bounds, to: bitmap)
        if let png = bitmap.representation(using: .png, properties: [:]) {
          try? png.write(to: URL(fileURLWithPath: snapshotPath))
          print("BRAINSTACK_MENU_SNAPSHOT \(snapshotPath)")
        } else {
          print("BRAINSTACK_MENU_SNAPSHOT failed")
        }
      } else {
        print("BRAINSTACK_MENU_SNAPSHOT failed")
      }
    }
    if env["BRAINSTACK_MENU_EXIT_AFTER_REPORT"] == "1" {
      NSApp.terminate(nil)
    }
  }

  @objc private func togglePopover(_ sender: Any?) {
    if popover.isShown {
      popover.performClose(sender)
      return
    }
    // The transient popover closes itself when the user clicks the status icon
    // (the anchor window is not its positioning view); without this guard the same
    // click would immediately reopen it.
    if Date().timeIntervalSince(lastPopoverCloseAt) < 0.3 {
      return
    }
    // Refresh when the menu opens, per spec.
    model.refresh()
    if model.operatorModeEnabled {
      model.loadProposals()
    }
    // Give AppKit a stable, already-laid-out size before showing; popovers shown
    // with a tiny initial size that immediately balloons can be repositioned badly.
    if let view = popover.contentViewController?.view {
      view.layoutSubtreeIfNeeded()
      let fitting = view.fittingSize
      if fitting.width > 10, fitting.height > 10 {
        popover.contentSize = fitting
      }
    }
    // Defer past the status-bar click event before showing: NSPopover shown
    // synchronously from menu-bar event tracking can mis-place at the mouse location.
    DispatchQueue.main.async { [weak self] in
      self?.showFromAnchorWindow()
      self?.popover.contentViewController?.view.window?.makeKey()
    }
  }

  private var anchorWindow: NSWindow?
  private var lastPopoverCloseAt = Date.distantPast

  /// Show the popover from an app-owned invisible 1pt window pinned just below the
  /// menu bar. Never anchor to the status button itself: crowded or notched menu
  /// bars can give the button's window phantom coordinates, and AppKit then falls
  /// back to centering the popover on the mouse.
  private func showFromAnchorWindow() {
    let buttonWindow = statusItem.button?.window
    let screen = buttonWindow?.screen ?? NSScreen.main ?? NSScreen.screens.first
    guard let screen else {
      return
    }
    let visible = screen.visibleFrame
    // Prefer the icon's x position when its window is sane; otherwise top-right.
    var anchorX = visible.maxX - 220
    if let frame = buttonWindow?.frame,
       screen.frame.intersects(frame),
       frame.midX > visible.minX,
       frame.midX < visible.maxX {
      anchorX = frame.midX
    }
    // Keep the full popover width on screen.
    let halfWidth = max(popover.contentSize.width / 2, 190) + 8
    anchorX = min(max(anchorX, visible.minX + halfWidth), visible.maxX - halfWidth)
    let anchorRect = NSRect(x: anchorX - 0.5, y: visible.maxY - 1, width: 1, height: 1)

    let window = anchorWindow ?? {
      let created = NSWindow(contentRect: anchorRect, styleMask: .borderless, backing: .buffered, defer: false)
      created.isOpaque = false
      created.backgroundColor = .clear
      created.level = .statusBar
      created.ignoresMouseEvents = true
      created.collectionBehavior = [.canJoinAllSpaces, .transient]
      created.isReleasedWhenClosed = false
      anchorWindow = created
      return created
    }()
    window.setFrame(anchorRect, display: false)
    window.orderFrontRegardless()
    if let anchorView = window.contentView {
      popover.show(relativeTo: anchorView.bounds, of: anchorView, preferredEdge: .minY)
    }
  }

  private func renderStatusItem() {
    guard let button = statusItem.button else {
      return
    }
    button.image = Self.statusImage(for: model.overallState, stale: model.stale)
    button.toolTip = model.tooltip
  }

  static func statusImage(for state: OverallState, stale: Bool) -> NSImage {
    let size = NSSize(width: 18, height: 18)
    let image = NSImage(size: size, flipped: false) { rect in
      let color: NSColor
      switch state {
      case .green: color = .systemGreen
      case .yellow: color = .systemYellow
      case .red: color = .systemRed
      case .gray: color = .systemGray
      }
      let circleRect = rect.insetBy(dx: 4.5, dy: 4.5)
      let path = NSBezierPath(ovalIn: circleRect)
      (stale ? color.withAlphaComponent(0.45) : color).setFill()
      path.fill()
      NSColor.labelColor.withAlphaComponent(0.35).setStroke()
      path.lineWidth = 0.8
      path.stroke()
      return true
    }
    image.isTemplate = false
    return image
  }

  func openPreferences() {
    if preferencesWindow == nil {
      let hosting = NSHostingController(rootView: PreferencesView(model: model))
      let window = NSWindow(contentViewController: hosting)
      window.title = "Brainstack Menu Preferences"
      window.styleMask = [.titled, .closable]
      window.isReleasedWhenClosed = false
      preferencesWindow = window
    }
    preferencesWindow?.center()
    preferencesWindow?.makeKeyAndOrderFront(nil)
    NSApp.activate(ignoringOtherApps: true)
  }

  func popoverDidClose(_ notification: Notification) {
    lastPopoverCloseAt = Date()
    anchorWindow?.orderOut(nil)
    renderStatusItem()
  }
}
