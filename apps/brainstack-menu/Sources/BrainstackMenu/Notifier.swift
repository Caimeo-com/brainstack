import Foundation
import UserNotifications

/// Quiet-by-default notification sender. UNUserNotificationCenter requires a real
/// app bundle; when running as a bare executable (swift run), notifications are
/// silently skipped so the app stays fail-open.
final class Notifier {
  private var authorized = false
  private let bundled = Bundle.main.bundleIdentifier != nil

  init() {
    guard bundled else {
      return
    }
    UNUserNotificationCenter.current().requestAuthorization(options: [.alert]) { [weak self] granted, _ in
      self?.authorized = granted
    }
  }

  func notify(title: String, body: String) {
    guard bundled, authorized else {
      return
    }
    let content = UNMutableNotificationContent()
    content.title = title
    content.body = body
    let request = UNNotificationRequest(identifier: UUID().uuidString, content: content, trigger: nil)
    UNUserNotificationCenter.current().add(request)
  }
}
