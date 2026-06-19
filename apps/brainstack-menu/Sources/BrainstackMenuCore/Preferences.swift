import Foundation

/// UserDefaults-backed preferences. Paths and toggles only; secrets never live here.
/// UserDefaults is documented thread-safe; the conformance is deliberate.
public struct Preferences: @unchecked Sendable {
  public static let pollIntervalChoices: [TimeInterval] = [15, 30, 60, 300]

  private enum Keys {
    static let binaryPath = "brainctlPath"
    static let configPath = "configPath"
    static let pollInterval = "pollIntervalSeconds"
    static let operatorMode = "operatorModeEnabled"
    static let notifications = "notificationsEnabled"
    static let launchAtLogin = "launchAtLoginEnabled"
    static let startTailscaleOnLaunch = "startTailscaleOnLaunch"
  }

  private let defaults: UserDefaults

  public init(defaults: UserDefaults = .standard) {
    self.defaults = defaults
  }

  public var binaryPathPreference: String? {
    get { defaults.string(forKey: Keys.binaryPath) }
    nonmutating set { defaults.set(newValue, forKey: Keys.binaryPath) }
  }

  public var configPath: String {
    get { defaults.string(forKey: Keys.configPath) ?? BrainctlClient.defaultConfigPath() }
    nonmutating set { defaults.set(newValue, forKey: Keys.configPath) }
  }

  public var pollInterval: TimeInterval {
    get {
      let stored = defaults.double(forKey: Keys.pollInterval)
      return Preferences.pollIntervalChoices.contains(stored) ? stored : 30
    }
    nonmutating set { defaults.set(newValue, forKey: Keys.pollInterval) }
  }

  public var operatorModeEnabled: Bool {
    get { defaults.bool(forKey: Keys.operatorMode) }
    nonmutating set { defaults.set(newValue, forKey: Keys.operatorMode) }
  }

  public var notificationsEnabled: Bool {
    get { defaults.bool(forKey: Keys.notifications) }
    nonmutating set { defaults.set(newValue, forKey: Keys.notifications) }
  }

  public var launchAtLoginEnabled: Bool {
    get { defaults.bool(forKey: Keys.launchAtLogin) }
    nonmutating set { defaults.set(newValue, forKey: Keys.launchAtLogin) }
  }

  public var startTailscaleOnLaunch: Bool {
    get { defaults.bool(forKey: Keys.startTailscaleOnLaunch) }
    nonmutating set { defaults.set(newValue, forKey: Keys.startTailscaleOnLaunch) }
  }
}
