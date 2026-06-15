import Darwin
import Foundation

/// Structured outcome of one bounded subprocess run. The UI only ever consumes this
/// shape; raw process state never leaks into views.
public struct CommandResult: Sendable {
  public let exitCode: Int32?
  public let stdout: String
  public let stderr: String
  public let timedOut: Bool
  public let launchFailure: String?
  public let duration: TimeInterval

  public var succeeded: Bool {
    launchFailure == nil && !timedOut && exitCode == 0
  }

  public init(exitCode: Int32?, stdout: String, stderr: String, timedOut: Bool, launchFailure: String?, duration: TimeInterval) {
    self.exitCode = exitCode
    self.stdout = stdout
    self.stderr = stderr
    self.timedOut = timedOut
    self.launchFailure = launchFailure
    self.duration = duration
  }
}

/// Async process runner with a hard timeout and process-group kill. Output is read
/// off the main thread; oversized output is truncated rather than buffered forever.
public enum CommandRunner {
  public static let maxCapturedBytes = 4 * 1024 * 1024

  public static func run(
    executable: String,
    arguments: [String],
    timeout: TimeInterval,
    environment: [String: String]? = nil
  ) async -> CommandResult {
    let started = Date()
    guard FileManager.default.isExecutableFile(atPath: executable) else {
      return CommandResult(
        exitCode: nil,
        stdout: "",
        stderr: "executable not found or not executable: \(executable)",
        timedOut: false,
        launchFailure: "missing-executable",
        duration: Date().timeIntervalSince(started)
      )
    }

    return await withCheckedContinuation { continuation in
      DispatchQueue.global(qos: .userInitiated).async {
        continuation.resume(returning: spawnAndWait(
          executable: executable,
          arguments: arguments,
          timeout: timeout,
          environment: environment,
          started: started
        ))
      }
    }
  }

  private static func spawnAndWait(
    executable: String,
    arguments: [String],
    timeout: TimeInterval,
    environment: [String: String]?,
    started: Date
  ) -> CommandResult {
    var stdoutPipe: [Int32] = [-1, -1]
    var stderrPipe: [Int32] = [-1, -1]
    guard pipe(&stdoutPipe) == 0, pipe(&stderrPipe) == 0 else {
      closeIfOpen(stdoutPipe[0])
      closeIfOpen(stdoutPipe[1])
      closeIfOpen(stderrPipe[0])
      closeIfOpen(stderrPipe[1])
      return launchFailure("failed to create output pipes", started: started)
    }

    let stdoutRead = FileDescriptorBox(stdoutPipe[0])
    let stderrRead = FileDescriptorBox(stderrPipe[0])
    var stdinFd: Int32 = open("/dev/null", O_RDONLY)
    if stdinFd < 0 {
      stdinFd = STDIN_FILENO
    }

    var actions: posix_spawn_file_actions_t? = nil
    var attrs: posix_spawnattr_t? = nil
    posix_spawn_file_actions_init(&actions)
    posix_spawnattr_init(&attrs)
    defer {
      posix_spawn_file_actions_destroy(&actions)
      posix_spawnattr_destroy(&attrs)
    }

    posix_spawn_file_actions_adddup2(&actions, stdinFd, STDIN_FILENO)
    posix_spawn_file_actions_adddup2(&actions, stdoutPipe[1], STDOUT_FILENO)
    posix_spawn_file_actions_adddup2(&actions, stderrPipe[1], STDERR_FILENO)
    posix_spawn_file_actions_addclose(&actions, stdoutPipe[0])
    posix_spawn_file_actions_addclose(&actions, stderrPipe[0])
    posix_spawn_file_actions_addclose(&actions, stdoutPipe[1])
    posix_spawn_file_actions_addclose(&actions, stderrPipe[1])
    if stdinFd != STDIN_FILENO {
      posix_spawn_file_actions_addclose(&actions, stdinFd)
    }

    let flags = Int16(POSIX_SPAWN_SETPGROUP)
    posix_spawnattr_setflags(&attrs, flags)
    posix_spawnattr_setpgroup(&attrs, 0)

    var pid = pid_t()
    let argvStorage = CStringArray([executable] + arguments)
    let envStorage = CStringArray(environmentStrings(environment))
    let spawnResult = posix_spawn(
      &pid,
      executable,
      &actions,
      &attrs,
      argvStorage.pointer,
      envStorage.pointer
    )

    if stdinFd != STDIN_FILENO {
      closeIfOpen(stdinFd)
    }
    closeIfOpen(stdoutPipe[1])
    closeIfOpen(stderrPipe[1])

    guard spawnResult == 0 else {
      stdoutRead.close()
      stderrRead.close()
      return launchFailure("failed to launch: \(String(cString: strerror(spawnResult)))", started: started)
    }

    let outputGroup = DispatchGroup()
    let outputLock = NSLock()
    var stdoutData = Data()
    var stderrData = Data()
    outputGroup.enter()
    DispatchQueue.global(qos: .utility).async {
      let data = readCapped(fd: stdoutRead)
      outputLock.withLock { stdoutData = data }
      outputGroup.leave()
    }
    outputGroup.enter()
    DispatchQueue.global(qos: .utility).async {
      let data = readCapped(fd: stderrRead)
      outputLock.withLock { stderrData = data }
      outputGroup.leave()
    }

    let timeoutState = TimeoutState()
    let killQueue = DispatchQueue.global(qos: .userInitiated)
    let termWork = DispatchWorkItem {
      timeoutState.markTimedOut()
      kill(-pid, SIGTERM)
      stdoutRead.close()
      stderrRead.close()
    }
    let killWork = DispatchWorkItem {
      timeoutState.markTimedOut()
      kill(-pid, SIGKILL)
      stdoutRead.close()
      stderrRead.close()
    }
    killQueue.asyncAfter(deadline: .now() + timeout, execute: termWork)
    killQueue.asyncAfter(deadline: .now() + timeout + 3.0, execute: killWork)

    var status: Int32 = 0
    while waitpid(pid, &status, 0) == -1 {
      if errno != EINTR {
        break
      }
    }

    let elapsedAfterParent = Date().timeIntervalSince(started)
    let remaining = max(0, timeout + 3.5 - elapsedAfterParent)
    if outputGroup.wait(timeout: .now() + remaining) != .success {
      timeoutState.markTimedOut()
      kill(-pid, SIGKILL)
      stdoutRead.close()
      stderrRead.close()
      outputGroup.wait()
    }

    termWork.cancel()
    killWork.cancel()

    let exitCode = exitCodeFromWaitStatus(status)
    return CommandResult(
      exitCode: exitCode,
      stdout: String(data: stdoutData, encoding: .utf8) ?? "",
      stderr: String(data: stderrData, encoding: .utf8) ?? "",
      timedOut: timeoutState.timedOut,
      launchFailure: nil,
      duration: Date().timeIntervalSince(started)
    )
  }

  private static func launchFailure(_ message: String, started: Date) -> CommandResult {
    CommandResult(
      exitCode: nil,
      stdout: "",
      stderr: message,
      timedOut: false,
      launchFailure: "launch-failed",
      duration: Date().timeIntervalSince(started)
    )
  }

  private static func readCapped(fd box: FileDescriptorBox) -> Data {
    var collected = Data()
    var truncated = false
    var buffer = [UInt8](repeating: 0, count: 16 * 1024)
    while true {
      let count = box.read(into: &buffer)
      if count <= 0 {
        break
      }
      if collected.count < maxCapturedBytes {
        let remaining = maxCapturedBytes - collected.count
        if count > remaining {
          collected.append(buffer, count: remaining)
          truncated = true
        } else {
          collected.append(buffer, count: count)
        }
      } else {
        truncated = true
      }
    }
    box.close()
    if truncated {
      collected.append(Data("\n[output truncated]".utf8))
    }
    return collected
  }

  private static func environmentStrings(_ environment: [String: String]?) -> [String] {
    let values = environment ?? ProcessInfo.processInfo.environment
    return values.map { "\($0.key)=\($0.value)" }
  }

  private static func exitCodeFromWaitStatus(_ status: Int32) -> Int32? {
    let statusBits = status & 0o177
    if statusBits == 0 {
      return (status >> 8) & 0x000000ff
    }
    if statusBits != 0o177 {
      return 128 + statusBits
    }
    return nil
  }

  private static func closeIfOpen(_ fd: Int32) {
    if fd >= 0 {
      close(fd)
    }
  }
}

private final class FileDescriptorBox: @unchecked Sendable {
  private let lock = NSLock()
  private var fd: Int32

  init(_ fd: Int32) {
    self.fd = fd
  }

  func read(into buffer: inout [UInt8]) -> Int {
    let current = lock.withLock { fd }
    if current < 0 {
      return -1
    }
    return Darwin.read(current, &buffer, buffer.count)
  }

  func close() {
    let current = lock.withLock { () -> Int32 in
      let current = fd
      fd = -1
      return current
    }
    if current >= 0 {
      Darwin.close(current)
    }
  }
}

private final class TimeoutState: @unchecked Sendable {
  private let lock = NSLock()
  private var value = false

  var timedOut: Bool {
    lock.withLock { value }
  }

  func markTimedOut() {
    lock.withLock {
      value = true
    }
  }
}

private final class CStringArray {
  private let storage: [UnsafeMutablePointer<CChar>?]
  let pointer: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>

  init(_ strings: [String]) {
    self.storage = strings.map { strdup($0) } + [nil]
    self.pointer = UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>.allocate(capacity: storage.count)
    for (index, value) in storage.enumerated() {
      pointer[index] = value
    }
  }

  deinit {
    for value in storage {
      free(value)
    }
    pointer.deallocate()
  }
}
