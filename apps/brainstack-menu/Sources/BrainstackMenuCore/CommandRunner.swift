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

/// Async process runner with a hard timeout and SIGKILL escalation. Output is read
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
        let process = Process()
        process.executableURL = URL(fileURLWithPath: executable)
        process.arguments = arguments
        if let environment {
          process.environment = environment
        }

        let stdoutPipe = Pipe()
        let stderrPipe = Pipe()
        process.standardOutput = stdoutPipe
        process.standardError = stderrPipe
        process.standardInput = FileHandle.nullDevice

        // Drain pipes concurrently so a chatty child cannot deadlock on a full pipe.
        let group = DispatchGroup()
        var stdoutData = Data()
        var stderrData = Data()
        group.enter()
        DispatchQueue.global(qos: .utility).async {
          stdoutData = Self.readCapped(handle: stdoutPipe.fileHandleForReading)
          group.leave()
        }
        group.enter()
        DispatchQueue.global(qos: .utility).async {
          stderrData = Self.readCapped(handle: stderrPipe.fileHandleForReading)
          group.leave()
        }

        do {
          try process.run()
        } catch {
          continuation.resume(returning: CommandResult(
            exitCode: nil,
            stdout: "",
            stderr: "failed to launch: \(error.localizedDescription)",
            timedOut: false,
            launchFailure: "launch-failed",
            duration: Date().timeIntervalSince(started)
          ))
          return
        }

        var timedOut = false
        let killQueue = DispatchQueue.global(qos: .userInitiated)
        let termWork = DispatchWorkItem {
          if process.isRunning {
            timedOut = true
            process.terminate()
          }
        }
        let killWork = DispatchWorkItem {
          if process.isRunning {
            timedOut = true
            kill(process.processIdentifier, SIGKILL)
          }
        }
        killQueue.asyncAfter(deadline: .now() + timeout, execute: termWork)
        killQueue.asyncAfter(deadline: .now() + timeout + 3.0, execute: killWork)

        process.waitUntilExit()
        termWork.cancel()
        killWork.cancel()
        group.wait()

        continuation.resume(returning: CommandResult(
          exitCode: process.terminationStatus,
          stdout: String(data: stdoutData, encoding: .utf8) ?? "",
          stderr: String(data: stderrData, encoding: .utf8) ?? "",
          timedOut: timedOut,
          launchFailure: nil,
          duration: Date().timeIntervalSince(started)
        ))
      }
    }
  }

  private static func readCapped(handle: FileHandle) -> Data {
    var collected = Data()
    var truncated = false
    while true {
      let chunk = handle.availableData
      if chunk.isEmpty {
        break
      }
      if collected.count < maxCapturedBytes {
        collected.append(chunk.prefix(maxCapturedBytes - collected.count))
      } else {
        truncated = true
      }
    }
    if truncated {
      collected.append(Data("\n[output truncated]".utf8))
    }
    return collected
  }
}
