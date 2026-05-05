import Foundation

// MARK: - Tool Handler

// Note: `ToolHandler` itself is value-free and checked `Sendable`; only the internal
// helper boxes below use lock-backed synchronization for process integration.
// execute() still performs blocking process I/O, but stdout/stderr are drained
// concurrently and process termination is awaited without the fast-exit continuation race.
public final class ToolHandler: NodeHandler, Sendable {
    public let handlerType: HandlerType = .tool

    public init() {}

    public func execute(
        node: Node,
        context: PipelineContext,
        graph: Graph,
        logsRoot: URL
    ) async throws -> Outcome {
        var command = node.prompt
        if command.isEmpty, let cmdAttr = node.rawAttributes["tool_command"] {
            command = cmdAttr.stringValue
        }
        if command.isEmpty, let cmdAttr = node.rawAttributes["command"] {
            command = cmdAttr.stringValue
        }

        guard !command.isEmpty else {
            return .fail(reason: "Tool node \(node.id) has no command specified")
        }

        let process = Process()
        let stdout = Pipe()
        let stderr = Pipe()
        let exitSignal = _ProcessExitSignal()
        let stdoutData = _LockedDataBox()
        let stderrData = _LockedDataBox()

        process.executableURL = URL(fileURLWithPath: "/bin/sh")
        process.arguments = ["-c", command]
        process.standardOutput = stdout
        process.standardError = stderr
        process.terminationHandler = { _ in
            exitSignal.signal()
        }

        let stdoutQueue = DispatchQueue(label: "tool.stdout")
        let stderrQueue = DispatchQueue(label: "tool.stderr")
        stdoutQueue.async {
            stdoutData.store(stdout.fileHandleForReading.readDataToEndOfFile())
        }
        stderrQueue.async {
            stderrData.store(stderr.fileHandleForReading.readDataToEndOfFile())
        }

        do {
            try process.run()
        } catch {
            try? stdout.fileHandleForWriting.close()
            try? stderr.fileHandleForWriting.close()
            stdoutQueue.sync {}
            stderrQueue.sync {}
            return .fail(reason: "Failed to launch command for node \(node.id): \(error)")
        }

        await exitSignal.wait()
        stdoutQueue.sync {}
        stderrQueue.sync {}

        let outStr = String(data: stdoutData.load(), encoding: .utf8) ?? ""
        let errStr = String(data: stderrData.load(), encoding: .utf8) ?? ""
        let exitCode = process.terminationStatus

        let stageDir = logsRoot.appendingPathComponent(node.id)
        try FileManager.default.createDirectory(at: stageDir, withIntermediateDirectories: true)

        let logFile = stageDir.appendingPathComponent("tool_output.txt")
        var logContent = "Command: \(command)\nExit code: \(exitCode)\n\n--- stdout ---\n\(outStr)"
        if !errStr.isEmpty {
            logContent += "\n\n--- stderr ---\n\(errStr)"
        }
        try Data(logContent.utf8).write(to: logFile)

        if exitCode != 0 {
            return Outcome(
                status: .fail,
                contextUpdates: [
                    "tool.output": outStr,
                    "tool.stderr": errStr,
                    "tool.exit_code": String(exitCode),
                    "tool_stdout": outStr,
                    "tool_stderr": errStr,
                    "tool_exit_code": String(exitCode),
                ],
                failureReason: "Command exited with code \(exitCode)"
            )
        }

        return Outcome(
            status: .success,
            contextUpdates: [
                "tool.output": outStr,
                "tool.exit_code": "0",
                "tool_stdout": outStr,
                "tool_exit_code": "0",
            ],
            notes: "Command completed successfully"
        )
    }
}

private final class _LockedDataBox: @unchecked Sendable {
    private let lock = NSLock()
    private var data = Data()

    func store(_ data: Data) {
        lock.lock()
        self.data = data
        lock.unlock()
    }

    func load() -> Data {
        lock.lock()
        defer { lock.unlock() }
        return data
    }
}

private final class _ProcessExitSignal: @unchecked Sendable {
    private let lock = NSLock()
    private var hasExited = false
    private var continuation: CheckedContinuation<Void, Never>?

    func signal() {
        lock.lock()
        if let continuation {
            self.continuation = nil
            lock.unlock()
            continuation.resume()
            return
        }
        hasExited = true
        lock.unlock()
    }

    func wait() async {
        if takeExitedFlag() {
            return
        }
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            if installContinuation(continuation) {
                return
            }
            continuation.resume()
        }
    }

    private func takeExitedFlag() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return hasExited
    }

    private func installContinuation(_ continuation: CheckedContinuation<Void, Never>) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if hasExited {
            return false
        }
        self.continuation = continuation
        return true
    }
}
