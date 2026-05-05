import Foundation
import OmniACPModel
#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

public enum PermissionStrategy: Sendable {
    case autoApprove
    case consolePrompt
}

public actor DefaultClientDelegate: ClientDelegate {
    private let rootDirectory: URL
    private let permissionStrategy: PermissionStrategy
    private let defaultOutputByteLimit: Int
    private var terminals: [String: LocalTerminalSession] = [:]

    public init(
        rootDirectory: URL,
        permissionStrategy: PermissionStrategy = .autoApprove,
        defaultOutputByteLimit: Int = 256_000
    ) {
        self.rootDirectory = rootDirectory.standardizedFileURL
        self.permissionStrategy = permissionStrategy
        self.defaultOutputByteLimit = defaultOutputByteLimit
    }

    public func handleReadTextFile(_ request: FileSystemReadTextFile.Parameters) async throws -> FileSystemReadTextFile.Result {
        let url = try resolvePath(request.path)
        let text = try String(contentsOf: url, encoding: .utf8)
        if let line = request.line, line > 0 {
            let lines = text.split(separator: "\n", omittingEmptySubsequences: false)
            let start = max(0, line - 1)
            let end = min(lines.count, start + max(request.limit ?? lines.count, 0))
            let slice = lines[start..<end].joined(separator: "\n")
            return .init(content: slice + (slice.isEmpty ? "" : "\n"), totalLines: lines.count)
        }
        return .init(content: text, totalLines: text.split(separator: "\n", omittingEmptySubsequences: false).count)
    }

    public func handleWriteTextFile(_ request: FileSystemWriteTextFile.Parameters) async throws -> FileSystemWriteTextFile.Result {
        let url = try resolvePath(request.path)
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data(request.content.utf8).write(to: url)
        return .init()
    }

    public func handlePermissionRequest(_ request: SessionRequestPermission.Parameters) async throws -> SessionRequestPermission.Result {
        let options = request.options ?? []
        switch permissionStrategy {
        case .autoApprove:
            if let option = options.first(where: { $0.kind == "allow_once" || $0.kind == "allow_always" }) ?? options.first {
                return .init(outcome: .selected(option.optionID))
            }
            return .init(outcome: .cancelled)
        case .consolePrompt:
            let message = request.message ?? request.toolCall?.toolCallID ?? "permission"
            return await promptForPermissionChoice(message: message, options: options)
        }
    }

    public func handleTerminalCreate(_ request: TerminalCreate.Parameters) async throws -> TerminalCreate.Result {
        #if os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
        throw ClientError.unsupportedPlatform("terminal/create is unavailable on this platform")
        #else
        let cwdURL: URL?
        if let cwd = request.cwd, !cwd.isEmpty {
            cwdURL = try resolvePath(cwd)
        } else {
            cwdURL = rootDirectory
        }
        let session = try LocalTerminalSession(
            command: request.command,
            args: request.args ?? [],
            cwd: cwdURL,
            env: request.env ?? [],
            outputByteLimit: request.outputByteLimit ?? defaultOutputByteLimit
        )
        terminals[session.id] = session
        return .init(terminalID: TerminalID(session.id))
        #endif
    }

    public func handleTerminalOutput(_ request: TerminalOutput.Parameters) async throws -> TerminalOutput.Result {
        guard let session = terminals[request.terminalID.value] else {
            throw ClientError.invalidResponse("Unknown terminal \(request.terminalID.value)")
        }
        return session.outputResult()
    }

    public func handleTerminalWaitForExit(_ request: TerminalWaitForExit.Parameters) async throws -> TerminalWaitForExit.Result {
        guard let session = terminals[request.terminalID.value] else {
            throw ClientError.invalidResponse("Unknown terminal \(request.terminalID.value)")
        }
        return try await session.waitForExitResult()
    }

    public func handleTerminalKill(_ request: TerminalKill.Parameters) async throws -> TerminalKill.Result {
        guard let session = terminals[request.terminalID.value] else {
            throw ClientError.invalidResponse("Unknown terminal \(request.terminalID.value)")
        }
        session.terminate()
        return .init(success: true)
    }

    public func handleTerminalRelease(_ request: TerminalRelease.Parameters) async throws -> TerminalRelease.Result {
        guard let session = terminals.removeValue(forKey: request.terminalID.value) else {
            throw ClientError.invalidResponse("Unknown terminal \(request.terminalID.value)")
        }
        session.release()
        return .init(success: true)
    }

    private func resolvePath(_ rawPath: String) throws -> URL {
        let candidateURL: URL
        if rawPath.hasPrefix("/") {
            candidateURL = URL(fileURLWithPath: rawPath)
        } else {
            candidateURL = rootDirectory.appendingPathComponent(rawPath)
        }
        let standardized = candidateURL.standardizedFileURL
        guard standardized.path == rootDirectory.path || standardized.path.hasPrefix(rootDirectory.path + "/") else {
            throw ClientError.pathOutsideRoot(rawPath)
        }
        return standardized
    }

    private nonisolated func promptForPermissionChoice(
        message: String,
        options: [PermissionOption]
    ) async -> SessionRequestPermission.Result {
        await Task.detached(priority: .userInitiated) {
            print("[ACP] Permission request: \(message)")
            for (index, option) in options.enumerated() {
                print("  [\(index + 1)] \(option.name) (\(option.kind))")
            }
            print("Choose an option number, or press Enter to cancel:", terminator: " ")
            fflush(stdout)
            guard let input = readLine(), let choice = Int(input), choice > 0, choice <= options.count else {
                return .init(outcome: .cancelled)
            }
            return .init(outcome: .selected(options[choice - 1].optionID))
        }.value
    }
}

#if !os(iOS) && !os(tvOS) && !os(watchOS) && !os(visionOS)
private final class LocalTerminalSession: @unchecked Sendable {
    let id: String
    private let process: Process
    private let outputPipe: Pipe
    private let errorPipe: Pipe
    private let lock = NSLock()
    private var outputBuffer = ""
    private var wasTruncated = false
    private let outputByteLimit: Int

    init(command: String, args: [String], cwd: URL?, env: [EnvVariable], outputByteLimit: Int) throws {
        self.id = UUID().uuidString
        self.process = Process()
        self.outputPipe = Pipe()
        self.errorPipe = Pipe()
        self.outputByteLimit = outputByteLimit

        process.executableURL = URL(fileURLWithPath: command)
        process.arguments = args
        process.standardOutput = outputPipe
        process.standardError = errorPipe
        if let cwd {
            process.currentDirectoryURL = cwd
        }
        var environment = ProcessInfo.processInfo.environment
        for entry in env {
            environment[entry.name] = entry.value
        }
        process.environment = environment

        installReadabilityHandler(outputPipe.fileHandleForReading)
        installReadabilityHandler(errorPipe.fileHandleForReading)
        try process.run()
    }

    func outputResult() -> TerminalOutput.Result {
        lock.lock()
        let output = outputBuffer
        let truncated = wasTruncated
        let status = process.isRunning ? nil : TerminalExitStatus(exitCode: Int(process.terminationStatus))
        lock.unlock()
        return .init(output: output, exitStatus: status, truncated: truncated)
    }

    func waitForExitResult() async throws -> TerminalWaitForExit.Result {
        while process.isRunning {
            try await Task.sleep(for: .milliseconds(50))
        }
        return .init(exitCode: Int(process.terminationStatus), signal: nil)
    }

    func terminate() {
        if process.isRunning {
            process.terminate()
            usleep(200_000)
            #if canImport(Darwin) || canImport(Glibc)
            if process.isRunning {
                _ = kill(process.processIdentifier, SIGKILL)
            }
            #endif
        }
        outputPipe.fileHandleForReading.readabilityHandler = nil
        errorPipe.fileHandleForReading.readabilityHandler = nil
    }

    func release() {
        outputPipe.fileHandleForReading.readabilityHandler = nil
        errorPipe.fileHandleForReading.readabilityHandler = nil
    }

    private func installReadabilityHandler(_ handle: FileHandle) {
        handle.readabilityHandler = { [weak self] fileHandle in
            let data = fileHandle.availableData
            guard !data.isEmpty else {
                fileHandle.readabilityHandler = nil
                return
            }
            guard let chunk = String(data: data, encoding: .utf8), !chunk.isEmpty else {
                return
            }
            self?.append(chunk)
        }
    }

    private func append(_ chunk: String) {
        lock.lock()
        defer { lock.unlock() }
        outputBuffer += chunk
        if outputByteLimit > 0 && outputBuffer.utf8.count > outputByteLimit {
            wasTruncated = true
            while outputBuffer.utf8.count > outputByteLimit, !outputBuffer.isEmpty {
                outputBuffer.removeFirst()
            }
        }
    }
}
#endif
