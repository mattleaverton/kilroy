import Foundation
#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif


private actor LocalExecutionEnvironmentCleanupState {
    private var didCleanup = false

    func beginCleanup() -> Bool {
        if didCleanup {
            return false
        }
        didCleanup = true
        return true
    }
}

public final class LocalExecutionEnvironment: ExecutionEnvironment, @unchecked Sendable {
    private let workingDir: String
    private let fileManager = FileManager.default
    private let cleanupHandler: (@Sendable () async throws -> Void)?
    private let cleanupState = LocalExecutionEnvironmentCleanupState()

    // Sensitive env var patterns to exclude
    private static let sensitivePatterns = [
        "_API_KEY", "_SECRET", "_TOKEN", "_PASSWORD", "_CREDENTIAL",
    ]

    // Always include these env vars
    private static let safeVarNames: Set<String> = [
        "PATH", "HOME", "USER", "SHELL", "LANG", "TERM", "TMPDIR",
        "GOPATH", "CARGO_HOME", "NVM_DIR", "RUSTUP_HOME",
        "PYENV_ROOT", "RBENV_ROOT", "JAVA_HOME",
        "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME",
    ]

    public init(
        workingDir: String? = nil,
        cleanupHandler: (@Sendable () async throws -> Void)? = nil
    ) {
        self.workingDir = workingDir ?? FileManager.default.currentDirectoryPath
        self.cleanupHandler = cleanupHandler
    }

    // MARK: - File Operations

    public func readFile(path: String, offset: Int?, limit: Int?) async throws -> String {
        let resolvedPath = resolvePath(path)
        guard fileManager.fileExists(atPath: resolvedPath) else {
            throw ToolError.fileNotFound(resolvedPath)
        }

        guard let data = fileManager.contents(atPath: resolvedPath) else {
            throw ToolError.permissionDenied(resolvedPath)
        }

        if let content = String(data: data, encoding: .utf8) {
            let lines = content.components(separatedBy: "\n")
            let startLine = (offset ?? 1) - 1  // 1-based to 0-based
            let maxLines = limit ?? 2000
            let endLine = min(startLine + maxLines, lines.count)

            guard startLine >= 0 && startLine < lines.count else {
                return ""
            }

            var result = ""
            for i in startLine..<endLine {
                let lineNum = String(format: "%4d", i + 1)
                result += "\(lineNum) | \(lines[i])\n"
            }
            return result
        }

        if let renderedImage = renderImageFileIfSupported(data: data, path: resolvedPath) {
            return renderedImage
        }

        // Keep existing behavior for non-text, non-image files.
        // Some parity tools intentionally treat arbitrary binaries as unreadable.
        throw ToolError.binaryFile(resolvedPath)
    }

    public func writeFile(path: String, content: String) async throws {
        let resolvedPath = resolvePath(path)
        let dir = (resolvedPath as NSString).deletingLastPathComponent

        // Create parent directories
        if !fileManager.fileExists(atPath: dir) {
            try fileManager.createDirectory(atPath: dir, withIntermediateDirectories: true)
        }

        guard let data = content.data(using: .utf8) else {
            throw ToolError.writeError("Failed to encode content as UTF-8")
        }

        let success = fileManager.createFile(atPath: resolvedPath, contents: data)
        if !success {
            throw ToolError.permissionDenied(resolvedPath)
        }
    }

    public func fileExists(path: String) async -> Bool {
        fileManager.fileExists(atPath: resolvePath(path))
    }

    public func listDirectory(path: String, depth: Int) async throws -> [DirEntry] {
        let resolvedPath = resolvePath(path)
        var isDir: ObjCBool = false
        guard fileManager.fileExists(atPath: resolvedPath, isDirectory: &isDir), isDir.boolValue else {
            throw ToolError.fileNotFound(resolvedPath)
        }

        let contents = try fileManager.contentsOfDirectory(atPath: resolvedPath)
        return contents.compactMap { name in
            let fullPath = (resolvedPath as NSString).appendingPathComponent(name)
            var isSubDir: ObjCBool = false
            fileManager.fileExists(atPath: fullPath, isDirectory: &isSubDir)
            let attrs = try? fileManager.attributesOfItem(atPath: fullPath)
            let size = attrs?[.size] as? Int
            return DirEntry(name: name, isDir: isSubDir.boolValue, size: size)
        }.sorted(by: { $0.name < $1.name })
    }

    // MARK: - Command Execution

    public func execCommand(
        command: String,
        timeoutMs: Int,
        workingDir: String?,
        envVars: [String: String]?
    ) async throws -> ExecResult {
        let resolvedWorkingDir = workingDir.map { resolvePath($0) } ?? self.workingDir
        let startTime = Date()

        let process = Process()
        // Prefer bash for compatibility with common developer workflows, but fall back
        // to /bin/sh for minimal Linux environments where bash may be absent.
        let shellPath = fileManager.isExecutableFile(atPath: "/bin/bash") ? "/bin/bash" : "/bin/sh"
        process.executableURL = URL(fileURLWithPath: shellPath)
        process.arguments = ["-c", command]
        process.currentDirectoryURL = URL(fileURLWithPath: resolvedWorkingDir)

        // Filter environment variables
        var env = filteredEnvironment()
        if let extra = envVars {
            env.merge(extra) { _, new in new }
        }
        process.environment = env

        let stdoutPipe = Pipe()
        let stderrPipe = Pipe()
        process.standardOutput = stdoutPipe
        process.standardError = stderrPipe

        process.qualityOfService = .userInitiated

        try process.run()

        let pid = process.processIdentifier

        // Place the child in its own process group so kill(-pid, SIGTERM) works
        _ = setpgid(pid, pid)

        // Read output concurrently so long-running commands cannot deadlock on full pipes.
        // Use DispatchQueue (not Task.detached) for blocking I/O to avoid consuming
        // cooperative thread pool threads — blocking calls in Task.detached can starve
        // the pool when multiple subprocesses run concurrently.
        let stdoutBox = _DataBox()
        let stderrBox = _DataBox()
        let stdoutQueue = DispatchQueue(label: "exec.stdout.\(pid)")
        let stderrQueue = DispatchQueue(label: "exec.stderr.\(pid)")
        stdoutQueue.async { stdoutBox.set(stdoutPipe.fileHandleForReading.readDataToEndOfFile()) }
        stderrQueue.async { stderrBox.set(stderrPipe.fileHandleForReading.readDataToEndOfFile()) }

        // Timeout task
        let timeoutFlag = TimeoutFlag()
        let timeoutTask = Task.detached(priority: .userInitiated) {
            try await Task.sleep(nanoseconds: UInt64(timeoutMs) * 1_000_000)
            timeoutFlag.markTimedOut()
            // SIGTERM to process group
            self.terminateProcess(pid: pid, signal: SIGTERM)
            try await Task.sleep(nanoseconds: 2_000_000_000) // 2 seconds
            if process.isRunning {
                self.terminateProcess(pid: pid, signal: SIGKILL)
            }
        }

        // Wait for process using terminationHandler — this does NOT block any thread.
        // Unlike waitUntilExit() on GCD + withCheckedContinuation, terminationHandler
        // is called by the OS directly when the process exits, avoiding cooperative
        // thread pool starvation when multiple subprocesses run in parallel.
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            process.terminationHandler = { _ in
                continuation.resume()
            }
        }
        timeoutTask.cancel()

        // Wait for pipe reads to finish after process exits.
        stdoutQueue.sync {}
        stderrQueue.sync {}
        let stdoutData = stdoutBox.get()
        let stderrData = stderrBox.get()

        let stdout = String(data: stdoutData, encoding: .utf8) ?? ""
        let stderr = String(data: stderrData, encoding: .utf8) ?? ""
        let durationMs = Int(Date().timeIntervalSince(startTime) * 1000)

        return ExecResult(
            stdout: stdout,
            stderr: stderr,
            exitCode: process.terminationStatus,
            timedOut: timeoutFlag.isTimedOut(),
            durationMs: durationMs
        )
    }

    // MARK: - Search Operations

    public func grep(pattern: String, path: String, options: GrepOptions) async throws -> String {
        let resolvedPath = resolvePath(path)
        var args = ["rg", "--no-heading", "--line-number"]

        if options.caseInsensitive {
            args.append("-i")
        }
        args.append("--max-count=\(options.maxResults)")

        if let globFilter = options.globFilter {
            args.append("--glob=\(globFilter)")
        }

        args.append(pattern)
        args.append(resolvedPath)

        let cmd = args.map { $0.contains(" ") ? "'\($0)'" : $0 }.joined(separator: " ")
        let result = try await execCommand(command: cmd, timeoutMs: 10_000, workingDir: nil, envVars: nil)

        if result.exitCode == 0 {
            return result.stdout
        } else if result.exitCode == 1 {
            return "No matches found."
        } else {
            // Fallback to grep if rg not available
            var grepArgs = ["grep", "-rn"]
            if options.caseInsensitive { grepArgs.append("-i") }
            if let globFilter = options.globFilter {
                grepArgs.append("--include=\(globFilter)")
            }
            grepArgs.append(pattern)
            grepArgs.append(resolvedPath)
            let grepCmd = grepArgs.map { $0.contains(" ") ? "'\($0)'" : $0 }.joined(separator: " ")
            let grepResult = try await execCommand(command: grepCmd, timeoutMs: 10_000, workingDir: nil, envVars: nil)
            return grepResult.stdout.isEmpty ? "No matches found." : grepResult.stdout
        }
    }

    public func glob(pattern: String, path: String) async throws -> [String] {
        let resolvedPath = resolvePath(path)
        // Sort by modification time (newest first) as required by the spec
        let cmd: String
        #if os(Linux)
        cmd = "find '\(resolvedPath)' -path '\(resolvedPath)/\(pattern)' -type f -printf '%T@ %p\\n' 2>/dev/null | sort -rn | cut -d' ' -f2- | head -1000"
        #else
        // macOS/Darwin: use stat -f for modification time
        cmd = "find '\(resolvedPath)' -path '\(resolvedPath)/\(pattern)' -type f -print0 2>/dev/null | xargs -0 stat -f '%m %N' 2>/dev/null | sort -rn | cut -d' ' -f2- | head -1000"
        #endif
        let result = try await execCommand(command: cmd, timeoutMs: 10_000, workingDir: nil, envVars: nil)

        if result.stdout.isEmpty {
            return []
        }
        return result.stdout.components(separatedBy: "\n").filter { !$0.isEmpty }
    }

    // MARK: - Lifecycle

    public func initialize() async throws {
        // Create working directory if needed
        if !fileManager.fileExists(atPath: workingDir) {
            try fileManager.createDirectory(atPath: workingDir, withIntermediateDirectories: true)
        }
    }

    public func cleanup() async throws {
        guard await cleanupState.beginCleanup() else { return }
        if let cleanupHandler {
            try await cleanupHandler()
        }
    }

    // MARK: - Metadata

    public func workingDirectory() -> String {
        workingDir
    }

    public func platform() -> String {
        #if os(macOS)
        return "darwin"
        #elseif os(Linux)
        return "linux"
        #elseif os(Windows)
        return "windows"
        #else
        return "unknown"
        #endif
    }

    public func osVersion() -> String {
        let info = ProcessInfo.processInfo
        let version = info.operatingSystemVersion
        let versionString = "\(version.majorVersion).\(version.minorVersion).\(version.patchVersion)"
        #if os(macOS)
        return "Darwin \(versionString)"
        #elseif os(Linux)
        // Try to read /etc/os-release for distribution info
        if let data = FileManager.default.contents(atPath: "/etc/os-release"),
           let content = String(data: data, encoding: .utf8) {
            for line in content.components(separatedBy: "\n") {
                if line.hasPrefix("PRETTY_NAME=") {
                    let name = line.dropFirst("PRETTY_NAME=".count)
                        .trimmingCharacters(in: CharacterSet(charactersIn: "\""))
                    return "Linux \(name)"
                }
            }
        }
        return "Linux \(versionString)"
        #else
        return "Unknown \(versionString)"
        #endif
    }

    // MARK: - Helpers

    private func resolvePath(_ path: String) -> String {
        if path.hasPrefix("/") {
            return path
        }
        return (workingDir as NSString).appendingPathComponent(path)
    }

    private func filteredEnvironment() -> [String: String] {
        var filtered: [String: String] = [:]
        for (key, value) in ProcessInfo.processInfo.environment {
            let upperKey = key.uppercased()
            let isSensitive = Self.sensitivePatterns.contains { upperKey.contains($0) }

            if Self.safeVarNames.contains(key) || !isSensitive {
                filtered[key] = value
            }
        }
        return filtered
    }

    private func renderImageFileIfSupported(data: Data, path: String) -> String? {
        let pathExtension = URL(fileURLWithPath: path).pathExtension.lowercased()
        let bytes = [UInt8](data)
        let dimensions = imageDimensions(bytes: bytes, pathExtension: pathExtension)

        let knownImageExtensions: Set<String> = [
            "png", "jpg", "jpeg", "gif", "webp", "bmp", "tif", "tiff", "heic", "heif",
        ]
        let isKnownImage = knownImageExtensions.contains(pathExtension) || dimensions != nil
        guard isKnownImage else {
            return nil
        }

        let dimensionString: String
        if let dimensions {
            dimensionString = "\(dimensions.width)x\(dimensions.height)"
        } else {
            dimensionString = "unknown"
        }

        return """
Image file: \(path)
MIME type: \(mimeType(forPath: path))
Dimensions: \(dimensionString)
Base64:
\(data.base64EncodedString())
"""
    }

    private func mimeType(forPath path: String) -> String {
        switch URL(fileURLWithPath: path).pathExtension.lowercased() {
        case "png": return "image/png"
        case "jpg", "jpeg": return "image/jpeg"
        case "gif": return "image/gif"
        case "webp": return "image/webp"
        case "bmp": return "image/bmp"
        case "tif", "tiff": return "image/tiff"
        case "heic": return "image/heic"
        case "heif": return "image/heif"
        default: return "application/octet-stream"
        }
    }

    private func imageDimensions(bytes: [UInt8], pathExtension: String) -> (width: Int, height: Int)? {
        if let png = pngDimensions(bytes) { return png }
        if let gif = gifDimensions(bytes) { return gif }
        if let jpeg = jpegDimensions(bytes) { return jpeg }
        if let webp = webpDimensions(bytes) { return webp }
        if let bmp = bmpDimensions(bytes) { return bmp }

        // Some formats (for example HEIC/HEIF/TIFF) are treated as image payloads
        // but do not have lightweight cross-platform dimension parsing here.
        let knownButUnparsed: Set<String> = ["heic", "heif", "tif", "tiff"]
        if knownButUnparsed.contains(pathExtension) {
            return nil
        }
        return nil
    }

    private func pngDimensions(_ bytes: [UInt8]) -> (width: Int, height: Int)? {
        guard bytes.count >= 24 else { return nil }
        let signature: [UInt8] = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]
        guard Array(bytes[0..<8]) == signature else { return nil }
        let width = beUInt32(bytes, at: 16)
        let height = beUInt32(bytes, at: 20)
        guard width > 0 && height > 0 else { return nil }
        return (width, height)
    }

    private func gifDimensions(_ bytes: [UInt8]) -> (width: Int, height: Int)? {
        guard bytes.count >= 10 else { return nil }
        guard let header = String(bytes: bytes[0..<6], encoding: .ascii), header == "GIF87a" || header == "GIF89a" else {
            return nil
        }
        let width = leUInt16(bytes, at: 6)
        let height = leUInt16(bytes, at: 8)
        guard width > 0 && height > 0 else { return nil }
        return (width, height)
    }

    private func jpegDimensions(_ bytes: [UInt8]) -> (width: Int, height: Int)? {
        guard bytes.count >= 4, bytes[0] == 0xFF, bytes[1] == 0xD8 else {
            return nil
        }

        var index = 2
        while index + 8 < bytes.count {
            while index < bytes.count && bytes[index] == 0xFF {
                index += 1
            }
            guard index < bytes.count else { break }

            let marker = bytes[index]
            index += 1

            if marker == 0xD8 || marker == 0xD9 || marker == 0x01 || (0xD0...0xD7).contains(marker) {
                continue
            }

            guard index + 1 < bytes.count else { break }
            let segmentLength = (Int(bytes[index]) << 8) | Int(bytes[index + 1])
            guard segmentLength >= 2, index + segmentLength <= bytes.count else { break }

            if (0xC0...0xCF).contains(marker), marker != 0xC4, marker != 0xC8, marker != 0xCC {
                guard index + 6 < bytes.count else { break }
                let height = (Int(bytes[index + 3]) << 8) | Int(bytes[index + 4])
                let width = (Int(bytes[index + 5]) << 8) | Int(bytes[index + 6])
                guard width > 0 && height > 0 else { return nil }
                return (width, height)
            }

            index += segmentLength
        }

        return nil
    }

    private func webpDimensions(_ bytes: [UInt8]) -> (width: Int, height: Int)? {
        guard bytes.count >= 30 else { return nil }
        guard String(bytes: bytes[0..<4], encoding: .ascii) == "RIFF",
              String(bytes: bytes[8..<12], encoding: .ascii) == "WEBP",
              let chunk = String(bytes: bytes[12..<16], encoding: .ascii)
        else {
            return nil
        }

        switch chunk {
        case "VP8X":
            let width = 1 + leUInt24(bytes, at: 24)
            let height = 1 + leUInt24(bytes, at: 27)
            return width > 0 && height > 0 ? (width, height) : nil
        case "VP8 ":
            let width = ((Int(bytes[26]) | (Int(bytes[27]) << 8)) & 0x3FFF)
            let height = ((Int(bytes[28]) | (Int(bytes[29]) << 8)) & 0x3FFF)
            return width > 0 && height > 0 ? (width, height) : nil
        case "VP8L":
            guard bytes.count >= 25, bytes[20] == 0x2F else { return nil }
            let b1 = Int(bytes[21])
            let b2 = Int(bytes[22])
            let b3 = Int(bytes[23])
            let b4 = Int(bytes[24])
            let width = 1 + (b1 | ((b2 & 0x3F) << 8))
            let height = 1 + ((b2 >> 6) | (b3 << 2) | ((b4 & 0x0F) << 10))
            return width > 0 && height > 0 ? (width, height) : nil
        default:
            return nil
        }
    }

    private func bmpDimensions(_ bytes: [UInt8]) -> (width: Int, height: Int)? {
        guard bytes.count >= 26, bytes[0] == 0x42, bytes[1] == 0x4D else { return nil }
        let width = leInt32(bytes, at: 18)
        let rawHeight = leInt32(bytes, at: 22)
        let height = abs(rawHeight)
        guard width > 0 && height > 0 else { return nil }
        return (width, height)
    }

    private func beUInt32(_ bytes: [UInt8], at index: Int) -> Int {
        guard index + 3 < bytes.count else { return 0 }
        return (Int(bytes[index]) << 24)
            | (Int(bytes[index + 1]) << 16)
            | (Int(bytes[index + 2]) << 8)
            | Int(bytes[index + 3])
    }

    private func leUInt16(_ bytes: [UInt8], at index: Int) -> Int {
        guard index + 1 < bytes.count else { return 0 }
        return Int(bytes[index]) | (Int(bytes[index + 1]) << 8)
    }

    private func leUInt24(_ bytes: [UInt8], at index: Int) -> Int {
        guard index + 2 < bytes.count else { return 0 }
        return Int(bytes[index]) | (Int(bytes[index + 1]) << 8) | (Int(bytes[index + 2]) << 16)
    }

    private func leInt32(_ bytes: [UInt8], at index: Int) -> Int {
        guard index + 3 < bytes.count else { return 0 }
        let value = Int32(littleEndian: Int32(bitPattern:
            UInt32(bytes[index])
            | (UInt32(bytes[index + 1]) << 8)
            | (UInt32(bytes[index + 2]) << 16)
            | (UInt32(bytes[index + 3]) << 24)
        ))
        return Int(value)
    }

    private func terminateProcess(pid: Int32, signal: Int32) {
        // Prefer terminating the process group first. If the child was not moved
        // into its own group, fall back to the single process.
        if kill(-pid, signal) != 0 {
            _ = kill(pid, signal)
        }
    }
}

private final class _DataBox: @unchecked Sendable {
    private let lock = NSLock()
    private var data = Data()

    func set(_ d: Data) {
        lock.lock()
        data = d
        lock.unlock()
    }

    func get() -> Data {
        lock.lock()
        defer { lock.unlock() }
        return data
    }
}

private final class TimeoutFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var timedOut = false

    func markTimedOut() {
        lock.lock()
        timedOut = true
        lock.unlock()
    }

    func isTimedOut() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return timedOut
    }
}

// MARK: - Tool Errors

public enum ToolError: Error, CustomStringConvertible {
    case fileNotFound(String)
    case permissionDenied(String)
    case binaryFile(String)
    case writeError(String)
    case editConflict(String)
    case patchError(String)
    case timeout(Int)
    case validationError(String)

    public var description: String {
        switch self {
        case .fileNotFound(let path): return "File not found: \(path)"
        case .permissionDenied(let path): return "Permission denied: \(path)"
        case .binaryFile(let path): return "Cannot read binary file: \(path)"
        case .writeError(let msg): return "Write error: \(msg)"
        case .editConflict(let msg): return "Edit conflict: \(msg)"
        case .patchError(let msg): return "Patch error: \(msg)"
        case .timeout(let ms): return "Command timed out after \(ms)ms"
        case .validationError(let msg): return "Validation error: \(msg)"
        }
    }
}
