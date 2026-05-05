import Foundation

public struct ExecResult: Sendable {
    public var stdout: String
    public var stderr: String
    public var exitCode: Int32
    public var timedOut: Bool
    public var durationMs: Int

    public init(stdout: String, stderr: String, exitCode: Int32, timedOut: Bool, durationMs: Int) {
        self.stdout = stdout
        self.stderr = stderr
        self.exitCode = exitCode
        self.timedOut = timedOut
        self.durationMs = durationMs
    }

    public var combinedOutput: String {
        var parts: [String] = []
        if !stdout.isEmpty { parts.append(stdout) }
        if !stderr.isEmpty { parts.append(stderr) }
        return parts.joined(separator: "\n")
    }
}

public struct DirEntry: Sendable {
    public var name: String
    public var isDir: Bool
    public var size: Int?

    public init(name: String, isDir: Bool, size: Int? = nil) {
        self.name = name
        self.isDir = isDir
        self.size = size
    }
}

public struct GrepOptions: Sendable {
    public var globFilter: String?
    public var caseInsensitive: Bool
    public var maxResults: Int

    public init(globFilter: String? = nil, caseInsensitive: Bool = false, maxResults: Int = 100) {
        self.globFilter = globFilter
        self.caseInsensitive = caseInsensitive
        self.maxResults = maxResults
    }
}

public protocol ExecutionEnvironment: Sendable {
    func readFile(path: String, offset: Int?, limit: Int?) async throws -> String
    func writeFile(path: String, content: String) async throws
    func fileExists(path: String) async -> Bool
    func listDirectory(path: String, depth: Int) async throws -> [DirEntry]

    func execCommand(
        command: String,
        timeoutMs: Int,
        workingDir: String?,
        envVars: [String: String]?
    ) async throws -> ExecResult

    func grep(pattern: String, path: String, options: GrepOptions) async throws -> String
    func glob(pattern: String, path: String) async throws -> [String]

    func initialize() async throws
    func cleanup() async throws

    func workingDirectory() -> String
    func platform() -> String
    func osVersion() -> String
}
