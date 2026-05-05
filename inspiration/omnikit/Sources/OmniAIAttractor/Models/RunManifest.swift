import Foundation

public enum RunCompletionState: String, Codable, Sendable {
    case running
    case completed
    case failed
}

public struct RunManifest: Codable, Sendable {
    public var dotPath: String
    public var backend: String
    public var workingDirectory: String
    public var logsRoot: String
    public var currentNode: String?
    public var createdAt: Date
    public var updatedAt: Date
    public var completionState: RunCompletionState

    public init(
        dotPath: String,
        backend: String,
        workingDirectory: String,
        logsRoot: String,
        currentNode: String? = nil,
        createdAt: Date = Date(),
        updatedAt: Date = Date(),
        completionState: RunCompletionState = .running
    ) {
        self.dotPath = dotPath
        self.backend = backend
        self.workingDirectory = workingDirectory
        self.logsRoot = logsRoot
        self.currentNode = currentNode
        self.createdAt = createdAt
        self.updatedAt = updatedAt
        self.completionState = completionState
    }

    public func save(to url: URL) throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        let data = try encoder.encode(self)
        try data.write(to: url, options: .atomic)
    }

    public static func load(from url: URL) throws -> RunManifest {
        let data = try Data(contentsOf: url)
        return try JSONDecoder().decode(RunManifest.self, from: data)
    }
}
