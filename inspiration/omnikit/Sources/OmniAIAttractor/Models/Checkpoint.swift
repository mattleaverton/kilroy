import Foundation

// MARK: - Checkpoint

public struct Checkpoint: Codable, Sendable {
    public var timestamp: Date
    public var currentNode: String
    public var completedNodes: [String]
    public var nodeRetries: [String: Int]
    public var nodeOutcomes: [String: String]
    public var contextValues: [String: String]
    public var logs: [String]

    public init(
        timestamp: Date = Date(),
        currentNode: String = "",
        completedNodes: [String] = [],
        nodeRetries: [String: Int] = [:],
        nodeOutcomes: [String: String] = [:],
        contextValues: [String: String] = [:],
        logs: [String] = []
    ) {
        self.timestamp = timestamp
        self.currentNode = currentNode
        self.completedNodes = completedNodes
        self.nodeRetries = nodeRetries
        self.nodeOutcomes = nodeOutcomes
        self.contextValues = contextValues
        self.logs = logs
    }

    public func save(to url: URL) throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        encoder.dateEncodingStrategy = .iso8601
        let data = try encoder.encode(self)
        try data.write(to: url)
    }

    public static func load(from url: URL) throws -> Checkpoint {
        let data = try Data(contentsOf: url)
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(Checkpoint.self, from: data)
    }
}

