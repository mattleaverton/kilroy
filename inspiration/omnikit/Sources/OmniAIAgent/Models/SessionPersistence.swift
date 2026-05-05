import Foundation
import OmniAICore

public protocol SessionStorageBackend: Sendable {
    func load(sessionID: String) async throws -> SessionSnapshot?
    func save(_ snapshot: SessionSnapshot) async throws
    func delete(sessionID: String) async throws
}

public struct SessionSnapshot: Codable, Sendable {
    public var sessionID: String
    public var providerID: String
    public var model: String
    public var workingDirectory: String
    public var state: SessionState
    public var history: [PersistedTurn]
    public var responseTimeline: [ResponseTimelineEntry]
    public var pendingTimelineTurns: [PersistedTurn]
    public var steeringQueue: [String]
    public var followupQueue: [String]
    public var config: SessionConfig
    public var abortSignaled: Bool
    public var updatedAt: Date

    public init(
        sessionID: String,
        providerID: String,
        model: String,
        workingDirectory: String,
        state: SessionState,
        history: [PersistedTurn],
        responseTimeline: [ResponseTimelineEntry] = [],
        pendingTimelineTurns: [PersistedTurn] = [],
        steeringQueue: [String],
        followupQueue: [String],
        config: SessionConfig,
        abortSignaled: Bool,
        updatedAt: Date = Date()
    ) {
        self.sessionID = sessionID
        self.providerID = providerID
        self.model = model
        self.workingDirectory = workingDirectory
        self.state = state
        self.history = history
        self.responseTimeline = responseTimeline
        self.pendingTimelineTurns = pendingTimelineTurns
        self.steeringQueue = steeringQueue
        self.followupQueue = followupQueue
        self.config = config
        self.abortSignaled = abortSignaled
        self.updatedAt = updatedAt
    }

    private enum CodingKeys: String, CodingKey {
        case sessionID
        case providerID
        case model
        case workingDirectory
        case state
        case history
        case responseTimeline
        case pendingTimelineTurns
        case steeringQueue
        case followupQueue
        case config
        case abortSignaled
        case updatedAt
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        sessionID = try container.decode(String.self, forKey: .sessionID)
        providerID = try container.decode(String.self, forKey: .providerID)
        model = try container.decode(String.self, forKey: .model)
        workingDirectory = try container.decode(String.self, forKey: .workingDirectory)
        state = try container.decode(SessionState.self, forKey: .state)
        history = try container.decode([PersistedTurn].self, forKey: .history)
        responseTimeline = try container.decodeIfPresent([ResponseTimelineEntry].self, forKey: .responseTimeline) ?? []
        pendingTimelineTurns = try container.decodeIfPresent([PersistedTurn].self, forKey: .pendingTimelineTurns) ?? []
        steeringQueue = try container.decode([String].self, forKey: .steeringQueue)
        followupQueue = try container.decode([String].self, forKey: .followupQueue)
        config = try container.decode(SessionConfig.self, forKey: .config)
        abortSignaled = try container.decode(Bool.self, forKey: .abortSignaled)
        updatedAt = try container.decode(Date.self, forKey: .updatedAt)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(sessionID, forKey: .sessionID)
        try container.encode(providerID, forKey: .providerID)
        try container.encode(model, forKey: .model)
        try container.encode(workingDirectory, forKey: .workingDirectory)
        try container.encode(state, forKey: .state)
        try container.encode(history, forKey: .history)
        try container.encode(responseTimeline, forKey: .responseTimeline)
        try container.encode(pendingTimelineTurns, forKey: .pendingTimelineTurns)
        try container.encode(steeringQueue, forKey: .steeringQueue)
        try container.encode(followupQueue, forKey: .followupQueue)
        try container.encode(config, forKey: .config)
        try container.encode(abortSignaled, forKey: .abortSignaled)
        try container.encode(updatedAt, forKey: .updatedAt)
    }
}

public actor FileSessionStorageBackend: SessionStorageBackend {
    private let rootDirectory: URL
    private let fm = FileManager.default
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    public init(rootDirectory: URL) {
        self.rootDirectory = rootDirectory
        self.encoder = JSONEncoder()
        self.decoder = JSONDecoder()
        self.encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
    }

    public func load(sessionID: String) async throws -> SessionSnapshot? {
        let file = snapshotPath(sessionID: sessionID)
        guard fm.fileExists(atPath: file.path) else { return nil }
        let data = try Data(contentsOf: file)
        return try decoder.decode(SessionSnapshot.self, from: data)
    }

    public func save(_ snapshot: SessionSnapshot) async throws {
        try fm.createDirectory(at: rootDirectory, withIntermediateDirectories: true)
        let file = snapshotPath(sessionID: snapshot.sessionID)
        let data = try encoder.encode(snapshot)
        try data.write(to: file, options: .atomic)
    }

    public func delete(sessionID: String) async throws {
        let file = snapshotPath(sessionID: sessionID)
        guard fm.fileExists(atPath: file.path) else { return }
        try fm.removeItem(at: file)
    }

    private func snapshotPath(sessionID: String) -> URL {
        rootDirectory.appendingPathComponent("\(sessionID).json")
    }
}

public enum PersistedTurn: Codable, Sendable {
    case user(UserTurn)
    case assistant(PersistedAssistantTurn)
    case toolResults(PersistedToolResultsTurn)
    case system(SystemTurn)
    case steering(SteeringTurn)

    private enum CodingKeys: String, CodingKey {
        case kind
        case payload
    }

    private enum Kind: String, Codable {
        case user
        case assistant
        case toolResults
        case system
        case steering
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let kind = try container.decode(Kind.self, forKey: .kind)
        switch kind {
        case .user:
            self = .user(try container.decode(UserTurn.self, forKey: .payload))
        case .assistant:
            self = .assistant(try container.decode(PersistedAssistantTurn.self, forKey: .payload))
        case .toolResults:
            self = .toolResults(try container.decode(PersistedToolResultsTurn.self, forKey: .payload))
        case .system:
            self = .system(try container.decode(SystemTurn.self, forKey: .payload))
        case .steering:
            self = .steering(try container.decode(SteeringTurn.self, forKey: .payload))
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .user(let payload):
            try container.encode(Kind.user, forKey: .kind)
            try container.encode(payload, forKey: .payload)
        case .assistant(let payload):
            try container.encode(Kind.assistant, forKey: .kind)
            try container.encode(payload, forKey: .payload)
        case .toolResults(let payload):
            try container.encode(Kind.toolResults, forKey: .kind)
            try container.encode(payload, forKey: .payload)
        case .system(let payload):
            try container.encode(Kind.system, forKey: .kind)
            try container.encode(payload, forKey: .payload)
        case .steering(let payload):
            try container.encode(Kind.steering, forKey: .kind)
            try container.encode(payload, forKey: .payload)
        }
    }
}

public struct PersistedAssistantTurn: Codable, Sendable {
    public var content: String
    public var toolCalls: [PersistedToolCall]
    public var reasoning: String?
    public var usage: PersistedUsage
    public var responseId: String?
    public var timestamp: Date

    public init(
        content: String,
        toolCalls: [PersistedToolCall],
        reasoning: String?,
        usage: PersistedUsage,
        responseId: String?,
        timestamp: Date
    ) {
        self.content = content
        self.toolCalls = toolCalls
        self.reasoning = reasoning
        self.usage = usage
        self.responseId = responseId
        self.timestamp = timestamp
    }
}

public struct PersistedToolResultsTurn: Codable, Sendable {
    public var results: [PersistedToolResult]
    public var timestamp: Date

    public init(results: [PersistedToolResult], timestamp: Date) {
        self.results = results
        self.timestamp = timestamp
    }
}

public struct PersistedToolResult: Codable, Sendable {
    public var toolCallId: String
    public var content: JSONValue
    public var isError: Bool
    public var imageData: [UInt8]?
    public var imageMediaType: String?

    public init(
        toolCallId: String,
        content: JSONValue,
        isError: Bool,
        imageData: [UInt8]?,
        imageMediaType: String?
    ) {
        self.toolCallId = toolCallId
        self.content = content
        self.isError = isError
        self.imageData = imageData
        self.imageMediaType = imageMediaType
    }
}

public struct PersistedToolCall: Codable, Sendable {
    public var id: String
    public var name: String
    public var arguments: [String: JSONValue]
    public var rawArguments: String?
    public var thoughtSignature: String?
    public var providerItemId: String?

    public init(
        id: String,
        name: String,
        arguments: [String: JSONValue],
        rawArguments: String?,
        thoughtSignature: String?,
        providerItemId: String?
    ) {
        self.id = id
        self.name = name
        self.arguments = arguments
        self.rawArguments = rawArguments
        self.thoughtSignature = thoughtSignature
        self.providerItemId = providerItemId
    }
}

public struct PersistedUsage: Codable, Sendable {
    public var inputTokens: Int
    public var outputTokens: Int
    public var reasoningTokens: Int?
    public var cacheReadTokens: Int?
    public var cacheWriteTokens: Int?
    public var raw: JSONValue?

    public init(
        inputTokens: Int,
        outputTokens: Int,
        reasoningTokens: Int?,
        cacheReadTokens: Int?,
        cacheWriteTokens: Int?,
        raw: JSONValue?
    ) {
        self.inputTokens = inputTokens
        self.outputTokens = outputTokens
        self.reasoningTokens = reasoningTokens
        self.cacheReadTokens = cacheReadTokens
        self.cacheWriteTokens = cacheWriteTokens
        self.raw = raw
    }
}

extension PersistedToolCall {
    init(_ toolCall: ToolCall) {
        self.init(
            id: toolCall.id,
            name: toolCall.name,
            arguments: toolCall.arguments,
            rawArguments: toolCall.rawArguments,
            thoughtSignature: toolCall.thoughtSignature,
            providerItemId: toolCall.providerItemId
        )
    }

    func toToolCall() -> ToolCall {
        ToolCall(
            id: id,
            name: name,
            arguments: arguments,
            rawArguments: rawArguments,
            thoughtSignature: thoughtSignature,
            providerItemId: providerItemId
        )
    }
}

extension PersistedToolResult {
    init(_ result: ToolResult) {
        self.init(
            toolCallId: result.toolCallId,
            content: result.content,
            isError: result.isError,
            imageData: result.imageData,
            imageMediaType: result.imageMediaType
        )
    }

    func toToolResult() -> ToolResult {
        ToolResult(
            toolCallId: toolCallId,
            content: content,
            isError: isError,
            imageData: imageData,
            imageMediaType: imageMediaType
        )
    }
}

extension PersistedToolResultsTurn {
    init(_ turn: ToolResultsTurn) {
        self.init(results: turn.results.map(PersistedToolResult.init), timestamp: turn.timestamp)
    }

    func toToolResultsTurn() -> ToolResultsTurn {
        ToolResultsTurn(results: results.map { $0.toToolResult() }, timestamp: timestamp)
    }
}

extension PersistedUsage {
    init(_ usage: Usage) {
        self.init(
            inputTokens: usage.inputTokens,
            outputTokens: usage.outputTokens,
            reasoningTokens: usage.reasoningTokens,
            cacheReadTokens: usage.cacheReadTokens,
            cacheWriteTokens: usage.cacheWriteTokens,
            raw: usage.raw
        )
    }

    func toUsage() -> Usage {
        Usage(
            inputTokens: inputTokens,
            outputTokens: outputTokens,
            reasoningTokens: reasoningTokens,
            cacheReadTokens: cacheReadTokens,
            cacheWriteTokens: cacheWriteTokens,
            raw: raw
        )
    }
}

extension PersistedAssistantTurn {
    init(_ turn: AssistantTurn) {
        self.init(
            content: turn.content,
            toolCalls: turn.toolCalls.map(PersistedToolCall.init),
            reasoning: turn.reasoning,
            usage: PersistedUsage(turn.usage),
            responseId: turn.responseId,
            timestamp: turn.timestamp
        )
    }

    func toAssistantTurn() -> AssistantTurn {
        AssistantTurn(
            content: content,
            toolCalls: toolCalls.map { $0.toToolCall() },
            reasoning: reasoning,
            rawContentParts: nil,
            usage: usage.toUsage(),
            responseId: responseId,
            timestamp: timestamp
        )
    }
}

extension PersistedTurn {
    init(_ turn: Turn) {
        switch turn {
        case .user(let t):
            self = .user(t)
        case .assistant(let t):
            self = .assistant(PersistedAssistantTurn(t))
        case .toolResults(let t):
            self = .toolResults(PersistedToolResultsTurn(t))
        case .system(let t):
            self = .system(t)
        case .steering(let t):
            self = .steering(t)
        }
    }

    func toTurn() -> Turn {
        switch self {
        case .user(let t):
            return .user(t)
        case .assistant(let t):
            return .assistant(t.toAssistantTurn())
        case .toolResults(let t):
            return .toolResults(t.toToolResultsTurn())
        case .system(let t):
            return .system(t)
        case .steering(let t):
            return .steering(t)
        }
    }
}
