import Foundation
import OmniAICore

public enum Turn: Sendable {
    case user(UserTurn)
    case assistant(AssistantTurn)
    case toolResults(ToolResultsTurn)
    case system(SystemTurn)
    case steering(SteeringTurn)
}

public struct UserTurn: Sendable, Codable {
    public var content: String
    public var timestamp: Date

    public init(content: String, timestamp: Date = Date()) {
        self.content = content
        self.timestamp = timestamp
    }
}

public struct AssistantTurn: Sendable {
    public var content: String
    public var toolCalls: [ToolCall]
    public var reasoning: String?
    /// Raw content parts from the provider response, used for faithful round-tripping
    /// of thinking blocks with signatures, redacted thinking, etc.
    public var rawContentParts: [ContentPart]?
    public var usage: Usage
    public var responseId: String?
    public var timestamp: Date

    public init(
        content: String,
        toolCalls: [ToolCall] = [],
        reasoning: String? = nil,
        rawContentParts: [ContentPart]? = nil,
        usage: Usage = .zero,
        responseId: String? = nil,
        timestamp: Date = Date()
    ) {
        self.content = content
        self.toolCalls = toolCalls
        self.reasoning = reasoning
        self.rawContentParts = rawContentParts
        self.usage = usage
        self.responseId = responseId
        self.timestamp = timestamp
    }
}

public struct ToolResultsTurn: Sendable {
    public var results: [ToolResult]
    public var timestamp: Date

    public init(results: [ToolResult], timestamp: Date = Date()) {
        self.results = results
        self.timestamp = timestamp
    }
}

public struct SystemTurn: Sendable, Codable {
    public var content: String
    public var timestamp: Date

    public init(content: String, timestamp: Date = Date()) {
        self.content = content
        self.timestamp = timestamp
    }
}

public struct SteeringTurn: Sendable, Codable {
    public var content: String
    public var timestamp: Date

    public init(content: String, timestamp: Date = Date()) {
        self.content = content
        self.timestamp = timestamp
    }
}
