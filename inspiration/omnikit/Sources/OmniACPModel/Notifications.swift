import Foundation

public struct SessionUpdateNotification: Notification {
    public static let name = "session/update"

    public struct Parameters: Codable, Sendable {
        public var sessionID: String
        public var update: SessionUpdate

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
            case update
        }

        public init(sessionID: String, update: SessionUpdate) {
            self.sessionID = sessionID
            self.update = update
        }
    }
}

public enum SessionUpdate: Codable, Hashable, Sendable {
    case agentMessageChunk(AgentMessageChunk)
    case userMessageChunk(UserMessageChunk)
    case agentThoughtChunk(AgentThoughtChunk)
    case toolCall(ToolCall)
    case toolCallUpdate(ToolCallUpdate)
    case plan(PlanUpdate)
    case availableCommandsUpdate(AvailableCommandsUpdate)
    case currentModeUpdate(CurrentModeUpdate)
    case configOptionUpdate(ConfigOptionUpdate)
    case sessionInfoUpdate(SessionInfoUpdate)
    case unknown(String, Value)

    private enum CodingKeys: String, CodingKey {
        case sessionUpdate
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let updateType = try container.decode(String.self, forKey: .sessionUpdate)
        switch updateType {
        case "agent_message_chunk":
            self = .agentMessageChunk(try AgentMessageChunk(from: decoder))
        case "user_message_chunk":
            self = .userMessageChunk(try UserMessageChunk(from: decoder))
        case "agent_thought_chunk":
            self = .agentThoughtChunk(try AgentThoughtChunk(from: decoder))
        case "tool_call":
            self = .toolCall(try ToolCall(from: decoder))
        case "tool_call_update":
            self = .toolCallUpdate(try ToolCallUpdate(from: decoder))
        case "plan":
            self = .plan(try PlanUpdate(from: decoder))
        case "available_commands_update":
            self = .availableCommandsUpdate(try AvailableCommandsUpdate(from: decoder))
        case "current_mode_update":
            self = .currentModeUpdate(try CurrentModeUpdate(from: decoder))
        case "config_option_update":
            self = .configOptionUpdate(try ConfigOptionUpdate(from: decoder))
        case "session_info_update":
            self = .sessionInfoUpdate(try SessionInfoUpdate(from: decoder))
        default:
            self = .unknown(updateType, try Value(from: decoder))
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case .agentMessageChunk(let value):
            try value.encode(to: encoder)
        case .userMessageChunk(let value):
            try value.encode(to: encoder)
        case .agentThoughtChunk(let value):
            try value.encode(to: encoder)
        case .toolCall(let value):
            try value.encode(to: encoder)
        case .toolCallUpdate(let value):
            try value.encode(to: encoder)
        case .plan(let value):
            try value.encode(to: encoder)
        case .availableCommandsUpdate(let value):
            try value.encode(to: encoder)
        case .currentModeUpdate(let value):
            try value.encode(to: encoder)
        case .configOptionUpdate(let value):
            try value.encode(to: encoder)
        case .sessionInfoUpdate(let value):
            try value.encode(to: encoder)
        case .unknown(_, let value):
            try value.encode(to: encoder)
        }
    }
}

public struct AgentMessageChunk: Codable, Hashable, Sendable {
    public var sessionUpdate: String = "agent_message_chunk"
    public var content: TextContentBlock

    public init(content: TextContentBlock) {
        self.content = content
    }
}

public struct UserMessageChunk: Codable, Hashable, Sendable {
    public var sessionUpdate: String = "user_message_chunk"
    public var content: TextContentBlock

    public init(content: TextContentBlock) {
        self.content = content
    }
}

public struct AgentThoughtChunk: Codable, Hashable, Sendable {
    public var sessionUpdate: String = "agent_thought_chunk"
    public var content: TextContentBlock

    public init(content: TextContentBlock) {
        self.content = content
    }
}

public struct ToolCall: Codable, Hashable, Sendable {
    public var sessionUpdate: String = "tool_call"
    public var toolCallID: String
    public var title: String?
    public var kind: String?
    public var status: String?
    public var locations: [ToolCallLocation]?
    public var rawInput: Value?

    private enum CodingKeys: String, CodingKey {
        case sessionUpdate
        case toolCallID = "toolCallId"
        case title
        case kind
        case status
        case locations
        case rawInput
    }

    public init(
        toolCallID: String,
        title: String? = nil,
        kind: String? = nil,
        status: String? = nil,
        locations: [ToolCallLocation]? = nil,
        rawInput: Value? = nil
    ) {
        self.toolCallID = toolCallID
        self.title = title
        self.kind = kind
        self.status = status
        self.locations = locations
        self.rawInput = rawInput
    }
}

public struct ToolCallUpdate: Codable, Hashable, Sendable {
    public var sessionUpdate: String = "tool_call_update"
    public var toolCallID: String
    public var title: String?
    public var kind: String?
    public var status: String?
    public var content: [ToolCallContent]?
    public var locations: [ToolCallLocation]?
    public var rawInput: Value?
    public var rawOutput: Value?

    private enum CodingKeys: String, CodingKey {
        case sessionUpdate
        case toolCallID = "toolCallId"
        case title
        case kind
        case status
        case content
        case locations
        case rawInput
        case rawOutput
    }

    public init(
        toolCallID: String,
        title: String? = nil,
        kind: String? = nil,
        status: String? = nil,
        content: [ToolCallContent]? = nil,
        locations: [ToolCallLocation]? = nil,
        rawInput: Value? = nil,
        rawOutput: Value? = nil
    ) {
        self.toolCallID = toolCallID
        self.title = title
        self.kind = kind
        self.status = status
        self.content = content
        self.locations = locations
        self.rawInput = rawInput
        self.rawOutput = rawOutput
    }
}

public struct ToolCallLocation: Codable, Hashable, Sendable {
    public var path: String
    public var line: Int?
    public var column: Int?

    public init(path: String, line: Int? = nil, column: Int? = nil) {
        self.path = path
        self.line = line
        self.column = column
    }
}

public enum ToolCallContent: Codable, Hashable, Sendable {
    case text(TextToolContent)
    case diff(DiffToolContent)
    case terminal(TerminalToolContent)

    private enum CodingKeys: String, CodingKey {
        case type
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch try container.decode(String.self, forKey: .type) {
        case "content":
            self = .text(try TextToolContent(from: decoder))
        case "diff":
            self = .diff(try DiffToolContent(from: decoder))
        case "terminal":
            self = .terminal(try TerminalToolContent(from: decoder))
        default:
            throw DecodingError.dataCorruptedError(forKey: .type, in: container, debugDescription: "Unknown tool content type")
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case .text(let value):
            try value.encode(to: encoder)
        case .diff(let value):
            try value.encode(to: encoder)
        case .terminal(let value):
            try value.encode(to: encoder)
        }
    }
}

public struct TextToolContent: Codable, Hashable, Sendable {
    public var type: String = "content"
    public var content: TextContentBlock

    public init(content: TextContentBlock) {
        self.content = content
    }
}

public struct DiffToolContent: Codable, Hashable, Sendable {
    public var type: String = "diff"
    public var path: String
    public var oldText: String?
    public var newText: String

    public init(path: String, oldText: String? = nil, newText: String) {
        self.path = path
        self.oldText = oldText
        self.newText = newText
    }
}

public struct TerminalToolContent: Codable, Hashable, Sendable {
    public var type: String = "terminal"
    public var terminalID: String

    private enum CodingKeys: String, CodingKey {
        case type
        case terminalID = "terminalId"
    }

    public init(terminalID: String) {
        self.terminalID = terminalID
    }
}

public struct PlanUpdate: Codable, Hashable, Sendable {
    public var sessionUpdate: String = "plan"
    public var entries: [PlanEntry]

    public init(entries: [PlanEntry]) {
        self.entries = entries
    }
}

public struct PlanEntry: Codable, Hashable, Sendable {
    public var content: String
    public var priority: String?
    public var status: String?

    public init(content: String, priority: String? = nil, status: String? = nil) {
        self.content = content
        self.priority = priority
        self.status = status
    }
}

public struct AvailableCommandsUpdate: Codable, Hashable, Sendable {
    public var sessionUpdate: String = "available_commands_update"
    public var availableCommands: [AvailableCommandUpdate]

    public init(availableCommands: [AvailableCommandUpdate]) {
        self.availableCommands = availableCommands
    }
}

public struct AvailableCommandUpdate: Codable, Hashable, Sendable {
    public var name: String
    public var description: String
    public var input: AvailableCommandInput?

    public init(name: String, description: String, input: AvailableCommandInput? = nil) {
        self.name = name
        self.description = description
        self.input = input
    }
}

public enum AvailableCommandInput: Codable, Hashable, Sendable {
    case unstructured(UnstructuredCommandInput)

    public init(from decoder: Decoder) throws {
        self = .unstructured(try UnstructuredCommandInput(from: decoder))
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case .unstructured(let value):
            try value.encode(to: encoder)
        }
    }
}

public struct UnstructuredCommandInput: Codable, Hashable, Sendable {
    public var hint: String

    public init(hint: String) {
        self.hint = hint
    }
}

public struct CurrentModeUpdate: Codable, Hashable, Sendable {
    public var sessionUpdate: String = "current_mode_update"
    public var currentModeID: String

    private enum CodingKeys: String, CodingKey {
        case sessionUpdate
        case currentModeID = "currentModeId"
    }

    public init(currentModeID: String) {
        self.currentModeID = currentModeID
    }
}

public struct ConfigOptionUpdate: Codable, Hashable, Sendable {
    public var sessionUpdate: String = "config_option_update"
    public var configOptions: [ConfigOptionDefinition]

    public init(configOptions: [ConfigOptionDefinition]) {
        self.configOptions = configOptions
    }
}

public struct SessionInfoUpdate: Codable, Hashable, Sendable {
    public var sessionUpdate: String = "session_info_update"
    public var title: String?
    public var updatedAt: String?
    public var meta: [String: Value]?

    private enum CodingKeys: String, CodingKey {
        case sessionUpdate
        case title
        case updatedAt
        case meta = "_meta"
    }

    public init(title: String? = nil, updatedAt: String? = nil, meta: [String: Value]? = nil) {
        self.title = title
        self.updatedAt = updatedAt
        self.meta = meta
    }
}
