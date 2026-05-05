import Foundation

public struct ClientInfo: Codable, Hashable, Sendable {
    public var name: String
    public var version: String
    public var title: String?
    public var description: String?

    public init(name: String, version: String, title: String? = nil, description: String? = nil) {
        self.name = name
        self.version = version
        self.title = title
        self.description = description
    }
}

public struct FileSystemCapabilities: Codable, Hashable, Sendable {
    public var readTextFile: Bool?
    public var writeTextFile: Bool?

    public init(readTextFile: Bool? = nil, writeTextFile: Bool? = nil) {
        self.readTextFile = readTextFile
        self.writeTextFile = writeTextFile
    }
}

public struct ClientCapabilities: Codable, Hashable, Sendable {
    public var fs: FileSystemCapabilities?

    public init(fs: FileSystemCapabilities? = nil) {
        self.fs = fs
    }
}

public struct AgentInfo: Codable, Hashable, Sendable {
    public var name: String
    public var version: String
    public var title: String?
    public var description: String?
    public var homepage: String?
    public var icon: String?

    public init(
        name: String,
        version: String,
        title: String? = nil,
        description: String? = nil,
        homepage: String? = nil,
        icon: String? = nil
    ) {
        self.name = name
        self.version = version
        self.title = title
        self.description = description
        self.homepage = homepage
        self.icon = icon
    }
}

public struct PromptCapabilities: Codable, Hashable, Sendable {
    public var image: Bool?
    public var audio: Bool?
    public var embeddedContext: Bool?

    public init(image: Bool? = nil, audio: Bool? = nil, embeddedContext: Bool? = nil) {
        self.image = image
        self.audio = audio
        self.embeddedContext = embeddedContext
    }
}

public struct MCPCapabilities: Codable, Hashable, Sendable {
    public var http: Bool?
    public var sse: Bool?

    public init(http: Bool? = nil, sse: Bool? = nil) {
        self.http = http
        self.sse = sse
    }
}

public struct AgentCapabilities: Codable, Hashable, Sendable {
    public var loadSession: Bool?
    public var listSessions: Bool?
    public var resumeSession: Bool?
    public var mcpCapabilities: MCPCapabilities?
    public var promptCapabilities: PromptCapabilities?
    public var sessionCapabilities: Value?

    public init(
        loadSession: Bool? = nil,
        listSessions: Bool? = nil,
        resumeSession: Bool? = nil,
        mcpCapabilities: MCPCapabilities? = nil,
        promptCapabilities: PromptCapabilities? = nil,
        sessionCapabilities: Value? = nil
    ) {
        self.loadSession = loadSession
        self.listSessions = listSessions
        self.resumeSession = resumeSession
        self.mcpCapabilities = mcpCapabilities
        self.promptCapabilities = promptCapabilities
        self.sessionCapabilities = sessionCapabilities
    }
}

public struct AgentMode: Codable, Hashable, Sendable {
    public var id: String
    public var name: String
    public var description: String?

    public init(id: String, name: String, description: String? = nil) {
        self.id = id
        self.name = name
        self.description = description
    }
}

public struct Modes: Codable, Hashable, Sendable {
    public var selected: String?
    public var available: [AgentMode]?

    public init(selected: String? = nil, available: [AgentMode]? = nil) {
        self.selected = selected
        self.available = available
    }
}

public struct AvailableCommand: Codable, Hashable, Sendable {
    public var id: String?
    public var name: String
    public var description: String?

    public init(id: String? = nil, name: String, description: String? = nil) {
        self.id = id
        self.name = name
        self.description = description
    }
}

public struct SessionSummary: Codable, Hashable, Sendable {
    public var sessionID: String
    public var title: String?
    public var createdAt: String?
    public var updatedAt: String?

    private enum CodingKeys: String, CodingKey {
        case sessionID = "sessionId"
        case title
        case createdAt
        case updatedAt
    }

    public init(sessionID: String, title: String? = nil, createdAt: String? = nil, updatedAt: String? = nil) {
        self.sessionID = sessionID
        self.title = title
        self.createdAt = createdAt
        self.updatedAt = updatedAt
    }
}

public enum StopReason: String, Codable, Hashable, Sendable {
    case endTurn = "end_turn"
    case cancelled
    case paused
    case error
    case maxTokens = "max_tokens"
}

public enum ContentBlock: Codable, Hashable, Sendable {
    case text(TextContentBlock)
    case image(ImageContentBlock)
    case audio(AudioContentBlock)
    case resource(ResourceContentBlock)
    case resourceLink(ResourceLinkContentBlock)

    private enum CodingKeys: String, CodingKey {
        case type
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let type = try container.decode(String.self, forKey: .type)
        switch type {
        case "text":
            self = .text(try TextContentBlock(from: decoder))
        case "image":
            self = .image(try ImageContentBlock(from: decoder))
        case "audio":
            self = .audio(try AudioContentBlock(from: decoder))
        case "resource":
            self = .resource(try ResourceContentBlock(from: decoder))
        case "resource_link":
            self = .resourceLink(try ResourceLinkContentBlock(from: decoder))
        default:
            throw DecodingError.dataCorruptedError(forKey: .type, in: container, debugDescription: "Unknown content type \(type)")
        }
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case .text(let block):
            try block.encode(to: encoder)
        case .image(let block):
            try block.encode(to: encoder)
        case .audio(let block):
            try block.encode(to: encoder)
        case .resource(let block):
            try block.encode(to: encoder)
        case .resourceLink(let block):
            try block.encode(to: encoder)
        }
    }

    public static func text(_ value: String) -> ContentBlock {
        .text(TextContentBlock(text: value))
    }
}

public struct TextContentBlock: Codable, Hashable, Sendable {
    public var type: String = "text"
    public var text: String

    public init(text: String) {
        self.text = text
    }
}

public struct ImageContentBlock: Codable, Hashable, Sendable {
    public var type: String = "image"
    public var mimeType: String
    public var data: String

    public init(data: String, mimeType: String) {
        self.mimeType = mimeType
        self.data = data
    }
}

public struct AudioContentBlock: Codable, Hashable, Sendable {
    public var type: String = "audio"
    public var mimeType: String
    public var data: String

    public init(data: String, mimeType: String) {
        self.mimeType = mimeType
        self.data = data
    }
}

public struct EmbeddedResource: Codable, Hashable, Sendable {
    public var uri: String
    public var mimeType: String?
    public var text: String?
    public var blob: String?

    public init(uri: String, mimeType: String? = nil, text: String? = nil, blob: String? = nil) {
        self.uri = uri
        self.mimeType = mimeType
        self.text = text
        self.blob = blob
    }
}

public struct ResourceContentBlock: Codable, Hashable, Sendable {
    public var type: String = "resource"
    public var resource: EmbeddedResource

    public init(resource: EmbeddedResource) {
        self.resource = resource
    }
}

public struct ResourceLinkContentBlock: Codable, Hashable, Sendable {
    public var type: String = "resource_link"
    public var uri: String
    public var name: String?
    public var mimeType: String?
    public var size: Int?

    public init(uri: String, name: String? = nil, mimeType: String? = nil, size: Int? = nil) {
        self.uri = uri
        self.name = name
        self.mimeType = mimeType
        self.size = size
    }
}

public struct MCPServer: Codable, Hashable, Sendable {
    public var name: String
    public var command: String
    public var args: [String]
    public var env: [String]

    public init(name: String, command: String, args: [String] = [], env: [String] = []) {
        self.name = name
        self.command = command
        self.args = args
        self.env = env
    }
}

public struct PermissionToolCall: Codable, Hashable, Sendable {
    public var toolCallID: String
    public var rawInput: Value?

    private enum CodingKeys: String, CodingKey {
        case toolCallID = "toolCallId"
        case rawInput
    }

    public init(toolCallID: String, rawInput: Value? = nil) {
        self.toolCallID = toolCallID
        self.rawInput = rawInput
    }
}

public struct PermissionOption: Codable, Hashable, Sendable {
    public var optionID: String
    public var name: String
    public var kind: String

    private enum CodingKeys: String, CodingKey {
        case optionID = "optionId"
        case name
        case kind
    }

    public init(optionID: String, name: String, kind: String) {
        self.optionID = optionID
        self.name = name
        self.kind = kind
    }
}

public struct PermissionOutcome: Codable, Hashable, Sendable {
    public var outcome: String
    public var optionID: String?

    private enum CodingKeys: String, CodingKey {
        case outcome
        case optionID = "optionId"
    }

    public init(outcome: String, optionID: String? = nil) {
        self.outcome = outcome
        self.optionID = optionID
    }

    public static func selected(_ optionID: String) -> PermissionOutcome {
        PermissionOutcome(outcome: "selected", optionID: optionID)
    }

    public static var cancelled: PermissionOutcome {
        PermissionOutcome(outcome: "cancelled")
    }
}

public struct TerminalID: Codable, Hashable, Sendable {
    public var value: String

    public init(_ value: String) {
        self.value = value
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        value = try container.decode(String.self)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(value)
    }
}

public struct EnvVariable: Codable, Hashable, Sendable {
    public var name: String
    public var value: String

    public init(name: String, value: String) {
        self.name = name
        self.value = value
    }
}

public struct TerminalExitStatus: Codable, Hashable, Sendable {
    public var exitCode: Int?
    public var signal: String?

    public init(exitCode: Int? = nil, signal: String? = nil) {
        self.exitCode = exitCode
        self.signal = signal
    }
}

public struct ConfigOptionChoice: Codable, Hashable, Sendable {
    public var name: String
    public var value: String
    public var description: String?

    public init(name: String, value: String, description: String? = nil) {
        self.name = name
        self.value = value
        self.description = description
    }
}

public struct ConfigOptionChoiceGroup: Codable, Hashable, Sendable {
    public var group: String?
    public var name: String
    public var options: [ConfigOptionChoice]

    public init(group: String? = nil, name: String, options: [ConfigOptionChoice]) {
        self.group = group
        self.name = name
        self.options = options
    }
}

public enum ConfigOptionChoiceItem: Codable, Hashable, Sendable {
    case option(ConfigOptionChoice)
    case group(ConfigOptionChoiceGroup)

    public init(from decoder: Decoder) throws {
        let single = try decoder.singleValueContainer()
        if let group = try? single.decode(ConfigOptionChoiceGroup.self), group.group != nil || !group.options.isEmpty {
            self = .group(group)
            return
        }
        self = .option(try single.decode(ConfigOptionChoice.self))
    }

    public func encode(to encoder: Encoder) throws {
        var single = encoder.singleValueContainer()
        switch self {
        case .option(let option):
            try single.encode(option)
        case .group(let group):
            try single.encode(group)
        }
    }
}

public struct ConfigOptionDefinition: Codable, Hashable, Sendable {
    public var type: String
    public var id: String
    public var name: String
    public var description: String?
    public var currentValue: String?
    public var options: [ConfigOptionChoiceItem]?

    public init(
        type: String,
        id: String,
        name: String,
        description: String? = nil,
        currentValue: String? = nil,
        options: [ConfigOptionChoiceItem]? = nil
    ) {
        self.type = type
        self.id = id
        self.name = name
        self.description = description
        self.currentValue = currentValue
        self.options = options
    }
}
