import Foundation

public enum Role: String, Sendable, Equatable {
    case system
    case user
    case assistant
    case tool
    case developer
}

public enum ContentKind: String, Sendable, Equatable {
    case text
    case image
    case audio
    case document
    case toolCall = "tool_call"
    case toolResult = "tool_result"
    case thinking
    case redactedThinking = "redacted_thinking"
}

public enum ContentKindTag: Sendable, Equatable {
    case standard(ContentKind)
    case custom(String)

    public var rawValue: String {
        switch self {
        case .standard(let k): k.rawValue
        case .custom(let s): s
        }
    }
}

public struct ImageData: Sendable, Equatable {
    public var url: String?
    public var data: [UInt8]?
    public var mediaType: String?
    public var detail: String?

    public init(url: String? = nil, data: [UInt8]? = nil, mediaType: String? = nil, detail: String? = nil) {
        self.url = url
        self.data = data
        self.mediaType = mediaType
        self.detail = detail
    }
}

public struct AudioData: Sendable, Equatable {
    public var url: String?
    public var data: [UInt8]?
    public var mediaType: String?

    public init(url: String? = nil, data: [UInt8]? = nil, mediaType: String? = nil) {
        self.url = url
        self.data = data
        self.mediaType = mediaType
    }
}

public struct DocumentData: Sendable, Equatable {
    public var url: String?
    public var data: [UInt8]?
    public var mediaType: String?
    public var fileName: String?

    public init(url: String? = nil, data: [UInt8]? = nil, mediaType: String? = nil, fileName: String? = nil) {
        self.url = url
        self.data = data
        self.mediaType = mediaType
        self.fileName = fileName
    }
}

public struct ToolCall: Sendable, Equatable {
    public var id: String
    public var name: String
    public var arguments: [String: JSONValue]
    public var rawArguments: String?
    // Gemini 2.0/3.x tool calling can require passing back a per-function-call thought signature.
    // We keep this provider-specific field here so the adapter can round-trip it.
    public var thoughtSignature: String?
    // OpenAI Responses function_call items include both an item `id` (e.g. "fc_...") and a `call_id`
    // (e.g. "call_..."). Tool outputs reference `call_id`, but some reasoning flows require the item
    // `id` to be replayed as well. We store it here so the adapter can round-trip it.
    public var providerItemId: String?

    public init(
        id: String,
        name: String,
        arguments: [String: JSONValue],
        rawArguments: String? = nil,
        thoughtSignature: String? = nil,
        providerItemId: String? = nil
    ) {
        self.id = id
        self.name = name
        self.arguments = arguments
        self.rawArguments = rawArguments
        self.thoughtSignature = thoughtSignature
        self.providerItemId = providerItemId
    }
}

public struct ToolResultData: Sendable, Equatable {
    public var toolCallId: String
    public var content: JSONValue
    public var isError: Bool
    public var imageData: [UInt8]?
    public var imageMediaType: String?

    public init(toolCallId: String, content: JSONValue, isError: Bool, imageData: [UInt8]? = nil, imageMediaType: String? = nil) {
        self.toolCallId = toolCallId
        self.content = content
        self.isError = isError
        self.imageData = imageData
        self.imageMediaType = imageMediaType
    }
}

public struct ThinkingData: Sendable, Equatable {
    public var text: String
    public var signature: String?
    public var redacted: Bool

    public init(text: String, signature: String? = nil, redacted: Bool = false) {
        self.text = text
        self.signature = signature
        self.redacted = redacted
    }
}

public struct ContentPart: Sendable, Equatable {
    public var kind: ContentKindTag
    public var text: String?
    public var image: ImageData?
    public var audio: AudioData?
    public var document: DocumentData?
    public var toolCall: ToolCall?
    public var toolResult: ToolResultData?
    public var thinking: ThinkingData?
    // For provider-specific extensions where we need to round-trip a structured payload.
    // Example: OpenAI Responses "reasoning" items must be passed back during tool calling.
    public var data: JSONValue?

    public init(
        kind: ContentKindTag,
        text: String? = nil,
        image: ImageData? = nil,
        audio: AudioData? = nil,
        document: DocumentData? = nil,
        toolCall: ToolCall? = nil,
        toolResult: ToolResultData? = nil,
        thinking: ThinkingData? = nil,
        data: JSONValue? = nil
    ) {
        self.kind = kind
        self.text = text
        self.image = image
        self.audio = audio
        self.document = document
        self.toolCall = toolCall
        self.toolResult = toolResult
        self.thinking = thinking
        self.data = data
    }

    /// Compatibility alias used in `OmniAILLMClient`.
    public var contentKind: ContentKind? {
        if case .standard(let k) = kind {
            return k
        }
        return nil
    }

    public static func text(_ value: String) -> ContentPart {
        ContentPart(kind: .standard(.text), text: value)
    }

    public static func image(_ image: ImageData) -> ContentPart {
        ContentPart(kind: .standard(.image), image: image)
    }

    public static func toolCall(_ call: ToolCall) -> ContentPart {
        ContentPart(kind: .standard(.toolCall), toolCall: call)
    }

    public static func toolResult(_ result: ToolResultData) -> ContentPart {
        ContentPart(kind: .standard(.toolResult), toolResult: result)
    }

    public static func thinking(_ t: ThinkingData) -> ContentPart {
        let k: ContentKind = t.redacted ? .redactedThinking : .thinking
        return ContentPart(kind: .standard(k), thinking: t)
    }
}

public struct Message: Sendable, Equatable {
    public var role: Role
    public var content: [ContentPart]
    public var name: String?
    public var toolCallId: String?

    public init(role: Role, content: [ContentPart], name: String? = nil, toolCallId: String? = nil) {
        self.role = role
        self.content = content
        self.name = name
        self.toolCallId = toolCallId
    }

    public static func system(_ text: String) -> Message {
        Message(role: .system, content: [.text(text)])
    }

    public static func developer(_ text: String) -> Message {
        Message(role: .developer, content: [.text(text)])
    }

    public static func user(_ text: String) -> Message {
        Message(role: .user, content: [.text(text)])
    }

    public static func assistant(_ text: String) -> Message {
        Message(role: .assistant, content: [.text(text)])
    }

    public static func toolResult(
        toolCallId: String,
        toolName: String? = nil,
        content: JSONValue,
        isError: Bool,
        imageData: [UInt8]? = nil,
        imageMediaType: String? = nil
    ) -> Message {
        Message(
            role: .tool,
            content: [.toolResult(.init(
                toolCallId: toolCallId,
                content: content,
                isError: isError,
                imageData: imageData,
                imageMediaType: imageMediaType
            ))],
            name: toolName,
            toolCallId: toolCallId
        )
    }

    public var text: String {
        content.compactMap { part in
            guard part.kind.rawValue == ContentKind.text.rawValue else { return nil }
            return part.text
        }.joined()
    }

    public var toolCalls: [ToolCall] {
        content.compactMap { part in
            guard part.kind.rawValue == ContentKind.toolCall.rawValue else { return nil }
            return part.toolCall
        }
    }

    public var reasoning: String? {
        let parts: [String] = content.compactMap { part -> String? in
            guard part.kind.rawValue == ContentKind.thinking.rawValue || part.kind.rawValue == ContentKind.redactedThinking.rawValue else {
                return nil
            }
            return part.thinking?.text
        }
        return parts.isEmpty ? nil : parts.joined()
    }
}
