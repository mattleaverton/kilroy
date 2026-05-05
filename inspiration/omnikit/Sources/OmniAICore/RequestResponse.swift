import Foundation

public struct ResponseFormat: Sendable, Equatable {
    public enum Kind: String, Sendable, Codable {
        case text = "text"
        case json = "json"
        case jsonSchema = "json_schema"
    }

    public var kind: Kind
    public var jsonSchema: JSONValue?
    public var strict: Bool

    public init(kind: Kind, jsonSchema: JSONValue? = nil, strict: Bool = false) {
        self.kind = kind
        self.jsonSchema = jsonSchema
        self.strict = strict
    }

    public var rawValue: String {
        kind.rawValue
    }

    public static let text = ResponseFormat(kind: .text)
    public static let json = ResponseFormat(kind: .json)
    public static func jsonSchema(_ schema: JSONValue, strict: Bool = false) -> ResponseFormat {
        ResponseFormat(kind: .jsonSchema, jsonSchema: schema, strict: strict)
    }
}

public struct FinishReason: Sendable, Equatable {
    public enum Kind: String, Sendable, Codable {
        case stop = "stop"
        case length = "length"
        case toolCalls = "tool_calls"
        case contentFilter = "content_filter"
        case error = "error"
        case other = "other"
    }

    public var kind: Kind
    public var raw: String?

    public init(kind: Kind, raw: String? = nil) {
        self.kind = kind
        self.raw = raw
    }

    public var rawValue: String {
        kind.rawValue
    }

    public static let stop = FinishReason(kind: .stop)
    public static let length = FinishReason(kind: .length)
    public static let toolCalls = FinishReason(kind: .toolCalls)
    public static let contentFilter = FinishReason(kind: .contentFilter)
    public static let error = FinishReason(kind: .error)
    public static let other = FinishReason(kind: .other)
}

public struct Usage: Sendable, Equatable {
    public var inputTokens: Int
    public var outputTokens: Int
    public var totalTokens: Int { inputTokens + outputTokens }
    public var reasoningTokens: Int?
    public var cacheReadTokens: Int?
    public var cacheWriteTokens: Int?
    public var raw: JSONValue?

    public init(
        inputTokens: Int,
        outputTokens: Int,
        reasoningTokens: Int? = nil,
        cacheReadTokens: Int? = nil,
        cacheWriteTokens: Int? = nil,
        raw: JSONValue? = nil
    ) {
        self.inputTokens = inputTokens
        self.outputTokens = outputTokens
        self.reasoningTokens = reasoningTokens
        self.cacheReadTokens = cacheReadTokens
        self.cacheWriteTokens = cacheWriteTokens
        self.raw = raw
    }

    public static func + (lhs: Usage, rhs: Usage) -> Usage {
        func sumOpt(_ a: Int?, _ b: Int?) -> Int? {
            switch (a, b) {
            case (nil, nil): return nil
            default: return (a ?? 0) + (b ?? 0)
            }
        }
        return Usage(
            inputTokens: lhs.inputTokens + rhs.inputTokens,
            outputTokens: lhs.outputTokens + rhs.outputTokens,
            reasoningTokens: sumOpt(lhs.reasoningTokens, rhs.reasoningTokens),
            cacheReadTokens: sumOpt(lhs.cacheReadTokens, rhs.cacheReadTokens),
            cacheWriteTokens: sumOpt(lhs.cacheWriteTokens, rhs.cacheWriteTokens),
            raw: nil
        )
    }

    public static let zero = Usage(inputTokens: 0, outputTokens: 0)
}

public struct Warning: Sendable, Equatable {
    public var message: String
    public var code: String?

    public init(message: String, code: String? = nil) {
        self.message = message
        self.code = code
    }
}

public struct RateLimitInfo: Sendable, Equatable {
    public var requestsRemaining: Int?
    public var requestsLimit: Int?
    public var tokensRemaining: Int?
    public var tokensLimit: Int?
    public var resetAt: Date?

    public init(
        requestsRemaining: Int? = nil,
        requestsLimit: Int? = nil,
        tokensRemaining: Int? = nil,
        tokensLimit: Int? = nil,
        resetAt: Date? = nil
    ) {
        self.requestsRemaining = requestsRemaining
        self.requestsLimit = requestsLimit
        self.tokensRemaining = tokensRemaining
        self.tokensLimit = tokensLimit
        self.resetAt = resetAt
    }
}

public struct Request: Sendable {
    public var model: String
    public var messages: [Message]
    public var provider: String?
    public var previousResponseId: String?

    public var tools: [Tool]?
    public var toolChoice: ToolChoice?
    public var responseFormat: ResponseFormat?

    public var temperature: Double?
    public var topP: Double?
    public var maxTokens: Int?
    public var stopSequences: [String]?
    public var reasoningEffort: String?
    public var metadata: [String: String]?

    // Escape hatch for provider-specific params: { "providerName": { ... } }
    public var providerOptions: [String: JSONValue]?

    // Execution controls (not part of provider payload).
    public var timeout: Timeout?
    public var abortSignal: AbortSignal?

    public init(
        model: String,
        messages: [Message],
        provider: String? = nil,
        previousResponseId: String? = nil,
        tools: [Tool]? = nil,
        toolChoice: ToolChoice? = nil,
        responseFormat: ResponseFormat? = nil,
        temperature: Double? = nil,
        topP: Double? = nil,
        maxTokens: Int? = nil,
        stopSequences: [String]? = nil,
        reasoningEffort: String? = nil,
        metadata: [String: String]? = nil,
        providerOptions: [String: JSONValue]? = nil,
        timeout: Timeout? = nil,
        abortSignal: AbortSignal? = nil
    ) {
        self.model = model
        self.messages = messages
        self.provider = provider
        self.previousResponseId = previousResponseId
        self.tools = tools
        self.toolChoice = toolChoice
        self.responseFormat = responseFormat
        self.temperature = temperature
        self.topP = topP
        self.maxTokens = maxTokens
        self.stopSequences = stopSequences
        self.reasoningEffort = reasoningEffort
        self.metadata = metadata
        self.providerOptions = providerOptions
        self.timeout = timeout
        self.abortSignal = abortSignal
    }

    func optionsObject(for provider: String) -> [String: JSONValue] {
        guard let providerOptions else { return [:] }
        guard let v = providerOptions[provider], let obj = v.objectValue else { return [:] }
        return obj
    }
}

public struct Response: Sendable, Equatable {
    public var id: String
    public var model: String
    public var provider: String
    public var message: Message
    public var finishReason: FinishReason
    public var usage: Usage
    public var raw: JSONValue?
    public var warnings: [Warning]
    public var rateLimit: RateLimitInfo?

    public init(
        id: String,
        model: String,
        provider: String,
        message: Message,
        finishReason: FinishReason,
        usage: Usage,
        raw: JSONValue? = nil,
        warnings: [Warning] = [],
        rateLimit: RateLimitInfo? = nil
    ) {
        self.id = id
        self.model = model
        self.provider = provider
        self.message = message
        self.finishReason = finishReason
        self.usage = usage
        self.raw = raw
        self.warnings = warnings
        self.rateLimit = rateLimit
    }

    public var text: String { message.text }

    public var toolCalls: [ToolCall] { message.toolCalls }

    public var reasoning: String? { message.reasoning }
}

public struct ToolResult: Sendable, Equatable {
    public var toolCallId: String
    public var content: JSONValue
    public var isError: Bool
    public var imageData: [UInt8]?
    public var imageMediaType: String?

    public init(
        toolCallId: String,
        content: JSONValue,
        isError: Bool,
        imageData: [UInt8]? = nil,
        imageMediaType: String? = nil
    ) {
        self.toolCallId = toolCallId
        self.content = content
        self.isError = isError
        self.imageData = imageData
        self.imageMediaType = imageMediaType
    }
}
