import Foundation

public struct ToolContinuationRequest: Sendable {
    public var model: String
    public var provider: String?
    public var previousResponseId: String?
    public var messages: [Message]
    public var toolCalls: [ToolCall]
    public var toolResults: [ToolResult]
    public var additionalMessages: [Message]

    public var tools: [Tool]?
    public var toolChoice: ToolChoice?
    public var responseFormat: ResponseFormat?

    public var temperature: Double?
    public var topP: Double?
    public var maxTokens: Int?
    public var stopSequences: [String]?
    public var reasoningEffort: String?
    public var metadata: [String: String]?

    public var providerOptions: [String: JSONValue]?

    public var timeout: Timeout?
    public var abortSignal: AbortSignal?

    public init(
        model: String,
        provider: String? = nil,
        previousResponseId: String? = nil,
        messages: [Message],
        toolCalls: [ToolCall],
        toolResults: [ToolResult],
        additionalMessages: [Message] = [],
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
        self.provider = provider
        self.previousResponseId = previousResponseId
        self.messages = messages
        self.toolCalls = toolCalls
        self.toolResults = toolResults
        self.additionalMessages = additionalMessages
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
}

internal func makeToolResultMessages(
    toolResults: [ToolResult],
    toolCalls: [ToolCall]
) -> [Message] {
    let toolNameById = Dictionary(uniqueKeysWithValues: toolCalls.map { ($0.id, $0.name) })
    return toolResults.map { result in
        Message.toolResult(
            toolCallId: result.toolCallId,
            toolName: toolNameById[result.toolCallId],
            content: result.content,
            isError: result.isError,
            imageData: result.imageData,
            imageMediaType: result.imageMediaType
        )
    }
}
