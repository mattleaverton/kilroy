import Foundation

public enum StreamEventType: String, Sendable, Equatable {
    case streamStart = "stream_start"
    case textStart = "text_start"
    case textDelta = "text_delta"
    case textEnd = "text_end"
    case reasoningStart = "reasoning_start"
    case reasoningDelta = "reasoning_delta"
    case reasoningEnd = "reasoning_end"
    case toolCallStart = "tool_call_start"
    case toolCallDelta = "tool_call_delta"
    case toolCallEnd = "tool_call_end"
    case finish = "finish"
    case error = "error"
    case providerEvent = "provider_event"

    // Spec extension (Section 4.4 / 5.9).
    case stepFinish = "step_finish"
}

public enum StreamEventTypeTag: Sendable, Equatable {
    case standard(StreamEventType)
    case custom(String)

    public var rawValue: String {
        switch self {
        case .standard(let t): t.rawValue
        case .custom(let s): s
        }
    }
}

public struct StreamEvent: Sendable {
    public var type: StreamEventTypeTag

    public var delta: String?
    public var textId: String?

    public var reasoningDelta: String?

    public var toolCall: ToolCall?

    public var finishReason: FinishReason?
    public var usage: Usage?
    public var response: Response?

    public var error: SDKError?

    public var raw: JSONValue?

    public init(
        type: StreamEventTypeTag,
        delta: String? = nil,
        textId: String? = nil,
        reasoningDelta: String? = nil,
        toolCall: ToolCall? = nil,
        finishReason: FinishReason? = nil,
        usage: Usage? = nil,
        response: Response? = nil,
        error: SDKError? = nil,
        raw: JSONValue? = nil
    ) {
        self.type = type
        self.delta = delta
        self.textId = textId
        self.reasoningDelta = reasoningDelta
        self.toolCall = toolCall
        self.finishReason = finishReason
        self.usage = usage
        self.response = response
        self.error = error
        self.raw = raw
    }

    public init(
        type: StreamEventType,
        delta: String? = nil,
        textId: String? = nil,
        reasoningDelta: String? = nil,
        toolCall: ToolCall? = nil,
        finishReason: FinishReason? = nil,
        usage: Usage? = nil,
        response: Response? = nil,
        error: SDKError? = nil,
        raw: JSONValue? = nil
    ) {
        self.init(
            type: .standard(type),
            delta: delta,
            textId: textId,
            reasoningDelta: reasoningDelta,
            toolCall: toolCall,
            finishReason: finishReason,
            usage: usage,
            response: response,
            error: error,
            raw: raw
        )
    }

    public init(
        typeString: String,
        delta: String? = nil,
        textId: String? = nil,
        reasoningDelta: String? = nil,
        toolCall: ToolCall? = nil,
        finishReason: FinishReason? = nil,
        usage: Usage? = nil,
        response: Response? = nil,
        error: SDKError? = nil,
        raw: JSONValue? = nil
    ) {
        let tag: StreamEventTypeTag
        if let known = StreamEventType(rawValue: typeString) {
            tag = .standard(known)
        } else {
            tag = .custom(typeString)
        }
        self.init(
            type: tag,
            delta: delta,
            textId: textId,
            reasoningDelta: reasoningDelta,
            toolCall: toolCall,
            finishReason: finishReason,
            usage: usage,
            response: response,
            error: error,
            raw: raw
        )
    }

    public var eventType: StreamEventType? {
        switch type {
        case .standard(let t):
            return t
        case .custom(let raw):
            return StreamEventType(rawValue: raw)
        }
    }
}
