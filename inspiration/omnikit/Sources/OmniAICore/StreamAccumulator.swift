import Foundation

public struct StreamAccumulator: Sendable {
    private var textOrder: [String] = []
    private var textById: [String: String] = [:]

    private var toolCallOrder: [String] = []
    private var toolCallById: [String: ToolCall] = [:]
    private var toolCallStartedIDs: Set<String> = []
    private var toolCallCompletedIDs: Set<String> = []

    private var reasoning: String = ""

    private var finishReason: FinishReason?
    private var usage: Usage?
    private var finalResponse: Response?

    public init() {}

    public mutating func process(_ event: StreamEvent) {
        // Always accumulate deltas first, even on finish events.
        switch event.type.rawValue {
        case StreamEventType.textStart.rawValue:
            let id = event.textId ?? "text_0"
            if textById[id] == nil {
                textOrder.append(id)
                textById[id] = ""
            }
        case StreamEventType.textDelta.rawValue:
            let id = event.textId ?? "text_0"
            if textById[id] == nil {
                textOrder.append(id)
                textById[id] = ""
            }
            textById[id, default: ""].append(event.delta ?? "")
        case StreamEventType.reasoningDelta.rawValue:
            reasoning.append(event.reasoningDelta ?? "")
        case StreamEventType.toolCallStart.rawValue, StreamEventType.toolCallDelta.rawValue, StreamEventType.toolCallEnd.rawValue:
            if let call = event.toolCall {
                if toolCallById[call.id] == nil {
                    toolCallOrder.append(call.id)
                }
                toolCallById[call.id] = call
                toolCallStartedIDs.insert(call.id)
                if event.type.rawValue == StreamEventType.toolCallEnd.rawValue {
                    toolCallCompletedIDs.insert(call.id)
                }
            }
        case StreamEventType.finish.rawValue:
            finishReason = event.finishReason
            usage = event.usage
            if let response = event.response {
                finalResponse = response
                finishReason = event.finishReason ?? response.finishReason
                usage = event.usage ?? response.usage
            }
        default:
            break
        }
    }

    public func response() -> Response? {
        // Prefer accumulated deltas over the final response payload when we
        // have richer data — the response.completed payload from some providers
        // (e.g. OpenAI WebSocket) may omit streamed tool calls or text.
        let accumulatedText = textOrder.compactMap { textById[$0] }.joined()
        let hasAccumulatedContent = !accumulatedText.isEmpty || !toolCallOrder.isEmpty || !reasoning.isEmpty

        if let finalResponse, !hasAccumulatedContent {
            return finalResponse
        }

        var parts: [ContentPart] = []
        if !accumulatedText.isEmpty {
            parts.append(.text(accumulatedText))
        } else if let finalResponse, !finalResponse.text.isEmpty {
            parts.append(.text(finalResponse.text))
        }

        for id in toolCallOrder where toolCallCompletedIDs.contains(id) {
            if let call = toolCallById[id] {
                parts.append(.toolCall(call))
            }
        }
        // If no accumulated completed tool calls but finalResponse has tool calls,
        // use those only when no streamed tool call started. If streamed calls started
        // but never completed, they are considered incomplete and must not execute.
        if toolCallOrder.isEmpty, let finalResponse, !finalResponse.toolCalls.isEmpty {
            for call in finalResponse.toolCalls {
                parts.append(.toolCall(call))
            }
        }

        if !reasoning.isEmpty {
            parts.append(.thinking(ThinkingData(text: reasoning, signature: nil, redacted: false)))
        }

        let resolvedFinishReason = finishReason ?? finalResponse?.finishReason
        let resolvedUsage = usage ?? finalResponse?.usage

        guard let resolvedFinishReason, let resolvedUsage else {
            return nil
        }

        let base = finalResponse
        return Response(
            id: base?.id ?? "accumulated",
            model: base?.model ?? "unknown",
            provider: base?.provider ?? "unknown",
            message: Message(role: .assistant, content: parts),
            finishReason: resolvedFinishReason,
            usage: resolvedUsage,
            raw: base?.raw,
            warnings: base?.warnings ?? [],
            rateLimit: base?.rateLimit
        )
    }

    public func hasIncompleteToolCalls() -> Bool {
        !toolCallStartedIDs.subtracting(toolCallCompletedIDs).isEmpty
    }

    public func incompleteToolCallCount() -> Int {
        toolCallStartedIDs.subtracting(toolCallCompletedIDs).count
    }
}
