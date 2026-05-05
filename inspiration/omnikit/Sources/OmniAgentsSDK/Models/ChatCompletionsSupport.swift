import Foundation
import OmniAICore

enum ModelConversion {
    static func inputItemsToMessages(systemInstructions: String?, input: StringOrInputList) -> [Message] {
        var messages: [Message] = []
        if let systemInstructions, !systemInstructions.isEmpty {
            messages.append(.system(systemInstructions))
        }

        var pendingAssistantToolCalls: [ToolCall] = []

        func flushToolCallsIfNeeded() {
            guard !pendingAssistantToolCalls.isEmpty else { return }
            let parts = pendingAssistantToolCalls.map(ContentPart.toolCall)
            messages.append(Message(role: .assistant, content: parts))
            pendingAssistantToolCalls.removeAll(keepingCapacity: true)
        }

        for item in input.inputItems {
            let type = item["type"]?.stringValue
            if type == "function_call" || type == "computer_call" || type == "shell_call" {
                let arguments: [String: JSONValue]
                if let argumentsObject = item["arguments"]?.objectValue {
                    arguments = argumentsObject
                } else if let argumentsString = item["arguments"]?.stringValue,
                          let data = argumentsString.data(using: .utf8),
                          let value = try? JSONValue.parse(data),
                          case .object(let object) = value {
                    arguments = object
                } else {
                    arguments = [:]
                }

                pendingAssistantToolCalls.append(ToolCall(
                    id: item["call_id"]?.stringValue ?? item["id"]?.stringValue ?? UUID().uuidString,
                    name: item["name"]?.stringValue ?? type ?? "tool",
                    arguments: arguments,
                    rawArguments: item["arguments"]?.stringValue,
                    thoughtSignature: item["thought_signature"]?.stringValue,
                    providerItemId: item["id"]?.stringValue
                ))
                continue
            }

            flushToolCallsIfNeeded()

            switch type {
            case "function_call_output", "computer_call_output", "shell_call_output":
                let content = item["output"] ?? .string("")
                let toolCallID = item["call_id"]?.stringValue ?? item["id"]?.stringValue ?? UUID().uuidString
                messages.append(.toolResult(toolCallId: toolCallID, content: content, isError: false))
            case "message", nil:
                let role = item["role"]?.stringValue ?? "user"
                if case .array(let contentArray)? = item["content"] {
                    let parts = contentArray.compactMap(messageContentPart)
                    if !parts.isEmpty {
                        messages.append(Message(role: roleFromString(role), content: parts))
                    }
                } else if let content = item["content"]?.stringValue {
                    messages.append(Message(role: roleFromString(role), content: [.text(content)]))
                }
            case "reasoning":
                if let text = item["text"]?.stringValue {
                    messages.append(Message(role: .assistant, content: [.thinking(.init(text: text))]))
                }
            default:
                if let content = item["content"]?.stringValue {
                    messages.append(Message(role: .user, content: [.text(content)]))
                }
            }
        }

        flushToolCallsIfNeeded()
        return messages
    }

    static func responseToModelResponse(_ response: Response) -> ModelResponse {
        if let rawOutput = response.raw?["output"]?.arrayValue {
            let normalized = rawOutput.compactMap { $0.objectValue }
            if !normalized.isEmpty {
                let usage = Usage(
                    requests: 1,
                    inputTokens: response.usage.inputTokens,
                    inputTokensDetails: InputTokensDetails(cachedTokens: response.usage.cacheReadTokens ?? 0),
                    outputTokens: response.usage.outputTokens,
                    outputTokensDetails: OutputTokensDetails(reasoningTokens: response.usage.reasoningTokens ?? 0),
                    totalTokens: response.usage.totalTokens
                )
                return ModelResponse(output: normalized, usage: usage, responseID: response.id, requestID: nil)
            }
        }

        var output: [TResponseOutputItem] = []

        let messageContent: [JSONValue] = response.message.content.compactMap { part in
            switch part.kind.rawValue {
            case ContentKind.text.rawValue:
                return .object(["type": .string("output_text"), "text": .string(part.text ?? "")])
            case ContentKind.thinking.rawValue, ContentKind.redactedThinking.rawValue:
                return .object(["type": .string("reasoning"), "text": .string(part.thinking?.text ?? "")])
            default:
                return nil
            }
        }

        if !messageContent.isEmpty {
            output.append([
                "id": .string(response.id),
                "type": .string("message"),
                "role": .string("assistant"),
                "content": .array(messageContent),
            ])
        }

        for toolCall in response.toolCalls {
            output.append([
                "id": .string(toolCall.providerItemId ?? toolCall.id),
                "call_id": .string(toolCall.id),
                "type": .string("function_call"),
                "name": .string(toolCall.name),
                "arguments": toolCall.rawArguments.map(JSONValue.string) ?? .object(toolCall.arguments),
            ])
        }

        if let reasoning = response.reasoning, !reasoning.isEmpty, output.isEmpty {
            output.append([
                "id": .string(response.id),
                "type": .string("reasoning"),
                "text": .string(reasoning),
            ])
        }

        let usage = Usage(
            requests: 1,
            inputTokens: response.usage.inputTokens,
            inputTokensDetails: InputTokensDetails(cachedTokens: response.usage.cacheReadTokens ?? 0),
            outputTokens: response.usage.outputTokens,
            outputTokensDetails: OutputTokensDetails(reasoningTokens: response.usage.reasoningTokens ?? 0),
            totalTokens: response.usage.totalTokens
        )
        return ModelResponse(output: output, usage: usage, responseID: response.id, requestID: nil)
    }

    static func streamEventToResponseEvent(_ event: StreamEvent) -> TResponseStreamEvent {
        var payload: TResponseStreamEvent = ["type": .string(event.type.rawValue)]
        if let delta = event.delta {
            payload["delta"] = .string(delta)
        }
        if let textID = event.textId {
            payload["text_id"] = .string(textID)
        }
        if let reasoningDelta = event.reasoningDelta {
            payload["reasoning_delta"] = .string(reasoningDelta)
        }
        if let toolCall = event.toolCall {
            payload["tool_call"] = .object([
                "id": .string(toolCall.id),
                "name": .string(toolCall.name),
                "arguments": .object(toolCall.arguments),
            ])
        }
        if let raw = event.raw {
            payload["raw"] = raw
        }
        return payload
    }

    private static func roleFromString(_ role: String) -> Role {
        switch role.lowercased() {
        case "assistant": return .assistant
        case "system": return .system
        case "tool": return .tool
        case "developer": return .developer
        default: return .user
        }
    }

    private static func messageContentPart(_ value: JSONValue) -> ContentPart? {
        guard case .object(let object) = value else {
            return nil
        }
        switch object["type"]?.stringValue {
        case "output_text", "text", "input_text":
            return .text(object["text"]?.stringValue ?? "")
        case "reasoning":
            return .thinking(.init(text: object["text"]?.stringValue ?? ""))
        default:
            return nil
        }
    }
}
