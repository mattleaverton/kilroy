import Foundation

import OmniHTTP

public final class CerebrasAdapter: ProviderAdapter, @unchecked Sendable {
    public let name: String = "cerebras"

    private let apiKey: String
    private let baseURL: String
    private let transport: HTTPTransport

    public init(apiKey: String, baseURL: String? = nil, transport: HTTPTransport) {
        self.apiKey = apiKey
        self.baseURL = baseURL ?? "https://api.cerebras.ai/v1"
        self.transport = transport
    }

    public func complete(request: Request) async throws -> Response {
        try await request.abortSignal?.check()

        let url = try _ProviderHTTP.makeURL(baseURL: baseURL, path: "/chat/completions")
        let body = try buildRequestBody(request: request, stream: false)

        var headers = HTTPHeaders()
        headers.set(name: "authorization", value: "Bearer \(apiKey)")
        headers.set(name: "content-type", value: "application/json")

        let timeout = request.timeout?.asConfig.total
        let http = try await transport.send(
            HTTPRequest(method: .post, url: url, headers: headers, body: .bytes(body)),
            timeout: timeout
        )

        if !(200..<300).contains(http.statusCode) {
            let json = _ProviderHTTP.parseJSONBody(http)
            let msg = _ProviderHTTP.errorMessage(from: json).message ?? "Cerebras error"
            let code = _ProviderHTTP.errorMessage(from: json).code
            let retryAfter = _ProviderHTTP.parseRetryAfterSeconds(http.headers)
            throw _ErrorMapping.sdkErrorFromHTTP(
                provider: name,
                statusCode: http.statusCode,
                message: msg,
                errorCode: code,
                retryAfter: retryAfter,
                raw: json
            )
        }

        let json = _ProviderHTTP.parseJSONBody(http)
        return try parseResponse(json: json, headers: http.headers, requestedModel: request.model)
    }

    public func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        try await request.abortSignal?.check()

        let url = try _ProviderHTTP.makeURL(baseURL: baseURL, path: "/chat/completions")
        let body = try buildRequestBody(request: request, stream: true)

        var headers = HTTPHeaders()
        headers.set(name: "authorization", value: "Bearer \(apiKey)")
        headers.set(name: "content-type", value: "application/json")
        headers.set(name: "accept", value: "text/event-stream")

        let timeout = request.timeout?.asConfig.total
        let res: HTTPStreamResponse
        do {
            res = try await transport.openStream(
                HTTPRequest(method: .post, url: url, headers: headers, body: .bytes(body)),
                timeout: timeout
            )
        } catch {
            // Linux FoundationNetworking does not currently support URLSession streaming.
            // Fallback to non-stream complete and synthesize stream events.
            return try await fallbackStreamViaComplete(request: request)
        }

        if !(200..<300).contains(res.statusCode) {
            var bytes: [UInt8] = []
            for try await chunk in res.body {
                bytes.append(contentsOf: chunk)
                if bytes.count > 512 * 1024 { break }
            }
            let json = (try? JSONValue.parse(bytes)) ?? nil
            let msg = _ProviderHTTP.errorMessage(from: json).message ?? "Cerebras error"
            let code = _ProviderHTTP.errorMessage(from: json).code
            let retryAfter = _ProviderHTTP.parseRetryAfterSeconds(res.headers)
            throw _ErrorMapping.sdkErrorFromHTTP(
                provider: name,
                statusCode: res.statusCode,
                message: msg,
                errorCode: code,
                retryAfter: retryAfter,
                raw: json
            )
        }

        let sse = SSE.parse(res.body)

        return AsyncThrowingStream { continuation in
            Task {
                continuation.yield(StreamEvent(type: .standard(.streamStart)))

                struct PartialToolCall {
                    var id: String
                    var name: String
                    var rawArguments: String
                }

                let textId = "text_0"
                var didStartText = false
                var accumulatedText = ""

                var didStartReasoning = false
                var accumulatedReasoning = ""

                var partialToolCalls: [Int: PartialToolCall] = [:]
                var toolCallOrder: [Int] = []
                var indexByToolID: [String: Int] = [:]
                var nextSyntheticIndex = 10_000

                var finishReasonRaw: String? = nil
                var responseId = "chatcmpl_\(UUID().uuidString)"
                var modelUsed = request.model
                var finalUsage = Usage(inputTokens: 0, outputTokens: 0)

                do {
                    for try await ev in sse {
                        if Task.isCancelled { break }

                        let data = ev.data.trimmingCharacters(in: .whitespacesAndNewlines)
                        if data == "[DONE]" {
                            break
                        }

                        guard let payloadData = data.data(using: .utf8),
                              let payload = try? JSONValue.parse(payloadData)
                        else {
                            continue
                        }

                        if let rid = payload["id"]?.stringValue {
                            responseId = rid
                        }
                        if let model = payload["model"]?.stringValue {
                            modelUsed = model
                        }
                        if let usage = payload["usage"] {
                            finalUsage = parseUsage(usage)
                        }

                        guard let firstChoice = payload["choices"]?.arrayValue?.first else {
                            continue
                        }

                        if let finish = firstChoice["finish_reason"]?.stringValue {
                            finishReasonRaw = finish
                        }

                        guard let delta = firstChoice["delta"] else {
                            continue
                        }

                        if let reasoningDelta = delta["reasoning"]?.stringValue, !reasoningDelta.isEmpty {
                            if !didStartReasoning {
                                didStartReasoning = true
                                continuation.yield(StreamEvent(type: .standard(.reasoningStart)))
                            }
                            accumulatedReasoning += reasoningDelta
                            continuation.yield(StreamEvent(type: .standard(.reasoningDelta), reasoningDelta: reasoningDelta, raw: payload))
                        }

                        if let textDelta = delta["content"]?.stringValue, !textDelta.isEmpty {
                            if !didStartText {
                                didStartText = true
                                continuation.yield(StreamEvent(type: .standard(.textStart), textId: textId))
                            }
                            accumulatedText += textDelta
                            continuation.yield(StreamEvent(type: .standard(.textDelta), delta: textDelta, textId: textId, raw: payload))
                        }

                        if let toolCallDeltas = delta["tool_calls"]?.arrayValue {
                            for raw in toolCallDeltas {
                                let providedID = raw["id"]?.stringValue
                                let explicitIndex: Int? = {
                                    if let n = raw["index"]?.doubleValue { return Int(n) }
                                    return nil
                                }()

                                let index: Int = {
                                    if let explicitIndex { return explicitIndex }
                                    if let providedID, let existing = indexByToolID[providedID] { return existing }
                                    let next = nextSyntheticIndex
                                    nextSyntheticIndex += 1
                                    return next
                                }()

                                let fn = raw["function"]
                                let deltaName = fn?["name"]?.stringValue
                                let deltaArgs = fn?["arguments"]?.stringValue ?? ""

                                let wasNew = partialToolCalls[index] == nil
                                var partial = partialToolCalls[index] ?? PartialToolCall(
                                    id: providedID ?? "call_\(UUID().uuidString)",
                                    name: deltaName ?? "",
                                    rawArguments: ""
                                )

                                if let providedID, !providedID.isEmpty {
                                    partial.id = providedID
                                    indexByToolID[providedID] = index
                                }
                                if let deltaName, !deltaName.isEmpty {
                                    partial.name = deltaName
                                }
                                if !deltaArgs.isEmpty {
                                    partial.rawArguments += deltaArgs
                                }

                                partialToolCalls[index] = partial
                                if wasNew {
                                    toolCallOrder.append(index)
                                    continuation.yield(StreamEvent(type: .standard(.toolCallStart), toolCall: ToolCall(id: partial.id, name: partial.name, arguments: [:], rawArguments: partial.rawArguments), raw: payload))
                                }
                                if !deltaArgs.isEmpty {
                                    continuation.yield(StreamEvent(type: .standard(.toolCallDelta), toolCall: ToolCall(id: partial.id, name: partial.name, arguments: [:], rawArguments: partial.rawArguments), raw: payload))
                                }
                            }
                        }
                    }

                    if didStartText {
                        continuation.yield(StreamEvent(type: .standard(.textEnd), textId: textId))
                    }
                    if didStartReasoning {
                        continuation.yield(StreamEvent(type: .standard(.reasoningEnd)))
                    }

                    var parts: [ContentPart] = []
                    if !accumulatedText.isEmpty {
                        parts.append(.text(accumulatedText))
                    }
                    if !accumulatedReasoning.isEmpty {
                        parts.append(.thinking(ThinkingData(text: accumulatedReasoning)))
                    }

                    var toolCallsForResponse: [ToolCall] = []
                    for idx in toolCallOrder {
                        guard let partial = partialToolCalls[idx] else { continue }
                        let args: [String: JSONValue] = {
                            guard !partial.rawArguments.isEmpty,
                                  let data = partial.rawArguments.data(using: .utf8),
                                  let parsed = try? JSONValue.parse(data),
                                  let obj = parsed.objectValue
                            else { return [:] }
                            return obj
                        }()
                        let call = ToolCall(
                            id: partial.id,
                            name: partial.name,
                            arguments: args,
                            rawArguments: partial.rawArguments
                        )
                        toolCallsForResponse.append(call)
                        parts.append(.toolCall(call))
                        continuation.yield(StreamEvent(type: .standard(.toolCallEnd), toolCall: call))
                    }

                    let finish = FinishReason(
                        kind: mapFinishReason(finishReasonRaw, hasToolCalls: !toolCallsForResponse.isEmpty),
                        raw: finishReasonRaw
                    )
                    let response = Response(
                        id: responseId,
                        model: modelUsed,
                        provider: name,
                        message: Message(role: .assistant, content: parts),
                        finishReason: finish,
                        usage: finalUsage,
                        raw: nil,
                        warnings: [],
                        rateLimit: _ProviderHTTP.parseRateLimitInfo(res.headers)
                    )
                    continuation.yield(StreamEvent(type: .standard(.finish), finishReason: finish, usage: finalUsage, response: response))
                    continuation.finish()
                } catch {
                    continuation.yield(StreamEvent(type: .standard(.error), error: (error as? SDKError) ?? StreamError(message: String(describing: error), cause: error)))
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    public func supportsToolChoice(_ mode: ToolChoiceMode) -> Bool {
        true
    }

    // MARK: - Request/Response Translation

    private func buildRequestBody(request: Request, stream: Bool) throws -> [UInt8] {
        let opts = request.optionsObject(for: name)

        var messages: [JSONValue] = []
        messages.reserveCapacity(request.messages.count)
        for message in request.messages {
            if let translated = try translateMessage(message) {
                messages.append(translated)
            }
        }

        var root: [String: JSONValue] = [
            "model": .string(request.model),
            "messages": .array(messages),
        ]

        if stream {
            root["stream"] = .bool(true)
        }
        if let temperature = request.temperature {
            root["temperature"] = .number(temperature)
        }
        if let topP = request.topP {
            root["top_p"] = .number(topP)
        }
        if let maxTokens = request.maxTokens {
            root["max_tokens"] = .number(Double(maxTokens))
        }
        if let stop = request.stopSequences, !stop.isEmpty {
            if stop.count == 1, let only = stop.first {
                root["stop"] = .string(only)
            } else {
                root["stop"] = .array(stop.map { .string($0) })
            }
        }
        if let reasoningEffort = request.reasoningEffort {
            root["reasoning_effort"] = .string(reasoningEffort)
        }
        if opts["disable_reasoning"] == nil {
            // GLM reasoning models can spend the entire completion budget in reasoning traces unless
            // explicitly disabled. Keep default behavior deterministic for generic text/tool flows.
            root["disable_reasoning"] = .bool(request.reasoningEffort == nil)
        }
        if let user = request.metadata?["user"], !user.isEmpty {
            root["user"] = .string(user)
        }

        if let responseFormat = request.responseFormat {
            switch responseFormat.kind {
            case .jsonSchema:
                if let schema = responseFormat.jsonSchema {
                    root["response_format"] = .object([
                        "type": .string("json_schema"),
                        "json_schema": .object([
                            "name": .string("response"),
                            "schema": schema,
                            "strict": .bool(responseFormat.strict),
                        ]),
                    ])
                }
            case .json:
                root["response_format"] = .object(["type": .string("json_object")])
            default:
                break
            }
        }

        if let tools = request.tools, !tools.isEmpty {
            root["tools"] = .array(tools.map { tool in
                .object([
                    "type": .string("function"),
                    "function": .object([
                        "name": .string(tool.name),
                        "description": .string(tool.description),
                        "parameters": tool.parameters,
                    ]),
                ])
            })

            if let choice = request.toolChoice {
                root["tool_choice"] = mapToolChoice(choice)
            } else {
                root["tool_choice"] = .string("auto")
            }
        }

        // Provider options escape hatch: shallow merge into root (cerebras-specific).
        for (k, v) in opts {
            root[k] = v
        }

        return try _ProviderHTTP.jsonBytes(.object(root))
    }

    private func translateMessage(_ msg: Message) throws -> JSONValue? {
        switch msg.role {
        case .system, .developer:
            let text = msg.text
            guard !text.isEmpty else { return nil }
            return .object([
                "role": .string("system"),
                "content": .string(text),
            ])
        case .user:
            let content = try translateUserContent(msg.content)
            return .object([
                "role": .string("user"),
                "content": content,
            ])
        case .assistant:
            var textParts: [String] = []
            var toolCalls: [JSONValue] = []

            for part in msg.content {
                switch part.kind.rawValue {
                case ContentKind.text.rawValue:
                    textParts.append(part.text ?? "")
                case ContentKind.toolCall.rawValue:
                    guard let call = part.toolCall else { continue }
                    let args: String = {
                        if let raw = call.rawArguments { return raw }
                        return _ProviderHTTP.stringifyJSON(.object(call.arguments))
                    }()
                    let callID = call.id.isEmpty ? "call_\(UUID().uuidString)" : call.id
                    toolCalls.append(
                        .object([
                            "id": .string(callID),
                            "type": .string("function"),
                            "function": .object([
                                "name": .string(call.name),
                                "arguments": .string(args),
                            ]),
                        ])
                    )
                case ContentKind.thinking.rawValue, ContentKind.redactedThinking.rawValue:
                    if let thinking = part.thinking?.text, !thinking.isEmpty {
                        textParts.append(thinking)
                    }
                default:
                    throw InvalidRequestError(
                        message: "Unsupported content kind for Cerebras assistant message: \(part.kind.rawValue)",
                        provider: name,
                        statusCode: nil,
                        errorCode: nil,
                        retryable: false
                    )
                }
            }

            var obj: [String: JSONValue] = ["role": .string("assistant")]
            let text = textParts.joined()
            if !text.isEmpty {
                obj["content"] = .string(text)
            } else if !toolCalls.isEmpty {
                obj["content"] = .null
            } else {
                obj["content"] = .string("")
            }
            if !toolCalls.isEmpty {
                obj["tool_calls"] = .array(toolCalls)
            }
            return .object(obj)
        case .tool:
            let toolCallID = msg.toolCallId ?? msg.content.first(where: { $0.kind.rawValue == ContentKind.toolResult.rawValue })?.toolResult?.toolCallId
            guard let toolCallID else {
                return nil
            }

            let content: String = {
                if let tr = msg.content.first(where: { $0.kind.rawValue == ContentKind.toolResult.rawValue })?.toolResult {
                    return _ProviderHTTP.stringifyJSON(tr.content)
                }
                return msg.text
            }()

            var obj: [String: JSONValue] = [
                "role": .string("tool"),
                "tool_call_id": .string(toolCallID),
                "content": .string(content),
            ]
            if let name = msg.name, !name.isEmpty {
                obj["name"] = .string(name)
            }
            return .object(obj)
        }
    }

    private func translateUserContent(_ parts: [ContentPart]) throws -> JSONValue {
        var textParts: [String] = []
        var structuredParts: [JSONValue] = []
        var hasNonTextPart = false

        for part in parts {
            switch part.kind.rawValue {
            case ContentKind.text.rawValue:
                let t = part.text ?? ""
                textParts.append(t)
                structuredParts.append(.object(["type": .string("text"), "text": .string(t)]))
            case ContentKind.image.rawValue:
                hasNonTextPart = true
                guard let image = part.image else { continue }
                structuredParts.append(try translateImagePart(image))
            default:
                throw InvalidRequestError(
                    message: "Unsupported content kind for Cerebras user message: \(part.kind.rawValue)",
                    provider: name,
                    statusCode: nil,
                    errorCode: nil,
                    retryable: false
                )
            }
        }

        if hasNonTextPart {
            return .array(structuredParts)
        }
        return .string(textParts.joined())
    }

    private func translateImagePart(_ image: ImageData) throws -> JSONValue {
        let imageURL: String
        if let url = image.url {
            if _ProviderHTTP.isProbablyLocalFilePath(url) {
                let bytes = try _ProviderHTTP.readLocalFileBytes(url)
                let mime = image.mediaType ?? _ProviderHTTP.mimeType(forPath: url) ?? "image/png"
                imageURL = "data:\(mime);base64,\(_ProviderHTTP.base64(bytes))"
            } else {
                imageURL = url
            }
        } else if let data = image.data {
            let mime = image.mediaType ?? "image/png"
            imageURL = "data:\(mime);base64,\(_ProviderHTTP.base64(data))"
        } else {
            throw InvalidRequestError(
                message: "Image content part must have url or data",
                provider: name,
                statusCode: nil,
                errorCode: nil,
                retryable: false
            )
        }

        var imageURLObject: [String: JSONValue] = ["url": .string(imageURL)]
        if let detail = image.detail, !detail.isEmpty {
            imageURLObject["detail"] = .string(detail)
        }

        return .object([
            "type": .string("image_url"),
            "image_url": .object(imageURLObject),
        ])
    }

    private func mapToolChoice(_ choice: ToolChoice) -> JSONValue {
        switch choice.mode {
        case .auto:
            return .string("auto")
        case .none:
            return .string("none")
        case .required:
            return .string("required")
        case .named:
            return .object([
                "type": .string("function"),
                "function": .object(["name": .string(choice.toolName ?? "")]),
            ])
        }
    }

    private func parseResponse(json: JSONValue, headers: HTTPHeaders, requestedModel: String) throws -> Response {
        let id = json["id"]?.stringValue ?? "chatcmpl_\(UUID().uuidString)"
        let model = json["model"]?.stringValue ?? requestedModel

        let choice = json["choices"]?.arrayValue?.first
        let message = choice?["message"]

        var parts: [ContentPart] = []
        if let content = parseMessageContent(message?["content"]), !content.isEmpty {
            parts.append(.text(content))
        }
        if let reasoning = message?["reasoning"]?.stringValue, !reasoning.isEmpty {
            parts.append(.thinking(ThinkingData(text: reasoning)))
        }

        var parsedToolCalls: [ToolCall] = []
        if let toolCalls = message?["tool_calls"]?.arrayValue {
            for rawCall in toolCalls {
                let call = parseToolCall(rawCall)
                parsedToolCalls.append(call)
                parts.append(.toolCall(call))
            }
        }

        let finishRaw = choice?["finish_reason"]?.stringValue
        let finish = FinishReason(
            kind: mapFinishReason(finishRaw, hasToolCalls: !parsedToolCalls.isEmpty),
            raw: finishRaw
        )
        let usage = parseUsage(json["usage"])

        return Response(
            id: id,
            model: model,
            provider: name,
            message: Message(role: .assistant, content: parts),
            finishReason: finish,
            usage: usage,
            raw: json,
            warnings: [],
            rateLimit: _ProviderHTTP.parseRateLimitInfo(headers)
        )
    }

    private func parseMessageContent(_ raw: JSONValue?) -> String? {
        guard let raw else { return nil }

        if let text = raw.stringValue {
            return text
        }

        guard let arr = raw.arrayValue else {
            return nil
        }

        var chunks: [String] = []
        for item in arr {
            if let text = item["text"]?.stringValue {
                chunks.append(text)
            }
        }
        return chunks.isEmpty ? nil : chunks.joined()
    }

    private func parseToolCall(_ raw: JSONValue) -> ToolCall {
        let callID = raw["id"]?.stringValue ?? "call_\(UUID().uuidString)"
        let function = raw["function"]
        let name = function?["name"]?.stringValue ?? ""

        let rawArguments: String? = {
            if let s = function?["arguments"]?.stringValue {
                return s
            }
            if let obj = function?["arguments"]?.objectValue {
                return _ProviderHTTP.stringifyJSON(.object(obj))
            }
            return nil
        }()

        let arguments: [String: JSONValue] = {
            if let obj = function?["arguments"]?.objectValue {
                return obj
            }
            guard let rawArguments,
                  let data = rawArguments.data(using: .utf8),
                  let parsed = try? JSONValue.parse(data),
                  let obj = parsed.objectValue
            else { return [:] }
            return obj
        }()

        return ToolCall(
            id: callID,
            name: name,
            arguments: arguments,
            rawArguments: rawArguments
        )
    }

    private func mapFinishReason(_ raw: String?, hasToolCalls: Bool) -> FinishReason.Kind {
        if hasToolCalls {
            return .toolCalls
        }
        switch raw {
        case "stop":
            return .stop
        case "length":
            return .length
        case "content_filter":
            return .contentFilter
        case nil:
            return .other
        default:
            return .other
        }
    }

    private func parseUsage(_ usage: JSONValue?) -> Usage {
        guard let usage, let obj = usage.objectValue else {
            return Usage(inputTokens: 0, outputTokens: 0, raw: usage)
        }

        func int(_ v: JSONValue?) -> Int? {
            if let n = v?.doubleValue { return Int(n) }
            return nil
        }

        let input = int(obj["prompt_tokens"]) ?? int(obj["input_tokens"]) ?? 0
        let output = int(obj["completion_tokens"]) ?? int(obj["output_tokens"]) ?? 0
        let reasoning = int(obj["completion_tokens_details"]?["reasoning_tokens"]) ?? int(obj["reasoning_tokens"])
        let cached = int(obj["prompt_tokens_details"]?["cached_tokens"]) ?? int(obj["cached_tokens"])

        return Usage(
            inputTokens: input,
            outputTokens: output,
            reasoningTokens: reasoning,
            cacheReadTokens: cached,
            cacheWriteTokens: nil,
            raw: usage
        )
    }

    private func fallbackStreamViaComplete(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        let response = try await complete(request: request)

        return AsyncThrowingStream { continuation in
            continuation.yield(StreamEvent(type: .standard(.streamStart)))

            if !response.text.isEmpty {
                let textId = "text_0"
                continuation.yield(StreamEvent(type: .standard(.textStart), textId: textId))
                continuation.yield(StreamEvent(type: .standard(.textDelta), delta: response.text, textId: textId))
                continuation.yield(StreamEvent(type: .standard(.textEnd), textId: textId))
            }

            if let reasoning = response.reasoning, !reasoning.isEmpty {
                continuation.yield(StreamEvent(type: .standard(.reasoningStart)))
                continuation.yield(StreamEvent(type: .standard(.reasoningDelta), reasoningDelta: reasoning))
                continuation.yield(StreamEvent(type: .standard(.reasoningEnd)))
            }

            for call in response.toolCalls {
                continuation.yield(StreamEvent(type: .standard(.toolCallStart), toolCall: ToolCall(id: call.id, name: call.name, arguments: [:], rawArguments: call.rawArguments)))
                continuation.yield(StreamEvent(type: .standard(.toolCallEnd), toolCall: call))
            }

            continuation.yield(
                StreamEvent(
                    type: .standard(.finish),
                    finishReason: response.finishReason,
                    usage: response.usage,
                    response: response
                )
            )
            continuation.finish()
        }
    }
}
