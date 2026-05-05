import Foundation

import OmniHTTP

public final class AnthropicAdapter: ProviderAdapter, @unchecked Sendable {
    public let name: String = "anthropic"

    private let apiKey: String
    private let baseURL: String
    private let transport: HTTPTransport
    private let apiVersion: String

    public init(apiKey: String, baseURL: String? = nil, transport: HTTPTransport, apiVersion: String = "2023-06-01") {
        self.apiKey = apiKey
        self.baseURL = baseURL ?? "https://api.anthropic.com"
        self.transport = transport
        self.apiVersion = apiVersion
    }

    public func complete(request: Request) async throws -> Response {
        try await request.abortSignal?.check()

        let url = try _ProviderHTTP.makeURL(baseURL: baseURL, path: "/v1/messages")
        let build = try buildRequest(request: request, stream: false)
        let timeout = request.timeout?.asConfig.total

        let http = try await transport.send(
            HTTPRequest(method: .post, url: url, headers: build.headers, body: .bytes(build.body)),
            timeout: timeout
        )

        if !(200..<300).contains(http.statusCode) {
            let json = _ProviderHTTP.parseJSONBody(http)
            let msg = _ProviderHTTP.errorMessage(from: json).message ?? "Anthropic error"
            let code = _ProviderHTTP.errorMessage(from: json).code
            let retryAfter = _ProviderHTTP.parseRetryAfterSeconds(http.headers)
            let err = _ErrorMapping.sdkErrorFromHTTP(
                provider: name,
                statusCode: http.statusCode,
                message: msg,
                errorCode: code,
                retryAfter: retryAfter,
                raw: json
            )
            throw err
        }

        let json = _ProviderHTTP.parseJSONBody(http)
        return try parseResponse(json: json, headers: http.headers, requestedModel: request.model)
    }

    public func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        try await request.abortSignal?.check()

        let url = try _ProviderHTTP.makeURL(baseURL: baseURL, path: "/v1/messages")
        let build = try buildRequest(request: request, stream: true)
        let timeout = request.timeout?.asConfig.total

        let res = try await transport.openStream(
            HTTPRequest(method: .post, url: url, headers: build.headers, body: .bytes(build.body)),
            timeout: timeout
        )

        if !(200..<300).contains(res.statusCode) {
            var bytes: [UInt8] = []
            for try await chunk in res.body {
                bytes.append(contentsOf: chunk)
                if bytes.count > 512 * 1024 { break }
            }
            let json = (try? JSONValue.parse(bytes)) ?? nil
            let msg = _ProviderHTTP.errorMessage(from: json).message ?? "Anthropic error"
            let code = _ProviderHTTP.errorMessage(from: json).code
            let retryAfter = _ProviderHTTP.parseRetryAfterSeconds(res.headers)
            let err = _ErrorMapping.sdkErrorFromHTTP(
                provider: name,
                statusCode: res.statusCode,
                message: msg,
                errorCode: code,
                retryAfter: retryAfter,
                raw: json
            )
            throw err
        }

        let sse = SSE.parse(res.body)

        return AsyncThrowingStream { continuation in
            Task {
                continuation.yield(StreamEvent(type: .standard(.streamStart)))

                enum BlockState {
                    case text(id: String, text: String)
                    case thinking(text: String, signature: String?)
                    case redactedThinking(data: String)
                    case toolUse(id: String, name: String, rawJSON: String)
                }

                var blocks: [Int: BlockState] = [:]
                var accumulatedParts: [ContentPart] = []

                var responseId: String = "msg_\(UUID().uuidString)"
                var modelUsed: String = request.model

                var latestUsage: JSONValue? = nil
                var stopReasonRaw: String? = nil

                do {
                    for try await ev in sse {
                        if Task.isCancelled { break }

                        let payload: JSONValue? = {
                            guard let data = ev.data.data(using: .utf8) else { return nil }
                            return try? JSONValue.parse(data)
                        }()
                        let type = ev.event ?? payload?["type"]?.stringValue ?? ""

                        switch type {
                        case "message_start":
                            if let msg = payload?["message"] {
                                responseId = msg["id"]?.stringValue ?? responseId
                                modelUsed = msg["model"]?.stringValue ?? modelUsed
                                latestUsage = msg["usage"] ?? latestUsage
                            }
                        case "content_block_start":
                            let block = payload?["content_block"]
                            let index = Int(payload?["index"]?.doubleValue ?? 0)
                            let bType = block?["type"]?.stringValue ?? ""
                            switch bType {
                            case "text":
                                let textId = "text_\(index)"
                                blocks[index] = .text(id: textId, text: "")
                                continuation.yield(StreamEvent(type: .standard(.textStart), textId: textId))
                            case "thinking":
                                let sig = block?["signature"]?.stringValue
                                blocks[index] = .thinking(text: "", signature: sig)
                                continuation.yield(StreamEvent(type: .standard(.reasoningStart)))
                            case "redacted_thinking":
                                let data = block?["data"]?.stringValue ?? ""
                                blocks[index] = .redactedThinking(data: data)
                                continuation.yield(StreamEvent(type: .standard(.reasoningStart)))
                                if !data.isEmpty {
                                    continuation.yield(StreamEvent(type: .standard(.reasoningDelta), reasoningDelta: data))
                                }
                            case "tool_use":
                                let id = block?["id"]?.stringValue ?? "tool_\(UUID().uuidString)"
                                let name = block?["name"]?.stringValue ?? ""
                                blocks[index] = .toolUse(id: id, name: name, rawJSON: "")
                                continuation.yield(StreamEvent(type: .standard(.toolCallStart), toolCall: ToolCall(id: id, name: name, arguments: [:], rawArguments: "")))
                            default:
                                break
                            }
                        case "content_block_delta":
                            let index = Int(payload?["index"]?.doubleValue ?? 0)
                            let delta = payload?["delta"]
                            let dType = delta?["type"]?.stringValue ?? ""
                            switch dType {
                            case "text_delta":
                                let text = delta?["text"]?.stringValue ?? ""
                                if case .text(let textId, let current)? = blocks[index] {
                                    let next = current + text
                                    blocks[index] = .text(id: textId, text: next)
                                    continuation.yield(StreamEvent(type: .standard(.textDelta), delta: text, textId: textId))
                                } else {
                                    let textId = "text_\(index)"
                                    blocks[index] = .text(id: textId, text: text)
                                    continuation.yield(StreamEvent(type: .standard(.textStart), textId: textId))
                                    continuation.yield(StreamEvent(type: .standard(.textDelta), delta: text, textId: textId))
                                }
                            case "thinking_delta":
                                let t = delta?["thinking"]?.stringValue ?? ""
                                if case .thinking(let current, let sig)? = blocks[index] {
                                    blocks[index] = .thinking(text: current + t, signature: sig)
                                }
                                continuation.yield(StreamEvent(type: .standard(.reasoningDelta), reasoningDelta: t))
                            case "input_json_delta":
                                // Tool input JSON is streamed as partial JSON strings.
                                let partial = delta?["partial_json"]?.stringValue ?? ""
                                if case .toolUse(let id, let name, let raw)? = blocks[index] {
                                    let next = raw + partial
                                    blocks[index] = .toolUse(id: id, name: name, rawJSON: next)
                                    continuation.yield(StreamEvent(type: .standard(.toolCallDelta), toolCall: ToolCall(id: id, name: name, arguments: [:], rawArguments: next)))
                                }
                            case "signature_delta":
                                let sig = delta?["signature"]?.stringValue ?? ""
                                if case .thinking(let text, _)? = blocks[index] {
                                    blocks[index] = .thinking(text: text, signature: sig)
                                }
                            case "redacted_thinking_delta":
                                let data = delta?["data"]?.stringValue ?? ""
                                if case .redactedThinking(let current)? = blocks[index] {
                                    blocks[index] = .redactedThinking(data: current + data)
                                } else {
                                    blocks[index] = .redactedThinking(data: data)
                                }
                                continuation.yield(StreamEvent(type: .standard(.reasoningDelta), reasoningDelta: data))
                            default:
                                break
                            }
                        case "content_block_stop":
                            let index = Int(payload?["index"]?.doubleValue ?? 0)
                            if let state = blocks[index] {
                                switch state {
                                case .text(let textId, let text):
                                    if !text.isEmpty { accumulatedParts.append(.text(text)) }
                                    continuation.yield(StreamEvent(type: .standard(.textEnd), textId: textId))
                                case .thinking(let text, let sig):
                                    accumulatedParts.append(.thinking(ThinkingData(text: text, signature: sig, redacted: false)))
                                    continuation.yield(StreamEvent(type: .standard(.reasoningEnd)))
                                case .redactedThinking(let data):
                                    accumulatedParts.append(.thinking(ThinkingData(text: data, signature: nil, redacted: true)))
                                    continuation.yield(StreamEvent(type: .standard(.reasoningEnd)))
                                case .toolUse(let id, let name, let raw):
                                    let args: [String: JSONValue] = {
                                        guard let data = raw.data(using: .utf8),
                                              let parsed = try? JSONValue.parse(data),
                                              let obj = parsed.objectValue
                                        else { return [:] }
                                        return obj
                                    }()
                                    let call = ToolCall(id: id, name: name, arguments: args, rawArguments: raw)
                                    accumulatedParts.append(.toolCall(call))
                                    continuation.yield(StreamEvent(type: .standard(.toolCallEnd), toolCall: call))
                                }
                                blocks[index] = nil
                            }
                        case "message_delta":
                            stopReasonRaw = payload?["delta"]?["stop_reason"]?.stringValue ?? stopReasonRaw
                            let usage = payload?["usage"]
                            if let usage { latestUsage = usage }
                        case "message_stop":
                            let finish = FinishReason(kind: mapFinishReason(stopReasonRaw), raw: stopReasonRaw)
                            let usage = parseUsage(latestUsage)
                            let msg = Message(role: .assistant, content: accumulatedParts)
                            let response = Response(
                                id: responseId,
                                model: modelUsed,
                                provider: name,
                                message: msg,
                                finishReason: finish,
                                usage: usage,
                                raw: payload,
                                warnings: [],
                                rateLimit: _ProviderHTTP.parseRateLimitInfo(res.headers)
                            )
                            continuation.yield(StreamEvent(type: .standard(.finish), finishReason: finish, usage: usage, response: response, raw: payload))
                            continuation.finish()
                            return
                        default:
                            continuation.yield(StreamEvent(type: .standard(.providerEvent), raw: payload))
                        }
                    }

                    // Stream ended unexpectedly.
                    let finish = FinishReason(kind: .other, raw: stopReasonRaw)
                    let usage = parseUsage(latestUsage)
                    let msg = Message(role: .assistant, content: accumulatedParts)
                    let response = Response(
                        id: "stream_ended",
                        model: modelUsed,
                        provider: name,
                        message: msg,
                        finishReason: finish,
                        usage: usage,
                        raw: nil,
                        warnings: [Warning(message: "Anthropic stream ended without message_stop")],
                        rateLimit: _ProviderHTTP.parseRateLimitInfo(res.headers)
                    )
                    continuation.yield(StreamEvent(type: .standard(.finish), finishReason: finish, usage: usage, response: response))
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

    private func buildRequest(request: Request, stream: Bool) throws -> (headers: HTTPHeaders, body: [UInt8]) {
        let opts = request.optionsObject(for: name)
        let autoCache = opts["auto_cache"]?.boolValue ?? true

        // Beta headers can be passed via provider_options.anthropic.beta_headers (or beta_features).
        let betaHeaders: [String] = {
            let arr = (opts["beta_headers"] ?? opts["beta_features"])?.arrayValue?.compactMap { $0.stringValue } ?? []
            return arr
        }()

        var headers = HTTPHeaders()
        headers.set(name: "x-api-key", value: apiKey)
        headers.set(name: "anthropic-version", value: opts["version"]?.stringValue ?? apiVersion)
        headers.set(name: "content-type", value: "application/json")
        headers.set(name: "accept", value: stream ? "text/event-stream" : "application/json")
        let appVersion = ProcessInfo.processInfo.environment["OMNIKIT_VERSION"] ?? "swift-omnikit-dev"
        let entrypoint = ProcessInfo.processInfo.environment["CLAUDE_CODE_ENTRYPOINT"] ?? "omnikit"
        headers.set(name: "x-anthropic-billing-header", value: "cc_version=\(appVersion); cc_entrypoint=\(entrypoint); cch=00000;")

        var beta = betaHeaders
        // Auto-inject 1M context beta for models that support it.
        let modelLower = request.model.lowercased()
        let supports1M = modelLower.contains("[1m]")
            || modelLower.contains("opus-4")
            || modelLower.contains("sonnet-4")
        if supports1M, !beta.contains("context-1m-2025-08-07") {
            beta.append("context-1m-2025-08-07")
        }
        if opts["dangerous_direct_browser_access"]?.boolValue == true {
            headers.set(name: "anthropic-dangerous-direct-browser-access", value: "true")
        }

        // System prompt: merge SYSTEM + DEVELOPER.
        var systemTextParts: [String] = []
        var translated: [(role: String, content: [JSONValue])] = []

        func translateContentPart(_ part: ContentPart) throws -> JSONValue? {
            switch part.kind.rawValue {
            case ContentKind.text.rawValue:
                return .object(["type": .string("text"), "text": .string(part.text ?? "")])
            case ContentKind.image.rawValue:
                guard let image = part.image else { return nil }
                if let url = image.url {
                    if _ProviderHTTP.isProbablyLocalFilePath(url) {
                        let bytes = try _ProviderHTTP.readLocalFileBytes(url)
                        let mediaType = image.mediaType ?? _ProviderHTTP.mimeType(forPath: url) ?? "image/png"
                        return .object([
                            "type": .string("image"),
                            "source": .object([
                                "type": .string("base64"),
                                "media_type": .string(mediaType),
                                "data": .string(_ProviderHTTP.base64(bytes)),
                            ]),
                        ])
                    }
                    return .object([
                        "type": .string("image"),
                        "source": .object(["type": .string("url"), "url": .string(url)]),
                    ])
                }
                if let data = image.data {
                    let mediaType = image.mediaType ?? "image/png"
                    return .object([
                        "type": .string("image"),
                        "source": .object([
                            "type": .string("base64"),
                            "media_type": .string(mediaType),
                            "data": .string(_ProviderHTTP.base64(data)),
                        ]),
                    ])
                }
                return nil
            case ContentKind.toolCall.rawValue:
                guard let call = part.toolCall else { return nil }
                return .object([
                    "type": .string("tool_use"),
                    "id": .string(call.id),
                    "name": .string(call.name),
                    "input": .object(call.arguments),
                ])
            case ContentKind.toolResult.rawValue:
                guard let tr = part.toolResult else { return nil }
                // Anthropic accepts content as a plain string or an array of content blocks.
                // Preserve the original type to avoid double-encoding strings.
                let content: JSONValue
                switch tr.content {
                case .string:
                    content = tr.content
                case .array:
                    content = tr.content
                default:
                    content = .string(_ProviderHTTP.stringifyJSON(tr.content))
                }
                return .object([
                    "type": .string("tool_result"),
                    "tool_use_id": .string(tr.toolCallId),
                    "content": content,
                    "is_error": .bool(tr.isError),
                ])
            case ContentKind.thinking.rawValue:
                guard let t = part.thinking else { return nil }
                var obj: [String: JSONValue] = [
                    "type": .string("thinking"),
                    "thinking": .string(t.text),
                ]
                if let sig = t.signature {
                    obj["signature"] = .string(sig)
                }
                return .object(obj)
            case ContentKind.redactedThinking.rawValue:
                guard let t = part.thinking else { return nil }
                // Redacted thinking is opaque data.
                return .object([
                    "type": .string("redacted_thinking"),
                    "data": .string(t.text),
                ])
            default:
                throw InvalidRequestError(message: "Unsupported content kind for Anthropic: \(part.kind.rawValue)", provider: name, statusCode: nil, errorCode: nil, retryable: false)
            }
        }

        for msg in request.messages {
            switch msg.role {
            case .system, .developer:
                let t = msg.text
                if !t.isEmpty { systemTextParts.append(t) }
            case .tool:
                // Tool results must appear as tool_result blocks within a user message.
                let blocks = try msg.content.compactMap(translateContentPart)
                if !blocks.isEmpty {
                    translated.append((role: "user", content: blocks))
                }
            case .user, .assistant:
                let role = (msg.role == .assistant) ? "assistant" : "user"
                let blocks = try msg.content.compactMap(translateContentPart)
                if !blocks.isEmpty {
                    translated.append((role: role, content: blocks))
                }
            }
        }

        // Strict alternation: merge consecutive messages of the same role by concatenating content blocks.
        var merged: [(role: String, content: [JSONValue])] = []
        for m in translated {
            if let last = merged.last, last.role == m.role {
                merged[merged.count - 1].content.append(contentsOf: m.content)
            } else {
                merged.append(m)
            }
        }

        // Prompt caching: inject a cache_control breakpoint into the conversation prefix (first user message).
        if autoCache {
            if let idx = merged.firstIndex(where: { $0.role == "user" }), !merged[idx].content.isEmpty {
                if var firstBlock = merged[idx].content[0].objectValue {
                    firstBlock["cache_control"] = .object(["type": .string("ephemeral")])
                    merged[idx].content[0] = .object(firstBlock)
                    if !beta.contains("prompt-caching-2024-07-31") {
                        beta.append("prompt-caching-2024-07-31")
                    }
                }
            }
        }

        let requestModel = canonicalAnthropicModelID(request.model)

        // Anthropic requires max_tokens. Use the request value if set, otherwise
        // default to the model's maximum output capacity, clamped to not exceed
        // the remaining context window (context_window - estimated_input_tokens).
        let effectiveMaxTokens: Int = {
            let m = requestModel.lowercased()

            let maxOutput: Int
            if m.contains("opus-4") { maxOutput = 128_000 }
            else if m.contains("sonnet-4") || m.contains("haiku-4") { maxOutput = 64_000 }
            else { maxOutput = 8192 }

            let contextWindow: Int
            if supports1M {
                contextWindow = 1_000_000
            } else if m.contains("opus-4") || m.contains("sonnet-4") || m.contains("haiku-4") {
                contextWindow = 200_000
            } else {
                contextWindow = 200_000
            }

            // Estimate input tokens from message content (~4 chars per token).
            let inputChars = merged.reduce(0) { total, msg in
                total + msg.content.reduce(0) { sum, part in
                    switch part {
                    case .string(let s): return sum + s.count
                    case .object(let o):
                        return sum + (o.values.reduce(0) { $0 + "\($1)".count })
                    default: return sum + 100
                    }
                }
            } + systemTextParts.joined().count
            let estimatedInputTokens = max(inputChars / 4, 1000)

            let remaining = contextWindow - estimatedInputTokens
            let desired = request.maxTokens ?? maxOutput
            return max(1024, min(desired, remaining))
        }()

        var root: [String: JSONValue] = [
            "model": .string(requestModel),
            "messages": .array(merged.map { .object(["role": .string($0.role), "content": .array($0.content)]) }),
            "max_tokens": .number(Double(effectiveMaxTokens)),
        ]

        if stream { root["stream"] = .bool(true) }

        if !systemTextParts.isEmpty {
            var systemBlock: [String: JSONValue] = ["type": .string("text"), "text": .string(systemTextParts.joined(separator: "\n"))]
            if autoCache {
                systemBlock["cache_control"] = .object(["type": .string("ephemeral")])
                if !beta.contains("prompt-caching-2024-07-31") {
                    beta.append("prompt-caching-2024-07-31")
                }
            }
            root["system"] = .array([.object(systemBlock)])
        }

        if let temperature = request.temperature { root["temperature"] = .number(temperature) }
        if let topP = request.topP { root["top_p"] = .number(topP) }
        if let stop = request.stopSequences, !stop.isEmpty {
            root["stop_sequences"] = .array(stop.map { .string($0) })
        }

        // Tools and tool choice
        if let tools = request.tools, !tools.isEmpty {
            if request.toolChoice?.mode == ToolChoiceMode.none {
                // Anthropic requires omitting tools entirely for "none".
            } else {
                root["tools"] = .array(tools.enumerated().map { (index, tool) in
                    var obj: [String: JSONValue] = [
                        "name": .string(tool.name),
                        "description": .string(tool.description),
                        "input_schema": tool.parameters,
                    ]
                    // Only add cache_control to the last tool to stay within
                    // Anthropic's 4 cache breakpoint limit (system + first user
                    // message + last tool = 3, leaving room for 1 more).
                    if autoCache && index == tools.count - 1 {
                        obj["cache_control"] = .object(["type": .string("ephemeral")])
                        if !beta.contains("prompt-caching-2024-07-31") {
                            beta.append("prompt-caching-2024-07-31")
                        }
                    }
                    return .object(obj)
                })

                if let choice = request.toolChoice {
                    root["tool_choice"] = mapToolChoice(choice)
                } else {
                    root["tool_choice"] = .object(["type": .string("auto")])
                }
            }
        }

        // Provider options escape hatch: merge fields (except headers-related keys).
        for (k, v) in opts {
            if k == "beta_headers" || k == "beta_features" || k == "version" || k == "auto_cache" { continue }
            root[k] = v
        }

        if !beta.isEmpty {
            headers.set(name: "anthropic-beta", value: beta.joined(separator: ","))
        }

        return (headers, try _ProviderHTTP.jsonBytes(.object(root)))
    }

    private func canonicalAnthropicModelID(_ model: String) -> String {
        // OmniKit uses "[1m]" as a local suffix to request the 1M context beta.
        // Anthropic expects the base model id in the payload and the beta in headers.
        let pattern = "\\s*\\[1m\\]\\s*"
        guard let regex = try? NSRegularExpression(pattern: pattern) else {
            return model.replacing("[1m]", with: "").trimmingCharacters(in: .whitespacesAndNewlines)
        }
        let range = NSRange(location: 0, length: model.utf16.count)
        let stripped = regex.stringByReplacingMatches(in: model, options: [], range: range, withTemplate: "")
        return stripped.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func mapToolChoice(_ choice: ToolChoice) -> JSONValue {
        switch choice.mode {
        case .auto:
            return .object(["type": .string("auto")])
        case .none:
            // handled by omitting tools
            return .object(["type": .string("auto")])
        case .required:
            return .object(["type": .string("any")])
        case .named:
            return .object(["type": .string("tool"), "name": .string(choice.toolName ?? "")])
        }
    }

    private func parseResponse(json: JSONValue, headers: HTTPHeaders, requestedModel: String) throws -> Response {
        let id = json["id"]?.stringValue ?? "msg_\(UUID().uuidString)"
        let model = json["model"]?.stringValue ?? requestedModel

        let content = json["content"]?.arrayValue ?? []
        var parts: [ContentPart] = []

        for block in content {
            let t = block["type"]?.stringValue ?? ""
            switch t {
            case "text":
                let text = block["text"]?.stringValue ?? ""
                if !text.isEmpty { parts.append(.text(text)) }
            case "tool_use":
                let toolId = block["id"]?.stringValue ?? UUID().uuidString
                let name = block["name"]?.stringValue ?? ""
                let input = block["input"]?.objectValue ?? [:]
                let call = ToolCall(id: toolId, name: name, arguments: input, rawArguments: nil)
                parts.append(.toolCall(call))
            case "thinking":
                let thinking = block["thinking"]?.stringValue ?? ""
                let sig = block["signature"]?.stringValue
                parts.append(.thinking(ThinkingData(text: thinking, signature: sig, redacted: false)))
            case "redacted_thinking":
                let data = block["data"]?.stringValue ?? ""
                parts.append(.thinking(ThinkingData(text: data, signature: nil, redacted: true)))
            default:
                break
            }
        }

        let stopRaw = json["stop_reason"]?.stringValue
        let finish = FinishReason(kind: mapFinishReason(stopRaw), raw: stopRaw)
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

    private func mapFinishReason(_ raw: String?) -> FinishReason.Kind {
        switch raw {
        case "end_turn", "stop_sequence", "stop":
            return .stop
        case "max_tokens":
            return .length
        case "tool_use":
            return .toolCalls
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

        let input = int(obj["input_tokens"]) ?? 0
        let output = int(obj["output_tokens"]) ?? 0
        let cacheRead = int(obj["cache_read_input_tokens"])
        let cacheWrite = int(obj["cache_creation_input_tokens"])

        return Usage(
            inputTokens: input,
            outputTokens: output,
            reasoningTokens: nil,
            cacheReadTokens: cacheRead,
            cacheWriteTokens: cacheWrite,
            raw: usage
        )
    }
}
