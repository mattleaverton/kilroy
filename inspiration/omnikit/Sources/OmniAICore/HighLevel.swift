import Foundation

public typealias StopCondition = @Sendable (_ steps: [StepResult]) -> Bool

public struct StepResult: Sendable, Equatable {
    public var text: String
    public var reasoning: String?
    public var toolCalls: [ToolCall]
    public var toolResults: [ToolResult]
    public var finishReason: FinishReason
    public var usage: Usage
    public var response: Response
    public var warnings: [Warning]

    public init(
        text: String,
        reasoning: String?,
        toolCalls: [ToolCall],
        toolResults: [ToolResult],
        finishReason: FinishReason,
        usage: Usage,
        response: Response,
        warnings: [Warning]
    ) {
        self.text = text
        self.reasoning = reasoning
        self.toolCalls = toolCalls
        self.toolResults = toolResults
        self.finishReason = finishReason
        self.usage = usage
        self.response = response
        self.warnings = warnings
    }
}

public struct GenerateResult: Sendable, Equatable {
    public var text: String
    public var reasoning: String?
    public var toolCalls: [ToolCall]
    public var toolResults: [ToolResult]
    public var finishReason: FinishReason
    public var usage: Usage
    public var totalUsage: Usage
    public var steps: [StepResult]
    public var response: Response
    public var output: JSONValue?

    public init(
        text: String,
        reasoning: String?,
        toolCalls: [ToolCall],
        toolResults: [ToolResult],
        finishReason: FinishReason,
        usage: Usage,
        totalUsage: Usage,
        steps: [StepResult],
        response: Response,
        output: JSONValue? = nil
    ) {
        self.text = text
        self.reasoning = reasoning
        self.toolCalls = toolCalls
        self.toolResults = toolResults
        self.finishReason = finishReason
        self.usage = usage
        self.totalUsage = totalUsage
        self.steps = steps
        self.response = response
        self.output = output
    }
}

private struct ToolExecutionOutcome: Sendable {
    var toolResults: [ToolResult]
    var repairMessages: [Message]
}

private struct SingleToolExecutionOutcome: Sendable {
    var index: Int
    var result: ToolResult
    var repairMessage: Message?
}

private func _standardizeMessages(prompt: String?, messages: [Message]?, system: String?) throws -> [Message] {
    if prompt != nil, messages != nil {
        throw ConfigurationError(message: "Provide either prompt or messages, not both.")
    }
    var out: [Message] = []
    if let system, !system.isEmpty {
        out.append(.system(system))
    }
    if let prompt {
        out.append(.user(prompt))
    } else if let messages {
        out.append(contentsOf: messages)
    } else {
        throw ConfigurationError(message: "Either prompt or messages is required.")
    }
    return out
}

private func _makeToolResultMessages(
    _ results: [ToolResult],
    toolCalls: [ToolCall]
) -> [Message] {
    var byId: [String: String] = [:]
    for call in toolCalls {
        byId[call.id] = call.name
    }
    return results.map { r in
        Message.toolResult(toolCallId: r.toolCallId, toolName: byId[r.toolCallId], content: r.content, isError: r.isError)
    }
}

private func _makeToolRepairMessage(toolCall: ToolCall, validationError: String) -> Message {
    let renderedArguments = JSONValue.object(toolCall.arguments).description
    return .developer(
        """
        The previous response attempted to call the tool \(toolCall.name) with arguments that do not match its JSON schema.
        Call ID: \(toolCall.id)
        Arguments: \(renderedArguments)
        Validation error: \(validationError)

        Do not repeat the same invalid call. Either emit a corrected call to \(toolCall.name) that matches the schema exactly, or continue without using that tool if it is unnecessary.
        """
    )
}

private func _executeToolCalls(
    tools: [Tool],
    toolCalls: [ToolCall],
    messages: [Message],
    abortSignal: AbortSignal?
) async throws -> ToolExecutionOutcome {
    let toolsByName = Dictionary(uniqueKeysWithValues: tools.map { ($0.name, $0) })

    // If any tool call targets a passive tool (no execute), do not auto-execute.
    for call in toolCalls {
        if let t = toolsByName[call.name], t.execute == nil {
            return ToolExecutionOutcome(toolResults: [], repairMessages: [])
        }
    }

    return try await withThrowingTaskGroup(of: SingleToolExecutionOutcome.self) { group in
        for (idx, call) in toolCalls.enumerated() {
            group.addTask {
                try await abortSignal?.check()

                guard let tool = toolsByName[call.name] else {
                    return SingleToolExecutionOutcome(
                        index: idx,
                        result: ToolResult(toolCallId: call.id, content: .string("Unknown tool: \(call.name)"), isError: true),
                        repairMessage: nil
                    )
                }
                guard let execute = tool.execute else {
                    // Passive tool: handled above by returning [] (no loop). Treat as unknown here defensively.
                    return SingleToolExecutionOutcome(
                        index: idx,
                        result: ToolResult(toolCallId: call.id, content: .string("Tool has no execute handler: \(call.name)"), isError: true),
                        repairMessage: nil
                    )
                }

                // Validate args against tool schema (best-effort).
                do {
                    try JSONSchema(tool.parameters).validate(.object(call.arguments))
                } catch {
                    let validationError = "Invalid tool arguments for \(call.name): \(error)"
                    return SingleToolExecutionOutcome(
                        index: idx,
                        result: ToolResult(toolCallId: call.id, content: .string(validationError), isError: true),
                        repairMessage: _makeToolRepairMessage(toolCall: call, validationError: validationError)
                    )
                }

                do {
                    let ctx = ToolExecutionContext(messages: messages, abortSignal: abortSignal, toolCallId: call.id)
                    let out = try await execute(call.arguments, ctx)
                    return SingleToolExecutionOutcome(
                        index: idx,
                        result: ToolResult(toolCallId: call.id, content: out, isError: false),
                        repairMessage: nil
                    )
                } catch {
                    return SingleToolExecutionOutcome(
                        index: idx,
                        result: ToolResult(toolCallId: call.id, content: .string(String(describing: error)), isError: true),
                        repairMessage: nil
                    )
                }
            }
        }

        var results: [SingleToolExecutionOutcome?] = Array(repeating: nil, count: toolCalls.count)
        for try await outcome in group {
            results[outcome.index] = outcome
        }
        let ordered = results.compactMap { $0 }
        return ToolExecutionOutcome(
            toolResults: ordered.map(\.result),
            repairMessages: ordered.compactMap(\.repairMessage)
        )
    }
}

public func generate(
    model: String,
    prompt: String? = nil,
    messages: [Message]? = nil,
    system: String? = nil,
    tools: [Tool]? = nil,
    toolChoice: ToolChoice? = nil,
    maxToolRounds: Int = 1,
    stopWhen: StopCondition? = nil,
    responseFormat: ResponseFormat? = nil,
    temperature: Double? = nil,
    topP: Double? = nil,
    maxTokens: Int? = nil,
    stopSequences: [String]? = nil,
    reasoningEffort: String? = nil,
    metadata: [String: String]? = nil,
    provider: String? = nil,
    providerOptions: [String: JSONValue]? = nil,
    maxRetries: Int = 2,
    retryPolicy: RetryPolicy? = nil,
    timeout: Timeout? = nil,
    abortSignal: AbortSignal? = nil,
    client: Client? = nil
) async throws -> GenerateResult {
    let resolvedClient: Client
    if let client {
        resolvedClient = client
    } else {
        resolvedClient = try await defaultClient()
    }

    let timeoutConfig = timeout?.asConfig ?? TimeoutConfig()
    let totalTimeout = timeoutConfig.total
    let perStepTimeout = timeoutConfig.perStep

    let policy: RetryPolicy = {
        var p = retryPolicy ?? RetryPolicy(maxRetries: maxRetries)
        p.maxRetries = maxRetries
        return p
    }()

    let initialMessages = try _standardizeMessages(prompt: prompt, messages: messages, system: system)
    let maxRounds = max(0, maxToolRounds)

    let operation: @Sendable () async throws -> GenerateResult = {
        var conversation = initialMessages
        var pendingContinuation: ToolContinuationRequest?
        var steps: [StepResult] = []
        var totalUsage = Usage(inputTokens: 0, outputTokens: 0)

        func effectiveToolChoice(stepIndex: Int) -> ToolChoice? {
            guard let toolChoice else { return nil }
            // If the caller forced a tool call (named/required), apply that only to the first step.
            // On continuation steps, let the model decide whether to call additional tools or answer.
            if stepIndex > 0, toolChoice.mode == .named || toolChoice.mode == .required {
                return nil
            }
            return toolChoice
        }

        func callLLM(_ conversationMessages: [Message], stepIndex: Int) async throws -> Response {
            let req = Request(
                model: model,
                messages: conversationMessages,
                provider: provider,
                tools: tools,
                toolChoice: effectiveToolChoice(stepIndex: stepIndex),
                responseFormat: responseFormat,
                temperature: temperature,
                topP: topP,
                maxTokens: maxTokens,
                stopSequences: stopSequences,
                reasoningEffort: reasoningEffort,
                metadata: metadata,
                providerOptions: providerOptions,
                timeout: perStepTimeout.map { Timeout.config(TimeoutConfig(total: $0)) },
                abortSignal: abortSignal
            )

            return try await retry(policy: policy, abortSignal: abortSignal) {
                try await _withOptionalTimeout(perStepTimeout) {
                    try await resolvedClient.complete(req)
                }
            }
        }

        func callToolContinuation(_ request: ToolContinuationRequest) async throws -> Response {
            return try await retry(policy: policy, abortSignal: abortSignal) {
                try await _withOptionalTimeout(perStepTimeout) {
                    try await resolvedClient.sendToolOutputs(request)
                }
            }
        }

        for stepIndex in 0..<(maxRounds + 1) {
            try await abortSignal?.check()

            let response: Response
            if let continuation = pendingContinuation {
                pendingContinuation = nil
                response = try await callToolContinuation(continuation)
            } else {
                response = try await callLLM(conversation, stepIndex: stepIndex)
            }
            totalUsage = totalUsage + response.usage

            let toolCalls = response.toolCalls
            let shouldToolLoop =
                (maxRounds > 0)
                && (stepIndex < maxRounds)
                && !(toolCalls.isEmpty)
                && response.finishReason.kind == .toolCalls
                && (tools?.contains(where: { $0.execute != nil }) ?? false)

            let toolExecution: ToolExecutionOutcome
            if shouldToolLoop, let tools {
                toolExecution = try await _executeToolCalls(tools: tools, toolCalls: toolCalls, messages: conversation + [response.message], abortSignal: abortSignal)
            } else {
                toolExecution = ToolExecutionOutcome(toolResults: [], repairMessages: [])
            }
            let toolResults = toolExecution.toolResults

            steps.append(
                StepResult(
                    text: response.text,
                    reasoning: response.reasoning,
                    toolCalls: toolCalls,
                    toolResults: toolResults,
                    finishReason: response.finishReason,
                    usage: response.usage,
                    response: response,
                    warnings: response.warnings
                )
            )

            let stopNow =
                toolCalls.isEmpty
                || response.finishReason.kind != .toolCalls
                || maxRounds <= 0
                || stepIndex >= maxRounds
                || (stopWhen?(steps) ?? false)
                || (!toolCalls.isEmpty && toolResults.isEmpty)

            if stopNow {
                return GenerateResult(
                    text: response.text,
                    reasoning: response.reasoning,
                    toolCalls: toolCalls,
                    toolResults: toolResults,
                    finishReason: response.finishReason,
                    usage: response.usage,
                    totalUsage: totalUsage,
                    steps: steps,
                    response: response,
                    output: nil
                )
            }

            // Continue with tool results.
            let baseConversation = conversation + [response.message]
            let toolResultMessages = makeToolResultMessages(toolResults: toolResults, toolCalls: toolCalls)
            let nextConversation = baseConversation + toolResultMessages + toolExecution.repairMessages
            conversation = nextConversation

            let supportsContinuation: Bool = {
                guard let adapter = try? resolvedClient.resolveAdapter(provider: provider) else { return false }
                return adapter is ToolContinuationProviderAdapter
            }()

            if supportsContinuation {
                pendingContinuation = ToolContinuationRequest(
                    model: model,
                    provider: provider,
                    previousResponseId: response.id,
                    messages: baseConversation,
                    toolCalls: toolCalls,
                    toolResults: toolResults,
                    additionalMessages: toolExecution.repairMessages,
                    tools: tools,
                    toolChoice: effectiveToolChoice(stepIndex: stepIndex + 1),
                    responseFormat: responseFormat,
                    temperature: temperature,
                    topP: topP,
                    maxTokens: maxTokens,
                    stopSequences: stopSequences,
                    reasoningEffort: reasoningEffort,
                    metadata: metadata,
                    providerOptions: providerOptions,
                    timeout: perStepTimeout.map { Timeout.config(TimeoutConfig(total: $0)) },
                    abortSignal: abortSignal
                )
            }
        }

        guard let last = steps.last else {
            throw SDKError(message: "Generate operation completed without producing any steps.")
        }
        return GenerateResult(
            text: last.text,
            reasoning: last.reasoning,
            toolCalls: last.toolCalls,
            toolResults: last.toolResults,
            finishReason: last.finishReason,
            usage: last.usage,
            totalUsage: totalUsage,
            steps: steps,
            response: last.response,
            output: nil
        )
    }

    if let totalTimeout {
        return try await _withTimeout(totalTimeout, operation: operation)
    }
    return try await operation()
}

public func generateObject(
    model: String,
    prompt: String? = nil,
    messages: [Message]? = nil,
    system: String? = nil,
    schema: JSONValue,
    temperature: Double? = nil,
    topP: Double? = nil,
    maxTokens: Int? = nil,
    stopSequences: [String]? = nil,
    reasoningEffort: String? = nil,
    metadata: [String: String]? = nil,
    provider: String? = nil,
    providerOptions: [String: JSONValue]? = nil,
    maxRetries: Int = 2,
    retryPolicy: RetryPolicy? = nil,
    timeout: Timeout? = nil,
    abortSignal: AbortSignal? = nil,
    client: Client? = nil
) async throws -> GenerateResult {
    let resolvedClient: Client
    if let client {
        resolvedClient = client
    } else {
        resolvedClient = try await defaultClient()
    }

    let providerName: String = {
        if let provider { return provider }
        if let def = resolvedClient.defaultProvider { return def }
        return "unknown"
    }()

    let outputToolName = "json_output"

    let tools: [Tool]?
    let toolChoice: ToolChoice?
    let responseFormat: ResponseFormat?
    if providerName == "anthropic" {
        // Fallback: force a tool call whose input schema matches the desired output.
        tools = [try Tool(name: outputToolName, description: "Return the output object matching the schema.", parameters: schema)]
        toolChoice = ToolChoice(mode: .named, toolName: outputToolName)
        responseFormat = nil
    } else {
        tools = nil
        toolChoice = nil
        responseFormat = ResponseFormat(kind: .jsonSchema, jsonSchema: schema, strict: true)
    }

    var result = try await generate(
        model: model,
        prompt: prompt,
        messages: messages,
        system: system,
        tools: tools,
        toolChoice: toolChoice,
        maxToolRounds: 0,
        stopWhen: nil,
        responseFormat: responseFormat,
        temperature: temperature,
        topP: topP,
        maxTokens: maxTokens,
        stopSequences: stopSequences,
        reasoningEffort: reasoningEffort,
        metadata: metadata,
        provider: provider,
        providerOptions: providerOptions,
        maxRetries: maxRetries,
        retryPolicy: retryPolicy,
        timeout: timeout,
        abortSignal: abortSignal,
        client: resolvedClient
    )

    let parsed: JSONValue = {
        if providerName == "anthropic" {
            guard let call = result.response.toolCalls.first(where: { $0.name == outputToolName }) else {
                return .null
            }
            return .object(call.arguments)
        }
        // Best-effort parse JSON from text.
        guard let data = result.text.data(using: .utf8), let json = try? JSONValue.parse(data) else {
            return .null
        }
        return json
    }()

    if parsed == .null {
        throw NoObjectGeneratedError(message: "No object generated (parse failed)")
    }

    do {
        try JSONSchema(schema).validate(parsed)
    } catch {
        throw NoObjectGeneratedError(message: "No object generated (schema validation failed): \(error)")
    }

    result.output = parsed
    return result
}

private func _parseJSONObjectFromUTF8(_ text: String) -> JSONValue? {
    guard let data = text.data(using: .utf8),
          let parsed = try? JSONValue.parse(data),
          parsed.objectValue != nil else {
        return nil
    }
    return parsed
}

/// A stream of parsed JSON objects derived from the text delta stream of `stream(...)`.
/// It yields progressively more complete values whenever the accumulated text becomes valid JSON.
public final class ObjectStreamResult<T>: AsyncSequence, @unchecked Sendable where T: Sendable {
    public typealias Element = T

    private let eventStream: AsyncThrowingStream<T, Error>
    private let underlyingStream: StreamResult

    init(eventStream: AsyncThrowingStream<T, Error>, underlyingStream: StreamResult) {
        self.eventStream = eventStream
        self.underlyingStream = underlyingStream
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        var base: AsyncThrowingStream<T, Error>.AsyncIterator

        public mutating func next() async throws -> T? {
            try await base.next()
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(base: eventStream.makeAsyncIterator())
    }

    /// Underlying stream result for access to metadata and final response.
    public var rawStream: StreamResult {
        underlyingStream
    }
}

public func streamObject(
    model: String,
    prompt: String? = nil,
    messages: [Message]? = nil,
    system: String? = nil,
    schema: JSONValue,
    temperature: Double? = nil,
    topP: Double? = nil,
    maxTokens: Int? = nil,
    stopSequences: [String]? = nil,
    reasoningEffort: String? = nil,
    metadata: [String: String]? = nil,
    provider: String? = nil,
    providerOptions: [String: JSONValue]? = nil,
    maxRetries: Int = 2,
    retryPolicy: RetryPolicy? = nil,
    timeout: Timeout? = nil,
    abortSignal: AbortSignal? = nil,
    client: Client? = nil
) async throws -> ObjectStreamResult<JSONValue> {
    let baseStream = try await stream(
        model: model,
        prompt: prompt,
        messages: messages,
        system: system,
        tools: nil,
        toolChoice: nil,
        maxToolRounds: 0,
        stopWhen: nil,
        responseFormat: ResponseFormat(kind: .jsonSchema, jsonSchema: schema, strict: true),
        temperature: temperature,
        topP: topP,
        maxTokens: maxTokens,
        stopSequences: stopSequences,
        reasoningEffort: reasoningEffort,
        metadata: metadata,
        provider: provider,
        providerOptions: providerOptions,
        maxRetries: maxRetries,
        retryPolicy: retryPolicy,
        timeout: timeout,
        abortSignal: abortSignal,
        client: client
    )

    let objectStream = AsyncThrowingStream<JSONValue, Error> { continuation in
        let task = Task {
            var accumulated = ""
            var lastParsed: JSONValue?

            do {
                for try await event in baseStream {
                    if event.type.rawValue == StreamEventType.textDelta.rawValue, let delta = event.delta {
                        accumulated += delta
                        if let parsed = _parseJSONObjectFromUTF8(accumulated), parsed != lastParsed {
                            lastParsed = parsed
                            continuation.yield(parsed)
                        }
                    }
                }

                if let parsed = _parseJSONObjectFromUTF8(accumulated), parsed != lastParsed {
                    continuation.yield(parsed)
                }

                continuation.finish()
            } catch {
                continuation.finish(throwing: error)
            }
        }

        continuation.onTermination = { _ in
            task.cancel()
        }
    }

    return ObjectStreamResult(eventStream: objectStream, underlyingStream: baseStream)
}

private actor _StreamFinal {
    private var response: Response?
    private var error: Error?
    private var waiters: [CheckedContinuation<Response, Error>] = []

    func setResponse(_ response: Response) {
        guard self.response == nil, self.error == nil else { return }
        self.response = response
        let ws = waiters
        waiters.removeAll(keepingCapacity: true)
        for w in ws { w.resume(returning: response) }
    }

    func setError(_ error: Error) {
        guard self.response == nil, self.error == nil else { return }
        self.error = error
        let ws = waiters
        waiters.removeAll(keepingCapacity: true)
        for w in ws { w.resume(throwing: error) }
    }

    func get() async throws -> Response {
        if let response { return response }
        if let error { throw error }
        return try await withCheckedThrowingContinuation { cont in
            waiters.append(cont)
        }
    }
}

private actor _PartialResponseStore {
    private var response: Response?
    func set(_ r: Response?) { response = r }
    func get() -> Response? { response }
}

public struct StreamResult: AsyncSequence, Sendable {
    public typealias Element = StreamEvent

    private let stream: AsyncThrowingStream<StreamEvent, Error>
    private let final: _StreamFinal
    private let partial: _PartialResponseStore

    fileprivate init(stream: AsyncThrowingStream<StreamEvent, Error>, final: _StreamFinal, partial: _PartialResponseStore) {
        self.stream = stream
        self.final = final
        self.partial = partial
    }

    public func makeAsyncIterator() -> AsyncThrowingStream<StreamEvent, Error>.AsyncIterator {
        stream.makeAsyncIterator()
    }

    public func response() async throws -> Response {
        try await final.get()
    }

    public var textStream: AsyncThrowingStream<String, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    for try await ev in self.stream {
                        if ev.type.rawValue == StreamEventType.textDelta.rawValue, let d = ev.delta {
                            continuation.yield(d)
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    public func partialResponse() async -> Response? {
        await partial.get()
    }
}

public func stream(
    model: String,
    prompt: String? = nil,
    messages: [Message]? = nil,
    system: String? = nil,
    tools: [Tool]? = nil,
    toolChoice: ToolChoice? = nil,
    maxToolRounds: Int = 1,
    stopWhen: StopCondition? = nil,
    responseFormat: ResponseFormat? = nil,
    temperature: Double? = nil,
    topP: Double? = nil,
    maxTokens: Int? = nil,
    stopSequences: [String]? = nil,
    reasoningEffort: String? = nil,
    metadata: [String: String]? = nil,
    provider: String? = nil,
    providerOptions: [String: JSONValue]? = nil,
    maxRetries: Int = 2,
    retryPolicy: RetryPolicy? = nil,
    timeout: Timeout? = nil,
    abortSignal: AbortSignal? = nil,
    client: Client? = nil
) async throws -> StreamResult {
    let resolvedClient: Client
    if let client {
        resolvedClient = client
    } else {
        resolvedClient = try await defaultClient()
    }

    let timeoutConfig = timeout?.asConfig ?? TimeoutConfig()
    let totalTimeout = timeoutConfig.total
    let perStepTimeout = timeoutConfig.perStep

    let initialMessages = try _standardizeMessages(prompt: prompt, messages: messages, system: system)

    let policy: RetryPolicy = {
        var p = retryPolicy ?? RetryPolicy(maxRetries: maxRetries)
        p.maxRetries = maxRetries
        return p
    }()

    let final = _StreamFinal()
    let partial = _PartialResponseStore()

    let s = AsyncThrowingStream<StreamEvent, Error> { continuation in
        Task {
            do {
                let maxRounds = max(0, maxToolRounds)

                let operation: @Sendable () async throws -> Void = {
                    continuation.yield(StreamEvent(type: .standard(.streamStart)))

                    var conversation = initialMessages
                    var steps: [StepResult] = []

                    func effectiveToolChoice(stepIndex: Int) -> ToolChoice? {
                        guard let toolChoice else { return nil }
                        if stepIndex > 0, toolChoice.mode == .named || toolChoice.mode == .required {
                            return nil
                        }
                        return toolChoice
                    }

                    func openStream(_ conversationMessages: [Message], stepIndex: Int) async throws -> AsyncThrowingStream<StreamEvent, Error> {
                        let req = Request(
                            model: model,
                            messages: conversationMessages,
                            provider: provider,
                            tools: tools,
                            toolChoice: effectiveToolChoice(stepIndex: stepIndex),
                            responseFormat: responseFormat,
                            temperature: temperature,
                            topP: topP,
                            maxTokens: maxTokens,
                            stopSequences: stopSequences,
                            reasoningEffort: reasoningEffort,
                            metadata: metadata,
                            providerOptions: providerOptions,
                            timeout: perStepTimeout.map { Timeout.config(TimeoutConfig(total: $0)) },
                            abortSignal: abortSignal
                        )

                        // Streaming retries apply only to establishing the stream (before any data is surfaced).
                        return try await retry(policy: policy, abortSignal: abortSignal) {
                            try await _withOptionalTimeout(perStepTimeout) {
                                try await resolvedClient.stream(req)
                            }
                        }
                    }

                    for stepIndex in 0..<(maxRounds + 1) {
                        try await abortSignal?.check()

                        let providerStream = try await openStream(conversation, stepIndex: stepIndex)

                        var stepResponse: Response?
                        var stepFinish: FinishReason?
                        var stepUsage: Usage?

                        for try await ev in providerStream {
                            try await abortSignal?.check()

                            if ev.type.rawValue == StreamEventType.streamStart.rawValue {
                                continue // suppress per-step streamStart
                            }

                            if ev.type.rawValue == StreamEventType.finish.rawValue {
                                stepResponse = ev.response
                                stepFinish = ev.finishReason ?? ev.response?.finishReason
                                stepUsage = ev.usage ?? ev.response?.usage
                                if let r = stepResponse {
                                    await partial.set(r)
                                }
                                continue // map to step_finish or final finish below
                            }

                            continuation.yield(ev)
                        }

                        guard let response = stepResponse else {
                            throw StreamError(message: "Stream ended without FINISH response")
                        }

                        let toolCalls = response.toolCalls

                        let shouldToolLoop =
                            (maxRounds > 0)
                            && (stepIndex < maxRounds)
                            && !(toolCalls.isEmpty)
                            && (stepFinish?.kind ?? response.finishReason.kind) == .toolCalls
                            && (tools?.contains(where: { $0.execute != nil }) ?? false)

                        let toolExecution: ToolExecutionOutcome
                        if shouldToolLoop, let tools {
                            toolExecution = try await _executeToolCalls(tools: tools, toolCalls: toolCalls, messages: conversation + [response.message], abortSignal: abortSignal)
                        } else {
                            toolExecution = ToolExecutionOutcome(toolResults: [], repairMessages: [])
                        }
                        let toolResults = toolExecution.toolResults

                        steps.append(
                            StepResult(
                                text: response.text,
                                reasoning: response.reasoning,
                                toolCalls: toolCalls,
                                toolResults: toolResults,
                                finishReason: stepFinish ?? response.finishReason,
                                usage: stepUsage ?? response.usage,
                                response: response,
                                warnings: response.warnings
                            )
                        )

                        let stopNow =
                            toolCalls.isEmpty
                            || (stepFinish?.kind ?? response.finishReason.kind) != .toolCalls
                            || maxRounds <= 0
                            || stepIndex >= maxRounds
                            || (stopWhen?(steps) ?? false)
                            || (!toolCalls.isEmpty && toolResults.isEmpty)

                        if stopNow {
                            let finishUsage = stepUsage ?? response.usage
                            let finish = stepFinish ?? response.finishReason
                            continuation.yield(StreamEvent(type: .standard(.finish), finishReason: finish, usage: finishUsage, response: response))
                            await final.setResponse(response)
                            continuation.finish()
                            return
                        }

                        // Continue with tool results.
                        conversation.append(response.message)
                        conversation.append(contentsOf: _makeToolResultMessages(toolResults, toolCalls: toolCalls))
                        conversation.append(contentsOf: toolExecution.repairMessages)

                        continuation.yield(StreamEvent(type: .standard(.stepFinish), finishReason: stepFinish ?? response.finishReason, usage: stepUsage ?? response.usage, response: response))
                    }

                    throw StreamError(message: "Stream exceeded max tool rounds without finishing")
                }

                if let totalTimeout {
                    try await _withTimeout(totalTimeout, operation: operation)
                } else {
                    try await operation()
                }
            } catch {
                let sdk = (error as? SDKError) ?? StreamError(message: String(describing: error), cause: error)
                continuation.yield(StreamEvent(type: .standard(.error), error: sdk))
                await final.setError(sdk)
                continuation.finish(throwing: sdk)
            }
        }
    }

    return StreamResult(stream: s, final: final, partial: partial)
}
