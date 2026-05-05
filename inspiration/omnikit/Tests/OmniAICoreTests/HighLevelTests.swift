import Testing
import Foundation

@testable import OmniAICore

private actor _MockAdapterState {
    var completeRequests: [Request] = []
    var completeCallCount: Int = 0

    func nextCompleteIndex(request: Request) -> Int {
        completeRequests.append(request)
        let idx = completeCallCount
        completeCallCount += 1
        return idx
    }

    func allCompleteRequests() -> [Request] { completeRequests }
    func completeCount() -> Int { completeCallCount }
}

private final class MockAdapter: ProviderAdapter, @unchecked Sendable {
    let name: String
    let state = _MockAdapterState()

    private let completeHandler: @Sendable (_ request: Request, _ index: Int) async throws -> Response
    private let streamHandler: (@Sendable (_ request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error>)?

    init(
        name: String = "test",
        complete: @Sendable @escaping (_ request: Request, _ index: Int) async throws -> Response,
        stream: (@Sendable (_ request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error>)? = nil
    ) {
        self.name = name
        self.completeHandler = complete
        self.streamHandler = stream
    }

    func complete(request: Request) async throws -> Response {
        let idx = await state.nextCompleteIndex(request: request)
        return try await completeHandler(request, idx)
    }

    func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        if let streamHandler {
            return try await streamHandler(request)
        }
        // Default: stream the complete() result as a single FINISH.
        let resp = try await complete(request: request)
        return AsyncThrowingStream { continuation in
            continuation.yield(StreamEvent(type: .standard(.streamStart)))
            continuation.yield(StreamEvent(type: .standard(.finish), finishReason: resp.finishReason, usage: resp.usage, response: resp))
            continuation.finish()
        }
    }
}

private actor _ContinuationState {
    var completeCount: Int = 0
    var continuationCount: Int = 0
    var lastContinuation: ToolContinuationRequest?

    func recordComplete() { completeCount += 1 }
    func recordContinuation(_ request: ToolContinuationRequest) {
        continuationCount += 1
        lastContinuation = request
    }
}

private final class MockContinuationAdapter: ProviderAdapter, ToolContinuationProviderAdapter, @unchecked Sendable {
    let name: String
    let state = _ContinuationState()
    private let completeHandler: @Sendable (Request) async throws -> Response
    private let continuationHandler: @Sendable (ToolContinuationRequest) async throws -> Response

    init(
        name: String = "test",
        complete: @Sendable @escaping (Request) async throws -> Response,
        continuation: @Sendable @escaping (ToolContinuationRequest) async throws -> Response
    ) {
        self.name = name
        self.completeHandler = complete
        self.continuationHandler = continuation
    }

    func complete(request: Request) async throws -> Response {
        await state.recordComplete()
        return try await completeHandler(request)
    }

    func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        let resp = try await complete(request: request)
        return AsyncThrowingStream { continuation in
            continuation.yield(StreamEvent(type: .standard(.streamStart)))
            continuation.yield(StreamEvent(type: .standard(.finish), finishReason: resp.finishReason, usage: resp.usage, response: resp))
            continuation.finish()
        }
    }

    func sendToolOutputs(request: ToolContinuationRequest) async throws -> Response {
        await state.recordContinuation(request)
        return try await continuationHandler(request)
    }
}

private func _response(
    provider: String,
    model: String,
    text: String = "",
    toolCalls: [ToolCall] = [],
    finishReason: String = "stop",
    usage: Usage = Usage(inputTokens: 1, outputTokens: 1)
) -> Response {
    var parts: [ContentPart] = []
    if !text.isEmpty { parts.append(.text(text)) }
    for c in toolCalls { parts.append(.toolCall(c)) }
    return Response(
        id: "resp_\(UUID().uuidString)",
        model: model,
        provider: provider,
        message: Message(role: .assistant, content: parts),
        finishReason: FinishReason(kind: FinishReason.Kind(rawValue: finishReason) ?? .other, raw: finishReason),
        usage: usage,
        raw: nil,
        warnings: [],
        rateLimit: nil
    )
}

@Suite
final class HighLevelTests {
    @Test
    func testGenerateWorksWithMessagesList() async throws {
        let adapter = MockAdapter { req, _ in
            XCTAssertEqual(req.messages.first?.role, .user)
            XCTAssertEqual(req.messages.first?.text, "hi")
            return _response(provider: "test", model: req.model, text: "ok")
        }
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await generate(
            model: "m",
            messages: [.user("hi")],
            provider: "test",
            maxRetries: 0,
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )
        XCTAssertEqual(result.text, "ok")
    }

    @Test
    func testModuleLevelDefaultClientOverride() async throws {
        let adapter = MockAdapter { req, _ in
            _response(provider: "test", model: req.model, text: "ok")
        }
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")
        await setDefaultClient(client)

        let result = try await generate(
            model: "m",
            prompt: "hi",
            provider: "test",
            maxRetries: 0,
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false)
        )
        XCTAssertEqual(result.text, "ok")
    }

    @Test
    func testGenerateRejectsPromptAndMessages() async {
        do {
            _ = try await generate(
                model: "m",
                prompt: "hi",
                messages: [.user("also")],
                client: try Client(providers: ["test": MockAdapter { _, _ in _response(provider: "test", model: "m", text: "ok") }], defaultProvider: "test")
            )
            XCTFail("Expected ConfigurationError")
        } catch is ConfigurationError {
            // ok
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    @Test
    func testToolLoopExecutesToolsInParallelAndBatchesResults() async throws {
        let clock = ContinuousClock()
        actor Timing {
            var starts: [String: ContinuousClock.Instant] = [:]
            var ends: [String: ContinuousClock.Instant] = [:]
            func start(_ id: String, _ t: ContinuousClock.Instant) { starts[id] = t }
            func end(_ id: String, _ t: ContinuousClock.Instant) { ends[id] = t }
            func snapshot() -> (starts: [String: ContinuousClock.Instant], ends: [String: ContinuousClock.Instant]) { (starts, ends) }
        }
        let timing = Timing()

        let toolSchema: JSONValue = .object([
            "type": .string("object"),
            "properties": .object(["x": .object(["type": .string("integer")])]),
            "required": .array([.string("x")]),
        ])

        let t1 = try Tool(name: "sleep1", description: "t1", parameters: toolSchema) { _, ctx in
            await timing.start(ctx.toolCallId, clock.now)
            try await Task.sleep(for: .milliseconds(150))
            await timing.end(ctx.toolCallId, clock.now)
            return .string("ok1")
        }
        let t2 = try Tool(name: "sleep2", description: "t2", parameters: toolSchema) { _, ctx in
            await timing.start(ctx.toolCallId, clock.now)
            try await Task.sleep(for: .milliseconds(150))
            await timing.end(ctx.toolCallId, clock.now)
            return .string("ok2")
        }

        let adapter = MockAdapter { req, idx in
            if idx == 0 {
                let calls = [
                    ToolCall(id: "c1", name: "sleep1", arguments: ["x": .number(1)], rawArguments: nil),
                    ToolCall(id: "c2", name: "sleep2", arguments: ["x": .number(2)], rawArguments: nil),
                ]
                return _response(provider: "test", model: req.model, toolCalls: calls, finishReason: "tool_calls")
            }

            // Second request: tool results must be batched in a single continuation request.
            let toolMsgs = req.messages.filter { $0.role == .tool }
            XCTAssertEqual(toolMsgs.count, 2)
            XCTAssertEqual(toolMsgs[0].toolCallId, "c1")
            XCTAssertEqual(toolMsgs[1].toolCallId, "c2")

            return _response(provider: "test", model: req.model, text: "done", finishReason: "stop")
        }

        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await generate(
            model: "m",
            prompt: "hi",
            tools: [t1, t2],
            maxToolRounds: 1,
            provider: "test",
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        XCTAssertEqual(result.steps.count, 2)
        XCTAssertEqual(result.text, "done")

        let snap = await timing.snapshot()
        XCTAssertEqual(snap.starts.keys.sorted(), ["c1", "c2"])
        XCTAssertEqual(snap.ends.keys.sorted(), ["c1", "c2"])

        // Ensure overlap: second tool started before the first one finished.
        let c1Start = snap.starts["c1"]!
        let c1End = snap.ends["c1"]!
        let c2Start = snap.starts["c2"]!
        XCTAssertLessThan(c2Start, c1End)
        XCTAssertLessThan(c1Start, c1End)
    }

    @Test
    func testGenerateUsesToolContinuationAdapterWhenAvailable() async throws {
        let toolSchema: JSONValue = .object([
            "type": .string("object"),
            "properties": .object(["x": .object(["type": .string("integer")])]),
            "required": .array([.string("x")]),
        ])

        let tool = try Tool(name: "demo", description: "t", parameters: toolSchema) { _, _ in
            .number(42)
        }

        let adapter = MockContinuationAdapter(
            complete: { req in
                let call = ToolCall(id: "call_1", name: "demo", arguments: ["x": .number(1)], rawArguments: nil)
                return Response(
                    id: "resp_1",
                    model: req.model,
                    provider: "test",
                    message: Message(role: .assistant, content: [.toolCall(call)]),
                    finishReason: FinishReason(kind: .toolCalls, raw: "tool_calls"),
                    usage: Usage(inputTokens: 1, outputTokens: 1),
                    raw: nil,
                    warnings: [],
                    rateLimit: nil
                )
            },
            continuation: { continuation in
                XCTAssertEqual(continuation.previousResponseId, "resp_1")
                XCTAssertEqual(continuation.toolResults.count, 1)
                return _response(provider: "test", model: continuation.model, text: "done", finishReason: "stop")
            }
        )

        let client = try Client(providers: ["test": adapter], defaultProvider: "test")
        let result = try await generate(
            model: "m",
            prompt: "hi",
            tools: [tool],
            maxToolRounds: 1,
            provider: "test",
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        XCTAssertEqual(result.text, "done")
        let completeCount = await adapter.state.completeCount
        let continuationCount = await adapter.state.continuationCount
        XCTAssertEqual(completeCount, 1)
        XCTAssertEqual(continuationCount, 1)
    }

    @Test
    func testMaxToolRoundsZeroDisablesAutoExecution() async throws {
        let toolSchema: JSONValue = .object(["type": .string("object")])
        let t1 = try Tool(name: "t", description: "t", parameters: toolSchema) { _, _ in .string("ok") }

        let adapter = MockAdapter { req, _ in
            let calls = [ToolCall(id: "c1", name: "t", arguments: [:], rawArguments: nil)]
            return _response(provider: "test", model: req.model, toolCalls: calls, finishReason: "tool_calls")
        }
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await generate(
            model: "m",
            prompt: "hi",
            tools: [t1],
            maxToolRounds: 0,
            provider: "test",
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        XCTAssertEqual(result.steps.count, 1)
        XCTAssertEqual(result.toolCalls.count, 1)
        XCTAssertTrue(result.toolResults.isEmpty)
        let calls = await adapter.state.completeCount()
        XCTAssertEqual(calls, 1)
    }

    @Test
    func testPassiveToolCallDoesNotLoop() async throws {
        let toolSchema: JSONValue = .object(["type": .string("object")])
        let passive = try Tool(name: "passive", description: "p", parameters: toolSchema, execute: nil)

        let adapter = MockAdapter { req, _ in
            let calls = [ToolCall(id: "c1", name: "passive", arguments: [:], rawArguments: nil)]
            return _response(provider: "test", model: req.model, toolCalls: calls, finishReason: "tool_calls")
        }
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await generate(
            model: "m",
            prompt: "hi",
            tools: [passive],
            maxToolRounds: 2,
            provider: "test",
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        XCTAssertEqual(result.steps.count, 1)
        XCTAssertEqual(result.toolCalls.count, 1)
        XCTAssertTrue(result.toolResults.isEmpty)
        let calls = await adapter.state.completeCount()
        XCTAssertEqual(calls, 1)
    }

    @Test
    func testUnknownToolCallSendsErrorResultAndContinues() async throws {
        let adapter = MockAdapter { req, idx in
            if idx == 0 {
                let calls = [ToolCall(id: "c1", name: "unknown", arguments: [:], rawArguments: nil)]
                return _response(provider: "test", model: req.model, toolCalls: calls, finishReason: "tool_calls")
            }
            let toolMsgs = req.messages.filter { $0.role == .tool }
            XCTAssertEqual(toolMsgs.count, 1)
            let tr = toolMsgs[0].content.first?.toolResult
            XCTAssertEqual(tr?.toolCallId, "c1")
            XCTAssertEqual(tr?.isError, true)
            XCTAssertTrue(tr?.content.stringValue?.contains("Unknown tool") ?? false)
            return _response(provider: "test", model: req.model, text: "done")
        }
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let activeSchema: JSONValue = .object(["type": .string("object")])
        let active = try Tool(name: "active", description: "a", parameters: activeSchema) { _, _ in .string("ok") }

        let result = try await generate(
            model: "m",
            prompt: "hi",
            tools: [active],
            maxToolRounds: 1,
            provider: "test",
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        XCTAssertEqual(result.text, "done")
        let calls = await adapter.state.completeCount()
        XCTAssertEqual(calls, 2)
    }

    @Test
    func testToolArgumentsAreValidatedBeforeExecution() async throws {
        actor Flag {
            var executed = false
            func set() { executed = true }
            func get() -> Bool { executed }
        }
        let flag = Flag()

        let schema: JSONValue = .object([
            "type": .string("object"),
            "properties": .object(["x": .object(["type": .string("integer")])]),
            "required": .array([.string("x")]),
        ])
        let tool = try Tool(name: "t", description: "t", parameters: schema) { _, _ in
            await flag.set()
            return .string("ok")
        }

        let adapter = MockAdapter { req, idx in
            if idx == 0 {
                // Invalid: x is a string, schema expects integer.
                let calls = [ToolCall(id: "c1", name: "t", arguments: ["x": .string("nope")], rawArguments: nil)]
                return _response(provider: "test", model: req.model, toolCalls: calls, finishReason: "tool_calls")
            }
            let toolMsgs = req.messages.filter { $0.role == .tool }
            XCTAssertEqual(toolMsgs.count, 1)
            let tr = toolMsgs[0].content.first?.toolResult
            XCTAssertEqual(tr?.toolCallId, "c1")
            XCTAssertEqual(tr?.isError, true)
            XCTAssertTrue(tr?.content.stringValue?.contains("Invalid tool arguments") ?? false)

            let repairMessages = req.messages.filter { $0.role == .developer }
            XCTAssertEqual(repairMessages.count, 1)
            XCTAssertTrue(repairMessages[0].text.contains("Do not repeat the same invalid call"))
            XCTAssertTrue(repairMessages[0].text.contains("Validation error:"))

            return _response(provider: "test", model: req.model, text: "done")
        }
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await generate(
            model: "m",
            prompt: "hi",
            tools: [tool],
            maxToolRounds: 1,
            provider: "test",
            maxRetries: 0,
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        XCTAssertEqual(result.text, "done")
        let executed = await flag.get()
        XCTAssertEqual(executed, false)
    }

    @Test
    func testRetryOnRateLimit() async throws {
        let adapter = MockAdapter { req, idx in
            if idx == 0 {
                throw RateLimitError(
                    message: "rate limited",
                    provider: "test",
                    statusCode: 429,
                    errorCode: nil,
                    retryable: true,
                    retryAfter: nil,
                    raw: nil
                )
            }
            return _response(provider: "test", model: req.model, text: "ok")
        }
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await generate(
            model: "m",
            prompt: "hi",
            provider: "test",
            maxRetries: 2,
            retryPolicy: RetryPolicy(maxRetries: 2, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )
        XCTAssertEqual(result.text, "ok")
        let calls = await adapter.state.completeCount()
        XCTAssertEqual(calls, 2)
    }

    @Test
    func testNonRetryableErrorIsNotRetried() async {
        let adapter = MockAdapter { _, _ in
            throw AuthenticationError(
                message: "bad key",
                provider: "test",
                statusCode: 401,
                errorCode: nil,
                retryable: false,
                retryAfter: nil,
                raw: nil
            )
        }
        let client = try! Client(providers: ["test": adapter], defaultProvider: "test")
        do {
            _ = try await generate(
                model: "m",
                prompt: "hi",
                provider: "test",
                maxRetries: 5,
                retryPolicy: RetryPolicy(maxRetries: 5, baseDelaySeconds: 0.0, jitter: false),
                client: client
            )
            XCTFail("Expected AuthenticationError")
        } catch is AuthenticationError {
            let calls = await adapter.state.completeCount()
            XCTAssertEqual(calls, 1)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    @Test
    func testRetryAfterExceedsMaxDelayDoesNotRetry() async {
        let adapter = MockAdapter { _, _ in
            throw RateLimitError(
                message: "rate limited",
                provider: "test",
                statusCode: 429,
                errorCode: nil,
                retryable: true,
                retryAfter: 10.0,
                raw: nil
            )
        }
        let client = try! Client(providers: ["test": adapter], defaultProvider: "test")
        do {
            _ = try await generate(
                model: "m",
                prompt: "hi",
                provider: "test",
                maxRetries: 3,
                retryPolicy: RetryPolicy(maxRetries: 3, baseDelaySeconds: 0.0, maxDelaySeconds: 0.1, jitter: false),
                client: client
            )
            XCTFail("Expected RateLimitError")
        } catch is RateLimitError {
            let calls = await adapter.state.completeCount()
            XCTAssertEqual(calls, 1)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    @Test
    func testPerStepTimeout() async {
        let adapter = MockAdapter { req, _ in
            try await Task.sleep(for: .milliseconds(200))
            return _response(provider: "test", model: req.model, text: "late")
        }
        let client = try! Client(providers: ["test": adapter], defaultProvider: "test")

        do {
            _ = try await generate(
                model: "m",
                prompt: "hi",
                provider: "test",
                maxRetries: 0,
                retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
                timeout: .config(TimeoutConfig(total: nil, perStep: .milliseconds(50))),
                client: client
            )
            XCTFail("Expected RequestTimeoutError")
        } catch is RequestTimeoutError {
            let calls = await adapter.state.completeCount()
            XCTAssertEqual(calls, 1)
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    @Test
    func testTotalTimeoutAppliesToWholeOperation() async {
        let adapter = MockAdapter { req, _ in
            try await Task.sleep(for: .milliseconds(200))
            return _response(provider: "test", model: req.model, text: "late")
        }
        let client = try! Client(providers: ["test": adapter], defaultProvider: "test")

        do {
            _ = try await generate(
                model: "m",
                prompt: "hi",
                provider: "test",
                maxRetries: 0,
                retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
                timeout: .config(TimeoutConfig(total: .milliseconds(50), perStep: .milliseconds(500))),
                client: client
            )
            XCTFail("Expected RequestTimeoutError")
        } catch is RequestTimeoutError {
            // ok
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    @Test
    func testAbortSignalCancelsGenerate() async {
        let signal = AbortSignal()
        signal.abort()
        await signal.wait()

        let adapter = MockAdapter { req, _ in
            return _response(provider: "test", model: req.model, text: "ok")
        }
        let client = try! Client(providers: ["test": adapter], defaultProvider: "test")

        do {
            _ = try await generate(
                model: "m",
                prompt: "hi",
                provider: "test",
                abortSignal: signal,
                client: client
            )
            XCTFail("Expected AbortError")
        } catch is AbortError {
            // ok
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }

    @Test
    func testHighLevelStreamAccumulatesTextDeltasAndResponse() async throws {
        let adapter = MockAdapter(
            complete: { req, _ in _response(provider: "test", model: req.model, text: "unused") },
            stream: { req in
                let resp = _response(provider: "test", model: req.model, text: "hello")
                return AsyncThrowingStream { continuation in
                    continuation.yield(StreamEvent(type: .standard(.streamStart)))
                    continuation.yield(StreamEvent(type: .standard(.textStart), textId: "text_0"))
                    continuation.yield(StreamEvent(type: .standard(.textDelta), delta: "he", textId: "text_0"))
                    continuation.yield(StreamEvent(type: .standard(.textDelta), delta: "llo", textId: "text_0"))
                    continuation.yield(StreamEvent(type: .standard(.textEnd), textId: "text_0"))
                    continuation.yield(StreamEvent(type: .standard(.finish), finishReason: resp.finishReason, usage: resp.usage, response: resp))
                    continuation.finish()
                }
            }
        )
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await stream(
            model: "m",
            prompt: "hi",
            provider: "test",
            maxRetries: 0,
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        var chunks: [String] = []
        for try await ev in result {
            if ev.type.rawValue == StreamEventType.textDelta.rawValue {
                chunks.append(ev.delta ?? "")
            }
        }
        XCTAssertEqual(chunks.joined(), "hello")

        let resp = try await result.response()
        XCTAssertEqual(resp.text, "hello")
    }

    @Test
    func testStreamYieldsStreamStartAndFinishWithMetadata() async throws {
        let adapter = MockAdapter(
            complete: { req, _ in _response(provider: "test", model: req.model, text: "unused") },
            stream: { req in
                let resp = _response(provider: "test", model: req.model, text: "hello")
                return AsyncThrowingStream { continuation in
                    continuation.yield(StreamEvent(type: .standard(.textStart), textId: "text_0"))
                    continuation.yield(StreamEvent(type: .standard(.textDelta), delta: "he", textId: "text_0"))
                    continuation.yield(StreamEvent(type: .standard(.textDelta), delta: "llo", textId: "text_0"))
                    continuation.yield(StreamEvent(type: .standard(.textEnd), textId: "text_0"))
                    continuation.yield(StreamEvent(type: .standard(.finish), finishReason: resp.finishReason, usage: resp.usage, response: resp))
                    continuation.finish()
                }
            }
        )
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await stream(
            model: "m",
            prompt: "hi",
            provider: "test",
            maxRetries: 0,
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        var events: [String] = []
        var chunks: [String] = []
        var finishEvent: StreamEvent?
        for try await ev in result {
            events.append(ev.type.rawValue)
            if ev.type.rawValue == StreamEventType.textDelta.rawValue {
                chunks.append(ev.delta ?? "")
            }
            if ev.type.rawValue == StreamEventType.finish.rawValue {
                finishEvent = ev
            }
        }

        XCTAssertEqual(events.first, StreamEventType.streamStart.rawValue)
        XCTAssertEqual(events.last, StreamEventType.finish.rawValue)

        let finish = try XCTUnwrap(finishEvent)
        XCTAssertNotNil(finish.finishReason)
        XCTAssertNotNil(finish.usage)
        XCTAssertEqual(finish.response?.text, "hello")
        XCTAssertEqual(chunks.joined(), "hello")
    }

    @Test
    func testGenerateObjectParsesAndValidatesOutput() async throws {
        let schema: JSONValue = .object([
            "type": .string("object"),
            "properties": .object([
                "name": .object(["type": .string("string")]),
                "age": .object(["type": .string("integer")]),
            ]),
            "required": .array([.string("name"), .string("age")]),
        ])

        let adapter = MockAdapter { req, _ in
            _response(provider: "test", model: req.model, text: #"{"name":"Alice","age":30}"#)
        }
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await generateObject(
            model: "m",
            prompt: "extract",
            schema: schema,
            provider: "test",
            maxRetries: 0,
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        XCTAssertNotNil(result.output)
        XCTAssertEqual(result.output?["name"]?.stringValue, "Alice")
        XCTAssertEqual(result.output?["age"]?.doubleValue, 30)
    }

    @Test
    func testGenerateObjectThrowsOnParseFailure() async throws {
        let schema: JSONValue = .object(["type": .string("object")])
        let adapter = MockAdapter { req, _ in
            _response(provider: "test", model: req.model, text: "not json")
        }
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        do {
            _ = try await generateObject(
                model: "m",
                prompt: "extract",
                schema: schema,
                provider: "test",
                maxRetries: 0,
                retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
                client: client
            )
            XCTFail("Expected NoObjectGeneratedError")
        } catch is NoObjectGeneratedError {
            // ok
        }
    }

    @Test
    func testAbortSignalCancelsStream() async throws {
        let signal = AbortSignal()

        let adapter = MockAdapter(
            complete: { req, _ in _response(provider: "test", model: req.model, text: "unused") },
            stream: { _ in
                AsyncThrowingStream { continuation in
                    continuation.yield(StreamEvent(type: .standard(.streamStart)))
                    Task {
                        try await Task.sleep(for: .milliseconds(50))
                        continuation.yield(StreamEvent(type: .standard(.textStart), textId: "text_0"))
                        continuation.yield(StreamEvent(type: .standard(.textDelta), delta: "hi", textId: "text_0"))
                        try await Task.sleep(for: .milliseconds(500))
                        continuation.yield(StreamEvent(type: .standard(.textEnd), textId: "text_0"))
                        let resp = _response(provider: "test", model: "m", text: "hi")
                        continuation.yield(StreamEvent(type: .standard(.finish), finishReason: resp.finishReason, usage: resp.usage, response: resp))
                        continuation.finish()
                    }
                }
            }
        )
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await stream(
            model: "m",
            prompt: "hi",
            provider: "test",
            maxRetries: 0,
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            abortSignal: signal,
            client: client
        )

        do {
            for try await ev in result {
                if ev.type.rawValue == StreamEventType.textDelta.rawValue {
                    signal.abort()
                }
            }
            XCTFail("Expected AbortError")
        } catch is AbortError {
            // ok
        }
    }

    @Test
    func testStepResultTracksToolCallsToolResultsAndUsage() async throws {
        let toolSchema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "t", description: "t", parameters: toolSchema) { _, _ in
            .string("ok")
        }

        let adapter = MockAdapter { req, idx in
            if idx == 0 {
                return _response(
                    provider: "test",
                    model: req.model,
                    toolCalls: [ToolCall(id: "c1", name: "t", arguments: [:], rawArguments: nil)],
                    finishReason: "tool_calls",
                    usage: Usage(inputTokens: 10, outputTokens: 0)
                )
            }
            return _response(
                provider: "test",
                model: req.model,
                text: "done",
                finishReason: "stop",
                usage: Usage(inputTokens: 1, outputTokens: 2)
            )
        }
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await generate(
            model: "m",
            prompt: "hi",
            tools: [tool],
            maxToolRounds: 1,
            provider: "test",
            maxRetries: 0,
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        XCTAssertEqual(result.steps.count, 2)
        XCTAssertEqual(result.steps[0].toolCalls.count, 1)
        XCTAssertEqual(result.steps[0].toolResults.count, 1)
        XCTAssertEqual(result.steps[0].usage.inputTokens, 10)
        XCTAssertEqual(result.steps[1].finishReason.kind.rawValue, "stop")

        XCTAssertEqual(result.totalUsage.inputTokens, 11)
        XCTAssertEqual(result.totalUsage.outputTokens, 2)
    }

    @Test
    func testMaxToolRoundsStopsAfterConfiguredRounds() async throws {
        actor Flag {
            var executed: Int = 0
            func inc() { executed += 1 }
            func get() -> Int { executed }
        }
        let flag = Flag()

        let toolSchema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "t", description: "t", parameters: toolSchema) { _, _ in
            await flag.inc()
            return .string("ok")
        }

        let adapter = MockAdapter { req, idx in
            if idx == 0 {
                return _response(
                    provider: "test",
                    model: req.model,
                    toolCalls: [ToolCall(id: "c1", name: "t", arguments: [:], rawArguments: nil)],
                    finishReason: "tool_calls"
                )
            }
            return _response(
                provider: "test",
                model: req.model,
                toolCalls: [ToolCall(id: "c2", name: "t", arguments: [:], rawArguments: nil)],
                finishReason: "tool_calls"
            )
        }
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await generate(
            model: "m",
            prompt: "hi",
            tools: [tool],
            maxToolRounds: 1,
            provider: "test",
            maxRetries: 0,
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        XCTAssertEqual(result.steps.count, 2)
        let completeCount = await adapter.state.completeCount()
        XCTAssertEqual(completeCount, 2)
        let executedCount = await flag.get()
        XCTAssertEqual(executedCount, 1)
    }

    @Test
    func testToolExecutionErrorsAreSentAsErrorResults() async throws {
        let toolSchema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "boom", description: "boom", parameters: toolSchema) { _, _ in
            struct ToolError: Error {}
            throw ToolError()
        }

        let adapter = MockAdapter { req, idx in
            if idx == 0 {
                return _response(
                    provider: "test",
                    model: req.model,
                    toolCalls: [ToolCall(id: "c1", name: "boom", arguments: [:], rawArguments: nil)],
                    finishReason: "tool_calls"
                )
            }
            let toolMsgs = req.messages.filter { $0.role == .tool }
            XCTAssertEqual(toolMsgs.count, 1)
            let tr = toolMsgs[0].content.first?.toolResult
            XCTAssertEqual(tr?.toolCallId, "c1")
            XCTAssertEqual(tr?.isError, true)
            return _response(provider: "test", model: req.model, text: "done", finishReason: "stop")
        }
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await generate(
            model: "m",
            prompt: "hi",
            tools: [tool],
            maxToolRounds: 1,
            provider: "test",
            maxRetries: 0,
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        XCTAssertEqual(result.text, "done")
    }

    @Test
    func testNamedToolChoiceIsOnlyAppliedOnFirstStep() async throws {
        let toolSchema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "t", description: "t", parameters: toolSchema) { _, _ in .string("ok") }

        let adapter = MockAdapter { req, idx in
            if idx == 0 {
                XCTAssertEqual(req.toolChoice?.mode, .named)
                XCTAssertEqual(req.toolChoice?.toolName, "t")
                return _response(
                    provider: "test",
                    model: req.model,
                    toolCalls: [ToolCall(id: "c1", name: "t", arguments: [:], rawArguments: nil)],
                    finishReason: "tool_calls"
                )
            }

            // Continuation request should not re-force the same tool again.
            XCTAssertNil(req.toolChoice)
            XCTAssertEqual(req.messages.filter { $0.role == .tool }.count, 1)
            return _response(provider: "test", model: req.model, text: "done", finishReason: "stop")
        }

        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await generate(
            model: "m",
            prompt: "hi",
            tools: [tool],
            toolChoice: ToolChoice(mode: .named, toolName: "t"),
            maxToolRounds: 1,
            provider: "test",
            maxRetries: 0,
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        XCTAssertEqual(result.text, "done")
        XCTAssertEqual(result.steps.count, 2)
    }

    @Test
    func testRetriesApplyPerStepNotWholeOperation() async throws {
        let toolSchema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "t", description: "t", parameters: toolSchema) { _, _ in .string("ok") }

        let adapter = MockAdapter { req, idx in
            if idx == 0 {
                XCTAssertTrue(req.messages.filter { $0.role == .tool }.isEmpty)
                return _response(
                    provider: "test",
                    model: req.model,
                    toolCalls: [ToolCall(id: "c1", name: "t", arguments: [:], rawArguments: nil)],
                    finishReason: "tool_calls"
                )
            }

            // Step 2: ensure we are continuing from step 1 (tool result present).
            XCTAssertEqual(req.messages.filter { $0.role == .tool }.count, 1)

            if idx == 1 {
                throw RateLimitError(
                    message: "rate limited",
                    provider: "test",
                    statusCode: 429,
                    errorCode: nil,
                    retryable: true,
                    retryAfter: nil,
                    raw: nil
                )
            }

            // Retry of step 2 should preserve the conversation (still has tool result).
            XCTAssertEqual(req.messages.filter { $0.role == .tool }.count, 1)
            return _response(provider: "test", model: req.model, text: "done", finishReason: "stop")
        }

        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await generate(
            model: "m",
            prompt: "hi",
            tools: [tool],
            maxToolRounds: 1,
            provider: "test",
            maxRetries: 1,
            retryPolicy: RetryPolicy(maxRetries: 1, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        XCTAssertEqual(result.text, "done")
        let completeCount = await adapter.state.completeCount()
        XCTAssertEqual(completeCount, 3)
    }

    @Test
    func testStreamingDoesNotRetryAfterPartialDataDelivered() async throws {
        actor State {
            var streamCalls: Int = 0
            func inc() { streamCalls += 1 }
            func get() -> Int { streamCalls }
        }
        let state = State()

        final class FailingStreamAdapter: ProviderAdapter, @unchecked Sendable {
            let name: String = "test"
            let state: State

            init(state: State) { self.state = state }

            func complete(request: Request) async throws -> Response {
                _response(provider: "test", model: request.model, text: "unused")
            }

            func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
                await state.inc()
                return AsyncThrowingStream { continuation in
                    continuation.yield(StreamEvent(type: .standard(.streamStart)))
                    continuation.yield(StreamEvent(type: .standard(.textStart), textId: "text_0"))
                    continuation.yield(StreamEvent(type: .standard(.textDelta), delta: "hi", textId: "text_0"))
                    continuation.finish(
                        throwing: RateLimitError(
                            message: "rate limited",
                            provider: "test",
                            statusCode: 429,
                            errorCode: nil,
                            retryable: true,
                            retryAfter: nil,
                            raw: nil
                        )
                    )
                }
            }
        }

        let client = try Client(providers: ["test": FailingStreamAdapter(state: state)], defaultProvider: "test")

        let result = try await stream(
            model: "m",
            prompt: "hi",
            provider: "test",
            maxRetries: 5,
            retryPolicy: RetryPolicy(maxRetries: 5, baseDelaySeconds: 0.0, jitter: false),
            client: client
        )

        var chunks: [String] = []
        do {
            for try await ev in result {
                if ev.type.rawValue == StreamEventType.textDelta.rawValue {
                    chunks.append(ev.delta ?? "")
                }
            }
            XCTFail("Expected stream to throw after partial data")
        } catch {
            // ok
        }

        XCTAssertEqual(chunks.joined(), "hi")
        let streamCalls = await state.get()
        XCTAssertEqual(streamCalls, 1)
    }
}
