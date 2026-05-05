import Testing
import Foundation

@testable import OmniAICore

private actor CompatibilityRecorder {
    private var events: [String] = []

    func add(_ value: String) {
        events.append(value)
    }

    func all() -> [String] {
        events
    }

    func clear() {
        events.removeAll(keepingCapacity: true)
    }
}

private func _compatResponse(
    provider: String = "test",
    model: String = "m",
    text: String = "ok",
    finishReason: FinishReason = .stop,
    usage: Usage = Usage(inputTokens: 1, outputTokens: 1)
) -> Response {
    Response(
        id: "r_\(UUID().uuidString)",
        model: model,
        provider: provider,
        message: .assistant(text),
        finishReason: finishReason,
        usage: usage
    )
}

private final class CompatibilityAdapter: ProviderAdapter, @unchecked Sendable {
    let name: String
    let completeHandler: @Sendable (Request) async throws -> Response
    let streamHandler: (@Sendable (Request) async throws -> AsyncThrowingStream<StreamEvent, Error>)?

    init(
        name: String = "test",
        complete: @Sendable @escaping (Request) async throws -> Response,
        stream: (@Sendable (Request) async throws -> AsyncThrowingStream<StreamEvent, Error>)? = nil
    ) {
        self.name = name
        self.completeHandler = complete
        self.streamHandler = stream
    }

    func complete(request: Request) async throws -> Response {
        try await completeHandler(request)
    }

    func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        if let streamHandler {
            return try await streamHandler(request)
        }
        let response = try await completeHandler(request)
        return AsyncThrowingStream { continuation in
            continuation.yield(StreamEvent(type: .streamStart))
            continuation.yield(
                StreamEvent(
                    type: .finish,
                    finishReason: response.finishReason,
                    usage: response.usage,
                    response: response
                )
            )
            continuation.finish()
        }
    }
}

@Suite
final class CompatibilityTests {
    @Test
    func testPresetsAndEventCompatibilityProperties() {
        XCTAssertEqual(ToolChoice.auto.mode, .auto)
        XCTAssertEqual(ToolChoice.none.mode, .none)
        XCTAssertEqual(ToolChoice.required.mode, .required)
        XCTAssertEqual(ToolChoice.named("weather").toolName, "weather")

        XCTAssertEqual(ResponseFormat.text.kind.rawValue, "text")
        XCTAssertEqual(ResponseFormat.json.kind.rawValue, "json")
        XCTAssertEqual(ResponseFormat.jsonSchema(.object(["type": .string("object")]), strict: true).kind.rawValue, "json_schema")

        XCTAssertEqual(FinishReason.stop.kind.rawValue, "stop")
        XCTAssertEqual(FinishReason.length.kind.rawValue, "length")
        XCTAssertEqual(FinishReason.toolCalls.kind.rawValue, "tool_calls")
        XCTAssertEqual(FinishReason.contentFilter.kind.rawValue, "content_filter")

        let known = StreamEvent(type: .textDelta, delta: "x", textId: "t0")
        XCTAssertEqual(known.eventType, .textDelta)

        let knownViaString = StreamEvent(typeString: "text_delta", delta: "x", textId: "t0")
        XCTAssertEqual(knownViaString.eventType, .textDelta)

        let custom = StreamEvent(typeString: "my_provider_event")
        XCTAssertNil(custom.eventType)
    }

    @Test
    func testClientSupportsClosureMiddlewareAndStreamMiddleware() async throws {
        let recorder = CompatibilityRecorder()
        let adapter = CompatibilityAdapter(complete: { request in
            _compatResponse(provider: "test", model: request.model, text: "ok")
        })
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        client.addMiddleware { request, next in
            await recorder.add("c1.in")
            let response = try await next(request)
            await recorder.add("c1.out")
            return response
        }
        client.addMiddleware { request, next in
            await recorder.add("c2.in")
            let response = try await next(request)
            await recorder.add("c2.out")
            return response
        }

        _ = try await client.complete(
            request: Request(model: "m", messages: [.user("hi")], provider: "test")
        )
        let completeEvents = await recorder.all()
        XCTAssertEqual(completeEvents, ["c1.in", "c2.in", "c2.out", "c1.out"])

        await recorder.clear()

        client.addStreamMiddleware { request, next in
            await recorder.add("s1.in")
            let stream = try await next(request)
            return AsyncThrowingStream { continuation in
                Task {
                    do {
                        for try await event in stream {
                            continuation.yield(event)
                        }
                        await recorder.add("s1.out")
                        continuation.finish()
                    } catch {
                        await recorder.add("s1.out")
                        continuation.finish(throwing: error)
                    }
                }
            }
        }
        client.addStreamMiddleware { request, next in
            await recorder.add("s2.in")
            let stream = try await next(request)
            return AsyncThrowingStream { continuation in
                Task {
                    do {
                        for try await event in stream {
                            continuation.yield(event)
                        }
                        await recorder.add("s2.out")
                        continuation.finish()
                    } catch {
                        await recorder.add("s2.out")
                        continuation.finish(throwing: error)
                    }
                }
            }
        }

        let stream = try await client.stream(
            request: Request(model: "m", messages: [.user("hi")], provider: "test")
        )
        for try await _ in stream {}
        let streamEvents = await recorder.all()
        XCTAssertEqual(streamEvents, ["s1.in", "s2.in", "s2.out", "s1.out"])
    }

    @Test
    func testClientProviderMutationAndLabeledMethods() async throws {
        let p1 = CompatibilityAdapter(name: "p1", complete: { request in
            _compatResponse(provider: "p1", model: request.model, text: "one")
        })
        let p2 = CompatibilityAdapter(name: "p2", complete: { request in
            _compatResponse(provider: "p2", model: request.model, text: "two")
        })

        let client = try Client(providers: [:], defaultProvider: nil)
        client.registerProvider("p1", adapter: p1)
        XCTAssertEqual(client.defaultProviderName, "p1")
        client.register(provider: "p2", adapter: p2)
        client.setDefault(provider: "p2")
        XCTAssertEqual(client.defaultProviderName, "p2")

        let response = try await client.complete(
            request: Request(model: "m", messages: [.user("hi")])
        )
        XCTAssertEqual(response.provider, "p2")
    }

    @Test
    func testFromEnvAllowingEmptyCreatesClientWithoutProviders() async throws {
        let client = try Client.fromEnv(environment: [:], allowEmptyProviders: true)
        XCTAssertNil(client.defaultProvider)

        do {
            _ = try await client.complete(
                request: Request(model: "m", messages: [.user("hi")])
            )
            XCTFail("Expected ConfigurationError")
        } catch is ConfigurationError {
            // ok
        }
    }

    @Test
    func testDefaultClientHelpers() async throws {
        let adapter = CompatibilityAdapter(complete: { request in
            _compatResponse(provider: "test", model: request.model, text: "ok")
        })
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        await setDefaultClient(client)
        let resolvedDefaultProvider = try await defaultClient().defaultProvider
        XCTAssertEqual(resolvedDefaultProvider, "test")
        await setDefaultClient(nil)
    }

    @Test
    func testStreamObjectParsesOutputAndExposesRawStream() async throws {
        let expectedText = #"{"name":"Alice","age":30}"#
        let schema: JSONValue = .object([
            "type": .string("object"),
            "properties": .object([
                "name": .object(["type": .string("string")]),
                "age": .object(["type": .string("integer")]),
            ]),
            "required": .array([.string("name"), .string("age")]),
        ])

        let adapter = CompatibilityAdapter(
            complete: { request in
                _compatResponse(provider: "test", model: request.model, text: expectedText)
            },
            stream: { request in
                XCTAssertEqual(request.responseFormat?.kind.rawValue, "json_schema")
                XCTAssertEqual(request.responseFormat?.strict, true)

                let finish = _compatResponse(provider: "test", model: request.model, text: expectedText)
                return AsyncThrowingStream { continuation in
                    continuation.yield(StreamEvent(type: .streamStart))
                    continuation.yield(StreamEvent(type: .textStart, textId: "text_0"))
                    continuation.yield(StreamEvent(type: .textDelta, delta: #"{"name":"Alice","#, textId: "text_0"))
                    continuation.yield(StreamEvent(type: .textDelta, delta: #""age":30}"#, textId: "text_0"))
                    continuation.yield(StreamEvent(type: .textEnd, textId: "text_0"))
                    continuation.yield(
                        StreamEvent(
                            type: .finish,
                            finishReason: finish.finishReason,
                            usage: finish.usage,
                            response: finish
                        )
                    )
                    continuation.finish()
                }
            }
        )
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")

        let result = try await streamObject(
            model: "m",
            prompt: "extract",
            schema: schema,
            provider: "test",
            maxRetries: 0,
            retryPolicy: RetryPolicy(maxRetries: 0, baseDelaySeconds: 0, jitter: false),
            client: client
        )

        var objects: [JSONValue] = []
        for try await object in result {
            objects.append(object)
        }

        XCTAssertFalse(objects.isEmpty)
        let lastObject = try XCTUnwrap(objects.last?.objectValue)
        XCTAssertEqual(lastObject["name"]?.stringValue, "Alice")
        XCTAssertEqual(lastObject["age"]?.doubleValue, 30)

        let final = try await result.rawStream.response()
        XCTAssertEqual(final.text, expectedText)
    }
}
