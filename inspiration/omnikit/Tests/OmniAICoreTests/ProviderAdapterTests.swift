import Testing

import Foundation

import OmniHTTP
@testable import OmniAICore

private actor StubTransport: HTTPTransport {
    private(set) var lastSendRequest: HTTPRequest?
    private(set) var lastStreamRequest: HTTPRequest?

    private let sendHandler: @Sendable (HTTPRequest) async throws -> HTTPResponse
    private let streamHandler: @Sendable (HTTPRequest) async throws -> HTTPStreamResponse

    init(
        send: @Sendable @escaping (HTTPRequest) async throws -> HTTPResponse = { _ in HTTPResponse(statusCode: 500, headers: HTTPHeaders(), body: []) },
        stream: @Sendable @escaping (HTTPRequest) async throws -> HTTPStreamResponse = { _ in HTTPStreamResponse(statusCode: 500, headers: HTTPHeaders(), body: AsyncThrowingStream { $0.finish() }) }
    ) {
        self.sendHandler = send
        self.streamHandler = stream
    }

    func send(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPResponse {
        lastSendRequest = request
        return try await sendHandler(request)
    }

    func openStream(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPStreamResponse {
        lastStreamRequest = request
        return try await streamHandler(request)
    }

    func shutdown() async throws {}
}

private actor StubOpenAIResponsesWebSocketTransport: OpenAIResponsesWebSocketTransport {
    private(set) var callCount: Int = 0
    private(set) var lastURL: URL?
    private(set) var lastHeaders: HTTPHeaders?
    private(set) var lastCreateEvent: JSONValue?
    private(set) var lastTimeout: Duration?

    private let openHandler: @Sendable () -> AsyncThrowingStream<JSONValue, Error>

    init(open: @Sendable @escaping () -> AsyncThrowingStream<JSONValue, Error>) {
        self.openHandler = open
    }

    func openResponseEventStream(
        url: URL,
        headers: HTTPHeaders,
        createEvent: JSONValue,
        timeout: Duration?
    ) async throws -> AsyncThrowingStream<JSONValue, Error> {
        callCount += 1
        lastURL = url
        lastHeaders = headers
        lastCreateEvent = createEvent
        lastTimeout = timeout
        return openHandler()
    }
}

private func bodyBytes(_ request: HTTPRequest) -> [UInt8] {
    switch request.body {
    case .none:
        return []
    case .bytes(let b):
        return b
    }
}

private func streamFromSSE(_ s: String) -> HTTPByteStream {
    let bytes = Array(s.utf8)
    return AsyncThrowingStream { continuation in
        continuation.yield(bytes)
        continuation.finish()
    }
}

@Suite
final class ProviderAdapterTests {
    @Test
    func testOpenAIAdapterBuildsResponsesAPIRequestAndParsesUsage() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("resp_1"),
            "model": .string("gpt-5.2"),
            "output": .array([
                .object([
                    "type": .string("message"),
                    "content": .array([.object(["type": .string("output_text"), "text": .string("Hello")])]),
                ]),
            ]),
            "finish_reason": .string("stop"),
            "usage": .object([
                "prompt_tokens": .number(5),
                "completion_tokens": .number(7),
                "completion_tokens_details": .object(["reasoning_tokens": .number(2)]),
                "prompt_tokens_details": .object(["cached_tokens": .number(3)]),
            ]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = OpenAIAdapter(
            apiKey: "sk-test",
            baseURL: "https://api.openai.com/v1",
            organizationID: "org_1",
            projectID: "proj_1",
            transport: transport
        )

        let schema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "t", description: "tool", parameters: schema)

        let req = Request(
            model: "gpt-5.2",
            messages: [.system("sys"), .developer("dev"), .user("hi")],
            tools: [tool],
            toolChoice: ToolChoice(mode: .named, toolName: "t"),
            providerOptions: ["openai": .object(["foo": .string("bar")])]
        )

        let resp = try await adapter.complete(request: req)
        XCTAssertEqual(resp.text, "Hello")
        XCTAssertEqual(resp.usage.inputTokens, 5)
        XCTAssertEqual(resp.usage.outputTokens, 7)
        XCTAssertEqual(resp.usage.reasoningTokens, 2)
        XCTAssertEqual(resp.usage.cacheReadTokens, 3)

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains("/responses"))
        XCTAssertEqual(sent.headers.firstValue(for: "authorization"), "Bearer sk-test")
        XCTAssertEqual(sent.headers.firstValue(for: "openai-organization"), "org_1")
        XCTAssertEqual(sent.headers.firstValue(for: "openai-project"), "proj_1")

        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertEqual(body["model"]?.stringValue, "gpt-5.2")
        XCTAssertTrue(body["instructions"]?.stringValue?.contains("sys") ?? false)
        XCTAssertTrue(body["instructions"]?.stringValue?.contains("dev") ?? false)
        XCTAssertEqual(body["foo"]?.stringValue, "bar")
        XCTAssertNotNil(body["tools"])
        XCTAssertNotNil(body["tool_choice"])
    }

    @Test
    func testOpenAIAdapterIncludesPreviousResponseIdInHTTPRequest() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("resp_2"),
            "model": .string("gpt-5.2"),
            "output": .array([
                .object([
                    "type": .string("message"),
                    "content": .array([.object(["type": .string("output_text"), "text": .string("ok")])]),
                ]),
            ]),
            "finish_reason": .string("stop"),
            "usage": .object([
                "prompt_tokens": .number(1),
                "completion_tokens": .number(1),
            ]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)
        _ = try await adapter.complete(
            request: Request(
                model: "gpt-5.2",
                messages: [.system("sys"), .user("continue")],
                previousResponseId: "resp_prev_123"
            )
        )

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertEqual(body["previous_response_id"]?.stringValue, "resp_prev_123")
        XCTAssertTrue(body["instructions"]?.stringValue?.contains("sys") ?? false)
    }

    @Test
    func testOpenAIAdapterInjectsNativeWebSearchToolFromProviderOptions() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("resp_1"),
            "model": .string("gpt-5.2"),
            "output": .array([
                .object([
                    "type": .string("message"),
                    "content": .array([.object(["type": .string("output_text"), "text": .string("ok")])]),
                ]),
            ]),
            "finish_reason": .string("stop"),
            "usage": .object([
                "prompt_tokens": .number(1),
                "completion_tokens": .number(1),
            ]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)
        _ = try await adapter.complete(
            request: Request(
                model: "gpt-5.2",
                messages: [.user("hi")],
                providerOptions: [
                    "openai": .object([
                        OpenAIProviderOptionKeys.includeNativeWebSearch: .bool(true),
                        OpenAIProviderOptionKeys.webSearchExternalWebAccess: .bool(false),
                    ]),
                ]
            )
        )

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        let body = try JSONValue.parse(bodyBytes(sent))
        let tools = body["tools"]?.arrayValue ?? []

        XCTAssertEqual(tools.count, 1)
        XCTAssertEqual(tools.first?["type"]?.stringValue, "web_search")
        XCTAssertEqual(tools.first?["external_web_access"]?.boolValue, false)
        XCTAssertEqual(body["tool_choice"]?.stringValue, "auto")
        XCTAssertNil(body[OpenAIProviderOptionKeys.includeNativeWebSearch])
        XCTAssertNil(body[OpenAIProviderOptionKeys.webSearchExternalWebAccess])
    }

    @Test
    func testOpenAIAdapterMergesNativeWebSearchWithFunctionTools() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("resp_1"),
            "model": .string("gpt-5.2"),
            "output": .array([
                .object([
                    "type": .string("message"),
                    "content": .array([.object(["type": .string("output_text"), "text": .string("ok")])]),
                ]),
            ]),
            "finish_reason": .string("stop"),
            "usage": .object([
                "prompt_tokens": .number(1),
                "completion_tokens": .number(1),
            ]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)
        let tool = try Tool(
            name: "demo_tool",
            description: "demo",
            parameters: .object([
                "type": .string("object"),
                "properties": .object([:]),
            ])
        )

        _ = try await adapter.complete(
            request: Request(
                model: "gpt-5.2",
                messages: [.user("hi")],
                tools: [tool],
                providerOptions: [
                    "openai": .object([
                        OpenAIProviderOptionKeys.includeNativeWebSearch: .bool(true),
                        OpenAIProviderOptionKeys.webSearchExternalWebAccess: .bool(true),
                        "foo": .string("bar"),
                    ]),
                ]
            )
        )

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        let body = try JSONValue.parse(bodyBytes(sent))
        let tools = body["tools"]?.arrayValue ?? []

        XCTAssertEqual(tools.count, 2)
        XCTAssertEqual(tools[0]["type"]?.stringValue, "function")
        XCTAssertEqual(tools[0]["name"]?.stringValue, "demo_tool")
        XCTAssertEqual(tools[1]["type"]?.stringValue, "web_search")
        XCTAssertEqual(tools[1]["external_web_access"]?.boolValue, true)
        XCTAssertEqual(body["foo"]?.stringValue, "bar")
        XCTAssertNil(body[OpenAIProviderOptionKeys.includeNativeWebSearch])
        XCTAssertNil(body[OpenAIProviderOptionKeys.webSearchExternalWebAccess])
    }

    @Test
    func testOpenAIAdapterReplaysReasoningItemsAndFunctionCallIdsForToolContinuations() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("resp_1"),
            "model": .string("gpt-5.2"),
            "output": .array([
                .object([
                    "type": .string("message"),
                    "content": .array([.object(["type": .string("output_text"), "text": .string("ok")])]),
                ]),
            ]),
            "finish_reason": .string("stop"),
            "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)

        let reasoningItem: JSONValue = .object([
            "id": .string("rs_1"),
            "type": .string("reasoning"),
            "summary": .array([]),
        ])
        let toolCall = ToolCall(
            id: "call_1",
            name: "add",
            arguments: ["a": .number(2), "b": .number(2)],
            rawArguments: #"{"a":2,"b":2}"#,
            providerItemId: "fc_1"
        )

        let req = Request(
            model: "gpt-5.2",
            messages: [
                .user("Use the add tool to compute 2+2."),
                Message(role: .assistant, content: [
                    ContentPart(kind: .custom("openai_input_item"), data: reasoningItem),
                    .toolCall(toolCall),
                ]),
                .toolResult(toolCallId: "call_1", toolName: "add", content: .number(4), isError: false),
            ]
        )

        _ = try await adapter.complete(request: req)

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        let body = try JSONValue.parse(bodyBytes(sent))
        let input = body["input"]?.arrayValue ?? []

        XCTAssertEqual(input.count, 4)
        XCTAssertEqual(input[0]["type"]?.stringValue, "message")
        XCTAssertEqual(input[1]["type"]?.stringValue, "reasoning")
        XCTAssertEqual(input[1]["id"]?.stringValue, "rs_1")

        XCTAssertEqual(input[2]["type"]?.stringValue, "function_call")
        XCTAssertEqual(input[2]["id"]?.stringValue, "fc_1")
        XCTAssertEqual(input[2]["call_id"]?.stringValue, "call_1")
        XCTAssertEqual(input[2]["name"]?.stringValue, "add")

        XCTAssertEqual(input[3]["type"]?.stringValue, "function_call_output")
        XCTAssertEqual(input[3]["call_id"]?.stringValue, "call_1")
    }

    @Test
    func testImageTranslationAcrossProviders() async throws {
        let pngBytes: [UInt8] = [0x89, 0x50, 0x4E, 0x47] // "\x89PNG" header prefix
        let expectedB64 = Data(pngBytes).base64EncodedString()
        let urlImage = "https://example.com/x.png"

        let tmpURL = FileManager.default.temporaryDirectory.appendingPathComponent("omni_image_\(UUID().uuidString).png")
        try Data(pngBytes).write(to: tmpURL)
        let localPath = tmpURL.path

        // OpenAI: input_image with data: URL.
        do {
            let responseJSON: JSONValue = .object([
                "id": .string("resp_1"),
                "model": .string("gpt-5.2"),
                "output": .array([.object(["type": .string("message"), "content": .array([.object(["type": .string("output_text"), "text": .string("ok")])])])]),
                "finish_reason": .string("stop"),
                "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
            ])

            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })

            let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)
            let req = Request(
                model: "gpt-5.2",
                messages: [
                    Message(role: .user, content: [
                        .text("what"),
                        .image(ImageData(data: pngBytes, mediaType: "image/png")),
                    ]),
                ]
            )
            _ = try await adapter.complete(request: req)

            let sentOpt = await transport.lastSendRequest
            let sent = try XCTUnwrap(sentOpt)
            let body = try JSONValue.parse(bodyBytes(sent))
            let input = body["input"]?.arrayValue ?? []
            let msg = input.first(where: { $0["type"]?.stringValue == "message" })
            let parts = msg?["content"]?.arrayValue ?? []
            let img = parts.first(where: { $0["type"]?.stringValue == "input_image" })
            let url = img?["image_url"]?.stringValue ?? ""
            XCTAssertTrue(url.hasPrefix("data:image/png;base64,"))
            XCTAssertTrue(url.contains(expectedB64))
        }

        // OpenAI: URL passthrough.
        do {
            let responseJSON: JSONValue = .object([
                "id": .string("resp_1"),
                "model": .string("gpt-5.2"),
                "output": .array([.object(["type": .string("message"), "content": .array([.object(["type": .string("output_text"), "text": .string("ok")])])])]),
                "finish_reason": .string("stop"),
                "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
            ])

            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })

            let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)
            _ = try await adapter.complete(
                request: Request(
                    model: "gpt-5.2",
                    messages: [Message(role: .user, content: [.image(ImageData(url: urlImage, mediaType: "image/png"))])]
                )
            )

            let sentOpt = await transport.lastSendRequest
            let sent = try XCTUnwrap(sentOpt)
            let body = try JSONValue.parse(bodyBytes(sent))
            let input = body["input"]?.arrayValue ?? []
            let msg = input.first(where: { $0["type"]?.stringValue == "message" })
            let parts = msg?["content"]?.arrayValue ?? []
            let img = parts.first(where: { $0["type"]?.stringValue == "input_image" })
            XCTAssertEqual(img?["image_url"]?.stringValue, urlImage)
        }

        // OpenAI: local file path -> base64 data URL.
        do {
            let responseJSON: JSONValue = .object([
                "id": .string("resp_1"),
                "model": .string("gpt-5.2"),
                "output": .array([.object(["type": .string("message"), "content": .array([.object(["type": .string("output_text"), "text": .string("ok")])])])]),
                "finish_reason": .string("stop"),
                "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
            ])

            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })

            let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)
            _ = try await adapter.complete(
                request: Request(
                    model: "gpt-5.2",
                    messages: [Message(role: .user, content: [.image(ImageData(url: localPath, mediaType: "image/png"))])]
                )
            )

            let sentOpt = await transport.lastSendRequest
            let sent = try XCTUnwrap(sentOpt)
            let body = try JSONValue.parse(bodyBytes(sent))
            let input = body["input"]?.arrayValue ?? []
            let msg = input.first(where: { $0["type"]?.stringValue == "message" })
            let parts = msg?["content"]?.arrayValue ?? []
            let img = parts.first(where: { $0["type"]?.stringValue == "input_image" })
            let url = img?["image_url"]?.stringValue ?? ""
            XCTAssertTrue(url.hasPrefix("data:image/png;base64,"))
            XCTAssertTrue(url.contains(expectedB64))
        }

        // Anthropic: image source base64.
        do {
            let responseJSON: JSONValue = .object([
                "id": .string("msg_1"),
                "model": .string("claude-opus-4-6"),
                "content": .array([.object(["type": .string("text"), "text": .string("ok")])]),
                "stop_reason": .string("end_turn"),
                "usage": .object(["input_tokens": .number(1), "output_tokens": .number(1)]),
            ])

            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })

            let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)
            let req = Request(
                model: "claude-opus-4-6",
                messages: [
                    Message(role: .user, content: [
                        .text("what"),
                        .image(ImageData(data: pngBytes, mediaType: "image/png")),
                    ]),
                ]
            )
            _ = try await adapter.complete(request: req)

            let sentOpt = await transport.lastSendRequest
            let sent = try XCTUnwrap(sentOpt)
            let body = try JSONValue.parse(bodyBytes(sent))
            let msgs = body["messages"]?.arrayValue ?? []
            let first = msgs.first
            let content = first?["content"]?.arrayValue ?? []
            let img = content.first(where: { $0["type"]?.stringValue == "image" })
            let src = img?["source"]
            XCTAssertEqual(src?["type"]?.stringValue, "base64")
            XCTAssertEqual(src?["media_type"]?.stringValue, "image/png")
            XCTAssertEqual(src?["data"]?.stringValue, expectedB64)
        }

        // Anthropic: URL passthrough.
        do {
            let responseJSON: JSONValue = .object([
                "id": .string("msg_1"),
                "model": .string("claude-opus-4-6"),
                "content": .array([.object(["type": .string("text"), "text": .string("ok")])]),
                "stop_reason": .string("end_turn"),
                "usage": .object(["input_tokens": .number(1), "output_tokens": .number(1)]),
            ])

            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })

            let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)
            _ = try await adapter.complete(
                request: Request(
                    model: "claude-opus-4-6",
                    messages: [Message(role: .user, content: [.image(ImageData(url: urlImage, mediaType: "image/png"))])]
                )
            )

            let sentOpt = await transport.lastSendRequest
            let sent = try XCTUnwrap(sentOpt)
            let body = try JSONValue.parse(bodyBytes(sent))
            let msgs = body["messages"]?.arrayValue ?? []
            let content = msgs.first?["content"]?.arrayValue ?? []
            let img = content.first(where: { $0["type"]?.stringValue == "image" })
            let src = img?["source"]
            XCTAssertEqual(src?["type"]?.stringValue, "url")
            XCTAssertEqual(src?["url"]?.stringValue, urlImage)
        }

        // Anthropic: local file path -> base64.
        do {
            let responseJSON: JSONValue = .object([
                "id": .string("msg_1"),
                "model": .string("claude-opus-4-6"),
                "content": .array([.object(["type": .string("text"), "text": .string("ok")])]),
                "stop_reason": .string("end_turn"),
                "usage": .object(["input_tokens": .number(1), "output_tokens": .number(1)]),
            ])

            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })

            let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)
            _ = try await adapter.complete(
                request: Request(
                    model: "claude-opus-4-6",
                    messages: [Message(role: .user, content: [.image(ImageData(url: localPath, mediaType: "image/png"))])]
                )
            )

            let sentOpt = await transport.lastSendRequest
            let sent = try XCTUnwrap(sentOpt)
            let body = try JSONValue.parse(bodyBytes(sent))
            let msgs = body["messages"]?.arrayValue ?? []
            let content = msgs.first?["content"]?.arrayValue ?? []
            let img = content.first(where: { $0["type"]?.stringValue == "image" })
            let src = img?["source"]
            XCTAssertEqual(src?["type"]?.stringValue, "base64")
            XCTAssertEqual(src?["data"]?.stringValue, expectedB64)
        }

        // Gemini: inlineData base64.
        do {
            let responseJSON: JSONValue = .object([
                "candidates": .array([
                    .object([
                        "finishReason": .string("STOP"),
                        "content": .object(["parts": .array([.object(["text": .string("ok")])])]),
                    ]),
                ]),
                "usageMetadata": .object(["promptTokenCount": .number(1), "candidatesTokenCount": .number(1)]),
            ])

            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })

            let adapter = GeminiAdapter(apiKey: "gemini-test", transport: transport)
            let req = Request(
                model: "gemini-3-flash-preview",
                messages: [
                    Message(role: .user, content: [
                        .text("what"),
                        .image(ImageData(data: pngBytes, mediaType: "image/png")),
                    ]),
                ]
            )
            _ = try await adapter.complete(request: req)

            let sentOpt = await transport.lastSendRequest
            let sent = try XCTUnwrap(sentOpt)
            let body = try JSONValue.parse(bodyBytes(sent))
            let contents = body["contents"]?.arrayValue ?? []
            let first = contents.first
            let parts = first?["parts"]?.arrayValue ?? []
            let inline = parts.first(where: { $0["inlineData"] != nil })?["inlineData"]
            XCTAssertEqual(inline?["mimeType"]?.stringValue, "image/png")
            XCTAssertEqual(inline?["data"]?.stringValue, expectedB64)
        }

        // Gemini: URL passthrough uses fileData.
        do {
            let responseJSON: JSONValue = .object([
                "candidates": .array([
                    .object([
                        "finishReason": .string("STOP"),
                        "content": .object(["parts": .array([.object(["text": .string("ok")])])]),
                    ]),
                ]),
                "usageMetadata": .object(["promptTokenCount": .number(1), "candidatesTokenCount": .number(1)]),
            ])

            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })

            let adapter = GeminiAdapter(apiKey: "gemini-test", transport: transport)
            _ = try await adapter.complete(
                request: Request(
                    model: "gemini-3-flash-preview",
                    messages: [Message(role: .user, content: [.image(ImageData(url: urlImage, mediaType: "image/png"))])]
                )
            )

            let sentOpt = await transport.lastSendRequest
            let sent = try XCTUnwrap(sentOpt)
            let body = try JSONValue.parse(bodyBytes(sent))
            let contents = body["contents"]?.arrayValue ?? []
            let parts = contents.first?["parts"]?.arrayValue ?? []
            let file = parts.first(where: { $0["fileData"] != nil })?["fileData"]
            XCTAssertEqual(file?["mimeType"]?.stringValue, "image/png")
            XCTAssertEqual(file?["fileUri"]?.stringValue, urlImage)
        }

        // Gemini: local file path -> inlineData.
        do {
            let responseJSON: JSONValue = .object([
                "candidates": .array([
                    .object([
                        "finishReason": .string("STOP"),
                        "content": .object(["parts": .array([.object(["text": .string("ok")])])]),
                    ]),
                ]),
                "usageMetadata": .object(["promptTokenCount": .number(1), "candidatesTokenCount": .number(1)]),
            ])

            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })

            let adapter = GeminiAdapter(apiKey: "gemini-test", transport: transport)
            _ = try await adapter.complete(
                request: Request(
                    model: "gemini-3-flash-preview",
                    messages: [Message(role: .user, content: [.image(ImageData(url: localPath, mediaType: "image/png"))])]
                )
            )

            let sentOpt = await transport.lastSendRequest
            let sent = try XCTUnwrap(sentOpt)
            let body = try JSONValue.parse(bodyBytes(sent))
            let contents = body["contents"]?.arrayValue ?? []
            let parts = contents.first?["parts"]?.arrayValue ?? []
            let inline = parts.first(where: { $0["inlineData"] != nil })?["inlineData"]
            XCTAssertEqual(inline?["mimeType"]?.stringValue, "image/png")
            XCTAssertEqual(inline?["data"]?.stringValue, expectedB64)
        }
    }

    @Test
    func testOpenAIAdapterMapsRetryAfterOn429() async throws {
        let errJSON: JSONValue = .object(["error": .object(["message": .string("rate limited"), "type": .string("rate_limit")])])

        let headers: HTTPHeaders = {
            var h = HTTPHeaders()
            h.set(name: "retry-after", value: "2")
            return h
        }()

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 429, headers: headers, body: Array(try errJSON.data()))
        })

        let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)

        do {
            _ = try await adapter.complete(request: Request(model: "gpt-5.2", messages: [.user("hi")]))
            XCTFail("Expected RateLimitError")
        } catch let e as RateLimitError {
            XCTAssertEqual(e.retryAfter, 2)
        }
    }

    @Test
    func testAnthropicAdapterInjectsPromptCachingAndBetaHeaders() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("msg_1"),
            "model": .string("claude-opus-4-6"),
            "content": .array([.object(["type": .string("text"), "text": .string("Hello")])]),
            "stop_reason": .string("end_turn"),
            "usage": .object([
                "input_tokens": .number(5),
                "output_tokens": .number(7),
                "cache_read_input_tokens": .number(4),
                "cache_creation_input_tokens": .number(1),
            ]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)

        let schema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "t", description: "tool", parameters: schema)

        let req = Request(
            model: "claude-opus-4-6",
            messages: [.system("sys"), .developer("dev"), .user("a"), .user("b")],
            tools: [tool],
            providerOptions: [
                "anthropic": .object([
                    "beta_headers": .array([.string("interleaved-thinking-2025-05-14")]),
                ]),
            ]
        )

        let resp = try await adapter.complete(request: req)
        XCTAssertEqual(resp.text, "Hello")
        XCTAssertEqual(resp.usage.cacheReadTokens, 4)
        XCTAssertEqual(resp.usage.cacheWriteTokens, 1)

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains("/v1/messages"))
        XCTAssertEqual(sent.headers.firstValue(for: "x-api-key"), "anthropic-test")

        let beta = sent.headers.firstValue(for: "anthropic-beta") ?? ""
        XCTAssertTrue(beta.contains("interleaved-thinking-2025-05-14"))
        XCTAssertTrue(beta.contains("prompt-caching-2024-07-31"))

        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertNotNil(body["system"])
        XCTAssertNotNil(body["tools"])

        // Strict alternation merge: two consecutive user messages should be merged into one.
        let messages = body["messages"]?.arrayValue ?? []
        XCTAssertEqual(messages.count, 1)
        let firstContent = messages.first?["content"]?.arrayValue ?? []
        XCTAssertEqual(firstContent.count, 2)

        // Conversation prefix cache_control injected on first user block.
        XCTAssertNotNil(firstContent.first?["cache_control"])
    }

    @Test
    func testAnthropicAutoCacheCanBeDisabledViaProviderOptions() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("msg_1"),
            "model": .string("claude-opus-4-6"),
            "content": .array([.object(["type": .string("text"), "text": .string("Hello")])]),
            "stop_reason": .string("end_turn"),
            "usage": .object(["input_tokens": .number(1), "output_tokens": .number(1)]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)

        let schema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "t", description: "tool", parameters: schema)

        _ = try await adapter.complete(
            request: Request(
                model: "claude-opus-4-6",
                messages: [.system("sys"), .user("hi")],
                tools: [tool],
                providerOptions: ["anthropic": .object(["auto_cache": .bool(false)])]
            )
        )

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)

        // No prompt-caching beta header unless explicitly provided.
        XCTAssertFalse((sent.headers.firstValue(for: "anthropic-beta") ?? "").contains("prompt-caching-2024-07-31"))

        let body = try JSONValue.parse(bodyBytes(sent))
        let sys = body["system"]?.arrayValue?.first
        XCTAssertNil(sys?["cache_control"])

        let msgs = body["messages"]?.arrayValue ?? []
        let content0 = msgs.first?["content"]?.arrayValue?.first
        XCTAssertNil(content0?["cache_control"])

        let tools = body["tools"]?.arrayValue ?? []
        let t0 = tools.first
        XCTAssertNil(t0?["cache_control"])
    }

    @Test
    func testAnthropicThinkingBlocksRoundTripWithSignature() async throws {
        final class Counter: @unchecked Sendable {
            private let lock = NSLock()
            private var n: Int = 0
            func next() -> Int {
                lock.lock()
                defer { lock.unlock() }
                n += 1
                return n
            }
        }
        let counter = Counter()

        let firstResponse: JSONValue = .object([
            "id": .string("msg_1"),
            "model": .string("claude-opus-4-6"),
            "content": .array([
                .object(["type": .string("thinking"), "thinking": .string("thought"), "signature": .string("sig123")]),
                .object(["type": .string("redacted_thinking"), "data": .string("REDACTED")]),
                .object(["type": .string("text"), "text": .string("Hello")]),
            ]),
            "stop_reason": .string("end_turn"),
            "usage": .object(["input_tokens": .number(1), "output_tokens": .number(1)]),
        ])

        let secondResponse: JSONValue = .object([
            "id": .string("msg_2"),
            "model": .string("claude-opus-4-6"),
            "content": .array([.object(["type": .string("text"), "text": .string("OK")])]),
            "stop_reason": .string("end_turn"),
            "usage": .object(["input_tokens": .number(1), "output_tokens": .number(1)]),
        ])

        let transport = StubTransport(send: { _ in
            let idx = counter.next()
            let json = (idx == 1) ? firstResponse : secondResponse
            return HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try json.data()))
        })

        let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)

        let r1 = try await adapter.complete(request: Request(model: "claude-opus-4-6", messages: [.user("hi")]))
        XCTAssertNotNil(r1.reasoning)

        _ = try await adapter.complete(request: Request(model: "claude-opus-4-6", messages: [.user("hi"), r1.message]))

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        let body = try JSONValue.parse(bodyBytes(sent))
        let msgs = body["messages"]?.arrayValue ?? []

        let assistant = msgs.first(where: { $0["role"]?.stringValue == "assistant" })
        let content = assistant?["content"]?.arrayValue ?? []
        let thinking = content.first(where: { $0["type"]?.stringValue == "thinking" })
        XCTAssertEqual(thinking?["thinking"]?.stringValue, "thought")
        XCTAssertEqual(thinking?["signature"]?.stringValue, "sig123")

        let redacted = content.first(where: { $0["type"]?.stringValue == "redacted_thinking" })
        XCTAssertEqual(redacted?["data"]?.stringValue, "REDACTED")
    }

    @Test
    func testAnthropicAdapterOmitToolsWhenToolChoiceNone() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("msg_1"),
            "model": .string("claude-opus-4-6"),
            "content": .array([.object(["type": .string("text"), "text": .string("Hello")])]),
            "stop_reason": .string("end_turn"),
            "usage": .object(["input_tokens": .number(1), "output_tokens": .number(1)]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)

        let schema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "t", description: "tool", parameters: schema)

        _ = try await adapter.complete(
            request: Request(
                model: "claude-opus-4-6",
                messages: [.user("hi")],
                tools: [tool],
                toolChoice: ToolChoice(mode: .none)
            )
        )

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertNil(body["tools"])
    }

    @Test
    func testGeminiAdapterBuildsGenerateContentRequest() async throws {
        let responseJSON: JSONValue = .object([
            "candidates": .array([
                .object([
                    "finishReason": .string("STOP"),
                    "content": .object(["parts": .array([.object(["text": .string("Hello")])])]),
                ]),
            ]),
            "usageMetadata": .object([
                "promptTokenCount": .number(5),
                "candidatesTokenCount": .number(7),
                "cachedContentTokenCount": .number(2),
                "thoughtsTokenCount": .number(1),
            ]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = GeminiAdapter(apiKey: "gemini-test", baseURL: "https://generativelanguage.googleapis.com", transport: transport)

        let schema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "t", description: "tool", parameters: schema)

        let req = Request(
            model: "gemini-3-flash-preview",
            messages: [.system("sys"), .developer("dev"), .user("hi")],
            tools: [tool],
            toolChoice: ToolChoice(mode: .named, toolName: "t"),
            providerOptions: ["gemini": .object(["safetySettings": .array([])])]
        )

        let resp = try await adapter.complete(request: req)
        XCTAssertEqual(resp.text, "Hello")
        XCTAssertEqual(resp.usage.cacheReadTokens, 2)
        XCTAssertEqual(resp.usage.reasoningTokens, 1)

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains(":generateContent"))
        XCTAssertTrue(sent.url.absoluteString.contains("key=gemini-test"))

        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertNotNil(body["systemInstruction"])
        XCTAssertNotNil(body["tools"])
        XCTAssertNotNil(body["toolConfig"])
        XCTAssertNotNil(body["safetySettings"])
    }

    @Test
    func testGeminiAdapterStripsAdditionalPropertiesFromToolSchema() async throws {
        let responseJSON: JSONValue = .object([
            "candidates": .array([
                .object([
                    "finishReason": .string("STOP"),
                    "content": .object(["parts": .array([.object(["text": .string("ok")])])]),
                ]),
            ]),
            "usageMetadata": .object([
                "promptTokenCount": .number(1),
                "candidatesTokenCount": .number(1),
            ]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = GeminiAdapter(apiKey: "gemini-test", transport: transport)
        let schema: JSONValue = .object([
            "type": .string("object"),
            "properties": .object([
                "payload": .object([
                    "type": .string("object"),
                    "properties": .object(["name": .object(["type": .string("string")])]),
                    "additionalProperties": .bool(false),
                ]),
            ]),
            "additionalProperties": .bool(false),
        ])
        let tool = try Tool(name: "t", description: "tool", parameters: schema)

        _ = try await adapter.complete(
            request: Request(
                model: "gemini-3-flash-preview",
                messages: [.user("hi")],
                tools: [tool],
                toolChoice: .auto
            )
        )

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        let body = try JSONValue.parse(bodyBytes(sent))
        let params = body["tools"]?.arrayValue?.first?["functionDeclarations"]?.arrayValue?.first?["parameters"]
        XCTAssertNotNil(params)
        XCTAssertNil(params?["additionalProperties"])
        XCTAssertNil(params?["properties"]?["payload"]?["additionalProperties"])
    }

    @Test
    func testCerebrasAdapterBuildsChatCompletionsRequestAndParsesUsage() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("chatcmpl_1"),
            "model": .string("zai-glm-4.7"),
            "choices": .array([
                .object([
                    "index": .number(0),
                    "message": .object([
                        "role": .string("assistant"),
                        "content": .string("Hello"),
                    ]),
                    "finish_reason": .string("stop"),
                ]),
            ]),
            "usage": .object([
                "prompt_tokens": .number(5),
                "completion_tokens": .number(7),
                "completion_tokens_details": .object(["reasoning_tokens": .number(2)]),
                "prompt_tokens_details": .object(["cached_tokens": .number(3)]),
            ]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = CerebrasAdapter(apiKey: "cerebras-test", baseURL: "https://api.cerebras.ai/v1", transport: transport)

        let schema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "t", description: "tool", parameters: schema)
        let priorCall = ToolCall(id: "call_1", name: "add", arguments: ["a": .number(1), "b": .number(2)], rawArguments: #"{"a":1,"b":2}"#)

        let req = Request(
            model: "zai-glm-4.7",
            messages: [
                .system("sys"),
                .developer("dev"),
                .user("hi"),
                Message(role: .assistant, content: [.text("calling tool"), .toolCall(priorCall)]),
                .toolResult(toolCallId: "call_1", toolName: "add", content: .number(3), isError: false),
            ],
            tools: [tool],
            toolChoice: ToolChoice(mode: .named, toolName: "t"),
            maxTokens: 256,
            providerOptions: ["cerebras": .object(["disable_reasoning": .bool(true)])]
        )

        let resp = try await adapter.complete(request: req)
        XCTAssertEqual(resp.text, "Hello")
        XCTAssertEqual(resp.usage.inputTokens, 5)
        XCTAssertEqual(resp.usage.outputTokens, 7)
        XCTAssertEqual(resp.usage.reasoningTokens, 2)
        XCTAssertEqual(resp.usage.cacheReadTokens, 3)

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains("/chat/completions"))
        XCTAssertEqual(sent.headers.firstValue(for: "authorization"), "Bearer cerebras-test")

        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertEqual(body["model"]?.stringValue, "zai-glm-4.7")
        XCTAssertEqual(body["max_tokens"]?.doubleValue, 256)
        XCTAssertEqual(body["disable_reasoning"]?.boolValue, true)
        XCTAssertEqual(body["tool_choice"]?["type"]?.stringValue, "function")
        XCTAssertEqual(body["tool_choice"]?["function"]?["name"]?.stringValue, "t")

        let tools = body["tools"]?.arrayValue ?? []
        XCTAssertEqual(tools.first?["type"]?.stringValue, "function")
        XCTAssertEqual(tools.first?["function"]?["name"]?.stringValue, "t")

        let messages = body["messages"]?.arrayValue ?? []
        XCTAssertEqual(messages.first?["role"]?.stringValue, "system")
        XCTAssertEqual(messages[1]["role"]?.stringValue, "system")
        XCTAssertEqual(messages.last?["role"]?.stringValue, "tool")
        XCTAssertEqual(messages.last?["tool_call_id"]?.stringValue, "call_1")
    }

    @Test
    func testGroqAdapterBuildsChatCompletionsRequestAndParsesUsage() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("chatcmpl_1"),
            "model": .string("openai/gpt-oss-20b"),
            "choices": .array([
                .object([
                    "index": .number(0),
                    "message": .object([
                        "role": .string("assistant"),
                        "content": .string("Hello"),
                        "reasoning": .string("Thinking..."),
                    ]),
                    "finish_reason": .string("stop"),
                ]),
            ]),
            "usage": .object([
                "prompt_tokens": .number(5),
                "completion_tokens": .number(7),
                "completion_tokens_details": .object(["reasoning_tokens": .number(2)]),
                "prompt_tokens_details": .object(["cached_tokens": .number(3)]),
            ]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = GroqAdapter(apiKey: "groq-test", baseURL: "https://api.groq.com/openai/v1", transport: transport)

        let schema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "t", description: "tool", parameters: schema)
        let priorCall = ToolCall(id: "call_1", name: "add", arguments: ["a": .number(1), "b": .number(2)], rawArguments: #"{"a":1,"b":2}"#)

        let req = Request(
            model: "openai/gpt-oss-20b",
            messages: [
                .system("sys"),
                .developer("dev"),
                .user("hi"),
                Message(role: .assistant, content: [.text("calling tool"), .toolCall(priorCall)]),
                .toolResult(toolCallId: "call_1", toolName: "add", content: .number(3), isError: false),
            ],
            tools: [tool],
            toolChoice: ToolChoice(mode: .named, toolName: "t"),
            maxTokens: 256,
            reasoningEffort: "default",
            providerOptions: ["groq": .object(["service_tier": .string("on_demand")])]
        )

        let resp = try await adapter.complete(request: req)
        XCTAssertEqual(resp.text, "Hello")
        XCTAssertEqual(resp.reasoning, "Thinking...")
        XCTAssertEqual(resp.usage.inputTokens, 5)
        XCTAssertEqual(resp.usage.outputTokens, 7)
        XCTAssertEqual(resp.usage.reasoningTokens, 2)
        XCTAssertEqual(resp.usage.cacheReadTokens, 3)

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains("/chat/completions"))
        XCTAssertEqual(sent.headers.firstValue(for: "authorization"), "Bearer groq-test")

        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertEqual(body["model"]?.stringValue, "openai/gpt-oss-20b")
        XCTAssertEqual(body["max_tokens"]?.doubleValue, 256)
        XCTAssertEqual(body["reasoning_effort"]?.stringValue, "medium")
        XCTAssertEqual(body["include_reasoning"]?.boolValue, true)
        XCTAssertEqual(body["service_tier"]?.stringValue, "on_demand")
        XCTAssertEqual(body["tool_choice"]?["type"]?.stringValue, "function")
        XCTAssertEqual(body["tool_choice"]?["function"]?["name"]?.stringValue, "t")

        let tools = body["tools"]?.arrayValue ?? []
        XCTAssertEqual(tools.first?["type"]?.stringValue, "function")
        XCTAssertEqual(tools.first?["function"]?["name"]?.stringValue, "t")

        let messages = body["messages"]?.arrayValue ?? []
        XCTAssertEqual(messages.first?["role"]?.stringValue, "system")
        XCTAssertEqual(messages[1]["role"]?.stringValue, "system")
        XCTAssertEqual(messages.last?["role"]?.stringValue, "tool")
        XCTAssertEqual(messages.last?["tool_call_id"]?.stringValue, "call_1")
    }

    @Test
    func testGroqAdapterTranscriptionBuildsMultipartAndParsesText() async throws {
        let responseJSON: JSONValue = .object([
            "text": .string("hello world"),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = GroqAdapter(apiKey: "groq-test", transport: transport)
        let req = Request(
            model: "whisper-large-v3",
            messages: [
                Message(
                    role: .user,
                    content: [
                        .text("transcribe literally"),
                        ContentPart(kind: .standard(.audio), audio: AudioData(data: [0x52, 0x49, 0x46, 0x46], mediaType: "audio/wav")),
                    ]
                ),
            ],
            providerOptions: ["groq": .object(["language": .string("en"), "response_format": .string("json")])]
        )

        let resp = try await adapter.complete(request: req)
        XCTAssertEqual(resp.text, "hello world")
        XCTAssertEqual(resp.provider, "groq")

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains("/audio/transcriptions"))
        XCTAssertEqual(sent.headers.firstValue(for: "authorization"), "Bearer groq-test")
        XCTAssertTrue((sent.headers.firstValue(for: "content-type") ?? "").contains("multipart/form-data"))

        let bodyString = String(decoding: bodyBytes(sent), as: UTF8.self)
        XCTAssertTrue(bodyString.contains("name=\"model\""))
        XCTAssertTrue(bodyString.contains("whisper-large-v3"))
        XCTAssertTrue(bodyString.contains("name=\"file\"; filename=\"audio.wav\""))
        XCTAssertTrue(bodyString.contains("name=\"prompt\""))
        XCTAssertTrue(bodyString.contains("transcribe literally"))
        XCTAssertTrue(bodyString.contains("name=\"language\""))
        XCTAssertTrue(bodyString.contains("name=\"response_format\""))
    }

    @Test
    func testGroqAdapterSpeechParsesAudioResponse() async throws {
        let responseHeaders: HTTPHeaders = {
            var h = HTTPHeaders()
            h.set(name: "content-type", value: "audio/wav")
            return h
        }()

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: responseHeaders, body: [0x52, 0x49, 0x46, 0x46])
        })

        let adapter = GroqAdapter(apiKey: "groq-test", transport: transport)
        let req = Request(
            model: "canopylabs/orpheus-v1-english",
            messages: [.user("hello there")],
            providerOptions: ["groq": .object(["voice": .string("tara"), "response_format": .string("wav")])]
        )

        let resp = try await adapter.complete(request: req)
        XCTAssertEqual(resp.provider, "groq")
        let audioPart = resp.message.content.first(where: { $0.kind.rawValue == ContentKind.audio.rawValue })
        XCTAssertEqual(audioPart?.audio?.mediaType, "audio/wav")
        XCTAssertEqual(audioPart?.audio?.data ?? [], [0x52, 0x49, 0x46, 0x46])

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains("/audio/speech"))
        XCTAssertEqual(sent.headers.firstValue(for: "authorization"), "Bearer groq-test")

        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertEqual(body["model"]?.stringValue, "canopylabs/orpheus-v1-english")
        XCTAssertEqual(body["input"]?.stringValue, "hello there")
        XCTAssertEqual(body["voice"]?.stringValue, "tara")
        XCTAssertEqual(body["response_format"]?.stringValue, "wav")
    }

    @Test
    func testOpenAIAdapterStreamingMapsTextEvents() async throws {
        let completedJSON: JSONValue = .object([
            "type": .string("response.completed"),
            "response": .object([
                "id": .string("resp_1"),
                "model": .string("gpt-5.2"),
                "output": .array([
                    .object([
                        "type": .string("message"),
                        "content": .array([.object(["type": .string("output_text"), "text": .string("hello")])]),
                    ]),
                ]),
                "finish_reason": .string("stop"),
                "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
            ]),
        ])

        let sse = """
        event: response.output_text.delta
        data: {\"type\":\"response.output_text.delta\",\"delta\":\"he\"}

        event: response.output_text.delta
        data: {\"type\":\"response.output_text.delta\",\"delta\":\"llo\"}

        event: response.output_text.done
        data: {\"type\":\"response.output_text.done\"}

        event: response.completed
        data: \(String(data: try completedJSON.data(), encoding: .utf8)!)

        """

        let transport = StubTransport(stream: { _ in
            HTTPStreamResponse(statusCode: 200, headers: HTTPHeaders(), body: streamFromSSE(sse))
        })

        let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)
        let stream = try await adapter.stream(request: Request(model: "gpt-5.2", messages: [.user("hi")]))

        var chunks: [String] = []
        var finish: Response?
        for try await ev in stream {
            if ev.type.rawValue == StreamEventType.textDelta.rawValue {
                chunks.append(ev.delta ?? "")
            }
            if ev.type.rawValue == StreamEventType.finish.rawValue {
                finish = ev.response
            }
        }

        XCTAssertEqual(chunks.joined(), "hello")
        XCTAssertEqual(finish?.text, "hello")
        XCTAssertEqual(finish?.id, "resp_1", "Response ID should be extracted from nested response.completed payload")
    }

    @Test
    func testOpenAIAdapterStreamingUsesWebSocketModeWhenConfigured() async throws {
        let completedJSON: JSONValue = .object([
            "type": .string("response.completed"),
            "response": .object([
                "id": .string("resp_ws_1"),
                "model": .string("gpt-5.2"),
                "output": .array([
                    .object([
                        "type": .string("message"),
                        "content": .array([.object(["type": .string("output_text"), "text": .string("hello")])]),
                    ]),
                ]),
                "finish_reason": .string("stop"),
                "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
            ]),
        ])

        let wsTransport = StubOpenAIResponsesWebSocketTransport(open: {
            AsyncThrowingStream { continuation in
                continuation.yield(.object(["type": .string("response.output_text.delta"), "delta": .string("he")]))
                continuation.yield(.object(["type": .string("response.output_text.delta"), "delta": .string("llo")]))
                continuation.yield(completedJSON)
                continuation.finish()
            }
        })

        let transport = StubTransport(stream: { _ in
            XCTFail("Expected websocket transport path, not SSE stream path")
            return HTTPStreamResponse(statusCode: 500, headers: HTTPHeaders(), body: AsyncThrowingStream { $0.finish() })
        })

        let adapter = OpenAIAdapter(
            apiKey: "sk-test",
            transport: transport,
            responsesWebSocketTransport: wsTransport
        )
        let request = Request(
            model: "gpt-5.2",
            messages: [.system("sys"), .user("hi")],
            providerOptions: [
                "openai": .object([
                    OpenAIProviderOptionKeys.responsesTransport: .string("websocket"),
                    OpenAIProviderOptionKeys.websocketBaseURL: .string("https://api.openai.com/v1"),
                ]),
            ]
        )

        let stream = try await adapter.stream(request: request)

        var chunks: [String] = []
        var finish: Response?
        for try await ev in stream {
            if ev.type.rawValue == StreamEventType.textDelta.rawValue {
                chunks.append(ev.delta ?? "")
            }
            if ev.type.rawValue == StreamEventType.finish.rawValue {
                finish = ev.response
            }
        }

        XCTAssertEqual(chunks.joined(), "hello")
        XCTAssertEqual(finish?.text, "hello")
        let streamRequest = await transport.lastStreamRequest
        XCTAssertNil(streamRequest)
        let webSocketCallCount = await wsTransport.callCount
        XCTAssertEqual(webSocketCallCount, 1)

        let capturedURL = await wsTransport.lastURL
        XCTAssertEqual(capturedURL?.absoluteString, "wss://api.openai.com/v1/responses")

        let capturedHeaders = await wsTransport.lastHeaders
        XCTAssertEqual(capturedHeaders?.firstValue(for: "authorization"), "Bearer sk-test")
        XCTAssertEqual(capturedHeaders?.firstValue(for: "content-type"), "application/json")

        let createEventOpt = await wsTransport.lastCreateEvent
        let createEvent = try XCTUnwrap(createEventOpt)
        XCTAssertEqual(createEvent["type"]?.stringValue, "response.create")
        XCTAssertEqual(createEvent["stream"]?.boolValue, true)
        XCTAssertEqual(createEvent["model"]?.stringValue, "gpt-5.2")
        XCTAssertTrue(createEvent["instructions"]?.stringValue?.contains("sys") ?? false)
        XCTAssertNil(createEvent[OpenAIProviderOptionKeys.responsesTransport])
        XCTAssertNil(createEvent[OpenAIProviderOptionKeys.websocketBaseURL])
    }

    @Test
    func testOpenAIAdapterStreamingViaWebSocketIncludesPreviousResponseId() async throws {
        let completedJSON: JSONValue = .object([
            "type": .string("response.completed"),
            "response": .object([
                "id": .string("resp_ws_prev"),
                "model": .string("gpt-5.2"),
                "output": .array([
                    .object([
                        "type": .string("message"),
                        "content": .array([.object(["type": .string("output_text"), "text": .string("ok")])]),
                    ]),
                ]),
                "finish_reason": .string("stop"),
                "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
            ]),
        ])

        let wsTransport = StubOpenAIResponsesWebSocketTransport(open: {
            AsyncThrowingStream { continuation in
                continuation.yield(completedJSON)
                continuation.finish()
            }
        })

        let transport = StubTransport(stream: { _ in
            XCTFail("Expected websocket transport path, not SSE stream path")
            return HTTPStreamResponse(statusCode: 500, headers: HTTPHeaders(), body: AsyncThrowingStream { $0.finish() })
        })

        let adapter = OpenAIAdapter(
            apiKey: "sk-test",
            transport: transport,
            responsesWebSocketTransport: wsTransport
        )

        let request = Request(
            model: "gpt-5.2",
            messages: [.system("sys"), .user("continue")],
            previousResponseId: "resp_prev_ws_1",
            providerOptions: [
                "openai": .object([
                    OpenAIProviderOptionKeys.responsesTransport: .string("websocket"),
                ]),
            ]
        )

        let stream = try await adapter.stream(request: request)
        for try await _ in stream {}
        let createEventOpt = await wsTransport.lastCreateEvent
        let createEvent = try XCTUnwrap(createEventOpt)
        XCTAssertEqual(createEvent["previous_response_id"]?.stringValue, "resp_prev_ws_1")
        XCTAssertTrue(createEvent["instructions"]?.stringValue?.contains("sys") ?? false)
    }

    @Test
    func testOpenAIAdapterStreamingViaWebSocketMapsErrorEvents() async throws {
        let wsTransport = StubOpenAIResponsesWebSocketTransport(open: {
            AsyncThrowingStream { continuation in
                continuation.yield(
                    .object([
                        "type": .string("response.error"),
                        "error": .object([
                            "message": .string("bad request"),
                            "type": .string("invalid_request_error"),
                        ]),
                    ])
                )
                continuation.finish()
            }
        })

        let transport = StubTransport(stream: { _ in
            XCTFail("Expected websocket transport path, not SSE stream path")
            return HTTPStreamResponse(statusCode: 500, headers: HTTPHeaders(), body: AsyncThrowingStream { $0.finish() })
        })

        let adapter = OpenAIAdapter(
            apiKey: "sk-test",
            transport: transport,
            responsesWebSocketTransport: wsTransport
        )
        let request = Request(
            model: "gpt-5.2",
            messages: [.user("hi")],
            providerOptions: [
                "openai": .object([
                    OpenAIProviderOptionKeys.responsesTransport: .string("websocket"),
                ]),
            ]
        )

        let stream = try await adapter.stream(request: request)

        do {
            for try await _ in stream {}
            XCTFail("Expected stream to throw on websocket error event")
        } catch let err as ProviderError {
            XCTAssertEqual(err.provider, "openai")
            XCTAssertEqual(err.message, "bad request")
            XCTAssertEqual(err.errorCode, "invalid_request_error")
            XCTAssertNil(err.statusCode)
        }
    }

    @Test
    func testAnthropicAdapterStreamingMapsTextEvents() async throws {
        let sse = """
        event: message_start
        data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-opus-4-6\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}

        event: content_block_start
        data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\"}}

        event: content_block_delta
        data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"he\"}}

        event: content_block_delta
        data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"llo\"}}

        event: content_block_stop
        data: {\"type\":\"content_block_stop\",\"index\":0}

        event: message_delta
        data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":7}}

        event: message_stop
        data: {\"type\":\"message_stop\"}

        """

        let transport = StubTransport(stream: { _ in
            HTTPStreamResponse(statusCode: 200, headers: HTTPHeaders(), body: streamFromSSE(sse))
        })

        let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)
        let stream = try await adapter.stream(request: Request(model: "claude-opus-4-6", messages: [.user("hi")]))

        var chunks: [String] = []
        var finish: Response?
        for try await ev in stream {
            if ev.type.rawValue == StreamEventType.textDelta.rawValue {
                chunks.append(ev.delta ?? "")
            }
            if ev.type.rawValue == StreamEventType.finish.rawValue {
                finish = ev.response
            }
        }

        XCTAssertEqual(chunks.joined(), "hello")
        XCTAssertEqual(finish?.text, "hello")
        XCTAssertEqual(finish?.finishReason.rawValue, "stop")
    }

    @Test
    func testGeminiAdapterStreamingMapsTextEvents() async throws {
        let sse = """
        data: {\"candidates\":[{\"finishReason\":\"STOP\",\"content\":{\"parts\":[{\"text\":\"he\"}]}}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":7}}

        data: {\"candidates\":[{\"finishReason\":\"STOP\",\"content\":{\"parts\":[{\"text\":\"llo\"}]}}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":7}}

        """

        let transport = StubTransport(stream: { _ in
            HTTPStreamResponse(statusCode: 200, headers: HTTPHeaders(), body: streamFromSSE(sse))
        })

        let adapter = GeminiAdapter(apiKey: "gemini-test", transport: transport)
        let stream = try await adapter.stream(request: Request(model: "gemini-3-flash-preview", messages: [.user("hi")]))

        var chunks: [String] = []
        var finish: Response?
        for try await ev in stream {
            if ev.type.rawValue == StreamEventType.textDelta.rawValue {
                chunks.append(ev.delta ?? "")
            }
            if ev.type.rawValue == StreamEventType.finish.rawValue {
                finish = ev.response
            }
        }

        XCTAssertEqual(chunks.joined(), "hello")
        XCTAssertEqual(finish?.text, "hello")
    }

    @Test
    func testGeminiAdapterStreamingFallsBackToCompleteWhenStreamingUnavailable() async throws {
        let responseJSON: JSONValue = .object([
            "candidates": .array([
                .object([
                    "finishReason": .string("STOP"),
                    "content": .object([
                        "parts": .array([
                            .object(["text": .string("hello")]),
                        ]),
                    ]),
                ]),
            ]),
            "usageMetadata": .object([
                "promptTokenCount": .number(5),
                "candidatesTokenCount": .number(7),
            ]),
        ])

        let transport = StubTransport(
            send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            },
            stream: { _ in
                throw OmniHTTPError.streamingNotSupported
            }
        )

        let adapter = GeminiAdapter(apiKey: "gemini-test", transport: transport)
        let stream = try await adapter.stream(request: Request(model: "gemini-3-flash-preview", messages: [.user("hi")]))

        var chunks: [String] = []
        var finish: Response?
        for try await ev in stream {
            if ev.type.rawValue == StreamEventType.textDelta.rawValue {
                chunks.append(ev.delta ?? "")
            }
            if ev.type.rawValue == StreamEventType.finish.rawValue {
                finish = ev.response
            }
        }

        XCTAssertEqual(chunks.joined(), "hello")
        XCTAssertEqual(finish?.text, "hello")
        let streamRequest = await transport.lastStreamRequest
        let sendRequest = await transport.lastSendRequest
        XCTAssertNotNil(streamRequest)
        XCTAssertNotNil(sendRequest)
    }

    @Test
    func testCerebrasAdapterStreamingMapsTextAndToolCallEvents() async throws {
        let sse = """
        data: {\"id\":\"chatcmpl_1\",\"model\":\"zai-glm-4.7\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"he\"},\"finish_reason\":null}]}

        data: {\"id\":\"chatcmpl_1\",\"model\":\"zai-glm-4.7\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"llo\"},\"finish_reason\":null}]}

        data: {\"id\":\"chatcmpl_1\",\"model\":\"zai-glm-4.7\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"add\",\"arguments\":\"{\\\"a\\\":1\"}}]},\"finish_reason\":null}]}

        data: {\"id\":\"chatcmpl_1\",\"model\":\"zai-glm-4.7\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\",\\\"b\\\":2}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":7}}

        data: [DONE]

        """

        let transport = StubTransport(stream: { _ in
            HTTPStreamResponse(statusCode: 200, headers: HTTPHeaders(), body: streamFromSSE(sse))
        })

        let adapter = CerebrasAdapter(apiKey: "cerebras-test", transport: transport)
        let stream = try await adapter.stream(request: Request(model: "zai-glm-4.7", messages: [.user("hi")]))

        var chunks: [String] = []
        var seenToolStart = false
        var seenToolDelta = false
        var seenToolEnd = false
        var finish: Response?

        for try await ev in stream {
            if ev.type.rawValue == StreamEventType.textDelta.rawValue {
                chunks.append(ev.delta ?? "")
            }
            if ev.type.rawValue == StreamEventType.toolCallStart.rawValue {
                seenToolStart = true
                XCTAssertEqual(ev.toolCall?.id, "call_1")
                XCTAssertEqual(ev.toolCall?.name, "add")
            }
            if ev.type.rawValue == StreamEventType.toolCallDelta.rawValue {
                seenToolDelta = true
                XCTAssertTrue((ev.toolCall?.rawArguments ?? "").contains("\"a\""))
            }
            if ev.type.rawValue == StreamEventType.toolCallEnd.rawValue {
                seenToolEnd = true
                XCTAssertEqual(ev.toolCall?.arguments["a"]?.doubleValue, 1)
                XCTAssertEqual(ev.toolCall?.arguments["b"]?.doubleValue, 2)
            }
            if ev.type.rawValue == StreamEventType.finish.rawValue {
                finish = ev.response
            }
        }

        XCTAssertEqual(chunks.joined(), "hello")
        XCTAssertTrue(seenToolStart)
        XCTAssertTrue(seenToolDelta)
        XCTAssertTrue(seenToolEnd)
        XCTAssertEqual(finish?.finishReason.rawValue, "tool_calls")
        XCTAssertEqual(finish?.toolCalls.first?.name, "add")
    }

    @Test
    func testGroqAdapterStreamingMapsTextReasoningAndToolCallEvents() async throws {
        let sse = """
        data: {\"id\":\"chatcmpl_1\",\"model\":\"openai/gpt-oss-20b\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"User \"},\"finish_reason\":null}]}

        data: {\"id\":\"chatcmpl_1\",\"model\":\"openai/gpt-oss-20b\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"asks.\"},\"finish_reason\":null}]}

        data: {\"id\":\"chatcmpl_1\",\"model\":\"openai/gpt-oss-20b\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"he\"},\"finish_reason\":null}]}

        data: {\"id\":\"chatcmpl_1\",\"model\":\"openai/gpt-oss-20b\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"llo\"},\"finish_reason\":null}]}

        data: {\"id\":\"chatcmpl_1\",\"model\":\"openai/gpt-oss-20b\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"add\",\"arguments\":\"{\\\"a\\\":1\"}}]},\"finish_reason\":null}]}

        data: {\"id\":\"chatcmpl_1\",\"model\":\"openai/gpt-oss-20b\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\",\\\"b\\\":2}\"}}]},\"finish_reason\":\"tool_calls\"}],\"x_groq\":{\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":7,\"completion_tokens_details\":{\"reasoning_tokens\":2}}}}

        data: [DONE]

        """

        let transport = StubTransport(stream: { _ in
            HTTPStreamResponse(statusCode: 200, headers: HTTPHeaders(), body: streamFromSSE(sse))
        })

        let adapter = GroqAdapter(apiKey: "groq-test", transport: transport)
        let stream = try await adapter.stream(request: Request(model: "openai/gpt-oss-20b", messages: [.user("hi")]))

        var chunks: [String] = []
        var reasoningChunks: [String] = []
        var seenToolStart = false
        var seenToolDelta = false
        var seenToolEnd = false
        var finish: Response?

        for try await ev in stream {
            if ev.type.rawValue == StreamEventType.textDelta.rawValue {
                chunks.append(ev.delta ?? "")
            }
            if ev.type.rawValue == StreamEventType.reasoningDelta.rawValue {
                reasoningChunks.append(ev.reasoningDelta ?? "")
            }
            if ev.type.rawValue == StreamEventType.toolCallStart.rawValue {
                seenToolStart = true
                XCTAssertEqual(ev.toolCall?.id, "call_1")
                XCTAssertEqual(ev.toolCall?.name, "add")
            }
            if ev.type.rawValue == StreamEventType.toolCallDelta.rawValue {
                seenToolDelta = true
                XCTAssertTrue((ev.toolCall?.rawArguments ?? "").contains("\"a\""))
            }
            if ev.type.rawValue == StreamEventType.toolCallEnd.rawValue {
                seenToolEnd = true
                XCTAssertEqual(ev.toolCall?.arguments["a"]?.doubleValue, 1)
                XCTAssertEqual(ev.toolCall?.arguments["b"]?.doubleValue, 2)
            }
            if ev.type.rawValue == StreamEventType.finish.rawValue {
                finish = ev.response
            }
        }

        XCTAssertEqual(chunks.joined(), "hello")
        XCTAssertEqual(reasoningChunks.joined(), "User asks.")
        XCTAssertTrue(seenToolStart)
        XCTAssertTrue(seenToolDelta)
        XCTAssertTrue(seenToolEnd)
        XCTAssertEqual(finish?.finishReason.rawValue, "tool_calls")
        XCTAssertEqual(finish?.usage.reasoningTokens, 2)
        XCTAssertEqual(finish?.toolCalls.first?.name, "add")
    }

    @Test
    func testOpenAIAdapterTranslatesAllRolesAndToolRoundTrip() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("resp_1"),
            "model": .string("gpt-5.2"),
            "output": .array([.object(["type": .string("message"), "content": .array([.object(["type": .string("output_text"), "text": .string("ok")])])])]),
            "finish_reason": .string("stop"),
            "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)

        let call = ToolCall(id: "call_1", name: "add", arguments: ["a": .number(1), "b": .number(2)], rawArguments: nil)
        let toolResultMsg = Message.toolResult(toolCallId: call.id, toolName: call.name, content: .string("4"), isError: false)

        _ = try await adapter.complete(
            request: Request(
                model: "gpt-5.2",
                messages: [
                    .system("sys"),
                    .developer("dev"),
                    Message(role: .user, content: [.text("u")]),
                    Message(role: .assistant, content: [.text("a"), .toolCall(call)]),
                    toolResultMsg,
                ]
            )
        )

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        let body = try JSONValue.parse(bodyBytes(sent))

        XCTAssertTrue(body["instructions"]?.stringValue?.contains("sys") ?? false)
        XCTAssertTrue(body["instructions"]?.stringValue?.contains("dev") ?? false)

        let input = body["input"]?.arrayValue ?? []
        let userMsg = input.first(where: { $0["type"]?.stringValue == "message" && $0["role"]?.stringValue == "user" })
        XCTAssertNotNil(userMsg)

        let assistantMsg = input.first(where: { $0["type"]?.stringValue == "message" && $0["role"]?.stringValue == "assistant" })
        let assistantParts = assistantMsg?["content"]?.arrayValue ?? []
        XCTAssertNotNil(assistantParts.first(where: { $0["type"]?.stringValue == "output_text" && $0["text"]?.stringValue == "a" }))

        let fnCall = input.first(where: { $0["type"]?.stringValue == "function_call" })
        XCTAssertEqual(fnCall?["call_id"]?.stringValue, "call_1")
        XCTAssertEqual(fnCall?["name"]?.stringValue, "add")
        XCTAssertTrue((fnCall?["arguments"]?.stringValue ?? "").contains("\"a\""))

        let fnOut = input.first(where: { $0["type"]?.stringValue == "function_call_output" })
        XCTAssertEqual(fnOut?["call_id"]?.stringValue, "call_1")
        XCTAssertEqual(fnOut?["output"]?.stringValue, "4")
    }

    @Test
    func testAnthropicAdapterTranslatesAllRolesAndToolRoundTrip() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("msg_1"),
            "model": .string("claude-opus-4-6"),
            "content": .array([.object(["type": .string("text"), "text": .string("ok")])]),
            "stop_reason": .string("end_turn"),
            "usage": .object(["input_tokens": .number(1), "output_tokens": .number(1)]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)

        let call = ToolCall(id: "tool_1", name: "add", arguments: ["a": .number(1), "b": .number(2)], rawArguments: nil)
        let toolResultMsg = Message.toolResult(toolCallId: call.id, toolName: call.name, content: .string("4"), isError: false)

        _ = try await adapter.complete(
            request: Request(
                model: "claude-opus-4-6",
                messages: [
                    .system("sys"),
                    .developer("dev"),
                    Message(role: .user, content: [.text("u")]),
                    Message(role: .assistant, content: [.text("a"), .toolCall(call)]),
                    toolResultMsg,
                ]
            )
        )

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        let body = try JSONValue.parse(bodyBytes(sent))

        let systemBlocks = body["system"]?.arrayValue ?? []
        let sysText = systemBlocks.first?["text"]?.stringValue ?? ""
        XCTAssertTrue(sysText.contains("sys"))
        XCTAssertTrue(sysText.contains("dev"))

        let messages = body["messages"]?.arrayValue ?? []
        XCTAssertEqual(messages.count, 3)

        XCTAssertEqual(messages[0]["role"]?.stringValue, "user")
        XCTAssertEqual(messages[0]["content"]?.arrayValue?.first?["type"]?.stringValue, "text")

        XCTAssertEqual(messages[1]["role"]?.stringValue, "assistant")
        let aContent = messages[1]["content"]?.arrayValue ?? []
        XCTAssertNotNil(aContent.first(where: { $0["type"]?.stringValue == "tool_use" && $0["id"]?.stringValue == "tool_1" && $0["name"]?.stringValue == "add" }))

        XCTAssertEqual(messages[2]["role"]?.stringValue, "user")
        let u2Content = messages[2]["content"]?.arrayValue ?? []
        let tr = u2Content.first(where: { $0["type"]?.stringValue == "tool_result" })
        XCTAssertEqual(tr?["tool_use_id"]?.stringValue, "tool_1")
        XCTAssertEqual(tr?["is_error"]?.boolValue, false)
    }

    @Test
    func testGeminiAdapterTranslatesAllRolesAndToolRoundTrip() async throws {
        let responseJSON: JSONValue = .object([
            "candidates": .array([
                .object([
                    "finishReason": .string("STOP"),
                    "content": .object(["parts": .array([.object(["text": .string("ok")])])]),
                ]),
            ]),
            "usageMetadata": .object(["promptTokenCount": .number(1), "candidatesTokenCount": .number(1)]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = GeminiAdapter(apiKey: "gemini-test", transport: transport)

        let call = ToolCall(id: "tool_1", name: "add", arguments: ["a": .number(1), "b": .number(2)], rawArguments: nil)
        let toolResultMsg = Message.toolResult(toolCallId: call.id, toolName: call.name, content: .object(["sum": .number(4)]), isError: false)

        _ = try await adapter.complete(
            request: Request(
                model: "gemini-3-flash-preview",
                messages: [
                    .system("sys"),
                    .developer("dev"),
                    Message(role: .user, content: [.text("u")]),
                    Message(role: .assistant, content: [.text("a"), .toolCall(call)]),
                    toolResultMsg,
                ]
            )
        )

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        let body = try JSONValue.parse(bodyBytes(sent))

        let sysParts = body["systemInstruction"]?["parts"]?.arrayValue ?? []
        XCTAssertEqual(sysParts.count, 2)

        let contents = body["contents"]?.arrayValue ?? []
        XCTAssertEqual(contents.count, 3)

        XCTAssertEqual(contents[0]["role"]?.stringValue, "user")
        XCTAssertEqual(contents[1]["role"]?.stringValue, "model")
        XCTAssertEqual(contents[2]["role"]?.stringValue, "user")

        let modelParts = contents[1]["parts"]?.arrayValue ?? []
        let fc = modelParts.first(where: { $0["functionCall"] != nil })?["functionCall"]
        XCTAssertEqual(fc?["name"]?.stringValue, "add")
        XCTAssertEqual(fc?["args"]?["a"]?.doubleValue, 1)

        let user2Parts = contents[2]["parts"]?.arrayValue ?? []
        let fr = user2Parts.first(where: { $0["functionResponse"] != nil })?["functionResponse"]
        XCTAssertEqual(fr?["name"]?.stringValue, "add")
        XCTAssertEqual(fr?["response"]?["sum"]?.doubleValue, 4)
    }

    @Test
    func testAnthropicProviderOptionsPassThrough() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("msg_1"),
            "model": .string("claude-opus-4-6"),
            "content": .array([.object(["type": .string("text"), "text": .string("ok")])]),
            "stop_reason": .string("end_turn"),
            "usage": .object(["input_tokens": .number(1), "output_tokens": .number(1)]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)

        _ = try await adapter.complete(
            request: Request(
                model: "claude-opus-4-6",
                messages: [.user("hi")],
                providerOptions: ["anthropic": .object(["foo": .string("bar")])]
            )
        )

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertEqual(body["foo"]?.stringValue, "bar")
    }

    @Test
    func testToolChoiceTranslationAcrossProviders() async throws {
        let schema: JSONValue = .object(["type": .string("object")])
        let tool = try Tool(name: "t", description: "tool", parameters: schema)

        // OpenAI
        do {
            let responseJSON: JSONValue = .object([
                "id": .string("resp_1"),
                "model": .string("gpt-5.2"),
                "output": .array([.object(["type": .string("message"), "content": .array([.object(["type": .string("output_text"), "text": .string("ok")])])])]),
                "finish_reason": .string("stop"),
                "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
            ])
            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })
            let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)

            func lastBody() async throws -> JSONValue {
                let sentOpt = await transport.lastSendRequest
                let sent = try XCTUnwrap(sentOpt)
                return try JSONValue.parse(bodyBytes(sent))
            }

            _ = try await adapter.complete(request: Request(model: "gpt-5.2", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .auto)))
            var body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?.stringValue, "auto")

            _ = try await adapter.complete(request: Request(model: "gpt-5.2", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .none)))
            body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?.stringValue, "none")

            _ = try await adapter.complete(request: Request(model: "gpt-5.2", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .required)))
            body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?.stringValue, "required")

            _ = try await adapter.complete(request: Request(model: "gpt-5.2", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .named, toolName: "t")))
            body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?["type"]?.stringValue, "function")
            XCTAssertEqual(body["tool_choice"]?["name"]?.stringValue, "t")
        }

        // Anthropic
        do {
            let responseJSON: JSONValue = .object([
                "id": .string("msg_1"),
                "model": .string("claude-opus-4-6"),
                "content": .array([.object(["type": .string("text"), "text": .string("ok")])]),
                "stop_reason": .string("end_turn"),
                "usage": .object(["input_tokens": .number(1), "output_tokens": .number(1)]),
            ])
            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })
            let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)

            func lastBody() async throws -> JSONValue {
                let sentOpt = await transport.lastSendRequest
                let sent = try XCTUnwrap(sentOpt)
                return try JSONValue.parse(bodyBytes(sent))
            }

            _ = try await adapter.complete(request: Request(model: "claude-opus-4-6", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .auto)))
            var body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?["type"]?.stringValue, "auto")

            _ = try await adapter.complete(request: Request(model: "claude-opus-4-6", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .required)))
            body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?["type"]?.stringValue, "any")

            _ = try await adapter.complete(request: Request(model: "claude-opus-4-6", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .named, toolName: "t")))
            body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?["type"]?.stringValue, "tool")
            XCTAssertEqual(body["tool_choice"]?["name"]?.stringValue, "t")

            _ = try await adapter.complete(request: Request(model: "claude-opus-4-6", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .none)))
            body = try await lastBody()
            XCTAssertNil(body["tools"])
        }

        // Gemini
        do {
            let responseJSON: JSONValue = .object([
                "candidates": .array([
                    .object([
                        "finishReason": .string("STOP"),
                        "content": .object(["parts": .array([.object(["text": .string("ok")])])]),
                    ]),
                ]),
                "usageMetadata": .object(["promptTokenCount": .number(1), "candidatesTokenCount": .number(1)]),
            ])
            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })
            let adapter = GeminiAdapter(apiKey: "gemini-test", transport: transport)

            func lastBody() async throws -> JSONValue {
                let sentOpt = await transport.lastSendRequest
                let sent = try XCTUnwrap(sentOpt)
                return try JSONValue.parse(bodyBytes(sent))
            }

            _ = try await adapter.complete(request: Request(model: "gemini-3-flash-preview", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .auto)))
            var body = try await lastBody()
            XCTAssertEqual(body["toolConfig"]?["functionCallingConfig"]?["mode"]?.stringValue, "AUTO")

            _ = try await adapter.complete(request: Request(model: "gemini-3-flash-preview", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .none)))
            body = try await lastBody()
            XCTAssertEqual(body["toolConfig"]?["functionCallingConfig"]?["mode"]?.stringValue, "NONE")

            _ = try await adapter.complete(request: Request(model: "gemini-3-flash-preview", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .required)))
            body = try await lastBody()
            XCTAssertEqual(body["toolConfig"]?["functionCallingConfig"]?["mode"]?.stringValue, "ANY")

            _ = try await adapter.complete(request: Request(model: "gemini-3-flash-preview", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .named, toolName: "t")))
            body = try await lastBody()
            XCTAssertEqual(body["toolConfig"]?["functionCallingConfig"]?["mode"]?.stringValue, "ANY")
            let allowed = body["toolConfig"]?["functionCallingConfig"]?["allowedFunctionNames"]?.arrayValue?.compactMap { $0.stringValue } ?? []
            XCTAssertEqual(allowed, ["t"])
        }

        // Cerebras
        do {
            let responseJSON: JSONValue = .object([
                "id": .string("chatcmpl_1"),
                "model": .string("zai-glm-4.7"),
                "choices": .array([
                    .object([
                        "index": .number(0),
                        "message": .object([
                            "role": .string("assistant"),
                            "content": .string("ok"),
                        ]),
                        "finish_reason": .string("stop"),
                    ]),
                ]),
                "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
            ])
            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })
            let adapter = CerebrasAdapter(apiKey: "cerebras-test", transport: transport)

            func lastBody() async throws -> JSONValue {
                let sentOpt = await transport.lastSendRequest
                let sent = try XCTUnwrap(sentOpt)
                return try JSONValue.parse(bodyBytes(sent))
            }

            _ = try await adapter.complete(request: Request(model: "zai-glm-4.7", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .auto)))
            var body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?.stringValue, "auto")

            _ = try await adapter.complete(request: Request(model: "zai-glm-4.7", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .none)))
            body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?.stringValue, "none")

            _ = try await adapter.complete(request: Request(model: "zai-glm-4.7", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .required)))
            body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?.stringValue, "required")

            _ = try await adapter.complete(request: Request(model: "zai-glm-4.7", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .named, toolName: "t")))
            body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?["type"]?.stringValue, "function")
            XCTAssertEqual(body["tool_choice"]?["function"]?["name"]?.stringValue, "t")
        }

        // Groq
        do {
            let responseJSON: JSONValue = .object([
                "id": .string("chatcmpl_1"),
                "model": .string("openai/gpt-oss-20b"),
                "choices": .array([
                    .object([
                        "index": .number(0),
                        "message": .object([
                            "role": .string("assistant"),
                            "content": .string("ok"),
                        ]),
                        "finish_reason": .string("stop"),
                    ]),
                ]),
                "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
            ])
            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
            })
            let adapter = GroqAdapter(apiKey: "groq-test", transport: transport)

            func lastBody() async throws -> JSONValue {
                let sentOpt = await transport.lastSendRequest
                let sent = try XCTUnwrap(sentOpt)
                return try JSONValue.parse(bodyBytes(sent))
            }

            _ = try await adapter.complete(request: Request(model: "openai/gpt-oss-20b", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .auto)))
            var body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?.stringValue, "auto")

            _ = try await adapter.complete(request: Request(model: "openai/gpt-oss-20b", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .none)))
            body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?.stringValue, "none")

            _ = try await adapter.complete(request: Request(model: "openai/gpt-oss-20b", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .required)))
            body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?.stringValue, "required")

            _ = try await adapter.complete(request: Request(model: "openai/gpt-oss-20b", messages: [.user("hi")], tools: [tool], toolChoice: ToolChoice(mode: .named, toolName: "t")))
            body = try await lastBody()
            XCTAssertEqual(body["tool_choice"]?["type"]?.stringValue, "function")
            XCTAssertEqual(body["tool_choice"]?["function"]?["name"]?.stringValue, "t")
        }
    }

    @Test
    func testAdaptersGracefullyRejectAudioAndDocuments() async throws {
        let transport = StubTransport(send: { _ in
            XCTFail("Transport should not be used for unsupported content kinds")
            return HTTPResponse(statusCode: 500, headers: HTTPHeaders(), body: [])
        })

        let audioPart = ContentPart(kind: .standard(.audio), audio: AudioData(data: [0x00], mediaType: "audio/wav"))
        let docPart = ContentPart(kind: .standard(.document), document: DocumentData(data: [0x00], mediaType: "application/pdf", fileName: "x.pdf"))

        let adapters: [(String, (Request) async throws -> Response)] = [
            ("openai", { req in try await OpenAIAdapter(apiKey: "sk", transport: transport).complete(request: req) }),
            ("anthropic", { req in try await AnthropicAdapter(apiKey: "ak", transport: transport).complete(request: req) }),
            ("gemini", { req in try await GeminiAdapter(apiKey: "gk", transport: transport).complete(request: req) }),
            ("cerebras", { req in try await CerebrasAdapter(apiKey: "ck", transport: transport).complete(request: req) }),
            ("groq", { req in try await GroqAdapter(apiKey: "groq-test", transport: transport).complete(request: req) }),
        ]

        for (name, complete) in adapters {
            do {
                _ = try await complete(Request(model: "m", messages: [Message(role: .user, content: [audioPart])]))
                XCTFail("Expected InvalidRequestError for audio (\(name))")
            } catch is InvalidRequestError {
                // ok
            }

            do {
                _ = try await complete(Request(model: "m", messages: [Message(role: .user, content: [docPart])]))
                XCTFail("Expected InvalidRequestError for document (\(name))")
            } catch is InvalidRequestError {
                // ok
            }
        }
    }

    @Test
    func testRetryAfterIsParsedForAnthropicGeminiCerebrasAndGroqErrors() async throws {
        let headers: HTTPHeaders = {
            var h = HTTPHeaders()
            h.set(name: "retry-after", value: "3")
            return h
        }()

        // Anthropic 429
        do {
            let errJSON: JSONValue = .object(["error": .object(["type": .string("rate_limit_error"), "message": .string("rate limited")])])
            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 429, headers: headers, body: Array(try errJSON.data()))
            })
            let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)
            do {
                _ = try await adapter.complete(request: Request(model: "claude-opus-4-6", messages: [.user("hi")]))
                XCTFail("Expected RateLimitError")
            } catch let e as RateLimitError {
                XCTAssertEqual(e.retryAfter, 3)
            }
        }

        // Gemini 429
        do {
            let errJSON: JSONValue = .object(["error": .object(["message": .string("rate limited"), "status": .string("RESOURCE_EXHAUSTED")])])
            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 429, headers: headers, body: Array(try errJSON.data()))
            })
            let adapter = GeminiAdapter(apiKey: "gemini-test", transport: transport)
            do {
                _ = try await adapter.complete(request: Request(model: "gemini-3-flash-preview", messages: [.user("hi")]))
                XCTFail("Expected RateLimitError")
            } catch let e as RateLimitError {
                XCTAssertEqual(e.retryAfter, 3)
            }
        }

        // Cerebras 429
        do {
            let errJSON: JSONValue = .object(["error": .object(["message": .string("rate limited"), "type": .string("rate_limit_error")])])
            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 429, headers: headers, body: Array(try errJSON.data()))
            })
            let adapter = CerebrasAdapter(apiKey: "cerebras-test", transport: transport)
            do {
                _ = try await adapter.complete(request: Request(model: "zai-glm-4.7", messages: [.user("hi")]))
                XCTFail("Expected RateLimitError")
            } catch let e as RateLimitError {
                XCTAssertEqual(e.retryAfter, 3)
            }
        }

        // Groq 429
        do {
            let errJSON: JSONValue = .object(["error": .object(["message": .string("rate limited"), "type": .string("rate_limit_error")])])
            let transport = StubTransport(send: { _ in
                HTTPResponse(statusCode: 429, headers: headers, body: Array(try errJSON.data()))
            })
            let adapter = GroqAdapter(apiKey: "groq-test", transport: transport)
            do {
                _ = try await adapter.complete(request: Request(model: "llama-3.1-8b-instant", messages: [.user("hi")]))
                XCTFail("Expected RateLimitError")
            } catch let e as RateLimitError {
                XCTAssertEqual(e.retryAfter, 3)
            }
        }
    }

    @Test
    func testOpenAIReasoningEffortIsPassedThrough() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("resp_1"),
            "model": .string("gpt-5.2"),
            "output": .array([.object(["type": .string("message"), "content": .array([.object(["type": .string("output_text"), "text": .string("ok")])])])]),
            "finish_reason": .string("stop"),
            "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })
        let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)

        _ = try await adapter.complete(
            request: Request(
                model: "gpt-5.2",
                messages: [.user("hi")],
                reasoningEffort: "high"
            )
        )

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertEqual(body["reasoning"]?["effort"]?.stringValue, "high")
    }

    @Test
    func testOpenAIAdapterStreamingMapsToolCallEvents() async throws {
        let completedJSON: JSONValue = .object([
            "type": .string("response.completed"),
            "response": .object([
                "id": .string("resp_1"),
                "model": .string("gpt-5.2"),
                "output": .array([
                    .object([
                        "type": .string("function_call"),
                        "call_id": .string("c1"),
                        "name": .string("add"),
                        "arguments": .string("{\"a\":1,\"b\":2}"),
                    ]),
                ]),
                "finish_reason": .string("tool_calls"),
                "usage": .object(["prompt_tokens": .number(1), "completion_tokens": .number(1)]),
            ]),
        ])

        let sse = """
        event: response.function_call_arguments.delta
        data: {\"type\":\"response.function_call_arguments.delta\",\"call_id\":\"c1\",\"name\":\"add\",\"delta\":\"{\\\"a\\\":1\"}

        event: response.function_call_arguments.delta
        data: {\"type\":\"response.function_call_arguments.delta\",\"call_id\":\"c1\",\"delta\":\",\\\"b\\\":2}\"}

        event: response.function_call_arguments.done
        data: {\"type\":\"response.function_call_arguments.done\",\"call_id\":\"c1\"}

        event: response.completed
        data: \(String(data: try completedJSON.data(), encoding: .utf8)!)

        """

        let transport = StubTransport(stream: { _ in
            HTTPStreamResponse(statusCode: 200, headers: HTTPHeaders(), body: streamFromSSE(sse))
        })

        let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)
        let stream = try await adapter.stream(request: Request(model: "gpt-5.2", messages: [.user("hi")]))

        var seenStart = false
        var seenDelta = false
        var seenEnd = false
        var finish: Response?

        for try await ev in stream {
            if ev.type.rawValue == StreamEventType.toolCallStart.rawValue {
                seenStart = true
                XCTAssertEqual(ev.toolCall?.id, "c1")
                XCTAssertEqual(ev.toolCall?.name, "add")
            }
            if ev.type.rawValue == StreamEventType.toolCallDelta.rawValue {
                seenDelta = true
                XCTAssertEqual(ev.toolCall?.id, "c1")
                XCTAssertTrue((ev.toolCall?.rawArguments ?? "").contains("\"a\""))
            }
            if ev.type.rawValue == StreamEventType.toolCallEnd.rawValue {
                seenEnd = true
                XCTAssertEqual(ev.toolCall?.id, "c1")
                XCTAssertEqual(ev.toolCall?.arguments["a"]?.doubleValue, 1)
                XCTAssertEqual(ev.toolCall?.arguments["b"]?.doubleValue, 2)
            }
            if ev.type.rawValue == StreamEventType.finish.rawValue {
                finish = ev.response
            }
        }

        XCTAssertTrue(seenStart)
        XCTAssertTrue(seenDelta)
        XCTAssertTrue(seenEnd)
        XCTAssertEqual(finish?.finishReason.rawValue, "tool_calls")
        XCTAssertEqual(finish?.toolCalls.first?.name, "add")
    }

    @Test
    func testAnthropicAdapterStreamingMapsToolCallEvents() async throws {
        let sse = """
        event: message_start
        data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"claude-opus-4-6\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}

        event: content_block_start
        data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"tool_1\",\"name\":\"add\"}}

        event: content_block_delta
        data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"a\\\":1,\\\"b\\\":2}\"}}

        event: content_block_stop
        data: {\"type\":\"content_block_stop\",\"index\":0}

        event: message_delta
        data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":7}}

        event: message_stop
        data: {\"type\":\"message_stop\"}

        """

        let transport = StubTransport(stream: { _ in
            HTTPStreamResponse(statusCode: 200, headers: HTTPHeaders(), body: streamFromSSE(sse))
        })

        let adapter = AnthropicAdapter(apiKey: "anthropic-test", transport: transport)
        let stream = try await adapter.stream(request: Request(model: "claude-opus-4-6", messages: [.user("hi")]))

        var seenStart = false
        var seenDelta = false
        var seenEnd = false
        var finish: Response?

        for try await ev in stream {
            if ev.type.rawValue == StreamEventType.toolCallStart.rawValue {
                seenStart = true
                XCTAssertEqual(ev.toolCall?.id, "tool_1")
                XCTAssertEqual(ev.toolCall?.name, "add")
            }
            if ev.type.rawValue == StreamEventType.toolCallDelta.rawValue {
                seenDelta = true
                XCTAssertTrue((ev.toolCall?.rawArguments ?? "").contains("\"a\""))
            }
            if ev.type.rawValue == StreamEventType.toolCallEnd.rawValue {
                seenEnd = true
                XCTAssertEqual(ev.toolCall?.arguments["a"]?.doubleValue, 1)
                XCTAssertEqual(ev.toolCall?.arguments["b"]?.doubleValue, 2)
            }
            if ev.type.rawValue == StreamEventType.finish.rawValue {
                finish = ev.response
            }
        }

        XCTAssertTrue(seenStart)
        XCTAssertTrue(seenDelta)
        XCTAssertTrue(seenEnd)
        XCTAssertEqual(finish?.finishReason.rawValue, "tool_calls")
        XCTAssertEqual(finish?.toolCalls.first?.name, "add")
    }
}
