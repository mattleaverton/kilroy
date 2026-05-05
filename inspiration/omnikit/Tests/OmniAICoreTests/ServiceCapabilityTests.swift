import Testing
import Foundation

import OmniHTTP
@testable import OmniAICore

private actor StubTransport: HTTPTransport {
    private(set) var lastSendRequest: HTTPRequest?

    private let sendHandler: @Sendable (HTTPRequest) async throws -> HTTPResponse

    init(send: @Sendable @escaping (HTTPRequest) async throws -> HTTPResponse) {
        self.sendHandler = send
    }

    func send(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPResponse {
        lastSendRequest = request
        return try await sendHandler(request)
    }

    func openStream(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPStreamResponse {
        lastSendRequest = request
        return HTTPStreamResponse(statusCode: 500, headers: HTTPHeaders(), body: AsyncThrowingStream { $0.finish() })
    }

    func shutdown() async throws {}
}

private func bodyBytes(_ request: HTTPRequest) -> [UInt8] {
    switch request.body {
    case .none:
        return []
    case .bytes(let bytes):
        return bytes
    }
}

@Suite
final class ServiceCapabilityTests {
    @Test
    func testOpenAIEmbedShapesRequest() async throws {
        let responseJSON: JSONValue = .object([
            "model": .string("text-embedding-3-small"),
            "data": .array([
                .object(["index": .number(0), "embedding": .array([.number(1.0), .number(2.0)])]),
                .object(["index": .number(1), "embedding": .array([.number(3.0), .number(4.0)])]),
            ]),
            "usage": .object(["prompt_tokens": .number(10), "total_tokens": .number(10)]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)
        let request = EmbedRequest(model: "text-embedding-3-small", input: ["a", "b"], dimensions: 8, user: "user")
        let response = try await adapter.embed(request: request)

        XCTAssertEqual(response.embeddings.count, 2)

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains("/embeddings"))
        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertEqual(body["model"]?.stringValue, "text-embedding-3-small")
        XCTAssertEqual(body["input"]?.arrayValue?.count, 2)
        XCTAssertEqual(body["dimensions"]?.doubleValue, 8)
        XCTAssertEqual(body["user"]?.stringValue, "user")
    }

    @Test
    func testGeminiEmbedShapesBatchRequest() async throws {
        let responseJSON: JSONValue = .object([
            "embeddings": .array([
                .object(["values": .array([.number(1.0), .number(2.0)])]),
                .object(["values": .array([.number(3.0), .number(4.0)])]),
            ]),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = GeminiAdapter(apiKey: "gemini-test", transport: transport)
        let request = EmbedRequest(model: "text-embedding-004", input: ["hello", "world"])
        let response = try await adapter.embed(request: request)

        XCTAssertEqual(response.embeddings.count, 2)

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains(":batchEmbedContents"))
        let body = try JSONValue.parse(bodyBytes(sent))
        let requests = body["requests"]?.arrayValue ?? []
        XCTAssertEqual(requests.count, 2)
        XCTAssertEqual(requests.first?["model"]?.stringValue, "models/text-embedding-004")
    }

    @Test
    func testOpenAIAudioSpeechUsesServiceNamespace() async throws {
        let transport = StubTransport(send: { _ in
            var headers = HTTPHeaders()
            headers.set(name: "content-type", value: "audio/mpeg")
            return HTTPResponse(statusCode: 200, headers: headers, body: [0x00, 0x01])
        })

        let adapter = OpenAIAdapter(apiKey: "sk-test", transport: transport)
        let client = try Client(providers: ["openai": adapter], defaultProvider: "openai")

        let response = try await client.openai.audio.speech(OpenAISpeechRequest(
            model: "gpt-4o-mini-tts",
            input: "hi",
            voice: "alloy"
        ))

        XCTAssertEqual(response.audio.data, [0x00, 0x01])

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains("/audio/speech"))
        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertEqual(body["model"]?.stringValue, "gpt-4o-mini-tts")
        XCTAssertEqual(body["input"]?.stringValue, "hi")
        XCTAssertEqual(body["voice"]?.stringValue, "alloy")
    }

    @Test
    func testGeminiTokenCountShapesRequest() async throws {
        let responseJSON: JSONValue = .object([
            "totalTokens": .number(12),
            "cachedContentTokenCount": .number(2),
        ])

        let transport = StubTransport(send: { _ in
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        })

        let adapter = GeminiAdapter(apiKey: "gemini-test", transport: transport)
        let client = try Client(providers: ["gemini": adapter], defaultProvider: "gemini")

        let response = try await client.gemini.tokens.countTokens(GeminiTokenCountRequest(
            model: "gemini-1.5-pro",
            messages: [.user("hello")]
        ))

        XCTAssertEqual(response.totalTokens, 12)

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains(":countTokens"))
        let body = try JSONValue.parse(bodyBytes(sent))
        XCTAssertNotNil(body["generateContentRequest"])
    }
}
