import Testing
import Foundation

import OmniHTTP
@testable import OmniAICore

private actor CapturingTransport: HTTPTransport {
    private(set) var lastSendRequest: HTTPRequest?

    private let response: HTTPResponse

    init(response: HTTPResponse) {
        self.response = response
    }

    func send(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPResponse {
        lastSendRequest = request
        return response
    }

    func openStream(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPStreamResponse {
        throw OmniHTTPError.streamingNotSupported
    }

    func shutdown() async throws {}
}

private actor SequencedTransport: HTTPTransport {
    private var queue: [HTTPResponse]
    private(set) var requests: [HTTPRequest] = []

    init(responses: [HTTPResponse]) {
        self.queue = responses
    }

    func send(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPResponse {
        requests.append(request)
        if queue.isEmpty {
            throw ConfigurationError(message: "SequencedTransport has no more responses")
        }
        return queue.removeFirst()
    }

    func openStream(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPStreamResponse {
        throw OmniHTTPError.streamingNotSupported
    }

    func shutdown() async throws {}
}

@Suite
final class FromEnvTests {
    @Test
    func testFromEnvThrowsConfigurationErrorWhenNoProvidersConfigured() {
        XCTAssertThrowsError(
            try Client.fromEnv(environment: [:], transport: URLSessionHTTPTransport())
        ) { err in
            XCTAssertTrue(err is ConfigurationError)
        }
    }

    @Test
    func testFromEnvRegistersOpenAIAndPassesOrgAndProjectHeaders() async throws {
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

        let transport = CapturingTransport(
            response: HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        )

        let client = try Client.fromEnv(
            environment: [
                "OPENAI_API_KEY": "sk-test",
                "OPENAI_ORG_ID": "org_1",
                "OPENAI_PROJECT_ID": "proj_1",
            ],
            transport: transport
        )

        let resp = try await client.complete(Request(model: "gpt-5.2", messages: [.user("hi")]))
        XCTAssertEqual(resp.provider, "openai")

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertEqual(sent.headers.firstValue(for: "openai-organization"), "org_1")
        XCTAssertEqual(sent.headers.firstValue(for: "openai-project"), "proj_1")
    }

    @Test
    func testFromEnvRegistersOpenAIUsingCodexOAuthIDTokenExchange() async throws {
        let exchangeResponse: JSONValue = .object([
            "access_token": .string("sk-exchanged"),
        ])
        let openAIResponse: JSONValue = .object([
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

        let transport = SequencedTransport(responses: [
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try exchangeResponse.data())),
            HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try openAIResponse.data())),
        ])

        let client = try await Client.fromEnvAsync(
            environment: [
                "OPENAI_OAUTH_ID_TOKEN": "id-token-123",
                "OPENAI_OAUTH_CLIENT_ID": "client-xyz",
                "OPENAI_OAUTH_ISSUER": "https://auth.openai.com",
            ],
            transport: transport
        )

        let response = try await client.complete(Request(model: "gpt-5.2", messages: [.user("hi")]))
        XCTAssertEqual(response.provider, "openai")

        let sent = await transport.requests
        XCTAssertEqual(sent.count, 2)
        XCTAssertEqual(sent[0].url.absoluteString, "https://auth.openai.com/oauth/token")
        XCTAssertEqual(sent[0].method, .post)
        XCTAssertEqual(sent[0].headers.firstValue(for: "content-type"), "application/x-www-form-urlencoded")

        let firstBody: String = {
            if case .bytes(let bytes) = sent[0].body {
                return String(decoding: bytes, as: UTF8.self)
            }
            return ""
        }()
        XCTAssertTrue(firstBody.contains("grant_type=urn:ietf:params:oauth:grant-type:token-exchange"))
        XCTAssertTrue(firstBody.contains("client_id=client-xyz"))
        XCTAssertTrue(firstBody.contains("requested_token=openai-api-key"))
        XCTAssertTrue(firstBody.contains("subject_token=id-token-123"))
        XCTAssertTrue(firstBody.contains("subject_token_type=urn:ietf:params:oauth:token-type:id_token"))

        XCTAssertEqual(sent[1].headers.firstValue(for: "authorization"), "Bearer sk-exchanged")
    }

    @Test
    func testFromEnvRegistersAnthropic() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("msg_1"),
            "model": .string("claude-opus-4-6"),
            "content": .array([.object(["type": .string("text"), "text": .string("ok")])]),
            "stop_reason": .string("end_turn"),
            "usage": .object(["input_tokens": .number(1), "output_tokens": .number(1)]),
        ])

        let transport = CapturingTransport(
            response: HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        )

        let client = try Client.fromEnv(
            environment: ["ANTHROPIC_API_KEY": "anthropic-test"],
            transport: transport
        )

        let resp = try await client.complete(Request(model: "claude-opus-4-6", messages: [.user("hi")]))
        XCTAssertEqual(resp.provider, "anthropic")

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertEqual(sent.headers.firstValue(for: "x-api-key"), "anthropic-test")
    }

    @Test
    func testFromEnvRegistersGeminiUsingGeminiAPIKey() async throws {
        let responseJSON: JSONValue = .object([
            "candidates": .array([
                .object([
                    "finishReason": .string("STOP"),
                    "content": .object(["parts": .array([.object(["text": .string("ok")])])]),
                ]),
            ]),
            "usageMetadata": .object(["promptTokenCount": .number(1), "candidatesTokenCount": .number(1)]),
        ])

        let transport = CapturingTransport(
            response: HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        )

        let client = try Client.fromEnv(
            environment: ["GEMINI_API_KEY": "gemini-test"],
            transport: transport
        )

        let resp = try await client.complete(Request(model: "gemini-3-flash-preview", messages: [.user("hi")]))
        XCTAssertEqual(resp.provider, "gemini")

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains("key=gemini-test"))
    }

    @Test
    func testFromEnvRegistersGeminiUsingGoogleAPIKeyFallback() async throws {
        let responseJSON: JSONValue = .object([
            "candidates": .array([
                .object([
                    "finishReason": .string("STOP"),
                    "content": .object(["parts": .array([.object(["text": .string("ok")])])]),
                ]),
            ]),
            "usageMetadata": .object(["promptTokenCount": .number(1), "candidatesTokenCount": .number(1)]),
        ])

        let transport = CapturingTransport(
            response: HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        )

        let client = try Client.fromEnv(
            environment: ["GOOGLE_API_KEY": "google-test"],
            transport: transport
        )

        let resp = try await client.complete(Request(model: "gemini-3-flash-preview", messages: [.user("hi")]))
        XCTAssertEqual(resp.provider, "gemini")

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertTrue(sent.url.absoluteString.contains("key=google-test"))
    }

    @Test
    func testFromEnvRegistersCerebras() async throws {
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

        let transport = CapturingTransport(
            response: HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        )

        let client = try Client.fromEnv(
            environment: ["CEREBRAS_API_KEY": "cerebras-test"],
            transport: transport
        )

        let resp = try await client.complete(Request(model: "zai-glm-4.7", messages: [.user("hi")]))
        XCTAssertEqual(resp.provider, "cerebras")

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertEqual(sent.headers.firstValue(for: "authorization"), "Bearer cerebras-test")
        XCTAssertTrue(sent.url.absoluteString.contains("/chat/completions"))
    }

    @Test
    func testFromEnvRegistersGroq() async throws {
        let responseJSON: JSONValue = .object([
            "id": .string("chatcmpl_1"),
            "model": .string("llama-3.1-8b-instant"),
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

        let transport = CapturingTransport(
            response: HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: Array(try responseJSON.data()))
        )

        let client = try Client.fromEnv(
            environment: ["GROQ_API_KEY": "groq-test"],
            transport: transport
        )

        let resp = try await client.complete(Request(model: "llama-3.1-8b-instant", messages: [.user("hi")]))
        XCTAssertEqual(resp.provider, "groq")

        let sentOpt = await transport.lastSendRequest
        let sent = try XCTUnwrap(sentOpt)
        XCTAssertEqual(sent.headers.firstValue(for: "authorization"), "Bearer groq-test")
        XCTAssertTrue(sent.url.absoluteString.contains("/chat/completions"))
    }
}
