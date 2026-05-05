import Testing
import Foundation
import OmniHTTP
@testable import OmniAICore

private final class StubRealtimeWebSocketSession: RealtimeWebSocketSession, @unchecked Sendable {
    private actor State {
        var sentTexts: [String] = []
        var sentBinaries: [Data] = []
        var closedCodes: [RealtimeWebSocketCloseCode?] = []

        func append(text: String) { sentTexts.append(text) }
        func append(binary: Data) { sentBinaries.append(binary) }
        func append(code: RealtimeWebSocketCloseCode?) { closedCodes.append(code) }
        func snapshot() -> (texts: [String], binaries: [Data], codes: [RealtimeWebSocketCloseCode?]) {
            (sentTexts, sentBinaries, closedCodes)
        }
    }

    private let state = State()
    private let stream: AsyncThrowingStream<RealtimeWebSocketMessage, Error>

    init(messages: [RealtimeWebSocketMessage]) {
        self.stream = AsyncThrowingStream { continuation in
            for message in messages {
                continuation.yield(message)
            }
            continuation.finish()
        }
    }

    func send(text: String, timeout: Duration?) async throws {
        let _ = timeout
        await state.append(text: text)
    }

    func send(binary: Data, timeout: Duration?) async throws {
        let _ = timeout
        await state.append(binary: binary)
    }

    func incomingMessages() -> AsyncThrowingStream<RealtimeWebSocketMessage, Error> {
        stream
    }

    func close(code: RealtimeWebSocketCloseCode?) async {
        await state.append(code: code)
    }

    func snapshot() async -> (texts: [String], binaries: [Data], codes: [RealtimeWebSocketCloseCode?]) {
        await state.snapshot()
    }
}

private final class StubRealtimeWebSocketTransport: RealtimeWebSocketTransport, @unchecked Sendable {
    private actor State {
        var lastURL: URL?
        var lastHeaders: HTTPHeaders?
        var lastTimeout: Duration?
        func record(url: URL, headers: HTTPHeaders, timeout: Duration?) {
            lastURL = url
            lastHeaders = headers
            lastTimeout = timeout
        }
        func snapshot() -> (URL?, HTTPHeaders?, Duration?) { (lastURL, lastHeaders, lastTimeout) }
    }

    private let state = State()
    let session: StubRealtimeWebSocketSession

    init(session: StubRealtimeWebSocketSession) {
        self.session = session
    }

    func connect(url: URL, headers: HTTPHeaders, timeout: Duration?) async throws -> any RealtimeWebSocketSession {
        await state.record(url: url, headers: headers, timeout: timeout)
        return session
    }

    func snapshot() async -> (URL?, HTTPHeaders?, Duration?) { await state.snapshot() }
}

struct RealtimeWebSocketTransportTests {
    @Test
    func json_session_serializes_outgoing_payloads() async throws {
        let session = StubRealtimeWebSocketSession(messages: [])
        let jsonSession = JSONRealtimeWebSocketSession(base: session)

        try await jsonSession.send(.object([
            "type": .string("session.update"),
            "value": .number(1),
        ]))

        let snapshot = await session.snapshot()
        #expect(snapshot.texts.count == 1)
        let parsed = try JSONValue.parse(Data(snapshot.texts[0].utf8))
        #expect(parsed == .object([
            "type": .string("session.update"),
            "value": .number(1),
        ]))
    }

    @Test
    func json_session_parses_text_and_binary_messages() async throws {
        let binaryPayload = try JSONValue.object([
            "type": .string("binary.event"),
        ]).data()
        let session = StubRealtimeWebSocketSession(messages: [
            .text("{\"type\":\"text.event\"}"),
            .binary(binaryPayload),
        ])
        let jsonSession = JSONRealtimeWebSocketSession(base: session)

        var types: [String] = []
        for try await event in jsonSession.events() {
            if let type = event["type"]?.stringValue {
                types.append(type)
            }
        }

        #expect(types == ["text.event", "binary.event"])
    }

    @Test
    func openai_bridge_sends_create_event_and_finishes_on_terminal_event() async throws {
        let session = StubRealtimeWebSocketSession(messages: [
            .text("{\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}"),
            .text("{\"type\":\"response.completed\"}"),
        ])
        let transport = StubRealtimeWebSocketTransport(session: session)
        guard let url = URL(string: "wss://example.invalid/realtime") else {
            throw NSError(domain: "RealtimeWebSocketTransportTests", code: 1, userInfo: [NSLocalizedDescriptionKey: "Invalid realtime websocket test URL"])
        }
        var headers = HTTPHeaders()
        headers.set(name: "authorization", value: "Bearer test")
        let createEvent: JSONValue = .object([
            "type": .string("response.create"),
            "stream": .bool(true),
        ])

        let stream = try await _openOpenAIResponseEventStream(
            transport: transport,
            url: url,
            headers: headers,
            createEvent: createEvent,
            timeout: .seconds(5)
        )

        var receivedTypes: [String] = []
        for try await payload in stream {
            if let type = payload["type"]?.stringValue {
                receivedTypes.append(type)
            }
        }

        #expect(receivedTypes == ["response.output_text.delta", "response.completed"])
        let transportSnapshot = await transport.snapshot()
        #expect(transportSnapshot.0 == url)
        #expect(transportSnapshot.1?.firstValue(for: "authorization") == "Bearer test")

        let snapshot = await session.snapshot()
        #expect(snapshot.texts.count == 1)
        let parsed = try JSONValue.parse(Data(snapshot.texts[0].utf8))
        #expect(parsed == createEvent)
        #expect(snapshot.codes.contains(.normalClosure))
    }
}
