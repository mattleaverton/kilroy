import Testing
import Foundation
import OmniHTTP
@testable import OmniAICore

private final class StubRealtimeClientSession: RealtimeWebSocketSession, @unchecked Sendable {
    private actor State {
        var sentTexts: [String] = []
        var closedCodes: [RealtimeWebSocketCloseCode?] = []
        func append(text: String) { sentTexts.append(text) }
        func append(code: RealtimeWebSocketCloseCode?) { closedCodes.append(code) }
        func snapshot() -> ([String], [RealtimeWebSocketCloseCode?]) { (sentTexts, closedCodes) }
    }

    private let state = State()
    private let stream: AsyncThrowingStream<RealtimeWebSocketMessage, Error>

    init(messages: [RealtimeWebSocketMessage], keepOpen: Bool = false) {
        self.stream = AsyncThrowingStream { continuation in
            for message in messages { continuation.yield(message) }
            if !keepOpen {
                continuation.finish()
            }
        }
    }

    func send(text: String, timeout: Duration?) async throws {
        let _ = timeout
        await state.append(text: text)
    }

    func send(binary: Data, timeout: Duration?) async throws {
        let _ = timeout
        Issue.record("binary send not expected in this test")
    }

    func incomingMessages() -> AsyncThrowingStream<RealtimeWebSocketMessage, Error> { stream }

    func close(code: RealtimeWebSocketCloseCode?) async {
        await state.append(code: code)
    }

    func snapshot() async -> ([String], [RealtimeWebSocketCloseCode?]) { await state.snapshot() }
}

private final class StubRealtimeClientTransport: RealtimeWebSocketTransport, @unchecked Sendable {
    private actor State {
        var lastURL: URL?
        var lastHeaders: HTTPHeaders?
        func record(url: URL, headers: HTTPHeaders) { lastURL = url; lastHeaders = headers }
        func snapshot() -> (URL?, HTTPHeaders?) { (lastURL, lastHeaders) }
    }

    private let state = State()
    let session: StubRealtimeClientSession

    init(session: StubRealtimeClientSession) {
        self.session = session
    }

    func connect(url: URL, headers: HTTPHeaders, timeout: Duration?) async throws -> any RealtimeWebSocketSession {
        let _ = timeout
        await state.record(url: url, headers: headers)
        return session
    }

    func snapshot() async -> (URL?, HTTPHeaders?) { await state.snapshot() }
}

struct OpenAIRealtimeClientTests {
    private func encodeJSON(_ object: [String: Any]) throws -> String {
        let data = try JSONSerialization.data(withJSONObject: object)
        return String(decoding: data, as: UTF8.self)
    }

    @Test
    func realtime_client_connects_with_expected_url_and_headers() async throws {
        guard #available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *) else { return }
        let session = StubRealtimeClientSession(messages: [
            .text(try encodeJSON([
                "type": "session.created",
                "session": [
                    "id": "sess_1",
                    "object": "realtime.session",
                    "model": "gpt-realtime",
                ],
            ]))
        ])
        let transport = StubRealtimeClientTransport(session: session)
        guard let realtimeURL = URL(string: "wss://example.invalid/v1/realtime") else {
            throw NSError(domain: "OpenAIRealtimeClientTests", code: 1, userInfo: [NSLocalizedDescriptionKey: "Invalid realtime test URL"])
        }
        let client = OpenAIRealtimeClient(
            apiKey: "sk-test",
            baseURL: realtimeURL,
            transport: transport
        )

        let stream = try await client.connect(model: "gpt-realtime-mini")
        var sawSession = false
        for try await event in stream {
            if case .sessionCreated(let session) = event {
                sawSession = true
                #expect(session.id == "sess_1")
            }
        }
        #expect(sawSession)

        let snapshot = await transport.snapshot()
        #expect(snapshot.0?.absoluteString == "wss://example.invalid/v1/realtime?model=gpt-realtime-mini")
        #expect(snapshot.1?.firstValue(for: "Authorization") == "Bearer sk-test")
        #expect(snapshot.1?.firstValue(for: "OpenAI-Beta") == "realtime=v1")
    }

    @Test
    func realtime_client_methods_throw_when_not_connected() async {
        guard #available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *) else { return }
        let session = StubRealtimeClientSession(messages: [], keepOpen: true)
        let transport = StubRealtimeClientTransport(session: session)
        let client = OpenAIRealtimeClient(apiKey: "sk-test", transport: transport)

        do {
            try await client.updateSession(RealtimeSessionConfig())
            Issue.record("Expected notConnected")
        } catch let error as RealtimeError {
            if case .notConnected = error {} else { Issue.record("Unexpected error \(error)") }
        } catch {
            Issue.record("Unexpected error \(error)")
        }

        do { try await client.appendInputAudio("AAA"); Issue.record("Expected notConnected") } catch {}
        do { try await client.commitInputAudio(); Issue.record("Expected notConnected") } catch {}
        do { try await client.createResponse(); Issue.record("Expected notConnected") } catch {}
        do { try await client.sendFunctionCallOutput(callId: "call_1", output: "{}") ; Issue.record("Expected notConnected") } catch {}
    }

    @Test
    func realtime_client_sends_session_audio_and_response_events() async throws {
        guard #available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *) else { return }
        let session = StubRealtimeClientSession(messages: [], keepOpen: true)
        let transport = StubRealtimeClientTransport(session: session)
        let client = OpenAIRealtimeClient(apiKey: "sk-test", transport: transport)
        let stream = try await client.connect(model: "gpt-realtime")
        _ = stream

        try await client.updateSession(RealtimeSessionConfig(
            instructions: "help",
            outputModalities: ["text", "audio"],
            voice: .alloy,
            inputAudioFormat: .pcm16k,
            outputAudioFormat: .pcm16k,
            turnDetection: .serverVad
        ))
        try await client.appendInputAudio("AAA")
        try await client.commitInputAudio()
        try await client.createResponse(.audioOnly(instructions: "respond"))
        try await client.sendFunctionCallOutput(callId: "call_1", output: "{}")
        await client.disconnect()

        let (sentTexts, closedCodes) = await session.snapshot()
        #expect(sentTexts.count == 5)
        let sentPayloads = try sentTexts.map { try JSONValue.parse(Data($0.utf8)) }
        #expect(sentPayloads[0]["type"]?.stringValue == "session.update")
        #expect(sentPayloads[1]["type"]?.stringValue == "input_audio_buffer.append")
        #expect(sentPayloads[2]["type"]?.stringValue == "input_audio_buffer.commit")
        #expect(sentPayloads[3]["type"]?.stringValue == "response.create")
        #expect(sentPayloads[4]["type"]?.stringValue == "conversation.item.create")
        #expect(closedCodes.contains(.normalClosure))
    }

    @Test
    func realtime_server_event_decoding_covers_audio_and_text_events() throws {
        let payloads: [[String: Any]] = [
            ["type": "response.text.delta", "delta": "h", "content_index": 0, "output_index": 0],
            ["type": "response.text.done", "text": "hi", "content_index": 0, "output_index": 0],
            ["type": "response.audio.delta", "delta": "AAA", "content_index": 0, "output_index": 0],
            ["type": "response.audio.done", "content_index": 0, "output_index": 0],
            ["type": "response.audio_transcript.delta", "delta": "t", "content_index": 0, "output_index": 0],
            ["type": "response.audio_transcript.done", "transcript": "done", "content_index": 0, "output_index": 0],
            ["type": "response.function_call_arguments.delta", "delta": "{\"a\":1}", "call_id": "c1", "name": "tool"],
            ["type": "response.function_call_arguments.done", "arguments": "{\"a\":1}", "call_id": "c1", "name": "tool"],
        ]

        for payload in payloads {
            let data = try JSONSerialization.data(withJSONObject: payload)
            _ = try JSONDecoder().decode(RealtimeServerEvent.self, from: data)
        }
    }
}
