import Foundation
import Testing
import OmniAICore
import OmniHTTP
@testable import OmniACP
@testable import OmniACPModel

private actor TransportNotificationRecorder {
    private var sawAgentMessage = false

    func record(_ notification: AnyMessage) throws {
        guard notification.method == SessionUpdateNotification.name else { return }
        let params = try notification.decodeParameters(SessionUpdateNotification.Parameters.self)
        if case .agentMessageChunk = params.update {
            sawAgentMessage = true
        }
    }

    func hasSeenAgentMessage() -> Bool {
        sawAgentMessage
    }
}

private actor ScriptedACPRemoteAgent {
    private let encoder = JSONEncoder()
    private let sessionID: String

    init(sessionID: String) {
        self.sessionID = sessionID
    }

    func handle(_ data: Data) throws -> [Data] {
        let object = (try JSONSerialization.jsonObject(with: data)) as? [String: Any]
        guard let object,
              let method = object["method"] as? String,
              object.keys.contains("id")
        else {
            return []
        }

        switch method {
        case Initialize.name:
            let request = try JSONDecoder().decode(Request<Initialize>.self, from: data)
            let response = Initialize.response(
                id: request.id,
                result: .init(
                    protocolVersion: request.params.protocolVersion,
                    agentInfo: .init(name: "StubRemoteAgent", version: "1.0.0"),
                    agentCapabilities: .init(loadSession: true, mcpCapabilities: .init(), promptCapabilities: .init()),
                    authMethods: []
                )
            )
            return [try encoder.encode(response)]
        case SessionNew.name:
            let request = try JSONDecoder().decode(Request<SessionNew>.self, from: data)
            return [try encoder.encode(SessionNew.response(id: request.id, result: .init(sessionID: sessionID)))]
        case SessionPrompt.name:
            let request = try JSONDecoder().decode(Request<SessionPrompt>.self, from: data)
            let update = Message<SessionUpdateNotification>(
                method: SessionUpdateNotification.name,
                params: .init(
                    sessionID: request.params.sessionID,
                    update: .agentMessageChunk(.init(content: .init(text: "hello from remote transport")))
                )
            )
            let response = SessionPrompt.response(id: request.id, result: .init(stopReason: .endTurn))
            return [try encoder.encode(update), try encoder.encode(response)]
        case SessionCancel.name:
            let request = try JSONDecoder().decode(Request<SessionCancel>.self, from: data)
            return [try encoder.encode(SessionCancel.response(id: request.id))]
        case SessionSetMode.name:
            let request = try JSONDecoder().decode(Request<SessionSetMode>.self, from: data)
            return [try encoder.encode(SessionSetMode.response(id: request.id))]
        default:
            return []
        }
    }
}

private final class StubACPRealtimeWebSocketSession: RealtimeWebSocketSession, @unchecked Sendable {
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
    private let agent: ScriptedACPRemoteAgent
    private let stream: AsyncThrowingStream<RealtimeWebSocketMessage, Error>
    private let continuation: AsyncThrowingStream<RealtimeWebSocketMessage, Error>.Continuation

    init(agent: ScriptedACPRemoteAgent) {
        self.agent = agent
        var continuationRef: AsyncThrowingStream<RealtimeWebSocketMessage, Error>.Continuation?
        self.stream = AsyncThrowingStream { continuation in
            continuationRef = continuation
        }
        guard let continuationRef else {
            preconditionFailure("Failed to initialize scripted websocket stream")
        }
        self.continuation = continuationRef
    }

    func send(text: String, timeout: Duration?) async throws {
        let _ = timeout
        await state.append(text: text)
        let responses = try await agent.handle(Data(text.utf8))
        for response in responses {
            continuation.yield(.text(String(decoding: response, as: UTF8.self)))
        }
    }

    func send(binary: Data, timeout: Duration?) async throws {
        let _ = timeout
        await state.append(binary: binary)
        let responses = try await agent.handle(binary)
        for response in responses {
            continuation.yield(.binary(response))
        }
    }

    func incomingMessages() -> AsyncThrowingStream<RealtimeWebSocketMessage, Error> {
        stream
    }

    func close(code: RealtimeWebSocketCloseCode?) async {
        await state.append(code: code)
        continuation.finish()
    }

    func snapshot() async -> (texts: [String], binaries: [Data], codes: [RealtimeWebSocketCloseCode?]) {
        await state.snapshot()
    }
}

private final class StubACPRealtimeWebSocketTransport: RealtimeWebSocketTransport, @unchecked Sendable {
    let session: StubACPRealtimeWebSocketSession

    init(session: StubACPRealtimeWebSocketSession) {
        self.session = session
    }

    func connect(url: URL, headers: HTTPHeaders, timeout: Duration?) async throws -> any RealtimeWebSocketSession {
        let _ = url
        let _ = headers
        let _ = timeout
        return session
    }
}

private final actor StubACPHTTPTransport: HTTPTransport {
    private let agent: ScriptedACPRemoteAgent
    private let stream: HTTPByteStream
    private var continuation: AsyncThrowingStream<[UInt8], Error>.Continuation?
    private var requests: [HTTPRequest] = []
    private var shutdownCount = 0

    init(agent: ScriptedACPRemoteAgent) {
        self.agent = agent
        var continuationRef: AsyncThrowingStream<[UInt8], Error>.Continuation?
        self.stream = AsyncThrowingStream { continuation in
            continuationRef = continuation
        }
        self.continuation = continuationRef
    }

    func send(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPResponse {
        let _ = timeout
        requests.append(request)
        if case .bytes(let bytes) = request.body {
            let responses = try await agent.handle(Data(bytes))
            for response in responses {
                let payload = Data("data: ".utf8) + response + Data("\n\n".utf8)
                continuation?.yield(Array(payload))
            }
        }
        return HTTPResponse(statusCode: 202, headers: HTTPHeaders(), body: [])
    }

    func openStream(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPStreamResponse {
        let _ = timeout
        requests.append(request)
        var headers = HTTPHeaders()
        headers.set(name: "content-type", value: "text/event-stream")
        return HTTPStreamResponse(statusCode: 200, headers: headers, body: stream)
    }

    func shutdown() async throws {
        shutdownCount += 1
        continuation?.finish()
        continuation = nil
    }

    func snapshot() -> (requests: [HTTPRequest], shutdownCount: Int) {
        (requests, shutdownCount)
    }
}

private enum TransportTestError: Error {
    case timeout(String)
}

private func waitForAgentMessage(
    _ recorder: TransportNotificationRecorder,
    timeoutNanoseconds: UInt64 = 1_000_000_000,
    pollIntervalNanoseconds: UInt64 = 5_000_000
) async throws {
    let attempts = max(1, Int(timeoutNanoseconds / pollIntervalNanoseconds))
    for _ in 0..<attempts {
        if await recorder.hasSeenAgentMessage() {
            return
        }
        try await Task.sleep(nanoseconds: pollIntervalNanoseconds)
    }
    throw TransportTestError.timeout("Timed out waiting for agent message notification")
}

struct TransportTests {
    @Test
    func websocket_transport_client_lifecycle_and_streaming_work() async throws {
        let agent = ScriptedACPRemoteAgent(sessionID: "sess_ws")
        let socketSession = StubACPRealtimeWebSocketSession(agent: agent)
        guard let websocketURL = URL(string: "wss://example.invalid/acp") else {
            throw TransportTestError.timeout("Invalid websocket test URL")
        }
        let transport = WebSocketTransport(
            configuration: .init(url: websocketURL),
            websocketTransport: StubACPRealtimeWebSocketTransport(session: socketSession)
        )
        let client = Client(name: "Tests", version: "1.0.0")
        defer {
            Task {
                await client.disconnect()
            }
        }

        let initResult = try await client.connect(transport: transport, timeout: .seconds(5))
        #expect(initResult.agentInfo?.name == "StubRemoteAgent")

        let recorder = TransportNotificationRecorder()
        let recorderTask = Task {
            for await notification in client.notifications {
                try? await recorder.record(notification)
            }
        }
        defer { recorderTask.cancel() }

        let session = try await client.newSession(cwd: "/tmp", timeout: .seconds(5))
        #expect(session.sessionID == "sess_ws")

        let promptResult = try await client.prompt(sessionID: session.sessionID, prompt: [.text("hello")], timeout: .seconds(5))
        #expect(promptResult.stopReason == .endTurn)
        try await waitForAgentMessage(recorder)

        let snapshot = await socketSession.snapshot()
        #expect(snapshot.texts.count >= 4)
        #expect(snapshot.binaries.isEmpty)
    }

    @Test
    func http_sse_transport_client_lifecycle_and_streaming_work() async throws {
        let agent = ScriptedACPRemoteAgent(sessionID: "sess_http")
        let httpTransport = StubACPHTTPTransport(agent: agent)
        guard let httpURL = URL(string: "https://example.invalid/acp") else {
            throw TransportTestError.timeout("Invalid HTTP test URL")
        }
        let transport = HTTPSSETransport(
            configuration: .init(url: httpURL),
            httpTransport: httpTransport
        )
        let client = Client(name: "Tests", version: "1.0.0")
        defer {
            Task {
                await client.disconnect()
            }
        }

        let initResult = try await client.connect(transport: transport, timeout: .seconds(5))
        #expect(initResult.agentInfo?.name == "StubRemoteAgent")

        let recorder = TransportNotificationRecorder()
        let recorderTask = Task {
            for await notification in client.notifications {
                try? await recorder.record(notification)
            }
        }
        defer { recorderTask.cancel() }

        let session = try await client.newSession(cwd: "/tmp", timeout: .seconds(5))
        #expect(session.sessionID == "sess_http")

        let promptResult = try await client.prompt(sessionID: session.sessionID, prompt: [.text("hello")], timeout: .seconds(5))
        #expect(promptResult.stopReason == .endTurn)
        try await waitForAgentMessage(recorder)

        let snapshot = await httpTransport.snapshot()
        #expect(snapshot.requests.count >= 4)
        #expect(snapshot.requests[0].method == .get)
        #expect(snapshot.requests[1].method == .post)
        #expect(snapshot.requests[1].url.absoluteString == "https://example.invalid/acp")
        #expect(snapshot.requests[1].headers.firstValue(for: "content-type") == "application/json")
    }
}
