import Foundation
import OmniHTTP

public protocol OpenAIResponsesWebSocketTransport: Sendable {
    func openResponseEventStream(
        url: URL,
        headers: OmniHTTP.HTTPHeaders,
        createEvent: JSONValue,
        timeout: Duration?
    ) async throws -> AsyncThrowingStream<JSONValue, Error>
}

public struct NIOOpenAIResponsesWebSocketTransport: OpenAIResponsesWebSocketTransport {
    private let transport: any RealtimeWebSocketTransport

    public init(transport: any RealtimeWebSocketTransport = NIORealtimeWebSocketTransport()) {
        self.transport = transport
    }

    public func openResponseEventStream(
        url: URL,
        headers: OmniHTTP.HTTPHeaders,
        createEvent: JSONValue,
        timeout: Duration?
    ) async throws -> AsyncThrowingStream<JSONValue, Error> {
        try await _openOpenAIResponseEventStream(
            transport: transport,
            url: url,
            headers: headers,
            createEvent: createEvent,
            timeout: timeout
        )
    }
}

func _openOpenAIResponseEventStream(
    transport: any RealtimeWebSocketTransport,
    url: URL,
    headers: OmniHTTP.HTTPHeaders,
    createEvent: JSONValue,
    timeout: Duration?
) async throws -> AsyncThrowingStream<JSONValue, Error> {
    let session = try await transport.connectJSON(url: url, headers: headers, timeout: timeout)
    try await session.send(createEvent, timeout: timeout)

    return AsyncThrowingStream { continuation in
        let finishOnce = _OpenAIFinishOnce()

        continuation.onTermination = { _ in
            Task {
                await session.close(code: .goingAway)
            }
        }

        Task {
            do {
                for try await payload in session.events() {
                    continuation.yield(payload)
                    if _isTerminalOpenAIEvent(payload["type"]?.stringValue) {
                        await session.close(code: .normalClosure)
                        finishOnce.run { continuation.finish() }
                        return
                    }
                }
                finishOnce.run { continuation.finish() }
            } catch {
                finishOnce.run { continuation.finish(throwing: error) }
            }
        }
    }
}

private func _isTerminalOpenAIEvent(_ type: String?) -> Bool {
    switch type {
    case "response.completed", "response.failed", "response.incomplete", "response.error", "error":
        return true
    default:
        return false
    }
}

private final class _OpenAIFinishOnce: @unchecked Sendable {
    private let lock = NSLock()
    private var finished = false

    func run(_ body: () -> Void) {
        lock.lock()
        if finished {
            lock.unlock()
            return
        }
        finished = true
        lock.unlock()
        body()
    }
}
