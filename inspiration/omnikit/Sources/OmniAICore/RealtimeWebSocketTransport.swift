import Foundation
import OmniHTTP

import NIOPosix
import NIOWebSocket
import WebSocketKit

public enum RealtimeWebSocketMessage: Sendable, Equatable {
    case text(String)
    case binary(Data)
}

public enum RealtimeWebSocketCloseCode: UInt16, Sendable, Equatable {
    case normalClosure = 1000
    case goingAway = 1001
    case unexpectedServerError = 1011
}

public protocol RealtimeWebSocketSession: Sendable {
    func send(text: String, timeout: Duration?) async throws
    func send(binary: Data, timeout: Duration?) async throws
    func incomingMessages() -> AsyncThrowingStream<RealtimeWebSocketMessage, Error>
    func close(code: RealtimeWebSocketCloseCode?) async
}

public protocol RealtimeWebSocketTransport: Sendable {
    func connect(
        url: URL,
        headers: OmniHTTP.HTTPHeaders,
        timeout: Duration?
    ) async throws -> any RealtimeWebSocketSession
}

public final class JSONRealtimeWebSocketSession: Sendable {
    public let base: any RealtimeWebSocketSession

    public init(base: any RealtimeWebSocketSession) {
        self.base = base
    }

    public func send(_ value: JSONValue, timeout: Duration? = nil) async throws {
        try await base.send(text: _ProviderHTTP.stringifyJSON(value), timeout: timeout)
    }

    public func events() -> AsyncThrowingStream<JSONValue, Error> {
        let base = self.base
        return AsyncThrowingStream { continuation in
            continuation.onTermination = { _ in
                Task {
                    await base.close(code: .goingAway)
                }
            }

            Task {
                do {
                    for try await message in base.incomingMessages() {
                        switch message {
                        case .text(let text):
                            guard let data = text.data(using: .utf8) else {
                                throw StreamError(message: "Realtime websocket received non-UTF8 text frame")
                            }
                            continuation.yield(try JSONValue.parse(data))
                        case .binary(let data):
                            continuation.yield(try JSONValue.parse(data))
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    public func close(code: RealtimeWebSocketCloseCode? = .normalClosure) async {
        await base.close(code: code)
    }
}

public extension RealtimeWebSocketTransport {
    func connectJSON(
        url: URL,
        headers: OmniHTTP.HTTPHeaders,
        timeout: Duration?
    ) async throws -> JSONRealtimeWebSocketSession {
        JSONRealtimeWebSocketSession(base: try await connect(url: url, headers: headers, timeout: timeout))
    }
}

public func defaultRealtimeWebSocketTransport() -> any RealtimeWebSocketTransport {
    #if canImport(Darwin)
    return URLSessionRealtimeWebSocketTransport()
    #else
    return NIORealtimeWebSocketTransport()
    #endif
}

public struct NIORealtimeWebSocketTransport: RealtimeWebSocketTransport {
    public init() {}

    public func connect(
        url: URL,
        headers: OmniHTTP.HTTPHeaders,
        timeout: Duration?
    ) async throws -> any RealtimeWebSocketSession {
        let client = WebSocketClient(eventLoopGroupProvider: .shared(MultiThreadedEventLoopGroup.singleton))
        let socket = try await _connectNIOWebSocket(client: client, url: url, headers: _toNIOWebSocketHeaders(headers), timeout: timeout)
        return NIORealtimeWebSocketSession(socket: socket, client: client)
    }
}

private final class NIORealtimeWebSocketSession: RealtimeWebSocketSession, @unchecked Sendable {
    private let socket: WebSocket
    private let client: WebSocketClient
    private let stream: AsyncThrowingStream<RealtimeWebSocketMessage, Error>
    private let continuation: AsyncThrowingStream<RealtimeWebSocketMessage, Error>.Continuation
    private let finishOnce = _RealtimeFinishOnce()

    init(socket: WebSocket, client: WebSocketClient) {
        self.socket = socket
        self.client = client
        let streamPair = _makeRealtimeMessageStream()
        self.stream = streamPair.stream
        self.continuation = streamPair.continuation
        configureCallbacks()
    }

    func send(text: String, timeout: Duration?) async throws {
        try await _sendTextOnNIOWebSocket(socket: socket, text: text, timeout: timeout)
    }

    func send(binary: Data, timeout: Duration?) async throws {
        try await _sendBinaryOnNIOWebSocket(socket: socket, data: binary, timeout: timeout)
    }

    func incomingMessages() -> AsyncThrowingStream<RealtimeWebSocketMessage, Error> {
        stream
    }

    func close(code: RealtimeWebSocketCloseCode?) async {
        let mapped = _mapNIOCloseCode(code)
        await withCheckedContinuation { continuation in
            self.socket.eventLoop.execute {
                self.socket.close(code: mapped).whenComplete { _ in
                    continuation.resume()
                }
            }
        }
        do {
            try client.syncShutdown()
        } catch {
        }
    }

    private func configureCallbacks() {
        socket.onText { [continuation] _, text in
            continuation.yield(.text(text))
        }

        socket.onBinary { [continuation] _, buffer in
            var copy = buffer
            let bytes = copy.readBytes(length: copy.readableBytes) ?? []
            continuation.yield(.binary(Data(bytes)))
        }

        socket.onClose.whenComplete { [continuation, finishOnce] result in
            finishOnce.run {
                if case .failure(let error) = result {
                    continuation.finish(throwing: error)
                } else {
                    continuation.finish()
                }
            }
        }
    }
}

#if canImport(Darwin)
public struct URLSessionRealtimeWebSocketTransport: RealtimeWebSocketTransport {
    public init() {}

    public func connect(
        url: URL,
        headers: OmniHTTP.HTTPHeaders,
        timeout: Duration?
    ) async throws -> any RealtimeWebSocketSession {
        var request = URLRequest(url: url)
        for (name, value) in headers.asDictionary {
            request.setValue(value, forHTTPHeaderField: name)
        }
        return URLSessionRealtimeWebSocketSession(request: request, timeout: timeout)
    }
}

private final class URLSessionRealtimeWebSocketSession: RealtimeWebSocketSession, @unchecked Sendable {
    private let session: URLSession
    private let task: URLSessionWebSocketTask
    private let timeout: Duration?
    private let stream: AsyncThrowingStream<RealtimeWebSocketMessage, Error>
    private let continuation: AsyncThrowingStream<RealtimeWebSocketMessage, Error>.Continuation
    private let finishOnce = _RealtimeFinishOnce()

    init(request: URLRequest, timeout: Duration?) {
        self.session = URLSession(configuration: .ephemeral)
        self.task = session.webSocketTask(with: request)
        self.timeout = timeout
        let streamPair = _makeRealtimeMessageStream()
        self.stream = streamPair.stream
        self.continuation = streamPair.continuation
        self.task.resume()
        startReceivingLoop()
    }

    func send(text: String, timeout: Duration?) async throws {
        _ = timeout
        try await task.send(.string(text))
    }

    func send(binary: Data, timeout: Duration?) async throws {
        _ = timeout
        try await task.send(.data(binary))
    }

    func incomingMessages() -> AsyncThrowingStream<RealtimeWebSocketMessage, Error> {
        stream
    }

    func close(code: RealtimeWebSocketCloseCode?) async {
        task.cancel(with: _mapURLSessionCloseCode(code), reason: nil)
        session.invalidateAndCancel()
        finishOnce.run { continuation.finish() }
    }

    private func startReceivingLoop() {
        Task {
            do {
                while !Task.isCancelled {
                    let message = try await task.receive()
                    switch message {
                    case .string(let text):
                        continuation.yield(.text(text))
                    case .data(let data):
                        continuation.yield(.binary(data))
                    @unknown default:
                        continue
                    }
                }
                finishOnce.run { continuation.finish() }
            } catch {
                finishOnce.run { continuation.finish(throwing: error) }
            }
        }
    }
}
#endif

private func _toNIOWebSocketHeaders(_ headers: OmniHTTP.HTTPHeaders) -> WebSocketKit.HTTPHeaders {
    var out: WebSocketKit.HTTPHeaders = [:]
    for (name, value) in headers.asDictionary {
        out.add(name: name, value: value)
    }
    return out
}

private func _makeRealtimeMessageStream() -> (
    stream: AsyncThrowingStream<RealtimeWebSocketMessage, Error>,
    continuation: AsyncThrowingStream<RealtimeWebSocketMessage, Error>.Continuation
) {
    var continuation: AsyncThrowingStream<RealtimeWebSocketMessage, Error>.Continuation?
    let stream = AsyncThrowingStream<RealtimeWebSocketMessage, Error> { cont in
        continuation = cont
    }
    guard let continuation else {
        preconditionFailure("Realtime websocket stream continuation was not initialized")
    }
    return (stream, continuation)
}

private func _connectNIOWebSocket(
    client: WebSocketClient,
    url: URL,
    headers: WebSocketKit.HTTPHeaders,
    timeout: Duration?
) async throws -> WebSocket {
    let scheme = (url.scheme ?? "").lowercased()
    guard scheme == "ws" || scheme == "wss", let host = url.host else {
        throw OmniHTTPError.invalidURL(url.absoluteString)
    }
    let port = url.port ?? (scheme == "wss" ? 443 : 80)
    let path = url.path.isEmpty ? "/" : url.path
    let query = url.query

    return try await withCheckedThrowingContinuation { continuation in
        let state = _RealtimeConnectResumeState()
        let future = client.connect(
            scheme: scheme,
            host: host,
            port: port,
            path: path,
            query: query,
            headers: headers
        ) { socket in
            state.resumeOnce(continuation) {
                continuation.resume(returning: socket)
            }
        }

        future.whenFailure { error in
            state.resumeOnce(continuation) {
                continuation.resume(throwing: error)
            }
        }

        if let timeout {
            let timeoutSeconds = _realtimeDurationSeconds(timeout)
            if timeoutSeconds > 0 {
                Task {
                    try? await Task.sleep(nanoseconds: UInt64(timeoutSeconds * 1_000_000_000))
                    state.resumeOnce(continuation) {
                        continuation.resume(
                            throwing: RequestTimeoutError(
                                message: "Realtime websocket connect timed out after \(timeoutSeconds)s"
                            )
                        )
                    }
                }
            }
        }
    }
}

private func _sendTextOnNIOWebSocket(
    socket: WebSocket,
    text: String,
    timeout: Duration?
) async throws {
    try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
        let state = _RealtimeConnectResumeState()

        socket.eventLoop.execute {
            socket.send(text)
            state.resumeOnce(continuation) {
                continuation.resume()
            }
        }

        if let timeout {
            let timeoutSeconds = _realtimeDurationSeconds(timeout)
            if timeoutSeconds > 0 {
                Task {
                    try? await Task.sleep(nanoseconds: UInt64(timeoutSeconds * 1_000_000_000))
                    state.resumeOnce(continuation) {
                        continuation.resume(
                            throwing: RequestTimeoutError(
                                message: "Realtime websocket send timed out after \(timeoutSeconds)s"
                            )
                        )
                    }
                }
            }
        }
    }
}

private func _sendBinaryOnNIOWebSocket(
    socket: WebSocket,
    data: Data,
    timeout: Duration?
) async throws {
    try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
        let state = _RealtimeConnectResumeState()

        socket.eventLoop.execute {
            socket.send(data)
            state.resumeOnce(continuation) {
                continuation.resume()
            }
        }

        if let timeout {
            let timeoutSeconds = _realtimeDurationSeconds(timeout)
            if timeoutSeconds > 0 {
                Task {
                    try? await Task.sleep(nanoseconds: UInt64(timeoutSeconds * 1_000_000_000))
                    state.resumeOnce(continuation) {
                        continuation.resume(
                            throwing: RequestTimeoutError(
                                message: "Realtime websocket send timed out after \(timeoutSeconds)s"
                            )
                        )
                    }
                }
            }
        }
    }
}

private func _realtimeDurationSeconds(_ duration: Duration) -> Double {
    Double(duration.components.seconds) + (Double(duration.components.attoseconds) / 1e18)
}

private func _mapNIOCloseCode(_ code: RealtimeWebSocketCloseCode?) -> WebSocketErrorCode {
    switch code ?? .goingAway {
    case .normalClosure:
        return .normalClosure
    case .goingAway:
        return .goingAway
    case .unexpectedServerError:
        return .unexpectedServerError
    }
}

#if canImport(Darwin)
private func _mapURLSessionCloseCode(_ code: RealtimeWebSocketCloseCode?) -> URLSessionWebSocketTask.CloseCode {
    switch code ?? .goingAway {
    case .normalClosure:
        return .normalClosure
    case .goingAway:
        return .goingAway
    case .unexpectedServerError:
        return .internalServerError
    }
}
#endif

private final class _RealtimeFinishOnce: @unchecked Sendable {
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

private final class _RealtimeConnectResumeState: @unchecked Sendable {
    private let lock = NSLock()
    private var resumed = false

    func resumeOnce<T>(_ continuation: CheckedContinuation<T, Error>, _ body: () -> Void) {
        lock.lock()
        if resumed {
            lock.unlock()
            return
        }
        resumed = true
        lock.unlock()
        body()
    }
}
