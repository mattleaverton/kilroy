import Foundation
import OmniAICore
import OmniHTTP

public enum WebSocketMessageEncoding: String, Sendable {
    case text
    case binary
}

public struct WebSocketTransportConfiguration: Sendable {
    public var url: URL
    public var headers: HTTPHeaders
    public var timeout: Duration?
    public var transportConfiguration: TransportConfiguration
    public var messageEncoding: WebSocketMessageEncoding

    public init(
        url: URL,
        headers: HTTPHeaders = HTTPHeaders(),
        timeout: Duration? = nil,
        transportConfiguration: TransportConfiguration = .default,
        messageEncoding: WebSocketMessageEncoding = .text
    ) {
        self.url = url
        self.headers = headers
        self.timeout = timeout
        self.transportConfiguration = transportConfiguration
        self.messageEncoding = messageEncoding
    }
}

public actor WebSocketTransport: Transport {
    private let configuration: WebSocketTransportConfiguration
    private let websocketTransport: any RealtimeWebSocketTransport
    private let stream: AsyncThrowingStream<Data, Error>
    private var continuation: AsyncThrowingStream<Data, Error>.Continuation?
    private var websocketSession: (any RealtimeWebSocketSession)?
    private var receiverTask: Task<Void, Never>?
    private var connected = false

    public init(configuration: WebSocketTransportConfiguration) {
        self.configuration = configuration
        self.websocketTransport = defaultRealtimeWebSocketTransport()
        var continuationRef: AsyncThrowingStream<Data, Error>.Continuation?
        self.stream = AsyncThrowingStream { continuation in
            continuationRef = continuation
        }
        self.continuation = continuationRef
    }

    init(
        configuration: WebSocketTransportConfiguration,
        websocketTransport: any RealtimeWebSocketTransport
    ) {
        self.configuration = configuration
        self.websocketTransport = websocketTransport
        var continuationRef: AsyncThrowingStream<Data, Error>.Continuation?
        self.stream = AsyncThrowingStream { continuation in
            continuationRef = continuation
        }
        self.continuation = continuationRef
    }

    public var isConnected: Bool {
        connected
    }

    public func connect() async throws {
        guard !connected else {
            throw ClientError.alreadyConnected
        }

        let session = try await websocketTransport.connect(
            url: configuration.url,
            headers: configuration.headers,
            timeout: configuration.timeout
        )
        websocketSession = session
        connected = true
        receiverTask = Task { [weak self] in
            await self?.runReceiveLoop(session: session)
        }
    }

    public func send(_ data: Data) async throws {
        guard connected, let websocketSession else {
            throw ClientError.transportClosed
        }
        try validateMessageSize(data, label: "Outgoing websocket message")

        switch configuration.messageEncoding {
        case .text:
            guard let text = String(data: data, encoding: .utf8) else {
                throw ClientError.invalidPayload("WebSocketTransport text mode requires UTF-8 payloads")
            }
            try await websocketSession.send(text: text, timeout: configuration.timeout)
        case .binary:
            try await websocketSession.send(binary: data, timeout: configuration.timeout)
        }
    }

    public nonisolated func receive() -> AsyncThrowingStream<Data, Error> {
        stream
    }

    public func disconnect() async {
        connected = false
        let receiverTask = self.receiverTask
        self.receiverTask = nil
        let websocketSession = self.websocketSession
        self.websocketSession = nil
        let continuation = self.continuation
        self.continuation = nil

        receiverTask?.cancel()
        if let websocketSession {
            await websocketSession.close(code: .normalClosure)
        }
        continuation?.finish()
    }

    private func runReceiveLoop(session: any RealtimeWebSocketSession) async {
        do {
            for try await message in session.incomingMessages() {
                let data: Data
                switch message {
                case .text(let text):
                    data = Data(text.utf8)
                case .binary(let binary):
                    data = binary
                }
                try validateMessageSize(data, label: "Incoming websocket message")
                continuation?.yield(data)
            }
            finishReceiveLoop()
        } catch is CancellationError {
            finishReceiveLoop()
        } catch {
            finishReceiveLoop(error: error)
        }
    }

    private func finishReceiveLoop(error: Error? = nil) {
        connected = false
        receiverTask = nil
        websocketSession = nil
        let continuation = self.continuation
        self.continuation = nil
        if let error {
            continuation?.finish(throwing: error)
        } else {
            continuation?.finish()
        }
    }

    private func validateMessageSize(_ data: Data, label: String) throws {
        let maxMessageSize = configuration.transportConfiguration.maxMessageSize
        guard maxMessageSize <= 0 || data.count <= maxMessageSize else {
            throw ClientError.invalidPayload("\(label) exceeded configured max size")
        }
    }
}
