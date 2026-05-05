import Foundation
import OmniHTTP
import OmniHTTPNIO

public struct HTTPSSETransportConfiguration: Sendable {
    public var sendURL: URL
    public var receiveURL: URL
    public var headers: HTTPHeaders
    public var connectTimeout: Duration?
    public var requestTimeout: Duration?
    public var transportConfiguration: TransportConfiguration

    public init(
        sendURL: URL,
        receiveURL: URL,
        headers: HTTPHeaders = HTTPHeaders(),
        connectTimeout: Duration? = nil,
        requestTimeout: Duration? = nil,
        transportConfiguration: TransportConfiguration = .default
    ) {
        self.sendURL = sendURL
        self.receiveURL = receiveURL
        self.headers = headers
        self.connectTimeout = connectTimeout
        self.requestTimeout = requestTimeout
        self.transportConfiguration = transportConfiguration
    }

    public init(
        url: URL,
        headers: HTTPHeaders = HTTPHeaders(),
        connectTimeout: Duration? = nil,
        requestTimeout: Duration? = nil,
        transportConfiguration: TransportConfiguration = .default
    ) {
        self.init(
            sendURL: url,
            receiveURL: url,
            headers: headers,
            connectTimeout: connectTimeout,
            requestTimeout: requestTimeout,
            transportConfiguration: transportConfiguration
        )
    }
}

public actor HTTPSSETransport: Transport {
    private let configuration: HTTPSSETransportConfiguration
    private let httpTransport: any HTTPTransport
    private let stream: AsyncThrowingStream<Data, Error>
    private var continuation: AsyncThrowingStream<Data, Error>.Continuation?
    private var receiverTask: Task<Void, Never>?
    private var connected = false

    public init(configuration: HTTPSSETransportConfiguration) {
        self.configuration = configuration
        #if canImport(Darwin)
        self.httpTransport = URLSessionHTTPTransport()
        #else
        self.httpTransport = NIOHTTPTransport()
        #endif
        var continuationRef: AsyncThrowingStream<Data, Error>.Continuation?
        self.stream = AsyncThrowingStream { continuation in
            continuationRef = continuation
        }
        self.continuation = continuationRef
    }

    init(
        configuration: HTTPSSETransportConfiguration,
        httpTransport: any HTTPTransport
    ) {
        self.configuration = configuration
        self.httpTransport = httpTransport
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

        let response = try await httpTransport.openStream(makeReceiveRequest(), timeout: configuration.connectTimeout)
        guard (200..<300).contains(response.statusCode) else {
            throw ClientError.invalidResponse("HTTP SSE receive endpoint returned status \(response.statusCode)")
        }

        connected = true
        receiverTask = Task { [weak self] in
            await self?.runReceiveLoop(byteStream: response.body)
        }
    }

    public func send(_ data: Data) async throws {
        guard connected else {
            throw ClientError.transportClosed
        }
        try validateMessageSize(data, label: "Outgoing HTTP message")

        let response = try await httpTransport.send(makeSendRequest(body: data), timeout: configuration.requestTimeout)
        guard (200..<300).contains(response.statusCode) else {
            throw ClientError.invalidResponse("HTTP send endpoint returned status \(response.statusCode)")
        }
    }

    public nonisolated func receive() -> AsyncThrowingStream<Data, Error> {
        stream
    }

    public func disconnect() async {
        connected = false
        let receiverTask = self.receiverTask
        self.receiverTask = nil
        let continuation = self.continuation
        self.continuation = nil

        receiverTask?.cancel()
        continuation?.finish()
        try? await httpTransport.shutdown()
    }

    private func runReceiveLoop(byteStream: HTTPByteStream) async {
        do {
            for try await event in SSE.parse(byteStream) {
                guard !event.data.isEmpty else {
                    continue
                }
                guard let data = event.data.data(using: .utf8) else {
                    throw ClientError.invalidPayload("SSE event data was not valid UTF-8")
                }
                try validateMessageSize(data, label: "Incoming HTTP message")
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
        let continuation = self.continuation
        self.continuation = nil
        if let error {
            continuation?.finish(throwing: error)
        } else {
            continuation?.finish()
        }
    }

    private func makeReceiveRequest() -> HTTPRequest {
        var headers = configuration.headers
        if headers.firstValue(for: "accept") == nil {
            headers.set(name: "accept", value: "text/event-stream")
        }
        if headers.firstValue(for: "cache-control") == nil {
            headers.set(name: "cache-control", value: "no-cache")
        }
        return HTTPRequest(method: .get, url: configuration.receiveURL, headers: headers)
    }

    private func makeSendRequest(body: Data) -> HTTPRequest {
        var headers = configuration.headers
        if headers.firstValue(for: "content-type") == nil {
            headers.set(name: "content-type", value: "application/json")
        }
        if headers.firstValue(for: "accept") == nil {
            headers.set(name: "accept", value: "application/json")
        }
        return HTTPRequest(method: .post, url: configuration.sendURL, headers: headers, body: .bytes(Array(body)))
    }

    private func validateMessageSize(_ data: Data, label: String) throws {
        let maxMessageSize = configuration.transportConfiguration.maxMessageSize
        guard maxMessageSize <= 0 || data.count <= maxMessageSize else {
            throw ClientError.invalidPayload("\(label) exceeded configured max size")
        }
    }
}
