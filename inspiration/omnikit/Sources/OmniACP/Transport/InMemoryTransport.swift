import Foundation

public actor InMemoryTransport: Transport {
    private var peer: InMemoryTransport?
    private var connected = false
    private let stream: AsyncThrowingStream<Data, Error>
    private var continuation: AsyncThrowingStream<Data, Error>.Continuation?

    public init() {
        var continuation: AsyncThrowingStream<Data, Error>.Continuation?
        self.stream = AsyncThrowingStream { cont in
            continuation = cont
        }
        self.continuation = continuation
    }

    public static func createConnectedPair() async -> (InMemoryTransport, InMemoryTransport) {
        let lhs = InMemoryTransport()
        let rhs = InMemoryTransport()
        await lhs.setPeer(rhs)
        await rhs.setPeer(lhs)
        return (lhs, rhs)
    }

    public var isConnected: Bool {
        connected
    }

    public func connect() async throws {
        connected = true
    }

    public func send(_ data: Data) async throws {
        guard connected else {
            throw ClientError.transportClosed
        }
        guard let peer else {
            throw ClientError.transportClosed
        }
        try await peer.receiveMessage(data)
    }

    public nonisolated func receive() -> AsyncThrowingStream<Data, Error> {
        stream
    }

    public func disconnect() async {
        connected = false
        continuation?.finish()
    }

    private func setPeer(_ peer: InMemoryTransport) {
        self.peer = peer
    }

    private func receiveMessage(_ data: Data) throws {
        guard connected else {
            throw ClientError.transportClosed
        }
        continuation?.yield(data)
    }
}
