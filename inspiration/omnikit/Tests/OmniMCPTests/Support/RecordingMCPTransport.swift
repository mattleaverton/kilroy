import Foundation
import OmniMCP

actor RecordingMCPTransport: MCPTransport {
    private var stream: AsyncThrowingStream<Data, Error>?
    private var continuation: AsyncThrowingStream<Data, Error>.Continuation?
    private var sentPayloads: [Data] = []
    private let responder: @Sendable (Data) -> Data?

    init(responder: @Sendable @escaping (Data) -> Data? = { _ in nil }) {
        self.responder = responder
    }

    func connect() async throws {
        guard stream == nil else { return }
        var localContinuation: AsyncThrowingStream<Data, Error>.Continuation?
        let stream = AsyncThrowingStream<Data, Error> { continuation in
            localContinuation = continuation
        }
        self.stream = stream
        continuation = localContinuation
    }

    func disconnect() async {
        continuation?.finish()
        continuation = nil
        stream = nil
    }

    func send(_ data: Data) async throws {
        sentPayloads.append(data)
        if let response = responder(data) {
            continuation?.yield(response)
        }
    }

    func messageStream() async throws -> AsyncThrowingStream<Data, Error> {
        guard let stream else { throw MCPError.notConnected }
        return stream
    }

    func sentRequests() -> [Data] {
        sentPayloads
    }
}
