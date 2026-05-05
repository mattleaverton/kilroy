import Foundation

public struct TransportConfiguration: Sendable {
    public var maxMessageSize: Int
    public var bufferSize: Int

    public init(maxMessageSize: Int = 0, bufferSize: Int = 65_536) {
        self.maxMessageSize = maxMessageSize
        self.bufferSize = bufferSize
    }

    public static let `default` = TransportConfiguration()
}

public protocol Transport: Sendable {
    func connect() async throws
    func send(_ data: Data) async throws
    func receive() -> AsyncThrowingStream<Data, Error>
    func disconnect() async
    var isConnected: Bool { get async }
}
