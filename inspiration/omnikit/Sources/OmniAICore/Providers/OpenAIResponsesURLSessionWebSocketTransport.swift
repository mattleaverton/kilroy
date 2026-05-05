import Foundation
import OmniHTTP

#if canImport(Darwin)
public struct URLSessionOpenAIResponsesWebSocketTransport: OpenAIResponsesWebSocketTransport {
    private let transport: any RealtimeWebSocketTransport

    public init(transport: any RealtimeWebSocketTransport = URLSessionRealtimeWebSocketTransport()) {
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
#endif
