import Testing
import Foundation

@testable import OmniHTTP

@Suite
final class SSEParserTests {
    @Test
    func testSSEParsesSingleEventWithMultilineData() async throws {
        let input = """
        :comment
        event: message
        id: 1
        data: hello
        data: world

        """

        let bytes = Array(input.utf8)
        let stream: HTTPByteStream = AsyncThrowingStream { continuation in
            continuation.yield(bytes)
            continuation.finish()
        }

        var events: [SSEEvent] = []
        for try await ev in SSE.parse(stream) {
            events.append(ev)
        }

        XCTAssertEqual(events.count, 1)
        XCTAssertEqual(events[0].event, "message")
        XCTAssertEqual(events[0].id, "1")
        XCTAssertEqual(events[0].data, "hello\nworld")
    }

    @Test
    func testSSEParsesMultipleEventsAndFinalDispatchWithoutTrailingBlankLine() async throws {
        let input = """
        event: a
        data: one

        event: b
        data: two
        """

        let bytes = Array(input.utf8)
        let stream: HTTPByteStream = AsyncThrowingStream { continuation in
            // Split into chunks to exercise buffering.
            continuation.yield(Array(bytes.prefix(10)))
            continuation.yield(Array(bytes.dropFirst(10)))
            continuation.finish()
        }

        var events: [SSEEvent] = []
        for try await ev in SSE.parse(stream) {
            events.append(ev)
        }

        XCTAssertEqual(events.count, 2)
        XCTAssertEqual(events[0].event, "a")
        XCTAssertEqual(events[0].data, "one")
        XCTAssertEqual(events[1].event, "b")
        XCTAssertEqual(events[1].data, "two")
    }
}

