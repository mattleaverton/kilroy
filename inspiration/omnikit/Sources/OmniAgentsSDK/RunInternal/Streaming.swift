import Foundation

final class StreamingEventBuffer<Element: Sendable>: Sendable {
    private let continuation: AsyncThrowingStream<Element, Error>.Continuation
    let stream: AsyncThrowingStream<Element, Error>

    init() {
        let streamPair = makeThrowingStream(of: Element.self)
        self.stream = streamPair.stream
        self.continuation = streamPair.continuation
    }

    func yield(_ element: Element) {
        continuation.yield(element)
    }

    func finish() {
        continuation.finish()
    }

    func fail(_ error: Error) {
        continuation.finish(throwing: error)
    }
}

