import Foundation

func makeThrowingStream<Element>(of _: Element.Type = Element.self) -> (
    stream: AsyncThrowingStream<Element, Error>,
    continuation: AsyncThrowingStream<Element, Error>.Continuation
) {
    var continuation: AsyncThrowingStream<Element, Error>.Continuation?
    let stream = AsyncThrowingStream<Element, Error> { resolvedContinuation in
        continuation = resolvedContinuation
    }
    guard let continuation else {
        preconditionFailure("Failed to initialize AsyncThrowingStream continuation")
    }
    return (stream, continuation)
}
