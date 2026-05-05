import Foundation
import OmniAICore

public struct SpanError: Sendable, Equatable {
    public var message: String
    public var data: [String: JSONValue]

    public init(message: String, data: [String: JSONValue] = [:]) {
        self.message = message
        self.data = data
    }
}

public protocol ErrorTracingSpan: Sendable {
    func setError(_ error: SpanError)
}

private final class ErrorTracingState: @unchecked Sendable {
    private let lock = NSLock()
    private var currentSpanProvider: (@Sendable () -> (any ErrorTracingSpan)?)?

    func setCurrentSpanProvider(_ provider: (@Sendable () -> (any ErrorTracingSpan)?)?) {
        lock.lock()
        currentSpanProvider = provider
        lock.unlock()
    }

    func currentSpan() -> (any ErrorTracingSpan)? {
        lock.lock()
        let provider = currentSpanProvider
        lock.unlock()
        return provider?()
    }
}

private enum ErrorTracingGlobalState {
    static let shared = ErrorTracingState()
}

public func setCurrentErrorTracingSpanProvider(_ provider: (@Sendable () -> (any ErrorTracingSpan)?)?) {
    ErrorTracingGlobalState.shared.setCurrentSpanProvider(provider)
}

public func attachErrorToSpan(_ span: any ErrorTracingSpan, error: SpanError) {
    span.setError(error)
}

public func attachErrorToCurrentSpan(_ error: SpanError) {
    guard let span = ErrorTracingGlobalState.shared.currentSpan() else {
        OmniAgentsLogger.warning("No span to add error \(error.message) to")
        return
    }
    attachErrorToSpan(span, error: error)
}