import Foundation
import OmniAICore

public enum EventKind: String, Sendable {
    case sessionStart = "session_start"
    case sessionEnd = "session_end"
    case userInput = "user_input"
    case assistantTextStart = "assistant_text_start"
    case assistantTextDelta = "assistant_text_delta"
    case assistantTextEnd = "assistant_text_end"
    case toolCallStart = "tool_call_start"
    case toolCallOutputDelta = "tool_call_output_delta"
    case toolCallEnd = "tool_call_end"
    case steeringInjected = "steering_injected"
    case turnLimit = "turn_limit"
    case loopDetection = "loop_detection"
    case warning = "warning"
    case error = "error"
}

public struct SessionEvent: Sendable {
    public var kind: EventKind
    public var timestamp: Date
    public var sessionId: String
    public var data: [String: JSONValue]

    public init(kind: EventKind, sessionId: String, data: [String: Any] = [:]) {
        self.kind = kind
        self.timestamp = Date()
        self.sessionId = sessionId
        self.data = SessionEvent.convertData(data)
    }

    public func stringValue(for key: String) -> String? {
        guard let value = data[key] else { return nil }
        if let string = value.stringValue {
            return string
        }
        if let bool = value.boolValue {
            return bool ? "true" : "false"
        }
        if let number = value.doubleValue {
            let rounded = number.rounded()
            if rounded == number {
                return String(Int(rounded))
            }
            return String(number)
        }
        return value.description
    }

    public func boolValue(for key: String) -> Bool? {
        data[key]?.boolValue
    }

    public func intValue(for key: String) -> Int? {
        guard let number = data[key]?.doubleValue else { return nil }
        let rounded = number.rounded()
        guard rounded == number else { return nil }
        return Int(rounded)
    }

    public func doubleValue(for key: String) -> Double? {
        data[key]?.doubleValue
    }

    private static func convertData(_ raw: [String: Any]) -> [String: JSONValue] {
        raw.reduce(into: [String: JSONValue]()) { partial, pair in
            if let jsonValue = pair.value as? JSONValue {
                partial[pair.key] = jsonValue
            } else if let converted = try? JSONValue(pair.value) {
                partial[pair.key] = converted
            } else {
                partial[pair.key] = .string(String(describing: pair.value))
            }
        }
    }
}

public actor EventEmitter {
    private var handlers: [@Sendable (SessionEvent) -> Void] = []
    private var eventBuffer: [SessionEvent] = []
    private var continuations: [AsyncStream<SessionEvent>.Continuation] = []

    public init() {}

    public func on(_ handler: @escaping @Sendable (SessionEvent) -> Void) {
        handlers.append(handler)
    }

    public func emit(_ event: SessionEvent) {
        for handler in handlers {
            handler(event)
        }
        for continuation in continuations {
            continuation.yield(event)
        }
        eventBuffer.append(event)
    }

    public func events() -> AsyncStream<SessionEvent> {
        let (stream, continuation) = AsyncStream<SessionEvent>.makeStream()
        continuations.append(continuation)
        return stream
    }

    public func flush() {
        for continuation in continuations {
            continuation.finish()
        }
        continuations.removeAll()
    }

    public func allEvents() -> [SessionEvent] {
        eventBuffer
    }
}
