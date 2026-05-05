import Foundation
import OmniAICore

public protocol EventProcessor: Sendable {
    func process(_ event: SessionEvent)
    func flush()
}

public final class HumanEventProcessor: @unchecked Sendable, EventProcessor {
    private let lock = NSLock()
    private var wroteText = false

    public init() {}

    public func process(_ event: SessionEvent) {
        lock.lock()
        defer { lock.unlock() }

        switch event.kind {
        case .assistantTextDelta:
            if let delta = event.stringValue(for: "delta"), !delta.isEmpty {
                writeStdout(delta)
                wroteText = true
            }
        case .assistantTextEnd:
            if wroteText {
                writeStdout("\n")
                wroteText = false
            }
        case .toolCallStart:
            let toolName = event.stringValue(for: "tool_name") ?? event.stringValue(for: "tool") ?? "tool"
            writeStderr("[tool] \(toolName) start\n")
        case .toolCallEnd:
            let toolName = event.stringValue(for: "tool_name") ?? event.stringValue(for: "tool") ?? "tool"
            if let error = event.stringValue(for: "error"), !error.isEmpty {
                writeStderr("[tool] \(toolName) error: \(error)\n")
            } else {
                writeStderr("[tool] \(toolName) done\n")
            }
        case .warning:
            if let message = event.stringValue(for: "message") {
                writeStderr("[warn] \(message)\n")
            }
        case .error:
            if let message = event.stringValue(for: "error") {
                writeStderr("[error] \(message)\n")
            }
        default:
            break
        }
    }

    public func flush() {
        lock.lock()
        defer { lock.unlock() }
        if wroteText {
            writeStdout("\n")
            wroteText = false
        }
    }

    private func writeStdout(_ text: String) {
        if let data = text.data(using: .utf8) {
            FileHandle.standardOutput.write(data)
        }
    }

    private func writeStderr(_ text: String) {
        if let data = text.data(using: .utf8) {
            FileHandle.standardError.write(data)
        }
    }
}

public final class JSONEventProcessor: @unchecked Sendable, EventProcessor {
    private let lock = NSLock()

    public init() {}

    public func process(_ event: SessionEvent) {
        lock.lock()
        defer { lock.unlock() }

        let payload: JSONValue = .object([
            "type": .string(event.kind.rawValue),
            "timestamp": .string(event.timestamp.ISO8601Format()),
            "session_id": .string(event.sessionId),
            "data": .object(event.data),
        ])

        guard let data = try? payload.data() else { return }
        if let string = String(data: data, encoding: .utf8) {
            if let newline = "\n".data(using: .utf8) {
                FileHandle.standardOutput.write(Data(string.utf8) + newline)
            }
        }
    }

    public func flush() {}
}
