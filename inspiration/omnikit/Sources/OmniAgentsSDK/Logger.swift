import Foundation

private final class LoggerState: @unchecked Sendable {
    private let lock = NSLock()
    private var _verboseStdoutLoggingEnabled = false

    func setVerboseStdoutLoggingEnabled(_ enabled: Bool) {
        lock.lock()
        _verboseStdoutLoggingEnabled = enabled
        lock.unlock()
    }

    func verboseStdoutLoggingEnabled() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        return _verboseStdoutLoggingEnabled
    }
}

private enum LoggerGlobalState {
    static let shared = LoggerState()
}

public enum OmniAgentsLogger {
    public static var verboseStdoutLoggingEnabled: Bool {
        LoggerGlobalState.shared.verboseStdoutLoggingEnabled()
    }

    public static func setVerboseStdoutLoggingEnabled(_ enabled: Bool) {
        LoggerGlobalState.shared.setVerboseStdoutLoggingEnabled(enabled)
    }

    public static func verbose(_ message: @autoclosure () -> String) {
        guard verboseStdoutLoggingEnabled else { return }
        print("[OmniAgentsSDK] \(message())")
    }

    public static func warning(_ message: @autoclosure () -> String) {
        let formatted = "[OmniAgentsSDK][warning] \(message())\n"
        if let data = formatted.data(using: .utf8) {
            FileHandle.standardError.write(data)
        }
    }
}

public func enableVerboseStdoutLogging() {
    OmniAgentsLogger.setVerboseStdoutLoggingEnabled(true)
}
