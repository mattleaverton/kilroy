import Foundation

private final class AgentToolStateStore: @unchecked Sendable {
    static let shared = AgentToolStateStore()

    private let lock = NSLock()
    private var scopeID: String?
    private var results: [String: Any] = [:]

    func getScopeID() -> String? {
        lock.lock()
        defer { lock.unlock() }
        return scopeID
    }

    func setScopeID(_ scopeID: String?) {
        lock.lock()
        self.scopeID = scopeID
        lock.unlock()
    }

    func record(_ result: Any, for key: String) {
        lock.lock()
        results[key] = result
        lock.unlock()
    }

    func peek(for key: String) -> Any? {
        lock.lock()
        defer { lock.unlock() }
        return results[key]
    }

    func consume(for key: String) -> Any? {
        lock.lock()
        defer { lock.unlock() }
        return results.removeValue(forKey: key)
    }
}

public func getAgentToolStateScope() -> String? {
    AgentToolStateStore.shared.getScopeID()
}

public func setAgentToolStateScope(_ scopeID: String?) {
    AgentToolStateStore.shared.setScopeID(scopeID)
}

public func recordAgentToolRunResult(_ result: Any, key: String) {
    AgentToolStateStore.shared.record(result, for: key)
}

public func peekAgentToolRunResult(key: String) -> Any? {
    AgentToolStateStore.shared.peek(for: key)
}

public func consumeAgentToolRunResult(key: String) -> Any? {
    AgentToolStateStore.shared.consume(for: key)
}

