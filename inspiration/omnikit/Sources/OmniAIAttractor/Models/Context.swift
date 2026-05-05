import Foundation

// MARK: - Pipeline Context

// Safety: @unchecked Sendable — all mutable state (store, log) is guarded by
// `lock`. Individual get/set operations are atomic, but compound read-then-write
// sequences (e.g. getString + set) are NOT atomic. Callers requiring atomicity
// across multiple operations should use applyUpdates() or coordinate externally.
// ParallelHandler branches receive cloned contexts; only the shared parent
// context is written to after all branches complete.
public final class PipelineContext: @unchecked Sendable {
    private let lock = NSLock()
    private var store: [String: any Sendable]
    private var log: [String]

    public init(_ initial: [String: any Sendable] = [:]) {
        self.store = initial
        self.log = []
    }

    public func set<Value: Sendable>(_ key: String, _ value: Value) {
        lock.lock()
        defer { lock.unlock() }
        store[key] = value
    }

    public func get(_ key: String, default defaultValue: (any Sendable)? = nil) -> (any Sendable)? {
        lock.lock()
        defer { lock.unlock() }
        return store[key] ?? defaultValue
    }

    public func getString(_ key: String, default defaultValue: String = "") -> String {
        guard let val = get(key) else { return defaultValue }
        if let s = val as? String { return s }
        return String(describing: val)
    }

    public func getInt(_ key: String, default defaultValue: Int = 0) -> Int {
        guard let val = get(key) else { return defaultValue }
        if let i = val as? Int { return i }
        if let s = val as? String, let i = Int(s) { return i }
        return defaultValue
    }

    public func getBool(_ key: String, default defaultValue: Bool = false) -> Bool {
        guard let val = get(key) else { return defaultValue }
        if let b = val as? Bool { return b }
        if let s = val as? String { return s == "true" }
        return defaultValue
    }

    public func appendLog(_ entry: String) {
        lock.lock()
        defer { lock.unlock() }
        log.append(entry)
    }

    public func getLogs() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return log
    }

    public func snapshot() -> [String: any Sendable] {
        lock.lock()
        defer { lock.unlock() }
        return store
    }

    public func clone() -> PipelineContext {
        lock.lock()
        defer { lock.unlock() }
        let ctx = PipelineContext(store)
        ctx.log = log
        return ctx
    }

    public func applyUpdates(_ updates: [String: any Sendable]) {
        lock.lock()
        defer { lock.unlock() }
        for (key, value) in updates {
            store[key] = value
        }
    }

    public func applyUpdates(_ updates: [String: String]) {
        lock.lock()
        defer { lock.unlock() }
        for (key, value) in updates {
            store[key] = value
        }
    }

    public func remove(_ key: String) {
        lock.lock()
        defer { lock.unlock() }
        store.removeValue(forKey: key)
    }

    public var keys: [String] {
        lock.lock()
        defer { lock.unlock() }
        return Array(store.keys)
    }

    // Serializable snapshot for checkpointing
    public func serializableSnapshot() -> [String: String] {
        lock.lock()
        defer { lock.unlock() }
        var result: [String: String] = [:]
        for (key, value) in store {
            if let s = value as? String { result[key] = s }
            else if let i = value as? Int { result[key] = String(i) }
            else if let b = value as? Bool { result[key] = b ? "true" : "false" }
            else if let d = value as? Double { result[key] = String(d) }
            else { result[key] = String(describing: value) }
        }
        return result
    }
}
