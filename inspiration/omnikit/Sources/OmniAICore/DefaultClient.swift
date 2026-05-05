import Foundation

// Module-level default client support.

private final class _DefaultClientStore: @unchecked Sendable {
    static let shared = _DefaultClientStore()

    private let lock = NSLock()
    private var client: Client?

    func set(_ client: Client?) {
        lock.lock()
        self.client = client
        lock.unlock()
    }

    func getOrInitialize() throws -> Client {
        lock.lock()
        defer { lock.unlock() }
        if let client { return client }
        let created = try Client.fromEnv()
        client = created
        return created
    }
}

/// Overrides the module-level default client used by high-level functions like `generate()` and `stream()`.
public func setDefaultClient(_ client: Client?) async {
    _DefaultClientStore.shared.set(client)
}

/// Returns the module-level default client, lazily initialized from environment variables on first use.
public func defaultClient() async throws -> Client {
    try _DefaultClientStore.shared.getOrInitialize()
}
