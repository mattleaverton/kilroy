/// OmniKit: a small foundation module for shared utilities.
///
/// This package is scaffolded for Swift 6 with strict concurrency enabled.
public enum OmniKit {
    public static let version = "0.1.0"
}

/// A simple actor-backed counter (example API that is naturally concurrency-safe).
public actor OmniCounter {
    private var value: Int

    public init(initialValue: Int = 0) {
        self.value = initialValue
    }

    @discardableResult
    public func increment() -> Int {
        value += 1
        return value
    }

    public func current() -> Int {
        value
    }
}
