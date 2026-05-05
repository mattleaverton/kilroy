import Foundation

public struct TimeoutConfig: Sendable, Equatable {
    public var total: Duration?
    public var perStep: Duration?

    public init(total: Duration? = nil, perStep: Duration? = nil) {
        self.total = total
        self.perStep = perStep
    }
}

public enum Timeout: Sendable, Equatable {
    case seconds(Double)
    case config(TimeoutConfig)

    public var asConfig: TimeoutConfig {
        switch self {
        case .seconds(let s):
            return TimeoutConfig(total: .milliseconds(Int64(s * 1000.0)))
        case .config(let c):
            return c
        }
    }
}

func _withOptionalTimeout<T: Sendable>(
    _ timeout: Duration?,
    operation: @Sendable @escaping () async throws -> T
) async throws -> T {
    if let timeout {
        return try await _withTimeout(timeout, operation: operation)
    }
    return try await operation()
}

func _withTimeout<T: Sendable>(
    _ timeout: Duration,
    operation: @Sendable @escaping () async throws -> T
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask {
            try await operation()
        }
        group.addTask {
            try await Task.sleep(for: timeout)
            throw RequestTimeoutError(message: "Request timed out after \(timeout)")
        }
        guard let first = try await group.next() else {
            throw RequestTimeoutError(message: "Request timed out")
        }
        group.cancelAll()
        return first
    }
}

