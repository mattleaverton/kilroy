import Foundation

// MARK: - Retry Policy

public enum RetryStrategy: String, Sendable {
    case none
    case standard
    case aggressive
    case linear
    case patient
}

public struct PipelineRetryPolicy: Sendable {
    public var strategy: RetryStrategy
    public var baseDelay: TimeInterval
    public var maxDelay: TimeInterval
    public var backoffMultiplier: Double
    public var jitter: Bool

    public init(
        strategy: RetryStrategy = .standard,
        baseDelay: TimeInterval = 1.0,
        maxDelay: TimeInterval = 60.0,
        backoffMultiplier: Double = 2.0,
        jitter: Bool = true
    ) {
        self.strategy = strategy
        self.baseDelay = baseDelay
        self.maxDelay = maxDelay
        self.backoffMultiplier = backoffMultiplier
        self.jitter = jitter
    }

    public func delay(forAttempt attempt: Int) -> TimeInterval {
        let raw: TimeInterval
        switch strategy {
        case .none:
            return 0
        case .standard:
            raw = baseDelay * pow(backoffMultiplier, Double(attempt))
        case .aggressive:
            raw = baseDelay * pow(backoffMultiplier * 1.5, Double(attempt))
        case .linear:
            raw = baseDelay * Double(attempt + 1)
        case .patient:
            raw = baseDelay * pow(backoffMultiplier, Double(attempt)) * 2.0
        }
        let capped = min(raw, maxDelay)
        if jitter {
            let jitterRange = capped * 0.25
            let jitterOffset = Double.random(in: -jitterRange...jitterRange)
            return max(0, capped + jitterOffset)
        }
        return capped
    }

    public static let `default` = PipelineRetryPolicy()
    public static let none = PipelineRetryPolicy(strategy: .none)
}
