import Foundation

public struct RetryPolicy: Sendable {
    public var maxRetries: Int
    public var baseDelay: Duration
    public var maxDelay: Duration
    public var backoffMultiplier: Double
    public var jitter: Bool
    public var onRetry: (@Sendable (_ error: SDKError, _ attempt: Int, _ delay: Duration) -> Void)?

    // For tests: deterministic jitter.
    var _randomJitterFactor: @Sendable () -> Double

    public init(
        maxRetries: Int = 2,
        baseDelaySeconds: Double = 1.0,
        maxDelaySeconds: Double = 60.0,
        backoffMultiplier: Double = 2.0,
        jitter: Bool = true,
        onRetry: (@Sendable (_ error: SDKError, _ attempt: Int, _ delay: Duration) -> Void)? = nil
    ) {
        self.maxRetries = max(0, maxRetries)
        self.baseDelay = .milliseconds(Int64(baseDelaySeconds * 1000.0))
        self.maxDelay = .milliseconds(Int64(maxDelaySeconds * 1000.0))
        self.backoffMultiplier = backoffMultiplier
        self.jitter = jitter
        self.onRetry = onRetry
        self._randomJitterFactor = { Double.random(in: 0.5...1.5) }
    }
}

public func retry<T: Sendable>(
    policy: RetryPolicy,
    abortSignal: AbortSignal? = nil,
    operation: @Sendable @escaping () async throws -> T
) async throws -> T {
    if policy.maxRetries <= 0 {
        try await abortSignal?.check()
        return try await operation()
    }

    var attempt = 0
    while true {
        try await abortSignal?.check()
        do {
            return try await operation()
        } catch {
            let sdk: SDKError = (error as? SDKError) ?? NetworkError(message: String(describing: error), cause: error)
            guard sdk.retryable else { throw sdk }
            guard attempt < policy.maxRetries else { throw sdk }

            let maxDelaySeconds = Double(policy.maxDelay.components.seconds) + Double(policy.maxDelay.components.attoseconds) / 1e18

            // Retry-After overrides backoff when present, within maxDelay.
            if let p = sdk as? ProviderError, let ra = p.retryAfter {
                if ra > maxDelaySeconds {
                    // Spec: do NOT retry if Retry-After exceeds maxDelay.
                    throw sdk
                }
                let delay = Duration.milliseconds(Int64(ra * 1000.0))
                policy.onRetry?(sdk, attempt, delay)
                try await abortSignal?.check()
                try await Task.sleep(for: delay)
                attempt += 1
                continue
            }

            // Exponential backoff with jitter.
            let powVal = Foundation.pow(policy.backoffMultiplier, Double(attempt))
            var delaySeconds = Double(policy.baseDelay.components.seconds) + Double(policy.baseDelay.components.attoseconds) / 1e18
            delaySeconds *= powVal

            let baseDelay = Duration.milliseconds(Int64(delaySeconds * 1000.0))
            var delay = baseDelay
            if delay > policy.maxDelay { delay = policy.maxDelay }

            if policy.jitter {
                let factor = policy._randomJitterFactor()
                let secs = Double(delay.components.seconds) + Double(delay.components.attoseconds) / 1e18
                delay = .milliseconds(Int64(secs * factor * 1000.0))
                if delay > policy.maxDelay { delay = policy.maxDelay }
            }

            policy.onRetry?(sdk, attempt, delay)
            try await abortSignal?.check()
            try await Task.sleep(for: delay)
            attempt += 1
        }
    }
}

