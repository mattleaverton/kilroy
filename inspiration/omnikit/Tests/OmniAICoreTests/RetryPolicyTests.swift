import Testing
import Foundation

@testable import OmniAICore

@Suite
final class RetryPolicyTests {
    @Test
    func testExponentialBackoffIncreasesDelaysWhenJitterDisabled() async {
        final class Recorder: @unchecked Sendable {
            private let lock = NSLock()
            private var delays: [Duration] = []
            func add(_ d: Duration) {
                lock.lock()
                delays.append(d)
                lock.unlock()
            }
            func all() -> [Duration] {
                lock.lock()
                let v = delays
                lock.unlock()
                return v
            }
        }
        let recorder = Recorder()

        let policy = RetryPolicy(
            maxRetries: 2,
            baseDelaySeconds: 0.001,
            maxDelaySeconds: 1.0,
            backoffMultiplier: 2.0,
            jitter: false,
            onRetry: { _, _, d in recorder.add(d) }
        )

        do {
            _ = try await retry(policy: policy) {
                throw RateLimitError(
                    message: "rate limited",
                    provider: "test",
                    statusCode: 429,
                    errorCode: nil,
                    retryable: true,
                    retryAfter: nil,
                    raw: nil
                )
            }
            XCTFail("Expected RateLimitError")
        } catch is RateLimitError {
            // ok
        } catch {
            XCTFail("Unexpected error: \(error)")
        }

        let delays = recorder.all()
        XCTAssertEqual(delays.count, 2)

        func ms(_ d: Duration) -> Int64 {
            Int64(d.components.seconds) * 1000 + Int64(d.components.attoseconds) / 1_000_000_000_000_000
        }

        XCTAssertGreaterThanOrEqual(ms(delays[0]), 1)
        XCTAssertGreaterThanOrEqual(ms(delays[1]), 2)
    }

    @Test
    func testRetryAfterOverridesBackoffDelay() async throws {
        final class Recorder: @unchecked Sendable {
            private let lock = NSLock()
            private var delay: Duration?
            func set(_ d: Duration) {
                lock.lock()
                delay = d
                lock.unlock()
            }
            func get() -> Duration? {
                lock.lock()
                let v = delay
                lock.unlock()
                return v
            }
        }
        let recorder = Recorder()

        final class AttemptCounter: @unchecked Sendable {
            private let lock = NSLock()
            private var count: Int = 0
            func next() -> Int {
                lock.lock()
                defer { lock.unlock() }
                count += 1
                return count
            }
        }
        let counter = AttemptCounter()

        let policy = RetryPolicy(
            maxRetries: 1,
            baseDelaySeconds: 0.001, // would normally be ~1ms
            maxDelaySeconds: 1.0,
            backoffMultiplier: 2.0,
            jitter: false,
            onRetry: { _, _, d in recorder.set(d) }
        )

        let out = try await retry(policy: policy) {
            if counter.next() == 1 {
                throw RateLimitError(
                    message: "rate limited",
                    provider: "test",
                    statusCode: 429,
                    errorCode: nil,
                    retryable: true,
                    retryAfter: 0.01, // 10ms should override backoff
                    raw: nil
                )
            }
            return "ok"
        }

        XCTAssertEqual(out, "ok")

        let d = try XCTUnwrap(recorder.get())
        let ms = Double(d.components.seconds) * 1000.0 + Double(d.components.attoseconds) / 1e15
        XCTAssertGreaterThanOrEqual(ms, 8.0)
    }
}
