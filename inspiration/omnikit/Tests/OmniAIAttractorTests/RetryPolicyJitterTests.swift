import Testing
import Foundation
@testable import OmniAIAttractor

@Suite
final class RetryPolicyJitterTests {

    // MARK: - Jitter is bounded

    @Test
    func testJitterBoundedRandomness() throws {
        let policy = PipelineRetryPolicy(
            strategy: .standard,
            baseDelay: 1.0,
            maxDelay: 60.0,
            backoffMultiplier: 2.0,
            jitter: true
        )

        // Run multiple samples for attempt 0
        // Expected raw delay = baseDelay * 2^0 = 1.0
        // Jitter range = 1.0 * 0.25 = 0.25
        // So delay should be in [0.75, 1.25]
        for _ in 0..<50 {
            let delay = policy.delay(forAttempt: 0)
            XCTAssertGreaterThanOrEqual(delay, 0.0, "Delay should never be negative")
            XCTAssertLessThanOrEqual(delay, 1.25, "Delay should be at most 1.25 for attempt 0 with jitter")
        }
    }

    @Test
    func testJitterProducesVariation() throws {
        let policy = PipelineRetryPolicy(
            strategy: .standard,
            baseDelay: 1.0,
            maxDelay: 60.0,
            backoffMultiplier: 2.0,
            jitter: true
        )

        // Collect 20 delays and check they are not all the same
        var delays: Set<String> = []
        for _ in 0..<20 {
            let delay = policy.delay(forAttempt: 2)
            delays.insert(String(format: "%.4f", delay))
        }
        XCTAssertGreaterThan(delays.count, 1,
            "Jittered delays should produce variation, got \(delays.count) unique values")
    }

    // MARK: - No jitter produces constant values

    @Test
    func testNoJitterConstant() throws {
        let policy = PipelineRetryPolicy(
            strategy: .standard,
            baseDelay: 1.0,
            maxDelay: 60.0,
            backoffMultiplier: 2.0,
            jitter: false
        )

        // Without jitter, same attempt should always return the same delay
        let delay1 = policy.delay(forAttempt: 0)
        let delay2 = policy.delay(forAttempt: 0)
        XCTAssertEqual(delay1, delay2, "Without jitter, delays should be deterministic")
        XCTAssertEqual(delay1, 1.0, accuracy: 0.001, "Expected base delay 1.0 for attempt 0")
    }

    // MARK: - Exponential backoff works

    @Test
    func testExponentialBackoff() throws {
        let policy = PipelineRetryPolicy(
            strategy: .standard,
            baseDelay: 1.0,
            maxDelay: 60.0,
            backoffMultiplier: 2.0,
            jitter: false
        )

        // attempt 0: 1.0 * 2^0 = 1.0
        XCTAssertEqual(policy.delay(forAttempt: 0), 1.0, accuracy: 0.001)
        // attempt 1: 1.0 * 2^1 = 2.0
        XCTAssertEqual(policy.delay(forAttempt: 1), 2.0, accuracy: 0.001)
        // attempt 2: 1.0 * 2^2 = 4.0
        XCTAssertEqual(policy.delay(forAttempt: 2), 4.0, accuracy: 0.001)
        // attempt 3: 1.0 * 2^3 = 8.0
        XCTAssertEqual(policy.delay(forAttempt: 3), 8.0, accuracy: 0.001)
    }

    // MARK: - Linear backoff works

    @Test
    func testLinearBackoff() throws {
        let policy = PipelineRetryPolicy(
            strategy: .linear,
            baseDelay: 2.0,
            maxDelay: 60.0,
            backoffMultiplier: 1.0,
            jitter: false
        )

        // attempt 0: 2.0 * (0+1) = 2.0
        XCTAssertEqual(policy.delay(forAttempt: 0), 2.0, accuracy: 0.001)
        // attempt 1: 2.0 * (1+1) = 4.0
        XCTAssertEqual(policy.delay(forAttempt: 1), 4.0, accuracy: 0.001)
        // attempt 2: 2.0 * (2+1) = 6.0
        XCTAssertEqual(policy.delay(forAttempt: 2), 6.0, accuracy: 0.001)
    }

    // MARK: - Max delay cap

    @Test
    func testMaxDelayCap() throws {
        let policy = PipelineRetryPolicy(
            strategy: .standard,
            baseDelay: 1.0,
            maxDelay: 10.0,
            backoffMultiplier: 2.0,
            jitter: false
        )

        // attempt 5: 1.0 * 2^5 = 32.0, capped to 10.0
        XCTAssertEqual(policy.delay(forAttempt: 5), 10.0, accuracy: 0.001)
    }

    @Test
    func testMaxDelayCapWithJitter() throws {
        let policy = PipelineRetryPolicy(
            strategy: .standard,
            baseDelay: 1.0,
            maxDelay: 10.0,
            backoffMultiplier: 2.0,
            jitter: true
        )

        // attempt 5: raw = 32.0, capped to 10.0
        // jitter range = 10.0 * 0.25 = 2.5
        // result in [7.5, 12.5], but capped + jitter means [7.5, 12.5]
        for _ in 0..<20 {
            let delay = policy.delay(forAttempt: 5)
            XCTAssertGreaterThanOrEqual(delay, 0.0)
            // Max with jitter: 10.0 + 2.5 = 12.5
            XCTAssertLessThanOrEqual(delay, 12.5,
                "Jittered delay should not exceed cap + jitter range")
        }
    }

    // MARK: - None strategy

    @Test
    func testNoneStrategyReturnsZero() throws {
        let policy = PipelineRetryPolicy(strategy: .none)
        XCTAssertEqual(policy.delay(forAttempt: 0), 0.0)
        XCTAssertEqual(policy.delay(forAttempt: 5), 0.0)
        XCTAssertEqual(policy.delay(forAttempt: 100), 0.0)
    }

    // MARK: - Aggressive strategy

    @Test
    func testAggressiveStrategy() throws {
        let policy = PipelineRetryPolicy(
            strategy: .aggressive,
            baseDelay: 1.0,
            maxDelay: 120.0,
            backoffMultiplier: 2.0,
            jitter: false
        )

        // aggressive multiplier = backoffMultiplier * 1.5 = 3.0
        // attempt 0: 1.0 * 3.0^0 = 1.0
        XCTAssertEqual(policy.delay(forAttempt: 0), 1.0, accuracy: 0.001)
        // attempt 1: 1.0 * 3.0^1 = 3.0
        XCTAssertEqual(policy.delay(forAttempt: 1), 3.0, accuracy: 0.001)
        // attempt 2: 1.0 * 3.0^2 = 9.0
        XCTAssertEqual(policy.delay(forAttempt: 2), 9.0, accuracy: 0.001)
    }

    // MARK: - Patient strategy

    @Test
    func testPatientStrategy() throws {
        let policy = PipelineRetryPolicy(
            strategy: .patient,
            baseDelay: 1.0,
            maxDelay: 120.0,
            backoffMultiplier: 2.0,
            jitter: false
        )

        // patient: baseDelay * 2^attempt * 2.0
        // attempt 0: 1.0 * 1 * 2.0 = 2.0
        XCTAssertEqual(policy.delay(forAttempt: 0), 2.0, accuracy: 0.001)
        // attempt 1: 1.0 * 2 * 2.0 = 4.0
        XCTAssertEqual(policy.delay(forAttempt: 1), 4.0, accuracy: 0.001)
    }
}
