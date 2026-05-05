import Testing
import Foundation
import OmniAICore
@testable import OmniAIAgent

@Suite(.tags(.e2e))
final class StreamingE2ETests {

    // MARK: - Streaming events emitted

    @Test
    func testStreamingEventsEmitted() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("What is the capital of France? Reply briefly.")

        let events = await session.eventEmitter.allEvents()
        let kinds = Set(events.map { $0.kind })

        XCTAssertTrue(kinds.contains(.sessionStart), "Expected sessionStart event")
        XCTAssertTrue(kinds.contains(.userInput), "Expected userInput event")
        XCTAssertTrue(kinds.contains(.assistantTextStart), "Expected assistantTextStart event")
        XCTAssertTrue(kinds.contains(.assistantTextEnd), "Expected assistantTextEnd event")
        XCTAssertTrue(kinds.contains(.sessionEnd), "Expected sessionEnd event")

        await session.close()
    }

    // MARK: - Token usage stats

    @Test
    func testTokenUsageStatsPresent() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("Say hello in one word.")

        let history = await session.getHistory()
        let hasUsage = history.contains { turn in
            if case .assistant(let t) = turn {
                return t.usage.inputTokens > 0 || t.usage.outputTokens > 0
            }
            return false
        }
        XCTAssertTrue(hasUsage, "Expected at least one assistant turn with non-zero token usage")

        await session.close()
    }

    // MARK: - Tool call events tracked

    @Test
    func testToolCallEventsTracked() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        try tempDir.writeFile("events_test.txt", content: "tracking events")
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("Read the file \(tempDir.path)/events_test.txt and tell me its content.")

        let events = await session.eventEmitter.allEvents()
        let kinds = Set(events.map { $0.kind })

        XCTAssertTrue(kinds.contains(.toolCallStart), "Expected toolCallStart event")
        XCTAssertTrue(kinds.contains(.toolCallEnd), "Expected toolCallEnd event")

        let toolStartEvents = events.filter { $0.kind == .toolCallStart }
        let hasToolName = toolStartEvents.contains { event in
            !(event.stringValue(for: "tool") ?? "").isEmpty
        }
        XCTAssertTrue(hasToolName, "Expected tool call event to include tool name")

        await session.close()
    }

    // MARK: - Text delta events contain content

    @Test
    func testTextDeltaEventsContainContent() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("Count from 1 to 5, each number on a new line.")

        let events = await session.eventEmitter.allEvents()
        let deltaEvents = events.filter { $0.kind == .assistantTextDelta }

        XCTAssertGreaterThan(deltaEvents.count, 0, "Expected at least one text delta event")

        let nonEmptyDeltas = deltaEvents.filter { event in
            let text = event.stringValue(for: "text") ?? ""
            return !text.isEmpty
        }
        XCTAssertGreaterThan(nonEmptyDeltas.count, 0, "Expected non-empty text in delta events")

        await session.close()
    }
}
