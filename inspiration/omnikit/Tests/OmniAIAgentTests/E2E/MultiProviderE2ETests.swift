import Testing
import Foundation
import OmniAICore
@testable import OmniAIAgent

@Suite(.tags(.e2e))
final class MultiProviderE2ETests {

    // MARK: - Anthropic: simple tool-using task

    @Test
    func testAnthropicSimpleToolTask() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        try tempDir.writeFile("provider_test.txt", content: "anthropic_marker_123")
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("Read \(tempDir.path)/provider_test.txt and tell me its content.")

        let response = await lastAssistantText(from: session)
        XCTAssertTrue(response.contains("anthropic_marker_123"), "Anthropic should read file content")

        let toolCalls = await totalToolCalls(from: session)
        XCTAssertGreaterThanOrEqual(toolCalls, 1)

        await session.close()
    }

    // MARK: - OpenAI: simple tool-using task

    @Test
    func testOpenAISimpleToolTask() async throws {
        try skipUnlessOpenAI()
        let tempDir = try TempTestDir()
        try tempDir.writeFile("provider_test.txt", content: "openai_marker_456")
        let session = try makeE2ESession(provider: "openai", tempDir: tempDir)

        await session.submit("Read the file provider_test.txt in the current directory and tell me its content.")

        let response = await lastAssistantText(from: session)
        XCTAssertTrue(response.contains("openai_marker_456"), "OpenAI should read file content, got: \(response)")

        let toolCalls = await totalToolCalls(from: session)
        XCTAssertGreaterThanOrEqual(toolCalls, 1)

        await session.close()
    }

    // MARK: - Gemini: simple tool-using task

    @Test
    func testGeminiSimpleToolTask() async throws {
        try skipUnlessGemini()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "gemini", tempDir: tempDir)

        await session.submit("Reply with exactly: GEMINI_E2E_OK")

        let state = await session.getState()
        XCTAssertEqual(state, .idle, "Gemini session should remain healthy after submit")

        await session.close()
    }

    // MARK: - Cross-provider: write and verify

    @Test
    func testAnthropicWriteAndVerify() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit(
            "Create a file at \(tempDir.path)/cross_test.txt with the text 'cross_provider_check'. " +
            "Then read it back and confirm the content."
        )

        let exists = tempDir.fileExists("cross_test.txt")
        XCTAssertTrue(exists, "File should be created")

        if exists {
            let content = try tempDir.readFile("cross_test.txt")
            XCTAssertTrue(content.contains("cross_provider_check"))
        }

        let toolCalls = await totalToolCalls(from: session)
        XCTAssertGreaterThanOrEqual(toolCalls, 2, "Expected at least write + read tool calls")

        await session.close()
    }

    // MARK: - Groq: text-only response

    @Test
    func testGroqSimpleResponse() async throws {
        try skipUnlessGroq()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "groq", tempDir: tempDir)

        await session.submit("Reply with exactly: GROQ_E2E_OK")

        let response = await lastAssistantText(from: session)
        XCTAssertTrue(response.contains("GROQ_E2E_OK"), "Groq should return marker text, got: \(response)")

        await session.close()
    }

    // MARK: - Cerebras: text-only response

    @Test
    func testCerebrasSimpleResponse() async throws {
        try skipUnlessCerebras()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "cerebras", tempDir: tempDir)

        await session.submit("Reply with exactly: CEREBRAS_E2E_OK")

        let response = await lastAssistantText(from: session)
        XCTAssertTrue(response.contains("CEREBRAS_E2E_OK"), "Cerebras should return marker text, got: \(response)")

        await session.close()
    }
}
