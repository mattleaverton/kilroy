import Testing
import Foundation
import OmniAICore
@testable import OmniAIAgent

@Suite(.tags(.e2e))
final class SessionE2ETests {

    // MARK: - Single-turn text response

    @Test
    func testSingleTurnTextResponse() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("What is 2 + 2? Reply with just the number.")

        let response = await lastAssistantText(from: session)
        XCTAssertTrue(response.contains("4"), "Expected response to contain '4', got: \(response)")

        let state = await session.getState()
        XCTAssertEqual(state, .idle)

        await session.close()
    }

    // MARK: - Multi-turn conversation

    @Test
    func testMultiTurnConversation() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("Remember this number: 42. Just confirm you've noted it.")

        let firstResponse = await lastAssistantText(from: session)
        XCTAssertFalse(firstResponse.isEmpty, "Expected non-empty first response")

        await session.submit("What number did I ask you to remember? Reply with just the number.")

        let secondResponse = await lastAssistantText(from: session)
        XCTAssertTrue(secondResponse.contains("42"), "Expected response to recall '42', got: \(secondResponse)")

        let turns = await assistantTurnCount(from: session)
        XCTAssertGreaterThanOrEqual(turns, 2)

        await session.close()
    }

    // MARK: - Tool execution: read file

    @Test
    func testToolExecutionReadFile() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        try tempDir.writeFile("test.txt", content: "Hello from E2E test!\nLine 2\nLine 3")
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("Read the file test.txt and tell me what it says. Quote the first line exactly.")

        let response = await lastAssistantText(from: session)
        XCTAssertTrue(response.contains("Hello from E2E test"), "Expected response to contain file content, got: \(response)")

        let toolCalls = await totalToolCalls(from: session)
        XCTAssertGreaterThanOrEqual(toolCalls, 1, "Expected at least 1 tool call for file reading")

        await session.close()
    }

    // MARK: - Tool execution: write file

    @Test
    func testToolExecutionWriteFile() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit(
            "Create a file called output.txt in the current directory with the content 'E2E test output'. " +
            "Use the absolute path \(tempDir.path)/output.txt."
        )

        let exists = tempDir.fileExists("output.txt")
        XCTAssertTrue(exists, "Expected output.txt to be created")

        if exists {
            let content = try tempDir.readFile("output.txt")
            XCTAssertTrue(content.contains("E2E test output"), "Expected file to contain 'E2E test output', got: \(content)")
        }

        await session.close()
    }

    // MARK: - Tool execution: edit file

    @Test
    func testToolExecutionEditFile() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        try tempDir.writeFile("edit_me.txt", content: "Hello World")
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit(
            "Edit the file at \(tempDir.path)/edit_me.txt: replace 'World' with 'E2E'. " +
            "Then read the file to confirm the change."
        )

        let content = try tempDir.readFile("edit_me.txt")
        XCTAssertTrue(content.contains("E2E"), "Expected file to contain 'E2E' after edit, got: \(content)")
        XCTAssertFalse(content.contains("World"), "Expected 'World' to be replaced")

        await session.close()
    }

    // MARK: - Tool execution: bash

    @Test
    func testToolExecutionBash() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("Run the shell command 'echo hello_from_bash' and tell me the output.")

        let response = await lastAssistantText(from: session)
        XCTAssertTrue(response.contains("hello_from_bash"), "Expected bash output in response, got: \(response)")

        await session.close()
    }

    // MARK: - Tool execution: glob

    @Test
    func testToolExecutionGlob() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        try tempDir.writeFile("src/main.swift", content: "print(\"hello\")")
        try tempDir.writeFile("src/utils.swift", content: "func util() {}")
        try tempDir.writeFile("README.md", content: "# readme")
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("Use the glob tool to find all .swift files under \(tempDir.path). List the filenames you found.")

        let response = await lastAssistantText(from: session)
        XCTAssertTrue(response.contains("main.swift"), "Expected glob to find main.swift, got: \(response)")
        XCTAssertTrue(response.contains("utils.swift"), "Expected glob to find utils.swift, got: \(response)")

        await session.close()
    }

    // MARK: - Tool execution: grep

    @Test
    func testToolExecutionGrep() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        try tempDir.writeFile("haystack.txt", content: "line 1\nneedle_found_here\nline 3\n")
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("Search for the pattern 'needle' in the file \(tempDir.path)/haystack.txt using grep. What line contains it?")

        let response = await lastAssistantText(from: session)
        XCTAssertTrue(response.contains("needle"), "Expected grep to find 'needle', got: \(response)")

        await session.close()
    }

    // MARK: - Session history correctness

    @Test
    func testSessionHistoryCorrectness() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("Say exactly: 'test_marker_alpha'")

        let history = await session.getHistory()

        // Should have at least user + assistant
        XCTAssertGreaterThanOrEqual(history.count, 2)

        // First turn must be user
        if case .user(let t) = history[0] {
            XCTAssertTrue(t.content.contains("test_marker_alpha"))
        } else {
            XCTFail("Expected first turn to be user turn")
        }

        // Should have an assistant turn with our marker
        let hasAssistant = history.contains { turn in
            if case .assistant(let t) = turn {
                return t.content.contains("test_marker_alpha")
            }
            return false
        }
        XCTAssertTrue(hasAssistant, "Expected assistant turn containing 'test_marker_alpha'")

        await session.close()
    }

    // MARK: - Session close/cleanup

    @Test
    func testSessionCloseCleanup() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("Say hello")

        let stateBeforeClose = await session.getState()
        XCTAssertEqual(stateBeforeClose, .idle)

        await session.close()

        let stateAfterClose = await session.getState()
        XCTAssertEqual(stateAfterClose, .closed)
    }
}

// MARK: - E2E Tag

extension Tag {
    @Tag static var e2e: Self
}
