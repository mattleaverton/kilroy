import Testing
import Foundation
import OmniAICore
@testable import OmniAIAgent

@Suite(.tags(.e2e))
final class ToolExecutionE2ETests {

    // MARK: - Read tool: offset and limit

    @Test
    func testReadFileWithOffsetAndLimit() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let lines = (1...20).map { "Line \($0)" }.joined(separator: "\n")
        try tempDir.writeFile("numbered.txt", content: lines)
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit(
            "Read lines 5-10 from the file \(tempDir.path)/numbered.txt (use offset=5, limit=6). " +
            "Tell me the first line number you see."
        )

        let response = await lastAssistantText(from: session)
        XCTAssertTrue(
            response.contains("5") || response.contains("Line 5"),
            "Expected response to reference line 5, got: \(response)"
        )

        await session.close()
    }

    // MARK: - Write tool: parent directory creation

    @Test
    func testWriteCreatesParentDirectories() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit(
            "Create a file at \(tempDir.path)/deep/nested/dir/file.txt with the content 'deeply nested'."
        )

        let exists = tempDir.fileExists("deep/nested/dir/file.txt")
        XCTAssertTrue(exists, "Expected deeply nested file to be created")

        if exists {
            let content = try tempDir.readFile("deep/nested/dir/file.txt")
            XCTAssertTrue(content.contains("deeply nested"))
        }

        await session.close()
    }

    // MARK: - Edit tool: string replacement

    @Test
    func testEditStringReplacement() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        try tempDir.writeFile("replace.txt", content: "The quick brown fox jumps over the lazy dog")
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit(
            "In the file \(tempDir.path)/replace.txt, replace 'quick brown fox' with 'slow red cat'. " +
            "Confirm the change was made."
        )

        let content = try tempDir.readFile("replace.txt")
        XCTAssertTrue(content.contains("slow red cat"), "Expected replacement to succeed, got: \(content)")
        XCTAssertFalse(content.contains("quick brown fox"), "Expected old text to be gone")

        await session.close()
    }

    // MARK: - Bash tool: command execution

    @Test
    func testBashCommandExecution() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit("Run 'echo timeout_test' in bash and tell me the output.")

        let response = await lastAssistantText(from: session)
        XCTAssertTrue(response.contains("timeout_test"), "Expected bash output in response")

        await session.close()
    }

    // MARK: - Glob tool: pattern matching

    @Test
    func testGlobPatternMatching() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        try tempDir.writeFile("a.json", content: "{}")
        try tempDir.writeFile("b.json", content: "{}")
        try tempDir.writeFile("c.txt", content: "text")
        try tempDir.writeFile("sub/d.json", content: "{}")
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit(
            "Use the glob tool to find all .json files (pattern '**/*.json') under \(tempDir.path). " +
            "How many .json files did you find?"
        )

        let response = await lastAssistantText(from: session)
        let hasMultiple = response.contains("3") || response.lowercased().contains("three")
        XCTAssertTrue(hasMultiple, "Expected to find 3 JSON files, response: \(response)")

        await session.close()
    }

    // MARK: - Grep tool: content search

    @Test
    func testGrepContentSearch() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        try tempDir.writeFile("code.swift", content: """
        func hello() {
            print("world")
        }
        func goodbye() {
            print("farewell")
        }
        """)
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit(
            "Use grep to search for the pattern 'func.*goodbye' in \(tempDir.path)/code.swift. " +
            "What did you find?"
        )

        let response = await lastAssistantText(from: session)
        XCTAssertTrue(response.contains("goodbye"), "Expected grep to find 'goodbye' function")

        await session.close()
    }

    // MARK: - Error case: reading nonexistent file

    @Test
    func testReadNonexistentFileHandledGracefully() async throws {
        try skipUnlessAnthropic()
        let tempDir = try TempTestDir()
        let session = try makeE2ESession(provider: "anthropic", tempDir: tempDir)

        await session.submit(
            "Try to read the file \(tempDir.path)/does_not_exist.txt. " +
            "Report whether you could read it or got an error."
        )

        let response = await lastAssistantText(from: session)
        let handledError = response.lowercased().contains("not") ||
                           response.lowercased().contains("error") ||
                           response.lowercased().contains("exist") ||
                           response.lowercased().contains("found")
        XCTAssertTrue(handledError, "Expected agent to handle missing file gracefully, got: \(response)")

        let state = await session.getState()
        XCTAssertEqual(state, .idle, "Session should remain idle after handling an error")

        await session.close()
    }
}
