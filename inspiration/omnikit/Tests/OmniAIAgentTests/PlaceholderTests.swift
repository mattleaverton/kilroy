import Testing
import Foundation
import OmniAICore
@testable import OmniAIAgent

@Suite
final class TruncationTests {

    // MARK: - Character-based truncation

    @Test
    func testTruncationHeadTailInsertsWarning() {
        let long = String(repeating: "x", count: 100)
        let result = truncateOutput(long, maxChars: 50, mode: "head_tail")
        XCTAssertTrue(result.contains("WARNING"))
        XCTAssertTrue(result.contains("characters were removed from the middle"))
    }

    @Test
    func testTruncationShortStringPassesThrough() {
        let short = "hello"
        let result = truncateOutput(short, maxChars: 50, mode: "head_tail")
        XCTAssertEqual(result, "hello")
    }

    @Test
    func testTruncationTailMode() {
        let long = String(repeating: "a", count: 200)
        let result = truncateOutput(long, maxChars: 50, mode: "tail")
        XCTAssertTrue(result.contains("WARNING"))
        XCTAssertTrue(result.contains("characters were removed"))
        // The tail portion should end with 'a's
        XCTAssertTrue(result.hasSuffix(String(repeating: "a", count: 50)))
    }

    @Test
    func testTruncationExactLengthPassesThrough() {
        let exact = String(repeating: "z", count: 50)
        let result = truncateOutput(exact, maxChars: 50, mode: "head_tail")
        XCTAssertEqual(result, exact)
    }

    @Test
    func testTruncationHeadTailPreservesHeadAndTail() {
        let head = String(repeating: "H", count: 50)
        let middle = String(repeating: "M", count: 100)
        let tail = String(repeating: "T", count: 50)
        let input = head + middle + tail
        let result = truncateOutput(input, maxChars: 100, mode: "head_tail")
        // Head should start with H's
        XCTAssertTrue(result.hasPrefix(String(repeating: "H", count: 50)))
        // Tail should end with T's
        XCTAssertTrue(result.hasSuffix(String(repeating: "T", count: 50)))
        XCTAssertTrue(result.contains("WARNING"))
    }

    // MARK: - Line-based truncation

    @Test
    func testLineTruncationShortPassesThrough() {
        let input = "line1\nline2\nline3"
        let result = truncateLines(input, maxLines: 10)
        XCTAssertEqual(result, input)
    }

    @Test
    func testLineTruncationInsertsOmittedMarker() {
        let lines = (1...20).map { "line \($0)" }.joined(separator: "\n")
        let result = truncateLines(lines, maxLines: 6)
        XCTAssertTrue(result.contains("lines omitted"))
        XCTAssertTrue(result.contains("line 1"))   // Head preserved
        XCTAssertTrue(result.contains("line 20"))  // Tail preserved
    }

    // MARK: - Combined truncation pipeline

    @Test
    func testTruncateToolOutputUsesDefaults() {
        let config = SessionConfig()
        let short = "hello"
        let result = truncateToolOutput(short, toolName: "read_file", config: config)
        XCTAssertEqual(result, "hello")
    }

    @Test
    func testTruncateToolOutputRespectsCustomLimits() {
        var config = SessionConfig()
        config.toolOutputLimits["shell"] = 10
        let long = String(repeating: "x", count: 100)
        let result = truncateToolOutput(long, toolName: "shell", config: config)
        XCTAssertTrue(result.contains("WARNING"))
    }
}

@Suite
final class SessionConfigTests {

    @Test
    func testDefaultValues() {
        let config = SessionConfig()
        XCTAssertEqual(config.maxTurns, 0)
        XCTAssertEqual(config.maxToolRoundsPerInput, 0)
        XCTAssertFalse(config.interactiveMode)
        XCTAssertEqual(config.defaultCommandTimeoutMs, 10_000)
        XCTAssertEqual(config.maxCommandTimeoutMs, 600_000)
        XCTAssertNil(config.reasoningEffort)
        XCTAssertTrue(config.toolOutputLimits.isEmpty)
        XCTAssertTrue(config.toolLineLimits.isEmpty)
        XCTAssertTrue(config.enableLoopDetection)
        XCTAssertEqual(config.loopDetectionWindow, 10)
        XCTAssertEqual(config.maxSubagentDepth, 1)
        XCTAssertNil(config.userInstructions)
        XCTAssertNil(config.parallelToolCalls)
    }

    @Test
    func testCustomValues() {
        let config = SessionConfig(
            maxTurns: 5,
            maxToolRoundsPerInput: 50,
            interactiveMode: true,
            defaultCommandTimeoutMs: 20_000,
            maxCommandTimeoutMs: 300_000,
            reasoningEffort: "high",
            toolOutputLimits: ["shell": 1000],
            toolLineLimits: ["shell": 100],
            enableLoopDetection: false,
            loopDetectionWindow: 5,
            maxSubagentDepth: 2,
            userInstructions: "Always use tabs",
            parallelToolCalls: true
        )
        XCTAssertEqual(config.maxTurns, 5)
        XCTAssertEqual(config.maxToolRoundsPerInput, 50)
        XCTAssertTrue(config.interactiveMode)
        XCTAssertEqual(config.defaultCommandTimeoutMs, 20_000)
        XCTAssertEqual(config.maxCommandTimeoutMs, 300_000)
        XCTAssertEqual(config.reasoningEffort, "high")
        XCTAssertEqual(config.toolOutputLimits["shell"], 1000)
        XCTAssertEqual(config.toolLineLimits["shell"], 100)
        XCTAssertFalse(config.enableLoopDetection)
        XCTAssertEqual(config.loopDetectionWindow, 5)
        XCTAssertEqual(config.maxSubagentDepth, 2)
        XCTAssertEqual(config.userInstructions, "Always use tabs")
        XCTAssertEqual(config.parallelToolCalls, true)
    }
}

@Suite
final class ToolRegistryTests {

    @Test
    func testAnthropicProfileHasExpectedTools() {
        let profile = AnthropicProfile()
        let names = Set(profile.toolRegistry.names())
        XCTAssertTrue(names.contains("Read"))
        XCTAssertTrue(names.contains("Write"))
        XCTAssertTrue(names.contains("Edit"))
        XCTAssertTrue(names.contains("Glob"))
        XCTAssertTrue(names.contains("Grep"))
        XCTAssertTrue(names.contains("NotebookEdit"))
        XCTAssertTrue(names.contains("WebFetch"))
        XCTAssertTrue(names.contains("WebSearch"))
        XCTAssertTrue(names.contains("Bash"))
        XCTAssertTrue(names.contains("TaskStop"))
        XCTAssertTrue(names.contains("Task"))
        XCTAssertTrue(names.contains("TaskCreate"))
        XCTAssertTrue(names.contains("TaskGet"))
        XCTAssertTrue(names.contains("TaskList"))
        XCTAssertTrue(names.contains("TaskUpdate"))
        XCTAssertTrue(names.contains("TeamCreate"))
        XCTAssertTrue(names.contains("TeamDelete"))
        XCTAssertTrue(names.contains("SendMessage"))
        XCTAssertTrue(names.contains("ToolSearch"))
        XCTAssertTrue(names.contains("AskUserQuestion"))
        XCTAssertTrue(names.contains("EnterPlanMode"))
        XCTAssertTrue(names.contains("ExitPlanMode"))
        XCTAssertTrue(names.contains("Skill"))
        XCTAssertTrue(names.contains("TaskOutput"))
        XCTAssertTrue(names.contains("TodoWrite"))
        XCTAssertTrue(names.contains("view_image"))
        // Anthropic should NOT have apply_patch
        XCTAssertFalse(names.contains("apply_patch"))
    }

    @Test
    func testOpenAIProfileHasExpectedTools() {
        let profile = OpenAIProfile()
        let names = Set(profile.toolRegistry.names())
        XCTAssertTrue(names.contains("read_file"))
        XCTAssertTrue(names.contains("apply_patch"))
        XCTAssertTrue(names.contains("exec_command"))
        XCTAssertTrue(names.contains("write_stdin"))
        XCTAssertTrue(names.contains("grep_files"))
        XCTAssertTrue(names.contains("list_dir"))
        XCTAssertTrue(names.contains("update_plan"))
        XCTAssertTrue(names.contains("view_image"))
        XCTAssertFalse(names.contains("write_file"))
        XCTAssertFalse(names.contains("edit_file"))
        XCTAssertFalse(names.contains("web_search"))
        XCTAssertFalse(names.contains("web_fetch"))
    }

    @Test
    func testGeminiProfileHasExpectedTools() {
        let profile = GeminiProfile()
        let names = Set(profile.toolRegistry.names())
        XCTAssertTrue(names.contains("read_file"))
        XCTAssertTrue(names.contains("read_many_files"))
        XCTAssertTrue(names.contains("write_file"))
        XCTAssertTrue(names.contains("replace"))
        XCTAssertTrue(names.contains("run_shell_command"))
        XCTAssertTrue(names.contains("grep_search"))
        XCTAssertTrue(names.contains("glob"))
        XCTAssertTrue(names.contains("list_directory"))
        XCTAssertTrue(names.contains("google_web_search"))
        XCTAssertTrue(names.contains("web_fetch"))
        XCTAssertTrue(names.contains("write_todos"))
        XCTAssertTrue(names.contains("save_memory"))
        XCTAssertTrue(names.contains("get_internal_docs"))
        XCTAssertTrue(names.contains("activate_skill"))
        XCTAssertTrue(names.contains("ask_user"))
        XCTAssertTrue(names.contains("enter_plan_mode"))
        XCTAssertTrue(names.contains("exit_plan_mode"))
        XCTAssertTrue(names.contains("view_image"))
    }

    @Test
    func testToolRegistryRegisterAndGet() {
        let registry = ToolRegistry()
        let tool = RegisteredTool(
            definition: AgentToolDefinition(
                name: "test_tool",
                description: "A test tool",
                parameters: ["type": "object", "properties": [:] as [String: Any]] as [String: Any]
            ),
            executor: { _, _ in "ok" }
        )
        registry.register(tool)
        XCTAssertNotNil(registry.get("test_tool"))
        XCTAssertNil(registry.get("nonexistent"))
    }

    @Test
    func testToolRegistryOverridesOnNameCollision() {
        let registry = ToolRegistry()
        let tool1 = RegisteredTool(
            definition: AgentToolDefinition(
                name: "my_tool",
                description: "Version 1",
                parameters: ["type": "object", "properties": [:] as [String: Any]] as [String: Any]
            ),
            executor: { _, _ in "v1" }
        )
        let tool2 = RegisteredTool(
            definition: AgentToolDefinition(
                name: "my_tool",
                description: "Version 2",
                parameters: ["type": "object", "properties": [:] as [String: Any]] as [String: Any]
            ),
            executor: { _, _ in "v2" }
        )
        registry.register(tool1)
        registry.register(tool2)
        XCTAssertEqual(registry.get("my_tool")?.definition.description, "Version 2")
    }

    @Test
    func testToolRegistryUnregister() {
        let registry = ToolRegistry()
        let tool = RegisteredTool(
            definition: AgentToolDefinition(
                name: "temp_tool",
                description: "Temporary",
                parameters: ["type": "object", "properties": [:] as [String: Any]] as [String: Any]
            ),
            executor: { _, _ in "ok" }
        )
        registry.register(tool)
        XCTAssertNotNil(registry.get("temp_tool"))
        registry.unregister("temp_tool")
        XCTAssertNil(registry.get("temp_tool"))
    }
}

@Suite
final class LocalExecutionEnvironmentTests {
    @Test
    func testReadFileSupportsBinaryPNGWithMetadata() async throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("omnikit-image-test-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let pngPath = tempDir.appendingPathComponent("tiny.png")
        let base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGNgYAAAAAMAASsJTYQAAAAASUVORK5CYII="
        let pngData = try XCTUnwrap(Data(base64Encoded: base64))
        try pngData.write(to: pngPath)

        let env = LocalExecutionEnvironment(workingDir: tempDir.path)
        let output = try await env.readFile(path: pngPath.path, offset: nil, limit: nil)

        XCTAssertTrue(output.contains("MIME type: image/png"))
        XCTAssertTrue(output.contains("Dimensions: 1x1"))
        XCTAssertTrue(output.contains("Base64:"))
        XCTAssertTrue(output.contains(base64))
    }
}

@Suite
final class ClaudeParityToolBehaviorTests {
    @Test
    func testNotebookEditReplaceAndInsert() async throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("omnikit-notebook-test-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let notebookPath = tempDir.appendingPathComponent("sample.ipynb")
        let notebook: [String: Any] = [
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": [
                "language_info": ["name": "python"],
            ],
            "cells": [
                [
                    "id": "cell-1",
                    "cell_type": "code",
                    "metadata": [:] as [String: Any],
                    "execution_count": NSNull(),
                    "outputs": [] as [Any],
                    "source": ["print('a')\n"],
                ] as [String: Any],
                [
                    "id": "cell-2",
                    "cell_type": "code",
                    "metadata": [:] as [String: Any],
                    "execution_count": NSNull(),
                    "outputs": [] as [Any],
                    "source": ["print('b')\n"],
                ] as [String: Any],
            ] as [Any],
        ]

        let initialData = try JSONSerialization.data(withJSONObject: notebook, options: [.prettyPrinted, .sortedKeys])
        try initialData.write(to: notebookPath)

        let env = LocalExecutionEnvironment(workingDir: tempDir.path)
        try await env.initialize()

        let tool = claudeNotebookEditTool()

        _ = try await tool.executor(
            [
                "notebook_path": notebookPath.path,
                "cell_number": 1,
                "new_source": "print('updated')",
                "edit_mode": "replace",
            ],
            env
        )

        _ = try await tool.executor(
            [
                "notebook_path": notebookPath.path,
                "cell_id": "cell-1",
                "new_source": "# inserted markdown",
                "cell_type": "markdown",
                "edit_mode": "insert",
            ],
            env
        )

        let finalData = try Data(contentsOf: notebookPath)
        guard
            let object = try JSONSerialization.jsonObject(with: finalData) as? [String: Any],
            let cells = object["cells"] as? [[String: Any]]
        else {
            XCTFail("Notebook JSON missing cells")
            return
        }

        XCTAssertEqual(cells.count, 3)
        XCTAssertEqual(cells[1]["cell_type"] as? String, "markdown")
        let replacedSource = (cells[2]["source"] as? [String])?.joined() ?? ""
        XCTAssertTrue(replacedSource.contains("updated"))
    }

    @Test
    func testEnterAndExitPlanModeNonInteractive() async throws {
        let tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent("omnikit-planmode-test-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: tempDir) }

        let env = LocalExecutionEnvironment(workingDir: tempDir.path)
        try await env.initialize()

        let mockAdapter = MockProviderAdapter(fixedResponse: Response(
            id: "resp-tool-test",
            model: "mock-model",
            provider: "mock",
            message: .assistant("ok"),
            finishReason: .stop,
            usage: Usage(inputTokens: 1, outputTokens: 1)
        ))
        let client = try Client(providers: ["mock": mockAdapter], defaultProvider: "mock")
        let config = SessionConfig(interactiveMode: false)
        let session = try Session(
            profile: AnthropicProfile(),
            environment: env,
            client: client,
            config: config
        )

        let enterTool = claudeEnterPlanModeTool(parentSession: session)
        let enterOutput = try await enterTool.executor([:], env)
        XCTAssertTrue(enterOutput.contains("Plan mode activated."))

        let planPath = tempDir
            .appendingPathComponent(".claude/plans/current_plan.md")
            .path
        let planExists = await env.fileExists(path: planPath)
        XCTAssertTrue(planExists)

        let exitTool = claudeExitPlanModeTool(parentSession: session)
        let exitOutput = try await exitTool.executor(
            [
                "allowedPrompts": [
                    ["tool": "Bash", "prompt": "run tests"],
                ],
            ],
            env
        )
        XCTAssertTrue(exitOutput.contains("Plan approved (non-interactive mode)."))
        XCTAssertTrue(exitOutput.contains("Allowed prompts:"))

        let history = await session.getHistory()
        let reminders = history.compactMap { turn -> String? in
            if case .system(let systemTurn) = turn {
                return systemTurn.content
            }
            return nil
        }
        XCTAssertFalse(reminders.isEmpty)
    }
}

@Suite
final class SystemPromptTests {

    @Test
    func testAnthropicPromptContainsEnvironmentBlock() {
        let profile = AnthropicProfile()
        let env = MockExecutionEnvironment()
        let prompt = profile.buildSystemPrompt(
            environment: env, projectDocs: nil, userInstructions: nil, gitContext: nil
        )
        XCTAssertTrue(prompt.contains("<env>"))
        XCTAssertTrue(prompt.contains("</env>"))
        XCTAssertTrue(prompt.contains("Platform:"))
        XCTAssertTrue(prompt.contains("Working directory:"))
        XCTAssertTrue(prompt.contains("Assistant knowledge cutoff is May 2025."))
        XCTAssertTrue(prompt.contains("Is directory a git repo: No"))
    }

    @Test
    func testOpenAIPromptContainsEnvironmentBlock() {
        let profile = OpenAIProfile()
        let env = MockExecutionEnvironment()
        let prompt = profile.buildSystemPrompt(
            environment: env, projectDocs: nil, userInstructions: nil, gitContext: nil
        )
        XCTAssertTrue(prompt.contains("# Environment Context"))
        XCTAssertTrue(prompt.contains("Working directory:"))
        XCTAssertTrue(prompt.contains("Platform:"))
        XCTAssertTrue(prompt.contains("When using shell tools, use \".\""))
    }

    @Test
    func testGeminiPromptContainsEnvironmentBlock() {
        let profile = GeminiProfile()
        let env = MockExecutionEnvironment()
        let prompt = profile.buildSystemPrompt(
            environment: env, projectDocs: nil, userInstructions: nil, gitContext: nil
        )
        XCTAssertTrue(prompt.contains("<env>"))
        XCTAssertTrue(prompt.contains("Working directory:"))
        XCTAssertTrue(prompt.contains("Platform:"))
        XCTAssertTrue(prompt.contains("GEMINI.md"))
        XCTAssertTrue(prompt.contains("run_shell_command"))
    }

    @Test
    func testGitContextIncludedInPrompt() {
        let profile = AnthropicProfile()
        let env = MockExecutionEnvironment()
        let git = GitContext(branch: "feature/test", modifiedFileCount: 3, recentCommits: "abc1234 Initial commit")
        let prompt = profile.buildSystemPrompt(
            environment: env, projectDocs: nil, userInstructions: nil, gitContext: git
        )
        XCTAssertTrue(prompt.contains("Is directory a git repo: Yes"))
        XCTAssertTrue(prompt.contains("Current branch: feature/test"))
        XCTAssertTrue(prompt.contains("Status: Has uncommitted changes"))
        XCTAssertTrue(prompt.contains("abc1234 Initial commit"))
    }

    @Test
    func testClaudePromptIncludesExternalDocsAndInstructions() {
        let profile = AnthropicProfile()
        let env = MockExecutionEnvironment()
        let prompt = profile.buildSystemPrompt(
            environment: env, projectDocs: "Some docs", userInstructions: "Always use tabs", gitContext: nil
        )
        XCTAssertTrue(prompt.contains("Some docs"))
        XCTAssertTrue(prompt.contains("Always use tabs"))
    }

    @Test
    func testProjectDocsIncludedInPrompt() {
        let profile = OpenAIProfile()
        let env = MockExecutionEnvironment()
        let prompt = profile.buildSystemPrompt(
            environment: env, projectDocs: "# My Project\nUse Python 3.12", userInstructions: nil, gitContext: nil
        )
        XCTAssertTrue(prompt.contains("# Environment Context"))
        XCTAssertTrue(prompt.contains("Working directory:"))
        XCTAssertTrue(prompt.contains("# Project Documentation"))
        XCTAssertTrue(prompt.contains("Use Python 3.12"))
    }

    @Test
    func testUserInstructionsIncludedInPrompt() {
        let profile = OpenAIProfile()
        let env = MockExecutionEnvironment()
        let prompt = profile.buildSystemPrompt(
            environment: env,
            projectDocs: nil,
            userInstructions: "Never use force push",
            gitContext: nil
        )
        XCTAssertTrue(prompt.contains("# User Instructions"))
        XCTAssertTrue(prompt.contains("Never use force push"))
    }

    @Test
    func testGeminiPromptRespectsInteractiveModeOption() {
        let profile = GeminiProfile(interactiveMode: false)
        let env = MockExecutionEnvironment()
        let prompt = profile.buildSystemPrompt(
            environment: env,
            projectDocs: nil,
            userInstructions: nil,
            gitContext: nil
        )
        XCTAssertTrue(prompt.contains("autonomous CLI agent"))
    }
}

@Suite
final class HistoryConversionTests {

    @Test
    func testTurnTypes() {
        // Test that all turn types can be created
        let userTurn = Turn.user(UserTurn(content: "hello"))
        let assistantTurn = Turn.assistant(AssistantTurn(content: "hi"))
        let systemTurn = Turn.system(SystemTurn(content: "system msg"))
        let steeringTurn = Turn.steering(SteeringTurn(content: "redirect"))

        // Verify they hold their content
        if case .user(let t) = userTurn { XCTAssertEqual(t.content, "hello") }
        else { XCTFail("Expected user turn") }

        if case .assistant(let t) = assistantTurn { XCTAssertEqual(t.content, "hi") }
        else { XCTFail("Expected assistant turn") }

        if case .system(let t) = systemTurn { XCTAssertEqual(t.content, "system msg") }
        else { XCTFail("Expected system turn") }

        if case .steering(let t) = steeringTurn { XCTAssertEqual(t.content, "redirect") }
        else { XCTFail("Expected steering turn") }
    }

    @Test
    func testToolResultsTurn() {
        // Verify ToolResultsTurn stores results
        let results = [
            ToolResult(toolCallId: "1", content: .string("ok"), isError: false),
            ToolResult(toolCallId: "2", content: .string("error"), isError: true),
        ]
        let turn = ToolResultsTurn(results: results)
        XCTAssertEqual(turn.results.count, 2)
        XCTAssertFalse(turn.results[0].isError)
        XCTAssertTrue(turn.results[1].isError)
    }
}

@Suite
final class LoopDetectionTests {

    @Test
    func testRepeatingPatternOfLength1Detected() {
        // Simulate: same tool call repeated 10 times
        // We test the signature extraction logic pattern
        let signatures = Array(repeating: "read_file:abc123", count: 10)
        XCTAssertTrue(detectRepeatingPattern(signatures, window: 10))
    }

    @Test
    func testRepeatingPatternOfLength2Detected() {
        let pattern = ["read_file:abc", "write_file:def"]
        let signatures = Array(repeating: pattern, count: 5).flatMap { $0 }
        XCTAssertTrue(detectRepeatingPattern(signatures, window: 10))
    }

    @Test
    func testNonRepeatingPatternNotDetected() {
        let signatures = (1...10).map { "tool_\($0):hash_\($0)" }
        XCTAssertFalse(detectRepeatingPattern(signatures, window: 10))
    }

    @Test
    func testTooFewSignaturesNotDetected() {
        let signatures = ["read_file:abc", "write_file:def"]
        XCTAssertFalse(detectRepeatingPattern(signatures, window: 10))
    }

    // Helper that mirrors Session's detectLoop logic
    private func detectRepeatingPattern(_ signatures: [String], window: Int) -> Bool {
        guard signatures.count >= window else { return false }
        let recent = Array(signatures.suffix(window))
        for patternLen in [1, 2, 3] {
            guard window % patternLen == 0 else { continue }
            let pattern = Array(recent[0..<patternLen])
            var allMatch = true
            for i in stride(from: patternLen, to: window, by: patternLen) {
                let chunk = Array(recent[i..<min(i + patternLen, recent.count)])
                if chunk != pattern {
                    allMatch = false
                    break
                }
            }
            if allMatch { return true }
        }
        return false
    }
}

@Suite
final class SessionLifecycleTests {

    @Test
    func testSessionFallsBackToCompleteWhenStreamingProducesNoResponse() async throws {
        let mockAdapter = MockProviderAdapter(fixedResponse: Response(
            id: "resp-1",
            model: "mock-model",
            provider: "anthropic",
            message: .assistant("Fallback response"),
            finishReason: .stop,
            usage: .zero
        ))
        let client = try Client(providers: ["anthropic": mockAdapter], defaultProvider: "anthropic")
        let env = MockExecutionEnvironment()
        let profile = AnthropicProfile()
        let session = try Session(profile: profile, environment: env, client: client)

        await session.submit("Use fallback")

        let history = await session.getHistory()
        XCTAssertGreaterThanOrEqual(history.count, 2)

        if case .assistant(let turn) = history[1] {
            XCTAssertEqual(turn.content, "Fallback response")
        } else {
            XCTFail("Expected assistant turn after fallback response")
        }

        let counts = await mockAdapter.callStats.snapshot()
        XCTAssertEqual(counts.stream, 1)
        XCTAssertEqual(counts.complete, 1)
    }

    @Test
    func testSessionLifecycleIdleToProcessingToIdle() async throws {
        let mockAdapter = MockProviderAdapter(fixedResponse: Response(
            id: "resp-1",
            model: "mock-model",
            provider: "anthropic",
            message: .assistant("Hello, I can help with that!"),
            finishReason: .stop,
            usage: Usage(inputTokens: 10, outputTokens: 5)
        ))
        let client = try Client(providers: ["anthropic": mockAdapter], defaultProvider: "anthropic")
        let env = MockExecutionEnvironment()
        let profile = AnthropicProfile()
        let session = try Session(profile: profile, environment: env, client: client)

        // Initially idle
        let initialState = await session.getState()
        XCTAssertEqual(initialState, .idle)

        // Submit input - session should process and return to idle
        await session.submit("Say hello")

        let finalState = await session.getState()
        XCTAssertEqual(finalState, .idle)

        // Verify history contains user + assistant turns
        let history = await session.getHistory()
        XCTAssertGreaterThanOrEqual(history.count, 2)

        // First turn should be user
        if case .user(let t) = history[0] {
            XCTAssertEqual(t.content, "Say hello")
        } else {
            XCTFail("Expected first turn to be user turn")
        }

        // Second turn should be assistant
        if case .assistant(let t) = history[1] {
            XCTAssertEqual(t.content, "Hello, I can help with that!")
            XCTAssertTrue(t.toolCalls.isEmpty, "Expected no tool calls for natural completion")
        } else {
            XCTFail("Expected second turn to be assistant turn")
        }
    }

    @Test
    func testSessionAbortTransitionsToClosed() async throws {
        let mockAdapter = MockProviderAdapter(fixedResponse: Response(
            id: "resp-1",
            model: "mock-model",
            provider: "anthropic",
            message: .assistant("Done"),
            finishReason: .stop,
            usage: .zero
        ))
        let client = try Client(providers: ["anthropic": mockAdapter], defaultProvider: "anthropic")
        let env = MockExecutionEnvironment()
        let profile = AnthropicProfile()
        let session = try Session(profile: profile, environment: env, client: client)

        await session.abort()

        let state = await session.getState()
        XCTAssertEqual(state, .closed)
    }

    @Test
    func testSessionMultipleSequentialInputs() async throws {
        let mockAdapter = MockProviderAdapter(fixedResponse: Response(
            id: "resp-1",
            model: "mock-model",
            provider: "anthropic",
            message: .assistant("Response"),
            finishReason: .stop,
            usage: .zero
        ))
        let client = try Client(providers: ["anthropic": mockAdapter], defaultProvider: "anthropic")
        let env = MockExecutionEnvironment()
        let profile = AnthropicProfile()
        let session = try Session(profile: profile, environment: env, client: client)

        await session.submit("First input")
        let stateAfterFirst = await session.getState()
        XCTAssertEqual(stateAfterFirst, .idle)

        await session.submit("Second input")
        let stateAfterSecond = await session.getState()
        XCTAssertEqual(stateAfterSecond, .idle)

        let history = await session.getHistory()
        // Should have: user1, assistant1, user2, assistant2
        XCTAssertGreaterThanOrEqual(history.count, 4)
    }

    @Test
    func testSessionEventsEmitted() async throws {
        let mockAdapter = MockProviderAdapter(fixedResponse: Response(
            id: "resp-1",
            model: "mock-model",
            provider: "anthropic",
            message: .assistant("Done"),
            finishReason: .stop,
            usage: .zero
        ))
        let client = try Client(providers: ["anthropic": mockAdapter], defaultProvider: "anthropic")
        let env = MockExecutionEnvironment()
        let profile = AnthropicProfile()
        let session = try Session(profile: profile, environment: env, client: client)

        await session.submit("Test")

        let events = await session.eventEmitter.allEvents()
        let kinds = events.map { $0.kind }

        XCTAssertTrue(kinds.contains(.sessionStart), "Expected sessionStart event")
        XCTAssertTrue(kinds.contains(.userInput), "Expected userInput event")
        XCTAssertTrue(kinds.contains(.assistantTextStart), "Expected assistantTextStart event")
        XCTAssertTrue(kinds.contains(.assistantTextDelta), "Expected assistantTextDelta event")
        XCTAssertTrue(kinds.contains(.assistantTextEnd), "Expected assistantTextEnd event")
        XCTAssertTrue(kinds.contains(.sessionEnd), "Expected sessionEnd event")
    }
}

// MARK: - Mock Provider Adapter

private actor MockProviderAdapterCallStats {
    private var completeCalls: Int = 0
    private var streamCalls: Int = 0

    func recordComplete() {
        completeCalls += 1
    }

    func recordStream() {
        streamCalls += 1
    }

    func snapshot() -> (complete: Int, stream: Int) {
        (completeCalls, streamCalls)
    }
}

private final class MockProviderAdapter: ProviderAdapter, @unchecked Sendable {
    let name = "mock"
    let fixedResponse: Response
    let callStats = MockProviderAdapterCallStats()

    init(fixedResponse: Response) {
        self.fixedResponse = fixedResponse
    }

    func complete(request: Request) async throws -> Response {
        await callStats.recordComplete()
        return fixedResponse
    }

    func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        await callStats.recordStream()
        return AsyncThrowingStream<StreamEvent, Error> { continuation in
            continuation.finish()
        }
    }
}

// MARK: - Mock Execution Environment

private final class MockExecutionEnvironment: ExecutionEnvironment, @unchecked Sendable {
    func readFile(path: String, offset: Int?, limit: Int?) async throws -> String { "" }
    func writeFile(path: String, content: String) async throws {}
    func fileExists(path: String) async -> Bool { false }
    func listDirectory(path: String, depth: Int) async throws -> [DirEntry] { [] }
    func execCommand(command: String, timeoutMs: Int, workingDir: String?, envVars: [String: String]?) async throws -> ExecResult {
        ExecResult(stdout: "", stderr: "", exitCode: 0, timedOut: false, durationMs: 0)
    }
    func grep(pattern: String, path: String, options: GrepOptions) async throws -> String { "" }
    func glob(pattern: String, path: String) async throws -> [String] { [] }
    func initialize() async throws {}
    func cleanup() async throws {}
    func workingDirectory() -> String { "/tmp/test" }
    func platform() -> String { "darwin" }
    func osVersion() -> String { "Darwin 24.0.0" }
}
