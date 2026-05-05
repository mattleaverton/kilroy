import Foundation
import Testing
import OmniAICore
@testable import OmniAIAgent

private actor CodingLoopTestAdapterState {
    var responses: [Response]
    var requests: [Request] = []
    var index = 0

    init(responses: [Response]) {
        self.responses = responses
    }

    func nextResponse(for request: Request) -> Response {
        requests.append(request)
        let current = index
        index += 1
        if current < responses.count {
            return responses[current]
        }
        guard let lastResponse = responses.last else {
            preconditionFailure("CodingLoopTestAdapterState requires at least one response")
        }
        return lastResponse
    }
}

private final class CodingLoopTestAdapter: ProviderAdapter, @unchecked Sendable {
    let name: String
    let state: CodingLoopTestAdapterState

    init(name: String = "test", responses: [Response]) {
        self.name = name
        self.state = CodingLoopTestAdapterState(responses: responses)
    }

    func complete(request: Request) async throws -> Response {
        await state.nextResponse(for: request)
    }

    func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        let response = await state.nextResponse(for: request)
        return AsyncThrowingStream { continuation in
            continuation.yield(StreamEvent(type: .standard(.streamStart)))
            continuation.yield(StreamEvent(type: .standard(.finish), finishReason: response.finishReason, usage: response.usage, response: response))
            continuation.finish()
        }
    }
}

private struct CodingLoopTestProfile: ProviderProfile {
    let id: String
    let model: String
    let toolRegistry: ToolRegistry
    let supportsReasoning = false
    let supportsStreaming = false
    let supportsParallelToolCalls = true
    let contextWindowSize = 16_000

    init(id: String = "test", model: String = "test-model", toolRegistry: ToolRegistry) {
        self.id = id
        self.model = model
        self.toolRegistry = toolRegistry
    }

    func buildSystemPrompt(environment: ExecutionEnvironment, projectDocs: String?, userInstructions: String?, gitContext: GitContext?) -> String {
        "You are a test profile."
    }

    func providerOptions() -> [String: JSONValue]? {
        nil
    }
}

private final class ToolOutputExecutionEnvironment: ExecutionEnvironment, @unchecked Sendable {
    let output: String
    let workingDir: String

    init(output: String, workingDir: String = FileManager.default.temporaryDirectory.path) {
        self.output = output
        self.workingDir = workingDir
    }

    func readFile(path: String, offset: Int?, limit: Int?) async throws -> String { "" }
    func writeFile(path: String, content: String) async throws {}
    func fileExists(path: String) async -> Bool { false }
    func listDirectory(path: String, depth: Int) async throws -> [DirEntry] { [] }
    func execCommand(command: String, timeoutMs: Int, workingDir: String?, envVars: [String: String]?) async throws -> ExecResult {
        ExecResult(stdout: output, stderr: "", exitCode: 0, timedOut: false, durationMs: 5)
    }
    func grep(pattern: String, path: String, options: GrepOptions) async throws -> String { "" }
    func glob(pattern: String, path: String) async throws -> [String] { [] }
    func initialize() async throws {}
    func cleanup() async throws {}
    func workingDirectory() -> String { workingDir }
    func platform() -> String { "darwin" }
    func osVersion() -> String { "Darwin test" }
}

private func codingLoopResponse(
    provider: String,
    model: String,
    text: String = "",
    toolCalls: [ToolCall] = [],
    finishReason: String = "stop"
) -> Response {
    var parts: [ContentPart] = []
    if !text.isEmpty {
        parts.append(.text(text))
    }
    for call in toolCalls {
        parts.append(.toolCall(call))
    }
    return Response(
        id: "resp_\(UUID().uuidString)",
        model: model,
        provider: provider,
        message: Message(role: .assistant, content: parts),
        finishReason: FinishReason(kind: FinishReason.Kind(rawValue: finishReason) ?? .other, raw: finishReason),
        usage: Usage(inputTokens: 1, outputTokens: 1),
        raw: nil,
        warnings: [],
        rateLimit: nil
    )
}

private func extractSessionID(from text: String) -> Int? {
    guard let match = text.firstMatch(of: /Session ID: ([0-9]+)/) else {
        return nil
    }
    return Int(match.1)
}

private func extractTaskID(from text: String) -> String? {
    guard let match = text.firstMatch(of: /ID: ([A-Za-z0-9\-]+)/) else {
        return nil
    }
    return String(match.1)
}

private func makeTempDirectory(prefix: String) throws -> URL {
    let url = FileManager.default.temporaryDirectory.appendingPathComponent("\(prefix)-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
    return url
}

private func runShell(_ command: String, in workingDirectory: String) async throws -> ExecResult {
    let env = LocalExecutionEnvironment(workingDir: workingDirectory)
    return try await env.execCommand(command: command, timeoutMs: 30_000, workingDir: workingDirectory, envVars: nil)
}

@Suite
struct CodingLoopParityTests {
    @Test
    func structuredToolEventsCarryTypedPayloadsAndOutputDeltas() async throws {
        let registry = ToolRegistry()
        registry.register(shellTool())
        let profile = CodingLoopTestProfile(toolRegistry: registry)
        let env = ToolOutputExecutionEnvironment(output: "streamed shell output")
        let adapter = CodingLoopTestAdapter(
            responses: [
                codingLoopResponse(
                    provider: "test",
                    model: "test-model",
                    toolCalls: [ToolCall(id: "call-1", name: "shell", arguments: ["command": .string("echo hi")], rawArguments: nil)],
                    finishReason: "tool_calls"
                ),
                codingLoopResponse(provider: "test", model: "test-model", text: "done")
            ]
        )
        let client = try Client(providers: ["test": adapter], defaultProvider: "test")
        let session = try Session(profile: profile, environment: env, client: client)

        await session.submit("run the shell command")

        let events = await session.eventEmitter.allEvents()
        let assistantStart = try #require(events.first(where: { $0.kind == .assistantTextStart }))
        let outputDelta = try #require(events.first(where: { $0.kind == .toolCallOutputDelta }))
        let toolEnd = try #require(events.first(where: { $0.kind == .toolCallEnd }))

        #expect(assistantStart.intValue(for: "tool_call_count") == 1)
        #expect(outputDelta.stringValue(for: "delta") == "streamed shell output")
        #expect(toolEnd.stringValue(for: "tool") == "shell")
        #expect(toolEnd.boolValue(for: "truncated") == false)
    }

    @Test
    func execCommandTTYFlagChangesTerminalDetection() async throws {
        let tempDir = try makeTempDirectory(prefix: "omnikit-codex-tty")
        let env = LocalExecutionEnvironment(workingDir: tempDir.path)
        try await env.initialize()
        let tool = execCommandTool()

        let noTTY = try await tool.executor(
            [
                "cmd": "if [ -t 0 ]; then echo tty; else echo notty; fi",
                "tty": false,
                "yield_time_ms": 200,
            ],
            env
        )
        let yesTTY = try await tool.executor(
            [
                "cmd": "if [ -t 0 ]; then echo tty; else echo notty; fi",
                "tty": true,
                "yield_time_ms": 200,
            ],
            env
        )

        #expect(noTTY.contains("Output:\nnotty"))
        #expect(yesTTY.contains("Output:\ntty"))
    }

    @Test
    func writeStdinSupportsInteractiveTTYSession() async throws {
        let tempDir = try makeTempDirectory(prefix: "omnikit-codex-stdin")
        let env = LocalExecutionEnvironment(workingDir: tempDir.path)
        try await env.initialize()
        let execTool = execCommandTool()
        let stdinTool = writeStdinTool()

        let started = try await execTool.executor(
            [
                "cmd": "read line; printf 'got:%s\\n' \"$line\"",
                "tty": true,
                "yield_time_ms": 100,
            ],
            env
        )

        let sessionID = try #require(extractSessionID(from: started))
        let completed = try await stdinTool.executor(
            [
                "session_id": sessionID,
                "chars": "hello\n",
                "yield_time_ms": 1_000,
            ],
            env
        )

        #expect(completed.contains("got:hello"))
        #expect(completed.contains("Status: completed"))
    }

    @Test
    func claudeTaskWorktreeIsolationCreatesSeparateCheckoutAndCleansUp() async throws {
        let repoRoot = try makeTempDirectory(prefix: "omnikit-worktree")
        let readme = repoRoot.appendingPathComponent("README.md")
        try "seed".write(to: readme, atomically: true, encoding: .utf8)
        _ = try await runShell("git init -q", in: repoRoot.path)
        _ = try await runShell("git add README.md", in: repoRoot.path)
        _ = try await runShell("git -c user.name='Test User' -c user.email='test@example.com' commit -qm init", in: repoRoot.path)

        let env = LocalExecutionEnvironment(workingDir: repoRoot.path)
        try await env.initialize()
        let adapter = CodingLoopTestAdapter(
            name: "anthropic",
            responses: [codingLoopResponse(provider: "anthropic", model: "claude-haiku-4-5", text: "done")]
        )
        let client = try Client(providers: ["anthropic": adapter], defaultProvider: "anthropic")
        let profile = AnthropicProfile(enableInteractiveTools: false)
        let parentSession = try Session(profile: profile, environment: env, client: client)
        let taskTool = claudeTaskTool(parentSession: parentSession)

        let started = try await taskTool.executor(
            [
                "description": "worktree task",
                "prompt": "Say done",
                "subagent_type": "general-purpose",
                "run_in_background": true,
                "isolation": "worktree",
            ],
            env
        )

        let taskID = try #require(extractTaskID(from: started))
        let handle = try #require(await parentSession.getSubagent(taskID))
        let worktreePath = await handle.session.workingDirectory()
        let isolatedFile = URL(fileURLWithPath: worktreePath, isDirectory: true).appendingPathComponent("isolated.txt")
        try "isolated".write(to: isolatedFile, atomically: true, encoding: .utf8)

        #expect(worktreePath.contains(".ai/subagents/worktrees"))
        #expect(FileManager.default.fileExists(atPath: isolatedFile.path))
        #expect(!FileManager.default.fileExists(atPath: repoRoot.appendingPathComponent("isolated.txt").path))

        await handle.session.close()
        await parentSession.removeSubagent(taskID)

        #expect(!FileManager.default.fileExists(atPath: worktreePath))
    }
}
