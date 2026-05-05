import Testing
import Foundation
import OmniAICore
@testable import OmniAIAgent

@Suite
final class ParallelToolExecutionTests {
    @Test
    func testParallelToolCallsEnabledRunsConcurrently() async throws {
        let firstResponse = Response(
            id: "resp-1",
            model: "mock-model",
            provider: "mock",
            message: Message(
                role: .assistant,
                content: [
                    .toolCall(ToolCall(id: "call-a", name: "slow_a", arguments: [:])),
                    .toolCall(ToolCall(id: "call-b", name: "slow_b", arguments: [:])),
                ]
            ),
            finishReason: .toolCalls,
            usage: .zero
        )
        let secondResponse = Response(
            id: "resp-2",
            model: "mock-model",
            provider: "mock",
            message: .assistant("done"),
            finishReason: .stop,
            usage: .zero
        )

        let adapter = ScriptedProviderAdapter(responses: [firstResponse, secondResponse])
        let client = try Client(providers: ["mock": adapter], defaultProvider: "mock")
        let profile = ParallelToolProfile(toolDelayMs: 400)
        let session = try Session(
            profile: profile,
            environment: ParallelTestExecutionEnvironment(),
            client: client,
            config: SessionConfig(
                interactiveMode: false,
                parallelToolCalls: true
            )
        )

        let start = ContinuousClock.now
        await session.submit("run tools")
        let elapsed = durationSeconds(ContinuousClock.now - start)
        let history = await session.getHistory()

        let toolResultTurns = history.filter {
            if case .toolResults = $0 { return true }
            return false
        }
        XCTAssertEqual(toolResultTurns.count, 1, "Expected one tool-results turn")
        if case .toolResults(let turn) = toolResultTurns[0] {
            XCTAssertEqual(turn.results.count, 2, "Expected both tool calls to produce results")
        }

        XCTAssertLessThan(
            elapsed,
            0.75,
            "Parallel tool execution should finish faster than sequential baseline (elapsed=\(elapsed)s)"
        )

        await session.close()
    }

    private func durationSeconds(_ duration: Duration) -> Double {
        Double(duration.components.seconds) + Double(duration.components.attoseconds) / 1e18
    }
}

private final class ScriptedProviderAdapter: ProviderAdapter, @unchecked Sendable {
    let name = "mock"
    private let queue: ScriptedResponseQueue

    init(responses: [Response]) {
        self.queue = ScriptedResponseQueue(responses: responses)
    }

    func complete(request: Request) async throws -> Response {
        try await queue.next()
    }

    func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        AsyncThrowingStream { continuation in
            continuation.finish()
        }
    }
}

private actor ScriptedResponseQueue {
    private var index = 0
    private let responses: [Response]

    init(responses: [Response]) {
        self.responses = responses
    }

    func next() throws -> Response {
        guard !responses.isEmpty else {
            throw SDKError(message: "No scripted responses configured")
        }
        let response = responses[min(index, responses.count - 1)]
        index += 1
        return response
    }
}

private final class ParallelToolProfile: ProviderProfile, @unchecked Sendable {
    let id = "mock"
    let model = "mock-model"
    let toolRegistry = ToolRegistry()
    let supportsReasoning = true
    let supportsStreaming = false
    let supportsParallelToolCalls = true
    let contextWindowSize = 128_000

    init(toolDelayMs: Int) {
        let delayNanos = UInt64(max(0, toolDelayMs)) * 1_000_000
        let schema: [String: Any] = [
            "type": "object",
            "properties": [:] as [String: Any],
            "additionalProperties": false,
        ]

        toolRegistry.register(RegisteredTool(
            definition: AgentToolDefinition(
                name: "slow_a",
                description: "slow tool a",
                parameters: schema
            ),
            executor: { _, _ in
                try await Task.sleep(nanoseconds: delayNanos)
                return "A"
            }
        ))
        toolRegistry.register(RegisteredTool(
            definition: AgentToolDefinition(
                name: "slow_b",
                description: "slow tool b",
                parameters: schema
            ),
            executor: { _, _ in
                try await Task.sleep(nanoseconds: delayNanos)
                return "B"
            }
        ))
    }

    func buildSystemPrompt(
        environment: ExecutionEnvironment,
        projectDocs: String?,
        userInstructions: String?,
        gitContext: GitContext?
    ) -> String {
        "You are a test assistant."
    }

    func providerOptions() -> [String: JSONValue]? {
        nil
    }
}

private final class ParallelTestExecutionEnvironment: ExecutionEnvironment, @unchecked Sendable {
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
    func workingDirectory() -> String { "/tmp/parallel-tool-test" }
    func platform() -> String { "darwin" }
    func osVersion() -> String { "Darwin 24.0.0" }
}
