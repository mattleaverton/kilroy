import Foundation
import Testing
import OmniAICore
@testable import OmniAIAgent

@Suite
struct SessionPersistenceTests {
    @Test
    func fileSessionStorageSaveLoadDeleteRoundTrip() async throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("omnikit-session-storage-\(UUID().uuidString)", isDirectory: true)
        let storage = FileSessionStorageBackend(rootDirectory: root)

        let snapshot = SessionSnapshot(
            sessionID: "session-a",
            providerID: "openai",
            model: "gpt-test",
            workingDirectory: "/tmp",
            state: .idle,
            history: [
                .user(UserTurn(content: "hello")),
                .assistant(PersistedAssistantTurn(
                    content: "world",
                    toolCalls: [],
                    reasoning: nil,
                    usage: PersistedUsage(
                        inputTokens: 1,
                        outputTokens: 1,
                        reasoningTokens: nil,
                        cacheReadTokens: nil,
                        cacheWriteTokens: nil,
                        raw: nil
                    ),
                    responseId: "resp-1",
                    timestamp: Date()
                )),
            ],
            steeringQueue: [],
            followupQueue: [],
            config: SessionConfig(),
            abortSignaled: false
        )

        try await storage.save(snapshot)
        let loaded = try await storage.load(sessionID: "session-a")
        #expect(loaded != nil)
        #expect(loaded?.sessionID == "session-a")
        #expect(loaded?.history.count == 2)

        try await storage.delete(sessionID: "session-a")
        let afterDelete = try await storage.load(sessionID: "session-a")
        #expect(afterDelete == nil)
    }

    @Test
    func sessionCanRestoreHistoryAndContinue() async throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("omnikit-session-restore-\(UUID().uuidString)", isDirectory: true)
        let storage = FileSessionStorageBackend(rootDirectory: root)

        let env1 = LocalExecutionEnvironment(workingDir: root.path)
        try await env1.initialize()
        let client1 = try Client(
            providers: [
                "openai": MockProviderAdapter(
                    responses: [makeResponse(text: "first reply")]
                ),
            ],
            defaultProvider: "openai"
        )
        let profile = TestProfile()
        let sessionID = "restore-session-1"

        let session1 = try Session(
            profile: profile,
            environment: env1,
            client: client1,
            sessionID: sessionID,
            storageBackend: storage
        )
        await session1.submit("hello")
        let originalHistory = await session1.getHistory()
        #expect(originalHistory.count == 2)

        let env2 = LocalExecutionEnvironment(workingDir: root.path)
        try await env2.initialize()
        let client2 = try Client(
            providers: [
                "openai": MockProviderAdapter(
                    responses: [makeResponse(text: "second reply")]
                ),
            ],
            defaultProvider: "openai"
        )
        let session2 = try Session(
            profile: profile,
            environment: env2,
            client: client2,
            sessionID: sessionID,
            storageBackend: storage
        )
        let restored = try await session2.restoreFromStorage()
        #expect(restored)

        let restoredHistory = await session2.getHistory()
        #expect(restoredHistory.count == originalHistory.count)

        await session2.submit("next")
        let resumedHistory = await session2.getHistory()
        #expect(resumedHistory.count >= 4)
        #expect(userText(at: 0, in: resumedHistory) == "hello")
        #expect(userText(at: 2, in: resumedHistory) == "next")
    }

    @Test
    func sessionRecoversPendingToolCallsBeforeNextInput() async throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("omnikit-session-pending-tools-\(UUID().uuidString)", isDirectory: true)
        let storage = FileSessionStorageBackend(rootDirectory: root)

        let toolRegistry = ToolRegistry()
        toolRegistry.register(RegisteredTool(
            definition: AgentToolDefinition(
                name: "echo_tool",
                description: "Echo",
                parameters: [
                    "type": "object",
                    "properties": [:] as [String: Any],
                    "additionalProperties": true,
                ]
            ),
            executor: { _, _ in "tool-ok" }
        ))
        let profile = TestProfile(toolRegistry: toolRegistry)
        let sessionID = "restore-session-pending-tools"

        let pendingCall = PersistedToolCall(
            id: "call-1",
            name: "echo_tool",
            arguments: [:],
            rawArguments: nil,
            thoughtSignature: nil,
            providerItemId: nil
        )
        let snapshot = SessionSnapshot(
            sessionID: sessionID,
            providerID: "openai",
            model: "gpt-test",
            workingDirectory: root.path,
            state: .processing,
            history: [
                .user(UserTurn(content: "run tool")),
                .assistant(PersistedAssistantTurn(
                    content: "",
                    toolCalls: [pendingCall],
                    reasoning: nil,
                    usage: PersistedUsage(
                        inputTokens: 1,
                        outputTokens: 1,
                        reasoningTokens: nil,
                        cacheReadTokens: nil,
                        cacheWriteTokens: nil,
                        raw: nil
                    ),
                    responseId: "resp-tool",
                    timestamp: Date()
                )),
            ],
            steeringQueue: [],
            followupQueue: [],
            config: SessionConfig(),
            abortSignaled: false
        )
        try await storage.save(snapshot)

        let env = LocalExecutionEnvironment(workingDir: root.path)
        try await env.initialize()
        let client = try Client(
            providers: [
                "openai": MockProviderAdapter(
                    responses: [makeResponse(text: "done after tool recovery")]
                ),
            ],
            defaultProvider: "openai"
        )
        let session = try Session(
            profile: profile,
            environment: env,
            client: client,
            sessionID: sessionID,
            storageBackend: storage
        )

        await session.submit("continue")
        let history = await session.getHistory()
        #expect(history.count >= 5)

        guard case .toolResults(let toolTurn) = history[2] else {
            Issue.record("Expected recovered tool results at history[2]")
            return
        }
        #expect(toolTurn.results.count == 1)
        #expect(toolTurn.results[0].toolCallId == "call-1")
        #expect(toolTurn.results[0].isError == false)
        #expect(userText(at: 3, in: history) == "continue")
    }

    @Test
    func sessionResumeSkipsDuplicatePromptForInProgressSnapshot() async throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("omnikit-session-resume-duplicate-\(UUID().uuidString)", isDirectory: true)
        let storage = FileSessionStorageBackend(rootDirectory: root)

        let toolRegistry = ToolRegistry()
        toolRegistry.register(RegisteredTool(
            definition: AgentToolDefinition(
                name: "echo_tool",
                description: "Echo",
                parameters: [
                    "type": "object",
                    "properties": [:] as [String: Any],
                    "additionalProperties": true,
                ]
            ),
            executor: { _, _ in "tool-ok" }
        ))
        let profile = TestProfile(toolRegistry: toolRegistry)
        let sessionID = "resume-duplicate-prompt"
        let repeatedPrompt = "resume original prompt"

        let pendingCall = PersistedToolCall(
            id: "call-dup-1",
            name: "echo_tool",
            arguments: [:],
            rawArguments: nil,
            thoughtSignature: nil,
            providerItemId: nil
        )
        let snapshot = SessionSnapshot(
            sessionID: sessionID,
            providerID: "openai",
            model: "gpt-test",
            workingDirectory: root.path,
            state: .processing,
            history: [
                .user(UserTurn(content: repeatedPrompt)),
                .assistant(PersistedAssistantTurn(
                    content: "",
                    toolCalls: [pendingCall],
                    reasoning: nil,
                    usage: PersistedUsage(
                        inputTokens: 1,
                        outputTokens: 1,
                        reasoningTokens: nil,
                        cacheReadTokens: nil,
                        cacheWriteTokens: nil,
                        raw: nil
                    ),
                    responseId: "resp-dup",
                    timestamp: Date()
                )),
            ],
            steeringQueue: [],
            followupQueue: [],
            config: SessionConfig(),
            abortSignaled: false
        )
        try await storage.save(snapshot)

        let env = LocalExecutionEnvironment(workingDir: root.path)
        try await env.initialize()
        let client = try Client(
            providers: [
                "openai": MockProviderAdapter(
                    responses: [makeResponse(text: "finished after resume")]
                ),
            ],
            defaultProvider: "openai"
        )
        let session = try Session(
            profile: profile,
            environment: env,
            client: client,
            sessionID: sessionID,
            storageBackend: storage
        )

        await session.submit(repeatedPrompt)
        let history = await session.getHistory()
        let repeatedCount = history.reduce(0) { partial, turn in
            guard case .user(let user) = turn else { return partial }
            return partial + (user.content == repeatedPrompt ? 1 : 0)
        }
        #expect(repeatedCount == 1)
    }
}

private actor MockProviderAdapter: ProviderAdapter {
    nonisolated let name = "openai"
    private var responses: [Response]

    init(responses: [Response]) {
        self.responses = responses
    }

    func complete(request: Request) async throws -> Response {
        if responses.isEmpty {
            return makeResponse(text: "ok")
        }
        return responses.removeFirst()
    }

    func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        AsyncThrowingStream { continuation in
            continuation.finish()
        }
    }
}

private struct TestProfile: ProviderProfile {
    let id: String = "openai"
    let model: String = "gpt-test"
    let toolRegistry: ToolRegistry

    init(toolRegistry: ToolRegistry = ToolRegistry()) {
        self.toolRegistry = toolRegistry
    }

    func buildSystemPrompt(environment: ExecutionEnvironment, projectDocs: String?, userInstructions: String?, gitContext: GitContext?) -> String {
        "Test system prompt"
    }

    func providerOptions() -> [String: JSONValue]? {
        nil
    }

    var supportsReasoning: Bool { false }
    var supportsStreaming: Bool { false }
    var supportsParallelToolCalls: Bool { false }
    var contextWindowSize: Int { 200_000 }
}

private func makeResponse(text: String, toolCalls: [ToolCall] = []) -> Response {
    var content: [ContentPart] = []
    if !text.isEmpty {
        content.append(.text(text))
    }
    for call in toolCalls {
        content.append(.toolCall(call))
    }
    if content.isEmpty {
        content = [.text("")]
    }

    return Response(
        id: UUID().uuidString,
        model: "gpt-test",
        provider: "openai",
        message: Message(role: .assistant, content: content),
        finishReason: .stop,
        usage: .zero
    )
}

private func userText(at index: Int, in history: [Turn]) -> String? {
    guard index >= 0 && index < history.count else { return nil }
    guard case .user(let userTurn) = history[index] else { return nil }
    return userTurn.content
}
