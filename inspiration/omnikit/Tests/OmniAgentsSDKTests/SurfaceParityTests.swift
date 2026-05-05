import Foundation
import Testing
import OmniAgentsSDK
import OmniAICore

private actor _ComputerState {
    var createCount = 0
    var disposeCount = 0
    func created() { createCount += 1 }
    func disposed() { disposeCount += 1 }
}

private struct DummyComputer: AsyncComputer {
    var environment: Environment { .browser }
    var dimensions: (Int, Int) { (1, 1) }
    func screenshot() async throws -> String { "img" }
    func click(x: Int, y: Int, button: Button) async throws {}
    func doubleClick(x: Int, y: Int) async throws {}
    func scroll(x: Int, y: Int, scrollX: Int, scrollY: Int) async throws {}
    func type(_ text: String) async throws {}
    func wait() async throws {}
    func move(x: Int, y: Int) async throws {}
    func keypress(_ keys: [String]) async throws {}
    func drag(_ path: [(Int, Int)]) async throws {}
}

struct SurfaceParityTests {
    @Test
    func config_and_session_helpers_exist_and_work() throws {
        setDefaultOpenAIKey("sk-test", useForTracing: true)
        setDefaultOpenAIAPI(.responses)
        setDefaultOpenAIResponsesTransport(.http)
        setTracingExportAPIKey("trace-key")
        setTracingDisabled(false)

        let snapshot = getGlobalConfig()
        #expect(snapshot.defaultOpenAIKey == "sk-test")
        #expect(snapshot.defaultOpenAIAPI == .responses)
        #expect(snapshot.defaultOpenAIResponsesTransport == .http)
        #expect(snapshot.tracingExportAPIKey == "trace-key")

        let session = OpenAIResponsesCompactionSession(sessionID: "s")
        #expect(isOpenAIResponsesCompactionAwareSession(session))
    }

    @Test
    func compaction_session_keeps_last_item_when_compacting_input() async throws {
        let session = OpenAIResponsesCompactionSession(sessionID: "s")
        try await session.addItems([
            ["id": .string("first"), "type": .string("message")],
            ["id": .string("last"), "type": .string("message")],
        ])

        try await session.runCompaction(args: OpenAIResponsesCompactionArgs(compactionMode: .input))

        let items = try await session.getItems()
        #expect(items.count == 1)
        #expect(items.first?["id"]?.stringValue == "last")
    }

    @Test
    func websocket_helper_exists() {
        let session = responsesWebSocketSession()
        #expect(session.provider.useResponsesWebSocket)
    }

    @Test
    func agent_tool_invocation_is_exposed_for_tool_context_results() {
        let ctx = ToolContext<Void>(
            context: (),
            toolName: "delegate",
            toolCallID: "call_123",
            toolArguments: "{\"x\":1}",
            agent: nil,
            runConfig: nil
        )
        let result = RunResult<Void>(
            input: .string("hello"),
            newItems: [],
            rawResponses: [],
            finalOutput: "ok",
            inputGuardrailResults: [],
            outputGuardrailResults: [],
            toolInputGuardrailResults: [],
            toolOutputGuardrailResults: [],
            contextWrapper: ctx,
            lastAgent: AnyAgent(erasing: "agent", name: "agent")
        )

        #expect(result.agent_tool_invocation == AgentToolInvocation(toolName: "delegate", toolCallID: "call_123", toolArguments: "{\"x\":1}"))
    }

    @Test
    func resolve_and_dispose_computer_helpers_manage_provider_lifecycle() async throws {
        let state = _ComputerState()
        let provider = ComputerProvider(
            create: {
                await state.created()
                return DummyComputer()
            },
            dispose: { _ in
                await state.disposed()
            }
        )
        let tool = ComputerTool(computer: .provider(provider))
        let context = RunContextWrapper<Any>(context: ())

        _ = try await resolveComputer(tool: tool, runContext: context)
        _ = try await resolveComputer(tool: tool, runContext: context)

        #expect(await state.createCount == 1)
        await disposeResolvedComputers(runContext: context)
        #expect(await state.disposeCount == 1)
    }

    @Test
    func tracing_helper_exports_create_expected_span_kinds() {
        #expect(!genTraceID().isEmpty)
        #expect(!genSpanID().isEmpty)

        #expect(agentSpan(name: "a").data?.kind == "agent")
        #expect(customSpan(name: "c").data?.kind == "custom")
        #expect(functionSpan(name: "f").data?.kind == "function")
        #expect(generationSpan().data?.kind == "generation")
        #expect(guardrailSpan(name: "g").data?.kind == "guardrail")
        #expect(handoffSpan().data?.kind == "handoff")
        #expect(mcpToolsSpan().data?.kind == "mcp_list_tools")
        #expect(speechSpan().data?.kind == "speech")
        #expect(speechGroupSpan().data?.kind == "speech_group")
        #expect(transcriptionSpan().data?.kind == "transcription")
    }

    @Test
    func tool_output_dict_aliases_are_usable_json_dictionaries() {
        let text: ToolOutputTextDict = ["type": .string("text"), "text": .string("hello")]
        let image: ToolOutputImageDict = ["type": .string("image"), "image_url": .string("https://example.com/image.png")]
        let file: ToolOutputFileContentDict = ["type": .string("file"), "file_id": .string("file_123")]
        #expect(text["type"] == .string("text"))
        #expect(image["type"] == .string("image"))
        #expect(file["type"] == .string("file"))
    }
}
