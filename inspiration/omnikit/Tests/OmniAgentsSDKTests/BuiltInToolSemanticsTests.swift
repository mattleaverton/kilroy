import Foundation
import Testing
import OmniAgentsSDK
import protocol OmniAICore.ProviderAdapter
import class OmniAICore.Client
import struct OmniAICore.Request
import struct OmniAICore.Response
import struct OmniAICore.Message
import struct OmniAICore.FinishReason
import struct OmniAICore.StreamEvent
import enum OmniAICore.StreamEventTypeTag
import enum OmniAICore.StreamEventType
import enum OmniAICore.JSONValue
import enum OmniAICore.OpenAIProviderOptionKeys

private actor _SequencedModelState {
    var turn: Int = 0
    var inputs: [StringOrInputList] = []
    func record(_ input: StringOrInputList) -> Int {
        inputs.append(input)
        defer { turn += 1 }
        return turn
    }
    func allInputs() -> [StringOrInputList] { inputs }
}

private final class SequencedModel: Model, @unchecked Sendable {
    let state = _SequencedModelState()
    let handler: @Sendable (Int, StringOrInputList) async throws -> ModelResponse

    init(handler: @escaping @Sendable (Int, StringOrInputList) async throws -> ModelResponse) {
        self.handler = handler
    }

    func close() async {}

    func getResponse(
        systemInstructions: String?,
        input: StringOrInputList,
        modelSettings: ModelSettings,
        tools: [Tool],
        outputSchema: (any AgentOutputSchemaBase)?,
        handoffs: [Any],
        tracing: ModelTracing,
        previousResponseID: String?,
        conversationID: String?,
        prompt: Prompt?
    ) async throws -> ModelResponse {
        let turn = await state.record(input)
        return try await handler(turn, input)
    }

    func streamResponse(
        systemInstructions: String?,
        input: StringOrInputList,
        modelSettings: ModelSettings,
        tools: [Tool],
        outputSchema: (any AgentOutputSchemaBase)?,
        handoffs: [Any],
        tracing: ModelTracing,
        previousResponseID: String?,
        conversationID: String?,
        prompt: Prompt?
    ) async throws -> AsyncThrowingStream<TResponseStreamEvent, Error> {
        let response = try await getResponse(
            systemInstructions: systemInstructions,
            input: input,
            modelSettings: modelSettings,
            tools: tools,
            outputSchema: outputSchema,
            handoffs: handoffs,
            tracing: tracing,
            previousResponseID: previousResponseID,
            conversationID: conversationID,
            prompt: prompt
        )
        return AsyncThrowingStream { continuation in
            continuation.yield(["type": .string("response.complete"), "response_id": .string(response.responseID ?? "resp")])
            continuation.finish()
        }
    }
}

private actor _RecordingComputerState {
    var calls: [String] = []
    func append(_ call: String) { calls.append(call) }
    func all() -> [String] { calls }
}

private final class RecordingComputer: AsyncComputer, @unchecked Sendable {
    let state = _RecordingComputerState()
    var environment: Environment { .browser }
    var dimensions: (Int, Int) { (1280, 720) }
    func screenshot() async throws -> String { await state.append("screenshot"); return "screen64" }
    func click(x: Int, y: Int, button: Button) async throws { await state.append("click:\(x):\(y):\(button.rawValue)") }
    func doubleClick(x: Int, y: Int) async throws { await state.append("double_click:\(x):\(y)") }
    func scroll(x: Int, y: Int, scrollX: Int, scrollY: Int) async throws { await state.append("scroll:\(x):\(y):\(scrollX):\(scrollY)") }
    func type(_ text: String) async throws { await state.append("type:\(text)") }
    func wait() async throws { await state.append("wait") }
    func move(x: Int, y: Int) async throws { await state.append("move:\(x):\(y)") }
    func keypress(_ keys: [String]) async throws { await state.append("keypress:\(keys.joined(separator: ","))") }
    func drag(_ path: [(Int, Int)]) async throws { await state.append("drag:\(path.count)") }
}

private actor _RecordingApplyPatchEditorState {
    var operations: [ApplyPatchOperation] = []
    func append(_ op: ApplyPatchOperation) { operations.append(op) }
    func all() -> [ApplyPatchOperation] { operations }
}

private final class RecordingApplyPatchEditor: ApplyPatchEditor, @unchecked Sendable {
    let state = _RecordingApplyPatchEditorState()
    func createFile(_ operation: ApplyPatchOperation) async throws -> ApplyPatchResult? { await state.append(operation); return .init(status: .completed, output: "created") }
    func updateFile(_ operation: ApplyPatchOperation) async throws -> ApplyPatchResult? { await state.append(operation); return .init(status: .completed, output: "updated") }
    func deleteFile(_ operation: ApplyPatchOperation) async throws -> ApplyPatchResult? { await state.append(operation); return .init(status: .completed, output: "deleted") }
}

private actor _RecordingShellExecutorState {
    var requests: [ShellCommandRequest] = []
    func append(_ req: ShellCommandRequest) { requests.append(req) }
    func all() -> [ShellCommandRequest] { requests }
}

private final class RecordingShellExecutor: @unchecked Sendable {
    let state = _RecordingShellExecutorState()
    func execute(_ request: ShellCommandRequest) async throws -> ShellResult {
        await state.append(request)
        return ShellResult(output: [ShellCommandOutput(stdout: "ok", stderr: "", outcome: .init(type: "exit", exitCode: 0), command: request.data.action.commands.joined(separator: " "))])
    }
}

private final class RecordingLocalShellExecutor: @unchecked Sendable {
    let state = _RecordingShellExecutorState()
    func execute(_ request: LocalShellCommandRequest) async throws -> ShellResult {
        await state.append(ShellCommandRequest(contextWrapper: request.contextWrapper, data: request.data))
        return ShellResult(output: [ShellCommandOutput(stdout: "local-ok", stderr: "", outcome: .init(type: "exit", exitCode: 0), command: request.data.action.commands.joined(separator: " "))])
    }
}

private final class HostedToolAdapter: ProviderAdapter, @unchecked Sendable {
    let name = "openai"
    var capturedRequest: Request?
    let response: Response

    init(response: Response) {
        self.response = response
    }

    func complete(request: Request) async throws -> Response {
        capturedRequest = request
        return response
    }

    func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        let response = try await complete(request: request)
        return AsyncThrowingStream { continuation in
            continuation.yield(StreamEvent(type: .standard(.finish), finishReason: response.finishReason, usage: response.usage, response: response))
            continuation.finish()
        }
    }
}

struct BuiltInToolSemanticsTests {
    @Test
    func openai_request_shapes_hosted_builtins() async throws {
        let computer = RecordingComputer()
        let shellExec = RecordingShellExecutor()
        let rawResponse: JSONValue = .object([
            "output": .array([
                .object([
                    "type": .string("message"),
                    "role": .string("assistant"),
                    "content": .array([
                        .object([
                            "type": .string("output_text"),
                            "text": .string("ok"),
                        ])
                    ]),
                ])
            ])
        ])
        let adapter = HostedToolAdapter(response: Response(
            id: "r",
            model: "m",
            provider: "openai",
            message: .assistant("ok"),
            finishReason: .stop,
            usage: .init(inputTokens: 1, outputTokens: 1),
            raw: rawResponse
        ))
        let client = try Client(providers: ["openai": adapter], defaultProvider: "openai")
        let model = OpenAIResponsesModel(modelName: "gpt-5.3-codex", client: client)

        _ = try await model.getResponse(
            systemInstructions: "hi",
            input: .string("hello"),
            modelSettings: ModelSettings(),
            tools: ([
                Tool.fileSearch(FileSearchTool(vectorStoreIDs: ["vs_1"], maxNumResults: 3, includeSearchResults: true)),
                Tool.computer(ComputerTool(computer: .instance(computer))),
                Tool.codeInterpreter(CodeInterpreterTool()),
                Tool.imageGeneration(ImageGenerationTool()),
                Tool.shell(ShellTool(executor: shellExec.execute, environment: .local(.init()))),
                Tool.applyPatch(ApplyPatchTool(editor: RecordingApplyPatchEditor())),
                Tool.localShell(LocalShellTool(executor: RecordingLocalShellExecutor().execute)),
            ] as [Tool]),
            outputSchema: nil,
            handoffs: [],
            tracing: .disabled,
            previousResponseID: nil,
            conversationID: nil,
            prompt: nil
        )

        let options = adapter.capturedRequest?.providerOptions?["openai"]?.objectValue ?? [:]
        let hosted = options[OpenAIProviderOptionKeys.hostedTools]?.arrayValue ?? []
        let types = hosted.compactMap { $0.objectValue?["type"]?.stringValue }
        #expect(types.contains("file_search"))
        #expect(types.contains("computer_use_preview"))
        #expect(types.contains("code_interpreter"))
        #expect(types.contains("image_generation"))
        #expect(types.contains("shell"))
        #expect(types.contains("apply_patch"))
        #expect(types.contains("local_shell"))
        #expect(options["include"] == .array([.string("file_search_call.results")]))
    }

    @Test
    func runner_executes_computer_call_and_feeds_back_computer_call_output() async throws {
        let computer = RecordingComputer()
        let model = SequencedModel { turn, input in
            if turn == 0 {
                return ModelResponse(output: [[
                    "id": .string("comp_1"),
                    "type": .string("computer_call"),
                    "call_id": .string("call_comp"),
                    "action": .object([
                        "type": .string("click"),
                        "x": .number(1),
                        "y": .number(2),
                        "button": .string("left"),
                    ]),
                ]], usage: Usage(), responseID: "resp1")
            }

            let hasComputerOutput = input.inputItems.contains { item in
                item["type"]?.stringValue == "computer_call_output"
            }
            #expect(hasComputerOutput)
            return ModelResponse(output: [[
                "id": .string("msg_2"),
                "type": .string("message"),
                "role": .string("assistant"),
                "content": .array([.object(["type": .string("output_text"), "text": .string("done")])]),
            ]], usage: Usage(), responseID: "resp2")
        }

        let agent = Agent<Void>(
            name: "computer-agent",
            instructions: .text("do it"),
            tools: [.computer(ComputerTool(computer: .instance(computer)))],
            model: .instance(model)
        )

        let result = try await Runner.run(agent, input: .string("go"), context: ())
        #expect(result.finalOutput as? String == "done")
        let calls = await computer.state.all()
        #expect(calls == ["click:1:2:left", "screenshot"])
    }

    @Test
    func runner_executes_local_shell_and_apply_patch_calls() async throws {
        let shell = RecordingLocalShellExecutor()
        let editor = RecordingApplyPatchEditor()
        let model = SequencedModel { turn, input in
            if turn == 0 {
                return ModelResponse(output: [[
                    "id": .string("lsh_1"),
                    "type": .string("local_shell_call"),
                    "call_id": .string("call_local"),
                    "action": .object([
                        "command": .array([.string("bash"), .string("-lc"), .string("echo hi")]),
                        "timeout_ms": .number(1000),
                    ]),
                ]], usage: Usage(), responseID: "resp1")
            } else if turn == 1 {
                let hasLocalShellOutput = input.inputItems.contains { $0["type"]?.stringValue == "local_shell_call_output" }
                #expect(hasLocalShellOutput)
                return ModelResponse(output: [[
                    "id": .string("patch_1"),
                    "type": .string("apply_patch_call"),
                    "call_id": .string("call_patch"),
                    "operation": .object([
                        "type": .string("update_file"),
                        "path": .string("test.md"),
                        "diff": .string("-a\n+b\n"),
                    ]),
                ]], usage: Usage(), responseID: "resp2")
            }

            let hasPatchOutput = input.inputItems.contains { $0["type"]?.stringValue == "apply_patch_call_output" }
            #expect(hasPatchOutput)
            return ModelResponse(output: [[
                "id": .string("msg_3"),
                "type": .string("message"),
                "role": .string("assistant"),
                "content": .array([.object(["type": .string("output_text"), "text": .string("patched")])]),
            ]], usage: Usage(), responseID: "resp3")
        }

        let agent = Agent<Void>(
            name: "patch-agent",
            instructions: .text("patch"),
            tools: [
                .localShell(LocalShellTool(executor: shell.execute)),
                .applyPatch(ApplyPatchTool(editor: editor)),
            ],
            model: .instance(model)
        )

        let result = try await Runner.run(agent, input: .string("go"), context: ())
        #expect(result.finalOutput as? String == "patched")
        #expect((await shell.state.all()).count == 1)
        #expect((await editor.state.all()).count == 1)
    }

    @Test
    func runner_skips_local_execution_for_hosted_shell_calls() async throws {
        let model = SequencedModel { turn, input in
            #expect(turn == 0)
            let _ = input
            return ModelResponse(output: [
                [
                    "id": .string("shell_1"),
                    "type": .string("shell_call"),
                    "call_id": .string("call_shell"),
                    "action": .object([
                        "type": .string("exec"),
                        "commands": .array([.string("echo hi")]),
                    ]),
                ],
                [
                    "id": .string("msg_1"),
                    "type": .string("message"),
                    "role": .string("assistant"),
                    "content": .array([.object(["type": .string("output_text"), "text": .string("hosted done")])]),
                ],
            ], usage: Usage(), responseID: "resp1")
        }

        let hostedEnvironment = ShellToolEnvironment.hosted(.containerAuto(.init()))
        let agent = Agent<Void>(
            name: "hosted-shell-agent",
            instructions: .text("shell"),
            tools: [.shell(ShellTool(executor: nil, environment: hostedEnvironment))],
            model: .instance(model)
        )

        let result = try await Runner.run(agent, input: .string("go"), context: ())
        #expect(result.finalOutput as? String == "hosted done")
        #expect(!result.newItems.contains { ($0 as? ToolCallOutputItem)?.rawItem["type"]?.stringValue == "shell_call_output" })
    }

    @Test
    func runner_processes_hosted_mcp_approval_callbacks() async throws {
        let model = SequencedModel { turn, input in
            if turn == 0 {
                return ModelResponse(output: [[
                    "id": .string("req_1"),
                    "type": .string("mcp_approval_request"),
                    "tool_name": .string("mcp_tool"),
                ]], usage: Usage(), responseID: "resp1")
            }
            let hasApprovalResponse = input.inputItems.contains { $0["type"]?.stringValue == "mcp_approval_response" }
            #expect(hasApprovalResponse)
            return ModelResponse(output: [[
                "id": .string("msg_2"),
                "type": .string("message"),
                "role": .string("assistant"),
                "content": .array([.object(["type": .string("output_text"), "text": .string("approved")])]),
            ]], usage: Usage(), responseID: "resp2")
        }

        let hostedMCP = HostedMCPTool(toolConfig: ["name": .string("mcp_tool")], onApprovalRequest: { _ in
            MCPToolApprovalFunctionResult(approve: true)
        })
        let agent = Agent<Void>(
            name: "mcp-agent",
            instructions: .text("mcp"),
            tools: [.hostedMCP(hostedMCP)],
            model: .instance(model)
        )

        let result = try await Runner.run(agent, input: .string("go"), context: ())
        #expect(result.finalOutput as? String == "approved")
    }
}
