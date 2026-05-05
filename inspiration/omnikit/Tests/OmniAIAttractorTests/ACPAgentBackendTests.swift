import Foundation
import Testing
@testable import OmniAIAttractor
@testable import OmniACP
@testable import OmniACPModel

private actor RecordedExecutionConfig {
    private(set) var values: [ACPExecutionConfiguration] = []
    func append(_ value: ACPExecutionConfiguration) {
        values.append(value)
    }
}

private actor AgentTaskStore {
    private var tasks: [Task<Void, Never>] = []
    func add(_ task: Task<Void, Never>) {
        tasks.append(task)
    }
    func cancelAll() {
        for task in tasks { task.cancel() }
        tasks.removeAll()
    }
}

private struct RecordingInMemoryTransportProvider: ACPTransportProvider {
    let recorder: RecordedExecutionConfig
    let taskStore: AgentTaskStore

    func makeTransport(configuration: ACPExecutionConfiguration) async throws -> any Transport {
        await recorder.append(configuration)
        let (clientTransport, agentTransport) = await InMemoryTransport.createConnectedPair()
        try await clientTransport.connect()
        try await agentTransport.connect()
        let task = startBackendAgent(on: agentTransport)
        await taskStore.add(task)
        return clientTransport
    }
}

private func startBackendAgent(on transport: InMemoryTransport) -> Task<Void, Never> {
    Task {
        try? await transport.connect()
        let encoder = JSONEncoder()
        let stream = transport.receive()
        do {
            for try await data in stream {
                let object = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
                let method = object?["method"] as? String
                let hasID = object?.keys.contains("id") == true
                guard let method, hasID else {
                    continue
                }
                switch method {
                case Initialize.name:
                    let request = try JSONDecoder().decode(Request<Initialize>.self, from: data)
                    let response = Initialize.response(
                        id: request.id,
                        result: .init(
                            protocolVersion: 1,
                            agentInfo: .init(name: "BackendAgent", version: "1.0.0"),
                            agentCapabilities: .init(loadSession: true, mcpCapabilities: .init(), promptCapabilities: .init())
                        )
                    )
                    try await transport.send(encoder.encode(response))
                case SessionNew.name:
                    let request = try JSONDecoder().decode(Request<SessionNew>.self, from: data)
                    try await transport.send(encoder.encode(SessionNew.response(id: request.id, result: .init(sessionID: "sess_backend"))))
                case SessionSetMode.name:
                    let request = try JSONDecoder().decode(Request<SessionSetMode>.self, from: data)
                    try await transport.send(encoder.encode(SessionSetMode.response(id: request.id)))
                case SessionPrompt.name:
                    let request = try JSONDecoder().decode(Request<SessionPrompt>.self, from: data)
                    let notifications: [Message<SessionUpdateNotification>] = [
                        .init(
                            method: SessionUpdateNotification.name,
                            params: .init(
                                sessionID: request.params.sessionID,
                                update: .plan(.init(entries: [
                                    .init(content: "Inspect repo", priority: "high", status: "completed"),
                                    .init(content: "Implement ACP backend", priority: "high", status: "completed"),
                                ]))
                            )
                        ),
                        .init(
                            method: SessionUpdateNotification.name,
                            params: .init(
                                sessionID: request.params.sessionID,
                                update: .toolCall(.init(toolCallID: "call_1", title: "Read file", kind: "read", status: "completed"))
                            )
                        ),
                        .init(
                            method: SessionUpdateNotification.name,
                            params: .init(
                                sessionID: request.params.sessionID,
                                update: .agentMessageChunk(.init(content: .init(text: "Backend agent response.\n\n```json\n{\"outcome\":\"success\",\"context_updates\":{\"acp_mock\":\"true\"},\"notes\":\"backend mock\"}\n```")))
                            )
                        ),
                    ]
                    for notification in notifications {
                        try await transport.send(encoder.encode(notification))
                    }
                    try await transport.send(encoder.encode(SessionPrompt.response(id: request.id, result: .init(stopReason: .endTurn))))
                case SessionCancel.name:
                    let request = try JSONDecoder().decode(Request<SessionCancel>.self, from: data)
                    try await transport.send(encoder.encode(SessionCancel.response(id: request.id)))
                default:
                    break
                }
            }
        } catch {
        }
    }
}

struct ACPAgentBackendTests {

    @Test
    func default_transport_provider_uses_websocket_transport_for_ws_urls() async throws {
        let transport = try await DefaultACPTransportProvider().makeTransport(configuration: .init(
            agentPath: "wss://example.invalid/acp",
            agentArguments: [],
            workingDirectory: FileManager.default.currentDirectoryPath,
            environment: [:],
            requestTimeout: .seconds(30),
            modeID: nil
        ))

        #expect(transport is WebSocketTransport)
    }

    @Test
    func default_transport_provider_uses_http_sse_transport_for_http_urls() async throws {
        let transport = try await DefaultACPTransportProvider().makeTransport(configuration: .init(
            agentPath: "https://example.invalid/acp",
            agentArguments: [],
            workingDirectory: FileManager.default.currentDirectoryPath,
            environment: [:],
            requestTimeout: .seconds(30),
            modeID: nil
        ))

        #expect(transport is HTTPSSETransport)
    }
    @Test
    func backend_runs_against_in_memory_agent_and_collects_metadata() async throws {
        let recorder = RecordedExecutionConfig()
        let taskStore = AgentTaskStore()
        defer {
            Task { await taskStore.cancelAll() }
        }
        let backend = ACPAgentBackend(
            configuration: .init(agentPath: "ignored", workingDirectory: FileManager.default.currentDirectoryPath),
            transportProvider: RecordingInMemoryTransportProvider(recorder: recorder, taskStore: taskStore),
            delegateProvider: DefaultACPClientDelegateProvider(permissionStrategy: .autoApprove)
        )
        let result = try await backend.run(
            prompt: "Summarize the repo",
            model: "gpt-5.3-codex",
            provider: "openai",
            reasoningEffort: "high",
            context: PipelineContext(["_graph_goal": "Ship ACP support"])
        )

        #expect(result.status == .success)
        #expect(result.contextUpdates["acp_mock"] == "true")
        #expect(result.contextUpdates["acp.plan"]?.contains("Inspect repo") == true)
        #expect(result.contextUpdates["acp.tool_calls"]?.contains("Read file") == true)
        #expect(result.notes.contains("backend mock"))
        #expect(result.notes.contains("ACP tool activity"))
        let configs = await recorder.values
        #expect(configs.count == 1)
        #expect(configs[0].agentPath == "ignored")
    }

    @Test
    func codergen_handler_plumbs_graph_and_node_acp_attrs_into_backend_configuration() async throws {
        let recorder = RecordedExecutionConfig()
        let taskStore = AgentTaskStore()
        defer {
            Task { await taskStore.cancelAll() }
        }
        let backend = ACPAgentBackend(
            configuration: .init(agentPath: "fallback-agent", workingDirectory: FileManager.default.currentDirectoryPath),
            transportProvider: RecordingInMemoryTransportProvider(recorder: recorder, taskStore: taskStore),
            delegateProvider: DefaultACPClientDelegateProvider(permissionStrategy: .autoApprove)
        )
        let handler = CodergenHandler(backend: backend)
        let node = Node(
            id: "codegen",
            label: "Codegen",
            prompt: "Do ACP work",
            rawAttributes: [
                "acp_agent_path": .string("node-agent"),
                "acp_agent_args": .string("--fast,--json"),
                "acp_cwd": .string("."),
                "acp_timeout_seconds": .string("30"),
                "acp_mode": .string("ask"),
            ]
        )
        let graph = Graph(
            id: "pipeline",
            nodes: [
                "codegen": node,
                "start": Node(id: "start", shape: "Mdiamond"),
                "done": Node(id: "done", shape: "Msquare"),
            ],
            edges: [Edge(from: "start", to: "codegen"), Edge(from: "codegen", to: "done")],
            rawAttributes: ["acp_agent_path": .string("graph-agent")]
        )
        let context = PipelineContext(["current_node": "codegen"])
        let logsRoot = URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: logsRoot, withIntermediateDirectories: true)

        let outcome = try await handler.execute(node: node, context: context, graph: graph, logsRoot: logsRoot)
        #expect(outcome.status == .success)
        let configs = await recorder.values
        #expect(configs.count == 1)
        #expect(configs[0].agentPath == "node-agent")
        #expect(configs[0].agentArguments == ["--fast", "--json"])
        #expect(configs[0].modeID == "ask")
        #expect(configs[0].requestTimeout != nil)
    }
}
