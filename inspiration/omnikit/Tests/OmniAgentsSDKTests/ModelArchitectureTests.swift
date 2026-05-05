import Foundation
import Testing
import OmniAgentsSDK
import OmniAICore

private actor _SDKMockAdapterState {
    var requests: [Request] = []
    func append(_ request: Request) { requests.append(request) }
    func all() -> [Request] { requests }
}

private final class SDKMockAdapter: ProviderAdapter, @unchecked Sendable {
    let name: String
    let state = _SDKMockAdapterState()
    private let handler: @Sendable (Request) async throws -> Response

    init(name: String, handler: @escaping @Sendable (Request) async throws -> Response) {
        self.name = name
        self.handler = handler
    }

    func complete(request: Request) async throws -> Response {
        await state.append(request)
        return try await handler(request)
    }

    func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        let response = try await complete(request: request)
        return AsyncThrowingStream { continuation in
            continuation.yield(StreamEvent(type: .standard(.streamStart)))
            continuation.yield(StreamEvent(type: .standard(.finish), finishReason: response.finishReason, usage: response.usage, response: response))
            continuation.finish()
        }
    }
}

private func sdkResponse(provider: String, model: String, text: String) -> Response {
    Response(
        id: "resp_\(UUID().uuidString)",
        model: model,
        provider: provider,
        message: Message(role: .assistant, content: [.text(text)]),
        finishReason: .stop,
        usage: Usage(inputTokens: 1, outputTokens: 1)
    )
}

struct ModelArchitectureTests {
    @Test
    func multi_provider_routes_prefixed_model_names_through_core_client() async throws {
        let gemini = SDKMockAdapter(name: "gemini") { request in
            #expect(request.provider == "gemini")
            #expect(request.model == "gemini-2.5-pro")
            return sdkResponse(provider: "gemini", model: request.model, text: "gemini-ok")
        }
        let client = try Client(providers: ["gemini": gemini], defaultProvider: "gemini")

        let modelProvider = MultiProvider(client: client)
        let agent = Agent<Void>(
            name: "assistant",
            instructions: .text("Be helpful."),
            model: .name("gemini/gemini-2.5-pro")
        )

        let result = try await Runner.run(
            agent,
            input: .string("Hello"),
            context: (),
            runConfig: RunConfig(modelProvider: modelProvider)
        )

        #expect((result.finalOutput as? String) == "gemini-ok")
        let requests = await gemini.state.all()
        #expect(requests.count == 1)
    }

    @Test
    func multi_provider_uses_client_default_provider_for_bare_model_names() async throws {
        let gemini = SDKMockAdapter(name: "gemini") { request in
            #expect(request.provider == "gemini")
            #expect(request.model == "gemini-2.5-flash")
            return sdkResponse(provider: "gemini", model: request.model, text: "flash-ok")
        }
        let client = try Client(providers: ["gemini": gemini], defaultProvider: "gemini")

        let modelProvider = MultiProvider(client: client)
        let model = modelProvider.getModel("gemini-2.5-flash")
        let response = try await model.getResponse(
            systemInstructions: "Say hi",
            input: .string("Hello"),
            modelSettings: ModelSettings(),
            tools: [],
            outputSchema: nil,
            handoffs: [],
            tracing: .disabled,
            previousResponseID: nil,
            conversationID: nil,
            prompt: nil
        )

        #expect(response.output.first?["content"] != nil)
        let requests = await gemini.state.all()
        #expect(requests.count == 1)
    }

    @Test
    func openai_model_builds_scoped_provider_options_for_core_adapter() async throws {
        let openai = SDKMockAdapter(name: "openai") { request in
            let options = request.providerOptions?["openai"]?.objectValue ?? [:]
            #expect(options[OpenAIProviderOptionKeys.includeNativeWebSearch] == .bool(true))
            #expect(options[OpenAIProviderOptionKeys.webSearchExternalWebAccess] == .bool(true))
            #expect(options["custom_body"] == .string("x"))
            return sdkResponse(provider: "openai", model: request.model, text: "openai-ok")
        }
        let client = try Client(providers: ["openai": openai], defaultProvider: "openai")
        let model = OpenAIResponsesModel(modelName: "gpt-5.3-codex", client: client)

        _ = try await model.getResponse(
            systemInstructions: "Search",
            input: StringOrInputList.string("Find something"),
            modelSettings: ModelSettings(extraBody: ["custom_body": JSONValue.string("x")]),
            tools: [Tool.webSearch(WebSearchTool(externalWebAccess: true))],
            outputSchema: (nil as (any AgentOutputSchemaBase)?),
            handoffs: [],
            tracing: ModelTracing.disabled,
            previousResponseID: nil as String?,
            conversationID: nil as String?,
            prompt: nil as Prompt?
        )

        let requests = await openai.state.all()
        #expect(requests.count == 1)
        #expect(requests[0].provider == "openai")
    }
}
