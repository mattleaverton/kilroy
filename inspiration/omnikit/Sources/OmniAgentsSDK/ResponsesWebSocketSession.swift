import Foundation

public struct ResponsesWebSocketSession: Sendable {
    public var provider: OpenAIProvider
    public var runConfig: RunConfig

    public init(provider: OpenAIProvider = OpenAIProvider(useResponsesWebSocket: true), runConfig: RunConfig = RunConfig()) {
        self.provider = provider
        var resolved = runConfig
        resolved.modelProvider = provider
        self.runConfig = resolved
    }

    public func run<TContext>(
        _ agent: Agent<TContext>,
        input: StringOrInputList,
        context: TContext,
        maxTurns: Int = DEFAULT_MAX_TURNS
    ) async throws -> RunResult<TContext> {
        try await Runner.run(
            agent,
            input: input,
            context: context,
            maxTurns: maxTurns,
            hooks: nil,
            runConfig: runConfig,
            errorHandlers: nil,
            previousResponseID: nil,
            autoPreviousResponseID: false,
            conversationID: nil,
            session: nil
        )
    }

    public func runStreamed<TContext>(
        _ agent: Agent<TContext>,
        input: StringOrInputList,
        context: TContext,
        maxTurns: Int = DEFAULT_MAX_TURNS
    ) -> RunResultStreaming<TContext> {
        Runner.runStreamed(
            agent,
            input: input,
            context: context,
            maxTurns: maxTurns,
            hooks: nil,
            runConfig: runConfig,
            previousResponseID: nil,
            autoPreviousResponseID: false,
            conversationID: nil,
            session: nil,
            errorHandlers: nil
        )
    }
}

public func responsesWebSocketSession(
    provider: OpenAIProvider = OpenAIProvider(useResponsesWebSocket: true),
    runConfig: RunConfig = RunConfig()
) -> ResponsesWebSocketSession {
    ResponsesWebSocketSession(provider: provider, runConfig: runConfig)
}
