import Foundation

public final class AgentRunner: @unchecked Sendable {
    public init() {}

    public func run<TContext>(
        _ startingAgent: Agent<TContext>,
        input: StringOrInputList,
        context: TContext? = nil,
        maxTurns: Int = DEFAULT_MAX_TURNS,
        hooks: RunHooks<TContext>? = nil,
        runConfig: RunConfig? = nil,
        errorHandlers: RunErrorHandlers<TContext>? = nil,
        previousResponseID: String? = nil,
        autoPreviousResponseID: Bool = false,
        conversationID: String? = nil,
        session: Session? = nil
    ) async throws -> RunResult<TContext> {
        let contextWrapper = try AgentRunnerHelpers.makeContextWrapper(context: context)
        return try await AgentRunLoop.run(
            startingAgent: startingAgent,
            input: input,
            contextWrapper: contextWrapper,
            maxTurns: maxTurns,
            hooks: hooks,
            runConfig: runConfig,
            errorHandlers: errorHandlers,
            previousResponseID: previousResponseID,
            autoPreviousResponseID: autoPreviousResponseID,
            conversationID: conversationID,
            session: session
        )
    }

    public func run<TContext>(
        _ startingAgent: Agent<TContext>,
        state: RunState<TContext>,
        context: TContext? = nil,
        maxTurns: Int = DEFAULT_MAX_TURNS,
        hooks: RunHooks<TContext>? = nil,
        runConfig: RunConfig? = nil,
        errorHandlers: RunErrorHandlers<TContext>? = nil,
        session: Session? = nil
    ) async throws -> RunResult<TContext> {
        let contextWrapper = try (state.contextWrapper ?? AgentRunnerHelpers.makeContextWrapper(context: context))
        return try await AgentRunLoop.run(
            startingAgent: startingAgent,
            input: state.originalInput,
            contextWrapper: contextWrapper,
            maxTurns: maxTurns,
            hooks: hooks,
            runConfig: runConfig,
            errorHandlers: errorHandlers,
            previousResponseID: state.previousResponseID,
            autoPreviousResponseID: state.autoPreviousResponseID,
            conversationID: state.conversationID,
            session: session,
            existingState: state
        )
    }

    public func runStreamed<TContext>(
        _ startingAgent: Agent<TContext>,
        input: StringOrInputList,
        context: TContext? = nil,
        maxTurns: Int = DEFAULT_MAX_TURNS,
        hooks: RunHooks<TContext>? = nil,
        runConfig: RunConfig? = nil,
        previousResponseID: String? = nil,
        autoPreviousResponseID: Bool = false,
        conversationID: String? = nil,
        session: Session? = nil,
        errorHandlers: RunErrorHandlers<TContext>? = nil
    ) -> RunResultStreaming<TContext> {
        let contextWrapper: RunContextWrapper<TContext>
        do {
            contextWrapper = try AgentRunnerHelpers.makeContextWrapper(context: context)
        } catch {
            preconditionFailure("Failed to create run context for streamed agent run: \(error)")
        }
        let streaming = RunResultStreaming<TContext>(
            input: input,
            contextWrapper: contextWrapper,
            currentAgent: AnyAgent(startingAgent),
            currentTurn: 0,
            maxTurns: maxTurns
        )
        let task = Task {
            do {
                let result = try await AgentRunLoop.run(
                    startingAgent: startingAgent,
                    input: input,
                    contextWrapper: contextWrapper,
                    maxTurns: maxTurns,
                    hooks: hooks,
                    runConfig: runConfig,
                    errorHandlers: errorHandlers,
                    previousResponseID: previousResponseID,
                    autoPreviousResponseID: autoPreviousResponseID,
                    conversationID: conversationID,
                    session: session,
                    eventSink: { event in
                        streaming.yield(event)
                    }
                )
                streaming.finish(with: result)
                return result
            } catch {
                streaming.fail(with: error)
                throw error
            }
        }
        streaming.attach(task: task)
        return streaming
    }
}

public let DEFAULT_AGENT_RUNNER = AgentRunner()

public enum Runner {
    public static func run<TContext>(
        _ startingAgent: Agent<TContext>,
        input: StringOrInputList,
        context: TContext? = nil,
        maxTurns: Int = DEFAULT_MAX_TURNS,
        hooks: RunHooks<TContext>? = nil,
        runConfig: RunConfig? = nil,
        errorHandlers: RunErrorHandlers<TContext>? = nil,
        previousResponseID: String? = nil,
        autoPreviousResponseID: Bool = false,
        conversationID: String? = nil,
        session: Session? = nil
    ) async throws -> RunResult<TContext> {
        try await DEFAULT_AGENT_RUNNER.run(
            startingAgent,
            input: input,
            context: context,
            maxTurns: maxTurns,
            hooks: hooks,
            runConfig: runConfig,
            errorHandlers: errorHandlers,
            previousResponseID: previousResponseID,
            autoPreviousResponseID: autoPreviousResponseID,
            conversationID: conversationID,
            session: session
        )
    }

    public static func runStreamed<TContext>(
        _ startingAgent: Agent<TContext>,
        input: StringOrInputList,
        context: TContext? = nil,
        maxTurns: Int = DEFAULT_MAX_TURNS,
        hooks: RunHooks<TContext>? = nil,
        runConfig: RunConfig? = nil,
        previousResponseID: String? = nil,
        autoPreviousResponseID: Bool = false,
        conversationID: String? = nil,
        session: Session? = nil,
        errorHandlers: RunErrorHandlers<TContext>? = nil
    ) -> RunResultStreaming<TContext> {
        DEFAULT_AGENT_RUNNER.runStreamed(
            startingAgent,
            input: input,
            context: context,
            maxTurns: maxTurns,
            hooks: hooks,
            runConfig: runConfig,
            previousResponseID: previousResponseID,
            autoPreviousResponseID: autoPreviousResponseID,
            conversationID: conversationID,
            session: session,
            errorHandlers: errorHandlers
        )
    }
}
