import Foundation

public struct RunErrorData<TContext>: Sendable {
    public var input: StringOrInputList
    public var newItems: [any RunItem]
    public var history: [TResponseInputItem]
    public var output: [TResponseInputItem]
    public var rawResponses: [ModelResponse]
    public var lastAgent: Agent<TContext>

    public init(
        input: StringOrInputList,
        newItems: [any RunItem],
        history: [TResponseInputItem],
        output: [TResponseInputItem],
        rawResponses: [ModelResponse],
        lastAgent: Agent<TContext>
    ) {
        self.input = input
        self.newItems = newItems
        self.history = history
        self.output = output
        self.rawResponses = rawResponses
        self.lastAgent = lastAgent
    }
}

public struct RunErrorHandlerInput<TContext>: Sendable {
    public var error: MaxTurnsExceeded
    public var context: RunContextWrapper<TContext>
    public var runData: RunErrorData<TContext>

    public init(error: MaxTurnsExceeded, context: RunContextWrapper<TContext>, runData: RunErrorData<TContext>) {
        self.error = error
        self.context = context
        self.runData = runData
    }
}

public struct RunErrorHandlerResult: @unchecked Sendable {
    public var finalOutput: Any
    public var includeInHistory: Bool

    public init(finalOutput: Any, includeInHistory: Bool = true) {
        self.finalOutput = finalOutput
        self.includeInHistory = includeInHistory
    }
}

public typealias RunErrorHandler<TContext> = @Sendable (RunErrorHandlerInput<TContext>) async throws -> RunErrorHandlerResult?

public struct RunErrorHandlers<TContext>: Sendable {
    public var maxTurns: RunErrorHandler<TContext>?

    public init(maxTurns: RunErrorHandler<TContext>? = nil) {
        self.maxTurns = maxTurns
    }
}
