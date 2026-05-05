import Foundation

enum ErrorHandlerRuntime {
    static func handleMaxTurns<TContext>(
        error: MaxTurnsExceeded,
        input: StringOrInputList,
        newItems: [any RunItem],
        rawResponses: [ModelResponse],
        history: [TResponseInputItem],
        lastAgent: AnyAgent,
        context: RunContextWrapper<TContext>,
        handlers: RunErrorHandlers<TContext>?
    ) async throws -> RunErrorHandlerResult? {
        guard let handler = handlers?.maxTurns else {
            return nil
        }
        guard let typedLastAgent = lastAgent.typed(as: TContext.self) else {
            return nil
        }
        let runData = RunErrorData(
            input: input,
            newItems: newItems,
            history: history,
            output: newItems.compactMap { try? $0.toInputItem() },
            rawResponses: rawResponses,
            lastAgent: typedLastAgent
        )
        return try await handler(.init(error: error, context: context, runData: runData))
    }
}
