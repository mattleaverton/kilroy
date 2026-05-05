import Foundation

public enum NextStep<TContext>: @unchecked Sendable {
    case finalOutput(Any)
    case handoff(Handoff<TContext>, TResponseOutputItem)
    case runAgain([TResponseInputItem])
    case interruption([ToolApprovalItem])
}

public struct ProcessedResponse<TContext>: @unchecked Sendable {
    public var items: [any RunItem]
    public var nextStep: NextStep<TContext>
    public var response: ModelResponse
    public var toolResults: [FunctionToolResult]

    public init(items: [any RunItem], nextStep: NextStep<TContext>, response: ModelResponse, toolResults: [FunctionToolResult] = []) {
        self.items = items
        self.nextStep = nextStep
        self.response = response
        self.toolResults = toolResults
    }
}
