import Foundation

public struct AgentToolInvocation: Sendable, Equatable {
    public var toolName: String
    public var toolCallID: String
    public var toolArguments: String

    public init(toolName: String, toolCallID: String, toolArguments: String) {
        self.toolName = toolName
        self.toolCallID = toolCallID
        self.toolArguments = toolArguments
    }
}

open class RunResultBase<TContext>: @unchecked Sendable, CustomStringConvertible {
    public var input: StringOrInputList
    public var newItems: [any RunItem]
    public var rawResponses: [ModelResponse]
    public var finalOutput: Any
    public var inputGuardrailResults: [InputGuardrailResult<TContext>]
    public var outputGuardrailResults: [OutputGuardrailResult<TContext>]
    public var toolInputGuardrailResults: [ToolInputGuardrailResult<TContext>]
    public var toolOutputGuardrailResults: [ToolOutputGuardrailResult<TContext>]
    public var contextWrapper: RunContextWrapper<TContext>
    public var trace: Trace?

    public init(
        input: StringOrInputList,
        newItems: [any RunItem],
        rawResponses: [ModelResponse],
        finalOutput: Any,
        inputGuardrailResults: [InputGuardrailResult<TContext>],
        outputGuardrailResults: [OutputGuardrailResult<TContext>],
        toolInputGuardrailResults: [ToolInputGuardrailResult<TContext>],
        toolOutputGuardrailResults: [ToolOutputGuardrailResult<TContext>],
        contextWrapper: RunContextWrapper<TContext>,
        trace: Trace? = nil
    ) {
        self.input = input
        self.newItems = newItems
        self.rawResponses = rawResponses
        self.finalOutput = finalOutput
        self.inputGuardrailResults = inputGuardrailResults
        self.outputGuardrailResults = outputGuardrailResults
        self.toolInputGuardrailResults = toolInputGuardrailResults
        self.toolOutputGuardrailResults = toolOutputGuardrailResults
        self.contextWrapper = contextWrapper
        self.trace = trace
    }

    open var lastAgent: AnyAgent? { nil }

    public func releaseAgents() {
        newItems.forEach { $0.releaseAgent() }
    }

    public func finalOutputAs<T>(_ type: T.Type) -> T? {
        finalOutput as? T
    }

    public func toInputList() -> [TResponseInputItem] {
        let inputItems = input.inputItems
        let generatedItems = newItems.compactMap { item -> TResponseInputItem? in
            try? item.toInputItem()
        }
        return inputItems + generatedItems
    }

    public func lastResponseID() -> String? {
        rawResponses.last?.responseID
    }

    public var agentToolInvocation: AgentToolInvocation? {
        guard let toolContext = contextWrapper as? ToolContext<TContext> else {
            return nil
        }
        return AgentToolInvocation(
            toolName: toolContext.toolName,
            toolCallID: toolContext.toolCallID,
            toolArguments: toolContext.toolArguments
        )
    }

    public var agent_tool_invocation: AgentToolInvocation? {
        agentToolInvocation
    }

    public var description: String {
        prettyPrintRunResult(self)
    }
}

public final class RunResult<TContext>: RunResultBase<TContext>, @unchecked Sendable {
    public var storedLastAgent: AnyAgent
    public var lastProcessedResponse: ProcessedResponse<TContext>?
    public var toolUseTrackerSnapshot: [String: [String]]
    public var currentTurnPersistedItemCount: Int
    public var currentTurn: Int
    public var modelInputItems: [TResponseInputItem]
    public var originalInput: StringOrInputList?
    public var conversationID: String?
    public var previousResponseID: String?
    public var autoPreviousResponseID: Bool
    public var reasoningItemIdPolicy: ReasoningItemIdPolicy?
    public var maxTurns: Int
    public var interruptions: [ToolApprovalItem]

    public init(
        input: StringOrInputList,
        newItems: [any RunItem],
        rawResponses: [ModelResponse],
        finalOutput: Any,
        inputGuardrailResults: [InputGuardrailResult<TContext>],
        outputGuardrailResults: [OutputGuardrailResult<TContext>],
        toolInputGuardrailResults: [ToolInputGuardrailResult<TContext>],
        toolOutputGuardrailResults: [ToolOutputGuardrailResult<TContext>],
        contextWrapper: RunContextWrapper<TContext>,
        lastAgent: AnyAgent,
        lastProcessedResponse: ProcessedResponse<TContext>? = nil,
        toolUseTrackerSnapshot: [String: [String]] = [:],
        currentTurnPersistedItemCount: Int = 0,
        currentTurn: Int = 0,
        modelInputItems: [TResponseInputItem] = [],
        originalInput: StringOrInputList? = nil,
        conversationID: String? = nil,
        previousResponseID: String? = nil,
        autoPreviousResponseID: Bool = false,
        reasoningItemIdPolicy: ReasoningItemIdPolicy? = nil,
        maxTurns: Int = DEFAULT_MAX_TURNS,
        interruptions: [ToolApprovalItem] = [],
        trace: Trace? = nil
    ) {
        self.storedLastAgent = lastAgent
        self.lastProcessedResponse = lastProcessedResponse
        self.toolUseTrackerSnapshot = toolUseTrackerSnapshot
        self.currentTurnPersistedItemCount = currentTurnPersistedItemCount
        self.currentTurn = currentTurn
        self.modelInputItems = modelInputItems
        self.originalInput = originalInput
        self.conversationID = conversationID
        self.previousResponseID = previousResponseID
        self.autoPreviousResponseID = autoPreviousResponseID
        self.reasoningItemIdPolicy = reasoningItemIdPolicy
        self.maxTurns = maxTurns
        self.interruptions = interruptions
        super.init(
            input: input,
            newItems: newItems,
            rawResponses: rawResponses,
            finalOutput: finalOutput,
            inputGuardrailResults: inputGuardrailResults,
            outputGuardrailResults: outputGuardrailResults,
            toolInputGuardrailResults: toolInputGuardrailResults,
            toolOutputGuardrailResults: toolOutputGuardrailResults,
            contextWrapper: contextWrapper,
            trace: trace
        )
    }

    public override var lastAgent: AnyAgent? {
        storedLastAgent
    }

    public func toState() -> RunState<TContext> {
        RunState(
            currentTurn: currentTurn,
            currentAgent: storedLastAgent.typed(as: TContext.self),
            originalInput: originalInput ?? input,
            modelResponses: rawResponses,
            contextWrapper: contextWrapper,
            generatedItems: newItems,
            maxTurns: maxTurns,
            conversationID: conversationID,
            previousResponseID: previousResponseID,
            autoPreviousResponseID: autoPreviousResponseID,
            reasoningItemIdPolicy: reasoningItemIdPolicy,
            inputGuardrailResults: inputGuardrailResults,
            outputGuardrailResults: outputGuardrailResults,
            toolInputGuardrailResults: toolInputGuardrailResults,
            toolOutputGuardrailResults: toolOutputGuardrailResults,
            currentTurnPersistedItemCount: currentTurnPersistedItemCount,
            toolUseTrackerSnapshot: toolUseTrackerSnapshot,
            trace: trace,
            modelInputItems: modelInputItems,
            interruptions: interruptions
        )
    }
}

public final class RunResultStreaming<TContext>: RunResultBase<TContext>, @unchecked Sendable {
    public var currentAgent: AnyAgent
    public var currentTurn: Int
    public var maxTurns: Int
    public var isComplete: Bool
    public var interruptions: [ToolApprovalItem]
    private let eventStreamStorage: AsyncThrowingStream<AgentStreamEvent, Error>
    private let eventContinuation: AsyncThrowingStream<AgentStreamEvent, Error>.Continuation
    private var runLoopTask: Task<RunResult<TContext>, Error>?
    private var storedResult: RunResult<TContext>?
    private var storedException: Error?

    public init(
        input: StringOrInputList,
        contextWrapper: RunContextWrapper<TContext>,
        currentAgent: AnyAgent,
        currentTurn: Int,
        maxTurns: Int,
        finalOutput: Any = NSNull(),
        trace: Trace? = nil
    ) {
        let streamPair = makeThrowingStream(of: AgentStreamEvent.self)
        let stream = streamPair.stream
        self.currentAgent = currentAgent
        self.currentTurn = currentTurn
        self.maxTurns = maxTurns
        self.isComplete = false
        self.interruptions = []
        self.eventStreamStorage = stream
        self.eventContinuation = streamPair.continuation
        super.init(
            input: input,
            newItems: [],
            rawResponses: [],
            finalOutput: finalOutput,
            inputGuardrailResults: [],
            outputGuardrailResults: [],
            toolInputGuardrailResults: [],
            toolOutputGuardrailResults: [],
            contextWrapper: contextWrapper,
            trace: trace
        )
    }

    public override var lastAgent: AnyAgent? {
        storedResult?.lastAgent ?? currentAgent
    }

    public func attach(task: Task<RunResult<TContext>, Error>) {
        self.runLoopTask = task
    }

    public func cancel() {
        runLoopTask?.cancel()
        eventContinuation.finish()
    }

    public func streamEvents() -> AsyncThrowingStream<AgentStreamEvent, Error> {
        eventStreamStorage
    }

    func yield(_ event: AgentStreamEvent) {
        eventContinuation.yield(event)
    }

    func finish(with result: RunResult<TContext>) {
        storedResult = result
        newItems = result.newItems
        rawResponses = result.rawResponses
        finalOutput = result.finalOutput
        inputGuardrailResults = result.inputGuardrailResults
        outputGuardrailResults = result.outputGuardrailResults
        toolInputGuardrailResults = result.toolInputGuardrailResults
        toolOutputGuardrailResults = result.toolOutputGuardrailResults
        interruptions = result.interruptions
        isComplete = true
        eventContinuation.finish()
    }

    func fail(with error: Error) {
        storedException = error
        eventContinuation.finish(throwing: error)
    }

    public func toState() -> RunState<TContext> {
        if let storedResult {
            return storedResult.toState()
        }
        return RunState(
            currentTurn: currentTurn,
            currentAgent: currentAgent.typed(as: TContext.self),
            originalInput: input,
            modelResponses: rawResponses,
            contextWrapper: contextWrapper,
            generatedItems: newItems,
            maxTurns: maxTurns,
            interruptions: interruptions
        )
    }

    public override var description: String {
        prettyPrintRunResultStreaming(self)
    }
}
