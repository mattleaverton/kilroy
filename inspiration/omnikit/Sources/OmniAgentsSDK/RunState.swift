import Foundation
import OmniAICore

public final class RunState<TContext>: @unchecked Sendable {
    public var currentTurn: Int
    public var currentAgent: Agent<TContext>?
    public var originalInput: StringOrInputList
    public var modelResponses: [ModelResponse]
    public var contextWrapper: RunContextWrapper<TContext>?
    public var generatedItems: [any RunItem]
    public var sessionItems: [any RunItem]
    public var maxTurns: Int
    public var conversationID: String?
    public var previousResponseID: String?
    public var autoPreviousResponseID: Bool
    public var reasoningItemIdPolicy: ReasoningItemIdPolicy?
    public var inputGuardrailResults: [InputGuardrailResult<TContext>]
    public var outputGuardrailResults: [OutputGuardrailResult<TContext>]
    public var toolInputGuardrailResults: [ToolInputGuardrailResult<TContext>]
    public var toolOutputGuardrailResults: [ToolOutputGuardrailResult<TContext>]
    public var currentTurnPersistedItemCount: Int
    public var toolUseTrackerSnapshot: [String: [String]]
    public var trace: Trace?
    public var modelInputItems: [TResponseInputItem]
    public var interruptions: [ToolApprovalItem]

    public init(
        currentTurn: Int = 0,
        currentAgent: Agent<TContext>? = nil,
        originalInput: StringOrInputList,
        modelResponses: [ModelResponse] = [],
        contextWrapper: RunContextWrapper<TContext>? = nil,
        generatedItems: [any RunItem] = [],
        sessionItems: [any RunItem] = [],
        maxTurns: Int = DEFAULT_MAX_TURNS,
        conversationID: String? = nil,
        previousResponseID: String? = nil,
        autoPreviousResponseID: Bool = false,
        reasoningItemIdPolicy: ReasoningItemIdPolicy? = nil,
        inputGuardrailResults: [InputGuardrailResult<TContext>] = [],
        outputGuardrailResults: [OutputGuardrailResult<TContext>] = [],
        toolInputGuardrailResults: [ToolInputGuardrailResult<TContext>] = [],
        toolOutputGuardrailResults: [ToolOutputGuardrailResult<TContext>] = [],
        currentTurnPersistedItemCount: Int = 0,
        toolUseTrackerSnapshot: [String: [String]] = [:],
        trace: Trace? = nil,
        modelInputItems: [TResponseInputItem] = [],
        interruptions: [ToolApprovalItem] = []
    ) {
        self.currentTurn = currentTurn
        self.currentAgent = currentAgent
        self.originalInput = originalInput
        self.modelResponses = modelResponses
        self.contextWrapper = contextWrapper
        self.generatedItems = generatedItems
        self.sessionItems = sessionItems
        self.maxTurns = maxTurns
        self.conversationID = conversationID
        self.previousResponseID = previousResponseID
        self.autoPreviousResponseID = autoPreviousResponseID
        self.reasoningItemIdPolicy = reasoningItemIdPolicy
        self.inputGuardrailResults = inputGuardrailResults
        self.outputGuardrailResults = outputGuardrailResults
        self.toolInputGuardrailResults = toolInputGuardrailResults
        self.toolOutputGuardrailResults = toolOutputGuardrailResults
        self.currentTurnPersistedItemCount = currentTurnPersistedItemCount
        self.toolUseTrackerSnapshot = toolUseTrackerSnapshot
        self.trace = trace
        self.modelInputItems = modelInputItems
        self.interruptions = interruptions
    }

    public func getInterruptions() -> [ToolApprovalItem] {
        interruptions
    }

    public func approve(_ approval: ToolApproval, alwaysApprove: Bool = false) {
        contextWrapper?.approveTool(approval, alwaysApprove: alwaysApprove)
    }

    public func approve(_ item: ToolApprovalItem, alwaysApprove: Bool = false) {
        contextWrapper?.approveTool(approvalItem: item, alwaysApprove: alwaysApprove)
    }

    public func reject(_ approval: ToolApproval, alwaysReject: Bool = false) {
        contextWrapper?.rejectTool(approval, alwaysReject: alwaysReject)
    }

    public func reject(_ item: ToolApprovalItem, alwaysReject: Bool = false) {
        contextWrapper?.rejectTool(approvalItem: item, alwaysReject: alwaysReject)
    }

    public func toJSON() -> [String: JSONValue] {
        var root: [String: JSONValue] = [
            "current_turn": .number(Double(currentTurn)),
            "max_turns": .number(Double(maxTurns)),
            "original_input": serializeOriginalInput(originalInput),
            "model_input_items": .array(modelInputItems.map(JSONValue.object)),
            "tool_use_tracker_snapshot": .object(toolUseTrackerSnapshot.mapValues { .array($0.map(JSONValue.string)) }),
        ]
        if let conversationID {
            root["conversation_id"] = .string(conversationID)
        }
        if let previousResponseID {
            root["previous_response_id"] = .string(previousResponseID)
        }
        if autoPreviousResponseID {
            root["auto_previous_response_id"] = .bool(true)
        }
        if let reasoningItemIdPolicy {
            root["reasoning_item_id_policy"] = .string(reasoningItemIdPolicy.rawValue)
        }
        if let currentAgent {
            root["current_agent_name"] = .string(currentAgent.name)
        }
        if let contextWrapper {
            root["approvals"] = .object(contextWrapper.serializedApprovals().mapValues(JSONValue.object))
        }
        return root
    }

    public func toString() throws -> String {
        let data = try JSONValue.object(toJSON()).data(prettyPrinted: true)
        return String(decoding: data, as: UTF8.self)
    }

    public static func fromString(
        _ string: String,
        startingAgent: Agent<TContext>,
        context: TContext
    ) async throws -> RunState<TContext> {
        guard let data = string.data(using: .utf8),
              let value = try? JSONValue.parse(data),
              case .object(let object) = value
        else {
            throw UserError(message: "Invalid run state JSON")
        }
        return try await fromJSON(object, startingAgent: startingAgent, context: context)
    }

    public static func fromJSON(
        _ object: [String: JSONValue],
        startingAgent: Agent<TContext>,
        context: TContext
    ) async throws -> RunState<TContext> {
        let wrapper = RunContextWrapper(context: context)
        if case .object(let approvals)? = object["approvals"] {
            let serialized = approvals.compactMapValues { value -> [String: JSONValue]? in
                value.objectValue
            }
            wrapper.rebuildApprovals(from: serialized)
        }
        let modelInputItems = object["model_input_items"]?.arrayValue?.compactMap { $0.objectValue } ?? []
        return RunState(
            currentTurn: Int(object["current_turn"]?.doubleValue ?? 0),
            currentAgent: startingAgent,
            originalInput: deserializeOriginalInput(object["original_input"]),
            contextWrapper: wrapper,
            maxTurns: Int(object["max_turns"]?.doubleValue ?? Double(DEFAULT_MAX_TURNS)),
            conversationID: object["conversation_id"]?.stringValue,
            previousResponseID: object["previous_response_id"]?.stringValue,
            autoPreviousResponseID: object["auto_previous_response_id"]?.boolValue ?? false,
            reasoningItemIdPolicy: object["reasoning_item_id_policy"]?.stringValue.flatMap(ReasoningItemIdPolicy.init(rawValue:)),
            toolUseTrackerSnapshot: object["tool_use_tracker_snapshot"]?.objectValue?.mapValues { value in
                value.arrayValue?.compactMap(\.stringValue) ?? []
            } ?? [:],
            modelInputItems: modelInputItems
        )
    }
}

private func serializeOriginalInput(_ input: StringOrInputList) -> JSONValue {
    switch input {
    case .string(let string):
        return .object(["kind": .string("string"), "value": .string(string)])
    case .inputList(let items):
        return .object(["kind": .string("input_list"), "value": .array(items.map(JSONValue.object))])
    }
}

private func deserializeOriginalInput(_ value: JSONValue?) -> StringOrInputList {
    guard case .object(let object)? = value else {
        return .inputList([])
    }
    switch object["kind"]?.stringValue {
    case "string":
        return .string(object["value"]?.stringValue ?? "")
    case "input_list":
        let items = object["value"]?.arrayValue?.compactMap { $0.objectValue } ?? []
        return .inputList(items)
    default:
        return .inputList([])
    }
}
