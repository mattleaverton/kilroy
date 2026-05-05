import Foundation
import OmniAICore

public struct HandoffInputData: Sendable {
    public var inputHistory: StringOrInputList
    public var preHandoffItems: [any RunItem]
    public var newItems: [any RunItem]
    public var runContext: RunContextWrapper<Any>?
    public var inputItems: [any RunItem]?

    public init(
        inputHistory: StringOrInputList,
        preHandoffItems: [any RunItem] = [],
        newItems: [any RunItem] = [],
        runContext: RunContextWrapper<Any>? = nil,
        inputItems: [any RunItem]? = nil
    ) {
        self.inputHistory = inputHistory
        self.preHandoffItems = preHandoffItems
        self.newItems = newItems
        self.runContext = runContext
        self.inputItems = inputItems
    }

    public func clone() -> HandoffInputData {
        HandoffInputData(
            inputHistory: inputHistory,
            preHandoffItems: preHandoffItems,
            newItems: newItems,
            runContext: runContext,
            inputItems: inputItems
        )
    }
}

public typealias HandoffInputFilter = @Sendable (HandoffInputData) async throws -> HandoffInputData
public typealias HandoffHistoryMapper = @Sendable (HandoffInputData) async throws -> [TResponseInputItem]

public struct Handoff<TContext>: @unchecked Sendable {
    public var toolName: String
    public var toolDescription: String
    public var inputJSONSchema: [String: JSONValue]
    public var onInvokeHandoff: @Sendable (RunContextWrapper<TContext>, String) async throws -> Agent<TContext>
    public var agentName: String
    public var inputFilter: HandoffInputFilter?
    public var nestHandoffHistory: Bool?
    public var strictJSONSchema: Bool
    public var isEnabled: ToolEnabledPredicate

    public init(
        toolName: String,
        toolDescription: String,
        inputJSONSchema: [String: JSONValue] = ensureStrictJSONSchema([
            "type": .string("object"),
            "properties": .object(["input": .object(["type": .string("string")])]),
            "required": .array([.string("input")]),
            "additionalProperties": .bool(false),
        ]),
        onInvokeHandoff: @escaping @Sendable (RunContextWrapper<TContext>, String) async throws -> Agent<TContext>,
        agentName: String,
        inputFilter: HandoffInputFilter? = nil,
        nestHandoffHistory: Bool? = nil,
        strictJSONSchema: Bool = true,
        isEnabled: ToolEnabledPredicate = .always(true)
    ) {
        self.toolName = toolName
        self.toolDescription = toolDescription
        self.inputJSONSchema = strictJSONSchema ? ensureStrictJSONSchema(inputJSONSchema) : inputJSONSchema
        self.onInvokeHandoff = onInvokeHandoff
        self.agentName = agentName
        self.inputFilter = inputFilter
        self.nestHandoffHistory = nestHandoffHistory
        self.strictJSONSchema = strictJSONSchema
        self.isEnabled = isEnabled
    }

    public func getTransferMessage() -> String {
        "Transferred to \(agentName)"
    }

    public static func defaultToolName(agent: Agent<TContext>) -> String {
        transformStringFunctionStyle("transfer_to_\(agent.name)")
    }

    public static func defaultToolDescription(agent: Agent<TContext>) -> String {
        let description = agent.handoffDescription?.trimmingCharacters(in: .whitespacesAndNewlines)
        if let description, !description.isEmpty {
            return description
        }
        return "Handoff to \(agent.name)"
    }
}

private final class ConversationHistoryWrapperStore: @unchecked Sendable {
    static let shared = ConversationHistoryWrapperStore()

    private let lock = NSLock()
    private var mapper: HandoffHistoryMapper?

    func set(_ mapper: HandoffHistoryMapper?) {
        lock.lock()
        self.mapper = mapper
        lock.unlock()
    }

    func get() -> HandoffHistoryMapper? {
        lock.lock()
        defer { lock.unlock() }
        return mapper
    }
}

public func setConversationHistoryWrappers(_ mapper: HandoffHistoryMapper?) {
    ConversationHistoryWrapperStore.shared.set(mapper)
}

public func getConversationHistoryWrappers() -> HandoffHistoryMapper? {
    ConversationHistoryWrapperStore.shared.get()
}

public func resetConversationHistoryWrappers() {
    ConversationHistoryWrapperStore.shared.set(nil)
}

public func defaultHandoffHistoryMapper(_ data: HandoffInputData) async throws -> [TResponseInputItem] {
    var lines: [String] = []
    switch data.inputHistory {
    case .string(let text):
        lines.append(text)
    case .inputList(let items):
        lines.append(items.map { ItemHelpers.stringifyJSON(.object($0)) }.joined(separator: "\n"))
    }
    let transcript = lines.filter { !$0.isEmpty }.joined(separator: "\n")
    return [[
        "role": .string("assistant"),
        "content": .string(transcript),
    ]]
}

public func nestHandoffHistory(_ data: HandoffInputData) async throws -> [TResponseInputItem] {
    if let mapper = getConversationHistoryWrappers() {
        return try await mapper(data)
    }
    return try await defaultHandoffHistoryMapper(data)
}

public func handoff<TContext>(
    _ agent: Agent<TContext>,
    toolName: String? = nil,
    toolDescription: String? = nil,
    inputJSONSchema: [String: JSONValue]? = nil,
    inputFilter: HandoffInputFilter? = nil,
    nestHandoffHistory: Bool? = nil,
    strictJSONSchema: Bool = true,
    isEnabled: ToolEnabledPredicate = .always(true)
) -> Handoff<TContext> {
    Handoff(
        toolName: toolName ?? Handoff.defaultToolName(agent: agent),
        toolDescription: toolDescription ?? Handoff.defaultToolDescription(agent: agent),
        inputJSONSchema: inputJSONSchema ?? [
            "type": .string("object"),
            "properties": .object(["input": .object(["type": .string("string")])]),
            "required": .array([.string("input")]),
            "additionalProperties": .bool(false),
        ],
        onInvokeHandoff: { _, _ in agent },
        agentName: agent.name,
        inputFilter: inputFilter,
        nestHandoffHistory: nestHandoffHistory,
        strictJSONSchema: strictJSONSchema,
        isEnabled: isEnabled
    )
}

