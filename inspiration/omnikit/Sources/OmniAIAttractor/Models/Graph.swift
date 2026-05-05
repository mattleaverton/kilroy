import Foundation

// MARK: - Graph

// Safety: @unchecked Sendable because Graph is mutated only during the
// parsing/transform phase (before pipeline execution begins). Once
// PipelineEngine.executeLoop starts, the graph is treated as read-only.
// ParallelHandler branches receive the same Graph reference but only read it.
// TODO: Convert to a value type or add locking if mutation during execution
// is ever needed.
public final class Graph: @unchecked Sendable {
    public var id: String
    public var nodes: [String: Node]
    public var edges: [Edge]
    public var attributes: GraphAttributes
    public var rawAttributes: [String: AttributeValue]

    public init(
        id: String = "pipeline",
        nodes: [String: Node] = [:],
        edges: [Edge] = [],
        attributes: GraphAttributes = GraphAttributes(),
        rawAttributes: [String: AttributeValue] = [:]
    ) {
        self.id = id
        self.nodes = nodes
        self.edges = edges
        self.attributes = attributes
        self.rawAttributes = rawAttributes
    }

    public var goal: String { attributes.goal }
    public var label: String { attributes.label }

    public func node(_ id: String) -> Node? { nodes[id] }

    public func outgoingEdges(from nodeId: String) -> [Edge] {
        edges.filter { $0.from == nodeId }
    }

    public func incomingEdges(to nodeId: String) -> [Edge] {
        edges.filter { $0.to == nodeId }
    }

    public var startNode: Node? {
        nodes.values.first { $0.handlerType == .start }
    }

    public var exitNodes: [Node] {
        nodes.values.filter { $0.handlerType == .exit }
    }

    public var goalGateNodes: [Node] {
        nodes.values.filter { $0.goalGate }
    }
}

// MARK: - Graph Attributes

public struct GraphAttributes: Sendable {
    public var goal: String
    public var label: String
    public var modelStylesheet: String
    public var defaultMaxRetry: Int
    public var retryTarget: String
    public var fallbackRetryTarget: String
    public var defaultFidelity: String
    public var stackChildDotfile: String
    public var stackChildWorkdir: String
    public var toolHooksPre: String
    public var toolHooksPost: String

    public init(
        goal: String = "",
        label: String = "",
        modelStylesheet: String = "",
        defaultMaxRetry: Int = 50,
        retryTarget: String = "",
        fallbackRetryTarget: String = "",
        defaultFidelity: String = "",
        stackChildDotfile: String = "",
        stackChildWorkdir: String = "",
        toolHooksPre: String = "",
        toolHooksPost: String = ""
    ) {
        self.goal = goal
        self.label = label
        self.modelStylesheet = modelStylesheet
        self.defaultMaxRetry = defaultMaxRetry
        self.retryTarget = retryTarget
        self.fallbackRetryTarget = fallbackRetryTarget
        self.defaultFidelity = defaultFidelity
        self.stackChildDotfile = stackChildDotfile
        self.stackChildWorkdir = stackChildWorkdir
        self.toolHooksPre = toolHooksPre
        self.toolHooksPost = toolHooksPost
    }
}

// MARK: - Node

// Safety: Same invariant as Graph — nodes are mutated only during
// parsing/transforms (e.g. StylesheetTransform, VariableExpansionTransform)
// before execution. During execution they are read-only.
// TODO: Convert to a value type or add locking if mutation during execution
// is ever needed.
public final class Node: @unchecked Sendable {
    public var id: String
    public var label: String
    public var shape: String
    public var type: String
    public var prompt: String
    public var maxRetries: Int
    public var goalGate: Bool
    public var retryTarget: String
    public var fallbackRetryTarget: String
    public var fidelity: String
    public var threadId: String
    public var cssClass: String
    public var timeout: Duration?
    public var llmModel: String
    public var llmProvider: String
    public var reasoningEffort: String
    public var autoStatus: Bool
    public var allowPartial: Bool
    public var rawAttributes: [String: AttributeValue]

    public init(
        id: String,
        label: String? = nil,
        shape: String = "box",
        type: String = "",
        prompt: String = "",
        maxRetries: Int = 0,
        goalGate: Bool = false,
        retryTarget: String = "",
        fallbackRetryTarget: String = "",
        fidelity: String = "",
        threadId: String = "",
        cssClass: String = "",
        timeout: Duration? = nil,
        llmModel: String = "",
        llmProvider: String = "",
        reasoningEffort: String = "high",
        autoStatus: Bool = false,
        allowPartial: Bool = false,
        rawAttributes: [String: AttributeValue] = [:]
    ) {
        self.id = id
        self.label = label ?? id
        self.shape = shape
        self.type = type
        self.prompt = prompt
        self.maxRetries = maxRetries
        self.goalGate = goalGate
        self.retryTarget = retryTarget
        self.fallbackRetryTarget = fallbackRetryTarget
        self.fidelity = fidelity
        self.threadId = threadId
        self.cssClass = cssClass
        self.timeout = timeout
        self.llmModel = llmModel
        self.llmProvider = llmProvider
        self.reasoningEffort = reasoningEffort
        self.autoStatus = autoStatus
        self.allowPartial = allowPartial
        self.rawAttributes = rawAttributes
    }

    public var handlerType: HandlerType {
        if !type.isEmpty, let ht = HandlerType(rawValue: type) {
            return ht
        }
        return HandlerType.fromShape(shape)
    }
}

// MARK: - Edge

public struct Edge: Sendable {
    public var from: String
    public var to: String
    public var label: String
    public var condition: String
    public var weight: Int
    public var fidelity: String
    public var threadId: String
    public var loopRestart: Bool
    public var rawAttributes: [String: AttributeValue]

    public init(
        from: String,
        to: String,
        label: String = "",
        condition: String = "",
        weight: Int = 0,
        fidelity: String = "",
        threadId: String = "",
        loopRestart: Bool = false,
        rawAttributes: [String: AttributeValue] = [:]
    ) {
        self.from = from
        self.to = to
        self.label = label
        self.condition = condition
        self.weight = weight
        self.fidelity = fidelity
        self.threadId = threadId
        self.loopRestart = loopRestart
        self.rawAttributes = rawAttributes
    }
}

// MARK: - Handler Type

public enum HandlerType: String, Sendable {
    case start
    case exit
    case codergen
    case waitHuman = "wait.human"
    case conditional
    case parallel
    case parallelFanIn = "parallel.fan_in"
    case tool
    case stackManagerLoop = "stack.manager_loop"

    public static func fromShape(_ shape: String) -> HandlerType {
        switch shape.lowercased() {
        case "mdiamond": return .start
        case "msquare": return .exit
        case "box": return .codergen
        case "hexagon": return .waitHuman
        case "diamond": return .conditional
        case "component": return .parallel
        case "tripleoctagon": return .parallelFanIn
        case "parallelogram": return .tool
        case "house": return .stackManagerLoop
        default: return .codergen
        }
    }
}

// MARK: - Attribute Value

public enum AttributeValue: Sendable, Equatable {
    case string(String)
    case integer(Int)
    case float(Double)
    case boolean(Bool)
    case duration(DurationValue)

    public var stringValue: String {
        switch self {
        case .string(let s): return s
        case .integer(let i): return String(i)
        case .float(let f): return String(f)
        case .boolean(let b): return b ? "true" : "false"
        case .duration(let d): return d.description
        }
    }

    public var intValue: Int? {
        switch self {
        case .integer(let i): return i
        case .string(let s): return Int(s)
        default: return nil
        }
    }

    public var boolValue: Bool? {
        switch self {
        case .boolean(let b): return b
        case .string(let s): return s == "true"
        default: return nil
        }
    }
}

// MARK: - Duration Value

public struct DurationValue: Sendable, Equatable, CustomStringConvertible {
    public var milliseconds: Int64

    public init(milliseconds: Int64) { self.milliseconds = milliseconds }

    public var seconds: Double { Double(milliseconds) / 1000.0 }

    public var description: String {
        if milliseconds < 1000 { return "\(milliseconds)ms" }
        let secs = milliseconds / 1000
        if secs < 60 { return "\(secs)s" }
        let mins = secs / 60
        if mins < 60 { return "\(mins)m" }
        let hours = mins / 60
        if hours < 24 { return "\(hours)h" }
        return "\(hours / 24)d"
    }

    public static func parse(_ s: String) -> DurationValue? {
        let trimmed = s.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty else { return nil }

        if trimmed.hasSuffix("ms") {
            let numStr = String(trimmed.dropLast(2))
            guard let val = Int64(numStr) else { return nil }
            return DurationValue(milliseconds: val)
        }
        guard let unit = trimmed.last else { return nil }
        let numStr = String(trimmed.dropLast())
        guard let val = Int64(numStr) else { return nil }
        switch unit {
        case "s": return DurationValue(milliseconds: val * 1000)
        case "m": return DurationValue(milliseconds: val * 60 * 1000)
        case "h": return DurationValue(milliseconds: val * 3600 * 1000)
        case "d": return DurationValue(milliseconds: val * 86400 * 1000)
        default: return nil
        }
    }

    public var asDuration: Duration {
        .milliseconds(milliseconds)
    }
}
