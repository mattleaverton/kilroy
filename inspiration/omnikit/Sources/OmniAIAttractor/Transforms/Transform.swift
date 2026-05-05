import Foundation

// MARK: - Transform Protocol

public protocol GraphTransform: Sendable {
    func apply(_ graph: Graph) -> Graph
}

// MARK: - Variable Expansion Transform

public struct VariableExpansionTransform: GraphTransform {
    public init() {}

    public func apply(_ graph: Graph) -> Graph {
        func expand(_ text: String, context: [String: String]) -> String {
            var result = text
            for (key, value) in context {
                result = result.replacing("$\(key)", with: value)
                result = result.replacing("${\(key)}", with: value)
            }
            return result
        }

        let snapshot = graph.rawAttributes.reduce(into: [String: String]()) { partial, entry in
            partial[entry.key] = entry.value.stringValue
        }

        for node in graph.nodes.values {
            node.prompt = expand(node.prompt, context: snapshot)
            node.label = expand(node.label, context: snapshot)
            if let cmd = node.rawAttributes["tool_command"] {
                node.rawAttributes["tool_command"] = .string(expand(cmd.stringValue, context: snapshot))
            }
        }

        return graph
    }
}

// MARK: - Prompt Preamble Transform

public struct PromptPreambleTransform: GraphTransform {
    private let context: PipelineContext
    private let priorOutcomes: [String: OutcomeStatus]
    private let goal: String
    private let incomingEdge: Edge?
    private let graphDefaultFidelity: String

    public init(
        context: PipelineContext,
        priorOutcomes: [String: OutcomeStatus],
        goal: String,
        incomingEdge: Edge? = nil,
        graphDefaultFidelity: String = ""
    ) {
        self.context = context
        self.priorOutcomes = priorOutcomes
        self.goal = goal
        self.incomingEdge = incomingEdge
        self.graphDefaultFidelity = graphDefaultFidelity
    }

    public func apply(_ graph: Graph) -> Graph {
        for node in graph.nodes.values where node.handlerType == .codergen {
            _ = ContextFidelity.resolve(
                edgeFidelity: incomingEdge?.fidelity ?? "",
                nodeFidelity: node.fidelity,
                graphDefault: graphDefaultFidelity
            )

            var preamble = ""
            if !goal.isEmpty {
                preamble += "Pipeline goal: \(goal)\n"
            }
            if !priorOutcomes.isEmpty {
                let entries = priorOutcomes.sorted(by: { $0.key < $1.key })
                    .map { "  - \($0.key): \($0.value.rawValue)" }
                    .joined(separator: "\n")
                preamble += "Recent stages:\n\(entries)\n"
            }
            let contextSnapshot = context.serializableSnapshot()
            if !contextSnapshot.isEmpty {
                let entries = contextSnapshot.sorted(by: { $0.key < $1.key })
                    .map { "  \($0.key) = \($0.value)" }
                    .joined(separator: "\n")
                preamble += "Context:\n\(entries)\n"
            }

            if !preamble.isEmpty && !node.prompt.isEmpty {
                node.prompt = preamble + "\n" + node.prompt
            }
        }

        return graph
    }
}

// MARK: - Transform Pipeline

/// Manages and applies a sequence of transforms to a graph.
/// Built-in transforms run first, followed by custom transforms in registration order.
public actor TransformPipeline {
    private var customTransforms: [GraphTransform] = []

    public init() {}

    public func register(_ transform: GraphTransform) {
        customTransforms.append(transform)
    }

    public func apply(to graph: Graph) -> Graph {
        let customs = customTransforms

        var result = graph
        result = VariableExpansionTransform().apply(result)
        result = StylesheetTransform().apply(result)
        for transform in customs {
            result = transform.apply(result)
        }
        return result
    }
}
