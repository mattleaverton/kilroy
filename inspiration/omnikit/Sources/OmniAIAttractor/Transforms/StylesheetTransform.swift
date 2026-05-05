import Foundation

// MARK: - Stylesheet Transform

/// Applies the parsed model_stylesheet to graph nodes.
/// Sets llm_model, llm_provider, and reasoning_effort on nodes
/// that don't already have explicit values for those properties.
public struct StylesheetTransform: GraphTransform {
    public init() {}

    public func apply(_ graph: Graph) -> Graph {
        let stylesheetSource = graph.attributes.modelStylesheet
        guard !stylesheetSource.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return graph
        }

        guard let stylesheet = try? StylesheetParser.parse(stylesheetSource) else {
            return graph
        }

        for (_, node) in graph.nodes {
            let classes = node.cssClass
                .split(separator: ",")
                .map { $0.trimmingCharacters(in: .whitespaces) }
                .filter { !$0.isEmpty }

            let resolved = stylesheet.resolve(nodeId: node.id, nodeShape: node.shape, nodeClasses: classes)

            // Only apply if the node doesn't already have an explicit value
            if let model = resolved.llmModel, node.llmModel.isEmpty {
                node.llmModel = model
            }
            if let provider = resolved.llmProvider, node.llmProvider.isEmpty {
                node.llmProvider = provider
            }
            if let effort = resolved.reasoningEffort, node.reasoningEffort == "high" {
                // Only override if node still has the default value
                // Check raw attributes to see if it was explicitly set
                if node.rawAttributes["reasoning_effort"] == nil {
                    node.reasoningEffort = effort
                }
            }
        }

        return graph
    }
}

