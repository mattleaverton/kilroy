import Foundation

// MARK: - Pipeline Validator

public struct PipelineValidator {

    /// Validate a graph and return all diagnostics.
    public static func validate(_ graph: Graph) -> [Diagnostic] {
        var diagnostics: [Diagnostic] = []

        diagnostics.append(contentsOf: validateStartNode(graph))
        diagnostics.append(contentsOf: validateTerminalNode(graph))
        diagnostics.append(contentsOf: validateEdgeTargetExists(graph))
        diagnostics.append(contentsOf: validateStartNoIncoming(graph))
        diagnostics.append(contentsOf: validateExitNoOutgoing(graph))
        diagnostics.append(contentsOf: validateReachability(graph))
        diagnostics.append(contentsOf: validateConditionSyntax(graph))
        diagnostics.append(contentsOf: validateStylesheetSyntax(graph))
        diagnostics.append(contentsOf: validateTypeKnown(graph))
        diagnostics.append(contentsOf: validateFidelityValid(graph))
        diagnostics.append(contentsOf: validateRetryTargetExists(graph))
        diagnostics.append(contentsOf: validateGoalGateHasRetry(graph))
        diagnostics.append(contentsOf: validatePromptOnLLMNodes(graph))

        return diagnostics
    }

    /// Validate a graph and throw if any error-severity diagnostics are found.
    public static func validateOrRaise(_ graph: Graph) throws {
        let diagnostics = validate(graph)
        let errors = diagnostics.filter { $0.isError }
        if !errors.isEmpty {
            throw AttractorError.validationFailed(diagnostics)
        }
    }

    // MARK: - Rule: start_node (ERROR)

    private static func validateStartNode(_ graph: Graph) -> [Diagnostic] {
        let startNodes = graph.nodes.values.filter { $0.handlerType == .start }
        if startNodes.isEmpty {
            return [Diagnostic(
                rule: "start_node",
                severity: .error,
                message: "Pipeline must have exactly one start node (shape=Mdiamond)",
                fix: "Add a node with shape=Mdiamond"
            )]
        }
        if startNodes.count > 1 {
            return [Diagnostic(
                rule: "start_node",
                severity: .error,
                message: "Pipeline has \(startNodes.count) start nodes; exactly one is required",
                fix: "Remove extra start nodes so only one remains"
            )]
        }
        return []
    }

    // MARK: - Rule: terminal_node (ERROR)

    private static func validateTerminalNode(_ graph: Graph) -> [Diagnostic] {
        let exitNodes = graph.exitNodes
        if exitNodes.isEmpty {
            return [Diagnostic(
                rule: "terminal_node",
                severity: .error,
                message: "Pipeline must have at least one exit node (shape=Msquare)",
                fix: "Add a node with shape=Msquare"
            )]
        }
        return []
    }

    // MARK: - Rule: edge_target_exists (ERROR)

    private static func validateEdgeTargetExists(_ graph: Graph) -> [Diagnostic] {
        var diagnostics: [Diagnostic] = []
        for edge in graph.edges {
            if graph.node(edge.from) == nil {
                diagnostics.append(Diagnostic(
                    rule: "edge_target_exists",
                    severity: .error,
                    message: "Edge source '\(edge.from)' does not reference an existing node",
                    edge: (edge.from, edge.to),
                    fix: "Add a node with id=\(edge.from) or fix the edge"
                ))
            }
            if graph.node(edge.to) == nil {
                diagnostics.append(Diagnostic(
                    rule: "edge_target_exists",
                    severity: .error,
                    message: "Edge target '\(edge.to)' does not reference an existing node",
                    edge: (edge.from, edge.to),
                    fix: "Add a node with id=\(edge.to) or fix the edge"
                ))
            }
        }
        return diagnostics
    }

    // MARK: - Rule: start_no_incoming (ERROR)

    private static func validateStartNoIncoming(_ graph: Graph) -> [Diagnostic] {
        guard let startNode = graph.startNode else { return [] }
        let incoming = graph.incomingEdges(to: startNode.id)
        if !incoming.isEmpty {
            return [Diagnostic(
                rule: "start_no_incoming",
                severity: .error,
                message: "Start node '\(startNode.id)' has \(incoming.count) incoming edge(s)",
                nodeId: startNode.id,
                fix: "Remove incoming edges to the start node"
            )]
        }
        return []
    }

    // MARK: - Rule: exit_no_outgoing (ERROR)

    private static func validateExitNoOutgoing(_ graph: Graph) -> [Diagnostic] {
        var diagnostics: [Diagnostic] = []
        for exitNode in graph.exitNodes {
            let outgoing = graph.outgoingEdges(from: exitNode.id)
            if !outgoing.isEmpty {
                diagnostics.append(Diagnostic(
                    rule: "exit_no_outgoing",
                    severity: .error,
                    message: "Exit node '\(exitNode.id)' has \(outgoing.count) outgoing edge(s)",
                    nodeId: exitNode.id,
                    fix: "Remove outgoing edges from the exit node"
                ))
            }
        }
        return diagnostics
    }

    // MARK: - Rule: reachability (ERROR)

    private static func validateReachability(_ graph: Graph) -> [Diagnostic] {
        guard let startNode = graph.startNode else { return [] }

        var visited = Set<String>()
        var queue = [startNode.id]
        visited.insert(startNode.id)

        while !queue.isEmpty {
            let current = queue.removeFirst()
            for edge in graph.outgoingEdges(from: current) {
                if !visited.contains(edge.to) {
                    visited.insert(edge.to)
                    queue.append(edge.to)
                }
            }
        }

        var diagnostics: [Diagnostic] = []
        for (nodeId, _) in graph.nodes {
            if !visited.contains(nodeId) {
                diagnostics.append(Diagnostic(
                    rule: "reachability",
                    severity: .error,
                    message: "Node '\(nodeId)' is not reachable from the start node",
                    nodeId: nodeId,
                    fix: "Add an edge path from start to '\(nodeId)' or remove the node"
                ))
            }
        }
        return diagnostics
    }

    // MARK: - Rule: condition_syntax (ERROR)

    private static func validateConditionSyntax(_ graph: Graph) -> [Diagnostic] {
        var diagnostics: [Diagnostic] = []
        for edge in graph.edges {
            let condition = edge.condition.trimmingCharacters(in: .whitespaces)
            if condition.isEmpty { continue }
            do {
                _ = try ConditionParser.parse(condition)
            } catch {
                diagnostics.append(Diagnostic(
                    rule: "condition_syntax",
                    severity: .error,
                    message: "Edge \(edge.from)->\(edge.to) has invalid condition '\(condition)': \(error)",
                    edge: (edge.from, edge.to),
                    fix: "Fix the condition expression syntax"
                ))
            }
        }
        return diagnostics
    }

    // MARK: - Rule: stylesheet_syntax (ERROR)

    private static func validateStylesheetSyntax(_ graph: Graph) -> [Diagnostic] {
        let stylesheet = graph.attributes.modelStylesheet
        if stylesheet.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return []
        }
        do {
            _ = try StylesheetParser.parse(stylesheet)
        } catch {
            return [Diagnostic(
                rule: "stylesheet_syntax",
                severity: .error,
                message: "Invalid model_stylesheet: \(error)",
                fix: "Fix the stylesheet syntax"
            )]
        }
        return []
    }

    // MARK: - Rule: type_known (WARNING)

    private static func validateTypeKnown(_ graph: Graph) -> [Diagnostic] {
        var diagnostics: [Diagnostic] = []
        let knownTypes = Set(HandlerType.allCases.map(\.rawValue))

        for (_, node) in graph.nodes {
            if !node.type.isEmpty && !knownTypes.contains(node.type) {
                diagnostics.append(Diagnostic(
                    rule: "type_known",
                    severity: .warning,
                    message: "Node '\(node.id)' has unrecognized type '\(node.type)'",
                    nodeId: node.id,
                    fix: "Use a recognized handler type or register a custom handler"
                ))
            }
        }
        return diagnostics
    }

    // MARK: - Rule: fidelity_valid (WARNING)

    private static func validateFidelityValid(_ graph: Graph) -> [Diagnostic] {
        var diagnostics: [Diagnostic] = []

        // Check node fidelity values
        for (_, node) in graph.nodes {
            if !node.fidelity.isEmpty && ContextFidelity.parse(node.fidelity) == nil {
                diagnostics.append(Diagnostic(
                    rule: "fidelity_valid",
                    severity: .warning,
                    message: "Node '\(node.id)' has invalid fidelity mode '\(node.fidelity)'",
                    nodeId: node.id,
                    fix: "Use one of: full, truncate, compact, summary:low, summary:medium, summary:high"
                ))
            }
        }

        // Check edge fidelity values
        for edge in graph.edges {
            if !edge.fidelity.isEmpty && ContextFidelity.parse(edge.fidelity) == nil {
                diagnostics.append(Diagnostic(
                    rule: "fidelity_valid",
                    severity: .warning,
                    message: "Edge \(edge.from)->\(edge.to) has invalid fidelity mode '\(edge.fidelity)'",
                    edge: (edge.from, edge.to),
                    fix: "Use one of: full, truncate, compact, summary:low, summary:medium, summary:high"
                ))
            }
        }

        // Check graph default fidelity
        let graphFidelity = graph.attributes.defaultFidelity
        if !graphFidelity.isEmpty && ContextFidelity.parse(graphFidelity) == nil {
            diagnostics.append(Diagnostic(
                rule: "fidelity_valid",
                severity: .warning,
                message: "Graph default_fidelity '\(graphFidelity)' is not a valid fidelity mode",
                fix: "Use one of: full, truncate, compact, summary:low, summary:medium, summary:high"
            ))
        }

        return diagnostics
    }

    // MARK: - Rule: retry_target_exists (WARNING)

    private static func validateRetryTargetExists(_ graph: Graph) -> [Diagnostic] {
        var diagnostics: [Diagnostic] = []

        for (_, node) in graph.nodes {
            if !node.retryTarget.isEmpty && graph.node(node.retryTarget) == nil {
                diagnostics.append(Diagnostic(
                    rule: "retry_target_exists",
                    severity: .warning,
                    message: "Node '\(node.id)' retry_target '\(node.retryTarget)' does not exist",
                    nodeId: node.id,
                    fix: "Set retry_target to an existing node ID"
                ))
            }
            if !node.fallbackRetryTarget.isEmpty && graph.node(node.fallbackRetryTarget) == nil {
                diagnostics.append(Diagnostic(
                    rule: "retry_target_exists",
                    severity: .warning,
                    message: "Node '\(node.id)' fallback_retry_target '\(node.fallbackRetryTarget)' does not exist",
                    nodeId: node.id,
                    fix: "Set fallback_retry_target to an existing node ID"
                ))
            }
        }

        // Check graph-level retry targets
        if !graph.attributes.retryTarget.isEmpty && graph.node(graph.attributes.retryTarget) == nil {
            diagnostics.append(Diagnostic(
                rule: "retry_target_exists",
                severity: .warning,
                message: "Graph retry_target '\(graph.attributes.retryTarget)' does not exist",
                fix: "Set retry_target to an existing node ID"
            ))
        }
        if !graph.attributes.fallbackRetryTarget.isEmpty && graph.node(graph.attributes.fallbackRetryTarget) == nil {
            diagnostics.append(Diagnostic(
                rule: "retry_target_exists",
                severity: .warning,
                message: "Graph fallback_retry_target '\(graph.attributes.fallbackRetryTarget)' does not exist",
                fix: "Set fallback_retry_target to an existing node ID"
            ))
        }

        return diagnostics
    }

    // MARK: - Rule: goal_gate_has_retry (WARNING)

    private static func validateGoalGateHasRetry(_ graph: Graph) -> [Diagnostic] {
        var diagnostics: [Diagnostic] = []
        for node in graph.goalGateNodes {
            let hasRetry = !node.retryTarget.isEmpty || !node.fallbackRetryTarget.isEmpty
            let graphHasRetry = !graph.attributes.retryTarget.isEmpty || !graph.attributes.fallbackRetryTarget.isEmpty
            if !hasRetry && !graphHasRetry {
                diagnostics.append(Diagnostic(
                    rule: "goal_gate_has_retry",
                    severity: .warning,
                    message: "Goal gate node '\(node.id)' has no retry_target or fallback_retry_target",
                    nodeId: node.id,
                    fix: "Add retry_target or fallback_retry_target to the node or graph"
                ))
            }
        }
        return diagnostics
    }

    // MARK: - Rule: prompt_on_llm_nodes (WARNING)

    private static func validatePromptOnLLMNodes(_ graph: Graph) -> [Diagnostic] {
        var diagnostics: [Diagnostic] = []
        for (_, node) in graph.nodes {
            if node.handlerType == .codergen {
                let hasPrompt = !node.prompt.isEmpty
                let hasLabel = !node.label.isEmpty && node.label != node.id
                if !hasPrompt && !hasLabel {
                    diagnostics.append(Diagnostic(
                        rule: "prompt_on_llm_nodes",
                        severity: .warning,
                        message: "LLM node '\(node.id)' has no prompt or meaningful label",
                        nodeId: node.id,
                        fix: "Add a prompt or label attribute to the node"
                    ))
                }
            }
        }
        return diagnostics
    }
}

// MARK: - HandlerType CaseIterable

extension HandlerType: CaseIterable {
    public static var allCases: [HandlerType] {
        [.start, .exit, .codergen, .waitHuman, .conditional, .parallel, .parallelFanIn, .tool, .stackManagerLoop]
    }
}
