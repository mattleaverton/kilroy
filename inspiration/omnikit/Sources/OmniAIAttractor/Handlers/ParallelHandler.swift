import Foundation

// MARK: - Parallel Handler

public final class ParallelHandler: NodeHandler, Sendable {
    public let handlerType: HandlerType = .parallel
    private let registry: HandlerRegistry

    public init(registry: HandlerRegistry) {
        self.registry = registry
    }

    public func execute(
        node: Node,
        context: PipelineContext,
        graph: Graph,
        logsRoot: URL
    ) async throws -> Outcome {
        let outgoing = graph.outgoingEdges(from: node.id)
        let branchEdges = outgoing.filter { edge in
            guard let target = graph.node(edge.to) else { return true }
            return target.handlerType != .parallelFanIn
        }

        if branchEdges.isEmpty {
            return .success(notes: "Parallel node with no branches")
        }

        // Run all branches concurrently
        let results: [(String, Outcome)] = try await withThrowingTaskGroup(
            of: (String, Outcome).self
        ) { group in
            for edge in branchEdges {
                let targetId = edge.to
                guard let targetNode = graph.node(targetId) else { continue }
                guard let handler = registry.resolve(targetNode.handlerType) else {
                    throw AttractorError.handlerNotFound(targetNode.handlerType.rawValue)
                }

                // Clone context for each branch
                let branchContext = context.clone()
                branchContext.set("current_node", targetId)

                group.addTask {
                    let outcome = try await handler.execute(
                        node: targetNode,
                        context: branchContext,
                        graph: graph,
                        logsRoot: logsRoot
                    )
                    return (targetId, outcome)
                }
            }

            var collected: [(String, Outcome)] = []
            for try await result in group {
                collected.append(result)
            }
            return collected
        }

        let fanInTarget = resolveCommonFanInTarget(
            branchTargetIds: branchEdges.map(\.to),
            graph: graph
        )

        // Store results in context
        var branchResults: [String: String] = [:]
        for (nodeId, outcome) in results {
            branchResults[nodeId] = outcome.status.rawValue
        }
        context.set("parallel.results", branchResults)

        // Merge context updates from all branches
        var mergedUpdates: [String: String] = [:]
        for (nodeId, outcome) in results {
            for (key, value) in outcome.contextUpdates {
                // Preserve the raw key for compatibility (last writer wins),
                // and also retain a branch-qualified key to avoid collisions.
                mergedUpdates[key] = value
                mergedUpdates["parallel.\(node.id).\(nodeId).\(key)"] = value
            }
        }

        let allSucceeded = results.allSatisfy { $0.1.status == .success || $0.1.status == .partialSuccess }
        let status: OutcomeStatus = allSucceeded ? .success : .partialSuccess

        var notes = "Parallel: \(results.count) branches completed"
        if let fanInTarget {
            notes += ", fan-in=\(fanInTarget)"
        }

        return Outcome(
            status: status,
            suggestedNextIds: fanInTarget.map { [$0] } ?? [],
            contextUpdates: mergedUpdates,
            notes: notes
        )
    }

    private func resolveCommonFanInTarget(
        branchTargetIds: [String],
        graph: Graph
    ) -> String? {
        guard !branchTargetIds.isEmpty else { return nil }

        var intersection: Set<String>?
        for targetId in branchTargetIds {
            let fanInTargets: Set<String> = Set(
                graph.outgoingEdges(from: targetId).compactMap { edge in
                    guard let node = graph.node(edge.to), node.handlerType == .parallelFanIn else {
                        return nil
                    }
                    return edge.to
                }
            )

            if let existing = intersection {
                intersection = existing.intersection(fanInTargets)
            } else {
                intersection = fanInTargets
            }
        }

        guard let candidates = intersection, !candidates.isEmpty else {
            return nil
        }
        return candidates.sorted().first
    }
}
