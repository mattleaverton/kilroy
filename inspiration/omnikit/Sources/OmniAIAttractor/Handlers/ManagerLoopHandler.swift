import Foundation

// MARK: - Manager Loop Handler

/// Handler for stack.manager_loop nodes.
/// Loads a child pipeline DOT from the graph's stackChildDotfile attribute,
/// creates a child PipelineEngine, and runs the child pipeline with retry.
public final class ManagerLoopHandler: NodeHandler, Sendable {
    public let handlerType: HandlerType = .stackManagerLoop
    private let backend: CodergenBackend

    public init(backend: CodergenBackend) {
        self.backend = backend
    }

    public func execute(
        node: Node,
        context: PipelineContext,
        graph: Graph,
        logsRoot: URL
    ) async throws -> Outcome {
        // 1. Read child dotfile path from graph attributes
        let dotfilePath = graph.attributes.stackChildDotfile
        guard !dotfilePath.isEmpty else {
            return .fail(reason: "Manager loop node \(node.id): no stack.child_dotfile specified")
        }

        // 2. Read optional child working directory
        let childWorkdir = graph.attributes.stackChildWorkdir

        // 3. Read manager config from node raw attributes
        let maxCycles = node.rawAttributes["manager.max_cycles"]?.intValue ?? 10
        let pollInterval = node.rawAttributes["manager.poll_interval"]?.intValue ?? 5

        // 4. Load the child DOT string from the file
        let resolvedPath: String
        if dotfilePath.hasPrefix("/") {
            resolvedPath = dotfilePath
        } else if !childWorkdir.isEmpty {
            resolvedPath = (childWorkdir as NSString).appendingPathComponent(dotfilePath)
        } else {
            resolvedPath = dotfilePath
        }

        let dotURL = URL(fileURLWithPath: resolvedPath)
        let childDOT: String
        do {
            childDOT = try String(contentsOf: dotURL, encoding: .utf8)
        } catch {
            return .fail(reason: "Manager loop node \(node.id): failed to read child DOT from \(resolvedPath): \(error)")
        }

        // 5. Run child pipeline with retry up to maxCycles
        var lastResult: PipelineResult?
        for cycle in 0..<maxCycles {
            let childLogsRoot = logsRoot
                .appendingPathComponent(node.id)
                .appendingPathComponent("cycle_\(cycle)")

            let childConfig = PipelineConfig(
                logsRoot: childLogsRoot,
                backend: backend
            )
            let childEngine = PipelineEngine(config: childConfig)

            do {
                let result = try await childEngine.run(dot: childDOT)
                lastResult = result

                if result.status == .success || result.status == .partialSuccess {
                    // Child pipeline succeeded
                    var updates: [String: String] = [:]
                    updates["manager.cycle_count"] = String(cycle + 1)
                    updates["manager.child_status"] = result.status.rawValue
                    for (k, v) in result.context {
                        updates["child.\(k)"] = v
                    }
                    return Outcome(
                        status: result.status == .success ? .success : .partialSuccess,
                        contextUpdates: updates,
                        notes: "Manager loop completed after \(cycle + 1) cycle(s)"
                    )
                }

                // Child failed, will retry if cycles remain
                if cycle < maxCycles - 1 {
                    // Wait before retry
                    if pollInterval > 0 {
                        try await Task.sleep(for: .seconds(pollInterval))
                    }
                }
            } catch {
                lastResult = nil
                if cycle < maxCycles - 1 {
                    if pollInterval > 0 {
                        try await Task.sleep(for: .seconds(pollInterval))
                    }
                    continue
                }
                return .fail(reason: "Manager loop node \(node.id): child pipeline threw after \(cycle + 1) cycle(s): \(error)")
            }
        }

        // All cycles exhausted
        let childStatus = lastResult?.status.rawValue ?? "unknown"
        return .fail(reason: "Manager loop node \(node.id): child pipeline failed after \(maxCycles) cycle(s), last status: \(childStatus)")
    }
}
