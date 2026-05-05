import Foundation

// MARK: - Fan-In Handler

public final class FanInHandler: NodeHandler, Sendable {
    public let handlerType: HandlerType = .parallelFanIn

    public init() {}

    public func execute(
        node: Node,
        context: PipelineContext,
        graph: Graph,
        logsRoot: URL
    ) async throws -> Outcome {
        // Read parallel results from context
        let parallelResults = context.get("parallel.results")

        var notes = "Fan-in consolidation"
        if let results = parallelResults as? [String: String] {
            let succeeded = results.values.filter { $0 == "success" || $0 == "partial_success" }.count
            let total = results.count
            notes = "Fan-in: \(succeeded)/\(total) branches succeeded"
        }

        // Clean up parallel results from context
        context.remove("parallel.results")

        return .success(notes: notes)
    }
}
