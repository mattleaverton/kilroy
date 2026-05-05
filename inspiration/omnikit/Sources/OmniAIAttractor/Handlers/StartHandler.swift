import Foundation

// MARK: - Start Handler

public final class StartHandler: NodeHandler, Sendable {
    public let handlerType: HandlerType = .start

    public init() {}

    public func execute(
        node: Node,
        context: PipelineContext,
        graph: Graph,
        logsRoot: URL
    ) async throws -> Outcome {
        .success(notes: "Pipeline started")
    }
}
