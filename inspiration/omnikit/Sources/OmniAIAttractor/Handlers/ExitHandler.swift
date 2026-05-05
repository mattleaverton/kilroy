import Foundation

// MARK: - Exit Handler

public final class ExitHandler: NodeHandler, Sendable {
    public let handlerType: HandlerType = .exit

    public init() {}

    public func execute(
        node: Node,
        context: PipelineContext,
        graph: Graph,
        logsRoot: URL
    ) async throws -> Outcome {
        .success(notes: "Pipeline exit reached")
    }
}
