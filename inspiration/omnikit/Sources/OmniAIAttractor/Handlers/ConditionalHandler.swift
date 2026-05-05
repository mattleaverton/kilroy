import Foundation

// MARK: - Conditional Handler

/// Pass-through handler for conditional (diamond) nodes.
/// The engine evaluates edge conditions for routing, not this handler.
public final class ConditionalHandler: NodeHandler, Sendable {
    public let handlerType: HandlerType = .conditional

    public init() {}

    public func execute(
        node: Node,
        context: PipelineContext,
        graph: Graph,
        logsRoot: URL
    ) async throws -> Outcome {
        .success(notes: "Conditional node evaluated")
    }
}
