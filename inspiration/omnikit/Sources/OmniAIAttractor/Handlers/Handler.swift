import Foundation

// MARK: - Handler Protocol

public protocol NodeHandler: Sendable {
    var handlerType: HandlerType { get }
    func execute(
        node: Node,
        context: PipelineContext,
        graph: Graph,
        logsRoot: URL
    ) async throws -> Outcome
}

// MARK: - Handler Registry

// Safety: @unchecked Sendable — all mutable state (handlers) is guarded by
// `lock`. Registration happens during setup; resolution happens during execution.
public final class HandlerRegistry: @unchecked Sendable {
    private var handlers: [String: NodeHandler] = [:]
    private let lock = NSLock()

    public init() {}

    public func register(_ handler: NodeHandler) {
        lock.lock()
        defer { lock.unlock() }
        handlers[handler.handlerType.rawValue] = handler
    }

    public func register(type: String, handler: NodeHandler) {
        lock.lock()
        defer { lock.unlock() }
        handlers[type] = handler
    }

    public func resolve(_ type: HandlerType) -> NodeHandler? {
        lock.lock()
        defer { lock.unlock() }
        return handlers[type.rawValue]
    }

    public func resolve(_ typeString: String) -> NodeHandler? {
        lock.lock()
        defer { lock.unlock() }
        return handlers[typeString]
    }

    public var registeredTypes: [String] {
        lock.lock()
        defer { lock.unlock() }
        return Array(handlers.keys)
    }
}
