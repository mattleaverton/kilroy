import Foundation

public struct AnyAgent: @unchecked Sendable {
    private let storage: Any
    public let name: String

    public init<TContext>(_ agent: AgentBase<TContext>) {
        self.storage = agent
        self.name = agent.name
    }

    public init<TContext>(_ agent: Agent<TContext>) {
        self.storage = agent
        self.name = agent.name
    }

    public init(erasing value: Any, name: String? = nil) {
        self.storage = value
        self.name = name ?? String(describing: value)
    }

    public func typed<TContext>(as _: TContext.Type = TContext.self) -> Agent<TContext>? {
        storage as? Agent<TContext>
    }

    public var base: Any {
        storage
    }
}

