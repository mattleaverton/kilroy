public enum MaybeAwaitable<Value>: @unchecked Sendable {
    case value(Value)
    case operation(@Sendable () async throws -> Value)

    public init(_ value: Value) {
        self = .value(value)
    }

    public init(operation: @escaping @Sendable () async throws -> Value) {
        self = .operation(operation)
    }

    public func resolve() async throws -> Value {
        switch self {
        case .value(let value):
            return value
        case .operation(let operation):
            return try await operation()
        }
    }
}