import Foundation
import OmniAICore

public struct AgentToolDefinition: Sendable {
    public var name: String
    public var description: String
    public var parameters: JSONValue

    public init(name: String, description: String, parameters: [String: Any]) {
        self.name = name
        self.description = description
        do {
            self.parameters = try JSONValue(parameters)
        } catch {
            preconditionFailure("Invalid JSON schema for tool '\(name)': \(error)")
        }
    }

    public init(name: String, description: String, parameters: JSONValue) {
        self.name = name
        self.description = description
        self.parameters = parameters
    }

    public func toLLMKitDefinition() -> Tool {
        do {
            return try Tool(name: name, description: description, parameters: parameters)
        } catch {
            preconditionFailure("Invalid tool definition '\(name)': \(error)")
        }
    }
}

public typealias ToolExecutor = @Sendable ([String: Any], ExecutionEnvironment) async throws -> String
public typealias StreamingToolOutputEmitter = @Sendable (String) async -> Void
public typealias StreamingToolExecutor = @Sendable ([String: Any], ExecutionEnvironment, StreamingToolOutputEmitter) async throws -> String

public struct RegisteredTool: Sendable {
    public var definition: AgentToolDefinition
    public var executor: ToolExecutor
    public var streamingExecutor: StreamingToolExecutor?

    public init(
        definition: AgentToolDefinition,
        executor: @escaping ToolExecutor,
        streamingExecutor: StreamingToolExecutor? = nil
    ) {
        self.definition = definition
        self.executor = executor
        self.streamingExecutor = streamingExecutor
    }
}

// Safety: @unchecked Sendable — all mutable state (tools) is guarded by `lock`.
// The lock is never held across suspension points.
public final class ToolRegistry: @unchecked Sendable {
    private var tools: [String: RegisteredTool] = [:]
    private let lock = NSLock()

    public init() {}

    public func register(_ tool: RegisteredTool) {
        lock.lock()
        defer { lock.unlock() }
        tools[tool.definition.name] = tool
    }

    public func unregister(_ name: String) {
        lock.lock()
        defer { lock.unlock() }
        tools.removeValue(forKey: name)
    }

    public func get(_ name: String) -> RegisteredTool? {
        lock.lock()
        defer { lock.unlock() }
        return tools[name]
    }

    public func definitions() -> [AgentToolDefinition] {
        lock.lock()
        defer { lock.unlock() }
        return Array(tools.values.map { $0.definition })
    }

    public func llmKitDefinitions() -> [Tool] {
        definitions().map { $0.toLLMKitDefinition() }
    }

    public func names() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return Array(tools.keys)
    }
}
