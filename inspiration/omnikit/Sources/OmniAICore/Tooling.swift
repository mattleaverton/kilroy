import Foundation

public struct ToolExecutionContext: Sendable {
    public var messages: [Message]
    public var abortSignal: AbortSignal?
    public var toolCallId: String

    public init(messages: [Message], abortSignal: AbortSignal?, toolCallId: String) {
        self.messages = messages
        self.abortSignal = abortSignal
        self.toolCallId = toolCallId
    }
}

public typealias ToolExecute = @Sendable (_ arguments: [String: JSONValue], _ context: ToolExecutionContext) async throws -> JSONValue
public typealias ToolExecuteHandler = @Sendable (_ arguments: [String: Any]) async throws -> Any

public struct Tool: Sendable {
    public var name: String
    public var description: String
    public var parameters: JSONValue
    public var execute: ToolExecute?

    public init(name: String, description: String, parameters: JSONValue, execute: ToolExecute? = nil) throws {
        try Tool.validateName(name)
        try Tool.validateParametersSchema(parameters)
        self.name = name
        self.description = description
        self.parameters = parameters
        self.execute = execute
    }

    /// Compatibility initializer for `OmniAILLMClient` style tool definitions.
    public init(
        name: String,
        description: String,
        parameters: [String: Any],
        execute: ToolExecuteHandler? = nil
    ) throws {
        let schema: JSONValue
        do {
            schema = try JSONValue(parameters)
        } catch {
            throw InvalidToolCallError(message: "Tool parameters must be valid JSON: \(error)")
        }

        let wrappedExecute: ToolExecute?
        if let legacyExecute = execute {
            wrappedExecute = { arguments, _ in
                let foundationArguments: [String: Any]
                do {
                    foundationArguments = try arguments.mapValues { try $0.asFoundationObject() }
                } catch {
                    throw InvalidToolCallError(message: "Tool arguments could not be converted to Foundation values: \(error)")
                }

                let output = try await legacyExecute(foundationArguments)
                do {
                    return try JSONValue(output)
                } catch {
                    throw InvalidToolCallError(message: "Legacy tool output must be valid JSON: \(error)")
                }
            }
        } else {
            wrappedExecute = nil
        }

        try self.init(name: name, description: description, parameters: schema, execute: wrappedExecute)
    }

    private static func validateName(_ name: String) throws {
        if name.count > 64 {
            throw InvalidToolCallError(message: "Tool name too long (max 64): \(name)")
        }
        if name.wholeMatch(of: #/^[A-Za-z][A-Za-z0-9_]*$/#) == nil {
            throw InvalidToolCallError(message: "Invalid tool name: \(name)")
        }
    }

    private static func validateParametersSchema(_ schema: JSONValue) throws {
        guard let obj = schema.objectValue else {
            throw InvalidToolCallError(message: "Tool parameters must be a JSON object schema")
        }
        guard let type = obj["type"]?.stringValue, type == "object" else {
            throw InvalidToolCallError(message: "Tool parameters schema root type must be 'object'")
        }
    }
}

public enum ToolChoiceMode: String, Sendable, Equatable {
    case auto
    case none
    case required
    case named
}

public struct ToolChoice: Sendable, Equatable {
    public var mode: ToolChoiceMode
    public var toolName: String?

    public init(mode: ToolChoiceMode, toolName: String? = nil) {
        self.mode = mode
        self.toolName = toolName
    }

    public static let auto = ToolChoice(mode: .auto)
    public static let none = ToolChoice(mode: .none)
    public static let required = ToolChoice(mode: .required)
    public static func named(_ name: String) -> ToolChoice {
        ToolChoice(mode: .named, toolName: name)
    }
}
