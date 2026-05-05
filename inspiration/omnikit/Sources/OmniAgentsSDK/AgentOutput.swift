import Foundation
import OmniAICore

private let _outputWrapperKey = "response"

public protocol AgentOutputSchemaBase: Sendable {
    var isPlainText: Bool { get }
    var name: String { get }
    var jsonSchema: [String: JSONValue]? { get }
    var isStrictJSONSchema: Bool { get }
    func validateJSON(_ jsonString: String) throws -> Any
}

public struct AgentOutputSchema<Output: Decodable & Sendable>: AgentOutputSchemaBase {
    public let outputType: Output.Type
    public let isStrictJSONSchema: Bool
    private let wrapped: Bool
    private let outputSchemaValue: [String: JSONValue]?

    public init(_ outputType: Output.Type, strictJSONSchema: Bool = true) throws {
        self.outputType = outputType
        self.isStrictJSONSchema = strictJSONSchema

        if Output.self == String.self {
            self.wrapped = false
            self.outputSchemaValue = nil
            return
        }

        let rawSchema = try FunctionSchema.jsonSchema(for: Output.self, strict: false)
        let shouldWrap = rawSchema["type"]?.stringValue != "object"
        self.wrapped = shouldWrap
        if shouldWrap {
            let schema: [String: JSONValue] = [
                "type": .string("object"),
                "properties": .object([_outputWrapperKey: rawSchema]),
                "required": .array([.string(_outputWrapperKey)]),
            ]
            self.outputSchemaValue = strictJSONSchema ? ensureStrictJSONSchema(schema) : schema
        } else if case .object(let object) = rawSchema {
            self.outputSchemaValue = strictJSONSchema ? ensureStrictJSONSchema(object) : object
        } else {
            throw UserError(message: "Agent output schema must resolve to an object schema.")
        }
    }

    public var isPlainText: Bool {
        Output.self == String.self
    }

    public var name: String {
        String(reflecting: Output.self)
    }

    public var jsonSchema: [String: JSONValue]? {
        outputSchemaValue
    }

    public func validateJSON(_ jsonString: String) throws -> Any {
        if isPlainText {
            return jsonString
        }

        if wrapped {
            let decoded: [String: JSONValue] = try FunctionSchema.decode(jsonString, as: [String: JSONValue].self)
            guard let wrappedValue = decoded[_outputWrapperKey] else {
                attachErrorToCurrentSpan(SpanError(message: "Invalid JSON", data: ["details": .string("Missing wrapped response field")]))
                throw ModelBehaviorError(message: "Expected wrapped response field in JSON output.")
            }
            let wrappedString: String
            if case .string(let stringValue) = wrappedValue {
                wrappedString = "\"\(stringValue)\""
            } else if let data = try? wrappedValue.data(), let string = String(data: data, encoding: .utf8) {
                wrappedString = string
            } else {
                wrappedString = String(describing: wrappedValue)
            }
            return try FunctionSchema.decode(wrappedString, as: Output.self)
        }

        return try FunctionSchema.decode(jsonString, as: Output.self)
    }
}

public enum AnyAgentOutputSchema {
    private static let plainTextSchema: any AgentOutputSchemaBase = {
        do {
            return try AgentOutputSchema(String.self)
        } catch {
            preconditionFailure("Failed to create plain-text agent output schema: \(error)")
        }
    }()

    public static func plainText() -> any AgentOutputSchemaBase {
        plainTextSchema
    }
}
