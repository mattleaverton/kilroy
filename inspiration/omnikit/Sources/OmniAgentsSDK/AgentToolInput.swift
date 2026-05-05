import Foundation
import OmniAICore

public struct AgentAsToolInput: Codable, Sendable, Equatable {
    public var input: String

    public init(input: String) {
        self.input = input
    }
}

public struct StructuredInputSchemaInfo: Sendable, Equatable {
    public var summary: String?
    public var jsonSchema: [String: JSONValue]?

    public init(summary: String? = nil, jsonSchema: [String: JSONValue]? = nil) {
        self.summary = summary
        self.jsonSchema = jsonSchema
    }
}

public struct StructuredToolInputBuilderOptions: Sendable, Equatable {
    public var params: [String: JSONValue]?
    public var summary: String?
    public var jsonSchema: [String: JSONValue]?

    public init(params: [String: JSONValue]? = nil, summary: String? = nil, jsonSchema: [String: JSONValue]? = nil) {
        self.params = params
        self.summary = summary
        self.jsonSchema = jsonSchema
    }
}

public enum StructuredToolInputBuilder {
    public static func buildSchemaInfo(_ options: StructuredToolInputBuilderOptions = .init()) -> StructuredInputSchemaInfo {
        if let jsonSchema = options.jsonSchema {
            return StructuredInputSchemaInfo(summary: options.summary, jsonSchema: ensureStrictJSONSchema(jsonSchema))
        }

        if let params = options.params, !params.isEmpty {
            return StructuredInputSchemaInfo(
                summary: options.summary,
                jsonSchema: ensureStrictJSONSchema([
                    "type": .string("object"),
                    "properties": .object(params),
                    "required": .array(params.keys.sorted().map(JSONValue.string)),
                    "additionalProperties": .bool(false),
                ])
            )
        }

        return StructuredInputSchemaInfo(summary: options.summary, jsonSchema: ensureStrictJSONSchema([
            "type": .string("object"),
            "properties": .object(["input": .object(["type": .string("string")])]),
            "required": .array([.string("input")]),
            "additionalProperties": .bool(false),
        ]))
    }
}

public func buildStructuredInputSchemaInfo(
    options: StructuredToolInputBuilderOptions = .init()
) -> StructuredInputSchemaInfo {
    StructuredToolInputBuilder.buildSchemaInfo(options)
}

public func resolveAgentToolInput(
    rawArguments: String,
    schemaInfo: StructuredInputSchemaInfo? = nil
) throws -> String {
    guard !rawArguments.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
        return ""
    }

    guard let data = rawArguments.data(using: .utf8),
          let jsonValue = try? JSONValue.parse(data)
    else {
        return rawArguments
    }

    if case .object(let object) = jsonValue, let input = object["input"]?.stringValue {
        return input
    }

    if let schema = schemaInfo?.jsonSchema {
        try JSONSchema(.object(schema)).validate(jsonValue)
    }

    return ItemHelpers.stringifyJSON(jsonValue)
}

