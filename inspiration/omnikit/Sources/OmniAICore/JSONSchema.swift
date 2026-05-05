import Foundation

public enum JSONSchemaValidationError: Error, Sendable, Equatable {
    case invalidSchema(String)
    case validationFailed(String)
}

public struct JSONSchema: Sendable, Equatable {
    public var root: JSONValue

    public init(_ root: JSONValue) {
        self.root = root
    }

    public func validate(_ value: JSONValue) throws {
        try validate(schema: root, value: value, path: "$")
    }

    private func validate(schema: JSONValue, value: JSONValue, path: String) throws {
        guard let obj = schema.objectValue else {
            throw JSONSchemaValidationError.invalidSchema("Schema at \(path) must be an object")
        }

        if let enumValues = obj["enum"]?.arrayValue {
            if !enumValues.contains(value) {
                throw JSONSchemaValidationError.validationFailed("Value at \(path) not in enum")
            }
        }

        guard let type = obj["type"]?.stringValue else {
            // If type is omitted, accept anything (best-effort).
            return
        }

        switch type {
        case "object":
            guard case .object(let vObj) = value else {
                throw JSONSchemaValidationError.validationFailed("Expected object at \(path)")
            }
            let required = obj["required"]?.arrayValue?.compactMap { $0.stringValue } ?? []
            for k in required {
                if vObj[k] == nil {
                    throw JSONSchemaValidationError.validationFailed("Missing required key '\(k)' at \(path)")
                }
            }
            if let props = obj["properties"]?.objectValue {
                for (k, propSchema) in props {
                    guard let vv = vObj[k] else { continue }
                    try validate(schema: propSchema, value: vv, path: "\(path).\(k)")
                }
            }
        case "array":
            guard case .array(let arr) = value else {
                throw JSONSchemaValidationError.validationFailed("Expected array at \(path)")
            }
            if let items = obj["items"] {
                for (idx, el) in arr.enumerated() {
                    try validate(schema: items, value: el, path: "\(path)[\(idx)]")
                }
            }
        case "string":
            guard case .string = value else {
                throw JSONSchemaValidationError.validationFailed("Expected string at \(path)")
            }
        case "boolean":
            guard case .bool = value else {
                throw JSONSchemaValidationError.validationFailed("Expected boolean at \(path)")
            }
        case "number":
            guard case .number = value else {
                throw JSONSchemaValidationError.validationFailed("Expected number at \(path)")
            }
        case "integer":
            guard case .number(let n) = value, n.rounded() == n else {
                throw JSONSchemaValidationError.validationFailed("Expected integer at \(path)")
            }
        default:
            // Unknown type: accept anything.
            return
        }
    }
}

