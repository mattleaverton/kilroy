import Foundation
import OmniAICore

public func ensureStrictJSONSchema(_ schema: JSONValue) -> JSONValue {
    _strictifySchema(schema)
}

public func ensureStrictJSONSchema(_ schema: [String: JSONValue]) -> [String: JSONValue] {
    if case .object(let object) = ensureStrictJSONSchema(.object(schema)) {
        return object
    }
    return schema
}

private func _strictifySchema(_ value: JSONValue) -> JSONValue {
    guard case .object(var object) = value else {
        if case .array(let array) = value {
            return .array(array.map(_strictifySchema))
        }
        return value
    }

    if let propertiesValue = object["properties"], case .object(let properties) = propertiesValue {
        object["properties"] = .object(properties.mapValues(_strictifySchema))
        if object["type"] == nil {
            object["type"] = .string("object")
        }
        if object["additionalProperties"] == nil {
            object["additionalProperties"] = .bool(false)
        }
    }

    if object["type"]?.stringValue == "object", object["additionalProperties"] == nil {
        object["additionalProperties"] = .bool(false)
    }

    if let items = object["items"] {
        object["items"] = _strictifySchema(items)
    }

    for key in ["allOf", "anyOf", "oneOf", "prefixItems"] {
        if let value = object[key], case .array(let items) = value {
            object[key] = .array(items.map(_strictifySchema))
        }
    }

    if let defs = object["$defs"], case .object(let values) = defs {
        object["$defs"] = .object(values.mapValues(_strictifySchema))
    }
    if let defs = object["definitions"], case .object(let values) = defs {
        object["definitions"] = .object(values.mapValues(_strictifySchema))
    }

    return .object(object)
}
