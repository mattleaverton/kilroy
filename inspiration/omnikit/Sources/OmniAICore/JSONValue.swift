import Foundation
#if canImport(CoreFoundation)
import CoreFoundation
#endif

public enum JSONValue: Sendable, Equatable, Codable {
    case null
    case bool(Bool)
    case number(Double)
    case string(String)
    case array([JSONValue])
    case object([String: JSONValue])

    public var objectValue: [String: JSONValue]? {
        guard case .object(let v) = self else { return nil }
        return v
    }

    public var arrayValue: [JSONValue]? {
        guard case .array(let v) = self else { return nil }
        return v
    }

    public var stringValue: String? {
        guard case .string(let v) = self else { return nil }
        return v
    }

    public var boolValue: Bool? {
        guard case .bool(let v) = self else { return nil }
        return v
    }

    public var doubleValue: Double? {
        guard case .number(let v) = self else { return nil }
        return v
    }

    public subscript(_ key: String) -> JSONValue? {
        get {
            guard case .object(let obj) = self else { return nil }
            return obj[key]
        }
        set {
            guard case .object(var obj) = self else { return }
            obj[key] = newValue
            self = .object(obj)
        }
    }

    public func data(prettyPrinted: Bool = false) throws -> Data {
        // JSONSerialization can't encode top-level scalars (it raises NSException).
        // JSONEncoder supports fragments like `"x"`, `4`, `true`, etc.
        let encoder = JSONEncoder()
        encoder.outputFormatting = prettyPrinted ? [.prettyPrinted, .sortedKeys] : [.sortedKeys]
        return try encoder.encode(self)
    }

    public static func parse(_ data: Data) throws -> JSONValue {
        let any = try JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed])
        return try JSONValue(any)
    }

    public static func parse(_ bytes: [UInt8]) throws -> JSONValue {
        try parse(Data(bytes))
    }

    public init(_ any: Any) throws {
        switch any {
        case is NSNull:
            self = .null
        case let v as NSNumber:
            // JSONSerialization represents both numbers and booleans as NSNumber.
            // Distinguish CFBoolean to avoid mis-parsing 0/1 as bools.
            if CFGetTypeID(v) == CFBooleanGetTypeID() {
                self = .bool(v.boolValue)
            } else {
                self = .number(v.doubleValue)
            }
        case let v as Bool:
            self = .bool(v)
        case let v as Int:
            self = .number(Double(v))
        case let v as Int64:
            self = .number(Double(v))
        case let v as Double:
            self = .number(v)
        case let v as String:
            self = .string(v)
        case let v as [Any]:
            self = .array(try v.map(JSONValue.init))
        case let v as [String: Any]:
            var out: [String: JSONValue] = [:]
            out.reserveCapacity(v.count)
            for (k, vv) in v {
                out[k] = try JSONValue(vv)
            }
            self = .object(out)
        default:
            throw JSONValueError.unsupportedType(String(describing: type(of: any)))
        }
    }

    public func asFoundationObject() throws -> Any {
        switch self {
        case .null:
            return NSNull()
        case .bool(let v):
            return v
        case .number(let v):
            guard v.isFinite else {
                throw JSONValueError.nonFiniteNumber(v)
            }
            return v
        case .string(let v):
            return v
        case .array(let vs):
            return try vs.map { try $0.asFoundationObject() }
        case .object(let obj):
            var out: [String: Any] = [:]
            out.reserveCapacity(obj.count)
            for (k, v) in obj {
                out[k] = try v.asFoundationObject()
            }
            return out
        }
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()

        if container.decodeNil() {
            self = .null
            return
        }
        if let b = try? container.decode(Bool.self) {
            self = .bool(b)
            return
        }
        if let n = try? container.decode(Double.self) {
            self = .number(n)
            return
        }
        if let s = try? container.decode(String.self) {
            self = .string(s)
            return
        }
        if let a = try? container.decode([JSONValue].self) {
            self = .array(a)
            return
        }
        if let o = try? container.decode([String: JSONValue].self) {
            self = .object(o)
            return
        }

        throw DecodingError.dataCorruptedError(in: container, debugDescription: "Unsupported JSON value")
    }

    public func encode(to encoder: Encoder) throws {
        switch self {
        case .null:
            var c = encoder.singleValueContainer()
            try c.encodeNil()
        case .bool(let v):
            var c = encoder.singleValueContainer()
            try c.encode(v)
        case .number(let v):
            guard v.isFinite else {
                throw JSONValueError.nonFiniteNumber(v)
            }
            var c = encoder.singleValueContainer()
            try c.encode(v)
        case .string(let v):
            var c = encoder.singleValueContainer()
            try c.encode(v)
        case .array(let vs):
            var c = encoder.unkeyedContainer()
            for v in vs { try c.encode(v) }
        case .object(let obj):
            var c = encoder.container(keyedBy: _CodingKey.self)
            for (k, v) in obj { try c.encode(v, forKey: _CodingKey(k)) }
        }
    }
}

public enum JSONValueError: Error, Sendable, Equatable {
    case unsupportedType(String)
    case nonFiniteNumber(Double)
}

private struct _CodingKey: CodingKey, Hashable {
    var stringValue: String
    var intValue: Int? { nil }

    init(_ string: String) {
        self.stringValue = string
    }

    init?(stringValue: String) {
        self.stringValue = stringValue
    }

    init?(intValue: Int) {
        return nil
    }
}

extension JSONValue: CustomStringConvertible {
    public var description: String {
        switch self {
        case .null:
            return "null"
        case .bool(let v):
            return v ? "true" : "false"
        case .number(let v):
            return String(v)
        case .string(let v):
            return "\"\(v)\""
        case .array(let vs):
            return "[" + vs.map(\.description).joined(separator: ", ") + "]"
        case .object(let obj):
            let inner = obj.keys.sorted().map { key in
                "\(key): \(obj[key]!.description)"
            }.joined(separator: ", ")
            return "{\(inner)}"
        }
    }
}
