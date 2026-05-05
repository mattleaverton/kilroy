import Foundation
import OmniAICore

public protocol JSONSchemaProviding {
    static func jsonSchema() throws -> JSONValue
}

public enum FunctionSchema {
    public static func jsonSchema<T: Decodable>(for type: T.Type, strict: Bool = false) throws -> JSONValue {
        let schema = try jsonSchema(for: type as Any.Type)
        return strict ? ensureStrictJSONSchema(schema) : schema
    }

    public static func jsonSchema(for type: Any.Type, strict: Bool = false) throws -> JSONValue {
        let schema = try _SchemaReflection.schema(for: type)
        return strict ? ensureStrictJSONSchema(schema) : schema
    }

    public static func decode<T: Decodable>(_ json: String, as type: T.Type = T.self) throws -> T {
        guard let data = json.data(using: .utf8) else {
            throw ModelBehaviorError(message: "Tool input is not valid UTF-8.")
        }
        let foundation = try JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed])
        return try _DynamicJSONDecoder.decode(type, from: foundation)
    }

    public static func decode(_ json: String, as type: any Decodable.Type) throws -> Any {
        guard let data = json.data(using: .utf8) else {
            throw ModelBehaviorError(message: "Tool input is not valid UTF-8.")
        }
        let foundation = try JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed])
        return try _DynamicJSONDecoder.decode(type, from: foundation)
    }
}

public struct FuncSchema<Parameters: Decodable & Sendable>: Sendable {
    public let name: String
    public let description: String
    public let paramsJSONSchema: [String: JSONValue]

    public init(name: String, description: String, strictJSONSchema: Bool = true) {
        self.name = name
        self.description = description
        if let schema = try? FunctionSchema.jsonSchema(for: Parameters.self, strict: strictJSONSchema),
           case .object(let object) = schema
        {
            self.paramsJSONSchema = object
        } else {
            self.paramsJSONSchema = ensureStrictJSONSchema([
                "type": .string("object"),
                "properties": .object([:]),
                "additionalProperties": .bool(false),
            ])
        }
    }

    public func toCallArgs(_ rawArguments: String) throws -> Parameters {
        try FunctionSchema.decode(rawArguments, as: Parameters.self)
    }
}

private enum _SchemaReflection {
    static func schema(for type: Any.Type) throws -> JSONValue {
        if let provider = type as? any JSONSchemaProviding.Type {
            return try provider.jsonSchema()
        }

        switch type {
        case is String.Type, is Substring.Type, is NSString.Type:
            return .object(["type": .string("string")])
        case is Bool.Type, is NSNumber.Type where _isBooleanNSNumber(type):
            return .object(["type": .string("boolean")])
        case is Int.Type, is Int8.Type, is Int16.Type, is Int32.Type, is Int64.Type,
             is UInt.Type, is UInt8.Type, is UInt16.Type, is UInt32.Type, is UInt64.Type:
            return .object(["type": .string("integer")])
        case is Double.Type, is Float.Type, is Decimal.Type:
            return .object(["type": .string("number")])
        case is Date.Type:
            return .object(["type": .string("string"), "format": .string("date-time")])
        case is UUID.Type:
            return .object(["type": .string("string"), "format": .string("uuid")])
        case is URL.Type:
            return .object(["type": .string("string"), "format": .string("uri")])
        case is JSONValue.Type:
            return .object(["type": .string("object")])
        default:
            break
        }

        if let optional = type as? any _OptionalType.Type {
            return try schema(for: optional.wrappedType)
        }

        if let arrayType = type as? any _ArrayType.Type {
            return .object([
                "type": .string("array"),
                "items": try schema(for: arrayType.elementType),
            ])
        }

        if let dictionaryType = type as? any _DictionaryType.Type,
           dictionaryType.keyType == String.self
        {
            return .object([
                "type": .string("object"),
                "additionalProperties": try schema(for: dictionaryType.valueType),
            ])
        }

        if let decodableType = type as? any Decodable.Type {
            return try recordSchema(for: decodableType)
        }

        return .object(["type": .string("string")])
    }

    private static func castPlaceholder<T>(_ value: Any, as type: T.Type) throws -> T {
        guard let castValue = value as? T else {
            throw ModelBehaviorError(message: "Failed to create schema placeholder for \(String(reflecting: type)).")
        }
        return castValue
    }

    static func placeholder<T: Decodable>(for type: T.Type) throws -> T {
        switch type {
        case let stringType as String.Type:
            return try castPlaceholder(stringType.init(), as: T.self)
        case let substringType as Substring.Type:
            return try castPlaceholder(substringType.init(), as: T.self)
        case let boolType as Bool.Type:
            return try castPlaceholder(boolType.init(false), as: T.self)
        case let intType as Int.Type:
            return try castPlaceholder(intType.init(), as: T.self)
        case let intType as Int8.Type:
            return try castPlaceholder(intType.init(), as: T.self)
        case let intType as Int16.Type:
            return try castPlaceholder(intType.init(), as: T.self)
        case let intType as Int32.Type:
            return try castPlaceholder(intType.init(), as: T.self)
        case let intType as Int64.Type:
            return try castPlaceholder(intType.init(), as: T.self)
        case let uintType as UInt.Type:
            return try castPlaceholder(uintType.init(), as: T.self)
        case let uintType as UInt8.Type:
            return try castPlaceholder(uintType.init(), as: T.self)
        case let uintType as UInt16.Type:
            return try castPlaceholder(uintType.init(), as: T.self)
        case let uintType as UInt32.Type:
            return try castPlaceholder(uintType.init(), as: T.self)
        case let uintType as UInt64.Type:
            return try castPlaceholder(uintType.init(), as: T.self)
        case let doubleType as Double.Type:
            return try castPlaceholder(doubleType.init(), as: T.self)
        case let floatType as Float.Type:
            return try castPlaceholder(floatType.init(), as: T.self)
        case let decimalType as Decimal.Type:
            return try castPlaceholder(decimalType.init(), as: T.self)
        case let dateType as Date.Type:
            return try castPlaceholder(dateType.init(timeIntervalSince1970: 0), as: T.self)
        case let uuidType as UUID.Type:
            return try castPlaceholder(uuidType.init(), as: T.self)
        case let urlType as URL.Type:
            guard let url = urlType.init(string: "https://example.invalid") else {
                throw ModelBehaviorError(message: "Failed to create schema placeholder URL.")
            }
            return try castPlaceholder(url, as: T.self)
        case let jsonType as JSONValue.Type:
            return try castPlaceholder(JSONValue.object([:]), as: T.self)
        default:
            break
        }

        if let arrayType = type as? any _ArrayConstructible.Type {
            return try castPlaceholder(arrayType.makeEmptyArray(), as: T.self)
        }
        if let dictionaryType = type as? any _DictionaryConstructible.Type {
            return try castPlaceholder(dictionaryType.makeEmptyDictionary(), as: T.self)
        }
        return try T(from: _SchemaRecordingDecoder())
    }

    static func recordSchema(for type: any Decodable.Type) throws -> JSONValue {
        let decoder = _SchemaRecordingDecoder()
        _ = try type.init(from: decoder)
        if let root = decoder.recorder.rootSchema {
            return root
        }
        return decoder.recorder.objectSchema()
    }

    private static func _isBooleanNSNumber(_ type: Any.Type) -> Bool {
        type == NSNumber.self
    }
}

private protocol _OptionalType {
    static var wrappedType: Any.Type { get }
}
extension Optional: _OptionalType {
    static var wrappedType: Any.Type { Wrapped.self }
}

private protocol _ArrayType {
    static var elementType: Any.Type { get }
}
extension Array: _ArrayType {
    static var elementType: Any.Type { Element.self }
}

private protocol _DictionaryType {
    static var keyType: Any.Type { get }
    static var valueType: Any.Type { get }
}
extension Dictionary: _DictionaryType {
    static var keyType: Any.Type { Key.self }
    static var valueType: Any.Type { Value.self }
}

private protocol _ArrayConstructible {
    static func makeEmptyArray() -> Any
}
extension Array: _ArrayConstructible {
    static func makeEmptyArray() -> Any { [Element]() }
}

private protocol _DictionaryConstructible {
    static func makeEmptyDictionary() -> Any
}
extension Dictionary: _DictionaryConstructible {
    static func makeEmptyDictionary() -> Any { [Key: Value]() }
}

private final class _SchemaRecorder {
    struct Field {
        var schema: JSONValue
        var required: Bool
    }

    var fields: [String: Field] = [:]
    var rootSchema: JSONValue?

    func recordField(_ key: String, schema: JSONValue, required: Bool) {
        fields[key] = Field(schema: schema, required: required)
    }

    func objectSchema() -> JSONValue {
        let properties = fields.mapValues(\Field.schema)
        let requiredKeys = fields.compactMap { $0.value.required ? $0.key : nil }.sorted()
        var object: [String: JSONValue] = [
            "type": .string("object"),
            "properties": .object(properties),
        ]
        if !requiredKeys.isEmpty {
            object["required"] = .array(requiredKeys.map(JSONValue.string))
        }
        return .object(object)
    }
}

private final class _SchemaRecordingDecoder: Decoder {
    let recorder: _SchemaRecorder
    var codingPath: [any CodingKey]
    var userInfo: [CodingUserInfoKey: Any] = [:]

    init(recorder: _SchemaRecorder = _SchemaRecorder(), codingPath: [any CodingKey] = []) {
        self.recorder = recorder
        self.codingPath = codingPath
    }

    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        let container = _SchemaKeyedContainer<Key>(decoder: self)
        return KeyedDecodingContainer(container)
    }

    func unkeyedContainer() throws -> any UnkeyedDecodingContainer {
        _SchemaUnkeyedContainer(decoder: self)
    }

    func singleValueContainer() throws -> any SingleValueDecodingContainer {
        _SchemaSingleValueContainer(decoder: self)
    }
}

private struct _SchemaKeyedContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
    let decoder: _SchemaRecordingDecoder
    var codingPath: [any CodingKey] { decoder.codingPath }
    var allKeys: [Key] { [] }
    func contains(_ key: Key) -> Bool { true }
    func decodeNil(forKey key: Key) throws -> Bool { true }

    func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool { try decodeGeneric(type, forKey: key) }
    func decode(_ type: String.Type, forKey key: Key) throws -> String { try decodeGeneric(type, forKey: key) }
    func decode(_ type: Double.Type, forKey key: Key) throws -> Double { try decodeGeneric(type, forKey: key) }
    func decode(_ type: Float.Type, forKey key: Key) throws -> Float { try decodeGeneric(type, forKey: key) }
    func decode(_ type: Int.Type, forKey key: Key) throws -> Int { try decodeGeneric(type, forKey: key) }
    func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 { try decodeGeneric(type, forKey: key) }
    func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 { try decodeGeneric(type, forKey: key) }
    func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 { try decodeGeneric(type, forKey: key) }
    func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 { try decodeGeneric(type, forKey: key) }
    func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt { try decodeGeneric(type, forKey: key) }
    func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 { try decodeGeneric(type, forKey: key) }
    func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 { try decodeGeneric(type, forKey: key) }
    func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 { try decodeGeneric(type, forKey: key) }
    func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 { try decodeGeneric(type, forKey: key) }

    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T : Decodable {
        try decodeGeneric(type, forKey: key)
    }

    func decodeIfPresent(_ type: Bool.Type, forKey key: Key) throws -> Bool? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: String.Type, forKey key: Key) throws -> String? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Double.Type, forKey key: Key) throws -> Double? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Float.Type, forKey key: Key) throws -> Float? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Int.Type, forKey key: Key) throws -> Int? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Int8.Type, forKey key: Key) throws -> Int8? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Int16.Type, forKey key: Key) throws -> Int16? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Int32.Type, forKey key: Key) throws -> Int32? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Int64.Type, forKey key: Key) throws -> Int64? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: UInt.Type, forKey key: Key) throws -> UInt? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: UInt8.Type, forKey key: Key) throws -> UInt8? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: UInt16.Type, forKey key: Key) throws -> UInt16? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: UInt32.Type, forKey key: Key) throws -> UInt32? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: UInt64.Type, forKey key: Key) throws -> UInt64? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent<T>(_ type: T.Type, forKey key: Key) throws -> T? where T : Decodable { try decodeOptional(type, forKey: key) }

    func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
        let childRecorder = _SchemaRecorder()
        let childDecoder = _SchemaRecordingDecoder(recorder: childRecorder, codingPath: codingPath + [key])
        decoder.recorder.recordField(key.stringValue, schema: childRecorder.objectSchema(), required: true)
        return try childDecoder.container(keyedBy: keyType)
    }

    func nestedUnkeyedContainer(forKey key: Key) throws -> any UnkeyedDecodingContainer {
        let childRecorder = _SchemaRecorder()
        decoder.recorder.recordField(key.stringValue, schema: .object(["type": .string("array"), "items": .object(["type": .string("string")])]), required: true)
        return _SchemaUnkeyedContainer(decoder: _SchemaRecordingDecoder(recorder: childRecorder, codingPath: codingPath + [key]))
    }

    func superDecoder() throws -> any Decoder { decoder }
    func superDecoder(forKey key: Key) throws -> any Decoder { decoder }

    private func decodeGeneric<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T {
        let schema = try _SchemaReflection.schema(for: type)
        decoder.recorder.recordField(key.stringValue, schema: schema, required: true)
        return try _SchemaReflection.placeholder(for: type)
    }

    private func decodeOptional<T: Decodable>(_ type: T.Type, forKey key: Key) throws -> T? {
        let schema = try _SchemaReflection.schema(for: type)
        decoder.recorder.recordField(key.stringValue, schema: schema, required: false)
        return nil
    }
}

private struct _SchemaUnkeyedContainer: UnkeyedDecodingContainer {
    let decoder: _SchemaRecordingDecoder
    var codingPath: [any CodingKey] { decoder.codingPath }
    var count: Int? { 0 }
    var isAtEnd: Bool { true }
    var currentIndex: Int = 0
    mutating func decodeNil() throws -> Bool { true }
    mutating func decode(_ type: Bool.Type) throws -> Bool { false }
    mutating func decode(_ type: String.Type) throws -> String { "" }
    mutating func decode(_ type: Double.Type) throws -> Double { 0 }
    mutating func decode(_ type: Float.Type) throws -> Float { 0 }
    mutating func decode(_ type: Int.Type) throws -> Int { 0 }
    mutating func decode(_ type: Int8.Type) throws -> Int8 { 0 }
    mutating func decode(_ type: Int16.Type) throws -> Int16 { 0 }
    mutating func decode(_ type: Int32.Type) throws -> Int32 { 0 }
    mutating func decode(_ type: Int64.Type) throws -> Int64 { 0 }
    mutating func decode(_ type: UInt.Type) throws -> UInt { 0 }
    mutating func decode(_ type: UInt8.Type) throws -> UInt8 { 0 }
    mutating func decode(_ type: UInt16.Type) throws -> UInt16 { 0 }
    mutating func decode(_ type: UInt32.Type) throws -> UInt32 { 0 }
    mutating func decode(_ type: UInt64.Type) throws -> UInt64 { 0 }
    mutating func decode<T>(_ type: T.Type) throws -> T where T : Decodable { try _SchemaReflection.placeholder(for: type) }
    mutating func decodeIfPresent<T>(_ type: T.Type) throws -> T? where T : Decodable { nil }
    mutating func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
        let childDecoder = _SchemaRecordingDecoder(codingPath: codingPath)
        return try childDecoder.container(keyedBy: keyType)
    }
    mutating func nestedUnkeyedContainer() throws -> any UnkeyedDecodingContainer { self }
    mutating func superDecoder() throws -> any Decoder { decoder }
}

private struct _SchemaSingleValueContainer: SingleValueDecodingContainer {
    let decoder: _SchemaRecordingDecoder
    var codingPath: [any CodingKey] { decoder.codingPath }
    func decodeNil() -> Bool { true }
    func decode(_ type: Bool.Type) throws -> Bool { false }
    func decode(_ type: String.Type) throws -> String { "" }
    func decode(_ type: Double.Type) throws -> Double { 0 }
    func decode(_ type: Float.Type) throws -> Float { 0 }
    func decode(_ type: Int.Type) throws -> Int { 0 }
    func decode(_ type: Int8.Type) throws -> Int8 { 0 }
    func decode(_ type: Int16.Type) throws -> Int16 { 0 }
    func decode(_ type: Int32.Type) throws -> Int32 { 0 }
    func decode(_ type: Int64.Type) throws -> Int64 { 0 }
    func decode(_ type: UInt.Type) throws -> UInt { 0 }
    func decode(_ type: UInt8.Type) throws -> UInt8 { 0 }
    func decode(_ type: UInt16.Type) throws -> UInt16 { 0 }
    func decode(_ type: UInt32.Type) throws -> UInt32 { 0 }
    func decode(_ type: UInt64.Type) throws -> UInt64 { 0 }
    func decode<T>(_ type: T.Type) throws -> T where T : Decodable {
        let schema = try _SchemaReflection.schema(for: type)
        decoder.recorder.rootSchema = schema
        return try _SchemaReflection.placeholder(for: type)
    }
}

private enum _DynamicJSONDecoder {
    static func decode<T: Decodable>(_ type: T.Type, from value: Any) throws -> T {
        try T(from: _ValueDecoder(value: value))
    }

    static func decode(_ type: any Decodable.Type, from value: Any) throws -> Any {
        try type.init(from: _ValueDecoder(value: value))
    }
}

private struct _AnyCodingKey: CodingKey, Hashable {
    var stringValue: String
    var intValue: Int?

    init(_ string: String) {
        stringValue = string
        intValue = nil
    }

    init(_ index: Int) {
        stringValue = String(index)
        intValue = index
    }

    init?(stringValue: String) {
        self.init(stringValue)
    }

    init?(intValue: Int) {
        self.init(intValue)
    }
}

private struct _ValueDecoder: Decoder {
    let value: Any
    var codingPath: [any CodingKey]
    var userInfo: [CodingUserInfoKey : Any] = [:]

    init(value: Any, codingPath: [any CodingKey] = []) {
        self.value = value
        self.codingPath = codingPath
    }

    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> {
        guard let object = value as? [String: Any] else {
            throw DecodingError.typeMismatch([String: Any].self, .init(codingPath: codingPath, debugDescription: "Expected object."))
        }
        return KeyedDecodingContainer(_ValueKeyedContainer<Key>(decoder: self, object: object))
    }

    func unkeyedContainer() throws -> any UnkeyedDecodingContainer {
        guard let array = value as? [Any] else {
            throw DecodingError.typeMismatch([Any].self, .init(codingPath: codingPath, debugDescription: "Expected array."))
        }
        return _ValueUnkeyedContainer(decoder: self, array: array)
    }

    func singleValueContainer() throws -> any SingleValueDecodingContainer {
        _ValueSingleValueContainer(decoder: self, value: value)
    }
}

private struct _ValueKeyedContainer<Key: CodingKey>: KeyedDecodingContainerProtocol {
    let decoder: _ValueDecoder
    let object: [String: Any]
    var codingPath: [any CodingKey] { decoder.codingPath }
    var allKeys: [Key] { object.keys.compactMap(Key.init(stringValue:)) }

    func contains(_ key: Key) -> Bool { object[key.stringValue] != nil }
    func decodeNil(forKey key: Key) throws -> Bool { object[key.stringValue] is NSNull }

    func decode(_ type: Bool.Type, forKey key: Key) throws -> Bool { try decodePrimitive(type, forKey: key) }
    func decode(_ type: String.Type, forKey key: Key) throws -> String { try decodePrimitive(type, forKey: key) }
    func decode(_ type: Double.Type, forKey key: Key) throws -> Double { try decodePrimitive(type, forKey: key) }
    func decode(_ type: Float.Type, forKey key: Key) throws -> Float { try decodePrimitive(type, forKey: key) }
    func decode(_ type: Int.Type, forKey key: Key) throws -> Int { try decodePrimitive(type, forKey: key) }
    func decode(_ type: Int8.Type, forKey key: Key) throws -> Int8 { try decodePrimitive(type, forKey: key) }
    func decode(_ type: Int16.Type, forKey key: Key) throws -> Int16 { try decodePrimitive(type, forKey: key) }
    func decode(_ type: Int32.Type, forKey key: Key) throws -> Int32 { try decodePrimitive(type, forKey: key) }
    func decode(_ type: Int64.Type, forKey key: Key) throws -> Int64 { try decodePrimitive(type, forKey: key) }
    func decode(_ type: UInt.Type, forKey key: Key) throws -> UInt { try decodePrimitive(type, forKey: key) }
    func decode(_ type: UInt8.Type, forKey key: Key) throws -> UInt8 { try decodePrimitive(type, forKey: key) }
    func decode(_ type: UInt16.Type, forKey key: Key) throws -> UInt16 { try decodePrimitive(type, forKey: key) }
    func decode(_ type: UInt32.Type, forKey key: Key) throws -> UInt32 { try decodePrimitive(type, forKey: key) }
    func decode(_ type: UInt64.Type, forKey key: Key) throws -> UInt64 { try decodePrimitive(type, forKey: key) }

    func decode<T>(_ type: T.Type, forKey key: Key) throws -> T where T : Decodable {
        let raw = try value(forKey: key)
        return try _DynamicJSONDecoder.decode(type, from: raw)
    }

    func decodeIfPresent(_ type: Bool.Type, forKey key: Key) throws -> Bool? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: String.Type, forKey key: Key) throws -> String? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Double.Type, forKey key: Key) throws -> Double? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Float.Type, forKey key: Key) throws -> Float? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Int.Type, forKey key: Key) throws -> Int? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Int8.Type, forKey key: Key) throws -> Int8? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Int16.Type, forKey key: Key) throws -> Int16? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Int32.Type, forKey key: Key) throws -> Int32? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: Int64.Type, forKey key: Key) throws -> Int64? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: UInt.Type, forKey key: Key) throws -> UInt? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: UInt8.Type, forKey key: Key) throws -> UInt8? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: UInt16.Type, forKey key: Key) throws -> UInt16? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: UInt32.Type, forKey key: Key) throws -> UInt32? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent(_ type: UInt64.Type, forKey key: Key) throws -> UInt64? { try decodeOptional(type, forKey: key) }
    func decodeIfPresent<T>(_ type: T.Type, forKey key: Key) throws -> T? where T : Decodable { try decodeOptional(type, forKey: key) }

    func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type, forKey key: Key) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
        let raw = try value(forKey: key)
        return try _ValueDecoder(value: raw, codingPath: codingPath + [key]).container(keyedBy: keyType)
    }

    func nestedUnkeyedContainer(forKey key: Key) throws -> any UnkeyedDecodingContainer {
        let raw = try value(forKey: key)
        return try _ValueDecoder(value: raw, codingPath: codingPath + [key]).unkeyedContainer()
    }

    func superDecoder() throws -> any Decoder { decoder }
    func superDecoder(forKey key: Key) throws -> any Decoder {
        let raw = try value(forKey: key)
        return _ValueDecoder(value: raw, codingPath: codingPath + [key])
    }

    private func value(forKey key: Key) throws -> Any {
        guard let raw = object[key.stringValue] else {
            throw DecodingError.keyNotFound(key, .init(codingPath: codingPath, debugDescription: "Missing key \(key.stringValue)."))
        }
        return raw
    }

    private func decodePrimitive<T>(_ type: T.Type, forKey key: Key) throws -> T where T: Decodable {
        let raw = try value(forKey: key)
        return try _coerce(raw, to: type, codingPath: codingPath + [key])
    }

    private func decodeOptional<T>(_ type: T.Type, forKey key: Key) throws -> T? where T: Decodable {
        guard let raw = object[key.stringValue], !(raw is NSNull) else { return nil }
        return try _coerce(raw, to: type, codingPath: codingPath + [key])
    }
}

private struct _ValueUnkeyedContainer: UnkeyedDecodingContainer {
    let decoder: _ValueDecoder
    let array: [Any]
    var codingPath: [any CodingKey] { decoder.codingPath }
    var count: Int? { array.count }
    var isAtEnd: Bool { currentIndex >= array.count }
    var currentIndex: Int = 0

    mutating func decodeNil() throws -> Bool {
        guard currentIndex < array.count else { return true }
        if array[currentIndex] is NSNull {
            currentIndex += 1
            return true
        }
        return false
    }

    mutating func decode(_ type: Bool.Type) throws -> Bool { try decodeGeneric(type) }
    mutating func decode(_ type: String.Type) throws -> String { try decodeGeneric(type) }
    mutating func decode(_ type: Double.Type) throws -> Double { try decodeGeneric(type) }
    mutating func decode(_ type: Float.Type) throws -> Float { try decodeGeneric(type) }
    mutating func decode(_ type: Int.Type) throws -> Int { try decodeGeneric(type) }
    mutating func decode(_ type: Int8.Type) throws -> Int8 { try decodeGeneric(type) }
    mutating func decode(_ type: Int16.Type) throws -> Int16 { try decodeGeneric(type) }
    mutating func decode(_ type: Int32.Type) throws -> Int32 { try decodeGeneric(type) }
    mutating func decode(_ type: Int64.Type) throws -> Int64 { try decodeGeneric(type) }
    mutating func decode(_ type: UInt.Type) throws -> UInt { try decodeGeneric(type) }
    mutating func decode(_ type: UInt8.Type) throws -> UInt8 { try decodeGeneric(type) }
    mutating func decode(_ type: UInt16.Type) throws -> UInt16 { try decodeGeneric(type) }
    mutating func decode(_ type: UInt32.Type) throws -> UInt32 { try decodeGeneric(type) }
    mutating func decode(_ type: UInt64.Type) throws -> UInt64 { try decodeGeneric(type) }
    mutating func decode<T>(_ type: T.Type) throws -> T where T : Decodable { try decodeGeneric(type) }
    mutating func decodeIfPresent<T>(_ type: T.Type) throws -> T? where T : Decodable {
        guard currentIndex < array.count, !(array[currentIndex] is NSNull) else {
            currentIndex += 1
            return nil
        }
        return try decodeGeneric(type)
    }
    mutating func nestedContainer<NestedKey>(keyedBy keyType: NestedKey.Type) throws -> KeyedDecodingContainer<NestedKey> where NestedKey : CodingKey {
        let raw = try nextValue()
        return try _ValueDecoder(value: raw, codingPath: codingPath + [_AnyCodingKey(currentIndex - 1)]).container(keyedBy: keyType)
    }
    mutating func nestedUnkeyedContainer() throws -> any UnkeyedDecodingContainer {
        let raw = try nextValue()
        return try _ValueDecoder(value: raw, codingPath: codingPath + [_AnyCodingKey(currentIndex - 1)]).unkeyedContainer()
    }
    mutating func superDecoder() throws -> any Decoder {
        let raw = try nextValue()
        return _ValueDecoder(value: raw, codingPath: codingPath + [_AnyCodingKey(currentIndex - 1)])
    }

    private mutating func nextValue() throws -> Any {
        guard currentIndex < array.count else {
            throw DecodingError.valueNotFound(Any.self, .init(codingPath: codingPath, debugDescription: "Array index out of bounds."))
        }
        defer { currentIndex += 1 }
        return array[currentIndex]
    }

    private mutating func decodeGeneric<T: Decodable>(_ type: T.Type) throws -> T {
        let raw = try nextValue()
        return try _DynamicJSONDecoder.decode(type, from: raw)
    }
}

private struct _ValueSingleValueContainer: SingleValueDecodingContainer {
    let decoder: _ValueDecoder
    let value: Any
    var codingPath: [any CodingKey] { decoder.codingPath }
    func decodeNil() -> Bool { value is NSNull }
    func decode(_ type: Bool.Type) throws -> Bool { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: String.Type) throws -> String { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: Double.Type) throws -> Double { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: Float.Type) throws -> Float { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: Int.Type) throws -> Int { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: Int8.Type) throws -> Int8 { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: Int16.Type) throws -> Int16 { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: Int32.Type) throws -> Int32 { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: Int64.Type) throws -> Int64 { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: UInt.Type) throws -> UInt { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: UInt8.Type) throws -> UInt8 { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: UInt16.Type) throws -> UInt16 { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: UInt32.Type) throws -> UInt32 { try _coerce(value, to: type, codingPath: codingPath) }
    func decode(_ type: UInt64.Type) throws -> UInt64 { try _coerce(value, to: type, codingPath: codingPath) }
    func decode<T>(_ type: T.Type) throws -> T where T : Decodable { try _DynamicJSONDecoder.decode(type, from: value) }
}

private func _checkedDecodedCast<T>(_ value: Any, to type: T.Type, codingPath: [any CodingKey]) throws -> T {
    guard let castValue = value as? T else {
        throw DecodingError.typeMismatch(
            type,
            DecodingError.Context(
                codingPath: codingPath,
                debugDescription: "Expected \(String(reflecting: type)) but found \(String(reflecting: Swift.type(of: value)))."
            )
        )
    }
    return castValue
}

private func _coerce<T: Decodable>(_ raw: Any, to type: T.Type, codingPath: [any CodingKey]) throws -> T {
    switch type {
    case let boolType as Bool.Type:
        if let value = raw as? Bool { return try _checkedDecodedCast(value, to: type, codingPath: codingPath) }
        if let number = raw as? NSNumber { return try _checkedDecodedCast(number.boolValue, to: type, codingPath: codingPath) }
    case let stringType as String.Type:
        if let value = raw as? String { return try _checkedDecodedCast(value, to: type, codingPath: codingPath) }
        return try _checkedDecodedCast(String(describing: raw), to: type, codingPath: codingPath)
    case let doubleType as Double.Type:
        if let number = raw as? NSNumber { return try _checkedDecodedCast(number.doubleValue, to: type, codingPath: codingPath) }
    case let floatType as Float.Type:
        if let number = raw as? NSNumber { return try _checkedDecodedCast(number.floatValue, to: type, codingPath: codingPath) }
    case let intType as Int.Type:
        if let number = raw as? NSNumber { return try _checkedDecodedCast(number.intValue, to: type, codingPath: codingPath) }
    case let intType as Int8.Type:
        if let number = raw as? NSNumber { return try _checkedDecodedCast(Int8(number.intValue), to: type, codingPath: codingPath) }
    case let intType as Int16.Type:
        if let number = raw as? NSNumber { return try _checkedDecodedCast(Int16(number.intValue), to: type, codingPath: codingPath) }
    case let intType as Int32.Type:
        if let number = raw as? NSNumber { return try _checkedDecodedCast(Int32(number.intValue), to: type, codingPath: codingPath) }
    case let intType as Int64.Type:
        if let number = raw as? NSNumber { return try _checkedDecodedCast(number.int64Value, to: type, codingPath: codingPath) }
    case let intType as UInt.Type:
        if let number = raw as? NSNumber { return try _checkedDecodedCast(UInt(number.uintValue), to: type, codingPath: codingPath) }
    case let intType as UInt8.Type:
        if let number = raw as? NSNumber { return try _checkedDecodedCast(UInt8(number.uintValue), to: type, codingPath: codingPath) }
    case let intType as UInt16.Type:
        if let number = raw as? NSNumber { return try _checkedDecodedCast(UInt16(number.uintValue), to: type, codingPath: codingPath) }
    case let intType as UInt32.Type:
        if let number = raw as? NSNumber { return try _checkedDecodedCast(UInt32(number.uintValue), to: type, codingPath: codingPath) }
    case let intType as UInt64.Type:
        if let number = raw as? NSNumber { return try _checkedDecodedCast(number.uint64Value, to: type, codingPath: codingPath) }
    case let dateType as Date.Type:
        if let string = raw as? String, let date = try? Date(string, strategy: .iso8601) {
            return try _checkedDecodedCast(date, to: type, codingPath: codingPath)
        }
    case let uuidType as UUID.Type:
        if let string = raw as? String, let uuid = UUID(uuidString: string) { return try _checkedDecodedCast(uuid, to: type, codingPath: codingPath) }
    case let urlType as URL.Type:
        if let string = raw as? String, let url = URL(string: string) { return try _checkedDecodedCast(url, to: type, codingPath: codingPath) }
    default:
        break
    }

    return try _DynamicJSONDecoder.decode(type, from: raw)
}
