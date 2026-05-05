import Foundation

private let jsonRPCVersion = "2.0"

public enum SchemaStatus: String, Sendable {
    case stable
    case draft
    case preview
}

public protocol SchemaStatusProviding: Sendable {
    static var schemaStatus: SchemaStatus { get }
}

public protocol NotRequired {
    init()
}

public struct Empty: Codable, Hashable, Sendable, NotRequired {
    public init() {}
}

public enum ID: Hashable, Codable, Sendable, ExpressibleByStringLiteral, ExpressibleByIntegerLiteral, CustomStringConvertible {
    case string(String)
    case number(Int)
    case null

    public static var random: ID {
        .string(UUID().uuidString)
    }

    public init(stringLiteral value: StringLiteralType) {
        self = .string(value)
    }

    public init(integerLiteral value: IntegerLiteralType) {
        self = .number(value)
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
            return
        }
        if let string = try? container.decode(String.self) {
            self = .string(string)
            return
        }
        self = .number(try container.decode(Int.self))
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .string(let string):
            try container.encode(string)
        case .number(let number):
            try container.encode(number)
        case .null:
            try container.encodeNil()
        }
    }

    public var description: String {
        switch self {
        case .string(let string):
            return string
        case .number(let number):
            return String(number)
        case .null:
            return "null"
        }
    }
}

public enum Value: Hashable, Codable, Sendable {
    case null
    case bool(Bool)
    case int(Int)
    case double(Double)
    case string(String)
    case array([Value])
    case object([String: Value])

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .null
        } else if let bool = try? container.decode(Bool.self) {
            self = .bool(bool)
        } else if let int = try? container.decode(Int.self) {
            self = .int(int)
        } else if let double = try? container.decode(Double.self) {
            self = .double(double)
        } else if let string = try? container.decode(String.self) {
            self = .string(string)
        } else if let array = try? container.decode([Value].self) {
            self = .array(array)
        } else {
            self = .object(try container.decode([String: Value].self))
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .null:
            try container.encodeNil()
        case .bool(let bool):
            try container.encode(bool)
        case .int(let int):
            try container.encode(int)
        case .double(let double):
            try container.encode(double)
        case .string(let string):
            try container.encode(string)
        case .array(let array):
            try container.encode(array)
        case .object(let object):
            try container.encode(object)
        }
    }

    public var boolValue: Bool? {
        if case .bool(let value) = self { return value }
        return nil
    }

    public var intValue: Int? {
        switch self {
        case .int(let value):
            return value
        case .double(let value):
            return Int(exactly: value)
        default:
            return nil
        }
    }

    public var doubleValue: Double? {
        switch self {
        case .int(let value):
            return Double(value)
        case .double(let value):
            return value
        default:
            return nil
        }
    }

    public var stringValue: String? {
        if case .string(let value) = self { return value }
        return nil
    }

    public var arrayValue: [Value]? {
        if case .array(let value) = self { return value }
        return nil
    }

    public var objectValue: [String: Value]? {
        if case .object(let value) = self { return value }
        return nil
    }

    public subscript(key: String) -> Value? {
        objectValue?[key]
    }
}

public struct RPCError: Error, Codable, Hashable, Sendable {
    public var code: Int
    public var message: String
    public var data: Value?

    public init(code: Int, message: String, data: Value? = nil) {
        self.code = code
        self.message = message
        self.data = data
    }

    public static func parseError(_ message: String = "Parse error") -> RPCError {
        RPCError(code: -32700, message: message)
    }

    public static func invalidRequest(_ message: String = "Invalid request") -> RPCError {
        RPCError(code: -32600, message: message)
    }

    public static func methodNotFound(_ method: String) -> RPCError {
        RPCError(code: -32601, message: "Method not found: \(method)")
    }

    public static func invalidParams(_ message: String = "Invalid params") -> RPCError {
        RPCError(code: -32602, message: message)
    }

    public static func internalError(_ message: String = "Internal error") -> RPCError {
        RPCError(code: -32603, message: message)
    }
}

public protocol Method: SchemaStatusProviding {
    associatedtype Parameters: Codable & Sendable = Empty
    associatedtype Result: Codable & Sendable = Empty
    static var name: String { get }
}

extension Method {
    public static var schemaStatus: SchemaStatus { .stable }

    public static func request(id: ID = .random, _ params: Parameters) -> Request<Self> {
        Request(id: id, method: name, params: params)
    }

    public static func response(id: ID, result: Result) -> Response<Self> {
        Response(id: id, result: result)
    }

    public static func response(id: ID, error: RPCError) -> Response<Self> {
        Response(id: id, error: error)
    }
}

extension Method where Parameters == Empty {
    public static func request(id: ID = .random) -> Request<Self> {
        Request(id: id, method: name, params: Empty())
    }
}

extension Method where Result == Empty {
    public static func response(id: ID) -> Response<Self> {
        Response(id: id, result: Empty())
    }
}

public protocol Notification: SchemaStatusProviding {
    associatedtype Parameters: Codable & Sendable = Empty
    static var name: String { get }
}

extension Notification {
    public static var schemaStatus: SchemaStatus { .stable }

    public static func message(_ params: Parameters) -> Message<Self> {
        Message(method: name, params: params)
    }
}

extension Notification where Parameters == Empty {
    public static func message() -> Message<Self> {
        Message(method: name, params: Empty())
    }
}

public struct Request<M: Method>: Codable, Sendable {
    public var id: ID
    public var method: String
    public var params: M.Parameters

    public init(id: ID, method: String, params: M.Parameters) {
        self.id = id
        self.method = method
        self.params = params
    }

    private enum CodingKeys: String, CodingKey {
        case jsonrpc
        case id
        case method
        case params
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let version = try container.decode(String.self, forKey: .jsonrpc)
        guard version == jsonRPCVersion else {
            throw DecodingError.dataCorruptedError(forKey: .jsonrpc, in: container, debugDescription: "Invalid JSON-RPC version")
        }
        id = try container.decode(ID.self, forKey: .id)
        method = try container.decode(String.self, forKey: .method)
        if let value = try container.decodeIfPresent(M.Parameters.self, forKey: .params) {
            params = value
        } else if M.Parameters.self == Empty.self {
            params = Empty() as! M.Parameters
        } else if let notRequired = M.Parameters.self as? NotRequired.Type {
            params = notRequired.init() as! M.Parameters
        } else {
            throw DecodingError.keyNotFound(CodingKeys.params, DecodingError.Context(codingPath: container.codingPath, debugDescription: "Missing params"))
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(jsonRPCVersion, forKey: .jsonrpc)
        try container.encode(id, forKey: .id)
        try container.encode(method, forKey: .method)
        try container.encode(params, forKey: .params)
    }
}

public struct Response<M: Method>: Codable, Sendable {
    public var id: ID
    public var result: M.Result?
    public var error: RPCError?

    public init(id: ID, result: M.Result) {
        self.id = id
        self.result = result
        self.error = nil
    }

    public init(id: ID, error: RPCError) {
        self.id = id
        self.result = nil
        self.error = error
    }

    private enum CodingKeys: String, CodingKey {
        case jsonrpc
        case id
        case result
        case error
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let version = try container.decode(String.self, forKey: .jsonrpc)
        guard version == jsonRPCVersion else {
            throw DecodingError.dataCorruptedError(forKey: .jsonrpc, in: container, debugDescription: "Invalid JSON-RPC version")
        }
        id = try container.decode(ID.self, forKey: .id)
        error = try container.decodeIfPresent(RPCError.self, forKey: .error)
        if error == nil {
            result = try container.decodeIfPresent(M.Result.self, forKey: .result)
        } else {
            result = nil
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(jsonRPCVersion, forKey: .jsonrpc)
        try container.encode(id, forKey: .id)
        if let error {
            try container.encode(error, forKey: .error)
        } else if let result {
            try container.encode(result, forKey: .result)
        } else if M.Result.self == Empty.self {
            try container.encode(Empty(), forKey: .result)
        }
    }
}

public struct Message<N: Notification>: Codable, Sendable {
    public var method: String
    public var params: N.Parameters

    public init(method: String, params: N.Parameters) {
        self.method = method
        self.params = params
    }

    private enum CodingKeys: String, CodingKey {
        case jsonrpc
        case method
        case params
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let version = try container.decode(String.self, forKey: .jsonrpc)
        guard version == jsonRPCVersion else {
            throw DecodingError.dataCorruptedError(forKey: .jsonrpc, in: container, debugDescription: "Invalid JSON-RPC version")
        }
        method = try container.decode(String.self, forKey: .method)
        if let value = try container.decodeIfPresent(N.Parameters.self, forKey: .params) {
            params = value
        } else if N.Parameters.self == Empty.self {
            params = Empty() as! N.Parameters
        } else if let notRequired = N.Parameters.self as? NotRequired.Type {
            params = notRequired.init() as! N.Parameters
        } else {
            throw DecodingError.keyNotFound(CodingKeys.params, DecodingError.Context(codingPath: container.codingPath, debugDescription: "Missing params"))
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(jsonRPCVersion, forKey: .jsonrpc)
        try container.encode(method, forKey: .method)
        try container.encode(params, forKey: .params)
    }
}

public struct AnyRequest: Codable, Sendable {
    public var id: ID
    public var method: String
    public var params: Value?

    public init(id: ID, method: String, params: Value?) {
        self.id = id
        self.method = method
        self.params = params
    }

    private enum CodingKeys: String, CodingKey {
        case jsonrpc
        case id
        case method
        case params
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let version = try container.decode(String.self, forKey: .jsonrpc)
        guard version == jsonRPCVersion else {
            throw DecodingError.dataCorruptedError(forKey: .jsonrpc, in: container, debugDescription: "Invalid JSON-RPC version")
        }
        id = try container.decode(ID.self, forKey: .id)
        method = try container.decode(String.self, forKey: .method)
        params = try container.decodeIfPresent(Value.self, forKey: .params)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(jsonRPCVersion, forKey: .jsonrpc)
        try container.encode(id, forKey: .id)
        try container.encode(method, forKey: .method)
        try container.encodeIfPresent(params, forKey: .params)
    }
}

public struct AnyResponse: Codable, Sendable {
    public var id: ID
    public var result: Value?
    public var error: RPCError?

    public init(id: ID, result: Value? = nil, error: RPCError? = nil) {
        self.id = id
        self.result = result
        self.error = error
    }

    private enum CodingKeys: String, CodingKey {
        case jsonrpc
        case id
        case result
        case error
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let version = try container.decode(String.self, forKey: .jsonrpc)
        guard version == jsonRPCVersion else {
            throw DecodingError.dataCorruptedError(forKey: .jsonrpc, in: container, debugDescription: "Invalid JSON-RPC version")
        }
        id = try container.decode(ID.self, forKey: .id)
        result = try container.decodeIfPresent(Value.self, forKey: .result)
        error = try container.decodeIfPresent(RPCError.self, forKey: .error)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(jsonRPCVersion, forKey: .jsonrpc)
        try container.encode(id, forKey: .id)
        try container.encodeIfPresent(result, forKey: .result)
        try container.encodeIfPresent(error, forKey: .error)
    }
}

public struct AnyMessage: Codable, Sendable {
    public var method: String
    public var params: Value?

    public init(method: String, params: Value?) {
        self.method = method
        self.params = params
    }

    private enum CodingKeys: String, CodingKey {
        case jsonrpc
        case method
        case params
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let version = try container.decode(String.self, forKey: .jsonrpc)
        guard version == jsonRPCVersion else {
            throw DecodingError.dataCorruptedError(forKey: .jsonrpc, in: container, debugDescription: "Invalid JSON-RPC version")
        }
        method = try container.decode(String.self, forKey: .method)
        params = try container.decodeIfPresent(Value.self, forKey: .params)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(jsonRPCVersion, forKey: .jsonrpc)
        try container.encode(method, forKey: .method)
        try container.encodeIfPresent(params, forKey: .params)
    }

    public func decodeParameters<T: Decodable>(_ type: T.Type, decoder: JSONDecoder = JSONDecoder()) throws -> T {
        let data = try JSONEncoder().encode(params ?? .object([:]))
        return try decoder.decode(T.self, from: data)
    }
}

public func encodeValue<T: Encodable>(_ value: T, encoder: JSONEncoder = JSONEncoder()) throws -> Value {
    let data = try encoder.encode(value)
    return try JSONDecoder().decode(Value.self, from: data)
}

public func decodeValue<T: Decodable>(_ value: Value, as type: T.Type, decoder: JSONDecoder = JSONDecoder()) throws -> T {
    let data = try JSONEncoder().encode(value)
    return try decoder.decode(T.self, from: data)
}
