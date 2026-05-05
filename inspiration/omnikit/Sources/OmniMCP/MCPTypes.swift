import Foundation
import OmniAICore

public enum MCPTransportKind: String, Codable, Sendable {
    case stdio
    case sse
    case streamableHTTP = "streamable_http"
}

public struct MCPServerConfig: Codable, Hashable, Sendable {
    public var name: String
    public var transport: MCPTransportKind
    public var command: String?
    public var args: [String]
    public var env: [String: String]
    public var url: String?
    public var requestURL: String?
    public var headers: [String: String]

    public init(
        name: String,
        transport: MCPTransportKind,
        command: String? = nil,
        args: [String] = [],
        env: [String: String] = [:],
        url: String? = nil,
        requestURL: String? = nil,
        headers: [String: String] = [:]
    ) {
        self.name = name
        self.transport = transport
        self.command = command
        self.args = args
        self.env = env
        self.url = url
        self.requestURL = requestURL
        self.headers = headers
    }
}

public struct MCPConnectionPolicy: Codable, Hashable, Sendable {
    public var autoReconnect: Bool
    public var maxRetries: Int
    public var retryDelaySeconds: Double
    public var refreshToolsOnReconnect: Bool

    public init(
        autoReconnect: Bool = true,
        maxRetries: Int = 2,
        retryDelaySeconds: Double = 1.0,
        refreshToolsOnReconnect: Bool = true
    ) {
        self.autoReconnect = autoReconnect
        self.maxRetries = maxRetries
        self.retryDelaySeconds = retryDelaySeconds
        self.refreshToolsOnReconnect = refreshToolsOnReconnect
    }
}

public struct MCPToolDefinition: Sendable, Equatable {
    public var name: String
    public var description: String
    public var inputSchema: JSONValue

    public init(name: String, description: String, inputSchema: JSONValue) {
        self.name = name
        self.description = description
        self.inputSchema = inputSchema
    }

    public init?(json: JSONValue) {
        guard let obj = json.objectValue,
              let name = obj["name"]?.stringValue else {
            return nil
        }
        let description = obj["description"]?.stringValue ?? ""
        let schema = obj["inputSchema"] ?? obj["input_schema"] ?? .object([
            "type": .string("object"),
            "properties": .object([:]),
            "additionalProperties": .bool(false),
        ])
        self.init(name: name, description: description, inputSchema: schema)
    }
}

public struct MCPToolCallResult: Sendable, Equatable {
    public var content: JSONValue
    public var isError: Bool

    public init(content: JSONValue, isError: Bool) {
        self.content = content
        self.isError = isError
    }
}

public enum MCPError: Error, Sendable, Equatable {
    case invalidConfiguration(String)
    case invalidResponse(String)
    case rpcError(code: Int, message: String)
    case notConnected
}
