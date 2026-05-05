import Foundation
import OmniAICore

public struct MCPDiscoveredTool: Sendable {
    public let server: any MCPServer
    public let definition: MCPToolDefinition

    public init(server: any MCPServer, definition: MCPToolDefinition) {
        self.server = server
        self.definition = definition
    }

    public var name: String { definition.name }
    public var description: String { definition.description }
    public var inputSchema: JSONValue { definition.inputSchema }

    public func call(arguments: JSONValue) async throws -> MCPToolCallResult {
        try await server.callTool(name: definition.name, arguments: arguments)
    }
}

public enum MCPToolDiscovery {
    public static func discoverTools(servers: [any MCPServer]) async throws -> [MCPDiscoveredTool] {
        var tools: [MCPDiscoveredTool] = []
        for server in servers {
            let definitions = try await server.listTools()
            tools.append(contentsOf: definitions.map { MCPDiscoveredTool(server: server, definition: $0) })
        }
        return tools
    }
}
