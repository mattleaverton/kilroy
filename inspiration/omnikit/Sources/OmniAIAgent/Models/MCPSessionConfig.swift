import Foundation
import OmniMCP

public struct MCPSessionConfig: Sendable, Codable, Equatable {
    public var servers: [MCPServerConfig]
    public var connectionPolicy: MCPConnectionPolicy

    public init(
        servers: [MCPServerConfig] = [],
        connectionPolicy: MCPConnectionPolicy = MCPConnectionPolicy()
    ) {
        self.servers = servers
        self.connectionPolicy = connectionPolicy
    }
}
