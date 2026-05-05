import Foundation
import OmniAICore
import OmniHTTP

public protocol MCPServer: Sendable {
    var name: String { get }
    func connect() async throws
    func cleanup() async
    func listTools() async throws -> [MCPToolDefinition]
    func callTool(name: String, arguments: JSONValue) async throws -> MCPToolCallResult
}

public actor MCPRemoteServer: MCPServer {
    public nonisolated let name: String
    private let client: MCPRequestClient
    private let policy: MCPConnectionPolicy
    private var cachedTools: [MCPToolDefinition] = []
    private var connected = false

    public init(name: String, client: MCPRequestClient, policy: MCPConnectionPolicy = MCPConnectionPolicy()) {
        self.name = name
        self.client = client
        self.policy = policy
    }

    public func connect() async throws {
        if connected { return }
        try await client.connect()
        connected = true
    }

    public func cleanup() async {
        connected = false
        await client.disconnect()
    }

    public func listTools() async throws -> [MCPToolDefinition] {
        if !connected {
            try await connect()
        }
        return try await withReconnect {
            let result = try await client.sendRequest(method: "tools/list", params: nil)
            let tools = try parseTools(from: result)
            cachedTools = tools
            return tools
        }
    }

    public func callTool(name: String, arguments: JSONValue) async throws -> MCPToolCallResult {
        if !connected {
            try await connect()
        }
        return try await withReconnect {
            let params = JSONValue.object([
                "name": .string(name),
                "arguments": arguments,
            ])
            let result = try await client.sendRequest(method: "tools/call", params: params)
            return try parseToolResult(from: result)
        }
    }

    private func parseTools(from result: JSONValue) throws -> [MCPToolDefinition] {
        guard let obj = result.objectValue else {
            throw MCPError.invalidResponse("tools/list result is not an object")
        }
        let toolsValue = obj["tools"] ?? obj["result"] ?? .array([])
        guard let array = toolsValue.arrayValue else {
            throw MCPError.invalidResponse("tools/list missing tools array")
        }
        return array.compactMap(MCPToolDefinition.init)
    }

    private func parseToolResult(from result: JSONValue) throws -> MCPToolCallResult {
        if let obj = result.objectValue {
            let content = obj["content"] ?? result
            let isError = obj["isError"]?.boolValue ?? obj["is_error"]?.boolValue ?? false
            return MCPToolCallResult(content: content, isError: isError)
        }
        return MCPToolCallResult(content: result, isError: false)
    }

    private func withReconnect<T>(_ operation: () async throws -> T) async throws -> T {
        do {
            return try await operation()
        } catch {
            guard policy.autoReconnect else { throw error }
            var lastError = error
            for _ in 0..<max(0, policy.maxRetries) {
                await client.disconnect()
                if policy.retryDelaySeconds > 0 {
                    let nanos = UInt64(policy.retryDelaySeconds * 1_000_000_000)
                    try? await Task.sleep(nanoseconds: nanos)
                }
                do {
                    try await client.connect()
                    if policy.refreshToolsOnReconnect {
                        _ = try? await listTools()
                    }
                    return try await operation()
                } catch {
                    lastError = error
                }
            }
            throw lastError
        }
    }
}

public enum MCPServerFactory {
    public static func makeServer(
        config: MCPServerConfig,
        policy: MCPConnectionPolicy = MCPConnectionPolicy(),
        transport: HTTPTransport = URLSessionHTTPTransport()
    ) throws -> MCPServer {
        switch config.transport {
        case .stdio:
            guard let command = config.command, !command.isEmpty else {
                throw MCPError.invalidConfiguration("stdio MCP server '\(config.name)' missing command")
            }
            let transport = StdioMCPTransport(command: command, args: config.args, env: config.env)
            let client = MCPJSONRPCClient(transport: transport)
            return MCPRemoteServer(name: config.name, client: client, policy: policy)
        case .sse:
            guard let urlString = config.url, let url = URL(string: urlString) else {
                throw MCPError.invalidConfiguration("sse MCP server '\(config.name)' missing url")
            }
            let requestURL = config.requestURL.flatMap(URL.init(string:))
            let transport = SSEMCPTransport(url: url, requestURL: requestURL, headers: config.headers, transport: transport)
            let client = MCPJSONRPCClient(transport: transport)
            return MCPRemoteServer(name: config.name, client: client, policy: policy)
        case .streamableHTTP:
            guard let urlString = config.url, let url = URL(string: urlString) else {
                throw MCPError.invalidConfiguration("streamable_http MCP server '\(config.name)' missing url")
            }
            let client = MCPStreamableHTTPClient(url: url, headers: config.headers, transport: transport)
            return MCPRemoteServer(name: config.name, client: client, policy: policy)
        }
    }
}

public final class MCPServerManager: @unchecked Sendable {
    private let lock = NSLock()
    private var servers: [any MCPServer]

    public init(servers: [any MCPServer] = []) {
        self.servers = servers
    }

    public func addServer(_ server: any MCPServer) {
        lock.lock(); servers.append(server); lock.unlock()
    }

    public func connectAll() async throws {
        let current = lock.withLock { servers }
        for server in current {
            try await server.connect()
        }
    }

    public func cleanupAll() async {
        let current = lock.withLock { servers }
        for server in current {
            await server.cleanup()
        }
    }

    public func currentServers() -> [any MCPServer] {
        lock.withLock { servers }
    }
}

private extension NSLock {
    func withLock<T>(_ body: () -> T) -> T {
        lock()
        defer { unlock() }
        return body()
    }
}
