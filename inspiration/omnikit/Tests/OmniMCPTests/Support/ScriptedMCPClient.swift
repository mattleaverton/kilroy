import OmniAICore
import OmniMCP

actor ScriptedMCPClient: MCPRequestClient {
    private let responses: [String: JSONValue]
    private let failFirst: Bool
    private var requestCount = 0
    private var connectCount = 0
    private var disconnectCount = 0

    init(responses: [String: JSONValue], failFirst: Bool = false) {
        self.responses = responses
        self.failFirst = failFirst
    }

    func connect() async throws {
        connectCount += 1
    }

    func disconnect() async {
        disconnectCount += 1
    }

    func sendRequest(method: String, params: JSONValue?) async throws -> JSONValue {
        requestCount += 1
        if failFirst, requestCount == 1 {
            throw MCPError.invalidResponse("Injected failure")
        }
        guard let response = responses[method] else {
            throw MCPError.invalidResponse("Missing response for \(method)")
        }
        return response
    }

    func snapshot() -> (connect: Int, disconnect: Int, requests: Int) {
        (connectCount, disconnectCount, requestCount)
    }
}
