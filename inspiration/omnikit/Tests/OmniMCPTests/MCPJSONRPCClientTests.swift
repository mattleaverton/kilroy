import Testing
import OmniAICore
@testable import OmniMCP

@Suite
final class MCPJSONRPCClientTests {
    @Test
    func testSendRequestParsesResponse() async throws {
        let transport = RecordingMCPTransport { data in
            guard let request = try? JSONValue.parse(data),
                  let id = request["id"]?.stringValue else {
                return nil
            }
            let response: JSONValue = .object([
                "jsonrpc": .string("2.0"),
                "id": .string(id),
                "result": .object(["ok": .bool(true)]),
            ])
            return try? response.data()
        }

        let client = MCPJSONRPCClient(transport: transport)
        try await client.connect()
        let result = try await client.sendRequest(method: "ping", params: .object(["value": .number(1)]))

        XCTAssertEqual(result["ok"]?.boolValue, true)

        let sent = await transport.sentRequests()
        let first = try XCTUnwrap(sent.first)
        let sentJSON = try JSONValue.parse(first)
        XCTAssertEqual(sentJSON["method"]?.stringValue, "ping")
    }
}
