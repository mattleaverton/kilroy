import Testing
import OmniAICore
@testable import OmniMCP

@Suite
final class MCPRemoteServerTests {
    @Test
    func testListToolsParsesDefinitions() async throws {
        let toolList: JSONValue = .object([
            "tools": .array([
                .object([
                    "name": .string("alpha"),
                    "description": .string("Alpha tool"),
                    "input_schema": .object([
                        "type": .string("object"),
                        "properties": .object([:]),
                        "additionalProperties": .bool(false),
                    ]),
                ]),
            ]),
        ])
        let client = ScriptedMCPClient(responses: ["tools/list": toolList])
        let server = MCPRemoteServer(name: "demo", client: client)

        let tools = try await server.listTools()
        XCTAssertEqual(tools.count, 1)
        XCTAssertEqual(tools.first?.name, "alpha")
        XCTAssertEqual(tools.first?.description, "Alpha tool")
    }

    @Test
    func testCallToolReconnectsAfterFailure() async throws {
        let callResult: JSONValue = .object([
            "content": .object(["ok": .bool(true)]),
            "isError": .bool(false),
        ])
        let client = ScriptedMCPClient(responses: ["tools/call": callResult], failFirst: true)
        let policy = MCPConnectionPolicy(autoReconnect: true, maxRetries: 1, retryDelaySeconds: 0, refreshToolsOnReconnect: false)
        let server = MCPRemoteServer(name: "demo", client: client, policy: policy)

        let result = try await server.callTool(name: "alpha", arguments: .object([:]))
        XCTAssertEqual(result.isError, false)

        let snapshot = await client.snapshot()
        XCTAssertEqual(snapshot.connect, 2)
        XCTAssertEqual(snapshot.disconnect, 1)
    }
}
