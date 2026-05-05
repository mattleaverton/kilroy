import Foundation
import Testing
import OmniAICore
import OmniHTTP
@testable import OmniMCP

@Suite
final class MCPStreamableHTTPClientTests {
    private let mcpStreamableHTTPClientTestsURL: URL = {
        guard let url = URL(string: "https://example.com/mcp") else {
            preconditionFailure("Invalid MCP test URL")
        }
        return url
    }()

    @Test
    func testParsesSSEStreamResult() async throws {
        let resultPayload: JSONValue = .object([
            "jsonrpc": .string("2.0"),
            "id": .string("1"),
            "result": .object([
                "tools": .array([]),
            ]),
        ])
        let resultString = String(decoding: try resultPayload.data(), as: UTF8.self)
        let ssePayload = "data: \(resultString)\n\n"
        let body = AsyncThrowingStream<[UInt8], Error> { continuation in
            continuation.yield(Array(ssePayload.utf8))
            continuation.finish()
        }
        var headers = HTTPHeaders()
        headers.set(name: "content-type", value: "text/event-stream")
        let streamResponse = HTTPStreamResponse(statusCode: 200, headers: headers, body: body)
        let transport = StubHTTPTransport(streamResponse: streamResponse)

        let client = MCPStreamableHTTPClient(url: mcpStreamableHTTPClientTestsURL, transport: transport)
        let result = try await client.sendRequest(method: "tools/list", params: nil)

        XCTAssertEqual(result["tools"]?.arrayValue?.count, 0)

        let snapshot = await transport.snapshot()
        XCTAssertEqual(snapshot.openStream, 1)
        XCTAssertEqual(snapshot.send, 0)
    }
}
