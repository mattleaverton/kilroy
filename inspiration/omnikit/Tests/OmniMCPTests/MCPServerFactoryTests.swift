import Testing
@testable import OmniMCP

@Suite
final class MCPServerFactoryTests {
    @Test
    func testInvalidConfigsThrow() {
        XCTAssertThrowsError(try MCPServerFactory.makeServer(config: MCPServerConfig(name: "bad-stdio", transport: .stdio))) { error in
            guard case MCPError.invalidConfiguration = error else {
                XCTFail("Expected invalidConfiguration for stdio")
                return
            }
        }

        XCTAssertThrowsError(try MCPServerFactory.makeServer(config: MCPServerConfig(name: "bad-sse", transport: .sse))) { error in
            guard case MCPError.invalidConfiguration = error else {
                XCTFail("Expected invalidConfiguration for sse")
                return
            }
        }

        XCTAssertThrowsError(try MCPServerFactory.makeServer(config: MCPServerConfig(name: "bad-http", transport: .streamableHTTP))) { error in
            guard case MCPError.invalidConfiguration = error else {
                XCTFail("Expected invalidConfiguration for streamable_http")
                return
            }
        }
    }
}
