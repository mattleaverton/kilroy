import Foundation
import Testing
import OmniAgentsSDK
import OmniAICore

private struct _FunctionSchemaURLProbe: Decodable, Sendable {
    let callback: URL
    let tags: [String]
    let metadata: [String: Int]
}

struct FunctionSchemaSafetyTests {
    @Test
    func jsonSchema_handles_url_and_collection_fields() throws {
        let schema = try FunctionSchema.jsonSchema(for: _FunctionSchemaURLProbe.self, strict: true)

        #expect(schema["type"]?.stringValue == "object")
        #expect(schema["properties"]?["callback"]?["format"]?.stringValue == "uri")
        #expect(schema["properties"]?["tags"]?["type"]?.stringValue == "array")
        #expect(schema["properties"]?["metadata"]?["type"]?.stringValue == "object")
    }
}
