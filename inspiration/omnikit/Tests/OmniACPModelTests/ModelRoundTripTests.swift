import Foundation
import Testing
@testable import OmniACPModel

struct ModelRoundTripTests {
    @Test
    func json_rpc_initialize_round_trip() throws {
        let request = Initialize.request(
            id: 1,
            .init(
                protocolVersion: 1,
                clientInfo: ClientInfo(name: "OmniKit", version: "1.0.0"),
                clientCapabilities: ClientCapabilities(
                    fs: FileSystemCapabilities(readTextFile: true, writeTextFile: true)
                )
            )
        )

        let data = try JSONEncoder().encode(request)
        let decoded = try JSONDecoder().decode(Request<Initialize>.self, from: data)
        #expect(decoded.id == .number(1))
        #expect(decoded.method == Initialize.name)
        #expect(decoded.params.protocolVersion == 1)
        #expect(decoded.params.clientCapabilities.fs?.readTextFile == true)
    }

    @Test
    func session_prompt_and_response_round_trip() throws {
        let params = SessionPrompt.Parameters(
            sessionID: "sess_123",
            prompt: [
                .text("hello"),
                .resource(ResourceContentBlock(resource: EmbeddedResource(uri: "file:///tmp/demo.txt", mimeType: "text/plain", text: "abc"))),
            ]
        )
        let request = SessionPrompt.request(id: "req_1", params)
        let encoded = try JSONEncoder().encode(request)
        let decoded = try JSONDecoder().decode(Request<SessionPrompt>.self, from: encoded)

        #expect(decoded.params.sessionID == "sess_123")
        #expect(decoded.params.prompt.count == 2)

        let response = SessionPrompt.response(id: "req_1", result: .init(stopReason: .endTurn))
        let responseData = try JSONEncoder().encode(response)
        let decodedResponse = try JSONDecoder().decode(Response<SessionPrompt>.self, from: responseData)
        #expect(decodedResponse.result?.stopReason == .endTurn)
    }

    @Test
    func session_update_decodes_unknown_fields_without_failing() throws {
        let json = Data("""
        {
          "sessionUpdate": "tool_call",
          "toolCallId": "call_1",
          "title": "Read file",
          "status": "pending",
          "ignoredField": { "nested": true }
        }
        """.utf8)

        let update = try JSONDecoder().decode(SessionUpdate.self, from: json)
        guard case .toolCall(let toolCall) = update else {
            Issue.record("expected toolCall")
            return
        }
        #expect(toolCall.toolCallID == "call_1")
        #expect(toolCall.title == "Read file")
    }

    @Test
    func draft_methods_are_marked_as_draft() {
        #expect(SessionList.schemaStatus == .draft)
        #expect(SessionResume.schemaStatus == .draft)
        #expect(SessionFork.schemaStatus == .draft)
        #expect(SessionNew.schemaStatus == .stable)
    }

    @Test
    func config_option_fixture_shape_decodes() throws {
        let json = Data("""
        {
          "sessionUpdate": "config_option_update",
          "configOptions": [
            {
              "type": "select",
              "id": "model",
              "name": "Model",
              "currentValue": "gpt-4o-mini",
              "options": [
                { "name": "GPT-4o Mini", "value": "gpt-4o-mini" },
                { "group": "quality", "name": "High", "options": [ { "name": "GPT-4o", "value": "gpt-4o" } ] }
              ]
            }
          ]
        }
        """.utf8)

        let update = try JSONDecoder().decode(SessionUpdate.self, from: json)
        guard case .configOptionUpdate(let configUpdate) = update else {
            Issue.record("expected config option update")
            return
        }
        #expect(configUpdate.configOptions.count == 1)
        #expect(configUpdate.configOptions[0].id == "model")
        #expect(configUpdate.configOptions[0].options?.count == 2)
    }
}
