import Foundation
import Testing
@testable import OmniACPModel

struct GoldenTests {
    private let subdirectory = "GoldenTests/PythonSDK"

    private func load(_ name: String) throws -> Data {
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .appendingPathComponent(subdirectory, isDirectory: true)
            .appendingPathComponent("\(name).json")
        guard FileManager.default.fileExists(atPath: url.path) else {
            throw NSError(domain: "GoldenTests", code: 1, userInfo: [NSLocalizedDescriptionKey: "Missing golden \(name)"])
        }
        return try Data(contentsOf: url)
    }

    private func jsonEqual(_ lhs: Data, _ rhs: Data) throws -> Bool {
        let left = try JSONSerialization.jsonObject(with: lhs) as AnyObject
        let right = try JSONSerialization.jsonObject(with: rhs) as AnyObject
        return left.isEqual(right)
    }

    @Test
    func golden_initialize_request_round_trip() throws {
        let golden = try load("initialize_request")
        let decoded = try JSONDecoder().decode(Initialize.Parameters.self, from: golden)
        #expect(decoded.protocolVersion == 1)
        #expect(decoded.clientCapabilities.fs?.readTextFile == true)
        let reencoded = try JSONEncoder().encode(decoded)
        #expect(try jsonEqual(golden, reencoded))
    }

    @Test
    func golden_initialize_response_round_trip() throws {
        let golden = try load("initialize_response")
        let decoded = try JSONDecoder().decode(Initialize.Result.self, from: golden)
        #expect(decoded.protocolVersion == 1)
        #expect(decoded.agentCapabilities.promptCapabilities?.image == true)
        let reencoded = try JSONEncoder().encode(decoded)
        #expect(try jsonEqual(golden, reencoded))
    }

    @Test
    func golden_new_session_and_prompt_round_trip() throws {
        let newSession = try load("new_session_request")
        let newSessionDecoded = try JSONDecoder().decode(SessionNew.Parameters.self, from: newSession)
        #expect(newSessionDecoded.cwd == "/home/user/project")
        #expect(newSessionDecoded.mcpServers.count == 1)
        #expect(try jsonEqual(newSession, JSONEncoder().encode(newSessionDecoded)))

        let prompt = try load("prompt_request")
        let promptDecoded = try JSONDecoder().decode(SessionPrompt.Parameters.self, from: prompt)
        #expect(promptDecoded.sessionID == "sess_abc123def456")
        #expect(promptDecoded.prompt.count == 2)
        #expect(try jsonEqual(prompt, JSONEncoder().encode(promptDecoded)))
    }

    @Test
    func golden_session_updates_round_trip() throws {
        let toolCall = try load("session_update_tool_call")
        let toolCallDecoded = try JSONDecoder().decode(SessionUpdate.self, from: toolCall)
        guard case .toolCall(let call) = toolCallDecoded else {
            Issue.record("expected tool_call")
            return
        }
        #expect(call.toolCallID == "call_001")
        #expect(try jsonEqual(toolCall, JSONEncoder().encode(toolCallDecoded)))

        let toolUpdate = try load("session_update_tool_call_update_more_fields")
        let toolUpdateDecoded = try JSONDecoder().decode(SessionUpdate.self, from: toolUpdate)
        guard case .toolCallUpdate(let update) = toolUpdateDecoded else {
            Issue.record("expected tool_call_update")
            return
        }
        #expect(update.toolCallID == "call_010")
        #expect(update.content?.count == 1)
        #expect(try jsonEqual(toolUpdate, JSONEncoder().encode(toolUpdateDecoded)))

        let plan = try load("session_update_plan")
        let planDecoded = try JSONDecoder().decode(SessionUpdate.self, from: plan)
        guard case .plan(let planUpdate) = planDecoded else {
            Issue.record("expected plan")
            return
        }
        #expect(planUpdate.entries.count == 2)
        #expect(try jsonEqual(plan, JSONEncoder().encode(planDecoded)))
    }

    @Test
    func golden_mode_and_config_updates_round_trip() throws {
        let mode = try load("session_update_current_mode_update")
        let modeDecoded = try JSONDecoder().decode(SessionUpdate.self, from: mode)
        guard case .currentModeUpdate(let currentMode) = modeDecoded else {
            Issue.record("expected current mode update")
            return
        }
        #expect(currentMode.currentModeID == "ask")
        #expect(try jsonEqual(mode, JSONEncoder().encode(modeDecoded)))

        let config = try load("session_update_config_option_update")
        let configDecoded = try JSONDecoder().decode(SessionUpdate.self, from: config)
        guard case .configOptionUpdate(let configUpdate) = configDecoded else {
            Issue.record("expected config option update")
            return
        }
        #expect(configUpdate.configOptions.count == 2)
        #expect(try jsonEqual(config, JSONEncoder().encode(configDecoded)))
    }

    @Test
    func golden_delegate_payloads_round_trip() throws {
        let cancel = try load("cancel_notification")
        let cancelDecoded = try JSONDecoder().decode(SessionCancel.Parameters.self, from: cancel)
        #expect(cancelDecoded.sessionID == "sess_abc123def456")
        #expect(try jsonEqual(cancel, JSONEncoder().encode(cancelDecoded)))

        let permission = try load("request_permission_request")
        let permissionDecoded = try JSONDecoder().decode(SessionRequestPermission.Parameters.self, from: permission)
        #expect(permissionDecoded.toolCall?.toolCallID == "call_001")
        #expect(try jsonEqual(permission, JSONEncoder().encode(permissionDecoded)))

        let permissionResponse = try load("request_permission_response_selected")
        let permissionResult = try JSONDecoder().decode(SessionRequestPermission.Result.self, from: permissionResponse)
        #expect(permissionResult.outcome.optionID == "allow-once")
        #expect(try jsonEqual(permissionResponse, JSONEncoder().encode(permissionResult)))

        let readRequest = try load("fs_read_text_file_request")
        let readRequestDecoded = try JSONDecoder().decode(FileSystemReadTextFile.Parameters.self, from: readRequest)
        #expect(readRequestDecoded.line == 10)
        #expect(try jsonEqual(readRequest, JSONEncoder().encode(readRequestDecoded)))

        let readResponse = try load("fs_read_text_file_response")
        let readResponseDecoded = try JSONDecoder().decode(FileSystemReadTextFile.Result.self, from: readResponse)
        #expect(readResponseDecoded.content.contains("hello_world"))
        #expect(try jsonEqual(readResponse, JSONEncoder().encode(readResponseDecoded)))
    }
}
