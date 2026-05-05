import Foundation
import Testing
@testable import OmniACP
@testable import OmniACPModel

private actor NotificationRecorder {
    private(set) var sawAgentMessage = false
    func record(_ notification: AnyMessage) throws {
        guard notification.method == SessionUpdateNotification.name else { return }
        let params = try notification.decodeParameters(SessionUpdateNotification.Parameters.self)
        if case .agentMessageChunk = params.update {
            sawAgentMessage = true
        }
    }
}

private actor CancellationObservation {
    private(set) var sawCancel = false

    func markCancelled() {
        sawCancel = true
    }
}

private actor AgentObservations {
    private(set) var permissionOptionID: String?
    private(set) var fileReadContent: String?
    private(set) var terminalOutput: String?

    func recordPermission(_ optionID: String?) {
        permissionOptionID = optionID
    }

    func recordFileRead(_ content: String?) {
        fileReadContent = content
    }

    func recordTerminalOutput(_ output: String?) {
        terminalOutput = output
    }
}

private func startPromptingAgent(on transport: InMemoryTransport, observations: AgentObservations? = nil) -> Task<Void, Never> {
    Task {
        try? await transport.connect()
        let encoder = JSONEncoder()
        let stream = transport.receive()
        do {
            for try await data in stream {
                let object = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
                let method = object?["method"] as? String
                let hasID = object?.keys.contains("id") == true

                if let method {
                    if hasID {
                        switch method {
                        case Initialize.name:
                            let request = try JSONDecoder().decode(Request<Initialize>.self, from: data)
                            let response = Initialize.response(
                                id: request.id,
                                result: .init(
                                    protocolVersion: request.params.protocolVersion,
                                    agentInfo: .init(name: "InMemoryAgent", version: "1.0.0"),
                                    agentCapabilities: .init(
                                        loadSession: true,
                                        mcpCapabilities: .init(),
                                        promptCapabilities: .init(image: true, audio: true, embeddedContext: true)
                                    ),
                                    authMethods: []
                                )
                            )
                            try await transport.send(encoder.encode(response))
                        case SessionNew.name:
                            let request = try JSONDecoder().decode(Request<SessionNew>.self, from: data)
                            let response = SessionNew.response(id: request.id, result: .init(sessionID: "sess_agent"))
                            try await transport.send(encoder.encode(response))
                        case SessionPrompt.name:
                            let request = try JSONDecoder().decode(Request<SessionPrompt>.self, from: data)
                            let update = Message<SessionUpdateNotification>(
                                method: SessionUpdateNotification.name,
                                params: .init(
                                    sessionID: request.params.sessionID,
                                    update: .agentMessageChunk(.init(content: .init(text: "hello from agent")))
                                )
                            )
                            try await transport.send(encoder.encode(update))
                            let response = SessionPrompt.response(id: request.id, result: .init(stopReason: .endTurn))
                            try await transport.send(encoder.encode(response))
                        case SessionCancel.name:
                            let request = try JSONDecoder().decode(Request<SessionCancel>.self, from: data)
                            try await transport.send(encoder.encode(SessionCancel.response(id: request.id)))
                        default:
                            break
                        }
                    } else if method == InitializedNotification.name, observations != nil {
                        let permissionRequest = SessionRequestPermission.request(
                            id: 900,
                            .init(
                                sessionID: "sess_agent",
                                toolCall: .init(toolCallID: "call_1"),
                                options: [
                                    .init(optionID: "allow-once", name: "Allow once", kind: "allow_once"),
                                    .init(optionID: "reject-once", name: "Reject", kind: "reject_once"),
                                ]
                            )
                        )
                        try await transport.send(encoder.encode(permissionRequest))
                    }
                    continue
                }

                guard hasID else {
                    continue
                }

                if let response = try? JSONDecoder().decode(Response<SessionRequestPermission>.self, from: data), response.result != nil {
                    await observations?.recordPermission(response.result?.outcome.optionID)
                    let readRequest = FileSystemReadTextFile.request(
                        id: 901,
                        .init(sessionID: "sess_agent", path: "sample.txt")
                    )
                    try await transport.send(encoder.encode(readRequest))
                    continue
                }
                if let response = try? JSONDecoder().decode(Response<FileSystemReadTextFile>.self, from: data), response.result != nil {
                    await observations?.recordFileRead(response.result?.content)
                    #if !os(iOS) && !os(tvOS) && !os(watchOS) && !os(visionOS)
                    let createRequest = TerminalCreate.request(
                        id: 902,
                        .init(
                            sessionID: "sess_agent",
                            command: "/bin/sh",
                            args: ["-c", "printf agent-terminal"],
                            cwd: "."
                        )
                    )
                    try await transport.send(encoder.encode(createRequest))
                    #endif
                    continue
                }
                #if !os(iOS) && !os(tvOS) && !os(watchOS) && !os(visionOS)
                if let response = try? JSONDecoder().decode(Response<TerminalCreate>.self, from: data),
                   let terminalID = response.result?.terminalID {
                    let waitRequest = TerminalWaitForExit.request(id: 903, .init(sessionID: "sess_agent", terminalID: terminalID))
                    let outputRequest = TerminalOutput.request(id: 904, .init(sessionID: "sess_agent", terminalID: terminalID))
                    try await transport.send(encoder.encode(waitRequest))
                    try await transport.send(encoder.encode(outputRequest))
                    continue
                }
                if let response = try? JSONDecoder().decode(Response<TerminalOutput>.self, from: data), response.result != nil {
                    await observations?.recordTerminalOutput(response.result?.output)
                    continue
                }
                #endif
            }
        } catch {
        }
    }
}


private func withTestTimeout<T: Sendable>(seconds: Double, label: String, operation: @escaping @Sendable () async throws -> T) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask {
            try await operation()
        }
        group.addTask {
            try await Task.sleep(for: .milliseconds(Int64(seconds * 1_000)))
            throw NSError(domain: "ClientTests", code: 99, userInfo: [NSLocalizedDescriptionKey: "Timed out during \(label)"])
        }
        guard let result = try await group.next() else {
            group.cancelAll()
            throw NSError(domain: "ClientTests", code: 100, userInfo: [NSLocalizedDescriptionKey: "No result produced during \(label)"])
        }
        group.cancelAll()
        return result
    }
}

struct ClientTests {
    @Test
    func promptCancellationSendsBestEffortSessionCancel() async throws {
        let (clientTransport, agentTransport) = await InMemoryTransport.createConnectedPair()
        try await clientTransport.connect()
        let cancellationObservation = CancellationObservation()
        let agentTask = Task {
            try? await agentTransport.connect()
            let encoder = JSONEncoder()
            let stream = agentTransport.receive()
            do {
                for try await data in stream {
                    let object = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
                    let method = object?["method"] as? String
                    let hasID = object?.keys.contains("id") == true
                    guard let method else { continue }
                    if hasID {
                        switch method {
                        case Initialize.name:
                            let request = try JSONDecoder().decode(Request<Initialize>.self, from: data)
                            let response = Initialize.response(
                                id: request.id,
                                result: .init(protocolVersion: 1, agentCapabilities: .init(mcpCapabilities: .init(), promptCapabilities: .init()), authMethods: [])
                            )
                            try await agentTransport.send(encoder.encode(response))
                        case SessionNew.name:
                            let request = try JSONDecoder().decode(Request<SessionNew>.self, from: data)
                            try await agentTransport.send(encoder.encode(SessionNew.response(id: request.id, result: .init(sessionID: "sess_cancel"))))
                        case SessionPrompt.name:
                            break
                        case SessionCancel.name:
                            let request = try JSONDecoder().decode(Request<SessionCancel>.self, from: data)
                            await cancellationObservation.markCancelled()
                            try await agentTransport.send(encoder.encode(SessionCancel.response(id: request.id)))
                        default:
                            break
                        }
                    }
                }
            } catch {
            }
        }
        defer { agentTask.cancel() }

        let client = Client(name: "Tests", version: "1.0.0")
        _ = try await withTestTimeout(seconds: 5, label: "connect") { try await client.connect(transport: clientTransport) }
        let session = try await withTestTimeout(seconds: 5, label: "newSession") { try await client.newSession(cwd: "/tmp") }

        let promptTask = Task {
            try await client.prompt(sessionID: session.sessionID, prompt: [.text("hang")], timeout: .seconds(30))
        }
        promptTask.cancel()
        _ = try? await promptTask.value

        try? await Task.sleep(for: .milliseconds(200))
        #expect(await cancellationObservation.sawCancel)
        await client.disconnect()
    }

    @Test
    func in_memory_client_lifecycle_and_streaming_work() async throws {
        let (clientTransport, agentTransport) = await InMemoryTransport.createConnectedPair()
        try await clientTransport.connect()
        let agentTask = startPromptingAgent(on: agentTransport)
        defer { agentTask.cancel() }

        let client = Client(name: "Tests", version: "1.0.0", capabilities: .init(fs: .init(readTextFile: true, writeTextFile: true)))
        let initResult = try await withTestTimeout(seconds: 5, label: "connect") { try await client.connect(transport: clientTransport) }
        #expect(initResult.agentInfo?.name == "InMemoryAgent")

        let recorder = NotificationRecorder()
        let recorderTask = Task {
            for await notification in client.notifications {
                try? await recorder.record(notification)
            }
        }
        defer { recorderTask.cancel() }

        let session = try await withTestTimeout(seconds: 5, label: "newSession") { try await client.newSession(cwd: "/tmp") }
        #expect(session.sessionID == "sess_agent")

        let promptResult = try await withTestTimeout(seconds: 5, label: "prompt") { try await client.prompt(sessionID: session.sessionID, prompt: [.text("hello")], timeout: .seconds(5)) }
        #expect(promptResult.stopReason == StopReason.endTurn)
        try? await Task.sleep(for: .milliseconds(100))
        #expect(await recorder.sawAgentMessage)
        await client.disconnect()
    }

    @Test
    func delegate_routes_permission_and_file_requests() async throws {
        let tempDirectory = URL(fileURLWithPath: NSTemporaryDirectory(), isDirectory: true)
            .appendingPathComponent(UUID().uuidString, isDirectory: true)
        try FileManager.default.createDirectory(at: tempDirectory, withIntermediateDirectories: true)
        try Data("sample-data\n".utf8).write(to: tempDirectory.appendingPathComponent("sample.txt"))

        let (clientTransport, agentTransport) = await InMemoryTransport.createConnectedPair()
        try await clientTransport.connect()
        let observations = AgentObservations()
        let agentTask = startPromptingAgent(on: agentTransport, observations: observations)
        defer { agentTask.cancel() }

        let client = Client(name: "Tests", version: "1.0.0", capabilities: .init(fs: .init(readTextFile: true, writeTextFile: true)))
        await client.setDelegate(DefaultClientDelegate(rootDirectory: tempDirectory, permissionStrategy: .autoApprove))
        _ = try await withTestTimeout(seconds: 5, label: "connect") { try await client.connect(transport: clientTransport) }
        try? await Task.sleep(for: .milliseconds(300))

        #expect(await observations.permissionOptionID == "allow-once")
        #expect(await observations.fileReadContent?.contains("sample-data") == true)
        #if !os(iOS) && !os(tvOS) && !os(watchOS) && !os(visionOS)
        #expect(await observations.terminalOutput?.contains("agent-terminal") == true)
        #endif
        await client.disconnect()
    }

    #if !os(iOS) && !os(tvOS) && !os(watchOS) && !os(visionOS)
    @Test
    func stdio_transport_process_is_terminated_after_prompt_timeout_and_disconnect() async throws {
        guard let python = try? findPython3() else {
            return
        }
        let sleepy = try fixturePath(named: "sleepy_acp_agent.py")
        let transport = StdioTransport(configuration: .init(
            executablePath: python,
            arguments: [sleepy.path],
            workingDirectory: FileManager.default.currentDirectoryPath
        ))
        let client = Client(name: "Tests", version: "1.0.0")
        _ = try await withTestTimeout(seconds: 5, label: "connect") { try await client.connect(transport: transport, timeout: .seconds(3)) }
        let session = try await withTestTimeout(seconds: 5, label: "newSession") { try await client.newSession(cwd: FileManager.default.currentDirectoryPath, timeout: .seconds(3)) }
        let pid = await transport.processID
        do {
            _ = try await client.prompt(sessionID: session.sessionID, prompt: [.text("hang")], timeout: .milliseconds(200))
            Issue.record("expected timeout")
        } catch let error as ClientError {
            #expect(error.description.contains("Timed out"))
        }
        await client.disconnect()
        try await Task.sleep(for: .milliseconds(200))
        if let pid {
            #expect(isProcessGone(pid))
        }
    }

    @Test
    func live_stdio_agent_test_is_env_gated() async throws {
        let env = ProcessInfo.processInfo.environment
        guard env["RUN_OMNIACP_LIVE_STDIO_TESTS"] == "1",
              let binary = env["OMNIACP_LIVE_AGENT_BIN"], !binary.isEmpty else {
            return
        }
        let args = (env["OMNIACP_LIVE_AGENT_ARGS"] ?? "")
            .split(separator: " ")
            .map(String.init)
        let transport = StdioTransport(configuration: .init(
            executablePath: binary,
            arguments: args,
            workingDirectory: env["OMNIACP_LIVE_AGENT_CWD"] ?? FileManager.default.currentDirectoryPath
        ))
        let client = Client(name: "Tests", version: "1.0.0")
        _ = try await client.connect(transport: transport, timeout: .seconds(10))
        await client.disconnect()
    }
    #endif
}

#if !os(iOS) && !os(tvOS) && !os(watchOS) && !os(visionOS)
private func fixturePath(named fileName: String) throws -> URL {
    let url = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent()
        .appendingPathComponent("Fixtures", isDirectory: true)
        .appendingPathComponent(fileName)
    guard FileManager.default.fileExists(atPath: url.path) else {
        throw NSError(domain: "ClientTests", code: 2, userInfo: [NSLocalizedDescriptionKey: "Missing fixture \(fileName)"])
    }
    return url
}

private func findPython3() throws -> String {
    let candidates = ["/usr/bin/python3", "/opt/homebrew/bin/python3", "/usr/local/bin/python3"]
    for candidate in candidates where FileManager.default.isExecutableFile(atPath: candidate) {
        return candidate
    }
    throw NSError(domain: "ClientTests", code: 3, userInfo: [NSLocalizedDescriptionKey: "python3 not found"])
}

private func isProcessGone(_ pid: Int32) -> Bool {
    #if canImport(Darwin) || canImport(Glibc)
    return kill(pid, 0) != 0
    #else
    return true
    #endif
}
#endif
