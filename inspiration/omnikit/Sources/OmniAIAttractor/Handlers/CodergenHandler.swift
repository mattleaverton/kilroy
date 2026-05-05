import Foundation

// MARK: - Codergen Backend Protocol

public protocol CodergenBackend: Sendable {
    func run(
        prompt: String,
        model: String,
        provider: String,
        reasoningEffort: String,
        context: PipelineContext
    ) async throws -> CodergenResult
}

// MARK: - Codergen Result

public struct CodergenResult: Sendable {
    public var response: String
    public var status: OutcomeStatus
    public var contextUpdates: [String: String]
    public var preferredLabel: String
    public var suggestedNextIds: [String]
    public var notes: String

    public init(
        response: String,
        status: OutcomeStatus = .success,
        contextUpdates: [String: String] = [:],
        preferredLabel: String = "",
        suggestedNextIds: [String] = [],
        notes: String = ""
    ) {
        self.response = response
        self.status = status
        self.contextUpdates = contextUpdates
        self.preferredLabel = preferredLabel
        self.suggestedNextIds = suggestedNextIds
        self.notes = notes
    }
}

// MARK: - Codergen Handler

public final class CodergenHandler: NodeHandler, Sendable {
    public let handlerType: HandlerType = .codergen
    private let llmBackend: CodergenBackend

    public init(backend: CodergenBackend) {
        self.llmBackend = backend
    }

    public func execute(
        node: Node,
        context: PipelineContext,
        graph: Graph,
        logsRoot: URL
    ) async throws -> Outcome {
        // 1. Build prompt
        let prompt = node.prompt.isEmpty ? node.label : node.prompt

        // 2. Create stage directory
        let stageDir = logsRoot.appendingPathComponent(node.id)
        try FileManager.default.createDirectory(at: stageDir, withIntermediateDirectories: true)
        context.set("_current_node_id", node.id)
        context.set("_current_stage_dir", stageDir.path)

        // 3. Write prompt
        let promptFile = stageDir.appendingPathComponent("prompt.md")
        try Data(prompt.utf8).write(to: promptFile)

        // 4. Execute pre-hooks (spec §9.7: exit code 0 means proceed; non-zero skips the tool call)
        if !graph.attributes.toolHooksPre.isEmpty {
            do {
                let hookResult = try runShellHook(graph.attributes.toolHooksPre, nodeId: node.id)
                context.set("hook_pre_output", hookResult)
            } catch {
                // Pre-hook returned non-zero: skip the LLM call and return FAIL
                context.set("hook_pre_output", "FAILED: \(error)")
                return Outcome.fail(reason: "Pre-hook blocked execution for node \(node.id): \(error)")
            }
        }

        // Also check node-level tool_hooks.pre override
        if let nodePreHook = node.rawAttributes["tool_hooks.pre"]?.stringValue, !nodePreHook.isEmpty {
            do {
                let hookResult = try runShellHook(nodePreHook, nodeId: node.id)
                context.set("hook_pre_output", hookResult)
            } catch {
                context.set("hook_pre_output", "FAILED: \(error)")
                return Outcome.fail(reason: "Node pre-hook blocked execution for node \(node.id): \(error)")
            }
        }

        // 5. Call LLM backend
        let model = node.llmModel.isEmpty ? "claude-sonnet-4-5-20250929" : node.llmModel
        let provider = node.llmProvider.isEmpty ? "anthropic" : node.llmProvider
        let reasoningEffort = node.reasoningEffort.isEmpty ? "high" : node.reasoningEffort

        // Pass node-level agent config to context for CodingAgentBackend.
        if let maxTurns = node.rawAttributes["max_agent_turns"]?.intValue {
            context.set("_current_node_max_agent_turns", String(maxTurns))
        }
        if let defaultTimeout = node.rawAttributes["default_command_timeout_ms"]?.intValue {
            context.set("_current_node_default_command_timeout_ms", String(defaultTimeout))
        }
        if let maxCommandTimeout = node.rawAttributes["max_command_timeout_ms"]?.intValue {
            context.set("_current_node_max_command_timeout_ms", String(maxCommandTimeout))
        }
        if let inactivityTimeout = node.rawAttributes["llm_inactivity_timeout_seconds"] {
            context.set("_current_node_llm_inactivity_timeout_seconds", inactivityTimeout.stringValue)
        }
        if let loopWindow = node.rawAttributes["loop_detection_window"]?.intValue {
            context.set("_current_node_loop_detection_window", String(loopWindow))
        }
        if let userInstructions = node.rawAttributes["user_instructions"]?.stringValue, !userInstructions.isEmpty {
            context.set("_current_node_user_instructions", userInstructions)
        }
        if let parallelToolCalls = node.rawAttributes["parallel_tool_calls"]?.boolValue {
            context.set("_current_node_parallel_tool_calls", parallelToolCalls ? "true" : "false")
        }
        if let excludedTools = node.rawAttributes["excluded_tools"]?.stringValue, !excludedTools.isEmpty {
            context.set("_current_node_excluded_tools", excludedTools)
        }
        if let artifactPath = node.rawAttributes["artifact_path"]?.stringValue, !artifactPath.isEmpty {
            context.set("_current_node_artifact_path", artifactPath)
        }
        if let acpAgentPath = (node.rawAttributes["acp_agent_path"]?.stringValue).flatMap({ $0.isEmpty ? nil : $0 })
            ?? (graph.rawAttributes["acp_agent_path"]?.stringValue).flatMap({ $0.isEmpty ? nil : $0 })
        {
            context.set("_current_node_acp_agent_path", acpAgentPath)
        }
        if let acpAgentArgs = (node.rawAttributes["acp_agent_args"]?.stringValue).flatMap({ $0.isEmpty ? nil : $0 })
            ?? (graph.rawAttributes["acp_agent_args"]?.stringValue).flatMap({ $0.isEmpty ? nil : $0 })
        {
            context.set("_current_node_acp_agent_args", acpAgentArgs)
        }
        if let acpWorkingDirectory = (node.rawAttributes["acp_cwd"]?.stringValue).flatMap({ $0.isEmpty ? nil : $0 })
            ?? (graph.rawAttributes["acp_cwd"]?.stringValue).flatMap({ $0.isEmpty ? nil : $0 })
        {
            context.set("_current_node_acp_cwd", acpWorkingDirectory)
        }
        if let acpTimeout = node.rawAttributes["acp_timeout_seconds"]?.stringValue
            ?? graph.rawAttributes["acp_timeout_seconds"]?.stringValue
        {
            context.set("_current_node_acp_timeout_seconds", acpTimeout)
        }
        if let acpMode = (node.rawAttributes["acp_mode"]?.stringValue).flatMap({ $0.isEmpty ? nil : $0 })
            ?? (graph.rawAttributes["acp_mode"]?.stringValue).flatMap({ $0.isEmpty ? nil : $0 })
        {
            context.set("_current_node_acp_mode", acpMode)
        }
        let resumeKey = buildNodeResumeKey(graphID: graph.id, nodeID: node.id, provider: provider, model: model, prompt: prompt)
        context.set("_current_node_resume_key", resumeKey)

        let result: CodergenResult
        do {
            result = try await llmBackend.run(
                prompt: prompt,
                model: model,
                provider: provider,
                reasoningEffort: reasoningEffort,
                context: context
            )
        } catch {
            throw AttractorError.llmError("LLM call failed for node \(node.id): \(error)")
        }

        // 6. Write response
        let responseFile = stageDir.appendingPathComponent("response.md")
        try Data(result.response.utf8).write(to: responseFile)

        // Host-owned artifact persistence for deterministic downstream handoff.
        if let artifactPath = node.rawAttributes["artifact_path"]?.stringValue, !artifactPath.isEmpty {
            let artifactURL: URL
            if artifactPath.hasPrefix("/") {
                artifactURL = URL(fileURLWithPath: artifactPath)
            } else {
                let cwd = URL(fileURLWithPath: FileManager.default.currentDirectoryPath, isDirectory: true)
                artifactURL = cwd.appendingPathComponent(artifactPath)
            }
            try FileManager.default.createDirectory(
                at: artifactURL.deletingLastPathComponent(),
                withIntermediateDirectories: true
            )
            try Data(result.response.utf8).write(to: artifactURL)
            context.set("artifact.\(node.id).path", artifactURL.path)
        }

        // 7. Execute post-hooks (spec §9.7: post-hook failures are logged but do not block execution)
        if !graph.attributes.toolHooksPost.isEmpty {
            do {
                let hookResult = try runShellHook(graph.attributes.toolHooksPost, nodeId: node.id)
                context.set("hook_post_output", hookResult)
            } catch {
                // Post-hook failure is recorded but does not block execution
                context.set("hook_post_output", "FAILED: \(error)")
            }
        }

        // Also check node-level tool_hooks.post override
        if let nodePostHook = node.rawAttributes["tool_hooks.post"]?.stringValue, !nodePostHook.isEmpty {
            do {
                let hookResult = try runShellHook(nodePostHook, nodeId: node.id)
                context.set("hook_post_output", hookResult)
            } catch {
                context.set("hook_post_output", "FAILED: \(error)")
            }
        }

        // 8. Determine status
        var status = result.status
        if status == .fail && node.autoStatus {
            status = .success
        }

        // 9. Store response in context for downstream stages
        var updates = result.contextUpdates
        updates["last_response"] = String(result.response.prefix(8000))
        updates["last_stage"] = node.id

        // 10. Write status.json
        let outcome = Outcome(
            status: status,
            preferredLabel: result.preferredLabel,
            suggestedNextIds: result.suggestedNextIds,
            contextUpdates: updates,
            notes: result.notes
        )
        let statusJSON = outcome.toStatusJSON()
        let statusData = try JSONSerialization.data(withJSONObject: statusJSON, options: [.prettyPrinted, .sortedKeys])
        let statusFile = stageDir.appendingPathComponent("status.json")
        try statusData.write(to: statusFile)

        return outcome
    }

    private func buildNodeResumeKey(
        graphID: String,
        nodeID: String,
        provider: String,
        model: String,
        prompt: String
    ) -> String {
        let raw = "\(graphID)|\(nodeID)|\(provider)|\(model)|\(prompt)"
        let hash = fnv1a64(raw)
        return "\(graphID).\(nodeID).\(provider).\(model).\(hash)"
    }

    private func fnv1a64(_ value: String) -> String {
        let offsetBasis: UInt64 = 14695981039346656037
        let prime: UInt64 = 1099511628211
        var hash = offsetBasis
        for b in value.utf8 {
            hash ^= UInt64(b)
            hash = hash &* prime
        }
        return String(hash, radix: 16)
    }

    // MARK: - Shell Hook Execution

    // Note: Blocks a cooperative thread while waiting for the hook process.
    // Stdout and stderr are drained concurrently to avoid pipe deadlocks.
    private func runShellHook(_ script: String, nodeId: String) throws -> String {
        let process = Process()
        let stdout = Pipe()
        let stderr = Pipe()
        let stdoutData = _ShellHookDataBox()
        let stderrData = _ShellHookDataBox()

        process.executableURL = URL(fileURLWithPath: "/bin/sh")
        process.arguments = ["-c", script]
        process.standardOutput = stdout
        process.standardError = stderr

        var env = ProcessInfo.processInfo.environment
        env["ATTRACTOR_NODE_ID"] = nodeId
        process.environment = env

        let stdoutQueue = DispatchQueue(label: "attractor.hook.stdout")
        let stderrQueue = DispatchQueue(label: "attractor.hook.stderr")
        stdoutQueue.async {
            stdoutData.store(stdout.fileHandleForReading.readDataToEndOfFile())
        }
        stderrQueue.async {
            stderrData.store(stderr.fileHandleForReading.readDataToEndOfFile())
        }

        do {
            try process.run()
        } catch {
            try? stdout.fileHandleForWriting.close()
            try? stderr.fileHandleForWriting.close()
            stdoutQueue.sync {}
            stderrQueue.sync {}
            throw error
        }
        process.waitUntilExit()
        stdoutQueue.sync {}
        stderrQueue.sync {}

        let outStr = String(data: stdoutData.load(), encoding: .utf8) ?? ""
        let errStr = String(data: stderrData.load(), encoding: .utf8) ?? ""

        if process.terminationStatus != 0 {
            throw AttractorError.executionFailed(
                "Tool hook failed for node \(nodeId) (exit \(process.terminationStatus)): \(errStr)"
            )
        }

        return outStr.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines)
    }
}

private final class _ShellHookDataBox: @unchecked Sendable {
    private let lock = NSLock()
    private var data = Data()

    func store(_ data: Data) {
        lock.lock()
        self.data = data
        lock.unlock()
    }

    func load() -> Data {
        lock.lock()
        defer { lock.unlock() }
        return data
    }
}
