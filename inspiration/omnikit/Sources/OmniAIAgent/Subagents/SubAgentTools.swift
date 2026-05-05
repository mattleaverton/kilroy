import Foundation
import OmniAICore

// MARK: - Subagent Tools

public func spawnAgentTool(parentSession: Session) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "spawn_agent",
            description: "Spawn a subagent to handle a scoped task autonomously. The subagent gets its own conversation history but shares the same filesystem.",
            parameters: [
                "type": "object",
                "properties": [
                    "task": ["type": "string", "description": "Natural language task description"],
                    "working_dir": ["type": "string", "description": "Subdirectory to scope the agent to"],
                    "model": ["type": "string", "description": "Model override (default: parent's model)"],
                    "max_turns": ["type": "integer", "description": "Turn limit for the subagent (default: 50)"],
                ] as [String: Any],
                "required": ["task"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let task = args["task"] as? String else {
                throw ToolError.validationError("task is required")
            }

            let currentDepth = await parentSession.currentDepth()
            let maxDepth = await parentSession.config.maxSubagentDepth

            guard currentDepth < maxDepth else {
                return "Error: Maximum subagent depth (\(maxDepth)) reached. Cannot spawn sub-sub-agents."
            }

            let workingDir = args["working_dir"] as? String
            let maxTurns = args["max_turns"] as? Int ?? 50

            // Create subagent session with same profile and env
            let subEnv: ExecutionEnvironment
            if let workingDir = workingDir {
                subEnv = LocalExecutionEnvironment(workingDir: workingDir)
            } else {
                subEnv = env
            }

            let profile = parentSession.providerProfile
            let client = parentSession.llmClient
            var subConfig = SessionConfig(maxTurns: maxTurns)
            subConfig.reasoningEffort = await parentSession.config.reasoningEffort

            let subSession = try Session(
                profile: profile,
                environment: subEnv,
                client: client,
                config: subConfig,
                depth: currentDepth + 1
            )

            let handle = SubAgentHandle(
                id: UUID().uuidString,
                session: subSession
            )

            await parentSession.registerSubagent(handle)

            // Start the subagent asynchronously. Intentionally fire-and-forget:
            // the subagent runs independently and is retrieved via 'wait' or
            // 'close_agent' tools. submit() does not throw.
            Task {
                await subSession.submit(task)
            }

            return "Subagent spawned with ID: \(handle.id). Use 'wait' tool with this ID to get results."
        }
    )
}

public func sendInputTool(parentSession: Session) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "send_input",
            description: "Send a message to a running subagent.",
            parameters: [
                "type": "object",
                "properties": [
                    "agent_id": ["type": "string", "description": "The subagent's ID"],
                    "message": ["type": "string", "description": "Message to send"],
                ] as [String: Any],
                "required": ["agent_id", "message"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let agentId = args["agent_id"] as? String else {
                throw ToolError.validationError("agent_id is required")
            }
            guard let message = args["message"] as? String else {
                throw ToolError.validationError("message is required")
            }

            guard let handle = await parentSession.getSubagent(agentId) else {
                return "Error: No subagent found with ID \(agentId)"
            }

            await handle.session.followUp(message)
            return "Message sent to subagent \(agentId)"
        }
    )
}

public func waitTool(parentSession: Session) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "wait",
            description: "Wait for a subagent to complete and return its result.",
            parameters: [
                "type": "object",
                "properties": [
                    "agent_id": ["type": "string", "description": "The subagent's ID"],
                ] as [String: Any],
                "required": ["agent_id"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let agentId = args["agent_id"] as? String else {
                throw ToolError.validationError("agent_id is required")
            }

            guard let handle = await parentSession.getSubagent(agentId) else {
                return "Error: No subagent found with ID \(agentId)"
            }

            // Wait for subagent to reach idle or closed state
            var attempts = 0
            while attempts < 600 { // Up to ~10 minutes
                let state = await handle.session.getState()
                if state == .idle || state == .closed {
                    break
                }
                try await Task.sleep(nanoseconds: 1_000_000_000) // 1 second
                attempts += 1
            }

            let history = await handle.session.getHistory()
            let state = await handle.session.getState()

            // Extract final output
            var output = ""
            for turn in history.reversed() {
                if case .assistant(let t) = turn, !t.content.isEmpty {
                    output = t.content
                    break
                }
            }

            let turnsUsed = history.count
            let success = state == .idle

            return """
            Subagent \(agentId) \(success ? "completed" : "failed").
            Turns used: \(turnsUsed)
            Output: \(output)
            """
        }
    )
}

public func closeAgentTool(parentSession: Session) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "close_agent",
            description: "Terminate a subagent.",
            parameters: [
                "type": "object",
                "properties": [
                    "agent_id": ["type": "string", "description": "The subagent's ID"],
                ] as [String: Any],
                "required": ["agent_id"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let agentId = args["agent_id"] as? String else {
                throw ToolError.validationError("agent_id is required")
            }

            guard let handle = await parentSession.getSubagent(agentId) else {
                return "Error: No subagent found with ID \(agentId)"
            }

            await handle.session.close()
            await parentSession.removeSubagent(agentId)
            return "Subagent \(agentId) terminated."
        }
    )
}
