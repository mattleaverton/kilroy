import Foundation
import OmniAICore
import OmniMCP

private struct EmptyStreamResponseError: Error {}

public struct GitContext: Sendable {
    public var branch: String?
    public var modifiedFileCount: Int = 0
    public var recentCommits: String?
}

public actor Session {
    public let id: String
    public let providerProfile: ProviderProfile
    public let executionEnv: ExecutionEnvironment
    public private(set) var history: [Turn] = []
    public let eventEmitter: EventEmitter
    public var config: SessionConfig
    public private(set) var state: SessionState = .idle
    public let llmClient: Client
    private var steeringQueue: [String] = []
    private var followupQueue: [String] = []
    private var responseTimeline: [ResponseTimelineEntry] = []
    private var pendingTimelineTurns: [PersistedTurn] = []
    private var mcpServers: [any MCPServer] = []
    private var mcpToolNames: Set<String> = []
    private var mcpInitialized = false
    private var subagents: [String: SubAgentHandle] = [:]
    public private(set) var abortSignaled: Bool = false
    private let depth: Int
    private var processingTask: Task<Void, Never>?
    private let storageBackend: (any SessionStorageBackend)?
    private let autoRestoreFromStorage: Bool
    private var didAttemptStorageRestore = false
    private var lastToolCallSignature: String?
    private var consecutiveIdenticalToolCallCount: Int = 0
    private var cachedProjectDocs: String?

    public init(
        profile: ProviderProfile,
        environment: ExecutionEnvironment,
        client: Client? = nil,
        config: SessionConfig = SessionConfig(),
        depth: Int = 0,
        sessionID: String = UUID().uuidString,
        storageBackend: (any SessionStorageBackend)? = nil,
        autoRestoreFromStorage: Bool = true
    ) throws {
        self.id = sessionID
        self.providerProfile = profile
        self.executionEnv = environment
        self.config = config
        self.eventEmitter = EventEmitter()
        self.depth = depth
        self.storageBackend = storageBackend
        self.autoRestoreFromStorage = autoRestoreFromStorage
        let mcpPolicy = config.mcp.connectionPolicy
        self.mcpServers = try config.mcp.servers.map { config in
            try MCPServerFactory.makeServer(config: config, policy: mcpPolicy)
        }

        if let client = client {
            self.llmClient = client
        } else {
            self.llmClient = try Client.fromEnv()
        }
    }

    // MARK: - Public API

    public func submit(_ input: String) async {
        // Call processInput directly on the actor to avoid a deadlock:
        // wrapping in Task { await self.processInput() } + await task.value
        // deadlocks because the Task re-enters the same actor serial executor
        // that is already suspended waiting for task.value.
        await processInput(input)
        await persistStateIfNeeded()
    }

    public func updateConfig(_ mutator: (inout SessionConfig) -> Void) async {
        mutator(&config)
        do {
            let oldServers = mcpServers
            mcpServers = try config.mcp.servers.map { config in
                try MCPServerFactory.makeServer(config: config, policy: self.config.mcp.connectionPolicy)
            }
            mcpToolNames.removeAll()
            mcpInitialized = false
            for server in oldServers {
                await server.cleanup()
            }
        } catch {
            await eventEmitter.emit(SessionEvent(
                kind: .warning,
                sessionId: id,
                data: ["message": "Failed to update MCP configuration: \(error)"]
            ))
        }
        await persistStateIfNeeded()
    }

    public func steer(_ message: String) async {
        steeringQueue.append(message)
        await persistStateIfNeeded()
    }

    public func followUp(_ message: String) async {
        followupQueue.append(message)
        await persistStateIfNeeded()
    }

    public func abort() async {
        abortSignaled = true
        state = .closed
        processingTask?.cancel()
        for (_, handle) in subagents {
            await handle.session.abort()
        }
        subagents.removeAll()
        await cleanupMCPServers()
        await persistStateIfNeeded()
        try? await executionEnv.cleanup()
    }

    public func close() async {
        abortSignaled = true
        state = .closed
        processingTask?.cancel()
        for (_, handle) in subagents {
            await handle.session.close()
        }
        subagents.removeAll()
        await cleanupMCPServers()
        await eventEmitter.emit(SessionEvent(kind: .sessionEnd, sessionId: id, data: ["state": "closed"]))
        await eventEmitter.flush()
        await persistStateIfNeeded()
        try? await executionEnv.cleanup()
    }

    public func addSystemReminder(_ reminder: String) async {
        appendTurn(.system(SystemTurn(content: wrapSystemReminder(reminder))))
        await persistStateIfNeeded()
    }

    public func getState() -> SessionState {
        state
    }

    public func getHistory() -> [Turn] {
        history
    }

    public func listResponseTimeline() -> [ResponseTimelineEntry] {
        responseTimeline
    }

    public func rewind(toResponseID responseId: String) async throws {
        guard let index = responseTimeline.firstIndex(where: { $0.responseId == responseId }) else {
            throw SessionTimelineError.responseNotFound(responseId)
        }

        let truncatedTimeline = Array(responseTimeline.prefix(index + 1))
        responseTimeline = truncatedTimeline
        pendingTimelineTurns.removeAll()
        history = rebuildHistory(from: truncatedTimeline, pending: [])
        steeringQueue.removeAll()
        followupQueue.removeAll()
        abortSignaled = false
        lastToolCallSignature = nil
        consecutiveIdenticalToolCallCount = 0
        state = .idle

        await persistStateIfNeeded()
    }

    public func workingDirectory() -> String {
        executionEnv.workingDirectory()
    }

    @discardableResult
    public func restoreFromStorage() async throws -> Bool {
        didAttemptStorageRestore = true
        guard let storageBackend else { return false }
        guard let snapshot = try await storageBackend.load(sessionID: id) else {
            return false
        }
        if snapshot.providerID != providerProfile.id || snapshot.model != providerProfile.model {
            await eventEmitter.emit(SessionEvent(
                kind: .warning,
                sessionId: id,
                data: [
                    "message": "Restored session was created for \(snapshot.providerID)/\(snapshot.model) but current session is \(providerProfile.id)/\(providerProfile.model).",
                ]
            ))
        }
        responseTimeline = snapshot.responseTimeline
        pendingTimelineTurns = snapshot.pendingTimelineTurns
        if !responseTimeline.isEmpty || !pendingTimelineTurns.isEmpty {
            history = rebuildHistory(from: responseTimeline, pending: pendingTimelineTurns)
        } else {
            history = snapshot.history.map { $0.toTurn() }
            let rebuilt = rebuildTimeline(from: history)
            responseTimeline = rebuilt.timeline
            pendingTimelineTurns = rebuilt.pending
        }
        steeringQueue = snapshot.steeringQueue
        followupQueue = snapshot.followupQueue
        config = snapshot.config
        do {
            mcpServers = try config.mcp.servers.map { config in
                try MCPServerFactory.makeServer(config: config, policy: self.config.mcp.connectionPolicy)
            }
            mcpToolNames.removeAll()
            mcpInitialized = false
        } catch {
            await eventEmitter.emit(SessionEvent(
                kind: .warning,
                sessionId: id,
                data: ["message": "Failed to restore MCP configuration: \(error)"]
            ))
        }
        state = snapshot.state
        abortSignaled = snapshot.abortSignaled
        await eventEmitter.emit(SessionEvent(
            kind: .warning,
            sessionId: id,
            data: ["message": "Session restored from storage (\(history.count) turns)"]
        ))
        return true
    }

    public func persistNow() async {
        await persistStateIfNeeded()
    }

    public func clearPersistedState() async throws {
        guard let storageBackend else { return }
        try await storageBackend.delete(sessionID: id)
    }

    // MARK: - Core Agentic Loop

    private func processInput(_ userInput: String) async {
        fputs("[Session] processInput START (\(userInput.count) chars)\n", stderr)
        fputs("[Session] step: restoreFromStorage check\n", stderr)
        if autoRestoreFromStorage && !didAttemptStorageRestore {
            do {
                _ = try await restoreFromStorage()
            } catch {
                await eventEmitter.emit(SessionEvent(
                    kind: .warning,
                    sessionId: id,
                    data: ["message": "Failed to restore session from storage: \(error)"]
                ))
            }
        }
        await loadMCPToolsIfNeeded()
        fputs("[Session] step: recoverPendingToolCalls\n", stderr)
        await recoverPendingToolCallsIfNeeded()

        // Emit SESSION_START on first input
        if history.isEmpty {
            await eventEmitter.emit(SessionEvent(kind: .sessionStart, sessionId: id))
        }
        let resumeContinuation = shouldTreatAsResumeContinuation(userInput: userInput)
        state = .processing
        if !resumeContinuation {
            appendTurn(.user(UserTurn(content: userInput)))
            await persistStateIfNeeded()
            await eventEmitter.emit(SessionEvent(kind: .userInput, sessionId: id, data: ["content": userInput]))
        } else {
            await eventEmitter.emit(SessionEvent(
                kind: .warning,
                sessionId: id,
                data: ["message": "Resuming in-progress session state for repeated prompt"]
            ))
        }

        fputs("[Session] step: drainSteering\n", stderr)
        await drainSteering()

        var roundCount = 0

        while true {
            // 1. Check limits
            if config.maxToolRoundsPerInput > 0 && roundCount >= config.maxToolRoundsPerInput {
                await eventEmitter.emit(SessionEvent(kind: .turnLimit, sessionId: id, data: ["round": roundCount]))
                break
            }

            if config.maxTurns > 0 && countTurns() >= config.maxTurns {
                await eventEmitter.emit(SessionEvent(kind: .turnLimit, sessionId: id, data: ["total_turns": countTurns()]))
                break
            }

            if abortSignaled {
                break
            }

            // 2. Build LLM request
            // Cache project docs after first discovery to avoid repeated subprocess
            // calls (git rev-parse, file reads) that cause scheduling pressure under
            // parallel session execution.
            if cachedProjectDocs == nil {
                fputs("[Session] step: discoverProjectDocs\n", stderr)
                cachedProjectDocs = await discoverProjectDocs(workingDir: executionEnv.workingDirectory()) ?? ""
            }
            let projectDocs: String? = cachedProjectDocs?.isEmpty == true ? nil : cachedProjectDocs
            fputs("[Session] step: gatherGitContext\n", stderr)
            // Skip git context gathering in non-interactive mode to avoid intermittent
            // subprocess hangs that stall the pipeline. Git context is decorative only.
            let gitCtx: GitContext? = config.interactiveMode
                ? await gatherGitContext()
                : nil
            fputs("[Session] step: buildSystemPrompt\n", stderr)
            let systemPrompt = providerProfile.buildSystemPrompt(
                environment: executionEnv,
                projectDocs: projectDocs,
                userInstructions: config.userInstructions,
                gitContext: gitCtx
            )
            let toolDefs = providerProfile.tools()
            let previousResponseId = providerProfile.id == "openai"
                ? latestAssistantResponseId()
                : nil
            let messages: [Message]
            if let prevId = previousResponseId {
                // When resuming via previous_response_id, only send turns added
                // after the assistant turn that produced that response. The server
                // already has everything up to and including that response.
                messages = convertIncrementalMessages(
                    systemPrompt: systemPrompt,
                    afterResponseId: prevId
                )
            } else {
                messages = convertHistoryToMessages(systemPrompt: systemPrompt)
            }

            let request = Request(
                model: providerProfile.model,
                messages: messages,
                provider: providerProfile.id,
                previousResponseId: previousResponseId,
                tools: toolDefs.isEmpty ? nil : toolDefs,
                toolChoice: toolDefs.isEmpty ? nil : ToolChoice.auto,
                reasoningEffort: config.reasoningEffort,
                providerOptions: providerProfile.providerOptions()
            )

            // 3. Call LLM
            fputs("[Session] Calling LLM: \(providerProfile.model) via \(providerProfile.id) (round \(roundCount), \(messages.count) messages, \(toolDefs.count) tools)\n", stderr)
            let response: Response
            do {
                response = try await completeWithTransientRetries(request: request)
                fputs("[Session] LLM returned: \(response.text.prefix(100))... (\(response.toolCalls.count) tool calls)\n", stderr)
            } catch {
                let errorDetail: String
                if let sdkError = error as? SDKError {
                    errorDetail = "\(type(of: sdkError)): \(sdkError.message)"
                } else {
                    errorDetail = "\(error)"
                }
                await eventEmitter.emit(SessionEvent(kind: .error, sessionId: id, data: ["error": errorDetail]))
                if error is AuthenticationError || error is ContextLengthError {
                    state = .closed
                    await persistStateIfNeeded()
                    await eventEmitter.emit(SessionEvent(kind: .sessionEnd, sessionId: id, data: ["state": "closed", "error": "\(error)"]))
                    return
                }
                break
            }

            // 4. Record assistant turn
            let resolvedResponseId = resolveResponseId(response)
            let assistantTurn = AssistantTurn(
                content: response.text,
                toolCalls: response.toolCalls,
                reasoning: response.reasoning,
                rawContentParts: response.message.content,
                usage: response.usage,
                responseId: resolvedResponseId
            )
            appendTurn(.assistant(assistantTurn))
            await persistStateIfNeeded()
            await eventEmitter.emit(SessionEvent(
                kind: .assistantTextStart,
                sessionId: id,
                data: [
                    "response_id": resolvedResponseId,
                    "tool_call_count": response.toolCalls.count,
                ]
            ))
            if !response.text.isEmpty {
                await eventEmitter.emit(SessionEvent(
                    kind: .assistantTextDelta,
                    sessionId: id,
                    data: [
                        "text": response.text,
                        "delta": response.text,
                    ]
                ))
            }
            await eventEmitter.emit(SessionEvent(
                kind: .assistantTextEnd,
                sessionId: id,
                data: [
                    "text": response.text,
                    "reasoning": response.reasoning ?? "",
                    "tool_call_count": response.toolCalls.count,
                ]
            ))

            // 5. If no tool calls, natural completion
            if response.toolCalls.isEmpty {
                finalizeTimelineEntry(responseId: resolvedResponseId)
                break
            }

            // 6. Execute tool calls
            roundCount += 1
            fputs("[Session] step: executeToolCalls (\(response.toolCalls.count) calls, round \(roundCount))\n", stderr)
            let results = await executeToolCalls(response.toolCalls)
            fputs("[Session] step: toolCalls complete, persisting\n", stderr)
            appendTurn(.toolResults(ToolResultsTurn(results: results)))
            await persistStateIfNeeded()
            fputs("[Session] step: persisted, draining steering\n", stderr)

            // 7. Drain steering
            await drainSteering()

            // 8. Loop detection
            if config.enableLoopDetection {
                if detectLoop(window: config.loopDetectionWindow) {
                    let warning = "Loop detected: the last \(config.loopDetectionWindow) tool calls follow a repeating pattern. Try a different approach."
                    appendTurn(.steering(SteeringTurn(content: warning)))
                    await persistStateIfNeeded()
                    await eventEmitter.emit(SessionEvent(kind: .loopDetection, sessionId: id, data: ["message": warning]))
                }
            }

            finalizeTimelineEntry(responseId: resolvedResponseId)

            // Context window awareness
            fputs("[Session] step: checkContextUsage\n", stderr)
            await checkContextUsage()
            fputs("[Session] step: looping to next LLM call\n", stderr)
        }

        // Process follow-up messages
        if !followupQueue.isEmpty {
            let nextInput = followupQueue.removeFirst()
            await persistStateIfNeeded()
            await processInput(nextInput)
            return
        }

        state = .idle
        await persistStateIfNeeded()
        await eventEmitter.emit(SessionEvent(kind: .sessionEnd, sessionId: id))
    }

    private func recoverPendingToolCallsIfNeeded() async {
        guard let pending = unresolvedToolCallsFromHistory() else { return }

        await eventEmitter.emit(SessionEvent(
            kind: .warning,
            sessionId: id,
            data: [
                "message": "Recovered pending tool-call round from session history (\(pending.count) call(s)).",
            ]
        ))

        let results = await executeToolCalls(pending)
        appendTurn(.toolResults(ToolResultsTurn(results: results)))
        if let responseId = latestAssistantResponseId() {
            finalizeTimelineEntry(responseId: responseId)
        }
        await persistStateIfNeeded()
    }

    private func unresolvedToolCallsFromHistory() -> [ToolCall]? {
        guard !history.isEmpty else { return nil }
        for idx in stride(from: history.count - 1, through: 0, by: -1) {
            let turn = history[idx]
            if case .assistant(let assistant) = turn, !assistant.toolCalls.isEmpty {
                if idx + 1 >= history.count {
                    return assistant.toolCalls
                }
                let tail = history[(idx + 1)...]
                for t in tail {
                    if case .toolResults = t {
                        return nil
                    }
                }
                return assistant.toolCalls
            }
            if case .toolResults = turn {
                return nil
            }
        }
        return nil
    }

    private func shouldTreatAsResumeContinuation(userInput: String) -> Bool {
        guard state == .processing else { return false }
        guard !userInput.isEmpty else { return false }
        guard let latestUser = latestUserInput(), latestUser == userInput else { return false }
        return unresolvedToolCallsFromHistory() != nil || !history.isEmpty
    }

    private func latestUserInput() -> String? {
        for turn in history.reversed() {
            if case .user(let t) = turn {
                return t.content
            }
        }
        return nil
    }

    private func persistStateIfNeeded() async {
        guard let storageBackend else { return }
        let snapshot = SessionSnapshot(
            sessionID: id,
            providerID: providerProfile.id,
            model: providerProfile.model,
            workingDirectory: executionEnv.workingDirectory(),
            state: state,
            history: history.map(PersistedTurn.init),
            responseTimeline: responseTimeline,
            pendingTimelineTurns: pendingTimelineTurns,
            steeringQueue: steeringQueue,
            followupQueue: followupQueue,
            config: config,
            abortSignaled: abortSignaled,
            updatedAt: Date()
        )
        do {
            try await storageBackend.save(snapshot)
        } catch {
            await eventEmitter.emit(SessionEvent(
                kind: .warning,
                sessionId: id,
                data: [
                    "message": "Failed to persist session state: \(error)",
                ]
            ))
        }
    }

    // MARK: - Steering

    private func drainSteering() async {
        while !steeringQueue.isEmpty {
            let msg = steeringQueue.removeFirst()
            appendTurn(.steering(SteeringTurn(content: msg)))
            await persistStateIfNeeded()
            await eventEmitter.emit(SessionEvent(kind: .steeringInjected, sessionId: id, data: ["content": msg]))
        }
    }

    // MARK: - MCP

    private func loadMCPToolsIfNeeded(force: Bool = false) async {
        guard !config.mcp.servers.isEmpty else { return }
        if mcpInitialized && !force { return }

        if mcpServers.isEmpty {
            do {
                mcpServers = try config.mcp.servers.map { config in
                    try MCPServerFactory.makeServer(config: config, policy: self.config.mcp.connectionPolicy)
                }
            } catch {
                await eventEmitter.emit(SessionEvent(
                    kind: .warning,
                    sessionId: id,
                    data: ["message": "Failed to configure MCP servers: \(error)"]
                ))
                return
            }
        }

        var newToolNames: Set<String> = []
        do {
            for server in mcpServers {
                let definitions = try await server.listTools()
                for definition in definitions {
                    let registered = buildMCPRegisteredTool(definition, server: server)
                    providerProfile.toolRegistry.register(registered)
                    newToolNames.insert(definition.name)
                }
            }
            for name in mcpToolNames where !newToolNames.contains(name) {
                providerProfile.toolRegistry.unregister(name)
            }
            mcpToolNames = newToolNames
            mcpInitialized = true
        } catch {
            await eventEmitter.emit(SessionEvent(
                kind: .warning,
                sessionId: id,
                data: ["message": "Failed to load MCP tools: \(error)"]
            ))
        }
    }

    private func buildMCPRegisteredTool(_ definition: MCPToolDefinition, server: MCPServer) -> RegisteredTool {
        let schemaValue = ensureStrictJSONSchema(definition.inputSchema)
        let schemaObject: JSONValue
        if case .object = schemaValue {
            schemaObject = schemaValue
        } else {
            schemaObject = .object([
                "type": .string("object"),
                "properties": .object([:]),
                "additionalProperties": .bool(false),
            ])
        }
        let toolDefinition = AgentToolDefinition(
            name: definition.name,
            description: definition.description,
            parameters: schemaObject
        )
        return RegisteredTool(
            definition: toolDefinition,
            executor: { arguments, _ in
                let jsonArgs = (try? JSONValue(arguments)) ?? .object([:])
                let result = try await server.callTool(name: definition.name, arguments: jsonArgs)
                let rendered = Self.renderMCPToolOutput(result.content)
                if result.isError {
                    throw MCPToolExecutionError(message: rendered)
                }
                return rendered
            }
        )
    }

    private nonisolated static func renderMCPToolOutput(_ value: JSONValue) -> String {
        if case .string(let text) = value {
            return text
        }
        if let data = try? value.data(prettyPrinted: true),
           let string = String(data: data, encoding: .utf8) {
            return string
        }
        return value.description
    }

    private func cleanupMCPServers() async {
        for server in mcpServers {
            await server.cleanup()
        }
    }

    private struct MCPToolExecutionError: Error, Sendable {
        let message: String
    }

    // MARK: - Tool Execution

    private func executeToolCalls(_ toolCalls: [ToolCall]) async -> [ToolResult] {
        // Pre-check: update duplicate-call tracking (actor-isolated state) serially
        // before entering the task group. This avoids actor reentrancy in the parallel path.
        var policyBlocked: [String: String] = [:] // toolCall.id -> denial message
        let policyDenial = "same tool run five times in a row. denied by policy. try something else"
        for toolCall in toolCalls {
            let signature = toolCallSignature(toolCall)
            if signature == lastToolCallSignature {
                consecutiveIdenticalToolCallCount += 1
            } else {
                lastToolCallSignature = signature
                consecutiveIdenticalToolCallCount = 1
            }
            if consecutiveIdenticalToolCallCount >= 6 {
                policyBlocked[toolCall.id] = policyDenial
            }
        }

        let shouldParallel = toolCalls.count > 1
            && providerProfile.supportsParallelToolCalls
            && config.parallelToolCalls == true

        // Capture actor-isolated state into local lets for use in nonisolated task group.
        let registry = providerProfile.toolRegistry
        let env = executionEnv
        let cfg = config
        let emitter = eventEmitter
        let sid = id

        if shouldParallel {
            return await withTaskGroup(of: (Int, ToolResult).self, returning: [ToolResult].self) { group in
                for (index, toolCall) in toolCalls.enumerated() {
                    let blocked = policyBlocked[toolCall.id]
                    group.addTask {
                        // Runs outside Session actor — no reentrancy.
                        let result = await Self.executeSingleToolNonisolated(
                            toolCall: toolCall,
                            policyBlocked: blocked,
                            registry: registry,
                            env: env,
                            config: cfg,
                            eventEmitter: emitter,
                            sessionID: sid
                        )
                        return (index, result)
                    }
                }

                var collected: [(Int, ToolResult)] = []
                for await tuple in group {
                    collected.append(tuple)
                }
                return collected.sorted { $0.0 < $1.0 }.map(\.1)
            }
        }

        // Serial fallback
        var results: [ToolResult] = []
        results.reserveCapacity(toolCalls.count)
        for toolCall in toolCalls {
            let blocked = policyBlocked[toolCall.id]
            let result = await Self.executeSingleToolNonisolated(
                toolCall: toolCall,
                policyBlocked: blocked,
                registry: registry,
                env: env,
                config: cfg,
                eventEmitter: emitter,
                sessionID: sid
            )
            results.append(result)
        }
        return results
    }

    /// Executes a single tool call without requiring Session actor isolation.
    /// All needed state is passed as parameters so this can run in a task group
    /// child task without re-entering the Session actor (which would deadlock).
    private static func executeSingleToolNonisolated(
        toolCall: ToolCall,
        policyBlocked: String?,
        registry: ToolRegistry,
        env: any ExecutionEnvironment,
        config: SessionConfig,
        eventEmitter: EventEmitter,
        sessionID: String
    ) async -> ToolResult {
        fputs("[Session] executeSingleToolNonisolated: START \(toolCall.name) (\(toolCall.id))\n", stderr)
        await eventEmitter.emit(SessionEvent(
            kind: .toolCallStart,
            sessionId: sessionID,
            data: [
                "tool": toolCall.name,
                "tool_name": toolCall.name,
                "call_id": toolCall.id,
            ]
        ))

        if let denial = policyBlocked {
            await eventEmitter.emit(SessionEvent(
                kind: .toolCallEnd,
                sessionId: sessionID,
                data: [
                    "call_id": toolCall.id,
                    "error": denial,
                    "policy_blocked": true,
                    "tool": toolCall.name,
                    "tool_name": toolCall.name,
                ]
            ))
            return ToolResult(toolCallId: toolCall.id, content: .string(denial), isError: true)
        }

        fputs("[Session] executeSingleToolNonisolated: emitted toolCallStart, looking up \(toolCall.name)\n", stderr)
        guard let registered = registry.get(toolCall.name) else {
            let errorMsg = "Unknown tool: \(toolCall.name)"
            await eventEmitter.emit(SessionEvent(
                kind: .toolCallEnd,
                sessionId: sessionID,
                data: ["call_id": toolCall.id, "error": errorMsg, "tool": toolCall.name, "tool_name": toolCall.name]
            ))
            return ToolResult(toolCallId: toolCall.id, content: .string(errorMsg), isError: true)
        }

        // Validate arguments against tool parameter schema
        do {
            try JSONSchema(registered.definition.parameters).validate(.object(toolCall.arguments))
        } catch {
            let validationError = "Invalid tool arguments: \(error)"
            await eventEmitter.emit(SessionEvent(
                kind: .toolCallEnd,
                sessionId: sessionID,
                data: ["call_id": toolCall.id, "error": validationError, "tool": toolCall.name, "tool_name": toolCall.name]
            ))
            return ToolResult(toolCallId: toolCall.id, content: .string(validationError), isError: true)
        }

        let foundationArguments: [String: Any]
        do {
            foundationArguments = try toolCall.arguments.mapValues { try $0.asFoundationObject() }
        } catch {
            let conversionError = "Invalid tool arguments for \(toolCall.name): \(error)"
            await eventEmitter.emit(SessionEvent(
                kind: .toolCallEnd,
                sessionId: sessionID,
                data: ["call_id": toolCall.id, "error": conversionError, "tool": toolCall.name, "tool_name": toolCall.name]
            ))
            return ToolResult(toolCallId: toolCall.id, content: .string(conversionError), isError: true)
        }

        do {
            let rawOutput: String
            if let streamingExecutor = registered.streamingExecutor {
                rawOutput = try await streamingExecutor(foundationArguments, env) { delta in
                    guard !delta.isEmpty else { return }
                    await eventEmitter.emit(SessionEvent(
                        kind: .toolCallOutputDelta,
                        sessionId: sessionID,
                        data: [
                            "call_id": toolCall.id,
                            "tool": toolCall.name,
                            "tool_name": toolCall.name,
                            "delta": delta,
                        ]
                    ))
                }
            } else {
                rawOutput = try await registered.executor(foundationArguments, env)
            }
            let truncatedOutput = truncateToolOutput(rawOutput, toolName: toolCall.name, config: config)

            // Resolve image attachment inline (no actor state needed)
            var imageData: [UInt8]?
            var imageMediaType: String?
            if toolCall.name == "view_image", let rawPath = foundationArguments["path"] as? String {
                let resolvedPath: String = rawPath.hasPrefix("/")
                    ? rawPath
                    : (env.workingDirectory() as NSString).appendingPathComponent(rawPath)
                if let data = try? Data(contentsOf: URL(fileURLWithPath: resolvedPath)), !data.isEmpty {
                    imageData = Array(data)
                    imageMediaType = {
                        switch URL(fileURLWithPath: resolvedPath).pathExtension.lowercased() {
                        case "png": return "image/png"
                        case "jpg", "jpeg": return "image/jpeg"
                        case "gif": return "image/gif"
                        case "webp": return "image/webp"
                        default: return "application/octet-stream"
                        }
                    }()
                }
            }

            await eventEmitter.emit(SessionEvent(
                kind: .toolCallEnd,
                sessionId: sessionID,
                data: [
                    "call_id": toolCall.id,
                    "tool": toolCall.name,
                    "tool_name": toolCall.name,
                    "output": rawOutput,
                    "truncated": rawOutput.count != truncatedOutput.count,
                ]
            ))

            return ToolResult(
                toolCallId: toolCall.id,
                content: .string(truncatedOutput),
                isError: false,
                imageData: imageData,
                imageMediaType: imageMediaType
            )
        } catch {
            let errorMsg = "Tool error (\(toolCall.name)): \(error)"
            await eventEmitter.emit(SessionEvent(
                kind: .toolCallEnd,
                sessionId: sessionID,
                data: ["call_id": toolCall.id, "error": errorMsg, "tool": toolCall.name, "tool_name": toolCall.name]
            ))
            return ToolResult(toolCallId: toolCall.id, content: .string(errorMsg), isError: true)
        }
    }

    private func toolCallSignature(_ toolCall: ToolCall) -> String {
        let args = JSONValue.object(toolCall.arguments).description
        return "\(toolCall.name)::\(args)"
    }

    // MARK: - Timeline

    private func appendTurn(_ turn: Turn) {
        history.append(turn)
        pendingTimelineTurns.append(PersistedTurn(turn))
    }

    private func finalizeTimelineEntry(responseId: String) {
        guard !pendingTimelineTurns.isEmpty else { return }
        let containsResponse = pendingTimelineTurns.contains { turn in
            guard case .assistant(let assistant) = turn else { return false }
            return assistant.responseId == responseId
        }
        guard containsResponse else { return }
        responseTimeline.append(ResponseTimelineEntry(responseId: responseId, turns: pendingTimelineTurns))
        pendingTimelineTurns.removeAll()
    }

    private func rebuildHistory(from timeline: [ResponseTimelineEntry], pending: [PersistedTurn]) -> [Turn] {
        var turns: [Turn] = []
        for entry in timeline {
            turns.append(contentsOf: entry.turns.map { $0.toTurn() })
        }
        turns.append(contentsOf: pending.map { $0.toTurn() })
        return turns
    }

    private func rebuildTimeline(from history: [Turn]) -> (timeline: [ResponseTimelineEntry], pending: [PersistedTurn]) {
        var timeline: [ResponseTimelineEntry] = []
        var buffer: [PersistedTurn] = []
        var activeResponseId: String?
        var activeTimestamp: Date = Date()

        for turn in history {
            if case .assistant(let assistant) = turn {
                if let activeResponseId, !buffer.isEmpty {
                    timeline.append(ResponseTimelineEntry(responseId: activeResponseId, turns: buffer, createdAt: activeTimestamp))
                    buffer.removeAll(keepingCapacity: true)
                }
                let responseId = assistant.responseId ?? "local-\(UUID().uuidString)"
                activeResponseId = responseId
                activeTimestamp = assistant.timestamp
            }
            buffer.append(PersistedTurn(turn))
        }

        if let activeResponseId, !buffer.isEmpty {
            timeline.append(ResponseTimelineEntry(responseId: activeResponseId, turns: buffer, createdAt: activeTimestamp))
            buffer.removeAll()
        }

        return (timeline, buffer)
    }

    private func resolveResponseId(_ response: Response) -> String {
        if !response.id.isEmpty {
            return response.id
        }
        return "local-\(UUID().uuidString)"
    }

    // MARK: - History Conversion

    private func convertHistoryToMessages(systemPrompt: String) -> [Message] {
        var messages: [Message] = [.system(systemPrompt)]
        if !config.interactiveMode {
            messages.append(.user("""
<system-reminder>
You are running in non-interactive (automated pipeline) mode. Complete your assigned task using the available tools, then provide your final response. Do not attempt to ask the user questions or wait for interactive input.
</system-reminder>
"""))
        }

        // Build a lookup of tool call ID → tool name from assistant turns so we
        // can include the name when sending tool results (required by OpenAI Responses API).
        var toolCallIdToName: [String: String] = [:]
        for turn in history {
            if case .assistant(let t) = turn {
                for call in t.toolCalls {
                    if !call.name.isEmpty {
                        toolCallIdToName[call.id] = call.name
                    }
                }
            }
        }

        for turn in history {
            switch turn {
            case .user(let t):
                messages.append(.user(t.content))
            case .assistant(let t):
                // Use raw content parts when available for faithful round-tripping
                // of thinking blocks (with signatures), redacted thinking, etc.
                if let rawParts = t.rawContentParts, !rawParts.isEmpty {
                    messages.append(Message(role: .assistant, content: rawParts))
                } else {
                    var parts: [ContentPart] = []
                    if let reasoning = t.reasoning, !reasoning.isEmpty {
                        parts.append(.thinking(ThinkingData(text: reasoning)))
                    }
                    if !t.content.isEmpty {
                        parts.append(.text(t.content))
                    }
                    for tc in t.toolCalls {
                        parts.append(.toolCall(tc))
                    }
                    if parts.isEmpty {
                        parts.append(.text(""))
                    }
                    messages.append(Message(role: .assistant, content: parts))
                }
            case .toolResults(let t):
                for result in t.results {
                    // Look up the tool name from the preceding assistant turn's tool calls.
                    let toolName = toolCallIdToName[result.toolCallId]
                    messages.append(.toolResult(
                        toolCallId: result.toolCallId,
                        toolName: toolName,
                        content: result.content,
                        isError: result.isError,
                        imageData: result.imageData,
                        imageMediaType: result.imageMediaType
                    ))
                    if let imageData = result.imageData {
                        let image = ImageData(data: imageData, mediaType: result.imageMediaType)
                        messages.append(Message(
                            role: .user,
                            content: [
                                .text("<system-reminder>Image content from the previous view_image tool call is attached below.</system-reminder>"),
                                .image(image),
                            ]
                        ))
                    }
                }
            case .system(let t):
                messages.append(.user(t.content))
            case .steering(let t):
                messages.append(.user(t.content))
            }
        }

        return messages
    }

    /// Build a message list containing only the system prompt and turns added
    /// AFTER the assistant turn whose `responseId` matches `afterResponseId`.
    /// This is used with OpenAI's `previous_response_id` so the server-side
    /// context supplies everything up to that response and we only send new items.
    private func convertIncrementalMessages(systemPrompt: String, afterResponseId: String) -> [Message] {
        // Find the index of the assistant turn that produced this response.
        var cutoffIndex: Int? = nil
        for idx in stride(from: history.count - 1, through: 0, by: -1) {
            if case .assistant(let t) = history[idx], t.responseId == afterResponseId {
                cutoffIndex = idx
                break
            }
        }

        // If we can't find the matching turn, fall back to full history.
        guard let cutoff = cutoffIndex, cutoff + 1 < history.count else {
            return convertHistoryToMessages(systemPrompt: systemPrompt)
        }

        // System/developer messages are sent as `instructions` by the adapter,
        // so we still include the system prompt. Only the input items change.
        var messages: [Message] = [.system(systemPrompt)]
        if !config.interactiveMode {
            messages.append(.user("""
<system-reminder>
You are running in non-interactive (automated pipeline) mode. Complete your assigned task using the available tools, then provide your final response. Do not attempt to ask the user questions or wait for interactive input.
</system-reminder>
"""))
        }

        // Build tool-call-id → name lookup from ALL history (the server knows
        // about these tool calls from the previous response chain).
        var toolCallIdToName: [String: String] = [:]
        for turn in history {
            if case .assistant(let t) = turn {
                for call in t.toolCalls {
                    if !call.name.isEmpty {
                        toolCallIdToName[call.id] = call.name
                    }
                }
            }
        }

        // Only convert turns that came after the matched assistant turn.
        let newTurns = history[(cutoff + 1)...]
        for turn in newTurns {
            switch turn {
            case .user(let t):
                messages.append(.user(t.content))
            case .assistant(let t):
                if let rawParts = t.rawContentParts, !rawParts.isEmpty {
                    messages.append(Message(role: .assistant, content: rawParts))
                } else {
                    var parts: [ContentPart] = []
                    if let reasoning = t.reasoning, !reasoning.isEmpty {
                        parts.append(.thinking(ThinkingData(text: reasoning)))
                    }
                    if !t.content.isEmpty {
                        parts.append(.text(t.content))
                    }
                    for tc in t.toolCalls {
                        parts.append(.toolCall(tc))
                    }
                    if parts.isEmpty {
                        parts.append(.text(""))
                    }
                    messages.append(Message(role: .assistant, content: parts))
                }
            case .toolResults(let t):
                for result in t.results {
                    let toolName = toolCallIdToName[result.toolCallId]
                    messages.append(.toolResult(
                        toolCallId: result.toolCallId,
                        toolName: toolName,
                        content: result.content,
                        isError: result.isError,
                        imageData: result.imageData,
                        imageMediaType: result.imageMediaType
                    ))
                    if let imageData = result.imageData {
                        let image = ImageData(data: imageData, mediaType: result.imageMediaType)
                        messages.append(Message(
                            role: .user,
                            content: [
                                .text("<system-reminder>Image content from the previous view_image tool call is attached below.</system-reminder>"),
                                .image(image),
                            ]
                        ))
                    }
                }
            case .system(let t):
                messages.append(.user(t.content))
            case .steering(let t):
                messages.append(.user(t.content))
            }
        }

        return messages
    }

    private func completeWithTransientRetries(request: Request) async throws -> Response {
        fputs("[Session] completeWithTransientRetries: entering (streaming=\(providerProfile.supportsStreaming), timeout=\(String(describing: config.llmInactivityTimeoutSeconds)))\n", stderr)
        let maxAttempts = 3
        var attempt = 1

        while true {
            do {
                // Use streaming when the provider supports it — this enables
                // WebSocket transport for OpenAI (faster, avoids HTTP timeouts).
                if providerProfile.supportsStreaming {
                    var streamRequest = request
                    if let inactivity = config.llmInactivityTimeoutSeconds, inactivity > 0, streamRequest.timeout == nil {
                        streamRequest.timeout = .seconds(inactivity)
                    }
                    do {
                        return try await streamAndAccumulate(request: streamRequest)
                    } catch is EmptyStreamResponseError {
                        fputs("[Session] Stream produced no response; falling back to complete()\n", stderr)
                        return try await llmClient.complete(request: request)
                    }
                }
                return try await llmClient.complete(request: request)
            } catch {
                if error is AuthenticationError || error is ContextLengthError {
                    throw error
                }

                let shouldRetry = isTransient(error)
                if !shouldRetry || attempt >= maxAttempts {
                    throw error
                }

                let delaySeconds = transientRetryDelaySeconds(forAttempt: attempt, error: error)
                await eventEmitter.emit(SessionEvent(
                    kind: .warning,
                    sessionId: id,
                    data: [
                        "message": "Transient LLM error; retrying attempt \(attempt + 1)/\(maxAttempts) in \(String(format: "%.2f", delaySeconds))s",
                        "error": "\(error)",
                    ]
                ))
                try await Task.sleep(for: .seconds(delaySeconds))
                attempt += 1
            }
        }
    }

    private func latestAssistantResponseId() -> String? {
        for turn in history.reversed() {
            guard case .assistant(let assistantTurn) = turn else { continue }
            if let responseId = assistantTurn.responseId, !responseId.isEmpty {
                return responseId
            }
        }
        return nil
    }

    private func streamAndAccumulate(request: Request) async throws -> Response {
        let rawStream = try await llmClient.stream(request: request)
        let inactivityTimeoutSeconds = config.llmInactivityTimeoutSeconds ?? 0
        let timeoutEnabled = inactivityTimeoutSeconds > 0
        let wallClockLimitSeconds = inactivityTimeoutSeconds * 3
        let activityState = StreamActivityState(start: ContinuousClock.now)
        let emitter = eventEmitter
        let sid = id

        if !timeoutEnabled {
            // No timeout configured — consume directly without task group overhead.
            return try await Self.consumeStream(
                rawStream, activityState: activityState, eventEmitter: emitter, sessionID: sid
            )
        }

        // Supervised: consumer races against watchdog in a task group.
        // When the watchdog throws RequestTimeoutError, group.cancelAll() cancels
        // the consumer task, which cancels the SSE stream, which cancels the
        // URLSession byte stream via the onTermination/onCancel chain in OmniHTTP.
        let response = try await withThrowingTaskGroup(of: Response?.self) { group in
            group.addTask {
                try await Self.consumeStream(
                    rawStream, activityState: activityState, eventEmitter: emitter, sessionID: sid
                )
            }

            group.addTask {
                try await Self.runInactivityWatchdog(
                    activityState: activityState,
                    inactivityLimit: inactivityTimeoutSeconds,
                    wallClockLimit: wallClockLimitSeconds
                )
                return nil // unreachable — watchdog throws or loops until cancelled
            }

            guard let first = try await group.next(), let result = first else {
                group.cancelAll()
                throw SDKError(message: "Stream supervision exited without a result")
            }
            group.cancelAll() // cancel the watchdog
            return result
        }

        return response
    }

    private static func consumeStream(
        _ rawStream: AsyncThrowingStream<StreamEvent, Error>,
        activityState: StreamActivityState,
        eventEmitter: EventEmitter,
        sessionID: String
    ) async throws -> Response {
        var accumulator = StreamAccumulator()
        var eventCount = 0
        var eventTypes: [String: Int] = [:]

        for try await event in rawStream {
            try Task.checkCancellation()
            if isActivityEvent(event) {
                await activityState.markActivity()
            }
            eventCount += 1
            eventTypes[event.type.rawValue, default: 0] += 1
            if event.type.rawValue == "finish" {
                fputs("[Session] finish event: response=\(event.response != nil), finishReason=\(String(describing: event.finishReason)), usage=\(String(describing: event.usage)), text=\(event.response?.text.prefix(100) ?? "nil"), toolCalls=\(event.response?.toolCalls.count ?? -1)\n", stderr)
            }
            accumulator.process(event)
            if let delta = event.delta, !delta.isEmpty {
                await eventEmitter.emit(SessionEvent(
                    kind: .assistantTextDelta,
                    sessionId: sessionID,
                    data: ["text": delta, "delta": delta]
                ))
            }
        }

        fputs("[Session] Stream finished: \(eventCount) events, types: \(eventTypes)\n", stderr)
        if accumulator.hasIncompleteToolCalls() {
            let count = accumulator.incompleteToolCallCount()
            let finish = accumulator.response()?.finishReason.rawValue ?? "unknown"
            if finish == "length" || finish == "other" {
                throw RequestTimeoutError(
                    message: "Stream ended with \(count) incomplete tool call(s) and finishReason=\(finish); retrying request"
                )
            }
        }
        guard let response = accumulator.response() else {
            fputs("[Session] StreamAccumulator produced no response from \(eventCount) events\n", stderr)
            if eventCount == 0 {
                throw EmptyStreamResponseError()
            }
            throw SDKError(message: "Stream completed without producing a response (\(eventCount) events)")
        }
        fputs("[Session] Accumulated response: \(response.text.count) chars, \(response.toolCalls.count) tool calls\n", stderr)
        return response
    }

    private static func runInactivityWatchdog(
        activityState: StreamActivityState,
        inactivityLimit: Double,
        wallClockLimit: Double
    ) async throws {
        while !Task.isCancelled {
            try await Task.sleep(for: .seconds(1))
            let now = ContinuousClock.now
            let snapshot = await activityState.snapshot()
            let idle = secondsBetween(snapshot.lastActivity, and: now)
            if idle >= inactivityLimit {
                throw RequestTimeoutError(
                    message: "LLM inactivity timeout after \(Int(idle))s"
                )
            }
            let elapsed = secondsBetween(snapshot.start, and: now)
            if elapsed >= wallClockLimit {
                throw RequestTimeoutError(
                    message: "LLM wall-clock timeout after \(Int(elapsed))s"
                )
            }
        }
    }

    private static func isActivityEvent(_ event: StreamEvent) -> Bool {
        switch event.type.rawValue {
        case StreamEventType.textStart.rawValue,
             StreamEventType.textDelta.rawValue,
             StreamEventType.reasoningDelta.rawValue,
             StreamEventType.toolCallStart.rawValue,
             StreamEventType.toolCallDelta.rawValue,
             StreamEventType.toolCallEnd.rawValue,
             StreamEventType.finish.rawValue:
            return true
        default:
            return false
        }
    }

    private static func secondsBetween(_ start: ContinuousClock.Instant, and end: ContinuousClock.Instant) -> Double {
        let duration = end - start
        return Double(duration.components.seconds) + Double(duration.components.attoseconds) / 1e18
    }

    private actor StreamActivityState {
        private let streamStart: ContinuousClock.Instant
        private var lastEventAt: ContinuousClock.Instant

        init(start: ContinuousClock.Instant) {
            streamStart = start
            lastEventAt = start
        }

        func markActivity() {
            lastEventAt = ContinuousClock.now
        }

        func snapshot() -> (start: ContinuousClock.Instant, lastActivity: ContinuousClock.Instant) {
            (streamStart, lastEventAt)
        }
    }

    private func isTransient(_ error: any Error) -> Bool {
        if let sdkError = error as? SDKError {
            return sdkError.retryable
        }
        if error is URLError {
            return true
        }
        return false
    }

    private func transientRetryDelaySeconds(forAttempt attempt: Int, error: any Error) -> Double {
        if let sdkError = error as? SDKError,
           let retryAfter = sdkError.retryAfter,
           retryAfter > 0
        {
            return retryAfter
        }
        // Backoff sequence for attempts 1..N
        switch attempt {
        case 1: return 0.5
        case 2: return 1.5
        default: return 3.0
        }
    }

    private func resolveImageAttachment(
        toolName: String,
        arguments: [String: Any]
    ) -> (data: [UInt8], mediaType: String)? {
        guard toolName == "view_image", let rawPath = arguments["path"] as? String else {
            return nil
        }

        let resolvedPath: String = {
            if rawPath.hasPrefix("/") { return rawPath }
            return (executionEnv.workingDirectory() as NSString).appendingPathComponent(rawPath)
        }()
        guard let data = try? Data(contentsOf: URL(fileURLWithPath: resolvedPath)), !data.isEmpty else {
            return nil
        }
        let mediaType = mimeType(for: resolvedPath)
        return (Array(data), mediaType)
    }

    private func mimeType(for path: String) -> String {
        switch URL(fileURLWithPath: path).pathExtension.lowercased() {
        case "png": return "image/png"
        case "jpg", "jpeg": return "image/jpeg"
        case "gif": return "image/gif"
        case "webp": return "image/webp"
        case "bmp": return "image/bmp"
        case "tif", "tiff": return "image/tiff"
        case "heic": return "image/heic"
        case "heif": return "image/heif"
        default: return "application/octet-stream"
        }
    }

    // MARK: - Loop Detection

    private func detectLoop(window: Int) -> Bool {
        let recentSignatures = extractToolCallSignatures(last: window)
        guard recentSignatures.count >= window else { return false }

        for patternLen in [1, 2, 3] {
            guard window % patternLen == 0 else { continue }
            let pattern = Array(recentSignatures[0..<patternLen])
            var allMatch = true
            for i in stride(from: patternLen, to: window, by: patternLen) {
                let chunk = Array(recentSignatures[i..<min(i + patternLen, recentSignatures.count)])
                if chunk != pattern {
                    allMatch = false
                    break
                }
            }
            if allMatch { return true }
        }
        return false
    }

    private func extractToolCallSignatures(last count: Int) -> [String] {
        var signatures: [String] = []
        for turn in history.reversed() {
            if case .assistant(let t) = turn {
                for tc in t.toolCalls.reversed() {
                    let argsHash = stableHash(tc.arguments)
                    signatures.insert("\(tc.name):\(argsHash)", at: 0)
                    if signatures.count >= count { return signatures }
                }
            }
        }
        return signatures
    }

    private func stableHash(_ dict: [String: JSONValue]) -> String {
        let sorted = dict.sorted(by: { $0.key < $1.key })
        let desc = sorted.map { "\($0.key)=\(stableValueDescription($0.value))" }.joined(separator: ",")
        var hasher = Hasher()
        hasher.combine(desc)
        return "\(hasher.finalize())"
    }

    // MARK: - Tool Argument Validation

    private func validateToolArguments(_ args: [String: JSONValue], against schema: JSONValue) -> String? {
        do {
            try JSONSchema(schema).validate(.object(args))
            return nil
        } catch {
            return "Invalid tool arguments: \(error)"
        }
    }

    // MARK: - Context Window Awareness

    private func checkContextUsage() async {
        let approxTokens = totalCharsInHistory() / 4
        let threshold = providerProfile.contextWindowSize * 80 / 100
        if approxTokens > threshold {
            let pct = approxTokens * 100 / providerProfile.contextWindowSize
            await eventEmitter.emit(SessionEvent(
                kind: .warning,
                sessionId: id,
                data: ["message": "Context usage at ~\(pct)% of context window", "context_usage_percent": pct]
            ))
        }
    }

    private func totalCharsInHistory() -> Int {
        var total = 0
        for turn in history {
            switch turn {
            case .user(let t): total += t.content.count
            case .assistant(let t): total += t.content.count + (t.reasoning?.count ?? 0)
            case .toolResults(let t): total += t.results.reduce(0) { $0 + stringifyJSONValue($1.content).count }
            case .system(let t): total += t.content.count
            case .steering(let t): total += t.content.count
            }
        }
        return total
    }

    private func countTurns() -> Int {
        history.count
    }

    private func stableValueDescription(_ value: JSONValue) -> String {
        switch value {
        case .null:
            return "null"
        case .bool(let b):
            return b ? "true" : "false"
        case .number(let n):
            return String(n)
        case .string(let s):
            return "\"\(s)\""
        case .array(let arr):
            return "[" + arr.map(stableValueDescription).joined(separator: ",") + "]"
        case .object(let obj):
            let parts = obj.keys.sorted().map { key in
                "\(key):\(stableValueDescription(obj[key]!))"
            }
            return "{\(parts.joined(separator: ","))}"
        }
    }

    private func stringifyJSONValue(_ value: JSONValue) -> String {
        if case .string(let s) = value {
            return s
        }
        return stableValueDescription(value)
    }

    private func wrapSystemReminder(_ content: String) -> String {
        let trimmed = content.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.contains("<system-reminder>") {
            return trimmed
        }
        return """
<system-reminder>
\(trimmed)
</system-reminder>
"""
    }

    // MARK: - Project Doc Discovery

    private func discoverProjectDocs(workingDir: String) async -> String? {
        let providerFiles: [String]
        switch providerProfile.id {
        case "openai":
            providerFiles = ["AGENTS.md", ".codex/instructions.md"]
        case "anthropic":
            providerFiles = ["AGENTS.md", "CLAUDE.md"]
        case "gemini":
            providerFiles = ["AGENTS.md", "GEMINI.md"]
        default:
            providerFiles = ["AGENTS.md"]
        }

        var docs: [String] = []
        var totalBytes = 0
        let budget = 32 * 1024

        // Try git root first
        let gitRoot = await findGitRoot(workingDir: workingDir)
        let searchDirs = gitRoot != nil && gitRoot != workingDir
            ? [gitRoot!, workingDir]
            : [workingDir]

        for dir in searchDirs {
            for fileName in providerFiles {
                let path = (dir as NSString).appendingPathComponent(fileName)
                if await executionEnv.fileExists(path: path) {
                    do {
                        let content = try await executionEnv.readFile(path: path, offset: nil, limit: nil)
                        if totalBytes + content.count <= budget {
                            docs.append("# \(fileName)\n\(content)")
                            totalBytes += content.count
                        } else {
                            let remaining = budget - totalBytes
                            if remaining > 0 {
                                docs.append("# \(fileName)\n\(String(content.prefix(remaining)))\n[Project instructions truncated at 32KB]")
                            }
                            return docs.joined(separator: "\n\n")
                        }
                    } catch {
                        continue
                    }
                }
            }
        }

        return docs.isEmpty ? nil : docs.joined(separator: "\n\n")
    }

    private func findGitRoot(workingDir: String) async -> String? {
        do {
            let result = try await executionEnv.execCommand(
                command: "git rev-parse --show-toplevel",
                timeoutMs: 5000,
                workingDir: workingDir,
                envVars: nil
            )
            if result.exitCode == 0 {
                return result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
            }
        } catch {}
        return nil
    }

    // MARK: - Git Context

    public func gatherGitContext() async -> GitContext? {
        let workingDir = executionEnv.workingDirectory()

        // Check if we're in a git repo
        guard let _ = await findGitRoot(workingDir: workingDir) else {
            return nil
        }

        var context = GitContext()

        // Branch name
        if let result = try? await executionEnv.execCommand(
            command: "git rev-parse --abbrev-ref HEAD",
            timeoutMs: 5000, workingDir: workingDir, envVars: nil
        ), result.exitCode == 0 {
            context.branch = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        }

        // Modified file count
        if let result = try? await executionEnv.execCommand(
            command: "git status --porcelain | wc -l",
            timeoutMs: 5000, workingDir: workingDir, envVars: nil
        ), result.exitCode == 0 {
            context.modifiedFileCount = Int(result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 0
        }

        // Recent commits
        if let result = try? await executionEnv.execCommand(
            command: "git log --oneline -5",
            timeoutMs: 5000, workingDir: workingDir, envVars: nil
        ), result.exitCode == 0 {
            context.recentCommits = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        }

        return context
    }

    // MARK: - Subagents

    public func registerSubagent(_ handle: SubAgentHandle) {
        subagents[handle.id] = handle
    }

    public func getSubagent(_ agentId: String) -> SubAgentHandle? {
        subagents[agentId]
    }

    public func removeSubagent(_ agentId: String) {
        subagents.removeValue(forKey: agentId)
    }

    public func currentDepth() -> Int {
        depth
    }
}
