import Foundation
import OmniAICore
import OmniAIAgent

// MARK: - Coding Agent Backend

/// A CodergenBackend that uses CodingAgentLoop sessions instead of raw LLM text generation.
/// Each pipeline node gets a full coding agent with file read/write, shell execution, grep, glob, etc.
public final class CodingAgentBackend: CodergenBackend, Sendable {
    private let client: Client?
    private let workingDirectory: String

    public init(client: Client? = nil, workingDirectory: String? = nil) {
        self.client = client
        self.workingDirectory = workingDirectory ?? FileManager.default.currentDirectoryPath
    }

    public func run(
        prompt: String,
        model: String,
        provider: String,
        reasoningEffort: String,
        context: PipelineContext
    ) async throws -> CodergenResult {
        let resolvedClient = try (client ?? Client.fromEnv())
        let overrides = resolveNodeOverrides(from: context)

        // 1. Create the provider profile wrapped with pipeline context
        let baseProfile = createProfile(provider: provider, model: model)
        let effectiveProfile: ProviderProfile = {
            if overrides.excludedTools.isEmpty {
                return baseProfile
            }
            return FilteredToolProfile(wrapping: baseProfile, excludedTools: overrides.excludedTools)
        }()
        let pipelineContext = PipelineProfileContext(
            goal: context.getString("_graph_goal"),
            previousStageLabel: context.getString("last_stage"),
            previousStageOutput: context.getString("last_response"),
            toolOutput: context.getString("tool.output")
        )
        let profile = PipelineProfile(wrapping: effectiveProfile, context: pipelineContext)

        // 2. Configure the session
        let sessionConfig = SessionConfig(
            maxTurns: overrides.maxTurns ?? 0,
            maxToolRoundsPerInput: 0,
            defaultCommandTimeoutMs: overrides.defaultCommandTimeoutMs ?? 10_000,
            maxCommandTimeoutMs: overrides.maxCommandTimeoutMs ?? 600_000,
            reasoningEffort: effectiveReasoningEffort(for: provider, requested: reasoningEffort),
            enableLoopDetection: true,
            loopDetectionWindow: overrides.loopDetectionWindow ?? 10,
            userInstructions: overrides.userInstructions,
            llmInactivityTimeoutSeconds: overrides.llmInactivityTimeoutSeconds ?? resolveLLMInactivityTimeoutSeconds(),
            parallelToolCalls: overrides.parallelToolCalls
        )

        // 3. Create execution environment
        let env = LocalExecutionEnvironment(workingDir: workingDirectory)
        let storageBackend = makeStorageBackend()
        let sessionID = buildSessionID(for: overrides.resumeKey)

        // On loop_restart cycles, clear persisted session state so the session
        // starts fresh instead of restoring unbounded history from prior cycles.
        let isLoopRestart = context.getString("loop_restart") == "true"
        if isLoopRestart {
            try? await storageBackend.delete(sessionID: sessionID)
        }

        // 4. Create the session
        let session = try Session(
            profile: profile,
            environment: env,
            client: resolvedClient,
            config: sessionConfig,
            sessionID: sessionID,
            storageBackend: storageBackend
        )

        // 5. Track tool calls and errors via events
        let tracker = ToolCallTracker()
        let errorTracker = ErrorTracker()
        await session.eventEmitter.on { event in
            tracker.process(event)
            errorTracker.process(event)
        }

        // 6. Submit the raw task prompt and wait for completion.
        // Stream-level inactivity timeout in Session.streamAndAccumulate() handles
        // stalled streams; callLLMWithRetry retries transient failures (including
        // RequestTimeoutError). No outer hard timeout here — sessions with many
        // tool call rounds legitimately run for minutes.
        fputs("[CodingAgentBackend] Submitting prompt (\(prompt.count) chars) to \(provider)/\(model)...\n", stderr)
        await session.submit(prompt)
        fputs("[CodingAgentBackend] Submit returned.\n", stderr)

        // 7. Check response quality and request follow-ups if needed
        let initialHistory = await session.getHistory()
        let initialResponse = buildFinalResponse(from: initialHistory, tracker: tracker)

        // Follow-up: If no JSON status block, ask for summary + block (single follow-up only)
        if extractJSONBlock(from: initialResponse) == nil {
            await session.submit(
                "Summarize everything you did: files read, changes made, findings. " +
                "Then end with the JSON status block:\n\n" +
                "```json\n{\n  \"outcome\": \"success\",\n  \"preferred_next_label\": \"\",\n  " +
                "\"context_updates\": {},\n  \"notes\": \"what you accomplished\"\n}\n```"
            )
        }

        // 8. Extract the final response from session history (including all follow-ups)
        let history = await session.getHistory()
        let finalResponse = buildFinalResponse(from: history, tracker: tracker)
        let parsed = parseResponse(finalResponse)

        // Log errors if agent produced no output
        let errors = errorTracker.errors
        if !errors.isEmpty {
            fputs("[CodingAgentBackend] Session errors:\n", stderr)
            for err in errors {
                fputs("  - \(err)\n", stderr)
            }
        }

        // Stage completed; clear persisted state for this node session.
        try? await session.clearPersistedState()
        await session.close()
        return parsed
    }

    // MARK: - Response Extraction

    private func buildFinalResponse(from history: [Turn], tracker: ToolCallTracker) -> String {
        // Collect ALL non-empty assistant text from every turn
        var allAssistantTexts: [String] = []
        var totalToolCalls = 0

        for turn in history {
            if case .assistant(let t) = turn {
                totalToolCalls += t.toolCalls.count
                if !t.content.isEmpty {
                    allAssistantTexts.append(t.content)
                }
            }
        }

        var parts: [String] = []

        // Concatenate text from ALL assistant turns (not just the last one)
        // This ensures findings from intermediate turns are preserved
        if !allAssistantTexts.isEmpty {
            parts.append(allAssistantTexts.joined(separator: "\n\n---\n\n"))
        }

        // Add summary line showing tool call count
        parts.append("\n\n[Agent completed \(allAssistantTexts.count) assistant turns, \(totalToolCalls) tool calls]")

        return parts.joined()
    }

    // MARK: - Profile Factory

    private func createProfile(provider: String, model: String) -> ProviderProfile {
        // Pipeline agents run non-interactively — skip interactive tools to avoid
        // schema validation issues and reduce token overhead.
        switch provider.lowercased() {
        case "anthropic":
            return AnthropicProfile(model: model.isEmpty ? "claude-opus-4-6" : model, enableInteractiveTools: false)
        case "openai":
            return OpenAIProfile(
                model: model.isEmpty ? "gpt-5.2-codex" : model,
                forceCodexSystemPrompt: true
            )
        case "gemini":
            return GeminiProfile(
                model: model.isEmpty ? "gemini-3.1-pro-preview-customtools" : model,
                interactiveMode: false,
                enableTodos: false,
                enablePlanTools: false
            )
        case "groq":
            return MinimalProviderProfile(
                id: "groq",
                model: model.isEmpty ? "openai/gpt-oss-20b" : model
            )
        case "cerebras":
            return MinimalProviderProfile(
                id: "cerebras",
                model: model.isEmpty ? "zai-glm-4.7" : model,
                options: ["cerebras": .object(["disable_reasoning": .bool(true)])]
            )
        default:
            return MinimalProviderProfile(
                id: provider,
                model: model.isEmpty ? "gpt-4.1-mini" : model
            )
        }
    }

    private func effectiveReasoningEffort(for provider: String, requested: String) -> String? {
        if provider.lowercased() == "cerebras" {
            return nil
        }
        return requested
    }

    private func resolveLLMInactivityTimeoutSeconds() -> Double {
        let env = ProcessInfo.processInfo.environment
        let keys = [
            "ATTRACTOR_AGENT_INACTIVITY_TIMEOUT_SECONDS",
            "ATTRACTOR_LLM_INACTIVITY_TIMEOUT_SECONDS",
        ]
        for key in keys {
            if let raw = env[key], let value = Double(raw), value > 0 {
                return value
            }
        }
        // Default watchdog for agent backend requests.
        return 90
    }

    private func resolveNodeOverrides(from context: PipelineContext) -> NodeOverrides {
        NodeOverrides(
            maxTurns: parseInt(context.getString("_current_node_max_agent_turns")),
            defaultCommandTimeoutMs: parseInt(context.getString("_current_node_default_command_timeout_ms")),
            maxCommandTimeoutMs: parseInt(context.getString("_current_node_max_command_timeout_ms")),
            llmInactivityTimeoutSeconds: parseDouble(context.getString("_current_node_llm_inactivity_timeout_seconds")),
            loopDetectionWindow: parseInt(context.getString("_current_node_loop_detection_window")),
            parallelToolCalls: parseBool(context.getString("_current_node_parallel_tool_calls")),
            userInstructions: {
                let value = context.getString("_current_node_user_instructions")
                return value.isEmpty ? nil : value
            }(),
            excludedTools: parseStringList(context.getString("_current_node_excluded_tools")),
            resumeKey: {
                let value = context.getString("_current_node_resume_key")
                return value.isEmpty ? nil : value
            }()
        )
    }

    private func makeStorageBackend() -> SessionStorageBackend {
        let root = URL(fileURLWithPath: workingDirectory, isDirectory: true)
            .appendingPathComponent(".ai/attractor-agent-state", isDirectory: true)
        return FileSessionStorageBackend(rootDirectory: root)
    }

    private func buildSessionID(for resumeKey: String?) -> String {
        guard let resumeKey, !resumeKey.isEmpty else {
            return UUID().uuidString
        }
        return sanitizeSessionID(resumeKey)
    }

    private func sanitizeSessionID(_ value: String) -> String {
        let allowed = Set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
        let sanitized = value.map { allowed.contains($0) ? $0 : "_" }
        return String(sanitized.prefix(180))
    }

    private func parseInt(_ value: String) -> Int? {
        guard !value.isEmpty else { return nil }
        return Int(value)
    }

    private func parseDouble(_ value: String) -> Double? {
        guard !value.isEmpty else { return nil }
        return Double(value)
    }

    private func parseBool(_ value: String) -> Bool? {
        guard !value.isEmpty else { return nil }
        switch value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
        case "true", "1", "yes", "on":
            return true
        case "false", "0", "no", "off":
            return false
        default:
            return nil
        }
    }

    private func parseStringList(_ value: String) -> [String] {
        guard !value.isEmpty else { return [] }
        if value.hasPrefix("[") && value.hasSuffix("]"),
           let data = value.data(using: .utf8),
           let list = try? JSONSerialization.jsonObject(with: data) as? [String]
        {
            return list.map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }.filter { !$0.isEmpty }
        }
        return value
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
    }

    // MARK: - Response Parsing (reused from LLMKitBackend)

    private func parseResponse(_ response: String) -> CodergenResult {
        if let jsonBlock = extractJSONBlock(from: response) {
            if let data = jsonBlock.data(using: .utf8),
               let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                let outcomeStr = json["outcome"] as? String ?? "success"
                let status = OutcomeStatus(rawValue: outcomeStr) ?? .success
                let preferredLabel = json["preferred_next_label"] as? String ?? ""
                let notes = json["notes"] as? String ?? ""

                var contextUpdates: [String: String] = [:]
                if let updates = json["context_updates"] as? [String: Any] {
                    for (k, v) in updates {
                        contextUpdates[k] = "\(v)"
                    }
                }

                var suggestedNextIds: [String] = []
                if let ids = json["suggested_next_ids"] as? [String] {
                    suggestedNextIds = ids
                }

                return CodergenResult(
                    response: response,
                    status: status,
                    contextUpdates: contextUpdates,
                    preferredLabel: preferredLabel,
                    suggestedNextIds: suggestedNextIds,
                    notes: notes
                )
            }
        }

        return CodergenResult(
            response: response,
            status: .partialSuccess,
            notes: "WARNING: No structured status block found in agent response; defaulting to partial_success"
        )
    }

    private func extractJSONBlock(from text: String) -> String? {
        let pattern = "```json\\s*\\n([\\s\\S]*?)\\n\\s*```"
        guard let regex = try? NSRegularExpression(pattern: pattern, options: []) else {
            return nil
        }
        let nsText = text as NSString
        let matches = regex.matches(in: text, options: [], range: NSRange(location: 0, length: nsText.length))
        guard let lastMatch = matches.last, lastMatch.numberOfRanges >= 2 else {
            return nil
        }
        return nsText.substring(with: lastMatch.range(at: 1))
    }
}

private struct NodeOverrides: Sendable {
    var maxTurns: Int?
    var defaultCommandTimeoutMs: Int?
    var maxCommandTimeoutMs: Int?
    var llmInactivityTimeoutSeconds: Double?
    var loopDetectionWindow: Int?
    var parallelToolCalls: Bool?
    var userInstructions: String?
    var excludedTools: [String]
    var resumeKey: String?
}

private final class FilteredToolProfile: ProviderProfile, Sendable {
    private let wrapped: ProviderProfile
    let toolRegistry: ToolRegistry
    let excludedNamesLowercased: Set<String>

    var id: String { wrapped.id }
    var model: String { wrapped.model }
    var supportsReasoning: Bool { wrapped.supportsReasoning }
    var supportsStreaming: Bool { wrapped.supportsStreaming }
    var supportsParallelToolCalls: Bool { wrapped.supportsParallelToolCalls }
    var contextWindowSize: Int { wrapped.contextWindowSize }

    init(wrapping profile: ProviderProfile, excludedTools: [String]) {
        self.wrapped = profile
        self.toolRegistry = ToolRegistry()
        self.excludedNamesLowercased = Set(excludedTools.map { $0.lowercased() })

        for name in profile.toolRegistry.names() {
            if excludedNamesLowercased.contains(name.lowercased()) {
                continue
            }
            if let tool = profile.toolRegistry.get(name) {
                self.toolRegistry.register(tool)
            }
        }
    }

    func buildSystemPrompt(
        environment: ExecutionEnvironment,
        projectDocs: String?,
        userInstructions: String?,
        gitContext: GitContext?
    ) -> String {
        wrapped.buildSystemPrompt(
            environment: environment,
            projectDocs: projectDocs,
            userInstructions: userInstructions,
            gitContext: gitContext
        )
    }

    func tools() -> [Tool] {
        toolRegistry.llmKitDefinitions()
    }

    func providerOptions() -> [String: JSONValue]? {
        wrapped.providerOptions()
    }
}

private final class MinimalProviderProfile: ProviderProfile, Sendable {
    let id: String
    let model: String
    let toolRegistry = ToolRegistry()
    let supportsReasoning = true
    let supportsStreaming = true
    let supportsParallelToolCalls = true
    let contextWindowSize = 128_000
    private let options: [String: JSONValue]?

    init(id: String, model: String, options: [String: JSONValue]? = nil) {
        self.id = id
        self.model = model
        self.options = options
    }

    func buildSystemPrompt(
        environment: ExecutionEnvironment,
        projectDocs: String?,
        userInstructions: String?,
        gitContext: GitContext?
    ) -> String {
        "You are a concise assistant. Follow instructions and always return a JSON status block."
    }

    func providerOptions() -> [String: JSONValue]? {
        options
    }
}

// MARK: - Tool Call Tracker

/// Tracks tool call count from session events.
/// Uses NSLock instead of actor isolation so the event handler can call
/// process() synchronously, avoiding fire-and-forget Task{} that starve
/// the cooperative thread pool under parallel session execution.
// Safety: @unchecked Sendable — mutable state (count) guarded by lock.
private final class ToolCallTracker: @unchecked Sendable {
    private let lock = NSLock()
    private var _count: Int = 0

    var count: Int {
        lock.lock()
        defer { lock.unlock() }
        return _count
    }

    func process(_ event: SessionEvent) {
        if event.kind == .toolCallEnd {
            lock.lock()
            _count += 1
            lock.unlock()
        }
    }
}

// MARK: - Error Tracker

/// Tracks errors from session events for debugging.
/// Uses NSLock instead of actor isolation for the same reason as ToolCallTracker.
// Safety: @unchecked Sendable — mutable state (errors) guarded by lock.
private final class ErrorTracker: @unchecked Sendable {
    private let lock = NSLock()
    private var _errors: [String] = []

    var errors: [String] {
        lock.lock()
        defer { lock.unlock() }
        return _errors
    }

    func process(_ event: SessionEvent) {
        if event.kind == .error {
            let detail = event.stringValue(for: "error") ?? "unknown error"
            lock.lock()
            _errors.append(detail)
            lock.unlock()
            fputs("[CodingAgentBackend:ERROR] \(detail)\n", stderr)
        }
        if event.kind == .sessionEnd,
           let state = event.stringValue(for: "state"), state == "closed",
           let error = event.stringValue(for: "error") {
            lock.lock()
            _errors.append("Session closed with error: \(error)")
            lock.unlock()
            fputs("[CodingAgentBackend:ERROR] Session closed: \(error)\n", stderr)
        }
        if event.kind == .warning {
            let msg = event.stringValue(for: "message") ?? ""
            fputs("[CodingAgentBackend:WARN] \(msg)\n", stderr)
        }
    }
}
