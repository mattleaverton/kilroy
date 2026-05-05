import Foundation
import OmniAICore

// MARK: - LLMKit Backend

public final class LLMKitBackend: CodergenBackend, Sendable {
    private let client: Client?

    public init(client: Client? = nil) {
        self.client = client
    }

    public func run(
        prompt: String,
        model: String,
        provider: String,
        reasoningEffort: String,
        context: PipelineContext
    ) async throws -> CodergenResult {
        let resolvedClient = try (client ?? Client.fromEnv())
        let goal = context.getString("_graph_goal")
        let systemPrompt = buildSystemPrompt(goal: goal, context: context)
        let inactivityTimeoutSeconds = resolveInactivityTimeoutSeconds(from: context)
        let modelCandidates = fallbackModels(for: model, provider: provider)

        var lastNotFound: NotFoundError?
        for candidateModel in modelCandidates {
            // Retry with exponential backoff for transient errors
            let maxRetries = 3
            var lastError: (any Error)?
            for attempt in 0..<maxRetries {
                do {
                    let result = try await runWithInactivityTimeout(
                        model: candidateModel,
                        prompt: prompt,
                        system: systemPrompt,
                        reasoningEffort: effectiveReasoningEffort(for: provider, requested: reasoningEffort),
                        provider: provider,
                        providerOptions: providerOptions(for: provider),
                        inactivityTimeoutSeconds: inactivityTimeoutSeconds,
                        client: resolvedClient
                    )
                    return try parseResponse(result.text)
                } catch let notFound as NotFoundError {
                    lastNotFound = notFound
                    break // Try next model, not retry
                } catch {
                    lastError = error
                    let isTransient = isTransientError(error)
                    if !isTransient || attempt >= maxRetries - 1 {
                        if !isTransient { throw error }
                        break
                    }
                    let delay = pow(2.0, Double(attempt)) * 1.0 // 1s, 2s, 4s
                    fputs("[LLMKitBackend] Transient error (attempt \(attempt + 1)/\(maxRetries)), retrying in \(delay)s: \(error)\n", stderr)
                    try await Task.sleep(for: .seconds(delay))
                }
            }
            if lastNotFound != nil { continue }
            if let lastError { throw lastError }
        }

        if let lastNotFound {
            throw lastNotFound
        }
        throw RequestTimeoutError(message: "LLM execution failed without a provider response")
    }

    private func buildSystemPrompt(goal: String, context: PipelineContext) -> String {
        var parts: [String] = []
        parts.append("You are a stage in an AI pipeline.")

        if !goal.isEmpty {
            parts.append("The pipeline goal is: \(goal)")
        }

        let lastStage = context.getString("last_stage")
        let lastResponse = context.getString("last_response")
        if !lastStage.isEmpty && !lastResponse.isEmpty {
            parts.append("The previous stage (\(lastStage)) produced this output:\n\n\(lastResponse)")
        }

        // Include tool output from previous tool nodes
        let toolOutput = context.getString("tool.output")
        if !toolOutput.isEmpty {
            parts.append("The previous tool node produced this output:\n\n\(toolOutput)")
        }

        let fidelity = context.getString("_fidelity")
        if !fidelity.isEmpty {
            parts.append("Context fidelity mode for this stage: \(fidelity)")
        }

        let preamble = context.getString("_preamble")
        if !preamble.isEmpty {
            parts.append("Context carryover preamble:\n\n\(preamble)")
        }

        parts.append("""
        After completing your task, include a JSON status block at the end of your response \
        in the following format:

        ```json
        {
          "outcome": "success",
          "preferred_next_label": "",
          "context_updates": {},
          "notes": ""
        }
        ```

        Valid outcome values: "success", "partial_success", "retry", "fail"
        - preferred_next_label: label of the preferred next edge/node (optional)
        - context_updates: key-value pairs to pass to subsequent stages (optional)
        - notes: any notes about the result (optional)
        """)

        return parts.joined(separator: "\n\n")
    }

    func resolveInactivityTimeoutSeconds(
        from context: PipelineContext,
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) -> Double {
        if let nodeTimeoutSeconds = Double(context.getString("_current_node_timeout")),
           nodeTimeoutSeconds > 0
        {
            return nodeTimeoutSeconds
        }
        if let envTimeout = environment["ATTRACTOR_LLM_INACTIVITY_TIMEOUT_SECONDS"],
           let seconds = Double(envTimeout),
           seconds > 0
        {
            return seconds
        }
        // Timeout semantics: only fail when no meaningful model/tool activity is observed
        // for this duration.
        return 300
    }

    private func isTransientError(_ error: any Error) -> Bool {
        if error is URLError { return true }
        if let sdkError = error as? SDKError { return sdkError.retryable }
        // Network errors, timeouts, and server errors are transient
        let desc = String(describing: error).lowercased()
        return desc.contains("network") || desc.contains("timeout") || desc.contains("connection")
            || desc.contains("server error") || desc.contains("502") || desc.contains("503")
    }

    private func fallbackModels(for model: String, provider: String) -> [String] {
        var candidates: [String] = [model]
        if provider.lowercased() == "anthropic", model.contains("[1m]") {
            candidates.append(model.replacing("[1m]", with: ""))
        }
        return Array(NSOrderedSet(array: candidates)) as? [String] ?? candidates
    }

    func providerOptions(
        for provider: String,
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) -> [String: JSONValue]? {
        switch provider.lowercased() {
        case "openai":
            var openAIOptions: [String: JSONValue] = [
                OpenAIProviderOptionKeys.responsesTransport: .string("websocket"),
            ]
            if let wsBase = environment["OPENAI_WEBSOCKET_BASE_URL"],
               !wsBase.isEmpty
            {
                openAIOptions[OpenAIProviderOptionKeys.websocketBaseURL] = .string(wsBase)
            }
            return ["openai": .object(openAIOptions)]
        case "cerebras":
            return ["cerebras": .object(["disable_reasoning": .bool(true)])]
        default:
            return nil
        }
    }

    private func effectiveReasoningEffort(for provider: String, requested: String) -> String? {
        if provider.lowercased() == "cerebras" {
            return nil
        }
        return requested
    }

    private func runWithInactivityTimeout(
        model: String,
        prompt: String,
        system: String,
        reasoningEffort: String?,
        provider: String,
        providerOptions: [String: JSONValue]?,
        inactivityTimeoutSeconds: Double,
        client: Client
    ) async throws -> GenerateResult {
        let streamResult = try await stream(
            model: model,
            prompt: prompt,
            system: system,
            reasoningEffort: reasoningEffort,
            provider: provider,
            providerOptions: providerOptions,
            timeout: nil,
            client: client
        )

        let activity = ActivityTracker()
        let collector = EventCollector()

        let response = try await withThrowingTaskGroup(of: Response.self) { group in
            group.addTask {
                for try await event in streamResult {
                    if Self.isActivityEvent(event) {
                        await activity.touch()
                    }
                    await collector.process(event)
                }
                if let accumulated = await collector.response() {
                    return accumulated
                }
                return try await streamResult.response()
            }

            group.addTask {
                let startTime = ContinuousClock.now
                let wallClockLimit = Duration.seconds(inactivityTimeoutSeconds * 3) // hard limit = 3x inactivity
                while true {
                    try await Task.sleep(for: .seconds(1))
                    let idle = await activity.idleSeconds()
                    if idle >= inactivityTimeoutSeconds {
                        throw RequestTimeoutError(
                            message: "LLM inactivity timeout after \(Int(inactivityTimeoutSeconds))s"
                        )
                    }
                    let elapsed = ContinuousClock.now - startTime
                    if elapsed >= wallClockLimit {
                        throw RequestTimeoutError(
                            message: "LLM wall-clock timeout after \(Int(elapsed.components.seconds))s"
                        )
                    }
                }
            }

            do {
                guard let first = try await group.next() else {
                    throw RequestTimeoutError(message: "LLM stream ended without a response")
                }
                group.cancelAll()
                return first
            } catch {
                group.cancelAll()
                throw error
            }
        }

        return GenerateResult(
            text: response.text,
            reasoning: response.reasoning,
            toolCalls: response.toolCalls,
            toolResults: [],
            finishReason: response.finishReason,
            usage: response.usage,
            totalUsage: response.usage,
            steps: [],
            response: response,
            output: nil
        )
    }

    static func isActivityEvent(_ event: StreamEvent) -> Bool {
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

    private func parseResponse(_ response: String) throws -> CodergenResult {
        // Try to find a JSON block in the response
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

        // No JSON block found - default to partialSuccess (safest; we cannot
        // confirm the goal was met without structured output)
        return CodergenResult(
            response: response,
            status: .partialSuccess,
            notes: "WARNING: No structured status block found in LLM response; defaulting to partial_success"
        )
    }

    private func extractJSONBlock(from text: String) -> String? {
        // Look for ```json ... ``` blocks from the end of the response
        let pattern = "```json\\s*\\n([\\s\\S]*?)\\n\\s*```"
        guard let regex = try? NSRegularExpression(pattern: pattern, options: []) else {
            return nil
        }
        let nsText = text as NSString
        let matches = regex.matches(in: text, options: [], range: NSRange(location: 0, length: nsText.length))

        // Use the last match (the status block is expected at the end)
        guard let lastMatch = matches.last, lastMatch.numberOfRanges >= 2 else {
            return nil
        }
        return nsText.substring(with: lastMatch.range(at: 1))
    }
}

private actor ActivityTracker {
    private var lastActivity = Date()

    func touch() {
        lastActivity = Date()
    }

    func idleSeconds() -> Double {
        Date().timeIntervalSince(lastActivity)
    }
}

private actor EventCollector {
    private var accumulator = StreamAccumulator()

    func process(_ event: StreamEvent) {
        accumulator.process(event)
    }

    func response() -> Response? {
        accumulator.response()
    }
}
