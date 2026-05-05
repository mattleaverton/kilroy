import Foundation

// MARK: - Pipeline Config

public struct PipelineConfig: Sendable {
    public var logsRoot: URL
    public var retryPolicy: PipelineRetryPolicy
    public var interviewer: Interviewer
    public var backend: CodergenBackend
    public var transforms: [GraphTransform]
    public var eventEmitter: PipelineEventEmitter?
    public var client: (any Sendable)?

    public init(
        logsRoot: URL,
        retryPolicy: PipelineRetryPolicy = .default,
        backend: CodergenBackend,
        interviewer: Interviewer = AutoApproveInterviewer(),
        transforms: [GraphTransform] = [],
        eventEmitter: PipelineEventEmitter? = nil,
        client: (any Sendable)? = nil
    ) {
        self.logsRoot = logsRoot
        self.retryPolicy = retryPolicy
        self.interviewer = interviewer
        self.backend = backend
        self.transforms = transforms
        self.eventEmitter = eventEmitter
        self.client = client
    }
}

// MARK: - Pipeline Result

public struct PipelineResult: Sendable {
    public var status: OutcomeStatus
    public var completedNodes: [String]
    public var nodeOutcomes: [String: OutcomeStatus]
    public var context: [String: String]
    public var logsRoot: URL

    public init(
        status: OutcomeStatus,
        completedNodes: [String] = [],
        nodeOutcomes: [String: OutcomeStatus] = [:],
        context: [String: String] = [:],
        logsRoot: URL
    ) {
        self.status = status
        self.completedNodes = completedNodes
        self.nodeOutcomes = nodeOutcomes
        self.context = context
        self.logsRoot = logsRoot
    }
}

// MARK: - Pipeline Engine

public final class PipelineEngine: Sendable {
    private let config: PipelineConfig
    private let registry: HandlerRegistry

    public init(config: PipelineConfig) {
        self.config = config
        self.registry = HandlerRegistry()
        installDefaultHandlers()
    }

    /// Register a custom handler.
    public func registerHandler(_ handler: NodeHandler) {
        registry.register(handler)
    }

    /// Register a handler for a specific type string.
    public func registerHandler(type: String, handler: NodeHandler) {
        registry.register(type: type, handler: handler)
    }

    // MARK: - Run from DOT

    public func run(dot: String) async throws -> PipelineResult {
        let graph = try DOTParser.parse(dot)
        return try await run(graph: graph)
    }

    // MARK: - Run from Graph

    public func run(graph: Graph) async throws -> PipelineResult {
        // 1. Apply transforms
        var g = graph
        let builtInTransforms: [GraphTransform] = [
            VariableExpansionTransform(),
            StylesheetTransform(),
        ]
        for transform in builtInTransforms + config.transforms {
            g = transform.apply(g)
        }

        // 2. Validate
        try PipelineValidator.validateOrRaise(g)

        var cycleIndex = 0
        var loopRestartCount = 0
        var restartStartNode: String? = nil

        while true {
            let cycleLogsRoot = logsRootForCycle(base: config.logsRoot, cycleIndex: cycleIndex)
            let context = PipelineContext()
            if loopRestartCount > 0 {
                context.set("internal.loop_restart_count", String(loopRestartCount))
                context.set("loop_restart", "true")
            }

            let state = ExecutionState(
                graph: g,
                context: context,
                logsRoot: cycleLogsRoot,
                artifactStore: ArtifactStore(logsRoot: cycleLogsRoot),
                cycleIndex: cycleIndex
            )

            try await initializeRun(state: state, startAt: restartStartNode)
            try await executeLoop(state: state)

            if let restartTarget = state.restartTargetNodeId {
                loopRestartCount += 1
                cycleIndex += 1

                // Enforce maximum cycle limit from graph's default_max_retry.
                let maxCycles = g.attributes.defaultMaxRetry
                if loopRestartCount >= maxCycles {
                    fputs("[PipelineEngine] loop_restart limit reached (\(loopRestartCount)/\(maxCycles) cycles). Stopping.\n", stderr)
                    state.pipelineStatus = .fail
                    return finalizeRun(state: state)
                }

                restartStartNode = restartTarget
                continue
            }

            return finalizeRun(state: state)
        }
    }

    // MARK: - Resume from Checkpoint

    public func resume(dot: String, checkpoint: Checkpoint) async throws -> PipelineResult {
        let graph = try DOTParser.parse(dot)

        var g = graph
        let builtInTransforms: [GraphTransform] = [
            VariableExpansionTransform(),
            StylesheetTransform(),
        ]
        for transform in builtInTransforms + config.transforms {
            g = transform.apply(g)
        }

        try PipelineValidator.validateOrRaise(g)

        let context = PipelineContext()
        // Restore context from checkpoint
        for (key, value) in checkpoint.contextValues {
            context.set(key, value)
        }
        for entry in checkpoint.logs {
            context.appendLog(entry)
        }

        var state = ExecutionState(
            graph: g,
            context: context,
            logsRoot: config.logsRoot,
            artifactStore: ArtifactStore(logsRoot: config.logsRoot),
            cycleIndex: 0
        )
        state.completedNodes = checkpoint.completedNodes
        state.nodeRetries = checkpoint.nodeRetries
        state.currentNodeId = checkpoint.currentNode

        // Restore node outcomes from checkpoint
        for (nodeId, statusStr) in checkpoint.nodeOutcomes {
            if let status = OutcomeStatus(rawValue: statusStr) {
                state.nodeOutcomes[nodeId] = status
            }
        }

        // Determine the next node after the checkpointed current node
        if let currentId = state.currentNodeId,
           g.node(currentId) != nil {
            // The checkpoint's current_node is the last completed node.
            // Use the actual last outcome from the checkpoint instead of hardcoded success.
            let lastStatusStr = checkpoint.nodeOutcomes[currentId] ?? "success"
            let lastStatus = OutcomeStatus(rawValue: lastStatusStr) ?? .success
            let preferredLabel = context.getString("preferred_label")
            let lastOutcome = Outcome(
                status: lastStatus,
                preferredLabel: preferredLabel
            )
            if let nextEdge = selectNextEdge(
                from: currentId,
                outcome: lastOutcome,
                context: context,
                graph: g
            ) {
                state.currentNodeId = nextEdge.to
            }
        }

        while true {
            try await executeLoop(state: state)
            if let restartTarget = state.restartTargetNodeId {
                let loopCount = max(1, state.context.getInt("internal.loop_restart_count"))

                let maxCycles = g.attributes.defaultMaxRetry
                if loopCount >= maxCycles {
                    fputs("[PipelineEngine] loop_restart limit reached (\(loopCount)/\(maxCycles) cycles). Stopping.\n", stderr)
                    state.pipelineStatus = .fail
                    return finalizeRun(state: state)
                }

                let nextLogsRoot = logsRootForCycle(base: config.logsRoot, cycleIndex: loopCount)
                let nextContext = PipelineContext()
                nextContext.set("internal.loop_restart_count", String(loopCount))
                nextContext.set("loop_restart", "true")
                let nextState = ExecutionState(
                    graph: g,
                    context: nextContext,
                    logsRoot: nextLogsRoot,
                    artifactStore: ArtifactStore(logsRoot: nextLogsRoot),
                    cycleIndex: loopCount
                )
                try await initializeRun(state: nextState, startAt: restartTarget)
                state = nextState
                continue
            }
            return finalizeRun(state: state)
        }
    }

    // MARK: - Private Execution

    private func installDefaultHandlers() {
        registerDefaultHandlers(
            registry: registry,
            backend: config.backend,
            interviewer: config.interviewer
        )
    }

    private func initializeRun(state: ExecutionState, startAt: String? = nil) async throws {
        // Create logs root directory
        try FileManager.default.createDirectory(at: state.logsRoot, withIntermediateDirectories: true)

        // Set initial context
        state.context.set("graph.goal", state.graph.attributes.goal)
        state.context.set("_graph_goal", state.graph.attributes.goal)

        // Find start node (or explicit restart target)
        if let startAt, !startAt.isEmpty {
            guard state.graph.node(startAt) != nil else {
                throw AttractorError.nodeNotFound(startAt)
            }
            state.currentNodeId = startAt
        } else {
            guard let startNode = state.graph.startNode else {
                throw AttractorError.noStartNode
            }
            state.currentNodeId = startNode.id
        }

        // Write manifest
        try writeManifest(state: state)

        // Emit pipeline started
        if let emitter = config.eventEmitter {
            await emitter.emit(PipelineEvent(
                kind: .pipelineStarted,
                data: [
                    "pipeline_id": state.graph.id,
                    "goal": state.graph.attributes.goal
                ]
            ))
        }
    }

    private func executeLoop(state: ExecutionState) async throws {
        while let currentId = state.currentNodeId {
            guard let node = state.graph.node(currentId) else {
                throw AttractorError.nodeNotFound(currentId)
            }

            // Check if already completed (resume scenario)
            if state.completedNodes.contains(currentId) && node.handlerType != .start {
                // Skip already-completed nodes during resume
                let outgoing = state.graph.outgoingEdges(from: currentId)
                if let edge = outgoing.first {
                    state.currentNodeId = edge.to
                } else {
                    state.currentNodeId = nil
                }
                continue
            }

            state.context.set("current_node", currentId)

            // Check if this is a terminal node
            if node.handlerType == .exit {
                // Goal gate enforcement before allowing exit
                try enforceGoalGates(state: state, exitNodeId: currentId)

                // Execute exit handler
                let outcome = try await executeHandler(node: node, state: state)
                await recordOutcome(node: node, outcome: outcome, state: state)

                // Emit pipeline completed
                if let emitter = config.eventEmitter {
                    await emitter.emit(PipelineEvent(
                        kind: .pipelineCompleted,
                        nodeId: currentId
                    ))
                }

                state.currentNodeId = nil
                break
            }

            // Emit stage started
            if let emitter = config.eventEmitter {
                await emitter.emit(PipelineEvent(
                    kind: .stageStarted,
                    nodeId: currentId
                ))
            }

            // Resolve context fidelity and build preamble for LLM nodes
            if node.handlerType == .codergen {
                let fidelity = resolveFidelity(
                    node: node,
                    incomingEdge: state.lastEdge,
                    graph: state.graph
                )
                let preamble = buildPreamble(fidelity: fidelity, state: state)
                state.context.set("_preamble", preamble)
                state.context.set("_fidelity", fidelity.rawValue)
            }

            // Execute handler with retry
            let outcome = try await executeWithRetry(node: node, state: state)

            // Record outcome
            await recordOutcome(node: node, outcome: outcome, state: state)

            // Write status.json
            try writeStatusJSON(nodeId: currentId, outcome: outcome, state: state)

            // Apply context updates
            if !outcome.contextUpdates.isEmpty {
                state.context.applyUpdates(outcome.contextUpdates)
            }
            state.context.set("outcome", outcome.status.rawValue)
            if !outcome.preferredLabel.isEmpty {
                state.context.set("preferred_label", outcome.preferredLabel)
            }

            // Save checkpoint
            try await saveCheckpoint(state: state)

            // Emit stage completed/failed
            if let emitter = config.eventEmitter {
                let kind: PipelineEventKind = (outcome.status == .fail) ? .stageFailed : .stageCompleted
                await emitter.emit(PipelineEvent(
                    kind: kind,
                    nodeId: currentId,
                    data: ["outcome": outcome.status.rawValue]
                ))
            }

            // Handle loop_restart
            if node.handlerType == .parallel && outcome.status != .fail {
                if let fanInNodeId = outcome.suggestedNextIds.first(where: {
                    guard let targetNode = state.graph.node($0) else { return false }
                    return targetNode.handlerType == .parallelFanIn
                }) {
                    state.lastEdge = nil
                    state.currentNodeId = fanInNodeId
                    continue
                }
            }

            // Select next edge
            if outcome.status == .fail {
                // Failure routing
                let failEdge = try resolveFailureEdge(node: node, outcome: outcome, state: state)
                if let edge = failEdge {
                    state.lastEdge = edge
                    if edge.loopRestart {
                        let loopCount = state.context.getInt("internal.loop_restart_count") + 1
                        state.context.set("internal.loop_restart_count", String(loopCount))
                        state.context.set("loop_restart", "true")
                        state.restartTargetNodeId = edge.to
                        state.currentNodeId = nil
                        break
                    }
                    state.currentNodeId = edge.to
                } else {
                    // No failure route - terminate with failure
                    state.pipelineStatus = .fail
                    state.currentNodeId = nil

                    if let emitter = config.eventEmitter {
                        await emitter.emit(PipelineEvent(
                            kind: .pipelineFailed,
                            data: ["reason": outcome.failureReason]
                        ))
                    }
                    break
                }
            } else {
                // Normal edge selection
                if let edge = selectNextEdge(from: currentId, outcome: outcome, context: state.context, graph: state.graph) {
                    state.lastEdge = edge
                    if edge.loopRestart {
                        // loop_restart semantics: terminate current run cycle and relaunch from
                        // the target node with a fresh state/log directory.
                        let loopCount = state.context.getInt("internal.loop_restart_count") + 1
                        state.context.set("internal.loop_restart_count", String(loopCount))
                        state.context.set("loop_restart", "true")
                        state.restartTargetNodeId = edge.to
                        state.currentNodeId = nil
                        break
                    }
                    state.currentNodeId = edge.to
                } else {
                    // No outgoing edges - done
                    state.lastEdge = nil
                    state.currentNodeId = nil
                }
            }
        }
    }

    // MARK: - Handler Execution with Retry

    private func executeWithRetry(node: Node, state: ExecutionState) async throws -> Outcome {
        let maxRetries = node.maxRetries > 0 ? node.maxRetries : state.graph.attributes.defaultMaxRetry
        let retryPolicy = effectiveRetryPolicy(node: node, graph: state.graph)
        var attempt = 0

        while true {
            let outcome = try await executeHandler(node: node, state: state)

            // Success or partial success: done
            if outcome.status == .success || outcome.status == .partialSuccess || outcome.status == .skipped {
                // Reset retry counter on success
                state.nodeRetries[node.id] = 0
                return outcome
            }

            // auto_status: if the node has auto_status=true and the handler wrote no
            // explicit status (i.e. the handler didn't actively report success), treat
            // the outcome as SUCCESS. This implements spec §2.6 auto_status behavior.
            if node.autoStatus {
                return Outcome(
                    status: .success,
                    preferredLabel: outcome.preferredLabel,
                    suggestedNextIds: outcome.suggestedNextIds,
                    contextUpdates: outcome.contextUpdates,
                    notes: outcome.notes.isEmpty ? "auto_status=true: auto-generated SUCCESS" : outcome.notes
                )
            }

            // Retry only on explicit RETRY outcomes (FAIL routes immediately)
            let currentRetries = state.nodeRetries[node.id] ?? 0
            if outcome.status == .retry && currentRetries < maxRetries {
                state.nodeRetries[node.id] = currentRetries + 1
                attempt += 1

                state.context.set("internal.retry_count.\(node.id)", String(currentRetries + 1))

                // Emit retrying event
                if let emitter = config.eventEmitter {
                    await emitter.emit(PipelineEvent(
                        kind: .stageRetrying,
                        nodeId: node.id,
                        data: ["attempt": String(attempt)]
                    ))
                }

                // Apply backoff delay
                let delay = retryPolicy.delay(forAttempt: attempt - 1)
                if delay > 0 {
                    try await Task.sleep(for: .milliseconds(Int(delay * 1000)))
                }
                continue
            }

            // Retries exhausted: check allow_partial for RETRY outcomes only.
            // FAIL outcomes do not consume retry budget and should route/fail immediately.
            if node.allowPartial && outcome.status == .retry {
                return Outcome(
                    status: .partialSuccess,
                    preferredLabel: outcome.preferredLabel,
                    suggestedNextIds: outcome.suggestedNextIds,
                    contextUpdates: outcome.contextUpdates,
                    notes: outcome.notes.isEmpty
                        ? "allow_partial=true: accepted PARTIAL_SUCCESS after retry exhaustion"
                        : outcome.notes + " (allow_partial=true: accepted as PARTIAL_SUCCESS)"
                )
            }

            // Retries exhausted or no retries configured
            return outcome
        }
    }

    private func effectiveRetryPolicy(node: Node, graph: Graph) -> PipelineRetryPolicy {
        var policy = config.retryPolicy
        policy = applyRetryPolicyOverrides(attrs: graph.rawAttributes, to: policy)
        policy = applyRetryPolicyOverrides(attrs: node.rawAttributes, to: policy)
        return policy
    }

    private func applyRetryPolicyOverrides(
        attrs: [String: AttributeValue],
        to base: PipelineRetryPolicy
    ) -> PipelineRetryPolicy {
        var policy = base

        if let strategyRaw = firstString(
            attrs,
            keys: ["retry_policy.strategy", "retry.strategy", "retry_strategy"]
        ),
           let strategy = RetryStrategy(rawValue: strategyRaw.trimmingCharacters(in: .whitespacesAndNewlines).lowercased())
        {
            policy.strategy = strategy
        }

        if let baseDelay = firstDouble(
            attrs,
            keys: ["retry_policy.base_delay", "retry.base_delay", "retry_base_delay"]
        ),
           baseDelay >= 0
        {
            policy.baseDelay = baseDelay
        }

        if let maxDelay = firstDouble(
            attrs,
            keys: ["retry_policy.max_delay", "retry.max_delay", "retry_max_delay"]
        ),
           maxDelay >= 0
        {
            policy.maxDelay = maxDelay
        }

        if let multiplier = firstDouble(
            attrs,
            keys: [
                "retry_policy.backoff_multiplier",
                "retry.backoff_multiplier",
                "retry_backoff_multiplier",
            ]
        ),
           multiplier > 0
        {
            policy.backoffMultiplier = multiplier
        }

        if let jitter = firstBool(
            attrs,
            keys: ["retry_policy.jitter", "retry.jitter", "retry_jitter"]
        ) {
            policy.jitter = jitter
        }

        return policy
    }

    private func firstString(_ attrs: [String: AttributeValue], keys: [String]) -> String? {
        for key in keys {
            if let value = attrs[key] {
                return value.stringValue
            }
        }
        return nil
    }

    private func firstDouble(_ attrs: [String: AttributeValue], keys: [String]) -> Double? {
        for key in keys {
            guard let value = attrs[key] else { continue }
            switch value {
            case .float(let f):
                return f
            case .integer(let i):
                return Double(i)
            case .string(let s):
                if let parsed = Double(s.trimmingCharacters(in: .whitespacesAndNewlines)) {
                    return parsed
                }
            default:
                continue
            }
        }
        return nil
    }

    private func firstBool(_ attrs: [String: AttributeValue], keys: [String]) -> Bool? {
        for key in keys {
            guard let value = attrs[key] else { continue }
            switch value {
            case .boolean(let b):
                return b
            case .string(let s):
                let normalized = s.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
                if ["true", "1", "yes", "on"].contains(normalized) {
                    return true
                }
                if ["false", "0", "no", "off"].contains(normalized) {
                    return false
                }
            default:
                continue
            }
        }
        return nil
    }

    private func executeHandler(node: Node, state: ExecutionState) async throws -> Outcome {
        let handler: NodeHandler
        if let h = registry.resolve(node.handlerType) {
            handler = h
        } else if let h = registry.resolve(node.type) {
            handler = h
        } else {
            // Fallback to codergen
            guard let h = registry.resolve(.codergen) else {
                throw AttractorError.handlerNotFound(node.handlerType.rawValue)
            }
            handler = h
        }

        // Execute with optional timeout
        if let timeout = node.timeout {
            // Extract values before task group to avoid capturing `state` across concurrency boundaries.
            let ctx = state.context
            let graph = state.graph
            let logsRoot = state.logsRoot
            return try await withThrowingTaskGroup(of: Outcome.self) { group in
                group.addTask {
                    try await handler.execute(
                        node: node,
                        context: ctx,
                        graph: graph,
                        logsRoot: logsRoot
                    )
                }
                group.addTask {
                    try await Task.sleep(for: timeout)
                    throw AttractorError.timeout(node.id, timeout.seconds)
                }

                guard let result = try await group.next() else {
                    throw AttractorError.executionFailed("Handler returned no result for node \(node.id)")
                }
                group.cancelAll()
                return result
            }
        }

        do {
            return try await handler.execute(
                node: node,
                context: state.context,
                graph: state.graph,
                logsRoot: state.logsRoot
            )
        } catch let error as AttractorError {
            throw error
        } catch {
            // Convert handler exceptions to FAIL outcomes
            return Outcome.fail(reason: error.localizedDescription)
        }
    }

    // MARK: - 5-Step Edge Selection

    func selectNextEdge(
        from nodeId: String,
        outcome: Outcome,
        context: PipelineContext,
        graph: Graph
    ) -> Edge? {
        let outgoing = graph.outgoingEdges(from: nodeId)
        if outgoing.isEmpty { return nil }

        // Step 1: Condition-matching edges
        var conditionMatched: [Edge] = []
        for edge in outgoing {
            let condition = edge.condition.trimmingCharacters(in: .whitespaces)
            if condition.isEmpty { continue }
            if let expr = try? ConditionParser.parse(condition) {
                if expr.evaluate(
                    outcome: outcome.status.rawValue,
                    preferredLabel: outcome.preferredLabel,
                    context: context
                ) {
                    conditionMatched.append(edge)
                }
            }
        }
        if !conditionMatched.isEmpty {
            return bestByWeightThenLexical(conditionMatched)
        }

        let eligibleForLabelAndSuggestions = outgoing.filter { edge in
            let condition = edge.condition.trimmingCharacters(in: .whitespaces)
            if condition.isEmpty { return true }
            guard let expr = try? ConditionParser.parse(condition) else { return false }
            return expr.evaluate(
                outcome: outcome.status.rawValue,
                preferredLabel: outcome.preferredLabel,
                context: context
            )
        }

        // Step 2: Preferred label match (with normalization)
        if !outcome.preferredLabel.isEmpty {
            let normalizedPreferred = normalizeLabel(outcome.preferredLabel)
            for edge in eligibleForLabelAndSuggestions {
                if normalizeLabel(edge.label) == normalizedPreferred {
                    return edge
                }
            }
        }

        // Step 3: Suggested next IDs
        if !outcome.suggestedNextIds.isEmpty {
            for edge in eligibleForLabelAndSuggestions {
                if outcome.suggestedNextIds.contains(edge.to) {
                    return edge
                }
            }
        }

        // Step 4 & 5: Among unconditional edges, highest weight with lexical tiebreak
        let unconditional = outgoing.filter { $0.condition.trimmingCharacters(in: .whitespaces).isEmpty }
        if unconditional.isEmpty {
            // All edges are conditional and none matched - use first edge as fallback
            return outgoing.first
        }

        return bestByWeightThenLexical(unconditional)
    }

    /// Select the best edge by weight (higher wins) then lexical tiebreak on target node ID.
    private func bestByWeightThenLexical(_ edges: [Edge]) -> Edge? {
        edges.sorted { a, b in
            if a.weight != b.weight {
                return a.weight > b.weight
            }
            return a.to < b.to
        }.first
    }

    /// Normalize a label for preferred-label matching per spec §3.3:
    /// lowercase, trim whitespace, strip accelerator prefixes like "[Y] ", "Y) ", "Y - ".
    private func normalizeLabel(_ label: String) -> String {
        var s = label.trimmingCharacters(in: .whitespaces).lowercased()
        // Strip [X] prefix
        if s.hasPrefix("["), let closeBracket = s.firstIndex(of: "]") {
            let afterBracket = s.index(after: closeBracket)
            s = String(s[afterBracket...]).trimmingCharacters(in: .whitespaces)
        }
        // Strip X) prefix
        else if s.count >= 2 {
            let idx1 = s.index(after: s.startIndex)
            if s[idx1] == ")" {
                s = String(s[s.index(s.startIndex, offsetBy: 2)...]).trimmingCharacters(in: .whitespaces)
            }
        }
        // Strip X - prefix
        if s.count >= 3 {
            let idx1 = s.index(s.startIndex, offsetBy: 1)
            let idx2 = s.index(s.startIndex, offsetBy: 2)
            if s[idx1] == " " && s[idx2] == "-" {
                s = String(s[s.index(s.startIndex, offsetBy: 3)...]).trimmingCharacters(in: .whitespaces)
            }
        }
        return s
    }

    // MARK: - Goal Gate Enforcement

    private func enforceGoalGates(state: ExecutionState, exitNodeId: String) throws {
        let goalGateNodes = state.graph.goalGateNodes
        if goalGateNodes.isEmpty { return }

        var unsatisfied: [String] = []
        for node in goalGateNodes {
            if let outcomeStatus = state.nodeOutcomes[node.id] {
                // Goal gates require full success only - partial_success does not satisfy
                if outcomeStatus != .success {
                    unsatisfied.append(node.id)
                }
            } else {
                // Goal gate node was never visited
                unsatisfied.append(node.id)
            }
        }

        if unsatisfied.isEmpty { return }

        // Try retry_target: node-level first, then graph-level
        // For simplicity during exit handling, check if we can route to a retry target
        // If retry targets exist, route to them instead of exiting
        for nodeId in unsatisfied {
            if let node = state.graph.node(nodeId) {
                if !node.retryTarget.isEmpty, state.graph.node(node.retryTarget) != nil {
                    state.currentNodeId = node.retryTarget
                    return
                }
                if !node.fallbackRetryTarget.isEmpty, state.graph.node(node.fallbackRetryTarget) != nil {
                    state.currentNodeId = node.fallbackRetryTarget
                    return
                }
            }
        }

        // Graph-level retry targets
        if !state.graph.attributes.retryTarget.isEmpty,
           state.graph.node(state.graph.attributes.retryTarget) != nil {
            state.currentNodeId = state.graph.attributes.retryTarget
            return
        }
        if !state.graph.attributes.fallbackRetryTarget.isEmpty,
           state.graph.node(state.graph.attributes.fallbackRetryTarget) != nil {
            state.currentNodeId = state.graph.attributes.fallbackRetryTarget
            return
        }

        throw AttractorError.goalGateUnsatisfied(unsatisfied)
    }

    // MARK: - Failure Routing

    /// Returns the matching edge for failure routing, preserving loop_restart metadata.
    private func resolveFailureEdge(node: Node, outcome: Outcome, state: ExecutionState) throws -> Edge? {
        // Step 1: Follow fail edge (condition="outcome=fail")
        let outgoing = state.graph.outgoingEdges(from: node.id)
        for edge in outgoing {
            let condition = edge.condition.trimmingCharacters(in: .whitespaces)
            if condition.isEmpty { continue }
            if let expr = try? ConditionParser.parse(condition) {
                if expr.evaluate(
                    outcome: outcome.status.rawValue,
                    preferredLabel: outcome.preferredLabel,
                    context: state.context
                ) {
                    return edge
                }
            }
        }

        // Step 2: Node's retry_target (synthesize edge)
        if !node.retryTarget.isEmpty, state.graph.node(node.retryTarget) != nil {
            return Edge(from: node.id, to: node.retryTarget)
        }

        // Step 3: Node's fallback_retry_target
        if !node.fallbackRetryTarget.isEmpty, state.graph.node(node.fallbackRetryTarget) != nil {
            return Edge(from: node.id, to: node.fallbackRetryTarget)
        }

        // Step 4: Graph's retry_target
        if !state.graph.attributes.retryTarget.isEmpty,
           state.graph.node(state.graph.attributes.retryTarget) != nil {
            return Edge(from: node.id, to: state.graph.attributes.retryTarget)
        }

        // Step 5: Graph's fallback_retry_target
        if !state.graph.attributes.fallbackRetryTarget.isEmpty,
           state.graph.node(state.graph.attributes.fallbackRetryTarget) != nil {
            return Edge(from: node.id, to: state.graph.attributes.fallbackRetryTarget)
        }

        // Step 6: No failure route
        return nil
    }

    // MARK: - Record Outcome

    private func recordOutcome(node: Node, outcome: Outcome, state: ExecutionState) async {
        state.nodeOutcomes[node.id] = outcome.status
        if !state.completedNodes.contains(node.id) {
            state.completedNodes.append(node.id)
        }
        state.context.appendLog("[\(node.id)] \(outcome.status.rawValue): \(outcome.notes)")

        // Persist node outcomes into the shared artifact store so larger stage payloads
        // can be retrieved without bloating routing context.
        let statusArtifactID = "\(node.id)-status-\(state.completedNodes.count)"
        if let info = try? await state.artifactStore.store(
            artifactId: statusArtifactID,
            name: "status:\(node.id)",
            data: outcome.toStatusJSON()
        ) {
            state.context.set("artifact.\(node.id).status", info.id)
        }

        if let response = outcome.contextUpdates["last_response"], !response.isEmpty {
            let responseArtifactID = "\(node.id)-response-\(state.completedNodes.count)"
            if let info = try? await state.artifactStore.store(
                artifactId: responseArtifactID,
                name: "response:\(node.id)",
                data: response
            ) {
                state.context.set("artifact.\(node.id).response", info.id)
            }
        }

        if let toolOutput = outcome.contextUpdates["tool.output"], !toolOutput.isEmpty {
            let toolArtifactID = "\(node.id)-tool-output-\(state.completedNodes.count)"
            if let info = try? await state.artifactStore.store(
                artifactId: toolArtifactID,
                name: "tool.output:\(node.id)",
                data: toolOutput
            ) {
                state.context.set("artifact.\(node.id).tool_output", info.id)
            }
        }
    }

    // MARK: - Write Files

    private func writeManifest(state: ExecutionState) throws {
        let manifest: [String: Any] = [
            "pipeline_id": state.graph.id,
            "graph_id": state.graph.id,
            "goal": state.graph.attributes.goal,
            "started_at": Date().ISO8601Format(),
            "node_count": state.graph.nodes.count,
            "edge_count": state.graph.edges.count,
            "cycle_index": state.cycleIndex,
        ]
        let data = try JSONSerialization.data(withJSONObject: manifest, options: [.prettyPrinted, .sortedKeys])
        let url = state.logsRoot.appendingPathComponent("manifest.json")
        try data.write(to: url)
    }

    private func logsRootForCycle(base: URL, cycleIndex: Int) -> URL {
        guard cycleIndex > 0 else { return base }
        return base
            .deletingLastPathComponent()
            .appendingPathComponent(base.lastPathComponent + "_cycle\(cycleIndex)")
    }

    private func writeStatusJSON(nodeId: String, outcome: Outcome, state: ExecutionState) throws {
        let stageDir = state.logsRoot.appendingPathComponent(nodeId)
        try FileManager.default.createDirectory(at: stageDir, withIntermediateDirectories: true)

        let statusJSON = outcome.toStatusJSON()
        let data = try JSONSerialization.data(withJSONObject: statusJSON, options: [.prettyPrinted, .sortedKeys])
        let url = stageDir.appendingPathComponent("status.json")
        try data.write(to: url)
    }

    private func saveCheckpoint(state: ExecutionState) async throws {
        let outcomeStrings = state.nodeOutcomes.mapValues { $0.rawValue }
        let checkpoint = Checkpoint(
            timestamp: Date(),
            currentNode: state.currentNodeId ?? "",
            completedNodes: state.completedNodes,
            nodeRetries: state.nodeRetries,
            nodeOutcomes: outcomeStrings,
            contextValues: state.context.serializableSnapshot(),
            logs: state.context.getLogs()
        )
        let url = state.logsRoot.appendingPathComponent("checkpoint.json")
        try checkpoint.save(to: url)

        // Emit checkpoint event
        if let emitter = config.eventEmitter {
            await emitter.emit(PipelineEvent(
                kind: .checkpointSaved,
                nodeId: state.currentNodeId
            ))
        }
    }

    // MARK: - Context Fidelity

    private func resolveFidelity(node: Node, incomingEdge: Edge?, graph: Graph) -> ContextFidelity {
        ContextFidelity.resolve(
            edgeFidelity: incomingEdge?.fidelity ?? "",
            nodeFidelity: node.fidelity,
            graphDefault: graph.attributes.defaultFidelity
        )
    }

    private func buildPreamble(fidelity: ContextFidelity, state: ExecutionState) -> String {
        let goal = state.graph.attributes.goal

        switch fidelity {
        case .full:
            // Full fidelity: no preamble needed, session reuse handles it
            return ""
        case .truncate:
            // Minimal: only goal and run ID
            return "Pipeline goal: \(goal)"
        case .compact:
            // Structured bullet-point summary
            var parts: [String] = []
            parts.append("Pipeline goal: \(goal)")
            if !state.completedNodes.isEmpty {
                let recent = state.completedNodes.suffix(5)
                let nodeList = recent.map { nodeId -> String in
                    let status = state.nodeOutcomes[nodeId]?.rawValue ?? "unknown"
                    return "  - \(nodeId): \(status)"
                }.joined(separator: "\n")
                parts.append("Recent stages:\n\(nodeList)")
            }
            return parts.joined(separator: "\n\n")
        case .summaryLow:
            // Brief: minimal event counts
            var parts: [String] = []
            parts.append("Goal: \(goal)")
            parts.append("Completed: \(state.completedNodes.count) stages")
            let successes = state.nodeOutcomes.values.filter { $0 == .success }.count
            let failures = state.nodeOutcomes.values.filter { $0 == .fail }.count
            parts.append("Results: \(successes) success, \(failures) fail")
            return parts.joined(separator: "\n")
        case .summaryMedium:
            // Moderate detail
            var parts: [String] = []
            parts.append("Pipeline goal: \(goal)")
            parts.append("Stages completed: \(state.completedNodes.count)")
            if !state.completedNodes.isEmpty {
                let recent = state.completedNodes.suffix(8)
                let nodeList = recent.map { nodeId -> String in
                    let status = state.nodeOutcomes[nodeId]?.rawValue ?? "unknown"
                    return "  - \(nodeId): \(status)"
                }.joined(separator: "\n")
                parts.append("Recent stages:\n\(nodeList)")
            }
            // Include key context values
            let snapshot = state.context.serializableSnapshot()
            let contextKeys = snapshot.filter { !$0.key.hasPrefix("internal.") && !$0.key.hasPrefix("_") }
            if !contextKeys.isEmpty {
                let kvList = contextKeys.prefix(10).map { "  \($0.key) = \($0.value)" }.joined(separator: "\n")
                parts.append("Context:\n\(kvList)")
            }
            return parts.joined(separator: "\n\n")
        case .summaryHigh:
            // Detailed: comprehensive context
            var parts: [String] = []
            parts.append("Pipeline goal: \(goal)")
            parts.append("Stages completed: \(state.completedNodes.count)")
            if !state.completedNodes.isEmpty {
                let nodeList = state.completedNodes.map { nodeId -> String in
                    let status = state.nodeOutcomes[nodeId]?.rawValue ?? "unknown"
                    return "  - \(nodeId): \(status)"
                }.joined(separator: "\n")
                parts.append("All stages:\n\(nodeList)")
            }
            let snapshot = state.context.serializableSnapshot()
            let contextKeys = snapshot.filter { !$0.key.hasPrefix("_") }
            if !contextKeys.isEmpty {
                let kvList = contextKeys.map { "  \($0.key) = \($0.value)" }.joined(separator: "\n")
                parts.append("Full context:\n\(kvList)")
            }
            let logs = state.context.getLogs()
            if !logs.isEmpty {
                let recentLogs = logs.suffix(20)
                parts.append("Recent logs:\n" + recentLogs.joined(separator: "\n"))
            }
            return parts.joined(separator: "\n\n")
        }
    }

    // MARK: - Finalize

    private func finalizeRun(state: ExecutionState) -> PipelineResult {
        // Determine overall status
        let status: OutcomeStatus
        if state.pipelineStatus != nil {
            status = state.pipelineStatus!
        } else {
            // Check if all goal gates are satisfied
            let goalGates = state.graph.goalGateNodes
            if goalGates.isEmpty {
                status = .success
            } else {
                // Goal gates require full success only - partial_success does not satisfy
                let allSatisfied = goalGates.allSatisfy { node in
                    let s = state.nodeOutcomes[node.id]
                    return s == .success
                }
                status = allSatisfied ? .success : .fail
            }
        }

        return PipelineResult(
            status: status,
            completedNodes: state.completedNodes,
            nodeOutcomes: state.nodeOutcomes,
            context: state.context.serializableSnapshot(),
            logsRoot: state.logsRoot
        )
    }
}

// MARK: - Execution State

private final class ExecutionState {
    let graph: Graph
    var context: PipelineContext
    var logsRoot: URL
    let artifactStore: ArtifactStore
    let cycleIndex: Int
    var currentNodeId: String?
    var completedNodes: [String] = []
    var nodeOutcomes: [String: OutcomeStatus] = [:]
    var nodeRetries: [String: Int] = [:]
    var pipelineStatus: OutcomeStatus?
    var lastEdge: Edge?
    var restartTargetNodeId: String?

    init(
        graph: Graph,
        context: PipelineContext,
        logsRoot: URL,
        artifactStore: ArtifactStore,
        cycleIndex: Int
    ) {
        self.graph = graph
        self.context = context
        self.logsRoot = logsRoot
        self.artifactStore = artifactStore
        self.cycleIndex = cycleIndex
    }
}

// MARK: - Duration Extension

extension Duration {
    var seconds: Double {
        let (seconds, attoseconds) = self.components
        return Double(seconds) + Double(attoseconds) / 1e18
    }
}
