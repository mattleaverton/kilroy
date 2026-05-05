import Foundation
import OmniACP
import OmniACPModel

public struct ACPBackendConfiguration: Sendable {
    public var agentPath: String?
    public var agentArguments: [String]
    public var workingDirectory: String?
    public var environment: [String: String]
    public var requestTimeout: Duration?
    public var modeID: String?

    public init(
        agentPath: String? = nil,
        agentArguments: [String] = [],
        workingDirectory: String? = nil,
        environment: [String: String] = [:],
        requestTimeout: Duration? = .seconds(120),
        modeID: String? = nil
    ) {
        self.agentPath = agentPath
        self.agentArguments = agentArguments
        self.workingDirectory = workingDirectory
        self.environment = environment
        self.requestTimeout = requestTimeout
        self.modeID = modeID
    }
}

public struct ACPExecutionConfiguration: Sendable {
    public var agentPath: String
    public var agentArguments: [String]
    public var workingDirectory: String
    public var environment: [String: String]
    public var requestTimeout: Duration?
    public var modeID: String?

    public init(
        agentPath: String,
        agentArguments: [String],
        workingDirectory: String,
        environment: [String: String],
        requestTimeout: Duration?,
        modeID: String?
    ) {
        self.agentPath = agentPath
        self.agentArguments = agentArguments
        self.workingDirectory = workingDirectory
        self.environment = environment
        self.requestTimeout = requestTimeout
        self.modeID = modeID
    }
}

public protocol ACPTransportProvider: Sendable {
    func makeTransport(configuration: ACPExecutionConfiguration) async throws -> any Transport
}

public protocol ACPClientDelegateProvider: Sendable {
    func makeDelegate(configuration: ACPExecutionConfiguration) async throws -> any ClientDelegate
}

public struct DefaultACPTransportProvider: ACPTransportProvider {
    public init() {}

    public func makeTransport(configuration: ACPExecutionConfiguration) async throws -> any Transport {
        if let url = remoteAgentURL(from: configuration.agentPath) {
            switch url.scheme?.lowercased() {
            case "ws", "wss":
                return WebSocketTransport(configuration: .init(
                    url: url,
                    timeout: configuration.requestTimeout
                ))
            case "http", "https":
                return HTTPSSETransport(configuration: .init(
                    url: url,
                    requestTimeout: configuration.requestTimeout
                ))
            default:
                break
            }
        }

        return StdioTransport(configuration: .init(
            executablePath: configuration.agentPath,
            arguments: configuration.agentArguments,
            workingDirectory: configuration.workingDirectory,
            environment: configuration.environment
        ))
    }

    private func remoteAgentURL(from rawValue: String) -> URL? {
        guard let url = URL(string: rawValue),
              let scheme = url.scheme?.lowercased(),
              ["ws", "wss", "http", "https"].contains(scheme)
        else {
            return nil
        }
        return url
    }
}

public struct DefaultACPClientDelegateProvider: ACPClientDelegateProvider {
    public var permissionStrategy: PermissionStrategy

    public init(permissionStrategy: PermissionStrategy = .autoApprove) {
        self.permissionStrategy = permissionStrategy
    }

    public func makeDelegate(configuration: ACPExecutionConfiguration) async throws -> any ClientDelegate {
        DefaultClientDelegate(
            rootDirectory: URL(fileURLWithPath: configuration.workingDirectory, isDirectory: true),
            permissionStrategy: permissionStrategy
        )
    }
}

public final class ACPAgentBackend: CodergenBackend, Sendable {
    private let configuration: ACPBackendConfiguration
    private let transportProvider: any ACPTransportProvider
    private let delegateProvider: any ACPClientDelegateProvider

    public init(
        configuration: ACPBackendConfiguration = ACPBackendConfiguration(),
        transportProvider: any ACPTransportProvider = DefaultACPTransportProvider(),
        delegateProvider: any ACPClientDelegateProvider = DefaultACPClientDelegateProvider()
    ) {
        self.configuration = configuration
        self.transportProvider = transportProvider
        self.delegateProvider = delegateProvider
    }

    public convenience init(
        configuration: ACPBackendConfiguration = ACPBackendConfiguration(),
        interactivePermissions: Bool,
        transportProvider: any ACPTransportProvider = DefaultACPTransportProvider()
    ) {
        self.init(
            configuration: configuration,
            transportProvider: transportProvider,
            delegateProvider: DefaultACPClientDelegateProvider(
                permissionStrategy: interactivePermissions ? .consolePrompt : .autoApprove
            )
        )
    }

    public func run(
        prompt: String,
        model: String,
        provider: String,
        reasoningEffort: String,
        context: PipelineContext
    ) async throws -> CodergenResult {
        let execution = try resolveConfiguration(
            model: model,
            provider: provider,
            reasoningEffort: reasoningEffort,
            context: context
        )
        let client = Client(
            name: "OmniAIAttractor",
            version: "1.0.0",
            capabilities: .init(fs: .init(readTextFile: true, writeTextFile: true))
        )

        do {
            let delegate = try await delegateProvider.makeDelegate(configuration: execution)
            await client.setDelegate(delegate)
            let transport = try await transportProvider.makeTransport(configuration: execution)
            _ = try await client.connect(transport: transport, timeout: execution.requestTimeout)

            let session = try await client.newSession(
                cwd: execution.workingDirectory,
                mcpServers: [],
                timeout: execution.requestTimeout
            )
            if let modeID = execution.modeID, !modeID.isEmpty {
                try await client.setMode(sessionID: session.sessionID, modeID: modeID, timeout: execution.requestTimeout)
            }

            let collector = ACPUpdateCollector()
            let observerID = await client.addNotificationObserver { notification in
                guard notification.method == SessionUpdateNotification.name else {
                    return
                }
                do {
                    let params = try notification.decodeParameters(SessionUpdateNotification.Parameters.self)
                    guard params.sessionID == session.sessionID else {
                        return
                    }
                    await collector.apply(params.update)
                } catch {
                    context.appendLog("[ACP] Failed to decode notification: \(error)")
                }
            }

            do {
                let promptResult = try await client.prompt(
                    sessionID: session.sessionID,
                    prompt: buildPrompt(
                        prompt: prompt,
                        model: model,
                        provider: provider,
                        reasoningEffort: reasoningEffort,
                        context: context
                    ),
                    timeout: execution.requestTimeout
                )
                await client.removeNotificationObserver(observerID)
                let collected = await collector.snapshot(sessionID: session.sessionID, stopReason: promptResult.stopReason)
                var parsed = parseResponse(collected.responseText)
                parsed.contextUpdates.merge(collected.contextUpdates) { current, _ in current }
                if !collected.notes.isEmpty {
                    parsed.notes = parsed.notes.isEmpty ? collected.notes : parsed.notes + "\n" + collected.notes
                }
                await client.disconnect()
                return parsed
            } catch {
                await client.removeNotificationObserver(observerID)
                await client.disconnect()
                throw error
            }
        } catch {
            await client.disconnect()
            throw error
        }
    }

    private func resolveConfiguration(
        model: String,
        provider: String,
        reasoningEffort: String,
        context: PipelineContext,
        environment: [String: String] = ProcessInfo.processInfo.environment
    ) throws -> ACPExecutionConfiguration {
        let agentPath = firstNonEmpty([
            context.getString("_current_node_acp_agent_path"),
            configuration.agentPath ?? "",
            environment["ATTRACTOR_ACP_AGENT_BIN"] ?? "",
        ])
        guard !agentPath.isEmpty else {
            throw AttractorError.llmError("ACP backend requires an agent endpoint via acp_agent_path or --acp-agent (local executable path, ws/wss URL, or http/https URL)")
        }
        let arguments = firstNonEmpty([
            context.getString("_current_node_acp_agent_args"),
            configuration.agentArguments.joined(separator: ","),
            environment["ATTRACTOR_ACP_AGENT_ARGS"] ?? "",
        ])
        let workingDirectory = firstNonEmpty([
            context.getString("_current_node_acp_cwd"),
            configuration.workingDirectory ?? "",
            environment["ATTRACTOR_ACP_CWD"] ?? "",
            FileManager.default.currentDirectoryPath,
        ])
        let timeoutString = firstNonEmpty([
            context.getString("_current_node_acp_timeout_seconds"),
            configuration.requestTimeout.map { String($0.components.seconds) } ?? "",
            environment["ATTRACTOR_ACP_TIMEOUT_SECONDS"] ?? "",
        ])
        let timeout: Duration?
        if let seconds = Double(timeoutString), seconds > 0 {
            timeout = .milliseconds(Int64(seconds * 1_000))
        } else {
            timeout = configuration.requestTimeout
        }
        let modeID = firstNonEmpty([
            context.getString("_current_node_acp_mode"),
            configuration.modeID ?? "",
            environment["ATTRACTOR_ACP_MODE"] ?? "",
        ])
        var mergedEnvironment = configuration.environment
        if let extraPath = environment["ATTRACTOR_ACP_EXTRA_PATH"], !extraPath.isEmpty {
            let currentPath = mergedEnvironment["PATH"] ?? environment["PATH"] ?? ""
            mergedEnvironment["PATH"] = extraPath + (currentPath.isEmpty ? "" : ":" + currentPath)
        }
        mergedEnvironment["OMNIAI_ATTRACTOR_MODEL"] = model
        mergedEnvironment["OMNIAI_ATTRACTOR_PROVIDER"] = provider
        mergedEnvironment["OMNIAI_ATTRACTOR_REASONING_EFFORT"] = reasoningEffort
        return ACPExecutionConfiguration(
            agentPath: agentPath,
            agentArguments: parseStringList(arguments),
            workingDirectory: workingDirectory,
            environment: mergedEnvironment,
            requestTimeout: timeout,
            modeID: modeID.isEmpty ? nil : modeID
        )
    }

    private func buildPrompt(
        prompt: String,
        model: String,
        provider: String,
        reasoningEffort: String,
        context: PipelineContext
    ) -> [ContentBlock] {
        var parts: [String] = []
        let goal = context.getString("_graph_goal")
        if !goal.isEmpty {
            parts.append("Pipeline goal: \(goal)")
        }
        let lastStage = context.getString("last_stage")
        let lastResponse = context.getString("last_response")
        if !lastStage.isEmpty && !lastResponse.isEmpty {
            parts.append("Previous stage (\(lastStage)) output:\n\n\(lastResponse)")
        }
        let toolOutput = context.getString("tool.output")
        if !toolOutput.isEmpty {
            parts.append("Previous tool output:\n\n\(toolOutput)")
        }
        let preamble = context.getString("_preamble")
        if !preamble.isEmpty {
            parts.append("Pipeline preamble:\n\n\(preamble)")
        }
        parts.append("Preferred model hint: \(model) via \(provider) with reasoning effort \(reasoningEffort).")
        parts.append(prompt)
        parts.append(statusInstruction())
        return [.text(parts.joined(separator: "\n\n"))]
    }

    private func statusInstruction() -> String {
        """
        After completing your task, include a JSON status block at the end of your response in the following format:

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
        """
    }

    private func parseResponse(_ response: String) -> CodergenResult {
        if let jsonBlock = extractJSONBlock(from: response),
           let data = jsonBlock.data(using: .utf8),
           let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
            let outcomeString = json["outcome"] as? String ?? OutcomeStatus.success.rawValue
            let status = OutcomeStatus(rawValue: outcomeString) ?? .success
            let preferredLabel = json["preferred_next_label"] as? String ?? ""
            let notes = json["notes"] as? String ?? ""
            var contextUpdates: [String: String] = [:]
            if let updates = json["context_updates"] as? [String: Any] {
                for (key, value) in updates {
                    contextUpdates[key] = String(describing: value)
                }
            }
            let suggestedNextIds = json["suggested_next_ids"] as? [String] ?? []
            return CodergenResult(
                response: response,
                status: status,
                contextUpdates: contextUpdates,
                preferredLabel: preferredLabel,
                suggestedNextIds: suggestedNextIds,
                notes: notes
            )
        }
        return CodergenResult(
            response: response,
            status: .partialSuccess,
            notes: "WARNING: No structured status block found in ACP response; defaulting to partial_success"
        )
    }

    private func extractJSONBlock(from text: String) -> String? {
        let pattern = "```json\\s*\\n([\\s\\S]*?)\\n\\s*```"
        guard let regex = try? NSRegularExpression(pattern: pattern) else {
            return nil
        }
        let nsText = text as NSString
        let matches = regex.matches(in: text, range: NSRange(location: 0, length: nsText.length))
        guard let match = matches.last, match.numberOfRanges >= 2 else {
            return nil
        }
        return nsText.substring(with: match.range(at: 1))
    }

    private func parseStringList(_ raw: String) -> [String] {
        guard !raw.isEmpty else { return [] }
        if raw.hasPrefix("[") && raw.hasSuffix("]"),
           let data = raw.data(using: .utf8),
           let list = try? JSONSerialization.jsonObject(with: data) as? [String] {
            return list
        }
        return raw
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
    }

    private func firstNonEmpty(_ values: [String]) -> String {
        values.first(where: { !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty })?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
    }
}

private actor ACPUpdateCollector {
    private var responseParts: [String] = []
    private var planEntries: [PlanEntry] = []
    private var toolStates: [String: ToolSnapshot] = [:]
    private var currentModeID: String?

    func apply(_ update: SessionUpdate) {
        switch update {
        case .agentMessageChunk(let chunk):
            responseParts.append(chunk.content.text)
        case .plan(let plan):
            planEntries = plan.entries
        case .toolCall(let call):
            var snapshot = toolStates[call.toolCallID] ?? ToolSnapshot(id: call.toolCallID)
            snapshot.title = call.title ?? snapshot.title
            snapshot.kind = call.kind ?? snapshot.kind
            snapshot.status = call.status ?? snapshot.status
            snapshot.locations = call.locations ?? snapshot.locations
            toolStates[call.toolCallID] = snapshot
        case .toolCallUpdate(let update):
            var snapshot = toolStates[update.toolCallID] ?? ToolSnapshot(id: update.toolCallID)
            snapshot.title = update.title ?? snapshot.title
            snapshot.kind = update.kind ?? snapshot.kind
            snapshot.status = update.status ?? snapshot.status
            snapshot.locations = update.locations ?? snapshot.locations
            if let content = update.content {
                snapshot.content = content
            }
            toolStates[update.toolCallID] = snapshot
        case .currentModeUpdate(let update):
            currentModeID = update.currentModeID
        default:
            break
        }
    }

    func snapshot(sessionID: String, stopReason: StopReason) -> ACPCollectedResult {
        let planSummary = planEntries.map { entry in
            [entry.status, entry.priority, entry.content].compactMap { $0 }.joined(separator: ": ")
        }.joined(separator: "\n")
        let toolSummary = toolStates.values.sorted { $0.id < $1.id }.map { snapshot in
            let locationText = snapshot.locations?.map { $0.path }.joined(separator: ", ") ?? ""
            return [snapshot.id, snapshot.kind, snapshot.status, snapshot.title, locationText]
                .compactMap { $0 }
                .filter { !$0.isEmpty }
                .joined(separator: " | ")
        }.joined(separator: "\n")
        var contextUpdates: [String: String] = [
            "acp.session_id": sessionID,
            "acp.stop_reason": stopReason.rawValue,
        ]
        if !planSummary.isEmpty {
            contextUpdates["acp.plan"] = planSummary
        }
        if !toolSummary.isEmpty {
            contextUpdates["acp.tool_calls"] = toolSummary
        }
        if let currentModeID, !currentModeID.isEmpty {
            contextUpdates["acp.mode"] = currentModeID
        }
        return ACPCollectedResult(
            responseText: responseParts.joined(),
            contextUpdates: contextUpdates,
            notes: toolSummary.isEmpty ? "" : "ACP tool activity:\n" + toolSummary
        )
    }
}

private struct ACPCollectedResult: Sendable {
    var responseText: String
    var contextUpdates: [String: String]
    var notes: String
}

private struct ToolSnapshot: Sendable {
    var id: String
    var title: String?
    var kind: String?
    var status: String?
    var locations: [ToolCallLocation]?
    var content: [ToolCallContent]?
}
