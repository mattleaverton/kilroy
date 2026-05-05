import Foundation
import OmniAICore

public final class OpenAIProfile: ProviderProfile, @unchecked Sendable {
    public let id = "openai"
    public let model: String
    public let toolRegistry: ToolRegistry
    private let includeNativeWebSearch: Bool
    private let webSearchExternalWebAccess: Bool?
    private let forceCodexSystemPrompt: Bool
    public let supportsReasoning = true
    public let supportsStreaming = true
    public let supportsParallelToolCalls = true
    public let contextWindowSize = 200_000

    public enum ShellType: Sendable {
        case shell
        case shellCommand
        case disabled
    }

    public init(
        model: String = "gpt-5.2",
        session: Session? = nil,
        shellType: ShellType = .shellCommand,
        includeApplyPatch: Bool = true,
        useUnifiedExec: Bool = true,
        includeCollabTools: Bool = false,
        includeWebSearch: Bool = false,
        webSearchExternalWebAccess: Bool? = true,
        forceCodexSystemPrompt: Bool = false
    ) {
        self.model = model
        self.includeNativeWebSearch = includeWebSearch
        self.webSearchExternalWebAccess = webSearchExternalWebAccess
        self.forceCodexSystemPrompt = forceCodexSystemPrompt

        let registry = ToolRegistry()
        if useUnifiedExec {
            registry.register(execCommandTool())
            registry.register(writeStdinTool())
        } else {
            switch shellType {
            case .shell:
                registry.register(codexShellTool(defaultTimeoutMs: 10_000))
            case .shellCommand:
                registry.register(shellCommandTool(defaultTimeoutMs: 10_000))
            case .disabled:
                break
            }
        }

        registry.register(codexReadFileTool())
        registry.register(grepFilesTool())
        registry.register(codexListDirTool())
        registry.register(updatePlanTool())
        registry.register(viewImageTool())
        if includeApplyPatch {
            registry.register(applyPatchTool())
        }

        if includeCollabTools, let session {
            registry.register(codexSpawnAgentTool(parentSession: session))
            registry.register(codexSendInputTool(parentSession: session))
            registry.register(codexWaitTool(parentSession: session))
            registry.register(codexCloseAgentTool(parentSession: session))
        }

        self.toolRegistry = registry
    }

    public func buildSystemPrompt(environment: ExecutionEnvironment, projectDocs: String?, userInstructions: String? = nil, gitContext: GitContext? = nil) -> String {
        let basePrompt = CodexSystemPrompt.openAIPrompt(for: model)
        let workingDir = environment.workingDirectory()
        let envContext = """

# Environment Context

- Working directory: \(workingDir)
- When using shell tools, use "." or "\(workingDir)" for the workdir parameter, not "/workspace"
- Platform: \(environment.platform()) \(environment.osVersion())
"""
        var sections: [String] = [basePrompt + envContext]

        if let projectDocs, !projectDocs.isEmpty {
            sections.append("""
# Project Documentation
\(projectDocs)
""")
        }

        if let userInstructions, !userInstructions.isEmpty {
            sections.append("""
# User Instructions
\(userInstructions)
""")
        }

        return sections.joined(separator: "\n\n")
    }

    public func providerOptions() -> [String: JSONValue]? {
        // Always use WebSocket transport for OpenAI Responses API — more reliable
        // than HTTP streaming and supports conversation-level resume.
        var options: [String: JSONValue] = [
            OpenAIProviderOptionKeys.responsesTransport: .string("websocket"),
        ]

        if includeNativeWebSearch {
            options[OpenAIProviderOptionKeys.includeNativeWebSearch] = .bool(true)
        }
        if let webSearchExternalWebAccess {
            options[OpenAIProviderOptionKeys.webSearchExternalWebAccess] = .bool(webSearchExternalWebAccess)
        }
        return [id: .object(options)]
    }
}
