import Foundation
import OmniAICore

public final class GeminiProfile: ProviderProfile, @unchecked Sendable {
    public let id: String = "gemini"
    public let model: String
    public let toolRegistry: ToolRegistry
    private let interactiveMode: Bool

    public let supportsReasoning: Bool = true
    public let supportsStreaming: Bool = true
    public let supportsParallelToolCalls: Bool = true
    public let contextWindowSize: Int = 1_000_000

    public init(model: String = "gemini-3-flash-preview", session: Session? = nil, interactiveMode: Bool = true, enableTodos: Bool = true, enablePlanTools: Bool = true) {
        self.model = model
        self.interactiveMode = interactiveMode
        self.toolRegistry = ToolRegistry()

        // Core Gemini CLI parity tools.
        toolRegistry.register(geminiGlobTool())
        toolRegistry.register(geminiReadFileTool())
        toolRegistry.register(geminiWriteFileTool())
        toolRegistry.register(geminiReplaceTool())
        toolRegistry.register(geminiGrepSearchTool())
        toolRegistry.register(geminiListDirectoryTool())
        toolRegistry.register(geminiReadManyFilesTool())
        toolRegistry.register(geminiRunShellCommandTool(enableInteractiveShell: interactiveMode, enableEfficiency: true))
        toolRegistry.register(geminiGoogleWebSearchTool())
        toolRegistry.register(geminiWebFetchTool())
        toolRegistry.register(geminiSaveMemoryTool())
        toolRegistry.register(geminiGetInternalDocsTool())
        toolRegistry.register(geminiActivateSkillTool())
        toolRegistry.register(viewImageTool())
        toolRegistry.register(geminiAskUserTool())
        if enableTodos {
            toolRegistry.register(geminiWriteTodosTool())
        }
        if enablePlanTools {
            toolRegistry.register(geminiEnterPlanModeTool())
            toolRegistry.register(geminiExitPlanModeTool())
        }

        // Session-backed collaboration tools can still be enabled explicitly.
        if let session {
            toolRegistry.register(codexSpawnAgentTool(parentSession: session))
            toolRegistry.register(codexSendInputTool(parentSession: session))
            toolRegistry.register(codexWaitTool(parentSession: session))
            toolRegistry.register(codexCloseAgentTool(parentSession: session))
        }
    }

    public func buildSystemPrompt(environment: ExecutionEnvironment, projectDocs: String?, userInstructions: String? = nil, gitContext: GitContext? = nil) -> String {
        let hasTodos = toolRegistry.names().contains(GEMINI_WRITE_TODOS_TOOL_NAME)
        let hasPlanTools = toolRegistry.names().contains(GEMINI_ENTER_PLAN_MODE_TOOL_NAME)
        let sandbox: GeminiSystemPrompt.SandboxType = .outside

        let prompt = GeminiSystemPrompt.buildPrompt(
            options: GeminiSystemPrompt.Options(
                interactiveMode: interactiveMode,
                enableWriteTodosTool: hasTodos,
                enableEnterPlanModeTool: hasPlanTools,
                isGitRepository: gitContext != nil,
                sandbox: sandbox
            )
        )

        var sections: [String] = [prompt]

        let today = Date.now.formatted(.iso8601.year().month().day())
        sections.append("""
<env>
Working directory: \(environment.workingDirectory())
Platform: \(environment.platform())
OS Version: \(environment.osVersion())
Today's date: \(today)
Model: \(model)
</env>
""")

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
        return nil
    }
}
