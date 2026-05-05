import Foundation
import OmniAICore

public final class AnthropicProfile: ProviderProfile, @unchecked Sendable {

    // MARK: - ProviderProfile

    public let id = "anthropic"
    public let model: String
    public let toolRegistry: ToolRegistry

    public let supportsReasoning = true
    public let supportsStreaming = true
    public let supportsParallelToolCalls = true
    public var contextWindowSize: Int { model.contains("[1m]") ? 1_000_000 : 200_000 }

    // MARK: - Init

    public init(model: String = "claude-haiku-4-5", session: Session? = nil, enableTodos: Bool = true, enableInteractiveTools: Bool = true) {
        self.model = model
        self.toolRegistry = ToolRegistry()

        // Core coding tools (always available).
        toolRegistry.register(claudeReadTool())
        toolRegistry.register(claudeWriteTool())
        toolRegistry.register(claudeEditTool())
        toolRegistry.register(claudeGlobTool())
        toolRegistry.register(claudeGrepTool())
        toolRegistry.register(claudeBashTool(defaultTimeoutMs: 120_000))
        toolRegistry.register(claudeWebFetchTool())
        toolRegistry.register(claudeWebSearchTool())
        toolRegistry.register(viewImageTool())

        if enableInteractiveTools {
            // Interactive/session-dependent tools.
            toolRegistry.register(claudeNotebookEditTool(parentSession: session))
            toolRegistry.register(claudeTaskStopTool(parentSession: session))
            toolRegistry.register(claudeTaskOutputTool(parentSession: session))
            toolRegistry.register(claudeTaskTool(parentSession: session))
            toolRegistry.register(claudeTaskCreateTool(parentSession: session))
            toolRegistry.register(claudeTaskGetTool(parentSession: session))
            toolRegistry.register(claudeTaskListTool(parentSession: session))
            toolRegistry.register(claudeTaskUpdateTool(parentSession: session))
            toolRegistry.register(claudeTeamCreateTool(parentSession: session))
            toolRegistry.register(claudeTeamDeleteTool(parentSession: session))
            toolRegistry.register(claudeSendMessageTool(parentSession: session))
            toolRegistry.register(claudeToolSearchTool(parentSession: session))
            toolRegistry.register(claudeAskUserTool(parentSession: session))
            toolRegistry.register(claudeEnterPlanModeTool(parentSession: session))
            toolRegistry.register(claudeExitPlanModeTool(parentSession: session))
            toolRegistry.register(claudeSkillTool())
            if enableTodos {
                toolRegistry.register(claudeTodoWriteTool())
            }
        }
    }

    // MARK: - System Prompt

    public func buildSystemPrompt(environment: ExecutionEnvironment, projectDocs: String?, userInstructions: String? = nil, gitContext: GitContext? = nil) -> String {
        let workingDirectory = URL(fileURLWithPath: environment.workingDirectory(), isDirectory: true)
        let skills = loadSkills(from: workingDirectory)
        let allowedTools = Set(tools().map(\.name))
        let todosEnabled = allowedTools.contains("TodoWrite")

        let promptGitInfo: ClaudePromptGitInfo?
        if let gitContext {
            promptGitInfo = ClaudePromptGitInfo(
                branch: gitContext.branch,
                hasUncommittedChanges: gitContext.modifiedFileCount > 0,
                recentCommits: gitContext.recentCommits
            )
        } else {
            promptGitInfo = nil
        }

        let promptEnvironment = ClaudePromptEnvironment(
            workingDirectory: workingDirectory,
            gitInfo: promptGitInfo
        )

        let basePrompt = ClaudeSystemPrompt.buildPrompt(
            environment: promptEnvironment,
            enableTodos: todosEnabled,
            modelName: modelDisplayName,
            modelId: model,
            availableSkills: skills,
            allowedTools: allowedTools
        )

        var sections: [String] = [basePrompt]

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

    private func loadSkills(from workingDirectory: URL) -> [ClaudePromptSkill] {
        let commandsDir = workingDirectory.appendingPathComponent(".claude/commands")

        guard FileManager.default.fileExists(atPath: commandsDir.path) else {
            return []
        }

        var skills: [ClaudePromptSkill] = []

        do {
            let files = try FileManager.default.contentsOfDirectory(
                at: commandsDir,
                includingPropertiesForKeys: [.isRegularFileKey],
                options: [.skipsHiddenFiles]
            )

            for file in files where file.pathExtension == "md" {
                if let skill = parseSkillFile(at: file) {
                    skills.append(skill)
                }
            }
        } catch {
            // Ignore skill loading errors for prompt generation.
        }

        return skills
    }

    private func parseSkillFile(at url: URL) -> ClaudePromptSkill? {
        guard let content = try? String(contentsOf: url, encoding: .utf8) else {
            return nil
        }

        let name = url.deletingPathExtension().lastPathComponent
        var description = "No description"
        var body = content

        if content.hasPrefix("---") {
            let parts = content.components(separatedBy: "---")
            if parts.count >= 3 {
                let yamlContent = parts[1]
                body = parts.dropFirst(2).joined(separator: "---")

                for line in yamlContent.components(separatedBy: .newlines) {
                    let trimmed = line.trimmingCharacters(in: .whitespaces)
                    if trimmed.hasPrefix("description:") {
                        description = String(trimmed.dropFirst("description:".count)).trimmingCharacters(in: .whitespaces)
                        break
                    }
                }
            }
        }

        return ClaudePromptSkill(
            name: name,
            description: description,
            content: body.trimmingCharacters(in: .whitespacesAndNewlines)
        )
    }

    private var modelDisplayName: String {
        switch model {
        case let id where id.contains("opus"):
            return "Claude Opus 4.6"
        case let id where id.contains("sonnet-4-6"):
            return "Claude Sonnet 4.6"
        case let id where id.contains("sonnet"):
            return "Claude Sonnet 4.5"
        case let id where id.contains("haiku"):
            return "Claude Haiku 4.5"
        default:
            return "Claude"
        }
    }

    // MARK: - Provider Options

    public func providerOptions() -> [String: JSONValue]? {
        return nil
    }
}
