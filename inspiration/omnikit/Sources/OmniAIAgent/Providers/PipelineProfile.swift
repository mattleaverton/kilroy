import Foundation
import OmniAICore

/// Context extracted from a pipeline for decorating a provider profile's system prompt.
public struct PipelineProfileContext: Sendable {
    public var goal: String
    public var previousStageLabel: String
    public var previousStageOutput: String
    public var toolOutput: String

    public init(
        goal: String = "",
        previousStageLabel: String = "",
        previousStageOutput: String = "",
        toolOutput: String = ""
    ) {
        self.goal = goal
        self.previousStageLabel = previousStageLabel
        self.previousStageOutput = previousStageOutput
        self.toolOutput = toolOutput
    }
}

/// Wraps any `ProviderProfile` and decorates its system prompt with pipeline-specific
/// instructions: goal, previous stage output, JSON status block requirement, and
/// stop-when-done directive. Delegates everything else to the wrapped profile.
public final class PipelineProfile: ProviderProfile, @unchecked Sendable {

    private let wrapped: ProviderProfile
    private let context: PipelineProfileContext

    public var id: String { wrapped.id }
    public var model: String { wrapped.model }
    public var toolRegistry: ToolRegistry { wrapped.toolRegistry }
    public var supportsReasoning: Bool { wrapped.supportsReasoning }
    public var supportsStreaming: Bool { wrapped.supportsStreaming }
    public var supportsParallelToolCalls: Bool { wrapped.supportsParallelToolCalls }
    public var contextWindowSize: Int { wrapped.contextWindowSize }

    public init(wrapping profile: ProviderProfile, context: PipelineProfileContext) {
        self.wrapped = profile
        self.context = context
    }

    public func tools() -> [Tool] {
        wrapped.tools()
    }

    public func providerOptions() -> [String: JSONValue]? {
        wrapped.providerOptions()
    }

    public func buildSystemPrompt(
        environment: ExecutionEnvironment,
        projectDocs: String?,
        userInstructions: String?,
        gitContext: GitContext?
    ) -> String {
        // Build the base prompt from the wrapped profile, injecting our pipeline
        // user instructions alongside any existing ones.
        let pipelineInstructions = buildPipelineInstructions()
        let mergedInstructions: String
        if let existing = userInstructions, !existing.isEmpty {
            mergedInstructions = existing + "\n\n" + pipelineInstructions
        } else {
            mergedInstructions = pipelineInstructions
        }

        return wrapped.buildSystemPrompt(
            environment: environment,
            projectDocs: projectDocs,
            userInstructions: mergedInstructions,
            gitContext: gitContext
        )
    }

    // MARK: - Private

    private func buildPipelineInstructions() -> String {
        var parts: [String] = []

        parts.append("You are a coding agent executing a stage in an automated pipeline.")

        if !context.goal.isEmpty {
            parts.append("Pipeline goal: \(context.goal)")
        }

        if !context.previousStageLabel.isEmpty && !context.previousStageOutput.isEmpty {
            parts.append("PREVIOUS STAGE (\(context.previousStageLabel)) OUTPUT:\n\(context.previousStageOutput)")
        }

        if !context.toolOutput.isEmpty {
            parts.append("PREVIOUS TOOL OUTPUT:\n\(context.toolOutput)")
        }

        parts.append("""
        CRITICAL INSTRUCTIONS - READ CAREFULLY:

        1. WRITE TEXT OUTPUT: Your text output will be passed to the next pipeline stage. \
        If you only use tools without writing text, the next stage will have NO context about what you did. \
        Always write a detailed summary of your findings, changes, and conclusions as text output.

        2. WRITE FILES: Write your findings and results to .ai/ files in the working directory \
        so downstream stages can read them even if text context is lost.

        3. JSON STATUS BLOCK (MANDATORY): When you have completed your task, you MUST output a JSON \
        status block at the very end of your final message in this EXACT format:

        ```json
        {
          "outcome": "success",
          "preferred_next_label": "",
          "context_updates": {},
          "notes": "brief summary of what you accomplished"
        }
        ```

        Valid outcome values: "success", "partial_success", "retry", "fail"
        - Use "success" only if you have fully completed the task with evidence
        - Use "partial_success" if you made progress but couldn't finish everything
        - Use "retry" if you hit a blocker that might be resolved with another attempt
        - Use "fail" if the task is fundamentally impossible

        WITHOUT this JSON block, the pipeline will treat your work as incomplete. \
        This is not optional - the JSON status block MUST appear in your response.

        4. STOP WHEN DONE: Once you have written all files and completed the task, \
        STOP making tool calls. Return your final summary text with the JSON status block \
        as a regular message (no tool calls). The session ends when you return a message \
        with zero tool calls. Do NOT keep reading or re-verifying files after writing them — \
        trust your work and finish.
        """)

        return parts.joined(separator: "\n\n")
    }
}
