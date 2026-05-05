import Foundation
import OmniAICore

open class ToolContext<TContext>: RunContextWrapper<TContext>, @unchecked Sendable {
    public let toolName: String
    public let toolCallID: String
    public let toolArguments: String
    public let toolCall: ToolCall?
    public let agent: AnyAgent?
    public let runConfig: RunConfig?

    public init(
        context: TContext,
        usage: Usage = Usage(),
        toolName: String,
        toolCallID: String,
        toolArguments: String,
        toolCall: ToolCall? = nil,
        agent: AnyAgent? = nil,
        runConfig: RunConfig? = nil,
        turnInput: [Any] = [],
        toolInput: Any? = nil
    ) {
        self.toolName = toolName
        self.toolCallID = toolCallID
        self.toolArguments = toolArguments
        self.toolCall = toolCall
        self.agent = agent
        self.runConfig = runConfig
        super.init(context: context, usage: usage, turnInput: turnInput)
        self.toolInput = toolInput
    }

    public static func fromAgentContext(
        _ context: RunContextWrapper<TContext>,
        toolCallID: String,
        toolCall: ToolCall? = nil,
        agent: AnyAgent? = nil,
        runConfig: RunConfig? = nil
    ) -> ToolContext<TContext> {
        let resolvedToolName = toolCall?.name ?? "unknown_tool"
        let resolvedToolArguments: String = {
            guard let toolCall else { return "{}" }
            if let rawArguments = toolCall.rawArguments {
                return rawArguments
            }
            if let data = try? JSONValue.object(toolCall.arguments).data(),
               let string = String(data: data, encoding: .utf8)
            {
                return string
            }
            return "{}"
        }()

        let result = ToolContext(
            context: context.context,
            usage: context.usage,
            toolName: resolvedToolName,
            toolCallID: toolCallID,
            toolArguments: resolvedToolArguments,
            toolCall: toolCall,
            agent: agent,
            runConfig: runConfig,
            turnInput: context.turnInput,
            toolInput: context.toolInput
        )
        result.rebuildApprovals(from: context.serializedApprovals())
        return result
    }
}
