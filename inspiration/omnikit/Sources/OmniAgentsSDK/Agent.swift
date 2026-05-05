import Foundation
import OmniAICore
import OmniMCP

public struct ToolsToFinalOutputResult: @unchecked Sendable {
    public var isFinalOutput: Bool
    public var finalOutput: Any?

    public init(isFinalOutput: Bool, finalOutput: Any? = nil) {
        self.isFinalOutput = isFinalOutput
        self.finalOutput = finalOutput
    }
}

public typealias ToolsToFinalOutputFunction<TContext> = @Sendable (
    RunContextWrapper<TContext>,
    [FunctionToolResult]
) async throws -> ToolsToFinalOutputResult

public struct AgentToolStreamEvent: @unchecked Sendable {
    public var event: AgentStreamEvent
    public var agent: AnyAgent
    public var toolCall: TResponseOutputItem?

    public init(event: AgentStreamEvent, agent: AnyAgent, toolCall: TResponseOutputItem? = nil) {
        self.event = event
        self.agent = agent
        self.toolCall = toolCall
    }
}

public struct StopAtTools: Sendable, Equatable {
    public var stopAtToolNames: [String]

    public init(stopAtToolNames: [String]) {
        self.stopAtToolNames = stopAtToolNames
    }
}

public struct MCPConfig: Sendable {
    public var convertSchemasToStrict: Bool
    public var failureErrorFunction: ToolErrorFunction?

    public init(convertSchemasToStrict: Bool = false, failureErrorFunction: ToolErrorFunction? = defaultToolErrorFunction) {
        self.convertSchemasToStrict = convertSchemasToStrict
        self.failureErrorFunction = failureErrorFunction
    }
}

public enum AgentInstructions<TContext>: @unchecked Sendable {
    case text(String)
    case dynamic(@Sendable (RunContextWrapper<TContext>, Agent<TContext>) async throws -> String)
}

public enum AgentPromptSource<TContext>: @unchecked Sendable {
    case prompt(Prompt)
    case dynamic(DynamicPromptFunction<TContext>)
}

public enum ToolUseBehavior<TContext>: @unchecked Sendable {
    case runLLMAgain
    case stopOnFirstTool
    case stopAtTools(StopAtTools)
    case custom(ToolsToFinalOutputFunction<TContext>)
}

open class AgentBase<TContext>: @unchecked Sendable {
    public var name: String
    public var handoffDescription: String?
    public var tools: [Tool]
    public var mcpServers: [any MCPServer]
    public var mcpConfig: MCPConfig

    public init(
        name: String,
        handoffDescription: String? = nil,
        tools: [Tool] = [],
        mcpServers: [any MCPServer] = [],
        mcpConfig: MCPConfig = MCPConfig()
    ) {
        self.name = name
        self.handoffDescription = handoffDescription
        self.tools = tools
        self.mcpServers = mcpServers
        self.mcpConfig = mcpConfig
    }

    open func getMCPTools(runContext: RunContextWrapper<TContext>) async throws -> [Tool] {
        let discovered = try await MCPToolDiscovery.discoverTools(servers: mcpServers)
        var tools: [Tool] = []
        tools.reserveCapacity(discovered.count)

        for tool in discovered {
            let schemaValue = mcpConfig.convertSchemasToStrict ? ensureStrictJSONSchema(tool.inputSchema) : tool.inputSchema
            let schemaObject: [String: JSONValue]
            if case .object(let object) = schemaValue {
                schemaObject = object
            } else {
                schemaObject = ensureStrictJSONSchema([
                    "type": .string("object"),
                    "properties": .object([:]),
                    "additionalProperties": .bool(false),
                ])
            }

            let functionTool = FunctionTool(
                name: tool.name,
                description: tool.description,
                paramsJSONSchema: schemaObject,
                onInvokeTool: { _, rawArguments in
                    let parsedArguments: JSONValue = {
                        if let data = rawArguments.data(using: .utf8),
                           let json = try? JSONValue.parse(data) {
                            return json
                        }
                        return .object([:])
                    }()

                    let result = try await tool.call(arguments: parsedArguments)
                    if result.isError {
                        let message = ItemHelpers.stringifyJSON(result.content)
                        throw UserError(message: message)
                    }
                    return result.content
                },
                strictJSONSchema: false,
                timeoutErrorFunction: mcpConfig.failureErrorFunction,
                isCodexTool: true
            )
            tools.append(.function(functionTool))
        }

        return tools
    }

    open func getAllTools(runContext: RunContextWrapper<TContext>) async throws -> [Tool] {
        let mcpTools = try await getMCPTools(runContext: runContext)
        var enabledTools: [Tool] = []
        for tool in tools {
            if try await ToolRuntime.isToolEnabled(tool, runContext: runContext, agent: AnyAgent(self)) {
                enabledTools.append(tool)
            }
        }
        let allTools = mcpTools + enabledTools
        try validateCodexToolNameCollisions(allTools)
        return allTools
    }
}

public final class Agent<TContext>: AgentBase<TContext>, @unchecked Sendable {
    public var instructions: AgentInstructions<TContext>?
    public var prompt: AgentPromptSource<TContext>?
    public var handoffs: [Handoff<TContext>]
    public var model: ModelReference?
    public var modelSettings: ModelSettings
    public var inputGuardrails: [InputGuardrail<TContext>]
    public var outputGuardrails: [OutputGuardrail<TContext>]
    public var outputType: (any AgentOutputSchemaBase)?
    public var hooks: AgentHooks<TContext>?
    public var toolUseBehavior: ToolUseBehavior<TContext>
    public var resetToolChoice: Bool

    public init(
        name: String,
        instructions: AgentInstructions<TContext>? = nil,
        prompt: AgentPromptSource<TContext>? = nil,
        handoffDescription: String? = nil,
        tools: [Tool] = [],
        mcpServers: [any MCPServer] = [],
        mcpConfig: MCPConfig = MCPConfig(),
        handoffs: [Handoff<TContext>] = [],
        model: ModelReference? = nil,
        modelSettings: ModelSettings = getDefaultModelSettings(),
        inputGuardrails: [InputGuardrail<TContext>] = [],
        outputGuardrails: [OutputGuardrail<TContext>] = [],
        outputType: (any AgentOutputSchemaBase)? = nil,
        hooks: AgentHooks<TContext>? = nil,
        toolUseBehavior: ToolUseBehavior<TContext> = .runLLMAgain,
        resetToolChoice: Bool = false
    ) {
        self.instructions = instructions
        self.prompt = prompt
        self.handoffs = handoffs
        self.model = model
        self.modelSettings = modelSettings
        self.inputGuardrails = inputGuardrails
        self.outputGuardrails = outputGuardrails
        self.outputType = outputType
        self.hooks = hooks
        self.toolUseBehavior = toolUseBehavior
        self.resetToolChoice = resetToolChoice
        super.init(
            name: name,
            handoffDescription: handoffDescription,
            tools: tools,
            mcpServers: mcpServers,
            mcpConfig: mcpConfig
        )
        precondition(!name.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty, "Agent name must not be empty")
    }

    public func clone() -> Agent<TContext> {
        Agent(
            name: name,
            instructions: instructions,
            prompt: prompt,
            handoffDescription: handoffDescription,
            tools: tools,
            mcpServers: mcpServers,
            mcpConfig: mcpConfig,
            handoffs: handoffs,
            model: model,
            modelSettings: modelSettings,
            inputGuardrails: inputGuardrails,
            outputGuardrails: outputGuardrails,
            outputType: outputType,
            hooks: hooks,
            toolUseBehavior: toolUseBehavior,
            resetToolChoice: resetToolChoice
        )
    }

    public func asTool(
        toolName: String? = nil,
        toolDescription: String? = nil,
        inputSchemaInfo: StructuredInputSchemaInfo? = nil,
        maxTurns: Int = DEFAULT_MAX_TURNS,
        runConfig: RunConfig? = nil
    ) -> Tool {
        let schemaInfo = inputSchemaInfo ?? buildStructuredInputSchemaInfo()
        let name = toolName ?? transformStringFunctionStyle(self.name)
        let description = toolDescription ?? handoffDescription ?? "Run the \(self.name) agent."

        return .function(FunctionTool(
            name: name,
            description: description,
            paramsJSONSchema: schemaInfo.jsonSchema ?? ensureStrictJSONSchema([
                "type": .string("object"),
                "properties": .object(["input": .object(["type": .string("string")])]),
                "required": .array([.string("input")]),
                "additionalProperties": .bool(false),
            ]),
            onInvokeTool: { context, rawArguments in
                guard let typedContext = context.context as? TContext else {
                    throw UserError(message: "Agent tool context type mismatch")
                }
                let input = try resolveAgentToolInput(rawArguments: rawArguments, schemaInfo: schemaInfo)
                let result = try await Runner.run(
                    self,
                    input: .string(input),
                    context: typedContext,
                    maxTurns: maxTurns,
                    hooks: nil,
                    runConfig: runConfig ?? context.runConfig,
                    errorHandlers: nil,
                    previousResponseID: nil,
                    autoPreviousResponseID: false,
                    conversationID: nil,
                    session: nil
                )
                return result.finalOutput
            },
            strictJSONSchema: true,
            isEnabled: .always(true),
            needsApproval: .always(false),
            isAgentTool: true,
            isCodexTool: false,
            agentInstance: AnyAgent(self)
        ))
    }

    public func getSystemPrompt(runContext: RunContextWrapper<TContext>) async throws -> String? {
        let baseInstructions: String?
        switch instructions {
        case .text(let string):
            baseInstructions = string
        case .dynamic(let function):
            baseInstructions = try await function(runContext, self)
        case .none:
            baseInstructions = nil
        }

        if let prompt = try await getPrompt(runContext: runContext) {
            return PromptUtil.render(prompt: prompt, baseInstructions: baseInstructions)
        }
        return baseInstructions
    }

    public func getPrompt(runContext: RunContextWrapper<TContext>) async throws -> Prompt? {
        switch prompt {
        case .prompt(let prompt):
            return prompt
        case .dynamic(let function):
            return try await function(.init(context: runContext, agent: self))
        case .none:
            return nil
        }
    }
}

private func validateCodexToolNameCollisions(_ tools: [Tool]) throws {
    let codexNames = tools.compactMap { tool -> String? in
        guard case .function(let functionTool) = tool, functionTool.isCodexTool else {
            return nil
        }
        return functionTool.name
    }
    let duplicates = Dictionary(grouping: codexNames, by: { $0 }).filter { $1.count > 1 }.keys.sorted()
    if !duplicates.isEmpty {
        throw UserError(message: "Duplicate Codex tool names found: \(duplicates.joined(separator: ", "))")
    }
}
