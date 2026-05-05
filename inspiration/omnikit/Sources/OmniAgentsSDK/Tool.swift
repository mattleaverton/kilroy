import Foundation
import OmniAICore

public typealias ToolParams = [String: JSONValue]
public typealias ToolErrorFunction = @Sendable (Error, String, String, ToolContext<Any>) async -> String

public enum ToolTimeoutBehavior: String, Sendable, Codable, Equatable {
    case messageToModel = "message_to_model"
    case raiseError = "raise_error"
}

public enum ToolEnabledPredicate: @unchecked Sendable {
    case always(Bool)
    case dynamic(@Sendable (RunContextWrapper<Any>, AnyAgent) async throws -> Bool)

    public func evaluate<TContext>(context: RunContextWrapper<TContext>, agent: AnyAgent) async throws -> Bool {
        switch self {
        case .always(let enabled):
            return enabled
        case .dynamic(let function):
            let erased = RunContextWrapper<Any>(context: context.context as Any, usage: context.usage, turnInput: context.turnInput)
            erased.rebuildApprovals(from: context.serializedApprovals())
            erased.toolInput = context.toolInput
            return try await function(erased, agent)
        }
    }
}

public enum ToolApprovalRequirement: @unchecked Sendable {
    case always(Bool)
    case dynamic(@Sendable (RunContextWrapper<Any>, [String: JSONValue], String) async throws -> Bool)

    public func evaluate<TContext>(context: RunContextWrapper<TContext>, arguments: [String: JSONValue], callID: String) async throws -> Bool {
        switch self {
        case .always(let required):
            return required
        case .dynamic(let function):
            let erased = RunContextWrapper<Any>(context: context.context as Any, usage: context.usage, turnInput: context.turnInput)
            erased.rebuildApprovals(from: context.serializedApprovals())
            erased.toolInput = context.toolInput
            return try await function(erased, arguments, callID)
        }
    }
}

public struct FunctionToolResult: @unchecked Sendable {
    public var tool: FunctionTool
    public var output: Any
    public var runItem: (any RunItem)?
    public var interruptions: [ToolApprovalItem]
    public var agentRunResult: Any?

    public init(tool: FunctionTool, output: Any, runItem: (any RunItem)? = nil, interruptions: [ToolApprovalItem] = [], agentRunResult: Any? = nil) {
        self.tool = tool
        self.output = output
        self.runItem = runItem
        self.interruptions = interruptions
        self.agentRunResult = agentRunResult
    }
}

public struct FunctionTool: @unchecked Sendable {
    public var name: String
    public var description: String
    public var paramsJSONSchema: [String: JSONValue]
    public var onInvokeTool: @Sendable (ToolContext<Any>, String) async throws -> Any
    public var strictJSONSchema: Bool
    public var isEnabled: ToolEnabledPredicate
    public var toolInputGuardrails: [ToolInputGuardrail<Any>]?
    public var toolOutputGuardrails: [ToolOutputGuardrail<Any>]?
    public var needsApproval: ToolApprovalRequirement
    public var timeoutSeconds: Double?
    public var timeoutBehavior: ToolTimeoutBehavior
    public var timeoutErrorFunction: ToolErrorFunction?
    public var isAgentTool: Bool
    public var isCodexTool: Bool
    public var agentInstance: AnyAgent?

    public init(
        name: String,
        description: String,
        paramsJSONSchema: [String: JSONValue],
        onInvokeTool: @escaping @Sendable (ToolContext<Any>, String) async throws -> Any,
        strictJSONSchema: Bool = true,
        isEnabled: ToolEnabledPredicate = .always(true),
        toolInputGuardrails: [ToolInputGuardrail<Any>]? = nil,
        toolOutputGuardrails: [ToolOutputGuardrail<Any>]? = nil,
        needsApproval: ToolApprovalRequirement = .always(false),
        timeoutSeconds: Double? = nil,
        timeoutBehavior: ToolTimeoutBehavior = .messageToModel,
        timeoutErrorFunction: ToolErrorFunction? = nil,
        isAgentTool: Bool = false,
        isCodexTool: Bool = false,
        agentInstance: AnyAgent? = nil
    ) {
        self.name = name
        self.description = description
        self.paramsJSONSchema = strictJSONSchema ? ensureStrictJSONSchema(paramsJSONSchema) : paramsJSONSchema
        self.onInvokeTool = onInvokeTool
        self.strictJSONSchema = strictJSONSchema
        self.isEnabled = isEnabled
        self.toolInputGuardrails = toolInputGuardrails
        self.toolOutputGuardrails = toolOutputGuardrails
        self.needsApproval = needsApproval
        self.timeoutSeconds = timeoutSeconds
        self.timeoutBehavior = timeoutBehavior
        self.timeoutErrorFunction = timeoutErrorFunction
        self.isAgentTool = isAgentTool
        self.isCodexTool = isCodexTool
        self.agentInstance = agentInstance
    }
}

public struct FileSearchTool: Sendable, Equatable {
    public var vectorStoreIDs: [String]
    public var maxNumResults: Int?
    public var includeSearchResults: Bool

    public init(vectorStoreIDs: [String], maxNumResults: Int? = nil, includeSearchResults: Bool = true) {
        self.vectorStoreIDs = vectorStoreIDs
        self.maxNumResults = maxNumResults
        self.includeSearchResults = includeSearchResults
    }

    public var name: String { "file_search" }
}

public struct WebSearchTool: Sendable, Equatable {
    public var userLocation: String?
    public var searchContextSize: String
    public var externalWebAccess: Bool?

    public init(userLocation: String? = nil, searchContextSize: String = "medium", externalWebAccess: Bool? = nil) {
        self.userLocation = userLocation
        self.searchContextSize = searchContextSize
        self.externalWebAccess = externalWebAccess
    }

    public var name: String { "web_search" }
}

public struct ComputerProvider: @unchecked Sendable {
    public var create: @Sendable () async throws -> any AsyncComputer
    public var dispose: (@Sendable (any AsyncComputer) async -> Void)?

    public init(
        create: @escaping @Sendable () async throws -> any AsyncComputer,
        dispose: (@Sendable (any AsyncComputer) async -> Void)? = nil
    ) {
        self.create = create
        self.dispose = dispose
    }
}

public enum ComputerConfig: @unchecked Sendable {
    case instance(any AsyncComputer)
    case provider(ComputerProvider)
}

public struct ComputerToolSafetyCheckData: @unchecked Sendable {
    public var contextWrapper: RunContextWrapper<Any>
    public var agent: AnyAgent
    public var toolCall: TResponseOutputItem
    public var safetyCheck: JSONValue?

    public init(contextWrapper: RunContextWrapper<Any>, agent: AnyAgent, toolCall: TResponseOutputItem, safetyCheck: JSONValue?) {
        self.contextWrapper = contextWrapper
        self.agent = agent
        self.toolCall = toolCall
        self.safetyCheck = safetyCheck
    }
}

public struct ComputerTool: @unchecked Sendable {
    public let id: UUID
    public var computer: ComputerConfig
    public var onSafetyCheck: (@Sendable (ComputerToolSafetyCheckData) async throws -> Bool)?

    public init(id: UUID = UUID(), computer: ComputerConfig, onSafetyCheck: (@Sendable (ComputerToolSafetyCheckData) async throws -> Bool)? = nil) {
        self.id = id
        self.computer = computer
        self.onSafetyCheck = onSafetyCheck
    }

    public var name: String { "computer_use_preview" }
}

public struct MCPToolApprovalRequest: Sendable {
    public var contextWrapper: RunContextWrapper<Any>
    public var data: [String: JSONValue]

    public init(contextWrapper: RunContextWrapper<Any>, data: [String: JSONValue]) {
        self.contextWrapper = contextWrapper
        self.data = data
    }
}

public struct MCPToolApprovalFunctionResult: Sendable, Equatable {
    public var approve: Bool
    public var reason: String?

    public init(approve: Bool, reason: String? = nil) {
        self.approve = approve
        self.reason = reason
    }
}

public typealias MCPToolApprovalFunction = @Sendable (MCPToolApprovalRequest) async throws -> MCPToolApprovalFunctionResult

public struct HostedMCPTool: @unchecked Sendable {
    public var toolConfig: [String: JSONValue]
    public var onApprovalRequest: MCPToolApprovalFunction?

    public init(toolConfig: [String: JSONValue], onApprovalRequest: MCPToolApprovalFunction? = nil) {
        self.toolConfig = toolConfig
        self.onApprovalRequest = onApprovalRequest
    }

    public var name: String { toolConfig["name"]?.stringValue ?? "hosted_mcp" }
}

public struct CodeInterpreterTool: Sendable, Equatable {
    public var toolConfig: [String: JSONValue]
    public init(toolConfig: [String: JSONValue] = [:]) { self.toolConfig = toolConfig }
    public var name: String { "code_interpreter" }
}

public struct ImageGenerationTool: Sendable, Equatable {
    public var toolConfig: [String: JSONValue]
    public init(toolConfig: [String: JSONValue] = [:]) { self.toolConfig = toolConfig }
    public var name: String { "image_generation" }
}

public struct LocalShellCommandRequest: Sendable {
    public var contextWrapper: RunContextWrapper<Any>
    public var data: ShellCallData

    public init(contextWrapper: RunContextWrapper<Any>, data: ShellCallData) {
        self.contextWrapper = contextWrapper
        self.data = data
    }
}

public typealias LocalShellExecutor = @Sendable (LocalShellCommandRequest) async throws -> ShellResult

public struct LocalShellTool: @unchecked Sendable {
    public var executor: LocalShellExecutor

    public init(executor: @escaping LocalShellExecutor) {
        self.executor = executor
    }

    public var name: String { "local_shell" }
}

public struct ShellToolLocalSkill: Sendable, Codable, Equatable {
    public var description: String
    public var name: String
    public var path: String

    public init(description: String, name: String, path: String) {
        self.description = description
        self.name = name
        self.path = path
    }
}

public struct ShellToolSkillReference: Sendable, Codable, Equatable {
    public var type: String
    public var skillID: String
    public var version: String?

    public init(skillID: String, version: String? = nil) {
        self.type = "skill_reference"
        self.skillID = skillID
        self.version = version
    }
}

public struct ShellToolInlineSkillSource: Sendable, Codable, Equatable {
    public var data: String
    public var mediaType: String
    public var type: String

    public init(data: String, mediaType: String = "application/zip", type: String = "base64") {
        self.data = data
        self.mediaType = mediaType
        self.type = type
    }
}

public struct ShellToolInlineSkill: Sendable, Codable, Equatable {
    public var description: String
    public var name: String
    public var source: ShellToolInlineSkillSource
    public var type: String

    public init(description: String, name: String, source: ShellToolInlineSkillSource) {
        self.description = description
        self.name = name
        self.source = source
        self.type = "inline"
    }
}

public enum ShellToolContainerSkill: Sendable, Codable, Equatable {
    case skillReference(ShellToolSkillReference)
    case inline(ShellToolInlineSkill)
}

public struct ShellToolContainerNetworkPolicyDomainSecret: Sendable, Codable, Equatable {
    public var domain: String
    public var name: String
    public var value: String
    public init(domain: String, name: String, value: String) {
        self.domain = domain
        self.name = name
        self.value = value
    }
}

public struct ShellToolContainerNetworkPolicyAllowlist: Sendable, Codable, Equatable {
    public var allowedDomains: [String]
    public var type: String
    public var domainSecrets: [ShellToolContainerNetworkPolicyDomainSecret]?

    public init(allowedDomains: [String], domainSecrets: [ShellToolContainerNetworkPolicyDomainSecret]? = nil) {
        self.allowedDomains = allowedDomains
        self.type = "allowlist"
        self.domainSecrets = domainSecrets
    }
}

public struct ShellToolContainerNetworkPolicyDisabled: Sendable, Codable, Equatable {
    public var type: String
    public init() { self.type = "disabled" }
}

public enum ShellToolContainerNetworkPolicy: Sendable, Codable, Equatable {
    case allowlist(ShellToolContainerNetworkPolicyAllowlist)
    case disabled(ShellToolContainerNetworkPolicyDisabled)
}

public struct ShellToolLocalEnvironment: Sendable, Codable, Equatable {
    public var type: String
    public var skills: [ShellToolLocalSkill]?
    public init(skills: [ShellToolLocalSkill]? = nil) {
        self.type = "local"
        self.skills = skills
    }
}

public struct ShellToolContainerAutoEnvironment: Sendable, Codable, Equatable {
    public var type: String
    public var fileIDs: [String]?
    public var memoryLimit: String?
    public var networkPolicy: ShellToolContainerNetworkPolicy?
    public var skills: [ShellToolContainerSkill]?
    public init(fileIDs: [String]? = nil, memoryLimit: String? = nil, networkPolicy: ShellToolContainerNetworkPolicy? = nil, skills: [ShellToolContainerSkill]? = nil) {
        self.type = "container_auto"
        self.fileIDs = fileIDs
        self.memoryLimit = memoryLimit
        self.networkPolicy = networkPolicy
        self.skills = skills
    }
}

public struct ShellToolContainerReferenceEnvironment: Sendable, Codable, Equatable {
    public var type: String
    public var containerID: String
    public init(containerID: String) {
        self.type = "container_reference"
        self.containerID = containerID
    }
}

public enum ShellToolHostedEnvironment: Sendable, Codable, Equatable {
    case containerAuto(ShellToolContainerAutoEnvironment)
    case containerReference(ShellToolContainerReferenceEnvironment)
}

public enum ShellToolEnvironment: Sendable, Codable, Equatable {
    case local(ShellToolLocalEnvironment)
    case hosted(ShellToolHostedEnvironment)
}

public struct ShellCallOutcome: Sendable, Codable, Equatable {
    public var type: String
    public var exitCode: Int?
    public init(type: String, exitCode: Int? = nil) {
        self.type = type
        self.exitCode = exitCode
    }
}

public struct ShellCommandOutput: Sendable, Codable, Equatable {
    public var stdout: String
    public var stderr: String
    public var outcome: ShellCallOutcome
    public var command: String?
    public var providerData: [String: JSONValue]?

    public init(stdout: String = "", stderr: String = "", outcome: ShellCallOutcome = .init(type: "exit", exitCode: 0), command: String? = nil, providerData: [String: JSONValue]? = nil) {
        self.stdout = stdout
        self.stderr = stderr
        self.outcome = outcome
        self.command = command
        self.providerData = providerData
    }

    public var exitCode: Int? { outcome.exitCode }
    public var status: String { outcome.type }
}

public struct ShellResult: Sendable, Codable, Equatable {
    public var output: [ShellCommandOutput]
    public var maxOutputLength: Int?
    public var providerData: [String: JSONValue]?
    public init(output: [ShellCommandOutput], maxOutputLength: Int? = nil, providerData: [String: JSONValue]? = nil) {
        self.output = output
        self.maxOutputLength = maxOutputLength
        self.providerData = providerData
    }
}

public struct ShellActionRequest: Sendable, Codable, Equatable {
    public var commands: [String]
    public var timeoutMS: Int?
    public var maxOutputLength: Int?
    public init(commands: [String], timeoutMS: Int? = nil, maxOutputLength: Int? = nil) {
        self.commands = commands
        self.timeoutMS = timeoutMS
        self.maxOutputLength = maxOutputLength
    }
}

public struct ShellCallData: Sendable, Codable, Equatable {
    public var callID: String
    public var action: ShellActionRequest
    public var status: String?
    public var raw: JSONValue?
    public init(callID: String, action: ShellActionRequest, status: String? = nil, raw: JSONValue? = nil) {
        self.callID = callID
        self.action = action
        self.status = status
        self.raw = raw
    }
}

public struct ShellCommandRequest: Sendable {
    public var contextWrapper: RunContextWrapper<Any>
    public var data: ShellCallData
    public init(contextWrapper: RunContextWrapper<Any>, data: ShellCallData) {
        self.contextWrapper = contextWrapper
        self.data = data
    }
}

public typealias ShellExecutor = @Sendable (ShellCommandRequest) async throws -> ShellResult
public typealias ShellApprovalFunction = @Sendable (ShellCommandRequest) async throws -> ShellOnApprovalFunctionResult
public typealias ApplyPatchApprovalFunction = @Sendable (ApplyPatchOperation, RunContextWrapper<Any>?) async throws -> ApplyPatchOnApprovalFunctionResult

public struct ShellOnApprovalFunctionResult: Sendable, Codable, Equatable {
    public var approve: Bool
    public var reason: String?
    public init(approve: Bool, reason: String? = nil) {
        self.approve = approve
        self.reason = reason
    }
}

public struct ApplyPatchOnApprovalFunctionResult: Sendable, Codable, Equatable {
    public var approve: Bool
    public var reason: String?
    public init(approve: Bool, reason: String? = nil) {
        self.approve = approve
        self.reason = reason
    }
}

public struct ShellTool: @unchecked Sendable {
    public var executor: ShellExecutor?
    public var name: String
    public var needsApproval: ToolApprovalRequirement
    public var onApproval: ShellApprovalFunction?
    public var environment: ShellToolEnvironment?

    public init(
        executor: ShellExecutor? = nil,
        name: String = "shell",
        needsApproval: ToolApprovalRequirement = .always(false),
        onApproval: ShellApprovalFunction? = nil,
        environment: ShellToolEnvironment? = nil
    ) {
        self.executor = executor
        self.name = name
        self.needsApproval = needsApproval
        self.onApproval = onApproval
        self.environment = environment
    }

    public var type: String { "shell" }
}

public struct ApplyPatchTool: @unchecked Sendable {
    public var editor: ApplyPatchEditor
    public var name: String
    public var needsApproval: ToolApprovalRequirement
    public var onApproval: ApplyPatchApprovalFunction?

    public init(editor: ApplyPatchEditor, name: String = "apply_patch", needsApproval: ToolApprovalRequirement = .always(false), onApproval: ApplyPatchApprovalFunction? = nil) {
        self.editor = editor
        self.name = name
        self.needsApproval = needsApproval
        self.onApproval = onApproval
    }

    public var type: String { "apply_patch" }
}

public enum Tool: @unchecked Sendable {
    case function(FunctionTool)
    case fileSearch(FileSearchTool)
    case webSearch(WebSearchTool)
    case computer(ComputerTool)
    case hostedMCP(HostedMCPTool)
    case codeInterpreter(CodeInterpreterTool)
    case imageGeneration(ImageGenerationTool)
    case shell(ShellTool)
    case applyPatch(ApplyPatchTool)
    case localShell(LocalShellTool)

    public var name: String {
        switch self {
        case .function(let tool): return tool.name
        case .fileSearch(let tool): return tool.name
        case .webSearch(let tool): return tool.name
        case .computer(let tool): return tool.name
        case .hostedMCP(let tool): return tool.name
        case .codeInterpreter(let tool): return tool.name
        case .imageGeneration(let tool): return tool.name
        case .shell(let tool): return tool.name
        case .applyPatch(let tool): return tool.name
        case .localShell(let tool): return tool.name
        }
    }

    public var type: String {
        switch self {
        case .function: return "function"
        case .fileSearch: return "file_search"
        case .webSearch: return "web_search"
        case .computer: return "computer"
        case .hostedMCP: return "hosted_mcp"
        case .codeInterpreter: return "code_interpreter"
        case .imageGeneration: return "image_generation"
        case .shell: return "shell"
        case .applyPatch: return "apply_patch"
        case .localShell: return "local_shell"
        }
    }

    public var description: String {
        switch self {
        case .function(let tool):
            return tool.description
        case .fileSearch:
            return "Search files in configured vector stores."
        case .webSearch:
            return "Search the web."
        case .computer:
            return "Use a computer interaction tool."
        case .hostedMCP:
            return "Invoke a hosted MCP tool."
        case .codeInterpreter:
            return "Invoke a code interpreter."
        case .imageGeneration:
            return "Generate an image."
        case .shell:
            return "Run shell commands."
        case .applyPatch:
            return "Apply structured file patches."
        case .localShell:
            return "Run a local shell command."
        }
    }

    public var inputSchema: [String: JSONValue] {
        switch self {
        case .function(let tool):
            return tool.paramsJSONSchema
        case .fileSearch(let tool):
            var properties: [String: JSONValue] = ["query": .object(["type": .string("string")])]
            if let maxNumResults = tool.maxNumResults {
                properties["max_num_results"] = .number(Double(maxNumResults))
            }
            return ensureStrictJSONSchema([
                "type": .string("object"),
                "properties": .object(properties),
                "required": .array([.string("query")]),
                "additionalProperties": .bool(false),
            ])
        case .webSearch:
            return ensureStrictJSONSchema([
                "type": .string("object"),
                "properties": .object([
                    "query": .object(["type": .string("string")]),
                ]),
                "required": .array([.string("query")]),
                "additionalProperties": .bool(false),
            ])
        case .computer:
            return ensureStrictJSONSchema([
                "type": .string("object"),
                "properties": .object([
                    "action": .object(["type": .string("string")]),
                    "x": .object(["type": .string("integer")]),
                    "y": .object(["type": .string("integer")]),
                ]),
                "required": .array([.string("action")]),
                "additionalProperties": .bool(false),
            ])
        case .hostedMCP, .codeInterpreter, .imageGeneration:
            return ensureStrictJSONSchema([
                "type": .string("object"),
                "properties": .object([:]),
                "required": .array([]),
                "additionalProperties": .bool(true),
            ])
        case .shell:
            return ensureStrictJSONSchema([
                "type": .string("object"),
                "properties": .object([
                    "commands": .object(["type": .string("array"), "items": .object(["type": .string("string")])]),
                    "timeout_ms": .object(["type": .string("integer")]),
                    "max_output_length": .object(["type": .string("integer")]),
                ]),
                "required": .array([.string("commands")]),
                "additionalProperties": .bool(false),
            ])
        case .applyPatch:
            return ensureStrictJSONSchema([
                "type": .string("object"),
                "properties": .object([
                    "operations": .object([
                        "type": .string("array"),
                        "items": .object([
                            "type": .string("object"),
                            "properties": .object([
                                "type": .object(["type": .string("string")]),
                                "path": .object(["type": .string("string")]),
                                "diff": .object(["type": .string("string")]),
                            ]),
                        ]),
                    ]),
                ]),
                "required": .array([.string("operations")]),
                "additionalProperties": .bool(false),
            ])
        case .localShell:
            return ensureStrictJSONSchema([
                "type": .string("object"),
                "properties": .object([
                    "commands": .object(["type": .string("array"), "items": .object(["type": .string("string")])]),
                    "timeout_ms": .object(["type": .string("integer")]),
                    "max_output_length": .object(["type": .string("integer")]),
                ]),
                "required": .array([.string("commands")]),
                "additionalProperties": .bool(false),
            ])
        }
    }

    public func llmDefinition() throws -> OmniAICore.Tool? {
        switch self {
        case .fileSearch, .webSearch, .computer, .hostedMCP, .codeInterpreter, .imageGeneration, .shell, .applyPatch, .localShell:
            return nil
        default:
            return try OmniAICore.Tool(name: name, description: description, parameters: .object(inputSchema))
        }
    }
}

public func defaultToolErrorFunction(
    error: Error,
    toolName: String,
    callID: String,
    context: ToolContext<Any>
) async -> String {
    "Tool \(toolName) failed: \(String(describing: error))"
}

private struct _ResolvedComputer: Sendable {
    let computer: any AsyncComputer
    let dispose: (@Sendable (any AsyncComputer) async -> Void)?
}

private actor _ResolvedComputerStore {
    static let shared = _ResolvedComputerStore()
    private var byRunContext: [ObjectIdentifier: [UUID: _ResolvedComputer]] = [:]

    func get(runContext: RunContextWrapper<Any>, toolID: UUID) -> _ResolvedComputer? {
        byRunContext[ObjectIdentifier(runContext)]?[toolID]
    }

    func set(runContext: RunContextWrapper<Any>, toolID: UUID, resolved: _ResolvedComputer) {
        let key = ObjectIdentifier(runContext)
        var tools = byRunContext[key] ?? [:]
        tools[toolID] = resolved
        byRunContext[key] = tools
    }

    func removeAll(runContext: RunContextWrapper<Any>) -> [(_ResolvedComputer, UUID)] {
        let key = ObjectIdentifier(runContext)
        let current = byRunContext.removeValue(forKey: key) ?? [:]
        return current.map { ($0.value, $0.key) }
    }
}

public func resolveComputer(tool: ComputerTool, runContext: RunContextWrapper<Any>) async throws -> any AsyncComputer {
    if let cached = await _ResolvedComputerStore.shared.get(runContext: runContext, toolID: tool.id) {
        return cached.computer
    }

    let resolved: _ResolvedComputer
    switch tool.computer {
    case .instance(let computer):
        resolved = _ResolvedComputer(computer: computer, dispose: nil)
    case .provider(let provider):
        let computer = try await provider.create()
        resolved = _ResolvedComputer(computer: computer, dispose: provider.dispose)
    }
    await _ResolvedComputerStore.shared.set(runContext: runContext, toolID: tool.id, resolved: resolved)
    return resolved.computer
}

public func disposeResolvedComputers(runContext: RunContextWrapper<Any>) async {
    let resolved = await _ResolvedComputerStore.shared.removeAll(runContext: runContext)
    for (item, _) in resolved {
        if let dispose = item.dispose {
            await dispose(item.computer)
        }
    }
}

public func functionTool<Parameters: Decodable & Sendable>(
    name: String,
    description: String,
    schema: FuncSchema<Parameters>? = nil,
    isEnabled: ToolEnabledPredicate = .always(true),
    needsApproval: ToolApprovalRequirement = .always(false),
    strictJSONSchema: Bool = true,
    timeoutSeconds: Double? = nil,
    timeoutBehavior: ToolTimeoutBehavior = .messageToModel,
    toolInputGuardrails: [ToolInputGuardrail<Any>]? = nil,
    toolOutputGuardrails: [ToolOutputGuardrail<Any>]? = nil,
    _ function: @escaping @Sendable (ToolContext<Any>, Parameters) async throws -> Any
) -> Tool {
    let resolvedSchema = schema ?? FuncSchema<Parameters>(name: name, description: description, strictJSONSchema: strictJSONSchema)
    return .function(FunctionTool(
        name: name,
        description: description,
        paramsJSONSchema: resolvedSchema.paramsJSONSchema,
        onInvokeTool: { context, rawArguments in
            let parameters = try resolvedSchema.toCallArgs(rawArguments)
            return try await function(context, parameters)
        },
        strictJSONSchema: strictJSONSchema,
        isEnabled: isEnabled,
        toolInputGuardrails: toolInputGuardrails,
        toolOutputGuardrails: toolOutputGuardrails,
        needsApproval: needsApproval,
        timeoutSeconds: timeoutSeconds,
        timeoutBehavior: timeoutBehavior,
        timeoutErrorFunction: defaultToolErrorFunction,
        isAgentTool: false,
        isCodexTool: false,
        agentInstance: nil
    ))
}
