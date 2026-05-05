import Foundation
import OmniAICore

let defaultOpenAIRealtimeSessionBaseURL: URL = {
    guard let url = URL(string: "wss://api.openai.com/v1/realtime") else {
        preconditionFailure("Invalid default OpenAI realtime URL")
    }
    return url
}()

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
public struct OpenAIRealtimeSessionOptions: Sendable {
    public var model: String
    public var outputModalities: [String]
    public var voice: RealtimeVoice?
    public var inputAudioFormat: RealtimeAudioFormat?
    public var outputAudioFormat: RealtimeAudioFormat?
    public var turnDetection: RealtimeTurnDetection?
    public var temperature: Double?
    public var maxOutputTokens: Int?
    public var toolChoice: String?

    public init(
        model: String = "gpt-realtime",
        outputModalities: [String] = ["text", "audio"],
        voice: RealtimeVoice? = nil,
        inputAudioFormat: RealtimeAudioFormat? = nil,
        outputAudioFormat: RealtimeAudioFormat? = nil,
        turnDetection: RealtimeTurnDetection? = nil,
        temperature: Double? = nil,
        maxOutputTokens: Int? = nil,
        toolChoice: String? = "auto"
    ) {
        self.model = model
        self.outputModalities = outputModalities
        self.voice = voice
        self.inputAudioFormat = inputAudioFormat
        self.outputAudioFormat = outputAudioFormat
        self.turnDetection = turnDetection
        self.temperature = temperature
        self.maxOutputTokens = maxOutputTokens
        self.toolChoice = toolChoice
    }
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
public struct OpenAIRealtimeSessionSnapshot: Sendable, Equatable {
    public var currentAgentName: String
    public var historyCount: Int
    public var isConnected: Bool
    public var lastText: String?
    public var lastTranscript: String?
    public var bufferedAudioBytes: Int

    public init(currentAgentName: String, historyCount: Int, isConnected: Bool, lastText: String?, lastTranscript: String?, bufferedAudioBytes: Int) {
        self.currentAgentName = currentAgentName
        self.historyCount = historyCount
        self.isConnected = isConnected
        self.lastText = lastText
        self.lastTranscript = lastTranscript
        self.bufferedAudioBytes = bufferedAudioBytes
    }
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
public enum OpenAIRealtimeSessionEvent<TContext: Sendable>: Sendable {
    case raw(RealtimeServerEvent)
    case textDelta(String)
    case textDone(String)
    case audioDelta(Data, outputIndex: Int?)
    case audioDone(Data?, outputIndex: Int?)
    case transcriptDelta(String)
    case transcriptDone(String)
    case toolStarted(name: String, callID: String)
    case toolFinished(name: String, callID: String, output: String)
    case toolFailed(name: String, callID: String, message: String)
    case agentUpdated(Agent<TContext>)
    case outputGuardrailTriggered(name: String, text: String)
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
public final actor OpenAIRealtimeSession<TContext: Sendable> {
    public let client: OpenAIRealtimeClient
    public let contextWrapper: RunContextWrapper<TContext>
    public let options: OpenAIRealtimeSessionOptions

    private var currentAgent: Agent<TContext>
    private let hooks: RunHooks<TContext>?
    private var history: [RealtimeConversationItem] = []
    private var lastText: String?
    private var lastTranscript: String?
    private var audioBuffers: [Int: Data] = [:]
    private var receiveTask: Task<Void, Never>?
    private let eventStreamStorage: AsyncThrowingStream<OpenAIRealtimeSessionEvent<TContext>, Error>
    private let eventContinuation: AsyncThrowingStream<OpenAIRealtimeSessionEvent<TContext>, Error>.Continuation

    public init(
        agent: Agent<TContext>,
        context: TContext,
        client: OpenAIRealtimeClient,
        options: OpenAIRealtimeSessionOptions = OpenAIRealtimeSessionOptions(),
        hooks: RunHooks<TContext>? = nil
    ) {
        self.currentAgent = agent
        self.contextWrapper = RunContextWrapper(context: context)
        self.client = client
        self.options = options
        self.hooks = hooks
        let eventStream = makeThrowingStream(of: OpenAIRealtimeSessionEvent<TContext>.self)
        self.eventStreamStorage = eventStream.stream
        self.eventContinuation = eventStream.continuation
    }

    public func connect() async throws {
        let stream = try await client.connect(model: options.model)
        try await applyCurrentAgentConfiguration()
        await hooks?.onAgentStart(agent: currentAgent, context: contextWrapper)
        await currentAgent.hooks?.onStart(agent: currentAgent, context: contextWrapper)
        receiveTask = Task { [weak self] in
            await self?.consume(stream)
        }
    }

    public func close() async {
        receiveTask?.cancel()
        receiveTask = nil
        await hooks?.onAgentEnd(agent: currentAgent, result: NSNull(), context: contextWrapper)
        await currentAgent.hooks?.onEnd(agent: currentAgent, output: NSNull(), context: contextWrapper)
        await client.disconnect()
        eventContinuation.finish()
    }

    public func events() -> AsyncThrowingStream<OpenAIRealtimeSessionEvent<TContext>, Error> {
        eventStreamStorage
    }

    public func snapshot() -> OpenAIRealtimeSessionSnapshot {
        OpenAIRealtimeSessionSnapshot(
            currentAgentName: currentAgent.name,
            historyCount: history.count,
            isConnected: client.isConnected,
            lastText: lastText,
            lastTranscript: lastTranscript,
            bufferedAudioBytes: audioBuffers.values.reduce(0) { $0 + $1.count }
        )
    }

    public func sendUserMessage(_ text: String) async throws {
        try await client.sendUserMessage(text)
    }

    public func sendUserAudio(_ audioData: Data) async throws {
        try await client.sendUserAudio(audioData)
    }

    public func appendInputAudio(_ audioData: Data) async throws {
        try await client.appendInputAudio(audioData)
    }

    public func commitInputAudio() async throws {
        try await client.commitInputAudio()
    }

    public func clearInputAudio() async throws {
        try await client.clearInputAudio()
    }

    public func createResponse(_ config: RealtimeResponseConfig? = nil) async throws {
        try await client.createResponse(config)
    }

    public func cancelResponse() async throws {
        try await client.cancelResponse()
    }

    private func consume(_ stream: AsyncThrowingStream<RealtimeServerEvent, Error>) async {
        do {
            for try await event in stream {
                await emit(.raw(event))
                await updateHistory(for: event)

                switch event {
                case .responseTextDelta(let delta):
                    await emit(.textDelta(delta.delta))
                case .responseTextDone(let done):
                    lastText = done.text
                    await emit(.textDone(done.text))
                    try await runOutputGuardrailsIfNeeded(text: done.text)
                case .responseAudioDelta(let delta):
                    let bytes = try delta.decodedAudioData()
                    let key = delta.outputIndex ?? 0
                    audioBuffers[key, default: Data()].append(bytes)
                    await emit(.audioDelta(bytes, outputIndex: delta.outputIndex))
                case .responseAudioDone(let done):
                    let key = done.outputIndex ?? 0
                    let data = audioBuffers[key]
                    await emit(.audioDone(data, outputIndex: done.outputIndex))
                case .responseAudioTranscriptDelta(let delta):
                    await emit(.transcriptDelta(delta.delta))
                case .responseAudioTranscriptDone(let done):
                    lastTranscript = done.transcript
                    await emit(.transcriptDone(done.transcript))
                    try await runOutputGuardrailsIfNeeded(text: done.transcript)
                case .responseFunctionCallArgumentsDone(let functionCall):
                    try await handleFunctionCall(functionCall)
                default:
                    break
                }
            }
            eventContinuation.finish()
        } catch {
            eventContinuation.finish(throwing: error)
        }
    }

    private func emit(_ event: OpenAIRealtimeSessionEvent<TContext>) {
        eventContinuation.yield(event)
    }

    private func updateHistory(for event: RealtimeServerEvent) {
        switch event {
        case .conversationItemAdded(let itemEvent), .conversationItemDone(let itemEvent):
            guard let itemID = itemEvent.item.id else {
                history.append(itemEvent.item)
                return
            }
            if let index = history.firstIndex(where: { $0.id == itemID }) {
                history[index] = itemEvent.item
            } else {
                history.append(itemEvent.item)
            }
        case .conversationItemDeleted(let itemID):
            history.removeAll { $0.id == itemID }
        case .conversationItemTruncated(let trunc):
            if let index = history.firstIndex(where: { $0.id == trunc.itemId }) {
                history[index] = RealtimeConversationItem(
                    id: history[index].id,
                    type: history[index].type,
                    role: history[index].role,
                    content: history[index].content,
                    callId: history[index].callId,
                    name: history[index].name,
                    arguments: history[index].arguments,
                    output: history[index].output,
                    status: history[index].status
                )
            }
        default:
            break
        }
    }

    private func applyCurrentAgentConfiguration() async throws {
        let instructions = try await currentAgent.getSystemPrompt(runContext: contextWrapper)
        let tools = try await realtimeTools(for: currentAgent)
        let config = RealtimeSessionConfig(
            model: options.model,
            instructions: instructions,
            outputModalities: options.outputModalities,
            voice: options.voice,
            inputAudioFormat: options.inputAudioFormat,
            outputAudioFormat: options.outputAudioFormat,
            turnDetection: options.turnDetection,
            tools: tools.isEmpty ? nil : tools,
            toolChoice: options.toolChoice,
            temperature: options.temperature,
            maxOutputTokens: options.maxOutputTokens
        )
        try await client.updateSession(config)
    }

    private func realtimeTools(for agent: Agent<TContext>) async throws -> [RealtimeTool] {
        let allTools = try await agent.getAllTools(runContext: contextWrapper)
        var realtimeTools: [RealtimeTool] = []
        for tool in allTools {
            switch tool {
            case .function(let functionTool):
                realtimeTools.append(convertRealtimeTool(functionTool))
            default:
                throw UserError(message: "OpenAI realtime currently supports only function tools and handoffs in OmniAgentsSDK.")
            }
        }
        for handoff in agent.handoffs {
            realtimeTools.append(RealtimeTool(
                name: handoff.toolName,
                description: handoff.toolDescription,
                parameters: convertRealtimeToolParameters(handoff.inputJSONSchema)
            ))
        }
        return realtimeTools
    }

    private func convertRealtimeTool(_ tool: FunctionTool) -> RealtimeTool {
        RealtimeTool(
            name: tool.name,
            description: tool.description,
            parameters: convertRealtimeToolParameters(tool.paramsJSONSchema)
        )
    }

    private func convertRealtimeToolParameters(_ schema: [String: JSONValue]) -> RealtimeToolParameters? {
        guard schema["type"]?.stringValue == "object" else {
            return nil
        }
        let propertiesObject = schema["properties"]?.objectValue ?? [:]
        let required = schema["required"]?.arrayValue?.compactMap(\.stringValue)
        let properties = propertiesObject.mapValues { value -> RealtimeToolProperty in
            let object = value.objectValue ?? [:]
            let type = object["type"]?.stringValue ?? "object"
            let description = object["description"]?.stringValue
            let enumValues = object["enum"]?.arrayValue?.compactMap(\.stringValue)
            return RealtimeToolProperty(type: type, description: description, enumValues: enumValues)
        }
        return RealtimeToolParameters(type: "object", properties: properties.isEmpty ? nil : properties, required: required)
    }

    private func handleFunctionCall(_ call: RealtimeFunctionCallEvent) async throws {
        guard let name = call.name, let callID = call.callId else {
            return
        }
        let arguments = call.arguments ?? "{}"

        if let handoff = currentAgent.handoffs.first(where: { $0.toolName == name }) {
            let previousAgent = currentAgent
            let nextAgent = try await handoff.onInvokeHandoff(contextWrapper, arguments)
            currentAgent = nextAgent
            await hooks?.onHandoff(from: previousAgent, to: nextAgent, context: contextWrapper)
            await previousAgent.hooks?.onHandoff(from: previousAgent, to: nextAgent, context: contextWrapper)
            try await applyCurrentAgentConfiguration()
            try await client.sendFunctionCallOutput(callId: callID, output: handoff.getTransferMessage())
            try await client.createResponse()
            await emit(.agentUpdated(nextAgent))
            return
        }

        let allTools = try await currentAgent.getAllTools(runContext: contextWrapper)
        guard case .function(let functionTool)? = allTools.first(where: { $0.name == name }) else {
            let errorString = "{\"error\":\"Tool \(name) not found\"}"
            await emit(.toolFailed(name: name, callID: callID, message: "Tool \(name) not found"))
            try await client.sendFunctionCallOutput(callId: callID, output: errorString)
            try await client.createResponse()
            return
        }

        await emit(.toolStarted(name: name, callID: callID))
        await hooks?.onToolStart(tool: .function(functionTool), context: contextWrapper, arguments: arguments, callID: callID)
        await currentAgent.hooks?.onToolStart(agent: currentAgent, tool: .function(functionTool), context: contextWrapper, arguments: arguments, callID: callID)

        let toolCall = ToolCall(
            id: callID,
            name: name,
            arguments: (try? JSONValue.parse(Data(arguments.utf8)).objectValue) ?? [:],
            rawArguments: arguments
        )
        let toolContext = ToolContext.fromAgentContext(
            contextWrapper,
            toolCallID: callID,
            toolCall: toolCall,
            agent: AnyAgent(currentAgent),
            runConfig: nil
        )

        do {
            let output = try await invokeRealtimeFunctionTool(functionTool, context: toolContext, rawArguments: arguments)
            let serialized = try serializeRealtimeToolOutput(output)
            try await client.sendFunctionCallOutput(callId: callID, output: serialized)
            try await client.createResponse()
            await hooks?.onToolEnd(tool: .function(functionTool), context: contextWrapper, result: output, callID: callID)
            await currentAgent.hooks?.onToolEnd(agent: currentAgent, tool: .function(functionTool), context: contextWrapper, result: output, callID: callID)
            await emit(.toolFinished(name: name, callID: callID, output: serialized))
        } catch {
            let message = await defaultToolErrorFunction(error: error, toolName: name, callID: callID, context: ToolContext<Any>(context: contextWrapper.context as Any, toolName: name, toolCallID: callID, toolArguments: arguments))
            try await client.sendFunctionCallOutput(callId: callID, output: "{\"error\":\"\(message.replacing("\"", with: "\\\""))\"}")
            try await client.createResponse()
            await emit(.toolFailed(name: name, callID: callID, message: message))
        }
    }

    private func runOutputGuardrailsIfNeeded(text: String) async throws {
        for guardrail in currentAgent.outputGuardrails {
            let result = try await guardrail.run(context: contextWrapper, agent: currentAgent, agentOutput: text)
            if result.output.tripwireTriggered {
                await emit(.outputGuardrailTriggered(name: guardrail.getName(), text: text))
                try? await client.cancelResponse()
            }
        }
    }
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
public final class OpenAIRealtimeRunner<TContext: Sendable>: Sendable {
    public let startingAgent: Agent<TContext>
    public let options: OpenAIRealtimeSessionOptions
    public let hooks: RunHooks<TContext>?
    private let clientFactory: @Sendable () throws -> OpenAIRealtimeClient

    public init(
        startingAgent: Agent<TContext>,
        options: OpenAIRealtimeSessionOptions = OpenAIRealtimeSessionOptions(),
        hooks: RunHooks<TContext>? = nil,
        client: OpenAIRealtimeClient
    ) {
        self.startingAgent = startingAgent
        self.options = options
        self.hooks = hooks
        self.clientFactory = { client }
    }

    public init(
        startingAgent: Agent<TContext>,
        apiKey: String,
        baseURL: URL? = nil,
        transport: any RealtimeWebSocketTransport = defaultRealtimeWebSocketTransport(),
        options: OpenAIRealtimeSessionOptions = OpenAIRealtimeSessionOptions(),
        hooks: RunHooks<TContext>? = nil
    ) {
        self.startingAgent = startingAgent
        self.options = options
        self.hooks = hooks
        self.clientFactory = {
            OpenAIRealtimeClient(apiKey: apiKey, baseURL: baseURL ?? defaultOpenAIRealtimeSessionBaseURL, transport: transport)
        }
    }

    public func run(context: TContext) async throws -> OpenAIRealtimeSession<TContext> {
        let session = OpenAIRealtimeSession(
            agent: startingAgent,
            context: context,
            client: try clientFactory(),
            options: options,
            hooks: hooks
        )
        try await session.connect()
        return session
    }
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
private func invokeRealtimeFunctionTool<TContext>(_ tool: FunctionTool, context: ToolContext<TContext>, rawArguments: String) async throws -> Any {
    try await withRealtimeTimeout(seconds: tool.timeoutSeconds) {
        try await tool.onInvokeTool(ToolContext<Any>(context: context.context as Any, usage: context.usage, toolName: context.toolName, toolCallID: context.toolCallID, toolArguments: context.toolArguments, toolCall: context.toolCall, agent: context.agent, runConfig: context.runConfig, turnInput: context.turnInput, toolInput: context.toolInput), rawArguments)
    }
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
private func serializeRealtimeToolOutput(_ output: Any) throws -> String {
    if let string = output as? String {
        return string
    }
    if let text = output as? ToolOutputText {
        return text.text
    }
    if let jsonValue = output as? JSONValue {
        let data = try jsonValue.data()
        return String(decoding: data, as: UTF8.self)
    }
    if let object = output as? [String: JSONValue] {
        let data = try JSONValue.object(object).data()
        return String(decoding: data, as: UTF8.self)
    }
    if let object = output as? [String: Any] {
        let data = try JSONValue(object).data()
        return String(decoding: data, as: UTF8.self)
    }
    if let array = output as? [Any] {
        let data = try JSONValue(array).data()
        return String(decoding: data, as: UTF8.self)
    }
    if let image = output as? ToolOutputImage {
        let object: [String: JSONValue] = [
            "type": .string("image"),
            "image_url": image.imageURL.map(JSONValue.string) ?? .null,
            "file_id": image.fileID.map(JSONValue.string) ?? .null,
            "detail": image.detail.map { .string($0.rawValue) } ?? .null,
        ]
        return String(decoding: try JSONValue.object(object).data(), as: UTF8.self)
    }
    if let file = output as? ToolOutputFileContent {
        let object: [String: JSONValue] = [
            "type": .string("file"),
            "file_data": file.fileData.map(JSONValue.string) ?? .null,
            "file_url": file.fileURL.map(JSONValue.string) ?? .null,
            "file_id": file.fileID.map(JSONValue.string) ?? .null,
            "filename": file.filename.map(JSONValue.string) ?? .null,
        ]
        return String(decoding: try JSONValue.object(object).data(), as: UTF8.self)
    }
    return String(describing: output)
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
private final class _RealtimeUncheckedBox<Value>: @unchecked Sendable {
    let value: Value
    init(_ value: Value) { self.value = value }
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
private func withRealtimeTimeout<T>(seconds: Double?, operation: @escaping @Sendable () async throws -> T) async throws -> T {
    guard let seconds else { return try await operation() }
    return try await withThrowingTaskGroup(of: _RealtimeUncheckedBox<T>.self) { group in
        group.addTask { _RealtimeUncheckedBox(try await operation()) }
        group.addTask {
            try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            throw ToolTimeoutError(toolName: "realtime_tool", timeoutSeconds: seconds)
        }
        guard let result = try await group.next() else {
            group.cancelAll()
            throw ToolTimeoutError(toolName: "realtime_tool", timeoutSeconds: seconds)
        }
        group.cancelAll()
        return result.value
    }
}
