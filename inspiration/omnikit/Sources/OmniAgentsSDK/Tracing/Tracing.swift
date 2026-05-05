import Foundation
import OmniAICore

public struct TracingConfig: Sendable, Equatable {
    public var disabled: Bool
    public var exportAPIKey: String?

    public init(disabled: Bool = false, exportAPIKey: String? = nil) {
        self.disabled = disabled
        self.exportAPIKey = exportAPIKey
    }
}

public struct SpanData: Sendable, Equatable, Codable {
    public var kind: String
    public var attributes: [String: JSONValue]

    public init(kind: String, attributes: [String: JSONValue] = [:]) {
        self.kind = kind
        self.attributes = attributes
    }
}

public typealias AgentSpanData = SpanData
public typealias GenerationSpanData = SpanData
public typealias FunctionSpanData = SpanData
public typealias GuardrailSpanData = SpanData
public typealias HandoffSpanData = SpanData
public typealias MCPListToolsSpanData = SpanData
public typealias CustomSpanData = SpanData

public struct SpeechSpanData: Sendable, Equatable {
    public var input: String?
    public var output: String?
    public var outputFormat: String?
    public var model: String?
    public var modelConfig: [String: JSONValue]?
    public var firstContentAt: String?

    public init(
        input: String? = nil,
        output: String? = nil,
        outputFormat: String? = "pcm",
        model: String? = nil,
        modelConfig: [String: JSONValue]? = nil,
        firstContentAt: String? = nil
    ) {
        self.input = input
        self.output = output
        self.outputFormat = outputFormat
        self.model = model
        self.modelConfig = modelConfig
        self.firstContentAt = firstContentAt
    }

    func asSpanData() -> SpanData {
        var attributes: [String: JSONValue] = [:]
        if let input { attributes["input"] = .string(input) }
        if let output { attributes["output"] = .string(output) }
        if let outputFormat { attributes["output_format"] = .string(outputFormat) }
        if let model { attributes["model"] = .string(model) }
        if let modelConfig { attributes["model_config"] = .object(modelConfig) }
        if let firstContentAt { attributes["first_content_at"] = .string(firstContentAt) }
        return SpanData(kind: "speech", attributes: attributes)
    }
}

public struct SpeechGroupSpanData: Sendable, Equatable {
    public var input: String?

    public init(input: String? = nil) {
        self.input = input
    }

    func asSpanData() -> SpanData {
        var attributes: [String: JSONValue] = [:]
        if let input { attributes["input"] = .string(input) }
        return SpanData(kind: "speech_group", attributes: attributes)
    }
}

public struct TranscriptionSpanData: Sendable, Equatable {
    public var input: String?
    public var inputFormat: String?
    public var output: String?
    public var model: String?
    public var modelConfig: [String: JSONValue]?

    public init(
        input: String? = nil,
        inputFormat: String? = "pcm",
        output: String? = nil,
        model: String? = nil,
        modelConfig: [String: JSONValue]? = nil
    ) {
        self.input = input
        self.inputFormat = inputFormat
        self.output = output
        self.model = model
        self.modelConfig = modelConfig
    }

    func asSpanData() -> SpanData {
        var attributes: [String: JSONValue] = [:]
        if let input { attributes["input"] = .string(input) }
        if let inputFormat { attributes["input_format"] = .string(inputFormat) }
        if let output { attributes["output"] = .string(output) }
        if let model { attributes["model"] = .string(model) }
        if let modelConfig { attributes["model_config"] = .object(modelConfig) }
        return SpanData(kind: "transcription", attributes: attributes)
    }
}

public struct Span: Sendable, Equatable {
    public var id: String
    public var name: String
    public var data: SpanData?
    public var startedAt: Date
    public var endedAt: Date?
    public var error: SpanError?

    public init(id: String = genSpanID(), name: String, data: SpanData? = nil, startedAt: Date = Date(), endedAt: Date? = nil, error: SpanError? = nil) {
        self.id = id
        self.name = name
        self.data = data
        self.startedAt = startedAt
        self.endedAt = endedAt
        self.error = error
    }
}

public struct Trace: Sendable, Equatable {
    public var id: String
    public var name: String
    public var groupID: String?
    public var metadata: [String: JSONValue]?
    public var startedAt: Date
    public var endedAt: Date?
    public var spans: [Span]

    public init(id: String = genTraceID(), name: String, groupID: String? = nil, metadata: [String: JSONValue]? = nil, startedAt: Date = Date(), endedAt: Date? = nil, spans: [Span] = []) {
        self.id = id
        self.name = name
        self.groupID = groupID
        self.metadata = metadata
        self.startedAt = startedAt
        self.endedAt = endedAt
        self.spans = spans
    }
}

public protocol TracingProcessor: Sendable {
    func process(trace: Trace) async
}

public protocol TraceProvider: Sendable {
    func registerProcessor(_ processor: TracingProcessor)
    func setProcessors(_ processors: [TracingProcessor])
    func getCurrentTrace() -> Trace?
    func getCurrentSpan() -> Span?
    func setDisabled(_ disabled: Bool)
    func createTrace(name: String, groupID: String?, metadata: [String: JSONValue]?) -> Trace
    func finish(trace: Trace) async
    func createSpan(name: String, spanData: SpanData?, spanID: String?, parent: Any?, disabled: Bool) -> Span
}

public final class ClosureTracingProcessor: TracingProcessor, Sendable {
    private let closure: @Sendable (Trace) async -> Void
    public init(_ closure: @escaping @Sendable (Trace) async -> Void) { self.closure = closure }
    public func process(trace: Trace) async { await closure(trace) }
}

private final class TracingState: @unchecked Sendable {
    static let shared = TracingState()

    private let lock = NSLock()
    private var currentTrace: Trace?
    private var currentSpan: Span?
    private var processors: [TracingProcessor] = []
    private var disabled = false

    func setCurrentTrace(_ trace: Trace?) {
        lock.lock()
        currentTrace = trace
        lock.unlock()
    }

    func getCurrentTrace() -> Trace? {
        lock.lock(); defer { lock.unlock() }
        return currentTrace
    }

    func setCurrentSpan(_ span: Span?) {
        lock.lock()
        currentSpan = span
        lock.unlock()
    }

    func getCurrentSpan() -> Span? {
        lock.lock(); defer { lock.unlock() }
        return currentSpan
    }

    func setProcessors(_ processors: [TracingProcessor]) {
        lock.lock()
        self.processors = processors
        lock.unlock()
    }

    func addProcessor(_ processor: TracingProcessor) {
        lock.lock()
        processors.append(processor)
        lock.unlock()
    }

    func getProcessors() -> [TracingProcessor] {
        lock.lock(); defer { lock.unlock() }
        return processors
    }

    func setDisabled(_ disabled: Bool) {
        lock.lock()
        self.disabled = disabled
        lock.unlock()
    }

    func isDisabled() -> Bool {
        lock.lock(); defer { lock.unlock() }
        return disabled
    }
}

private final class DefaultTraceProvider: TraceProvider, Sendable {
    func registerProcessor(_ processor: TracingProcessor) {
        TracingState.shared.addProcessor(processor)
    }

    func setProcessors(_ processors: [TracingProcessor]) {
        TracingState.shared.setProcessors(processors)
    }

    func getCurrentTrace() -> Trace? {
        TracingState.shared.getCurrentTrace()
    }

    func getCurrentSpan() -> Span? {
        TracingState.shared.getCurrentSpan()
    }

    func setDisabled(_ disabled: Bool) {
        TracingState.shared.setDisabled(disabled)
    }

    func createTrace(name: String, groupID: String?, metadata: [String: JSONValue]?) -> Trace {
        let trace = Trace(name: name, groupID: groupID, metadata: metadata)
        if !TracingState.shared.isDisabled() {
            TracingState.shared.setCurrentTrace(trace)
        }
        return trace
    }

    func finish(trace: Trace) async {
        guard !TracingState.shared.isDisabled() else {
            TracingState.shared.setCurrentTrace(nil)
            return
        }
        var finished = trace
        finished.endedAt = Date()
        for processor in TracingState.shared.getProcessors() {
            await processor.process(trace: finished)
        }
        TracingState.shared.setCurrentTrace(nil)
    }

    func createSpan(name: String, spanData: SpanData?, spanID: String?, parent: Any?, disabled: Bool) -> Span {
        let _ = parent
        let _ = disabled
        return Span(id: spanID ?? genSpanID(), name: name, data: spanData)
    }
}

private final class TraceProviderStore: @unchecked Sendable {
    static let shared = TraceProviderStore()
    private let lock = NSLock()
    private var provider: TraceProvider?

    func set(_ provider: TraceProvider?) {
        lock.lock(); self.provider = provider; lock.unlock()
    }

    func get() -> TraceProvider {
        lock.lock(); defer { lock.unlock() }
        return provider ?? DefaultTraceProvider()
    }
}

private struct TraceSpanAdapter: ErrorTracingSpan {
    let spanID: String
    let traceID: String?
    func setError(_ error: SpanError) {}
}

public func genTraceID() -> String { UUID().uuidString.filter { $0 != "-" } }
public func genSpanID() -> String { UUID().uuidString.filter { $0 != "-" } }

public func getTraceProvider() -> TraceProvider {
    TraceProviderStore.shared.get()
}

public func setTraceProvider(_ provider: TraceProvider?) {
    TraceProviderStore.shared.set(provider)
}

public func setTraceProcessors(_ processors: [TracingProcessor]) {
    getTraceProvider().setProcessors(processors)
}

public func addTraceProcessor(_ processor: TracingProcessor) {
    getTraceProvider().registerProcessor(processor)
}

public func getCurrentTrace() -> Trace? {
    getTraceProvider().getCurrentTrace()
}

public func getCurrentSpan() -> Span? {
    getTraceProvider().getCurrentSpan()
}

public func createTraceForRun(name: String, groupID: String? = nil, metadata: [String: JSONValue]? = nil) -> Trace {
    getTraceProvider().createTrace(name: name, groupID: groupID, metadata: metadata)
}

public func trace(name: String, groupID: String? = nil, metadata: [String: JSONValue]? = nil) -> Trace {
    createTraceForRun(name: name, groupID: groupID, metadata: metadata)
}

public func finishTrace(_ trace: Trace) async {
    await getTraceProvider().finish(trace: trace)
}

public func withSpan<T>(name: String, data: SpanData? = nil, operation: () async throws -> T) async rethrows -> T {
    let span = Span(name: name, data: data)
    TracingState.shared.setCurrentSpan(span)
    setCurrentErrorTracingSpanProvider {
        TraceSpanAdapter(spanID: span.id, traceID: getCurrentTrace()?.id)
    }
    defer {
        TracingState.shared.setCurrentSpan(nil)
        setCurrentErrorTracingSpanProvider(nil)
    }
    return try await operation()
}

public func agentSpan(name: String, handoffs: [String]? = nil, tools: [String]? = nil, outputType: String? = nil, spanID: String? = nil, parent: Any? = nil, disabled: Bool = false) -> Span {
    var attributes: [String: JSONValue] = ["name": .string(name)]
    if let handoffs { attributes["handoffs"] = .array(handoffs.map(JSONValue.string)) }
    if let tools { attributes["tools"] = .array(tools.map(JSONValue.string)) }
    if let outputType { attributes["output_type"] = .string(outputType) }
    return getTraceProvider().createSpan(name: name, spanData: SpanData(kind: "agent", attributes: attributes), spanID: spanID, parent: parent, disabled: disabled)
}

public func customSpan(name: String, data: [String: JSONValue]? = nil, spanID: String? = nil, parent: Any? = nil, disabled: Bool = false) -> Span {
    getTraceProvider().createSpan(name: name, spanData: SpanData(kind: "custom", attributes: data ?? [:]), spanID: spanID, parent: parent, disabled: disabled)
}

public func functionSpan(name: String, input: String? = nil, output: String? = nil, spanID: String? = nil, parent: Any? = nil, disabled: Bool = false) -> Span {
    var attributes: [String: JSONValue] = ["name": .string(name)]
    if let input { attributes["input"] = .string(input) }
    if let output { attributes["output"] = .string(output) }
    return getTraceProvider().createSpan(name: name, spanData: SpanData(kind: "function", attributes: attributes), spanID: spanID, parent: parent, disabled: disabled)
}

public func generationSpan(input: [TResponseInputItem]? = nil, output: [TResponseOutputItem]? = nil, model: String? = nil, modelConfig: [String: JSONValue]? = nil, usage: [String: JSONValue]? = nil, spanID: String? = nil, parent: Any? = nil, disabled: Bool = false) -> Span {
    var attributes: [String: JSONValue] = [:]
    if let input { attributes["input"] = .array(input.map(JSONValue.object)) }
    if let output { attributes["output"] = .array(output.map(JSONValue.object)) }
    if let model { attributes["model"] = .string(model) }
    if let modelConfig { attributes["model_config"] = .object(modelConfig) }
    if let usage { attributes["usage"] = .object(usage) }
    return getTraceProvider().createSpan(name: "generation", spanData: SpanData(kind: "generation", attributes: attributes), spanID: spanID, parent: parent, disabled: disabled)
}

public func guardrailSpan(name: String, triggered: Bool = false, spanID: String? = nil, parent: Any? = nil, disabled: Bool = false) -> Span {
    getTraceProvider().createSpan(name: name, spanData: SpanData(kind: "guardrail", attributes: ["name": .string(name), "triggered": .bool(triggered)]), spanID: spanID, parent: parent, disabled: disabled)
}

public func handoffSpan(fromAgent: String? = nil, toAgent: String? = nil, spanID: String? = nil, parent: Any? = nil, disabled: Bool = false) -> Span {
    var attributes: [String: JSONValue] = [:]
    if let fromAgent { attributes["from_agent"] = .string(fromAgent) }
    if let toAgent { attributes["to_agent"] = .string(toAgent) }
    return getTraceProvider().createSpan(name: "handoff", spanData: SpanData(kind: "handoff", attributes: attributes), spanID: spanID, parent: parent, disabled: disabled)
}

public func mcpToolsSpan(server: String? = nil, result: [String]? = nil, spanID: String? = nil, parent: Any? = nil, disabled: Bool = false) -> Span {
    var attributes: [String: JSONValue] = [:]
    if let server { attributes["server"] = .string(server) }
    if let result { attributes["result"] = .array(result.map(JSONValue.string)) }
    return getTraceProvider().createSpan(name: "mcp_list_tools", spanData: SpanData(kind: "mcp_list_tools", attributes: attributes), spanID: spanID, parent: parent, disabled: disabled)
}

public func speechSpan(input: String? = nil, output: String? = nil, outputFormat: String? = "pcm", model: String? = nil, modelConfig: [String: JSONValue]? = nil, firstContentAt: String? = nil, spanID: String? = nil, parent: Any? = nil, disabled: Bool = false) -> Span {
    let data = SpeechSpanData(input: input, output: output, outputFormat: outputFormat, model: model, modelConfig: modelConfig, firstContentAt: firstContentAt)
    return getTraceProvider().createSpan(name: "speech", spanData: data.asSpanData(), spanID: spanID, parent: parent, disabled: disabled)
}

public func speechGroupSpan(input: String? = nil, spanID: String? = nil, parent: Any? = nil, disabled: Bool = false) -> Span {
    let data = SpeechGroupSpanData(input: input)
    return getTraceProvider().createSpan(name: "speech_group", spanData: data.asSpanData(), spanID: spanID, parent: parent, disabled: disabled)
}

public func transcriptionSpan(input: String? = nil, inputFormat: String? = "pcm", output: String? = nil, model: String? = nil, modelConfig: [String: JSONValue]? = nil, spanID: String? = nil, parent: Any? = nil, disabled: Bool = false) -> Span {
    let data = TranscriptionSpanData(input: input, inputFormat: inputFormat, output: output, model: model, modelConfig: modelConfig)
    return getTraceProvider().createSpan(name: "transcription", spanData: data.asSpanData(), spanID: spanID, parent: parent, disabled: disabled)
}
