import Foundation
import OmniAICore

public enum OpenAIDefaultAPI: String, Codable, Sendable {
    case chatCompletions = "chat_completions"
    case responses
}

public enum OpenAIResponsesTransport: String, Codable, Sendable {
    case http
    case websocket
}

public struct TraceEvent: Sendable, Equatable {
    public var name: String
    public var attributes: [String: String]

    public init(name: String, attributes: [String: String] = [:]) {
        self.name = name
        self.attributes = attributes
    }
}

public typealias TraceProcessor = @Sendable (TraceEvent) -> Void

public struct OmniAgentsGlobalConfigSnapshot: Sendable {
    public var defaultOpenAIKey: String?
    public var defaultOpenAIClient: Client?
    public var defaultOpenAIAPI: OpenAIDefaultAPI
    public var defaultOpenAIResponsesTransport: OpenAIResponsesTransport
    public var tracingDisabled: Bool
    public var traceProcessors: [TraceProcessor]
    public var tracingExportAPIKey: String?

    public init(
        defaultOpenAIKey: String? = nil,
        defaultOpenAIClient: Client? = nil,
        defaultOpenAIAPI: OpenAIDefaultAPI = .responses,
        defaultOpenAIResponsesTransport: OpenAIResponsesTransport = .http,
        tracingDisabled: Bool = false,
        traceProcessors: [TraceProcessor] = [],
        tracingExportAPIKey: String? = nil
    ) {
        self.defaultOpenAIKey = defaultOpenAIKey
        self.defaultOpenAIClient = defaultOpenAIClient
        self.defaultOpenAIAPI = defaultOpenAIAPI
        self.defaultOpenAIResponsesTransport = defaultOpenAIResponsesTransport
        self.tracingDisabled = tracingDisabled
        self.traceProcessors = traceProcessors
        self.tracingExportAPIKey = tracingExportAPIKey
    }
}

private final class GlobalConfigState: @unchecked Sendable {
    private let lock = NSLock()
    private var value = OmniAgentsGlobalConfigSnapshot()

    func mutate(_ transform: (inout OmniAgentsGlobalConfigSnapshot) -> Void) {
        lock.lock()
        transform(&value)
        lock.unlock()
    }

    func snapshot() -> OmniAgentsGlobalConfigSnapshot {
        lock.lock()
        defer { lock.unlock() }
        return value
    }
}

private enum GlobalConfig {
    static let shared = GlobalConfigState()
}

public func setDefaultOpenAIKey(_ key: String, useForTracing: Bool = true) {
    GlobalConfig.shared.mutate {
        $0.defaultOpenAIKey = key
        if useForTracing {
            $0.tracingExportAPIKey = key
        }
    }
}

public func setDefaultOpenAIClient(_ client: Client, useForTracing: Bool = true) {
    GlobalConfig.shared.mutate {
        $0.defaultOpenAIClient = client
        if useForTracing, let apiKey = extractOpenAIAPIKey(from: client) {
            $0.tracingExportAPIKey = apiKey
        }
    }
}

public func setDefaultOpenAIAPI(_ api: OpenAIDefaultAPI) {
    GlobalConfig.shared.mutate { $0.defaultOpenAIAPI = api }
}

public func setDefaultOpenAIAPI(_ api: String) throws {
    switch api.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
    case OpenAIDefaultAPI.chatCompletions.rawValue:
        setDefaultOpenAIAPI(.chatCompletions)
    case OpenAIDefaultAPI.responses.rawValue:
        setDefaultOpenAIAPI(.responses)
    default:
        throw UserError(message: "Invalid OpenAI API. Expected one of: 'chat_completions', 'responses'.")
    }
}

public func setDefaultOpenAIResponsesTransport(_ transport: OpenAIResponsesTransport) {
    GlobalConfig.shared.mutate { $0.defaultOpenAIResponsesTransport = transport }
}

public func setDefaultOpenAIResponsesTransport(_ transport: String) throws {
    switch transport.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
    case OpenAIResponsesTransport.http.rawValue:
        setDefaultOpenAIResponsesTransport(.http)
    case OpenAIResponsesTransport.websocket.rawValue:
        setDefaultOpenAIResponsesTransport(.websocket)
    default:
        throw UserError(message: "Invalid OpenAI Responses transport. Expected one of: 'http', 'websocket'.")
    }
}

public func setTracingDisabled(_ disabled: Bool) {
    GlobalConfig.shared.mutate { $0.tracingDisabled = disabled }
}

public func setTraceProcessors(_ processors: [TraceProcessor]) {
    GlobalConfig.shared.mutate { $0.traceProcessors = processors }
}

public func setTracingExportAPIKey(_ key: String?) {
    GlobalConfig.shared.mutate { $0.tracingExportAPIKey = key }
}

public func getGlobalConfig() -> OmniAgentsGlobalConfigSnapshot {
    GlobalConfig.shared.snapshot()
}

private func extractOpenAIAPIKey(from client: Client) -> String? {
    let providers = reflectProviders(from: client)
    guard let openAIProvider = providers["openai"] else {
        return nil
    }

    guard let apiKey = extractStringProperty(named: "apiKey", from: openAIProvider), !apiKey.isEmpty else {
        return nil
    }
    return apiKey
}

private func reflectProviders(from client: Client) -> [String: Any] {
    var currentMirror: Mirror? = Mirror(reflecting: client)
    while let mirror = currentMirror {
        for child in mirror.children where child.label == "providers" {
            if let providers = child.value as? [String: any ProviderAdapter] {
                var result: [String: Any] = [:]
                for (name, adapter) in providers {
                    result[name] = adapter
                }
                return result
            }
            if let providers = child.value as? [String: Any] {
                return providers
            }
        }
        currentMirror = mirror.superclassMirror
    }
    return [:]
}

private func extractStringProperty(named propertyName: String, from value: Any) -> String? {
    var currentMirror: Mirror? = Mirror(reflecting: value)
    while let mirror = currentMirror {
        for child in mirror.children {
            guard child.label == propertyName else { continue }
            return child.value as? String
        }
        currentMirror = mirror.superclassMirror
    }
    return nil
}
