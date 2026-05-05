import Foundation
import OmniAICore

open class OmniAICoreProvider: ModelProvider, @unchecked Sendable {
    public let providerName: String?
    public let client: Client
    public let providerOptions: [String: JSONValue]

    public init(
        providerName: String? = nil,
        client: Client? = nil,
        providerOptions: [String: JSONValue] = [:]
    ) {
        self.providerName = providerName
        self.client = client ?? makeDefaultClient(for: String(reflecting: Self.self))
        self.providerOptions = providerOptions
    }

    open func getModel(_ modelName: String?) -> any Model {
        OmniAICoreModel(
            modelName: modelName,
            providerName: providerName,
            client: client,
            providerOptions: providerOptions
        )
    }

    public func close() async {
        await client.close()
    }
}

public final class AnthropicProvider: OmniAICoreProvider, @unchecked Sendable {
    public init(client: Client? = nil, providerOptions: [String: JSONValue] = [:]) {
        super.init(providerName: "anthropic", client: client, providerOptions: providerOptions)
    }
}

public final class GeminiProvider: OmniAICoreProvider, @unchecked Sendable {
    public init(client: Client? = nil, providerOptions: [String: JSONValue] = [:]) {
        super.init(providerName: "gemini", client: client, providerOptions: providerOptions)
    }
}

public final class CerebrasProvider: OmniAICoreProvider, @unchecked Sendable {
    public init(client: Client? = nil, providerOptions: [String: JSONValue] = [:]) {
        super.init(providerName: "cerebras", client: client, providerOptions: providerOptions)
    }
}

public final class GroqProvider: OmniAICoreProvider, @unchecked Sendable {
    public init(client: Client? = nil, providerOptions: [String: JSONValue] = [:]) {
        super.init(providerName: "groq", client: client, providerOptions: providerOptions)
    }
}

public final class MultiProvider: ModelProvider, @unchecked Sendable {
    public let client: Client
    private let lock = NSLock()
    private var mapping: [String: any ModelProvider]
    private var fallbackDefaultProviderName: String?

    public init(
        providerMap: [String: any ModelProvider] = [:],
        client: Client? = nil,
        defaultProviderName: String? = nil
    ) {
        self.client = client ?? makeDefaultClient(for: String(reflecting: Self.self))
        self.mapping = providerMap
        self.fallbackDefaultProviderName = defaultProviderName
    }

    public var defaultProviderName: String? {
        lock.withLock {
            fallbackDefaultProviderName ?? client.defaultProviderName
        }
    }

    public func setDefaultProviderName(_ providerName: String?) {
        lock.withLock {
            fallbackDefaultProviderName = providerName
        }
    }

    public func getProvider(prefix: String) -> (any ModelProvider)? {
        lock.withLock { mapping[prefix] }
    }

    public func addProvider(prefix: String, provider: any ModelProvider) {
        lock.withLock {
            mapping[prefix] = provider
        }
    }

    public func removeProvider(prefix: String) {
        _ = lock.withLock {
            mapping.removeValue(forKey: prefix)
        }
    }

    public func getModel(_ modelName: String?) -> any Model {
        let (prefix, bareName) = split(modelName)

        if let prefix, let provider = getProvider(prefix: prefix) {
            return provider.getModel(bareName)
        }

        let resolvedProviderName = prefix ?? defaultProviderName
        return OmniAICoreProvider(
            providerName: resolvedProviderName,
            client: client
        ).getModel(bareName ?? modelName)
    }

    public func close() async {
        let providers = lock.withLock { Array(mapping.values) }
        for provider in providers {
            await provider.close()
        }
        await client.close()
    }

    private func split(_ modelName: String?) -> (String?, String?) {
        guard let modelName else {
            return (nil, nil)
        }
        guard let slash = modelName.firstIndex(of: "/") else {
            return (nil, modelName)
        }
        let prefix = String(modelName[..<slash])
        let bareName = String(modelName[modelName.index(after: slash)...])
        return (prefix, bareName)
    }
}

private extension NSLock {
    func withLock<T>(_ body: () -> T) -> T {
        lock()
        defer { unlock() }
        return body()
    }
}
