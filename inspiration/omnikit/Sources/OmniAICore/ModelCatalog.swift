import Foundation

public struct ModelInfo: Sendable, Equatable, Codable {
    public var id: String
    public var provider: String
    public var displayName: String
    public var contextWindow: Int
    public var maxOutput: Int?
    public var supportsTools: Bool
    public var supportsVision: Bool
    public var supportsReasoning: Bool
    public var inputCostPerMillion: Double?
    public var outputCostPerMillion: Double?
    public var aliases: [String]

    enum CodingKeys: String, CodingKey {
        case id
        case provider
        case displayName = "display_name"
        case contextWindow = "context_window"
        case maxOutput = "max_output"
        case supportsTools = "supports_tools"
        case supportsVision = "supports_vision"
        case supportsReasoning = "supports_reasoning"
        case inputCostPerMillion = "input_cost_per_million"
        case outputCostPerMillion = "output_cost_per_million"
        case aliases
    }
}

public enum ModelCapability: String, Sendable, Equatable {
    case tools
    case vision
    case reasoning
}

public struct ModelCatalog: Sendable, Equatable {
    private var models: [ModelInfo]
    private var byId: [String: ModelInfo]
    private var byAlias: [String: ModelInfo]

    public init(models: [ModelInfo]) {
        self.models = models
        var byId: [String: ModelInfo] = [:]
        var byAlias: [String: ModelInfo] = [:]
        for m in models {
            byId[m.id] = m
            for a in m.aliases {
                byAlias[a] = m
            }
        }
        self.byId = byId
        self.byAlias = byAlias
    }

    public static var `default`: ModelCatalog {
        ModelCatalog(models: KnownModels.all)
    }

    public func getModelInfo(_ modelIdOrAlias: String) -> ModelInfo? {
        if let m = byId[modelIdOrAlias] { return m }
        if let m = byAlias[modelIdOrAlias] { return m }
        return nil
    }

    public func listModels(provider: String? = nil) -> [ModelInfo] {
        if let provider {
            return models.filter { $0.provider == provider }
        }
        return models
    }

    public func getLatestModel(provider: String, capability: ModelCapability? = nil) -> ModelInfo? {
        func supports(_ m: ModelInfo) -> Bool {
            guard let capability else { return true }
            switch capability {
            case .tools: return m.supportsTools
            case .vision: return m.supportsVision
            case .reasoning: return m.supportsReasoning
            }
        }
        // The list is ordered by preference (best/newest first) per provider.
        return models.first(where: { $0.provider == provider && supports($0) })
    }

}
