import Foundation
import OmniAICore

public struct MCPToolChoice: Codable, Sendable, Equatable {
    public var serverLabel: String
    public var name: String

    public init(serverLabel: String, name: String) {
        self.serverLabel = serverLabel
        self.name = name
    }

    enum CodingKeys: String, CodingKey {
        case serverLabel = "server_label"
        case name
    }
}

public enum ToolChoice: Sendable, Equatable {
    case auto
    case required
    case none
    case named(String)
    case mcpToolChoice(MCPToolChoice)
}

extension ToolChoice: Codable {
    public init(from decoder: any Decoder) throws {
        let singleValue = try decoder.singleValueContainer()
        if let literal = try? singleValue.decode(String.self) {
            switch literal {
            case "auto":
                self = .auto
            case "required":
                self = .required
            case "none":
                self = .none
            default:
                self = .named(literal)
            }
            return
        }

        let mcpChoice = try singleValue.decode(MCPToolChoice.self)
        self = .mcpToolChoice(mcpChoice)
    }

    public func encode(to encoder: any Encoder) throws {
        var singleValue = encoder.singleValueContainer()
        switch self {
        case .auto:
            try singleValue.encode("auto")
        case .required:
            try singleValue.encode("required")
        case .none:
            try singleValue.encode("none")
        case .named(let name):
            try singleValue.encode(name)
        case .mcpToolChoice(let choice):
            try singleValue.encode(choice)
        }
    }
}

public enum TruncationStrategy: String, Codable, Sendable {
    case auto
    case disabled
}

public enum ModelVerbosity: String, Codable, Sendable {
    case low
    case medium
    case high
}

public enum PromptCacheRetention: String, Codable, Sendable {
    case inMemory = "in_memory"
    case twentyFourHours = "24h"
}

public struct Reasoning: Codable, Sendable, Equatable {
    public var effort: String?
    public var summary: String?

    public init(effort: String? = nil, summary: String? = nil) {
        self.effort = effort
        self.summary = summary
    }

    public init(from decoder: any Decoder) throws {
        let singleValue = try decoder.singleValueContainer()
        if let effort = try? singleValue.decode(String.self) {
            self = Reasoning(effort: effort)
            return
        }

        let container = try decoder.container(keyedBy: CodingKeys.self)
        effort = try container.decodeIfPresent(String.self, forKey: .effort)
        summary = try container.decodeIfPresent(String.self, forKey: .summary)
    }

    enum CodingKeys: String, CodingKey {
        case effort
        case summary
    }
}

public enum ResponseIncludable: String, Codable, Sendable, Equatable {
    case fileSearchCallResults = "file_search_call.results"
    case messageOutputTextLogprobs = "message.output_text.logprobs"
}

public enum ResponseIncludeItem: Sendable, Equatable, Codable {
    case typed(ResponseIncludable)
    case raw(String)

    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        let value = try container.decode(String.self)
        if let includable = ResponseIncludable(rawValue: value) {
            self = .typed(includable)
        } else {
            self = .raw(value)
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .typed(let includable):
            try container.encode(includable.rawValue)
        case .raw(let value):
            try container.encode(value)
        }
    }

    fileprivate var rawValue: String {
        switch self {
        case .typed(let includable):
            return includable.rawValue
        case .raw(let value):
            return value
        }
    }
}

public enum HeaderValue: Sendable, Equatable, Codable {
    case value(String)
    case omit

    public init(from decoder: any Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() {
            self = .omit
            return
        }
        self = .value(try container.decode(String.self))
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .value(let value):
            try container.encode(value)
        case .omit:
            try container.encodeNil()
        }
    }

    fileprivate var asJSONValue: JSONValue {
        switch self {
        case .value(let value):
            return .string(value)
        case .omit:
            return .null
        }
    }
}

public struct ModelSettings: Codable, Sendable, Equatable {
    public var temperature: Double?
    public var topP: Double?
    public var frequencyPenalty: Double?
    public var presencePenalty: Double?
    public var toolChoice: ToolChoice?
    public var parallelToolCalls: Bool?
    public var truncation: TruncationStrategy?
    public var maxTokens: Int?
    public var reasoning: Reasoning?
    public var verbosity: ModelVerbosity?
    public var metadata: [String: String]?
    public var store: Bool?
    public var promptCacheRetention: PromptCacheRetention?
    public var includeUsage: Bool?
    public var responseInclude: [ResponseIncludeItem]?
    public var topLogprobs: Int?
    public var extraQuery: [String: JSONValue]?
    public var extraBody: [String: JSONValue]?
    public var extraHeaders: [String: HeaderValue]?
    public var extraArgs: [String: JSONValue]?

    public init(
        temperature: Double? = nil,
        topP: Double? = nil,
        frequencyPenalty: Double? = nil,
        presencePenalty: Double? = nil,
        toolChoice: ToolChoice? = nil,
        parallelToolCalls: Bool? = nil,
        truncation: TruncationStrategy? = nil,
        maxTokens: Int? = nil,
        reasoning: Reasoning? = nil,
        verbosity: ModelVerbosity? = nil,
        metadata: [String: String]? = nil,
        store: Bool? = nil,
        promptCacheRetention: PromptCacheRetention? = nil,
        includeUsage: Bool? = nil,
        responseInclude: [ResponseIncludeItem]? = nil,
        topLogprobs: Int? = nil,
        extraQuery: [String: JSONValue]? = nil,
        extraBody: [String: JSONValue]? = nil,
        extraHeaders: [String: HeaderValue]? = nil,
        extraArgs: [String: JSONValue]? = nil
    ) {
        self.temperature = temperature
        self.topP = topP
        self.frequencyPenalty = frequencyPenalty
        self.presencePenalty = presencePenalty
        self.toolChoice = toolChoice
        self.parallelToolCalls = parallelToolCalls
        self.truncation = truncation
        self.maxTokens = maxTokens
        self.reasoning = reasoning
        self.verbosity = verbosity
        self.metadata = metadata
        self.store = store
        self.promptCacheRetention = promptCacheRetention
        self.includeUsage = includeUsage
        self.responseInclude = responseInclude
        self.topLogprobs = topLogprobs
        self.extraQuery = extraQuery
        self.extraBody = extraBody
        self.extraHeaders = extraHeaders
        self.extraArgs = extraArgs
    }

    public func resolve(override: ModelSettings?) -> ModelSettings {
        guard let override else { return self }

        var merged = self
        merged.temperature = override.temperature ?? temperature
        merged.topP = override.topP ?? topP
        merged.frequencyPenalty = override.frequencyPenalty ?? frequencyPenalty
        merged.presencePenalty = override.presencePenalty ?? presencePenalty
        merged.toolChoice = override.toolChoice ?? toolChoice
        merged.parallelToolCalls = override.parallelToolCalls ?? parallelToolCalls
        merged.truncation = override.truncation ?? truncation
        merged.maxTokens = override.maxTokens ?? maxTokens
        merged.reasoning = override.reasoning ?? reasoning
        merged.verbosity = override.verbosity ?? verbosity
        merged.metadata = override.metadata ?? metadata
        merged.store = override.store ?? store
        merged.promptCacheRetention = override.promptCacheRetention ?? promptCacheRetention
        merged.includeUsage = override.includeUsage ?? includeUsage
        merged.responseInclude = override.responseInclude ?? responseInclude
        merged.topLogprobs = override.topLogprobs ?? topLogprobs
        merged.extraQuery = override.extraQuery ?? extraQuery
        merged.extraBody = override.extraBody ?? extraBody
        merged.extraHeaders = override.extraHeaders ?? extraHeaders

        if extraArgs != nil || override.extraArgs != nil {
            var mergedArgs = extraArgs ?? [:]
            if let overrideArgs = override.extraArgs {
                for (key, value) in overrideArgs {
                    mergedArgs[key] = value
                }
            }
            merged.extraArgs = mergedArgs.isEmpty ? nil : mergedArgs
        }

        return merged
    }

    /// Python parity helper for `ModelSettings.to_json_dict()`.
    public func toJSONDictionary() -> [String: JSONValue] {
        func numberValue(_ value: Double?) -> JSONValue {
            guard let value else { return .null }
            return .number(value)
        }

        func integerValue(_ value: Int?) -> JSONValue {
            guard let value else { return .null }
            return .number(Double(value))
        }

        func boolValue(_ value: Bool?) -> JSONValue {
            guard let value else { return .null }
            return .bool(value)
        }

        func stringValue(_ value: String?) -> JSONValue {
            guard let value else { return .null }
            return .string(value)
        }

        func responseIncludeListValue(_ value: [ResponseIncludeItem]?) -> JSONValue {
            guard let value else { return .null }
            return .array(value.map { .string($0.rawValue) })
        }

        func jsonMapValue(_ value: [String: JSONValue]?) -> JSONValue {
            guard let value else { return .null }
            return .object(value)
        }

        func reasoningValue(_ value: Reasoning?) -> JSONValue {
            guard let value else { return .null }
            return .object([
                "effort": value.effort.map(JSONValue.string) ?? .null,
                "summary": value.summary.map(JSONValue.string) ?? .null,
            ])
        }

        func toolChoiceValue(_ value: ToolChoice?) -> JSONValue {
            guard let value else { return .null }
            switch value {
            case .auto:
                return .string("auto")
            case .required:
                return .string("required")
            case .none:
                return .string("none")
            case .named(let name):
                return .string(name)
            case .mcpToolChoice(let choice):
                return .object([
                    "server_label": .string(choice.serverLabel),
                    "name": .string(choice.name),
                ])
            }
        }

        func headerMapValue(_ value: [String: HeaderValue]?) -> JSONValue {
            guard let value else { return .null }
            return .object(value.mapValues { $0.asJSONValue })
        }

        return [
            "temperature": numberValue(temperature),
            "top_p": numberValue(topP),
            "frequency_penalty": numberValue(frequencyPenalty),
            "presence_penalty": numberValue(presencePenalty),
            "tool_choice": toolChoiceValue(toolChoice),
            "parallel_tool_calls": boolValue(parallelToolCalls),
            "truncation": stringValue(truncation?.rawValue),
            "max_tokens": integerValue(maxTokens),
            "reasoning": reasoningValue(reasoning),
            "verbosity": stringValue(verbosity?.rawValue),
            "metadata": metadata.map { .object($0.mapValues(JSONValue.string)) } ?? .null,
            "store": boolValue(store),
            "prompt_cache_retention": stringValue(promptCacheRetention?.rawValue),
            "include_usage": boolValue(includeUsage),
            "response_include": responseIncludeListValue(responseInclude),
            "top_logprobs": integerValue(topLogprobs),
            "extra_query": jsonMapValue(extraQuery),
            "extra_body": jsonMapValue(extraBody),
            "extra_headers": headerMapValue(extraHeaders),
            "extra_args": jsonMapValue(extraArgs),
        ]
    }

    enum CodingKeys: String, CodingKey {
        case temperature
        case topP = "top_p"
        case frequencyPenalty = "frequency_penalty"
        case presencePenalty = "presence_penalty"
        case toolChoice = "tool_choice"
        case parallelToolCalls = "parallel_tool_calls"
        case truncation
        case maxTokens = "max_tokens"
        case reasoning
        case verbosity
        case metadata
        case store
        case promptCacheRetention = "prompt_cache_retention"
        case includeUsage = "include_usage"
        case responseInclude = "response_include"
        case topLogprobs = "top_logprobs"
        case extraQuery = "extra_query"
        case extraBody = "extra_body"
        case extraHeaders = "extra_headers"
        case extraArgs = "extra_args"
    }
}
