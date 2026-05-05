import Foundation
import OmniAICore

public struct InputTokensDetails: Codable, Sendable, Equatable {
    public var cachedTokens: Int

    public init(cachedTokens: Int = 0) {
        self.cachedTokens = cachedTokens
    }

    enum CodingKeys: String, CodingKey {
        case cachedTokens = "cached_tokens"
    }
}

public struct OutputTokensDetails: Codable, Sendable, Equatable {
    public var reasoningTokens: Int

    public init(reasoningTokens: Int = 0) {
        self.reasoningTokens = reasoningTokens
    }

    enum CodingKeys: String, CodingKey {
        case reasoningTokens = "reasoning_tokens"
    }
}

public struct RequestUsage: Codable, Sendable, Equatable {
    public var inputTokens: Int
    public var outputTokens: Int
    public var totalTokens: Int
    public var inputTokensDetails: InputTokensDetails
    public var outputTokensDetails: OutputTokensDetails

    public init(
        inputTokens: Int,
        outputTokens: Int,
        totalTokens: Int? = nil,
        inputTokensDetails: InputTokensDetails = InputTokensDetails(),
        outputTokensDetails: OutputTokensDetails = OutputTokensDetails()
    ) {
        self.inputTokens = inputTokens
        self.outputTokens = outputTokens
        self.totalTokens = totalTokens ?? (inputTokens + outputTokens)
        self.inputTokensDetails = inputTokensDetails
        self.outputTokensDetails = outputTokensDetails
    }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        inputTokens = try container.decodeIfPresent(Int.self, forKey: .inputTokens) ?? 0
        outputTokens = try container.decodeIfPresent(Int.self, forKey: .outputTokens) ?? 0
        totalTokens = try container.decodeIfPresent(Int.self, forKey: .totalTokens) ?? (inputTokens + outputTokens)
        inputTokensDetails = try Self.decodeInputTokenDetails(container: container)
        outputTokensDetails = try Self.decodeOutputTokenDetails(container: container)
    }

    enum CodingKeys: String, CodingKey {
        case inputTokens = "input_tokens"
        case outputTokens = "output_tokens"
        case totalTokens = "total_tokens"
        case inputTokensDetails = "input_tokens_details"
        case outputTokensDetails = "output_tokens_details"
    }

    private static func decodeInputTokenDetails(
        container: KeyedDecodingContainer<CodingKeys>
    ) throws -> InputTokensDetails {
        if let details = try? container.decodeIfPresent(InputTokensDetails.self, forKey: .inputTokensDetails) {
            return details
        }
        if let detailsList = try? container.decodeIfPresent([InputTokensDetails].self, forKey: .inputTokensDetails),
           let first = detailsList.first {
            return first
        }
        return InputTokensDetails()
    }

    private static func decodeOutputTokenDetails(
        container: KeyedDecodingContainer<CodingKeys>
    ) throws -> OutputTokensDetails {
        if let details = try? container.decodeIfPresent(OutputTokensDetails.self, forKey: .outputTokensDetails) {
            return details
        }
        if let detailsList = try? container.decodeIfPresent([OutputTokensDetails].self, forKey: .outputTokensDetails),
           let first = detailsList.first {
            return first
        }
        return OutputTokensDetails()
    }
}

public struct Usage: Codable, Sendable, Equatable {
    public var requests: Int
    public var inputTokens: Int
    public var inputTokensDetails: InputTokensDetails
    public var outputTokens: Int
    public var outputTokensDetails: OutputTokensDetails
    public var totalTokens: Int
    public var requestUsageEntries: [RequestUsage]

    public init(
        requests: Int = 0,
        inputTokens: Int = 0,
        inputTokensDetails: InputTokensDetails = InputTokensDetails(),
        outputTokens: Int = 0,
        outputTokensDetails: OutputTokensDetails = OutputTokensDetails(),
        totalTokens: Int = 0,
        requestUsageEntries: [RequestUsage] = []
    ) {
        self.requests = requests
        self.inputTokens = inputTokens
        self.inputTokensDetails = inputTokensDetails
        self.outputTokens = outputTokens
        self.outputTokensDetails = outputTokensDetails
        self.totalTokens = totalTokens
        self.requestUsageEntries = requestUsageEntries
    }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        requests = try container.decodeIfPresent(Int.self, forKey: .requests) ?? 0
        inputTokens = try container.decodeIfPresent(Int.self, forKey: .inputTokens) ?? 0
        outputTokens = try container.decodeIfPresent(Int.self, forKey: .outputTokens) ?? 0
        totalTokens = try container.decodeIfPresent(Int.self, forKey: .totalTokens) ?? 0
        requestUsageEntries = try container.decodeIfPresent([RequestUsage].self, forKey: .requestUsageEntries) ?? []
        inputTokensDetails = try Self.decodeInputTokenDetails(container: container)
        outputTokensDetails = try Self.decodeOutputTokenDetails(container: container)
    }

    public mutating func add(_ other: Usage) {
        requests += other.requests
        inputTokens += other.inputTokens
        outputTokens += other.outputTokens
        totalTokens += other.totalTokens
        inputTokensDetails.cachedTokens += other.inputTokensDetails.cachedTokens
        outputTokensDetails.reasoningTokens += other.outputTokensDetails.reasoningTokens

        if other.requests == 1, other.totalTokens > 0 {
            requestUsageEntries.append(
                RequestUsage(
                    inputTokens: other.inputTokens,
                    outputTokens: other.outputTokens,
                    totalTokens: other.totalTokens,
                    inputTokensDetails: other.inputTokensDetails,
                    outputTokensDetails: other.outputTokensDetails
                )
            )
        } else if !other.requestUsageEntries.isEmpty {
            requestUsageEntries.append(contentsOf: other.requestUsageEntries)
        }
    }

    /// Python parity helper (`serialize_usage`) for persisting and restoring run state.
    public func toJSONDictionary() -> [String: JSONValue] {
        func serializeInputDetails(_ details: InputTokensDetails) -> JSONValue {
            .object(["cached_tokens": .number(Double(details.cachedTokens))])
        }

        func serializeOutputDetails(_ details: OutputTokensDetails) -> JSONValue {
            .object(["reasoning_tokens": .number(Double(details.reasoningTokens))])
        }

        let serializedEntries = requestUsageEntries.map { entry in
            JSONValue.object([
                "input_tokens": .number(Double(entry.inputTokens)),
                "output_tokens": .number(Double(entry.outputTokens)),
                "total_tokens": .number(Double(entry.totalTokens)),
                "input_tokens_details": serializeInputDetails(entry.inputTokensDetails),
                "output_tokens_details": serializeOutputDetails(entry.outputTokensDetails),
            ])
        }

        return [
            "requests": .number(Double(requests)),
            "input_tokens": .number(Double(inputTokens)),
            "input_tokens_details": .array([serializeInputDetails(inputTokensDetails)]),
            "output_tokens": .number(Double(outputTokens)),
            "output_tokens_details": .array([serializeOutputDetails(outputTokensDetails)]),
            "total_tokens": .number(Double(totalTokens)),
            "request_usage_entries": .array(serializedEntries),
        ]
    }

    /// Python parity helper (`deserialize_usage`) for resuming a persisted run.
    public static func fromJSONDictionary(_ usageData: [String: JSONValue]) -> Usage {
        func intValue(_ value: JSONValue?) -> Int {
            guard let value else { return 0 }
            switch value {
            case .number(let number):
                return Int(number)
            case .string(let string):
                return Int(string) ?? 0
            default:
                return 0
            }
        }

        func decodeInputDetails(_ value: JSONValue?) -> InputTokensDetails {
            guard let value else { return InputTokensDetails() }
            let payload: JSONValue
            if case .array(let array) = value, let first = array.first {
                payload = first
            } else {
                payload = value
            }
            guard case .object(let object) = payload else { return InputTokensDetails() }
            return InputTokensDetails(cachedTokens: intValue(object["cached_tokens"]))
        }

        func decodeOutputDetails(_ value: JSONValue?) -> OutputTokensDetails {
            guard let value else { return OutputTokensDetails() }
            let payload: JSONValue
            if case .array(let array) = value, let first = array.first {
                payload = first
            } else {
                payload = value
            }
            guard case .object(let object) = payload else { return OutputTokensDetails() }
            return OutputTokensDetails(reasoningTokens: intValue(object["reasoning_tokens"]))
        }

        func decodeRequestEntries(_ value: JSONValue?) -> [RequestUsage] {
            guard case .array(let array) = value else { return [] }
            return array.compactMap { entryValue in
                guard case .object(let entry) = entryValue else { return nil }
                return RequestUsage(
                    inputTokens: intValue(entry["input_tokens"]),
                    outputTokens: intValue(entry["output_tokens"]),
                    totalTokens: intValue(entry["total_tokens"]),
                    inputTokensDetails: decodeInputDetails(entry["input_tokens_details"]),
                    outputTokensDetails: decodeOutputDetails(entry["output_tokens_details"])
                )
            }
        }

        return Usage(
            requests: intValue(usageData["requests"]),
            inputTokens: intValue(usageData["input_tokens"]),
            inputTokensDetails: decodeInputDetails(usageData["input_tokens_details"]),
            outputTokens: intValue(usageData["output_tokens"]),
            outputTokensDetails: decodeOutputDetails(usageData["output_tokens_details"]),
            totalTokens: intValue(usageData["total_tokens"]),
            requestUsageEntries: decodeRequestEntries(usageData["request_usage_entries"])
        )
    }

    enum CodingKeys: String, CodingKey {
        case requests
        case inputTokens = "input_tokens"
        case inputTokensDetails = "input_tokens_details"
        case outputTokens = "output_tokens"
        case outputTokensDetails = "output_tokens_details"
        case totalTokens = "total_tokens"
        case requestUsageEntries = "request_usage_entries"
    }

    private static func decodeInputTokenDetails(
        container: KeyedDecodingContainer<CodingKeys>
    ) throws -> InputTokensDetails {
        if let details = try? container.decodeIfPresent(InputTokensDetails.self, forKey: .inputTokensDetails) {
            return details
        }
        if let detailsList = try? container.decodeIfPresent([InputTokensDetails].self, forKey: .inputTokensDetails),
           let first = detailsList.first {
            return first
        }
        return InputTokensDetails()
    }

    private static func decodeOutputTokenDetails(
        container: KeyedDecodingContainer<CodingKeys>
    ) throws -> OutputTokensDetails {
        if let details = try? container.decodeIfPresent(OutputTokensDetails.self, forKey: .outputTokensDetails) {
            return details
        }
        if let detailsList = try? container.decodeIfPresent([OutputTokensDetails].self, forKey: .outputTokensDetails),
           let first = detailsList.first {
            return first
        }
        return OutputTokensDetails()
    }
}

public func serializeUsage(_ usage: Usage) -> [String: JSONValue] {
    usage.toJSONDictionary()
}

public func deserializeUsage(_ usageData: [String: JSONValue]) -> Usage {
    Usage.fromJSONDictionary(usageData)
}
