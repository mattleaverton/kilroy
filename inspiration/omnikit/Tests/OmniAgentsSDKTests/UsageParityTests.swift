import Foundation
import Testing
import OmniAgentsSDK
import OmniAICore

struct UsageParityTests {
    @Test
    func usage_round_trips_through_json_dictionary() {
        let original = Usage(
            requests: 2,
            inputTokens: 10,
            inputTokensDetails: InputTokensDetails(cachedTokens: 3),
            outputTokens: 7,
            outputTokensDetails: OutputTokensDetails(reasoningTokens: 2),
            totalTokens: 17,
            requestUsageEntries: [
                RequestUsage(
                    inputTokens: 4,
                    outputTokens: 1,
                    inputTokensDetails: InputTokensDetails(cachedTokens: 1),
                    outputTokensDetails: OutputTokensDetails(reasoningTokens: 0)
                ),
                RequestUsage(
                    inputTokens: 6,
                    outputTokens: 6,
                    inputTokensDetails: InputTokensDetails(cachedTokens: 2),
                    outputTokensDetails: OutputTokensDetails(reasoningTokens: 2)
                ),
            ]
        )

        let encoded = original.toJSONDictionary()
        let decoded = Usage.fromJSONDictionary(encoded)
        #expect(decoded == original)
        #expect(serializeUsage(original) == encoded)
        #expect(deserializeUsage(encoded) == original)
    }

    @Test
    func usage_tolerates_list_and_object_token_detail_shapes() throws {
        let listPayload: [String: Any] = [
            "input_tokens": 5,
            "output_tokens": 4,
            "total_tokens": 9,
            "input_tokens_details": [["cached_tokens": 2]],
            "output_tokens_details": [["reasoning_tokens": 1]],
        ]
        let objectPayload: [String: JSONValue] = [
            "requests": .number(1),
            "input_tokens": .number(5),
            "input_tokens_details": .object(["cached_tokens": .number(2)]),
            "output_tokens": .number(4),
            "output_tokens_details": .object(["reasoning_tokens": .number(1)]),
            "total_tokens": .number(9),
            "request_usage_entries": .array([]),
        ]

        let requestUsageData = try JSONSerialization.data(withJSONObject: listPayload)
        let requestUsage = try JSONDecoder().decode(RequestUsage.self, from: requestUsageData)
        #expect(requestUsage.inputTokensDetails == InputTokensDetails(cachedTokens: 2))
        #expect(requestUsage.outputTokensDetails == OutputTokensDetails(reasoningTokens: 1))

        let usage = Usage.fromJSONDictionary(objectPayload)
        #expect(usage.inputTokensDetails == InputTokensDetails(cachedTokens: 2))
        #expect(usage.outputTokensDetails == OutputTokensDetails(reasoningTokens: 1))
    }

    @Test
    func usage_add_preserves_request_usage_entry_semantics() {
        var aggregate = Usage(
            requests: 1,
            inputTokens: 3,
            inputTokensDetails: InputTokensDetails(cachedTokens: 1),
            outputTokens: 2,
            outputTokensDetails: OutputTokensDetails(reasoningTokens: 1),
            totalTokens: 5,
            requestUsageEntries: [
                RequestUsage(
                    inputTokens: 3,
                    outputTokens: 2,
                    totalTokens: 5,
                    inputTokensDetails: InputTokensDetails(cachedTokens: 1),
                    outputTokensDetails: OutputTokensDetails(reasoningTokens: 1)
                )
            ]
        )
        let singleRequest = Usage(
            requests: 1,
            inputTokens: 4,
            inputTokensDetails: InputTokensDetails(cachedTokens: 2),
            outputTokens: 6,
            outputTokensDetails: OutputTokensDetails(reasoningTokens: 3),
            totalTokens: 10
        )
        let preExpanded = Usage(
            requests: 2,
            inputTokens: 1,
            outputTokens: 1,
            totalTokens: 2,
            requestUsageEntries: [
                RequestUsage(inputTokens: 1, outputTokens: 0, totalTokens: 1),
                RequestUsage(inputTokens: 0, outputTokens: 1, totalTokens: 1),
            ]
        )

        aggregate.add(singleRequest)
        aggregate.add(preExpanded)

        #expect(aggregate.requests == 4)
        #expect(aggregate.inputTokens == 8)
        #expect(aggregate.outputTokens == 9)
        #expect(aggregate.totalTokens == 17)
        #expect(aggregate.inputTokensDetails.cachedTokens == 3)
        #expect(aggregate.outputTokensDetails.reasoningTokens == 4)
        #expect(aggregate.requestUsageEntries.count == 4)
        #expect(aggregate.requestUsageEntries[1] == RequestUsage(
            inputTokens: 4,
            outputTokens: 6,
            totalTokens: 10,
            inputTokensDetails: InputTokensDetails(cachedTokens: 2),
            outputTokensDetails: OutputTokensDetails(reasoningTokens: 3)
        ))
    }
}
