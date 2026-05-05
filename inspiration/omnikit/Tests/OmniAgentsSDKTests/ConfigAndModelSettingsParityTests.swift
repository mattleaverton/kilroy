import Foundation
import Testing
import OmniAgentsSDK
import OmniAICore

struct ConfigAndModelSettingsParityTests {
    @Test
    func config_defaults_and_trace_export_precedence() throws {
        let initial = getGlobalConfig()
        #expect(initial.defaultOpenAIAPI == .responses)
        #expect(initial.defaultOpenAIResponsesTransport == .http)

        setDefaultOpenAIKey("sk-phase1-key", useForTracing: true)
        var snapshot = getGlobalConfig()
        #expect(snapshot.defaultOpenAIKey == "sk-phase1-key")
        #expect(snapshot.tracingExportAPIKey == "sk-phase1-key")

        let client = try Client.fromEnv(environment: ["OPENAI_API_KEY": "sk-phase1-client"])
        setDefaultOpenAIClient(client, useForTracing: true)
        try setDefaultOpenAIAPI("chat_completions")
        try setDefaultOpenAIResponsesTransport("websocket")
        setTracingDisabled(true)
        setTraceProcessors([{ _ in }])

        snapshot = getGlobalConfig()
        #expect(snapshot.defaultOpenAIAPI == .chatCompletions)
        #expect(snapshot.defaultOpenAIResponsesTransport == .websocket)
        #expect(snapshot.tracingDisabled)
        #expect(snapshot.tracingExportAPIKey == "sk-phase1-client")
        #expect(snapshot.traceProcessors.count == 1)

        let lock = NSLock()
        var snapshots: [OmniAgentsGlobalConfigSnapshot] = []
        snapshots.reserveCapacity(32)
        DispatchQueue.concurrentPerform(iterations: 32) { _ in
            let snapshot = getGlobalConfig()
            lock.lock()
            snapshots.append(snapshot)
            lock.unlock()
        }
        #expect(snapshots.allSatisfy { $0.defaultOpenAIAPI == .chatCompletions })
        #expect(snapshots.allSatisfy { $0.defaultOpenAIResponsesTransport == .websocket })
        #expect(snapshots.allSatisfy { $0.tracingExportAPIKey == "sk-phase1-client" })
    }

    @Test
    func model_settings_resolve_merges_extra_args_and_overrides_scalars() {
        let base = ModelSettings(
            temperature: 0.2,
            topP: 0.7,
            reasoning: Reasoning(effort: "low", summary: "base"),
            includeUsage: false,
            extraHeaders: ["x-base": .value("one")],
            extraArgs: [
                "shared": .string("base"),
                "base_only": .number(1),
            ]
        )
        let override = ModelSettings(
            temperature: 0.9,
            includeUsage: true,
            extraHeaders: ["x-override": .value("two")],
            extraArgs: [
                "shared": .string("override"),
                "override_only": .bool(true),
            ]
        )

        let merged = base.resolve(override: override)
        #expect(merged.temperature == 0.9)
        #expect(merged.topP == 0.7)
        #expect(merged.includeUsage == true)
        #expect(merged.reasoning == Reasoning(effort: "low", summary: "base"))
        #expect(merged.extraHeaders == ["x-override": HeaderValue.value("two")])
        #expect(merged.extraArgs == [
            "shared": JSONValue.string("override"),
            "base_only": JSONValue.number(1),
            "override_only": JSONValue.bool(true),
        ])
    }

    @Test
    func model_settings_to_json_dictionary_uses_snake_case_and_nulls() {
        let settings = ModelSettings(
            temperature: 0.3,
            toolChoice: .mcpToolChoice(MCPToolChoice(serverLabel: "math", name: "add")),
            parallelToolCalls: false,
            truncation: .auto,
            maxTokens: 128,
            reasoning: Reasoning(effort: "high"),
            verbosity: .high,
            metadata: ["team": "sdk"],
            store: true,
            promptCacheRetention: .inMemory,
            includeUsage: false,
            responseInclude: [.typed(.fileSearchCallResults), .raw("custom.include")],
            topLogprobs: 2,
            extraQuery: ["alpha": .number(1)],
            extraBody: ["beta": .bool(true)],
            extraHeaders: [
                "x-token": .value("secret"),
                "x-none": .omit,
            ],
            extraArgs: ["gamma": .string("delta")]
        )

        let json = settings.toJSONDictionary()
        #expect(json["temperature"] == .number(0.3))
        #expect(json["top_p"] == .null)
        #expect(json["tool_choice"] == .object([
            "server_label": .string("math"),
            "name": .string("add"),
        ]))
        #expect(json["parallel_tool_calls"] == .bool(false))
        #expect(json["truncation"] == .string("auto"))
        #expect(json["max_tokens"] == .number(128))
        #expect(json["reasoning"] == .object([
            "effort": .string("high"),
            "summary": .null,
        ]))
        #expect(json["verbosity"] == .string("high"))
        #expect(json["metadata"] == .object(["team": .string("sdk")]))
        #expect(json["store"] == .bool(true))
        #expect(json["prompt_cache_retention"] == .string("in_memory"))
        #expect(json["include_usage"] == .bool(false))
        #expect(json["response_include"] == .array([
            .string("file_search_call.results"),
            .string("custom.include"),
        ]))
        #expect(json["top_logprobs"] == .number(2))
        #expect(json["extra_query"] == .object(["alpha": .number(1)]))
        #expect(json["extra_body"] == .object(["beta": .bool(true)]))
        #expect(json["extra_headers"] == .object([
            "x-token": .string("secret"),
            "x-none": .null,
        ]))
        #expect(json["extra_args"] == .object(["gamma": .string("delta")]))
    }
}
