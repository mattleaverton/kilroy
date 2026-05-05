import Testing

import Foundation

@testable import OmniAICore

@Suite
final class IntegrationSmokeTests {
    private func loadDotEnvIfPresent() -> [String: String] {
        // Best-effort dotenv loader so contributors can store keys in `.env` without exporting.
        let path = FileManager.default.currentDirectoryPath + "/.env"
        guard FileManager.default.fileExists(atPath: path) else { return [:] }
        guard let contents = try? String(contentsOfFile: path, encoding: .utf8) else { return [:] }

        var out: [String: String] = [:]
        for rawLine in contents.split(separator: "\n", omittingEmptySubsequences: false) {
            let line = rawLine.trimmingCharacters(in: .whitespacesAndNewlines)
            if line.isEmpty { continue }
            if line.hasPrefix("#") { continue }

            guard let eq = line.firstIndex(of: "=") else { continue }
            let key = String(line[..<eq]).trimmingCharacters(in: .whitespacesAndNewlines)
            var value = String(line[line.index(after: eq)...]).trimmingCharacters(in: .whitespacesAndNewlines)
            if value.hasPrefix("\""), value.hasSuffix("\""), value.count >= 2 {
                value = String(value.dropFirst().dropLast())
            } else if value.hasPrefix("'"), value.hasSuffix("'"), value.count >= 2 {
                value = String(value.dropFirst().dropLast())
            }
            if !key.isEmpty {
                out[key] = value
            }
        }
        return out
    }

    private func integrationEnvironment() -> [String: String] {
        var env = ProcessInfo.processInfo.environment
        for (k, v) in loadDotEnvIfPresent() {
            // Preserve explicitly-exported env vars over `.env`.
            if let existing = env[k], !existing.isEmpty { continue }
            env[k] = v
        }
        return env
    }

    private func integrationEnabled() -> Bool {
        return integrationEnvironment()["RUN_OMNIAI_INTEGRATION_TESTS"] == "1"
    }

    private func hasKey(_ name: String, env: [String: String]) -> Bool {
        let v = env[name] ?? ""
        return !v.isEmpty
    }

    private func integrationProviders(env: [String: String], dotEnv: [String: String]) -> [String] {
        if let raw = env["OMNIAI_INTEGRATION_PROVIDERS"], !raw.isEmpty {
            let parts = raw
                .split(separator: ",")
                .map { $0.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() }
                .filter { !$0.isEmpty }
            if !parts.isEmpty { return parts }
        }

        // Prefer providers explicitly configured in `.env` so local exported env vars don't
        // accidentally opt contributors into extra providers/cost/quota.
        if !dotEnv.isEmpty {
            var providers: [String] = []
            if hasKey("OPENAI_API_KEY", env: dotEnv) { providers.append("openai") }
            if hasKey("ANTHROPIC_API_KEY", env: dotEnv) { providers.append("anthropic") }
            if hasKey("GEMINI_API_KEY", env: dotEnv) || hasKey("GOOGLE_API_KEY", env: dotEnv) { providers.append("gemini") }
            if hasKey("CEREBRAS_API_KEY", env: dotEnv) { providers.append("cerebras") }
            if hasKey("GROQ_API_KEY", env: dotEnv) { providers.append("groq") }
            if !providers.isEmpty { return providers }
        }

        return ["openai", "anthropic", "gemini", "cerebras", "groq"]
    }

    private func credentialKey(for provider: String, env: [String: String]) -> String {
        switch provider {
        case "openai": return "OPENAI_API_KEY"
        case "anthropic": return "ANTHROPIC_API_KEY"
        case "gemini": return hasKey("GEMINI_API_KEY", env: env) ? "GEMINI_API_KEY" : "GOOGLE_API_KEY"
        case "cerebras": return "CEREBRAS_API_KEY"
        case "groq": return "GROQ_API_KEY"
        default: return ""
        }
    }

    private func integrationModel(provider: String, client: Client, env: [String: String]) -> String? {
        // Default to cheaper models to reduce accidental spend.
        let useLatest = env["OMNIAI_INTEGRATION_USE_LATEST"] == "1"
        if useLatest {
            return client.getLatestModel(provider: provider)?.id
        }

        switch provider {
        case "openai":
            return (env["OPENAI_INTEGRATION_MODEL"]?.isEmpty == false ? env["OPENAI_INTEGRATION_MODEL"] : nil) ?? "gpt-5-nano-2025-08-07"
        case "anthropic":
            return (env["ANTHROPIC_INTEGRATION_MODEL"]?.isEmpty == false ? env["ANTHROPIC_INTEGRATION_MODEL"] : nil) ?? "claude-haiku-4-5"
        case "gemini":
            return (env["GEMINI_INTEGRATION_MODEL"]?.isEmpty == false ? env["GEMINI_INTEGRATION_MODEL"] : nil) ?? "gemini-3-flash-preview"
        case "cerebras":
            return (env["CEREBRAS_INTEGRATION_MODEL"]?.isEmpty == false ? env["CEREBRAS_INTEGRATION_MODEL"] : nil) ?? "zai-glm-4.7"
        case "groq":
            return (env["GROQ_INTEGRATION_MODEL"]?.isEmpty == false ? env["GROQ_INTEGRATION_MODEL"] : nil) ?? "openai/gpt-oss-20b"
        default:
            return nil
        }
    }

    private func integrationProviderOptions(provider: String) -> [String: JSONValue]? {
        switch provider {
        case "cerebras":
            return ["cerebras": .object(["disable_reasoning": .bool(true)])]
        default:
            return nil
        }
    }

    private func anthropicParityModels(env: [String: String]) -> [String] {
        if let raw = env["ANTHROPIC_PARITY_MODELS"], !raw.isEmpty {
            let parsed = raw
                .split(separator: ",")
                .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                .filter { !$0.isEmpty }
            if !parsed.isEmpty {
                return parsed
            }
        }
        return [
            "claude-sonnet-4-6",
            "claude-sonnet-4-6 [1m]",
            "claude-opus-4-6",
            "claude-opus-4-6 [1m]",
            "claude-haiku-4-5",
        ]
    }

    @Test
    func testBasicGenerationAcrossProviders() async throws {
        guard integrationEnabled() else { return }

        let dotEnv = loadDotEnvIfPresent()
        let env = integrationEnvironment()
        let client = try Client.fromEnv(environment: env)
        for provider in integrationProviders(env: env, dotEnv: dotEnv) {
            let keyName: String = {
                switch provider {
                case "openai": return "OPENAI_API_KEY"
                case "anthropic": return "ANTHROPIC_API_KEY"
                case "gemini": return (hasKey("GEMINI_API_KEY", env: env) ? "GEMINI_API_KEY" : "GOOGLE_API_KEY")
                case "cerebras": return "CEREBRAS_API_KEY"
                case "groq": return "GROQ_API_KEY"
                default: return ""
                }
            }()
            if keyName.isEmpty || !hasKey(keyName, env: env) { continue }

            guard let model = integrationModel(provider: provider, client: client, env: env) else {
                XCTFail("No integration model resolved for provider \(provider)")
                continue
            }

            let result: GenerateResult
            do {
                result = try await generate(
                    model: model,
                    prompt: "Return exactly the word hello.",
                    maxTokens: 128,
                    reasoningEffort: provider == "openai" ? "low" : nil,
                    provider: provider,
                    providerOptions: integrationProviderOptions(provider: provider),
                    maxRetries: 1,
                    client: client
                )
            } catch let e as ProviderError {
                let status = e.statusCode.map(String.init) ?? "nil"
                XCTFail("provider=\(provider) model=\(model) ProviderError status=\(status) message=\(e.message)")
                continue
            } catch let e as SDKError {
                XCTFail("provider=\(provider) model=\(model) SDKError message=\(e.message)")
                continue
            } catch {
                XCTFail("provider=\(provider) model=\(model) error=\(String(describing: error))")
                continue
            }

            XCTAssertFalse(
                result.text.isEmpty,
                "provider=\(provider) model=\(model) finish=\(result.finishReason.rawValue)"
            )
            XCTAssertGreaterThan(result.usage.inputTokens, 0, "provider=\(provider) model=\(model)")
            XCTAssertGreaterThan(result.usage.outputTokens, 0, "provider=\(provider) model=\(model)")
            XCTAssertEqual(result.finishReason.rawValue, "stop", "provider=\(provider) model=\(model)")
        }
    }

    @Test
    func testStreamingTextMatchesAccumulatedResponse() async throws {
        guard integrationEnabled() else { return }

        let dotEnv = loadDotEnvIfPresent()
        let env = integrationEnvironment()
        let client = try Client.fromEnv(environment: env)
        for provider in integrationProviders(env: env, dotEnv: dotEnv) {
            let keyName: String = {
                switch provider {
                case "openai": return "OPENAI_API_KEY"
                case "anthropic": return "ANTHROPIC_API_KEY"
                case "gemini": return (hasKey("GEMINI_API_KEY", env: env) ? "GEMINI_API_KEY" : "GOOGLE_API_KEY")
                case "cerebras": return "CEREBRAS_API_KEY"
                case "groq": return "GROQ_API_KEY"
                default: return ""
                }
            }()
            if keyName.isEmpty || !hasKey(keyName, env: env) { continue }

            guard let model = integrationModel(provider: provider, client: client, env: env) else {
                XCTFail("No integration model resolved for provider \(provider)")
                continue
            }

            let result: StreamResult
            do {
                result = try await stream(
                    model: model,
                    prompt: "Return exactly the word hello.",
                    maxTokens: 128,
                    reasoningEffort: provider == "openai" ? "low" : nil,
                    provider: provider,
                    providerOptions: integrationProviderOptions(provider: provider),
                    maxRetries: 1,
                    client: client
                )
            } catch let e as ProviderError {
                let status = e.statusCode.map(String.init) ?? "nil"
                XCTFail("provider=\(provider) model=\(model) ProviderError status=\(status) message=\(e.message)")
                continue
            } catch let e as SDKError {
                XCTFail("provider=\(provider) model=\(model) SDKError message=\(e.message)")
                continue
            } catch {
                XCTFail("provider=\(provider) model=\(model) error=\(String(describing: error))")
                continue
            }

            var chunks: [String] = []
            for try await ev in result {
                if ev.type.rawValue == StreamEventType.textDelta.rawValue, let d = ev.delta {
                    chunks.append(d)
                }
            }

            let response = try await result.response()
            XCTAssertEqual(chunks.joined(), response.text, "provider=\(provider) model=\(model)")
        }
    }

    @Test
    func testToolCallingRoundTripSingleToolAcrossProviders() async throws {
        guard integrationEnabled() else { return }

        let dotEnv = loadDotEnvIfPresent()
        let env = integrationEnvironment()
        let client = try Client.fromEnv(environment: env)

        let schema: JSONValue = .object([
            "type": .string("object"),
            "properties": .object([
                "a": .object(["type": .string("integer")]),
                "b": .object(["type": .string("integer")]),
            ]),
            "required": .array([.string("a"), .string("b")]),
        ])

        let addTool = try Tool(name: "add", description: "Add two integers", parameters: schema) { args, _ in
            let a = Int(args["a"]?.doubleValue ?? 0)
            let b = Int(args["b"]?.doubleValue ?? 0)
            return .number(Double(a + b))
        }

        for provider in integrationProviders(env: env, dotEnv: dotEnv) {
            let keyName: String = {
                switch provider {
                case "openai": return "OPENAI_API_KEY"
                case "anthropic": return "ANTHROPIC_API_KEY"
                case "gemini": return (hasKey("GEMINI_API_KEY", env: env) ? "GEMINI_API_KEY" : "GOOGLE_API_KEY")
                case "cerebras": return "CEREBRAS_API_KEY"
                case "groq": return "GROQ_API_KEY"
                default: return ""
                }
            }()
            if keyName.isEmpty || !hasKey(keyName, env: env) { continue }

            guard let model = integrationModel(provider: provider, client: client, env: env) else {
                XCTFail("No integration model resolved for provider \(provider)")
                continue
            }

            let result: GenerateResult
            do {
                result = try await generate(
                    model: model,
                    prompt: "Use the add tool to compute 2+2. Then reply with just the number.",
                    tools: [addTool],
                    toolChoice: ToolChoice(mode: .named, toolName: "add"),
                    maxToolRounds: 3,
                    maxTokens: 128,
                    reasoningEffort: provider == "openai" ? "low" : nil,
                    provider: provider,
                    providerOptions: integrationProviderOptions(provider: provider),
                    // Live providers can return transient 5xx/429s; keep this smoke test resilient.
                    maxRetries: 1,
                    client: client
                )
            } catch let e as ProviderError {
                let status = e.statusCode.map(String.init) ?? "nil"
                XCTFail("provider=\(provider) model=\(model) ProviderError status=\(status) message=\(e.message)")
                continue
            } catch let e as SDKError {
                XCTFail("provider=\(provider) model=\(model) SDKError message=\(e.message)")
                continue
            } catch {
                XCTFail("provider=\(provider) model=\(model) error=\(String(describing: error))")
                continue
            }

            XCTAssertTrue(result.steps.count >= 2, "provider=\(provider) model=\(model)")
            XCTAssertTrue(result.text.contains("4"), "provider=\(provider) model=\(model) text=\(result.text)")
        }
    }

    @Test
    func testCrossProviderParityMatrixOpenAIAnthropicGemini() async throws {
        guard integrationEnabled() else { return }

        let env = integrationEnvironment()
        let client = try Client.fromEnv(environment: env)

        let targetProviders = ["openai", "anthropic", "gemini"]
        let enabled = targetProviders.filter { provider in
            let key = credentialKey(for: provider, env: env)
            return !key.isEmpty && hasKey(key, env: env)
        }
        if enabled.count < targetProviders.count {
            return
        }

        let schema: JSONValue = .object([
            "type": .string("object"),
            "properties": .object([
                "a": .object(["type": .string("integer")]),
                "b": .object(["type": .string("integer")]),
            ]),
            "required": .array([.string("a"), .string("b")]),
        ])
        let addTool = try Tool(name: "add", description: "Add two integers", parameters: schema) { args, _ in
            let a = Int(args["a"]?.doubleValue ?? 0)
            let b = Int(args["b"]?.doubleValue ?? 0)
            return .number(Double(a + b))
        }

        for provider in enabled {
            guard let model = integrationModel(provider: provider, client: client, env: env) else {
                XCTFail("No integration model resolved for provider \(provider)")
                continue
            }

            // Scenario 1: Basic generation
            let basic = try await generate(
                model: model,
                prompt: "Return exactly the word parity.",
                maxTokens: 64,
                reasoningEffort: provider == "openai" ? "low" : nil,
                provider: provider,
                providerOptions: integrationProviderOptions(provider: provider),
                maxRetries: 1,
                client: client
            )
            XCTAssertTrue(basic.text.lowercased().contains("parity"), "provider=\(provider) model=\(model)")

            // Scenario 2: Streaming should produce a non-empty response
            let streamResult = try await stream(
                model: model,
                prompt: "Reply with exactly stream-ok.",
                maxTokens: 64,
                reasoningEffort: provider == "openai" ? "low" : nil,
                provider: provider,
                providerOptions: integrationProviderOptions(provider: provider),
                maxRetries: 1,
                client: client
            )
            var streamText = ""
            for try await event in streamResult {
                if event.type.rawValue == StreamEventType.textDelta.rawValue {
                    streamText += event.delta ?? ""
                }
            }
            let streamResponse = try await streamResult.response()
            XCTAssertFalse(streamResponse.text.isEmpty, "provider=\(provider) model=\(model)")
            XCTAssertEqual(streamText, streamResponse.text, "provider=\(provider) model=\(model)")

            // Scenario 3: Tool-calling round trip
            let toolResult = try await generate(
                model: model,
                prompt: "Use add tool to compute 7+5 and answer with just the number.",
                tools: [addTool],
                toolChoice: ToolChoice(mode: .named, toolName: "add"),
                maxToolRounds: 3,
                maxTokens: 64,
                reasoningEffort: provider == "openai" ? "low" : nil,
                provider: provider,
                providerOptions: integrationProviderOptions(provider: provider),
                maxRetries: 1,
                client: client
            )
            let hasFinalNumericText = toolResult.text.contains("12")
            let emittedToolResults = !toolResult.toolResults.isEmpty
                || toolResult.steps.contains(where: { !$0.toolResults.isEmpty })
            XCTAssertTrue(
                hasFinalNumericText || emittedToolResults,
                "provider=\(provider) model=\(model) text=\(toolResult.text)"
            )
        }
    }

    @Test
    func testMultiTurnCacheReadAcrossOpenAIAnthropicGemini() async throws {
        guard integrationEnabled() else { return }

        let env = integrationEnvironment()
        guard env["RUN_OMNIAI_CACHE_INTEGRATION_TESTS"] == "1" else {
            return
        }

        let client = try Client.fromEnv(environment: env)
        let targetProviders = ["openai", "anthropic", "gemini"]
        let enabled = targetProviders.filter { provider in
            let key = credentialKey(for: provider, env: env)
            return !key.isEmpty && hasKey(key, env: env)
        }
        if enabled.count < targetProviders.count {
            return
        }

        let ratioThreshold = Double(env["OMNIAI_CACHE_RATIO_THRESHOLD"] ?? "") ?? 0.5
        let repeatedContext = Array(repeating: "CacheTestPrefix:alpha-beta-gamma-delta-epsilon-zeta", count: 280).joined(separator: " ")

        for provider in enabled {
            guard let model = integrationModel(provider: provider, client: client, env: env) else {
                XCTFail("No integration model resolved for provider \(provider)")
                continue
            }

            var messages: [Message] = [
                .system("You are a deterministic assistant for cache verification."),
                .user("Shared context (do not repeat fully):\n\(repeatedContext)"),
            ]

            var usageAtTurnFivePlus: Usage?
            for turn in 1...6 {
                messages.append(.user("Turn \(turn): Reply exactly with 'ack-\(turn)'."))
                let request = Request(
                    model: model,
                    messages: messages,
                    provider: provider,
                    maxTokens: 64,
                    reasoningEffort: provider == "openai" ? "low" : nil,
                    providerOptions: integrationProviderOptions(provider: provider)
                )
                let response = try await client.complete(request: request)
                messages.append(.assistant(response.text))
                if turn >= 5 {
                    usageAtTurnFivePlus = response.usage
                }
            }

            guard let usage = usageAtTurnFivePlus else {
                XCTFail("provider=\(provider) model=\(model) expected usage on turn 5+")
                continue
            }

            let cacheRead = usage.cacheReadTokens ?? 0
            XCTAssertGreaterThan(cacheRead, 0, "provider=\(provider) model=\(model) expected cache_read_tokens > 0 on turn 5+")

            let input = max(usage.inputTokens, 1)
            let ratio = Double(cacheRead) / Double(input)
            XCTAssertGreaterThanOrEqual(
                ratio,
                ratioThreshold,
                "provider=\(provider) model=\(model) cache ratio=\(ratio) threshold=\(ratioThreshold)"
            )
        }
    }

    @Test
    func testCerebrasReasoningMode() async throws {
        guard integrationEnabled() else { return }

        let env = integrationEnvironment()
        guard hasKey("CEREBRAS_API_KEY", env: env) else {
            return
        }

        let client = try Client.fromEnv(environment: env)
        guard let model = integrationModel(provider: "cerebras", client: client, env: env) else {
            XCTFail("No integration model resolved for provider cerebras")
            return
        }

        let result = try await generate(
            model: model,
            prompt: "Think through how to add 17 and 25, then answer with the final sum only.",
            maxTokens: 128,
            provider: "cerebras",
            providerOptions: ["cerebras": .object(["disable_reasoning": .bool(false)])],
            maxRetries: 1,
            client: client
        )

        // In reasoning mode, some responses may only emit reasoning text and terminate by token budget.
        XCTAssertTrue(
            (result.usage.reasoningTokens ?? 0) > 0 || !(result.reasoning?.isEmpty ?? true),
            "provider=cerebras model=\(model) expected reasoning tokens/text"
        )
        XCTAssertTrue(
            ["stop", "length", "other"].contains(result.finishReason.rawValue),
            "provider=cerebras model=\(model) finish=\(result.finishReason.rawValue)"
        )
    }

    @Test
    func testGroqReasoningMode() async throws {
        guard integrationEnabled() else { return }

        let env = integrationEnvironment()
        guard hasKey("GROQ_API_KEY", env: env) else {
            return
        }

        let client = try Client.fromEnv(environment: env)
        let model = (env["GROQ_REASONING_INTEGRATION_MODEL"]?.isEmpty == false ? env["GROQ_REASONING_INTEGRATION_MODEL"] : nil) ?? "openai/gpt-oss-20b"

        let result = try await generate(
            model: model,
            prompt: "Think through how to add 17 and 25, then answer with the final sum only.",
            maxTokens: 128,
            reasoningEffort: "low",
            provider: "groq",
            providerOptions: ["groq": .object(["include_reasoning": .bool(true)])],
            maxRetries: 1,
            client: client
        )

        XCTAssertTrue(
            (result.usage.reasoningTokens ?? 0) > 0 || !(result.reasoning?.isEmpty ?? true),
            "provider=groq model=\(model) expected reasoning tokens/text"
        )
        XCTAssertTrue(
            ["stop", "length", "other"].contains(result.finishReason.rawValue),
            "provider=groq model=\(model) finish=\(result.finishReason.rawValue)"
        )
    }

    @Test
    func testAnthropicClaudeLiveParityMatrix() async throws {
        let env = integrationEnvironment()
        guard env["RUN_ANTHROPIC_LIVE_PARITY_TESTS"] == "1" else {
            return
        }
        guard hasKey("ANTHROPIC_API_KEY", env: env) else {
            return
        }

        let client = try Client.fromEnv(environment: env)
        let models = anthropicParityModels(env: env)

        for model in models {
            let result = try await generate(
                model: model,
                prompt: "Reply with exactly CLAUDE_PARITY_OK.",
                maxTokens: 64,
                provider: "anthropic",
                maxRetries: 1,
                client: client
            )

            let trimmed = result.text.trimmingCharacters(in: .whitespacesAndNewlines)
            XCTAssertFalse(trimmed.isEmpty, "provider=anthropic model=\(model) response text is empty")
            XCTAssertTrue(
                ["stop", "length", "other"].contains(result.finishReason.rawValue),
                "provider=anthropic model=\(model) finish=\(result.finishReason.rawValue)"
            )
        }
    }
}
