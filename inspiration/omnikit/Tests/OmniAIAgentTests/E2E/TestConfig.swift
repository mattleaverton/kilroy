import Foundation
import Testing
import OmniAICore
@testable import OmniAIAgent

// MARK: - API Key Configuration

/// Loads API keys from environment variables and provides helpers for E2E tests.
enum E2EConfig {

    static var openAIKey: String? { ProcessInfo.processInfo.environment["OPENAI_API_KEY"] }
    static var anthropicKey: String? { ProcessInfo.processInfo.environment["ANTHROPIC_API_KEY"] }
    static var geminiKey: String? { ProcessInfo.processInfo.environment["GEMINI_API_KEY"] }
    static var groqKey: String? { ProcessInfo.processInfo.environment["GROQ_API_KEY"] }
    static var cerebrasKey: String? { ProcessInfo.processInfo.environment["CEREBRAS_API_KEY"] }

    static var hasOpenAI: Bool { openAIKey != nil && !openAIKey!.isEmpty }
    static var hasAnthropic: Bool { anthropicKey != nil && !anthropicKey!.isEmpty }
    static var hasGemini: Bool { geminiKey != nil && !geminiKey!.isEmpty }
    static var hasGroq: Bool { groqKey != nil && !groqKey!.isEmpty }
    static var hasCerebras: Bool { cerebrasKey != nil && !cerebrasKey!.isEmpty }
    static var hasAnyProvider: Bool { hasOpenAI || hasAnthropic || hasGemini || hasGroq || hasCerebras }

    /// Create a Client from environment variables. Throws if no API keys are set.
    static func makeClient() throws -> Client {
        try Client.fromEnv()
    }

    /// Create a Client that allows empty providers (won't throw if no keys set).
    static func makeClientAllowingEmpty() throws -> Client {
        try Client.fromEnvAllowingEmpty()
    }
}

// MARK: - Skip Helpers

/// Call at the top of an E2E test to skip when the required API key is missing.
func skipUnlessAnthropic() throws {
    guard E2EConfig.hasAnthropic else {
        throw SkipE2E(provider: "Anthropic")
    }
}

func skipUnlessOpenAI() throws {
    guard E2EConfig.hasOpenAI else {
        throw SkipE2E(provider: "OpenAI")
    }
}

func skipUnlessGemini() throws {
    guard E2EConfig.hasGemini else {
        throw SkipE2E(provider: "Gemini")
    }
}

func skipUnlessGroq() throws {
    guard E2EConfig.hasGroq else {
        throw SkipE2E(provider: "Groq")
    }
}

func skipUnlessCerebras() throws {
    guard E2EConfig.hasCerebras else {
        throw SkipE2E(provider: "Cerebras")
    }
}

func skipUnlessAnyProvider() throws {
    guard E2EConfig.hasAnyProvider else {
        throw SkipE2E(provider: "any")
    }
}

private struct SkipE2E: Error, CustomStringConvertible {
    let provider: String
    var description: String { "\(provider) API key not set — skipping E2E test" }
}
