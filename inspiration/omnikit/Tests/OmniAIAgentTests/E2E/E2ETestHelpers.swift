import Foundation
import Testing
import OmniAICore
@testable import OmniAIAgent

// MARK: - Temp Directory Helper

/// Creates an isolated temporary directory for a test and cleans it up on deinit.
final class TempTestDir: @unchecked Sendable {
    let path: String
    let url: URL

    init() throws {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("omnikit-e2e-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        self.url = dir
        self.path = dir.path
    }

    deinit {
        try? FileManager.default.removeItem(at: url)
    }

    /// Write a file into the temp directory.
    func writeFile(_ relativePath: String, content: String) throws {
        let filePath = url.appendingPathComponent(relativePath)
        let parentDir = filePath.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: parentDir, withIntermediateDirectories: true)
        try content.write(to: filePath, atomically: true, encoding: .utf8)
    }

    /// Read a file from the temp directory.
    func readFile(_ relativePath: String) throws -> String {
        let filePath = url.appendingPathComponent(relativePath)
        return try String(contentsOf: filePath, encoding: .utf8)
    }

    /// Check if a file exists in the temp directory.
    func fileExists(_ relativePath: String) -> Bool {
        FileManager.default.fileExists(atPath: url.appendingPathComponent(relativePath).path)
    }
}

// MARK: - Session Factory

/// Create a Session configured for E2E testing with a real provider.
func makeE2ESession(
    provider: String,
    model: String? = nil,
    tempDir: TempTestDir,
    config: SessionConfig = SessionConfig(
        defaultCommandTimeoutMs: 30_000,
        maxCommandTimeoutMs: 120_000
    )
) throws -> Session {
    let client = try E2EConfig.makeClient()
    let env = LocalExecutionEnvironment(workingDir: tempDir.path)
    let profile = makeProfile(provider: provider, model: model)
    return try Session(
        profile: profile,
        environment: env,
        client: client,
        config: config
    )
}

/// Create a provider profile for E2E tests.
func makeProfile(provider: String, model: String? = nil) -> ProviderProfile {
    switch provider.lowercased() {
    case "anthropic":
        return AnthropicProfile(model: model ?? "claude-haiku-4-5", enableInteractiveTools: false)
    case "openai":
        return OpenAIProfile(model: model ?? "gpt-4.1-mini")
    case "gemini":
        return GeminiProfile(model: model ?? "gemini-3-flash-preview")
    case "groq":
        return GenericE2EProfile(id: "groq", model: model ?? "openai/gpt-oss-20b")
    case "cerebras":
        return GenericE2EProfile(id: "cerebras", model: model ?? "zai-glm-4.7")
    default:
        return GenericE2EProfile(id: provider, model: model ?? "gpt-4.1-mini")
    }
}

final class GenericE2EProfile: ProviderProfile, @unchecked Sendable {
    let id: String
    let model: String
    let toolRegistry = ToolRegistry()
    let supportsReasoning = true
    let supportsStreaming = true
    let supportsParallelToolCalls = true
    let contextWindowSize = 128_000

    init(id: String, model: String) {
        self.id = id
        self.model = model
    }

    func buildSystemPrompt(
        environment: ExecutionEnvironment,
        projectDocs: String?,
        userInstructions: String?,
        gitContext: GitContext?
    ) -> String {
        var sections = ["You are a concise assistant. Follow the user's instruction exactly."]
        if let userInstructions, !userInstructions.isEmpty {
            sections.append(userInstructions)
        }
        return sections.joined(separator: "\n\n")
    }

    func providerOptions() -> [String: JSONValue]? {
        if id == "cerebras" {
            return ["cerebras": .object(["disable_reasoning": .bool(true)])]
        }
        return nil
    }
}

// MARK: - Assertion Helpers

/// Wait for session to return to idle and extract the last assistant response.
func lastAssistantText(from session: Session) async -> String {
    let history = await session.getHistory()
    for turn in history.reversed() {
        if case .assistant(let t) = turn, !t.content.isEmpty {
            return t.content
        }
    }
    return ""
}

/// Count assistant turns in session history.
func assistantTurnCount(from session: Session) async -> Int {
    let history = await session.getHistory()
    return history.filter {
        if case .assistant = $0 { return true }
        return false
    }.count
}

/// Count tool call turns in session history.
func totalToolCalls(from session: Session) async -> Int {
    let history = await session.getHistory()
    var count = 0
    for turn in history {
        if case .assistant(let t) = turn {
            count += t.toolCalls.count
        }
    }
    return count
}

// MARK: - Retry Helper

/// Retry a block up to `maxAttempts` times with exponential backoff for transient API errors.
func withRetry<T>(
    maxAttempts: Int = 3,
    initialDelay: Duration = .seconds(2),
    _ block: () async throws -> T
) async throws -> T {
    var lastError: Error?
    var delay = initialDelay
    for attempt in 1...maxAttempts {
        do {
            return try await block()
        } catch {
            lastError = error
            let errorDesc = String(describing: error)
            // Only retry on rate limits or transient network errors
            let isTransient = errorDesc.contains("rate") ||
                              errorDesc.contains("429") ||
                              errorDesc.contains("500") ||
                              errorDesc.contains("502") ||
                              errorDesc.contains("503") ||
                              errorDesc.contains("timeout") ||
                              errorDesc.contains("URLError")
            guard isTransient && attempt < maxAttempts else { break }
            fputs("[E2E retry] Attempt \(attempt) failed: \(errorDesc). Retrying in \(delay)...\n", stderr)
            try await Task.sleep(for: delay)
            delay *= 2
        }
    }
    throw lastError!
}
