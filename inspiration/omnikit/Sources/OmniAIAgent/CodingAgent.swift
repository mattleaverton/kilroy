import Foundation
import OmniAICore

/// High-level factory for creating coding agent sessions.
public struct CodingAgent {
    /// Create a session with a specific provider profile.
    public static func createSession(
        profile: ProviderProfile,
        workingDir: String? = nil,
        config: SessionConfig = SessionConfig(),
        client: Client? = nil,
        sessionID: String = UUID().uuidString,
        storageBackend: (any SessionStorageBackend)? = nil,
        autoRestoreFromStorage: Bool = true
    ) async throws -> Session {
        let env = LocalExecutionEnvironment(workingDir: workingDir)
        try await env.initialize()

        let session = try Session(
            profile: profile,
            environment: env,
            client: client,
            config: config,
            sessionID: sessionID,
            storageBackend: storageBackend,
            autoRestoreFromStorage: autoRestoreFromStorage
        )

        await session.eventEmitter.emit(SessionEvent(kind: .sessionStart, sessionId: session.id))
        return session
    }

    /// Create an OpenAI-backed session.
    public static func openai(
        model: String = "gpt-5.2",
        workingDir: String? = nil,
        config: SessionConfig = SessionConfig(),
        client: Client? = nil,
        sessionID: String = UUID().uuidString,
        storageBackend: (any SessionStorageBackend)? = nil,
        autoRestoreFromStorage: Bool = true
    ) async throws -> Session {
        let profile = OpenAIProfile(model: model)
        let session = try await createSession(
            profile: profile,
            workingDir: workingDir,
            config: config,
            client: client,
            sessionID: sessionID,
            storageBackend: storageBackend,
            autoRestoreFromStorage: autoRestoreFromStorage
        )
        // Register subagent tools now that we have the session
        let fullProfile = OpenAIProfile(model: model, session: session)
        return try Session(
            profile: fullProfile,
            environment: session.executionEnv,
            client: try (client ?? Client.fromEnv()),
            config: config,
            sessionID: sessionID,
            storageBackend: storageBackend,
            autoRestoreFromStorage: autoRestoreFromStorage
        )
    }

    /// Create an Anthropic-backed session.
    public static func anthropic(
        model: String = "claude-haiku-4-5",
        workingDir: String? = nil,
        config: SessionConfig = SessionConfig(),
        client: Client? = nil,
        sessionID: String = UUID().uuidString,
        storageBackend: (any SessionStorageBackend)? = nil,
        autoRestoreFromStorage: Bool = true
    ) async throws -> Session {
        let profile = AnthropicProfile(model: model)
        let session = try await createSession(
            profile: profile,
            workingDir: workingDir,
            config: config,
            client: client,
            sessionID: sessionID,
            storageBackend: storageBackend,
            autoRestoreFromStorage: autoRestoreFromStorage
        )
        let fullProfile = AnthropicProfile(model: model, session: session)
        return try Session(
            profile: fullProfile,
            environment: session.executionEnv,
            client: try (client ?? Client.fromEnv()),
            config: config,
            sessionID: sessionID,
            storageBackend: storageBackend,
            autoRestoreFromStorage: autoRestoreFromStorage
        )
    }

    /// Create a Gemini-backed session.
    public static func gemini(
        model: String = "gemini-3-flash-preview",
        workingDir: String? = nil,
        config: SessionConfig = SessionConfig(),
        client: Client? = nil,
        sessionID: String = UUID().uuidString,
        storageBackend: (any SessionStorageBackend)? = nil,
        autoRestoreFromStorage: Bool = true
    ) async throws -> Session {
        let profile = GeminiProfile(model: model)
        let session = try await createSession(
            profile: profile,
            workingDir: workingDir,
            config: config,
            client: client,
            sessionID: sessionID,
            storageBackend: storageBackend,
            autoRestoreFromStorage: autoRestoreFromStorage
        )
        let fullProfile = GeminiProfile(model: model, session: session)
        return try Session(
            profile: fullProfile,
            environment: session.executionEnv,
            client: try (client ?? Client.fromEnv()),
            config: config,
            sessionID: sessionID,
            storageBackend: storageBackend,
            autoRestoreFromStorage: autoRestoreFromStorage
        )
    }
}
