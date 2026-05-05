import Foundation
import OmniHTTP
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
public final class OpenAIRealtimeClient: @unchecked Sendable {
    private enum ConnectionStatus {
        case idle
        case connecting
        case connected
    }

    private struct State {
        var status: ConnectionStatus = .idle
        var websocketSession: JSONRealtimeWebSocketSession?
        var eventContinuation: AsyncThrowingStream<RealtimeServerEvent, Error>.Continuation?
        var receiverTask: Task<Void, Never>?
    }

    private let apiKey: String
    private let baseURL: URL
    private let transport: any RealtimeWebSocketTransport
    private let stateLock = NSLock()
    private var state = State()
    private let encoder: JSONEncoder
    private let decoder: JSONDecoder

    public var isConnected: Bool {
        withStateLock { $0.status == .connected }
    }

    public init(
        apiKey: String,
        baseURL: URL? = nil,
        transport: any RealtimeWebSocketTransport = defaultRealtimeWebSocketTransport()
    ) {
        self.apiKey = apiKey
        self.baseURL = baseURL ?? resolvedDefaultOpenAIRealtimeBaseURL()
        self.transport = transport
        self.encoder = JSONEncoder()
        self.decoder = JSONDecoder()
    }

    public convenience init(
        apiKey: String,
        transport: any RealtimeWebSocketTransport = defaultRealtimeWebSocketTransport()
    ) {
        self.init(apiKey: apiKey, baseURL: resolvedDefaultOpenAIRealtimeBaseURL(), transport: transport)
    }

    public func connect(model: String = "gpt-realtime") async throws -> AsyncThrowingStream<RealtimeServerEvent, Error> {
        let canConnect = withStateLock { state in
            guard state.status == .idle else { return false }
            state.status = .connecting
            return true
        }
        guard canConnect else {
            throw RealtimeError.alreadyConnected
        }

        var shouldResetState = true
        defer {
            if shouldResetState {
                withStateLock { state in
                    if state.status == .connecting {
                        state.status = .idle
                    }
                }
            }
        }

        guard var components = URLComponents(url: baseURL, resolvingAgainstBaseURL: false) else {
            throw RealtimeError.connectionFailed("Invalid realtime URL components")
        }
        var queryItems = components.queryItems ?? []
        queryItems.append(URLQueryItem(name: "model", value: model))
        components.queryItems = queryItems
        guard let url = components.url else {
            throw RealtimeError.connectionFailed("Invalid realtime URL")
        }

        var headers = OmniHTTP.HTTPHeaders()
        headers.set(name: "Authorization", value: "Bearer \(apiKey)")
        headers.set(name: "OpenAI-Beta", value: "realtime=v1")

        let jsonSession = try await transport.connectJSON(url: url, headers: headers, timeout: nil)

        let stream = AsyncThrowingStream<RealtimeServerEvent, Error> { continuation in
            self.withStateLock { state in
                state.eventContinuation = continuation
            }
            continuation.onTermination = { [weak self] _ in
                Task { await self?.disconnect() }
            }
        }

        let receiverTask = Task { [weak self] in
            guard let self else { return }
            await self.receiveMessages(session: jsonSession)
        }

        withStateLock { state in
            state.websocketSession = jsonSession
            state.receiverTask = receiverTask
            state.status = .connected
        }
        shouldResetState = false

        return stream
    }

    public func disconnect() async {
        let snapshot = withStateLock { state -> (JSONRealtimeWebSocketSession?, AsyncThrowingStream<RealtimeServerEvent, Error>.Continuation?) in
            state.receiverTask?.cancel()
            state.receiverTask = nil
            let session = state.websocketSession
            let continuation = state.eventContinuation
            state.websocketSession = nil
            state.eventContinuation = nil
            state.status = .idle
            return (session, continuation)
        }

        if let websocketSession = snapshot.0 {
            await websocketSession.close(code: .normalClosure)
        }
        snapshot.1?.finish()
    }

    public func updateSession(_ config: RealtimeSessionConfig) async throws {
        let event = RealtimeClientEvent.sessionUpdate(session: config)
        try await send(event)
    }

    public func sendUserMessage(_ text: String) async throws {
        let item = RealtimeConversationItem(type: .message, role: .user, content: [.inputText(text: text)])
        let event = RealtimeClientEvent.conversationItemCreate(item: item)
        try await send(event)
    }

    public func sendUserAudio(_ audioBase64: String) async throws {
        let item = RealtimeConversationItem(type: .message, role: .user, content: [.inputAudio(audio: audioBase64)])
        let event = RealtimeClientEvent.conversationItemCreate(item: item)
        try await send(event)
    }

    public func sendUserImage(_ imageBase64: String, format: String) async throws {
        let imageURL = "data:image/\(format);base64,\(imageBase64)"
        let item = RealtimeConversationItem(type: .message, role: .user, content: [.inputImage(imageUrl: imageURL)])
        let event = RealtimeClientEvent.conversationItemCreate(item: item)
        try await send(event)
    }

    public func createConversationItem(_ item: RealtimeConversationItem) async throws {
        let event = RealtimeClientEvent.conversationItemCreate(item: item)
        try await send(event)
    }

    public func deleteConversationItem(itemId: String) async throws {
        try await send(.conversationItemDelete(itemId: itemId))
    }

    public func truncateConversationItem(itemId: String, contentIndex: Int = 0, audioEndMs: Int) async throws {
        try await send(.conversationItemTruncate(itemId: itemId, contentIndex: contentIndex, audioEndMs: audioEndMs))
    }

    public func appendInputAudio(_ audioBase64: String) async throws {
        try await send(.inputAudioBufferAppend(audio: audioBase64))
    }

    public func commitInputAudio() async throws {
        try await send(.inputAudioBufferCommit)
    }

    public func clearInputAudio() async throws {
        try await send(.inputAudioBufferClear)
    }

    public func createResponse(_ config: RealtimeResponseConfig? = nil) async throws {
        try await send(.responseCreate(response: config))
    }

    public func cancelResponse() async throws {
        try await send(.responseCancel)
    }

    public func sendFunctionCallOutput(callId: String, output: String) async throws {
        let item = RealtimeConversationItem(type: .functionCallOutput, callId: callId, output: output)
        try await send(.conversationItemCreate(item: item))
    }

    private func send(_ event: RealtimeClientEvent) async throws {
        guard let websocketSession = withStateLock({ state -> JSONRealtimeWebSocketSession? in
            guard state.status == .connected else { return nil }
            return state.websocketSession
        }) else {
            throw RealtimeError.notConnected
        }
        let data = try encoder.encode(event)
        let payload = try JSONValue.parse(data)
        try await websocketSession.send(payload)
    }

    private func receiveMessages(session: JSONRealtimeWebSocketSession) async {
        do {
            for try await payload in session.events() {
                let continuation = withStateLock { state -> AsyncThrowingStream<RealtimeServerEvent, Error>.Continuation? in
                    guard state.status == .connected, state.websocketSession === session else { return nil }
                    return state.eventContinuation
                }
                guard let continuation else { break }

                do {
                    let data = try payload.data()
                    let event = try decoder.decode(RealtimeServerEvent.self, from: data)
                    continuation.yield(event)
                } catch {
                    if payload["type"]?.stringValue == "error" {
                        let errorEvent = RealtimeServerEvent.error(RealtimeErrorEvent(
                            type: payload["error"]?["type"]?.stringValue ?? payload["type"]?.stringValue ?? "unknown",
                            code: payload["error"]?["code"]?.stringValue,
                            message: payload["error"]?["message"]?.stringValue ?? "Unknown error",
                            param: payload["error"]?["param"]?.stringValue,
                            eventId: payload["event_id"]?.stringValue
                        ))
                        continuation.yield(errorEvent)
                    }
                }
            }
            let continuation = withStateLock { state -> AsyncThrowingStream<RealtimeServerEvent, Error>.Continuation? in
                guard state.status == .connected, state.websocketSession === session else { return nil }
                return state.eventContinuation
            }
            continuation?.finish()
        } catch {
            let continuation = withStateLock { state -> AsyncThrowingStream<RealtimeServerEvent, Error>.Continuation? in
                guard state.status == .connected, state.websocketSession === session else { return nil }
                return state.eventContinuation
            }
            continuation?.finish(throwing: error)
        }

        withStateLock { state in
            if state.websocketSession === session {
                state.websocketSession = nil
                state.receiverTask = nil
                state.eventContinuation = nil
                state.status = .idle
            }
        }
    }

    private func withStateLock<T>(_ body: (inout State) -> T) -> T {
        stateLock.lock()
        defer { stateLock.unlock() }
        return body(&state)
    }
}

private func resolvedDefaultOpenAIRealtimeBaseURL() -> URL {
    guard let url = URL(string: "wss://api.openai.com/v1/realtime") else {
        preconditionFailure("Invalid default OpenAI realtime URL")
    }
    return url
}

public enum RealtimeError: Error, Sendable {
    case notConnected
    case alreadyConnected
    case connectionFailed(String)
    case invalidResponse
    case serverError(String)
}

// MARK: - Client Event Types

/// Events sent from client to server.
public enum RealtimeClientEvent: Encodable, Sendable {
    case sessionUpdate(session: RealtimeSessionConfig)
    case conversationItemCreate(item: RealtimeConversationItem)
    case conversationItemDelete(itemId: String)
    case conversationItemTruncate(itemId: String, contentIndex: Int, audioEndMs: Int)
    case inputAudioBufferAppend(audio: String)
    case inputAudioBufferCommit
    case inputAudioBufferClear
    case responseCreate(response: RealtimeResponseConfig?)
    case responseCancel

    private enum CodingKeys: String, CodingKey {
        case type
        case eventId = "event_id"
        case session
        case item
        case itemId = "item_id"
        case contentIndex = "content_index"
        case audioEndMs = "audio_end_ms"
        case audio
        case response
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)

        switch self {
        case .sessionUpdate(let session):
            try container.encode("session.update", forKey: .type)
            try container.encode(session, forKey: .session)

        case .conversationItemCreate(let item):
            try container.encode("conversation.item.create", forKey: .type)
            try container.encode(item, forKey: .item)

        case .conversationItemDelete(let itemId):
            try container.encode("conversation.item.delete", forKey: .type)
            try container.encode(itemId, forKey: .itemId)

        case .conversationItemTruncate(let itemId, let contentIndex, let audioEndMs):
            try container.encode("conversation.item.truncate", forKey: .type)
            try container.encode(itemId, forKey: .itemId)
            try container.encode(contentIndex, forKey: .contentIndex)
            try container.encode(audioEndMs, forKey: .audioEndMs)

        case .inputAudioBufferAppend(let audio):
            try container.encode("input_audio_buffer.append", forKey: .type)
            try container.encode(audio, forKey: .audio)

        case .inputAudioBufferCommit:
            try container.encode("input_audio_buffer.commit", forKey: .type)

        case .inputAudioBufferClear:
            try container.encode("input_audio_buffer.clear", forKey: .type)

        case .responseCreate(let response):
            try container.encode("response.create", forKey: .type)
            try container.encodeIfPresent(response, forKey: .response)

        case .responseCancel:
            try container.encode("response.cancel", forKey: .type)
        }
    }
}

// MARK: - Session Configuration

/// Configuration for a Realtime session.
public struct RealtimeSessionConfig: Codable, Sendable {
    /// The session type.
    public let type: String

    /// The model to use.
    public let model: String?

    /// System instructions.
    public let instructions: String?

    /// Output modalities (e.g., ["text"], ["audio"], ["text", "audio"]).
    public let outputModalities: [String]?

    /// Audio configuration.
    public let audio: RealtimeAudioConfig?

    /// Available tools/functions.
    public let tools: [RealtimeTool]?

    /// Tool choice mode.
    public let toolChoice: String?

    /// Temperature for sampling.
    public let temperature: Double?

    /// Maximum output tokens.
    public let maxOutputTokens: Int?

    private enum CodingKeys: String, CodingKey {
        case type
        case model
        case instructions
        case outputModalities = "output_modalities"
        case audio
        case tools
        case toolChoice = "tool_choice"
        case temperature
        case maxOutputTokens = "max_output_tokens"
    }

    public init(
        type: String = "realtime",
        model: String? = nil,
        instructions: String? = nil,
        outputModalities: [String]? = nil,
        voice: RealtimeVoice? = nil,
        inputAudioFormat: RealtimeAudioFormat? = nil,
        outputAudioFormat: RealtimeAudioFormat? = nil,
        turnDetection: RealtimeTurnDetection? = nil,
        tools: [RealtimeTool]? = nil,
        toolChoice: String? = nil,
        temperature: Double? = nil,
        maxOutputTokens: Int? = nil
    ) {
        self.type = type
        self.model = model
        self.instructions = instructions
        self.outputModalities = outputModalities
        self.tools = tools
        self.toolChoice = toolChoice
        self.temperature = temperature
        self.maxOutputTokens = maxOutputTokens

        // Build audio config if any audio settings provided
        if voice != nil || inputAudioFormat != nil || outputAudioFormat != nil || turnDetection != nil {
            var inputConfig: RealtimeAudioInputConfig?
            if inputAudioFormat != nil || turnDetection != nil {
                inputConfig = RealtimeAudioInputConfig(
                    format: inputAudioFormat,
                    turnDetection: turnDetection
                )
            }

            var outputConfig: RealtimeAudioOutputConfig?
            if voice != nil || outputAudioFormat != nil {
                outputConfig = RealtimeAudioOutputConfig(
                    voice: voice?.rawValue,
                    format: outputAudioFormat
                )
            }

            self.audio = RealtimeAudioConfig(input: inputConfig, output: outputConfig)
        } else {
            self.audio = nil
        }
    }
}

/// Audio configuration for Realtime sessions.
public struct RealtimeAudioConfig: Codable, Sendable {
    public let input: RealtimeAudioInputConfig?
    public let output: RealtimeAudioOutputConfig?

    public init(input: RealtimeAudioInputConfig? = nil, output: RealtimeAudioOutputConfig? = nil) {
        self.input = input
        self.output = output
    }
}

/// Input audio configuration.
public struct RealtimeAudioInputConfig: Codable, Sendable {
    public let format: RealtimeAudioFormat?
    public let turnDetection: RealtimeTurnDetection?

    private enum CodingKeys: String, CodingKey {
        case format
        case turnDetection = "turn_detection"
    }

    public init(format: RealtimeAudioFormat? = nil, turnDetection: RealtimeTurnDetection? = nil) {
        self.format = format
        self.turnDetection = turnDetection
    }
}

/// Output audio configuration.
public struct RealtimeAudioOutputConfig: Codable, Sendable {
    public let voice: String?
    public let format: RealtimeAudioFormat?

    public init(voice: String? = nil, format: RealtimeAudioFormat? = nil) {
        self.voice = voice
        self.format = format
    }
}

/// Audio format configuration.
public struct RealtimeAudioFormat: Codable, Sendable {
    public let type: String
    public let rate: Int?

    public init(type: String, rate: Int? = nil) {
        self.type = type
        self.rate = rate
    }

    /// PCM 16-bit audio at 24kHz.
    public static let pcm24k = RealtimeAudioFormat(type: "audio/pcm", rate: 24000)

    /// PCM 16-bit audio at 16kHz.
    public static let pcm16k = RealtimeAudioFormat(type: "audio/pcm", rate: 16000)

    /// G.711 mu-law.
    public static let pcmu = RealtimeAudioFormat(type: "audio/pcmu")

    /// G.711 A-law.
    public static let pcma = RealtimeAudioFormat(type: "audio/pcma")
}

/// Turn detection configuration.
public struct RealtimeTurnDetection: Codable, Sendable {
    public let type: String
    public let threshold: Double?
    public let prefixPaddingMs: Int?
    public let silenceDurationMs: Int?
    public let interruptResponse: Bool?
    public let createResponse: Bool?

    private enum CodingKeys: String, CodingKey {
        case type
        case threshold
        case prefixPaddingMs = "prefix_padding_ms"
        case silenceDurationMs = "silence_duration_ms"
        case interruptResponse = "interrupt_response"
        case createResponse = "create_response"
    }

    public init(
        type: String,
        threshold: Double? = nil,
        prefixPaddingMs: Int? = nil,
        silenceDurationMs: Int? = nil,
        interruptResponse: Bool? = nil,
        createResponse: Bool? = nil
    ) {
        self.type = type
        self.threshold = threshold
        self.prefixPaddingMs = prefixPaddingMs
        self.silenceDurationMs = silenceDurationMs
        self.interruptResponse = interruptResponse
        self.createResponse = createResponse
    }

    /// Semantic VAD (voice activity detection).
    public static let semanticVad = RealtimeTurnDetection(type: "semantic_vad")

    /// Server VAD with default settings.
    public static let serverVad = RealtimeTurnDetection(type: "server_vad")

    /// Disable VAD (push-to-talk mode).
    public static let disabled: RealtimeTurnDetection? = nil
}

/// Available voices for Realtime output.
public enum RealtimeVoice: String, Codable, Sendable {
    case alloy
    case ash
    case ballad
    case coral
    case echo
    case sage
    case shimmer
    case verse
    case marin
    case cedar
}

// MARK: - Tool Types

/// A tool/function available in Realtime sessions.
public struct RealtimeTool: Codable, Sendable {
    public let type: String
    public let name: String
    public let description: String?
    public let parameters: RealtimeToolParameters?

    public init(
        type: String = "function",
        name: String,
        description: String? = nil,
        parameters: RealtimeToolParameters? = nil
    ) {
        self.type = type
        self.name = name
        self.description = description
        self.parameters = parameters
    }
}

/// Parameters schema for a Realtime tool.
public struct RealtimeToolParameters: Codable, Sendable {
    public let type: String
    public let properties: [String: RealtimeToolProperty]?
    public let required: [String]?

    public init(
        type: String = "object",
        properties: [String: RealtimeToolProperty]? = nil,
        required: [String]? = nil
    ) {
        self.type = type
        self.properties = properties
        self.required = required
    }
}

/// A property in a tool's parameter schema.
public struct RealtimeToolProperty: Codable, Sendable {
    public let type: String
    public let description: String?
    public let enumValues: [String]?

    private enum CodingKeys: String, CodingKey {
        case type
        case description
        case enumValues = "enum"
    }

    public init(type: String, description: String? = nil, enumValues: [String]? = nil) {
        self.type = type
        self.description = description
        self.enumValues = enumValues
    }
}

// MARK: - Conversation Item Types

/// A conversation item in a Realtime session.
public struct RealtimeConversationItem: Codable, Sendable {
    public let id: String?
    public let type: RealtimeItemType
    public let role: RealtimeRole?
    public let content: [RealtimeContentPart]?
    public let callId: String?
    public let name: String?
    public let arguments: String?
    public let output: String?
    public let status: String?

    private enum CodingKeys: String, CodingKey {
        case id
        case type
        case role
        case content
        case callId = "call_id"
        case name
        case arguments
        case output
        case status
    }

    public init(
        id: String? = nil,
        type: RealtimeItemType,
        role: RealtimeRole? = nil,
        content: [RealtimeContentPart]? = nil,
        callId: String? = nil,
        name: String? = nil,
        arguments: String? = nil,
        output: String? = nil,
        status: String? = nil
    ) {
        self.id = id
        self.type = type
        self.role = role
        self.content = content
        self.callId = callId
        self.name = name
        self.arguments = arguments
        self.output = output
        self.status = status
    }
}

/// Types of conversation items.
public enum RealtimeItemType: String, Codable, Sendable {
    case message
    case functionCall = "function_call"
    case functionCallOutput = "function_call_output"
}

/// Roles in a conversation.
public enum RealtimeRole: String, Codable, Sendable {
    case user
    case assistant
    case system
}

/// Content parts in a conversation item.
public enum RealtimeContentPart: Codable, Sendable {
    case inputText(text: String)
    case inputAudio(audio: String)
    case inputImage(imageUrl: String)
    case outputText(text: String)
    case outputAudio(audio: String?, transcript: String?)

    private enum CodingKeys: String, CodingKey {
        case type
        case text
        case audio
        case imageUrl = "image_url"
        case transcript
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let type = try container.decode(String.self, forKey: .type)

        switch type {
        case "input_text":
            let text = try container.decode(String.self, forKey: .text)
            self = .inputText(text: text)
        case "input_audio":
            let audio = try container.decode(String.self, forKey: .audio)
            self = .inputAudio(audio: audio)
        case "input_image":
            let imageUrl = try container.decode(String.self, forKey: .imageUrl)
            self = .inputImage(imageUrl: imageUrl)
        case "output_text", "text":
            let text = try container.decode(String.self, forKey: .text)
            self = .outputText(text: text)
        case "output_audio", "audio":
            let audio = try container.decodeIfPresent(String.self, forKey: .audio)
            let transcript = try container.decodeIfPresent(String.self, forKey: .transcript)
            self = .outputAudio(audio: audio, transcript: transcript)
        default:
            throw DecodingError.dataCorruptedError(
                forKey: .type,
                in: container,
                debugDescription: "Unknown content type: \(type)"
            )
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)

        switch self {
        case .inputText(let text):
            try container.encode("input_text", forKey: .type)
            try container.encode(text, forKey: .text)
        case .inputAudio(let audio):
            try container.encode("input_audio", forKey: .type)
            try container.encode(audio, forKey: .audio)
        case .inputImage(let imageUrl):
            try container.encode("input_image", forKey: .type)
            try container.encode(imageUrl, forKey: .imageUrl)
        case .outputText(let text):
            try container.encode("output_text", forKey: .type)
            try container.encode(text, forKey: .text)
        case .outputAudio(let audio, let transcript):
            try container.encode("output_audio", forKey: .type)
            try container.encodeIfPresent(audio, forKey: .audio)
            try container.encodeIfPresent(transcript, forKey: .transcript)
        }
    }
}

// MARK: - Response Configuration

/// Configuration for creating a response.
public struct RealtimeResponseConfig: Codable, Sendable {
    /// Output modalities for this response.
    public let outputModalities: [String]?

    /// Instructions for this response.
    public let instructions: String?

    /// Conversation mode ("none" for out-of-band responses).
    public let conversation: String?

    /// Metadata for identifying the response.
    public let metadata: [String: String]?

    /// Custom input items for the response.
    public let input: [RealtimeConversationItem]?

    /// Tools available for this response.
    public let tools: [RealtimeTool]?

    /// Tool choice mode.
    public let toolChoice: String?

    /// Temperature for sampling.
    public let temperature: Double?

    /// Maximum output tokens.
    public let maxOutputTokens: Int?

    private enum CodingKeys: String, CodingKey {
        case outputModalities = "output_modalities"
        case instructions
        case conversation
        case metadata
        case input
        case tools
        case toolChoice = "tool_choice"
        case temperature
        case maxOutputTokens = "max_output_tokens"
    }

    public init(
        outputModalities: [String]? = nil,
        instructions: String? = nil,
        conversation: String? = nil,
        metadata: [String: String]? = nil,
        input: [RealtimeConversationItem]? = nil,
        tools: [RealtimeTool]? = nil,
        toolChoice: String? = nil,
        temperature: Double? = nil,
        maxOutputTokens: Int? = nil
    ) {
        self.outputModalities = outputModalities
        self.instructions = instructions
        self.conversation = conversation
        self.metadata = metadata
        self.input = input
        self.tools = tools
        self.toolChoice = toolChoice
        self.temperature = temperature
        self.maxOutputTokens = maxOutputTokens
    }

    /// Create a text-only response configuration.
    public static func textOnly(instructions: String? = nil) -> RealtimeResponseConfig {
        RealtimeResponseConfig(outputModalities: ["text"], instructions: instructions)
    }

    /// Create an audio-only response configuration.
    public static func audioOnly(instructions: String? = nil) -> RealtimeResponseConfig {
        RealtimeResponseConfig(outputModalities: ["audio"], instructions: instructions)
    }

    /// Create an out-of-band response configuration.
    public static func outOfBand(
        instructions: String? = nil,
        metadata: [String: String]? = nil
    ) -> RealtimeResponseConfig {
        RealtimeResponseConfig(
            outputModalities: ["text"],
            instructions: instructions,
            conversation: "none",
            metadata: metadata
        )
    }
}

// MARK: - Server Event Types

/// Events received from the Realtime server.
public enum RealtimeServerEvent: Decodable, Sendable {
    // Session events
    case sessionCreated(RealtimeSession)
    case sessionUpdated(RealtimeSession)

    // Conversation events
    case conversationItemAdded(RealtimeItemEvent)
    case conversationItemDone(RealtimeItemEvent)
    case conversationItemDeleted(itemId: String)
    case conversationItemTruncated(RealtimeTruncateEvent)

    // Input audio buffer events
    case inputAudioBufferSpeechStarted(RealtimeSpeechEvent)
    case inputAudioBufferSpeechStopped(RealtimeSpeechEvent)
    case inputAudioBufferCommitted(RealtimeAudioBufferEvent)
    case inputAudioBufferCleared

    // Response events
    case responseCreated(RealtimeResponseEvent)
    case responseDone(RealtimeResponseEvent)
    case responseCancelled(RealtimeResponseEvent)
    case responseOutputItemAdded(RealtimeOutputItemEvent)
    case responseOutputItemDone(RealtimeOutputItemEvent)
    case responseContentPartAdded(RealtimeContentPartEvent)
    case responseContentPartDone(RealtimeContentPartEvent)
    case responseTextDelta(RealtimeTextDeltaEvent)
    case responseTextDone(RealtimeTextDoneEvent)
    case responseAudioDelta(RealtimeAudioDeltaEvent)
    case responseAudioDone(RealtimeAudioDoneEvent)
    case responseAudioTranscriptDelta(RealtimeTranscriptDeltaEvent)
    case responseAudioTranscriptDone(RealtimeTranscriptDoneEvent)
    case responseFunctionCallArgumentsDelta(RealtimeFunctionCallEvent)
    case responseFunctionCallArgumentsDone(RealtimeFunctionCallEvent)

    // Rate limits
    case rateLimitsUpdated(RealtimeRateLimitsEvent)

    // Errors
    case error(RealtimeErrorEvent)

    // Unknown event
    case unknown(type: String)

    private enum CodingKeys: String, CodingKey {
        case type
        case eventId = "event_id"
        case session
        case item
        case itemId = "item_id"
        case response
        case outputIndex = "output_index"
        case contentIndex = "content_index"
        case part
        case delta
        case text
        case audio
        case transcript
        case name
        case callId = "call_id"
        case arguments
        case audioStartMs = "audio_start_ms"
        case audioEndMs = "audio_end_ms"
        case rateLimits = "rate_limits"
        case error
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let type = try container.decode(String.self, forKey: .type)

        switch type {
        case "session.created":
            let session = try container.decode(RealtimeSession.self, forKey: .session)
            self = .sessionCreated(session)

        case "session.updated":
            let session = try container.decode(RealtimeSession.self, forKey: .session)
            self = .sessionUpdated(session)

        case "conversation.item.added", "conversation.item.created":
            let item = try container.decode(RealtimeConversationItem.self, forKey: .item)
            let eventId = try container.decodeIfPresent(String.self, forKey: .eventId)
            self = .conversationItemAdded(RealtimeItemEvent(eventId: eventId, item: item))

        case "conversation.item.done":
            let item = try container.decode(RealtimeConversationItem.self, forKey: .item)
            let eventId = try container.decodeIfPresent(String.self, forKey: .eventId)
            self = .conversationItemDone(RealtimeItemEvent(eventId: eventId, item: item))

        case "conversation.item.deleted":
            let itemId = try container.decode(String.self, forKey: .itemId)
            self = .conversationItemDeleted(itemId: itemId)

        case "conversation.item.truncated":
            let itemId = try container.decode(String.self, forKey: .itemId)
            let contentIndex = try container.decode(Int.self, forKey: .contentIndex)
            let audioEndMs = try container.decode(Int.self, forKey: .audioEndMs)
            self = .conversationItemTruncated(RealtimeTruncateEvent(
                itemId: itemId, contentIndex: contentIndex, audioEndMs: audioEndMs
            ))

        case "input_audio_buffer.speech_started":
            let audioStartMs = try container.decodeIfPresent(Int.self, forKey: .audioStartMs)
            let itemId = try container.decodeIfPresent(String.self, forKey: .itemId)
            self = .inputAudioBufferSpeechStarted(RealtimeSpeechEvent(
                audioStartMs: audioStartMs, audioEndMs: nil, itemId: itemId
            ))

        case "input_audio_buffer.speech_stopped":
            let audioEndMs = try container.decodeIfPresent(Int.self, forKey: .audioEndMs)
            let itemId = try container.decodeIfPresent(String.self, forKey: .itemId)
            self = .inputAudioBufferSpeechStopped(RealtimeSpeechEvent(
                audioStartMs: nil, audioEndMs: audioEndMs, itemId: itemId
            ))

        case "input_audio_buffer.committed":
            let itemId = try container.decodeIfPresent(String.self, forKey: .itemId)
            self = .inputAudioBufferCommitted(RealtimeAudioBufferEvent(itemId: itemId))

        case "input_audio_buffer.cleared":
            self = .inputAudioBufferCleared

        case "response.created":
            let response = try container.decode(RealtimeResponse.self, forKey: .response)
            self = .responseCreated(RealtimeResponseEvent(response: response))

        case "response.done":
            let response = try container.decode(RealtimeResponse.self, forKey: .response)
            self = .responseDone(RealtimeResponseEvent(response: response))

        case "response.cancelled":
            let response = try container.decode(RealtimeResponse.self, forKey: .response)
            self = .responseCancelled(RealtimeResponseEvent(response: response))

        case "response.output_item.added":
            let item = try container.decode(RealtimeConversationItem.self, forKey: .item)
            let outputIndex = try container.decodeIfPresent(Int.self, forKey: .outputIndex)
            self = .responseOutputItemAdded(RealtimeOutputItemEvent(item: item, outputIndex: outputIndex))

        case "response.output_item.done":
            let item = try container.decode(RealtimeConversationItem.self, forKey: .item)
            let outputIndex = try container.decodeIfPresent(Int.self, forKey: .outputIndex)
            self = .responseOutputItemDone(RealtimeOutputItemEvent(item: item, outputIndex: outputIndex))

        case "response.content_part.added":
            let part = try container.decode(RealtimeContentPart.self, forKey: .part)
            let contentIndex = try container.decodeIfPresent(Int.self, forKey: .contentIndex)
            self = .responseContentPartAdded(RealtimeContentPartEvent(part: part, contentIndex: contentIndex))

        case "response.content_part.done":
            let part = try container.decode(RealtimeContentPart.self, forKey: .part)
            let contentIndex = try container.decodeIfPresent(Int.self, forKey: .contentIndex)
            self = .responseContentPartDone(RealtimeContentPartEvent(part: part, contentIndex: contentIndex))

        case "response.output_text.delta", "response.text.delta":
            let delta = try container.decode(String.self, forKey: .delta)
            let contentIndex = try container.decodeIfPresent(Int.self, forKey: .contentIndex)
            let outputIndex = try container.decodeIfPresent(Int.self, forKey: .outputIndex)
            self = .responseTextDelta(RealtimeTextDeltaEvent(
                delta: delta, contentIndex: contentIndex, outputIndex: outputIndex
            ))

        case "response.output_text.done", "response.text.done":
            let text = try container.decode(String.self, forKey: .text)
            let contentIndex = try container.decodeIfPresent(Int.self, forKey: .contentIndex)
            let outputIndex = try container.decodeIfPresent(Int.self, forKey: .outputIndex)
            self = .responseTextDone(RealtimeTextDoneEvent(
                text: text, contentIndex: contentIndex, outputIndex: outputIndex
            ))

        case "response.output_audio.delta", "response.audio.delta":
            let delta = try container.decode(String.self, forKey: .delta)
            let contentIndex = try container.decodeIfPresent(Int.self, forKey: .contentIndex)
            let outputIndex = try container.decodeIfPresent(Int.self, forKey: .outputIndex)
            self = .responseAudioDelta(RealtimeAudioDeltaEvent(
                delta: delta, contentIndex: contentIndex, outputIndex: outputIndex
            ))

        case "response.output_audio.done", "response.audio.done":
            let contentIndex = try container.decodeIfPresent(Int.self, forKey: .contentIndex)
            let outputIndex = try container.decodeIfPresent(Int.self, forKey: .outputIndex)
            self = .responseAudioDone(RealtimeAudioDoneEvent(
                contentIndex: contentIndex, outputIndex: outputIndex
            ))

        case "response.output_audio_transcript.delta", "response.audio_transcript.delta":
            let delta = try container.decode(String.self, forKey: .delta)
            let contentIndex = try container.decodeIfPresent(Int.self, forKey: .contentIndex)
            let outputIndex = try container.decodeIfPresent(Int.self, forKey: .outputIndex)
            self = .responseAudioTranscriptDelta(RealtimeTranscriptDeltaEvent(
                delta: delta, contentIndex: contentIndex, outputIndex: outputIndex
            ))

        case "response.output_audio_transcript.done", "response.audio_transcript.done":
            let transcript = try container.decode(String.self, forKey: .transcript)
            let contentIndex = try container.decodeIfPresent(Int.self, forKey: .contentIndex)
            let outputIndex = try container.decodeIfPresent(Int.self, forKey: .outputIndex)
            self = .responseAudioTranscriptDone(RealtimeTranscriptDoneEvent(
                transcript: transcript, contentIndex: contentIndex, outputIndex: outputIndex
            ))

        case "response.function_call_arguments.delta":
            let delta = try container.decode(String.self, forKey: .delta)
            let callId = try container.decodeIfPresent(String.self, forKey: .callId)
            let name = try container.decodeIfPresent(String.self, forKey: .name)
            self = .responseFunctionCallArgumentsDelta(RealtimeFunctionCallEvent(
                callId: callId, name: name, arguments: nil, delta: delta
            ))

        case "response.function_call_arguments.done":
            let arguments = try container.decode(String.self, forKey: .arguments)
            let callId = try container.decodeIfPresent(String.self, forKey: .callId)
            let name = try container.decodeIfPresent(String.self, forKey: .name)
            self = .responseFunctionCallArgumentsDone(RealtimeFunctionCallEvent(
                callId: callId, name: name, arguments: arguments, delta: nil
            ))

        case "rate_limits.updated":
            let rateLimits = try container.decode([RealtimeRateLimit].self, forKey: .rateLimits)
            self = .rateLimitsUpdated(RealtimeRateLimitsEvent(rateLimits: rateLimits))

        case "error":
            let error = try container.decode(RealtimeErrorInfo.self, forKey: .error)
            let eventId = try container.decodeIfPresent(String.self, forKey: .eventId)
            self = .error(RealtimeErrorEvent(
                type: error.type,
                code: error.code,
                message: error.message ?? "Unknown error",
                param: error.param,
                eventId: eventId
            ))

        default:
            self = .unknown(type: type)
        }
    }
}

// MARK: - Server Event Payloads

/// A Realtime session.
public struct RealtimeSession: Codable, Sendable {
    public let id: String
    public let object: String?
    public let model: String?
    public let instructions: String?
    public let voice: String?
    public let outputModalities: [String]?
    public let tools: [RealtimeTool]?
    public let toolChoice: String?
    public let temperature: Double?
    public let maxOutputTokens: Int?

    private enum CodingKeys: String, CodingKey {
        case id
        case object
        case model
        case instructions
        case voice
        case outputModalities = "output_modalities"
        case tools
        case toolChoice = "tool_choice"
        case temperature
        case maxOutputTokens = "max_output_tokens"
    }
}

/// A Realtime response.
public struct RealtimeResponse: Codable, Sendable {
    public let id: String
    public let object: String?
    public let status: String?
    public let statusDetails: RealtimeStatusDetails?
    public let output: [RealtimeConversationItem]?
    public let metadata: [String: String]?
    public let usage: RealtimeUsage?

    private enum CodingKeys: String, CodingKey {
        case id
        case object
        case status
        case statusDetails = "status_details"
        case output
        case metadata
        case usage
    }
}

/// Status details for a response.
public struct RealtimeStatusDetails: Codable, Sendable {
    public let type: String?
    public let reason: String?
    public let error: RealtimeErrorInfo?
}

/// Error information.
public struct RealtimeErrorInfo: Codable, Sendable {
    public let type: String?
    public let code: String?
    public let message: String?
    public let param: String?
}

/// Usage information for a response.
public struct RealtimeUsage: Codable, Sendable {
    public let totalTokens: Int?
    public let inputTokens: Int?
    public let outputTokens: Int?
    public let inputTokenDetails: RealtimeTokenDetails?
    public let outputTokenDetails: RealtimeTokenDetails?

    private enum CodingKeys: String, CodingKey {
        case totalTokens = "total_tokens"
        case inputTokens = "input_tokens"
        case outputTokens = "output_tokens"
        case inputTokenDetails = "input_token_details"
        case outputTokenDetails = "output_token_details"
    }
}

/// Token details.
public struct RealtimeTokenDetails: Codable, Sendable {
    public let textTokens: Int?
    public let audioTokens: Int?
    public let cachedTokens: Int?

    private enum CodingKeys: String, CodingKey {
        case textTokens = "text_tokens"
        case audioTokens = "audio_tokens"
        case cachedTokens = "cached_tokens"
    }
}

/// Rate limit information.
public struct RealtimeRateLimit: Codable, Sendable {
    public let name: String
    public let limit: Int
    public let remaining: Int
    public let resetSeconds: Double

    private enum CodingKeys: String, CodingKey {
        case name
        case limit
        case remaining
        case resetSeconds = "reset_seconds"
    }
}

// MARK: - Event Payloads

/// Item event payload.
public struct RealtimeItemEvent: Sendable {
    public let eventId: String?
    public let item: RealtimeConversationItem
}

/// Truncate event payload.
public struct RealtimeTruncateEvent: Sendable {
    public let itemId: String
    public let contentIndex: Int
    public let audioEndMs: Int
}

/// Speech event payload.
public struct RealtimeSpeechEvent: Sendable {
    public let audioStartMs: Int?
    public let audioEndMs: Int?
    public let itemId: String?
}

/// Audio buffer event payload.
public struct RealtimeAudioBufferEvent: Sendable {
    public let itemId: String?
}

/// Response event payload.
public struct RealtimeResponseEvent: Sendable {
    public let response: RealtimeResponse
}

/// Output item event payload.
public struct RealtimeOutputItemEvent: Sendable {
    public let item: RealtimeConversationItem
    public let outputIndex: Int?
}

/// Content part event payload.
public struct RealtimeContentPartEvent: Sendable {
    public let part: RealtimeContentPart
    public let contentIndex: Int?
}

/// Text delta event payload.
public struct RealtimeTextDeltaEvent: Sendable {
    public let delta: String
    public let contentIndex: Int?
    public let outputIndex: Int?
}

/// Text done event payload.
public struct RealtimeTextDoneEvent: Sendable {
    public let text: String
    public let contentIndex: Int?
    public let outputIndex: Int?
}

/// Audio delta event payload.
public struct RealtimeAudioDeltaEvent: Sendable {
    public let delta: String
    public let contentIndex: Int?
    public let outputIndex: Int?
}

/// Audio done event payload.
public struct RealtimeAudioDoneEvent: Sendable {
    public let contentIndex: Int?
    public let outputIndex: Int?
}

/// Transcript delta event payload.
public struct RealtimeTranscriptDeltaEvent: Sendable {
    public let delta: String
    public let contentIndex: Int?
    public let outputIndex: Int?
}

/// Transcript done event payload.
public struct RealtimeTranscriptDoneEvent: Sendable {
    public let transcript: String
    public let contentIndex: Int?
    public let outputIndex: Int?
}

/// Function call event payload.
public struct RealtimeFunctionCallEvent: Sendable {
    public let callId: String?
    public let name: String?
    public let arguments: String?
    public let delta: String?
}

/// Rate limits event payload.
public struct RealtimeRateLimitsEvent: Sendable {
    public let rateLimits: [RealtimeRateLimit]
}

/// Error event payload.
public struct RealtimeErrorEvent: Sendable {
    public let type: String?
    public let code: String?
    public let message: String
    public let param: String?
    public let eventId: String?
}
