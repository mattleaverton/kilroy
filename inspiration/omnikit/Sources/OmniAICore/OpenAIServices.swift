import Foundation

public struct OpenAIServiceNamespace: Sendable {
    private let client: Client
    private let providerName: String

    public init(client: Client, providerName: String = "openai") {
        self.client = client
        self.providerName = providerName
    }

    public var audio: OpenAIAudioService {
        OpenAIAudioService(client: client, providerName: providerName)
    }

    public var images: OpenAIImagesService {
        OpenAIImagesService(client: client, providerName: providerName)
    }

    public var moderations: OpenAIModerationsService {
        OpenAIModerationsService(client: client, providerName: providerName)
    }

    public var batches: OpenAIBatchesService {
        OpenAIBatchesService(client: client, providerName: providerName)
    }

    @available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
    public var realtime: OpenAIRealtimeClient {
        get throws {
            let adapter = try client.resolveAdapter(provider: providerName)
            guard let realtimeProvider = adapter as? RealtimeProviderAdapter else {
                throw UnsupportedCapabilityError(provider: providerName, capability: "realtime")
            }
            return try realtimeProvider.makeRealtimeClient()
        }
    }
}

public struct OpenAIAudioService: Sendable {
    private let client: Client
    private let providerName: String

    init(client: Client, providerName: String) {
        self.client = client
        self.providerName = providerName
    }

    public func speech(_ request: OpenAISpeechRequest) async throws -> OpenAISpeechResponse {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let audioProvider = adapter as? OpenAIAudioProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "openai.audio")
        }
        return try await audioProvider.createSpeech(request: request)
    }

    public func transcribe(_ request: OpenAITranscriptionRequest) async throws -> OpenAITranscriptionResponse {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let audioProvider = adapter as? OpenAIAudioProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "openai.audio")
        }
        return try await audioProvider.createTranscription(request: request)
    }

    public func translate(_ request: OpenAITranslationRequest) async throws -> OpenAITranslationResponse {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let audioProvider = adapter as? OpenAIAudioProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "openai.audio")
        }
        return try await audioProvider.createTranslation(request: request)
    }
}

public struct OpenAIImagesService: Sendable {
    private let client: Client
    private let providerName: String

    init(client: Client, providerName: String) {
        self.client = client
        self.providerName = providerName
    }

    public func generate(_ request: OpenAIImageGenerationRequest) async throws -> OpenAIImageGenerationResponse {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let imagesProvider = adapter as? OpenAIImagesProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "openai.images")
        }
        return try await imagesProvider.generateImages(request: request)
    }
}

public struct OpenAIModerationsService: Sendable {
    private let client: Client
    private let providerName: String

    init(client: Client, providerName: String) {
        self.client = client
        self.providerName = providerName
    }

    public func create(_ request: OpenAIModerationRequest) async throws -> OpenAIModerationResponse {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let moderationProvider = adapter as? OpenAIModerationsProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "openai.moderations")
        }
        return try await moderationProvider.createModeration(request: request)
    }
}

public struct OpenAIBatchesService: Sendable {
    private let client: Client
    private let providerName: String

    init(client: Client, providerName: String) {
        self.client = client
        self.providerName = providerName
    }

    public func create(_ request: OpenAIBatchRequest) async throws -> OpenAIBatchResponse {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let batchProvider = adapter as? OpenAIBatchesProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "openai.batches")
        }
        return try await batchProvider.createBatch(request: request)
    }

    public func retrieve(_ batchId: String) async throws -> OpenAIBatchResponse {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let batchProvider = adapter as? OpenAIBatchesProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "openai.batches")
        }
        return try await batchProvider.retrieveBatch(id: batchId)
    }

    public func cancel(_ batchId: String) async throws -> OpenAIBatchResponse {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let batchProvider = adapter as? OpenAIBatchesProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "openai.batches")
        }
        return try await batchProvider.cancelBatch(id: batchId)
    }
}

public enum OpenAISpeechResponseFormat: String, Sendable, Codable, Equatable {
    case mp3
    case opus
    case aac
    case flac
    case wav
    case pcm
}

public enum OpenAIAudioTextResponseFormat: String, Sendable, Codable, Equatable {
    case json
    case text
    case srt
    case verboseJSON = "verbose_json"
    case vtt
}

public enum OpenAIImageResponseFormat: String, Sendable, Codable, Equatable {
    case url
    case b64JSON = "b64_json"
}

public struct OpenAISpeechRequest: Sendable {
    public var model: String
    public var input: String
    public var voice: String
    public var responseFormat: OpenAISpeechResponseFormat?
    public var speed: Double?
    public var providerOptions: [String: JSONValue]?
    public var timeout: Timeout?

    public init(
        model: String,
        input: String,
        voice: String,
        responseFormat: OpenAISpeechResponseFormat? = nil,
        speed: Double? = nil,
        providerOptions: [String: JSONValue]? = nil,
        timeout: Timeout? = nil
    ) {
        self.model = model
        self.input = input
        self.voice = voice
        self.responseFormat = responseFormat
        self.speed = speed
        self.providerOptions = providerOptions
        self.timeout = timeout
    }
}

public struct OpenAISpeechResponse: Sendable, Equatable {
    public var audio: AudioData

    public init(audio: AudioData) {
        self.audio = audio
    }
}

public struct OpenAITranscriptionRequest: Sendable {
    public var model: String
    public var fileName: String
    public var fileData: [UInt8]
    public var mediaType: String
    public var prompt: String?
    public var responseFormat: OpenAIAudioTextResponseFormat?
    public var temperature: Double?
    public var language: String?
    public var providerOptions: [String: JSONValue]?
    public var timeout: Timeout?

    public init(
        model: String,
        fileName: String,
        fileData: [UInt8],
        mediaType: String,
        prompt: String? = nil,
        responseFormat: OpenAIAudioTextResponseFormat? = nil,
        temperature: Double? = nil,
        language: String? = nil,
        providerOptions: [String: JSONValue]? = nil,
        timeout: Timeout? = nil
    ) {
        self.model = model
        self.fileName = fileName
        self.fileData = fileData
        self.mediaType = mediaType
        self.prompt = prompt
        self.responseFormat = responseFormat
        self.temperature = temperature
        self.language = language
        self.providerOptions = providerOptions
        self.timeout = timeout
    }
}

public struct OpenAITranscriptionResponse: Sendable, Equatable {
    public var text: String
    public var raw: JSONValue?

    public init(text: String, raw: JSONValue? = nil) {
        self.text = text
        self.raw = raw
    }
}

public struct OpenAITranslationRequest: Sendable {
    public var model: String
    public var fileName: String
    public var fileData: [UInt8]
    public var mediaType: String
    public var prompt: String?
    public var responseFormat: OpenAIAudioTextResponseFormat?
    public var temperature: Double?
    public var providerOptions: [String: JSONValue]?
    public var timeout: Timeout?

    public init(
        model: String,
        fileName: String,
        fileData: [UInt8],
        mediaType: String,
        prompt: String? = nil,
        responseFormat: OpenAIAudioTextResponseFormat? = nil,
        temperature: Double? = nil,
        providerOptions: [String: JSONValue]? = nil,
        timeout: Timeout? = nil
    ) {
        self.model = model
        self.fileName = fileName
        self.fileData = fileData
        self.mediaType = mediaType
        self.prompt = prompt
        self.responseFormat = responseFormat
        self.temperature = temperature
        self.providerOptions = providerOptions
        self.timeout = timeout
    }
}

public struct OpenAITranslationResponse: Sendable, Equatable {
    public var text: String
    public var raw: JSONValue?

    public init(text: String, raw: JSONValue? = nil) {
        self.text = text
        self.raw = raw
    }
}

public struct OpenAIImageGenerationRequest: Sendable {
    public var prompt: String
    public var model: String?
    public var size: String?
    public var quality: String?
    public var responseFormat: OpenAIImageResponseFormat?
    public var numberOfImages: Int?
    public var user: String?
    public var providerOptions: [String: JSONValue]?
    public var timeout: Timeout?

    public init(
        prompt: String,
        model: String? = nil,
        size: String? = nil,
        quality: String? = nil,
        responseFormat: OpenAIImageResponseFormat? = nil,
        numberOfImages: Int? = nil,
        user: String? = nil,
        providerOptions: [String: JSONValue]? = nil,
        timeout: Timeout? = nil
    ) {
        self.prompt = prompt
        self.model = model
        self.size = size
        self.quality = quality
        self.responseFormat = responseFormat
        self.numberOfImages = numberOfImages
        self.user = user
        self.providerOptions = providerOptions
        self.timeout = timeout
    }
}

public struct OpenAIImage: Sendable, Equatable {
    public var url: String?
    public var base64: String?
    public var revisedPrompt: String?

    public init(url: String? = nil, base64: String? = nil, revisedPrompt: String? = nil) {
        self.url = url
        self.base64 = base64
        self.revisedPrompt = revisedPrompt
    }
}

public struct OpenAIImageGenerationResponse: Sendable, Equatable {
    public var created: Int?
    public var images: [OpenAIImage]
    public var raw: JSONValue?

    public init(created: Int? = nil, images: [OpenAIImage], raw: JSONValue? = nil) {
        self.created = created
        self.images = images
        self.raw = raw
    }
}

public struct OpenAIModerationRequest: Sendable {
    public var input: [String]
    public var model: String?
    public var providerOptions: [String: JSONValue]?
    public var timeout: Timeout?

    public init(input: [String], model: String? = nil, providerOptions: [String: JSONValue]? = nil, timeout: Timeout? = nil) {
        self.input = input
        self.model = model
        self.providerOptions = providerOptions
        self.timeout = timeout
    }

    public init(input: String, model: String? = nil, providerOptions: [String: JSONValue]? = nil, timeout: Timeout? = nil) {
        self.init(input: [input], model: model, providerOptions: providerOptions, timeout: timeout)
    }
}

public struct OpenAIModerationResponse: Sendable, Equatable {
    public var model: String?
    public var results: [JSONValue]
    public var raw: JSONValue?

    public init(model: String? = nil, results: [JSONValue], raw: JSONValue? = nil) {
        self.model = model
        self.results = results
        self.raw = raw
    }
}

public struct OpenAIBatchRequest: Sendable {
    public var inputFileId: String
    public var endpoint: String
    public var completionWindow: String
    public var metadata: [String: String]?
    public var timeout: Timeout?

    public init(
        inputFileId: String,
        endpoint: String,
        completionWindow: String = "24h",
        metadata: [String: String]? = nil,
        timeout: Timeout? = nil
    ) {
        self.inputFileId = inputFileId
        self.endpoint = endpoint
        self.completionWindow = completionWindow
        self.metadata = metadata
        self.timeout = timeout
    }
}

public struct OpenAIBatchResponse: Sendable, Equatable {
    public var id: String
    public var status: String?
    public var inputFileId: String?
    public var outputFileId: String?
    public var errorFileId: String?
    public var createdAt: Int?
    public var completedAt: Int?
    public var expiresAt: Int?
    public var metadata: JSONValue?
    public var raw: JSONValue?

    public init(
        id: String,
        status: String? = nil,
        inputFileId: String? = nil,
        outputFileId: String? = nil,
        errorFileId: String? = nil,
        createdAt: Int? = nil,
        completedAt: Int? = nil,
        expiresAt: Int? = nil,
        metadata: JSONValue? = nil,
        raw: JSONValue? = nil
    ) {
        self.id = id
        self.status = status
        self.inputFileId = inputFileId
        self.outputFileId = outputFileId
        self.errorFileId = errorFileId
        self.createdAt = createdAt
        self.completedAt = completedAt
        self.expiresAt = expiresAt
        self.metadata = metadata
        self.raw = raw
    }
}
