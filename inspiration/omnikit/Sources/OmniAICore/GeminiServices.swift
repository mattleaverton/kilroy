import Foundation

public struct GeminiServiceNamespace: Sendable {
    private let client: Client
    private let providerName: String

    public init(client: Client, providerName: String = "gemini") {
        self.client = client
        self.providerName = providerName
    }

    public var files: GeminiFilesService {
        GeminiFilesService(client: client, providerName: providerName)
    }

    public var fileSearch: GeminiFileSearchService {
        GeminiFileSearchService(client: client, providerName: providerName)
    }

    public var tokens: GeminiTokenService {
        GeminiTokenService(client: client, providerName: providerName)
    }

    public var live: GeminiLiveService {
        GeminiLiveService(client: client, providerName: providerName)
    }
}

public struct GeminiFilesService: Sendable {
    private let client: Client
    private let providerName: String

    init(client: Client, providerName: String) {
        self.client = client
        self.providerName = providerName
    }

    public func create(_ request: GeminiFileCreateRequest) async throws -> GeminiFile {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFilesProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.files")
        }
        return try await provider.createFile(request: request)
    }

    public func upload(_ request: GeminiFileUploadRequest) async throws -> GeminiFile {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFilesProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.files")
        }
        return try await provider.uploadFile(request: request)
    }

    public func get(_ name: String) async throws -> GeminiFile {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFilesProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.files")
        }
        return try await provider.getFile(name: name)
    }

    public func list(pageSize: Int? = nil, pageToken: String? = nil) async throws -> GeminiFileListResponse {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFilesProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.files")
        }
        return try await provider.listFiles(pageSize: pageSize, pageToken: pageToken)
    }

    public func delete(_ name: String) async throws {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFilesProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.files")
        }
        try await provider.deleteFile(name: name)
    }
}

public struct GeminiFileSearchService: Sendable {
    private let client: Client
    private let providerName: String

    init(client: Client, providerName: String) {
        self.client = client
        self.providerName = providerName
    }

    public func createStore(_ request: GeminiFileSearchStoreCreateRequest) async throws -> GeminiFileSearchStore {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFileSearchProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.fileSearch")
        }
        return try await provider.createFileSearchStore(request: request)
    }

    public func getStore(_ name: String) async throws -> GeminiFileSearchStore {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFileSearchProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.fileSearch")
        }
        return try await provider.getFileSearchStore(name: name)
    }

    public func listStores(pageSize: Int? = nil, pageToken: String? = nil) async throws -> GeminiFileSearchStoreListResponse {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFileSearchProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.fileSearch")
        }
        return try await provider.listFileSearchStores(pageSize: pageSize, pageToken: pageToken)
    }

    public func deleteStore(_ name: String, force: Bool = false) async throws {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFileSearchProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.fileSearch")
        }
        try await provider.deleteFileSearchStore(name: name, force: force)
    }

    public func importFile(_ request: GeminiFileSearchImportRequest) async throws -> GeminiOperation {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFileSearchProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.fileSearch")
        }
        return try await provider.importFileToSearchStore(request: request)
    }

    public func getDocument(_ name: String) async throws -> GeminiFileSearchDocument {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFileSearchProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.fileSearch")
        }
        return try await provider.getDocument(name: name)
    }

    public func listDocuments(storeName: String, pageSize: Int? = nil, pageToken: String? = nil) async throws -> GeminiFileSearchDocumentListResponse {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFileSearchProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.fileSearch")
        }
        return try await provider.listDocuments(storeName: storeName, pageSize: pageSize, pageToken: pageToken)
    }

    public func deleteDocument(_ name: String, force: Bool = false) async throws {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiFileSearchProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.fileSearch")
        }
        try await provider.deleteDocument(name: name, force: force)
    }
}

public struct GeminiTokenService: Sendable {
    private let client: Client
    private let providerName: String

    init(client: Client, providerName: String) {
        self.client = client
        self.providerName = providerName
    }

    public func countTokens(_ request: GeminiTokenCountRequest) async throws -> GeminiTokenCountResponse {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiTokensProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.tokens")
        }
        return try await provider.countTokens(request: request)
    }
}

public struct GeminiLiveService: Sendable {
    private let client: Client
    private let providerName: String

    init(client: Client, providerName: String) {
        self.client = client
        self.providerName = providerName
    }

    public func connect(_ config: GeminiLiveConfig) async throws -> GeminiLiveSession {
        let adapter = try client.resolveAdapter(provider: providerName)
        guard let provider = adapter as? GeminiLiveProviderAdapter else {
            throw UnsupportedCapabilityError(provider: providerName, capability: "gemini.live")
        }
        return try await provider.connectLive(config: config)
    }
}

public struct GeminiFile: Sendable, Equatable {
    public var name: String
    public var displayName: String?
    public var mimeType: String?
    public var sizeBytes: Int64?
    public var createTime: Date?
    public var updateTime: Date?
    public var expirationTime: Date?
    public var sha256Hash: String?
    public var uri: String?
    public var downloadUri: String?
    public var state: String?
    public var source: String?

    public init(
        name: String,
        displayName: String? = nil,
        mimeType: String? = nil,
        sizeBytes: Int64? = nil,
        createTime: Date? = nil,
        updateTime: Date? = nil,
        expirationTime: Date? = nil,
        sha256Hash: String? = nil,
        uri: String? = nil,
        downloadUri: String? = nil,
        state: String? = nil,
        source: String? = nil
    ) {
        self.name = name
        self.displayName = displayName
        self.mimeType = mimeType
        self.sizeBytes = sizeBytes
        self.createTime = createTime
        self.updateTime = updateTime
        self.expirationTime = expirationTime
        self.sha256Hash = sha256Hash
        self.uri = uri
        self.downloadUri = downloadUri
        self.state = state
        self.source = source
    }
}

public struct GeminiFileCreateRequest: Sendable {
    public var name: String?
    public var displayName: String?
    public var source: String?
    public var timeout: Timeout?

    public init(name: String? = nil, displayName: String? = nil, source: String? = nil, timeout: Timeout? = nil) {
        self.name = name
        self.displayName = displayName
        self.source = source
        self.timeout = timeout
    }
}

public struct GeminiFileUploadRequest: Sendable {
    public var data: [UInt8]
    public var displayName: String
    public var mimeType: String
    public var timeout: Timeout?

    public init(data: [UInt8], displayName: String, mimeType: String, timeout: Timeout? = nil) {
        self.data = data
        self.displayName = displayName
        self.mimeType = mimeType
        self.timeout = timeout
    }
}

public struct GeminiFileListResponse: Sendable, Equatable {
    public var files: [GeminiFile]
    public var nextPageToken: String?

    public init(files: [GeminiFile], nextPageToken: String? = nil) {
        self.files = files
        self.nextPageToken = nextPageToken
    }
}

public struct GeminiCustomMetadata: Sendable, Equatable {
    public var key: String
    public var stringValue: String?
    public var numberValue: Int64?

    public init(key: String, stringValue: String? = nil, numberValue: Int64? = nil) {
        self.key = key
        self.stringValue = stringValue
        self.numberValue = numberValue
    }
}

public struct GeminiChunkingConfig: Sendable, Equatable {
    public var chunkSize: Int
    public var chunkOverlap: Int

    public init(chunkSize: Int, chunkOverlap: Int) {
        self.chunkSize = chunkSize
        self.chunkOverlap = chunkOverlap
    }
}

public struct GeminiFileSearchStore: Sendable, Equatable {
    public var name: String
    public var displayName: String?
    public var createTime: Date?
    public var updateTime: Date?
    public var activeDocumentsCount: Int64?
    public var pendingDocumentsCount: Int64?
    public var failedDocumentsCount: Int64?
    public var sizeBytes: Int64?

    public init(
        name: String,
        displayName: String? = nil,
        createTime: Date? = nil,
        updateTime: Date? = nil,
        activeDocumentsCount: Int64? = nil,
        pendingDocumentsCount: Int64? = nil,
        failedDocumentsCount: Int64? = nil,
        sizeBytes: Int64? = nil
    ) {
        self.name = name
        self.displayName = displayName
        self.createTime = createTime
        self.updateTime = updateTime
        self.activeDocumentsCount = activeDocumentsCount
        self.pendingDocumentsCount = pendingDocumentsCount
        self.failedDocumentsCount = failedDocumentsCount
        self.sizeBytes = sizeBytes
    }
}

public struct GeminiFileSearchStoreCreateRequest: Sendable {
    public var displayName: String
    public var timeout: Timeout?

    public init(displayName: String, timeout: Timeout? = nil) {
        self.displayName = displayName
        self.timeout = timeout
    }
}

public struct GeminiFileSearchStoreListResponse: Sendable, Equatable {
    public var stores: [GeminiFileSearchStore]
    public var nextPageToken: String?

    public init(stores: [GeminiFileSearchStore], nextPageToken: String? = nil) {
        self.stores = stores
        self.nextPageToken = nextPageToken
    }
}

public struct GeminiFileSearchImportRequest: Sendable {
    public var storeName: String
    public var fileName: String
    public var customMetadata: [GeminiCustomMetadata]
    public var chunkingConfig: GeminiChunkingConfig?
    public var timeout: Timeout?

    public init(
        storeName: String,
        fileName: String,
        customMetadata: [GeminiCustomMetadata] = [],
        chunkingConfig: GeminiChunkingConfig? = nil,
        timeout: Timeout? = nil
    ) {
        self.storeName = storeName
        self.fileName = fileName
        self.customMetadata = customMetadata
        self.chunkingConfig = chunkingConfig
        self.timeout = timeout
    }
}

public struct GeminiOperation: Sendable, Equatable {
    public var name: String
    public var done: Bool?
    public var metadata: JSONValue?
    public var response: JSONValue?
    public var error: JSONValue?

    public init(name: String, done: Bool? = nil, metadata: JSONValue? = nil, response: JSONValue? = nil, error: JSONValue? = nil) {
        self.name = name
        self.done = done
        self.metadata = metadata
        self.response = response
        self.error = error
    }
}

public struct GeminiFileSearchDocument: Sendable, Equatable {
    public var name: String
    public var displayName: String?
    public var createTime: Date?
    public var updateTime: Date?
    public var state: String?
    public var sizeBytes: Int64?
    public var mimeType: String?
    public var customMetadata: [GeminiCustomMetadata]

    public init(
        name: String,
        displayName: String? = nil,
        createTime: Date? = nil,
        updateTime: Date? = nil,
        state: String? = nil,
        sizeBytes: Int64? = nil,
        mimeType: String? = nil,
        customMetadata: [GeminiCustomMetadata] = []
    ) {
        self.name = name
        self.displayName = displayName
        self.createTime = createTime
        self.updateTime = updateTime
        self.state = state
        self.sizeBytes = sizeBytes
        self.mimeType = mimeType
        self.customMetadata = customMetadata
    }
}

public struct GeminiFileSearchDocumentListResponse: Sendable, Equatable {
    public var documents: [GeminiFileSearchDocument]
    public var nextPageToken: String?

    public init(documents: [GeminiFileSearchDocument], nextPageToken: String? = nil) {
        self.documents = documents
        self.nextPageToken = nextPageToken
    }
}

public struct GeminiTokenCountRequest: Sendable {
    public var model: String
    public var messages: [Message]
    public var timeout: Timeout?

    public init(model: String, messages: [Message], timeout: Timeout? = nil) {
        self.model = model
        self.messages = messages
        self.timeout = timeout
    }
}

public struct GeminiTokenCountResponse: Sendable, Equatable {
    public var totalTokens: Int
    public var cachedContentTokens: Int?
    public var raw: JSONValue?

    public init(totalTokens: Int, cachedContentTokens: Int? = nil, raw: JSONValue? = nil) {
        self.totalTokens = totalTokens
        self.cachedContentTokens = cachedContentTokens
        self.raw = raw
    }
}

public struct GeminiLiveConfig: Sendable {
    public var model: String
    public var systemInstruction: String?
    public var temperature: Double?
    public var topP: Double?
    public var topK: Int?
    public var maxOutputTokens: Int?
    public var responseModalities: [String]
    public var tools: [JSONValue]

    public init(
        model: String,
        systemInstruction: String? = nil,
        temperature: Double? = nil,
        topP: Double? = nil,
        topK: Int? = nil,
        maxOutputTokens: Int? = nil,
        responseModalities: [String] = [],
        tools: [JSONValue] = []
    ) {
        self.model = model
        self.systemInstruction = systemInstruction
        self.temperature = temperature
        self.topP = topP
        self.topK = topK
        self.maxOutputTokens = maxOutputTokens
        self.responseModalities = responseModalities
        self.tools = tools
    }
}

public struct GeminiLiveFunctionResponse: Sendable, Equatable {
    public var name: String
    public var response: JSONValue

    public init(name: String, response: JSONValue) {
        self.name = name
        self.response = response
    }
}

public struct GeminiLiveServerMessage: Sendable, Equatable {
    public var setupComplete: Bool
    public var serverContent: JSONValue?
    public var toolCall: JSONValue?
    public var toolCallCancellation: JSONValue?
    public var usageMetadata: JSONValue?
    public var raw: JSONValue

    public init(
        setupComplete: Bool,
        serverContent: JSONValue? = nil,
        toolCall: JSONValue? = nil,
        toolCallCancellation: JSONValue? = nil,
        usageMetadata: JSONValue? = nil,
        raw: JSONValue
    ) {
        self.setupComplete = setupComplete
        self.serverContent = serverContent
        self.toolCall = toolCall
        self.toolCallCancellation = toolCallCancellation
        self.usageMetadata = usageMetadata
        self.raw = raw
    }
}

public final class GeminiLiveSession: @unchecked Sendable {
    private let session: JSONRealtimeWebSocketSession

    init(session: JSONRealtimeWebSocketSession) {
        self.session = session
    }

    public func send(_ payload: JSONValue) async throws {
        try await session.send(payload)
    }

    public func sendText(_ text: String, endOfTurn: Bool = true) async throws {
        let turn: JSONValue = .object([
            "role": .string("user"),
            "parts": .array([.object(["text": .string(text)])]),
        ])
        let payload: JSONValue = .object([
            "clientContent": .object([
                "turns": .array([turn]),
                "turnComplete": .bool(endOfTurn),
            ]),
        ])
        try await send(payload)
    }

    public func sendAudio(_ data: [UInt8], mimeType: String) async throws {
        let encoded = Data(data).base64EncodedString()
        let payload: JSONValue = .object([
            "realtimeInput": .object([
                "mediaChunks": .array([
                    .object([
                        "mimeType": .string(mimeType),
                        "data": .string(encoded),
                    ]),
                ]),
            ]),
        ])
        try await send(payload)
    }

    public func sendToolResponses(_ responses: [GeminiLiveFunctionResponse]) async throws {
        let functionResponses = responses.map { response in
            JSONValue.object([
                "name": .string(response.name),
                "response": response.response,
            ])
        }
        let payload: JSONValue = .object([
            "toolResponse": .object([
                "functionResponses": .array(functionResponses),
            ]),
        ])
        try await send(payload)
    }

    public func events() -> AsyncThrowingStream<GeminiLiveServerMessage, Error> {
        let upstream = session.events()
        return AsyncThrowingStream { continuation in
            Task {
                do {
                    for try await payload in upstream {
                        let message = GeminiLiveServerMessage(
                            setupComplete: payload["setupComplete"] != nil,
                            serverContent: payload["serverContent"],
                            toolCall: payload["toolCall"],
                            toolCallCancellation: payload["toolCallCancellation"],
                            usageMetadata: payload["usageMetadata"],
                            raw: payload
                        )
                        continuation.yield(message)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    public func close() async {
        await session.close(code: .normalClosure)
    }
}
