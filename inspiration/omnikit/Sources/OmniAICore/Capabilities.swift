import Foundation

public protocol ToolContinuationProviderAdapter: Sendable {
    func sendToolOutputs(request: ToolContinuationRequest) async throws -> Response
}

public protocol EmbeddingProviderAdapter: Sendable {
    func embed(request: EmbedRequest) async throws -> EmbedResponse
}

@available(macOS 12.0, iOS 15.0, watchOS 8.0, tvOS 15.0, *)
public protocol RealtimeProviderAdapter: Sendable {
    func makeRealtimeClient() throws -> OpenAIRealtimeClient
}

public protocol OpenAIAudioProviderAdapter: Sendable {
    func createSpeech(request: OpenAISpeechRequest) async throws -> OpenAISpeechResponse
    func createTranscription(request: OpenAITranscriptionRequest) async throws -> OpenAITranscriptionResponse
    func createTranslation(request: OpenAITranslationRequest) async throws -> OpenAITranslationResponse
}

public protocol OpenAIImagesProviderAdapter: Sendable {
    func generateImages(request: OpenAIImageGenerationRequest) async throws -> OpenAIImageGenerationResponse
}

public protocol OpenAIModerationsProviderAdapter: Sendable {
    func createModeration(request: OpenAIModerationRequest) async throws -> OpenAIModerationResponse
}

public protocol OpenAIBatchesProviderAdapter: Sendable {
    func createBatch(request: OpenAIBatchRequest) async throws -> OpenAIBatchResponse
    func retrieveBatch(id: String) async throws -> OpenAIBatchResponse
    func cancelBatch(id: String) async throws -> OpenAIBatchResponse
}

public protocol GeminiFilesProviderAdapter: Sendable {
    func createFile(request: GeminiFileCreateRequest) async throws -> GeminiFile
    func uploadFile(request: GeminiFileUploadRequest) async throws -> GeminiFile
    func getFile(name: String) async throws -> GeminiFile
    func listFiles(pageSize: Int?, pageToken: String?) async throws -> GeminiFileListResponse
    func deleteFile(name: String) async throws
}

public protocol GeminiFileSearchProviderAdapter: Sendable {
    func createFileSearchStore(request: GeminiFileSearchStoreCreateRequest) async throws -> GeminiFileSearchStore
    func getFileSearchStore(name: String) async throws -> GeminiFileSearchStore
    func listFileSearchStores(pageSize: Int?, pageToken: String?) async throws -> GeminiFileSearchStoreListResponse
    func deleteFileSearchStore(name: String, force: Bool) async throws
    func importFileToSearchStore(request: GeminiFileSearchImportRequest) async throws -> GeminiOperation

    func getDocument(name: String) async throws -> GeminiFileSearchDocument
    func listDocuments(storeName: String, pageSize: Int?, pageToken: String?) async throws -> GeminiFileSearchDocumentListResponse
    func deleteDocument(name: String, force: Bool) async throws
}

public protocol GeminiTokensProviderAdapter: Sendable {
    func countTokens(request: GeminiTokenCountRequest) async throws -> GeminiTokenCountResponse
}

public protocol GeminiLiveProviderAdapter: Sendable {
    func connectLive(config: GeminiLiveConfig) async throws -> GeminiLiveSession
}
