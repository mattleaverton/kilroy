import Foundation

public struct EmbedRequest: Sendable {
    public var model: String
    public var input: [String]
    public var provider: String?
    public var dimensions: Int?
    public var taskType: String?
    public var user: String?
    public var metadata: [String: String]?
    public var providerOptions: [String: JSONValue]?
    public var timeout: Timeout?
    public var abortSignal: AbortSignal?

    public init(
        model: String,
        input: [String],
        provider: String? = nil,
        dimensions: Int? = nil,
        taskType: String? = nil,
        user: String? = nil,
        metadata: [String: String]? = nil,
        providerOptions: [String: JSONValue]? = nil,
        timeout: Timeout? = nil,
        abortSignal: AbortSignal? = nil
    ) {
        self.model = model
        self.input = input
        self.provider = provider
        self.dimensions = dimensions
        self.taskType = taskType
        self.user = user
        self.metadata = metadata
        self.providerOptions = providerOptions
        self.timeout = timeout
        self.abortSignal = abortSignal
    }

    public init(
        model: String,
        input: String,
        provider: String? = nil,
        dimensions: Int? = nil,
        taskType: String? = nil,
        user: String? = nil,
        metadata: [String: String]? = nil,
        providerOptions: [String: JSONValue]? = nil,
        timeout: Timeout? = nil,
        abortSignal: AbortSignal? = nil
    ) {
        self.init(
            model: model,
            input: [input],
            provider: provider,
            dimensions: dimensions,
            taskType: taskType,
            user: user,
            metadata: metadata,
            providerOptions: providerOptions,
            timeout: timeout,
            abortSignal: abortSignal
        )
    }
}

public struct Embedding: Sendable, Equatable {
    public var index: Int
    public var vector: [Double]

    public init(index: Int, vector: [Double]) {
        self.index = index
        self.vector = vector
    }
}

public struct EmbedUsage: Sendable, Equatable {
    public var promptTokens: Int
    public var totalTokens: Int
    public var raw: JSONValue?

    public init(promptTokens: Int, totalTokens: Int, raw: JSONValue? = nil) {
        self.promptTokens = promptTokens
        self.totalTokens = totalTokens
        self.raw = raw
    }
}

public struct EmbedResponse: Sendable, Equatable {
    public var model: String
    public var provider: String
    public var embeddings: [Embedding]
    public var usage: EmbedUsage?
    public var raw: JSONValue?

    public init(
        model: String,
        provider: String,
        embeddings: [Embedding],
        usage: EmbedUsage? = nil,
        raw: JSONValue? = nil
    ) {
        self.model = model
        self.provider = provider
        self.embeddings = embeddings
        self.usage = usage
        self.raw = raw
    }
}
