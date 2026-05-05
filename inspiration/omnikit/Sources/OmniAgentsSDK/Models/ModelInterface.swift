import Foundation
import OmniAICore

public enum ModelTracing: Sendable, Equatable {
    case disabled
    case enabled
    case enabledWithoutData

    public func isDisabled() -> Bool {
        self == .disabled
    }

    public func includeData() -> Bool {
        self == .enabled
    }
}

public enum ModelReference: @unchecked Sendable {
    case name(String)
    case instance(any Model)
}

public protocol Model: Sendable {
    func close() async

    func getResponse(
        systemInstructions: String?,
        input: StringOrInputList,
        modelSettings: ModelSettings,
        tools: [Tool],
        outputSchema: (any AgentOutputSchemaBase)?,
        handoffs: [Any],
        tracing: ModelTracing,
        previousResponseID: String?,
        conversationID: String?,
        prompt: Prompt?
    ) async throws -> ModelResponse

    func streamResponse(
        systemInstructions: String?,
        input: StringOrInputList,
        modelSettings: ModelSettings,
        tools: [Tool],
        outputSchema: (any AgentOutputSchemaBase)?,
        handoffs: [Any],
        tracing: ModelTracing,
        previousResponseID: String?,
        conversationID: String?,
        prompt: Prompt?
    ) async throws -> AsyncThrowingStream<TResponseStreamEvent, Error>
}

public protocol ModelProvider: Sendable {
    func getModel(_ modelName: String?) -> any Model
    func close() async
}

extension ModelProvider {
    public func close() async {}
}

