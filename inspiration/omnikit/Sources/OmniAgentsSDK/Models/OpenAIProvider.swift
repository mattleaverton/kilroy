import Foundation
import OmniAICore

public final class OpenAIProvider: OmniAICoreProvider, @unchecked Sendable {
    public let useResponsesWebSocket: Bool

    public init(
        client: Client? = nil,
        useResponsesWebSocket: Bool = false,
        providerOptions: [String: JSONValue] = [:]
    ) {
        self.useResponsesWebSocket = useResponsesWebSocket
        super.init(providerName: "openai", client: client, providerOptions: providerOptions)
    }

    public override func getModel(_ modelName: String?) -> any Model {
        if useResponsesWebSocket {
            return OpenAIResponsesWSModel(modelName: modelName, client: client)
        }
        return OpenAIResponsesModel(modelName: modelName, client: client, providerOptions: providerOptions)
    }
}
