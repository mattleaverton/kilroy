import Foundation
import OmniAICore

public final class OpenAIChatCompletionsModel: OpenAIResponsesModel, @unchecked Sendable {
    public override init(modelName: String? = nil, client: Client? = nil, providerOptions: [String: JSONValue] = [:]) {
        super.init(modelName: modelName, client: client, providerOptions: providerOptions)
    }
}
