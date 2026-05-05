import Foundation

public struct OpenAIServerConversationTracker: Sendable {
    public var conversationID: String?
    public var previousResponseID: String?
    public var autoPreviousResponseID: Bool

    public init(conversationID: String? = nil, previousResponseID: String? = nil, autoPreviousResponseID: Bool = false) {
        self.conversationID = conversationID
        self.previousResponseID = previousResponseID
        self.autoPreviousResponseID = autoPreviousResponseID
    }

    public mutating func record(_ response: ModelResponse) {
        if autoPreviousResponseID {
            previousResponseID = response.responseID
        }
    }
}

