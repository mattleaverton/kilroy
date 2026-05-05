import Foundation

public final actor OpenAIConversationsSession: Session {
    public let sessionID: String
    public let sessionSettings: SessionSettings?
    public private(set) var conversationID: String?
    private var items: [TResponseInputItem]

    public init(sessionID: String, conversationID: String? = nil, sessionSettings: SessionSettings? = nil) {
        self.sessionID = sessionID
        self.sessionSettings = sessionSettings
        self.conversationID = conversationID
        self.items = []
    }

    public func getItems(limit: Int? = nil) async throws -> [TResponseInputItem] {
        let resolvedLimit = resolveSessionLimit(limit, settings: sessionSettings)
        guard let resolvedLimit else { return items }
        return Array(items.suffix(resolvedLimit))
    }

    public func addItems(_ items: [TResponseInputItem]) async throws {
        self.items.append(contentsOf: items)
    }

    public func popItem() async throws -> TResponseInputItem? {
        items.popLast()
    }

    public func clearSession() async throws {
        items.removeAll()
    }
}

