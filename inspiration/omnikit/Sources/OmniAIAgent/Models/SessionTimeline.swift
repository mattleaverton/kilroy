import Foundation

public struct ResponseTimelineEntry: Codable, Sendable {
    public var responseId: String
    public var turns: [PersistedTurn]
    public var createdAt: Date

    public init(responseId: String, turns: [PersistedTurn], createdAt: Date = Date()) {
        self.responseId = responseId
        self.turns = turns
        self.createdAt = createdAt
    }
}

public enum SessionTimelineError: Error, Sendable, Equatable {
    case responseNotFound(String)
}
