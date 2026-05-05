import Foundation
import OmniAICore

public func resolveSessionLimit(_ explicitLimit: Int?, settings: SessionSettings?) -> Int? {
    explicitLimit ?? settings?.limit
}

public struct SessionSettings: Sendable, Codable, Equatable {
    public var limit: Int?

    public init(limit: Int? = nil) {
        self.limit = limit
    }

    public func resolve(_ override: SessionSettings?) -> SessionSettings {
        guard let override else { return self }
        return SessionSettings(limit: override.limit ?? limit)
    }

    public func toDictionary() -> [String: JSONValue] {
        ["limit": limit.map { .number(Double($0)) } ?? .null]
    }
}

public protocol Session: Sendable {
    var sessionID: String { get }
    var sessionSettings: SessionSettings? { get }
    func getItems(limit: Int?) async throws -> [TResponseInputItem]
    func addItems(_ items: [TResponseInputItem]) async throws
    func popItem() async throws -> TResponseInputItem?
    func clearSession() async throws
}

open class SessionABC: Session, @unchecked Sendable {
    public let sessionID: String
    public let sessionSettings: SessionSettings?

    public init(sessionID: String, sessionSettings: SessionSettings? = nil) {
        self.sessionID = sessionID
        self.sessionSettings = sessionSettings
    }

    open func getItems(limit: Int? = nil) async throws -> [TResponseInputItem] { [] }
    open func addItems(_ items: [TResponseInputItem]) async throws {}
    open func popItem() async throws -> TResponseInputItem? { nil }
    open func clearSession() async throws {}
}

public enum OpenAIResponsesCompactionMode: String, Sendable, Codable, Equatable {
    case input
    case previousResponseID = "previous_response_id"
    case automatic = "auto"
}

public struct OpenAIResponsesCompactionArgs: Sendable, Codable, Equatable {
    public var responseID: String?
    public var compactionMode: OpenAIResponsesCompactionMode?
    public var store: Bool?
    public var force: Bool?

    public init(responseID: String? = nil, compactionMode: OpenAIResponsesCompactionMode? = nil, store: Bool? = nil, force: Bool? = nil) {
        self.responseID = responseID
        self.compactionMode = compactionMode
        self.store = store
        self.force = force
    }
}

public protocol OpenAIResponsesCompactionAwareSession: Session {
    func runCompaction(args: OpenAIResponsesCompactionArgs?) async throws
}

public func isOpenAIResponsesCompactionAwareSession(_ session: Session?) -> Bool {
    session is any OpenAIResponsesCompactionAwareSession
}
