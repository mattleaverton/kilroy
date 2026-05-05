import Foundation

public struct SubAgentHandle: Sendable {
    public var id: String
    public var session: Session
    public var status: SubAgentStatus

    public init(id: String, session: Session, status: SubAgentStatus = .running) {
        self.id = id
        self.session = session
        self.status = status
    }
}

public enum SubAgentStatus: String, Sendable {
    case running
    case completed
    case failed
}

public struct SubAgentResult: Sendable {
    public var output: String
    public var success: Bool
    public var turnsUsed: Int

    public init(output: String, success: Bool, turnsUsed: Int) {
        self.output = output
        self.success = success
        self.turnsUsed = turnsUsed
    }
}
