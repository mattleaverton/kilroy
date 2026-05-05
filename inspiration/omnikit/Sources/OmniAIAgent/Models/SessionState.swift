import Foundation

public enum SessionState: String, Sendable, Codable {
    case idle = "idle"
    case processing = "processing"
    case awaitingInput = "awaiting_input"
    case closed = "closed"
}
