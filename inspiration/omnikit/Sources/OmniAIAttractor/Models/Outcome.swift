import Foundation

// MARK: - Outcome Status

public enum OutcomeStatus: String, Sendable, Codable, Equatable {
    case success = "success"
    case partialSuccess = "partial_success"
    case retry = "retry"
    case fail = "fail"
    case skipped = "skipped"
}

// MARK: - Outcome

public struct Outcome: Sendable {
    public var status: OutcomeStatus
    public var preferredLabel: String
    public var suggestedNextIds: [String]
    public var contextUpdates: [String: String]
    public var notes: String
    public var failureReason: String

    public init(
        status: OutcomeStatus,
        preferredLabel: String = "",
        suggestedNextIds: [String] = [],
        contextUpdates: [String: String] = [:],
        notes: String = "",
        failureReason: String = ""
    ) {
        self.status = status
        self.preferredLabel = preferredLabel
        self.suggestedNextIds = suggestedNextIds
        self.contextUpdates = contextUpdates
        self.notes = notes
        self.failureReason = failureReason
    }

    public static func success(
        preferredLabel: String = "",
        contextUpdates: [String: String] = [:],
        notes: String = ""
    ) -> Outcome {
        Outcome(status: .success, preferredLabel: preferredLabel, contextUpdates: contextUpdates, notes: notes)
    }

    public static func fail(
        reason: String,
        preferredLabel: String = "",
        contextUpdates: [String: String] = [:]
    ) -> Outcome {
        Outcome(status: .fail, preferredLabel: preferredLabel, contextUpdates: contextUpdates, failureReason: reason)
    }

    public static func retry(
        reason: String = "",
        contextUpdates: [String: String] = [:]
    ) -> Outcome {
        Outcome(status: .retry, contextUpdates: contextUpdates, notes: reason)
    }

    // JSON-serializable representation for status.json
    public func toStatusJSON() -> [String: Any] {
        var dict: [String: Any] = ["outcome": status.rawValue]
        if !preferredLabel.isEmpty { dict["preferred_next_label"] = preferredLabel }
        if !suggestedNextIds.isEmpty { dict["suggested_next_ids"] = suggestedNextIds }
        if !contextUpdates.isEmpty {
            dict["context_updates"] = contextUpdates
        }
        if !notes.isEmpty { dict["notes"] = notes }
        if !failureReason.isEmpty { dict["failure_reason"] = failureReason }
        return dict
    }
}
