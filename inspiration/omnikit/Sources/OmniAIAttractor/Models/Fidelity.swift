import Foundation

// MARK: - Context Fidelity

public enum ContextFidelity: Sendable, Equatable {
    case full
    case truncate
    case compact
    case summaryLow
    case summaryMedium
    case summaryHigh

    public static func parse(_ s: String) -> ContextFidelity? {
        switch s.lowercased().trimmingCharacters(in: .whitespaces) {
        case "full": return .full
        case "truncate": return .truncate
        case "compact": return .compact
        case "summary:low": return .summaryLow
        case "summary:medium": return .summaryMedium
        case "summary:high": return .summaryHigh
        case "": return nil
        default: return nil
        }
    }

    public var rawValue: String {
        switch self {
        case .full: return "full"
        case .truncate: return "truncate"
        case .compact: return "compact"
        case .summaryLow: return "summary:low"
        case .summaryMedium: return "summary:medium"
        case .summaryHigh: return "summary:high"
        }
    }

    public var tokenBudget: Int {
        switch self {
        case .full: return Int.max
        case .truncate: return 100
        case .compact: return 800
        case .summaryLow: return 600
        case .summaryMedium: return 1500
        case .summaryHigh: return 3000
        }
    }

    public var useFreshSession: Bool {
        self != .full
    }

    /// Resolve fidelity from edge, node, and graph defaults
    public static func resolve(
        edgeFidelity: String,
        nodeFidelity: String,
        graphDefault: String
    ) -> ContextFidelity {
        if let f = parse(edgeFidelity) { return f }
        if let f = parse(nodeFidelity) { return f }
        if let f = parse(graphDefault) { return f }
        return .compact
    }
}
