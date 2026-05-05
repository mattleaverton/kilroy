import Foundation

// MARK: - Diagnostic Severity

public enum DiagnosticSeverity: String, Sendable {
    case error
    case warning
    case info
}

// MARK: - Diagnostic

public struct Diagnostic: Sendable {
    public var rule: String
    public var severity: DiagnosticSeverity
    public var message: String
    public var nodeId: String?
    public var edge: (String, String)?
    public var fix: String?

    public init(
        rule: String,
        severity: DiagnosticSeverity,
        message: String,
        nodeId: String? = nil,
        edge: (String, String)? = nil,
        fix: String? = nil
    ) {
        self.rule = rule
        self.severity = severity
        self.message = message
        self.nodeId = nodeId
        self.edge = edge
        self.fix = fix
    }

    public var isError: Bool { severity == .error }
    public var isWarning: Bool { severity == .warning }
}
