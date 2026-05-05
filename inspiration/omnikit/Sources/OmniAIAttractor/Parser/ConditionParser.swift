import Foundation

// MARK: - Condition Operator

public enum ConditionOperator: String, Sendable {
    case equals = "="
    case notEquals = "!="
    case truthy = "truthy"
}

// MARK: - Condition Clause

public struct ConditionClause: Sendable {
    public let key: String
    public let op: ConditionOperator
    public let value: String

    public init(key: String, op: ConditionOperator, value: String) {
        self.key = key
        self.op = op
        self.value = value
    }
}

// MARK: - Condition Expression

public struct ConditionExpression: Sendable {
    public let clauses: [ConditionClause]

    public init(clauses: [ConditionClause]) {
        self.clauses = clauses
    }

    public func evaluate(outcome: String, preferredLabel: String, context: PipelineContext) -> Bool {
        if clauses.isEmpty { return true }

        for clause in clauses {
            let resolved = resolveKey(clause.key, outcome: outcome, preferredLabel: preferredLabel, context: context)
            let matches: Bool
            switch clause.op {
            case .equals:
                matches = resolved == clause.value
            case .notEquals:
                matches = resolved != clause.value
            case .truthy:
                // Bare key truthiness: non-empty and not "false" and not "0"
                matches = !resolved.isEmpty && resolved != "false" && resolved != "0"
            }
            if !matches { return false }
        }
        return true
    }

    private func resolveKey(_ key: String, outcome: String, preferredLabel: String, context: PipelineContext) -> String {
        if key == "outcome" {
            return outcome
        }
        if key == "preferred_label" {
            return preferredLabel
        }
        if key.hasPrefix("context.") {
            let contextKey = String(key.dropFirst("context.".count))
            // Try the full key first (with context. prefix)
            let fullVal = context.getString(key)
            if !fullVal.isEmpty { return fullVal }
            // Then try without the prefix
            let shortVal = context.getString(contextKey)
            if !shortVal.isEmpty { return shortVal }
            return ""
        }
        // Direct context lookup for unqualified keys
        let val = context.getString(key)
        return val
    }
}

// MARK: - Condition Parser

public struct ConditionParser {

    public static func parse(_ expression: String) throws -> ConditionExpression {
        let trimmed = expression.trimmingCharacters(in: .whitespaces)
        if trimmed.isEmpty {
            return ConditionExpression(clauses: [])
        }

        let parts = trimmed.components(separatedBy: "&&")
        var clauses: [ConditionClause] = []

        for part in parts {
            let clause = part.trimmingCharacters(in: .whitespaces)
            if clause.isEmpty { continue }
            clauses.append(try parseClause(clause))
        }

        return ConditionExpression(clauses: clauses)
    }

    private static func parseClause(_ clause: String) throws -> ConditionClause {
        // Check for != first (before =) to avoid partial match
        if let range = clause.range(of: "!=") {
            let key = String(clause[clause.startIndex..<range.lowerBound]).trimmingCharacters(in: .whitespaces)
            let value = String(clause[range.upperBound...]).trimmingCharacters(in: .whitespaces)
            guard !key.isEmpty else {
                throw AttractorError.parseError("Empty key in condition clause: \(clause)")
            }
            return ConditionClause(key: key, op: .notEquals, value: stripQuotes(value))
        }

        if let range = clause.range(of: "=") {
            let key = String(clause[clause.startIndex..<range.lowerBound]).trimmingCharacters(in: .whitespaces)
            let value = String(clause[range.upperBound...]).trimmingCharacters(in: .whitespaces)
            guard !key.isEmpty else {
                throw AttractorError.parseError("Empty key in condition clause: \(clause)")
            }
            return ConditionClause(key: key, op: .equals, value: stripQuotes(value))
        }

        // Bare key: check if truthy (spec §10.5)
        let key = clause.trimmingCharacters(in: .whitespaces)
        guard !key.isEmpty else {
            throw AttractorError.parseError("Empty condition clause")
        }
        return ConditionClause(key: key, op: .truthy, value: "")
    }

    private static func stripQuotes(_ s: String) -> String {
        if s.hasPrefix("\"") && s.hasSuffix("\"") && s.count >= 2 {
            return String(s.dropFirst().dropLast())
        }
        return s
    }
}



