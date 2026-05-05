import Foundation

// MARK: - Style Selector

public enum StyleSelector: Sendable, Equatable {
    case universal           // * - specificity 0
    case shapeName(String)   // box, diamond, etc. - specificity 1
    case className(String)   // .fast - specificity 2
    case nodeId(String)      // #review - specificity 3
}

// MARK: - Style Properties

public struct StyleProperties: Sendable {
    public var llmModel: String?
    public var llmProvider: String?
    public var reasoningEffort: String?

    public init(
        llmModel: String? = nil,
        llmProvider: String? = nil,
        reasoningEffort: String? = nil
    ) {
        self.llmModel = llmModel
        self.llmProvider = llmProvider
        self.reasoningEffort = reasoningEffort
    }

    /// Merge another set of properties onto this one. Values from `other` override `self`.
    public func merging(_ other: StyleProperties) -> StyleProperties {
        StyleProperties(
            llmModel: other.llmModel ?? llmModel,
            llmProvider: other.llmProvider ?? llmProvider,
            reasoningEffort: other.reasoningEffort ?? reasoningEffort
        )
    }
}

// MARK: - Style Rule

public struct StyleRule: Sendable {
    public let selector: StyleSelector
    public let properties: StyleProperties
    public var specificity: Int {
        switch selector {
        case .universal: return 0
        case .shapeName: return 1
        case .className: return 2
        case .nodeId: return 3
        }
    }

    public init(selector: StyleSelector, properties: StyleProperties) {
        self.selector = selector
        self.properties = properties
    }
}

// MARK: - Stylesheet

public struct Stylesheet: Sendable {
    public let rules: [StyleRule]

    public init(rules: [StyleRule]) {
        self.rules = rules
    }

    /// Resolve the effective style properties for a node, given its ID, shape, and classes.
    /// Rules are applied in specificity order (low to high).
    /// Among rules of equal specificity, later rules override earlier ones.
    public func resolve(nodeId: String, nodeShape: String = "", nodeClasses: [String]) -> StyleProperties {
        // Group matching rules by specificity
        var matchingRules: [(index: Int, rule: StyleRule)] = []

        for (index, rule) in rules.enumerated() {
            let matches: Bool
            switch rule.selector {
            case .universal:
                matches = true
            case .shapeName(let shape):
                matches = shape.lowercased() == nodeShape.lowercased()
            case .className(let cls):
                matches = nodeClasses.contains(cls)
            case .nodeId(let id):
                matches = id == nodeId
            }
            if matches {
                matchingRules.append((index, rule))
            }
        }

        // Sort by specificity (ascending), then by source order (ascending) for stable override
        matchingRules.sort { a, b in
            if a.rule.specificity != b.rule.specificity {
                return a.rule.specificity < b.rule.specificity
            }
            return a.index < b.index
        }

        // Merge in order: later entries override earlier
        var result = StyleProperties()
        for (_, rule) in matchingRules {
            result = result.merging(rule.properties)
        }

        return result
    }
}

// MARK: - Stylesheet Parser

public struct StylesheetParser {

    public static func parse(_ source: String) throws -> Stylesheet {
        let trimmed = source.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty {
            return Stylesheet(rules: [])
        }

        var rules: [StyleRule] = []
        var pos = trimmed.startIndex

        while pos < trimmed.endIndex {
            skipWhitespace(in: trimmed, pos: &pos)
            if pos >= trimmed.endIndex { break }

            // Parse selector
            let selector = try parseSelector(in: trimmed, pos: &pos)
            skipWhitespace(in: trimmed, pos: &pos)

            // Expect '{'
            guard pos < trimmed.endIndex && trimmed[pos] == "{" else {
                throw AttractorError.parseError("Expected '{' after selector in stylesheet")
            }
            pos = trimmed.index(after: pos)

            // Parse declarations
            var properties = StyleProperties()
            while pos < trimmed.endIndex {
                skipWhitespace(in: trimmed, pos: &pos)
                if pos >= trimmed.endIndex { break }
                if trimmed[pos] == "}" {
                    pos = trimmed.index(after: pos)
                    break
                }

                let (prop, value) = try parseDeclaration(in: trimmed, pos: &pos)
                switch prop {
                case "llm_model", "model":
                    properties.llmModel = value
                case "llm_provider", "provider":
                    properties.llmProvider = value
                case "reasoning_effort":
                    properties.reasoningEffort = value
                default:
                    throw AttractorError.parseError("Unknown stylesheet property: \(prop)")
                }
            }

            rules.append(StyleRule(selector: selector, properties: properties))
        }

        return Stylesheet(rules: rules)
    }

    // MARK: - Helpers

    private static func skipWhitespace(in source: String, pos: inout String.Index) {
        while pos < source.endIndex && source[pos].isWhitespace {
            pos = source.index(after: pos)
        }
    }

    private static func parseSelector(in source: String, pos: inout String.Index) throws -> StyleSelector {
        guard pos < source.endIndex else {
            throw AttractorError.parseError("Unexpected end of stylesheet while parsing selector")
        }

        let ch = source[pos]
        if ch == "*" {
            pos = source.index(after: pos)
            return .universal
        }
        if ch == "#" {
            pos = source.index(after: pos)
            let id = readIdentifier(in: source, pos: &pos)
            guard !id.isEmpty else {
                throw AttractorError.parseError("Expected identifier after '#' in stylesheet selector")
            }
            return .nodeId(id)
        }
        if ch == "." {
            pos = source.index(after: pos)
            let cls = readClassName(in: source, pos: &pos)
            guard !cls.isEmpty else {
                throw AttractorError.parseError("Expected class name after '.' in stylesheet selector")
            }
            return .className(cls)
        }
        // Bare identifier = shape name selector (e.g., box, diamond, hexagon)
        if ch.isLetter || ch == "_" {
            let name = readIdentifier(in: source, pos: &pos)
            guard !name.isEmpty else {
                throw AttractorError.parseError("Expected shape name in stylesheet selector")
            }
            return .shapeName(name)
        }
        throw AttractorError.parseError("Invalid stylesheet selector starting with '\(ch)'")
    }

    private static func readIdentifier(in source: String, pos: inout String.Index) -> String {
        var result = ""
        while pos < source.endIndex {
            let ch = source[pos]
            if ch.isLetter || ch.isNumber || ch == "_" {
                result.append(ch)
                pos = source.index(after: pos)
            } else {
                break
            }
        }
        return result
    }

    private static func readClassName(in source: String, pos: inout String.Index) -> String {
        var result = ""
        while pos < source.endIndex {
            let ch = source[pos]
            if ch.isLetter || ch.isNumber || ch == "-" || ch == "_" {
                result.append(ch)
                pos = source.index(after: pos)
            } else {
                break
            }
        }
        return result
    }

    private static func parseDeclaration(in source: String, pos: inout String.Index) throws -> (String, String) {
        skipWhitespace(in: source, pos: &pos)

        // Read property name
        let prop = readIdentifier(in: source, pos: &pos)
        guard !prop.isEmpty else {
            throw AttractorError.parseError("Expected property name in stylesheet declaration")
        }

        skipWhitespace(in: source, pos: &pos)

        // Expect ':'
        guard pos < source.endIndex && source[pos] == ":" else {
            throw AttractorError.parseError("Expected ':' after property name '\(prop)' in stylesheet")
        }
        pos = source.index(after: pos)

        skipWhitespace(in: source, pos: &pos)

        // Read value (up to ; or })
        let value = readPropertyValue(in: source, pos: &pos)

        // Consume optional semicolon
        skipWhitespace(in: source, pos: &pos)
        if pos < source.endIndex && source[pos] == ";" {
            pos = source.index(after: pos)
        }

        return (prop, value)
    }

    private static func readPropertyValue(in source: String, pos: inout String.Index) -> String {
        var result = ""
        while pos < source.endIndex {
            let ch = source[pos]
            if ch == ";" || ch == "}" {
                break
            }
            result.append(ch)
            pos = source.index(after: pos)
        }
        return result.trimmingCharacters(in: .whitespaces)
    }
}




