import Foundation

// MARK: - DOT Parser

public struct DOTParser {

    public static func parse(_ source: String) throws -> Graph {
        var lexer = DOTLexer(source: source)
        let tokens = try lexer.tokenize()
        var parser = DOTTokenParser(tokens: tokens)
        return try parser.parseGraph()
    }
}

// MARK: - Token

enum DOTToken: Equatable {
    case keyword(String)       // digraph, graph, node, edge, subgraph, strict
    case identifier(String)    // bare identifier
    case stringLiteral(String) // double-quoted string (contents unescaped)
    case integerLiteral(Int)
    case floatLiteral(Double)
    case booleanLiteral(Bool)
    case durationLiteral(String) // raw string like "900s"
    case arrow                 // ->
    case equals                // =
    case comma                 // ,
    case semicolon             // ;
    case lbrace                // {
    case rbrace                // }
    case lbracket              // [
    case rbracket              // ]
    case dot                   // .
    case eof
}

// MARK: - Lexer

struct DOTLexer {
    let source: String
    private var chars: [Character]
    private var pos: Int

    init(source: String) {
        self.source = source
        self.chars = Array(source)
        self.pos = 0
    }

    mutating func tokenize() throws -> [DOTToken] {
        var tokens: [DOTToken] = []
        while pos < chars.count {
            skipWhitespace()
            if pos >= chars.count { break }

            let ch = chars[pos]

            // Line comment
            if ch == "/" && pos + 1 < chars.count && chars[pos + 1] == "/" {
                skipLineComment()
                continue
            }

            // Block comment
            if ch == "/" && pos + 1 < chars.count && chars[pos + 1] == "*" {
                try skipBlockComment()
                continue
            }

            // String literal
            if ch == "\"" {
                tokens.append(try readString())
                continue
            }

            // Arrow
            if ch == "-" && pos + 1 < chars.count && chars[pos + 1] == ">" {
                pos += 2
                tokens.append(.arrow)
                continue
            }

            // Undirected edge (rejected)
            if ch == "-" && pos + 1 < chars.count && chars[pos + 1] == "-" {
                throw AttractorError.invalidDOT("Undirected edges (--) are not supported; use -> for directed edges")
            }

            // Single-char tokens
            switch ch {
            case "=":
                pos += 1
                tokens.append(.equals)
                continue
            case ",":
                pos += 1
                tokens.append(.comma)
                continue
            case ";":
                pos += 1
                tokens.append(.semicolon)
                continue
            case "{":
                pos += 1
                tokens.append(.lbrace)
                continue
            case "}":
                pos += 1
                tokens.append(.rbrace)
                continue
            case "[":
                pos += 1
                tokens.append(.lbracket)
                continue
            case "]":
                pos += 1
                tokens.append(.rbracket)
                continue
            default:
                break
            }

            // Number or duration (starts with digit or minus followed by digit)
            if ch.isNumber || (ch == "-" && pos + 1 < chars.count && chars[pos + 1].isNumber) {
                tokens.append(try readNumber())
                continue
            }

            // Identifier or keyword
            if ch.isLetter || ch == "_" {
                tokens.append(readIdentifierOrKeyword())
                continue
            }

            // Dot (for qualified identifiers)
            if ch == "." {
                pos += 1
                tokens.append(.dot)
                continue
            }

            throw AttractorError.parseError("Unexpected character '\(ch)' at position \(pos)")
        }
        tokens.append(.eof)
        return tokens
    }

    // MARK: - Lexer Helpers

    private mutating func skipWhitespace() {
        while pos < chars.count && chars[pos].isWhitespace {
            pos += 1
        }
    }

    private mutating func skipLineComment() {
        pos += 2 // skip //
        while pos < chars.count && chars[pos] != "\n" {
            pos += 1
        }
    }

    private mutating func skipBlockComment() throws {
        pos += 2 // skip /*
        while pos < chars.count {
            if chars[pos] == "*" && pos + 1 < chars.count && chars[pos + 1] == "/" {
                pos += 2
                return
            }
            pos += 1
        }
        throw AttractorError.parseError("Unterminated block comment")
    }

    private mutating func readString() throws -> DOTToken {
        pos += 1 // skip opening quote
        var result = ""
        while pos < chars.count {
            let ch = chars[pos]
            if ch == "\\" {
                pos += 1
                guard pos < chars.count else {
                    throw AttractorError.parseError("Unterminated escape sequence in string")
                }
                let escaped = chars[pos]
                switch escaped {
                case "n": result.append("\n")
                case "t": result.append("\t")
                case "\\": result.append("\\")
                case "\"": result.append("\"")
                default: result.append("\\"); result.append(escaped)
                }
                pos += 1
            } else if ch == "\"" {
                pos += 1
                return .stringLiteral(result)
            } else {
                result.append(ch)
                pos += 1
            }
        }
        throw AttractorError.parseError("Unterminated string literal")
    }

    private mutating func readNumber() throws -> DOTToken {
        let start = pos
        if chars[pos] == "-" { pos += 1 }
        while pos < chars.count && chars[pos].isNumber {
            pos += 1
        }

        // Check for float
        if pos < chars.count && chars[pos] == "." && pos + 1 < chars.count && chars[pos + 1].isNumber {
            pos += 1
            while pos < chars.count && chars[pos].isNumber {
                pos += 1
            }
            let str = String(chars[start..<pos])
            guard let val = Double(str) else {
                throw AttractorError.parseError("Invalid float literal: \(str)")
            }
            return .floatLiteral(val)
        }

        // Check for duration suffix
        if pos < chars.count {
            if chars[pos] == "m" && pos + 1 < chars.count && chars[pos + 1] == "s" {
                pos += 2
                let str = String(chars[start..<pos])
                return .durationLiteral(str)
            }
            if chars[pos] == "s" || chars[pos] == "m" || chars[pos] == "h" || chars[pos] == "d" {
                // Make sure the next char after the unit is not a letter (to avoid misreading identifiers)
                if pos + 1 >= chars.count || !chars[pos + 1].isLetter {
                    pos += 1
                    let str = String(chars[start..<pos])
                    return .durationLiteral(str)
                }
            }
        }

        let str = String(chars[start..<pos])
        guard let val = Int(str) else {
            throw AttractorError.parseError("Invalid integer literal: \(str)")
        }
        return .integerLiteral(val)
    }

    private mutating func readIdentifierOrKeyword() -> DOTToken {
        let start = pos
        while pos < chars.count && (chars[pos].isLetter || chars[pos].isNumber || chars[pos] == "_") {
            pos += 1
        }
        let word = String(chars[start..<pos])

        switch word {
        case "digraph", "graph", "node", "edge", "subgraph", "strict":
            return .keyword(word)
        case "true":
            return .booleanLiteral(true)
        case "false":
            return .booleanLiteral(false)
        default:
            return .identifier(word)
        }
    }
}

// MARK: - Token Parser

struct DOTTokenParser {
    let tokens: [DOTToken]
    private var pos: Int = 0

    // Defaults that accumulate as we parse
    private var nodeDefaults: [String: AttributeValue] = [:]
    private var edgeDefaults: [String: AttributeValue] = [:]

    init(tokens: [DOTToken]) {
        self.tokens = tokens
    }

    private var current: DOTToken {
        pos < tokens.count ? tokens[pos] : .eof
    }

    private func peek(ahead: Int = 0) -> DOTToken {
        let idx = pos + ahead
        return idx < tokens.count ? tokens[idx] : .eof
    }

    @discardableResult
    private mutating func advance() -> DOTToken {
        let tok = current
        pos += 1
        return tok
    }

    private mutating func expect(_ expected: DOTToken) throws {
        guard current == expected else {
            throw AttractorError.parseError("Expected \(expected), got \(current)")
        }
        advance()
    }

    private mutating func expectKeyword(_ kw: String) throws {
        guard current == .keyword(kw) else {
            throw AttractorError.parseError("Expected keyword '\(kw)', got \(current)")
        }
        advance()
    }

    private mutating func consumeOptionalSemicolon() {
        if current == .semicolon { advance() }
    }

    // MARK: - Parse Graph

    mutating func parseGraph() throws -> Graph {
        // Reject strict modifier
        if current == .keyword("strict") {
            throw AttractorError.invalidDOT("'strict' modifier is not supported")
        }

        try expectKeyword("digraph")

        // Graph name/identifier
        let graphId: String
        switch current {
        case .identifier(let name):
            graphId = name
            advance()
        case .stringLiteral(let name):
            graphId = name
            advance()
        default:
            graphId = "pipeline"
        }

        try expect(.lbrace)

        let graph = Graph(id: graphId)
        try parseStatements(into: graph)

        try expect(.rbrace)

        // Reject multiple graphs
        if current != .eof {
            throw AttractorError.invalidDOT("Only one digraph per file is supported")
        }

        return graph
    }

    // MARK: - Parse Statements

    private mutating func parseStatements(into graph: Graph) throws {
        while current != .rbrace && current != .eof {
            try parseStatement(into: graph)
        }
    }

    private mutating func parseStatement(into graph: Graph) throws {
        switch current {
        case .keyword("graph"):
            try parseGraphAttrStmt(into: graph)
        case .keyword("node"):
            try parseNodeDefaults()
        case .keyword("edge"):
            try parseEdgeDefaults()
        case .keyword("subgraph"):
            try parseSubgraph(into: graph)
        case .identifier, .stringLiteral:
            try parseNodeOrEdgeStmt(into: graph)
        case .semicolon:
            advance()
        default:
            throw AttractorError.parseError("Unexpected token \(current) in statement position")
        }
    }

    // MARK: - Graph Attributes

    private mutating func parseGraphAttrStmt(into graph: Graph) throws {
        try expectKeyword("graph")
        if current == .lbracket {
            let attrs = try parseAttrBlock()
            for (key, value) in attrs {
                graph.rawAttributes[key] = value
            }
            applyGraphAttributes(attrs, to: graph)
        }
        consumeOptionalSemicolon()
    }

    private func applyGraphAttributes(_ attrs: [String: AttributeValue], to graph: Graph) {
        for (key, value) in attrs {
            switch key {
            case "goal":
                graph.attributes.goal = value.stringValue
            case "label":
                graph.attributes.label = value.stringValue
            case "model_stylesheet":
                graph.attributes.modelStylesheet = value.stringValue
            case "default_max_retry":
                graph.attributes.defaultMaxRetry = value.intValue ?? 50
            case "retry_target":
                graph.attributes.retryTarget = value.stringValue
            case "fallback_retry_target":
                graph.attributes.fallbackRetryTarget = value.stringValue
            case "default_fidelity":
                graph.attributes.defaultFidelity = value.stringValue
            case "stack.child_dotfile":
                graph.attributes.stackChildDotfile = value.stringValue
            case "stack.child_workdir":
                graph.attributes.stackChildWorkdir = value.stringValue
            case "tool_hooks.pre":
                graph.attributes.toolHooksPre = value.stringValue
            case "tool_hooks.post":
                graph.attributes.toolHooksPost = value.stringValue
            default:
                break
            }
        }
    }

    // MARK: - Node/Edge Defaults

    private mutating func parseNodeDefaults() throws {
        try expectKeyword("node")
        if current == .lbracket {
            let attrs = try parseAttrBlock()
            for (key, value) in attrs {
                nodeDefaults[key] = value
            }
        }
        consumeOptionalSemicolon()
    }

    private mutating func parseEdgeDefaults() throws {
        try expectKeyword("edge")
        if current == .lbracket {
            let attrs = try parseAttrBlock()
            for (key, value) in attrs {
                edgeDefaults[key] = value
            }
        }
        consumeOptionalSemicolon()
    }

    // MARK: - Subgraph

    private mutating func parseSubgraph(into graph: Graph) throws {
        try expectKeyword("subgraph")

        // Optional subgraph name
        var subgraphLabel: String? = nil
        switch current {
        case .identifier(let name):
            advance()
            // Cluster subgraphs may have label-derived classes
            if name.hasPrefix("cluster_") {
                subgraphLabel = String(name.dropFirst("cluster_".count))
            }
        case .stringLiteral(let name):
            advance()
            subgraphLabel = name
        default:
            break
        }

        try expect(.lbrace)

        // Save current defaults so subgraph defaults are scoped
        let savedNodeDefaults = nodeDefaults
        let savedEdgeDefaults = edgeDefaults

        // Track which nodes are added in this subgraph
        let existingNodeIds = Set(graph.nodes.keys)

        // Parse subgraph body
        while current != .rbrace && current != .eof {
            // Check for top-level key=value in subgraph (e.g., label = "...")
            if case .identifier(let key) = current, peek(ahead: 1) == .equals {
                advance() // consume key
                advance() // consume =
                let value = try parseValue()
                consumeOptionalSemicolon()
                if key == "label" {
                    subgraphLabel = value.stringValue
                }
                continue
            }
            try parseStatement(into: graph)
        }

        try expect(.rbrace)

        // Derive class from subgraph label and apply to nodes in this subgraph
        if let label = subgraphLabel {
            let derivedClass = deriveClassName(from: label)
            let newNodeIds = Set(graph.nodes.keys).subtracting(existingNodeIds)
            for nodeId in newNodeIds {
                if let node = graph.nodes[nodeId] {
                    if node.cssClass.isEmpty {
                        node.cssClass = derivedClass
                    } else if !node.cssClass.split(separator: ",").map(String.init).contains(derivedClass) {
                        node.cssClass += ",\(derivedClass)"
                    }
                }
            }
        }

        // Restore defaults
        nodeDefaults = savedNodeDefaults
        edgeDefaults = savedEdgeDefaults

        consumeOptionalSemicolon()
    }

    private func deriveClassName(from label: String) -> String {
        String(label.lowercased().map { $0 == " " ? "-" : $0 })
            .filter { $0.isLetter || $0.isNumber || $0 == "-" }
    }

    // MARK: - Node or Edge Statement

    private mutating func parseNodeOrEdgeStmt(into graph: Graph) throws {
        let firstId = try parseNodeId()

        // Check for top-level key=value (e.g., rankdir=LR)
        if current == .equals {
            advance()
            let value = try parseValue()
            graph.rawAttributes[firstId] = value
            applyGraphAttributes([firstId: value], to: graph)
            consumeOptionalSemicolon()
            return
        }

        // Check for edge chain
        if current == .arrow {
            var nodeIds = [firstId]
            while current == .arrow {
                advance()
                let nextId = try parseNodeId()
                nodeIds.append(nextId)
            }

            let attrs = current == .lbracket ? try parseAttrBlock() : [:]

            // Ensure all nodes in the chain exist
            for nodeId in nodeIds {
                if graph.nodes[nodeId] == nil {
                    let node = makeNode(id: nodeId, explicitAttrs: [:])
                    graph.nodes[nodeId] = node
                }
            }

            // Create edges for each pair
            for i in 0..<(nodeIds.count - 1) {
                let edge = makeEdge(from: nodeIds[i], to: nodeIds[i + 1], explicitAttrs: attrs)
                graph.edges.append(edge)
            }

            consumeOptionalSemicolon()
            return
        }

        // Node statement
        let attrs = current == .lbracket ? try parseAttrBlock() : [:]
        let node: Node
        if let existing = graph.nodes[firstId] {
            // Update existing node with explicit attributes
            applyNodeAttributes(attrs, to: existing)
            node = existing
        } else {
            node = makeNode(id: firstId, explicitAttrs: attrs)
        }
        graph.nodes[firstId] = node
        consumeOptionalSemicolon()
    }

    private mutating func parseNodeId() throws -> String {
        switch current {
        case .identifier(let id):
            advance()
            return id
        case .stringLiteral(let id):
            advance()
            return id
        default:
            throw AttractorError.parseError("Expected node identifier, got \(current)")
        }
    }

    // MARK: - Attribute Block

    private mutating func parseAttrBlock() throws -> [String: AttributeValue] {
        try expect(.lbracket)
        var attrs: [String: AttributeValue] = [:]

        while current != .rbracket && current != .eof {
            let key = try parseAttrKey()
            try expect(.equals)
            let value = try parseValue()
            attrs[key] = value

            if current == .comma {
                advance()
            }
        }

        try expect(.rbracket)
        return attrs
    }

    private mutating func parseAttrKey() throws -> String {
        var key: String
        switch current {
        case .identifier(let id):
            key = id
            advance()
        case .keyword(let kw):
            // Allow keywords like "graph", "node", "edge" as attribute keys
            key = kw
            advance()
        default:
            throw AttractorError.parseError("Expected attribute key, got \(current)")
        }

        // Handle qualified identifiers (e.g., stack.child_dotfile)
        while current == .dot {
            advance()
            switch current {
            case .identifier(let id):
                key += ".\(id)"
                advance()
            case .keyword(let kw):
                key += ".\(kw)"
                advance()
            default:
                throw AttractorError.parseError("Expected identifier after '.' in qualified key, got \(current)")
            }
        }

        return key
    }

    private mutating func parseValue() throws -> AttributeValue {
        switch current {
        case .stringLiteral(let s):
            advance()
            return .string(s)
        case .integerLiteral(let i):
            advance()
            return .integer(i)
        case .floatLiteral(let f):
            advance()
            return .float(f)
        case .booleanLiteral(let b):
            advance()
            return .boolean(b)
        case .durationLiteral(let s):
            advance()
            guard let dv = DurationValue.parse(s) else {
                throw AttractorError.parseError("Invalid duration value: \(s)")
            }
            return .duration(dv)
        case .identifier(let s):
            // Bare identifier used as a value (e.g., shape=box, rankdir=LR)
            advance()
            return .string(s)
        case .keyword(let s):
            // Keywords used as values
            advance()
            return .string(s)
        default:
            throw AttractorError.parseError("Expected value, got \(current)")
        }
    }

    // MARK: - Node/Edge Construction

    private func makeNode(id: String, explicitAttrs: [String: AttributeValue]) -> Node {
        // Merge defaults with explicit attributes (explicit wins)
        var merged = nodeDefaults
        for (key, value) in explicitAttrs {
            merged[key] = value
        }

        let node = Node(id: id)
        applyNodeAttributes(merged, to: node)
        node.rawAttributes = merged
        return node
    }

    private func applyNodeAttributes(_ attrs: [String: AttributeValue], to node: Node) {
        for (key, value) in attrs {
            switch key {
            case "label":
                node.label = value.stringValue
            case "shape":
                node.shape = value.stringValue
            case "type":
                node.type = value.stringValue
            case "prompt":
                node.prompt = value.stringValue
            case "max_retries":
                node.maxRetries = value.intValue ?? 0
            case "goal_gate":
                node.goalGate = value.boolValue ?? false
            case "retry_target":
                node.retryTarget = value.stringValue
            case "fallback_retry_target":
                node.fallbackRetryTarget = value.stringValue
            case "fidelity":
                node.fidelity = value.stringValue
            case "thread_id":
                node.threadId = value.stringValue
            case "class":
                node.cssClass = value.stringValue
            case "timeout":
                if case .duration(let dv) = value {
                    node.timeout = dv.asDuration
                } else if let dv = DurationValue.parse(value.stringValue) {
                    node.timeout = dv.asDuration
                }
            case "llm_model":
                node.llmModel = value.stringValue
            case "llm_provider":
                node.llmProvider = value.stringValue
            case "reasoning_effort":
                node.reasoningEffort = value.stringValue
            case "auto_status":
                node.autoStatus = value.boolValue ?? false
            case "allow_partial":
                node.allowPartial = value.boolValue ?? false
            default:
                break
            }
            node.rawAttributes[key] = value
        }
    }

    private func makeEdge(from: String, to: String, explicitAttrs: [String: AttributeValue]) -> Edge {
        var merged = edgeDefaults
        for (key, value) in explicitAttrs {
            merged[key] = value
        }

        var edge = Edge(from: from, to: to)
        for (key, value) in merged {
            switch key {
            case "label":
                edge.label = value.stringValue
            case "condition":
                edge.condition = value.stringValue
            case "weight":
                edge.weight = value.intValue ?? 0
            case "fidelity":
                edge.fidelity = value.stringValue
            case "thread_id":
                edge.threadId = value.stringValue
            case "loop_restart":
                edge.loopRestart = value.boolValue ?? false
            default:
                break
            }
        }
        edge.rawAttributes = merged
        return edge
    }
}
