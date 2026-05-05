import Foundation

public enum ApplyDiffMode: String, Sendable {
    case `default`
    case create
}

public struct ApplyDiffChunk: Sendable, Equatable {
    public var origIndex: Int
    public var deleteLines: [String]
    public var insertLines: [String]

    public init(origIndex: Int, deleteLines: [String], insertLines: [String]) {
        self.origIndex = origIndex
        self.deleteLines = deleteLines
        self.insertLines = insertLines
    }
}

public func applyDiff(_ input: String, diff: String, mode: ApplyDiffMode = .default) throws -> String {
    let newline = input.contains("\r\n") ? "\r\n" : "\n"
    let diffLines = _normalizeDiffLines(diff)
    switch mode {
    case .create:
        return diffLines.compactMap { line in
            guard line.hasPrefix("+") else { return nil }
            return String(line.dropFirst())
        }.joined(separator: newline)
    case .default:
        let normalizedInput = input.replacing("\r\n", with: "\n")
        let chunks = try _parseUpdateDiff(diffLines, input: normalizedInput)
        let output = try _applyChunks(normalizedInput, chunks)
        return newline == "\n" ? output : output.replacing("\n", with: "\r\n")
    }
}

private func _normalizeDiffLines(_ diff: String) -> [String] {
    var lines = diff.replacing("\r\n", with: "\n").components(separatedBy: "\n")
    if lines.last == "" { lines.removeLast() }
    return lines
}

private func _parseUpdateDiff(_ lines: [String], input: String) throws -> [ApplyDiffChunk] {
    let inputLines = input.components(separatedBy: "\n")
    var index = 0
    var cursor = 0
    var chunks: [ApplyDiffChunk] = []

    while index < lines.count {
        let line = lines[index]
        if line.hasPrefix("*** End Patch") || line.hasPrefix("*** Update File:") || line.hasPrefix("*** Delete File:") || line.hasPrefix("*** Add File:") {
            break
        }

        var anchor: String? = nil
        if line.hasPrefix("@@ ") {
            anchor = String(line.dropFirst(3))
            index += 1
        } else if line == "@@" {
            anchor = nil
            index += 1
        }

        if let anchor, !anchor.isEmpty {
            if let found = inputLines[cursor...].firstIndex(of: anchor) {
                cursor = found + 1
            }
        }

        let result = try _readSection(lines: lines, startIndex: index)
        index = result.nextIndex
        guard let found = _findContext(inputLines, context: result.context, startIndex: cursor, eof: result.eof) else {
            throw AgentsError(message: "Invalid diff context while applying patch.")
        }
        cursor = found + result.context.count
        for chunk in result.chunks {
            chunks.append(ApplyDiffChunk(origIndex: chunk.origIndex + found, deleteLines: chunk.deleteLines, insertLines: chunk.insertLines))
        }
    }

    return chunks
}

private struct _SectionResult {
    var context: [String]
    var chunks: [ApplyDiffChunk]
    var nextIndex: Int
    var eof: Bool
}

private func _readSection(lines: [String], startIndex: Int) throws -> _SectionResult {
    var context: [String] = []
    var deleteLines: [String] = []
    var insertLines: [String] = []
    var chunks: [ApplyDiffChunk] = []
    var index = startIndex
    var mode: Character = " "

    func flushChunk() {
        guard !deleteLines.isEmpty || !insertLines.isEmpty else { return }
        chunks.append(ApplyDiffChunk(origIndex: context.count - deleteLines.count, deleteLines: deleteLines, insertLines: insertLines))
        deleteLines.removeAll(keepingCapacity: true)
        insertLines.removeAll(keepingCapacity: true)
    }

    while index < lines.count {
        let raw = lines[index]
        if raw.hasPrefix("@@") || raw.hasPrefix("*** End Patch") || raw.hasPrefix("*** Update File:") || raw.hasPrefix("*** Delete File:") || raw.hasPrefix("*** Add File:") || raw.hasPrefix("*** End of File") {
            break
        }
        if raw == "***" { break }
        let normalized = raw.isEmpty ? " " : raw
        guard let prefix = normalized.first, ["+", "-", " "].contains(prefix) else {
            throw AgentsError(message: "Invalid diff line: \(raw)")
        }
        index += 1
        if prefix == " ", mode != " " { flushChunk() }
        mode = prefix
        let content = String(normalized.dropFirst())
        switch prefix {
        case "-":
            deleteLines.append(content)
            context.append(content)
        case "+":
            insertLines.append(content)
        default:
            context.append(content)
        }
    }
    flushChunk()

    return _SectionResult(
        context: context,
        chunks: chunks,
        nextIndex: index,
        eof: index < lines.count && lines[index] == "*** End of File"
    )
}

private func _findContext(_ lines: [String], context: [String], startIndex: Int, eof: Bool) -> Int? {
    if context.isEmpty {
        return eof ? lines.count : startIndex
    }
    let maxIndex = lines.count - context.count
    guard maxIndex >= 0 else { return nil }
    if eof {
        return Array(lines.suffix(context.count)) == context ? lines.count - context.count : nil
    }
    for candidate in startIndex...maxIndex {
        if Array(lines[candidate..<(candidate + context.count)]) == context {
            return candidate
        }
    }
    return nil
}

private func _applyChunks(_ input: String, _ chunks: [ApplyDiffChunk]) throws -> String {
    var lines = input.components(separatedBy: "\n")
    var offset = 0
    for chunk in chunks {
        let start = chunk.origIndex + offset
        let end = start + chunk.deleteLines.count
        guard start >= 0, end <= lines.count else {
            throw AgentsError(message: "Diff chunk index out of bounds.")
        }
        if !chunk.deleteLines.isEmpty {
            let existing = Array(lines[start..<end])
            guard existing == chunk.deleteLines else {
                throw AgentsError(message: "Diff delete context mismatch.")
            }
        }
        lines.replaceSubrange(start..<end, with: chunk.insertLines)
        offset += chunk.insertLines.count - chunk.deleteLines.count
    }
    return lines.joined(separator: "\n")
}
