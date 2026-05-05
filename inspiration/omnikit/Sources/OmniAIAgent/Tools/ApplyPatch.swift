import Foundation

// MARK: - apply_patch (OpenAI v4a format)

public func applyPatchTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "apply_patch",
            description: "Apply code patches to files using the Codex patch format.",
            parameters: [
                "type": "object",
                "properties": [
                    "patch": ["type": "string", "description": "The patch content in Codex format (*** Begin Patch ... *** End Patch)"],
                ] as [String: Any],
                "required": ["patch"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let patch = args["patch"] as? String else {
                throw ToolError.validationError("patch is required")
            }
            return try await applyV4aPatch(patch, env: env)
        }
    )
}

// MARK: - V4a Patch Parser and Applicator

enum PatchOperation {
    case addFile(path: String, lines: [String])
    case deleteFile(path: String)
    case updateFile(path: String, moveTo: String?, chunks: [UpdateChunk])
}

struct UpdateChunk {
    var contextHint: String?
    var oldLines: [String]   // context + delete lines (what to find in file)
    var newLines: [String]   // context + add lines (what to replace with)
    var isEndOfFile: Bool
}

func parseV4aPatch(_ patch: String) throws -> [PatchOperation] {
    var text = patch.trimmingCharacters(in: .whitespacesAndNewlines)

    // Strip heredoc wrappers (lenient mode, matching codex-rs)
    for prefix in ["<<'EOF'", "<<\"EOF\"", "<<EOF"] {
        if text.hasPrefix(prefix) {
            text = String(text.dropFirst(prefix.count))
            if text.hasSuffix("EOF") {
                text = String(text.dropLast(3))
            }
            text = text.trimmingCharacters(in: .whitespacesAndNewlines)
            break
        }
    }

    let lines = text.components(separatedBy: "\n")
    var idx = 0
    var operations: [PatchOperation] = []

    // Skip to "*** Begin Patch"
    while idx < lines.count && !lines[idx].trimmingCharacters(in: .whitespaces).hasPrefix("*** Begin Patch") {
        idx += 1
    }
    guard idx < lines.count else {
        throw ToolError.patchError("Missing '*** Begin Patch' header")
    }
    idx += 1

    while idx < lines.count {
        let line = lines[idx].trimmingCharacters(in: .whitespaces)

        if line.hasPrefix("*** End Patch") {
            break
        }

        if line.hasPrefix("*** Add File: ") {
            let path = String(line.dropFirst("*** Add File: ".count))
            idx += 1
            var addedLines: [String] = []
            while idx < lines.count && !lines[idx].hasPrefix("***") {
                if lines[idx].hasPrefix("+") {
                    addedLines.append(String(lines[idx].dropFirst()))
                }
                idx += 1
            }
            operations.append(.addFile(path: path, lines: addedLines))
            continue
        }

        if line.hasPrefix("*** Delete File: ") {
            let path = String(line.dropFirst("*** Delete File: ".count))
            idx += 1
            operations.append(.deleteFile(path: path))
            continue
        }

        if line.hasPrefix("*** Update File: ") {
            let path = String(line.dropFirst("*** Update File: ".count))
            idx += 1

            var moveTo: String? = nil
            if idx < lines.count && lines[idx].trimmingCharacters(in: .whitespaces).hasPrefix("*** Move to: ") {
                moveTo = String(lines[idx].trimmingCharacters(in: .whitespaces).dropFirst("*** Move to: ".count))
                idx += 1
            }

            var chunks: [UpdateChunk] = []
            var allowMissingContext = true  // first chunk can omit @@ header

            while idx < lines.count && !lines[idx].hasPrefix("*** ") {
                var contextHint: String? = nil

                if lines[idx].hasPrefix("@@") {
                    let hintLine = lines[idx]
                    if hintLine.hasPrefix("@@ ") {
                        contextHint = String(hintLine.dropFirst(3))
                    }
                    // else just "@@" with no hint text
                    idx += 1
                } else if !allowMissingContext {
                    break
                }
                allowMissingContext = false

                var oldLines: [String] = []
                var newLines: [String] = []
                var isEndOfFile = false
                var hasDiffLines = false

                while idx < lines.count {
                    let hLine = lines[idx]
                    if hLine.hasPrefix("@@ ") || hLine == "@@" || hLine.hasPrefix("*** ") {
                        break
                    }
                    if hLine.hasPrefix("*** End of File") {
                        isEndOfFile = true
                        idx += 1
                        break
                    }
                    if hLine.hasPrefix(" ") {
                        let content = String(hLine.dropFirst())
                        oldLines.append(content)
                        newLines.append(content)
                        hasDiffLines = true
                    } else if hLine.hasPrefix("-") {
                        oldLines.append(String(hLine.dropFirst()))
                        hasDiffLines = true
                    } else if hLine.hasPrefix("+") {
                        newLines.append(String(hLine.dropFirst()))
                        hasDiffLines = true
                    } else if hLine.isEmpty {
                        // Empty line treated as empty context line
                        oldLines.append("")
                        newLines.append("")
                        hasDiffLines = true
                    } else if hasDiffLines {
                        // Unrecognized line after diff content — stop this chunk
                        break
                    } else {
                        // Unrecognized line before any diff content — skip
                        idx += 1
                        continue
                    }
                    idx += 1
                }

                if hasDiffLines || contextHint != nil {
                    chunks.append(UpdateChunk(
                        contextHint: contextHint,
                        oldLines: oldLines,
                        newLines: newLines,
                        isEndOfFile: isEndOfFile
                    ))
                }
            }

            guard !chunks.isEmpty else {
                throw ToolError.patchError("Update file hunk for path '\(path)' is empty")
            }

            operations.append(.updateFile(path: path, moveTo: moveTo, chunks: chunks))
            continue
        }

        idx += 1
    }

    return operations
}

func applyV4aPatch(_ patch: String, env: ExecutionEnvironment) async throws -> String {
    let operations = try parseV4aPatch(patch)
    var results: [String] = []

    for op in operations {
        switch op {
        case .addFile(let path, let addedLines):
            let content = addedLines.joined(separator: "\n")
            try await env.writeFile(path: path, content: content)
            results.append("Created: \(path)")

        case .deleteFile(let path):
            try deleteFileAtPath(path, env: env)
            results.append("Deleted: \(path)")

        case .updateFile(let path, let moveTo, let chunks):
            let rawContent = try await env.readFile(path: path, offset: nil, limit: nil)
            let content = stripPatchLineNumbers(rawContent)
            var fileLines = content.components(separatedBy: "\n")

            // Pop trailing empty element from final newline (matching codex-rs)
            if fileLines.last == "" {
                fileLines.removeLast()
            }

            // Compute all replacements forward, then apply in reverse
            let replacements = try computeReplacements(fileLines: fileLines, chunks: chunks)
            fileLines = applyReplacements(fileLines: fileLines, replacements: replacements)

            // Ensure trailing newline (codex-rs always adds one)
            if fileLines.last != "" {
                fileLines.append("")
            }

            let newContent = fileLines.joined(separator: "\n")
            let targetPath = moveTo ?? path

            if let moveTo = moveTo, moveTo != path {
                try await env.writeFile(path: moveTo, content: newContent)
                try deleteFileAtPath(path, env: env)
                results.append("Updated and moved: \(path) -> \(moveTo)")
            } else {
                try await env.writeFile(path: targetPath, content: newContent)
                results.append("Updated: \(targetPath)")
            }
        }
    }

    return results.joined(separator: "\n")
}

// MARK: - Replacement Computation (matching codex-rs compute_replacements)

private struct Replacement {
    var startIndex: Int
    var oldLength: Int
    var newLines: [String]
}

private func computeReplacements(fileLines: [String], chunks: [UpdateChunk]) throws -> [Replacement] {
    var replacements: [Replacement] = []
    var lineIndex = 0

    for chunk in chunks {
        // If context hint is provided, seek to that line first
        if let hint = chunk.contextHint, !hint.isEmpty {
            if let hintIdx = seekSequence(
                lines: fileLines,
                pattern: [hint],
                startIndex: lineIndex,
                anchorEnd: false
            ) {
                lineIndex = hintIdx + 1
            }
            // If hint not found, continue from current position (best effort)
        }

        if chunk.oldLines.isEmpty {
            // Pure addition — insert before the trailing position
            let insertIdx = fileLines.count
            replacements.append(Replacement(startIndex: insertIdx, oldLength: 0, newLines: chunk.newLines))
        } else {
            // Find oldLines sequence in file starting from lineIndex
            var found = seekSequence(
                lines: fileLines,
                pattern: chunk.oldLines,
                startIndex: lineIndex,
                anchorEnd: chunk.isEndOfFile
            )

            // Retry without trailing empty line (codex-rs fallback)
            if found == nil && chunk.oldLines.last == "" && chunk.oldLines.count > 1 {
                let trimmed = Array(chunk.oldLines.dropLast())
                found = seekSequence(
                    lines: fileLines,
                    pattern: trimmed,
                    startIndex: lineIndex,
                    anchorEnd: chunk.isEndOfFile
                )
                if let f = found {
                    // Use the trimmed pattern length
                    replacements.append(Replacement(
                        startIndex: f,
                        oldLength: trimmed.count,
                        newLines: chunk.newLines.last == "" ? Array(chunk.newLines.dropLast()) : chunk.newLines
                    ))
                    lineIndex = f + trimmed.count
                    continue
                }
            }

            guard let startIdx = found else {
                let preview = chunk.oldLines.prefix(3).joined(separator: "\\n")
                throw ToolError.patchError("Could not find matching context for chunk: \(chunk.contextHint ?? preview)")
            }

            replacements.append(Replacement(
                startIndex: startIdx,
                oldLength: chunk.oldLines.count,
                newLines: chunk.newLines
            ))
            lineIndex = startIdx + chunk.oldLines.count
        }
    }

    return replacements.sorted { $0.startIndex < $1.startIndex }
}

private func applyReplacements(fileLines: [String], replacements: [Replacement]) -> [String] {
    var result = fileLines
    // Apply in reverse to preserve indices
    for replacement in replacements.reversed() {
        let range = replacement.startIndex..<min(replacement.startIndex + replacement.oldLength, result.count)
        result.replaceSubrange(range, with: replacement.newLines)
    }
    return result
}

// MARK: - Fuzzy Line Matching (matching codex-rs seek_sequence)

/// 4-pass fuzzy sequence matching, matching codex-rs behavior:
/// 1. Exact match
/// 2. Trailing whitespace trimmed
/// 3. Fully trimmed
/// 4. Unicode normalization (smart quotes, dashes → ASCII)
private func seekSequence(
    lines: [String],
    pattern: [String],
    startIndex: Int,
    anchorEnd: Bool
) -> Int? {
    guard !pattern.isEmpty else { return startIndex }
    guard pattern.count <= lines.count else { return nil }

    let passes: [(String) -> String] = [
        { $0 },                                                    // pass 1: exact
        { $0.trimmingTrailingWhitespace() },                       // pass 2: rstrip
        { $0.trimmingCharacters(in: .whitespaces) },               // pass 3: full trim
        { normalizeUnicode($0).trimmingCharacters(in: .whitespaces) },  // pass 4: unicode normalize + trim
    ]

    for normalize in passes {
        let normalizedPattern = pattern.map(normalize)

        // If anchorEnd, try from the end first
        if anchorEnd {
            let endStart = lines.count - pattern.count
            if endStart >= 0 {
                let slice = lines[endStart...].map(normalize)
                if Array(slice) == normalizedPattern {
                    return endStart
                }
            }
        }

        // Forward search from startIndex
        let searchStart = max(0, startIndex)
        let searchEnd = lines.count - pattern.count
        for i in searchStart...max(searchStart, searchEnd) {
            let slice = (i..<(i + pattern.count)).map { normalize(lines[$0]) }
            if slice == normalizedPattern {
                return i
            }
        }
    }

    return nil
}

/// Normalize common Unicode punctuation to ASCII equivalents (matching codex-rs).
private func normalizeUnicode(_ s: String) -> String {
    var result = s
    // Unicode dashes → ASCII hyphen
    for dash in ["\u{2010}", "\u{2011}", "\u{2012}", "\u{2013}", "\u{2014}", "\u{2015}", "\u{2212}"] {
        result = result.replacing(dash, with: "-")
    }
    // Smart single quotes → ASCII apostrophe
    for q in ["\u{2018}", "\u{2019}", "\u{201A}", "\u{201B}"] {
        result = result.replacing(q, with: "'")
    }
    // Smart double quotes → ASCII quote
    for q in ["\u{201C}", "\u{201D}", "\u{201E}", "\u{201F}"] {
        result = result.replacing(q, with: "\"")
    }
    // Non-breaking and typographic spaces → regular space
    for sp in ["\u{00A0}", "\u{2002}", "\u{2003}", "\u{2009}", "\u{200A}"] {
        result = result.replacing(sp, with: " ")
    }
    return result
}

private func deleteFileAtPath(_ path: String, env: ExecutionEnvironment) throws {
    let resolved: String
    if path.hasPrefix("/") {
        resolved = path
    } else {
        resolved = (env.workingDirectory() as NSString).appendingPathComponent(path)
    }
    try FileManager.default.removeItem(atPath: resolved)
}

private func stripPatchLineNumbers(_ input: String) -> String {
    let lines = input.components(separatedBy: "\n")
    return lines.map { line in
        if let pipeIdx = line.firstIndex(of: "|") {
            let prefix = line[line.startIndex..<pipeIdx]
            if prefix.trimmingCharacters(in: .whitespaces).allSatisfy({ $0.isNumber }) {
                let afterPipe = line.index(after: pipeIdx)
                if afterPipe < line.endIndex && line[afterPipe] == " " {
                    return String(line[line.index(after: afterPipe)...])
                }
                return String(line[afterPipe...])
            }
        }
        return line
    }.joined(separator: "\n")
}

private extension String {
    func trimmingTrailingWhitespace() -> String {
        var end = endIndex
        while end > startIndex {
            let prev = index(before: end)
            if self[prev].isWhitespace {
                end = prev
            } else {
                break
            }
        }
        return String(self[startIndex..<end])
    }
}
