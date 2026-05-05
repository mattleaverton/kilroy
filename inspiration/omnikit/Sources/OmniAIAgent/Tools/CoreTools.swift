import Foundation

// MARK: - read_file

public func readFileTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "read_file",
            description: "Read a file from the filesystem. Returns line-numbered content.",
            parameters: [
                "type": "object",
                "properties": [
                    "file_path": ["type": "string", "description": "Absolute path to the file"],
                    "offset": ["type": "integer", "description": "1-based line number to start reading from"],
                    "limit": ["type": "integer", "description": "Maximum number of lines to read (default: 2000)"],
                ] as [String: Any],
                "required": ["file_path"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let filePath = args["file_path"] as? String else {
                throw ToolError.validationError("file_path is required")
            }
            let offset = intValue(args["offset"])
            let limit = intValue(args["limit"])
            return try await env.readFile(path: filePath, offset: offset, limit: limit)
        }
    )
}

// MARK: - write_file

public func writeFileTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "write_file",
            description: "Write content to a file. Creates the file and parent directories if needed.",
            parameters: [
                "type": "object",
                "properties": [
                    "file_path": ["type": "string", "description": "Absolute path to the file"],
                    "content": ["type": "string", "description": "The full file content to write"],
                ] as [String: Any],
                "required": ["file_path", "content"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let filePath = args["file_path"] as? String else {
                throw ToolError.validationError("file_path is required")
            }
            guard let content = args["content"] as? String else {
                throw ToolError.validationError("content is required")
            }
            try await env.writeFile(path: filePath, content: content)
            return "Successfully wrote \(content.utf8.count) bytes to \(filePath)"
        }
    )
}

// MARK: - edit_file (Anthropic-native: old_string/new_string)

public func editFileTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "edit_file",
            description: "Replace an exact string occurrence in a file. The old_string must be unique in the file unless replace_all is true.",
            parameters: [
                "type": "object",
                "properties": [
                    "file_path": ["type": "string", "description": "Absolute path to the file"],
                    "old_string": ["type": "string", "description": "Exact text to find in the file"],
                    "new_string": ["type": "string", "description": "Replacement text"],
                    "replace_all": ["type": "boolean", "description": "Replace all occurrences (default: false)"],
                ] as [String: Any],
                "required": ["file_path", "old_string", "new_string"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let filePath = args["file_path"] as? String else {
                throw ToolError.validationError("file_path is required")
            }
            guard let oldString = args["old_string"] as? String else {
                throw ToolError.validationError("old_string is required")
            }
            guard let newString = args["new_string"] as? String else {
                throw ToolError.validationError("new_string is required")
            }
            let replaceAll = args["replace_all"] as? Bool ?? false

            let rawContent = try await env.readFile(path: filePath, offset: nil, limit: nil)
            // Strip line numbers from readFile output
            let content = stripLineNumbers(rawContent)

            guard content.contains(oldString) else {
                // Try fuzzy match with whitespace normalization
                let normalizedContent = normalizeWhitespace(content)
                let normalizedOld = normalizeWhitespace(oldString)
                if normalizedContent.contains(normalizedOld) {
                    throw ToolError.editConflict("Exact match not found, but a fuzzy match exists. The old_string may have whitespace differences. Please check the file content and try again with the exact text.")
                }
                throw ToolError.editConflict("old_string not found in file: \(filePath)")
            }

            if !replaceAll {
                let occurrences = content.components(separatedBy: oldString).count - 1
                if occurrences > 1 {
                    throw ToolError.editConflict("old_string matches \(occurrences) locations. Provide more context to make it unique, or set replace_all=true.")
                }
            }

            let newContent: String
            if replaceAll {
                newContent = content.replacing(oldString, with: newString)
                let count = content.components(separatedBy: oldString).count - 1
                try await env.writeFile(path: filePath, content: newContent)
                return "Replaced \(count) occurrence(s) in \(filePath)"
            } else {
                if let range = content.range(of: oldString) {
                    newContent = content.replacingCharacters(in: range, with: newString)
                } else {
                    throw ToolError.editConflict("old_string not found")
                }
                try await env.writeFile(path: filePath, content: newContent)
                return "Replaced 1 occurrence in \(filePath)"
            }
        }
    )
}

// MARK: - shell

public func shellTool(defaultTimeoutMs: Int = 10_000, maxTimeoutMs: Int = 600_000) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "shell",
            description: "Execute a shell command. Returns stdout, stderr, and exit code.",
            parameters: [
                "type": "object",
                "properties": [
                    "command": [
                        "oneOf": [
                            ["type": "string"],
                            ["type": "array", "items": ["type": "string"]],
                        ],
                        "description": "The command to run. Accepts either a shell string or argv array.",
                    ] as [String: Any],
                    "timeout_ms": ["type": "integer", "description": "Override default timeout in milliseconds"],
                    "workdir": ["type": "string", "description": "Optional working directory for command execution"],
                    "login": ["type": "boolean", "description": "If command is a string, run via login shell semantics (default: true)"],
                    "sandbox_permissions": ["type": "string", "description": "Compatibility field. Accepted and ignored."],
                    "justification": ["type": "string", "description": "Compatibility field. Accepted and ignored."],
                    "description": ["type": "string", "description": "Human-readable description of what this does"],
                ] as [String: Any],
                "required": ["command"],
            ] as [String: Any]
        ),
        executor: { args, env in
            let commandValue = args["command"]
            let command: String
            if let shellCommand = commandValue as? String {
                command = shellCommand
            } else if let argv = stringArrayValue(commandValue), !argv.isEmpty {
                command = shellJoin(argv)
            } else {
                throw ToolError.validationError("command is required")
            }

            var timeoutMs = defaultTimeoutMs
            if let override = intValue(args["timeout_ms"]) {
                timeoutMs = min(override, maxTimeoutMs)
            }
            let workdir = args["workdir"] as? String
            return try await coreExecuteCommand(
                env: env,
                command: command,
                timeoutMs: timeoutMs,
                workdir: workdir,
                emitOutputDelta: { _ in }
            )
        },
        streamingExecutor: { args, env, emitOutputDelta in
            let commandValue = args["command"]
            let command: String
            if let shellCommand = commandValue as? String {
                command = shellCommand
            } else if let argv = stringArrayValue(commandValue), !argv.isEmpty {
                command = shellJoin(argv)
            } else {
                throw ToolError.validationError("command is required")
            }

            var timeoutMs = defaultTimeoutMs
            if let override = intValue(args["timeout_ms"]) {
                timeoutMs = min(override, maxTimeoutMs)
            }
            let workdir = args["workdir"] as? String

            return try await coreExecuteCommand(
                env: env,
                command: command,
                timeoutMs: timeoutMs,
                workdir: workdir,
                emitOutputDelta: emitOutputDelta
            )
        }
    )
}

private func coreExecuteCommand(
    env: ExecutionEnvironment,
    command: String,
    timeoutMs: Int,
    workdir: String?,
    emitOutputDelta: StreamingToolOutputEmitter
) async throws -> String {
    let result = try await env.execCommand(
        command: command,
        timeoutMs: timeoutMs,
        workingDir: workdir,
        envVars: nil
    )

    let combinedOutput = result.combinedOutput
    if !combinedOutput.isEmpty {
        await emitOutputDelta(combinedOutput)
    }

    var output = combinedOutput
    if result.timedOut {
        output += "\n\n[ERROR: Command timed out after \(timeoutMs)ms. Partial output is shown above. You can retry with a longer timeout by setting the timeout_ms parameter.]"
    }

    if result.exitCode != 0 && !result.timedOut {
        output += "\n[Exit code: \(result.exitCode)]"
    }

    output += "\n[Duration: \(result.durationMs)ms]"
    return output
}

// MARK: - grep

public func grepTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "grep",
            description: "Search file contents using regex patterns. Returns matching lines with file paths and line numbers.",
            parameters: [
                "type": "object",
                "properties": [
                    "pattern": ["type": "string", "description": "Regex pattern to search for"],
                    "path": ["type": "string", "description": "Directory or file to search (default: working directory)"],
                    "glob_filter": ["type": "string", "description": "File pattern filter (e.g., '*.py')"],
                    "case_insensitive": ["type": "boolean", "description": "Case insensitive search (default: false)"],
                    "max_results": ["type": "integer", "description": "Maximum results to return (default: 100)"],
                ] as [String: Any],
                "required": ["pattern"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let pattern = args["pattern"] as? String else {
                throw ToolError.validationError("pattern is required")
            }
            let path = args["path"] as? String ?? env.workingDirectory()
            let options = GrepOptions(
                globFilter: args["glob_filter"] as? String,
                caseInsensitive: args["case_insensitive"] as? Bool ?? false,
                maxResults: intValue(args["max_results"]) ?? 100
            )
            return try await env.grep(pattern: pattern, path: path, options: options)
        }
    )
}

// MARK: - glob

public func globTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "glob",
            description: "Find files matching a glob pattern. Returns list of matching file paths.",
            parameters: [
                "type": "object",
                "properties": [
                    "pattern": ["type": "string", "description": "Glob pattern (e.g., '**/*.ts')"],
                    "path": ["type": "string", "description": "Base directory (default: working directory)"],
                ] as [String: Any],
                "required": ["pattern"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let pattern = args["pattern"] as? String else {
                throw ToolError.validationError("pattern is required")
            }
            let path = args["path"] as? String ?? env.workingDirectory()
            let matches = try await env.glob(pattern: pattern, path: path)
            if matches.isEmpty {
                return "No files matching pattern '\(pattern)'"
            }
            return matches.joined(separator: "\n")
        }
    )
}

// MARK: - list_dir (Gemini-specific)

public func listDirTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "list_dir",
            description: "List the contents of a directory.",
            parameters: [
                "type": "object",
                "properties": [
                    "path": ["type": "string", "description": "Directory path to list"],
                    "dir_path": ["type": "string", "description": "Codex-compatible directory path key"],
                    "offset": ["type": "integer", "description": "1-based entry offset (default: 1)"],
                    "limit": ["type": "integer", "description": "Max entries to return (default: 200)"],
                    "depth": ["type": "integer", "description": "How many levels deep to list (default: 1)"],
                ] as [String: Any],
                "required": [],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let path = (args["path"] as? String) ?? (args["dir_path"] as? String) else {
                throw ToolError.validationError("path or dir_path is required")
            }
            let depth = max(1, intValue(args["depth"]) ?? 1)
            let offset = max(1, intValue(args["offset"]) ?? 1)
            let limit = max(1, intValue(args["limit"]) ?? 200)
            let entries = try await env.listDirectory(path: path, depth: depth)
            let start = min(offset - 1, entries.count)
            let end = min(start + limit, entries.count)
            return entries[start..<end].enumerated().map { index, entry in
                let suffix = entry.isDir ? "/" : ""
                let sizeStr = entry.size.map { " (\($0) bytes)" } ?? ""
                return "\(start + index + 1)\t\(entry.name)\(suffix)\(sizeStr)"
            }.joined(separator: "\n")
        }
    )
}

// MARK: - read_many_files (Gemini-specific)

public func readManyFilesTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "read_many_files",
            description: "Read multiple files at once. Returns contents of all specified files.",
            parameters: [
                "type": "object",
                "properties": [
                    "file_paths": [
                        "type": "array",
                        "items": ["type": "string"],
                        "description": "List of file paths to read",
                    ] as [String: Any],
                ] as [String: Any],
                "required": ["file_paths"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let filePaths = stringArrayValue(args["file_paths"]) else {
                throw ToolError.validationError("file_paths is required and must be an array of strings")
            }
            var results: [String] = []
            for path in filePaths {
                do {
                    let content = try await env.readFile(path: path, offset: nil, limit: nil)
                    results.append("=== \(path) ===\n\(content)")
                } catch {
                    results.append("=== \(path) ===\nError: \(error)")
                }
            }
            return results.joined(separator: "\n\n")
        }
    )
}

// MARK: - web_search (Gemini-specific)

public func webSearchTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "web_search",
            description: "Search the web for information. Returns search results with titles, URLs, and snippets.",
            parameters: [
                "type": "object",
                "properties": [
                    "query": ["type": "string", "description": "The search query"],
                    "num_results": ["type": "integer", "description": "Maximum results to return (default: 10)"],
                    "allowed_domains": [
                        "type": "array",
                        "items": ["type": "string"],
                        "description": "Restrict results to these domains",
                    ] as [String: Any],
                    "blocked_domains": [
                        "type": "array",
                        "items": ["type": "string"],
                        "description": "Exclude results from these domains",
                    ] as [String: Any],
                ] as [String: Any],
                "required": ["query"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let query = args["query"] as? String else {
                throw ToolError.validationError("query is required")
            }
            let numResults = intValue(args["num_results"]) ?? 10
            let allowedDomains = stringArrayValue(args["allowed_domains"])
            let blockedDomains = stringArrayValue(args["blocked_domains"])

            let results = try await WebSearchClient.search(
                query: query,
                allowedDomains: allowedDomains,
                blockedDomains: blockedDomains,
                maxResults: numResults
            )

            guard !results.isEmpty else {
                return "Search results for: \(query)\n\nNo results found."
            }

            var output = "Search results for: \(query)\n\n"
            for (index, result) in results.enumerated() {
                output += "\(index + 1). \(result.title)\n"
                output += "   \(result.url)\n"
                if !result.snippet.isEmpty {
                    output += "   \(result.snippet)\n"
                }
                output += "\n"
            }
            return output.trimmingCharacters(in: .whitespacesAndNewlines)
        }
    )
}

// MARK: - web_fetch (Gemini-specific)

public func webFetchTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "web_fetch",
            description: "Fetch content from a URL. Returns the page content as text.",
            parameters: [
                "type": "object",
                "properties": [
                    "url": ["type": "string", "description": "The URL to fetch content from"],
                    "prompt": ["type": "string", "description": "Optional extraction prompt for parity with Claude/Gemini tools"],
                    "max_chars": ["type": "integer", "description": "Max characters to return (default: 50,000)"],
                ] as [String: Any],
                "required": ["url"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let url = args["url"] as? String else {
                throw ToolError.validationError("url is required")
            }
            guard url.hasPrefix("http://") || url.hasPrefix("https://") else {
                throw ToolError.validationError("url must start with http:// or https://")
            }
            let prompt = args["prompt"] as? String
            let maxChars = max(1_000, min(intValue(args["max_chars"]) ?? 50_000, 200_000))

            let loader = WebFetchLoader(maxRedirects: 10, allowCrossHostRedirects: true, timeout: 30)
            let result = try await loader.fetch(url)

            let text = stripHTMLForToolOutput(result.content)
            let trimmed = text.count > maxChars ? String(text.prefix(maxChars)) + "\n\n[Content truncated...]" : text

            var output = ""
            if result.wasRedirected {
                output += "Fetched from: \(result.finalURL.absoluteString)\n(redirected from \(result.originalURL.absoluteString))\n\n"
            } else {
                output += "Fetched from: \(result.finalURL.absoluteString)\n\n"
            }
            if let prompt, !prompt.isEmpty {
                output += "Prompt: \(prompt)\n\n"
            }
            output += "Content:\n\(trimmed)"
            return output
        }
    )
}

// MARK: - Helpers

private func stripLineNumbers(_ input: String) -> String {
    // readFile returns "NNNN | content" format, strip the prefix
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

private func normalizeWhitespace(_ text: String) -> String {
    text.components(separatedBy: .whitespacesAndNewlines)
        .filter { !$0.isEmpty }
        .joined(separator: " ")
}

private func intValue(_ raw: Any?) -> Int? {
    switch raw {
    case let value as Int:
        return value
    case let value as Double:
        return Int(value)
    case let value as NSNumber:
        return value.intValue
    default:
        return nil
    }
}

private func stringArrayValue(_ raw: Any?) -> [String]? {
    if let value = raw as? [String] {
        return value
    }
    if let value = raw as? [Any] {
        let strings = value.compactMap { $0 as? String }
        return strings.count == value.count ? strings : nil
    }
    return nil
}

private func shellJoin(_ argv: [String]) -> String {
    argv.map(shellEscape).joined(separator: " ")
}

private func shellEscape(_ arg: String) -> String {
    if arg.isEmpty {
        return "''"
    }
    if arg.rangeOfCharacter(from: CharacterSet.alphanumerics.inverted.subtracting(CharacterSet(charactersIn: "_-./:"))) == nil {
        return arg
    }
    return "'" + arg.replacing("'", with: "'\\''") + "'"
}
