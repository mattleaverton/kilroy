import Foundation
import OmniAICore

// MARK: - Tool Name Constants (Gemini CLI parity)

public let GEMINI_GLOB_TOOL_NAME = "glob"
public let GEMINI_GREP_TOOL_NAME = "grep_search"
public let GEMINI_LS_TOOL_NAME = "list_directory"
public let GEMINI_READ_FILE_TOOL_NAME = "read_file"
public let GEMINI_SHELL_TOOL_NAME = "run_shell_command"
public let GEMINI_WRITE_FILE_TOOL_NAME = "write_file"
public let GEMINI_EDIT_TOOL_NAME = "replace"
public let GEMINI_WEB_SEARCH_TOOL_NAME = "google_web_search"
public let GEMINI_WRITE_TODOS_TOOL_NAME = "write_todos"
public let GEMINI_WEB_FETCH_TOOL_NAME = "web_fetch"
public let GEMINI_READ_MANY_FILES_TOOL_NAME = "read_many_files"
public let GEMINI_MEMORY_TOOL_NAME = "save_memory"
public let GEMINI_GET_INTERNAL_DOCS_TOOL_NAME = "get_internal_docs"
public let GEMINI_ACTIVATE_SKILL_TOOL_NAME = "activate_skill"
public let GEMINI_ASK_USER_TOOL_NAME = "ask_user"
public let GEMINI_EXIT_PLAN_MODE_TOOL_NAME = "exit_plan_mode"
public let GEMINI_ENTER_PLAN_MODE_TOOL_NAME = "enter_plan_mode"

// MARK: - glob

public func geminiGlobTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_GLOB_TOOL_NAME,
            description: "Efficiently finds files matching specific glob patterns (e.g., `src/**/*.ts`, `**/*.md`), returning absolute paths sorted by modification time (newest first). Ideal for quickly locating files based on their name or path structure, especially in large codebases.",
            parameters: [
                "type": "object",
                "properties": [
                    "pattern": [
                        "description": "The glob pattern to match against (e.g., '**/*.py', 'docs/*.md').",
                        "type": "string",
                    ],
                    "dir_path": [
                        "description": "Optional: The absolute path to the directory to search within. If omitted, searches the root directory.",
                        "type": "string",
                    ],
                    "case_sensitive": [
                        "description": "Optional: Whether the search should be case-sensitive. Defaults to false.",
                        "type": "boolean",
                    ],
                    "respect_git_ignore": [
                        "description": "Optional: Whether to respect .gitignore patterns when finding files. Only available in git repositories. Defaults to true.",
                        "type": "boolean",
                    ],
                    "respect_gemini_ignore": [
                        "description": "Optional: Whether to respect .geminiignore patterns when finding files. Defaults to true.",
                        "type": "boolean",
                    ],
                ] as [String: Any],
                "required": ["pattern"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let pattern = args["pattern"] as? String else {
                throw ToolError.validationError("pattern is required")
            }
            let basePath = (args["dir_path"] as? String) ?? env.workingDirectory()
            let matches = try await env.glob(pattern: pattern, path: basePath)
            return matches.isEmpty ? "No matching files found." : matches.joined(separator: "\n")
        }
    )
}

// MARK: - read_file

public func geminiReadFileTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_READ_FILE_TOOL_NAME,
            description: "Reads and returns the content of a specified file. If the file is large, the content will be truncated. The tool's response will clearly indicate if truncation has occurred and will provide details on how to read more of the file using the 'offset' and 'limit' parameters. Handles text, images (PNG, JPG, GIF, WEBP, SVG, BMP), audio files (MP3, WAV, AIFF, AAC, OGG, FLAC), and PDF files. For text files, it can read specific line ranges.",
            parameters: [
                "type": "object",
                "properties": [
                    "file_path": [
                        "description": "The path to the file to read.",
                        "type": "string",
                    ],
                    "offset": [
                        "description": "Optional: For text files, the 0-based line number to start reading from. Requires 'limit' to be set. Use for paginating through large files.",
                        "type": "number",
                    ],
                    "limit": [
                        "description": "Optional: For text files, maximum number of lines to read. Use with 'offset' to paginate through large files. If omitted, reads the entire file (if feasible, up to a default limit).",
                        "type": "number",
                    ],
                ] as [String: Any],
                "required": ["file_path"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let filePath = args["file_path"] as? String else {
                throw ToolError.validationError("file_path is required")
            }
            let offset = geminiInt(args["offset"])
            let limit = geminiInt(args["limit"])
            return try await env.readFile(path: filePath, offset: offset, limit: limit)
        }
    )
}

// MARK: - write_file

public func geminiWriteFileTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_WRITE_FILE_TOOL_NAME,
            description: """
Writes content to a specified file in the local filesystem.

      The user has the ability to modify `content`. If modified, this will be stated in the response.
""",
            parameters: [
                "type": "object",
                "properties": [
                    "file_path": [
                        "description": "The path to the file to write to.",
                        "type": "string",
                    ],
                    "content": [
                        "description": "The content to write to the file.",
                        "type": "string",
                    ],
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
            return "File written successfully: \(filePath)"
        }
    )
}

// MARK: - replace

public func geminiReplaceTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_EDIT_TOOL_NAME,
            description: """
Replaces text within a file. By default, replaces a single occurrence, but can replace multiple occurrences when `expected_replacements` is specified. This tool requires providing significant context around the change to ensure precise targeting. Always use the read_file tool to examine the file's current content before attempting a text replacement.
      
      The user has the ability to modify the `new_string` content. If modified, this will be stated in the response.
      
      Expectation for required parameters:
      1. `old_string` MUST be the exact literal text to replace (including all whitespace, indentation, newlines, and surrounding code etc.).
      2. `new_string` MUST be the exact literal text to replace `old_string` with (also including all whitespace, indentation, newlines, and surrounding code etc.). Ensure the resulting code is correct and idiomatic and that `old_string` and `new_string` are different.
      3. `instruction` is the detailed instruction of what needs to be changed. It is important to Make it specific and detailed so developers or large language models can understand what needs to be changed and perform the changes on their own if necessary. 
      4. NEVER escape `old_string` or `new_string`, that would break the exact literal text requirement.
      **Important:** If ANY of the above are not satisfied, the tool will fail. CRITICAL for `old_string`: Must uniquely identify the single instance to change. Include at least 3 lines of context BEFORE and AFTER the target text, matching whitespace and indentation precisely. If this string matches multiple locations, or does not match exactly, the tool will fail.
      5. Prefer to break down complex and long changes into multiple smaller atomic calls to this tool. Always check the content of the file after changes or not finding a string to match.
      **Multiple replacements:** Set `expected_replacements` to the number of occurrences you want to replace. The tool will replace ALL occurrences that match `old_string` exactly. Ensure the number of replacements matches your expectation.
""",
            parameters: [
                "type": "object",
                "properties": [
                    "file_path": [
                        "description": "The path to the file to modify.",
                        "type": "string",
                    ],
                    "instruction": [
                        "description": "A clear, semantic instruction for the code change, acting as a high-quality prompt for an expert LLM assistant. It must be self-contained and explain the goal of the change.",
                        "type": "string",
                    ],
                    "old_string": [
                        "description": "The exact literal text to replace, preferably unescaped. For single replacements (default), include at least 3 lines of context BEFORE and AFTER the target text, matching whitespace and indentation precisely. If this string is not the exact literal text (i.e. you escaped it) or does not match exactly, the tool will fail.",
                        "type": "string",
                    ],
                    "new_string": [
                        "description": "The exact literal text to replace `old_string` with, preferably unescaped. Provide the EXACT text. Ensure the resulting code is correct and idiomatic.",
                        "type": "string",
                    ],
                    "expected_replacements": [
                        "type": "number",
                        "description": "Number of replacements expected. Defaults to 1 if not specified. Use when you want to replace multiple occurrences.",
                        "minimum": 1,
                    ],
                ] as [String: Any],
                "required": ["file_path", "instruction", "old_string", "new_string"],
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
            guard oldString != newString else {
                throw ToolError.validationError("old_string and new_string must be different")
            }

            let expected = max(1, geminiInt(args["expected_replacements"]) ?? 1)
            let raw = try await env.readFile(path: filePath, offset: nil, limit: nil)
            let content = geminiStripLineNumbers(raw)

            let occurrences = content.components(separatedBy: oldString).count - 1
            guard occurrences > 0 else {
                throw ToolError.editConflict("old_string not found in file")
            }
            guard occurrences == expected else {
                throw ToolError.editConflict("Found \(occurrences) occurrences but expected \(expected)")
            }

            let updated = content.replacing(oldString, with: newString)
            try await env.writeFile(path: filePath, content: updated)
            return "Replaced \(occurrences) occurrence(s) in \(filePath)"
        }
    )
}

// MARK: - grep_search

public func geminiGrepSearchTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_GREP_TOOL_NAME,
            description: "Searches for a regular expression pattern within file contents. Max 100 matches.",
            parameters: [
                "type": "object",
                "properties": [
                    "pattern": [
                        "description": "The regular expression (regex) pattern to search for within file contents (e.g., 'function\\s+myFunction', 'import\\s+\\{.*\\}\\s+from\\s+.*').",
                        "type": "string",
                    ],
                    "dir_path": [
                        "description": "Optional: The absolute path to the directory to search within. If omitted, searches the current working directory.",
                        "type": "string",
                    ],
                    "include": [
                        "description": "Optional: A glob pattern to filter which files are searched (e.g., '*.js', '*.{ts,tsx}', 'src/**'). If omitted, searches all files (respecting potential global ignores).",
                        "type": "string",
                    ],
                    "exclude_pattern": [
                        "description": "Optional: A regular expression pattern to exclude from the search results. If a line matches both the pattern and the exclude_pattern, it will be omitted.",
                        "type": "string",
                    ],
                    "names_only": [
                        "description": "Optional: If true, only the file paths of the matches will be returned, without the line content or line numbers. This is useful for gathering a list of files.",
                        "type": "boolean",
                    ],
                    "max_matches_per_file": [
                        "description": "Optional: Maximum number of matches to return per file. Use this to prevent being overwhelmed by repetitive matches in large files.",
                        "type": "integer",
                        "minimum": 1,
                    ],
                    "total_max_matches": [
                        "description": "Optional: Maximum number of total matches to return. Use this to limit the overall size of the response. Defaults to 100 if omitted.",
                        "type": "integer",
                        "minimum": 1,
                    ],
                ] as [String: Any],
                "required": ["pattern"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let pattern = args["pattern"] as? String else {
                throw ToolError.validationError("pattern is required")
            }

            let dirPath = (args["dir_path"] as? String) ?? env.workingDirectory()
            let namesOnly = geminiBool(args["names_only"]) ?? false
            let totalLimit = max(1, geminiInt(args["total_max_matches"]) ?? 100)
            let perFileLimit = max(1, geminiInt(args["max_matches_per_file"]) ?? 100)

            var command = "rg"
            if namesOnly {
                command += " -l"
            } else {
                command += " -n"
            }
            command += " --max-count \(perFileLimit)"

            if let include = args["include"] as? String, !include.isEmpty {
                command += " --glob " + geminiShellEscape(include)
            }
            command += " " + geminiShellEscape(pattern)
            command += " " + geminiShellEscape(dirPath)
            command += " | head -n \(totalLimit)"

            let result = try await env.execCommand(command: command, timeoutMs: 15_000, workingDir: nil, envVars: nil)
            var output = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)

            if let excludePattern = args["exclude_pattern"] as? String, !excludePattern.isEmpty, !output.isEmpty {
                if let regex = try? NSRegularExpression(pattern: excludePattern) {
                    output = output
                        .components(separatedBy: "\n")
                        .filter { line in
                            let range = NSRange(location: 0, length: (line as NSString).length)
                            return regex.firstMatch(in: line, options: [], range: range) == nil
                        }
                        .joined(separator: "\n")
                }
            }

            return output.isEmpty ? "No matches found." : output
        }
    )
}

// MARK: - list_directory

public func geminiListDirectoryTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_LS_TOOL_NAME,
            description: "Lists the names of files and subdirectories directly within a specified directory path. Can optionally ignore entries matching provided glob patterns.",
            parameters: [
                "type": "object",
                "properties": [
                    "dir_path": [
                        "description": "The path to the directory to list",
                        "type": "string",
                    ],
                    "ignore": [
                        "description": "List of glob patterns to ignore",
                        "items": ["type": "string"],
                        "type": "array",
                    ] as [String: Any],
                    "file_filtering_options": [
                        "description": "Optional: Whether to respect ignore patterns from .gitignore or .geminiignore",
                        "type": "object",
                        "properties": [
                            "respect_git_ignore": [
                                "description": "Optional: Whether to respect .gitignore patterns when listing files. Only available in git repositories. Defaults to true.",
                                "type": "boolean",
                            ],
                            "respect_gemini_ignore": [
                                "description": "Optional: Whether to respect .geminiignore patterns when listing files. Defaults to true.",
                                "type": "boolean",
                            ],
                        ] as [String: Any],
                    ] as [String: Any],
                ] as [String: Any],
                "required": ["dir_path"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let dirPath = args["dir_path"] as? String else {
                throw ToolError.validationError("dir_path is required")
            }
            let entries = try await env.listDirectory(path: dirPath, depth: 1)
            let ignores = geminiStringArray(args["ignore"]) ?? []

            let lines = entries.compactMap { entry -> String? in
                if ignores.contains(where: { geminiSimpleGlobMatch(path: entry.name, pattern: $0) }) {
                    return nil
                }
                return entry.name + (entry.isDir ? "/" : "")
            }
            return lines.isEmpty ? "No entries found." : lines.joined(separator: "\n")
        }
    )
}

// MARK: - run_shell_command

public func geminiRunShellCommandTool(enableInteractiveShell: Bool = true, enableEfficiency: Bool = true) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_SHELL_TOOL_NAME,
            description: geminiShellToolDescription(enableInteractiveShell: enableInteractiveShell, enableEfficiency: enableEfficiency),
            parameters: [
                "type": "object",
                "properties": [
                    "command": [
                        "type": "string",
                        "description": geminiShellCommandDescription(),
                    ],
                    "description": [
                        "type": "string",
                        "description": "Brief description of the command for the user. Be specific and concise. Ideally a single sentence. Can be up to 3 sentences for clarity. No line breaks.",
                    ],
                    "dir_path": [
                        "type": "string",
                        "description": "(OPTIONAL) The path of the directory to run the command in. If not provided, the project root directory is used. Must be a directory within the workspace and must already exist.",
                    ],
                    "is_background": [
                        "type": "boolean",
                        "description": "Set to true if this command should be run in the background (e.g. for long-running servers or watchers). The command will be started, allowed to run for a brief moment to check for immediate errors, and then moved to the background.",
                    ],
                ] as [String: Any],
                "required": ["command"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let command = args["command"] as? String else {
                throw ToolError.validationError("command is required")
            }
            let dirPath = args["dir_path"] as? String
            let isBackground = geminiBool(args["is_background"]) ?? false

            if isBackground {
                let bgCommand = "nohup bash -lc \(" + geminiShellEscape(command) + ") >/tmp/gemini_bg_\(UUID().uuidString).log 2>&1 & echo $!"
                let started = try await env.execCommand(command: bgCommand, timeoutMs: 5_000, workingDir: dirPath, envVars: nil)
                let pid = started.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
                return pid.isEmpty ? "Background process started." : "Background process started. PID: \(pid)"
            }

            return try await geminiExecuteCommand(
                env: env,
                command: command,
                dirPath: dirPath,
                emitOutputDelta: { _ in }
            )
        },
        streamingExecutor: { args, env, emitOutputDelta in
            guard let command = args["command"] as? String else {
                throw ToolError.validationError("command is required")
            }
            let dirPath = args["dir_path"] as? String
            let isBackground = geminiBool(args["is_background"]) ?? false

            if isBackground {
                let bgCommand = "nohup bash -lc \(" + geminiShellEscape(command) + ") >/tmp/gemini_bg_\(UUID().uuidString).log 2>&1 & echo $!"
                let started = try await env.execCommand(command: bgCommand, timeoutMs: 5_000, workingDir: dirPath, envVars: nil)
                let pid = started.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
                return pid.isEmpty ? "Background process started." : "Background process started. PID: \(pid)"
            }

            return try await geminiExecuteCommand(
                env: env,
                command: command,
                dirPath: dirPath,
                emitOutputDelta: emitOutputDelta
            )
        }
    )
}

private func geminiExecuteCommand(
    env: ExecutionEnvironment,
    command: String,
    dirPath: String?,
    emitOutputDelta: StreamingToolOutputEmitter
) async throws -> String {
    let result = try await env.execCommand(command: command, timeoutMs: 120_000, workingDir: dirPath, envVars: nil)
    let combinedOutput = result.combinedOutput
    if !combinedOutput.isEmpty {
        await emitOutputDelta(combinedOutput)
    }

    var output = combinedOutput
    if output.isEmpty { output = "(empty)" }

    var lines: [String] = ["Output:", output]
    if result.exitCode != 0 {
        lines.append("Exit Code: \(result.exitCode)")
    }
    return lines.joined(separator: "\n")
}

// MARK: - google_web_search

public func geminiGoogleWebSearchTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_WEB_SEARCH_TOOL_NAME,
            description: "Performs a web search using Google Search (via the Gemini API) and returns the results. This tool is useful for finding information on the internet based on a query.",
            parameters: [
                "type": "object",
                "properties": [
                    "query": [
                        "type": "string",
                        "description": "The search query to find information on the web.",
                    ],
                ] as [String: Any],
                "required": ["query"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let query = args["query"] as? String else {
                throw ToolError.validationError("query is required")
            }
            return try await geminiNativeWebSearch(query: query)
        }
    )
}

// MARK: - web_fetch

public func geminiWebFetchTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_WEB_FETCH_TOOL_NAME,
            description: "Processes content from URL(s), including local and private network addresses (e.g., localhost), embedded in a prompt. Include up to 20 URLs and instructions (e.g., summarize, extract specific data) directly in the 'prompt' parameter.",
            parameters: [
                "type": "object",
                "properties": [
                    "prompt": [
                        "description": "A comprehensive prompt that includes the URL(s) (up to 20) to fetch and specific instructions on how to process their content (e.g., \"Summarize https://example.com/article and extract key points from https://another.com/data\"). All URLs to be fetched must be valid and complete, starting with \"http://\" or \"https://\", and be fully-formed with a valid hostname (e.g., a domain name like \"example.com\" or an IP address). For example, \"https://example.com\" is valid, but \"example.com\" is not.",
                        "type": "string",
                    ],
                ] as [String: Any],
                "required": ["prompt"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let prompt = args["prompt"] as? String else {
                throw ToolError.validationError("prompt is required")
            }

            let urls = geminiExtractURLs(from: prompt)
            guard !urls.isEmpty else {
                throw ToolError.validationError("prompt must include at least one valid http:// or https:// URL")
            }
            let limited = Array(urls.prefix(20))

            let loader = WebFetchLoader(maxRedirects: 10, allowCrossHostRedirects: true, timeout: 30)
            var outputParts: [String] = []
            for url in limited {
                let result = try await loader.fetch(url)
                let cleaned = stripHTMLForToolOutput(result.content)
                let clipped = cleaned.count > 20_000 ? String(cleaned.prefix(20_000)) + "\n\n[Content truncated...]" : cleaned
                outputParts.append("URL: \(result.finalURL.absoluteString)\n\(clipped)")
            }

            return "Instruction Prompt: \(prompt)\n\n" + outputParts.joined(separator: "\n\n---\n\n")
        }
    )
}

// MARK: - read_many_files

public func geminiReadManyFilesTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_READ_MANY_FILES_TOOL_NAME,
            description: "Reads content from multiple files specified by glob patterns within a configured target directory. For text files, it concatenates their content into a single string. It is primarily designed for text-based files.",
            parameters: [
                "type": "object",
                "properties": [
                    "include": [
                        "type": "array",
                        "items": [
                            "type": "string",
                            "minLength": 1,
                        ] as [String: Any],
                        "minItems": 1,
                        "description": "An array of glob patterns or paths. Examples: [\"src/**/*.ts\"], [\"README.md\", \"docs/\"]",
                    ] as [String: Any],
                    "exclude": [
                        "type": "array",
                        "items": [
                            "type": "string",
                            "minLength": 1,
                        ] as [String: Any],
                        "description": "Optional. Glob patterns for files/directories to exclude. Added to default excludes if useDefaultExcludes is true. Example: \"**/*.log\", \"temp/\"",
                        "default": [],
                    ] as [String: Any],
                    "recursive": [
                        "type": "boolean",
                        "description": "Optional. Whether to search recursively (primarily controlled by `**` in glob patterns). Defaults to true.",
                        "default": true,
                    ],
                    "useDefaultExcludes": [
                        "type": "boolean",
                        "description": "Optional. Whether to apply a list of default exclusion patterns (e.g., node_modules, .git, binary files). Defaults to true.",
                        "default": true,
                    ],
                    "file_filtering_options": [
                        "description": "Whether to respect ignore patterns from .gitignore or .geminiignore",
                        "type": "object",
                        "properties": [
                            "respect_git_ignore": [
                                "description": "Optional: Whether to respect .gitignore patterns when listing files. Only available in git repositories. Defaults to true.",
                                "type": "boolean",
                            ],
                            "respect_gemini_ignore": [
                                "description": "Optional: Whether to respect .geminiignore patterns when listing files. Defaults to true.",
                                "type": "boolean",
                            ],
                        ] as [String: Any],
                    ] as [String: Any],
                ] as [String: Any],
                "required": ["include"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let include = geminiStringArray(args["include"]), !include.isEmpty else {
                throw ToolError.validationError("include is required")
            }

            let defaultExcludes = ["node_modules/**", ".git/**", "dist/**", "build/**", ".next/**", "coverage/**"]
            let useDefaults = geminiBool(args["useDefaultExcludes"]) ?? true
            let excludes = (useDefaults ? defaultExcludes : []) + (geminiStringArray(args["exclude"]) ?? [])

            var files: [String] = []
            for pattern in include {
                let matched = try await env.glob(pattern: pattern, path: env.workingDirectory())
                files.append(contentsOf: matched)
            }
            files = Array(Set(files)).sorted()

            files = files.filter { path in
                !excludes.contains(where: { geminiSimpleGlobMatch(path: path, pattern: $0) })
            }

            if files.isEmpty {
                return "No files matching the criteria were found."
            }

            var parts: [String] = []
            for path in files.prefix(100) {
                if FileManager.default.fileExists(atPath: path) {
                    let raw = try await env.readFile(path: path, offset: nil, limit: nil)
                    let body = geminiStripLineNumbers(raw)
                    parts.append("--- \(path) ---\n\n\(body)")
                }
            }

            if parts.isEmpty {
                return "No files were successfully read."
            }

            return parts.joined(separator: "\n\n") + "\n\n--- End of content ---"
        }
    )
}

// MARK: - save_memory

public func geminiSaveMemoryTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_MEMORY_TOOL_NAME,
            description: """
Saves concise global user context (preferences, facts) for use across ALL workspaces.

### CRITICAL: GLOBAL CONTEXT ONLY
NEVER save workspace-specific context, local paths, or commands (e.g. \"The entry point is src/index.js\", \"The test command is npm test\"). These are local to the current workspace and must NOT be saved globally. EXCLUSIVELY for context relevant across ALL workspaces.

- Use for \"Remember X\" or clear personal facts.
- Do NOT use for session context.
""",
            parameters: [
                "type": "object",
                "properties": [
                    "fact": [
                        "type": "string",
                        "description": "The specific fact or piece of information to remember. Should be a clear, self-contained statement.",
                    ],
                ] as [String: Any],
                "required": ["fact"],
                "additionalProperties": false,
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let fact = args["fact"] as? String, !fact.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
                throw ToolError.validationError("fact is required")
            }
            return "Saved memory: \(fact)"
        }
    )
}

// MARK: - write_todos

public func geminiWriteTodosTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_WRITE_TODOS_TOOL_NAME,
            description: "This tool can help you list out the current subtasks that are required to be completed for a given user request. The list of subtasks helps you keep track of the current task, organize complex queries and help ensure that you don't miss any steps.",
            parameters: [
                "type": "object",
                "properties": [
                    "todos": [
                        "type": "array",
                        "description": "The complete list of todo items. This will replace the existing list.",
                        "items": [
                            "type": "object",
                            "description": "A single todo item.",
                            "properties": [
                                "description": [
                                    "type": "string",
                                    "description": "The description of the task.",
                                ],
                                "status": [
                                    "type": "string",
                                    "description": "The current status of the task.",
                                    "enum": ["pending", "in_progress", "completed", "cancelled"],
                                ],
                            ] as [String: Any],
                            "required": ["description", "status"],
                            "additionalProperties": false,
                        ] as [String: Any],
                    ] as [String: Any],
                ] as [String: Any],
                "required": ["todos"],
                "additionalProperties": false,
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let todos = args["todos"] as? [Any], !todos.isEmpty else {
                throw ToolError.validationError("todos is required")
            }

            var output = "Todo list updated:\n\n"
            for (index, raw) in todos.enumerated() {
                guard let todo = raw as? [String: Any] else { continue }
                let text = (todo["description"] as? String) ?? "Task \(index + 1)"
                let status = ((todo["status"] as? String) ?? "pending").lowercased()
                let icon: String
                switch status {
                case "completed": icon = "[x]"
                case "in_progress": icon = "[~]"
                case "cancelled": icon = "[-]"
                default: icon = "[ ]"
                }
                output += "\(index + 1). \(icon) \(text)\n"
            }
            return output
        }
    )
}

// MARK: - get_internal_docs

public func geminiGetInternalDocsTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_GET_INTERNAL_DOCS_TOOL_NAME,
            description: "Returns the content of Gemini CLI internal documentation files. If no path is provided, returns a list of all available documentation paths.",
            parameters: [
                "type": "object",
                "properties": [
                    "path": [
                        "description": "The relative path to the documentation file (e.g., 'cli/commands.md'). If omitted, lists all available documentation.",
                        "type": "string",
                    ],
                ] as [String: Any],
            ] as [String: Any]
        ),
        executor: { args, env in
            let docsDirCandidates = [
                (env.workingDirectory() as NSString).appendingPathComponent("docs"),
                (env.workingDirectory() as NSString).appendingPathComponent("documentation"),
                (env.workingDirectory() as NSString).appendingPathComponent("doc"),
            ]
            guard let docsRoot = docsDirCandidates.first(where: { path in
                var isDir: ObjCBool = false
                return FileManager.default.fileExists(atPath: path, isDirectory: &isDir) && isDir.boolValue
            }) else {
                return "No internal documentation directory found."
            }

            if let path = args["path"] as? String, !path.isEmpty {
                let fullPath = (docsRoot as NSString).appendingPathComponent(path)
                guard FileManager.default.fileExists(atPath: fullPath) else {
                    throw ToolError.fileNotFound(path)
                }
                return try String(contentsOfFile: fullPath, encoding: .utf8)
            }

            let docs = try await env.glob(pattern: "**/*.md", path: docsRoot)
            if docs.isEmpty {
                return "No documentation files found."
            }
            let listed = docs.sorted().map { "- \($0.replacing(docsRoot + "/", with: ""))" }.joined(separator: "\n")
            return "Available documentation files:\n\n\(listed)"
        }
    )
}

// MARK: - activate_skill

public func geminiActivateSkillTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_ACTIVATE_SKILL_TOOL_NAME,
            description: "Activates a specialized agent skill by name. Returns the skill's instructions wrapped in `<activated_skill>` tags. These provide specialized guidance for the current task. Use this when you identify a task that matches a skill's description.",
            parameters: [
                "type": "object",
                "properties": [
                    "name": [
                        "type": "string",
                        "description": "The name of the skill to activate.",
                    ],
                ] as [String: Any],
                "required": ["name"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let name = args["name"] as? String else {
                throw ToolError.validationError("name is required")
            }
            let candidates = [
                (env.workingDirectory() as NSString).appendingPathComponent(".gemini/skills/\(name).md"),
                (env.workingDirectory() as NSString).appendingPathComponent(".gemini/skills/\(name)/SKILL.md"),
                (env.workingDirectory() as NSString).appendingPathComponent("skills/\(name).md"),
            ]
            for path in candidates where FileManager.default.fileExists(atPath: path) {
                let content = try String(contentsOfFile: path, encoding: .utf8)
                return """
<activated_skill name="\(name)">
  <instructions>
\(content)
  </instructions>
</activated_skill>
"""
            }
            throw ToolError.validationError("Skill '\(name)' not found")
        }
    )
}

// MARK: - ask_user

public func geminiAskUserTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_ASK_USER_TOOL_NAME,
            description: "Ask the user one or more questions to gather preferences, clarify requirements, or make decisions.",
            parameters: [
                "type": "object",
                "required": ["questions"],
                "properties": [
                    "questions": [
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 4,
                        "items": [
                            "type": "object",
                            "required": ["question", "header", "type"],
                            "properties": [
                                "question": [
                                    "type": "string",
                                    "description": "The complete question to ask the user. Should be clear, specific, and end with a question mark.",
                                ],
                                "header": [
                                    "type": "string",
                                    "maxLength": 16,
                                    "description": "MUST be 16 characters or fewer or the call will fail. Very short label displayed as a chip/tag. Use abbreviations: \"Auth\" not \"Authentication\", \"Config\" not \"Configuration\". Examples: \"Auth method\", \"Library\", \"Approach\", \"Database\".",
                                ],
                                "type": [
                                    "type": "string",
                                    "enum": ["choice", "text", "yesno"],
                                    "default": "choice",
                                    "description": "Question type: 'choice' (default) for multiple-choice with options, 'text' for free-form input, 'yesno' for Yes/No confirmation.",
                                ],
                                "options": [
                                    "type": "array",
                                    "description": "The selectable choices for 'choice' type questions. Provide 2-4 options. An 'Other' option is automatically added. Not needed for 'text' or 'yesno' types.",
                                    "items": [
                                        "type": "object",
                                        "required": ["label", "description"],
                                        "properties": [
                                            "label": [
                                                "type": "string",
                                                "description": "The display text for this option (1-5 words). Example: \"OAuth 2.0\"",
                                            ],
                                            "description": [
                                                "type": "string",
                                                "description": "Brief explanation of this option. Example: \"Industry standard, supports SSO\"",
                                            ],
                                        ] as [String: Any],
                                    ] as [String: Any],
                                ] as [String: Any],
                                "multiSelect": [
                                    "type": "boolean",
                                    "description": "Only applies when type='choice'. Set to true to allow selecting multiple options.",
                                ],
                                "placeholder": [
                                    "type": "string",
                                    "description": "Hint text shown in the input field. For type='text', shown in the main input. For type='choice', shown in the 'Other' custom input.",
                                ],
                            ] as [String: Any],
                        ] as [String: Any],
                    ] as [String: Any],
                ] as [String: Any],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let questions = args["questions"] as? [Any], !questions.isEmpty else {
                throw ToolError.validationError("questions is required")
            }

            var output = "Questions for user:\n\n"
            for (index, raw) in questions.enumerated() {
                guard let q = raw as? [String: Any] else { continue }
                let text = (q["question"] as? String) ?? "Question \(index + 1)?"
                output += "Q\(index + 1): \(text)\n"
                if let options = q["options"] as? [Any], !options.isEmpty {
                    for (optIdx, rawOption) in options.enumerated() {
                        guard let option = rawOption as? [String: Any] else { continue }
                        let label = (option["label"] as? String) ?? "Option \(optIdx + 1)"
                        let desc = (option["description"] as? String) ?? ""
                        output += "  \(optIdx + 1). \(label)"
                        if !desc.isEmpty { output += " - \(desc)" }
                        output += "\n"
                    }
                }
                output += "\n"
            }
            output += "[Waiting for user response...]"
            return output
        }
    )
}

// MARK: - enter_plan_mode / exit_plan_mode

public func geminiEnterPlanModeTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_ENTER_PLAN_MODE_TOOL_NAME,
            description: "Switch to Plan Mode to safely research, design, and plan complex changes using read-only tools.",
            parameters: [
                "type": "object",
                "properties": [
                    "reason": [
                        "type": "string",
                        "description": "Short reason explaining why you are entering plan mode.",
                    ],
                ] as [String: Any],
            ] as [String: Any]
        ),
        executor: { _, _ in
            "Entering plan mode."
        }
    )
}

public func geminiExitPlanModeTool(plansDir: String = ".gemini/plans") -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: GEMINI_EXIT_PLAN_MODE_TOOL_NAME,
            description: "Finalizes the planning phase and transitions to implementation by presenting the plan for user approval. This tool MUST be used to exit Plan Mode before any source code edits can be performed. Call this whenever a plan is ready or the user requests implementation.",
            parameters: [
                "type": "object",
                "required": ["plan_path"],
                "properties": [
                    "plan_path": [
                        "type": "string",
                        "description": "The file path to the finalized plan (e.g., \"\(plansDir)/feature-x.md\"). This path MUST be within the designated plans directory: \(plansDir)/",
                    ],
                ] as [String: Any],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let planPath = args["plan_path"] as? String else {
                throw ToolError.validationError("plan_path is required")
            }
            return "Plan mode exited with plan: \(planPath)"
        }
    )
}

// MARK: - Helpers

private func geminiInt(_ raw: Any?) -> Int? {
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

private struct GeminiGroundingSource {
    let title: String
    let uri: String
}

private struct GeminiCitationInsertion {
    let index: Int
    let marker: String
}

private func geminiNativeWebSearch(query: String) async throws -> String {
    do {
        let client = try Client.fromEnv()
        let request = Request(
            model: "web-search",
            messages: [.user(query)],
            provider: "gemini"
        )
        let response = try await client.complete(request: request)
        let responseText = response.text.trimmingCharacters(in: .whitespacesAndNewlines)

        guard !responseText.isEmpty else {
            return "No search results or information found for query: \"\(query)\""
        }

        let sources = geminiGroundingSources(from: response.raw)
        let insertions = geminiGroundingInsertions(from: response.raw)

        var modifiedResponseText = responseText
        if !insertions.isEmpty {
            modifiedResponseText = geminiInsertCitationMarkers(
                text: modifiedResponseText,
                insertions: insertions
            )
        }

        if !sources.isEmpty {
            let sourceLines = sources.enumerated().map { index, source in
                "[\(index + 1)] \(source.title) (\(source.uri))"
            }.joined(separator: "\n")
            modifiedResponseText += "\n\nSources:\n\(sourceLines)"
        }

        return "Web search results for \"\(query)\":\n\n\(modifiedResponseText)"
    } catch {
        let message = "Error during web search for query \"\(query)\": \(error)"
        return "Error: \(message)"
    }
}

private func geminiGroundingSources(from raw: JSONValue?) -> [GeminiGroundingSource] {
    guard let grounding = geminiGroundingMetadata(from: raw) else {
        return []
    }

    let chunks = grounding["groundingChunks"]?.arrayValue ?? []
    return chunks.map { chunk in
        GeminiGroundingSource(
            title: chunk["web"]?["title"]?.stringValue ?? "Untitled",
            uri: chunk["web"]?["uri"]?.stringValue ?? "No URI"
        )
    }
}

private func geminiGroundingInsertions(from raw: JSONValue?) -> [GeminiCitationInsertion] {
    guard let grounding = geminiGroundingMetadata(from: raw) else {
        return []
    }

    let supports = grounding["groundingSupports"]?.arrayValue ?? []
    return supports.compactMap { support in
        guard let endIndex = geminiJSONInt(support["segment"]?["endIndex"]),
              let chunkIndices = support["groundingChunkIndices"]?.arrayValue
        else {
            return nil
        }

        let marker = chunkIndices
            .compactMap(geminiJSONInt)
            .map { "[\($0 + 1)]" }
            .joined()
        guard !marker.isEmpty else {
            return nil
        }

        return GeminiCitationInsertion(index: endIndex, marker: marker)
    }
}

private func geminiGroundingMetadata(from raw: JSONValue?) -> JSONValue? {
    guard let candidates = raw?["candidates"]?.arrayValue,
          let first = candidates.first
    else {
        return nil
    }
    return first["groundingMetadata"]
}

private func geminiJSONInt(_ value: JSONValue?) -> Int? {
    if let number = value?.doubleValue {
        return Int(number)
    }
    if let text = value?.stringValue {
        return Int(text)
    }
    return nil
}

private func geminiInsertCitationMarkers(text: String, insertions: [GeminiCitationInsertion]) -> String {
    var bytes = Array(text.utf8)
    for insertion in insertions.sorted(by: { $0.index > $1.index }) {
        let clampedIndex = max(0, min(insertion.index, bytes.count))
        bytes.insert(contentsOf: insertion.marker.utf8, at: clampedIndex)
    }
    return String(decoding: bytes, as: UTF8.self)
}

private func geminiBool(_ raw: Any?) -> Bool? {
    switch raw {
    case let value as Bool:
        return value
    case let value as NSNumber:
        return value.boolValue
    default:
        return nil
    }
}

private func geminiStringArray(_ raw: Any?) -> [String]? {
    if let strings = raw as? [String] {
        return strings
    }
    if let array = raw as? [Any] {
        let strings = array.compactMap { $0 as? String }
        return strings.count == array.count ? strings : nil
    }
    return nil
}

private func geminiShellEscape(_ value: String) -> String {
    if value.isEmpty {
        return "''"
    }
    return "'" + value.replacing("'", with: "'\\''") + "'"
}

private func geminiStripLineNumbers(_ text: String) -> String {
    text.components(separatedBy: "\n").map { line in
        if let pipe = line.firstIndex(of: "|") {
            let prefix = line[..<pipe].trimmingCharacters(in: .whitespaces)
            if !prefix.isEmpty, prefix.allSatisfy(\.isNumber) {
                let idx = line.index(after: pipe)
                if idx < line.endIndex && line[idx] == " " {
                    return String(line[line.index(after: idx)...])
                }
                return String(line[idx...])
            }
        }
        return line
    }.joined(separator: "\n")
}

private func geminiSimpleGlobMatch(path: String, pattern: String) -> Bool {
    if pattern == "*" || pattern == "**" {
        return true
    }
    if pattern.hasPrefix("**/") {
        let suffix = String(pattern.dropFirst(3))
        return path.hasSuffix(suffix)
    }
    if pattern.hasSuffix("/**") {
        let prefix = String(pattern.dropLast(3))
        return path.hasPrefix(prefix)
    }
    if pattern.hasPrefix("*.") {
        return path.hasSuffix(String(pattern.dropFirst(1)))
    }
    if pattern.contains("*") {
        let needle = pattern.replacing("*", with: "")
        return path.contains(needle)
    }
    return path == pattern
}

private func geminiShellToolDescription(enableInteractiveShell: Bool, enableEfficiency: Bool) -> String {
    let efficiencyGuidelines = enableEfficiency
        ? """

      Efficiency Guidelines:
      - Quiet Flags: Always prefer silent or quiet flags (e.g., `npm install --silent`, `git --no-pager`) to reduce output volume while still capturing necessary information.
      - Pagination: Always disable terminal pagination to ensure commands terminate (e.g., use `git --no-pager`, `systemctl --no-pager`, or set `PAGER=cat`).
"""
        : ""

    let returnedInfo = """

      The following information is returned:

      Output: Combined stdout/stderr. Can be `(empty)` or partial on error and for any unwaited background processes.
      Exit Code: Only included if non-zero (command failed).
      Error: Only included if a process-level error occurred (e.g., spawn failure).
      Signal: Only included if process was terminated by a signal.
      Background PIDs: Only included if background processes were started.
      Process Group PGID: Only included if available.
"""

    #if os(Windows)
    let backgroundInstructions = enableInteractiveShell
        ? "To run a command in the background, set the `is_background` parameter to true. Do NOT use PowerShell background constructs."
        : "Command can start background processes using PowerShell constructs such as `Start-Process -NoNewWindow` or `Start-Job`."
    return "This tool executes a given shell command as `powershell.exe -NoProfile -Command <command>`. \(backgroundInstructions)\(efficiencyGuidelines)\(returnedInfo)"
    #else
    let backgroundInstructions = enableInteractiveShell
        ? "To run a command in the background, set the `is_background` parameter to true. Do NOT use `&` to background commands."
        : "Command can start background processes using `&`."
    return "This tool executes a given shell command as `bash -c <command>`. \(backgroundInstructions) Command is executed as a subprocess that leads its own process group. Command process group can be terminated as `kill -- -PGID` or signaled as `kill -s SIGNAL -- -PGID`.\(efficiencyGuidelines)\(returnedInfo)"
    #endif
}

private func geminiShellCommandDescription() -> String {
    #if os(Windows)
    return "Exact command to execute as `powershell.exe -NoProfile -Command <command>`"
    #else
    return "Exact bash command to execute as `bash -c <command>`"
    #endif
}

private func geminiExtractURLs(from text: String) -> [String] {
    let pattern = #"https?://[^\s)\]>\"']+"#
    guard let regex = try? NSRegularExpression(pattern: pattern) else {
        return []
    }
    let range = NSRange(location: 0, length: (text as NSString).length)
    let matches = regex.matches(in: text, options: [], range: range)
    return matches.compactMap { match in
        guard let range = Range(match.range, in: text) else { return nil }
        return String(text[range])
    }
}
