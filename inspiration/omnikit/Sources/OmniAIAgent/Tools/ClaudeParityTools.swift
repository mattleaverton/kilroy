import Foundation
import OmniAICore

// MARK: - Claude file tools

public func claudeReadTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "Read",
            description: """
    Reads a file from the local filesystem. You can access any file directly by using this tool.
    Assume this tool is able to read all files on the machine. If the User provides a path to a file assume that path is valid.

    Usage:
    - The file_path parameter must be an absolute path, not a relative path
    - By default, it reads up to 2000 lines starting from the beginning of the file
    - You can optionally specify a line offset and limit (especially handy for long files)
    - Any lines longer than 2000 characters will be truncated
    - Results are returned using cat -n format, with line numbers starting at 1
    - This tool allows reading images (PNG, JPG, GIF, WebP). When reading an image file the contents are presented as base64 data with dimensions.
    - This tool can read Jupyter notebooks (.ipynb files) and returns all cells with their outputs.
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "file_path": ["type": "string", "description": "The absolute path to the file to read"],
                    "offset": ["type": "number", "description": "The line number to start reading from. Only provide if the file is too large to read at once"],
                    "limit": ["type": "number", "description": "The number of lines to read. Only provide if the file is too large to read at once."],
                ] as [String: Any],
                "required": ["file_path"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let filePath = args["file_path"] as? String else {
                throw ToolError.validationError("file_path is required")
            }
            return try await env.readFile(
                path: filePath,
                offset: claudeInt(args["offset"]),
                limit: claudeInt(args["limit"])
            )
        }
    )
}

public func claudeWriteTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "Write",
            description: """
    Writes a file to the local filesystem.

    Usage:
    - This tool will overwrite the existing file if there is one at the provided path.
    - If this is an existing file, you MUST use the Read tool first to read the file's contents.
    - ALWAYS prefer editing existing files in the codebase. NEVER write new files unless explicitly required.
    - NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "file_path": ["type": "string", "description": "The absolute path to the file to write (must be absolute, not relative)"],
                    "content": ["type": "string", "description": "The content to write to the file"],
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

public func claudeEditTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "Edit",
            description: """
    Performs exact string replacements in files.

    Usage:
    - You must use your Read tool at least once in the conversation before editing.
    - The edit will FAIL if old_string is not unique in the file. Either provide a larger string with more surrounding context to make it unique or use replace_all to change every instance.
    - Use replace_all for replacing and renaming strings across the file. This parameter is useful if you want to rename a variable for instance.
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "file_path": ["type": "string", "description": "The absolute path to the file to modify"],
                    "old_string": ["type": "string", "description": "The text to replace"],
                    "new_string": ["type": "string", "description": "The text to replace it with (must be different from old_string)"],
                    "replace_all": ["type": "boolean", "description": "Replace all occurences of old_string (default false)"],
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

            let replaceAll = claudeBool(args["replace_all"]) ?? false
            let raw = try await env.readFile(path: filePath, offset: nil, limit: nil)
            let content = claudeStripLineNumbers(raw)

            guard content.contains(oldString) else {
                throw ToolError.editConflict("old_string not found in file: \(filePath)")
            }

            let occurrences = content.components(separatedBy: oldString).count - 1
            if occurrences > 1 && !replaceAll {
                throw ToolError.editConflict("old_string found \(occurrences) times. Use replace_all=true or provide more context.")
            }

            let newContent: String
            if replaceAll {
                newContent = content.replacing(oldString, with: newString)
            } else {
                guard let range = content.range(of: oldString) else {
                    throw ToolError.editConflict("old_string not found")
                }
                newContent = content.replacingCharacters(in: range, with: newString)
            }

            try await env.writeFile(path: filePath, content: newContent)
            return "Replaced \(replaceAll ? occurrences : 1) occurrence(s) in \(filePath)"
        }
    )
}

public func claudeGlobTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "Glob",
            description: """
    - Fast file pattern matching tool that works with any codebase size
    - Supports glob patterns like "**/*.js" or "src/**/*.ts"
    - Returns matching file paths sorted by modification time
    - Use this tool when you need to find files by name patterns
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "pattern": ["type": "string", "description": "The glob pattern to match files against"],
                    "path": ["type": "string", "description": "The directory to search in. If not specified, the current working directory will be used."],
                ] as [String: Any],
                "required": ["pattern"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let pattern = args["pattern"] as? String else {
                throw ToolError.validationError("pattern is required")
            }
            let path = (args["path"] as? String) ?? env.workingDirectory()
            let matches = try await env.glob(pattern: pattern, path: path)
            return matches.isEmpty ? "No matching files found." : matches.joined(separator: "\n")
        }
    )
}

public func claudeGrepTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "Grep",
            description: """
    A powerful search tool built on ripgrep

    Usage:
    - Supports full regex syntax (e.g., "log.*Error", "function\\s+\\w+")
    - Filter files with glob parameter (e.g., "*.js", "**/*.tsx") or type parameter (e.g., "js", "py", "rust")
    - Output modes: "content" shows matching lines, "files_with_matches" shows only file paths (default), "count" shows match counts
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "pattern": ["type": "string", "description": "The regular expression pattern to search for in file contents"],
                    "path": ["type": "string", "description": "File or directory to search in (rg PATH). Defaults to current working directory."],
                    "glob": ["type": "string", "description": "Glob pattern to filter files (e.g. \"*.js\", \"*.{ts,tsx}\") - maps to rg --glob"],
                    "output_mode": ["type": "string", "description": "Output mode: \"content\" shows matching lines, \"files_with_matches\" shows file paths (default), \"count\" shows match counts."],
                    "-A": ["type": "number", "description": "Number of lines to show after each match (rg -A)"],
                    "-B": ["type": "number", "description": "Number of lines to show before each match (rg -B)"],
                    "-C": ["type": "number", "description": "Number of lines to show before and after each match (rg -C)"],
                    "-n": ["type": "boolean", "description": "Show line numbers in output (rg -n). Defaults to true."],
                    "-i": ["type": "boolean", "description": "Case insensitive search (rg -i)"],
                    "type": ["type": "string", "description": "File type to search (rg --type). Common types: js, py, rust, go, java, etc."],
                    "head_limit": ["type": "number", "description": "Limit output to first N lines/entries. Defaults to 0 (unlimited)."],
                    "offset": ["type": "number", "description": "Skip first N lines/entries before applying head_limit. Defaults to 0."],
                    "multiline": ["type": "boolean", "description": "Enable multiline mode where . matches newlines. Default: false."],
                ] as [String: Any],
                "required": ["pattern"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let pattern = args["pattern"] as? String else {
                throw ToolError.validationError("pattern is required")
            }

            let searchPath = (args["path"] as? String) ?? env.workingDirectory()
            let glob = args["glob"] as? String
            let caseInsensitive = claudeBool(args["-i"]) ?? false
            let limit = max(1, claudeInt(args["head_limit"]) ?? 200)
            let offset = max(0, claudeInt(args["offset"]) ?? 0)
            let mode = (args["output_mode"] as? String) ?? "files_with_matches"

            if mode == "files_with_matches" {
                var command = "rg -l --max-count=1"
                if caseInsensitive {
                    command += " -i"
                }
                if let after = claudeInt(args["-A"]), after > 0 {
                    command += " -A \(after)"
                }
                if let before = claudeInt(args["-B"]), before > 0 {
                    command += " -B \(before)"
                }
                if let both = claudeInt(args["-C"]), both > 0 {
                    command += " -C \(both)"
                }
                if let fileType = args["type"] as? String, !fileType.isEmpty {
                    command += " --type " + claudeShellEscape(fileType)
                }
                if claudeBool(args["multiline"]) == true {
                    command += " -U --multiline-dotall"
                }
                if let glob, !glob.isEmpty {
                    command += " --glob " + claudeShellEscape(glob)
                }
                command += " " + claudeShellEscape(pattern) + " " + claudeShellEscape(searchPath)
                command += " | tail -n +\(offset + 1) | head -n \(limit)"
                let result = try await env.execCommand(command: command, timeoutMs: 15_000, workingDir: nil, envVars: nil)
                let output = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
                return output.isEmpty ? "No matches found." : output
            }

            let text = try await env.grep(
                pattern: pattern,
                path: searchPath,
                options: GrepOptions(globFilter: glob, caseInsensitive: caseInsensitive, maxResults: limit + offset)
            )
            let lines = text.components(separatedBy: "\n")
            let sliced = Array(lines.dropFirst(offset).prefix(limit))
            if mode == "count" {
                let count = sliced.filter { !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }.count
                return "\(count)"
            }
            return sliced.joined(separator: "\n").trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                ? "No matches found."
                : sliced.joined(separator: "\n")
        }
    )
}

public func claudeNotebookEditTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "NotebookEdit",
            description: """
    Completely replaces the contents of a specific cell in a Jupyter notebook (.ipynb file) with new source. Jupyter notebooks are interactive documents that combine code, text, and visualizations, commonly used for data analysis and scientific computing.
    The notebook_path parameter must be an absolute path, not a relative path.
    The cell_id parameter targets a specific cell ID. For compatibility, cell_number (0-indexed) is also accepted.
    Use edit_mode=insert to add a new cell (after cell_id, or at beginning if no target is specified).
    Use edit_mode=delete to delete the targeted cell.
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "notebook_path": ["type": "string", "description": "The absolute path to the Jupyter notebook file to edit (must be absolute, not relative)"],
                    "cell_id": ["type": "string", "description": "The ID of the cell to edit. When inserting, the new cell is inserted after this cell."],
                    "cell_number": ["type": "number", "description": "Legacy compatibility: the 0-indexed cell number to edit"],
                    "new_source": ["type": "string", "description": "The new source for the cell"],
                    "cell_type": ["type": "string", "description": "The type of the cell (code or markdown). Required when edit_mode=insert."],
                    "edit_mode": ["type": "string", "description": "The type of edit to make (replace, insert, delete). Defaults to replace."],
                ] as [String: Any],
                "required": ["notebook_path", "new_source"],
            ] as [String: Any]
        ),
        executor: { args, env in
            try await claudeExecuteNotebookEdit(args: args, env: env, parentSession: parentSession)
        }
    )
}

// MARK: - Claude shell/task tools

public func claudeBashTool(defaultTimeoutMs: Int = 120_000, maxTimeoutMs: Int = 600_000) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "Bash",
            description: """
    Executes a given bash command with optional timeout. Working directory persists between commands; shell state (everything else) does not. The shell environment is initialized from the user's profile (bash or zsh).

    IMPORTANT: This tool is for terminal operations like git, npm, docker, etc. DO NOT use it for file operations (reading, writing, editing, searching, finding files) - use the specialized tools for this instead.

    Usage notes:
    - The command argument is required.
    - You can specify an optional timeout in milliseconds (up to 600000ms / 10 minutes). If not specified, commands will timeout after 120000ms (2 minutes).
    - It is very helpful if you write a clear, concise description of what this command does.
    - You can use the run_in_background parameter to run the command in the background.
    - Foreground commands run in a persistent shell session that maintains state (working directory, environment variables, functions, aliases) across calls.
    - Background commands run in isolated processes and do not share state.
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "command": ["type": "string", "description": "The command to execute"],
                    "description": ["type": "string", "description": "Clear, concise description of what this command does in active voice."],
                    "timeout": ["type": "number", "description": "Optional timeout in milliseconds (max 600000)"],
                    "run_in_background": ["type": "boolean", "description": "Set to true to run this command in the background. Use TaskOutput to read the output later."],
                    "dangerouslyDisableSandbox": ["type": "boolean", "description": "Set this to true to dangerously override sandbox mode and run commands without sandboxing."],
                ] as [String: Any],
                "required": ["command"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let command = args["command"] as? String else {
                throw ToolError.validationError("command is required")
            }

            let timeoutMs = min(max(1_000, claudeInt(args["timeout"]) ?? defaultTimeoutMs), maxTimeoutMs)
            let runInBackground = claudeBool(args["run_in_background"]) ?? false

            if runInBackground {
                let taskID = await ClaudeBackgroundTaskStore.shared.spawn(
                    command: command,
                    timeoutMs: timeoutMs,
                    env: env
                )
                return """
                Background task started with ID: \(taskID)
                Use TaskOutput with task_id="\(taskID)" to retrieve output.
                """
            }

            return try await claudeExecuteCommand(
                env: env,
                command: command,
                timeoutMs: timeoutMs,
                emitOutputDelta: { _ in }
            )
        },
        streamingExecutor: { args, env, emitOutputDelta in
            guard let command = args["command"] as? String else {
                throw ToolError.validationError("command is required")
            }

            let timeoutMs = min(max(1_000, claudeInt(args["timeout"]) ?? defaultTimeoutMs), maxTimeoutMs)
            let runInBackground = claudeBool(args["run_in_background"]) ?? false
            if runInBackground {
                let taskID = await ClaudeBackgroundTaskStore.shared.spawn(
                    command: command,
                    timeoutMs: timeoutMs,
                    env: env
                )
                return """
                Background task started with ID: \(taskID)
                Use TaskOutput with task_id="\(taskID)" to retrieve output.
                """
            }

            return try await claudeExecuteCommand(
                env: env,
                command: command,
                timeoutMs: timeoutMs,
                emitOutputDelta: emitOutputDelta
            )
        }
    )
}

public func claudeTaskStopTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "TaskStop",
            description: """
    - Stops a running background task by its ID
    - Takes a task_id parameter identifying the task to stop
    - Returns a success or failure status
    - Use this tool when you need to terminate a long-running task
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "task_id": ["type": "string", "description": "The ID of the background task to stop"],
                ] as [String: Any],
                "required": [],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let taskID = args["task_id"] as? String, !taskID.isEmpty else {
                throw ToolError.validationError("task_id is required")
            }

            if await ClaudeBackgroundTaskStore.shared.cancel(taskID: taskID) {
                return "Task \(taskID) stopped successfully."
            }

            if let parentSession, let handle = await parentSession.getSubagent(taskID) {
                await handle.session.close()
                await parentSession.removeSubagent(taskID)
                await ClaudeCoordinationStore.shared.unbindAgent(agentID: taskID)
                return "Task \(taskID) stopped successfully."
            }

            return "Task \(taskID) not found."
        }
    )
}

public func claudeKillShellTool(parentSession: Session? = nil) -> RegisteredTool {
    var tool = claudeTaskStopTool(parentSession: parentSession)
    tool.definition.name = "KillShell"
    tool.definition.description = """
    - Stops a running background task by its ID
    - Takes a task_id parameter identifying the task to stop
    - Returns a success or failure status
    - Use this tool when you need to terminate a long-running task
    """
    return tool
}

public func claudeTaskOutputTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "TaskOutput",
            description: """
    - Retrieves output from a running or completed task (background shell, agent, or remote session)
    - Takes a task_id parameter identifying the task
    - Returns the task output along with status information
    - Use block=true (default) to wait for task completion
    - Use block=false for non-blocking check of current status
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "task_id": ["type": "string", "description": "The task ID to get output from"],
                    "block": ["type": "boolean", "description": "Whether to wait for completion"],
                    "timeout": ["type": "number", "description": "Max wait time in ms"],
                ] as [String: Any],
                "required": ["task_id"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let taskID = args["task_id"] as? String else {
                throw ToolError.validationError("task_id is required")
            }
            let block = claudeBool(args["block"]) ?? true
            let timeoutMs = max(100, claudeInt(args["timeout"]) ?? 30_000)

            if let background = await ClaudeBackgroundTaskStore.shared.get(taskID: taskID, block: block, timeoutMs: timeoutMs) {
                return """
                Task \(taskID):
                Status: \(background.status)

                Output:
                \(background.output.isEmpty ? "[No output yet]" : background.output)
                """
            }

            guard let parentSession else {
                return "Task \(taskID) not found."
            }
            guard let handle = await parentSession.getSubagent(taskID) else {
                return "Task \(taskID) not found."
            }

            if block {
                let deadline = Date().addingTimeInterval(Double(timeoutMs) / 1000.0)
                while Date() < deadline {
                    let state = await handle.session.getState()
                    if state == .idle || state == .closed {
                        break
                    }
                    try await Task.sleep(nanoseconds: 250_000_000)
                }
            }

            let state = await handle.session.getState()
            let history = await handle.session.getHistory()
            let output = claudeLastAssistantOutput(from: history)
            let status = state == .idle ? "completed" : (state == .closed ? "closed" : "running")

            return """
            Task \(taskID):
            Status: \(status)

            Output:
            \(output.isEmpty ? "[No output yet]" : output)
            """
        }
    )
}

// MARK: - Claude web tools

public func claudeWebFetchTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "WebFetch",
            description: """
    - Fetches content from a specified URL and processes it using an AI model
    - Takes a URL and a prompt as input
    - Fetches the URL content, converts HTML to markdown
    - Processes the content with the prompt using a small, fast model
    - Returns the model's response about the content
    - Use this tool when you need to retrieve and analyze web content

    Usage notes:
      - IMPORTANT: If an MCP-provided web fetch tool is available, prefer using that tool instead of this one, as it may have fewer restrictions.
      - The URL must be a fully-formed valid URL
      - HTTP URLs will be automatically upgraded to HTTPS
      - The prompt should describe what information you want to extract from the page
      - This tool is read-only and does not modify any files
      - Results may be summarized if the content is very large
      - Includes a self-cleaning 15-minute cache for faster responses when repeatedly accessing the same URL
      - When a URL redirects to a different host, the tool will inform you and provide the redirect URL in a special format. You should then make a new WebFetch request with the redirect URL to fetch the content.
      - For GitHub URLs, prefer using the gh CLI via Bash instead (e.g., gh pr view, gh issue view, gh api).
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "url": ["type": "string", "description": "The URL to fetch content from"],
                    "prompt": ["type": "string", "description": "The prompt to run on the fetched content"],
                ] as [String: Any],
                "required": ["url", "prompt"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let url = args["url"] as? String else {
                throw ToolError.validationError("url is required")
            }
            guard let prompt = args["prompt"] as? String else {
                throw ToolError.validationError("prompt is required")
            }

            let cacheKey = "\(url)\n\(prompt)"
            if let cached = await ClaudeWebCacheStore.shared.get(key: cacheKey) {
                return cached
            }

            let loader = WebFetchLoader(maxRedirects: 10, allowCrossHostRedirects: true, timeout: 30)
            let result = try await loader.fetch(url)
            let cleaned = stripHTMLForToolOutput(result.content)
            let sourceURL = result.finalURL.absoluteString

            if result.wasRedirected, result.originalURL.host != result.finalURL.host {
                let message = """
                WebFetch redirect notice:
                The requested URL redirected to a different host.
                Redirect URL: \(sourceURL)

                Make a new WebFetch call with this redirect URL to fetch content.
                """
                await ClaudeWebCacheStore.shared.set(key: cacheKey, value: message)
                return message
            }

            let processed = await claudeProcessWebContent(prompt: prompt, content: cleaned, sourceURL: sourceURL)
            let output: String
            if let processed {
                output = """
                Fetched from: \(sourceURL)

                \(processed)
                """
            } else {
                let truncated = cleaned.count > 50_000
                    ? String(cleaned.prefix(50_000)) + "\n\n[Content truncated...]"
                    : cleaned
                output = """
                Fetched from: \(sourceURL)

                Prompt: \(prompt)

                Content:
                \(truncated)
                """
            }

            await ClaudeWebCacheStore.shared.set(key: cacheKey, value: output)
            return output
        }
    )
}

public func claudeWebSearchTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "WebSearch",
            description: """
    - Allows Claude to search the web and use the results to inform responses
    - Provides up-to-date information for current events and recent data
    - Returns search result information formatted as search result blocks, including links as markdown hyperlinks
    - Use this tool for accessing information beyond Claude's knowledge cutoff
    - Searches are performed automatically within a single API call

    CRITICAL REQUIREMENT - You MUST follow this:
      - After answering the user's question, you MUST include a "Sources:" section at the end of your response
      - In the Sources section, list all relevant URLs from the search results as markdown hyperlinks: [Title](URL)
      - This is MANDATORY - never skip including sources in your response
      - Example format:

        [Your answer here]

        Sources:
        - [Source Title 1](https://example.com/1)
        - [Source Title 2](https://example.com/2)

    Usage notes:
      - Domain filtering is supported to include or block specific websites
      - Web search is only available in the US

    IMPORTANT - Use the correct year in search queries:
      - The current month is February 2026. You MUST use this year when searching for recent information, documentation, or current events.
      - Example: If the user asks for "latest React docs", search for "React documentation" with the current year, NOT last year
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "query": ["type": "string", "description": "The search query to use"],
                    "allowed_domains": [
                        "type": "array",
                        "description": "Only include search results from these domains",
                        "items": ["type": "string", "description": "Domain name"],
                    ] as [String: Any],
                    "blocked_domains": [
                        "type": "array",
                        "description": "Never include search results from these domains",
                        "items": ["type": "string", "description": "Domain name"],
                    ] as [String: Any],
                ] as [String: Any],
                "required": ["query"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let query = args["query"] as? String else {
                throw ToolError.validationError("query is required")
            }

            let results = try await WebSearchClient.search(
                query: query,
                allowedDomains: claudeStringArray(args["allowed_domains"]),
                blockedDomains: claudeStringArray(args["blocked_domains"]),
                maxResults: 10
            )

            guard !results.isEmpty else {
                return "Search results for: \(query)\n\nNo results found for this query."
            }

            var output = "Search results for: \(query)\n\n"
            for (index, result) in results.enumerated() {
                output += "\(index + 1). [\(result.title)](\(result.url))\n"
                if !result.snippet.isEmpty {
                    output += "   \(result.snippet)\n"
                }
                output += "\n"
            }

            output += "Sources:\n"
            for result in results {
                output += "- [\(result.title)](\(result.url))\n"
            }
            return output.trimmingCharacters(in: .whitespacesAndNewlines)
        }
    )
}

// MARK: - Claude agent tools

public func claudeTaskTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "Task",
            description: """
    Launch a new agent to handle complex, multi-step tasks autonomously.

    The Task tool launches specialized agents (subprocesses) that autonomously handle complex tasks. Each agent type has specific capabilities and tools available to it.

    Available agent types:
    - Bash: Command execution specialist for running bash commands. Use for git operations, command execution, and terminal tasks.
    - general-purpose: General-purpose agent for researching complex questions, searching for code, and executing multi-step tasks.
    - Explore: Fast agent specialized for exploring codebases. Use for quick file searches, code searches, or codebase questions.
    - Plan: Software architect agent for designing implementation plans.

    Usage notes:
    - Always include a short description (3-5 words) summarizing what the agent will do
    - Launch multiple agents concurrently whenever possible, to maximize performance
    - When the agent is done, it will return a single message back to you
    - You can run agents in the background using the run_in_background parameter
    - Agents can be resumed using the resume parameter by passing the agent ID from a previous invocation
    - Provide clear, detailed prompts so the agent can work autonomously
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "description": ["type": "string", "description": "A short (3-5 word) description of the task"],
                    "prompt": ["type": "string", "description": "The task for the agent to perform"],
                    "subagent_type": ["type": "string", "description": "The type of specialized agent to use for this task"],
                    "name": ["type": "string", "description": "Optional teammate name when spawning into a team"],
                    "team_name": ["type": "string", "description": "Optional team name this agent should join"],
                    "model": ["type": "string", "description": "Optional model to use for this agent. If not specified, inherits from parent. Options: sonnet, opus, haiku"],
                    "run_in_background": ["type": "boolean", "description": "Set to true to run this agent in the background. The tool result will include an output_file path."],
                    "resume": ["type": "string", "description": "Optional agent ID to resume from. If provided, the agent will continue from the previous execution transcript."],
                    "isolation": ["type": "string", "description": "Optional isolation mode. Use \"worktree\" to run in an isolated git worktree."],
                    "max_turns": ["type": "integer", "description": "Maximum number of agentic turns (API round-trips) before stopping."],
                ] as [String: Any],
                "required": ["description", "prompt", "subagent_type"],
            ] as [String: Any]
        ),
        executor: { args, env in
            guard let parentSession else {
                return "Cannot spawn Task: session context unavailable."
            }
            guard let prompt = args["prompt"] as? String else {
                throw ToolError.validationError("prompt is required")
            }

            let sessionID = parentSession.id
            let runInBackground = claudeBool(args["run_in_background"]) ?? false
            let maxTurns = max(1, claudeInt(args["max_turns"]) ?? 10)
            let resumeID = args["resume"] as? String
            let requestedIsolation = (args["isolation"] as? String)?.lowercased()
            let teammateName = args["name"] as? String
            let teamName = args["team_name"] as? String

            if let teamName, !teamName.isEmpty {
                await ClaudeCoordinationStore.shared.attachSession(sessionID: sessionID, toTeam: teamName)
                if let teammateName, !teammateName.isEmpty {
                    await ClaudeCoordinationStore.shared.addMember(name: teammateName, teamName: teamName)
                }
            }

            if let resumeID, !resumeID.isEmpty, let existing = await parentSession.getSubagent(resumeID) {
                Task {
                    await existing.session.submit(prompt)
                }
                if runInBackground {
                    return """
                    Background task resumed with ID: \(resumeID)
                    Use TaskOutput with task_id="\(resumeID)" to retrieve results.
                    """
                }

                var attempts = 0
                while attempts < 600 {
                    let state = await existing.session.getState()
                    if state == .idle || state == .closed {
                        break
                    }
                    try await Task.sleep(nanoseconds: 1_000_000_000)
                    attempts += 1
                }

                let history = await existing.session.getHistory()
                let output = claudeLastAssistantOutput(from: history)
                return output.isEmpty ? "[Task completed with no output]\n\nAgent ID: \(resumeID)" : "\(output)\n\nAgent ID: \(resumeID)"
            }

            let currentDepth = await parentSession.currentDepth()
            let maxDepth = await parentSession.config.maxSubagentDepth
            guard currentDepth < maxDepth else {
                return "Error: Maximum subagent depth (\(maxDepth)) reached."
            }

            let agentID = UUID().uuidString
            let profile = parentSession.providerProfile
            let client = parentSession.llmClient
            var subConfig = SessionConfig(maxTurns: maxTurns)
            subConfig.reasoningEffort = await parentSession.config.reasoningEffort
            subConfig.interactiveMode = false

            let subEnv: ExecutionEnvironment
            if requestedIsolation == "worktree" {
                subEnv = try await createGitWorktreeEnvironment(from: env, agentID: agentID)
            } else {
                subEnv = env
            }

            let subSession = try Session(
                profile: profile,
                environment: subEnv,
                client: client,
                config: subConfig,
                depth: currentDepth + 1
            )
            let handle = SubAgentHandle(id: agentID, session: subSession)
            await parentSession.registerSubagent(handle)

            if let teamName, !teamName.isEmpty, let teammateName, !teammateName.isEmpty {
                await ClaudeCoordinationStore.shared.bindAgent(
                    sessionID: sessionID,
                    teamName: teamName,
                    memberName: teammateName,
                    agentID: handle.id
                )
            }

            Task {
                await subSession.submit(prompt)
            }

            let isolationNote = requestedIsolation == "worktree"
                ? "\nIsolation: worktree at \(subEnv.workingDirectory())"
                : ""

            if runInBackground {
                return """
                Background task started with ID: \(handle.id)
                Use TaskOutput with task_id="\(handle.id)" to retrieve results.
                \(isolationNote)
                """
            }

            var attempts = 0
            while attempts < 600 {
                let state = await subSession.getState()
                if state == .idle || state == .closed {
                    break
                }
                try await Task.sleep(nanoseconds: 1_000_000_000)
                attempts += 1
            }

            let history = await subSession.getHistory()
            let output = claudeLastAssistantOutput(from: history)
            let body = output.isEmpty ? "[Task completed with no output]" : output
            if requestedIsolation == "worktree" {
                await subSession.close()
                await parentSession.removeSubagent(handle.id)
                await ClaudeCoordinationStore.shared.unbindAgent(agentID: handle.id)
                return "\(body)\n\nAgent ID: \(handle.id)\nIsolation: worktree cleaned up"
            }
            return "\(body)\n\nAgent ID: \(handle.id)\(isolationNote)"
        }
    )
}

public func claudeTaskCreateTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "TaskCreate",
            description: """
    Create a task in the active task list. If a team is active, this creates a team task.
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "subject": ["type": "string", "description": "A brief title for the task"],
                    "description": ["type": "string", "description": "Optional detailed description"],
                    "status": ["type": "string", "description": "Initial status (pending, in_progress, completed, blocked, cancelled)"],
                    "owner": ["type": "string", "description": "Optional owner name"],
                    "blockedBy": [
                        "type": "array",
                        "description": "Task IDs this task is blocked by",
                        "items": ["type": "string"],
                    ] as [String: Any],
                ] as [String: Any],
                "required": ["subject"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let subject = args["subject"] as? String, !subject.isEmpty else {
                throw ToolError.validationError("subject is required")
            }
            let sessionID = parentSession?.id ?? "global"
            let task = await ClaudeCoordinationStore.shared.createTask(
                sessionID: sessionID,
                subject: subject,
                details: args["description"] as? String,
                status: claudeTaskStatus(args["status"] as? String),
                owner: args["owner"] as? String,
                blockedBy: claudeStringArray(args["blockedBy"]) ?? []
            )
            return claudeRenderTask(task)
        }
    )
}

public func claudeTaskGetTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "TaskGet",
            description: "Get a task by ID from the active task list.",
            parameters: [
                "type": "object",
                "properties": [
                    "taskId": ["type": "string", "description": "The ID of the task to retrieve"],
                ] as [String: Any],
                "required": ["taskId"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let taskID = args["taskId"] as? String, !taskID.isEmpty else {
                throw ToolError.validationError("taskId is required")
            }
            let sessionID = parentSession?.id ?? "global"
            guard let task = await ClaudeCoordinationStore.shared.getTask(sessionID: sessionID, taskID: taskID) else {
                return "Task \(taskID) not found."
            }
            return claudeRenderTask(task)
        }
    )
}

public func claudeTaskListTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "TaskList",
            description: "List all tasks in the active task list.",
            parameters: [
                "type": "object",
                "properties": [:] as [String: Any],
                "required": [],
            ] as [String: Any]
        ),
        executor: { _, _ in
            let sessionID = parentSession?.id ?? "global"
            let tasks = await ClaudeCoordinationStore.shared.listTasks(sessionID: sessionID)
            guard !tasks.isEmpty else {
                return "No tasks in the active task list."
            }
            return tasks.map(claudeRenderTask).joined(separator: "\n\n")
        }
    )
}

public func claudeTaskUpdateTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "TaskUpdate",
            description: "Update a task in the active task list.",
            parameters: [
                "type": "object",
                "properties": [
                    "taskId": ["type": "string", "description": "Task ID to update"],
                    "subject": ["type": "string", "description": "Updated task title"],
                    "description": ["type": "string", "description": "Updated task details"],
                    "status": ["type": "string", "description": "Updated status"],
                    "owner": ["type": "string", "description": "Updated owner; pass empty string to clear"],
                    "blockedBy": [
                        "type": "array",
                        "description": "Updated dependency list",
                        "items": ["type": "string"],
                    ] as [String: Any],
                ] as [String: Any],
                "required": ["taskId"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let taskID = args["taskId"] as? String, !taskID.isEmpty else {
                throw ToolError.validationError("taskId is required")
            }

            let owner: String?
            if let rawOwner = args["owner"] as? String {
                owner = rawOwner.isEmpty ? nil : rawOwner
            } else {
                owner = nil
            }

            let sessionID = parentSession?.id ?? "global"
            let updated = await ClaudeCoordinationStore.shared.updateTask(
                sessionID: sessionID,
                taskID: taskID,
                subject: args["subject"] as? String,
                details: args["description"] as? String,
                status: claudeTaskStatus(args["status"] as? String),
                owner: args.keys.contains("owner") ? (owner ?? "") : nil,
                blockedBy: args.keys.contains("blockedBy") ? (claudeStringArray(args["blockedBy"]) ?? []) : nil
            )
            guard let updated else {
                return "Task \(taskID) not found."
            }
            return claudeRenderTask(updated)
        }
    )
}

public func claudeTeamCreateTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "TeamCreate",
            description: """
    # TeamCreate

    Create a new team to coordinate multiple agents working on a project. Teams have a 1:1 correspondence with task lists (Team = TaskList).
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "team_name": ["type": "string", "description": "Name for the new team to create."],
                    "description": ["type": "string", "description": "Optional team description"],
                ] as [String: Any],
                "required": ["team_name"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let teamName = args["team_name"] as? String, !teamName.isEmpty else {
                throw ToolError.validationError("team_name is required")
            }
            let sessionID = parentSession?.id ?? "global"
            let team = await ClaudeCoordinationStore.shared.createOrAttachTeam(
                sessionID: sessionID,
                teamName: teamName,
                description: args["description"] as? String
            )
            return """
            Team created and active:
            - team_name: \(team.name)
            - description: \(team.description ?? "")
            - task_list: \(team.taskListKey)
            """
        }
    )
}

public func claudeTeamDeleteTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "TeamDelete",
            description: """
    # TeamDelete

    Remove team and task directories when the swarm work is complete.

    **IMPORTANT**: TeamDelete will fail if the team still has active members. Gracefully terminate teammates first, then call TeamDelete after all teammates have shut down.
    """,
            parameters: [
                "type": "object",
                "properties": [:] as [String: Any],
                "required": [],
            ] as [String: Any]
        ),
        executor: { _, _ in
            let sessionID = parentSession?.id ?? "global"
            return await ClaudeCoordinationStore.shared.deleteActiveTeam(sessionID: sessionID)
        }
    )
}

public func claudeSendMessageTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "SendMessage",
            description: """
    # SendMessageTool

    Send messages to agent teammates and handle protocol requests/responses in a team.
    """,
            parameters: [
                "type": "object",
                "oneOf": [
                    [
                        "type": "object",
                        "properties": [
                            "type": ["type": "string", "enum": ["message"]],
                            "recipient": ["type": "string", "description": "Teammate name"],
                            "content": ["type": "string", "description": "Message content"],
                            "summary": ["type": "string", "description": "5-10 word preview"],
                        ] as [String: Any],
                        "required": ["type", "recipient", "content", "summary"],
                    ] as [String: Any],
                    [
                        "type": "object",
                        "properties": [
                            "type": ["type": "string", "enum": ["broadcast"]],
                            "content": ["type": "string", "description": "Broadcast content"],
                            "summary": ["type": "string", "description": "5-10 word preview"],
                        ] as [String: Any],
                        "required": ["type", "content", "summary"],
                    ] as [String: Any],
                    [
                        "type": "object",
                        "properties": [
                            "type": ["type": "string", "enum": ["shutdown_request"]],
                            "recipient": ["type": "string", "description": "Teammate name"],
                            "content": ["type": "string", "description": "Optional reason"],
                        ] as [String: Any],
                        "required": ["type", "recipient"],
                    ] as [String: Any],
                    [
                        "type": "object",
                        "properties": [
                            "type": ["type": "string", "enum": ["shutdown_response"]],
                            "request_id": ["type": "string", "description": "Shutdown request ID"],
                            "approve": ["type": "boolean", "description": "Approve or reject"],
                            "content": ["type": "string", "description": "Optional rejection reason"],
                        ] as [String: Any],
                        "required": ["type", "request_id", "approve"],
                    ] as [String: Any],
                    [
                        "type": "object",
                        "properties": [
                            "type": ["type": "string", "enum": ["plan_approval_response"]],
                            "request_id": ["type": "string", "description": "Plan request ID"],
                            "recipient": ["type": "string", "description": "Teammate name"],
                            "approve": ["type": "boolean", "description": "Approve or reject"],
                            "content": ["type": "string", "description": "Optional rejection feedback"],
                        ] as [String: Any],
                        "required": ["type", "request_id", "recipient", "approve"],
                    ] as [String: Any],
                ] as [Any],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let type = args["type"] as? String else {
                throw ToolError.validationError("type is required")
            }
            let sessionID = parentSession?.id ?? "global"
            let sender = await ClaudeCoordinationStore.shared.senderName(for: sessionID)

            switch type {
            case "message":
                guard let recipient = args["recipient"] as? String, !recipient.isEmpty else {
                    throw ToolError.validationError("recipient is required for type=message")
                }
                guard let content = args["content"] as? String, !content.isEmpty else {
                    throw ToolError.validationError("content is required for type=message")
                }
                guard let summary = args["summary"] as? String, !summary.isEmpty else {
                    throw ToolError.validationError("summary is required for type=message")
                }
                await ClaudeCoordinationStore.shared.sendMessage(
                    sessionID: sessionID,
                    from: sender,
                    to: recipient,
                    content: content,
                    summary: summary
                )
                return "Message sent to \(recipient)."
            case "broadcast":
                guard let content = args["content"] as? String, !content.isEmpty else {
                    throw ToolError.validationError("content is required for type=broadcast")
                }
                guard let summary = args["summary"] as? String, !summary.isEmpty else {
                    throw ToolError.validationError("summary is required for type=broadcast")
                }
                let count = await ClaudeCoordinationStore.shared.broadcast(
                    sessionID: sessionID,
                    from: sender,
                    content: content,
                    summary: summary
                )
                return "Broadcast sent to \(count) teammate(s)."
            case "shutdown_request":
                guard let recipient = args["recipient"] as? String, !recipient.isEmpty else {
                    throw ToolError.validationError("recipient is required for type=shutdown_request")
                }
                let content = args["content"] as? String
                let requestID = await ClaudeCoordinationStore.shared.createProtocolRequest(
                    sessionID: sessionID,
                    from: sender,
                    to: recipient,
                    type: "shutdown_request",
                    content: content
                )
                return "Shutdown request sent to \(recipient). request_id=\(requestID)"
            case "shutdown_response":
                guard let requestID = args["request_id"] as? String, !requestID.isEmpty else {
                    throw ToolError.validationError("request_id is required for type=shutdown_response")
                }
                guard let approve = claudeBool(args["approve"]) else {
                    throw ToolError.validationError("approve is required for type=shutdown_response")
                }
                let content = args["content"] as? String
                let ok = await ClaudeCoordinationStore.shared.resolveProtocolRequest(
                    sessionID: sessionID,
                    requestID: requestID,
                    approver: sender,
                    approve: approve,
                    content: content
                )
                return ok
                    ? "Shutdown response recorded for request_id=\(requestID). approve=\(approve)"
                    : "No protocol request found for request_id=\(requestID)."
            case "plan_approval_response":
                guard let requestID = args["request_id"] as? String, !requestID.isEmpty else {
                    throw ToolError.validationError("request_id is required for type=plan_approval_response")
                }
                guard let recipient = args["recipient"] as? String, !recipient.isEmpty else {
                    throw ToolError.validationError("recipient is required for type=plan_approval_response")
                }
                guard let approve = claudeBool(args["approve"]) else {
                    throw ToolError.validationError("approve is required for type=plan_approval_response")
                }
                let content = args["content"] as? String
                let ok = await ClaudeCoordinationStore.shared.resolveProtocolRequest(
                    sessionID: sessionID,
                    requestID: requestID,
                    approver: sender,
                    approve: approve,
                    content: content
                )
                if ok {
                    await ClaudeCoordinationStore.shared.sendMessage(
                        sessionID: sessionID,
                        from: sender,
                        to: recipient,
                        content: approve ? "Plan approved." : (content ?? "Plan rejected."),
                        summary: approve ? "Plan approved" : "Plan rejected"
                    )
                }
                return ok
                    ? "Plan approval response sent to \(recipient). request_id=\(requestID)"
                    : "No protocol request found for request_id=\(requestID)."
            default:
                throw ToolError.validationError("Unsupported message type: \(type)")
            }
        }
    )
}

public func claudeToolSearchTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "ToolSearch",
            description: "Search available tools. Use \"select:<tool_name>\" for direct selection, or a keyword query.",
            parameters: [
                "type": "object",
                "properties": [
                    "query": ["type": "string", "description": "Query to find deferred tools. Use \"select:<tool_name>\" for direct selection, or keywords to search."],
                ] as [String: Any],
                "required": ["query"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let query = args["query"] as? String, !query.isEmpty else {
                throw ToolError.validationError("query is required")
            }

            guard let parentSession else {
                return "Tool search unavailable: session context not found."
            }

            let defs = parentSession.providerProfile.toolRegistry.definitions()
            if query.hasPrefix("select:") {
                let name = String(query.dropFirst("select:".count)).trimmingCharacters(in: .whitespacesAndNewlines)
                if let exact = defs.first(where: { $0.name == name }) {
                    return "Selected tool: \(exact.name)\n\(exact.description)"
                }
                return "Tool not found: \(name)"
            }

            let needle = query.lowercased()
            let matches = defs.filter {
                $0.name.lowercased().contains(needle) || $0.description.lowercased().contains(needle)
            }
            guard !matches.isEmpty else {
                return "No tools matched query: \(query)"
            }

            return matches
                .sorted(by: { $0.name < $1.name })
                .map { "- \($0.name): \($0.description.split(separator: "\n").first ?? "")" }
                .joined(separator: "\n")
        }
    )
}

public func claudeTodoWriteTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "TodoWrite",
            description: """
    Use this tool to create and manage a structured task list for your current coding session. This helps you track progress, organize complex tasks, and demonstrate thoroughness to the user.

    ## When to Use This Tool
    Use this tool in these scenarios:
    1. Complex multi-step tasks - When a task requires 3 or more distinct steps
    2. Non-trivial and complex tasks - Tasks that require careful planning
    3. User explicitly requests todo list
    4. User provides multiple tasks

    ## When NOT to Use This Tool
    Skip using this tool when:
    1. There is only a single, straightforward task
    2. The task is trivial and tracking provides no benefit
    3. The task can be completed in less than 3 trivial steps

    ## Task States
    - pending: Task not yet started
    - in_progress: Currently working on (limit to ONE task at a time)
    - completed: Task finished successfully

    IMPORTANT: Task descriptions must have two forms:
    - content: The imperative form (e.g., "Run tests")
    - activeForm: The present continuous form (e.g., "Running tests")
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "todos": [
                        "type": "array",
                        "description": "The updated todo list",
                        "items": [
                            "type": "object",
                            "properties": [
                                "content": ["type": "string", "description": "The task description in imperative form"],
                                "status": ["type": "string", "description": "Task status: pending, in_progress, or completed"],
                                "activeForm": ["type": "string", "description": "The task description in present continuous form"],
                            ] as [String: Any],
                            "required": ["content", "status", "activeForm"],
                        ] as [String: Any],
                    ] as [String: Any],
                ] as [String: Any],
                "required": ["todos"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let todos = args["todos"] as? [Any], !todos.isEmpty else {
                throw ToolError.validationError("todos is required")
            }

            var output = "Todo list updated:\n\n"
            var completed = 0
            for (index, rawTodo) in todos.enumerated() {
                guard let todo = rawTodo as? [String: Any] else { continue }
                let content = (todo["content"] as? String) ?? "Task \(index + 1)"
                let status = ((todo["status"] as? String) ?? (todo["state"] as? String) ?? "pending").lowercased()
                let icon: String
                switch status {
                case "completed":
                    icon = "[x]"
                    completed += 1
                case "in_progress":
                    icon = "[~]"
                default:
                    icon = "[ ]"
                }
                output += "\(index + 1). \(icon) \(content)\n"
            }
            output += "\nProgress: \(completed)/\(todos.count) completed"
            return output
        }
    )
}

public func claudeAskUserTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "AskUserQuestion",
            description: """
    Use this tool when you need to ask the user questions during execution. This allows you to:
    1. Gather user preferences or requirements
    2. Clarify ambiguous instructions
    3. Get decisions on implementation choices as you work
    4. Offer choices to the user about what direction to take

    Usage notes:
    - Users will always be able to select "Other" to provide custom text input
    - Use multiSelect: true to allow multiple answers to be selected
    - If you recommend a specific option, make that the first option and add "(Recommended)" at the end

    Plan mode note: In plan mode, use this tool to clarify requirements or choose between approaches BEFORE finalizing your plan. Do NOT use this tool to ask "Is my plan ready?" or "Should I proceed?" - use ExitPlanMode for plan approval. IMPORTANT: Do not reference "the plan" in your questions (e.g., "Do you have feedback about the plan?", "Does the plan look good?") because the user cannot see the plan in the UI until you call ExitPlanMode. If you need plan approval, use ExitPlanMode instead.
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "questions": [
                        "type": "array",
                        "description": "Questions to ask the user (1-4 questions)",
                        "items": [
                            "type": "object",
                            "properties": [
                                "question": ["type": "string", "description": "The complete question to ask the user"],
                                "header": ["type": "string", "description": "Very short label displayed as a chip/tag (max 12 chars)"],
                                "options": [
                                    "type": "array",
                                    "description": "The available choices (2-4 options)",
                                    "items": [
                                        "type": "object",
                                        "properties": [
                                            "label": ["type": "string", "description": "The display text for this option"],
                                            "description": ["type": "string", "description": "Explanation of what this option means"],
                                        ] as [String: Any],
                                        "required": ["label", "description"],
                                    ] as [String: Any],
                                ] as [String: Any],
                                "multiSelect": ["type": "boolean", "description": "Whether multiple options can be selected"],
                            ] as [String: Any],
                            "required": ["question", "header", "options", "multiSelect"],
                        ] as [String: Any],
                    ] as [String: Any],
                ] as [String: Any],
                "required": ["questions"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            if let parentSession, !(await parentSession.config.interactiveMode) {
                return "AskUserQuestion is disabled because OmniKit is running in non-interactive mode."
            }

            guard let questions = args["questions"] as? [Any], !questions.isEmpty else {
                throw ToolError.validationError("questions is required")
            }

            var output = "Questions for user:\n\n"
            for (index, rawQuestion) in questions.enumerated() {
                guard let question = rawQuestion as? [String: Any] else { continue }
                let text = (question["question"] as? String) ?? "Question \(index + 1)"
                let header = (question["header"] as? String) ?? "Question"
                output += "Q\(index + 1): \(text)\n"
                output += "[\(header)]\n"
                if let options = question["options"] as? [Any] {
                    for (optIndex, rawOption) in options.enumerated() {
                        guard let option = rawOption as? [String: Any] else { continue }
                        let label = (option["label"] as? String) ?? "Option \(optIndex + 1)"
                        let description = option["description"] as? String
                        output += "  \(optIndex + 1). \(label)"
                        if let description, !description.isEmpty {
                            output += " - \(description)"
                        }
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

public func claudeEnterPlanModeTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "EnterPlanMode",
            description: """
    Use this tool proactively when you're about to start a non-trivial implementation task. Getting user sign-off on your approach before writing code prevents wasted effort and ensures alignment. This tool transitions you into plan mode where you can explore the codebase and design an implementation approach for user approval.

    ## When to Use This Tool

    **Prefer using EnterPlanMode** for implementation tasks unless they're simple. Use it when ANY of these conditions apply:

    1. **New Feature Implementation**: Adding meaningful new functionality
       - Example: "Add a logout button" - where should it go? What should happen on click?
       - Example: "Add form validation" - what rules? What error messages?

    2. **Multiple Valid Approaches**: The task can be solved in several different ways
       - Example: "Add caching to the API" - could use Redis, in-memory, file-based, etc.
       - Example: "Improve performance" - many optimization strategies possible

    3. **Code Modifications**: Changes that affect existing behavior or structure
       - Example: "Update the login flow" - what exactly should change?
       - Example: "Refactor this component" - what's the target architecture?

    4. **Architectural Decisions**: The task requires choosing between patterns or technologies
       - Example: "Add real-time updates" - WebSockets vs SSE vs polling
       - Example: "Implement state management" - Redux vs Context vs custom solution

    5. **Multi-File Changes**: The task will likely touch more than 2-3 files
       - Example: "Refactor the authentication system"
       - Example: "Add a new API endpoint with tests"

    6. **Unclear Requirements**: You need to explore before understanding the full scope
       - Example: "Make the app faster" - need to profile and identify bottlenecks
       - Example: "Fix the bug in checkout" - need to investigate root cause

    7. **User Preferences Matter**: The implementation could reasonably go multiple ways
       - If you would use AskUserQuestion to clarify the approach, use EnterPlanMode instead
       - Plan mode lets you explore first, then present options with context

    ## When NOT to Use This Tool

    Only skip EnterPlanMode for simple tasks:
    - Single-line or few-line fixes (typos, obvious bugs, small tweaks)
    - Adding a single function with clear requirements
    - Tasks where the user has given very specific, detailed instructions
    - Pure research/exploration tasks (use the Task tool with explore agent instead)

    ## What Happens in Plan Mode

    In plan mode, you'll:
    1. Thoroughly explore the codebase using Glob, Grep, and Read tools
    2. Understand existing patterns and architecture
    3. Design an implementation approach
    4. Present your plan to the user for approval
    5. Use AskUserQuestion if you need to clarify approaches
    6. Exit plan mode with ExitPlanMode when ready to implement

    ## Important Notes

    - This tool REQUIRES user approval - they must consent to entering plan mode
    - If unsure whether to use it, err on the side of planning - it's better to get alignment upfront than to redo work
    - Users appreciate being consulted before significant changes are made to their codebase
    """,
            parameters: ["type": "object", "properties": [:] as [String: Any], "required": []] as [String: Any]
        ),
        executor: { _, env in
            let sessionID = parentSession?.id ?? "global"
            let state = await ClaudePlanModeStore.shared.enter(sessionID: sessionID, workingDirectory: env.workingDirectory())

            if !(await env.fileExists(path: state.planFilePath)) {
                let template = """
                # Implementation Plan

                ## Goal
                -

                ## Files To Change
                -

                ## Steps
                1.

                ## Risks
                -
                """
                try await env.writeFile(path: state.planFilePath, content: template)
            }

            if let parentSession {
                await parentSession.addSystemReminder("""
                Plan mode is now active.
                Write your plan to \(state.planFilePath).
                Do not implement code changes until you call ExitPlanMode.
                """)
            }

            return """
            Plan mode activated.
            Plan file: \(state.planFilePath)
            Draft your implementation plan in that file, then call ExitPlanMode.
            """
        }
    )
}

public func claudeExitPlanModeTool(parentSession: Session? = nil) -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "ExitPlanMode",
            description: """
    Use this tool when you are in plan mode and have finished writing your plan to the plan file and are ready for user approval.

    ## How This Tool Works
    - You should have already written your plan to the plan file specified in the plan mode system message
    - This tool does NOT take the plan content as a parameter - it will read the plan from the file you wrote
    - This tool simply signals that you're done planning and ready for the user to review and approve

    ## When to Use This Tool
    IMPORTANT: Only use this tool when the task requires planning the implementation steps of a task that requires writing code. For research tasks where you're gathering information, searching files, reading files or in general trying to understand the codebase - do NOT use this tool.
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "allowedPrompts": [
                        "type": "array",
                        "description": "Prompt-based permissions needed to implement the plan.",
                        "items": [
                            "type": "object",
                            "properties": [
                                "tool": ["type": "string", "enum": ["Bash"]],
                                "prompt": ["type": "string", "description": "Semantic description of the allowed action"],
                            ] as [String: Any],
                            "required": ["tool", "prompt"],
                        ] as [String: Any],
                    ] as [String: Any],
                ] as [String: Any],
                "required": [],
            ] as [String: Any]
        ),
        executor: { args, env in
            let sessionID = parentSession?.id ?? "global"
            guard let state = await ClaudePlanModeStore.shared.get(sessionID: sessionID), state.isActive else {
                return "Plan mode is not active."
            }

            guard await env.fileExists(path: state.planFilePath) else {
                throw ToolError.fileNotFound(state.planFilePath)
            }

            let rawPlan = try await claudeReadRawTextFile(path: state.planFilePath, env: env)
            let plan = rawPlan.trimmingCharacters(in: .whitespacesAndNewlines)
            if plan.isEmpty {
                throw ToolError.validationError("Plan file is empty. Write your plan before calling ExitPlanMode.")
            }

            let allowedPrompts = claudeParseAllowedPrompts(args["allowedPrompts"])
            await ClaudePlanModeStore.shared.exit(sessionID: sessionID)

            let interactive = await parentSession?.config.interactiveMode ?? false
            if let parentSession {
                if interactive {
                    await parentSession.addSystemReminder("ExitPlanMode was called. Wait for explicit user approval before implementing.")
                } else {
                    await parentSession.addSystemReminder("ExitPlanMode was called in non-interactive mode. The plan is auto-approved; proceed with implementation.")
                }
            }

            var output: [String] = []
            output.append(interactive ? "Plan ready for user approval." : "Plan approved (non-interactive mode).")
            output.append("Plan file: \(state.planFilePath)")
            if !allowedPrompts.isEmpty {
                output.append("Allowed prompts:")
                for item in allowedPrompts {
                    output.append("- \(item.tool): \(item.prompt)")
                }
            }
            output.append("")
            output.append("Plan content:")
            output.append(plan)
            return output.joined(separator: "\n")
        }
    )
}

public func claudeSkillTool() -> RegisteredTool {
    RegisteredTool(
        definition: AgentToolDefinition(
            name: "Skill",
            description: """
    Execute a skill within the main conversation.

    When users ask you to perform tasks, check if any of the available skills can help complete the task more effectively. Skills provide specialized capabilities and domain knowledge.

    When users ask you to run a "slash command" or reference "/<something>" (e.g., "/commit", "/review-pr"), they are referring to a skill. Use this tool to invoke the corresponding skill.

    Example:
      User: "run /commit"
      Assistant: [Calls Skill tool with skill: "commit"]

    How to invoke:
    - Use this tool with the skill name and optional arguments
    - Examples:
      - `skill: "sprint"` - invoke the sprint skill
      - `skill: "commit", args: "-m 'Fix bug'"` - invoke with arguments
      - `skill: "review-pr", args: "123"` - invoke with arguments

    Important:
    - When a skill is relevant, you must invoke this tool IMMEDIATELY as your first action
    - NEVER just announce or mention a skill in your text response without actually calling this tool
    - This is a BLOCKING REQUIREMENT: invoke the relevant Skill tool BEFORE generating any other response about the task
    - Only use skills that are available (listed in the system prompt)
    - Do not invoke a skill that is already running
    """,
            parameters: [
                "type": "object",
                "properties": [
                    "skill": ["type": "string", "description": "The skill name. E.g., \"commit\", \"review-pr\", or \"sprint\""],
                    "args": ["type": "string", "description": "Optional arguments for the skill"],
                ] as [String: Any],
                "required": ["skill"],
            ] as [String: Any]
        ),
        executor: { args, _ in
            guard let skill = args["skill"] as? String else {
                throw ToolError.validationError("skill is required")
            }
            let skillArgs = (args["args"] as? String) ?? ""
            return """
            <skill-invocation name="\(skill)">
            args: \(skillArgs)
            </skill-invocation>

            Skill execution passthrough acknowledged.
            """
        }
    )
}

private struct ClaudeCoordinationTask: Sendable {
    var id: String
    var subject: String
    var details: String?
    var status: String
    var owner: String?
    var blockedBy: [String]
    var createdAt: Date
    var updatedAt: Date
}

private struct ClaudeCoordinationTeam: Sendable {
    var name: String
    var description: String?
    var createdAt: Date
    var memberNames: Set<String>
    var memberAgentIDs: [String: String] // member name -> agent id
    var taskListKey: String
}

private struct ClaudeProtocolRequest: Sendable {
    var id: String
    var type: String
    var from: String
    var to: String
    var content: String?
    var createdAt: Date
}

private struct ClaudeTeamSnapshot: Sendable {
    var name: String
    var description: String?
    var taskListKey: String
}

private actor ClaudeCoordinationStore {
    static let shared = ClaudeCoordinationStore()

    private var activeTeamBySession: [String: String] = [:]
    private var teamsByName: [String: ClaudeCoordinationTeam] = [:]
    private var tasksByListKey: [String: [ClaudeCoordinationTask]] = [:]
    private var nextTaskNumberByListKey: [String: Int] = [:]
    private var protocolRequests: [String: ClaudeProtocolRequest] = [:]
    private var teamMessages: [String: [String]] = [:]

    func attachSession(sessionID: String, toTeam teamName: String) {
        activeTeamBySession[sessionID] = teamName
        ensureTeam(teamName: teamName)
    }

    func senderName(for sessionID: String) -> String {
        if let teamName = activeTeamBySession[sessionID],
           let team = teamsByName[teamName],
           let leaderName = team.memberNames.sorted().first {
            return leaderName
        }
        return "team-lead"
    }

    func createOrAttachTeam(sessionID: String, teamName: String, description: String?) -> ClaudeTeamSnapshot {
        ensureTeam(teamName: teamName, description: description)
        activeTeamBySession[sessionID] = teamName
        let team = teamsByName[teamName]!
        return ClaudeTeamSnapshot(name: team.name, description: team.description, taskListKey: team.taskListKey)
    }

    func deleteActiveTeam(sessionID: String) -> String {
        guard let teamName = activeTeamBySession[sessionID], let team = teamsByName[teamName] else {
            return "No active team to delete."
        }
        if !team.memberAgentIDs.isEmpty {
            let names = team.memberAgentIDs.keys.sorted().joined(separator: ", ")
            return "TeamDelete failed: team has active members (\(names)). Shut them down first."
        }

        teamsByName.removeValue(forKey: teamName)
        tasksByListKey.removeValue(forKey: team.taskListKey)
        nextTaskNumberByListKey.removeValue(forKey: team.taskListKey)
        teamMessages.removeValue(forKey: teamName)
        activeTeamBySession = activeTeamBySession.filter { $0.value != teamName }
        return "Team deleted: \(teamName)"
    }

    func addMember(name: String, teamName: String) {
        ensureTeam(teamName: teamName)
        guard var team = teamsByName[teamName] else { return }
        team.memberNames.insert(name)
        teamsByName[teamName] = team
    }

    func bindAgent(sessionID: String, teamName: String, memberName: String, agentID: String) {
        ensureTeam(teamName: teamName)
        activeTeamBySession[sessionID] = teamName
        guard var team = teamsByName[teamName] else { return }
        team.memberNames.insert(memberName)
        team.memberAgentIDs[memberName] = agentID
        teamsByName[teamName] = team
    }

    func unbindAgent(agentID: String) {
        for (teamName, var team) in teamsByName {
            if let entry = team.memberAgentIDs.first(where: { $0.value == agentID }) {
                team.memberAgentIDs.removeValue(forKey: entry.key)
                teamsByName[teamName] = team
                return
            }
        }
    }

    func createTask(
        sessionID: String,
        subject: String,
        details: String?,
        status: String,
        owner: String?,
        blockedBy: [String]
    ) -> ClaudeCoordinationTask {
        let listKey = taskListKey(for: sessionID)
        let next = (nextTaskNumberByListKey[listKey] ?? 0) + 1
        nextTaskNumberByListKey[listKey] = next
        let taskID = "task-\(next)"
        let now = Date()
        let task = ClaudeCoordinationTask(
            id: taskID,
            subject: subject,
            details: details,
            status: status,
            owner: owner,
            blockedBy: blockedBy,
            createdAt: now,
            updatedAt: now
        )
        var tasks = tasksByListKey[listKey] ?? []
        tasks.append(task)
        tasksByListKey[listKey] = tasks
        return task
    }

    func getTask(sessionID: String, taskID: String) -> ClaudeCoordinationTask? {
        let listKey = taskListKey(for: sessionID)
        return tasksByListKey[listKey]?.first(where: { $0.id == taskID })
    }

    func listTasks(sessionID: String) -> [ClaudeCoordinationTask] {
        let listKey = taskListKey(for: sessionID)
        return (tasksByListKey[listKey] ?? []).sorted { lhs, rhs in
            lhs.id.localizedStandardCompare(rhs.id) == .orderedAscending
        }
    }

    func updateTask(
        sessionID: String,
        taskID: String,
        subject: String?,
        details: String?,
        status: String?,
        owner: String?,
        blockedBy: [String]?
    ) -> ClaudeCoordinationTask? {
        let listKey = taskListKey(for: sessionID)
        guard var tasks = tasksByListKey[listKey],
              let index = tasks.firstIndex(where: { $0.id == taskID }) else {
            return nil
        }

        if let subject {
            tasks[index].subject = subject
        }
        if let details {
            tasks[index].details = details
        }
        if let status {
            tasks[index].status = status
        }
        if let owner {
            tasks[index].owner = owner.isEmpty ? nil : owner
        }
        if let blockedBy {
            tasks[index].blockedBy = blockedBy
        }
        tasks[index].updatedAt = Date()
        tasksByListKey[listKey] = tasks
        return tasks[index]
    }

    func sendMessage(sessionID: String, from: String, to: String, content: String, summary: String) {
        let teamName = activeTeamBySession[sessionID] ?? "session-\(sessionID)"
        let line = "[message] \(from) -> \(to): \(summary)\n\(content)"
        teamMessages[teamName, default: []].append(line)
    }

    func broadcast(sessionID: String, from: String, content: String, summary: String) -> Int {
        let teamName = activeTeamBySession[sessionID] ?? "session-\(sessionID)"
        let recipients = teamsByName[teamName]?.memberNames ?? []
        let count = max(recipients.count, 1)
        let line = "[broadcast] \(from): \(summary)\n\(content)"
        teamMessages[teamName, default: []].append(line)
        return count
    }

    func createProtocolRequest(sessionID: String, from: String, to: String, type: String, content: String?) -> String {
        let requestID = UUID().uuidString
        let request = ClaudeProtocolRequest(
            id: requestID,
            type: type,
            from: from,
            to: to,
            content: content,
            createdAt: Date()
        )
        protocolRequests[requestID] = request
        let teamName = activeTeamBySession[sessionID] ?? "session-\(sessionID)"
        let line = "[\(type)] \(from) -> \(to) request_id=\(requestID)"
        teamMessages[teamName, default: []].append(line)
        return requestID
    }

    func resolveProtocolRequest(sessionID: String, requestID: String, approver: String, approve: Bool, content: String?) -> Bool {
        guard let request = protocolRequests.removeValue(forKey: requestID) else {
            return false
        }
        let teamName = activeTeamBySession[sessionID] ?? "session-\(sessionID)"
        var line = "[\(request.type)_response] \(approver) -> \(request.from) request_id=\(requestID) approve=\(approve)"
        if let content, !content.isEmpty {
            line += "\n\(content)"
        }
        teamMessages[teamName, default: []].append(line)
        return true
    }

    private func ensureTeam(teamName: String, description: String? = nil) {
        if var existing = teamsByName[teamName] {
            if existing.description == nil, let description {
                existing.description = description
                teamsByName[teamName] = existing
            }
            return
        }
        let listKey = "team:\(teamName)"
        teamsByName[teamName] = ClaudeCoordinationTeam(
            name: teamName,
            description: description,
            createdAt: Date(),
            memberNames: ["team-lead"],
            memberAgentIDs: [:],
            taskListKey: listKey
        )
        if tasksByListKey[listKey] == nil {
            tasksByListKey[listKey] = []
        }
    }

    private func taskListKey(for sessionID: String) -> String {
        if let teamName = activeTeamBySession[sessionID], let team = teamsByName[teamName] {
            return team.taskListKey
        }
        return "session:\(sessionID)"
    }
}

private actor ClaudeWebCacheStore {
    static let shared = ClaudeWebCacheStore()

    private struct Entry {
        var value: String
        var expiresAt: Date
    }

    private var entries: [String: Entry] = [:]
    private let ttl: TimeInterval = 15 * 60

    func get(key: String) -> String? {
        purgeExpired()
        guard let entry = entries[key], entry.expiresAt > Date() else { return nil }
        return entry.value
    }

    func set(key: String, value: String) {
        purgeExpired()
        entries[key] = Entry(value: value, expiresAt: Date().addingTimeInterval(ttl))
    }

    private func purgeExpired() {
        let now = Date()
        entries = entries.filter { $0.value.expiresAt > now }
    }
}

// MARK: - Background Task Store

private struct ClaudeBackgroundTaskSnapshot: Sendable {
    let status: String
    let output: String
}

private actor ClaudeBackgroundTaskStore {
    static let shared = ClaudeBackgroundTaskStore()

    private struct Record {
        var status: String
        var output: String
        var worker: Task<Void, Never>
    }

    private var records: [String: Record] = [:]

    func spawn(command: String, timeoutMs: Int, env: ExecutionEnvironment) -> String {
        let id = UUID().uuidString

        let worker = Task {
            do {
                let result = try await env.execCommand(
                    command: command,
                    timeoutMs: timeoutMs,
                    workingDir: nil,
                    envVars: nil
                )
                let rendered = claudeFormatExecResult(result, timeoutMs: timeoutMs)
                self.complete(taskID: id, status: "completed", output: rendered)
            } catch {
                self.complete(taskID: id, status: "failed", output: "\(error)")
            }
        }

        records[id] = Record(status: "running", output: "", worker: worker)
        return id
    }

    func complete(taskID: String, status: String, output: String) {
        guard var record = records[taskID] else { return }
        record.status = status
        record.output = output
        records[taskID] = record
    }

    func cancel(taskID: String) -> Bool {
        guard var record = records[taskID] else { return false }
        record.worker.cancel()
        record.status = "cancelled"
        records[taskID] = record
        return true
    }

    func get(taskID: String, block: Bool, timeoutMs: Int) async -> ClaudeBackgroundTaskSnapshot? {
        guard let initial = records[taskID] else { return nil }

        if !block || initial.status != "running" {
            return ClaudeBackgroundTaskSnapshot(status: initial.status, output: initial.output)
        }

        let deadline = Date().addingTimeInterval(Double(timeoutMs) / 1000.0)
        while Date() < deadline {
            guard let current = records[taskID] else { return nil }
            if current.status != "running" {
                return ClaudeBackgroundTaskSnapshot(status: current.status, output: current.output)
            }
            try? await Task.sleep(nanoseconds: 200_000_000)
        }

        if let current = records[taskID] {
            return ClaudeBackgroundTaskSnapshot(status: "running", output: current.output)
        }
        return nil
    }
}

private struct ClaudePlanModeState: Sendable {
    var isActive: Bool
    var planFilePath: String
}

private actor ClaudePlanModeStore {
    static let shared = ClaudePlanModeStore()

    private var states: [String: ClaudePlanModeState] = [:]

    func enter(sessionID: String, workingDirectory: String) -> ClaudePlanModeState {
        if let existing = states[sessionID], existing.isActive {
            return existing
        }
        let planPath = (workingDirectory as NSString).appendingPathComponent(".claude/plans/current_plan.md")
        let state = ClaudePlanModeState(isActive: true, planFilePath: planPath)
        states[sessionID] = state
        return state
    }

    func get(sessionID: String) -> ClaudePlanModeState? {
        states[sessionID]
    }

    func exit(sessionID: String) {
        guard var state = states[sessionID] else { return }
        state.isActive = false
        states[sessionID] = state
    }
}

private struct ClaudeAllowedPrompt: Sendable {
    var tool: String
    var prompt: String
}

private func claudeParseAllowedPrompts(_ raw: Any?) -> [ClaudeAllowedPrompt] {
    guard let entries = raw as? [Any] else { return [] }
    return entries.compactMap { item in
        guard let dict = item as? [String: Any] else { return nil }
        guard let tool = dict["tool"] as? String, !tool.isEmpty else { return nil }
        guard let prompt = dict["prompt"] as? String, !prompt.isEmpty else { return nil }
        return ClaudeAllowedPrompt(tool: tool, prompt: prompt)
    }
}

private func claudeReadRawTextFile(path: String, env: ExecutionEnvironment) async throws -> String {
    let escapedPath = claudeShellEscape(path)
    let catCommand = "cat -- \(escapedPath)"
    let catResult = try await env.execCommand(command: catCommand, timeoutMs: 15_000, workingDir: nil, envVars: nil)
    if catResult.exitCode == 0 {
        return catResult.stdout
    }
    // Fallback for restricted environments where shell execution is unavailable.
    let numbered = try await env.readFile(path: path, offset: 1, limit: 100_000)
    return claudeStripLineNumbers(numbered)
}

private func claudeNotebookSourceArray(_ source: String) -> [String] {
    let lines = source.split(separator: "\n", omittingEmptySubsequences: false)
    guard !lines.isEmpty else { return [] }
    return lines.enumerated().map { index, line in
        let text = String(line)
        return index == lines.count - 1 ? text : text + "\n"
    }
}

private func claudeNotebookLanguage(_ notebook: [String: Any]) -> String {
    if let metadata = notebook["metadata"] as? [String: Any] {
        if let languageInfo = metadata["language_info"] as? [String: Any],
           let language = languageInfo["name"] as? String,
           !language.isEmpty
        {
            return language
        }
        if let kernelSpec = metadata["kernelspec"] as? [String: Any],
           let language = kernelSpec["language"] as? String,
           !language.isEmpty
        {
            return language
        }
    }
    return "unknown"
}

private func claudeNotebookFindIndex(
    cells: [[String: Any]],
    cellID: String?,
    cellNumber: Int?,
    requireTarget: Bool
) throws -> Int? {
    if let cellID, !cellID.isEmpty {
        guard let idx = cells.firstIndex(where: { ($0["id"] as? String) == cellID }) else {
            throw ToolError.validationError("Cell with id '\(cellID)' was not found")
        }
        return idx
    }

    if let cellNumber {
        guard cellNumber >= 0 && cellNumber < cells.count else {
            throw ToolError.validationError("cell_number \(cellNumber) is out of range")
        }
        return cellNumber
    }

    if requireTarget {
        throw ToolError.validationError("Provide cell_id (preferred) or cell_number for this edit")
    }
    return nil
}

private func claudeExecuteNotebookEdit(
    args: [String: Any],
    env: ExecutionEnvironment,
    parentSession: Session?
) async throws -> String {
    guard let notebookPath = args["notebook_path"] as? String, !notebookPath.isEmpty else {
        throw ToolError.validationError("notebook_path is required")
    }
    guard notebookPath.hasPrefix("/") else {
        throw ToolError.validationError("notebook_path must be absolute")
    }
    guard let newSource = args["new_source"] as? String else {
        throw ToolError.validationError("new_source is required")
    }

    let editMode = (args["edit_mode"] as? String)?.lowercased() ?? "replace"
    guard ["replace", "insert", "delete"].contains(editMode) else {
        throw ToolError.validationError("edit_mode must be one of: replace, insert, delete")
    }

    let cellID = args["cell_id"] as? String
    let cellNumber = claudeInt(args["cell_number"])
    let requestedCellType = (args["cell_type"] as? String)?.lowercased()

    let originalFile = try await claudeReadRawTextFile(path: notebookPath, env: env)
    guard let originalData = originalFile.data(using: .utf8) else {
        throw ToolError.validationError("Notebook is not valid UTF-8 text")
    }

    guard var notebook = (try JSONSerialization.jsonObject(with: originalData)) as? [String: Any] else {
        throw ToolError.validationError("Notebook root JSON must be an object")
    }
    guard var cells = notebook["cells"] as? [[String: Any]] else {
        throw ToolError.validationError("Notebook is missing 'cells' array")
    }

    let language = claudeNotebookLanguage(notebook)
    var effectiveCellID = cellID
    var effectiveCellType = requestedCellType ?? "code"

    switch editMode {
    case "replace":
        guard let targetIndex = try claudeNotebookFindIndex(
            cells: cells,
            cellID: cellID,
            cellNumber: cellNumber,
            requireTarget: true
        ) else {
            throw ToolError.validationError("No target cell specified")
        }

        var target = cells[targetIndex]
        let existingType = (target["cell_type"] as? String)?.lowercased() ?? "code"
        let nextType = requestedCellType ?? existingType
        if !["code", "markdown"].contains(nextType) {
            throw ToolError.validationError("cell_type must be code or markdown")
        }

        target["cell_type"] = nextType
        target["source"] = claudeNotebookSourceArray(newSource)
        if nextType == "code" {
            if target["execution_count"] == nil { target["execution_count"] = NSNull() }
            if target["outputs"] == nil { target["outputs"] = [] as [Any] }
        } else {
            target.removeValue(forKey: "execution_count")
            target.removeValue(forKey: "outputs")
        }
        if target["metadata"] == nil { target["metadata"] = [:] as [String: Any] }
        if target["id"] == nil {
            target["id"] = UUID().uuidString.lowercased()
        }
        cells[targetIndex] = target
        effectiveCellID = target["id"] as? String
        effectiveCellType = nextType

    case "insert":
        guard let insertType = requestedCellType, ["code", "markdown"].contains(insertType) else {
            throw ToolError.validationError("cell_type is required for edit_mode=insert and must be code or markdown")
        }

        let insertionIndex: Int
        if let existingIndex = try claudeNotebookFindIndex(
            cells: cells,
            cellID: cellID,
            cellNumber: nil,
            requireTarget: false
        ) {
            insertionIndex = min(cells.count, existingIndex + 1)
        } else if let cellNumber {
            guard cellNumber >= 0 && cellNumber <= cells.count else {
                throw ToolError.validationError("cell_number \(cellNumber) is out of range for insert")
            }
            insertionIndex = cellNumber
        } else {
            insertionIndex = 0
        }

        let newCellID = UUID().uuidString.lowercased()
        var newCell: [String: Any] = [
            "id": newCellID,
            "cell_type": insertType,
            "metadata": [:] as [String: Any],
            "source": claudeNotebookSourceArray(newSource),
        ]
        if insertType == "code" {
            newCell["execution_count"] = NSNull()
            newCell["outputs"] = [] as [Any]
        }

        cells.insert(newCell, at: insertionIndex)
        effectiveCellID = newCellID
        effectiveCellType = insertType

    case "delete":
        guard let targetIndex = try claudeNotebookFindIndex(
            cells: cells,
            cellID: cellID,
            cellNumber: cellNumber,
            requireTarget: true
        ) else {
            throw ToolError.validationError("No target cell specified")
        }
        let removed = cells.remove(at: targetIndex)
        effectiveCellID = removed["id"] as? String
        effectiveCellType = (removed["cell_type"] as? String)?.lowercased() ?? "code"

    default:
        throw ToolError.validationError("Unsupported edit_mode")
    }

    notebook["cells"] = cells
    let serialized = try JSONSerialization.data(
        withJSONObject: notebook,
        options: [.prettyPrinted, .sortedKeys]
    )
    guard let updatedFile = String(data: serialized, encoding: .utf8) else {
        throw ToolError.writeError("Failed to encode updated notebook JSON as UTF-8")
    }

    try await env.writeFile(path: notebookPath, content: updatedFile + "\n")
    if let parentSession {
        await parentSession.addSystemReminder("NotebookEdit updated \(notebookPath) in \(editMode) mode.")
    }

    var output: [String: Any] = [
        "new_source": newSource,
        "cell_type": effectiveCellType,
        "language": language,
        "edit_mode": editMode,
        "notebook_path": notebookPath,
        "original_file": originalFile,
        "updated_file": updatedFile,
    ]
    if let effectiveCellID, !effectiveCellID.isEmpty {
        output["cell_id"] = effectiveCellID
    }

    let responseData = try JSONSerialization.data(withJSONObject: output, options: [.prettyPrinted, .sortedKeys])
    return String(data: responseData, encoding: .utf8) ?? "Notebook edited successfully."
}

// MARK: - Helpers

private func claudeInt(_ raw: Any?) -> Int? {
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

private func claudeBool(_ raw: Any?) -> Bool? {
    switch raw {
    case let value as Bool:
        return value
    case let value as NSNumber:
        return value.boolValue
    default:
        return nil
    }
}

private func claudeStringArray(_ raw: Any?) -> [String]? {
    if let strings = raw as? [String] {
        return strings
    }
    if let anyArray = raw as? [Any] {
        let strings = anyArray.compactMap { $0 as? String }
        return strings.count == anyArray.count ? strings : nil
    }
    return nil
}

private func claudeTaskStatus(_ raw: String?) -> String {
    guard let raw else { return "pending" }
    switch raw.lowercased() {
    case "pending", "in_progress", "completed", "blocked", "cancelled":
        return raw.lowercased()
    default:
        return "pending"
    }
}

private func claudeRenderTask(_ task: ClaudeCoordinationTask) -> String {
    var lines: [String] = []
    lines.append("Task \(task.id)")
    lines.append("Subject: \(task.subject)")
    lines.append("Status: \(task.status)")
    if let owner = task.owner, !owner.isEmpty {
        lines.append("Owner: \(owner)")
    }
    if let details = task.details, !details.isEmpty {
        lines.append("Description: \(details)")
    }
    if !task.blockedBy.isEmpty {
        lines.append("Blocked By: \(task.blockedBy.joined(separator: ", "))")
    }
    lines.append("Updated: \(task.updatedAt.ISO8601Format())")
    return lines.joined(separator: "\n")
}

private func claudeProcessWebContent(prompt: String, content: String, sourceURL: String) async -> String? {
    let maxChars = 120_000
    let normalizedContent = content.count > maxChars
        ? String(content.prefix(maxChars)) + "\n\n[Content truncated for processing]"
        : content

    let systemPrompt = """
You are a web content extractor. Answer ONLY from the provided page content.
If the requested information is not present, say that clearly.
Keep the answer concise and factual.
"""

    let userPrompt = """
URL: \(sourceURL)

User extraction prompt:
\(prompt)

Page content:
\(normalizedContent)
"""

    do {
        let client = try Client.fromEnv()
        let response = try await client.complete(
            request: Request(
                model: "claude-haiku-4-5",
                messages: [.system(systemPrompt), .user(userPrompt)],
                provider: "anthropic",
                maxTokens: 1200
            )
        )
        let text = response.text.trimmingCharacters(in: .whitespacesAndNewlines)
        return text.isEmpty ? nil : text
    } catch {
        return nil
    }
}

private func claudeShellEscape(_ value: String) -> String {
    if value.isEmpty {
        return "''"
    }
    return "'" + value.replacing("'", with: "'\\''") + "'"
}

private func claudeStripLineNumbers(_ text: String) -> String {
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

private func claudeExecuteCommand(
    env: ExecutionEnvironment,
    command: String,
    timeoutMs: Int,
    emitOutputDelta: StreamingToolOutputEmitter
) async throws -> String {
    let result = try await env.execCommand(command: command, timeoutMs: timeoutMs, workingDir: nil, envVars: nil)
    let combinedOutput = result.combinedOutput
    if !combinedOutput.isEmpty {
        await emitOutputDelta(combinedOutput)
    }
    return claudeFormatExecResult(result, timeoutMs: timeoutMs)
}

private func claudeFormatExecResult(_ result: ExecResult, timeoutMs: Int) -> String {
    var output = result.combinedOutput
    if output.isEmpty {
        output = "[No output]"
    }
    if result.timedOut {
        output += "\n\n[ERROR: Command timed out after \(timeoutMs)ms.]"
    } else if result.exitCode != 0 {
        output += "\n[Exit code: \(result.exitCode)]"
    }
    return output
}

private func claudeLastAssistantOutput(from history: [Turn]) -> String {
    for turn in history.reversed() {
        if case .assistant(let assistant) = turn, !assistant.content.isEmpty {
            return assistant.content
        }
    }
    return ""
}
