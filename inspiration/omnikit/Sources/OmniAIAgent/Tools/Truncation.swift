import Foundation

// MARK: - Default Limits

public let defaultToolOutputLimits: [String: Int] = [
    "read_file": 50_000,
    "shell": 30_000,
    "shell_command": 30_000,
    "grep": 20_000,
    "grep_files": 20_000,
    "glob": 20_000,
    "list_dir": 20_000,
    "edit_file": 10_000,
    "apply_patch": 10_000,
    "write_file": 1_000,
    "spawn_agent": 20_000,
    "send_input": 20_000,
    "wait": 20_000,
    "close_agent": 20_000,
    "update_plan": 8_000,
    "view_image": 2_000,
    "exec_command": 40_000,
    "write_stdin": 40_000,
    "web_search": 30_000,
    "web_fetch": 50_000,
    "grep_search": 20_000,
    "list_directory": 20_000,
    "run_shell_command": 30_000,
    "google_web_search": 30_000,
    "replace": 10_000,
    "write_todos": 8_000,
    "save_memory": 4_000,
    "get_internal_docs": 50_000,
    "activate_skill": 30_000,
    "ask_user": 20_000,
    "enter_plan_mode": 4_000,
    "exit_plan_mode": 4_000,
    "Read": 50_000,
    "Write": 2_000,
    "Edit": 10_000,
    "Glob": 20_000,
    "Grep": 20_000,
    "Bash": 30_000,
    "WebSearch": 30_000,
    "WebFetch": 50_000,
    "Task": 20_000,
    "TaskOutput": 20_000,
    "TaskStop": 8_000,
    "TaskCreate": 12_000,
    "TaskGet": 12_000,
    "TaskList": 20_000,
    "TaskUpdate": 12_000,
    "TeamCreate": 12_000,
    "TeamDelete": 8_000,
    "SendMessage": 12_000,
    "ToolSearch": 12_000,
    "TodoWrite": 8_000,
]

public let defaultToolLineLimits: [String: Int] = [
    "shell": 256,
    "shell_command": 256,
    "grep": 200,
    "grep_files": 200,
    "glob": 500,
    "list_dir": 500,
    "exec_command": 300,
    "write_stdin": 300,
    "web_search": 300,
    "web_fetch": 500,
    "grep_search": 200,
    "list_directory": 500,
    "run_shell_command": 256,
    "google_web_search": 300,
    "Bash": 256,
    "Grep": 200,
    "Glob": 500,
    "WebSearch": 300,
    "WebFetch": 500,
    "TaskList": 300,
    "ToolSearch": 200,
]

public let defaultTruncationModes: [String: String] = [
    "read_file": "head_tail",
    "shell": "head_tail",
    "shell_command": "head_tail",
    "grep": "tail",
    "grep_files": "tail",
    "glob": "tail",
    "list_dir": "tail",
    "edit_file": "tail",
    "apply_patch": "tail",
    "write_file": "tail",
    "spawn_agent": "head_tail",
    "send_input": "head_tail",
    "wait": "head_tail",
    "close_agent": "head_tail",
    "exec_command": "head_tail",
    "write_stdin": "head_tail",
    "web_search": "head_tail",
    "web_fetch": "head_tail",
    "grep_search": "tail",
    "list_directory": "tail",
    "run_shell_command": "head_tail",
    "google_web_search": "head_tail",
    "replace": "tail",
    "write_todos": "tail",
    "save_memory": "tail",
    "get_internal_docs": "head_tail",
    "activate_skill": "head_tail",
    "ask_user": "head_tail",
    "enter_plan_mode": "tail",
    "exit_plan_mode": "tail",
    "Read": "head_tail",
    "Write": "tail",
    "Edit": "tail",
    "Glob": "tail",
    "Grep": "tail",
    "Bash": "head_tail",
    "WebSearch": "head_tail",
    "WebFetch": "head_tail",
    "Task": "head_tail",
    "TaskOutput": "head_tail",
    "TaskStop": "tail",
    "TaskCreate": "tail",
    "TaskGet": "tail",
    "TaskList": "head_tail",
    "TaskUpdate": "tail",
    "TeamCreate": "tail",
    "TeamDelete": "tail",
    "SendMessage": "tail",
    "ToolSearch": "tail",
    "TodoWrite": "tail",
]

// MARK: - Character-Based Truncation

public func truncateOutput(_ output: String, maxChars: Int, mode: String) -> String {
    guard output.count > maxChars else { return output }

    if mode == "head_tail" {
        let half = maxChars / 2
        let removed = output.count - maxChars
        let headEnd = output.index(output.startIndex, offsetBy: half)
        let tailStart = output.index(output.endIndex, offsetBy: -half)
        return String(output[output.startIndex..<headEnd])
            + "\n\n[WARNING: Tool output was truncated. "
            + "\(removed) characters were removed from the middle. "
            + "The full output is available in the event stream. "
            + "If you need to see specific parts, re-run the tool with more targeted parameters.]\n\n"
            + String(output[tailStart..<output.endIndex])
    }

    if mode == "tail" {
        let removed = output.count - maxChars
        let tailStart = output.index(output.endIndex, offsetBy: -maxChars)
        return "[WARNING: Tool output was truncated. First "
            + "\(removed) characters were removed. "
            + "The full output is available in the event stream.]\n\n"
            + String(output[tailStart..<output.endIndex])
    }

    // Default: head truncation
    let headEnd = output.index(output.startIndex, offsetBy: maxChars)
    return String(output[output.startIndex..<headEnd])
}

// MARK: - Line-Based Truncation

public func truncateLines(_ output: String, maxLines: Int) -> String {
    let lines = output.components(separatedBy: "\n")
    guard lines.count > maxLines else { return output }

    let headCount = maxLines / 2
    let tailCount = maxLines - headCount
    let omitted = lines.count - headCount - tailCount

    let head = lines[0..<headCount].joined(separator: "\n")
    let tail = lines[(lines.count - tailCount)...].joined(separator: "\n")

    return head + "\n[... \(omitted) lines omitted ...]\n" + tail
}

// MARK: - Combined Truncation Pipeline

public func truncateToolOutput(_ output: String, toolName: String, config: SessionConfig) -> String {
    let maxChars = config.toolOutputLimits[toolName] ?? defaultToolOutputLimits[toolName] ?? 50_000
    let mode = defaultTruncationModes[toolName] ?? "head_tail"

    // Step 1: Character-based truncation (always runs first)
    var result = truncateOutput(output, maxChars: maxChars, mode: mode)

    // Step 2: Line-based truncation (secondary, for readability)
    let maxLines = config.toolLineLimits[toolName] ?? defaultToolLineLimits[toolName]
    if let maxLines = maxLines {
        result = truncateLines(result, maxLines: maxLines)
    }

    return result
}
