import Foundation
import OmniAICore

private enum _ToolCallType: String, CaseIterable, Sendable {
    case functionCall = "function_call"
    case computerCall = "computer_call"
    case shellCall = "shell_call"
    case applyPatchCall = "apply_patch_call"
    case localShellCall = "local_shell_call"
    case webSearchCall = "web_search_call"
    case fileSearchCall = "file_search_call"
    case codeInterpreterCall = "code_interpreter_call"
    case imageGenerationCall = "image_generation_call"
    case mcpCall = "mcp_call"
    case mcpApprovalRequest = "mcp_approval_request"
}

enum ToolPlanningRuntime {
    static func handoffMap<TContext>(for agent: Agent<TContext>) -> [String: Handoff<TContext>] {
        Dictionary(uniqueKeysWithValues: agent.handoffs.map { ($0.toolName, $0) })
    }

    static func toolMap(for tools: [Tool]) -> [String: Tool] {
        Dictionary(uniqueKeysWithValues: tools.map { ($0.name, $0) })
    }

    static func extractToolCalls(from response: ModelResponse) -> [TResponseOutputItem] {
        let executableTypes: Set<_ToolCallType> = [.functionCall, .computerCall, .shellCall, .applyPatchCall, .localShellCall]
        return response.output.filter { item in
            guard let typeRaw = item["type"]?.stringValue, let type = _ToolCallType(rawValue: typeRaw) else { return false }
            return executableTypes.contains(type)
        }
    }

    static func toolName(for item: TResponseOutputItem) -> String {
        if let name = item["name"]?.stringValue, !name.isEmpty {
            return name
        }
        switch _ToolCallType(rawValue: item["type"]?.stringValue ?? "") {
        case .computerCall:
            return "computer_use_preview"
        case .shellCall:
            return "shell"
        case .applyPatchCall:
            return "apply_patch"
        case .localShellCall:
            return "local_shell"
        case .webSearchCall:
            return "web_search"
        case .fileSearchCall:
            return "file_search"
        case .codeInterpreterCall:
            return "code_interpreter"
        case .imageGenerationCall:
            return "image_generation"
        case .mcpCall:
            return item["tool_name"]?.stringValue ?? "hosted_mcp"
        default:
            return item["type"]?.stringValue ?? "tool"
        }
    }

    static func shouldExecuteLocally(_ tool: Tool) -> Bool {
        switch tool {
        case .function, .computer, .applyPatch, .localShell:
            return true
        case .shell(let shellTool):
            if let environment = shellTool.environment {
                switch environment {
                case .hosted:
                    return false
                case .local:
                    return true
                }
            }
            return true
        case .fileSearch, .webSearch, .hostedMCP, .codeInterpreter, .imageGeneration:
            return false
        }
    }

    static func hostedApprovalRequests(from response: ModelResponse) -> [TResponseOutputItem] {
        response.output.filter { _ToolCallType(rawValue: $0["type"]?.stringValue ?? "") == .mcpApprovalRequest }
    }

    static func hostedToolUsedNames(from response: ModelResponse) -> [String] {
        let hostedTypes: Set<_ToolCallType> = [.fileSearchCall, .webSearchCall, .codeInterpreterCall, .imageGenerationCall, .mcpCall, .shellCall, .applyPatchCall, .localShellCall, .computerCall]
        return response.output.compactMap { item in
            guard let typeRaw = item["type"]?.stringValue, let type = _ToolCallType(rawValue: typeRaw), hostedTypes.contains(type) else { return nil }
            return toolName(for: item)
        }
    }
}
