import Foundation

enum RunItemFactory {
    static func items(
        from response: ModelResponse,
        agent: AnyAgent,
        handoffNames: Set<String>
    ) -> [any RunItem] {
        response.output.map { item in
            switch item["type"]?.stringValue {
            case "message":
                return MessageOutputItem(agent: agent, rawItem: item)
            case "function_call":
                if let name = item["name"]?.stringValue, handoffNames.contains(name) {
                    return HandoffCallItem(agent: agent, rawItem: item)
                }
                return ToolCallItem(agent: agent, rawItem: item)
            case "computer_call", "file_search_call", "web_search_call", "mcp_call", "code_interpreter_call", "image_generation_call", "shell_call", "apply_patch_call", "local_shell_call":
                return ToolCallItem(agent: agent, rawItem: item)
            case "reasoning":
                return ReasoningItem(agent: agent, rawItem: item)
            case "mcp_list_tools":
                return MCPListToolsItem(agent: agent, rawItem: item)
            case "mcp_approval_request":
                return MCPApprovalRequestItem(agent: agent, rawItem: item)
            default:
                return MessageOutputItem(agent: agent, rawItem: item)
            }
        }
    }
}
