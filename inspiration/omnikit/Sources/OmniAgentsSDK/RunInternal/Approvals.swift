import Foundation
import OmniAICore

enum ApprovalRuntime {
    static func evaluateNeedsApprovalSetting<TContext>(
        tool: Tool,
        runContext: RunContextWrapper<TContext>,
        arguments: [String: JSONValue],
        callID: String
    ) async throws -> Bool {
        switch tool {
        case .function(let functionTool):
            return try await functionTool.needsApproval.evaluate(context: runContext, arguments: arguments, callID: callID)
        case .shell(let shellTool):
            return try await shellTool.needsApproval.evaluate(context: runContext, arguments: arguments, callID: callID)
        case .applyPatch(let applyPatchTool):
            return try await applyPatchTool.needsApproval.evaluate(context: runContext, arguments: arguments, callID: callID)
        default:
            return false
        }
    }

    static func defaultApprovalRejectedMessage(toolName: String) -> String {
        "Approval rejected for tool \(toolName)."
    }

    static func makeApprovalItem(agent: AnyAgent, toolName: String, callID: String, rawItem: TResponseOutputItem) -> ToolApprovalItem {
        var item = rawItem
        item["type"] = .string("tool_approval")
        item["name"] = .string(toolName)
        item["call_id"] = .string(callID)
        return ToolApprovalItem(agent: agent, rawItem: item, toolName: toolName)
    }
}
