import Foundation
import OmniAICore

public struct ToolApproval: Sendable, Codable, Equatable {
    public var toolName: String
    public var callID: String?

    public init(toolName: String, callID: String? = nil) {
        self.toolName = toolName
        self.callID = callID
    }

    enum CodingKeys: String, CodingKey {
        case toolName = "tool_name"
        case callID = "call_id"
    }
}

private struct ApprovalRecord {
    var approvedForAllCalls: Bool = false
    var rejectedForAllCalls: Bool = false
    var approvedCallIDs: Set<String> = []
    var rejectedCallIDs: Set<String> = []
}

open class RunContextWrapper<TContext>: @unchecked Sendable {
    public let context: TContext
    public var usage: Usage
    public var turnInput: [Any]
    public var toolInput: Any?

    private var approvals: [String: ApprovalRecord]
    private let stateLock = NSLock()

    public init(context: TContext, usage: Usage = Usage(), turnInput: [Any] = []) {
        self.context = context
        self.usage = usage
        self.turnInput = turnInput
        self.approvals = [:]
    }

    public func approveTool(_ approval: ToolApproval, alwaysApprove: Bool = false) {
        _applyApprovalDecision(toolName: approval.toolName, callID: approval.callID, always: alwaysApprove, approve: true)
    }

    public func approveTool(approvalItem: Any, alwaysApprove: Bool = false) {
        let toolName = Self.resolveToolName(from: approvalItem)
        let callID = Self.resolveCallID(from: approvalItem)
        _applyApprovalDecision(toolName: toolName, callID: callID, always: alwaysApprove, approve: true)
    }

    public func rejectTool(_ approval: ToolApproval, alwaysReject: Bool = false) {
        _applyApprovalDecision(toolName: approval.toolName, callID: approval.callID, always: alwaysReject, approve: false)
    }

    public func rejectTool(approvalItem: Any, alwaysReject: Bool = false) {
        let toolName = Self.resolveToolName(from: approvalItem)
        let callID = Self.resolveCallID(from: approvalItem)
        _applyApprovalDecision(toolName: toolName, callID: callID, always: alwaysReject, approve: false)
    }

    public func approveTool(toolName: String, callID: String? = nil, alwaysApprove: Bool = false) {
        _applyApprovalDecision(toolName: toolName, callID: callID, always: alwaysApprove, approve: true)
    }

    public func rejectTool(toolName: String, callID: String? = nil, alwaysReject: Bool = false) {
        _applyApprovalDecision(toolName: toolName, callID: callID, always: alwaysReject, approve: false)
    }

    public func isToolApproved(toolName: String, callID: String) -> Bool? {
        stateLock.lock()
        defer { stateLock.unlock() }

        guard let entry = approvals[toolName] else {
            return nil
        }
        if entry.approvedForAllCalls && entry.rejectedForAllCalls {
            return true
        }
        if entry.approvedForAllCalls {
            return true
        }
        if entry.rejectedForAllCalls {
            return false
        }
        if entry.approvedCallIDs.contains(callID) {
            return true
        }
        if entry.rejectedCallIDs.contains(callID) {
            return false
        }
        return nil
    }

    public func getApprovalStatus(toolName: String, callID: String, fallbackToolName: String? = nil) -> Bool? {
        let status = isToolApproved(toolName: toolName, callID: callID)
        if let status {
            return status
        }
        guard let fallbackToolName else {
            return nil
        }
        return isToolApproved(toolName: fallbackToolName, callID: callID)
    }

    public func getApprovalStatus(
        toolName: String,
        callID: String,
        existingPendingApprovalItem: Any?
    ) -> Bool? {
        let fallbackToolName = existingPendingApprovalItem.map { Self.resolveToolName(from: $0) }
        return getApprovalStatus(toolName: toolName, callID: callID, fallbackToolName: fallbackToolName)
    }

    public static func resolveToolName(from approvalItem: Any) -> String {
        if let approval = approvalItem as? ToolApproval {
            return approval.toolName
        }

        if let approvalDict = approvalItem as? [String: Any] {
            if let toolName = _toStringOrNil(approvalDict["tool_name"] ?? approvalDict["toolName"]) {
                return toolName
            }
            if let rawItem = approvalDict["raw_item"] ?? approvalDict["rawItem"],
               let fromRaw = _resolveToolNameFromRawItem(rawItem)
            {
                return fromRaw
            }
            if let fromRaw = _resolveToolNameFromRawItem(approvalDict) {
                return fromRaw
            }
            return "unknown_tool"
        }

        if let toolName = _toStringOrNil(_reflectedProperty(named: "toolName", from: approvalItem)
            ?? _reflectedProperty(named: "tool_name", from: approvalItem))
        {
            return toolName
        }

        if let rawItem = _reflectedProperty(named: "rawItem", from: approvalItem)
            ?? _reflectedProperty(named: "raw_item", from: approvalItem),
           let fromRaw = _resolveToolNameFromRawItem(rawItem)
        {
            return fromRaw
        }

        if let fromRaw = _resolveToolNameFromRawItem(approvalItem) {
            return fromRaw
        }

        return "unknown_tool"
    }

    public static func resolveCallID(from approvalItem: Any) -> String? {
        if let approval = approvalItem as? ToolApproval {
            return approval.callID
        }

        if let approvalDict = approvalItem as? [String: Any] {
            if let rawItem = approvalDict["raw_item"] ?? approvalDict["rawItem"],
               let callID = _resolveCallIDFromRawItem(rawItem)
            {
                return callID
            }
            return _resolveCallIDFromRawItem(approvalDict)
        }

        if let rawItem = _reflectedProperty(named: "rawItem", from: approvalItem)
            ?? _reflectedProperty(named: "raw_item", from: approvalItem),
           let callID = _resolveCallIDFromRawItem(rawItem)
        {
            return callID
        }

        return _resolveCallIDFromRawItem(approvalItem)
    }

    /// Restores approval data from serialized state (`approved`/`rejected` as bool or call-id list).
    public func rebuildApprovals(from serializedApprovals: [String: [String: JSONValue]]) {
        stateLock.lock()
        defer { stateLock.unlock() }

        approvals = [:]
        for (toolName, recordDict) in serializedApprovals {
            var record = ApprovalRecord()
            Self._hydrateApprovalField(recordValue: recordDict["approved"], approve: true, record: &record)
            Self._hydrateApprovalField(recordValue: recordDict["rejected"], approve: false, record: &record)
            approvals[toolName] = record
        }
    }

    public func serializedApprovals() -> [String: [String: JSONValue]] {
        stateLock.lock()
        defer { stateLock.unlock() }

        var result: [String: [String: JSONValue]] = [:]
        for (toolName, record) in approvals {
            let approvedValue: JSONValue = record.approvedForAllCalls
                ? .bool(true)
                : .array(record.approvedCallIDs.sorted().map(JSONValue.string))
            let rejectedValue: JSONValue = record.rejectedForAllCalls
                ? .bool(true)
                : .array(record.rejectedCallIDs.sorted().map(JSONValue.string))
            result[toolName] = [
                "approved": approvedValue,
                "rejected": rejectedValue,
            ]
        }
        return result
    }

    public func forkWithToolInput(_ toolInput: Any) -> RunContextWrapper<TContext> {
        let fork = RunContextWrapper(context: context, usage: usage, turnInput: turnInput)
        fork.toolInput = toolInput
        stateLock.lock()
        fork.approvals = approvals
        stateLock.unlock()
        return fork
    }

    public func forkWithoutToolInput() -> RunContextWrapper<TContext> {
        let fork = RunContextWrapper(context: context, usage: usage, turnInput: turnInput)
        stateLock.lock()
        fork.approvals = approvals
        stateLock.unlock()
        return fork
    }

    private func _applyApprovalDecision(
        toolName: String,
        callID: String?,
        always: Bool,
        approve: Bool
    ) {
        stateLock.lock()
        defer { stateLock.unlock() }

        var entry = approvals[toolName] ?? ApprovalRecord()
        if always || callID == nil {
            entry.approvedForAllCalls = approve
            entry.rejectedForAllCalls = !approve
            entry.approvedCallIDs.removeAll()
            entry.rejectedCallIDs.removeAll()
            approvals[toolName] = entry
            return
        }

        guard let callID else {
            approvals[toolName] = entry
            return
        }

        if approve {
            entry.rejectedCallIDs.remove(callID)
            entry.approvedCallIDs.insert(callID)
        } else {
            entry.approvedCallIDs.remove(callID)
            entry.rejectedCallIDs.insert(callID)
        }

        approvals[toolName] = entry
    }

    private static func _toStringOrNil(_ value: Any?) -> String? {
        guard let value else { return nil }
        if let string = value as? String {
            return string
        }
        return String(describing: value)
    }

    private static func _resolveToolNameFromRawItem(_ rawItem: Any) -> String? {
        if let rawDict = rawItem as? [String: Any] {
            return _toStringOrNil(rawDict["name"] ?? rawDict["type"] ?? rawDict["tool_name"] ?? rawDict["toolName"])
        }
        return _toStringOrNil(_reflectedProperty(named: "name", from: rawItem)
            ?? _reflectedProperty(named: "type", from: rawItem)
            ?? _reflectedProperty(named: "toolName", from: rawItem)
            ?? _reflectedProperty(named: "tool_name", from: rawItem))
    }

    private static func _resolveCallIDFromRawItem(_ rawItem: Any) -> String? {
        if let rawDict = rawItem as? [String: Any] {
            if let providerData = rawDict["provider_data"] as? [String: Any],
               _toStringOrNil(providerData["type"]) == "mcp_approval_request",
               let mcpID = _toStringOrNil(providerData["id"]) {
                return mcpID
            }

            return _toStringOrNil(rawDict["call_id"] ?? rawDict["id"] ?? rawDict["callID"])
        }

        if let providerData = _reflectedProperty(named: "providerData", from: rawItem)
            ?? _reflectedProperty(named: "provider_data", from: rawItem),
           let providerDict = providerData as? [String: Any],
           _toStringOrNil(providerDict["type"]) == "mcp_approval_request",
           let mcpID = _toStringOrNil(providerDict["id"])
        {
            return mcpID
        }

        return _toStringOrNil(_reflectedProperty(named: "callID", from: rawItem)
            ?? _reflectedProperty(named: "call_id", from: rawItem)
            ?? _reflectedProperty(named: "id", from: rawItem))
    }

    private static func _reflectedProperty(named propertyName: String, from value: Any) -> Any? {
        var currentMirror: Mirror? = Mirror(reflecting: value)
        while let mirror = currentMirror {
            for child in mirror.children where child.label == propertyName {
                return child.value
            }
            currentMirror = mirror.superclassMirror
        }
        return nil
    }

    private static func _hydrateApprovalField(
        recordValue: JSONValue?,
        approve: Bool,
        record: inout ApprovalRecord
    ) {
        guard let recordValue else { return }

        switch recordValue {
        case .bool(let allCalls):
            if approve {
                record.approvedForAllCalls = allCalls
                if allCalls {
                    record.approvedCallIDs.removeAll()
                }
            } else {
                record.rejectedForAllCalls = allCalls
                if allCalls {
                    record.rejectedCallIDs.removeAll()
                }
            }
        case .array(let values):
            let ids = values.compactMap { value -> String? in
                guard case .string(let id) = value else { return nil }
                return id
            }
            if approve {
                record.approvedForAllCalls = false
                record.approvedCallIDs = Set(ids)
            } else {
                record.rejectedForAllCalls = false
                record.rejectedCallIDs = Set(ids)
            }
        default:
            break
        }
    }
}

open class AgentHookContext<TContext>: RunContextWrapper<TContext>, @unchecked Sendable {}
