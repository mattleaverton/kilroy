import Foundation

public struct RawResponsesStreamEvent: Sendable {
    public var data: TResponseStreamEvent
    public var type: String

    public init(data: TResponseStreamEvent, type: String = "raw_response_event") {
        self.data = data
        self.type = type
    }
}

public struct RunItemStreamEvent: Sendable {
    public enum Name: String, Sendable {
        case messageOutputCreated = "message_output_created"
        case handoffRequested = "handoff_requested"
        case handoffOccured = "handoff_occured"
        case toolCalled = "tool_called"
        case toolOutput = "tool_output"
        case reasoningItemCreated = "reasoning_item_created"
        case mcpApprovalRequested = "mcp_approval_requested"
        case mcpApprovalResponse = "mcp_approval_response"
        case mcpListTools = "mcp_list_tools"
    }

    public var name: Name
    public var item: any RunItem
    public var type: String

    public init(name: Name, item: any RunItem, type: String = "run_item_stream_event") {
        self.name = name
        self.item = item
        self.type = type
    }
}

public struct AgentUpdatedStreamEvent: @unchecked Sendable {
    public var newAgent: Any
    public var type: String

    public init(newAgent: Any, type: String = "agent_updated_stream_event") {
        self.newAgent = newAgent
        self.type = type
    }
}

public enum AgentStreamEvent: @unchecked Sendable {
    case rawResponse(RawResponsesStreamEvent)
    case runItem(RunItemStreamEvent)
    case agentUpdated(AgentUpdatedStreamEvent)
}
