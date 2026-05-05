import Foundation

public actor AgentToolUseTracker {
    private var usage: [String: [String]] = [:]

    public init() {}

    public func record(agentName: String, toolName: String) {
        usage[agentName, default: []].append(toolName)
    }

    public func snapshot() -> [String: [String]] {
        usage
    }
}

