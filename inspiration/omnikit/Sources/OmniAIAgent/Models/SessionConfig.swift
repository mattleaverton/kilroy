import Foundation

public struct SessionConfig: Sendable, Codable {
    public var maxTurns: Int
    public var maxToolRoundsPerInput: Int
    public var interactiveMode: Bool
    public var defaultCommandTimeoutMs: Int
    public var maxCommandTimeoutMs: Int
    public var reasoningEffort: String?
    public var toolOutputLimits: [String: Int]
    public var toolLineLimits: [String: Int]
    public var enableLoopDetection: Bool
    public var loopDetectionWindow: Int
    public var maxSubagentDepth: Int
    public var userInstructions: String?
    public var llmInactivityTimeoutSeconds: Double?
    public var parallelToolCalls: Bool?
    public var mcp: MCPSessionConfig

    public init(
        maxTurns: Int = 0,
        maxToolRoundsPerInput: Int = 0,
        interactiveMode: Bool = false,
        defaultCommandTimeoutMs: Int = 10_000,
        maxCommandTimeoutMs: Int = 600_000,
        reasoningEffort: String? = nil,
        toolOutputLimits: [String: Int] = [:],
        toolLineLimits: [String: Int] = [:],
        enableLoopDetection: Bool = true,
        loopDetectionWindow: Int = 10,
        maxSubagentDepth: Int = 1,
        userInstructions: String? = nil,
        llmInactivityTimeoutSeconds: Double? = nil,
        parallelToolCalls: Bool? = nil,
        mcp: MCPSessionConfig = MCPSessionConfig()
    ) {
        self.maxTurns = maxTurns
        self.maxToolRoundsPerInput = maxToolRoundsPerInput
        self.interactiveMode = interactiveMode
        self.defaultCommandTimeoutMs = defaultCommandTimeoutMs
        self.maxCommandTimeoutMs = maxCommandTimeoutMs
        self.reasoningEffort = reasoningEffort
        self.toolOutputLimits = toolOutputLimits
        self.toolLineLimits = toolLineLimits
        self.enableLoopDetection = enableLoopDetection
        self.loopDetectionWindow = loopDetectionWindow
        self.maxSubagentDepth = maxSubagentDepth
        self.userInstructions = userInstructions
        self.llmInactivityTimeoutSeconds = llmInactivityTimeoutSeconds
        self.parallelToolCalls = parallelToolCalls
        self.mcp = mcp
    }
}
