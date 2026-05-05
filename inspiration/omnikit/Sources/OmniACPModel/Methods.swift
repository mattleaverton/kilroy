import Foundation

public enum Initialize: Method {
    public static let name = "initialize"

    public struct Parameters: Codable, Sendable {
        public var protocolVersion: Int
        public var clientInfo: ClientInfo?
        public var clientCapabilities: ClientCapabilities
        public var options: [String: Value]?

        public init(
            protocolVersion: Int,
            clientInfo: ClientInfo? = nil,
            clientCapabilities: ClientCapabilities,
            options: [String: Value]? = nil
        ) {
            self.protocolVersion = protocolVersion
            self.clientInfo = clientInfo
            self.clientCapabilities = clientCapabilities
            self.options = options
        }
    }

    public struct Result: Codable, Sendable {
        public var protocolVersion: Int
        public var agentInfo: AgentInfo?
        public var agentCapabilities: AgentCapabilities
        public var modes: Modes?
        public var availableCommands: [AvailableCommand]?
        public var authMethods: [Value]?

        public init(
            protocolVersion: Int,
            agentInfo: AgentInfo? = nil,
            agentCapabilities: AgentCapabilities,
            modes: Modes? = nil,
            availableCommands: [AvailableCommand]? = nil,
            authMethods: [Value]? = nil
        ) {
            self.protocolVersion = protocolVersion
            self.agentInfo = agentInfo
            self.agentCapabilities = agentCapabilities
            self.modes = modes
            self.availableCommands = availableCommands
            self.authMethods = authMethods
        }
    }
}

public struct InitializedNotification: Notification {
    public static let name = "notifications/initialized"
    public typealias Parameters = Empty
}

public enum SessionNew: Method {
    public static let name = "session/new"

    public struct Parameters: Codable, Sendable {
        public var cwd: String
        public var mcpServers: [MCPServer]

        public init(cwd: String, mcpServers: [MCPServer] = []) {
            self.cwd = cwd
            self.mcpServers = mcpServers
        }
    }

    public struct Result: Codable, Sendable {
        public var sessionID: String

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
        }

        public init(sessionID: String) {
            self.sessionID = sessionID
        }
    }
}

public enum SessionPrompt: Method {
    public static let name = "session/prompt"

    public struct Parameters: Codable, Sendable {
        public var sessionID: String
        public var prompt: [ContentBlock]

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
            case prompt
        }

        public init(sessionID: String, prompt: [ContentBlock]) {
            self.sessionID = sessionID
            self.prompt = prompt
        }
    }

    public struct Result: Codable, Sendable {
        public var stopReason: StopReason

        public init(stopReason: StopReason) {
            self.stopReason = stopReason
        }
    }
}

public enum SessionCancel: Method {
    public static let name = "session/cancel"

    public struct Parameters: Codable, Sendable {
        public var sessionID: String

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
        }

        public init(sessionID: String) {
            self.sessionID = sessionID
        }
    }

    public typealias Result = Empty
}

public enum SessionSetMode: Method {
    public static let name = "session/set_mode"

    public struct Parameters: Codable, Sendable {
        public var sessionID: String
        public var modeID: String

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
            case modeID = "modeId"
        }

        public init(sessionID: String, modeID: String) {
            self.sessionID = sessionID
            self.modeID = modeID
        }
    }

    public typealias Result = Empty
}

public enum SessionList: Method {
    public static let name = "session/list"
    public static let schemaStatus: SchemaStatus = .draft

    public typealias Parameters = Empty

    public struct Result: Codable, Sendable {
        public var sessions: [SessionSummary]

        public init(sessions: [SessionSummary]) {
            self.sessions = sessions
        }
    }
}

public enum SessionResume: Method {
    public static let name = "session/resume"
    public static let schemaStatus: SchemaStatus = .draft

    public struct Parameters: Codable, Sendable {
        public var sessionID: String
        public var cwd: String

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
            case cwd
        }

        public init(sessionID: String, cwd: String) {
            self.sessionID = sessionID
            self.cwd = cwd
        }
    }

    public struct Result: Codable, Sendable {
        public var sessionID: String

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
        }

        public init(sessionID: String) {
            self.sessionID = sessionID
        }
    }
}

public enum SessionFork: Method {
    public static let name = "session/fork"
    public static let schemaStatus: SchemaStatus = .draft

    public struct Parameters: Codable, Sendable {
        public var sessionID: String

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
        }

        public init(sessionID: String) {
            self.sessionID = sessionID
        }
    }

    public struct Result: Codable, Sendable {
        public var sessionID: String

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
        }

        public init(sessionID: String) {
            self.sessionID = sessionID
        }
    }
}

public enum SessionRequestPermission: Method {
    public static let name = "session/request_permission"

    public struct Parameters: Codable, Sendable {
        public var sessionID: String?
        public var message: String?
        public var toolCall: PermissionToolCall?
        public var options: [PermissionOption]?

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
            case message
            case toolCall
            case options
        }

        public init(
            sessionID: String? = nil,
            message: String? = nil,
            toolCall: PermissionToolCall? = nil,
            options: [PermissionOption]? = nil
        ) {
            self.sessionID = sessionID
            self.message = message
            self.toolCall = toolCall
            self.options = options
        }
    }

    public struct Result: Codable, Sendable {
        public var outcome: PermissionOutcome

        public init(outcome: PermissionOutcome) {
            self.outcome = outcome
        }
    }
}

public enum FileSystemReadTextFile: Method {
    public static let name = "fs/read_text_file"

    public struct Parameters: Codable, Sendable {
        public var sessionID: String
        public var path: String
        public var line: Int?
        public var limit: Int?

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
            case path
            case line
            case limit
        }

        public init(sessionID: String, path: String, line: Int? = nil, limit: Int? = nil) {
            self.sessionID = sessionID
            self.path = path
            self.line = line
            self.limit = limit
        }
    }

    public struct Result: Codable, Sendable {
        public var content: String
        public var totalLines: Int?

        private enum CodingKeys: String, CodingKey {
            case content
            case totalLines = "total_lines"
        }

        public init(content: String, totalLines: Int? = nil) {
            self.content = content
            self.totalLines = totalLines
        }
    }
}

public enum FileSystemWriteTextFile: Method {
    public static let name = "fs/write_text_file"

    public struct Parameters: Codable, Sendable {
        public var sessionID: String
        public var path: String
        public var content: String

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
            case path
            case content
        }

        public init(sessionID: String, path: String, content: String) {
            self.sessionID = sessionID
            self.path = path
            self.content = content
        }
    }

    public typealias Result = Empty
}

public enum TerminalCreate: Method {
    public static let name = "terminal/create"

    public struct Parameters: Codable, Sendable {
        public var sessionID: String
        public var command: String
        public var args: [String]?
        public var cwd: String?
        public var env: [EnvVariable]?
        public var outputByteLimit: Int?

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
            case command
            case args
            case cwd
            case env
            case outputByteLimit
        }

        public init(
            sessionID: String,
            command: String,
            args: [String]? = nil,
            cwd: String? = nil,
            env: [EnvVariable]? = nil,
            outputByteLimit: Int? = nil
        ) {
            self.sessionID = sessionID
            self.command = command
            self.args = args
            self.cwd = cwd
            self.env = env
            self.outputByteLimit = outputByteLimit
        }
    }

    public struct Result: Codable, Sendable {
        public var terminalID: TerminalID

        private enum CodingKeys: String, CodingKey {
            case terminalID = "terminalId"
        }

        public init(terminalID: TerminalID) {
            self.terminalID = terminalID
        }
    }
}

public enum TerminalOutput: Method {
    public static let name = "terminal/output"

    public struct Parameters: Codable, Sendable {
        public var sessionID: String
        public var terminalID: TerminalID

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
            case terminalID = "terminalId"
        }

        public init(sessionID: String, terminalID: TerminalID) {
            self.sessionID = sessionID
            self.terminalID = terminalID
        }
    }

    public struct Result: Codable, Sendable {
        public var output: String
        public var exitStatus: TerminalExitStatus?
        public var truncated: Bool

        public init(output: String, exitStatus: TerminalExitStatus? = nil, truncated: Bool) {
            self.output = output
            self.exitStatus = exitStatus
            self.truncated = truncated
        }
    }
}

public enum TerminalWaitForExit: Method {
    public static let name = "terminal/wait_for_exit"

    public struct Parameters: Codable, Sendable {
        public var sessionID: String
        public var terminalID: TerminalID

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
            case terminalID = "terminalId"
        }

        public init(sessionID: String, terminalID: TerminalID) {
            self.sessionID = sessionID
            self.terminalID = terminalID
        }
    }

    public struct Result: Codable, Sendable {
        public var exitCode: Int?
        public var signal: String?

        public init(exitCode: Int? = nil, signal: String? = nil) {
            self.exitCode = exitCode
            self.signal = signal
        }
    }
}

public enum TerminalKill: Method {
    public static let name = "terminal/kill"

    public struct Parameters: Codable, Sendable {
        public var sessionID: String
        public var terminalID: TerminalID

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
            case terminalID = "terminalId"
        }

        public init(sessionID: String, terminalID: TerminalID) {
            self.sessionID = sessionID
            self.terminalID = terminalID
        }
    }

    public struct Result: Codable, Sendable {
        public var success: Bool

        public init(success: Bool) {
            self.success = success
        }
    }
}

public enum TerminalRelease: Method {
    public static let name = "terminal/release"

    public struct Parameters: Codable, Sendable {
        public var sessionID: String
        public var terminalID: TerminalID

        private enum CodingKeys: String, CodingKey {
            case sessionID = "sessionId"
            case terminalID = "terminalId"
        }

        public init(sessionID: String, terminalID: TerminalID) {
            self.sessionID = sessionID
            self.terminalID = terminalID
        }
    }

    public struct Result: Codable, Sendable {
        public var success: Bool

        public init(success: Bool) {
            self.success = success
        }
    }
}
