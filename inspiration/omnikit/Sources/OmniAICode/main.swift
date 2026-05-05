import Foundation
import OmniAIAgent
import OmniAICore
import OmniMCP

@main
struct OmniAICodeCLI {
    static func main() async {
        do {
            let command = try CLICommand(arguments: Array(CommandLine.arguments.dropFirst()))
            try await command.run()
        } catch let error as ExitError {
            if !error.message.isEmpty {
                fputs("Error: \(error.message)\n", stderr)
            }
            Foundation.exit(error.code)
        } catch {
            fputs("Error: \(error)\n", stderr)
            Foundation.exit(1)
        }
    }
}

private struct CLICommand {
    enum Mode {
        case exec
    }

    let mode: Mode
    let promptParts: [String]
    let provider: String
    let model: String
    let workdir: String?
    let sessionID: String?
    let resume: Bool
    let jsonOutput: Bool
    let rewindResponseId: String?
    let mcpConfigPaths: [String]
    let mcpInlineConfigs: [String]

    init(arguments: [String]) throws {
        guard let first = arguments.first else {
            throw ExitError(code: 2, message: usageText)
        }

        switch first {
        case "exec":
            mode = .exec
        case "-h", "--help", "help":
            throw ExitError(code: 0, message: usageText)
        default:
            throw ExitError(code: 2, message: "Unknown command '\(first)'.\n\n\(usageText)")
        }

        var idx = 1
        var promptParts: [String] = []
        var provider = "openai"
        var model = ""
        var workdir: String?
        var sessionID: String?
        var resume = false
        var jsonOutput = false
        var rewindResponseId: String?
        var mcpConfigPaths: [String] = []
        var mcpInlineConfigs: [String] = []

        while idx < arguments.count {
            let arg = arguments[idx]
            switch arg {
            case "--provider":
                idx += 1
                guard idx < arguments.count else {
                    throw ExitError(code: 2, message: "--provider requires a value")
                }
                provider = arguments[idx]
            case "--model":
                idx += 1
                guard idx < arguments.count else {
                    throw ExitError(code: 2, message: "--model requires a value")
                }
                model = arguments[idx]
            case "--workdir":
                idx += 1
                guard idx < arguments.count else {
                    throw ExitError(code: 2, message: "--workdir requires a value")
                }
                workdir = arguments[idx]
            case "--session-id":
                idx += 1
                guard idx < arguments.count else {
                    throw ExitError(code: 2, message: "--session-id requires a value")
                }
                sessionID = arguments[idx]
            case "--resume":
                resume = true
            case "--json":
                jsonOutput = true
            case "--rewind":
                idx += 1
                guard idx < arguments.count else {
                    throw ExitError(code: 2, message: "--rewind requires a response id")
                }
                rewindResponseId = arguments[idx]
            case "--mcp-config":
                idx += 1
                guard idx < arguments.count else {
                    throw ExitError(code: 2, message: "--mcp-config requires a path")
                }
                mcpConfigPaths.append(arguments[idx])
            case "--mcp-server":
                idx += 1
                guard idx < arguments.count else {
                    throw ExitError(code: 2, message: "--mcp-server requires JSON")
                }
                mcpInlineConfigs.append(arguments[idx])
            case "-h", "--help":
                throw ExitError(code: 0, message: usageText)
            default:
                if arg.hasPrefix("-") {
                    throw ExitError(code: 2, message: "Unknown option '\(arg)'")
                }
                promptParts.append(arg)
            }
            idx += 1
        }

        if rewindResponseId != nil && !resume {
            throw ExitError(code: 2, message: "--rewind requires --resume")
        }

        self.promptParts = promptParts
        self.provider = provider
        self.model = model
        self.workdir = workdir
        self.sessionID = sessionID
        self.resume = resume
        self.jsonOutput = jsonOutput
        self.rewindResponseId = rewindResponseId
        self.mcpConfigPaths = mcpConfigPaths
        self.mcpInlineConfigs = mcpInlineConfigs
    }

    func run() async throws {
        switch mode {
        case .exec:
            try await runExec()
        }
    }

    private func runExec() async throws {
        if let workdir {
            guard FileManager.default.changeCurrentDirectoryPath(workdir) else {
                throw ExitError(code: 2, message: "Failed to set working directory to \(workdir)")
            }
        }

        let prompt = try readPrompt(from: promptParts)
        let sessionsRoot = URL(fileURLWithPath: ".ai/omni-code-sessions", isDirectory: true)
        let storageBackend = FileSessionStorageBackend(rootDirectory: sessionsRoot)

        let resolvedSessionID: String = try await {
            if resume {
                if let sessionID {
                    guard try await storageBackend.load(sessionID: sessionID) != nil else {
                        throw ExitError(code: 2, message: "No session found with id \(sessionID)")
                    }
                    return sessionID
                }
                return try await findLatestSessionID(in: sessionsRoot)
            }
            return sessionID ?? UUID().uuidString
        }()

        let mcpServers = try loadMCPServers(paths: mcpConfigPaths, inline: mcpInlineConfigs)
        let sessionConfig = SessionConfig(mcp: MCPSessionConfig(servers: mcpServers))

        let env = LocalExecutionEnvironment(workingDir: FileManager.default.currentDirectoryPath)
        try await env.initialize()

        let client = try await Client.fromEnvAsync()
        let profile = try buildProfile(provider: provider, model: model)

        let session = try Session(
            profile: profile,
            environment: env,
            client: client,
            config: sessionConfig,
            sessionID: resolvedSessionID,
            storageBackend: storageBackend,
            autoRestoreFromStorage: resume
        )

        let processor: EventProcessor = jsonOutput ? JSONEventProcessor() : HumanEventProcessor()
        await session.eventEmitter.on { event in
            processor.process(event)
        }

        if let rewindResponseId {
            try await session.rewind(toResponseID: rewindResponseId)
        }

        await session.submit(prompt)
        processor.flush()
    }
}

private func readPrompt(from parts: [String]) throws -> String {
    if parts.isEmpty || (parts.count == 1 && parts[0] == "-") {
        let data = FileHandle.standardInput.readDataToEndOfFile()
        guard let text = String(data: data, encoding: .utf8),
              !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw ExitError(code: 2, message: "Prompt is required (pass as argument or via stdin)")
        }
        return text
    }
    return parts.joined(separator: " ")
}

private func buildProfile(provider: String, model: String) throws -> ProviderProfile {
    switch provider.lowercased() {
    case "openai":
        return OpenAIProfile(
            model: model.isEmpty ? "gpt-5.2-codex" : model,
            forceCodexSystemPrompt: true
        )
    case "anthropic":
        return AnthropicProfile(
            model: model.isEmpty ? "claude-opus-4-6" : model,
            enableTodos: false,
            enableInteractiveTools: false
        )
    case "gemini":
        return GeminiProfile(
            model: model.isEmpty ? "gemini-3.1-pro-preview-customtools" : model,
            interactiveMode: false,
            enableTodos: false,
            enablePlanTools: false
        )
    default:
        throw ExitError(code: 2, message: "Unknown provider '\(provider)'. Use openai, anthropic, or gemini.")
    }
}

private func loadMCPServers(paths: [String], inline: [String]) throws -> [MCPServerConfig] {
    var servers: [MCPServerConfig] = []
    for path in paths {
        let data = try Data(contentsOf: URL(fileURLWithPath: path))
        servers.append(contentsOf: try decodeMCPServerConfigs(from: data))
    }
    for inlineConfig in inline {
        guard let data = inlineConfig.data(using: .utf8) else {
            throw ExitError(code: 2, message: "Invalid MCP JSON config string")
        }
        servers.append(contentsOf: try decodeMCPServerConfigs(from: data))
    }
    return servers
}

private func decodeMCPServerConfigs(from data: Data) throws -> [MCPServerConfig] {
    let decoder = JSONDecoder()
    if let list = try? decoder.decode([MCPServerConfig].self, from: data) {
        return list
    }
    if let single = try? decoder.decode(MCPServerConfig.self, from: data) {
        return [single]
    }
    throw ExitError(code: 2, message: "Failed to decode MCP server config")
}

private func findLatestSessionID(in root: URL) async throws -> String {
    let fm = FileManager.default
    guard fm.fileExists(atPath: root.path) else {
        throw ExitError(code: 2, message: "No sessions found to resume")
    }

    let files = try fm.contentsOfDirectory(at: root, includingPropertiesForKeys: [.contentModificationDateKey])
        .filter { $0.pathExtension == "json" }

    var latest: (id: String, updatedAt: Date)? = nil
    let decoder = JSONDecoder()

    for file in files {
        if let data = try? Data(contentsOf: file),
           let snapshot = try? decoder.decode(SessionSnapshot.self, from: data) {
            let updated = snapshot.updatedAt
            if latest == nil || updated > latest!.updatedAt {
                latest = (snapshot.sessionID, updated)
            }
        } else if let modDate = try? file.resourceValues(forKeys: [.contentModificationDateKey]).contentModificationDate {
            let id = file.deletingPathExtension().lastPathComponent
            if latest == nil || modDate > latest!.updatedAt {
                latest = (id, modDate)
            }
        }
    }

    guard let latest else {
        throw ExitError(code: 2, message: "No sessions found to resume")
    }
    return latest.id
}

private struct ExitError: Error {
    let code: Int32
    let message: String
}

private let usageText = """
Usage:
  omni-ai-code exec [prompt] [options]

Options:
  --provider <name>       Provider (openai, anthropic, gemini)
  --model <name>          Override model name
  --workdir <path>        Working directory
  --session-id <id>       Session identifier
  --resume                Resume latest or named session
  --rewind <response-id>  Rewind to response id before running
  --json                  Emit JSON events
  --mcp-config <path>     Load MCP server config JSON file
  --mcp-server <json>     Inline MCP server config JSON
  -h, --help              Show help
"""
