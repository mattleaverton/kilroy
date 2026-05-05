import Foundation

public struct ClaudePromptSkill: Sendable {
    public let name: String
    public let description: String
    public let content: String

    public init(name: String, description: String, content: String) {
        self.name = name
        self.description = description
        self.content = content
    }
}

public struct ClaudePromptGitInfo: Sendable {
    public let branch: String?
    public let hasUncommittedChanges: Bool
    public let recentCommits: String?

    public init(branch: String?, hasUncommittedChanges: Bool, recentCommits: String?) {
        self.branch = branch
        self.hasUncommittedChanges = hasUncommittedChanges
        self.recentCommits = recentCommits
    }
}

public struct ClaudePromptEnvironment: Sendable {
    public let workingDirectory: URL
    public let gitInfo: ClaudePromptGitInfo?

    public init(workingDirectory: URL, gitInfo: ClaudePromptGitInfo?) {
        self.workingDirectory = workingDirectory
        self.gitInfo = gitInfo
    }
}

public typealias Skill = ClaudePromptSkill
public typealias GitInfo = ClaudePromptGitInfo
public typealias CodergenEnvironment = ClaudePromptEnvironment
