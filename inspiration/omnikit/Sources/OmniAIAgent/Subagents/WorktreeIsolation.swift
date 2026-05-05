import Foundation

struct ManagedGitWorktree: Sendable {
    let repoRoot: String
    let path: String
    let branch: String
}

func createGitWorktreeEnvironment(from env: ExecutionEnvironment, agentID: String) async throws -> LocalExecutionEnvironment {
    let repoRoot = try await resolveGitRepoRoot(from: env)
    let worktreesRoot = URL(fileURLWithPath: repoRoot, isDirectory: true)
        .appendingPathComponent(".ai/subagents/worktrees", isDirectory: true)
    try FileManager.default.createDirectory(at: worktreesRoot, withIntermediateDirectories: true)

    let branchSuffix = String(UUID().uuidString.lowercased().prefix(8))
    let branch = "omnikit-subagent-\(sanitizeBranchComponent(agentID.prefix(12)))\(branchSuffix)"
    let worktreePath = worktreesRoot.appendingPathComponent(agentID, isDirectory: true).path

    let createCommand = "git -C \(worktreeShellEscape(repoRoot)) worktree add -b \(worktreeShellEscape(branch)) \(worktreeShellEscape(worktreePath)) HEAD"
    let createResult = try await env.execCommand(
        command: createCommand,
        timeoutMs: 60_000,
        workingDir: repoRoot,
        envVars: nil
    )

    guard createResult.exitCode == 0, !createResult.timedOut else {
        throw ToolError.validationError(
            "Failed to create git worktree at \(worktreePath): \(compactCommandOutput(createResult))"
        )
    }

    let worktree = ManagedGitWorktree(repoRoot: repoRoot, path: worktreePath, branch: branch)
    let isolatedEnv = LocalExecutionEnvironment(
        workingDir: worktreePath,
        cleanupHandler: {
            try await cleanupGitWorktree(worktree)
        }
    )
    try await isolatedEnv.initialize()
    return isolatedEnv
}

private func resolveGitRepoRoot(from env: ExecutionEnvironment) async throws -> String {
    let workingDir = env.workingDirectory()
    let result = try await env.execCommand(
        command: "git rev-parse --show-toplevel",
        timeoutMs: 10_000,
        workingDir: workingDir,
        envVars: nil
    )
    guard result.exitCode == 0, !result.timedOut else {
        throw ToolError.validationError(
            "worktree isolation requires a git repository; \(workingDir) is not inside one"
        )
    }

    let repoRoot = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !repoRoot.isEmpty else {
        throw ToolError.validationError(
            "worktree isolation requires a git repository; unable to resolve repository root from \(workingDir)"
        )
    }
    return repoRoot
}

private func cleanupGitWorktree(_ worktree: ManagedGitWorktree) async throws {
    let cleanupEnv = LocalExecutionEnvironment(workingDir: worktree.repoRoot)
    _ = try? await cleanupEnv.execCommand(
        command: "git -C \(worktreeShellEscape(worktree.repoRoot)) worktree remove --force \(worktreeShellEscape(worktree.path))",
        timeoutMs: 60_000,
        workingDir: worktree.repoRoot,
        envVars: nil
    )
    _ = try? await cleanupEnv.execCommand(
        command: "git -C \(worktreeShellEscape(worktree.repoRoot)) worktree prune",
        timeoutMs: 60_000,
        workingDir: worktree.repoRoot,
        envVars: nil
    )
    _ = try? await cleanupEnv.execCommand(
        command: "git -C \(worktreeShellEscape(worktree.repoRoot)) branch -D \(worktreeShellEscape(worktree.branch))",
        timeoutMs: 60_000,
        workingDir: worktree.repoRoot,
        envVars: nil
    )

    if FileManager.default.fileExists(atPath: worktree.path) {
        try? FileManager.default.removeItem(atPath: worktree.path)
    }
}

private func compactCommandOutput(_ result: ExecResult) -> String {
    let text = result.combinedOutput.trimmingCharacters(in: .whitespacesAndNewlines)
    if text.isEmpty {
        return result.timedOut ? "command timed out" : "exit code \(result.exitCode)"
    }
    return text
}

private func sanitizeBranchComponent<S: StringProtocol>(_ value: S) -> String {
    let allowed = Set("abcdefghijklmnopqrstuvwxyz0123456789-")
    let lowercased = value.lowercased()
    let mapped = lowercased.map { allowed.contains($0) ? String($0) : "-" }.joined()
    let trimmed = mapped.trimmingCharacters(in: CharacterSet(charactersIn: "-"))
    return trimmed.isEmpty ? "agent" : trimmed
}

private func worktreeShellEscape(_ value: String) -> String {
    if value.isEmpty {
        return "''"
    }
    return "'" + value.replacing("'", with: "'\\''") + "'"
}
