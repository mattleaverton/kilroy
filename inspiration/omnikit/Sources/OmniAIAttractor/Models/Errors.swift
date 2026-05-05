import Foundation

// MARK: - Attractor Errors

public enum AttractorError: Error, Sendable, CustomStringConvertible {
    // Parse errors
    case parseError(String)
    case invalidDOT(String)

    // Validation errors
    case validationFailed([Diagnostic])

    // Execution errors
    case noStartNode
    case noExitNode
    case handlerNotFound(String)
    case nodeNotFound(String)
    case executionFailed(String)
    case goalGateUnsatisfied([String])
    case retryExhausted(String, Int)
    case timeout(String, TimeInterval)

    // LLM errors
    case llmError(String)
    case llmTimeout(String)

    // Pipeline errors
    case pipelineError(String)
    case checkpointError(String)

    public var description: String {
        switch self {
        case .parseError(let msg): return "Parse error: \(msg)"
        case .invalidDOT(let msg): return "Invalid DOT: \(msg)"
        case .validationFailed(let diags):
            let errors = diags.filter { $0.isError }
            return "Validation failed with \(errors.count) error(s): \(errors.map(\.message).joined(separator: "; "))"
        case .noStartNode: return "No start node (shape=Mdiamond) found"
        case .noExitNode: return "No exit node (shape=Msquare) found"
        case .handlerNotFound(let type): return "No handler registered for type: \(type)"
        case .nodeNotFound(let id): return "Node not found: \(id)"
        case .executionFailed(let msg): return "Execution failed: \(msg)"
        case .goalGateUnsatisfied(let nodes): return "Goal gate nodes unsatisfied: \(nodes.joined(separator: ", "))"
        case .retryExhausted(let node, let count): return "Retry exhausted for node \(node) after \(count) attempts"
        case .timeout(let node, let secs): return "Timeout for node \(node) after \(secs)s"
        case .llmError(let msg): return "LLM error: \(msg)"
        case .llmTimeout(let msg): return "LLM timeout: \(msg)"
        case .pipelineError(let msg): return "Pipeline error: \(msg)"
        case .checkpointError(let msg): return "Checkpoint error: \(msg)"
        }
    }
}
