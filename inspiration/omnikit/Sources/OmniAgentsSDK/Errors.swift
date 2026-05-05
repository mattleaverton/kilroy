import Foundation

/// Base error type for OmniAgentsSDK, matching Python's `AgentsException` role.
open class AgentsError: Error, LocalizedError, CustomStringConvertible, @unchecked Sendable {
    public let message: String
    public let cause: (any Error)?
    public var runData: RunErrorDetails?

    public init(message: String, cause: (any Error)? = nil, runData: RunErrorDetails? = nil) {
        self.message = message
        self.cause = cause
        self.runData = runData
    }

    public var errorDescription: String? {
        message
    }

    public var description: String {
        if let runData {
            return "\(message)\n\(prettyPrintRunErrorDetails(runData))"
        }
        return message
    }
}

public final class MaxTurnsExceeded: AgentsError, @unchecked Sendable {
    public init(message: String) {
        super.init(message: message)
    }
}

public final class ModelBehaviorError: AgentsError, @unchecked Sendable {
    public init(message: String) {
        super.init(message: message)
    }
}

public final class UserError: AgentsError, @unchecked Sendable {
    public init(message: String) {
        super.init(message: message)
    }
}

public final class ToolTimeoutError: AgentsError, @unchecked Sendable {
    public let toolName: String
    public let timeoutSeconds: TimeInterval

    public init(toolName: String, timeoutSeconds: TimeInterval) {
        self.toolName = toolName
        self.timeoutSeconds = timeoutSeconds
        super.init(message: "Tool '\(toolName)' timed out after \(timeoutSeconds.formatted(.number.precision(.fractionLength(0...3)))) seconds.")
    }
}

public final class InputGuardrailTripwireTriggered: AgentsError, @unchecked Sendable {
    public let guardrailResult: Any

    public init(guardrailResult: Any, guardrailName: String? = nil) {
        self.guardrailResult = guardrailResult
        let resolvedName = guardrailName ?? String(describing: type(of: guardrailResult))
        super.init(message: "Guardrail \(resolvedName) triggered tripwire")
    }
}

public final class OutputGuardrailTripwireTriggered: AgentsError, @unchecked Sendable {
    public let guardrailResult: Any

    public init(guardrailResult: Any, guardrailName: String? = nil) {
        self.guardrailResult = guardrailResult
        let resolvedName = guardrailName ?? String(describing: type(of: guardrailResult))
        super.init(message: "Guardrail \(resolvedName) triggered tripwire")
    }
}

public final class ToolInputGuardrailTripwireTriggered: AgentsError, @unchecked Sendable {
    public let guardrail: Any
    public let output: Any

    public init(guardrail: Any, output: Any, guardrailName: String? = nil) {
        self.guardrail = guardrail
        self.output = output
        let resolvedName = guardrailName ?? String(describing: type(of: guardrail))
        super.init(message: "Tool input guardrail \(resolvedName) triggered tripwire")
    }
}

public final class ToolOutputGuardrailTripwireTriggered: AgentsError, @unchecked Sendable {
    public let guardrail: Any
    public let output: Any

    public init(guardrail: Any, output: Any, guardrailName: String? = nil) {
        self.guardrail = guardrail
        self.output = output
        let resolvedName = guardrailName ?? String(describing: type(of: guardrail))
        super.init(message: "Tool output guardrail \(resolvedName) triggered tripwire")
    }
}

/// Snapshot of run state captured when an `AgentsError` is thrown.
public struct RunErrorDetails: CustomStringConvertible, @unchecked Sendable {
    public var input: Any
    public var newItems: [Any]
    public var rawResponses: [Any]
    public var lastAgent: AnyAgent?
    public var contextWrapper: Any?
    public var inputGuardrailResults: [Any]
    public var outputGuardrailResults: [Any]

    public var guardrailResults: [Any] {
        inputGuardrailResults + outputGuardrailResults
    }

    public init(
        input: Any,
        newItems: [Any] = [],
        rawResponses: [Any] = [],
        lastAgent: AnyAgent? = nil,
        contextWrapper: Any? = nil,
        inputGuardrailResults: [Any] = [],
        outputGuardrailResults: [Any] = []
    ) {
        self.input = input
        self.newItems = newItems
        self.rawResponses = rawResponses
        self.lastAgent = lastAgent
        self.contextWrapper = contextWrapper
        self.inputGuardrailResults = inputGuardrailResults
        self.outputGuardrailResults = outputGuardrailResults
    }

    public var description: String {
        prettyPrintRunErrorDetails(self)
    }
}

public func prettyPrintRunErrorDetails(_ details: RunErrorDetails) -> String {
    let agentDescription: String = {
        guard let lastAgent = details.lastAgent else { return "none" }
        if let agentName = reflectedStringProperty(named: "name", from: lastAgent) {
            return "Agent(name=\"\(agentName)\", ...)"
        }
        return String(describing: type(of: lastAgent))
    }()

    return [
        "RunErrorDetails:",
        "- Last agent: \(agentDescription)",
        "- \(details.newItems.count) new item(s)",
        "- \(details.rawResponses.count) raw response(s)",
        "- \(details.inputGuardrailResults.count) input guardrail result(s)",
        "(See `RunErrorDetails` for more details)",
    ].joined(separator: "\n")
}

private func reflectedStringProperty(named propertyName: String, from value: Any) -> String? {
    var currentMirror: Mirror? = Mirror(reflecting: value)
    while let mirror = currentMirror {
        for child in mirror.children where child.label == propertyName {
            return child.value as? String
        }
        currentMirror = mirror.superclassMirror
    }
    return nil
}
