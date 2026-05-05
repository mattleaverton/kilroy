import Foundation

public struct ToolInputGuardrailResult<TContext>: Sendable {
    public var guardrail: ToolInputGuardrail<TContext>
    public var output: ToolGuardrailFunctionOutput

    public init(guardrail: ToolInputGuardrail<TContext>, output: ToolGuardrailFunctionOutput) {
        self.guardrail = guardrail
        self.output = output
    }
}

public struct ToolOutputGuardrailResult<TContext>: Sendable {
    public var guardrail: ToolOutputGuardrail<TContext>
    public var output: ToolGuardrailFunctionOutput

    public init(guardrail: ToolOutputGuardrail<TContext>, output: ToolGuardrailFunctionOutput) {
        self.guardrail = guardrail
        self.output = output
    }
}

public struct ToolGuardrailFunctionOutput: @unchecked Sendable {
    public enum Behavior: Sendable {
        case allow
        case rejectContent(message: String)
        case raiseException
    }

    public var outputInfo: Any
    public var behavior: Behavior

    public init(outputInfo: Any = (), behavior: Behavior = .allow) {
        self.outputInfo = outputInfo
        self.behavior = behavior
    }

    public static func allow(outputInfo: Any = ()) -> ToolGuardrailFunctionOutput {
        ToolGuardrailFunctionOutput(outputInfo: outputInfo, behavior: .allow)
    }

    public static func rejectContent(_ message: String, outputInfo: Any = ()) -> ToolGuardrailFunctionOutput {
        ToolGuardrailFunctionOutput(outputInfo: outputInfo, behavior: .rejectContent(message: message))
    }

    public static func raiseException(outputInfo: Any = ()) -> ToolGuardrailFunctionOutput {
        ToolGuardrailFunctionOutput(outputInfo: outputInfo, behavior: .raiseException)
    }
}

public struct ToolInputGuardrailData<TContext>: Sendable {
    public var context: ToolContext<TContext>
    public var agent: AnyAgent

    public init(context: ToolContext<TContext>, agent: AnyAgent) {
        self.context = context
        self.agent = agent
    }
}

public struct ToolOutputGuardrailData<TContext>: @unchecked Sendable {
    public var context: ToolContext<TContext>
    public var agent: AnyAgent
    public var output: Any

    public init(context: ToolContext<TContext>, agent: AnyAgent, output: Any) {
        self.context = context
        self.agent = agent
        self.output = output
    }
}

public struct ToolInputGuardrail<TContext>: Sendable {
    public typealias GuardrailFunction = @Sendable (ToolInputGuardrailData<TContext>) -> MaybeAwaitable<ToolGuardrailFunctionOutput>
    public var guardrailFunction: GuardrailFunction
    public var name: String?

    public init(name: String? = nil, guardrailFunction: @escaping GuardrailFunction) {
        self.name = name
        self.guardrailFunction = guardrailFunction
    }

    public func getName() -> String {
        name ?? "tool_input_guardrail"
    }

    public func run(_ data: ToolInputGuardrailData<TContext>) async throws -> ToolGuardrailFunctionOutput {
        try await guardrailFunction(data).resolve()
    }
}

public struct ToolOutputGuardrail<TContext>: Sendable {
    public typealias GuardrailFunction = @Sendable (ToolOutputGuardrailData<TContext>) -> MaybeAwaitable<ToolGuardrailFunctionOutput>
    public var guardrailFunction: GuardrailFunction
    public var name: String?

    public init(name: String? = nil, guardrailFunction: @escaping GuardrailFunction) {
        self.name = name
        self.guardrailFunction = guardrailFunction
    }

    public func getName() -> String {
        name ?? "tool_output_guardrail"
    }

    public func run(_ data: ToolOutputGuardrailData<TContext>) async throws -> ToolGuardrailFunctionOutput {
        try await guardrailFunction(data).resolve()
    }
}
