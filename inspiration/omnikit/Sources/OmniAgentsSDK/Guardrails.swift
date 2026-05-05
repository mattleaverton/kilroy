import Foundation

public struct GuardrailFunctionOutput: @unchecked Sendable {
    public var outputInfo: Any
    public var tripwireTriggered: Bool

    public init(outputInfo: Any = (), tripwireTriggered: Bool) {
        self.outputInfo = outputInfo
        self.tripwireTriggered = tripwireTriggered
    }
}

public struct InputGuardrailResult<TContext>: Sendable {
    public var guardrail: InputGuardrail<TContext>
    public var output: GuardrailFunctionOutput

    public init(guardrail: InputGuardrail<TContext>, output: GuardrailFunctionOutput) {
        self.guardrail = guardrail
        self.output = output
    }
}

public struct OutputGuardrailResult<TContext>: @unchecked Sendable {
    public var guardrail: OutputGuardrail<TContext>
    public var agentOutput: Any
    public var agent: Agent<TContext>
    public var output: GuardrailFunctionOutput

    public init(
        guardrail: OutputGuardrail<TContext>,
        agentOutput: Any,
        agent: Agent<TContext>,
        output: GuardrailFunctionOutput
    ) {
        self.guardrail = guardrail
        self.agentOutput = agentOutput
        self.agent = agent
        self.output = output
    }
}

public struct InputGuardrail<TContext>: Sendable {
    public typealias GuardrailFunction = @Sendable (RunContextWrapper<TContext>, Agent<TContext>, StringOrInputList) -> MaybeAwaitable<GuardrailFunctionOutput>

    public var guardrailFunction: GuardrailFunction
    public var name: String?
    public var runInParallel: Bool

    public init(
        name: String? = nil,
        runInParallel: Bool = true,
        guardrailFunction: @escaping GuardrailFunction
    ) {
        self.guardrailFunction = guardrailFunction
        self.name = name
        self.runInParallel = runInParallel
    }

    public func getName() -> String {
        name ?? "input_guardrail"
    }

    public func run(
        agent: Agent<TContext>,
        input: StringOrInputList,
        context: RunContextWrapper<TContext>
    ) async throws -> InputGuardrailResult<TContext> {
        let output = try await guardrailFunction(context, agent, input).resolve()
        return InputGuardrailResult(guardrail: self, output: output)
    }
}

public struct OutputGuardrail<TContext>: Sendable {
    public typealias GuardrailFunction = @Sendable (RunContextWrapper<TContext>, Agent<TContext>, Any) -> MaybeAwaitable<GuardrailFunctionOutput>

    public var guardrailFunction: GuardrailFunction
    public var name: String?

    public init(name: String? = nil, guardrailFunction: @escaping GuardrailFunction) {
        self.guardrailFunction = guardrailFunction
        self.name = name
    }

    public func getName() -> String {
        name ?? "output_guardrail"
    }

    public func run(
        context: RunContextWrapper<TContext>,
        agent: Agent<TContext>,
        agentOutput: Any
    ) async throws -> OutputGuardrailResult<TContext> {
        let output = try await guardrailFunction(context, agent, agentOutput).resolve()
        return OutputGuardrailResult(guardrail: self, agentOutput: agentOutput, agent: agent, output: output)
    }
}
