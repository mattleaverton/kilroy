import Foundation

enum GuardrailRuntime {
    static func runInputGuardrails<TContext>(
        _ guardrails: [InputGuardrail<TContext>],
        context: RunContextWrapper<TContext>,
        agent: Agent<TContext>,
        input: StringOrInputList
    ) async throws -> [InputGuardrailResult<TContext>] {
        var results: [InputGuardrailResult<TContext>] = []
        for guardrail in guardrails {
            let result = try await guardrail.run(agent: agent, input: input, context: context)
            results.append(result)
            if result.output.tripwireTriggered {
                throw InputGuardrailTripwireTriggered(guardrailResult: result, guardrailName: guardrail.getName())
            }
        }
        return results
    }

    static func runOutputGuardrails<TContext>(
        _ guardrails: [OutputGuardrail<TContext>],
        context: RunContextWrapper<TContext>,
        agent: Agent<TContext>,
        output: Any
    ) async throws -> [OutputGuardrailResult<TContext>] {
        var results: [OutputGuardrailResult<TContext>] = []
        for guardrail in guardrails {
            let result = try await guardrail.run(context: context, agent: agent, agentOutput: output)
            results.append(result)
            if result.output.tripwireTriggered {
                throw OutputGuardrailTripwireTriggered(guardrailResult: result, guardrailName: guardrail.getName())
            }
        }
        return results
    }
}
