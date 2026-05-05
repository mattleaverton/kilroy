import Foundation
import OmniAICore

enum AgentRunnerHelpers {
    static func makeContextWrapper<TContext>(context: TContext?) throws -> RunContextWrapper<TContext> {
        if let context {
            return RunContextWrapper(context: context)
        }
        if TContext.self == Void.self, let voidContext = () as? TContext {
            return RunContextWrapper(context: voidContext)
        }
        throw UserError(message: "Context is required for this agent run")
    }

    static func resolveModel<TContext>(agent: Agent<TContext>, runConfig: RunConfig?) -> any Model {
        let provider = runConfig?.modelProvider ?? MultiProvider()

        let resolvedReference = runConfig?.model ?? agent.model
        switch resolvedReference {
        case .instance(let instance):
            return instance
        case .name(let name):
            return provider.getModel(name)
        case .none:
            return provider.getModel(nil)
        }
    }

    static func resolveModelSettings<TContext>(agent: Agent<TContext>, runConfig: RunConfig?) -> ModelSettings {
        agent.modelSettings.resolve(override: runConfig?.modelSettings)
    }

    static func renderFinalOutput<TContext>(response: ModelResponse, agent: Agent<TContext>) throws -> Any {
        let text = response.output.compactMap { item -> String? in
            guard item["type"]?.stringValue == "message" else { return nil }
            return try? ItemHelpers.extractLastContent(message: item)
        }.joined()

        if let outputSchema = agent.outputType {
            if outputSchema.isPlainText {
                return text
            }
            return try outputSchema.validateJSON(text)
        }

        return text
    }
}
