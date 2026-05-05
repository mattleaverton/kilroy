import Foundation

open class RunHooksBase<TContext>: @unchecked Sendable {
    public init() {}

    open func onLLMStart(
        agent: Agent<TContext>,
        input: [TResponseInputItem],
        context: RunContextWrapper<TContext>
    ) async {}

    open func onLLMEnd(
        agent: Agent<TContext>,
        response: ModelResponse,
        context: RunContextWrapper<TContext>
    ) async {}

    open func onAgentStart(agent: Agent<TContext>, context: RunContextWrapper<TContext>) async {}

    open func onAgentEnd(agent: Agent<TContext>, result: Any, context: RunContextWrapper<TContext>) async {}

    open func onHandoff(
        from: Agent<TContext>,
        to: Agent<TContext>,
        context: RunContextWrapper<TContext>
    ) async {}

    open func onToolStart(
        tool: Tool,
        context: RunContextWrapper<TContext>,
        arguments: String,
        callID: String
    ) async {}

    open func onToolEnd(
        tool: Tool,
        context: RunContextWrapper<TContext>,
        result: Any,
        callID: String
    ) async {}
}

open class AgentHooksBase<TContext>: @unchecked Sendable {
    public init() {}

    open func onStart(agent: Agent<TContext>, context: RunContextWrapper<TContext>) async {}
    open func onEnd(agent: Agent<TContext>, output: Any, context: RunContextWrapper<TContext>) async {}
    open func onHandoff(from: Agent<TContext>, to: Agent<TContext>, context: RunContextWrapper<TContext>) async {}
    open func onToolStart(agent: Agent<TContext>, tool: Tool, context: RunContextWrapper<TContext>, arguments: String, callID: String) async {}
    open func onToolEnd(agent: Agent<TContext>, tool: Tool, context: RunContextWrapper<TContext>, result: Any, callID: String) async {}

    open func onLLMStart(
        agent: Agent<TContext>,
        input: [TResponseInputItem],
        context: RunContextWrapper<TContext>
    ) async {}

    open func onLLMEnd(
        agent: Agent<TContext>,
        response: ModelResponse,
        context: RunContextWrapper<TContext>
    ) async {}
}

public typealias RunHooks<TContext> = RunHooksBase<TContext>
public typealias AgentHooks<TContext> = AgentHooksBase<TContext>
