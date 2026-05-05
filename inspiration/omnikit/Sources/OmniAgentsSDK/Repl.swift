import Foundation

public func runDemoLoop<TContext>(
    agent: Agent<TContext>,
    stream: Bool = true,
    context: TContext? = nil,
    maxTurns: Int = DEFAULT_MAX_TURNS
) async throws {
    var currentAgent = agent
    var history: [TResponseInputItem] = []

    while true {
        print("\n> ", terminator: "")
        fflush(stdout)
        guard let line = readLine() else { break }
        let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
        if ["exit", "quit"].contains(trimmed.lowercased()) {
            break
        }

        let newInput = ItemHelpers.inputToNewInputList(input: line)
        let input: StringOrInputList = history.isEmpty ? .string(line) : .inputList(history + newInput)

        if stream {
            let result = Runner.runStreamed(
                currentAgent,
                input: input,
                context: context,
                maxTurns: maxTurns
            )

            for try await event in result.streamEvents() {
                switch event {
                case .runItem(let itemEvent):
                    if case .messageOutputCreated = itemEvent.name,
                       let messageItem = itemEvent.item as? MessageOutputItem {
                        let text = ItemHelpers.textMessageOutput(message: messageItem)
                        if !text.isEmpty { print(text) }
                    }
                case .agentUpdated(let agentEvent):
                    if let nextAgent = agentEvent.newAgent as? Agent<TContext> {
                        currentAgent = nextAgent
                        print("[handoff -> \(nextAgent.name)]")
                    }
                case .rawResponse:
                    break
                }
            }

            let state = result.toState()
            history = state.modelInputItems + state.generatedItems.compactMap { try? $0.toInputItem() }
            if let lastAgent = result.lastAgent?.typed(as: TContext.self) {
                currentAgent = lastAgent
            }
        } else {
            let result = try await Runner.run(
                currentAgent,
                input: input,
                context: context,
                maxTurns: maxTurns
            )
            if let text = result.finalOutput as? String, !text.isEmpty {
                print(text)
            } else {
                print(String(describing: result.finalOutput))
            }
            history = result.toInputList()
            if let lastAgent = result.lastAgent?.typed(as: TContext.self) {
                currentAgent = lastAgent
            }
        }
    }
}

public func run_demo_loop<TContext>(
    _ agent: Agent<TContext>,
    stream: Bool = true,
    context: TContext? = nil,
    max_turns: Int = DEFAULT_MAX_TURNS
) async throws {
    try await runDemoLoop(agent: agent, stream: stream, context: context, maxTurns: max_turns)
}
