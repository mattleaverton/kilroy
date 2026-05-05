import Foundation

// MARK: - Default Handler Registration

public func registerDefaultHandlers(
    registry: HandlerRegistry,
    backend: CodergenBackend,
    interviewer: Interviewer
) {
    registry.register(StartHandler())
    registry.register(ExitHandler())
    registry.register(CodergenHandler(backend: backend))
    registry.register(WaitHumanHandler(interviewer: interviewer))
    registry.register(ConditionalHandler())
    registry.register(ParallelHandler(registry: registry))
    registry.register(FanInHandler())
    registry.register(ToolHandler())
    registry.register(ManagerLoopHandler(backend: backend))
}
