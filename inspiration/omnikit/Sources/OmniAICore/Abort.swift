import Foundation

public final class AbortSignal: @unchecked Sendable {
    // Actor-based cancellation token.
    private let state = _AbortState()

    public init() {}

    public func abort() {
        Task { await state.abort() }
    }

    public var isAborted: Bool {
        get async { await state.isAborted }
    }

    public func check() async throws {
        if await state.isAborted {
            throw AbortError(message: "Aborted")
        }
    }

    public func wait() async {
        await state.wait()
    }
}

private actor _AbortState {
    var isAborted: Bool = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func abort() {
        guard !isAborted else { return }
        isAborted = true
        let ws = waiters
        waiters.removeAll(keepingCapacity: true)
        for w in ws {
            w.resume(returning: ())
        }
    }

    func wait() async {
        if isAborted { return }
        await withCheckedContinuation { cont in
            waiters.append(cont)
        }
    }
}

