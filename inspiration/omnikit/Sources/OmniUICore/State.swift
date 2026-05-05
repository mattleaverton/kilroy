/// A SwiftUI-like `Binding`.
@propertyWrapper
public struct Binding<Value>: @unchecked Sendable {
    public var wrappedValue: Value {
        get { get() }
        nonmutating set { set(newValue) }
    }

    public var projectedValue: Binding<Value> { self }

    let get: () -> Value
    let set: (Value) -> Void

    public init(get: @escaping () -> Value, set: @escaping (Value) -> Void) {
        self.get = get
        self.set = set
    }
}

/// A SwiftUI-like `State` backed by the current `_UIRuntime` via a build context.
///
/// This is intentionally limited and is designed to be portable and strict-concurrency friendly.
@propertyWrapper
public struct State<Value> {
    private let seed: _StateSeed
    private let initial: () -> Value
    private let location: _StateLocation

    public init(wrappedValue: Value, fileID: StaticString = #fileID, line: UInt = #line) {
        self.seed = _StateSeed(fileID: fileID, line: line)
        self.initial = { wrappedValue }
        self.location = _StateLocation()
    }

    public var wrappedValue: Value {
        get {
            if let resolved = location.resolved {
                return resolved.runtime._getState(seed: seed, path: resolved.path, initial: initial)
            }
            guard let runtime = _UIRuntime._current, let path = _UIRuntime._currentPath else {
                // Accessing outside a runtime build: fall back to the initial value.
                return initial()
            }
            // Resolve the owning view path the first time we access this State during a build.
            // This ensures mutations from nested event contexts (e.g. inside ForEach rows) still
            // update the same state slot instead of keying off the event's TaskLocal path.
            location.resolved = (runtime: runtime, path: path)
            return runtime._getState(seed: seed, path: path, initial: initial)
        }
        nonmutating set {
            if let resolved = location.resolved {
                resolved.runtime._setState(seed: seed, path: resolved.path, value: newValue)
                return
            }
            guard let runtime = _UIRuntime._current, let path = _UIRuntime._currentPath else { return }
            location.resolved = (runtime: runtime, path: path)
            runtime._setState(seed: seed, path: path, value: newValue)
        }
    }

    public var projectedValue: Binding<Value> {
        // Capture the runtime/path at projection time so the binding remains valid when invoked
        // from other event contexts (e.g. a focused TextField receiving keypresses).
        if let resolved = location.resolved {
            return Binding(
                get: { resolved.runtime._getState(seed: seed, path: resolved.path, initial: initial) },
                set: { resolved.runtime._setState(seed: seed, path: resolved.path, value: $0) }
            )
        }

        if let runtime = _UIRuntime._current, let path = _UIRuntime._currentPath {
            location.resolved = (runtime: runtime, path: path)
            return Binding(
                get: { runtime._getState(seed: seed, path: path, initial: initial) },
                set: { runtime._setState(seed: seed, path: path, value: $0) }
            )
        }

        return Binding(
            get: { self.wrappedValue },
            set: { self.wrappedValue = $0 }
        )
    }
}

struct _StateSeed {
    let fileID: StaticString
    let line: UInt
}

final class _StateLocation: @unchecked Sendable {
    // Resolved at first build-time access.
    var resolved: (runtime: _UIRuntime, path: [Int])? = nil
    init() {}
}
