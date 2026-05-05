/// Minimal SwiftUI-compatible `@FocusState`.
///
/// This integrates with OmniUI's focus system via the `.focused(...)` modifier, which registers
/// a focus binding with the runtime and keeps it in sync.
@propertyWrapper
public struct FocusState<Value: Hashable> {
    private let seed: _StateSeed
    private let initial: () -> Value
    private let location: _StateLocation

    public init(wrappedValue: Value, fileID: StaticString = #fileID, line: UInt = #line) {
        self.seed = _StateSeed(fileID: fileID, line: line)
        self.initial = { wrappedValue }
        self.location = _StateLocation()
    }

    // SwiftUI allows `@FocusState var isFocused: Bool` with no explicit default.
    public init(fileID: StaticString = #fileID, line: UInt = #line) where Value == Bool {
        self.init(wrappedValue: false, fileID: fileID, line: line)
    }

    public var wrappedValue: Value {
        get {
            if let resolved = location.resolved {
                return resolved.runtime._getState(seed: seed, path: resolved.path, initial: initial)
            }
            guard let runtime = _UIRuntime._current, let path = _UIRuntime._currentPath else {
                return initial()
            }
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

    public struct Binding {
        public var wrappedValue: Value {
            get { get() }
            nonmutating set { set(newValue) }
        }

        let get: () -> Value
        let set: (Value) -> Void
    }

    public var projectedValue: Binding {
        if let resolved = location.resolved {
            let runtime = resolved.runtime
            let path = resolved.path
            return Binding(
                get: { [weak runtime] in
                    runtime?._getState(seed: seed, path: path, initial: initial) ?? initial()
                },
                set: { [weak runtime] value in
                    runtime?._setState(seed: seed, path: path, value: value)
                }
            )
        }
        if let runtime = _UIRuntime._current, let path = _UIRuntime._currentPath {
            location.resolved = (runtime: runtime, path: path)
            return Binding(
                get: { [weak runtime] in
                    runtime?._getState(seed: seed, path: path, initial: initial) ?? initial()
                },
                set: { [weak runtime] value in
                    runtime?._setState(seed: seed, path: path, value: value)
                }
            )
        }
        return Binding(get: { self.wrappedValue }, set: { self.wrappedValue = $0 })
    }
}
