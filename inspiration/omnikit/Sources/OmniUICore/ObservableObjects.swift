// Minimal SwiftUI-like observable object property wrappers.
//
// NOTE: This intentionally does not use Combine (portable to Linux).
// Updates are reflected because the renderer rebuilds the view hierarchy every frame.

public protocol ObservableObject: AnyObject {}

@propertyWrapper
@dynamicMemberLookup
public struct ObservedObject<ObjectType: ObservableObject> {
    public var wrappedValue: ObjectType

    public init(wrappedValue: ObjectType) {
        self.wrappedValue = wrappedValue
    }

    public var projectedValue: ObservedObject<ObjectType> { self }

    public subscript<Value>(dynamicMember keyPath: ReferenceWritableKeyPath<ObjectType, Value>) -> Binding<Value> {
        let runtime = _UIRuntime._current
        let path = _UIRuntime._currentPath
        return Binding(
            get: { wrappedValue[keyPath: keyPath] },
            set: { newValue in
                wrappedValue[keyPath: keyPath] = newValue
                if let runtime, let path { runtime._markDirtyFromBinding(path: path) }
            }
        )
    }
}

@propertyWrapper
@dynamicMemberLookup
public struct StateObject<ObjectType: ObservableObject> {
    private let seed: _StateSeed
    private let initial: () -> ObjectType

    public init(wrappedValue: @autoclosure @escaping () -> ObjectType, fileID: StaticString = #fileID, line: UInt = #line) {
        self.seed = _StateSeed(fileID: fileID, line: line)
        self.initial = wrappedValue
    }

    public var wrappedValue: ObjectType {
        get {
            guard let runtime = _UIRuntime._current, let path = _UIRuntime._currentPath else {
                return initial()
            }
            return runtime._getState(seed: seed, path: path, initial: initial)
        }
    }

    public var projectedValue: StateObject<ObjectType> { self }

    public subscript<Value>(dynamicMember keyPath: ReferenceWritableKeyPath<ObjectType, Value>) -> Binding<Value> {
        let runtime = _UIRuntime._current
        let path = _UIRuntime._currentPath
        return Binding(
            get: { wrappedValue[keyPath: keyPath] },
            set: { newValue in
                wrappedValue[keyPath: keyPath] = newValue
                if let runtime, let path { runtime._markDirtyFromBinding(path: path) }
            }
        )
    }
}

final class _EnvironmentObjectLocation<ObjectType: AnyObject>: @unchecked Sendable {
    var object: ObjectType? = nil
    init() {}
}

@propertyWrapper
@dynamicMemberLookup
public struct EnvironmentObject<ObjectType: ObservableObject> {
    private let location = _EnvironmentObjectLocation<ObjectType>()

    public init() {}

    public var wrappedValue: ObjectType {
        if let cached = location.object {
            return cached
        }
        let env = _UIRuntime._currentEnvironment
        if let object = env?._getObject(ObjectType.self) {
            location.object = object
            return object
        }
        fatalError("Missing EnvironmentObject<\(ObjectType.self)>. Use .environmentObject(_) on an ancestor view.")
    }

    public var projectedValue: EnvironmentObject<ObjectType> { self }

    public subscript<Value>(dynamicMember keyPath: ReferenceWritableKeyPath<ObjectType, Value>) -> Binding<Value> {
        // Capture the object now so later event contexts (keypresses, clicks) don't need TaskLocal environment.
        let object = wrappedValue
        let runtime = _UIRuntime._current
        let path = _UIRuntime._currentPath
        return Binding(
            get: { object[keyPath: keyPath] },
            set: { newValue in
                object[keyPath: keyPath] = newValue
                if let runtime, let path { runtime._markDirtyFromBinding(path: path) }
            }
        )
    }
}

// SwiftUI's `@Bindable` comes from Observation; provide a tiny stand-in that works with our
// "always rebuild" renderer model.
@propertyWrapper
@dynamicMemberLookup
public struct Bindable<ObjectType: AnyObject> {
    public var wrappedValue: ObjectType

    public init(wrappedValue: ObjectType) {
        self.wrappedValue = wrappedValue
    }

    public var projectedValue: Bindable<ObjectType> { self }

    public subscript<Value>(dynamicMember keyPath: ReferenceWritableKeyPath<ObjectType, Value>) -> Binding<Value> {
        let runtime = _UIRuntime._current
        let path = _UIRuntime._currentPath
        return Binding(
            get: { wrappedValue[keyPath: keyPath] },
            set: { newValue in
                wrappedValue[keyPath: keyPath] = newValue
                if let runtime, let path { runtime._markDirtyFromBinding(path: path) }
            }
        )
    }
}
