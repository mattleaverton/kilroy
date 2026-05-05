import Foundation

// A small, portable SwiftData-like surface. The goal is not full persistence, just enough API
// for common `Schema`/`ModelContainer`/`ModelContext`/`@Query` usage to compile and be usable.

public enum SortOrder: Sendable {
    case forward
    case reverse
}

public struct Schema: @unchecked Sendable {
    public let modelTypes: [Any.Type]

    public init(_ modelTypes: [Any.Type]) {
        self.modelTypes = modelTypes
    }
}

public struct ModelConfiguration: Sendable {
    public let schema: Schema
    public let isStoredInMemoryOnly: Bool

    public init(schema: Schema, isStoredInMemoryOnly: Bool = false) {
        self.schema = schema
        self.isStoredInMemoryOnly = isStoredInMemoryOnly
    }
}

public final class ModelContainer: @unchecked Sendable {
    public let schema: Schema
    public let configurations: [ModelConfiguration]
    public let mainContext: ModelContext
    private let store: _ModelStore

    public init(for schema: Schema, configurations: [ModelConfiguration] = []) throws {
        self.schema = schema
        self.configurations = configurations
        self.store = _ModelStore()
        self.mainContext = ModelContext(schema: schema, store: store)
    }

    public convenience init(for modelTypes: [Any.Type], inMemory: Bool = false) throws {
        let schema = Schema(modelTypes)
        let cfg = ModelConfiguration(schema: schema, isStoredInMemoryOnly: inMemory)
        try self.init(for: schema, configurations: [cfg])
    }

    public func makeContext() -> ModelContext {
        ModelContext(schema: schema, store: store)
    }
}

/// A minimal in-memory store for SwiftData-like models (`@Model` classes).
public final class ModelContext: @unchecked Sendable {
    private let schema: Schema?
    private let allowedTypeIDs: Set<ObjectIdentifier>?
    private let store: _ModelStore
    private let observerID: UUID
    private let lock = NSLock()

    public init(schema: Schema? = nil) {
        self.schema = schema
        self.allowedTypeIDs = schema.map { Set($0.modelTypes.map(ObjectIdentifier.init)) }
        self.store = _ModelStore()
        self.observerID = UUID()
        store.registerObserver(id: observerID) {
            _UIRuntime._current?._markDirtyFromModelContext()
        }
    }

    init(schema: Schema? = nil, store: _ModelStore) {
        self.schema = schema
        self.allowedTypeIDs = schema.map { Set($0.modelTypes.map(ObjectIdentifier.init)) }
        self.store = store
        self.observerID = UUID()
        store.registerObserver(id: observerID) {
            _UIRuntime._current?._markDirtyFromModelContext()
        }
    }

    deinit {
        store.unregisterObserver(id: observerID)
    }

    private func _isAllowedType<T: AnyObject>(_ type: T.Type) -> Bool {
        guard let allowedTypeIDs else { return true }
        return allowedTypeIDs.contains(ObjectIdentifier(type))
    }

    public func insert<T: AnyObject>(_ model: T) {
        lock.lock()
        defer { lock.unlock() }
        guard _isAllowedType(T.self) else { return }
        store.insert(model)
    }

    public func delete<T: AnyObject>(_ model: T) {
        lock.lock()
        defer { lock.unlock() }
        store.delete(model)
    }

    public func fetch<T: AnyObject>(_ type: T.Type = T.self) -> [T] {
        lock.lock()
        defer { lock.unlock() }
        guard _isAllowedType(T.self) else { return [] }
        return store.fetch(type)
    }

    public func save() throws {
        // In-memory backend commits immediately.
    }

    public func reset() {
        lock.lock()
        defer { lock.unlock() }
        store.reset()
    }
}

final class _ModelStore: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [ObjectIdentifier: [AnyObject]] = [:]
    private var observers: [UUID: () -> Void] = [:]

    func registerObserver(id: UUID, _ observer: @escaping () -> Void) {
        lock.lock()
        defer { lock.unlock() }
        observers[id] = observer
    }

    func unregisterObserver(id: UUID) {
        lock.lock()
        defer { lock.unlock() }
        observers.removeValue(forKey: id)
    }

    func insert<T: AnyObject>(_ model: T) {
        lock.lock()
        let key = ObjectIdentifier(T.self)
        var arr = storage[key] ?? []
        if !arr.contains(where: { $0 === model }) {
            arr.append(model)
            storage[key] = arr
            let callbacks = Array(observers.values)
            lock.unlock()
            callbacks.forEach { $0() }
            return
        }
        lock.unlock()
    }

    func delete<T: AnyObject>(_ model: T) {
        lock.lock()
        let key = ObjectIdentifier(T.self)
        guard var arr = storage[key] else {
            lock.unlock()
            return
        }
        let originalCount = arr.count
        arr.removeAll(where: { $0 === model })
        storage[key] = arr
        let changed = arr.count != originalCount
        let callbacks = changed ? Array(observers.values) : []
        lock.unlock()
        callbacks.forEach { $0() }
    }

    func fetch<T: AnyObject>(_ type: T.Type) -> [T] {
        lock.lock()
        defer { lock.unlock() }
        let key = ObjectIdentifier(type)
        return (storage[key] ?? []).compactMap { $0 as? T }
    }

    func reset() {
        lock.lock()
        storage.removeAll(keepingCapacity: true)
        let callbacks = Array(observers.values)
        lock.unlock()
        callbacks.forEach { $0() }
    }
}

@propertyWrapper
public struct Query<Element: AnyObject> {
    private let sort: ((Element, Element) -> Bool)?

    public init() {
        self.sort = nil
    }

    public init<V>(sort: KeyPath<Element, V>, order: SortOrder = .forward) where V: Comparable {
        self.sort = { lhs, rhs in
            let l = lhs[keyPath: sort]
            let r = rhs[keyPath: sort]
            switch order {
            case .forward:
                return l < r
            case .reverse:
                return l > r
            }
        }
    }

    public var wrappedValue: [Element] {
        let runtime = _UIRuntime._current
        let env = _UIRuntime._currentEnvironment ?? runtime?._baseEnvironment
        let ctx = env?.modelContext
        var items = ctx?.fetch(Element.self) ?? []
        if let sort {
            items.sort(by: sort)
        }
        return items
    }
}
