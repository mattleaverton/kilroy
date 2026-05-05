import Foundation

// MARK: - AppStorage

public protocol _AppStorageValue {
    static func _read(from store: UserDefaults, key: String) -> Self?
    static func _write(_ value: Self, to store: UserDefaults, key: String)
}

extension Bool: _AppStorageValue {
    public static func _read(from store: UserDefaults, key: String) -> Bool? {
        store.object(forKey: key) as? Bool
    }

    public static func _write(_ value: Bool, to store: UserDefaults, key: String) {
        store.set(value, forKey: key)
    }
}

extension Int: _AppStorageValue {
    public static func _read(from store: UserDefaults, key: String) -> Int? {
        store.object(forKey: key) as? Int
    }

    public static func _write(_ value: Int, to store: UserDefaults, key: String) {
        store.set(value, forKey: key)
    }
}

extension Double: _AppStorageValue {
    public static func _read(from store: UserDefaults, key: String) -> Double? {
        store.object(forKey: key) as? Double
    }

    public static func _write(_ value: Double, to store: UserDefaults, key: String) {
        store.set(value, forKey: key)
    }
}

extension String: _AppStorageValue {
    public static func _read(from store: UserDefaults, key: String) -> String? {
        store.string(forKey: key)
    }

    public static func _write(_ value: String, to store: UserDefaults, key: String) {
        store.set(value, forKey: key)
    }
}

extension URL: _AppStorageValue {
    public static func _read(from store: UserDefaults, key: String) -> URL? {
        store.url(forKey: key)
    }

    public static func _write(_ value: URL, to store: UserDefaults, key: String) {
        store.set(value, forKey: key)
    }
}

extension Data: _AppStorageValue {
    public static func _read(from store: UserDefaults, key: String) -> Data? {
        store.data(forKey: key)
    }

    public static func _write(_ value: Data, to store: UserDefaults, key: String) {
        store.set(value, forKey: key)
    }
}

@propertyWrapper
public struct AppStorage<Value> {
    private let key: String
    private let store: UserDefaults
    private let write: (Value) -> Void

    @State private var value: Value

    public init(wrappedValue: Value, _ key: String, store: UserDefaults = .standard) where Value: _AppStorageValue {
        self.key = key
        self.store = store

        let initial = Value._read(from: store, key: key) ?? wrappedValue
        self._value = State(wrappedValue: initial)
        self.write = { v in
            Value._write(v, to: store, key: key)
        }
    }

    public init(wrappedValue: Value, _ key: String, store: UserDefaults = .standard) where Value: RawRepresentable, Value.RawValue: _AppStorageValue {
        self.key = key
        self.store = store

        let raw = Value.RawValue._read(from: store, key: key)
        let initial = raw.flatMap(Value.init(rawValue:)) ?? wrappedValue
        self._value = State(wrappedValue: initial)
        self.write = { v in
            Value.RawValue._write(v.rawValue, to: store, key: key)
        }
    }

    public var wrappedValue: Value {
        get { value }
        nonmutating set {
            value = newValue
            write(newValue)
        }
    }

    public var projectedValue: Binding<Value> {
        Binding(get: { wrappedValue }, set: { wrappedValue = $0 })
    }
}

// MARK: - Namespace

@propertyWrapper
public struct Namespace {
    public struct ID: Hashable, Sendable {
        let rawValue: UUID
        init() { self.rawValue = UUID() }
    }

    private let id: ID

    public init() {
        self.id = ID()
    }

    public var wrappedValue: ID { id }
}

