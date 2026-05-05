import Foundation

public final actor SQLiteSession: Session {
    public let sessionID: String
    public let sessionSettings: SessionSettings?
    private let fileURL: URL
    private var cachedItems: [TResponseInputItem]

    public init(sessionID: String, path: String? = nil, sessionSettings: SessionSettings? = nil) {
        self.sessionID = sessionID
        self.sessionSettings = sessionSettings
        let baseURL = path.map(URL.init(fileURLWithPath:)) ?? FileManager.default.temporaryDirectory
        self.fileURL = baseURL.appendingPathComponent("\(sessionID).sqlite.json")
        if let data = try? Data(contentsOf: fileURL),
           let decoded = try? JSONDecoder().decode([TResponseInputItem].self, from: data) {
            self.cachedItems = decoded
        } else {
            self.cachedItems = []
        }
    }

    public func getItems(limit: Int? = nil) async throws -> [TResponseInputItem] {
        let resolvedLimit = resolveSessionLimit(limit, settings: sessionSettings)
        guard let resolvedLimit else {
            return cachedItems
        }
        return Array(cachedItems.suffix(resolvedLimit))
    }

    public func addItems(_ items: [TResponseInputItem]) async throws {
        cachedItems.append(contentsOf: items)
        try persist()
    }

    public func popItem() async throws -> TResponseInputItem? {
        let item = cachedItems.popLast()
        try persist()
        return item
    }

    public func clearSession() async throws {
        cachedItems.removeAll()
        try persist()
    }

    private func persist() throws {
        let directory = fileURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let data = try JSONEncoder().encode(cachedItems)
        try data.write(to: fileURL)
    }
}

