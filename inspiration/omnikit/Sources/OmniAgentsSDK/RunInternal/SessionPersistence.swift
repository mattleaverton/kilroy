import Foundation

enum SessionPersistenceRuntime {
    static func prepareInput(
        session: Session?,
        input: StringOrInputList,
        runConfig: RunConfig?
    ) async throws -> [TResponseInputItem] {
        let newItems = input.inputItems
        guard let session else {
            return newItems
        }
        let history = try await session.getItems(limit: runConfig?.sessionSettings?.limit)
        if let callback = runConfig?.sessionInputCallback {
            return try await callback(history, newItems)
        }
        return history + newItems
    }

    static func persist(session: Session?, items: [TResponseInputItem]) async throws {
        guard let session, !items.isEmpty else {
            return
        }
        try await session.addItems(items)
    }
}
