import Foundation

public enum ApplyPatchOperationType: String, Codable, Sendable {
    case createFile = "create_file"
    case updateFile = "update_file"
    case deleteFile = "delete_file"
}

public struct ApplyPatchOperation: @unchecked Sendable {
    public var type: ApplyPatchOperationType
    public var path: String
    public var diff: String?
    public var contextWrapper: RunContextWrapper<Any>?

    public init(
        type: ApplyPatchOperationType,
        path: String,
        diff: String? = nil,
        contextWrapper: RunContextWrapper<Any>? = nil
    ) {
        self.type = type
        self.path = path
        self.diff = diff
        self.contextWrapper = contextWrapper
    }
}

public struct ApplyPatchResult: Sendable, Codable {
    public enum Status: String, Codable, Sendable {
        case completed
        case failed
    }

    public var status: Status?
    public var output: String?

    public init(status: Status? = nil, output: String? = nil) {
        self.status = status
        self.output = output
    }
}

public protocol ApplyPatchEditor: Sendable {
    func createFile(_ operation: ApplyPatchOperation) async throws -> ApplyPatchResult?
    func updateFile(_ operation: ApplyPatchOperation) async throws -> ApplyPatchResult?
    func deleteFile(_ operation: ApplyPatchOperation) async throws -> ApplyPatchResult?
}
