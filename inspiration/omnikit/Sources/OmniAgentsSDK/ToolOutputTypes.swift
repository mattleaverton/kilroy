import Foundation
import OmniAICore

public struct ToolOutputText: Codable, Sendable, Equatable {
    public var type: String
    public var text: String

    public init(text: String) {
        type = "text"
        self.text = text
    }

    enum CodingKeys: String, CodingKey {
        case type
        case text
    }
}

public typealias ToolOutputTextDict = [String: JSONValue]

public enum ToolOutputImageDetail: String, Codable, Sendable, Equatable {
    case low
    case high
    case auto
}

public struct ToolOutputImage: Codable, Sendable, Equatable {
    public var type: String
    public var imageURL: String?
    public var fileID: String?
    public var detail: ToolOutputImageDetail?

    public init(imageURL: String? = nil, fileID: String? = nil, detail: ToolOutputImageDetail? = nil) {
        type = "image"
        self.imageURL = imageURL
        self.fileID = fileID
        self.detail = detail
    }

    enum CodingKeys: String, CodingKey {
        case type
        case imageURL = "image_url"
        case fileID = "file_id"
        case detail
    }
}

public typealias ToolOutputImageDict = [String: JSONValue]

public struct ToolOutputFileContent: Codable, Sendable, Equatable {
    public var type: String
    public var fileData: String?
    public var fileURL: String?
    public var fileID: String?
    public var filename: String?

    public init(
        fileData: String? = nil,
        fileURL: String? = nil,
        fileID: String? = nil,
        filename: String? = nil
    ) {
        type = "file"
        self.fileData = fileData
        self.fileURL = fileURL
        self.fileID = fileID
        self.filename = filename
    }

    enum CodingKeys: String, CodingKey {
        case type
        case fileData = "file_data"
        case fileURL = "file_url"
        case fileID = "file_id"
        case filename
    }
}

public typealias ToolOutputFileContentDict = [String: JSONValue]
