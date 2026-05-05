import Foundation

public enum StringOrInputList: Sendable, Equatable, Codable {
    case string(String)
    case inputList([TResponseInputItem])

    public init(_ string: String) {
        self = .string(string)
    }

    public init(_ inputList: [TResponseInputItem]) {
        self = .inputList(inputList)
    }

    public var stringValue: String? {
        if case .string(let value) = self {
            return value
        }
        return nil
    }

    public var inputItems: [TResponseInputItem] {
        switch self {
        case .string(let value):
            return ItemHelpers.inputToNewInputList(input: value)
        case .inputList(let items):
            return items
        }
    }

    enum CodingKeys: String, CodingKey {
        case type
        case string
        case inputList = "input_list"
    }

    public init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let type = try container.decode(String.self, forKey: .type)
        switch type {
        case "string":
            self = .string(try container.decode(String.self, forKey: .string))
        case "input_list":
            self = .inputList(try container.decode([TResponseInputItem].self, forKey: .inputList))
        default:
            throw DecodingError.dataCorruptedError(forKey: .type, in: container, debugDescription: "Unknown StringOrInputList type: \(type)")
        }
    }

    public func encode(to encoder: any Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .string(let value):
            try container.encode("string", forKey: .type)
            try container.encode(value, forKey: .string)
        case .inputList(let items):
            try container.encode("input_list", forKey: .type)
            try container.encode(items, forKey: .inputList)
        }
    }
}

public typealias RunInput = StringOrInputList
