import Foundation
import OmniAICore

public protocol RunItem: Sendable {
    var agent: AnyAgent? { get }
    var type: String { get }

    func toInputItem() throws -> TResponseInputItem
    func releaseAgent()
}

open class RunItemBase<TRaw>: RunItem, @unchecked Sendable {
    private var strongAgent: AnyAgent?

    public var agent: AnyAgent? {
        strongAgent
    }

    public var rawItem: TRaw

    open class var itemType: String {
        "run_item"
    }

    public var type: String {
        Self.itemType
    }

    public init(agent: AnyAgent?, rawItem: TRaw) {
        strongAgent = agent
        self.rawItem = rawItem
    }

    open func toInputItem() throws -> TResponseInputItem {
        try Self.coerceRawItemToInputItem(rawItem)
    }

    open func releaseAgent() {
        strongAgent = nil
    }

    private static func coerceRawItemToInputItem(_ rawItem: TRaw) throws -> TResponseInputItem {
        if let inputItem = rawItem as? TResponseInputItem {
            return inputItem
        }

        if let rawJSONValue = rawItem as? JSONValue,
           case .object(let inputItem) = rawJSONValue {
            return inputItem
        }

        if let rawDictionary = rawItem as? [String: Any] {
            var inputItem: TResponseInputItem = [:]
            inputItem.reserveCapacity(rawDictionary.count)
            for (key, value) in rawDictionary {
                inputItem[key] = try JSONValue(value)
            }
            return inputItem
        }

        throw AgentsError(message: "Unexpected raw item type: \(String(describing: TRaw.self))")
    }
}

public final class MessageOutputItem: RunItemBase<TResponseOutputItem>, @unchecked Sendable {
    public override class var itemType: String {
        "message_output_item"
    }
}

public final class HandoffCallItem: RunItemBase<TResponseOutputItem>, @unchecked Sendable {
    public override class var itemType: String {
        "handoff_call_item"
    }
}

public final class HandoffOutputItem: RunItemBase<TResponseInputItem>, @unchecked Sendable {
    private var strongSourceAgent: AnyAgent?
    private var strongTargetAgent: AnyAgent?

    public var sourceAgent: AnyAgent? {
        strongSourceAgent
    }

    public var targetAgent: AnyAgent? {
        strongTargetAgent
    }

    public override class var itemType: String {
        "handoff_output_item"
    }

    public init(agent: AnyAgent?, rawItem: TResponseInputItem, sourceAgent: AnyAgent?, targetAgent: AnyAgent?) {
        strongSourceAgent = sourceAgent
        strongTargetAgent = targetAgent
        super.init(agent: agent, rawItem: rawItem)
    }

    public override func releaseAgent() {
        super.releaseAgent()
        strongSourceAgent = nil
        strongTargetAgent = nil
    }
}

public typealias ToolCallItemTypes = TResponseOutputItem
public typealias ToolCallOutputTypes = TResponseInputItem

public final class ToolCallItem: RunItemBase<ToolCallItemTypes>, @unchecked Sendable {
    public let description: String?

    public override class var itemType: String {
        "tool_call_item"
    }

    public init(agent: AnyAgent?, rawItem: TResponseOutputItem, description: String? = nil) {
        self.description = description
        super.init(agent: agent, rawItem: rawItem)
    }
}

public final class ToolCallOutputItem: RunItemBase<ToolCallOutputTypes>, @unchecked Sendable {
    public let output: Any

    public override class var itemType: String {
        "tool_call_output_item"
    }

    public init(agent: AnyAgent?, rawItem: TResponseInputItem, output: Any) {
        self.output = output
        super.init(agent: agent, rawItem: rawItem)
    }

    public override func toInputItem() throws -> TResponseInputItem {
        var payload = try super.toInputItem()
        guard payload["type"]?.stringValue == "shell_call_output" else {
            return payload
        }

        payload["status"] = nil
        payload["shell_output"] = nil
        payload["provider_data"] = nil

        if case .array(let outputs)? = payload["output"] {
            var normalizedOutputs: [JSONValue] = []
            normalizedOutputs.reserveCapacity(outputs.count)

            for outputValue in outputs {
                guard case .object(var outputEntry) = outputValue else {
                    normalizedOutputs.append(outputValue)
                    continue
                }

                if case .object(let outcome)? = outputEntry["outcome"],
                   outcome["type"]?.stringValue == "exit"
                {
                    outputEntry["outcome"] = .object(outcome)
                }

                normalizedOutputs.append(.object(outputEntry))
            }

            payload["output"] = .array(normalizedOutputs)
        }

        return payload
    }
}

public final class ReasoningItem: RunItemBase<TResponseOutputItem>, @unchecked Sendable {
    public override class var itemType: String {
        "reasoning_item"
    }
}

public final class MCPListToolsItem: RunItemBase<TResponseOutputItem>, @unchecked Sendable {
    public override class var itemType: String {
        "mcp_list_tools_item"
    }
}

public final class ToolApprovalItem: RunItemBase<TResponseOutputItem>, @unchecked Sendable {
    public let toolName: String?

    public override class var itemType: String {
        "tool_approval_item"
    }

    public init(agent: AnyAgent?, rawItem: TResponseOutputItem, toolName: String? = nil) {
        self.toolName = toolName ?? rawItem["name"]?.stringValue
        super.init(agent: agent, rawItem: rawItem)
    }

    public var name: String? {
        if let toolName {
            return toolName
        }
        return rawItem["name"]?.stringValue ?? rawItem["tool_name"]?.stringValue
    }

    public var arguments: String? {
        if let arguments = rawItem["arguments"] {
            return ItemHelpers.stringifyJSON(arguments)
        }
        if let arguments = rawItem["params"] {
            return ItemHelpers.stringifyJSON(arguments)
        }
        if let arguments = rawItem["input"] {
            return ItemHelpers.stringifyJSON(arguments)
        }
        return nil
    }

    public var callID: String? {
        rawItem["call_id"]?.stringValue ?? rawItem["id"]?.stringValue
    }

    public override func toInputItem() throws -> TResponseInputItem {
        throw AgentsError(
            message: "ToolApprovalItem cannot be converted to an input item. Filter these before API input preparation."
        )
    }
}

public final class MCPApprovalRequestItem: RunItemBase<TResponseOutputItem>, @unchecked Sendable {
    public override class var itemType: String {
        "mcp_approval_request_item"
    }
}

public final class MCPApprovalResponseItem: RunItemBase<TResponseInputItem>, @unchecked Sendable {
    public override class var itemType: String {
        "mcp_approval_response_item"
    }
}

public final class CompactionItem: RunItemBase<TResponseInputItem>, @unchecked Sendable {
    public override class var itemType: String {
        "compaction_item"
    }

    public override func toInputItem() throws -> TResponseInputItem {
        rawItem
    }
}

public struct ModelResponse: Sendable, Codable, Equatable {
    public var output: [TResponseOutputItem]
    public var usage: Usage
    public var responseID: String?
    public var requestID: String?

    public init(
        output: [TResponseOutputItem],
        usage: Usage,
        responseID: String? = nil,
        requestID: String? = nil
    ) {
        self.output = output
        self.usage = usage
        self.responseID = responseID
        self.requestID = requestID
    }

    public func toInputItems() -> [TResponseInputItem] {
        output
    }

    enum CodingKeys: String, CodingKey {
        case output
        case usage
        case responseID = "response_id"
        case requestID = "request_id"
    }
}

public enum ItemHelpers {
    public static func extractLastContent(message: TResponseOutputItem) throws -> String {
        guard message["type"]?.stringValue == "message" else {
            return ""
        }

        guard case .array(let contents)? = message["content"],
              let lastItem = contents.last,
              case .object(let content) = lastItem
        else {
            return ""
        }

        switch content["type"]?.stringValue {
        case "output_text", "text", "input_text":
            return content["text"]?.stringValue ?? ""
        case "output_refusal", "refusal":
            return content["refusal"]?.stringValue ?? ""
        default:
            throw ModelBehaviorError(
                message: "Unexpected content type: \(content["type"]?.stringValue ?? "unknown")"
            )
        }
    }

    public static func extractLastText(message: TResponseOutputItem) -> String? {
        guard message["type"]?.stringValue == "message" else {
            return nil
        }

        guard case .array(let contents)? = message["content"],
              let lastItem = contents.last,
              case .object(let content) = lastItem,
              ["output_text", "text", "input_text"].contains(content["type"]?.stringValue ?? "")
        else {
            return nil
        }

        return content["text"]?.stringValue
    }

    public static func inputToNewInputList(input: String) -> [TResponseInputItem] {
        [[
            "content": .string(input),
            "role": .string("user"),
        ]]
    }

    public static func inputToNewInputList(input: [TResponseInputItem]) -> [TResponseInputItem] {
        input
    }

    public static func textMessageOutputs(items: [any RunItem]) -> String {
        items.compactMap { item in
            guard let messageOutput = item as? MessageOutputItem else {
                return nil
            }
            return textMessageOutput(message: messageOutput)
        }.joined()
    }

    public static func textMessageOutput(message: MessageOutputItem) -> String {
        guard case .array(let contents)? = message.rawItem["content"] else {
            return ""
        }

        return contents.compactMap { contentItem in
            guard case .object(let contentDictionary) = contentItem,
                  ["output_text", "text", "input_text"].contains(contentDictionary["type"]?.stringValue ?? "")
            else {
                return nil
            }

            return contentDictionary["text"]?.stringValue
        }.joined()
    }

    public static func toolCallOutputItem(toolCall: TResponseOutputItem, output: Any) -> TResponseInputItem {
        var payload: TResponseInputItem = [
            "output": convertToolOutput(output),
            "type": .string("function_call_output"),
        ]

        if let callID = toolCall["call_id"]?.stringValue ?? toolCall["id"]?.stringValue {
            payload["call_id"] = .string(callID)
        }

        return payload
    }

    static func stringifyJSON(_ value: JSONValue) -> String {
        switch value {
        case .string(let stringValue):
            return stringValue
        default:
            if let data = try? value.data(), let stringValue = String(data: data, encoding: .utf8) {
                return stringValue
            }
            return value.description
        }
    }

    private static func convertToolOutput(_ output: Any) -> JSONValue {
        if let convertedList = convertStructuredToolOutputList(output) {
            return .array(convertedList.map(JSONValue.object))
        }

        if let convertedOutput = convertStructuredToolOutput(output) {
            return .array([.object(convertedOutput)])
        }

        if let jsonValue = output as? JSONValue {
            return .string(stringifyJSON(jsonValue))
        }

        if let text = output as? String {
            return .string(text)
        }

        return .string(String(describing: output))
    }

    private static func convertStructuredToolOutputList(_ output: Any) -> [[String: JSONValue]]? {
        if let outputList = output as? [Any] {
            var convertedList: [[String: JSONValue]] = []
            convertedList.reserveCapacity(outputList.count)

            for item in outputList {
                guard let converted = convertStructuredToolOutput(item) else {
                    return nil
                }
                convertedList.append(converted)
            }

            return convertedList
        }

        if let jsonValue = output as? JSONValue,
           case .array(let values) = jsonValue
        {
            var convertedList: [[String: JSONValue]] = []
            convertedList.reserveCapacity(values.count)

            for value in values {
                guard let converted = convertStructuredToolOutput(value) else {
                    return nil
                }
                convertedList.append(converted)
            }

            return convertedList
        }

        return nil
    }

    private static func convertStructuredToolOutput(_ output: Any) -> [String: JSONValue]? {
        if let textOutput = output as? ToolOutputText {
            return [
                "type": .string("input_text"),
                "text": .string(textOutput.text),
            ]
        }

        if let imageOutput = output as? ToolOutputImage {
            guard imageOutput.imageURL != nil || imageOutput.fileID != nil else {
                return nil
            }

            var result: [String: JSONValue] = ["type": .string("input_image")]
            if let imageURL = imageOutput.imageURL {
                result["image_url"] = .string(imageURL)
            }
            if let fileID = imageOutput.fileID {
                result["file_id"] = .string(fileID)
            }
            if let detail = imageOutput.detail {
                result["detail"] = .string(detail.rawValue)
            }
            return result
        }

        if let fileOutput = output as? ToolOutputFileContent {
            guard fileOutput.fileData != nil || fileOutput.fileURL != nil || fileOutput.fileID != nil else {
                return nil
            }

            var result: [String: JSONValue] = ["type": .string("input_file")]
            if let fileData = fileOutput.fileData {
                result["file_data"] = .string(fileData)
            }
            if let fileURL = fileOutput.fileURL {
                result["file_url"] = .string(fileURL)
            }
            if let fileID = fileOutput.fileID {
                result["file_id"] = .string(fileID)
            }
            if let filename = fileOutput.filename {
                result["filename"] = .string(filename)
            }
            return result
        }

        if let object = output as? [String: JSONValue] {
            return convertStructuredToolOutputDictionary(object)
        }

        if let jsonValue = output as? JSONValue,
           case .object(let object) = jsonValue
        {
            return convertStructuredToolOutputDictionary(object)
        }

        if let object = output as? [String: Any],
           let jsonValue = try? JSONValue(object),
           case .object(let jsonObject) = jsonValue
        {
            return convertStructuredToolOutputDictionary(jsonObject)
        }

        return nil
    }

    private static func convertStructuredToolOutputDictionary(
        _ output: [String: JSONValue]
    ) -> [String: JSONValue]? {
        guard case .string(let outputType)? = output["type"] else {
            return nil
        }

        switch outputType {
        case "text":
            guard case .string(let text)? = output["text"] else {
                return nil
            }
            return [
                "type": .string("input_text"),
                "text": .string(text),
            ]
        case "image":
            let imageURL = output["image_url"]?.stringValue
            let fileID = output["file_id"]?.stringValue
            guard imageURL != nil || fileID != nil else {
                return nil
            }

            var result: [String: JSONValue] = ["type": .string("input_image")]
            if let imageURL {
                result["image_url"] = .string(imageURL)
            }
            if let fileID {
                result["file_id"] = .string(fileID)
            }
            if let detail = output["detail"]?.stringValue {
                result["detail"] = .string(detail)
            }
            return result
        case "file":
            let fileData = output["file_data"]?.stringValue
            let fileURL = output["file_url"]?.stringValue
            let fileID = output["file_id"]?.stringValue
            guard fileData != nil || fileURL != nil || fileID != nil else {
                return nil
            }

            var result: [String: JSONValue] = ["type": .string("input_file")]
            if let fileData {
                result["file_data"] = .string(fileData)
            }
            if let fileURL {
                result["file_url"] = .string(fileURL)
            }
            if let fileID {
                result["file_id"] = .string(fileID)
            }
            if let filename = output["filename"]?.stringValue {
                result["filename"] = .string(filename)
            }
            return result
        default:
            return nil
        }
    }
}
