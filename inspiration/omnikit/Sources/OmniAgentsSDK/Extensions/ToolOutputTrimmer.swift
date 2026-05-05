import Foundation

public struct ToolOutputTrimmer: Sendable {
    public var recentTurns: Int
    public var maxOutputChars: Int
    public var previewChars: Int
    public var trimmableTools: Set<String>?

    public init(
        recentTurns: Int = 2,
        maxOutputChars: Int = 500,
        previewChars: Int = 200,
        trimmableTools: Set<String>? = nil
    ) {
        self.recentTurns = max(1, recentTurns)
        self.maxOutputChars = max(1, maxOutputChars)
        self.previewChars = max(0, previewChars)
        self.trimmableTools = trimmableTools
    }

    public func callAsFunction<TContext>(_ data: CallModelData<TContext>) -> ModelInputData {
        let modelData = data.modelData
        let items = modelData.input
        guard !items.isEmpty else { return modelData }

        let boundary = recentBoundary(in: items)
        guard boundary > 0 else { return modelData }

        let callIDToName = buildCallIDToName(items: items)
        let newItems: [TResponseInputItem] = items.enumerated().map { index, item in
            guard index < boundary,
                  item["type"]?.stringValue == "function_call_output"
            else {
                return item
            }

            let outputString: String = {
                if let output = item["output"] {
                    switch output {
                    case .string(let value):
                        return value
                    default:
                        if let data = try? output.data(), let string = String(data: data, encoding: .utf8) {
                            return string
                        }
                        return output.description
                    }
                }
                return ""
            }()
            guard outputString.count > maxOutputChars else { return item }
            let callID = item["call_id"]?.stringValue ?? ""
            let toolName = callIDToName[callID] ?? ""
            if let trimmableTools, !trimmableTools.contains(toolName) {
                return item
            }
            let preview = String(outputString.prefix(previewChars))
            let summary = "[Trimmed: \(toolName.isEmpty ? "unknown_tool" : toolName) output — \(outputString.count) chars → \(previewChars) char preview]\n\(preview)..."
            guard summary.count < outputString.count else { return item }
            var trimmed = item
            trimmed["output"] = .string(summary)
            return trimmed
        }

        return ModelInputData(input: newItems, instructions: modelData.instructions)
    }

    private func recentBoundary(in items: [TResponseInputItem]) -> Int {
        var userMessageCount = 0
        for index in stride(from: items.count - 1, through: 0, by: -1) {
            if items[index]["role"]?.stringValue == "user" {
                userMessageCount += 1
                if userMessageCount >= recentTurns {
                    return index
                }
            }
        }
        return 0
    }

    private func buildCallIDToName(items: [TResponseInputItem]) -> [String: String] {
        var mapping: [String: String] = [:]
        for item in items where item["type"]?.stringValue == "function_call" {
            if let callID = item["call_id"]?.stringValue,
               let name = item["name"]?.stringValue
            {
                mapping[callID] = name
            }
        }
        return mapping
    }
}
