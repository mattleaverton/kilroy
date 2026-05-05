import Foundation

public enum HandoffFilters {
    public static func removeAllTools(_ handoffInputData: HandoffInputData) -> HandoffInputData {
        let filteredHistory: RunInput = {
            switch handoffInputData.inputHistory {
            case .string:
                return handoffInputData.inputHistory
            case .inputList(let items):
                let filtered = items.filter { item in
                    guard let type = item["type"]?.stringValue else { return true }
                    return ![
                        "function_call",
                        "function_call_output",
                        "computer_call",
                        "computer_call_output",
                        "file_search_call",
                        "web_search_call",
                    ].contains(type)
                }
                return .inputList(filtered)
            }
        }()

        let filteredPre = handoffInputData.preHandoffItems.filter { item in
            !(item is HandoffCallItem || item is HandoffOutputItem || item is ToolCallItem || item is ToolCallOutputItem || item is ReasoningItem)
        }
        let filteredNew = handoffInputData.newItems.filter { item in
            !(item is HandoffCallItem || item is HandoffOutputItem || item is ToolCallItem || item is ToolCallOutputItem || item is ReasoningItem)
        }

        return HandoffInputData(
            inputHistory: filteredHistory,
            preHandoffItems: filteredPre,
            newItems: filteredNew,
            runContext: handoffInputData.runContext,
            inputItems: handoffInputData.inputItems
        )
    }
}
