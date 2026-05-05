import Foundation
import Testing
import OmniAgentsSDK
import OmniAICore

struct RunContextAndItemsParityTests {
    @Test
    func approvals_round_trip_and_approve_wins_when_both_global_flags_present() {
        let context = RunContextWrapper(context: "ctx")
        context.approveTool(toolName: "shell", callID: "call-1")
        context.rejectTool(toolName: "shell", callID: "call-2")

        let encoded = context.serializedApprovals()
        let restored = RunContextWrapper(context: "ctx")
        restored.rebuildApprovals(from: encoded)

        #expect(restored.getApprovalStatus(toolName: "shell", callID: "call-1") == true)
        #expect(restored.getApprovalStatus(toolName: "shell", callID: "call-2") == false)

        restored.rebuildApprovals(from: [
            "shell": [
                "approved": .bool(true),
                "rejected": .bool(true),
            ]
        ])
        #expect(restored.isToolApproved(toolName: "shell", callID: "any-call") == true)
    }

    @Test
    func mcp_approval_call_id_is_extracted_from_provider_data() {
        let approvalItem: [String: Any] = [
            "raw_item": [
                "provider_data": [
                    "type": "mcp_approval_request",
                    "id": "mcp-123",
                ]
            ]
        ]

        #expect(RunContextWrapper<String>.resolveCallID(from: approvalItem) == "mcp-123")
        #expect(RunContextWrapper<String>.resolveToolName(from: ["raw_item": ["name": "shell"]]) == "shell")
    }

    @Test
    func run_item_base_coerces_jsonvalue_and_foundation_dictionaries() throws {
        let fromJSONValue = RunItemBase<JSONValue>(
            agent: nil,
            rawItem: .object(["role": .string("user"), "content": .string("hello")])
        )
        let fromFoundation = RunItemBase<[String: Any]>(
            agent: nil,
            rawItem: ["role": "user", "content": "hello", "count": 1]
        )

        #expect(try fromJSONValue.toInputItem() == [
            "role": .string("user"),
            "content": .string("hello"),
        ])
        #expect(try fromFoundation.toInputItem() == [
            "role": .string("user"),
            "content": .string("hello"),
            "count": .number(1),
        ])
    }

    @Test
    func shell_call_output_strips_hosted_fields_and_normalizes_nested_outcomes() throws {
        let item = ToolCallOutputItem(
            agent: nil,
            rawItem: [
                "type": .string("shell_call_output"),
                "call_id": .string("call-shell"),
                "status": .string("completed"),
                "shell_output": .string("large debug payload"),
                "provider_data": .object(["provider": .string("openai")]),
                "output": .array([
                    .object([
                        "stdout": .string("hello"),
                        "outcome": .object([
                            "type": .string("exit"),
                            "exit_code": .number(0),
                        ]),
                    ]),
                    .string("unchanged"),
                ]),
            ],
            output: "hello"
        )

        let converted = try item.toInputItem()
        #expect(converted["status"] == nil)
        #expect(converted["shell_output"] == nil)
        #expect(converted["provider_data"] == nil)
        #expect(converted["output"] == .array([
            .object([
                "stdout": .string("hello"),
                "outcome": .object([
                    "type": .string("exit"),
                    "exit_code": .number(0),
                ]),
            ]),
            .string("unchanged"),
        ]))
    }

    @Test
    func item_helpers_extract_text_and_normalize_tool_outputs() throws {
        let message: TResponseOutputItem = [
            "type": .string("message"),
            "content": .array([
                .object(["type": .string("output_refusal"), "refusal": .string("no")]),
                .object(["type": .string("output_text"), "text": .string("done")]),
            ]),
        ]
        let toolCall: TResponseOutputItem = [
            "type": .string("function_call"),
            "call_id": .string("tool-1"),
        ]

        #expect(try ItemHelpers.extractLastContent(message: message) == "done")
        #expect(ItemHelpers.extractLastText(message: message) == "done")

        let refusalMessage: TResponseOutputItem = [
            "type": .string("message"),
            "content": .array([
                .object(["type": .string("output_refusal"), "refusal": .string("cannot comply")]),
            ]),
        ]
        #expect(try ItemHelpers.extractLastContent(message: refusalMessage) == "cannot comply")
        #expect(ItemHelpers.extractLastText(message: refusalMessage) == nil)

        let textOutput = ItemHelpers.toolCallOutputItem(toolCall: toolCall, output: ToolOutputText(text: "hello"))
        #expect(textOutput == [
            "type": .string("function_call_output"),
            "call_id": .string("tool-1"),
            "output": .array([
                .object([
                    "type": .string("input_text"),
                    "text": .string("hello"),
                ])
            ]),
        ])

        let imageOutput = ItemHelpers.toolCallOutputItem(toolCall: toolCall, output: [
            "type": "image",
            "image_url": "https://example.com/cat.png",
            "detail": "high",
        ])
        #expect(imageOutput["output"] == .array([
            .object([
                "type": .string("input_image"),
                "image_url": .string("https://example.com/cat.png"),
                "detail": .string("high"),
            ])
        ]))

        let fileOutput = ItemHelpers.toolCallOutputItem(toolCall: toolCall, output: ToolOutputFileContent(
            fileID: "file-123",
            filename: "report.txt"
        ))
        #expect(fileOutput["output"] == .array([
            .object([
                "type": .string("input_file"),
                "file_id": .string("file-123"),
                "filename": .string("report.txt"),
            ])
        ]))
    }
}
