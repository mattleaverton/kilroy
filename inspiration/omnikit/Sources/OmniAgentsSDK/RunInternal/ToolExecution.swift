import Foundation
import OmniAICore

private struct AnySendableBox: @unchecked Sendable {
    let value: Any
}

enum ToolRuntime {
    static func isToolEnabled<TContext>(_ tool: Tool, runContext: RunContextWrapper<TContext>, agent: AnyAgent) async throws -> Bool {
        switch tool {
        case .function(let functionTool):
            return try await functionTool.isEnabled.evaluate(context: runContext, agent: agent)
        default:
            return true
        }
    }

    static func execute<TContext>(
        tool: Tool,
        call: TResponseOutputItem,
        runContext: RunContextWrapper<TContext>,
        agent: AnyAgent,
        runConfig: RunConfig?
    ) async throws -> FunctionToolResult {
        let callID = call["call_id"]?.stringValue ?? call["id"]?.stringValue ?? UUID().uuidString
        let argumentsObject: [String: JSONValue]
        let rawArguments: String
        if let object = call["arguments"]?.objectValue {
            argumentsObject = object
            rawArguments = ItemHelpers.stringifyJSON(.object(object))
        } else if let string = call["arguments"]?.stringValue {
            rawArguments = string
            if let data = string.data(using: .utf8), let value = try? JSONValue.parse(data), case .object(let object) = value {
                argumentsObject = object
            } else {
                argumentsObject = [:]
            }
        } else if let action = call["action"]?.objectValue {
            argumentsObject = action
            rawArguments = ItemHelpers.stringifyJSON(.object(action))
        } else if let operation = call["operation"]?.objectValue {
            argumentsObject = ["operations": .array([.object(operation)])]
            rawArguments = ItemHelpers.stringifyJSON(.object(argumentsObject))
        } else {
            argumentsObject = [:]
            rawArguments = "{}"
        }

        let erasedContext = ToolContext<Any>(
            context: runContext.context as Any,
            usage: runContext.usage,
            toolName: ToolPlanningRuntime.toolName(for: call),
            toolCallID: callID,
            toolArguments: rawArguments,
            toolCall: buildToolCall(from: call, fallbackID: callID),
            agent: agent,
            runConfig: runConfig,
            turnInput: runContext.turnInput,
            toolInput: runContext.toolInput
        )
        erasedContext.rebuildApprovals(from: runContext.serializedApprovals())
        erasedContext.toolInput = runContext.toolInput

        switch tool {
        case .function(let functionTool):
            for guardrail in functionTool.toolInputGuardrails ?? [] {
                let result = try await guardrail.run(.init(context: erasedContext, agent: agent))
                switch result.behavior {
                case .allow:
                    break
                case .rejectContent(let message):
                    return FunctionToolResult(tool: functionTool, output: message)
                case .raiseException:
                    throw ToolInputGuardrailTripwireTriggered(guardrail: guardrail, output: result, guardrailName: guardrail.getName())
                }
            }

            let output = try await withTimeoutAny(seconds: functionTool.timeoutSeconds) {
                try await functionTool.onInvokeTool(erasedContext, rawArguments)
            }

            for guardrail in functionTool.toolOutputGuardrails ?? [] {
                let result = try await guardrail.run(.init(context: erasedContext, agent: agent, output: output))
                switch result.behavior {
                case .allow:
                    break
                case .rejectContent(let message):
                    return FunctionToolResult(tool: functionTool, output: message)
                case .raiseException:
                    throw ToolOutputGuardrailTripwireTriggered(guardrail: guardrail, output: result, guardrailName: guardrail.getName())
                }
            }

            return FunctionToolResult(tool: functionTool, output: output)

        case .computer(let computerTool):
            let acknowledgedSafetyChecks = try await acknowledgeComputerSafetyChecks(
                tool: computerTool,
                call: call,
                runContext: RunContextWrapper<Any>(context: runContext.context as Any, usage: runContext.usage, turnInput: runContext.turnInput),
                agent: agent
            )
            let computer = try await resolveComputer(tool: computerTool, runContext: RunContextWrapper<Any>(context: runContext.context as Any, usage: runContext.usage, turnInput: runContext.turnInput))
            let screenshot = try await executeComputerActionAndCapture(computer: computer, call: call)
            let imageURL = screenshot.isEmpty ? "" : "data:image/png;base64,\(screenshot)"
            let rawItem: TResponseInputItem = [
                "type": .string("computer_call_output"),
                "call_id": .string(callID),
                "output": .object([
                    "type": .string("computer_screenshot"),
                    "image_url": .string(imageURL),
                ]),
                "acknowledged_safety_checks": .array(acknowledgedSafetyChecks.map { .object($0) }),
            ]
            let wrapped = FunctionTool(
                name: computerTool.name,
                description: "Execute computer actions.",
                paramsJSONSchema: Tool.computer(computerTool).inputSchema,
                onInvokeTool: { _, _ in imageURL }
            )
            return FunctionToolResult(tool: wrapped, output: imageURL, runItem: ToolCallOutputItem(agent: agent, rawItem: rawItem, output: imageURL))

        case .shell(let shellTool):
            guard let executor = shellTool.executor else {
                throw UserError(message: "Shell tool has no executor")
            }
            let request = ShellCommandRequest(
                contextWrapper: RunContextWrapper<Any>(context: runContext.context as Any, usage: runContext.usage, turnInput: runContext.turnInput),
                data: .init(
                    callID: callID,
                    action: .init(
                        commands: argumentsObject["commands"]?.arrayValue?.compactMap(\.stringValue)
                            ?? argumentsObject["command"]?.arrayValue?.compactMap(\.stringValue)
                            ?? [],
                        // tolerate provider payloads that use singular `command` arrays.
                        timeoutMS: argumentsObject["timeout_ms"]?.doubleValue.map(Int.init),
                        maxOutputLength: argumentsObject["max_output_length"]?.doubleValue.map(Int.init)
                    ),
                    status: nil,
                    raw: .object(call)
                )
            )
            let result = try await executor(request)
            let shellOutputEntries = result.output.map { entry -> JSONValue in
                .object([
                    "stdout": .string(entry.stdout),
                    "stderr": .string(entry.stderr),
                    "outcome": .object([
                        "type": .string(entry.outcome.type),
                        "exit_code": entry.outcome.exitCode.map { .number(Double($0)) } ?? .null,
                    ]),
                    "command": entry.command.map(JSONValue.string) ?? .null,
                    "provider_data": entry.providerData.map(JSONValue.object) ?? .null,
                ])
            }
            let structuredOutput = shellOutputEntries
            let rawItem: TResponseInputItem = [
                "type": .string("shell_call_output"),
                "call_id": .string(callID),
                "output": .array(structuredOutput),
                "status": .string("completed"),
            ]
            let wrapped = FunctionTool(
                name: shellTool.name,
                description: "Run shell commands.",
                paramsJSONSchema: Tool.shell(shellTool).inputSchema,
                onInvokeTool: { _, _ in result }
            )
            let outputText = result.output.map { [$0.stdout, $0.stderr].filter { !$0.isEmpty }.joined(separator: "\n") }.filter { !$0.isEmpty }.joined(separator: "\n")
            return FunctionToolResult(tool: wrapped, output: outputText, runItem: ToolCallOutputItem(agent: agent, rawItem: rawItem, output: outputText))

        case .localShell(let localShellTool):
            let request = LocalShellCommandRequest(
                contextWrapper: RunContextWrapper<Any>(context: runContext.context as Any, usage: runContext.usage, turnInput: runContext.turnInput),
                data: .init(
                    callID: callID,
                    action: .init(
                        commands: argumentsObject["commands"]?.arrayValue?.compactMap(\.stringValue)
                            ?? argumentsObject["command"]?.arrayValue?.compactMap(\.stringValue)
                            ?? [],
                        timeoutMS: argumentsObject["timeout_ms"]?.doubleValue.map(Int.init),
                        maxOutputLength: argumentsObject["max_output_length"]?.doubleValue.map(Int.init)
                    ),
                    status: nil,
                    raw: .object(call)
                )
            )
            let result = try await localShellTool.executor(request)
            let outputText = result.output.map { [$0.stdout, $0.stderr].filter { !$0.isEmpty }.joined(separator: "\n") }.filter { !$0.isEmpty }.joined(separator: "\n")
            let rawItem: TResponseInputItem = [
                "type": .string("local_shell_call_output"),
                "call_id": .string(callID),
                "output": .string(outputText),
            ]
            let wrapped = FunctionTool(
                name: localShellTool.name,
                description: "Run local shell commands.",
                paramsJSONSchema: Tool.localShell(localShellTool).inputSchema,
                onInvokeTool: { _, _ in result }
            )
            return FunctionToolResult(tool: wrapped, output: outputText, runItem: ToolCallOutputItem(agent: agent, rawItem: rawItem, output: outputText))

        case .applyPatch(let applyPatchTool):
            let operations = argumentsObject["operations"]?.arrayValue?.compactMap { value -> ApplyPatchOperation? in
                guard case .object(let object) = value else { return nil }
                guard let type = object["type"]?.stringValue.flatMap(ApplyPatchOperationType.init(rawValue:)),
                      let path = object["path"]?.stringValue
                else { return nil }
                return ApplyPatchOperation(type: type, path: path, diff: object["diff"]?.stringValue)
            } ?? []
            var outputs: [String] = []
            for operation in operations {
                switch operation.type {
                case .createFile:
                    if let result = try await applyPatchTool.editor.createFile(operation), let output = result.output { outputs.append(output) }
                case .updateFile:
                    if let result = try await applyPatchTool.editor.updateFile(operation), let output = result.output { outputs.append(output) }
                case .deleteFile:
                    if let result = try await applyPatchTool.editor.deleteFile(operation), let output = result.output { outputs.append(output) }
                }
            }
            let outputText = outputs.joined(separator: "\n")
            let rawItem: TResponseInputItem = [
                "type": .string("apply_patch_call_output"),
                "call_id": .string(callID),
                "status": .string("completed"),
                "output": .string(outputText),
            ]
            let wrapped = FunctionTool(
                name: applyPatchTool.name,
                description: "Apply filesystem patches.",
                paramsJSONSchema: Tool.applyPatch(applyPatchTool).inputSchema,
                onInvokeTool: { _, _ in outputText }
            )
            return FunctionToolResult(tool: wrapped, output: outputText, runItem: ToolCallOutputItem(agent: agent, rawItem: rawItem, output: outputText))

        default:
            throw UserError(message: "Tool \(tool.name) cannot be executed locally by this runtime")
        }
    }
}

private func acknowledgeComputerSafetyChecks(
    tool: ComputerTool,
    call: TResponseOutputItem,
    runContext: RunContextWrapper<Any>,
    agent: AnyAgent
) async throws -> [[String: JSONValue]] {
    guard let callback = tool.onSafetyCheck,
          let checks = call["pending_safety_checks"]?.arrayValue,
          !checks.isEmpty
    else {
        return []
    }

    var acknowledged: [[String: JSONValue]] = []
    for check in checks {
        guard case .object(let checkObject) = check else { continue }
        let ack = try await callback(.init(contextWrapper: runContext, agent: agent, toolCall: call, safetyCheck: check))
        if !ack {
            throw UserError(message: "Computer tool safety check was not acknowledged")
        }
        var entry: [String: JSONValue] = [:]
        if let id = checkObject["id"] { entry["id"] = id }
        if let code = checkObject["code"] { entry["code"] = code }
        if let message = checkObject["message"] { entry["message"] = message }
        acknowledged.append(entry)
    }
    return acknowledged
}

private func executeComputerActionAndCapture(computer: any AsyncComputer, call: TResponseOutputItem) async throws -> String {
    guard let action = call["action"]?.objectValue,
          let type = action["type"]?.stringValue
    else {
        throw ModelBehaviorError(message: "Computer call missing action")
    }

    switch type {
    case "click":
        try await computer.click(
            x: Int(action["x"]?.doubleValue ?? 0),
            y: Int(action["y"]?.doubleValue ?? 0),
            button: Button(rawValue: action["button"]?.stringValue ?? "left") ?? .left
        )
    case "double_click":
        try await computer.doubleClick(
            x: Int(action["x"]?.doubleValue ?? 0),
            y: Int(action["y"]?.doubleValue ?? 0)
        )
    case "drag":
        if let path = action["path"]?.arrayValue {
            let points = path.compactMap { value -> (Int, Int)? in
                guard case .object(let point) = value else { return nil }
                return (Int(point["x"]?.doubleValue ?? 0), Int(point["y"]?.doubleValue ?? 0))
            }
            try await computer.drag(points)
        } else {
            let start = (Int(action["start_x"]?.doubleValue ?? 0), Int(action["start_y"]?.doubleValue ?? 0))
            let end = (Int(action["end_x"]?.doubleValue ?? 0), Int(action["end_y"]?.doubleValue ?? 0))
            try await computer.drag([start, end])
        }
    case "keypress":
        try await computer.keypress(action["keys"]?.arrayValue?.compactMap(\.stringValue) ?? [])
    case "move":
        try await computer.move(
            x: Int(action["x"]?.doubleValue ?? 0),
            y: Int(action["y"]?.doubleValue ?? 0)
        )
    case "screenshot":
        _ = try await computer.screenshot()
    case "scroll":
        try await computer.scroll(
            x: Int(action["x"]?.doubleValue ?? 0),
            y: Int(action["y"]?.doubleValue ?? 0),
            scrollX: Int(action["scroll_x"]?.doubleValue ?? 0),
            scrollY: Int(action["scroll_y"]?.doubleValue ?? 0)
        )
    case "type":
        try await computer.type(action["text"]?.stringValue ?? "")
    case "wait":
        try await computer.wait()
    default:
        break
    }

    return try await computer.screenshot()
}

private func buildToolCall(from item: TResponseOutputItem, fallbackID: String) -> ToolCall {
    let arguments: [String: JSONValue]
    let rawArguments: String?
    if let object = item["arguments"]?.objectValue {
        arguments = object
        rawArguments = ItemHelpers.stringifyJSON(.object(object))
    } else if let string = item["arguments"]?.stringValue {
        rawArguments = string
        if let data = string.data(using: .utf8), let value = try? JSONValue.parse(data), case .object(let object) = value {
            arguments = object
        } else {
            arguments = [:]
        }
    } else {
        arguments = [:]
        rawArguments = nil
    }

    return ToolCall(
        id: item["call_id"]?.stringValue ?? item["id"]?.stringValue ?? fallbackID,
        name: item["name"]?.stringValue ?? item["type"]?.stringValue ?? "tool",
        arguments: arguments,
        rawArguments: rawArguments,
        thoughtSignature: item["thought_signature"]?.stringValue,
        providerItemId: item["id"]?.stringValue
    )
}

private final class _UncheckedValueBox<Value>: @unchecked Sendable {
    let value: Value
    init(_ value: Value) { self.value = value }
}

private func withTimeoutAny<T>(seconds: Double?, operation: @escaping @Sendable () async throws -> T) async throws -> T {
    guard let seconds else {
        return try await operation()
    }
    return try await withThrowingTaskGroup(of: _UncheckedValueBox<T>.self) { group in
        group.addTask {
            _UncheckedValueBox(try await operation())
        }
        group.addTask {
            try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
            throw ToolTimeoutError(toolName: "tool", timeoutSeconds: seconds)
        }
        guard let result = try await group.next() else {
            group.cancelAll()
            throw ToolTimeoutError(toolName: "tool", timeoutSeconds: seconds)
        }
        group.cancelAll()
        return result.value
    }
}
