import Foundation
import OmniAICore

public func prettyPrintRunResult(_ result: Any) -> String {
    let lastAgentDescription = prettyPrintAgentDescription(reflectedProperty(named: "lastAgent", from: result))
    let finalOutput = reflectedProperty(named: "finalOutput", from: result)
    let finalOutputType = finalOutput.map { String(describing: type(of: $0)) } ?? "nil"

    return [
        "RunResult:",
        "- Last agent: \(lastAgentDescription)",
        "- Final output (\(finalOutputType)):\n\(indent(prettyFinalOutput(finalOutput), level: 2))",
        "- \(countForProperty(named: "newItems", in: result)) new item(s)",
        "- \(countForProperty(named: "rawResponses", in: result)) raw response(s)",
        "- \(countForProperty(named: "inputGuardrailResults", in: result)) input guardrail result(s)",
        "- \(countForProperty(named: "outputGuardrailResults", in: result)) output guardrail result(s)",
        "(See `RunResult` for more details)",
    ].joined(separator: "\n")
}

public func prettyPrintRunResultStreaming(_ result: Any) -> String {
    let currentAgentDescription = prettyPrintAgentDescription(reflectedProperty(named: "currentAgent", from: result))
    let finalOutput = reflectedProperty(named: "finalOutput", from: result)
    let finalOutputType = finalOutput.map { String(describing: type(of: $0)) } ?? "nil"

    return [
        "RunResultStreaming:",
        "- Current agent: \(currentAgentDescription)",
        "- Current turn: \(intProperty(named: "currentTurn", in: result) ?? 0)",
        "- Max turns: \(intProperty(named: "maxTurns", in: result) ?? 0)",
        "- Is complete: \(boolProperty(named: "isComplete", in: result) ?? false)",
        "- Final output (\(finalOutputType)):\n\(indent(prettyFinalOutput(finalOutput), level: 2))",
        "- \(countForProperty(named: "newItems", in: result)) new item(s)",
        "- \(countForProperty(named: "rawResponses", in: result)) raw response(s)",
        "- \(countForProperty(named: "inputGuardrailResults", in: result)) input guardrail result(s)",
        "- \(countForProperty(named: "outputGuardrailResults", in: result)) output guardrail result(s)",
        "(See `RunResultStreaming` for more details)",
    ].joined(separator: "\n")
}

private func indent(_ text: String, level: Int) -> String {
    let indentString = String(repeating: "  ", count: max(level, 0))
    return text
        .split(separator: "\n", omittingEmptySubsequences: false)
        .map { "\(indentString)\($0)" }
        .joined(separator: "\n")
}

private func prettyFinalOutput(_ output: Any?) -> String {
    guard let output else {
        return "None"
    }

    if let string = output as? String {
        return string
    }

    if let jsonValue = output as? JSONValue,
       let data = try? jsonValue.data(prettyPrinted: true),
       let rendered = String(data: data, encoding: .utf8)
    {
        return rendered
    }

    if let dictionary = output as? [String: Any],
       JSONSerialization.isValidJSONObject(dictionary),
       let data = try? JSONSerialization.data(withJSONObject: dictionary, options: [.prettyPrinted, .sortedKeys]),
       let rendered = String(data: data, encoding: .utf8)
    {
        return rendered
    }

    if let list = output as? [Any],
       JSONSerialization.isValidJSONObject(list),
       let data = try? JSONSerialization.data(withJSONObject: list, options: [.prettyPrinted, .sortedKeys]),
       let rendered = String(data: data, encoding: .utf8)
    {
        return rendered
    }

    return String(describing: output)
}

private func prettyPrintAgentDescription(_ agent: Any?) -> String {
    guard let agent else {
        return "none"
    }
    if let name = reflectedProperty(named: "name", from: agent) as? String {
        return "Agent(name=\"\(name)\", ...)"
    }
    return String(describing: type(of: agent))
}

private func countForProperty(named name: String, in value: Any) -> Int {
    guard let property = reflectedProperty(named: name, from: value) else {
        return 0
    }

    let mirror = Mirror(reflecting: property)
    if mirror.displayStyle == .collection || mirror.displayStyle == .set || mirror.displayStyle == .dictionary {
        return mirror.children.count
    }
    return 0
}

private func intProperty(named name: String, in value: Any) -> Int? {
    guard let property = reflectedProperty(named: name, from: value) else {
        return nil
    }
    if let intValue = property as? Int {
        return intValue
    }
    if let int32Value = property as? Int32 {
        return Int(int32Value)
    }
    if let int64Value = property as? Int64 {
        return Int(int64Value)
    }
    if let stringValue = property as? String {
        return Int(stringValue)
    }
    return nil
}

private func boolProperty(named name: String, in value: Any) -> Bool? {
    guard let property = reflectedProperty(named: name, from: value) else {
        return nil
    }
    if let boolValue = property as? Bool {
        return boolValue
    }
    if let stringValue = property as? String {
        return ["true", "1", "yes"].contains(stringValue.lowercased())
    }
    return nil
}

private func reflectedProperty(named propertyName: String, from value: Any) -> Any? {
    var currentMirror: Mirror? = Mirror(reflecting: value)
    while let mirror = currentMirror {
        for child in mirror.children where child.label == propertyName {
            return child.value
        }
        currentMirror = mirror.superclassMirror
    }
    return nil
}
