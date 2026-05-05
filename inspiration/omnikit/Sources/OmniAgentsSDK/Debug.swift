import Foundation

private func debugFlagEnabled(_ flag: String, default defaultValue: Bool = false) -> Bool {
    guard let rawValue = ProcessInfo.processInfo.environment[flag] else {
        return defaultValue
    }
    let normalized = rawValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return normalized == "1" || normalized == "true"
}

private func loadDontLogModelData() -> Bool {
    debugFlagEnabled("OPENAI_AGENTS_DONT_LOG_MODEL_DATA", default: true)
}

private func loadDontLogToolData() -> Bool {
    debugFlagEnabled("OPENAI_AGENTS_DONT_LOG_TOOL_DATA", default: true)
}

public enum OmniAgentsDebug {
    public static let dontLogModelData = loadDontLogModelData()
    public static let dontLogToolData = loadDontLogToolData()
}

/// Python parity with `agents._debug.DONT_LOG_MODEL_DATA`.
public let DONT_LOG_MODEL_DATA = OmniAgentsDebug.dontLogModelData

/// Python parity with `agents._debug.DONT_LOG_TOOL_DATA`.
public let DONT_LOG_TOOL_DATA = OmniAgentsDebug.dontLogToolData