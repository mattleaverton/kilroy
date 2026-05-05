import Foundation

public let OPENAI_DEFAULT_MODEL_ENV_VARIABLE_NAME = "OPENAI_DEFAULT_MODEL"

private let gpt5DefaultModelSettings = ModelSettings(
    reasoning: Reasoning(effort: "low", summary: nil),
    verbosity: .low
)

private let gpt5NoneDefaultModelSettings = ModelSettings(
    reasoning: Reasoning(effort: "none", summary: nil),
    verbosity: .low
)

private let gpt5NoneEffortModels: Set<String> = ["gpt-5.1", "gpt-5.2"]

public func getDefaultModel() -> String {
    ProcessInfo.processInfo.environment[OPENAI_DEFAULT_MODEL_ENV_VARIABLE_NAME] ?? "gpt-5.3-codex"
}

public func getDefaultModelSettings(modelName: String? = nil) -> ModelSettings {
    let resolved = modelName ?? getDefaultModel()
    return isGPT5NoneEffortModel(resolved) ? gpt5NoneDefaultModelSettings : gpt5DefaultModelSettings
}

public func isGPT5Default(_ modelName: String) -> Bool {
    modelName.hasPrefix("gpt-5")
}

public func gpt5ReasoningSettingsRequired(_ modelName: String) -> Bool {
    guard isGPT5Default(modelName) else {
        return false
    }
    return !modelName.hasPrefix("gpt-5-chat")
}

public func isGPT5NoneEffortModel(_ modelName: String) -> Bool {
    gpt5NoneEffortModels.contains(modelName)
}

