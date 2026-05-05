import Foundation
import OmniAICore

public struct Prompt: Sendable, Equatable, Codable {
    public var id: String
    public var version: String?
    public var variables: [String: JSONValue]?

    public init(id: String, version: String? = nil, variables: [String: JSONValue]? = nil) {
        self.id = id
        self.version = version
        self.variables = variables
    }
}

public typealias DynamicPromptFunction<TContext> = @Sendable (GenerateDynamicPromptData<TContext>) async throws -> Prompt

public struct GenerateDynamicPromptData<TContext>: Sendable {
    public let context: RunContextWrapper<TContext>
    public let agent: Agent<TContext>

    public init(context: RunContextWrapper<TContext>, agent: Agent<TContext>) {
        self.context = context
        self.agent = agent
    }
}

public enum PromptUtil {
    public static func render(prompt: Prompt?, baseInstructions: String?) -> String? {
        guard let prompt else { return baseInstructions }
        var sections: [String] = []
        if let baseInstructions, !baseInstructions.isEmpty {
            sections.append(baseInstructions)
        }
        sections.append("[Prompt ID: \(prompt.id)]")
        if let version = prompt.version, !version.isEmpty {
            sections.append("[Prompt Version: \(version)]")
        }
        if let variables = prompt.variables, !variables.isEmpty {
            let renderedVariables: String = {
                if let data = try? JSONValue.object(variables).data(prettyPrinted: true),
                   let text = String(data: data, encoding: .utf8)
                {
                    return text
                }
                return String(describing: variables)
            }()
            sections.append("[Prompt Variables]\n\(renderedVariables)")
        }
        return sections.joined(separator: "\n\n")
    }
}
