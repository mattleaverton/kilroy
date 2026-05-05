import Foundation

// MARK: - Wait Human Handler

public final class WaitHumanHandler: NodeHandler, Sendable {
    public let handlerType: HandlerType = .waitHuman
    private let interviewer: Interviewer

    public init(interviewer: Interviewer) {
        self.interviewer = interviewer
    }

    public func execute(
        node: Node,
        context: PipelineContext,
        graph: Graph,
        logsRoot: URL
    ) async throws -> Outcome {
        let outgoing = graph.outgoingEdges(from: node.id)
        if outgoing.isEmpty {
            return .fail(reason: "No outgoing edges for human gate \(node.id)")
        }

        let choices = outgoing.map { edge -> (edge: Edge, option: InterviewOption) in
            let label = edge.label.isEmpty ? edge.to : edge.label
            let key = parseAcceleratorKey(from: label)
            return (edge, InterviewOption(key: key, label: label))
        }
        let options = choices.map(\.option)

        let questionText = node.prompt.isEmpty ? node.label : node.prompt

        // Determine timeout
        var timeoutSeconds: Double? = nil
        if let duration = node.timeout {
            let ns = duration.components.seconds * 1_000_000_000 + Int64(duration.components.attoseconds / 1_000_000_000)
            timeoutSeconds = Double(ns) / 1_000_000_000.0
        }

        let defaultChoice = resolveDefaultChoice(node: node, choices: choices)
        let defaultAnswer = defaultChoice.map { choice in
            InterviewAnswer.option(choice.option)
        }

        let questionType: QuestionType = options.count > 0 ? .singleSelect : .freeText

        let question = InterviewQuestion(
            text: questionText,
            type: questionType,
            options: options,
            defaultAnswer: defaultAnswer,
            timeoutSeconds: timeoutSeconds,
            stage: node.id
        )

        let answer = await interviewer.ask(question)

        // Handle timeout
        if answer.answerValue == AnswerValue.timeout {
            if let defaultChoice {
                let label = defaultChoice.edge.label.isEmpty ? defaultChoice.edge.to : defaultChoice.edge.label
                return Outcome(
                    status: .success,
                    preferredLabel: label,
                    suggestedNextIds: [defaultChoice.edge.to],
                    contextUpdates: [
                        "human.gate.selected": defaultChoice.option.key,
                        "human.gate.label": label,
                        "human_input": label,
                    ],
                    notes: "Used human.default_choice due to timeout"
                )
            }
            return Outcome(status: .retry, failureReason: "human gate timeout, no default choice configured")
        }

        if answer.answerValue == AnswerValue.skipped {
            return Outcome(status: .fail, failureReason: "human skipped interaction")
        }

        // Match selected option to a choice; fallback to the first choice if unmatched.
        let selectedChoice: (edge: Edge, option: InterviewOption)
        if let selected = answer.selectedOption,
           let matched = choices.first(where: { normalizeLabel($0.option.label) == normalizeLabel(selected.label) })
        {
            selectedChoice = matched
        } else if let matched = choices.first(where: { normalizeLabel($0.option.label) == normalizeLabel(answer.value) || normalizeLabel($0.edge.to) == normalizeLabel(answer.value) }) {
            selectedChoice = matched
        } else if let first = choices.first {
            selectedChoice = first
        } else {
            return .fail(reason: "No selectable options for human gate \(node.id)")
        }

        let selectedLabel = selectedChoice.edge.label.isEmpty ? selectedChoice.edge.to : selectedChoice.edge.label

        return Outcome(
            status: .success,
            preferredLabel: selectedLabel,
            suggestedNextIds: [selectedChoice.edge.to],
            contextUpdates: [
                "human.gate.selected": selectedChoice.option.key,
                "human.gate.label": selectedLabel,
                "human_input": answer.value,
            ],
            notes: "Human selected: \(selectedLabel)"
        )
    }

    private func resolveDefaultChoice(
        node: Node,
        choices: [(edge: Edge, option: InterviewOption)]
    ) -> (edge: Edge, option: InterviewOption)? {
        guard let raw = node.rawAttributes["human.default_choice"]?.stringValue
            .trimmingCharacters(in: .whitespacesAndNewlines),
            !raw.isEmpty
        else {
            return nil
        }

        let normalizedRaw = normalizeLabel(raw)
        return choices.first { choice in
            let label = choice.edge.label.isEmpty ? choice.edge.to : choice.edge.label
            return normalizeLabel(choice.edge.to) == normalizedRaw
                || normalizeLabel(label) == normalizedRaw
                || normalizeLabel(choice.option.key) == normalizedRaw
        }
    }

    private func normalizeLabel(_ text: String) -> String {
        text.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    }

    /// Parse accelerator key from edge labels.
    /// Supports formats: `[K] Label`, `K) Label`, `K - Label`, or uses first character.
    private func parseAcceleratorKey(from label: String) -> String {
        let trimmed = label.trimmingCharacters(in: .whitespaces)

        // [K] Label
        if trimmed.hasPrefix("["), let closeBracket = trimmed.firstIndex(of: "]") {
            let start = trimmed.index(after: trimmed.startIndex)
            let key = String(trimmed[start..<closeBracket])
            if !key.isEmpty { return key }
        }

        // K) Label
        if trimmed.count >= 2 {
            let secondChar = trimmed[trimmed.index(after: trimmed.startIndex)]
            if secondChar == ")" {
                return String(trimmed.first!)
            }
        }

        // K - Label
        if trimmed.count >= 3 {
            let idx1 = trimmed.index(trimmed.startIndex, offsetBy: 1)
            let idx2 = trimmed.index(trimmed.startIndex, offsetBy: 2)
            if trimmed[idx1] == " " && trimmed[idx2] == "-" {
                return String(trimmed.first!)
            }
        }

        // Default: first character
        if let first = trimmed.first {
            return String(first)
        }
        return ""
    }
}
