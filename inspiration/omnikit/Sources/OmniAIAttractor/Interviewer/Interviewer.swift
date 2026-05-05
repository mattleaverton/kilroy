import Foundation

// MARK: - Question Type

public enum QuestionType: String, Sendable {
    case singleSelect = "single_select"
    case multiSelect = "multi_select"
    case freeText = "free_text"
    case confirm = "confirm"
    // Aliases for compatibility
    case yesNo = "yes_no"
    case multipleChoice = "multiple_choice"
    case freeform = "freeform"
    case confirmation = "confirmation"
}

// MARK: - Option

public struct InterviewOption: Sendable {
    public var key: String
    public var label: String

    public init(key: String, label: String) {
        self.key = key
        self.label = label
    }
}

// MARK: - Question

public struct InterviewQuestion: Sendable {
    public var text: String
    public var type: QuestionType
    public var options: [InterviewOption]
    public var defaultAnswer: InterviewAnswer?
    public var timeoutSeconds: Double?
    public var stage: String
    public var metadata: [String: String]

    public init(
        text: String,
        type: QuestionType = .multipleChoice,
        options: [InterviewOption] = [],
        defaultAnswer: InterviewAnswer? = nil,
        timeoutSeconds: Double? = nil,
        stage: String = "",
        metadata: [String: String] = [:]
    ) {
        self.text = text
        self.type = type
        self.options = options
        self.defaultAnswer = defaultAnswer
        self.timeoutSeconds = timeoutSeconds
        self.stage = stage
        self.metadata = metadata
    }
}

// MARK: - Answer Value

public enum AnswerValue: String, Sendable {
    case yes
    case no
    case skipped
    case timeout
}

// MARK: - Answer

public struct InterviewAnswer: Sendable {
    public var value: String
    public var selectedOption: InterviewOption?
    public var answerValue: AnswerValue?

    public init(value: String, selectedOption: InterviewOption? = nil, answerValue: AnswerValue? = nil) {
        self.value = value
        self.selectedOption = selectedOption
        self.answerValue = answerValue
    }

    public static func yes() -> InterviewAnswer {
        InterviewAnswer(value: "yes", answerValue: .yes)
    }

    public static func no() -> InterviewAnswer {
        InterviewAnswer(value: "no", answerValue: .no)
    }

    public static func skipped() -> InterviewAnswer {
        InterviewAnswer(value: "", answerValue: .skipped)
    }

    public static func timedOut() -> InterviewAnswer {
        InterviewAnswer(value: "", answerValue: .timeout)
    }

    public static func option(_ opt: InterviewOption) -> InterviewAnswer {
        InterviewAnswer(value: opt.label, selectedOption: opt)
    }

    public static func freeText(_ text: String) -> InterviewAnswer {
        InterviewAnswer(value: text)
    }
}

// MARK: - Interviewer Protocol

public protocol Interviewer: Sendable {
    func ask(_ question: InterviewQuestion) async -> InterviewAnswer
    func askMultiple(_ questions: [InterviewQuestion]) async -> [InterviewAnswer]
    func inform(_ message: String, stage: String) async
}

extension Interviewer {
    public func askMultiple(_ questions: [InterviewQuestion]) async -> [InterviewAnswer] {
        var answers: [InterviewAnswer] = []
        for q in questions {
            answers.append(await ask(q))
        }
        return answers
    }

    public func inform(_ message: String, stage: String) async {
        // Default no-op
    }
}

// MARK: - AutoApproveInterviewer

public struct AutoApproveInterviewer: Interviewer {
    public init() {}

    public func ask(_ question: InterviewQuestion) async -> InterviewAnswer {
        if let first = question.options.first {
            return .option(first)
        }
        if question.type == .yesNo || question.type == .confirmation || question.type == .confirm {
            return .yes()
        }
        return .skipped()
    }
}

// MARK: - ConsoleInterviewer

public struct ConsoleInterviewer: Interviewer {
    public init() {}

    public func ask(_ question: InterviewQuestion) async -> InterviewAnswer {
        print("\n--- \(question.stage.isEmpty ? "Question" : question.stage) ---")
        print(question.text)

        if !question.options.isEmpty {
            for (i, opt) in question.options.enumerated() {
                print("  [\(opt.key.isEmpty ? String(i + 1) : opt.key)] \(opt.label)")
            }
        }

        if let defaultAns = question.defaultAnswer {
            print("  (default: \(defaultAns.value))")
        }

        print("> ", terminator: "")
        guard let line = readLine()?.trimmingCharacters(in: .whitespacesAndNewlines) else {
            return question.defaultAnswer ?? .skipped()
        }

        if line.isEmpty {
            return question.defaultAnswer ?? .skipped()
        }

        // Try to match option
        if let matched = question.options.first(where: { $0.key.lowercased() == line.lowercased() || $0.label.lowercased() == line.lowercased() }) {
            return .option(matched)
        }

        // Try index-based selection
        if let idx = Int(line), idx >= 1, idx <= question.options.count {
            return .option(question.options[idx - 1])
        }

        return .freeText(line)
    }

    public func inform(_ message: String, stage: String) async {
        print("[\(stage)] \(message)")
    }
}

// MARK: - CallbackInterviewer

public struct CallbackInterviewer: Interviewer {
    private let callback: @Sendable (InterviewQuestion) async -> InterviewAnswer

    public init(_ callback: @escaping @Sendable (InterviewQuestion) async -> InterviewAnswer) {
        self.callback = callback
    }

    public func ask(_ question: InterviewQuestion) async -> InterviewAnswer {
        await callback(question)
    }
}

// MARK: - QueueInterviewer

public actor QueueInterviewer: Interviewer {
    private var queue: [InterviewAnswer]
    public private(set) var askedQuestions: [InterviewQuestion] = []

    public init(answers: [InterviewAnswer]) {
        self.queue = answers
    }

    public func ask(_ question: InterviewQuestion) async -> InterviewAnswer {
        askedQuestions.append(question)
        if queue.isEmpty {
            return .skipped()
        }
        return queue.removeFirst()
    }
}

// MARK: - RecordingInterviewer

public actor RecordingInterviewer: Interviewer {
    private let inner: Interviewer
    public private(set) var recordings: [(InterviewQuestion, InterviewAnswer)] = []

    public init(wrapping inner: Interviewer) {
        self.inner = inner
    }

    public func ask(_ question: InterviewQuestion) async -> InterviewAnswer {
        let answer = await inner.ask(question)
        recordings.append((question, answer))
        return answer
    }

    public func inform(_ message: String, stage: String) async {
        await inner.inform(message, stage: stage)
    }
}

