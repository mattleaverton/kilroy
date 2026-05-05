import Foundation
#if canImport(Network)
import Network
#endif

// MARK: - Pipeline Run State

public actor PipelineRunState {
    public nonisolated let id: String
    public nonisolated let dotSource: String
    public nonisolated let eventEmitter: PipelineEventEmitter

    private var _status: PipelineRunStatus = .pending
    private var _result: PipelineResult?
    private var _error: String?
    private var _pendingQuestions: [PendingQuestion] = []
    private var questionAnswers: [String: InterviewAnswer] = [:]
    private var questionContinuations: [String: CheckedContinuation<InterviewAnswer, Never>] = [:]
    private var task: Task<Void, Never>?

    public init(id: String, dotSource: String) {
        self.id = id
        self.dotSource = dotSource
        self.eventEmitter = PipelineEventEmitter()
    }

    public enum PipelineRunStatus: String, Sendable {
        case pending, running, completed, failed, cancelled
    }

    public struct PendingQuestion: Sendable {
        public let id: String
        public let question: InterviewQuestion
        public let createdAt: Date
    }

    public var status: PipelineRunStatus { _status }
    public var result: PipelineResult? { _result }
    public var error: String? { _error }
    public var pendingQuestions: [PendingQuestion] { _pendingQuestions }

    func setStatus(_ s: PipelineRunStatus) {
        _status = s
    }

    func setResult(_ r: PipelineResult) {
        _result = r
        _status = r.status == .success ? .completed : .failed
    }

    func setError(_ e: String) {
        _error = e
        _status = .failed
    }

    func setTask(_ t: Task<Void, Never>) {
        task = t
    }

    func cancel() {
        task?.cancel()
        _status = .cancelled
        let pendingQuestionsByID = Dictionary(uniqueKeysWithValues: _pendingQuestions.map { ($0.id, $0.question) })
        let pendingContinuations = questionContinuations
        questionContinuations.removeAll()
        for (id, question) in pendingQuestionsByID where questionAnswers[id] == nil {
            questionAnswers[id] = question.defaultAnswer ?? .skipped()
        }
        _pendingQuestions.removeAll()

        for (id, continuation) in pendingContinuations {
            let answer = pendingQuestionsByID[id]?.defaultAnswer ?? .skipped()
            continuation.resume(returning: answer)
        }
    }

    func addPendingQuestion(_ q: PendingQuestion) {
        _pendingQuestions.append(q)
    }

    func removePendingQuestion(_ qid: String) {
        _pendingQuestions.removeAll { $0.id == qid }
    }

    func registerQuestionContinuation(_ qid: String, _ cont: CheckedContinuation<InterviewAnswer, Never>) {
        if let answer = questionAnswers[qid] {
            questionAnswers.removeValue(forKey: qid)
            cont.resume(returning: answer)
        } else if _status == .cancelled {
            cont.resume(returning: .skipped())
        } else {
            questionContinuations[qid] = cont
        }
    }

    func submitAnswer(_ qid: String, _ answer: InterviewAnswer) {
        if let cont = questionContinuations[qid] {
            questionContinuations.removeValue(forKey: qid)
            cont.resume(returning: answer)
        } else {
            questionAnswers[qid] = answer
        }
    }
}

// MARK: - HTTP Server Interviewer

public final class HTTPServerInterviewer: Sendable, Interviewer {
    private let runState: PipelineRunState

    public init(runState: PipelineRunState) {
        self.runState = runState
    }

    public func ask(_ question: InterviewQuestion) async -> InterviewAnswer {
        let qid = "q-\(UUID().uuidString.lowercased())"

        let pending = PipelineRunState.PendingQuestion(
            id: qid,
            question: question,
            createdAt: Date()
        )
        await runState.addPendingQuestion(pending)

        await runState.eventEmitter.emit(PipelineEvent(
            kind: .interviewStarted,
            data: ["question_id": qid, "question": question.text, "stage": question.stage]
        ))

        let answer: InterviewAnswer = await withCheckedContinuation { continuation in
            Task {
                await runState.registerQuestionContinuation(qid, continuation)
            }
        }

        await runState.removePendingQuestion(qid)

        await runState.eventEmitter.emit(PipelineEvent(
            kind: .interviewCompleted,
            data: ["question_id": qid, "answer": answer.value]
        ))

        return answer
    }
}

// MARK: - Attractor HTTP Server

public actor AttractorHTTPServer {
    private let backend: CodergenBackend
    private let port: UInt16
    private var runs: [String: PipelineRunState] = [:]
    private var listener: Any? = nil

    public init(backend: CodergenBackend, port: UInt16 = 8080) {
        self.backend = backend
        self.port = port
    }

    public func createRun(dotSource: String) -> PipelineRunState {
        let id = UUID().uuidString.lowercased().prefix(8).description
        let state = PipelineRunState(id: id, dotSource: dotSource)
        runs[id] = state
        return state
    }

    public func getRun(_ id: String) -> PipelineRunState? {
        runs[id]
    }

    public func startRun(_ state: PipelineRunState) async {
        await state.setStatus(.running)
        let backend = self.backend
        let runID = state.id
        let dotSource = state.dotSource
        let eventEmitter = state.eventEmitter

        let task = Task {
            let logsRoot = FileManager.default.temporaryDirectory
                .appendingPathComponent("attractor-server/\(runID)")
            let interviewer = HTTPServerInterviewer(runState: state)

            let config = PipelineConfig(
                logsRoot: logsRoot,
                backend: backend,
                interviewer: interviewer,
                eventEmitter: eventEmitter
            )

            let engine = PipelineEngine(config: config)
            do {
                let result = try await engine.run(dot: dotSource)
                await state.setResult(result)
            } catch {
                await state.setError(error.localizedDescription)
            }
        }
        await state.setTask(task)
    }

    public func cancelRun(_ id: String) async -> Bool {
        guard let state = runs[id] else { return false }
        await state.cancel()
        return true
    }

    public func submitAnswer(runId: String, questionId: String, answer: InterviewAnswer) async -> Bool {
        guard let state = runs[runId] else { return false }
        await state.submitAnswer(questionId, answer)
        return true
    }

    public func handleRequest(method: String, path: String, body: Data?) async -> HTTPResponse {
        let components = path.split(separator: "/").map(String.init)

        if method == "POST" && components == ["pipelines"] {
            guard let body = body,
                  let dotSource = String(data: body, encoding: .utf8),
                  !dotSource.isEmpty else {
                return HTTPResponse(status: 400, body: "{\"error\":\"Missing DOT source in request body\"}")
            }
            let state = createRun(dotSource: dotSource)
            await startRun(state)
            let status = await state.status.rawValue
            return HTTPResponse(
                status: 201,
                body: "{\"id\":\"\(state.id)\",\"status\":\"\(status)\"}"
            )
        }

        if method == "GET" && components.count == 2 && components[0] == "pipelines" {
            let id = components[1]
            guard let state = getRun(id) else {
                return HTTPResponse(status: 404, body: "{\"error\":\"Pipeline not found\"}")
            }
            let status = await state.status
            let result = await state.result
            let error = await state.error
            var json: [String: Any] = [
                "id": state.id,
                "status": status.rawValue,
            ]
            if let result {
                json["completed_nodes"] = result.completedNodes
                json["node_outcomes"] = result.nodeOutcomes.mapValues { $0.rawValue }
            }
            if let error {
                json["error"] = error
            }
            let data = try? JSONSerialization.data(withJSONObject: json, options: [.sortedKeys])
            return HTTPResponse(status: 200, body: String(data: data ?? Data(), encoding: .utf8) ?? "{}")
        }

        if method == "GET" && components.count == 3 && components[0] == "pipelines" && components[2] == "events" {
            let id = components[1]
            guard let state = getRun(id) else {
                return HTTPResponse(status: 404, body: "{\"error\":\"Pipeline not found\"}")
            }
            let events = await state.eventEmitter.history()
            var lines: [String] = []
            for event in events {
                var eventData: [String: Any] = [
                    "kind": event.kind.rawValue,
                    "timestamp": event.timestamp.ISO8601Format(),
                ]
                if let nodeId = event.nodeId { eventData["node_id"] = nodeId }
                for (k, v) in event.data { eventData[k] = v }
                if let data = try? JSONSerialization.data(withJSONObject: eventData, options: [.sortedKeys]),
                   let str = String(data: data, encoding: .utf8) {
                    lines.append("data: \(str)\n")
                }
            }
            return HTTPResponse(
                status: 200,
                body: lines.joined(separator: "\n"),
                contentType: "text/event-stream"
            )
        }

        if method == "POST" && components.count == 3 && components[0] == "pipelines" && components[2] == "cancel" {
            let id = components[1]
            if await cancelRun(id) {
                return HTTPResponse(status: 200, body: "{\"status\":\"cancelled\"}")
            }
            return HTTPResponse(status: 404, body: "{\"error\":\"Pipeline not found\"}")
        }

        if method == "GET" && components.count == 3 && components[0] == "pipelines" && components[2] == "questions" {
            let id = components[1]
            guard let state = getRun(id) else {
                return HTTPResponse(status: 404, body: "{\"error\":\"Pipeline not found\"}")
            }
            let pendingQuestions = await state.pendingQuestions
            var questions: [[String: Any]] = []
            for q in pendingQuestions {
                var qDict: [String: Any] = [
                    "id": q.id,
                    "text": q.question.text,
                    "type": q.question.type.rawValue,
                    "stage": q.question.stage,
                ]
                if !q.question.options.isEmpty {
                    qDict["options"] = q.question.options.map { ["key": $0.key, "label": $0.label] }
                }
                questions.append(qDict)
            }
            let data = try? JSONSerialization.data(withJSONObject: ["questions": questions], options: [.sortedKeys])
            return HTTPResponse(status: 200, body: String(data: data ?? Data(), encoding: .utf8) ?? "{}")
        }

        if method == "POST" && components.count == 5
            && components[0] == "pipelines" && components[2] == "questions" && components[4] == "answer" {
            let runId = components[1]
            let qid = components[3]
            guard let body = body,
                  let json = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                  let value = json["value"] as? String else {
                return HTTPResponse(status: 400, body: "{\"error\":\"Missing 'value' in request body\"}")
            }

            let answer: InterviewAnswer
            if let optionKey = json["option_key"] as? String,
               let optionLabel = json["option_label"] as? String {
                answer = .option(InterviewOption(key: optionKey, label: optionLabel))
            } else {
                answer = .freeText(value)
            }

            if await submitAnswer(runId: runId, questionId: qid, answer: answer) {
                return HTTPResponse(status: 200, body: "{\"status\":\"answered\"}")
            }
            return HTTPResponse(status: 404, body: "{\"error\":\"Pipeline or question not found\"}")
        }

        if method == "GET" && components.count == 3 && components[0] == "pipelines" && components[2] == "checkpoint" {
            let id = components[1]
            guard let state = getRun(id) else {
                return HTTPResponse(status: 404, body: "{\"error\":\"Pipeline not found\"}")
            }
            if let result = await state.result {
                let json: [String: Any] = [
                    "completed_nodes": result.completedNodes,
                    "node_outcomes": result.nodeOutcomes.mapValues { $0.rawValue },
                    "context": result.context,
                ]
                let data = try? JSONSerialization.data(withJSONObject: json, options: [.sortedKeys])
                return HTTPResponse(status: 200, body: String(data: data ?? Data(), encoding: .utf8) ?? "{}")
            }
            return HTTPResponse(status: 200, body: "{\"status\":\"in_progress\"}")
        }

        if method == "GET" && components.count == 3 && components[0] == "pipelines" && components[2] == "context" {
            let id = components[1]
            guard let state = getRun(id) else {
                return HTTPResponse(status: 404, body: "{\"error\":\"Pipeline not found\"}")
            }
            if let result = await state.result {
                let data = try? JSONSerialization.data(withJSONObject: result.context, options: [.sortedKeys])
                return HTTPResponse(status: 200, body: String(data: data ?? Data(), encoding: .utf8) ?? "{}")
            }
            return HTTPResponse(status: 200, body: "{}")
        }

        return HTTPResponse(status: 404, body: "{\"error\":\"Not found\"}")
    }
}

// MARK: - HTTP Response

public struct HTTPResponse: Sendable {
    public let status: Int
    public let body: String
    public let contentType: String

    public init(status: Int, body: String, contentType: String = "application/json") {
        self.status = status
        self.body = body
        self.contentType = contentType
    }
}
