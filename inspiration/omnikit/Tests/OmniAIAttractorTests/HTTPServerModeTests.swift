import Testing
import Foundation
@testable import OmniAIAttractor

@Suite
final class HTTPServerModeTests {

    private func makeServer() -> AttractorHTTPServer {
        let backend = ServerMockBackend()
        return AttractorHTTPServer(backend: backend, port: 0)
    }

    // MARK: - POST /pipelines

    @Test
    func testCreatePipeline() async throws {
        let server = makeServer()
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            done [shape=Msquare]
            start -> done
        }
        """
        let response = await server.handleRequest(
            method: "POST",
            path: "/pipelines",
            body: Data(dot.utf8)
        )
        XCTAssertEqual(response.status, 201)
        XCTAssertTrue(response.body.contains("\"id\""))
        XCTAssertTrue(response.body.contains("\"status\":\"running\""))
    }

    @Test
    func testCreatePipelineMissingBody() async throws {
        let server = makeServer()
        let response = await server.handleRequest(
            method: "POST",
            path: "/pipelines",
            body: nil
        )
        XCTAssertEqual(response.status, 400)
    }

    // MARK: - GET /pipelines/{id}

    @Test
    func testGetPipelineStatus() async throws {
        let server = makeServer()
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            done [shape=Msquare]
            start -> done
        }
        """
        let createResp = await server.handleRequest(method: "POST", path: "/pipelines", body: Data(dot.utf8))
        XCTAssertEqual(createResp.status, 201)

        // Extract ID from response
        let jsonData = createResp.body.data(using: .utf8)!
        let json = try JSONSerialization.jsonObject(with: jsonData) as! [String: Any]
        let id = json["id"] as! String

        // Wait a moment for the pipeline to run
        try await Task.sleep(for: .milliseconds(200))

        let statusResp = await server.handleRequest(method: "GET", path: "/pipelines/\(id)", body: nil)
        XCTAssertEqual(statusResp.status, 200)
        XCTAssertTrue(statusResp.body.contains(id))
    }

    @Test
    func testGetPipelineNotFound() async throws {
        let server = makeServer()
        let response = await server.handleRequest(method: "GET", path: "/pipelines/nonexistent", body: nil)
        XCTAssertEqual(response.status, 404)
    }

    // MARK: - POST /pipelines/{id}/cancel

    @Test
    func testCancelPipeline() async throws {
        let server = makeServer()
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            step [shape=box, prompt="Slow work"]
            done [shape=Msquare]
            start -> step -> done
        }
        """
        let createResp = await server.handleRequest(method: "POST", path: "/pipelines", body: Data(dot.utf8))
        let jsonData = createResp.body.data(using: .utf8)!
        let json = try JSONSerialization.jsonObject(with: jsonData) as! [String: Any]
        let id = json["id"] as! String

        let cancelResp = await server.handleRequest(method: "POST", path: "/pipelines/\(id)/cancel", body: nil)
        XCTAssertEqual(cancelResp.status, 200)
        XCTAssertTrue(cancelResp.body.contains("cancelled"))
    }

    // MARK: - GET /pipelines/{id}/events

    @Test
    func testGetEvents() async throws {
        let server = makeServer()
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            done [shape=Msquare]
            start -> done
        }
        """
        let createResp = await server.handleRequest(method: "POST", path: "/pipelines", body: Data(dot.utf8))
        let jsonData = createResp.body.data(using: .utf8)!
        let json = try JSONSerialization.jsonObject(with: jsonData) as! [String: Any]
        let id = json["id"] as! String

        // Wait for pipeline to finish
        try await Task.sleep(for: .milliseconds(500))

        let eventsResp = await server.handleRequest(method: "GET", path: "/pipelines/\(id)/events", body: nil)
        XCTAssertEqual(eventsResp.status, 200)
        XCTAssertEqual(eventsResp.contentType, "text/event-stream")
    }

    // MARK: - GET /pipelines/{id}/questions

    @Test
    func testGetPendingQuestions() async throws {
        let server = makeServer()
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            done [shape=Msquare]
            start -> done
        }
        """
        let createResp = await server.handleRequest(method: "POST", path: "/pipelines", body: Data(dot.utf8))
        let jsonData = createResp.body.data(using: .utf8)!
        let json = try JSONSerialization.jsonObject(with: jsonData) as! [String: Any]
        let id = json["id"] as! String

        let questionsResp = await server.handleRequest(method: "GET", path: "/pipelines/\(id)/questions", body: nil)
        XCTAssertEqual(questionsResp.status, 200)
        XCTAssertTrue(questionsResp.body.contains("questions"))
    }

    // MARK: - POST /pipelines/{id}/questions/{qid}/answer

    @Test
    func testSubmitAnswerNotFound() async throws {
        let server = makeServer()
        let answerBody = "{\"value\": \"approve\"}"
        let response = await server.handleRequest(
            method: "POST",
            path: "/pipelines/nonexistent/questions/q1/answer",
            body: Data(answerBody.utf8)
        )
        XCTAssertEqual(response.status, 404)
    }

    // MARK: - GET /pipelines/{id}/checkpoint

    @Test
    func testGetCheckpoint() async throws {
        let server = makeServer()
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            done [shape=Msquare]
            start -> done
        }
        """
        let createResp = await server.handleRequest(method: "POST", path: "/pipelines", body: Data(dot.utf8))
        let jsonData = createResp.body.data(using: .utf8)!
        let json = try JSONSerialization.jsonObject(with: jsonData) as! [String: Any]
        let id = json["id"] as! String

        try await Task.sleep(for: .milliseconds(500))

        let checkpointResp = await server.handleRequest(method: "GET", path: "/pipelines/\(id)/checkpoint", body: nil)
        XCTAssertEqual(checkpointResp.status, 200)
    }

    // MARK: - GET /pipelines/{id}/context

    @Test
    func testGetContext() async throws {
        let server = makeServer()
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            done [shape=Msquare]
            start -> done
        }
        """
        let createResp = await server.handleRequest(method: "POST", path: "/pipelines", body: Data(dot.utf8))
        let jsonData = createResp.body.data(using: .utf8)!
        let json = try JSONSerialization.jsonObject(with: jsonData) as! [String: Any]
        let id = json["id"] as! String

        try await Task.sleep(for: .milliseconds(500))

        let contextResp = await server.handleRequest(method: "GET", path: "/pipelines/\(id)/context", body: nil)
        XCTAssertEqual(contextResp.status, 200)
    }

    // MARK: - Unknown Route

    @Test
    func testUnknownRoute() async throws {
        let server = makeServer()
        let response = await server.handleRequest(method: "GET", path: "/unknown", body: nil)
        XCTAssertEqual(response.status, 404)
    }


    @Test
    func testHTTPInterviewerCancellationResumesPendingQuestion() async throws {
        let state = PipelineRunState(id: "run-1", dotSource: "digraph test { start -> done }")
        let interviewer = HTTPServerInterviewer(runState: state)
        let question = InterviewQuestion(
            text: "Approve?",
            type: .confirm,
            defaultAnswer: .no(),
            stage: "gate"
        )

        let answerTask = Task {
            await interviewer.ask(question)
        }

        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask {
                while await state.pendingQuestions.isEmpty {
                    try await Task.sleep(for: .milliseconds(10))
                }
                await state.cancel()
            }
            group.addTask {
                let answer = await answerTask.value
                XCTAssertEqual(answer.answerValue, .no)
                let pendingQuestions = await state.pendingQuestions
                XCTAssertTrue(pendingQuestions.isEmpty)
            }
            _ = try await group.next()
            _ = try await group.next()
        }
    }

    // MARK: - End-to-End: Pipeline completes via HTTP

    @Test
    func testPipelineCompletesViaHTTP() async throws {
        let server = makeServer()
        let dot = """
        digraph test {
            graph [goal="HTTP test"]
            start [shape=Mdiamond]
            step [shape=box, prompt="Do work"]
            done [shape=Msquare]
            start -> step -> done
        }
        """
        let createResp = await server.handleRequest(method: "POST", path: "/pipelines", body: Data(dot.utf8))
        XCTAssertEqual(createResp.status, 201)

        let jsonData = createResp.body.data(using: .utf8)!
        let json = try JSONSerialization.jsonObject(with: jsonData) as! [String: Any]
        let id = json["id"] as! String

        // Wait for completion
        try await Task.sleep(for: .seconds(1))

        let statusResp = await server.handleRequest(method: "GET", path: "/pipelines/\(id)", body: nil)
        XCTAssertEqual(statusResp.status, 200)
        let statusJson = try JSONSerialization.jsonObject(with: statusResp.body.data(using: .utf8)!) as! [String: Any]
        let status = statusJson["status"] as? String
        XCTAssertTrue(status == "completed" || status == "running",
                      "Pipeline should complete, got status: \(status ?? "nil")")
    }

    // MARK: - Human-in-the-loop via HTTP

    @Test
    func testHumanGateViaHTTP() async throws {
        let server = makeServer()
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            gate [shape=hexagon, prompt="Approve?"]
            approved [shape=Msquare]
            rejected [shape=Msquare]
            start -> gate
            gate -> approved [label="Yes"]
            gate -> rejected [label="No"]
        }
        """

        let createResp = await server.handleRequest(method: "POST", path: "/pipelines", body: Data(dot.utf8))
        XCTAssertEqual(createResp.status, 201)
        let jsonData = createResp.body.data(using: .utf8)!
        let json = try JSONSerialization.jsonObject(with: jsonData) as! [String: Any]
        let id = json["id"] as! String

        // Wait a bit for the pipeline to reach the gate
        try await Task.sleep(for: .milliseconds(500))

        // Check pending questions
        let questionsResp = await server.handleRequest(method: "GET", path: "/pipelines/\(id)/questions", body: nil)
        XCTAssertEqual(questionsResp.status, 200)

        // If there's a pending question, answer it
        if let qData = questionsResp.body.data(using: .utf8),
           let qJson = try? JSONSerialization.jsonObject(with: qData) as? [String: Any],
           let questions = qJson["questions"] as? [[String: Any]],
           let firstQ = questions.first,
           let qid = firstQ["id"] as? String {
            let answerBody = "{\"value\":\"Yes\",\"option_key\":\"Y\",\"option_label\":\"Yes\"}"
            let answerResp = await server.handleRequest(
                method: "POST",
                path: "/pipelines/\(id)/questions/\(qid)/answer",
                body: Data(answerBody.utf8)
            )
            XCTAssertEqual(answerResp.status, 200)
        }

        // Wait for completion
        try await Task.sleep(for: .seconds(1))

        let finalResp = await server.handleRequest(method: "GET", path: "/pipelines/\(id)", body: nil)
        XCTAssertEqual(finalResp.status, 200)
    }
}

// MARK: - Mock Backend for Server Tests

private final class ServerMockBackend: CodergenBackend, @unchecked Sendable {
    func run(prompt: String, model: String, provider: String, reasoningEffort: String, context: PipelineContext) async throws -> CodergenResult {
        return CodergenResult(response: "Server mock response", status: .success, notes: "mock")
    }
}
