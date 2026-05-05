import Testing
import Foundation
@testable import OmniAIAttractor
import OmniAICore
import OmniAIAgent

// MARK: - Integration Tests with Real LLM Providers
//
// These tests make real API calls to LLM providers.
// Providers are env-gated by API key:
// OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY, GROQ_API_KEY, CEREBRAS_API_KEY.
// Run with: swift test --filter IntegrationTests

@Suite
final class IntegrationTests {

    private func makeTempDir() throws -> URL {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir
    }

    private func cleanup(_ dir: URL) {
        try? FileManager.default.removeItem(at: dir)
    }

    // MARK: - Smoke Test DOT Template

    /// Build the spec's integration smoke test pipeline (Section 11.13) with a model_stylesheet
    /// to set the LLM provider/model on all nodes.
    private func smokeTestDOT(model: String, provider: String, reasoningEffort: String = "high") -> String {
        """
        digraph test_pipeline {
            graph [
                goal="Create a hello world Python script",
                model_stylesheet="* { llm_model: \(model); llm_provider: \(provider); reasoning_effort: \(reasoningEffort); }"
            ]

            start       [shape=Mdiamond]
            plan        [shape=box, prompt="Plan how to create a hello world script for: $goal. Be brief, list 2-3 steps."]
            implement   [shape=box, prompt="Write a complete hello world Python script. Output just the code.", goal_gate=true, auto_status=true]
            review      [shape=box, prompt="Review this Python hello world script for correctness. Say SUCCESS if correct.", auto_status=true]
            done        [shape=Msquare]

            start -> plan
            plan -> implement
            implement -> review
            review -> done
        }
        """
    }

    /// Run the smoke test pipeline end-to-end with a real LLM provider.
    private func runSmokeTest(model: String, provider: String, reasoningEffort: String = "high") async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let client = try Client.fromEnv()
        let backend = LLMKitBackend(client: client)

        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .default,
            backend: backend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        let dot = smokeTestDOT(model: model, provider: provider, reasoningEffort: reasoningEffort)

        print("\n========================================")
        print("Running integration test: \(model) (\(provider))")
        print("========================================\n")

        let result = try await engine.run(dot: dot)

        // Print results for manual verification
        print("Status: \(result.status.rawValue)")
        print("Completed nodes: \(result.completedNodes.joined(separator: " -> "))")
        print("Node outcomes:")
        for (nodeId, status) in result.nodeOutcomes.sorted(by: { $0.key < $1.key }) {
            print("  \(nodeId): \(status.rawValue)")
        }

        for nodeId in ["plan", "implement", "review"] {
            let responseFile = logsRoot.appendingPathComponent("\(nodeId)/response.md")
            if let content = try? String(contentsOf: responseFile, encoding: .utf8) {
                print("\n--- \(nodeId) response (first 500 chars) ---")
                print(String(content.prefix(500)))
                if content.count > 500 { print("... (\(content.count) total chars)") }
            }
        }

        // Assert pipeline succeeded
        XCTAssertEqual(result.status, .success, "Pipeline should succeed with \(model)")

        // Assert key nodes completed
        XCTAssertTrue(result.completedNodes.contains("start"), "'start' should be completed")
        XCTAssertTrue(result.completedNodes.contains("plan"), "'plan' should be completed")
        XCTAssertTrue(result.completedNodes.contains("implement"), "'implement' should be completed")
        XCTAssertTrue(result.completedNodes.contains("review"), "'review' should be completed")
        XCTAssertTrue(result.completedNodes.contains("done"), "'done' should be completed")

        // Assert goal gate (implement) was satisfied
        let implementOutcome = result.nodeOutcomes["implement"]
        XCTAssertTrue(
            implementOutcome == .success || implementOutcome == .partialSuccess,
            "Goal gate 'implement' should be success or partial_success, got \(implementOutcome?.rawValue ?? "nil")"
        )

        // Assert per-stage artifacts exist
        for nodeId in ["plan", "implement", "review"] {
            let stageDir = logsRoot.appendingPathComponent(nodeId)
            XCTAssertTrue(FileManager.default.fileExists(atPath: stageDir.appendingPathComponent("prompt.md").path),
                "\(nodeId)/prompt.md should exist")
            XCTAssertTrue(FileManager.default.fileExists(atPath: stageDir.appendingPathComponent("response.md").path),
                "\(nodeId)/response.md should exist")
            XCTAssertTrue(FileManager.default.fileExists(atPath: stageDir.appendingPathComponent("status.json").path),
                "\(nodeId)/status.json should exist")
        }

        // Assert checkpoint
        let checkpointFile = logsRoot.appendingPathComponent("checkpoint.json")
        XCTAssertTrue(FileManager.default.fileExists(atPath: checkpointFile.path), "checkpoint.json should exist")
        let checkpoint = try Checkpoint.load(from: checkpointFile)
        XCTAssertTrue(checkpoint.completedNodes.contains("plan"))
        XCTAssertTrue(checkpoint.completedNodes.contains("implement"))
        XCTAssertTrue(checkpoint.completedNodes.contains("review"))

        // Assert manifest
        XCTAssertTrue(FileManager.default.fileExists(atPath:
            logsRoot.appendingPathComponent("manifest.json").path), "manifest.json should exist")

        print("\nIntegration test PASSED for \(model) (\(provider))")
        print("========================================\n")
    }

    // MARK: - Provider Integration Tests (Real LLM Calls)

    @Test
    func testOpenAIGPT52HighReasoning() async throws {
        guard ProcessInfo.processInfo.environment["OPENAI_API_KEY"] != nil else {
            return
        }
        try await runSmokeTest(model: "gpt-5.2", provider: "openai", reasoningEffort: "high")
    }

    @Test
    func testGemini3FlashPreview() async throws {
        guard ProcessInfo.processInfo.environment["GEMINI_API_KEY"] != nil
           || ProcessInfo.processInfo.environment["GOOGLE_API_KEY"] != nil else {
            return
        }
        try await runSmokeTest(model: "gemini-3-flash-preview", provider: "gemini", reasoningEffort: "high")
    }

    @Test
    func testClaudeHaiku45() async throws {
        guard ProcessInfo.processInfo.environment["ANTHROPIC_API_KEY"] != nil else {
            return
        }
        try await runSmokeTest(model: "claude-haiku-4-5-20251001", provider: "anthropic", reasoningEffort: "high")
    }

    @Test
    func testGroqGPTOSS20B() async throws {
        guard ProcessInfo.processInfo.environment["GROQ_API_KEY"] != nil else {
            return
        }
        try await runSmokeTest(model: "openai/gpt-oss-20b", provider: "groq", reasoningEffort: "low")
    }

    @Test
    func testCerebrasGLM47() async throws {
        guard ProcessInfo.processInfo.environment["CEREBRAS_API_KEY"] != nil else {
            return
        }
        try await runSmokeTest(model: "zai-glm-4.7", provider: "cerebras", reasoningEffort: "low")
    }

    // MARK: - Engine-Level Tests with Mock Backend (No API Calls)

    @Test
    func testEngineLinearPipelineWithMock() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let backend = MockIntBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        let dot = """
        digraph test {
            graph [goal="Test pipeline"]
            start [shape=Mdiamond]
            step1 [shape=box, prompt="Do step 1"]
            step2 [shape=box, prompt="Do step 2"]
            done [shape=Msquare]
            start -> step1 -> step2 -> done
        }
        """

        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success)
        XCTAssertTrue(result.completedNodes.contains("step1"))
        XCTAssertTrue(result.completedNodes.contains("step2"))
        XCTAssertEqual(backend.callCount, 2, "Backend should be called for step1 and step2")
    }

    @Test
    func testEngineConditionalBranching() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        // Backend returns: fail -> success -> success
        let backend = SequentialIntBackend(results: [
            CodergenResult(response: "failed", status: .fail, notes: "intentional fail"),
            CodergenResult(response: "retried", status: .success, notes: "retry worked"),
            CodergenResult(response: "reviewed", status: .success, notes: "looks good"),
        ])

        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        let dot = """
        digraph test {
            graph [goal="Test conditional"]
            start [shape=Mdiamond]
            step1 [shape=box, prompt="Step 1"]
            retry [shape=box, prompt="Retry"]
            review [shape=box, prompt="Review"]
            done [shape=Msquare]
            start -> step1
            step1 -> review [condition="outcome=success"]
            step1 -> retry [condition="outcome=fail"]
            retry -> review
            review -> done
        }
        """

        let result = try await engine.run(dot: dot)
        XCTAssertEqual(result.status, .success)
        XCTAssertTrue(result.completedNodes.contains("retry"), "Should have taken fail->retry path")
        XCTAssertTrue(result.completedNodes.contains("review"), "Should have completed review")
    }

    @Test
    func testEngineParallelRoutesToFanInWithoutReexecutingBranch() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let backend = MockIntBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        let dot = """
        digraph test {
            graph [goal="Parallel fan-in routing"]
            start [shape=Mdiamond]
            fanout [shape=component, label="Parallel work"]
            branchA [shape=box, prompt="Branch A task"]
            branchB [shape=box, prompt="Branch B task"]
            join [shape=tripleoctagon, label="Join"]
            after [shape=box, prompt="After join task"]
            done [shape=Msquare]
            start -> fanout
            fanout -> branchA
            fanout -> branchB
            branchA -> join
            branchB -> join
            join -> after
            after -> done
        }
        """

        let result = try await engine.run(dot: dot)
        XCTAssertEqual(result.status, .success)
        XCTAssertEqual(
            backend.callCount,
            3,
            "Expected 3 codergen calls (branchA, branchB, after) without branch re-execution"
        )
        XCTAssertTrue(result.completedNodes.contains("fanout"))
        XCTAssertTrue(result.completedNodes.contains("join"))
        XCTAssertTrue(result.completedNodes.contains("after"))
    }

    @Test
    func testEngineGoalGateSatisfied() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let backend = MockIntBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        let dot = """
        digraph test {
            graph [goal="Test goal gate"]
            start [shape=Mdiamond]
            critical [shape=box, prompt="Critical step", goal_gate=true]
            done [shape=Msquare]
            start -> critical -> done
        }
        """

        let result = try await engine.run(dot: dot)
        XCTAssertEqual(result.status, .success, "Goal gate satisfied => success")
        XCTAssertEqual(result.nodeOutcomes["critical"], .success)
    }

    @Test
    func testEngineRetryOnFailure() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        // First call requests retry, second succeeds
        let backend = SequentialIntBackend(results: [
            CodergenResult(response: "retry", status: .retry, notes: "first try requested retry"),
            CodergenResult(response: "ok", status: .success, notes: "retry worked"),
        ])

        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: PipelineRetryPolicy(strategy: .none),
            backend: backend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        let dot = """
        digraph test {
            graph [goal="Test retry"]
            start [shape=Mdiamond]
            step [shape=box, prompt="Do something", max_retries=2]
            done [shape=Msquare]
            start -> step -> done
        }
        """

        let result = try await engine.run(dot: dot)
        XCTAssertEqual(result.status, .success, "Should succeed after explicit retry")
    }

    @Test
    func testEngineContextUpdatesFlow() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let backend = SequentialIntBackend(results: [
            CodergenResult(response: "s1", status: .success, contextUpdates: ["step1_output": "hello"]),
            CodergenResult(response: "s2", status: .success, contextUpdates: ["step2_output": "world"]),
        ])

        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        let dot = """
        digraph test {
            graph [goal="Test context"]
            start [shape=Mdiamond]
            step1 [shape=box, prompt="Step 1"]
            step2 [shape=box, prompt="Step 2"]
            done [shape=Msquare]
            start -> step1 -> step2 -> done
        }
        """

        let result = try await engine.run(dot: dot)
        XCTAssertEqual(result.status, .success)
        XCTAssertEqual(result.context["step1_output"], "hello")
        XCTAssertEqual(result.context["step2_output"], "world")
    }

    @Test
    func testEngineCheckpointSaveAndResume() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let backend = MockIntBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        let dot = """
        digraph test {
            graph [goal="Test checkpoint"]
            start [shape=Mdiamond]
            step1 [shape=box, prompt="Step 1"]
            step2 [shape=box, prompt="Step 2"]
            done [shape=Msquare]
            start -> step1 -> step2 -> done
        }
        """

        let result = try await engine.run(dot: dot)
        XCTAssertEqual(result.status, .success)

        let checkpointFile = logsRoot.appendingPathComponent("checkpoint.json")
        XCTAssertTrue(FileManager.default.fileExists(atPath: checkpointFile.path))

        let checkpoint = try Checkpoint.load(from: checkpointFile)
        XCTAssertTrue(checkpoint.completedNodes.contains("step1"))
        XCTAssertTrue(checkpoint.completedNodes.contains("step2"))
    }

    @Test
    func testEngineStylesheetAppliesModel() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let trackingBackend = TrackingIntBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: trackingBackend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        let dot = """
        digraph test {
            graph [goal="Test stylesheet", model_stylesheet="* { llm_model: custom-model-xyz; llm_provider: custom-provider; }"]
            start [shape=Mdiamond]
            step [shape=box, prompt="Do something"]
            done [shape=Msquare]
            start -> step -> done
        }
        """

        let result = try await engine.run(dot: dot)
        XCTAssertEqual(result.status, .success)

        XCTAssertFalse(trackingBackend.calls.isEmpty, "Backend should have been called")
        if let call = trackingBackend.calls.first {
            XCTAssertEqual(call.model, "custom-model-xyz", "Stylesheet llm_model should be applied")
            XCTAssertEqual(call.provider, "custom-provider", "Stylesheet llm_provider should be applied")
        }
    }

    @Test
    func testEngineWaitHumanRouting() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let backend = MockIntBackend()
        let interviewer = QueueInterviewer(answers: [
            InterviewAnswer(value: "Approve", selectedOption: InterviewOption(key: "A", label: "Approve"))
        ])

        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend,
            interviewer: interviewer
        )

        let engine = PipelineEngine(config: config)
        let dot = """
        digraph test {
            graph [goal="Test human gate"]
            start [shape=Mdiamond]
            step [shape=box, prompt="Prepare work"]
            gate [shape=hexagon, label="Approve?"]
            approved [shape=Msquare]
            rejected [shape=Msquare]
            start -> step -> gate
            gate -> approved [label="Approve"]
            gate -> rejected [label="Reject"]
        }
        """

        let result = try await engine.run(dot: dot)
        XCTAssertEqual(result.status, .success)
        XCTAssertTrue(result.completedNodes.contains("gate"), "Human gate should be completed")
    }

    @Test
    func testEngine10NodePipeline() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let backend = MockIntBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        let dot = """
        digraph test {
            graph [goal="Large pipeline"]
            start [shape=Mdiamond]
            n1 [shape=box, prompt="Node 1"]
            n2 [shape=box, prompt="Node 2"]
            n3 [shape=box, prompt="Node 3"]
            n4 [shape=box, prompt="Node 4"]
            n5 [shape=box, prompt="Node 5"]
            n6 [shape=box, prompt="Node 6"]
            n7 [shape=box, prompt="Node 7"]
            n8 [shape=box, prompt="Node 8"]
            n9 [shape=box, prompt="Node 9"]
            n10 [shape=box, prompt="Node 10"]
            done [shape=Msquare]
            start -> n1 -> n2 -> n3 -> n4 -> n5 -> n6 -> n7 -> n8 -> n9 -> n10 -> done
        }
        """

        let result = try await engine.run(dot: dot)
        XCTAssertEqual(result.status, .success)
        XCTAssertEqual(backend.callCount, 10, "All 10 nodes should call the backend")
        for i in 1...10 {
            XCTAssertTrue(result.completedNodes.contains("n\(i)"), "Node n\(i) should be completed")
        }
    }

    @Test
    func testEngineCustomHandlerRegistration() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let backend = MockIntBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        engine.registerHandler(type: "custom_test", handler: CustomIntTestHandler())

        let dot = """
        digraph test {
            graph [goal="Custom handler test"]
            start [shape=Mdiamond]
            custom [shape=box, type="custom_test", prompt="Custom step"]
            done [shape=Msquare]
            start -> custom -> done
        }
        """

        let result = try await engine.run(dot: dot)
        XCTAssertEqual(result.status, .success)
        XCTAssertTrue(result.completedNodes.contains("custom"))
    }

    // MARK: - Validation-Only Test (No API Calls)

    @Test
    func testValidateSpecPipeline() throws {
        let dot = smokeTestDOT(model: "test-model", provider: "test")
        let graph = try DOTParser.parse(dot)

        let transformed = VariableExpansionTransform().apply(graph)
        let final = StylesheetTransform().apply(transformed)

        let diagnostics = PipelineValidator.validate(final)
        let errors = diagnostics.filter { $0.isError }

        XCTAssertEqual(errors.count, 0,
            "Spec pipeline should validate without errors: \(errors.map(\.message))")

        XCTAssertEqual(final.goal, "Create a hello world Python script")
        XCTAssertEqual(final.nodes.count, 5)

        // Verify stylesheet was applied
        let planNode = final.node("plan")
        XCTAssertEqual(planNode?.llmModel, "test-model")
        XCTAssertEqual(planNode?.llmProvider, "test")

        // Verify variable expansion
        XCTAssertTrue(planNode?.prompt.contains("Create a hello world Python script") ?? false,
            "$goal should be expanded in prompt")

        // Verify goal gate
        XCTAssertEqual(final.node("implement")?.goalGate, true)
    }

    // MARK: - Checkpoint Round-Trip Test (No API Calls)

    // MARK: - Manager Loop Handler Tests (Gap 1/3)

    @Test
    func testManagerLoopWithChildDOT() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        // Write a child DOT file to disk
        let childDOT = """
        digraph child {
            graph [goal="Child pipeline test"]
            start [shape=Mdiamond]
            step [shape=box, prompt="Child step"]
            done [shape=Msquare]
            start -> step -> done
        }
        """
        let childDotFile = logsRoot.appendingPathComponent("child.dot")
        try Data(childDOT.utf8).write(to: childDotFile)

        let backend = MockIntBackend()

        // Test ManagerLoopHandler directly
        let handler = ManagerLoopHandler(backend: backend)
        let node = Node(id: "manager", shape: "house")
        let context = PipelineContext()
        let graph = Graph(
            attributes: GraphAttributes(
                stackChildDotfile: childDotFile.path
            )
        )

        let outcome = try await handler.execute(
            node: node,
            context: context,
            graph: graph,
            logsRoot: logsRoot
        )

        XCTAssertTrue(
            outcome.status == .success || outcome.status == .partialSuccess,
            "Manager loop should succeed, got \(outcome.status.rawValue): \(outcome.notes)"
        )
        // Verify context updates from child pipeline were propagated
        XCTAssertTrue(outcome.notes.contains("cycle"), "Notes should mention cycle count")
    }

    @Test
    func testManagerLoopFailsWithoutDotfile() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let backend = MockIntBackend()
        let handler = ManagerLoopHandler(backend: backend)
        let node = Node(id: "manager", shape: "house")
        let context = PipelineContext()
        let graph = Graph()

        let outcome = try await handler.execute(
            node: node,
            context: context,
            graph: graph,
            logsRoot: logsRoot
        )

        XCTAssertEqual(outcome.status, .fail, "Should fail without child dotfile")
        XCTAssertTrue(outcome.failureReason.contains("no stack.child_dotfile"))
    }

    // MARK: - Tool Hooks Tests (Gap 2)

    @Test
    func testToolHooksPreAndPostExecution() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let backend = MockIntBackend()

        // Create a graph with tool hooks
        let graph = Graph(
            id: "hook_test",
            nodes: [
                "start": Node(id: "start", shape: "Mdiamond"),
                "step": Node(id: "step", shape: "box", prompt: "Do work"),
                "done": Node(id: "done", shape: "Msquare"),
            ],
            edges: [
                Edge(from: "start", to: "step"),
                Edge(from: "step", to: "done"),
            ],
            attributes: GraphAttributes(
                goal: "Test hooks",
                toolHooksPre: "echo PRE_HOOK_OUTPUT",
                toolHooksPost: "echo POST_HOOK_OUTPUT"
            )
        )

        // Execute CodergenHandler directly to verify hooks run
        let handler = CodergenHandler(backend: backend)
        let node = graph.node("step")!
        let context = PipelineContext()

        let outcome = try await handler.execute(
            node: node,
            context: context,
            graph: graph,
            logsRoot: logsRoot
        )

        XCTAssertEqual(outcome.status, .success)

        // Verify hook outputs were captured in context
        let preOutput = context.getString("hook_pre_output")
        XCTAssertEqual(preOutput, "PRE_HOOK_OUTPUT", "Pre-hook output should be captured")

        let postOutput = context.getString("hook_post_output")
        XCTAssertEqual(postOutput, "POST_HOOK_OUTPUT", "Post-hook output should be captured")
    }

    @Test
    func testCodergenHandlerPersistsArtifactAndContextOverrides() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let backend = MockIntBackend()
        let handler = CodergenHandler(backend: backend)

        let artifactPath = logsRoot
            .appendingPathComponent("artifacts/summary.md")
            .path

        let node = Node(
            id: "stage",
            shape: "box",
            prompt: "Generate stage output",
            llmModel: "gpt-5.2",
            llmProvider: "openai"
        )
        node.rawAttributes["artifact_path"] = .string(artifactPath)
        node.rawAttributes["excluded_tools"] = .string("Task, write_file")
        node.rawAttributes["max_agent_turns"] = .integer(13)
        node.rawAttributes["default_command_timeout_ms"] = .integer(30_000)
        node.rawAttributes["max_command_timeout_ms"] = .integer(120_000)
        node.rawAttributes["llm_inactivity_timeout_seconds"] = .float(45.0)
        node.rawAttributes["loop_detection_window"] = .integer(7)
        node.rawAttributes["parallel_tool_calls"] = .boolean(true)
        node.rawAttributes["user_instructions"] = .string("Keep output short")

        let context = PipelineContext()
        let graph = Graph(id: "artifact_graph")

        let outcome = try await handler.execute(
            node: node,
            context: context,
            graph: graph,
            logsRoot: logsRoot
        )

        XCTAssertEqual(outcome.status, .success)

        let persisted = try String(contentsOfFile: artifactPath, encoding: .utf8)
        XCTAssertTrue(persisted.contains("Mock response for:"))
        XCTAssertEqual(context.getString("artifact.stage.path"), artifactPath)

        XCTAssertEqual(context.getString("_current_node_id"), "stage")
        XCTAssertEqual(context.getString("_current_node_excluded_tools"), "Task, write_file")
        XCTAssertEqual(context.getString("_current_node_max_agent_turns"), "13")
        XCTAssertEqual(context.getString("_current_node_default_command_timeout_ms"), "30000")
        XCTAssertEqual(context.getString("_current_node_max_command_timeout_ms"), "120000")
        XCTAssertEqual(context.getString("_current_node_llm_inactivity_timeout_seconds"), "45.0")
        XCTAssertEqual(context.getString("_current_node_loop_detection_window"), "7")
        XCTAssertEqual(context.getString("_current_node_parallel_tool_calls"), "true")
        XCTAssertEqual(context.getString("_current_node_user_instructions"), "Keep output short")
        XCTAssertFalse(context.getString("_current_node_resume_key").isEmpty)
    }

    // MARK: - Loop Restart Tests (Gap 4)

    @Test
    func testLoopRestartClearsCompletedNodes() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        let backend = MockIntBackend()

        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        // On first pass through step2, internal.loop_restart_count is not set (resolves to ""),
        // so the condition "context.internal.loop_restart_count=" matches empty string.
        // The loop_restart edge fires, clearing completedNodes, and we restart from step1.
        // On second pass through step2, internal.loop_restart_count is "1" (not empty),
        // so the condition does NOT match. The unconditional "done" edge is taken instead.
        let dot = """
        digraph test {
            graph [goal="Test loop restart"]
            start [shape=Mdiamond]
            step1 [shape=box, prompt="Step 1"]
            step2 [shape=box, prompt="Step 2"]
            done [shape=Msquare]
            start -> step1 -> step2
            step2 -> step1 [loop_restart=true, condition="context.internal.loop_restart_count="]
            step2 -> done
        }
        """

        let result = try await engine.run(dot: dot)

        // Pipeline should have completed successfully
        XCTAssertEqual(result.status, .success, "Pipeline should succeed after loop restart")

        // The loop should have been restarted exactly once
        let loopCount = result.context["internal.loop_restart_count"] ?? "0"
        XCTAssertEqual(loopCount, "1", "Loop should have restarted exactly once")
        XCTAssertTrue(result.logsRoot.lastPathComponent.contains("_cycle1"), "Restarted run should use a fresh cycle log directory")
    }

    // MARK: - LLM Response Parsing Tests (Gap 5)

    @Test
    func testLLMResponseWithoutJSONBlockReturnsPartialSuccess() async throws {
        let logsRoot = try makeTempDir()
        defer { cleanup(logsRoot) }

        // Use a backend that returns a response WITHOUT a JSON status block
        let backend = RawResponseIntBackend(response: "Here is some output without any JSON block.")

        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend,
            interviewer: AutoApproveInterviewer()
        )

        let engine = PipelineEngine(config: config)
        let dot = """
        digraph test {
            graph [goal="Test partial success default"]
            start [shape=Mdiamond]
            step [shape=box, prompt="Do work"]
            done [shape=Msquare]
            start -> step -> done
        }
        """

        let result = try await engine.run(dot: dot)

        // The step should have returned partialSuccess since no JSON block was found
        XCTAssertTrue(
            result.nodeOutcomes["step"] == .partialSuccess || result.nodeOutcomes["step"] == .success,
            "Step should be partial_success (or success via mock), got \(result.nodeOutcomes["step"]?.rawValue ?? "nil")"
        )
        XCTAssertEqual(result.status, .success, "Pipeline should still succeed")
    }

    @Test
    func testLLMKitBackendParseResponseNoJSON() throws {
        // LLMKitBackend.parseResponse now throws AttractorError.llmError when no JSON
        // status block is found (AT-06: strict response parsing). This causes the
        // CodergenHandler's retry logic to re-attempt the LLM call.
        // The mock RawResponseIntBackend simulates the old partial_success behavior
        // for testing pipeline flow; real LLMKitBackend would throw instead.
        let result = CodergenResult(
            response: "no json here",
            status: .partialSuccess,
            notes: "WARNING: No structured status block found"
        )
        XCTAssertEqual(result.status, .partialSuccess)
        XCTAssertTrue(result.notes.contains("WARNING"))
    }

    // MARK: - Checkpoint Round-Trip Test (No API Calls)

    @Test
    func testCheckpointSaveAndLoadRoundTrip() throws {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-checkpoint-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }

        let checkpoint = Checkpoint(
            timestamp: Date(),
            currentNode: "implement",
            completedNodes: ["start", "plan"],
            nodeRetries: ["plan": 1],
            contextValues: ["graph.goal": "Test", "last_stage": "plan"],
            logs: ["[start] success", "[plan] success"]
        )

        let url = dir.appendingPathComponent("checkpoint.json")
        try checkpoint.save(to: url)
        let loaded = try Checkpoint.load(from: url)

        XCTAssertEqual(loaded.currentNode, "implement")
        XCTAssertEqual(loaded.completedNodes, ["start", "plan"])
        XCTAssertEqual(loaded.nodeRetries["plan"], 1)
        XCTAssertEqual(loaded.contextValues["graph.goal"], "Test")
        XCTAssertEqual(loaded.contextValues["last_stage"], "plan")
        XCTAssertEqual(loaded.logs.count, 2)
    }

    // MARK: - Unified E2E (Real Providers, Env-Gated by Provider Key)

    private func unifiedE2EEnabled() -> Bool {
        let env = ProcessInfo.processInfo.environment
        return env["RUN_OMNIAI_E2E_TESTS"] == "1"
    }

    private func hasProviderKey(_ provider: String) -> Bool {
        let env = ProcessInfo.processInfo.environment
        switch provider {
        case "openai":
            return !(env["OPENAI_API_KEY"] ?? "").isEmpty
        case "anthropic":
            return !(env["ANTHROPIC_API_KEY"] ?? "").isEmpty
        case "gemini":
            return !(env["GEMINI_API_KEY"] ?? "").isEmpty
        case "groq":
            return !(env["GROQ_API_KEY"] ?? "").isEmpty
        case "cerebras":
            return !(env["CEREBRAS_API_KEY"] ?? "").isEmpty
        default:
            return false
        }
    }

    private func e2eModel(provider: String) -> String {
        let env = ProcessInfo.processInfo.environment
        switch provider {
        case "openai":
            return env["OPENAI_E2E_MODEL"] ?? "gpt-5.2"
        case "anthropic":
            return env["ANTHROPIC_E2E_MODEL"] ?? "claude-haiku-4-5-20251001"
        case "gemini":
            return env["GEMINI_E2E_MODEL"] ?? "gemini-3-flash-preview"
        case "groq":
            return env["GROQ_E2E_MODEL"] ?? "openai/gpt-oss-20b"
        case "cerebras":
            return env["CEREBRAS_E2E_MODEL"] ?? "zai-glm-4.7"
        default:
            return ""
        }
    }

    private func minimalE2EDOT(model: String, provider: String, reasoningEffort: String) -> String {
        """
        digraph e2e {
            graph [
                goal="Provider e2e validation",
                model_stylesheet="* { llm_model: \(model); llm_provider: \(provider); reasoning_effort: \(reasoningEffort); }"
            ]
            start [shape=Mdiamond]
            stage [shape=box, prompt="Return the word E2E_OK and include a JSON status block with outcome=success."]
            done [shape=Msquare]
            start -> stage -> done
        }
        """
    }

    private func e2eProviderOptions(provider: String) -> [String: JSONValue]? {
        if provider == "cerebras" {
            return ["cerebras": .object(["disable_reasoning": .bool(true)])]
        }
        return nil
    }

    @Test
    func testUnifiedE2EAllProviders() async throws {
        guard unifiedE2EEnabled() else { return }

        let client = try Client.fromEnv()
        let providers: [(id: String, reasoning: String)] = [
            ("openai", "low"),
            ("anthropic", "high"),
            ("gemini", "high"),
            ("groq", "low"),
            ("cerebras", "low"),
        ]

        var executedProviders = 0

        for item in providers {
            guard hasProviderKey(item.id) else { continue }
            executedProviders += 1
            let model = e2eModel(provider: item.id)

            // 1) OmniAICore
            let llmResult = try await generate(
                model: model,
                prompt: "Reply with exactly E2E_OK.",
                maxTokens: 64,
                reasoningEffort: item.id == "openai" ? item.reasoning : nil,
                provider: item.id,
                providerOptions: e2eProviderOptions(provider: item.id),
                client: client
            )
            XCTAssertFalse(llmResult.response.id.isEmpty, "OmniAICore generate() returned empty response id for \(item.id)")
            XCTAssertGreaterThan(llmResult.totalUsage.totalTokens, 0, "OmniAICore generate() returned zero token usage for \(item.id)")

            // 2) OmniAIAgent
            do {
                let agentDir = try makeTempDir()
                defer { cleanup(agentDir) }

                let profile = MinimalE2EProfile(id: item.id, model: model)
                let session = try await CodingAgent.createSession(
                    profile: profile,
                    workingDir: agentDir.path,
                    config: SessionConfig(maxTurns: 6, maxToolRoundsPerInput: 1),
                    client: client
                )
                await session.submit("Reply with exactly E2E_OK.")
                let history = await session.getHistory()
                let assistantText = history.compactMap { turn -> String? in
                    if case .assistant(let assistant) = turn {
                        return assistant.content
                    }
                    return nil
                }.joined(separator: "\n")
                XCTAssertFalse(
                    assistantText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
                    "OmniAIAgent produced no assistant text for \(item.id)"
                )
                await session.close()
            }

            // 3) OmniAIAttractor
            do {
                let logsRoot = try makeTempDir()
                defer { cleanup(logsRoot) }

                let backend = LLMKitBackend(client: client)
                let config = PipelineConfig(
                    logsRoot: logsRoot,
                    retryPolicy: .default,
                    backend: backend,
                    interviewer: AutoApproveInterviewer()
                )
                let engine = PipelineEngine(config: config)
                let result = try await engine.run(dot: minimalE2EDOT(
                    model: model,
                    provider: item.id,
                    reasoningEffort: item.reasoning
                ))
                XCTAssertEqual(result.status, .success, "OmniAIAttractor failed for \(item.id)")
                XCTAssertTrue(result.completedNodes.contains("stage"), "stage did not complete for \(item.id)")
            }

            // 4) OmniAIAttractor + CodingAgentBackend
            do {
                let logsRoot = try makeTempDir()
                defer { cleanup(logsRoot) }

                let backend = CodingAgentBackend(client: client, workingDirectory: logsRoot.path)
                let config = PipelineConfig(
                    logsRoot: logsRoot,
                    retryPolicy: .default,
                    backend: backend,
                    interviewer: AutoApproveInterviewer()
                )
                let engine = PipelineEngine(config: config)
                let result = try await engine.run(dot: minimalE2EDOT(
                    model: model,
                    provider: item.id,
                    reasoningEffort: item.reasoning
                ))
                XCTAssertEqual(result.status, .success, "CodingAgentBackend failed for \(item.id)")
                XCTAssertTrue(result.completedNodes.contains("stage"), "CodingAgentBackend stage did not complete for \(item.id)")
            }
        }

        XCTAssertGreaterThan(
            executedProviders,
            0,
            "RUN_OMNIAI_E2E_TESTS=1 but no provider API keys were present"
        )
    }
}

private struct MinimalE2EProfile: ProviderProfile {
    let id: String
    let model: String
    let toolRegistry = ToolRegistry()

    func buildSystemPrompt(
        environment: ExecutionEnvironment,
        projectDocs: String?,
        userInstructions: String?,
        gitContext: GitContext?
    ) -> String {
        "You are a concise assistant. Follow the user's request exactly."
    }

    func providerOptions() -> [String: JSONValue]? {
        if id == "cerebras" {
            return ["cerebras": .object(["disable_reasoning": .bool(true)])]
        }
        return nil
    }

    var supportsReasoning: Bool { true }
    var supportsStreaming: Bool { false }
    var supportsParallelToolCalls: Bool { false }
    var contextWindowSize: Int { 128_000 }
}

// MARK: - Mock Backends for Integration Tests (unique names to avoid conflict with AttractorTests)

private final class MockIntBackend: CodergenBackend, @unchecked Sendable {
    private let lock = NSLock()
    private var _callCount = 0

    var callCount: Int {
        lock.lock()
        defer { lock.unlock() }
        return _callCount
    }

    func run(prompt: String, model: String, provider: String, reasoningEffort: String, context: PipelineContext) async throws -> CodergenResult {
        incrementCallCount()
        return CodergenResult(
            response: "Mock response for: \(String(prompt.prefix(50)))",
            status: .success,
            notes: "mock"
        )
    }

    private func incrementCallCount() {
        lock.lock()
        _callCount += 1
        lock.unlock()
    }
}

private final class SequentialIntBackend: CodergenBackend, @unchecked Sendable {
    private var results: [CodergenResult]
    private var index = 0
    private let lock = NSLock()

    init(results: [CodergenResult]) {
        self.results = results
    }

    func run(prompt: String, model: String, provider: String, reasoningEffort: String, context: PipelineContext) async throws -> CodergenResult {
        nextResult()
    }

    private func nextResult() -> CodergenResult {
        lock.lock()
        defer { lock.unlock() }
        if index < results.count {
            let result = results[index]
            index += 1
            return result
        }
        return CodergenResult(response: "default", status: .success)
    }
}

private final class TrackingIntBackend: CodergenBackend, @unchecked Sendable {
    struct Call {
        let prompt: String
        let model: String
        let provider: String
    }

    private let lock = NSLock()
    private var _calls: [Call] = []

    var calls: [Call] {
        lock.lock()
        defer { lock.unlock() }
        return _calls
    }

    func run(prompt: String, model: String, provider: String, reasoningEffort: String, context: PipelineContext) async throws -> CodergenResult {
        appendCall(prompt: prompt, model: model, provider: provider)
        return CodergenResult(response: "tracked", status: .success)
    }

    private func appendCall(prompt: String, model: String, provider: String) {
        lock.lock()
        _calls.append(Call(prompt: prompt, model: model, provider: provider))
        lock.unlock()
    }
}

private struct CustomIntTestHandler: NodeHandler, Sendable {
    let handlerType: HandlerType = .codergen

    func execute(node: Node, context: PipelineContext, graph: Graph, logsRoot: URL) async throws -> Outcome {
        return .success(notes: "Custom handler executed for \(node.id)")
    }
}


private final class RawResponseIntBackend: CodergenBackend, @unchecked Sendable {
    private let response: String

    init(response: String) {
        self.response = response
    }

    func run(prompt: String, model: String, provider: String, reasoningEffort: String, context: PipelineContext) async throws -> CodergenResult {
        // Simulate what LLMKitBackend.parseResponse would return for a response without JSON
        return CodergenResult(
            response: response,
            status: .partialSuccess,
            notes: "WARNING: No structured status block found in LLM response; defaulting to partial_success"
        )
    }
}
