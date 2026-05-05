import Testing
import Foundation
@testable import OmniAIAttractor

@Suite
final class AttractorTests {

    private func withAsyncTimeout<T: Sendable>(seconds: Double, label: String, operation: @escaping @Sendable () async throws -> T) async throws -> T {
        try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask {
                try await operation()
            }
            group.addTask {
                try await Task.sleep(for: .milliseconds(Int64(seconds * 1_000)))
                throw NSError(domain: "AttractorTests", code: 99, userInfo: [NSLocalizedDescriptionKey: "Timed out during \(label)"])
            }
            guard let result = try await group.next() else {
                group.cancelAll()
                throw NSError(domain: "AttractorTests", code: 100, userInfo: [NSLocalizedDescriptionKey: "No result produced during \(label)"])
            }
            group.cancelAll()
            return result
        }
    }

    // MARK: - DOT Parsing Tests

    @Test
    func testParseSimpleLinearPipeline() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            task1 [label="Task One", shape=box, prompt="Do something"]
            done [shape=Msquare]

            start -> task1 -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        XCTAssertEqual(graph.id, "pipeline")
        XCTAssertEqual(graph.nodes.count, 3)
        XCTAssertEqual(graph.edges.count, 2)

        let startNode = graph.node("start")
        XCTAssertNotNil(startNode)
        XCTAssertEqual(startNode?.handlerType, .start)

        let task1 = graph.node("task1")
        XCTAssertNotNil(task1)
        XCTAssertEqual(task1?.label, "Task One")
        XCTAssertEqual(task1?.prompt, "Do something")
        XCTAssertEqual(task1?.handlerType, .codergen)

        let done = graph.node("done")
        XCTAssertNotNil(done)
        XCTAssertEqual(done?.handlerType, .exit)

        // Check edges
        let edge1 = graph.edges.first { $0.from == "start" && $0.to == "task1" }
        XCTAssertNotNil(edge1)
        let edge2 = graph.edges.first { $0.from == "task1" && $0.to == "done" }
        XCTAssertNotNil(edge2)
    }

    @Test
    func testParseGraphLevelAttributes() throws {
        let dot = """
        digraph myPipeline {
            graph [
                goal="Build a REST API",
                label="API Builder",
                default_max_retry=3,
                default_fidelity="compact"
            ]
            start [shape=Mdiamond]
            done [shape=Msquare]
            start -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        XCTAssertEqual(graph.id, "myPipeline")
        XCTAssertEqual(graph.goal, "Build a REST API")
        XCTAssertEqual(graph.label, "API Builder")
        XCTAssertEqual(graph.attributes.defaultMaxRetry, 3)
        XCTAssertEqual(graph.attributes.defaultFidelity, "compact")
    }

    @Test
    func testParseMultiLineNodeAttributes() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            review [
                label="Code Review",
                shape=box,
                prompt="Review the code for correctness",
                llm_model="claude-sonnet-4-5-20250929",
                llm_provider="anthropic",
                reasoning_effort="high",
                goal_gate=true,
                max_retries=5,
                auto_status=true
            ]
            done [shape=Msquare]
            start -> review -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        let review = graph.node("review")
        XCTAssertNotNil(review)
        XCTAssertEqual(review?.label, "Code Review")
        XCTAssertEqual(review?.prompt, "Review the code for correctness")
        XCTAssertEqual(review?.llmModel, "claude-sonnet-4-5-20250929")
        XCTAssertEqual(review?.llmProvider, "anthropic")
        XCTAssertEqual(review?.reasoningEffort, "high")
        XCTAssertEqual(review?.goalGate, true)
        XCTAssertEqual(review?.maxRetries, 5)
        XCTAssertEqual(review?.autoStatus, true)
    }

    // MARK: - Validation Tests

    @Test
    func testValidateMissingStartNode() throws {
        let dot = """
        digraph pipeline {
            task1 [shape=box]
            done [shape=Msquare]
            task1 -> done
        }
        """
        let graph = try DOTParser.parse(dot)
        let diagnostics = PipelineValidator.validate(graph)
        let errors = diagnostics.filter { $0.isError }

        let startError = errors.first { $0.rule == "start_node" }
        XCTAssertNotNil(startError, "Should report missing start node")
    }

    @Test
    func testValidateMissingExitNode() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            task1 [shape=box]
            start -> task1
        }
        """
        let graph = try DOTParser.parse(dot)
        let diagnostics = PipelineValidator.validate(graph)
        let errors = diagnostics.filter { $0.isError }

        let exitError = errors.first { $0.rule == "terminal_node" }
        XCTAssertNotNil(exitError, "Should report missing exit node")
    }

    @Test
    func testValidateOrphanNode() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            task1 [shape=box]
            orphan [shape=box]
            done [shape=Msquare]
            start -> task1 -> done
        }
        """
        let graph = try DOTParser.parse(dot)
        let diagnostics = PipelineValidator.validate(graph)

        let reachabilityErrors = diagnostics.filter { $0.rule == "reachability" && $0.isError }
        XCTAssertTrue(reachabilityErrors.contains { $0.nodeId == "orphan" },
                      "Should report orphan node as unreachable")
    }

    // MARK: - Condition Expression Tests

    @Test
    func testConditionEquals() throws {
        let expr = try ConditionParser.parse("outcome=success")
        let ctx = PipelineContext()

        XCTAssertTrue(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))
        XCTAssertFalse(expr.evaluate(outcome: "fail", preferredLabel: "", context: ctx))
    }

    @Test
    func testConditionNotEquals() throws {
        let expr = try ConditionParser.parse("outcome!=fail")
        let ctx = PipelineContext()

        XCTAssertTrue(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))
        XCTAssertFalse(expr.evaluate(outcome: "fail", preferredLabel: "", context: ctx))
    }

    @Test
    func testConditionAnd() throws {
        let expr = try ConditionParser.parse("outcome=success && context.approved=true")
        let ctx = PipelineContext()
        ctx.set("approved", "true")

        XCTAssertTrue(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))

        ctx.set("approved", "false")
        XCTAssertFalse(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))
    }

    @Test
    func testConditionOutcomeVariable() throws {
        let expr = try ConditionParser.parse("outcome=partial_success")
        let ctx = PipelineContext()

        XCTAssertTrue(expr.evaluate(outcome: "partial_success", preferredLabel: "", context: ctx))
        XCTAssertFalse(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))
    }

    @Test
    func testConditionPreferredLabel() throws {
        let expr = try ConditionParser.parse("preferred_label=approve")
        let ctx = PipelineContext()

        XCTAssertTrue(expr.evaluate(outcome: "success", preferredLabel: "approve", context: ctx))
        XCTAssertFalse(expr.evaluate(outcome: "success", preferredLabel: "reject", context: ctx))
    }

    @Test
    func testConditionContextVariable() throws {
        let expr = try ConditionParser.parse("context.language=swift")
        let ctx = PipelineContext()
        ctx.set("language", "swift")

        XCTAssertTrue(expr.evaluate(outcome: "", preferredLabel: "", context: ctx))

        ctx.set("language", "python")
        XCTAssertFalse(expr.evaluate(outcome: "", preferredLabel: "", context: ctx))
    }

    @Test
    func testEmptyConditionAlwaysTrue() throws {
        let expr = try ConditionParser.parse("")
        let ctx = PipelineContext()

        XCTAssertTrue(expr.evaluate(outcome: "fail", preferredLabel: "whatever", context: ctx))
        XCTAssertTrue(expr.evaluate(outcome: "", preferredLabel: "", context: ctx))
    }

    // MARK: - Stylesheet Tests

    @Test
    func testStylesheetByClassName() throws {
        let css = """
        .fast {
            llm_model: gpt-4o-mini;
            reasoning_effort: low;
        }
        """
        let stylesheet = try StylesheetParser.parse(css)
        let resolved = stylesheet.resolve(nodeId: "myNode", nodeClasses: ["fast"])

        XCTAssertEqual(resolved.llmModel, "gpt-4o-mini")
        XCTAssertEqual(resolved.reasoningEffort, "low")
    }

    @Test
    func testStylesheetByNodeId() throws {
        let css = """
        #review {
            llm_model: claude-opus-4-6;
            reasoning_effort: high;
        }
        """
        let stylesheet = try StylesheetParser.parse(css)
        let resolved = stylesheet.resolve(nodeId: "review", nodeClasses: [])

        XCTAssertEqual(resolved.llmModel, "claude-opus-4-6")
        XCTAssertEqual(resolved.reasoningEffort, "high")

        // Should not match a different node
        let other = stylesheet.resolve(nodeId: "other", nodeClasses: [])
        XCTAssertNil(other.llmModel)
    }

    @Test
    func testStylesheetSpecificityOrder() throws {
        let css = """
        * {
            llm_model: gpt-4o-mini;
            reasoning_effort: low;
        }
        .premium {
            llm_model: claude-sonnet-4-5-20250929;
        }
        #critical {
            llm_model: claude-opus-4-6;
        }
        """
        let stylesheet = try StylesheetParser.parse(css)

        // Universal only
        let basic = stylesheet.resolve(nodeId: "basic", nodeClasses: [])
        XCTAssertEqual(basic.llmModel, "gpt-4o-mini")
        XCTAssertEqual(basic.reasoningEffort, "low")

        // Class overrides universal
        let premium = stylesheet.resolve(nodeId: "someNode", nodeClasses: ["premium"])
        XCTAssertEqual(premium.llmModel, "claude-sonnet-4-5-20250929")
        XCTAssertEqual(premium.reasoningEffort, "low") // Inherited from universal

        // ID overrides class and universal
        let critical = stylesheet.resolve(nodeId: "critical", nodeClasses: ["premium"])
        XCTAssertEqual(critical.llmModel, "claude-opus-4-6")
        XCTAssertEqual(critical.reasoningEffort, "low") // Inherited from universal
    }

    @Test
    func testStylesheetOverriddenByNodeAttributes() throws {
        // The StylesheetTransform only applies to nodes that don't already have explicit values
        let dot = """
        digraph pipeline {
            graph [model_stylesheet="* { llm_model: gpt-4o-mini; }"]
            start [shape=Mdiamond]
            task1 [shape=box, llm_model="claude-opus-4-6"]
            task2 [shape=box]
            done [shape=Msquare]
            start -> task1 -> task2 -> done
        }
        """
        let graph = try DOTParser.parse(dot)
        let transform = StylesheetTransform()
        let _ = transform.apply(graph)

        // task1 has explicit llm_model, should keep it
        XCTAssertEqual(graph.node("task1")?.llmModel, "claude-opus-4-6")

        // task2 has no explicit llm_model, stylesheet should apply
        XCTAssertEqual(graph.node("task2")?.llmModel, "gpt-4o-mini")
    }

    // MARK: - Edge Selection Tests

    @Test
    func testEdgeSelectionConditionMatchWins() throws {
        // Build a graph with conditional edges
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            decide [shape=diamond]
            pathA [shape=box, label="Path A"]
            pathB [shape=box, label="Path B"]
            done [shape=Msquare]

            start -> decide
            decide -> pathA [condition="outcome=success"]
            decide -> pathB [condition="outcome=fail"]
            pathA -> done
            pathB -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        let edges = graph.outgoingEdges(from: "decide")
        XCTAssertEqual(edges.count, 2)

        let ctx = PipelineContext()

        // Evaluate conditions to find matching edge
        let successExpr = try ConditionParser.parse(edges[0].condition)
        let failExpr = try ConditionParser.parse(edges[1].condition)

        // When outcome=success, first edge should match
        XCTAssertTrue(successExpr.evaluate(outcome: "success", preferredLabel: "", context: ctx))
        XCTAssertFalse(failExpr.evaluate(outcome: "success", preferredLabel: "", context: ctx))

        // When outcome=fail, second edge should match
        XCTAssertFalse(successExpr.evaluate(outcome: "fail", preferredLabel: "", context: ctx))
        XCTAssertTrue(failExpr.evaluate(outcome: "fail", preferredLabel: "", context: ctx))
    }

    @Test
    func testEdgeSelectionWeightBreaksTies() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            fork [shape=diamond]
            pathA [shape=box]
            pathB [shape=box]
            done [shape=Msquare]

            start -> fork
            fork -> pathA [weight=10]
            fork -> pathB [weight=5]
            pathA -> done
            pathB -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        let edges = graph.outgoingEdges(from: "fork")
        XCTAssertEqual(edges.count, 2)

        // Both have empty conditions (always true), so weight should break the tie
        let sorted = edges.sorted { $0.weight > $1.weight }
        XCTAssertEqual(sorted.first?.to, "pathA")
        XCTAssertEqual(sorted.first?.weight, 10)
    }

    @Test
    func testEdgeSelectionLexicalTiebreak() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            fork [shape=diamond]
            alpha [shape=box]
            beta [shape=box]
            done [shape=Msquare]

            start -> fork
            fork -> beta
            fork -> alpha
            alpha -> done
            beta -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        let edges = graph.outgoingEdges(from: "fork")
        XCTAssertEqual(edges.count, 2)

        // Equal weight (0), equal condition (empty), lexical sort on target
        let sorted = edges.sorted { $0.to < $1.to }
        XCTAssertEqual(sorted.first?.to, "alpha")
    }

    // MARK: - Context Tests

    @Test
    func testContextUpdatesVisible() throws {
        let ctx = PipelineContext()

        // Set values
        ctx.set("key1", "value1")
        ctx.set("count", 42)
        ctx.set("flag", true)

        // Read back
        XCTAssertEqual(ctx.getString("key1"), "value1")
        XCTAssertEqual(ctx.getInt("count"), 42)
        XCTAssertEqual(ctx.getBool("flag"), true)

        // Clone should see same values
        let clone = ctx.clone()
        XCTAssertEqual(clone.getString("key1"), "value1")

        // Modifying clone should not affect original
        clone.set("key1", "modified")
        XCTAssertEqual(ctx.getString("key1"), "value1")
        XCTAssertEqual(clone.getString("key1"), "modified")

        // Apply updates
        ctx.applyUpdates(["key2": "value2", "key1": "updated"])
        XCTAssertEqual(ctx.getString("key1"), "updated")
        XCTAssertEqual(ctx.getString("key2"), "value2")
    }

    // MARK: - Handler Tests

    @Test
    func testStartHandlerReturnsSuccess() async throws {
        let handler = StartHandler()
        let node = Node(id: "start", shape: "Mdiamond")
        let ctx = PipelineContext()
        let graph = Graph()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler.execute(node: node, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .success)
    }

    @Test
    func testExitHandlerReturnsSuccess() async throws {
        let handler = ExitHandler()
        let node = Node(id: "done", shape: "Msquare")
        let ctx = PipelineContext()
        let graph = Graph()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler.execute(node: node, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .success)
    }

    @Test
    func testConditionalHandlerPassthrough() async throws {
        let handler = ConditionalHandler()
        let node = Node(id: "decide", shape: "diamond")
        let ctx = PipelineContext()
        let graph = Graph()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler.execute(node: node, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .success)
    }

    @Test
    func testWaitHumanPresentsChoices() async throws {
        // Build a graph with outgoing edges from a hexagon node
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            ask [shape=hexagon, prompt="Pick a path"]
            pathA [shape=box, label="Path A"]
            pathB [shape=box, label="Path B"]
            done [shape=Msquare]

            start -> ask
            ask -> pathA [label="[A] Approve"]
            ask -> pathB [label="[B] Reject"]
            pathA -> done
            pathB -> done
        }
        """
        let graph = try DOTParser.parse(dot)
        let askNode = graph.node("ask")!

        // Use QueueInterviewer to simulate picking the first option
        let interviewer = QueueInterviewer(answers: [
            .option(InterviewOption(key: "A", label: "[A] Approve"))
        ])
        let handler = WaitHumanHandler(interviewer: interviewer)
        let ctx = PipelineContext()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler.execute(node: askNode, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .success)
        XCTAssertEqual(outcome.preferredLabel, "[A] Approve")

        // Verify the question was presented with correct options
        let askedQuestions = await interviewer.askedQuestions
        XCTAssertEqual(askedQuestions.count, 1)
        let q = askedQuestions[0]
        XCTAssertEqual(q.text, "Pick a path")
        XCTAssertEqual(q.options.count, 2)
        XCTAssertEqual(q.options[0].label, "[A] Approve")
        XCTAssertEqual(q.options[1].label, "[B] Reject")
    }

    @Test
    func testWaitHumanAutoApprove() async throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            ask [shape=hexagon, prompt="Continue?"]
            next [shape=box]
            done [shape=Msquare]

            start -> ask
            ask -> next [label="Yes"]
            next -> done
        }
        """
        let graph = try DOTParser.parse(dot)
        let askNode = graph.node("ask")!

        let interviewer = AutoApproveInterviewer()
        let handler = WaitHumanHandler(interviewer: interviewer)
        let ctx = PipelineContext()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler.execute(node: askNode, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .success)
        XCTAssertEqual(outcome.preferredLabel, "Yes")
    }

    @Test
    func testWaitHumanTimeoutUsesHumanDefaultChoice() async throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            ask [shape=hexagon, prompt="Pick one", human.default_choice="pathDefault"]
            pathA [shape=box]
            pathDefault [shape=box]
            done [shape=Msquare]

            start -> ask
            ask -> pathA [label="Option A"]
            ask -> pathDefault [label="Option B"]
            pathA -> done
            pathDefault -> done
        }
        """
        let graph = try DOTParser.parse(dot)
        let askNode = graph.node("ask")!

        // Simulate a timeout
        let interviewer = QueueInterviewer(answers: [.timedOut()])
        let handler = WaitHumanHandler(interviewer: interviewer)
        let ctx = PipelineContext()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler.execute(node: askNode, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .success)
        XCTAssertEqual(outcome.preferredLabel, "Option B")
        XCTAssertEqual(outcome.suggestedNextIds, ["pathDefault"])
    }

    @Test
    func testWaitHumanSkipReturnsFail() async throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            ask [shape=hexagon, prompt="Pick one", human.default_choice="pathDefault"]
            pathA [shape=box]
            pathDefault [shape=box]
            done [shape=Msquare]

            start -> ask
            ask -> pathA [label="Option A"]
            ask -> pathDefault [label="Option B"]
            pathA -> done
            pathDefault -> done
        }
        """
        let graph = try DOTParser.parse(dot)
        let askNode = graph.node("ask")!

        let interviewer = QueueInterviewer(answers: [.skipped()])
        let handler = WaitHumanHandler(interviewer: interviewer)
        let ctx = PipelineContext()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler.execute(node: askNode, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .fail)
    }

    // MARK: - Variable Expansion

    @Test
    func testGoalVariableExpansion() throws {
        let dot = """
        digraph pipeline {
            graph [goal="Build a CLI tool"]
            start [shape=Mdiamond]
            task1 [shape=box, prompt="Your goal is: $goal"]
            done [shape=Msquare]
            start -> task1 -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        let transform = VariableExpansionTransform()
        let _ = transform.apply(graph)

        let task1 = graph.node("task1")
        XCTAssertEqual(task1?.prompt, "Your goal is: Build a CLI tool")
    }

    // MARK: - Custom Handler Registration

    @Test
    func testCustomHandlerRegistration() async throws {
        // Define a custom handler
        final class EchoHandler: NodeHandler, @unchecked Sendable {
            let handlerType: HandlerType = .codergen
            func execute(node: Node, context: PipelineContext, graph: Graph, logsRoot: URL) async throws -> Outcome {
                .success(contextUpdates: ["echo": node.prompt], notes: "echoed")
            }
        }

        let registry = HandlerRegistry()
        registry.register(type: "echo", handler: EchoHandler())

        let handler = registry.resolve("echo")
        XCTAssertNotNil(handler)

        let node = Node(id: "test", prompt: "hello")
        let ctx = PipelineContext()
        let graph = Graph()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler!.execute(node: node, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .success)
        XCTAssertEqual(outcome.contextUpdates["echo"] as? String, "hello")
    }

    // MARK: - Handler Registry Default Registration

    @Test
    func testDefaultHandlerRegistration() throws {
        let registry = HandlerRegistry()

        // Use a mock backend since we don't need real LLM calls
        let backend = MockCodergenBackend()
        let interviewer = AutoApproveInterviewer()

        registerDefaultHandlers(registry: registry, backend: backend, interviewer: interviewer)

        // All handler types should be registered
        XCTAssertNotNil(registry.resolve(.start))
        XCTAssertNotNil(registry.resolve(.exit))
        XCTAssertNotNil(registry.resolve(.codergen))
        XCTAssertNotNil(registry.resolve(.waitHuman))
        XCTAssertNotNil(registry.resolve(.conditional))
        XCTAssertNotNil(registry.resolve(.parallel))
        XCTAssertNotNil(registry.resolve(.parallelFanIn))
        XCTAssertNotNil(registry.resolve(.tool))
        XCTAssertNotNil(registry.resolve(.stackManagerLoop))
    }

    // MARK: - FanIn Handler

    @Test
    func testFanInConsolidatesResults() async throws {
        let handler = FanInHandler()
        let node = Node(id: "fanin", shape: "tripleoctagon")
        let ctx = PipelineContext()
        ctx.set("parallel.results", ["branch1": "success", "branch2": "success", "branch3": "fail"])

        let graph = Graph()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler.execute(node: node, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .success)
        XCTAssertTrue(outcome.notes.contains("2/3"))

        // parallel.results should be cleaned up
        XCTAssertNil(ctx.get("parallel.results"))
    }

    // MARK: - ToolHandler

    @Test
    func testToolHandlerExecutesCommand() async throws {
        let handler = ToolHandler()
        let node = Node(id: "tool1", shape: "parallelogram", prompt: "echo hello_world")
        let ctx = PipelineContext()
        let graph = Graph()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler.execute(node: node, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .success)

        let specStdout = outcome.contextUpdates["tool.output"] as? String ?? ""
        XCTAssertTrue(specStdout.contains("hello_world"))

        let stdout = outcome.contextUpdates["tool_stdout"] as? String ?? ""
        XCTAssertTrue(stdout.contains("hello_world"))
    }

    @Test
    func testToolHandlerFailsOnBadCommand() async throws {
        let handler = ToolHandler()
        let node = Node(id: "tool2", shape: "parallelogram", prompt: "false")
        let ctx = PipelineContext()
        let graph = Graph()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler.execute(node: node, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .fail)
    }

    @Test
    func testToolHandlerNoCommandFails() async throws {
        let handler = ToolHandler()
        let node = Node(id: "tool3", shape: "parallelogram")
        let ctx = PipelineContext()
        let graph = Graph()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler.execute(node: node, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .fail)
    }

    @Test
    func testToolHandlerFastExitDoesNotHang() async throws {
        let handler = ToolHandler()
        let node = Node(id: "tool-fast", shape: "parallelogram", prompt: "true")
        let ctx = PipelineContext()
        let graph = Graph()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await withAsyncTimeout(seconds: 2, label: "fast tool exit") {
            try await handler.execute(node: node, context: ctx, graph: graph, logsRoot: logsRoot)
        }
        XCTAssertEqual(outcome.status, .success)
    }

    @Test
    func testCodergenHandlerLargeStderrPreHookDoesNotDeadlock() async throws {
        guard let python = try? findPython3ForAttractorTests() else {
            return
        }
        let backend = MockCodergenBackend(result: CodergenResult(response: "done", status: .success))
        let handler = CodergenHandler(backend: backend)
        let node = Node(id: "codegen", shape: "box", prompt: "Do work")
        let ctx = PipelineContext()
        let graph = Graph(
            id: "pipeline",
            nodes: [
                "start": Node(id: "start", shape: "Mdiamond"),
                "codegen": node,
                "done": Node(id: "done", shape: "Msquare"),
            ],
            edges: [Edge(from: "start", to: "codegen"), Edge(from: "codegen", to: "done")],
            attributes: GraphAttributes(
                toolHooksPre: "\(python) -c \"import sys; sys.stderr.write('x'*200000); print('hook-ok')\""
            )
        )
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await withAsyncTimeout(seconds: 5, label: "codergen pre-hook") {
            try await handler.execute(node: node, context: ctx, graph: graph, logsRoot: logsRoot)
        }
        XCTAssertEqual(outcome.status, .success)
        XCTAssertEqual(ctx.getString("hook_pre_output"), "hook-ok")
    }

    // MARK: - ManagerLoop Handler

    @Test
    func testManagerLoopHandlerFailsWithoutDotfile() async throws {
        let backend = MockCodergenBackend()
        let handler = ManagerLoopHandler(backend: backend)
        let node = Node(id: "manager", shape: "house")
        let ctx = PipelineContext()
        let graph = Graph()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let outcome = try await handler.execute(node: node, context: ctx, graph: graph, logsRoot: logsRoot)
        XCTAssertEqual(outcome.status, .fail, "Should fail without stack.child_dotfile")
    }

    // MARK: - Accelerator Key Parsing (via WaitHuman)

    @Test
    func testWaitHumanAcceleratorKeyParsing() async throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            ask [shape=hexagon, prompt="Choose"]
            a [shape=box]
            b [shape=box]
            c [shape=box]
            d [shape=box]
            done [shape=Msquare]

            start -> ask
            ask -> a [label="[Y] Yes"]
            ask -> b [label="N) No"]
            ask -> c [label="M - Maybe"]
            ask -> d [label="Plain label"]
            a -> done
            b -> done
            c -> done
            d -> done
        }
        """
        let graph = try DOTParser.parse(dot)
        let askNode = graph.node("ask")!

        // We just need to check the question options have correct keys
        let recorder = RecordingInterviewer(wrapping: AutoApproveInterviewer())
        let handler = WaitHumanHandler(interviewer: recorder)
        let ctx = PipelineContext()
        let logsRoot = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)

        let _ = try await handler.execute(node: askNode, context: ctx, graph: graph, logsRoot: logsRoot)

        let recordings = await recorder.recordings
        XCTAssertEqual(recordings.count, 1)
        let options = recordings[0].0.options
        XCTAssertEqual(options.count, 4)

        // [Y] Yes -> key "Y"
        XCTAssertEqual(options[0].key, "Y")
        // N) No -> key "N"
        XCTAssertEqual(options[1].key, "N")
        // M - Maybe -> key "M"
        XCTAssertEqual(options[2].key, "M")
        // Plain label -> first char "P"
        XCTAssertEqual(options[3].key, "P")
    }

    // MARK: - Condition Parser Edge Cases

    @Test
    func testConditionParserQuotedValues() throws {
        let expr = try ConditionParser.parse("outcome=\"success\"")
        let ctx = PipelineContext()
        XCTAssertTrue(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))
    }

    @Test
    func testConditionParserMultipleClauses() throws {
        let expr = try ConditionParser.parse("outcome=success && preferred_label=deploy && context.env=prod")
        XCTAssertEqual(expr.clauses.count, 3)
        XCTAssertEqual(expr.clauses[0].key, "outcome")
        XCTAssertEqual(expr.clauses[1].key, "preferred_label")
        XCTAssertEqual(expr.clauses[2].key, "context.env")
    }

    // MARK: - Validation: Full Valid Pipeline

    @Test
    func testValidateFullValidPipeline() throws {
        let dot = """
        digraph pipeline {
            graph [goal="Test pipeline"]
            start [shape=Mdiamond]
            task1 [shape=box, prompt="Do work"]
            done [shape=Msquare]
            start -> task1 -> done
        }
        """
        let graph = try DOTParser.parse(dot)
        let diagnostics = PipelineValidator.validate(graph)
        let errors = diagnostics.filter { $0.isError }
        XCTAssertEqual(errors.count, 0, "A valid pipeline should have no errors: \(errors.map(\.message))")
    }

    // MARK: - auto_status Tests

    @Test
    func testEngineAutoStatusConvertsFailToSuccess() async throws {
        // Backend returns FAIL, but node has auto_status=true
        let backend = MockCodergenBackend(result: CodergenResult(
            response: "failed",
            status: .fail,
            notes: "Something went wrong"
        ))
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            task [shape=box, prompt="Do task", auto_status=true]
            done [shape=Msquare]
            start -> task -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success, "auto_status=true should convert FAIL to SUCCESS")
        XCTAssertEqual(result.nodeOutcomes["task"], .success)
        XCTAssertTrue(result.completedNodes.contains("done"),
                      "Pipeline should reach done after auto_status converts fail to success")

        try? FileManager.default.removeItem(at: logsRoot)
    }

    @Test
    func testEngineAutoStatusDoesNotAffectRealSuccess() async throws {
        // Backend returns SUCCESS; auto_status=true should not change anything
        let backend = MockCodergenBackend(result: CodergenResult(
            response: "ok",
            status: .success
        ))
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            task [shape=box, prompt="Do task", auto_status=true]
            done [shape=Msquare]
            start -> task -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success)
        XCTAssertEqual(result.nodeOutcomes["task"], .success)

        try? FileManager.default.removeItem(at: logsRoot)
    }

    // MARK: - allow_partial Tests

    @Test
    func testEngineAllowPartialAcceptsPartialSuccess() async throws {
        // Backend returns RETRY, node has allow_partial=true and max_retries=1.
        // After retry budget is exhausted, allow_partial converts RETRY -> PARTIAL_SUCCESS.
        let backend = MockCodergenBackend(result: CodergenResult(
            response: "retry",
            status: .retry,
            notes: "persistent retry request"
        ))
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            task [shape=box, prompt="Flaky task", allow_partial=true, max_retries=1]
            done [shape=Msquare]
            start -> task -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: PipelineRetryPolicy(strategy: .none),
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        // allow_partial converts the final RETRY to PARTIAL_SUCCESS
        XCTAssertEqual(result.nodeOutcomes["task"], .partialSuccess,
                       "allow_partial=true should convert exhausted RETRY to PARTIAL_SUCCESS")
        XCTAssertEqual(result.status, .success,
                       "Pipeline should succeed since partial_success is not a failure")

        try? FileManager.default.removeItem(at: logsRoot)
    }

    // MARK: - Bare Key Truthiness Tests

    @Test
    func testConditionBareKeyTruthy() throws {
        let expr = try ConditionParser.parse("context.feature_enabled")
        let ctx = PipelineContext()

        // Not set => empty => falsy
        XCTAssertFalse(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))

        // Set to "true" => truthy
        ctx.set("feature_enabled", "true")
        XCTAssertTrue(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))

        // Set to "false" => falsy
        ctx.set("feature_enabled", "false")
        XCTAssertFalse(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))

        // Set to "0" => falsy
        ctx.set("feature_enabled", "0")
        XCTAssertFalse(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))

        // Set to non-empty string => truthy
        ctx.set("feature_enabled", "yes")
        XCTAssertTrue(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))
    }

    @Test
    func testConditionBareKeyWithAnd() throws {
        let expr = try ConditionParser.parse("outcome=success && context.ready")
        let ctx = PipelineContext()

        // outcome=success but context.ready is empty => false
        XCTAssertFalse(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))

        // Both true
        ctx.set("ready", "1")
        XCTAssertTrue(expr.evaluate(outcome: "success", preferredLabel: "", context: ctx))

        // outcome=fail but context.ready is truthy
        XCTAssertFalse(expr.evaluate(outcome: "fail", preferredLabel: "", context: ctx))
    }

    // MARK: - Duration Parsing Tests

    @Test
    func testParseDurationQuotedString() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            task1 [shape=box, timeout="900s"]
            task2 [shape=box, timeout="5m"]
            task3 [shape=box, timeout="250ms"]
            done [shape=Msquare]
            start -> task1 -> task2 -> task3 -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        let task1 = graph.node("task1")
        XCTAssertNotNil(task1?.timeout, "timeout='900s' should parse to a Duration")

        let task2 = graph.node("task2")
        XCTAssertNotNil(task2?.timeout, "timeout='5m' should parse to a Duration")

        let task3 = graph.node("task3")
        XCTAssertNotNil(task3?.timeout, "timeout='250ms' should parse to a Duration")
    }

    // MARK: - Subgraph Class Derivation Tests

    @Test
    func testSubgraphClassDerivation() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            done [shape=Msquare]

            subgraph cluster_loop {
                label = "Loop A"
                plan [shape=box, prompt="Plan"]
                implement [shape=box, prompt="Implement"]
            }

            start -> plan -> implement -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        let plan = graph.node("plan")
        XCTAssertNotNil(plan)
        XCTAssertTrue(plan?.cssClass.contains("loop-a") ?? false,
                      "Plan should inherit class 'loop-a' from subgraph label 'Loop A', got: \(plan?.cssClass ?? "")")

        let implement = graph.node("implement")
        XCTAssertNotNil(implement)
        XCTAssertTrue(implement?.cssClass.contains("loop-a") ?? false,
                      "Implement should inherit class 'loop-a' from subgraph label 'Loop A', got: \(implement?.cssClass ?? "")")
    }

    // MARK: - Checkpoint with nodeOutcomes Tests

    @Test
    func testCheckpointRoundTripWithOutcomes() throws {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-checkpoint-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }

        let checkpoint = Checkpoint(
            timestamp: Date(),
            currentNode: "review",
            completedNodes: ["start", "plan", "implement"],
            nodeRetries: ["plan": 1],
            nodeOutcomes: ["start": "success", "plan": "success", "implement": "partial_success"],
            contextValues: ["graph.goal": "Test"],
            logs: ["[start] success"]
        )

        let url = dir.appendingPathComponent("checkpoint.json")
        try checkpoint.save(to: url)
        let loaded = try Checkpoint.load(from: url)

        XCTAssertEqual(loaded.nodeOutcomes["implement"], "partial_success")
        XCTAssertEqual(loaded.nodeOutcomes["start"], "success")
        XCTAssertEqual(loaded.nodeOutcomes.count, 3)
    }

    // MARK: - Outcome Model

    @Test
    func testOutcomeToStatusJSON() throws {
        let outcome = Outcome(
            status: .success,
            preferredLabel: "next",
            contextUpdates: ["key": "val"],
            notes: "All good"
        )
        let json = outcome.toStatusJSON()
        XCTAssertEqual(json["outcome"] as? String, "success")
        XCTAssertEqual(json["preferred_next_label"] as? String, "next")
        XCTAssertEqual(json["notes"] as? String, "All good")
    }

    // MARK: - Context Serialization

    @Test
    func testContextSerializableSnapshot() throws {
        let ctx = PipelineContext()
        ctx.set("name", "test")
        ctx.set("count", 5)
        ctx.set("flag", true)

        let snapshot = ctx.serializableSnapshot()
        XCTAssertEqual(snapshot["name"], "test")
        XCTAssertEqual(snapshot["count"], "5")
        XCTAssertEqual(snapshot["flag"], "true")
    }

    // MARK: - Fidelity Parsing

    @Test
    func testFidelityParsing() throws {
        XCTAssertEqual(ContextFidelity.parse("full"), .full)
        XCTAssertEqual(ContextFidelity.parse("truncate"), .truncate)
        XCTAssertEqual(ContextFidelity.parse("compact"), .compact)
        XCTAssertEqual(ContextFidelity.parse("summary:low"), .summaryLow)
        XCTAssertEqual(ContextFidelity.parse("summary:medium"), .summaryMedium)
        XCTAssertEqual(ContextFidelity.parse("summary:high"), .summaryHigh)
        XCTAssertNil(ContextFidelity.parse(""))
        XCTAssertNil(ContextFidelity.parse("invalid"))
    }

    @Test
    func testFidelityResolution() throws {
        // Edge > Node > Graph
        XCTAssertEqual(ContextFidelity.resolve(edgeFidelity: "full", nodeFidelity: "compact", graphDefault: "truncate"), .full)
        XCTAssertEqual(ContextFidelity.resolve(edgeFidelity: "", nodeFidelity: "compact", graphDefault: "truncate"), .compact)
        XCTAssertEqual(ContextFidelity.resolve(edgeFidelity: "", nodeFidelity: "", graphDefault: "truncate"), .truncate)
        XCTAssertEqual(ContextFidelity.resolve(edgeFidelity: "", nodeFidelity: "", graphDefault: ""), .compact)
    }

    // MARK: - DOT Parser Edge Cases

    @Test
    func testParseEdgeChain() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            a [shape=box]
            b [shape=box]
            c [shape=box]
            done [shape=Msquare]
            start -> a -> b -> c -> done
        }
        """
        let graph = try DOTParser.parse(dot)
        XCTAssertEqual(graph.edges.count, 4)
        XCTAssertNotNil(graph.edges.first { $0.from == "start" && $0.to == "a" })
        XCTAssertNotNil(graph.edges.first { $0.from == "a" && $0.to == "b" })
        XCTAssertNotNil(graph.edges.first { $0.from == "b" && $0.to == "c" })
        XCTAssertNotNil(graph.edges.first { $0.from == "c" && $0.to == "done" })
    }

    @Test
    func testParseCommentsIgnored() throws {
        let dot = """
        // This is a comment
        digraph pipeline {
            /* Block comment */
            start [shape=Mdiamond]
            done [shape=Msquare]
            start -> done // inline comment
        }
        """
        let graph = try DOTParser.parse(dot)
        XCTAssertEqual(graph.nodes.count, 2)
        XCTAssertEqual(graph.edges.count, 1)
    }

    @Test
    func testParseDurationAttributes() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            task1 [shape=box, timeout=30s]
            done [shape=Msquare]
            start -> task1 -> done
        }
        """
        let graph = try DOTParser.parse(dot)
        let task1 = graph.node("task1")
        XCTAssertNotNil(task1?.timeout)
    }

    // MARK: - Pipeline Engine: Basic End-to-End

    @Test
    func testEngineSimpleLinearPipeline() async throws {
        let dot = """
        digraph test {
            graph [goal="Test basic pipeline"]
            start [shape=Mdiamond]
            A [shape=box, prompt="Do A"]
            B [shape=box, prompt="Do B"]
            done [shape=Msquare]
            start -> A -> B -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let backend = MockCodergenBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success)
        XCTAssertTrue(result.completedNodes.contains("start"))
        XCTAssertTrue(result.completedNodes.contains("A"))
        XCTAssertTrue(result.completedNodes.contains("B"))
        XCTAssertTrue(result.completedNodes.contains("done"))
        XCTAssertEqual(result.nodeOutcomes["A"], .success)
        XCTAssertEqual(result.nodeOutcomes["B"], .success)
    }

    @Test
    func testEngineConditionalBranching() async throws {
        let dot = """
        digraph test {
            graph [goal="Test branching"]
            start [shape=Mdiamond]
            task [shape=box, prompt="Do task"]
            decide [shape=diamond]
            pathA [shape=box, prompt="Path A work"]
            pathB [shape=box, prompt="Path B work"]
            done [shape=Msquare]

            start -> task -> decide
            decide -> pathA [condition="outcome=success"]
            decide -> pathB [condition="outcome=fail"]
            pathA -> done
            pathB -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let backend = MockCodergenBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success)
        // The conditional handler returns success, and the condition "outcome=success"
        // should route to pathA
        XCTAssertTrue(result.completedNodes.contains("pathA"))
        XCTAssertFalse(result.completedNodes.contains("pathB"))
    }

    @Test
    func testEngineContextPassthrough() async throws {
        // Use a backend that sets context updates
        let backend = MockCodergenBackend(result: CodergenResult(
            response: "done",
            status: .success,
            contextUpdates: ["generated_value": "42"]
        ))
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            A [shape=box, prompt="Generate"]
            B [shape=box, prompt="Consume"]
            done [shape=Msquare]
            start -> A -> B -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success)
        XCTAssertEqual(result.context["generated_value"], "42")
    }

    @Test
    func testEngineGoalVariableExpansion() async throws {
        let dot = """
        digraph test {
            graph [goal="Build something"]
            start [shape=Mdiamond]
            task [shape=box, prompt="Your goal is: $goal"]
            done [shape=Msquare]
            start -> task -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let backend = RecordingMockBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success)
        // The prompt should have $goal replaced
        XCTAssertTrue(backend.prompts.contains { $0.contains("Build something") },
                      "Expected expanded goal in prompts: \(backend.prompts)")
    }

    @Test
    func testEngineWritesLogFiles() async throws {
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            task [shape=box, prompt="Do work"]
            done [shape=Msquare]
            start -> task -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let backend = MockCodergenBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success)

        // Check that manifest.json was created
        let manifestURL = logsRoot.appendingPathComponent("manifest.json")
        XCTAssertTrue(FileManager.default.fileExists(atPath: manifestURL.path),
                      "manifest.json should exist at \(manifestURL.path)")

        // Check that checkpoint.json was created
        let checkpointURL = logsRoot.appendingPathComponent("checkpoint.json")
        XCTAssertTrue(FileManager.default.fileExists(atPath: checkpointURL.path),
                      "checkpoint.json should exist")

        // Check that status.json was written for the task node
        let taskStatusURL = logsRoot.appendingPathComponent("task/status.json")
        XCTAssertTrue(FileManager.default.fileExists(atPath: taskStatusURL.path),
                      "task/status.json should exist")

        // Clean up
        try? FileManager.default.removeItem(at: logsRoot)
    }

    @Test
    func testEngineWithHumanInTheLoop() async throws {
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            ask [shape=hexagon, prompt="Choose path"]
            pathA [shape=box, prompt="Do A"]
            pathB [shape=box, prompt="Do B"]
            done [shape=Msquare]

            start -> ask
            ask -> pathA [label="Go A"]
            ask -> pathB [label="Go B"]
            pathA -> done
            pathB -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let backend = MockCodergenBackend()
        // QueueInterviewer: select the second option "Go B"
        let interviewer = QueueInterviewer(answers: [
            .option(InterviewOption(key: "G", label: "Go B"))
        ])
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend,
            interviewer: interviewer
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success)
        // The preferred label "Go B" should route to pathB
        XCTAssertTrue(result.completedNodes.contains("pathB"),
                      "Expected pathB in completed nodes: \(result.completedNodes)")
        XCTAssertFalse(result.completedNodes.contains("pathA"))

        try? FileManager.default.removeItem(at: logsRoot)
    }

    @Test
    func testEngineFailureTerminatesPipeline() async throws {
        let backend = MockCodergenBackend(result: CodergenResult(
            response: "failed",
            status: .fail,
            notes: "Something went wrong"
        ))
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            task [shape=box, prompt="Fail here"]
            done [shape=Msquare]
            start -> task -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .fail)
        XCTAssertTrue(result.completedNodes.contains("task"))
        XCTAssertFalse(result.completedNodes.contains("done"),
                       "done should not be reached after failure")

        try? FileManager.default.removeItem(at: logsRoot)
    }

    @Test
    func testEngineRunFromDOTString() async throws {
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            done [shape=Msquare]
            start -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let backend = MockCodergenBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success)
        XCTAssertTrue(result.completedNodes.contains("start"))
        XCTAssertTrue(result.completedNodes.contains("done"))

        try? FileManager.default.removeItem(at: logsRoot)
    }

    @Test
    func testEngineRejectsInvalidGraph() async throws {
        // No start node
        let dot = """
        digraph test {
            task [shape=box]
            done [shape=Msquare]
            task -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let backend = MockCodergenBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)

        do {
            let _ = try await engine.run(dot: dot)
            XCTFail("Should have thrown validation error")
        } catch {
            // Expected
            XCTAssertTrue(error is AttractorError)
        }

        try? FileManager.default.removeItem(at: logsRoot)
    }

    @Test
    func testEngineEdgeSelectionByWeight() async throws {
        // Two unconditional edges from a diamond node; higher weight should win
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            decide [shape=diamond]
            heavy [shape=box, prompt="Heavy path"]
            light [shape=box, prompt="Light path"]
            done [shape=Msquare]

            start -> decide
            decide -> light [weight=1]
            decide -> heavy [weight=10]
            heavy -> done
            light -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let backend = MockCodergenBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success)
        XCTAssertTrue(result.completedNodes.contains("heavy"),
                      "Expected heavy (weight=10) to be selected: \(result.completedNodes)")

        try? FileManager.default.removeItem(at: logsRoot)
    }

    @Test
    func testEnginePreferredLabelRouting() async throws {
        // Backend returns preferredLabel that should route to the matching edge
        let backend = MockCodergenBackend(result: CodergenResult(
            response: "chose path B",
            status: .success,
            preferredLabel: "Go B"
        ))
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            task [shape=box, prompt="Choose"]
            pathA [shape=box, prompt="A"]
            pathB [shape=box, prompt="B"]
            done [shape=Msquare]

            start -> task
            task -> pathA [label="Go A"]
            task -> pathB [label="Go B"]
            pathA -> done
            pathB -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success)
        XCTAssertTrue(result.completedNodes.contains("pathB"),
                      "Expected pathB via preferredLabel routing: \(result.completedNodes)")
        XCTAssertFalse(result.completedNodes.contains("pathA"))

        try? FileManager.default.removeItem(at: logsRoot)
    }

    @Test
    func testEngineCheckpointSerialization() async throws {
        let dot = """
        digraph test {
            start [shape=Mdiamond]
            task [shape=box, prompt="Work"]
            done [shape=Msquare]
            start -> task -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let backend = MockCodergenBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let _ = try await engine.run(dot: dot)

        // Load and verify checkpoint
        let checkpointURL = logsRoot.appendingPathComponent("checkpoint.json")
        let checkpoint = try Checkpoint.load(from: checkpointURL)
        XCTAssertTrue(checkpoint.completedNodes.contains("task"))

        try? FileManager.default.removeItem(at: logsRoot)
    }

    @Test
    func testEngineStylesheetApplied() async throws {
        let dot = """
        digraph test {
            graph [model_stylesheet="* { llm_model: test-model; }"]
            start [shape=Mdiamond]
            task [shape=box, prompt="Work"]
            done [shape=Msquare]
            start -> task -> done
        }
        """
        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        let backend = RecordingMockBackend()
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)
        let result = try await engine.run(dot: dot)

        XCTAssertEqual(result.status, .success)
        // The model should have been set by the stylesheet
        XCTAssertTrue(backend.models.contains("test-model"),
                      "Expected stylesheet model applied: \(backend.models)")

        try? FileManager.default.removeItem(at: logsRoot)
    }

    @Test
    func testGraphRetryPolicyOverridesEngineConfig() async throws {
        let dot = """
        digraph test {
            graph [
                retry_policy.strategy="linear",
                retry_policy.base_delay=0.12,
                retry_policy.max_delay=0.12,
                retry_policy.jitter=false
            ]
            start [shape=Mdiamond]
            task [shape=box, prompt="Flaky", allow_partial=true, max_retries=1]
            done [shape=Msquare]
            start -> task -> done
        }
        """

        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: logsRoot) }

        let backend = MockCodergenBackend(result: CodergenResult(response: "retry", status: .retry))
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)

        let start = ContinuousClock.now
        let result = try await engine.run(dot: dot)
        let elapsed = durationSeconds(ContinuousClock.now - start)

        XCTAssertEqual(result.status, .success)
        XCTAssertEqual(result.nodeOutcomes["task"], .partialSuccess)
        XCTAssertGreaterThanOrEqual(
            elapsed,
            0.10,
            "Graph retry_policy override should add measurable backoff delay, elapsed=\(elapsed)"
        )
    }

    @Test
    func testNodeRetryPolicyOverridesGraphRetryPolicy() async throws {
        let dot = """
        digraph test {
            graph [
                retry_policy.strategy="linear",
                retry_policy.base_delay=0.30,
                retry_policy.max_delay=0.30,
                retry_policy.jitter=false
            ]
            start [shape=Mdiamond]
            task [
                shape=box,
                prompt="Flaky",
                allow_partial=true,
                max_retries=1,
                retry_policy.strategy="none"
            ]
            done [shape=Msquare]
            start -> task -> done
        }
        """

        let logsRoot = FileManager.default.temporaryDirectory
            .appendingPathComponent("attractor-test-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: logsRoot) }

        let backend = MockCodergenBackend(result: CodergenResult(response: "retry", status: .retry))
        let config = PipelineConfig(
            logsRoot: logsRoot,
            retryPolicy: .none,
            backend: backend
        )
        let engine = PipelineEngine(config: config)

        let start = ContinuousClock.now
        let result = try await engine.run(dot: dot)
        let elapsed = durationSeconds(ContinuousClock.now - start)

        XCTAssertEqual(result.status, .success)
        XCTAssertEqual(result.nodeOutcomes["task"], .partialSuccess)
        XCTAssertLessThan(
            elapsed,
            0.20,
            "Node retry_policy override should disable graph backoff, elapsed=\(elapsed)"
        )
    }

    private func durationSeconds(_ duration: Duration) -> Double {
        Double(duration.components.seconds) + Double(duration.components.attoseconds) / 1e18
    }
}

// MARK: - Test Helpers

final class MockCodergenBackend: CodergenBackend, @unchecked Sendable {
    var lastPrompt: String = ""
    var resultToReturn: CodergenResult

    init(result: CodergenResult = CodergenResult(response: "mock response", status: .success)) {
        self.resultToReturn = result
    }

    func run(
        prompt: String,
        model: String,
        provider: String,
        reasoningEffort: String,
        context: PipelineContext
    ) async throws -> CodergenResult {
        lastPrompt = prompt
        return resultToReturn
    }
}

/// A mock backend that records all calls for verification.
final class RecordingMockBackend: CodergenBackend, @unchecked Sendable {
    private let lock = NSLock()
    private var _prompts: [String] = []
    private var _models: [String] = []
    var resultToReturn: CodergenResult

    var prompts: [String] {
        lock.lock()
        defer { lock.unlock() }
        return _prompts
    }

    var models: [String] {
        lock.lock()
        defer { lock.unlock() }
        return _models
    }

    init(result: CodergenResult = CodergenResult(response: "recorded response", status: .success)) {
        self.resultToReturn = result
    }

    func run(
        prompt: String,
        model: String,
        provider: String,
        reasoningEffort: String,
        context: PipelineContext
    ) async throws -> CodergenResult {
        record(prompt: prompt, model: model)
        return resultToReturn
    }

    private func record(prompt: String, model: String) {
        lock.lock()
        _prompts.append(prompt)
        _models.append(model)
        lock.unlock()
    }
}


private func findPython3ForAttractorTests() throws -> String {
    let candidates = ["/usr/bin/python3", "/opt/homebrew/bin/python3", "/usr/local/bin/python3"]
    for candidate in candidates where FileManager.default.isExecutableFile(atPath: candidate) {
        return candidate
    }
    throw NSError(domain: "AttractorTests", code: 3, userInfo: [NSLocalizedDescriptionKey: "python3 not found"])
}
