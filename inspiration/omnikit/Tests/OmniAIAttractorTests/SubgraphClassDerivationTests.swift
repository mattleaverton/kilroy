import Testing
import Foundation
@testable import OmniAIAttractor

@Suite
final class SubgraphClassDerivationTests {

    // MARK: - Subgraph label -> class derivation

    @Test
    func testSubgraphLabelDerivesClass() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            done [shape=Msquare]

            subgraph cluster_loop {
                label = "Loop A"
                node [thread_id="loop-a"]

                plan [label="Plan next step"]
                implement [label="Implement"]
            }

            start -> plan -> implement -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        let plan = graph.node("plan")
        XCTAssertNotNil(plan)
        // Spec: label "Loop A" -> class "loop-a" (lowercased, spaces to hyphens)
        XCTAssertTrue(plan!.cssClass.contains("loop-a"),
            "Expected class 'loop-a' derived from subgraph label 'Loop A', got: '\(plan!.cssClass)'")

        let implement = graph.node("implement")
        XCTAssertNotNil(implement)
        XCTAssertTrue(implement!.cssClass.contains("loop-a"),
            "Expected class 'loop-a' on implement node, got: '\(implement!.cssClass)'")
    }

    @Test
    func testSubgraphClassParticipatesInStylesheet() throws {
        let dot = """
        digraph pipeline {
            graph [model_stylesheet=".loop-a { llm_model: fast-model; }"]
            start [shape=Mdiamond]
            done [shape=Msquare]

            subgraph cluster_loop {
                label = "Loop A"
                plan [label="Plan"]
                implement [label="Implement"]
            }

            start -> plan -> implement -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        // Apply stylesheet transform
        let transformed = StylesheetTransform().apply(graph)

        let plan = transformed.node("plan")
        XCTAssertNotNil(plan)
        XCTAssertEqual(plan!.llmModel, "fast-model",
            "Stylesheet should apply via derived subgraph class 'loop-a'")

        let implement = transformed.node("implement")
        XCTAssertNotNil(implement)
        XCTAssertEqual(implement!.llmModel, "fast-model",
            "Stylesheet should apply to all nodes in the subgraph")
    }

    @Test
    func testSubgraphClassDerivationSpecCompliant() throws {
        // Spec: "lowercasing the label, replacing spaces with hyphens,
        // and stripping non-alphanumeric characters (except hyphens)"
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            done [shape=Msquare]

            subgraph cluster_test {
                label = "My Complex! Label 123"
                task1 [shape=box]
            }

            start -> task1 -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        let task1 = graph.node("task1")
        XCTAssertNotNil(task1)
        // "My Complex! Label 123" -> "my-complex-label-123"
        // (lowercased, spaces->hyphens, stripped '!')
        XCTAssertTrue(task1!.cssClass.contains("my-complex-label-123"),
            "Expected derived class 'my-complex-label-123', got: '\(task1!.cssClass)'")
    }

    @Test
    func testSubgraphDoesNotOverrideExplicitClass() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            done [shape=Msquare]

            subgraph cluster_loop {
                label = "Loop A"
                task1 [shape=box, class="custom"]
            }

            start -> task1 -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        let task1 = graph.node("task1")
        XCTAssertNotNil(task1)
        // Explicit class "custom" + derived class "loop-a" should be merged
        XCTAssertTrue(task1!.cssClass.contains("custom"),
            "Explicit class should be preserved")
        XCTAssertTrue(task1!.cssClass.contains("loop-a"),
            "Derived class should be appended")
    }

    @Test
    func testSubgraphScopeDefaults() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            done [shape=Msquare]

            subgraph cluster_loop {
                label = "Loop A"
                node [timeout="900s"]

                plan [label="Plan next step"]
                implement [label="Implement", timeout="1800s"]
            }

            start -> plan -> implement -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        let plan = graph.node("plan")
        XCTAssertNotNil(plan)
        // Plan should inherit timeout=900s from subgraph defaults
        XCTAssertNotNil(plan!.timeout, "Plan should inherit timeout from subgraph defaults")

        let implement = graph.node("implement")
        XCTAssertNotNil(implement)
        // Implement should override timeout to 1800s
        XCTAssertNotNil(implement!.timeout, "Implement should have timeout")
    }

    @Test
    func testSubgraphDefaultsDoNotLeakOutside() throws {
        let dot = """
        digraph pipeline {
            start [shape=Mdiamond]
            done [shape=Msquare]

            subgraph cluster_inner {
                node [timeout="900s"]
                inner_task [shape=box, label="Inner"]
            }

            outer_task [shape=box, label="Outer"]
            start -> inner_task -> outer_task -> done
        }
        """
        let graph = try DOTParser.parse(dot)

        let inner = graph.node("inner_task")
        XCTAssertNotNil(inner)
        XCTAssertNotNil(inner!.timeout, "Inner task should have timeout from subgraph defaults")

        let outer = graph.node("outer_task")
        XCTAssertNotNil(outer)
        XCTAssertNil(outer!.timeout, "Outer task should NOT inherit subgraph defaults")
    }
}
