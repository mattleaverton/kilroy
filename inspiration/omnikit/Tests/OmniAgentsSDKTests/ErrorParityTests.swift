import Testing
import OmniAgentsSDK

struct ErrorParityTests {
    @Test
    func pretty_print_run_error_details_matches_python_shape() {
        let details = RunErrorDetails(
            input: "hello",
            newItems: ["a", "b"],
            rawResponses: ["r1"],
            lastAgent: AnyAgent(erasing: NamedAgent(name: "planner"), name: "planner"),
            contextWrapper: nil,
            inputGuardrailResults: ["g1"],
            outputGuardrailResults: ["g2"]
        )

        let rendered = prettyPrintRunErrorDetails(details)
        #expect(rendered.contains("RunErrorDetails:"))
        #expect(rendered.contains("Agent(name=\"planner\", ...)"))
        #expect(rendered.contains("- 2 new item(s)"))
        #expect(rendered.contains("- 1 raw response(s)"))
        #expect(rendered.contains("- 1 input guardrail result(s)"))
        #expect(!rendered.contains("output guardrail"))
    }
}

private struct NamedAgent {
    let name: String
}
