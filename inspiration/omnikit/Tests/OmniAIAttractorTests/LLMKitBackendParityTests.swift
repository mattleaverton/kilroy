import Testing
import Foundation
@testable import OmniAIAttractor
import OmniAICore

@Suite
final class LLMKitBackendParityTests {
    @Test
    func testOpenAIProviderOptionsDefaultToWebSocket() {
        let backend = LLMKitBackend()
        let options = backend.providerOptions(for: "openai", environment: [:])
        let openAI = options?["openai"]?.objectValue

        XCTAssertEqual(openAI?[OpenAIProviderOptionKeys.responsesTransport]?.stringValue, "websocket")
        XCTAssertNil(openAI?[OpenAIProviderOptionKeys.websocketBaseURL])
    }

    @Test
    func testOpenAIProviderOptionsRespectWebSocketBaseOverride() {
        let backend = LLMKitBackend()
        let options = backend.providerOptions(
            for: "openai",
            environment: ["OPENAI_WEBSOCKET_BASE_URL": "https://api.openai.com/v1"]
        )
        let openAI = options?["openai"]?.objectValue

        XCTAssertEqual(openAI?[OpenAIProviderOptionKeys.responsesTransport]?.stringValue, "websocket")
        XCTAssertEqual(openAI?[OpenAIProviderOptionKeys.websocketBaseURL]?.stringValue, "https://api.openai.com/v1")
    }

    @Test
    func testNonOpenAIProviderOptionsAreNil() {
        let backend = LLMKitBackend()
        XCTAssertNil(backend.providerOptions(for: "anthropic", environment: [:]))
        XCTAssertNil(backend.providerOptions(for: "gemini", environment: [:]))
    }

    @Test
    func testInactivityTimeoutPriorityNodeOverEnvOverDefault() {
        let backend = LLMKitBackend()
        let context = PipelineContext()

        let defaultValue = backend.resolveInactivityTimeoutSeconds(from: context, environment: [:])
        XCTAssertEqual(defaultValue, 300)

        let envValue = backend.resolveInactivityTimeoutSeconds(
            from: context,
            environment: ["ATTRACTOR_LLM_INACTIVITY_TIMEOUT_SECONDS": "123"]
        )
        XCTAssertEqual(envValue, 123)

        context.set("_current_node_timeout", "42")
        let nodeValue = backend.resolveInactivityTimeoutSeconds(
            from: context,
            environment: ["ATTRACTOR_LLM_INACTIVITY_TIMEOUT_SECONDS": "123"]
        )
        XCTAssertEqual(nodeValue, 42)
    }

    @Test
    func testInactivityActivityEventsMatchParityContract() {
        XCTAssertTrue(LLMKitBackend.isActivityEvent(StreamEvent(type: .textStart)))
        XCTAssertTrue(LLMKitBackend.isActivityEvent(StreamEvent(type: .textDelta)))
        XCTAssertTrue(LLMKitBackend.isActivityEvent(StreamEvent(type: .reasoningDelta)))
        XCTAssertTrue(LLMKitBackend.isActivityEvent(StreamEvent(type: .toolCallStart)))
        XCTAssertTrue(LLMKitBackend.isActivityEvent(StreamEvent(type: .toolCallDelta)))
        XCTAssertTrue(LLMKitBackend.isActivityEvent(StreamEvent(type: .toolCallEnd)))
        XCTAssertTrue(LLMKitBackend.isActivityEvent(StreamEvent(type: .finish)))

        XCTAssertFalse(LLMKitBackend.isActivityEvent(StreamEvent(type: .providerEvent)))
        XCTAssertFalse(LLMKitBackend.isActivityEvent(StreamEvent(type: .textEnd)))
        XCTAssertFalse(LLMKitBackend.isActivityEvent(StreamEvent(type: .reasoningEnd)))
        XCTAssertFalse(LLMKitBackend.isActivityEvent(StreamEvent(type: .error)))
    }
}
