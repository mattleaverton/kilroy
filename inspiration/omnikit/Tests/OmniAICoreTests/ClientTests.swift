import Testing
import Foundation

@testable import OmniAICore

private actor OrderRecorder {
    private var events: [String] = []
    func add(_ e: String) { events.append(e) }
    func all() -> [String] { events }
}

private struct RecordingMiddleware: Middleware {
    let name: String
    let recorder: OrderRecorder

    func complete(request: Request, next: @Sendable @escaping (Request) async throws -> Response) async throws -> Response {
        await recorder.add("\(name).in")
        let resp = try await next(request)
        await recorder.add("\(name).out")
        return resp
    }

    func stream(
        request: Request,
        next: @Sendable @escaping (Request) async throws -> AsyncThrowingStream<StreamEvent, Error>
    ) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        await recorder.add("\(name).in")
        let s = try await next(request)
        return AsyncThrowingStream { continuation in
            Task {
                do {
                    for try await ev in s {
                        continuation.yield(ev)
                    }
                    await recorder.add("\(name).out")
                    continuation.finish()
                } catch {
                    await recorder.add("\(name).out")
                    continuation.finish(throwing: error)
                }
            }
        }
    }
}

private final class StaticAdapter: ProviderAdapter, @unchecked Sendable {
    let name: String
    let response: Response
    init(name: String, response: Response) {
        self.name = name
        self.response = response
    }
    func complete(request: Request) async throws -> Response { response }
    func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        AsyncThrowingStream { continuation in
            continuation.yield(StreamEvent(type: .standard(.streamStart)))
            continuation.yield(StreamEvent(type: .standard(.finish), finishReason: response.finishReason, usage: response.usage, response: response))
            continuation.finish()
        }
    }
}

@Suite
final class ClientTests {
    @Test
    func testMiddlewareOrderOnionPattern() async throws {
        let recorder = OrderRecorder()
        let m1 = RecordingMiddleware(name: "A", recorder: recorder)
        let m2 = RecordingMiddleware(name: "B", recorder: recorder)

        let resp = Response(
            id: "r",
            model: "m",
            provider: "p",
            message: .assistant("ok"),
            finishReason: FinishReason(kind: .stop, raw: "stop"),
            usage: Usage(inputTokens: 1, outputTokens: 1)
        )

        let client = try Client(
            providers: ["p": StaticAdapter(name: "p", response: resp)],
            defaultProvider: "p",
            middleware: [m1, m2]
        )

        _ = try await client.complete(Request(model: "m", messages: [.user("hi")], provider: "p"))
        let events = await recorder.all()
        XCTAssertEqual(events, ["A.in", "B.in", "B.out", "A.out"])
    }

    @Test
    func testProviderRoutingAndDefaultProvider() async throws {
        let r1 = Response(
            id: "r1",
            model: "m",
            provider: "p1",
            message: .assistant("one"),
            finishReason: FinishReason(kind: .stop, raw: "stop"),
            usage: Usage(inputTokens: 1, outputTokens: 1)
        )
        let r2 = Response(
            id: "r2",
            model: "m",
            provider: "p2",
            message: .assistant("two"),
            finishReason: FinishReason(kind: .stop, raw: "stop"),
            usage: Usage(inputTokens: 1, outputTokens: 1)
        )

        let client = try Client(
            providers: ["p1": StaticAdapter(name: "p1", response: r1), "p2": StaticAdapter(name: "p2", response: r2)],
            defaultProvider: "p1"
        )

        let a = try await client.complete(Request(model: "m", messages: [.user("hi")], provider: nil))
        XCTAssertEqual(a.text, "one")

        let b = try await client.complete(Request(model: "m", messages: [.user("hi")], provider: "p2"))
        XCTAssertEqual(b.text, "two")
    }

    @Test
    func testConfigurationErrorWhenNoDefaultProvider() async {
        let client = try! Client(providers: [:], defaultProvider: nil)
        do {
            _ = try await client.complete(Request(model: "m", messages: [.user("hi")], provider: nil))
            XCTFail("Expected ConfigurationError")
        } catch is ConfigurationError {
            // ok
        } catch {
            XCTFail("Unexpected error: \(error)")
        }
    }
}

