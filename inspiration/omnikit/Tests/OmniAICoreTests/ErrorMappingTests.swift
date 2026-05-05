import Testing
import Foundation

@testable import OmniAICore

@Suite
final class ErrorMappingTests {
    @Test
    func testHTTPStatusMappingMatchesSpecTable() {
        func assertType(_ status: Int, _ type: SDKError.Type, retryable: Bool) {
            let err = _ErrorMapping.sdkErrorFromHTTP(
                provider: "test",
                statusCode: status,
                message: "x",
                errorCode: nil,
                retryAfter: nil,
                raw: nil
            )
            XCTAssertTrue(Swift.type(of: err) == type)
            XCTAssertEqual(err.retryable, retryable)
        }

        assertType(400, InvalidRequestError.self, retryable: false)
        assertType(401, AuthenticationError.self, retryable: false)
        assertType(403, AccessDeniedError.self, retryable: false)
        assertType(404, NotFoundError.self, retryable: false)
        assertType(408, RequestTimeoutError.self, retryable: true)
        assertType(413, ContextLengthError.self, retryable: false)
        assertType(422, InvalidRequestError.self, retryable: false)
        assertType(429, RateLimitError.self, retryable: true)
        assertType(500, ServerError.self, retryable: true)
        assertType(502, ServerError.self, retryable: true)
        assertType(503, ServerError.self, retryable: true)
        assertType(504, ServerError.self, retryable: true)
    }
}

