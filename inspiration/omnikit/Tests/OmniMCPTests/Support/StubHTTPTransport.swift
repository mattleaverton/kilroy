import OmniHTTP

actor StubHTTPTransport: HTTPTransport {
    private let streamResponse: HTTPStreamResponse
    private let sendResponse: HTTPResponse
    private var openStreamCount = 0
    private var sendCount = 0
    private var lastRequest: HTTPRequest?

    init(streamResponse: HTTPStreamResponse, sendResponse: HTTPResponse = HTTPResponse(statusCode: 200, headers: HTTPHeaders(), body: [])) {
        self.streamResponse = streamResponse
        self.sendResponse = sendResponse
    }

    func send(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPResponse {
        sendCount += 1
        lastRequest = request
        return sendResponse
    }

    func openStream(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPStreamResponse {
        openStreamCount += 1
        lastRequest = request
        return streamResponse
    }

    func shutdown() async throws {}

    func snapshot() -> (openStream: Int, send: Int, lastRequest: HTTPRequest?) {
        (openStreamCount, sendCount, lastRequest)
    }
}
