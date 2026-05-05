import Foundation
import OmniAICore
import OmniHTTP

public protocol MCPRequestClient: Sendable {
    func connect() async throws
    func disconnect() async
    func sendRequest(method: String, params: JSONValue?) async throws -> JSONValue
}

public actor MCPJSONRPCClient: MCPRequestClient {
    private let transport: MCPTransport
    private var nextID: Int = 1
    private var pending: [String: CheckedContinuation<JSONValue, Error>] = [:]
    private var messageTask: Task<Void, Never>?
    private var connected = false

    public init(transport: MCPTransport) {
        self.transport = transport
    }

    public func connect() async throws {
        guard !connected else { return }
        try await transport.connect()
        connected = true
        messageTask = Task { await readLoop() }
    }

    public func disconnect() async {
        messageTask?.cancel()
        messageTask = nil
        failAllPending(with: MCPError.notConnected)
        await transport.disconnect()
        connected = false
    }

    public func sendRequest(method: String, params: JSONValue?) async throws -> JSONValue {
        guard connected else { throw MCPError.notConnected }
        let id = String(nextID)
        nextID += 1
        let payload = try encodeRequest(id: id, method: method, params: params)
        try await transport.send(payload)
        return try await withCheckedThrowingContinuation { continuation in
            pending[id] = continuation
        }
    }

    private func readLoop() async {
        do {
            let stream = try await transport.messageStream()
            for try await data in stream {
                guard !data.isEmpty else { continue }
                let json = try JSONValue.parse(data)
                handleMessage(json)
            }
        } catch is CancellationError {
            return
        } catch {
            failAllPending(with: error)
        }
    }

    private func handleMessage(_ json: JSONValue) {
        guard case .object(let object) = json else { return }
        guard let idValue = object["id"], let id = parseID(idValue) else { return }
        guard let continuation = pending.removeValue(forKey: id) else { return }

        if let errorObj = object["error"]?.objectValue {
            let code = Int(errorObj["code"]?.doubleValue ?? -1)
            let message = errorObj["message"]?.stringValue ?? "MCP error"
            continuation.resume(throwing: MCPError.rpcError(code: code, message: message))
            return
        }
        if let result = object["result"] {
            continuation.resume(returning: result)
            return
        }
        continuation.resume(throwing: MCPError.invalidResponse("Missing result for id=\(id)"))
    }

    private func failAllPending(with error: Error) {
        for (_, continuation) in pending {
            continuation.resume(throwing: error)
        }
        pending.removeAll()
    }

    private func parseID(_ value: JSONValue) -> String? {
        if let string = value.stringValue {
            return string
        }
        if let number = value.doubleValue {
            return String(Int(number))
        }
        return nil
    }
}

public actor MCPStreamableHTTPClient: MCPRequestClient {
    private let url: URL
    private let headers: [String: String]
    private let transport: HTTPTransport
    private var nextID: Int = 1

    public init(url: URL, headers: [String: String] = [:], transport: HTTPTransport = URLSessionHTTPTransport()) {
        self.url = url
        self.headers = headers
        self.transport = transport
    }

    public func connect() async throws {}

    public func disconnect() async {
        try? await transport.shutdown()
    }

    public func sendRequest(method: String, params: JSONValue?) async throws -> JSONValue {
        let id = String(nextID)
        nextID += 1
        let payload = try encodeRequest(id: id, method: method, params: params)
        var httpHeaders = HTTPHeaders()
        httpHeaders.set(name: "content-type", value: "application/json")
        for (k, v) in headers {
            httpHeaders.set(name: k, value: v)
        }
        let request = HTTPRequest(method: .post, url: url, headers: httpHeaders, body: .bytes(Array(payload)))

        do {
            let response = try await transport.openStream(request, timeout: .seconds(60))
            return try await parseStreamResponse(response: response, requestID: id)
        } catch OmniHTTPError.streamingNotSupported {
            let response = try await transport.send(request, timeout: .seconds(60))
            let json = try JSONValue.parse(Data(response.body))
            return try extractResult(from: json, requestID: id)
        }
    }

    private func parseStreamResponse(response: HTTPStreamResponse, requestID: String) async throws -> JSONValue {
        if let contentType = response.headers.firstValue(for: "content-type")?.lowercased(),
           contentType.contains("text/event-stream") {
            let sseStream = SSE.parse(response.body)
            for try await event in sseStream {
                if event.data == "[DONE]" { break }
                guard let data = event.data.data(using: .utf8) else { continue }
                let json = try JSONValue.parse(data)
                if let result = try? extractResult(from: json, requestID: requestID) {
                    return result
                }
            }
            throw MCPError.invalidResponse("Stream ended before response id=\(requestID)")
        }

        var buffer: [UInt8] = []
        buffer.reserveCapacity(4096)
        for try await chunk in response.body {
            buffer.append(contentsOf: chunk)
            while let newline = buffer.firstIndex(of: 0x0A) {
                let line = Data(buffer[..<newline])
                buffer.removeSubrange(...newline)
                if line.isEmpty { continue }
                let json = try JSONValue.parse(line)
                if let result = try? extractResult(from: json, requestID: requestID) {
                    return result
                }
            }
        }
        if !buffer.isEmpty {
            let json = try JSONValue.parse(Data(buffer))
            return try extractResult(from: json, requestID: requestID)
        }
        throw MCPError.invalidResponse("Stream ended before response id=\(requestID)")
    }
}

private func encodeRequest(id: String, method: String, params: JSONValue?) throws -> Data {
    var payload: [String: JSONValue] = [
        "jsonrpc": .string("2.0"),
        "id": .string(id),
        "method": .string(method),
    ]
    if let params {
        payload["params"] = params
    }
    return try JSONValue.object(payload).data()
}

private func extractResult(from json: JSONValue, requestID: String) throws -> JSONValue {
    guard case .object(let object) = json else {
        throw MCPError.invalidResponse("Invalid JSON-RPC response")
    }
    let idValue = object["id"]
    if let idValue {
        if let idString = idValue.stringValue, idString != requestID {
            throw MCPError.invalidResponse("Unexpected response id \(idString)")
        }
        if let idNumber = idValue.doubleValue, String(Int(idNumber)) != requestID {
            throw MCPError.invalidResponse("Unexpected response id \(idNumber)")
        }
    }
    if let errorObj = object["error"]?.objectValue {
        let code = Int(errorObj["code"]?.doubleValue ?? -1)
        let message = errorObj["message"]?.stringValue ?? "MCP error"
        throw MCPError.rpcError(code: code, message: message)
    }
    if let result = object["result"] {
        return result
    }
    throw MCPError.invalidResponse("Missing result in response")
}
