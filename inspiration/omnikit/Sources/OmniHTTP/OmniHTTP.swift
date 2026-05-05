import Foundation

#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

public enum OmniHTTPError: Error, Sendable, Equatable {
    case invalidURL(String)
    case invalidResponse
    case streamingNotSupported
}

public enum HTTPMethod: String, Sendable, Equatable {
    case get = "GET"
    case post = "POST"
    case put = "PUT"
    case patch = "PATCH"
    case delete = "DELETE"
    case head = "HEAD"
    case options = "OPTIONS"
}

public struct HTTPHeaders: Sendable, Equatable {
    private var storage: [String: [String]] = [:] // lowercased key -> values

    public init() {}

    public init(_ headers: [String: String]) {
        for (k, v) in headers {
            self.add(name: k, value: v)
        }
    }

    public mutating func add(name: String, value: String) {
        let key = name.lowercased()
        storage[key, default: []].append(value)
    }

    public mutating func set(name: String, value: String) {
        let key = name.lowercased()
        storage[key] = [value]
    }

    public func values(for name: String) -> [String] {
        storage[name.lowercased()] ?? []
    }

    public func firstValue(for name: String) -> String? {
        values(for: name).first
    }

    public var asDictionary: [String: String] {
        var out: [String: String] = [:]
        for (k, vs) in storage {
            // If multiple values exist, join with commas (common HTTP convention).
            out[k] = vs.joined(separator: ",")
        }
        return out
    }
}

public enum HTTPBody: Sendable, Equatable {
    case none
    case bytes([UInt8])

    public static func text(_ string: String, encoding: String.Encoding = .utf8) -> HTTPBody {
        .bytes(Array((string.data(using: encoding) ?? Data())))
    }
}

public struct HTTPRequest: Sendable, Equatable {
    public var method: HTTPMethod
    public var url: URL
    public var headers: HTTPHeaders
    public var body: HTTPBody

    public init(method: HTTPMethod, url: URL, headers: HTTPHeaders = HTTPHeaders(), body: HTTPBody = .none) {
        self.method = method
        self.url = url
        self.headers = headers
        self.body = body
    }
}

public struct HTTPResponse: Sendable, Equatable {
    public var statusCode: Int
    public var headers: HTTPHeaders
    public var body: [UInt8]

    public init(statusCode: Int, headers: HTTPHeaders, body: [UInt8]) {
        self.statusCode = statusCode
        self.headers = headers
        self.body = body
    }
}

public typealias HTTPByteStream = AsyncThrowingStream<[UInt8], Error>

public struct HTTPStreamResponse: Sendable {
    public var statusCode: Int
    public var headers: HTTPHeaders
    public var body: HTTPByteStream

    public init(statusCode: Int, headers: HTTPHeaders, body: HTTPByteStream) {
        self.statusCode = statusCode
        self.headers = headers
        self.body = body
    }
}

public protocol HTTPTransport: Sendable {
    func send(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPResponse
    func openStream(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPStreamResponse
    func shutdown() async throws
}

extension HTTPTransport {
    public func shutdown() async throws {}
}

public final class URLSessionHTTPTransport: HTTPTransport, @unchecked Sendable {
    private let session: URLSession

    public init(configuration: URLSessionConfiguration = .default) {
        self.session = URLSession(configuration: configuration)
    }

    public func send(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPResponse {
        var urlRequest = URLRequest(url: request.url)
        urlRequest.httpMethod = request.method.rawValue
        for (k, v) in request.headers.asDictionary {
            urlRequest.setValue(v, forHTTPHeaderField: k)
        }
        switch request.body {
        case .none:
            break
        case .bytes(let bytes):
            urlRequest.httpBody = Data(bytes)
        }

        if let timeout {
            let seconds = Double(timeout.components.seconds) + Double(timeout.components.attoseconds) / 1e18
            urlRequest.timeoutInterval = seconds
        }

        let (data, response) = try await session.data(for: urlRequest)
        guard let http = response as? HTTPURLResponse else {
            throw OmniHTTPError.invalidResponse
        }

        var headers = HTTPHeaders()
        for (kAny, vAny) in http.allHeaderFields {
            guard let k = kAny as? String else { continue }
            if let v = vAny as? String {
                headers.add(name: k, value: v)
            } else {
                headers.add(name: k, value: String(describing: vAny))
            }
        }

        return HTTPResponse(statusCode: http.statusCode, headers: headers, body: Array(data))
    }

    public func openStream(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPStreamResponse {
        // NOTE: URLSession streaming APIs differ across platforms.
        // - On Darwin, `URLSession.bytes(for:)` is available on newer OS versions.
        // - On Linux (FoundationNetworking), that API isn't currently available.
        #if os(WASI)
        throw OmniHTTPError.streamingNotSupported
        #elseif canImport(FoundationNetworking)
        throw OmniHTTPError.streamingNotSupported
        #else
        var urlRequest = URLRequest(url: request.url)
        urlRequest.httpMethod = request.method.rawValue
        for (k, v) in request.headers.asDictionary {
            urlRequest.setValue(v, forHTTPHeaderField: k)
        }
        switch request.body {
        case .none:
            break
        case .bytes(let bytes):
            urlRequest.httpBody = Data(bytes)
        }
        if let timeout {
            let seconds = Double(timeout.components.seconds) + Double(timeout.components.attoseconds) / 1e18
            urlRequest.timeoutInterval = seconds
        }

        guard #available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, *) else {
            throw OmniHTTPError.streamingNotSupported
        }

        // `bytes(for:)` yields individual bytes; we batch them into chunks.
        let (asyncBytes, response) = try await session.bytes(for: urlRequest)
        guard let http = response as? HTTPURLResponse else {
            throw OmniHTTPError.invalidResponse
        }

        var headers = HTTPHeaders()
        for (kAny, vAny) in http.allHeaderFields {
            guard let k = kAny as? String else { continue }
            if let v = vAny as? String {
                headers.add(name: k, value: v)
            } else {
                headers.add(name: k, value: String(describing: vAny))
            }
        }

        let stream: HTTPByteStream = AsyncThrowingStream { continuation in
            let producer = Task {
                do {
                    try await withTaskCancellationHandler {
                        var buffer: [UInt8] = []
                        buffer.reserveCapacity(4096)
                        for try await b in asyncBytes {
                            try Task.checkCancellation()
                            buffer.append(b)
                            if buffer.count >= 4096 {
                                continuation.yield(buffer)
                                buffer.removeAll(keepingCapacity: true)
                            }
                        }
                        if !buffer.isEmpty {
                            continuation.yield(buffer)
                        }
                        continuation.finish()
                    } onCancel: {
                        asyncBytes.task.cancel()
                    }
                } catch is CancellationError {
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { @Sendable _ in
                producer.cancel()
            }
        }

        return HTTPStreamResponse(statusCode: http.statusCode, headers: headers, body: stream)
        #endif
    }
}

public struct SSEEvent: Sendable, Equatable {
    public var event: String?
    public var data: String
    public var id: String?

    public init(event: String?, data: String, id: String?) {
        self.event = event
        self.data = data
        self.id = id
    }
}

public enum SSE {
    public static func parse(_ byteStream: HTTPByteStream) -> AsyncThrowingStream<SSEEvent, Error> {
        AsyncThrowingStream { continuation in
            let parser = Task {
                do {
                    var buffer: [UInt8] = []
                    buffer.reserveCapacity(8 * 1024)

                    var currentEvent: String? = nil
                    var currentID: String? = nil
                    var dataLines: [String] = []
                    dataLines.reserveCapacity(4)

                    func dispatchIfNeeded() {
                        if !dataLines.isEmpty || currentEvent != nil || currentID != nil {
                            let data = dataLines.joined(separator: "\n")
                            continuation.yield(SSEEvent(event: currentEvent, data: data, id: currentID))
                        }
                        currentEvent = nil
                        currentID = nil
                        dataLines.removeAll(keepingCapacity: true)
                    }

                    func handleLine(_ lineRaw: [UInt8]) {
                        // Trim trailing \r
                        var line = lineRaw
                        if line.last == 0x0D { _ = line.popLast() }

                        if line.isEmpty {
                            dispatchIfNeeded()
                            return
                        }
                        if line.first == 0x3A { // ':'
                            return // comment
                        }

                        // Split on first ':'
                        var fieldBytes: [UInt8] = []
                        fieldBytes.reserveCapacity(line.count)
                        var valueBytes: [UInt8] = []
                        var seenColon = false
                        for b in line {
                            if !seenColon, b == 0x3A { // ':'
                                seenColon = true
                                continue
                            }
                            if !seenColon {
                                fieldBytes.append(b)
                            } else {
                                valueBytes.append(b)
                            }
                        }
                        let field = String(decoding: fieldBytes, as: UTF8.self)
                        var value = String(decoding: valueBytes, as: UTF8.self)
                        if value.first == " " { value.removeFirst() }

                        switch field {
                        case "event":
                            currentEvent = value
                        case "data":
                            dataLines.append(value)
                        case "id":
                            currentID = value
                        case "retry":
                            // Ignored (client-level reconnection policy lives above this parser).
                            break
                        default:
                            break
                        }
                    }

                    for try await chunk in byteStream {
                        try Task.checkCancellation()
                        buffer.append(contentsOf: chunk)

                        while true {
                            guard let newlineIdx = buffer.firstIndex(of: 0x0A) else { break } // '\n'
                            let line = Array(buffer[..<newlineIdx])
                            buffer.removeSubrange(...newlineIdx)
                            handleLine(line)
                        }
                    }

                    if !buffer.isEmpty {
                        handleLine(buffer)
                        buffer.removeAll(keepingCapacity: true)
                    }
                    dispatchIfNeeded()
                    continuation.finish()
                } catch is CancellationError {
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { @Sendable _ in
                parser.cancel()
            }
        }
    }
}
