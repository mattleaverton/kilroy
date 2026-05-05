import Foundation
import OmniHTTP

public protocol MCPTransport: Sendable {
    func connect() async throws
    func disconnect() async
    func send(_ data: Data) async throws
    func messageStream() async throws -> AsyncThrowingStream<Data, Error>
}

public actor StdioMCPTransport: MCPTransport {
    private let command: String
    private let arguments: [String]
    private let environment: [String: String]
    private var process: Process?
    private var stdinHandle: FileHandle?
    private var stdoutHandle: FileHandle?
    private var stderrHandle: FileHandle?
    private var cachedStream: AsyncThrowingStream<Data, Error>?
    private var stderrTask: Task<Void, Never>?

    public init(command: String, args: [String] = [], env: [String: String] = [:]) {
        self.command = command
        self.arguments = args
        self.environment = env
    }

    public func connect() async throws {
        guard process == nil else { return }
        let proc = Process()
        let resolvedURL: URL
        var resolvedArgs: [String]

        if command.contains("/") {
            resolvedURL = URL(fileURLWithPath: command)
            resolvedArgs = arguments
        } else {
            resolvedURL = URL(fileURLWithPath: "/usr/bin/env")
            resolvedArgs = [command] + arguments
        }

        proc.executableURL = resolvedURL
        proc.arguments = resolvedArgs

        var merged = ProcessInfo.processInfo.environment
        for (k, v) in environment {
            merged[k] = v
        }
        proc.environment = merged

        let stdinPipe = Pipe()
        let stdoutPipe = Pipe()
        let stderrPipe = Pipe()
        proc.standardInput = stdinPipe
        proc.standardOutput = stdoutPipe
        proc.standardError = stderrPipe

        try proc.run()
        process = proc
        stdinHandle = stdinPipe.fileHandleForWriting
        stdoutHandle = stdoutPipe.fileHandleForReading
        stderrHandle = stderrPipe.fileHandleForReading

        cachedStream = makeLineStream(from: stdoutPipe.fileHandleForReading)
        if let stderrHandle {
            stderrTask = Task.detached {
                _ = try? stderrHandle.readToEnd()
            }
        }
    }

    public func disconnect() async {
        stderrTask?.cancel()
        stderrTask = nil
        cachedStream = nil
        stdoutHandle?.closeFile()
        stdinHandle?.closeFile()
        stderrHandle?.closeFile()
        stdoutHandle = nil
        stdinHandle = nil
        stderrHandle = nil
        if let process {
            process.terminate()
            process.waitUntilExit()
        }
        process = nil
    }

    public func send(_ data: Data) async throws {
        guard let stdinHandle else { throw MCPError.notConnected }
        var payload = data
        if payload.last != 0x0A {
            payload.append(0x0A)
        }
        stdinHandle.write(payload)
    }

    public func messageStream() async throws -> AsyncThrowingStream<Data, Error> {
        guard let cachedStream else { throw MCPError.notConnected }
        return cachedStream
    }

    private func makeLineStream(from handle: FileHandle) -> AsyncThrowingStream<Data, Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    var buffer: [UInt8] = []
                    buffer.reserveCapacity(4096)
                    for try await byte in handle.bytes {
                        if byte == 0x0A {
                            continuation.yield(Data(buffer))
                            buffer.removeAll(keepingCapacity: true)
                        } else {
                            buffer.append(byte)
                        }
                    }
                    if !buffer.isEmpty {
                        continuation.yield(Data(buffer))
                    }
                    continuation.finish()
                } catch is CancellationError {
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { @Sendable _ in
                task.cancel()
            }
        }
    }
}

public actor SSEMCPTransport: MCPTransport {
    private let url: URL
    private let requestURL: URL
    private let headers: [String: String]
    private let transport: HTTPTransport
    private var cachedStream: AsyncThrowingStream<Data, Error>?

    public init(
        url: URL,
        requestURL: URL? = nil,
        headers: [String: String] = [:],
        transport: HTTPTransport = URLSessionHTTPTransport()
    ) {
        self.url = url
        self.requestURL = requestURL ?? url
        self.headers = headers
        self.transport = transport
    }

    public func connect() async throws {
        guard cachedStream == nil else { return }
        var httpHeaders = HTTPHeaders()
        for (k, v) in headers {
            httpHeaders.set(name: k, value: v)
        }
        let request = HTTPRequest(method: .get, url: url, headers: httpHeaders, body: .none)
        let response = try await transport.openStream(request, timeout: nil)
        let sseStream = SSE.parse(response.body)

        cachedStream = AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    for try await event in sseStream {
                        continuation.yield(Data(event.data.utf8))
                    }
                    continuation.finish()
                } catch is CancellationError {
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            continuation.onTermination = { @Sendable _ in
                task.cancel()
            }
        }
    }

    public func disconnect() async {
        cachedStream = nil
        try? await transport.shutdown()
    }

    public func send(_ data: Data) async throws {
        var httpHeaders = HTTPHeaders()
        httpHeaders.set(name: "content-type", value: "application/json")
        for (k, v) in headers {
            httpHeaders.set(name: k, value: v)
        }
        let request = HTTPRequest(method: .post, url: requestURL, headers: httpHeaders, body: .bytes(Array(data)))
        _ = try await transport.send(request, timeout: .seconds(30))
    }

    public func messageStream() async throws -> AsyncThrowingStream<Data, Error> {
        guard let cachedStream else { throw MCPError.notConnected }
        return cachedStream
    }
}
