import Foundation
#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#endif

public struct StdioTransportConfiguration: Sendable {
    public var executablePath: String
    public var arguments: [String]
    public var workingDirectory: String?
    public var environment: [String: String]
    public var transportConfiguration: TransportConfiguration

    public init(
        executablePath: String,
        arguments: [String] = [],
        workingDirectory: String? = nil,
        environment: [String: String] = [:],
        transportConfiguration: TransportConfiguration = .default
    ) {
        self.executablePath = executablePath
        self.arguments = arguments
        self.workingDirectory = workingDirectory
        self.environment = environment
        self.transportConfiguration = transportConfiguration
    }
}

public actor StdioTransport: Transport {
    private let configuration: StdioTransportConfiguration
    private nonisolated let stream: AsyncThrowingStream<Data, Error>
    private nonisolated let readPump: _StdioReadPump
    private var connected = false

    #if !os(iOS) && !os(tvOS) && !os(watchOS) && !os(visionOS)
    private var process: Process?
    private var stdinPipe: Pipe?
    private var stdoutPipe: Pipe?
    private var stderrPipe: Pipe?
    #endif

    public init(configuration: StdioTransportConfiguration) {
        self.configuration = configuration
        var continuation: AsyncThrowingStream<Data, Error>.Continuation?
        self.stream = AsyncThrowingStream { cont in
            continuation = cont
        }
        self.readPump = _StdioReadPump(
            continuation: continuation!,
            maxMessageSize: configuration.transportConfiguration.maxMessageSize
        )
    }

    public var isConnected: Bool {
        connected
    }

    public var processID: Int32? {
        #if !os(iOS) && !os(tvOS) && !os(watchOS) && !os(visionOS)
        return process?.processIdentifier
        #else
        return nil
        #endif
    }

    public func connect() async throws {
        #if os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
        throw ClientError.unsupportedPlatform("StdioTransport is unavailable on this platform")
        #else
        guard !connected else {
            throw ClientError.alreadyConnected
        }

        let process = Process()
        process.executableURL = URL(fileURLWithPath: configuration.executablePath)
        process.arguments = configuration.arguments
        if let workingDirectory = configuration.workingDirectory, !workingDirectory.isEmpty {
            process.currentDirectoryURL = URL(fileURLWithPath: workingDirectory, isDirectory: true)
        }
        var environment = ProcessInfo.processInfo.environment
        for (key, value) in configuration.environment {
            environment[key] = value
        }
        process.environment = environment

        let stdinPipe = Pipe()
        let stdoutPipe = Pipe()
        let stderrPipe = Pipe()
        process.standardInput = stdinPipe
        process.standardOutput = stdoutPipe
        process.standardError = stderrPipe
        process.terminationHandler = { [weak self] process in
            Task {
                await self?.handleTermination(status: process.terminationStatus)
            }
        }

        self.process = process
        self.stdinPipe = stdinPipe
        self.stdoutPipe = stdoutPipe
        self.stderrPipe = stderrPipe
        readPump.reset()

        installReadabilityHandler(stdoutPipe.fileHandleForReading)
        installStderrHandler(stderrPipe.fileHandleForReading)
        try process.run()
        connected = true
        #endif
    }

    public func send(_ data: Data) async throws {
        guard connected else {
            throw ClientError.transportClosed
        }
        #if os(iOS) || os(tvOS) || os(watchOS) || os(visionOS)
        throw ClientError.unsupportedPlatform("StdioTransport is unavailable on this platform")
        #else
        guard let stdinPipe else {
            throw ClientError.transportClosed
        }
        var line = data
        line.append(0x0A)
        try stdinPipe.fileHandleForWriting.write(contentsOf: line)
        #endif
    }

    public nonisolated func receive() -> AsyncThrowingStream<Data, Error> {
        stream
    }

    public func disconnect() async {
        guard connected else { return }
        connected = false
        #if !os(iOS) && !os(tvOS) && !os(watchOS) && !os(visionOS)
        process?.terminationHandler = nil
        stdoutPipe?.fileHandleForReading.readabilityHandler = nil
        stderrPipe?.fileHandleForReading.readabilityHandler = nil
        try? stdinPipe?.fileHandleForWriting.close()
        try? stdoutPipe?.fileHandleForReading.close()
        try? stderrPipe?.fileHandleForReading.close()
        if let process, process.isRunning {
            process.terminate()
            usleep(250_000)
            #if canImport(Darwin) || canImport(Glibc)
            if process.isRunning {
                _ = kill(process.processIdentifier, SIGKILL)
            }
            #endif
        }
        self.process = nil
        self.stdinPipe = nil
        self.stdoutPipe = nil
        self.stderrPipe = nil
        #endif
        readPump.finish()
    }

    #if !os(iOS) && !os(tvOS) && !os(watchOS) && !os(visionOS)
    private nonisolated func installReadabilityHandler(_ handle: FileHandle) {
        let pump = readPump
        handle.readabilityHandler = { fileHandle in
            let data = fileHandle.availableData
            guard !data.isEmpty else {
                fileHandle.readabilityHandler = nil
                return
            }
            pump.append(data)
        }
    }

    private nonisolated func installStderrHandler(_ handle: FileHandle) {
        handle.readabilityHandler = { fileHandle in
            let data = fileHandle.availableData
            if data.isEmpty {
                fileHandle.readabilityHandler = nil
            }
        }
    }

    private func handleTermination(status: Int32) {
        connected = false
        process = nil
        stdinPipe = nil
        stdoutPipe = nil
        stderrPipe = nil
        if status != 0 {
            readPump.finish(throwing: ClientError.processExited(status))
        } else {
            readPump.finish()
        }
    }
    #endif
}

private final class _StdioReadPump: @unchecked Sendable {
    private let lock = NSLock()
    private var readBuffer = Data()
    private var continuation: AsyncThrowingStream<Data, Error>.Continuation?
    private let maxMessageSize: Int

    init(continuation: AsyncThrowingStream<Data, Error>.Continuation, maxMessageSize: Int) {
        self.continuation = continuation
        self.maxMessageSize = maxMessageSize
    }

    func reset() {
        lock.lock()
        readBuffer.removeAll(keepingCapacity: true)
        lock.unlock()
    }

    func append(_ data: Data) {
        var messages: [Data] = []
        var errorToFinish: Error?

        lock.lock()
        if continuation == nil {
            lock.unlock()
            return
        }
        readBuffer.append(data)
        if maxMessageSize > 0 && readBuffer.count > maxMessageSize {
            continuation = nil
            readBuffer.removeAll(keepingCapacity: false)
            errorToFinish = ClientError.invalidPayload("Message exceeded configured max size")
            lock.unlock()
            if let errorToFinish {
                finish(throwing: errorToFinish)
            }
            return
        }
        while let message = popNextMessageLocked() {
            messages.append(message)
        }
        let continuation = self.continuation
        lock.unlock()

        for message in messages {
            continuation?.yield(message)
        }
    }

    func finish(throwing error: Error? = nil) {
        lock.lock()
        let continuation = self.continuation
        self.continuation = nil
        readBuffer.removeAll(keepingCapacity: false)
        lock.unlock()

        if let error {
            continuation?.finish(throwing: error)
        } else {
            continuation?.finish()
        }
    }

    private func popNextMessageLocked() -> Data? {
        let whitespace: Set<UInt8> = [0x20, 0x09, 0x0D, 0x0A]
        while let first = readBuffer.first, whitespace.contains(first) {
            readBuffer.removeFirst()
        }
        guard let first = readBuffer.first else {
            return nil
        }
        guard first == 0x7B || first == 0x5B else {
            if let newline = readBuffer.firstIndex(of: 0x0A) {
                let removeCount = readBuffer.distance(from: readBuffer.startIndex, to: newline) + 1
                readBuffer.removeFirst(min(removeCount, readBuffer.count))
            } else if readBuffer.count > 4_096 {
                readBuffer.removeAll(keepingCapacity: true)
            }
            return nil
        }

        let bytes = Array(readBuffer)
        var depth = 0
        var inString = false
        var escaped = false
        for index in bytes.indices {
            let byte = bytes[index]
            if inString {
                if escaped {
                    escaped = false
                    continue
                }
                if byte == 0x5C {
                    escaped = true
                    continue
                }
                if byte == 0x22 {
                    inString = false
                }
                continue
            }
            if byte == 0x22 {
                inString = true
                continue
            }
            if byte == 0x7B || byte == 0x5B {
                depth += 1
            } else if byte == 0x7D || byte == 0x5D {
                depth -= 1
                if depth == 0 {
                    let message = Data(bytes[0...index])
                    readBuffer.removeFirst(min(index + 1, readBuffer.count))
                    return message
                }
            }
        }
        return nil
    }
}
