import Foundation
import OmniACPModel

public actor Client {
    public nonisolated let name: String
    public nonisolated let version: String
    public nonisolated let protocolVersion: Int
    public nonisolated let capabilities: ClientCapabilities

    private var transport: (any Transport)?
    private var delegate: (any ClientDelegate)?
    private var pendingRequests: [ID: AnyPendingRequest] = [:]
    private var notificationObservers: [UUID: @Sendable (AnyMessage) async -> Void] = [:]
    private var messageLoopTask: Task<Void, Never>?
    private var cancelLoopTask: Task<Void, Never>?

    private let notificationStream: AsyncStream<AnyMessage>
    private let notificationContinuation: AsyncStream<AnyMessage>.Continuation
    private nonisolated let cancelRequestStream: AsyncStream<String>
    private nonisolated let cancelRequestContinuation: AsyncStream<String>.Continuation

    public nonisolated var notifications: AsyncStream<AnyMessage> {
        notificationStream
    }

    public init(
        name: String = "OmniACP",
        version: String = "1.0.0",
        protocolVersion: Int = 1,
        capabilities: ClientCapabilities = ClientCapabilities()
    ) {
        self.name = name
        self.version = version
        self.protocolVersion = protocolVersion
        self.capabilities = capabilities

        let notificationPair = Self.makeAsyncStream(of: AnyMessage.self)
        self.notificationStream = notificationPair.stream
        self.notificationContinuation = notificationPair.continuation

        let cancelPair = Self.makeAsyncStream(of: String.self)
        self.cancelRequestStream = cancelPair.stream
        self.cancelRequestContinuation = cancelPair.continuation
    }

    public func setDelegate(_ delegate: (any ClientDelegate)?) {
        self.delegate = delegate
    }

    @discardableResult
    public func addNotificationObserver(_ handler: @escaping @Sendable (AnyMessage) async -> Void) -> UUID {
        let id = UUID()
        notificationObservers[id] = handler
        return id
    }

    public func removeNotificationObserver(_ id: UUID) {
        notificationObservers.removeValue(forKey: id)
    }

    public func connect(transport: any Transport, timeout: Duration? = nil) async throws -> Initialize.Result {
        guard self.transport == nil else {
            throw ClientError.alreadyConnected
        }
        try await transport.connect()
        self.transport = transport
        startMessageLoop(using: transport)
        startCancelLoop()
        let result = try await initialize(timeout: timeout)
        try await notify(InitializedNotification.message())
        return result
    }

    public func disconnect() async {
        messageLoopTask?.cancel()
        messageLoopTask = nil
        cancelLoopTask?.cancel()
        cancelLoopTask = nil
        failAllPending(with: ClientError.connectionClosed)
        notificationObservers.removeAll()
        notificationContinuation.finish()
        if let transport {
            await transport.disconnect()
        }
        transport = nil
    }

    public var isConnected: Bool {
        transport != nil
    }

    public func initialize(timeout: Duration? = nil) async throws -> Initialize.Result {
        let params = Initialize.Parameters(
            protocolVersion: protocolVersion,
            clientInfo: ClientInfo(name: name, version: version),
            clientCapabilities: capabilities
        )
        return try await send(Initialize.request(id: .number(1), params), timeout: timeout)
    }

    public func newSession(cwd: String, mcpServers: [MCPServer] = [], timeout: Duration? = nil) async throws -> SessionNew.Result {
        try await send(SessionNew.request(.init(cwd: cwd, mcpServers: mcpServers)), timeout: timeout)
    }

    public func prompt(sessionID: String, prompt: [ContentBlock], timeout: Duration? = nil) async throws -> SessionPrompt.Result {
        let request = SessionPrompt.request(.init(sessionID: sessionID, prompt: prompt))
        do {
            return try await send(request, timeout: timeout)
        } catch is CancellationError {
            cancelRequestContinuation.yield(sessionID)
            throw CancellationError()
        } catch let clientError as ClientError {
            if case .timedOut = clientError {
                await sendBestEffortCancel(sessionID: sessionID)
            }
            throw clientError
        } catch {
            throw error
        }
    }

    public func cancel(sessionID: String, timeout: Duration? = nil) async throws {
        let _: Empty = try await send(SessionCancel.request(.init(sessionID: sessionID)), timeout: timeout)
    }

    public func setMode(sessionID: String, modeID: String, timeout: Duration? = nil) async throws {
        let _: Empty = try await send(SessionSetMode.request(.init(sessionID: sessionID, modeID: modeID)), timeout: timeout)
    }

    public func send<M: OmniACPModel.Method>(_ request: Request<M>, timeout: Duration? = nil) async throws -> M.Result {
        guard let transport else {
            throw ClientError.notConnected
        }

        let pending = PendingResultStream<M.Result>()
        pendingRequests[request.id] = AnyPendingRequest(continuation: pending.continuation, resultType: M.Result.self)
        let data = try JSONEncoder().encode(request)

        do {
            try await transport.send(data)
            let result = try await withTaskCancellationHandler {
                try await withTimeout(timeout, label: request.method) {
                    var iterator = pending.stream.makeAsyncIterator()
                    guard let result = try await iterator.next() else {
                        throw ClientError.connectionClosed
                    }
                    return result
                }
            } onCancel: {
                pending.continuation.finish(throwing: CancellationError())
            }
            return result
        } catch {
            removePendingRequest(id: request.id, failingWith: error)
            throw error
        }
    }

    public func notify<N: OmniACPModel.Notification>(_ message: Message<N>) async throws {
        guard let transport else {
            throw ClientError.notConnected
        }
        let data = try JSONEncoder().encode(message)
        try await transport.send(data)
    }

    private func withTimeout<T: Sendable>(
        _ timeout: Duration?,
        label: String,
        operation: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        guard let timeout else {
            return try await operation()
        }
        return try await withThrowingTaskGroup(of: T.self) { group in
            group.addTask {
                try await operation()
            }
            group.addTask {
                try await Task.sleep(for: timeout)
                throw ClientError.timedOut(label)
            }
            guard let result = try await group.next() else {
                throw ClientError.connectionClosed
            }
            group.cancelAll()
            return result
        }
    }

    private nonisolated static func makeAsyncStream<Element>(of _: Element.Type) -> (stream: AsyncStream<Element>, continuation: AsyncStream<Element>.Continuation) {
        var continuation: AsyncStream<Element>.Continuation?
        let stream = AsyncStream<Element> { cont in
            continuation = cont
        }
        guard let continuation else {
            preconditionFailure("AsyncStream continuation was not initialized")
        }
        return (stream, continuation)
    }

    private func startMessageLoop(using transport: any Transport) {
        messageLoopTask = Task {
            do {
                let stream = transport.receive()
                for try await data in stream {
                    await self.handleIncomingData(data)
                }
                self.failAllPending(with: ClientError.connectionClosed)
            } catch {
                self.failAllPending(with: error)
            }
        }
    }

    private func startCancelLoop() {
        cancelLoopTask?.cancel()
        let cancelStream = cancelRequestStream
        cancelLoopTask = Task {
            for await sessionID in cancelStream {
                await self.sendBestEffortCancel(sessionID: sessionID)
            }
        }
    }

    private func sendBestEffortCancel(sessionID: String) async {
        do {
            let _: Empty = try await send(SessionCancel.request(.init(sessionID: sessionID)), timeout: .seconds(1))
        } catch {
        }
    }

    private func failAllPending(with error: Error) {
        let pending = pendingRequests.values
        pendingRequests.removeAll()
        for request in pending {
            request.fail(with: error)
        }
    }

    private func removePendingRequest(id: ID, failingWith error: Error) {
        guard let pending = pendingRequests.removeValue(forKey: id) else {
            return
        }
        pending.fail(with: error)
    }

    private func handleIncomingData(_ data: Data) async {
        let envelope: EnvelopeType
        do {
            envelope = try parseEnvelope(from: data)
        } catch {
            await sendErrorResponse(id: .null, error: .parseError())
            return
        }

        switch envelope {
        case .request(let request):
            await handleRequest(request)
        case .notification(let notification):
            let observers = Array(notificationObservers.values)
            for observer in observers {
                await observer(notification)
            }
            notificationContinuation.yield(notification)
        case .response(let response):
            handleResponse(response)
        }
    }

    private func parseEnvelope(from data: Data) throws -> EnvelopeType {
        let object = try JSONSerialization.jsonObject(with: data, options: [])
        guard let dictionary = object as? [String: Any] else {
            throw ClientError.invalidPayload("JSON-RPC payload must be an object")
        }
        let hasMethod = dictionary["method"] != nil
        let hasID = dictionary.keys.contains("id")
        if hasMethod && hasID {
            return .request(try JSONDecoder().decode(AnyRequest.self, from: data))
        }
        if hasMethod {
            return .notification(try JSONDecoder().decode(AnyMessage.self, from: data))
        }
        if hasID {
            return .response(try JSONDecoder().decode(AnyResponse.self, from: data))
        }
        throw ClientError.invalidPayload("Unrecognized JSON-RPC envelope")
    }

    private func handleResponse(_ response: AnyResponse) {
        guard let pending = pendingRequests.removeValue(forKey: response.id) else {
            return
        }
        if let error = response.error {
            pending.fail(with: error)
        } else {
            pending.succeed(with: response.result ?? .object([:]))
        }
    }

    private func handleRequest(_ request: AnyRequest) async {
        guard let transport else { return }
        do {
            let resultValue = try await routeRequest(request)
            let response = AnyResponse(id: request.id, result: resultValue)
            try await transport.send(JSONEncoder().encode(response))
        } catch let rpcError as RPCError {
            await sendErrorResponse(id: request.id, error: rpcError)
        } catch {
            await sendErrorResponse(id: request.id, error: .internalError(String(describing: error)))
        }
    }

    private func routeRequest(_ request: AnyRequest) async throws -> Value {
        guard let delegate else {
            throw RPCError.methodNotFound(request.method)
        }
        switch request.method {
        case SessionRequestPermission.name:
            let params: SessionRequestPermission.Parameters = try decodeParams(request.params)
            let result = try await delegate.handlePermissionRequest(params)
            return try encodeValue(result)
        case FileSystemReadTextFile.name:
            let params: FileSystemReadTextFile.Parameters = try decodeParams(request.params)
            let result = try await delegate.handleReadTextFile(params)
            return try encodeValue(result)
        case FileSystemWriteTextFile.name:
            let params: FileSystemWriteTextFile.Parameters = try decodeParams(request.params)
            let result = try await delegate.handleWriteTextFile(params)
            return try encodeValue(result)
        case TerminalCreate.name:
            let params: TerminalCreate.Parameters = try decodeParams(request.params)
            let result = try await delegate.handleTerminalCreate(params)
            return try encodeValue(result)
        case TerminalOutput.name:
            let params: TerminalOutput.Parameters = try decodeParams(request.params)
            let result = try await delegate.handleTerminalOutput(params)
            return try encodeValue(result)
        case TerminalWaitForExit.name:
            let params: TerminalWaitForExit.Parameters = try decodeParams(request.params)
            let result = try await delegate.handleTerminalWaitForExit(params)
            return try encodeValue(result)
        case TerminalKill.name:
            let params: TerminalKill.Parameters = try decodeParams(request.params)
            let result = try await delegate.handleTerminalKill(params)
            return try encodeValue(result)
        case TerminalRelease.name:
            let params: TerminalRelease.Parameters = try decodeParams(request.params)
            let result = try await delegate.handleTerminalRelease(params)
            return try encodeValue(result)
        default:
            throw RPCError.methodNotFound(request.method)
        }
    }

    private func decodeParams<T: Decodable>(_ value: Value?) throws -> T {
        let payload = value ?? .object([:])
        return try decodeValue(payload, as: T.self)
    }

    private func sendErrorResponse(id: ID, error: RPCError) async {
        guard let transport else { return }
        let response = AnyResponse(id: id, error: error)
        do {
            try await transport.send(JSONEncoder().encode(response))
        } catch {
        }
    }

    private enum EnvelopeType {
        case request(AnyRequest)
        case notification(AnyMessage)
        case response(AnyResponse)
    }
}

private struct PendingResultStream<T: Sendable> {
    let stream: AsyncThrowingStream<T, Error>
    let continuation: AsyncThrowingStream<T, Error>.Continuation

    init() {
        var continuation: AsyncThrowingStream<T, Error>.Continuation?
        self.stream = AsyncThrowingStream { cont in
            continuation = cont
        }
        self.continuation = continuation!
    }
}

private final class AnyPendingRequest: @unchecked Sendable {
    private let _succeed: @Sendable (Value) -> Void
    private let _fail: @Sendable (Error) -> Void

    init<T: Decodable & Sendable>(continuation: AsyncThrowingStream<T, Error>.Continuation, resultType: T.Type) {
        self._succeed = { value in
            do {
                let decoded = try decodeValue(value, as: T.self)
                continuation.yield(decoded)
                continuation.finish()
            } catch {
                continuation.finish(throwing: error)
            }
        }
        self._fail = { error in
            continuation.finish(throwing: error)
        }
    }

    func succeed(with value: Value) {
        _succeed(value)
    }

    func fail(with error: Error) {
        _fail(error)
    }
}
