import Foundation
import OmniHTTP

public typealias CompleteMiddleware = @Sendable (
    Request,
    @Sendable (Request) async throws -> Response
) async throws -> Response

public typealias StreamMiddleware = @Sendable (
    Request,
    @Sendable (Request) async throws -> AsyncThrowingStream<StreamEvent, Error>
) async throws -> AsyncThrowingStream<StreamEvent, Error>

public protocol ProviderAdapter: Sendable {
    var name: String { get }

    func complete(request: Request) async throws -> Response
    func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error>

    func close() async
    func initialize() throws
    func supportsToolChoice(_ mode: ToolChoiceMode) -> Bool
}

private func exchangeCodexOAuthIDTokenForOpenAIApiKey(
    idToken: String,
    issuer: String,
    clientID: String,
    transport: HTTPTransport
) async throws -> String {
    let normalizedIssuer = issuer
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .trimmingCharacters(in: CharacterSet(charactersIn: "/"))
    guard let url = URL(string: "\(normalizedIssuer)/oauth/token") else {
        throw ConfigurationError(message: "Invalid OPENAI_OAUTH_ISSUER URL: \(issuer)")
    }

    let body = [
        ("grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"),
        ("client_id", clientID),
        ("requested_token", "openai-api-key"),
        ("subject_token", idToken),
        ("subject_token_type", "urn:ietf:params:oauth:token-type:id_token"),
    ]
    .map { "\(formURLEncode($0.0))=\(formURLEncode($0.1))" }
    .joined(separator: "&")

    var headers = HTTPHeaders()
    headers.set(name: "content-type", value: "application/x-www-form-urlencoded")
    let request = HTTPRequest(
        method: .post,
        url: url,
        headers: headers,
        body: .text(body)
    )

    let response = try await transport.send(request, timeout: .seconds(30))

    guard (200..<300).contains(response.statusCode) else {
        let bodyString = String(decoding: response.body, as: UTF8.self)
        throw ConfigurationError(
            message: "OpenAI OAuth token exchange failed with status \(response.statusCode): \(bodyString)"
        )
    }

    let parsed = try JSONSerialization.jsonObject(with: Data(response.body), options: [])
    guard let object = parsed as? [String: Any],
          let accessToken = object["access_token"] as? String,
          !accessToken.isEmpty
    else {
        throw ConfigurationError(message: "OpenAI OAuth token exchange response missing access_token")
    }
    return accessToken
}

private func formURLEncode(_ value: String) -> String {
    var allowed = CharacterSet.urlQueryAllowed
    allowed.remove(charactersIn: "+&=?")
    return value.addingPercentEncoding(withAllowedCharacters: allowed) ?? value
}

extension ProviderAdapter {
    public func close() async {}
    public func initialize() throws {}
    public func supportsToolChoice(_ mode: ToolChoiceMode) -> Bool { true }
}

public protocol Middleware: Sendable {
    func complete(
        request: Request,
        next: @Sendable @escaping (Request) async throws -> Response
    ) async throws -> Response

    func stream(
        request: Request,
        next: @Sendable @escaping (Request) async throws -> AsyncThrowingStream<StreamEvent, Error>
    ) async throws -> AsyncThrowingStream<StreamEvent, Error>
}

private final class _ClosureMiddleware: Middleware, @unchecked Sendable {
    private let completeClosure: CompleteMiddleware?
    private let streamClosure: StreamMiddleware?

    init(complete: CompleteMiddleware?, stream: StreamMiddleware?) {
        self.completeClosure = complete
        self.streamClosure = stream
    }

    func complete(
        request: Request,
        next: @Sendable @escaping (Request) async throws -> Response
    ) async throws -> Response {
        if let completeClosure {
            return try await completeClosure(request, next)
        }
        return try await next(request)
    }

    func stream(
        request: Request,
        next: @Sendable @escaping (Request) async throws -> AsyncThrowingStream<StreamEvent, Error>
    ) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        if let streamClosure {
            return try await streamClosure(request, next)
        }
        return try await next(request)
    }
}

public final class Client: @unchecked Sendable {
    private var providers: [String: ProviderAdapter]
    private var _defaultProvider: String?
    private var _middleware: [Middleware]
    private let stateLock = NSLock()
    public let modelCatalog: ModelCatalog

    public init(
        providers: [String: ProviderAdapter],
        defaultProvider: String? = nil,
        middleware: [Middleware] = [],
        completeMiddleware: [CompleteMiddleware] = [],
        streamMiddleware: [StreamMiddleware] = [],
        modelCatalog: ModelCatalog = .default
    ) throws {
        self.providers = providers
        self._defaultProvider = defaultProvider ?? providers.first?.key
        self._middleware = middleware
        self._middleware.append(contentsOf: completeMiddleware.map { _ClosureMiddleware(complete: $0, stream: nil) })
        self._middleware.append(contentsOf: streamMiddleware.map { _ClosureMiddleware(complete: nil, stream: $0) })
        self.modelCatalog = modelCatalog

        for (_, adapter) in providers {
            try adapter.initialize()
        }
    }

    public var defaultProvider: String? {
        _withStateLock { _defaultProvider }
    }

    public var defaultProviderName: String? {
        defaultProvider
    }

    public var middleware: [Middleware] {
        _withStateLock { _middleware }
    }

    public var openai: OpenAIServiceNamespace {
        OpenAIServiceNamespace(client: self)
    }

    public var gemini: GeminiServiceNamespace {
        GeminiServiceNamespace(client: self)
    }

    public static func fromEnv(
        environment: [String: String],
        transport: OmniHTTP.HTTPTransport = OmniHTTP.URLSessionHTTPTransport(),
        modelCatalog: ModelCatalog = .default,
        middleware: [Middleware] = [],
        completeMiddleware: [CompleteMiddleware] = [],
        streamMiddleware: [StreamMiddleware] = [],
        allowEmptyProviders: Bool = false
    ) throws -> Client {
        // Synchronous overload — skips OAuth token exchange (which requires async networking).
        // If OPENAI_API_KEY is set directly, this works fine. If only OPENAI_OAUTH_ID_TOKEN
        // is set, callers must use the async fromEnvAsync() variant instead.
        let env = environment
        let providers = try _buildProviders(env: env, transport: transport, oauthApiKey: nil, allowEmpty: allowEmptyProviders)
        return try Client(
            providers: providers,
            defaultProvider: providers.first?.key,
            middleware: middleware,
            completeMiddleware: completeMiddleware,
            streamMiddleware: streamMiddleware,
            modelCatalog: modelCatalog
        )
    }

    public static func fromEnvAsync(
        environment: [String: String],
        transport: OmniHTTP.HTTPTransport = OmniHTTP.URLSessionHTTPTransport(),
        modelCatalog: ModelCatalog = .default,
        middleware: [Middleware] = [],
        completeMiddleware: [CompleteMiddleware] = [],
        streamMiddleware: [StreamMiddleware] = [],
        allowEmptyProviders: Bool = false
    ) async throws -> Client {
        let env = environment
        // Resolve OAuth key asynchronously if needed.
        let oauthKey = try await _resolveOpenAIApiKeyAsync(env: env, transport: transport)
        let providers = try _buildProviders(env: env, transport: transport, oauthApiKey: oauthKey, allowEmpty: allowEmptyProviders)
        return try Client(
            providers: providers,
            defaultProvider: providers.first?.key,
            middleware: middleware,
            completeMiddleware: completeMiddleware,
            streamMiddleware: streamMiddleware,
            modelCatalog: modelCatalog
        )
    }

    public static func fromEnvAsync(
        transport: OmniHTTP.HTTPTransport = OmniHTTP.URLSessionHTTPTransport(),
        modelCatalog: ModelCatalog = .default,
        middleware: [Middleware] = [],
        completeMiddleware: [CompleteMiddleware] = [],
        streamMiddleware: [StreamMiddleware] = [],
        allowEmptyProviders: Bool = false
    ) async throws -> Client {
        try await fromEnvAsync(
            environment: ProcessInfo.processInfo.environment,
            transport: transport,
            modelCatalog: modelCatalog,
            middleware: middleware,
            completeMiddleware: completeMiddleware,
            streamMiddleware: streamMiddleware,
            allowEmptyProviders: allowEmptyProviders
        )
    }

    private static func _resolveOpenAIApiKeyAsync(
        env: [String: String],
        transport: HTTPTransport
    ) async throws -> String? {
        func firstNonEmpty(_ keys: [String]) -> String? {
            for key in keys {
                if let value = env[key], !value.isEmpty { return value }
            }
            return nil
        }

        if let explicit = firstNonEmpty(["OPENAI_API_KEY", "DR_OPENAI_API_KEY"]) {
            return explicit
        }

        guard let idToken = firstNonEmpty(["OPENAI_OAUTH_ID_TOKEN", "DR_OPENAI_OAUTH_ID_TOKEN"]) else {
            return nil
        }

        let issuer = firstNonEmpty(["OPENAI_OAUTH_ISSUER", "DR_OPENAI_OAUTH_ISSUER"])
            ?? "https://auth.openai.com"
        let clientID = firstNonEmpty(["OPENAI_OAUTH_CLIENT_ID", "DR_OPENAI_OAUTH_CLIENT_ID"])
            ?? "app_EMoamEEZ73f0CkXaXp7hrann"

        return try await exchangeCodexOAuthIDTokenForOpenAIApiKey(
            idToken: idToken,
            issuer: issuer,
            clientID: clientID,
            transport: transport
        )
    }

    private static func _buildProviders(
        env: [String: String],
        transport: HTTPTransport,
        oauthApiKey: String?,
        allowEmpty: Bool
    ) throws -> [String: ProviderAdapter] {
        func firstNonEmpty(_ keys: [String]) -> String? {
            for key in keys {
                if let value = env[key], !value.isEmpty { return value }
            }
            return nil
        }

        var providers: [String: ProviderAdapter] = [:]

        let openaiKey = firstNonEmpty(["OPENAI_API_KEY", "DR_OPENAI_API_KEY"]) ?? oauthApiKey
        if let key = openaiKey {
            providers["openai"] = OpenAIAdapter(
                apiKey: key,
                baseURL: env["OPENAI_BASE_URL"],
                organizationID: env["OPENAI_ORG_ID"],
                projectID: env["OPENAI_PROJECT_ID"],
                transport: transport
            )
        }
        if let key = firstNonEmpty(["ANTHROPIC_API_KEY", "DR_ANTHROPIC_API_KEY"]) {
            providers["anthropic"] = AnthropicAdapter(apiKey: key, baseURL: env["ANTHROPIC_BASE_URL"], transport: transport)
        }
        if let key = firstNonEmpty(["GEMINI_API_KEY", "DR_GEMINI_API_KEY", "GOOGLE_API_KEY"]) {
            providers["gemini"] = GeminiAdapter(apiKey: key, baseURL: env["GEMINI_BASE_URL"], transport: transport)
        }
        if let key = env["CEREBRAS_API_KEY"], !key.isEmpty {
            providers["cerebras"] = CerebrasAdapter(apiKey: key, baseURL: env["CEREBRAS_BASE_URL"], transport: transport)
        }
        if let key = env["GROQ_API_KEY"], !key.isEmpty {
            providers["groq"] = GroqAdapter(apiKey: key, baseURL: env["GROQ_BASE_URL"], transport: transport)
        }

        if providers.isEmpty && !allowEmpty {
            throw ConfigurationError(message: "No providers configured. Set OPENAI_API_KEY (or DR_OPENAI_API_KEY) or OPENAI_OAUTH_ID_TOKEN (or DR_OPENAI_OAUTH_ID_TOKEN) and/or ANTHROPIC_API_KEY (or DR_ANTHROPIC_API_KEY) and/or GEMINI_API_KEY (or DR_GEMINI_API_KEY) and/or CEREBRAS_API_KEY and/or GROQ_API_KEY.")
        }

        return providers
    }

    public static func fromEnv(
        transport: OmniHTTP.HTTPTransport = OmniHTTP.URLSessionHTTPTransport(),
        modelCatalog: ModelCatalog = .default,
        middleware: [Middleware] = [],
        completeMiddleware: [CompleteMiddleware] = [],
        streamMiddleware: [StreamMiddleware] = [],
        allowEmptyProviders: Bool = false
    ) throws -> Client {
        try fromEnv(
            environment: ProcessInfo.processInfo.environment,
            transport: transport,
            modelCatalog: modelCatalog,
            middleware: middleware,
            completeMiddleware: completeMiddleware,
            streamMiddleware: streamMiddleware,
            allowEmptyProviders: allowEmptyProviders
        )
    }

    public static func fromEnvAllowingEmpty(
        environment: [String: String],
        transport: OmniHTTP.HTTPTransport = OmniHTTP.URLSessionHTTPTransport(),
        modelCatalog: ModelCatalog = .default,
        middleware: [Middleware] = [],
        completeMiddleware: [CompleteMiddleware] = [],
        streamMiddleware: [StreamMiddleware] = []
    ) throws -> Client {
        try fromEnv(
            environment: environment,
            transport: transport,
            modelCatalog: modelCatalog,
            middleware: middleware,
            completeMiddleware: completeMiddleware,
            streamMiddleware: streamMiddleware,
            allowEmptyProviders: true
        )
    }

    public static func fromEnvAllowingEmpty(
        transport: OmniHTTP.HTTPTransport = OmniHTTP.URLSessionHTTPTransport(),
        modelCatalog: ModelCatalog = .default,
        middleware: [Middleware] = [],
        completeMiddleware: [CompleteMiddleware] = [],
        streamMiddleware: [StreamMiddleware] = []
    ) throws -> Client {
        try fromEnv(
            environment: ProcessInfo.processInfo.environment,
            transport: transport,
            modelCatalog: modelCatalog,
            middleware: middleware,
            completeMiddleware: completeMiddleware,
            streamMiddleware: streamMiddleware,
            allowEmptyProviders: true
        )
    }

    public func register(provider name: String, adapter: ProviderAdapter) {
        _withStateLock {
            providers[name] = adapter
            if _defaultProvider == nil {
                _defaultProvider = name
            }
        }
    }

    public func registerProvider(_ name: String, adapter: ProviderAdapter) {
        register(provider: name, adapter: adapter)
    }

    public func setDefault(provider: String) {
        _withStateLock {
            _defaultProvider = provider
        }
    }

    public func close() async {
        for (_, adapter) in _withStateLock({ providers }) {
            await adapter.close()
        }
    }

    public func listModels(provider: String? = nil) -> [ModelInfo] {
        modelCatalog.listModels(provider: provider)
    }

    public func getModelInfo(_ id: String) -> ModelInfo? {
        modelCatalog.getModelInfo(id)
    }

    public func getLatestModel(provider: String, capability: ModelCapability? = nil) -> ModelInfo? {
        modelCatalog.getLatestModel(provider: provider, capability: capability)
    }

    public func complete(_ request: Request) async throws -> Response {
        let adapter = try resolveAdapter(for: request)
        let middleware = _withStateLock { _middleware }
        let base: @Sendable (Request) async throws -> Response = { req in
            try await adapter.complete(request: req)
        }

        var handler = base
        for m in middleware.reversed() {
            let next = handler
            handler = { req in
                try await m.complete(request: req, next: next)
            }
        }
        return try await handler(request)
    }

    public func complete(request: Request) async throws -> Response {
        try await complete(request)
    }

    public func stream(_ request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        let adapter = try resolveAdapter(for: request)
        let middleware = _withStateLock { _middleware }
        let base: @Sendable (Request) async throws -> AsyncThrowingStream<StreamEvent, Error> = { req in
            try await adapter.stream(request: req)
        }

        var handler = base
        for m in middleware.reversed() {
            let next = handler
            handler = { req in
                try await m.stream(request: req, next: next)
            }
        }
        let upstream = try await handler(request)
        if let inactivityTimeout = request.timeout?.asConfig.total,
           Self.durationSeconds(inactivityTimeout) > 0
        {
            return Self.withInactivityWatchdog(
                stream: upstream,
                inactivityTimeout: inactivityTimeout
            )
        }
        return upstream
    }

    public func stream(request: Request) async throws -> AsyncThrowingStream<StreamEvent, Error> {
        try await stream(request)
    }

    public func addMiddleware(_ middleware: Middleware) {
        _withStateLock {
            _middleware.append(middleware)
        }
    }

    public func addMiddleware(_ middleware: @escaping CompleteMiddleware) {
        addMiddleware(_ClosureMiddleware(complete: middleware, stream: nil))
    }

    public func addStreamMiddleware(_ middleware: @escaping StreamMiddleware) {
        addMiddleware(_ClosureMiddleware(complete: nil, stream: middleware))
    }

    private func _withStateLock<T>(_ body: () throws -> T) rethrows -> T {
        stateLock.lock()
        defer { stateLock.unlock() }
        return try body()
    }

    func resolveAdapter(provider: String?) throws -> ProviderAdapter {
        try _withStateLock {
            if let provider {
                guard let adapter = providers[provider] else {
                    throw ConfigurationError(message: "Provider '\(provider)' not configured")
                }
                return adapter
            }
            guard let def = _defaultProvider else {
                throw ConfigurationError(message: "No default provider configured and provider is nil")
            }
            guard let adapter = providers[def] else {
                throw ConfigurationError(message: "Default provider '\(def)' not configured")
            }
            return adapter
        }
    }

    private func resolveAdapter(for request: Request) throws -> ProviderAdapter {
        try resolveAdapter(provider: request.provider)
    }

    public func embed(_ request: EmbedRequest) async throws -> EmbedResponse {
        let adapter = try resolveAdapter(provider: request.provider)
        guard let embeddingAdapter = adapter as? EmbeddingProviderAdapter else {
            throw UnsupportedCapabilityError(provider: request.provider ?? defaultProvider, capability: "embeddings")
        }
        return try await embeddingAdapter.embed(request: request)
    }

    public func sendToolOutputs(_ request: ToolContinuationRequest) async throws -> Response {
        let adapter = try resolveAdapter(provider: request.provider)
        guard let continuationAdapter = adapter as? ToolContinuationProviderAdapter else {
            throw UnsupportedCapabilityError(provider: request.provider ?? defaultProvider, capability: "tool_continuation")
        }
        return try await continuationAdapter.sendToolOutputs(request: request)
    }

    private static func withInactivityWatchdog(
        stream: AsyncThrowingStream<StreamEvent, Error>,
        inactivityTimeout: Duration
    ) -> AsyncThrowingStream<StreamEvent, Error> {
        let activityState = StreamActivityState(start: ContinuousClock.now)
        return AsyncThrowingStream<StreamEvent, Error> { continuation in
            let producer = Task {
                do {
                    for try await event in stream {
                        if isActivityEvent(event) {
                            await activityState.markActivity()
                        }
                        continuation.yield(event)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }

            let monitor = Task {
                while !Task.isCancelled {
                    try? await Task.sleep(for: .seconds(1))
                    let snapshot = await activityState.snapshot()
                    if (ContinuousClock.now - snapshot.lastActivity) >= inactivityTimeout {
                        producer.cancel()
                        continuation.finish(throwing: RequestTimeoutError(
                            message: "Stream inactivity timeout after \(Int(durationSeconds(inactivityTimeout)))s"
                        ))
                        return
                    }
                }
            }

            continuation.onTermination = { @Sendable _ in
                producer.cancel()
                monitor.cancel()
            }
        }
    }

    private static func isActivityEvent(_ event: StreamEvent) -> Bool {
        switch event.type.rawValue {
        case StreamEventType.textStart.rawValue,
             StreamEventType.textDelta.rawValue,
             StreamEventType.reasoningDelta.rawValue,
             StreamEventType.toolCallStart.rawValue,
             StreamEventType.toolCallDelta.rawValue,
             StreamEventType.toolCallEnd.rawValue,
             StreamEventType.finish.rawValue:
            return true
        default:
            return false
        }
    }

    private static func durationSeconds(_ duration: Duration) -> Double {
        Double(duration.components.seconds) + Double(duration.components.attoseconds) / 1e18
    }
}

private actor StreamActivityState {
    private let start: ContinuousClock.Instant
    private var lastActivity: ContinuousClock.Instant

    init(start: ContinuousClock.Instant) {
        self.start = start
        self.lastActivity = start
    }

    func markActivity() {
        lastActivity = ContinuousClock.now
    }

    func snapshot() -> (start: ContinuousClock.Instant, lastActivity: ContinuousClock.Instant) {
        (start, lastActivity)
    }
}
