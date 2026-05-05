import Foundation

open class SDKError: Error, @unchecked Sendable {
    public let message: String
    public let cause: (any Error)?

    open var retryable: Bool { false }
    open var retryAfter: TimeInterval? { nil }

    public init(message: String, cause: (any Error)? = nil) {
        self.message = message
        self.cause = cause
    }
}

open class ProviderError: SDKError, @unchecked Sendable {
    public let provider: String
    public let statusCode: Int?
    public let errorCode: String?
    public let raw: JSONValue?

    private let _retryable: Bool
    private let _retryAfter: TimeInterval?

    public override var retryable: Bool { _retryable }
    public override var retryAfter: TimeInterval? { _retryAfter }

    public init(
        message: String,
        provider: String,
        statusCode: Int?,
        errorCode: String? = nil,
        retryable: Bool,
        retryAfter: TimeInterval? = nil,
        raw: JSONValue? = nil,
        cause: (any Error)? = nil
    ) {
        self.provider = provider
        self.statusCode = statusCode
        self.errorCode = errorCode
        self._retryable = retryable
        self._retryAfter = retryAfter
        self.raw = raw
        super.init(message: message, cause: cause)
    }
}

public final class AuthenticationError: ProviderError, @unchecked Sendable {}
public final class AccessDeniedError: ProviderError, @unchecked Sendable {}
public final class NotFoundError: ProviderError, @unchecked Sendable {}
public final class InvalidRequestError: ProviderError, @unchecked Sendable {}
public final class RateLimitError: ProviderError, @unchecked Sendable {}
public final class ServerError: ProviderError, @unchecked Sendable {}
public final class ContentFilterError: ProviderError, @unchecked Sendable {}
public final class ContextLengthError: ProviderError, @unchecked Sendable {}
public final class QuotaExceededError: ProviderError, @unchecked Sendable {}

public final class RequestTimeoutError: SDKError, @unchecked Sendable {
    public override var retryable: Bool { true }
}

public final class AbortError: SDKError, @unchecked Sendable {
    public override var retryable: Bool { false }
}

public final class NetworkError: SDKError, @unchecked Sendable {
    public override var retryable: Bool { true }
}

public final class StreamError: SDKError, @unchecked Sendable {
    public override var retryable: Bool { true }
}

public final class InvalidToolCallError: SDKError, @unchecked Sendable {
    public override var retryable: Bool { false }
}

public final class NoObjectGeneratedError: SDKError, @unchecked Sendable {
    public override var retryable: Bool { false }
}

public final class ConfigurationError: SDKError, @unchecked Sendable {
    public override var retryable: Bool { false }
}

public final class UnsupportedToolChoiceError: SDKError, @unchecked Sendable {
    public override var retryable: Bool { false }
}

public final class UnsupportedCapabilityError: SDKError, @unchecked Sendable {
    public let provider: String?
    public let capability: String

    public override var retryable: Bool { false }

    public init(provider: String?, capability: String) {
        self.provider = provider
        self.capability = capability
        let providerLabel = provider.map { " provider '\($0)'" } ?? " configured provider"
        super.init(message: "Unsupported capability '\(capability)' for\(providerLabel).")
    }
}

enum _ErrorMapping {
    static func sdkErrorFromHTTP(
        provider: String,
        statusCode: Int?,
        message: String,
        errorCode: String?,
        retryAfter: TimeInterval?,
        raw: JSONValue?
    ) -> SDKError {
        let lc = message.lowercased()

        func parseRetryAfterFromMessage(_ lcMessage: String) -> TimeInterval? {
            // Common provider phrasing: "Please retry in 33.101321537s."
            guard let r = lcMessage.range(of: "retry in") else { return nil }
            var tail = lcMessage[r.upperBound...]
            while let first = tail.first, first == " " || first == "\t" || first == "\n" || first == "\r" {
                tail = tail.dropFirst()
            }
            var num = ""
            for ch in tail {
                if ch.isNumber || ch == "." {
                    num.append(ch)
                } else {
                    break
                }
            }
            guard !num.isEmpty, let v = Double(num) else { return nil }
            return v
        }

        let code = statusCode ?? -1
        let effectiveRetryAfter = retryAfter ?? parseRetryAfterFromMessage(lc)

        // Providers sometimes use "quota" phrasing for 429 rate limits; prefer the HTTP status.
        if code == 429 {
            return RateLimitError(
                message: message,
                provider: provider,
                statusCode: statusCode,
                errorCode: errorCode,
                retryable: true,
                retryAfter: effectiveRetryAfter,
                raw: raw
            )
        }

        // Message-based classification (Spec 6.5) for ambiguous provider behaviors.
        if lc.contains("content filter") || lc.contains("safety") {
            return ContentFilterError(
                message: message,
                provider: provider,
                statusCode: statusCode,
                errorCode: errorCode,
                retryable: false,
                retryAfter: retryAfter,
                raw: raw
            )
        }
        if lc.contains("quota") || lc.contains("insufficient quota") || lc.contains("insufficient_quota") || lc.contains("billing") {
            return QuotaExceededError(
                message: message,
                provider: provider,
                statusCode: statusCode,
                errorCode: errorCode,
                retryable: false,
                retryAfter: retryAfter,
                raw: raw
            )
        }
        if lc.contains("context length") || lc.contains("too many tokens") || lc.contains("maximum context") {
            return ContextLengthError(
                message: message,
                provider: provider,
                statusCode: statusCode,
                errorCode: errorCode,
                retryable: false,
                retryAfter: retryAfter,
                raw: raw
            )
        }

        // Defaults (per spec): unknown defaults to retryable.
        switch code {
        case 400, 422:
            return InvalidRequestError(
                message: message,
                provider: provider,
                statusCode: statusCode,
                errorCode: errorCode,
                retryable: false,
                retryAfter: retryAfter,
                raw: raw
            )
        case 401:
            return AuthenticationError(
                message: message,
                provider: provider,
                statusCode: statusCode,
                errorCode: errorCode,
                retryable: false,
                retryAfter: retryAfter,
                raw: raw
            )
        case 403:
            return AccessDeniedError(
                message: message,
                provider: provider,
                statusCode: statusCode,
                errorCode: errorCode,
                retryable: false,
                retryAfter: retryAfter,
                raw: raw
            )
        case 404:
            return NotFoundError(
                message: message,
                provider: provider,
                statusCode: statusCode,
                errorCode: errorCode,
                retryable: false,
                retryAfter: retryAfter,
                raw: raw
            )
        case 408:
            return RequestTimeoutError(message: message)
        case 413:
            return ContextLengthError(
                message: message,
                provider: provider,
                statusCode: statusCode,
                errorCode: errorCode,
                retryable: false,
                retryAfter: retryAfter,
                raw: raw
            )
        case 429:
            return RateLimitError(
                message: message,
                provider: provider,
                statusCode: statusCode,
                errorCode: errorCode,
                retryable: true,
                retryAfter: effectiveRetryAfter,
                raw: raw
            )
        case 500, 502, 503, 504:
            return ServerError(
                message: message,
                provider: provider,
                statusCode: statusCode,
                errorCode: errorCode,
                retryable: true,
                retryAfter: retryAfter,
                raw: raw
            )
        default:
            return ProviderError(
                message: message,
                provider: provider,
                statusCode: statusCode,
                errorCode: errorCode,
                retryable: true,
                retryAfter: retryAfter,
                raw: raw
            )
        }
    }
}
