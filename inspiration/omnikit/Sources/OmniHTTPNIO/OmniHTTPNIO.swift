import Foundation

import AsyncHTTPClient
import NIOCore
import NIOHTTP1
import NIOPosix

#if canImport(NIOTransportServices) && (os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS))
import NIOTransportServices
#endif

import OmniHTTP

public final class NIOHTTPTransport: HTTPTransport, @unchecked Sendable {
    public struct Configuration: Sendable {
        public var maximumBodySizeBytes: Int

        public init(maximumBodySizeBytes: Int = 64 * 1024 * 1024) {
            self.maximumBodySizeBytes = maximumBodySizeBytes
        }
    }

    private let client: HTTPClient
    private let eventLoopGroup: EventLoopGroup?
    private let ownsEventLoopGroup: Bool
    private let configuration: Configuration

    public init(
        configuration: Configuration = Configuration(),
        httpClientConfiguration: HTTPClient.Configuration = HTTPClient.Configuration(),
        eventLoopGroup: EventLoopGroup? = nil
    ) {
        self.configuration = configuration
        if let eventLoopGroup {
            self.eventLoopGroup = eventLoopGroup
            self.ownsEventLoopGroup = false
            self.client = HTTPClient(eventLoopGroupProvider: .shared(eventLoopGroup), configuration: httpClientConfiguration)
        } else {
            let group = Self.makePlatformDefaultEventLoopGroup()
            self.eventLoopGroup = group
            self.ownsEventLoopGroup = true
            self.client = HTTPClient(eventLoopGroupProvider: .shared(group), configuration: httpClientConfiguration)
        }
    }

    deinit {
        // Best-effort: if users forget to call shutdown, the process will clean up. We avoid async work here.
    }

    public func shutdown() async throws {
        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
            client.shutdown { err in
                if let err { cont.resume(throwing: err) } else { cont.resume(returning: ()) }
            }
        }
        if ownsEventLoopGroup, let group = eventLoopGroup {
            try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, Error>) in
                group.shutdownGracefully { err in
                    if let err { cont.resume(throwing: err) } else { cont.resume(returning: ()) }
                }
            }
        }
    }

    public func send(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPResponse {
        let res = try await openStream(request, timeout: timeout)

        var out: [UInt8] = []
        out.reserveCapacity(8 * 1024)
        var total = 0
        for try await chunk in res.body {
            total += chunk.count
            if total > configuration.maximumBodySizeBytes {
                throw OmniHTTPError.invalidResponse
            }
            out.append(contentsOf: chunk)
        }
        return HTTPResponse(statusCode: res.statusCode, headers: res.headers, body: out)
    }

    public func openStream(_ request: HTTPRequest, timeout: Duration?) async throws -> HTTPStreamResponse {
        var headers = NIOHTTP1.HTTPHeaders()
        for (k, v) in request.headers.asDictionary {
            headers.add(name: k, value: v)
        }

        let timeoutAmount: TimeAmount? = timeout.map { d in
            let seconds = Int64(d.components.seconds)
            let nanosFromAttos = d.components.attoseconds / 1_000_000_000 // 1ns == 1e9 attoseconds
            return .nanoseconds(seconds * 1_000_000_000 + nanosFromAttos)
        }

        // Use the modern AsyncHTTPClient request/response APIs when available.
        if #available(macOS 12.0, iOS 15.0, tvOS 15.0, watchOS 8.0, visionOS 1.0, *) {
            var req = HTTPClientRequest(url: request.url.absoluteString)
            switch request.method {
            case .get: req.method = .GET
            case .post: req.method = .POST
            case .put: req.method = .PUT
            case .patch: req.method = .PATCH
            case .delete: req.method = .DELETE
            case .head: req.method = .HEAD
            case .options: req.method = .OPTIONS
            }
            req.headers = headers
            switch request.body {
            case .none:
                break
            case .bytes(let bytes):
                req.body = .bytes(bytes)
            }

            let response: HTTPClientResponse
            if let timeoutAmount {
                response = try await client.execute(req, timeout: timeoutAmount)
            } else {
                response = try await client.execute(req, deadline: .distantFuture)
            }

            var outHeaders = HTTPHeaders()
            for (name, value) in response.headers {
                outHeaders.add(name: name, value: value)
            }

            let stream: HTTPByteStream = AsyncThrowingStream { continuation in
                Task {
                    do {
                        for try await part in response.body {
                            var buf = part
                            let bytes = buf.readBytes(length: buf.readableBytes) ?? []
                            if !bytes.isEmpty { continuation.yield(bytes) }
                        }
                        continuation.finish()
                    } catch {
                        continuation.finish(throwing: error)
                    }
                }
            }

            return HTTPStreamResponse(statusCode: Int(response.status.code), headers: outHeaders, body: stream)
        } else {
            // Fallback to non-streaming on older Apple OS versions; the package minimums should avoid this path.
            throw OmniHTTPError.streamingNotSupported
        }
    }

    private static func makePlatformDefaultEventLoopGroup() -> EventLoopGroup {
        #if canImport(NIOTransportServices) && (os(macOS) || os(iOS) || os(tvOS) || os(watchOS) || os(visionOS))
        return NIOTSEventLoopGroup(loopCount: System.coreCount)
        #else
        return MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        #endif
    }
}
