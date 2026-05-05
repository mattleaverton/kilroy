import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

public struct WebSearchResult: Sendable, Equatable {
    public let title: String
    public let url: String
    public let snippet: String

    public init(title: String, url: String, snippet: String) {
        self.title = title
        self.url = url
        self.snippet = snippet
    }
}

public enum WebSearchError: Error, LocalizedError, Sendable {
    case invalidQuery(String)
    case networkError(String)
    case parseError(String)

    public var errorDescription: String? {
        switch self {
        case .invalidQuery(let msg):
            return "Invalid query: \(msg)"
        case .networkError(let msg):
            return "Network error: \(msg)"
        case .parseError(let msg):
            return "Parse error: \(msg)"
        }
    }
}

public struct WebSearchClient: Sendable {
    private static let endpoint = "https://api.duckduckgo.com/"

    public static func search(
        query: String,
        allowedDomains: [String]? = nil,
        blockedDomains: [String]? = nil,
        maxResults: Int = 10
    ) async throws -> [WebSearchResult] {
        guard let encodedQuery = query.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) else {
            throw WebSearchError.invalidQuery("Could not URL encode query")
        }

        guard let url = URL(string: "\(endpoint)?q=\(encodedQuery)&format=json&no_html=1&skip_disambig=1") else {
            throw WebSearchError.invalidQuery("Could not construct query URL")
        }

        let (data, response): (Data, URLResponse)
        do {
            (data, response) = try await URLSession.shared.data(from: url)
        } catch {
            throw WebSearchError.networkError(error.localizedDescription)
        }

        guard let http = response as? HTTPURLResponse else {
            throw WebSearchError.networkError("Invalid HTTP response")
        }
        guard (200..<300).contains(http.statusCode) else {
            throw WebSearchError.networkError("HTTP \(http.statusCode)")
        }

        let parsed: DuckDuckGoResponse
        do {
            parsed = try JSONDecoder().decode(DuckDuckGoResponse.self, from: data)
        } catch {
            throw WebSearchError.parseError(error.localizedDescription)
        }

        var results = extractResults(from: parsed)
        results = filter(results: results, allowedDomains: allowedDomains, blockedDomains: blockedDomains)
        return Array(results.prefix(max(1, min(maxResults, 20))))
    }

    private static func extractResults(from response: DuckDuckGoResponse) -> [WebSearchResult] {
        var output: [WebSearchResult] = []

        if let abstractURL = response.abstractURL,
           let abstractText = response.abstractText,
           !abstractURL.isEmpty,
           !abstractText.isEmpty {
            output.append(
                WebSearchResult(
                    title: response.heading ?? domainTitle(from: abstractURL),
                    url: abstractURL,
                    snippet: abstractText
                )
            )
        }

        for topic in response.relatedTopics ?? [] {
            if let result = extractResult(from: topic) {
                output.append(result)
            }
        }

        for result in response.results ?? [] {
            guard let firstURL = result.firstURL, !firstURL.isEmpty else { continue }
            let text = result.text ?? ""
            let (title, snippet) = parseText(text, fallbackURL: firstURL)
            output.append(WebSearchResult(title: title, url: firstURL, snippet: snippet))
        }

        return dedupe(results: output)
    }

    private static func extractResult(from topic: DuckDuckGoTopic) -> WebSearchResult? {
        if let nested = topic.topics {
            for child in nested {
                if let hit = extractResult(from: child) {
                    return hit
                }
            }
            return nil
        }

        guard let firstURL = topic.firstURL, !firstURL.isEmpty else {
            return nil
        }

        let text = topic.text ?? ""
        let (title, snippet) = parseText(text, fallbackURL: firstURL)
        return WebSearchResult(title: title, url: firstURL, snippet: snippet)
    }

    private static func parseText(_ text: String, fallbackURL: String) -> (String, String) {
        if let dash = text.range(of: " - ") {
            let title = String(text[..<dash.lowerBound]).trimmingCharacters(in: .whitespacesAndNewlines)
            let snippet = String(text[dash.upperBound...]).trimmingCharacters(in: .whitespacesAndNewlines)
            return (title.isEmpty ? domainTitle(from: fallbackURL) : title, snippet)
        }
        return (domainTitle(from: fallbackURL), text.trimmingCharacters(in: .whitespacesAndNewlines))
    }

    private static func domainTitle(from url: String) -> String {
        guard let host = URL(string: url)?.host?.lowercased() else {
            return "Result"
        }
        return host.hasPrefix("www.") ? String(host.dropFirst(4)) : host
    }

    private static func filter(
        results: [WebSearchResult],
        allowedDomains: [String]?,
        blockedDomains: [String]?
    ) -> [WebSearchResult] {
        var filtered = results

        if let allowedDomains, !allowedDomains.isEmpty {
            filtered = filtered.filter { result in
                url(result.url, matchesAnyDomain: allowedDomains)
            }
        }

        if let blockedDomains, !blockedDomains.isEmpty {
            filtered = filtered.filter { result in
                !url(result.url, matchesAnyDomain: blockedDomains)
            }
        }

        return filtered
    }

    private static func url(_ value: String, matchesAnyDomain domains: [String]) -> Bool {
        guard let host = URL(string: value)?.host?.lowercased() else { return false }
        for rawDomain in domains {
            let normalized = rawDomain.lowercased().trimmingCharacters(in: .whitespacesAndNewlines)
            if normalized.isEmpty { continue }
            if host == normalized || host.hasSuffix(".\(normalized)") {
                return true
            }
        }
        return false
    }

    private static func dedupe(results: [WebSearchResult]) -> [WebSearchResult] {
        var seen: Set<String> = []
        var deduped: [WebSearchResult] = []
        for result in results {
            if seen.insert(result.url).inserted {
                deduped.append(result)
            }
        }
        return deduped
    }
}

public struct WebFetchResult: Sendable {
    public let content: String
    public let originalURL: URL
    public let finalURL: URL
    public let statusCode: Int

    public init(content: String, originalURL: URL, finalURL: URL, statusCode: Int) {
        self.content = content
        self.originalURL = originalURL
        self.finalURL = finalURL
        self.statusCode = statusCode
    }

    public var wasRedirected: Bool {
        originalURL.absoluteString != finalURL.absoluteString
    }
}

public enum WebFetchError: Error, LocalizedError, Sendable {
    case invalidURL(String)
    case tooManyRedirects(Int)
    case httpError(Int, URL)
    case decodingError(URL)
    case networkError(String)
    case invalidResponse
    case crossHostRedirect(URL, URL)

    public var errorDescription: String? {
        switch self {
        case .invalidURL(let s):
            return "Invalid URL: \(s)"
        case .tooManyRedirects(let count):
            return "Too many redirects (\(count))"
        case .httpError(let status, let url):
            return "HTTP \(status) from \(url.absoluteString)"
        case .decodingError(let url):
            return "Could not decode response from \(url.absoluteString)"
        case .networkError(let msg):
            return "Network error: \(msg)"
        case .invalidResponse:
            return "Invalid HTTP response"
        case .crossHostRedirect(let from, let to):
            return "Cross-host redirect blocked: \(from.host ?? "unknown") -> \(to.host ?? "unknown")"
        }
    }
}

public struct WebFetchLoader: Sendable {
    public let maxRedirects: Int
    public let allowCrossHostRedirects: Bool
    public let timeout: TimeInterval

    public init(maxRedirects: Int = 10, allowCrossHostRedirects: Bool = true, timeout: TimeInterval = 30) {
        self.maxRedirects = maxRedirects
        self.allowCrossHostRedirects = allowCrossHostRedirects
        self.timeout = timeout
    }

    public func fetch(_ rawURL: String) async throws -> WebFetchResult {
        let upgraded = upgradeToHTTPSIfNeeded(rawURL)
        guard let originalURL = URL(string: upgraded) else {
            throw WebFetchError.invalidURL(rawURL)
        }

        var currentURL = originalURL
        var redirects = 0

        let sessionConfig = URLSessionConfiguration.ephemeral
        sessionConfig.timeoutIntervalForRequest = timeout
        sessionConfig.httpShouldSetCookies = false

        let session = URLSession(configuration: sessionConfig, delegate: RedirectBlockingDelegate(), delegateQueue: nil)
        defer { session.invalidateAndCancel() }

        while redirects <= maxRedirects {
            var request = URLRequest(url: currentURL)
            request.httpMethod = "GET"
            request.timeoutInterval = timeout
            request.setValue("Mozilla/5.0 (compatible; OmniAIAgent/1.0)", forHTTPHeaderField: "User-Agent")

            let data: Data
            let response: URLResponse
            do {
                (data, response) = try await session.data(for: request)
            } catch {
                throw WebFetchError.networkError(error.localizedDescription)
            }

            guard let http = response as? HTTPURLResponse else {
                throw WebFetchError.invalidResponse
            }

            if (300..<400).contains(http.statusCode) {
                guard let location = http.value(forHTTPHeaderField: "Location"),
                      let next = URL(string: location, relativeTo: currentURL)?.absoluteURL else {
                    throw WebFetchError.httpError(http.statusCode, currentURL)
                }

                if !allowCrossHostRedirects && next.host != currentURL.host {
                    throw WebFetchError.crossHostRedirect(currentURL, next)
                }

                redirects += 1
                if redirects > maxRedirects {
                    throw WebFetchError.tooManyRedirects(redirects)
                }
                currentURL = next
                continue
            }

            guard (200..<300).contains(http.statusCode) else {
                throw WebFetchError.httpError(http.statusCode, currentURL)
            }

            guard let content = String(data: data, encoding: .utf8) else {
                throw WebFetchError.decodingError(currentURL)
            }

            return WebFetchResult(
                content: content,
                originalURL: originalURL,
                finalURL: currentURL,
                statusCode: http.statusCode
            )
        }

        throw WebFetchError.tooManyRedirects(redirects)
    }

    private func upgradeToHTTPSIfNeeded(_ urlString: String) -> String {
        guard urlString.hasPrefix("http://"),
              let url = URL(string: urlString),
              let host = url.host else {
            return urlString
        }
        if host == "localhost" || host == "127.0.0.1" || host == "::1" {
            return urlString
        }
        return "https://" + urlString.dropFirst("http://".count)
    }
}

private final class RedirectBlockingDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        willPerformHTTPRedirection response: HTTPURLResponse,
        newRequest request: URLRequest,
        completionHandler: @escaping @Sendable (URLRequest?) -> Void
    ) {
        completionHandler(nil)
    }
}

public func stripHTMLForToolOutput(_ html: String) -> String {
    var text = html
    text = text.replacingOccurrences(of: "<script[^>]*>[\\s\\S]*?</script>", with: "", options: .regularExpression)
    text = text.replacingOccurrences(of: "<style[^>]*>[\\s\\S]*?</style>", with: "", options: .regularExpression)
    text = text.replacingOccurrences(of: "<[^>]+>", with: "", options: .regularExpression)
    text = text.replacing("&nbsp;", with: " ")
    text = text.replacing("&amp;", with: "&")
    text = text.replacing("&lt;", with: "<")
    text = text.replacing("&gt;", with: ">")
    text = text.replacing("&quot;", with: "\"")
    text = text.replacing("&#39;", with: "'")
    text = text.replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)
    return text.trimmingCharacters(in: .whitespacesAndNewlines)
}

private struct DuckDuckGoResponse: Decodable {
    let abstractText: String?
    let abstractURL: String?
    let heading: String?
    let relatedTopics: [DuckDuckGoTopic]?
    let results: [DuckDuckGoResult]?

    enum CodingKeys: String, CodingKey {
        case abstractText = "AbstractText"
        case abstractURL = "AbstractURL"
        case heading = "Heading"
        case relatedTopics = "RelatedTopics"
        case results = "Results"
    }
}

private struct DuckDuckGoTopic: Decodable {
    let firstURL: String?
    let text: String?
    let topics: [DuckDuckGoTopic]?

    enum CodingKeys: String, CodingKey {
        case firstURL = "FirstURL"
        case text = "Text"
        case topics = "Topics"
    }
}

private struct DuckDuckGoResult: Decodable {
    let firstURL: String?
    let text: String?

    enum CodingKeys: String, CodingKey {
        case firstURL = "FirstURL"
        case text = "Text"
    }
}
