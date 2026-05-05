import Foundation

import OmniHTTP

enum _ProviderHTTP {
    static func makeURL(baseURL: String, path: String, query: [String: String] = [:]) throws -> URL {
        let base = baseURL.hasSuffix("/") ? String(baseURL.dropLast()) : baseURL
        let p = path.hasPrefix("/") ? path : "/" + path
        guard var comps = URLComponents(string: base + p) else {
            throw OmniHTTPError.invalidURL(base + p)
        }
        if !query.isEmpty {
            comps.queryItems = (comps.queryItems ?? []) + query.map { URLQueryItem(name: $0.key, value: $0.value) }
        }
        guard let url = comps.url else {
            throw OmniHTTPError.invalidURL(base + p)
        }
        return url
    }

    static func parseRetryAfterSeconds(_ headers: HTTPHeaders) -> TimeInterval? {
        guard let v = headers.firstValue(for: "retry-after") else { return nil }
        if let s = Double(v.trimmingCharacters(in: .whitespacesAndNewlines)) {
            return s
        }
        // HTTP-date parsing is intentionally omitted; most providers return seconds.
        return nil
    }

    static func parseJSONBody(_ response: HTTPResponse) -> JSONValue {
        (try? JSONValue.parse(response.body)) ?? .object([:])
    }

    static func jsonBytes(_ value: JSONValue) throws -> [UInt8] {
        try Array(value.data(prettyPrinted: false))
    }

    static func errorMessage(from json: JSONValue?) -> (message: String?, code: String?) {
        guard let json else { return (nil, nil) }
        if let msg = json["message"]?.stringValue {
            return (msg, json["code"]?.stringValue)
        }
        // Common provider shape: { "error": { "message": "...", "type": "...", "code": "..." } }
        if let err = json["error"] {
            let message = err["message"]?.stringValue ?? err["error"]?.stringValue
            let code = err["code"]?.stringValue ?? err["type"]?.stringValue
            return (message, code)
        }
        // Anthropic often uses { "error": { "type": "...", "message": "..." } }
        if let err = json["error"] {
            return (err["message"]?.stringValue, err["type"]?.stringValue)
        }
        return (nil, nil)
    }

    static func mimeType(forPath path: String) -> String? {
        let lower = (path as NSString).pathExtension.lowercased()
        switch lower {
        case "png": return "image/png"
        case "jpg", "jpeg": return "image/jpeg"
        case "gif": return "image/gif"
        case "webp": return "image/webp"
        case "heic": return "image/heic"
        case "heif": return "image/heif"
        case "pdf": return "application/pdf"
        default: return nil
        }
    }

    static func isProbablyLocalFilePath(_ s: String) -> Bool {
        if s.hasPrefix("/") { return true }
        if s.hasPrefix("./") { return true }
        if s.hasPrefix("../") { return true }
        if s.hasPrefix("~") { return true }
        return false
    }

    static func readLocalFileBytes(_ path: String) throws -> [UInt8] {
        let expanded = (path as NSString).expandingTildeInPath
        let url = URL(fileURLWithPath: expanded)
        let data = try Data(contentsOf: url)
        return Array(data)
    }

    static func base64(_ bytes: [UInt8]) -> String {
        Data(bytes).base64EncodedString()
    }

    static func stringifyJSON(_ value: JSONValue) -> String {
        switch value {
        case .string(let s):
            return s
        default:
            if let data = try? value.data(prettyPrinted: false), let s = String(data: data, encoding: .utf8) {
                return s
            }
            return String(describing: value)
        }
    }

    static func parseRateLimitInfo(_ headers: HTTPHeaders) -> RateLimitInfo? {
        func int(_ name: String) -> Int? {
            guard let v = headers.firstValue(for: name) else { return nil }
            return Int(v.trimmingCharacters(in: .whitespacesAndNewlines))
        }
        let rr = int("x-ratelimit-remaining-requests") ?? int("x-ratelimit-requests-remaining")
        let rl = int("x-ratelimit-limit-requests") ?? int("x-ratelimit-requests-limit")
        let tr = int("x-ratelimit-remaining-tokens") ?? int("x-ratelimit-tokens-remaining")
        let tl = int("x-ratelimit-limit-tokens") ?? int("x-ratelimit-tokens-limit")
        if rr == nil, rl == nil, tr == nil, tl == nil {
            return nil
        }
        return RateLimitInfo(requestsRemaining: rr, requestsLimit: rl, tokensRemaining: tr, tokensLimit: tl, resetAt: nil)
    }
}

