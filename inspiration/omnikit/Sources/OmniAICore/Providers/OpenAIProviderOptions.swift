import Foundation

/// Internal provider option keys used by `OpenAIAdapter`.
public enum OpenAIProviderOptionKeys {
    /// Enables native OpenAI Responses API web search tool injection.
    public static let includeNativeWebSearch = "_omnikit_include_native_web_search"
    /// Optional native web search mode selector: `true` for live, `false` for cached.
    public static let webSearchExternalWebAccess = "_omnikit_web_search_external_web_access"
    /// Streaming transport for Responses API. Accepted values: `"sse"` (default), `"websocket"`.
    public static let responsesTransport = "_omnikit_responses_transport"
    /// Optional WebSocket base URL override (e.g. `wss://api.openai.com/v1`).
    public static let websocketBaseURL = "_omnikit_websocket_base_url"
    /// Hosted OpenAI Responses tools that should be injected directly into the request payload.
    public static let hostedTools = "_omnikit_hosted_tools"

    static let internalKeys: Set<String> = [
        includeNativeWebSearch,
        webSearchExternalWebAccess,
        responsesTransport,
        websocketBaseURL,
        hostedTools,
    ]
}
