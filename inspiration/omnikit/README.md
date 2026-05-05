# OmniKit

> Look, if you had one shot or one opportunity
<br>To seize everything you ever wanted in one moment
<br>Would you capture it or just let it slip?

OmniKit is my highly opinionated ultimate personal Swift Library. Swift 6.0+ with strict concurrency.

## OmniAI

Unified AI

### OmniAICore

Unified multi-provider LLM API (OpenAI, Anthropic, OpenRouter, Gemini, Groq, Cerebras, e.t.c).

Provider preferred APIs:

- OpenAI: Responses API (`/v1/responses`)
- Anthropic: Messages API (`/v1/messages`)
- Gemini: GenerateContent / StreamGenerateContent (`/v1beta/...:generateContent`)

### OmniAICoderAgent

Coding agent runtime with tool calling and state management with provider prompts and tools.

Implemented module: `OmniAIAgent`.

### OmniAIAgentsSDK

Framework for multi-agent workflows.

Current status in this repo: multi-agent orchestration is provided through `OmniAIAttractor` DOT pipelines.

### OmniACPModel

Typed Agent Client Protocol (ACP) model layer: JSON-RPC envelopes, ACP methods, notifications, and wire-compatible payload types.

### OmniACP

Actor-based Agent Client Protocol (ACP) client runtime with `stdio`, WebSocket, HTTP+SSE, and in-memory transports plus delegate-driven filesystem, permission, and terminal handling.

### OmniAIAttractor

A DOT-based pipeline runner that uses directed graphs to orchestrate multi-stage AI workflows.

### OmniAILLMClient

Unified LLM client for LLM providers with provider preferred APIs being used.

Implemented module: `OmniAICore`.

### OmniAIAgent

Agentic loop runtime with tool support and state management.

Implemented module: `OmniAIAgent`.

### OmniAICode

Interactive coding agent CLI.

Current status in this repo: `AttractorCLI` and agent backends are available; standalone `OmniAICode` CLI packaging is not split as a separate product yet.

## OmniUI (Experimental)

Drop in SwiftUI replacement bringing SwiftUI apps to other platforms. Simply changing `import SwiftUI` to `import OmniUI` gets apps running on another renderer.

### OmniUICore

1:1 SwiftUI APIs reimplemented.

- Non-renderer SwiftUI parity audit and roadmap: `docs/swiftui-non-renderer-parity.md`
- Refresh the audit report: `python3 scripts/swiftui_non_renderer_parity.py --check --swiftui-sdk auto --write-markdown docs/swiftui-non-renderer-parity.md`
- Raw exact-title symbol diff: `docs/swiftui-non-renderer-symbol-diff.md`
- Run the full non-renderer parity gate: `./scripts/run_swiftui_parity_gates.sh`
- The gate now also builds `OmniUINotcursesRenderer` / `KitchenSink` and runs a 1-second notcurses smoke pass.

### OmniUInotcursesRenderer

TUI renderer built on top of notcurses.

- Scrolling
- Mouse clicks
- Image support (on supported terminals)
- Animations
- Color support

Implemented module: `OmniUINotcursesRenderer`.

### OmniUIAdwaitaRenderer (Planned)

GTK renderer using Adwaita theme pack.

### OmniUIWeb (Planned)

Render to web with opinionated styling so SwiftUI apps can target the web.

### OmniUILVGL (Planned)

LVGL renderer.


## Core Networking

- `OmniHTTP`: lightweight HTTP abstraction + SSE parser.
  - `URLSessionHTTPTransport` works on Apple platforms and Linux via `FoundationNetworking`.
- `OmniHTTPNIO`: `NIOHTTPTransport` built on `swift-nio` + `async-http-client` (uses `swift-nio-transport-services` on Apple platforms when available).

## OmniAICore Details

### Environment Variables

- `OPENAI_API_KEY` (optional: `OPENAI_ORG_ID`, `OPENAI_PROJECT_ID`)
- OpenAI Codex OAuth alternative (token exchange to API key):
  - `OPENAI_OAUTH_ID_TOKEN` (or `DR_OPENAI_OAUTH_ID_TOKEN`)
  - optional `OPENAI_OAUTH_CLIENT_ID` (default: Codex client id)
  - optional `OPENAI_OAUTH_ISSUER` (default: `https://auth.openai.com`)
- `ANTHROPIC_API_KEY`
- `GEMINI_API_KEY` (or `GOOGLE_API_KEY`)
- `CEREBRAS_API_KEY`
- `GROQ_API_KEY`

### Quickstart

```swift
import OmniAICore

let client = try Client.fromEnv()

let result = try await generate(
    model: "gpt-5-nano-2025-08-07",
    prompt: "Return exactly the word hello.",
    provider: "openai",
    client: client
)

print(result.text)
```

### Use SwiftNIO Transport

```swift
import OmniAICore
import OmniHTTPNIO

let client = try Client.fromEnv(transport: NIOHTTPTransport())
```

### Tool Calling

```swift
import OmniAICore

let schema: JSONValue = .object([
    "type": .string("object"),
    "properties": .object([
        "a": .object(["type": .string("integer")]),
        "b": .object(["type": .string("integer")]),
    ]),
    "required": .array([.string("a"), .string("b")]),
    "additionalProperties": .bool(false),
])

let add = try Tool(name: "add", description: "Add two integers", parameters: schema) { args, _ in
    let a = Int(args["a"]?.doubleValue ?? 0)
    let b = Int(args["b"]?.doubleValue ?? 0)
    return .number(Double(a + b))
}

let result = try await generate(
    model: "claude-haiku-4-5",
    prompt: "Use the add tool to compute 2+2. Then reply with just the number.",
    provider: "anthropic",
    tools: [add],
    toolChoice: ToolChoice(mode: .named, toolName: "add"),
    maxToolRounds: 3,
    client: client
)

print(result.text)
```

## Tests

- Unit tests: `swift test`
- Static parity gates (`#1` source pinning + `#5` transport/idle-timeout checks): `./scripts/run_parity_gates.sh`
- Live integration smoke tests (reads `.env` if present):
  - `RUN_OMNIAI_INTEGRATION_TESTS=1 swift test --filter IntegrationSmokeTests`
  - Select providers: `OMNIAI_INTEGRATION_PROVIDERS=openai,anthropic,gemini,cerebras,groq`
  - Override models: `OPENAI_INTEGRATION_MODEL`, `ANTHROPIC_INTEGRATION_MODEL`, `GEMINI_INTEGRATION_MODEL`, `CEREBRAS_INTEGRATION_MODEL`, `GROQ_INTEGRATION_MODEL`
  - Use catalog "latest": `OMNIAI_INTEGRATION_USE_LATEST=1`
- Live provider matrix parity (`#3`): `RUN_OMNIAI_INTEGRATION_TESTS=1 OPENAI_API_KEY=... ANTHROPIC_API_KEY=... GEMINI_API_KEY=... swift test --filter testCrossProviderParityMatrixOpenAIAnthropicGemini`
- Live multi-turn cache parity (`#3`): `RUN_OMNIAI_INTEGRATION_TESTS=1 RUN_OMNIAI_CACHE_INTEGRATION_TESTS=1 OPENAI_API_KEY=... ANTHROPIC_API_KEY=... GEMINI_API_KEY=... swift test --filter testMultiTurnCacheReadAcrossOpenAIAnthropicGemini`
- Claude-specific live parity matrix (`#4`): `RUN_ANTHROPIC_LIVE_PARITY_TESTS=1 ANTHROPIC_API_KEY=... swift test --filter testAnthropicClaudeLiveParityMatrix`
  - Default models: `claude-sonnet-4-6`, `claude-sonnet-4-6 [1m]`, `claude-opus-4-6`, `claude-opus-4-6 [1m]`, `claude-haiku-4-5`
  - Override list: `ANTHROPIC_PARITY_MODELS="modelA,modelB"`
- Full parity runner (`#1/#3/#4/#5`): `RUN_OMNIAI_LIVE_PARITY_GATES=1 OPENAI_API_KEY=... ANTHROPIC_API_KEY=... GEMINI_API_KEY=... ./scripts/run_parity_gates.sh`
- Unified OmniAI E2E across all three providers:
  - `RUN_OMNIAI_E2E_TESTS=1 OPENAI_API_KEY=... ANTHROPIC_API_KEY=... GEMINI_API_KEY=... swift test --filter testUnifiedE2EAllProviders`
