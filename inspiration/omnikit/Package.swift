// swift-tools-version: 6.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription
import CompilerPluginSupport

let commonSwiftSettings: [SwiftSetting] = [
    .unsafeFlags(["-warn-concurrency", "-strict-concurrency=complete"]),
    .unsafeFlags(["-enable-actor-data-race-checks"], .when(configuration: .debug)),
]

let swift6CommonSwiftSettings: [SwiftSetting] = [
    .swiftLanguageMode(.v6),
] + commonSwiftSettings

let package = Package(
    name: "OmniKit",
    platforms: [
        .macOS(.v13),
        .iOS(.v16),
        .tvOS(.v16),
        .watchOS(.v9),
        .visionOS(.v1),
    ],
    products: [
        // Products define the executables and libraries a package produces, making them visible to other packages.
        .library(
            name: "SwiftUI",
            targets: ["OmniSwiftUI"]
        ),
        .library(
            name: "SwiftData",
            targets: ["OmniSwiftData"]
        ),
        .library(
            name: "OmniKit",
            targets: ["OmniKit"]
        ),
        .library(
            name: "OmniHTTP",
            targets: ["OmniHTTP"]
        ),
        .library(
            name: "OmniHTTPNIO",
            targets: ["OmniHTTPNIO"]
        ),
        .library(
            name: "OmniAICore",
            targets: ["OmniAICore"]
        ),
        .library(
            name: "OmniAIAgent",
            targets: ["OmniAIAgent"]
        ),
        .library(
            name: "OmniAIAttractor",
            targets: ["OmniAIAttractor"]
        ),
        .library(
            name: "OmniACPModel",
            targets: ["OmniACPModel"]
        ),
        .library(
            name: "OmniACP",
            targets: ["OmniACP"]
        ),
        .library(
            name: "OmniMCP",
            targets: ["OmniMCP"]
        ),
        .library(
            name: "OmniAgentsSDK",
            targets: ["OmniAgentsSDK"]
        ),
        .library(
            name: "OmniUICore",
            targets: ["OmniUICore"]
        ),
        .library(
            name: "OmniSwiftUISymbolExtras",
            targets: ["OmniSwiftUISymbolExtras"]
        ),
        .library(
            name: "OmniUI",
            targets: ["OmniUI"]
        ),
        .library(
            name: "OmniUINotcursesRenderer",
            targets: ["OmniUINotcursesRenderer"]
        ),
        .executable(
            name: "KitchenSink",
            targets: ["KitchenSink"]
        ),
        .executable(
            name: "AttractorCLI",
            targets: ["AttractorCLI"]
        ),
        .executable(
            name: "OmniAICode",
            targets: ["OmniAICode"]
        ),
    ],
    dependencies: [
        // Cross-platform networking + streaming.
        .package(url: "https://github.com/swift-server/async-http-client.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-nio.git", from: "2.0.0"),
        .package(url: "https://github.com/apple/swift-nio-transport-services.git", from: "1.0.0"),
        .package(url: "https://github.com/vapor/websocket-kit.git", from: "2.15.0"),
        // For Swift macro stubs (SwiftData compatibility).
        .package(url: "https://github.com/swiftlang/swift-syntax.git", from: "602.0.0"),
        // Swift-native testing DSL (`import Testing`, `@Test`, `#expect`).
        .package(url: "https://github.com/swiftlang/swift-testing.git", from: "6.2.0"),
    ],
    targets: [
        // Targets are the basic building blocks of a package, defining a module or a test suite.
        // Targets can depend on other targets in this package and products from dependencies.
        .target(
            name: "OmniSwiftUI",
            dependencies: ["OmniUI", "SwiftUIMacros"],
            path: "Sources/SwiftUI",
            swiftSettings: commonSwiftSettings + [
                // Some larger SwiftUI view trees can trip the default solver timeout.
                .unsafeFlags(["-Xfrontend", "-solver-expression-time-threshold=300"]),
            ]
        ),
        .macro(
            name: "SwiftUIMacros",
            dependencies: [
                .product(name: "SwiftCompilerPlugin", package: "swift-syntax"),
                .product(name: "SwiftSyntaxMacros", package: "swift-syntax"),
            ]
        ),
        .macro(
            name: "SwiftDataMacros",
            dependencies: [
                .product(name: "SwiftCompilerPlugin", package: "swift-syntax"),
                .product(name: "SwiftSyntaxMacros", package: "swift-syntax"),
            ]
        ),
        .target(
            name: "OmniSwiftData",
            dependencies: ["OmniUICore", "SwiftDataMacros"],
            path: "Sources/SwiftData",
            swiftSettings: commonSwiftSettings
        ),
        .executableTarget(
            name: "SwiftUICompatibilityHarness",
            dependencies: ["OmniSwiftUI", "OmniSwiftData", "OmniSwiftUISymbolExtras"],
            swiftSettings: commonSwiftSettings + [
                // Keep source compatibility with `import SwiftUI` / `import SwiftData`.
                .unsafeFlags(["-module-alias", "SwiftUI=OmniSwiftUI"]),
                .unsafeFlags(["-module-alias", "SwiftData=OmniSwiftData"]),
            ]
        ),
        .target(
            name: "OmniKit",
            swiftSettings: commonSwiftSettings
        ),
        .target(
            name: "OmniHTTP",
            swiftSettings: commonSwiftSettings
        ),
        .target(
            name: "OmniHTTPNIO",
            dependencies: [
                "OmniHTTP",
                .product(name: "AsyncHTTPClient", package: "async-http-client"),
                .product(name: "NIOCore", package: "swift-nio"),
                .product(name: "NIOHTTP1", package: "swift-nio"),
                .product(
                    name: "NIOTransportServices",
                    package: "swift-nio-transport-services",
                    condition: .when(platforms: [.macOS, .iOS, .tvOS, .watchOS, .visionOS])
                ),
            ],
            swiftSettings: commonSwiftSettings
        ),
        .target(
            name: "OmniAICore",
            dependencies: [
                "OmniHTTP",
                .product(name: "NIOPosix", package: "swift-nio"),
                .product(name: "WebSocketKit", package: "websocket-kit"),
            ],
            swiftSettings: commonSwiftSettings
        ),
        .target(
            name: "OmniAIAgent",
            dependencies: ["OmniAICore", "OmniMCP"],
            path: "Sources/OmniAIAgent",
            resources: [
                .process("Resources"),
            ],
            swiftSettings: commonSwiftSettings
        ),
        .target(
            name: "OmniACPModel",
            path: "Sources/OmniACPModel",
            swiftSettings: swift6CommonSwiftSettings
        ),
        .target(
            name: "OmniACP",
            dependencies: ["OmniACPModel", "OmniHTTP", "OmniHTTPNIO", "OmniAICore"],
            path: "Sources/OmniACP",
            swiftSettings: swift6CommonSwiftSettings
        ),
        .target(
            name: "OmniMCP",
            dependencies: ["OmniAICore", "OmniHTTP"],
            path: "Sources/OmniMCP",
            swiftSettings: swift6CommonSwiftSettings
        ),
        .target(
            name: "OmniAIAttractor",
            dependencies: ["OmniAICore", "OmniAIAgent", "OmniACP"],
            path: "Sources/OmniAIAttractor",
            swiftSettings: commonSwiftSettings
        ),
        .target(
            name: "OmniAgentsSDK",
            dependencies: ["OmniAICore", "OmniMCP"],
            path: "Sources/OmniAgentsSDK",
            swiftSettings: swift6CommonSwiftSettings
        ),
        .target(
            name: "OmniUICore",
            swiftSettings: commonSwiftSettings
        ),
        .target(
            name: "OmniUI",
            dependencies: ["OmniUICore", "OmniUINotcursesRenderer"],
            swiftSettings: commonSwiftSettings
        ),
        .target(
            name: "OmniSwiftUISymbolExtras",
            dependencies: [],
            path: "Sources/OmniSwiftUISymbolExtras",
            swiftSettings: commonSwiftSettings
        ),
        .target(
            name: "CNotcurses",
            path: "Sources/CNotcurses",
            publicHeadersPath: "include",
            cSettings: [
                // SwiftPM only accepts relative `headerSearchPath`, so use explicit `-I` flags.
                // Homebrew (Apple Silicon default prefix).
                .unsafeFlags(["-I/opt/homebrew/opt/notcurses/include"], .when(platforms: [.macOS])),
                // Homebrew (Intel default prefix).
                .unsafeFlags(["-I/usr/local/include"], .when(platforms: [.macOS])),
                // Common Linux include prefix for distro packages.
                .unsafeFlags(["-I/usr/include"], .when(platforms: [.linux])),
            ]
        ),
        .target(
            name: "OmniUINotcursesRenderer",
            dependencies: [
                "OmniUICore",
                .target(name: "CNotcurses", condition: .when(platforms: [.linux, .macOS])),
            ],
            swiftSettings: commonSwiftSettings + [
                // Make notcurses headers visible to the Swift compiler when importing the Clang module.
                .unsafeFlags(["-Xcc", "-I/opt/homebrew/opt/notcurses/include"], .when(platforms: [.macOS])),
                .unsafeFlags(["-Xcc", "-I/usr/local/include"], .when(platforms: [.macOS])),
                .unsafeFlags(["-Xcc", "-I/usr/include"], .when(platforms: [.linux])),
            ],
            linkerSettings: [
                .linkedLibrary("notcurses", .when(platforms: [.linux, .macOS])),
                .linkedLibrary("notcurses-core", .when(platforms: [.linux, .macOS])),
                .unsafeFlags(["-L/opt/homebrew/opt/notcurses/lib"], .when(platforms: [.macOS])),
                .unsafeFlags(["-L/usr/local/lib"], .when(platforms: [.macOS])),
            ]
        ),
        .executableTarget(
            name: "KitchenSink",
            dependencies: ["OmniSwiftUI", "OmniUI", "OmniUINotcursesRenderer"],
            swiftSettings: [
                .unsafeFlags(["-warn-concurrency", "-strict-concurrency=complete"]),
                .unsafeFlags(["-enable-actor-data-race-checks"], .when(configuration: .debug)),
                .unsafeFlags(["-Xfrontend", "-solver-expression-time-threshold=300"]),
            ]
        ),
        .executableTarget(
            name: "AttractorCLI",
            dependencies: ["OmniAIAttractor"],
            path: "Sources/AttractorCLI",
            swiftSettings: commonSwiftSettings
        ),
        .executableTarget(
            name: "OmniAICode",
            dependencies: ["OmniAIAgent", "OmniAICore", "OmniMCP"],
            path: "Sources/OmniAICode",
            swiftSettings: commonSwiftSettings
        ),
        .testTarget(
            name: "OmniKitTests",
            dependencies: [
                "OmniKit",
                .product(name: "Testing", package: "swift-testing"),
            ]
        ),
        .testTarget(
            name: "OmniHTTPTests",
            dependencies: [
                "OmniHTTP",
                .product(name: "Testing", package: "swift-testing"),
            ]
        ),
        .testTarget(
            name: "OmniAICoreTests",
            dependencies: [
                "OmniAICore",
                .product(name: "Testing", package: "swift-testing"),
            ]
        ),
        .testTarget(
            name: "OmniMCPTests",
            dependencies: [
                "OmniMCP",
                "OmniHTTP",
                "OmniAICore",
                .product(name: "Testing", package: "swift-testing"),
            ],
            swiftSettings: commonSwiftSettings
        ),
        .testTarget(
            name: "OmniAgentsSDKTests",
            dependencies: [
                "OmniAgentsSDK",
                .product(name: "Testing", package: "swift-testing"),
            ],
            swiftSettings: commonSwiftSettings
        ),
        .testTarget(
            name: "OmniAIAgentTests",
            dependencies: [
                "OmniAIAgent",
                "OmniAICore",
                .product(name: "Testing", package: "swift-testing"),
            ],
            swiftSettings: commonSwiftSettings
        ),
        .testTarget(
            name: "OmniACPModelTests",
            dependencies: [
                "OmniACPModel",
                .product(name: "Testing", package: "swift-testing"),
            ],
            swiftSettings: swift6CommonSwiftSettings
        ),
        .testTarget(
            name: "OmniACPTests",
            dependencies: [
                "OmniACP",
                "OmniACPModel",
                "OmniHTTP",
                "OmniAICore",
                .product(name: "Testing", package: "swift-testing"),
            ],
            resources: [
                .process("GoldenTests"),
                .copy("Fixtures"),
            ],
            swiftSettings: swift6CommonSwiftSettings
        ),
        .testTarget(
            name: "OmniAIAttractorTests",
            dependencies: [
                "OmniAIAttractor",
                "OmniAICore",
                "OmniAIAgent",
                "OmniACP",
                "OmniACPModel",
                .product(name: "Testing", package: "swift-testing"),
            ],
            swiftSettings: commonSwiftSettings
        ),
        .testTarget(
            name: "OmniUICoreTests",
            dependencies: [
                "OmniUICore",
                .product(name: "Testing", package: "swift-testing"),
            ]
        ),
    ],
    swiftLanguageModes: [.v6]
)
