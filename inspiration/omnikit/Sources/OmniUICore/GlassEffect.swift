// Terminal-friendly approximations for iOS/macOS 26 "Liquid Glass" APIs.

import Foundation

public struct GlassEffect: Hashable, Sendable {
    public let rawValue: String
    public init(_ rawValue: String) { self.rawValue = rawValue }

    public static let regular = GlassEffect("regular")

    public func interactive() -> GlassEffect {
        GlassEffect(rawValue + ".interactive")
    }
}

public struct GlassEffectShape: Hashable, Sendable {
    public let rawValue: String
    public init(_ rawValue: String) { self.rawValue = rawValue }

    public static func rect(cornerRadius: CGFloat) -> GlassEffectShape {
        GlassEffectShape("rect(cornerRadius:\(cornerRadius))")
    }

    public static let capsule = GlassEffectShape("capsule")
}

public struct GlassEffectContainer<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    let spacing: CGFloat
    let content: Content

    public init(spacing: CGFloat = 0, @ViewBuilder content: () -> Content) {
        self.spacing = spacing
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _ = spacing
        return ctx.buildChild(content)
    }
}
