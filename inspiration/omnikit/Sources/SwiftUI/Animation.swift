public struct AnimationTimelineSchedule: Hashable, Sendable { public init() {} }

public struct Animation: Hashable, Sendable {
    public let rawValue: String
    public init(_ rawValue: String) { self.rawValue = rawValue }

    public static let `default` = Animation("default")

    public static func spring() -> Animation { Animation("spring") }
    public static func easeInOut(duration: Double = 0.35) -> Animation { Animation("easeInOut(\(duration))") }
    public static func easeIn(duration: Double = 0.35) -> Animation { Animation("easeIn(\(duration))") }
    public static func easeOut(duration: Double = 0.35) -> Animation { Animation("easeOut(\(duration))") }
    public static func linear(duration: Double = 0.35) -> Animation { Animation("linear(\(duration))") }

    public func delay(_ delay: Double) -> Animation {
        Animation("\(rawValue).delay(\(delay))")
    }

    public func repeatForever(autoreverses: Bool = true) -> Animation {
        Animation("\(rawValue).repeatForever(\(autoreverses))")
    }
}

@discardableResult
public func withAnimation<T>(_ animation: Animation? = nil, _ body: () -> T) -> T {
    _ = animation
    return body()
}

public extension View {
    func animation<V: Equatable>(_ animation: Animation?, value: V) -> some View {
        _ = animation
        _ = value
        return _Passthrough(self)
    }

    func animation(_ animation: Animation?) -> some View {
        _ = animation
        return _Passthrough(self)
    }

    func phaseAnimator<Phases: Collection, Content: View>(
        _ phases: Phases,
        @ViewBuilder content: (Self, Phases.Element) -> Content
    ) -> some View {
        guard let phase = phases.first else { return AnyView(self) }
        return AnyView(content(self, phase))
    }

    func phaseAnimator<Phases: Collection, Content: View>(
        _ phases: Phases,
        @ViewBuilder content: (Self, Phases.Element) -> Content,
        animation: (Phases.Element) -> Animation
    ) -> some View {
        guard let phase = phases.first else { return AnyView(self) }
        _ = animation(phase)
        return AnyView(content(self, phase))
    }
}
