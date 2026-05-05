import Foundation

public struct Color: Hashable, Sendable {
    public enum RGBColorSpace: Hashable, Sendable {
        case sRGB
        case sRGBLinear
        case displayP3
    }

    public let name: String
    public let alpha: CGFloat

    public init(_ name: String, alpha: CGFloat = 1.0) {
        self.name = name
        self.alpha = alpha
    }

    // Compatibility initializers used by many SwiftUI call sites.
    public init(_ any: Any) {
        self.name = String(describing: any)
        self.alpha = 1.0
    }

    public init(red: CGFloat, green: CGFloat, blue: CGFloat, opacity: CGFloat = 1.0) {
        self.name = "rgb(\(red),\(green),\(blue))"
        self.alpha = opacity
    }

    public init(_ colorSpace: RGBColorSpace, red: CGFloat, green: CGFloat, blue: CGFloat, opacity: CGFloat = 1.0) {
        self.name = "\(colorSpace):rgb(\(red),\(green),\(blue))"
        self.alpha = opacity
    }

    public func opacity(_ alpha: CGFloat) -> Color {
        Color(name, alpha: alpha)
    }

    public static let clear = Color("clear", alpha: 0)
    public static let primary = Color("primary")
    public static let secondary = Color("secondary")
    public static let tertiary = Color("tertiary")

    public static let accentColor = Color("accentColor")
    public static let black = Color("black")
    public static let white = Color("white")
    public static let gray = Color("gray")
    public static let red = Color("red")
    public static let orange = Color("orange")
    public static let yellow = Color("yellow")
    public static let green = Color("green")
    public static let mint = Color("mint")
    public static let teal = Color("teal")
    public static let cyan = Color("cyan")
    public static let blue = Color("blue")
    public static let indigo = Color("indigo")
    public static let purple = Color("purple")
    public static let pink = Color("pink")
    public static let brown = Color("brown")
}

extension Color: RawRepresentable {
    public typealias RawValue = String

    public init?(rawValue: String) {
        // Format: `<name>|<alpha>`
        let parts = rawValue.split(separator: "|", maxSplits: 1, omittingEmptySubsequences: false).map(String.init)
        if parts.count == 2, let a = Double(parts[1]) {
            self.init(parts[0], alpha: CGFloat(a))
            return
        }
        // Back-compat: treat the entire raw value as the name (alpha=1).
        self.init(rawValue, alpha: 1.0)
    }

    public var rawValue: String {
        "\(name)|\(alpha)"
    }
}

extension Color: View, _PrimitiveView {
    public typealias Body = Never

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        // A Color acts like a background fill in SwiftUI. We model this as a background-style node
        // that fills the available layout rect in `_DebugLayout`.
        .style(fg: nil, bg: self, child: .empty)
    }
}

public struct Font: Hashable, Sendable {
    public enum Design: Hashable, Sendable {
        case `default`
        case monospaced
    }

    public enum Weight: Hashable, Sendable {
        case ultraLight
        case thin
        case light
        case regular
        case medium
        case semibold
        case bold
        case heavy
        case black
    }

    public let name: String

    public static let largeTitle = Font(name: "largeTitle")
    public static let title = Font(name: "title")
    public static let title2 = Font(name: "title2")
    public static let title3 = Font(name: "title3")
    public static let subheadline = Font(name: "subheadline")
    public static let headline = Font(name: "headline")
    public static let body = Font(name: "body")
    public static let caption = Font(name: "caption")
    public static let caption2 = Font(name: "caption2")

    public static func system(size: CGFloat, design: Design = .default) -> Font {
        Font(name: "system(\(size),\(design))")
    }

    public static func system(size: CGFloat, weight: Weight, design: Design = .default) -> Font {
        Font(name: "system(\(size),\(weight),\(design))")
    }

    // SwiftUI exposes `.system(_:, design:)` overloads; this keeps call sites compiling.
    public static func system(_ style: Font, design: Design = .default) -> Font {
        Font(name: "system(\(style.name),\(design))")
    }

    public func weight(_ weight: Weight) -> Font {
        Font(name: "\(name).weight(\(weight))")
    }

    public func weight(_ any: Any = ()) -> Font {
        Font(name: "\(name).weight")
    }

    public func bold() -> Font {
        Font(name: "\(name).bold")
    }
}

public enum TextAlignment: Sendable {
    case leading
    case center
    case trailing
}

public enum HorizontalAlignment: Sendable {
    case leading
    case center
    case trailing
}

public enum VerticalAlignment: Sendable {
    case top
    case center
    case bottom
}

public struct Alignment: Hashable, Sendable {
    public let raw: String
    public init(_ raw: String) { self.raw = raw }

    public static let center = Alignment("center")
    public static let leading = Alignment("leading")
    public static let trailing = Alignment("trailing")
    public static let top = Alignment("top")
    public static let bottom = Alignment("bottom")
}

public enum Edge: Hashable, Sendable {
    case top
    case leading
    case bottom
    case trailing

    public struct Set: OptionSet, Hashable, Sendable {
        public let rawValue: Int
        public init(rawValue: Int) { self.rawValue = rawValue }

        public static let top = Set(rawValue: 1 << 0)
        public static let leading = Set(rawValue: 1 << 1)
        public static let bottom = Set(rawValue: 1 << 2)
        public static let trailing = Set(rawValue: 1 << 3)

        public static let horizontal: Set = [.leading, .trailing]
        public static let vertical: Set = [.top, .bottom]
        public static let all: Set = [.top, .leading, .bottom, .trailing]
    }
}

public enum ControlSize: Hashable, Sendable {
    case mini
    case small
    case regular
    case large
}

public struct PresentationDetent: Hashable, Sendable {
    public let rawValue: String
    public init(_ rawValue: String) { self.rawValue = rawValue }

    public static let medium = PresentationDetent("medium")
    public static let large = PresentationDetent("large")
}

public enum Axis: Sendable {
    public struct Set: OptionSet, Hashable, Sendable {
        public let rawValue: Int
        public init(rawValue: Int) { self.rawValue = rawValue }

        public static let vertical = Set(rawValue: 1 << 0)
        public static let horizontal = Set(rawValue: 1 << 1)
    }
}

// MARK: Text Input (stubs)

public enum UIKeyboardType: Hashable, Sendable {
    case `default`
    case URL
}

public enum TextInputAutocapitalization: Hashable, Sendable {
    case never
    case sentences
    case words
    case characters
}

public struct UITextContentType: Hashable, Sendable, ExpressibleByStringLiteral {
    public var rawValue: String
    public init(_ rawValue: String) { self.rawValue = rawValue }
    public init(stringLiteral value: StringLiteralType) { self.rawValue = value }

    public static let URL = UITextContentType("URL")
}

// SwiftUI materials (placeholder).
public struct Material: Hashable, Sendable {
    public let raw: String
    public init(_ raw: String) { self.raw = raw }
    public static let bar = Material("bar")
    public static let background = Material("background")
    public static let regularMaterial = Material("regularMaterial")
    public static let ultraThinMaterial = Material("ultraThinMaterial")
}

public enum Visibility: Hashable, Sendable {
    case automatic
    case visible
    case hidden
}

public enum TextSelection: Hashable, Sendable {
    case enabled
    case disabled
}

public struct NavigationTransition: Hashable, Sendable {
    public init() {}
}

public struct ContentTransition: Hashable, Sendable {
    public init() {}
}

public struct AnyTransition: Hashable, Sendable {
    public let rawValue: String
    public init(_ rawValue: String) { self.rawValue = rawValue }

    public static var opacity: AnyTransition { AnyTransition("opacity") }
    public static func move(edge: Edge) -> AnyTransition { AnyTransition("move(\(edge))") }

    public func combined(with other: AnyTransition) -> AnyTransition {
        AnyTransition("\(rawValue)+\(other.rawValue)")
    }
}
