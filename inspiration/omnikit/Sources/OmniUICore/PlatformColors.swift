import Foundation

// SwiftUI on Apple platforms provides bridging initializers between `Color` and UIKit/AppKit colors.
// We provide lightweight stubs when UIKit/AppKit are unavailable so cross-platform sources compile.

// MARK: - UIKit

#if canImport(UIKit)
import UIKit

public extension Color {
    init(_ uiColor: UIKit.UIColor) {
        // Compile-first shim: keep a stable-ish name for debugging.
        self.init(String(describing: uiColor))
    }

    init(uiColor: UIKit.UIColor) {
        self.init(uiColor)
    }
}

public extension UIKit.UIColor {
    convenience init(_ color: OmniUICore.Color) {
        // Best-effort mapping for common named colors.
        let (r, g, b) = _OmniPlatformColor._rgb(for: color.name)
        self.init(red: r, green: g, blue: b, alpha: color.alpha)
    }
}
#else

/// Minimal stand-in for UIKit's `UIColor` to keep iOS-only call sites compiling on non-UIKit platforms.
///
/// This is intentionally tiny, but conforms to `NSSecureCoding` so color bridging code can archive/unarchive
/// it via `NSKeyedArchiver`.
public final class UIColor: NSObject, NSSecureCoding, @unchecked Sendable {
    public static var supportsSecureCoding: Bool { true }

    public let name: String
    public let alpha: CGFloat

    public init(_ name: String, alpha: CGFloat = 1.0) {
        self.name = name
        self.alpha = alpha
        super.init()
    }

    public convenience init(_ color: OmniUICore.Color) {
        self.init(color.name, alpha: color.alpha)
    }

    public required convenience init?(coder: NSCoder) {
        let name = coder.decodeObject(of: NSString.self, forKey: "name") as String? ?? "black"
        let alpha = coder.decodeDouble(forKey: "alpha")
        self.init(name, alpha: alpha == 0 ? 1.0 : alpha)
    }

    public func encode(with coder: NSCoder) {
        coder.encode(name as NSString, forKey: "name")
        coder.encode(Double(alpha), forKey: "alpha")
    }

    // Common system colors.
    public static let white = UIColor("white")
    public static let black = UIColor("black")
    public static let blue = UIColor("blue")
    public static let systemBlue = UIColor("systemBlue")
    public static let systemGray = UIColor("systemGray")
    public static let systemGray6 = UIColor("systemGray6")
    public static let systemBackground = UIColor("systemBackground")
    public static let secondarySystemBackground = UIColor("secondarySystemBackground")
}

public extension Color {
    #if !canImport(AppKit)
        init(_ uiColor: UIColor) {
            self.init(uiColor.name, alpha: uiColor.alpha)
        }
    #endif

    init(uiColor: UIColor) {
        self.init(uiColor.name, alpha: uiColor.alpha)
    }
}
#endif

// MARK: - AppKit

#if canImport(AppKit)
import AppKit

public extension Color {
    init(_ nsColor: AppKit.NSColor) {
        self.init(String(describing: nsColor))
    }

    init(nsColor: AppKit.NSColor) {
        self.init(nsColor)
    }
}

public extension AppKit.NSColor {
    convenience init(_ color: OmniUICore.Color) {
        let (r, g, b) = _OmniPlatformColor._rgb(for: color.name)
        self.init(calibratedRed: r, green: g, blue: b, alpha: color.alpha)
    }
}
#endif

// MARK: - Shared helpers

enum _OmniPlatformColor {
    static func _rgb(for name: String) -> (CGFloat, CGFloat, CGFloat) {
        switch name {
        case "black": return (0, 0, 0)
        case "white": return (1, 1, 1)
        case "gray", "systemGray": return (0.5, 0.5, 0.5)
        case "red": return (1, 0, 0)
        case "green": return (0, 1, 0)
        case "blue", "systemBlue": return (0, 0, 1)
        case "yellow": return (1, 1, 0)
        case "systemBackground": return (1, 1, 1)
        case "secondarySystemBackground", "systemGray6": return (0.94, 0.94, 0.94)
        default:
            // If it's already our rgb(...) format, try to parse it.
            if let parsed = _parseRGBFunction(name) { return parsed }
            return (0.0, 0.0, 0.0)
        }
    }

    private static func _parseRGBFunction(_ s: String) -> (CGFloat, CGFloat, CGFloat)? {
        // Supports `rgb(r,g,b)` where r/g/b are Doubles.
        guard s.hasPrefix("rgb("), s.hasSuffix(")") else { return nil }
        let inner = s.dropFirst(4).dropLast()
        let parts = inner.split(separator: ",", omittingEmptySubsequences: false).map { String($0) }
        guard parts.count == 3 else { return nil }
        guard
            let r = Double(parts[0]),
            let g = Double(parts[1]),
            let b = Double(parts[2])
        else { return nil }
        return (CGFloat(r), CGFloat(g), CGFloat(b))
    }
}
