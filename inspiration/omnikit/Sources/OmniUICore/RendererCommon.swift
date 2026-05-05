// Shared renderer utilities used by notcurses rendering paths.
// Extracted to avoid duplicating color resolution, border detection, and other logic.

import Foundation

// MARK: - TextStyle

/// Text style flags. Bit layout matches notcurses NCSTYLE_* so the notcurses renderer
/// can pass `.rawValue` directly to `omni_ncplane_set_styles`.
public struct TextStyle: OptionSet, Hashable, Sendable {
    public var rawValue: UInt16
    public init(rawValue: UInt16) { self.rawValue = rawValue }

    public static let none      = TextStyle([])
    public static let struck    = TextStyle(rawValue: 0x0001) // NCSTYLE_STRUCK
    public static let bold      = TextStyle(rawValue: 0x0002) // NCSTYLE_BOLD
    public static let undercurl = TextStyle(rawValue: 0x0004) // NCSTYLE_UNDERCURL
    public static let underline = TextStyle(rawValue: 0x0008) // NCSTYLE_UNDERLINE
    public static let italic    = TextStyle(rawValue: 0x0010) // NCSTYLE_ITALIC
}

// MARK: - Shared RGB

/// 24-bit RGB value used by renderer implementations.
public struct _RGB: Equatable, Hashable, Sendable {
    public var r: UInt8
    public var g: UInt8
    public var b: UInt8
    public init(r: UInt8, g: UInt8, b: UInt8) { self.r = r; self.g = g; self.b = b }
}

// MARK: - Color resolution

/// Resolve a `Color` to a concrete 24-bit RGB triple.
/// Handles all SwiftUI named colors plus the `rgb(r,g,b)` format.
public func _resolveColorToRGB(_ c: Color?) -> _RGB? {
    guard let c, c.alpha > 0 else { return nil }
    switch c.name {
    case "primary":      return _RGB(r: 0xD8, g: 0xDB, b: 0xE2)
    case "secondary":    return _RGB(r: 0xA5, g: 0xAC, b: 0xB8)
    case "tertiary":     return _RGB(r: 0x7D, g: 0x86, b: 0x96)
    case "white":        return _RGB(r: 0xFF, g: 0xFF, b: 0xFF)
    case "black":        return _RGB(r: 0x00, g: 0x00, b: 0x00)
    case "gray":         return _RGB(r: 0x99, g: 0xA1, b: 0xAE)
    case "red":          return _RGB(r: 0xFF, g: 0x3B, b: 0x30)
    case "orange":       return _RGB(r: 0xFF, g: 0x9F, b: 0x0A)
    case "yellow":       return _RGB(r: 0xFA, g: 0xD3, b: 0x5D)
    case "green":        return _RGB(r: 0x34, g: 0xC7, b: 0x59)
    case "mint":         return _RGB(r: 0x00, g: 0xC7, b: 0xBE)
    case "teal":         return _RGB(r: 0x30, g: 0xB0, b: 0xC7)
    case "cyan":         return _RGB(r: 0x32, g: 0xD7, b: 0xD8)
    case "blue":         return _RGB(r: 0x00, g: 0x7A, b: 0xFF)
    case "indigo":       return _RGB(r: 0x5E, g: 0x5C, b: 0xE6)
    case "purple":       return _RGB(r: 0xBF, g: 0x5A, b: 0xF2)
    case "pink":         return _RGB(r: 0xFF, g: 0x2D, b: 0x55)
    case "brown":        return _RGB(r: 0xA2, g: 0x84, b: 0x5E)
    case "accentColor":  return _RGB(r: 0x34, g: 0xD3, b: 0x99)
    case "clear":        return nil
    default:
        // Parse "rgb(r,g,b)" where r/g/b are 0.0–1.0 doubles.
        if c.name.hasPrefix("rgb("), c.name.hasSuffix(")") {
            let inner = c.name.dropFirst(4).dropLast()
            let parts = inner.split(separator: ",")
            if parts.count == 3,
               let r = Double(parts[0]),
               let g = Double(parts[1]),
               let b = Double(parts[2]) {
                return _RGB(
                    r: UInt8(clamping: Int(min(1, max(0, r)) * 255)),
                    g: UInt8(clamping: Int(min(1, max(0, g)) * 255)),
                    b: UInt8(clamping: Int(min(1, max(0, b)) * 255))
                )
            }
        }
        return nil
    }
}

// MARK: - Border glyph detection

public func _isBorderGlyphShared(_ c: Character) -> Bool {
    switch c {
    case "┌", "┐", "└", "┘", "┬", "┴", "├", "┤", "┼", "─", "│",
         "╭", "╮", "╰", "╯", "═", "║", "╬", "╦", "╩", "╠", "╣":
        return true
    default:
        return false
    }
}

// MARK: - Box drawing conversion

public func _boxifyShared(_ c: Character, left: Character?, right: Character?, up: Character?, down: Character?) -> Character {
    switch c {
    case "|":
        return "│"
    case "-":
        return "─"
    case "+":
        let l = (left == "-" || left == "+")
        let r = (right == "-" || right == "+")
        let u = (up == "|" || up == "+")
        let d = (down == "|" || down == "+")
        if r && d && !l && !u { return "┌" }
        if l && d && !r && !u { return "┐" }
        if r && u && !l && !d { return "└" }
        if l && u && !r && !d { return "┘" }
        if l && r && d && !u { return "┬" }
        if l && r && u && !d { return "┴" }
        if u && d && r && !l { return "├" }
        if u && d && l && !r { return "┤" }
        if (l || r) && (u || d) { return "┼" }
        if l || r { return "─" }
        if u || d { return "│" }
        return "┼"
    default:
        return c
    }
}

// MARK: - Renderer theme

/// Shared theme constants used by renderer implementations.
public struct RendererTheme: Sendable {
    public var baseFG: _RGB
    public var baseBG: _RGB
    public var focusFG: _RGB
    public var focusBG: _RGB
    public var accentFG: _RGB
    public var borderFG: _RGB
    public var shapeFillBG: _RGB

    public static let `default` = RendererTheme(
        baseFG:      _RGB(r: 0xD8, g: 0xDB, b: 0xE2),
        baseBG:      _RGB(r: 0x0B, g: 0x10, b: 0x20),
        focusFG:     _RGB(r: 0xFF, g: 0xFF, b: 0xFF),
        focusBG:     _RGB(r: 0x1D, g: 0x4E, b: 0xD8),
        accentFG:    _RGB(r: 0x34, g: 0xD3, b: 0x99),
        borderFG:    _RGB(r: 0xF2, g: 0xF4, b: 0xF8),
        shapeFillBG: _RGB(r: 0x12, g: 0x1B, b: 0x33)
    )
}

// MARK: - Cell type

/// A single cell in the renderer's cell buffer, with text style support.
public struct _RendererCell: Equatable, Sendable {
    public var ch: String
    public var fg: _RGB
    public var bg: _RGB
    public var styles: UInt16

    public init(ch: String, fg: _RGB, bg: _RGB, styles: UInt16 = 0) {
        self.ch = ch
        self.fg = fg
        self.bg = bg
        self.styles = styles
    }
}

// MARK: - Cell buffer rasterization

/// Rasterize a list of `RenderOp`s into a cell buffer and z-buffer.
/// This is shared logic for converting typed ops into paintable cells.
public func _rasterizeOps(
    ops: [RenderOp],
    size: _Size,
    theme: RendererTheme,
    focusedRect: _Rect?
) -> [_RendererCell] {
    let width = size.width
    let height = size.height
    guard width > 0, height > 0 else { return [] }

    var curr = Array(repeating: _RendererCell(ch: " ", fg: theme.baseFG, bg: theme.baseBG), count: width * height)
    var zbuf = Array(repeating: Int.min, count: width * height)

    func setCell(_ x: Int, _ y: Int, _ egc: String, _ fg: _RGB?, _ bg: _RGB?, z: Int, style: UInt16 = 0) {
        guard x >= 0, y >= 0, x < width, y < height else { return }
        let idx = y * width + x
        if z < zbuf[idx] { return }
        var c = curr[idx]
        c.ch = egc
        if let fg { c.fg = fg }
        if let bg { c.bg = bg }
        if style != 0 { c.styles = style }
        curr[idx] = c
        zbuf[idx] = z
    }

    func intersect(_ a: _Rect, _ b: _Rect) -> _Rect? {
        let x0 = max(a.origin.x, b.origin.x)
        let y0 = max(a.origin.y, b.origin.y)
        let x1 = min(a.origin.x + a.size.width, b.origin.x + b.size.width)
        let y1 = min(a.origin.y + a.size.height, b.origin.y + b.size.height)
        if x1 <= x0 || y1 <= y0 { return nil }
        return _Rect(origin: _Point(x: x0, y: y0), size: _Size(width: x1 - x0, height: y1 - y0))
    }

    let fullClip = _Rect(origin: _Point(x: 0, y: 0), size: size)
    var clipStack: [_Rect] = [fullClip]
    func inClip(_ x: Int, _ y: Int) -> Bool {
        guard let c = clipStack.last else { return true }
        return c.contains(_Point(x: x, y: y))
    }

    for op in ops {
        let style = op.textStyle.rawValue
        switch op.kind {
        case .glyph(let x, let y, let egc, let fg, let bg):
            if !inClip(x, y) { break }
            let mapped = egc.first ?? " "
            var outFG = _resolveColorToRGB(fg) ?? theme.baseFG
            let outBG = _resolveColorToRGB(bg) ?? theme.baseBG
            if mapped == "*" { outFG = theme.accentFG }
            if _isBorderGlyphShared(mapped) { outFG = theme.borderFG }
            setCell(x, y, egc, outFG, outBG, z: op.zIndex, style: style)
        case .textRun(let x, let y, let text, let fg, let bg):
            let outFG = _resolveColorToRGB(fg) ?? theme.baseFG
            let outBG = _resolveColorToRGB(bg) ?? theme.baseBG
            var xx = x
            for ch in text {
                if inClip(xx, y) {
                    var fg2 = outFG
                    if ch == "*" { fg2 = theme.accentFG }
                    if _isBorderGlyphShared(ch) { fg2 = theme.borderFG }
                    setCell(xx, y, String(ch), fg2, outBG, z: op.zIndex, style: style)
                }
                xx += 1
                if xx >= width { break }
            }
        case .fillRect(let rect, let color):
            guard let c = _resolveColorToRGB(color) else { continue }
            guard let top = clipStack.last, let rr = intersect(rect, top) else { continue }
            let x0 = max(0, rr.origin.x)
            let y0 = max(0, rr.origin.y)
            let x1 = min(width, rr.origin.x + rr.size.width)
            let y1 = min(height, rr.origin.y + rr.size.height)
            if x1 <= x0 || y1 <= y0 { continue }
            for yy in y0..<y1 {
                for xx in x0..<x1 {
                    setCell(xx, yy, " ", nil, c, z: op.zIndex)
                }
            }
        case .pushClip(let r):
            if let top = clipStack.last, let i = intersect(top, r) {
                clipStack.append(i)
            } else {
                clipStack.append(_Rect(origin: _Point(x: 0, y: 0), size: _Size(width: 0, height: 0)))
            }
        case .popClip:
            if clipStack.count > 1 { _ = clipStack.popLast() }
        case .shape:
            break // Shapes are handled separately by each renderer (sprixel vs braille).
        }
    }

    // Focus highlight.
    if let fr = focusedRect {
        let y0 = max(0, fr.origin.y)
        let x0 = max(0, fr.origin.x)
        let y1 = min(height, fr.origin.y + fr.size.height)
        let x1 = min(width, fr.origin.x + fr.size.width)
        if y1 > y0, x1 > x0 {
            for y in y0..<y1 {
                for x in x0..<x1 {
                    let idx = y * width + x
                    curr[idx].fg = theme.focusFG
                    curr[idx].bg = theme.focusBG
                }
            }
        }
    }

    return curr
}
