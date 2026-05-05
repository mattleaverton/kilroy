#if os(Linux)
import Glibc
#else
import Darwin
#endif
import Foundation

// Shared, portable braille rasterization for shape render ops.
//
// This is used as a fallback when pixel/sprixel rendering isn't available, and as a
// convenient way to clip shapes (sprixel clipping is non-trivial).
public enum BrailleRaster {
    // Render shapes into a character-cell grid using braille patterns.
    //
    // - Shapes are rasterized at 2x4 subpixels per terminal cell (Unicode braille layout).
    // - The `clip` rect is in terminal cell coordinates.
    // - `isEmpty` is used to avoid clobbering already-rendered text (e.g. overlay labels).
    public static func render<C>(
        termSize: _Size,
        shapes: [(_Rect, _ShapeNode)],
        clip: _Rect? = nil,
        fillBG: C,
        strokeFG: C,
        baseBG: C,
        isEmpty: (_ x: Int, _ y: Int) -> Bool,
        set: (_ x: Int, _ y: Int, _ ch: String, _ fg: C, _ bg: C) -> Void
    ) {
        guard termSize.width > 0, termSize.height > 0 else { return }

        let full = _Rect(origin: _Point(x: 0, y: 0), size: termSize)
        let clip = clip.flatMap { _intersect(full, $0) } ?? full
        if clip.size.width <= 0 || clip.size.height <= 0 { return }

        func dotBit(_ sx: Int, _ sy: Int) -> UInt8 {
            // Braille dot numbering:
            // 1 4
            // 2 5
            // 3 6
            // 7 8
            switch (sx, sy) {
            case (0, 0): return 0x01
            case (0, 1): return 0x02
            case (0, 2): return 0x04
            case (1, 0): return 0x08
            case (1, 1): return 0x10
            case (1, 2): return 0x20
            case (0, 3): return 0x40
            case (1, 3): return 0x80
            default: return 0
            }
        }

        func insideRoundedRect(x: Double, y: Double, w: Double, h: Double, rx: Double, ry: Double) -> Bool {
            let crx = min(rx, w / 2.0)
            let cry = min(ry, h / 2.0)
            if x >= crx && x <= w - crx { return true }
            if y >= cry && y <= h - cry { return true }
            let cx = (x < crx) ? crx : (w - crx)
            let cy = (y < cry) ? cry : (h - cry)
            let dx = (x - cx) / max(1e-6, crx)
            let dy = (y - cy) / max(1e-6, cry)
            return (dx * dx + dy * dy) <= 1.0
        }

        func strokePathMask(elements: [Path.Element], subW: Int, subH: Int) -> ([Bool], [(Int, Int, Int, Int)]) {
            var subStroke = Array(repeating: false, count: subW * subH)
            func setStroke(_ sx: Int, _ sy: Int) {
                guard sx >= 0, sy >= 0, sx < subW, sy < subH else { return }
                subStroke[sy * subW + sx] = true
            }

            // Bounds in source path coordinates.
            var minX = Double.greatestFiniteMagnitude
            var minY = Double.greatestFiniteMagnitude
            var maxX = -Double.greatestFiniteMagnitude
            var maxY = -Double.greatestFiniteMagnitude

            func consider(_ p: CGPoint) {
                minX = min(minX, Double(p.x))
                minY = min(minY, Double(p.y))
                maxX = max(maxX, Double(p.x))
                maxY = max(maxY, Double(p.y))
            }

            for e in elements {
                switch e {
                case .move(to: let p), .line(to: let p):
                    consider(p)
                case .quadCurve(to: let p, control: let c):
                    consider(p); consider(c)
                case .curve(to: let p, control1: let c1, control2: let c2):
                    consider(p); consider(c1); consider(c2)
                case .rect(let r):
                    consider(r.origin)
                    consider(CGPoint(x: r.origin.x + r.size.width, y: r.origin.y + r.size.height))
                case .ellipse(let r):
                    consider(r.origin)
                    consider(CGPoint(x: r.origin.x + r.size.width, y: r.origin.y + r.size.height))
                case .closeSubpath:
                    break
                }
            }

            if !minX.isFinite || !minY.isFinite || !maxX.isFinite || !maxY.isFinite {
                return (subStroke, [])
            }
            let rangeX = max(1e-6, maxX - minX)
            let rangeY = max(1e-6, maxY - minY)

            func map(_ p: CGPoint) -> (Int, Int) {
                let nx = (Double(p.x) - minX) / rangeX
                let ny = (Double(p.y) - minY) / rangeY
                let x = Int(nx * Double(max(1, subW - 1)))
                let y = Int(ny * Double(max(1, subH - 1)))
                return (x, y)
            }

            func line(_ x0: Int, _ y0: Int, _ x1: Int, _ y1: Int) {
                var x0 = x0, y0 = y0
                let dx = abs(x1 - x0)
                let sx = x0 < x1 ? 1 : -1
                let dy = -abs(y1 - y0)
                let sy = y0 < y1 ? 1 : -1
                var err = dx + dy
                while true {
                    setStroke(x0, y0)
                    if x0 == x1 && y0 == y1 { break }
                    let e2 = 2 * err
                    if e2 >= dy { err += dy; x0 += sx }
                    if e2 <= dx { err += dx; y0 += sy }
                }
            }

            func lerp(_ a: CGPoint, _ b: CGPoint, _ t: Double) -> CGPoint {
                CGPoint(
                    x: CGFloat(Double(a.x) + (Double(b.x) - Double(a.x)) * t),
                    y: CGFloat(Double(a.y) + (Double(b.y) - Double(a.y)) * t)
                )
            }

            func cubic(_ p0: CGPoint, _ c1: CGPoint, _ c2: CGPoint, _ p3: CGPoint, _ t: Double) -> CGPoint {
                let u = 1.0 - t
                let tt = t * t
                let uu = u * u
                let uuu = uu * u
                let ttt = tt * t
                let x =
                    uuu * Double(p0.x) +
                    3.0 * uu * t * Double(c1.x) +
                    3.0 * u * tt * Double(c2.x) +
                    ttt * Double(p3.x)
                let y =
                    uuu * Double(p0.y) +
                    3.0 * uu * t * Double(c1.y) +
                    3.0 * u * tt * Double(c2.y) +
                    ttt * Double(p3.y)
                return CGPoint(x: CGFloat(x), y: CGFloat(y))
            }

            var segments: [(Int, Int, Int, Int)] = []
            segments.reserveCapacity(max(16, elements.count * 3))

            var curr: (Int, Int)? = nil
            var start: (Int, Int)? = nil
            var currSrc: CGPoint? = nil
            var startSrc: CGPoint? = nil

            for e in elements {
                switch e {
                case .move(to: let p):
                    let mp = map(p)
                    curr = mp
                    start = mp
                    currSrc = p
                    startSrc = p
                case .line(to: let p):
                    let mp = map(p)
                    if let c = curr {
                        line(c.0, c.1, mp.0, mp.1)
                        segments.append((c.0, c.1, mp.0, mp.1))
                    }
                    curr = mp
                    currSrc = p
                case .rect(let r):
                    let p0 = map(r.origin)
                    let p1 = map(CGPoint(x: r.origin.x + r.size.width, y: r.origin.y + r.size.height))
                    let ax0 = min(p0.0, p1.0), ay0 = min(p0.1, p1.1)
                    let ax1 = max(p0.0, p1.0), ay1 = max(p0.1, p1.1)
                    let edges = [(ax0, ay0, ax1, ay0), (ax1, ay0, ax1, ay1), (ax1, ay1, ax0, ay1), (ax0, ay1, ax0, ay0)]
                    for e in edges { line(e.0, e.1, e.2, e.3); segments.append(e) }
                case .ellipse(let r):
                    let p0 = map(r.origin)
                    let p1 = map(CGPoint(x: r.origin.x + r.size.width, y: r.origin.y + r.size.height))
                    let ax0 = min(p0.0, p1.0), ay0 = min(p0.1, p1.1)
                    let ax1 = max(p0.0, p1.0), ay1 = max(p0.1, p1.1)
                    let cx = Double(ax0 + ax1) / 2.0
                    let cy = Double(ay0 + ay1) / 2.0
                    let rx = max(1.0, Double(ax1 - ax0) / 2.0)
                    let ry = max(1.0, Double(ay1 - ay0) / 2.0)
                    var prev: (Int, Int)? = nil
                    let steps = 64
                    for i in 0...steps {
                        let t = Double(i) * (2.0 * Double.pi) / Double(steps)
                        let x = Int(cx + cos(t) * rx)
                        let y = Int(cy + sin(t) * ry)
                        if let p = prev {
                            line(p.0, p.1, x, y)
                            segments.append((p.0, p.1, x, y))
                        }
                        prev = (x, y)
                    }
                case .quadCurve(to: let p, control: let c):
                    guard let s0 = currSrc else {
                        currSrc = p
                        curr = map(p)
                        break
                    }
                    let steps = 24
                    var prevP = s0
                    for i in 1...steps {
                        let tt = Double(i) / Double(steps)
                        let a = lerp(s0, c, tt)
                        let b = lerp(c, p, tt)
                        let q = lerp(a, b, tt)
                        let m0 = map(prevP)
                        let m1 = map(q)
                        line(m0.0, m0.1, m1.0, m1.1)
                        segments.append((m0.0, m0.1, m1.0, m1.1))
                        prevP = q
                    }
                    currSrc = p
                    curr = map(p)
                case .curve(to: let p, control1: let c1, control2: let c2):
                    guard let s0 = currSrc else {
                        currSrc = p
                        curr = map(p)
                        break
                    }
                    let steps = 32
                    var prevP = s0
                    for i in 1...steps {
                        let tt = Double(i) / Double(steps)
                        let q = cubic(s0, c1, c2, p, tt)
                        let m0 = map(prevP)
                        let m1 = map(q)
                        line(m0.0, m0.1, m1.0, m1.1)
                        segments.append((m0.0, m0.1, m1.0, m1.1))
                        prevP = q
                    }
                    currSrc = p
                    curr = map(p)
                case .closeSubpath:
                    if let c = curr, let s = start {
                        line(c.0, c.1, s.0, s.1)
                        segments.append((c.0, c.1, s.0, s.1))
                    }
                    curr = start
                    currSrc = startSrc
                }
            }

            return (subStroke, segments)
        }

        for (shapeRect, shape) in shapes {
            // Constrain to both term bounds + clip.
            guard let r0 = _intersect(full, shapeRect), let r = _intersect(r0, clip) else { continue }

            let x0 = r.origin.x
            let y0 = r.origin.y
            let x1 = r.origin.x + r.size.width
            let y1 = r.origin.y + r.size.height
            if x1 <= x0 || y1 <= y0 { continue }

            let regionW = x1 - x0
            let regionH = y1 - y0
            let subW = regionW * 2
            let subH = regionH * 4
            if subW <= 0 || subH <= 0 { continue }

            let fillEnabled = (shape.fillStyle != nil)
            let eoFill = shape.fillStyle?.isEOFilled ?? false

            func insideFilledShape(_ sx: Int, _ sy: Int) -> Bool {
                let x = Double(sx) + 0.5
                let y = Double(sy) + 0.5
                let w = Double(subW)
                let h = Double(subH)

                switch shape.kind {
                case .rectangle:
                    return true
                case .roundedRectangle(let crCells):
                    let rx = max(1.0, Double(crCells) * 2.0)
                    let ry = max(1.0, Double(crCells) * 4.0)
                    return insideRoundedRect(x: x, y: y, w: w, h: h, rx: rx, ry: ry)
                case .capsule:
                    let rr = max(1.0, min(w, h) / 2.0)
                    return insideRoundedRect(x: x, y: y, w: w, h: h, rx: rr, ry: rr)
                case .circle, .ellipse:
                    let cx = w / 2.0
                    let cy = h / 2.0
                    let rx = max(1.0, (w - 1.0) / 2.0)
                    let ry = max(1.0, (h - 1.0) / 2.0)
                    let dx = (x - cx) / rx
                    let dy = (y - cy) / ry
                    return (dx * dx + dy * dy) <= 1.0
                case .path:
                    return false
                }
            }

            if shape.kind == .path {
                let (subStroke, segments) = strokePathMask(elements: shape.pathElements ?? [], subW: subW, subH: subH)

                func windingContains(_ x: Double, _ y: Double) -> Bool {
                    var winding = 0
                    for s in segments {
                        let y0 = Double(s.1)
                        let y1 = Double(s.3)
                        let x0 = Double(s.0)
                        let x1 = Double(s.2)
                        if y0 == y1 { continue }
                        let upward = y0 < y1
                        let ymin = min(y0, y1)
                        let ymax = max(y0, y1)
                        if y < ymin || y >= ymax { continue }
                        let t = (y - y0) / (y1 - y0)
                        let ix = x0 + t * (x1 - x0)
                        if ix <= x { continue }
                        winding += upward ? 1 : -1
                    }
                    return winding != 0
                }

                func evenOddContains(_ x: Double, _ y: Double) -> Bool {
                    var inside = false
                    for s in segments {
                        let y0 = Double(s.1)
                        let y1 = Double(s.3)
                        let x0 = Double(s.0)
                        let x1 = Double(s.2)
                        if y0 == y1 { continue }
                        let ymin = min(y0, y1)
                        let ymax = max(y0, y1)
                        if y < ymin || y >= ymax { continue }
                        let t = (y - y0) / (y1 - y0)
                        let ix = x0 + t * (x1 - x0)
                        if ix > x { inside.toggle() }
                    }
                    return inside
                }

                if segments.isEmpty { continue }

                for cy in y0..<y1 {
                    for cx in x0..<x1 {
                        if !isEmpty(cx, cy) { continue }
                        let baseSX = (cx - x0) * 2
                        let baseSY = (cy - y0) * 4

                        var strokeMask: UInt8 = 0
                        for sy in 0..<4 {
                            for sx in 0..<2 {
                                if subStroke[(baseSY + sy) * subW + (baseSX + sx)] {
                                    strokeMask |= dotBit(sx, sy)
                                }
                            }
                        }

                        var fillMask: UInt8 = 0
                        var fillCount = 0
                        if fillEnabled {
                            for sy in 0..<4 {
                                for sx in 0..<2 {
                                    let px = Double(baseSX + sx) + 0.5
                                    let py = Double(baseSY + sy) + 0.5
                                    let ins = eoFill ? evenOddContains(px, py) : windingContains(px, py)
                                    if ins {
                                        fillCount += 1
                                        fillMask |= dotBit(sx, sy)
                                    }
                                }
                            }
                        }

                        if fillEnabled, fillCount == 8, strokeMask == 0 {
                            set(cx, cy, " ", fillBG, fillBG)
                            continue
                        }

                        let mask = strokeMask | ((fillEnabled && fillCount > 0 && fillCount < 8) ? fillMask : 0)
                        if mask == 0 { continue }
                        let scalar = UnicodeScalar(0x2800 + Int(mask))!
                        let bg = fillEnabled ? fillBG : baseBG
                        set(cx, cy, String(Character(scalar)), strokeFG, bg)
                    }
                }
                continue
            }

            // Filled primitives.
            for cy in y0..<y1 {
                for cx in x0..<x1 {
                    if !isEmpty(cx, cy) { continue }
                    let baseSX = (cx - x0) * 2
                    let baseSY = (cy - y0) * 4
                    var mask: UInt8 = 0
                    var insideCount = 0
                    for sy in 0..<4 {
                        for sx in 0..<2 {
                            if insideFilledShape(baseSX + sx, baseSY + sy) {
                                insideCount += 1
                                mask |= dotBit(sx, sy)
                            }
                        }
                    }
                    if fillEnabled, insideCount == 8 {
                        set(cx, cy, " ", fillBG, fillBG)
                        continue
                    }
                    if mask == 0 { continue }
                    let scalar = UnicodeScalar(0x2800 + Int(mask))!
                    let bg = fillEnabled ? fillBG : baseBG
                    set(cx, cy, String(Character(scalar)), strokeFG, bg)
                }
            }
        }
    }
}

private func _intersect(_ a: _Rect, _ b: _Rect) -> _Rect? {
    let x0 = max(a.origin.x, b.origin.x)
    let y0 = max(a.origin.y, b.origin.y)
    let x1 = min(a.origin.x + a.size.width, b.origin.x + b.size.width)
    let y1 = min(a.origin.y + a.size.height, b.origin.y + b.size.height)
    if x1 <= x0 || y1 <= y0 { return nil }
    return _Rect(origin: _Point(x: x0, y: y0), size: _Size(width: x1 - x0, height: y1 - y0))
}
