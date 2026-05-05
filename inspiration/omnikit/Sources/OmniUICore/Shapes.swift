// Minimal shape API surface for SwiftUI compatibility.
//
// These lower into the shared render tree and are rasterized by the debug/notcurses renderers.

import Foundation

// On Linux, CGPoint/CGSize/CGRect from swift-corelibs-foundation don't
// conform to Hashable. Provide conformance so Path.Element can derive it.
#if !canImport(CoreGraphics)
extension CGPoint: @retroactive Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(x)
        hasher.combine(y)
    }
}
extension CGSize: @retroactive Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(width)
        hasher.combine(height)
    }
}
extension CGRect: @retroactive Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(origin)
        hasher.combine(size)
    }
}
#endif

public enum RoundedCornerStyle: Hashable, Sendable {
    case circular
    case continuous
}

public struct Path: Hashable, Sendable, Shape, _PrimitiveView {
    public enum Element: Hashable, Sendable {
        case move(to: CGPoint)
        case line(to: CGPoint)
        case quadCurve(to: CGPoint, control: CGPoint)
        case curve(to: CGPoint, control1: CGPoint, control2: CGPoint)
        case rect(CGRect)
        case ellipse(CGRect)
        case closeSubpath
    }

    public private(set) var elements: [Element] = []

    public init() {}

    public init(_ rect: CGRect) {
        elements = [.rect(rect)]
    }

    public mutating func move(to p: CGPoint) { elements.append(.move(to: p)) }
    public mutating func addLine(to p: CGPoint) { elements.append(.line(to: p)) }
    public mutating func addQuadCurve(to p: CGPoint, control: CGPoint) { elements.append(.quadCurve(to: p, control: control)) }
    public mutating func addCurve(to p: CGPoint, control1: CGPoint, control2: CGPoint) { elements.append(.curve(to: p, control1: control1, control2: control2)) }
    public mutating func addRect(_ rect: CGRect) { elements.append(.rect(rect)) }
    public mutating func addEllipse(in rect: CGRect) { elements.append(.ellipse(rect)) }
    public mutating func closeSubpath() { elements.append(.closeSubpath) }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .shape(_ShapeNode(kind: .path, pathElements: elements))
    }
}

public protocol Shape: View {}

public struct Rectangle: Shape, _PrimitiveView {
    public typealias Body = Never
    public init() {}
    func _makeNode(_ ctx: inout _BuildContext) -> _VNode { .shape(_ShapeNode(kind: .rectangle)) }
}

public struct RoundedRectangle: Shape, _PrimitiveView {
    public typealias Body = Never
    public var cornerRadius: CGFloat

    public init(cornerRadius: CGFloat) {
        self.cornerRadius = cornerRadius
    }

    public init(cornerRadius: CGFloat, style: RoundedCornerStyle = .circular) {
        _ = style
        self.cornerRadius = cornerRadius
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .shape(_ShapeNode(kind: .roundedRectangle(cornerRadius: Int(cornerRadius))))
    }
}

public struct Circle: Shape, _PrimitiveView {
    public typealias Body = Never
    public init() {}
    func _makeNode(_ ctx: inout _BuildContext) -> _VNode { .shape(_ShapeNode(kind: .circle)) }
}

public struct Ellipse: Shape, _PrimitiveView {
    public typealias Body = Never
    public init() {}
    func _makeNode(_ ctx: inout _BuildContext) -> _VNode { .shape(_ShapeNode(kind: .ellipse)) }
}

public struct Capsule: Shape, _PrimitiveView {
    public typealias Body = Never
    public init() {}
    public init(style: RoundedCornerStyle = .circular) { _ = style }
    func _makeNode(_ ctx: inout _BuildContext) -> _VNode { .shape(_ShapeNode(kind: .capsule)) }
}

public struct FillStyle: Hashable, Sendable {
    public var isEOFilled: Bool
    public var antialiased: Bool
    public init(eoFill: Bool = false, antialiased: Bool = true) {
        self.isEOFilled = eoFill
        self.antialiased = antialiased
    }
}

public struct StrokeStyle: Hashable, Sendable {
    public var lineWidth: CGFloat
    public init(lineWidth: CGFloat = 1) { self.lineWidth = lineWidth }
}

public struct ShapeView<S: Shape>: View, _PrimitiveView {
    public typealias Body = Never
    let shape: S
    public init(_ shape: S) { self.shape = shape }
    func _makeNode(_ ctx: inout _BuildContext) -> _VNode { ctx.buildChild(shape) }
}

public extension Shape {
    func fill(_ content: Color = .primary, style: FillStyle = FillStyle()) -> some View {
        _ShapeStyle(content: AnyView(self), fill: style, stroke: nil, fillColor: content, strokeColor: nil)
    }

    func fill(_ content: Material, style: FillStyle = FillStyle()) -> some View {
        _ShapeStyle(content: AnyView(self), fill: style, stroke: nil, fillColor: Color(content.raw), strokeColor: nil)
    }

    func fill<S>(_ content: S, style: FillStyle = FillStyle()) -> some View {
        _ = content
        // Stub: treat arbitrary ShapeStyle-ish values (e.g. gradients) as a simple fill.
        return _ShapeStyle(content: AnyView(self), fill: style, stroke: nil, fillColor: .secondary, strokeColor: nil)
    }

    func stroke(_ content: Color = .primary, style: StrokeStyle = StrokeStyle()) -> some View {
        _ShapeStyle(content: AnyView(self), fill: nil, stroke: style, fillColor: nil, strokeColor: content)
    }

    func stroke<S>(_ content: S, lineWidth: CGFloat = 1) -> some View {
        _ = content
        return _ShapeStyle(content: AnyView(self), fill: nil, stroke: StrokeStyle(lineWidth: lineWidth), fillColor: nil, strokeColor: .primary)
    }

    func stroke(lineWidth: CGFloat = 1) -> some View {
        _ShapeStyle(content: AnyView(self), fill: nil, stroke: StrokeStyle(lineWidth: lineWidth), fillColor: nil, strokeColor: .primary)
    }
}

private struct _ShapeStyle: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let fill: FillStyle?
    let stroke: StrokeStyle?
    let fillColor: Color?
    let strokeColor: Color?

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let n = ctx.buildChild(content)
        guard case .shape(var s) = n else { return n }
        s.fillStyle = fill
        s.strokeStyle = stroke
        if let fillColor { s.fillColor = fillColor }
        if let strokeColor { s.strokeColor = strokeColor }
        return .shape(s)
    }
}
