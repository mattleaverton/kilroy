import Foundation

public struct GraphicsContext: Sendable {
    fileprivate enum _Command: Hashable, Sendable {
        case fillShape(_ShapeNode)
    }

    fileprivate var commands: [_Command] = []

    public init() {}

    public struct Shading: Hashable, Sendable {
        enum _Kind: Hashable, Sendable {
            case color(Color)
        }

        var kind: _Kind

        public static func color(_ color: Color) -> Shading {
            Shading(kind: .color(color))
        }
    }

    public mutating func fill<S: Shape>(_ shape: S, with shading: Shading, style: FillStyle = FillStyle()) {
        guard var node = _shapeNode(for: shape) else { return }
        node.fillStyle = style
        node.strokeStyle = nil
        switch shading.kind {
        case .color(let color):
            node.fillColor = color
        }
        commands.append(.fillShape(node))
    }
}

public struct Canvas: View, _PrimitiveView {
    public typealias Body = Never

    let renderer: (inout GraphicsContext, CGSize) -> Void

    public init(
        opaque: Bool = false,
        colorMode: Any = (),
        rendersAsynchronously: Bool = false,
        renderer: @escaping (inout GraphicsContext, CGSize) -> Void
    ) {
        _ = opaque
        _ = colorMode
        _ = rendersAsynchronously
        self.renderer = renderer
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let rs = _UIRuntime._currentRenderSize ?? _Size(width: 0, height: 0)
        var gc = GraphicsContext()
        renderer(&gc, CGSize(width: CGFloat(rs.width), height: CGFloat(rs.height)))
        let nodes = gc.commands.compactMap { command -> _VNode? in
            switch command {
            case .fillShape(let shape):
                return .shape(shape)
            }
        }
        return nodes.isEmpty ? .empty : .group(nodes)
    }
}

public struct Gradient: Hashable, Sendable {
    public var colors: [Color]
    public init(colors: [Color]) {
        self.colors = colors
    }
}

public struct UnitPoint: Hashable, Sendable {
    public var x: CGFloat
    public var y: CGFloat

    public init(x: CGFloat, y: CGFloat) {
        self.x = x
        self.y = y
    }

    public static let center = UnitPoint(x: 0.5, y: 0.5)
    public static let topLeading = UnitPoint(x: 0.0, y: 0.0)
    public static let bottomTrailing = UnitPoint(x: 1.0, y: 1.0)
}

public struct LinearGradient: View, _PrimitiveView {
    public typealias Body = Never

    public let gradient: Gradient
    public let startPoint: UnitPoint
    public let endPoint: UnitPoint

    public init(gradient: Gradient, startPoint: UnitPoint, endPoint: UnitPoint) {
        self.gradient = gradient
        self.startPoint = startPoint
        self.endPoint = endPoint
    }

    public init(colors: [Color], startPoint: UnitPoint, endPoint: UnitPoint) {
        self.init(gradient: Gradient(colors: colors), startPoint: startPoint, endPoint: endPoint)
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _ = ctx
        return .gradient(
            _GradientNode(
                kind: .linear(startPoint: startPoint, endPoint: endPoint),
                colors: gradient.colors
            )
        )
    }
}

public struct RadialGradient: View, _PrimitiveView {
    public typealias Body = Never

    public let gradient: Gradient
    public let center: UnitPoint
    public let startRadius: CGFloat
    public let endRadius: CGFloat

    public init(gradient: Gradient, center: UnitPoint, startRadius: CGFloat, endRadius: CGFloat) {
        self.gradient = gradient
        self.center = center
        self.startRadius = startRadius
        self.endRadius = endRadius
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _ = ctx
        return .gradient(
            _GradientNode(
                kind: .radial(center: center, startRadius: startRadius, endRadius: endRadius),
                colors: gradient.colors
            )
        )
    }
}

private func _shapeNode<S: Shape>(for shape: S) -> _ShapeNode? {
    if let path = shape as? Path {
        return _ShapeNode(kind: .path, pathElements: path.elements)
    }
    if let rounded = shape as? RoundedRectangle {
        return _ShapeNode(kind: .roundedRectangle(cornerRadius: Int(rounded.cornerRadius.rounded())))
    }
    if shape is Rectangle {
        return _ShapeNode(kind: .rectangle)
    }
    if shape is Circle {
        return _ShapeNode(kind: .circle)
    }
    if shape is Ellipse {
        return _ShapeNode(kind: .ellipse)
    }
    if shape is Capsule {
        return _ShapeNode(kind: .capsule)
    }
    return nil
}
