import Foundation

public struct _Point: Hashable, Sendable {
    public var x: Int
    public var y: Int
    public init(x: Int, y: Int) { self.x = x; self.y = y }
}

public struct _Size: Hashable, Sendable {
    public var width: Int
    public var height: Int
    public init(width: Int, height: Int) { self.width = width; self.height = height }
}

public struct _Rect: Hashable, Sendable {
    public var origin: _Point
    public var size: _Size
    public init(origin: _Point, size: _Size) { self.origin = origin; self.size = size }

    public func contains(_ p: _Point) -> Bool {
        p.x >= origin.x &&
        p.y >= origin.y &&
        p.x < origin.x + size.width &&
        p.y < origin.y + size.height
    }
}

struct _ActionID: Hashable {
    let raw: Int
}

struct _HoverID: Hashable {
    let raw: Int
}

enum _Axis: Sendable {
    case horizontal
    case vertical
}

indirect enum _VNode {
    case empty
    case group([_VNode])
    case text(String)
    case image(String)
    case style(fg: Color?, bg: Color?, child: _VNode)
    case textStyled(style: TextStyle, child: _VNode)
    case contentShapeRect(child: _VNode)
    case clip(kind: _ShapeKind, child: _VNode)
    case shadow(child: _VNode, color: Color, radius: Int, x: Int, y: Int)
    case background(child: _VNode, background: _VNode)
    case overlay(child: _VNode, overlay: _VNode)
    case frame(width: Int?, height: Int?, minWidth: Int?, maxWidth: Int?, minHeight: Int?, maxHeight: Int?, child: _VNode)
    case edgePadding(top: Int, leading: Int, bottom: Int, trailing: Int, child: _VNode)
    case spacer
    case stack(axis: _Axis, spacing: Int, children: [_VNode])
    case zstack(children: [_VNode])
    case gradient(_GradientNode)
    case shape(_ShapeNode)
    case offset(x: Int, y: Int, child: _VNode)
    case opacity(CGFloat, child: _VNode)
    case button(id: _ActionID, isFocused: Bool, label: _VNode)
    case tapTarget(id: _ActionID, child: _VNode)
    case hover(id: _HoverID, child: _VNode)
    case toggle(id: _ActionID, isFocused: Bool, isOn: Bool, label: _VNode)
    case textField(id: _ActionID, placeholder: String, text: String, cursor: Int, isFocused: Bool, style: _TextFieldStyleKind)
    case scrollView(id: _ActionID, path: [Int], isFocused: Bool, axis: _Axis, offset: Int, content: _VNode)
    case identified(id: AnyHashable, readerScopePath: [Int]?, child: _VNode)
    case onDelete(actionScopePath: [Int], action: (IndexSet) -> Void, child: _VNode)
    case menu(
        id: _ActionID,
        isFocused: Bool,
        isExpanded: Bool,
        title: String,
        value: String,
        items: [(id: _ActionID, isSelected: Bool, isFocused: Bool, label: String)]
    )
    case tagged(value: AnyHashable, label: _VNode)
    case divider
}

public enum _ShapeKind: Hashable, Sendable {
    case rectangle
    case roundedRectangle(cornerRadius: Int)
    case circle
    case ellipse
    case capsule
    case path
}

public enum _GradientKind: Hashable, Sendable {
    case linear(startPoint: UnitPoint, endPoint: UnitPoint)
    case radial(center: UnitPoint, startRadius: CGFloat, endRadius: CGFloat)
}

public struct _GradientNode: Hashable, Sendable {
    public var kind: _GradientKind
    public var colors: [Color]

    public init(kind: _GradientKind, colors: [Color]) {
        self.kind = kind
        self.colors = colors
    }
}

public struct _ShapeNode: Hashable, Sendable {
    public var kind: _ShapeKind
    public var pathElements: [Path.Element]?
    public var fillStyle: FillStyle?
    public var strokeStyle: StrokeStyle?
    public var fillColor: Color?
    public var strokeColor: Color?

    public init(
        kind: _ShapeKind,
        pathElements: [Path.Element]? = nil,
        fillStyle: FillStyle? = FillStyle(),
        strokeStyle: StrokeStyle? = StrokeStyle(lineWidth: 1),
        fillColor: Color? = nil,
        strokeColor: Color? = nil
    ) {
        self.kind = kind
        self.pathElements = pathElements
        self.fillStyle = fillStyle
        self.strokeStyle = strokeStyle
        self.fillColor = fillColor
        self.strokeColor = strokeColor
    }
}
