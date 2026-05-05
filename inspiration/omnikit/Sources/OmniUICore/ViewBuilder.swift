/// A tiny subset of SwiftUI's `ViewBuilder`.
@resultBuilder
public enum ViewBuilder {
    public static func buildExpression(_ expression: Never) -> Never {
        fatalError("Unreachable")
    }

    public static func buildExpression(_ expression: AnyView) -> AnyView {
        expression
    }

    public static func buildExpression<V: View>(_ expression: V) -> AnyView {
        AnyView(expression)
    }

    public static func buildBlock(_ content: Never) -> Never {
        content
    }

    // Keep the builder's component type stable (`AnyView`) to avoid generic inference
    // failures in complex `if` / `switch` / availability blocks.
    public static func buildBlock() -> AnyView { AnyView(EmptyView()) }
    public static func buildBlock(_ c0: AnyView) -> AnyView { c0 }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView, _ c3: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2, c3]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView, _ c3: AnyView, _ c4: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2, c3, c4]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView, _ c3: AnyView, _ c4: AnyView, _ c5: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2, c3, c4, c5]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView, _ c3: AnyView, _ c4: AnyView, _ c5: AnyView, _ c6: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2, c3, c4, c5, c6]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView, _ c3: AnyView, _ c4: AnyView, _ c5: AnyView, _ c6: AnyView, _ c7: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2, c3, c4, c5, c6, c7]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView, _ c3: AnyView, _ c4: AnyView, _ c5: AnyView, _ c6: AnyView, _ c7: AnyView, _ c8: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2, c3, c4, c5, c6, c7, c8]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView, _ c3: AnyView, _ c4: AnyView, _ c5: AnyView, _ c6: AnyView, _ c7: AnyView, _ c8: AnyView, _ c9: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2, c3, c4, c5, c6, c7, c8, c9]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView, _ c3: AnyView, _ c4: AnyView, _ c5: AnyView, _ c6: AnyView, _ c7: AnyView, _ c8: AnyView, _ c9: AnyView, _ c10: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView, _ c3: AnyView, _ c4: AnyView, _ c5: AnyView, _ c6: AnyView, _ c7: AnyView, _ c8: AnyView, _ c9: AnyView, _ c10: AnyView, _ c11: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView, _ c3: AnyView, _ c4: AnyView, _ c5: AnyView, _ c6: AnyView, _ c7: AnyView, _ c8: AnyView, _ c9: AnyView, _ c10: AnyView, _ c11: AnyView, _ c12: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView, _ c3: AnyView, _ c4: AnyView, _ c5: AnyView, _ c6: AnyView, _ c7: AnyView, _ c8: AnyView, _ c9: AnyView, _ c10: AnyView, _ c11: AnyView, _ c12: AnyView, _ c13: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13]))
    }

    public static func buildBlock(_ c0: AnyView, _ c1: AnyView, _ c2: AnyView, _ c3: AnyView, _ c4: AnyView, _ c5: AnyView, _ c6: AnyView, _ c7: AnyView, _ c8: AnyView, _ c9: AnyView, _ c10: AnyView, _ c11: AnyView, _ c12: AnyView, _ c13: AnyView, _ c14: AnyView) -> AnyView {
        AnyView(TupleView([c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14]))
    }

    public static func buildOptional(_ content: AnyView?) -> AnyView {
        content ?? AnyView(EmptyView())
    }

    public static func buildEither(first: AnyView) -> AnyView { first }
    public static func buildEither(second: AnyView) -> AnyView { second }

    public static func buildArray(_ components: [AnyView]) -> AnyView {
        AnyView(TupleView(components))
    }
}

public struct EmptyView: View, _PrimitiveView {
    public typealias Body = Never
    public init() {}

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .empty
    }
}

public struct TupleView: View, _PrimitiveView {
    public typealias Body = Never

    let children: [AnyView]

    public init(_ children: [AnyView]) {
        self.children = children
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        var nodes: [_VNode] = []
        nodes.reserveCapacity(children.count)
        for child in children {
            nodes.append(ctx.buildChild(child))
        }
        return .group(nodes)
    }
}
