public protocol ViewModifier {
    associatedtype Body: View

    /// Matches SwiftUI's `ViewModifier.Content` typealias.
    typealias Content = _ViewModifier_Content<Self>

    @ViewBuilder func body(content: Content) -> Body
}

public struct _ViewModifier_Content<Modifier: ViewModifier>: View, _PrimitiveView {
    public typealias Body = Never

    let content: AnyView

    public init(_ content: AnyView) {
        self.content = content
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.buildChild(content)
    }
}

public extension View {
    func modifier<M: ViewModifier>(_ modifier: M) -> some View {
        modifier.body(content: _ViewModifier_Content(AnyView(self)))
    }
}

