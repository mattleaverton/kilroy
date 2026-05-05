/// SwiftUI-like base protocol.
///
/// OmniUI aims to preserve the surface area and value semantics of SwiftUI where practical,
/// while keeping the runtime portable (Linux) and concurrency-friendly (Swift 6 strict mode).
public protocol View {
    associatedtype Body: View
    @ViewBuilder var body: Body { get }
}

public extension View where Body == Never {
    var body: Never { fatalError("Primitive views have no body") }
}

extension Never: View {
    public typealias Body = Never
    public var body: Never { fatalError("Never has no body") }
}

/// Type erasure for `View`.
public struct AnyView: View {
    public typealias Body = Never

    let _makeNode: (inout _BuildContext) -> _VNode

    public init<V: View>(_ view: V) {
        self._makeNode = { ctx in
            OmniUICore._makeNode(view, &ctx)
        }
    }
}

extension AnyView: _PrimitiveView {
    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _makeNode(&ctx)
    }
}

/// Internal protocol for primitive views that directly lower to nodes (do not go through `body`).
protocol _PrimitiveView {
    func _makeNode(_ ctx: inout _BuildContext) -> _VNode
}

@inline(__always)
func _makeNode<V: View>(_ view: V, _ ctx: inout _BuildContext) -> _VNode {
    if let primitive = view as? _PrimitiveView {
        return primitive._makeNode(&ctx)
    }
    return _makeNode(view.body, &ctx)
}
