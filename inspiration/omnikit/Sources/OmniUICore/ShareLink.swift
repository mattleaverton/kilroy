import Foundation

/// Minimal `ShareLink` shim.
///
/// SwiftUI's `ShareLink` presents a platform share sheet. For OmniUICore, we model it as a button that
/// invokes the current `openURL` environment action for `URL` items.
public struct ShareLink<Label: View>: View, _PrimitiveView {
    public typealias Body = Never

    let item: URL
    let label: Label
    let actionScopePath: [Int]

    public init(item: URL, @ViewBuilder label: () -> Label) {
        self.item = item
        self.label = label()
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime

        guard _UIRuntime._hitTestingEnabled else {
            return ctx.buildChild(label)
        }

        // Capture `openURL` at build time so environment overrides apply when the action fires.
        let openURL = (_UIRuntime._currentEnvironment ?? runtime._baseEnvironment).openURL
        let controlPath = ctx.path
        let isFocused = runtime._isFocused(path: controlPath)

        let id = runtime._registerAction({
            runtime._setFocus(path: controlPath)
            _ = openURL(item)
        }, path: actionScopePath)
        runtime._registerFocusable(path: controlPath, activate: id)

        let labelNode = ctx.buildChild(label)
        return .button(id: id, isFocused: isFocused, label: labelNode)
    }
}

