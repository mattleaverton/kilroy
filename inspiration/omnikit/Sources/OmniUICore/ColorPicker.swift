// Terminal-friendly ColorPicker approximation for SwiftUI compatibility.

public struct ColorPicker: View, _PrimitiveView {
    public typealias Body = Never

    let title: String
    let selection: Binding<Color>
    let supportsOpacity: Bool
    let actionScopePath: [Int]

    public init(_ titleKey: String, selection: Binding<Color>, supportsOpacity: Bool = true) {
        self.title = titleKey
        self.selection = selection
        self.supportsOpacity = supportsOpacity
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _ = supportsOpacity

        let labelText = _UIRuntime._labelsHidden ? selection.wrappedValue.name : "\(title): \(selection.wrappedValue.name)"

        guard _UIRuntime._hitTestingEnabled else {
            return .text(labelText)
        }

        let runtime = ctx.runtime
        let controlPath = ctx.path
        let isFocused = runtime._isFocused(path: controlPath)

        let id = runtime._registerAction({
            runtime._setFocus(path: controlPath)
            selection.wrappedValue = ColorPicker._nextColor(after: selection.wrappedValue)
        }, path: actionScopePath)
        runtime._registerFocusable(path: controlPath, activate: id)

        let labelNode = ctx.buildChild(Text(labelText))
        return .button(id: id, isFocused: isFocused, label: labelNode)
    }

    private static let _cycle: [Color] = [.red, .green, .blue, .yellow, .white, .black, .gray]

    private static func _nextColor(after current: Color) -> Color {
        guard !_cycle.isEmpty else { return current }
        if let idx = _cycle.firstIndex(where: { $0.name == current.name }) {
            return _cycle[(idx + 1) % _cycle.count]
        }
        return _cycle[0]
    }
}
