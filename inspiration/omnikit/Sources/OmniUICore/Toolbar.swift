public enum ToolbarItemPlacement: Hashable, Sendable {
    case automatic
    case cancellationAction
    case confirmationAction
    case topBarLeading
    case topBarTrailing
    case navigationBarLeading
    case navigationBarTrailing
    case bottomBar
    case principal
}

public enum ToolbarPlacement: Hashable, Sendable {
    case sidebarToggle
}

struct _ToolbarItemTag: Hashable {
    let placement: ToolbarItemPlacement
}

struct _ToolbarLayoutItems {
    var leading: [_VNode] = []
    var principal: [_VNode] = []
    var trailing: [_VNode] = []
    var bottom: [_VNode] = []

    var isEmpty: Bool {
        leading.isEmpty && principal.isEmpty && trailing.isEmpty && bottom.isEmpty
    }
}

func _collectToolbarItems(from node: _VNode) -> _ToolbarLayoutItems {
    var out = _ToolbarLayoutItems()

    func walk(_ node: _VNode) {
        switch node {
        case .tagged(let value, let label):
            if let tag = value.base as? _ToolbarItemTag {
                switch tag.placement {
                case .navigationBarLeading, .topBarLeading, .cancellationAction:
                    out.leading.append(label)
                case .navigationBarTrailing, .topBarTrailing, .confirmationAction, .automatic:
                    out.trailing.append(label)
                case .principal:
                    out.principal.append(label)
                case .bottomBar:
                    out.bottom.append(label)
                }
                return
            }
            walk(label)
        case .group(let children):
            for child in children { walk(child) }
        case .stack(_, _, let children):
            for child in children { walk(child) }
        case .zstack(let children):
            for child in children { walk(child) }
        case .background(let child, let background):
            walk(child)
            walk(background)
        case .overlay(let child, let overlay):
            walk(child)
            walk(overlay)
        case .frame(_, _, _, _, _, _, let child):
            walk(child)
        case .offset(_, _, let child):
            walk(child)
        case .opacity(_, let child):
            walk(child)
        case .edgePadding(_, _, _, _, let child):
            walk(child)
        case .contentShapeRect(let child):
            walk(child)
        case .clip(_, let child):
            walk(child)
        case .shadow(let child, _, _, _, _):
            walk(child)
        case .style(_, _, let child):
            walk(child)
        case .textStyled(_, let child):
            walk(child)
        case .gradient:
            break
        default:
            break
        }
    }

    walk(node)
    return out
}

public struct ToolbarItem<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    public let placement: ToolbarItemPlacement
    let content: Content

    public init(placement: ToolbarItemPlacement = .automatic, @ViewBuilder content: () -> Content) {
        self.placement = placement
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .tagged(
            value: AnyHashable(_ToolbarItemTag(placement: placement)),
            label: ctx.buildChild(content)
        )
    }
}

public struct ToolbarItemGroup<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    public let placement: ToolbarItemPlacement
    let content: Content

    public init(placement: ToolbarItemPlacement = .automatic, @ViewBuilder content: () -> Content) {
        self.placement = placement
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .tagged(
            value: AnyHashable(_ToolbarItemTag(placement: placement)),
            label: ctx.buildChild(content)
        )
    }
}

public struct EditButton: View, _PrimitiveView {
    public typealias Body = Never
    let actionScopePath: [Int]

    public init() {
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let controlPath = ctx.path
        let isFocused = runtime._isFocused(path: controlPath)
        let editModeBinding = (_UIRuntime._currentEnvironment ?? runtime._baseEnvironment).editMode
        let isEditing = editModeBinding?.wrappedValue.isEditing == true

        let id = runtime._registerAction({
            runtime._setFocus(path: controlPath)
            guard let editModeBinding else { return }
            editModeBinding.wrappedValue = isEditing ? .inactive : .active
        }, path: actionScopePath)
        runtime._registerFocusable(path: controlPath, activate: id)

        return .button(id: id, isFocused: isFocused, label: .text(isEditing ? "Done" : "Edit"))
    }
}
