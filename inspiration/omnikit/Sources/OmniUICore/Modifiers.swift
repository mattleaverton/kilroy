import Foundation

public extension View {
    func font(_ font: Font?) -> some View {
        environment(\.font, font)
    }
    func foregroundStyle(_ color: Color) -> some View { _Style(content: AnyView(self), fg: color, bg: nil) }
    func foregroundColor(_ color: Color) -> some View { _Style(content: AnyView(self), fg: color, bg: nil) }
    func fontWeight(_ weight: Font.Weight?) -> some View {
        switch weight {
        case .bold, .heavy, .black, .semibold, .medium:
            return AnyView(_TextStyleModifier(content: AnyView(self), style: .bold))
        case .light, .thin, .ultraLight:
            return AnyView(_Style(content: AnyView(self), fg: .secondary, bg: nil))
        case .regular, .none:
            return AnyView(self)
        }
    }
    func bold() -> some View { _TextStyleModifier(content: AnyView(self), style: .bold) }
    func italic() -> some View { _TextStyleModifier(content: AnyView(self), style: .italic) }
    func underline() -> some View { _TextStyleModifier(content: AnyView(self), style: .underline) }
    func strikethrough() -> some View { _TextStyleModifier(content: AnyView(self), style: .struck) }
    func multilineTextAlignment(_ alignment: TextAlignment) -> some View {
        environment(\.multilineTextAlignment, alignment)
    }
    func lineLimit(_ limit: Int?) -> some View {
        environment(\.lineLimit, limit)
    }
    func monospacedDigit() -> some View {
        environment(\.monospacedDigits, true)
    }
    func textSelection(_ selection: TextSelection) -> some View {
        environment(\.textSelectionEnabled, selection == .enabled)
    }
    func accentColor(_ color: Color?) -> some View {
        tint(color)
    }

    func tint(_ color: Color?) -> some View {
        if let color {
            return AnyView(environment(\.tint, color))
        }
        return AnyView(environment(\.tint, nil))
    }
    func cornerRadius(_ radius: CGFloat) -> some View {
        clipShape(RoundedRectangle(cornerRadius: radius))
    }
    func shadow(color: Color, radius: CGFloat) -> some View {
        shadow(color: color, radius: radius, x: 0, y: 0)
    }
    func shadow(color: Color, radius: CGFloat, x: CGFloat = 0, y: CGFloat = 0) -> some View {
        _Shadow(content: AnyView(self), color: color, radius: radius, x: x, y: y)
    }
    func opacity(_ value: CGFloat) -> some View {
        _OpacityModifier(content: AnyView(self), opacity: value)
    }
    func offset(x: CGFloat = 0, y: CGFloat = 0) -> some View {
        _OffsetModifier(content: AnyView(self), x: x, y: y)
    }
    func transition(_ t: AnyTransition) -> some View {
        _TransitionModifier(content: AnyView(self), transition: t)
    }
    func ignoresSafeArea() -> some View { _IgnoresSafeAreaModifier(content: AnyView(self)) }
    func clipShape<S: Shape>(_ shape: S, style: FillStyle = FillStyle()) -> some View {
        _ = style
        let kind: _ShapeKind = {
            if let rr = shape as? RoundedRectangle {
                return .roundedRectangle(cornerRadius: Int(rr.cornerRadius.rounded()))
            }
            if shape is Rectangle { return .rectangle }
            if shape is Circle { return .circle }
            if shape is Ellipse { return .ellipse }
            if shape is Capsule { return .capsule }
            if shape is Path { return .path }
            return .rectangle
        }()
        return _Clip(content: AnyView(self), kind: kind)
    }
    func contentShape<S: Shape>(_ shape: S, eoFill: Bool = false) -> some View {
        _ = shape
        _ = eoFill
        return _ContentShapeRect(content: AnyView(self))
    }
    func mask<M: View>(_ mask: M) -> some View {
        _MaskModifier(content: AnyView(self), mask: AnyView(mask))
    }
    func labelsHidden() -> some View {
        _LabelsHidden(content: AnyView(self), hidden: true)
    }

    // Liquid Glass (compile-only stubs)
    func glassEffect() -> some View { _GlassEffectModifier(content: AnyView(self), style: .regular, shape: nil) }
    func glassEffect(_ style: GlassEffect) -> some View { _GlassEffectModifier(content: AnyView(self), style: style, shape: nil) }
    func glassEffect(in shape: GlassEffectShape) -> some View {
        _GlassEffectModifier(content: AnyView(self), style: .regular, shape: shape)
    }

    func background<B: View>(_ background: B) -> some View { _Background(content: AnyView(self), background: AnyView(background)) }
    func background<B: View>(@ViewBuilder _ background: () -> B) -> some View {
        _Background(content: AnyView(self), background: AnyView(background()))
    }
    func background(_ color: Color) -> some View { _Style(content: AnyView(self), fg: nil, bg: color) }
    func background(_ material: Material) -> some View {
        // Terminal-friendly approximation.
        switch material.raw {
        case Material.bar.raw:
            return AnyView(background(Color.gray.opacity(0.2)))
        case Material.background.raw:
            return AnyView(background(Color.gray.opacity(0.1)))
        case Material.ultraThinMaterial.raw:
            return AnyView(background(Color.gray.opacity(0.15)))
        case Material.regularMaterial.raw:
            return AnyView(background(Color.gray.opacity(0.25)))
        default:
            return AnyView(background(Color.gray.opacity(0.2)))
        }
    }
    func overlay<O: View>(_ overlay: O) -> some View { _Overlay(content: AnyView(self), overlay: AnyView(overlay)) }
    func overlay<O: View>(@ViewBuilder _ overlay: () -> O) -> some View {
        _Overlay(content: AnyView(self), overlay: AnyView(overlay()))
    }

    // MARK: SwiftUI API Surface (stubs/passthrough)
    func navigationTitle(_ title: String) -> some View {
        environment(\.navigationTitle, title)
    }
    func navigationTransition(_ transition: NavigationTransition) -> some View {
        _NavigationTransitionModifier(content: AnyView(self), transition: transition)
    }
    func contentTransition(_ transition: ContentTransition) -> some View {
        _ContentTransitionModifier(content: AnyView(self), transition: transition)
    }
    func navigationTitle(_ title: Text) -> some View {
        environment(\.navigationTitle, title.content)
    }
    func navigationBarTitleDisplayMode(_ mode: _NavigationTitleDisplayModeKind = .automatic) -> some View {
        environment(\.navigationTitleDisplayMode, mode)
    }

    func onTapGesture(perform action: @escaping () -> Void) -> some View {
        _TapGesture(content: AnyView(self), count: 1, action: action, actionScopePath: _UIRuntime._currentPath ?? [])
    }

    func onTapGesture(count: Int, perform action: @escaping () -> Void) -> some View {
        _TapGesture(content: AnyView(self), count: count, action: action, actionScopePath: _UIRuntime._currentPath ?? [])
    }

    func allowsHitTesting(_ enabled: Bool) -> some View {
        _AllowsHitTesting(content: AnyView(self), enabled: enabled)
    }

    @ViewBuilder
    func preferredColorScheme(_ scheme: ColorScheme?) -> some View {
        if let scheme {
            environment(\.colorScheme, scheme)
        } else {
            self
        }
    }

    func listStyle<S: ListStyle>(_ style: S) -> some View {
        let kind: _ListStyleKind
        switch style {
        case is SidebarListStyle:
            kind = .sidebar
        case is PlainListStyle:
            kind = .plain
        default:
            kind = .automatic
        }
        return environment(\.listStyleKind, kind)
    }
    func formStyle<S: FormStyle>(_ style: S) -> some View {
        let kind: _FormStyleKind
        switch style {
        case is GroupedFormStyle:
            kind = .grouped
        default:
            kind = .automatic
        }
        return environment(\.formStyleKind, kind)
    }
    func pickerStyle<S: PickerStyle>(_ style: S) -> some View {
        let kind: _PickerStyleKind
        switch style {
        case is SegmentedPickerStyle:
            kind = .segmented
        case is MenuPickerStyle:
            kind = .menu
        default:
            kind = .automatic
        }
        return environment(\.pickerStyleKind, kind)
    }
    func buttonStyle<S: ButtonStyle>(_ style: S) -> some View {
        let kind: _ButtonStyleKind
        switch style {
        case is PlainButtonStyle:
            kind = .plain
        case is BorderedProminentButtonStyle:
            kind = .borderedProminent
        case is PrimaryFillButtonStyle:
            kind = .primaryFill
        case is BorderedButtonStyle:
            kind = .bordered
        default:
            kind = .automatic
        }
        return environment(\.buttonStyleKind, kind)
    }
    func textFieldStyle<S: TextFieldStyle>(_ style: S) -> some View {
        let kind: _TextFieldStyleKind
        switch style {
        case is PlainTextFieldStyle:
            kind = .plain
        case is RoundedBorderTextFieldStyle:
            kind = .roundedBorder
        default:
            kind = .automatic
        }
        return environment(\.textFieldStyleKind, kind)
    }
    func datePickerStyle<S: DatePickerStyle>(_ style: S) -> some View {
        let kind: _DatePickerStyleKind
        switch style {
        case is CompactDatePickerStyle:
            kind = .compact
        case is GraphicalDatePickerStyle:
            kind = .graphical
        case is FieldDatePickerStyle:
            kind = .field
        case is StepperFieldDatePickerStyle:
            kind = .stepperField
        default:
            kind = .automatic
        }
        return environment(\.datePickerStyleKind, kind)
    }
    func gaugeStyle<S: GaugeStyle>(_ style: S) -> some View {
        let kind: _GaugeStyleKind = style is DefaultGaugeStyle ? .default : .automatic
        return environment(\.gaugeStyleKind, kind)
    }
    func toggleStyle<S: ToggleStyle>(_ style: S) -> some View {
        let kind: _ToggleStyleKind = style is SwitchToggleStyle ? .switch : .automatic
        return environment(\.toggleStyleKind, kind)
    }
    func labelStyle<S: LabelStyle>(_ style: S) -> some View {
        let kind: _LabelStyleKind = style is IconOnlyLabelStyle ? .iconOnly : .automatic
        return environment(\.labelStyleKind, kind)
    }
    func navigationViewStyle<S: NavigationViewStyle>(_ style: S) -> some View {
        let kind: _NavigationViewStyleKind
        switch style {
        case is ColumnNavigationViewStyle:
            kind = .column
        case is DoubleColumnNavigationViewStyle:
            kind = .doubleColumn
        case is DefaultNavigationViewStyle:
            kind = .default
        default:
            kind = .automatic
        }
        return environment(\.navigationViewStyleKind, kind)
    }
    func scrollContentBackground(_ visibility: Visibility) -> some View {
        environment(\.scrollContentBackgroundVisibility, visibility)
    }
    func listRowSeparator(_ visibility: Visibility) -> some View {
        environment(\.listRowSeparatorVisibility, visibility)
    }
    func listRowBackground<B: View>(_ background: B?) -> some View {
        if let background {
            return AnyView(environment(\.listRowBackground, _ListRowBackgroundValue(AnyView(background))))
        }
        return AnyView(environment(\.listRowBackground, nil))
    }
    func toolbar<Content: View>(@ViewBuilder content: () -> Content) -> some View {
        _ToolbarModifier(content: AnyView(self), toolbar: AnyView(content()))
    }
    func toolbar(removing placements: ToolbarPlacement...) -> some View {
        let existing = (_UIRuntime._currentEnvironment ?? EnvironmentValues()).toolbarRemovedPlacements
        return environment(\.toolbarRemovedPlacements, existing.union(placements))
    }
    func toolbar(_ any: Any = ()) -> some View {
        if let visibility = any as? Visibility {
            return AnyView(environment(\.toolbarVisibility, visibility))
        }
        if let color = any as? Color {
            var next = (_UIRuntime._currentEnvironment ?? EnvironmentValues()).toolbarBackgroundStyle
            next.color = color
            next.material = nil
            return AnyView(environment(\.toolbarBackgroundStyle, next))
        }
        if let material = any as? Material {
            var next = (_UIRuntime._currentEnvironment ?? EnvironmentValues()).toolbarBackgroundStyle
            next.material = material
            next.color = nil
            return AnyView(environment(\.toolbarBackgroundStyle, next))
        }
        return AnyView(self)
    }
    func toolbarBackground(_ any: Any = (), for: Any = ()) -> some View {
        _ = `for`
        let current = (_UIRuntime._currentEnvironment ?? EnvironmentValues()).toolbarBackgroundStyle
        var next = current
        if let visibility = any as? Visibility {
            next.visibility = visibility
        } else if let color = any as? Color {
            next.color = color
            next.material = nil
        } else if let material = any as? Material {
            next.material = material
            next.color = nil
        }
        return environment(\.toolbarBackgroundStyle, next)
    }
    func controlSize(_ size: ControlSize) -> some View {
        environment(\.controlSize, size)
    }
    func modelContainer(_ any: Any) -> some View {
        if let container = any as? ModelContainer {
            return AnyView(modelContainer(container))
        }
        return AnyView(self)
    }
    func modelContainer(_ container: ModelContainer) -> some View {
        _ModelContainerProvider(content: AnyView(self), container: container)
    }
    func modelContainer(for modelTypes: [Any.Type], inMemory: Bool = false) -> some View {
        _ModelContainerBuilder(content: AnyView(self), modelTypes: modelTypes, inMemory: inMemory)
    }

    func keyboardShortcut(_ key: KeyEquivalent, modifiers: EventModifiers = []) -> some View {
        _KeyboardShortcutBinder(content: AnyView(self), shortcut: KeyboardShortcut(key, modifiers: modifiers))
    }
    func keyboardShortcut(_ shortcut: KeyboardShortcut) -> some View {
        _KeyboardShortcutBinder(content: AnyView(self), shortcut: shortcut)
    }
    func help(_ text: String) -> some View {
        _HelpModifier(content: AnyView(self), text: text)
    }
    func onExitCommand(perform action: @escaping () -> Void) -> some View {
        _OnExitCommandBinder(content: AnyView(self), action: action, actionScopePath: _UIRuntime._currentPath ?? [])
    }

    func sheet<Content: View>(isPresented: Binding<Bool>, @ViewBuilder content: () -> Content) -> some View {
        _Sheet(content: AnyView(self), isPresented: isPresented, onDismiss: nil, sheet: AnyView(content()))
    }
    func sheet<Content: View>(isPresented: Binding<Bool>, onDismiss: (() -> Void)?, @ViewBuilder content: () -> Content) -> some View {
        _Sheet(content: AnyView(self), isPresented: isPresented, onDismiss: onDismiss, sheet: AnyView(content()))
    }
    func sheet<Item: Identifiable, Content: View>(item: Binding<Item?>, onDismiss: (() -> Void)? = nil, @ViewBuilder content: @escaping (Item) -> Content) -> some View {
        _ItemPresentationOverlay(content: AnyView(self), item: item, onDismiss: onDismiss, title: "Sheet", showsScrim: true, presented: { AnyView(content($0)) })
    }
    func fullScreenCover<Content: View>(isPresented: Binding<Bool>, onDismiss: (() -> Void)? = nil, @ViewBuilder content: () -> Content) -> some View {
        _PresentationOverlay(content: AnyView(self), isPresented: isPresented, onDismiss: onDismiss, title: "Full Screen", showsScrim: true, presented: AnyView(content()))
    }
    func popover<Content: View>(isPresented: Binding<Bool>, attachmentAnchor: Any = (), arrowEdge: Edge = .top, @ViewBuilder content: () -> Content) -> some View {
        _ = attachmentAnchor
        _ = arrowEdge
        return _PresentationOverlay(content: AnyView(self), isPresented: isPresented, onDismiss: nil, title: "Popover", showsScrim: false, presented: AnyView(content()))
    }
    func popover<Item: Identifiable, Content: View>(item: Binding<Item?>, attachmentAnchor: Any = (), arrowEdge: Edge = .top, @ViewBuilder content: @escaping (Item) -> Content) -> some View {
        _ = attachmentAnchor
        _ = arrowEdge
        return _ItemPresentationOverlay(content: AnyView(self), item: item, onDismiss: nil, title: "Popover", showsScrim: false, presented: { AnyView(content($0)) })
    }
    func confirmationDialog<Actions: View>(_ title: String, isPresented: Binding<Bool>, titleVisibility: Visibility = .automatic, @ViewBuilder actions: () -> Actions) -> some View {
        _ConfirmationDialog(content: AnyView(self), title: title, isPresented: isPresented, titleVisibility: titleVisibility, actions: AnyView(actions()), message: nil)
    }
    func confirmationDialog<Actions: View, Message: View>(_ title: String, isPresented: Binding<Bool>, titleVisibility: Visibility = .automatic, @ViewBuilder actions: () -> Actions, @ViewBuilder message: () -> Message) -> some View {
        _ConfirmationDialog(content: AnyView(self), title: title, isPresented: isPresented, titleVisibility: titleVisibility, actions: AnyView(actions()), message: AnyView(message()))
    }
    func confirmationDialog<Data, Actions: View>(_ title: String, isPresented: Binding<Bool>, titleVisibility: Visibility = .automatic, presenting data: Data, @ViewBuilder actions: (Data) -> Actions) -> some View {
        _ConfirmationDialog(content: AnyView(self), title: title, isPresented: isPresented, titleVisibility: titleVisibility, actions: AnyView(actions(data)), message: nil)
    }
    func confirmationDialog<Data, Actions: View, Message: View>(_ title: String, isPresented: Binding<Bool>, titleVisibility: Visibility = .automatic, presenting data: Data, @ViewBuilder actions: (Data) -> Actions, @ViewBuilder message: (Data) -> Message) -> some View {
        _ConfirmationDialog(content: AnyView(self), title: title, isPresented: isPresented, titleVisibility: titleVisibility, actions: AnyView(actions(data)), message: AnyView(message(data)))
    }
    func navigationDestination<Value: Hashable, Destination: View>(for value: Value.Type, @ViewBuilder destination: @escaping (Value) -> Destination) -> some View {
        _NavigationDestinationResolver(content: AnyView(self), valueType: value, destination: { AnyView(destination($0)) })
    }
    func navigationDestination<Destination: View>(isPresented: Binding<Bool>, @ViewBuilder destination: () -> Destination) -> some View {
        _NavigationDestinationBool(content: AnyView(self), isPresented: isPresented, destination: AnyView(destination()), ownerKeyOverride: nil)
    }
    func navigationDestination<Item: Identifiable, Destination: View>(item: Binding<Item?>, @ViewBuilder destination: @escaping (Item) -> Destination) -> some View {
        _NavigationDestinationItem(content: AnyView(self), item: item, destination: { AnyView(destination($0)) })
    }
    func alert<Content: View>(isPresented: Binding<Bool>, @ViewBuilder content: () -> Content) -> some View {
        _Alert(content: AnyView(self), isPresented: isPresented, alert: AnyView(content()))
    }

    func alert(isPresented: Binding<Bool>, content: @escaping () -> Alert) -> some View {
        _AlertFromType(content: AnyView(self), isPresented: isPresented, makeAlert: content)
    }

    func focused(_ isFocused: Binding<Bool>) -> some View {
        _FocusBoolBinder(content: AnyView(self), get: { isFocused.wrappedValue }, set: { isFocused.wrappedValue = $0 })
    }

    func focused(_ isFocused: Bool) -> some View {
        _FocusBoolValueBinder(content: AnyView(self), isFocused: isFocused)
    }

    func focused(_ isFocused: FocusState<Bool>.Binding) -> some View {
        _FocusBoolBinder(content: AnyView(self), get: { isFocused.wrappedValue }, set: { isFocused.wrappedValue = $0 })
    }

    func id<ID: Hashable>(_ id: ID) -> some View {
        _Identified(content: AnyView(self), id: AnyHashable(id), readerScopePath: _UIRuntime._currentScrollReaderScopePath)
    }

    func onDelete(perform action: @escaping (IndexSet) -> Void) -> some View {
        _OnDelete(content: AnyView(self), action: action, actionScopePath: _UIRuntime._currentPath ?? [])
    }

    func onChange<V: Equatable>(of value: V, perform action: @escaping (_ newValue: V) -> Void) -> some View {
        _OnChange(content: AnyView(self), value: value, action: { _, newValue in action(newValue) })
    }

    func onChange<V: Equatable>(of value: V, perform action: @escaping (_ oldValue: V, _ newValue: V) -> Void) -> some View {
        _OnChange(content: AnyView(self), value: value, action: action)
    }

    func onChange<V: Equatable>(of value: V, initial: Bool = false, _ action: @escaping () -> Void) -> some View {
        _OnChangeSimple(content: AnyView(self), value: value, initial: initial, action: action)
    }

    func onChange<V: Equatable>(of value: V, initial: Bool = false, _ action: @escaping (_ oldValue: V, _ newValue: V) -> Void) -> some View {
        _OnChangeWithInitial(content: AnyView(self), value: value, initial: initial, action: action)
    }

    func task(priority: Any? = nil, _ action: @escaping () async -> Void) -> some View {
        _ = priority
        return _TaskBinder(content: AnyView(self), action: action)
    }

    func onSubmit(_ action: @escaping () -> Void) -> some View {
        _OnSubmitBinder(content: AnyView(self), action: action, actionScopePath: _UIRuntime._currentPath ?? [])
    }
    func submitLabel(_ label: SubmitLabel) -> some View {
        environment(\.submitLabel, label)
    }

    func keyboardType(_ type: UIKeyboardType) -> some View {
        environment(\.textInputKeyboardType, type)
    }
    func textInputAutocapitalization(_ style: TextInputAutocapitalization?) -> some View {
        environment(\.textInputAutocapitalization, style)
    }
    func textContentType(_ type: UITextContentType?) -> some View {
        environment(\.textContentType, type)
    }
    func disableAutocorrection(_ disable: Bool? = true) -> some View {
        environment(\.autocorrectionDisabled, disable)
    }
    func autocorrectionDisabled(_ disabled: Bool = true) -> some View {
        disableAutocorrection(disabled)
    }

    func searchable(text: Binding<String>) -> some View {
        _SearchableModifier(content: AnyView(self), text: text)
    }
    func refreshable(action: @escaping () async -> Void) -> some View {
        _RefreshableModifier(content: AnyView(self), action: action, actionScopePath: _UIRuntime._currentPath ?? [])
    }

    func disabled(_ disabled: Bool) -> some View {
        _DisabledModifier(content: AnyView(self), disabled: disabled)
    }

    func hidden() -> some View {
        _Hidden(content: AnyView(self))
    }

    func quickLookPreview(_ url: Binding<URL?>) -> some View {
        _QuickLookPreviewModifier(content: AnyView(self), url: url)
    }

    func onHover(perform action: @escaping (Bool) -> Void) -> some View {
        _HoverModifier(content: AnyView(self), action: action, actionScopePath: _UIRuntime._currentPath ?? [])
    }

    func frame(
        width: CGFloat? = nil,
        height: CGFloat? = nil,
        alignment: Alignment = .center
    ) -> some View {
        _Frame(
            content: AnyView(self),
            width: width,
            height: height,
            minWidth: nil,
            maxWidth: nil,
            minHeight: nil,
            maxHeight: nil
        )
    }

    func frame(
        minWidth: CGFloat? = nil,
        idealWidth: CGFloat? = nil,
        maxWidth: CGFloat? = nil,
        minHeight: CGFloat? = nil,
        idealHeight: CGFloat? = nil,
        maxHeight: CGFloat? = nil,
        alignment: Alignment = .center
    ) -> some View {
        _Frame(
            content: AnyView(self),
            width: idealWidth,
            height: idealHeight,
            minWidth: minWidth,
            maxWidth: maxWidth,
            minHeight: minHeight,
            maxHeight: maxHeight
        )
    }

    func padding(_ edges: Edge.Set = .all, _ length: CGFloat? = nil) -> some View {
        // Negative padding is used in some upstream SwiftUI views for fine visual tuning.
        // OmniUI's box model doesn't support negative extents safely, so clamp to zero.
        let amt = max(0, Int(length ?? 1))
        let t = edges.contains(.top) || edges == .all || edges.contains(.vertical)
        let b = edges.contains(.bottom) || edges == .all || edges.contains(.vertical)
        let l = edges.contains(.leading) || edges == .all || edges.contains(.horizontal)
        let r = edges.contains(.trailing) || edges == .all || edges.contains(.horizontal)
        return AnyView(_EdgePadding(
            content: AnyView(self),
            top: t ? amt : 0,
            leading: l ? amt : 0,
            bottom: b ? amt : 0,
            trailing: r ? amt : 0
        ))
    }

    func padding(_ length: CGFloat) -> some View {
        padding(.all, length)
    }

    func onAppear(perform action: @escaping () -> Void) -> some View {
        _OnAppear(content: AnyView(self), action: action)
    }

    func onDisappear(perform action: @escaping () -> Void) -> some View {
        _OnDisappear(content: AnyView(self), action: action)
    }

    func safeAreaInset<Content: View>(edge: Edge, @ViewBuilder content: () -> Content) -> some View {
        _SafeAreaInset(base: AnyView(self), edge: edge, inset: AnyView(content()))
    }

    func presentationDetents(_ detents: Set<PresentationDetent>) -> some View {
        environment(\.presentationDetents, detents)
    }

    func presentationDragIndicator(_ visibility: Visibility) -> some View {
        environment(\.presentationDragIndicatorVisibility, visibility)
    }

    func navigationSplitViewColumnWidth(min: CGFloat? = nil, ideal: CGFloat, max maxWidth: CGFloat? = nil) -> some View {
        let fallback = ideal > 0 ? ideal : (min ?? maxWidth ?? 0)
        let columns = Swift.max(1, Int((fallback / 8).rounded()))
        return frame(width: CGFloat(columns))
    }

    func scaleEffect(_ scale: CGFloat, anchor: UnitPoint = .center) -> some View {
        _ = anchor
        if scale > 1 {
            return AnyView(bold())
        }
        return AnyView(self)
    }

    func scaleEffect(x: CGFloat = 1, y: CGFloat = 1, anchor: UnitPoint = .center) -> some View {
        _ = anchor
        return scaleEffect(max(x, y))
    }

    func previewDisplayName(_ name: String) -> some View {
        _PreviewDisplayNameModifier(content: AnyView(self), name: name)
    }
}

private struct _Hidden: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        // Still build the subtree so it can register actions/shortcuts, but don't render it.
        _ = ctx.buildChild(content)
        return .empty
    }
}

public enum SubmitLabel: Hashable, Sendable {
    case done
    case go
    case search
    case send
    case next
    case `return`
}

private struct _Sheet: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let isPresented: Binding<Bool>
    let onDismiss: (() -> Void)?
    let sheet: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let env = _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment
        if isPresented.wrappedValue {
            let dismiss = {
                if isPresented.wrappedValue {
                    isPresented.wrappedValue = false
                    onDismiss?()
                }
            }
            ctx.runtime._registerOverlay(view: AnyView(
                ZStack {
                    // Dim background.
                    Color.gray.opacity(0.35)
                    _presentationChrome(title: "Sheet", dismiss: dismiss, env: _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment) {
                        sheet
                    }
                }
            ), dismiss: dismiss)
        }
        return ctx.buildChild(content)
    }
}


private struct _PresentationOverlay: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let isPresented: Binding<Bool>
    let onDismiss: (() -> Void)?
    let title: String
    let showsScrim: Bool
    let presented: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let env = _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment
        if isPresented.wrappedValue {
            let dismiss = {
                if isPresented.wrappedValue {
                    isPresented.wrappedValue = false
                    onDismiss?()
                }
            }
            ctx.runtime._registerOverlay(view: AnyView(
                ZStack {
                    if showsScrim {
                        Color.gray.opacity(0.35)
                    }
                    _presentationChrome(title: title, dismiss: dismiss, env: env) {
                        presented
                    }
                }
            ), dismiss: dismiss)
        }
        return ctx.buildChild(content)
    }
}

private struct _ItemPresentationOverlay<Item: Identifiable>: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let item: Binding<Item?>
    let onDismiss: (() -> Void)?
    let title: String
    let showsScrim: Bool
    let presented: (Item) -> AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let env = _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment
        if let current = item.wrappedValue {
            let dismiss = {
                if item.wrappedValue != nil {
                    item.wrappedValue = nil
                    onDismiss?()
                }
            }
            ctx.runtime._registerOverlay(view: AnyView(
                ZStack {
                    if showsScrim {
                        Color.gray.opacity(0.35)
                    }
                    _presentationChrome(title: title, dismiss: dismiss, env: env) {
                        presented(current)
                    }
                }
            ), dismiss: dismiss)
        }
        return ctx.buildChild(content)
    }
}

private struct _ConfirmationDialog: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let title: String
    let isPresented: Binding<Bool>
    let titleVisibility: Visibility
    let actions: AnyView
    let message: AnyView?

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let env = _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment
        if isPresented.wrappedValue {
            let runtime = ctx.runtime
            let captureID = runtime._beginMenuCapture()
            _UIRuntime.$_currentMenuCaptureID.withValue(captureID) {
                var actionsCtx = _BuildContext(runtime: runtime, path: ctx.path + [91_000], nextChildIndex: 0)
                _ = _BuildContext.withRuntime(runtime, path: actionsCtx.path) {
                    OmniUICore._makeNode(actions, &actionsCtx)
                }
            }
            let captured = runtime._endMenuCapture(captureID)
            let dismiss = {
                if isPresented.wrappedValue {
                    isPresented.wrappedValue = false
                }
            }
            let shouldShowTitle = titleVisibility != .hidden && !title.isEmpty
            let messageView = message ?? AnyView(EmptyView())
            ctx.runtime._registerOverlay(view: AnyView(
                ZStack {
                    Color.gray.opacity(0.35)
                    _presentationChrome(title: shouldShowTitle ? title : "", dismiss: dismiss, env: env) {
                        VStack(spacing: 1) {
                            messageView
                            if captured.isEmpty {
                                Button("OK") { dismiss() }
                            } else {
                                ForEach(0..<captured.count, id: \.self) { idx in
                                    let entry = captured[idx]
                                    Button(entry.label) {
                                        dismiss()
                                        runtime._invokeCapturedMenuItem(entry)
                                    }
                                }
                            }
                        }
                    }
                }
            ), dismiss: dismiss)
        }
        return ctx.buildChild(content)
    }
}


private struct _NavigationDestinationResolver<Value: Hashable>: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let valueType: Value.Type
    let destination: (Value) -> AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        if let stackPath = ctx.runtime._nearestNavStackRoot(from: ctx.path) {
            ctx.runtime._registerNavDestinationResolver(stackPath: stackPath, valueType: valueType, destination: destination)
        }
        return ctx.buildChild(content)
    }
}

private struct _NavigationDestinationBool: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let isPresented: Binding<Bool>
    let destination: AnyView
    let ownerKeyOverride: String?

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        guard let stackPath = ctx.runtime._nearestNavStackRoot(from: ctx.path) else {
            return ctx.buildChild(content)
        }
        let ownerKey = ownerKeyOverride ?? ctx.runtime._viewPathKey(path: ctx.path)
        let isActive = ctx.runtime._navContainsOwner(stackPath: stackPath, ownerKey: ownerKey)

        if isPresented.wrappedValue {
            if !isActive {
                ctx.runtime._navPush(
                    stackPath: stackPath,
                    view: destination,
                    ownerKey: ownerKey,
                    onPop: {
                        if isPresented.wrappedValue {
                            isPresented.wrappedValue = false
                        }
                    }
                )
            }
        } else if isActive {
            ctx.runtime._navRemoveOwned(stackPath: stackPath, ownerKey: ownerKey)
        }
        return ctx.buildChild(content)
    }
}

private struct _NavigationDestinationItem<Item: Identifiable>: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let item: Binding<Item?>
    let destination: (Item) -> AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let isPresented = Binding<Bool>(
            get: { item.wrappedValue != nil },
            set: { presented in
                if !presented {
                    item.wrappedValue = nil
                }
            }
        )
        let ownerKey = ctx.runtime._viewPathKey(path: ctx.path)
        let target = item.wrappedValue.map(destination) ?? AnyView(EmptyView())
        return ctx.buildChild(
            _NavigationDestinationBool(content: content, isPresented: isPresented, destination: target, ownerKeyOverride: ownerKey)
        )
    }
}

private struct _Alert: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let isPresented: Binding<Bool>
    let alert: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        if isPresented.wrappedValue {
            let dismiss = {
                if isPresented.wrappedValue {
                    isPresented.wrappedValue = false
                }
            }
            ctx.runtime._registerOverlay(view: AnyView(
                ZStack {
                    Color.gray.opacity(0.35)
                    VStack(spacing: 1) {
                        alert
                        HStack(spacing: 1) {
                            Spacer()
                            Button("OK") { dismiss() }
                        }
                    }
                    .padding(1)
                }
            ), dismiss: dismiss)
        }
        return ctx.buildChild(content)
    }
}

public struct Alert {
    public struct Button {
        enum Kind {
            case `default`
            case cancel
            case destructive
        }

        let kind: Kind
        let label: Text
        let action: (() -> Void)?

        private init(kind: Kind, label: Text, action: (() -> Void)?) {
            self.kind = kind
            self.label = label
            self.action = action
        }

        public static func `default`(_ label: Text, action: (() -> Void)? = nil) -> Button {
            Button(kind: .default, label: label, action: action)
        }

        public static func cancel(_ label: Text, action: (() -> Void)? = nil) -> Button {
            Button(kind: .cancel, label: label, action: action)
        }

        public static func destructive(_ label: Text, action: (() -> Void)? = nil) -> Button {
            Button(kind: .destructive, label: label, action: action)
        }
    }

    public let title: Text
    public let message: Text?
    public let dismissButton: Button?

    public init(title: Text, message: Text? = nil, dismissButton: Button? = nil) {
        self.title = title
        self.message = message
        self.dismissButton = dismissButton
    }
}

private struct _AlertFromType: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let isPresented: Binding<Bool>
    let makeAlert: () -> Alert

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        if isPresented.wrappedValue {
            let alert = makeAlert()

            let dismiss = {
                if isPresented.wrappedValue {
                    isPresented.wrappedValue = false
                }
            }

            let button = alert.dismissButton ?? .default(Text("OK"), action: nil)
            let messageView: AnyView = alert.message.map(AnyView.init) ?? AnyView(EmptyView())
            ctx.runtime._registerOverlay(view: AnyView(
                ZStack {
                    Color.gray.opacity(0.35)
                    VStack(spacing: 1) {
                        VStack(spacing: 1) {
                            alert.title
                            messageView
                        }
                        HStack(spacing: 1) {
                            Spacer()
                            Button(button.label.content) {
                                dismiss()
                                button.action?()
                            }
                        }
                    }
                    .padding(1)
                }
            ), dismiss: dismiss)
        }
        return ctx.buildChild(content)
    }
}

private func _presentationChrome<Content: View>(title: String, dismiss: @escaping () -> Void, env: EnvironmentValues, @ViewBuilder content: () -> Content) -> some View {
    VStack(spacing: 1) {
        if env.presentationDragIndicatorVisibility != .hidden {
            Text("──")
                .foregroundStyle(.secondary)
        }
        HStack(spacing: 1) {
            if !title.isEmpty {
                Text(title)
            }
            Spacer()
            Button("Close") { dismiss() }
        }
        content()
    }
    .padding(1)
    .background(_presentationBackgroundColor(env: env))
}

private func _presentationBackgroundColor(env: EnvironmentValues) -> Color {
    if env.presentationDetents.contains(.large) {
        return Color.gray.opacity(0.18)
    }
    if env.presentationDetents.contains(.medium) {
        return Color.gray.opacity(0.12)
    }
    return Color.gray.opacity(0.10)
}

private struct _TapGesture: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let count: Int
    let action: () -> Void
    let actionScopePath: [Int]

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _ = count // multi-tap not modeled; retained for call-site compatibility.
        guard _UIRuntime._hitTestingEnabled else {
            return ctx.buildChild(content)
        }

        let runtime = ctx.runtime
        let controlPath = ctx.path
        let id = runtime._registerAction({
            runtime._setFocus(path: controlPath)
            action()
        }, path: actionScopePath)
        runtime._registerFocusable(path: controlPath, activate: id)
        return .tapTarget(id: id, child: ctx.buildChild(content))
    }
}

private struct _AllowsHitTesting: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let enabled: Bool

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _UIRuntime.$_hitTestingEnabled.withValue(enabled) {
            ctx.buildChild(content)
        }
    }
}

private struct _SafeAreaInset: View, _PrimitiveView {
    typealias Body = Never

    let base: AnyView
    let edge: Edge
    let inset: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        switch edge {
        case .top:
            return ctx.buildChild(VStack(spacing: 0) { inset; base })
        case .bottom:
            return ctx.buildChild(VStack(spacing: 0) { base; inset })
        case .leading:
            return ctx.buildChild(HStack(spacing: 0) { inset; base })
        case .trailing:
            return ctx.buildChild(HStack(spacing: 0) { base; inset })
        }
    }
}

private struct _ToolbarModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let toolbar: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let baseNode = ctx.buildChild(content)
        let toolbarNode = ctx.buildChild(toolbar)
        var items = _collectToolbarItems(from: toolbarNode)
        let env = _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment

        if env.toolbarRemovedPlacements.contains(.sidebarToggle) {
            items.leading.removeAll()
        }

        if items.principal.isEmpty, let title = env.navigationTitle, !title.isEmpty {
            let titleNode: _VNode = env.navigationTitleDisplayMode == .large
                ? .textStyled(style: .bold, child: .text(title))
                : .text(title)
            items.principal.append(titleNode)
        }

        guard env.toolbarVisibility != .hidden, !items.isEmpty else { return baseNode }

        var root: [_VNode] = []
        if let top = _toolbarTopBar(items: items, env: env) {
            root.append(top)
        }
        root.append(baseNode)
        if let bottom = _toolbarBottomBar(items: items, env: env) {
            root.append(bottom)
        }
        return .stack(axis: .vertical, spacing: 0, children: root)
    }
}

private func _toolbarTopBar(items: _ToolbarLayoutItems, env: EnvironmentValues) -> _VNode? {
    if items.leading.isEmpty && items.principal.isEmpty && items.trailing.isEmpty {
        return nil
    }

    var row: [_VNode] = []
    if !items.leading.isEmpty {
        row.append(.stack(axis: .horizontal, spacing: 1, children: items.leading))
    }
    if !items.principal.isEmpty {
        if !row.isEmpty { row.append(.spacer) }
        row.append(.stack(axis: .horizontal, spacing: 1, children: items.principal))
        row.append(.spacer)
    } else if !items.leading.isEmpty && !items.trailing.isEmpty {
        row.append(.spacer)
    }
    if !items.trailing.isEmpty {
        row.append(.stack(axis: .horizontal, spacing: 1, children: items.trailing))
    }

    let bar = _VNode.stack(axis: .vertical, spacing: 0, children: [
        .stack(axis: .horizontal, spacing: 1, children: row),
        .divider,
    ])
    return _applyToolbarBackground(to: bar, env: env)
}

private func _toolbarBottomBar(items: _ToolbarLayoutItems, env: EnvironmentValues) -> _VNode? {
    guard !items.bottom.isEmpty else { return nil }
    let bar = _VNode.stack(axis: .vertical, spacing: 0, children: [
        .divider,
        .stack(axis: .horizontal, spacing: 1, children: items.bottom),
    ])
    return _applyToolbarBackground(to: bar, env: env)
}

private func _applyToolbarBackground(to node: _VNode, env: EnvironmentValues) -> _VNode {
    let style = env.toolbarBackgroundStyle
    guard style.visibility != .hidden else { return node }
    if let color = style.color {
        return .background(child: node, background: .style(fg: nil, bg: color, child: .empty))
    }
    if let material = style.material {
        let backgroundColor: Color
        switch material.raw {
        case Material.bar.raw:
            backgroundColor = Color.gray.opacity(0.18)
        case Material.background.raw:
            backgroundColor = Color.gray.opacity(0.10)
        case Material.ultraThinMaterial.raw:
            backgroundColor = Color.gray.opacity(0.14)
        default:
            backgroundColor = Color.gray.opacity(0.22)
        }
        return .background(child: node, background: .style(fg: nil, bg: backgroundColor, child: .empty))
    }
    if style.visibility == .visible {
        return .background(child: node, background: .style(fg: nil, bg: Color.gray.opacity(0.10), child: .empty))
    }
    return node
}

private struct _Frame: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let width: CGFloat?
    let height: CGFloat?
    let minWidth: CGFloat?
    let maxWidth: CGFloat?
    let minHeight: CGFloat?
    let maxHeight: CGFloat?

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        func toInt(_ v: CGFloat?) -> Int? {
            guard let v else { return nil }
            if v.isInfinite { return Int.max }
            return max(0, Int(v.rounded()))
        }
        return .frame(
            width: toInt(width),
            height: toInt(height),
            minWidth: toInt(minWidth),
            maxWidth: toInt(maxWidth),
            minHeight: toInt(minHeight),
            maxHeight: toInt(maxHeight),
            child: ctx.buildChild(content)
        )
    }
}

private struct _EdgePadding: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let top: Int
    let leading: Int
    let bottom: Int
    let trailing: Int

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .edgePadding(top: top, leading: leading, bottom: bottom, trailing: trailing, child: ctx.buildChild(content))
    }
}

private struct _FocusBoolBinder: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let get: () -> Bool
    let set: (Bool) -> Void

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let captureID = runtime._beginFocusCapture()
        let node = _UIRuntime.$_currentFocusCaptureID.withValue(captureID) {
            ctx.buildChild(content)
        }
        guard let focusPath = runtime._endFocusCapture(captureID) else {
            return node
        }

        runtime._registerFocusBoolBinding(path: focusPath, set: set)

        let wantsFocus = get()
        let isFocused = runtime._isFocused(path: focusPath)
        if wantsFocus && !isFocused {
            runtime._setFocus(path: focusPath)
        } else if !wantsFocus && isFocused {
            runtime._setFocus(path: nil)
        }

        return node
    }
}

private struct _FocusBoolValueBinder: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let isFocused: Bool

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let captureID = runtime._beginFocusCapture()
        let node = _UIRuntime.$_currentFocusCaptureID.withValue(captureID) {
            ctx.buildChild(content)
        }
        guard let focusPath = runtime._endFocusCapture(captureID) else {
            return node
        }

        let currentlyFocused = runtime._isFocused(path: focusPath)
        if isFocused && !currentlyFocused {
            runtime._setFocus(path: focusPath)
        } else if !isFocused && currentlyFocused {
            runtime._setFocus(path: nil)
        }

        return node
    }
}

private struct _OnSubmitBinder: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let action: () -> Void
    let actionScopePath: [Int]

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let captureID = runtime._beginFocusCapture()
        let node = _UIRuntime.$_currentFocusCaptureID.withValue(captureID) {
            ctx.buildChild(content)
        }
        if let focusPath = runtime._endFocusCapture(captureID) {
            runtime._registerSubmitHandler(controlPath: focusPath, actionScopePath: actionScopePath, action: action)
        }
        return node
    }
}

private struct _Style: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let fg: Color?
    let bg: Color?

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .style(fg: fg, bg: bg, child: ctx.buildChild(content))
    }
}

private struct _TextStyleModifier: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let style: TextStyle

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .textStyled(style: style, child: ctx.buildChild(content))
    }
}

private struct _Shadow: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let color: Color
    let radius: CGFloat
    let x: CGFloat
    let y: CGFloat

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .shadow(
            child: ctx.buildChild(content),
            color: color,
            radius: max(0, Int(radius.rounded())),
            x: Int(x.rounded()),
            y: Int(y.rounded())
        )
    }
}

private struct _Clip: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let kind: _ShapeKind

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .clip(kind: kind, child: ctx.buildChild(content))
    }
}

private struct _ContentShapeRect: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .contentShapeRect(child: ctx.buildChild(content))
    }
}

private struct _Background: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let background: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .background(child: ctx.buildChild(content), background: ctx.buildChild(background))
    }
}

private struct _Overlay: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let overlay: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .overlay(child: ctx.buildChild(content), overlay: ctx.buildChild(overlay))
    }
}

private struct _LabelsHidden: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let hidden: Bool

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _UIRuntime.$_labelsHidden.withValue(hidden) {
            ctx.buildChild(content)
        }
    }
}

private struct _KeyboardShortcutBinder: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let shortcut: KeyboardShortcut

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let captureID = runtime._beginFocusCapture()
        let node = _UIRuntime.$_currentFocusCaptureID.withValue(captureID) {
            ctx.buildChild(content)
        }
        if let focusPath = runtime._endFocusCapture(captureID) {
            runtime._registerKeyboardShortcut(shortcut, forFocusablePath: focusPath)
        }
        return node
    }
}

// MARK: Keyboard shortcuts

public struct KeyboardShortcut: Hashable, Sendable {
    public var key: KeyEquivalent
    public var modifiers: EventModifiers
    public init(_ key: KeyEquivalent, modifiers: EventModifiers = []) {
        self.key = key
        self.modifiers = modifiers
    }

    public static let cancelAction = KeyboardShortcut(.escape)
    public static let defaultAction = KeyboardShortcut(.return)
}

public struct KeyEquivalent: Hashable, Sendable, ExpressibleByStringLiteral {
    public var rawValue: String
    public init(stringLiteral value: StringLiteralType) { self.rawValue = value }

    public static let escape: KeyEquivalent = "\u{001B}"
    public static let `return`: KeyEquivalent = "\n"
}

public struct EventModifiers: OptionSet, Hashable, Sendable {
    public let rawValue: Int
    public init(rawValue: Int) { self.rawValue = rawValue }
    public static let command = EventModifiers(rawValue: 1 << 0)
    public static let shift = EventModifiers(rawValue: 1 << 1)
    public static let option = EventModifiers(rawValue: 1 << 2)
    public static let control = EventModifiers(rawValue: 1 << 3)
}

// MARK: Tagged options (Picker)
public extension View {
    func tag<V: Hashable>(_ value: V) -> some View {
        _Tag(content: AnyView(self), value: AnyHashable(value))
    }
}

private struct _Tag: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let value: AnyHashable

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .tagged(value: value, label: ctx.buildChild(content))
    }
}

private struct _Identified: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let id: AnyHashable
    let readerScopePath: [Int]?

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .identified(id: id, readerScopePath: readerScopePath, child: ctx.buildChild(content))
    }
}

private struct _OnDelete: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let action: (IndexSet) -> Void
    let actionScopePath: [Int]

    @Environment(\.editMode) private var editMode

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let child = ctx.buildChild(content)
        let wrapped = _VNode.onDelete(actionScopePath: actionScopePath, action: action, child: child)
        guard editMode?.wrappedValue.isEditing == true else {
            return wrapped
        }
        return _attachDeleteControls(node: wrapped, runtime: ctx.runtime)
    }

    private func _attachDeleteControls(node: _VNode, runtime: _UIRuntime) -> _VNode {
        let rows = _collectDeleteRows(node)
        guard !rows.isEmpty else { return node }

        var entries: [_VNode] = []
        entries.reserveCapacity(rows.count)

        for (index, row) in rows.enumerated() {
            let deletePath = (_UIRuntime._currentPath ?? []) + [70_000 + index]
            let deleteID = runtime._registerAction({
                action(IndexSet(integer: index))
            }, path: actionScopePath)
            runtime._registerFocusable(path: deletePath, activate: deleteID)

            let deleteButton = _VNode.button(
                id: deleteID,
                isFocused: runtime._isFocused(path: deletePath),
                label: .text("Del")
            )

            entries.append(.stack(axis: .horizontal, spacing: 1, children: [deleteButton, row]))
        }

        return .group(entries)
    }

    private func _collectDeleteRows(_ node: _VNode) -> [_VNode] {
        switch node {
        case .empty:
            return []
        case .group(let children):
            return children.flatMap(_collectDeleteRows)
        case .onDelete(_, _, let child):
            return _collectDeleteRows(child)
        default:
            return [node]
        }
    }
}

private struct _ModelContainerProvider: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let container: ModelContainer

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let current = _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment
        var next = current
        next.modelContext = container.mainContext
        return _UIRuntime.$_currentEnvironment.withValue(next) {
            ctx.buildChild(content)
        }
    }
}

private struct _ModelContainerBuilder: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let modelTypes: [Any.Type]
    let inMemory: Bool

    @State private var container: ModelContainer? = nil

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        if container == nil {
            container = try? ModelContainer(for: modelTypes, inMemory: inMemory)
        }
        guard let container else {
            return ctx.buildChild(content)
        }

        let current = _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment
        var next = current
        next.modelContext = container.mainContext
        return _UIRuntime.$_currentEnvironment.withValue(next) {
            ctx.buildChild(content)
        }
    }
}

/// Used for API compatibility: modifiers that we don't yet model in the node tree.
private struct _TransitionModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let transition: AnyTransition

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let raw = transition.rawValue.lowercased()
        let base = ctx.buildChild(content)
        if raw.contains("move(") {
            if raw.contains("top") {
                return .offset(x: 0, y: -1, child: base)
            }
            if raw.contains("bottom") {
                return .offset(x: 0, y: 1, child: base)
            }
            if raw.contains("leading") {
                return .offset(x: -1, y: 0, child: base)
            }
            if raw.contains("trailing") {
                return .offset(x: 1, y: 0, child: base)
            }
        }
        if raw.contains("opacity") {
            return .opacity(0.85, child: base)
        }
        return base
    }
}

private struct _IgnoresSafeAreaModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.buildChild(content)
    }
}

private struct _GlassEffectModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let style: GlassEffect
    let shape: GlassEffectShape?

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _ = style
        let child = ctx.buildChild(content)
        let clipped: _VNode = {
            guard let shape else { return child }
            if shape.rawValue.contains("capsule") {
                return .clip(kind: .capsule, child: child)
            }
            if shape.rawValue.contains("cornerRadius"),
               let radiusStart = shape.rawValue.firstIndex(of: ":"),
               let radiusEnd = shape.rawValue.firstIndex(of: ")"),
               let radius = Int(shape.rawValue[shape.rawValue.index(after: radiusStart)..<radiusEnd]) {
                return .clip(kind: .roundedRectangle(cornerRadius: radius), child: child)
            }
            return .clip(kind: .rectangle, child: child)
        }()
        return .background(child: .shadow(child: clipped, color: .white.opacity(0.08), radius: 1, x: 0, y: 0), background: ctx.buildChild(Color.gray.opacity(0.16)))
    }
}

private struct _NavigationTransitionModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let transition: NavigationTransition

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _ = transition
        return .style(fg: (_UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment).tint ?? .accentColor, bg: nil, child: ctx.buildChild(content))
    }
}

private struct _ContentTransitionModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let transition: ContentTransition

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _ = transition
        return .textStyled(style: .italic, child: ctx.buildChild(content))
    }
}

private struct _HoverModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let action: (Bool) -> Void
    let actionScopePath: [Int]

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let id = ctx.runtime._registerHoverHandler(actionScopePath: ctx.path, action: action)
        return .hover(id: id, child: ctx.buildChild(content))
    }
}

private struct _PreviewDisplayNameModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let name: String

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        guard !name.isEmpty else { return ctx.buildChild(content) }
        return ctx.buildChild(
            VStack(alignment: .leading, spacing: 0) {
                Text(name).foregroundStyle(.secondary)
                content
            }
        )
    }
}

private struct _OpacityModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let opacity: CGFloat

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .opacity(opacity, child: ctx.buildChild(content))
    }
}

private struct _OffsetModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let x: CGFloat
    let y: CGFloat

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .offset(x: Int(x.rounded()), y: Int(y.rounded()), child: ctx.buildChild(content))
    }
}

private struct _MaskModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let mask: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let child = ctx.buildChild(content)
        let maskNode = ctx.buildChild(mask)
        let kind: _ShapeKind = {
            guard case .shape(let shape) = maskNode else { return .rectangle }
            return shape.kind
        }()
        return .clip(kind: kind, child: child)
    }
}

private struct _DisabledModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let disabled: Bool

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let current = _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment
        var next = current
        next.isEnabled = current.isEnabled && !disabled
        return _UIRuntime.$_currentEnvironment.withValue(next) {
            _UIRuntime.$_hitTestingEnabled.withValue(_UIRuntime._hitTestingEnabled && !disabled) {
                let node = ctx.buildChild(content)
                if disabled {
                    return _VNode.style(fg: .secondary, bg: nil, child: node)
                }
                return node
            }
        }
    }
}

private struct _SearchableModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let text: Binding<String>

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.buildChild(
            VStack(alignment: .leading, spacing: 1) {
                HStack(spacing: 1) {
                    Image(systemName: "magnifyingglass")
                    TextField("Search", text: text)
                        .submitLabel(.search)
                }
                content
            }
        )
    }
}

private struct _RefreshableModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let action: () async -> Void
    let actionScopePath: [Int]

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let actionPath = actionScopePath
        let refreshAction = action
        return ctx.buildChild(
            VStack(alignment: .leading, spacing: 1) {
                Button("Refresh") {
                    runtime._launchAsyncAction(path: actionPath, action: refreshAction)
                }
                .keyboardShortcut("r", modifiers: .control)
                content
            }
        )
    }
}

private struct _HelpModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let text: String

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        guard !text.isEmpty else { return ctx.buildChild(content) }
        return ctx.buildChild(
            VStack(alignment: .leading, spacing: 0) {
                content
                Text(text)
                    .foregroundStyle(.tertiary)
            }
        )
    }
}

private struct _OnExitCommandBinder: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let action: () -> Void
    let actionScopePath: [Int]

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.runtime._registerExitCommand(actionScopePath: actionScopePath, action: action)
        return ctx.buildChild(content)
    }
}

private struct _QuickLookPreviewModifier: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let url: Binding<URL?>

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        if let currentURL = url.wrappedValue {
            let dismiss = {
                if url.wrappedValue != nil {
                    url.wrappedValue = nil
                }
            }
            ctx.runtime._registerOverlay(view: AnyView(
                ZStack {
                    Color.gray.opacity(0.25)
                    VStack(spacing: 1) {
                        Text("Quick Look")
                            .bold()
                        Text(currentURL.absoluteString)
                        Button("Close") {
                            dismiss()
                        }
                    }
                    .padding(1)
                }
            ), dismiss: dismiss)
        }
        return ctx.buildChild(content)
    }
}

public struct _Passthrough: View, _PrimitiveView {
    public typealias Body = Never

    // Type-erase immediately to avoid exponential generic growth in large `body` expressions.
    let content: AnyView

    public init<V: View>(_ content: V) { self.content = AnyView(content) }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.buildChild(content)
    }
}

public struct _OnAppear: View, _PrimitiveView {
    public typealias Body = Never

    let content: AnyView
    let action: () -> Void

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let node = ctx.buildChild(content)
        ctx.runtime._registerOnAppear(path: ctx.path, action: action)
        return node
    }
}

public struct _OnDisappear: View, _PrimitiveView {
    public typealias Body = Never

    let content: AnyView
    let action: () -> Void

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.runtime._registerOnDisappear(path: ctx.path, action: action)
        return ctx.buildChild(content)
    }
}

public struct _OnChange<V: Equatable>: View, _PrimitiveView {
    public typealias Body = Never

    let content: AnyView
    let value: V
    let action: (_ oldValue: V, _ newValue: V) -> Void

    @State private var last: V? = nil

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        if let prev = last, prev != value {
            action(prev, value)
        }
        last = value
        return ctx.buildChild(content)
    }
}

public struct _OnChangeSimple<V: Equatable>: View, _PrimitiveView {
    public typealias Body = Never

    let content: AnyView
    let value: V
    let initial: Bool
    let action: () -> Void

    @State private var last: V? = nil
    @State private var firedInitial = false

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        if initial && !firedInitial {
            action()
            firedInitial = true
        } else if let prev = last, prev != value {
            action()
        }
        last = value
        return ctx.buildChild(content)
    }
}

public struct _OnChangeWithInitial<V: Equatable>: View, _PrimitiveView {
    public typealias Body = Never

    let content: AnyView
    let value: V
    let initial: Bool
    let action: (_ oldValue: V, _ newValue: V) -> Void

    @State private var last: V? = nil
    @State private var firedInitial = false

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        if initial && !firedInitial {
            action(value, value)
            firedInitial = true
        } else if let prev = last, prev != value {
            action(prev, value)
        }
        last = value
        return ctx.buildChild(content)
    }
}

private struct _TaskBinder: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let action: () async -> Void

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.runtime._registerTask(path: ctx.path, action: action)
        return ctx.buildChild(content)
    }
}
