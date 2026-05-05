// Minimal SwiftUI-like environment system.
//
// This is intentionally small: enough for `@Environment`, `@EnvironmentObject`,
// and `.environmentObject(_)` to compile and work in our debug/notcurses renderers
// without relying on Combine or Apple-only frameworks.

import Foundation

public protocol EnvironmentKey {
    associatedtype Value
    static var defaultValue: Value { get }
}

public struct EnvironmentValues: @unchecked Sendable {
    private var storage: [ObjectIdentifier: Any] = [:]

    public init() {}

    public subscript<K: EnvironmentKey>(_ key: K.Type) -> K.Value {
        get { (storage[ObjectIdentifier(key)] as? K.Value) ?? K.defaultValue }
        set { storage[ObjectIdentifier(key)] = newValue }
    }

    // EnvironmentObject storage.
    mutating func _setObject<T: AnyObject>(_ value: T, as type: T.Type = T.self) {
        storage[ObjectIdentifier(type)] = value
    }

    func _getObject<T: AnyObject>(_ type: T.Type = T.self) -> T? {
        storage[ObjectIdentifier(type)] as? T
    }
}

@propertyWrapper
public struct Environment<Value> {
    private let keyPath: KeyPath<EnvironmentValues, Value>

    public init(_ keyPath: KeyPath<EnvironmentValues, Value>) {
        self.keyPath = keyPath
    }

    public var wrappedValue: Value {
        let env = _UIRuntime._currentEnvironment ?? EnvironmentValues()
        return env[keyPath: keyPath]
    }
}

public enum ColorScheme: Sendable {
    case light
    case dark
}

public struct DismissAction: @unchecked Sendable {
    let _action: () -> Void
    public init(_ action: @escaping () -> Void = {}) { self._action = action }
    public func callAsFunction() { _action() }
}

public struct PresentationMode: @unchecked Sendable {
    let _dismiss: () -> Void
    public init(dismiss: @escaping () -> Void = {}) { self._dismiss = dismiss }
    public mutating func dismiss() { _dismiss() }
}

public struct OpenURLAction: @unchecked Sendable {
    public enum Result: Sendable {
        case handled
        case discarded
        case systemAction
    }

    let _open: (URL) -> Result
    public init(_ open: @escaping (URL) -> Result = { _ in .systemAction }) {
        self._open = open
    }

    @discardableResult
    public func callAsFunction(_ url: URL) -> Result {
        _open(url)
    }
}

public enum EditMode: Hashable, Sendable {
    case inactive
    case active
    case transient

    public var isEditing: Bool {
        switch self {
        case .inactive:
            return false
        case .active, .transient:
            return true
        }
    }
}

private enum _ColorSchemeKey: EnvironmentKey {
    static let defaultValue: ColorScheme = .dark
}

private enum _DismissKey: EnvironmentKey {
    static let defaultValue: DismissAction = DismissAction()
}

private enum _PresentationModeKey: EnvironmentKey {
    static let defaultValue: Binding<PresentationMode> = Binding(get: { PresentationMode() }, set: { _ in })
}

private enum _ModelContextKey: EnvironmentKey {
    static let defaultValue: ModelContext = ModelContext()
}

private enum _OpenURLKey: EnvironmentKey {
    static let defaultValue: OpenURLAction = OpenURLAction()
}

private enum _EditModeKey: EnvironmentKey {
    static let defaultValue: Binding<EditMode>? = nil
}

public enum _FormStyleKind: Hashable, Sendable {
    case automatic
    case grouped
}

public enum _ListStyleKind: Hashable, Sendable {
    case automatic
    case plain
    case sidebar
}

public enum _PickerStyleKind: Hashable, Sendable {
    case automatic
    case menu
    case segmented
}

public enum _ButtonStyleKind: Hashable, Sendable {
    case automatic
    case plain
    case bordered
    case borderedProminent
    case primaryFill
}

public enum _TextFieldStyleKind: Hashable, Sendable {
    case automatic
    case plain
    case roundedBorder
}

public enum _DatePickerStyleKind: Hashable, Sendable {
    case automatic
    case compact
    case graphical
    case field
    case stepperField
}

public enum _GaugeStyleKind: Hashable, Sendable {
    case automatic
    case `default`
}

public enum _ToggleStyleKind: Hashable, Sendable {
    case automatic
    case `switch`
}

public enum _LabelStyleKind: Hashable, Sendable {
    case automatic
    case iconOnly
}

public enum _NavigationViewStyleKind: Hashable, Sendable {
    case automatic
    case `default`
    case column
    case doubleColumn
}

public struct _ToolbarBackgroundStyle: Hashable, Sendable {
    public var visibility: Visibility
    public var color: Color?
    public var material: Material?

    public init(visibility: Visibility = .automatic, color: Color? = nil, material: Material? = nil) {
        self.visibility = visibility
        self.color = color
        self.material = material
    }
}

public struct _ListRowBackgroundValue: @unchecked Sendable {
    public let view: AnyView

    public init(_ view: AnyView) {
        self.view = view
    }
}

public enum _NavigationTitleDisplayModeKind: Hashable, Sendable {
    case automatic
    case inline
    case large
}

public struct _GlassEffectConfig: Hashable, Sendable {
    public var style: GlassEffect
    public var shape: GlassEffectShape?

    public init(style: GlassEffect = .regular, shape: GlassEffectShape? = nil) {
        self.style = style
        self.shape = shape
    }
}

private enum _TintColorKey: EnvironmentKey {
    static let defaultValue: Color? = nil
}

private enum _FormStyleKey: EnvironmentKey {
    static let defaultValue: _FormStyleKind = .automatic
}

private enum _ListStyleKey: EnvironmentKey {
    static let defaultValue: _ListStyleKind = .automatic
}

private enum _PickerStyleKey: EnvironmentKey {
    static let defaultValue: _PickerStyleKind = .automatic
}

private enum _ButtonStyleKey: EnvironmentKey {
    static let defaultValue: _ButtonStyleKind = .automatic
}

private enum _TextFieldStyleKey: EnvironmentKey {
    static let defaultValue: _TextFieldStyleKind = .automatic
}

private enum _DatePickerStyleKey: EnvironmentKey {
    static let defaultValue: _DatePickerStyleKind = .automatic
}

private enum _GaugeStyleKey: EnvironmentKey {
    static let defaultValue: _GaugeStyleKind = .automatic
}

private enum _ToggleStyleKey: EnvironmentKey {
    static let defaultValue: _ToggleStyleKind = .automatic
}

private enum _LabelStyleKey: EnvironmentKey {
    static let defaultValue: _LabelStyleKind = .automatic
}

private enum _NavigationViewStyleKey: EnvironmentKey {
    static let defaultValue: _NavigationViewStyleKind = .automatic
}

private enum _TextSelectionEnabledKey: EnvironmentKey {
    static let defaultValue: Bool = false
}

private enum _NavigationTitleDisplayModeKey: EnvironmentKey {
    static let defaultValue: _NavigationTitleDisplayModeKind = .automatic
}

private enum _ScrollContentBackgroundKey: EnvironmentKey {
    static let defaultValue: Visibility = .automatic
}

private enum _ListRowSeparatorKey: EnvironmentKey {
    static let defaultValue: Visibility = .visible
}

private enum _ListRowBackgroundKey: EnvironmentKey {
    static let defaultValue: _ListRowBackgroundValue? = nil
}

private enum _ToolbarRemovedPlacementsKey: EnvironmentKey {
    static let defaultValue: Set<ToolbarPlacement> = []
}

private enum _ToolbarVisibilityKey: EnvironmentKey {
    static let defaultValue: Visibility = .automatic
}

private enum _ToolbarBackgroundKey: EnvironmentKey {
    static let defaultValue: _ToolbarBackgroundStyle = _ToolbarBackgroundStyle()
}

private enum _PresentationDetentsKey: EnvironmentKey {
    static let defaultValue: Set<PresentationDetent> = []
}

private enum _PresentationDragIndicatorKey: EnvironmentKey {
    static let defaultValue: Visibility = .automatic
}


private enum _IsEnabledKey: EnvironmentKey {
    static let defaultValue: Bool = true
}

private enum _ControlSizeKey: EnvironmentKey {
    static let defaultValue: ControlSize = .regular
}

private enum _SubmitLabelKey: EnvironmentKey {
    static let defaultValue: SubmitLabel = .return
}

private enum _FontKey: EnvironmentKey {
    static let defaultValue: Font? = nil
}

private enum _MultilineTextAlignmentKey: EnvironmentKey {
    static let defaultValue: TextAlignment = .leading
}

private enum _LineLimitKey: EnvironmentKey {
    static let defaultValue: Int? = nil
}

private enum _MonospacedDigitsKey: EnvironmentKey {
    static let defaultValue: Bool = false
}

private enum _TextInputKeyboardTypeKey: EnvironmentKey {
    static let defaultValue: UIKeyboardType = .default
}

private enum _TextInputAutocapitalizationKey: EnvironmentKey {
    static let defaultValue: TextInputAutocapitalization? = nil
}

private enum _TextContentTypeKey: EnvironmentKey {
    static let defaultValue: UITextContentType? = nil
}

private enum _AutocorrectionDisabledKey: EnvironmentKey {
    static let defaultValue: Bool? = nil
}

private enum _GlassEffectConfigKey: EnvironmentKey {
    static let defaultValue: _GlassEffectConfig? = nil
}

private enum _NavigationTransitionEnabledKey: EnvironmentKey {
    static let defaultValue: Bool = false
}

private enum _ContentTransitionEnabledKey: EnvironmentKey {
    static let defaultValue: Bool = false
}

private enum _AnyTransitionKey: EnvironmentKey {
    static let defaultValue: AnyTransition? = nil
}

private enum _IgnoredSafeAreaKey: EnvironmentKey {
    static let defaultValue: Bool = true
}

private enum _PreviewDisplayNameKey: EnvironmentKey {
    static let defaultValue: String? = nil
}

private enum _NavigationTitleKey: EnvironmentKey {
    static let defaultValue: String? = nil
}

public extension EnvironmentValues {
    var colorScheme: ColorScheme {
        get { self[_ColorSchemeKey.self] }
        set { self[_ColorSchemeKey.self] = newValue }
    }

    var dismiss: DismissAction {
        get { self[_DismissKey.self] }
        set { self[_DismissKey.self] = newValue }
    }

    var presentationMode: Binding<PresentationMode> {
        get { self[_PresentationModeKey.self] }
        set { self[_PresentationModeKey.self] = newValue }
    }

    var modelContext: ModelContext {
        get { self[_ModelContextKey.self] }
        set { self[_ModelContextKey.self] = newValue }
    }

    var openURL: OpenURLAction {
        get { self[_OpenURLKey.self] }
        set { self[_OpenURLKey.self] = newValue }
    }

    var editMode: Binding<EditMode>? {
        get { self[_EditModeKey.self] }
        set { self[_EditModeKey.self] = newValue }
    }

    var tint: Color? {
        get { self[_TintColorKey.self] }
        set { self[_TintColorKey.self] = newValue }
    }

    var formStyleKind: _FormStyleKind {
        get { self[_FormStyleKey.self] }
        set { self[_FormStyleKey.self] = newValue }
    }

    var listStyleKind: _ListStyleKind {
        get { self[_ListStyleKey.self] }
        set { self[_ListStyleKey.self] = newValue }
    }

    var pickerStyleKind: _PickerStyleKind {
        get { self[_PickerStyleKey.self] }
        set { self[_PickerStyleKey.self] = newValue }
    }

    var buttonStyleKind: _ButtonStyleKind {
        get { self[_ButtonStyleKey.self] }
        set { self[_ButtonStyleKey.self] = newValue }
    }

    var textFieldStyleKind: _TextFieldStyleKind {
        get { self[_TextFieldStyleKey.self] }
        set { self[_TextFieldStyleKey.self] = newValue }
    }

    var datePickerStyleKind: _DatePickerStyleKind {
        get { self[_DatePickerStyleKey.self] }
        set { self[_DatePickerStyleKey.self] = newValue }
    }

    var gaugeStyleKind: _GaugeStyleKind {
        get { self[_GaugeStyleKey.self] }
        set { self[_GaugeStyleKey.self] = newValue }
    }

    var toggleStyleKind: _ToggleStyleKind {
        get { self[_ToggleStyleKey.self] }
        set { self[_ToggleStyleKey.self] = newValue }
    }

    var labelStyleKind: _LabelStyleKind {
        get { self[_LabelStyleKey.self] }
        set { self[_LabelStyleKey.self] = newValue }
    }

    var navigationViewStyleKind: _NavigationViewStyleKind {
        get { self[_NavigationViewStyleKey.self] }
        set { self[_NavigationViewStyleKey.self] = newValue }
    }

    var textSelectionEnabled: Bool {
        get { self[_TextSelectionEnabledKey.self] }
        set { self[_TextSelectionEnabledKey.self] = newValue }
    }

    var navigationTitleDisplayMode: _NavigationTitleDisplayModeKind {
        get { self[_NavigationTitleDisplayModeKey.self] }
        set { self[_NavigationTitleDisplayModeKey.self] = newValue }
    }

    var scrollContentBackgroundVisibility: Visibility {
        get { self[_ScrollContentBackgroundKey.self] }
        set { self[_ScrollContentBackgroundKey.self] = newValue }
    }

    var listRowSeparatorVisibility: Visibility {
        get { self[_ListRowSeparatorKey.self] }
        set { self[_ListRowSeparatorKey.self] = newValue }
    }

    var listRowBackground: _ListRowBackgroundValue? {
        get { self[_ListRowBackgroundKey.self] }
        set { self[_ListRowBackgroundKey.self] = newValue }
    }

    var toolbarRemovedPlacements: Set<ToolbarPlacement> {
        get { self[_ToolbarRemovedPlacementsKey.self] }
        set { self[_ToolbarRemovedPlacementsKey.self] = newValue }
    }

    var toolbarVisibility: Visibility {
        get { self[_ToolbarVisibilityKey.self] }
        set { self[_ToolbarVisibilityKey.self] = newValue }
    }

    var toolbarBackgroundStyle: _ToolbarBackgroundStyle {
        get { self[_ToolbarBackgroundKey.self] }
        set { self[_ToolbarBackgroundKey.self] = newValue }
    }

    var presentationDetents: Set<PresentationDetent> {
        get { self[_PresentationDetentsKey.self] }
        set { self[_PresentationDetentsKey.self] = newValue }
    }

    var presentationDragIndicatorVisibility: Visibility {
        get { self[_PresentationDragIndicatorKey.self] }
        set { self[_PresentationDragIndicatorKey.self] = newValue }
    }

    var navigationTitle: String? {
        get { self[_NavigationTitleKey.self] }
        set { self[_NavigationTitleKey.self] = newValue }
    }

    var isEnabled: Bool {
        get { self[_IsEnabledKey.self] }
        set { self[_IsEnabledKey.self] = newValue }
    }

    var controlSize: ControlSize {
        get { self[_ControlSizeKey.self] }
        set { self[_ControlSizeKey.self] = newValue }
    }

    var submitLabel: SubmitLabel {
        get { self[_SubmitLabelKey.self] }
        set { self[_SubmitLabelKey.self] = newValue }
    }

    var font: Font? {
        get { self[_FontKey.self] }
        set { self[_FontKey.self] = newValue }
    }

    var multilineTextAlignment: TextAlignment {
        get { self[_MultilineTextAlignmentKey.self] }
        set { self[_MultilineTextAlignmentKey.self] = newValue }
    }

    var lineLimit: Int? {
        get { self[_LineLimitKey.self] }
        set { self[_LineLimitKey.self] = newValue }
    }

    var monospacedDigits: Bool {
        get { self[_MonospacedDigitsKey.self] }
        set { self[_MonospacedDigitsKey.self] = newValue }
    }

    var textInputKeyboardType: UIKeyboardType {
        get { self[_TextInputKeyboardTypeKey.self] }
        set { self[_TextInputKeyboardTypeKey.self] = newValue }
    }

    var textInputAutocapitalization: TextInputAutocapitalization? {
        get { self[_TextInputAutocapitalizationKey.self] }
        set { self[_TextInputAutocapitalizationKey.self] = newValue }
    }

    var textContentType: UITextContentType? {
        get { self[_TextContentTypeKey.self] }
        set { self[_TextContentTypeKey.self] = newValue }
    }

    var autocorrectionDisabled: Bool? {
        get { self[_AutocorrectionDisabledKey.self] }
        set { self[_AutocorrectionDisabledKey.self] = newValue }
    }

    var glassEffectConfig: _GlassEffectConfig? {
        get { self[_GlassEffectConfigKey.self] }
        set { self[_GlassEffectConfigKey.self] = newValue }
    }

    var navigationTransitionEnabled: Bool {
        get { self[_NavigationTransitionEnabledKey.self] }
        set { self[_NavigationTransitionEnabledKey.self] = newValue }
    }

    var contentTransitionEnabled: Bool {
        get { self[_ContentTransitionEnabledKey.self] }
        set { self[_ContentTransitionEnabledKey.self] = newValue }
    }

    var anyTransition: AnyTransition? {
        get { self[_AnyTransitionKey.self] }
        set { self[_AnyTransitionKey.self] = newValue }
    }

    var ignoredSafeArea: Bool {
        get { self[_IgnoredSafeAreaKey.self] }
        set { self[_IgnoredSafeAreaKey.self] = newValue }
    }

    var previewDisplayName: String? {
        get { self[_PreviewDisplayNameKey.self] }
        set { self[_PreviewDisplayNameKey.self] = newValue }
    }
}

public extension View {
    func environment<V>(_ keyPath: WritableKeyPath<EnvironmentValues, V>, _ value: V) -> some View {
        _EnvironmentValueProvider(content: AnyView(self), keyPath: keyPath, value: value)
    }

    func environmentObject<T: AnyObject>(_ object: T) -> some View {
        _EnvironmentObjectProvider(object: object, content: AnyView(self))
    }

    func onOpenURL(perform action: @escaping (URL) -> Void) -> some View {
        environment(\.openURL, OpenURLAction({ url in action(url); return .handled }))
    }
}

struct _EnvironmentValueProvider<V>: View, _PrimitiveView {
    public typealias Body = Never

    let content: AnyView
    let keyPath: WritableKeyPath<EnvironmentValues, V>
    let value: V

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let current = _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment
        var next = current
        next[keyPath: keyPath] = value
        return _UIRuntime.$_currentEnvironment.withValue(next) {
            ctx.buildChild(content)
        }
    }
}

struct _EnvironmentObjectProvider<T: AnyObject>: View, _PrimitiveView {
    public typealias Body = Never
    let object: T
    let content: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let current = _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment
        var next = current
        next._setObject(object)
        return _UIRuntime.$_currentEnvironment.withValue(next) {
            ctx.buildChild(content)
        }
    }
}
