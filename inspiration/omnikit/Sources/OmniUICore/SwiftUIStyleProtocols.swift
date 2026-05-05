// SwiftUI-style protocols and configuration types.
//
// These are primarily compile-surface shims so third-party SwiftUI code can build.

// MARK: - ButtonStyle

public struct ButtonStyleConfiguration {
    public let label: AnyView
    public let isPressed: Bool

    public init(label: AnyView, isPressed: Bool) {
        self.label = label
        self.isPressed = isPressed
    }
}

public protocol ButtonStyle {
    associatedtype Body: View
    typealias Configuration = ButtonStyleConfiguration
    @ViewBuilder func makeBody(configuration: Configuration) -> Body
}

public struct PlainButtonStyle: ButtonStyle, Hashable, Sendable {
    public init() {}
    public func makeBody(configuration: Configuration) -> some View { configuration.label }
}

public struct BorderedButtonStyle: ButtonStyle, Hashable, Sendable {
    public init() {}
    public func makeBody(configuration: Configuration) -> some View { configuration.label }
}

public struct BorderedProminentButtonStyle: ButtonStyle, Hashable, Sendable {
    public init() {}
    public func makeBody(configuration: Configuration) -> some View { configuration.label }
}

/// Kept for source compatibility with code that uses this style name.
public struct PrimaryFillButtonStyle: ButtonStyle, Hashable, Sendable {
    public init() {}
    public func makeBody(configuration: Configuration) -> some View { configuration.label }
}

public extension ButtonStyle where Self == PlainButtonStyle {
    static var plain: PlainButtonStyle { PlainButtonStyle() }
}

public extension ButtonStyle where Self == BorderedButtonStyle {
    static var bordered: BorderedButtonStyle { BorderedButtonStyle() }
}

public extension ButtonStyle where Self == BorderedProminentButtonStyle {
    static var borderedProminent: BorderedProminentButtonStyle { BorderedProminentButtonStyle() }
}

// MARK: - ToggleStyle

public protocol ToggleStyle {}

public struct SwitchToggleStyle: ToggleStyle, Hashable, Sendable { public init() {} }

public extension ToggleStyle where Self == SwitchToggleStyle {
    static var `switch`: SwitchToggleStyle { SwitchToggleStyle() }
}

// MARK: - PickerStyle

public protocol PickerStyle {}

public struct SegmentedPickerStyle: PickerStyle, Hashable, Sendable { public init() {} }
public struct MenuPickerStyle: PickerStyle, Hashable, Sendable { public init() {} }

public extension PickerStyle where Self == SegmentedPickerStyle {
    static var segmented: SegmentedPickerStyle { SegmentedPickerStyle() }
}

public extension PickerStyle where Self == MenuPickerStyle {
    static var menu: MenuPickerStyle { MenuPickerStyle() }
}

// MARK: - TextFieldStyle

public protocol DatePickerStyle {}
public struct DefaultDatePickerStyle: DatePickerStyle, Hashable, Sendable { public init() {} }
public struct CompactDatePickerStyle: DatePickerStyle, Hashable, Sendable { public init() {} }
public struct GraphicalDatePickerStyle: DatePickerStyle, Hashable, Sendable { public init() {} }
public struct FieldDatePickerStyle: DatePickerStyle, Hashable, Sendable { public init() {} }
public struct StepperFieldDatePickerStyle: DatePickerStyle, Hashable, Sendable { public init() {} }
public extension DatePickerStyle where Self == DefaultDatePickerStyle {
    static var `default`: DefaultDatePickerStyle { DefaultDatePickerStyle() }
}
public extension DatePickerStyle where Self == CompactDatePickerStyle {
    static var compact: CompactDatePickerStyle { CompactDatePickerStyle() }
}
public extension DatePickerStyle where Self == GraphicalDatePickerStyle {
    static var graphical: GraphicalDatePickerStyle { GraphicalDatePickerStyle() }
}

public protocol GaugeStyle {}
public struct DefaultGaugeStyle: GaugeStyle, Hashable, Sendable { public init() {} }
public extension GaugeStyle where Self == DefaultGaugeStyle {
    static var `default`: DefaultGaugeStyle { DefaultGaugeStyle() }
}

public protocol TextFieldStyle {}

public struct RoundedBorderTextFieldStyle: TextFieldStyle, Hashable, Sendable { public init() {} }
public struct PlainTextFieldStyle: TextFieldStyle, Hashable, Sendable { public init() {} }

public extension TextFieldStyle where Self == PlainTextFieldStyle {
    static var plain: PlainTextFieldStyle { PlainTextFieldStyle() }
}

public extension TextFieldStyle where Self == RoundedBorderTextFieldStyle {
    static var roundedBorder: RoundedBorderTextFieldStyle { RoundedBorderTextFieldStyle() }
}

// MARK: - ListStyle

public protocol ListStyle {}

public struct PlainListStyle: ListStyle, Hashable, Sendable { public init() {} }
public struct SidebarListStyle: ListStyle, Hashable, Sendable { public init() {} }

public extension ListStyle where Self == PlainListStyle {
    static var plain: PlainListStyle { PlainListStyle() }
}

public extension ListStyle where Self == SidebarListStyle {
    static var sidebar: SidebarListStyle { SidebarListStyle() }
}

// MARK: - FormStyle

public protocol NavigationViewStyle {}
public struct DefaultNavigationViewStyle: NavigationViewStyle, Hashable, Sendable { public init() {} }
public struct ColumnNavigationViewStyle: NavigationViewStyle, Hashable, Sendable { public init() {} }
public struct DoubleColumnNavigationViewStyle: NavigationViewStyle, Hashable, Sendable { public init() {} }
public extension NavigationViewStyle where Self == DefaultNavigationViewStyle {
    static var `default`: DefaultNavigationViewStyle { DefaultNavigationViewStyle() }
}

public protocol FormStyle {}

public struct AutomaticFormStyle: FormStyle, Hashable, Sendable { public init() {} }
public struct GroupedFormStyle: FormStyle, Hashable, Sendable { public init() {} }

public extension FormStyle where Self == AutomaticFormStyle {
    static var automatic: AutomaticFormStyle { AutomaticFormStyle() }
}

public extension FormStyle where Self == GroupedFormStyle {
    static var grouped: GroupedFormStyle { GroupedFormStyle() }
}

// MARK: - LabelStyle

public struct LabelStyleConfiguration {
    public let title: AnyView
    public let icon: AnyView

    public init(title: AnyView, icon: AnyView) {
        self.title = title
        self.icon = icon
    }
}

public protocol LabelStyle {
    associatedtype Body: View
    typealias Configuration = LabelStyleConfiguration
    @ViewBuilder func makeBody(configuration: Configuration) -> Body
}

public struct IconOnlyLabelStyle: LabelStyle, Hashable, Sendable {
    public init() {}
    public func makeBody(configuration: Configuration) -> some View { configuration.icon }
}

public extension LabelStyle where Self == IconOnlyLabelStyle {
    static var iconOnly: IconOnlyLabelStyle { IconOnlyLabelStyle() }
}
