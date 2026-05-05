import SwiftSyntax
import SwiftSyntaxBuilder
import SwiftSyntaxMacros

/// Lightweight fallback for Observation's `@Observable`.
///
/// We only synthesize `ObservableObject` conformance so OmniUI wrappers like `@Bindable`
/// and `@EnvironmentObject` can be used in environments without the Observation module.
public struct ObservableMacro: MemberMacro, ExtensionMacro {
    public static func expansion(
        of attribute: AttributeSyntax,
        providingMembersOf declaration: some DeclGroupSyntax,
        conformingTo protocols: [TypeSyntax],
        in context: some MacroExpansionContext
    ) throws -> [DeclSyntax] {
        _ = attribute
        _ = declaration
        _ = protocols
        _ = context
        return []
    }

    public static func expansion(
        of attribute: AttributeSyntax,
        attachedTo declaration: some DeclGroupSyntax,
        providingExtensionsOf type: some TypeSyntaxProtocol,
        conformingTo protocols: [TypeSyntax],
        in context: some MacroExpansionContext
    ) throws -> [ExtensionDeclSyntax] {
        _ = attribute
        _ = declaration
        _ = protocols
        _ = context

        let ext: DeclSyntax = "extension \(type.trimmed): OmniUICore.ObservableObject {}"
        return [ext.cast(ExtensionDeclSyntax.self)]
    }
}
