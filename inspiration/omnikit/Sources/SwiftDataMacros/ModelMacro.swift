import SwiftSyntax
import SwiftSyntaxBuilder
import SwiftSyntaxMacros

/// Stub implementation for SwiftData's `@Model` macro.
///
/// The real SwiftData macro synthesizes persistence + observation metadata. For OmniKit's
/// compatibility layer, we only need the attribute to be recognized by the compiler.
public struct ModelMacro: MemberMacro, ExtensionMacro {
    public static func expansion(
        of attribute: AttributeSyntax,
        providingMembersOf declaration: some DeclGroupSyntax,
        conformingTo protocols: [TypeSyntax],
        in context: some MacroExpansionContext
    ) throws -> [DeclSyntax] {
        []
    }

    public static func expansion(
        of attribute: AttributeSyntax,
        attachedTo declaration: some DeclGroupSyntax,
        providingExtensionsOf type: some TypeSyntaxProtocol,
        conformingTo protocols: [TypeSyntax],
        in context: some MacroExpansionContext
    ) throws -> [ExtensionDeclSyntax] {
        // SwiftData models are typically used with `ForEach(models) { ... }`, which relies on
        // `Identifiable` conformance. Most model types already declare an `id` property, so
        // adding this conformance provides a useful "drop-in" behavior for our shim.
        let ext: DeclSyntax = "extension \(type.trimmed): Identifiable {}"
        return [ext.cast(ExtensionDeclSyntax.self)]
    }
}
