import SwiftSyntax
import SwiftSyntaxMacros

/// Stub for SwiftUI's `#Preview` macro.
///
/// In Xcode this expands into preview provider declarations. For OmniKit's Linux/TUI
/// use case we just need `#Preview { ... }` blocks to parse and be ignored.
public struct PreviewMacro: DeclarationMacro {
    public static func expansion(
        of node: some FreestandingMacroExpansionSyntax,
        in context: some MacroExpansionContext
    ) throws -> [DeclSyntax] {
        []
    }
}
