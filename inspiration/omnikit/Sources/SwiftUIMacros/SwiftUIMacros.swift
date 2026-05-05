import SwiftCompilerPlugin
import SwiftSyntaxMacros

@main
struct SwiftUIMacrosPlugin: CompilerPlugin {
    let providingMacros: [Macro.Type] = [
        PreviewMacro.self,
        ObservableMacro.self
    ]
}
