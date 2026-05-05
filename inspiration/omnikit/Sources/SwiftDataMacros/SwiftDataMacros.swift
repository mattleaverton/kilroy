import SwiftCompilerPlugin
import SwiftSyntaxMacros

@main
struct SwiftDataMacrosPlugin: CompilerPlugin {
    let providingMacros: [Macro.Type] = [
        ModelMacro.self
    ]
}

