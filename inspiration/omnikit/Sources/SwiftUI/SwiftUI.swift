@_exported import Foundation
@_exported import OmniUI

@freestanding(declaration, names: named(__OmniPreview))
public macro Preview(_ name: String? = nil, @ViewBuilder _ body: () -> AnyView) = #externalMacro(module: "SwiftUIMacros", type: "PreviewMacro")

@attached(member, names: arbitrary)
@attached(extension, conformances: OmniUICore.ObservableObject, names: arbitrary)
public macro Observable() = #externalMacro(module: "SwiftUIMacros", type: "ObservableMacro")
