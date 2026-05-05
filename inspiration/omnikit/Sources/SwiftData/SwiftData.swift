// Minimal SwiftData compatibility surface.
//
// This module intentionally does not attempt to implement SwiftData's persistence.
// It exists so `import SwiftData` and common call sites (e.g. `@Model`, `@Query`)
// compile when targeting non-Apple platforms.

@_exported import Foundation
import OmniUICore

@attached(member, names: arbitrary)
@attached(extension, conformances: Identifiable, names: arbitrary)
public macro Model() = #externalMacro(module: "SwiftDataMacros", type: "ModelMacro")

public typealias SortOrder = OmniUICore.SortOrder
public typealias Query = OmniUICore.Query

public typealias Schema = OmniUICore.Schema
public typealias ModelConfiguration = OmniUICore.ModelConfiguration
public typealias ModelContainer = OmniUICore.ModelContainer
public typealias ModelContext = OmniUICore.ModelContext
