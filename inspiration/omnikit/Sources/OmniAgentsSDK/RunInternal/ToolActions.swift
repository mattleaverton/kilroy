import Foundation

public struct ToolAction: Sendable, Equatable {
    public var name: String
    public var callID: String

    public init(name: String, callID: String) {
        self.name = name
        self.callID = callID
    }
}

