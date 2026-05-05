import Foundation
import OmniACPModel

public protocol FileSystemDelegate: Sendable {
    func handleReadTextFile(_ request: FileSystemReadTextFile.Parameters) async throws -> FileSystemReadTextFile.Result
    func handleWriteTextFile(_ request: FileSystemWriteTextFile.Parameters) async throws -> FileSystemWriteTextFile.Result
}

public protocol TerminalDelegate: Sendable {
    func handleTerminalCreate(_ request: TerminalCreate.Parameters) async throws -> TerminalCreate.Result
    func handleTerminalOutput(_ request: TerminalOutput.Parameters) async throws -> TerminalOutput.Result
    func handleTerminalWaitForExit(_ request: TerminalWaitForExit.Parameters) async throws -> TerminalWaitForExit.Result
    func handleTerminalKill(_ request: TerminalKill.Parameters) async throws -> TerminalKill.Result
    func handleTerminalRelease(_ request: TerminalRelease.Parameters) async throws -> TerminalRelease.Result
}

public protocol PermissionDelegate: Sendable {
    func handlePermissionRequest(_ request: SessionRequestPermission.Parameters) async throws -> SessionRequestPermission.Result
}

public protocol ClientDelegate: FileSystemDelegate, TerminalDelegate, PermissionDelegate {}
