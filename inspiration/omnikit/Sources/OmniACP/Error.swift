import Foundation

public enum ClientError: Error, Sendable, CustomStringConvertible {
    case alreadyConnected
    case notConnected
    case connectionClosed
    case invalidPayload(String)
    case invalidResponse(String)
    case missingResult(String)
    case timedOut(String)
    case unsupportedPlatform(String)
    case permissionDenied(String)
    case pathOutsideRoot(String)
    case transportClosed
    case processExited(Int32)

    public var description: String {
        switch self {
        case .alreadyConnected:
            return "Client is already connected"
        case .notConnected:
            return "Client is not connected"
        case .connectionClosed:
            return "Connection closed"
        case .invalidPayload(let message):
            return "Invalid payload: \(message)"
        case .invalidResponse(let message):
            return "Invalid response: \(message)"
        case .missingResult(let method):
            return "Missing result for method \(method)"
        case .timedOut(let operation):
            return "Timed out while waiting for \(operation)"
        case .unsupportedPlatform(let detail):
            return "Unsupported platform: \(detail)"
        case .permissionDenied(let detail):
            return "Permission denied: \(detail)"
        case .pathOutsideRoot(let path):
            return "Path is outside allowed root: \(path)"
        case .transportClosed:
            return "Transport is closed"
        case .processExited(let status):
            return "Process exited with status \(status)"
        }
    }
}
