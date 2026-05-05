import Foundation

public enum Environment: String, Codable, Sendable {
    case mac
    case windows
    case ubuntu
    case browser
}

public enum Button: String, Codable, Sendable {
    case left
    case right
    case wheel
    case back
    case forward
}

public protocol Computer: Sendable {
    var environment: Environment { get }
    var dimensions: (Int, Int) { get }
    func screenshot() throws -> String
    func click(x: Int, y: Int, button: Button) throws
    func doubleClick(x: Int, y: Int) throws
    func scroll(x: Int, y: Int, scrollX: Int, scrollY: Int) throws
    func type(_ text: String) throws
    func wait() throws
    func move(x: Int, y: Int) throws
    func keypress(_ keys: [String]) throws
    func drag(_ path: [(Int, Int)]) throws
}

public protocol AsyncComputer: Sendable {
    var environment: Environment { get }
    var dimensions: (Int, Int) { get }
    func screenshot() async throws -> String
    func click(x: Int, y: Int, button: Button) async throws
    func doubleClick(x: Int, y: Int) async throws
    func scroll(x: Int, y: Int, scrollX: Int, scrollY: Int) async throws
    func type(_ text: String) async throws
    func wait() async throws
    func move(x: Int, y: Int) async throws
    func keypress(_ keys: [String]) async throws
    func drag(_ path: [(Int, Int)]) async throws
}
