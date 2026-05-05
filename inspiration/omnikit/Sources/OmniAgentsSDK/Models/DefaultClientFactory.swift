import Foundation
import OmniAICore

func makeDefaultClient(for owner: String) -> Client {
    do {
        return try Client.fromEnvAllowingEmpty()
    } catch {
        OmniAgentsLogger.warning("\(owner) could not create a default Client from the environment; using an empty Client instead: \(error)")
        do {
            return try Client(providers: [:], defaultProvider: nil)
        } catch {
            preconditionFailure("Failed to create fallback empty Client for \(owner): \(error)")
        }
    }
}
