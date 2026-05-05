import Testing
import Foundation
@testable import OmniKit

@Test func example() async throws {
    #expect(OmniKit.version == "0.1.0")

    let counter = OmniCounter()
    #expect(await counter.current() == 0)
    #expect(await counter.increment() == 1)
    #expect(await counter.increment() == 2)
}
