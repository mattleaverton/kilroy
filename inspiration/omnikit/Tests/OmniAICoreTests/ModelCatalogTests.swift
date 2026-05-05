import Testing
import Foundation

@testable import OmniAICore

@Suite
final class ModelCatalogTests {
    @Test
    func testModelCatalogLookupByIdAndAlias() {
        let catalog = ModelCatalog.default
        XCTAssertNotNil(catalog.getModelInfo("gpt-5.2"))
        XCTAssertNotNil(catalog.getModelInfo("opus"))
        XCTAssertNotNil(catalog.getModelInfo("latest-groq"))
    }

    @Test
    func testModelCatalogListAndLatest() {
        let catalog = ModelCatalog.default
        let anthropic = catalog.listModels(provider: "anthropic")
        XCTAssertFalse(anthropic.isEmpty)

        let latest = catalog.getLatestModel(provider: "anthropic")
        XCTAssertEqual(latest?.provider, "anthropic")
    }
}
