@_exported import OmniUICore
import Foundation
import OmniUINotcursesRenderer

private struct _OmniUIAppRootView: View, @unchecked Sendable {
    let root: AnyView
    let commands: AnyView?
    let preferredSize: CGSize?

    var body: some View {
        let sized: AnyView = {
            guard let preferredSize else { return root }
            return AnyView(root.frame(width: preferredSize.width, height: preferredSize.height))
        }()

        if let commands {
            return AnyView(
                VStack(spacing: 0) {
                    sized
                    Divider()
                    commands
                        .foregroundStyle(.secondary)
                        .padding(.horizontal, 1)
                }
            )
        }
        return sized
    }
}

public extension App {
    @MainActor
    static func main() async throws {
        let app = Self.init()
        try await NotcursesApp(scene: app.body).run()
    }
}
