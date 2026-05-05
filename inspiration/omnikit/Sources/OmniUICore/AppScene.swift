// Minimal SwiftUI App/Scene/Commands surface.
//
// This remains intentionally lightweight, but it now preserves enough metadata
// for renderer-backed app launching from the `OmniUI` umbrella.

import Foundation

// MARK: - App

public protocol App {
    associatedtype Body: Scene
    @SceneBuilder var body: Body { get }
    init()
}

// MARK: - Scene

public protocol Scene {
    associatedtype Body
    @SceneBuilder var body: Body { get }
}

public extension Scene where Body == Never {
    var body: Never { fatalError("Primitive scenes have no body") }
}

public struct EmptyScene: Scene {
    public typealias Body = Never
    public init() {}
}

public struct AnyScene: Scene {
    public typealias Body = Never
    let _box: Any

    public init<S: Scene>(_ scene: S) {
        self._box = scene
    }
}

public struct TupleScene: Scene {
    public typealias Body = Never
    let scenes: [AnyScene]

    public init(_ scenes: [AnyScene]) {
        self.scenes = scenes
    }
}

@resultBuilder
public enum SceneBuilder {
    public static func buildBlock() -> EmptyScene { EmptyScene() }
    public static func buildBlock<S0: Scene>(_ s0: S0) -> S0 { s0 }

    public static func buildBlock<S0: Scene, S1: Scene>(_ s0: S0, _ s1: S1) -> TupleScene {
        TupleScene([AnyScene(s0), AnyScene(s1)])
    }

    public static func buildBlock<S0: Scene, S1: Scene, S2: Scene>(_ s0: S0, _ s1: S1, _ s2: S2) -> TupleScene {
        TupleScene([AnyScene(s0), AnyScene(s1), AnyScene(s2)])
    }

    public static func buildOptional<S: Scene>(_ scene: S?) -> AnyScene {
        if let scene { AnyScene(scene) } else { AnyScene(EmptyScene()) }
    }

    public static func buildEither<S: Scene>(first scene: S) -> AnyScene { AnyScene(scene) }
    public static func buildEither<S: Scene>(second scene: S) -> AnyScene { AnyScene(scene) }
}

// MARK: - Scene Types + Modifiers

public struct WindowGroup<Content: View>: Scene {
    public typealias Body = Never
    let content: Content

    public init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }
}

public struct Settings<Content: View>: Scene {
    public typealias Body = Never
    let content: Content

    public init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }
}

public protocol Commands {}

public struct EmptyCommands: Commands { public init() {} }

public enum CommandGroupPlacement: Hashable, Sendable {
    case appInfo
}

public struct CommandGroup<Content: View>: Commands {
    public let placement: CommandGroupPlacement
    let content: Content

    public init(after placement: CommandGroupPlacement, @ViewBuilder content: () -> Content) {
        self.placement = placement
        self.content = content()
    }

    public init(before placement: CommandGroupPlacement, @ViewBuilder content: () -> Content) {
        self.placement = placement
        self.content = content()
    }
}

public struct AnyCommands: Commands {
    let _box: Any
    public init<C: Commands>(_ commands: C) { self._box = commands }
}

public struct TupleCommands: Commands {
    let commands: [AnyCommands]
    public init(_ commands: [AnyCommands]) { self.commands = commands }
}

@resultBuilder
public enum CommandsBuilder {
    public static func buildBlock() -> EmptyCommands { EmptyCommands() }
    public static func buildBlock<C0: Commands>(_ c0: C0) -> C0 { c0 }

    public static func buildBlock<C0: Commands, C1: Commands>(_ c0: C0, _ c1: C1) -> TupleCommands {
        TupleCommands([AnyCommands(c0), AnyCommands(c1)])
    }

    public static func buildBlock<C0: Commands, C1: Commands, C2: Commands>(_ c0: C0, _ c1: C1, _ c2: C2) -> TupleCommands {
        TupleCommands([AnyCommands(c0), AnyCommands(c1), AnyCommands(c2)])
    }

    public static func buildOptional<C: Commands>(_ commands: C?) -> AnyCommands {
        if let commands { AnyCommands(commands) } else { AnyCommands(EmptyCommands()) }
    }

    public static func buildEither<C: Commands>(first commands: C) -> AnyCommands { AnyCommands(commands) }
    public static func buildEither<C: Commands>(second commands: C) -> AnyCommands { AnyCommands(commands) }
}

public struct SidebarCommands: Commands { public init() {} }

private protocol _CommandsRoot {
    func _commandViews() -> [AnyView]
}

extension EmptyCommands: _CommandsRoot {
    fileprivate func _commandViews() -> [AnyView] { [] }
}

extension CommandGroup: _CommandsRoot {
    fileprivate func _commandViews() -> [AnyView] { [AnyView(content)] }
}

extension AnyCommands: _CommandsRoot {
    fileprivate func _commandViews() -> [AnyView] {
        (self._box as? _CommandsRoot)?._commandViews() ?? []
    }
}

extension TupleCommands: _CommandsRoot {
    fileprivate func _commandViews() -> [AnyView] {
        commands.flatMap { $0._commandViews() }
    }
}

extension SidebarCommands: _CommandsRoot {
    fileprivate func _commandViews() -> [AnyView] {
        [AnyView(Button("Sidebar") {})]
    }
}

private struct _CommandsBar: View, _PrimitiveView {
    public typealias Body = Never
    let items: [AnyView]

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .stack(axis: .horizontal, spacing: 1, children: items.map { ctx.buildChild($0) })
    }
}

public struct _ScenePassthrough<Content: Scene>: Scene {
    public typealias Body = Never
    let content: Content
    init(_ content: Content) { self.content = content }
}

public struct _SceneModelContainerProvider<Content: Scene>: Scene {
    public typealias Body = Never
    let content: Content
    let container: ModelContainer
    init(_ content: Content, container: ModelContainer) {
        self.content = content
        self.container = container
    }
}

public struct _SceneCommandsProvider<Content: Scene>: Scene {
    public typealias Body = Never
    let content: Content
    let commands: AnyCommands
}

public struct _SceneDefaultSizeProvider<Content: Scene>: Scene {
    public typealias Body = Never
    let content: Content
    let preferredSize: CGSize
}

public protocol _OmniUISceneRoot {
    func _omniUIRootView() -> AnyView?
    func _omniUICommandsView() -> AnyView?
    var _omniUIPreferredSize: CGSize? { get }
}

private protocol _CommandsRenderable {
    func _commandsView() -> AnyView?
}

private struct _CommandsViewStack: View, _PrimitiveView {
    public typealias Body = Never
    let items: [AnyView]

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .stack(axis: .horizontal, spacing: 1, children: items.map { ctx.buildChild($0) })
    }
}

private func _mergeCommandViews(_ lhs: AnyView?, _ rhs: AnyView?) -> AnyView? {
    switch (lhs, rhs) {
    case (nil, nil):
        return nil
    case let (lhs?, nil):
        return lhs
    case let (nil, rhs?):
        return rhs
    case let (lhs?, rhs?):
        return AnyView(_CommandsViewStack(items: [lhs, rhs]))
    }
}

extension EmptyCommands: _CommandsRenderable {
    fileprivate func _commandsView() -> AnyView? { nil }
}

extension SidebarCommands: _CommandsRenderable {
    fileprivate func _commandsView() -> AnyView? {
        AnyView(Button("Sidebar") {})
    }
}

extension CommandGroup: _CommandsRenderable {
    fileprivate func _commandsView() -> AnyView? { AnyView(content) }
}

extension AnyCommands: _CommandsRenderable {
    fileprivate func _commandsView() -> AnyView? {
        (self._box as? _CommandsRenderable)?._commandsView()
    }
}

extension TupleCommands: _CommandsRenderable {
    fileprivate func _commandsView() -> AnyView? {
        let views = commands.compactMap { ($0._box as? _CommandsRenderable)?._commandsView() }
        guard !views.isEmpty else { return nil }
        return AnyView(_CommandsViewStack(items: views))
    }
}

extension WindowGroup: _OmniUISceneRoot {
    public func _omniUIRootView() -> AnyView? { AnyView(content) }
    public func _omniUICommandsView() -> AnyView? { nil }
    public var _omniUIPreferredSize: CGSize? { nil }
}

extension Settings: _OmniUISceneRoot {
    public func _omniUIRootView() -> AnyView? { AnyView(content) }
    public func _omniUICommandsView() -> AnyView? { nil }
    public var _omniUIPreferredSize: CGSize? { nil }
}

extension AnyScene: _OmniUISceneRoot {
    public func _omniUIRootView() -> AnyView? {
        (self._box as? _OmniUISceneRoot)?._omniUIRootView()
    }

    public func _omniUICommandsView() -> AnyView? {
        (self._box as? _OmniUISceneRoot)?._omniUICommandsView()
    }

    public var _omniUIPreferredSize: CGSize? {
        (self._box as? _OmniUISceneRoot)?._omniUIPreferredSize
    }
}

extension TupleScene: _OmniUISceneRoot {
    public func _omniUIRootView() -> AnyView? {
        for scene in scenes {
            if let root = (scene._box as? _OmniUISceneRoot)?._omniUIRootView() {
                return root
            }
        }
        return nil
    }

    public func _omniUICommandsView() -> AnyView? {
        scenes.reduce(nil) { partial, scene in
            _mergeCommandViews(partial, (scene._box as? _OmniUISceneRoot)?._omniUICommandsView())
        }
    }

    public var _omniUIPreferredSize: CGSize? {
        for scene in scenes {
            if let size = (scene._box as? _OmniUISceneRoot)?._omniUIPreferredSize {
                return size
            }
        }
        return nil
    }
}

extension _ScenePassthrough: _OmniUISceneRoot where Content: _OmniUISceneRoot {
    public func _omniUIRootView() -> AnyView? {
        content._omniUIRootView()
    }

    public func _omniUICommandsView() -> AnyView? {
        content._omniUICommandsView()
    }

    public var _omniUIPreferredSize: CGSize? {
        content._omniUIPreferredSize
    }
}

extension _SceneModelContainerProvider: _OmniUISceneRoot where Content: _OmniUISceneRoot {
    public func _omniUIRootView() -> AnyView? {
        guard let root = content._omniUIRootView() else { return nil }
        return AnyView(root.modelContainer(container))
    }

    public func _omniUICommandsView() -> AnyView? {
        content._omniUICommandsView()
    }

    public var _omniUIPreferredSize: CGSize? {
        content._omniUIPreferredSize
    }
}

extension _SceneCommandsProvider: _OmniUISceneRoot where Content: _OmniUISceneRoot {
    public func _omniUIRootView() -> AnyView? {
        content._omniUIRootView()
    }

    public func _omniUICommandsView() -> AnyView? {
        _mergeCommandViews(content._omniUICommandsView(), commands._commandsView())
    }

    public var _omniUIPreferredSize: CGSize? {
        content._omniUIPreferredSize
    }
}

extension _SceneDefaultSizeProvider: _OmniUISceneRoot where Content: _OmniUISceneRoot {
    public func _omniUIRootView() -> AnyView? {
        content._omniUIRootView()
    }

    public func _omniUICommandsView() -> AnyView? {
        content._omniUICommandsView()
    }

    public var _omniUIPreferredSize: CGSize? {
        preferredSize
    }
}

public extension Scene {
    func commands<C: Commands>(@CommandsBuilder _ content: () -> C) -> some Scene {
        _SceneCommandsProvider(content: self, commands: AnyCommands(content()))
    }

    func modelContainer(_ any: Any) -> some Scene {
        if let container = any as? ModelContainer {
            return AnyScene(_SceneModelContainerProvider(self, container: container))
        }
        return AnyScene(_ScenePassthrough(self))
    }

    func modelContainer(_ container: ModelContainer) -> some Scene {
        _SceneModelContainerProvider(self, container: container)
    }

    func defaultSize(width: CGFloat, height: CGFloat) -> some Scene {
        _SceneDefaultSizeProvider(content: self, preferredSize: CGSize(width: width, height: height))
    }
}


public func _sceneRootView<S: Scene>(_ scene: S) -> AnyView? {
    (scene as? _OmniUISceneRoot)?._omniUIRootView()
}

public func _sceneCommandsView<S: Scene>(_ scene: S) -> AnyView? {
    (scene as? _OmniUISceneRoot)?._omniUICommandsView()
}

public func _scenePreferredSize<S: Scene>(_ scene: S) -> CGSize? {
    (scene as? _OmniUISceneRoot)?._omniUIPreferredSize
}
