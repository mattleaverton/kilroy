import Foundation

public struct Text: View, _PrimitiveView {
    public typealias Body = Never
    public let content: String

    public init(_ content: String) {
        self.content = content
    }

    public init(_ image: Image) {
        // SwiftUI supports `Text(Image(...))` for inline symbols. Our renderer is text-based,
        // so approximate with the image's symbol name.
        self.content = image.name
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _applyTextEnvironment(content: content, env: _currentEnvironmentValues(for: ctx))
    }
}

public struct Image: View, _PrimitiveView {
    public typealias Body = Never

    let name: String

    public init(systemName: String) {
        self.name = systemName
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .image(name)
    }
}

public struct Spacer: View, _PrimitiveView {
    public typealias Body = Never
    public init() {}
    public init(minLength: CGFloat? = nil) { self.init() }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode { .spacer }
}

public struct Divider: View, _PrimitiveView {
    public typealias Body = Never
    public init() {}

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode { .divider }
}

public struct ScrollView<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    let axes: Axis.Set
    let showsIndicators: Bool
    let content: Content
    let actionScopePath: [Int]

    public init(_ axes: Axis.Set = .vertical, showsIndicators: Bool = true, @ViewBuilder content: () -> Content) {
        self.axes = axes
        self.showsIndicators = showsIndicators
        self.content = content()
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let controlPath = ctx.path
        let isFocused = runtime._isFocused(path: controlPath)

        // ScrollView is a container; it should not participate in tab focus order.
        // Wheel routing uses `scrollRegions` hit-testing and does not require focus.
        let id = runtime._registerAction({}, path: actionScopePath)

        let axis: _Axis = axes.contains(.horizontal) && !axes.contains(.vertical) ? .horizontal : .vertical
        let offset = runtime._getScrollOffset(path: controlPath)

        return .scrollView(
            id: id,
            path: controlPath,
            isFocused: isFocused,
            axis: axis,
            offset: offset,
            content: ctx.buildChild(content)
        )
    }
}

public struct ScrollViewProxy {
    let _scrollTo: (AnyHashable, Alignment?) -> Void

    public func scrollTo<ID: Hashable>(_ id: ID, anchor: Alignment? = nil) {
        _scrollTo(AnyHashable(id), anchor)
    }
}

public struct ScrollViewReader<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    let content: (ScrollViewProxy) -> Content

    public init(@ViewBuilder content: @escaping (ScrollViewProxy) -> Content) {
        self.content = content
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let scopePath = ctx.path
        let proxy = ScrollViewProxy(_scrollTo: { id, anchor in
            runtime._requestScrollTo(id: id, anchor: anchor, scopePath: scopePath)
        })
        return _UIRuntime.$_currentScrollReaderScopePath.withValue(scopePath) {
            ctx.buildChild(content(proxy))
        }
    }
}

public struct GeometryProxy: Sendable {
    public let size: CGSize
}

public struct GeometryReader<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    let content: (GeometryProxy) -> Content

    public init(@ViewBuilder content: @escaping (GeometryProxy) -> Content) {
        self.content = content
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        // SwiftUI's GeometryReader provides the proposed size of its container.
        // We approximate this with the current render size (terminal grid).
        let rs = _UIRuntime._currentRenderSize ?? _Size(width: 0, height: 0)
        let proxy = GeometryProxy(size: CGSize(width: CGFloat(rs.width), height: CGFloat(rs.height)))
        return ctx.buildChild(content(proxy))
    }
}

public struct NavigationView<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    let content: Content

    public init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let env = _currentEnvironmentValues(for: ctx)
        let root = ctx.buildChild(NavigationStack { content })
        switch env.navigationViewStyleKind {
        case .automatic, .default:
            return root
        case .column, .doubleColumn:
            return .stack(axis: .horizontal, spacing: 1, children: [
                .style(fg: .secondary, bg: nil, child: .text("▏")),
                root,
            ])
        }
    }
}

public struct DatePicker<Label: View>: View, _PrimitiveView {
    public typealias Body = Never

    let label: Label
    let selection: Binding<Date>

    public init(selection: Binding<Date>, @ViewBuilder label: () -> Label) {
        self.label = label()
        self.selection = selection
    }

    public init(_ title: String, selection: Binding<Date>) where Label == Text {
        self.label = Text(title)
        self.selection = selection
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let env = _currentEnvironmentValues(for: ctx)
        let renderedLabel = _menuLabelText(from: ctx.buildChild(label))
        let title = renderedLabel.isEmpty ? "Date" : renderedLabel
        let calendar = Calendar.current
        let formatted = _formattedDatePickerDate(selection.wrappedValue, style: env.datePickerStyleKind)
        let controls = HStack(spacing: 1) {
            Button("-") {
                selection.wrappedValue = calendar.date(byAdding: .day, value: -1, to: selection.wrappedValue) ?? selection.wrappedValue
            }
            Text(formatted)
            Button("+") {
                selection.wrappedValue = calendar.date(byAdding: .day, value: 1, to: selection.wrappedValue) ?? selection.wrappedValue
            }
        }
        switch env.datePickerStyleKind {
        case .graphical:
            return ctx.buildChild(
                GroupBox {
                    VStack(alignment: .leading, spacing: 1) {
                        HStack(spacing: 1) { Image(systemName: "calendar"); Text(title) }
                        controls
                    }
                }
            )
        case .compact, .field, .stepperField, .automatic:
            return ctx.buildChild(
                HStack(spacing: 1) {
                    Text("\(title):")
                    controls
                }
            )
        }
    }
}

public struct Gauge<Label: View, CurrentValueLabel: View>: View, _PrimitiveView {
    public typealias Body = Never

    let value: Double
    let bounds: ClosedRange<Double>
    let label: Label
    let currentValueLabel: CurrentValueLabel

    public init(value: Double, in bounds: ClosedRange<Double> = 0...1, @ViewBuilder label: () -> Label, @ViewBuilder currentValueLabel: () -> CurrentValueLabel) {
        self.value = value
        self.bounds = bounds
        self.label = label()
        self.currentValueLabel = currentValueLabel()
    }

    public init(value: Double, in bounds: ClosedRange<Double> = 0...1, @ViewBuilder label: () -> Label) where CurrentValueLabel == EmptyView {
        self.value = value
        self.bounds = bounds
        self.label = label()
        self.currentValueLabel = EmptyView()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let env = _currentEnvironmentValues(for: ctx)
        _ = env.gaugeStyleKind
        let denom = bounds.upperBound - bounds.lowerBound
        let pct = denom == 0 ? 0 : Swift.max(0, Swift.min(1, (value - bounds.lowerBound) / denom))
        let renderedLabel = _menuLabelText(from: ctx.buildChild(label))
        let renderedCurrentValue = _menuLabelText(from: ctx.buildChild(currentValueLabel))
        let percentText = Int((pct * 100).rounded()).formatted(.number)
        let summary = renderedCurrentValue.isEmpty ? "\(percentText)%" : renderedCurrentValue
        let title = renderedLabel.isEmpty ? "Gauge" : renderedLabel
        return ctx.buildChild(
            VStack(alignment: .leading, spacing: 1) {
                Text(title)
                ProgressView(value: pct, total: 1)
                Text(summary).foregroundStyle(.secondary)
            }
        )
    }
}

public enum AsyncImagePhase {
    case empty
    case success(Image)
    case failure(String)
}

public struct AsyncImage<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    @State private var didAttemptLoad = false
    @State private var didLoadSucceed = false
    @State private var failureDescription: String? = nil

    let url: URL?
    let content: (AsyncImagePhase) -> Content

    public init(url: URL?, @ViewBuilder content: @escaping (AsyncImagePhase) -> Content) {
        self.url = url
        self.content = content
    }

    public init(url: URL?) where Content == Image {
        self.url = url
        self.content = { phase in
            switch phase {
            case .success(let image):
                return image
            case .empty, .failure:
                return Image(systemName: "photo")
            }
        }
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        if let url, !didAttemptLoad, !didLoadSucceed, failureDescription == nil {
            let path = ctx.path
            ctx.runtime._registerTask(path: path) {
                didAttemptLoad = true
                do {
                    if url.isFileURL {
                        _ = try Data(contentsOf: url)
                    } else {
                        _ = try await URLSession.shared.data(from: url)
                    }
                    didLoadSucceed = true
                    failureDescription = nil
                } catch {
                    failureDescription = error.localizedDescription
                }
            }
        }
        let phase: AsyncImagePhase
        if let failureDescription {
            phase = .failure(failureDescription)
        } else if didLoadSucceed {
            phase = .success(Image(systemName: "photo"))
        } else {
            phase = .empty
        }
        return ctx.buildChild(content(phase))
    }
}

public struct TimelineView<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    @State private var now: Date = Date()

    public struct Context: Sendable {
        public enum Cadence: Sendable {
            case live
            case seconds
            case minutes
        }

        public let date: Date
        public let cadence: Cadence
    }

    let content: (Context) -> Content

    public init(content: @escaping (Context) -> Content) {
        self.content = content
    }

    public init(_ schedule: Any = (), content: @escaping (Context) -> Content) {
        _ = schedule
        self.content = content
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let path = ctx.path
        ctx.runtime._registerTask(path: path) {
            while true {
                try? await Task.sleep(nanoseconds: 1_000_000_000)
                now = Date()
            }
        }
        return ctx.buildChild(content(Context(date: now, cadence: .live)))
    }
}

public struct ProgressView: View, _PrimitiveView {
    public typealias Body = Never

    let label: String?
    let value: Double?
    let total: Double

    public init() {
        self.label = nil
        self.value = nil
        self.total = 1.0
    }

    public init(_ title: String) {
        self.label = title
        self.value = nil
        self.total = 1.0
    }

    public init(value: Double?, total: Double = 1.0) {
        self.label = nil
        self.value = value
        self.total = total
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _ = ctx
        if let value {
            let denom = total == 0 ? 1 : total
            let pct = Swift.max(0, Swift.min(1, value / denom))
            let filled = Int((pct * 10).rounded())
            let bar = String(repeating: "#", count: filled) + String(repeating: "-", count: Swift.max(0, 10 - filled))
            return .text("[\(bar)] \(Int((pct * 100).rounded()))%")
        }

        let glyphs = ["-", "\\", "|", "/"]
        let tick = Int(Date().timeIntervalSinceReferenceDate * 8)
        let spinner = glyphs[tick % glyphs.count]
        let title = label ?? "Loading"
        return .text("\(spinner) \(title)")
    }
}

private struct _AnySelectionAdapter {
    let get: () -> AnyHashable?
    let set: (AnyHashable) -> Void
}

public struct List<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    private let content: Content
    private let selection: _AnySelectionAdapter?
    private let actionScopePath: [Int]
    private let customRowBuilder: ((inout _BuildContext) -> [_VNode])?

    public init(@ViewBuilder content: () -> Content) {
        self.content = content()
        self.selection = nil
        self.actionScopePath = _UIRuntime._currentPath ?? []
        self.customRowBuilder = nil
    }

    public init<SelectionValue: Hashable>(
        selection: Binding<SelectionValue>,
        @ViewBuilder content: () -> Content
    ) {
        self.content = content()
        self.selection = _AnySelectionAdapter(
            get: { AnyHashable(selection.wrappedValue) },
            set: { any in
                if let typed = any.base as? SelectionValue {
                    selection.wrappedValue = typed
                }
            }
        )
        self.actionScopePath = _UIRuntime._currentPath ?? []
        self.customRowBuilder = nil
    }

    public init<SelectionValue: Hashable>(
        selection: Binding<SelectionValue?>,
        @ViewBuilder content: () -> Content
    ) {
        self.content = content()
        self.selection = _AnySelectionAdapter(
            get: { selection.wrappedValue.map(AnyHashable.init) },
            set: { any in
                selection.wrappedValue = any.base as? SelectionValue
            }
        )
        self.actionScopePath = _UIRuntime._currentPath ?? []
        self.customRowBuilder = nil
    }

    public init<Data: RandomAccessCollection, ID: Hashable, RowContent: View>(
        _ data: Data,
        id: KeyPath<Data.Element, ID>,
        @ViewBuilder rowContent: @escaping (Data.Element) -> RowContent
    ) where Content == ForEach<Data, ID, RowContent> {
        self.content = ForEach(data, id: id, content: rowContent)
        self.selection = nil
        self.actionScopePath = _UIRuntime._currentPath ?? []
        self.customRowBuilder = nil
    }

    public init<Data: RandomAccessCollection, RowContent: View>(
        _ data: Data,
        @ViewBuilder rowContent: @escaping (Data.Element) -> RowContent
    ) where Data.Element: Identifiable, Content == ForEach<Data, Data.Element.ID, RowContent> {
        self.content = ForEach(data, content: rowContent)
        self.selection = nil
        self.actionScopePath = _UIRuntime._currentPath ?? []
        self.customRowBuilder = nil
    }

    public init<Data: RandomAccessCollection, RowContent: View>(
        _ data: Data,
        children: KeyPath<Data.Element, Data?>,
        @ViewBuilder rowContent: @escaping (Data.Element) -> RowContent
    ) where Data.Element: Identifiable, Content == ForEach<Data, Data.Element.ID, RowContent> {
        self.content = ForEach(data, content: rowContent)
        self.selection = nil
        self.actionScopePath = _UIRuntime._currentPath ?? []
        self.customRowBuilder = { ctx in
            var rows: [_VNode] = []

            func appendRows(_ nodes: Data, depth: Int) {
                for element in nodes {
                    var rowNode = ctx.buildChild(rowContent(element))
                    if depth > 0 {
                        let padding = String(repeating: "  ", count: depth)
                        rowNode = .stack(axis: .horizontal, spacing: 0, children: [.text(padding), rowNode])
                    }
                    rows.append(rowNode)

                    if let childrenNodes = element[keyPath: children], !childrenNodes.isEmpty {
                        appendRows(childrenNodes, depth: depth + 1)
                    }
                }
            }

            appendRows(data, depth: 0)
            return rows
        }
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let env = _currentEnvironmentValues(for: ctx)
        let controlPath = ctx.path
        let isFocused = runtime._isFocused(path: controlPath)
        let id = runtime._registerAction({}, path: actionScopePath)
        let offset = runtime._getScrollOffset(path: controlPath)

        let rows = customRowBuilder?(&ctx) ?? _flatten(ctx.buildChild(content))
        let selected = selection?.get()
        var rowIndex = 0
        var renderedRows: [_VNode] = []
        renderedRows.reserveCapacity(rows.count * 2)

        for (index, row) in rows.enumerated() {
            var rowNode: _VNode
            if let selection, case .tagged(let value, let label) = row {
                let path = controlPath + [10_000 + rowIndex]
                rowIndex += 1
                let actionID = runtime._registerAction({
                    runtime._setFocus(path: path)
                    selection.set(value)
                }, path: actionScopePath)
                runtime._registerFocusable(path: path, activate: actionID)

                var rowLabel = label
                if selected == value {
                    let tint = env.tint ?? .accentColor
                    if env.listStyleKind == .sidebar {
                        rowLabel = .style(fg: Color.white, bg: tint, child: .edgePadding(top: 0, leading: 1, bottom: 0, trailing: 1, child: rowLabel))
                    } else {
                        rowLabel = .style(fg: tint, bg: nil, child: rowLabel)
                    }
                }
                rowNode = .button(id: actionID, isFocused: runtime._isFocused(path: path), label: rowLabel)
            } else {
                rowNode = row
            }

            if let listRowBackground = env.listRowBackground {
                rowNode = .background(child: rowNode, background: ctx.buildChild(listRowBackground.view))
            }
            if env.listStyleKind == .sidebar {
                rowNode = .edgePadding(top: 0, leading: 1, bottom: 0, trailing: 1, child: rowNode)
            }
            renderedRows.append(rowNode)
            if env.listRowSeparatorVisibility != .hidden, index != rows.count - 1 {
                renderedRows.append(.divider)
            }
        }

        var contentNode: _VNode = .stack(axis: .vertical, spacing: 0, children: renderedRows)
        if env.scrollContentBackgroundVisibility != .hidden {
            let backgroundColor: Color = env.listStyleKind == .sidebar ? Color.gray.opacity(0.12) : Color.gray.opacity(0.06)
            contentNode = .style(fg: nil, bg: backgroundColor, child: contentNode)
        }

        return .scrollView(
            id: id,
            path: controlPath,
            isFocused: isFocused,
            axis: .vertical,
            offset: offset,
            content: contentNode
        )
    }
}

public struct Form<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    let content: Content

    public init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let env = _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment
        let isGrouped = env.formStyleKind == .grouped
        let shouldShowBackground = env.scrollContentBackgroundVisibility != .hidden

        if isGrouped {
            return ctx.buildChild(
                ScrollView {
                    VStack(alignment: .leading, spacing: 1) { content }
                        .padding(1)
                        .background(shouldShowBackground ? Color.gray.opacity(0.08) : .clear)
                }
            )
        }

        return ctx.buildChild(
            ScrollView {
                VStack(alignment: .leading, spacing: 0) { content }
                    .padding(1)
                    .background(shouldShowBackground ? Color.gray.opacity(0.04) : .clear)
            }
        )
    }
}

public struct GridItem: Hashable, Sendable {
    public enum Size: Hashable, Sendable {
        case fixed(CGFloat)
        case flexible(minimum: CGFloat = 10, maximum: CGFloat = .infinity)
        case adaptive(minimum: CGFloat, maximum: CGFloat = .infinity)
    }

    public var size: Size
    public var spacing: CGFloat?

    public init(_ size: Size, spacing: CGFloat? = nil) {
        self.size = size
        self.spacing = spacing
    }
}

public struct LazyVGrid<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    let columns: [GridItem]
    let alignment: HorizontalAlignment
    let spacing: CGFloat?
    let content: Content

    public init(
        columns: [GridItem],
        alignment: HorizontalAlignment = .center,
        spacing: CGFloat? = nil,
        @ViewBuilder content: () -> Content
    ) {
        self.columns = columns
        self.alignment = alignment
        self.spacing = spacing
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        _ = alignment
        let items = _flatten(ctx.buildChild(content))
        guard !items.isEmpty else { return .empty }

        let gap = Swift.max(0, Int((spacing ?? 1).rounded()))
        let availableWidth = (_UIRuntime._currentRenderSize ?? _Size(width: 80, height: 0)).width
        let columnsCount = _gridColumnCount(columns, availableWidth: availableWidth, spacing: gap)

        guard columnsCount > 1 else {
            return .stack(axis: .vertical, spacing: gap, children: items)
        }

        var rows: [_VNode] = []
        rows.reserveCapacity((items.count + columnsCount - 1) / columnsCount)

        var index = 0
        while index < items.count {
            let end = Swift.min(index + columnsCount, items.count)
            let row = Array(items[index..<end])
            rows.append(.stack(axis: .horizontal, spacing: gap, children: row))
            index = end
        }

        return .stack(axis: .vertical, spacing: gap, children: rows)
    }
}

private func _gridColumnCount(_ columns: [GridItem], availableWidth: Int, spacing: Int) -> Int {
    guard let first = columns.first else { return 1 }

    switch first.size {
    case .adaptive(let minimum, _):
        let minCell = Swift.max(1, Int((minimum / 8).rounded()))
        let stride = Swift.max(1, minCell + spacing)
        return Swift.max(1, availableWidth / stride)
    default:
        return Swift.max(1, columns.count)
    }
}

public struct ForEach<Data: RandomAccessCollection, ID: Hashable, Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    let data: Data
    let id: KeyPath<Data.Element, ID>
    let content: (Data.Element) -> Content

    public init(_ data: Data, id: KeyPath<Data.Element, ID>, @ViewBuilder content: @escaping (Data.Element) -> Content) {
        self.data = data
        self.id = id
        self.content = content
    }

    public init(_ data: Data, @ViewBuilder content: @escaping (Data.Element) -> Content) where Data.Element: Identifiable, ID == Data.Element.ID {
        self.data = data
        self.id = \.id
        self.content = content
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        // NOTE: We currently don't incorporate `id` into the build-path (path is `[Int]`).
        // This preserves call-site compatibility, but state may "move" if data is reordered.
        var nodes: [_VNode] = []
        nodes.reserveCapacity(data.count)
        for element in data {
            _ = element[keyPath: id] // keep the id "used" (and future-proof for stable identity work)
            nodes.append(ctx.buildChild(content(element)))
        }
        return .group(nodes)
    }
}

public struct NavigationPath: Hashable, @unchecked Sendable {
    public struct CodableRepresentation: Codable, Hashable, Sendable {
        public init() {}
    }

    var elements: [AnyHashable]

    public init() {
        self.elements = []
    }

    public init<S: Sequence>(_ elements: S) where S.Element: Hashable {
        self.elements = elements.map(AnyHashable.init)
    }

    public var count: Int { elements.count }
    public var isEmpty: Bool { elements.isEmpty }

    public mutating func append<V: Hashable>(_ value: V) {
        elements.append(AnyHashable(value))
    }

    public mutating func removeLast(_ count: Int = 1) {
        guard count > 0 else { return }
        elements.removeLast(Swift.min(count, elements.count))
    }
}

public struct NavigationStack<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    let path: Binding<NavigationPath>?
    let content: Content

    public init(@ViewBuilder content: () -> Content) {
        self.path = nil
        self.content = content()
    }

    public init(path: Binding<NavigationPath>, @ViewBuilder content: () -> Content) {
        self.path = path
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let stackPath = ctx.path
        runtime._registerNavStackRoot(path: stackPath)

        let rootNode = ctx.buildChild(content)

        if let path {
            let desiredElements = path.wrappedValue.elements
            while runtime._navDepth(stackPath: stackPath) > desiredElements.count {
                runtime._navPop(stackPath: stackPath)
            }
            if runtime._navDepth(stackPath: stackPath) < desiredElements.count {
                for value in desiredElements.dropFirst(runtime._navDepth(stackPath: stackPath)) {
                    guard let resolved = runtime._resolveNavDestination(stackPath: stackPath, value: value) else { break }
                    runtime._navPush(stackPath: stackPath, view: resolved)
                }
            }
            if runtime._navDepth(stackPath: stackPath) < desiredElements.count {
                var next = path.wrappedValue
                while next.count > runtime._navDepth(stackPath: stackPath) {
                    next.removeLast()
                }
                path.wrappedValue = next
            }
        }

        let depth = runtime._navDepth(stackPath: stackPath)
        let top: AnyView? = runtime._navTop(stackPath: stackPath)

        if depth == 0 {
            return rootNode
        }

        let current = _UIRuntime._currentEnvironment ?? runtime._baseEnvironment
        var next = current
        next.dismiss = DismissAction {
            runtime._navPop(stackPath: stackPath)
            if let path, runtime._navDepth(stackPath: stackPath) < path.wrappedValue.count {
                var updated = path.wrappedValue
                while updated.count > runtime._navDepth(stackPath: stackPath) {
                    updated.removeLast()
                }
                path.wrappedValue = updated
            }
        }
        let mode = PresentationMode(dismiss: {
            runtime._navPop(stackPath: stackPath)
            if let path, runtime._navDepth(stackPath: stackPath) < path.wrappedValue.count {
                var updated = path.wrappedValue
                while updated.count > runtime._navDepth(stackPath: stackPath) {
                    updated.removeLast()
                }
                path.wrappedValue = updated
            }
        })
        next.presentationMode = Binding(get: { mode }, set: { _ in })

        return .stack(axis: .vertical, spacing: 1, children: _flatten(.group([
            ctx.buildChild(
                HStack(spacing: 1) {
                    Button("Back") {
                        runtime._navPop(stackPath: stackPath)
                        if let path, runtime._navDepth(stackPath: stackPath) < path.wrappedValue.count {
                            var updated = path.wrappedValue
                            while updated.count > runtime._navDepth(stackPath: stackPath) {
                                updated.removeLast()
                            }
                            path.wrappedValue = updated
                        }
                    }
                    Spacer()
                }
            ),
            _UIRuntime.$_currentEnvironment.withValue(next) {
                top.map { ctx.buildChild($0) } ?? rootNode
            },
        ])))
    }
}

public struct NavigationLink<Label: View, Destination: View>: View, _PrimitiveView {
    public typealias Body = Never

    let makeDestination: (() -> AnyView)?
    let navValue: AnyHashable?
    let label: Label
    let actionScopePath: [Int]

    public init(destination: @escaping () -> Destination, @ViewBuilder label: () -> Label) {
        self.makeDestination = { AnyView(destination()) }
        self.navValue = nil
        self.label = label()
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    public init(destination: Destination, @ViewBuilder label: () -> Label) {
        self.makeDestination = { AnyView(destination) }
        self.navValue = nil
        self.label = label()
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    public init(_ title: String, destination: @escaping () -> Destination) where Label == Text {
        self.makeDestination = { AnyView(destination()) }
        self.navValue = nil
        self.label = Text(title)
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    public init(_ title: String, destination: Destination) where Label == Text {
        self.makeDestination = { AnyView(destination) }
        self.navValue = nil
        self.label = Text(title)
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    public init<V: Hashable>(value: V, @ViewBuilder label: () -> Label) where Destination == EmptyView {
        self.makeDestination = nil
        self.navValue = AnyHashable(value)
        self.label = label()
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    public init<V: Hashable>(_ title: String, value: V) where Label == Text, Destination == EmptyView {
        self.makeDestination = nil
        self.navValue = AnyHashable(value)
        self.label = Text(title)
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let env = _currentEnvironmentValues(for: ctx)
        let labelNode = ctx.buildChild(label)
        guard env.isEnabled, _UIRuntime._hitTestingEnabled else {
            return _disabledButtonNode(label: labelNode)
        }
        guard let stackPath = ctx.runtime._nearestNavStackRoot(from: ctx.path) else {
            let runtime = ctx.runtime
            let controlPath = ctx.path
            let isFocused = runtime._isFocused(path: controlPath)
            let id = runtime._registerAction({ runtime._setFocus(path: controlPath) }, path: actionScopePath)
            runtime._registerFocusable(path: controlPath, activate: id)
            return _applyControlPadding(.button(id: id, isFocused: isFocused, label: labelNode), env: env)
        }
        let runtime = ctx.runtime
        let controlPath = ctx.path
        let isFocused = runtime._isFocused(path: controlPath)
        let id = runtime._registerAction({
            runtime._setFocus(path: controlPath)
            if let makeDestination {
                runtime._navPush(stackPath: stackPath, view: makeDestination())
            } else if let navValue, let resolved = runtime._resolveNavDestination(stackPath: stackPath, value: navValue) {
                runtime._navPush(stackPath: stackPath, view: resolved)
            }
        }, path: actionScopePath)
        runtime._registerFocusable(path: controlPath, activate: id)
        return _applyControlPadding(.button(id: id, isFocused: isFocused, label: labelNode), env: env)
    }
}

public enum NavigationSplitViewVisibility: Hashable, Sendable {
    case automatic
    case all
    case doubleColumn
    case detailOnly
}

public struct NavigationSplitView<Sidebar: View, Detail: View>: View, _PrimitiveView {
    public typealias Body = Never

    let columnVisibility: Binding<NavigationSplitViewVisibility>
    let sidebar: Sidebar
    let detail: Detail

    public init(
        columnVisibility: Binding<NavigationSplitViewVisibility> = Binding(get: { .automatic }, set: { _ in }),
        @ViewBuilder sidebar: () -> Sidebar,
        @ViewBuilder detail: () -> Detail
    ) {
        self.columnVisibility = columnVisibility
        self.sidebar = sidebar()
        self.detail = detail()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let resolved = _resolvedVisibility(
            requested: columnVisibility.wrappedValue,
            size: _UIRuntime._currentRenderSize ?? _Size(width: 80, height: 0)
        )

        switch resolved {
        case .detailOnly:
            return ctx.buildChild(detail)
        case .all, .doubleColumn, .automatic:
            return ctx.buildChild(
                HStack(spacing: 1) {
                    sidebar
                    detail
                }
            )
        }
    }

    private func _resolvedVisibility(
        requested: NavigationSplitViewVisibility,
        size: _Size
    ) -> NavigationSplitViewVisibility {
        guard requested == .automatic else { return requested }

        // Mirror platform behavior: in compact widths, collapse to detail-only;
        // in regular widths, keep both columns visible.
        return size.width >= 72 ? .doubleColumn : .detailOnly
    }
}

private struct _TabItemLabelTag: Hashable {
    let text: String
}

private struct _TabEntry {
    let value: AnyHashable?
    let label: String
    let content: _VNode
}

public struct TabView<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    private let content: Content
    private let selection: _AnySelectionAdapter?
    private let actionScopePath: [Int]
    @State private var localSelectionIndex: Int = 0

    public init(@ViewBuilder content: () -> Content) {
        self.content = content()
        self.selection = nil
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    public init<SelectionValue: Hashable>(selection: Binding<SelectionValue>, @ViewBuilder content: () -> Content) {
        self.content = content()
        self.selection = _AnySelectionAdapter(
            get: { AnyHashable(selection.wrappedValue) },
            set: { any in
                if let typed = any.base as? SelectionValue {
                    selection.wrappedValue = typed
                }
            }
        )
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    public init<SelectionValue: Hashable>(selection: Binding<SelectionValue?>, @ViewBuilder content: () -> Content) {
        self.content = content()
        self.selection = _AnySelectionAdapter(
            get: { selection.wrappedValue.map(AnyHashable.init) },
            set: { any in
                selection.wrappedValue = any.base as? SelectionValue
            }
        )
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let source = ctx.buildChild(content)
        let tabs = _collectTabEntries(from: source)
        guard !tabs.isEmpty else { return source }

        let selectedValue = selection?.get()
        let resolvedIndex: Int = {
            if let selectedValue, let idx = tabs.firstIndex(where: { $0.value == selectedValue }) {
                return idx
            }
            return Swift.min(Swift.max(0, localSelectionIndex), tabs.count - 1)
        }()

        guard tabs.indices.contains(resolvedIndex) else { return source }
        let resolvedTab = tabs[resolvedIndex]
        if let selectedValue = resolvedTab.value, selection?.get() != selectedValue {
            selection?.set(selectedValue)
        }

        var tabButtons: [_VNode] = []
        tabButtons.reserveCapacity(tabs.count)
        let tint = (_UIRuntime._currentEnvironment ?? runtime._baseEnvironment).tint ?? .accentColor

        for (idx, tab) in tabs.enumerated() {
            let path = ctx.path + [20_000 + idx]
            let actionID = runtime._registerAction({
                runtime._setFocus(path: path)
                if let value = tab.value {
                    selection?.set(value)
                } else {
                    localSelectionIndex = idx
                }
            }, path: actionScopePath)
            runtime._registerFocusable(path: path, activate: actionID)

            let title = idx == resolvedIndex ? "[\(tab.label)]" : tab.label
            let labelNode: _VNode = idx == resolvedIndex
                ? .style(fg: tint, bg: nil, child: .text(title))
                : .text(title)
            tabButtons.append(.button(id: actionID, isFocused: runtime._isFocused(path: path), label: labelNode))
        }

        return .stack(axis: .vertical, spacing: 1, children: [
            .stack(axis: .horizontal, spacing: 1, children: tabButtons),
            resolvedTab.content,
        ])
    }
}

private func _collectTabEntries(from node: _VNode) -> [_TabEntry] {
    let topLevel = _flatten(node)
    var entries: [_TabEntry] = []
    entries.reserveCapacity(topLevel.count)

    for child in topLevel {
        if case .tagged(let value, let labelNode) = child {
            let extracted = _stripTabItemLabel(from: labelNode)
            entries.append(_TabEntry(
                value: value,
                label: extracted.label ?? String(describing: value),
                content: extracted.content
            ))
            continue
        }

        let extracted = _stripTabItemLabel(from: child)
        if let label = extracted.label {
            entries.append(_TabEntry(value: nil, label: label, content: extracted.content))
        } else {
            entries.append(_TabEntry(value: nil, label: "Tab", content: child))
        }
    }

    return entries
}

private func _stripTabItemLabel(from node: _VNode) -> (content: _VNode, label: String?) {
    switch node {
    case .overlay(let child, let overlay):
        if case .tagged(let v, _) = overlay, let tag = v.base as? _TabItemLabelTag {
            return (child, tag.text)
        }
        return (node, nil)
    default:
        return (node, nil)
    }
}

public extension View {
    func tabItem<Label: View>(@ViewBuilder _ label: () -> Label) -> some View {
        _TabItem(content: AnyView(self), label: AnyView(label()))
    }
}

private struct _TabItem: View, _PrimitiveView {
    typealias Body = Never

    let content: AnyView
    let label: AnyView

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let childNode = ctx.buildChild(content)
        let labelText = _menuLabelText(from: ctx.buildChild(label))
        return .overlay(
            child: childNode,
            overlay: .tagged(value: AnyHashable(_TabItemLabelTag(text: labelText)), label: .empty)
        )
    }
}

public struct ZStack<Content: View>: View, _PrimitiveView {
    public typealias Body = Never
    public let alignment: Alignment
    public let content: Content

    public init(alignment: Alignment = .center, @ViewBuilder content: () -> Content) {
        self.alignment = alignment
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        // Keep ZStack as a single node so parent stacks don't flatten it into siblings.
        let child = ctx.buildChild(content)
        return .zstack(children: _flatten(child))
    }
}

public struct Group<Content: View>: View, _PrimitiveView {
    public typealias Body = Never
    public let content: Content

    public init(@ViewBuilder content: () -> Content) {
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.buildChild(content)
    }
}

public struct GroupBox<Label: View, Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    let label: Label
    let content: Content

    public init(@ViewBuilder content: () -> Content) where Label == EmptyView {
        self.label = EmptyView()
        self.content = content()
    }

    public init(@ViewBuilder content: () -> Content, @ViewBuilder label: () -> Label) {
        self.label = label()
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.buildChild(
            VStack(alignment: .leading, spacing: 1) {
                label
                content
            }
            .padding(1)
        )
    }
}

public struct LabeledContent: View, _PrimitiveView {
    public typealias Body = Never

    let label: String
    let value: String

    public init(_ label: String, value: String) {
        self.label = label
        self.value = value
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.buildChild(
            HStack(spacing: 1) {
                Text(label)
                Spacer()
                Text(value)
            }
        )
    }
}

public struct ContentUnavailableView<Description: View>: View, _PrimitiveView {
    public typealias Body = Never

    let title: String
    let systemImage: String
    let description: Description

    public init(_ title: String, systemImage: String, description: Description) {
        self.title = title
        self.systemImage = systemImage
        self.description = description
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.buildChild(
            VStack(spacing: 1) {
                Image(systemName: systemImage)
                Text(title)
                description
            }
            .padding(1)
        )
    }
}

public struct Section<Parent: View, Content: View, Footer: View>: View, _PrimitiveView {
    public typealias Body = Never

    let header: Parent
    let content: Content
    let footer: Footer

    public init(@ViewBuilder content: () -> Content) where Parent == EmptyView, Footer == EmptyView {
        self.header = EmptyView()
        self.content = content()
        self.footer = EmptyView()
    }

    public init(header: Parent, @ViewBuilder content: () -> Content) where Footer == EmptyView {
        self.header = header
        self.content = content()
        self.footer = EmptyView()
    }

    public init(header: Parent, footer: Footer, @ViewBuilder content: () -> Content) {
        self.header = header
        self.content = content()
        self.footer = footer
    }

    public init(@ViewBuilder content: () -> Content, @ViewBuilder header: () -> Parent) where Footer == EmptyView {
        self.header = header()
        self.content = content()
        self.footer = EmptyView()
    }

    public init(@ViewBuilder content: () -> Content, @ViewBuilder header: () -> Parent, @ViewBuilder footer: () -> Footer) {
        self.header = header()
        self.content = content()
        self.footer = footer()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.buildChild(
            VStack(spacing: 0) {
                header
                content
                footer
            }
        )
    }
}

public struct Table<Data: RandomAccessCollection, ID: Hashable, RowContent: View>: View, _PrimitiveView {
    public typealias Body = Never

    let data: Data
    let id: KeyPath<Data.Element, ID>
    let rowContent: (Data.Element) -> RowContent

    public init(_ data: Data, id: KeyPath<Data.Element, ID>, @ViewBuilder rowContent: @escaping (Data.Element) -> RowContent) {
        self.data = data
        self.id = id
        self.rowContent = rowContent
    }

    public init(_ data: Data, @ViewBuilder rowContent: @escaping (Data.Element) -> RowContent) where Data.Element: Identifiable, ID == Data.Element.ID {
        self.data = data
        self.id = \.id
        self.rowContent = rowContent
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        ctx.buildChild(
            List(data, id: id, rowContent: rowContent)
                .listStyle(.plain)
        )
    }
}

public struct VStack<Content: View>: View, _PrimitiveView {
    public typealias Body = Never
    public let spacing: Int
    public let content: Content

    public init(spacing: Int = 0, @ViewBuilder content: () -> Content) {
        self.spacing = spacing
        self.content = content()
    }

    public init(alignment: HorizontalAlignment = .center, spacing: CGFloat? = nil, @ViewBuilder content: () -> Content) {
        let s = Int(spacing ?? 0)
        self.spacing = s
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let child = ctx.buildChild(content)
        return .stack(axis: .vertical, spacing: spacing, children: _flatten(child))
    }
}

public struct LazyVStack<Content: View>: View, _PrimitiveView {
    public typealias Body = Never

    let alignment: HorizontalAlignment
    let spacing: CGFloat?
    let content: Content

    public init(alignment: HorizontalAlignment = .center, spacing: CGFloat? = nil, @ViewBuilder content: () -> Content) {
        self.alignment = alignment
        self.spacing = spacing
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        // We don't currently implement laziness/virtualization; render as a regular VStack.
        ctx.buildChild(VStack(alignment: alignment, spacing: spacing) { content })
    }
}

public struct HStack<Content: View>: View, _PrimitiveView {
    public typealias Body = Never
    public let spacing: Int
    public let content: Content

    public init(spacing: Int = 0, @ViewBuilder content: () -> Content) {
        self.spacing = spacing
        self.content = content()
    }

    public init(alignment: VerticalAlignment = .center, spacing: CGFloat? = nil, @ViewBuilder content: () -> Content) {
        let s = Int(spacing ?? 0)
        self.spacing = s
        self.content = content()
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let child = ctx.buildChild(content)
        return .stack(axis: .horizontal, spacing: spacing, children: _flatten(child))
    }
}

public enum ButtonRole: Hashable, Sendable {
    case destructive
    case cancel
}

public struct Button<Label: View>: View, _PrimitiveView {
    public typealias Body = Never

    let action: () -> Void
    let actionScopePath: [Int]
    let role: ButtonRole?
    let label: Label

    public init(action: @escaping () -> Void, @ViewBuilder label: () -> Label) {
        self.action = action
        self.actionScopePath = _UIRuntime._currentPath ?? []
        self.role = nil
        self.label = label()
    }

    public init(role: ButtonRole?, action: @escaping () -> Void, @ViewBuilder label: () -> Label) {
        self.action = action
        self.actionScopePath = _UIRuntime._currentPath ?? []
        self.role = role
        self.label = label()
    }

    public init(_ title: String, action: @escaping () -> Void) where Label == Text {
        self.action = action
        self.actionScopePath = _UIRuntime._currentPath ?? []
        self.role = nil
        self.label = Text(title)
    }

    public init(_ title: String, role: ButtonRole?, action: @escaping () -> Void) where Label == Text {
        self.action = action
        self.actionScopePath = _UIRuntime._currentPath ?? []
        self.role = role
        self.label = Text(title)
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime

        if let captureID = _UIRuntime._currentMenuCaptureID {
            let labelNode = ctx.buildChild(label)
            let text = _menuLabelText(from: labelNode)
            let env = _UIRuntime._currentEnvironment ?? runtime._baseEnvironment
            runtime._registerMenuCaptureItem(
                _UIRuntime._MenuCaptureItem(
                    label: text,
                    role: role,
                    actionScopePath: actionScopePath,
                    env: env,
                    action: action
                ),
                captureID: captureID
            )
            return .empty
        }

        let env = _currentEnvironmentValues(for: ctx)
        var labelNode = ctx.buildChild(label)
        if role == .destructive {
            labelNode = .style(fg: .red, bg: nil, child: labelNode)
        }

        guard env.isEnabled else {
            return _applyControlPadding(_disabledButtonNode(label: labelNode), env: env)
        }

        guard _UIRuntime._hitTestingEnabled else {
            return _applyControlPadding(labelNode, env: env)
        }

        let controlPath = ctx.path
        let isFocused = runtime._isFocused(path: controlPath)
        let id = runtime._registerAction({
            runtime._setFocus(path: controlPath)
            action()
        }, path: actionScopePath)
        runtime._registerFocusable(path: controlPath, activate: id)

        let node: _VNode = {
            switch env.buttonStyleKind {
            case .plain:
                let plainLabel: _VNode = isFocused
                    ? .stack(axis: .horizontal, spacing: 0, children: [.text("> "), labelNode])
                    : labelNode
                return .tapTarget(id: id, child: plainLabel)
            case .borderedProminent, .primaryFill:
                let tint = env.tint ?? (role == .destructive ? .red : .accentColor)
                let prominentLabel: _VNode = .style(fg: Color.white, bg: tint, child: labelNode)
                return .button(id: id, isFocused: isFocused, label: prominentLabel)
            case .automatic, .bordered:
                return .button(id: id, isFocused: isFocused, label: labelNode)
            }
        }()
        return _applyControlPadding(node, env: env)
    }
}

public struct Label<Title: View, Icon: View>: View, _PrimitiveView {
    public typealias Body = Never

    let title: Title
    let icon: Icon

    public init(@ViewBuilder title: () -> Title, @ViewBuilder icon: () -> Icon) {
        self.title = title()
        self.icon = icon()
    }

    public init(_ title: String, systemImage: String) where Title == Text, Icon == Image {
        self.title = Text(title)
        self.icon = Image(systemName: systemImage)
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let env = _currentEnvironmentValues(for: ctx)
        let iconNode = ctx.buildChild(icon)
        let titleNode = ctx.buildChild(title)
        if env.labelStyleKind == .iconOnly {
            return iconNode
        }
        return .stack(axis: .horizontal, spacing: 1, children: [iconNode, titleNode])
    }
}

public struct Toggle<Label: View>: View, _PrimitiveView {
    public typealias Body = Never

    let isOn: Binding<Bool>
    let label: Label
    let actionScopePath: [Int]

    public init(isOn: Binding<Bool>, @ViewBuilder label: () -> Label) {
        self.isOn = isOn
        self.label = label()
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    public init(_ title: String, isOn: Binding<Bool>) where Label == Text {
        self.isOn = isOn
        self.label = Text(title)
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let env = _currentEnvironmentValues(for: ctx)
        let labelNode = ctx.buildChild(label)

        guard env.isEnabled, _UIRuntime._hitTestingEnabled else {
            return _applyControlPadding(_disabledToggleNode(label: labelNode, isOn: isOn.wrappedValue), env: env)
        }

        let controlPath = ctx.path
        let isFocused = runtime._isFocused(path: controlPath)
        let id = runtime._registerAction({
            runtime._setFocus(path: controlPath)
            isOn.wrappedValue.toggle()
        }, path: actionScopePath)
        runtime._registerFocusable(path: controlPath, activate: id)

        let node: _VNode = {
            if env.toggleStyleKind == .switch {
                let stateNode = _VNode.style(
                    fg: isOn.wrappedValue ? .white : .secondary,
                    bg: isOn.wrappedValue ? (env.tint ?? .accentColor) : nil,
                    child: .text(isOn.wrappedValue ? " ON " : " OFF ")
                )
                let body = _VNode.stack(axis: .horizontal, spacing: 1, children: [labelNode, stateNode])
                if isFocused {
                    return .tapTarget(id: id, child: .stack(axis: .horizontal, spacing: 0, children: [.text("> "), body]))
                }
                return .tapTarget(id: id, child: body)
            }
            return .toggle(id: id, isFocused: isFocused, isOn: isOn.wrappedValue, label: labelNode)
        }()
        return _applyControlPadding(node, env: env)
    }
}

public struct TextField: View, _PrimitiveView {
    public typealias Body = Never

    let placeholder: String
    let text: Binding<String>
    let actionScopePath: [Int]

    public init(_ placeholder: String, text: Binding<String>) {
        self.placeholder = placeholder
        self.text = text
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    public init(_ placeholder: String, text: Binding<String>, prompt: Text?) {
        self.placeholder = prompt?.content ?? placeholder
        self.text = text
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let env = _currentEnvironmentValues(for: ctx)
        let controlPath = ctx.path
        let isFocused = runtime._isFocused(path: controlPath)
        var cursor = runtime._getTextCursor(path: controlPath)
        let keyboardType = env.textInputKeyboardType
        let capitalization = env.textInputAutocapitalization
        let contentType = env.textContentType
        let autocorrectionDisabled = env.autocorrectionDisabled ?? true

        guard env.isEnabled, _UIRuntime._hitTestingEnabled else {
            return _applyControlPadding(_disabledTextFieldNode(placeholder: placeholder, text: text.wrappedValue), env: env)
        }

        let id = runtime._registerAction({
            runtime._ensureTextCursorAtEndIfUnset(path: controlPath, text: text.wrappedValue)
            runtime._setFocus(path: controlPath)
        }, path: actionScopePath)
        runtime._registerFocusable(path: controlPath, activate: id)

        runtime._registerTextEditor(path: controlPath, _TextEditor(handle: { ev in
            var scalars = Array(text.wrappedValue.unicodeScalars)
            cursor = min(max(0, cursor), scalars.count)

            func save() {
                text.wrappedValue = String(String.UnicodeScalarView(scalars))
                runtime._setTextCursor(path: controlPath, cursor)
            }

            func isURLField() -> Bool {
                keyboardType == .URL || contentType == .URL
            }

            func shouldUppercase(_ scalar: UnicodeScalar) -> Bool {
                guard !isURLField() else { return false }
                guard let capitalization else { return false }
                guard CharacterSet.lowercaseLetters.contains(scalar) || CharacterSet.uppercaseLetters.contains(scalar) else { return false }
                switch capitalization {
                case .never:
                    return false
                case .characters:
                    return true
                case .words:
                    guard cursor > 0 else { return true }
                    return CharacterSet.whitespacesAndNewlines.contains(scalars[cursor - 1])
                case .sentences:
                    guard cursor > 0 else { return true }
                    let previous = scalars[cursor - 1]
                    if CharacterSet.whitespacesAndNewlines.contains(previous) {
                        if cursor == 1 { return true }
                        let prior = scalars[cursor - 2]
                        return prior == UnicodeScalar(".") || prior == UnicodeScalar("!") || prior == UnicodeScalar("?")
                    }
                    return previous == UnicodeScalar(".") || previous == UnicodeScalar("!") || previous == UnicodeScalar("?")
                }
            }

            switch ev {
            case .left:
                cursor = max(0, cursor - 1)
                runtime._setTextCursor(path: controlPath, cursor)
            case .right:
                cursor = min(scalars.count, cursor + 1)
                runtime._setTextCursor(path: controlPath, cursor)
            case .home:
                cursor = 0
                runtime._setTextCursor(path: controlPath, cursor)
            case .end:
                cursor = scalars.count
                runtime._setTextCursor(path: controlPath, cursor)
            case .killToEnd:
                guard cursor < scalars.count else { return }
                scalars.removeSubrange(cursor..<scalars.count)
                save()
            case .backspace:
                guard cursor > 0, !scalars.isEmpty else { return }
                scalars.remove(at: cursor - 1)
                cursor -= 1
                save()
            case .delete:
                guard cursor < scalars.count else { return }
                scalars.remove(at: cursor)
                save()
            case .char(let codepoint):
                guard var scalar = UnicodeScalar(codepoint) else { return }
                let v = scalar.value
                guard v >= 32 && v != 127 else { return }
                if isURLField() {
                    if CharacterSet.whitespacesAndNewlines.contains(scalar) { return }
                    if let lower = UnicodeScalar(String(scalar).lowercased()) { scalar = lower }
                }
                if shouldUppercase(scalar), let upper = UnicodeScalar(String(scalar).uppercased()) {
                    scalar = upper
                }
                scalars.insert(scalar, at: cursor)
                cursor += 1
                if !autocorrectionDisabled, scalar == UnicodeScalar(" "), cursor >= 4 {
                    let window = String(String.UnicodeScalarView(Array(scalars[(cursor - 4)..<(cursor - 1)])))
                    if window == "teh" {
                        scalars.replaceSubrange((cursor - 4)..<(cursor - 1), with: [UnicodeScalar("t"), UnicodeScalar("h"), UnicodeScalar("e")].compactMap { $0 })
                    }
                }
                save()
            }
        }))

        return _applyControlPadding(
            .textField(
                id: id,
                placeholder: placeholder,
                text: text.wrappedValue,
                cursor: runtime._getTextCursor(path: controlPath),
                isFocused: isFocused,
                style: env.textFieldStyleKind
            ),
            env: env
        )
    }
}

public struct SecureField: View, _PrimitiveView {
    public typealias Body = Never

    let placeholder: String
    let text: Binding<String>

    public init(_ placeholder: String, text: Binding<String>) {
        self.placeholder = placeholder
        self.text = text
    }

    public init(_ placeholder: String, text: Binding<String>, prompt: Text?) {
        self.placeholder = prompt?.content ?? placeholder
        self.text = text
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let node = ctx.buildChild(TextField(placeholder, text: text))
        if case .textField(let id, let placeholder, _, let cursor, let isFocused, let style) = node {
            let masked = String(repeating: "•", count: text.wrappedValue.count)
            return .textField(id: id, placeholder: placeholder, text: masked, cursor: cursor, isFocused: isFocused, style: style)
        }
        return node
    }
}

public struct Picker<SelectionValue: Hashable>: View, _PrimitiveView {
    public typealias Body = Never

    let selection: Binding<SelectionValue>
    let title: String
    let options: [(SelectionValue, String)]?
    let content: AnyView?
    let actionScopePath: [Int]

    public init(_ title: String, selection: Binding<SelectionValue>, options: [(SelectionValue, String)]) {
        self.title = title
        self.selection = selection
        self.options = options
        self.content = nil
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    public init<Content: View>(_ title: String, selection: Binding<SelectionValue>, @ViewBuilder content: () -> Content) {
        self.title = title
        self.selection = selection
        self.options = nil
        self.content = AnyView(content())
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let runtime = ctx.runtime
        let env = _currentEnvironmentValues(for: ctx)
        let controlPath = ctx.path

        let values: [SelectionValue]
        let labelsText: [String]
        if let options = options {
            values = options.map { $0.0 }
            labelsText = options.map { $0.1 }
        } else if let content {
            let node = ctx.buildChild(content)
            let collected = _collectTaggedPickerOptions(node: node, valueType: SelectionValue.self)
            values = collected.values
            labelsText = collected.labels
        } else {
            values = []
            labelsText = []
        }

        let safeValues = values.isEmpty ? [selection.wrappedValue] : values
        let safeLabels = labelsText.isEmpty ? [String(describing: selection.wrappedValue)] : labelsText
        let selectedIndex = safeValues.firstIndex(of: selection.wrappedValue) ?? 0
        let isExpanded = env.isEnabled && runtime._isPickerExpanded(path: controlPath)

        // Paths for focusability inside the picker: header is 0, options are 1...N.
        let headerPath = controlPath + [0]

        guard env.isEnabled, _UIRuntime._hitTestingEnabled else {
            let valueText = (selectedIndex < safeLabels.count) ? safeLabels[selectedIndex] : String(describing: selection.wrappedValue)
            return _applyControlPadding(_disabledMenuNode(title: title, value: valueText), env: env)
        }

        if env.pickerStyleKind == .segmented {
            var segmentNodes: [_VNode] = []
            segmentNodes.reserveCapacity(safeValues.count)
            for (idx, value) in safeValues.enumerated() {
                let optionPath = controlPath + [1 + idx]
                let optionID = runtime._registerAction({
                    runtime._setFocus(path: optionPath)
                    selection.wrappedValue = value
                }, path: actionScopePath)
                runtime._registerFocusable(path: optionPath, activate: optionID)
                let optionLabel = (idx < safeLabels.count) ? safeLabels[idx] : String(describing: value)
                let labelNode: _VNode = value == selection.wrappedValue
                    ? .style(fg: Color.white, bg: env.tint ?? .accentColor, child: .text(optionLabel))
                    : .text(optionLabel)
                segmentNodes.append(.button(id: optionID, isFocused: runtime._isFocused(path: optionPath), label: labelNode))
            }
            return _applyControlPadding(
                .stack(axis: .horizontal, spacing: 1, children: segmentNodes),
                env: env
            )
        }

        // Header button.
        let valueText = (selectedIndex < safeLabels.count) ? safeLabels[selectedIndex] : String(describing: selection.wrappedValue)
        let headerIsFocused = runtime._isFocused(path: headerPath)
        let toggleExpandedID = runtime._registerAction({
            runtime._setFocus(path: headerPath)
            if runtime._isPickerExpanded(path: controlPath) {
                runtime._closePicker(path: controlPath)
            } else {
                runtime._openPicker(path: controlPath)
                let preferred = min(max(0, selectedIndex), max(0, safeValues.count - 1))
                runtime._setFocus(path: controlPath + [1 + preferred])
            }
        }, path: actionScopePath)
        runtime._registerFocusable(path: headerPath, activate: toggleExpandedID)

        // Dropdown options as buttons beneath.
        var items: [(id: _ActionID, isSelected: Bool, isFocused: Bool, label: String)] = []
        if isExpanded {
            items.reserveCapacity(safeValues.count)
            for (idx, value) in safeValues.enumerated() {
                let optionPath = controlPath + [1 + idx]
                let optionIsFocused = runtime._isFocused(path: optionPath)
                let optionID = runtime._registerAction({
                    runtime._setFocus(path: optionPath)
                    selection.wrappedValue = value
                    runtime._closePicker(path: controlPath)
                    runtime._setFocus(path: headerPath)
                }, path: actionScopePath)
                runtime._registerFocusable(path: optionPath, activate: optionID)
                let label = (idx < safeLabels.count) ? safeLabels[idx] : String(describing: value)
                items.append((id: optionID, isSelected: value == selection.wrappedValue, isFocused: optionIsFocused, label: label))
            }
        }

        return _applyControlPadding(
            .menu(
                id: toggleExpandedID,
                isFocused: headerIsFocused,
                isExpanded: isExpanded,
                title: title,
                value: valueText,
                items: items
            ),
            env: env
        )
    }
}

public struct Menu<Content: View, Label: View>: View, _PrimitiveView {
    public typealias Body = Never

    let content: Content
    let label: Label
    let actionScopePath: [Int]

    public init(@ViewBuilder content: () -> Content, @ViewBuilder label: () -> Label) {
        self.content = content()
        self.label = label()
        self.actionScopePath = _UIRuntime._currentPath ?? []
    }

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        let env = _currentEnvironmentValues(for: ctx)
        guard env.isEnabled else {
            return _applyControlPadding(_disabledButtonNode(label: ctx.buildChild(label)), env: env)
        }
        guard _UIRuntime._hitTestingEnabled else {
            return _applyControlPadding(ctx.buildChild(label), env: env)
        }

        let runtime = ctx.runtime
        let controlPath = ctx.path
        let isExpanded = runtime._isPickerExpanded(path: controlPath)

        // Paths for focusability: header is 0, items are 1...N.
        let headerPath = controlPath + [0]
        let headerIsFocused = runtime._isFocused(path: headerPath)

        let labelText: String = {
            var childCtx = _BuildContext(runtime: runtime, path: headerPath, nextChildIndex: 0)
            let node = _BuildContext.withRuntime(runtime, path: childCtx.path) {
                OmniUICore._makeNode(label, &childCtx)
            }
            let t = _menuLabelText(from: node)
            return t.isEmpty ? "Menu" : t
        }()

        let toggleExpandedID = runtime._registerAction({
            runtime._setFocus(path: headerPath)
            if runtime._isPickerExpanded(path: controlPath) {
                runtime._closePicker(path: controlPath)
            } else {
                runtime._openPicker(path: controlPath)
                runtime._setFocus(path: controlPath + [1])
            }
        }, path: actionScopePath)
        runtime._registerFocusable(path: headerPath, activate: toggleExpandedID)

        var items: [(id: _ActionID, isSelected: Bool, isFocused: Bool, label: String)] = []
        if isExpanded {
            // Collect nested Buttons into a list of menu items.
            let captureID = runtime._beginMenuCapture()
            _UIRuntime.$_currentMenuCaptureID.withValue(captureID) {
                var contentCtx = _BuildContext(runtime: runtime, path: controlPath + [1], nextChildIndex: 0)
                _ = _BuildContext.withRuntime(runtime, path: contentCtx.path) {
                    OmniUICore._makeNode(content, &contentCtx)
                }
            }
            let captured = runtime._endMenuCapture(captureID)

            items.reserveCapacity(captured.count)
            for (idx, entry) in captured.enumerated() {
                let optionPath = controlPath + [1 + idx]
                let optionIsFocused = runtime._isFocused(path: optionPath)
                let optionID = runtime._registerAction({
                    runtime._setFocus(path: optionPath)
                    runtime._closePicker(path: controlPath)
                    runtime._setFocus(path: headerPath)
                    runtime._invokeCapturedMenuItem(entry)
                }, path: entry.actionScopePath)
                runtime._registerFocusable(path: optionPath, activate: optionID)
                items.append((id: optionID, isSelected: false, isFocused: optionIsFocused, label: entry.label))
            }
        }

        return _applyControlPadding(
            .menu(
                id: toggleExpandedID,
                isFocused: headerIsFocused,
                isExpanded: isExpanded,
                title: "",
                value: labelText,
                items: items
            ),
            env: env
        )
    }
}

private func _collectTaggedPickerOptions<T: Hashable>(node: _VNode, valueType: T.Type) -> (values: [T], labels: [String]) {
    var values: [T] = []
    var labels: [String] = []

    func labelText(_ n: _VNode) -> String? {
        switch n {
        case .text(let s):
            return s
        case .textStyled(_, let child):
            return labelText(child)
        case .style(_, _, let child):
            return labelText(child)
        case .contentShapeRect(let child):
            return labelText(child)
        case .clip(_, let child):
            return labelText(child)
        case .shadow(let child, _, _, _, _):
            return labelText(child)
        case .group(let nodes):
            // Prefer first text we find.
            for c in nodes {
                if let t = labelText(c) { return t }
            }
            return nil
        case .stack(_, _, let children):
            for c in children {
                if let t = labelText(c) { return t }
            }
            return nil
        case .zstack(let children):
            for c in children {
                if let t = labelText(c) { return t }
            }
            return nil
        default:
            return nil
        }
    }

    func walk(_ n: _VNode) {
        switch n {
        case .tagged(let v, let label):
            if let tv = v.base as? T {
                values.append(tv)
                labels.append(labelText(label) ?? String(describing: tv))
            }
        case .contentShapeRect(let child):
            walk(child)
        case .clip(_, let child):
            walk(child)
        case .shadow(let child, _, _, _, _):
            walk(child)
        case .group(let nodes):
            for c in nodes { walk(c) }
        case .stack(_, _, let children):
            for c in children { walk(c) }
        case .zstack(let children):
            for c in children { walk(c) }
        case .background(let child, let bg):
            // Picker content doesn't use this typically; but walk both to be safe.
            walk(bg)
            walk(child)
        case .overlay(let child, let ov):
            walk(child)
            walk(ov)
        case .textStyled(_, let child):
            walk(child)
        case .style(_, _, let child):
            walk(child)
        case .hover(_, let child):
            walk(child)
        case .offset(_, _, let child):
            walk(child)
        case .opacity(_, let child):
            walk(child)
        case .gradient:
            break
        default:
            break
        }
    }

    walk(node)
    return (values, labels)
}

public extension View {
    func padding(_ amount: Int = 1) -> some View {
        _EdgePadding(content: AnyView(self), top: amount, leading: amount, bottom: amount, trailing: amount)
    }
}


private func _formattedDatePickerDate(_ date: Date, style: _DatePickerStyleKind) -> String {
    switch style {
    case .graphical:
        return date.formatted(date: .complete, time: .omitted)
    case .compact:
        return date.formatted(date: .numeric, time: .omitted)
    case .field, .stepperField:
        return date.formatted(date: .abbreviated, time: .omitted)
    case .automatic:
        return date.formatted(date: .abbreviated, time: .omitted)
    }
}


private func _currentEnvironmentValues(for ctx: _BuildContext) -> EnvironmentValues {
    _UIRuntime._currentEnvironment ?? ctx.runtime._baseEnvironment
}

private func _applyTextEnvironment(content: String, env: EnvironmentValues) -> _VNode {
    let rawLines = _wrapTextContent(content, lineLimit: env.lineLimit)
    let aligned = _alignTextLines(rawLines, alignment: env.multilineTextAlignment)
    let baseNode: _VNode
    if aligned.count <= 1 {
        baseNode = .text(aligned.first ?? "")
    } else {
        baseNode = .stack(axis: .vertical, spacing: 0, children: aligned.map(_VNode.text))
    }

    var node = baseNode
    if env.textSelectionEnabled {
        node = .textStyled(style: .underline, child: node)
    }
    guard let font = env.font else { return node }
    let lowercased = font.name.lowercased()
    if lowercased.contains("large") || lowercased.contains("title") || lowercased.contains("headline") || lowercased.contains("bold") || lowercased.contains("heavy") || lowercased.contains("black") || lowercased.contains("semibold") {
        node = .textStyled(style: .bold, child: node)
    }
    if lowercased.contains("caption") || lowercased.contains("subheadline") {
        node = .style(fg: .secondary, bg: nil, child: node)
    }
    return node
}

private func _wrapTextContent(_ content: String, lineLimit: Int?) -> [String] {
    let raw = content.split(separator: "\n", omittingEmptySubsequences: false).map(String.init)
    let limit = lineLimit ?? Int.max
    if raw.isEmpty { return [""] }
    return Array(raw.prefix(limit))
}

private func _alignTextLines(_ lines: [String], alignment: TextAlignment) -> [String] {
    guard lines.count > 1 else { return lines }
    let width = lines.map { $0.count }.max() ?? 0
    guard width > 0 else { return lines }
    return lines.map { line in
        let pad = max(0, width - line.count)
        switch alignment {
        case .leading:
            return line
        case .center:
            return String(repeating: " ", count: pad / 2) + line
        case .trailing:
            return String(repeating: " ", count: pad) + line
        }
    }
}

private func _controlPaddingAmount(for size: ControlSize) -> Int {
    switch size {
    case .mini, .small, .regular:
        return 0
    case .large:
        return 1
    }
}

private func _applyControlPadding(_ node: _VNode, env: EnvironmentValues) -> _VNode {
    let pad = _controlPaddingAmount(for: env.controlSize)
    guard pad > 0 else { return node }
    return .edgePadding(top: 0, leading: pad, bottom: 0, trailing: pad, child: node)
}

private func _disabledButtonNode(label: _VNode) -> _VNode {
    .style(
        fg: .secondary,
        bg: nil,
        child: .stack(axis: .horizontal, spacing: 1, children: [.text("["), label, .text("]")])
    )
}

private func _disabledToggleNode(label: _VNode, isOn: Bool) -> _VNode {
    .style(
        fg: .secondary,
        bg: nil,
        child: .stack(axis: .horizontal, spacing: 1, children: [.text(isOn ? "[x]" : "[ ]"), label])
    )
}

private func _disabledTextFieldNode(placeholder: String, text: String) -> _VNode {
    let display = text.isEmpty ? placeholder : text
    return .style(fg: .secondary, bg: nil, child: .text("[\(display)]"))
}

private func _disabledMenuNode(title: String, value: String) -> _VNode {
    let display = title.isEmpty ? value : "\(title): \(value)"
    return .style(fg: .secondary, bg: nil, child: .text("[ \(display) v ]"))
}

// Int-based padding lives here for convenience; edge-based padding is implemented in `Modifiers.swift`.
private struct _EdgePadding: View, _PrimitiveView {
    typealias Body = Never
    let content: AnyView
    let top: Int
    let leading: Int
    let bottom: Int
    let trailing: Int

    func _makeNode(_ ctx: inout _BuildContext) -> _VNode {
        .edgePadding(top: top, leading: leading, bottom: bottom, trailing: trailing, child: ctx.buildChild(content))
    }
}

func _flatten(_ node: _VNode) -> [_VNode] {
    switch node {
    case .empty:
        return []
    case .group(let nodes):
        return nodes.flatMap(_flatten)
    case .onDelete(_, _, let child):
        return _flatten(child)
    case .identified:
        // Preserve identity wrappers so `ScrollViewReader.scrollTo(...)` can resolve targets.
        return [node]
    default:
        return [node]
    }
}

private func _menuLabelText(from node: _VNode) -> String {
    var parts: [String] = []

    func walk(_ n: _VNode) {
        switch n {
        case .text(let s):
            let t = s.trimmingCharacters(in: .whitespacesAndNewlines)
            if !t.isEmpty { parts.append(t) }
        case .image:
            // Ignore icon-only nodes; the renderers already handle images.
            break
        case .textStyled(_, let child):
            walk(child)
        case .style(_, _, let child):
            walk(child)
        case .hover(_, let child):
            walk(child)
        case .contentShapeRect(let child):
            walk(child)
        case .clip(_, let child):
            walk(child)
        case .shadow(let child, _, _, _, _):
            walk(child)
        case .background(let child, _):
            walk(child)
        case .overlay(let child, _):
            walk(child)
        case .frame(_, _, _, _, _, _, let child):
            walk(child)
        case .edgePadding(_, _, _, _, let child):
            walk(child)
        case .tagged(_, let label):
            walk(label)
        case .group(let nodes):
            for c in nodes { walk(c) }
        case .stack(_, _, let children):
            for c in children { walk(c) }
        case .zstack(let children):
            for c in children { walk(c) }
        default:
            break
        }
    }

    walk(node)
    return parts.joined(separator: " ")
}
