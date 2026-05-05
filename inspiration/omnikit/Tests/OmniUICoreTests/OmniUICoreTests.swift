import Testing
import Foundation
import OmniUICore

struct CounterView: View {
    @State private var count: Int = 0
    var body: some View {
        VStack(spacing: 1) {
            Text("Count: \(count)")
            Button("Inc") { count += 1 }
        }
        .padding(1)
    }
}

@Test func debugSnapshot_click_increments_state() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 30, height: 6)

    let s0 = runtime.debugRender(CounterView(), size: size)
    #expect(s0.text.contains("Count: 0"))

    // Button is rendered as "[ Inc ]" at roughly x=1,y=3 (after padding + text lines).
    s0.click(x: 2, y: 3)

    let s1 = runtime.debugRender(CounterView(), size: size)
    #expect(s1.text.contains("Count: 1"))
}

struct TextFieldView: View {
    @State private var text: String = ""
    var body: some View {
        VStack(spacing: 1) {
            TextField("placeholder", text: $text)
            Text("Value: \(text)")
        }
    }
}

@Test func debugSnapshot_textField_focus_and_typing_updates_state() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 30, height: 4)

    let s0 = runtime.debugRender(TextFieldView(), size: size)
    #expect(s0.text.contains("placeholder"))

    // Click the text field (top row).
    s0.click(x: 1, y: 0)
    s0.type("abc")

    let s1 = runtime.debugRender(TextFieldView(), size: size)
    #expect(s1.text.contains("Value: abc"))
}

@Test func debugSnapshot_textField_readline_key_events_work() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 40, height: 4)

    let s0 = runtime.debugRender(TextFieldView(), size: size)
    s0.click(x: 1, y: 0)
    s0.type("abcdef")

    runtime._handleKey(.left)
    runtime._handleKey(.left)
    runtime._handleKey(.killToEnd)
    runtime._handleKey(.home)
    runtime._handleKey(.char("X".unicodeScalars.first!.value))
    runtime._handleKey(.end)
    runtime._handleKey(.char("Z".unicodeScalars.first!.value))

    let s1 = runtime.debugRender(TextFieldView(), size: size)
    #expect(s1.text.contains("Value: XabcdZ"))
}

struct SimplePickerView: View {
    enum Choice: String, Hashable {
        case a = "A"
        case b = "B"
        case c = "C"
    }

    @State private var choice: Choice = .a

    var body: some View {
        VStack(spacing: 0) {
            Picker("Choice", selection: $choice, options: [(.a, "A"), (.b, "B"), (.c, "C")])
        }
    }
}

@Test func debugSnapshot_picker_dropdown_click_selects_option() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 20, height: 6)

    // Collapsed.
    let s0 = runtime.debugRender(SimplePickerView(), size: size)
    #expect(s0.text.contains("Choice: A"))

    // Open dropdown.
    s0.click(x: 1, y: 0)
    let s1 = runtime.debugRender(SimplePickerView(), size: size)
    #expect(s1.text.contains("Choice: A"))
    #expect(s1.text.contains("*A") || s1.text.contains("* A"))

    // Click the "C" option (options are rendered on subsequent rows).
    // Menu layout:
    // y=0 header
    // y=1 top border
    // y=2 option A
    // y=3 option B
    // y=4 option C
    s1.click(x: 2, y: 4)
    let s2 = runtime.debugRender(SimplePickerView(), size: size)
    #expect(s2.text.contains("Choice: C"))
}

struct PickerOverlayView: View {
    enum Choice: String, Hashable {
        case a = "A"
        case b = "B"
        case c = "C"
    }

    @State private var choice: Choice = .a

    var body: some View {
        VStack(spacing: 0) {
            Picker("Choice", selection: $choice, options: [(.a, "A"), (.b, "B"), (.c, "C")])
            Text("Tip: this should stay in-place")
        }
    }
}

@Test func debugSnapshot_picker_dropdown_is_overlay_not_layout() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 40, height: 6)

    let s0 = runtime.debugRender(PickerOverlayView(), size: size)
    #expect(s0.text.contains("Tip: this should stay in-place"))

    // Expand dropdown. It should draw over the Tip line, not push it down.
    s0.click(x: 1, y: 0)
    let s1 = runtime.debugRender(PickerOverlayView(), size: size)
    #expect(!s1.text.contains("Tip: this should stay in-place"))

    // Collapse dropdown. Tip should become visible again.
    s1.click(x: 1, y: 0)
    let s2 = runtime.debugRender(PickerOverlayView(), size: size)
    #expect(s2.text.contains("Tip: this should stay in-place"))
}

struct ListRenderView: View {
    var body: some View {
        List(0..<5, id: \.self) { i in
            Text("Row \(i)")
        }
    }
}

@Test func debugSnapshot_list_renders_rows() async throws {
    let runtime = _UIRuntime()
    let s0 = runtime.debugRender(ListRenderView(), size: _Size(width: 20, height: 10))
    #expect(s0.text.contains("Row 0"))
    #expect(s0.text.contains("Row 4"))
}

@Test func debugSnapshot_kitchensink_contains_list_row() async throws {
    // Mirrors the demo: nested ScrollView + List.
    struct Sink: View {
        @State var pickedRow = 0
        var body: some View {
            ScrollView {
                VStack(spacing: 1) {
                    Text("Header")
                    Text("List:")
                    List(0..<12, id: \.self) { i in
                        HStack(spacing: 1) {
                            Text("Row \(i)")
                            Spacer()
                            Button("Pick") { pickedRow = i }
                        }
                    }
                }
                .padding(1)
            }
        }
    }

    let runtime = _UIRuntime()
    let s0 = runtime.debugRender(Sink(), size: _Size(width: 80, height: 24))
    #expect(s0.text.contains("Row 0"))
}

@Test func debugSnapshot_list_row_button_updates_parent_state() async throws {
    struct V: View {
        @State var pickedRow: Int = 0
        var body: some View {
            VStack(spacing: 1) {
                Text("picked: \(pickedRow)")
                List(0..<2, id: \.self) { i in
                    HStack(spacing: 1) {
                        Text("Row \(i)")
                        Spacer()
                        Button("Pick") { pickedRow = i }
                    }
                }
            }
            .padding(1)
        }
    }

    func findPickButton(_ snap: DebugSnapshot, occurrence: Int) -> (x: Int, y: Int)? {
        var seen = 0
        for (y, line) in snap.lines.enumerated() {
            let hay = Array(line)
            let needle = Array("[ Pick ]")
            if hay.count >= needle.count {
                for x0 in 0...(hay.count - needle.count) {
                    var ok = true
                    for i in 0..<needle.count {
                        if hay[x0 + i] != needle[i] { ok = false; break }
                    }
                    if ok {
                        if seen == occurrence {
                            // Click inside the button.
                            return (x: x0 + 2, y: y)
                        }
                        seen += 1
                        break
                    }
                }
            }
        }
        return nil
    }

    let runtime = _UIRuntime()
    let s0 = runtime.debugRender(V(), size: _Size(width: 50, height: 12))
    #expect(s0.text.contains("picked: 0"))

    // Click the second row's Pick button (occurrence 1) so the value actually changes.
    guard let p = findPickButton(s0, occurrence: 1) else {
        #expect(Bool(false), "Could not find Pick button in snapshot")
        return
    }
    s0.click(x: p.x, y: p.y)

    let s1 = runtime.debugRender(V(), size: _Size(width: 50, height: 12))
    #expect(s1.text.contains("picked: 1"))
}

@Test func swiftDataCompat_query_reflects_modelContext_inserts() async throws {
    final class M {
        var id: Int
        init(id: Int) { self.id = id }
    }

    struct V: View {
        @Environment(\.modelContext) private var modelContext
        @Query(sort: \M.id, order: .reverse) private var models: [M]
        @State private var seeded: Bool = false

        var body: some View {
            VStack(spacing: 1) {
                Text("count: \(models.count)")
                Text("first: \(models.first?.id ?? -1)")
            }
            .onAppear {
                guard !seeded else { return }
                seeded = true
                modelContext.insert(M(id: 1))
                modelContext.insert(M(id: 2))
            }
        }
    }

    let runtime = _UIRuntime()
    let size = _Size(width: 30, height: 6)

    let s0 = runtime.debugRender(V(), size: size)
    #expect(s0.text.contains("count: 0"))

    let s1 = runtime.debugRender(V(), size: size)
    #expect(s1.text.contains("count: 2"))
    #expect(s1.text.contains("first: 2"))
}

struct TapGestureView: View {
    @State private var tapped: Int = 0

    var body: some View {
        VStack(spacing: 0) {
            Text("tapped: \(tapped)")
            Text("TapMe")
                .onTapGesture { tapped += 1 }
        }
    }
}

@Test func debugSnapshot_onTapGesture_click_updates_state() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 20, height: 4)

    let s0 = runtime.debugRender(TapGestureView(), size: size)
    #expect(s0.text.contains("tapped: 0"))

    // TapMe is on the second row at x≈0,y=1.
    s0.click(x: 0, y: 1)

    let s1 = runtime.debugRender(TapGestureView(), size: size)
    #expect(s1.text.contains("tapped: 1"))
}

struct MenuGestureView: View {
    @State private var picked: String = "-"

    var body: some View {
        VStack(spacing: 0) {
            Text("picked: \(picked)")
            Menu {
                Button("A") { picked = "A" }
                Button("B") { picked = "B" }
            } label: {
                Text("Menu")
            }
        }
    }
}

@Test func debugSnapshot_menu_click_selects_item() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 40, height: 8)

    // Collapsed.
    let s0 = runtime.debugRender(MenuGestureView(), size: size)
    #expect(s0.text.contains("picked: -"))

    // Open dropdown (Menu header is on y=1).
    s0.click(x: 1, y: 1)
    let s1 = runtime.debugRender(MenuGestureView(), size: size)

    // Click the second option ("B") in the dropdown.
    // Menu layout:
    // y=1 header
    // y=2 top border
    // y=3 option A
    // y=4 option B
    s1.click(x: 2, y: 4)

    let s2 = runtime.debugRender(MenuGestureView(), size: size)
    #expect(s2.text.contains("picked: B"))
}

@Test func keyboardShortcut_defaultAction_invokes_button_action() async throws {
    final class Box {
        var value: Int = 0
    }

    struct V: View {
        let box: Box
        var body: some View {
            Button("Inc") { box.value += 1 }
                .keyboardShortcut(.defaultAction)
        }
    }

    let box = Box()
    let runtime = _UIRuntime()
    _ = runtime.render(V(box: box), size: _Size(width: 20, height: 3))

    #expect(box.value == 0)
    #expect(runtime.invokeKeyboardShortcut(.return))
    #expect(box.value == 1)
}

@Test func keyboardShortcut_cancelAction_invokes_button_action() async throws {
    final class Box {
        var cancelled: Bool = false
    }

    struct V: View {
        let box: Box
        var body: some View {
            Button("Cancel") { box.cancelled = true }
                .keyboardShortcut(.cancelAction)
        }
    }

    let box = Box()
    let runtime = _UIRuntime()
    _ = runtime.render(V(box: box), size: _Size(width: 20, height: 3))

    #expect(!box.cancelled)
    #expect(runtime.invokeKeyboardShortcut(.escape))
    #expect(box.cancelled)
}

@Test func keyboardShortcut_action_runs_with_captured_environment() async throws {
    final class Box {
        var opened: URL? = nil
    }

    struct V: View {
        private static let openURLTestsURL: URL = {
            guard let url = URL(string: "https://example.com") else {
                preconditionFailure("Invalid OmniUICore openURL test URL")
            }
            return url
        }()

        let box: Box
        @Environment(\.openURL) private var openURL
        var body: some View {
            Button("Open") {
                _ = openURL(Self.openURLTestsURL)
            }
            .keyboardShortcut(.defaultAction)
        }
    }

    let box = Box()
    let runtime = _UIRuntime()
    let root = V(box: box)
        .environment(\.openURL, OpenURLAction({ url in box.opened = url; return .handled }))
    _ = runtime.render(root, size: _Size(width: 20, height: 3))

    #expect(box.opened == nil)
    #expect(runtime.invokeKeyboardShortcut(.return))
    #expect(box.opened?.absoluteString == "https://example.com")
}

@MainActor
@Test func task_runs_and_cancels_with_view_lifecycle() async throws {
    final class Box {
        var started: Int = 0
        var cancelled: Int = 0
    }

    struct WithTask: View {
        let box: Box
        var body: some View {
            Text("Hi")
                .task {
                    box.started += 1
                    do {
                        while true {
                            try await Task.sleep(nanoseconds: 1_000_000) // 1ms
                        }
                    } catch {
                        // Expected on cancellation.
                    }
                    box.cancelled += 1
                }
        }
    }

    let box = Box()
    let runtime = _UIRuntime()

    _ = runtime.render(WithTask(box: box), size: _Size(width: 10, height: 2))
    for _ in 0..<50 {
        if box.started == 1 { break }
        await Task.yield()
    }
    #expect(box.started == 1)

    // Remove the task from the tree; it should be cancelled on the next frame.
    _ = runtime.render(Text("Hi"), size: _Size(width: 10, height: 2))
    for _ in 0..<50 {
        if box.cancelled == 1 { break }
        await Task.yield()
    }
    #expect(box.cancelled == 1)
}

@Test func contentShape_rectangle_expands_button_hit_region() async throws {
    final class Box {
        var taps: Int = 0
    }

    struct V: View {
        let box: Box
        var body: some View {
            Button(action: { box.taps += 1 }) {
                HStack(spacing: 0) {
                    Text("Tap")
                }
                .contentShape(Rectangle())
            }
        }
    }

    let box = Box()
    let runtime = _UIRuntime()
    let s0 = runtime.debugRender(V(box: box), size: _Size(width: 20, height: 3))
    #expect(box.taps == 0)

    // Click far to the right of the visible "[ Tap ]" label; `contentShape(Rectangle())`
    // should make the whole row hit-testable.
    s0.click(x: 19, y: 0)
    let s1 = runtime.debugRender(V(box: box), size: _Size(width: 20, height: 3))
    _ = s1
    #expect(box.taps == 1)
}

@Test func runtime_needsRender_tracks_dirty_state_and_size() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 30, height: 6)

    #expect(runtime.needsRender(size: size))

    _ = runtime.render(CounterView(), size: size)
    #expect(!runtime.needsRender(size: size))

    let s0 = runtime.debugRender(CounterView(), size: size)
    s0.click(x: 2, y: 3)
    #expect(runtime.needsRender(size: size))

    _ = runtime.render(CounterView(), size: size)
    #expect(!runtime.needsRender(size: size))

    #expect(runtime.needsRender(size: _Size(width: 31, height: 6)))
}

final class _BuildCountBox {
    var left: Int = 0
    var center: Int = 0
    var right: Int = 0
}

struct _BuildProbe: View {
    let label: String
    let tick: () -> Void

    var body: some View {
        tick()
        return Text(label)
    }
}

struct _CenterCounterProbe: View {
    let box: _BuildCountBox
    @State private var value: Int = 0

    var body: some View {
        box.center += 1
        return VStack(spacing: 1) {
            Text("center: \(value)")
            Button("Inc") { value += 1 }
        }
    }
}

struct _TargetedInvalidationProbeView: View {
    let box: _BuildCountBox

    var body: some View {
        HStack(spacing: 2) {
            _BuildProbe(label: "left") { box.left += 1 }
            _CenterCounterProbe(box: box)
            _BuildProbe(label: "right") { box.right += 1 }
        }
    }
}

private func _findButton(_ snap: DebugSnapshot, title: String, occurrence: Int = 0) -> (x: Int, y: Int)? {
    var seen = 0
    let needle = Array("[ \(title) ]")
    for (y, line) in snap.lines.enumerated() {
        let hay = Array(line)
        guard hay.count >= needle.count else { continue }
        for x0 in 0...(hay.count - needle.count) {
            var ok = true
            for i in 0..<needle.count where hay[x0 + i] != needle[i] {
                ok = false
                break
            }
            if ok {
                if seen == occurrence {
                    return (x: x0 + 2, y: y)
                }
                seen += 1
                break
            }
        }
    }
    return nil
}

@Test func state_invalidation_rebuilds_only_affected_subtree() async throws {
    let runtime = _UIRuntime()
    let box = _BuildCountBox()
    let size = _Size(width: 60, height: 8)

    let s0 = runtime.debugRender(_TargetedInvalidationProbeView(box: box), size: size)
    #expect(box.left == 1)
    #expect(box.center == 1)
    #expect(box.right == 1)

    guard let p = _findButton(s0, title: "Inc") else {
        #expect(Bool(false), "Could not find Inc button")
        return
    }
    s0.click(x: p.x, y: p.y)

    let s1 = runtime.debugRender(_TargetedInvalidationProbeView(box: box), size: size)
    #expect(s1.text.contains("center: 1"))
    #expect(box.center >= 2)
    #expect(box.left == 1)
    #expect(box.right == 1)
}

struct _ScrollReaderProbeView: View {
    var body: some View {
        ScrollViewReader { proxy in
            VStack(spacing: 1) {
                Button("Jump") { proxy.scrollTo(35, anchor: .top) }
                ScrollView {
                    VStack(spacing: 0) {
                        ForEach(0..<50, id: \.self) { i in
                            Text("Row \(i)").id(i)
                        }
                    }
                }
                .frame(height: 6)
            }
        }
    }
}

@Test func scrollViewReader_scrollTo_moves_scroll_position() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 40, height: 10)

    let s0 = runtime.debugRender(_ScrollReaderProbeView(), size: size)
    #expect(s0.text.contains("Row 0"))

    guard let jump = _findButton(s0, title: "Jump") else {
        #expect(Bool(false), "Could not find Jump button")
        return
    }
    s0.click(x: jump.x, y: jump.y)

    let s1 = runtime.debugRender(_ScrollReaderProbeView(), size: size)
    if s1.text.contains("Row 35") {
        #expect(Bool(true))
    } else {
        // `scrollTo` requests may be applied at the end of a frame; verify the next frame.
        let s2 = runtime.debugRender(_ScrollReaderProbeView(), size: size)
        #expect(s2.text.contains("Row 35"))
    }
}

struct _TreeProbeNode: Identifiable {
    let id: String
    let title: String
    let children: [_TreeProbeNode]?
}

struct _HierarchicalListProbeView: View {
    let nodes: [_TreeProbeNode] = [
        _TreeProbeNode(
            id: "root",
            title: "Root",
            children: [
                _TreeProbeNode(
                    id: "child",
                    title: "Child",
                    children: [
                        _TreeProbeNode(id: "leaf", title: "Leaf", children: nil),
                    ]
                ),
            ]
        ),
    ]

    var body: some View {
        List(nodes, children: \.children) { node in
            Text(node.title)
        }
    }
}

@Test func list_children_renders_nested_nodes() async throws {
    let runtime = _UIRuntime()
    let s0 = runtime.debugRender(_HierarchicalListProbeView(), size: _Size(width: 40, height: 10))
    #expect(s0.text.contains("Root"))
    #expect(s0.text.contains("Child"))
    #expect(s0.text.contains("Leaf"))
}

struct _EditableListProbeView: View {
    @State private var items: [Int] = [10, 20, 30]

    var body: some View {
        VStack(spacing: 1) {
            EditButton()
            List {
                ForEach(items, id: \.self) { item in
                    Text("Item \(item)")
                }
                .onDelete { offsets in
                    for index in offsets.sorted(by: >) {
                        guard items.indices.contains(index) else { continue }
                        items.remove(at: index)
                    }
                }
            }
        }
    }
}

@Test func editButton_enables_onDelete_actions() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 50, height: 12)

    let s0 = runtime.debugRender(_EditableListProbeView(), size: size)
    #expect(!s0.text.contains("[ Del ]"))
    #expect(s0.text.contains("Item 10"))

    guard let edit = _findButton(s0, title: "Edit") else {
        #expect(Bool(false), "Could not find Edit button")
        return
    }
    s0.click(x: edit.x, y: edit.y)

    let s1 = runtime.debugRender(_EditableListProbeView(), size: size)
    #expect(s1.text.contains("[ Del ]"))

    guard let del = _findButton(s1, title: "Del", occurrence: 0) else {
        #expect(Bool(false), "Could not find Del button")
        return
    }
    s1.click(x: del.x, y: del.y)

    let s2 = runtime.debugRender(_EditableListProbeView(), size: size)
    #expect(!s2.text.contains("Item 10"))
    #expect(s2.text.contains("Item 20"))
}

final class _StableModelRecord {
    var id: Int
    init(id: Int) { self.id = id }
}

struct _StableModelContainerProbe: View {
    @Environment(\.modelContext) private var modelContext
    @Query(sort: \_StableModelRecord.id, order: .forward) private var records: [_StableModelRecord]
    @State private var seeded: Bool = false

    var body: some View {
        Text("count: \(records.count)")
            .onAppear {
                guard !seeded else { return }
                seeded = true
                modelContext.insert(_StableModelRecord(id: 1))
            }
    }
}

struct _OnAppearLifecycleProbeView: View {
    @State private var appearCount: Int = 0
    @State private var rerenderCount: Int = 0
    @State private var showChild: Bool = true

    var body: some View {
        VStack(spacing: 1) {
            Text("appear: \(appearCount)")
            Text("rerender: \(rerenderCount)")
            Button("Rerender") { rerenderCount += 1 }
            Button(showChild ? "Hide" : "Show") { showChild.toggle() }

            if showChild {
                Text("Child")
                    .onAppear { appearCount += 1 }
            }
        }
    }
}

@Test func modelContainer_for_keeps_context_stable_across_renders() async throws {
    let runtime = _UIRuntime()
    let root = _StableModelContainerProbe()
        .modelContainer(for: [_StableModelRecord.self], inMemory: true)
    let size = _Size(width: 40, height: 6)

    let s0 = runtime.debugRender(root, size: size)
    #expect(s0.text.contains("count: 0"))

    let s1 = runtime.debugRender(root, size: size)
    #expect(s1.text.contains("count: 1"))

    let s2 = runtime.debugRender(root, size: size)
    #expect(s2.text.contains("count: 1"))
}

@Test func onAppear_fires_once_for_a_stable_mount() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 30, height: 8)

    let s0 = runtime.debugRender(_OnAppearLifecycleProbeView(), size: size)
    #expect(s0.text.contains("appear: 0"))

    let s1 = runtime.debugRender(_OnAppearLifecycleProbeView(), size: size)
    #expect(s1.text.contains("appear: 1"))

    let s2 = runtime.debugRender(_OnAppearLifecycleProbeView(), size: size)
    #expect(s2.text.contains("appear: 1"))

    guard let rerender = _findButton(s2, title: "Rerender") else {
        #expect(Bool(false), "Could not find Rerender button")
        return
    }
    s2.click(x: rerender.x, y: rerender.y)

    let s3 = runtime.debugRender(_OnAppearLifecycleProbeView(), size: size)
    #expect(s3.text.contains("rerender: 1"))
    #expect(s3.text.contains("appear: 1"))
}

@Test func onAppear_fires_again_after_remount() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 30, height: 8)

    _ = runtime.debugRender(_OnAppearLifecycleProbeView(), size: size)
    let s1 = runtime.debugRender(_OnAppearLifecycleProbeView(), size: size)
    #expect(s1.text.contains("appear: 1"))

    guard let hide = _findButton(s1, title: "Hide") else {
        #expect(Bool(false), "Could not find Hide button")
        return
    }
    s1.click(x: hide.x, y: hide.y)

    let s2 = runtime.debugRender(_OnAppearLifecycleProbeView(), size: size)
    #expect(!s2.text.contains("Child"))
    #expect(s2.text.contains("appear: 1"))

    guard let show = _findButton(s2, title: "Show") else {
        #expect(Bool(false), "Could not find Show button")
        return
    }
    s2.click(x: show.x, y: show.y)

    let s3 = runtime.debugRender(_OnAppearLifecycleProbeView(), size: size)
    #expect(s3.text.contains("appear: 1"))

    let s4 = runtime.debugRender(_OnAppearLifecycleProbeView(), size: size)
    #expect(s4.text.contains("appear: 2"))
}

private struct _SheetItemProbeItem: Identifiable {
    let id: Int
}

struct _SheetItemProbeView: View {
    @State private var item: _SheetItemProbeItem? = nil

    var body: some View {
        VStack(spacing: 1) {
            Text("item: \(item?.id ?? -1)")
            Button("Show") {
                item = _SheetItemProbeItem(id: 1)
            }
        }
        .sheet(item: $item) { current in
            Text("Item \(current.id)")
        }
    }
}

@Test func sheet_item_presents_and_dismisses() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 40, height: 10)

    let s0 = runtime.debugRender(_SheetItemProbeView(), size: size)
    #expect(s0.text.contains("item: -1"))

    guard let show = _findButton(s0, title: "Show") else {
        #expect(Bool(false), "Could not find Show button")
        return
    }
    s0.click(x: show.x, y: show.y)

    let s1 = runtime.debugRender(_SheetItemProbeView(), size: size)
    #expect(s1.text.contains("Item 1"))
    #expect(s1.text.contains("item: 1"))

    guard let close = _findButton(s1, title: "Close") else {
        #expect(Bool(false), "Could not find Close button")
        return
    }
    s1.click(x: close.x, y: close.y)

    let s2 = runtime.debugRender(_SheetItemProbeView(), size: size)
    #expect(!s2.text.contains("Item 1"))
    #expect(s2.text.contains("item: -1"))
}

struct _PopoverProbeView: View {
    @State private var isPresented: Bool = false

    var body: some View {
        VStack(spacing: 1) {
            Text("popover: \(isPresented ? "open" : "closed")")
            Button("Show") { isPresented = true }
        }
        .popover(isPresented: $isPresented) {
            Text("Popover Body")
        }
    }
}

@Test func popover_isPresented_shows_and_dismisses() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 40, height: 10)

    let s0 = runtime.debugRender(_PopoverProbeView(), size: size)
    #expect(s0.text.contains("popover: closed"))

    guard let show = _findButton(s0, title: "Show") else {
        #expect(Bool(false), "Could not find Show button")
        return
    }
    s0.click(x: show.x, y: show.y)

    let s1 = runtime.debugRender(_PopoverProbeView(), size: size)
    #expect(s1.text.contains("Popover Body"))
    #expect(s1.text.contains("popover: open"))

    guard let close = _findButton(s1, title: "Close") else {
        #expect(Bool(false), "Could not find Close button")
        return
    }
    s1.click(x: close.x, y: close.y)

    let s2 = runtime.debugRender(_PopoverProbeView(), size: size)
    #expect(!s2.text.contains("Popover Body"))
    #expect(s2.text.contains("popover: closed"))
}

struct _ConfirmationDialogProbeView: View {
    @State private var isPresented: Bool = false
    @State private var picked: String = "-"

    var body: some View {
        VStack(spacing: 1) {
            Text("picked: \(picked)")
            Button("Show") { isPresented = true }
        }
        .confirmationDialog("Confirm", isPresented: $isPresented) {
            Button("One") { picked = "One" }
            Button("Two", role: .destructive) { picked = "Two" }
            Button("Cancel", role: .cancel) { picked = "Cancel" }
        } message: {
            Text("Choose")
        }
    }
}

@Test func confirmationDialog_captures_buttons_and_runs_selected_action() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 40, height: 12)

    let s0 = runtime.debugRender(_ConfirmationDialogProbeView(), size: size)
    #expect(s0.text.contains("picked: -"))

    guard let show = _findButton(s0, title: "Show") else {
        #expect(Bool(false), "Could not find Show button")
        return
    }
    s0.click(x: show.x, y: show.y)

    let s1 = runtime.debugRender(_ConfirmationDialogProbeView(), size: size)
    #expect(s1.text.contains("Confirm"))
    #expect(s1.text.contains("Choose"))

    guard let two = _findButton(s1, title: "Two") else {
        #expect(Bool(false), "Could not find Two button")
        return
    }
    s1.click(x: two.x, y: two.y)

    let s2 = runtime.debugRender(_ConfirmationDialogProbeView(), size: size)
    #expect(s2.text.contains("picked: Two"))
    #expect(!s2.text.contains("Choose"))
}

private struct _PopoverItemProbeItem: Identifiable {
    let id: Int
}

struct _PopoverItemProbeView: View {
    @State private var item: _PopoverItemProbeItem? = nil

    var body: some View {
        VStack(spacing: 1) {
            Text("popover item: \(item?.id ?? -1)")
            Button("Show") { item = _PopoverItemProbeItem(id: 7) }
        }
        .popover(item: $item) { current in
            Text("Popover Item \(current.id)")
        }
    }
}

@Test func popover_item_presents_and_dismisses() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 40, height: 10)

    let s0 = runtime.debugRender(_PopoverItemProbeView(), size: size)
    #expect(s0.text.contains("popover item: -1"))

    guard let show = _findButton(s0, title: "Show") else {
        #expect(Bool(false), "Could not find Show button")
        return
    }
    s0.click(x: show.x, y: show.y)

    let s1 = runtime.debugRender(_PopoverItemProbeView(), size: size)
    #expect(s1.text.contains("Popover Item 7"))
    #expect(s1.text.contains("popover item: 7"))

    guard let close = _findButton(s1, title: "Close") else {
        #expect(Bool(false), "Could not find Close button")
        return
    }
    s1.click(x: close.x, y: close.y)

    let s2 = runtime.debugRender(_PopoverItemProbeView(), size: size)
    #expect(!s2.text.contains("Popover Item 7"))
    #expect(s2.text.contains("popover item: -1"))
}

struct _NavigationDestinationBoolProbeView: View {
    @State private var isShowingDetail: Bool = false

    var body: some View {
        NavigationStack {
            VStack(spacing: 1) {
                Text("root")
                Button("Show") { isShowingDetail = true }
            }
            .navigationDestination(isPresented: $isShowingDetail) {
                Text("Bool Detail")
            }
        }
    }
}

@Test func navigationDestination_isPresented_pushes_and_pops() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 40, height: 10)

    let s0 = runtime.debugRender(_NavigationDestinationBoolProbeView(), size: size)
    #expect(s0.text.contains("root"))

    guard let show = _findButton(s0, title: "Show") else {
        #expect(Bool(false), "Could not find Show button")
        return
    }
    s0.click(x: show.x, y: show.y)

    let s1 = runtime.debugRender(_NavigationDestinationBoolProbeView(), size: size)
    #expect(s1.text.contains("Bool Detail"))
    #expect(s1.text.contains("[ Back ]"))

    guard let back = _findButton(s1, title: "Back") else {
        #expect(Bool(false), "Could not find Back button")
        return
    }
    s1.click(x: back.x, y: back.y)

    let s2 = runtime.debugRender(_NavigationDestinationBoolProbeView(), size: size)
    #expect(s2.text.contains("root"))
    #expect(!s2.text.contains("Bool Detail"))
}

private struct _NavigationDestinationItemProbeItem: Identifiable {
    let id: Int
}

struct _NavigationDestinationItemProbeView: View {
    @State private var selected: _NavigationDestinationItemProbeItem? = nil

    var body: some View {
        NavigationStack {
            VStack(spacing: 1) {
                Text("selected: \(selected?.id ?? -1)")
                Button("Show") { selected = _NavigationDestinationItemProbeItem(id: 5) }
            }
            .navigationDestination(item: $selected) { item in
                Text("Item Detail \(item.id)")
            }
        }
    }
}

@Test func navigationDestination_item_pushes_and_clears_on_back() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 40, height: 10)

    let s0 = runtime.debugRender(_NavigationDestinationItemProbeView(), size: size)
    #expect(s0.text.contains("selected: -1"))

    guard let show = _findButton(s0, title: "Show") else {
        #expect(Bool(false), "Could not find Show button")
        return
    }
    s0.click(x: show.x, y: show.y)

    let s1 = runtime.debugRender(_NavigationDestinationItemProbeView(), size: size)
    #expect(s1.text.contains("Item Detail 5"))

    guard let back = _findButton(s1, title: "Back") else {
        #expect(Bool(false), "Could not find Back button")
        return
    }
    s1.click(x: back.x, y: back.y)

    let s2 = runtime.debugRender(_NavigationDestinationItemProbeView(), size: size)
    #expect(s2.text.contains("selected: -1"))
    #expect(!s2.text.contains("Item Detail 5"))
}

struct _NavigationDestinationValueProbeView: View {
    var body: some View {
        NavigationStack {
            VStack(spacing: 1) {
                NavigationLink(value: 42) {
                    Text("Value Link")
                }
            }
            .navigationDestination(for: Int.self) { value in
                Text("Value \(value)")
            }
        }
    }
}

@Test func navigationDestination_for_value_pushes_resolved_destination() async throws {
    let runtime = _UIRuntime()
    let size = _Size(width: 40, height: 10)

    let s0 = runtime.debugRender(_NavigationDestinationValueProbeView(), size: size)
    #expect(s0.text.contains("Value Link"))

    guard let link = _findButton(s0, title: "Value Link") else {
        #expect(Bool(false), "Could not find Value Link button")
        return
    }
    s0.click(x: link.x, y: link.y)

    let s1 = runtime.debugRender(_NavigationDestinationValueProbeView(), size: size)
    #expect(s1.text.contains("Value 42"))
    #expect(s1.text.contains("[ Back ]"))
}

struct _CoreParityViewsProbe: View {
    @State private var date: Date = Date(timeIntervalSinceReferenceDate: 123456789)

    var body: some View {
        NavigationView {
            VStack(spacing: 1) {
                DatePicker("When", selection: $date)
                    .datePickerStyle(.graphical)
                Gauge(value: 0.4) {
                    Text("Gauge")
                } currentValueLabel: {
                    Text("40%")
                }
                .gaugeStyle(.default)
                AsyncImage(url: URL(string: "https://example.com/image.png")) { _ in
                    Text("photo")
                }
                TimelineView(()) { context in
                    Text(context.cadence == .live ? "Timeline" : "Other")
                }
                .contentTransition(ContentTransition())
                .navigationTransition(NavigationTransition())
            }
        }
        .navigationViewStyle(.default)
    }
}

@Test func core_parity_views_render_basic_output() async throws {
    let runtime = _UIRuntime()
    let s0 = runtime.debugRender(_CoreParityViewsProbe(), size: _Size(width: 60, height: 12))
    #expect(s0.text.contains("When"))
    #expect(s0.text.contains("Gauge"))
    #expect(s0.text.contains("40%"))
        }

struct _DrawingParityProbe: View {
    var body: some View {
        VStack(spacing: 1) {
            LinearGradient(colors: [.red, .blue], startPoint: .topLeading, endPoint: .bottomTrailing)
                .frame(width: 4, height: 2)
            Canvas { context, _ in
                context.fill(Rectangle(), with: .color(.green))
            }
            .frame(width: 4, height: 2)
        }
    }
}

@Test func asyncImage_loads_local_file() async throws {
    let runtime = _UIRuntime()
    let tempURL = URL.temporaryDirectory.appending(path: "omniui-async-image-test.bin")
    try Data([0x89, 0x50, 0x4E, 0x47]).write(to: tempURL)
    struct Probe: View {
        let url: URL
        var body: some View {
            AsyncImage(url: url) { phase in
                switch phase {
                case .success:
                    return Text("photo")
                case .failure:
                    return Text("failure")
                case .empty:
                    return Text("empty")
                }
            }
        }
    }
    let initial = runtime.debugRender(Probe(url: tempURL), size: _Size(width: 20, height: 4))
    #expect(initial.text.contains("empty"))
    try await Task.sleep(nanoseconds: 100_000_000)
    let next = runtime.debugRender(Probe(url: tempURL), size: _Size(width: 20, height: 4))
    #expect(next.text.contains("photo"))
}

@Test func timelineView_ticks_over_time() async throws {
    let runtime = _UIRuntime()
    struct Probe: View {
        var body: some View {
            TimelineView(()) { context in
                Text(context.date.formatted(date: .omitted, time: .standard))
            }
        }
    }
    let first = runtime.debugRender(Probe(), size: _Size(width: 20, height: 4)).text
    try await Task.sleep(nanoseconds: 1_100_000_000)
    let second = runtime.debugRender(Probe(), size: _Size(width: 20, height: 4)).text
    #expect(first != second)
}

@Test func drawing_primitives_emit_renderer_ops() async throws {
    let runtime = _UIRuntime()
    let snapshot = runtime.render(_DrawingParityProbe(), size: _Size(width: 6, height: 6))
    #expect(!snapshot.ops.isEmpty)
    #expect(!snapshot.shapeRegions.isEmpty)
}

struct _DisabledButtonProbe: View {
    @State private var count = 0

    var body: some View {
        VStack(spacing: 1) {
            Text("Count: \(count)")
            Button("Inc") { count += 1 }
                .disabled(true)
        }
    }
}

@Test func disabled_button_blocks_interaction() async throws {
    let runtime = _UIRuntime()
    let initial = runtime.debugRender(_DisabledButtonProbe(), size: _Size(width: 30, height: 6))
    #expect(initial.text.contains("Count: 0"))
    initial.click(x: 2, y: 2)
    let next = runtime.debugRender(_DisabledButtonProbe(), size: _Size(width: 30, height: 6))
    #expect(next.text.contains("Count: 0"))
}

struct _SearchableProbe: View {
    @State private var query = ""

    var body: some View {
        Text("Query: \(query)")
            .searchable(text: $query)
    }
}

@Test func searchable_renders_field_and_updates_binding() async throws {
    let runtime = _UIRuntime()
    let initial = runtime.debugRender(_SearchableProbe(), size: _Size(width: 40, height: 6))
    #expect(initial.text.contains("Query:"))
    #expect(initial.text.contains("Search"))
    initial.click(x: 3, y: 0)
    initial.type("abc")
    let next = runtime.debugRender(_SearchableProbe(), size: _Size(width: 40, height: 6))
    #expect(next.text.contains("Query: abc"))
}

struct _RefreshableProbe: View {
    @State private var refreshCount = 0

    var body: some View {
        Text("Refreshes: \(refreshCount)")
            .refreshable {
                refreshCount += 1
            }
    }
}

@Test func refreshable_exposes_button_and_runs_action() async throws {
    let runtime = _UIRuntime()
    let initial = runtime.debugRender(_RefreshableProbe(), size: _Size(width: 40, height: 6))
    #expect(initial.text.contains("Refreshes: 0"))
    #expect(initial.text.contains("Refresh"))
    initial.click(x: 2, y: 0)
    try await Task.sleep(nanoseconds: 50_000_000)
    let next = runtime.debugRender(_RefreshableProbe(), size: _Size(width: 40, height: 6))
    #expect(next.text.contains("Refreshes: 1"))
}

struct _OffsetOpacityProbe: View {
    @State private var count = 0

    var body: some View {
        VStack(spacing: 1) {
            Text("Count: \(count)")
            Button("Go") { count += 1 }
                .offset(x: 4, y: 0)
            Text("Fade")
                .opacity(0.5)
        }
    }
}

@Test func offset_and_opacity_modifiers_affect_render_tree() async throws {
    let runtime = _UIRuntime()
    let initial = runtime.debugRender(_OffsetOpacityProbe(), size: _Size(width: 30, height: 8))
    initial.click(x: 8, y: 2)
    let next = runtime.debugRender(_OffsetOpacityProbe(), size: _Size(width: 30, height: 8))
    #expect(next.text.contains("Count: 1"))

    let faded = runtime.render(Text("Fade").opacity(0.5), size: _Size(width: 10, height: 2))
    let hasAlpha = faded.ops.contains { op in
        switch op.kind {
        case .glyph(_, _, _, let fg, _), .textRun(_, _, _, let fg, _):
            return (fg?.alpha ?? 1.0) < 1.0
        default:
            return false
        }
    }
    #expect(hasAlpha)
}

struct _TextEnvironmentProbe: View {
    var body: some View {
        Text("First\nSecond\nThird")
            .lineLimit(2)
            .multilineTextAlignment(.trailing)
            .font(.headline)
    }
}

@Test func text_environment_modifiers_affect_output() async throws {
    let runtime = _UIRuntime()
    let snapshot = runtime.debugRender(_TextEnvironmentProbe(), size: _Size(width: 20, height: 5))
    #expect(snapshot.text.contains("First"))
    #expect(snapshot.text.contains("Second"))
    #expect(!snapshot.text.contains("Third"))
}

struct _ExitCommandProbe: View {
    @State private var exits = 0

    var body: some View {
        Text("Exits: \(exits)")
            .onExitCommand {
                exits += 1
            }
    }
}

@Test func onExitCommand_registers_runtime_handler() async throws {
    let runtime = _UIRuntime()
    _ = runtime.debugRender(_ExitCommandProbe(), size: _Size(width: 20, height: 4))
    #expect(runtime.invokeExitCommand())
    let next = runtime.debugRender(_ExitCommandProbe(), size: _Size(width: 20, height: 4))
    #expect(next.text.contains("Exits: 1"))
}

struct _QuickLookProbe: View {
    let url: Binding<URL?>

    var body: some View {
        Text("Host")
            .quickLookPreview(url)
    }
}

@Test func quickLookPreview_registers_overlay() async throws {
    let runtime = _UIRuntime()
    var currentURL: URL? = URL(string: "https://example.com/file.txt")
    let binding = Binding<URL?>(get: { currentURL }, set: { currentURL = $0 })
    let snapshot = runtime.debugRender(_QuickLookProbe(url: binding), size: _Size(width: 50, height: 10))
    #expect(snapshot.text.contains("Quick Look"))
    #expect(snapshot.text.contains("file.txt"))
    currentURL = nil
    let next = runtime.debugRender(_QuickLookProbe(url: binding), size: _Size(width: 50, height: 10))
    #expect(!next.text.contains("Quick Look"))
}

struct _HoverProbe: View {
    @State private var hovered = false

    var body: some View {
        Text(hovered ? "Hover ON" : "Hover OFF")
            .onHover { hovered = $0 }
    }
}

@Test func onHover_updates_state_via_snapshot_hover() async throws {
    let runtime = _UIRuntime()
    let initial = runtime.debugRender(_HoverProbe(), size: _Size(width: 20, height: 4))
    #expect(initial.text.contains("Hover OFF"))
    initial.hover(x: 1, y: 0)
    let hovered = runtime.debugRender(_HoverProbe(), size: _Size(width: 20, height: 4))
    #expect(hovered.text.contains("Hover ON"))
    hovered.hover(x: 19, y: 3)
    let cleared = runtime.debugRender(_HoverProbe(), size: _Size(width: 20, height: 4))
    #expect(cleared.text.contains("Hover OFF"))
}

struct _SceneCommandProbe: Scene {
    @SceneBuilder var body: some Scene {
        WindowGroup {
            Text("Scene Root")
        }
        .commands {
            CommandGroup(after: .appInfo) {
                Button("Command") {}
            }
        }
        .defaultSize(width: 90, height: 30)
    }
}

@Test func scene_commands_and_default_size_are_exposed() async throws {
    let scene = _SceneCommandProbe().body
    let root = _sceneRootView(scene)
    #expect(root != nil)
    let commands = _sceneCommandsView(scene)
    #expect(commands != nil)
    let preferred = _scenePreferredSize(scene)
    #expect(preferred?.width == 90)
    #expect(preferred?.height == 30)

    let runtime = _UIRuntime()
    let snapshot = runtime.debugRender(root!, size: _Size(width: 30, height: 6))
    #expect(snapshot.text.contains("Scene Root"))
}

struct _TextFieldConfigProbe: View {
    @State private var url = ""
    @State private var corrected = ""

    var body: some View {
        VStack(spacing: 1) {
            TextField("URL", text: $url)
                .keyboardType(.URL)
                .textInputAutocapitalization(.characters)
                .textContentType(.URL)
                .textFieldStyle(.plain)
            Text(url)
            TextField("Auto", text: $corrected)
                .autocorrectionDisabled(false)
            Text(corrected)
        }
    }
}

@Test func textField_style_and_input_configs_affect_behavior() async throws {
    let runtime = _UIRuntime()
    let initial = runtime.debugRender(_TextFieldConfigProbe(), size: _Size(width: 30, height: 8))
    #expect(!initial.text.contains("[URL]"))
    initial.click(x: 1, y: 0)
    initial.type("Ab C")
    let urlState = runtime.debugRender(_TextFieldConfigProbe(), size: _Size(width: 30, height: 8))
    #expect(urlState.text.contains("abc"))
    #expect(!urlState.text.contains("Ab C"))

    urlState.click(x: 1, y: 4)
    urlState.type("teh ")
    let corrected = runtime.debugRender(_TextFieldConfigProbe(), size: _Size(width: 30, height: 8))
    #expect(corrected.text.contains("the "))
}

@Test func scene_commands_and_default_size_metadata_are_preserved() async throws {
    let scene = AnyScene(
        WindowGroup {
            Text("Root")
        }
        .commands {
            CommandGroup(after: .appInfo) {
                Button("Command") {}
            }
        }
        .defaultSize(width: 42, height: 17)
    )

    let rootScene: _OmniUISceneRoot = scene
    #expect(rootScene._omniUIRootView() != nil)
    #expect(rootScene._omniUICommandsView() != nil)
    #expect(rootScene._omniUIPreferredSize == CGSize(width: 42, height: 17))

    let runtime = _UIRuntime()
    if let commandsView = rootScene._omniUICommandsView() {
        let snapshot = runtime.debugRender(commandsView, size: _Size(width: 20, height: 4))
        #expect(snapshot.text.contains("Command"))
    }
}

struct _SplitAutomaticProbeView: View {
    var body: some View {
        NavigationSplitView(
            columnVisibility: Binding(get: { .automatic }, set: { _ in }),
            sidebar: { Text("SIDEBAR") },
            detail: { Text("DETAIL") }
        )
    }
}

@Test func navigationSplitView_automatic_adapts_by_width() async throws {
    let runtime = _UIRuntime()

    let narrow = runtime.debugRender(_SplitAutomaticProbeView(), size: _Size(width: 40, height: 6))
    #expect(narrow.text.contains("DETAIL"))
    #expect(!narrow.text.contains("SIDEBAR"))

    let wide = runtime.debugRender(_SplitAutomaticProbeView(), size: _Size(width: 100, height: 6))
    #expect(wide.text.contains("DETAIL"))
    #expect(wide.text.contains("SIDEBAR"))
}

struct _TabViewSelectionFallbackProbe: View {
    let selection: Binding<String>

    var body: some View {
        TabView(selection: selection) {
            Text("First body")
                .tabItem { Text("First") }
                .tag("first")
            Text("Second body")
                .tabItem { Text("Second") }
                .tag("second")
        }
    }
}

@Test func tabView_reconciles_missing_selection_to_first_available_tab() async throws {
    let runtime = _UIRuntime()
    var selection = "missing"
    let binding = Binding(get: { selection }, set: { selection = $0 })

    let snapshot = runtime.debugRender(_TabViewSelectionFallbackProbe(selection: binding), size: _Size(width: 30, height: 8))

    #expect(snapshot.text.contains("[First]"))
    #expect(snapshot.text.contains("First body"))
    #expect(selection == "first")
}
