import Foundation
import SwiftData
import SwiftUI

@Model
final class HarnessModel {
    var id: Int
    init(id: Int) { self.id = id }
}

@MainActor
@Observable
final class HarnessObservableModel {
    var counter: Int = 0
    var flag: Bool = false
}

private struct HarnessModifier: ViewModifier {
    func body(content: Content) -> some View {
        content
    }
}

private struct HarnessView: View {
    @AppStorage("harnessFlag") private var harnessFlag: Bool = false
    @Namespace private var ns
    @State private var text: String = ""
    @State private var showAlert: Bool = false
    @State private var picked: Color = .blue
    @State private var quickLookURL: URL? = nil
    @State private var didSeedModels: Bool = false
    @State private var observableModel = MainActor.assumeIsolated { HarnessObservableModel() }

    @Query(sort: \HarnessModel.id, order: .reverse) private var models: [HarnessModel]
    @Environment(\.modelContext) private var modelContext

    var body: some View {
        let observableModel = observableModel

        VStack(spacing: 1) {
            Group {
                Toggle("Flag", isOn: $harnessFlag)
                    .toggleStyle(.switch)

                TextField("Search", text: $text)
                    .textFieldStyle(.roundedBorder)

                Text("Tap Target")
                    .onTapGesture { showAlert = true }

                Divider()

                Text(Image(systemName: "sparkles"))

                GeometryReader { _ in
                    Text("GeometryReader")
                }

                Canvas { context, size in
                    for y in stride(from: 0.0, to: size.height, by: 4.0) {
                        let rect = CGRect(x: 0, y: y, width: size.width, height: 1)
                        context.fill(Path(rect), with: .color(Color.black.opacity(0.2)))
                    }
                }

                Text("Platform Colors")
                    .background(Color(uiColor: .systemGray6))
                    .background(Color(uiColor: .systemBackground))

                RoundedRectangle(cornerRadius: 2, style: .continuous)
                    .fill(.ultraThinMaterial)
                    .glassEffect(in: .rect(cornerRadius: 2))

                RoundedRectangle(cornerRadius: 2)
                    .stroke(
                        LinearGradient(colors: [.red, .blue], startPoint: .topLeading, endPoint: .bottomTrailing),
                        lineWidth: 1
                    )

                RadialGradient(
                    gradient: Gradient(colors: [.clear, .black.opacity(0.2)]),
                    center: .center,
                    startRadius: 0,
                    endRadius: 10
                )

                if let shareURL = URL(string: "https://example.com") {
                    ShareLink(item: shareURL) {
                        Label("Share", systemImage: "square.and.arrow.up")
                    }
                }

                Button("QuickLook") {
                    quickLookURL = URL(filePath: "/tmp/example.txt")
                }
                .quickLookPreview($quickLookURL)

                HarnessObservableRow(model: observableModel)
            }

            Group {
                GroupBox {
                    Form {
                        Section {
                            LabeledContent("Hello", value: "World")
                        } header: {
                            Text("Section Header")
                        }
                    }
                } label: {
                    Label("GroupBox", systemImage: "star")
                }

            if models.isEmpty {
                ContentUnavailableView("No Data", systemImage: "xmark", description: Text("Empty"))
            } else {
                Text("Models: \(models.count)")
            }

                ColorPicker("Color", selection: $picked)
                    .labelsHidden()

                Menu {
                    Button("One") {}
                    Button("Two") {}
                } label: {
                    Text("Menu")
                }
                .controlSize(.large)

                Button("OK") {}
                    .buttonStyle(.borderedProminent)
                    .keyboardShortcut(.defaultAction)

                Label("Icon", systemImage: "star")
                    .labelStyle(.iconOnly)

                Button("Cancel") { showAlert = false }
                    .keyboardShortcut(.cancelAction)
            }
        }
        .modifier(HarnessModifier())
        .listStyle(.plain)
        .pickerStyle(.segmented)
        .tag(ns) // Ensure `Namespace.ID` is hashable
        .safeAreaInset(edge: .top) { Text("Inset") }
        .toolbar {
            ToolbarItemGroup(placement: .automatic) {
                Button("Inc") {
                    MainActor.assumeIsolated {
                        observableModel.counter += 1
                    }
                }
                Button("Toggle") {
                    MainActor.assumeIsolated {
                        observableModel.flag.toggle()
                    }
                }
            }
            ToolbarItem(placement: .confirmationAction) {
                Button("Done") {
                    withAnimation(.spring()) {
                        showAlert = false
                    }
                }
            }
        }
        .toolbar(removing: .sidebarToggle)
        .alert(isPresented: $showAlert) {
            Alert(title: Text("Alert"), message: Text("Hello"), dismissButton: .default(Text("OK")))
        }
        .onAppear {
            guard !didSeedModels else { return }
            didSeedModels = true
            modelContext.insert(HarnessModel(id: 1))
            modelContext.insert(HarnessModel(id: 2))
        }
    }
}

private struct HarnessObservableRow: View {
    @Bindable var model: HarnessObservableModel

    var body: some View {
        let model = model
        let counter = MainActor.assumeIsolated { model.counter }
        let flag = MainActor.assumeIsolated { model.flag }

        Text("Counter \(counter)")
            .onHover { _ in }
            .onChange(of: counter) { _ in }
            .onChange(of: flag) { }
            .onChange(of: counter, initial: true) { _, _ in }
            .phaseAnimator([false, true]) { content, phase in
                content.opacity(phase ? 1 : 0.85)
            } animation: { _ in
                .easeInOut(duration: 0.2)
            }
            .background(.bar)
            .background(.background)
    }
}

@main
struct SwiftUICompatibilityHarnessApp: App {
    var container: ModelContainer = Self.makeContainer()

    private static func makeContainer() -> ModelContainer {
        let schema = Schema([HarnessModel.self])
        let config = ModelConfiguration(schema: schema, isStoredInMemoryOnly: true)
        do {
            return try ModelContainer(for: schema, configurations: [config])
        } catch {
            preconditionFailure("Failed to create in-memory ModelContainer for SwiftUICompatibilityHarness: \(error)")
        }
    }

    var body: some Scene {
        WindowGroup {
            HarnessView()
                .modelContainer(container)
        }
        .defaultSize(width: 1200, height: 800)
        .commands {
            CommandGroup(after: .appInfo) {
                Button("Harness Command") {}
            }
            #if os(macOS)
                SidebarCommands()
            #endif
        }
    }
}
