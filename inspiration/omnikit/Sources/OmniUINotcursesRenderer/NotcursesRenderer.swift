import Foundation
import OmniUICore

#if os(Linux) || os(macOS)
import CNotcurses
#if os(Linux)
import Glibc
#else
import Darwin
#endif
#endif

public enum OmniUINotcursesRendererError: Error {
    case notSupportedOnThisPlatform
    case notcursesUnavailable
}

private struct _TermCaps {
    var supportedStyles: UInt32
    var canBraille: Bool
    var canTruecolor: Bool
    var canHalfblock: Bool
    var canFade: Bool
}

private struct _NCShapePlaneEntry {
    var plane: OpaquePointer
    var rect: _Rect
    var sig: Int
}

#if os(Linux) || os(macOS)
private func _writeToStderr(_ message: String) {
    // Avoid `stderr` (a C global `var`) under Swift 6 strict concurrency.
    let bytes = Array(message.utf8)
    bytes.withUnsafeBytes { raw in
        guard let base = raw.baseAddress else { return }
        #if os(Linux)
        _ = Glibc.write(STDERR_FILENO, base, raw.count)
        #else
        _ = Darwin.write(STDERR_FILENO, base, raw.count)
        #endif
    }
}
#endif

/// A minimal notcurses-based renderer loop.
///
/// This renders OmniUICore's typed `RenderOp`s and uses notcurses for drawing + input
/// (mouse clicks + scroll wheel + basic keyboard).
public struct NotcursesApp<V: View>: @unchecked Sendable {
    let root: () -> V

    public init(root: @escaping () -> V) {
        self.root = root
    }

}

public extension NotcursesApp where V == AnyView {
    init<S: Scene>(scene: S) {
        self.init(root: {
            let root = _sceneRootView(scene) ?? AnyView(Text("Empty Scene"))
            if let commands = _sceneCommandsView(scene) {
                return AnyView(root.safeAreaInset(edge: .bottom) { commands })
            }
            return root
        })
    }

    init<A: App>(_ appType: A.Type) {
        let app = A.init()
        self.init(scene: app.body)
    }
}

extension NotcursesApp {
    @MainActor
    public func run() async throws {
        #if os(Linux) || os(macOS)
        omni_install_signal_handlers()

        var opts = notcurses_options()
        opts.flags |= UInt64(NCOPTION_SUPPRESS_BANNERS)
        guard let nc = notcurses_init(&opts, nil) else {
            omni_restore_terminal()
            throw OmniUINotcursesRendererError.notcursesUnavailable
        }
        // Prime notcurses once so the stdplane dimensions/colors are stable before our first frame.
        // Without this, some terminals report 0x0 for a short period and we'd "render" a 1x1 frame.
        _ = notcurses_render(nc)
        var userRequestedExit = false
        var exitNote: String? = nil
        let clock = ContinuousClock()
        let smokeDeadline: ContinuousClock.Instant? = {
            guard let raw = getenv("OMNIUI_SMOKE_SECONDS") else { return nil }
            let s = String(cString: raw)
            guard let secs = Double(s), secs > 0 else { return nil }
            return clock.now.advanced(by: .milliseconds(Int64(secs * 1000.0)))
        }()
        // Smoke mode is enabled via `OMNIUI_SMOKE_SECONDS` (useful for automated runs).
        defer {
            _ = notcurses_stop(nc)
            omni_restore_terminal()
            if !userRequestedExit, let exitNote {
                _writeToStderr("OmniUI notcurses renderer exited: \(exitNote)\n")
            }
        }

        _ = notcurses_mice_enable(nc, omni_ncmice_all_events())
        // We install our own signal handlers; leave terminal line signals enabled.

        let runtime = _UIRuntime()
        var prev: [_NCCell]? = nil
        // (overlay plane is always erased + full-painted each frame; no prev tracking needed)
        var lastSnapshot: RenderSnapshot? = nil
        let forceNoPixels = (getenv("TMUX") != nil) // tmux generally won't support Kitty graphics passthrough
        var didRenderAtLeastOneFullFrame = false
        var shapePlanes: [Int: _NCShapePlaneEntry] = [:]
        var overlayPlane: OpaquePointer? = nil
        var overlayRect: _Rect? = nil
        var activeNCMenu: (widget: OpaquePointer, rect: _Rect, itemIDs: [Int])? = nil
        var activeNCSelector: (widget: OpaquePointer, rect: _Rect, itemIDs: [Int])? = nil
        var activeNCReader: (widget: OpaquePointer, sourceRect: _Rect, visibleRect: _Rect, fieldID: Int)? = nil
        var cursorCurrentlyEnabled = false

        func destroyActiveNCMenu() {
            guard let menu = activeNCMenu else { return }
            omni_ncmenu_destroy(menu.widget)
            activeNCMenu = nil
        }

        func destroyActiveNCSelector() {
            guard let selector = activeNCSelector else { return }
            omni_ncselector_destroy(selector.widget)
            activeNCSelector = nil
        }

        func destroyActiveNCReader() {
            guard let reader = activeNCReader else { return }
            omni_ncreader_destroy(reader.widget)
            activeNCReader = nil
        }

        func destroyAllNativeWidgets() {
            destroyActiveNCReader()
            destroyActiveNCSelector()
            destroyActiveNCMenu()
        }

        defer {
            destroyAllNativeWidgets()
            if let op = overlayPlane { ncplane_destroy(op) }
            for (_, e) in shapePlanes {
                ncplane_destroy(e.plane)
            }
            shapePlanes.removeAll()
        }

        let q: UInt32 = 113
        let esc: UInt32 = omni_nckey_esc()
        let backspace: UInt32 = omni_nckey_backspace()
        let enter: UInt32 = omni_nckey_enter()
        let up: UInt32 = omni_nckey_up()
        let down: UInt32 = omni_nckey_down()
        let left: UInt32 = omni_nckey_left()
        let right: UInt32 = omni_nckey_right()
        let home: UInt32 = omni_nckey_home()
        let end: UInt32 = omni_nckey_end()
        let del: UInt32 = omni_nckey_delete()
        let button1: UInt32 = omni_nckey_button1()
        let button2: UInt32 = omni_nckey_button2()
        let button3: UInt32 = omni_nckey_button3()
        let motion: UInt32 = omni_nckey_motion()
        let scrollUp: UInt32 = omni_nckey_scroll_up()
        let scrollDown: UInt32 = omni_nckey_scroll_down()
        let resize: UInt32 = omni_nckey_resize()
        let pgup: UInt32 = omni_nckey_pgup()
        let pgdown: UInt32 = omni_nckey_pgdown()
        let tabKey: UInt32 = omni_nckey_tab()
        let fKeyRange = omni_nckey_f01()...omni_nckey_f12()

        // Query terminal capabilities once at init.
        let termCaps = _TermCaps(
            supportedStyles: omni_notcurses_supported_styles(nc),
            canBraille: omni_notcurses_canbraille(nc) != 0,
            canTruecolor: omni_notcurses_cantruecolor(nc) != 0,
            canHalfblock: omni_notcurses_canhalfblock(nc) != 0,
            canFade: omni_notcurses_canfade(nc) != 0
        )
        _ = termCaps // Used below for style-aware rendering.

        func eventModifiers(_ ni: inout ncinput) -> EventModifiers {
            var mods: EventModifiers = []
            if omni_ncinput_shift(&ni) != 0 { mods.insert(.shift) }
            if omni_ncinput_ctrl(&ni) != 0 { mods.insert(.control) }
            if omni_ncinput_alt(&ni) != 0 { mods.insert(.option) }
            if omni_ncinput_meta(&ni) != 0 { mods.insert(.command) }
            if omni_ncinput_super(&ni) != 0 { mods.insert(.command) }
            return mods
        }

        while !Task.isCancelled {
            if let deadline = smokeDeadline, clock.now >= deadline {
                userRequestedExit = true
                return
            }
            let sig = omni_signal_received()
            if sig != 0 {
                exitNote = "signal \(sig)"
                return
            }
            guard let stdplane = notcurses_stdplane(nc) else { break }

            var rows: UInt32 = 0
            var cols: UInt32 = 0
            ncplane_dim_yx(stdplane, &rows, &cols)

            // Some terminals report 0x0 until at least one render; avoid "1x1" diffs that
            // make it look like nothing rendered until the user interacts.
            if rows == 0 || cols == 0 {
                _ = notcurses_render(nc)
                try await Task.sleep(nanoseconds: 16_000_000)
                continue
            }

            let height = Int(rows)
            let width = Int(cols)

            let renderSize = _Size(width: width, height: height)
            if runtime.needsRender(size: renderSize) || lastSnapshot == nil {
                let snapshot = runtime.render(root(), size: renderSize)
                lastSnapshot = snapshot

            let baseFG = _NCRGB(r: 0xD8, g: 0xDB, b: 0xE2)
            let baseBG = _NCRGB(r: 0x0B, g: 0x10, b: 0x20)
            let focusFG = _NCRGB(r: 0xFF, g: 0xFF, b: 0xFF)
            let focusBG = _NCRGB(r: 0x1D, g: 0x4E, b: 0xD8)
            let accentFG = _NCRGB(r: 0x34, g: 0xD3, b: 0x99)
            let shapeFillBG = _NCRGB(r: 0x12, g: 0x1B, b: 0x33)
            let borderFG = _NCRGB(r: 0xF2, g: 0xF4, b: 0xF8)

            let focusRect = snapshot.focusedRect

            // Force a full repaint on the very first frame and any terminal resize.
            // This fixes "background/shapes only appear after first interaction" issues by ensuring
            // we explicitly set every cell at least once.
            if !didRenderAtLeastOneFullFrame || (prev != nil && prev!.count != width * height) {
                prev = nil
                didRenderAtLeastOneFullFrame = true
                ncplane_home(stdplane)
                ncplane_erase(stdplane)
                if let op = overlayPlane { ncplane_erase(op) }
            }

            // Render shapes/paths using sprixels/pixels.
            let cellpix = _ncCellPix(nc)
            let canSprixel: Bool = {
                if forceNoPixels { return false }
                guard let cellpix else { return false }
                if notcurses_check_pixel_support(nc) == NCPIXEL_NONE { return false }
                // Only require valid cell pixel dimensions; maxpixel{x,y} may be 0
                // on terminals like Ghostty that support Kitty graphics but don't
                // report maximum image dimensions via ncvisual_geom.
                return cellpix.cdimx > 0 && cellpix.cdimy > 0
            }()

            var curr = Array(repeating: _NCCell(ch: " ", fg: baseFG, bg: baseBG), count: width * height)
            var zbuf = Array(repeating: Int.min, count: width * height)
            // Overlay buffers for ops with z >= 1000 (rendered on a separate ncplane).
            var overlayCurr = Array(repeating: _NCCell(ch: " ", fg: baseFG, bg: baseBG), count: width * height)
            var overlayZbuf = Array(repeating: Int.min, count: width * height)
            var hasOverlayOps = false

            func setCell(_ x: Int, _ y: Int, _ egc: String, _ fg: _NCRGB?, _ bg: _NCRGB?, z: Int, style: UInt16 = 0) {
                guard x >= 0, y >= 0, x < width, y < height else { return }
                let idx = y * width + x
                if z >= 1000 {
                    // Overlay content goes to the overlay buffer.
                    if z < overlayZbuf[idx] { return }
                    hasOverlayOps = true
                    var c = overlayCurr[idx]
                    c.ch = egc
                    if let fg { c.fg = fg }
                    if let bg { c.bg = bg }
                    if style != 0 { c.styles = style }
                    overlayCurr[idx] = c
                    overlayZbuf[idx] = z
                } else {
                    if z < zbuf[idx] { return }
                    var c = curr[idx]
                    c.ch = egc
                    if let fg { c.fg = fg }
                    if let bg { c.bg = bg }
                    if style != 0 { c.styles = style }
                    curr[idx] = c
                    zbuf[idx] = z
                }
            }

            func intersect(_ a: _Rect, _ b: _Rect) -> _Rect? {
                let x0 = max(a.origin.x, b.origin.x)
                let y0 = max(a.origin.y, b.origin.y)
                let x1 = min(a.origin.x + a.size.width, b.origin.x + b.size.width)
                let y1 = min(a.origin.y + a.size.height, b.origin.y + b.size.height)
                if x1 <= x0 || y1 <= y0 { return nil }
                return _Rect(origin: _Point(x: x0, y: y0), size: _Size(width: x1 - x0, height: y1 - y0))
            }

            let fullClip = _Rect(origin: _Point(x: 0, y: 0), size: _Size(width: width, height: height))
            var clipStack: [_Rect] = [fullClip]
            func inClip(_ x: Int, _ y: Int) -> Bool {
                guard let c = clipStack.last else { return true }
                return c.contains(_Point(x: x, y: y))
            }

            // Shapes are rendered via sprixels. We clip at cell granularity and blit only
            // the visible subregion from each shape visual.
            var shapesForSprixel: [(sourceRect: _Rect, visibleRect: _Rect, shape: _ShapeNode)] = []
            shapesForSprixel.reserveCapacity(16)

            for op in snapshot.ops {
                let opStyle = op.textStyle.rawValue
                switch op.kind {
                case .glyph(let x, let y, let egc, let fg, let bg):
                    if !inClip(x, y) { break }
                    let mapped = egc.first ?? " "
                    var outFG = _resolveColorNC(fg) ?? baseFG
                    let outBG = _resolveColorNC(bg) ?? baseBG
                    if mapped == "*" { outFG = accentFG }
                    if _isBorderGlyphShared(mapped) { outFG = borderFG }
                    setCell(x, y, egc, outFG, outBG, z: op.zIndex, style: opStyle)
                case .textRun(let x, let y, let text, let fg, let bg):
                    let outFG = _resolveColorNC(fg) ?? baseFG
                    let outBG = _resolveColorNC(bg) ?? baseBG
                    var xx = x
                    for ch in text {
                        if inClip(xx, y) {
                        var fg2 = outFG
                        if ch == "*" { fg2 = accentFG }
                        if _isBorderGlyphShared(ch) { fg2 = borderFG }
                        setCell(xx, y, String(ch), fg2, outBG, z: op.zIndex, style: opStyle)
                        }
                        xx += 1
                        if xx >= width { break }
                    }
                case .fillRect(let rect, let color):
                    guard let c = _resolveColorNC(color) else { continue }
                    guard let top = clipStack.last, let rr = intersect(rect, top) else { continue }
                    let x0 = max(0, rr.origin.x)
                    let y0 = max(0, rr.origin.y)
                    let x1 = min(width, rr.origin.x + rr.size.width)
                    let y1 = min(height, rr.origin.y + rr.size.height)
                    if x1 <= x0 || y1 <= y0 { continue }
                    for yy in y0..<y1 {
                        for xx in x0..<x1 {
                            setCell(xx, yy, " ", nil, c, z: op.zIndex)
                        }
                    }
                case .pushClip(let r):
                    if let top = clipStack.last, let i = intersect(top, r) {
                        clipStack.append(i)
                    } else {
                        clipStack.append(_Rect(origin: _Point(x: 0, y: 0), size: _Size(width: 0, height: 0)))
                    }
                case .popClip:
                    if clipStack.count > 1 { _ = clipStack.popLast() }
                case .shape(let rect, let shape):
                    guard canSprixel else { continue }
                    guard let top = clipStack.last,
                          let clippedToOp = intersect(rect, top),
                          let visible = intersect(clippedToOp, fullClip) else { continue }
                    shapesForSprixel.append((sourceRect: rect, visibleRect: visible, shape: shape))
                }
            }

            // Update per-shape sprixel planes after collecting shapes for this frame.
            if canSprixel, let cellpix {
                // If any shape position changed (scroll, resize, layout shift),
                // destroy all planes and rebuild. This prevents Kitty graphics
                // ghost artifacts — the Kitty protocol stores images separately
                // from the cell grid, so ncplane_erase/destroy alone doesn't
                // clean up stale pixel placements.
                let positionsChanged = shapesForSprixel.enumerated().contains { (idx, entry) in
                    guard let existing = shapePlanes[idx] else { return false }
                    return existing.rect.origin != entry.visibleRect.origin
                } || shapesForSprixel.count != shapePlanes.count
                if positionsChanged && !shapePlanes.isEmpty {
                    for (_, e) in shapePlanes { ncplane_destroy(e.plane) }
                    shapePlanes.removeAll()
                }

                var alive: Set<Int> = []
                alive.reserveCapacity(shapesForSprixel.count)

                for (idx, entry) in shapesForSprixel.enumerated() {
                    alive.insert(idx)

                    let sourceRect = entry.sourceRect
                    let visibleRect = entry.visibleRect
                    let s = entry.shape

                    var hasher = Hasher()
                    hasher.combine(width)
                    hasher.combine(height)
                    hasher.combine(cellpix.cdimx)
                    hasher.combine(cellpix.cdimy)
                    hasher.combine(sourceRect)
                    hasher.combine(visibleRect)
                    hasher.combine(s.kind)
                    hasher.combine(s.fillStyle)
                    hasher.combine(s.strokeStyle)
                    if let e = s.pathElements {
                        hasher.combine(e.count)
                        for el in e { hasher.combine(el) }
                    }
                    let sig = hasher.finalize()

                    let needsNewPlane: Bool = {
                        guard let existing = shapePlanes[idx] else { return true }
                        if existing.rect.size != visibleRect.size { return true }
                        return false
                    }()

                    if needsNewPlane {
                        if let existing = shapePlanes[idx] {
                            ncplane_destroy(existing.plane)
                            shapePlanes[idx] = nil
                        }
                        var popts = ncplane_options()
                        popts.y = Int32(visibleRect.origin.y)
                        popts.x = Int32(visibleRect.origin.x)
                        popts.rows = UInt32(visibleRect.size.height)
                        popts.cols = UInt32(visibleRect.size.width)
                        popts.name = nil
                        popts.userptr = nil
                        popts.resizecb = nil
                        popts.flags = 0
                        popts.margin_b = 0
                        popts.margin_r = 0
                        if let p = ncplane_create(stdplane, &popts) {
                            // Place shape planes above stdplane so pixel content is visible.
                            _ = ncplane_move_above(p, stdplane)
                            // Don't set transparent base; pixel blitting replaces the cell content.
                            shapePlanes[idx] = _NCShapePlaneEntry(plane: p, rect: visibleRect, sig: Int.min)
                        } else {
                            // If we can't create shape planes, clear and retry next frame.
                            for (_, e) in shapePlanes { ncplane_destroy(e.plane) }
                            shapePlanes.removeAll()
                            break
                        }
                    } else if let existing = shapePlanes[idx], existing.rect.origin != visibleRect.origin {
                        // Shape moved (e.g. scroll). Erase old Kitty graphics placement,
                        // move the plane, and invalidate sig to force re-blit at new position.
                        ncplane_erase(existing.plane)
                        ncplane_move_yx(existing.plane, Int32(visibleRect.origin.y), Int32(visibleRect.origin.x))
                        shapePlanes[idx]?.rect = visibleRect
                        shapePlanes[idx]?.sig = Int.min
                    }

                    guard var planeEntry = shapePlanes[idx] else { continue }
                    if planeEntry.sig == sig { continue }

                    let shapeLocalRect = _Rect(
                        origin: _Point(
                            x: sourceRect.origin.x - visibleRect.origin.x,
                            y: sourceRect.origin.y - visibleRect.origin.y
                        ),
                        size: sourceRect.size
                    )
                    ncplane_erase(planeEntry.plane)
                    let ok = _renderSprixels(
                        nc: nc,
                        plane: planeEntry.plane,
                        termSize: visibleRect.size,
                        cellpix: cellpix,
                        shapes: [(shapeLocalRect, s)],
                        fill: shapeFillBG,
                        stroke: borderFG
                    )
                    if ok {
                        planeEntry.sig = sig
                        planeEntry.rect = visibleRect
                        shapePlanes[idx] = planeEntry
                    } else {
                        // Pixel blit failed; clear all planes and retry next frame.
                        for (_, e) in shapePlanes { ncplane_destroy(e.plane) }
                        shapePlanes.removeAll()
                        break
                    }
                }

                // Delete planes for shapes that no longer exist.
                // Erase before destroy to clear any Kitty graphics placements
                // that would otherwise leave pixel artifacts on screen.
                if !shapePlanes.isEmpty {
                    for (idx, e) in shapePlanes where !alive.contains(idx) {
                        ncplane_erase(e.plane)
                        ncplane_destroy(e.plane)
                        shapePlanes[idx] = nil
                    }
                }
            } else {
                // No pixel support; tear down any existing sprixel planes so they can't leave artifacts.
                for (_, e) in shapePlanes { ncplane_destroy(e.plane) }
                shapePlanes.removeAll()
            }

            if let fr = focusRect {
                // Focus highlight over cells (applied to whichever layer the cell belongs to).
                let y0 = max(0, fr.origin.y)
                let x0 = max(0, fr.origin.x)
                let y1 = min(height, fr.origin.y + fr.size.height)
                let x1 = min(width, fr.origin.x + fr.size.width)
                if y1 > y0, x1 > x0 {
                    for y in y0..<y1 {
                        for x in x0..<x1 {
                            let idx = y * width + x
                            if overlayZbuf[idx] > Int.min {
                                overlayCurr[idx].fg = focusFG
                                overlayCurr[idx].bg = focusBG
                            } else {
                                curr[idx].fg = focusFG
                                curr[idx].bg = focusBG
                            }
                        }
                    }
                }
            }

            var widgetRects: [_Rect] = []
            if let m = activeNCMenu, snapshot.activeMenu?.boundingRect == m.rect {
                widgetRects.append(m.rect)
            }
            if let s = activeNCSelector, snapshot.activePicker?.boundingRect == s.rect {
                widgetRects.append(s.rect)
            }
            if let r = activeNCReader,
               let tf = snapshot.activeTextField,
               tf.boundingRect == r.sourceRect,
               tf.actionID == r.fieldID {
                widgetRects.append(r.visibleRect)
            }

            func isInWidgetRect(_ x: Int, _ y: Int) -> Bool {
                let p = _Point(x: x, y: y)
                return widgetRects.contains { $0.contains(p) }
            }

            // Differential paint (cell-level) on stdplane.
            // Scroll regions are handled by the layout system (scroll offset adjusts which ops
            // are visible); painting all cells to stdplane avoids stale-plane artifacts.
            let toPaint: [(Int, _NCCell)]
            if let prev {
                toPaint = curr.enumerated().compactMap { idx, c in prev[idx] == c ? nil : (idx, c) }
            } else {
                toPaint = curr.enumerated().map { ($0.offset, $0.element) }
            }

            var lastFG: _NCRGB? = nil
            var lastBG: _NCRGB? = nil
            var lastStyles: UInt16 = 0

            for (idx, cell) in toPaint {
                let y = idx / width
                let x = idx % width
                if isInWidgetRect(x, y) { continue }
                if lastFG == nil || lastFG! != cell.fg {
                    _ = ncplane_set_fg_rgb8(stdplane, UInt32(cell.fg.r), UInt32(cell.fg.g), UInt32(cell.fg.b))
                    lastFG = cell.fg
                }
                if lastBG == nil || lastBG! != cell.bg {
                    _ = ncplane_set_bg_rgb8(stdplane, UInt32(cell.bg.r), UInt32(cell.bg.g), UInt32(cell.bg.b))
                    lastBG = cell.bg
                }
                if cell.styles != lastStyles {
                    omni_ncplane_set_styles(stdplane, UInt32(cell.styles))
                    lastStyles = cell.styles
                }
                cell.ch.utf8CString.withUnsafeBufferPointer { buf in
                    if let p = buf.baseAddress {
                        _ = ncplane_putegc_yx(stdplane, Int32(y), Int32(x), p, nil)
                    }
                }
            }
            // Reset styles after painting to avoid leaking into next frame.
            if lastStyles != 0 {
                omni_ncplane_set_styles(stdplane, 0)
            }

            prev = curr

            // ── Overlay plane management ──
            // Overlay ops (z >= 1000) are painted on a separate ncplane above stdplane.
            if hasOverlayOps {
                let isNewOverlay = (overlayPlane == nil)
                // Create overlay plane if it doesn't exist or dimensions changed.
                if overlayPlane == nil || overlayRect?.size != _Size(width: width, height: height) {
                    if let op = overlayPlane { ncplane_destroy(op) }
                    var popts = ncplane_options()
                    popts.y = 0
                    popts.x = 0
                    popts.rows = UInt32(height)
                    popts.cols = UInt32(width)
                    popts.name = nil
                    popts.userptr = nil
                    popts.resizecb = nil
                    popts.flags = 0
                    popts.margin_b = 0
                    popts.margin_r = 0
                    overlayPlane = ncplane_create(stdplane, &popts)
                    if let op = overlayPlane {
                        _ = ncplane_move_above(op, stdplane)
                        _ = omni_ncplane_set_base_transparent(op)
                    }
                    overlayRect = _Rect(origin: _Point(x: 0, y: 0), size: _Size(width: width, height: height))
                    }

                if let op = overlayPlane {
                    // Always erase and full-paint the overlay plane each frame.
                    // Overlay content is typically small (dropdown menus, alerts) so
                    // differential painting isn't needed, and erasing prevents stale
                    // cells from persisting when overlay content changes shape.
                    ncplane_erase(op)

                    var oLastFG: _NCRGB? = nil
                    var oLastBG: _NCRGB? = nil
                    var oLastStyles: UInt16 = 0

                    for (idx, cell) in overlayCurr.enumerated() {
                        guard overlayZbuf[idx] > Int.min else { continue }
                        let y = idx / width
                        let x = idx % width
                        if isInWidgetRect(x, y) { continue }
                        if oLastFG == nil || oLastFG! != cell.fg {
                            _ = ncplane_set_fg_rgb8(op, UInt32(cell.fg.r), UInt32(cell.fg.g), UInt32(cell.fg.b))
                            oLastFG = cell.fg
                        }
                        if oLastBG == nil || oLastBG! != cell.bg {
                            _ = ncplane_set_bg_rgb8(op, UInt32(cell.bg.r), UInt32(cell.bg.g), UInt32(cell.bg.b))
                            oLastBG = cell.bg
                        }
                        if cell.styles != oLastStyles {
                            omni_ncplane_set_styles(op, UInt32(cell.styles))
                            oLastStyles = cell.styles
                        }
                        cell.ch.utf8CString.withUnsafeBufferPointer { buf in
                            if let p = buf.baseAddress {
                                _ = ncplane_putegc_yx(op, Int32(y), Int32(x), p, nil)
                            }
                        }
                    }
                    if oLastStyles != 0 {
                        omni_ncplane_set_styles(op, 0)
                    }

                    // Fade-in when overlay first appears (if terminal supports it).
                    if isNewOverlay, termCaps.canFade {
                        _ = notcurses_render(nc)
                        _ = omni_ncplane_fadein(op, 100)
                    }
                }
            } else {
                // No overlay ops: fade out and destroy the overlay plane if it exists.
                if let op = overlayPlane {
                    if termCaps.canFade {
                        _ = notcurses_render(nc)
                        _ = omni_ncplane_fadeout(op, 100)
                    }
                    ncplane_destroy(op)
                    overlayPlane = nil
                    overlayRect = nil
                    }
            }

            // ── Native widget lifecycle ──
            var widgetStateChanged = false

            if let menuInfo = snapshot.activeMenu {
                let itemIDs = menuInfo.items.map { $0.actionID }
                let needsRecreate: Bool = {
                    guard let menu = activeNCMenu else { return true }
                    return menu.rect != menuInfo.boundingRect || menu.itemIDs != itemIDs
                }()
                if needsRecreate {
                    if activeNCMenu != nil {
                        destroyActiveNCMenu()
                        widgetStateChanged = true
                    }
                    if !menuInfo.items.isEmpty {
                        let labels = menuInfo.items.map { $0.label }
                        let cLabels = labels.map { strdup($0) }
                        defer { cLabels.forEach { free($0) } }
                        if !cLabels.contains(where: { $0 == nil }) {
                            var ptrs: [UnsafePointer<CChar>?] = cLabels.map { UnsafePointer($0) }
                            let menuWidget = ptrs.withUnsafeMutableBufferPointer { buf -> OpaquePointer? in
                                guard let base = buf.baseAddress else { return nil }
                                return menuInfo.title.withCString { titlePtr in
                                    omni_ncmenu_create_flat(stdplane, titlePtr, base, Int32(labels.count), 0)
                                }
                            }
                            if let widget = menuWidget {
                                _ = omni_ncmenu_unroll(widget, 0)
                                activeNCMenu = (widget: widget, rect: menuInfo.boundingRect, itemIDs: itemIDs)
                                widgetStateChanged = true
                            }
                        }
                    }
                }
            } else if activeNCMenu != nil {
                destroyActiveNCMenu()
                widgetStateChanged = true
            }

            if let pickerInfo = snapshot.activePicker {
                let itemIDs = pickerInfo.options.map { $0.actionID }
                let needsRecreate: Bool = {
                    guard let selector = activeNCSelector else { return true }
                    return selector.rect != pickerInfo.boundingRect || selector.itemIDs != itemIDs
                }()
                if needsRecreate {
                    if activeNCSelector != nil {
                        destroyActiveNCSelector()
                        widgetStateChanged = true
                    }
                    if !pickerInfo.options.isEmpty {
                        let options = pickerInfo.options.map { $0.label }
                        let cOpts = options.map { strdup($0) }
                        let cDescs = options.map { _ in strdup("") }
                        defer {
                            cOpts.forEach { free($0) }
                            cDescs.forEach { free($0) }
                        }

                        if !cOpts.contains(where: { $0 == nil }) && !cDescs.contains(where: { $0 == nil }) {
                            // Create a child plane large enough for the ncselector.
                            // ncselector renders at (0,0) of its plane, so the plane
                            // position determines where the dropdown appears.
                            // The ncselector creates its content as a child plane. Give it
                            // a parent plane sized to accommodate options + borders + title.
                            let maxLabelWidth = options.map { $0.count }.max() ?? 10
                            // ncselector layout: border(1) + option(N) + " " + desc(N) + border(1)
                            let selectorWidth = max(maxLabelWidth + 6, 30)
                            let selectorHeight = min(max(options.count + 4, 6), height / 2)

                            let dropY = pickerInfo.boundingRect.origin.y
                            let dropX = pickerInfo.boundingRect.origin.x
                            let clampedY = max(0, min(dropY, height - selectorHeight))
                            let clampedX = max(0, min(dropX, width - selectorWidth))

                            var popts = ncplane_options()
                            popts.y = Int32(clampedY)
                            popts.x = Int32(clampedX)
                            popts.rows = UInt32(min(selectorHeight, height - clampedY))
                            popts.cols = UInt32(min(selectorWidth, width - clampedX))
                            popts.name = nil
                            popts.userptr = nil
                            popts.resizecb = nil
                            popts.flags = 0
                            popts.margin_b = 0
                            popts.margin_r = 0

                            if let selectorPlane = ncplane_create(stdplane, &popts) {
                                var optPtrs: [UnsafePointer<CChar>?] = cOpts.map { UnsafePointer($0) }
                                var descPtrs: [UnsafePointer<CChar>?] = cDescs.map { UnsafePointer($0) }
                                let defaultIndex = UInt32(min(max(0, pickerInfo.selectedIndex ?? 0), max(0, options.count - 1)))
                                let maxDisplay = UInt32(max(1, min(options.count, selectorHeight - 2)))
                                let selectorWidget = optPtrs.withUnsafeMutableBufferPointer { optBuf -> OpaquePointer? in
                                    descPtrs.withUnsafeMutableBufferPointer { descBuf -> OpaquePointer? in
                                        guard let optBase = optBuf.baseAddress,
                                              let descBase = descBuf.baseAddress else { return nil }
                                        return pickerInfo.title.withCString { titlePtr in
                                            omni_ncselector_create(
                                                selectorPlane,
                                                optBase,
                                                descBase,
                                                Int32(options.count),
                                                defaultIndex,
                                                maxDisplay,
                                                titlePtr,
                                                nil
                                            )
                                        }
                                    }
                                }

                                if let widget = selectorWidget {
                                    // Query the ncselector's actual plane for the widget rect.
                                    let selPlane = omni_ncselector_plane(widget)
                                    var selRows: UInt32 = 0; var selCols: UInt32 = 0
                                    var selY: Int32 = 0; var selX: Int32 = 0
                                    if let sp = selPlane {
                                        ncplane_dim_yx(sp, &selRows, &selCols)
                                        ncplane_yx(sp, &selY, &selX)
                                        // Ensure selector is above overlay and shape planes.
                                        if let op = overlayPlane {
                                            _ = ncplane_move_above(sp, op)
                                        }
                                        for (_, e) in shapePlanes {
                                            _ = ncplane_move_above(sp, e.plane)
                                        }
                                    }
                                    let widgetRect = _Rect(
                                        origin: _Point(x: Int(selY > 0 ? selX : Int32(clampedX)),
                                                       y: Int(selY > 0 ? selY : Int32(clampedY))),
                                        size: _Size(width: max(Int(selCols), selectorWidth),
                                                    height: max(Int(selRows), selectorHeight))
                                    )
                                    activeNCSelector = (widget: widget, rect: widgetRect, itemIDs: itemIDs)
                                    widgetStateChanged = true
                                } else {
                                    ncplane_destroy(selectorPlane)
                                }
                            }
                        }
                    }
                }
            } else if activeNCSelector != nil {
                destroyActiveNCSelector()
                widgetStateChanged = true
            }

            if let tfInfo = snapshot.activeTextField {
                let needsRecreate: Bool = {
                    guard let reader = activeNCReader else { return true }
                    return reader.sourceRect != tfInfo.boundingRect || reader.fieldID != tfInfo.actionID
                }()
                if needsRecreate {
                    if activeNCReader != nil {
                        destroyActiveNCReader()
                        widgetStateChanged = true
                    }

                    if let visibleRect = intersect(tfInfo.boundingRect, fullClip),
                       visibleRect.size.width > 0,
                       visibleRect.size.height > 0 {
                        var popts = ncplane_options()
                        popts.y = Int32(visibleRect.origin.y)
                        popts.x = Int32(visibleRect.origin.x)
                        popts.rows = UInt32(max(1, visibleRect.size.height))
                        popts.cols = UInt32(max(1, visibleRect.size.width))
                        popts.name = nil
                        popts.userptr = nil
                        popts.resizecb = nil
                        popts.flags = 0
                        popts.margin_b = 0
                        popts.margin_r = 0
                        if let readerPlane = ncplane_create(stdplane, &popts) {
                            if let op = overlayPlane {
                                _ = ncplane_move_above(readerPlane, op)
                            } else {
                                _ = ncplane_move_above(readerPlane, stdplane)
                            }
                            let flags = omni_ncreader_option_horscroll() | omni_ncreader_option_cursor() | omni_ncreader_option_nocmdkeys()
                            if let widget = omni_ncreader_create(readerPlane, flags) {
                                activeNCReader = (
                                    widget: widget,
                                    sourceRect: tfInfo.boundingRect,
                                    visibleRect: visibleRect,
                                    fieldID: tfInfo.actionID
                                )
                                widgetStateChanged = true
                            } else {
                                ncplane_destroy(readerPlane)
                            }
                        }
                    }
                }

                if let reader = activeNCReader?.widget {
                    _ = omni_ncreader_clear(reader)
                    for scalar in tfInfo.text.unicodeScalars {
                        var feedNi = ncinput()
                        feedNi.id = scalar.value
                        feedNi.evtype = NCTYPE_PRESS
                        _ = omni_ncreader_offer_input(reader, &feedNi)
                    }
                    let scalarCount = tfInfo.text.unicodeScalars.count
                    let cursorOffset = min(max(0, tfInfo.cursorOffset), scalarCount)
                    let trailing = max(0, scalarCount - cursorOffset)
                    if trailing > 0 {
                        for _ in 0..<trailing {
                            var moveNi = ncinput()
                            moveNi.id = left
                            moveNi.evtype = NCTYPE_PRESS
                            _ = omni_ncreader_offer_input(reader, &moveNi)
                        }
                    }
                }
            } else if activeNCReader != nil {
                destroyActiveNCReader()
                widgetStateChanged = true
            }

            if let menuPlane = activeNCMenu.flatMap({ omni_ncmenu_plane($0.widget) }) {
                if let op = overlayPlane {
                    _ = ncplane_move_above(menuPlane, op)
                } else {
                    _ = ncplane_move_above(menuPlane, stdplane)
                }
            }
            if let selectorPlane = activeNCSelector.flatMap({ omni_ncselector_plane($0.widget) }) {
                if let op = overlayPlane {
                    _ = ncplane_move_above(selectorPlane, op)
                } else {
                    _ = ncplane_move_above(selectorPlane, stdplane)
                }
                for (_, e) in shapePlanes {
                    _ = ncplane_move_above(selectorPlane, e.plane)
                }
            }
            if let readerPlane = activeNCReader.flatMap({ omni_ncreader_plane($0.widget) }) {
                if let op = overlayPlane {
                    _ = ncplane_move_above(readerPlane, op)
                } else {
                    _ = ncplane_move_above(readerPlane, stdplane)
                }
            }

            if widgetStateChanged {
                prev = nil
            }

            // Hardware cursor: enable at the focused text field's cursor position, disable otherwise.
            // Guard with cursorCurrentlyEnabled to avoid calling disable when already disabled
            // (notcurses documents this as an error condition).
            if let cp = snapshot.cursorPosition,
               cp.x >= 0, cp.y >= 0,
               cp.x < width, cp.y < height {
                _ = omni_notcurses_cursor_enable(nc, Int32(cp.y), Int32(cp.x))
                cursorCurrentlyEnabled = true
            } else if cursorCurrentlyEnabled {
                _ = omni_notcurses_cursor_disable(nc)
                cursorCurrentlyEnabled = false
            }

            let rr = notcurses_render(nc)
            if rr != 0 {
                exitNote = "notcurses_render() failed (\(rr))"
                return
            }
            }

            guard let snapshot = lastSnapshot else {
                // No snapshot yet; sleep briefly to avoid a tight spin.
                try await Task.sleep(nanoseconds: 16_000_000)
                continue
            }

            // Non-blocking input check. We avoid the blocking omni_notcurses_get here
            // because this runs on @MainActor — a blocking C call would prevent Swift
            // concurrency from making progress and can cause deadlocks.
            var ni = ncinput()
            let id = notcurses_get_nblock(nc, &ni)

            // If no input and no pending render, sleep to avoid tight-spinning.
            if id == 0 && !runtime.needsRender(size: renderSize) {
                try await Task.sleep(nanoseconds: 16_000_000)
                continue
            }

            // Yield to Swift concurrency so async tasks can make progress.
            await Task.yield()

            if id == UInt32.max {
                exitNote = "notcurses_get() error"
                return
            }
            if id == 0 {
                // Timeout expired, no input. Loop back to re-render if needed.
                continue
            }

            // Handle NCKEY_RESIZE before anything else.
            if id == resize {
                var newRows: UInt32 = 0
                var newCols: UInt32 = 0
                _ = omni_notcurses_refresh(nc, &newRows, &newCols)
                destroyAllNativeWidgets()
                // Force full repaint on next frame.
                prev = nil
                continue
            }

            if let menu = activeNCMenu {
                if omni_ncmenu_offer_input(menu.widget, &ni) {
                    var selNi = ncinput()
                    if let sel = omni_ncmenu_selected(menu.widget, &selNi) {
                        let selected = String(cString: sel)
                        if let idx = snapshot.activeMenu?.items.firstIndex(where: { $0.label == selected }),
                           idx < menu.itemIDs.count {
                            runtime.invokeActionByRawID(menu.itemIDs[idx])
                        }
                    }
                    continue
                }
            }

            if let selector = activeNCSelector {
                if omni_ncselector_offer_input(selector.widget, &ni) {
                    if id == enter || id == 10 || id == 13 {
                        if let sel = omni_ncselector_selected(selector.widget) {
                            let selected = String(cString: sel)
                            if let idx = snapshot.activePicker?.options.firstIndex(where: { $0.label == selected }),
                               idx < selector.itemIDs.count {
                                runtime.invokeActionByRawID(selector.itemIDs[idx])
                            }
                        }
                    }
                    continue
                }
            }

            if let reader = activeNCReader {
                _ = omni_ncreader_offer_input(reader.widget, &ni)
            }

            // Treat all non-RELEASE events as "press-like" to avoid dropping keyboard input,
            // while still preventing double-firing on RELEASE.
            guard ni.evtype != NCTYPE_RELEASE else { continue }

            let mods = eventModifiers(&ni)

            if id == q {
                if runtime.invokeExitCommand() {
                    continue
                }
                userRequestedExit = true
                return
            }

            if id == esc {
                if runtime.hasExpandedPicker() {
                    runtime.collapseExpandedPicker()
                    continue
                }
                if runtime.invokeKeyboardShortcut(.escape, modifiers: mods) {
                    continue
                }
                if runtime.invokeExitCommand() {
                    continue
                }
                if runtime.canPopNavigation() {
                    runtime.popNavigation()
                }
                continue
            }

            // Mouse buttons / movement.
            if id == motion {
                snapshot.hover(x: Int(ni.x), y: Int(ni.y))
                continue
            }
            if id == button1 {
                snapshot.hover(x: Int(ni.x), y: Int(ni.y))
                snapshot.click(x: Int(ni.x), y: Int(ni.y))
                continue
            } else if id == button2 || id == button3 {
                snapshot.hover(x: Int(ni.x), y: Int(ni.y))
                snapshot.click(x: Int(ni.x), y: Int(ni.y))
                continue
            } else if id == scrollUp {
                snapshot.hover(x: Int(ni.x), y: Int(ni.y))
                snapshot.scroll(x: Int(ni.x), y: Int(ni.y), deltaY: -1)
                continue
            } else if id == scrollDown {
                snapshot.hover(x: Int(ni.x), y: Int(ni.y))
                snapshot.scroll(x: Int(ni.x), y: Int(ni.y), deltaY: 1)
                continue
            }

            if runtime.isTextEditingFocused() {
                let ctrlA = id == 1 || (mods.contains(.control) && (id == UInt32(UInt8(ascii: "a")) || id == UInt32(UInt8(ascii: "A"))))
                let ctrlE = id == 5 || (mods.contains(.control) && (id == UInt32(UInt8(ascii: "e")) || id == UInt32(UInt8(ascii: "E"))))
                let ctrlK = id == 11 || (mods.contains(.control) && (id == UInt32(UInt8(ascii: "k")) || id == UInt32(UInt8(ascii: "K"))))
                if ctrlA {
                    runtime._handleKey(.home)
                    continue
                }
                if ctrlE {
                    runtime._handleKey(.end)
                    continue
                }
                if ctrlK {
                    runtime._handleKey(.killToEnd)
                    continue
                }
            }

            // Tab / Shift-Tab navigation.
            if id == 9 || id == tabKey {
                let isShift = omni_ncinput_shift(&ni) != 0
                if isShift {
                    if runtime.hasExpandedPicker() {
                        runtime.focusPrevWithinExpandedPicker()
                    } else {
                        runtime.focusPrev()
                    }
                } else {
                    if runtime.hasExpandedPicker() {
                        runtime.focusNextWithinExpandedPicker()
                    } else {
                        runtime.focusNext()
                    }
                }
            } else if id == up {
                if runtime.hasExpandedPicker() {
                    runtime.focusPrevWithinExpandedPicker()
                } else {
                    runtime.focusPrev()
                }
            } else if id == down {
                if runtime.hasExpandedPicker() {
                    runtime.focusNextWithinExpandedPicker()
                } else {
                    runtime.focusNext()
                }
            } else if id == pgup {
                // Page Up: scroll up by a larger amount.
                if let snap = lastSnapshot {
                    snap.scroll(x: 0, y: 0, deltaY: -10)
                }
            } else if id == pgdown {
                // Page Down: scroll down by a larger amount.
                if let snap = lastSnapshot {
                    snap.scroll(x: 0, y: 0, deltaY: 10)
                }
            } else if id == enter || id == 10 || id == 13 {
                if runtime.hasExpandedPicker() {
                    runtime.activateFocused()
                    continue
                }
                if runtime.invokeKeyboardShortcut(.return, modifiers: mods) {
                    continue
                }
                if runtime.isTextEditingFocused() { runtime.submitFocusedTextEditor() }
                else { runtime.activateFocused() }
            } else if id == 32 { // Space
                if runtime.isTextEditingFocused() {
                    runtime._handleKey(.char(32))
                } else {
                    runtime.activateFocused()
                }
            } else if id == backspace || id == 127 {
                if runtime.isTextEditingFocused() {
                    runtime._handleKey(.backspace)
                } else if runtime.canPopNavigation() {
                    runtime.popNavigation()
                }
            } else if id == del {
                if runtime.isTextEditingFocused() {
                    runtime._handleKey(.delete)
                }
            } else if id == left {
                if runtime.isTextEditingFocused() { runtime._handleKey(.left) }
            } else if id == right {
                if runtime.isTextEditingFocused() { runtime._handleKey(.right) }
            } else if id == home {
                if runtime.isTextEditingFocused() { runtime._handleKey(.home) }
            } else if id == end {
                if runtime.isTextEditingFocused() { runtime._handleKey(.end) }
            } else if fKeyRange.contains(id) {
                // F1-F12: currently no-op, reserved for future key bindings.
                break
            } else if id < 0x110000 {
                if mods.contains(.control), id >= 1, id <= 26 {
                    let v = UInt32(UInt8(ascii: "a")) + (id - 1)
                    if let letter = UnicodeScalar(v) {
                        let key = KeyEquivalent(stringLiteral: String(letter))
                        if runtime.invokeKeyboardShortcut(key, modifiers: mods) {
                            continue
                        }
                    }
                }
                if !mods.isEmpty, let scalar = UnicodeScalar(id) {
                    let key = KeyEquivalent(stringLiteral: String(scalar))
                    if runtime.invokeKeyboardShortcut(key, modifiers: mods) {
                        continue
                    }
                }
                runtime._handleKey(.char(id))
            }
        }
        if Task.isCancelled {
            exitNote = "task cancelled"
        } else {
            exitNote = "loop ended"
        }
        #else
        throw OmniUINotcursesRendererError.notSupportedOnThisPlatform
        #endif
    }
}

private struct _CellPix {
    var cdimy: Int
    var cdimx: Int
    var maxpixely: Int
    var maxpixelx: Int
}

private func _resolveColorNC(_ c: Color?) -> _NCRGB? {
    _resolveColorToRGB(c)
}

private func _ncCellPix(_ nc: OpaquePointer) -> _CellPix? {
    var cdimy: UInt32 = 0
    var cdimx: UInt32 = 0
    var maxpixely: UInt32 = 0
    var maxpixelx: UInt32 = 0
    let rc = omni_notcurses_cellpix(nc, &cdimy, &cdimx, &maxpixely, &maxpixelx)
    if rc != 0 { return nil }
    return _CellPix(cdimy: Int(cdimy), cdimx: Int(cdimx), maxpixely: Int(maxpixely), maxpixelx: Int(maxpixelx))
}

private struct _RGBA {
    var r: UInt8
    var g: UInt8
    var b: UInt8
    var a: UInt8
}

private func _renderSprixels(
    nc: OpaquePointer,
    plane: OpaquePointer,
    termSize: _Size,
    cellpix: _CellPix,
    shapes: [(_Rect, _ShapeNode)],
    fill: _NCRGB,
    stroke: _NCRGB
) -> Bool {
    guard termSize.width > 0, termSize.height > 0 else { return false }

    func rgba(_ c: _NCRGB) -> _RGBA { _RGBA(r: c.r, g: c.g, b: c.b, a: 0xFF) }

    func clamp01(_ x: Double) -> Double { min(1.0, max(0.0, x)) }

    func blend(dst: inout _RGBA, src: _RGBA) {
        let sa = Double(src.a) / 255.0
        if sa <= 0 { return }
        let da = Double(dst.a) / 255.0
        let oa = sa + da * (1.0 - sa)
        if oa <= 0 {
            dst = _RGBA(r: 0, g: 0, b: 0, a: 0)
            return
        }
        func comp(_ sc: UInt8, _ dc: UInt8) -> UInt8 {
            let s = Double(sc) / 255.0
            let d = Double(dc) / 255.0
            let o = (s * sa + d * da * (1.0 - sa)) / oa
            return UInt8(clamping: Int(o * 255.0))
        }
        dst.r = comp(src.r, dst.r)
        dst.g = comp(src.g, dst.g)
        dst.b = comp(src.b, dst.b)
        dst.a = UInt8(clamping: Int(oa * 255.0))
    }

    func sdfRoundedRect(px: Double, py: Double, w: Double, h: Double, r: Double) -> Double {
        // In pixels, centered at (0,0) spanning [-w/2,w/2]x[-h/2,h/2].
        let qx = abs(px) - (w / 2.0 - r)
        let qy = abs(py) - (h / 2.0 - r)
        let ox = max(qx, 0.0)
        let oy = max(qy, 0.0)
        let outside = hypot(ox, oy)
        let inside = min(max(qx, qy), 0.0)
        return outside + inside - r
    }

    func sdfEllipse(px: Double, py: Double, rx: Double, ry: Double) -> Double {
        // Approximate distance in pixels using normalized radial distance.
        let nx = px / max(1e-6, rx)
        let ny = py / max(1e-6, ry)
        let k = sqrt(nx * nx + ny * ny)
        // Scale back to pixels (roughly) so AA thickness stays consistent.
        return (k - 1.0) * min(rx, ry)
    }

    func rasterizeFilledShape(
        kind: _ShapeKind,
        pixelW: Int,
        pixelH: Int,
        radiusPx: Double?,
        fillEnabled: Bool,
        strokeWidthPx: Double,
        fillRGB: _RGBA,
        strokeRGB: _RGBA
    ) -> [UInt8] {
        var buf = Array(repeating: UInt8(0), count: pixelW * pixelH * 4)

        func get(_ x: Int, _ y: Int) -> _RGBA {
            let i = (y * pixelW + x) * 4
            return _RGBA(r: buf[i + 0], g: buf[i + 1], b: buf[i + 2], a: buf[i + 3])
        }
        func set(_ x: Int, _ y: Int, _ c: _RGBA) {
            let i = (y * pixelW + x) * 4
            buf[i + 0] = c.r
            buf[i + 1] = c.g
            buf[i + 2] = c.b
            buf[i + 3] = c.a
        }

        let w = Double(pixelW)
        let h = Double(pixelH)
        let cx = (w - 1.0) / 2.0
        let cy = (h - 1.0) / 2.0

        let halfStroke = max(0.0, strokeWidthPx) / 2.0

        for y in 0..<pixelH {
            for x in 0..<pixelW {
                let px = Double(x) - cx
                let py = Double(y) - cy

                let dist: Double
                switch kind {
                case .rectangle:
                    dist = sdfRoundedRect(px: px, py: py, w: w, h: h, r: 0.0)
                case .roundedRectangle:
                    let r = max(0.0, min(min(w, h) / 2.0, radiusPx ?? 0.0))
                    dist = sdfRoundedRect(px: px, py: py, w: w, h: h, r: r)
                case .capsule:
                    let r = max(0.0, min(w, h) / 2.0)
                    dist = sdfRoundedRect(px: px, py: py, w: w, h: h, r: r)
                case .circle:
                    let r = min(w, h) / 2.0
                    dist = sdfEllipse(px: px, py: py, rx: r, ry: r)
                case .ellipse:
                    let rx = w / 2.0
                    let ry = h / 2.0
                    dist = sdfEllipse(px: px, py: py, rx: rx, ry: ry)
                case .path:
                    continue
                }

                // AA: treat the edge as 1px wide transition.
                let fillA = fillEnabled ? clamp01(0.5 - dist) : 0.0 // inside => 1, outside => 0
                let strokeA = (strokeWidthPx > 0) ? clamp01((halfStroke + 0.5) - abs(dist)) : 0.0
                if fillA <= 0, strokeA <= 0 { continue }

                var out = _RGBA(r: 0, g: 0, b: 0, a: 0)
                if fillA > 0 {
                    out = _RGBA(r: fillRGB.r, g: fillRGB.g, b: fillRGB.b, a: UInt8(clamping: Int(fillA * 255.0)))
                }
                if strokeA > 0 {
                    let s = _RGBA(r: strokeRGB.r, g: strokeRGB.g, b: strokeRGB.b, a: UInt8(clamping: Int(strokeA * 255.0)))
                    blend(dst: &out, src: s)
                }

                let cur = get(x, y)
                var dst = cur
                blend(dst: &dst, src: out)
                set(x, y, dst)
            }
        }

        return buf
    }

    func rasterizePath(
        elements: [Path.Element],
        pixelW: Int,
        pixelH: Int,
        fillEnabled: Bool,
        eoFill: Bool,
        antialiased: Bool,
        strokeWidthPx: Double,
        fillRGB: _RGBA,
        strokeRGB: _RGBA
    ) -> [UInt8] {
        var buf = Array(repeating: UInt8(0), count: pixelW * pixelH * 4)

        func put(_ x: Int, _ y: Int, _ c: _RGBA) {
            guard x >= 0, y >= 0, x < pixelW, y < pixelH else { return }
            let i = (y * pixelW + x) * 4
            var dst = _RGBA(r: buf[i + 0], g: buf[i + 1], b: buf[i + 2], a: buf[i + 3])
            blend(dst: &dst, src: c)
            buf[i + 0] = dst.r
            buf[i + 1] = dst.g
            buf[i + 2] = dst.b
            buf[i + 3] = dst.a
        }

        struct Seg { var ax: Double; var ay: Double; var bx: Double; var by: Double }

        func distToSegment(px: Double, py: Double, _ s: Seg) -> Double {
            let vx = s.bx - s.ax
            let vy = s.by - s.ay
            let wx = px - s.ax
            let wy = py - s.ay
            let vv = vx * vx + vy * vy
            if vv <= 1e-9 {
                return hypot(px - s.ax, py - s.ay)
            }
            var t = (wx * vx + wy * vy) / vv
            t = min(1.0, max(0.0, t))
            let cx = s.ax + t * vx
            let cy = s.ay + t * vy
            return hypot(px - cx, py - cy)
        }

        // Build a flattened segment list in pixel space using the same mapping as `_strokePath`.
        var segments: [Seg] = []
        segments.reserveCapacity(max(16, elements.count * 4))
        _strokePathCollectSegments(
            elements: elements,
            x0: 0, y0: 0, x1: pixelW, y1: pixelH,
            addSegment: { x0, y0, x1, y1 in
                segments.append(Seg(ax: Double(x0), ay: Double(y0), bx: Double(x1), by: Double(y1)))
            },
            fillEllipse: { _, _, _, _ in },
            strokeRect: { _, _, _, _ in }
        )

        // Fill using a simple point-in-polygon test against the flattened segments.
        // This supports both even-odd and (approx) nonzero winding rules.
        func windingContains(_ x: Double, _ y: Double) -> Bool {
            var winding = 0
            for s in segments {
                let y0 = s.ay
                let y1 = s.by
                let x0 = s.ax
                let x1 = s.bx
                // Ignore horizontal edges.
                if y0 == y1 { continue }
                let upward = y0 < y1
                let ymin = min(y0, y1)
                let ymax = max(y0, y1)
                if y < ymin || y >= ymax { continue }
                // Compute x intersection at y.
                let t = (y - y0) / (y1 - y0)
                let ix = x0 + t * (x1 - x0)
                if ix <= x { continue }
                if upward {
                    winding += 1
                } else {
                    winding -= 1
                }
            }
            return winding != 0
        }

        func evenOddContains(_ x: Double, _ y: Double) -> Bool {
            var inside = false
            for s in segments {
                let y0 = s.ay
                let y1 = s.by
                let x0 = s.ax
                let x1 = s.bx
                if y0 == y1 { continue }
                let ymin = min(y0, y1)
                let ymax = max(y0, y1)
                if y < ymin || y >= ymax { continue }
                let t = (y - y0) / (y1 - y0)
                let ix = x0 + t * (x1 - x0)
                if ix > x {
                    inside.toggle()
                }
            }
            return inside
        }

        if fillEnabled, !segments.isEmpty {
            let samples: [(Double, Double)] = antialiased ? [(0.25, 0.25), (0.75, 0.25), (0.25, 0.75), (0.75, 0.75)] : [(0.5, 0.5)]
            for y in 0..<pixelH {
                for x in 0..<pixelW {
                    var covered = 0
                    for (ox, oy) in samples {
                        let px = Double(x) + ox
                        let py = Double(y) + oy
                        let ins = eoFill ? evenOddContains(px, py) : windingContains(px, py)
                        if ins { covered += 1 }
                    }
                    if covered == 0 { continue }
                    let a = UInt8(clamping: Int((Double(covered) / Double(samples.count)) * 255.0))
                    put(x, y, _RGBA(r: fillRGB.r, g: fillRGB.g, b: fillRGB.b, a: a))
                }
            }
        }

        if strokeWidthPx > 0, !segments.isEmpty {
            let half = strokeWidthPx / 2.0
            for y in 0..<pixelH {
                for x in 0..<pixelW {
                    let px = Double(x) + 0.5
                    let py = Double(y) + 0.5
                    var best = Double.greatestFiniteMagnitude
                    for s in segments {
                        best = min(best, distToSegment(px: px, py: py, s))
                    }
                    // 1px AA ramp.
                    let aa = antialiased ? 0.75 : 0.0
                    let alpha = clamp01((half + aa) - best)
                    if alpha <= 0 { continue }
                    put(x, y, _RGBA(r: strokeRGB.r, g: strokeRGB.g, b: strokeRGB.b, a: UInt8(clamping: Int(alpha * 255.0))))
                }
            }
        }

        return buf
    }

    func intersect(_ a: _Rect, _ b: _Rect) -> _Rect? {
        let x0 = max(a.origin.x, b.origin.x)
        let y0 = max(a.origin.y, b.origin.y)
        let x1 = min(a.origin.x + a.size.width, b.origin.x + b.size.width)
        let y1 = min(a.origin.y + a.size.height, b.origin.y + b.size.height)
        if x1 <= x0 || y1 <= y0 { return nil }
        return _Rect(origin: _Point(x: x0, y: y0), size: _Size(width: x1 - x0, height: y1 - y0))
    }

    let planeRect = _Rect(origin: _Point(x: 0, y: 0), size: termSize)

    var ok = true
    for (r, s) in shapes {
        guard r.size.width > 0, r.size.height > 0 else { continue }
        guard let visible = intersect(r, planeRect) else { continue }
        guard visible.size.width > 0, visible.size.height > 0 else { continue }

        let subBegX = visible.origin.x - r.origin.x
        let subBegY = visible.origin.y - r.origin.y
        let subLenX = visible.size.width
        let subLenY = visible.size.height
        if subBegX < 0 || subBegY < 0 || subLenX <= 0 || subLenY <= 0 { continue }

        let pixelW = r.size.width * cellpix.cdimx
        let pixelH = r.size.height * cellpix.cdimy
        if pixelW <= 0 || pixelH <= 0 { continue }
        // Only enforce maxpixel bounds when the terminal actually reports them;
        // some terminals (Ghostty) report 0 via ncvisual_geom but still support blitting.
        if cellpix.maxpixelx > 0 && pixelW > cellpix.maxpixelx { continue }
        if cellpix.maxpixely > 0 && pixelH > cellpix.maxpixely { continue }

        // Default to filled when no explicit style is set (matches SwiftUI behavior
        // where bare shapes like `Circle()` render as filled with foreground color).
        let hasNoStyle = s.fillStyle == nil && s.strokeStyle == nil
        let fillEnabled = s.fillStyle != nil || hasNoStyle
        let eoFill = s.fillStyle?.isEOFilled ?? false
        let aa = s.fillStyle?.antialiased ?? true
        let strokeWidthPx: Double = {
            guard let st = s.strokeStyle else { return hasNoStyle ? 1.0 : 0.0 }
            // Treat `lineWidth` as "terminal points" and map to pixels conservatively.
            return max(1.0, Double(st.lineWidth))
        }()

        let localFill = (s.fillColor.flatMap(_resolveColorNC) ?? fill)
        let localStroke = (s.strokeColor.flatMap(_resolveColorNC) ?? stroke)
        let fillRGB = rgba(localFill)
        let strokeRGB = rgba(localStroke)

        let buf: [UInt8]
        switch s.kind {
        case .path:
            buf = rasterizePath(
                elements: s.pathElements ?? [],
                pixelW: pixelW,
                pixelH: pixelH,
                fillEnabled: fillEnabled,
                eoFill: eoFill,
                antialiased: aa,
                strokeWidthPx: strokeWidthPx,
                fillRGB: fillRGB,
                strokeRGB: strokeRGB
            )
        case .roundedRectangle(let cr):
            buf = rasterizeFilledShape(
                kind: s.kind,
                pixelW: pixelW,
                pixelH: pixelH,
                radiusPx: Double(cr) * Double(cellpix.cdimx),
                fillEnabled: fillEnabled,
                strokeWidthPx: strokeWidthPx,
                fillRGB: fillRGB,
                strokeRGB: strokeRGB
            )
        default:
            buf = rasterizeFilledShape(
                kind: s.kind,
                pixelW: pixelW,
                pixelH: pixelH,
                radiusPx: nil,
                fillEnabled: fillEnabled,
                strokeWidthPx: strokeWidthPx,
                fillRGB: fillRGB,
                strokeRGB: strokeRGB
            )
        }

        var blitOK = false
        buf.withUnsafeBytes { bytes in
            guard let base = bytes.baseAddress else { return }
            guard let ncv = ncvisual_from_rgba(base, Int32(pixelH), Int32(pixelW * 4), Int32(pixelW)) else { return }
            defer { ncvisual_destroy(ncv) }

            var vopts = ncvisual_options()
            vopts.n = plane
            vopts.scaling = NCSCALE_NONE
            vopts.y = Int32(visible.origin.y)
            vopts.x = Int32(visible.origin.x)
            vopts.begy = UInt32(subBegY)
            vopts.begx = UInt32(subBegX)
            vopts.leny = UInt32(subLenY)
            vopts.lenx = UInt32(subLenX)
            vopts.blitter = ncblitter_e(rawValue: omni_ncblit_pixel())
            vopts.flags = 0
            vopts.transcolor = 0
            vopts.pxoffy = 0
            vopts.pxoffx = 0

            blitOK = (ncvisual_blit(nc, ncv, &vopts) != nil)
        }
        if !blitOK {
            ok = false
            break
        }
    }

    return ok
}

private func _fillRoundedRect(
    x0: Int,
    y0: Int,
    x1: Int,
    y1: Int,
    radius: Int,
    border: Int,
    fill: _RGBA,
    stroke: _RGBA,
    setPixel: (Int, Int, _RGBA) -> Void
) {
    let w = x1 - x0
    let h = y1 - y0
    guard w > 0, h > 0 else { return }

    let rOuter = max(0, radius)
    let rInner = max(0, radius - border)

    func inside(_ x: Int, _ y: Int, _ inset: Int, _ r: Int) -> Bool {
        let ax0 = x0 + inset
        let ay0 = y0 + inset
        let ax1 = x1 - inset
        let ay1 = y1 - inset
        if x < ax0 || x >= ax1 || y < ay0 || y >= ay1 { return false }

        let rx = r
        let ry = r
        // Fast path: not in a corner square.
        if x >= ax0 + rx && x < ax1 - rx { return true }
        if y >= ay0 + ry && y < ay1 - ry { return true }

        // Corner arcs.
        let cx = (x < ax0 + rx) ? (ax0 + rx - 1) : (ax1 - rx)
        let cy = (y < ay0 + ry) ? (ay0 + ry - 1) : (ay1 - ry)
        let dx = x - cx
        let dy = y - cy
        return dx * dx + dy * dy <= max(1, rx - 1) * max(1, rx - 1)
    }

    for y in y0..<y1 {
        for x in x0..<x1 {
            let out = inside(x, y, 0, rOuter)
            if !out { continue }
            let inn = inside(x, y, border, rInner)
            setPixel(x, y, inn ? fill : stroke)
        }
    }
}

private func _strokePath(
    elements: [Path.Element],
    x0: Int, y0: Int, x1: Int, y1: Int,
    drawLine: (Int, Int, Int, Int) -> Void,
    fillEllipse: (Int, Int, Int, Int) -> Void,
    strokeRect: (Int, Int, Int, Int) -> Void
) {
    // Map element coordinates into the target pixel rect by normalizing to the
    // bounds of the path data.
    var minX = Double.greatestFiniteMagnitude
    var minY = Double.greatestFiniteMagnitude
    var maxX = -Double.greatestFiniteMagnitude
    var maxY = -Double.greatestFiniteMagnitude

    func consider(_ p: CGPoint) {
        minX = min(minX, Double(p.x))
        minY = min(minY, Double(p.y))
        maxX = max(maxX, Double(p.x))
        maxY = max(maxY, Double(p.y))
    }

    for e in elements {
        switch e {
        case .move(let p), .line(let p):
            consider(p)
        case .quadCurve(let p, let c):
            consider(p)
            consider(c)
        case .curve(let p, let c1, let c2):
            consider(p)
            consider(c1)
            consider(c2)
        case .rect(let r):
            consider(r.origin)
            consider(CGPoint(x: r.origin.x + r.size.width, y: r.origin.y + r.size.height))
        case .ellipse(let r):
            consider(r.origin)
            consider(CGPoint(x: r.origin.x + r.size.width, y: r.origin.y + r.size.height))
        case .closeSubpath:
            break
        }
    }

    if !minX.isFinite || !minY.isFinite || !maxX.isFinite || !maxY.isFinite {
        return
    }

    let rangeX = max(1e-6, maxX - minX)
    let rangeY = max(1e-6, maxY - minY)
    let pad = 2
    let tw = max(1, (x1 - x0) - pad * 2)
    let th = max(1, (y1 - y0) - pad * 2)

    func map(_ p: CGPoint) -> (Int, Int) {
        let nx = (Double(p.x) - minX) / rangeX
        let ny = (Double(p.y) - minY) / rangeY
        let px = x0 + pad + Int(nx * Double(tw - 1))
        let py = y0 + pad + Int(ny * Double(th - 1))
        return (px, py)
    }

    var curr: (Int, Int)? = nil
    var start: (Int, Int)? = nil
    var currSrc: CGPoint? = nil
    var startSrc: CGPoint? = nil

    for e in elements {
        switch e {
        case .move(let p):
            let mp = map(p)
            curr = mp
            start = mp
            currSrc = p
            startSrc = p
        case .line(let p):
            let mp = map(p)
            if let c = curr {
                drawLine(c.0, c.1, mp.0, mp.1)
            }
            curr = mp
            currSrc = p
        case .rect(let r):
            let p0 = map(r.origin)
            let p1 = map(CGPoint(x: r.origin.x + r.size.width, y: r.origin.y + r.size.height))
            strokeRect(min(p0.0, p1.0), min(p0.1, p1.1), max(p0.0, p1.0), max(p0.1, p1.1))
        case .ellipse(let r):
            let p0 = map(r.origin)
            let p1 = map(CGPoint(x: r.origin.x + r.size.width, y: r.origin.y + r.size.height))
            fillEllipse(min(p0.0, p1.0), min(p0.1, p1.1), max(p0.0, p1.0), max(p0.1, p1.1))
        case .quadCurve(let p, let c):
            guard let s0 = currSrc else {
                currSrc = p
                curr = map(p)
                break
            }
            let steps = 24
            var prev = s0
            for i in 1...steps {
                let t = Double(i) / Double(steps)
                let a = _lerp(s0, c, t)
                let b = _lerp(c, p, t)
                let q = _lerp(a, b, t)
                let m0 = map(prev)
                let m1 = map(q)
                drawLine(m0.0, m0.1, m1.0, m1.1)
                prev = q
            }
            currSrc = p
            curr = map(p)
        case .curve(let p, let c1, let c2):
            guard let s0 = currSrc else {
                currSrc = p
                curr = map(p)
                break
            }
            let steps = 32
            var prev = s0
            for i in 1...steps {
                let t = Double(i) / Double(steps)
                let q = _cubic(s0, c1, c2, p, t)
                let m0 = map(prev)
                let m1 = map(q)
                drawLine(m0.0, m0.1, m1.0, m1.1)
                prev = q
            }
            currSrc = p
            curr = map(p)
        case .closeSubpath:
            if let c = curr, let s = start {
                drawLine(c.0, c.1, s.0, s.1)
            }
            curr = start
            currSrc = startSrc
        }
    }
}

private func _strokePathCollectSegments(
    elements: [Path.Element],
    x0: Int, y0: Int, x1: Int, y1: Int,
    addSegment: (Int, Int, Int, Int) -> Void,
    fillEllipse: (Int, Int, Int, Int) -> Void,
    strokeRect: (Int, Int, Int, Int) -> Void
) {
    _strokePath(
        elements: elements,
        x0: x0, y0: y0, x1: x1, y1: y1,
        drawLine: { ax, ay, bx, by in
            addSegment(ax, ay, bx, by)
        },
        fillEllipse: { ex0, ey0, ex1, ey1 in
            // Approximate ellipse boundary with a polyline so fills have a contour.
            let cx = Double(ex0 + ex1) / 2.0
            let cy = Double(ey0 + ey1) / 2.0
            let rx = max(1.0, Double(ex1 - ex0) / 2.0)
            let ry = max(1.0, Double(ey1 - ey0) / 2.0)
            let steps = 48
            var prevX = cx + rx
            var prevY = cy
            for i in 1...steps {
                let t = (Double(i) / Double(steps)) * (Double.pi * 2.0)
                let x = cx + cos(t) * rx
                let y = cy + sin(t) * ry
                addSegment(Int(prevX.rounded()), Int(prevY.rounded()), Int(x.rounded()), Int(y.rounded()))
                prevX = x
                prevY = y
            }
            fillEllipse(ex0, ey0, ex1, ey1)
        },
        strokeRect: { rx0, ry0, rx1, ry1 in
            addSegment(rx0, ry0, rx1, ry0)
            addSegment(rx1, ry0, rx1, ry1)
            addSegment(rx1, ry1, rx0, ry1)
            addSegment(rx0, ry1, rx0, ry0)
            strokeRect(rx0, ry0, rx1, ry1)
        }
    )
}

private func _lerp(_ a: CGPoint, _ b: CGPoint, _ t: Double) -> CGPoint {
    CGPoint(
        x: CGFloat(Double(a.x) + (Double(b.x) - Double(a.x)) * t),
        y: CGFloat(Double(a.y) + (Double(b.y) - Double(a.y)) * t)
    )
}

private func _cubic(_ p0: CGPoint, _ c1: CGPoint, _ c2: CGPoint, _ p3: CGPoint, _ t: Double) -> CGPoint {
    let u = 1.0 - t
    let tt = t * t
    let uu = u * u
    let uuu = uu * u
    let ttt = tt * t
    let x =
        uuu * Double(p0.x) +
        3.0 * uu * t * Double(c1.x) +
        3.0 * u * tt * Double(c2.x) +
        ttt * Double(p3.x)
    let y =
        uuu * Double(p0.y) +
        3.0 * uu * t * Double(c1.y) +
        3.0 * u * tt * Double(c2.y) +
        ttt * Double(p3.y)
    return CGPoint(x: CGFloat(x), y: CGFloat(y))
}

private extension Array {
    subscript(safe idx: Int) -> Element? {
        guard idx >= 0, idx < count else { return nil }
        return self[idx]
    }
}

private typealias _NCRGB = _RGB

private struct _NCCell: Equatable {
    var ch: String
    var fg: _NCRGB
    var bg: _NCRGB
    var styles: UInt16 = 0
}
