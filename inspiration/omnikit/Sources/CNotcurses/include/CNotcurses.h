#pragma once

#include <notcurses/notcurses.h>
#include <notcurses/nckeys.h>

// ── Input key constants (macros not directly importable by Swift) ────────────

uint32_t omni_nckey_button1(void);
uint32_t omni_nckey_button2(void);
uint32_t omni_nckey_button3(void);
uint32_t omni_nckey_motion(void);
uint32_t omni_nckey_scroll_up(void);
uint32_t omni_nckey_scroll_down(void);
uint32_t omni_nckey_esc(void);
uint32_t omni_nckey_backspace(void);
uint32_t omni_nckey_enter(void);
uint32_t omni_nckey_up(void);
uint32_t omni_nckey_down(void);
uint32_t omni_nckey_left(void);
uint32_t omni_nckey_right(void);
uint32_t omni_nckey_home(void);
uint32_t omni_nckey_end(void);
uint32_t omni_nckey_delete(void);
uint32_t omni_nckey_resize(void);
uint32_t omni_nckey_pgup(void);
uint32_t omni_nckey_pgdown(void);
uint32_t omni_nckey_tab(void);

// Function keys F1–F12.
uint32_t omni_nckey_f01(void);
uint32_t omni_nckey_f02(void);
uint32_t omni_nckey_f03(void);
uint32_t omni_nckey_f04(void);
uint32_t omni_nckey_f05(void);
uint32_t omni_nckey_f06(void);
uint32_t omni_nckey_f07(void);
uint32_t omni_nckey_f08(void);
uint32_t omni_nckey_f09(void);
uint32_t omni_nckey_f10(void);
uint32_t omni_nckey_f11(void);
uint32_t omni_nckey_f12(void);

unsigned omni_ncmice_all_events(void);

// ── Modifier helpers ────────────────────────────────────────────────────────

uint32_t omni_ncinput_shift(const struct ncinput* ni);
uint32_t omni_ncinput_ctrl(const struct ncinput* ni);
uint32_t omni_ncinput_alt(const struct ncinput* ni);
uint32_t omni_ncinput_meta(const struct ncinput* ni);
uint32_t omni_ncinput_super(const struct ncinput* ni);

// ── Input with timeout ──────────────────────────────────────────────────────

// Block up to `timeout_ms` milliseconds waiting for input.
// Returns the key ID (0 if timeout expired, UINT32_MAX on error).
uint32_t omni_notcurses_get(struct notcurses* nc, unsigned timeout_ms, struct ncinput* ni);

// ── Resize ──────────────────────────────────────────────────────────────────

// Refresh after SIGWINCH / NCKEY_RESIZE.  Writes new rows/cols.
int omni_notcurses_refresh(struct notcurses* nc, unsigned* rows, unsigned* cols);

// ── Text style constants (macros) ───────────────────────────────────────────

unsigned omni_ncstyle_none(void);
unsigned omni_ncstyle_bold(void);
unsigned omni_ncstyle_italic(void);
unsigned omni_ncstyle_underline(void);
unsigned omni_ncstyle_undercurl(void);
unsigned omni_ncstyle_struck(void);

// Convenience wrapper: set styles atomically on a plane.
void omni_ncplane_set_styles(struct ncplane* n, unsigned stylebits);

// ── Capability queries ──────────────────────────────────────────────────────

unsigned omni_notcurses_supported_styles(struct notcurses* nc);
int omni_notcurses_canbraille(const struct notcurses* nc);
int omni_notcurses_cantruecolor(const struct notcurses* nc);
int omni_notcurses_canhalfblock(const struct notcurses* nc);
int omni_notcurses_canfade(const struct notcurses* nc);

// ── Hardware cursor ─────────────────────────────────────────────────────────

int omni_notcurses_cursor_enable(struct notcurses* nc, int y, int x);
int omni_notcurses_cursor_disable(struct notcurses* nc);

// ── Fade transitions ────────────────────────────────────────────────────────

int omni_ncplane_fadein(struct ncplane* n, unsigned ms);
int omni_ncplane_fadeout(struct ncplane* n, unsigned ms);

// ── Overlay plane helpers ───────────────────────────────────────────────────

// Set the base cell of a plane to be fully transparent.
// This makes erased/unpainted cells transparent so layers below show through.
int omni_ncplane_set_base_transparent(struct ncplane* n);

// ── Plane option constants ──────────────────────────────────────────────────

uint64_t omni_ncplane_option_vscroll(void);

// ── ncmenu helpers ──────────────────────────────────────────────────────────

uint64_t omni_ncmenu_option_bottom(void);

// Create a flat ncmenu (single section at the top or bottom of the plane).
// `labels` is an array of `count` C strings.
// Returns NULL on failure.
struct ncmenu* omni_ncmenu_create_flat(struct ncplane* n,
                                       const char* section_name,
                                       const char** labels, int count,
                                       uint64_t flags);
bool omni_ncmenu_offer_input(struct ncmenu* menu, const struct ncinput* ni);
const char* omni_ncmenu_selected(struct ncmenu* menu, struct ncinput* ni);
void omni_ncmenu_destroy(struct ncmenu* menu);
struct ncplane* omni_ncmenu_plane(struct ncmenu* menu);
int omni_ncmenu_unroll(struct ncmenu* menu, int section_idx);
int omni_ncmenu_rollup(struct ncmenu* menu);

// ── ncselector helpers ──────────────────────────────────────────────────────

// Create an ncselector from parallel arrays of option/desc strings.
struct ncselector* omni_ncselector_create(struct ncplane* n,
                                          const char** options,
                                          const char** descs,
                                          int count,
                                          unsigned defidx,
                                          unsigned maxdisplay,
                                          const char* title,
                                          const char* footer);
bool omni_ncselector_offer_input(struct ncselector* sel, const struct ncinput* ni);
const char* omni_ncselector_selected(struct ncselector* sel);
void omni_ncselector_destroy(struct ncselector* sel);
struct ncplane* omni_ncselector_plane(struct ncselector* sel);

// ── ncreader helpers ────────────────────────────────────────────────────────

uint64_t omni_ncreader_option_horscroll(void);
uint64_t omni_ncreader_option_cursor(void);
uint64_t omni_ncreader_option_nocmdkeys(void);

struct ncreader* omni_ncreader_create(struct ncplane* n, uint64_t flags);
bool omni_ncreader_offer_input(struct ncreader* reader, const struct ncinput* ni);
char* omni_ncreader_contents(const struct ncreader* reader);
void omni_ncreader_destroy(struct ncreader* reader);
int omni_ncreader_clear(struct ncreader* reader);
struct ncplane* omni_ncreader_plane(struct ncreader* reader);

// ── Visual/pixel helpers ────────────────────────────────────────────────────

int omni_notcurses_cellpix(struct notcurses* nc, unsigned* cdimy, unsigned* cdimx,
                           unsigned* maxpixely, unsigned* maxpixelx);
uint32_t omni_ncblit_pixel(void);
uint64_t omni_ncvisual_option_blend(void);
uint64_t omni_ncvisual_option_nodegrade(void);

// ── Terminal safety helpers ─────────────────────────────────────────────────

void omni_install_signal_handlers(void);
int omni_signal_received(void);
void omni_restore_terminal(void);
