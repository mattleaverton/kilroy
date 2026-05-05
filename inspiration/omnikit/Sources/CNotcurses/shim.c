// This target exists to provide a stable Clang module for notcurses, and to surface
// some macro-defined constants that are not directly importable into Swift.

#include "CNotcurses.h"

#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

// ── Input key constants ─────────────────────────────────────────────────────

uint32_t omni_nckey_button1(void) { return (uint32_t)NCKEY_BUTTON1; }
uint32_t omni_nckey_button2(void) { return (uint32_t)NCKEY_BUTTON2; }
uint32_t omni_nckey_button3(void) { return (uint32_t)NCKEY_BUTTON3; }
uint32_t omni_nckey_motion(void) { return (uint32_t)NCKEY_MOTION; }
uint32_t omni_nckey_scroll_up(void) { return (uint32_t)NCKEY_SCROLL_UP; }
uint32_t omni_nckey_scroll_down(void) { return (uint32_t)NCKEY_SCROLL_DOWN; }
uint32_t omni_nckey_esc(void) { return (uint32_t)NCKEY_ESC; }
uint32_t omni_nckey_backspace(void) { return (uint32_t)NCKEY_BACKSPACE; }
uint32_t omni_nckey_enter(void) { return (uint32_t)NCKEY_ENTER; }
uint32_t omni_nckey_up(void) { return (uint32_t)NCKEY_UP; }
uint32_t omni_nckey_down(void) { return (uint32_t)NCKEY_DOWN; }
uint32_t omni_nckey_left(void) { return (uint32_t)NCKEY_LEFT; }
uint32_t omni_nckey_right(void) { return (uint32_t)NCKEY_RIGHT; }
uint32_t omni_nckey_home(void) { return (uint32_t)NCKEY_HOME; }
uint32_t omni_nckey_end(void) { return (uint32_t)NCKEY_END; }
uint32_t omni_nckey_delete(void) { return (uint32_t)NCKEY_DEL; }
uint32_t omni_nckey_resize(void) { return (uint32_t)NCKEY_RESIZE; }
uint32_t omni_nckey_pgup(void) { return (uint32_t)NCKEY_PGUP; }
uint32_t omni_nckey_pgdown(void) { return (uint32_t)NCKEY_PGDOWN; }
uint32_t omni_nckey_tab(void) { return (uint32_t)NCKEY_TAB; }

uint32_t omni_nckey_f01(void) { return (uint32_t)NCKEY_F01; }
uint32_t omni_nckey_f02(void) { return (uint32_t)NCKEY_F02; }
uint32_t omni_nckey_f03(void) { return (uint32_t)NCKEY_F03; }
uint32_t omni_nckey_f04(void) { return (uint32_t)NCKEY_F04; }
uint32_t omni_nckey_f05(void) { return (uint32_t)NCKEY_F05; }
uint32_t omni_nckey_f06(void) { return (uint32_t)NCKEY_F06; }
uint32_t omni_nckey_f07(void) { return (uint32_t)NCKEY_F07; }
uint32_t omni_nckey_f08(void) { return (uint32_t)NCKEY_F08; }
uint32_t omni_nckey_f09(void) { return (uint32_t)NCKEY_F09; }
uint32_t omni_nckey_f10(void) { return (uint32_t)NCKEY_F10; }
uint32_t omni_nckey_f11(void) { return (uint32_t)NCKEY_F11; }
uint32_t omni_nckey_f12(void) { return (uint32_t)NCKEY_F12; }

unsigned omni_ncmice_all_events(void) { return (unsigned)NCMICE_ALL_EVENTS; }

// ── Modifier helpers ────────────────────────────────────────────────────────

uint32_t omni_ncinput_shift(const struct ncinput* ni){
  if(ni == NULL) return 0;
  return (ni->modifiers & NCKEY_MOD_SHIFT) ? 1u : 0u;
}

uint32_t omni_ncinput_ctrl(const struct ncinput* ni){
  if(ni == NULL) return 0;
  return (ni->modifiers & NCKEY_MOD_CTRL) ? 1u : 0u;
}

uint32_t omni_ncinput_alt(const struct ncinput* ni){
  if(ni == NULL) return 0;
  return (ni->modifiers & NCKEY_MOD_ALT) ? 1u : 0u;
}

uint32_t omni_ncinput_meta(const struct ncinput* ni){
  if(ni == NULL) return 0;
  return (ni->modifiers & NCKEY_MOD_META) ? 1u : 0u;
}

uint32_t omni_ncinput_super(const struct ncinput* ni){
  if(ni == NULL) return 0;
  return (ni->modifiers & NCKEY_MOD_SUPER) ? 1u : 0u;
}

// ── Input with timeout ──────────────────────────────────────────────────────

uint32_t omni_notcurses_get(struct notcurses* nc, unsigned timeout_ms, struct ncinput* ni){
  if(!nc || !ni) return UINT32_MAX;
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec  += (time_t)(timeout_ms / 1000u);
  ts.tv_nsec += (long)(timeout_ms % 1000u) * 1000000L;
  if(ts.tv_nsec >= 1000000000L){
    ts.tv_sec  += 1;
    ts.tv_nsec -= 1000000000L;
  }
  return notcurses_get(nc, &ts, ni);
}

// ── Resize ──────────────────────────────────────────────────────────────────

int omni_notcurses_refresh(struct notcurses* nc, unsigned* rows, unsigned* cols){
  if(!nc || !rows || !cols) return -1;
  return notcurses_refresh(nc, rows, cols);
}

// ── Text style constants ────────────────────────────────────────────────────

unsigned omni_ncstyle_none(void)      { return (unsigned)NCSTYLE_NONE; }
unsigned omni_ncstyle_bold(void)      { return (unsigned)NCSTYLE_BOLD; }
unsigned omni_ncstyle_italic(void)    { return (unsigned)NCSTYLE_ITALIC; }
unsigned omni_ncstyle_underline(void) { return (unsigned)NCSTYLE_UNDERLINE; }
unsigned omni_ncstyle_undercurl(void) { return (unsigned)NCSTYLE_UNDERCURL; }
unsigned omni_ncstyle_struck(void)    { return (unsigned)NCSTYLE_STRUCK; }

void omni_ncplane_set_styles(struct ncplane* n, unsigned stylebits){
  if(!n) return;
  ncplane_off_styles(n, NCSTYLE_MASK);
  if(stylebits != NCSTYLE_NONE){
    ncplane_on_styles(n, stylebits);
  }
}

// ── Capability queries ──────────────────────────────────────────────────────

unsigned omni_notcurses_supported_styles(struct notcurses* nc){
  if(!nc) return 0;
  return notcurses_supported_styles(nc);
}

int omni_notcurses_canbraille(const struct notcurses* nc){
  if(!nc) return 0;
  return notcurses_canbraille(nc) ? 1 : 0;
}

int omni_notcurses_cantruecolor(const struct notcurses* nc){
  if(!nc) return 0;
  return notcurses_cantruecolor(nc) ? 1 : 0;
}

int omni_notcurses_canhalfblock(const struct notcurses* nc){
  if(!nc) return 0;
  return notcurses_canhalfblock(nc) ? 1 : 0;
}

int omni_notcurses_canfade(const struct notcurses* nc){
  if(!nc) return 0;
  return notcurses_canfade(nc) ? 1 : 0;
}

// ── Hardware cursor ─────────────────────────────────────────────────────────

int omni_notcurses_cursor_enable(struct notcurses* nc, int y, int x){
  if(!nc) return -1;
  return notcurses_cursor_enable(nc, y, x);
}

int omni_notcurses_cursor_disable(struct notcurses* nc){
  if(!nc) return -1;
  return notcurses_cursor_disable(nc);
}

// ── Fade transitions ────────────────────────────────────────────────────────

int omni_ncplane_fadein(struct ncplane* n, unsigned ms){
  if(!n) return -1;
  struct timespec ts;
  ts.tv_sec  = (time_t)(ms / 1000u);
  ts.tv_nsec = (long)(ms % 1000u) * 1000000L;
  return ncplane_fadein(n, &ts, NULL, NULL);
}

int omni_ncplane_fadeout(struct ncplane* n, unsigned ms){
  if(!n) return -1;
  struct timespec ts;
  ts.tv_sec  = (time_t)(ms / 1000u);
  ts.tv_nsec = (long)(ms % 1000u) * 1000000L;
  return ncplane_fadeout(n, &ts, NULL, NULL);
}

// ── Overlay plane helpers ───────────────────────────────────────────────────

int omni_ncplane_set_base_transparent(struct ncplane* n){
  if(!n) return -1;
  uint64_t channels = 0;
  ncchannels_set_fg_alpha(&channels, NCALPHA_TRANSPARENT);
  ncchannels_set_bg_alpha(&channels, NCALPHA_TRANSPARENT);
  return ncplane_set_base(n, " ", 0, channels);
}

// ── Plane option constants ──────────────────────────────────────────────────

uint64_t omni_ncplane_option_vscroll(void){
  return (uint64_t)NCPLANE_OPTION_VSCROLL;
}

// ── ncmenu helpers ──────────────────────────────────────────────────────────

uint64_t omni_ncmenu_option_bottom(void){
  return (uint64_t)NCMENU_OPTION_BOTTOM;
}

struct ncmenu* omni_ncmenu_create_flat(struct ncplane* n,
                                       const char* section_name,
                                       const char** labels, int count,
                                       uint64_t flags){
  if(!n || !labels || count <= 0) return NULL;

  struct ncmenu_item* items = calloc((size_t)count, sizeof(struct ncmenu_item));
  if(!items) return NULL;
  for(int i = 0; i < count; i++){
    items[i].desc = labels[i];
    memset(&items[i].shortcut, 0, sizeof(struct ncinput));
  }

  struct ncmenu_section section;
  memset(&section, 0, sizeof(section));
  section.name = section_name ? section_name : "";
  section.itemcount = count;
  section.items = items;

  ncmenu_options opts;
  memset(&opts, 0, sizeof(opts));
  opts.sections = &section;
  opts.sectioncount = 1;
  opts.flags = flags;

  struct ncmenu* menu = ncmenu_create(n, &opts);
  free(items);
  return menu;
}

bool omni_ncmenu_offer_input(struct ncmenu* menu, const struct ncinput* ni){
  if(!menu || !ni) return false;
  return ncmenu_offer_input(menu, ni);
}

const char* omni_ncmenu_selected(struct ncmenu* menu, struct ncinput* ni){
  if(!menu) return NULL;
  return ncmenu_selected(menu, ni);
}

void omni_ncmenu_destroy(struct ncmenu* menu){
  if(menu) ncmenu_destroy(menu);
}

struct ncplane* omni_ncmenu_plane(struct ncmenu* menu){
  if(!menu) return NULL;
  return ncmenu_plane(menu);
}

int omni_ncmenu_unroll(struct ncmenu* menu, int section_idx){
  if(!menu) return -1;
  return ncmenu_unroll(menu, section_idx);
}

int omni_ncmenu_rollup(struct ncmenu* menu){
  if(!menu) return -1;
  return ncmenu_rollup(menu);
}

// ── ncselector helpers ──────────────────────────────────────────────────────

struct ncselector* omni_ncselector_create(struct ncplane* n,
                                          const char** options,
                                          const char** descs,
                                          int count,
                                          unsigned defidx,
                                          unsigned maxdisplay,
                                          const char* title,
                                          const char* footer){
  if(!n || !options || count <= 0) return NULL;

  // Allocate count+1 items; the last entry must have option=NULL to
  // terminate the list (ncselector_create reads until NULL sentinel).
  struct ncselector_item* items = calloc((size_t)(count + 1), sizeof(struct ncselector_item));
  if(!items) return NULL;
  for(int i = 0; i < count; i++){
    items[i].option = options[i];
    items[i].desc = descs ? descs[i] : "";
  }
  // items[count] is already zeroed by calloc (option=NULL sentinel).

  ncselector_options opts;
  memset(&opts, 0, sizeof(opts));
  opts.title = title;
  opts.footer = footer;
  opts.items = items;
  opts.defidx = defidx;
  opts.maxdisplay = maxdisplay > 0 ? maxdisplay : (unsigned)count;

  struct ncselector* sel = ncselector_create(n, &opts);
  free(items);
  return sel;
}

bool omni_ncselector_offer_input(struct ncselector* sel, const struct ncinput* ni){
  if(!sel || !ni) return false;
  return ncselector_offer_input(sel, ni);
}

const char* omni_ncselector_selected(struct ncselector* sel){
  if(!sel) return NULL;
  return ncselector_selected(sel);
}

void omni_ncselector_destroy(struct ncselector* sel){
  if(!sel) return;
  ncselector_destroy(sel, NULL);
}

struct ncplane* omni_ncselector_plane(struct ncselector* sel){
  if(!sel) return NULL;
  return ncselector_plane(sel);
}

// ── ncreader helpers ────────────────────────────────────────────────────────

uint64_t omni_ncreader_option_horscroll(void){ return (uint64_t)NCREADER_OPTION_HORSCROLL; }
uint64_t omni_ncreader_option_cursor(void)   { return (uint64_t)NCREADER_OPTION_CURSOR; }
uint64_t omni_ncreader_option_nocmdkeys(void){ return (uint64_t)NCREADER_OPTION_NOCMDKEYS; }

struct ncreader* omni_ncreader_create(struct ncplane* n, uint64_t flags){
  if(!n) return NULL;
  ncreader_options opts;
  memset(&opts, 0, sizeof(opts));
  opts.flags = flags;
  return ncreader_create(n, &opts);
}

bool omni_ncreader_offer_input(struct ncreader* reader, const struct ncinput* ni){
  if(!reader || !ni) return false;
  return ncreader_offer_input(reader, ni);
}

char* omni_ncreader_contents(const struct ncreader* reader){
  if(!reader) return NULL;
  return ncreader_contents(reader);
}

void omni_ncreader_destroy(struct ncreader* reader){
  if(!reader) return;
  ncreader_destroy(reader, NULL);
}

int omni_ncreader_clear(struct ncreader* reader){
  if(!reader) return -1;
  return ncreader_clear(reader);
}

struct ncplane* omni_ncreader_plane(struct ncreader* reader){
  if(!reader) return NULL;
  return ncreader_plane(reader);
}

// ── Visual/pixel helpers ────────────────────────────────────────────────────

int omni_notcurses_cellpix(struct notcurses* nc, unsigned* cdimy, unsigned* cdimx,
                           unsigned* maxpixely, unsigned* maxpixelx){
  if(!nc || !cdimy || !cdimx || !maxpixely || !maxpixelx){
    return -1;
  }
  ncvgeom geom;
  memset(&geom, 0, sizeof(geom));
  if(ncvisual_geom(nc, NULL, NULL, &geom)){
    return -1;
  }
  *cdimy = geom.cdimy;
  *cdimx = geom.cdimx;
  *maxpixely = geom.maxpixely;
  *maxpixelx = geom.maxpixelx;
  return 0;
}

uint32_t omni_ncblit_pixel(void){ return (uint32_t)NCBLIT_PIXEL; }
uint64_t omni_ncvisual_option_blend(void){ return (uint64_t)NCVISUAL_OPTION_BLEND; }
uint64_t omni_ncvisual_option_nodegrade(void){ return (uint64_t)NCVISUAL_OPTION_NODEGRADE; }

// ── Terminal safety ─────────────────────────────────────────────────────────

static volatile sig_atomic_t g_omni_signal = 0;

static void
omni_signal_handler(int signo){
  g_omni_signal = signo;
}

void omni_install_signal_handlers(void){
  signal(SIGINT, omni_signal_handler);
  signal(SIGTERM, omni_signal_handler);
  signal(SIGHUP, omni_signal_handler);
  signal(SIGQUIT, omni_signal_handler);
}

int omni_signal_received(void){
  return (int)g_omni_signal;
}

void omni_restore_terminal(void){
  // Clean up terminal state after notcurses exits:
  //   1. Delete all kitty graphics placements and images
  //   2. Reset SGR attributes
  //   3. Show cursor
  //   4. Exit alternate screen buffer
  //   5. Disable bracketed paste
  //   6. Disable mouse reporting
  //   7. Reset kitty keyboard protocol
  const char* seq =
    "\x1b_Ga=d\x1b\\"             /* kitty: delete all images */
    "\x1b_Ga=d,d=A\x1b\\"         /* kitty: delete all placements */
    "\x1b[0m"                      /* reset SGR */
    "\x1b[?25h"                    /* show cursor */
    "\x1b[?1049l"                  /* exit alt screen */
    "\x1b[?2004l"                  /* disable bracketed paste */
    "\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1006l" /* disable mouse modes */
    "\x1b[>0u"                     /* reset kitty keyboard protocol */
    "\x1b" "c"                     /* full terminal reset (RIS) */
    ;
  (void)write(STDOUT_FILENO, seq, strlen(seq));
}
