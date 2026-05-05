FROM swiftlang/swift:nightly-6.1-jammy

# ── System dependencies ──────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Build essentials for notcurses
    build-essential cmake pkg-config \
    libunistring-dev libdeflate-dev libgpm-dev \
    libncurses-dev libtinfo-dev \
    # X11 virtual framebuffer + input + screenshots + kitty X11 deps
    xvfb x11-utils xdotool scrot imagemagick \
    libxcb-xkb1 libxkbcommon-x11-0 libxkbcommon0 \
    libgl1-mesa-dri libgl1-mesa-glx libegl1-mesa libgles2-mesa \
    libdbus-1-3 libxcursor1 libxrandr2 libxi6 libxinerama1 \
    libfontconfig1 libharfbuzz0b libpng16-16 liblcms2-2 \
    libatk1.0-0 libatk-bridge2.0-0 libgtk-3-0 \
    libxcomposite1 libxdamage1 libxfixes3 libpango-1.0-0 libcairo2 \
    libcups2 libasound2 \
    libnss3 \
    librsync2 libxxhash0 libcrypt1 \
    dbus dbus-x11 \
    # Terminal multiplexer (fallback tests)
    tmux \
    # Video recording
    ffmpeg \
    # Deterministic fonts
    fonts-dejavu-core \
    # Terminal info database
    ncurses-term \
    # For odiff
    nodejs npm \
    # Misc
    curl ca-certificates git python3 \
    && rm -rf /var/lib/apt/lists/*

# ── Generate machine-id for DBUS (needed by kitty) ───────────────────────────
RUN mkdir -p /var/lib/dbus \
    && dbus-uuidgen --ensure=/var/lib/dbus/machine-id \
    && cp /var/lib/dbus/machine-id /etc/machine-id

# ── Build notcurses 3.x from source ─────────────────────────────────────────
# The apt package on Ubuntu 22.04 is too old for our APIs (NCTYPE_PRESS, etc.)
RUN git clone --depth 1 --branch v3.0.11 https://github.com/dankamongmen/notcurses.git /tmp/notcurses \
    && cd /tmp/notcurses && mkdir build && cd build \
    && cmake .. \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DUSE_MULTIMEDIA=none \
        -DUSE_DOCTEST=off \
        -DUSE_GPM=off \
        -DUSE_QRCODEGEN=off \
        -DUSE_PANDOC=off \
        -DUSE_DOXYGEN=off \
    && make -j"$(nproc)" && make install \
    && ldconfig \
    && rm -rf /tmp/notcurses

# ── Install kitty terminal (official binary, supports Kitty graphics) ────────
RUN curl -fsSL https://sw.kovidgoyal.net/kitty/installer.sh | sh /dev/stdin launch=n \
    && ln -sf ~/.local/kitty.app/bin/kitty /usr/local/bin/kitty \
    && ln -sf ~/.local/kitty.app/bin/kitten /usr/local/bin/kitten

# ── Install VHS (charmbracelet) ──────────────────────────────────────────────
RUN ARCH="$(uname -m)" \
    && case "$ARCH" in \
        x86_64|amd64) VHS_ARCH="x86_64" ;; \
        aarch64|arm64) VHS_ARCH="arm64" ;; \
        *) echo "Unsupported arch for VHS: $ARCH" >&2; exit 1 ;; \
       esac \
    && curl -fsSL "https://github.com/charmbracelet/vhs/releases/download/v0.8.0/vhs_0.8.0_Linux_${VHS_ARCH}.tar.gz" \
        | tar xz --strip-components=1 -C /usr/local/bin "vhs_0.8.0_Linux_${VHS_ARCH}/vhs"

# ── Install ttyd (required by VHS) ────────────────────────────────────────────
RUN ARCH="$(uname -m)" \
    && case "$ARCH" in \
        x86_64|amd64) TTYD_ARCH="x86_64" ;; \
        aarch64|arm64) TTYD_ARCH="aarch64" ;; \
        *) echo "Unsupported arch for ttyd: $ARCH" >&2; exit 1 ;; \
       esac \
    && curl -fsSL "https://github.com/tsl0922/ttyd/releases/download/1.7.7/ttyd.${TTYD_ARCH}" \
        -o /usr/local/bin/ttyd \
    && chmod +x /usr/local/bin/ttyd

# ── Install odiff for pixel comparison ───────────────────────────────────────
RUN npm install -g odiff-bin 2>/dev/null || true

# ── Copy project and build ───────────────────────────────────────────────────
WORKDIR /app
COPY Package.swift Package.resolved ./
COPY Sources/ Sources/
COPY Tests/ Tests/

# Build KitchenSink — use || true since emit-module warnings look like errors
# but the binary still links successfully
RUN swift build --product KitchenSink 2>&1; \
    test -f "$(swift build --show-bin-path 2>/dev/null)/KitchenSink" \
    && echo "KitchenSink built OK" \
    || (echo "FATAL: KitchenSink binary not found" && exit 1)

# Copy test infrastructure (after build to leverage caching)
COPY scripts/ scripts/
COPY Tests/tui/ Tests/tui/

RUN chmod +x scripts/*.sh

ENV DISPLAY=:99
ENV TERM=xterm-kitty
ENV COLORTERM=truecolor

ENTRYPOINT ["scripts/tui-test.sh"]
