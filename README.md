# Stream Server

<div align="center">

**🚀 Open Source Torrent Streaming Engine**

*A modern, high-performance alternative to Stremio's closed-source `server.js`*

[![Release Build](https://github.com/perpetus/stream-server/actions/workflows/release.yml/badge.svg)](https://github.com/perpetus/stream-server/actions/workflows/release.yml)
[![License](https://img.shields.io/badge/license-MIT-blue?style=flat-square)](LICENSE)
[![Open Source](https://img.shields.io/badge/Open%20Source-✓-brightgreen?style=flat-square)](https://github.com/perpetus/stream-server)

</div>

---

## 💡 About

Stream Server is a **fully open-source** replacement for Stremio's proprietary `server.js`. While Stremio's streaming backend remains closed-source, this project provides complete transparency, community-driven development, and the freedom to run your streaming engine locally on your own terms.

**Built in Rust** for maximum performance, minimal memory footprint, and rock-solid reliability.

### Projects using Stream Server

* **[stremio-native](https://github.com/perpetus/stremio-native)**: The recommended way to use Stream Server as a complete desktop application. It integrates the server directly with a native Stremio client and player. **Stremio Native is still in the early stages of development, so expect incomplete features and breaking changes.**
* **[stremio-android](https://github.com/perpetus/stremio-android)**: A native Kotlin/Compose Stremio client app for Android that integrates `stream-server` as a local JNI library/service for high-performance torrent streaming.

---

## 🌟 Why Stream Server?

| Feature | Stream Server | Stremio server.js |
|---------|--------------|-------------------|
| **Open Source** | ✅ **Yes** (MIT License) | ❌ Closed source |
| **Performance** | ⚡ Native Rust | Node.js overhead |
| **Memory Usage** | ~50MB | ~200MB+ |
| **Control** | ✅ Full local control | ⚠️ Limited |
| **Customizable** | ✅ Fork & modify | ❌ No access |
| **HLS Transcoding** | ✅ Built-in | ✅ |
| **Seekable Streams** | ✅ Instant | ⚠️ Variable |
| **Archive Streaming** | ✅ RAR/ZIP/7Z | ✅ |

> **Seamless Migration**: Drop-in compatible with existing Stremio setups. Same API endpoints, same functionality — just faster and open source.

---

## ✨ Features

### Core Streaming
- **🚀 High Performance**: Native Rust with optional C++ libtorrent backend
- **📺 HLS Transcoding**: Real-time video transcoding via FFmpeg (master.m3u8, stream.m3u8)
- **🔧 Multiple Backends**: `librqbit` (pure Rust) or `libtorrent` (battle-tested C++)
- **📡 HTTP Range Requests**: Full support for instant seeking

### Media Support
- **📝 Subtitle Extraction**: Automatic detection, OpenSubtitles hash calculation
- **🎬 Video Probing**: FFprobe integration for track analysis
- **📦 Archive Streaming**: Direct playback from RAR, ZIP, 7Z, TAR archives

### API Compatibility
- **🔌 Stats API**: `/stats.json` for server status and torrent info
- **🌐 Network Info**: `/network-info` endpoint for interface discovery
- **💓 Heartbeat**: `/heartbeat` for health checks
- **⚙️ Settings**: Runtime-configurable via `/settings`
- **🔒 BitTorrent Privacy Controls**: DHT, PeX, LSD, encryption, interface binding, ports, and proxy settings. See [BitTorrent Settings](docs/bittorrent-settings.md).

---

## 📦 Installation

### Pre-built Binaries

Download from [Releases](https://github.com/perpetus/stream-server/releases):

| Platform | Download |
|----------|----------|
| Windows (portable) | [Download EXE](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-windows-amd64.exe) |
| Windows settings GUI | [Download EXE](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-settings-windows-amd64.exe) |
| Windows installer | [Download MSI](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-windows-amd64.msi) |
| Debian / Ubuntu | [Download DEB](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-linux-amd64.deb) |
| Linux (portable) | [Download binary](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-linux-amd64) |
| Linux settings GUI | [Download binary](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-settings-linux-amd64) |
| Linux (AppImage) | [Download AppImage](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-linux-amd64.AppImage) |
| Arch Linux | [Download package](https://github.com/perpetus/stream-server/releases/latest/download/stream-server-arch-x86_64.pkg.tar.zst) |
| Checksums and all assets | [View latest release](https://github.com/perpetus/stream-server/releases/latest) |

### Build from Source

```bash
# Default build (librqbit - recommended)
cargo build --release

# With libtorrent backend (advanced)
cargo build --release --features libtorrent --no-default-features
```

---

## 🚀 Quick Start

```bash
# Run the server
./stream-server

# Or with cargo
cargo run --release -p server
```

The server starts on `http://localhost:11470` by default (compatible with standard streaming server port).

---

## 🐳 Docker

A multi-stage `Dockerfile` builds the `libtorrent` backend the same way CI does (libtorrent 2.1.1 from source, `--features libtorrent --no-default-features`):

```bash
docker build -t stream-server:libtorrent .

docker run -d --name stream-server \
  -p 11470:11470 \
  -p 42000-42010:42000-42010/tcp \
  -p 42000-42010:42000-42010/udp \
  -v stream-server-config:/root/.config/stremio-server \
  -v stream-server-cache:/root/.cache/stremio-server \
  stream-server:libtorrent
```

- The container starts headless automatically (equivalent to `stream-server --no-tray`) — no display server required.
- Mount `/root/.config/stremio-server` (settings, HTTPS certs, logs) and `/root/.cache/stremio-server` (downloads/cache) as volumes so they survive container recreation.
- BitTorrent peer traffic defaults to ports `42000-42010` (TCP+UDP, `btListenInterfaces` in `/settings`). Publish that range (or run with `--network host`) for good peer connectivity.
- `RUST_LOG` is set to `info` in the image; override with `-e RUST_LOG=debug` for more verbose logs.

This has been built and smoke-tested end-to-end (image build + container run + `/heartbeat`, `/settings`, `/stats.json`) on an x86_64 Fedora/Docker host.

## 🖥️ Accessing & Configuring the UI

Stream Server has **no bundled web UI of its own** — it's a drop-in backend that a Stremio client (web, desktop, or mobile app) talks to, the same way it talks to Stremio's official `server.js`. To use it:

1. Open the [Stremio web app](https://web.stremio.com) or a desktop/mobile Stremio client.
2. Go to **Settings → Streaming Server**.
3. Set **Server URL** to where this container/binary is reachable, e.g. `http://192.168.1.5:11470/` (swap in your host's IP or hostname).
4. Stremio should report the server as connected; playing a torrent stream will now route through this server instead of Stremio's built-in one.

Useful endpoints for checking the server directly (from any machine that can reach it):

```bash
curl http://192.168.1.5:11470/heartbeat     # health check
curl http://192.168.1.5:11470/stats.json    # server + torrent stats
curl http://192.168.1.5:11470/settings      # current settings, including BitTorrent options
```

If desktop-app config is what you want instead, `settings-gui` (built alongside `server` on Windows/macOS/Linux desktop builds) provides a native settings window — not applicable to the headless Docker image above.

## ⚙️ Configuring libtorrent

BitTorrent/libtorrent behavior is configured at runtime via the same `/settings` API used by the UI above, and persisted to `settings.json` in the config directory (`~/.config/stremio-server/settings.json` on Linux, or the `stream-server-config` volume in Docker) — no rebuild needed. Full option reference: [docs/bittorrent-settings.md](docs/bittorrent-settings.md).

```bash
# View current settings
curl http://192.168.1.5:11470/settings

# Update just the keys you want to change
curl -X POST http://192.168.1.5:11470/settings \
  -H "Content-Type: application/json" \
  -d '{"btEnableDht": false, "btEncryptionMode": "require"}'
```

Key options: `btEnableDht`, `btEnablePex`, `btEnableLsd` (peer discovery), `btEncryptionMode` (`allow`/`require`/`disable`), `btAnonymousMode`, `btAllowMultipleConnectionsPerIp`, `btValidateHttpsTrackers`, `btSsrfMitigation`.

---

## 🔧 Build Instructions

<details>
<summary><b>🐧 Arch Linux</b></summary>

```bash
sudo pacman -S rustup
rustup default stable

# For libtorrent backend
sudo pacman -S libtorrent-rasterbar boost pkg-config
```

</details>

<details>
<summary><b>🐧 Ubuntu / Debian</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

sudo apt update
sudo apt install build-essential pkg-config libssl-dev

# For libtorrent backend
sudo apt install libtorrent-rasterbar-dev libboost-all-dev
```

</details>

<details>
<summary><b>🐧 Fedora / RHEL</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

sudo dnf install gcc gcc-c++ pkg-config openssl-devel

# For libtorrent backend
sudo dnf install rb_libtorrent-devel boost-devel
```

</details>

<details>
<summary><b>🍎 macOS</b></summary>

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# For libtorrent backend
brew install libtorrent-rasterbar boost pkg-config
```

</details>

<details>
<summary><b>🪟 Windows</b></summary>

```powershell
# 1. Install Rust from https://rustup.rs
# 2. Install Visual Studio Build Tools with "Desktop development with C++"
# 3. For libtorrent, use vcpkg with the repository's v3 triplet:
git clone https://github.com/microsoft/vcpkg
.\vcpkg\bootstrap-vcpkg.bat
.\vcpkg\vcpkg install `
  --triplet=x64-windows-v3-static-md-release `
  --overlay-triplets="$PWD\triplets" `
  --overlay-ports="$PWD\vcpkg-overlays"
```

The v3 `static-md` triplet keeps libtorrent static, compiles native release
objects with `/arch:AVX2`, and uses the dynamic MSVC runtime (`/MD`). This
matches the repository's x86-64-v3 Rust binaries and applications using
prebuilt Skia binaries.

</details>

---

## 📊 Backend Comparison

| Feature | librqbit | libtorrent |
|---------|----------|------------|
| **Language** | Pure Rust | C++ via FFI |
| **Binary Size** | Smaller | Larger |
| **Maturity** | Newer | Battle-tested |
| **DHT** | ✅ | ✅ |
| **uTP** | ✅ | ✅ |
| **Piece Deadline** | ❌ | ✅ |
| **Windows Setup** | ✅ Easy | ⚠️ Complex |

---

## 📁 Project Structure

```
stream-server/
├── server/           # HTTP server and API routes
├── enginefs/         # Torrent engine abstraction
│   └── src/backend/
│       ├── librqbit.rs   # Pure Rust backend
│       └── libtorrent.rs # Native C++ backend
└── libtorrent-sys/   # FFI bindings to libtorrent-rasterbar
```

---

## 🐛 Troubleshooting

<details>
<summary><b>libtorrent not found</b></summary>

```bash
pkg-config --exists libtorrent-rasterbar && echo "Found" || echo "Not found"
pkg-config --cflags --libs libtorrent-rasterbar
```

</details>

<details>
<summary><b>Boost not found</b></summary>

Install boost development headers:
- **Arch**: `boost`
- **Ubuntu**: `libboost-all-dev`
- **Fedora**: `boost-devel`
- **macOS**: `brew install boost`

</details>

---

## 📄 License

MIT License - see [LICENSE](LICENSE) for details.

---

<p align="center">
  <b>⭐ Star this repo if you find it useful!</b>
</p>

## Keywords

`stremio server.js alternative` `open source streaming engine` `torrent streaming` `local streaming` `desktop streaming` `hls transcoding` `rust torrent` `libtorrent` `video streaming server` `media engine` `torrent player` `stream torrents` `stremio alternative` `enginefs` `stremio open source`
