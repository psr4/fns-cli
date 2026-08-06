# FNS CLI (Rust)

[简体中文](README.zh-CN.md) | [English](README.md)

---

A high-performance Rust command-line client for bidirectional, near real-time Obsidian note sync. It works with [Fast Note Sync Service](https://github.com/haierkeys/fast-note-sync-service) and is intended for headless Linux servers, offering sync capabilities comparable to the Obsidian desktop/mobile plugin.

## Features

- **Bidirectional real-time sync**: local changes are pushed to the server; remote changes are pulled to the local vault
- **Full content sync**: `.md` notes, attachments (images, PDF, etc.), and `.obsidian/` configuration
- **Auto-reconnect**: exponential backoff with automatic re-sync after connection drops
- **Echo suppression**: prevents feedback loops when receiving server updates
- **Message queuing**: pre-auth messages are queued and sent after authentication
- **Incremental sync**: uses `lastSyncTime` to sync only what changed
- **Binary chunking**: large files are split into chunks for reliable transfer

## Project Layout

```
fns-cli/
├── src/
│   ├── lib.rs              # Library entry point
│   ├── main.rs             # CLI entry point
│   ├── config.rs           # Configuration parsing
│   ├── ws_client.rs        # WebSocket client with reconnect
│   ├── sync/
│   │   ├── coordinator.rs  # Sync orchestration
│   │   ├── note.rs         # Note (.md) sync engine
│   │   ├── file.rs         # File (binary) sync engine
│   │   ├── setting.rs      # Config (.obsidian/) sync engine
│   │   └── folder.rs       # Folder operations
│   ├── protocol.rs         # WebSocket message encoding/decoding
│   ├── watcher.rs          # File system watcher
│   ├── state.rs            # Sync state persistence
│   └── hash.rs             # Content hashing
├── Cargo.toml
└── config.yaml             # Example configuration
```

## Requirements

- Rust 1.85+ (edition 2024)
- Linux / macOS

## Build

```bash
cd fns-cli
cargo build --release
```

The binary will be at `target/release/fns-cli`.

## Configuration

Create a `config.yaml`:

```yaml
server:
  api: "https://your-server-address"   # Fast Note Sync Service base URL
  token: "your_api_token"              # API token from the admin panel
  vault: "notes"                       # Vault name; must match the Obsidian plugin

sync:
  watch_path: "./vault"                # Local vault path (relative or absolute)
  sync_notes: true                     # Sync .md files
  sync_files: true                     # Sync attachments
  sync_config: true                    # Sync .obsidian/ config
  exclude_patterns:
    - ".git/**"
    - ".trash/**"
    - "*.tmp"
    - "*.bak"
  file_chunk_size: 524288              # Chunk size for binary transfers

client:
  reconnect_max_retries: 15            # Max reconnect attempts (0 = unlimited)
  reconnect_base_delay: 3              # Base delay in seconds (exponential backoff)
  heartbeat_interval: 30               # WebSocket heartbeat interval

logging:
  level: "INFO"                        # TRACE, DEBUG, INFO, WARN, ERROR
  file: ""                             # Log file path (empty = stdout only)
```

### How to obtain a token

1. Open the Fast Note Sync Service web UI (e.g. `https://your-server-address`)
2. Sign in
3. Click **"Copy API Config"**
4. Copy `api`, `apiToken`, and `vault` from the JSON into `config.yaml`

### Environment variables (optional)

Override settings without modifying the config file:

```bash
export FNS_API="https://your-server-address"
export FNS_TOKEN="your_api_token"
```

## Usage

### Run (continuous sync)

```bash
./fns-cli run -c config.yaml
```

Long-running mode: initial sync + file watcher + receive remote updates.

### Sync (one-shot)

```bash
./fns-cli sync -c config.yaml
```

Full bidirectional sync, then exit.

### Pull (download only)

```bash
./fns-cli pull -c config.yaml
```

Pull remote changes only, then exit.

### Push (upload only)

```bash
./fns-cli push -c config.yaml
```

Push all local files, then exit.

### Status

```bash
./fns-cli status -c config.yaml
```

Show configuration and sync state.

## Daemon & Boot (systemd)

On Linux, systemd provides auto-restart on crash, start on boot, and centralized logs.

Create `/etc/systemd/system/fns-cli.service`:

```ini
[Unit]
Description=FNS CLI - Obsidian vault sync (Rust)
Documentation=https://github.com/haierkeys/fast-note-sync-service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=your_user
Group=your_user
WorkingDirectory=/opt/fns-cli
Environment=RUST_LOG=info
ExecStart=/opt/fns-cli/fns-cli run -c /opt/fns-cli/config.yaml
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable fns-cli
sudo systemctl start fns-cli
sudo systemctl status fns-cli
```

View logs:

```bash
journalctl -u fns-cli -f
journalctl -u fns-cli --since today
```

## Sync Behavior

### Run Flow

```
1. WebSocket connect → authenticate
2. Incremental pull (NoteSync + FileSync + SettingSync)
3. Start file watcher on local vault
4. Continuous bidirectional sync
5. On disconnect → reconnect with exponential backoff → incremental re-sync
```

### Echo Suppression

The client tracks outbound changes in an echo cache to avoid re-uploading files that were just received from the server. This prevents feedback loops in bidirectional sync.

### Reconnection Handling

When the WebSocket disconnects:
1. Wait with exponential backoff (base 3s, max 300s)
2. Reconnect and authenticate
3. Trigger incremental re-sync to catch up on missed changes
4. Resume normal operation

### State File

Progress is stored in `vault/.fns_state.json`. After restart, sync resumes incrementally from the last checkpoint.

## Development

### Run Tests

```bash
cargo test --lib
```

### Run with Debug Logging

```bash
RUST_LOG=debug ./fns-cli run -c config.yaml
```

### Build with Clippy

```bash
cargo clippy -- -D warnings
```

## Comparison with Python Version

| Feature | Python | Rust |
|---------|--------|------|
| Bidirectional sync | ✅ | ✅ |
| Auto-reconnect | ✅ | ✅ |
| Echo suppression | ✅ | ✅ |
| Message queue | ✅ | ✅ |
| Move event handling | ✅ | ✅ |
| Binary chunking | ✅ | ✅ |
| Memory usage | Higher | Lower |
| Startup time | Slower | Faster |
| Binary | Requires Python | Standalone |

## Related Projects

- [Fast Note Sync Service](https://github.com/haierkeys/fast-note-sync-service) — backend server
- [obsidian-fast-note-sync](https://github.com/haierkeys/obsidian-fast-note-sync) — Obsidian plugin
- [FastNodeSync-CLI](https://github.com/Go1c/FastNodeSync-CLI) — Python CLI version

## License

MIT
