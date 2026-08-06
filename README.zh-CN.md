# FNS CLI (Rust)

一个高性能的 Rust 命令行客户端，用于 Obsidian 笔记的双向近实时同步。与 [Fast Note Sync Service](https://github.com/haierkeys/fast-note-sync-service) 配合使用，适用于无头 Linux 服务器，提供与 Obsidian 桌面/移动端插件相当的同步能力。

## 功能特性

- **双向实时同步**：本地变更推送到服务器；远程变更拉取到本地仓库
- **全内容同步**：`.md` 笔记、附件（图片、PDF 等）、`.obsidian/` 配置
- **自动重连**：指数退避算法，连接断开后自动重连并重新同步
- **回声抑制**：防止接收服务器更新时产生反馈循环
- **消息队列**：认证前的消息排队，认证后自动发送
- **增量同步**：使用 `lastSyncTime` 仅同步变更内容
- **二进制分块**：大文件分块传输，确保可靠性

## 项目结构

```
fns-cli/
├── src/
│   ├── lib.rs              # 库入口
│   ├── main.rs             # CLI 入口
│   ├── config.rs           # 配置解析
│   ├── ws_client.rs        # WebSocket 客户端（含重连）
│   ├── sync/
│   │   ├── coordinator.rs  # 同步协调器
│   │   ├── note.rs         # 笔记 (.md) 同步引擎
│   │   ├── file.rs         # 文件（二进制）同步引擎
│   │   ├── setting.rs      # 配置 (.obsidian/) 同步引擎
│   │   └── folder.rs       # 文件夹操作
│   ├── protocol.rs         # WebSocket 消息编解码
│   ├── watcher.rs          # 文件系统监视器
│   ├── state.rs            # 同步状态持久化
│   └── hash.rs             # 内容哈希计算
├── Cargo.toml
└── config.yaml             # 示例配置
```

## 环境要求

- Rust 1.85+ (edition 2024)
- Linux / macOS

## 构建

```bash
cd fns-cli
cargo build --release
```

编译后的二进制文件位于 `target/release/fns-cli`。

## 配置

创建 `config.yaml`：

```yaml
server:
  api: "https://your-server-address"   # Fast Note Sync Service 地址
  token: "your_api_token"              # 管理面板获取的 API token
  vault: "notes"                       # 仓库名称，需与 Obsidian 插件设置一致

sync:
  watch_path: "./vault"                # 本地仓库路径（相对或绝对路径）
  sync_notes: true                     # 同步 .md 文件
  sync_files: true                     # 同步附件
  sync_config: true                    # 同步 .obsidian/ 配置
  exclude_patterns:
    - ".git/**"
    - ".trash/**"
    - "*.tmp"
    - "*.bak"
  file_chunk_size: 524288              # 二进制传输分块大小

client:
  reconnect_max_retries: 15            # 最大重连次数（0 = 无限）
  reconnect_base_delay: 3              # 基础延迟秒数（指数退避）
  heartbeat_interval: 30               # WebSocket 心跳间隔

logging:
  level: "INFO"                        # TRACE, DEBUG, INFO, WARN, ERROR
  file: ""                             # 日志文件路径（空 = 仅输出到 stdout）
```

### 如何获取 Token

1. 打开 Fast Note Sync Service 网页界面（如 `https://your-server-address`）
2. 登录
3. 点击 **"Copy API Config"**
4. 将 JSON 中的 `api`、`apiToken` 和 `vault` 复制到 `config.yaml`

### 环境变量（可选）

无需修改配置文件即可覆盖设置：

```bash
export FNS_API="https://your-server-address"
export FNS_TOKEN="your_api_token"
```

## 使用方法

### run（持续同步）

```bash
./fns-cli run -c config.yaml
```

长期运行模式：初始同步 + 文件监视 + 接收远程更新。

### sync（一次性同步）

```bash
./fns-cli sync -c config.yaml
```

完整双向同步后退出。

### pull（仅下载）

```bash
./fns-cli pull -c config.yaml
```

仅拉取远程变更后退出。

### push（仅上传）

```bash
./fns-cli push -c config.yaml
```

推送所有本地文件后退出。

### status（状态查看）

```bash
./fns-cli status -c config.yaml
```

显示配置和同步状态。

## 守护进程与开机自启（systemd）

在 Linux 上，systemd 提供崩溃自动重启、开机自启和集中日志管理。

创建 `/etc/systemd/system/fns-cli.service`：

```ini
[Unit]
Description=FNS CLI - Obsidian 仓库同步 (Rust)
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

启用并启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable fns-cli
sudo systemctl start fns-cli
sudo systemctl status fns-cli
```

查看日志：

```bash
journalctl -u fns-cli -f
journalctl -u fns-cli --since today
```

## 同步行为

### 运行流程

```
1. WebSocket 连接 → 认证
2. 增量拉取（NoteSync + FileSync + SettingSync）
3. 启动本地仓库文件监视器
4. 持续双向同步
5. 断开连接时 → 指数退避重连 → 增量重新同步
```

### 回声抑制

客户端在回声缓存中追踪出站变更，避免重新上传刚从服务器接收的文件。这可以防止双向同步中的反馈循环。

### 重连处理

当 WebSocket 断开连接时：
1. 指数退避等待（基础 3 秒，最大 300 秒）
2. 重新连接并认证
3. 触发增量重新同步以追赶错过的变更
4. 恢复正常运行

### 状态文件

进度存储在 `vault/.fns_state.json` 中。重启后，同步从上次检查点增量继续。

## 开发

### 运行测试

```bash
cargo test --lib
```

### 调试日志运行

```bash
RUST_LOG=debug ./fns-cli run -c config.yaml
```

### Clippy 检查

```bash
cargo clippy -- -D warnings
```

## 与 Python 版本对比

| 功能 | Python | Rust |
|------|--------|------|
| 双向同步 | ✅ | ✅ |
| 自动重连 | ✅ | ✅ |
| 回声抑制 | ✅ | ✅ |
| 消息队列 | ✅ | ✅ |
| 移动事件处理 | ✅ | ✅ |
| 二进制分块 | ✅ | ✅ |
| 内存占用 | 较高 | 较低 |
| 启动时间 | 较慢 | 较快 |
| 运行方式 | 需要 Python 环境 | 独立二进制 |

## 相关项目

- [Fast Note Sync Service](https://github.com/haierkeys/fast-note-sync-service) — 后端服务
- [obsidian-fast-note-sync](https://github.com/haierkeys/obsidian-fast-note-sync) — Obsidian 插件
- [FastNodeSync-CLI](https://github.com/Go1c/FastNodeSync-CLI) — Python CLI 版本

## 许可证

MIT
