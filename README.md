# qcp-lib-go

**QCP Go 参考实现 — 新时代 UDP 可靠协议（游戏 / IoT · 超低延迟 · 高保证）**

本仓库是 QCP 的**规范参考实现**。新特性在此开发与验证，通过 `qcp-benchmark` 压测（KCP 仅作基线对手）后移植至其他语言。

```
规范 (PROTOCOL.md) → qcp-lib-go (Go 验证) → qcp-benchmark (压测) → qcp-core / 其他绑定
```

## 安装

```bash
go get github.com/neko233-com/qcp-lib-go
```

## 快速开始

```go
conn, _ := qcp.Dial("server:9000")

// 游戏移动 / IoT 遥测 — 最新覆盖
conn.SendWithStream(pos, qcp.STREAM_REALTIME, 0)

// 射击 / 设备指令 — deadline 内有界可靠
conn.SendWithStream(cmd, qcp.STREAM_CRITICAL, 8*time.Millisecond)

// 聊天 / OTA — 强可靠
conn.SendWithStream(data, qcp.STREAM_BATCH, 0)
```

## 核心特性

### 1. TLB 语义交付
- REALTIME: 不可靠，最新 tick 覆盖
- CRITICAL: deadline 内 Fast NACK + 有界 ARQ
- BATCH: 强可靠 ARQ + ACK

### 2. Recovery Policy（按需）
- 多路径 Race > Fast NACK > Network Coding > ARQ

### 3. 基础设施
- Zero-Copy Ring Buffer（64KB 预分配）
- Lock-Free 队列（100K+ 并发）
- Multi-Path Manager（WiFi + 5G）
- AI 拥塞控制

## 目录

| 文件 | 说明 |
|------|------|
| `qcp/qcp.go` | 连接、包格式、多路径、Ring Buffer |
| `qcp/arq.go` | ARQ 引擎、Fast NACK |
| `qcp/listener.go` | `Listen` / `Accept` 服务端 |

## 开发

```bash
go build ./...
cd ../qcp-benchmark && go run . -mode all -duration 5s
```

## License

MIT License
