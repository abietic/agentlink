# agentlink

在 Tanka 群里 @ 机器人，消息就交给一台配对好的机器，由 Claude Code 在那台机器上执行，结果回帖到同一个群（挂在原消息下面）。服务端只负责收发和路由；源码、凭证和执行都留在 worker 所在的机器上。

```
Tanka 群 ──@bot──▶ agentlink-server ◀──长轮询── agentlink-worker ──▶ claude -p
   ▲      app_mention   (公网 HTTPS)    出站 HTTPS   （本机 / 内网）   （工作目录 + 会话）
   └──────── response_url / 群 webhook ◀── 结果 ──────┘
```

- **server**：校验 Tanka 签名，按 `room_id` 找到配对的设备，把消息放进该设备的内存队列；worker 交回结果后，用事件里的 `response_url` 回帖，失败再用该群的 incoming webhook。
- **worker**：凭 device id / token 与 server 配对，只发出站请求（不需要公网入口）。每个群在本机有自己的工作目录和当前会话：默认续接当前会话，也可以新建、切回旧会话或换目录。
- **为什么不用 Agent SDK**：官方 Agent SDK 只有 Python 和 TypeScript。官方文档给其他语言的路径是以子进程方式跑 `claude -p --output-format json`，底层与 SDK 是同一套 harness。新建会话时 worker 自己生成 UUID，用 `--session-id` 传入；续接用 `--resume <id>`；prompt 走 stdin。

## 群里的用法

先 @ 机器人，再写下面的内容：

| 写法 | 作用 |
|---|---|
| `pair <device-id>` / `unpair` | 把本群绑定到某台 worker，或解绑（device id 在 worker 启动日志里） |
| `status` | 设备、工作目录、当前会话、正在跑什么；prompt 运行中也会立即回答 |
| `cd <path>` / `workdir [path]` | 切换工作目录，只能在 `workdir_roots` 之内；切换后下一条 prompt 新开会话 |
| `new [prompt]` | 新开会话，可以同时带上第一条 prompt |
| `sessions` | 本群最近 10 个会话 |
| `resume <序号 \| id 前缀 \| 完整 UUID> [prompt]` | 切回旧会话；用完整 UUID 可以接手在终端里开的会话 |
| `help` 或只 @ 不写内容 | 帮助 |
| 其他任何内容 | 作为 prompt，在当前会话里执行 |

命令词不区分大小写，可以带 `/`。同一个群的任务按顺序执行，所有群合计最多同时跑 `max_concurrency` 个。

## 部署

### 1. Tanka 应用

在 Tanka 开放平台建一个应用（或沿用已有应用），然后：

1. 打开 Event Subscriptions，Request URL 填 `https://<server 域名>/tanka/events`。保存时 Tanka 会发一条带签名的 `url_verification` 请求，server 必须先跑起来。
2. 订阅 `app_mention` 事件。
3. 记下 App Signing Secret。
4. 把机器人拉进要用的群。

如果要启用 webhook 兜底，就给每个群建一个 incoming webhook，填进 `room_webhooks`。

### 2. server（公网，HTTPS）

```bash
go install ./cmd/agentlink-server
export AGENTLINK_SIGNING_SECRET=...   # Tanka App Signing Secret
export AGENTLINK_ENROLL_TOKEN=...     # 自己生成的长随机串，worker 首次注册时要用
agentlink-server -config examples/server.json
```

- server 只监听 HTTP，TLS 由前面的反向代理或 ingress 负责。
- 反代的读超时要大于 `poll_wait_sec`（默认 25 秒）。
- `/healthz` 是存活探针。
- 状态文件（`state_path`）只保存设备（token 存 SHA-256）和群绑定，权限是 0600。
- 消息正文只放在内存里，交回结果后即删除；日志里只有 id，没有正文。

### 3. worker（干活的机器）

```bash
go install ./cmd/agentlink-worker
mkdir -p ~/.agentlink && cp examples/worker.json ~/.agentlink/worker.json   # 改 server_url / allowed_users
AGENTLINK_ENROLL_TOKEN=... agentlink-worker
```

1. 首次启动会向 server 注册，拿到 `device_id` / token，存进 `~/.agentlink/worker.state.json`（0600）。之后启动就不再需要 enroll token。
2. 在群里 @ 机器人，发 `pair <device-id>`。
3. 发一条 prompt。如果 `allowed_users` 是空的，回复会写出你的 Tanka user id。把它填进 `allowed_users`，再重启 worker。
4. 需要常驻的话，见 `examples/com.agentlink.worker.plist`（launchd）。

## 安全模型

- **入口**
  - 每个事件都要通过 HMAC 签名校验，时间戳在 ±5 分钟窗口内。
  - `event_id` 去重，Tanka 重投不会重复执行。
  - `response_url` 只接受 `response_url_prefix` 开头的地址。
- **谁能驱动机器**
  - server 的 `allowed_users` 是可选的粗过滤。
  - 最终由 worker 本机的 `allowed_users` 决定。默认拒绝所有人，名单不在 server 上。
  - 设备注册需要 enroll token。
- **能做什么**
  - 工作目录限制在 `workdir_roots` 之内（先解析符号链接再判断）。
  - 默认参数 `--permission-prompts none`：需要人工确认的工具调用一律拒绝，回复里会注明被拒绝的次数。要放开写文件或执行 Bash，在 `claude_args` 里加 `--permission-mode acceptEdits`、`--allowedTools ...` 等。
  - 群消息是不可信输入，可能被用来做 prompt 注入，所以权限要按需最小化。
- **数据**
  - 回复里会带 worker 的输出，内容由你的 prompt 决定。
  - 会话的完整记录留在 worker 本机，位于 Claude Code 自己的 `~/.claude/projects`。

## 联调时要确认的点

文档里没写清楚、要拿真实 Tanka 验证的地方：

- `app_mention` 的 `text` 里机器人提及的实际格式（`@名字` 还是 `<@ID>`）。名字带空格时，要把它配进 `bot_names`。
- `response_url` 的实际域名是否就是 `https://api.tanka.ai/`，以及有效期多长。长任务超过有效期时会走 webhook 兜底，所以 `room_webhooks` 最好配上。
- 群消息对 markdown 的渲染效果，以及单条消息的长度上限（`max_reply_chars` / `max_output_chars`）。

## 开发

```bash
go test -race ./...                                              # 签名、路由、去重、租约过期、会话生命周期、端到端（用假 claude）
AGENTLINK_REAL_CLAUDE=1 go test -run TestRealClaude ./internal/worker   # 真 CLI：新建会话 + --resume，约 $0.01
```
