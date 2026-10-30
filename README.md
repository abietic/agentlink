# agentlink

在 Tanka 群里 @ 机器人，消息就交给一台配对好的机器，由 Claude Agent SDK 在那台机器上执行，结果回帖到同一个群（挂在原消息下面）。服务端只负责收发和路由；源码、凭证和执行都留在 worker 所在的机器上。

```
Tanka 群 ──@bot──▶ agentlink-server ◀──长轮询── agentlink-worker ──▶ node sdk/runner.mjs ──▶ Agent SDK query()
   ▲   app_mention   (公网 HTTPS)  密文任务/结果  （本机 / 内网）        （工作目录 + 会话）
   └──────── response_url / 群 webhook ◀───────────┘
```

- **server**：校验 Tanka 签名，按 `(bot, room_id)` 找到配对的设备，把任务加密后放进该设备的队列（SQLite）；worker 交回结果后，用事件里的 `response_url` 回帖，失败再用该群的 incoming webhook。
- **worker**：凭 device id / token 与 server 配对，只发出站请求（不需要公网入口）。每个群在本机有自己的工作目录和当前会话：默认续接当前会话，也可以新建、切回旧会话或换目录。
- **执行走官方 Agent SDK**：Go 没有官方 SDK，worker 每条 prompt 起一个 `node sdk/runner.mjs`，由它调用 `@anthropic-ai/claude-agent-sdk` 的 `query()`。新会话用 `options.sessionId`（worker 自己生成的 UUID），续接用 `options.resume`，会话 id 不变；SIGTERM 会 abort 这次 query，已做的部分留在会话里。`sdk_options` 原样传给 `query()`。

## 多机器人、多设备

- server 可以同时接多个 Tanka 应用（`bots`），每个的事件地址是 `POST /tanka/events/<name>`；`POST /tanka/events` 是默认机器人。
- worker 用 `bot` 选择通过哪个机器人配对，不填就是 server 的默认机器人。设备只能在自己那个机器人所在的群里 `pair`。
- 每台设备有独立的任务队列，多台设备同时在线互不影响；同一台设备上，每个群按顺序执行，所有群合计最多同时跑 `max_concurrency` 个。

## 加密

- **端到端（server ⇄ worker）**：设备注册时生成 X25519 密钥对并上报公钥，同时记下 server 的公钥。任务在 server 收到的那一刻就用「临时密钥 + 双方长期密钥」派生的 AES-256-GCM 密钥加密给该设备，结果同样加密给 server；附加数据绑定 `task|result <device-id> <task-id>`，所以一个设备的任务不能被另一个设备解开，结果也不能被冒充或挪到别的任务上。中间的 TLS 终止代理（如 Cloudflare）和 server 的数据库都只看到密文。
- **落盘**：两边都用 SQLite（`modernc.org/sqlite`，纯 Go）。server 的 `response_url` 和自己的私钥用本地存储密钥（`key_path`，首次启动生成，0600）加密；任务内容本来就是只有设备能解的密文。worker 的身份、令牌、私钥、各群状态和会话标题整体加密存在 `~/.agentlink/worker.db`，密钥在 `~/.agentlink/worker.key`。
- Tanka → server 这一段只能是明文（server 要读命令），靠 Tanka 签名校验。

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

命令词不区分大小写，可以带 `/`。

## 触发式任务（没人 @ 也能跑）

Tanka 不会把机器人或 incoming webhook 发出的 @ 投递成 `app_mention`，所以「一个机器人 @ 另一个机器人」触发不了任务。没人发消息也要跑的任务走下面两条路，两条可以同时用：

1. **server 的 Sentry 钩子**：在 Sentry 建一个 Internal Integration，webhook 填 `https://<server 域名>/sentry/<bot name>`，在告警规则里加这个 integration 作为动作。server 用 client secret 校验 `Sentry-Hook-Signature`，按 `Request-ID` 去重，只处理 `event_alert`，把告警按 `sentry.prompt` 模板（可用 `{issue_id}` `{title}` `{url}` `{rule}` `{project}`）变成任务，交给 `sentry.room_id` 绑定的设备。
2. **worker 的 `triggers[].command`**：worker 每 `every_sec` 秒跑一次这个命令，stdout 每行一个 `{"key": ..., "at": RFC3339 时间, "text": prompt}`。某个 key 出现了比上次更新的 `at` 就排一个任务；第一次见到、但已经超过 `max_age_sec` 的不排。这样可以先在本机轮询，等 Sentry 那边配好推送再并行。

```json
"sentry": {"room_id": "<群 id>", "trigger": "sentry", "prompt": "Triage Sentry issue {issue_id}: {title} ({url})"}
```

- 任务带着触发名（Sentry 钩子默认 `sentry`）。worker 只执行 `triggers` 里有同名配置的任务；同一个 key 在 `dedupe_sec`（默认 30 分钟）内只跑一次，推送和轮询同时到也只跑一次。不认识的触发名、重复的 key 都静默跳过。
- 触发任务不检查 `allowed_users`，文本不当群命令解析；在该 trigger 的 `workdir` 新开会话，这个会话成为群里的当前会话，群里的人 @ 机器人可以接着追问。
- 结果没有 `response_url`，只能发到群的 incoming webhook，所以该群必须配 `room_webhooks`。空结果不发；排队超时的触发任务静默丢弃。
- server 的 `rooms`（群 id → device id）在启动时写入绑定，触发用的群不必有人去发 `pair`。

## 部署

### 1. Tanka 应用

在 [Tanka 开放平台](https://open.tanka.ai/apps) 建一个应用（或沿用已有应用），然后：

1. Event Subscriptions 的 Request URL 填 `https://<server 域名>/tanka/events/<bot name>`。保存时 Tanka 会发一条带签名的 `url_verification`，server 必须先跑起来并配好这个机器人的 Signing Secret。
2. 订阅 `app_mention`。
3. 把机器人拉进要用的群。需要 webhook 兜底的群，建一个 incoming webhook 填进该机器人的 `room_webhooks`。

### 2. server（公网，HTTPS）

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin/agentlink-server ./cmd/agentlink-server
```

- 配置见 `examples/server.json`；密钥用环境变量给：`AGENTLINK_ENROLL_TOKEN`（自己生成的长随机串，worker 首次注册用）、`AGENTLINK_SIGNING_SECRET_<BOT>`（机器人名大写，`-` 换成 `_`）、`AGENTLINK_SENTRY_SECRET_<BOT>`（Sentry integration 的 client secret）、`AGENTLINK_ROOM_WEBHOOKS_<BOT>`（JSON 对象 `{"群 id": "webhook URL"}`，与配置文件里的 `room_webhooks` 合并；webhook URL 本身就是发消息的凭证，不要写进仓库）。
- systemd 单元见 `examples/agentlink-server.service`。server 只监听 HTTP（默认 `127.0.0.1:8080`），TLS 由前面的反向代理负责，读超时要大于 `poll_wait_sec`（默认 25 秒）。
- `/healthz` 是存活探针。日志里只有 id，没有正文。
- 队列持久化：server 重启后，排队中的任务照样会交给设备；没人取的任务 `queue_ttl_sec`（默认 10 分钟）后丢弃并回帖说明，取走但没交结果的 `lease_sec` 后丢弃。

### 3. worker（干活的机器）

```bash
go install ./cmd/agentlink-worker
(cd sdk && npm ci)                       # 装 Agent SDK
mkdir -p ~/.agentlink && cp examples/worker.json ~/.agentlink/worker.json   # 改 server_url / runner / allowed_users
AGENTLINK_ENROLL_TOKEN=... agentlink-worker
```

1. 首次启动会生成密钥对、向 server 注册，拿到 `device_id`。之后启动不再需要 enroll token；换 `server_url` 或 `bot` 会重新注册（新的 device id）。
2. 在群里 @ 机器人，发 `pair <device-id>`。
3. 发一条 prompt。如果 `allowed_users` 是空的，回复会写出你的 Tanka user id；填进 `allowed_users` 后重启 worker。
4. 常驻见 `examples/com.agentlink.worker.plist`（launchd）。

## 安全模型

- **入口**：每个事件都要通过对应机器人的 HMAC 签名校验（±5 分钟）；配了 `app_id` 的机器人还要求 `api_app_id` 一致；`event_id` 去重（持久化，重启后也不会重复执行）；`response_url` 只接受 `response_url_prefix` 开头的地址。
- **谁能驱动机器**：server 的 `allowed_users` 是可选的粗过滤；最终由 worker 本机的 `allowed_users` 决定，默认拒绝所有人。设备注册需要 enroll token。
- **能做什么**：工作目录限制在 `workdir_roots` 之内（先解析符号链接再判断）。默认 `permissionMode: "auto"`：由模型分类器逐个批准或拒绝工具调用，判断不了的一律拒绝（没人在场回答权限询问），回复里会注明被拒绝的次数。要更严就改成 `"dontAsk"` 并用 `allowedTools` 预先放行，要更松就在 `allowedTools` 里加。群消息是不可信输入，可能被用来做 prompt 注入；分类器能挡住一部分，但不等于沙箱，`allowed_users` 要保持最小。
- **触发任务**：只有两个来源能产生——server 的 Sentry 钩子（HMAC 校验）和设备自己（用设备密钥加密），所以不过 `allowed_users`。但告警标题等字段是外部可控的文本，同样是 prompt 注入面：模板里的字段会被压成一行并截断，prompt 里要写明它们只是数据。
- **数据**：会话的完整记录留在 worker 本机（Claude Code 自己的 `~/.claude/projects`）。

## 开发

```bash
go test -race ./...    # 加密、签名、多机器人路由、去重、持久化与重启、过期、会话生命周期、触发任务、端到端（测试二进制充当 SDK runner）
AGENTLINK_REAL_CLAUDE=1 go test -run TestRealSDK ./internal/worker   # 真 Agent SDK：新建会话 + resume，约 $0.02
```
