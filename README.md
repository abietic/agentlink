# agentlink

在 Tanka 群里 @ 机器人，消息就交给一台配对好的机器，由那台机器上的 harness（Claude Agent SDK、mirasim 或 Kimi Code）执行，结果回帖到同一个群（挂在原消息下面）。服务端只负责收发和路由；源码、凭证和执行都留在 worker 所在的机器上。

```
Tanka 群 ──@bot──▶ agentlink-server ◀──长轮询── agentlink-worker ──▶ node sdk/runner.mjs ──▶ Agent SDK query()
   ▲   app_mention   (公网 HTTPS)  密文任务/结果  （本机 / 内网）        （工作目录 + 会话）
   └──────── response_url / 群 webhook ◀───────────┘
```

- **server**：校验 Tanka 签名，按 `(bot, room_id)` 找到配对的设备，把任务加密后放进该设备在内存里的队列；worker 取走时，任务就从 server 上删掉，回帖去向（`response_url` 等）用 server 的存储密钥封好随任务交给 worker，worker 交回结果时带回来，server 据此回帖，失败再用该群的 incoming webhook。消息不落盘：SQLite 里只有设备、配对、server 的密钥和元数据（任务流水、worker 上报的状态，见「元数据与统计」）。
- **worker**：凭 device id / token 与 server 配对，只发出站请求（不需要公网入口）。每个群在本机有自己的工作目录和当前会话：默认续接当前会话，也可以新建、切回旧会话或换目录。
- **执行走官方 Agent SDK**：Go 没有官方 SDK，worker 每条 prompt 起一个 `node sdk/runner.mjs`，由它调用 `@anthropic-ai/claude-agent-sdk` 的 `query()`。新会话用 `options.sessionId`（worker 自己生成的 UUID），续接用 `options.resume`，会话 id 不变；SIGTERM 会 abort 这次 query，已做的部分留在会话里。`sdk_options` 原样传给 `query()`。

## 多机器人、多设备

- server 可以同时接多个 Tanka 应用（`bots`），每个的事件地址是 `POST /tanka/events/<name>`；`POST /tanka/events` 是默认机器人。
- worker 用 `bot` 选择通过哪个机器人配对，不填就是 server 的默认机器人。设备只能在自己那个机器人所在的群里 `pair`。
- 每台设备有独立的任务队列，多台设备同时在线互不影响；同一台设备上，每个群按顺序执行，所有群合计最多同时跑 `max_concurrency` 个。
- 一台机器可以跑多个 worker（各自的配置文件和 `state_dir`），各自配对一个机器人，比如给 mirasim 单独一个机器人：见下面的 harness。

## Harness（用什么执行）

worker 配置里的 `harnesses` 列出这台设备能用的执行方式，`harness` 是默认的那个；群里发 `harness <名字>` 切换本群的，切换后下一条 prompt 新开会话。每个会话记着自己是哪个 harness 跑的，续接时（当前会话或用会话 id）都走原来的那个；设备默认 harness 改了，已有会话也照旧，`new` 才用新的。

| `type` | 怎么执行 | 配置 |
|---|---|---|
| `sdk`（默认） | `node sdk/runner.mjs` 调 Agent SDK 的 `query()`，见上面 | `runner`、`sdk_options`，不填就用顶层的；顶层配了 `runner` 就自动有一个叫 `claude` 的 |
| `mirasim` | `mirasim ui-cli prompt` 交给正在运行的 mirasim（默认桌面版，端口 4970），会话出现在 mirasim 的侧栏和手机端 | `command`（默认 `["mirasim"]`）、`port`、`agent`（默认 `claude`）、`model`、`effort`、`route`、`members` |
| `kimi` | Kimi Code CLI 的非交互模式 `kimi -p <prompt> --output-format stream-json`，续接用 `-S` | `command`（默认 `["kimi"]`）、`model`（不填用 kimi 自己的默认） |

每个 harness 都可以配 `fallback`（harness 名字的列表，按顺序）：它在用量上限里、或者根本起不来（没有返回会话）时，prompt 交给列表里下一个能用的。回退出来的是新会话，因为一个 harness 续接不了另一个的会话；这个新会话成为本群的当前会话，之后在它自己的 harness 上续接，`new` 才回到本群的 harness。

### 用量上限

- **Claude（`sdk`）**：runner 从 Agent SDK 的消息流里读 `rate_limit_event`（`status: rejected` 时带 `rateLimitType`：`five_hour`、`seven_day`、`seven_day_opus`、`seven_day_sonnet`、`overage` 等，和 `resetsAt`）；重试用完后 SDK 发出 `error: rate_limit` 的 assistant 消息，runner 就把这个上限交给 worker。
- **Kimi（`kimi`）**：额度只能从 kimi 的本地服务查。一轮失败时，worker 临时起一个 `kimi web --no-open`，用它启动时打印的令牌请求 `GET /api/v1/oauth/usage`（令牌不写日志、不外传），查完就停掉。`limit5h`、`limit7d`、`monthCode`、`monthTotal` 里有用满的（`usedRatio ≥ 1`），就按其中最晚重置的那个记上限。额度耗尽的 429 kimi 不重试，立即失败。
- worker 按 harness 记「在哪个上限里、到什么时候」，加密存在本地，重启不丢。到点之前这个 harness 不再跑，直接交给 `fallback`；没说重置时间的上限按 30 分钟后再试。
- 只有**第一次**撞上时回帖说明：哪个 harness、哪个上限、几点重置（本机时区）。之后同一轮上限里，有回退就安静地交给回退（页脚显示 harness 名）；续接中的会话被回退接走时，回帖注明新会话没有原会话的上下文。没有可用 harness 时，触发任务什么都不发（只记日志），有人 @ 时回一行「都在上限里，几点恢复」。这个 harness 之后有一轮正常跑完，才算出了这轮上限；`status` 列出还在上限里的 harness。

Kimi 的几点限制：

- prompt 在命令行参数里（`-p` 不读 stdin），本机其他用户用 `ps` 能看到。
- `-p` 模式不问人：工具调用按 kimi 的 `auto` 权限策略执行，配置里的静态拒绝规则照样生效。
- 图片写进临时目录，用 `--add-dir` 交给 kimi，并在 prompt 里列出路径让它用 `ReadMediaFile` 读；这轮结束就删。能不能看图取决于所用模型。
- worker 设了 `KIMI_DISABLE_TELEMETRY=1`。prompt、代码和群里的图片会发给 Moonshot（Kimi 的服务方）；用它做 `fallback` 前，先确认这些数据可以给它。
- 新会话的 id 由 kimi 分配，worker 记的是 `session_` 后面那段。回帖页脚的轮数是工具调用的次数，没有费用。

mirasim 的几点限制：

- 要 mirasim 桌面版开着；没开时回帖是 ui-cli 的报错。
- `route` 默认 `own`：用这台机器上 agent 自己的登录；`platform` 走 mirasim 的账号，prompt 和文件会经过 mirasim 的服务器。
- 工具调用的审批按 mirasim 里 agent 的设置。mirasim 给 claude 的审批模式只有 `auto` 和 `bypassPermissions`（默认），提问工具默认关闭，所以默认设置下 claude 不会停下来等审批或提问；打开提问工具后才会。轮次在等审批或提问时，worker 一直等到 `timeout_sec`，然后回帖列出在等什么（每个问题和它的选项），这一轮留在 mirasim 里，回答以后继续跑；在超时前回答的，结果照常回帖。没在等任何东西又超时的，会被停掉。
- 新会话的 id 由 mirasim 分配，worker 记的是会话键里 `<agent>:` 后面那段。
- `members` 把 Tanka 用户 id 映射到 mirasim 空间里的人（`usr_…`，空间设置 › 成员），被映射的人发起的轮次记在本人名下（`ui-cli --by`）：
  - 会话里显示发起人的名字；
  - 轮次完成或等回答时，只通知发起人自己的手机；
  - 发起人能用哪些模型、扣谁的额度由 mirasim 决定，本机的登录只给所有者和管理员用。

  没映射的人和触发式任务仍以机器所有者身份运行。映射到的人如果不是这个空间的成员，mirasim 会悄悄按所有者执行，所以 worker 会先核对成员名单，并在回帖里说明。配了 `members` 后，worker 把每个会话都设成「与成员共享」，因为 mirasim 默认只允许会话创建者在会话里发消息。
- ui-cli 不读 stdin，prompt 在命令行参数里，本机其他用户用 `ps` 能看到；以 `/` 开头的文字可能被当成 mirasim 的宿主命令（如 `/branch`）。
- 回帖页脚没有轮数和费用。
- 触发式任务用设备的默认 harness（在上限里时按它的 `fallback`）。

给 mirasim 单独配一个机器人：在 Tanka 建第二个应用，server 的 `bots` 里加一项（事件地址 `/tanka/events/<name>`）；本机再跑一个 worker，配置见 `examples/worker-mirasim.json`，launchd 见 `examples/com.agentlink.worker.mirasim.plist`。自动更新会等所有 worker 都空闲，再一起重启（`com.agentlink.worker` 和 `com.agentlink.worker.*`）。

## 加密

- **端到端（server ⇄ worker）**：设备注册时生成 X25519 密钥对并上报公钥，同时记下 server 的公钥。任务在 server 收到的那一刻就用「临时密钥 + 双方长期密钥」派生的 AES-256-GCM 密钥加密给该设备，结果同样加密给 server；附加数据绑定 `task|result <device-id> <task-id>`，所以一个设备的任务不能被另一个设备解开，结果也不能被冒充或挪到别的任务上。中间的 TLS 终止代理（如 Cloudflare）和 server 的数据库都只看到密文。
- **落盘**：两边都用 SQLite（`modernc.org/sqlite`，纯 Go）。server 存设备、配对、自己的私钥（用本地存储密钥 `key_path` 加密，首次启动生成，0600）和元数据（见「元数据与统计」），任务内容和事件 id 都只在内存里；从 0.4 起启动时删掉旧版留下的任务表并 `VACUUM`。随任务交给 worker 的回帖去向也用这把存储密钥封装，worker 解不开也改不了。worker 的身份、令牌、私钥、各群状态和会话标题整体加密存在 `~/.agentlink/worker.db`，密钥在 `~/.agentlink/worker.key`。
- Tanka → server 这一段只能是明文（server 要读命令），靠 Tanka 签名校验。

## 群里的用法

先 @ 机器人，再写下面的内容：

| 写法 | 作用 |
|---|---|
| `pair <device-id>` / `unpair` | 把本群绑定到某台 worker，或解绑（device id 在 worker 启动日志里） |
| `status` | 设备、工作目录、当前会话、正在跑什么；prompt 运行中也会立即回答 |
| `cd <path>` / `workdir [path]` | 切换工作目录，只能在 `workdir_roots` 之内；切换后下一条 prompt 新开会话 |
| `new [prompt]` | 新开会话，可以同时带上第一条 prompt |
| `sessions` | 本群最近 15 个会话（记住 50 个） |
| `resume <序号 \| id 前缀 \| 完整 UUID> [prompt]` | 切回旧会话；用完整 UUID 可以接手在终端里开的会话 |
| `harness [名字]` | 查看或切换本群的 harness，切换后下一条 prompt 新开会话 |
| `help` 或只 @ 不写内容（也不带图） | 帮助 |
| `<会话 id 前缀> <prompt>` | 在那个会话里继续（至少 6 位，回帖页脚和 `sessions` 里有），不切换群的当前会话；不同会话可以同时跑 |
| 其他任何内容 | 作为 prompt，在当前会话里执行 |

命令词不区分大小写，可以带 `/`。

**图片**：和 @ 同一条消息发出的图片（PNG / JPEG / GIF / WebP）会和文字一起交给 Claude；只发图不写字也算 prompt。图片来自事件里文档没写的 `attachments` 字段：server 只把下载链接加密转给 worker，由 worker 去下载，并且只下载 `tanka.ai` 域名下的 https 链接。每条消息最多 5 张，每张不超过 3.75 MB（API 对 base64 后 5 MB 的上限），超出的、不是图片的附件会在回帖里列出来，不会悄悄丢掉。

## 触发式任务（没人 @ 也能跑）

没人发消息也要跑的任务走下面两条路，两条可以同时用（机器人之间互相 @ 交接工作见「安全模型」）：

1. **server 的 Sentry 钩子**：在 Sentry 建一个 Internal Integration，webhook 填 `https://<server 域名>/sentry/<bot name>`，在告警规则里加这个 integration 作为动作。server 用 client secret 校验 `Sentry-Hook-Signature`，按 `Request-ID` 去重，只处理 `event_alert`，把告警按 `sentry.prompt` 模板（可用 `{issue_id}` `{title}` `{url}` `{rule}` `{project}`）变成任务，交给 `sentry.room_id` 绑定的设备。
2. **worker 的 `triggers[].command`**：worker 每 `every_sec` 秒跑一次这个命令，stdout 每行一个 `{"key": ..., "at": RFC3339 时间, "text": prompt}`。某个 key 出现了比上次更新的 `at` 就排一个任务；第一次见到、但已经超过 `max_age_sec` 的不排。这样可以先在本机轮询，等 Sentry 那边配好推送再并行。

```json
"sentry": {"room_id": "<群 id>", "trigger": "sentry", "prompt": "Triage Sentry issue {issue_id}: {title} ({url})"}
```

- 任务带着触发名（Sentry 钩子默认 `sentry`）。worker 只执行 `triggers` 里有同名配置的任务；同一个 key 在 `dedupe_sec`（默认 30 分钟）内只跑一次，推送和轮询同时到也只跑一次。不认识的触发名、重复的 key 都静默跳过。
- 触发任务不检查 `allowed_users`，文本不当群命令解析；在该 trigger 的 `workdir` 新开会话。这个会话记进群的会话历史，但不成为当前会话：回帖页脚写着会话 id，群里的人 @ 机器人、以这个 id 开头就能接着追问，每条告警各自一条线，互不串话。
- 排队：群自己的命令和当前会话的 prompt 排一队；指名某个会话的消息只跟同一会话排队；每个触发任务单独一队。不同队列并行，总数受 `max_concurrency` 限制；同一个会话无论从哪个队列来，同一时刻只跑一个。
- 结果没有 `response_url`，只能发到群的 incoming webhook，所以该群必须配 `room_webhooks`。空结果不发；排队超时的触发任务静默丢弃。
- server 的 `rooms`（群 id → device id）在启动时写入绑定，触发用的群不必有人去发 `pair`。

### 停滞检测与中断后续跑

两个互相独立的配置块，默认都关着；不配就是原来的行为：一次跑到 `timeout_sec` 为止，中断了就把原因回到群里，不再重跑。

**`stall_sec`（顶层，只对 sdk harness 生效）**：runner 每收到 SDK 的一条消息（模型输出、工具调用开始或结束）就往 stdout 写一行不带内容的进度行；连续 `stall_sec` 秒没有任何进度就判定这次运行卡死并停掉，回帖是 `Stalled`，会话保留。有了它，`timeout_sec` 可以放宽成总预算：正常在跑的长任务不会被总时长误杀，真卡住的又不用等满总时长。`stall_sec` 要大于单次工具调用可能的最长时间，`timeout_sec` 仍要小于 server 的 `lease_sec`。

**`recovery`（按任务来源配置）**：键是 `trigger:<名字>`（某一个触发器）或 `trigger`（其余触发器）。有人 @ 的任务（`mention`）暂不支持，配了会在启动时报错；台账里每条记录已带来源字段，以后加不用改存储。

```json
"stall_sec": 900,
"timeout_sec": 7200,
"recovery": {
  "trigger": {
    "on": ["shutdown", "stall", "timeout", "error", "limit"],
    "max_attempts": 2,
    "retry_after_sec": 300,
    "resume": true,
    "quiet": true,
    "announce_limit": true,
    "notice_after_sec": 3600,
    "give_up_after_sec": 86400
  }
}
```

| 字段 | 含义 |
| --- | --- |
| `on` | 哪些结束方式值得再跑：`shutdown`（worker 停了）、`stall`、`timeout`、`error`（harness 失败或以错误结束）、`limit`（用量上限）。不在列表里的照旧直接回帖 |
| `max_attempts` | 一个任务总共最多跑几次，默认 2。因为上限根本没跑起来的那次不算 |
| `retry_after_sec` | 下一次运行前等多久，默认 300。`shutdown` 在 worker 回来后立刻重跑，`limit` 等到上限重置 |
| `resume` | 续接上一次留下的会话（prompt 用 `resume_prompt`，有默认值），而不是从头再来；会话续接不了就从头跑 |
| `quiet` | 还会重跑的那次中断不往群里发任何东西，群里只看到最终结果。不开则每次中断都回帖，并写明几点自动重跑。|
| `announce_limit` | `quiet` 打开时，harness 第一次撞上限的那一行说明（哪个上限、几点重置，每轮上限只说一次）是否照发；默认不发 |
| `notice_after_sec` | 任务有过中断、且到这个时长还没有最终结果时，发一行「还没有结果、下一次几点跑」；0 为不发 |
| `give_up_after_sec` | 从任务第一次到达算起，超过就不再重跑，发一行放弃说明（带会话 id），默认 86400 |

- 配了 `recovery` 的任务从被接受起就记在 worker 本机加密存储的台账里，直到有最终回帖才删，所以 worker 被强杀、断电也能在重启后接上。
- 重跑不是在原任务上继续等：原任务先以空结果结束，到点后 worker 像触发器命令那样向 server 再排一个同 key 的任务，结果走平常的路径回群。这个任务 5 分钟内没回来（比如 server 重启丢了）会再排一次。
- 没有 key 的触发任务不进台账，按原来的行为处理。同一个 key 在台账里只有一条：它还在排队或运行时，新来的同 key 任务被跳过；它在等重跑时，新来的同 key 任务直接当作这次重跑。
- 最后一次仍然失败时，回帖是那次的失败原因，外加一行说明这是第几次运行。

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
- 队列只在内存：server 重启会丢掉还没被设备取走的任务（在线设备通常一秒内就取走）；已取走的任务不受影响，结果带着回帖去向回来，照样回帖。没人取的任务 `queue_ttl_sec`（默认 10 分钟）后丢弃并回帖说明，取走但没交结果的 `lease_sec` 后丢弃，晚到的结果不再回帖。

### 元数据与统计

server 不存消息，但会存下面这些，用来管理、对账、排查和统计：

- **任务流水 `task_log`**（保留 90 天）：任务 id、机器人、群 id、设备、来源（`mention` 或触发器名）、发送人（@ 的人的 Tanka id；worker 触发的是设备 id；Sentry 钩子为空）、附件个数、加密后任务和结果的字节数、入队 / 被取走 / 租约到期 / 结束时间、结局：
  - `posted`：已回帖；
  - `empty`：worker 没有要发的；
  - `reply_failed`：Tanka 没收下；
  - `not_taken`：设备在 `queue_ttl_sec` 内没取；
  - `no_result`：取走后 `lease_sec` 内没交结果；
  - `lost`：server 重启时还没被取走。

  不存正文、附件链接和文件名、`response_url`。
- **设备水位 `device_stats`**：同时在排或在跑的任务数峰值及时间，以及 worker 每次轮询上报的状态：版本、在跑数、排队数、哪些 harness 在用量上限里到几点。这份状态放在请求头里，不加密（没有任务内容）。
- **重启**：还没被取走的任务记为 `lost`。配了 webhook 的群会收到一条只有数量和来源的说明（如「2 个任务：mention ×1，`alerts` ×1」），没有内容。已取走的任务不受影响。
- **worker 本地流水** `~/.agentlink/runs.jsonl`（保留 90 天）：
  - 每次跑 harness 一行：任务 id、触发器、harness、会话、耗时、失败 / 中断、撞到的上限、轮数、花费；
  - 每次交结果一行：任务 id、字节数，server 没收下时记原因。

  用任务 id 和 server 的流水对账。不记 prompt、回复和群 id。

在 server 上查看：

```bash
sudo /opt/agentlink/agentlink-server stats -config /etc/agentlink/server.json            # 设备、各群 / 设备 / 来源与发送人的结局分布、等待与运行耗时分位数、最近 20 个任务；-days 改统计天数
sudo /opt/agentlink/agentlink-server stats -config /etc/agentlink/server.json -task t-…  # 单个任务的全部记录
```

`stats` 以只读方式打开数据库，可以和 server 同时运行。没有 HTTP 接口。

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

## 发布与自动更新

- 推 `v*` tag 触发 `.github/workflows/release.yml`：跑测试，构建 server（Linux amd64 / arm64）、worker（macOS arm64 / amd64）和 SDK runner 包，生成 `SHA256SUMS`，用 GitHub artifact attestation 记录构建来源，发成 Release。日常 push 和 PR 由 `ci.yml` 跑 gofmt、vet 和 `go test -race`。
- **server**：把 `scripts/update-server.sh` 放到 `/opt/agentlink/`，装上 `examples/agentlink-update.service` 和 `.timer`（`AGENTLINK_HEALTH` 改成 server 的监听地址）。每小时检查一次：有新版本就下载、校验 sha256，再用 `gh attestation verify` 确认它是本仓库 `release.yml` 在对应 tag 上构建的（证明包从公开 API 取，不用登录 GitHub；`gh` 放在 `/opt/agentlink/bin/gh`），然后替换、重启 `agentlink-server`。任何一项校验不过就不装，正在跑的版本不受影响。`update-server.sh --check` 只下载和校验、不安装；`/healthz` 20 秒内不通就换回旧版本，之后不再重试这个版本。只动 agentlink 自己的单元。
- **worker**：把 `scripts/update-worker.sh` 放到 `~/.agentlink/bin/`，装上 `examples/com.agentlink.update.plist`。每小时检查一次：下载 worker 和 SDK runner，校验 sha256 并用 `gh attestation verify` 确认是本仓库 `release.yml` 在对应 tag 上构建的（需要已登录的 `gh`），在 `~/.agentlink/sdk` 里 `npm ci`，等所有 worker 都没有正在跑的 prompt 时替换并重启它们。用自动更新时，`runner` 指向 `~/.agentlink/sdk/runner.mjs`。

## 安全模型

- **入口**：每个事件都要通过对应机器人的 HMAC 签名校验（±5 分钟）；配了 `app_id` 的机器人还要求 `api_app_id` 一致；`event_id` 去重（在内存里，server 重启前后各算一次：重启那一刻 Tanka 恰好重投的事件可能执行两次）；同一个结果只回帖一次（同样记在内存里）；`response_url` 只接受 `response_url_prefix` 开头的地址。
- **谁能驱动机器**：server 的 `allowed_users` 是可选的粗过滤；最终由 worker 本机的 `allowed_users`（人）和 `allowed_bots`（别的机器人）决定，默认拒绝所有人。设备注册需要 enroll token。
- **机器人之间交接**：Tanka 自 2026-11-05 起投递机器人和 webhook 发出的 @（事件里 `sender_type` 为 `bot`，机器人 @ 自己不投递）。`allowed_bots` 里的机器人 @ 本机器人时照常执行，用来让一个 agent 把工作交给另一个：交出方在回复里写 `<@下一个机器人> 要它做的事`，下一个机器人收到的 prompt 是这个 @ 之后的全部文字（@ 之前的内容是写给群里的人看的，不会带过去）。几条防护：
  - 回帖不会 @ 回机器人；不在名单里的机器人不会得到任何回帖，只记日志。
  - 机器人发的 `pair` / `unpair` 被忽略：群绑哪台设备只能由人决定。
  - 同一个群里连续 3 次来自机器人的 @ 之后，server 回一句说明并不再受理，直到有人 @ 机器人，或安静满两个 `lease_sec`。两个机器人互相放行也不会无限来回。
  - 把机器人放进 `allowed_bots`，等于信任它消息里 @ 之后的全部文字：只放行消息内容由你控制的机器人。
- **能做什么**：工作目录限制在 `workdir_roots` 之内（先解析符号链接再判断）。默认 `permissionMode: "auto"`：由模型分类器逐个批准或拒绝工具调用，判断不了的一律拒绝（没人在场回答权限询问），回复里会注明被拒绝的次数。要更严就改成 `"dontAsk"` 并用 `allowedTools` 预先放行，要更松就在 `allowedTools` 里加。群消息是不可信输入，可能被用来做 prompt 注入；分类器能挡住一部分，但不等于沙箱，`allowed_users` 要保持最小。
- **触发任务**：只有两个来源能产生——server 的 Sentry 钩子（HMAC 校验）和设备自己（用设备密钥加密），所以不过 `allowed_users`。但告警标题等字段是外部可控的文本，同样是 prompt 注入面：模板里的字段会被压成一行并截断，prompt 里要写明它们只是数据。
- **数据**：会话的完整记录留在 worker 本机（Claude Code 自己的 `~/.claude/projects`；mirasim harness 则在 mirasim 的存储里，mirasim 的手机端和协作空间成员按 mirasim 的权限能看到），群里发的图片也在里面。附件链接不用登录就能下载，所以只在加密的任务里传，不写日志、不出现在错误信息里。

## 开发

```bash
go test -race ./...    # 加密、签名、多机器人路由、去重、重启不留消息、任务流水与统计、过期、会话生命周期、触发任务、停滞检测与中断后续跑、用量上限与回退、端到端（测试二进制充当 SDK runner、mirasim 和 kimi）
AGENTLINK_REAL_CLAUDE=1 go test -run TestRealSDK ./internal/worker   # 真 Agent SDK：新建会话 + resume，约 $0.02
AGENTLINK_REAL_KIMI=1 go test -run TestRealKimi ./internal/worker     # 真 kimi：新建会话 + 续接 + 查额度，用一点 Kimi 额度
```
