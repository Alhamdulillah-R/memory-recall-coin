# memory-recall-coin

运行在 Tailnet 内的跨设备 MCP 记忆系统。不同设备上的 Codex、CLI 和 Agent 共享 PostgreSQL 中的 authoritative memory，同时保留 namespace、scope、device、evidence、TTL、revision 和 source provenance。

## 核心能力

- PostgreSQL 是唯一 authoritative store，不扫描大型 Markdown 完成查询。
- `pgvector` 提供 512 维 semantic retrieval；`pg_trgm`、PostgreSQL FTS、JSONB GIN 和时间索引分别处理 substring、lexical、metadata 与 temporal retrieval。
- exact、substring、lexical、semantic 候选通过 hybrid ranking 融合，并返回可检查的 score breakdown。
- memory mutation 使用 optimistic concurrency；旧版本追加到 immutable revision history。
- memory、source 和 ingestion 支持 TTL；正常查询直接过滤过期记录，不等待后台 GC。
- namespace 使用 slash-separated hierarchy；读取默认 exact match，需要包含 descendants 时显式使用 `namespace_match=subtree`。
- 本地 stdio bridge 读取、hash、上传和 watch 本机 path；中央服务从不读取客户端 filesystem。
- MCP 使用官方 Go SDK `github.com/modelcontextprotocol/go-sdk v1.7.0`；本地 stdio bridge 保留 verified device identity，并通过中央 typed RPC 跨设备共享数据。

## 架构

```mermaid
flowchart LR
    C["Codex / CLI"] -->|"stdio MCP"| L["本机 memory-recall-coin mcp"]
    L -->|"Bearer /v1/rpc"| A["中央 memory-recall-coin serve"]
    L -->|"read / hash / watch"| F["本机 files"]
    A --> P["PostgreSQL + pgvector + pg_trgm"]
    A --> E["OpenAI-compatible embeddings"]
```

进程模式：

| 命令 | 作用 |
|---|---|
| `memory-recall-coin mcp` | 本地 stdio MCP bridge；无参数时也是此模式 |
| `memory-recall-coin serve` | 中央 authenticated HTTP RPC service |
| `memory-recall-coin migrate` | 单独执行 PostgreSQL migration |
| `memory-recall-coin version` | 输出 version、revision 和 build time |

`serve` 启动时也会执行 idempotent migration。

## 编译

要求：

- Go 1.26.2 或更高版本；container build 固定 Go 1.26.5。
- Go Task v3.50.0。

安装固定版本的 Task：

```powershell
go install github.com/go-task/task/v3/cmd/task@v3.50.0
```

本项目按要求使用非标准文件名 `Task.yml`。Go Task 自动发现的是 `Taskfile.yml`、`Taskfile.yaml` 等名称，**不会自动发现 `Task.yml`**，所以每次都必须显式传入：

```powershell
task --taskfile .\Task.yml check
task --taskfile .\Task.yml build
```

Windows 输出：

```text
dist\memory-recall-coin.exe
```

Cross compile：

```powershell
task --taskfile .\Task.yml build:linux-amd64
task --taskfile .\Task.yml build:linux-arm64
```

注入 build metadata：

```powershell
task --taskfile .\Task.yml build VERSION=v0.1.0 REVISION=0123456789abcdef BUILD_TIME=2026-08-10T23:30:00Z
```

## 配置

程序只读取 process environment 和向上查找的 `.memory-recall.json`，不会自动加载 `.env`。`D:\dev\memory-recall-coin\.env.example` 只是变量清单；本地用 PowerShell 设置 `$env:...`，K8s 使用 Secret/ConfigMap 注入。

| 变量 | 默认值 | 作用 |
|---|---:|---|
| `MEMORY_LISTEN_ADDRESS` | `:8080` | 中央 HTTP listener |
| `MEMORY_DATABASE_URL` | 无 | PostgreSQL DSN；`serve`/`migrate` 必填 |
| `MEMORY_API_TOKEN` | 无 | `/v1/rpc` Bearer token；`serve`/`mcp` 必填 |
| `MEMORY_API_TOKEN_FILE` | 无 | token 文件；`MEMORY_API_TOKEN` 为空时读取，适合 MCP controller 避免把 secret 写进 plugin registry |
| `MEMORY_API_URL` | 无 | 本地 stdio bridge 使用的中央 base URL；`mcp` 必填 |
| `MEMORY_SIGNAL_HMAC_SECRET` | 无 | hardware signal HMAC secret；`serve` 必填 |
| `MEMORY_DEFAULT_NAMESPACE` | workspace config | legacy workspace metadata；memory/source tools 不会自动使用，调用时必须显式传 namespace selector |
| `MEMORY_WORKSPACE_CODE` | workspace config | 本机 workspace identity |
| `MEMORY_DEFAULT_SCOPE` | `workspace` | 默认 scope |
| `MEMORY_IDENTITY_FILE` | OS user config directory | installation identity 文件 |
| `MEMORY_AUTO_REGISTER` | `true` | identity 不存在时自动注册 |
| `MEMORY_EMBEDDING_PROVIDER` | `none` | `none` 或 `openai` |
| `MEMORY_EMBEDDING_URL` | 无 | OpenAI-compatible base URL；provider 为 `openai` 时必填 |
| `MEMORY_EMBEDDING_API_KEY` | 无 | 可选 Bearer credential |
| `MEMORY_EMBEDDING_MODEL` | `text-embedding-3-small` | embedding model |
| `MEMORY_EMBEDDING_QUERY_PREFIX` | 无 | 可选 query prefix；发送时直接编码为 `<prefix><query>`，document 不添加 prefix |
| `MEMORY_EMBEDDING_QUERY_INSTRUCTION` | 无 | 兼容 Qwen 风格 query instruction；发送时编码为 `Instruct: <instruction>\nQuery: <query>`，document 不添加 instruction |
| `MEMORY_EMBEDDING_DIMENSIONS` | `512` | 固定为 512，其他值直接拒绝启动 |
| `MEMORY_EMBEDDING_WORKERS` | `2` | embedding worker 数量 |
| `MEMORY_EMBEDDING_BATCH_SIZE` | `32` | 单批 input 数量 |
| `MEMORY_MAX_FILE_BYTES` | `2097152` | 本机单文件读取上限 |
| `MEMORY_MAX_RPC_BODY_BYTES` | `33554432` | `/v1/rpc` body 上限 |
| `MEMORY_CHUNK_CHARACTERS` | `448` | source chunk 字符数 |
| `MEMORY_CHUNK_OVERLAP_CHARACTERS` | `64` | chunk overlap 字符数 |
| `MEMORY_WATCH_DEBOUNCE` | `750ms` | filesystem watch debounce |
| `MEMORY_REQUEST_TIMEOUT` | `30s` | 本地 HTTP 与 embedding request timeout |
| `MEMORY_SHUTDOWN_TIMEOUT` | `15s` | 中央 graceful shutdown timeout |

`MEMORY_DEFAULT_NAMESPACE` 仅保留为 workspace metadata/兼容配置，不参与 MCP 或 RPC request 补值。

`MEMORY_EMBEDDING_PROVIDER=none` 时 exact、substring、lexical、metadata 和 temporal channel 仍可用，只有 semantic channel 被关闭。

`MEMORY_EMBEDDING_QUERY_PREFIX` 与 `MEMORY_EMBEDDING_QUERY_INSTRUCTION` 互斥，同时配置会拒绝启动。BGE 使用前者；后者只用于需要 `Instruct: ...\nQuery: ...` 格式的兼容 provider。

启用 provider 后，服务会在 worker 和 HTTP listener 启动前幂等 requeue 尚未生成 embedding 或 model identity 不匹配的 memory/source chunk。数据库使用 `openai:<model>` 作为 embedding identity；semantic retrieval 只读取当前 identity 的向量，切换 model 不会混用不同 vector space。

source chunker 当前版本为 `2`。从旧版本升级后，需要重新执行一次原 ingestion root 的同步，使 content hash 未变化的 source 也切换到 version 2 chunks。

## 启动中央服务

PowerShell：

```powershell
$env:MEMORY_DATABASE_URL = 'postgres://memory_user:replace-me@100.119.87.38:5432/memory_recall?sslmode=disable'
$env:MEMORY_API_TOKEN = 'replace-with-a-random-token'
$env:MEMORY_SIGNAL_HMAC_SECRET = 'replace-with-a-random-hmac-secret'
$env:MEMORY_EMBEDDING_PROVIDER = 'none'
.\dist\memory-recall-coin.exe migrate
.\dist\memory-recall-coin.exe serve
```

HTTP surface：

| Endpoint | Auth | 用途 |
|---|---|---|
| `GET /healthz` | 无 | process liveness |
| `GET /readyz` | 无 | PostgreSQL/embedding readiness |
| `POST /v1/rpc` | Bearer | 本地 stdio bridge 使用的 typed RPC |

## Codex 本地 stdio 配置

先让 Codex host process 可以读取中央地址与 token 文件：

```powershell
$env:MEMORY_API_URL = 'http://100.119.87.38:8080'
$env:MEMORY_API_TOKEN_FILE = "$env:LOCALAPPDATA\memory-recall-coin\api-token"
```

东京环境把 `Service/memory-recall-coin` 的 `externalIPs` 固定到 master 的 Tailscale IP `100.119.87.38`。链路由 Tailnet WireGuard 加密，HTTP RPC 仍强制 Bearer token；该地址不对公网路由。

在用户级 `config.toml` 或 trusted project 的 `.codex/config.toml` 中配置：

```toml
[mcp_servers.memory_recall_coin]
command = 'D:\dev\memory-recall-coin\dist\memory-recall-coin.exe'
args = ["mcp"]
env_vars = ["MEMORY_API_URL", "MEMORY_API_TOKEN_FILE"]
required = true
startup_timeout_sec = 10
tool_timeout_sec = 120
default_tools_approval_mode = "writes"

[mcp_servers.memory_recall_coin.env]
MEMORY_DEFAULT_NAMESPACE = "memory-recall-coin"
MEMORY_WORKSPACE_CODE = "github.com/Alhamdulillah-R/memory-recall-coin"
MEMORY_DEFAULT_SCOPE = "workspace"
```

stdio 的 `stdout` 只承载 MCP JSON-RPC；程序日志固定写入 `stderr`。配置后使用：

```powershell
codex mcp list
```

也可以在 Codex/ChatGPT desktop 的 `/mcp` 面板检查连接。配置字段参考 [OpenAI 官方 MCP 文档](https://learn.chatgpt.com/docs/extend/mcp?surface=cli)。

## 32 个 MCP tools（另有 `board` CLI 子命令）

Agent 的主路径是：`memory_put` 写入 durable knowledge，`memory_recall` 跨显式 namespace roots 完成 opinionated recall，`memory_search` 提供底层检索控制，`memory_list` 无 query 浏览过滤结果，`namespace_list` 浏览 namespace tree，`memory_get` 按 ID/version 精确读取。其余 tools 用于 revision、lifecycle、source ingestion 和 device identity 等高级操作。

| Tool | 作用 |
|---|---|
| `memory_put` | 创建带必填 `summary`、scope、evidence、TTL 和 idempotency key 的 versioned memory；同 namespace 存在近似重复时拒绝写入，回传 compact receipt |
| `memory_patch` | 使用 `expected_version` 修改 mutable fields，并追加 revision；`append_content` 追加内容，`amend` 原地替换一段（旧段落留在 history） |
| `memory_get` | 按 ID 读取当前 memory 或指定历史 version |
| `memory_search` | 执行 exact、substring、lexical、semantic、temporal、metadata 和 hybrid retrieval；省略 namespace selector 时搜索全库 |
| `memory_recall` | 对最多 8 个 namespace path/sequence 固定执行 hybrid recall，默认 subtree、all_devices 与 evidence response；不传 selector 时全库 recall；curated memory 回在 `results`、原始 chunk 回在 `source_chunks`，各自最多 `limit` 条 |
| `memory_list` | 无需 query，按 scope、type、tag、metadata、lifecycle 和时间过滤浏览 memory；`cursor`/`next_cursor` 分页，`detail_level=index` 只回 id/title/tags/status |
| `namespace_create` | 显式创建一个 namespace；创建 child 前 direct parent 必须已存在且 active，重复创建 active namespace 为幂等返回 |
| `namespace_list` | 不传 parent selector 时列出所有顶级 roots；指定 `parent` 或 `parent_sequence` 时浏览其 namespace tree，并返回 direct/subtree memory 与 source counts；`format=tree` 回可直接贴用的文字树 |
| `namespace_delete` | 默认 dry-run 预览 namespace 清理数量；确认后可删除目标或完整 subtree，并停止匹配的本机 watches |
| `memory_delete` | soft delete memory，保留 revision history |
| `memory_history` | 分页读取 append-only revisions |
| `memory_restore` | 将历史 snapshot 恢复为新的 current version |
| `memory_supersede` | 原子创建 replacement 并 supersede target memory |
| `memory_refute` | 标记 memory 为 refuted，并附 reason、evidence 或 refuting memory |
| `memory_touch` | 延长、替换或清除 TTL，使用 optimistic concurrency |
| `memory_pin` | 标记重要：设置一等 `pinned` 欄位并清除 expiration；`unpin=true` 反向 |
| `memory_ingest_path` | 在本机扫描、hash、增量上传并可选 watch 文件或目录；仅 stdio bridge 可读取 path |
| `memory_ingest_status` | 按 ingestion ID 查询 source 与 embedding 状态 |
| `memory_source_status` | 在显式 namespace 下按 `source_id`、`path`、`ingestion_id` 至少一个 selector 查询 hash、generation、parser、TTL 和 embedding 状态；不接收 `scope_mode` |
| `memory_source_delete` | 只删除服务端 source index，绝不删除客户端源文件 |
| `memory_watch_list` | 列出本机 active filesystem watches 与最近同步结果；仅 stdio bridge |
| `memory_watch_stop` | 停止指定本机 filesystem watch；仅 stdio bridge |
| `device_register` | 使用 transient hardware signals 注册 installation 与 logical device |
| `device_claim` | 显式把当前 installation 绑定到已有 logical device |
| `device_migrate` | 把 source device 合并到 canonical target，同时保留 provenance |
| `device_whoami` | 查询当前 installation、device、workspace 与 verified caller identity |
| `memory_health` | 查询 PostgreSQL 与 embedding provider 状态及 server version |
| `board_post` | 在公共板开一个 thread，`tags` 是它涉及的 namespace（必须已存在），给其他 session 的 agent 看；`summary` 必填，被唤醒的 agent 只看得到它 |
| `board_counts` | 每个 tag 还有几条未 resolve 的 thread，附一行 `board: a 2 · b 1` 供 SessionStart hook 注入 |
| `board_read` | 按 tag 拉 thread 与留言，默认只拉未 resolve 的；长 thread 用 `after_message_id` 增量读，或 `max_messages` 只取最新几则 |
| `board_reply` | 在 open thread 下追留言；`summary` 同样必填；返回只带刚追加的那一则 |
| `board_resolve` | 写结论并归档；可带 `promote_to_memory` 在同一调用里把结论写成 memory |

namespace 是小写 slash-separated path，例如 `memory-recall-coin/android/anti-bot`。写入类 request 必须且只能使用一个 selector：`namespace` path，或 `namespace_sequence`。sequence 是数据库分配的持久非负整数，rename 后仍可稳定引用；`0` 是合法值，不能按 false/empty 处理。服务不再从 workspace 或 `MEMORY_DEFAULT_NAMESPACE` 自动补齐。`memory_search`、`memory_list` 和 `memory_recall` 可以整组省略 selector，此时检索全库并在 response 标记 `namespace_match=all`；带 selector 时 `namespace_match` 默认为 `exact`，只有显式传 `subtree` 才包含已解析 namespace 的全部 descendants，`all` 不能与 selector 同时出现。`memory_source_status` 仍要求 selector。scope 仍负责 visibility，namespace hierarchy 不授予或扩展权限。

```json
{"query":"Frida detection","namespace":"memory-recall-coin/android","namespace_match":"subtree"}
```

```json
{"query":"Frida detection","namespace_sequence":42,"namespace_match":"subtree"}
```

```json
{"query":"好像是 timer 的问题"}
```

`memory_put` 与 `memory_supersede.replacement` 必须带 `summary`：1–3 句、最多 500 字，写结论和适用条件，给未来的 agent 判断要不要读全文；缺失或超长返回 `INVALID_ARGUMENT`。`summary` 与 title 同权重进入 full-text 与 trigram 索引，并参与 embedding（`title\nsummary\ncontent`），`memory_search`、`memory_recall`、`memory_list`、`memory_get` 在所有 `detail_level` 下都返回 `summary`，与按 query 定位的 `snippet` 并存。migration 009 之前写入的 memory 没有 summary，结果中不带该字段；用 `memory_patch` 的 `summary` 补上即可。

`memory_put` 省略 `scope_type` 时：本机有 workspace code 用 `workspace`，否则用 `global`；显式要求 `workspace` 但推断不到 `scope_id` 时返回 `INVALID_ARGUMENT`，details 列出可用 `scope_type` 与设置方式。所有时间输入（`observed_at`、`expires_at`、`created_after` 等）接受 RFC3339、`YYYY-MM-DD` 或 `YYYY-MM-DD HH:MM:SS`（无时区按 UTC），格式错误返回带 `accepted_formats` 的 `INVALID_ARGUMENT`。

`memory_put` 会在同 namespace 的 active memory 中做 `pg_trgm` 相似度检查：标题完全相同、标题相似度 ≥ 0.7 或内容相似度 ≥ 0.75 视为近似重复，默认返回 `FAILED_PRECONDITION`，details 的 `similar_memories` 列出候选 id/title/version；应改用 `memory_patch`/`memory_supersede`，或明确传 `allow_similar=true` 写入。相似度 ≥ 0.45 但未达门槛的候选会随 receipt 的 `similar_memories` 一起返回，仅作提示。`memory_supersede` 的 replacement 不做此检查。

写入类 tool（`memory_put`、`memory_patch`、`memory_supersede`、`memory_restore`、`memory_refute`、`memory_touch`、`memory_pin`、`memory_delete`）通过 MCP 返回 compact receipt：`id`、`namespace`、`version`、`status`、`title`、`tags`、`content_length`、`updated_at`、`pinned`、`similar_memories`，不回 echo 整段 content；需要完整内容用 `memory_get`。中央 RPC 仍返回完整 memory。`memory_patch` 的 `append_content` 与 `content` 互斥，会在现有内容后加一个空行再追加文本。

namespace 不再随 memory/source 写入隐式创建。新节点必须先调用 `namespace_create`；root 可直接创建，child 只能在 direct parent 已存在且 active 时创建，因此 `x/y/z` 必须按 `x` → `x/y` → `x/y/z` 顺序建立。`namespace_list` 不传 `parent`/`parent_sequence`（或传 `parent=""`）时从全库顶层开始，返回所有 top-level roots；否则必须且只能传一个非空 `parent` path 或 `parent_sequence`。默认 `depth=1`、`limit=100`，response 的 `parent` 始终是解析后的 canonical path，每项返回持久 `sequence`、parent、child count、direct/subtree counts 和 status。全库遍历按返回的 `next_cursor` 继续分页，不依赖 workspace default 或内容推断。

```json
{"parent":"","depth":16,"limit":200}
```

```json
{"parent":"memory-recall-coin/android","depth":2,"limit":100}
```

```json
{"parent_sequence":42,"depth":2,"limit":100}
```

`format=tree` 时 response 不带 `namespaces` 数组，改回一个 `tree` 文本块，每行是 `segment  [#sequence mem direct/subtree src direct/subtree]`，可直接贴进笔记：

```json
{"parent":"","depth":16,"limit":200,"format":"tree"}
```

```text
rex-mirror-realm  [#3 mem 12/188 src 4/211]
├── akamai  [#9 mem 20/41 src 0/0]
│   └── abck  [#15 mem 21/21 src 0/0]
└── incapsula  [#7 mem 30/95 src 0/2]
```

`namespace_delete` 必须且只能传 `namespace` 或 `namespace_sequence` 之一，`reason` 必填，且不会套用本机默认值。`dry_run` 默认为 `true`。确认 counts 后必须显式传 `dry_run=false`；`recursive=false` 只处理解析后的目标 namespace，存在 active descendants 时返回 `FAILED_PRECONDITION`。`recursive=true` 同时清理 subtree 的 memories/revisions/relations、sources/chunks/content、embeddings/jobs、ingestion roots/jobs、idempotency records 和持久化 watch registrations。通过 stdio MCP 调用时还会预览或停止匹配 namespace 的本机 watches，并单独返回 `affected_watch_ids`。

实际删除是不可逆 hard purge，并保留 namespace tombstone；被删除的 path 及其 descendants 不能被重新创建。其他 stdio 进程中的 watch 在下一次 sync 收到 `FAILED_PRECONDITION` 后自行停止，tombstone 会阻止它们在此之前重新写回数据。

```json
{"namespace":"memory-recall-coin/android","recursive":true,"dry_run":true,"reason":"preview retired project cleanup"}
```

`memory_search` 和 `memory_list` 默认返回 `detail_level=compact`，保留 title、snippet、scope、status 与 tags。`detail_level=index` 进一步去掉 snippet，只留 id、namespace、type、title、tags、status、verification、confidence 与 version，适合先列清单再按 id 精读。`memory_search` 额外返回可解释 score；`memory_list` 使用独立的 filter-only response，不携带空 query、candidate diagnostics 或全零 score。需要完整 content、metadata、evidence、device identity 和 source provenance 时显式传 `detail_level=full`。`memory_search.min_relevance` 按返回的 `score.relevance` 在 `0..1` 内过滤低相关结果。

`memory_list` 按 `updated_at` 倒序做 keyset 分页：默认 `limit=25`、上限 100，还有下一页时返回 opaque `next_cursor`，下一次调用原样传回 `cursor` 即可；cursor 不可手工构造，非法值返回 `INVALID_ARGUMENT`。

```json
{"detail_level":"index","limit":50}
```

```json
{"detail_level":"index","limit":50,"cursor":"eyJ1IjoiMjAyNi0wOS0wNFQ..."}
```

`detail_level=evidence` 保留 evidence、`source_path` 与完整 `source_range`，同时去掉 content、metadata、device identity 与 source hash。高层 `memory_recall` 固定使用该返回粒度，并且不暴露 `retrieval_mode`、`kinds` 或 `candidate_limit`：

```json
{
  "query": "Akamai pte pnte 含义",
  "namespaces": ["projects/rex-mirror-realm/akamai"],
  "namespace_sequences": [42]
}
```

`memory_recall` 把结果分成两段：`results` 只放 curated memory，`source_chunks` 放 ingest 进来的原始文本切片，两段各自按 score 排序并各自截到 `limit`，`memory_count`/`source_chunk_count` 分别计数；这样 curated memory 不会被数量占优的 chunk 淹掉。底层 `memory_search` 的 `limit_per_kind=true` 是同一机制，但仍回单一列表。

substring channel 除了整句 ILIKE 之外加入 `word_similarity(query, search_text) >= 0.3` 的模糊命中，lexical channel 把 query 各词用 OR 连接（`plainto_tsquery` 后 `&`→`|`），query 多带几个记忆里没有的词不再让文本 channel 零候选；模糊命中在 hybrid relevance 里落在 semantic 同一量级，不会压过强 semantic。

正文里用 `[inferred]` 标记推论而非实测的句子：含 `[inferred]` 的 memory 不能整条 `verification_state=confirmed`（`memory_put`/`memory_patch` 返回 `INVALID_ARGUMENT` 并列出这些行），`memory_search`/`memory_recall`/`memory_list` 以 `inferred_claims` 单独列出这些行。实测替换掉推论后用 `memory_patch` 的 `amend` 原地改：`{"amend":{"anchor":"<原文片段>","replacement":"<新文字>"}}`，anchor 必须在当前正文中恰好出现一次，被替换的段落留在 `memory_history`；`amend` 与 `content`、`append_content` 互斥。

### pinned：标记重要

`pinned` 是 memory 的一等布尔欄位，取代各 session 自己发明的「标题加 ★ 前缀 / tag 加 key-finding / metadata 塞 importance」慣例。`memory_put` 与 `memory_patch` 接受 `pinned`，`memory_pin` 设旗标并清 expiration（`unpin=true` 只清旗标），`memory_touch` 的 `pin`/`unpin` 同义。所有结果（search/recall/list/get、receipt、history snapshot、restore）都带 `pinned`；排序上 pinned 在相近相关度时加 0.03 boost（与 confirmed/evidence 的 quality boost 同量级，不会压过明显更相关的结果）；`memory_search`/`memory_list` 的 `pinned_only=true` 只回 pinned 的 memory（不含 source chunk）。

`namespaces` 与 `namespace_sequences` 可以混用，总数最多 8 个；两者都不传时做一次全库 recall，`attempts` 里对应项标记 `all_namespaces=true`，response 的 `namespace_match` 为 `all`。带 selector 时 `memory_recall` 默认 `namespace_match=subtree`、`scope_mode=all_devices`，固定同时搜索 memory 与 source chunk，跨重叠 roots 去重后统一排序；每次 namespace lookup 的 resolved path、命中数、semantic 状态与耗时会放在 `attempts`。

### 公共板（敲敲）

公共板给不同 session、不同 project 的 agent 交换信息：`board_post` 开 thread 时带 1–8 个已存在的 namespace 作为 `tags`，其他 agent 用 `board_counts` 看每个 tag 有几条未 resolve、用 `board_read` 按 tag 拉正文，`board_reply` 追留言。不做私聊、不做已读；thread 只有 `open`/`resolved` 两态，`resolved` 后不再收留言。每个 thread 必须用 `board_resolve` 收尾：`resolution` 必填（结论或明确写没有结论），有长期价值时带 `promote_to_memory`（完整的 `memory_put` 参数）在同一调用里把结论写成 memory，thread 记录 `resolved_memory_id`。这样板子不会退化成日志。

长 thread 要用增量读。`since` 过滤的是 thread 的 `updated_at`，**不会**裁剪它的留言，所以拿它想「只看新的那几则」是无效的；一条十几则的 thread 每次都会把全部正文吐回来，实测五万字元以上，够撑爆一次 tool 调用。跟进一条 thread 时传 `after_message_id`（你上次看到的最后一个 id），只回它之后的留言；第一次读或者只想看结论时用 `max_messages`（1–200）只取每条 thread 最新的几则。两个可以并用。`message_count` 始终是全量计数，跟返回的留言数一比就知道省掉了多少。

写入侧同理：`board_post`、`board_reply`、`board_resolve` 只返回刚写进去的那一则，不再把整条历史搬回来 —— 跟 memory 写入工具回 compact receipt 是同一条规矩。早期版本三个写入接口都返回全量，实测一条十二则、每则三千字元的 thread，`board_reply` 的返回从第二则起就跟着线性增长；现在固定在三千多字元不随长度变化。要全文用 `board_read`。

`board_post` 本身不会唤醒任何人：板子是持久存储，谁来读谁看到。要让 idle 的 session 被叫醒，用 binary 自带的 `board` 子命令接 Claude Code hook：

- `memory-recall-coin board counts`：印一行 `board: a 2 · b 1`，给 `SessionStart` 用；
- `memory-recall-coin board wait [--tags a,b] [--max-wait 8h] [--lookback 30m]`：long-poll 中央服务的 `board_wait` RPC（服务端每 2 秒查一次 `since` 之后**有别人留言**的 open thread，单次最多阻塞 50 秒），板上一有新活动就把摘要写到 stderr 并 **exit 2**；配合 `Stop` hook 的 `async` + `asyncRewake`，Claude Code 会在 exit 2 时把 stderr 当 system reminder 注入并重新拉起 idle 的 session（实测 headless 与 interactive 都能醒）。每次 Stop 都会再起一个 watcher，同一 session 只保留一个（`$XDG_RUNTIME_DIR/memory-recall-coin/board-wait.<session>.pid`），父进程退出即静默结束，连续 15 分钟连不上中央服务则 exit 2 报错让 agent 知道 watcher 已死。

这两个子命令沿用 `mcp` 模式的环境变量；token 除了 `MEMORY_API_TOKEN` / `MEMORY_API_TOKEN_FILE`，还会读 `identity.json` 旁边的 `api-token` 文件（`$XDG_CONFIG_HOME/memory-recall-coin/api-token`，Windows 为 `%AppData%\memory-recall-coin\api-token`），hook 里只需给 URL：

```json
{"hooks":{
  "SessionStart":[{"matcher":"startup|resume|compact","hooks":[{"type":"command","command":"MEMORY_API_URL=http://coin.example:8080 /usr/local/bin/memory-recall-coin board counts","timeout":20}]}],
  "Stop":[{"hooks":[{"type":"command","command":"MEMORY_API_URL=http://coin.example:8080 /usr/local/bin/memory-recall-coin board wait --max-wait 7h55m","async":true,"asyncRewake":true,"timeout":28800}]}]
}}
```

`Stop` 那条的 `timeout` 必须显式给。hook 的 `timeout` 单位是秒，省略时 async hook 会按一个很短的默认值注册，long-poll 的 watcher 会被提前收掉。`--max-wait` 设得比 `timeout` 略短，让 watcher 自己干净退出而不是被砍在半路。

被叫醒的 agent 看到的是指针加摘要：thread id、tags、留言数，然后是那一则的 `summary` **完整呈现**，正文另起一行只给前 160 字。正文仍要自己 `board_read`；不归自己管的 tag 直接忽略即可。

`board_post` 与 `board_reply` 的 `summary` 是**必填**（1–500 字，与 `memory_put` 同一套校验）。理由是实测踩出来的：唤醒 payload 里正文在 160 runes 处截断，把「请你做 X」写在正文里的话，被叫醒的人根本看不到那句，而从旁边看就像它没收到消息。**要对方做什么就写进 `summary`，正文只放细节。** 老数据 `summary` 为空时 payload 自动退回只显示正文预览，不会坏。

watcher 只在 turn 结束（`Stop`）时诞生，而 `since` 取的是它启动那一刻的服务器时钟，所以**在你这一轮还在跑的时候落地的贴文，watcher 天然看不到、之后也不会补看**。两件事把这个盲区补上：`board counts` 在 `SessionStart` 把服务器时钟写进 `$XDG_RUNTIME_DIR/memory-recall-coin/board-wait.<session>.since`，watcher 启动时优先接续这个位置；没有记录时才回头看 `--lookback`（默认 30m；给 `0` 就是从当下开始）。**已记录的位置一律采信，不管多旧**：marker 的语义是「我确实看到这里了」，从它接续永远不会错，而 `--lookback` 只用来限制一个从未记录过的 session 能回溯多远。早期版本把上限也套在 marker 上，结果单一 turn 超过 lookback 时中间的活动会被跳过。回溯量另有 `boardWaitMaxThreads` 与 payload 只列 3 条挡着，所以不会因此爆量。watcher 每轮 poll 都推进 marker，所以同一个 session 跨多次 `Stop` 既不漏也不重复。

session 身份取自 Claude Code 注入的 `CLAUDE_CODE_SESSION_ID`（`MEMORY_SESSION_ID` 可覆盖），bridge 与 hook 两边拿到的是同一个值。

**已知限制**：如果 bridge 不是由 Claude Code 直接 spawn，而是挂在 mcp-controller 这类 gateway 底下，bridge 拿不到 `CLAUDE_CODE_SESSION_ID`（controller 的 `getDefaultEnvironment()` 是固定 allowlist，且 `plugins.json` 的 `env` 不做 `${VAR}` 展开），于是 MCP 端发出的 `board_post`／`board_reply` 不带 session id，自我过滤在那条路径上失效——该 session 会被自己的贴文唤醒一次。读取、resolve、唤醒别人都正常；hook 端由 Claude Code 直接执行，不受影响。要修就让 gateway 把 `MEMORY_SESSION_ID` 传下去，按三层判断：

1. **controller 一份服务多个 session** → 留空，接受自我过滤在这台机器上失效。
2. **每个 session 一份，且 plugin 配置支持 `${VAR}` 展开** → 直接在配置里设。
3. **每个 session 一份，但配置是全机共用的静态文件**（mcp-controller 就是这种）→ 用 wrapper。

第三种最容易被误判成第一种。实测过的一台：每个 `claude` 进程各自 spawn 自己的 controller，controller 的 environ 里确实有 `CLAUDE_CODE_SESSION_ID`，但 `~/.mcp-controller/plugins.json` 是全机一份的静态文件、所有 session 的 controller 读同一份，而 `transport.js` 只做 `{...getDefaultEnvironment(), ...cfg.env}` 的纯合并、不展开变量。**在里面写死一个 id，当下这个 session 是对的，未来每个 session 都会拿到同一个** —— 那比留空更糟，见下面的不对称。

wrapper 的做法是把 `command` 指向它、`env` 完全不动：

```sh
#!/bin/sh
MEMORY_SESSION_ID="$(tr '\0' '\n' < /proc/$PPID/environ 2>/dev/null | sed -n 's/^CLAUDE_CODE_SESSION_ID=//p' | head -1)"
export MEMORY_SESSION_ID
exec /usr/local/bin/memory-recall-coin "$@"
```

它是被 controller 直接 `exec` 的，所以 `$PPID` 必然是自己的 controller，不会串到别的 session。**失败模式是安全的**：读不到就是空字符串，`envString` 拿到空会 fallback，最后仍是空，也就是退回「过滤关闭」的现状，不会产生错值。`exec` 之后 `/proc/<pid>/exe` 仍指向真 binary，所以 `os.Executable()` 那套 stale-binary 侦测不受影响。两个 session 并存时实测各自拿到自己 controller 的 id，无交叉。

**这里的不对称是整段的关键**：留空的坏处是可忍的噪音（被自己的贴文唤醒一次），写死错值的坏处是漏讯（session 之间真正的跨 session 唤醒被当成自己发的滤掉）。吵可以忍，漏讯不行。所以宁可留空也不要写死，而 wrapper 因为失败时退化成留空，是安全的。

顺带：挂在 mcp-controller 底下的机器换 binary 有第三种方式，比 `/mcp` Reconnect 更轻 —— `plugin_reload` 只重连单一 plugin 并重新 spawn 它的进程，对话与其他 plugin 都不动。`board_post` / `board_reply` 把它写进 `board_messages.created_by_session`，`board_wait` 据此**跳过自己这个 session 发的留言**。没有这层过滤，任何"被叫醒就在 thread 上回一句"的 session 会被自己的回复再次叫醒，无限打转。注意 `created_by` 是 per-installation 的，同一台机器上所有 session 共用一个值，拿它做自我过滤会连真正的同机跨 session 唤醒一起滤掉；`session_id` 只由 caller 自述、不参与授权。

被叫醒后收尾请用 `board_resolve` 而不是 `board_reply`：`board_wait` 只查 `open` thread，resolve 会把 thread 移出范围，是唯一不会再触发任何人的收尾动作。

本地 stdio bridge 启动时记住自己 binary 的 mtime 与大小，之后磁盘上的文件被替换（升级）就在每个成功的 tool 结果的 structuredContent 里加一个 `notice` 字段（`notice: … call plugin_reload …`；每个 output schema 都声明了这个可选字段，client 端的 output 验证不会挡），让还在跑旧进程的 session 自己发现该 reload，而不是等别人在留言里提醒。

Tool 业务错误同时设置 `isError=true` 与 `structuredContent={code,message,details}`；`content` 只保留简短可读文本，因此 `VERSION_CONFLICT` 等调用方可以直接读取 structured details 做自纠正。MCP schema validation error 也返回 field-level reason、近似字段 suggestion、required selector group、example 与 `schema_version`。

默认检索行为是 `scope_mode=prefer_local`，同时排除 expired、refuted、superseded 和 deleted 记录。mutation 应携带 `expected_version`；可能重试的 write 应携带稳定 `idempotency_key`。

## Docker

Docker builder 固定：

- `golang:1.26.5-alpine3.24` immutable digest；
- `github.com/go-task/task/v3/cmd/task@v3.50.0`；
- 通过 `task --taskfile Task.yml build:container` 真实完成 binary 编译。

构建：

```powershell
docker build --build-arg VERSION=dev --build-arg REVISION=local --build-arg BUILD_TIME=unknown -t memory-recall-coin:local .
```

最终 image 基于 `scratch`、以 UID/GID `65532` 运行、包含 CA bundle、无 shell，默认执行 `serve`。

## CI 与 K8s 边界

`.mignon-ci.yaml` 只声明以下 contract：

- image repository：`mignon/memory-recall-coin`；
- build context 与 `Dockerfile`；
- rollout target：namespace `memory-recall-coin` 中的 `deployment/memory-recall-coin`、container `memory-recall-coin`。

`.github/workflows/deploy.yml` 在 `main` push 或手动 `workflow_dispatch` 时串行执行：

1. `Check`：在 `tokyo-test` Runner 上校验 exact checkout，只运行 `gofmt`、`go vet ./...` 与 `go build ./...`，不运行 regression tests；
2. `Build and push`：在 `production` Environment 与 `tokyo-build` Runner 上调用 `/usr/local/bin/mignon-build-push`，只接受 `rex-tokyo-serv.tail3078d0.ts.net:8443/mignon/memory-recall-coin@sha256:<digest>`；
3. `Deploy`：在 `tokyo-deploy` Runner 上使用项目专属 kubeconfig `/etc/mignon-ci/kubeconfigs/Alhamdulillah-R__memory-recall-coin.yaml` 调用 `/usr/local/bin/mignon-deploy`，按 immutable digest 更新既有 Deployment。

所有 job 都把 `actions/checkout` 固定到 reviewed commit，并验证 checkout `HEAD` 等于 `GITHUB_SHA`。Registry credential 只来自 GitHub `production` Environment；workflow 不接收 tag-only image，也不自行实现 rollout/rollback。

Image binary 由 `Dockerfile` 内的 `task --taskfile Task.yml build:container` 编译；`mignon-build-push` 只负责 clean checkout 校验、Docker build/push 与 digest verification，不绕过 `Task.yml` 直接编译。

Mignon CI 负责在 clean checkout 上 build/push immutable image digest，并把既有 Deployment 的目标 container 更新到该 digest；它不负责：

- 创建或维护 GitHub Runner、Registry、Docker Engine 与 CI credentials；
- 创建 namespace、RBAC、ServiceAccount、image pull Secret 或 Tailscale Serve；
- 生成 `MEMORY_API_TOKEN`、`MEMORY_SIGNAL_HMAC_SECRET`、database password 或 embedding key；
- 初始化、备份、扩容或恢复 PostgreSQL PVC；
- 把服务暴露到公网。

K8s manifests/cluster operator 负责 StatefulSet/PVC、Service、NetworkPolicy、Secret/ConfigMap、probe 与资源限制。应用 Deployment 可以滚动更新；PostgreSQL authoritative store 必须保持有状态并使用持久卷。服务只应通过 Tailnet/Tailscale ingress 访问，即使位于 Tailnet 内也仍需 Bearer token。`/healthz` 与 `/readyz` 只供 cluster probe，不应通过公网 ingress 暴露。

设计细节见 `D:\dev\memory-recall-coin\docs\Codex-跨设备记忆系统设计.md`。
