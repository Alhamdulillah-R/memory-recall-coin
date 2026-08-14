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

## 27 个 MCP tools

Agent 的主路径是：`memory_put` 写入 durable knowledge，`memory_recall` 跨显式 namespace roots 完成 opinionated recall，`memory_search` 提供底层检索控制，`memory_list` 无 query 浏览过滤结果，`namespace_list` 浏览 namespace tree，`memory_get` 按 ID/version 精确读取。其余 tools 用于 revision、lifecycle、source ingestion 和 device identity 等高级操作。

| Tool | 作用 |
|---|---|
| `memory_put` | 创建带 scope、evidence、TTL 和 idempotency key 的 versioned memory |
| `memory_patch` | 使用 `expected_version` 修改 mutable fields，并追加 revision |
| `memory_get` | 按 ID 读取当前 memory 或指定历史 version |
| `memory_search` | 执行 exact、substring、lexical、semantic、temporal、metadata 和 hybrid retrieval |
| `memory_recall` | 对最多 8 个显式 namespace path/sequence 固定执行 hybrid memory + source_chunk recall，默认 subtree、all_devices 与 evidence response |
| `memory_list` | 无需 query，按 scope、type、tag、metadata、lifecycle 和时间过滤浏览 memory |
| `namespace_create` | 显式创建一个 namespace；创建 child 前 direct parent 必须已存在且 active，重复创建 active namespace 为幂等返回 |
| `namespace_list` | 不传 parent selector 时列出所有顶级 roots；指定 `parent` 或 `parent_sequence` 时浏览其 namespace tree，并返回 direct/subtree memory 与 source counts |
| `namespace_delete` | 默认 dry-run 预览 namespace 清理数量；确认后可删除目标或完整 subtree，并停止匹配的本机 watches |
| `memory_delete` | soft delete memory，保留 revision history |
| `memory_history` | 分页读取 append-only revisions |
| `memory_restore` | 将历史 snapshot 恢复为新的 current version |
| `memory_supersede` | 原子创建 replacement 并 supersede target memory |
| `memory_refute` | 标记 memory 为 refuted，并附 reason、evidence 或 refuting memory |
| `memory_touch` | 延长、替换或清除 TTL，使用 optimistic concurrency |
| `memory_pin` | 清除 expiration，并把 TTL 变化写入 history |
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

namespace 是小写 slash-separated path，例如 `memory-recall-coin/android/anti-bot`。每个 memory/source request 必须且只能使用一个 selector：`namespace` path，或 `namespace_sequence`。sequence 是数据库分配的持久非负整数，rename 后仍可稳定引用；`0` 是合法值，不能按 false/empty 处理。服务不再从 workspace 或 `MEMORY_DEFAULT_NAMESPACE` 自动补齐。`memory_search`、`memory_list` 和 `memory_source_status` 的 `namespace_match` 默认为 `exact`；只有显式传 `subtree` 才包含已解析 namespace 的全部 descendants。scope 仍负责 visibility，namespace hierarchy 不授予或扩展权限。

```json
{"query":"Frida detection","namespace":"memory-recall-coin/android","namespace_match":"subtree"}
```

```json
{"query":"Frida detection","namespace_sequence":42,"namespace_match":"subtree"}
```

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

`namespace_delete` 必须且只能传 `namespace` 或 `namespace_sequence` 之一，`reason` 必填，且不会套用本机默认值。`dry_run` 默认为 `true`。确认 counts 后必须显式传 `dry_run=false`；`recursive=false` 只处理解析后的目标 namespace，存在 active descendants 时返回 `FAILED_PRECONDITION`。`recursive=true` 同时清理 subtree 的 memories/revisions/relations、sources/chunks/content、embeddings/jobs、ingestion roots/jobs、idempotency records 和持久化 watch registrations。通过 stdio MCP 调用时还会预览或停止匹配 namespace 的本机 watches，并单独返回 `affected_watch_ids`。

实际删除是不可逆 hard purge，并保留 namespace tombstone；被删除的 path 及其 descendants 不能被重新创建。其他 stdio 进程中的 watch 在下一次 sync 收到 `FAILED_PRECONDITION` 后自行停止，tombstone 会阻止它们在此之前重新写回数据。

```json
{"namespace":"memory-recall-coin/android","recursive":true,"dry_run":true,"reason":"preview retired project cleanup"}
```

`memory_search` 和 `memory_list` 默认返回 `detail_level=compact`，保留 title、snippet、scope、status 与 tags。`memory_search` 额外返回可解释 score；`memory_list` 使用独立的 filter-only response，不携带空 query、candidate diagnostics 或全零 score。需要完整 content、metadata、evidence、device identity 和 source provenance 时显式传 `detail_level=full`。`memory_search.min_relevance` 按返回的 `score.relevance` 在 `0..1` 内过滤低相关结果。

`detail_level=evidence` 保留 evidence、`source_path` 与完整 `source_range`，同时去掉 content、metadata、device identity 与 source hash。高层 `memory_recall` 固定使用该返回粒度，并且不暴露 `retrieval_mode`、`kinds` 或 `candidate_limit`：

```json
{
  "query": "Akamai pte pnte 含义",
  "namespaces": ["projects/rex-mirror-realm/akamai"],
  "namespace_sequences": [42]
}
```

`namespaces` 与 `namespace_sequences` 可以混用，总数最多 8 个。`memory_recall` 默认 `namespace_match=subtree`、`scope_mode=all_devices`，固定同时搜索 memory 与 source chunk，跨重叠 roots 去重后统一排序；每次 namespace lookup 的 resolved path、命中数、semantic 状态与耗时会放在 `attempts`。

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
